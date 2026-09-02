// Regression test: an agent pinned to a SUPERSEDED controller instance must
// migrate itself to the instance that is actually serving users.
//
// The production failure (2026-09-02): every deploy emptied the machine list.
// Cloud Run starts the new revision but cannot drain the old one while a
// WebSocket holds it open, so the old instance keeps running — it never even
// receives SIGTERM, so hub.shutdown()'s io.close() never fires — and keeps
// serving the agents pinned to it. Browser traffic, meanwhile, is routed to the
// NEW revision, whose in-memory machine registry has never seen those agents.
// Both sides are healthy; they are simply talking to different instances.
//
// Nothing the agent can observe on its own socket reveals this: the peer is
// alive, answers, and keeps pinging (so the liveness watchdog stays quiet), and
// re-announcing HELLO only re-registers with the same superseded instance. The
// distinguishing signal is the revision serving USER traffic, which the agent
// reaches over plain HTTP through the same frontend the browser uses.
//
// Here one server plays both roles: the WebSocket keeps reporting the old
// revision (a pinned socket) while /api/health starts reporting a new one (a
// completed rollout). A correct agent notices and redials.

import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer } from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { Server } from "socket.io";

const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const POLL_MS = 200;
const OLD_REVISION = "controller-00064-old";
const NEW_REVISION = "controller-00065-new";

// What /api/health reports: the revision serving user traffic.
let servingRevision = OLD_REVISION;
// What the agent's WebSocket reports: the instance its socket is pinned to.
// It stays OLD for the whole test — a pinned socket never migrates by itself.
let connections = 0;

const httpServer = createServer((req, res) => {
  if (req.url === "/api/health") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: true, revision: servingRevision }));
    return;
  }
  res.writeHead(404);
  res.end();
});

const io = new Server(httpServer, { path: "/agent/connect", transports: ["websocket"], pingInterval: 100 });
io.on("connection", (socket) => {
  connections += 1;
  socket.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }
    if (msg.t !== "hello") return;
    // The pinned instance always identifies itself as the OLD revision.
    socket.send(JSON.stringify({ t: "info", revision: OLD_REVISION, features: {} }));
  });
});

await new Promise((resolve) => httpServer.listen(0, "127.0.0.1", resolve));
const { port } = httpServer.address();

const childScript = `
import { runAgent } from ${JSON.stringify(path.join(root, "lib/agent.mjs"))};
runAgent(${JSON.stringify(`http://127.0.0.1:${port}`)}, {
  tmux: async () => "t", readdir: async () => [], branch: async () => ({ branch: "", worktree: false }),
});
`;

const child = spawn(process.execPath, ["--input-type=module", "-e", childScript], {
  env: {
    ...process.env,
    AGENT_MACHINE: "revision-migration",
    AGENT_REVISION_POLL_MS: String(POLL_MS),
    AGENT_REVISION_POLL_TIMEOUT_MS: "2000",
    // Keep the liveness watchdog out of it: the server here is chatty, so the
    // watchdog would not fire anyway, but pin it so a failure can only mean the
    // revision path did (or did not) work.
    AGENT_TRANSPORT_STALE_MS: "600000",
    TMUX_MOBILE_AGENT_ID: "10000000-0000-4000-8000-00000000000d",
  },
  stdio: "ignore",
});

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function waitFor(predicate, timeoutMs, label) {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    if (predicate()) return true;
    await sleep(25);
  }
  assert.fail(`timed out waiting for ${label}`);
}

try {
  await waitFor(() => connections >= 1, 10_000, "the agent's first connection");
  const connectionsBefore = connections;

  // Steady state: serving revision matches the socket's. The agent must NOT
  // churn its connection while nothing has changed — otherwise this test would
  // pass on an agent that simply reconnects in a loop.
  await sleep(POLL_MS * 6);
  assert.equal(
    connections,
    connectionsBefore,
    "agent reconnected while the revision was unchanged (redial loop, not a migration)",
  );

  // The rollout completes: user traffic now goes to a new revision, while this
  // agent's socket is still pinned to the old instance.
  servingRevision = NEW_REVISION;

  await waitFor(
    () => connections > connectionsBefore,
    15_000,
    "the agent to redial after the serving revision changed under its pinned socket",
  );

  console.log("agent-revision-migration: ok");
} finally {
  child.kill("SIGKILL");
  io.close();
  await new Promise((resolve) => httpServer.close(resolve));
}
