// Agent mode transport (`server.mjs --register <hubUrl>`). Dials the controller
// over Socket.IO and serves the controller's tmux/readdir requests using the
// local backend. Socket.IO owns connection management — reconnection with
// exponential backoff, heartbeat liveness (Engine.IO ping/pong on BOTH ends),
// and the connection/handshake timeout — so this file no longer hand-rolls any
// of that. It enforces the tmux subcommand allowlist so the controller can never
// make the agent run a dangerous tmux command.

import os from "node:os";
import { randomUUID } from "node:crypto";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { Readable } from "node:stream";
import { io } from "socket.io-client";
import {
  AGENT_WS_PATH,
  MSG,
  OP,
  helloFrame,
  inventoryFrame,
  isAllowedTmux,
  resErr,
  resOk,
} from "./protocol.mjs";
import { appRevision } from "./revision.mjs";
import { createAgentTranscriptPublisher } from "./transcript-publisher.mjs";
import { discoverTranscriptFiles } from "./transcript-discovery.mjs";
import { detectSshHostCandidates } from "./ssh-hosts.mjs";

// Reconnection + heartbeat + handshake-timeout are all Socket.IO built-ins now
// (configured in the io() options below); these two values just tune them.
//
// Reconnect backoff ceiling. Kept low so a genuine drop recovers in seconds:
// Socket.IO grows the delay 1s, 2s, 4s, 8s and caps here rather than climbing.
const MAX_BACKOFF_MS = Number(process.env.AGENT_MAX_BACKOFF_MS) || 8_000;
// Connection (handshake) timeout. A dial can wedge BEFORE the connection is
// established — TCP connects but the upgrade never completes and no FIN/RST
// arrives — which an app-level ping (it only guards an open connection) can't
// catch. Socket.IO's `timeout` aborts such a dial and retries. This is the class
// of failure that stranded the whole fleet when the controller OOM-restarted.
const CONNECT_TIMEOUT_MS = Number(process.env.AGENT_CONNECT_TIMEOUT_MS) || 15_000;
// Liveness watchdog for an ALREADY-ESTABLISHED socket. CONNECT_TIMEOUT_MS above
// only guards the initial dial; it does nothing once the socket is open. But a
// controller instance can vanish WITHOUT sending a TCP FIN/RST — every Cloud Run
// deploy/instance-replacement (and OOM restart) can do this. The agent's OS then
// keeps the socket ESTABLISHED forever, so Socket.IO's client never sees a
// disconnect and never reconnects: the connector believes it's registered while
// the controller shows "no machine connected", stranded indefinitely. (Observed
// 2026-08-20: two back-to-back deploys stranded the whole fleet ~40+ min.)
//
// Engine.IO's heartbeat is server->client PING; a dead server sends no ping, and
// the client-side pong-timeout does NOT reliably fire on a silently-dead peer.
// So we run our own watchdog: bump `lastServerContactMs` on every inbound signal
// (connect, any message, and low-level engine packets/pings), and if the socket
// claims to be connected but we've heard NOTHING for STALE_MS, treat it as a
// dead half-open connection and force a fresh reconnect. The server pings every
// ~10s (PING_INTERVAL in lib/hub.mjs), so a ~30s silence is unambiguous.
const TRANSPORT_STALE_MS = Number(process.env.AGENT_TRANSPORT_STALE_MS) || 30_000;
const TRANSPORT_WATCHDOG_INTERVAL_MS =
  Number(process.env.AGENT_TRANSPORT_WATCHDOG_INTERVAL_MS) || 10_000;
// How often to check which revision is serving USER traffic, so an agent pinned
// to a superseded instance migrates itself. Cheap (one small unauthenticated GET)
// and bounds how long a machine can be invisible after a deploy. 0 disables it.
const REVISION_POLL_MS = Number(process.env.AGENT_REVISION_POLL_MS ?? 30_000);
const REVISION_POLL_TIMEOUT_MS =
  Number(process.env.AGENT_REVISION_POLL_TIMEOUT_MS) || 10_000;
const INVENTORY_POLL_MS = envMs("AGENT_INVENTORY_POLL_MS", 4_000);
const INVENTORY_TIMEOUT_MS = envMs("AGENT_INVENTORY_TIMEOUT_MS", 12_000);
// Command Center inventory is a frequently refreshed preview, not the full
// transcript transport. A single tool result can make the latest turn several
// megabytes; repeating that text every poll can fill the ordered WebSocket and
// strand Engine.IO heartbeats behind it. Full transcripts use the independent
// archive upload path below, so keep only a bounded UTF-8 preview here.
const INVENTORY_PREVIEW_MAX_BYTES = envMs(
  "TMUX_MOBILE_INVENTORY_PREVIEW_MAX_BYTES",
  4 * 1024,
);
const TRANSCRIPT_ACK_TIMEOUT_MS = envMs("TMUX_MOBILE_TRANSCRIPT_ACK_TIMEOUT_MS", 30_000);
const TRANSCRIPT_UPLOAD_BYTES_PER_SECOND = envMs(
  "TMUX_MOBILE_TRANSCRIPT_UPLOAD_BYTES_PER_SECOND",
  64 * 1024,
);
const TRANSCRIPT_UPLOAD_SLICE_BYTES = 16 * 1024;

export function agentAuthState(hubUrl) {
  if (process.env.AGENT_TOKEN) return { hasAuth: true, source: "AGENT_TOKEN" };
  if (loadStoredAgentToken(hubUrl)) return { hasAuth: true, source: "stored_config" };
  if (process.env.AGENT_SECRET) return { hasAuth: true, source: "legacy_secret" };
  return { hasAuth: false, source: "none" };
}

export function agentIdForController(hubUrl) {
  return ensureStoredAgentId(hubUrl);
}

export function runAgent(
  hubUrl,
  backend,
  { logEvent = () => {}, inventoryProvider = null } = {},
) {
  const wsUrl = toWsUrl(hubUrl); // for logs only; Socket.IO dials hubUrl + path
  const systemHostname = os.hostname();
  const username = os.userInfo().username;
  const machineName = process.env.AGENT_MACHINE || systemHostname;
  const agentId = ensureStoredAgentId(hubUrl);
  const agentRevision = appRevision(process.cwd());
  const agentCwd = process.cwd();
  const configToken = loadStoredAgentToken(hubUrl);
  const tokenSource = process.env.AGENT_TOKEN
    ? "AGENT_TOKEN"
    : configToken
      ? "stored_config"
      : "";
  const storedToken = process.env.AGENT_TOKEN || configToken;
  let stopped = false; // set on auth rejection, replacement, or stop()
  let stopInventoryPublisher = null;
  let transcriptUploadPath = "";
  let activeTranscriptUploadController = null;

  function backendMuxKinds() {
    if (typeof backend.muxKinds === "function") {
      const muxes = backend
        .muxKinds()
        .map((item) => String(item || "").trim().toLowerCase())
        .filter((item) => item === "tmux" || item === "rmux");
      if (muxes.length > 0) return [...new Set(muxes)];
    }
    const muxCommand =
      typeof backend.muxCommand === "function" ? backend.muxCommand() : "tmux";
    const mux =
      typeof backend.muxKind === "function"
        ? backend.muxKind()
        : path.basename(String(muxCommand || "tmux"));
    return [mux === "rmux" ? "rmux" : "tmux"];
  }

  async function describeMuxes() {
    const muxes = [];
    for (const mux of backendMuxKinds()) {
      const muxCommand =
        typeof backend.muxCommand === "function" ? backend.muxCommand(mux) : mux;
      try {
        muxes.push({
          mux,
          kind: mux,
          muxCommand,
          version: (await backend.tmux(["-V"], { mux, timeout: 3000 })).trim(),
        });
      } catch (error) {
        logEvent("agent_mux_unavailable", {
          controller: new URL(hubUrl).origin,
          machine: machineName,
          agentId,
          mux,
          muxCommand,
          message: error.message || String(error),
        });
      }
    }
    return muxes;
  }

  async function describeMux() {
    const muxes = await describeMuxes();
    const primary = muxes[0];
    if (primary) return { ...primary, muxes };
    const muxCommand =
      typeof backend.muxCommand === "function" ? backend.muxCommand() : "tmux";
    const mux =
      typeof backend.muxKind === "function"
        ? backend.muxKind()
        : path.basename(String(muxCommand || "tmux"));
    return { mux, kind: mux, muxCommand, version: "", muxes: [] };
  }

  // Credentials ride in the Socket.IO handshake `auth` payload; the controller's
  // io.use() middleware reads them. Prefer the device token; fall back to the
  // legacy shared secret / user for old setups.
  const auth = {};
  if (storedToken) {
    auth.token = storedToken;
  } else {
    if (process.env.AGENT_USER) auth.user = process.env.AGENT_USER;
    if (process.env.AGENT_SECRET) auth.secret = process.env.AGENT_SECRET;
  }

  const socket = io(hubUrl, {
    path: AGENT_WS_PATH,
    transports: ["websocket"], // Node<->Node; skip the long-polling fallback
    reconnection: true,
    reconnectionDelay: 1_000,
    reconnectionDelayMax: MAX_BACKOFF_MS,
    timeout: CONNECT_TIMEOUT_MS,
    auth,
  });

  // --- Liveness watchdog (detect a silently-dead, still-ESTABLISHED socket) ---
  // `lastServerContactMs` is bumped by any sign of life from the controller. The
  // low-level engine "packet" event fires for EVERY inbound frame including
  // Engine.IO pings, so it's the most reliable heartbeat signal — more so than
  // the app-level "message" handler, which only sees request frames.
  // Revision of the controller instance currently serving this socket, learned
  // from INFO frames. A change means we are pinned to a superseded instance.
  let controllerRevision = "";
  let lastServerContactMs = Date.now();
  let watchdogTimer = null;
  const noteServerContact = () => {
    lastServerContactMs = Date.now();
  };
  const bindEnginePacketListener = () => {
    // socket.io.engine is (re)created per connection attempt; (re)bind on each.
    const engine = socket.io?.engine;
    if (!engine || engine.__livenessBound) return;
    engine.__livenessBound = true;
    engine.on("packet", noteServerContact);
    engine.on("ping", noteServerContact);
    engine.on("pong", noteServerContact);
  };
  const startWatchdog = () => {
    if (watchdogTimer) return;
    watchdogTimer = setInterval(() => {
      if (stopped) return;
      // Only meaningful while we THINK we're connected. If Socket.IO already
      // knows it's disconnected, its own reconnect loop is handling it.
      if (!socket.connected) return;
      const silentMs = Date.now() - lastServerContactMs;
      if (silentMs < TRANSPORT_STALE_MS) return;
      // Connected-but-silent past the threshold: the peer is almost certainly
      // gone (half-open TCP after an instance replacement). Socket.IO won't
      // reconnect a socket it still considers open, so force a clean cycle:
      // disconnect() then connect() re-arms the reconnect machinery.
      logEvent("agent_transport_stale_reconnect", {
        controller: new URL(hubUrl).origin,
        machine: machineName,
        agentId,
        silentMs,
        message:
          "No controller contact past the liveness threshold on an open socket; forcing reconnect (suspected dead half-open connection).",
      });
      lastServerContactMs = Date.now(); // avoid a tight re-fire during the cycle
      try {
        socket.disconnect();
        socket.connect();
      } catch {}
    }, TRANSPORT_WATCHDOG_INTERVAL_MS);
    watchdogTimer.unref?.();
  };
  const stopWatchdog = () => {
    if (watchdogTimer) {
      clearInterval(watchdogTimer);
      watchdogTimer = null;
    }
  };

  const transcriptPublisher =
    typeof inventoryProvider === "function"
      ? createAgentTranscriptPublisher({
          logEvent,
          discoverFiles: discoverTranscriptFiles,
          uploadChunk: (chunk) => uploadTranscriptChunk(chunk),
        })
      : null;

  function sendControlResponse(op, frame) {
    const serialized = JSON.stringify(frame);
    const bytes = Buffer.byteLength(serialized);
    if (bytes >= 64 * 1024) {
      logEvent("agent_large_control_response", {
        controller: new URL(hubUrl).origin,
        machine: machineName,
        agentId,
        op,
        bytes,
        message: "Large agent RPC response is using the control WebSocket.",
      });
    }
    socket.send(serialized);
  }

  async function uploadTranscriptChunk(chunk) {
    if (!transcriptUploadPath) {
      const error = new Error("Controller has no independent transcript upload endpoint");
      error.code = "transcript_http_unavailable";
      throw error;
    }
    const bytes = Buffer.isBuffer(chunk?.bytes)
      ? chunk.bytes
      : Buffer.from(String(chunk?.base64 || ""), "base64");
    if (!bytes.length) throw new Error("Transcript chunk is empty");

    const metadata = { ...chunk };
    delete metadata.bytes;
    delete metadata.base64;
    const headers = {
      "content-type": "application/x-ndjson",
      "content-length": String(bytes.length),
      "x-agent-id": agentId,
      "x-machine-id": machineName,
      "x-transcript-metadata": Buffer.from(JSON.stringify(metadata)).toString("base64url"),
    };
    if (storedToken) headers.authorization = `Bearer ${storedToken}`;
    if (!storedToken && process.env.AGENT_SECRET) {
      headers["x-agent-secret"] = process.env.AGENT_SECRET;
    }
    if (!storedToken && process.env.AGENT_USER) {
      headers["x-agent-user"] = process.env.AGENT_USER;
    }

    const controller = new AbortController();
    activeTranscriptUploadController = controller;
    const minimumTransferMs = Math.ceil(
      (bytes.length / Math.max(1, TRANSCRIPT_UPLOAD_BYTES_PER_SECOND)) * 1000,
    );
    const timeout = setTimeout(
      () => controller.abort(),
      Math.max(TRANSCRIPT_ACK_TIMEOUT_MS, minimumTransferMs + 30_000),
    );
    timeout.unref?.();
    try {
      const response = await fetch(new URL(transcriptUploadPath, hubUrl), {
        method: "POST",
        headers,
        body: rateLimitedBody(bytes, TRANSCRIPT_UPLOAD_BYTES_PER_SECOND),
        duplex: "half",
        signal: controller.signal,
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok || payload.ok !== true) {
        const error = new Error(
          payload.error?.message || payload.error || `Transcript upload failed (${response.status})`,
        );
        error.code = payload.error?.code;
        error.expected = payload.error?.expected;
        throw error;
      }
      return { ack: true, ...(payload.result || {}) };
    } finally {
      clearTimeout(timeout);
      if (activeTranscriptUploadController === controller) {
        activeTranscriptUploadController = null;
      }
    }
  }

  // Deploy-migration poll.
  //
  // A revision change can only be observed on our socket if the OLD instance
  // happens to send us an INFO — and it has no reason to. So ask over plain
  // HTTP: that request is routed by the same frontend the BROWSER uses, so it
  // reaches whatever instance is serving user traffic. If that revision differs
  // from the one answering our WebSocket, we are pinned to a superseded
  // instance and the users' controller does not know this machine exists.
  //
  // (Cloud Run cannot drain the old instance while our WebSocket holds it open,
  // so it keeps running and never gets the SIGTERM that would make
  // hub.shutdown() close our socket for us. Nothing recovers this without a
  // redial — observed 2026-09-02: agents stayed registered on a superseded
  // revision indefinitely while the UI showed no machines.)
  let revisionPollTimer = null;
  const startRevisionPoll = () => {
    if (revisionPollTimer || REVISION_POLL_MS <= 0) return;
    revisionPollTimer = setInterval(async () => {
      if (stopped || !socket.connected || !controllerRevision) return;
      let serving = "";
      try {
        const res = await fetch(new URL("/api/health", hubUrl), {
          signal: AbortSignal.timeout(REVISION_POLL_TIMEOUT_MS),
        });
        if (!res.ok) return; // transient: try again next tick
        serving = String((await res.json())?.revision || "").trim();
      } catch {
        return; // network blip: never redial on an inconclusive probe
      }
      if (!serving || serving === controllerRevision) return;
      logEvent("agent_controller_revision_changed", {
        controller: new URL(hubUrl).origin,
        machine: machineName,
        agentId,
        from: controllerRevision,
        to: serving,
        message:
          "Controller now serving a different revision than this socket; reconnecting so this machine registers with the instance serving traffic.",
      });
      controllerRevision = serving; // don't re-fire while the redial completes
      try {
        socket.disconnect();
        socket.connect();
      } catch {}
    }, REVISION_POLL_MS);
    revisionPollTimer.unref?.();
  };
  const stopRevisionPoll = () => {
    if (revisionPollTimer) {
      clearInterval(revisionPollTimer);
      revisionPollTimer = null;
    }
  };

  socket.on("connect", async () => {
    noteServerContact();
    bindEnginePacketListener();
    startWatchdog();
    startRevisionPoll();
    logEvent("agent_registered", {
      controller: new URL(hubUrl).origin,
      websocket: wsUrl,
      machine: machineName,
      agentId,
      auth: storedToken ? "device_token" : "legacy_secret",
      tokenSource: tokenSource || undefined,
      message: "Agent connected and is registering this machine with the controller.",
    });
    const muxInfo = await describeMux();
    const sshHosts = await detectSshHostCandidates({ systemHostname });
    socket.send(
      JSON.stringify(
        helloFrame({
          agentId,
          machine: machineName,
          systemHostname,
          username,
          sshHosts,
          os: process.platform,
          arch: process.arch,
          tmux: muxInfo.version,
          mux: muxInfo.mux,
          muxCommand: muxInfo.muxCommand,
          muxVersion: muxInfo.version,
          muxes: muxInfo.muxes,
          revision: agentRevision,
          cwd: agentCwd,
          homeDir: os.homedir(),
          node: process.execPath,
        }),
      ),
    );
    if (stopInventoryPublisher) stopInventoryPublisher();
    const observedInventoryProvider = async () => {
      const result = await inventoryProvider();
      await transcriptPublisher?.observeAgents(result?.agents || []);
      return result;
    };
    stopInventoryPublisher = startInventoryPublisher(socket, observedInventoryProvider, {
      hubUrl,
      machineName,
      agentId,
      logEvent,
    });
  });

  // The controller brokers requests as JSON "message" frames; the agent answers
  // each by id. (Kept as message frames rather than per-op events so the wire
  // contract in protocol.mjs is unchanged across the transport swap.)
  socket.on("message", async (raw) => {
    noteServerContact();
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }
    if (msg.t === MSG.INFO) {
      // The controller stamps every INFO with the revision serving this socket.
      // A CHANGE while our socket stays up means this connection is pinned to a
      // superseded instance: Cloud Run starts the new revision but cannot drain
      // the old one while a WebSocket holds it open, so the old instance keeps
      // running (it never even receives SIGTERM, so hub.shutdown()'s io.close()
      // never fires) and keeps answering us. Meanwhile browser traffic goes to
      // the NEW revision, whose in-memory machine registry has never seen us —
      // the machine list shows nothing while this agent is perfectly "connected".
      //
      // Neither the liveness watchdog nor a re-announced HELLO can see this: the
      // peer is alive and chatty, it just isn't the instance serving users. The
      // revision stamp is the only signal that distinguishes the two, which is
      // why the hub sends it (see test/revision.mjs — it must be unique per
      // deploy). Redial: a fresh dial resolves to whatever is serving traffic now.
      const serverRevision = String(msg.revision || "").trim();
      if (serverRevision) {
        if (controllerRevision && serverRevision !== controllerRevision) {
          logEvent("agent_controller_revision_changed", {
            controller: new URL(hubUrl).origin,
            machine: machineName,
            agentId,
            from: controllerRevision,
            to: serverRevision,
            message:
              "Controller revision changed on an open socket; reconnecting so this machine registers with the instance now serving traffic.",
          });
          controllerRevision = serverRevision;
          try {
            socket.disconnect();
            socket.connect();
          } catch {}
          return;
        }
        controllerRevision = serverRevision;
      }
      transcriptUploadPath = String(msg.features?.transcriptUploadPath || "").trim();
      transcriptPublisher?.setEnabled(
        Boolean(msg.features?.transcriptArchive && transcriptUploadPath),
      );
      transcriptPublisher?.setDiscoveryEnabled(
        Boolean(msg.features?.transcriptRootDiscovery),
      );
      return;
    }
    if (msg.t !== MSG.REQ) return;
    try {
      if (msg.op === OP.TMUX) {
        if (!isAllowedTmux(msg.args)) {
          throw new Error(`tmux subcommand not allowed: ${msg.args?.[0]}`);
        }
        const stdout = await backend.tmux(msg.args, {
          ...(msg.options || {}),
          mux: msg.mux || msg.options?.mux,
        });
        sendControlResponse(msg.op, resOk(msg.id, { stdout }));
      } else if (msg.op === OP.READDIR) {
        const entries = await backend.readdir(msg.path);
        sendControlResponse(msg.op, resOk(msg.id, { entries }));
      } else if (msg.op === OP.BRANCH) {
        const info = await backend.branch(msg.path);
        sendControlResponse(msg.op, resOk(msg.id, info));
      } else if (msg.op === OP.READFILE) {
        const result = await backend.readfile(msg.path, {
          baseDir: msg.baseDir,
          maxBytes: msg.maxBytes,
        });
        sendControlResponse(msg.op, resOk(msg.id, result));
      } else if (msg.op === OP.REPO) {
        const info = await backend.repo(msg.path);
        sendControlResponse(msg.op, resOk(msg.id, info));
      } else if (msg.op === OP.PANECMD) {
        const result = await backend.paneCommand(msg.tty);
        sendControlResponse(msg.op, resOk(msg.id, result));
      } else if (msg.op === OP.WRITEFILE) {
        const result = await backend.writeTempFile(msg.name, msg.base64);
        sendControlResponse(msg.op, resOk(msg.id, result));
      } else if (msg.op === OP.PROCESS_TREE) {
        const processes = await backend.processTree(msg.rootPid);
        sendControlResponse(msg.op, resOk(msg.id, { processes }));
      } else if (msg.op === OP.AGENT_LAST_RESPONSE) {
        // The transcript lives on the agent machine — read it here and
        // hand the result back as one round-trip.
        const result = await backend.agentLastResponse({
          rootPid: msg.rootPid,
          cwd: msg.cwd,
        });
        sendControlResponse(msg.op, resOk(msg.id, { result }));
      } else if (msg.op === OP.AGENT_TRANSCRIPT) {
        const result = await backend.agentTranscript({
          rootPid: msg.rootPid,
          cwd: msg.cwd,
          foregroundCommand: msg.foregroundCommand || "",
        });
        sendControlResponse(msg.op, resOk(msg.id, { result }));
      } else if (msg.op === OP.WORKTREE_ADD) {
        const result = await backend.worktreeAdd({
          fromDir: msg.fromDir,
          branch: msg.branch,
        });
        sendControlResponse(msg.op, resOk(msg.id, result));
      } else if (msg.op === OP.RMUX_WEB_SHARE) {
        const result = await backend.rmuxWebShare({
          target: msg.target,
          ttlSeconds: msg.ttlSeconds,
          tunnelProvider: msg.tunnelProvider,
          frontendUrl: msg.frontendUrl,
        });
        sendControlResponse(msg.op, resOk(msg.id, result));
      } else if (msg.op === OP.SSH_AUTHORIZE_KEY) {
        const result = await backend.authorizeSshKey({
          publicKey: msg.publicKey,
          marker: msg.marker,
        });
        sendControlResponse(msg.op, resOk(msg.id, result));
      } else {
        throw new Error(`unknown op: ${msg.op}`);
      }
    } catch (error) {
      sendControlResponse(msg?.op || "unknown", resErr(msg.id, error));
    }
  });

  // Auth rejection from the controller's middleware is PERMANENT — a
  // bad/missing/expired token or a connector too old for this controller's auth.
  // Retrying forever just loops with an opaque error, so stop and tell the user
  // what to do. Any other connect_error is transient: Socket.IO keeps retrying.
  socket.on("connect_error", (error) => {
    if (stopped) return;
    if (error?.data?.code === "auth") {
      stopped = true;
      logEvent("agent_auth_rejected", {
        controller: new URL(hubUrl).origin,
        machine: machineName,
        agentId,
        authSource: tokenSource || "none",
        message: storedToken
          ? `Controller rejected this machine's agent token. It may be expired, for a different account, or this connector is out of date. Re-authenticate: node server.mjs --register ${new URL(hubUrl).origin} --login (and 'git pull' if the connector is old).`
          : `Controller requires a Google login but no agent token is stored. Run: node server.mjs --register ${new URL(hubUrl).origin} --login`,
      });
      socket.io.reconnection(false);
      socket.disconnect();
      transcriptPublisher?.stop();
      logEvent("agent_stopped", {
        controller: new URL(hubUrl).origin,
        machine: machineName,
        agentId,
        message: "Stopped: controller rejected authentication. Re-run with --login.",
      });
      return;
    }
    logEvent("agent_connection_error", {
      controller: new URL(hubUrl).origin,
      machine: machineName,
      agentId,
      message: error?.message || String(error),
    });
  });

  socket.on("disconnect", (reason) => {
    if (stopInventoryPublisher) {
      stopInventoryPublisher();
      stopInventoryPublisher = null;
    }
    // Even on a separate TCP connection, archive backfill still shares this
    // machine's physical uplink. Stop it immediately when control health is
    // lost; the durable pending chunk/cursor makes the next retry identical.
    // This prevents a saturated upload from starving the reconnect itself.
    transcriptPublisher?.setEnabled(false);
    activeTranscriptUploadController?.abort();
    if (stopped || reason === "io client disconnect") return; // our own stop()
    // The controller force-disconnected this socket because a newer connector
    // registered the same machine. Socket.IO will NOT auto-reconnect after an
    // "io server disconnect", which is exactly what we want for a replaced
    // (stale) connector — don't fight the replacement.
    if (reason === "io server disconnect") {
      stopped = true;
      transcriptPublisher?.stop();
      logEvent("agent_stopped", {
        controller: new URL(hubUrl).origin,
        machine: machineName,
        agentId,
        message: "Stopped: another connector registered this machine.",
      });
      return;
    }
    // Any other reason (transport close, ping timeout, transport error):
    // Socket.IO reconnects automatically with backoff.
    logEvent("agent_reconnect_scheduled", {
      controller: new URL(hubUrl).origin,
      machine: machineName,
      agentId,
      reason,
      message: "Agent disconnected; Socket.IO is retrying the controller connection.",
    });
  });

  return {
    stop() {
      stopped = true;
      stopWatchdog();
      stopRevisionPoll();
      if (stopInventoryPublisher) {
        stopInventoryPublisher();
        stopInventoryPublisher = null;
      }
      transcriptPublisher?.stop();
      activeTranscriptUploadController?.abort();
      try {
        socket.io.reconnection(false);
        socket.disconnect();
      } catch {}
    },
  };
}

function rateLimitedBody(bytes, bytesPerSecond) {
  const rate = Math.max(1, Number(bytesPerSecond) || 1);
  async function* chunks() {
    for (let offset = 0; offset < bytes.length; offset += TRANSCRIPT_UPLOAD_SLICE_BYTES) {
      const end = Math.min(bytes.length, offset + TRANSCRIPT_UPLOAD_SLICE_BYTES);
      const startedAt = Date.now();
      yield bytes.subarray(offset, end);
      if (end >= bytes.length) continue;
      const targetMs = Math.ceil(((end - offset) / rate) * 1000);
      const remainingMs = targetMs - (Date.now() - startedAt);
      if (remainingMs > 0) {
        await new Promise((resolve) => setTimeout(resolve, remainingMs));
      }
    }
  }
  return Readable.from(chunks());
}

function envMs(name, fallback) {
  const raw = process.env[name];
  if (raw === undefined) return fallback;
  const value = Number(raw);
  return Number.isFinite(value) ? value : fallback;
}

function startInventoryPublisher(
  ws,
  inventoryProvider,
  { hubUrl, machineName, agentId, logEvent = () => {} } = {},
) {
  if (typeof inventoryProvider !== "function" || INVENTORY_POLL_MS <= 0) return () => {};
  let stopped = false;
  let timer = null;
  let sequence = 0;
  let inFlight = null;

  async function tick() {
    if (stopped || inFlight || !ws.connected) return;
    const startedAt = Date.now();
    const scan = Promise.resolve().then(() => inventoryProvider());
    inFlight = scan;
    let timedOut = false;
    try {
      const result = await withTimeout(scan, INVENTORY_TIMEOUT_MS, "Command Center inventory scan");
      if (stopped || !ws.connected) return;
      const agents = compactInventoryAgents(
        Array.isArray(result?.agents) ? result.agents : [],
        INVENTORY_PREVIEW_MAX_BYTES,
      );
      sendInventory(ws, {
        ok: true,
        sequence: ++sequence,
        observedAt: Date.now(),
        durationMs: Date.now() - startedAt,
        agents,
      });
    } catch (error) {
      timedOut = Boolean(error?.timeout);
      if (!stopped && ws.connected) {
        sendInventory(ws, {
          ok: false,
          sequence: ++sequence,
          observedAt: Date.now(),
          durationMs: Date.now() - startedAt,
          error: { message: error?.message || String(error) },
        });
      }
      if (!timedOut) {
        logEvent("agent_inventory_scan_failed", {
          controller: new URL(hubUrl).origin,
          machine: machineName,
          agentId,
          message: error?.message || String(error),
        });
      }
    } finally {
      if (timedOut) {
        scan
          .finally(() => {
            if (inFlight === scan) inFlight = null;
          })
          .catch(() => {});
      } else if (inFlight === scan) {
        inFlight = null;
      }
    }
  }

  timer = setInterval(() => {
    void tick();
  }, INVENTORY_POLL_MS);
  timer.unref?.();
  void tick();

  return () => {
    stopped = true;
    if (timer) {
      clearInterval(timer);
      timer = null;
    }
  };
}

function sendInventory(ws, info) {
  try {
    // Inventory snapshots are replaceable. Socket.IO volatile delivery drops
    // this poll when the transport is busy instead of building an unbounded
    // queue of obsolete snapshots in front of control RPCs and heartbeats.
    ws.volatile.send(JSON.stringify(inventoryFrame(info)));
  } catch {}
}

function compactInventoryAgents(agents, maxPreviewBytes) {
  return agents.map((agent) => ({
    ...agent,
    lastUserText: truncateUtf8Preview(agent?.lastUserText, maxPreviewBytes),
    lastAssistantText: truncateUtf8Preview(agent?.lastAssistantText, maxPreviewBytes),
  }));
}

function truncateUtf8Preview(value, maxBytes) {
  const text = String(value || "");
  const limit = Math.max(0, Math.floor(Number(maxBytes) || 0));
  if (!text || Buffer.byteLength(text) <= limit) return text;
  if (limit === 0) return "";

  // Binary-search a code-unit boundary, then back away from a dangling high
  // surrogate so the preview always remains valid Unicode and within budget.
  let low = 0;
  let high = text.length;
  while (low < high) {
    const middle = Math.ceil((low + high) / 2);
    if (Buffer.byteLength(text.slice(0, middle)) <= limit) low = middle;
    else high = middle - 1;
  }
  if (low > 0 && /[\uD800-\uDBFF]/.test(text[low - 1])) low -= 1;
  return text.slice(0, low);
}

function withTimeout(promise, timeoutMs, label) {
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) return promise;
  let timer = null;
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      timer = setTimeout(() => {
        const error = new Error(`${label} timed out`);
        error.timeout = true;
        reject(error);
      }, timeoutMs);
    }),
  ]).finally(() => {
    if (timer) clearTimeout(timer);
  });
}

export async function loginAgent(hubUrl, { log = console.log } = {}) {
  const start = await postJson(new URL("/auth/device/start", hubUrl), {});
  log("tmux-mobile agent needs Google device login before it can register this machine.");
  log(`Controller: ${new URL(hubUrl).origin}`);
  log(`Open in a browser: ${start.verificationUrlComplete || start.verificationUrl}`);
  if (!start.verificationUrlComplete) log(`Enter code: ${start.userCode}`);
  log("Waiting for Google authorization...");

  let intervalMs = Math.max(Number(start.interval || 5), 1) * 1000;
  const expiresAt = Date.now() + Math.max(Number(start.expiresIn || 600), 60) * 1000;
  while (Date.now() < expiresAt) {
    await sleep(intervalMs);
    const response = await fetch(new URL("/auth/device/poll", hubUrl), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ id: start.id }),
    });
    const body = await response.json().catch(() => ({}));
    if (response.status === 202) {
      intervalMs = Math.max(Number(body.interval || start.interval || 5), 1) * 1000;
      continue;
    }
    if (!response.ok) {
      throw new Error(body.error || `Device login failed with HTTP ${response.status}`);
    }
    if (!body.token) throw new Error("Device login did not return an agent token");
    const savedPath = await saveStoredAgentToken(hubUrl, body.token, body.user || {});
    log(`Google login complete: ${body.user?.email || "Google user"}.`);
    log(`Agent token saved: ${savedPath}`);
    log("Starting agent registration with the controller...");
    return body;
  }
  throw new Error("Device login expired");
}

function toWsUrl(hubUrl) {
  const url = new URL(hubUrl);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  url.pathname = AGENT_WS_PATH;
  url.search = "";
  return url.toString();
}

async function postJson(url, body) {
  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const json = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(json.error || `HTTP ${response.status}`);
  }
  return json;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function controllerKey(hubUrl) {
  const url = new URL(hubUrl);
  return url.origin;
}

function agentConfigPath() {
  return (
    process.env.TMUX_MOBILE_AGENT_CONFIG ||
    path.join(os.homedir(), ".config", "tmux-mobile", "agent.json")
  );
}

function loadStoredAgentToken(hubUrl) {
  try {
    const config = JSON.parse(
      readFileSync(agentConfigPath(), "utf8") || "{}",
    );
    return config.controllers?.[controllerKey(hubUrl)]?.token || "";
  } catch {
    return "";
  }
}

function ensureStoredAgentId(hubUrl) {
  const envId = normalizeAgentId(process.env.TMUX_MOBILE_AGENT_ID || process.env.AGENT_ID);
  if (envId) return envId;

  const filePath = agentConfigPath();
  let config = {};
  try {
    config = JSON.parse(readFileSync(filePath, "utf8") || "{}");
  } catch {}
  config.controllers ||= {};
  const key = controllerKey(hubUrl);
  config.controllers[key] ||= {};
  const existing = normalizeAgentId(config.controllers[key].agentId);
  if (existing) return existing;

  const agentId = randomUUID();
  config.controllers[key].agentId = agentId;
  mkdirSync(path.dirname(filePath), { recursive: true, mode: 0o700 });
  writeFileSync(filePath, `${JSON.stringify(config, null, 2)}\n`, { mode: 0o600 });
  return agentId;
}

async function readAgentConfig() {
  try {
    return JSON.parse(await readFile(agentConfigPath(), "utf8"));
  } catch {
    return {};
  }
}

async function saveStoredAgentToken(hubUrl, token, user) {
  const filePath = agentConfigPath();
  const config = await readAgentConfig();
  config.controllers ||= {};
  const key = controllerKey(hubUrl);
  config.controllers[key] = {
    ...(config.controllers[key] || {}),
    token,
    user,
    updatedAt: new Date().toISOString(),
  };
  await mkdir(path.dirname(filePath), { recursive: true, mode: 0o700 });
  await writeFile(filePath, `${JSON.stringify(config, null, 2)}\n`, {
    mode: 0o600,
  });
  return filePath;
}

function normalizeAgentId(value) {
  const id = String(value || "").trim().toLowerCase();
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(id)
    ? id
    : "";
}
