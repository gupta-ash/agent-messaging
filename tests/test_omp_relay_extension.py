"""R2 OMP extension lifecycle, model, delivery, and isolation acceptance tests.

These tests execute the TypeScript extension under Bun. The harness uses only disposable roots
and a deterministic process-query seam; production Windows ABI coverage is provided by the real
OMP executable smoke recorded in the R2 evidence.
"""

from __future__ import annotations

import errno
import json
import os
import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
EXTENSION = ROOT / "harness" / "omp" / "omp_relay_extension.ts"
NODE_PATH = Path(__import__("os").environ["BROKER_TEST_OMP_NODE_MODULES"])


HARNESS = r"""
import { appendFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { generateKeyPairSync } from "node:crypto";
import * as path from "node:path";

const scenario = process.env.SCENARIO!;
const stateRoot = process.env.STATE_ROOT!;
const runtimeRoot = process.env.RUNTIME_ROOT!;
mkdirSync(stateRoot, { recursive: true });
mkdirSync(runtimeRoot, { recursive: true });
const aliasCase = (value) => [...value].map(character => /[A-Za-z]/.test(character) ? (character === character.toLowerCase() ? character.toUpperCase() : character.toLowerCase()) : character).join("");
const declaredState = scenario === "case-alias" ? aliasCase(stateRoot) : stateRoot;
const declaredRuntime = scenario === "case-alias" ? aliasCase(runtimeRoot) : runtimeRoot;

const relay = await import(process.env.EXTENSION_URL!);
relay.__testing.reset();
// The roots come from the `omp-relay` entry omp launched the child from, supplied here in place
// of omp's MCP manager; omp's own environment carries no roots.
relay.__testing.setMcpManager({
  getServerConfig(name) {
    return name === "omp-relay"
      ? { command: process.execPath, args: ["omp_relay_mcp.py", "--state-root", declaredState, "--runtime-root", declaredRuntime] }
      : undefined;
  },
  getConnectionStatus(name) { return name === "omp-relay" ? "connected" : "disconnected"; },
  addConnectionStatusListener() { return () => undefined; },
});

const SESSION = "0197f0c2-552b-7db4-8d43-6c338d7d3021";
const SECOND_SESSION = "1197f0c2-552b-7db4-8d43-6c338d7d3021";
const PROCESS_EPOCH = "22222222-2222-4222-8222-222222222222";
let processEpoch = PROCESS_EPOCH;
const INSTANCE = "33333333-3333-4333-8333-333333333333";
const CONTROL_EPOCH = "44444444-4444-4444-8444-444444444444";
const ATTEMPT = "55555555-5555-4555-8555-555555555555";
const BINDING = "66666666-6666-4666-8666-666666666666";
const OFFER = "77777777-7777-4777-8777-777777777777";
const MCP_PID = 901;
const BOOT = 52;
const START_MS = 1780001000000;
const MCP_WITNESS = { pid: MCP_PID, start_key: "9001", sequence_number: "7001", boot_id: BOOT };
const OMP_WITNESS = { pid: process.pid, start_key: "8001", sequence_number: "6001", boot_id: BOOT };
const OLD_EPOCH = "88888888-8888-4888-8888-888888888888";
const OLD_CLAIM = "99999999-9999-4999-8999-999999999999";
const OLD_GATE = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
let ownerMode = "dead";
const sessionFile = path.join(stateRoot, "session.jsonl");
writeFileSync(sessionFile, JSON.stringify({ type: "title", v: 1, title: "", updatedAt: new Date().toISOString(), pad: " " }) + "\n" + JSON.stringify({ type: "session", version: 3, id: SESSION, timestamp: new Date().toISOString(), cwd: stateRoot }) + "\n");

let parentValid = true;
relay.__testing.setProcessQuery((pid) => {
  if (pid === process.pid) return { witness: OMP_WITNESS, parent_pid: 77, start_ms: START_MS, alive: true };
  if (pid === MCP_PID) {
    if (scenario === "native-unknown") throw new Error("NATIVE_WITNESS_UNKNOWN");
    return { witness: MCP_WITNESS, parent_pid: parentValid ? process.pid : process.pid + 1, start_ms: START_MS + 1, alive: true };
  }
  if (pid === 812) {
    if (ownerMode === "unknown") throw new Error("query unavailable");
    return { witness: { pid, start_key: "12", sequence_number: "12", boot_id: BOOT }, parent_pid: 1, start_ms: 12_000, alive: ownerMode === "live" };
  }
  return { witness: { pid, start_key: "1", sequence_number: "1", boot_id: BOOT }, parent_pid: 1, start_ms: 1, alive: false };
});

const handlers = new Map();
const commands = new Map();
const sent = [];
const timers = [];
let activeTools = [
  "mcp__omp_relay_register",
  "mcp__omp_relay_name",
  "mcp__omp_relay_check",
  "mcp__omp_relay_send",
  "read",
];
let tools = [
  "mcp__omp_relay_register",
  "mcp__omp_relay_name",
  "mcp__omp_relay_check",
  "mcp__omp_relay_send",
].map(name => ({ name, sourceInfo: { source: "mcp", mcpServerName: "omp-relay" } }));
let failures = scenario === "failure" ? 1 : 0;
const api = {
  on(name, fn) { const values = handlers.get(name) ?? []; values.push(fn); handlers.set(name, values); },
  registerCommand(name, command) { commands.set(name, command); },
  getActiveTools() { return [...activeTools]; },
  async setActiveTools(values) { activeTools = [...values]; },
  getAllTools() { return [...tools]; },
  async sendUserMessage(text, options) {
    if (failures > 0) { failures -= 1; throw new Error("injection failed"); }
    sent.push({ text, options });
  },
};
const notices = [];
const ctx = {
  agent: { kind: "main" },
  mode: "tui",
  hasUI: true,
  isStreaming: scenario === "core-active",
  sessionManager: {
    getSessionId() { return SESSION; },
    getSessionFile() { return sessionFile; },
    getSessionName() { return "r2-disposable"; },
  },
  setTimeout(fn, ms) { timers.push({ fn, ms }); return timers.length; },
  abort() { throw new Error("aborted"); },
  ui: {
    async confirm() { return true; },
    notify(message, level) { notices.push({ message, level }); },
  },
};
const secondCtx = {
  ...ctx,
  sessionManager: {
    ...ctx.sessionManager,
    getSessionId() { return SECOND_SESSION; },
  },
};
relay.default(api);

async function emit(name, event, context = ctx) {
  const out = [];
  for (const fn of handlers.get(name) ?? []) out.push(await fn(event, context));
  return out;
}
function parse(file) { return JSON.parse(readFileSync(file, "utf8")); }
function canonical(value) {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${canonical(value[key])}`).join(",")}}`;
}
function mirrorPath(eventId) { return path.join(stateRoot, "sessions", SESSION, "offers", `${eventId}.json`); }
function health() { return parse(path.join(runtimeRoot, `mcp-${INSTANCE}`, "health.json")); }
function owner() { return parse(path.join(stateRoot, "sessions", SESSION, "lock", "owner.json")); }
function notification(method, params, context = ctx) { return emit("mcp_notification", { server: "omp-relay", method, params }, context); }
function plantPriorOwner(mode = "dead") {
  ownerMode = mode;
  const file = path.join(stateRoot, "sessions", SESSION, "lock", "owner.json");
  mkdirSync(path.dirname(file), { recursive: true });
  writeFileSync(file, canonical({
    schema_version: 1,
    holder: {
      session_id: SESSION,
      omp_pid: 812,
      omp_process_start_ms: 12000,
      omp_process_epoch: OLD_EPOCH,
      holder_claim_generation: OLD_CLAIM,
      holder_gate_generation: OLD_GATE,
    },
    dead_predecessors: [{
      session_id: SESSION,
      omp_pid: 811,
      omp_process_start_ms: 11000,
      omp_process_epoch: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
    }],
  }) + "\n");
}


async function bind({ validParent = true, instance = INSTANCE, sequence = "7001", context = ctx,
  attempt = ATTEMPT, binding = BINDING, expected = null, beforeBound = null, rendezvous = null } = {}) {
  parentValid = validParent;
  const rdz = rendezvous ?? path.join(runtimeRoot, `mcp-${instance}`);
  if (!rendezvous) {
    mkdirSync(path.join(rdz, "control", "requests"), { recursive: true });
    mkdirSync(path.join(rdz, "control", "results"), { recursive: true });
  }
  const witness = { ...MCP_WITNESS, sequence_number: sequence };
  const request = {
    schema_version: 1,
    type: "identity_request",
    instance_id: instance,
    control_epoch: CONTROL_EPOCH,
    mcp_process: witness,
    rendezvous_dir: rdz,
    bind_attempt_id: attempt,
    new_binding_epoch: binding,
    expected_binding_epoch: expected,
    control_token_hash: "a".repeat(64),
  };
  await notification("notifications/omp/relay/control", request, context);
  const identityPath = path.join(rdz, "identity.json");
  if (!existsSync(identityPath)) return { rdz, request, refusal: null };
  const identity = parse(identityPath);
  if (identity.ok === false) return { rdz, request, refusal: identity };
  processEpoch = identity.omp_process_epoch;
  const authorization = parse(path.join(rdz, "control", "requests", `${attempt}.json`));
  if (beforeBound) await beforeBound();
  await notification("notifications/omp/relay/control", {
    schema_version: 1,
    type: "identity_bound",
    instance_id: instance,
    control_epoch: CONTROL_EPOCH,
    bind_attempt_id: attempt,
    binding_epoch: binding,
    omp_process_epoch: identity.omp_process_epoch,
    session_id: identity.session_id,
    gate_generation: authorization.args.gate_generation,
    claim_generation: authorization.args.claim_generation,
  }, context);
  return { rdz, request, identity, authorization };
}

function delivery(eventId = "9007199254740993") {
  return {
    event_id: eventId,
    client_event_id: "client-r2",
    from_agent: "peer.agent",
    to_agent: "omp.r2",
    channel: "#agents",
    thread_id: null,
    kind: "agent_message",
    content: "hello from relay",
    ts: 1780001000100,
  };
}
function display(eventId = "9007199254740993") { return `[relay] from=peer.agent to=omp.r2 event_id=${eventId} kind=agent_message\n\nhello from relay`; }
function received(eventId = "9007199254740993") {
  return {
    schema_version: 1,
    session_id: SESSION,
    recipient_id: SESSION,
    event_id: eventId,
    offer_token: OFFER,
    omp_process_epoch: processEpoch,
    state: "received",
    broker_terminal_state: "none",
    identity_acknowledged: false,
    health_reason: "NONE",
    reject_reason_code: null,
    application_acceptance_observed: false,
    updated_at_ms: Date.now(),
  };
}
function messageParams(eventId = "9007199254740993") {
  return {
    schema_version: 1,
    instance_id: INSTANCE,
    binding_epoch: BINDING,
    recipient_id: SESSION,
    offer_token: OFFER,
    offer_generation: 1,
    current_relay_name: "omp.r2",
    delivery: delivery(eventId),
    display_text: display(eventId),
  };
}
function plantMirror(eventId = "9007199254740993") {
  mkdirSync(path.dirname(mirrorPath(eventId)), { recursive: true });
  writeFileSync(mirrorPath(eventId), canonical(received(eventId)) + "\n");
}
function appendReceipt(text) {
  appendFileSync(sessionFile, JSON.stringify({ type: "message", id: "msg-1", parentId: null, timestamp: new Date().toISOString(), message: { role: "user", content: [{ type: "text", text }], timestamp: Date.now() } }) + "\n");
}
async function settleAccepted(eventId = "9007199254740993") {
  appendReceipt(sent.at(-1).text);
  await emit("message_end", {});
  const held = await emit("session_before_switch", {});
  await notification("notifications/omp/relay/control", {
    schema_version: 1, type: "offer_accepted", instance_id: INSTANCE, binding_epoch: BINDING,
    recipient_id: SESSION, event_id: eventId, offer_token: OFFER,
  });
  await notification("notifications/omp/relay/control", {
    schema_version: 1, type: "identity_acked", instance_id: INSTANCE, binding_epoch: BINDING,
    recipient_id: SESSION, event_id: eventId, offer_token: OFFER,
  });
  return held;
}

const completedControls = new Set();
async function answerControl(pending, action, ok, code, result, mutate = null) {
  const directory = path.join(runtimeRoot, `mcp-${INSTANCE}`, "control", "requests");
  let request = null;
  const deadline = Date.now() + 2000;
  while (!request && Date.now() < deadline) {
    for (const name of new Bun.Glob("*.json").scanSync({ cwd: directory, onlyFiles: true })) {
      const candidate = parse(path.join(directory, name));
      if (!completedControls.has(candidate.command_id) && candidate.action === action) {
        completedControls.add(candidate.command_id);
        request = candidate;
        break;
      }
    }
    if (!request) await Bun.sleep(5);
  }
  if (!request) throw new Error(`missing ${action} request`);
  const response = {
    schema_version: 1,
    command_id: request.command_id,
    instance_id: request.instance_id,
    control_epoch: request.control_epoch,
    binding_epoch: request.binding_epoch,
    omp_process_epoch: request.omp_process_epoch,
    session_id: request.session_id,
    action,
    ok,
    code,
    result,
  };
  if (mutate) mutate(response);
  writeFileSync(path.join(runtimeRoot, `mcp-${INSTANCE}`, "control", "results", `${request.command_id}.json`), canonical(response) + "\n");
  await pending;
  return request;
}

if (scenario.startsWith("owner-") || scenario.startsWith("recovery-")) {
  plantPriorOwner(scenario === "owner-live" ? "live" : scenario === "owner-unknown" ? "unknown" : "dead");
}
let releaseFifo = null;
if (scenario.startsWith("q-")) {
  const coordinator = relay.__testing.coordinatorFor(SESSION);
  coordinator.fifo_tail = new Promise(resolve => { releaseFifo = resolve; });
}

await emit("session_start", {});
if (scenario === "q-after") {
  releaseFifo();
  await Bun.sleep(5);
}

if (scenario === "core-idle" || scenario === "core-active") {
  const bound = await bind();
  plantMirror();
  await notification("notifications/omp/relay/message", messageParams());
  await notification("notifications/omp/relay/message", messageParams());
  const injected = parse(mirrorPath("9007199254740993"));
  const held = await settleAccepted();
  const accepted = parse(mirrorPath("9007199254740993"));
  console.log(JSON.stringify({
    identity: bound.identity,
    sent,
    injected,
    held,
    accepted,
    health: health(),
    tools: tools.map(value => value.name),
  }));
} else if (scenario === "failure") {
  await bind();
  plantMirror("42");
  const params = messageParams("42");
  params.delivery.event_id = "42";
  await notification("notifications/omp/relay/message", params);
  const afterFailure = parse(mirrorPath("42"));
  const retryHealth = health();
  await notification("notifications/omp/relay/message", params);
  console.log(JSON.stringify({ afterFailure, retryHealth, afterRetry: parse(mirrorPath("42")), sent }));
} else if (scenario === "malformed-isolation") {
  const bound = await bind();
  plantMirror("43");
  const bad = { ...messageParams("43"), extra: true };
  bad.delivery.event_id = "43";
  await notification("notifications/omp/relay/message", bad);
  const malformedHealth = health();
  const sub = { ...ctx, agent: { kind: "sub" } };
  await emit("session_start", {}, sub);
  await notification("notifications/omp/relay/message", messageParams("43"), sub);
  const isolatedTools = [...activeTools];
  const gate = relay.__testing.getGlobalState().gates.get(SESSION);
  gate.claim.ownership = "unknown";
  writeFileSync(path.join(bound.rdz, "health.json"), canonical({
    schema_version: 1, instance_id: INSTANCE, binding_epoch: BINDING,
    state: "connected", code: "CONNECTED", since_ms: Date.now(), retry_in_ms: null,
  }) + "\n");
  tools.push({ name: "mcp__omp_relay_evil", sourceInfo: { source: "mcp", mcpServerName: "omp-relay" } });
  await notification("notifications/tools/list_changed", {}, ctx);
  const privilegeHealth = health();
  const rpcCtx = { ...ctx, mode: "rpc", hasUI: false };
  await commands.get("relay-takeover").handler("stolen.name", rpcCtx);
  const beforeOwner = owner();
  await notification("notifications/omp/relay/control", {
    schema_version: 1, type: "rebind_reconcile", instance_id: INSTANCE, control_epoch: CONTROL_EPOCH,
  });
  await Bun.sleep(5);
  console.log(JSON.stringify({ sent, malformedHealth, isolatedTools, privilegeHealth, notices, commandNames: [...commands.keys()], rebindOwnership: gate.claim.ownership, rebindOwnerUnchanged: canonical(beforeOwner) === canonical(owner()) }));
} else if (scenario === "terminal-reject") {
  await bind();
  plantMirror("46");
  const params = messageParams("46");
  params.delivery.event_id = "46";
  await notification("notifications/omp/relay/message", params);
  const rejection = {
    schema_version: 1, type: "offer_rejected", instance_id: INSTANCE, binding_epoch: BINDING,
    recipient_id: SESSION, event_id: "46", offer_token: OFFER, reject_reason_code: "USER_REJECTED",
  };
  await notification("notifications/omp/relay/control", rejection);
  await notification("notifications/omp/relay/control", rejection);
  console.log(JSON.stringify({ mirror: parse(mirrorPath("46")), sent, health: health() }));
} else if (new Set(["terminal-recovery-accepted", "terminal-recovery-rejected", "terminal-recovery-accepted-acked"]).has(scenario)) {
  await bind();
  mkdirSync(path.dirname(mirrorPath("47")), { recursive: true });
  writeFileSync(mirrorPath("47"), "{malformed\n");
  const terminal = scenario.includes("accepted") ? "accepted" : "rejected";
  await notification("notifications/omp/relay/control", {
    schema_version: 1, type: "offer_terminal_recovery", instance_id: INSTANCE, binding_epoch: BINDING,
    recipient_id: SESSION, event_id: "47", offer_token: OFFER, terminal_state: terminal,
    reject_reason_code: terminal === "rejected" ? "USER_REJECTED" : null, recovery_source: "malformed_mirror",
  });
  let reopened: string | null = null;
  let reopenedHealth: any = null;
  let laterMirror: any = null;
  if (scenario === "terminal-recovery-accepted-acked") {
    await notification("notifications/omp/relay/control", {
      schema_version: 1, type: "identity_acked", instance_id: INSTANCE, binding_epoch: BINDING,
      recipient_id: SESSION, event_id: "47", offer_token: OFFER,
    });
    reopened = relay.__testing.getGlobalState().gates.get(SESSION).state;
    reopenedHealth = health();
    await notification("notifications/omp/relay/message", messageParams("48"));
    laterMirror = parse(mirrorPath("48"));
  }
  console.log(JSON.stringify({ mirror: parse(mirrorPath("47")), sent, health: health(), reopened, reopenedHealth, laterMirror }));
} else if (new Set(["queued-ack-barrier", "queued-reject-barrier"]).has(scenario)) {
  const rejectionBarrier = scenario === "queued-reject-barrier";
  await bind({ beforeBound: async () => {
    mkdirSync(path.dirname(mirrorPath("47")), { recursive: true });
    writeFileSync(mirrorPath("47"), canonical({
      ...received("47"),
      state: rejectionBarrier ? "injected" : "accepted",
      broker_terminal_state: rejectionBarrier ? "none" : "accepted",
      application_acceptance_observed: !rejectionBarrier,
    }) + "\n");
  }});
  const blocked = relay.__testing.getGlobalState().gates.get(SESSION).state;
  await notification("notifications/omp/relay/message", messageParams("48"));
  const queuedBeforeTerminal = existsSync(mirrorPath("48"));
  const blockedHealth = health();
  await notification("notifications/omp/relay/control", rejectionBarrier ? {
    schema_version: 1, type: "offer_rejected", instance_id: INSTANCE, binding_epoch: BINDING,
    recipient_id: SESSION, event_id: "47", offer_token: OFFER, reject_reason_code: "USER_REJECTED",
  } : {
    schema_version: 1, type: "identity_acked", instance_id: INSTANCE, binding_epoch: BINDING,
    recipient_id: SESSION, event_id: "47", offer_token: OFFER,
  });
  const reopened = relay.__testing.getGlobalState().gates.get(SESSION).state;
  const priorMirror = parse(mirrorPath("47"));
  await notification("notifications/omp/relay/message", messageParams("48"));
  console.log(JSON.stringify({ blocked, blockedHealth, queuedBeforeTerminal, reopened, priorMirror, laterMirror: parse(mirrorPath("48")), sent }));
} else if (scenario === "owner-dead" || scenario === "owner-live" || scenario === "owner-unknown") {
  await Bun.sleep(10);
  const gate = relay.__testing.getGlobalState().gates.get(SESSION);
  console.log(JSON.stringify({ owner: owner(), ownership: gate.claim.ownership, lineage: gate.claim.transfer_source?.claim_generation ?? null }));
} else if (scenario === "q-before" || scenario === "q-during" || scenario === "q-after") {
  const coordinator = relay.__testing.coordinatorFor(SESSION);
  const firstGate = relay.__testing.getGlobalState().gates.get(SESSION);
  const before = await emit("session_before_switch", {});
  if (scenario === "q-during") {
    releaseFifo();
    await Bun.sleep(5);
  }
  await commands.get("relay-unquiesce").handler("", ctx);
  if (scenario === "q-before") releaseFifo();
  await Bun.sleep(15);
  const finalGate = relay.__testing.getGlobalState().gates.get(SESSION);
  console.log(JSON.stringify({
    before,
    sameCoordinator: coordinator === relay.__testing.coordinatorFor(SESSION),
    queueGeneration: coordinator.queue_generation,
    firstLifecycle: firstGate.claim.lifecycle,
    finalOwnership: finalGate.claim.ownership,
    finalOwner: owner().holder,
    finalExpected: finalGate.claim.expected_owner,
    demandedFirst: relay.__testing.isClaimDemanded(firstGate.claim),
    claims: coordinator.claims.size,
  }));
} else if (scenario === "recovery-send" || scenario === "recovery-receipt" || scenario === "recovery-null") {
  await Bun.sleep(10);
  await bind();
  const eventId = "45";
  const oldMirror = received(eventId);
  oldMirror.omp_process_epoch = OLD_EPOCH;
  mkdirSync(path.dirname(mirrorPath(eventId)), { recursive: true });
  writeFileSync(mirrorPath(eventId), canonical(oldMirror) + "\n");
  if (scenario === "recovery-receipt") appendReceipt(`${display(eventId)}\n\n[omp-relay-accept:v1 event_id=${eventId} token=${OFFER} generation=1]`);
  const status = {
    recipient_id: SESSION, event_id: eventId, offer_token: OFFER, offer_generation: 1,
    binding_epoch: BINDING, omp_process_epoch: processEpoch, offer_state: "offered",
    delivery_status: "outstanding", reject_reason_code: null,
  };
  await notification("notifications/omp/relay/control", {
    schema_version: 1, type: "offer_recovery_status", instance_id: INSTANCE, control_epoch: CONTROL_EPOCH,
    binding_epoch: BINDING, omp_process_epoch: processEpoch, status,
    delivery: scenario === "recovery-null" ? null : delivery(eventId),
  });
  await Bun.sleep(15);
  await notification("notifications/omp/relay/control", {
    schema_version: 1, type: "offer_recovery_status", instance_id: INSTANCE, control_epoch: CONTROL_EPOCH,
    binding_epoch: BINDING, omp_process_epoch: processEpoch, status,
    delivery: scenario === "recovery-null" ? null : delivery(eventId),
  });
  await Bun.sleep(10);
  console.log(JSON.stringify({ mirror: parse(mirrorPath(eventId)), sent, owner: owner() }));
} else if (scenario === "lifecycle") {
  const bound = await bind();
  plantMirror("44");
  const params = messageParams("44");
  params.delivery.event_id = "44";
  await notification("notifications/omp/relay/message", params);
  const held = await emit("session_before_branch", {});
  await emit("session_start", {});
  const ownerAfterReload = owner();
  await emit("session_shutdown", {});
  const ownerAfterStop = owner();
  const rendezvousRemoved = !existsSync(bound.rdz);
  relay.default(api);
  await emit("session_start", {});
  const rebound = await bind({
    attempt: "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
    binding: "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
  });
  console.log(JSON.stringify({
    held,
    ownerAfterReload,
    ownerAfterStop,
    rendezvousRemoved,
    mirrorRetained: existsSync(mirrorPath("44")),
    stoppingAfterReload: relay.__testing.getGlobalState().stopping,
    reboundIdentity: rebound.identity?.session_id ?? null,
  }));
} else if (scenario === "stale-bind-completion") {
  const bound = await bind({ beforeBound: async () => { await emit("session_before_switch", {}); } });
  const gate = relay.__testing.getGlobalState().gates.get(SESSION);
  console.log(JSON.stringify({ identity: bound.identity.session_id, binding: gate.binding, state: gate.state, health: health() }));
} else if (scenario === "user-controls") {
  await bind();
  const takeover = commands.get("relay-takeover").handler("claimed.name", ctx);
  const takeoverRequest = await answerControl(takeover, "TAKEOVER_NAME", true, "NAME_CLAIMED", { relay_name: "claimed.name", displaced_id: null });
  plantMirror("50");
  const params = messageParams("50");
  params.delivery.event_id = "50";
  await notification("notifications/omp/relay/message", params);
  const reject = commands.get("relay-reject").handler("50", ctx);
  const rejectRequest = await answerControl(reject, "REJECT_DELIVERY", true, "REJECTED", {
    event_id: "50", offer_state: "rejected", delivery_status: "rejected", reject_reason_code: "USER_REJECTED",
  });
  const invalid = commands.get("relay-takeover").handler("other.name", ctx);
  await answerControl(invalid, "TAKEOVER_NAME", true, "NAME_CLAIMED", { relay_name: "other.name", displaced_id: null }, response => {
    response.binding_epoch = "abababab-abab-4bab-8bab-abababababab";
  });
  console.log(JSON.stringify({ takeoverRequest, rejectRequest, notices }));
} else if (scenario === "terminal-acked-adoption") {
  await Bun.sleep(10);
  await bind();
  const event = "50";
  const oldMirror = received(event);
  oldMirror.omp_process_epoch = OLD_EPOCH;
  oldMirror.state = "accepted";
  oldMirror.broker_terminal_state = "accepted";
  oldMirror.identity_acknowledged = true;
  oldMirror.application_acceptance_observed = true;
  mkdirSync(path.dirname(mirrorPath(event)), { recursive: true });
  writeFileSync(mirrorPath(event), canonical(oldMirror) + "\n");
  const status = {
    recipient_id: SESSION, event_id: event, offer_token: OFFER, offer_generation: 1,
    binding_epoch: BINDING, omp_process_epoch: processEpoch, offer_state: "accepted",
    delivery_status: "acked", reject_reason_code: null,
  };
  await notification("notifications/omp/relay/control", {
    schema_version: 1, type: "offer_recovery_status", instance_id: INSTANCE, control_epoch: CONTROL_EPOCH,
    binding_epoch: BINDING, omp_process_epoch: processEpoch, status, delivery: null,
  });
  await Bun.sleep(10);
  const entry = relay.__testing.getGlobalState().entries.get(`${SESSION}:${event}`);
  const gate = relay.__testing.getGlobalState().gates.get(SESSION);
  console.log(JSON.stringify({ mirror: parse(mirrorPath(event)), currentEpoch: processEpoch, state: gate.state, health: health(), unresolved: entry.unresolved }));
} else if (scenario === "adoption-ambiguous") {
  await bind();
  const event = "49";
  const oldMirror = received(event);
  oldMirror.omp_process_epoch = OLD_EPOCH;
  mkdirSync(path.dirname(mirrorPath(event)), { recursive: true });
  writeFileSync(mirrorPath(event), canonical(oldMirror) + "\n");
  relay.__testing.getGlobalState().entries.set(`${SESSION}:${event}`, {
    key: `${SESSION}:${event}`, session_id: SESSION, event_id: event, offer_token: OFFER,
    omp_process_epoch: processEpoch, delivery: null, display_text: null, chain: Promise.resolve(),
    unresolved: true, terminal: false, send_right: false, send_consumed: false,
    pending_adoption: false, adoption_ambiguous: true,
  });
  const status = {
    recipient_id: SESSION, event_id: event, offer_token: OFFER, offer_generation: 1,
    binding_epoch: BINDING, omp_process_epoch: processEpoch, offer_state: "offered",
    delivery_status: "outstanding", reject_reason_code: null,
  };
  await notification("notifications/omp/relay/control", {
    schema_version: 1, type: "offer_recovery_status", instance_id: INSTANCE, control_epoch: CONTROL_EPOCH,
    binding_epoch: BINDING, omp_process_epoch: processEpoch, status, delivery: delivery(event),
  });
  const params = messageParams(event);
  params.delivery.event_id = event;
  await notification("notifications/omp/relay/message", params);
  console.log(JSON.stringify({ sent, health: health(), entry: relay.__testing.getGlobalState().entries.get(`${SESSION}:${event}`) }));
} else if (scenario === "duplicate-json") {
  await bind();
  mkdirSync(path.dirname(mirrorPath("48")), { recursive: true });
  const duplicate = canonical(received("48")).replace('"state":"received"', '"state":"received","state":"received"');
  writeFileSync(mirrorPath("48"), duplicate + "\n");
  const params = messageParams("48");
  params.delivery.event_id = "48";
  await notification("notifications/omp/relay/message", params);
  console.log(JSON.stringify({ sent, health: health() }));
} else if (scenario === "q-retired") {
  const oldGate = relay.__testing.getGlobalState().gates.get(SESSION);
  const before = await emit("session_before_switch", {});
  await emit("session_switch", {}, secondCtx);
  releaseFifo();
  await Bun.sleep(20);
  console.log(JSON.stringify({ before, pending: oldGate.claim.pending, pins: oldGate.claim.operation_pins, lifecycle: oldGate.claim.lifecycle }));
} else if (scenario === "same-session-tree") {
  await bind();
  const oldGate = relay.__testing.getGlobalState().gates.get(SESSION);
  const before = await emit("session_before_tree", {});
  await emit("session_tree", {}, ctx);
  await Bun.sleep(10);
  const fresh = relay.__testing.getGlobalState().gates.get(SESSION);
  const rebound = await bind({
    context: ctx,
    attempt: "ffffffff-ffff-4fff-8fff-ffffffffffff",
    binding: "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
  });
  console.log(JSON.stringify({
    before,
    identity: rebound.identity?.session_id ?? null,
    oldLifecycle: oldGate.claim.lifecycle,
    source: fresh.claim.transfer_source?.claim_generation ?? null,
    ownership: fresh.claim.ownership,
    holderClaim: owner().holder?.holder_claim_generation ?? null,
    freshClaim: fresh.claim.claim_generation,
  }));
} else if (scenario.startsWith("transition-")) {
  await bind();
  const kind = scenario.slice("transition-".length);
  const before = await emit(`session_before_${kind}`, {});
  await emit(`session_${kind}`, {}, secondCtx);
  await Bun.sleep(10);
  await notification("notifications/omp/relay/control", {
    schema_version: 1, type: "rebind_reconcile", instance_id: INSTANCE, control_epoch: CONTROL_EPOCH,
  }, secondCtx);
  await Bun.sleep(10);
  const binding = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee";
  const rebound = await bind({
    context: secondCtx,
    attempt: "ffffffff-ffff-4fff-8fff-ffffffffffff",
    binding,
  });
  const gate = relay.__testing.getGlobalState().gates.get(SECOND_SESSION);
  console.log(JSON.stringify({ before, identity: rebound.identity?.session_id ?? null, state: gate.state, ownership: gate.claim.ownership, binding: gate.binding?.binding_epoch ?? null }));
} else if (scenario === "case-alias") {
  const bound = await bind();
  const global = relay.__testing.getGlobalState();
  console.log(JSON.stringify({ stateRoot: global.state_root, runtimeRoot: global.runtime_root, identity: bound.identity?.session_id ?? null, gateState: global.gates.get(SESSION)?.state ?? null }));
} else if (scenario === "no-gate-transition") {
  const noGateCtx = {
    ...ctx,
    mode: "json",
    sessionManager: {
      ...ctx.sessionManager,
      getSessionId() { return SECOND_SESSION; },
    },
  };
  const beforeSwitch = (await emit("session_before_switch", {}, noGateCtx))[0] ?? null;
  const beforeBranch = (await emit("session_before_branch", {}, noGateCtx))[0] ?? null;
  const beforeTree = (await emit("session_before_tree", {}, noGateCtx))[0] ?? null;
  console.log(JSON.stringify({ beforeSwitch, beforeBranch, beforeTree }));
} else if (scenario === "mode-unsupported") {
  const unsupported = { ...ctx, mode: "json" };
  const result = await bind({ context: unsupported });
  console.log(JSON.stringify({ refusal: result.refusal, sent }));
} else if (scenario === "native-guard" || scenario === "native-unknown") {
  const result = await bind({ validParent: scenario !== "native-guard" });
  console.log(JSON.stringify({ refusal: result.refusal, sent }));
} else if (scenario === "rendezvous-alias") {
  const result = await bind({ rendezvous: process.env.RENDEZVOUS_ALIAS! });
  if (result.identity) {
    plantMirror();
    await notification("notifications/omp/relay/message", messageParams());
  }
  console.log(JSON.stringify({
    refusal: result.refusal ?? null,
    identity: result.identity?.session_id ?? null,
    identityExists: existsSync(path.join(process.env.RENDEZVOUS_REAL!, "identity.json")),
    sent,
  }));
} else {
  throw new Error(`unknown scenario ${scenario}`);
}
"""


def _run_scenario(tmp_path: Path, scenario: str, extra_env: dict[str, str] | None = None) -> dict:
    state = tmp_path / "state"
    runtime = tmp_path / "runtime"
    state.mkdir(exist_ok=True)
    runtime.mkdir(exist_ok=True)
    harness = tmp_path / "harness.ts"
    harness.write_text(HARNESS, encoding="utf-8")
    env = os.environ.copy()
    env.update(
        SCENARIO=scenario,
        STATE_ROOT=str(state.resolve()),
        RUNTIME_ROOT=str(runtime.resolve()),
        EXTENSION_URL=EXTENSION.resolve().as_uri(),
        NODE_PATH=str(NODE_PATH),
    )
    if extra_env:
        env.update(extra_env)
    completed = subprocess.run(
        ["bun", str(harness)],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )
    assert completed.returncode == 0, f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    assert lines, completed.stderr
    return json.loads(lines[-1])

ROOT_INITIALIZER = r"""
const relay = await import(process.env.EXTENSION_URL);
relay.__testing.reset();
const declaration = { command: process.execPath, args: ["omp_relay_mcp.py", "--state-root", process.env.DECLARED_STATE_ROOT, "--runtime-root", process.env.DECLARED_RUNTIME_ROOT] };
relay.__testing.setMcpManager({
  getServerConfig(name) { return name === "omp-relay" ? declaration : undefined; },
  getConnectionStatus() { return "connected"; },
  addConnectionStatusListener() { return () => undefined; },
});
relay.__testing.setProcessQuery((pid) => ({ witness: { pid, start_key: "1", sequence_number: "1", boot_id: 1 }, parent_pid: 1, start_ms: 1, alive: true }));
const handlers = new Map();
const api = {
  on(name, fn) { const values = handlers.get(name) ?? []; values.push(fn); handlers.set(name, values); },
  registerCommand() {},
  getActiveTools() { return []; },
  async setActiveTools() {},
  getAllTools() { return []; },
};
relay.default(api);
const ctx = {
  agent: { kind: "main" },
  mode: "rpc",
  hasUI: false,
  sessionManager: { getSessionId() { return "0197f0c2-552b-7db4-8d43-6c338d7d3021"; } },
  setTimeout() { return 0; },
  ui: {},
};
for (const fn of handlers.get("session_start") ?? []) await fn({}, ctx);
const state = relay.__testing.getGlobalState();
console.log(JSON.stringify({ stateRoot: state.state_root, runtimeRoot: state.runtime_root, handlerCount: [...handlers.values()].flat().length }));
"""


def _run_root_initializer(state_root: Path, runtime_root: Path) -> dict:
    harness = ROOT_INITIALIZER
    env = os.environ.copy()
    env.pop("OMP_RELAY_STATE_ROOT", None)
    env.pop("OMP_RELAY_RUNTIME_ROOT", None)
    env.update(
        DECLARED_STATE_ROOT=str(state_root),
        DECLARED_RUNTIME_ROOT=str(runtime_root),
        EXTENSION_URL=EXTENSION.resolve().as_uri(),
        NODE_PATH=str(NODE_PATH),
    )
    completed = subprocess.run(
        ["bun", "-e", harness],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    assert lines, completed.stderr
    return json.loads(lines[-1])


def _native_directory_symlink(target: Path, link: Path) -> None:
    try:
        os.symlink(target, link, target_is_directory=True)
    except NotImplementedError as exc:
        pytest.skip(f"physical directory symlink unavailable: {exc}")
    except OSError as exc:
        if getattr(exc, "winerror", None) == 1314 or exc.errno in (
            errno.ENOSYS,
            errno.EOPNOTSUPP,
            errno.ENOTSUP,
        ):
            pytest.skip(f"physical directory symlink unavailable: {exc}")
        raise


def _native_directory_aliases(tmp_path: Path) -> tuple[Path, Path]:
    target = tmp_path / "target"
    target.mkdir()
    junction = tmp_path / "junction"
    linked = subprocess.run(
        ["cmd.exe", "/d", "/c", "mklink", "/J", str(junction), str(target)],
        text=True,
        capture_output=True,
        check=False,
    )
    assert linked.returncode == 0, f"native junction unavailable: {linked.stderr or linked.stdout}"
    nested = target / "nested"
    nested.mkdir()
    real = tmp_path / "real"
    real.mkdir()
    hidden = junction / "nested" / ".." / ".." / "real"
    return junction, hidden


@pytest.mark.parametrize("scenario", ["core-idle", "core-active"])
def test_one_raw_notification_maps_to_one_attributed_message_and_explicit_receipt(tmp_path, scenario):
    result = _run_scenario(tmp_path, scenario)
    assert result["identity"]["session_id"] == "0197f0c2-552b-7db4-8d43-6c338d7d3021"
    assert len(result["sent"]) == 1
    assert result["sent"][0]["text"].startswith("[relay] from=peer.agent to=omp.r2 event_id=9007199254740993 kind=agent_message")
    assert result["sent"][0]["options"] == {"deliverAs": "aside", "attribution": "agent"}
    assert result["injected"]["event_id"] == "9007199254740993"
    assert result["injected"]["state"] == "injected"
    assert result["held"] == [{"cancel": True}]
    assert result["accepted"]["state"] == "accepted"
    assert result["accepted"]["broker_terminal_state"] == "accepted"
    assert result["accepted"]["identity_acknowledged"] is True
    assert result["health"]["state"] == "connected"
    assert result["tools"] == [
        "mcp__omp_relay_register",
        "mcp__omp_relay_name",
        "mcp__omp_relay_check",
        "mcp__omp_relay_send",
    ]


def test_injection_failure_keeps_offer_received_and_redeliverable(tmp_path):
    result = _run_scenario(tmp_path, "failure")
    assert result["afterFailure"]["state"] == "received"
    assert result["retryHealth"]["code"] == "DELIVERY_RETRY_PENDING"
    assert result["afterRetry"]["state"] == "injected"
    assert len(result["sent"]) == 1


def test_malformed_privilege_and_subagent_paths_refuse_without_parent_delivery(tmp_path):
    result = _run_scenario(tmp_path, "malformed-isolation")
    assert result["sent"] == []
    assert result["malformedHealth"]["code"] == "PROTOCOL_INVALID"
    assert not any(name.startswith("mcp__omp_relay_") for name in result["isolatedTools"])
    assert result["privilegeHealth"]["code"] == "PROTOCOL_INVALID"
    assert result["notices"][-1]["message"] == "RELAY_COMMAND_REFUSED"
    assert result["commandNames"] == ["relay-takeover", "relay-reject", "relay-unquiesce"]
    assert result["rebindOwnership"] == "self"
    assert result["rebindOwnerUnchanged"] is True


def test_held_event_barrier_reload_identity_and_shutdown_cleanup(tmp_path):
    result = _run_scenario(tmp_path, "lifecycle")
    assert result["held"] == [{"cancel": True}]
    assert result["ownerAfterReload"]["holder"] is not None
    assert result["ownerAfterStop"]["holder"] == result["ownerAfterReload"]["holder"]
    assert result["ownerAfterStop"]["dead_predecessors"] == result["ownerAfterReload"]["dead_predecessors"]
    assert result["rendezvousRemoved"] is True
    assert result["mirrorRetained"] is True
    assert result["stoppingAfterReload"] is False
    assert result["reboundIdentity"] == "0197f0c2-552b-7db4-8d43-6c338d7d3021"


def test_stale_bind_completion_cannot_publish_into_quiescing_gate(tmp_path):
    result = _run_scenario(tmp_path, "stale-bind-completion")
    assert result["identity"] == "0197f0c2-552b-7db4-8d43-6c338d7d3021"
    assert result["binding"] is None
    assert result["state"] == "quiescing"
    assert result["health"]["code"] == "BINDING_CONFLICT"


def test_privileged_commands_route_exact_actions_and_validate_private_results(tmp_path):
    result = _run_scenario(tmp_path, "user-controls")
    assert result["takeoverRequest"]["action"] == "TAKEOVER_NAME"
    assert result["takeoverRequest"]["args"] == {"name": "claimed.name"}
    assert result["rejectRequest"]["action"] == "REJECT_DELIVERY"
    assert result["rejectRequest"]["args"] == {
        "event_id": "50",
        "offer_token": "77777777-7777-4777-8777-777777777777",
        "reason_code": "USER_REJECTED",
    }
    assert result["notices"][-3:] == [
        {"message": "NAME_CLAIMED", "level": "info"},
        {"message": "REJECTED", "level": "info"},
        {"message": "CONTROL_CHANNEL_INVALID", "level": "error"},
    ]


def test_terminal_acked_adoption_needs_no_death_witness_and_reopens(tmp_path):
    result = _run_scenario(tmp_path, "terminal-acked-adoption")
    assert result["mirror"]["state"] == "accepted"
    assert result["mirror"]["broker_terminal_state"] == "accepted"
    assert result["mirror"]["identity_acknowledged"] is True
    assert result["mirror"]["omp_process_epoch"] == result["currentEpoch"]
    assert result["unresolved"] is False
    assert result["state"] == "open"
    assert result["health"]["code"] == "CONNECTED"


def test_ambiguous_adoption_status_and_message_paths_never_regenerate_send_right(tmp_path):
    result = _run_scenario(tmp_path, "adoption-ambiguous")
    assert result["sent"] == []
    assert result["health"]["code"] == "PERSISTENCE_STALLED"
    assert result["entry"]["adoption_ambiguous"] is True
    assert result["entry"]["send_right"] is False
    assert result["entry"]["send_consumed"] is False


def test_duplicate_json_member_is_refused_before_delivery(tmp_path):
    result = _run_scenario(tmp_path, "duplicate-json")
    assert result["sent"] == []
    assert result["health"]["code"] == "DURABILITY_MISMATCH"


def test_queued_retired_acquire_releases_pins_and_owner_authority(tmp_path):
    result = _run_scenario(tmp_path, "q-retired")
    assert result["before"] == [None]
    assert result["pending"] == "idle"
    assert result["pins"] == 0
    assert result["lifecycle"] == "released"


@pytest.mark.parametrize("scenario", ["transition-switch", "transition-branch", "transition-tree"])
def test_transition_rebind_installs_only_current_session_binding(tmp_path, scenario):
    result = _run_scenario(tmp_path, scenario)
    assert result["before"] == [None]
    assert result["identity"] == "1197f0c2-552b-7db4-8d43-6c338d7d3021"
    assert result["ownership"] == "self"
    assert result["state"] == "open"
    assert result["binding"] == "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"


def test_same_session_tree_transfers_exact_claim_without_unowned_gap(tmp_path):
    result = _run_scenario(tmp_path, "same-session-tree")
    assert result["before"] == [None]
    assert result["identity"] == "0197f0c2-552b-7db4-8d43-6c338d7d3021"
    assert result["oldLifecycle"] == "superseded"
    assert result["source"] is None
    assert result["ownership"] == "self"
    assert result["holderClaim"] == result["freshClaim"]


@pytest.mark.skipif(os.name != "nt", reason="Windows canonical-case contract")
def test_case_alias_roots_bind_with_exact_canonical_paths(tmp_path):
    result = _run_scenario(tmp_path, "case-alias")
    assert str(result["stateRoot"]) == str((tmp_path / "state").resolve(strict=True))
    assert str(result["runtimeRoot"]) == str((tmp_path / "runtime").resolve(strict=True))
    assert result["identity"] == "0197f0c2-552b-7db4-8d43-6c338d7d3021"
    assert result["gateState"] == "open"

@pytest.mark.skipif(os.name != "nt", reason="native Windows reparse contract")
def test_native_case_sensitive_and_reparse_roots_fail_closed(tmp_path):
    sensitive = tmp_path / "sensitive"
    sensitive.mkdir()
    enabled = subprocess.run(
        ["fsutil.exe", "file", "SetCaseSensitiveInfo", str(sensitive), "enable"],
        text=True,
        capture_output=True,
        check=False,
    )
    assert enabled.returncode == 0, f"native case-sensitive directory unavailable: {enabled.stderr or enabled.stdout}"
    upper = sensitive / "Root"
    lower = sensitive / "root"
    upper.mkdir()
    lower.mkdir()
    accepted = _run_root_initializer(upper, lower)
    upper_resolved = str(upper.resolve(strict=True))
    lower_resolved = str(lower.resolve(strict=True))
    assert accepted["stateRoot"] == upper_resolved
    assert accepted["runtimeRoot"] == lower_resolved
    assert upper_resolved != lower_resolved
    assert accepted["handlerCount"] > 0

    absent = sensitive / "ROOT"
    absent_result = _run_root_initializer(absent, lower)
    assert absent_result["stateRoot"] is None
    assert absent_result["runtimeRoot"] is None
    assert absent_result["handlerCount"] > 0

    safe_state = tmp_path / "safe-state"
    safe_runtime = tmp_path / "safe-runtime"
    safe_state.mkdir()
    safe_runtime.mkdir()
    junction, hidden = _native_directory_aliases(tmp_path)
    for candidate, field, other in (
        (junction, "stateRoot", safe_runtime),
        (hidden, "stateRoot", safe_runtime),
        (junction, "runtimeRoot", safe_state),
        (hidden, "runtimeRoot", safe_state),
    ):
        initialized = _run_root_initializer(candidate if field == "stateRoot" else other, other if field == "stateRoot" else candidate)
        assert initialized["stateRoot"] is None
        assert initialized["runtimeRoot"] is None
        assert initialized["handlerCount"] > 0


@pytest.mark.skipif(os.name != "nt", reason="native Windows directory symlink contract")
@pytest.mark.parametrize("field", ["stateRoot", "runtimeRoot"])
def test_native_directory_symlink_roots_fail_closed(tmp_path, field):
    target = tmp_path / "target"
    target.mkdir()
    symlink = tmp_path / "symlink"
    _native_directory_symlink(target, symlink)
    other = tmp_path / "safe-root"
    other.mkdir()
    initialized = _run_root_initializer(
        symlink if field == "stateRoot" else other,
        other if field == "stateRoot" else symlink,
    )
    assert initialized["stateRoot"] is None
    assert initialized["runtimeRoot"] is None
    assert initialized["handlerCount"] > 0


@pytest.mark.skipif(os.name != "nt", reason="native Windows rendezvous reparse contract")
def test_rendezvous_lexical_alias_refuses_without_bind_or_send(tmp_path):
    scenario_root = tmp_path / "scenario"
    runtime = scenario_root / "runtime"
    runtime.mkdir(parents=True)
    nested = runtime / "nested"
    nested.mkdir()
    alias = runtime / "alias"
    _native_directory_symlink(nested, alias)
    instance = "33333333-3333-4333-8333-333333333333"
    real = runtime / f"mcp-{instance}"
    (real / "control" / "requests").mkdir(parents=True)
    (real / "control" / "results").mkdir(parents=True)
    candidate = alias / ".." / f"mcp-{instance}"
    result = _run_scenario(
        scenario_root,
        "rendezvous-alias",
        {"RENDEZVOUS_ALIAS": str(candidate), "RENDEZVOUS_REAL": str(real)},
    )
    assert result["refusal"] is None
    assert result["identity"] is None
    assert result["identityExists"] is False
    assert result["sent"] == []


def test_rendezvous_nonlink_lexical_traversal_binds_and_sends(tmp_path):
    scenario_root = tmp_path / "scenario"
    runtime = scenario_root / "runtime"
    nested = runtime / "nested"
    nested.mkdir(parents=True)
    instance = "33333333-3333-4333-8333-333333333333"
    real = runtime / f"mcp-{instance}"
    (real / "control" / "requests").mkdir(parents=True)
    (real / "control" / "results").mkdir(parents=True)
    candidate = nested / ".." / f"mcp-{instance}"
    result = _run_scenario(
        scenario_root,
        "rendezvous-alias",
        {"RENDEZVOUS_ALIAS": str(candidate), "RENDEZVOUS_REAL": str(real)},
    )
    assert result["refusal"] is None
    assert result["identity"] == "0197f0c2-552b-7db4-8d43-6c338d7d3021"
    assert result["identityExists"] is True
    assert len(result["sent"]) == 1
    assert result["sent"][0]["text"].startswith(
        "[relay] from=peer.agent to=omp.r2 event_id=9007199254740993 kind=agent_message"
    )
    assert result["sent"][0]["options"] == {"deliverAs": "aside", "attribution": "agent"}


@pytest.mark.parametrize("scenario", ["native-guard", "native-unknown"])
def test_native_direct_child_failures_use_closed_identity_refusal(tmp_path, scenario):
    result = _run_scenario(tmp_path, scenario)
    assert result["refusal"]["ok"] is False
    assert result["refusal"]["code"] == "CONTROL_CHANNEL_INVALID"
    assert result["sent"] == []


def test_unsupported_main_mode_gets_closed_identity_refusal(tmp_path):
    result = _run_scenario(tmp_path, "mode-unsupported")
    assert result["refusal"]["ok"] is False
    assert result["refusal"]["code"] == "MODE_UNSUPPORTED"
    assert result["sent"] == []


def test_session_navigation_is_not_cancelled_without_a_relay_gate(tmp_path):
    result = _run_scenario(tmp_path, "no-gate-transition")
    assert result == {"beforeSwitch": None, "beforeBranch": None, "beforeTree": None}


@pytest.mark.parametrize(
    ("scenario", "ownership", "holder_pid", "dead_count"),
    [
        ("owner-dead", "self", None, 2),
        ("owner-live", "none", 812, 1),
        ("owner-unknown", "unknown", 812, 1),
    ],
)
def test_no_owner_live_owner_ambiguous_owner_and_transitive_dead_adoption(
    tmp_path, scenario, ownership, holder_pid, dead_count
):
    result = _run_scenario(tmp_path, scenario)
    assert result["ownership"] == ownership
    if holder_pid is None:
        assert result["owner"]["holder"]["omp_pid"] != 812
    else:
        assert result["owner"]["holder"]["omp_pid"] == holder_pid
    assert len(result["owner"]["dead_predecessors"]) == dead_count


@pytest.mark.parametrize("scenario", ["q-before", "q-during", "q-after"])
def test_pending_acquisition_quiescing_cancellation_and_fifo_lineage_orderings(tmp_path, scenario):
    result = _run_scenario(tmp_path, scenario)
    assert result["before"] == [None]
    assert result["sameCoordinator"] is True
    assert result["firstLifecycle"] == "superseded"
    assert result["finalOwnership"] == "self"
    assert result["finalOwner"] == result["finalExpected"]
    assert result["demandedFirst"] is False
    assert result["claims"] == 2


def test_live_rejection_terminalizes_exact_entry_without_ack_or_resend(tmp_path):
    result = _run_scenario(tmp_path, "terminal-reject")
    assert result["mirror"]["state"] == "rejected"
    assert result["mirror"]["broker_terminal_state"] == "rejected"
    assert result["mirror"]["reject_reason_code"] == "USER_REJECTED"
    assert result["mirror"]["identity_acknowledged"] is False
    assert len(result["sent"]) == 1
    assert result["health"]["state"] == "connected"


@pytest.mark.parametrize("terminal", ["accepted", "rejected"])
def test_malformed_mirror_terminal_recovery_uses_authenticated_result(tmp_path, terminal):
    result = _run_scenario(tmp_path, f"terminal-recovery-{terminal}")
    assert result["mirror"]["state"] == terminal
    assert result["mirror"]["broker_terminal_state"] == terminal
    assert result["mirror"]["identity_acknowledged"] is False
    assert result["sent"] == []
    if terminal == "accepted":
        assert result["mirror"]["application_acceptance_observed"] is True
    else:
        assert result["mirror"]["reject_reason_code"] == "USER_REJECTED"


def test_malformed_accepted_acked_recovery_reopens_and_admits_next_delivery(tmp_path):
    result = _run_scenario(tmp_path, "terminal-recovery-accepted-acked")
    assert result["mirror"]["state"] == "accepted"
    assert result["mirror"]["broker_terminal_state"] == "accepted"
    assert result["mirror"]["identity_acknowledged"] is True
    assert result["reopened"] == "open"
    assert result["reopenedHealth"]["code"] == "CONNECTED"
    assert result["laterMirror"]["event_id"] == "48"
    assert result["laterMirror"]["state"] == "injected"
    assert len(result["sent"]) == 1


def test_queued_delivery_waits_for_identity_ack_then_enters_same_gate(tmp_path):
    result = _run_scenario(tmp_path, "queued-ack-barrier")
    assert result["blocked"] == "event_fenced"
    assert result["queuedBeforeTerminal"] is False
    assert result["blockedHealth"]["code"] == "DELIVERY_PENDING"
    assert result["reopened"] == "open"
    assert result["laterMirror"]["event_id"] == "48"
    assert result["laterMirror"]["state"] == "injected"
    assert len(result["sent"]) == 1


def test_queued_delivery_waits_for_rejection_then_enters_same_gate(tmp_path):
    result = _run_scenario(tmp_path, "queued-reject-barrier")
    assert result["blocked"] == "event_fenced"
    assert result["queuedBeforeTerminal"] is False
    assert result["blockedHealth"]["code"] == "DELIVERY_PENDING"
    assert result["priorMirror"]["state"] == "rejected"
    assert result["priorMirror"]["broker_terminal_state"] == "rejected"
    assert result["reopened"] == "open"
    assert result["laterMirror"]["event_id"] == "48"
    assert result["laterMirror"]["state"] == "injected"
    assert len(result["sent"]) == 1


@pytest.mark.parametrize(
    ("scenario", "state", "send_count"),
    [
        ("recovery-send", "injected", 1),
        ("recovery-receipt", "accepted", 0),
        ("recovery-null", "received", 0),
    ],
)
def test_prior_process_state_specific_adoption_and_repeated_status_ticks(tmp_path, scenario, state, send_count):
    result = _run_scenario(tmp_path, scenario)
    assert result["mirror"]["state"] == state
    assert len(result["sent"]) == send_count
    assert len(result["owner"]["dead_predecessors"]) == 2


# BEGIN REVIEWED R0B BOUNDED LIFECYCLE MODEL
# Source SHA-256: 0161a250c01eda5c1fd7d5cdf6ce8cfde750d771bc398adfe25c01f4919f14e4
# Vendored into the R2 test owner so every lifecycle/model family remains Channel A.
#!/usr/bin/env python3
"""Bounded executable specification of the reviewed R0B design, not a relay.

Run: python docs/scripts/omp-r0b-lifecycle-model.py --json
Only the Python standard library is used. No files, network, processes, credentials,
clocks, or installed OMP modules are accessed. Named DAGs enumerate ALL their
causal linearizations (at most 12 externally scheduled events); longer named
traces exercise specified repeated histories. This is not exhaustive exploration
of an implementation, an unbounded protocol, or OS/filesystem failure behavior.

An issued operation and its later accounting callback are separate events. Its
physical critical section is synchronous/atomic except in the explicit guarded
compare/publish and stalled-holder scenarios. A settle event drains a FIFO with
invariants checked at each internal enqueue/start/completion; its internal steps
are counted, but are NOT advertised as independently permuted event depth.
"""

import argparse
import copy
import hashlib
import json
import sys
from dataclasses import asdict, dataclass, field, is_dataclass
from typing import Any


BOUND = 12
MUTANTS = (
    "stale_completion_retirement", "lost_source_retention", "incomplete_tuple_delete",
    "fifo_reset", "unguarded_compare_mutate", "stale_orphan_bind_refresh",
    "lost_transitive_witnesses", "epoch_only_adoption", "stuck_no_owner_transfer",
    "timeout_theft", "ambiguous_publication_send_regeneration",
    "competing_adoption_reservation", "consumed_send_replay",
    "marker_only_adoption_receipt",
)


class Violation(Exception):
    def __init__(self, rule: str, detail: Any):
        self.rule, self.detail = rule, detail
        super().__init__(f"{rule}: {detail}")


def require(condition: bool, rule: str, detail: Any) -> None:
    if not condition:
        raise Violation(rule, detail)


def normalized(value: Any) -> Any:
    if is_dataclass(value):
        return normalized(asdict(value))
    if isinstance(value, dict):
        return {str(k): normalized(v) for k, v in sorted(value.items(), key=lambda x: str(x[0]))}
    if isinstance(value, (set, frozenset)):
        return sorted((normalized(v) for v in value), key=repr)
    if isinstance(value, (list, tuple)):
        return [normalized(v) for v in value]
    if isinstance(value, bytes):
        return {"bytes_hex": value.hex()}
    return value


@dataclass(frozen=True)
class Owner:
    session: str
    pid: int
    start: int
    process: str
    claim: int
    holder_gate: int

    def witness(self) -> tuple:
        return self.session, self.pid, self.start, self.process


@dataclass
class Envelope:
    holder: Owner | None = None
    witnesses: set = field(default_factory=set)
    malformed: bool = False


@dataclass
class Claim:
    key: str
    owner: Owner
    lifecycle: str = "live"
    knowledge: str = "none"
    pending: str = "idle"
    operation_generation: int = 0
    source: str | None = None
    pins: int = 0


@dataclass
class Gate:
    session: str
    generation: int
    claim: str
    state: str = "rebind_blocked"
    resume: str = "rebind_blocked"
    was_bound: bool = False
    binding: str | None = None
    binding_instance: str | None = None
    command_returned: bool = False


@dataclass
class Coordinator:
    identity: str
    tail: int = 0
    queue: list = field(default_factory=list)
    running: str | None = None


@dataclass
class Operation:
    key: str
    process: str
    session: str
    coordinator: str
    sequence: int
    claim: str
    kind: str
    gate_generation: int | None
    instance: str
    pinned: tuple
    started: bool = False
    result: str = "not_started"
    resolved_sources: tuple = ()


@dataclass
class Process:
    pid: int
    start: int
    alive: bool = True
    stopping: bool = False
    stalled: bool = False
    gate: Gate | None = None
    generation: int = 0
    instance: str = "M1"
    high_water: int = 1
    children: dict = field(default_factory=lambda: {"M1": (1, True, True)})
    coordinators: dict = field(default_factory=dict)
    next_claim: dict = field(default_factory=dict)


@dataclass
class Attempt:
    process: str
    session: str
    instance: str
    gate: int
    claim: str
    expected: str | None
    new_epoch: str
    lawful: bool
    issued: bool = False
    result: str = "pending"


@dataclass
class Mirror:
    process: str
    state: str
    token: str
    application: bool = False
    terminal: str = "none"
    ack: bool = False
    malformed: bool = False


@dataclass
class Offer:
    session: str
    token: str
    state: str = "offered"
    delivery: str = "outstanding"
    reason: str | None = None
    from_agent: str = "sender.name"
    to_agent: str = "addressed.name"
    kind: str = "agent_message"
    content: str = "body"


@dataclass
class Entry:
    reservation: int
    state: str
    token: str
    send_right: bool = False
    consumed: bool = False
    sends: int = 0
    terminal: bool = False


class World:
    def __init__(self, mutant: str | None = None):
        self.mutant = mutant
        self.processes = {p: Process(100 + i, 1000 + i) for i, p in enumerate(("P0", "P1", "P2"))}
        self.envelopes = {s: Envelope() for s in ("A", "B")}
        self.claims: dict[str, Claim] = {}
        self.operations: dict[str, Operation] = {}
        self.guards: dict[str, str] = {}
        self.coordinator_roots: dict[tuple, str] = {}
        self.fifo_high_water: dict[tuple, int] = {}
        self.completed_sequences: dict[tuple, int] = {}
        self.claim_history: dict[str, str] = {}
        self.attempts: dict[str, Attempt] = {}
        self.broker_bindings: dict[str, tuple] = {}
        self.bind_ledger: dict[str, tuple] = {}
        self.successful_epochs: set = set()
        self.offers: dict[str, Offer] = {}
        self.mirrors: dict[str, Mirror] = {}
        self.entries: dict[tuple, Entry] = {}
        self.reservations: dict[tuple, list] = {}
        self.next_reservation = 0
        self.ambiguity_fences: dict[tuple, int] = {}
        self.send_log: list = []
        self.publications: list = []
        self.mutations: list = []
        self.unauthorized_commits: list = []
        self.guard_thefts: list = []
        self.expected_witnesses: dict[str, set] = {s: set() for s in ("A", "B")}
        self.comparisons: dict[tuple, Owner | None] = {}
        self.jsonl: dict[str, bytes] = {}
        self.observations: dict[str, Any] = {}
        self.operator_ledger: dict[str, tuple] = {}
        self.notice_count = 0
        self.internal_steps = 0
        self.status_reads = 0
        self.status_deliveries = 0
        self.terminal_history: dict[str, str] = {}
        self.ack_history: set = set()
        self.peak_claims = 0
        self.peak_claims_per_session = 0

    def snapshot(self) -> dict:
        return normalized(self.__dict__)

    def fingerprint(self) -> str:
        return hashlib.sha256(json.dumps(self.snapshot(), sort_keys=True).encode()).hexdigest()

    def view(self) -> dict:
        return normalized({
            "owners": self.envelopes, "gates": {p: v.gate for p, v in self.processes.items()},
            "claims": self.claims, "coordinators": {p: v.coordinators for p, v in self.processes.items()},
            "entries": self.entries, "mirrors": self.mirrors, "offers": self.offers,
            "sends": self.send_log, "observations": self.observations,
            "status_reads": self.status_reads, "status_deliveries": self.status_deliveries,
        })

    def lineage(self, key: str | None) -> list[str]:
        result = []
        while key is not None and key in self.claims:
            require(key not in result, "acyclic_source_lineage", result + [key])
            result.append(key)
            key = self.claims[key].source
        return result

    def demanded(self) -> set:
        result = set()
        for process in self.processes.values():
            if process.alive and process.gate:
                keys = self.lineage(process.gate.claim)
                result.update(keys[:1] if self.mutant == "lost_source_retention" else keys)
        return result

    def exact_self(self, claim: Claim) -> bool:
        envelope = self.envelopes[claim.owner.session]
        return not envelope.malformed and envelope.holder == claim.owner and claim.knowledge == "self"

    def current(self, p: str, claim: str, generation: int | None, instance: str) -> bool:
        process = self.processes[p]
        gate = process.gate
        return bool(process.alive and not process.stopping and gate and gate.claim == claim
                    and gate.generation == generation and process.instance == instance
                    and gate.state not in ("quiescing", "corrupt"))

    def unresolved(self, session: str) -> bool:
        for event, offer in self.offers.items():
            if offer.session != session:
                continue
            mirror = self.mirrors.get(event)
            installed = any(e == event and self.processes[p].alive for p, e in self.entries)
            if mirror is None and not installed:
                continue  # An unclaimed broker offer is not a local admission fence.
            if mirror is None or mirror.malformed or mirror.terminal == "none":
                return True
            if mirror.terminal == "accepted" and not mirror.ack:
                return True
        return False

    def checkpoint(self) -> None:
        self.internal_steps += 1
        self.peak_claims = max(self.peak_claims, len(self.claims))
        for p, process in self.processes.items():
            for session, coordinator in process.coordinators.items():
                root = (p, session)
                # Completion order, not a comparison of constant names, checks
                # FIFO safety. A lost queue is exposed by failed foreign-resume
                # progress after departure in the FIFO-reset control.
                self.fifo_high_water[root] = max(coordinator.tail, self.fifo_high_water.get(root, 0))
                count = sum(c.owner.process == p and c.owner.session == session for c in self.claims.values())
                self.peak_claims_per_session = max(self.peak_claims_per_session, count)
            gate = process.gate
            if gate and process.alive and gate.state not in ("corrupt",):
                claim = self.claims.get(gate.claim)
                if claim is not None:
                    require(claim.lifecycle == "live", "retained_gate_cannot_use_retired_claim",
                            {"process": p, "gate": gate.state, "claim": claim.key, "lifecycle": claim.lifecycle})
                    if gate.state == "open":
                        require(self.exact_self(claim) and not self.unresolved(gate.session),
                                "open_admission_requires_owner_and_resolved_delivery", gate)
                        require(gate.binding_instance == process.instance and gate.binding is not None,
                                "open_admission_requires_current_mcp_binding", gate)
        expected_pins = {key: 0 for key in self.claims}
        for op in self.operations.values():
            for key in op.pinned:
                require(key in self.claims, "operation_cannot_lose_pinned_claim", op)
                expected_pins[key] += 1
        for key, claim in self.claims.items():
            require(claim.pins == expected_pins[key], "balanced_operation_pins", claim)
            old = self.claim_history.get(key)
            require(not (old in ("retired", "released", "superseded") and claim.lifecycle == "live"),
                    "retirement_is_monotone", claim)
            self.claim_history[key] = claim.lifecycle
            matching = [op for op in self.operations.values() if op.claim == key]
            require(len(matching) <= 1 and (claim.pending == "idle") == (not matching),
                    "one_operation_per_claim", claim)
            if claim.source in self.claims:
                require(self.claims[claim.source].owner.claim < claim.owner.claim,
                        "source_points_to_older_claim", claim)
        for mutation in self.mutations:
            require(mutation["compared"] == mutation["actual_before"],
                    "owner_compare_and_mutate_is_atomic", mutation)
            if mutation["kind"] == "release":
                require(mutation["actual_before"] == mutation["expected_owner"],
                        "release_must_match_complete_original_tuple", mutation)
        require(not self.unauthorized_commits, "orphan_cannot_invalidate_current_binding", self.unauthorized_commits)
        require(not self.guard_thefts, "live_guard_cannot_be_stolen_by_timeout", self.guard_thefts)
        for key, owners in self.reservations.items():
            require(len(owners) <= 1, "one_reservation_completion_authority", {"entry": key, "owners": owners})
        for key, entry in self.entries.items():
            require(entry.sends <= 1, "no_same_process_duplicate_host_send", {"entry": key, "sends": entry.sends})
            if key in self.ambiguity_fences:
                require(entry.sends == self.ambiguity_fences[key],
                        "ambiguous_publication_forbids_same_process_send", {"entry": key, "sends": entry.sends})
        for event, offer in self.offers.items():
            old = self.terminal_history.get(event)
            if old:
                require(offer.state == old, "broker_terminal_winner_cannot_reverse", (event, old, offer.state))
            if offer.state in ("accepted", "rejected"):
                self.terminal_history[event] = offer.state
            mirror = self.mirrors.get(event)
            if mirror and mirror.ack:
                require(mirror.terminal == "accepted" and offer.delivery == "acked",
                        "ack_requires_exact_explicit_broker_proof", event)
                self.ack_history.add(event)
            if event in self.ack_history and mirror:
                require(mirror.ack, "ack_true_cannot_be_overwritten", event)

    def coordinator(self, p: str, session: str) -> Coordinator:
        process = self.processes[p]
        if session not in process.coordinators:
            identity = f"{p}:{session}:lifetime"
            process.coordinators[session] = Coordinator(identity)
            self.coordinator_roots[(p, session)] = identity
        return process.coordinators[session]

    def fresh_claim(self, p: str, session: str, generation: int, source: str | None = None) -> Claim:
        process = self.processes[p]
        number = process.next_claim.get(session, 0) + 1
        process.next_claim[session] = number
        owner = Owner(session, process.pid, process.start, p, number, generation)
        claim = Claim(f"{p}:{session}:C{number}", owner, source=source)
        self.claims[claim.key] = claim
        return claim

    def enter(self, p: str, session: str, retain: bool = True) -> None:
        process = self.processes[p]
        require(process.gate is None, "enter_requires_detached_gate", p)
        self.coordinator(p, session)
        sources = [c for c in self.claims.values() if c.owner.process == p and c.owner.session == session
                   and c.lifecycle not in ("released", "superseded")]
        source = max(sources, key=lambda c: c.owner.claim).key if retain and sources else None
        process.generation += 1
        claim = self.fresh_claim(p, session, process.generation, source)
        process.gate = Gate(session, process.generation, claim.key,
                            "restoring" if source else "rebind_blocked")
        self.enqueue(p, claim.key, "transfer" if source else "acquire")

    def enqueue(self, p: str, key: str, kind: str) -> None:
        claim = self.claims[key]
        if claim.pending != "idle":
            return
        process = self.processes[p]
        if not process.alive or process.stalled:
            return
        if kind != "release" and (process.stopping or claim.lifecycle != "live"):
            return
        coordinator = self.coordinator(p, claim.owner.session)
        coordinator.tail += 1
        claim.operation_generation += 1
        op_key = f"{key}:O{claim.operation_generation}"
        pinned = tuple(self.lineage(key))
        gate = process.gate
        operation = Operation(op_key, p, claim.owner.session, coordinator.identity, coordinator.tail,
                              key, kind, gate.generation if gate and gate.claim == key else None,
                              process.instance, pinned)
        self.operations[op_key] = operation
        coordinator.queue.append(op_key)
        claim.pending = kind
        for pin in pinned:
            self.claims[pin].pins += 1

    def acquire_guard(self, p: str, session: str) -> bool:
        holder = self.guards.get(session)
        if holder is not None and holder != p:
            return False
        self.guards[session] = p
        return True

    def release_guard(self, p: str, session: str) -> None:
        if self.guards.get(session) == p:
            del self.guards[session]

    def publish_owner(self, p: str, session: str, target: Owner | None,
                      compared: Owner | None, kind: str, expected: Owner | None = None) -> None:
        envelope = self.envelopes[session]
        self.mutations.append({"kind": kind, "process": p, "session": session,
                               "guarded": self.guards.get(session) == p,
                               "compared": compared, "actual_before": envelope.holder,
                               "expected_owner": expected, "after": target})
        envelope.holder = target

    def physical(self, op: Operation, outcome: str) -> None:
        claim = self.claims[op.claim]
        envelope = self.envelopes[op.session]
        if not self.acquire_guard(op.process, op.session):
            op.result = "guard_busy"
            return
        try:
            if envelope.malformed:
                op.result = "unknown"
                return
            if outcome == "failure":
                op.result = "failure"
                return
            holder = envelope.holder
            if op.kind == "release":
                equal = holder == claim.owner
                if self.mutant == "incomplete_tuple_delete" and holder:
                    equal = holder.witness() == claim.owner.witness()
                if equal:
                    if outcome in ("ambiguous", "ambiguous_keep"):
                        op.result = "unknown"
                        if outcome == "ambiguous":
                            self.publish_owner(op.process, op.session, None, holder, "release", claim.owner)
                    else:
                        self.publish_owner(op.process, op.session, None, holder, "release", claim.owner)
                        op.result = "released"
                else:
                    op.result = "released"
                return
            lineage = self.lineage(claim.source)
            source_owners = {self.claims[key].owner for key in lineage}
            if op.kind == "transfer" and outcome == "ambiguous_keep":
                op.result = "unknown"
                return
            if holder == claim.owner:
                op.result, op.resolved_sources = "self", tuple(lineage)
                return
            if holder in source_owners:
                self.publish_owner(op.process, op.session, claim.owner, holder, "transfer")
                op.result = "unknown" if outcome == "ambiguous" else "self"
                if op.result == "self":
                    op.resolved_sources = tuple(lineage)
                return
            if op.kind == "transfer" and self.mutant == "stuck_no_owner_transfer":
                op.result = "unresolved_source"
                return
            if outcome == "ambiguous_absent":
                op.result = "unknown"
                return
            op.resolved_sources = tuple(lineage)
            if holder is not None and self.processes[holder.process].alive:
                op.result = "foreign"
                return
            if holder is not None:
                witness = holder.witness()
                self.expected_witnesses[op.session].add(witness)
                if self.mutant == "lost_transitive_witnesses":
                    envelope.witnesses = {witness}
                else:
                    envelope.witnesses.add(witness)
            self.publish_owner(op.process, op.session, claim.owner, holder, "acquire")
            op.result = "unknown" if outcome == "ambiguous" else "self"
        finally:
            self.release_guard(op.process, op.session)

    def start(self, p: str, session: str, outcome: str = "success") -> None:
        process = self.processes[p]
        coordinator = self.coordinator(p, session)
        if not process.alive or process.stalled or coordinator.running or not coordinator.queue:
            return
        key = coordinator.queue.pop(0)
        op = self.operations[key]
        coordinator.running = key
        op.started = True
        claim = self.claims[op.claim]
        if op.kind == "release" and claim.key in self.demanded():
            op.result = "retained"
        elif op.kind != "release" and (claim.lifecycle != "live" or process.stopping):
            op.result = "skipped"
        else:
            self.physical(op, outcome)

    def finish_restore(self, p: str) -> None:
        process = self.processes[p]
        gate = process.gate
        if not gate or gate.state != "restoring":
            return
        claim = self.claims[gate.claim]
        if not self.exact_self(claim) or claim.source:
            return
        if gate.resume == "rebind_blocked":
            gate.state = "rebind_blocked"
        elif gate.binding_instance != process.instance or not gate.binding or self.unresolved(gate.session):
            gate.state = "event_fenced"
        else:
            gate.state = "open"

    def complete(self, p: str, session: str) -> None:
        process = self.processes[p]
        coordinator = self.coordinator(p, session)
        if not process.alive or process.stalled or coordinator.running is None:
            return
        op = self.operations.pop(coordinator.running)
        coordinator.running = None
        claim = self.claims[op.claim]
        root = (p, session)
        require(op.coordinator == coordinator.identity and op.sequence > self.completed_sequences.get(root, 0),
                "physical_completion_uses_original_advancing_fifo", op)
        self.completed_sequences[root] = op.sequence
        claim.pending = "idle"
        for key in op.pinned:
            self.claims[key].pins -= 1
        if op.result == "self":
            claim.knowledge = "self"
        elif op.result == "unknown":
            claim.knowledge = "unknown"
        elif op.result in ("foreign", "released", "skipped"):
            claim.knowledge = "none"
        if op.kind == "release" and op.result == "released":
            claim.lifecycle = "released"
        for key in op.resolved_sources:
            source = self.claims[key]
            source.lifecycle = "superseded" if op.result == "self" else "released"
            source.knowledge = "none"
            source.source = None
        if op.resolved_sources:
            claim.source = None
        is_current = self.current(p, claim.key, op.gate_generation, op.instance)
        if not is_current and self.mutant == "stale_completion_retirement" and claim.lifecycle == "live":
            claim.lifecycle = "retired"
        if is_current:
            self.publications.append((p, session, op.gate_generation, claim.key, op.instance, op.result))
            self.observations[f"health:{p}:{session}"] = (
                "SESSION_ALREADY_OWNED" if op.result == "foreign" else
                "OWNERSHIP_UNKNOWN" if claim.knowledge == "unknown" else claim.knowledge)
            self.finish_restore(p)
        self.retire_undemanded(p)

    def retire_undemanded(self, p: str) -> None:
        demanded = self.demanded()
        for claim in list(self.claims.values()):
            if claim.owner.process != p or claim.key in demanded:
                continue
            if claim.lifecycle == "live":
                claim.lifecycle = "retired"
            if claim.lifecycle == "retired" and claim.pending == "idle":
                if claim.knowledge in ("self", "unknown"):
                    self.enqueue(p, claim.key, "release")
                else:
                    claim.lifecycle = "released"
        # Retire every source before removing any obsolete edge.
        for claim in self.claims.values():
            if claim.key not in demanded and claim.source:
                sources = self.lineage(claim.source)
                if all(self.claims[key].lifecycle in ("released", "superseded") for key in sources):
                    claim.source = None

    def settle(self, p: str, session: str, outcome: str = "success") -> None:
        coordinator = self.coordinator(p, session)
        # Finite drain, not a retry loop: a failed release may enqueue a retry,
        # but that retry awaits the prescribed later reconciliation event.
        remaining = len(coordinator.queue) + int(coordinator.running is not None)
        for _ in range(remaining):
            if coordinator.running is None:
                self.start(p, session, outcome)
                self.checkpoint()
            self.complete(p, session)
            self.checkpoint()

    def before(self, p: str) -> None:
        process = self.processes[p]
        gate = process.gate
        if not gate or process.stopping:
            return
        claim = self.claims.get(gate.claim)
        if claim is None or claim.lifecycle != "live":
            gate.state = "corrupt"
            self.observations[f"before:{p}"] = "invalid_association_refused"
            return
        if gate.state == "restoring" and gate.resume == "open":
            self.observations[f"before:{p}"] = "bound_restoration_refused"
            return
        if gate.state in ("event_fenced", "corrupt", "quiescing"):
            return
        resume = "open" if gate.was_bound or gate.state == "open" else "rebind_blocked"
        if resume == "open":
            gate.state = "checking"
            if self.unresolved(gate.session):
                gate.state = "event_fenced"
                self.observations[f"before:{p}"] = "delivery_pending"
                return
        process.generation += 1
        process.gate = Gate(gate.session, process.generation, gate.claim, "quiescing", resume,
                            gate.was_bound, gate.binding, gate.binding_instance)

    def cancel(self, p: str) -> None:
        gate = self.processes[p].gate
        if gate and gate.state == "quiescing":
            gate.command_returned = True

    def unquiesce(self, p: str, mode: str = "tui", main: bool = True,
                  ui: bool = True, confirmed: bool = True) -> None:
        process = self.processes[p]
        gate = process.gate
        if not (gate and gate.state == "quiescing" and gate.command_returned
                and mode == "tui" and main and ui and confirmed and not process.stopping):
            self.observations[f"unquiesce:{p}"] = "refused"
            return
        process.generation += 1
        claim = self.fresh_claim(p, gate.session, process.generation, gate.claim)
        process.gate = Gate(gate.session, process.generation, claim.key, "restoring", gate.resume,
                            gate.was_bound, gate.binding, gate.binding_instance)
        self.retire_undemanded(p)
        self.enqueue(p, claim.key, "transfer")

    def after(self, p: str, new_session: str | None = None) -> None:
        process = self.processes[p]
        gate = process.gate
        if not gate or gate.state != "quiescing":
            return
        process.gate = None
        self.retire_undemanded(p)
        if self.mutant == "fifo_reset":
            coordinator = process.coordinators[gate.session]
            process.coordinators[gate.session] = Coordinator(coordinator.identity + ":replacement")
        if new_session:
            self.enter(p, new_session)

    def wake(self, p: str, instance: str = "M1") -> None:
        process = self.processes[p]
        if process.instance != instance or not process.alive or process.stalled:
            self.observations[f"wake:{p}:{instance}"] = "stale_or_unavailable"
            return
        gate = process.gate
        if gate and not process.stopping and gate.state in ("rebind_blocked", "restoring"):
            claim = self.claims.get(gate.claim)
            if claim and claim.lifecycle == "live" and not self.exact_self(claim):
                self.enqueue(p, claim.key, "transfer" if claim.source else "acquire")
            elif claim and gate.state == "restoring":
                self.enqueue(p, claim.key, "transfer")
        self.retire_undemanded(p)

    def gc(self, p: str) -> None:
        referenced = self.demanded() | {c.source for c in self.claims.values() if c.source}
        for key, claim in list(self.claims.items()):
            if claim.owner.process == p and claim.lifecycle in ("released", "superseded"):
                if key not in referenced and claim.pending == "idle" and claim.pins == 0:
                    del self.claims[key]

    def reload(self, p: str) -> None:
        # Deliberately no reinitialization: the process-global object is reused.
        self.observations[f"reload:{p}"] = tuple(
            (s, c.identity, c.tail, c.running) for s, c in self.processes[p].coordinators.items())

    def stop(self, p: str) -> None:
        process = self.processes[p]
        if process.stalled:
            self.observations[f"stop:{p}"] = "thread_blocked"
            return
        process.stopping = True
        if process.gate:
            process.gate.state = "event_fenced"
        self.retire_undemanded(p)

    def death(self, p: str) -> None:
        process = self.processes[p]
        process.alive = False
        process.stalled = False
        for session in list(self.guards):
            self.release_guard(p, session)
        # No dead process callback, queued host send, or cleanup is replayed.

    def hold_guard(self, p: str, session: str, stall: bool = False) -> None:
        acquired = self.acquire_guard(p, session)
        self.observations[f"guard:{p}:{session}"] = acquired
        if acquired and stall:
            self.processes[p].stalled = True

    def timeout(self, p: str, session: str) -> None:
        old = self.guards.get(session)
        if old and self.mutant == "timeout_theft":
            self.guard_thefts.append((session, old, p, self.processes[old].alive))
            self.guards[session] = p
        self.observations[f"timeout:{session}"] = "no_authority"

    def heartbeat(self, p: str) -> None:
        self.observations[f"heartbeat:{p}"] = not self.processes[p].stalled and self.processes[p].alive

    def compare(self, p: str, session: str) -> None:
        if self.mutant == "unguarded_compare_mutate" or self.acquire_guard(p, session):
            self.comparisons[(p, session)] = self.envelopes[session].holder

    def compare_publish(self, p: str, session: str) -> None:
        key = (p, session)
        if key not in self.comparisons:
            return
        old = self.comparisons.pop(key)
        claim = self.claims[self.processes[p].gate.claim]
        if old is None or not self.processes[old.process].alive:
            if old:
                self.envelopes[session].witnesses.add(old.witness())
                self.expected_witnesses[session].add(old.witness())
            self.publish_owner(p, session, claim.owner, old, "contender_acquire")
            claim.knowledge = "self"
        self.release_guard(p, session)

    def identity(self, p: str, instance: str, sequence: int, direct: bool = True,
                 live: bool = True, exact: bool = True) -> None:
        process = self.processes[p]
        process.children[instance] = (sequence, direct, live)
        valid = direct and live and exact and sequence >= process.high_water
        if sequence == process.high_water and instance != process.instance:
            valid = False
        if valid:
            changed = process.instance != instance
            process.instance, process.high_water = instance, sequence
            if changed and process.gate and process.gate.was_bound:
                if process.gate.state != "restoring":
                    process.gate.state = "event_fenced"
        self.observations[f"identity:{p}:{instance}"] = "accepted" if valid else "refused"

    def propose(self, p: str, attempt: str, instance: str | None = None) -> None:
        process = self.processes[p]
        gate = process.gate
        instance = instance or process.instance
        claim = self.claims.get(gate.claim) if gate else None
        lawful = bool(gate and claim and self.exact_self(claim) and not process.stopping
                      and process.alive and instance == process.instance
                      and gate.state in ("open", "event_fenced", "rebind_blocked"))
        if not lawful and self.mutant != "stale_orphan_bind_refresh":
            self.observations[f"proposal:{attempt}"] = "refused"
            return
        if not gate:
            return
        current = self.broker_bindings.get(gate.session)
        self.attempts[attempt] = Attempt(p, gate.session, instance, gate.generation, gate.claim,
                                         current[0] if current else None, f"epoch:{attempt}", lawful)

    def bind_http(self, attempt: str) -> None:
        if attempt not in self.attempts:
            return
        request = self.attempts[attempt]
        digest = (request.process, request.session, request.instance, request.gate,
                  request.claim, request.expected, request.new_epoch)
        current = self.broker_bindings.get(request.session)
        current_epoch = current[0] if current else None
        request.issued = True
        if attempt in self.bind_ledger:
            original, won = self.bind_ledger[attempt]
            request.result = "success" if original == digest and won and current_epoch == request.new_epoch else "conflict"
            return
        won = current_epoch == request.expected and request.new_epoch not in self.successful_epochs
        self.bind_ledger[attempt] = (digest, won)
        request.result = "success" if won else "conflict"
        if won:
            self.broker_bindings[request.session] = (request.new_epoch, request.process, request.instance)
            self.successful_epochs.add(request.new_epoch)
            if not request.lawful:
                self.unauthorized_commits.append((attempt, current, self.broker_bindings[request.session]))

    def bind_response(self, attempt: str) -> None:
        if attempt not in self.attempts:
            return
        request = self.attempts[attempt]
        if request.result != "success" or not self.current(request.process, request.claim, request.gate, request.instance):
            return
        process = self.processes[request.process]
        gate = process.gate
        if not self.exact_self(self.claims[gate.claim]) or gate.state == "restoring":
            return
        gate.binding, gate.binding_instance, gate.was_bound = request.new_epoch, request.instance, True
        gate.state = "event_fenced"
        self.maybe_open(request.process)

    def bind(self, p: str, attempt: str, instance: str | None = None) -> None:
        self.propose(p, attempt, instance)
        self.checkpoint()
        self.bind_http(attempt)
        self.checkpoint()
        self.bind_response(attempt)

    def bind_failure(self, p: str, attempt: str) -> None:
        self.propose(p, attempt)
        request = self.attempts.get(attempt)
        if request:
            request.issued, request.result = True, "transient_failure"
            self.observations[f"bind_failure:{p}"] = "rebind_blocked_with_self_lock"

    def broker_retry(self, p: str, attempt: str) -> None:
        process = self.processes[p]
        gate = process.gate
        if gate and self.exact_self(self.claims[gate.claim]) and gate.state == "rebind_blocked":
            self.propose(p, attempt)
            self.observations[f"broker_retry:{p}"] = "authorized"
        else:
            self.observations[f"broker_retry:{p}"] = "ownership_driver_required"

    def authenticated(self, p: str, event: str, instance: str | None = None) -> bool:
        process = self.processes[p]
        gate = process.gate
        if not (process.alive and not process.stopping and gate and event in self.offers):
            return False
        if instance is not None and instance != process.instance:
            return False
        return (gate.session == self.offers[event].session and gate.binding_instance == process.instance
                and self.broker_bindings.get(gate.session) == (gate.binding, p, process.instance)
                and gate.state in ("open", "event_fenced") and self.exact_self(self.claims[gate.claim]))

    def maybe_open(self, p: str) -> None:
        process = self.processes[p]
        gate = process.gate
        if (gate and gate.state == "event_fenced" and not process.stopping
                and gate.binding_instance == process.instance and gate.binding
                and self.exact_self(self.claims[gate.claim]) and not self.unresolved(gate.session)):
            gate.state = "open"

    def offer(self, event: str = "1", session: str = "A") -> None:
        self.offers[event] = Offer(session, f"token:{event}")

    def reserve(self, p: str, event: str, state: str) -> Entry:
        key = (p, event)
        if key in self.entries and not (self.mutant == "competing_adoption_reservation"
                                        and self.entries[key].state == "pending_adoption"):
            return self.entries[key]
        self.next_reservation += 1
        entry = Entry(self.next_reservation, state, self.offers[event].token)
        self.entries[key] = entry
        self.reservations.setdefault(key, []).append(entry.reservation)
        return entry

    def send(self, p: str, event: str) -> None:
        entry = self.entries[(p, event)]
        process = self.processes[p]
        if not entry.send_right or not self.authenticated(p, event):
            return
        entry.send_right = False  # consume synchronously BEFORE the host API
        entry.consumed = True
        entry.sends += 1
        entry.state = "sent"
        self.send_log.append((p, event, entry.reservation))
        process.gate.state = "event_fenced"

    def notify(self, p: str, event: str = "1", failure: str = "ok") -> None:
        process = self.processes[p]
        key = (p, event)
        if key in self.entries or not self.authenticated(p, event) or process.gate.state != "open":
            return
        if any(int(e) < int(event) and o.session == self.offers[event].session and o.delivery == "outstanding"
               for e, o in self.offers.items()):
            return
        entry = self.reserve(p, event, "received")
        if event in self.mirrors or failure != "ok":
            process.gate.state = "event_fenced"  # fence BEFORE classification
            if failure == "malformed":
                self.mirrors[event] = Mirror(p, "received", entry.token, malformed=True)
            elif failure == "valid":
                self.mirrors[event] = Mirror(p, "received", entry.token)
            return
        self.mirrors[event] = Mirror(p, "received", entry.token)
        entry.send_right = True
        self.send(p, event)

    def status_tick(self, p: str, event: str = "1", failure: str = "ok", instance: str | None = None) -> None:
        # This is the ONLY adoption driver: simulated MCP tick -> authenticated
        # exact per-event status -> exact head claim -> offer_recovery_status.
        if not self.authenticated(p, event, instance):
            return
        self.status_reads += 1
        offer = self.offers[event]
        head = min((int(e) for e, o in self.offers.items()
                    if o.session == offer.session and o.delivery == "outstanding"), default=None)
        delivery = offer.state not in ("accepted", "rejected") and head == int(event)
        self.status_deliveries += int(delivery)
        self.recovery_observation(p, event, offer.token, offer.state, delivery, failure)

    def recovery_observation(self, p: str, event: str, token: str, terminal: str,
                             delivery: bool, failure: str) -> None:
        # Internal continuation, never independently scheduled by the explorer.
        if not self.authenticated(p, event):
            return
        key = (p, event)
        mirror = self.mirrors.get(event)
        entry = self.entries.get(key)
        if mirror is None and entry is not None:
            self.observations[f"recovery:{p}:{event}"] = "absent_live_entry_restart_only"
            return
        if entry and entry.token != token:
            return
        if mirror and not mirror.malformed and mirror.token != token:
            return
        if terminal in ("accepted", "rejected"):
            # Broker token is authoritative for malformed/no-entry recovery.
            entry = entry or self.reserve(p, event, "terminal_recovery")
            if not self.acquire_guard(p, self.offers[event].session):
                return
            try:
                if not self.authenticated(p, event):
                    return
                ack = bool(mirror and mirror.ack and terminal == "accepted")
                self.mirrors[event] = Mirror(p, terminal, token, terminal == "accepted", terminal, ack)
                entry.terminal, entry.send_right, entry.state = True, False, "terminal"
            finally:
                self.release_guard(p, self.offers[event].session)
            if terminal == "accepted" and self.offers[event].delivery == "acked":
                self.ack(p, event, "status_acked")
            self.maybe_open(p)
            return
        if mirror is None or mirror.malformed:
            return
        if mirror.process == p:
            if entry and self.mutant == "consumed_send_replay" and entry.consumed and delivery:
                entry.send_right = True
                self.send(p, event)
            return
        envelope = self.envelopes[self.offers[event].session]
        predecessor = self.processes[mirror.process]
        witness = (self.offers[event].session, predecessor.pid, predecessor.start, mirror.process)
        if witness not in envelope.witnesses:
            self.observations[f"recovery:{p}:{event}"] = "missing_dead_witness"
            return
        entry = self.reserve(p, event, "pending_adoption")
        if key in self.ambiguity_fences and self.mutant != "ambiguous_publication_send_regeneration":
            return
        if self.mutant == "epoch_only_adoption":
            mirror.process = p
            self.observations[f"recovery:{p}:{event}"] = "epoch_rewritten_without_decision"
            return
        accepted = mirror.state == "accepted" and mirror.application
        if not accepted and not delivery:
            return  # no exact current-head payload means no scan-based proof
        if not accepted:
            accepted = self.receipt(event)
            if self.mutant == "marker_only_adoption_receipt":
                marker = f"[omp-relay-accept:v1 event_id={event} token={token} generation=1]"
                accepted = marker.encode() in self.jsonl.get(self.offers[event].session, b"")
        session = self.offers[event].session
        if not self.acquire_guard(p, session):
            entry.state = "pending_adoption"
            return
        try:
            if not self.authenticated(p, event) or self.offers[event].state != terminal:
                return
            if failure == "temp":
                entry.state = "pending_adoption"
                return
            replacement = Mirror(p, "accepted" if accepted else "received", token, accepted)
            if failure in ("ambiguous_old", "ambiguous_new"):
                if failure == "ambiguous_new":
                    self.mirrors[event] = replacement
                self.ambiguity_fences[key] = entry.sends
                entry.state, entry.send_right = "ambiguous", False
                return
            self.mirrors[event] = replacement
            entry.state = "application_accepted" if accepted else "adopted"
            entry.send_right = not accepted
        finally:
            self.release_guard(p, session)
        self.send(p, event)

    def receipt_content(self, event: str) -> str:
        offer = self.offers[event]
        display = (f"[relay] from={offer.from_agent} to={offer.to_agent} "
                   f"event_id={event} kind={offer.kind}\n\n{offer.content}")
        return f"{display}\n\n[omp-relay-accept:v1 event_id={event} token={offer.token} generation=1]"

    def write_jsonl(self, event: str, variant: str = "complete") -> None:
        session = self.offers[event].session
        header = {"type": "session", "id": session if variant != "wrong_header" else "B"}
        message = {"type": "message", "message": {
            "role": "assistant" if variant == "wrong_role" else "user",
            "content": [{"type": "text", "text": self.receipt_content(event) + (" extra" if variant == "wrong_content" else "")}],
        }}
        first, second = json.dumps(header).encode() + b"\n", json.dumps(message).encode()
        self.jsonl[session] = first + second + (b"" if variant == "unterminated" else b"\n")

    def receipt(self, event: str) -> bool:
        session = self.offers[event].session
        records = []
        for line in self.jsonl.get(session, b"").splitlines(keepends=True):
            if not line.endswith(b"\n"):
                continue
            try:
                records.append(json.loads(line))
            except (ValueError, UnicodeDecodeError):
                continue
        if not records or records[0].get("type") != "session" or records[0].get("id") != session:
            return False
        for record in records[1:]:
            message = record.get("message", {})
            content = message.get("content")
            exact = [{"type": "text", "text": self.receipt_content(event)}]
            if record.get("type") == "message" and message.get("role") == "user" and content == exact:
                return True
        return False

    def scan(self, p: str, event: str, label: str) -> None:
        found = self.receipt(event)
        self.observations[label] = found
        mirror = self.mirrors.get(event)
        if found and mirror and not mirror.malformed and mirror.process == p and mirror.terminal == "none":
            mirror.state, mirror.application = "accepted", True

    def terminal(self, p: str, event: str, target: str) -> None:
        offer = self.offers[event]
        if target == "accepted":
            mirror = self.mirrors.get(event)
            if not (self.authenticated(p, event) and mirror and mirror.process == p and mirror.application
                    and not mirror.malformed and mirror.token == offer.token):
                return
        if offer.state in ("accepted", "rejected"):
            return
        offer.state = target
        if target == "rejected":
            offer.delivery, offer.reason = "rejected", "DELIVERY_UNCERTAIN_CONTENT_MAY_HAVE_BEEN_SEEN"

    def ack(self, p: str, event: str = "1", proof: str = "post", instance: str | None = None) -> None:
        if not self.authenticated(p, event, instance):
            return
        mirror = self.mirrors.get(event)
        offer = self.offers[event]
        if not (mirror and mirror.process == p and not mirror.malformed and mirror.terminal == "accepted"
                and mirror.token == offer.token and offer.state == "accepted"):
            return
        if proof in ("post", "already_acked"):
            if proof == "already_acked" and offer.delivery != "acked":
                return
            offer.delivery = "acked"
        elif proof != "status_acked" or offer.delivery != "acked":
            return
        mirror.ack = True
        self.maybe_open(p)

    def broker_ack_only(self, p: str, event: str = "1") -> None:
        mirror = self.mirrors.get(event)
        if self.authenticated(p, event) and mirror and mirror.terminal == "accepted":
            self.offers[event].delivery = "acked"

    def damage_mirror(self, event: str, kind: str) -> None:
        if kind == "absent":
            self.mirrors.pop(event, None)
        elif kind == "token":
            self.mirrors[event].token = "different_token"
        elif kind == "entry_token":
            self.mirrors[event].malformed = True
            for (p, candidate), entry in self.entries.items():
                if candidate == event and self.processes[p].alive:
                    entry.token = "different_token"
        else:
            self.mirrors[event].malformed = True
        for process in self.processes.values():
            if process.gate and process.gate.session == self.offers[event].session:
                process.gate.state = "event_fenced"

    def remove_entry(self, p: str, event: str) -> None:
        self.entries.pop((p, event), None)
        self.reservations.pop((p, event), None)

    def corrupt_claim(self, p: str, kind: str) -> None:
        gate = self.processes[p].gate
        if kind == "missing":
            gate.claim = "missing-record"
        else:
            self.claims[gate.claim].lifecycle = "released"
        # Corruption is external input; the next before handler must diagnose,
        # not dereference, delete, or silently authorize it.
        gate.state = "corrupt"

    def host_switched_after_corruption(self, p: str, session: str) -> None:
        process = self.processes[p]
        process.generation += 1
        self.coordinator(p, session)
        claim = self.fresh_claim(p, session, process.generation)
        process.gate = Gate(session, process.generation, claim.key, "corrupt")
        # Only exact independently recorded old claims can be cleaned. The new
        # session has no bind/admission authority from the invalid association.
        self.retire_undemanded(p)

    def absent_owner_record(self, session: str) -> None:
        # Explicit external missing-file observation: absence adds no witness.
        self.envelopes[session].holder = None

    def malformed_owner(self, session: str, malformed: bool = True) -> None:
        self.envelopes[session].malformed = malformed

    def obsolete_release(self, p: str, session: str, old_number: int) -> None:
        old = next(c for c in self.claims.values() if c.owner.process == p and c.owner.session == session
                   and c.owner.claim == old_number)
        self.enqueue(p, old.key, "release")

    def command(self, p: str, action: str, label: str, mode: str = "tui", main: bool = True,
                ui: bool = True, confirmed: bool = True, signed: bool = True) -> None:
        process = self.processes[p]
        gate = process.gate
        current_binding = bool(gate and gate.binding and gate.binding_instance == process.instance)
        exact_privilege = mode == "tui" and main and ui and confirmed and signed
        if action in ("send", "register", "name"):
            permitted = current_binding and gate.state == "open"
        elif action == "reject":
            permitted = current_binding and exact_privilege and gate.state in ("open", "event_fenced")
        elif action == "takeover":
            permitted = current_binding and exact_privilege and gate.state == "open"
        else:
            permitted = False
        self.observations[label] = bool(permitted)

    def operator(self, request: str, method: str = "POST", route: str = "bound",
                 path: str = "A", reason: str = "PROTOCOL_INVALID", authorized: bool = True) -> None:
        if not authorized:
            self.observations[f"operator:{request}:last"] = "auth_refused"
            return
        digest = (method, route, path, "1", reason)
        if request in self.operator_ledger:
            original, result = self.operator_ledger[request]
            self.observations[f"operator:{request}:last"] = result if original == digest else "REQUEST_CONFLICT"
            return
        offer = self.offers["1"]
        if route != "bound" or path != offer.session:
            result = "RECIPIENT_CONFLICT"
        elif offer.state in ("accepted", "rejected"):
            result = "TERMINAL_CONFLICT"
        else:
            self.terminal("P1", "1", "rejected")
            self.notice_count += 1
            result = "REJECTED"
        self.operator_ledger[request] = (digest, result)
        self.observations[f"operator:{request}:last"] = result

    def observe(self, label: str, subject: str, *args: Any) -> None:
        self.observations[label] = self.value(subject, *args)

    def value(self, subject: str, *args: Any) -> Any:
        if subject == "gate":
            gate = self.processes[args[0]].gate
            return gate.state if gate else None
        if subject == "owner":
            owner = self.envelopes[args[0]].holder
            return owner.process if owner else None
        if subject == "owner_claim":
            owner = self.envelopes[args[0]].holder
            return owner.claim if owner else None
        if subject == "self":
            gate = self.processes[args[0]].gate
            return bool(gate and self.exact_self(self.claims[gate.claim]))
        if subject == "sends":
            return sum(p == args[0] and e == args[1] for p, e, _ in self.send_log)
        if subject == "reservations":
            return len(self.reservations.get((args[0], args[1]), ()))
        if subject == "witness":
            return any(w[3] == args[1] for w in self.envelopes[args[0]].witnesses)
        if subject == "mirror":
            mirror = self.mirrors.get(args[0])
            return getattr(mirror, args[1]) if mirror else None
        if subject == "offer":
            return getattr(self.offers[args[0]], args[1])
        if subject == "observation":
            return self.observations.get(args[0])
        if subject == "claim_count":
            return sum(c.owner.process == args[0] and c.owner.session == args[1] for c in self.claims.values())
        if subject == "pending":
            return sum(op.process == args[0] and op.session == args[1] for op in self.operations.values())
        if subject == "alive":
            return self.processes[args[0]].alive
        if subject == "notice_count":
            return self.notice_count
        if subject == "current_instance":
            return self.processes[args[0]].instance
        if subject == "status_reads":
            return self.status_reads
        raise ValueError(f"Unknown observation {subject}")

    def apply(self, action: tuple) -> None:
        name, *args = action
        getattr(self, name)(*args)
        self.checkpoint()


@dataclass(frozen=True)
class Event:
    name: str
    action: tuple
    after: tuple = ()


@dataclass(frozen=True)
class Goal:
    subject: str
    args: tuple
    expected: Any

    def check(self, world: World) -> None:
        actual = world.value(self.subject, *self.args)
        require(actual == self.expected, "bounded_progress_or_observable_outcome",
                {"subject": self.subject, "args": self.args, "expected": self.expected, "actual": actual})


@dataclass(frozen=True)
class Scenario:
    name: str
    group: str
    seed: tuple
    events: tuple
    goals: tuple
    mode: str = "causal_permutations"


def event(name: str, action: tuple, *after: str) -> Event:
    return Event(name, action, tuple(after))


def goal(subject: str, *args_and_expected: Any) -> Goal:
    return Goal(subject, tuple(args_and_expected[:-1]), args_and_expected[-1])


def chain(*actions: tuple) -> tuple:
    return tuple(Event(f"{i + 1:02d}:{action[0]}", action, (f"{i:02d}:{actions[i - 1][0]}",) if i else ())
                 for i, action in enumerate(actions))


def held(p: str = "P1", session: str = "A") -> tuple:
    return (("enter", p, session), ("settle", p, session))


def bound(p: str = "P1", session: str = "A", attempt: str = "initial") -> tuple:
    return held(p, session) + (("bind", p, attempt),)


def adoption_seed() -> tuple:
    return bound("P0", attempt="old") + (
        ("offer",), ("notify", "P0"), ("death", "P0"),
        ("enter", "P1", "A"), ("settle", "P1", "A"), ("bind", "P1", "new"),
    )


def scenarios() -> list[Scenario]:
    result = []
    add = result.append
    for outcome in ("success", "failure", "ambiguous", "ambiguous_absent"):
        add(Scenario(f"pending_acquisition_departure_{outcome}", "Pending acquisition",
            (("enter", "P1", "A"),), (
                event("issue", ("start", "P1", "A", outcome)),
                event("before", ("before", "P1")),
                event("account", ("complete", "P1", "A"), "issue"),
                event("after", ("after", "P1"), "before"),
                event("cleanup", ("settle", "P1", "A"), "account", "after"),
                event("wake", ("wake", "P1"), "cleanup"),
                event("cleanup_retry", ("settle", "P1", "A"), "wake"),
                event("foreign_enter", ("enter", "P2", "A"), "cleanup_retry"),
                event("foreign_acquire", ("settle", "P2", "A"), "foreign_enter"),
            ), (goal("owner", "A", "P2"), goal("alive", "P1", True))))
    add(Scenario("pending_acquisition_foreign_loss", "Pending acquisition", held("P2") + (("enter", "P1", "A"),), (
        event("issue", ("start", "P1", "A")), event("before", ("before", "P1")),
        event("account", ("complete", "P1", "A"), "issue"),
        event("after", ("after", "P1"), "before"),
        event("cleanup", ("settle", "P1", "A"), "account", "after"),
    ), (goal("owner", "A", "P2"), goal("pending", "P1", "A", 0))))
    add(Scenario("held_departure_foreign_resume_before_exit", "Held departure", held(), chain(
        ("bind_failure", "P1", "failed_http"),
        ("broker_retry", "P1", "transient"), ("before", "P1"), ("after", "P1"),
        ("start", "P1", "A"), ("complete", "P1", "A"),
        ("enter", "P2", "A"), ("settle", "P2", "A"),
    ), (goal("owner", "A", "P2"), goal("alive", "P1", True),
        goal("observation", "broker_retry:P1", "authorized"),
        goal("observation", "bind_failure:P1", "rebind_blocked_with_self_lock"))))
    for outcome in ("success", "failure", "ambiguous", "ambiguous_absent"):
        add(Scenario(f"cancel_paused_completion_{outcome}", "Cancellation", (("enter", "P1", "A"),), (
            event("issue", ("start", "P1", "A", outcome)),
            event("before", ("before", "P1")),
            event("account", ("complete", "P1", "A"), "issue"),
            event("cancel", ("cancel", "P1"), "before"),
            event("restore", ("unquiesce", "P1"), "cancel"),
            event("transfer", ("settle", "P1", "A"), "restore", "account"),
            event("wake", ("wake", "P1"), "transfer"),
            event("finish", ("settle", "P1", "A"), "wake"),
        ), (goal("self", "P1", True), goal("gate", "P1", "rebind_blocked"))))
    add(Scenario("cancel_retains_original_live_holder", "Cancellation", held(), (
        event("before", ("before", "P1")),
        event("wake", ("wake", "P1")),
        event("cancel", ("cancel", "P1"), "before"),
        event("reload", ("reload", "P1")),
        event("drain", ("settle", "P1", "A"), "wake", "cancel", "reload"),
    ), (goal("owner_claim", "A", 1), goal("gate", "P1", "quiescing"))))
    add(Scenario("no_owner_transfer_resolves_and_acquires", "Retry and cleanup", (
        ("enter", "P1", "A"), ("settle", "P1", "A", "failure"),
    ), chain(("before", "P1"), ("cancel", "P1"), ("unquiesce", "P1"),
             ("settle", "P1", "A"), ("wake", "P1"), ("settle", "P1", "A")),
        (goal("self", "P1", True), goal("gate", "P1", "rebind_blocked"))))
    add(Scenario("failed_transfer_publication_retries_original_source", "Corruption", held(), chain(
        ("before", "P1"), ("cancel", "P1"), ("unquiesce", "P1"),
        ("settle", "P1", "A", "failure"), ("wake", "P1"), ("settle", "P1", "A"),
    ), (goal("owner_claim", "A", 2), goal("self", "P1", True))))
    add(Scenario("lost_source_must_not_release_retained_holder", "Cancellation", held(), chain(
        ("before", "P1"), ("cancel", "P1"), ("unquiesce", "P1"),
        ("start", "P1", "A"), ("complete", "P1", "A"),
        ("observe", "retained_holder", "owner", "A"),
        ("settle", "P1", "A"),
    ), (goal("observation", "retained_holder", "P1"), goal("self", "P1", True))))
    for outcome in ("success", "failure", "ambiguous", "ambiguous_keep"):
        add(Scenario(f"fast_A_B_A_release_{outcome}", "Fast return", held(), (
            event("before_A", ("before", "P1")),
            event("after_B", ("after", "P1", "B"), "before_A"),
            event("old_release_issue", ("start", "P1", "A", outcome), "after_B"),
            event("B_acquire", ("settle", "P1", "B"), "after_B"),
            event("before_B", ("before", "P1"), "B_acquire"),
            event("after_A", ("after", "P1", "A"), "before_B"),
            event("old_release_account", ("complete", "P1", "A"), "old_release_issue"),
            event("A_restore", ("settle", "P1", "A"), "after_A", "old_release_account"),
            event("A_wake", ("wake", "P1"), "A_restore"),
            event("A_finish", ("settle", "P1", "A"), "A_wake"),
            event("B_cleanup", ("settle", "P1", "B"), "after_A"),
        ), (goal("self", "P1", True), goal("owner_claim", "A", 2), goal("owner", "B", None))))
    add(Scenario("obsolete_cleanup_cannot_delete_new_tuple", "Fast return", held(), chain(
        ("before", "P1"), ("cancel", "P1"), ("unquiesce", "P1"), ("settle", "P1", "A"),
        ("obsolete_release", "P1", "A", 1), ("settle", "P1", "A"),
    ), (goal("owner_claim", "A", 2), goal("self", "P1", True))))
    add(Scenario("duplicate_binding_free_wakes_single_flight", "Retry and cleanup", (("enter", "P1", "A"),), (
        event("wake_one", ("wake", "P1")), event("wake_two", ("wake", "P1")),
        event("issue", ("start", "P1", "A")),
        event("account", ("complete", "P1", "A"), "issue"),
        event("drain", ("settle", "P1", "A"), "wake_one", "wake_two", "account"),
    ), (goal("self", "P1", True), goal("pending", "P1", "A", 0))))
    add(Scenario("foreign_live_uses_ownership_not_broker_retry", "Retry and cleanup", held("P2") + held(), chain(
        ("broker_retry", "P1", "must_not_exist"), ("wake", "P1"), ("settle", "P1", "A"),
        ("before", "P2"), ("after", "P2"), ("settle", "P2", "A"),
        ("wake", "P1"), ("settle", "P1", "A"), ("broker_retry", "P1", "now_allowed"),
    ), (goal("self", "P1", True), goal("observation", "broker_retry:P1", "authorized"))))
    add(Scenario("empty_claim_map_retains_lifetime_fifo", "Lifetime", held(), chain(
        ("before", "P1"), ("after", "P1"), ("settle", "P1", "A"), ("gc", "P1"),
        ("observe", "empty", "claim_count", "P1", "A"), ("reload", "P1"),
        ("enter", "P1", "A"), ("settle", "P1", "A"),
    ), (goal("observation", "empty", 0), goal("self", "P1", True))))
    add(Scenario("stop_retains_current_owner_until_os_death", "Lifetime", held(), chain(
        ("stop", "P1"), ("wake", "P1"), ("settle", "P1", "A"),
        ("observe", "stop_owner", "owner", "A"), ("enter", "P2", "A"), ("settle", "P2", "A"),
        ("death", "P1"), ("wake", "P2"), ("settle", "P2", "A"),
    ), (goal("observation", "stop_owner", "P1"), goal("owner", "A", "P2"), goal("witness", "A", "P1", True))))
    add(Scenario("reload_preserves_queued_operation_pins", "Lifetime", (("enter", "P1", "A"),), (
        event("issue", ("start", "P1", "A")), event("reload", ("reload", "P1")),
        event("account", ("complete", "P1", "A"), "issue"),
        event("gc", ("gc", "P1"), "reload"),
    ), (goal("self", "P1", True), goal("claim_count", "P1", "A", 1))))
    for kind in ("missing", "terminal"):
        add(Scenario(f"invalid_{kind}_claim_before_handler", "Corruption", held(), chain(
            ("corrupt_claim", "P1", kind), ("before", "P1"), ("after", "P1", "B"),
        ), (goal("gate", "P1", "corrupt"), goal("owner", "A", "P1"),
            goal("observation", "before:P1", "invalid_association_refused"))))
    add(Scenario("malformed_owner_fails_closed_then_inspection_recovers", "Corruption", (
        ("malformed_owner", "A"), ("enter", "P1", "A"),
    ), chain(("settle", "P1", "A"), ("observe", "unknown_self", "self", "P1"),
             ("malformed_owner", "A", False), ("wake", "P1"), ("settle", "P1", "A")),
        (goal("observation", "unknown_self", False), goal("self", "P1", True))))
    add(Scenario("two_dead_owner_contenders_guarded_compare_publish", "Corruption", held("P0") + (
        ("death", "P0"), ("enter", "P1", "A"), ("enter", "P2", "A"),
    ), (
        event("compare_one", ("compare", "P1", "A")), event("compare_two", ("compare", "P2", "A")),
        event("publish_one", ("compare_publish", "P1", "A"), "compare_one"),
        event("publish_two", ("compare_publish", "P2", "A"), "compare_two"),
        event("account_one", ("settle", "P1", "A"), "publish_one", "publish_two"),
        event("account_two", ("settle", "P2", "A"), "publish_one", "publish_two"),
    ), (goal("witness", "A", "P0", True),)))
    add(Scenario("synchronous_stall_needs_external_termination", "Adoption contention and synchronous stall", (
        ("enter", "P0", "B"), ("hold_guard", "P0", "A", True), ("enter", "P1", "A"),
    ), chain(("heartbeat", "P0"), ("stop", "P0"), ("timeout", "P1", "A"),
             ("settle", "P0", "B"), ("observe", "blocked_other_fifo", "owner", "B"),
             ("settle", "P1", "A"), ("observe", "before_death_owner", "owner", "A"),
             ("death", "P0"), ("wake", "P1"), ("settle", "P1", "A")),
        (goal("observation", "heartbeat:P0", False), goal("observation", "stop:P0", "thread_blocked"),
         goal("observation", "blocked_other_fifo", None),
         goal("observation", "before_death_owner", None), goal("self", "P1", True))))
    add(Scenario("bind_http_and_mcp_restart_cas_race", "Authority", held() + (("propose", "P1", "old"),), (
        event("old_http", ("bind_http", "old")), event("restart", ("identity", "P1", "M2", 2)),
        event("new_proposal", ("propose", "P1", "new", "M2"), "restart", "old_http"),
        event("new_http", ("bind_http", "new"), "new_proposal"),
        event("new_response", ("bind_response", "new"), "new_http"),
        event("old_response", ("bind_response", "old"), "old_http", "restart"),
        event("old_wake", ("wake", "P1", "M1"), "restart"),
    ), (goal("current_instance", "P1", "M2"), goal("gate", "P1", "open"))))
    add(Scenario("stale_orphan_cannot_refresh_after_new_bind", "Authority", bound() + (
        ("identity", "P1", "M2", 2), ("bind", "P1", "replacement", "M2"),
    ), chain(("propose", "P1", "orphan_refresh", "M1"), ("bind_http", "orphan_refresh"),
             ("bind_response", "orphan_refresh")),
        (goal("gate", "P1", "open"), goal("current_instance", "P1", "M2"))))
    add(Scenario("stale_bind_completion_after_departure", "Authority", held() + (("propose", "P1", "departing"),), (
        event("before", ("before", "P1")), event("http", ("bind_http", "departing")),
        event("after", ("after", "P1"), "before"),
        event("response", ("bind_response", "departing"), "http", "before"),
        event("cleanup", ("settle", "P1", "A"), "after", "response"),
    ), (goal("gate", "P1", None), goal("owner", "A", None))))
    add(Scenario("direct_child_ordering_and_redirector_refusal", "Interpreter launch", held(), chain(
        ("identity", "P1", "M2", 2), ("identity", "P1", "M1", 1),
        ("identity", "P1", "redirector_grandchild", 3, False),
        ("identity", "P1", "same_birth_changed_pair", 2),
        ("identity", "P1", "dead_child", 4, True, False),
        ("identity", "P1", "wrong_start", 5, True, True, False), ("reload", "P1"),
    ), (goal("current_instance", "P1", "M2"),
        goal("observation", "identity:P1:M1", "refused"),
        goal("observation", "identity:P1:redirector_grandchild", "refused"))))
    add(Scenario("bound_restoration_restart_stays_event_fenced", "Cancellation", bound(), chain(
        ("before", "P1"), ("cancel", "P1"), ("unquiesce", "P1"), ("before", "P1"),
        ("identity", "P1", "M2", 2), ("settle", "P1", "A"), ("wake", "P1", "M2"),
        ("settle", "P1", "A"), ("observe", "restored_fence", "gate", "P1"),
        ("bind", "P1", "refresh", "M2"),
    ), (goal("observation", "before:P1", "bound_restoration_refused"),
        goal("observation", "restored_fence", "event_fenced"), goal("gate", "P1", "open"))))
    add(Scenario("duplicate_receipt_exclusive_creation_and_reload", "Delivery", bound() + (("offer",),), (
        event("notification_one", ("notify", "P1")), event("notification_two", ("notify", "P1")),
        event("reload", ("reload", "P1")),
        event("late_duplicate", ("notify", "P1"), "notification_one", "notification_two"),
    ), (goal("sends", "P1", "1", 1), goal("reservations", "P1", "1", 1))))
    for failure in ("absent", "malformed", "valid"):
        add(Scenario(f"receipt_failure_{failure}_fences_before_recovery", "Delivery", bound() + (("offer",),), chain(
            ("notify", "P1", "1", failure), ("notify", "P1"),
            ("terminal", "P1", "1", "rejected"), ("status_tick", "P1"),
        ), (goal("sends", "P1", "1", 0),
            goal("gate", "P1", "event_fenced" if failure == "absent" else "open"))))
    for winner in ("accepted", "rejected"):
        other = "rejected" if winner == "accepted" else "accepted"
        add(Scenario(f"event_fenced_terminal_{winner}_winner", "Delivery", bound() + (
            ("offer",), ("notify", "P1"), ("write_jsonl", "1"), ("scan", "P1", "1", "receipt"),
        ), chain(("terminal", "P1", "1", winner), ("terminal", "P1", "1", other),
                 ("status_tick", "P1"), ("before", "P1"),
                 ("observe", "before_ack", "gate", "P1"), ("ack", "P1")),
            (goal("offer", "1", "state", winner), goal("mirror", "1", "ack", winner == "accepted"),
             goal("observation", "before_ack", "event_fenced" if winner == "accepted" else "quiescing"))))
    add(Scenario("acceptance_rejection_cas_causal_race", "Delivery", bound() + (
        ("offer",), ("notify", "P1"), ("write_jsonl", "1"), ("scan", "P1", "1", "receipt"),
    ), (
        event("accept", ("terminal", "P1", "1", "accepted")),
        event("reject", ("terminal", "P1", "1", "rejected")),
        event("reconcile", ("status_tick", "P1"), "accept", "reject"),
        event("ack", ("ack", "P1"), "reconcile"),
    ), (goal("gate", "P1", "open"), goal("sends", "P1", "1", 1))))
    add(Scenario("newline_receipt_and_explicit_ack_boundaries", "Delivery", bound() + (("offer",), ("notify", "P1")), chain(
        ("write_jsonl", "1", "unterminated"), ("scan", "P1", "1", "unterminated"),
        ("write_jsonl", "1"), ("scan", "P1", "1", "newline"),
        ("terminal", "P1", "1", "accepted"), ("status_tick", "P1"),
        ("ack", "P1", "1", "inbox_empty"), ("ack", "P1", "1", "cursor"),
        ("observe", "unproved_ack", "mirror", "1", "ack"),
        ("broker_ack_only", "P1"), ("status_tick", "P1"),
    ), (goal("observation", "unterminated", False), goal("observation", "newline", True),
        goal("observation", "unproved_ack", False), goal("mirror", "1", "ack", True))))
    for variant in ("wrong_header", "wrong_role", "wrong_content"):
        add(Scenario(f"receipt_rejects_{variant}", "Delivery", bound() + (("offer",), ("notify", "P1")), chain(
            ("write_jsonl", "1", variant), ("scan", "P1", "1", variant), ("terminal", "P1", "1", "accepted"),
        ), (goal("observation", variant, False), goal("offer", "1", "state", "offered"))))
    for with_entry in (True, False):
        seed = bound() + (("offer",), ("notify", "P1"), ("damage_mirror", "1", "malformed"))
        if not with_entry:
            seed += (("remove_entry", "P1", "1"),)
        add(Scenario(f"malformed_terminal_recovery_entry_{with_entry}", "Delivery", seed, chain(
            ("status_tick", "P1"), ("terminal", "P1", "1", "rejected"), ("status_tick", "P1"),
        ), (goal("mirror", "1", "terminal", "rejected"), goal("gate", "P1", "open"),
            goal("sends", "P1", "1", 1))))
    add(Scenario("malformed_entry_token_mismatch_refuses_broker_token", "Delivery", bound() + (
        ("offer",), ("notify", "P1"), ("damage_mirror", "1", "entry_token"),
    ), chain(("terminal", "P1", "1", "rejected"), ("status_tick", "P1")),
        (goal("mirror", "1", "terminal", "none"), goal("gate", "P1", "event_fenced"))))
    add(Scenario("adoption_contention_status_tick_same_reservation", "Adoption contention and synchronous stall", adoption_seed(), chain(
        ("hold_guard", "P2", "A"), ("status_tick", "P1"),
        ("observe", "reserved_no_send", "sends", "P1", "1"), ("release_guard", "P2", "A"),
        ("status_tick", "P1"), ("status_tick", "P1"),
    ), (goal("observation", "reserved_no_send", 0), goal("sends", "P1", "1", 1),
        goal("reservations", "P1", "1", 1), goal("status_reads", 3))))
    add(Scenario("adoption_temp_failure_preserves_reservation", "Adoption contention and synchronous stall", adoption_seed(), chain(
        ("status_tick", "P1", "1", "temp"), ("status_tick", "P1"), ("status_tick", "P1"),
    ), (goal("sends", "P1", "1", 1), goal("reservations", "P1", "1", 1))))
    for failure in ("ambiguous_old", "ambiguous_new"):
        add(Scenario(f"adoption_{failure}_zero_send_until_terminal", "Adoption contention and synchronous stall", adoption_seed(), chain(
            ("status_tick", "P1", "1", failure), ("status_tick", "P1"), ("reload", "P1"),
            ("status_tick", "P1"), ("terminal", "P1", "1", "rejected"), ("status_tick", "P1"),
        ), (goal("sends", "P1", "1", 0), goal("mirror", "1", "terminal", "rejected"),
            goal("gate", "P1", "open"))))
    add(Scenario("adoption_marker_found_never_resends", "Delivery", adoption_seed() + (("write_jsonl", "1"),), chain(
        ("status_tick", "P1"), ("terminal", "P1", "1", "accepted"),
        ("status_tick", "P1"), ("ack", "P1"),
    ), (goal("sends", "P1", "1", 0), goal("mirror", "1", "ack", True), goal("gate", "P1", "open"))))
    for variant in ("unterminated", "wrong_role", "wrong_header", "wrong_content"):
        add(Scenario(f"adoption_rejects_{variant}_receipt", "Delivery", bound("P0", attempt="old") + (
            ("offer",), ("notify", "P0"), ("write_jsonl", "1", variant), ("death", "P0"),
            ("enter", "P1", "A"), ("settle", "P1", "A"), ("bind", "P1", "new"),
        ), chain(("status_tick", "P1"), ("status_tick", "P1"),
                 ("terminal", "P1", "1", "accepted")),
            (goal("sends", "P1", "1", 1), goal("offer", "1", "state", "offered"),
             goal("mirror", "1", "state", "received"), goal("mirror", "1", "application", False),
             goal("mirror", "1", "ack", False))))
    add(Scenario("prior_application_acceptance_adopts_before_cas", "Delivery", bound("P0", attempt="old") + (
        ("offer",), ("notify", "P0"), ("write_jsonl", "1"), ("scan", "P0", "1", "old_receipt"),
        ("death", "P0"), ("enter", "P1", "A"), ("settle", "P1", "A"), ("bind", "P1", "new"),
    ), chain(("status_tick", "P1"), ("terminal", "P1", "1", "accepted"),
             ("status_tick", "P1"), ("ack", "P1")),
        (goal("sends", "P1", "1", 0), goal("mirror", "1", "ack", True))))
    add(Scenario("model_and_exact_tui_privilege_boundaries", "Authority", bound() + (("offer",), ("notify", "P1")), chain(
        ("command", "P1", "send", "model_send"), ("command", "P1", "register", "model_register"),
        ("command", "P1", "name", "model_name"), ("command", "P1", "takeover", "takeover"),
        ("command", "P1", "reject", "rpc_reject", "rpc"),
        ("command", "P1", "reject", "subagent_reject", "tui", False),
        ("command", "P1", "reject", "unsigned_reject", "tui", True, True, True, False),
        ("command", "P1", "reject", "no_ui_reject", "tui", True, False),
        ("command", "P1", "reject", "unconfirmed_reject", "tui", True, True, False),
        ("command", "P1", "reject", "tui_reject"),
    ), tuple(goal("observation", label, label == "tui_reject") for label in (
        "model_send", "model_register", "model_name", "takeover", "rpc_reject", "subagent_reject",
        "unsigned_reject", "no_ui_reject", "unconfirmed_reject", "tui_reject"))))
    add(Scenario("operator_idempotency_method_route_path_body", "Authority", bound() + (("offer",),), chain(
        ("operator", "request"), ("operator", "request"),
        ("observe", "replayed", "observation", "operator:request:last"),
        ("operator", "request", "POST", "unbound"),
        ("observe", "route_conflict", "observation", "operator:request:last"),
        ("operator", "request", "POST", "bound", "B"),
        ("observe", "path_conflict", "observation", "operator:request:last"),
        ("operator", "request", "POST", "bound", "A", "PERSISTENCE_UNRECOVERABLE"),
        ("observe", "body_conflict", "observation", "operator:request:last"),
        ("operator", "request", "PUT"),
    ), (goal("notice_count", 1), goal("observation", "replayed", "REJECTED"),
        goal("observation", "route_conflict", "REQUEST_CONFLICT"), goal("observation", "path_conflict", "REQUEST_CONFLICT"),
        goal("observation", "body_conflict", "REQUEST_CONFLICT"), goal("observation", "operator:request:last", "REQUEST_CONFLICT"))))
    # Longer histories are explicit linear traces, not depth-12 exhaustive claims.
    add(Scenario("four_claims_two_restorations_full_source_chain", "Cancellation", held(), chain(
        ("before", "P1"), ("cancel", "P1"), ("unquiesce", "P1"),
        ("before", "P1"), ("cancel", "P1"), ("unquiesce", "P1"),
        ("before", "P1"), ("cancel", "P1"), ("unquiesce", "P1"),
        ("observe", "four_claims", "claim_count", "P1", "A"),
        ("settle", "P1", "A"), ("wake", "P1"), ("settle", "P1", "A"),
        ("gc", "P1"), ("before", "P1"), ("after", "P1"),
        ("settle", "P1", "A"), ("gc", "P1"), ("enter", "P2", "A"), ("settle", "P2", "A"),
    ), (goal("observation", "four_claims", 4), goal("owner", "A", "P2"), goal("claim_count", "P1", "A", 0)), "named_long_trace"))
    add(Scenario("three_process_transitive_witness_adoption_chain", "Lifetime", adoption_seed(), chain(
        ("death", "P1"), ("enter", "P2", "A"), ("settle", "P2", "A"),
        ("bind", "P2", "third"), ("status_tick", "P2"), ("status_tick", "P2"),
        ("write_jsonl", "1"), ("scan", "P2", "1", "receipt"),
        ("terminal", "P2", "1", "accepted"), ("status_tick", "P2"), ("ack", "P2"),
        ("before", "P2"), ("after", "P2"), ("settle", "P2", "A"), ("gc", "P2"),
    ), (goal("sends", "P2", "1", 1), goal("witness", "A", "P0", True),
        goal("witness", "A", "P1", True), goal("owner", "A", None), goal("mirror", "1", "ack", True)), "named_long_trace"))
    add(Scenario("ambiguous_adoption_requires_full_process_restart", "Delivery", adoption_seed(), chain(
        ("status_tick", "P1", "1", "ambiguous_new"), ("status_tick", "P1"),
        ("identity", "P1", "M2", 2), ("bind", "P1", "mcp_only", "M2"), ("status_tick", "P1"),
        ("death", "P1"), ("enter", "P2", "A"), ("settle", "P2", "A"),
        ("bind", "P2", "restart"), ("status_tick", "P2"), ("status_tick", "P2"),
        ("write_jsonl", "1"), ("scan", "P2", "1", "receipt"),
        ("terminal", "P2", "1", "accepted"), ("status_tick", "P2"), ("ack", "P2"),
    ), (goal("sends", "P1", "1", 0), goal("sends", "P2", "1", 1), goal("mirror", "1", "ack", True)), "named_long_trace"))
    add(Scenario("unquiesce_requires_exact_tui_after_command_return", "Authority", held(), chain(
        ("before", "P1"), ("unquiesce", "P1"), ("observe", "still_waiting", "gate", "P1"),
        ("cancel", "P1"), ("unquiesce", "P1", "rpc"),
        ("observe", "rpc_closed", "gate", "P1"), ("unquiesce", "P1"), ("settle", "P1", "A"),
    ), (goal("observation", "still_waiting", "quiescing"), goal("observation", "rpc_closed", "quiescing"),
        goal("self", "P1", True))))
    add(Scenario("corruption_discovered_after_switch_keeps_new_session_closed", "Corruption", held(), chain(
        ("corrupt_claim", "P1", "missing"), ("host_switched_after_corruption", "P1", "B"),
        ("settle", "P1", "A"), ("wake", "P1"), ("settle", "P1", "B"),
        ("bind", "P1", "invalid_association"),
    ), (goal("gate", "P1", "corrupt"), goal("owner", "A", None), goal("owner", "B", None))))
    add(Scenario("absence_is_not_death_proof_but_terminal_recovery_is_allowed", "Delivery", bound("P0", attempt="old") + (
        ("offer",), ("notify", "P0"), ("death", "P0"), ("absent_owner_record", "A"),
        ("enter", "P1", "A"), ("settle", "P1", "A"), ("bind", "P1", "new"),
    ), chain(("status_tick", "P1"), ("observe", "no_witness_send", "sends", "P1", "1"),
             ("terminal", "P1", "1", "rejected"), ("status_tick", "P1")),
        (goal("observation", "no_witness_send", 0), goal("witness", "A", "P0", False),
         goal("sends", "P1", "1", 0), goal("mirror", "1", "terminal", "rejected"))))
    return result


CONTROL_SCENARIOS = {
    "stale_completion_retirement": "cancel_paused_completion_success",
    "lost_source_retention": "lost_source_must_not_release_retained_holder",
    "incomplete_tuple_delete": "obsolete_cleanup_cannot_delete_new_tuple",
    "fifo_reset": "held_departure_foreign_resume_before_exit",
    "unguarded_compare_mutate": "two_dead_owner_contenders_guarded_compare_publish",
    "stale_orphan_bind_refresh": "stale_orphan_cannot_refresh_after_new_bind",
    "lost_transitive_witnesses": "three_process_transitive_witness_adoption_chain",
    "epoch_only_adoption": "adoption_contention_status_tick_same_reservation",
    "stuck_no_owner_transfer": "no_owner_transfer_resolves_and_acquires",
    "timeout_theft": "synchronous_stall_needs_external_termination",
    "ambiguous_publication_send_regeneration": "adoption_ambiguous_old_zero_send_until_terminal",
    "competing_adoption_reservation": "adoption_contention_status_tick_same_reservation",
    "consumed_send_replay": "adoption_contention_status_tick_same_reservation",
    "marker_only_adoption_receipt": "adoption_rejects_unterminated_receipt",
}


def explore(scenario: Scenario, mutant: str | None = None) -> dict:
    require(scenario.mode != "causal_permutations" or len(scenario.events) <= BOUND,
            "declared_depth_bound", scenario.name)
    names = {e.name for e in scenario.events}
    require(len(names) == len(scenario.events), "unique_event_names", scenario.name)
    require(all(set(e.after) <= names for e in scenario.events), "closed_causal_dependencies", scenario.name)
    stats = {"scenario": scenario.name, "group": scenario.group, "mode": scenario.mode,
             "seed_events": len(scenario.seed), "scheduled_events": len(scenario.events),
             "complete_schedules": 0, "visited_prefixes": 0, "distinct_states": 0,
             "max_depth": 0, "max_internal_steps": 0, "peak_claims_per_process_session": 0,
             "max_status_reads": 0, "goals": [normalized(g) for g in scenario.goals]}
    fingerprints = set()
    failure = None
    world = World(mutant)
    seed_trace = []
    try:
        for action in scenario.seed:
            seed_trace.append("seed:" + repr(action))
            world.apply(action)
    except Violation as error:
        failure = {"rule": error.rule, "detail": normalized(error.detail), "trace": seed_trace, "state": world.view()}
    if failure is None:
        stack = [(world, frozenset(), [])]
        while stack:
            state, done, trace = stack.pop()
            stats["visited_prefixes"] += 1
            fingerprints.add(state.fingerprint())
            stats["max_depth"] = max(stats["max_depth"], len(trace))
            stats["max_internal_steps"] = max(stats["max_internal_steps"], state.internal_steps)
            stats["peak_claims_per_process_session"] = max(stats["peak_claims_per_process_session"], state.peak_claims_per_session)
            stats["max_status_reads"] = max(stats["max_status_reads"], state.status_reads)
            if len(done) == len(scenario.events):
                try:
                    for expected in scenario.goals:
                        expected.check(state)
                    stats["complete_schedules"] += 1
                    if stats["complete_schedules"] == 1:
                        stats["example_schedule"] = seed_trace + trace
                        stats["observed_goals"] = [
                            {"subject": g.subject, "args": normalized(g.args),
                             "actual": normalized(state.value(g.subject, *g.args))}
                            for g in scenario.goals]
                except Violation as error:
                    failure = {"rule": error.rule, "detail": normalized(error.detail),
                               "trace": seed_trace + trace, "state": state.view()}
                    break
                continue
            enabled = [e for e in scenario.events if e.name not in done and set(e.after) <= done]
            require(bool(enabled), "acyclic_scenario_dag", scenario.name)
            # Reverse push preserves deterministic declaration-order DFS.
            for e in reversed(enabled):
                successor = copy.deepcopy(state)
                next_trace = trace + [e.name + ":" + repr(e.action)]
                try:
                    successor.apply(e.action)
                except Violation as error:
                    failure = {"rule": error.rule, "detail": normalized(error.detail),
                               "trace": seed_trace + next_trace, "state": successor.view()}
                    break
                stack.append((successor, done | {e.name}, next_trace))
            if failure:
                break
    stats["distinct_states"] = len(fingerprints)
    stats["passed"] = failure is None
    if failure:
        stats["counterexample"] = failure
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="Emit actual bounded exploration and mutation evidence as JSON")
    args = parser.parse_args()
    definitions = scenarios()
    positives = [explore(s) for s in definitions]
    by_name = {s.name: s for s in definitions}
    controls = []
    for mutant in MUTANTS:
        evidence = explore(by_name[CONTROL_SCENARIOS[mutant]], mutant)
        controls.append({"mutant": mutant, "killed": not evidence["passed"], "evidence": evidence})
    passed = all(row["passed"] for row in positives) and all(row["killed"] for row in controls)
    report = {
        "specification": "R0B bounded lifecycle/entry specification", "passed": passed,
        "bounds": {"causal_scenario_depth": BOUND, "processes": 3, "sessions": 2,
                   "admitted_mcp_instances_per_process": 2, "declared_claim_bound_per_process_session": 4,
                   "actual_max_claims_per_process_session": max(r["peak_claims_per_process_session"] for r in positives),
                   "max_scheduled_depth": max(r["max_depth"] for r in positives),
                   "scope": "all causal linearizations of each listed bounded DAG; listed longer linear traces only",
                   "state_fingerprint": "complete model state including operation and observation audit histories",
                   "atomicity": "synchronous guarded physical mutation; separate issued-operation accounting callback",
                   "settle": "finite FIFO drain; internal steps checked and counted, not independently permuted"},
        "totals": {"positive_scenarios": len(positives), "complete_schedules": sum(r["complete_schedules"] for r in positives),
                   "visited_prefixes": sum(r["visited_prefixes"] for r in positives),
                   "distinct_states_summed_by_scenario": sum(r["distinct_states"] for r in positives),
                   "required_mutants": len(MUTANTS), "killed_mutants": sum(r["killed"] for r in controls)},
        "scenario_coverage": {group: [r["scenario"] for r in positives if r["group"] == group]
                              for group in sorted({r["group"] for r in positives})},
        "positive_scenarios": positives, "mutation_controls": controls,
        "modeled": ["full owner tuples and transitive dead witnesses", "independent claims, pins, source lineage and GC",
                    "advancing process/session lifetime FIFO", "publication versus demand versus physical accounting",
                    "bound/unbound restoration and MCP CAS currentness", "status-tick-driven reserved adoption and failure partition",
                    "exclusive receipt/send, exact newline JSONL, terminal winners and explicit identity ack",
                    "exact TUI versus model privileges and scoped operator idempotency"],
        "excluded": ["relay runtime implementation or integration", "native Windows ABI and real process liveness proof",
                     "real filesystem guards, links, flush/replace behavior and power loss", "HTTP/SSE/broker SQLite transactions",
                     "signature/credential cryptography and arbitrary privileged-shell attacks", "actual OMP host queue/JSONL writer",
                     "unbounded schedules, arbitrary corruption and fairness without eventual successful I/O"],
    }
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        totals = report["totals"]
        print(f"R0B bounded model: {'PASS' if passed else 'FAIL'}")
        print(f"{totals['positive_scenarios']} scenarios; {totals['complete_schedules']} complete schedules; "
              f"{totals['visited_prefixes']} prefixes; {totals['distinct_states_summed_by_scenario']} scenario-local states")
        print(f"Depth <= {BOUND} causal DAGs plus named longer traces; "
              f"mutants killed {totals['killed_mutants']}/{totals['required_mutants']}")
        for row in positives:
            print(f"  {'PASS' if row['passed'] else 'FAIL'} {row['scenario']}: "
                  f"schedules={row['complete_schedules']} states={row['distinct_states']} depth={row['max_depth']}")
            if not row["passed"]:
                print(json.dumps(row["counterexample"], sort_keys=True))
        for row in controls:
            rule = row["evidence"].get("counterexample", {}).get("rule", "SURVIVED")
            print(f"  {'KILLED' if row['killed'] else 'SURVIVED'} {row['mutant']}: {rule}")
        print("Design evidence only; use --json for bounds, scenarios and observable counterexamples.")
    return 0 if passed else 1





def test_reviewed_bounded_lifecycle_model_and_broken_rule_controls():
    definitions = scenarios()
    positives = [explore(scenario) for scenario in definitions]
    by_name = {scenario.name: scenario for scenario in definitions}
    controls = {
        mutant: explore(by_name[CONTROL_SCENARIOS[mutant]], mutant)
        for mutant in MUTANTS
    }
    assert all(row["passed"] for row in positives)
    assert all(not row["passed"] for row in controls.values())
    assert len(positives) == 66
    assert sum(row["complete_schedules"] for row in positives) == 396
    assert sum(row["visited_prefixes"] for row in positives) == 1930
    assert sum(row["distinct_states"] for row in positives) == 872
    assert max(row["max_depth"] for row in positives) == 20
    assert max(row["peak_claims_per_process_session"] for row in positives) <= 4
    assert len(controls) == 14
    assert {row["group"] for row in positives} == {
        "Adoption contention and synchronous stall", "Authority", "Cancellation",
        "Corruption", "Delivery", "Fast return", "Held departure",
        "Interpreter launch", "Lifetime", "Pending acquisition", "Retry and cleanup",
    }

# END REVIEWED R0B BOUNDED LIFECYCLE MODEL
