"""omp relay roots come from the `omp-relay` entry omp launched, never from omp's own environment.

Acceptance cases for plan `omp-config-root-discovery` Revision 2 and its root-change guard
(broker project CONSTRAINTS C-68). Every case runs the real TypeScript extension under Bun with
disposable roots. The omp MCP manager is supplied through `__testing.setMcpManager`, except in
the final smoke, which loads omp's real `@oh-my-pi/pi-coding-agent/mcp` module and a real
`mcp.json` in a disposable agent directory.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
EXTENSION = ROOT / "harness" / "omp" / "omp_relay_extension.ts"
NODE_PATH = Path(__import__("os").environ["BROKER_TEST_OMP_NODE_MODULES"])
OMP_MCP_MODULE = NODE_PATH / "@oh-my-pi" / "pi-coding-agent" / "src" / "mcp" / "index.ts"
OMP_SESSION_MODULE = NODE_PATH / "@oh-my-pi" / "pi-coding-agent" / "src" / "session" / "session-manager.ts"

SESSION = "0197f0c2-552b-7db4-8d43-6c338d7d3021"
SECOND_SESSION = "1197f0c2-552b-7db4-8d43-6c338d7d3021"
BINDING = "66666666-6666-4666-8666-666666666666"
SECOND_BINDING = "67676767-6767-4767-8767-676767676767"

IDENTITY_REFUSAL_CODES = {
    "MODE_UNSUPPORTED",
    "SESSION_ALREADY_OWNED",
    "IDENTITY_INVALID",
    "CONTROL_CHANNEL_INVALID",
    "BROKER_CAPABILITY_MISSING",
    "DELIVERY_PENDING",
}
IDENTITY_REFUSAL_FIELDS = {"schema_version", "ok", "instance_id", "control_epoch", "bind_attempt_id", "code"}


HARNESS = r"""
import { appendFileSync, existsSync, mkdirSync, readFileSync, readdirSync, realpathSync, writeFileSync } from "node:fs";
import * as path from "node:path";

const scenario = process.env.CASE!;
const base = process.env.BASE!;
const stateRoot = path.join(base, "state");
const runtimeRoot = path.join(base, "runtime");
const otherState = path.join(base, "state-b");
const otherRuntime = path.join(base, "runtime-b");
const agentDir = path.join(base, "agent");
for (const directory of [stateRoot, runtimeRoot, otherState, otherRuntime, agentDir]) mkdirSync(directory, { recursive: true });
delete process.env.OMP_RELAY_STATE_ROOT;
delete process.env.OMP_RELAY_RUNTIME_ROOT;
if (scenario === "parent-env-ignored") {
  process.env.OMP_RELAY_STATE_ROOT = otherState;
  process.env.OMP_RELAY_RUNTIME_ROOT = otherRuntime;
}
if (scenario.startsWith("env-variable-name")) process.env[stateRoot] = otherState;

const relay = await import(process.env.EXTENSION_URL!);
relay.__testing.reset();

const SESSION = "0197f0c2-552b-7db4-8d43-6c338d7d3021";
const SECOND_SESSION = "1197f0c2-552b-7db4-8d43-6c338d7d3021";
const INSTANCE = "33333333-3333-4333-8333-333333333333";
const SECOND_INSTANCE = "34343434-3434-4434-8434-343434343434";
const CONTROL_EPOCH = "44444444-4444-4444-8444-444444444444";
const ATTEMPT = "55555555-5555-4555-8555-555555555555";
const SECOND_ATTEMPT = "56565656-5656-4656-8656-565656565656";
const BINDING = "66666666-6666-4666-8666-666666666666";
const SECOND_BINDING = "67676767-6767-4767-8767-676767676767";
const BOOT = 52;
const START_MS = 1780001000000;
const OMP_WITNESS = { pid: process.pid, start_key: "8001", sequence_number: "6001", boot_id: BOOT };
const CHILDREN = new Map([
  [901, { pid: 901, start_key: "9001", sequence_number: "7001", boot_id: BOOT }],
  [902, { pid: 902, start_key: "9002", sequence_number: "7002", boot_id: BOOT }],
]);
relay.__testing.setProcessQuery((pid) => {
  if (pid === process.pid) return { witness: OMP_WITNESS, parent_pid: 77, start_ms: START_MS, alive: true };
  const child = CHILDREN.get(pid);
  if (child) return { witness: child, parent_pid: process.pid, start_ms: START_MS + 1, alive: true };
  return { witness: { pid, start_key: "1", sequence_number: "1", boot_id: BOOT }, parent_pid: 1, start_ms: 1, alive: false };
});

// The declaration omp would have stored for `omp-relay`, in each form under test.
const EXE = process.execPath;
const batchFile = path.join(base, "launch.cmd");
writeFileSync(batchFile, "@echo off\r\n");
const args = (state, runtime) => ["omp_relay_mcp.py", "--state-root", state, "--runtime-root", runtime];
const envRoots = (state, runtime) => ({ OMP_RELAY_STATE_ROOT: state, OMP_RELAY_RUNTIME_ROOT: runtime });
const PLACEHOLDER = "$" + "{HOME}";
const ENTRIES = {
  "form-env": { command: EXE, args: ["omp_relay_mcp.py"], env: envRoots(stateRoot, runtimeRoot) },
  "form-both": { command: EXE, args: args(stateRoot, runtimeRoot), env: envRoots(stateRoot, runtimeRoot) },
  "form-mixed": { command: EXE, args: ["omp_relay_mcp.py", "--state-root", stateRoot], env: { OMP_RELAY_RUNTIME_ROOT: runtimeRoot } },
  "root-in-neither": { command: EXE, args: ["omp_relay_mcp.py", "--state-root", stateRoot] },
  "non-stdio": { type: "http", url: "http://127.0.0.1:9/mcp", command: EXE, args: args(stateRoot, runtimeRoot) },
  "equals-form": { command: EXE, args: ["omp_relay_mcp.py", `--state-root=${stateRoot}`, "--runtime-root", runtimeRoot] },
  "abbreviation": { command: EXE, args: ["omp_relay_mcp.py", "--state", stateRoot, "--runtime-root", runtimeRoot] },
  "repeated": { command: EXE, args: ["omp_relay_mcp.py", "--state-root", stateRoot, "--state-root", stateRoot, "--runtime-root", runtimeRoot] },
  "end-of-options": { command: EXE, args: ["omp_relay_mcp.py", "--", "--state-root", stateRoot, "--runtime-root", runtimeRoot] },
  "missing-value": { command: EXE, args: ["omp_relay_mcp.py", "--runtime-root", runtimeRoot, "--state-root"] },
  "non-string": { command: EXE, args: ["omp_relay_mcp.py", "--runtime-root", runtimeRoot], env: { OMP_RELAY_STATE_ROOT: 7 } },
  "placeholder": { command: EXE, args: args(path.join(PLACEHOLDER, "state"), runtimeRoot) },
  "batch-command": { command: batchFile, args: args(stateRoot, runtimeRoot) },
  "relative-command": { command: "bun", args: args(stateRoot, runtimeRoot) },
  "absent-command": { command: path.join(base, "absent.exe"), args: args(stateRoot, runtimeRoot) },
  "env-command": { command: EXE, args: ["omp_relay_mcp.py", "--runtime-root", runtimeRoot], env: { OMP_RELAY_STATE_ROOT: `!echo ${stateRoot}` } },
  "env-variable-name": { command: EXE, args: ["omp_relay_mcp.py", "--runtime-root", runtimeRoot], env: { OMP_RELAY_STATE_ROOT: stateRoot } },
  "env-variable-name-literal": { command: EXE, args: ["omp_relay_mcp.py", "--runtime-root", runtimeRoot], env: { OMP_RELAY_STATE_ROOT: stateRoot }, envPolicy: "literal" },
  "env-variable-name-literal-key": { command: EXE, args: ["omp_relay_mcp.py", "--runtime-root", runtimeRoot], env: { OMP_RELAY_STATE_ROOT: stateRoot }, envLiteralKeys: ["OMP_RELAY_STATE_ROOT"] },
  "env-empty": { command: EXE, args: ["omp_relay_mcp.py", "--runtime-root", runtimeRoot], env: { OMP_RELAY_STATE_ROOT: "" } },
  "conflict": { command: EXE, args: args(stateRoot, runtimeRoot), env: { OMP_RELAY_STATE_ROOT: otherState } },
  "state-invalid": { command: EXE, args: args(path.join(base, "absent-state"), runtimeRoot) },
  "runtime-invalid": { command: EXE, args: args(stateRoot, "runtime") },
};
const ABSENT = new Set(["held-pre-bind-completed", "held-pre-bind-cancelled", "window-fence-switch", "window-fence-branch", "window-fence-tree", "window-cancelled-held", "missing-rpc", "tui-no-entry-no-event", "tui-connecting-without-relay", "tui-late-entry", "during-switch", "during-branch", "during-tree", "during-shutdown"]);
let entry = ABSENT.has(scenario) ? undefined : (ENTRIES[scenario] ?? { command: EXE, args: args(stateRoot, runtimeRoot) });
let connection = scenario === "rpc-pending" || scenario === "tui-late-entry" ? "connecting" : scenario === "failed-launch" ? "disconnected" : "connected";
const listeners = new Set();
let connectionObject = { child: 1 };
const manager = {
  getConnection(name) { return name === "omp-relay" && entry && connection === "connected" ? connectionObject : undefined; },
  getServerConfig(name) { return name === "omp-relay" ? entry : undefined; },
  getSource(name) {
    return name === "omp-relay" && entry ? { provider: "test", providerName: "test", path: path.join(agentDir, "mcp.json"), level: "user" } : undefined;
  },
  getConnectionStatus(name) { return name === "omp-relay" && entry ? connection : "disconnected"; },
  addConnectionStatusListener(listener) { listeners.add(listener); return () => listeners.delete(listener); },
};
// The kind of the status line now; tests assert kinds, never wording.
const shown = () => statuses.at(-1)?.kind ?? null;
async function statusEvent(event) {
  for (const listener of [...listeners]) listener(event);
  await Bun.sleep(10);
}

const handlers = new Map();
const commands = new Map();
const statuses = [];
const notices = [];
const logs = [];
// Delivery witnesses, kept apart: `sent` records only that omp's sendUserMessage accepted the call
// (queue admission; its resolved Promise proves nothing more). Journal receipt is a user line the
// test itself appends to the session file. Internal ACK is the child's offer_accepted and
// identity_acked controls. Each is read back only through the mirror file and the owner file.
const sent = [];
const OFFER = "77777777-7777-4777-8777-777777777777";
const journalFor = session => path.join(base, `${session}.jsonl`);
for (const session of [SESSION, SECOND_SESSION]) {
  writeFileSync(journalFor(session), JSON.stringify({ type: "session", version: 3, id: session, timestamp: new Date().toISOString(), cwd: base }) + "\n");
}
const record = level => (message, detail) => logs.push({ level, message, detail: detail ?? null });
const api = {
  on(name, fn) { const values = handlers.get(name) ?? []; values.push(fn); handlers.set(name, values); },
  registerCommand(name, command) { commands.set(name, command); },
  getActiveTools() { return []; },
  async setActiveTools() {},
  getAllTools() { return []; },
  async sendUserMessage(text, options) { sent.push({ text, options }); },
  logger: { info: record("info"), warn: record("warn"), error: record("error"), debug: record("debug") },
  pi: { getAgentDir() { return agentDir; } },
};
// Timer callbacks the extension registers; none runs unless a case runs it.
const timers = [];
const tui = scenario.startsWith("late-") || scenario.startsWith("f2-") || scenario.startsWith("fence-") || scenario.startsWith("retained-") || scenario.startsWith("held-") || scenario.startsWith("window-") || scenario.startsWith("tui-") || scenario.startsWith("during-") || scenario === "subagent-waiting" || scenario === "change-before-unquiesce";
const ctx = {
  agent: { kind: "main" },
  mode: tui ? "tui" : "rpc",
  hasUI: true,
  sessionManager: {
    getSessionId() { return SESSION; },
    getSessionFile() { return journalFor(this.getSessionId()); },
    getSessionName() { return "config-roots"; },
  },
  setTimeout(fn) { timers.push(fn); return 0; },
  abort() {},
  ui: {
    setStatus(key, text) { statuses.push({ key, text, kind: relay.__testing.getGlobalState().relay_status_kind ?? null }); },
    notify(message, level) { notices.push({ message, level }); },
    async confirm() { return true; },
  },
};
const sub = { ...ctx, agent: { kind: "sub" } };
const secondCtx = { ...ctx, sessionManager: { ...ctx.sessionManager, getSessionId() { return SECOND_SESSION; } } };

async function emit(name, event = {}, context = ctx) {
  const out = [];
  for (const fn of handlers.get(name) ?? []) out.push(await fn(event, context));
  return out;
}
function control(params, context = ctx) {
  return emit("mcp_notification", { server: "omp-relay", method: "notifications/omp/relay/control", params }, context);
}
async function requestIdentity({ pid = 901, instance = INSTANCE, attempt = ATTEMPT, binding = BINDING, runtime = runtimeRoot, context = ctx } = {}) {
  const rdz = path.join(runtime, `mcp-${instance}`);
  mkdirSync(path.join(rdz, "control", "requests"), { recursive: true });
  mkdirSync(path.join(rdz, "control", "results"), { recursive: true });
  await control({
    schema_version: 1,
    type: "identity_request",
    instance_id: instance,
    control_epoch: CONTROL_EPOCH,
    mcp_process: CHILDREN.get(pid),
    rendezvous_dir: rdz,
    bind_attempt_id: attempt,
    new_binding_epoch: binding,
    expected_binding_epoch: null,
    control_token_hash: "a".repeat(64),
  }, context);
  const file = path.join(rdz, "identity.json");
  return { rdz, identity: existsSync(file) ? JSON.parse(readFileSync(file, "utf8")) : null };
}
async function completeBind(requested, { instance = INSTANCE, attempt = ATTEMPT, binding = BINDING, context = ctx } = {}) {
  if (!requested.identity?.ok) return requested;
  const authorization = JSON.parse(readFileSync(path.join(requested.rdz, "control", "requests", `${attempt}.json`), "utf8"));
  await control({
    schema_version: 1,
    type: "identity_bound",
    instance_id: instance,
    control_epoch: CONTROL_EPOCH,
    bind_attempt_id: attempt,
    binding_epoch: binding,
    omp_process_epoch: requested.identity.omp_process_epoch,
    session_id: requested.identity.session_id,
    gate_generation: authorization.args.gate_generation,
    claim_generation: authorization.args.claim_generation,
  }, context);
  return requested;
}
async function bind(options = {}) {
  return completeBind(await requestIdentity(options), options);
}
const SECOND = { pid: 902, instance: SECOND_INSTANCE, attempt: SECOND_ATTEMPT, binding: SECOND_BINDING };
// The same child's next proposal, after an earlier one was refused or bound.
const AGAIN = { attempt: SECOND_ATTEMPT, binding: SECOND_BINDING };
function authorizationOf(requested, attempt) {
  const file = path.join(requested.rdz, "control", "requests", `${attempt}.json`);
  return existsSync(file) ? JSON.parse(readFileSync(file, "utf8")).args : null;
}
function healthOf(requested) {
  const file = path.join(requested.rdz, "health.json");
  return existsSync(file) ? JSON.parse(readFileSync(file, "utf8")) : null;
}
const deliveryFor = event => ({
  event_id: event, client_event_id: "client-cr", from_agent: "peer.agent", to_agent: "omp.cr",
  channel: "#agents", thread_id: null, kind: "agent_message", content: "hello from relay", ts: 1780001000100,
});
const displayFor = event => `[relay] from=peer.agent to=omp.cr event_id=${event} kind=agent_message\n\nhello from relay`;
function offer(event, { session = SESSION, instance = INSTANCE, binding = BINDING, context = ctx } = {}) {
  return emit("mcp_notification", { server: "omp-relay", method: "notifications/omp/relay/message", params: {
    schema_version: 1, instance_id: instance, binding_epoch: binding, recipient_id: session, offer_token: OFFER,
    offer_generation: 1, current_relay_name: "omp.cr", delivery: deliveryFor(event), display_text: displayFor(event),
  } }, context);
}
function terminal(type, event, { session = SESSION, instance = INSTANCE, binding = BINDING, context = ctx } = {}) {
  return control({ schema_version: 1, type, instance_id: instance, binding_epoch: binding, recipient_id: session, event_id: event, offer_token: OFFER }, context);
}
function appendReceipt(session, text) {
  appendFileSync(journalFor(session), JSON.stringify({ type: "message", id: `msg-${session}`, parentId: null, timestamp: new Date().toISOString(), message: { role: "user", content: [{ type: "text", text }], timestamp: Date.now() } }) + "\n");
}
function mirrorFor(session, event) {
  const file = path.join(stateRoot, "sessions", session, "offers", `${event}.json`);
  return existsSync(file) ? JSON.parse(readFileSync(file, "utf8")) : null;
}
// transitionClear reads the offers below the state root, so it has no answer before roots are pinned.
function clearOf(session) {
  return relay.__testing.getGlobalState().state_root ? relay.__testing.transitionClear(session) : null;
}
function claimsOf(session) {
  return relay.__testing.getGlobalState().coordinators.get(session)?.claims.size ?? 0;
}
function ownerFor(session) {
  const file = path.join(stateRoot, "sessions", session, "lock", "owner.json");
  return existsSync(file) ? JSON.parse(readFileSync(file, "utf8")) : null;
}
function ownerText() {
  const file = path.join(stateRoot, "sessions", SESSION, "lock", "owner.json");
  return existsSync(file) ? readFileSync(file, "utf8") : null;
}
function snapshot(extra = {}) {
  const global = relay.__testing.getGlobalState();
  const gate = global.gates.get(SESSION);
  const coordinator = global.coordinators.get(SESSION);
  return {
    expectedState: realpathSync.native(stateRoot),
    expectedRuntime: realpathSync.native(runtimeRoot),
    stateRoot: global.state_root,
    runtimeRoot: global.runtime_root,
    gates: [...global.gates.keys()],
    claims: coordinator ? coordinator.claims.size : 0,
    bound: gate?.binding ? gate.binding.binding_epoch : null,
    status: statuses.at(-1)?.text ?? null,
    statusKind: statuses.at(-1)?.kind ?? null,
    statusKinds: statuses.map(item => item.kind),
    statusKeys: [...new Set(statuses.map(item => item.key))],
    notices,
    logs,
    otherStateEntries: readdirSync(otherState),
    otherRuntimeEntries: readdirSync(otherRuntime),
    ...extra,
  };
}

// Held-state records the extension logged, in order, and the status kind after each step.
const heldRecords = () => logs.filter(item => typeof item.detail?.held === "string").map(item => item.detail);
const fenceRecords = () => logs.filter(item => typeof item.detail?.fenced === "string").map(item => item.detail);
const trail = [];
async function step(label, action) {
  await action();
  await Bun.sleep(5);
  trail.push({ label, kind: shown(), text: statuses.at(-1)?.text ?? null });
}
const RECONCILE = { schema_version: 1, type: "rebind_reconcile", instance_id: INSTANCE, control_epoch: CONTROL_EPOCH };

relay.__testing.setMcpManager?.(scenario === "unavailable" ? null : manager);
relay.default(api);
await Bun.sleep(0);

if (scenario === "failed-launch") {
  await emit("session_start");
  await statusEvent({ type: "failed", serverName: "omp-relay", error: "spawn failed" });
  console.log(JSON.stringify(snapshot()));
} else if (scenario === "connected-no-identity" || scenario === "rpc-pending") {
  await emit("session_start");
  await Bun.sleep(10);
  console.log(JSON.stringify(snapshot()));
} else if (scenario === "tui-no-entry-no-event") {
  await emit("session_start");
  await emit("before_agent_start");
  await emit("turn_end");
  console.log(JSON.stringify(snapshot()));
} else if (scenario === "tui-connecting-without-relay") {
  await emit("session_start");
  const before = shown();
  await statusEvent({ type: "connecting", serverNames: ["other-server"] });
  console.log(JSON.stringify(snapshot({ before })));
} else if (scenario === "tui-late-entry") {
  await emit("session_start");
  entry = { command: EXE, args: args(stateRoot, runtimeRoot) };
  await statusEvent({ type: "connecting", serverNames: ["omp-relay"] });
  connection = "connected";
  await statusEvent({ type: "connected", serverName: "omp-relay" });
  const requested = await bind();
  console.log(JSON.stringify(snapshot({ identity: requested.identity })));
} else if (scenario === "overlap") {
  const [, requested] = await Promise.all([emit("session_start"), requestIdentity()]);
  await completeBind(requested);
  console.log(JSON.stringify(snapshot({ identity: requested.identity })));
} else if (scenario === "repeated-events") {
  await emit("session_start");
  for (let index = 0; index < 3; index += 1) {
    await statusEvent({ type: "connected", serverName: "omp-relay" });
    await emit("mcp_notification", { server: "omp-relay", method: "notifications/omp/relay/noop", params: {} });
    await emit("before_agent_start");
    await emit("turn_end");
  }
  await Bun.sleep(10);
  const requested = await bind();
  await emit("turn_end");
  console.log(JSON.stringify(snapshot({ identity: requested.identity, owner: ownerText() })));
} else if (scenario === "during-switch" || scenario === "during-branch" || scenario === "during-tree") {
  const kind = scenario.slice("during-".length);
  await emit("session_start");
  const before = await emit(`session_before_${kind}`);
  entry = { command: EXE, args: args(stateRoot, runtimeRoot) };
  connection = "connected";
  await statusEvent({ type: "connecting", serverNames: ["omp-relay"] });
  const early = await requestIdentity();
  const middle = { stateRoot: relay.__testing.getGlobalState().state_root, gates: [...relay.__testing.getGlobalState().gates.keys()], identity: early.identity };
  await emit(`session_${kind}`);
  const requested = await bind();
  console.log(JSON.stringify(snapshot({ before, middle, identity: requested.identity })));
} else if (scenario === "during-shutdown") {
  await emit("session_start");
  await emit("session_shutdown");
  entry = { command: EXE, args: args(stateRoot, runtimeRoot) };
  await statusEvent({ type: "connecting", serverNames: ["omp-relay"] });
  const requested = await requestIdentity();
  console.log(JSON.stringify(snapshot({ identity: requested.identity })));
} else if (scenario === "subagent-waiting") {
  await emit("session_start", {}, sub);
  await emit("before_agent_start", {}, sub);
  await statusEvent({ type: "connecting", serverNames: ["omp-relay"] });
  await control({ schema_version: 1, type: "rebind_reconcile", instance_id: INSTANCE, control_epoch: CONTROL_EPOCH }, sub);
  console.log(JSON.stringify(snapshot()));
} else if (scenario === "change-before-bound") {
  // Review round 1, finding 0: the declaration changes after the identity request was authorized
  // and before identity_bound commits the binding.
  await emit("session_start");
  const requested = await requestIdentity();
  const ownerBefore = ownerText();
  entry = { command: EXE, args: args(otherState, runtimeRoot) };
  await completeBind(requested);
  const healthFile = path.join(requested.rdz, "health.json");
  console.log(JSON.stringify(snapshot({
    authorized: requested.identity,
    health: existsSync(healthFile) ? JSON.parse(readFileSync(healthFile, "utf8")) : null,
    ownerUnchanged: ownerBefore !== null && ownerBefore === ownerText(),
  })));
} else if (scenario === "change-before-switch" || scenario === "change-before-branch" || scenario === "change-before-tree") {
  // Review round 1, finding 1: roots are pinned and bound, the declaration changes, then a
  // transition completes. switch/branch land on a second session; tree stays on the same one.
  const kind = scenario.slice("change-before-".length);
  await emit("session_start");
  const first = await bind();
  const ownerBefore = ownerFor(SESSION);
  const before = await emit(`session_before_${kind}`);
  entry = { command: EXE, args: args(otherState, runtimeRoot) };
  await emit(`session_${kind}`, {}, kind === "tree" ? ctx : secondCtx);
  await Bun.sleep(20);
  console.log(JSON.stringify(snapshot({
    first: first.identity,
    before,
    allGates: [...relay.__testing.getGlobalState().gates.keys()],
    ownerBefore,
    ownerAfter: ownerFor(SESSION),
    secondOwner: ownerFor(SECOND_SESSION),
  })));
} else if (scenario === "change-before-unquiesce") {
  // Same class as finding 1 through the /relay-unquiesce command: a fresh gate and a transferred
  // claim for the restored transition, after the declaration changed.
  await emit("session_start");
  const first = await bind();
  const before = await emit("session_before_switch");
  const ownerBefore = ownerText();
  entry = { command: EXE, args: args(otherState, runtimeRoot) };
  await commands.get("relay-unquiesce").handler("", ctx);
  await Bun.sleep(20);
  const gate = relay.__testing.getGlobalState().gates.get(SESSION);
  console.log(JSON.stringify(snapshot({
    first: first.identity,
    before,
    gateState: gate?.state ?? null,
    ownerUnchanged: ownerBefore !== null && ownerBefore === ownerText(),
  })));
} else if (scenario === "window-fence-switch" || scenario === "window-fence-branch" || scenario === "window-fence-tree") {
  // Transition regression: a transition is pending for a session that has no gate yet,
  // a turn starts, the relay entry appears, the child asks for an identity and offers a message, and
  // the turn ends. Until omp's own completion event nothing may be claimed, bound or admitted, and
  // the completion may not remove or release a gate whose deliveries are not clear.
  const kind = scenario.slice("window-fence-".length);
  const target = kind === "tree" ? ctx : secondCtx;
  await emit("session_start");
  const before = await emit(`session_before_${kind}`);
  await emit("before_agent_start");
  entry = { command: EXE, args: args(stateRoot, runtimeRoot) };
  await statusEvent({ type: "connecting", serverNames: ["omp-relay"] });
  await statusEvent({ type: "connected", serverName: "omp-relay" });
  await Bun.sleep(10);
  const requested = await bind();
  await offer("41");
  await emit("turn_end");
  await Bun.sleep(10);
  const global = relay.__testing.getGlobalState();
  const windowGate = global.gates.get(SESSION) ?? null;
  const during = {
    gates: [...global.gates.keys()],
    claims: claimsOf(SESSION),
    owner: ownerFor(SESSION)?.holder?.session_id ?? null,
    identity: requested.identity,
    bound: windowGate?.binding?.binding_epoch ?? null,
    admitted: sent.length,
    mirror: mirrorFor(SESSION, "41")?.state ?? null,
    clear: clearOf(SESSION),
  };
  await emit(`session_${kind}`, {}, target);
  await Bun.sleep(30);
  const completion = {
    clear: clearOf(SESSION),
    windowGateKept: windowGate === null || global.gates.get(SESSION) === windowGate,
    windowLifecycle: windowGate?.claim.lifecycle ?? null,
    owner: ownerFor(SESSION)?.holder?.holder_claim_generation ?? null,
    windowClaim: windowGate?.claim.claim_generation ?? null,
  };
  // After omp's completion event the session it landed on claims and binds as usual.
  const destination = await bind({ ...AGAIN, context: target });
  const destinationSession = target.sessionManager.getSessionId();
  const destinationGate = global.gates.get(destinationSession);
  console.log(JSON.stringify(snapshot({
    before,
    during,
    completion,
    identity: destination.identity,
    destinationBound: destinationGate?.binding?.binding_epoch ?? null,
    destinationOwner: ownerFor(destinationSession)?.holder?.session_id ?? null,
    allGates: [...global.gates.keys()],
  })));
} else if (scenario === "window-cancelled-held") {
  // The documented limitation: the pre-bind transition is cancelled after this extension's
  // before-hook ran (omp 18.8.6 tells extensions nothing), so no completion event comes. Turns, the
  // child's requests and offers, and connection events do not end the hold; the next completed
  // transition does.
  await emit("session_start");
  const statusBefore = shown();
  const before = await emit("session_before_switch");
  entry = { command: EXE, args: args(stateRoot, runtimeRoot) };
  await statusEvent({ type: "connecting", serverNames: ["omp-relay"] });
  await statusEvent({ type: "connected", serverName: "omp-relay" });
  await emit("before_agent_start");
  const requested = await bind();
  await offer("41");
  await emit("turn_end");
  await emit("before_agent_start");
  await emit("turn_end");
  await Bun.sleep(10);
  const global = relay.__testing.getGlobalState();
  const held = {
    gates: [...global.gates.keys()],
    claims: claimsOf(SESSION),
    owner: ownerFor(SESSION)?.holder?.session_id ?? null,
    identity: requested.identity,
    admitted: sent.length,
    status: shown(),
  };
  const later = await emit("session_before_tree");
  await emit("session_tree");
  await Bun.sleep(20);
  const rebound = await bind(AGAIN);
  console.log(JSON.stringify(snapshot({ statusBefore, before, held, later, identity: rebound.identity })));
} else if (scenario === "window-cancelled-bound") {
  // The same limitation for a session that was already bound: its gate stays quiescing.
  await emit("session_start");
  const first = await bind();
  const before = await emit("session_before_switch");
  await emit("before_agent_start");
  await offer("41");
  await emit("turn_end");
  const global = relay.__testing.getGlobalState();
  const gate = global.gates.get(SESSION);
  const held = {
    gateState: gate?.state ?? null,
    admitted: sent.length,
    mirror: mirrorFor(SESSION, "41")?.state ?? null,
    health: healthOf(first)?.code ?? null,
    status: shown(),
  };
  const next = await emit("session_before_switch");
  await commands.get("relay-unquiesce").handler("", ctx);
  await Bun.sleep(20);
  await offer("42");
  const restored = global.gates.get(SESSION);
  console.log(JSON.stringify(snapshot({
    before,
    held,
    next,
    restoredState: restored?.state ?? null,
    restoredAdmitted: sent.length,
    restoredMirror: mirrorFor(SESSION, "42")?.state ?? null,
  })));
} else if (scenario === "window-unquiesce-unclear") {
  // /relay-unquiesce restores a quiesced gate while the switch is in fact still pending; the
  // restored gate admits a message, which gets a journal receipt and the child's offer_accepted but
  // no identity_acked. The completion then arrives and must leave that gate and its lock alone.
  await emit("session_start");
  await bind();
  const before = await emit("session_before_switch");
  await commands.get("relay-unquiesce").handler("", ctx);
  await Bun.sleep(20);
  const global = relay.__testing.getGlobalState();
  const replacement = global.gates.get(SESSION) ?? null;
  const replacementState = replacement?.state ?? null;
  await offer("41");
  const admitted = { sent: sent.length, mirror: mirrorFor(SESSION, "41"), clear: clearOf(SESSION) };
  appendReceipt(SESSION, sent.at(-1)?.text ?? "");
  await emit("message_end");
  await Bun.sleep(10);
  const receipted = { mirror: mirrorFor(SESSION, "41"), clear: clearOf(SESSION) };
  await terminal("offer_accepted", "41");
  const accepted = { mirror: mirrorFor(SESSION, "41"), clear: clearOf(SESSION) };
  await emit("session_switch", {}, secondCtx);
  await Bun.sleep(30);
  console.log(JSON.stringify(snapshot({
    before,
    replacementState,
    admitted,
    receipted,
    accepted,
    completionClear: clearOf(SESSION),
    replacementKept: replacement !== null && global.gates.get(SESSION) === replacement,
    replacementLifecycle: replacement?.claim.lifecycle ?? null,
    replacementClaim: replacement?.claim.claim_generation ?? null,
    oldOwnerClaim: ownerFor(SESSION)?.holder?.holder_claim_generation ?? null,
    allGates: [...global.gates.keys()],
  })));
} else if (scenario === "window-unquiesce-switch") {
  // C-55, same window: /relay-unquiesce replaces the quiesced gate while the switch is in flight;
  // the completion must retire the replacement too, not drop it from the map with its lock held.
  await emit("session_start");
  await bind();
  const before = await emit("session_before_switch");
  await commands.get("relay-unquiesce").handler("", ctx);
  await Bun.sleep(20);
  const global = relay.__testing.getGlobalState();
  const replacement = global.gates.get(SESSION) ?? null;
  const ownerDuring = ownerFor(SESSION);
  await emit("session_switch", {}, secondCtx);
  await Bun.sleep(30);
  console.log(JSON.stringify(snapshot({
    before,
    replacementState: replacement?.state ?? null,
    ownerDuringClaim: ownerDuring?.holder?.holder_claim_generation ?? null,
    replacementClaim: replacement?.claim.claim_generation ?? null,
    replacementLifecycle: replacement?.claim.lifecycle ?? null,
    allGates: [...global.gates.keys()],
    oldOwner: ownerFor(SESSION),
    secondOwner: ownerFor(SECOND_SESSION),
  })));
} else if (scenario === "manual-reconnect-after-bound") {
  // Review round 3, finding 0: omp's manual `/mcp reconnect` replaces the connection and emits only
  // `connected` (no connecting / reconnecting); the replacement child has not bound yet.
  await emit("session_start");
  await bind();
  const boundStatus = shown();
  connectionObject = { child: 2 };
  await statusEvent({ type: "connected", serverName: "omp-relay" });
  const replacementStatus = shown();
  const rebound = await bind(SECOND);
  console.log(JSON.stringify(snapshot({ boundStatus, replacementStatus, identity: rebound.identity })));
} else if (scenario === "manual-reconnect-silent") {
  // omp 18.8.6 `/mcp reconnect` (MCPManager.reconnectServer manual -> #doReconnect) drops the
  // connection synchronously and emits nothing until the replacement's `connected`. While it runs,
  // getConnection has nothing and getConnectionStatus says "connecting". Nothing polls.
  await emit("session_start");
  await bind();
  const boundStatus = shown();
  const changesBefore = statuses.length;
  connection = "connecting";
  await Bun.sleep(50);
  const silentStatus = shown();
  const silentChanges = statuses.length - changesBefore;
  await emit("turn_end");
  const nextEventStatus = shown();
  connection = "connected";
  connectionObject = { child: 2 };
  await statusEvent({ type: "connected", serverName: "omp-relay" });
  const replacementStatus = shown();
  const rebound = await bind(SECOND);
  console.log(JSON.stringify(snapshot({ boundStatus, silentStatus, silentChanges, nextEventStatus, replacementStatus, identity: rebound.identity })));
} else if (scenario === "attribution-late-bound" || scenario === "attribution-claim-wait") {
  // Connection-attribution regression: child A is authorized over connection A, omp replaces
  // that connection with B, and A's identity_bound arrives late. In the claim-wait variant the
  // replacement lands while A's identity request is still waiting for the session claim.
  const claimWait = scenario === "attribution-claim-wait";
  let releaseFifo = null;
  if (claimWait) relay.__testing.coordinatorFor(SESSION).fifo_tail = new Promise(resolve => { releaseFifo = resolve; });
  await emit("session_start");
  const global = relay.__testing.getGlobalState();
  const gateBefore = global.gates.get(SESSION);
  let requested;
  if (claimWait) {
    const pending = requestIdentity();
    await Bun.sleep(10);
    connectionObject = { child: 2 };
    releaseFifo();
    requested = await pending;
  } else {
    requested = await requestIdentity();
    connectionObject = { child: 2 };
    await statusEvent({ type: "connected", serverName: "omp-relay" });
  }
  const approvalsAfterRequest = global.pending_approvals.size;
  const authorizedA = authorizationOf(requested, ATTEMPT);
  await completeBind(requested);
  const gate = global.gates.get(SESSION);
  const afterA = {
    bound: gate?.binding?.binding_epoch ?? null,
    boundConnection: global.bound_connection?.child ?? null,
    status: shown(),
    health: healthOf(requested),
    sameGate: gate === gateBefore,
  };
  const rebound = await bind(SECOND);
  const authorizedB = authorizationOf(rebound, SECOND_ATTEMPT);
  console.log(JSON.stringify(snapshot({
    identityA: requested.identity,
    approvalsAfterRequest,
    authorizedA,
    afterA,
    identity: rebound.identity,
    authorizedB,
    gateGeneration: gateBefore?.gate_generation ?? null,
    claimGeneration: gateBefore?.claim.claim_generation ?? null,
    boundConnection: global.bound_connection?.child ?? null,
  })));
} else if (scenario === "change-during-claim-wait") {
  // Review round 2, finding 0: the identity request passes the declaration check, then waits on
  // the per-session FIFO behind the claim; the declaration changes during that wait.
  let releaseFifo = null;
  relay.__testing.coordinatorFor(SESSION).fifo_tail = new Promise(resolve => { releaseFifo = resolve; });
  await emit("session_start");
  const pending = requestIdentity();
  await Bun.sleep(10);
  entry = { command: EXE, args: args(otherState, runtimeRoot) };
  releaseFifo();
  const requested = await pending;
  const global = relay.__testing.getGlobalState();
  console.log(JSON.stringify(snapshot({
    identity: requested.identity,
    authorizationWritten: existsSync(path.join(requested.rdz, "control", "requests", `${ATTEMPT}.json`)),
    pendingApprovals: global.pending_approvals.size,
  })));
} else if (scenario === "reconnect-after-bound") {
  // Review round 2, finding 2: a bound child's transport closes; omp emits `reconnecting` and
  // starts a replacement child that has not bound an identity yet.
  connection = "connected";
  await emit("session_start");
  await bind();
  const boundStatus = shown();
  connection = "connecting";
  await statusEvent({ type: "reconnecting", serverName: "omp-relay" });
  const reconnectingStatus = shown();
  connection = "connected";
  connectionObject = { child: 2 };
  await statusEvent({ type: "connected", serverName: "omp-relay" });
  const replacementStatus = shown();
  const rebound = await bind(SECOND);
  console.log(JSON.stringify(snapshot({ boundStatus, reconnectingStatus, replacementStatus, identity: rebound.identity })));
} else if (scenario === "guard-unchanged-reconnect") {
  await emit("session_start");
  const first = await bind();
  await statusEvent({ type: "reconnecting", serverName: "omp-relay" });
  await statusEvent({ type: "connected", serverName: "omp-relay" });
  const second = await bind(SECOND);
  console.log(JSON.stringify(snapshot({ first: first.identity, identity: second.identity })));
} else if (scenario === "guard-runtime-changed" || scenario === "guard-state-changed" || scenario === "guard-reload-changed") {
  await emit("session_start");
  const first = await bind();
  const ownerBefore = ownerText();
  const runtimeChanged = scenario === "guard-runtime-changed";
  entry = { command: EXE, args: runtimeChanged ? args(stateRoot, otherRuntime) : args(otherState, runtimeRoot) };
  if (scenario === "guard-reload-changed") {
    handlers.clear();
    relay.default(api);
    await Bun.sleep(0);
    await emit("session_start");
  } else {
    await statusEvent({ type: "connecting", serverNames: ["omp-relay"] });
  }
  const refused = await requestIdentity({ ...SECOND, runtime: runtimeChanged ? otherRuntime : runtimeRoot });
  const rendezvousEntries = readdirSync(refused.rdz).sort();
  console.log(JSON.stringify(snapshot({
    first: first.identity,
    identity: refused.identity,
    rendezvousEntries,
    ownerUnchanged: ownerBefore !== null && ownerBefore === ownerText(),
  })));
} else if (scenario === "held-pre-bind-completed" || scenario === "held-pre-bind-cancelled") {
  // A transition begins before the first bind. Pending, or cancelled without omp telling extensions,
  // the session is held through every event the extension sees until a transition completes.
  await emit("session_start");
  await step("session_before_switch", () => emit("session_before_switch"));
  entry = { command: EXE, args: args(stateRoot, runtimeRoot) };
  await step("connecting", () => statusEvent({ type: "connecting", serverNames: ["omp-relay"] }));
  await step("connected", () => statusEvent({ type: "connected", serverName: "omp-relay" }));
  await step("before_agent_start", () => emit("before_agent_start"));
  await step("identity_request", () => requestIdentity());
  await step("turn_end", () => emit("turn_end"));
  await step("timer", () => Bun.sleep(200));
  if (scenario === "held-pre-bind-cancelled") {
    await step("session_before_tree", () => emit("session_before_tree"));
    await step("session_tree", () => emit("session_tree"));
  } else {
    await step("session_switch", () => emit("session_switch", {}, secondCtx));
  }
  console.log(JSON.stringify(snapshot({ trail, records: heldRecords() })));
} else if (scenario.startsWith("held-quiescing-")) {
  // A gate quiesces for a switch. Pending, or cancelled without omp telling extensions, it stays held
  // until the switch completes, or until /relay-unquiesce restores it and the restore claim lands.
  await emit("session_start");
  if (scenario !== "held-quiescing-unbound") await bind();
  const ready = shown();
  await step("session_before_switch", () => emit("session_before_switch"));
  await step("before_agent_start", () => emit("before_agent_start"));
  await step("offer", () => offer("41"));
  await step("rebind_reconcile", () => control(RECONCILE));
  await step("connected", () => statusEvent({ type: "connected", serverName: "omp-relay" }));
  await step("turn_end", () => emit("turn_end"));
  await step("timer", () => Bun.sleep(200));
  if (scenario === "held-quiescing-unquiesced") {
    await step("session_before_branch", () => emit("session_before_branch"));
    // Hold the claim queue so the restoring state can be observed before the restore claim lands.
    let release = null;
    relay.__testing.coordinatorFor(SESSION).fifo_tail = new Promise(resolve => { release = resolve; });
    const restoring = commands.get("relay-unquiesce").handler("", ctx);
    await step("relay-unquiesce", () => Bun.sleep(10));
    await step("restoring turn_end", () => emit("turn_end"));
    release();
    await step("claim_result", () => restoring);
  } else {
    await step("session_switch", () => emit("session_switch", {}, secondCtx));
  }
  console.log(JSON.stringify(snapshot({ ready, trail, records: heldRecords() })));
} else if (scenario === "held-retained-tree") {
  // A gate restored by /relay-unquiesce while the tree navigation was in fact pending admits a
  // message that is receipted and terminally accepted but not acknowledged; the navigation then
  // completes and the gate is kept with its lock. It is held until the acknowledgement lands.
  await emit("session_start");
  await bind();
  await emit("session_before_tree");
  await commands.get("relay-unquiesce").handler("", ctx);
  await Bun.sleep(20);
  await offer("41");
  appendReceipt(SESSION, sent.at(-1)?.text ?? "");
  await emit("message_end");
  await Bun.sleep(10);
  await terminal("offer_accepted", "41");
  const unclear = { clear: clearOf(SESSION), mirror: mirrorFor(SESSION, "41") };
  await step("session_tree", () => emit("session_tree"));
  await step("turn_end", () => emit("turn_end"));
  await step("before_agent_start", () => emit("before_agent_start"));
  await step("connected", () => statusEvent({ type: "connected", serverName: "omp-relay" }));
  await step("rebind_reconcile", () => control(RECONCILE));
  await step("timer", () => Bun.sleep(200));
  await step("identity_acked", () => terminal("identity_acked", "41"));
  console.log(JSON.stringify(snapshot({ unclear, trail, records: heldRecords(), clear: clearOf(SESSION) })));
} else if (scenario.startsWith("late-")) {
  // Final review, finding 1 (2026-10-09). S1's gate is kept across a completed switch to S2 because
  // an offer is unacknowledged. Identity or claim work S1 began while it was current finishes once S2
  // is current; it must not commit S1's binding, write the child's health, or fence S2.
  const variant = scenario.slice("late-".length);
  const global = relay.__testing.getGlobalState();
  const THIRD = { attempt: "57575757-5757-4757-8757-575757575757", binding: "68686868-6868-4868-8868-686868686868" };
  await emit("session_start");
  const first = await bind();
  const gateView = session => {
    const gate = global.gates.get(session);
    return gate ? {
      state: gate.state, retained: gate.retained, fence: gate.fence_code,
      binding: gate.binding ? [gate.binding.instance_id, gate.binding.binding_epoch, gate.binding.omp_process_epoch] : null,
      claim: gate.claim.claim_generation,
    } : null;
  };
  const view = () => ({
    s1: { gate: gateView(SESSION), owner: ownerFor(SESSION)?.holder?.holder_claim_generation ?? null },
    s2: { gate: gateView(SECOND_SESSION), owner: ownerFor(SECOND_SESSION)?.holder?.holder_claim_generation ?? null },
    health: healthOf(first),
    boundBinding: global.bound_binding?.binding_epoch ?? null,
    status: shown(),
  });
  await emit("session_before_switch");
  await commands.get("relay-unquiesce").handler("", ctx);
  await Bun.sleep(20);
  await offer("41");
  appendReceipt(SESSION, sent.at(-1)?.text ?? "");
  await emit("message_end");
  await Bun.sleep(10);
  await terminal("offer_accepted", "41");
  let requested = null;
  let release = null;
  let pending = null;
  let liveSession = SESSION;
  const live = { ...ctx, sessionManager: { ...ctx.sessionManager, getSessionId() { return liveSession; } } };
  if (variant === "bind-s2-unbound" || variant === "bind-s2-bound") {
    // The child's identity request for S1, authorized while S1 is current.
    requested = await requestIdentity(THIRD);
  } else if (variant === "request-wait") {
    // The same request, still waiting for S1's claim queue when the switch completes. Its handler is
    // in flight across the switch, so it runs on a context like omp's: every omp context shares one
    // SessionManager, whose session id follows the switch.
    relay.__testing.coordinatorFor(SESSION).fifo_tail = new Promise(resolve => { release = resolve; });
    pending = requestIdentity({ ...THIRD, context: live });
    await Bun.sleep(10);
  } else {
    // claim-result: the child's periodic reconcile queues a claim for S1 behind a busy queue.
    relay.__testing.coordinatorFor(SESSION).fifo_tail = new Promise(resolve => { release = resolve; });
    await control(RECONCILE);
  }
  const kept = global.gates.get(SESSION);
  liveSession = SECOND_SESSION;
  await emit("session_switch", {}, secondCtx);
  await Bun.sleep(30);
  if (variant === "bind-s2-bound") await bind({ ...AGAIN, context: secondCtx });
  const before = view();
  if (variant === "bind-s2-unbound" || variant === "bind-s2-bound") {
    await completeBind(requested, { ...THIRD, context: secondCtx });
  } else {
    release();
    if (pending) requested = await pending;
  }
  await Bun.sleep(30);
  const after = view();
  const navigation = await emit("session_before_switch", {}, secondCtx);
  console.log(JSON.stringify(snapshot({
    before, after, navigation, sameGate: global.gates.get(SESSION) === kept,
    identity: requested?.identity ?? null, authorization: requested ? authorizationOf(requested, THIRD.attempt) : null,
    fences: fenceRecords(),
  })));
} else if (scenario.startsWith("f2-")) {
  // F2 (2026-10-09). S1 has an accepted, unacknowledged offer when a switch to S2 completes, so S1's
  // gate is kept with its lock. A result about S1 then arrives while S2 is current. It belongs to the
  // session its binding proves; it must not fence or rewrite S2, nor overwrite the child's health.
  const [, kase, nav] = scenario.split("-");
  const global = relay.__testing.getGlobalState();
  const THIRD = { attempt: "57575757-5757-4757-8757-575757575757", binding: "68686868-6868-4868-8868-686868686868" };
  const OTHER_TOKEN = "79797979-7979-4979-8979-797979797979";
  const STALE_BINDING = "6a6a6a6a-6a6a-4a6a-8a6a-6a6a6a6a6a6a";
  const UNKNOWN_SESSION = "2197f0c2-552b-7db4-8d43-6c338d7d3021";
  await emit("session_start");
  const first = await bind();
  const gateView = session => {
    const gate = global.gates.get(session);
    return gate ? {
      state: gate.state, retained: gate.retained, fence: gate.fence_code,
      binding: gate.binding ? [gate.binding.instance_id, gate.binding.binding_epoch, gate.binding.omp_process_epoch] : null,
      claim: gate.claim.claim_generation, lifecycle: gate.claim.lifecycle, ownership: gate.claim.ownership,
    } : null;
  };
  const view = () => ({
    s1: { mirror: mirrorFor(SESSION, "41"), gate: gateView(SESSION), owner: ownerFor(SESSION)?.holder?.holder_claim_generation ?? null, clear: clearOf(SESSION) },
    s2: { mirror: mirrorFor(SECOND_SESSION, "61"), gate: gateView(SECOND_SESSION), owner: ownerFor(SECOND_SESSION)?.holder?.holder_claim_generation ?? null },
    health: healthOf(first),
    status: shown(),
  });
  await emit("session_before_switch");
  await commands.get("relay-unquiesce").handler("", ctx);
  await Bun.sleep(20);
  await offer("41");
  appendReceipt(SESSION, sent.at(-1)?.text ?? "");
  await emit("message_end");
  await Bun.sleep(10);
  await terminal("offer_accepted", "41");
  const kept = global.gates.get(SESSION);
  await emit("session_switch", {}, secondCtx);
  await Bun.sleep(30);
  const onS2 = { context: secondCtx };
  const asS2 = { session: SECOND_SESSION, binding: SECOND_BINDING, context: secondCtx };
  if (kase !== "a") await bind({ ...AGAIN, ...onS2 });
  if (kase === "b2") {
    // S2's own delivery, accepted and not yet acknowledged: real unresolved delivery on S2.
    await offer("61", asS2);
    appendReceipt(SECOND_SESSION, sent.at(-1)?.text ?? "");
    await emit("message_end", {}, secondCtx);
    await Bun.sleep(10);
    await terminal("offer_accepted", "61", asS2);
  }
  const before = view();
  const s1Ack = () => terminal("identity_acked", "41", { session: SESSION, ...onS2 });
  if (kase === "a" || kase === "b" || kase === "b2") {
    await s1Ack();
  } else if (kase === "d") {
    await s1Ack();
    await s1Ack();
    await terminal("offer_accepted", "41", { session: SESSION, ...onS2 });
  } else if (kase === "e1") {
    await control({ schema_version: 1, type: "identity_acked", instance_id: INSTANCE, binding_epoch: BINDING, recipient_id: SESSION, event_id: "41", offer_token: OTHER_TOKEN }, secondCtx);
  } else if (kase === "e2") {
    await terminal("identity_acked", "41", { session: SESSION, binding: STALE_BINDING, ...onS2 });
  } else if (kase === "e3") {
    await terminal("identity_acked", "41", { session: UNKNOWN_SESSION, ...onS2 });
  } else {
    // f: a genuinely invalid control for S2 itself (no such offer of S2's).
    await terminal("identity_acked", "41", asS2);
  }
  await Bun.sleep(20);
  const after = view();
  const steps = {};
  if (kase === "a") {
    await bind({ ...AGAIN, ...onS2 });
    steps.s2Bound = view();
  }
  if (kase === "b2") {
    steps.blocked = await emit(`session_before_${nav}`, {}, secondCtx);
    steps.blockedView = view();
    await terminal("identity_acked", "61", asS2);
    await Bun.sleep(10);
    steps.s2Acked = view();
  }
  steps.navigation = await emit(`session_before_${nav}`, {}, secondCtx);
  if (steps.navigation[0]?.cancel !== true) {
    if (nav === "tree") {
      await emit("session_tree", {}, secondCtx);
      await Bun.sleep(30);
      steps.completed = view();
    } else {
      // Back to S1, then the child's existing recovery: its periodic reconcile, a rebind when health
      // names another epoch, and an acknowledgement if S1's offer still needs one.
      await emit(`session_${nav}`, {}, ctx);
      await Bun.sleep(30);
      steps.returned = { ...view(), sameGate: global.gates.get(SESSION) === kept };
      await control(RECONCILE);
      await Bun.sleep(10);
      steps.reconciled = view();
      let binding = SECOND_BINDING;
      if (steps.reconciled.health?.binding_epoch !== SECOND_BINDING) {
        await bind(THIRD);
        binding = THIRD.binding;
        steps.rebound = view();
      }
      if (!mirrorFor(SESSION, "41")?.identity_acknowledged) await terminal("identity_acked", "41", { binding });
      await Bun.sleep(10);
      steps.recovered = { ...view(), sameGate: global.gates.get(SESSION) === kept };
    }
  }
  console.log(JSON.stringify(snapshot({ before, after, steps, records: heldRecords(), fences: fenceRecords() })));
} else if (scenario.startsWith("fence-quiescing-")) {
  // F1: while a bound, clear gate is quiescing for a switch that has not completed, a replayed or
  // duplicate terminal control, or a recovery status, must change no gate or offer state and must
  // not let a delivery in.
  const variant = scenario.slice("fence-quiescing-".length);
  await emit("session_start");
  const first = await bind();
  await offer("41");
  appendReceipt(SESSION, sent.at(-1)?.text ?? "");
  await emit("message_end");
  await Bun.sleep(10);
  await terminal("offer_accepted", "41");
  await terminal("identity_acked", "41");
  const offers = path.join(stateRoot, "sessions", SESSION, "offers");
  const PRIOR_OFFER = "78787878-7878-4878-8878-787878787878";
  const PRIOR_EPOCH = "88888888-8888-4888-8888-888888888888";
  if (variant === "recovery-status") {
    // An offer of a prior omp process for this session, terminal and acknowledged.
    writeFileSync(path.join(offers, "50.json"), JSON.stringify({
      schema_version: 1, session_id: SESSION, recipient_id: SESSION, event_id: "50", offer_token: PRIOR_OFFER,
      omp_process_epoch: PRIOR_EPOCH, state: "accepted", broker_terminal_state: "accepted", identity_acknowledged: true,
      health_reason: "NONE", reject_reason_code: null, application_acceptance_observed: true, updated_at_ms: 1780001000000,
    }) + "\n");
  }
  const global = relay.__testing.getGlobalState();
  const gate = global.gates.get(SESSION);
  const setup = { clear: clearOf(SESSION), state: gate.state };
  const hook = await emit("session_before_switch");
  if (variant === "terminal-recovery") writeFileSync(path.join(offers, "41.json"), "{\n");
  const quiesced = { state: gate.state, status: shown() };
  const files = () => Object.fromEntries(readdirSync(offers).sort().map(name => [name, readFileSync(path.join(offers, name), "utf8")]));
  const filesBefore = files();
  const entriesBefore = global.entries.size;
  const heldBefore = heldRecords().length;
  if (variant === "identity-acked") {
    await terminal("identity_acked", "41");
  } else if (variant === "offer-rejected") {
    await control({ schema_version: 1, type: "offer_rejected", instance_id: INSTANCE, binding_epoch: BINDING, recipient_id: SESSION, event_id: "41", offer_token: OFFER, reject_reason_code: "USER_REJECTED" });
  } else if (variant === "terminal-recovery") {
    await control({ schema_version: 1, type: "offer_terminal_recovery", instance_id: INSTANCE, binding_epoch: BINDING, recipient_id: SESSION, event_id: "41", offer_token: OFFER, terminal_state: "rejected", reject_reason_code: "USER_REJECTED", recovery_source: "malformed_mirror" });
  } else {
    await control({ schema_version: 1, type: "offer_recovery_status", instance_id: INSTANCE, control_epoch: CONTROL_EPOCH, binding_epoch: BINDING,
      omp_process_epoch: first.identity.omp_process_epoch, delivery: null, status: { recipient_id: SESSION, event_id: "50", offer_token: PRIOR_OFFER,
      offer_generation: 1, binding_epoch: BINDING, omp_process_epoch: PRIOR_EPOCH, offer_state: "accepted", delivery_status: "acked", reject_reason_code: null } });
  }
  await Bun.sleep(20);
  const after = {
    state: gate.state, status: shown(), sameGate: global.gates.get(SESSION) === gate, offersUnchanged: JSON.stringify(files()) === JSON.stringify(filesBefore),
    entries: global.entries.size - entriesBefore, heldChanges: heldRecords().slice(heldBefore),
  };
  await offer("42");
  const second = { sent: sent.length, mirror42: mirrorFor(SESSION, "42")?.state ?? null, state: gate.state };
  console.log(JSON.stringify(snapshot({ setup, hook, quiesced, after, second })));
} else if (scenario.startsWith("fence-error-")) {
  // F3: a bound gate fenced by an error, as opposed to the delivery barrier, shows a status kind that
  // is not ready, and the log records the fence with its code until the gate reopens.
  const variant = scenario.slice("fence-error-".length);
  await emit("session_start");
  await bind();
  await step("bound", () => Bun.sleep(0));
  if (variant === "protocol") {
    await step("error", () => control({ schema_version: 1, type: "no_such_control" }));
  } else if (variant === "receipt-conflict") {
    // A result for the current session, under its own binding, naming an offer it does not have.
    await step("error", () => terminal("identity_acked", "41"));
  } else if (variant === "tool-surface-timer") {
    // The tool-surface check omp runs 500 ms after session_start, outside any relay event.
    await step("error", async () => { for (const fn of timers.splice(0)) fn(); });
  } else if (variant === "tool-surface") {
    await step("error", () => emit("mcp_notification", { server: "omp-relay", method: "notifications/tools/list_changed", params: {} }));
  } else {
    // The delivery barrier, not an error: an accepted offer waits for its acknowledgement.
    await offer("41");
    appendReceipt(SESSION, sent.at(-1)?.text ?? "");
    await emit("message_end");
    await Bun.sleep(10);
    await step("error", () => terminal("offer_accepted", "41"));
  }
  const fencedState = relay.__testing.getGlobalState().gates.get(SESSION)?.state ?? null;
  await step("turn_end", () => emit("turn_end"));
  await step("connected", () => statusEvent({ type: "connected", serverName: "omp-relay" }));
  if (variant === "barrier") await step("reopen", () => terminal("identity_acked", "41"));
  else await step("reopen", () => bind(AGAIN));
  const finalState = relay.__testing.getGlobalState().gates.get(SESSION)?.state ?? null;
  console.log(JSON.stringify(snapshot({ trail, fencedState, finalState, fences: fenceRecords() })));
} else if (scenario.startsWith("retained-")) {
  // Retained-owner recovery (2026-10-09). A source session's offer reaches a cut point, then a
  // transition completes while that offer is unclear, so the source gate is kept with its lock.
  // Journals are written by omp's real SessionManager in omp's own format. The child's controls
  // follow omp_relay_mcp.py: the in-flight ACK, the rebind when health names another binding
  // epoch, the periodic rebind_reconcile, then terminal acceptance and identity_acked.
  const [, kind, cut] = scenario.split("-");
  const away = kind !== "tree";
  const { SessionManager } = await import(Bun.pathToFileURL(path.join(process.env.NODE_PATH!, "@oh-my-pi", "pi-coding-agent", "src", "session", "session-manager.ts")).href);
  const journalDir = path.join(base, "omp-sessions");
  const sm1 = SessionManager.create(base, journalDir);
  await sm1.ensureOnDisk();
  const sm2 = away ? SessionManager.create(base, journalDir) : sm1;
  if (away) await sm2.ensureOnDisk();
  const S1 = sm1.getSessionId();
  const S2 = sm2.getSessionId();
  const ctx1 = { ...ctx, sessionManager: sm1 };
  const ctx2 = { ...ctx, sessionManager: sm2 };
  const THIRD = { attempt: "57575757-5757-4757-8757-575757575757", binding: "68686868-6868-4868-8868-686868686868" };
  const global = relay.__testing.getGlobalState();
  const journalHas = (sm, text) => readFileSync(sm.getSessionFile(), "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line))
    .some(row => row.type === "message" && row.message?.role === "user" && row.message.content?.[0]?.text === text);
  async function journal(sm, text) {
    sm.appendMessage({ role: "user", content: [{ type: "text", text }], attribution: "agent", timestamp: Date.now() });
    await sm.flush();
  }
  let delivered = null;
  const witness = () => {
    const mirror = mirrorFor(S1, "41");
    return {
      admitted: sent.filter(item => item.text === delivered).length,
      journalled: delivered !== null && journalHas(sm1, delivered),
      mirror_state: mirror?.state ?? null,
      receipt_recorded: mirror?.application_acceptance_observed ?? null,
      terminal: mirror?.broker_terminal_state ?? null,
      acked: mirror?.identity_acknowledged ?? null,
      clear: clearOf(S1),
    };
  };
  const gateView = session => {
    const gate = global.gates.get(session);
    if (!gate) return null;
    return {
      gate_generation: gate.gate_generation, state: gate.state, retained: gate.retained, resume_gate: gate.resume_gate,
      lock_claim_generation: gate.lock_claim_generation,
      binding: gate.binding ? { instance_id: gate.binding.instance_id, binding_epoch: gate.binding.binding_epoch, gate_generation: gate.binding.gate_generation, claim_generation: gate.binding.claim_generation } : null,
      claim: { claim_generation: gate.claim.claim_generation, holder_gate_generation: gate.claim.holder_gate_generation, lifecycle: gate.claim.lifecycle,
        ownership: gate.claim.ownership, pending: gate.claim.pending, expected_owner: gate.claim.expected_owner, observed_owner: gate.claim.observed_owner,
        transfer_source: gate.claim.transfer_source?.claim_generation ?? null },
    };
  };
  const stage = extra => ({ witness: witness(), source_gate: gateView(S1), owner: ownerFor(S1), status: shown(), ...extra });
  const stages = {};

  await emit("session_start", {}, ctx1);
  const first = await bind({ context: ctx1 });
  stages.bound = stage({ health: healthOf(first) });
  stages.before = await emit(`session_before_${kind}`, {}, ctx1);
  // The only path today by which an offer is unclear at completion: /relay-unquiesce while the
  // transition was in fact still pending.
  await commands.get("relay-unquiesce").handler("", ctx1);
  await Bun.sleep(20);
  await offer("41", { session: S1, context: ctx1 });
  delivered = sent.at(-1)?.text ?? null;
  stages.admitted = stage();
  if (cut === "accepted" || cut === "lateack") {
    await journal(sm1, delivered);
    await emit("message_end", {}, ctx1);
    await Bun.sleep(10);
    stages.receipted = stage();
    await terminal("offer_accepted", "41", { session: S1, context: ctx1 });
    stages.accepted = stage();
  }
  const kept = global.gates.get(S1);
  await emit(`session_${kind}`, {}, ctx2);
  await Bun.sleep(30);
  stages.completion = stage({ same_gate: global.gates.get(S1) === kept, current: ctx2.sessionManager.getSessionId(), destination_gate: away ? gateView(S2) : null, destination_owner: away ? ownerFor(S2) : null });

  if (away) {
    // The child's ACK for the source offer, sent before it notices the destination's health.
    if (cut === "accepted") {
      await terminal("identity_acked", "41", { session: S1, context: ctx2 });
      stages.ack_in_flight = stage({ destination_state: global.gates.get(S2)?.state ?? null, health: healthOf(first) });
    }
    // The child rebinds to the current session, as it does when health names another epoch.
    const destination = await bind({ ...AGAIN, context: ctx2 });
    stages.destination_bound = stage({ identity: destination.identity, destination_gate: gateView(S2), destination_owner: ownerFor(S2), health: healthOf(first) });
    if (cut === "lateack") {
      // The same ACK, delayed until after the destination has bound.
      await terminal("identity_acked", "41", { session: S1, context: ctx2 });
      const fenced = stage({ destination_state: global.gates.get(S2)?.state ?? null, health: healthOf(first) });
      await control(RECONCILE, ctx2);
      await Bun.sleep(10);
      fenced.after_reconcile = { destination_state: global.gates.get(S2)?.state ?? null, health: healthOf(first) };
      fenced.return_before_hook = await emit("session_before_switch", {}, ctx2);
      stages.late_ack = fenced;
    }
    if (cut === "admitted") {
      // omp's writer journals the source's line after the switch: is it recorded while S2 is current?
      await journal(sm1, delivered);
      await emit("message_end", {}, ctx2);
      await emit("turn_end", {}, ctx2);
      await Bun.sleep(10);
      stages.late_receipt = stage();
    }
  }
  if (away && cut !== "lateack") {
    const quiet = stage();
    await Bun.sleep(1500);
    await emit("before_agent_start", {}, ctx2);
    await emit("turn_end", {}, ctx2);
    await control(RECONCILE, ctx2);
    await Bun.sleep(10);
    stages.away = stage({ unchanged_owner: JSON.stringify(quiet.owner) === JSON.stringify(ownerFor(S1)), unchanged_gate: JSON.stringify(quiet.source_gate) === JSON.stringify(gateView(S1)), same_gate: global.gates.get(S1) === kept });
    // Return to the source session.
    const back = await emit("session_before_switch", {}, ctx2);
    await emit("session_switch", {}, ctx1);
    await Bun.sleep(30);
    stages.returned = stage({ back, same_gate: global.gates.get(S1) === kept, destination_owner: ownerFor(S2), destination_gate: gateView(S2), health: healthOf(first) });
  }

  // The existing recovery path, as the child runs it. Its periodic rebind_reconcile republishes the
  // source gate's health; health naming an epoch other than the child's makes it rebind.
  if (cut !== "lateack") {
    await control(RECONCILE, ctx1);
    await Bun.sleep(10);
    stages.reconcile = stage({ health: healthOf(first) });
    const childEpoch = away ? SECOND_BINDING : BINDING;
    let binding = childEpoch;
    if (stages.reconcile.health?.binding_epoch !== childEpoch) {
      const rebound = await bind({ ...THIRD, context: ctx1 });
      binding = THIRD.binding;
      stages.rebound = stage({ identity: rebound.identity, health: healthOf(first) });
    }
    if (cut === "admitted" || cut === "unjournalled") {
      await emit("message_end", {}, ctx1);
      await Bun.sleep(10);
      stages.receipt_on_return = stage();
      if (stages.receipt_on_return.witness.receipt_recorded) {
        await terminal("offer_accepted", "41", { session: S1, binding, context: ctx1 });
        stages.accepted_on_return = stage();
      }
    }
    if (mirrorFor(S1, "41")?.broker_terminal_state === "accepted") {
      await terminal("identity_acked", "41", { session: S1, binding, context: ctx1 });
    }
    await Bun.sleep(10);
    stages.recovered = stage({ health: healthOf(first), same_gate: global.gates.get(S1) === kept });
  }
  console.log(JSON.stringify(snapshot({ S1, S2, stages, records: heldRecords() })));
} else {
  await emit("session_start");
  await Bun.sleep(10);
  const requested = await bind();
  console.log(JSON.stringify(snapshot({ identity: requested.identity })));
}
"""


def _run(tmp_path: Path, case: str) -> dict:
    harness = tmp_path / "harness.ts"
    harness.write_text(HARNESS, encoding="utf-8")
    base = tmp_path / "case"
    base.mkdir()
    env = os.environ.copy()
    env.pop("OMP_RELAY_STATE_ROOT", None)
    env.pop("OMP_RELAY_RUNTIME_ROOT", None)
    env.update(
        CASE=case,
        BASE=str(base.resolve()),
        EXTENSION_URL=EXTENSION.resolve().as_uri(),
        NODE_PATH=str(NODE_PATH),
    )
    completed = subprocess.run(
        ["bun", str(harness)],
        cwd=ROOT,
        env=env,
        text=True,
        encoding="utf-8",
        capture_output=True,
        timeout=60,
        check=False,
    )
    assert completed.returncode == 0, f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    assert lines, completed.stderr
    return json.loads(lines[-1])


def _assert_bound(result: dict, binding: str = BINDING) -> None:
    assert result["stateRoot"] == result["expectedState"]
    assert result["runtimeRoot"] == result["expectedRuntime"]
    assert result["gates"] == [SESSION]
    assert result["claims"] == 1
    assert result["identity"]["ok"] is True
    assert result["identity"]["session_id"] == SESSION
    assert result["bound"] == binding
    assert result["statusKind"] == "ready"
    assert result["statusKeys"] == ["omp-relay"]


def _assert_blocked(result: dict, code: str) -> None:
    # The code reaches the status line and one error notice; the wording around it is not tested.
    assert result["statusKind"] == "blocked"
    assert code in result["status"]
    assert any(code in notice["message"] and notice["level"] == "error" for notice in result["notices"])


def _assert_nothing_bound(result: dict) -> None:
    assert result["stateRoot"] is None
    assert result["runtimeRoot"] is None
    assert result["gates"] == []
    assert result["claims"] == 0
    assert result["bound"] is None
    assert result.get("identity") is None


# Plan section 5, case 1: the regression. Fails on bfb2dca, which reads roots only from
# omp's own environment and binds nothing when both variables are absent.
def test_regression_declared_roots_bind_with_both_parent_variables_absent(tmp_path):
    result = _run(tmp_path, "regression-args")
    _assert_bound(result)
    assert result["notices"] == []
    assert result["otherStateEntries"] == []
    assert result["otherRuntimeEntries"] == []


# Case 2: each supported form binds the declared roots.
@pytest.mark.parametrize("case", ["form-env", "form-both", "form-mixed"])
def test_each_supported_form_binds_the_declared_roots(tmp_path, case):
    result = _run(tmp_path, case)
    _assert_bound(result)
    assert result["notices"] == []


# Case 3: each refused form and each omp-side code. Nothing binds; the code reaches the status
# line, one error notice and the log, and never the child.
REFUSED = {
    "unavailable": "RELAY_CONFIG_UNAVAILABLE",
    "missing-rpc": "RELAY_CONFIG_MISSING",
    "root-in-neither": "RELAY_CONFIG_MISSING",
    "non-stdio": "RELAY_CONFIG_INVALID",
    "equals-form": "RELAY_CONFIG_INVALID",
    "abbreviation": "RELAY_CONFIG_INVALID",
    "repeated": "RELAY_CONFIG_INVALID",
    "end-of-options": "RELAY_CONFIG_INVALID",
    "missing-value": "RELAY_CONFIG_INVALID",
    "non-string": "RELAY_CONFIG_INVALID",
    "placeholder": "RELAY_CONFIG_INVALID",
    "batch-command": "RELAY_CONFIG_UNPROVABLE",
    "relative-command": "RELAY_CONFIG_UNPROVABLE",
    "absent-command": "RELAY_CONFIG_UNPROVABLE",
    "env-command": "RELAY_CONFIG_UNPROVABLE",
    "env-variable-name": "RELAY_CONFIG_UNPROVABLE",
    "env-empty": "RELAY_CONFIG_UNPROVABLE",
    "conflict": "RELAY_CONFIG_CONFLICT",
    "state-invalid": "STATE_ROOT_INVALID",
    "runtime-invalid": "RUNTIME_ROOT_INVALID",
}


@pytest.mark.parametrize(("case", "code"), sorted(REFUSED.items()))
def test_refused_declaration_binds_nothing_and_shows_its_code(tmp_path, case, code):
    result = _run(tmp_path, case)
    _assert_nothing_bound(result)
    _assert_blocked(result, code)
    assert result["statusKeys"] == ["omp-relay"]
    assert len(result["notices"]) == 1
    details = [entry["detail"] for entry in result["logs"] if entry["detail"] and entry["detail"].get("code") == code]
    assert details, result["logs"]
    for detail in details:
        assert set(detail) == {"code", "server", "agent_dir", "source"}
        assert detail["server"] == "omp-relay"
    if case != "unavailable":
        assert details[0]["agent_dir"].endswith("agent")


# Case 4: environment indirection. Command-backed, variable-name and empty values are refused
# above; the same variable-name value is accepted when omp keeps it literal.
@pytest.mark.parametrize("case", ["env-variable-name-literal", "env-variable-name-literal-key"])
def test_literal_policy_accepts_a_value_omp_would_otherwise_replace(tmp_path, case):
    result = _run(tmp_path, case)
    _assert_bound(result)


# Case 5: a failed launch keeps its stored entry. Roots may bind; nothing reports ready.
def test_failed_launch_shows_failed_and_binds_no_identity(tmp_path):
    result = _run(tmp_path, "failed-launch")
    assert result["stateRoot"] == result["expectedState"]
    assert result["bound"] is None
    assert result["statusKind"] == "failed"
    assert "ready" not in result["statusKinds"]


# Case 6: no first notification.
def test_tui_without_any_status_event_stays_waiting_and_visible(tmp_path):
    result = _run(tmp_path, "tui-no-entry-no-event")
    _assert_nothing_bound(result)
    assert result["statusKind"] == "waiting"
    assert result["notices"] == []


def test_tui_connecting_event_without_relay_turns_missing(tmp_path):
    result = _run(tmp_path, "tui-connecting-without-relay")
    _assert_nothing_bound(result)
    assert result["before"] == "waiting"
    _assert_blocked(result, "RELAY_CONFIG_MISSING")
    assert len(result["notices"]) == 1


def test_tui_entry_stored_late_binds_on_the_connecting_event(tmp_path):
    result = _run(tmp_path, "tui-late-entry")
    _assert_bound(result)
    assert result["statusKinds"][0] == "waiting"
    assert "starting" in result["statusKinds"]


def test_connected_child_without_identity_request_stays_pending(tmp_path):
    result = _run(tmp_path, "connected-no-identity")
    assert result["stateRoot"] == result["expectedState"]
    assert result["gates"] == [SESSION]
    assert result["bound"] is None
    assert result["statusKind"] == "pending"


# Case 7: ordering.
def test_session_start_overlapping_the_identity_request_binds_once(tmp_path):
    result = _run(tmp_path, "overlap")
    _assert_bound(result)


def test_repeated_events_make_exactly_one_claim(tmp_path):
    result = _run(tmp_path, "repeated-events")
    _assert_bound(result)
    assert json.loads(result["owner"])["holder"]["session_id"] == SESSION


def test_rpc_pending_startup_binds_roots_and_shows_starting(tmp_path):
    result = _run(tmp_path, "rpc-pending")
    assert result["stateRoot"] == result["expectedState"]
    assert result["gates"] == [SESSION]
    assert result["claims"] == 1
    assert result["bound"] is None
    assert result["statusKind"] == "starting"


# No turn starts meanwhile. The fence tests below cover a turn starting while the transition is
# still pending.
@pytest.mark.parametrize("kind", ["switch", "branch", "tree"])
def test_first_notification_during_a_transition_binds_only_after_it_completes(tmp_path, kind):
    result = _run(tmp_path, f"during-{kind}")
    assert result["before"] == [None]
    assert result["middle"] == {"stateRoot": None, "gates": [], "identity": None}
    _assert_bound(result)


def test_first_notification_after_shutdown_never_binds(tmp_path):
    result = _run(tmp_path, "during-shutdown")
    _assert_nothing_bound(result)


def test_subagent_events_in_the_waiting_window_never_bind_or_claim(tmp_path):
    result = _run(tmp_path, "subagent-waiting")
    _assert_nothing_bound(result)
    assert result["statusKinds"] == []


# Case 8: omp's own environment naming DIFFERENT directories does not override the declaration.
def test_parent_environment_roots_are_ignored(tmp_path):
    result = _run(tmp_path, "parent-env-ignored")
    _assert_bound(result)
    assert result["otherStateEntries"] == []
    assert result["otherRuntimeEntries"] == []


# Case 8a: the root-change guard (C-68).
def test_unchanged_declaration_allows_a_new_child_instance(tmp_path):
    result = _run(tmp_path, "guard-unchanged-reconnect")
    assert result["first"]["ok"] is True
    _assert_bound(result, SECOND_BINDING)


def test_changed_runtime_root_is_refused_without_writing(tmp_path):
    result = _run(tmp_path, "guard-runtime-changed")
    assert result["first"]["ok"] is True
    assert result["identity"] is None
    assert result["rendezvousEntries"] == ["control"]
    assert result["stateRoot"] == result["expectedState"]
    assert result["runtimeRoot"] == result["expectedRuntime"]
    assert result["bound"] == BINDING
    _assert_blocked(result, "RELAY_ROOT_CHANGED")
    assert result["ownerUnchanged"] is True
    assert result["otherStateEntries"] == []


@pytest.mark.parametrize("case", ["guard-state-changed", "guard-reload-changed"])
def test_changed_state_root_with_same_runtime_root_is_refused(tmp_path, case):
    result = _run(tmp_path, case)
    assert result["first"]["ok"] is True
    assert result["identity"]["ok"] is False
    assert result["identity"]["code"] == "IDENTITY_INVALID"
    assert result["stateRoot"] == result["expectedState"]
    assert result["runtimeRoot"] == result["expectedRuntime"]
    assert result["bound"] == BINDING
    _assert_blocked(result, "RELAY_ROOT_CHANGED")
    assert result["ownerUnchanged"] is True
    assert result["otherStateEntries"] == []
    assert result["otherRuntimeEntries"] == []


# Review round 1 (65737b6), finding 0: a declaration change between an authorized identity request
# and identity_bound must not commit the binding. The refusal reaches the child only through the
# existing conflict path (health BINDING_CONFLICT); omp shows RELAY_ROOT_CHANGED.
def test_changed_declaration_before_identity_bound_commits_no_binding(tmp_path):
    result = _run(tmp_path, "change-before-bound")
    assert result["authorized"]["ok"] is True
    assert result["bound"] is None
    assert result["health"]["state"] == "blocked"
    assert result["health"]["code"] == "BINDING_CONFLICT"
    _assert_blocked(result, "RELAY_ROOT_CHANGED")
    assert result["stateRoot"] == result["expectedState"]
    assert result["runtimeRoot"] == result["expectedRuntime"]
    assert result["ownerUnchanged"] is True
    assert result["otherStateEntries"] == []


# Review round 1, finding 1: a transition that completes after the declaration changed starts no
# claim work (no new gate, no new claim generation) under the pinned roots.
@pytest.mark.parametrize("kind", ["switch", "branch", "tree"])
def test_changed_declaration_before_transition_completion_claims_nothing(tmp_path, kind):
    result = _run(tmp_path, f"change-before-{kind}")
    assert result["first"]["ok"] is True
    assert result["before"] == [None]
    assert result["allGates"] == []
    assert result["secondOwner"] is None
    assert result["ownerAfter"]["holder"] is None
    _assert_blocked(result, "RELAY_ROOT_CHANGED")
    assert result["stateRoot"] == result["expectedState"]
    assert result["runtimeRoot"] == result["expectedRuntime"]
    assert result["otherStateEntries"] == []


# The same class through /relay-unquiesce: refused, the quiesced gate stays as it was.
def test_changed_declaration_before_unquiesce_restores_nothing(tmp_path):
    result = _run(tmp_path, "change-before-unquiesce")
    assert result["first"]["ok"] is True
    assert result["before"] == [None]
    assert result["gateState"] == "quiescing"
    assert result["ownerUnchanged"] is True
    assert {"message": "RELAY_ROOT_CHANGED", "level": "error"} in result["notices"]
    _assert_blocked(result, "RELAY_ROOT_CHANGED")
    assert result["otherStateEntries"] == []


# Review round 2 (1d1cecc), finding 0: a declaration change while the identity request waits on
# the per-session FIFO yields IDENTITY_INVALID, not a positive authorization.
def test_changed_declaration_during_the_claim_wait_authorizes_nothing(tmp_path):
    result = _run(tmp_path, "change-during-claim-wait")
    assert result["identity"]["ok"] is False
    assert result["identity"]["code"] == "IDENTITY_INVALID"
    assert result["authorizationWritten"] is False
    assert result["pendingApprovals"] == 0
    assert result["bound"] is None
    _assert_blocked(result, "RELAY_ROOT_CHANGED")
    assert result["stateRoot"] == result["expectedState"]
    assert result["otherStateEntries"] == []


# Review round 2, finding 2: a lost child's binding is not shown as ready while omp reconnects,
# nor while the replacement child is connected but unbound.
def test_ready_needs_a_live_connection_and_a_current_binding(tmp_path):
    result = _run(tmp_path, "reconnect-after-bound")
    assert result["boundStatus"] == "ready"
    assert result["reconnectingStatus"] == "starting"
    assert result["replacementStatus"] == "pending"
    _assert_bound(result, SECOND_BINDING)


# Transition regression: a turn event cleared the pre-bind hold, the
# session being left was claimed and bound and admitted a message during the pending transition, and
# the completion then removed that gate and released its owner although its delivery was not clear.
# Turn events prove neither completion nor cancellation, so the hold lasts until omp's own
# completion event. Every broken property is listed, not just the first.
@pytest.mark.parametrize("kind", ["switch", "branch", "tree"])
def test_pending_transition_admits_no_claim_binding_or_delivery_until_its_completion(tmp_path, kind):
    result = _run(tmp_path, f"window-fence-{kind}")
    during = result["during"]
    completion = result["completion"]
    violations = []
    if during["gates"] != []:
        violations.append(f"a gate was born during the pending {kind}: {during['gates']}")
    if during["claims"] != 0 or during["owner"] is not None:
        violations.append(f"the session was claimed during the pending {kind}: claims={during['claims']} owner={during['owner']}")
    if during["identity"] is not None:
        violations.append(f"an identity was answered during the pending {kind}: {during['identity']}")
    if during["bound"] is not None:
        violations.append(f"a binding was committed during the pending {kind}: {during['bound']}")
    if during["admitted"] != 0 or during["mirror"] is not None:
        violations.append(f"a delivery was admitted during the pending {kind}: sent={during['admitted']} mirror={during['mirror']}")
    if not completion["windowGateKept"] and completion["clear"] is not True:
        violations.append(
            f"session_{kind} removed the gate holding an unclear delivery: lifecycle={completion['windowLifecycle']}"
            f" transitionClear={completion['clear']}"
        )
    if completion["windowClaim"] is not None and completion["owner"] != completion["windowClaim"] and completion["clear"] is not True:
        violations.append(f"session_{kind} released or moved the owner while transitionClear was {completion['clear']}")
    assert violations == []
    assert result["before"] == [None]
    destination = SESSION if kind == "tree" else SECOND_SESSION
    assert result["identity"]["ok"] is True
    assert result["identity"]["session_id"] == destination
    assert result["destinationBound"] == SECOND_BINDING
    assert result["destinationOwner"] == destination
    assert result["allGates"] == [destination]
    assert result["statusKind"] == "ready"


# The documented limitation: a pre-bind transition cancelled after the relay's before-hook leaves
# the session held (no claim, no binding, no admission) through turns, the child's requests and
# connection events, and shown as held; the next completed transition binds it.
def test_cancelled_pre_bind_transition_holds_until_the_next_completed_transition(tmp_path):
    result = _run(tmp_path, "window-cancelled-held")
    held = result["held"]
    assert result["before"] == [None]
    assert held["gates"] == []
    assert held["claims"] == 0
    assert held["owner"] is None
    assert held["identity"] is None
    assert held["admitted"] == 0
    assert result["statusBefore"] == "waiting"
    assert held["status"] == "held"
    assert result["later"] == [None]
    _assert_bound(result, SECOND_BINDING)


# The same limitation once bound: the gate stays quiescing, refuses delivery and refuses the next
# transition, and only /relay-unquiesce (interactive mode) restores it.
def test_cancelled_transition_of_a_bound_session_stays_quiescing_until_unquiesce(tmp_path):
    result = _run(tmp_path, "window-cancelled-bound")
    held = result["held"]
    assert result["before"] == [None]
    assert held["gateState"] == "quiescing"
    assert held["admitted"] == 0
    assert held["mirror"] is None
    assert held["health"] == "DELIVERY_PENDING"
    assert result["next"] == [{"cancel": True}]
    assert result["restoredState"] == "open"
    assert result["restoredAdmitted"] == 1
    assert result["restoredMirror"] == "injected"


# Completion is not a delivery barrier. A gate restored by /relay-unquiesce while the switch was in
# fact still pending admits a message; admission, journal receipt and the child's offer_accepted are
# each observed, and identity_acked never arrives. The completion must leave that gate and its lock.
def test_completion_keeps_a_gate_whose_delivery_is_not_acknowledged(tmp_path):
    result = _run(tmp_path, "window-unquiesce-unclear")
    admitted = result["admitted"]
    receipted = result["receipted"]
    accepted = result["accepted"]
    assert result["before"] == [None]
    assert result["replacementState"] == "open"
    # Admission: omp accepted the send call; the mirror says injected and nothing more.
    assert admitted["sent"] == 1
    assert admitted["mirror"]["state"] == "injected"
    assert admitted["mirror"]["application_acceptance_observed"] is False
    assert admitted["mirror"]["broker_terminal_state"] == "none"
    assert admitted["clear"] is False
    # Journal receipt: the exact user line in the session file.
    assert receipted["mirror"]["state"] == "accepted"
    assert receipted["mirror"]["application_acceptance_observed"] is True
    assert receipted["mirror"]["broker_terminal_state"] == "none"
    assert receipted["clear"] is False
    # Internal ACK, first half: broker-terminal accepted, identity acknowledgement still false.
    assert accepted["mirror"]["broker_terminal_state"] == "accepted"
    assert accepted["mirror"]["identity_acknowledged"] is False
    assert accepted["clear"] is False
    assert result["completionClear"] is False
    violations = []
    if not result["replacementKept"]:
        violations.append(f"session_switch removed the gate holding the unacknowledged delivery: lifecycle={result['replacementLifecycle']}")
    if result["replacementLifecycle"] != "live":
        violations.append(f"session_switch retired that gate's claim: {result['replacementLifecycle']}")
    if result["oldOwnerClaim"] != result["replacementClaim"]:
        violations.append(f"session_switch released or moved the owner: holder claim {result['oldOwnerClaim']}")
    assert violations == []
    assert sorted(result["allGates"]) == sorted([SESSION, SECOND_SESSION])


# C-55, same window: a /relay-unquiesce replacement made while the switch is in flight is retired
# at completion and its lock released when its deliveries are clear, instead of being dropped from
# the map with the lock held.
def test_unquiesce_replacement_during_a_switch_is_retired_at_completion(tmp_path):
    result = _run(tmp_path, "window-unquiesce-switch")
    assert result["before"] == [None]
    assert result["ownerDuringClaim"] == result["replacementClaim"]
    assert result["allGates"] == [SECOND_SESSION]
    assert result["oldOwner"]["holder"] is None
    assert result["secondOwner"]["holder"]["session_id"] == SECOND_SESSION
    assert result["replacementLifecycle"] in {"retired", "released"}


# Review round 3, finding 0: a replaced connection with no connecting/reconnecting event (omp's
# manual reconnect) is not ready until the child on the new connection binds.
def test_replaced_connection_is_not_ready_until_its_child_binds(tmp_path):
    result = _run(tmp_path, "manual-reconnect-after-bound")
    assert result["boundStatus"] == "ready"
    assert result["replacementStatus"] == "pending"
    _assert_bound(result, SECOND_BINDING)


# A manual reconnect that emits nothing until its end leaves no supported surface to observe and
# nothing polls, so the line says what it is: readiness as last observed. The next re-evaluation
# event shows the reconnect, and the replacement is ready only once its own child binds.
def test_event_silent_manual_reconnect_shows_readiness_as_last_observed(tmp_path):
    result = _run(tmp_path, "manual-reconnect-silent")
    assert result["boundStatus"] == "ready"
    assert result["silentStatus"] == "ready"
    assert result["silentChanges"] == 0
    assert result["nextEventStatus"] == "starting"
    assert result["replacementStatus"] == "pending"
    _assert_bound(result, SECOND_BINDING)


# Connection-attribution regression: child A's delayed identity_bound,
# arriving after omp replaced A's connection with B, committed and credited B, so the line read
# ready before B's own child had bound. Readiness belongs to the connection A was authorized over.
def test_late_identity_bound_after_replacement_credits_no_connection(tmp_path):
    result = _run(tmp_path, "attribution-late-bound")
    after_a = result["afterA"]
    violations = []
    if after_a["bound"] is not None:
        violations.append(f"A's late identity_bound committed binding {after_a['bound']}")
    if after_a["boundConnection"] == 2:
        violations.append("A's late identity_bound credited replacement connection B")
    if after_a["status"] == "ready":
        violations.append(f"ready was shown before B's own child bound: {after_a['status']}")
    assert violations == []
    assert result["identityA"]["ok"] is True
    assert result["approvalsAfterRequest"] == 1
    # The refusal reaches the child only through the existing conflict path.
    assert after_a["health"]["code"] == "BINDING_CONFLICT"
    assert after_a["status"] == "pending"
    # Gate, claim and roots are untouched: B is authorized under the same generations as A.
    assert after_a["sameGate"] is True
    assert result["authorizedA"]["gate_generation"] == result["gateGeneration"]
    assert result["authorizedA"]["claim_generation"] == result["claimGeneration"]
    assert result["authorizedB"]["gate_generation"] == result["gateGeneration"]
    assert result["authorizedB"]["claim_generation"] == result["claimGeneration"]
    assert result["boundConnection"] == 2
    _assert_bound(result, SECOND_BINDING)
    assert result["otherStateEntries"] == []


# The same, with the replacement landing while A's identity request waits for the session claim:
# A gets a closed refusal and no authorization, and only B's own identity makes the line ready.
def test_connection_replaced_during_the_claim_wait_authorizes_nothing(tmp_path):
    result = _run(tmp_path, "attribution-claim-wait")
    identity_a = result["identityA"]
    after_a = result["afterA"]
    violations = []
    if identity_a is None or identity_a.get("ok") is not False:
        violations.append(f"A was answered with {identity_a} after its connection was replaced")
    if result["authorizedA"] is not None:
        violations.append("an AUTHORIZE_BIND request was written for A")
    if result["approvalsAfterRequest"] != 0:
        violations.append(f"{result['approvalsAfterRequest']} approval(s) recorded for A")
    if after_a["bound"] is not None or after_a["boundConnection"] == 2:
        violations.append(f"A bound {after_a['bound']} and credited connection {after_a['boundConnection']}")
    if after_a["status"] == "ready":
        violations.append(f"ready was shown before B's own child bound: {after_a['status']}")
    assert violations == []
    # The child protocol stays closed: one of the six codes, the six refusal fields.
    assert set(identity_a) == IDENTITY_REFUSAL_FIELDS
    assert identity_a["code"] == "CONTROL_CHANNEL_INVALID"
    assert identity_a["code"] in IDENTITY_REFUSAL_CODES
    assert after_a["sameGate"] is True
    assert result["authorizedB"]["gate_generation"] == result["gateGeneration"]
    assert result["authorizedB"]["claim_generation"] == result["claimGeneration"]
    assert result["boundConnection"] == 2
    _assert_bound(result, SECOND_BINDING)
    assert result["otherStateEntries"] == []


# Held states (2026-10-09): a session held by a session transition never shows a ready status, and
# the log records entry to and exit from each held state with its reason and the event that caused
# it. Turns, a timer, relay notifications and connection events do not end a hold; only the event
# that ends it today does. Each case lists every broken property, not just the first.
def _held_violations(result: dict, session: str, exit_label: str, expected: list) -> list:
    violations = []
    held = True
    for item in result["trail"]:
        if item["label"] == exit_label:
            held = False
        if held and item["kind"] != "held":
            violations.append(f"after {item['label']} the status kind was {item['kind']!r} ({item['text']!r}), not held")
        if not held and item["kind"] == "held":
            violations.append(f"after {item['label']} the status still read held")
    records = [(r["held"], r["reason"], r["event"]) for r in result["records"] if r["session_id"] == session]
    if records != expected:
        violations.append(f"held log records were {records}, expected {expected}")
    return violations


@pytest.mark.parametrize(("variant", "exit_event"), [("completed", "session_switch"), ("cancelled", "session_tree")])
def test_pre_bind_hold_is_shown_and_logged_as_held_until_a_transition_completes(tmp_path, variant, exit_event):
    result = _run(tmp_path, f"held-pre-bind-{variant}")
    reason = "TRANSITION_PENDING_BEFORE_BIND"
    expected = [("entered", reason, "session_before_switch"), ("left", reason, exit_event)]
    assert _held_violations(result, SESSION, exit_event, expected) == []
    assert result["statusKinds"][0] == "waiting"


@pytest.mark.parametrize("variant", ["completed", "unbound", "unquiesced"])
def test_quiescing_session_is_shown_and_logged_as_held_until_its_exit(tmp_path, variant):
    result = _run(tmp_path, f"held-quiescing-{variant}")
    quiesced = "TRANSITION_PENDING_QUIESCED"
    if variant == "unquiesced":
        restoring = "RESTORING_AFTER_UNQUIESCE"
        exit_label = "claim_result"
        expected = [
            ("entered", quiesced, "session_before_switch"),
            ("left", quiesced, "relay-unquiesce"),
            ("entered", restoring, "relay-unquiesce"),
            ("left", restoring, "claim_result"),
        ]
    else:
        exit_label = "session_switch"
        expected = [("entered", quiesced, "session_before_switch"), ("left", quiesced, "session_switch")]
    assert _held_violations(result, SESSION, exit_label, expected) == []
    assert result["ready"] == ("pending" if variant == "unbound" else "ready")
    if variant == "unquiesced":
        assert result["statusKind"] == "ready"


def test_gate_kept_at_an_unclear_completion_is_shown_and_logged_as_held_until_acknowledged(tmp_path):
    result = _run(tmp_path, "held-retained-tree")
    assert result["unclear"]["clear"] is False
    assert result["unclear"]["mirror"]["broker_terminal_state"] == "accepted"
    assert result["unclear"]["mirror"]["identity_acknowledged"] is False
    quiesced = "TRANSITION_PENDING_QUIESCED"
    restoring = "RESTORING_AFTER_UNQUIESCE"
    unresolved = "DELIVERY_UNRESOLVED_AT_COMPLETION"
    expected = [
        ("entered", quiesced, "session_before_tree"),
        ("left", quiesced, "relay-unquiesce"),
        ("entered", restoring, "relay-unquiesce"),
        ("left", restoring, "claim_result"),
        ("entered", unresolved, "session_tree"),
        ("left", unresolved, "mcp_notification"),
    ]
    assert _held_violations(result, SESSION, "identity_acked", expected) == []
    assert result["clear"] is True
    assert result["statusKind"] == "ready"


# Final review, finding 1 (2026-10-09). Work S1 started while current finishes after the switch to S2:
#   bind-s2-unbound  S1's identity_bound, authorized before the switch, arrives before S2 binds
#   bind-s2-bound    the same after S2 has bound
#   request-wait     S1's identity request is still in its claim wait when the switch completes
#   claim-result     a claim S1 queued lands after the switch
# None may commit S1's binding, move the child's last committed binding, write the child's health,
# fence S2 or change either lock. Each property is asserted separately.
@pytest.mark.parametrize("variant", ["bind-s2-unbound", "bind-s2-bound", "request-wait", "claim-result"])
def test_late_source_work_does_not_touch_the_current_session(tmp_path, variant):
    result = _run(tmp_path, f"late-{variant}")
    before, after = result["before"], result["after"]
    v = []
    if after["health"] != before["health"]:
        v.append(f"the child's health was overwritten: {before['health']} -> {after['health']}")
    if after["boundBinding"] != before["boundBinding"]:
        v.append(f"the child's last committed binding moved: {before['boundBinding']} -> {after['boundBinding']}")
    if after["status"] != before["status"]:
        v.append(f"S2's readiness changed: {before['status']} -> {after['status']}")
    if after["s1"]["gate"]["binding"] != before["s1"]["gate"]["binding"]:
        v.append(f"S1's binding was committed: {before['s1']['gate']['binding']} -> {after['s1']['gate']['binding']}")
    if after["s1"]["owner"] != before["s1"]["owner"] or not result["sameGate"]:
        v.append(f"S1's gate or lock changed: {before['s1']} -> {after['s1']}")
    if after["s2"]["owner"] != before["s2"]["owner"]:
        v.append(f"S2's lock changed: {before['s2']['owner']} -> {after['s2']['owner']}")
    if after["s2"]["gate"] != before["s2"]["gate"]:
        v.append(f"S2's gate changed: {before['s2']['gate']} -> {after['s2']['gate']}")
    if result["navigation"] != [None]:
        v.append(f"a switch away from S2 was refused: {result['navigation']}")
    if variant == "request-wait":
        identity = result["identity"]
        if identity is None or identity.get("ok") is not False or identity.get("code") != "IDENTITY_INVALID":
            v.append(f"S1's identity request was answered after the switch: {identity}")
        if result["authorization"] is not None:
            v.append("an AUTHORIZE_BIND request was written for S1 after the switch")
    assert result["before"]["s1"]["gate"]["retained"] is True
    assert v == []


# F2 (2026-10-09). A terminal result is charged to the session its binding proves, not to whichever
# session is current. Each case is driven while S2 is current with S1's gate kept, and asserts
# separately: S1's and S2's mirrors, both binding tuples, both owner locks, the receipt and ACK
# state, the current child's health, S2's readiness, and navigation from S2.
#   a   S1's ACK before S2 binds          b   S1's ACK after S2 binds
#   b2  as b, while S2 has its own unacknowledged delivery (real unresolved delivery blocks navigation)
#   d   duplicate terminal results        e1  wrong offer token for S1's binding (invalid, provable)
#   e2  stale binding for S1 (unprovable) e3  a session with no gate (unprovable)
#   f   a genuinely invalid control for S2 itself: it still fences S2
F2_ACKED = {"a", "b", "b2", "d"}


@pytest.mark.parametrize("nav", ["switch", "branch", "tree"])
@pytest.mark.parametrize("case", ["a", "b", "b2", "d", "e1", "e2", "e3", "f"])
def test_terminal_result_is_charged_to_its_proven_session(tmp_path, case, nav):
    result = _run(tmp_path, f"f2-{case}-{nav}")
    before, after, steps = result["before"], result["after"], result["steps"]
    s1b, s1a, s2b, s2a = before["s1"], after["s1"], before["s2"], after["s2"]
    v = []
    # Preconditions: S1 kept with an accepted, unacknowledged offer; S2 current.
    assert s1b["gate"]["retained"] is True and s1b["clear"] is False
    assert s1b["mirror"]["broker_terminal_state"] == "accepted" and s1b["mirror"]["identity_acknowledged"] is False
    # S1 mirror: the receipt and ACK state of S1's own offer.
    if case in F2_ACKED:
        m = s1a["mirror"]
        if not (m["identity_acknowledged"] and m["broker_terminal_state"] == "accepted" and m["state"] == "accepted" and m["offer_token"] == s1b["mirror"]["offer_token"]):
            v.append(f"S1's valid result was not recorded on S1: {m}")
        if s1a["clear"] is not True:
            v.append("S1's barrier is not clear after its own acknowledgement")
    elif s1a["mirror"] != s1b["mirror"]:
        v.append(f"S1's mirror was rewritten: {s1a['mirror']}")
    # S2 mirror.
    if s2a["mirror"] != s2b["mirror"]:
        v.append(f"S2's mirror was rewritten: {s2a['mirror']}")
    # Binding tuples and owner locks of both sessions.
    for side, b, a in (("S1", s1b, s1a), ("S2", s2b, s2a)):
        if (a["gate"] or {}).get("binding") != (b["gate"] or {}).get("binding"):
            v.append(f"{side}'s binding changed: {b['gate']} -> {a['gate']}")
        if a["owner"] != b["owner"]:
            v.append(f"{side}'s owner lock changed: {b['owner']} -> {a['owner']}")
    # S1's gate.
    if case in F2_ACKED:
        if (s1a["gate"]["state"], s1a["gate"]["retained"], s1a["gate"]["fence"]) != ("open", False, None):
            v.append(f"S1's gate after its acknowledgement: {s1a['gate']}")
    elif case == "e1":
        if (s1a["gate"]["state"], s1a["gate"]["fence"]) != ("event_fenced", "RECEIPT_CONFLICT"):
            v.append(f"S1's invalid result did not fence S1: {s1a['gate']}")
    elif s1a["gate"] != s1b["gate"]:
        v.append(f"S1's gate changed: {s1b['gate']} -> {s1a['gate']}")
    # S2's gate, the current child's health and S2's readiness.
    if case == "f":
        if (s2a["gate"]["state"], s2a["gate"]["fence"]) != ("event_fenced", "RECEIPT_CONFLICT"):
            v.append(f"S2's invalid control did not fence S2: {s2a['gate']}")
        if (after["health"]["code"], after["health"]["binding_epoch"]) != ("RECEIPT_CONFLICT", SECOND_BINDING):
            v.append(f"S2's health after its own invalid control: {after['health']}")
        if after["status"] != "fenced":
            v.append(f"S2's status after its own invalid control: {after['status']}")
    else:
        if s2a["gate"] != s2b["gate"]:
            v.append(f"S2's gate changed: {s2b['gate']} -> {s2a['gate']}")
        if after["health"] != before["health"]:
            v.append(f"the current child's health was overwritten: {before['health']} -> {after['health']}")
        expected_status = "pending" if case == "a" else "ready"
        if (before["status"], after["status"]) != (expected_status, expected_status):
            v.append(f"S2's readiness changed or is wrong: {before['status']} -> {after['status']}")
    # Navigation from S2.
    if case == "b2":
        if steps["blocked"] != [{"cancel": True}]:
            v.append(f"S2's own unresolved delivery did not block {nav}: {steps['blocked']}")
        if steps["blockedView"]["s1"]["mirror"] != s1a["mirror"]:
            v.append("the refused navigation rewrote S1")
    want = [{"cancel": True}] if case == "f" else [None]
    if steps["navigation"] != want:
        v.append(f"{nav} from S2 returned {steps['navigation']}, expected {want}")
    if case == "a" and steps["s2Bound"]["status"] != "ready":
        v.append(f"S2 did not become ready once bound: {steps['s2Bound']['status']}")
    # After the navigation.
    if "completed" in steps:
        done = steps["completed"]
        if (done["s1"]["gate"], done["s1"]["mirror"], done["s1"]["owner"]) != (s1a["gate"], s1a["mirror"], s1a["owner"]):
            v.append(f"S2's tree navigation changed S1: {done['s1']}")
    if "returned" in steps:
        back = steps["returned"]
        if not back["sameGate"] or back["s1"]["owner"] != s1a["owner"]:
            v.append(f"returning to S1 did not reinstate its kept gate and lock: {back['s1']}")
        if back["s2"]["owner"] is not None:
            v.append(f"S2's lock was not released by the completed return: {back['s2']['owner']}")
        expected_back = "pending" if case in F2_ACKED else "held"
        if back["status"] != expected_back:
            v.append(f"S1's readiness on return, before the child rebinds it: {back['status']} (expected {expected_back})")
        rec = steps["recovered"]
        if (rec["s1"]["mirror"]["identity_acknowledged"], rec["s1"]["clear"], rec["s1"]["gate"]["state"], rec["status"]) != (True, True, "open", "ready"):
            v.append(f"S1 did not recover through the existing path: {rec['s1']['gate']} {rec['status']}")
        if not rec["sameGate"] or rec["s1"]["owner"] != s1a["owner"]:
            v.append("S1's recovery changed its gate or lock")
    assert v == []


# F1 (2026-10-09): every path that can reopen a gate goes through the bound-gate check: a replayed
# identity_acked, an offer_rejected for an acknowledged offer, a malformed-mirror recovery and a
# prior-process recovery status. Each arrives while a bound, clear gate is quiescing for a switch
# that has not completed; none may change the gate, an offer or the entries, or let a delivery in.
@pytest.mark.parametrize("variant", ["identity-acked", "offer-rejected", "terminal-recovery", "recovery-status"])
def test_quiescing_gate_takes_no_terminal_control_or_recovery_before_its_completion(tmp_path, variant):
    result = _run(tmp_path, f"fence-quiescing-{variant}")
    assert result["setup"] == {"clear": True, "state": "open"}
    assert result["hook"] == [None]
    assert result["quiesced"] == {"state": "quiescing", "status": "held"}
    after = result["after"]
    second = result["second"]
    violations = []
    if after["state"] != "quiescing" or not after["sameGate"]:
        violations.append(f"the gate changed to {after['state']} (same object: {after['sameGate']})")
    if after["status"] != "held":
        violations.append(f"the status kind became {after['status']}")
    if not after["offersUnchanged"]:
        violations.append("an offer mirror was rewritten")
    if after["entries"] != 0:
        violations.append(f"{after['entries']} delivery entr(y/ies) created")
    if after["heldChanges"]:
        violations.append(f"the hold changed: {after['heldChanges']}")
    if second["sent"] != 1 or second["mirror42"] is not None:
        violations.append(f"a delivery was admitted during the pending switch: sent={second['sent']} mirror={second['mirror42']}")
    assert violations == []


# F3 (2026-10-09): a gate fenced by an error shows a status kind that is not ready, through turns and
# connection events, and the log records the fence with its code; the existing reopening path (here
# the child's rebind) ends it. The delivery barrier is not an error and still shows ready.
# Since F2 a control for another session fences no gate, so the receipt-conflict variant (a result
# for the current session naming an offer it lacks) replaced the foreign-session one.
@pytest.mark.parametrize(("variant", "code", "fenced_by"), [
    ("protocol", "PROTOCOL_INVALID", "mcp_notification"),
    ("receipt-conflict", "RECEIPT_CONFLICT", "mcp_notification"),
    ("tool-surface", "PROTOCOL_INVALID", "mcp_notification"),
    ("tool-surface-timer", "PROTOCOL_INVALID", "tool_surface_check"),
    ("barrier", None, None),
])
def test_error_fenced_gate_is_not_shown_ready_and_its_fence_is_logged(tmp_path, variant, code, fenced_by):
    result = _run(tmp_path, f"fence-error-{variant}")
    kinds = {item["label"]: item["kind"] for item in result["trail"]}
    assert result["fencedState"] == "event_fenced"
    assert result["finalState"] == "open"
    assert kinds["bound"] == "ready"
    assert kinds["reopen"] == "ready"
    fences = [(r["fenced"], r["reason"], r["event"], r["session_id"]) for r in result["fences"]]
    violations = []
    if code is None:
        for label in ("error", "turn_end", "connected"):
            if kinds[label] != "ready":
                violations.append(f"the delivery barrier showed {kinds[label]!r} after {label}")
        if fences:
            violations.append(f"the delivery barrier was logged as a fence: {fences}")
    else:
        for label in ("error", "turn_end", "connected"):
            if kinds[label] in ("ready", None):
                violations.append(f"after {label} the error-fenced gate showed {kinds[label]!r}")
        expected = [("entered", code, fenced_by, SESSION), ("left", code, "mcp_notification", SESSION)]
        if fences != expected:
            violations.append(f"fence log records were {fences}, expected {expected}")
    assert violations == []


# Retained-owner recovery (2026-10-09). An offer of the source session is driven to a cut point and
# a transition then completes while it is unclear, so the source gate keeps its claim and the owner
# lock. Admission, native journal receipt, terminal acceptance and internal ACK are separate
# witnesses. The run is printed in full for the evidence; the assertions are the safety properties
# and the recovery outcome it measures.
RETAINED = [
    "retained-switch-accepted",
    "retained-branch-accepted",
    "retained-tree-accepted",
    "retained-switch-admitted",
    "retained-switch-unjournalled",
    "retained-switch-lateack",
]


@pytest.mark.skipif(not OMP_SESSION_MODULE.is_file(), reason=f"omp session module not installed at {OMP_SESSION_MODULE}")
@pytest.mark.parametrize("case", RETAINED)
def test_retained_owner_across_a_completed_transition(tmp_path, case):
    result = _run(tmp_path, case)
    print(json.dumps({"case": case, "S1": result["S1"], "S2": result["S2"], "stages": result["stages"], "records": result["records"]}, sort_keys=True))
    stages = result["stages"]
    kind, cut = case.split("-")[1:]
    admitted = stages["admitted"]["witness"]
    assert (admitted["admitted"], admitted["journalled"], admitted["mirror_state"], admitted["terminal"], admitted["acked"], admitted["clear"]) == (
        1, False, "injected", "none", False, False)
    if cut in ("accepted", "lateack"):
        receipted = stages["receipted"]["witness"]
        assert (receipted["journalled"], receipted["mirror_state"], receipted["receipt_recorded"], receipted["terminal"], receipted["acked"]) == (
            True, "accepted", True, "none", False)
        accepted = stages["accepted"]["witness"]
        assert (accepted["terminal"], accepted["acked"], accepted["clear"]) == ("accepted", False, False)
    # Completion kept the source gate, its live claim and the owner lock, exactly.
    completion = stages["completion"]
    gate = completion["source_gate"]
    assert completion["same_gate"] is True
    assert gate["retained"] is True
    assert (gate["claim"]["lifecycle"], gate["claim"]["ownership"]) == ("live", "self")
    assert completion["owner"]["holder"] == gate["claim"]["expected_owner"]
    assert completion["witness"]["clear"] is False
    holder = completion["owner"]["holder"]
    for name, item in stages.items():
        if isinstance(item, dict) and "owner" in item and name not in ("bound", "admitted", "receipted", "accepted"):
            assert item["owner"]["holder"] == holder, name
    if kind != "tree":
        assert completion["current"] == result["S2"]
        # While S1 is not current its receipt cannot be recorded (receipts are scanned for the current
        # session), but S1's own acknowledgement, proven by S1's binding, is recorded on S1 (F2). The
        # destination is neither fenced nor rewritten by it.
        for name in ("ack_in_flight", "destination_bound", "late_receipt", "away"):
            if name in stages:
                progressed = cut == "accepted"
                assert stages[name]["witness"]["acked"] is progressed, name
                assert stages[name]["witness"]["clear"] is progressed, name
        if "ack_in_flight" in stages:
            assert stages["ack_in_flight"]["destination_state"] == "rebind_blocked"
            assert stages["ack_in_flight"]["health"]["code"] == "REBINDING"
    if cut == "lateack":
        late = stages["late_ack"]
        assert (late["witness"]["acked"], late["witness"]["clear"]) == (True, True)
        assert (late["destination_state"], late["status"], late["health"]["code"]) == ("open", "ready", "CONNECTED")
        assert late["return_before_hook"] == [None]
        return
    if kind != "tree":
        assert stages["away"]["unchanged_owner"] is True
        assert stages["away"]["same_gate"] is True
        assert stages["returned"]["same_gate"] is True
        # Acknowledged while away: clear, but not ready until the child binds S1 again.
        assert stages["returned"]["status"] == ("pending" if cut == "accepted" else "held")
    recovered = stages["recovered"]
    if cut == "unjournalled":
        assert (recovered["witness"]["acked"], recovered["witness"]["clear"], recovered["status"]) == (False, False, "held")
    else:
        assert (recovered["witness"]["acked"], recovered["witness"]["clear"]) == (True, True)
        assert recovered["source_gate"]["state"] == "open"
        assert recovered["status"] == "ready"


SMOKE = r"""
import { mkdirSync, realpathSync, writeFileSync } from "node:fs";
import * as path from "node:path";

const base = process.env.BASE!;
const project = path.join(base, "project");
const agentDir = process.env.PI_CODING_AGENT_DIR!;
const stateRoot = path.join(base, "state");
const runtimeRoot = path.join(base, "runtime");
for (const directory of [project, agentDir, stateRoot, runtimeRoot]) mkdirSync(directory, { recursive: true });
const mcpJson = path.join(agentDir, "mcp.json");
// The child is a real direct executable that exits at once: omp stores the entry, the launch
// fails, and no relay process ever runs.
writeFileSync(mcpJson, JSON.stringify({
  mcpServers: {
    "omp-relay": {
      command: process.env.CHILD_EXE,
      args: ["-c", "import sys; sys.exit(0)", "--state-root", stateRoot, "--runtime-root", runtimeRoot],
    },
  },
}, null, 2));

const omp = await import("@oh-my-pi/pi-coding-agent/mcp");
const loaded = await omp.loadAllMCPConfigs(project, { enableProjectConfig: true, filterExa: true });
// Only the `omp-relay` entry is ever connected; other discovered servers are neither launched
// nor printed.
if (!loaded.configs["omp-relay"]) {
  console.log(JSON.stringify({ aborted: "omp-relay not discovered" }));
  process.exit(0);
}
const manager = new omp.MCPManager(project);
await manager.connectServers({ "omp-relay": loaded.configs["omp-relay"] }, { "omp-relay": loaded.sources["omp-relay"] }, undefined, 5000);
omp.MCPManager.setInstance(manager);

const relay = await import(process.env.EXTENSION_URL!);
relay.__testing.reset();
const handlers = new Map();
const statuses = [];
const api = {
  on(name, fn) { const values = handlers.get(name) ?? []; values.push(fn); handlers.set(name, values); },
  registerCommand() {},
  getActiveTools() { return []; },
  async setActiveTools() {},
  getAllTools() { return []; },
  logger: { info() {}, warn() {}, error() {}, debug() {} },
};
relay.default(api);
const ctx = {
  agent: { kind: "main" },
  mode: "rpc",
  hasUI: true,
  sessionManager: { getSessionId() { return "0197f0c2-552b-7db4-8d43-6c338d7d3021"; } },
  setTimeout() { return 0; },
  ui: { setStatus(key, text) { statuses.push(relay.__testing.getGlobalState().relay_status_kind ?? null); }, notify() {} },
};
for (const fn of handlers.get("session_start") ?? []) await fn({}, ctx);
const global = relay.__testing.getGlobalState();
console.log(JSON.stringify({
  source: manager.getSource("omp-relay")?.path ?? null,
  mcpJson: realpathSync.native(mcpJson),
  storedArgs: manager.getServerConfig("omp-relay")?.args ?? null,
  connection: manager.getConnectionStatus("omp-relay"),
  stateRoot: global.state_root,
  runtimeRoot: global.runtime_root,
  expectedState: realpathSync.native(stateRoot),
  expectedRuntime: realpathSync.native(runtimeRoot),
  gates: [...global.gates.keys()],
  statuses,
}));
await manager.disconnectAll();
process.exit(0);
"""


# Case 10: the resolver against omp's REAL manager module. This proves the resolver reads what the
# real manager stored; it cannot prove that a normally loaded extension shares omp's own manager
# instance, which is the host integration gate.
@pytest.mark.skipif(os.name != "nt", reason="the smoke links omp's Windows global install")
@pytest.mark.skipif(not OMP_MCP_MODULE.is_file(), reason=f"omp MCP module not installed at {OMP_MCP_MODULE}")
def test_resolver_reads_the_entry_stored_by_omps_real_manager(tmp_path):
    base = (tmp_path / "smoke").resolve()
    agent = base / "agent"
    base.mkdir(parents=True)
    # Bun resolves a bare specifier from the importing file's directory and, without a
    # node_modules there, through its own install cache, which loads a second copy of omp.
    # A junction to the installed packages, next to a copy of the extension, makes the smoke
    # and the extension load the SAME omp module, as omp's own bundle does for an extension.
    modules = base / "node_modules"
    linked = subprocess.run(
        ["cmd.exe", "/d", "/c", "mklink", "/J", str(modules), str(NODE_PATH)],
        text=True,
        capture_output=True,
        check=False,
    )
    assert linked.returncode == 0, linked.stderr or linked.stdout
    try:
        extension = base / "harness" / "omp" / "omp_relay_extension.ts"
        extension.parent.mkdir(parents=True)
        shutil.copyfile(EXTENSION, extension)
        assert extension.read_bytes() == EXTENSION.read_bytes()
        script = base / "smoke.ts"
        script.write_text(SMOKE, encoding="utf-8")
        env = os.environ.copy()
        env.pop("OMP_RELAY_STATE_ROOT", None)
        env.pop("OMP_RELAY_RUNTIME_ROOT", None)
        # A disposable agent directory and project: the `omp-relay` entry can come only from the
        # smoke's own mcp.json, which the source assertion below checks.
        env.update(
            BASE=str(base),
            PI_CODING_AGENT_DIR=str(agent),
            CHILD_EXE=str(Path(sys.executable).resolve()),
            EXTENSION_URL=extension.as_uri(),
        )
        completed = subprocess.run(
            ["bun", "--no-install", str(script)],
            cwd=base,
            env=env,
            text=True,
            encoding="utf-8",
            capture_output=True,
            timeout=120,
            check=False,
        )
    finally:
        # Removes the junction itself, never the installed packages it points at.
        os.rmdir(modules)
    assert completed.returncode == 0, f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
    lines = [line for line in completed.stdout.splitlines() if line.strip().startswith("{")]
    assert lines, completed.stderr
    result = json.loads(lines[-1])
    assert "aborted" not in result, result
    assert result["source"] is not None
    assert Path(result["source"]).resolve() == Path(result["mcpJson"]).resolve()
    assert result["storedArgs"][-4:] == ["--state-root", str(base / "state"), "--runtime-root", str(base / "runtime")]
    assert result["stateRoot"] == result["expectedState"]
    assert result["runtimeRoot"] == result["expectedRuntime"]
    assert result["gates"] == [SESSION]
    assert result["statuses"][-1] in {"starting", "failed", "pending"}
    print(json.dumps(result, indent=2))
