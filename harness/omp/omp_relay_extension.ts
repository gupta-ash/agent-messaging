import { dlopen, FFIType, ptr } from "bun:ffi";
import {
	closeSync,
	constants as fsConstants,
	existsSync,
	fsyncSync,
	lstatSync,
	mkdirSync,
	openSync,
	readFileSync,
	realpathSync,
	renameSync,
	rmSync,
	statSync,
	unlinkSync,
	writeFileSync,
} from "node:fs";
import {
	generateKeyPairSync,
	sign as cryptoSign,
	type KeyObject,
} from "node:crypto";
import * as path from "node:path";
import { withFileLockSync } from "@oh-my-pi/pi-utils";

const SERVER_NAME = "omp-relay";
const MESSAGE_METHOD = "notifications/omp/relay/message";
const CONTROL_METHOD = "notifications/omp/relay/control";
const GLOBAL_KEY = Symbol.for("omp.relay.deliverySingleFlight.v1");
const PROCESS_EPOCH_KEY = Symbol.for("omp.relay.processEpoch");
const SCHEMA_VERSION = 1;
const APPROVAL_MAX_AGE_MS = 30_000;
const JS_SAFE_INT_MAX = Number.MAX_SAFE_INTEGER;
const EVENT_ID = /^[1-9][0-9]{0,18}$/;
const UUID_TEXT = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const HASH_TEXT = /^[0-9a-f]{64}$/;
const RELAY_NAME = /^[a-z0-9._-]{1,64}$/;
const EXPECTED_TOOL_NAMES = new Set([
	"mcp__omp_relay_register",
	"mcp__omp_relay_name",
	"mcp__omp_relay_check",
	"mcp__omp_relay_send",
]);
const MIRROR_FIELDS = new Set([
	"schema_version",
	"session_id",
	"recipient_id",
	"event_id",
	"offer_token",
	"omp_process_epoch",
	"state",
	"broker_terminal_state",
	"identity_acknowledged",
	"health_reason",
	"reject_reason_code",
	"application_acceptance_observed",
	"updated_at_ms",
]);
const REJECT_REASONS = new Set([
	"USER_REJECTED",
	"PROTOCOL_INVALID",
	"RECIPIENT_POLICY_REFUSED",
	"PERSISTENCE_UNRECOVERABLE",
	"DELIVERY_UNCERTAIN_CONTENT_MAY_HAVE_BEEN_SEEN",
]);
const IDENTITY_REFUSAL_CODES = new Set([
	"MODE_UNSUPPORTED",
	"SESSION_ALREADY_OWNED",
	"IDENTITY_INVALID",
	"CONTROL_CHANNEL_INVALID",
	"BROKER_CAPABILITY_MISSING",
	"DELIVERY_PENDING",
]);
// The relay roots come from the `omp-relay` entry omp launched the child from, never from omp's
// own environment (plan omp-config-root-discovery, Revision 2 with the root-change guard).
const STATE_ROOT_FLAG = "--state-root";
const RUNTIME_ROOT_FLAG = "--runtime-root";
const STATE_ROOT_ENV = "OMP_RELAY_STATE_ROOT";
const RUNTIME_ROOT_ENV = "OMP_RELAY_RUNTIME_ROOT";
const RELAY_MODES = new Set(["tui", "rpc"]);
const STATUS_WAITING = "relay: waiting for MCP configuration";
const STATUS_STARTING = "relay: MCP starting";
const STATUS_FAILED = "relay: MCP failed to start — see omp's MCP status";
const STATUS_PENDING = "relay: connected, identity pending";
// omp 18.8.6 gives no supported signal when a manual `/mcp reconnect` drops the connection, and
// nothing here polls, so readiness is only ever what the last re-evaluation observed.
const STATUS_READY = "relay: ready (last observed)";
// A held session admits no new claim, binding or delivery because of a session transition, and its
// line never reads ready. omp 18.8.6 tells extensions nothing when a transition is cancelled, so a
// pending transition and a cancelled one look the same here.
const HELD_STATUS: Record<HeldReason, string> = {
	TRANSITION_PENDING_BEFORE_BIND: "relay: held — session transition pending",
	TRANSITION_PENDING_QUIESCED: "relay: held — session transition pending",
	RESTORING_AFTER_UNQUIESCE: "relay: held — restoring after /relay-unquiesce",
	DELIVERY_UNRESOLVED_AT_COMPLETION: "relay: held — delivery unresolved across a session transition",
};
// A bound gate fenced by an error, not by the delivery barrier (fenceGate), is not ready either.
const STATUS_FENCED_PREFIX = "relay: fenced — ";
// What each status line means, independent of its wording; blocked lines carry their code instead.
const STATUS_KIND = new Map([
	[STATUS_WAITING, "waiting"],
	[STATUS_STARTING, "starting"],
	[STATUS_FAILED, "failed"],
	[STATUS_PENDING, "pending"],
	[STATUS_READY, "ready"],
	...Object.values(HELD_STATUS).map(text => [text, "held"] as const),
]);
// omp-side configuration codes: always the log, plus the status line and a notice where omp has a
// UI; never the child's protocol.
const DECLARATION_CODES = new Set([
	"RELAY_CONFIG_MISSING",
	"RELAY_CONFIG_INVALID",
	"RELAY_CONFIG_UNPROVABLE",
	"RELAY_CONFIG_CONFLICT",
	"STATE_ROOT_INVALID",
	"RUNTIME_ROOT_INVALID",
]);
// omp 18.8.6 resolveStdioSpawnCommand uses this list when PATHEXT is unset or empty.
const DEFAULT_WINDOWS_PATHEXT = [".com", ".exe", ".bat", ".cmd"];
const WINDOWS_BATCH_EXTENSIONS = new Set([".bat", ".cmd"]);
const USER_CONTROL_FAILURE_CODES = new Set([
	"CONTROL_CHANNEL_INVALID",
	"PROTOCOL_INVALID",
	"IDENTITY_INVALID",
	"BINDING_CONFLICT",
	"ATTEMPT_CONFLICT",
	"DELIVERY_PENDING",
	"TERMINAL_CONFLICT",
	"HEAD_CONFLICT",
	"BROKER_UNREACHABLE",
	"BROKER_CAPABILITY_MISSING",
]);

export type GateState = "open" | "checking" | "event_fenced" | "quiescing" | "rebind_blocked" | "restoring";
type ClaimLifecycle = "live" | "retired" | "released" | "superseded";
type Ownership = "none" | "self" | "unknown";
type PendingOperation = "idle" | "acquire" | "transfer" | "release";
type MirrorState = "received" | "injected" | "accepted" | "rejected" | "uncertain";
// Each ends only on: a completed switch, branch or tree navigation, or session_start (the first);
// that completion or /relay-unquiesce (the second); the restore claim landing (the third); every
// offer of the kept gate's session clearing (the last).
type HeldReason =
	| "TRANSITION_PENDING_BEFORE_BIND"
	| "TRANSITION_PENDING_QUIESCED"
	| "RESTORING_AFTER_UNQUIESCE"
	| "DELIVERY_UNRESOLVED_AT_COMPLETION";
type BrokerTerminalState = "none" | "accepted" | "rejected";

type RelayManager = {
	getServerConfig(name: string): unknown;
	getSource?(name: string): unknown;
	getConnectionStatus?(name: string): unknown;
	getConnection?(name: string): unknown;
	addConnectionStatusListener?(listener: (event: unknown) => void): () => void;
};

type Declaration =
	| { ok: true; state_root: string; runtime_root: string }
	| { ok: false; code: string; entry_absent: boolean };

type ProcessWitness = {
	pid: number;
	start_key: string;
	sequence_number: string;
	boot_id: number;
};

type OwnerTuple = {
	session_id: string;
	omp_pid: number;
	omp_process_start_ms: number;
	omp_process_epoch: string;
	holder_claim_generation: string;
	holder_gate_generation: string;
};

type DeadWitness = {
	session_id: string;
	omp_pid: number;
	omp_process_start_ms: number;
	omp_process_epoch: string;
};

type OwnerEnvelope = {
	schema_version: 1;
	holder: OwnerTuple | null;
	dead_predecessors: DeadWitness[];
};

type Claim = {
	claim_generation: string;
	holder_gate_generation: string;
	lifecycle: ClaimLifecycle;
	ownership: Ownership;
	expected_owner: OwnerTuple;
	observed_owner: OwnerTuple | null;
	pending: PendingOperation;
	operation_generation: string | null;
	transfer_source: Claim | null;
	operation_pins: number;
};

type Binding = {
	instance_id: string;
	control_epoch: string;
	bind_attempt_id: string;
	binding_epoch: string;
	omp_process_epoch: string;
	session_id: string;
	gate_generation: string;
	claim_generation: string;
};

type Gate = {
	session_id: string;
	omp_process_epoch: string;
	gate_generation: string;
	state: GateState;
	claim: Claim;
	lock_claim_generation: string | null;
	resume_gate: "open" | "rebind_blocked" | null;
	binding: Binding | null;
	context: any;
	// Kept by a completed transition while an offer of its session was unclear (finishTransition).
	retained: boolean;
	// The error that fenced this gate, or null for the delivery barrier (fenceGate, trackHeld).
	fence_code: string | null;
};

type Coordinator = {
	session_id: string;
	omp_process_epoch: string;
	queue_generation: string;
	fifo_tail: Promise<void>;
	claims: Map<string, Claim>;
};

type Delivery = {
	event_id: string;
	client_event_id: string | null;
	from_agent: string;
	to_agent: string;
	channel: string;
	thread_id: string | null;
	kind: "agent_message" | "agent_reply" | "handoff";
	content: string;
	ts: number;
};

type Mirror = {
	schema_version: 1;
	session_id: string;
	recipient_id: string;
	event_id: string;
	offer_token: string;
	omp_process_epoch: string;
	state: MirrorState;
	broker_terminal_state: BrokerTerminalState;
	identity_acknowledged: boolean;
	health_reason:
		| "NONE"
		| "BINDING_CHANGED"
		| "SESSION_TRANSITION"
		| "SEND_REJECTED"
		| "PERSISTENCE_STALLED"
		| "PROTOCOL_INVALID";
	reject_reason_code: string | null;
	application_acceptance_observed: boolean;
	updated_at_ms: number;
};

type Entry = {
	key: string;
	session_id: string;
	event_id: string;
	offer_token: string;
	omp_process_epoch: string;
	delivery: Delivery | null;
	display_text: string | null;
	chain: Promise<void>;
	unresolved: boolean;
	terminal: boolean;
	send_right: boolean;
	send_consumed: boolean;
	pending_adoption: boolean;
	adoption_ambiguous: boolean;
};

type Pair = {
	instance_id: string;
	control_epoch: string;
	witness: ProcessWitness;
	rendezvous_dir: string;
};

type PendingApproval = {
	instance_id: string;
	control_epoch: string;
	bind_attempt_id: string;
	binding_epoch: string;
	gate: Gate;
	claim: Claim;
	control_token_hash: string;
	witness: ProcessWitness;
	connection: unknown;
};

type GlobalState = {
	entries: Map<string, Entry>;
	gates: Map<string, Gate>;
	coordinators: Map<string, Coordinator>;
	stopping: boolean;
	keypair: { publicKey: KeyObject; privateKey: KeyObject };
	current_pair: Pair | null;
	high_water_sequence: bigint | null;
	high_water_boot: number | null;
	superseded_instances: Set<string>;
	pending_approvals: Map<string, PendingApproval>;
	pending_transition: Gate | null;
	state_root: string | null;
	runtime_root: string | null;
	owned_rendezvous: Set<string>;
	test_process_query: ((pid: number) => ProcessRecord) | null;
	test_mcp_manager: RelayManager | null | undefined;
	mcp_module: Promise<unknown> | null;
	mcp_module_value: unknown;
	relay_ctx: any;
	relay_api: any;
	relay_status: string | null;
	relay_status_kind: string | null;
	relay_failed: boolean;
	bound_connection: unknown;
	bound_binding: Binding | null;
	connecting_without_relay: boolean;
	transition_pending: boolean;
	held: Map<string, HeldReason>;
	fenced: Map<string, string>;
	status_generation: number;
	status_unsubscribe: (() => void) | null;
};

type ProcessRecord = {
	witness: ProcessWitness;
	parent_pid: number;
	start_ms: number;
	alive: boolean;
};

function randomUuid(): string {
	return crypto.randomUUID();
}

function getProcessEpoch(): string {
	const root = globalThis as Record<PropertyKey, unknown>;
	const existing = root[PROCESS_EPOCH_KEY];
	if (typeof existing === "string" && UUID_TEXT.test(existing)) return existing;
	const epoch = randomUuid();
	root[PROCESS_EPOCH_KEY] = epoch;
	return epoch;
}

function getGlobalState(): GlobalState {
	const root = globalThis as Record<PropertyKey, unknown>;
	const existing = root[GLOBAL_KEY] as GlobalState | undefined;
	if (existing) return existing;
	const keypair = generateKeyPairSync("ed25519");
	const state: GlobalState = {
		entries: new Map(),
		gates: new Map(),
		coordinators: new Map(),
		stopping: false,
		keypair,
		current_pair: null,
		high_water_sequence: null,
		high_water_boot: null,
		superseded_instances: new Set(),
		pending_approvals: new Map(),
		pending_transition: null,
		state_root: null,
		runtime_root: null,
		owned_rendezvous: new Set(),
		test_process_query: null,
		test_mcp_manager: undefined,
		mcp_module: null,
		mcp_module_value: null,
		relay_ctx: null,
		relay_api: null,
		relay_status: null,
		relay_status_kind: null,
		relay_failed: false,
		bound_connection: null,
		bound_binding: null,
		connecting_without_relay: false,
		transition_pending: false,
		held: new Map(),
		fenced: new Map(),
		status_generation: 0,
		status_unsubscribe: null,
	};
	root[GLOBAL_KEY] = state;
	return state;
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return value !== null && typeof value === "object" && !Array.isArray(value);
}

function closed(value: unknown, fields: ReadonlySet<string>): Record<string, unknown> {
	if (!isRecord(value)) throw new Error("PROTOCOL_INVALID");
	const keys = Object.keys(value);
	if (keys.length !== fields.size || keys.some(key => !fields.has(key))) throw new Error("PROTOCOL_INVALID");
	return value;
}

function uuid(value: unknown): string {
	if (typeof value !== "string" || !UUID_TEXT.test(value)) throw new Error("PROTOCOL_INVALID");
	return value;
}

function eventId(value: unknown): string {
	if (typeof value !== "string" || !EVENT_ID.test(value)) throw new Error("PROTOCOL_INVALID");
	if (BigInt(value) > 9_223_372_036_854_775_807n) throw new Error("PROTOCOL_INVALID");
	return value;
}

function safeInt(value: unknown, positive = false): number {
	if (
		typeof value !== "number" ||
		!Number.isSafeInteger(value) ||
		value < (positive ? 1 : 0) ||
		value > JS_SAFE_INT_MAX
	) {
		throw new Error("PROTOCOL_INVALID");
	}
	return value;
}

function strictBool(value: unknown): boolean {
	if (typeof value !== "boolean") throw new Error("PROTOCOL_INVALID");
	return value;
}

function validateWitness(value: unknown): ProcessWitness {
	const record = closed(value, new Set(["pid", "start_key", "sequence_number", "boot_id"]));
	const pid = safeInt(record.pid, true);
	const startKey = uint64String(record.start_key);
	const sequence = uint64String(record.sequence_number);
	const bootId = record.boot_id;
	if (typeof bootId !== "number" || !Number.isInteger(bootId) || bootId < 0 || bootId > 0xffff_ffff) {
		throw new Error("PROTOCOL_INVALID");
	}
	return { pid, start_key: startKey, sequence_number: sequence, boot_id: bootId };
}

function uint64String(value: unknown): string {
	if (typeof value !== "string" || !/^[1-9][0-9]{0,19}$/.test(value)) throw new Error("PROTOCOL_INVALID");
	const number = BigInt(value);
	if (number > 18_446_744_073_709_551_615n) throw new Error("PROTOCOL_INVALID");
	return value;
}

function compareEventIds(left: string, right: string): number {
	if (left.length !== right.length) return left.length - right.length;
	return left < right ? -1 : left > right ? 1 : 0;
}

function validateDelivery(value: unknown): Delivery {
	const d = closed(
		value,
		new Set([
			"event_id",
			"client_event_id",
			"from_agent",
			"to_agent",
			"channel",
			"thread_id",
			"kind",
			"content",
			"ts",
		]),
	);
	const result: Delivery = {
		event_id: eventId(d.event_id),
		client_event_id: d.client_event_id as string | null,
		from_agent: d.from_agent as string,
		to_agent: d.to_agent as string,
		channel: d.channel as string,
		thread_id: d.thread_id as string | null,
		kind: d.kind as Delivery["kind"],
		content: d.content as string,
		ts: safeInt(d.ts),
	};
	if (
		(result.client_event_id !== null && typeof result.client_event_id !== "string") ||
		typeof result.from_agent !== "string" ||
		typeof result.to_agent !== "string" ||
		typeof result.channel !== "string" ||
		(result.thread_id !== null && typeof result.thread_id !== "string") ||
		!new Set(["agent_message", "agent_reply", "handoff"]).has(result.kind) ||
		typeof result.content !== "string"
	) {
		throw new Error("PROTOCOL_INVALID");
	}
	return result;
}

export function renderDisplay(delivery: Delivery): string {
	const d = validateDelivery(delivery);
	return `[relay] from=${d.from_agent} to=${d.to_agent} event_id=${d.event_id} kind=${d.kind}\n\n${d.content}`;
}

function marker(event: string, token: string): string {
	return `[omp-relay-accept:v1 event_id=${event} token=${token} generation=1]`;
}

function canonicalJson(value: unknown): string {
	if (value === null) return "null";
	if (typeof value === "string" || typeof value === "boolean") return JSON.stringify(value);
	if (typeof value === "number") {
		if (!Number.isSafeInteger(value)) throw new Error("PROTOCOL_INVALID");
		return String(value);
	}
	if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
	if (!isRecord(value)) throw new Error("PROTOCOL_INVALID");
	const keys = Object.keys(value).sort();
	return `{${keys.map(key => `${JSON.stringify(key)}:${canonicalJson(value[key])}`).join(",")}}`;
}

function base64url(data: Uint8Array): string {
	return Buffer.from(data).toString("base64url");
}

function atomicWriteJson(destination: string, value: Record<string, unknown>): void {
	mkdirSync(path.dirname(destination), { recursive: true, mode: 0o700 });
	const temporary = path.join(path.dirname(destination), `.${path.basename(destination)}.${randomUuid()}.tmp`);
	let descriptor: number | null = null;
	try {
		descriptor = openSync(temporary, "wx", 0o600);
		writeFileSync(descriptor, `${canonicalJson(value)}\n`, "utf8");
		fsyncSync(descriptor);
		closeSync(descriptor);
		descriptor = null;
		renameSync(temporary, destination);
		flushDirectory(path.dirname(destination));
	} finally {
		if (descriptor !== null) closeSync(descriptor);
		try {
			unlinkSync(temporary);
		} catch {
			// Temp cleanup is best-effort; a temp file has no authority.
		}
	}
}

function exclusiveWriteJson(destination: string, value: Record<string, unknown>): boolean {
	mkdirSync(path.dirname(destination), { recursive: true, mode: 0o700 });
	let descriptor: number | null = null;
	try {
		descriptor = openSync(destination, fsConstants.O_CREAT | fsConstants.O_EXCL | fsConstants.O_WRONLY, 0o600);
		writeFileSync(descriptor, `${canonicalJson(value)}\n`, "utf8");
		fsyncSync(descriptor);
		closeSync(descriptor);
		descriptor = null;
		flushDirectory(path.dirname(destination));
		return true;
	} catch (error) {
		if (descriptor !== null) closeSync(descriptor);
		if ((error as NodeJS.ErrnoException).code === "EEXIST") return false;
		throw error;
	}
}

function flushDirectory(directory: string): void {
	let descriptor: number | null = null;
	try {
		descriptor = openSync(directory, fsConstants.O_RDONLY);
		fsyncSync(descriptor);
	} catch (error) {
		if (process.platform !== "win32") throw error;
		// Windows Node/Bun does not expose a supported directory FlushFileBuffers
		// primitive. File flush + atomic replacement is the documented limit.
	} finally {
		if (descriptor !== null) closeSync(descriptor);
	}
}

function assertNoDuplicateJsonMembers(text: string): void {
	let offset = 0;
	const whitespace = () => {
		while (offset < text.length && /\s/.test(text[offset])) offset += 1;
	};
	const stringValue = (): string => {
		const start = offset;
		if (text[offset++] !== '"') throw new Error("PROTOCOL_INVALID");
		while (offset < text.length) {
			const character = text[offset++];
			if (character === '"') return JSON.parse(text.slice(start, offset));
			if (character === "\\") {
				if (offset >= text.length) throw new Error("PROTOCOL_INVALID");
				offset += 1;
			}
		}
		throw new Error("PROTOCOL_INVALID");
	};
	const value = (): void => {
		whitespace();
		if (text[offset] === "{") {
			offset += 1;
			whitespace();
			const keys = new Set<string>();
			if (text[offset] === "}") {
				offset += 1;
				return;
			}
			while (offset < text.length) {
				whitespace();
				const key = stringValue();
				if (keys.has(key)) throw new Error("PROTOCOL_INVALID");
				keys.add(key);
				whitespace();
				if (text[offset++] !== ":") throw new Error("PROTOCOL_INVALID");
				value();
				whitespace();
				const separator = text[offset++];
				if (separator === "}") return;
				if (separator !== ",") throw new Error("PROTOCOL_INVALID");
			}
			throw new Error("PROTOCOL_INVALID");
		}
		if (text[offset] === "[") {
			offset += 1;
			whitespace();
			if (text[offset] === "]") {
				offset += 1;
				return;
			}
			while (offset < text.length) {
				value();
				whitespace();
				const separator = text[offset++];
				if (separator === "]") return;
				if (separator !== ",") throw new Error("PROTOCOL_INVALID");
			}
			throw new Error("PROTOCOL_INVALID");
		}
		if (text[offset] === '"') {
			stringValue();
			return;
		}
		const start = offset;
		while (offset < text.length && !/[\s,\]}]/.test(text[offset])) offset += 1;
		if (start === offset) throw new Error("PROTOCOL_INVALID");
	};
	value();
	whitespace();
	if (offset !== text.length) throw new Error("PROTOCOL_INVALID");
}

function parseJsonFile(file: string): unknown {
	const text = readFileSync(file, "utf8");
	assertNoDuplicateJsonMembers(text);
	return JSON.parse(text);
}

function rejectLexicalReparseAncestors(raw: string, code: string): void {
	const parsed = path.parse(raw);
	let current = parsed.root;
	for (const part of raw.slice(parsed.root.length).split(/[\\/]+/).filter(Boolean)) {
		if (part === ".") continue;
		if (part === "..") {
			current = path.dirname(current);
			continue;
		}
		current = path.join(current, part);
		const stat = lstatSync(current);
		if (stat.isSymbolicLink() || (stat.mode & 0o170000) === 0o120000) throw new Error(code);
		if (process.platform === "win32") {
			const encoded = Buffer.from(`${current}\0`, "utf16le");
			const attributes = processLibraries().kernel32.symbols.GetFileAttributesW(ptr(encoded));
			if (attributes === 0xffff_ffff || (attributes & 0x400) !== 0) throw new Error(code);
		}
	}
}


function canonicalRoot(raw: string | undefined, code: string): string {
	if (!raw || !path.isAbsolute(raw)) throw new Error(code);
	rejectLexicalReparseAncestors(raw, code);
	const resolved = realpathSync.native(raw);
	const normalized = path.normalize(raw);
	const comparable = process.platform === "win32" ? normalized.toLowerCase() : normalized;
	const resolvedComparable = process.platform === "win32" ? path.normalize(resolved).toLowerCase() : path.normalize(resolved);
	if (comparable !== resolvedComparable || resolved === path.parse(resolved).root) throw new Error(code);
	let current = path.parse(resolved).root;
	for (const part of resolved.slice(current.length).split(path.sep).filter(Boolean)) {
		current = path.join(current, part);
		const stat = lstatSync(current);
		if (!stat.isDirectory() || stat.isSymbolicLink() || (stat.mode & 0o170000) === 0o120000) throw new Error(code);
	}
	return resolved;
}

function directChildRendezvous(runtimeRoot: string, candidate: unknown, instanceId: string): string {
	if (typeof candidate !== "string" || !path.isAbsolute(candidate)) throw new Error("CONTROL_CHANNEL_INVALID");
	rejectLexicalReparseAncestors(candidate, "CONTROL_CHANNEL_INVALID");
	const resolved = realpathSync.native(candidate);
	if (path.dirname(resolved) !== runtimeRoot) throw new Error("CONTROL_CHANNEL_INVALID");
	if (path.basename(resolved) !== `mcp-${instanceId}`) throw new Error("CONTROL_CHANNEL_INVALID");
	const stat = lstatSync(resolved);
	if (!stat.isDirectory() || stat.isSymbolicLink()) throw new Error("CONTROL_CHANNEL_INVALID");
	return resolved;
}

// omp 18.8.6: `@oh-my-pi/pi-coding-agent/mcp` is a public package export and a bundled host-module
// key, and createAgentSession installs the top-level session's manager with MCPManager.setInstance
// before extensions load. The specifier stays a literal so omp's extension loader can rewrite it to
// the bundled module. Whether a normally loaded extension reaches omp's OWN instance this way is an
// host integration gate, not something a test here can prove.
function loadRelayManager(): Promise<RelayManager | null> {
	const global = getGlobalState();
	if (global.test_mcp_manager !== undefined) return Promise.resolve(global.test_mcp_manager);
	global.mcp_module ??= import("@oh-my-pi/pi-coding-agent/mcp").then(
		module => {
			getGlobalState().mcp_module_value = module;
			return module;
		},
		() => null,
	);
	return global.mcp_module.then(() => currentRelayManager());
}

function currentRelayManager(): RelayManager | null {
	const global = getGlobalState();
	if (global.test_mcp_manager !== undefined) return global.test_mcp_manager;
	try {
		const manager = (global.mcp_module_value as any)?.MCPManager?.instance?.();
		return manager && typeof manager.getServerConfig === "function" ? manager : null;
	} catch {
		return null;
	}
}

// The stored entry is omp's PRESERVED declaration, after discovery-time `${VAR}` expansion and
// before launch-time env resolution; it also survives a failed launch, so its presence never means
// a child is running. Only forms whose agreement with the child's real inputs is provable pass.
function readDeclaration(manager: RelayManager | null): Declaration {
	if (!manager) return { ok: false, code: "RELAY_CONFIG_UNAVAILABLE", entry_absent: false };
	let entry: unknown;
	try {
		entry = manager.getServerConfig(SERVER_NAME);
	} catch {
		return { ok: false, code: "RELAY_CONFIG_UNAVAILABLE", entry_absent: false };
	}
	if (entry === undefined || entry === null) return { ok: false, code: "RELAY_CONFIG_MISSING", entry_absent: true };
	try {
		return { ok: true, ...declaredRoots(entry) };
	} catch (error) {
		const code = fixedError(error);
		return { ok: false, code: DECLARATION_CODES.has(code) ? code : "RELAY_CONFIG_INVALID", entry_absent: false };
	}
}

function declaredRoots(entry: unknown): { state_root: string; runtime_root: string } {
	if (!isRecord(entry)) throw new Error("RELAY_CONFIG_INVALID");
	if (entry.type !== undefined && entry.type !== "stdio") throw new Error("RELAY_CONFIG_INVALID");
	const args = entry.args ?? [];
	const env = entry.env ?? {};
	if (
		typeof entry.command !== "string" ||
		!Array.isArray(args) ||
		args.some(arg => typeof arg !== "string") ||
		!isRecord(env)
	) {
		throw new Error("RELAY_CONFIG_INVALID");
	}
	const fromArgs = rootsFromArgs(args as string[]);
	if (fromArgs.size > 0) assertDirectExecutable(entry.command, env);
	return {
		state_root: declaredRoot(entry, fromArgs.get(STATE_ROOT_FLAG), env, STATE_ROOT_ENV, "STATE_ROOT_INVALID"),
		runtime_root: declaredRoot(entry, fromArgs.get(RUNTIME_ROOT_FLAG), env, RUNTIME_ROOT_ENV, "RUNTIME_ROOT_INVALID"),
	};
}

// The child parses its argv with argparse defaults, so `--flag=value`, a unique-prefix
// abbreviation, a repeated flag (the last one wins) and a `--` token all reach it in ways this
// reader does not model. Only the separate-token form, each flag exactly once, is accepted.
function rootsFromArgs(args: string[]): Map<string, string> {
	const found = new Map<string, string>();
	for (let index = 0; index < args.length; index += 1) {
		const token = args[index];
		if (token === "--") throw new Error("RELAY_CONFIG_INVALID");
		const flag = token.split("=", 1)[0];
		for (const name of [STATE_ROOT_FLAG, RUNTIME_ROOT_FLAG]) {
			if (token === name) {
				const value = args[index + 1];
				if (found.has(name) || value === undefined || value.startsWith("-")) throw new Error("RELAY_CONFIG_INVALID");
				found.set(name, value);
				index += 1;
				break;
			}
			if (flag.length > 2 && name.startsWith(flag)) throw new Error("RELAY_CONFIG_INVALID");
		}
	}
	return found;
}

// omp keeps `args` verbatim after discovery. On Windows it spawns [command, ...args] directly only
// when the command resolves to an existing file whose extension is in PATHEXT and is not .bat/.cmd;
// anything else goes through cmd.exe, which re-escapes every argument. Elsewhere it always spawns
// [command, ...args] verbatim (omp 18.8.6 resolveStdioSpawnCommand). So command-line roots are
// provable only for an absolute, existing, directly spawned executable.
function assertDirectExecutable(command: string, env: Record<string, unknown>): void {
	if (!path.isAbsolute(command)) throw new Error("RELAY_CONFIG_UNPROVABLE");
	try {
		if (!statSync(command).isFile()) throw new Error("RELAY_CONFIG_UNPROVABLE");
	} catch {
		throw new Error("RELAY_CONFIG_UNPROVABLE");
	}
	if (process.platform !== "win32") return;
	const extension = path.extname(command).toLowerCase();
	if (!extension || WINDOWS_BATCH_EXTENSIONS.has(extension) || !windowsPathExt(env).includes(extension)) {
		throw new Error("RELAY_CONFIG_UNPROVABLE");
	}
}

// omp spawns with {...Bun.env, ...entry.env} and reads PATHEXT from that, case-insensitively.
function windowsPathExt(env: Record<string, unknown>): string[] {
	const merged: Record<string, unknown> = { ...process.env, ...env };
	let raw = merged.PATHEXT;
	if (raw === undefined) {
		for (const [key, value] of Object.entries(merged)) {
			if (key.toLowerCase() === "pathext") {
				raw = value;
				break;
			}
		}
	}
	if (typeof raw !== "string" || !raw) return DEFAULT_WINDOWS_PATHEXT;
	const extensions = raw
		.split(";")
		.map(part => part.trim())
		.filter(Boolean)
		.map(part => (part.startsWith(".") ? part : `.${part}`).toLowerCase());
	return extensions.length > 0 ? extensions : DEFAULT_WINDOWS_PATHEXT;
}

// The child prefers its command-line value; the env value is only argparse's default. When both
// are stated they must name the same canonical directory.
function declaredRoot(
	entry: Record<string, unknown>,
	argValue: string | undefined,
	env: Record<string, unknown>,
	key: string,
	code: string,
): string {
	const envValue = Object.prototype.hasOwnProperty.call(env, key) ? literalEnvRoot(entry, env[key], key) : undefined;
	if (argValue === undefined && envValue === undefined) throw new Error("RELAY_CONFIG_MISSING");
	if (argValue !== undefined && argValue.includes("${")) throw new Error("RELAY_CONFIG_INVALID");
	const fromArgs = argValue === undefined ? undefined : validRoot(argValue, code);
	const fromEnv = envValue === undefined ? undefined : validRoot(envValue, code);
	if (fromArgs !== undefined && fromEnv !== undefined && fromArgs !== fromEnv) throw new Error("RELAY_CONFIG_CONFLICT");
	return (fromArgs ?? fromEnv) as string;
}

// At launch omp passes each env value through resolveConfigValue unless the entry's envPolicy is
// "literal" or the key is in envLiteralKeys: a `!` prefix runs a command, a value exactly naming a
// variable in omp's environment is replaced by that variable, and an empty result is dropped so
// the child inherits omp's own (omp 18.8.6 MCPManager auth-config resolution). Any other value
// reaches the child unchanged.
function literalEnvRoot(entry: Record<string, unknown>, value: unknown, key: string): string {
	if (typeof value !== "string" || value.includes("${")) throw new Error("RELAY_CONFIG_INVALID");
	const literal =
		entry.envPolicy === "literal" || (Array.isArray(entry.envLiteralKeys) && entry.envLiteralKeys.includes(key));
	if (literal) return value;
	if (value === "" || value.startsWith("!") || exactEnvironmentName(value)) throw new Error("RELAY_CONFIG_UNPROVABLE");
	return value;
}

// Mirrors omp's $envExact: Windows env lookups are case-insensitive, so only an enumerated key
// with identical casing counts as a variable name.
function exactEnvironmentName(value: string): boolean {
	for (const key in process.env) {
		if (key === value) return true;
	}
	return false;
}

function validRoot(raw: string, code: string): string {
	try {
		return canonicalRoot(raw, code);
	} catch {
		throw new Error(code);
	}
}

// The first accepted pair is pinned for the omp process lifetime; any later declaration must
// repeat it. A changed root is refused, never followed: no stale reuse, migration or switch.
function pinnedRootsMatch(declaration: { state_root: string; runtime_root: string }): boolean {
	const global = getGlobalState();
	if (global.state_root === null && global.runtime_root === null) return true;
	return global.state_root === declaration.state_root && global.runtime_root === declaration.runtime_root;
}

function blockedStatus(code: string): string {
	return `relay: blocked — ${code}`;
}

// Every change goes to the log. Where omp has a UI it also sets one status line and raises one error
// notice on entering a blocked state; headless rpc (`--no-ui`) makes both UI calls no-ops, so there
// the log is the only record. Diagnostics carry only the code, the server name, the active agent directory and the
// declaration's source file: no env values, command output, other servers or launch errors.
function showRelayStatus(ctx: any, text: string, code: string | null, manager: RelayManager | null): void {
	const global = getGlobalState();
	if (global.relay_status === text) return;
	global.relay_status = text;
	global.relay_status_kind = code
		? "blocked"
		: (STATUS_KIND.get(text) ?? (text.startsWith(STATUS_FENCED_PREFIX) ? "fenced" : null));
	const detail = { code, server: SERVER_NAME, agent_dir: relayAgentDir(), source: relaySource(manager) };
	try {
		ctx.ui?.setStatus?.(SERVER_NAME, text);
	} catch {
		// The log below still records the change; headless rpc has no status line at all.
	}
	try {
		const logger = global.relay_api?.logger;
		if (code) logger?.warn?.(`omp-relay ${text}`, detail);
		else logger?.info?.(`omp-relay ${text}`, detail);
	} catch {
		// Logging must never decide whether the relay binds.
	}
	if (!code) return;
	try {
		ctx.ui?.notify?.(text, "error");
	} catch {
		// As above.
	}
}

// The omp-relay connection object omp holds now, or null. omp 18.8.6 stores a connection in exactly
// two places (MCPManager.connectServers and #connectAndWireServer) and sends tools/list only after
// storing it; the child starts its identity exchange on its first tools/list. omp's mcp_notification
// names only the server, never the connection, so an identity request is credited to the connection
// omp holds when the request is handled, and that object must still be the current one after the
// claim wait and when identity_bound commits (handleIdentityRequest, handleIdentityBound).
function relayConnection(manager: RelayManager | null): unknown {
	try {
		return manager?.getConnection?.(SERVER_NAME) ?? null;
	} catch {
		return null;
	}
}

function relayAgentDir(): string | null {
	try {
		const directory = getGlobalState().relay_api?.pi?.getAgentDir?.();
		return typeof directory === "string" ? directory : null;
	} catch {
		return null;
	}
}

function relaySource(manager: RelayManager | null): string | null {
	try {
		const source = manager?.getSource?.(SERVER_NAME);
		return isRecord(source) && typeof source.path === "string" ? source.path : null;
	} catch {
		return null;
	}
}

// The root-change guard in its synchronous form, shared by every path outside evaluateRelay that
// authorizes a new identity, commits a binding, or starts a new gate or claim generation: the
// CURRENT declaration must still name the pinned pair. On refusal it shows the omp-side code and
// returns false, and the caller does no identity, binding or claim work. It never awaits, so no
// caller has to move its own work behind an await to apply it.
function currentDeclarationPinned(ctx: any): boolean {
	const manager = currentRelayManager();
	const declaration = readDeclaration(manager);
	if (declaration.ok && pinnedRootsMatch(declaration)) return true;
	const code = declaration.ok ? "RELAY_ROOT_CHANGED" : declaration.code;
	showRelayStatus(ctx, blockedStatus(code), code, manager);
	return false;
}

// MCP connection readiness is omp's; relay readiness is the bound identity.
function connectedStatus(ctx: any, manager: RelayManager): string {
	const global = getGlobalState();
	const gate = global.gates.get(ctx.sessionManager.getSessionId());
	const held = gate ? gateHeldReason(gate) : null;
	if (held) return HELD_STATUS[held];
	if (gate?.state === "event_fenced" && gate.fence_code) return STATUS_FENCED_PREFIX + gate.fence_code;
	if (global.relay_failed) return STATUS_FAILED;
	let status: unknown;
	try {
		status = manager.getConnectionStatus?.(SERVER_NAME);
	} catch {
		status = undefined;
	}
	// Ready needs the binding to have been made over the connection omp holds NOW. omp replaces the
	// connection object for every new child, by paths that do and do not emit events (a manual
	// `/mcp reconnect` emits only `connected`, at its end), so readiness compares the objects, not
	// events. "Now" is this re-evaluation: between events the line shows what was last observed.
	// The binding must also be the one the child made last: a gate kept while the child bound another
	// session is not ready again until the child binds it again.
	const bound = Boolean(gate?.binding) && gate?.binding === global.bound_binding;
	const current = relayConnection(manager);
	if (bound && current !== null && current === global.bound_connection) return STATUS_READY;
	if (status === "connected") return STATUS_PENDING;
	// omp stores the entry and registers its pending connection in one synchronous step, so a stored
	// entry that is neither connecting nor connected is a launch that has ended (omp 18.8.6
	// MCPManager.connectServers). omp's own MCP display carries the error.
	if (status === "disconnected") return STATUS_FAILED;
	return STATUS_STARTING;
}

// The one idempotent step every re-evaluation event runs: session_start, each connection-status
// event, each omp-relay mcp_notification, before_agent_start and turn_end. There is no polling.
// While a transition started before the first bind is pending it returns before claiming, binding
// or re-evaluating the status line, which shows the hold (trackHeld); only that transition's own
// completion event ends the hold (the before-hooks below).
async function evaluateRelay(api: any, ctx: any): Promise<void> {
	const global = getGlobalState();
	if (ctx?.agent?.kind !== "main" || !RELAY_MODES.has(ctx.mode)) return;
	global.relay_ctx = ctx;
	if (api) global.relay_api = api;
	if (global.stopping || global.transition_pending) return;
	const manager = await loadRelayManager();
	if (global.stopping || global.transition_pending) return;
	const declaration = readDeclaration(manager);
	if (!declaration.ok) {
		// Interactive discovery is deferred and omp 18.8.6 has no public "discovery finished" signal,
		// so an absent entry is only missing once a `connecting` event has started other servers
		// without omp-relay. ctx.mode "rpc" also covers `--mode rpc-ui`, whose discovery is deferred
		// too; there an early RELAY_CONFIG_MISSING clears on the next connecting event.
		if (declaration.entry_absent && ctx.mode === "tui" && !global.connecting_without_relay) {
			showRelayStatus(ctx, STATUS_WAITING, null, manager);
		} else {
			showRelayStatus(ctx, blockedStatus(declaration.code), declaration.code, manager);
		}
		return;
	}
	if (!pinnedRootsMatch(declaration)) {
		showRelayStatus(ctx, blockedStatus("RELAY_ROOT_CHANGED"), "RELAY_ROOT_CHANGED", manager);
		return;
	}
	global.state_root = declaration.state_root;
	global.runtime_root = declaration.runtime_root;
	startSession(ctx);
	showRelayStatus(ctx, connectedStatus(ctx, manager as RelayManager), null, manager);
}

// omp 18.8.6 MCPManager.addConnectionStatusListener emits connecting / reconnecting / connected /
// failed and never replays past events, so a listener attached after rpc discovery sees none of
// them; connectedStatus therefore also reads getConnectionStatus.
function onConnectionStatus(event: unknown): void {
	if (!isRecord(event)) return;
	const global = getGlobalState();
	if (event.type === "connecting") {
		const names = Array.isArray(event.serverNames) ? event.serverNames : [];
		if (names.includes(SERVER_NAME)) global.relay_failed = false;
		else global.connecting_without_relay = true;
	} else if (event.serverName !== SERVER_NAME) {
		return;
	} else if (event.type === "failed") {
		global.relay_failed = true;
	} else if (event.type === "connected" || event.type === "reconnecting") {
		global.relay_failed = false;
	} else {
		return;
	}
	const ctx = global.relay_ctx;
	if (!ctx) return;
	trackHeld(ctx, "connection_status");
	void evaluateRelay(global.relay_api, ctx).catch(() => undefined);
}

async function subscribeConnectionStatus(): Promise<void> {
	const generation = ++getGlobalState().status_generation;
	const manager = await loadRelayManager();
	const global = getGlobalState();
	if (generation !== global.status_generation) return;
	global.status_unsubscribe?.();
	global.status_unsubscribe = null;
	if (typeof manager?.addConnectionStatusListener !== "function") return;
	try {
		global.status_unsubscribe = manager.addConnectionStatusListener(onConnectionStatus);
	} catch {
		// Without events the state is still re-read on every other re-evaluation event.
	}
}

let nativeLibraries: { kernel32: any; ntdll: any } | null = null;

function processLibraries(): { kernel32: any; ntdll: any } {
	if (nativeLibraries) return nativeLibraries;
	if (process.platform !== "win32" || process.arch !== "x64" || new Uint8Array(new Uint16Array([1]).buffer)[0] !== 1) {
		throw new Error("NATIVE_ABI_UNSUPPORTED");
	}
	const kernel32 = dlopen("kernel32.dll", {
		OpenProcess: { args: [FFIType.u32, FFIType.bool, FFIType.u32], returns: FFIType.ptr },
		WaitForSingleObject: { args: [FFIType.ptr, FFIType.u32], returns: FFIType.u32 },
		CloseHandle: { args: [FFIType.ptr], returns: FFIType.bool },
		GetProcessTimes: {
			args: [FFIType.ptr, FFIType.ptr, FFIType.ptr, FFIType.ptr, FFIType.ptr],
			returns: FFIType.bool,
		},
		GetFileAttributesW: { args: [FFIType.ptr], returns: FFIType.u32 },
		GetLastError: { args: [], returns: FFIType.u32 },
	});
	const ntdll = dlopen("ntdll.dll", {
		NtQueryInformationProcess: {
			args: [FFIType.ptr, FFIType.i32, FFIType.ptr, FFIType.u32, FFIType.ptr],
			returns: FFIType.i32,
		},
	});
	nativeLibraries = { kernel32, ntdll };
	return nativeLibraries;
}

function queryProcessNative(pid: number): ProcessRecord {
	if (!Number.isInteger(pid) || pid <= 0 || pid > 0xffff_ffff) throw new Error("NATIVE_WITNESS_INVALID");
	const { kernel32, ntdll } = processLibraries();
	const handle = kernel32.symbols.OpenProcess(0x0010_1000, false, pid);
	if (handle === null || handle === 0) {
		const error = kernel32.symbols.GetLastError();
		if (error === 87) return { witness: null as never, parent_pid: 0, start_ms: 0, alive: false };
		throw new Error("NATIVE_WITNESS_UNKNOWN");
	}
	try {
		const basic = new Uint8Array(48);
		const returned = new Uint32Array(1);
		const basicStatus = ntdll.symbols.NtQueryInformationProcess(handle, 0, ptr(basic), basic.byteLength, ptr(returned));
		if (basicStatus < 0 || returned[0] !== 48) throw new Error("NATIVE_ABI_UNSUPPORTED");
		const basicView = new DataView(basic.buffer, basic.byteOffset, basic.byteLength);
		const observedPid = basicView.getBigUint64(32, true);
		const parentPid = basicView.getBigUint64(40, true);
		if (observedPid !== BigInt(pid) || parentPid > 0xffff_ffffn) throw new Error("NATIVE_WITNESS_INVALID");

		const telemetry = new Uint8Array(65_536);
		returned[0] = 0;
		const telemetryStatus = ntdll.symbols.NtQueryInformationProcess(
			handle,
			64,
			ptr(telemetry),
			telemetry.byteLength,
			ptr(returned),
		);
		if (telemetryStatus < 0 || returned[0] < 64 || returned[0] > telemetry.byteLength) {
			throw new Error("NATIVE_ABI_UNSUPPORTED");
		}
		const view = new DataView(telemetry.buffer, telemetry.byteOffset, telemetry.byteLength);
		const headerSize = view.getUint32(0, true);
		const telemetryPid = view.getUint32(4, true);
		if (telemetryPid !== pid || headerSize < 64 || headerSize > returned[0] || headerSize % 8 !== 0) {
			throw new Error("NATIVE_WITNESS_INVALID");
		}
		const startKey = view.getBigUint64(8, true);
		const sequence = view.getBigUint64(40, true);
		const bootId = view.getUint32(60, true);
		if (startKey === 0n || sequence === 0n || kernel32.symbols.WaitForSingleObject(handle, 0) !== 0x102) {
			return { witness: null as never, parent_pid: Number(parentPid), start_ms: 0, alive: false };
		}
		const creation = new Uint8Array(8);
		const exit = new Uint8Array(8);
		const kernel = new Uint8Array(8);
		const user = new Uint8Array(8);
		if (!kernel32.symbols.GetProcessTimes(handle, ptr(creation), ptr(exit), ptr(kernel), ptr(user))) {
			throw new Error("NATIVE_WITNESS_INVALID");
		}
		const windowsTicks = new DataView(creation.buffer).getBigUint64(0, true);
		const unixMs = windowsTicks / 10_000n - 11_644_473_600_000n;
		if (unixMs < 0n || unixMs > BigInt(Number.MAX_SAFE_INTEGER)) throw new Error("NATIVE_WITNESS_INVALID");
		return {
			witness: {
				pid,
				start_key: startKey.toString(),
				sequence_number: sequence.toString(),
				boot_id: bootId,
			},
			parent_pid: Number(parentPid),
			start_ms: Number(unixMs),
			alive: true,
		};
	} finally {
		kernel32.symbols.CloseHandle(handle);
	}
}

function queryProcess(pid: number): ProcessRecord {
	const test = getGlobalState().test_process_query;
	return test ? test(pid) : queryProcessNative(pid);
}

function verifyDirectChild(reported: ProcessWitness): ProcessRecord {
	const child = queryProcess(reported.pid);
	const parent = queryProcess(process.pid);
	if (
		!child.alive ||
		!parent.alive ||
		child.parent_pid !== process.pid ||
		child.witness.pid !== reported.pid ||
		child.witness.start_key !== reported.start_key ||
		child.witness.sequence_number !== reported.sequence_number ||
		child.witness.boot_id !== reported.boot_id ||
		child.witness.boot_id !== parent.witness.boot_id
	) {
		throw new Error("CONTROL_CHANNEL_INVALID");
	}
	return child;
}

function sameOwner(left: OwnerTuple | null, right: OwnerTuple | null): boolean {
	if (left === null || right === null) return left === right;
	return (
		left.session_id === right.session_id &&
		left.omp_pid === right.omp_pid &&
		left.omp_process_start_ms === right.omp_process_start_ms &&
		left.omp_process_epoch === right.omp_process_epoch &&
		left.holder_claim_generation === right.holder_claim_generation &&
		left.holder_gate_generation === right.holder_gate_generation
	);
}

function deadWitnessFor(owner: OwnerTuple): DeadWitness {
	return {
		session_id: owner.session_id,
		omp_pid: owner.omp_pid,
		omp_process_start_ms: owner.omp_process_start_ms,
		omp_process_epoch: owner.omp_process_epoch,
	};
}

function ownerDirectory(stateRoot: string, sessionId: string): string {
	return path.join(stateRoot, "sessions", sessionId, "lock");
}

function ownerPath(stateRoot: string, sessionId: string): string {
	return path.join(ownerDirectory(stateRoot, sessionId), "owner.json");
}

function guardResource(stateRoot: string, sessionId: string): string {
	return path.join(stateRoot, "guards", sessionId);
}

function readOwnerEnvelope(stateRoot: string, sessionId: string): OwnerEnvelope {
	const file = ownerPath(stateRoot, sessionId);
	if (!existsSync(file)) return { schema_version: 1, holder: null, dead_predecessors: [] };
	const value = closed(parseJsonFile(file), new Set(["schema_version", "holder", "dead_predecessors"]));
	if (value.schema_version !== 1 || !Array.isArray(value.dead_predecessors)) throw new Error("OWNERSHIP_UNKNOWN");
	const holder = value.holder === null ? null : validateOwner(value.holder);
	const witnesses = value.dead_predecessors.map(validateDeadWitness);
	return { schema_version: 1, holder, dead_predecessors: witnesses };
}

function validateOwner(value: unknown): OwnerTuple {
	const owner = closed(
		value,
		new Set([
			"session_id",
			"omp_pid",
			"omp_process_start_ms",
			"omp_process_epoch",
			"holder_claim_generation",
			"holder_gate_generation",
		]),
	);
	return {
		session_id: uuid(owner.session_id),
		omp_pid: safeInt(owner.omp_pid, true),
		omp_process_start_ms: safeInt(owner.omp_process_start_ms),
		omp_process_epoch: uuid(owner.omp_process_epoch),
		holder_claim_generation: uuid(owner.holder_claim_generation),
		holder_gate_generation: uuid(owner.holder_gate_generation),
	};
}

function validateDeadWitness(value: unknown): DeadWitness {
	const witness = closed(
		value,
		new Set(["session_id", "omp_pid", "omp_process_start_ms", "omp_process_epoch"]),
	);
	return {
		session_id: uuid(witness.session_id),
		omp_pid: safeInt(witness.omp_pid, true),
		omp_process_start_ms: safeInt(witness.omp_process_start_ms),
		omp_process_epoch: uuid(witness.omp_process_epoch),
	};
}

function writeOwnerEnvelope(stateRoot: string, sessionId: string, envelope: OwnerEnvelope): void {
	mkdirSync(ownerDirectory(stateRoot, sessionId), { recursive: true, mode: 0o700 });
	atomicWriteJson(ownerPath(stateRoot, sessionId), envelope as unknown as Record<string, unknown>);
}

function ownerLiveness(owner: OwnerTuple): "live" | "dead" | "unknown" {
	let current: ProcessRecord;
	try {
		current = queryProcess(owner.omp_pid);
	} catch {
		return "unknown";
	}
	if (!current.alive) return "dead";
	if (current.start_ms !== owner.omp_process_start_ms) return "dead";
	return "live";
}

function enqueue(coordinator: Coordinator, operation: () => void | Promise<void>): Promise<void> {
	const next = coordinator.fifo_tail.then(operation, operation).then(
		() => undefined,
		() => undefined,
	);
	coordinator.fifo_tail = next;
	return next;
}

function coordinatorFor(sessionId: string): Coordinator {
	const state = getGlobalState();
	let coordinator = state.coordinators.get(sessionId);
	if (!coordinator) {
		coordinator = {
			session_id: sessionId,
			omp_process_epoch: getProcessEpoch(),
			queue_generation: randomUuid(),
			fifo_tail: Promise.resolve(),
			claims: new Map(),
		};
		state.coordinators.set(sessionId, coordinator);
	}
	return coordinator;
}

function freshClaim(sessionId: string, gateGeneration: string, transferSource: Claim | null = null): Claim {
	const processRecord = queryProcess(process.pid);
	if (!processRecord.alive) throw new Error("IDENTITY_INVALID");
	const claimGeneration = randomUuid();
	const claim: Claim = {
		claim_generation: claimGeneration,
		holder_gate_generation: gateGeneration,
		lifecycle: "live",
		ownership: "none",
		expected_owner: {
			session_id: sessionId,
			omp_pid: process.pid,
			omp_process_start_ms: processRecord.start_ms,
			omp_process_epoch: getProcessEpoch(),
			holder_claim_generation: claimGeneration,
			holder_gate_generation: gateGeneration,
		},
		observed_owner: null,
		pending: "idle",
		operation_generation: null,
		transfer_source: transferSource,
		operation_pins: 0,
	};
	coordinatorFor(sessionId).claims.set(claimGeneration, claim);
	return claim;
}

function freshGate(sessionId: string, context: any, state: GateState, source: Claim | null = null): Gate {
	const gateGeneration = randomUuid();
	const claim = freshClaim(sessionId, gateGeneration, source);
	return {
		session_id: sessionId,
		omp_process_epoch: getProcessEpoch(),
		gate_generation: gateGeneration,
		state,
		claim,
		lock_claim_generation: claim.claim_generation,
		resume_gate: null,
		binding: null,
		context,
		retained: false,
		fence_code: null,
	};
}

function isClaimDemanded(target: Claim): boolean {
	for (const gate of getGlobalState().gates.values()) {
		let claim: Claim | null = gate.claim;
		const seen = new Set<string>();
		while (claim && !seen.has(claim.claim_generation)) {
			if (claim === target) return true;
			seen.add(claim.claim_generation);
			claim = claim.transfer_source;
		}
	}
	return false;
}

function inspectOrAcquire(gate: Gate): void {
	const state = getGlobalState();
	if (!state.state_root || state.stopping) return;
	const claim = gate.claim;
	if (claim.pending !== "idle" || claim.lifecycle !== "live") return;
	claim.pending = claim.transfer_source ? "transfer" : "acquire";
	claim.operation_generation = randomUuid();
	claim.operation_pins += 1;
	const operationGeneration = claim.operation_generation;
	const coordinator = coordinatorFor(gate.session_id);
	void enqueue(coordinator, () => {
		try {
			if (claim.operation_generation !== operationGeneration) return;
			if (claim.lifecycle === "retired" && !claim.transfer_source) return;
			mkdirSync(path.dirname(guardResource(state.state_root!, gate.session_id)), { recursive: true, mode: 0o700 });
			try {
				withFileLockSync(
					guardResource(state.state_root!, gate.session_id),
					() => inspectOrAcquireGuarded(gate, claim),
					{ retries: 1 },
				);
			} catch {
				// Definite no-mutation contention/failure: retain authority knowledge.
			}
		} finally {
			claim.pending = "idle";
			claim.operation_generation = null;
			claim.operation_pins -= 1;
			publishClaimResult(gate, claim);
			if (claim.lifecycle === "retired") releaseRetiredClaim(gate.session_id, claim);
		}
	});
}

function inspectOrAcquireGuarded(gate: Gate, claim: Claim): void {
	const stateRoot = getGlobalState().state_root!;
	const envelope = readOwnerEnvelope(stateRoot, gate.session_id);
	if (sameOwner(envelope.holder, claim.expected_owner)) {
		claim.ownership = "self";
		claim.observed_owner = envelope.holder;
		return;
	}
	const lineage: Claim[] = [];
	let source = claim.transfer_source;
	while (source) {
		lineage.push(source);
		source = source.transfer_source;
	}
	const exactSource = lineage.find(item => sameOwner(envelope.holder, item.expected_owner));
	if (exactSource) {
		writeOwnerEnvelope(stateRoot, gate.session_id, {
			schema_version: 1,
			holder: claim.expected_owner,
			dead_predecessors: envelope.dead_predecessors,
		});
		exactSource.lifecycle = "superseded";
		exactSource.ownership = "none";
		claim.transfer_source = null;
		claim.ownership = "self";
		claim.observed_owner = claim.expected_owner;
		return;
	}
	if (envelope.holder === null) {
		writeOwnerEnvelope(stateRoot, gate.session_id, {
			schema_version: 1,
			holder: claim.expected_owner,
			dead_predecessors: envelope.dead_predecessors,
		});
		claim.transfer_source = null;
		claim.ownership = "self";
		claim.observed_owner = claim.expected_owner;
		return;
	}
	const liveness = ownerLiveness(envelope.holder);
	if (liveness === "live") {
		claim.ownership = "none";
		claim.observed_owner = envelope.holder;
		return;
	}
	if (liveness === "unknown") {
		claim.ownership = "unknown";
		claim.observed_owner = envelope.holder;
		return;
	}
	const witness = deadWitnessFor(envelope.holder);
	const inherited = envelope.dead_predecessors.some(
		item => item.omp_process_epoch === witness.omp_process_epoch && item.omp_pid === witness.omp_pid,
	)
		? envelope.dead_predecessors
		: [...envelope.dead_predecessors, witness];
	writeOwnerEnvelope(stateRoot, gate.session_id, {
		schema_version: 1,
		holder: claim.expected_owner,
		dead_predecessors: inherited,
	});
	claim.transfer_source = null;
	claim.ownership = "self";
	claim.observed_owner = claim.expected_owner;
}

function publishClaimResult(gate: Gate, claim: Claim): void {
	const current = getGlobalState().gates.get(gate.session_id);
	if (current !== gate || gate.claim !== claim || getGlobalState().stopping) return;
	if (claim.ownership === "self") {
		if (gate.state === "restoring") {
			if (gate.resume_gate === "open") {
				gate.state = gate.binding && transitionClear(gate.session_id) ? "open" : "event_fenced";
			} else {
				gate.state = "rebind_blocked";
			}
			gate.resume_gate = null;
		}
		publishHealth(gate, gate.binding ? (gate.state === "open" ? "connected" : "blocked") : "rebinding", gate.binding ? (gate.state === "open" ? "CONNECTED" : "DELIVERY_PENDING") : "REBINDING", null);
	} else if (claim.ownership === "none") {
		publishHealth(gate, "blocked", "SESSION_ALREADY_OWNED", 30_000);
	} else {
		publishHealth(gate, "blocked", "OWNERSHIP_UNKNOWN", 30_000);
	}
}

function releaseRetiredClaim(sessionId: string, claim: Claim): void {
	const state = getGlobalState();
	if (!state.state_root || claim.pending !== "idle" || claim.lifecycle !== "retired") return;
	claim.pending = "release";
	claim.operation_generation = randomUuid();
	claim.operation_pins += 1;
	const operationGeneration = claim.operation_generation;
	void enqueue(coordinatorFor(sessionId), () => {
		try {
			if (claim.operation_generation !== operationGeneration || isClaimDemanded(claim)) return;
			withFileLockSync(
				guardResource(state.state_root!, sessionId),
				() => {
					const envelope = readOwnerEnvelope(state.state_root!, sessionId);
					if (sameOwner(envelope.holder, claim.expected_owner)) {
						writeOwnerEnvelope(state.state_root!, sessionId, {
							schema_version: 1,
							holder: null,
							dead_predecessors: envelope.dead_predecessors,
						});
					}
				},
				{ retries: 1 },
			);
			claim.ownership = "none";
			claim.lifecycle = "released";
			claim.observed_owner = null;
		} catch {
			claim.ownership = "unknown";
		} finally {
			claim.pending = "idle";
			claim.operation_generation = null;
			claim.operation_pins -= 1;
		}
	});
}

function offerPath(sessionId: string, event: string): string {
	const stateRoot = getGlobalState().state_root;
	if (!stateRoot) throw new Error("IDENTITY_INVALID");
	return path.join(stateRoot, "sessions", sessionId, "offers", `${eventId(event)}.json`);
}

function validateMirror(value: unknown): Mirror {
	const mirror = closed(value, MIRROR_FIELDS);
	if (mirror.schema_version !== 1) throw new Error("PROTOCOL_INVALID");
	const sessionId = uuid(mirror.session_id);
	if (uuid(mirror.recipient_id) !== sessionId) throw new Error("PROTOCOL_INVALID");
	const state = mirror.state;
	const terminal = mirror.broker_terminal_state;
	if (!new Set(["received", "injected", "accepted", "rejected", "uncertain"]).has(state as string)) {
		throw new Error("PROTOCOL_INVALID");
	}
	if (!new Set(["none", "accepted", "rejected"]).has(terminal as string)) throw new Error("PROTOCOL_INVALID");
	const result = {
		schema_version: 1,
		session_id: sessionId,
		recipient_id: sessionId,
		event_id: eventId(mirror.event_id),
		offer_token: uuid(mirror.offer_token),
		omp_process_epoch: uuid(mirror.omp_process_epoch),
		state: state as MirrorState,
		broker_terminal_state: terminal as BrokerTerminalState,
		identity_acknowledged: strictBool(mirror.identity_acknowledged),
		health_reason: mirror.health_reason as Mirror["health_reason"],
		reject_reason_code: mirror.reject_reason_code as string | null,
		application_acceptance_observed: strictBool(mirror.application_acceptance_observed),
		updated_at_ms: safeInt(mirror.updated_at_ms),
	} satisfies Mirror;
	if (!new Set(["NONE", "BINDING_CHANGED", "SESSION_TRANSITION", "SEND_REJECTED", "PERSISTENCE_STALLED", "PROTOCOL_INVALID"]).has(result.health_reason)) {
		throw new Error("PROTOCOL_INVALID");
	}
	if (
		(result.state === "rejected") !== (result.reject_reason_code !== null) ||
		(result.reject_reason_code !== null && !REJECT_REASONS.has(result.reject_reason_code))
	) {
		throw new Error("PROTOCOL_INVALID");
	}
	if (
		result.identity_acknowledged &&
		!(result.state === "accepted" && result.broker_terminal_state === "accepted" && result.application_acceptance_observed)
	) {
		throw new Error("PROTOCOL_INVALID");
	}
	return result;
}

function readMirror(sessionId: string, event: string): { kind: "absent" | "malformed" | "valid"; mirror: Mirror | null } {
	const file = offerPath(sessionId, event);
	try {
		const stat = lstatSync(file);
		if (!stat.isFile() || stat.isSymbolicLink()) return { kind: "malformed", mirror: null };
		const mirror = validateMirror(parseJsonFile(file));
		if (mirror.session_id !== sessionId || mirror.event_id !== event) return { kind: "malformed", mirror: null };
		return { kind: "valid", mirror };
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return { kind: "absent", mirror: null };
		return { kind: "malformed", mirror: null };
	}
}

function receivedMirror(sessionId: string, event: string, token: string): Mirror {
	return {
		schema_version: 1,
		session_id: sessionId,
		recipient_id: sessionId,
		event_id: event,
		offer_token: token,
		omp_process_epoch: getProcessEpoch(),
		state: "received",
		broker_terminal_state: "none",
		identity_acknowledged: false,
		health_reason: "NONE",
		reject_reason_code: null,
		application_acceptance_observed: false,
		updated_at_ms: Date.now(),
	};
}

function writeMirror(mirror: Mirror): void {
	atomicWriteJson(offerPath(mirror.session_id, mirror.event_id), mirror as unknown as Record<string, unknown>);
}

function transitionClear(sessionId: string): boolean {
	for (const entry of getGlobalState().entries.values()) {
		if (entry.session_id === sessionId && entry.unresolved) return false;
	}
	const directory = path.dirname(offerPath(sessionId, "1"));
	let names: string[];
	try {
		names = Array.from(new Bun.Glob("*.json").scanSync({ cwd: directory, onlyFiles: true }));
	} catch {
		return true;
	}
	for (const name of names) {
		const match = /^([1-9][0-9]{0,18})\.json$/.exec(name);
		if (!match) continue;
		const result = readMirror(sessionId, match[1]);
		if (result.kind !== "valid" || !result.mirror) return false;
		if (result.mirror.broker_terminal_state === "none") return false;
		if (result.mirror.state === "accepted" && !result.mirror.identity_acknowledged) return false;
	}
	return true;
}

function entryKey(sessionId: string, event: string): string {
	return `${sessionId}:${event}`;
}

function serializeEntry(entry: Entry, operation: () => void | Promise<void>): void {
	entry.chain = entry.chain.then(operation, operation).then(
		() => undefined,
		() => undefined,
	);
}

function unresolvedForSession(sessionId: string, except?: string): boolean {
	for (const entry of getGlobalState().entries.values()) {
		if (entry.session_id === sessionId && entry.unresolved && entry.event_id !== except) return true;
	}
	return false;
}

// The session omp is in now. In omp every context shares one SessionManager, so the latest main
// context's session id follows switches even for a handler still running from before one.
function activeSessionId(): string | null {
	return getGlobalState().relay_ctx?.sessionManager.getSessionId() ?? null;
}

function publishHealth(gate: Gate, healthState: string, code: string, retryInMs: number | null): void {
	const global = getGlobalState();
	const pair = global.current_pair;
	if (!pair || global.stopping || global.gates.get(gate.session_id) !== gate) return;
	// The health file is the child's, and the child serves the session omp is in: a gate kept for a
	// session omp has left writes nothing there, whatever path (claim, bind, terminal) reaches here.
	if (gate.session_id !== activeSessionId()) return;
	const file = path.join(pair.rendezvous_dir, "health.json");
	atomicWriteJson(file, {
		schema_version: 1,
		instance_id: pair.instance_id,
		binding_epoch: gate.binding?.binding_epoch ?? null,
		state: healthState,
		code,
		since_ms: Date.now(),
		retry_in_ms: retryInMs,
	});
}

function signEnvelope(payload: Record<string, unknown>): Record<string, unknown> {
	const signature = cryptoSign(null, Buffer.from(canonicalJson(payload), "utf8"), getGlobalState().keypair.privateKey);
	return { ...payload, signature_b64url: base64url(signature) };
}

function publicKeyRaw(): string {
	const der = getGlobalState().keypair.publicKey.export({ format: "der", type: "spki" }) as Buffer;
	return base64url(der.subarray(der.length - 32));
}

function identityRefusal(rendezvous: string, request: Record<string, unknown>, code: string): void {
	atomicWriteJson(path.join(rendezvous, "identity.json"), {
		schema_version: 1,
		ok: false,
		instance_id: request.instance_id,
		control_epoch: request.control_epoch,
		bind_attempt_id: request.bind_attempt_id,
		code,
	});
}

async function handleIdentityRequest(params: unknown, ctx: any): Promise<void> {
	if (ctx.agent?.kind !== "main") return;
	const fields = new Set([
		"schema_version",
		"type",
		"instance_id",
		"control_epoch",
		"mcp_process",
		"rendezvous_dir",
		"bind_attempt_id",
		"new_binding_epoch",
		"expected_binding_epoch",
		"control_token_hash",
	]);
	let request: Record<string, unknown>;
	let rendezvous: string | null = null;
	try {
		request = closed(params, fields);
		if (request.schema_version !== 1 || request.type !== "identity_request") throw new Error("PROTOCOL_INVALID");
		const instanceId = uuid(request.instance_id);
		const controlEpoch = uuid(request.control_epoch);
		const bindAttempt = uuid(request.bind_attempt_id);
		uuid(request.new_binding_epoch);
		if (request.expected_binding_epoch !== null) uuid(request.expected_binding_epoch);
		if (typeof request.control_token_hash !== "string" || !HASH_TEXT.test(request.control_token_hash)) {
			throw new Error("PROTOCOL_INVALID");
		}
		const witness = validateWitness(request.mcp_process);
		const global = getGlobalState();
		if (!global.runtime_root) throw new Error("IDENTITY_INVALID");
		rendezvous = directChildRendezvous(global.runtime_root, request.rendezvous_dir, instanceId);
		const child = verifyDirectChild(witness);
		if (!new Set(["tui", "rpc"]).has(ctx.mode)) {
			identityRefusal(rendezvous, request, "MODE_UNSUPPORTED");
			return;
		}
		// Root-change guard: before authorizing any new child identity the CURRENT declaration must
		// still name the pinned pair. The refusal goes only into this rendezvous, which was already
		// validated as a direct child of the pinned runtime root.
		if (!currentDeclarationPinned(ctx)) {
			identityRefusal(rendezvous, request, "IDENTITY_INVALID");
			return;
		}
		const sequence = BigInt(child.witness.sequence_number);
		if (global.high_water_boot !== null && global.high_water_boot !== child.witness.boot_id) {
			throw new Error("CONTROL_CHANNEL_INVALID");
		}
		if (global.superseded_instances.has(instanceId)) throw new Error("CONTROL_CHANNEL_INVALID");
		if (global.high_water_sequence !== null) {
			if (sequence < global.high_water_sequence) throw new Error("CONTROL_CHANNEL_INVALID");
			if (sequence === global.high_water_sequence) {
				const pair = global.current_pair;
				if (
					!pair ||
					pair.instance_id !== instanceId ||
					pair.control_epoch !== controlEpoch ||
					canonicalJson(pair.witness) !== canonicalJson(witness)
				) {
					throw new Error("CONTROL_CHANNEL_INVALID");
				}
			}
		}
		if (global.current_pair && global.current_pair.instance_id !== instanceId) {
			global.superseded_instances.add(global.current_pair.instance_id);
			for (const [attempt, approval] of global.pending_approvals) {
				if (approval.instance_id !== instanceId) global.pending_approvals.delete(attempt);
			}
		}
		global.high_water_boot = witness.boot_id;
		global.high_water_sequence = sequence;
		global.current_pair = { instance_id: instanceId, control_epoch: controlEpoch, witness, rendezvous_dir: rendezvous };
		global.owned_rendezvous.add(rendezvous);

		const sessionId = uuid(ctx.sessionManager.getSessionId());
		const gate = global.gates.get(sessionId);
		if (!gate) throw new Error("IDENTITY_INVALID");
		const connection = relayConnection(currentRelayManager());
		await coordinatorFor(sessionId).fifo_tail;
		if (global.gates.get(sessionId) !== gate || gate.claim.ownership !== "self" || global.stopping) {
			identityRefusal(rendezvous, request, gate?.claim.ownership === "none" ? "SESSION_ALREADY_OWNED" : "IDENTITY_INVALID");
			return;
		}
		if (gate.state === "quiescing" || gate.state === "restoring") {
			identityRefusal(rendezvous, request, "DELIVERY_PENDING");
			return;
		}
		// omp may have moved to another session during the claim wait; an identity is authorized only
		// for the session omp is in.
		if (activeSessionId() !== sessionId) {
			identityRefusal(rendezvous, request, "IDENTITY_INVALID");
			return;
		}
		// The claim wait above can be long; the declaration may have changed during it. Re-checked
		// here, before any approval is recorded or either authorization file is written.
		if (!currentDeclarationPinned(ctx)) {
			identityRefusal(rendezvous, request, "IDENTITY_INVALID");
			return;
		}
		// A connection replaced during the claim wait no longer carries this child, so nothing may be
		// authorized, or later credited, for it.
		if (relayConnection(currentRelayManager()) !== connection) {
			identityRefusal(rendezvous, request, "CONTROL_CHANNEL_INVALID");
			return;
		}
		const pending: PendingApproval = {
			instance_id: instanceId,
			control_epoch: controlEpoch,
			bind_attempt_id: bindAttempt,
			binding_epoch: uuid(request.new_binding_epoch),
			gate,
			claim: gate.claim,
			control_token_hash: request.control_token_hash as string,
			witness,
			connection,
		};
		for (const [attempt, approval] of global.pending_approvals) {
			if (approval.instance_id === instanceId && approval.control_epoch === controlEpoch) {
				global.pending_approvals.delete(attempt);
			}
		}
		global.pending_approvals.set(bindAttempt, pending);
		const processRecord = queryProcess(process.pid);
		atomicWriteJson(path.join(rendezvous, "identity.json"), {
			schema_version: 1,
			ok: true,
			instance_id: instanceId,
			control_epoch: controlEpoch,
			mcp_process: witness,
			bind_attempt_id: bindAttempt,
			binding_epoch: pending.binding_epoch,
			omp_process_epoch: getProcessEpoch(),
			omp_pid: process.pid,
			omp_process_start_ms: processRecord.start_ms,
			session_id: sessionId,
			mode: ctx.mode,
			agent_kind: "main",
			session_name: ctx.sessionManager.getSessionName?.() ?? null,
			control_public_key_b64url: publicKeyRaw(),
			written_at_ms: Date.now(),
		});
		const authorization = signEnvelope({
			schema_version: 1,
			command_id: bindAttempt,
			instance_id: instanceId,
			control_epoch: controlEpoch,
			binding_epoch: pending.binding_epoch,
			omp_process_epoch: getProcessEpoch(),
			session_id: sessionId,
			issued_at_ms: Date.now(),
			action: "AUTHORIZE_BIND",
			args: {
				bind_attempt_id: bindAttempt,
				expected_binding_epoch: request.expected_binding_epoch,
				gate_generation: gate.gate_generation,
				claim_generation: gate.claim.claim_generation,
				mcp_process: witness,
				control_token_hash: request.control_token_hash,
			},
		});
		atomicWriteJson(path.join(rendezvous, "control", "requests", `${bindAttempt}.json`), authorization);
		publishHealth(gate, "rebinding", "REBINDING", null);
	} catch (error) {
		if (rendezvous && isRecord(params)) identityRefusal(rendezvous, params, identityRefusalCode(error));
	}
}

function handleIdentityBound(params: unknown, ctx: any): void {
	if (ctx.agent?.kind !== "main") return;
	const p = closed(
		params,
		new Set([
			"schema_version",
			"type",
			"instance_id",
			"control_epoch",
			"bind_attempt_id",
			"binding_epoch",
			"omp_process_epoch",
			"session_id",
			"gate_generation",
			"claim_generation",
		]),
	);
	if (p.schema_version !== 1 || p.type !== "identity_bound") throw new Error("PROTOCOL_INVALID");
	const global = getGlobalState();
	const approval = global.pending_approvals.get(uuid(p.bind_attempt_id));
	if (
		!approval ||
		approval.instance_id !== uuid(p.instance_id) ||
		approval.control_epoch !== uuid(p.control_epoch) ||
		approval.binding_epoch !== uuid(p.binding_epoch) ||
		p.omp_process_epoch !== getProcessEpoch() ||
		approval.gate.session_id !== uuid(p.session_id) ||
		approval.gate.gate_generation !== uuid(p.gate_generation) ||
		approval.claim.claim_generation !== uuid(p.claim_generation) ||
		global.gates.get(approval.gate.session_id) !== approval.gate ||
		approval.gate.claim !== approval.claim ||
		approval.claim.ownership !== "self" ||
		approval.claim.lifecycle !== "live" ||
		approval.gate.state === "quiescing" || approval.gate.state === "restoring" ||
		global.current_pair?.instance_id !== approval.instance_id ||
		// Authorized while its session was current; omp has since moved on, so it commits nothing.
		approval.gate.session_id !== activeSessionId()
	) {
		throw new Error("BINDING_CONFLICT");
	}
	// Root-change guard at commit: the declaration may have changed since this identity request was
	// authorized. The child sees only the existing conflict path; nothing is bound.
	if (!currentDeclarationPinned(ctx)) throw new Error("BINDING_CONFLICT");
	// The binding, and the readiness it gives, belong to the connection this identity was authorized
	// over. If omp has replaced it since, this is a late completion from a child omp has dropped.
	if (relayConnection(currentRelayManager()) !== approval.connection) throw new Error("BINDING_CONFLICT");
	const binding: Binding = {
		instance_id: approval.instance_id,
		control_epoch: approval.control_epoch,
		bind_attempt_id: approval.bind_attempt_id,
		binding_epoch: approval.binding_epoch,
		omp_process_epoch: getProcessEpoch(),
		session_id: approval.gate.session_id,
		gate_generation: approval.gate.gate_generation,
		claim_generation: approval.claim.claim_generation,
	};
	approval.gate.binding = binding;
	global.bound_connection = approval.connection;
	global.bound_binding = binding;
	approval.gate.state = "event_fenced";
	global.pending_approvals.delete(approval.bind_attempt_id);
	if (transitionClear(binding.session_id)) approval.gate.state = "open";
	publishHealth(
		approval.gate,
		approval.gate.state === "open" ? "connected" : "blocked",
		approval.gate.state === "open" ? "CONNECTED" : "DELIVERY_PENDING",
		null,
	);
}

// A notification proves which gate it concerns only through that gate's stored binding: the
// recipient's gate counts when the notification names exactly its binding, made by the current child
// in this omp process, and its claim still owns the session. The recipient id alone selects nothing.
function provenGate(params: Record<string, unknown>, ctx: any): Gate {
	if (ctx.agent?.kind !== "main") throw new Error("IDENTITY_INVALID");
	const gate = getGlobalState().gates.get(uuid(params.recipient_id));
	const pair = getGlobalState().current_pair;
	if (
		!gate?.binding ||
		pair?.instance_id !== uuid(params.instance_id) ||
		gate.binding.instance_id !== params.instance_id ||
		gate.binding.binding_epoch !== uuid(params.binding_epoch) ||
		gate.binding.omp_process_epoch !== getProcessEpoch() ||
		gate.claim.ownership !== "self"
	) {
		throw new Error("BINDING_CONFLICT");
	}
	// Every delivery, terminal control and recovery comes through here, and each can reopen a gate or
	// admit an offer. A quiescing gate's transition has not completed (or was cancelled unseen), so it
	// takes none of them and nothing about it changes.
	if (gate.state === "quiescing") throw new Error("DELIVERY_PENDING");
	return gate;
}

// New-message admission, and recovery (which can admit), are for the current session only.
function currentBoundGate(params: Record<string, unknown>, ctx: any): Gate {
	if (ctx.sessionManager.getSessionId() !== uuid(params.recipient_id)) throw new Error("IDENTITY_INVALID");
	return provenGate(params, ctx);
}

// Result handling for a proven gate, which need not be the current session's: a child's result can
// complete after omp has moved on. A fault found from here on concerns that gate, and only the
// current session's gate may write the child's health.
function forProvenGate(gate: Gate, ctx: any, apply: (current: boolean) => void): void {
	const current = gate.session_id === ctx.sessionManager.getSessionId();
	try {
		apply(current);
	} catch (error) {
		fenceGate(gate, fixedError(error), current);
	}
}

async function handleMessage(params: unknown, ctx: any): Promise<void> {
	if (ctx.agent?.kind !== "main") return;
	const p = closed(
		params,
		new Set([
			"schema_version",
			"instance_id",
			"binding_epoch",
			"recipient_id",
			"offer_token",
			"offer_generation",
			"current_relay_name",
			"delivery",
			"display_text",
		]),
	);
	if (p.schema_version !== 1 || p.offer_generation !== 1 || typeof p.current_relay_name !== "string" || !RELAY_NAME.test(p.current_relay_name)) {
		throw new Error("PROTOCOL_INVALID");
	}
	const gate = currentBoundGate(p, ctx);
	const delivery = validateDelivery(p.delivery);
	const sessionId = uuid(p.recipient_id);
	const token = uuid(p.offer_token);
	if (p.display_text !== renderDisplay(delivery)) throw new Error("PROTOCOL_INVALID");
	const key = entryKey(sessionId, delivery.event_id);
	let entry = getGlobalState().entries.get(key);
	if (entry && entry.offer_token !== token) throw new Error("RECEIPT_CONFLICT");
	if (entry?.adoption_ambiguous) {
		fenceGate(gate, "PERSISTENCE_STALLED");
		return;
	}
	if (!entry) {
		if (gate.state !== "open" || unresolvedForSession(sessionId)) throw new Error("DELIVERY_PENDING");
		entry = {
			key,
			session_id: sessionId,
			event_id: delivery.event_id,
			offer_token: token,
			omp_process_epoch: getProcessEpoch(),
			delivery,
			display_text: p.display_text as string,
			chain: Promise.resolve(),
			unresolved: true,
			terminal: false,
			send_right: false,
			send_consumed: false,
			pending_adoption: false,
			adoption_ambiguous: false,
		};
		getGlobalState().entries.set(key, entry);
		if (!exclusiveWriteJson(offerPath(sessionId, delivery.event_id), receivedMirror(sessionId, delivery.event_id, token) as unknown as Record<string, unknown>)) {
			const current = readMirror(sessionId, delivery.event_id);
			if (current.kind !== "valid" || current.mirror?.offer_token !== token || current.mirror.omp_process_epoch !== getProcessEpoch()) {
				fenceGate(gate, "DURABILITY_MISMATCH");
				return;
			}
			if (current.mirror.state !== "received") return;
		}
	}
	const current = readMirror(sessionId, delivery.event_id);
	if (current.kind !== "valid" || !current.mirror || current.mirror.offer_token !== token || current.mirror.omp_process_epoch !== getProcessEpoch()) {
		fenceGate(gate, "DURABILITY_MISMATCH");
		return;
	}
	if (current.mirror.state !== "received" || entry.send_consumed) return;
	const sender = ctx.api?.sendUserMessage ?? ctx.__ompApi?.sendUserMessage;
	if (typeof sender !== "function") {
		publishHealth(gate, "connected", "DELIVERY_RETRY_PENDING", 1_000);
		return;
	}
	entry.send_consumed = true;
	try {
		await sender(`${p.display_text}\n\n${marker(delivery.event_id, token)}`, {
			deliverAs: "aside",
			attribution: "agent",
		});
	} catch {
		entry.send_consumed = false;
		publishHealth(gate, "connected", "DELIVERY_RETRY_PENDING", 1_000);
		return;
	}
	const reread = readMirror(sessionId, delivery.event_id);
	if (reread.kind !== "valid" || !reread.mirror || reread.mirror.offer_token !== token) {
		fenceGate(gate, "DELIVERY_UNCERTAIN_CONTENT_MAY_HAVE_BEEN_SEEN");
		return;
	}
	try {
		writeMirror({ ...reread.mirror, state: "injected", updated_at_ms: Date.now() });
	} catch {
		fenceGate(gate, "DELIVERY_UNCERTAIN_CONTENT_MAY_HAVE_BEEN_SEEN");
	}
}

function fenceGate(gate: Gate, code: string, publish = true): void {
	if (gate.state === "open" || gate.state === "checking") {
		gate.state = "event_fenced";
		gate.fence_code = null;
	}
	// DELIVERY_PENDING is the delivery barrier every delivery passes; any other code is an error.
	if (gate.state === "event_fenced" && code !== "DELIVERY_PENDING") gate.fence_code = code;
	if (publish) publishHealth(gate, "blocked", code, null);
}

function terminalControl(params: unknown, ctx: any): void {
	const p = closed(params, new Set(["schema_version", "type", "instance_id", "binding_epoch", "recipient_id", "event_id", "offer_token", ...(isRecord(params) && params.type === "offer_rejected" ? ["reject_reason_code"] : [])]));
	if (p.schema_version !== 1 || !new Set(["offer_accepted", "offer_rejected", "identity_acked"]).has(p.type as string)) {
		throw new Error("PROTOCOL_INVALID");
	}
	const gate = provenGate(p, ctx);
	forProvenGate(gate, ctx, current => {
		const event = eventId(p.event_id);
		const token = uuid(p.offer_token);
		const mirrorResult = readMirror(gate.session_id, event);
		if (mirrorResult.kind !== "valid" || !mirrorResult.mirror || mirrorResult.mirror.offer_token !== token) {
			throw new Error("RECEIPT_CONFLICT");
		}
		const entry = getGlobalState().entries.get(entryKey(gate.session_id, event));
		if (entry && entry.offer_token !== token) throw new Error("RECEIPT_CONFLICT");
		if (p.type === "offer_accepted") {
			if (mirrorResult.mirror.state !== "accepted" || mirrorResult.mirror.broker_terminal_state !== "none") {
				if (mirrorResult.mirror.state === "accepted" && mirrorResult.mirror.broker_terminal_state === "accepted") return;
				throw new Error("RECEIPT_CONFLICT");
			}
			fenceGate(gate, "DELIVERY_PENDING", current);
			writeMirror({ ...mirrorResult.mirror, broker_terminal_state: "accepted", identity_acknowledged: false, updated_at_ms: Date.now() });
			if (entry) {
				entry.unresolved = false;
				entry.terminal = true;
			}
			return;
		}
		if (p.type === "offer_rejected") {
			const reason = p.reject_reason_code;
			if (typeof reason !== "string" || !REJECT_REASONS.has(reason)) throw new Error("PROTOCOL_INVALID");
			if (mirrorResult.mirror.state === "rejected" && mirrorResult.mirror.reject_reason_code === reason) return;
			if (!new Set(["received", "injected", "uncertain", "accepted"]).has(mirrorResult.mirror.state)) {
				throw new Error("RECEIPT_CONFLICT");
			}
			fenceGate(gate, "DELIVERY_PENDING", current);
			writeMirror({
				...mirrorResult.mirror,
				state: "rejected",
				broker_terminal_state: "rejected",
				identity_acknowledged: false,
				reject_reason_code: reason,
				updated_at_ms: Date.now(),
			});
			if (entry) {
				entry.unresolved = false;
				entry.terminal = true;
			}
			if (transitionClear(gate.session_id)) {
				gate.state = "open";
				if (current) publishHealth(gate, "connected", "CONNECTED", null);
			}
			return;
		}
		if (mirrorResult.mirror.state !== "accepted" || mirrorResult.mirror.broker_terminal_state !== "accepted") {
			throw new Error("RECEIPT_CONFLICT");
		}
		fenceGate(gate, "DELIVERY_PENDING", current);
		writeMirror({ ...mirrorResult.mirror, omp_process_epoch: getProcessEpoch(), identity_acknowledged: true, updated_at_ms: Date.now() });
		if (transitionClear(gate.session_id)) {
			gate.state = "open";
			if (current) publishHealth(gate, "connected", "CONNECTED", null);
		}
	});
}

function validateOfferStatus(value: unknown): Record<string, unknown> {
	const status = closed(
		value,
		new Set([
			"recipient_id",
			"event_id",
			"offer_token",
			"offer_generation",
			"binding_epoch",
			"omp_process_epoch",
			"offer_state",
			"delivery_status",
			"reject_reason_code",
		]),
	);
	uuid(status.recipient_id);
	eventId(status.event_id);
	uuid(status.binding_epoch);
	uuid(status.omp_process_epoch);
	if (status.offer_token === null) {
		if (status.offer_generation !== null || status.offer_state !== null) throw new Error("PROTOCOL_INVALID");
	} else {
		uuid(status.offer_token);
		if (status.offer_generation !== 1 || !new Set(["offered", "injected", "accepted", "rejected"]).has(status.offer_state as string)) {
			throw new Error("PROTOCOL_INVALID");
		}
	}
	if (!new Set(["outstanding", "acked", "rejected"]).has(status.delivery_status as string)) throw new Error("PROTOCOL_INVALID");
	if (status.reject_reason_code !== null && (typeof status.reject_reason_code !== "string" || !REJECT_REASONS.has(status.reject_reason_code))) {
		throw new Error("PROTOCOL_INVALID");
	}
	return status;
}

function terminalRecovery(params: unknown, ctx: any): void {
	const p = closed(
		params,
		new Set([
			"schema_version",
			"type",
			"instance_id",
			"binding_epoch",
			"recipient_id",
			"event_id",
			"offer_token",
			"terminal_state",
			"reject_reason_code",
			"recovery_source",
		]),
	);
	if (p.schema_version !== 1 || p.type !== "offer_terminal_recovery" || p.recovery_source !== "malformed_mirror") {
		throw new Error("PROTOCOL_INVALID");
	}
	const gate = provenGate(p, ctx);
	forProvenGate(gate, ctx, current => {
		const event = eventId(p.event_id);
		const token = uuid(p.offer_token);
		if (!new Set(["accepted", "rejected"]).has(p.terminal_state as string)) throw new Error("PROTOCOL_INVALID");
		const existing = readMirror(gate.session_id, event);
		if (existing.kind === "absent" || existing.kind === "valid") throw new Error("RECEIPT_CONFLICT");
		const entry = getGlobalState().entries.get(entryKey(gate.session_id, event));
		if (entry && entry.offer_token !== token) throw new Error("RECEIPT_CONFLICT");
		const reason = p.reject_reason_code;
		if ((p.terminal_state === "rejected") !== (typeof reason === "string" && REJECT_REASONS.has(reason))) {
			throw new Error("PROTOCOL_INVALID");
		}
		fenceGate(gate, "DELIVERY_PENDING", current);
		const recovered = receivedMirror(gate.session_id, event, token);
		recovered.state = p.terminal_state as "accepted" | "rejected";
		recovered.broker_terminal_state = p.terminal_state as "accepted" | "rejected";
		recovered.application_acceptance_observed = p.terminal_state === "accepted";
		recovered.reject_reason_code = p.terminal_state === "rejected" ? (reason as string) : null;
		writeMirror(recovered);
		if (entry) {
			entry.unresolved = false;
			entry.terminal = true;
		}
		if (p.terminal_state === "rejected" && transitionClear(gate.session_id)) {
			gate.state = "open";
			if (current) publishHealth(gate, "connected", "CONNECTED", null);
		}
	});
}

function recoveryStatus(params: unknown, ctx: any, api: any): void {
	const p = closed(
		params,
		new Set([
			"schema_version",
			"type",
			"instance_id",
			"control_epoch",
			"binding_epoch",
			"omp_process_epoch",
			"status",
			"delivery",
		]),
	);
	if (p.schema_version !== 1 || p.type !== "offer_recovery_status") throw new Error("PROTOCOL_INVALID");
	const status = validateOfferStatus(p.status);
	const gate = currentBoundGate({ ...p, recipient_id: status.recipient_id }, ctx);
	if (p.control_epoch !== gate.binding?.control_epoch || p.omp_process_epoch !== getProcessEpoch()) {
		throw new Error("BINDING_CONFLICT");
	}
	const event = eventId(status.event_id);
	const token = status.offer_token === null ? null : uuid(status.offer_token);
	if (!token) return;
	const current = readMirror(gate.session_id, event);
	if (current.kind !== "valid" || !current.mirror || current.mirror.omp_process_epoch === getProcessEpoch()) return;
	const key = entryKey(gate.session_id, event);
	let entry = getGlobalState().entries.get(key);
	if (entry && entry.offer_token !== token) throw new Error("RECEIPT_CONFLICT");
	if (!entry) {
		entry = {
			key,
			session_id: gate.session_id,
			event_id: event,
			offer_token: token,
			omp_process_epoch: getProcessEpoch(),
			delivery: null,
			display_text: null,
			chain: Promise.resolve(),
			unresolved: true,
			terminal: false,
			send_right: false,
			send_consumed: false,
			pending_adoption: true,
			adoption_ambiguous: false,
		};
		getGlobalState().entries.set(key, entry);
	}
	const candidate = entry;
	fenceGate(gate, "DELIVERY_PENDING");
	if (candidate.adoption_ambiguous && status.offer_state !== "accepted" && status.offer_state !== "rejected") {
		candidate.pending_adoption = false;
		return;
	}
	serializeEntry(candidate, async () => {
		candidate.pending_adoption = true;
		const delivery = p.delivery === null ? null : validateDelivery(p.delivery);
		if (delivery && delivery.event_id !== event) throw new Error("PROTOCOL_INVALID");
		await coordinatorFor(gate.session_id).fifo_tail;
		const global = getGlobalState();
		if (global.gates.get(gate.session_id) !== gate || gate.claim.ownership !== "self" || global.stopping) return;
		const stateRoot = global.state_root!;
		try {
			withFileLockSync(
				guardResource(stateRoot, gate.session_id),
				() => {
					const envelope = readOwnerEnvelope(stateRoot, gate.session_id);
					if (!sameOwner(envelope.holder, gate.claim.expected_owner)) return;
					const offerState = status.offer_state;
					if (offerState === "accepted" || offerState === "rejected") {
						const replacement = receivedMirror(gate.session_id, event, token);
						replacement.state = offerState;
						replacement.broker_terminal_state = offerState;
						replacement.application_acceptance_observed = offerState === "accepted";
						replacement.identity_acknowledged = offerState === "accepted" && status.delivery_status === "acked";
						replacement.reject_reason_code = offerState === "rejected" ? (status.reject_reason_code as string) : null;
						writeMirror(replacement);
						candidate.unresolved = false;
						candidate.terminal = true;
						candidate.pending_adoption = false;
						candidate.adoption_ambiguous = false;
						return;
					}
					if (!envelope.dead_predecessors.some(w => w.omp_process_epoch === current.mirror!.omp_process_epoch)) return;
					if (current.mirror!.state === "accepted" && current.mirror!.application_acceptance_observed) {
						writeMirror({
							...current.mirror!,
							omp_process_epoch: getProcessEpoch(),
							broker_terminal_state: "none",
							identity_acknowledged: false,
							updated_at_ms: Date.now(),
						});
						candidate.pending_adoption = false;
						candidate.adoption_ambiguous = false;
						return;
					}
					if (!delivery) return;
					const accepted = hasExactReceipt(ctx, delivery, token);
					const replacement = receivedMirror(gate.session_id, event, token);
					if (accepted) {
						replacement.state = "accepted";
						replacement.application_acceptance_observed = true;
						writeMirror(replacement);
						candidate.pending_adoption = false;
						candidate.adoption_ambiguous = false;
						return;
					}
					const destination = offerPath(gate.session_id, event);
					const temporary = path.join(path.dirname(destination), `.${path.basename(destination)}.${randomUuid()}.tmp`);
					let replaceInvoked = false;
					try {
						let descriptor: number | null = openSync(temporary, "wx", 0o600);
						try {
							writeFileSync(descriptor, `${canonicalJson(replacement)}\n`, "utf8");
							fsyncSync(descriptor);
						} finally {
							closeSync(descriptor);
							descriptor = null;
						}
						replaceInvoked = true;
						renameSync(temporary, destination);
						flushDirectory(path.dirname(destination));
						candidate.send_right = true;
						candidate.pending_adoption = false;
						candidate.adoption_ambiguous = false;
					} catch {
						if (replaceInvoked) {
							candidate.adoption_ambiguous = true;
							candidate.pending_adoption = false;
						}
						return;
					} finally {
						try {
							unlinkSync(temporary);
						} catch {
							// no authority
						}
					}
				},
				{ retries: 1 },
			);
		} catch {
			return;
		}
		if (candidate.terminal) {
			if (transitionClear(gate.session_id)) {
				gate.state = "open";
				publishHealth(gate, "connected", "CONNECTED", null);
			}
			return;
		}
		if (candidate.send_right && !candidate.send_consumed && delivery) {
			candidate.send_right = false;
			candidate.send_consumed = true;
			candidate.delivery = delivery;
			candidate.display_text = renderDisplay(delivery);
			try {
				await api.sendUserMessage(`${candidate.display_text}\n\n${marker(event, token)}`, {
					deliverAs: "aside",
					attribution: "agent",
				});
			} catch {
				candidate.send_right = true;
				candidate.send_consumed = false;
				return;
			}
			const mirror = readMirror(gate.session_id, event);
			if (mirror.kind === "valid" && mirror.mirror) writeMirror({ ...mirror.mirror, state: "injected", updated_at_ms: Date.now() });
		}
	});
}

function hasExactReceipt(ctx: any, delivery: Delivery, token: string): boolean {
	const sessionFile = ctx.sessionManager.getSessionFile?.();
	if (typeof sessionFile !== "string" || !path.isAbsolute(sessionFile)) return false;
	let stat;
	let bytes: Buffer;
	try {
		stat = lstatSync(sessionFile);
		if (!stat.isFile() || stat.isSymbolicLink()) return false;
		bytes = readFileSync(sessionFile);
	} catch {
		return false;
	}
	if (bytes.length === 0 || bytes[bytes.length - 1] !== 0x0a) return false;
	const lines = bytes.toString("utf8").split("\n");
	lines.pop();
	if (lines.length === 0) return false;
	let headerCount = 0;
	let exactReceipt = false;
	const sessionId = deliveryRecipient(ctx);
	const expected = `${renderDisplay(delivery)}\n\n${marker(delivery.event_id, token)}`;
	for (const line of lines) {
		let row: any;
		try {
			row = JSON.parse(line);
		} catch {
			return false;
		}
		if (isRecord(row) && row.type === "session") {
			headerCount += 1;
			if (row.id !== sessionId) return false;
			continue;
		}
		if (headerCount !== 1) continue;
		const message = isRecord(row) && row.type === "message" && isRecord(row.message) ? row.message : null;
		if (!message || message.role !== "user" || !Array.isArray(message.content) || message.content.length !== 1) continue;
		const block = message.content[0];
		if (isRecord(block) && block.type === "text" && block.text === expected) exactReceipt = true;
	}
	return headerCount === 1 && exactReceipt;
}

function deliveryRecipient(ctx: any): string {
	return uuid(ctx.sessionManager.getSessionId());
}

function scanReceipts(ctx: any): void {
	if (ctx.agent?.kind !== "main") return;
	const sessionId = deliveryRecipient(ctx);
	for (const entry of getGlobalState().entries.values()) {
		if (entry.session_id !== sessionId || !entry.unresolved || !entry.delivery) continue;
		const current = readMirror(sessionId, entry.event_id);
		if (
			current.kind !== "valid" ||
			!current.mirror ||
			current.mirror.omp_process_epoch !== getProcessEpoch() ||
			!new Set(["received", "injected", "uncertain"]).has(current.mirror.state)
		) {
			continue;
		}
		if (hasExactReceipt(ctx, entry.delivery, entry.offer_token)) {
			serializeEntry(entry, () => {
				const reread = readMirror(sessionId, entry.event_id);
				if (reread.kind !== "valid" || !reread.mirror || reread.mirror.offer_token !== entry.offer_token) return;
				writeMirror({
					...reread.mirror,
					state: "accepted",
					application_acceptance_observed: true,
					broker_terminal_state: "none",
					updated_at_ms: Date.now(),
				});
			});
		}
	}
}

function handleRebindReconcile(params: unknown, ctx: any): void {
	const p = closed(params, new Set(["schema_version", "type", "instance_id", "control_epoch"]));
	const pair = getGlobalState().current_pair;
	if (
		p.schema_version !== 1 ||
		p.type !== "rebind_reconcile" ||
		!pair ||
		pair.instance_id !== uuid(p.instance_id) ||
		pair.control_epoch !== uuid(p.control_epoch) ||
		ctx.agent?.kind !== "main"
	) {
		throw new Error("CONTROL_CHANNEL_INVALID");
	}
	const gate = currentSessionGate(ctx);
	if (!gate) throw new Error("IDENTITY_INVALID");
	inspectOrAcquire(gate);
	for (const coordinator of getGlobalState().coordinators.values()) {
		for (const claim of coordinator.claims.values()) if (claim.lifecycle === "retired") releaseRetiredClaim(coordinator.session_id, claim);
	}
}

function fixedError(error: unknown): string {
	const text = error instanceof Error ? error.message : String(error);
	return /^[A-Z][A-Z0-9_]{2,63}$/.test(text) ? text : "PROTOCOL_INVALID";
}

function identityRefusalCode(error: unknown): string {
	const code = fixedError(error);
	if (IDENTITY_REFUSAL_CODES.has(code)) return code;
	return code === "PROTOCOL_INVALID" ? "IDENTITY_INVALID" : "CONTROL_CHANNEL_INVALID";
}

function currentSessionGate(ctx: any): Gate | null {
	if (ctx.agent?.kind !== "main") return null;
	return getGlobalState().gates.get(ctx.sessionManager.getSessionId()) ?? null;
}

function beforeTransition(ctx: any): { cancel?: boolean } | void {
	const gate = currentSessionGate(ctx);
	if (!gate) return;
	if (getGlobalState().stopping) return { cancel: true };
	if (gate.state === "open") {
		gate.state = "checking";
		if (!transitionClear(gate.session_id)) {
			gate.state = "event_fenced";
			publishHealth(gate, "blocked", "DELIVERY_PENDING", null);
			return { cancel: true };
		}
		gate.state = "quiescing";
		gate.resume_gate = "open";
		getGlobalState().pending_transition = gate;
		publishHealth(gate, "quiescing", "QUIESCING", null);
		return;
	}
	if (gate.state === "rebind_blocked") {
		gate.state = "quiescing";
		gate.resume_gate = "rebind_blocked";
		getGlobalState().pending_transition = gate;
		publishHealth(gate, "quiescing", "QUIESCING", null);
		return;
	}
	return { cancel: true };
}

function finishTransition(ctx: any): void {
	const global = getGlobalState();
	const quiesced = global.pending_transition;
	global.pending_transition = null;
	// The session being left holds the quiesced gate, or the gate /relay-unquiesce put in its place
	// while the transition was still pending. Completion is not a delivery barrier: while any offer of
	// that session is unresolved, unreceipted or unacknowledged, its gate stays where it is and its
	// claim keeps the lock.
	const held = quiesced ? (global.gates.get(quiesced.session_id) ?? quiesced) : null;
	const oldGate = held && transitionClear(held.session_id) ? held : null;
	if (held && !oldGate) held.retained = true;
	if (quiesced && oldGate && quiesced !== oldGate) {
		quiesced.claim.lifecycle = "retired";
		releaseRetiredClaim(quiesced.session_id, quiesced.claim);
	}
	// Before the first bind there are no roots; the completion handler's evaluateRelay binds then.
	// With roots pinned, a completion starts claim work only while the current declaration still
	// names them. Otherwise the old claim is released as after any finished transition, and nothing
	// new is claimed until evaluateRelay sees a matching declaration again.
	const claimAllowed = global.state_root !== null && global.runtime_root !== null && currentDeclarationPinned(ctx);
	if (oldGate) {
		global.gates.delete(oldGate.session_id);
		oldGate.claim.lifecycle = "retired";
		const sameSession =
			ctx.agent?.kind === "main" &&
			new Set(["tui", "rpc"]).has(ctx.mode) &&
			uuid(ctx.sessionManager.getSessionId()) === oldGate.session_id;
		if (sameSession && claimAllowed) {
			const fresh = freshGate(oldGate.session_id, ctx, "rebind_blocked", oldGate.claim);
			global.gates.set(oldGate.session_id, fresh);
			inspectOrAcquire(fresh);
			return;
		}
		releaseRetiredClaim(oldGate.session_id, oldGate.claim);
	}
	if (claimAllowed) startSession(ctx);
}

function gateHeldReason(gate: Gate): HeldReason | null {
	if (gate.state === "quiescing") return "TRANSITION_PENDING_QUIESCED";
	if (gate.state === "restoring") return "RESTORING_AFTER_UNQUIESCE";
	return gate.retained ? "DELIVERY_UNRESOLVED_AT_COMPLETION" : null;
}

function logHeld(kind: "held" | "fenced", change: "entered" | "left", sessionId: string, reason: string, event: string): void {
	try {
		const logger = getGlobalState().relay_api?.logger;
		const detail = { [kind]: change, reason, session_id: sessionId, event, server: SERVER_NAME };
		if (kind === "fenced" && change === "entered") logger?.warn?.(`omp-relay ${kind} ${change}: ${reason}`, detail);
		else logger?.info?.(`omp-relay ${kind} ${change}: ${reason}`, detail);
	} catch {
		// Logging must never decide whether the relay binds.
	}
}

// Run after each event that can change a hold or an error fence: logs entry to and exit from every
// held state and every error fence with its reason and that event, and shows either the moment the
// current session enters it. It ends neither itself; it only notices that every offer of a kept
// gate's session has cleared, and that a gate no longer fenced has no fence code.
function trackHeld(ctx: any, event: string): void {
	if (ctx?.agent?.kind !== "main" || !RELAY_MODES.has(ctx.mode)) return;
	const global = getGlobalState();
	const sessionId = ctx.sessionManager.getSessionId();
	const now = new Map<string, HeldReason>();
	// No gate is created while this holds, so it belongs to the session the before-hook ran for.
	if (global.transition_pending && !global.gates.has(sessionId)) now.set(sessionId, "TRANSITION_PENDING_BEFORE_BIND");
	const fenced = new Map<string, string>();
	for (const gate of global.gates.values()) {
		if (gate.retained && transitionClear(gate.session_id)) gate.retained = false;
		if (gate.state !== "event_fenced") gate.fence_code = null;
		const reason = gateHeldReason(gate);
		if (reason) now.set(gate.session_id, reason);
		if (gate.fence_code) fenced.set(gate.session_id, gate.fence_code);
	}
	for (const [kind, before, after] of [["held", global.held, now], ["fenced", global.fenced, fenced]] as const) {
		for (const [session, reason] of before as Map<string, string>) {
			if (after.get(session) !== reason) logHeld(kind, "left", session, reason, event);
		}
		for (const [session, reason] of after as Map<string, string>) {
			if (before.get(session) !== reason) logHeld(kind, "entered", session, reason, event);
		}
	}
	const changed = now.get(sessionId) !== global.held.get(sessionId) || fenced.get(sessionId) !== global.fenced.get(sessionId);
	global.held = now;
	global.fenced = fenced;
	// Leaving needs the full re-evaluation, which every caller that can end a hold or fence runs next.
	const heldNow = now.get(sessionId);
	const fenceNow = fenced.get(sessionId);
	if (changed && heldNow) showRelayStatus(ctx, HELD_STATUS[heldNow], null, currentRelayManager());
	else if (changed && fenceNow) showRelayStatus(ctx, STATUS_FENCED_PREFIX + fenceNow, null, currentRelayManager());
}

function unquiesce(ctx: any): void {
	const oldGate = currentSessionGate(ctx);
	if (!oldGate || oldGate.state !== "quiescing" || oldGate.session_id !== ctx.sessionManager.getSessionId()) {
		throw new Error("DELIVERY_PENDING");
	}
	// Restoring installs a new gate and claim generation; the refusal is an omp-side command notice.
	if (!currentDeclarationPinned(ctx)) throw new Error("RELAY_ROOT_CHANGED");
	const fresh = freshGate(oldGate.session_id, ctx, "restoring", oldGate.claim);
	fresh.binding = oldGate.binding;
	fresh.resume_gate = oldGate.resume_gate;
	getGlobalState().gates.set(oldGate.session_id, fresh);
	inspectOrAcquire(fresh);
	publishHealth(fresh, "restoring", "RESTORING", 30_000);
}

function startSession(ctx: any): void {
	const global = getGlobalState();
	if (ctx.agent?.kind !== "main" || !new Set(["tui", "rpc"]).has(ctx.mode)) return;
	const sessionId = uuid(ctx.sessionManager.getSessionId());
	const existing = global.gates.get(sessionId);
	if (existing && existing.omp_process_epoch === getProcessEpoch()) {
		existing.context = ctx;
		return;
	}
	const gate = freshGate(sessionId, ctx, "rebind_blocked");
	global.gates.set(sessionId, gate);
	inspectOrAcquire(gate);
}

function assertMainToolSurface(api: any, ctx: any): void {
	if (ctx.agent?.kind !== "main") return;
	const tools = api
		.getAllTools()
		.filter((tool: any) => tool?.sourceInfo?.source === "mcp" && (EXPECTED_TOOL_NAMES.has(tool.name) || String(tool.name).startsWith("mcp__omp_relay_")));
	const names = new Set(tools.map((tool: any) => tool.name));
	if (tools.length !== 4 || names.size !== 4 || [...EXPECTED_TOOL_NAMES].some(name => !names.has(name))) {
		const gate = currentSessionGate(ctx);
		if (gate) fenceGate(gate, "PROTOCOL_INVALID");
	}
}

async function stripRelayTools(api: any, ctx: any): Promise<void> {
	if (ctx.agent?.kind !== "sub") return;
	const active = api.getActiveTools();
	const filtered = active.filter((name: string) => !EXPECTED_TOOL_NAMES.has(name));
	if (filtered.length !== active.length) await api.setActiveTools(filtered);
}

function assertChildIsolation(api: any, ctx: any): void {
	if (ctx.agent?.kind !== "sub") return;
	const active = new Set(api.getActiveTools());
	if ([...EXPECTED_TOOL_NAMES].some(name => active.has(name))) {
		ctx.abort();
		throw new Error("RELAY_TOOLS_FORBIDDEN");
	}
}

function commandAllowed(ctx: any): boolean {
	return ctx.agent?.kind === "main" && ctx.mode === "tui" && ctx.hasUI === true;
}

async function writeUserControl(action: "TAKEOVER_NAME" | "REJECT_DELIVERY", args: Record<string, unknown>, ctx: any): Promise<Record<string, unknown>> {
	const gate = currentSessionGate(ctx);
	const pair = getGlobalState().current_pair;
	if (!gate?.binding || !pair || gate.binding.instance_id !== pair.instance_id || gate.claim.ownership !== "self") {
		throw new Error("BINDING_CONFLICT");
	}
	if (action === "TAKEOVER_NAME" && gate.state !== "open") throw new Error("DELIVERY_PENDING");
	if (action === "REJECT_DELIVERY" && !new Set(["open", "event_fenced"]).has(gate.state)) throw new Error("DELIVERY_PENDING");
	const commandId = randomUuid();
	const envelope = signEnvelope({
		schema_version: 1,
		command_id: commandId,
		instance_id: pair.instance_id,
		control_epoch: pair.control_epoch,
		binding_epoch: gate.binding.binding_epoch,
		omp_process_epoch: getProcessEpoch(),
		session_id: gate.session_id,
		issued_at_ms: Date.now(),
		action,
		args,
	});
	atomicWriteJson(path.join(pair.rendezvous_dir, "control", "requests", `${commandId}.json`), envelope);
	const resultPath = path.join(pair.rendezvous_dir, "control", "results", `${commandId}.json`);
	const deadline = Date.now() + APPROVAL_MAX_AGE_MS;
	while (Date.now() <= deadline) {
		if (existsSync(resultPath)) {
			const result = closed(
				parseJsonFile(resultPath),
				new Set([
					"schema_version",
					"command_id",
					"instance_id",
					"control_epoch",
					"binding_epoch",
					"omp_process_epoch",
					"session_id",
					"action",
					"ok",
					"code",
					"result",
				]),
			);
			if (
				result.schema_version !== 1 ||
				uuid(result.command_id) !== commandId ||
				uuid(result.instance_id) !== pair.instance_id ||
				uuid(result.control_epoch) !== pair.control_epoch ||
				uuid(result.binding_epoch) !== gate.binding.binding_epoch ||
				uuid(result.omp_process_epoch) !== getProcessEpoch() ||
				uuid(result.session_id) !== gate.session_id ||
				result.action !== action
			) {
				throw new Error("CONTROL_CHANNEL_INVALID");
			}
			const ok = strictBool(result.ok);
			if (typeof result.code !== "string") throw new Error("PROTOCOL_INVALID");
			if (!ok) {
				if (!USER_CONTROL_FAILURE_CODES.has(result.code) || result.result !== null) throw new Error("PROTOCOL_INVALID");
				return result;
			}
			if (action === "TAKEOVER_NAME") {
				const actionResult = closed(result.result, new Set(["relay_name", "displaced_id"]));
				if (
					result.code !== "NAME_CLAIMED" ||
					typeof actionResult.relay_name !== "string" ||
					!RELAY_NAME.test(actionResult.relay_name) ||
					(actionResult.displaced_id !== null &&
						(typeof actionResult.displaced_id !== "string" || actionResult.displaced_id.length < 1 || actionResult.displaced_id.length > 256))
				) {
					throw new Error("PROTOCOL_INVALID");
				}
				return result;
			}
			const actionResult = closed(
				result.result,
				new Set(["event_id", "offer_state", "delivery_status", "reject_reason_code"]),
			);
			if (
				result.code !== "REJECTED" ||
				eventId(actionResult.event_id) !== args.event_id ||
				actionResult.offer_state !== "rejected" ||
				actionResult.delivery_status !== "rejected" ||
				!REJECT_REASONS.has(actionResult.reject_reason_code as string)
			) {
				throw new Error("PROTOCOL_INVALID");
			}
			return result;
		}
		await Bun.sleep(25);
	}
	throw new Error("BROKER_UNREACHABLE");
}

function cleanupRendezvous(): void {
	const state = getGlobalState();
	for (const directory of state.owned_rendezvous) {
		try {
			rmSync(directory, { recursive: true, force: true });
		} catch {
			// Process exit remains the final cleanup boundary.
		}
	}
	state.owned_rendezvous.clear();
}

async function dispatchRelayNotification(api: any, event: any, ctx: any): Promise<void> {
	if (event.method === "notifications/tools/list_changed") {
		assertMainToolSurface(api, ctx);
		return;
	}
	try {
		if (event.method === MESSAGE_METHOD) {
			await handleMessage(event.params, ctx);
			return;
		}
		if (event.method !== CONTROL_METHOD) return;
		const type = isRecord(event.params) ? event.params.type : null;
		if (type === "identity_request") await handleIdentityRequest(event.params, ctx);
		else if (type === "identity_bound") handleIdentityBound(event.params, ctx);
		else if (type === "rebind_reconcile") handleRebindReconcile(event.params, ctx);
		else if (type === "offer_accepted" || type === "offer_rejected" || type === "identity_acked") {
			terminalControl(event.params, ctx);
		} else if (type === "offer_terminal_recovery") terminalRecovery(event.params, ctx);
		else if (type === "offer_recovery_status") recoveryStatus(event.params, ctx, api);
		else throw new Error("PROTOCOL_INVALID");
	} catch (error) {
		// No gate was proven (forProvenGate handles faults after proof). Only a notification addressed
		// to the current session, or to none, can fence the current gate; an old session's stale or
		// unprovable traffic is refused and fences nothing.
		const gate = currentSessionGate(ctx);
		const recipient = addressedSession(event.params);
		if (recipient !== null && recipient !== ctx.sessionManager.getSessionId()) logRefused(event.params, recipient, fixedError(error));
		else if (gate) fenceGate(gate, fixedError(error));
	}
}

function addressedSession(params: unknown): string | null {
	if (!isRecord(params)) return null;
	const recipient = isRecord(params.status)
		? params.status.recipient_id
		: params.type === "identity_bound"
			? params.session_id
			: params.recipient_id;
	return typeof recipient === "string" ? recipient : null;
}

function logRefused(params: Record<string, unknown> | unknown, recipient: string, code: string): void {
	try {
		const type = isRecord(params) && typeof params.type === "string" ? params.type : "message";
		getGlobalState().relay_api?.logger?.warn?.(`omp-relay refused ${type} for another session: ${code}`, {
			code,
			type,
			recipient_id: recipient,
			server: SERVER_NAME,
		});
	} catch {
		// Logging must never decide whether the relay binds.
	}
}

export default function ompRelayExtension(api: any): void {
	const global = getGlobalState();
	global.stopping = false;
	// Roots are read from the `omp-relay` entry on the first event that can see it, never here and
	// never from omp's own environment (evaluateRelay).
	void subscribeConnectionStatus();

	api.on("session_start", async (_event: unknown, ctx: any) => {
		ctx.__ompApi = api;
		if (ctx.agent?.kind === "sub") {
			await stripRelayTools(api, ctx);
			return;
		}
		if (!RELAY_MODES.has(ctx.mode)) return;
		if (ctx.agent?.kind === "main") global.transition_pending = false;
		trackHeld(ctx, "session_start");
		await evaluateRelay(api, ctx);
		if (!global.state_root || !global.runtime_root) return;
		ctx.setTimeout?.(() => {
			assertMainToolSurface(api, ctx);
			trackHeld(ctx, "tool_surface_check");
		}, 500);
	});
	api.on("before_agent_start", async (_event: unknown, ctx: any) => {
		if (ctx.agent?.kind === "sub") {
			await stripRelayTools(api, ctx);
			assertChildIsolation(api, ctx);
			return;
		}
		trackHeld(ctx, "before_agent_start");
		await evaluateRelay(api, ctx);
	});
	api.on("mcp_notification", async (event: any, ctx: any) => {
		if (event.server !== SERVER_NAME) return;
		ctx.__ompApi = api;
		// Bind first, so an identity request that arrives early waits behind the queued claim.
		await evaluateRelay(api, ctx);
		await dispatchRelayNotification(api, event, ctx);
		trackHeld(ctx, "mcp_notification");
		await evaluateRelay(api, ctx);
	});
	for (const eventName of ["message_end", "turn_end", "agent_end"] as const) {
		api.on(eventName, (_event: unknown, ctx: any) => scanReceipts(ctx));
	}
	api.on("turn_end", async (_event: unknown, ctx: any) => {
		if (ctx.agent?.kind !== "main") return;
		trackHeld(ctx, "turn_end");
		await evaluateRelay(api, ctx);
	});
	for (const eventName of ["session_before_switch", "session_before_branch", "session_before_tree"] as const) {
		api.on(eventName, (_event: unknown, ctx: any) => {
			const result = beforeTransition(ctx);
			// Before the first bind there is no gate to quiesce, but the session still must not claim or
			// bind until this transition's completion event. omp 18.8.6 lets a turn run while a
			// transition is pending and tells extensions nothing when one is cancelled or fails after
			// this hook, so no turn event ends the hold: a cancelled transition leaves it held until the
			// next completed one (README, Session ownership and transitions).
			if (!result?.cancel && ctx.agent?.kind === "main" && RELAY_MODES.has(ctx.mode) && !currentSessionGate(ctx)) {
				global.transition_pending = true;
			}
			trackHeld(ctx, eventName);
			return result;
		});
	}
	for (const eventName of ["session_switch", "session_branch", "session_tree"] as const) {
		api.on(eventName, async (_event: unknown, ctx: any) => {
			finishTransition(ctx);
			global.transition_pending = false;
			trackHeld(ctx, eventName);
			await evaluateRelay(api, ctx);
		});
	}
	api.on("session_shutdown", (_event: unknown, ctx: any) => {
		if (ctx.agent?.kind !== "main") return;
		for (const gate of global.gates.values()) publishHealth(gate, "stopped", "STOPPED", null);
		global.stopping = true;
		cleanupRendezvous();
	});

	api.registerCommand("relay-takeover", {
		description: "Claim a relay name with explicit user-confirmed takeover.",
		handler: async (args: string, ctx: any) => {
			if (!commandAllowed(ctx)) {
				ctx.ui?.notify?.("RELAY_COMMAND_REFUSED", "error");
				return;
			}
			const name = args.trim();
			if (!RELAY_NAME.test(name)) {
				ctx.ui.notify("PROTOCOL_INVALID", "error");
				return;
			}
			const gate = currentSessionGate(ctx);
			const currentOwner = gate?.session_id ?? "unbound";
			if (!(await ctx.ui.confirm("Relay takeover", `Current identity: ${currentOwner}\nRequested relay name: ${name}`))) return;
			try {
				const result = await writeUserControl("TAKEOVER_NAME", { name }, ctx);
				ctx.ui.notify(String(result.code), result.ok ? "info" : "error");
			} catch (error) {
				ctx.ui.notify(fixedError(error), "error");
			}
		},
	});
	api.registerCommand("relay-reject", {
		description: "Reject the exact held relay delivery after user confirmation.",
		handler: async (args: string, ctx: any) => {
			if (!commandAllowed(ctx)) {
				ctx.ui?.notify?.("RELAY_COMMAND_REFUSED", "error");
				return;
			}
			let event: string;
			try {
				event = eventId(args.trim());
			} catch {
				ctx.ui.notify("PROTOCOL_INVALID", "error");
				return;
			}
			const gate = currentSessionGate(ctx);
			const entry = gate ? global.entries.get(entryKey(gate.session_id, event)) : null;
			const mirror = gate ? readMirror(gate.session_id, event) : { kind: "absent" as const, mirror: null };
			const token = entry?.offer_token ?? mirror.mirror?.offer_token;
			if (!token) {
				ctx.ui.notify("DELIVERY_PENDING", "error");
				return;
			}
			if (!(await ctx.ui.confirm("Reject relay delivery", `Reject event ${event} as USER_REJECTED?`))) return;
			try {
				const result = await writeUserControl("REJECT_DELIVERY", { event_id: event, offer_token: token, reason_code: "USER_REJECTED" }, ctx);
				ctx.ui.notify(String(result.code), result.ok ? "info" : "error");
			} catch (error) {
				ctx.ui.notify(fixedError(error), "error");
			}
		},
	});
	api.registerCommand("relay-unquiesce", {
		description: "Restore a cancelled relay session transition without bypassing ownership checks.",
		handler: async (_args: string, ctx: any) => {
			if (!commandAllowed(ctx)) {
				ctx.ui?.notify?.("RELAY_COMMAND_REFUSED", "error");
				return;
			}
			try {
				unquiesce(ctx);
			} catch (error) {
				ctx.ui.notify(fixedError(error), "error");
				return;
			}
			trackHeld(ctx, "relay-unquiesce");
			// The restoring hold ends when the restore claim lands on the session's queue. The command
			// does not wait for it: an acquisition queued ahead of it may still be pending.
			void coordinatorFor(ctx.sessionManager.getSessionId())
				.fifo_tail.then(() => {
					trackHeld(ctx, "claim_result");
					return evaluateRelay(api, ctx);
				})
				.catch(() => undefined);
		},
	});
}

export const __testing = {
	getGlobalState,
	queryProcess,
	validateDelivery,
	validateMirror,
	validateOfferStatus,
	compareEventIds,
	transitionClear,
	readMirror,
	hasExactReceipt,
	beforeTransition,
	finishTransition,
	unquiesce,
	coordinatorFor,
	freshGate,
	inspectOrAcquire,
	inspectOrAcquireGuarded,
	isClaimDemanded,
	releaseRetiredClaim,
	readOwnerEnvelope,
	writeOwnerEnvelope,
	sameOwner,
	reset(): void {
		const root = globalThis as Record<PropertyKey, unknown>;
		delete root[GLOBAL_KEY];
		delete root[PROCESS_EPOCH_KEY];
	},
	setProcessQuery(query: (pid: number) => ProcessRecord): void {
		getGlobalState().test_process_query = query;
	},
	setMcpManager(manager: RelayManager | null): void {
		getGlobalState().test_mcp_manager = manager;
	},
};
