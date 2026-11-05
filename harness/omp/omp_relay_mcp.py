"""OMP relay stdio MCP adapter.

The broker owns delivery and terminal state.  This process owns the private broker
credential and HTTP/SSE transport; the companion OMP extension owns host admission,
filesystem mirrors, process ownership, and user-message injection.
"""

from __future__ import annotations

import argparse
import asyncio
import contextvars
import functools
import base64
import ctypes
import hashlib
import json
import os
import re
import shutil
import struct
import sys
import time
from pathlib import Path
from typing import Annotated, Any, Coroutine, Literal, Protocol
from urllib.parse import quote
from uuid import UUID, uuid4

import httpx
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from mcp import types as mcp_types
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError
from mcp.shared.message import SessionMessage
from pydantic import Field

SCHEMA_VERSION = 1
SERVER_NAME = "omp-relay"
MESSAGE_METHOD = "notifications/omp/relay/message"
CONTROL_METHOD = "notifications/omp/relay/control"
SQLITE_I64_MAX = 9_223_372_036_854_775_807
JS_SAFE_INT_MAX = 9_007_199_254_740_991
RECONCILE_INTERVAL_SECONDS = 30.0
APPROVAL_MAX_AGE_MS = 30_000
DEFAULT_BROKER_URL = "http://127.0.0.1:8787"
EVENT_ID_RE = re.compile(r"[1-9][0-9]{0,18}\Z")
CURSOR_RE = re.compile(r"(?:0|[1-9][0-9]{0,18})\Z")
UUID_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\Z"
)
HASH_RE = re.compile(r"[0-9a-f]{64}\Z")
B64URL_32_RE = re.compile(r"[A-Za-z0-9_-]{43}\Z")
B64URL_64_RE = re.compile(r"[A-Za-z0-9_-]{86}\Z")
RELAY_NAME_RE = re.compile(r"[a-z0-9._-]{1,64}\Z")

AGENT_STATUSES = frozenset({"idle", "queued", "running", "blocked", "done", "failed"})
EVENT_KINDS = frozenset({"agent_message", "agent_reply", "handoff"})
MIRROR_STATES = frozenset({"received", "injected", "accepted", "rejected", "uncertain"})
BROKER_TERMINAL_STATES = frozenset({"none", "accepted", "rejected"})
REJECT_REASONS = frozenset(
    {
        "USER_REJECTED",
        "PROTOCOL_INVALID",
        "RECIPIENT_POLICY_REFUSED",
        "PERSISTENCE_UNRECOVERABLE",
        "DELIVERY_UNCERTAIN_CONTENT_MAY_HAVE_BEEN_SEEN",
    }
)
TOOL_NAMES = ("register", "name", "check", "send")

IDENTITY_REFUSAL_CODES = frozenset(
    {
        "MODE_UNSUPPORTED",
        "SESSION_ALREADY_OWNED",
        "IDENTITY_INVALID",
        "CONTROL_CHANNEL_INVALID",
        "BROKER_CAPABILITY_MISSING",
        "DELIVERY_PENDING",
    }
)
USER_CONTROL_FAILURE_CODES = frozenset(
    {
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
    }
)


def _user_control_failure_code(code: str) -> str:
    if code in USER_CONTROL_FAILURE_CODES:
        return code
    return {
        "CONTROL_REQUIRED": "CONTROL_CHANNEL_INVALID",
        "CONTROL_INVALID": "CONTROL_CHANNEL_INVALID",
        "PERSISTENCE_STALLED": "CONTROL_CHANNEL_INVALID",
        "IDENTITY_CONFLICT": "IDENTITY_INVALID",
        "DELIVERY_NOT_FOUND": "HEAD_CONFLICT",
        "DELIVERY_INVALID": "PROTOCOL_INVALID",
    }.get(code, "PROTOCOL_INVALID")

StatusArg = Literal["idle", "queued", "running", "blocked", "done", "failed"]
KindArg = Literal["agent_message", "agent_reply", "handoff"]
CursorArg = Annotated[str, Field(pattern=r"^(?:0|[1-9][0-9]{0,18})$")]
LimitArg = Annotated[int, Field(ge=1, le=100)]
RelayNameArg = Annotated[str, Field(pattern=r"^[a-z0-9._-]{1,64}$")]
AgentNameArg = Annotated[str, Field(min_length=1, max_length=256)]
ChannelArg = Annotated[str, Field(min_length=1, max_length=256)]


class _Notifier(Protocol):
    async def __call__(self, method: str, params: dict[str, Any]) -> None: ...


class RelayProtocolError(RuntimeError):
    """A closed protocol refusal safe to expose as a fixed code."""

    def __init__(self, code: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(code)
        self.code = code
        self.details = details or {}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _uuid(value: Any) -> str:
    if not isinstance(value, str) or UUID_RE.fullmatch(value) is None:
        raise RelayProtocolError("PROTOCOL_INVALID")
    try:
        if str(UUID(value)) != value:
            raise RelayProtocolError("PROTOCOL_INVALID")
    except ValueError as exc:
        raise RelayProtocolError("PROTOCOL_INVALID") from exc
    return value


def _event_id(value: Any, *, cursor: bool = False) -> str:
    pattern = CURSOR_RE if cursor else EVENT_ID_RE
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise RelayProtocolError("PROTOCOL_INVALID")
    number = int(value)
    if number > SQLITE_I64_MAX or (not cursor and number < 1):
        raise RelayProtocolError("PROTOCOL_INVALID")
    return value


def _safe_int(value: Any, *, positive: bool = False) -> int:
    if type(value) is not int or value < (1 if positive else 0) or value > JS_SAFE_INT_MAX:
        raise RelayProtocolError("PROTOCOL_INVALID")
    return value


def _uint64_string(value: Any) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[1-9][0-9]{0,19}", value):
        raise RelayProtocolError("PROTOCOL_INVALID")
    number = int(value)
    if number > 18_446_744_073_709_551_615:
        raise RelayProtocolError("PROTOCOL_INVALID")
    return value


def _closed(value: Any, fields: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise RelayProtocolError("PROTOCOL_INVALID")
    return value


def _b64url_decode(value: Any, *, size: int) -> bytes:
    pattern = B64URL_32_RE if size == 32 else B64URL_64_RE
    if not isinstance(value, str) or pattern.fullmatch(value) is None or "=" in value:
        raise RelayProtocolError("CONTROL_CHANNEL_INVALID")
    try:
        decoded = base64.urlsafe_b64decode(value + "=" * ((4 - len(value) % 4) % 4))
    except (ValueError, TypeError) as exc:
        raise RelayProtocolError("CONTROL_CHANNEL_INVALID") from exc
    if len(decoded) != size:
        raise RelayProtocolError("CONTROL_CHANNEL_INVALID")
    return decoded


def _canonical_json(value: Any) -> bytes:
    """RFC-8785 bytes for this protocol's fixed-key, integer-only value domain."""

    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _read_closed_json(path: Path, fields: set[str]) -> dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8")
        value = json.loads(raw, object_pairs_hook=_reject_duplicate_pairs)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise RelayProtocolError("PERSISTENCE_STALLED") from exc
    return _closed(value, fields)


def _reject_duplicate_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise RelayProtocolError("PROTOCOL_INVALID")
        result[key] = value
    return result


def _atomic_write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temp = path.with_name(f".{path.name}.{uuid4()}.tmp")
    data = _canonical_json(value) + b"\n"
    try:
        with temp.open("xb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
        _flush_directory(path.parent)
    finally:
        try:
            temp.unlink()
        except FileNotFoundError:
            pass


def _flush_directory(directory: Path) -> None:
    if os.name == "nt":
        # Python cannot portably open a Windows directory for FlushFileBuffers.
        # Atomic replacement and file flush are supported; the documented platform
        # limit is never represented as stronger directory-fsync proof.
        return
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _reject_lexical_reparse_ancestors(path: Path, label: str) -> None:
    current = Path(path.anchor)
    for part in path.parts[1:]:
        if part == ".":
            continue
        if part == "..":
            current = current.parent
            continue
        current /= part
        try:
            info = current.lstat()
        except OSError as exc:
            raise RelayProtocolError(f"{label}_INVALID") from exc
        attributes = getattr(info, "st_file_attributes", 0)
        if current.is_symlink() or attributes & 0x400:  # FILE_ATTRIBUTE_REPARSE_POINT
            raise RelayProtocolError(f"{label}_INVALID")


def _canonical_root(raw: str, label: str) -> Path:
    if not raw:
        raise RelayProtocolError(f"{label}_INVALID")
    path = Path(raw)
    if not path.is_absolute():
        raise RelayProtocolError(f"{label}_INVALID")
    _reject_lexical_reparse_ancestors(path, label)
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise RelayProtocolError(f"{label}_INVALID") from exc
    norm = os.path.normcase(os.path.normpath(str(path)))
    if norm != os.path.normcase(os.path.normpath(str(resolved))):
        raise RelayProtocolError(f"{label}_INVALID")
    if resolved == Path(resolved.anchor) or not resolved.is_dir():
        raise RelayProtocolError(f"{label}_INVALID")
    current = Path(resolved.anchor)
    for part in resolved.parts[1:]:
        current /= part
        try:
            info = current.lstat()
        except OSError as exc:
            raise RelayProtocolError(f"{label}_INVALID") from exc
        attributes = getattr(info, "st_file_attributes", 0)
        if current.is_symlink() or attributes & 0x400:  # FILE_ATTRIBUTE_REPARSE_POINT
            raise RelayProtocolError(f"{label}_INVALID")
    return resolved


def _validate_base_interpreter() -> None:
    base = getattr(sys, "_base_executable", None)
    if not isinstance(base, str) or not base:
        raise RelayProtocolError("INTERPRETER_INVALID")
    executable = Path(sys.executable).resolve(strict=True)
    base_executable = Path(base).resolve(strict=True)
    if os.path.normcase(str(executable)) != os.path.normcase(str(base_executable)):
        raise RelayProtocolError("INTERPRETER_INVALID")
    lower_parts = tuple(part.lower() for part in executable.parts)
    if len(lower_parts) >= 2 and lower_parts[-2:] == ("scripts", "python.exe"):
        raise RelayProtocolError("INTERPRETER_INVALID")


class _WindowsProcessApi:
    PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
    SYNCHRONIZE = 0x00100000
    WAIT_TIMEOUT = 0x00000102
    PROCESS_BASIC_INFORMATION = 0
    PROCESS_TELEMETRY_ID_INFORMATION = 64

    def __init__(self) -> None:
        if os.name != "nt" or ctypes.sizeof(ctypes.c_void_p) != 8 or sys.byteorder != "little":
            raise RelayProtocolError("NATIVE_ABI_UNSUPPORTED")
        self.kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        self.ntdll = ctypes.WinDLL("ntdll")
        self.kernel32.OpenProcess.argtypes = (ctypes.c_uint32, ctypes.c_int, ctypes.c_uint32)
        self.kernel32.OpenProcess.restype = ctypes.c_void_p
        self.kernel32.WaitForSingleObject.argtypes = (ctypes.c_void_p, ctypes.c_uint32)
        self.kernel32.WaitForSingleObject.restype = ctypes.c_uint32
        self.kernel32.CloseHandle.argtypes = (ctypes.c_void_p,)
        self.kernel32.CloseHandle.restype = ctypes.c_int
        self.ntdll.NtQueryInformationProcess.argtypes = (
            ctypes.c_void_p,
            ctypes.c_int,
            ctypes.c_void_p,
            ctypes.c_uint32,
            ctypes.POINTER(ctypes.c_uint32),
        )
        self.ntdll.NtQueryInformationProcess.restype = ctypes.c_long

    def query(self, pid: int) -> tuple[dict[str, Any], int]:
        if type(pid) is not int or pid <= 0 or pid > 0xFFFFFFFF:
            raise RelayProtocolError("NATIVE_WITNESS_INVALID")
        handle = self.kernel32.OpenProcess(
            self.PROCESS_QUERY_LIMITED_INFORMATION | self.SYNCHRONIZE, False, pid
        )
        if not handle:
            raise RelayProtocolError("NATIVE_WITNESS_INVALID")
        try:
            basic = ctypes.create_string_buffer(48)
            returned = ctypes.c_uint32(0)
            status = self.ntdll.NtQueryInformationProcess(
                handle,
                self.PROCESS_BASIC_INFORMATION,
                basic,
                ctypes.sizeof(basic),
                ctypes.byref(returned),
            )
            if status < 0 or returned.value != 48:
                raise RelayProtocolError("NATIVE_ABI_UNSUPPORTED")
            unique_pid = struct.unpack_from("<Q", basic.raw, 32)[0]
            parent_pid = struct.unpack_from("<Q", basic.raw, 40)[0]
            if unique_pid != pid or parent_pid > 0xFFFFFFFF:
                raise RelayProtocolError("NATIVE_WITNESS_INVALID")

            telemetry = ctypes.create_string_buffer(65_536)
            returned = ctypes.c_uint32(0)
            status = self.ntdll.NtQueryInformationProcess(
                handle,
                self.PROCESS_TELEMETRY_ID_INFORMATION,
                telemetry,
                ctypes.sizeof(telemetry),
                ctypes.byref(returned),
            )
            if status < 0 or returned.value < 64 or returned.value > 65_536:
                raise RelayProtocolError("NATIVE_ABI_UNSUPPORTED")
            header_size, telemetry_pid = struct.unpack_from("<II", telemetry.raw, 0)
            if (
                telemetry_pid != pid
                or header_size < 64
                or header_size > returned.value
                or header_size % 8 != 0
            ):
                raise RelayProtocolError("NATIVE_WITNESS_INVALID")
            start_key = struct.unpack_from("<Q", telemetry.raw, 8)[0]
            sequence = struct.unpack_from("<Q", telemetry.raw, 40)[0]
            boot_id = struct.unpack_from("<I", telemetry.raw, 60)[0]
            if start_key == 0 or sequence == 0:
                raise RelayProtocolError("NATIVE_WITNESS_INVALID")
            if self.kernel32.WaitForSingleObject(handle, 0) != self.WAIT_TIMEOUT:
                raise RelayProtocolError("NATIVE_WITNESS_INVALID")
            return (
                {
                    "pid": pid,
                    "start_key": str(start_key),
                    "sequence_number": str(sequence),
                    "boot_id": boot_id,
                },
                int(parent_pid),
            )
        finally:
            self.kernel32.CloseHandle(handle)


def _validate_process_witness(value: Any) -> dict[str, Any]:
    witness = _closed(value, {"pid", "start_key", "sequence_number", "boot_id"})
    _safe_int(witness["pid"], positive=True)
    _uint64_string(witness["start_key"])
    _uint64_string(witness["sequence_number"])
    if type(witness["boot_id"]) is not int or not 0 <= witness["boot_id"] <= 0xFFFFFFFF:
        raise RelayProtocolError("PROTOCOL_INVALID")
    return witness


def _validate_delivery(value: Any) -> dict[str, Any]:
    delivery = _closed(
        value,
        {
            "event_id",
            "client_event_id",
            "from_agent",
            "to_agent",
            "channel",
            "thread_id",
            "kind",
            "content",
            "ts",
        },
    )
    _event_id(delivery["event_id"])
    if delivery["client_event_id"] is not None and not isinstance(delivery["client_event_id"], str):
        raise RelayProtocolError("PROTOCOL_INVALID")
    for field in ("from_agent", "to_agent", "channel", "content"):
        if not isinstance(delivery[field], str):
            raise RelayProtocolError("PROTOCOL_INVALID")
    if delivery["thread_id"] is not None and not isinstance(delivery["thread_id"], str):
        raise RelayProtocolError("PROTOCOL_INVALID")
    if delivery["kind"] not in EVENT_KINDS:
        raise RelayProtocolError("PROTOCOL_INVALID")
    _safe_int(delivery["ts"])
    return delivery


def render_display(delivery: dict[str, Any]) -> str:
    delivery = _validate_delivery(delivery)
    return (
        f"[relay] from={delivery['from_agent']} to={delivery['to_agent']} "
        f"event_id={delivery['event_id']} kind={delivery['kind']}\n\n{delivery['content']}"
    )


# The session, binding, process epoch and control credential of the operation running in this task.
_OPERATION_BINDING: contextvars.ContextVar[tuple[str, str, str, bytes] | None] = contextvars.ContextVar(
    "omp_relay_operation_binding", default=None
)


def _attributed(method):
    # Pins, for one operation, the session, binding, process epoch and control credential its first broker
    # request uses; every later request in the operation reuses them (_request_binding, _private_headers),
    # so a rebind while one is in flight cannot send the rest to another session's endpoints. The broker
    # keeps each recipient's binding row until that recipient is bound again (server.py), so the first
    # session's own endpoint still authenticates them. A nested operation keeps its caller's pin; an
    # unbound runtime pins nothing, and its requests refuse as before.
    @functools.wraps(method)
    async def run(self, *args, **kwargs):
        if _OPERATION_BINDING.get() is not None or not self.bound:
            return await method(self, *args, **kwargs)
        pin = _OPERATION_BINDING.set((*self._require_bound(), self.control_token))
        try:
            return await method(self, *args, **kwargs)
        finally:
            _OPERATION_BINDING.reset(pin)

    return run


def _request_attribution(request: dict[str, Any]) -> tuple[str, str, str]:
    return request["session_id"], request["binding_epoch"], request["omp_process_epoch"]


def _validate_offer_status(value: Any) -> dict[str, Any]:
    status = _closed(
        value,
        {
            "recipient_id",
            "event_id",
            "offer_token",
            "offer_generation",
            "binding_epoch",
            "omp_process_epoch",
            "offer_state",
            "delivery_status",
            "reject_reason_code",
        },
    )
    _uuid(status["recipient_id"])
    _event_id(status["event_id"])
    _uuid(status["binding_epoch"])
    _uuid(status["omp_process_epoch"])
    if status["offer_token"] is None:
        if status["offer_generation"] is not None or status["offer_state"] is not None:
            raise RelayProtocolError("PROTOCOL_INVALID")
    else:
        _uuid(status["offer_token"])
        if status["offer_generation"] != 1 or status["offer_state"] not in {
            "offered",
            "injected",
            "accepted",
            "rejected",
        }:
            raise RelayProtocolError("PROTOCOL_INVALID")
    if status["delivery_status"] not in {"outstanding", "acked", "rejected"}:
        raise RelayProtocolError("PROTOCOL_INVALID")
    reason = status["reject_reason_code"]
    if reason is not None and reason not in REJECT_REASONS:
        raise RelayProtocolError("PROTOCOL_INVALID")
    if status["delivery_status"] == "acked" and status["offer_state"] != "accepted":
        raise RelayProtocolError("PROTOCOL_INVALID")
    if status["delivery_status"] == "rejected":
        if reason is None or status["offer_state"] not in {None, "rejected"}:
            raise RelayProtocolError("PROTOCOL_INVALID")
    elif reason is not None:
        raise RelayProtocolError("PROTOCOL_INVALID")
    return status


def _validate_mirror(value: Any) -> dict[str, Any]:
    mirror = _closed(
        value,
        {
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
        },
    )
    if mirror["schema_version"] != 1:
        raise RelayProtocolError("PROTOCOL_INVALID")
    if _uuid(mirror["session_id"]) != _uuid(mirror["recipient_id"]):
        raise RelayProtocolError("PROTOCOL_INVALID")
    _event_id(mirror["event_id"])
    _uuid(mirror["offer_token"])
    _uuid(mirror["omp_process_epoch"])
    if mirror["state"] not in MIRROR_STATES or mirror["broker_terminal_state"] not in BROKER_TERMINAL_STATES:
        raise RelayProtocolError("PROTOCOL_INVALID")
    if type(mirror["identity_acknowledged"]) is not bool or type(
        mirror["application_acceptance_observed"]
    ) is not bool:
        raise RelayProtocolError("PROTOCOL_INVALID")
    if mirror["health_reason"] not in {
        "NONE",
        "BINDING_CHANGED",
        "SESSION_TRANSITION",
        "SEND_REJECTED",
        "PERSISTENCE_STALLED",
        "PROTOCOL_INVALID",
    }:
        raise RelayProtocolError("PROTOCOL_INVALID")
    reason = mirror["reject_reason_code"]
    if (mirror["state"] == "rejected") != (reason is not None) or (
        reason is not None and reason not in REJECT_REASONS
    ):
        raise RelayProtocolError("PROTOCOL_INVALID")
    if mirror["identity_acknowledged"] and not (
        mirror["state"] == "accepted"
        and mirror["broker_terminal_state"] == "accepted"
        and mirror["application_acceptance_observed"]
    ):
        raise RelayProtocolError("PROTOCOL_INVALID")
    if mirror["state"] == "rejected" and mirror["identity_acknowledged"]:
        raise RelayProtocolError("PROTOCOL_INVALID")
    _safe_int(mirror["updated_at_ms"])
    return mirror


class OmpRelayRuntime:
    """One immutable MCP-child instance and its current broker binding."""

    def __init__(
        self,
        *,
        state_root: Path,
        runtime_root: Path,
        broker_url: str = DEFAULT_BROKER_URL,
        bearer_token: str | None = None,
        http_client: httpx.AsyncClient | None = None,
        notifier: _Notifier | None = None,
        process_witness: dict[str, Any] | None = None,
        parent_pid: int | None = None,
        validate_interpreter: bool = True,
    ) -> None:
        if validate_interpreter:
            _validate_base_interpreter()
        self.state_root = _canonical_root(str(state_root), "STATE_ROOT")
        self.runtime_root = _canonical_root(str(runtime_root), "RUNTIME_ROOT")
        self.broker_url = broker_url.rstrip("/")
        self.bearer_token = bearer_token
        self.http_client = http_client
        self.notifier = notifier
        self.server_session: Any = None
        self.instance_id = str(uuid4())
        self.control_epoch = str(uuid4())
        self.bind_attempt_id = str(uuid4())
        self.new_binding_epoch = str(uuid4())
        self.expected_binding_epoch: str | None = None
        self.control_token = os.urandom(32)
        self.control_token_hash = hashlib.sha256(self.control_token).hexdigest()
        if process_witness is None:
            api = _WindowsProcessApi()
            process_witness, observed_parent = api.query(os.getpid())
            if parent_pid is not None and observed_parent != parent_pid:
                raise RelayProtocolError("NATIVE_WITNESS_INVALID")
            parent_pid = observed_parent
        self.process_witness = _validate_process_witness(process_witness)
        self.parent_pid = parent_pid
        self.rendezvous_dir = self.runtime_root / f"mcp-{self.instance_id}"
        self.identity: dict[str, Any] | None = None
        self.binding_epoch: str | None = None
        self.omp_process_epoch: str | None = None
        self.session_id: str | None = None
        self.relay_name: str | None = None
        self.held: tuple[str, str] | None = None
        self._held_delivery: dict[str, Any] | None = None
        self._held_status: dict[str, Any] | None = None
        self._held_status_due = 0.0
        self._held_mirror_observation: tuple[Any, ...] | None = None
        self._next_delivery_notification = 0.0
        self.pending_ack: set[tuple[str, str]] = set()
        self.health_state = "starting"
        self.health_code = "STARTING"
        self.last_broker_contact_ms: int | None = None
        self._started = False
        self._stopping = False
        self._background_started = False
        self._tasks: set[asyncio.Task[Any]] = set()
        self._start_lock = asyncio.Lock()
        self._reconcile_lock = asyncio.Lock()
        self._event_locks: dict[str, asyncio.Lock] = {}
        self._reconcile_wake = asyncio.Event()
        self._command_digests: dict[str, bytes] = {}
        self._bind_attempt_ids = {self.bind_attempt_id}

    @property
    def bound(self) -> bool:
        return all((self.binding_epoch, self.omp_process_epoch, self.session_id, self.identity))

    def set_server_session(self, session: Any) -> None:
        self.server_session = session

    async def _notify(self, method: str, params: dict[str, Any]) -> None:
        if self.notifier is not None:
            await self.notifier(method, params)
            return
        if self.server_session is None:
            raise RelayProtocolError("CONTROL_CHANNEL_INVALID")
        notification = mcp_types.JSONRPCNotification(jsonrpc="2.0", method=method, params=params)
        message = mcp_types.JSONRPCMessage(notification)
        await self.server_session.send_message(SessionMessage(message))

    def _spawn(self, coroutine: Coroutine[Any, Any, Any]) -> None:
        task = asyncio.create_task(coroutine)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def start(self) -> None:
        async with self._start_lock:
            if self._started:
                return
            self._started = True
            try:
                await self._start_once()
            except RelayProtocolError as exc:
                self.health_state = "blocked"
                self.health_code = exc.code
                shutil.rmtree(self.rendezvous_dir, ignore_errors=True)
                self._spawn(self._startup_retry_loop(exc.code))
            except Exception:
                self.health_state = "blocked"
                self.health_code = "CONTROL_CHANNEL_INVALID"
                shutil.rmtree(self.rendezvous_dir, ignore_errors=True)
                self._spawn(self._startup_retry_loop("CONTROL_CHANNEL_INVALID"))

    async def _start_once(self) -> None:
        self._ensure_rendezvous()
        await self._require_capability()
        self.health_state = "waiting_identity"
        self.health_code = "WAITING_IDENTITY"
        await self._publish_identity_request()
        await self._publish_rebind_reconcile()
        self._spawn(self._complete_identity_handshake())

    async def _startup_retry_loop(self, prior_code: str) -> None:
        code = prior_code
        while not self._stopping:
            await asyncio.sleep(30.0 if code == "BROKER_CAPABILITY_MISSING" else 1.0)
            try:
                await self._start_once()
                return
            except RelayProtocolError as exc:
                code = exc.code
                self.health_state = "blocked"
                self.health_code = code
                shutil.rmtree(self.rendezvous_dir, ignore_errors=True)
            except Exception:
                code = "CONTROL_CHANNEL_INVALID"
                self.health_state = "blocked"
                self.health_code = code
                shutil.rmtree(self.rendezvous_dir, ignore_errors=True)

    async def _require_capability(self) -> None:
        payload = await self._request("GET", "/api/health", private=False)
        checks = payload.get("checks")
        if not isinstance(checks, dict) or checks.get("omp_relay_contract") != 1:
            self.health_state = "unsupported"
            self.health_code = "BROKER_CAPABILITY_MISSING"
            raise RelayProtocolError("BROKER_CAPABILITY_MISSING")

    async def _publish_identity_request(self) -> None:
        await self._notify(
            CONTROL_METHOD,
            {
                "schema_version": 1,
                "type": "identity_request",
                "instance_id": self.instance_id,
                "control_epoch": self.control_epoch,
                "mcp_process": self.process_witness,
                "rendezvous_dir": str(self.rendezvous_dir),
                "bind_attempt_id": self.bind_attempt_id,
                "new_binding_epoch": self.new_binding_epoch,
                "expected_binding_epoch": self.expected_binding_epoch,
                "control_token_hash": self.control_token_hash,
            },
        )

    def _ensure_rendezvous(self) -> None:
        self.rendezvous_dir.mkdir(mode=0o700, parents=False, exist_ok=True)
        (self.rendezvous_dir / "control" / "requests").mkdir(parents=True, mode=0o700, exist_ok=True)
        (self.rendezvous_dir / "control" / "results").mkdir(parents=True, mode=0o700, exist_ok=True)

    def _prepare_bind_attempt(self, expected_binding_epoch: str | None) -> None:
        self.bind_attempt_id = str(uuid4())
        self.new_binding_epoch = str(uuid4())
        self.expected_binding_epoch = expected_binding_epoch
        self.control_token = os.urandom(32)
        self.control_token_hash = hashlib.sha256(self.control_token).hexdigest()
        self._bind_attempt_ids.add(self.bind_attempt_id)
        self.identity = None
        self._ensure_rendezvous()
        try:
            (self.rendezvous_dir / "identity.json").unlink()
        except FileNotFoundError:
            pass

    async def _publish_rebind_reconcile(self) -> None:
        await self._notify(
            CONTROL_METHOD,
            {
                "schema_version": 1,
                "type": "rebind_reconcile",
                "instance_id": self.instance_id,
                "control_epoch": self.control_epoch,
            },
        )

    async def _await_bind_authorization(self) -> tuple[dict[str, Any], dict[str, Any]]:
        identity_path = self.rendezvous_dir / "identity.json"
        request_path = self.rendezvous_dir / "control" / "requests" / f"{self.bind_attempt_id}.json"
        deadline = time.monotonic() + 30.0
        while not self._stopping:
            if identity_path.exists():
                try:
                    identity = self._read_identity(identity_path)
                except RelayProtocolError:
                    try:
                        refusal = self._read_identity_refusal(identity_path)
                    except RelayProtocolError:
                        refusal = None
                    if refusal is not None:
                        raise RelayProtocolError(refusal["code"])
                else:
                    if request_path.exists():
                        return identity, self._read_bind_authorization(request_path, identity)
            if time.monotonic() >= deadline:
                raise RelayProtocolError("CONTROL_CHANNEL_INVALID")
            await asyncio.sleep(0.025)
        raise RelayProtocolError("CONTROL_CHANNEL_INVALID")

    async def _bind_with_backoff(self, authorization: dict[str, Any]) -> None:
        delay = 0.25
        body = self._bind_body()
        while not self._stopping:
            try:
                await self._bind(authorization, body)
                return
            except RelayProtocolError as exc:
                age = _now_ms() - authorization["issued_at_ms"]
                if exc.code != "BROKER_UNREACHABLE" or age + int(delay * 1000) > APPROVAL_MAX_AGE_MS:
                    raise
                await asyncio.sleep(delay)
                delay = min(delay * 2, 4.0)
        raise RelayProtocolError("CONTROL_CHANNEL_INVALID")

    async def _complete_identity_handshake(self) -> None:
        candidate_published = True
        expected_epoch: str | None = self.expected_binding_epoch
        while not self._stopping:
            try:
                if not candidate_published:
                    self._prepare_bind_attempt(expected_epoch)
                    await self._publish_rebind_reconcile()
                    await self._publish_identity_request()
                candidate_published = False
                identity, authorization = await self._await_bind_authorization()
                self.identity = identity
                await self._bind_with_backoff(authorization)
            except RelayProtocolError as exc:
                self.health_state = "blocked"
                self.health_code = exc.code
                if exc.code == "BINDING_CONFLICT":
                    current_epoch = exc.details.get("current_binding_epoch")
                    if current_epoch is None:
                        self.health_code = "PROTOCOL_INVALID"
                        expected_epoch = None
                        await asyncio.sleep(1.0)
                        continue
                    try:
                        _uuid(current_epoch)
                    except RelayProtocolError:
                        self.health_code = "PROTOCOL_INVALID"
                        expected_epoch = None
                        await asyncio.sleep(1.0)
                        continue
                    expected_epoch = current_epoch
                    await asyncio.sleep(1.0)
                    continue
                expected_epoch = None
                await asyncio.sleep(
                    30.0 if exc.code in {"MODE_UNSUPPORTED", "SESSION_ALREADY_OWNED", "OWNERSHIP_UNKNOWN"} else 1.0
                )
                continue
            except Exception:
                self.health_state = "blocked"
                self.health_code = "CONTROL_CHANNEL_INVALID"
                expected_epoch = None
                await asyncio.sleep(1.0)
                continue

            if not self._background_started:
                self._background_started = True
                self._spawn(self._reconciliation_loop())
                self._spawn(self._control_loop())
                if self.http_client is None:
                    self._spawn(self._sse_loop())

            confirmation_deadline = time.monotonic() + 30.0
            confirmed = False
            while not self._stopping and time.monotonic() < confirmation_deadline:
                if not self.rendezvous_dir.exists():
                    break
                health = self._read_health()
                if health.get("instance_id") == self.instance_id and health.get("binding_epoch") == self.binding_epoch:
                    confirmed = True
                    break
                await asyncio.sleep(0.025)
            if self._stopping:
                return
            if not confirmed:
                expected_epoch = self.binding_epoch
                self.health_state = "blocked"
                self.health_code = "CONTROL_CHANNEL_INVALID"
                self.binding_epoch = None
                self.omp_process_epoch = None
                self.session_id = None
                self.relay_name = None
                continue

            next_wake = 0.0
            while not self._stopping and self.bound:
                if not self.rendezvous_dir.exists():
                    break
                health = self._read_health()
                if health.get("instance_id") == self.instance_id and health.get("binding_epoch") != self.binding_epoch:
                    break
                if time.monotonic() >= next_wake:
                    try:
                        await self._publish_rebind_reconcile()
                    except RelayProtocolError:
                        pass
                    next_wake = time.monotonic() + RECONCILE_INTERVAL_SECONDS
                await asyncio.sleep(0.25)
            if self._stopping:
                return
            # The durable broker offer remains attached to its stable recipient.
            # Drop only this binding-local barrier; the next current-session claim
            # returns the same token and no acknowledgement is inferred.
            self._clear_held_event()
            self.pending_ack.clear()
            self._reconcile_wake.set()
            self.binding_epoch = None
            self.omp_process_epoch = None
            self.session_id = None
            self.relay_name = None
            expected_epoch = None

    def _read_identity_refusal(self, path: Path) -> dict[str, Any]:
        value = _read_closed_json(
            path,
            {"schema_version", "ok", "instance_id", "control_epoch", "bind_attempt_id", "code"},
        )
        if (
            value["schema_version"] != 1
            or value["ok"] is not False
            or value["instance_id"] != self.instance_id
            or value["control_epoch"] != self.control_epoch
            or value["bind_attempt_id"] != self.bind_attempt_id
            or value["code"] not in IDENTITY_REFUSAL_CODES
        ):
            raise RelayProtocolError("IDENTITY_INVALID")
        return value

    def _read_identity(self, path: Path) -> dict[str, Any]:
        fields = {
            "schema_version",
            "ok",
            "instance_id",
            "control_epoch",
            "mcp_process",
            "bind_attempt_id",
            "binding_epoch",
            "omp_process_epoch",
            "omp_pid",
            "omp_process_start_ms",
            "session_id",
            "mode",
            "agent_kind",
            "session_name",
            "control_public_key_b64url",
            "written_at_ms",
        }
        value = _read_closed_json(path, fields)
        if (
            value["schema_version"] != 1
            or value["ok"] is not True
            or value["instance_id"] != self.instance_id
            or value["control_epoch"] != self.control_epoch
            or value["mcp_process"] != self.process_witness
            or value["bind_attempt_id"] != self.bind_attempt_id
            or value["binding_epoch"] != self.new_binding_epoch
            or value["mode"] not in {"tui", "rpc"}
            or value["agent_kind"] != "main"
            or (value["session_name"] is not None and not isinstance(value["session_name"], str))
        ):
            raise RelayProtocolError("IDENTITY_INVALID")
        _uuid(value["omp_process_epoch"])
        _safe_int(value["omp_pid"], positive=True)
        _safe_int(value["omp_process_start_ms"])
        _uuid(value["session_id"])
        _safe_int(value["written_at_ms"])
        _b64url_decode(value["control_public_key_b64url"], size=32)
        return value

    def _read_bind_authorization(self, path: Path, identity: dict[str, Any]) -> dict[str, Any]:
        envelope = _read_closed_json(
            path,
            {
                "schema_version",
                "command_id",
                "instance_id",
                "control_epoch",
                "binding_epoch",
                "omp_process_epoch",
                "session_id",
                "issued_at_ms",
                "action",
                "args",
                "signature_b64url",
            },
        )
        args = _closed(
            envelope["args"],
            {
                "bind_attempt_id",
                "expected_binding_epoch",
                "gate_generation",
                "claim_generation",
                "mcp_process",
                "control_token_hash",
            },
        )
        if (
            envelope["schema_version"] != 1
            or envelope["command_id"] != self.bind_attempt_id
            or envelope["instance_id"] != self.instance_id
            or envelope["control_epoch"] != self.control_epoch
            or envelope["binding_epoch"] != self.new_binding_epoch
            or envelope["omp_process_epoch"] != identity["omp_process_epoch"]
            or envelope["session_id"] != identity["session_id"]
            or envelope["action"] != "AUTHORIZE_BIND"
            or args["bind_attempt_id"] != self.bind_attempt_id
            or args["expected_binding_epoch"] != self.expected_binding_epoch
            or args["mcp_process"] != self.process_witness
            or args["control_token_hash"] != self.control_token_hash
        ):
            raise RelayProtocolError("CONTROL_CHANNEL_INVALID")
        _uuid(args["gate_generation"])
        _uuid(args["claim_generation"])
        issued = _safe_int(envelope["issued_at_ms"])
        age = _now_ms() - issued
        if age < 0 or age > APPROVAL_MAX_AGE_MS:
            raise RelayProtocolError("CONTROL_CHANNEL_INVALID")
        signature = _b64url_decode(envelope["signature_b64url"], size=64)
        unsigned = dict(envelope)
        del unsigned["signature_b64url"]
        public_key = Ed25519PublicKey.from_public_bytes(
            _b64url_decode(identity["control_public_key_b64url"], size=32)
        )
        try:
            public_key.verify(signature, _canonical_json(unsigned))
        except InvalidSignature as exc:
            raise RelayProtocolError("CONTROL_CHANNEL_INVALID") from exc
        return envelope

    def _bind_body(self) -> dict[str, Any]:
        assert self.identity is not None
        return {
            "schema_version": 1,
            "transport": "omp-mcp",
            "kind": "omp-session",
            "channel": "#omp-relay",
            "thread_id": None,
            "status": "idle",
            "current_task": None,
            "last_activity_ts": _now_ms(),
            "display_name": self.identity["session_name"],
            "operation": "bind",
            "bind_attempt_id": self.bind_attempt_id,
            "expected_binding_epoch": self.expected_binding_epoch,
            "new_binding_epoch": self.new_binding_epoch,
            "omp_process_epoch": self.identity["omp_process_epoch"],
            "instance_id": self.instance_id,
            "control_epoch": self.control_epoch,
            "control_token_hash": self.control_token_hash,
        }

    async def _bind(self, authorization: dict[str, Any], body: dict[str, Any] | None = None) -> None:
        assert self.identity is not None
        if _now_ms() - authorization["issued_at_ms"] > APPROVAL_MAX_AGE_MS:
            raise RelayProtocolError("CONTROL_CHANNEL_INVALID")
        if body is None:
            body = self._bind_body()
        try:
            response = await self._request(
                "POST", f"/api/agent/{quote(self.identity['session_id'], safe='')}/status", json_body=body
            )
        except RelayProtocolError as exc:
            await self._write_bind_result(False, exc.code)
            raise
        expected_fields = {
            "schema_version",
            "id",
            "transport",
            "bind_attempt_id",
            "binding_epoch",
            "omp_process_epoch",
            "instance_id",
            "control_epoch",
            "relay_name",
            "registered_at",
            "event_id",
        }
        response = _closed(response, expected_fields)
        if (
            response["schema_version"] != 1
            or response["id"] != self.identity["session_id"]
            or response["transport"] != "omp-mcp"
            or response["bind_attempt_id"] != self.bind_attempt_id
            or response["binding_epoch"] != self.new_binding_epoch
            or response["omp_process_epoch"] != self.identity["omp_process_epoch"]
            or response["instance_id"] != self.instance_id
            or response["control_epoch"] != self.control_epoch
            or (response["relay_name"] is not None and not isinstance(response["relay_name"], str))
        ):
            raise RelayProtocolError("PROTOCOL_INVALID")
        _safe_int(response["registered_at"])
        _event_id(response["event_id"])
        # Install private current binding before any recovery observation.
        self.binding_epoch = response["binding_epoch"]
        self.omp_process_epoch = response["omp_process_epoch"]
        self.session_id = response["id"]
        self.relay_name = response["relay_name"]
        self.health_state = "unnamed" if self.relay_name is None else "connected"
        self.health_code = "NAME_REQUIRED" if self.relay_name is None else "CONNECTED"
        await self._write_bind_result(True, "BOUND")
        await self._notify(
            CONTROL_METHOD,
            {
                "schema_version": 1,
                "type": "identity_bound",
                "instance_id": self.instance_id,
                "control_epoch": self.control_epoch,
                "bind_attempt_id": self.bind_attempt_id,
                "binding_epoch": self.binding_epoch,
                "omp_process_epoch": self.omp_process_epoch,
                "session_id": self.session_id,
                "gate_generation": authorization["args"]["gate_generation"],
                "claim_generation": authorization["args"]["claim_generation"],
            },
        )

    async def _write_bind_result(self, ok: bool, code: str) -> None:
        if self.identity is None:
            return
        result = (
            {"bind_attempt_id": self.bind_attempt_id, "binding_epoch": self.new_binding_epoch}
            if ok
            else None
        )
        _atomic_write_json(
            self.rendezvous_dir / "control" / "results" / f"{self.bind_attempt_id}.json",
            {
                "schema_version": 1,
                "command_id": self.bind_attempt_id,
                "instance_id": self.instance_id,
                "control_epoch": self.control_epoch,
                "binding_epoch": self.new_binding_epoch,
                "omp_process_epoch": self.identity["omp_process_epoch"],
                "session_id": self.identity["session_id"],
                "action": "AUTHORIZE_BIND",
                "ok": ok,
                "code": code,
                "result": result,
            },
        )

    def _general_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.bearer_token}"} if self.bearer_token else {}

    def _private_headers(self) -> dict[str, str]:
        pinned = _OPERATION_BINDING.get()
        credential = pinned[3] if pinned is not None else self.control_token
        token = base64.urlsafe_b64encode(credential).rstrip(b"=").decode("ascii")
        return {"Authorization": f"RelayControl {token}"}

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        private: bool = False,
    ) -> dict[str, Any]:
        headers = self._private_headers() if private else self._general_headers()
        # Read before the await: the binding this request is made under, whatever is current when it fails.
        binding = self._operation_binding()

        async def send(client: httpx.AsyncClient) -> httpx.Response:
            return await client.request(method, path, params=params, json=json_body, headers=headers)

        try:
            if self.http_client is not None:
                response = await send(self.http_client)
            else:
                async with httpx.AsyncClient(base_url=self.broker_url, timeout=30.0) as client:
                    response = await send(client)
        except httpx.RequestError as exc:
            self._report_unreachable(binding)
            raise RelayProtocolError("BROKER_UNREACHABLE") from exc
        try:
            payload = response.json()
        except ValueError as exc:
            if response.status_code >= 500:
                self._report_unreachable(binding)
                raise RelayProtocolError("BROKER_UNREACHABLE") from exc
            raise RelayProtocolError("PROTOCOL_INVALID") from exc
        if not isinstance(payload, dict):
            if response.status_code >= 500:
                self._report_unreachable(binding)
                raise RelayProtocolError("BROKER_UNREACHABLE")
            raise RelayProtocolError("PROTOCOL_INVALID")
        self.last_broker_contact_ms = _now_ms()
        if response.is_error:
            error = payload.get("error")
            code = error.get("code") if isinstance(error, dict) else None
            details = error.get("details") if isinstance(error, dict) else None
            if not isinstance(code, str):
                code = "BROKER_UNREACHABLE" if response.status_code >= 500 else "PROTOCOL_INVALID"
            raise RelayProtocolError(code, details if isinstance(details, dict) else None)
        return payload

    def _report_unreachable(self, binding: tuple[str | None, str | None, str | None]) -> None:
        # A failed request is that binding's health; the error itself always goes to the operation.
        if self._still_bound_to(binding):
            self.health_state = "degraded" if self.bound else "rebinding"
            self.health_code = "BROKER_UNREACHABLE" if self.bound else "REBINDING"

    def _request_binding(self) -> tuple[str, str, str]:
        # A broker request's session and binding: the running operation's pin (_attributed), else current.
        pinned = _OPERATION_BINDING.get()
        return pinned[:3] if pinned is not None else self._require_bound()

    def _operation_binding(self) -> tuple[str | None, str | None, str | None]:
        # The binding the running operation started under: its pin (_attributed), else the current one.
        pinned = _OPERATION_BINDING.get()
        return pinned[:3] if pinned is not None else (self.session_id, self.binding_epoch, self.omp_process_epoch)

    def _still_bound_to(self, binding: tuple[str | None, str | None, str | None]) -> bool:
        # Binding-local state (the hold and its companions, the relay name, the child's health) belongs to
        # the current binding, so an operation writes it only while the binding it started under is still
        # current. The whole binding is compared: a same-session rebind keeps the session.
        return binding == (self.session_id, self.binding_epoch, self.omp_process_epoch)

    def _require_bound(self) -> tuple[str, str, str]:
        if not self.bound:
            raise ToolError(self.health_code)
        assert self.session_id and self.binding_epoch and self.omp_process_epoch
        return self.session_id, self.binding_epoch, self.omp_process_epoch

    def _observe_health(self) -> tuple[str, dict[str, Any] | None]:
        # health.json as last written, whatever instance and binding epoch it names: "absent", "malformed"
        # (unreadable or not the closed record), or "valid" with the record.
        if not self.rendezvous_dir.exists():
            return "absent", None
        health_path = self.rendezvous_dir / "health.json"
        if not health_path.exists():
            return "absent", None
        try:
            value = _read_closed_json(
                health_path,
                {
                    "schema_version",
                    "instance_id",
                    "binding_epoch",
                    "state",
                    "code",
                    "since_ms",
                    "retry_in_ms",
                },
            )
        except RelayProtocolError:
            return "malformed", None
        return "valid", value

    def _own_health(self) -> dict[str, Any]:
        # The child's in-memory health, as a record for its current binding.
        return {
            "schema_version": 1,
            "instance_id": self.instance_id,
            "binding_epoch": self.binding_epoch,
            "state": self.health_state,
            "code": self.health_code,
            "since_ms": _now_ms(),
            "retry_in_ms": None,
        }

    def _read_health(self) -> dict[str, Any]:
        # A record for this instance is returned whatever binding epoch it names: the binding driver learns from
        # one naming another epoch that the binding it holds has ended.
        kind, value = self._observe_health()
        if (
            kind == "valid"
            and value is not None
            and value["schema_version"] == 1
            and value["instance_id"] == self.instance_id
        ):
            return value
        return self._own_health()

    def _check_health(self, binding_epoch: str) -> dict[str, Any]:
        # check's health for the binding it is pinned to, read before its first await while that binding is
        # current. With no record, the child's own health. A record is used only when it names this instance and
        # this binding epoch; any other record, or one that cannot be read, is refused before the broker is asked.
        kind, value = self._observe_health()
        if kind == "absent":
            return self._own_health()
        if (
            kind == "valid"
            and value is not None
            and value["schema_version"] == 1
            and value["instance_id"] == self.instance_id
            and value["binding_epoch"] == binding_epoch
        ):
            return value
        raise ToolError("BINDING_CONFLICT")

    def _require_model_mutation(self, *, allow_unnamed: bool = False) -> None:
        self._require_bound()
        health = self._read_health()
        permitted = {"connected"}
        if allow_unnamed:
            permitted.add("unnamed")
        if health.get("state") not in permitted:
            raise ToolError(str(health.get("code", "DELIVERY_PENDING")))

    async def tool_register(self, status: str, current_task: str | None = None) -> dict[str, Any]:
        self._require_model_mutation(allow_unnamed=True)
        if status not in AGENT_STATUSES or (current_task is not None and not isinstance(current_task, str)):
            raise ToolError("PROTOCOL_INVALID")
        session_id, binding_epoch, process_epoch = self._require_bound()
        display_name = self.identity["session_name"] if self.identity else None
        response = await self._request(
            "POST",
            f"/api/agent/{quote(session_id, safe='')}/status",
            private=True,
            json_body={
                "schema_version": 1,
                "transport": "omp-mcp",
                "kind": "omp-session",
                "channel": "#omp-relay",
                "thread_id": None,
                "status": status,
                "current_task": current_task,
                "last_activity_ts": _now_ms(),
                "display_name": display_name,
                "operation": "refresh",
                "binding_epoch": binding_epoch,
                "omp_process_epoch": process_epoch,
                "request_id": str(uuid4()),
            },
        )
        response = dict(response)
        if "event_id" in response:
            response["event_id"] = _event_id(response["event_id"])
        return response

    async def tool_name(self, relay_name: str) -> dict[str, Any]:
        self._require_model_mutation(allow_unnamed=True)
        if RELAY_NAME_RE.fullmatch(relay_name) is None:
            raise ToolError("PROTOCOL_INVALID")
        session_id, binding_epoch, process_epoch = self._require_bound()
        response = await self._request(
            "PUT",
            f"/api/agent/{quote(session_id, safe='')}/relay-name",
            private=True,
            json_body={
                "schema_version": 1,
                "relay_name": relay_name,
                "takeover": True,
                "binding_epoch": binding_epoch,
                "omp_process_epoch": process_epoch,
            },
        )
        # Not _attributed: the reconcile spawned below would inherit the pin and run under this binding.
        if self._still_bound_to((session_id, binding_epoch, process_epoch)):
            self.relay_name = relay_name
            self.health_state = "connected"
            self.health_code = "CONNECTED"
        self._spawn(self.reconcile_once())
        return response

    async def tool_send(
        self, to_agent: str, content: str, channel: str = "#omp-relay", kind: str = "agent_message"
    ) -> dict[str, Any]:
        self._require_model_mutation()
        if (
            not isinstance(to_agent, str)
            or not 1 <= len(to_agent) <= 256
            or not isinstance(content, str)
            or not isinstance(channel, str)
            or not channel
            or kind not in EVENT_KINDS
            or not self.relay_name
        ):
            raise ToolError("PROTOCOL_INVALID")
        response = await self._request(
            "POST",
            "/api/event",
            json_body={
                "from_agent": self.relay_name,
                "to_agent": to_agent,
                "channel": channel,
                "kind": kind,
                "content": content,
                "ts": _now_ms(),
                "client_event_id": str(uuid4()),
            },
        )
        if "event_id" not in response:
            raise ToolError("PROTOCOL_INVALID")
        result = dict(response)
        result["event_id"] = _event_id(str(result["event_id"]))
        return result

    @_attributed
    async def tool_check(self, after: str = "0", limit: int = 20) -> dict[str, Any]:
        session_id, binding_epoch, _ = self._require_bound()
        _event_id(after, cursor=True)
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ToolError("PROTOCOL_INVALID")
        # The local half of the result is read before the first await, while the binding is the one the
        # broker half is pinned to (_attributed), so a rebind during the requests cannot mix in another's.
        health = self._check_health(binding_epoch)
        ok = self.health_code in {"CONNECTED", "NAME_REQUIRED"}
        last_broker_contact_ms = self.last_broker_contact_ms
        mirror_values = self._read_all_valid_mirrors(current_only=True)
        identity = await self._request("GET", f"/api/agent/{quote(session_id, safe='')}/status")
        page = await self._request(
            "GET",
            f"/api/agent/{quote(session_id, safe='')}/deliveries",
            params={"after": int(after), "limit": limit},
        )
        raw_deliveries = page.get("deliveries")
        if not isinstance(raw_deliveries, list):
            raise ToolError("PROTOCOL_INVALID")
        deliveries: list[dict[str, Any]] = []
        accepted_unacked = 0
        offered_event: str | None = None
        for raw in raw_deliveries:
            projected = dict(raw) if isinstance(raw, dict) else {}
            projected["event_id"] = _event_id(str(projected.get("event_id")))
            projected["recipient_id"] = session_id
            try:
                status = await self._status(projected["event_id"])
            except RelayProtocolError as exc:
                raise ToolError(exc.code) from None
            projected.update(
                offer_token=status["offer_token"],
                offer_generation=status["offer_generation"],
                offer_state=status["offer_state"],
            )
            if offered_event is None and status["offer_token"] is not None:
                offered_event = projected["event_id"]
            if status["offer_state"] == "accepted" and status["delivery_status"] != "acked":
                accepted_unacked += 1
            deliveries.append(projected)
        next_cursor_raw = page.get("next_cursor")
        next_cursor = None if next_cursor_raw is None else _event_id(str(next_cursor_raw), cursor=True)
        result = {
            "schema_version": 1,
            "ok": ok,
            "code": health["code"],
            "identity": {
                "session_id": session_id,
                "relay_name": identity.get("relay_name"),
                "transport": "omp-mcp",
            },
            "health": {
                "state": health["state"],
                "code": health["code"],
                "since_ms": health["since_ms"],
                "last_broker_contact_ms": last_broker_contact_ms,
            },
            "queue": {
                "oldest_event_id": deliveries[0]["event_id"] if deliveries else None,
                "outstanding_count": len(deliveries),
                "offered_event_id": offered_event,
                "accepted_unacked_count": accepted_unacked,
                "rejected_count": sum(1 for item in mirror_values if item["state"] == "rejected"),
            },
            "deliveries": deliveries,
            "next_cursor": next_cursor,
        }
        return result

    def _offer_path(self, event_id: str, session_id: str | None = None) -> Path:
        if session_id is None:
            session_id, _, _ = self._require_bound()
        _event_id(event_id)
        path = self.state_root / "sessions" / session_id / "offers" / f"{event_id}.json"
        expected_parent = self.state_root / "sessions" / session_id / "offers"
        if path.parent != expected_parent:
            raise RelayProtocolError("PROTOCOL_INVALID")
        return path

    def _read_mirror(self, event_id: str, session_id: str | None = None) -> tuple[str, dict[str, Any] | None]:
        if session_id is None:
            session_id, _, _ = self._require_bound()
        path = self._offer_path(event_id, session_id)
        try:
            info = path.lstat()
        except FileNotFoundError:
            return "absent", None
        except OSError:
            return "malformed", None
        if path.is_symlink() or not path.is_file() or getattr(info, "st_file_attributes", 0) & 0x400:
            return "malformed", None
        try:
            raw = path.read_text(encoding="utf-8")
            value = json.loads(raw, object_pairs_hook=_reject_duplicate_pairs)
            mirror = _validate_mirror(value)
        except (OSError, UnicodeError, json.JSONDecodeError, RelayProtocolError):
            return "malformed", None
        if mirror["session_id"] != session_id or mirror["event_id"] != event_id:
            return "malformed", None
        return "valid", mirror

    def _read_all_valid_mirrors(self, *, current_only: bool) -> list[dict[str, Any]]:
        if not self.session_id:
            return []
        directory = self.state_root / "sessions" / self.session_id / "offers"
        try:
            children = tuple(directory.iterdir())
        except FileNotFoundError:
            return []
        except OSError:
            return []
        values: list[dict[str, Any]] = []
        for child in children:
            match = re.fullmatch(r"([1-9][0-9]{0,18})\.json", child.name)
            if not match:
                continue
            event_id = match.group(1)
            try:
                _event_id(event_id)
            except RelayProtocolError:
                continue
            kind, mirror = self._read_mirror(event_id)
            if kind == "valid" and mirror is not None:
                if not current_only or mirror["omp_process_epoch"] == self.omp_process_epoch:
                    values.append(mirror)
        values.sort(key=lambda item: int(item["event_id"]))
        return values

    def _read_malformed_mirror_event_ids(self) -> list[str]:
        if not self.session_id:
            return []
        directory = self.state_root / "sessions" / self.session_id / "offers"
        try:
            children = tuple(directory.iterdir())
        except (FileNotFoundError, OSError):
            return []
        event_ids: list[str] = []
        for child in children:
            match = re.fullmatch(r"([1-9][0-9]{0,18})\.json", child.name)
            if not match:
                continue
            event_id = match.group(1)
            try:
                _event_id(event_id)
            except RelayProtocolError:
                continue
            kind, _ = self._read_mirror(event_id)
            if kind == "malformed":
                event_ids.append(event_id)
        event_ids.sort(key=int)
        return event_ids

    async def _claim_head(self) -> dict[str, Any]:
        session_id, binding_epoch, process_epoch = self._request_binding()
        response = await self._request(
            "POST",
            f"/api/agent/{quote(session_id, safe='')}/delivery-offer",
            private=True,
            json_body={
                "schema_version": 1,
                "binding_epoch": binding_epoch,
                "omp_process_epoch": process_epoch,
            },
        )
        response = _closed(response, {"schema_version", "code", "status", "delivery"})
        if response["schema_version"] != 1 or response["code"] not in {"HEAD", "EMPTY"}:
            raise RelayProtocolError("PROTOCOL_INVALID")
        if response["code"] == "EMPTY":
            if response["status"] is not None or response["delivery"] is not None:
                raise RelayProtocolError("PROTOCOL_INVALID")
            return response
        status = _validate_offer_status(response["status"])
        delivery = _validate_delivery(response["delivery"])
        if status["event_id"] != delivery["event_id"] or status["recipient_id"] != session_id:
            raise RelayProtocolError("PROTOCOL_INVALID")
        return response

    async def _status(self, event_id: str) -> dict[str, Any]:
        session_id, binding_epoch, process_epoch = self._request_binding()
        response = await self._request(
            "GET",
            f"/api/agent/{quote(session_id, safe='')}/delivery/{event_id}/status",
            private=True,
            params={
                "schema_version": "1",
                "binding_epoch": binding_epoch,
                "omp_process_epoch": process_epoch,
            },
        )
        response = _closed(response, {"schema_version", "status"})
        if response["schema_version"] != 1:
            raise RelayProtocolError("PROTOCOL_INVALID")
        return _validate_offer_status(response["status"])

    async def _cas(self, event_id: str, offer_token: str, target: str, reason: str | None = None) -> dict[str, Any]:
        session_id, binding_epoch, process_epoch = self._request_binding()
        response = await self._request(
            "POST",
            f"/api/agent/{quote(session_id, safe='')}/delivery-offer-status",
            private=True,
            json_body={
                "schema_version": 1,
                "event_id": event_id,
                "offer_token": offer_token,
                "binding_epoch": binding_epoch,
                "omp_process_epoch": process_epoch,
                "target_state": target,
                "reason_code": reason,
            },
        )
        response = _closed(response, {"schema_version", "code", "status"})
        if response["schema_version"] != 1 or response["code"] not in {"UPDATED", "UNCHANGED"}:
            raise RelayProtocolError("PROTOCOL_INVALID")
        return _validate_offer_status(response["status"])

    async def _ack(self, event_id: str, offer_token: str) -> dict[str, Any]:
        session_id, binding_epoch, process_epoch = self._request_binding()
        response = await self._request(
            "POST",
            f"/api/agent/{quote(session_id, safe='')}/delivery-ack",
            private=True,
            json_body={
                "schema_version": 1,
                "event_id": event_id,
                "offer_token": offer_token,
                "binding_epoch": binding_epoch,
                "omp_process_epoch": process_epoch,
            },
        )
        response = _closed(response, {"schema_version", "code", "status"})
        if response["schema_version"] != 1 or response["code"] not in {"ACKED", "ALREADY_ACKED"}:
            raise RelayProtocolError("PROTOCOL_INVALID")
        status = _validate_offer_status(response["status"])
        if status["delivery_status"] != "acked" or status["offer_state"] != "accepted":
            raise RelayProtocolError("PROTOCOL_INVALID")
        return status

    def _control_base(self, notification_type: str, status: dict[str, Any]) -> dict[str, Any]:
        # A control names the session and binding of the operation it reports, never the current
        # ones: every status echoes the recipient and binding its request authenticated with
        # (server.py _offer_status), so a rebind while the request was in flight cannot relabel it.
        return {
            "schema_version": 1,
            "type": notification_type,
            "instance_id": self.instance_id,
            "binding_epoch": status["binding_epoch"],
            "recipient_id": status["recipient_id"],
        }

    async def _notify_message(self, status: dict[str, Any], delivery: dict[str, Any]) -> None:
        if status["offer_token"] is None:
            raise RelayProtocolError("PROTOCOL_INVALID")
        await self._notify(
            MESSAGE_METHOD,
            {
                "schema_version": 1,
                "instance_id": self.instance_id,
                "binding_epoch": status["binding_epoch"],
                "recipient_id": status["recipient_id"],
                "offer_token": status["offer_token"],
                "offer_generation": 1,
                "current_relay_name": self.relay_name,
                "delivery": delivery,
                "display_text": render_display(delivery),
            },
        )

    async def _notify_terminal(self, status: dict[str, Any]) -> None:
        if status["offer_token"] is None or status["offer_state"] not in {"accepted", "rejected"}:
            return
        notification_type = "offer_accepted" if status["offer_state"] == "accepted" else "offer_rejected"
        payload = self._control_base(notification_type, status)
        payload.update(event_id=status["event_id"], offer_token=status["offer_token"])
        if notification_type == "offer_rejected":
            payload["reject_reason_code"] = status["reject_reason_code"]
        await self._notify(CONTROL_METHOD, payload)

    async def _notify_identity_acked(self, status: dict[str, Any]) -> None:
        payload = self._control_base("identity_acked", status)
        payload.update(event_id=status["event_id"], offer_token=status["offer_token"])
        await self._notify(CONTROL_METHOD, payload)

    async def _notify_terminal_recovery(self, status: dict[str, Any]) -> None:
        if status["offer_token"] is None or status["offer_state"] not in {"accepted", "rejected"}:
            return
        payload = self._control_base("offer_terminal_recovery", status)
        payload.update(
            event_id=status["event_id"],
            offer_token=status["offer_token"],
            terminal_state=status["offer_state"],
            reject_reason_code=status["reject_reason_code"],
            recovery_source="malformed_mirror",
        )
        await self._notify(CONTROL_METHOD, payload)

    async def _notify_recovery_status(
        self, status: dict[str, Any], delivery: dict[str, Any] | None
    ) -> None:
        binding_epoch, process_epoch = status["binding_epoch"], status["omp_process_epoch"]
        await self._notify(
            CONTROL_METHOD,
            {
                "schema_version": 1,
                "type": "offer_recovery_status",
                "instance_id": self.instance_id,
                "control_epoch": self.control_epoch,
                "binding_epoch": binding_epoch,
                "omp_process_epoch": process_epoch,
                "status": status,
                "delivery": delivery,
            },
        )

    def _hold_event(self, status: dict[str, Any], delivery: dict[str, Any]) -> None:
        held = (status["event_id"], status["offer_token"])
        if held != self.held:
            self._next_delivery_notification = 0.0
        self.held = held
        self._held_delivery = delivery
        self._held_status = status
        self._held_status_due = time.monotonic() + RECONCILE_INTERVAL_SECONDS
        self._held_mirror_observation = None

    def _remember_held_status(self, status: dict[str, Any]) -> None:
        if self.held == (status["event_id"], status["offer_token"]):
            self._held_status = status

    def _clear_held_event(self) -> None:
        self.held = None
        self._held_delivery = None
        self._held_status = None
        self._held_status_due = 0.0
        self._held_mirror_observation = None

    async def _reconcile_held_local(self) -> bool:
        if self.held is None:
            return True
        event_id, offer_token = self.held
        kind, mirror = self._read_mirror(event_id)
        if kind == "absent":
            if self._held_status is not None:
                await self._retry_message_notification(self._held_status, None)
            return True
        if kind == "malformed":
            return True
        assert mirror is not None
        if mirror["offer_token"] != offer_token:
            raise RelayProtocolError("RECEIPT_CONFLICT")
        if mirror["omp_process_epoch"] != self.omp_process_epoch:
            return True
        state = mirror["state"]
        terminal = mirror["broker_terminal_state"]
        if state == "received":
            if self._held_status is not None:
                await self._retry_message_notification(self._held_status, None)
            return True
        if state == "uncertain":
            return True
        if state == "accepted" and terminal == "accepted" and mirror["identity_acknowledged"]:
            self.pending_ack.discard((event_id, offer_token))
            self._clear_held_event()
            return True
        if state == "rejected" and terminal == "rejected":
            self._clear_held_event()
            return True
        observation = (
            state,
            terminal,
            mirror["identity_acknowledged"],
            mirror["application_acceptance_observed"],
            mirror["updated_at_ms"],
        )
        if observation == self._held_mirror_observation:
            return True
        self._held_mirror_observation = observation
        status = self._held_status
        if status is None:
            return True
        if state == "injected":
            if status["offer_state"] == "offered":
                try:
                    status = await self._cas(event_id, offer_token, "injected")
                except RelayProtocolError:
                    return True
                self._remember_held_status(status)
            if status["offer_state"] in {"accepted", "rejected"}:
                await self._notify_terminal(status)
            return True
        if state == "accepted" and terminal == "none":
            if status["offer_state"] in {"offered", "injected"}:
                try:
                    status = await self._cas(event_id, offer_token, "accepted")
                except RelayProtocolError:
                    return True
                self._remember_held_status(status)
            if status["offer_state"] in {"accepted", "rejected"}:
                await self._notify_terminal(status)
            return True
        if (
            state == "accepted"
            and terminal == "accepted"
            and mirror["application_acceptance_observed"]
            and not mirror["identity_acknowledged"]
        ):
            self.pending_ack.add((event_id, offer_token))
            try:
                status = await self._ack(event_id, offer_token)
            except RelayProtocolError:
                return True
            self._remember_held_status(status)
            if status["delivery_status"] == "acked":
                await self._notify_identity_acked(status)
        return True

    async def reconcile_once(self) -> None:
        async with self._reconcile_lock:
            if not self.bound or self._stopping or not self.relay_name:
                return
            # The binding _reconcile_step pins; a failure carries it so the loop reports it only there.
            binding = self._operation_binding()
            try:
                await self._reconcile_step()
            except (RelayProtocolError, ToolError) as exc:
                exc.binding = binding  # type: ignore[attr-defined]
                raise

    @_attributed
    async def _reconcile_step(self) -> None:
        binding = self._operation_binding()
        if self.held is None:
            if await self._reconcile_prior_candidates(None):
                return
            response = await self._claim_head()
            if response["code"] == "EMPTY":
                return
            status = response["status"]
            delivery = response["delivery"]
            assert isinstance(status, dict) and isinstance(delivery, dict)
            if status["offer_token"] is None:
                raise RelayProtocolError("PROTOCOL_INVALID")
            # A head claimed for a binding since replaced is not the current binding's to hold or offer: the
            # broker keeps the offer on its recipient and serves that session's next current claim the same
            # token. An accepted head is still completed below, under the attribution it was claimed with.
            current = self._still_bound_to(binding)
            if current:
                self._hold_event(status, delivery)
                self._reconcile_wake.set()
            if status["offer_state"] in {"offered", "injected"}:
                if current:
                    await self._notify_message(status, delivery)
                    if self._still_bound_to(binding):
                        self._next_delivery_notification = time.monotonic() + 1.0
            elif status["offer_state"] == "accepted":
                await self._reconcile_event(status["event_id"], status["offer_token"], delivery)
            return
        event_id, offer_token = self.held
        if time.monotonic() < self._held_status_due and await self._reconcile_held_local():
            return
        self._held_status_due = time.monotonic() + RECONCILE_INTERVAL_SECONDS
        await self._reconcile_event(event_id, offer_token, None)

    @_attributed
    async def _reconcile_event(
        self, event_id: str, offer_token: str, delivery: dict[str, Any] | None
    ) -> None:
        lock = self._event_locks.setdefault(event_id, asyncio.Lock())
        if lock.locked():
            return
        async with lock:
            status = await self._status(event_id)
            self._remember_held_status(status)
            if status["offer_token"] != offer_token:
                raise RelayProtocolError("RECEIPT_CONFLICT")
            # The answer belongs to the session and omp process its request was made under; a
            # rebind while it was in flight must not make this read another session's mirror.
            session_id, process_epoch = status["recipient_id"], status["omp_process_epoch"]
            kind, mirror = self._read_mirror(event_id, session_id)
            if kind == "absent":
                # A duplicate notification is safe: extension admission is single-flight,
                # and an existing missing entry has no second send right.
                await self._retry_message_notification(status, delivery)
                return
            if kind == "malformed":
                if status["offer_state"] in {"accepted", "rejected"}:
                    await self._notify_terminal_recovery(status)
                return
            assert mirror is not None
            if mirror["offer_token"] != offer_token:
                raise RelayProtocolError("RECEIPT_CONFLICT")
            if mirror["omp_process_epoch"] != process_epoch:
                claim_delivery = delivery
                if claim_delivery is None and status["offer_state"] not in {"accepted", "rejected"}:
                    head = await self._claim_head()
                    if (
                        head["code"] == "HEAD"
                        and head["status"]["event_id"] == event_id
                        and head["status"]["recipient_id"] == session_id
                    ):
                        claim_delivery = head["delivery"]
                await self._notify_recovery_status(status, claim_delivery)
                return
            state = mirror["state"]
            terminal = mirror["broker_terminal_state"]
            if status["offer_state"] == "rejected":
                if not (state == "rejected" and terminal == "rejected"):
                    await self._notify_terminal(status)
                    return
            elif status["offer_state"] == "accepted":
                if state == "accepted" and terminal == "none":
                    await self._notify_terminal(status)
                    return
                if state != "accepted":
                    await self._notify_terminal_recovery(status)
                    return
            elif state == "injected":
                if status["offer_state"] == "offered":
                    status = await self._cas(event_id, offer_token, "injected")
                    self._remember_held_status(status)
                return
            elif state == "received":
                await self._retry_message_notification(status, delivery)
                return
            elif state == "uncertain":
                return
            elif state == "accepted" and terminal == "none":
                try:
                    status = await self._cas(event_id, offer_token, "accepted")
                except RelayProtocolError:
                    status = await self._status(event_id)
                self._remember_held_status(status)
                if status["offer_state"] in {"accepted", "rejected"}:
                    await self._notify_terminal(status)
                return

            # Re-read the exact canonical current mirror before ack or barrier release.
            kind, current = self._read_mirror(event_id, session_id)
            if kind != "valid" or current is None or current["omp_process_epoch"] != process_epoch:
                return
            if (
                current["state"] == "accepted"
                and current["broker_terminal_state"] == "accepted"
                and current["application_acceptance_observed"]
                and not current["identity_acknowledged"]
            ):
                self.pending_ack.add((event_id, offer_token))
                ack_status = status
                if ack_status["delivery_status"] != "acked":
                    try:
                        ack_status = await self._ack(event_id, offer_token)
                    except RelayProtocolError:
                        ack_status = await self._status(event_id)
                        if ack_status["delivery_status"] != "acked":
                            self._remember_held_status(ack_status)
                            return
                self._remember_held_status(ack_status)
                await self._notify_identity_acked(ack_status)
                return
            if current["state"] == "accepted" and current["identity_acknowledged"]:
                self.pending_ack.discard((event_id, offer_token))
                self._clear_held_event()
                return
            if current["state"] == "rejected" and current["broker_terminal_state"] == "rejected":
                self._clear_held_event()

    async def _retry_message_notification(
        self, status: dict[str, Any], delivery: dict[str, Any] | None
    ) -> None:
        if status["offer_state"] not in {"offered", "injected"} or time.monotonic() < self._next_delivery_notification:
            return
        binding = self._operation_binding()
        current_delivery = delivery or self._held_delivery
        if current_delivery is None:
            head = await self._claim_head()
            if (
                head["code"] != "HEAD"
                or head["status"]["event_id"] != status["event_id"]
                or head["status"]["recipient_id"] != status["recipient_id"]
            ):
                return
            current_delivery = head["delivery"]
            if self._still_bound_to(binding):
                self._held_delivery = current_delivery
        await self._notify_message(status, current_delivery)
        if self._still_bound_to(binding):
            self._next_delivery_notification = time.monotonic() + 1.0

    @_attributed
    async def _reconcile_prior_candidates(self, head: dict[str, Any] | None) -> bool:
        # Every mirror the scan reads, and the process epoch it compares them with, is read before the
        # first await, so a rebind during a status read cannot turn it to another binding's mirrors.
        malformed = self._read_malformed_mirror_event_ids()
        mirrors = self._read_all_valid_mirrors(current_only=False)
        process_epoch = self.omp_process_epoch
        for event_id in malformed:
            status = await self._status(event_id)
            if status["offer_state"] in {"accepted", "rejected"}:
                await self._notify_terminal_recovery(status)
                if status["offer_state"] == "accepted" and status["delivery_status"] == "acked":
                    await self._notify_identity_acked(status)
                return True

        for mirror in mirrors:
            if mirror["omp_process_epoch"] == process_epoch:
                if (
                    (mirror["state"] == "rejected" and mirror["broker_terminal_state"] == "rejected")
                    or (
                        mirror["state"] == "accepted"
                        and mirror["broker_terminal_state"] == "accepted"
                        and mirror["identity_acknowledged"]
                    )
                ):
                    continue
                await self._reconcile_event(mirror["event_id"], mirror["offer_token"], None)
                return True
            if (
                (mirror["state"] == "rejected" and mirror["broker_terminal_state"] == "rejected")
                or (
                    mirror["state"] == "accepted"
                    and mirror["broker_terminal_state"] == "accepted"
                    and mirror["application_acceptance_observed"]
                    and mirror["identity_acknowledged"]
                )
            ):
                continue
            event_id = mirror["event_id"]
            status = await self._status(event_id)
            delivery: dict[str, Any] | None = None
            if status["offer_state"] not in {"accepted", "rejected"}:
                candidate: dict[str, Any]
                if head is None:
                    try:
                        candidate = await self._claim_head()
                    except RelayProtocolError:
                        candidate = {"code": "EMPTY"}
                else:
                    candidate = head
                candidate_status = candidate.get("status")
                candidate_delivery = candidate.get("delivery")
                if (
                    candidate.get("code") == "HEAD"
                    and isinstance(candidate_status, dict)
                    and candidate_status.get("event_id") == event_id
                    and candidate_status.get("recipient_id") == status["recipient_id"]
                    and isinstance(candidate_delivery, dict)
                ):
                    delivery = candidate_delivery
            await self._notify_recovery_status(status, delivery)
            return True
        return False

    async def _reconciliation_loop(self) -> None:
        while not self._stopping:
            try:
                await self.reconcile_once()
            except (RelayProtocolError, ToolError) as exc:
                if self._still_bound_to(getattr(exc, "binding", None)):
                    self.health_state = "degraded" if self.bound else "blocked"
                    self.health_code = exc.code if isinstance(exc, RelayProtocolError) else str(exc)
            timeout = 0.1 if self.held is not None else RECONCILE_INTERVAL_SECONDS
            try:
                await asyncio.wait_for(self._reconcile_wake.wait(), timeout=timeout)
            except TimeoutError:
                pass
            self._reconcile_wake.clear()

    async def _sse_loop(self) -> None:
        while not self._stopping:
            try:
                async with httpx.AsyncClient(base_url=self.broker_url, timeout=None) as client:
                    async with client.stream("GET", "/api/events", headers=self._general_headers()) as response:
                        response.raise_for_status()
                        async for line in response.aiter_lines():
                            if self._stopping:
                                return
                            if line.startswith("data:"):
                                self._spawn(self.reconcile_once())
            except (httpx.HTTPError, RelayProtocolError):
                if self._stopping:
                    return
                await asyncio.sleep(1.0)

    async def _control_loop(self) -> None:
        while not self._stopping:
            try:
                await self._process_user_controls()
            except (RelayProtocolError, ToolError) as exc:
                self.health_state = "degraded" if self.bound else "blocked"
                self.health_code = exc.code if isinstance(exc, RelayProtocolError) else str(exc)
            await asyncio.sleep(0.1)

    async def _process_user_controls(self) -> None:
        if not self.bound or self.identity is None:
            return
        directory = self.rendezvous_dir / "control" / "requests"
        try:
            requests = sorted(directory.glob("*.json"))
        except OSError:
            return
        for request_path in requests:
            command_id = request_path.stem
            if command_id in self._bind_attempt_ids:
                continue
            result_path = self.rendezvous_dir / "control" / "results" / request_path.name
            if result_path.exists():
                continue
            action = self._peek_user_control_action(request_path)
            request: dict[str, Any] | None = None
            try:
                request = self._read_user_control(request_path)
                action = request["action"]
                await self._execute_user_control(request)
            except (RelayProtocolError, ToolError) as exc:
                code = exc.code if isinstance(exc, RelayProtocolError) else str(exc)
                attribution = None if request is None else _request_attribution(request)
                await self._write_user_result(
                    command_id, action, False, _user_control_failure_code(code), None, attribution
                )

    def _peek_user_control_action(self, path: Path) -> str:
        try:
            envelope = _read_closed_json(
                path,
                {
                    "schema_version",
                    "command_id",
                    "instance_id",
                    "control_epoch",
                    "binding_epoch",
                    "omp_process_epoch",
                    "session_id",
                    "issued_at_ms",
                    "action",
                    "args",
                    "signature_b64url",
                },
            )
        except RelayProtocolError:
            return "REJECT_DELIVERY"
        return envelope["action"] if envelope.get("action") in {"TAKEOVER_NAME", "REJECT_DELIVERY"} else "REJECT_DELIVERY"

    def _read_user_control(self, path: Path) -> dict[str, Any]:
        envelope = _read_closed_json(
            path,
            {
                "schema_version",
                "command_id",
                "instance_id",
                "control_epoch",
                "binding_epoch",
                "omp_process_epoch",
                "session_id",
                "issued_at_ms",
                "action",
                "args",
                "signature_b64url",
            },
        )
        command_id = _uuid(envelope["command_id"])
        if (
            envelope["schema_version"] != 1
            or envelope["instance_id"] != self.instance_id
            or envelope["control_epoch"] != self.control_epoch
            or envelope["binding_epoch"] != self.binding_epoch
            or envelope["omp_process_epoch"] != self.omp_process_epoch
            or envelope["session_id"] != self.session_id
            or envelope["action"] not in {"TAKEOVER_NAME", "REJECT_DELIVERY"}
        ):
            raise RelayProtocolError("CONTROL_CHANNEL_INVALID")
        age = _now_ms() - _safe_int(envelope["issued_at_ms"])
        if age < 0 or age > APPROVAL_MAX_AGE_MS:
            raise RelayProtocolError("CONTROL_CHANNEL_INVALID")
        if envelope["action"] == "TAKEOVER_NAME":
            args = _closed(envelope["args"], {"name"})
            if not isinstance(args["name"], str) or RELAY_NAME_RE.fullmatch(args["name"]) is None:
                raise RelayProtocolError("PROTOCOL_INVALID")
        else:
            args = _closed(envelope["args"], {"event_id", "offer_token", "reason_code"})
            _event_id(args["event_id"])
            _uuid(args["offer_token"])
            if args["reason_code"] != "USER_REJECTED":
                raise RelayProtocolError("PROTOCOL_INVALID")
        unsigned = dict(envelope)
        del unsigned["signature_b64url"]
        digest = hashlib.sha256(_canonical_json(unsigned)).digest()
        previous = self._command_digests.get(command_id)
        if previous is not None and previous != digest:
            raise RelayProtocolError("ATTEMPT_CONFLICT")
        assert self.identity is not None
        public_key = Ed25519PublicKey.from_public_bytes(
            _b64url_decode(self.identity["control_public_key_b64url"], size=32)
        )
        try:
            public_key.verify(
                _b64url_decode(envelope["signature_b64url"], size=64), _canonical_json(unsigned)
            )
        except InvalidSignature as exc:
            raise RelayProtocolError("CONTROL_CHANNEL_INVALID") from exc
        self._command_digests[command_id] = digest
        return envelope

    async def _execute_user_control(self, request: dict[str, Any]) -> None:
        action = request["action"]
        attribution = _request_attribution(request)
        if action == "TAKEOVER_NAME":
            session_id, binding_epoch, process_epoch = attribution
            response = await self._request(
                "PUT",
                f"/api/agent/{quote(session_id, safe='')}/relay-name",
                private=True,
                json_body={
                    "schema_version": 1,
                    "relay_name": request["args"]["name"],
                    "takeover": True,
                    "binding_epoch": binding_epoch,
                    "omp_process_epoch": process_epoch,
                },
            )
            # The name was claimed for the request's session; a rebind while the request was in
            # flight must not make it the new session's current name.
            if (self.session_id, self.binding_epoch, self.omp_process_epoch) == attribution:
                self.relay_name = response.get("relay_name")
            await self._write_user_result(
                request["command_id"],
                action,
                True,
                "NAME_CLAIMED",
                {
                    "relay_name": response.get("relay_name"),
                    "displaced_id": response.get("displaced_id"),
                },
                attribution,
            )
            return
        event_id = request["args"]["event_id"]
        offer_token = request["args"]["offer_token"]
        status = await self._cas(event_id, offer_token, "rejected", "USER_REJECTED")
        if (
            status["offer_state"] != "rejected"
            or status["delivery_status"] != "rejected"
            or status["reject_reason_code"] != "USER_REJECTED"
        ):
            raise RelayProtocolError("TERMINAL_CONFLICT")
        await self._notify_terminal(status)
        await self._write_user_result(
            request["command_id"],
            action,
            True,
            "REJECTED",
            {
                "event_id": event_id,
                "offer_state": status["offer_state"],
                "delivery_status": status["delivery_status"],
                "reject_reason_code": status["reject_reason_code"],
            },
            attribution,
        )

    async def _write_user_result(
        self,
        command_id: str,
        action: str,
        ok: bool,
        code: str,
        result: dict[str, Any] | None,
        attribution: tuple[str, str, str] | None = None,
    ) -> None:
        # A result reports under the attribution of the signed request it answers; only a request
        # that could not be read falls back to the current binding.
        if attribution is None:
            if not self.bound:
                return
            attribution = (self.session_id, self.binding_epoch, self.omp_process_epoch)
        session_id, binding_epoch, process_epoch = attribution
        if not ok:
            code = _user_control_failure_code(code)
            result = None
        _atomic_write_json(
            self.rendezvous_dir / "control" / "results" / f"{command_id}.json",
            {
                "schema_version": 1,
                "command_id": command_id,
                "instance_id": self.instance_id,
                "control_epoch": self.control_epoch,
                "binding_epoch": binding_epoch,
                "omp_process_epoch": process_epoch,
                "session_id": session_id,
                "action": action,
                "ok": ok,
                "code": code,
                "result": result,
            },
        )

    async def stop(self) -> None:
        self._stopping = True
        for task in tuple(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*tuple(self._tasks), return_exceptions=True)
        self._tasks.clear()
        # Runtime rendezvous is process-instance state; durable state mirrors and
        # ownership/death witnesses deliberately remain under state_root.
        try:
            shutil.rmtree(self.rendezvous_dir)
        except FileNotFoundError:
            pass
        self.control_token = b""


def create_server(runtime: OmpRelayRuntime) -> FastMCP:
    """Create the exact four-tool OMP relay MCP server."""

    server = FastMCP(SERVER_NAME, instructions="OMP relay: register, name, check, and send only.")

    @server.tool(name="register")
    async def register(status: StatusArg, current_task: str | None = None) -> dict[str, Any]:
        """Refresh this bound OMP session's public broker status."""

        return await runtime.tool_register(status, current_task)

    @server.tool(name="name")
    async def name(relay_name: RelayNameArg) -> dict[str, Any]:
        """Claim or rename this session's relay name, taking it over from any previous owner."""

        return await runtime.tool_name(relay_name)

    @server.tool(name="check")
    async def check(after: CursorArg = "0", limit: LimitArg = 20) -> dict[str, Any]:
        """Read projected identity, health, queue, and delivery state without acknowledgement."""

        return await runtime.tool_check(after, limit)

    @server.tool(name="send")
    async def send(
        to_agent: AgentNameArg,
        content: str,
        channel: ChannelArg = "#omp-relay",
        kind: KindArg = "agent_message",
    ) -> dict[str, Any]:
        """Store one directed relay event and return its string event ID."""

        return await runtime.tool_send(to_agent, content, channel, kind)

    # Client notifications deliberately have no MCP request context. Start on the
    # first tools/list request instead: it is the first post-initialize request OMP
    # makes and gives the runtime the exact ServerSession needed for notifications.
    list_tools_handler = server._mcp_server.request_handlers[mcp_types.ListToolsRequest]

    async def list_tools_and_start(request: mcp_types.ListToolsRequest) -> mcp_types.ServerResult:
        runtime.set_server_session(server._mcp_server.request_context.session)
        await runtime.start()
        return await list_tools_handler(request)

    server._mcp_server.request_handlers[mcp_types.ListToolsRequest] = list_tools_and_start
    return server


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--state-root",
        default=os.environ.get("OMP_RELAY_STATE_ROOT"),
        required="OMP_RELAY_STATE_ROOT" not in os.environ,
    )
    parser.add_argument(
        "--runtime-root",
        default=os.environ.get("OMP_RELAY_RUNTIME_ROOT"),
        required="OMP_RELAY_RUNTIME_ROOT" not in os.environ,
    )
    parser.add_argument("--broker-url", default=os.environ.get("BROKER_URL", DEFAULT_BROKER_URL))
    parser.add_argument("--bearer-token", default=os.environ.get("BROKER_BEARER_TOKEN"))
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    runtime = OmpRelayRuntime(
        state_root=Path(args.state_root),
        runtime_root=Path(args.runtime_root),
        broker_url=args.broker_url,
        bearer_token=args.bearer_token,
    )
    server = create_server(runtime)
    try:
        server.run(transport="stdio")
    finally:
        # FastMCP owns the loop.  The process is exiting here, so OS handles and
        # in-memory credentials are gone even if asynchronous cleanup cannot run.
        runtime.control_token = b""
        try:
            shutil.rmtree(runtime.rendezvous_dir)
        except FileNotFoundError:
            pass


if __name__ == "__main__":
    main()
