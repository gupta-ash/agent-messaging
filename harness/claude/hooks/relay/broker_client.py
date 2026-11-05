# -*- coding: utf-8 -*-
"""broker_client.py — HTTP helpers for agent-orchestration broker.

Per contract.md v1.1 § 1 (REST API) + § 2 (SQLite operational contract retry
policy reuse) + § 5 (Delivery semantics) + § 6 (Agent Registration Protocol).
Stdlib only: urllib.request, urllib.error, json, uuid, time, os, pathlib,
logging. No new pip dependencies.

Public functions:
- post_status(agent_id, kind, channel, status, ...) -> int | None       (event_id)
- post_event(from_agent, to_agent, channel, kind, content, ...) -> int | None  (event_id)
- register_project(name, root_path, state_db_path) -> str | None        (project_id; raises ValueError on 4xx)
- refresh_project(name) -> dict | None
- flush_outbox(broker_url=None) -> dict (counts: flushed/dropped_stale/dropped_corrupted/failed)
- health_check(broker_url=None) -> dict | None
- get_agent_messages(agent_id, *, since=0, kind="agent_message", limit=20, broker_url=None) -> dict | None
- get_all_agent_messages(*, since=0, kind="agent_message", limit=20, broker_url=None) -> dict | None  (cross-recipient aggregate; v1.4)
- post_ack(agent_id, event_ids, *, fast_fail=False, broker_url=None) -> dict | None  (WS-B-client #19; mirror local acks to broker delivery ledger; chunks <=500)

Internal helpers:
- _post(endpoint, body, *, idempotency_key=None, raise_on_4xx=False,
        no_outbox_on_failure=False, broker_url=None, attempts=None,
        timeout_s=None) -> dict | None        (full broker response dict)
- _get(endpoint, *, broker_url=None, suppress_auth=False) -> dict | None  (full broker response dict)
- _enqueue_outbox(endpoint, payload, client_event_id, enqueued_ts) -> None
- _outbox_path() -> Path

Failure tolerance: returns None on broker unavailability (5xx / timeout /
ConnectionRefusedError); appends to outbox at
~/.claude/channels/discord/outbox.jsonl (one JSON line per failed POST).
Caller never raises EXCEPT register_project(), which raises ValueError on 4xx
so callers can surface misconfigured project metadata rather than retry it.

Retry policy: exactly five attempts with sleeps at 0/50/100/200/400 ms. The
800ms tuple entry is reserved and never reached by the five-attempt cap.
Exhausted requests are enqueued to the outbox. Caller-error 4xx responses are
neither retried nor enqueued because replaying invalid input cannot succeed.

Outbox TTL: 24h. Stale entries dropped with WARN log on
flush_outbox() invocation. Corrupted entries (JSON decode failure or missing
required fields) are dropped on a separate counter (dropped_corrupted) so
TTL-drop telemetry is not conflated with parser-drop telemetry.

Outbox flush ordering: /api/project/register
entries are flushed FIRST regardless of their enqueue order, because broker's
project_registry must contain the row before any post_status / post_event
arrives that depends on the project being registered. Within the same
endpoint type, enqueue order is preserved.

Idempotency-Key generation: UUID4 generated AT ENQUEUE TIME for the original
event creation. Persisted in the outbox tuple together with payload.
On retry, the SAME key is reused — broker dedupes via idempotency_ledger
(contract.md § 2). Concurrent flushes from multiple worker threads are
naturally safe.

Outbox 1000-line warning (per contract § 5): the module maintains a
process-local line counter `_outbox_line_count` lazy-init on first enqueue
(one O(N) read), incremented per enqueue (O(1)), reset to a fresh count after
flush_outbox() returns. Concurrent processes appending to the same outbox
bypass this counter (their increments are invisible to this process); the
warning surface is best-effort per-process rather than globally accurate.
Documented limitation.

Configuration via environment variables:
- BROKER_BASE_URL (REQUIRED; full URL with scheme+host+port; module raises at import if unset; NO default per the broker URL contract — NON-NEGOTIABLE config-from-file discipline)
- BROKER_BEARER_TOKEN (optional; required when broker bound non-loopback;
  AUTOMATICALLY suppressed for /api/health per contract § 1: "/api/health
  requires NO auth even on non-loopback bind")
- BROKER_OUTBOX_PATH (default '~/.claude/channels/discord/outbox.jsonl')
- BROKER_TIMEOUT_MS (default 5000; per-attempt timeout)

Windows UTF-8 discipline: subprocess entrypoints configure stdout/stderr UTF-8
before emitting user-visible JSON or text.

Schema discipline (callers MUST follow; server enforces; client does NOT
validate locally):

- `content` MUST be a string. The broker server's `message_log.content`
  column is `TEXT NOT NULL` (per contract.md § 2 SQLite Schemas;
  content-opaque principle). If you have structured data, JSON-stringify
  it yourself: `content=json.dumps({"key": "value"}, ensure_ascii=False)`.
  The client does NOT auto-coerce. Live convention (see hooks at
  ~/.claude/hooks/broker/*.py + state_transition.py): always
  `json.dumps(...)` before passing.
  Exception: `kind='error'` content (v1.7.1+) MUST be a JSON-string
  envelope `{type, severity, errors[], context}` — broker validates this
  shape strictly (422 on malformed). See contract.md § 1 v1.7.1.

- `kind` is a CLOSED SET (per contract.md § 1):
    post_event   kind: user_msg | agent_reply | status | handoff
                       | tool_call | error | pipeline_event | permission_reply
                       | agent_message
    post_status  status: idle | queued | running | blocked | done | failed
                 (NOTE: post_status's `kind` parameter is separate from
                 `status`; default 'orchestrator' for primaries OR
                 '<subagent_type>' for subagents — see contract.md
                 § 6 Agent Registration.)

Off-spec values → broker 400/422 → outbox NOT enqueued (4xx is a caller
bug; retrying would not succeed; outbox enqueue would mask the bug).
"""

import ipaddress
import json
import logging
import os
import sqlite3
import sys
import time
import traceback
import uuid
from pathlib import Path
from typing import Optional, Dict, Any
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse, quote, urlencode

log = logging.getLogger("broker_client")

# ---------------------------------------------------------------------------
# Soft-import shared.py from cc-session-relay (multi-project dashboard Layer 1+3)
# ---------------------------------------------------------------------------
# Per the dashboard-relay shared-module contract:
# `from shared import X` MUST be guarded such that an ImportError logs WARN to
# stderr and falls back to None-returning stubs. broker_client may be invoked
# outside a CC session (no shared.py on path); the helpers must still load and
# the per-cwd auto-bootstrap helpers degrade to no-op when shared is absent.
#
# Lazily resolved on first use rather than at import-time so the import-time
# critical path stays untouched (shared.py is heavyweight; pulls msvcrt/fcntl
# transitively via register_session.py callers).
_shared_module: Any = None
_shared_import_attempted: bool = False


def _get_shared_module() -> Any:
    """Resolve and cache the cc-session-relay `shared` module, or None.

    Adds `~/.claude/hooks/relay` to sys.path on first call so the module is
    importable even when broker_client is invoked outside a CC session (e.g.,
    from a peer skill or a one-off `python -c` test). Soft-fails: on any
    ImportError logs ONE WARN line to stderr and caches None for subsequent
    calls (no per-call retry storms). Returns None when shared cannot be
    imported — callers must check before attribute access.
    """
    global _shared_module, _shared_import_attempted
    if _shared_import_attempted:
        return _shared_module
    _shared_import_attempted = True
    try:
        relay_dir = Path.home() / ".claude" / "hooks" / "relay"
        relay_dir_str = str(relay_dir)
        if relay_dir.is_dir() and relay_dir_str not in sys.path:
            sys.path.insert(0, relay_dir_str)
        import shared as _shared  # type: ignore[import-not-found]
        _shared_module = _shared
    except Exception as e:
        sys.stderr.write(
            f"broker_client: shared module unavailable ({type(e).__name__}: {e}); "
            f"display_name lookup will degrade to None for post_status_self/"
            f"post_event_self (broker row still distinguishable via session_id).\n"
        )
        _shared_module = None
    return _shared_module

# Per the broker URL contract (env-var-driven URL discipline; NON-NEGOTIABLE):
# BROKER_BASE_URL is REQUIRED at import time. Refuse to start if unset, empty,
# whitespace-only, missing scheme, missing host, malformed authority, or
# containing control characters. NO code default — make misconfiguration LOUD.
# Strict authority validation. A prior pass used
# `urlparse(...).netloc` which admitted (a) `http://user@` (userinfo only,
# no host); (b) `http://:8787` (no host, just port); (c) `http://host:badport`
# (non-numeric port); (d) embedded whitespace/control chars. Requires:
# parsed.hostname non-empty, parsed.port either None or valid integer (via
# try/except .port), and zero control chars in the canonical URL.
def _validate_broker_base_url(raw: str) -> Optional[str]:
    """Validate BROKER_BASE_URL value. Returns None if valid, else error reason str."""
    if not raw:
        return "value is empty or whitespace-only"
    # reject ANY whitespace (incl ASCII space 0x20 not caught
    # by the < 0x20 check) anywhere in the canonical URL. Real broker URLs have
    # no whitespace; embedded space is a config typo (e.g., 'http://bad host:8787').
    if any(c.isspace() for c in raw):
        return "value contains whitespace characters (incl. embedded ASCII space)"
    if any((ord(c) < 0x20 or ord(c) == 0x7F) for c in raw):
        return "value contains control characters"
    try:
        parsed = urlparse(raw)
    except ValueError as e:
        return f"urlparse rejected the value ({e})"
    if parsed.scheme not in ("http", "https"):
        return f"scheme must be http or https; got {parsed.scheme!r}"
    if not parsed.hostname:
        return "missing host (parsed.hostname is empty); URLs like 'http://user@' or 'http://:8787' are rejected"
    try:
        port = parsed.port  # raises ValueError on non-numeric port (e.g., 'badport')
    except ValueError:
        return "port is not a valid integer (e.g., 'http://host:badport' rejected)"
    # Explicit port REQUIRED per the broker URL contract
    # ("BROKER_BASE_URL ... Full URL including scheme + host + port"). Without
    # this, scheme-default port (80/443) silently degrades broker visibility:
    # _post/_get target wrong port, Connection refused, retry+outbox enqueue.
    # Closes the spec-vs-impl divergence an earlier validation pass surfaced.
    if port is None:
        return "explicit port REQUIRED per the broker URL contract ('Full URL including scheme + host + port'); scheme-default ports (80/443) would silently target wrong port and degrade broker visibility"
    # BROKER_BASE_URL must be a base URL (scheme + authority only),
    # NOT a request URL with path/query/fragment. Naive `BASE.rstrip('/') + endpoint`
    # concat at _post/_get would produce malformed URLs (e.g., base 'http://host/api'
    # + endpoint '/api/event' -> 'http://host/api/api/event' double-prefix; base with
    # '#frag' -> '...8787#frag/api/event' fragment in wrong position). Reject these
    # to fail-loud at import rather than silently mask as 4xx-no-outbox at runtime.
    # NOTE: `/` (trailing slash on bare host) is allowed; rstrip('/') handles it.
    if parsed.path not in ("", "/"):
        return f"must be a base URL; non-empty path not allowed (would produce malformed concat URLs); got path={parsed.path!r}"
    if parsed.params:
        return f"must be a base URL; URL params not allowed; got params={parsed.params!r}"
    if parsed.query:
        return f"must be a base URL; query string not allowed; got query={parsed.query!r}"
    if parsed.fragment:
        return f"must be a base URL; URL fragment not allowed; got fragment={parsed.fragment!r}"
    return None

_broker_base_url_raw = os.environ.get("BROKER_BASE_URL", "").strip()
_broker_base_url_invalid = _validate_broker_base_url(_broker_base_url_raw)
if _broker_base_url_invalid is not None:
    raise RuntimeError(
        f"BROKER_BASE_URL env var INVALID: {_broker_base_url_invalid}; "
        f"got {os.environ.get('BROKER_BASE_URL', '<unset>')!r}. "
        "Must be non-empty, no control chars or whitespace, scheme http(s)://, with valid host AND explicit integer port (per the broker URL contract). "
        "Set it in the ENVIRONMENT of the process that imports this module: the ~/.claude/settings.json env block (CC), or the shell/user environment before launch (Codex, plain terminals). "
        "For a local broker use http://127.0.0.1:8787. "
        "Per the broker URL contract."
    )
# Canonical (stripped + validated). Runtime code MUST use this constant, NOT re-read
# os.environ["BROKER_BASE_URL"] (which would re-admit unstripped values).
_BROKER_BASE_URL: str = _broker_base_url_raw
del _broker_base_url_raw, _broker_base_url_invalid, _validate_broker_base_url

# Pre-validate BROKER_TIMEOUT_MS at
# import-time AND bound to sane max (10 min) AND precompute as float to prevent
# OverflowError at runtime division. A prior pass left `_BROKER_TIMEOUT_MS / 1000.0`
# as runtime ops in _post + _get; theoretically could OverflowError on huge int
# (sys.maxsize+1), violating the fail-soft contract.
_BROKER_TIMEOUT_MAX_MS = 600_000  # 10-min sanity ceiling
_broker_timeout_ms_raw = os.environ.get("BROKER_TIMEOUT_MS")
if _broker_timeout_ms_raw is not None:
    try:
        _BROKER_TIMEOUT_MS: int = int(_broker_timeout_ms_raw)
    except ValueError:
        raise RuntimeError(
            f"BROKER_TIMEOUT_MS env var must be an integer (milliseconds); "
            f"got {_broker_timeout_ms_raw!r}. Unset to use default (5000ms)."
        )
    if _BROKER_TIMEOUT_MS <= 0:
        raise RuntimeError(
            f"BROKER_TIMEOUT_MS must be positive; got {_BROKER_TIMEOUT_MS}."
        )
    if _BROKER_TIMEOUT_MS > _BROKER_TIMEOUT_MAX_MS:
        raise RuntimeError(
            f"BROKER_TIMEOUT_MS must be <= {_BROKER_TIMEOUT_MAX_MS} ({_BROKER_TIMEOUT_MAX_MS//60_000} min ceiling); "
            f"got {_BROKER_TIMEOUT_MS}. Bounding prevents OverflowError at conversion-time."
        )
else:
    _BROKER_TIMEOUT_MS = 5000  # default 5s per-attempt timeout
# Precompute as float at validation time. Runtime sites
# (_post, _get) use _BROKER_TIMEOUT_S directly — no per-call division.
_BROKER_TIMEOUT_S: float = _BROKER_TIMEOUT_MS / 1000.0
del _broker_timeout_ms_raw
DEFAULT_OUTBOX = Path.home() / ".claude" / "channels" / "discord" / "outbox.jsonl"
DEFAULT_TIMEOUT_MS = 5000
TTL_MS = 86_400_000  # 24h per the broker contract
# Five attempts total: one initial call and four backoffs. The 800ms tuple entry
# is reserved; extending the loop to a sixth attempt changes the retry contract.
RETRY_BACKOFF_MS = (50, 100, 200, 400, 800)  # NOTE: only first 4 are used at 5-attempt cap

# Process-local outbox line counter (lazy-init; reset on flush).
# -1 sentinel = uninitialized; first _enqueue_outbox call computes the real count.
_outbox_line_count: int = -1


# ---------------------------------------------------------------------------
# Public: post_status
# ---------------------------------------------------------------------------

def post_status(
    agent_id: str,
    kind: str,
    channel: str,
    status: str,  # one of: idle|queued|running|blocked|done|failed (per contract § 2 enum)
    current_task: Optional[str] = None,
    capabilities: Optional[list[str]] = None,
    thread_id: Optional[str] = None,
    last_activity_ts: Optional[int] = None,  # epoch ms; default = now
    client_event_id: Optional[str] = None,  # UUID4; default = generate
    broker_url: Optional[str] = None,
    *,
    display_name: Optional[str] = None,  # v1.3.1; broker stores in agent_status.display_name (full-replace)
) -> Optional[int]:
    """POST /api/agent/{id}/status per contract.md § 1.

    Schema discipline: `status` MUST be one of the closed set
    `idle|queued|running|blocked|done|failed`. `kind` defaults to
    'orchestrator' for primaries OR '<subagent_type>' for subagents.
    See module-level "Schema discipline" docstring section.

    Returns event_id (int) on success; None on broker unavailability (request
    enqueued to outbox in that case).

    `display_name` (kw-only, default None) is conditionally included in the body
    when non-None per v1.3.1 backward-compat semantics. Existing callers (no kwargs
    passed) are unaffected.
    """
    body: Dict[str, Any] = {
        "kind": kind,
        "channel": channel,
        "status": status,
        "last_activity_ts": last_activity_ts if last_activity_ts is not None else int(time.time() * 1000),
    }
    if current_task is not None:
        body["current_task"] = current_task
    if capabilities is not None:
        body["capabilities"] = capabilities
    if thread_id is not None:
        body["thread_id"] = thread_id
    if display_name is not None:
        body["display_name"] = display_name
    resp = _post(
        endpoint=f"/api/agent/{agent_id}/status",
        body=body,
        idempotency_key=client_event_id,
        broker_url=broker_url,
    )
    if resp is None:
        return None
    return resp.get("event_id")  # extract event_id from full dict, no 0-sentinel


# ---------------------------------------------------------------------------
# Public: post_event
# ---------------------------------------------------------------------------

def post_event(
    from_agent: str,
    to_agent: Optional[str],  # None for broadcast / system-originated
    channel: str,
    kind: str,  # one of: user_msg|agent_reply|status|handoff|tool_call|error|pipeline_event (per contract § 2)
    content: str,
    thread_id: Optional[str] = None,
    ts: Optional[int] = None,  # epoch ms; default = now
    client_event_id: Optional[str] = None,  # UUID4; default = generate
    broker_url: Optional[str] = None,
    *,
    display_name: Optional[str] = None,  # v1.3.1; cosmetic; broker may pass through to message_log payload
) -> Optional[int]:
    """POST /api/event per contract.md § 1.

    Schema discipline: `content` MUST be a string (`json.dumps(...)` any
    dict first; broker `message_log.content` is TEXT NOT NULL). `kind` MUST
    be one of the closed set
    `user_msg|agent_reply|status|handoff|tool_call|error|pipeline_event|permission_reply|agent_message`.
    See module-level "Schema discipline" docstring section for full rules.

    Returns event_id (int) on success; None on broker unavailability (request
    enqueued to outbox in that case).

    `display_name` (kw-only, default None) is conditionally included in body
    when non-None. Existing callers unaffected.
    """
    body: Dict[str, Any] = {
        "from_agent": from_agent,
        "to_agent": to_agent,
        "channel": channel,
        "kind": kind,
        "content": content,
        "ts": ts if ts is not None else int(time.time() * 1000),
    }
    if thread_id is not None:
        body["thread_id"] = thread_id
    if display_name is not None:
        body["display_name"] = display_name
    resp = _post(
        endpoint="/api/event",
        body=body,
        idempotency_key=client_event_id,
        broker_url=broker_url,
    )
    if resp is None:
        return None
    return resp.get("event_id")  # extract event_id from full dict


# ---------------------------------------------------------------------------
# Public: register_project
# ---------------------------------------------------------------------------

def register_project(
    name: str,
    root_path: str,  # absolute path to project repo
    state_db_path: str,  # absolute path to projects/{name}/state.db (Windows-shaped OR POSIX)
    local_validation: Optional[bool] = None,  # None=auto-detect from broker host; True=force local; False=force remote
    broker_url: Optional[str] = None,
) -> Optional[str]:
    """POST /api/project/register per contract.md § 1 (v1.2).

    Broker validates: state_db_path is absolute (Windows-shaped or POSIX,
    per contract v1.2). When `local_validation=True` (or omitted, broker
    default): broker also checks file exists + contains required tables
    (pipelines, pipeline_steps, pipeline_dependencies). When
    `local_validation=False`: broker records project_registry row and trusts
    caller's metadata; pipelines_view rows are then driven entirely via
    subsequent post_event(kind="pipeline_event") and post_status(...) calls.

    Returns project_id (str) on success; None on broker unavailability
    (request enqueued to outbox); RAISES ValueError on broker-validation
    failure (4xx; fix the inputs).

    `local_validation` auto-detect: when caller leaves it None, this helper
    inspects the resolved broker URL (broker_url override OR
    BROKER_BASE_URL env). If the host is loopback (`localhost`, `127.0.0.1`,
    `::1`, or any `127.x.x.x`): omit the field (broker default
    local_validation=True applies). For a non-loopback broker, send
    `local_validation=False`: a remote broker cannot open this machine's
    state.db. Callers can override explicitly with True or False.

    This is the only public helper that opts in to raise on 4xx; callers should
    handle ValueError separately from broker unavailability.

    Idempotent on `name`: re-POSTing with same name re-validates and refreshes
    last_refresh_ts.
    """
    body: Dict[str, Any] = {"name": name, "root_path": root_path, "state_db_path": state_db_path}

    # Auto-detect cross-machine per the cross-machine broker contract.
    # Loopback host = local broker can open
    # the file → omit field, broker default True applies. Non-loopback host =
    # remote broker can't open the file → send False, broker records metadata
    # and trusts caller. Such metadata-only projects do not populate pipelines_view;
    # that projection is rebuilt from state.db by the broker's refresh operation.
    # Caller can override with explicit True/False.
    if local_validation is None:
        target_url = broker_url or _BROKER_BASE_URL  # Must match the _post/_get route source.
        host = (urlparse(target_url).hostname or "").lower()
        # Loopback detection: `localhost` is a hostname (not IP literal); IP
        # literals use ipaddress.is_loopback (full 127.0.0.0/8 + ::1) plus an
        # explicit ipv4_mapped fold for ::ffff:127.x IPv4-mapped IPv6 — older
        # Python versions (including this workstation's 3.12.3) do NOT fold
        # ipv4_mapped into IPv6Address.is_loopback; later 3.12.x patch levels
        # and 3.13 added the fold via CPython issue #117566. The explicit
        # ipv4_mapped guard below works across all runtimes. Hostnames like
        # `127.example.com` correctly fail ipaddress.ip_address() parsing and
        # stay non-loopback (fixes the prior `startswith("127.")` false-positive).
        is_loopback = host == "localhost"
        if host and not is_loopback:
            try:
                addr = ipaddress.ip_address(host)
                mapped = getattr(addr, "ipv4_mapped", None)
                is_loopback = addr.is_loopback or (mapped is not None and mapped.is_loopback)
            except ValueError:
                pass  # non-IP hostname (not localhost) → non-loopback by default
        if not is_loopback and host:
            local_validation = False

    if local_validation is not None:
        body["local_validation"] = local_validation

    resp = _post(
        endpoint="/api/project/register",
        body=body,
        raise_on_4xx=True,  # register_project DOES raise on caller-error per docstring
        broker_url=broker_url,
    )
    if resp is None:
        return None
    return resp.get("project_id")


# ---------------------------------------------------------------------------
# Public: refresh_project
# ---------------------------------------------------------------------------

def refresh_project(name: str, broker_url: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """POST /api/project/{name}/refresh per contract.md § 1.

    Force re-materialization of pipelines_view for a project (bypasses
    file-watcher debounce). Returns the full broker response dict on success
    ({refreshed_at, pipeline_count}); None on broker unavailability (NOT
    enqueued — refresh is a hint, not a must-deliver event; broker auto-
    refreshes on next mtime change anyway).
    """
    return _post(
        endpoint=f"/api/project/{name}/refresh",
        body={},
        no_outbox_on_failure=True,  # refresh is a hint; no outbox enqueue
        broker_url=broker_url,
    )


# ---------------------------------------------------------------------------
# Public: health_check
# ---------------------------------------------------------------------------

def health_check(broker_url: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """GET /api/health per contract.md § 1.

    Returns the full broker response dict on success
    ({status, checks: {sqlite, last_event_ts, ...}}); None on broker
    unavailability.

    NO auth required even on non-loopback bind (per contract § 1) — _get()
    invocation suppresses the Bearer header explicitly via suppress_auth=True
    regardless of BROKER_BEARER_TOKEN env var presence.
    NO retry; NO outbox (a health check that can't reach the broker IS the
    information we want to surface — no point queuing).
    """
    return _get(
        endpoint="/api/health",
        broker_url=broker_url,
        suppress_auth=True,  # contract § 1: /api/health never requires auth
    )


# ---------------------------------------------------------------------------
# Public: get_agent_messages
# ---------------------------------------------------------------------------

def get_agent_messages(
    agent_id: str,
    *,
    since: int = 0,
    kind: str = "agent_message",
    limit: int = 20,
    broker_url: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """GET /api/agent/{id}/messages per peer broker v1.3+ contract § 1.

    Reads queued messages addressed to `agent_id` from the broker's
    message_log, optionally filtered by `kind` and bounded by `limit`, with
    `since` acting as a per-consumer cursor (return only event_id > since).

    Return shape on success: {"messages": [...], "next_cursor": <int>|None}.
    Returns None on 4xx, 5xx, or broker-unreachable per `_get()` discipline
    (no retry, no outbox enqueue — this is a read query, not a must-deliver
    event; queuing it would mask broker outage from the caller).

    Caller-side validation per peer's hardening: `since` and `limit` MUST be
    non-negative integers; broker rejects out-of-range values with HTTP 422
    (per integer-coercion contract). This helper coerces both via `int(...)` to align with the
    sibling `fetch_pending_messages` discipline in cc-session-relay shared.py.

    Caveat (per the int-coercion contract): passing `since=None` or `limit=None`
    raises `TypeError` from `int(None)` BEFORE the request is built. Callers
    MUST pass int values (or rely on the int defaults `since=0`/`limit=20`); do
    NOT pass None expecting fall-through to defaults. Validate at the call site
    if accepting user-supplied values.

    `agent_id` is URL-encoded via `urllib.parse.quote(..., safe="")` so that
    callers passing slugs containing reserved characters (e.g., `coder@team`,
    `cc-session-<uuid>`) do not break the path. Query string built via
    `urllib.parse.urlencode` — no f-string concatenation of untrusted values.

    Per the broker contract: this endpoint requires Bearer auth when broker is
    bound non-loopback; `_get()` adds the header automatically from
    BROKER_BEARER_TOKEN env (suppress_auth=False here — auth IS required for
    /api/agent/{id}/messages, unlike /api/health).

    `agent_id` MUST be a slug-shaped identifier (UUID, alphanumeric+hyphens).
    Path-traversal segments ('.', '..', '/', '\\') are rejected with ValueError
    because urllib.parse.quote does NOT encode '.', so an unrejected '..' would
    produce '/api/agent/../messages' which the broker may normalize to an
    unexpected route. CC session UUIDs always satisfy the slug shape;
    validation is defense-in-depth for future debug-skill / kanban callers
    that take user-supplied input. """
    # reject path-traversal agent_id BEFORE
    # quote() encodes it. quote(safe="") encodes /, ?, #, control chars but
    # NOT '.' — so '..' passes through unchanged, producing /api/agent/../messages.
    if not isinstance(agent_id, str) or not agent_id:
        raise ValueError(f"agent_id must be a non-empty string; got {type(agent_id).__name__}={agent_id!r}")
    if agent_id in (".", "..") or "/" in agent_id or "\\" in agent_id:
        raise ValueError(f"agent_id is path-traversal-shaped; reject '..', '.', '/', '\\'; got {agent_id!r}")
    encoded_id = quote(agent_id, safe="")
    qs = urlencode({"since": int(since), "kind": kind, "limit": int(limit)})
    return _get(
        endpoint=f"/api/agent/{encoded_id}/messages?{qs}",
        broker_url=broker_url,
    )


# ---------------------------------------------------------------------------
# Public: get_all_agent_messages (v1.4)
# ---------------------------------------------------------------------------

def get_all_agent_messages(
    *,
    since: int = 0,
    kind: str = "agent_message",
    limit: int = 20,
    broker_url: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """GET /api/agent-messages — cross-recipient aggregate inbox.

    Sibling to get_agent_messages(agent_id, ...). Aggregates agent_message
    rows across all recipients in event_id ASC order. Same return shape:
    {"messages": [...], "next_cursor": int|None}. Returns None on 4xx/5xx
    /unreachable per _get() discipline (silent-degrade). Per contract.md
    v1.4 § 1.

    Args:
      since: cursor (event_id strictly greater than this); default 0 returns
        from start. MUST be int; passing None raises TypeError from int(None)
        before the request is built (mirrors get_agent_messages caveat).
      kind: event kind filter; default 'agent_message'.
      limit: page size; default 20, cap 200 server-side (broker enforces 422).
        MUST be int; same None caveat as `since`.
      broker_url: override for BROKER_BASE_URL env (testing/multi-broker).
    """
    qs = urlencode({"since": int(since), "kind": kind, "limit": int(limit)})
    return _get(
        endpoint=f"/api/agent-messages?{qs}",
        broker_url=broker_url,
    )


# ---------------------------------------------------------------------------
# Public: post_ack (WS-B-client / task #19)
# ---------------------------------------------------------------------------

def post_ack(
    agent_id: str,
    event_ids: list,
    *,
    floor: Optional[int] = None,
    fast_fail: bool = False,
    broker_url: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """POST recipient acks to the broker delivery ledger (WS-B-client / task #19).

    Mirrors this recipient's LOCAL confirmed-set to the broker so its `delivery`
    shadow-rows flip 'outstanding' -> 'acked' and the escalation reaper (task #18)
    can tell a processed message from an un-processed one. Keyed by `agent_id` =
    the recipient's CURRENT friendly name. (Residual: a message addressed to an OLD
    name before a mid-flight rename won't match its old-name row — the #18 reaper
    reconciles rows whose to_agent no longer resolves; NOT a silent loss.)
    Idempotent broker-side (POST /api/agent/{name}/ack, WHERE status!='acked').

    WS-B-broker-2 (#18 mirror fix): optional `floor` mirrors the recipient's
    gap-aware confirmed-set high-water. The broker crosses off every OUTSTANDING
    row for this recipient with event_id <= floor (in ADDITION to `event_ids`).
    Safe because `floor` advances only over a CONTIGUOUS confirmed run (never past
    an unread id — see the receive-hook _collapse_confirmed), so acking <= floor
    can never cross off a gap message. Attached to the FIRST chunk only (idempotent
    broker-side). A floor-only call (empty `event_ids`) sends a single
    {event_ids: [], floor} post. An invalid floor is dropped defensively (a bad
    floor must never black-hole a legit event_ids ack).

    Chunks to <= 500 ids (broker AckBody.max_length): a >500 batch would 422, and
    _post DROPS 4xx WITHOUT enqueuing to the outbox (a retry can't fix a 4xx) =>
    silent loss. Chunking is therefore mandatory, not an optimization.

    fast_fail=True => 1 attempt + 1s timeout, for the receive-HOOK call site (on
    the turn's critical path): a down LOCAL broker refuses instantly, a wedged one
    caps at ~1s; either way a miss still enqueues to the outbox and replays on the
    next MCP SSE reconnect (flush_outbox). The async MCP broker_ack call site uses
    the default (5-attempt) path — it runs on a worker thread, so latency is moot.

    Best-effort: the LOCAL confirmed-set is the delivery-completeness authority;
    this only mirrors it for escalation. Callers ignore the return. Returns the
    last chunk's broker dict on success, else None (failed / outboxed / no ids).
    """
    if not isinstance(agent_id, str) or not agent_id:
        raise ValueError(f"agent_id must be a non-empty string; got {agent_id!r}")
    if agent_id in (".", "..") or "/" in agent_id or "\\" in agent_id:
        raise ValueError(f"agent_id is path-traversal-shaped; reject '..','.','/','\\'; got {agent_id!r}")
    # ints only, positive, IN-i64-RANGE, deduped, sorted (bool is an int subclass —
    # exclude it). The upper bound mirrors the server AckBody + the floor guard below:
    # an out-of-i64 id survives Python but 422s the WHOLE body server-side, and
    # outbox_on_4xx would then replay that permanently-failing chunk forever, trapping
    # the valid ids in it (cross-family iter-2 MEDIUM — the twin of the floor bug). Drop it.
    ids = sorted({int(e) for e in event_ids
                  if isinstance(e, int) and not isinstance(e, bool)
                  and 0 < e <= 9223372036854775807})
    # Bound floor to the broker's accepted range [1, SQLite i64 max]; an out-of-range
    # or non-int floor is DROPPED (bool is an int subclass). Must never fail/black-hole
    # a legit event_ids ack: an out-of-i64 floor would 422 the WHOLE body server-side,
    # and outbox_on_4xx would then replay that permanently-failing payload forever with
    # the valid ids trapped inside it (cross-family iter-1 MEDIUM). Upper-bound here to
    # match server AckBody so a bad floor is simply dropped, not shipped.
    if floor is not None and (not isinstance(floor, int) or isinstance(floor, bool)
                              or floor < 1 or floor > 9223372036854775807):
        floor = None
    if not ids and floor is None:
        return None
    enc = quote(agent_id, safe="")
    kw = {"attempts": 1, "timeout_s": 1.0, "skip_outbox_count": True} if fast_fail else {}
    if not ids:
        # Floor-only mirror: one post, no chunk loop. A failure is outboxed WITH the floor
        # by _post (outbox_on_4xx) and replays on reconnect — INCLUDING an OLD-broker
        # deploy-skew 422 (the floor-only body 422s an old broker's event_ids min_length=1),
        # which flush_outbox now RETAINS (not drops) so it self-heals once the broker
        # upgrades to floor-awareness.
        return _post(
            f"/api/agent/{enc}/ack",
            {"event_ids": [], "floor": floor},
            broker_url=broker_url,
            outbox_on_4xx=True,
            **kw,
        )
    last: Optional[Dict[str, Any]] = None
    n = len(ids)
    for idx, i in enumerate(range(0, n, 500)):
        chunk_body: Dict[str, Any] = {"event_ids": ids[i:i + 500]}
        if idx == 0 and floor is not None:
            # Attach the floor to the FIRST chunk only (idempotent broker-side; a
            # chunk-0 failure outboxes WITH the floor so it replays on reconnect).
            chunk_body["floor"] = floor
        last = _post(
            f"/api/agent/{enc}/ack",
            chunk_body,
            broker_url=broker_url,
            outbox_on_4xx=True,   # F1: a 404 (broker predates /ack) / deploy-skew must
                                  # outbox + replay, NOT silently drop the mirror.
            **kw,
        )
        if fast_fail and last is None and i + 500 < n:
            # This chunk failed. With outbox_on_4xx=True (above) _post has ALREADY
            # enqueued THIS chunk on ANY failure class — a 4xx (404/405 old-broker /
            # deploy-skew) OR network-unreachable/wedged. Don't pay another timeout
            # per remaining chunk on the turn critical path — outbox the rest directly
            # (<=500 ids each, loss-free), replayed on the MCP SSE reconnect flush.
            # Bounds the hook to ~one chunk of HTTP. (floor already rode chunk 0.)
            for j in range(i + 500, n, 500):
                _enqueue_outbox(
                    f"/api/agent/{enc}/ack",
                    {"event_ids": ids[j:j + 500]},
                    str(uuid.uuid4()),
                    int(time.time() * 1000),
                    update_count=False,
                )
            break
    return last


# ---------------------------------------------------------------------------
# Public: flush_outbox
# ---------------------------------------------------------------------------

def flush_outbox(broker_url: Optional[str] = None, *, max_entries: Optional[int] = None,
                 attempts: Optional[int] = None, timeout_s: Optional[float] = None) -> Dict[str, int]:
    """Flush pending outbox entries to broker. Returns counts dict.

    Bounded mode (hot-path callers — the receive hook + send.py): `max_entries`
    caps how many entries are replayed THIS call; the remainder are re-queued
    verbatim (FIFO preserved) so a large backlog drains across several calls
    instead of stalling one turn. `attempts` / `timeout_s` are forwarded to
    `_post` so each replay is a bounded single-shot (e.g. attempts=1,
    timeout_s=1.0). All three default None -> the original UNBOUNDED full-drain
    behaviour, used by the off-hot-path MCP-reconnect background flusher.

    Reads ~/.claude/channels/discord/outbox.jsonl line-by-line; for each entry:
    - Check 24h TTL: if now_ms - enqueued_ts > 86_400_000, drop with
      WARN log + increment dropped_stale.
    - JSON decode failure or missing required fields: drop + increment
      dropped_corrupted (separate counter from dropped_stale).
    - Else attempt POST with the entry's PERSISTED client_event_id.
      Broker dedupes via idempotency_ledger; safe to retry.
    - On success: increment flushed.
    - On failure (5xx / timeout / connection-refused): increment failed; line
      is preserved (rewritten to outbox file at end).

    Flush ordering: /api/project/register entries
    are processed FIRST regardless of enqueue order. Within the same endpoint
    type, enqueue order is preserved. Rationale: broker's project_registry row
    must exist before any post_status / post_event referencing that project
    lands.

    Atomically rewrites outbox.jsonl with only the failed-this-cycle lines via
    tempfile + os.replace pattern (per Q1c — same atomic-rename discipline
    applies to ANY persistent file write, not just state.db).

    Returns {"flushed": N, "dropped_stale": M, "dropped_corrupted": K, "failed": L}.
    """
    global _outbox_line_count
    counts = {"flushed": 0, "dropped_stale": 0, "dropped_corrupted": 0, "failed": 0}
    outbox_path = _outbox_path()
    # Orphan-recovery runs HERE, not at module import (#18/F2): only flushers pay it
    # (the long-lived MCP server flushes on every SSE reconnect), so the per-fire
    # receive hook — which imports this module but never flushes — no longer pays a
    # per-fire glob/fsync sweep on the turn critical path. Recovery may re-create
    # outbox.jsonl by merging an orphan, so it PRECEDES the exists() check below.
    _recover_orphan_processing_outboxes()
    if not outbox_path.exists():
        _outbox_line_count = 0
        return counts

    # Concurrent-append safety: ROTATE-then-process pattern.
    # Previous in-place rewrite would overwrite any line another process appended
    # AFTER our read but BEFORE our os.replace, silently dropping it. New pattern:
    # (1) atomically rename outbox.jsonl -> outbox.jsonl.processing-<uuid> so any
    #     concurrent enqueue creates a fresh outbox.jsonl with NEW lines we never
    #     touch; (2) process the rotated file in isolation; (3) APPEND survivors
    #     back to the live outbox.jsonl (which may have grown via concurrent
    #     enqueues — appending preserves them).
    rotation_uuid = str(uuid.uuid4())
    # Construct rotation path via with_name (not with_suffix) so the resulting
    # filename matches the recovery glob `outbox_path.name + ".processing-*"`
    # in _recover_orphan_processing_outboxes(). with_suffix REPLACES the suffix,
    # which silently rewrites non-.jsonl override outbox names (e.g., foo.txt
    # would become foo.jsonl.processing-<uuid> and never get recovered). with_name
    # appends so both sites share the same `<outbox-name>.processing-<uuid>` shape.
    processing_path = outbox_path.with_name(f"{outbox_path.name}.processing-{rotation_uuid}")
    try:
        os.replace(outbox_path, processing_path)
        # Reset mtime to NOW so _recover_orphan_processing_outboxes() age
        # filter (60s) correctly treats this freshly-rotated file as in-flight,
        # not as a stale orphan. os.replace inherits the source file's mtime —
        # if the outbox has had no recent appends, the rotated path's mtime
        # could be hours/days old, which would defeat the orphan-recovery
        # sweep's age discriminator. Per the orphan-recovery design (companion to
        # the _recover_orphan_processing_outboxes helper below).
        processing_path.touch()
    except FileNotFoundError:
        # Race: another process flushed concurrently and rotated already.
        _outbox_line_count = 0
        return counts

    now_ms = int(time.time() * 1000)
    raw_lines = []  # list[tuple[str, dict]]: (raw_line, parsed_entry); only valid entries land here

    with processing_path.open("r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.rstrip("\n")
            if not line:
                continue
            try:
                entry = json.loads(line)
                # validate required fields per outbox contract:
                if not isinstance(entry, dict) or not all(
                    k in entry for k in ("client_event_id", "endpoint", "payload", "enqueued_ts")
                ):
                    raise ValueError("missing required outbox fields")
    # Tighten type validation. Without these, malformed entries
    # (non-int enqueued_ts, unhashable endpoint, dict payload swapped to string)
    # escape this block but raise during sort (PRIORITY.get key) or age calc
    # (now_ms - entry["enqueued_ts"]), aborting the WHOLE flush AFTER rotation
    # and orphaning the .processing-<uuid> file. Drop malformed entries into
    # dropped_corrupted bucket HERE, before rotation-state hazard. (Crash-mid-
    # flush orphan-recovery sweep is a separate hazard; deferred.)
                if not isinstance(entry["client_event_id"], str):
                    raise ValueError(f"client_event_id must be str; got {type(entry['client_event_id']).__name__}")
                if not isinstance(entry["endpoint"], str):
                    raise ValueError(f"endpoint must be str; got {type(entry['endpoint']).__name__}")
                if not isinstance(entry["payload"], dict):
                    raise ValueError(f"payload must be dict; got {type(entry['payload']).__name__}")
                # bool is int subclass; reject bool explicitly to avoid True/False as timestamp.
                if not isinstance(entry["enqueued_ts"], int) or isinstance(entry["enqueued_ts"], bool):
                    raise ValueError(f"enqueued_ts must be int (epoch ms); got {type(entry['enqueued_ts']).__name__}")
            except (json.JSONDecodeError, ValueError) as e:
                # separate counter for corrupted (vs stale-by-TTL).
                log.error("outbox: corrupted line dropped (%s): %s", e, line[:100])
                counts["dropped_corrupted"] += 1
                continue
            raw_lines.append((line, entry))

    # Per the flush-ordering contract: /api/project/register flushes FIRST.
    # Stable sort preserves enqueue order WITHIN each endpoint type.
    PRIORITY = {"/api/project/register": 0}
    raw_lines.sort(key=lambda pair: PRIORITY.get(pair[1]["endpoint"], 1))

    # Bounded mode: replay at most `max_entries` this call; the rest are re-queued
    # verbatim (priority/enqueue order already applied above, so FIFO is preserved)
    # so a backlog drains across turns instead of stalling one hot-path call.
    deferred = []  # list[str]
    if max_entries is not None and max_entries >= 0 and len(raw_lines) > max_entries:
        deferred = [pair[0] for pair in raw_lines[max_entries:]]
        raw_lines = raw_lines[:max_entries]

    survivors = []  # list[str]
    for line, entry in raw_lines:
        # 24h TTL drop BEFORE retry attempt
        age_ms = now_ms - entry["enqueued_ts"]
        if age_ms > TTL_MS:
            log.warning(
                "outbox: dropping stale entry %s (age=%dh, endpoint=%s) "
                "per the broker contract 24h TTL",
                entry["client_event_id"],
                age_ms // 3_600_000,
                entry["endpoint"],
            )
            counts["dropped_stale"] += 1
            continue

        # reuse the PERSISTED client_event_id verbatim. raise_on_4xx=True lets replay
        # distinguish a 4xx from a 5xx/network failure: 5xx/network is retryable
        # (survivors); a 4xx is permanent (drop) — EXCEPT a transient deploy-skew
        # 404/405 on the /ack endpoint, which IS retained (see the except handler).
        # Without raise_on_4xx, _post returns None for BOTH 4xx and 5xx-exhausted,
        # collapsing the distinction during replay.
        try:
            resp = _post(
                endpoint=entry["endpoint"],
                body=entry["payload"],
                idempotency_key=entry["client_event_id"],
                broker_url=broker_url,
                no_outbox_on_failure=True,  # we ARE the flush; do not re-enqueue
                raise_on_4xx=True,  # distinguish 4xx (drop) from 5xx/network (retain) on replay
                attempts=attempts,  # bounded-mode: single-shot on the hot path (None = default retries)
                timeout_s=timeout_s,  # bounded-mode: short per-POST timeout (None = default)
            )
            if resp is not None:
                counts["flushed"] += 1
            else:
                counts["failed"] += 1
                survivors.append(line)
        except ValueError as ve:
            # 4xx on replay. A 404/405 on the /ack endpoint is a TRANSIENT deploy-skew
            # (the broker predates POST /api/agent/{name}/ack, or a rolling client/
            # server version skew) — retryable once the broker is upgraded, so RETAIN
            # it (survivors) and let the 24h TTL above bound it (#18/F1 replay half).
            # Every other 4xx (400/422/... = malformed body / unknown route) is
            # permanent -> drop as corrupted.
            code = getattr(ve, "status_code", None)
            ep = entry["endpoint"]
            # Predicate assumes the canonical ack-endpoint shape post_ack emits
            # (/api/agent/{quoted-name}/ack — no trailing slash / query string). If a
            # future producer emits a different /ack shape, widen this match.
            is_ack = ep.startswith("/api/agent/") and ep.endswith("/ack")
            # A floor-bearing /ack body that 422s is ALSO transient deploy-skew: an OLD
            # broker (has /ack from WS-B-broker-1 but predates the `floor` field) 422s a
            # floor-only {event_ids:[], floor} body on its `event_ids` min_length=1 (and/or
            # a forbid-extra floor field). It self-heals once the broker upgrades to
            # floor-awareness. A floor-bearing body can NOT 422 on a CURRENT broker (post_ack
            # bounds floor+ids to i64, and floor present => not empty-both), so a 422 HERE
            # ⟹ skew ⟹ RETAIN (24h-TTL-bounded), matching the 404/405 branch. Without this
            # the floor-only ack was dropped_corrupted on first flush during the skew window
            # (old-broker-422 compatibility case).
            _payload = entry.get("payload") or {}
            skew_floor_422 = (code == 422 and is_ack and isinstance(_payload, dict)
                              and _payload.get("floor") is not None)
            if (code in (404, 405) and is_ack) or skew_floor_422:
                log.warning("outbox: /ack replay got %s (broker predates /ack or its floor "
                            "field? deploy skew) for %s — retaining for the 24h TTL window",
                            code, entry["client_event_id"])
                counts["failed"] += 1
                survivors.append(line)
            else:
                log.error("outbox: 4xx replay failure for %s (client_event_id=%s) — "
                          "dropping: %s", ep, entry["client_event_id"], ve)
                counts["dropped_corrupted"] += 1
        except Exception as e:
            log.error("outbox: POST raised unexpectedly: %s", e)
            counts["failed"] += 1
            survivors.append(line)

    # Bounded mode: re-queue the entries we deferred this call so they flush next time.
    survivors.extend(deferred)

    # APPEND survivors back to the live outbox (which may have grown
    # via concurrent enqueues during processing — appending preserves them). Use
    # the same atomic-append discipline as _enqueue_outbox.
    if survivors:
        outbox_path.parent.mkdir(parents=True, exist_ok=True)
        with outbox_path.open("a", encoding="utf-8") as f:
            for line in survivors:
                f.write(line + "\n")
            f.flush()
            os.fsync(f.fileno())

    # Remove the rotated processing file now that survivors have been re-queued.
    try:
        os.unlink(processing_path)
    except FileNotFoundError:
        pass  # benign; some other path already cleaned up

    # reset process-local counter — concurrent enqueues may have added
    # lines we don't see, so the counter is best-effort per-process. Re-init via
    # -1 sentinel so the next _enqueue_outbox call lazy-counts the actual file.
    _outbox_line_count = -1

    return counts


# ---------------------------------------------------------------------------
# Public: post_status_self / post_event_self (Layer 1 of multi-project dashboard)
# ---------------------------------------------------------------------------

def _load_self_registry() -> Optional[dict]:
    """One-shot registry load shared with `_resolve_self_display_name`.
    Replaces the previous pattern where the resolver did its own disk read — a
    redundant disk read per `post_status_self` / `post_event_self` call.

    Prefers `shared._load_registry_for_format()` (the canonical hot-path loader
    in shared.py L1164-1173). Falls back to None (resolvers then do their own
    disk read) when shared is unavailable. Never raises.
    """
    shared_mod = _get_shared_module()
    if shared_mod is None:
        return None
    loader = getattr(shared_mod, "_load_registry_for_format", None)
    if loader is None:
        return None
    try:
        result = loader()
        return result if isinstance(result, dict) else None
    except Exception as e:
        sys.stderr.write(
            f"broker_client._load_self_registry: shared loader raised "
            f"({type(e).__name__}: {e}); resolvers will fall back to their own disk read\n"
        )
        return None


def _resolve_self_session_id() -> Optional[str]:
    """Resolve calling CC session_id via CLAUDE_CODE_SESSION_ID env var.

    Per the no-auto-derive contract: CC injects
    CLAUDE_CODE_SESSION_ID into every subprocess env at process-spawn time
    (Node.js child_process.spawn with env propagation). Verified empirically
    on Windows Git Bash via 9-test probe (T1-T7 PASS including heartbeat-style
    fully-detached subprocess pattern); inferred HIGH confidence on Linux/macOS
    via claude.exe binary symmetry + cross-platform Node.js semantics.

    Returns None if env var unset (caller is NOT running inside CC, or CC
    version is too old to inject the var). Caller MUST handle None per the no-auto-derive contract:
    fail loud + log diagnostic; NO uuid4 fallback (would inject ephemeral
    sids not joinable to other session-tagged data).

    Pre-no-auto-derive used pid-walk + uuid4 fallback; both removed:
    - pid-walk: undocumented + broken on Windows ($PPID=1, orphaned subprocess);
      was a confabulated invention
    - uuid4 fallback: violated the no-auto-derive contract (ephemeral sids pollute broker without
      cross-data linkage)
    """
    return os.environ.get("CLAUDE_CODE_SESSION_ID")


def _resolve_self_display_name(
    session_id: str,
    *,
    registry: Optional[dict] = None,
) -> Optional[str]:
    """Resolve the friendly display_name for the given session_id.

    Priority:
      1. shared.resolve_sender_name(session_id) — reads ~/.claude/relay/registry.json
      2. None — no display_name at all. A resolver that could not establish
         identity must not invent an identity-shaped one (C-55).

    Returns the display_name string, or None when no name could be
    ESTABLISHED: shared.py unimportable, its resolver RAISED, or session_id
    empty. A valid session_id no longer guarantees a non-None return -- that
    sentence used to say None required an EMPTY session_id, and cross-family
    review caught it as an unchanged context line beside the changed code.
    Inventing a label from an id we could not resolve is exactly the pattern
    this file stopped carrying (C-55). The None case lets post_status omit the
    field per backward-compat, which is why omission is safe for callers.
    NOTE shared.resolve_sender_name may itself answer the literal '<unknown>'
    for a session-id-shaped miss -- a STRING, not None.

    Accepts optional `registry` kwarg (a pre-loaded dict),
    forwarded to `shared.resolve_sender_name(..., registry=registry)` which
    already supports the same pattern (per shared.py L1131-1161). When
    `registry is None` (existing callers), `resolve_sender_name` falls back
    to its own disk read — preserving backward compat.
    """
    shared_mod = _get_shared_module()
    if shared_mod is not None:
        resolver = getattr(shared_mod, "resolve_sender_name", None)
        if resolver is not None:
            try:
                if registry is not None:
                    return resolver(session_id, registry=registry)
                return resolver(session_id)
            except Exception as e:
                sys.stderr.write(
                    f"broker_client._resolve_self_display_name: resolver raised "
                    f"({type(e).__name__}: {e}); no display_name will be sent\n"
                )
    # WAS `(session_id[:8] + "…") if len(session_id) > 8 else session_id`. This
    # arm is reached ONLY when shared.py is unimportable or its resolver RAISED
    # — i.e. exactly when we could not establish who we are — and it answered
    # with an identity-shaped string anyway. That is the same root-cause pattern
    # this change removes from `shared.resolve_sender_name` and
    # `send.py::_resolve_sender_name`: a resolver whose failure path is
    # indistinguishable from an answer (D181/D182, C-55).
    #
    # `None` rather than `"<unknown>"` because display_name is an OPTIONAL Layer-2
    # overlay (C-8) and this function's own next line already returns None for
    # "nothing to say" — omitting the overlay leaves `agent_status.id`, the
    # session_id, as the Layer-1 identity the dashboard falls back to. Inventing
    # a label is what we are deleting; a second invented label is not the fix.
    #
    # MEASURED, so this is disclosed as latent rather than sold as a live save:
    # ZERO ellipsis-bearing values exist in agent_status.display_name,
    # message_log.from_agent or message_log.to_agent across 6,174 rows — this arm
    # has never reached the broker. Changed because the PATTERN is present and
    # C-55 scopes by the pattern's footprint, not by whether it has bitten yet.
    return None

def post_status_self(
    *,
    kind: str,
    channel: str,
    status: str,
    current_task: Optional[str] = None,
    last_activity_ts: Optional[int] = None,
    client_event_id: Optional[str] = None,  # TeamCreate-hook dedup: forwarded to post_status() for broker dedup
    broker_url: Optional[str] = None,
) -> Optional[int]:
    """POST /api/agent/{self_id}/status with auto-resolved session_id + display_name.

    Per the multi-project dashboard plan § Layer 1 (two-layer architecture: session_id is the
    Layer 1 primitive; friendly name is the Layer 2 messaging-only overlay).
    Used by direct callers that want self-identity resolution.

    Resolution chain (post-no-auto-derive):
      session_id   ← CLAUDE_CODE_SESSION_ID env (injected by claude.exe)
                     → None (caller fails loud per the no-auto-derive contract; NO uuid4 fallback)
      display_name ← shared.resolve_sender_name(session_id) → None if unresolvable (cosmetic overlay)

    Soft-import shared at module-top (guarded). If shared.py unimportable
    (broker_client used standalone), display_name is omitted rather than invented.
    If CLAUDE_CODE_SESSION_ID is unset, broker post is SKIPPED + diagnostic
    logged to stderr (per the no-auto-derive contract).

    Returns event_id (int) on success; None on broker unavailability per
    post_status() semantics.

    the post-cwd-derive-revert contract: Tier-2 lazy state.db bootstrap +
    project resolution removed. Sub-agent broker emissions use explicit
    `<subagent_type>@<parent_session_id>` agent_id via post_status() per
    the cwd-derive contract appendix.
    """
    session_id = _resolve_self_session_id()
    if session_id is None:
        sys.stderr.write(
            "broker_client.post_status_self: CLAUDE_CODE_SESSION_ID env var not set; "
            "broker post skipped (per the no-auto-derive contract). This script must "
            "run inside a Claude Code session, OR caller must set CLAUDE_CODE_SESSION_ID "
            "explicitly before calling.\n"
        )
        return None
    registry = _load_self_registry()
    display_name = _resolve_self_display_name(session_id, registry=registry)
    return post_status(
        agent_id=session_id,
        kind=kind,
        channel=channel,
        status=status,
        current_task=current_task,
        last_activity_ts=last_activity_ts,
        client_event_id=client_event_id,  # TeamCreate-hook dedup: hook needs deterministic dedup key (`hook-tc:{team_name}`)
        broker_url=broker_url,
        display_name=display_name,
    )


def post_event_self(
    *,
    to_agent: Optional[str],
    channel: str,
    kind: str,
    content: str,
    ts: Optional[int] = None,
    client_event_id: Optional[str] = None,
    broker_url: Optional[str] = None,
) -> Optional[int]:
    """POST /api/event with from_agent auto-resolved to the calling CC session_id.

    Sibling to post_status_self() — same Layer 1 resolution chain for `from_agent`.
    Used by direct callers that want self-identity resolution.

    Returns event_id (int) on success; None on broker unavailability.

    the post-cwd-derive-revert contract: Tier-2 lazy state.db bootstrap +
    project resolution removed. See post_status_self for full rationale.
    """
    session_id = _resolve_self_session_id()
    if session_id is None:
        sys.stderr.write(
            "broker_client.post_event_self: CLAUDE_CODE_SESSION_ID env var not set; "
            "broker post skipped (per the no-auto-derive contract). This script must "
            "run inside a Claude Code session, OR caller must set CLAUDE_CODE_SESSION_ID "
            "explicitly before calling.\n"
        )
        return None
    registry = _load_self_registry()
    display_name = _resolve_self_display_name(session_id, registry=registry)
    return post_event(
        from_agent=session_id,
        to_agent=to_agent,
        channel=channel,
        kind=kind,
        content=content,
        ts=ts,
        client_event_id=client_event_id,
        broker_url=broker_url,
        display_name=display_name,
    )


# ---------------------------------------------------------------------------
# Internal: _post
# ---------------------------------------------------------------------------

class Broker4xxError(ValueError):
    """Raised by _post(raise_on_4xx=True) on a 4xx response. Subclass of ValueError
    so existing `except ValueError` handlers still catch it, but carries
    .status_code so flush_outbox() can tell a TRANSIENT deploy-skew 404/405 (the
    endpoint/method is absent on a broker that predates it, or a rolling client/
    server version skew — retryable once the broker is upgraded) from a PERMANENT
    4xx (400/422 — malformed body / unknown route — drop)."""

    def __init__(self, status_code: int, endpoint: str, body: str) -> None:
        self.status_code = status_code
        super().__init__(f"broker 4xx for {endpoint}: {status_code} {body[:500]}")


def _post(
    endpoint: str,
    body: dict,
    *,
    idempotency_key: Optional[str] = None,
    raise_on_4xx: bool = False,            # True only for register_project
    no_outbox_on_failure: bool = False,    # True for refresh_project + flush_outbox
    broker_url: Optional[str] = None,
    attempts: Optional[int] = None,        # None => default 5; hook fast_fail passes 1
    timeout_s: Optional[float] = None,     # None => _BROKER_TIMEOUT_S; hook passes ~1.0
    skip_outbox_count: bool = False,       # hook fast_fail: skip O(N) count on the crit path
    outbox_on_4xx: bool = False,           # post_ack: enqueue on 4xx (404/405 deploy-skew) not silent-drop
) -> Optional[Dict[str, Any]]:
    """Internal: HTTP POST with retry + outbox-on-failure.

    Returns the full broker response dict on success (callers extract their own
    fields: event_id for post_status/post_event; project_id for register_project;
    full dict for refresh_project). Returns None on failure.

    returns Dict (not Optional[int]) so callers can extract their
    own fields without the union-of-types lie or 0-sentinel hack.

    if raise_on_4xx=True (register_project only), 4xx responses
    raise ValueError with the broker error body. Default False matches the
    "fail soft" contract for post_status/post_event.

    If no_outbox_on_failure=False (default), 5xx-exhausted requests are
    enqueued to outbox.

    Bearer token added from BROKER_BEARER_TOKEN env if set (required when
    broker bound non-loopback per contract § 1).
    """
    # NB: defense-in-depth — KeyError raised at request-time IF the import-time
    # guard for BROKER_BASE_URL was somehow bypassed. Per the broker URL contract
    # — NON-NEGOTIABLE config-from-file discipline — NO os.environ.get() default.
    # Use the canonicalized module constant _BROKER_BASE_URL
    # instead of re-reading os.environ["BROKER_BASE_URL"] (which would re-admit
    # surrounding-whitespace values that the import-time stripped validator caught).
    url = (broker_url or _BROKER_BASE_URL).rstrip("/") + endpoint
    if idempotency_key is None:
        idempotency_key = str(uuid.uuid4())
    # client_event_id belongs in body ONLY for /api/event and
    # /api/agent/{id}/status per contract.md § 1. Project-registration endpoints
    # (/api/project/register, /api/project/{name}/refresh) do NOT accept it;
    # body stays as-passed. Idempotency-Key HTTP header (below) still goes on
    # every request (broker-side idempotency middleware).
    _endpoint_accepts_client_event_id = (
        endpoint == "/api/event"
        or (endpoint.startswith("/api/agent/") and endpoint.endswith("/status"))
    )
    if _endpoint_accepts_client_event_id and "client_event_id" not in body:
        body["client_event_id"] = idempotency_key

    headers = {
        "Content-Type": "application/json",
        "Idempotency-Key": idempotency_key,
    }
    bearer = os.environ.get("BROKER_BEARER_TOKEN")
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"

    payload_bytes = json.dumps(body).encode("utf-8")
    enqueued_ts = int(time.time() * 1000)

    # Per-call timeout override (hook fast_fail passes ~1.0s to bound the turn
    # critical path); else the precomputed _BROKER_TIMEOUT_S float (import-time).
    timeout_s = timeout_s if timeout_s is not None else _BROKER_TIMEOUT_S
    last_error: Optional[str] = None

    # Five attempts, with sleeps at 0/50/100/200/400 ms. The 800ms tuple entry
    # is reserved; a sixth attempt would change the retry contract.
    sleeps_ms = [0] + list(RETRY_BACKOFF_MS[:4])  # [0, 50, 100, 200, 400] = 5 attempts
    if attempts is not None:
        # Bounded single-shot for the hook's fast_fail path (turn critical-path
        # latency): 1 attempt => refused-broker fails instantly, wedged <= timeout_s.
        sleeps_ms = sleeps_ms[:max(1, attempts)]
    MAX_ATTEMPTS = len(sleeps_ms)  # = 5 default; = `attempts` when overridden

    for attempt_idx, backoff_ms in enumerate(sleeps_ms):
        if backoff_ms > 0:
            time.sleep(backoff_ms / 1000.0)
        try:
            req = Request(url, data=payload_bytes, headers=headers, method="POST")
            with urlopen(req, timeout=timeout_s) as resp:
                resp_body = json.loads(resp.read().decode("utf-8"))
                if attempt_idx > 0:
                    # Log success-after-retry for observability
                    log.info("_post: succeeded on attempt %d/%d after backoff",
                             attempt_idx + 1, MAX_ATTEMPTS)
                return resp_body if isinstance(resp_body, dict) else {}
        except HTTPError as e:
            try:
                err_body = e.read().decode("utf-8")
            except Exception:
                err_body = "<no body>"
            if 400 <= e.code < 500:
                log.error("_post 4xx %s -> %s: %s", url, e.code, err_body)
                if raise_on_4xx:
                    raise Broker4xxError(e.code, endpoint, err_body)
                # do NOT retry a 4xx. By default do NOT enqueue either. EXCEPTION
                # (post_ack sets outbox_on_4xx): enqueue the ack so it can replay —
                # this keeps post_ack's fast_fail invariant "None means outboxed" true
                # for EVERY failure class, so it never silently drops a chunk. The
                # REPLAY side (flush_outbox) then decides: a transient deploy-skew
                # 404/405 on /ack is RETAINED for the 24h TTL (self-heals once the
                # broker is upgraded); a permanent 4xx (422) drops on first replay.
                # Honor skip_outbox_count (hook stays O(1)); respect no_outbox_on_failure.
                if outbox_on_4xx and not no_outbox_on_failure:
                    _enqueue_outbox(endpoint, body, idempotency_key, enqueued_ts,
                                    update_count=not skip_outbox_count)
                return None  # do NOT retry
            last_error = f"HTTP {e.code}: {err_body[:200]}"
            log.warning("_post 5xx attempt %d/%d %s: %s",
                        attempt_idx + 1, MAX_ATTEMPTS, url, last_error)
        except URLError as e:
            last_error = f"URLError: {e.reason}"
            log.warning("_post urlerror attempt %d/%d %s: %s",
                        attempt_idx + 1, MAX_ATTEMPTS, url, last_error)
        except Exception as e:
            last_error = f"{type(e).__name__}: {e}"
            log.warning("_post unexpected attempt %d/%d %s: %s",
                        attempt_idx + 1, MAX_ATTEMPTS, url, last_error)

    # All retries exhausted; enqueue unless caller is the flusher itself
    if not no_outbox_on_failure:
        _enqueue_outbox(endpoint, body, idempotency_key, enqueued_ts,
                        update_count=not skip_outbox_count)
        log.warning("_post: enqueued to outbox after %d attempts (last error: %s)",
                    MAX_ATTEMPTS, last_error)
    return None


# ---------------------------------------------------------------------------
# Internal: _get
# ---------------------------------------------------------------------------

def _get(
    endpoint: str,
    *,
    broker_url: Optional[str] = None,
    suppress_auth: bool = False,  # True for /api/health per contract § 1
) -> Optional[Dict[str, Any]]:
    """Internal: HTTP GET with NO retry + NO outbox.

    Returns the full broker response dict on success; None on
    ConnectionRefusedError / URLError / timeout / non-200 status.

    Used by health_check() (suppress_auth=True per contract § 1: "/api/health
    requires NO auth even on non-loopback bind"). Other GET endpoints (e.g.,
    /api/state, /api/discord/cursors) MAY be added later but are not in v1.2
    scope.

    NO retry because the GET endpoints used here are diagnostic (health + state
    snapshot); the caller wants to surface unavailability immediately, not mask
    it with retries.
    """
    # NB: defense-in-depth — KeyError raised at request-time IF the import-time
    # guard for BROKER_BASE_URL was somehow bypassed. Per the broker URL contract
    # — NON-NEGOTIABLE config-from-file discipline — NO os.environ.get() default.
    # Use the canonicalized module constant _BROKER_BASE_URL.
    url = (broker_url or _BROKER_BASE_URL).rstrip("/") + endpoint
    headers = {"Accept": "application/json"}
    bearer = os.environ.get("BROKER_BEARER_TOKEN")
    if bearer and not suppress_auth:
        headers["Authorization"] = f"Bearer {bearer}"

    # Use precomputed _BROKER_TIMEOUT_S float.
    timeout_s = _BROKER_TIMEOUT_S
    try:
        req = Request(url, headers=headers, method="GET")
        with urlopen(req, timeout=timeout_s) as resp:
            resp_body = json.loads(resp.read().decode("utf-8"))
            return resp_body if isinstance(resp_body, dict) else {}
    except HTTPError as e:
        try:
            err_body = e.read().decode("utf-8")
        except Exception:
            err_body = "<no body>"
        log.warning("_get %s -> %s: %s", url, e.code, err_body[:200])
        return None
    except URLError as e:
        log.warning("_get urlerror %s: %s", url, e.reason)
        return None
    except Exception as e:
        log.warning("_get unexpected %s: %s: %s", url, type(e).__name__, e)
        return None


# ---------------------------------------------------------------------------
# Internal: _enqueue_outbox
# ---------------------------------------------------------------------------

def _enqueue_outbox(endpoint: str, payload: dict, client_event_id: str, enqueued_ts: int,
                    *, update_count: bool = True) -> None:
    """Append one outbox entry (atomic-append per POSIX whole-line semantics).

    maintains process-local _outbox_line_count for O(1) threshold
    checks. Counter is lazy-init on first call (one O(N) read) and reset by
    flush_outbox() return. Concurrent processes' enqueues are NOT visible to
    this counter — best-effort per-process; documented limitation.
    """
    global _outbox_line_count
    outbox_path = _outbox_path()
    outbox_path.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "client_event_id": client_event_id,
        "endpoint": endpoint,
        "payload": payload,
        "enqueued_ts": enqueued_ts,
    }
    # Append-only; no atomic-rename needed for append (POSIX guarantees
    # whole-line append atomicity for writes < PIPE_BUF on local filesystems;
    # Windows single-write of a small JSON line is also atomic-ish in practice).
    with outbox_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")
        f.flush()
        os.fsync(f.fileno())

    # O(1) threshold check via process-local counter (lazy-init).
    # WS-B-client (#19): the hook's fast_fail enqueue (update_count=False) must not
    # pay the lazy O(N) whole-file re-read on the critical path. Skip ONLY that
    # expensive re-read (when the counter is still uninitialized); if the counter is
    # already initialized, keep the cheap O(1) increment so the >1000 "visibility
    # degraded" warning stays accurate for long-lived callers.
    if _outbox_line_count < 0:
        if not update_count:
            # F3 (accepted, LOW): the counter stays uninitialized (-1) here, so a
            # process that ONLY ever takes the fast_fail path never trips the >1000
            # warning below. Moot for the real callers — the receive hook is a
            # short-lived per-fire subprocess (counter never persists across fires),
            # and the MCP server uses the default counting path; a later non-fast_fail
            # enqueue recounts. We deliberately do NOT reintroduce the O(N) read here
            # (that crit-path cost is exactly what #19 removed).
            return
        # Lazy-init: count current lines (one-shot O(N) read; FD-leak safe per `with`).
        with outbox_path.open("r", encoding="utf-8") as f:
            _outbox_line_count = sum(1 for _ in f)
    else:
        _outbox_line_count += 1

    # Per contract § 5: outbox > 1000 lines = "broker visibility degraded" warning.
    if _outbox_line_count > 1000:
        log.warning("outbox: %d entries pending; broker visibility degraded "
                    "per contract.md § 5", _outbox_line_count)


# ---------------------------------------------------------------------------
# Internal: _outbox_path
# ---------------------------------------------------------------------------

def _outbox_path() -> Path:
    """Resolve the outbox file path.

    Honors BROKER_OUTBOX_PATH env override (with `~` expansion); defaults to
    DEFAULT_OUTBOX (~/.claude/channels/discord/outbox.jsonl) per contract § 5
    per the canonical-path declaration.

    Definition gap-fix: previously the module shape listed _outbox_path() and both
    flush_outbox() + _enqueue_outbox() called it, but no implementation was
    given — that would NameError on the broker-down path. Verbatim definition
    here closes the gap.
    """
    override = os.environ.get("BROKER_OUTBOX_PATH")
    if override:
        return Path(override).expanduser()
    return DEFAULT_OUTBOX


# ---------------------------------------------------------------------------
# Internal: _recover_orphan_processing_outboxes (orphan-recovery sweep)
# ---------------------------------------------------------------------------

def _recover_orphan_processing_outboxes() -> None:
    """One-shot startup sweep for orphan .processing-<uuid> outbox files.

    flush_outbox() uses a rotate-then-process pattern: atomically renames
    outbox.jsonl -> outbox.jsonl.processing-<uuid>, processes it, deletes on
    success. If the process crashes between rotation and deletion (OOM,
    kill -9, hardware failure), the .processing-<uuid> file is orphaned
    indefinitely — its events never reach the broker, never return to the
    live outbox. The flush_outbox() comment acknowledges this
    explicit gap; this sweep closes it.

    Algorithm:
      1. Glob outbox parent dir for `<outbox_name>.processing-*`.
      2. Age filter: only consider files older than ORPHAN_AGE_S (60s). A
         genuine orphan is minutes-to-months old; in-flight files being
         processed by a sibling process's flush_outbox() are seconds old.
         Filter avoids accidentally claiming a live rotation.
      3. Claim each candidate via os.replace -> `.recovering-<pid>-<uuid>`.
         Concurrent recovery sweeps race here; one wins (FileNotFoundError on
         the loser is benign + silent).
      4. Read claimed file line-by-line; append non-empty lines to live
         outbox.jsonl (same atomic-append discipline as _enqueue_outbox).
         Broker dedupes via idempotency_ledger so any duplicate replays from
         a flushed-but-incomplete-cleanup scenario are absorbed harmlessly.
      5. Delete claimed file after successful merge.

    Failure handling: any per-file error is logged + that file's claim stays
    on disk (claim-name distinct from `.processing-*` so next sweep won't
    retry it automatically — manual review needed for that case; rare).
    Module load NEVER raises from this sweep; outer try/except catches all.
    Worst case: orphans persist for next module load to retry.
    """
    ORPHAN_AGE_S = 60.0
    try:
        outbox_path = _outbox_path()
        parent = outbox_path.parent
        if not parent.is_dir():
            return
        glob_pattern = outbox_path.name + ".processing-*"
        now = time.time()
        recovered_files = 0
        recovered_lines = 0
        for orphan in parent.glob(glob_pattern):
            try:
                try:
                    mtime = orphan.stat().st_mtime
                except FileNotFoundError:
                    continue
                if now - mtime < ORPHAN_AGE_S:
                    continue  # too fresh; likely in-flight flush_outbox in sibling process
                # Claim name lives OUTSIDE the `.processing-*` glob namespace so a
                # merge-failed stranded claim file is NOT re-picked-up by the next
                # sweep (per design failure-handling note: manual review only).
                claim_name = f"{outbox_path.name}.recovering-{os.getpid()}-{uuid.uuid4()}"
                claim_path = orphan.with_name(claim_name)
                # Re-stat immediately before claim to close the sub-millisecond
                # pre-claim race: a sibling flush_outbox could rotate-then-touch
                # a fresh in-flight file at this path between our initial stat
                # and os.replace below (inheriting old mtime that passed the age
                # filter, then resetting to NOW via touch()). Deferred LOW
                # (closed in a later pass).
                try:
                    if time.time() - orphan.stat().st_mtime < ORPHAN_AGE_S:
                        continue  # raced; sibling just rotated + touched; benign
                except FileNotFoundError:
                    continue  # raced and lost; benign
                try:
                    os.replace(orphan, claim_path)
                except FileNotFoundError:
                    continue  # another sweeper claimed it first; benign
                except Exception as e:
                    log.warning("broker_client orphan-recovery: claim failed for %s: %s",
                                orphan.name, e)
                    continue
                try:
                    with claim_path.open("r", encoding="utf-8") as src:
                        lines = [ln.rstrip("\n") for ln in src if ln.strip()]
                    if lines:
                        outbox_path.parent.mkdir(parents=True, exist_ok=True)
                        with outbox_path.open("a", encoding="utf-8") as dst:
                            for ln in lines:
                                dst.write(ln + "\n")
                            dst.flush()
                            os.fsync(dst.fileno())
                    try:
                        claim_path.unlink()
                    except FileNotFoundError:
                        pass
                    recovered_files += 1
                    recovered_lines += len(lines)
                except Exception as e:
                    log.error("broker_client orphan-recovery: merge failed for %s "
                              "(claim file %s stranded; manual review needed): %s",
                              orphan.name, claim_path.name, e)
            except Exception as e:
                # Per-file isolation: ANY error on this candidate (PermissionError
                # from AV-locked file, OSError, etc.) is logged + skip; sweep
                # continues to the next candidate. Outer try/except still wraps
                # the whole sweep as the final safety net.
                log.warning("broker_client orphan-recovery: skipped %s: %s",
                            orphan.name, e)
                continue
        if recovered_files > 0:
            log.info("broker_client orphan-recovery: merged %d orphan file(s); "
                     "%d line(s) re-queued",
                     recovered_files, recovered_lines)
    except Exception as e:
        log.error("broker_client orphan-recovery: unexpected failure: %s", e)


# NOTE (#18/F2): the orphan-recovery sweep is NO LONGER run at module import — that
# made every per-fire receive-hook subprocess (which imports this module but never
# flushes) pay a glob/fsync sweep on the turn critical path. It now runs as the first
# statement inside flush_outbox() instead, so only FLUSHERS (the long-lived MCP push
# server, on each SSE reconnect) pay it — which is exactly where orphans are created
# and where they must be recovered. See flush_outbox().


# ---------------------------------------------------------------------------
# CLI / __main__: --self-check
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    """CLI entry-point for module self-check smoke.

    Recognized commands:
      python broker_client.py --self-check
        Imports/executes health_check() against BROKER_BASE_URL env var (REQUIRED;
        raises at import if unset); prints `PASS: broker reachable` (exit 0) on
        success; prints `FAIL: broker unavailable (semantic)` (exit 0 — broker-down
        is a SEMANTIC FAIL not an import failure; the module is loadable). Prints
        `ERROR: <type>: <msg>` (exit 1) only on import / syntax / internal exception
        (which would indicate a broken module, not a missing broker).

    Definition gap-fix: previously the verification checklist referenced this
    self-check but no __main__ block existed.
    Verbatim implementation here closes the gap.
    """
    import sys

    # Windows UTF-8 discipline per common system prompt § Tool Usage:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")

    # Configure basic logging so the user sees broker_client log output:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    if len(sys.argv) < 2 or sys.argv[1] != "--self-check":
        print("Usage: python broker_client.py --self-check", file=sys.stderr)
        sys.exit(2)

    try:
        result = health_check()
    except Exception as e:
        # Import / syntax / internal error — the MODULE is broken (not the broker).
        print(f"ERROR: {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(1)

    if result is None:
        # Broker unreachable — semantic FAIL but the module loaded + ran cleanly.
        # use module constant (consistent with _post / _get).
        print(f"FAIL: broker unavailable at {_BROKER_BASE_URL} (semantic)")
        sys.exit(0)
    else:
        print(f"PASS: broker reachable; health = {result}")
        sys.exit(0)
