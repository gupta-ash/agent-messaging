"""
Agent Orchestration Research — Broker Discord REST Poller (M2)

Implements D16 § (a)-(e) RECEIVE path: periodic asyncio task that polls
Discord REST API per registered (channel_id, thread_id) target, gates messages
per access.json policy, decodes permission-reply patterns, and broadcasts new
messages via SSE so subagents can subscribe to inbound user messages.

Authority: `decisions.md` D16 § (a)-(e) (RATIFIED 2026-05-05).

The Claude Code binary drops `notifications/claude/channel` MCP push for
API/Copilot users. The plugin MCP TOOLS for SEND work; only RECEIVE is broken.
This poller is the RECEIVE path.

Token NEVER appears in logs, error responses, or stderr. See `_load_env_token`
and `_make_client_headers` for the masking pattern (token loaded into a single
local + httpx Authorization header; never stringified into log/error paths).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import re
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import aiosqlite
import httpx

# Server module imports — broker patterns we MUST reuse, not duplicate.
# (connect_db, with_retry, now_ms, broadcast_event are the shared primitives.)
from broker import server as _srv

logger = logging.getLogger("broker.discord_poller")

# --------------------------------------------------------------- Constants

# Discord epoch: Jan 1 2015 00:00:00 UTC in ms.
# https://discord.com/developers/docs/reference#snowflakes
DISCORD_EPOCH_MS = 1_420_070_400_000

# Plugin parity: PERMISSION_REPLY_RE from server.ts:79.
# 5 lowercase letters a-z minus 'l'. Case-insensitive.
PERMISSION_REPLY_RE = re.compile(r"^\s*(y|yes|n|no)\s+([a-km-z]{5})\s*$", re.IGNORECASE)

# State dir mirrors plugin's STATE_DIR resolution (server.ts:37).
DISCORD_STATE_DIR = Path(
    os.environ.get("DISCORD_STATE_DIR")
    or (Path.home() / ".claude" / "channels" / "discord")
)
ENV_FILE = DISCORD_STATE_DIR / ".env"
ACCESS_FILE = DISCORD_STATE_DIR / "access.json"

# Discord REST API base.
DISCORD_API_BASE = "https://discord.com/api/v10"

# Poll cadence per D16 § (d).5.
POLL_INTERVAL_SEC = 15
POLL_JITTER_SEC = 3  # uniform [-3, +3]

# Rate-limit state machine per D16 § (d).
MAX_429_ATTEMPTS_BEFORE_CIRCUIT = 5  # 6th opens
INITIAL_PAUSE_WINDOW_SEC = 300  # 5 min
MAX_PAUSE_WINDOW_SEC = 3600  # 60 min cap
PERSISTENT_OPEN_THRESHOLD = 3  # 3x 60-min caps → escalate
HALF_OPEN_5XX_RETRY_SEC = 30

# Global token bucket per D16 § (d).4.
GLOBAL_BUCKET_CAPACITY = 50
GLOBAL_BUCKET_FILL_PER_SEC = 50

# Catch-up window cap per D16 § (e).
CATCHUP_WINDOW_MS = 3_600_000  # 1h

# Discord message types per D16 § (c). Only DEFAULT (0) and REPLY (19) are user content.
USER_CONTENT_MESSAGE_TYPES = {0, 19}

# message_log channel/from_agent conventions for Discord-originated rows.
DISCORD_CHANNEL_PREFIX = "#discord:"  # Used as message_log.channel value.
DISCORD_USER_PREFIX = "user:"  # message_log.from_agent prefix per D16.
DISCORD_BROKER_AGENT = "system:discord_poller"  # for kind=error rows.


# --------------------------------------------------------------- Metrics
# Module-scope dict; mutation only on the asyncio loop thread (no lock needed
# because all writers are coroutines on a single event loop).

broker_metrics: dict[str, Any] = {
    "discord_dropped_total": 0,
    "discord_polls_total": 0,
    "discord_429_total": 0,
    "discord_messages_broadcast_total": 0,
    # Per-channel circuit-break state: { channel_id: bool }
    "discord_rate_limit_circuit_open": {},
    # Per-channel persistent-open escalation tracker: { channel_id: int }
    "discord_consecutive_capped_failures": {},
}


def _inc(key: str, n: int = 1) -> None:
    broker_metrics[key] = broker_metrics.get(key, 0) + n


def _route_label(url: str) -> str:
    """Strip /api/v10 prefix from a Discord URL for terse logging.
    Used by _discord_request error logs so we don't dump full URLs (which
    are not sensitive but make grep noisy).
    """
    base = DISCORD_API_BASE
    if url.startswith(base):
        return url[len(base):] or "/"
    return url


# --------------------------------------------------------------- Snowflake

def decode_snowflake(snowflake: int | str) -> int:
    """Decode a Discord snowflake to a UTC epoch-ms timestamp.

    Per Discord docs (https://discord.com/developers/docs/reference#snowflakes):
        timestamp_ms = (snowflake >> 22) + DISCORD_EPOCH_MS

    Discord epoch (DISCORD_EPOCH_MS) = 1420070400000 = 2015-01-01T00:00:00.000Z.

    Pure function. Accepts int or stringified int (Discord IDs are 64-bit; JSON
    delivers them as strings in some places, ints in others — accept both).
    """
    sf = int(snowflake)
    if sf < 0:
        raise ValueError(f"snowflake must be non-negative; got {sf}")
    return (sf >> 22) + DISCORD_EPOCH_MS


# --------------------------------------------------------------- Env / access

def _load_env_token() -> Optional[str]:
    """Read DISCORD_BOT_TOKEN from real env first, else stdlib-parse plugin's .env.

    Plugin parity (server.ts:42-51): the .env format is `KEY=VALUE` per line,
    matched with `/^(\\w+)=(.*)$/`; real env wins.

    Returns None on any failure; caller logs ONE warning (no token in logs,
    no full path in non-WARN logs).
    """
    real = os.environ.get("DISCORD_BOT_TOKEN")
    if real:
        return real
    try:
        text = ENV_FILE.read_text(encoding="utf-8")
    except (FileNotFoundError, OSError):
        return None
    pat = re.compile(r"^(\w+)=(.*)$")
    for line in text.split("\n"):
        m = pat.match(line)
        if m and m.group(1) == "DISCORD_BOT_TOKEN":
            tok = m.group(2).strip()
            return tok or None
    return None


async def post_alert(
    content: str,
    channel_id: str,
    *,
    token: Optional[str] = None,
) -> bool:
    """Standalone Discord channel POST for broker escalation alerts (#18 reaper).

    Deliberately does NOT require a running DiscordPoller: the poller only exists
    when poll targets are registered, but the escalation reaper must be able to
    alert regardless of poller state. It therefore builds its own short-lived
    httpx client — a second, independent rate-limiter for the rare-traffic alert
    channel is negligible vs coupling the reaper to the poller lifecycle. (Docs
    the rationale so a future reader doesn't "consolidate" it back into the
    poller's _discord_request.)

    Token resolution: explicit `token` arg else `os.environ["DISCORD_BOT_TOKEN"]` (env-ONLY;
    D82 — the reaper path deliberately does NOT use `_load_env_token`'s plugin-.env fallback, so
    a missing bot token is a fail-loud config error, not a silent read from an unmanaged file;
    the inbound DiscordPoller keeps its own `_load_env_token` handling). server.py's
    refuse-to-start precondition guarantees the env is set whenever the reaper is enabled.
    Missing token -> log once (never the token itself) + return False (reaper degrades; the
    broker keeps serving everything else).

    Posts `{"content": content[:1900], "allowed_mentions": {"parse": []}, "flags": 4}`
    (Discord's 2000-char limit, headroom; no mentions parsed; SUPPRESS_EMBEDS so a preview
    URL never unfurls) to POST /channels/{channel_id}/messages with `Authorization:
    Bot <token>`. Honors a SINGLE 429 Retry-After retry (miniature of
    DiscordPoller._discord_request).
    Any non-2xx / transport error -> log + return False so the caller leaves
    `alerted_ts` NULL and retries next cycle (at-least-once, dedup-guarded).

    Returns True only on a 2xx create. Never raises (best-effort egress).
    """
    tok = token or os.environ.get("DISCORD_BOT_TOKEN")  # D82: env-ONLY in the reaper path (no plugin-.env fallback)
    if not tok:
        logger.warning("post_alert: no DISCORD_BOT_TOKEN available; skipping alert")
        return False
    url = f"{DISCORD_API_BASE}/channels/{channel_id}/messages"
    headers = {
        "Authorization": f"Bot {tok}",
        "User-Agent": "agent-orchestration-broker (broker, reaper-alert)",
    }
    # allowed_mentions parse:[] => Discord parses NO mentions from the content, whatever
    # the body says — a hard API-level guarantee that a spliced peer name can never ping
    # @everyone/@here/roles/users. flags=4 (SUPPRESS_EMBEDS) stops Discord from unfurling
    # any URL in the untrusted preview into a rich embed card, so the alert stays one inert
    # line. Both are defense-in-depth atop the reaper's text sanitizer.
    payload = {"content": content[:1900], "allowed_mentions": {"parse": []}, "flags": 4}
    try:
        async with httpx.AsyncClient(headers=headers, timeout=30.0) as http:
            attempts_429 = 0
            while True:
                try:
                    resp = await http.post(url, json=payload)
                except httpx.HTTPError as e:
                    # F-CC21 A3 parity: full traceback, route label only (no token/URL).
                    logger.warning("post_alert: transport error to %s: %s",
                                   _route_label(url), e, exc_info=True)
                    return False
                if resp.status_code == 429 and attempts_429 < 1:
                    # Honor Retry-After (max of body + header), then retry once.
                    _inc("discord_429_total")
                    try:
                        body_retry = float((resp.json()).get("retry_after", 0) or 0)
                    except (ValueError, json.JSONDecodeError, AttributeError):
                        body_retry = 0.0
                    try:
                        header_retry = float(resp.headers.get("Retry-After", "0") or 0)
                    except ValueError:
                        header_retry = 0.0
                    sleep_for = max(body_retry, header_retry)
                    if sleep_for > 0:
                        await asyncio.sleep(sleep_for)
                    attempts_429 += 1
                    continue
                if 200 <= resp.status_code < 300:
                    return True
                logger.warning("post_alert: Discord returned %s for %s",
                               resp.status_code, _route_label(url))
                return False
    except Exception as e:
        # Belt-and-suspenders: alert egress must never raise into the reaper loop.
        logger.warning("post_alert: unexpected error: %s", e, exc_info=True)
        return False


def _load_access() -> Optional[dict[str, Any]]:
    """Read access.json once at startup; return parsed dict or None on failure."""
    try:
        text = ACCESS_FILE.read_text(encoding="utf-8")
        return json.loads(text)
    except (FileNotFoundError, OSError, json.JSONDecodeError) as e:
        # F-CC21 A3: full traceback (was type(e).__name__ only — masked exception detail).
        logger.warning("discord_poller: access.json unreadable: %s", e, exc_info=True)
        return None


# --------------------------------------------------------------- Gate (parity)

@dataclass
class GateContext:
    """Pre-computed snapshot of access.json + bot user-id for fast per-message gating.

    Loaded once at poller start; no live re-read in hot path (see D16 anti-pattern
    rule: load .env and access.json once at startup with stdlib open)."""
    dm_policy: str  # 'disabled' | 'allowlist' | 'pairing'
    allow_from: set[str]
    groups: dict[str, dict[str, Any]]  # channel_id -> {requireMention, allowFrom, ...}
    mention_patterns: list[str]
    bot_user_id: Optional[str]


def _build_gate_context(access: dict[str, Any], bot_user_id: Optional[str]) -> GateContext:
    return GateContext(
        dm_policy=access.get("dmPolicy", "pairing"),
        allow_from=set(access.get("allowFrom") or []),
        groups=access.get("groups") or {},
        mention_patterns=access.get("mentionPatterns") or [],
        bot_user_id=bot_user_id,
    )


def _is_user_content_message(msg: dict[str, Any]) -> bool:
    """Bot/webhook/system filter per D16 § (c).

    Drop:
      1. author.bot === true (catches our own bot's reply echoes + any other bot)
      2. webhook_id != null (webhook messages)
      3. type not in {0, 19} (system messages: RECIPIENT_ADD, CHANNEL_PINNED_MESSAGE,
         GUILD_MEMBER_JOIN, etc. — only DEFAULT (0) and REPLY (19) are user content.)
    Discord message types reference:
        https://discord.com/developers/docs/resources/message#message-object-message-types
    """
    author = msg.get("author") or {}
    if author.get("bot"):
        return False
    if msg.get("webhook_id"):
        return False
    msg_type = msg.get("type", 0)
    if msg_type not in USER_CONTENT_MESSAGE_TYPES:
        return False
    return True


def _mentions_bot(msg: dict[str, Any], bot_user_id: Optional[str]) -> bool:
    """Check `mentions[]` for bot user id (parity with msg.mentions.has(client.user))."""
    if not bot_user_id:
        return False
    for m in msg.get("mentions") or []:
        if isinstance(m, dict) and m.get("id") == bot_user_id:
            return True
    return False


async def _is_reply_to_bot(
    msg: dict[str, Any],
    bot_user_id: Optional[str],
    http: httpx.AsyncClient,
    channel_id: str,
    discord_request: Optional[Any] = None,
) -> bool:
    """Check if msg is a reply to a bot-authored message.

    Plugin (server.ts:298-307) uses recentSentIds set + fetchReference fallback.
    Broker can't observe plugin send activity, so always fetches the referenced
    message and checks author.id === bot_user_id.

    Failures (deleted ref, no perms) → False, no raise.

    F11 (iter2 discriminator): when called from production code, the caller
    MUST pass `discord_request=poller._discord_request` so the fetch consumes
    a global token + honors per-route bucket headers + 429 backoff. The raw
    `http` fallback is for unit tests with mocked transports only — production
    never exercises the None branch.
    """
    if not bot_user_id:
        return False
    ref = msg.get("message_reference") or msg.get("referenced_message")
    ref_id = None
    # Discord API can present ref message inline (referenced_message) or just
    # via message_reference.message_id requiring a fetch.
    if isinstance(msg.get("referenced_message"), dict):
        ref_msg = msg["referenced_message"]
        ref_author = (ref_msg.get("author") or {}).get("id")
        return ref_author == bot_user_id
    if isinstance(ref, dict):
        ref_id = ref.get("message_id")
    if not ref_id:
        return False
    url = f"{DISCORD_API_BASE}/channels/{channel_id}/messages/{ref_id}"
    if discord_request is not None:
        # F11: production path — global token + bucket-aware + 429 backoff.
        resp = await discord_request(
            "GET", url, target_key=("ref_message", channel_id),
        )
        if resp is None or resp.status_code != 200:
            return False
        try:
            data = resp.json()
        except (ValueError, json.JSONDecodeError):
            return False
        return (data.get("author") or {}).get("id") == bot_user_id
    # Test fallback: raw http.get with mocked transport (httpx.MockTransport).
    # Production code MUST pass discord_request — see F11 contract above.
    try:
        resp = await http.get(url)
        if resp.status_code == 200:
            data = resp.json()
            return (data.get("author") or {}).get("id") == bot_user_id
    except (httpx.HTTPError, ValueError):
        pass
    return False


def _matches_mention_patterns(content: str, patterns: list[str]) -> bool:
    for pat in patterns:
        try:
            if re.search(pat, content, re.IGNORECASE):
                return True
        except re.error:
            continue
    return False


async def gate_message(
    msg: dict[str, Any],
    target_channel_id: str,
    is_dm: bool,
    parent_channel_id: Optional[str],
    ctx: GateContext,
    http: httpx.AsyncClient,
    discord_request: Optional[Any] = None,
) -> bool:
    """Plugin-gate parity per D16 § (b). Returns True if message should be delivered.

    `target_channel_id`: the actual channel (or thread) we polled — used for HTTP fetches.
    `parent_channel_id`: for threads, the parent channel id (used for groups[] lookup).
        For non-threads, equals target_channel_id.
    `is_dm`: whether the polled target is a DM channel.
    `discord_request`: optional production helper from DiscordPoller; threaded
        to `_is_reply_to_bot` so its REST fetch goes through the centralized
        rate-limit pipeline (F11 iter2). When None, raw `http` is used (tests
        with mocked transports only).
    """
    sender_id = (msg.get("author") or {}).get("id")
    if not sender_id:
        return False

    # § (b) row 1+2+3: dmPolicy applies ONLY to DMs.
    # F3 (iter1 discriminator): the previous code checked dm_policy=='disabled'
    # BEFORE the is_dm branch, which over-blocked every guild-channel message
    # whenever the user disabled DMs. D16 says guild channel policy is governed
    # by groups[] alone — dmPolicy must NOT gate guild traffic.
    if is_dm:
        if ctx.dm_policy == "disabled":
            return False
        # § (b) row 2: allowlist mode → require sender ∈ allowFrom
        if ctx.dm_policy == "allowlist":
            return sender_id in ctx.allow_from
        # § (b) row 3: pairing mode → broker DROPS (pairing is plugin-only)
        if ctx.dm_policy == "pairing":
            return False
        # Defensive: unknown mode → drop
        return False

    # Guild channel (or thread). § (b) row 4: thread → resolve to parent channel_id.
    lookup_id = parent_channel_id or target_channel_id
    # § (b) row 5: groups[channelId] missing → drop
    policy = ctx.groups.get(lookup_id)
    if not policy:
        return False
    # § (b) row 6: groupAllowFrom non-empty → sender ∈ it
    group_allow = policy.get("allowFrom") or []
    if group_allow and sender_id not in group_allow:
        return False
    # § (b) row 7: requireMention
    require_mention = policy.get("requireMention", True)
    if require_mention:
        if _mentions_bot(msg, ctx.bot_user_id):
            return True
        if await _is_reply_to_bot(msg, ctx.bot_user_id, http, target_channel_id,
                                  discord_request=discord_request):
            return True
        # § (b) row 8: mentionPatterns regex match (last-resort within requireMention)
        if _matches_mention_patterns(msg.get("content", "") or "", ctx.mention_patterns):
            return True
        return False
    return True


# --------------------------------------------------------------- Global token bucket

class GlobalTokenBucket:
    """Asyncio token bucket. capacity=50, fill=50/s per D16 § (d).4."""

    def __init__(self, capacity: int = GLOBAL_BUCKET_CAPACITY,
                 fill_per_sec: float = GLOBAL_BUCKET_FILL_PER_SEC):
        self.capacity = capacity
        self.fill_per_sec = fill_per_sec
        self.tokens = float(capacity)
        self.last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, n: int = 1) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self.last_refill
                self.tokens = min(self.capacity, self.tokens + elapsed * self.fill_per_sec)
                self.last_refill = now
                if self.tokens >= n:
                    self.tokens -= n
                    return
                deficit = n - self.tokens
                wait_sec = deficit / self.fill_per_sec
            await asyncio.sleep(wait_sec)


# --------------------------------------------------------------- Per-bucket registry (F4 iter1)

@dataclass
class BucketState:
    """Tracks Discord per-route rate-limit state from X-RateLimit-* response headers.

    Keyed by `X-RateLimit-Bucket` value in the registry below. Discord docs:
    https://discord.com/developers/docs/topics/rate-limits#header-format
    """
    limit: int = 0
    remaining: int = 0
    reset: float = 0.0          # absolute monotonic time when window resets
    reset_after: float = 0.0    # last-seen reset_after for diagnostics


class BucketRegistry:
    """Registry of per-bucket rate-limit state.

    F4 (iter1 discriminator): D16 § (d).1 requires reading X-RateLimit-* headers
    and waiting until reset when remaining=0. The previous implementation only
    inspected status codes, so a route that returned remaining=0 + future reset
    would still be hit on the next cycle and trigger a 429.

    Updates from EVERY response (200 / 429 / 5xx) when headers are present.
    Per-bucket state mutation is guarded by an asyncio.Lock for parity with
    GlobalTokenBucket's pattern (matches acceptance criterion 5 in task-spec).
    """

    def __init__(self):
        self._buckets: dict[str, BucketState] = {}
        # Map (channel_id, thread_id) → last-seen bucket id for that target.
        self._target_to_bucket: dict[tuple[str, str], str] = {}
        self._lock = asyncio.Lock()

    async def update_from_response(
        self,
        target_key: tuple[str, str],
        headers: Any,
    ) -> Optional[str]:
        """Parse X-RateLimit-* headers off a response and update bucket state.

        Returns the bucket id (X-RateLimit-Bucket) seen, or None if absent.
        """
        bucket_id = headers.get("X-RateLimit-Bucket") if hasattr(headers, "get") else None
        if not bucket_id:
            return None
        # Parse the four numeric headers; missing headers leave fields untouched.
        try:
            limit = int(headers.get("X-RateLimit-Limit", "0") or 0)
        except ValueError:
            limit = 0
        try:
            remaining = int(headers.get("X-RateLimit-Remaining", "0") or 0)
        except ValueError:
            remaining = 0
        try:
            reset_after = float(headers.get("X-RateLimit-Reset-After", "0") or 0)
        except ValueError:
            reset_after = 0.0
        # X-RateLimit-Reset is epoch-seconds; we convert to monotonic by adding
        # reset_after to monotonic-now (more reliable across clock skew).
        async with self._lock:
            state = self._buckets.setdefault(bucket_id, BucketState())
            state.limit = limit or state.limit
            state.remaining = remaining
            state.reset_after = reset_after
            if reset_after > 0:
                state.reset = time.monotonic() + reset_after
            self._target_to_bucket[target_key] = bucket_id
        return bucket_id

    async def wait_if_exhausted(self, target_key: tuple[str, str]) -> float:
        """If the target's last-seen bucket has remaining=0, await until reset.

        Returns the actual sleep duration (0.0 if no wait was needed).
        """
        async with self._lock:
            bucket_id = self._target_to_bucket.get(target_key)
            if not bucket_id:
                return 0.0
            state = self._buckets.get(bucket_id)
            if not state or state.remaining > 0:
                return 0.0
            wait_for = state.reset - time.monotonic()
        if wait_for <= 0:
            return 0.0
        await asyncio.sleep(wait_for)
        return wait_for


# --------------------------------------------------------------- Per-target state

@dataclass
class TargetState:
    """Per-(channel_id, thread_id) state for the poll loop."""
    channel_id: str
    thread_id: str  # '' for channel-level
    registered_at_ms: int
    is_dm: bool = False
    parent_channel_id: Optional[str] = None  # for threads
    consecutive_429: int = 0
    # F5 (iter1 discriminator): track 5xx separately from 429. The previous code
    # incremented consecutive_429 on 5xx, which let a series of 5xx responses
    # falsely open the rate-limit circuit ("5xx must NOT circuit-break" per D16).
    consecutive_5xx: int = 0
    pause_window_sec: int = INITIAL_PAUSE_WINDOW_SEC
    pause_until: float = 0.0  # monotonic time
    half_open: bool = False
    consecutive_capped: int = 0
    last_check_ts: int = 0
    cursor: Optional[str] = None  # last_seen_message_id

    @property
    def key(self) -> tuple[str, str]:
        return (self.channel_id, self.thread_id)


# --------------------------------------------------------------- Poller

class DiscordPoller:
    """Main poller. Holds bot token, gate context, target list, and the loop task.

    Single instance owned by the FastAPI lifespan. Tests construct directly with
    a mocked httpx transport.
    """

    def __init__(
        self,
        token: str,
        access: dict[str, Any],
        http_client: Optional[httpx.AsyncClient] = None,
        global_bucket: Optional[GlobalTokenBucket] = None,
        bucket_registry: Optional[BucketRegistry] = None,
    ):
        # Token is captured here and passed only to httpx as a header value.
        # Never logged, never echoed in errors, never serialized.
        self._token = token
        self._access = access
        self._http = http_client  # may be injected by tests
        self._owns_http = http_client is None
        self._global_bucket = global_bucket or GlobalTokenBucket()
        self._bucket_registry = bucket_registry or BucketRegistry()
        self._gate_ctx: Optional[GateContext] = None
        self._targets: dict[tuple[str, str], TargetState] = {}
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._poll_event = asyncio.Event()  # for tests to trigger one cycle
        self._cycle_count = 0  # for tests
        self._bot_user_id: Optional[str] = None

    # ------------------------------------------------- HTTP plumbing

    def _make_client(self) -> httpx.AsyncClient:
        # Authorization header — token lives only in this dict.
        headers = {
            "Authorization": f"Bot {self._token}",
            "User-Agent": "agent-orchestration-broker (broker, M2)",
        }
        return httpx.AsyncClient(headers=headers, timeout=30.0)

    async def _get_http(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = self._make_client()
        return self._http

    # ------------------------------------------------- Centralized Discord REST (F11)

    async def _discord_request(
        self,
        method: str,
        url: str,
        *,
        target_key: Optional[tuple[str, str]] = None,
        max_429_retries: int = 1,
        **kwargs: Any,
    ) -> Optional[httpx.Response]:
        """Centralized Discord REST helper. ALL Discord REST calls in this
        module MUST go through this method — raw `http.get/post(...)` to
        discord.com/api is forbidden (F11 iter2 discriminator).

        Per D16 § (d).4: every request consumes 1 global token.
        Per D16 § (d).1: honor X-RateLimit-Remaining=0 by waiting until reset.
        Per D16 § (d).2: on 429, sleep max(body.retry_after, header Retry-After).

        Behavior:
          - Acquires 1 global bucket token (await self._global_bucket.acquire(1)).
          - If `target_key` is provided AND the registry has remaining=0 for it,
            awaits reset before issuing the request.
          - Issues the httpx request.
          - Feeds response headers into the BucketRegistry for future waits.
          - On 429, increments the metric, sleeps per Retry-After, and either
            returns the 429 response (max_429_retries=0) or retries (default
            once). Circuit-break / 5xx-counter is the poll loop's responsibility
            and does NOT apply to ancillary calls.
          - On httpx.HTTPError, logs warning + returns None (matches pre-fix
            ancillary semantics).

        Returns the httpx.Response (status may be 200/4xx/5xx/429) or None on
        transport error.
        """
        # Use a synthetic per-route key when caller didn't provide a target;
        # this lets the BucketRegistry still cache per-route exhaustion state
        # for ancillary endpoints.
        effective_key = target_key
        http = await self._get_http()
        attempts_429 = 0
        while True:
            await self._global_bucket.acquire(1)
            if effective_key is not None:
                await self._bucket_registry.wait_if_exhausted(effective_key)
            try:
                resp = await http.request(method, url, **kwargs)
            except httpx.HTTPError as e:
                # F-CC21 A3: full traceback (was type(e).__name__ only).
                logger.warning("discord_request http error %s %s: %s",
                               method, _route_label(url), e, exc_info=True)
                return None
            if effective_key is not None:
                await self._bucket_registry.update_from_response(effective_key, resp.headers)
            if resp.status_code != 429 or attempts_429 >= max_429_retries:
                return resp
            # 429 — honor Retry-After (max of body + header), then retry once.
            _inc("discord_429_total")
            try:
                body = resp.json()
                body_retry = float(body.get("retry_after", 0) or 0)
            except (ValueError, json.JSONDecodeError):
                body_retry = 0.0
            try:
                header_retry = float(resp.headers.get("Retry-After", "0") or 0)
            except ValueError:
                header_retry = 0.0
            sleep_for = max(body_retry, header_retry)
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
            attempts_429 += 1

    # ------------------------------------------------- Bot user id (GET /users/@me)

    async def fetch_bot_user_id(self) -> Optional[str]:
        """Fetch bot user id for mention checks. Returns None on failure."""
        # F11 (iter2): goes through _discord_request so it consumes global
        # token + honors per-route bucket headers.
        resp = await self._discord_request(
            "GET",
            f"{DISCORD_API_BASE}/users/@me",
            target_key=("bot_user", ""),
        )
        if resp is None:
            return None
        if resp.status_code == 200:
            try:
                return resp.json().get("id")
            except (ValueError, json.JSONDecodeError):
                return None
        return None

    # ------------------------------------------------- Bootstrap from access.json

    async def bootstrap_targets(self) -> None:
        """Per D16 § (a) tier 1: read access.json on startup; each groups{} key
        becomes a guild-channel poll target. DM targets derive from allowFrom[].

        DM channel resolution is LAZY (open via POST /users/@me/channels) — for
        M2, we skip lazy DM-open and only pick up DM channels if they're already
        present in access.allowFrom AND we successfully open them. If open fails,
        log and continue.

        F10 (iter2 discriminator): TargetState.registered_at_ms MUST come from
        the persisted `discord_poll_targets.registered_at` row when one exists,
        not from the current startup time. Otherwise the snowflake first-poll
        filter (msg_ts < registered_at_ms) drops legitimate messages received
        during broker downtime — silent message loss after every restart.
        Only use now_ms() for genuinely new bootstrap rows; persist immediately
        so the next restart sees the original timestamp.
        """
        access = self._access
        groups = access.get("groups") or {}
        now = _srv.now_ms()
        # Build gate context with bot user id (if available)
        self._bot_user_id = await self.fetch_bot_user_id()
        self._gate_ctx = _build_gate_context(access, self._bot_user_id)
        # F10: pre-read existing access_json rows so we can preserve their
        # original registered_at across restart instead of overwriting with `now`.
        existing_registered_at = await self._read_existing_registered_at(
            list(groups.keys()), source="access_json", thread_id="",
        )
        # Insert/upsert bootstrap rows for guild-channel targets.
        for channel_id in groups:
            stored = existing_registered_at.get(channel_id)
            registered_at = stored if stored is not None else now
            await self._upsert_target_row(channel_id, "", "access_json", registered_at)
            self._targets[(channel_id, "")] = TargetState(
                channel_id=channel_id,
                thread_id="",
                registered_at_ms=registered_at,
                is_dm=False,
            )
        # DM targets — opened lazily; if HTTP open fails, skip (no crash).
        # F3 (iter1): if dmPolicy='disabled' there is no point opening DM
        # channels — every message would be dropped at the gate anyway, and
        # the DM-open requests would burn rate-limit quota for nothing.
        if access.get("dmPolicy") != "disabled":
            for user_id in access.get("allowFrom") or []:
                dm_channel_id = await self._open_dm_channel(user_id)
                if dm_channel_id:
                    # F10: same restart-preservation logic for DM rows.
                    existing_dm = await self._read_existing_registered_at(
                        [dm_channel_id], source="access_json", thread_id="",
                    )
                    stored_dm = existing_dm.get(dm_channel_id)
                    registered_at_dm = stored_dm if stored_dm is not None else now
                    await self._upsert_target_row(
                        dm_channel_id, "", "access_json", registered_at_dm,
                    )
                    self._targets[(dm_channel_id, "")] = TargetState(
                        channel_id=dm_channel_id,
                        thread_id="",
                        registered_at_ms=registered_at_dm,
                        is_dm=True,
                    )
        # Also load any pre-existing runtime targets from DB.
        await self._load_runtime_targets()
        # Also restore cursors so we resume from prior broker run.
        await self._restore_cursors()

    async def _read_existing_registered_at(
        self, channel_ids: list[str], source: str, thread_id: str,
    ) -> dict[str, int]:
        """F10: bulk-read discord_poll_targets.registered_at for the given
        channel_ids. Returns { channel_id: registered_at } for rows that exist;
        absent rows are omitted (caller falls back to now_ms()).
        """
        if not channel_ids:
            return {}
        out: dict[str, int] = {}
        # Use a parameterized IN clause; SQLite's max ?-args is 999 so this is
        # fine for any realistic group/allow_from list size.
        placeholders = ",".join("?" for _ in channel_ids)
        sql = (
            f"SELECT channel_id, registered_at FROM discord_poll_targets "
            f"WHERE source = ? AND thread_id = ? AND channel_id IN ({placeholders})"
        )
        async with _srv.connect_db() as conn:
            cur = await conn.execute(sql, (source, thread_id, *channel_ids))
            rows = await cur.fetchall()
            await cur.close()
        for r in rows:
            out[r["channel_id"]] = r["registered_at"]
        return out

    async def _open_dm_channel(self, user_id: str) -> Optional[str]:
        # F11 (iter2): goes through _discord_request so it consumes global
        # token + honors per-route bucket headers + 429 backoff.
        resp = await self._discord_request(
            "POST",
            f"{DISCORD_API_BASE}/users/@me/channels",
            target_key=("dm_channels", ""),
            json={"recipient_id": user_id},
        )
        if resp is None:
            return None
        if resp.status_code in (200, 201):
            try:
                return resp.json().get("id")
            except (ValueError, json.JSONDecodeError):
                return None
        # Don't log the user_id at WARN — keep terse.
        logger.warning("open_dm_channel: status=%s", resp.status_code)
        return None

    # ------------------------------------------------- Registry CRUD (DB)

    async def _upsert_target_row(
        self, channel_id: str, thread_id: str, source: str, registered_at_ms: int,
    ) -> None:
        async def _do():
            async with _srv.connect_db() as conn:
                await conn.execute("BEGIN IMMEDIATE")
                try:
                    await conn.execute(
                        "INSERT INTO discord_poll_targets "
                        "(channel_id, thread_id, registered_at, source, last_polled_ts) "
                        "VALUES (?, ?, ?, ?, NULL) "
                        "ON CONFLICT(channel_id, thread_id) DO UPDATE SET "
                        " source=excluded.source",
                        (channel_id, thread_id, registered_at_ms, source),
                    )
                    await conn.commit()
                except Exception:
                    await conn.rollback()
                    raise
        await _srv.with_retry(_do)

    async def _load_runtime_targets(self) -> None:
        """Load runtime_register rows from DB into in-memory targets dict."""
        async with _srv.connect_db() as conn:
            cur = await conn.execute(
                "SELECT channel_id, thread_id, registered_at FROM discord_poll_targets "
                "WHERE source = 'runtime_register'"
            )
            rows = await cur.fetchall()
            await cur.close()
        for r in rows:
            key = (r["channel_id"], r["thread_id"])
            if key not in self._targets:
                # Threads vs channels: if thread_id != '', treat as thread; parent
                # channel id is unknown without a Discord fetch. We'll resolve
                # parent on first poll cycle (we use thread id as both target +
                # bookkeeping, and look up parent via Discord's GET /channels).
                self._targets[key] = TargetState(
                    channel_id=r["channel_id"],
                    thread_id=r["thread_id"],
                    registered_at_ms=r["registered_at"],
                    is_dm=False,
                )

    async def _restore_cursors(self) -> None:
        """Load discord_cursors rows into in-memory TargetState.

        F10 (iter2): the empty-string `last_seen_message_id` sentinel — written
        by _persist_check_ts when a target polled successfully but never saw a
        message id — must be translated back to TargetState.cursor=None so the
        next poll uses the limit=100 first-poll path. last_check_ts is always
        restored verbatim so the catch-up window math is accurate after restart.
        """
        async with _srv.connect_db() as conn:
            cur = await conn.execute(
                "SELECT channel_id, thread_id, last_seen_message_id, last_check_ts "
                "FROM discord_cursors"
            )
            rows = await cur.fetchall()
            await cur.close()
        for r in rows:
            key = (r["channel_id"], r["thread_id"])
            if key in self._targets:
                # F10: '' sentinel → cursor stays None (next poll = first-poll
                # path). Real message ids are non-empty snowflake strings.
                last_id = r["last_seen_message_id"] or None
                self._targets[key].cursor = last_id
                self._targets[key].last_check_ts = r["last_check_ts"]

    # ------------------------------------------------- Public registry API (called from server.py endpoints)

    async def register_target(self, channel_id: str, thread_id: str = "") -> bool:
        """Register a runtime poll target. Returns True if newly registered or
        upserted. In-memory + DB; takes effect on next poll cycle.
        """
        now = _srv.now_ms()
        await self._upsert_target_row(channel_id, thread_id, "runtime_register", now)
        key = (channel_id, thread_id)
        if key not in self._targets:
            self._targets[key] = TargetState(
                channel_id=channel_id,
                thread_id=thread_id,
                registered_at_ms=now,
                is_dm=False,
            )
        return True

    async def unregister_target(self, channel_id: str, thread_id: str = "") -> bool:
        """Idempotent delete. Returns True if a row was deleted, False if absent."""
        async def _do():
            async with _srv.connect_db() as conn:
                await conn.execute("BEGIN IMMEDIATE")
                try:
                    cur = await conn.execute(
                        "DELETE FROM discord_poll_targets "
                        "WHERE channel_id = ? AND thread_id = ?",
                        (channel_id, thread_id),
                    )
                    deleted = cur.rowcount or 0
                    await cur.close()
                    await conn.commit()
                    return deleted
                except Exception:
                    await conn.rollback()
                    raise
        deleted = await _srv.with_retry(_do)
        # Remove from in-memory targets too — next loop iteration won't poll.
        self._targets.pop((channel_id, thread_id), None)
        return deleted > 0

    async def list_targets(self) -> list[dict[str, Any]]:
        async with _srv.connect_db() as conn:
            cur = await conn.execute(
                "SELECT channel_id, thread_id, registered_at, source, last_polled_ts "
                "FROM discord_poll_targets ORDER BY registered_at, channel_id, thread_id"
            )
            rows = [dict(r) for r in await cur.fetchall()]
            await cur.close()
        return rows

    # ------------------------------------------------- Poll loop

    async def start(self) -> None:
        """Spawn the poll loop as a background task."""
        await self.bootstrap_targets()
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        """Cancel the loop task and close http client (if owned)."""
        self._stop_event.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        if self._owns_http and self._http is not None:
            await self._http.aclose()

    async def _loop(self) -> None:
        """Main loop: per cycle, poll each non-paused target concurrently then sleep
        15s + jitter. Honors stop_event for clean shutdown.
        """
        while not self._stop_event.is_set():
            try:
                await self.poll_once()
            except asyncio.CancelledError:
                return
            except Exception as e:
                # F-CC21 A3 (PRIMARY MASKING FIX): was type(e).__name__ only —
                # this single line hid every exception class from the broker's
                # most prolific background task. Per smoking-gun #1 in F-CC21.
                logger.warning("poll_once raised: %s", e, exc_info=True)
            # Cadence: 15s + jitter [-3, +3]
            delay = POLL_INTERVAL_SEC + random.uniform(-POLL_JITTER_SEC, POLL_JITTER_SEC)
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
                return  # stop signalled
            except asyncio.TimeoutError:
                pass

    async def poll_once(self) -> None:
        """Execute one poll cycle across all registered targets concurrently.

        Test entrypoint: tests call poll_once() directly to exercise one cycle
        without the cadence sleep.
        """
        self._cycle_count += 1
        _inc("discord_polls_total", 0)  # ensure key exists
        targets = list(self._targets.values())
        if not targets:
            return
        now_mono = time.monotonic()

        async def _poll_one(t: TargetState):
            # Pause check — circuit-break window
            if t.pause_until > now_mono and not t.half_open:
                return
            # If the pause window has expired, transition to half-open
            if t.pause_until and t.pause_until <= now_mono and not t.half_open:
                t.half_open = True
            await self._poll_target(t)

        await asyncio.gather(*(_poll_one(t) for t in targets), return_exceptions=True)

    # ------------------------------------------------- Per-target poll

    async def _poll_target(self, t: TargetState) -> None:
        await self._global_bucket.acquire(1)
        # F4 (iter1 discriminator): if Discord told us this target's bucket is
        # exhausted (X-RateLimit-Remaining=0 with future reset) we MUST wait
        # before issuing another request. Hitting it early would just earn a
        # 429 and burn quota.
        await self._bucket_registry.wait_if_exhausted(t.key)
        http = await self._get_http()
        # Build URL — half-open uses limit=1 probe, normal uses limit=100.
        is_first_poll = t.cursor is None
        if t.half_open:
            url, params = self._build_messages_url(t, probe=True)
        else:
            url, params = self._build_messages_url(t, probe=False)
        try:
            resp = await http.get(url, params=params)
        except httpx.HTTPError as e:
            # F-CC21 A3: full traceback (was type(e).__name__ only).
            logger.warning("poll http error on channel %s: %s", t.channel_id, e, exc_info=True)
            return

        _inc("discord_polls_total")

        # F4: feed every response (200/429/5xx) into the bucket registry — Discord
        # ships X-RateLimit-* on success too so we know how close we are to the cap.
        await self._bucket_registry.update_from_response(t.key, resp.headers)

        if resp.status_code == 429:
            await self._handle_429(t, resp)
            return
        if 500 <= resp.status_code < 600:
            await self._handle_5xx(t)
            return
        if resp.status_code != 200:
            # 401/403/404 → log + drop; don't attempt circuit
            logger.warning("poll status=%s on channel %s", resp.status_code, t.channel_id)
            return

        # Success path
        if t.half_open:
            # Probe success — close circuit
            t.half_open = False
            t.consecutive_429 = 0
            t.consecutive_5xx = 0  # F5: reset 5xx counter on any 200
            t.consecutive_capped = 0
            t.pause_window_sec = INITIAL_PAUSE_WINDOW_SEC
            t.pause_until = 0.0
            broker_metrics["discord_rate_limit_circuit_open"][t.channel_id] = False
            broker_metrics["discord_consecutive_capped_failures"][t.channel_id] = 0
            # half-open probe was limit=1; don't process its body — wait for
            # next normal cycle to do the full read. This avoids gap-bookkeeping
            # complications.
            return

        # Reset 429 counter on any 200
        t.consecutive_429 = 0
        t.consecutive_5xx = 0  # F5: also reset 5xx counter on any successful poll

        try:
            messages = resp.json()
        except (ValueError, json.JSONDecodeError):
            messages = []

        # Catch-up truncation check per D16 § (e)
        now = _srv.now_ms()
        if (
            t.last_check_ts > 0
            and (now - t.last_check_ts) > CATCHUP_WINDOW_MS
            and len(messages) >= 100
        ):
            await self._emit_error(
                t,
                json.dumps({
                    "type": "discord_catchup_truncated",
                    "severity": "warn",
                    "errors": [{
                        "kind": "catchup_window_exceeded",
                        "detail": f"gap may exceed 100 messages on (channel_id={t.channel_id}, thread_id={t.thread_id})",
                        "auto_recovered": False,
                    }],
                    "context": {
                        "channel_id": t.channel_id,
                        "thread_id": t.thread_id,
                        "last_check_ts": t.last_check_ts,
                        "catchup_window_ms": CATCHUP_WINDOW_MS,
                    },
                }),
            )

        # Discord returns newest-first; iterate oldest-first to advance cursor monotonically.
        # On first poll, also drop messages predating registered_at via snowflake decode.
        # Determine parent channel for threads (needed for gate lookup).
        parent_channel_id = t.parent_channel_id
        if t.thread_id and not parent_channel_id:
            parent_channel_id = await self._fetch_thread_parent(t.thread_id)
            t.parent_channel_id = parent_channel_id

        target_channel_for_gate = t.thread_id or t.channel_id
        newest_id_seen: Optional[str] = t.cursor

        for msg in reversed(messages):
            msg_id = str(msg.get("id"))
            # Track newest by snowflake int compare (string compare also works
            # for fixed-length snowflakes, but int is safer).
            if newest_id_seen is None or int(msg_id) > int(newest_id_seen or "0"):
                newest_id_seen = msg_id

            # Bot/webhook/system filter
            if not _is_user_content_message(msg):
                _inc("discord_dropped_total")
                continue

            # First-poll snowflake activation filter
            if is_first_poll:
                msg_ts = decode_snowflake(msg_id)
                if msg_ts < t.registered_at_ms:
                    _inc("discord_dropped_total")
                    continue

            # Gate
            assert self._gate_ctx is not None
            allowed = await gate_message(
                msg,
                target_channel_id=target_channel_for_gate,
                is_dm=t.is_dm,
                parent_channel_id=parent_channel_id,
                ctx=self._gate_ctx,
                http=http,
                discord_request=self._discord_request,  # F11: rate-limit reply lookups
            )
            if not allowed:
                _inc("discord_dropped_total")
                continue

            # PERMISSION_REPLY_RE intercept
            content = msg.get("content", "") or ""
            perm_match = PERMISSION_REPLY_RE.match(content)
            sender_id = (msg.get("author") or {}).get("id", "")
            if perm_match:
                yes_no = perm_match.group(1).lower()
                code = perm_match.group(2).lower()
                behavior = "allow" if yes_no.startswith("y") else "deny"
                payload = json.dumps({"request_id": code, "behavior": behavior})
                await self._insert_and_broadcast(
                    t, kind="permission_reply", content=payload, sender_id=sender_id, msg_id=msg_id,
                )
            else:
                await self._insert_and_broadcast(
                    t, kind="user_msg", content=content, sender_id=sender_id, msg_id=msg_id,
                )

        # Update cursor (atomic per cycle, AFTER processing).
        # F6 (iter1) + F10 (iter2 discriminator): D16 § (e) requires durable
        # last_check_ts on EVERY successful poll, including first-poll-with-
        # zero-messages. Iter-1 only persisted when newest_id_seen existed OR
        # t.cursor was set, so a target's first poll returning zero messages
        # left no row — silent message loss after broker restart because
        # _restore_cursors found nothing and snowflake filter dropped messages
        # received during downtime.
        #
        # Iter-2 fix:
        #   - newest_id_seen present and changed → _persist_cursor (writes
        #     real message id + last_check_ts).
        #   - else → _persist_check_ts (UPSERT: if no row, INSERT with empty
        #     last_seen_message_id sentinel; if row exists, refresh
        #     last_check_ts only).
        if newest_id_seen and newest_id_seen != t.cursor:
            t.cursor = newest_id_seen
            await self._persist_cursor(t)
        else:
            # F10: covers both (a) cursor unchanged with existing row, and
            # (b) first-poll-with-zero-messages where no row exists yet.
            # _persist_check_ts UPSERTs with '' sentinel for case (b).
            await self._persist_check_ts(t)
        t.last_check_ts = now
        await self._update_last_polled(t, now)

    # ------------------------------------------------- HTTP URL builder

    def _build_messages_url(self, t: TargetState, probe: bool) -> tuple[str, dict[str, Any]]:
        """For threads, polling target IS the thread channel; the parent channel id
        is only used for gate lookup. So the URL channel_id is `thread_id` if
        thread, else `channel_id`."""
        target_id = t.thread_id or t.channel_id
        url = f"{DISCORD_API_BASE}/channels/{target_id}/messages"
        params: dict[str, Any] = {}
        if probe:
            params["limit"] = 1
            if t.cursor:
                params["after"] = t.cursor
        elif t.cursor is None:
            # First poll — limit=100 per D16 § (c) initial cursor seeding
            params["limit"] = 100
        else:
            params["after"] = t.cursor
            params["limit"] = 100
        return url, params

    async def _fetch_thread_parent(self, thread_id: str) -> Optional[str]:
        # F11 (iter2): goes through _discord_request so it consumes global
        # token + honors per-route bucket headers + 429 backoff.
        resp = await self._discord_request(
            "GET",
            f"{DISCORD_API_BASE}/channels/{thread_id}",
            target_key=("thread_parent", thread_id),
        )
        if resp is None:
            return None
        if resp.status_code == 200:
            try:
                return resp.json().get("parent_id")
            except (ValueError, json.JSONDecodeError):
                return None
        return None

    # ------------------------------------------------- 429 + 5xx

    async def _handle_429(self, t: TargetState, resp: httpx.Response) -> None:
        _inc("discord_429_total")
        t.consecutive_429 += 1
        # Parse retry_after from body + header; sleep max
        try:
            body = resp.json()
            body_retry = float(body.get("retry_after", 0) or 0)
        except (ValueError, json.JSONDecodeError):
            body_retry = 0.0
        try:
            header_retry = float(resp.headers.get("Retry-After", "0") or 0)
        except ValueError:
            header_retry = 0.0
        sleep_for = max(body_retry, header_retry)

        if t.consecutive_429 >= 2:
            # Exponential backoff: 1.5x with ±20% jitter, capped at 5 attempts before circuit
            backoff_factor = 1.5 ** (t.consecutive_429 - 1)
            jitter = 1.0 + random.uniform(-0.2, 0.2)
            sleep_for = max(sleep_for, backoff_factor * jitter)

        if t.half_open:
            # Half-open probe got 429 — circuit stays open, double pause window
            t.pause_window_sec = min(t.pause_window_sec * 2, MAX_PAUSE_WINDOW_SEC)
            if t.pause_window_sec >= MAX_PAUSE_WINDOW_SEC:
                t.consecutive_capped += 1
                broker_metrics["discord_consecutive_capped_failures"][t.channel_id] = t.consecutive_capped
                if t.consecutive_capped >= PERSISTENT_OPEN_THRESHOLD:
                    await self._emit_error(
                        t,
                        json.dumps({
                            "type": "discord_rate_limit_persistent_open",
                            "severity": "error",
                            "errors": [{
                                "kind": "rate_limit_circuit_persistent",
                                "detail": f"channel_id={t.channel_id} probe_count={t.consecutive_capped}",
                                "auto_recovered": False,
                            }],
                            "context": {
                                "channel_id": t.channel_id,
                                "probe_count": t.consecutive_capped,
                                "pause_window_sec": t.pause_window_sec,
                            },
                        }),
                    )
            t.pause_until = time.monotonic() + t.pause_window_sec
            t.half_open = False
            broker_metrics["discord_rate_limit_circuit_open"][t.channel_id] = True
            return

        if t.consecutive_429 >= MAX_429_ATTEMPTS_BEFORE_CIRCUIT + 1:
            # 6th 429 in a row — open circuit
            t.pause_until = time.monotonic() + t.pause_window_sec
            broker_metrics["discord_rate_limit_circuit_open"][t.channel_id] = True
            return

        # Brief sleep within attempt budget; outer loop retries on next cycle.
        # F4 (iter1 discriminator): D16 § (d).2 requires honoring the FULL
        # max(body retry_after, header Retry-After). The previous code had
        # `min(sleep_for, 5.0)` which would truncate a Discord-mandated
        # 60-second pause to 5 seconds and earn the bot a rate-limit ban.
        await asyncio.sleep(sleep_for)

    async def _handle_5xx(self, t: TargetState) -> None:
        # Transient — exponential backoff. Per D16 § (d) "5xx: do not circuit-break".
        # F5 (iter1 discriminator): use a SEPARATE consecutive_5xx counter so
        # that 5xx storms cannot leak into the 429 counter and falsely open the
        # rate-limit circuit. Also fix half-open 5xx: we set half_open=False
        # AFTER scheduling pause_until so poll_once's pause check (pause_until>now
        # AND not half_open) actually skips the target during the 30s pause.
        if t.half_open:
            # Half-open 5xx — retry probe in 30s, do not advance pause window.
            # Setting half_open=False ensures poll_once skips this target until
            # pause_until expires; on the next cycle (after the 30s pause) the
            # pause-expiry path in poll_once flips half_open back to True so the
            # next attempt is a probe.
            t.pause_until = time.monotonic() + HALF_OPEN_5XX_RETRY_SEC
            t.half_open = False
            return
        t.consecutive_5xx += 1
        backoff_factor = 1.5 ** max(0, t.consecutive_5xx - 1)
        jitter = 1.0 + random.uniform(-0.2, 0.2)
        await asyncio.sleep(min(backoff_factor * jitter, 30.0))

    # ------------------------------------------------- DB write + SSE broadcast

    async def _insert_and_broadcast(
        self,
        t: TargetState,
        kind: str,
        content: str,
        sender_id: str,
        msg_id: str,
    ) -> None:
        ts = _srv.now_ms()
        from_agent = f"{DISCORD_USER_PREFIX}{sender_id}"
        # Channel naming: use Discord channel id with prefix so it's
        # distinguishable from the existing #cc-* channels.
        channel = f"{DISCORD_CHANNEL_PREFIX}{t.channel_id}"
        thread_id = t.thread_id or None

        async def _do():
            async with _srv.connect_db() as conn:
                await conn.execute("BEGIN IMMEDIATE")
                try:
                    cur = await conn.execute(
                        "INSERT INTO message_log "
                        "(client_event_id, from_agent, to_agent, channel, thread_id, kind, content, ts) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (None, from_agent, None, channel, thread_id, kind, content, ts),
                    )
                    event_id = cur.lastrowid
                    await cur.close()
                    await conn.commit()
                    return event_id
                except Exception:
                    await conn.rollback()
                    raise

        event_id = await _srv.with_retry(_do)
        _inc("discord_messages_broadcast_total")
        await _srv.broadcast_event({
            "event_id": event_id,
            "ts": ts,
            "kind": kind,
            "agent": from_agent,
            "data": {
                "to_agent": None,
                "content": content,
                "channel": channel,
                "thread_id": thread_id,
            },
        })

    async def _emit_error(self, t: TargetState, content: str) -> None:
        ts = _srv.now_ms()
        channel = f"{DISCORD_CHANNEL_PREFIX}{t.channel_id}"
        thread_id = t.thread_id or None

        async def _do():
            async with _srv.connect_db() as conn:
                await conn.execute("BEGIN IMMEDIATE")
                try:
                    cur = await conn.execute(
                        "INSERT INTO message_log "
                        "(client_event_id, from_agent, to_agent, channel, thread_id, kind, content, ts) "
                        "VALUES (?, ?, ?, ?, ?, 'error', ?, ?)",
                        (None, DISCORD_BROKER_AGENT, None, channel, thread_id, content, ts),
                    )
                    event_id = cur.lastrowid
                    await cur.close()
                    await conn.commit()
                    return event_id
                except Exception:
                    await conn.rollback()
                    raise

        event_id = await _srv.with_retry(_do)
        await _srv.broadcast_event({
            "event_id": event_id,
            "ts": ts,
            "kind": "error",
            "agent": DISCORD_BROKER_AGENT,
            "data": {
                "to_agent": None,
                "content": content,
                "channel": channel,
                "thread_id": thread_id,
            },
        })

    async def _persist_cursor(self, t: TargetState) -> None:
        async def _do():
            async with _srv.connect_db() as conn:
                await conn.execute("BEGIN IMMEDIATE")
                try:
                    await conn.execute(
                        "INSERT INTO discord_cursors "
                        "(channel_id, thread_id, last_seen_message_id, last_check_ts) "
                        "VALUES (?, ?, ?, ?) "
                        "ON CONFLICT(channel_id, thread_id) DO UPDATE SET "
                        " last_seen_message_id=excluded.last_seen_message_id, "
                        " last_check_ts=excluded.last_check_ts",
                        (t.channel_id, t.thread_id, t.cursor or "", _srv.now_ms()),
                    )
                    await conn.commit()
                except Exception:
                    await conn.rollback()
                    raise

        await _srv.with_retry(_do)

    async def _persist_check_ts(self, t: TargetState) -> None:
        """F6 + F10 (iter2 discriminator): refresh discord_cursors.last_check_ts
        AND ensure a row exists. Per D16 § (e), every successful poll (including
        zero-message polls) must persist a durable last_check_ts so that after
        a broker restart, the catch-up math sees an accurate window.

        Iter-1 made this UPDATE-only, which silently no-op'd when no cursor row
        existed yet (first-poll-with-zero-messages case). Iter-2 UPSERTs with
        an empty-string `last_seen_message_id` sentinel to record "we polled
        successfully but never saw a message id". The empty sentinel is documented
        in schema.sql (DEFAULT '' on thread_id) and consumed by _restore_cursors
        as "no message ever seen → cursor stays None so next poll uses limit=100
        first-poll path".
        """
        async def _do():
            async with _srv.connect_db() as conn:
                await conn.execute("BEGIN IMMEDIATE")
                try:
                    # F10: UPSERT — preserve last_seen_message_id if a row already
                    # exists (we don't want to clobber a real message id with '').
                    # COALESCE keeps the existing value on UPDATE; INSERT writes ''.
                    await conn.execute(
                        "INSERT INTO discord_cursors "
                        "(channel_id, thread_id, last_seen_message_id, last_check_ts) "
                        "VALUES (?, ?, '', ?) "
                        "ON CONFLICT(channel_id, thread_id) DO UPDATE SET "
                        " last_check_ts=excluded.last_check_ts",
                        (t.channel_id, t.thread_id, _srv.now_ms()),
                    )
                    await conn.commit()
                except Exception:
                    await conn.rollback()
                    raise

        await _srv.with_retry(_do)

    async def _update_last_polled(self, t: TargetState, now_ts: int) -> None:
        async def _do():
            async with _srv.connect_db() as conn:
                await conn.execute("BEGIN IMMEDIATE")
                try:
                    await conn.execute(
                        "UPDATE discord_poll_targets SET last_polled_ts = ? "
                        "WHERE channel_id = ? AND thread_id = ?",
                        (now_ts, t.channel_id, t.thread_id),
                    )
                    await conn.commit()
                except Exception:
                    await conn.rollback()
                    raise

        await _srv.with_retry(_do)


# --------------------------------------------------------------- Module-level singleton

_poller_instance: Optional[DiscordPoller] = None


def get_poller() -> Optional[DiscordPoller]:
    """Return the active poller instance (set by lifespan startup) or None."""
    return _poller_instance


def set_poller(poller: Optional[DiscordPoller]) -> None:
    global _poller_instance
    _poller_instance = poller


async def maybe_start_poller() -> Optional[DiscordPoller]:
    """Lifespan helper. Loads token + access; if either missing, log warning and
    return None (poller disabled — broker still serves all other endpoints).

    Returns the started poller on success, None on graceful skip.
    """
    token = _load_env_token()
    if not token:
        logger.warning(
            "discord_poller: DISCORD_BOT_TOKEN not found in env or %s — poller disabled",
            ENV_FILE,
        )
        return None
    access = _load_access()
    if access is None:
        logger.warning(
            "discord_poller: %s missing or unreadable — poller disabled",
            ACCESS_FILE,
        )
        return None
    poller = DiscordPoller(token=token, access=access)
    try:
        await poller.start()
    except Exception as e:
        # Mask token: only log exception class.
        logger.warning("discord_poller: start failed: %s", type(e).__name__)
        return None
    set_poller(poller)
    logger.info("discord_poller: started with %d target(s)", len(poller._targets))
    return poller


async def maybe_stop_poller() -> None:
    p = get_poller()
    if p is not None:
        await p.stop()
        set_poller(None)
