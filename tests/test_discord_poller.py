"""Tests for the M2 broker Discord REST poller (D16 § (a)-(e)).

Mocks Discord REST via httpx.MockTransport. NEVER hits live Discord.

Coverage matrix per task-spec § Tests:
- Snowflake decode (≥2 known vectors)
- Bot/webhook/system-type drops
- Cursor seeding (first poll uses limit=100; subsequent uses ?after=)
- Each gate branch in § (b): dmPolicy disabled/allowlist/pairing,
  groups[] miss, groupAllowFrom miss, requireMention pass+fail, mentionPatterns
- PERMISSION_REPLY_RE: positive (allow + deny), negative (false-positive avoid)
- 429 handling: backoff with jitter, 5-attempt cap, circuit-break on 6th
- Circuit-break recovery: 5→10→20→40→60min progression, half-open success/fail
- Persistent-open escalation after 3×60min caps
- Catch-up truncation (100 results + last_check_ts > 1h ago → kind=error)
- Poll-target endpoint trio: POST/GET/DELETE; idempotent re-POST; DELETE 404
- Lifespan: poller starts, runs ≥1 poll cycle (mocked), stops cleanly
- Graceful degrade: missing .env → broker still starts, no crash
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Optional

import httpx
import pytest
import pytest_asyncio

from broker import discord_poller as dp
from broker import server as srv

# --------------------------------------------------------------- Helpers


# F7 (iter1 discriminator): the previous monkeypatch was
#   `lambda *a, **k: asyncio.sleep(0)`
# but `dp.asyncio` is the same module reference as the test module's `asyncio`,
# so after `monkeypatch.setattr(dp.asyncio, "sleep", lambda...)` the lambda's
# call to `asyncio.sleep(0)` recurses into the patched function. This stub
# avoids invoking any sleep at all — pure no-op coroutine.
async def fast_sleep(*args, **kwargs):
    return None


# F4 (iter1): some new tests need to capture the requested sleep duration
# (to assert that Retry-After=60 is honored, not capped). Use this stub via
# `sleeps = make_sleep_recorder()` and pass `sleeps.stub` to monkeypatch.
class _SleepRecorder:
    def __init__(self):
        self.calls: list[float] = []
    async def stub(self, *args, **kwargs):
        # signature: asyncio.sleep(delay, result=None)
        if args:
            self.calls.append(float(args[0]))
        return None


def make_sleep_recorder() -> _SleepRecorder:
    return _SleepRecorder()


def make_msg(
    msg_id: str,
    author_id: str = "111111111111111111",
    content: str = "hello",
    bot: bool = False,
    msg_type: int = 0,
    webhook_id: Optional[str] = None,
    mentions: Optional[list[dict]] = None,
    referenced_message: Optional[dict] = None,
    message_reference: Optional[dict] = None,
) -> dict:
    msg: dict[str, Any] = {
        "id": msg_id,
        "type": msg_type,
        "content": content,
        "author": {"id": author_id, "username": "u", "bot": bot},
        "mentions": mentions or [],
    }
    if webhook_id:
        msg["webhook_id"] = webhook_id
    if referenced_message:
        msg["referenced_message"] = referenced_message
    if message_reference:
        msg["message_reference"] = message_reference
    return msg


# Snowflake = ((ts_ms - epoch) << 22)
def make_snowflake(ts_ms: int) -> str:
    return str((ts_ms - dp.DISCORD_EPOCH_MS) << 22)


def build_mock_transport(routes: dict[str, Any]):
    """Build an httpx.MockTransport from a route map.

    routes: { (method, path_substring) : handler_fn(request) -> httpx.Response }
    Path matching is substring-based because Discord URLs include channel ids.
    """
    def _h(req: httpx.Request) -> httpx.Response:
        for (method, substr), handler in routes.items():
            if req.method == method and substr in str(req.url):
                if callable(handler):
                    return handler(req)
                return handler
        return httpx.Response(404, json={"message": "no route", "code": 0})
    return httpx.MockTransport(_h)


@pytest_asyncio.fixture
async def schema_db(tmp_path, monkeypatch):
    """Ephemeral coordination.db with schema bootstrapped + reset metrics."""
    db_path = tmp_path / "coordination.db"
    monkeypatch.setenv("BROKER_DB_PATH", str(db_path))
    monkeypatch.setenv("BROKER_HOST", "127.0.0.1")
    monkeypatch.delenv("BROKER_BEARER_TOKEN", raising=False)
    await srv.ensure_schema()
    # Reset metrics so test order doesn't matter
    dp.broker_metrics["discord_dropped_total"] = 0
    dp.broker_metrics["discord_polls_total"] = 0
    dp.broker_metrics["discord_429_total"] = 0
    dp.broker_metrics["discord_messages_broadcast_total"] = 0
    dp.broker_metrics["discord_rate_limit_circuit_open"] = {}
    dp.broker_metrics["discord_consecutive_capped_failures"] = {}
    return str(db_path)


# --------------------------------------------------------------- Snowflake decode

def test_decode_snowflake_discord_epoch_zero():
    """A snowflake of 0 decodes to the Discord epoch."""
    assert dp.decode_snowflake(0) == dp.DISCORD_EPOCH_MS


def test_decode_snowflake_known_vector_string():
    """A synthetic snowflake string decodes to its exact millisecond timestamp."""
    assert dp.decode_snowflake("222222222222222222") == 1_473_052_306_467


def test_decode_snowflake_round_trip_known_ts():
    """Round-trip a known ts → snowflake → ts."""
    ts = 1_725_000_000_000  # arbitrary synthetic timestamp
    sf = (ts - dp.DISCORD_EPOCH_MS) << 22
    assert dp.decode_snowflake(sf) == ts


def test_decode_snowflake_negative_raises():
    with pytest.raises(ValueError):
        dp.decode_snowflake(-1)


# --------------------------------------------------------------- Bot/webhook/system filter

def test_user_content_message_pass_default():
    assert dp._is_user_content_message(make_msg("1", msg_type=0)) is True


def test_user_content_message_pass_reply():
    assert dp._is_user_content_message(make_msg("1", msg_type=19)) is True


def test_user_content_message_drops_bot():
    assert dp._is_user_content_message(make_msg("1", bot=True)) is False


def test_user_content_message_drops_webhook():
    assert dp._is_user_content_message(make_msg("1", webhook_id="wh-123")) is False


def test_user_content_message_drops_system_type_pinned():
    # type 6 = CHANNEL_PINNED_MESSAGE
    assert dp._is_user_content_message(make_msg("1", msg_type=6)) is False


def test_user_content_message_drops_system_type_member_join():
    # type 7 = USER_JOIN / GUILD_MEMBER_JOIN
    assert dp._is_user_content_message(make_msg("1", msg_type=7)) is False


def test_user_content_message_drops_chat_input_command():
    # type 20 = CHAT_INPUT_COMMAND
    assert dp._is_user_content_message(make_msg("1", msg_type=20)) is False


# --------------------------------------------------------------- Gate branches

ALLOWED_USER = "111111111111111111"
GUILD_CH = "222222222222222222"
BOT_USER = "999999999999999999"


def make_access(
    dm_policy: str = "allowlist",
    allow_from: Optional[list[str]] = None,
    groups: Optional[dict] = None,
    mention_patterns: Optional[list[str]] = None,
) -> dict:
    return {
        "dmPolicy": dm_policy,
        "allowFrom": allow_from or [ALLOWED_USER],
        "groups": groups or {GUILD_CH: {"requireMention": True, "allowFrom": [ALLOWED_USER]}},
        "pending": {},
        "mentionPatterns": mention_patterns or [],
    }


@pytest.fixture
def http_no_calls():
    """An httpx client whose calls all 404 — for gate tests that shouldn't fetch."""
    return httpx.AsyncClient(transport=httpx.MockTransport(
        lambda req: httpx.Response(404, json={})
    ))


@pytest.mark.asyncio
async def test_gate_dm_policy_disabled_drops(http_no_calls):
    ctx = dp._build_gate_context(make_access(dm_policy="disabled"), BOT_USER)
    msg = make_msg("1", author_id=ALLOWED_USER)
    out = await dp.gate_message(msg, "ch", is_dm=True, parent_channel_id=None, ctx=ctx, http=http_no_calls)
    assert out is False


@pytest.mark.asyncio
async def test_gate_dm_allowlist_passes(http_no_calls):
    ctx = dp._build_gate_context(make_access(dm_policy="allowlist", allow_from=[ALLOWED_USER]), BOT_USER)
    msg = make_msg("1", author_id=ALLOWED_USER)
    out = await dp.gate_message(msg, "ch", is_dm=True, parent_channel_id=None, ctx=ctx, http=http_no_calls)
    assert out is True


@pytest.mark.asyncio
async def test_gate_dm_allowlist_drops_other_sender(http_no_calls):
    ctx = dp._build_gate_context(make_access(dm_policy="allowlist", allow_from=[ALLOWED_USER]), BOT_USER)
    msg = make_msg("1", author_id="someone-else")
    out = await dp.gate_message(msg, "ch", is_dm=True, parent_channel_id=None, ctx=ctx, http=http_no_calls)
    assert out is False


@pytest.mark.asyncio
async def test_gate_dm_pairing_drops(http_no_calls):
    """Pairing flow is plugin-only; broker drops."""
    ctx = dp._build_gate_context(make_access(dm_policy="pairing"), BOT_USER)
    msg = make_msg("1", author_id=ALLOWED_USER)
    out = await dp.gate_message(msg, "ch", is_dm=True, parent_channel_id=None, ctx=ctx, http=http_no_calls)
    assert out is False


@pytest.mark.asyncio
async def test_gate_guild_groups_miss_drops(http_no_calls):
    ctx = dp._build_gate_context(make_access(groups={}), BOT_USER)
    msg = make_msg("1", author_id=ALLOWED_USER)
    out = await dp.gate_message(msg, GUILD_CH, is_dm=False, parent_channel_id=None, ctx=ctx, http=http_no_calls)
    assert out is False


@pytest.mark.asyncio
async def test_gate_guild_group_allow_from_miss_drops(http_no_calls):
    groups = {GUILD_CH: {"requireMention": False, "allowFrom": [ALLOWED_USER]}}
    ctx = dp._build_gate_context(make_access(groups=groups), BOT_USER)
    msg = make_msg("1", author_id="not-allowed")
    out = await dp.gate_message(msg, GUILD_CH, is_dm=False, parent_channel_id=None, ctx=ctx, http=http_no_calls)
    assert out is False


@pytest.mark.asyncio
async def test_gate_guild_require_mention_passes_with_mention(http_no_calls):
    ctx = dp._build_gate_context(make_access(), BOT_USER)
    msg = make_msg("1", author_id=ALLOWED_USER, content="hi", mentions=[{"id": BOT_USER}])
    out = await dp.gate_message(msg, GUILD_CH, is_dm=False, parent_channel_id=None, ctx=ctx, http=http_no_calls)
    assert out is True


@pytest.mark.asyncio
async def test_gate_guild_require_mention_drops_without_mention(http_no_calls):
    ctx = dp._build_gate_context(make_access(), BOT_USER)
    msg = make_msg("1", author_id=ALLOWED_USER, content="just chatting")
    out = await dp.gate_message(msg, GUILD_CH, is_dm=False, parent_channel_id=None, ctx=ctx, http=http_no_calls)
    assert out is False


@pytest.mark.asyncio
async def test_gate_mention_patterns_match(http_no_calls):
    ctx = dp._build_gate_context(
        make_access(mention_patterns=[r"\bclaude\b"]),
        BOT_USER,
    )
    msg = make_msg("1", author_id=ALLOWED_USER, content="hey claude what's up")
    out = await dp.gate_message(msg, GUILD_CH, is_dm=False, parent_channel_id=None, ctx=ctx, http=http_no_calls)
    assert out is True


# F3 (iter1 discriminator): dmPolicy='disabled' must NOT block guild messages.
@pytest.mark.asyncio
async def test_gate_dm_disabled_still_allows_guild_message(http_no_calls):
    """F3: with dmPolicy='disabled', a guild message that satisfies groups[]
    + requireMention=False must STILL pass gate_message(). The DM policy
    governs DM channels only; guild traffic is policed by groups[] alone."""
    groups = {GUILD_CH: {"requireMention": False, "allowFrom": [ALLOWED_USER]}}
    ctx = dp._build_gate_context(
        make_access(dm_policy="disabled", groups=groups),
        BOT_USER,
    )
    msg = make_msg("1", author_id=ALLOWED_USER, content="hello channel")
    out = await dp.gate_message(
        msg, GUILD_CH, is_dm=False, parent_channel_id=None,
        ctx=ctx, http=http_no_calls,
    )
    assert out is True, (
        "F3 bug: dmPolicy='disabled' is incorrectly blocking guild messages "
        "(should only gate DM messages)"
    )


@pytest.mark.asyncio
async def test_gate_dm_disabled_with_require_mention_passes_with_mention(http_no_calls):
    """F3 corollary: dmPolicy='disabled' + requireMention=True + mention →
    guild message still passes."""
    groups = {GUILD_CH: {"requireMention": True, "allowFrom": [ALLOWED_USER]}}
    ctx = dp._build_gate_context(
        make_access(dm_policy="disabled", groups=groups),
        BOT_USER,
    )
    msg = make_msg("1", author_id=ALLOWED_USER, content="hi", mentions=[{"id": BOT_USER}])
    out = await dp.gate_message(
        msg, GUILD_CH, is_dm=False, parent_channel_id=None,
        ctx=ctx, http=http_no_calls,
    )
    assert out is True


@pytest.mark.asyncio
async def test_gate_dm_disabled_blocks_dm_message(http_no_calls):
    """F3 sanity: dmPolicy='disabled' STILL drops DM messages (the original
    intent — we just stopped over-blocking guild traffic)."""
    ctx = dp._build_gate_context(make_access(dm_policy="disabled"), BOT_USER)
    msg = make_msg("1", author_id=ALLOWED_USER)
    out = await dp.gate_message(
        msg, "dm-channel", is_dm=True, parent_channel_id=None,
        ctx=ctx, http=http_no_calls,
    )
    assert out is False


@pytest.mark.asyncio
async def test_gate_thread_uses_parent_channel():
    """Thread → resolve to parent channel for groups[] lookup."""
    parent = GUILD_CH
    thread_id = "9999999999999999999"
    ctx = dp._build_gate_context(make_access(), BOT_USER)
    msg = make_msg("1", author_id=ALLOWED_USER, mentions=[{"id": BOT_USER}])
    out = await dp.gate_message(msg, thread_id, is_dm=False, parent_channel_id=parent,
                                ctx=ctx, http=httpx.AsyncClient())
    assert out is True


# --------------------------------------------------------------- Reply-to-bot mention

@pytest.mark.asyncio
async def test_reply_to_bot_via_inline_referenced_message():
    ctx = dp._build_gate_context(make_access(), BOT_USER)
    ref = {"id": "abc", "author": {"id": BOT_USER}}
    msg = make_msg("1", author_id=ALLOWED_USER, content="answer", referenced_message=ref)
    http = httpx.AsyncClient()  # never called — inline ref present
    out = await dp.gate_message(msg, GUILD_CH, is_dm=False, parent_channel_id=None, ctx=ctx, http=http)
    assert out is True


@pytest.mark.asyncio
async def test_reply_to_bot_via_fetch():
    ctx = dp._build_gate_context(make_access(), BOT_USER)
    ref = {"message_id": "abc", "channel_id": GUILD_CH}
    msg = make_msg("1", author_id=ALLOWED_USER, content="answer", message_reference=ref)
    transport = httpx.MockTransport(
        lambda req: httpx.Response(200, json={"id": "abc", "author": {"id": BOT_USER}})
    )
    http = httpx.AsyncClient(transport=transport)
    out = await dp.gate_message(msg, GUILD_CH, is_dm=False, parent_channel_id=None, ctx=ctx, http=http)
    assert out is True


# --------------------------------------------------------------- PERMISSION_REPLY_RE

def test_permission_reply_re_match_yes():
    m = dp.PERMISSION_REPLY_RE.match("y abcde")
    assert m
    assert m.group(1).lower() == "y"
    assert m.group(2) == "abcde"


def test_permission_reply_re_match_yes_uppercase():
    m = dp.PERMISSION_REPLY_RE.match("YES KGMNO")
    assert m
    assert m.group(2).lower() == "kgmno"


def test_permission_reply_re_match_no():
    m = dp.PERMISSION_REPLY_RE.match("n abcde")
    assert m
    assert m.group(1).lower() == "n"


def test_permission_reply_re_no_match_letter_l_in_code():
    """Code excludes 'l' (ambiguous with '1')."""
    m = dp.PERMISSION_REPLY_RE.match("y abldd")
    assert m is None


def test_permission_reply_re_no_match_extra_text():
    """Strict: no chatter before/after."""
    m = dp.PERMISSION_REPLY_RE.match("yes abcde please")
    assert m is None


def test_permission_reply_re_no_match_bare_yes():
    m = dp.PERMISSION_REPLY_RE.match("yes")
    assert m is None


def test_permission_reply_re_no_match_4_letter_code():
    m = dp.PERMISSION_REPLY_RE.match("y abcd")
    assert m is None


# --------------------------------------------------------------- Cursor seeding + insert + broadcast

@pytest.mark.asyncio
async def test_first_poll_uses_limit_100_and_seeds_cursor(schema_db):
    """First poll fetches limit=100; subsequent polls use ?after=<cursor>."""
    msg_ts = dp.now_ms() if hasattr(dp, "now_ms") else srv.now_ms()
    sf = make_snowflake(msg_ts)

    fetched_urls: list[str] = []

    def _h(req):
        fetched_urls.append(str(req.url))
        if "/users/@me" in str(req.url):
            return httpx.Response(200, json={"id": BOT_USER})
        if "/messages" in str(req.url):
            # Return one user message (mentions bot)
            m = make_msg(sf, author_id=ALLOWED_USER, content="hi bot", mentions=[{"id": BOT_USER}])
            return httpx.Response(200, json=[m])
        return httpx.Response(404, json={})

    http = httpx.AsyncClient(transport=httpx.MockTransport(_h))
    poller = dp.DiscordPoller(token="test-token-DO-NOT-LOG", access=make_access(), http_client=http)
    await poller.bootstrap_targets()
    # bootstrap registered GUILD_CH
    assert (GUILD_CH, "") in poller._targets

    # First poll
    await poller.poll_once()
    msg_urls = [u for u in fetched_urls if "/messages" in u]
    assert any("limit=100" in u for u in msg_urls), f"expected limit=100 on first poll; got {msg_urls}"
    assert all("after=" not in u for u in msg_urls), f"expected no ?after= on first poll; got {msg_urls}"

    # Second poll — should use ?after=<cursor>
    fetched_urls.clear()
    await poller.poll_once()
    msg_urls = [u for u in fetched_urls if "/messages" in u]
    assert any("after=" in u for u in msg_urls), f"expected ?after= on second poll; got {msg_urls}"


@pytest.mark.asyncio
async def test_first_poll_drops_pre_registration_messages(schema_db):
    """Snowflake decode < registered_at_ms → drop."""
    pre_ts = dp.DISCORD_EPOCH_MS + 1000  # ancient
    sf_old = make_snowflake(pre_ts)

    def _h(req):
        if "/users/@me" in str(req.url):
            return httpx.Response(200, json={"id": BOT_USER})
        if "/messages" in str(req.url):
            m = make_msg(sf_old, author_id=ALLOWED_USER, mentions=[{"id": BOT_USER}])
            return httpx.Response(200, json=[m])
        return httpx.Response(404, json={})

    http = httpx.AsyncClient(transport=httpx.MockTransport(_h))
    poller = dp.DiscordPoller(token="x", access=make_access(), http_client=http)
    await poller.bootstrap_targets()
    await poller.poll_once()
    # Cursor was advanced (newest_id_seen even if dropped) but no message_log row
    async with srv.connect_db() as conn:
        cur = await conn.execute("SELECT COUNT(*) AS n FROM message_log WHERE kind='user_msg'")
        row = await cur.fetchone()
        await cur.close()
    assert row["n"] == 0
    assert dp.broker_metrics["discord_dropped_total"] >= 1


@pytest.mark.asyncio
async def test_poll_inserts_user_msg_and_broadcasts(schema_db):
    """Gate-passed message → message_log row + broadcast_event called."""
    now_ts = srv.now_ms()
    sf = make_snowflake(now_ts + 1000)  # future-ish so passes activation filter

    def _h(req):
        url = str(req.url)
        if "/users/@me/channels" in url:
            return httpx.Response(403, json={"message": "no DM open"})
        if "/users/@me" in url:
            return httpx.Response(200, json={"id": BOT_USER})
        if f"/channels/{GUILD_CH}/messages" in url:
            m = make_msg(sf, author_id=ALLOWED_USER, content="hi @bot", mentions=[{"id": BOT_USER}])
            return httpx.Response(200, json=[m])
        return httpx.Response(404, json={})

    http = httpx.AsyncClient(transport=httpx.MockTransport(_h))
    poller = dp.DiscordPoller(token="x", access=make_access(), http_client=http)
    await poller.bootstrap_targets()

    broadcast_calls: list[dict] = []

    async def _capture(ev):
        broadcast_calls.append(ev)

    orig = srv.broadcast_event
    srv.broadcast_event = _capture
    try:
        await poller.poll_once()
    finally:
        srv.broadcast_event = orig

    async with srv.connect_db() as conn:
        cur = await conn.execute(
            "SELECT kind, content, from_agent, channel FROM message_log WHERE kind='user_msg'"
        )
        rows = list(await cur.fetchall())
        await cur.close()
    assert len(rows) == 1
    assert rows[0]["from_agent"] == f"user:{ALLOWED_USER}"
    assert rows[0]["channel"] == f"#discord:{GUILD_CH}"
    assert "hi @bot" in rows[0]["content"]
    assert any(c["kind"] == "user_msg" for c in broadcast_calls)


@pytest.mark.asyncio
async def test_poll_inserts_permission_reply(schema_db):
    """PERMISSION_REPLY_RE match → kind='permission_reply' + JSON payload."""
    now_ts = srv.now_ms()
    sf = make_snowflake(now_ts + 1000)

    def _h(req):
        url = str(req.url)
        if "/users/@me/channels" in url:
            return httpx.Response(403, json={})
        if "/users/@me" in url:
            return httpx.Response(200, json={"id": BOT_USER})
        if f"/channels/{GUILD_CH}/messages" in url:
            m = make_msg(sf, author_id=ALLOWED_USER, content="y abcde", mentions=[{"id": BOT_USER}])
            return httpx.Response(200, json=[m])
        return httpx.Response(404, json={})

    http = httpx.AsyncClient(transport=httpx.MockTransport(_h))
    poller = dp.DiscordPoller(token="x", access=make_access(), http_client=http)
    await poller.bootstrap_targets()
    await poller.poll_once()

    async with srv.connect_db() as conn:
        cur = await conn.execute(
            "SELECT kind, content FROM message_log WHERE kind='permission_reply'"
        )
        rows = list(await cur.fetchall())
        await cur.close()
    assert len(rows) == 1
    payload = json.loads(rows[0]["content"])
    assert payload["request_id"] == "abcde"
    assert payload["behavior"] == "allow"


@pytest.mark.asyncio
async def test_poll_permission_reply_deny(schema_db):
    now_ts = srv.now_ms()
    sf = make_snowflake(now_ts + 1000)

    def _h(req):
        url = str(req.url)
        if "/users/@me/channels" in url:
            return httpx.Response(403, json={})
        if "/users/@me" in url:
            return httpx.Response(200, json={"id": BOT_USER})
        if f"/channels/{GUILD_CH}/messages" in url:
            m = make_msg(sf, author_id=ALLOWED_USER, content="no kgmno", mentions=[{"id": BOT_USER}])
            return httpx.Response(200, json=[m])
        return httpx.Response(404, json={})

    http = httpx.AsyncClient(transport=httpx.MockTransport(_h))
    poller = dp.DiscordPoller(token="x", access=make_access(), http_client=http)
    await poller.bootstrap_targets()
    await poller.poll_once()
    async with srv.connect_db() as conn:
        cur = await conn.execute("SELECT content FROM message_log WHERE kind='permission_reply'")
        rows = list(await cur.fetchall())
        await cur.close()
    assert len(rows) == 1
    payload = json.loads(rows[0]["content"])
    assert payload["behavior"] == "deny"
    assert payload["request_id"] == "kgmno"


@pytest.mark.asyncio
async def test_poll_drops_bot_author_message(schema_db):
    """Our own bot's reply echoes via REST should drop."""
    now_ts = srv.now_ms()
    sf = make_snowflake(now_ts + 1000)

    def _h(req):
        if "/users/@me" in str(req.url):
            return httpx.Response(200, json={"id": BOT_USER})
        if "/messages" in str(req.url):
            # bot=True → drop
            m = make_msg(sf, author_id="some-bot", content="echo", bot=True)
            return httpx.Response(200, json=[m])
        return httpx.Response(404, json={})

    http = httpx.AsyncClient(transport=httpx.MockTransport(_h))
    poller = dp.DiscordPoller(token="x", access=make_access(), http_client=http)
    await poller.bootstrap_targets()
    await poller.poll_once()
    async with srv.connect_db() as conn:
        cur = await conn.execute("SELECT COUNT(*) AS n FROM message_log WHERE kind='user_msg'")
        row = await cur.fetchone()
        await cur.close()
    assert row["n"] == 0


# --------------------------------------------------------------- 429 + circuit-break

@pytest.mark.asyncio
async def test_429_increments_counter_and_sleeps(schema_db, monkeypatch):
    """Single 429 increments consecutive_429 + sleeps; doesn't open circuit yet."""
    # Avoid actual sleep
    monkeypatch.setattr(dp.asyncio, "sleep", fast_sleep)

    def _h(req):
        if "/users/@me" in str(req.url):
            return httpx.Response(200, json={"id": BOT_USER})
        if "/messages" in str(req.url):
            return httpx.Response(429,
                                  headers={"Retry-After": "0.01"},
                                  json={"retry_after": 0.01, "global": False})
        return httpx.Response(404, json={})

    http = httpx.AsyncClient(transport=httpx.MockTransport(_h))
    poller = dp.DiscordPoller(token="x", access=make_access(), http_client=http)
    await poller.bootstrap_targets()
    await poller.poll_once()
    target = poller._targets[(GUILD_CH, "")]
    assert target.consecutive_429 == 1
    # circuit not open yet
    assert not dp.broker_metrics["discord_rate_limit_circuit_open"].get(GUILD_CH, False)


@pytest.mark.asyncio
async def test_429_circuit_opens_on_6th_attempt(schema_db, monkeypatch):
    """6 consecutive 429s open the circuit and pause polling."""
    monkeypatch.setattr(dp.asyncio, "sleep", fast_sleep)

    def _h(req):
        if "/users/@me" in str(req.url):
            return httpx.Response(200, json={"id": BOT_USER})
        if "/messages" in str(req.url):
            return httpx.Response(429, headers={"Retry-After": "0.01"},
                                  json={"retry_after": 0.01})
        return httpx.Response(404, json={})

    http = httpx.AsyncClient(transport=httpx.MockTransport(_h))
    poller = dp.DiscordPoller(token="x", access=make_access(), http_client=http)
    await poller.bootstrap_targets()
    target = poller._targets[(GUILD_CH, "")]
    # Drive 6 consecutive 429s
    for _ in range(6):
        # Reset pause_until between iterations to allow the next poll
        target.pause_until = 0.0
        await poller.poll_once()
    assert dp.broker_metrics["discord_rate_limit_circuit_open"].get(GUILD_CH) is True
    assert target.pause_until > 0


@pytest.mark.asyncio
async def test_circuit_break_pause_window_progression(schema_db, monkeypatch):
    """Half-open 429 → double pause_window: 5→10→20→40→60min CAP."""
    monkeypatch.setattr(dp.asyncio, "sleep", fast_sleep)

    def _h(req):
        if "/users/@me" in str(req.url):
            return httpx.Response(200, json={"id": BOT_USER})
        return httpx.Response(429, json={"retry_after": 0.01})

    http = httpx.AsyncClient(transport=httpx.MockTransport(_h))
    poller = dp.DiscordPoller(token="x", access=make_access(), http_client=http)
    await poller.bootstrap_targets()
    target = poller._targets[(GUILD_CH, "")]
    # Manually simulate half-open + 429 several times
    target.consecutive_429 = 6
    target.pause_window_sec = dp.INITIAL_PAUSE_WINDOW_SEC
    progression = []
    for _ in range(6):
        target.half_open = True
        await poller._handle_429(target, httpx.Response(429, json={"retry_after": 0.0}))
        progression.append(target.pause_window_sec)
    # Expect doubling but cap at 3600
    assert progression[0] == 600
    assert progression[1] == 1200
    assert progression[2] == 2400
    assert progression[3] == 3600  # cap
    assert progression[4] == 3600  # capped
    assert progression[5] == 3600  # capped


@pytest.mark.asyncio
async def test_persistent_open_escalation_after_3_caps(schema_db, monkeypatch):
    """3 consecutive 60-min caps → escalate (kind=error message_log row)."""
    monkeypatch.setattr(dp.asyncio, "sleep", fast_sleep)

    http = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(429, json={})))
    poller = dp.DiscordPoller(token="x", access=make_access(), http_client=http)
    poller._gate_ctx = dp._build_gate_context(make_access(), BOT_USER)
    poller._targets[(GUILD_CH, "")] = dp.TargetState(
        channel_id=GUILD_CH, thread_id="", registered_at_ms=srv.now_ms(),
    )
    target = poller._targets[(GUILD_CH, "")]
    target.pause_window_sec = dp.MAX_PAUSE_WINDOW_SEC  # already at cap
    # Hit cap 3x via half-open 429s
    for _ in range(3):
        target.half_open = True
        await poller._handle_429(target, httpx.Response(429, json={"retry_after": 0.0}))
    # Should have inserted an error row (v1.7 envelope: type='discord_rate_limit_persistent_open')
    async with srv.connect_db() as conn:
        cur = await conn.execute(
            "SELECT content FROM message_log WHERE kind='error' AND content LIKE '%discord_rate_limit_persistent_open%'"
        )
        rows = list(await cur.fetchall())
        await cur.close()
    assert len(rows) >= 1
    # v1.7: content is now an envelope; verify it parses + has the right type
    import json as _json
    env = _json.loads(rows[0][0])
    assert env["type"] == "discord_rate_limit_persistent_open"
    assert env["severity"] == "error"
    assert isinstance(env["errors"], list) and len(env["errors"]) >= 1
    assert env["errors"][0]["kind"] == "rate_limit_circuit_persistent"


@pytest.mark.asyncio
async def test_circuit_break_recovery_on_200_probe(schema_db, monkeypatch):
    """Half-open 200 → close circuit, reset pause_window, reset 429 counter."""
    monkeypatch.setattr(dp.asyncio, "sleep", fast_sleep)

    def _h(req):
        if "/users/@me" in str(req.url):
            return httpx.Response(200, json={"id": BOT_USER})
        if "/messages" in str(req.url):
            return httpx.Response(200, json=[])  # empty success
        return httpx.Response(404, json={})

    http = httpx.AsyncClient(transport=httpx.MockTransport(_h))
    poller = dp.DiscordPoller(token="x", access=make_access(), http_client=http)
    await poller.bootstrap_targets()
    target = poller._targets[(GUILD_CH, "")]
    # Simulate a circuit-open state ready to half-open
    target.consecutive_429 = 6
    target.pause_window_sec = 1200
    target.consecutive_capped = 2
    target.half_open = True
    target.pause_until = 0.0  # already elapsed
    dp.broker_metrics["discord_rate_limit_circuit_open"][GUILD_CH] = True
    await poller.poll_once()
    assert dp.broker_metrics["discord_rate_limit_circuit_open"][GUILD_CH] is False
    assert target.pause_window_sec == dp.INITIAL_PAUSE_WINDOW_SEC
    assert target.consecutive_429 == 0
    assert target.consecutive_capped == 0


# F4 (iter1 discriminator): per-bucket rate-limit registry + uncap Retry-After.

@pytest.mark.asyncio
async def test_bucket_registry_records_headers_from_response():
    """F4: BucketRegistry.update_from_response parses X-RateLimit-* headers."""
    reg = dp.BucketRegistry()
    headers = {
        "X-RateLimit-Bucket": "abc123",
        "X-RateLimit-Limit": "5",
        "X-RateLimit-Remaining": "0",
        "X-RateLimit-Reset-After": "10",
    }
    bucket_id = await reg.update_from_response(("ch", ""), headers)
    assert bucket_id == "abc123"
    state = reg._buckets["abc123"]
    assert state.limit == 5
    assert state.remaining == 0
    assert state.reset_after == 10


@pytest.mark.asyncio
async def test_bucket_registry_wait_when_exhausted_sleeps_until_reset(monkeypatch):
    """F4: when remaining=0 + reset_after=10, wait_if_exhausted sleeps ~10s.
    Captures asyncio.sleep duration via the recorder stub (does not actually sleep)."""
    sleeps = make_sleep_recorder()
    monkeypatch.setattr(dp.asyncio, "sleep", sleeps.stub)
    reg = dp.BucketRegistry()
    await reg.update_from_response(("ch1", ""), {
        "X-RateLimit-Bucket": "b1",
        "X-RateLimit-Limit": "5",
        "X-RateLimit-Remaining": "0",
        "X-RateLimit-Reset-After": "10",
    })
    await reg.wait_if_exhausted(("ch1", ""))
    assert len(sleeps.calls) == 1
    # Allow tiny wall-time slop between the header parse and the wait call.
    assert 9.5 <= sleeps.calls[0] <= 10.5, (
        f"expected ~10s sleep, got {sleeps.calls[0]}"
    )


@pytest.mark.asyncio
async def test_bucket_registry_no_wait_when_remaining_positive(monkeypatch):
    """F4: when remaining>0, wait_if_exhausted is a no-op."""
    sleeps = make_sleep_recorder()
    monkeypatch.setattr(dp.asyncio, "sleep", sleeps.stub)
    reg = dp.BucketRegistry()
    await reg.update_from_response(("ch1", ""), {
        "X-RateLimit-Bucket": "b1",
        "X-RateLimit-Remaining": "3",
        "X-RateLimit-Reset-After": "10",
    })
    await reg.wait_if_exhausted(("ch1", ""))
    assert sleeps.calls == []


@pytest.mark.asyncio
async def test_bucket_registry_no_wait_when_unknown_target(monkeypatch):
    """F4: a target with no bucket history must not wait."""
    sleeps = make_sleep_recorder()
    monkeypatch.setattr(dp.asyncio, "sleep", sleeps.stub)
    reg = dp.BucketRegistry()
    await reg.wait_if_exhausted(("never-seen", ""))
    assert sleeps.calls == []


@pytest.mark.asyncio
async def test_handle_429_honors_full_retry_after_no_5s_cap(schema_db, monkeypatch):
    """F4 critical: a Discord-mandated Retry-After=60s MUST result in a 60s
    sleep, NOT 5s. The previous `min(sleep_for, 5.0)` would truncate the
    Retry-After and earn a rate-limit ban in production."""
    sleeps = make_sleep_recorder()
    monkeypatch.setattr(dp.asyncio, "sleep", sleeps.stub)

    poller = dp.DiscordPoller(
        token="x", access=make_access(),
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(
            lambda r: httpx.Response(404, json={})
        )),
    )
    poller._gate_ctx = dp._build_gate_context(make_access(), BOT_USER)
    poller._targets[(GUILD_CH, "")] = dp.TargetState(
        channel_id=GUILD_CH, thread_id="", registered_at_ms=srv.now_ms(),
    )
    target = poller._targets[(GUILD_CH, "")]
    # First 429 only (consecutive_429 will become 1 after the call)
    resp = httpx.Response(429,
                          headers={"Retry-After": "60"},
                          json={"retry_after": 60.0})
    await poller._handle_429(target, resp)
    assert sleeps.calls, "_handle_429 must call asyncio.sleep at least once"
    # The recorded sleep duration must be 60 (not 5)
    assert max(sleeps.calls) >= 60.0, (
        f"F4 bug: 60-second Retry-After truncated to {max(sleeps.calls)}s "
        f"(min cap removed)"
    )


@pytest.mark.asyncio
async def test_handle_429_uses_max_of_body_and_header_retry(schema_db, monkeypatch):
    """F4: when body retry_after=2 + header Retry-After=10, sleep MUST be 10
    (the max of the two), not 5."""
    sleeps = make_sleep_recorder()
    monkeypatch.setattr(dp.asyncio, "sleep", sleeps.stub)

    poller = dp.DiscordPoller(token="x", access=make_access())
    poller._gate_ctx = dp._build_gate_context(make_access(), BOT_USER)
    poller._targets[(GUILD_CH, "")] = dp.TargetState(
        channel_id=GUILD_CH, thread_id="", registered_at_ms=srv.now_ms(),
    )
    target = poller._targets[(GUILD_CH, "")]
    resp = httpx.Response(429,
                          headers={"Retry-After": "10"},
                          json={"retry_after": 2.0})
    await poller._handle_429(target, resp)
    assert max(sleeps.calls) >= 10.0


@pytest.mark.asyncio
async def test_poll_target_waits_for_exhausted_bucket(schema_db, monkeypatch):
    """F4 integration: a target whose last-seen bucket is exhausted MUST wait
    before issuing the next request. Asserts wait_if_exhausted is called and
    the recorded sleep matches reset_after."""
    sleeps = make_sleep_recorder()
    monkeypatch.setattr(dp.asyncio, "sleep", sleeps.stub)

    def _h(req):
        if "/users/@me" in str(req.url):
            return httpx.Response(200, json={"id": BOT_USER})
        return httpx.Response(200, json=[])

    http = httpx.AsyncClient(transport=httpx.MockTransport(_h))
    poller = dp.DiscordPoller(token="x", access=make_access(), http_client=http)
    await poller.bootstrap_targets()
    target = poller._targets[(GUILD_CH, "")]

    # Pre-seed the registry with an exhausted bucket for this target
    await poller._bucket_registry.update_from_response(target.key, {
        "X-RateLimit-Bucket": "b1",
        "X-RateLimit-Remaining": "0",
        "X-RateLimit-Reset-After": "7",
    })

    sleeps.calls.clear()
    await poller._poll_target(target)
    # At least one sleep call should be ~7s (the bucket wait)
    assert any(6.5 <= s <= 7.5 for s in sleeps.calls), (
        f"expected a ~7s wait_if_exhausted sleep; recorded sleeps: {sleeps.calls}"
    )


# F5 (iter1 discriminator): 5xx counter separation + half-open 5xx behavior.

@pytest.mark.asyncio
async def test_5xx_does_not_increment_429_counter(schema_db, monkeypatch):
    """F5: 5 consecutive 5xx responses must NOT touch consecutive_429.
    Previously _handle_5xx incremented consecutive_429 (with a TODO comment
    'reuse counter for backoff math') which let 5xx storms falsely trigger
    the rate-limit circuit-breaker."""
    monkeypatch.setattr(dp.asyncio, "sleep", fast_sleep)

    def _h(req):
        if "/users/@me" in str(req.url):
            return httpx.Response(200, json={"id": BOT_USER})
        if "/messages" in str(req.url):
            return httpx.Response(503, json={"error": "service unavailable"})
        return httpx.Response(404, json={})

    http = httpx.AsyncClient(transport=httpx.MockTransport(_h))
    poller = dp.DiscordPoller(token="x", access=make_access(), http_client=http)
    await poller.bootstrap_targets()
    target = poller._targets[(GUILD_CH, "")]
    for _ in range(5):
        target.pause_until = 0.0
        await poller.poll_once()
    assert target.consecutive_429 == 0, (
        f"F5 bug: 5xx incremented consecutive_429 to {target.consecutive_429} "
        f"(should stay 0; only consecutive_5xx should advance)"
    )
    assert target.consecutive_5xx == 5
    # Circuit must NOT be open (5xx never opens the rate-limit circuit per D16).
    assert not dp.broker_metrics["discord_rate_limit_circuit_open"].get(GUILD_CH, False)


@pytest.mark.asyncio
async def test_5xx_storm_then_429_does_not_open_circuit(schema_db, monkeypatch):
    """F5 critical: 5 consecutive 5xx then a single 429 must yield
    consecutive_429 == 1 (NOT 6 — which would falsely open the circuit)."""
    monkeypatch.setattr(dp.asyncio, "sleep", fast_sleep)

    state = {"phase": "5xx"}

    def _h(req):
        if "/users/@me" in str(req.url):
            return httpx.Response(200, json={"id": BOT_USER})
        if "/messages" in str(req.url):
            if state["phase"] == "5xx":
                return httpx.Response(502, json={})
            return httpx.Response(429, headers={"Retry-After": "0.01"},
                                  json={"retry_after": 0.01})
        return httpx.Response(404, json={})

    http = httpx.AsyncClient(transport=httpx.MockTransport(_h))
    poller = dp.DiscordPoller(token="x", access=make_access(), http_client=http)
    await poller.bootstrap_targets()
    target = poller._targets[(GUILD_CH, "")]

    for _ in range(5):
        target.pause_until = 0.0
        await poller.poll_once()
    assert target.consecutive_429 == 0
    assert target.consecutive_5xx == 5

    # Now switch to 429 — only one 429 has occurred, must not be 6th
    state["phase"] = "429"
    target.pause_until = 0.0
    await poller.poll_once()
    assert target.consecutive_429 == 1, (
        f"F5 bug: 429 counter is {target.consecutive_429} after 5 5xx + 1 429; "
        f"should be 1 (5xx must not contribute to the rate-limit circuit)"
    )
    assert not dp.broker_metrics["discord_rate_limit_circuit_open"].get(GUILD_CH, False)


@pytest.mark.asyncio
async def test_5xx_counter_resets_on_200(schema_db, monkeypatch):
    """F5: a successful 200 must reset BOTH consecutive_429 and consecutive_5xx."""
    monkeypatch.setattr(dp.asyncio, "sleep", fast_sleep)

    state = {"phase": "5xx", "calls": 0}

    def _h(req):
        if "/users/@me" in str(req.url):
            return httpx.Response(200, json={"id": BOT_USER})
        if "/messages" in str(req.url):
            state["calls"] += 1
            if state["phase"] == "5xx":
                return httpx.Response(500, json={})
            return httpx.Response(200, json=[])  # empty success
        return httpx.Response(404, json={})

    http = httpx.AsyncClient(transport=httpx.MockTransport(_h))
    poller = dp.DiscordPoller(token="x", access=make_access(), http_client=http)
    await poller.bootstrap_targets()
    target = poller._targets[(GUILD_CH, "")]
    for _ in range(3):
        target.pause_until = 0.0
        await poller.poll_once()
    assert target.consecutive_5xx == 3
    state["phase"] = "200"
    target.pause_until = 0.0
    await poller.poll_once()
    assert target.consecutive_5xx == 0
    assert target.consecutive_429 == 0


@pytest.mark.asyncio
async def test_half_open_5xx_skips_target_during_pause_window(schema_db, monkeypatch):
    """F5: when a half-open probe gets a 5xx, the target MUST be skipped during
    the 30-second pause window. Previously the code left half_open=True so the
    target stayed eligible for polling immediately on the next cycle."""
    monkeypatch.setattr(dp.asyncio, "sleep", fast_sleep)

    poll_count = {"n": 0}

    def _h(req):
        url = str(req.url)
        if "/users/@me/channels" in url:
            # Decline DM-open so bootstrap_targets doesn't add a DM target
            # (which would inflate poll_count below).
            return httpx.Response(403, json={})
        if "/users/@me" in url:
            return httpx.Response(200, json={"id": BOT_USER})
        if "/messages" in url:
            poll_count["n"] += 1
            return httpx.Response(503, json={})
        return httpx.Response(404, json={})

    http = httpx.AsyncClient(transport=httpx.MockTransport(_h))
    poller = dp.DiscordPoller(token="x", access=make_access(), http_client=http)
    await poller.bootstrap_targets()
    # Sanity: only the guild target should be registered
    assert list(poller._targets.keys()) == [(GUILD_CH, "")]
    target = poller._targets[(GUILD_CH, "")]
    # Put target in a half-open state so the 5xx triggers the half-open branch
    target.half_open = True
    target.pause_until = 0.0  # ready to poll
    poll_count["n"] = 0
    await poller.poll_once()
    # The half-open probe was issued (1 call), then _handle_5xx scheduled a 30s pause.
    assert poll_count["n"] == 1
    assert target.pause_until > time.monotonic(), (
        "half-open 5xx must schedule pause_until in the future"
    )
    assert target.half_open is False, (
        "F5 bug: after half-open 5xx, half_open must be False so poll_once "
        "actually skips the target during the pause window (poll_once only "
        "skips when pause_until > now AND not half_open)"
    )

    # Next cycle: target must be SKIPPED (no new HTTP call), because pause_until is in the future.
    poll_count["n"] = 0
    await poller.poll_once()
    assert poll_count["n"] == 0, (
        f"F5 bug: half-open 5xx target was polled during pause window "
        f"(extra calls: {poll_count['n']})"
    )


# --------------------------------------------------------------- F6: cursor durability

@pytest.mark.asyncio
async def test_last_check_ts_updated_on_no_new_message_poll(schema_db, monkeypatch):
    """F6 (iter1 discriminator): D16 uses discord_cursors.last_check_ts for
    restart catch-up window calculations. A successful poll that returns 0 new
    messages must STILL update last_check_ts in discord_cursors — otherwise
    after a broker restart the catch-up window math is based on a stale
    timestamp from the last poll that DID return new messages."""

    def _h(req):
        url = str(req.url)
        if "/users/@me/channels" in url:
            return httpx.Response(403, json={})
        if "/users/@me" in url:
            return httpx.Response(200, json={"id": BOT_USER})
        if "/messages" in url:
            return httpx.Response(200, json=[])  # zero new messages
        return httpx.Response(404, json={})

    http = httpx.AsyncClient(transport=httpx.MockTransport(_h))
    poller = dp.DiscordPoller(token="x", access=make_access(), http_client=http)
    await poller.bootstrap_targets()
    target = poller._targets[(GUILD_CH, "")]
    # Seed an existing cursor with a STALE last_check_ts (e.g., 2 hours old).
    stale_ts = srv.now_ms() - 7_200_000
    target.cursor = "1234567890"
    target.last_check_ts = stale_ts
    async with srv.connect_db() as conn:
        await conn.execute(
            "INSERT INTO discord_cursors "
            "(channel_id, thread_id, last_seen_message_id, last_check_ts) "
            "VALUES (?, ?, ?, ?)",
            (GUILD_CH, "", "1234567890", stale_ts),
        )
        await conn.commit()

    poll_started_at_ms = srv.now_ms()
    await poller.poll_once()

    # Read back the row — last_check_ts MUST be advanced past the stale value.
    async with srv.connect_db() as conn:
        cur = await conn.execute(
            "SELECT last_seen_message_id, last_check_ts FROM discord_cursors "
            "WHERE channel_id = ? AND thread_id = ?",
            (GUILD_CH, ""),
        )
        row = await cur.fetchone()
        await cur.close()
    assert row is not None
    assert row["last_seen_message_id"] == "1234567890", (
        "cursor must NOT change when no new messages arrive"
    )
    assert row["last_check_ts"] >= poll_started_at_ms, (
        f"F6 bug: last_check_ts not updated on no-new-message poll. "
        f"Stale={stale_ts}, expected>={poll_started_at_ms}, got={row['last_check_ts']}"
    )


@pytest.mark.asyncio
async def test_first_poll_with_zero_messages_creates_durable_cursor_row(schema_db, monkeypatch):
    """F10 (iter2 discriminator): per D16 § (e), every successful poll —
    INCLUDING first-poll-with-zero-messages — must persist a durable
    discord_cursors row with `last_check_ts=now_ms()`. Without this, after a
    broker restart _restore_cursors finds nothing and the snowflake first-poll
    filter drops legitimate messages received during downtime (silent loss).

    Iter-1 had this assertion INVERTED ('no row should exist'). Iter-2 inverts
    back to the correct behavior: row IS created, with empty-string sentinel
    for `last_seen_message_id` (documented in _persist_check_ts)."""

    def _h(req):
        url = str(req.url)
        if "/users/@me/channels" in url:
            return httpx.Response(403, json={})
        if "/users/@me" in url:
            return httpx.Response(200, json={"id": BOT_USER})
        if "/messages" in url:
            return httpx.Response(200, json=[])
        return httpx.Response(404, json={})

    http = httpx.AsyncClient(transport=httpx.MockTransport(_h))
    poller = dp.DiscordPoller(token="x", access=make_access(), http_client=http)
    await poller.bootstrap_targets()
    # No pre-existing cursor.
    poll_started_at_ms = srv.now_ms()
    await poller.poll_once()
    async with srv.connect_db() as conn:
        cur = await conn.execute(
            "SELECT last_seen_message_id, last_check_ts FROM discord_cursors "
            "WHERE channel_id = ? AND thread_id = ''",
            (GUILD_CH,),
        )
        row = await cur.fetchone()
        await cur.close()
    assert row is not None, (
        "F10: a discord_cursors row MUST be created on first-poll-with-zero-"
        "messages so durable last_check_ts survives broker restart"
    )
    assert row["last_seen_message_id"] == "", (
        f"F10: empty-string sentinel expected for last_seen_message_id when no "
        f"message was ever observed, got {row['last_seen_message_id']!r}"
    )
    assert row["last_check_ts"] >= poll_started_at_ms, (
        f"F10: last_check_ts must be >= poll start time. "
        f"poll_started={poll_started_at_ms}, got={row['last_check_ts']}"
    )


# --------------------------------------------------------------- Catch-up truncation

@pytest.mark.asyncio
async def test_catchup_truncated_emits_error(schema_db, monkeypatch):
    """100 results returned + last_check_ts > 1h ago → kind=error catchup row."""
    now_ts = srv.now_ms()

    def _h(req):
        if "/users/@me" in str(req.url):
            return httpx.Response(200, json={"id": BOT_USER})
        if "/messages" in str(req.url):
            # Return 100 messages all bot-authored so they drop but trigger truncation check
            msgs = [make_msg(make_snowflake(now_ts - 10), author_id="x", bot=True) for _ in range(100)]
            return httpx.Response(200, json=msgs)
        return httpx.Response(404, json={})

    http = httpx.AsyncClient(transport=httpx.MockTransport(_h))
    poller = dp.DiscordPoller(token="x", access=make_access(), http_client=http)
    await poller.bootstrap_targets()
    target = poller._targets[(GUILD_CH, "")]
    # Stale last_check_ts (>1h)
    target.last_check_ts = now_ts - 7200_000  # 2h ago
    target.cursor = "1"  # not first poll
    await poller.poll_once()
    # v1.7: catchup truncation now emits envelope (type='discord_catchup_truncated')
    async with srv.connect_db() as conn:
        cur = await conn.execute(
            "SELECT content FROM message_log WHERE kind='error' AND content LIKE '%discord_catchup_truncated%'"
        )
        rows = list(await cur.fetchall())
        await cur.close()
    assert len(rows) >= 1
    import json as _json
    env = _json.loads(rows[0][0])
    assert env["type"] == "discord_catchup_truncated"
    assert env["severity"] == "warn"
    assert env["errors"][0]["kind"] == "catchup_window_exceeded"


# --------------------------------------------------------------- Endpoint trio

@pytest_asyncio.fixture
async def client(tmp_path, monkeypatch):
    """Mirror conftest.client fixture but local so we can also exercise the
    discord_poller singleton lifecycle."""
    db_path = tmp_path / "coordination.db"
    monkeypatch.setenv("BROKER_DB_PATH", str(db_path))
    monkeypatch.setenv("BROKER_HOST", "127.0.0.1")
    monkeypatch.delenv("BROKER_BEARER_TOKEN", raising=False)
    monkeypatch.delenv("BROKER_ADMIN_ENABLED", raising=False)
    # Critical: ensure no real .env is picked up at lifespan start
    monkeypatch.setenv("DISCORD_STATE_DIR", str(tmp_path / "discord_state"))
    monkeypatch.delenv("DISCORD_BOT_TOKEN", raising=False)
    # Reload module-level constants in discord_poller
    import importlib
    from broker import discord_poller as _dp
    importlib.reload(_dp)
    # Reset poller singleton
    _dp.set_poller(None)

    await srv.ensure_schema()
    transport = httpx.ASGITransport(app=srv.app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
    _dp.set_poller(None)


@pytest.mark.asyncio
async def test_post_poll_target_creates_row(client):
    r = await client.post("/api/discord/poll-targets",
                          json={"channel_id": "12345", "thread_id": "67890"})
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["channel_id"] == "12345"
    assert data["thread_id"] == "67890"
    assert data["registered"] is True


@pytest.mark.asyncio
async def test_post_poll_target_thread_id_omitted_normalizes_to_empty(client):
    r = await client.post("/api/discord/poll-targets", json={"channel_id": "98765"})
    assert r.status_code == 200, r.text
    assert r.json()["thread_id"] == ""


@pytest.mark.asyncio
async def test_post_poll_target_idempotent_re_post(client):
    r1 = await client.post("/api/discord/poll-targets",
                           json={"channel_id": "1", "thread_id": "2"})
    assert r1.status_code == 200
    r2 = await client.post("/api/discord/poll-targets",
                           json={"channel_id": "1", "thread_id": "2"})
    assert r2.status_code == 200
    # GET returns one row
    r3 = await client.get("/api/discord/poll-targets")
    assert len([r for r in r3.json()["poll_targets"] if r["channel_id"] == "1"]) == 1


@pytest.mark.asyncio
async def test_get_poll_targets_returns_list(client):
    await client.post("/api/discord/poll-targets", json={"channel_id": "a"})
    await client.post("/api/discord/poll-targets", json={"channel_id": "b"})
    r = await client.get("/api/discord/poll-targets")
    assert r.status_code == 200
    targets = r.json()["poll_targets"]
    assert len(targets) >= 2
    chan_ids = {t["channel_id"] for t in targets}
    assert "a" in chan_ids and "b" in chan_ids


@pytest.mark.asyncio
async def test_delete_poll_target_success(client):
    await client.post("/api/discord/poll-targets",
                      json={"channel_id": "del-me", "thread_id": "t"})
    r = await client.delete("/api/discord/poll-targets",
                            params={"channel_id": "del-me", "thread_id": "t"})
    assert r.status_code == 200, r.text
    assert r.json()["deleted"] is True


@pytest.mark.asyncio
async def test_delete_poll_target_unknown_returns_404(client):
    r = await client.delete("/api/discord/poll-targets",
                            params={"channel_id": "nonexistent"})
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_delete_poll_target_idempotent_after_delete(client):
    await client.post("/api/discord/poll-targets", json={"channel_id": "x"})
    r1 = await client.delete("/api/discord/poll-targets", params={"channel_id": "x"})
    assert r1.status_code == 200
    r2 = await client.delete("/api/discord/poll-targets", params={"channel_id": "x"})
    assert r2.status_code == 404


# --------------------------------------------------------------- Lifespan + graceful skip

@pytest.mark.asyncio
async def test_graceful_skip_when_no_token(tmp_path, monkeypatch):
    """Missing token → maybe_start_poller returns None, no crash."""
    monkeypatch.delenv("DISCORD_BOT_TOKEN", raising=False)
    monkeypatch.setenv("DISCORD_STATE_DIR", str(tmp_path / "no-such"))
    import importlib
    from broker import discord_poller as _dp
    importlib.reload(_dp)
    result = await _dp.maybe_start_poller()
    assert result is None
    assert _dp.get_poller() is None


@pytest.mark.asyncio
async def test_graceful_skip_when_no_access_json(tmp_path, monkeypatch):
    """Token present but access.json missing → skip."""
    state_dir = tmp_path / "discord-state"
    state_dir.mkdir()
    (state_dir / ".env").write_text("DISCORD_BOT_TOKEN=fake-token-for-test\n", encoding="utf-8")
    # access.json deliberately absent
    monkeypatch.setenv("DISCORD_STATE_DIR", str(state_dir))
    monkeypatch.delenv("DISCORD_BOT_TOKEN", raising=False)
    import importlib
    from broker import discord_poller as _dp
    importlib.reload(_dp)
    result = await _dp.maybe_start_poller()
    assert result is None


@pytest.mark.asyncio
async def test_load_env_token_real_env_wins(tmp_path, monkeypatch):
    """Real env var DISCORD_BOT_TOKEN takes precedence over .env file."""
    state_dir = tmp_path / "ds"
    state_dir.mkdir()
    (state_dir / ".env").write_text("DISCORD_BOT_TOKEN=from-file\n", encoding="utf-8")
    monkeypatch.setenv("DISCORD_BOT_TOKEN", "from-env")
    monkeypatch.setenv("DISCORD_STATE_DIR", str(state_dir))
    import importlib
    from broker import discord_poller as _dp
    importlib.reload(_dp)
    assert _dp._load_env_token() == "from-env"


@pytest.mark.asyncio
async def test_load_env_token_from_file(tmp_path, monkeypatch):
    """Stdlib parse of .env when real env missing."""
    state_dir = tmp_path / "ds"
    state_dir.mkdir()
    (state_dir / ".env").write_text(
        "# comment line ignored\nDISCORD_BOT_TOKEN=from-file-OK\nOTHER=ignored\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("DISCORD_BOT_TOKEN", raising=False)
    monkeypatch.setenv("DISCORD_STATE_DIR", str(state_dir))
    import importlib
    from broker import discord_poller as _dp
    importlib.reload(_dp)
    assert _dp._load_env_token() == "from-file-OK"


# F3 (iter1 discriminator): bootstrap_targets must skip DM target creation
# when dmPolicy='disabled'.
@pytest.mark.asyncio
async def test_bootstrap_skips_dm_targets_when_dm_policy_disabled(schema_db):
    """F3: with dmPolicy='disabled' + non-empty allowFrom, bootstrap_targets
    must NOT call _open_dm_channel (which would burn rate-limit quota for
    channels that would never deliver anything anyway)."""
    open_dm_calls: list[str] = []

    def _h(req):
        url = str(req.url)
        if "/users/@me/channels" in url:
            # Track POSTs that would open DM channels
            open_dm_calls.append(url)
            return httpx.Response(200, json={"id": "dm-channel-id"})
        if "/users/@me" in url:
            return httpx.Response(200, json={"id": BOT_USER})
        return httpx.Response(404, json={})

    http = httpx.AsyncClient(transport=httpx.MockTransport(_h))
    access = make_access(dm_policy="disabled", allow_from=[ALLOWED_USER, "user-2"])
    poller = dp.DiscordPoller(token="x", access=access, http_client=http)
    await poller.bootstrap_targets()
    # No DM open requests should have been issued
    assert open_dm_calls == [], (
        f"F3 bug: bootstrap_targets opened DM channels even though dmPolicy='disabled' "
        f"(opened: {open_dm_calls})"
    )
    # No DM-typed targets in the registry
    dm_targets = [t for t in poller._targets.values() if t.is_dm]
    assert dm_targets == []


@pytest.mark.asyncio
async def test_bootstrap_opens_dm_targets_when_dm_policy_allowlist(schema_db):
    """F3 sanity: with dmPolicy='allowlist' (the normal case), DM targets ARE
    opened. We don't want the F3 fix to over-correct and disable all DMs."""
    open_dm_calls: list[str] = []

    def _h(req):
        url = str(req.url)
        if "/users/@me/channels" in url:
            open_dm_calls.append(url)
            return httpx.Response(200, json={"id": "dm-ch-1"})
        if "/users/@me" in url:
            return httpx.Response(200, json={"id": BOT_USER})
        return httpx.Response(404, json={})

    http = httpx.AsyncClient(transport=httpx.MockTransport(_h))
    access = make_access(dm_policy="allowlist", allow_from=[ALLOWED_USER])
    poller = dp.DiscordPoller(token="x", access=access, http_client=http)
    await poller.bootstrap_targets()
    assert len(open_dm_calls) == 1
    dm_targets = [t for t in poller._targets.values() if t.is_dm]
    assert len(dm_targets) == 1


# --------------------------------------------------------------- Token masking proof

@pytest.mark.asyncio
async def test_token_not_in_log_messages(tmp_path, monkeypatch, caplog):
    """Verify the bot token never lands in any logger output, even on errors."""
    state_dir = tmp_path / "ds"
    state_dir.mkdir()
    SECRET = "SECRET-TOKEN-DO-NOT-LOG-MTI3.foobar"
    (state_dir / ".env").write_text(f"DISCORD_BOT_TOKEN={SECRET}\n", encoding="utf-8")
    # No access.json → poller skips after loading token (gives us a code path that touches the token)
    monkeypatch.delenv("DISCORD_BOT_TOKEN", raising=False)
    monkeypatch.setenv("DISCORD_STATE_DIR", str(state_dir))
    import importlib
    from broker import discord_poller as _dp
    importlib.reload(_dp)

    with caplog.at_level("DEBUG", logger="broker.discord_poller"):
        await _dp.maybe_start_poller()
    full_log = "\n".join(r.getMessage() for r in caplog.records)
    assert SECRET not in full_log


@pytest.mark.asyncio
async def test_token_not_in_http_error_path(tmp_path, monkeypatch, caplog):
    """If HTTP fails, the token must not appear in the warning."""
    SECRET = "SECRET-MTIz.deadbeef.payload"
    transport = httpx.MockTransport(lambda r: httpx.Response(500, json={"e": "boom"}))
    http = httpx.AsyncClient(transport=transport)
    poller = dp.DiscordPoller(token=SECRET, access=make_access(), http_client=http)
    poller._gate_ctx = dp._build_gate_context(make_access(), BOT_USER)
    poller._targets[(GUILD_CH, "")] = dp.TargetState(
        channel_id=GUILD_CH, thread_id="", registered_at_ms=srv.now_ms(),
    )
    with caplog.at_level("DEBUG", logger="broker.discord_poller"):
        await poller.poll_once()
    full_log = "\n".join(r.getMessage() for r in caplog.records)
    assert SECRET not in full_log


# --------------------------------------------------------------- Lifespan poll cycle

@pytest.mark.asyncio
async def test_lifespan_full_poll_cycle(tmp_path, monkeypatch):
    """End-to-end: start poller, run 1 poll cycle, verify message inserted, stop cleanly."""
    state_dir = tmp_path / "ds"
    state_dir.mkdir()
    (state_dir / ".env").write_text("DISCORD_BOT_TOKEN=lifecycle-test-token\n", encoding="utf-8")
    (state_dir / "access.json").write_text(json.dumps(make_access()), encoding="utf-8")
    monkeypatch.delenv("DISCORD_BOT_TOKEN", raising=False)
    monkeypatch.setenv("DISCORD_STATE_DIR", str(state_dir))
    monkeypatch.setenv("BROKER_DB_PATH", str(tmp_path / "coordination.db"))
    monkeypatch.setenv("BROKER_HOST", "127.0.0.1")

    import importlib
    from broker import discord_poller as _dp
    importlib.reload(_dp)
    await srv.ensure_schema()

    # Patch the http client factory
    now_ts = srv.now_ms()
    sf = make_snowflake(now_ts + 1000)

    def _h(req):
        url = str(req.url)
        if "/users/@me/channels" in url:
            return httpx.Response(403, json={})
        if "/users/@me" in url:
            return httpx.Response(200, json={"id": BOT_USER})
        if f"/channels/{GUILD_CH}/messages" in url:
            return httpx.Response(200, json=[
                make_msg(sf, author_id=ALLOWED_USER, content="hi", mentions=[{"id": BOT_USER}])
            ])
        return httpx.Response(404, json={})

    transport = httpx.MockTransport(_h)
    monkeypatch.setattr(
        _dp.DiscordPoller, "_make_client",
        lambda self: httpx.AsyncClient(transport=transport)
    )

    poller = await _dp.maybe_start_poller()
    assert poller is not None
    # The background loop in start() will fire on its own. Cancel it so we can
    # control timing precisely with one explicit poll_once.
    if poller._task is not None:
        poller._task.cancel()
        try:
            await poller._task
        except (asyncio.CancelledError, Exception):
            pass
    # Run one explicit cycle (deterministic; background loop's race has been removed).
    await poller.poll_once()
    await _dp.maybe_stop_poller()
    assert _dp.get_poller() is None

    async with srv.connect_db() as conn:
        cur = await conn.execute("SELECT COUNT(*) AS n FROM message_log WHERE kind='user_msg'")
        row = await cur.fetchone()
        await cur.close()
    assert row["n"] == 1


# --------------------------------------------------------------- Health endpoint reflects circuit state

@pytest.mark.asyncio
async def test_health_reflects_persistent_open(client):
    """Health endpoint surfaces persistent_open as 'degraded'."""
    from broker import discord_poller as _dp
    _dp.broker_metrics["discord_rate_limit_circuit_open"]["test-channel"] = True
    _dp.broker_metrics["discord_consecutive_capped_failures"]["test-channel"] = _dp.PERSISTENT_OPEN_THRESHOLD
    r = await client.get("/api/health")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "degraded"
    assert data["checks"].get("discord_rate_limit_circuit_open", {}).get("test-channel") is True
    # Cleanup so other tests aren't polluted
    _dp.broker_metrics["discord_rate_limit_circuit_open"].pop("test-channel", None)
    _dp.broker_metrics["discord_consecutive_capped_failures"].pop("test-channel", None)


@pytest.mark.asyncio
async def test_health_ok_when_circuit_closed(client):
    """Health stays 'ok' when no circuits are open.

    The event post is now BELT-AND-BRACES rather than required, and the change is
    worth recording because the note here previously said the opposite. It used to
    be load-bearing: an empty log read 'degraded' on its own, which would have
    masked the circuit dimension this test measures. Since 2026-08-08 idleness no
    longer drives `status` at all, so an empty database is already 'ok' and the
    post only makes the fixture more realistic.

    The test is STRONGER as a result: with age and emptiness gone, the only
    remaining paths to 'degraded' are a failing SQLite probe and this circuit, so
    an 'ok' here is now a much narrower claim than it was.
    """
    from broker import discord_poller as _dp
    from broker.server import now_ms
    _dp.broker_metrics["discord_rate_limit_circuit_open"] = {}
    _dp.broker_metrics["discord_consecutive_capped_failures"] = {}
    r = await client.post("/api/event", json={
        "from_agent": "circuit-probe", "channel": "#h", "kind": "user_msg",
        "content": "x", "ts": now_ms(),
    })
    assert r.status_code == 200, r.text
    r = await client.get("/api/health")
    assert r.json()["status"] == "ok"


# --------------------------------------------------------------- F11 (iter2): ancillary REST calls go through _discord_request

@pytest.mark.asyncio
async def test_fetch_bot_user_id_consumes_global_token(schema_db, monkeypatch):
    """F11: GET /users/@me must consume 1 global bucket token. Pre-fix this
    call bypassed the bucket entirely."""

    def _h(req):
        if "/users/@me" in str(req.url):
            return httpx.Response(200, json={"id": BOT_USER})
        return httpx.Response(404, json={})

    http = httpx.AsyncClient(transport=httpx.MockTransport(_h))
    poller = dp.DiscordPoller(token="x", access=make_access(), http_client=http)

    # Spy on the global bucket
    acquire_calls: list[int] = []
    real_acquire = poller._global_bucket.acquire

    async def _spy_acquire(n: int = 1):
        acquire_calls.append(n)
        await real_acquire(n)

    poller._global_bucket.acquire = _spy_acquire  # type: ignore[assignment]

    bot_id = await poller.fetch_bot_user_id()
    assert bot_id == BOT_USER
    assert len(acquire_calls) >= 1, (
        f"F11: fetch_bot_user_id must acquire >=1 global token, got {acquire_calls}"
    )


@pytest.mark.asyncio
async def test_open_dm_channel_consumes_global_token(schema_db, monkeypatch):
    """F11: POST /users/@me/channels must consume 1 global bucket token."""

    def _h(req):
        if "/users/@me/channels" in str(req.url):
            return httpx.Response(200, json={"id": "dm-channel-id"})
        return httpx.Response(404, json={})

    http = httpx.AsyncClient(transport=httpx.MockTransport(_h))
    poller = dp.DiscordPoller(token="x", access=make_access(), http_client=http)

    acquire_calls: list[int] = []
    real_acquire = poller._global_bucket.acquire

    async def _spy_acquire(n: int = 1):
        acquire_calls.append(n)
        await real_acquire(n)

    poller._global_bucket.acquire = _spy_acquire  # type: ignore[assignment]

    dm_id = await poller._open_dm_channel(ALLOWED_USER)
    assert dm_id == "dm-channel-id"
    assert len(acquire_calls) >= 1, (
        f"F11: _open_dm_channel must acquire >=1 global token, got {acquire_calls}"
    )


@pytest.mark.asyncio
async def test_fetch_thread_parent_consumes_global_token(schema_db, monkeypatch):
    """F11: GET /channels/{thread_id} must consume 1 global bucket token."""

    def _h(req):
        url = str(req.url)
        if "/channels/" in url:
            return httpx.Response(200, json={"parent_id": "PARENT-CH"})
        return httpx.Response(404, json={})

    http = httpx.AsyncClient(transport=httpx.MockTransport(_h))
    poller = dp.DiscordPoller(token="x", access=make_access(), http_client=http)

    acquire_calls: list[int] = []
    real_acquire = poller._global_bucket.acquire

    async def _spy_acquire(n: int = 1):
        acquire_calls.append(n)
        await real_acquire(n)

    poller._global_bucket.acquire = _spy_acquire  # type: ignore[assignment]

    parent = await poller._fetch_thread_parent("THREAD-ID")
    assert parent == "PARENT-CH"
    assert len(acquire_calls) >= 1, (
        f"F11: _fetch_thread_parent must acquire >=1 global token, got {acquire_calls}"
    )


@pytest.mark.asyncio
async def test_is_reply_to_bot_via_discord_request_consumes_global_token(schema_db, monkeypatch):
    """F11: when _is_reply_to_bot fetches the referenced message via the
    centralized _discord_request, it must consume 1 global bucket token.
    Pre-fix this call bypassed the bucket entirely."""

    def _h(req):
        url = str(req.url)
        if "/messages/" in url:
            return httpx.Response(200, json={
                "id": "REF-MSG", "author": {"id": BOT_USER, "bot": True},
            })
        return httpx.Response(404, json={})

    http = httpx.AsyncClient(transport=httpx.MockTransport(_h))
    poller = dp.DiscordPoller(token="x", access=make_access(), http_client=http)

    acquire_calls: list[int] = []
    real_acquire = poller._global_bucket.acquire

    async def _spy_acquire(n: int = 1):
        acquire_calls.append(n)
        await real_acquire(n)

    poller._global_bucket.acquire = _spy_acquire  # type: ignore[assignment]

    msg = make_msg("USER-MSG", message_reference={"message_id": "REF-MSG"})
    is_reply = await dp._is_reply_to_bot(
        msg, BOT_USER, http, GUILD_CH,
        discord_request=poller._discord_request,
    )
    assert is_reply is True
    assert len(acquire_calls) >= 1, (
        f"F11: _is_reply_to_bot must acquire >=1 global token via _discord_request, "
        f"got {acquire_calls}"
    )


@pytest.mark.asyncio
async def test_discord_request_honors_429_retry_after(schema_db, monkeypatch):
    """F11: ancillary 429 responses MUST be honored — sleep per Retry-After
    then retry. Pre-fix the ancillary calls silently failed without any
    backoff, allowing the broker to spam Discord into a ban."""
    sleeps = make_sleep_recorder()
    monkeypatch.setattr(dp.asyncio, "sleep", sleeps.stub)

    call_count = {"n": 0}

    def _h(req):
        call_count["n"] += 1
        if call_count["n"] == 1:
            # First call: 429 with Retry-After=7
            return httpx.Response(
                429,
                headers={"Retry-After": "7"},
                json={"message": "rate limited", "retry_after": 7.0},
            )
        # Second call (retry): success
        if "/users/@me" in str(req.url):
            return httpx.Response(200, json={"id": BOT_USER})
        return httpx.Response(404, json={})

    http = httpx.AsyncClient(transport=httpx.MockTransport(_h))
    poller = dp.DiscordPoller(token="x", access=make_access(), http_client=http)

    bot_id = await poller.fetch_bot_user_id()
    assert bot_id == BOT_USER
    assert call_count["n"] == 2, (
        f"F11: 429 must trigger one retry, but observed {call_count['n']} calls"
    )
    assert any(abs(s - 7.0) < 0.01 for s in sleeps.calls), (
        f"F11: 429 Retry-After=7 must be honored, but recorded sleeps were {sleeps.calls}"
    )


@pytest.mark.asyncio
async def test_no_raw_discord_http_calls_in_module():
    """F11 enforcement: production code MUST route ALL Discord REST through
    _discord_request. Grep the module source for raw `await http.<verb>(...)`
    or `await self._http.<verb>(...)` for ANY httpx verb (get/post/put/patch/
    delete/request) — there should be exactly FOUR allowed sites:

      - `_is_reply_to_bot` test fallback (`await http.get(url)`); production
        gate_message always passes discord_request, so this branch is dead code
        in prod but kept for httpx.MockTransport-only tests.
      - the canonical `_discord_request` implementation itself
        (`await http.request(method, url, **kwargs)`). This IS the wrapper
        everyone else must funnel through.
      - the main poll loop (`await http.get(url, params=params)`); inlines its
        own bucket/429 handling, intentionally NOT going through `_discord_request`.
      - `post_alert` (`await http.post(url, json=payload)`) — WS-B-broker-2 (#18)
        standalone escalation-alert egress. DELIBERATELY does not use
        `_discord_request`: it must work with NO live DiscordPoller (the poller
        only exists when poll targets are registered), so it builds its own
        short-lived client + inlines a single 429 Retry-After retry. The alert
        channel is rare-traffic, so a second independent rate-limiter is
        negligible (see post_alert docstring).

    F13 (iter3 discriminator): pre-fix, this test only counted the literal
    substrings `"await http.get("` and `"await http.post("`, which let
    regressions via `http.request(...)`, `http.put/patch/delete(...)`, or
    `self._http.get(...)` slip past undetected. The regex below covers all
    httpx verbs AND both `http`/`self._http` reference patterns.

    This test fails loudly if a future change reintroduces a raw call.
    """
    import inspect
    import re
    src = inspect.getsource(dp)

    # Match: `await http.<verb>(`  OR  `await self._http.<verb>(`
    # for any httpx HTTP verb. Word boundary after the verb prevents matching
    # things like `.gettr(` or `.requests` (defensive — no current legit case
    # but cheap).
    pattern = re.compile(
        r"await\s+(?:self\._http|http)\.(?:get|post|put|patch|delete|request)\b",
        re.MULTILINE,
    )
    matches = pattern.findall(src)
    # Allowed: 4 (the 3 legacy sites + post_alert, see docstring above).
    # ANY higher count = regression that must funnel through _discord_request.
    assert len(matches) <= 4, (
        f"F11 violation: too many raw httpx-vs-Discord calls in "
        f"discord_poller.py ({len(matches)} found, max allowed 4). All "
        f"Discord REST must go through _discord_request except the four "
        f"documented sites: _is_reply_to_bot test fallback, the "
        f"_discord_request wrapper itself, the main poll loop, and "
        f"post_alert (#18 standalone escalation egress). Matches found: {matches}"
    )


# --------------------------------------------------------------- F10 (iter2): bootstrap registered_at + last_check_ts preservation across restart

@pytest.mark.asyncio
async def test_bootstrap_preserves_registered_at_across_restart(schema_db, monkeypatch):
    """F10 (iter2 discriminator): bootstrap targets must preserve their original
    `registered_at` across broker restart. Iter-1 reset registered_at_ms to
    startup time on every bootstrap, which let the snowflake first-poll filter
    drop legitimate messages received during downtime.

    Scenario:
      (a) Bootstrap with T1 (no DB row exists) → row written with T1.
      (b) Successful poll at T1+5s with zero messages → cursor row UPSERTed
          with last_check_ts=T1+5s.
      (c) Simulate broker restart: construct a NEW poller with the SAME db.
      (d) After bootstrap, in-memory TargetState.registered_at_ms must equal T1
          (NOT new startup time).
      (e) After bootstrap, in-memory TargetState.last_check_ts must equal T1+5s.
    """

    def _h(req):
        url = str(req.url)
        if "/users/@me/channels" in url:
            return httpx.Response(403, json={})
        if "/users/@me" in url:
            return httpx.Response(200, json={"id": BOT_USER})
        if "/messages" in url:
            return httpx.Response(200, json=[])
        return httpx.Response(404, json={})

    # ---------- (a) First boot: bootstrap creates the row at T1 ----------
    http1 = httpx.AsyncClient(transport=httpx.MockTransport(_h))
    poller1 = dp.DiscordPoller(token="x", access=make_access(), http_client=http1)
    await poller1.bootstrap_targets()
    target = poller1._targets[(GUILD_CH, "")]
    t1 = target.registered_at_ms
    assert t1 > 0

    # Verify DB row was written with T1
    async with srv.connect_db() as conn:
        cur = await conn.execute(
            "SELECT registered_at FROM discord_poll_targets "
            "WHERE channel_id = ? AND thread_id = ''",
            (GUILD_CH,),
        )
        row = await cur.fetchone()
        await cur.close()
    assert row is not None
    assert row["registered_at"] == t1

    # ---------- (b) Successful poll → last_check_ts written ----------
    # Sleep enough that subsequent now_ms() differs from t1
    await asyncio.sleep(0.01)
    poll_ts = srv.now_ms()
    assert poll_ts > t1
    await poller1.poll_once()
    async with srv.connect_db() as conn:
        cur = await conn.execute(
            "SELECT last_check_ts FROM discord_cursors "
            "WHERE channel_id = ? AND thread_id = ''",
            (GUILD_CH,),
        )
        row = await cur.fetchone()
        await cur.close()
    assert row is not None, "F10: cursor row must be created on first-poll-zero"
    poll_check_ts = row["last_check_ts"]
    assert poll_check_ts >= poll_ts

    # ---------- (c) Restart: new poller instance against the same DB ----------
    # Sleep so any "now" used in the new bootstrap would be strictly > t1, making
    # accidental override visible.
    await asyncio.sleep(0.01)
    restart_now = srv.now_ms()
    assert restart_now > t1

    http2 = httpx.AsyncClient(transport=httpx.MockTransport(_h))
    poller2 = dp.DiscordPoller(token="x", access=make_access(), http_client=http2)
    await poller2.bootstrap_targets()
    restored = poller2._targets[(GUILD_CH, "")]

    # ---------- (d) registered_at_ms must equal T1 (not new startup time) ----------
    assert restored.registered_at_ms == t1, (
        f"F10 bug: bootstrap reset registered_at_ms across restart. "
        f"Expected original t1={t1}, got {restored.registered_at_ms} "
        f"(restart_now={restart_now}). Snowflake filter would now drop "
        f"messages received during downtime."
    )

    # ---------- (e) last_check_ts must equal poll_check_ts (restored from DB) ----------
    assert restored.last_check_ts == poll_check_ts, (
        f"F10: last_check_ts must be restored from DB across restart. "
        f"Expected {poll_check_ts}, got {restored.last_check_ts}"
    )

    # Sanity: the DB row's registered_at must STILL equal t1 (UPSERT preserves it).
    async with srv.connect_db() as conn:
        cur = await conn.execute(
            "SELECT registered_at FROM discord_poll_targets "
            "WHERE channel_id = ? AND thread_id = ''",
            (GUILD_CH,),
        )
        row = await cur.fetchone()
        await cur.close()
    assert row["registered_at"] == t1, (
        f"F10: discord_poll_targets.registered_at must NOT be overwritten on "
        f"restart bootstrap. Expected t1={t1}, got {row['registered_at']}."
    )


@pytest.mark.asyncio
async def test_persist_check_ts_upserts_when_no_row_exists(schema_db):
    """F10: _persist_check_ts MUST UPSERT — the previous UPDATE-only impl
    silently no-op'd when no cursor row existed yet (first-poll-zero case),
    breaking durable bookkeeping per spec § (e)."""
    poller = dp.DiscordPoller(token="x", access=make_access())
    target = dp.TargetState(
        channel_id="C-NEW",
        thread_id="",
        registered_at_ms=srv.now_ms(),
    )
    # Pre-condition: no row exists for (C-NEW, '').
    async with srv.connect_db() as conn:
        cur = await conn.execute(
            "SELECT COUNT(*) AS n FROM discord_cursors WHERE channel_id = 'C-NEW'",
        )
        row = await cur.fetchone()
        await cur.close()
    assert row["n"] == 0

    await poller._persist_check_ts(target)

    # Post-condition: row exists with empty sentinel + non-zero last_check_ts.
    async with srv.connect_db() as conn:
        cur = await conn.execute(
            "SELECT last_seen_message_id, last_check_ts FROM discord_cursors "
            "WHERE channel_id = 'C-NEW' AND thread_id = ''",
        )
        row = await cur.fetchone()
        await cur.close()
    assert row is not None, "F10: _persist_check_ts must INSERT when no row exists"
    assert row["last_seen_message_id"] == ""
    assert row["last_check_ts"] > 0


@pytest.mark.asyncio
async def test_persist_check_ts_preserves_real_message_id_on_upsert(schema_db):
    """F10: when a row already has a real last_seen_message_id, _persist_check_ts
    must NOT overwrite it with the empty sentinel — only update last_check_ts."""
    poller = dp.DiscordPoller(token="x", access=make_access())
    target = dp.TargetState(
        channel_id="C-EXISTS",
        thread_id="",
        registered_at_ms=srv.now_ms(),
    )
    # Pre-seed a real cursor row.
    async with srv.connect_db() as conn:
        await conn.execute(
            "INSERT INTO discord_cursors "
            "(channel_id, thread_id, last_seen_message_id, last_check_ts) "
            "VALUES ('C-EXISTS', '', 'real-msg-id-123', 1000)",
        )
        await conn.commit()

    await poller._persist_check_ts(target)

    async with srv.connect_db() as conn:
        cur = await conn.execute(
            "SELECT last_seen_message_id, last_check_ts FROM discord_cursors "
            "WHERE channel_id = 'C-EXISTS'",
        )
        row = await cur.fetchone()
        await cur.close()
    assert row["last_seen_message_id"] == "real-msg-id-123", (
        f"F10: _persist_check_ts UPSERT must NOT clobber real message id with "
        f"empty sentinel. Got {row['last_seen_message_id']!r}."
    )
    assert row["last_check_ts"] > 1000, "last_check_ts must have advanced"
