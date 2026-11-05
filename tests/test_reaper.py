"""WS-B-broker-2 (#18): relay escalation reaper + post_alert + uv1->2 cutover.

Covers:
- reaper query selects ONLY outstanding ^ aged-past-grace ^ alerted_ts IS NULL
- read -> release -> POST(outside any txn) -> separate write-txn stamp: a concurrent
  write is NOT blocked while post_alert runs (no BEGIN IMMEDIATE held across the POST)
- alerted_ts dedup (one alert per row across cycles)
- POST failure leaves alerted_ts NULL (retried next cycle)
- untrusted-preview control-strip + truncate + Discord-mention defang
- post_alert: payload shape, content[:1900] truncation, single 429 Retry-After retry,
  no-token -> False, non-2xx -> False
- ensure_schema uv1->2: adds delivery.alerted_ts on an EXISTING (pre-#18) DB + cutover
  reconcile stamps the pre-enable outstanding backlog so the reaper skips it
- lifespan: BROKER_REAPER_ENABLED gates spawn (true -> task; false -> None)

All Discord egress is mocked -- NO live network in unit tests.
Source is pure-ASCII: the zero-width space / ellipsis are built via chr() so no
invisible glyph lives in this file.
"""

from __future__ import annotations

import os
import sqlite3

import pytest

pytestmark = pytest.mark.asyncio

ZWSP = chr(0x200B)   # zero-width space the defang splices in
ELL = chr(0x2026)    # ellipsis the preview truncation appends


# ---------------------------------------------------------------- helpers

async def _seed(db, *, to_agent, event_id, created_ts, status="outstanding",
                alerted_ts=None, from_agent="alice", kind="agent_message", content="hi",
                reject_reason_code=None):
    """Seed a message_log parent + a delivery row with an explicit event_id."""
    import aiosqlite
    if status == "rejected" and reject_reason_code is None:
        reject_reason_code = "PROTOCOL_INVALID"
    async with aiosqlite.connect(db) as conn:
        await conn.execute(
            "INSERT INTO message_log (event_id, from_agent, to_agent, channel, kind, content, ts) "
            "VALUES (?,?,?,?,?,?,?)",
            (event_id, from_agent, to_agent, "#x", kind, content, created_ts),
        )
        await conn.execute(
            "INSERT INTO delivery (to_agent,event_id,status,attempts,created_ts,"
            "updated_ts,acked_ts,alerted_ts,reject_reason_code,rejected_ts) "
            "VALUES (?,?,?,0,?,?,?,?,?,?)",
            (
                to_agent, event_id, status, created_ts, created_ts, None, alerted_ts,
                reject_reason_code, created_ts if status == "rejected" else None,
            ),
        )
        await conn.commit()


async def _alerted_ts(db, to_agent, event_id):
    import aiosqlite
    async with aiosqlite.connect(db) as conn:
        cur = await conn.execute(
            "SELECT alerted_ts FROM delivery WHERE to_agent=? AND event_id=?",
            (to_agent, event_id),
        )
        row = await cur.fetchone()
        await cur.close()
        return row[0] if row else None


class _FakeResp:
    def __init__(self, status, json_body=None, headers=None):
        self.status_code = status
        self._j = json_body if json_body is not None else {}
        self.headers = headers or {}

    def json(self):
        return self._j


class _FakeClient:
    """Async-context httpx.AsyncClient stand-in; pops queued responses per .post()."""
    def __init__(self, responses, sent):
        self._responses = list(responses)
        self._sent = sent

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def post(self, url, json=None):
        self._sent.append((url, json))
        return self._responses.pop(0)


# ---------------------------------------------------------------- reaper query filter

async def test_reaper_selects_only_aged_outstanding_unalerted(db_path, monkeypatch):
    from broker import server, discord_poller
    now = server.now_ms()
    old = now - 700 * 1000     # older than the 600s grace -> eligible
    fresh = now - 60 * 1000    # within grace -> NOT eligible
    await _seed(db_path, to_agent="bob", event_id=101, created_ts=old)                     # eligible
    await _seed(db_path, to_agent="bob", event_id=102, created_ts=fresh)                   # too fresh
    await _seed(db_path, to_agent="carol", event_id=103, created_ts=old, status="acked")   # acked
    await _seed(db_path, to_agent="dave", event_id=104, created_ts=old, alerted_ts=now)    # already alerted

    posted = []

    async def fake_post(content, channel_id, *, token=None):
        posted.append((content, channel_id))
        return True

    monkeypatch.setattr(discord_poller, "post_alert", fake_post)
    n = await server._reaper_run_once(threshold_sec=600, channel_id="chan", host="H")
    assert n == 1
    assert len(posted) == 1
    body, chan = posted[0]
    assert chan == "chan"
    assert "101" in body and "bob" in body          # the eligible row's id + name
    assert "102" not in body and "103" not in body and "104" not in body


# ---------------------------------------------------------------- txn discipline

async def test_reaper_post_not_held_under_write_lock(db_path, monkeypatch):
    """The Discord POST must run OUTSIDE any txn -- a concurrent BEGIN IMMEDIATE write
    must NOT be blocked while post_alert is in flight (else it would stall inbound relay
    writes)."""
    from broker import server, discord_poller
    import aiosqlite
    now = server.now_ms()
    await _seed(db_path, to_agent="bob", event_id=201, created_ts=now - 700 * 1000)
    concurrent_ok = {"v": False}

    async def slow_post(content, channel_id, *, token=None):
        # Separate connection, short busy timeout: if the reaper WERE holding a write
        # lock across the POST, this BEGIN IMMEDIATE would block then raise instead of
        # succeeding immediately.
        async with aiosqlite.connect(db_path, timeout=2) as c2:
            await c2.execute("BEGIN IMMEDIATE")
            await c2.execute(
                "INSERT INTO delivery (to_agent,event_id,status,attempts,created_ts,updated_ts) "
                "VALUES ('zzz',9999,'outstanding',0,1,1)"
            )
            await c2.commit()
        concurrent_ok["v"] = True
        return True

    monkeypatch.setattr(discord_poller, "post_alert", slow_post)
    await server._reaper_run_once(threshold_sec=600, channel_id="c", host="H")
    assert concurrent_ok["v"] is True
    assert await _alerted_ts(db_path, "bob", 201) is not None


# ---------------------------------------------------------------- dedup + failure retry

async def test_reaper_dedups_across_cycles(db_path, monkeypatch):
    from broker import server, discord_poller
    now = server.now_ms()
    await _seed(db_path, to_agent="bob", event_id=301, created_ts=now - 700 * 1000)
    posted = []

    async def fake_post(content, channel_id, *, token=None):
        posted.append(content)
        return True

    monkeypatch.setattr(discord_poller, "post_alert", fake_post)
    assert await server._reaper_run_once(threshold_sec=600, channel_id="c", host="H") == 1
    assert await server._reaper_run_once(threshold_sec=600, channel_id="c", host="H") == 0
    assert len(posted) == 1
    assert await _alerted_ts(db_path, "bob", 301) is not None


async def test_reaper_post_failure_leaves_null_then_retries(db_path, monkeypatch):
    from broker import server, discord_poller
    now = server.now_ms()
    await _seed(db_path, to_agent="bob", event_id=401, created_ts=now - 700 * 1000)

    async def fail_post(content, channel_id, *, token=None):
        return False

    monkeypatch.setattr(discord_poller, "post_alert", fail_post)
    assert await server._reaper_run_once(threshold_sec=600, channel_id="c", host="H") == 0
    assert await _alerted_ts(db_path, "bob", 401) is None   # NOT stamped on failure

    async def ok_post(content, channel_id, *, token=None):
        return True

    monkeypatch.setattr(discord_poller, "post_alert", ok_post)
    assert await server._reaper_run_once(threshold_sec=600, channel_id="c", host="H") == 1
    assert await _alerted_ts(db_path, "bob", 401) is not None


async def test_reaper_alert_defangs_untrusted_preview(db_path, monkeypatch):
    from broker import server, discord_poller
    now = server.now_ms()
    await _seed(db_path, to_agent="bob", event_id=501, created_ts=now - 700 * 1000,
                content='{"text":"ping @everyone and <@42> now"}')
    posted = []

    async def fake_post(content, channel_id, *, token=None):
        posted.append(content)
        return True

    monkeypatch.setattr(discord_poller, "post_alert", fake_post)
    await server._reaper_run_once(threshold_sec=600, channel_id="c", host="H")
    assert len(posted) == 1
    body = posted[0]
    assert ("@" + ZWSP + "everyone") in body     # mass-mention defanged
    assert ("<" + ZWSP + "@42>") in body         # id-mention defanged
    assert "@everyone" not in body               # no contiguous mass-mention survives


# ---------------------------------------------------------------- pure sanitizers

async def test_defang_all_mention_forms():
    from broker import server
    assert server._reaper_defang("yo @everyone") == "yo @" + ZWSP + "everyone"
    assert server._reaper_defang("@here!") == "@" + ZWSP + "here!"
    assert server._reaper_defang("<@123>") == "<" + ZWSP + "@123>"
    assert server._reaper_defang("<@!123>") == "<" + ZWSP + "@!123>"
    assert server._reaper_defang("<@&99>") == "<" + ZWSP + "@&99>"    # role mention
    assert server._reaper_defang("<#55>") == "<" + ZWSP + "#55>"      # channel mention


async def test_sanitize_preview_strips_control_and_truncates():
    from broker import server
    s = server._reaper_sanitize_preview("line1\nline2\ttab\r\nx", limit=100)
    assert "\n" not in s and "\t" not in s and "\r" not in s
    out = server._reaper_sanitize_preview("x" * 500, limit=100)
    assert len(out) == 100 and out.endswith(ELL)


async def test_preview_unwraps_text_envelope():
    from broker import server
    assert server._reaper_preview_text('{"text":"hi there"}') == "hi there"
    assert server._reaper_preview_text("plain string") == "plain string"
    assert server._reaper_preview_text('{"other":1}') == '{"other":1}'   # not a text-envelope
    assert server._reaper_preview_text(None) == ""


async def test_build_alert_leads_with_name_and_action():
    """The operator page must lead with the agent and carry an ACTIONABLE remedy.

    UPDATED, not swept. This assertion used to require `/mcp reconnect` to be
    PRESENT. That remedy was measured NOT to fix the one cause in the reaper's
    own list that never recovers on its own -- a session whose registry row lost
    its name -- because a reconnect re-reads the same records and derives the
    same delivery key. The assertion now pins the remedy that does work.

    DELIBERATELY NOT A FIND-AND-REPLACE: four other tests in this file use the
    phrase `/mcp reconnect` as a PROMPT-INJECTION PAYLOAD (a forged action line
    an attacker wants the operator to act on). Sweeping the phrase out of those
    would leave them green while testing nothing, so their payloads are
    untouched and only THIS assertion -- the one that describes our OWN output
    -- moved.
    """
    from broker import server
    a = server._build_reaper_alert(to_agent="trading_infra", event_id=42,
                                   from_agent="broker", preview="x", age_s=99, host="H1")
    assert "trading_infra" in a and "42" in a
    assert "/relay-name" in a and "99s" in a and "broker H1" in a
    # The remedy that does not fix the permanent cause must be gone. Positive
    # control on the same string above ("trading_infra"/"99s") proves the builder
    # ran, so this absence is answered by an instrument that could have found the
    # counter-example.
    assert "/mcp reconnect" not in a


# ---------------------------------------------------------------- post_alert (mocked httpx)

async def test_post_alert_success_payload(monkeypatch):
    from broker import discord_poller
    monkeypatch.setenv("DISCORD_BOT_TOKEN", "tok")
    sent = []
    monkeypatch.setattr(discord_poller.httpx, "AsyncClient",
                        lambda *a, **k: _FakeClient([_FakeResp(200)], sent))
    ok = await discord_poller.post_alert("hello", "chan123")
    assert ok is True
    assert len(sent) == 1
    url, body = sent[0]
    assert url.endswith("/channels/chan123/messages")
    assert body == {"content": "hello", "allowed_mentions": {"parse": []}, "flags": 4}


async def test_post_alert_truncates_to_1900(monkeypatch):
    from broker import discord_poller
    monkeypatch.setenv("DISCORD_BOT_TOKEN", "tok")
    sent = []
    monkeypatch.setattr(discord_poller.httpx, "AsyncClient",
                        lambda *a, **k: _FakeClient([_FakeResp(200)], sent))
    ok = await discord_poller.post_alert("A" * 5000, "c")
    assert ok is True
    assert len(sent[0][1]["content"]) == 1900


async def test_post_alert_429_then_success(monkeypatch):
    from broker import discord_poller
    monkeypatch.setenv("DISCORD_BOT_TOKEN", "tok")
    sent = []
    responses = [_FakeResp(429, {"retry_after": 0}, {"Retry-After": "0"}), _FakeResp(200)]
    monkeypatch.setattr(discord_poller.httpx, "AsyncClient",
                        lambda *a, **k: _FakeClient(responses, sent))

    async def no_sleep(_s):
        return None

    monkeypatch.setattr(discord_poller.asyncio, "sleep", no_sleep)
    ok = await discord_poller.post_alert("x", "c")
    assert ok is True
    assert len(sent) == 2       # retried exactly once


async def test_post_alert_no_token_returns_false(monkeypatch):
    from broker import discord_poller
    monkeypatch.delenv("DISCORD_BOT_TOKEN", raising=False)   # D82: reaper path reads env only
    called = {"v": False}

    def _boom(*a, **k):
        called["v"] = True
        raise AssertionError("must not build a client without a token")

    monkeypatch.setattr(discord_poller.httpx, "AsyncClient", _boom)
    assert await discord_poller.post_alert("x", "c") is False
    assert called["v"] is False


async def test_post_alert_non_2xx_returns_false(monkeypatch):
    from broker import discord_poller
    monkeypatch.setenv("DISCORD_BOT_TOKEN", "tok")
    sent = []
    monkeypatch.setattr(discord_poller.httpx, "AsyncClient",
                        lambda *a, **k: _FakeClient([_FakeResp(403)], sent))
    assert await discord_poller.post_alert("x", "c") is False
    assert len(sent) == 1       # no retry on a non-429 error


# ---------------------------------------------------------------- uv1->2 migration + cutover

async def test_ensure_schema_uv1to2_adds_column_and_suppresses_backlog(tmp_path, monkeypatch):
    """An EXISTING pre-#18 DB (uv=1, delivery WITHOUT alerted_ts, an outstanding row):
    ensure_schema must ADD alerted_ts, stamp the pre-enable outstanding backlog
    (cutover reconcile), set user_version=2, and the reaper must then skip it."""
    from broker import server, discord_poller
    db = tmp_path / "c.db"
    monkeypatch.setenv("BROKER_DB_PATH", str(db))
    conn = sqlite3.connect(str(db))
    conn.executescript(
        "CREATE TABLE message_log (event_id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "client_event_id TEXT, from_agent TEXT NOT NULL, to_agent TEXT, channel TEXT NOT NULL, "
        "thread_id TEXT, kind TEXT NOT NULL, content TEXT NOT NULL, ts INTEGER NOT NULL);"
        "CREATE TABLE delivery (to_agent TEXT NOT NULL, event_id INTEGER NOT NULL, "
        "status TEXT NOT NULL DEFAULT 'outstanding', attempts INTEGER NOT NULL DEFAULT 0, "
        "next_push_ts INTEGER, created_ts INTEGER NOT NULL, updated_ts INTEGER NOT NULL, "
        "acked_ts INTEGER, PRIMARY KEY (to_agent,event_id));"
    )
    conn.execute("INSERT INTO message_log (event_id, from_agent, to_agent, channel, kind, content, ts) "
                 "VALUES (1,'a','bob','#x','agent_message','m',1)")
    conn.execute("INSERT INTO delivery (to_agent,event_id,status,attempts,created_ts,updated_ts) "
                 "VALUES ('bob',1,'outstanding',0,1,1)")
    conn.execute("PRAGMA user_version=1")
    conn.commit()
    conn.close()

    await server.ensure_schema()   # adds alerted_ts + reconciles + uv 1->2

    conn = sqlite3.connect(str(db))
    cols = {r[1] for r in conn.execute("PRAGMA table_info(delivery)").fetchall()}
    uv = conn.execute("PRAGMA user_version").fetchone()[0]
    alerted = conn.execute("SELECT alerted_ts FROM delivery WHERE to_agent='bob' AND event_id=1").fetchone()[0]
    conn.close()
    assert "alerted_ts" in cols
    assert uv == 5
    assert alerted is not None      # pre-enable backlog stamped (suppressed)

    posted = []

    async def fake_post(content, channel_id, *, token=None):
        posted.append(content)
        return True

    monkeypatch.setattr(discord_poller, "post_alert", fake_post)
    # threshold 0 => every row is "aged"; the reconciled row must STILL be skipped.
    assert await server._reaper_run_once(threshold_sec=0, channel_id="c", host="H") == 0
    assert posted == []


async def test_ensure_schema_uv1to2_is_run_once(tmp_path, monkeypatch):
    """Second ensure_schema on a uv=2 DB must NOT re-stamp a genuinely-new outstanding
    row (the migration is run-once, guarded by user_version)."""
    from broker import server
    db = tmp_path / "c.db"
    monkeypatch.setenv("BROKER_DB_PATH", str(db))
    await server.ensure_schema()   # fresh -> uv=2
    # Insert a NEW outstanding row AFTER the migration; a re-run must not stamp it.
    conn = sqlite3.connect(str(db))
    conn.execute("INSERT INTO message_log (event_id, from_agent, to_agent, channel, kind, content, ts) "
                 "VALUES (7,'a','bob','#x','agent_message','m',1)")
    conn.execute("INSERT INTO delivery (to_agent,event_id,status,attempts,created_ts,updated_ts) "
                 "VALUES ('bob',7,'outstanding',0,1,1)")
    conn.commit()
    conn.close()
    await server.ensure_schema()   # uv already 2 -> reconcile must NOT run again
    assert await _alerted_ts(str(db), "bob", 7) is None


# ---------------------------------------------------------------- lifespan spawn gate

async def test_reaper_enabled_spawns_and_cancels(tmp_path, monkeypatch):
    from broker import server, discord_poller
    monkeypatch.setenv("BROKER_DB_PATH", str(tmp_path / "c.db"))
    monkeypatch.setenv("BROKER_HOST", "127.0.0.1")
    monkeypatch.setenv("BROKER_REAPER_ENABLED", "true")
    # D82: reaper enabled -> its Discord config MUST be present (mirrors the real deploy +
    # server.py's refuse-to-start precondition; the reaper reads the channel from env at task start).
    monkeypatch.setenv("BROKER_ALERT_DISCORD_CHANNEL_ID", str(10**17))  # synthetic snowflake, not a destination
    monkeypatch.setenv("DISCORD_BOT_TOKEN", "test-token")

    async def _noop(*a, **k):
        return None

    monkeypatch.setattr(discord_poller, "maybe_start_poller", _noop)
    monkeypatch.setattr(discord_poller, "maybe_stop_poller", _noop)

    async with server.lifespan(server.app):
        assert server.app.state.reaper_task is not None
        assert not server.app.state.reaper_task.done()
    assert server.app.state.reaper_task is None    # cancelled + nulled on shutdown


async def test_reaper_disabled_skips_spawn(tmp_path, monkeypatch):
    from broker import server, discord_poller
    monkeypatch.setenv("BROKER_DB_PATH", str(tmp_path / "c.db"))
    monkeypatch.setenv("BROKER_HOST", "127.0.0.1")
    monkeypatch.setenv("BROKER_REAPER_ENABLED", "false")

    async def _noop(*a, **k):
        return None

    monkeypatch.setattr(discord_poller, "maybe_start_poller", _noop)
    monkeypatch.setattr(discord_poller, "maybe_stop_poller", _noop)

    async with server.lifespan(server.app):
        assert server.app.state.reaper_task is None


async def test_reaper_default_dormant_when_env_absent(tmp_path, monkeypatch):
    """Ship-safety: with BROKER_REAPER_ENABLED UNSET the reaper must NOT spawn (default OFF),
    so pulling this bundle can't auto-enable Discord alerting on a fleet machine before the
    D75 mirror is proven flipping delivery rows there (enable only after delivery mirroring is configured)."""
    from broker import server, discord_poller
    monkeypatch.setenv("BROKER_DB_PATH", str(tmp_path / "c.db"))
    monkeypatch.setenv("BROKER_HOST", "127.0.0.1")
    monkeypatch.delenv("BROKER_REAPER_ENABLED", raising=False)   # env ABSENT -> default

    async def _noop(*a, **k):
        return None

    monkeypatch.setattr(discord_poller, "maybe_start_poller", _noop)
    monkeypatch.setattr(discord_poller, "maybe_stop_poller", _noop)

    async with server.lifespan(server.app):
        assert server.app.state.reaper_task is None


# ---------------------------------------------------------------- Option A: /api/health reaper_running

async def test_health_reports_reaper_running_true_when_enabled(tmp_path, monkeypatch):
    """Option A (D80): GET /api/health reports checks.reaper_running == True when the
    reaper was spawned (BROKER_REAPER_ENABLED=true). This is the authoritative,
    provenance-correct signal enable-reaper.ps1 queries -- only the process that bound
    :8787 can answer this request, so a failed-bind child can't be mistaken for it."""
    from broker import server, discord_poller
    monkeypatch.setenv("BROKER_DB_PATH", str(tmp_path / "c.db"))
    monkeypatch.setenv("BROKER_HOST", "127.0.0.1")
    monkeypatch.setenv("BROKER_REAPER_ENABLED", "true")
    # D82: reaper enabled -> its Discord config MUST be present (mirrors the real deploy +
    # server.py's refuse-to-start precondition; the reaper reads the channel from env at task start).
    monkeypatch.setenv("BROKER_ALERT_DISCORD_CHANNEL_ID", str(10**17))  # synthetic snowflake, not a destination
    monkeypatch.setenv("DISCORD_BOT_TOKEN", "test-token")

    async def _noop(*a, **k):
        return None

    monkeypatch.setattr(discord_poller, "maybe_start_poller", _noop)
    monkeypatch.setattr(discord_poller, "maybe_stop_poller", _noop)

    async with server.lifespan(server.app):
        h = await server.get_health()
        assert h["checks"]["reaper_running"] is True
        assert h["checks"]["reaper_task_present"] is True


async def test_health_reports_reaper_running_false_when_disabled(tmp_path, monkeypatch):
    """Option A (D80): with the reaper dormant (BROKER_REAPER_ENABLED=false) reaper_running is
    False AND reaper_task_present is False (never spawned). The fields reflect the RUNNING
    process state (app.state.reaper_task is None), never broker.env's declared value. (The
    D80-era enable-reaper.ps1 reconcile that consumed these was DELETED, D81; they are now
    diagnostic-only -- this test still asserts the emitted field values.)"""
    from broker import server, discord_poller
    monkeypatch.setenv("BROKER_DB_PATH", str(tmp_path / "c.db"))
    monkeypatch.setenv("BROKER_HOST", "127.0.0.1")
    monkeypatch.setenv("BROKER_REAPER_ENABLED", "false")

    async def _noop(*a, **k):
        return None

    monkeypatch.setattr(discord_poller, "maybe_start_poller", _noop)
    monkeypatch.setattr(discord_poller, "maybe_stop_poller", _noop)

    async with server.lifespan(server.app):
        h = await server.get_health()
        assert h["checks"]["reaper_running"] is False
        assert h["checks"]["reaper_task_present"] is False


async def test_health_reaper_present_but_done_reads_as_not_running(tmp_path, monkeypatch):
    """Option A (D80) fail-safe (discriminator F1): a reaper that SPAWNED but has since
    exited/crashed reports reaper_running=False YET reaper_task_present=True -- distinguishing
    it from the never-spawned case (both have reaper_running=False). (The D80-era
    enable-reaper.ps1 reconcile that used this distinction was DELETED, D81 -- no DB signal can
    prove a row safe to stamp; the fields are now diagnostic-only. This test still asserts the
    emitted field values.)"""
    import asyncio
    from broker import server, discord_poller
    monkeypatch.setenv("BROKER_DB_PATH", str(tmp_path / "c.db"))
    monkeypatch.setenv("BROKER_HOST", "127.0.0.1")
    monkeypatch.setenv("BROKER_REAPER_ENABLED", "false")

    async def _noop(*a, **k):
        return None

    monkeypatch.setattr(discord_poller, "maybe_start_poller", _noop)
    monkeypatch.setattr(discord_poller, "maybe_stop_poller", _noop)

    async def _dead():
        return None

    async with server.lifespan(server.app):
        t = asyncio.ensure_future(_dead())
        await t                                    # drive the task to done()
        server.app.state.reaper_task = t           # simulate spawned-but-exited
        try:
            h = await server.get_health()
            assert h["checks"]["reaper_running"] is False       # not alive
            assert h["checks"]["reaper_task_present"] is True    # but WAS spawned -> Unknown
        finally:
            server.app.state.reaper_task = None    # restore for later tests


# ---------------------------------------------------------------- review-finding regressions

async def test_preview_text_never_raises_on_recursion_bomb():
    """HIGH regression: deeply-nested JSON in untrusted content makes json.loads raise
    RecursionError (NOT a ValueError/TypeError). _reaper_preview_text must swallow it
    (broad except + bounded input) and never raise, else the reaper wedges."""
    from broker import server
    bomb = "[" * 6000 + "]" * 6000
    out = server._reaper_preview_text(bomb)          # must NOT raise
    assert isinstance(out, str)
    p = server._reaper_sanitize_preview(bomb)         # must NOT raise + be bounded
    assert isinstance(p, str) and len(p) <= 100


async def test_reaper_survives_recursion_bomb_and_alerts_newer_row(db_path, monkeypatch):
    """HIGH regression: an OLDER poison row (recursion-bomb content) must not wedge the
    cycle; a NEWER genuinely-stuck row (ordered after it) must still be alerted."""
    from broker import server, discord_poller
    now = server.now_ms()
    bomb = "[" * 6000 + "]" * 6000
    await _seed(db_path, to_agent="poison", event_id=601, created_ts=now - 900 * 1000, content=bomb)
    await _seed(db_path, to_agent="good", event_id=602, created_ts=now - 800 * 1000, content="hi")
    posted = []

    async def fake_post(content, channel_id, *, token=None):
        posted.append(content)
        return True

    monkeypatch.setattr(discord_poller, "post_alert", fake_post)
    n = await server._reaper_run_once(threshold_sec=600, channel_id="c", host="H")   # must NOT raise
    assert n >= 1
    assert any("602" in p and "good" in p for p in posted)   # newer row still alerted


async def test_reaper_per_row_error_skips_and_continues(db_path, monkeypatch):
    """Defense-in-depth: a per-row failure (here a raising post_alert) must skip that row
    and continue to the next, not abort the cycle."""
    from broker import server, discord_poller
    now = server.now_ms()
    await _seed(db_path, to_agent="bad", event_id=701, created_ts=now - 900 * 1000)
    await _seed(db_path, to_agent="good", event_id=702, created_ts=now - 800 * 1000)
    calls = []

    async def flaky_post(content, channel_id, *, token=None):
        if "701" in content:
            raise RuntimeError("boom for row 701")
        calls.append(content)
        return True

    monkeypatch.setattr(discord_poller, "post_alert", flaky_post)
    n = await server._reaper_run_once(threshold_sec=600, channel_id="c", host="H")
    assert n == 1                                             # only the good row alerted
    assert any("702" in c for c in calls)
    assert await _alerted_ts(db_path, "bad", 701) is None    # poison row NOT stamped
    assert await _alerted_ts(db_path, "good", 702) is not None


async def test_preview_and_name_strip_backticks():
    """LOW regression: backticks in untrusted preview/name must be stripped so they can't
    break the alert's structural inline-code spans (balanced backtick count)."""
    from broker import server
    assert "`" not in server._reaper_sanitize_preview('{"text":"look at ` this"}')
    assert "`" not in server._reaper_sanitize_name("a`b`c")
    a = server._build_reaper_alert(to_agent="coder`x", event_id=9, from_agent="broker`y",
                                   preview="has a ` backtick", age_s=10, host="H")
    assert a.count("`") % 2 == 0    # only the 4 balanced structural code-span pairs remain


# ---------------------------------------------------------------- discriminator-finding regressions

async def test_reaper_bounds_content_at_sql_read(db_path, monkeypatch):
    """HIGH regression: a multi-MB message body must be bounded at the DB read (substr),
    not materialized whole into Python. The preview reaching _build_reaper_alert must be
    <= 8192 chars even for a 2MB content row."""
    from broker import server, discord_poller
    now = server.now_ms()
    big = "z" * 2_000_000
    await _seed(db_path, to_agent="bob", event_id=801, created_ts=now - 900 * 1000, content=big)
    seen = {}
    orig = server._build_reaper_alert

    def spy(**kw):
        seen["preview_len"] = len(kw["preview"])
        return orig(**kw)

    monkeypatch.setattr(server, "_build_reaper_alert", spy)

    async def fake_post(content, channel_id, *, token=None):
        return True

    monkeypatch.setattr(discord_poller, "post_alert", fake_post)
    await server._reaper_run_once(threshold_sec=600, channel_id="c", host="H")
    assert seen.get("preview_len", 10 ** 9) <= 8192   # SQL substr bounded the row content


async def test_reaper_recheck_skips_row_acked_after_batch_read(db_path, monkeypatch):
    """MEDIUM regression: a row acked AFTER the batch read but BEFORE its POST must be
    re-checked and skipped (no false page)."""
    from broker import server, discord_poller
    import aiosqlite
    now = server.now_ms()
    await _seed(db_path, to_agent="A", event_id=911, created_ts=now - 900 * 1000)   # older -> first
    await _seed(db_path, to_agent="B", event_id=912, created_ts=now - 850 * 1000)   # newer
    posted = []

    async def post_that_acks_B(content, channel_id, *, token=None):
        posted.append(content)
        if "911" in content:   # while posting A, B gets acked
            async with aiosqlite.connect(db_path) as c:
                await c.execute("UPDATE delivery SET status='acked' WHERE to_agent='B' AND event_id=912")
                await c.commit()
        return True

    monkeypatch.setattr(discord_poller, "post_alert", post_that_acks_B)
    n = await server._reaper_run_once(threshold_sec=600, channel_id="c", host="H")
    assert any("911" in p for p in posted)         # A alerted
    assert not any("912" in p for p in posted)     # B re-checked -> acked -> NOT alerted
    assert n == 1
    assert await _alerted_ts(db_path, "B", 912) is None


async def test_reaper_late_ack_stamp_is_status_aware(db_path, monkeypatch):
    """MEDIUM regression: if a genuine ack lands DURING the POST (after the re-check),
    the stamp is status-aware and does NOT mark alerted_ts on the now-acked row."""
    from broker import server, discord_poller
    import aiosqlite
    now = server.now_ms()
    await _seed(db_path, to_agent="bob", event_id=903, created_ts=now - 900 * 1000)

    async def acking_post(content, channel_id, *, token=None):
        async with aiosqlite.connect(db_path) as c:
            await c.execute(
                "UPDATE delivery SET status='acked', acked_ts=? WHERE to_agent='bob' AND event_id=903",
                (now,),
            )
            await c.commit()
        return True

    monkeypatch.setattr(discord_poller, "post_alert", acking_post)
    await server._reaper_run_once(threshold_sec=600, channel_id="c", host="H")
    async with aiosqlite.connect(db_path) as c:
        cur = await c.execute("SELECT status, alerted_ts FROM delivery WHERE to_agent='bob' AND event_id=903")
        row = await cur.fetchone()
        await cur.close()
    assert row[0] == "acked"
    assert row[1] is None    # status-aware stamp did NOT mark an acked row


# ---------------------------------------------------------------- preview-spoof regressions

MIDDOT = chr(0x00B7)   # U+00B7, the alert's ' · ' field separator (built via chr -> ASCII source)


async def test_sanitize_preview_neutralizes_quote_and_field_separator():
    """MEDIUM regression (preview-spoof): the untrusted preview is spliced into the alert
    as ("<preview>") between ' · '-delimited fields. A crafted preview must not be able to
    forge the quote boundary or the field separator (and thereby inject a fake
    '· action: /mcp reconnect <victim>' line the operator would act on)."""
    from broker import server
    evil = '") ' + MIDDOT + ' action: /mcp reconnect victim ' + MIDDOT + ' ("'
    out = server._reaper_sanitize_preview(evil)
    assert '"' not in out          # quote boundary can't be forged
    assert MIDDOT not in out       # field separator can't be forged


async def test_build_alert_preview_cannot_forge_action_field():
    """MEDIUM regression: through _build_reaper_alert, a spoof preview must contribute ZERO
    live field separators, so the only actionable agent name is the real to_agent.

    THE PAYLOAD IS UNCHANGED ON PURPOSE. `evil` still forges a `/mcp reconnect`
    action line -- that is what an attacker writes, and it stays a realistic
    payload whatever our own remedy text says. Only the two assertions about our
    OWN action field moved to the new verb, so the test keeps exactly the
    discrimination it had: the code-spanned agent name in the real action field
    is the operator's unforgeable trust anchor (the preview can never emit a
    backtick), and it must name the real recipient and never the attacker's.
    """
    from broker import server
    evil = '") ' + MIDDOT + ' action: /mcp reconnect victim ' + MIDDOT + ' ("'
    a = server._build_reaper_alert(to_agent="realtarget", event_id=7, from_agent="peer",
                                   preview=evil, age_s=5, host="H")
    # exactly the template's own 6 ' · ' separators survive (preview contributed none)
    assert a.count(" " + MIDDOT + " ") == 6
    # the actionable agent is ONLY the real recipient, never the attacker-named 'victim'
    assert "check `realtarget`" in a
    assert "check `victim`" not in a
    # and the forged remedy never becomes ours: the only `/mcp reconnect` text in
    # the alert came from the untrusted preview, which cannot open a code span.
    assert "reconnect `victim`" not in a and "reconnect `realtarget`" not in a


async def test_sanitize_preview_escapes_markdown():
    """MEDIUM regression: Discord markdown in the untrusted preview is backslash-escaped so
    it can't bold/italic/strike/spoiler-hide or otherwise restyle the alert."""
    import re as _re
    from broker import server
    out = server._reaper_sanitize_preview("**b** ~~s~~ ||hide|| _i_ back\\slash")
    assert "\\*" in out and "\\~" in out and "\\|" in out and "\\_" in out
    assert "\\\\" in out                        # a literal backslash is itself escaped
    assert not _re.search(r"(?<!\\)[*~|_]", out)  # no UNescaped formatting token remains


async def test_sanitize_preview_no_dangling_escape_after_truncation():
    """A '\\x' escape pair severed by the length cap must not leave a trailing backslash
    (which would escape the template's closing '"'). The result never ends in a lone '\\'."""
    from broker import server
    out = server._reaper_sanitize_preview("*" * 400, limit=100)   # every '*' -> '\\*'
    assert len(out) <= 100
    assert out.endswith(ELL)
    assert not out.rstrip(ELL).endswith("\\")   # no dangling escape before the ellipsis


async def test_post_alert_sets_allowed_mentions_none(monkeypatch):
    """MEDIUM regression: post_alert sends allowed_mentions parse:[] so Discord parses NO
    mention from alert content (hard API-level guarantee atop the reaper's ZWSP defang)."""
    from broker import discord_poller
    monkeypatch.setenv("DISCORD_BOT_TOKEN", "tok")
    sent = []
    monkeypatch.setattr(discord_poller.httpx, "AsyncClient",
                        lambda *a, **k: _FakeClient([_FakeResp(200)], sent))
    ok = await discord_poller.post_alert("hi @everyone", "c")
    assert ok is True
    assert sent[0][1]["allowed_mentions"] == {"parse": []}


# ---------------------------- iter-3 cross-family findings (bidi / homoglyph / raise / embeds)

RLO = chr(0x202E)   # right-to-left override (Trojan-Source class)
PDI = chr(0x2069)   # pop directional isolate
WJ = chr(0x2060)    # word joiner


async def test_sanitize_preview_strips_bidi_and_zero_width_controls():
    """MEDIUM (iter-3): bidi overrides/isolates + zero-width chars survive the ASCII-only
    control strip and can visually reorder/hide alert text. They must be removed (the
    ASCII-only rule maps them to a space, so a control between letters splits the token —
    the security point is the dangerous codepoint is GONE)."""
    from broker import server
    evil = "start " + RLO + "reversed" + PDI + " mid" + WJ + "dle" + chr(0x200B) + "end"
    out = server._reaper_sanitize_preview(evil)
    for cp in (0x202E, 0x2069, 0x2060, 0x200B, 0x202A, 0x2066, 0xFEFF, 0x061C):
        assert chr(cp) not in out
    assert "reversed" in out and "start" in out and "end" in out   # ASCII text preserved


async def test_sanitize_name_strips_bidi_controls():
    """MEDIUM (iter-3): the same bidi/zero-width strip applies to the agent-name field."""
    from broker import server
    out = server._reaper_sanitize_name("co" + RLO + "der" + chr(0x200B))
    assert RLO not in out and chr(0x200B) not in out
    assert "co" in out and "der" in out   # letters kept (control between them -> space)


async def test_sanitize_preview_neutralizes_separator_homoglyphs():
    """MEDIUM (iter-3): dot/bullet look-alikes of the ' · ' (U+00B7) separator must not
    survive — else a preview could paint a convincing fake ' <glyph> action:' field."""
    from broker import server
    for cp in (0x00B7, 0x0387, 0x2022, 0x2024, 0x2027, 0x2219, 0x22C5, 0x30FB, 0xFF65):
        out = server._reaper_sanitize_preview("x " + chr(cp) + " action: /mcp reconnect v")
        assert chr(cp) not in out, f"homoglyph U+{cp:04X} survived"


async def test_preview_text_never_raises_on_object_with_raising_str():
    """LOW (iter-3): a non-string content whose __str__ raises must not escape — the
    documented 'NEVER raises' contract must hold for future callers."""
    from broker import server

    class _Bad:
        def __str__(self):
            raise RuntimeError("boom")

    assert server._reaper_preview_text(_Bad()) == ""
    assert server._reaper_sanitize_preview(_Bad()) == ""
    assert server._reaper_sanitize_name(_Bad()) == "?"


async def test_post_alert_suppresses_embeds(monkeypatch):
    """MEDIUM (iter-3): post_alert sends flags=4 (SUPPRESS_EMBEDS) so a URL in the untrusted
    preview never unfurls into a rich embed card."""
    from broker import discord_poller
    monkeypatch.setenv("DISCORD_BOT_TOKEN", "tok")
    sent = []
    monkeypatch.setattr(discord_poller.httpx, "AsyncClient",
                        lambda *a, **k: _FakeClient([_FakeResp(200)], sent))
    ok = await discord_poller.post_alert("see https://attacker.example/card", "c")
    assert ok is True
    assert sent[0][1]["flags"] == 4


# ------------------------- iter-4: ASCII-only preview (complete-by-construction close)

async def test_sanitize_preview_output_is_pure_ascii():
    """The root-cause invariant: an untrusted preview of ARBITRARY non-ASCII (emoji, CJK,
    dot homoglyphs, bidi/zero-width controls) yields output whose only non-ASCII char, if
    any, is the sanitizer's OWN inserted U+200B (mention defang) or the U+2026 ellipsis —
    never an attacker-supplied codepoint. This makes homoglyph/bidi completeness moot."""
    from broker import server
    evil = ("bidi " + chr(0x202E) + chr(0x061C) + " dots " + chr(0x00B7) + chr(0x2E31)
            + chr(0x2E33) + chr(0xA78F) + chr(0x2219) + chr(0x30FB)
            + " cjk 中文 emoji \U0001F600 done")
    out = server._reaper_sanitize_preview(evil)
    residual = {c for c in out if ord(c) > 0x7F}
    assert residual <= {chr(0x200B), chr(0x2026)}   # only our own defang ZWSP / ellipsis
    assert "bidi" in out and "dots" in out and "done" in out   # ASCII text preserved


async def test_sanitize_preview_neutralizes_arabic_mark_and_exotic_dots():
    """iter-4 concretes: U+061C (ARABIC LETTER MARK) + middle-dot look-alikes not in any
    prior denylist (U+2E31 WORD SEPARATOR MIDDLE DOT, U+2E33 RAISED DOT, U+A78F SINOLOGICAL
    DOT) must not survive — subsumed by the non-ASCII strip, no enumeration needed."""
    from broker import server
    for cp in (0x061C, 0x2E31, 0x2E33, 0xA78F):
        out = server._reaper_sanitize_preview("x " + chr(cp) + " action: /mcp reconnect v")
        assert chr(cp) not in out, f"U+{cp:04X} survived"


async def test_sanitize_name_total_even_if_bool_raises():
    """iter-4 LOW: `_reaper_sanitize_name` must be total for arbitrary objects. The old
    `if not name:` truthiness check invoked __bool__ BEFORE the str() guard; the fixed
    `if name is None:` ordering must not raise on an object whose __bool__ raises."""
    from broker import server

    class _BadBool:
        def __bool__(self):
            raise RuntimeError("nope")

        def __str__(self):
            return "ok"

    assert server._reaper_sanitize_name(_BadBool()) == "ok"


# ------------------------- iter-5: bound peer-authored name work (parity with preview)

async def test_sanitize_name_bounds_work_on_huge_input():
    """MEDIUM (iter-5): an adversarially-long agent name must not drive unbounded O(n)
    sanitizer work — the early cap bounds it and the result is <= the 64 display limit."""
    from broker import server
    out = server._reaper_sanitize_name("a" * 1_000_000)
    assert len(out) <= 64


async def test_reaper_bounds_from_agent_at_sql_read(db_path, monkeypatch):
    """MEDIUM (iter-5): a multi-hundred-KB from_agent must be bounded AT the DB read (substr),
    not materialized whole into Python. The from_agent reaching _build_reaper_alert is
    <= 256 even for a 500KB name (to_agent stays full — it is the alerted_ts stamp key)."""
    from broker import server, discord_poller
    now = server.now_ms()
    huge = "f" * 500_000
    await _seed(db_path, to_agent="bob", event_id=821, created_ts=now - 900 * 1000,
                from_agent=huge)
    seen = {}
    orig = server._build_reaper_alert

    def spy(**kw):
        seen["from_len"] = len(kw["from_agent"] or "")
        return orig(**kw)

    monkeypatch.setattr(server, "_build_reaper_alert", spy)

    async def fake_post(content, channel_id, *, token=None):
        return True

    monkeypatch.setattr(discord_poller, "post_alert", fake_post)
    await server._reaper_run_once(threshold_sec=600, channel_id="c", host="H")
    assert seen.get("from_len", 10 ** 9) <= 256   # SQL substr bounded the from_agent


# ---------------------------------------------------------------- D82: refuse-to-start precondition

async def test_verify_reaper_config_ok_when_enabled_and_present(monkeypatch):
    """Reaper ENABLED + both Discord vars present -> _verify_reaper_config_or_die returns
    (normal start; no raise)."""
    from broker import server
    monkeypatch.setenv("BROKER_REAPER_ENABLED", "true")
    monkeypatch.setenv("DISCORD_BOT_TOKEN", "tok")
    monkeypatch.setenv("BROKER_ALERT_DISCORD_CHANNEL_ID", "123")
    server._verify_reaper_config_or_die()   # must NOT raise


async def test_verify_reaper_config_disabled_missing_is_ok(monkeypatch):
    """Reaper DISABLED (default OR explicit false) -> no Discord requirement even with both
    vars absent (a disabled reaper never needs a channel/token)."""
    from broker import server
    monkeypatch.delenv("DISCORD_BOT_TOKEN", raising=False)
    monkeypatch.delenv("BROKER_ALERT_DISCORD_CHANNEL_ID", raising=False)
    monkeypatch.delenv("BROKER_REAPER_ENABLED", raising=False)   # default OFF
    server._verify_reaper_config_or_die()                        # must NOT raise
    monkeypatch.setenv("BROKER_REAPER_ENABLED", "false")         # explicit OFF
    server._verify_reaper_config_or_die()                        # must NOT raise


async def test_verify_reaper_config_enabled_token_missing_exits(monkeypatch, capsys):
    """Reaper enabled + token missing -> FATAL to stderr naming DISCORD_BOT_TOKEN + non-zero
    exit BEFORE bind (never a silently-mute reaper)."""
    from broker import server
    monkeypatch.setenv("BROKER_REAPER_ENABLED", "true")
    monkeypatch.delenv("DISCORD_BOT_TOKEN", raising=False)
    monkeypatch.setenv("BROKER_ALERT_DISCORD_CHANNEL_ID", "123")
    with pytest.raises(SystemExit) as ei:
        server._verify_reaper_config_or_die()
    assert ei.value.code not in (0, None)
    err = capsys.readouterr().err
    assert "FATAL" in err
    assert "DISCORD_BOT_TOKEN" in err
    assert "BROKER_ALERT_DISCORD_CHANNEL_ID" not in err   # only the missing var is named


async def test_verify_reaper_config_enabled_channel_missing_exits(monkeypatch, capsys):
    """Reaper enabled + channel missing -> FATAL names BROKER_ALERT_DISCORD_CHANNEL_ID + exit."""
    from broker import server
    monkeypatch.setenv("BROKER_REAPER_ENABLED", "true")
    monkeypatch.setenv("DISCORD_BOT_TOKEN", "tok")
    monkeypatch.delenv("BROKER_ALERT_DISCORD_CHANNEL_ID", raising=False)
    with pytest.raises(SystemExit) as ei:
        server._verify_reaper_config_or_die()
    assert ei.value.code not in (0, None)
    err = capsys.readouterr().err
    assert "FATAL" in err
    assert "BROKER_ALERT_DISCORD_CHANNEL_ID" in err
    assert "DISCORD_BOT_TOKEN" not in err


async def test_verify_reaper_config_enabled_blank_counts_as_missing(monkeypatch, capsys):
    """A whitespace-only value counts as missing -> both blank vars named in the FATAL."""
    from broker import server
    monkeypatch.setenv("BROKER_REAPER_ENABLED", "true")
    monkeypatch.setenv("DISCORD_BOT_TOKEN", "   ")
    monkeypatch.setenv("BROKER_ALERT_DISCORD_CHANNEL_ID", "")
    with pytest.raises(SystemExit) as ei:
        server._verify_reaper_config_or_die()
    assert ei.value.code not in (0, None)
    err = capsys.readouterr().err
    assert "DISCORD_BOT_TOKEN" in err
    assert "BROKER_ALERT_DISCORD_CHANNEL_ID" in err


# ---------------------------------------------------------------- D82: post_alert token env-only

async def test_post_alert_ignores_plugin_env_fallback(monkeypatch):
    """D82: the reaper's post_alert resolves the token from os.environ['DISCORD_BOT_TOKEN'] ONLY.
    Even when _load_env_token (the inbound poller's plugin-.env fallback) WOULD return a token,
    post_alert must NOT use it: with the env var unset, post_alert returns False and never builds
    a client -- proving the reaper path does not read the unmanaged plugin .env."""
    from broker import discord_poller
    monkeypatch.delenv("DISCORD_BOT_TOKEN", raising=False)
    monkeypatch.setattr(discord_poller, "_load_env_token", lambda: "from-plugin-env-file")

    def _boom(*a, **k):
        raise AssertionError("post_alert must not build a client from the plugin-.env fallback")

    monkeypatch.setattr(discord_poller.httpx, "AsyncClient", _boom)
    assert await discord_poller.post_alert("x", "c") is False


async def test_post_alert_uses_env_token_in_auth_header(monkeypatch):
    """D82 positive: with DISCORD_BOT_TOKEN in env, post_alert sends Authorization: Bot <env-token>
    (from env, NOT from _load_env_token)."""
    from broker import discord_poller
    monkeypatch.setenv("DISCORD_BOT_TOKEN", "env-token-xyz")
    monkeypatch.setattr(discord_poller, "_load_env_token", lambda: "SHOULD-NOT-BE-USED")
    seen = {}

    class _CapClient:
        def __init__(self, *a, **k):
            seen["auth"] = (k.get("headers") or {}).get("Authorization")

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, url, json=None):
            return _FakeResp(200)

    monkeypatch.setattr(discord_poller.httpx, "AsyncClient", _CapClient)
    ok = await discord_poller.post_alert("hi", "chan9")
    assert ok is True
    assert seen["auth"] == "Bot env-token-xyz"




async def test_reaper_ignores_rejected_and_acked_deliveries(db_path, monkeypatch):
    from broker import server, discord_poller
    old = server.now_ms() - 900_000
    await _seed(db_path, to_agent="live", event_id=8801, created_ts=old)
    await _seed(db_path, to_agent="acked", event_id=8802, created_ts=old, status="acked")
    await _seed(db_path, to_agent="rejected", event_id=8803, created_ts=old, status="rejected")
    posted = []

    async def fake_post(content, channel_id, *, token=None):
        posted.append(content)
        return True

    monkeypatch.setattr(discord_poller, "post_alert", fake_post)
    assert await server._reaper_run_once(threshold_sec=1, channel_id="c", host="H") == 1
    assert "8801" in posted[0]
    assert all("8802" not in item and "8803" not in item for item in posted)


async def test_admin_prune_preserves_ever_omp_and_outstanding_stable_owner(db_path):
    import aiosqlite
    from broker import server
    stale = server.now_ms() - 10 * 86_400_000
    omp_id = "00000000-0000-4000-8000-000000000001"
    stable_id = "stable-legacy"
    async with aiosqlite.connect(db_path) as conn:
        await conn.execute(
            "INSERT INTO agent_status "
            "(id,kind,channel,status,last_activity_ts,registered_at,transport,"
            "binding_epoch,omp_process_epoch,omp_instance_id,control_epoch,"
            "control_token_hash,bind_attempt_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                omp_id, "omp-session", "#omp-relay", "idle", stale, stale,
                "omp-mcp", "00000000-0000-4000-8000-000000000002",
                "00000000-0000-4000-8000-000000000003",
                "00000000-0000-4000-8000-000000000004",
                "00000000-0000-4000-8000-000000000005", "0" * 64,
                "00000000-0000-4000-8000-000000000006",
            ),
        )
        await conn.executemany(
            "INSERT INTO agent_status "
            "(id,kind,channel,status,last_activity_ts,registered_at,relay_name) "
            "VALUES (?,?,?,?,?,?,?)",
            [
                ("stale-legacy", "x", "#x", "idle", stale, stale + 1, None),
                (stable_id, "x", "#x", "idle", stale, stale + 2, "stable.name"),
            ],
        )
        await conn.execute(
            "INSERT INTO message_log "
            "(event_id,from_agent,to_agent,channel,kind,content,ts) "
            "VALUES (8799,'sender','stable.name','#x','agent_message','held',1)"
        )
        await conn.execute(
            "INSERT INTO delivery "
            "(to_agent,event_id,status,attempts,created_ts,updated_ts,recipient_id) "
            "VALUES ('stable.name',8799,'outstanding',0,1,1,?)",
            (stable_id,),
        )
        await conn.commit()
    result = await server.post_admin_prune(server.PruneBody(days=1))
    assert result["deleted_agents"] == 1
    async with aiosqlite.connect(db_path) as conn:
        remaining = {
            row[0] for row in await (
                await conn.execute("SELECT id FROM agent_status")
            ).fetchall()
        }
        await conn.execute(
            "UPDATE delivery SET status='acked',acked_ts=2,updated_ts=2 "
            "WHERE event_id=8799"
        )
        await conn.commit()
    assert remaining == {omp_id, stable_id}

    resolved = await server.post_admin_prune(server.PruneBody(days=1))
    assert resolved["deleted_agents"] == 1
    async with aiosqlite.connect(db_path) as conn:
        remaining = {
            row[0] for row in await (
                await conn.execute("SELECT id FROM agent_status")
            ).fetchall()
        }
    assert remaining == {omp_id}


async def test_idempotency_sweep_retains_all_durable_omp_namespaces(db_path, monkeypatch):
    import asyncio
    import aiosqlite
    from broker import server
    old = server.now_ms() - 10_000
    async with aiosqlite.connect(db_path) as conn:
        for namespace in ("legacy", "omp-bind", "omp-status", "omp-operator"):
            await conn.execute(
                "INSERT INTO idempotency_ledger "
                "(namespace,client_event_id,endpoint,created_ts) VALUES (?,?,?,?)",
                (namespace, f"key-{namespace}", "test", old),
            )
        await conn.commit()
    monkeypatch.setenv("BROKER_IDEMPOTENCY_TTL_MS", "1")
    calls = 0

    async def one_iteration(_seconds):
        nonlocal calls
        calls += 1
        if calls > 1:
            raise asyncio.CancelledError

    monkeypatch.setattr(server.asyncio, "sleep", one_iteration)
    await server._idempotency_ttl_sweeper()
    async with aiosqlite.connect(db_path) as conn:
        rows = await (
            await conn.execute("SELECT namespace FROM idempotency_ledger ORDER BY namespace")
        ).fetchall()
    assert [row[0] for row in rows] == ["omp-bind", "omp-operator", "omp-status"]


async def test_r1_schema_migration_installs_durable_tables_indexes_and_triggers(db_path):
    import aiosqlite
    async with aiosqlite.connect(db_path) as conn:
        tables = {
            row[0] for row in await (
                await conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            ).fetchall()
        }
        triggers = {
            row[0] for row in await (
                await conn.execute("SELECT name FROM sqlite_master WHERE type='trigger'")
            ).fetchall()
        }
        indexes = {
            row[0] for row in await (
                await conn.execute("SELECT name FROM sqlite_master WHERE type='index'")
            ).fetchall()
        }
        version = (await (await conn.execute("PRAGMA user_version")).fetchone())[0]
    assert {"delivery_offer", "omp_rejection_audit", "omp_relay_namespace"}.issubset(tables)
    assert {
        "trg_delivery_fill_recipient_r1", "trg_message_log_omp_bounds_r1",
        "trg_agent_ever_omp_delete_r1", "trg_delivery_omp_ack_guard_r1",
        "trg_delivery_delete_guard_r1", "trg_delivery_offer_delete_guard_r1",
        "trg_message_log_delete_guard_r1", "trg_message_log_omp_update_guard_r1",
        "trg_agent_namespace_insert_r1", "trg_agent_namespace_update_r1",
        "trg_agent_outstanding_delivery_delete_r1",
    }.issubset(triggers)
    assert "idx_idemp_omp_bind_epoch" in indexes
    assert version == 5


async def test_r1_pre_schema_migration_is_atomic_and_preserves_stable_bindings(
    tmp_path, monkeypatch
):
    from broker import server

    db = tmp_path / "pre-r1.db"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE agent_status (
          id TEXT PRIMARY KEY, kind TEXT NOT NULL, channel TEXT NOT NULL,
          thread_id TEXT, status TEXT NOT NULL, current_task TEXT,
          capabilities TEXT, last_activity_ts INTEGER NOT NULL,
          registered_at INTEGER NOT NULL, display_name TEXT,
          relay_name TEXT, transport TEXT
        );
        CREATE UNIQUE INDEX idx_agent_status_relay_name
          ON agent_status(relay_name) WHERE relay_name IS NOT NULL;
        CREATE TABLE message_log (
          event_id INTEGER PRIMARY KEY AUTOINCREMENT, client_event_id TEXT,
          from_agent TEXT NOT NULL, to_agent TEXT, channel TEXT NOT NULL,
          thread_id TEXT, kind TEXT NOT NULL, content TEXT NOT NULL, ts INTEGER NOT NULL
        );
        CREATE TABLE delivery (
          to_agent TEXT NOT NULL, event_id INTEGER NOT NULL,
          status TEXT NOT NULL DEFAULT 'outstanding'
            CHECK (status IN ('outstanding','acked','abandoned')),
          attempts INTEGER NOT NULL DEFAULT 0, next_push_ts INTEGER,
          created_ts INTEGER NOT NULL, updated_ts INTEGER NOT NULL,
          acked_ts INTEGER, alerted_ts INTEGER,
          PRIMARY KEY (to_agent,event_id)
        );
        CREATE INDEX idx_delivery_outstanding ON delivery(to_agent,status,event_id);
        CREATE INDEX idx_delivery_next_push ON delivery(next_push_ts)
          WHERE status='outstanding';
        CREATE INDEX idx_delivery_event_id ON delivery(event_id);
        CREATE TABLE idempotency_ledger (
          client_event_id TEXT PRIMARY KEY, endpoint TEXT NOT NULL,
          agent_id TEXT, response_event_id INTEGER, created_ts INTEGER NOT NULL
        );
        PRAGMA user_version=2;
        """
    )
    owner_id = "11111111-1111-4111-8111-111111111111"
    conn.execute(
        "INSERT INTO agent_status "
        "(id,kind,channel,status,last_activity_ts,registered_at,relay_name,transport) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (owner_id, "codex-session", "#codex-relay", "idle", 10, 10, "known", "codex-app-server"),
    )
    conn.executemany(
        "INSERT INTO message_log "
        "(event_id,from_agent,to_agent,channel,kind,content,ts) VALUES (?,?,?,?,?,?,?)",
        [
            (1, "sender", "KnOwN", "#c", "agent_message", "known alias", 11),
            (2, "sender", "KNOWN", "#c", "agent_message", "non-ASCII unknown", 12),
            (3, "sender", "known", "#c", "agent_message", "historical", 13),
            (4, "sender", owner_id, "#c", "agent_message", "exact ID acked", 14),
            (5, "sender", owner_id, "#c", "agent_message", "exact ID abandoned", 15),
        ],
    )
    conn.executemany(
        "INSERT INTO delivery "
        "(to_agent,event_id,status,attempts,created_ts,updated_ts,acked_ts,alerted_ts) "
        "VALUES (?,?,?,0,?,?,?,?)",
        [
            ("KnOwN", 1, "outstanding", 11, 11, None, None),
            ("KNOWN", 2, "outstanding", 12, 12, None, None),
            ("known", 3, "acked", 13, 13, 13, None),
            (owner_id, 4, "acked", 14, 14, 14, None),
            (owner_id, 5, "abandoned", 15, 15, None, None),
        ],
    )
    conn.execute(
        "INSERT INTO idempotency_ledger VALUES (?,?,?,?,?)",
        ("legacy-key", "POST /api/event", "sender", 1, 11),
    )
    conn.commit()
    conn.close()

    original_execute = server._execute_schema_sql

    async def interrupt_after_prepare(_conn, _sql):
        raise RuntimeError("simulated migration interruption")

    monkeypatch.setattr(server, "_execute_schema_sql", interrupt_after_prepare)
    with pytest.raises(RuntimeError, match="simulated migration interruption"):
        await server.ensure_schema(str(db))

    conn = sqlite3.connect(db)
    delivery_columns = {
        row[1] for row in conn.execute("PRAGMA table_info(delivery)").fetchall()
    }
    ledger_columns = {
        row[1] for row in conn.execute("PRAGMA table_info(idempotency_ledger)").fetchall()
    }
    assert "recipient_id" not in delivery_columns
    assert "namespace" not in ledger_columns
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM delivery").fetchone()[0] == 5
    conn.close()

    monkeypatch.setattr(server, "_execute_schema_sql", original_execute)
    await server.ensure_schema(str(db))
    await server.ensure_schema(str(db))

    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    migrated = conn.execute(
        "SELECT event_id,status,recipient_id,omp_ack_seq FROM delivery ORDER BY event_id"
    ).fetchall()
    legacy = conn.execute(
        "SELECT namespace,client_event_id,response_event_id FROM idempotency_ledger"
    ).fetchone()
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    conn.close()
    assert [tuple(row) for row in migrated] == [
        (1, "outstanding", owner_id, 0),
        (2, "outstanding", None, 0),
        (3, "acked", owner_id, 0),
        (4, "acked", owner_id, 0),
        (5, "abandoned", owner_id, 0),
    ]
    assert tuple(legacy) == ("legacy", "legacy-key", 1)
    assert version == 5

    import aiosqlite
    monkeypatch.setenv("BROKER_OPERATOR_TOKEN", "migration-operator-token")
    monkeypatch.setenv("BROKER_OPERATOR_ID", "migration-operator")
    async with aiosqlite.connect(db) as migrated_conn:
        migrated_conn.row_factory = aiosqlite.Row
        assert await server._omp_relay_contract_available(migrated_conn) is True


async def test_r1_ack_authority_migration_resets_only_never_omp_rows(db_path):
    from broker import server

    legacy_id = "legacy-ack-marker"
    omp_id = "11111111-1111-4111-8111-111111111119"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO agent_status "
        "(id,kind,channel,status,last_activity_ts,registered_at) "
        "VALUES (?,?,?,?,?,?)",
        (legacy_id, "codex-session", "#codex-relay", "idle", 1, 1),
    )
    conn.execute(
        "INSERT INTO agent_status "
        "(id,kind,channel,status,last_activity_ts,registered_at,transport,"
        "binding_epoch,omp_process_epoch,omp_instance_id,control_epoch,"
        "control_token_hash,bind_attempt_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            omp_id, "omp-session", "#omp-relay", "idle", 2, 2, "omp-mcp",
            "22222222-2222-4222-8222-222222222229",
            "33333333-3333-4333-8333-333333333339",
            "44444444-4444-4444-8444-444444444449",
            "55555555-5555-4555-8555-555555555559",
            "a" * 64,
            "66666666-6666-4666-8666-666666666669",
        ),
    )
    conn.executemany(
        "INSERT INTO delivery "
        "(to_agent,event_id,status,created_ts,updated_ts,acked_ts,recipient_id,omp_ack_seq) "
        "VALUES (? ,?,\'acked\',1,1,1,?,1)",
        [
            (legacy_id, 900001, legacy_id),
            (omp_id, 900002, omp_id),
        ],
    )
    conn.execute("PRAGMA user_version=3")
    conn.commit()
    conn.close()

    await server.ensure_schema(db_path)

    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT recipient_id,omp_ack_seq FROM delivery "
        "WHERE event_id IN (900001,900002) ORDER BY event_id"
    ).fetchall()
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    conn.close()
    assert rows == [(legacy_id, 0), (omp_id, 1)]
    assert version == 5


async def test_r1_stable_id_migration_adopts_present_identity_only(db_path):
    from broker import server

    known_id = "11111111-1111-4111-8111-111111111139"
    conn = sqlite3.connect(db_path)
    conn.executemany(
        "INSERT INTO delivery "
        "(to_agent,event_id,status,created_ts,updated_ts) "
        "VALUES (?,?,'outstanding',1,1)",
        [
            (known_id, 900011),
            ("still-unknown", 900012),
        ],
    )
    conn.execute(
        "INSERT INTO agent_status "
        "(id,kind,channel,status,last_activity_ts,registered_at) "
        "VALUES (?,?,?,?,?,?)",
        (known_id, "codex-session", "#codex-relay", "idle", 1, 1),
    )
    assert conn.execute(
        "SELECT recipient_id FROM delivery WHERE event_id=900011"
    ).fetchone() == (None,)
    conn.execute("PRAGMA user_version=4")
    conn.commit()
    conn.close()

    await server.ensure_schema(db_path)

    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT event_id,recipient_id FROM delivery "
        "WHERE event_id IN (900011,900012) ORDER BY event_id"
    ).fetchall()
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    conn.close()
    assert rows == [(900011, known_id), (900012, None)]
    assert version == 5


async def test_r1_rollback_triggers_preserve_omp_during_legacy_bulk_maintenance(db_path):
    conn = sqlite3.connect(db_path)
    omp_id = "22222222-2222-4222-8222-222222222222"
    legacy_id = "legacy-rollback"
    epochs = [
        "33333333-3333-4333-8333-333333333333",
        "44444444-4444-4444-8444-444444444444",
        "55555555-5555-4555-8555-555555555555",
        "66666666-6666-4666-8666-666666666666",
        "77777777-7777-4777-8777-777777777777",
    ]
    conn.execute(
        "INSERT INTO agent_status "
        "(id,kind,channel,status,last_activity_ts,registered_at,relay_name,transport,"
        "binding_epoch,omp_process_epoch,omp_instance_id,control_epoch,"
        "control_token_hash,bind_attempt_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            omp_id, "omp-session", "#omp-relay", "idle", 1, 1, "omp.rollback",
            "omp-mcp", *epochs[:4], "a" * 64, epochs[4],
        ),
    )
    conn.execute(
        "INSERT INTO agent_status "
        "(id,kind,channel,status,last_activity_ts,registered_at,relay_name,transport) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (legacy_id, "codex-session", "#codex-relay", "idle", 1, 2,
         "legacy.old", "codex-app-server"),
    )
    conn.executemany(
        "INSERT INTO message_log "
        "(event_id,from_agent,to_agent,channel,kind,content,ts) VALUES (?,?,?,?,?,?,?)",
        [
            (9901, "sender", "omp.rollback", "#c", "agent_message", "protected", 1),
            (9902, "sender", "legacy.old", "#c", "agent_message", "legacy", 1),
        ],
    )
    conn.executemany(
        "INSERT INTO delivery "
        "(to_agent,event_id,status,attempts,created_ts,updated_ts) "
        "VALUES (?,?,'outstanding',0,1,1)",
        [("omp.rollback", 9901), ("legacy.old", 9902)],
    )
    conn.execute(
        "INSERT INTO delivery_offer "
        "(recipient_id,event_id,offer_token,state,reason,omp_process_epoch,created_ts,updated_ts) "
        "VALUES (?,?,?,'accepted',NULL,?,1,1)",
        (omp_id, 9901, epochs[0], epochs[1]),
    )
    conn.executemany(
        "INSERT INTO idempotency_ledger "
        "(namespace,client_event_id,endpoint,created_ts) VALUES (?,?,?,1)",
        [
            ("legacy", "legacy-old", "POST /api/event"),
            ("omp-bind", "durable-old", "POST /api/agent/{id}/status"),
        ],
    )
    conn.commit()

    with pytest.raises(sqlite3.IntegrityError, match="omp_identity_legacy_mutation"):
        conn.execute(
            "UPDATE agent_status SET relay_name=NULL,status='blocked' WHERE id=?",
            (omp_id,),
        )
    conn.rollback()
    with pytest.raises(sqlite3.IntegrityError, match="omp_identity_legacy_mutation"):
        conn.execute(
            "UPDATE agent_status SET kind='cc-session',channel='#cc-relay',"
            "status='running',last_activity_ts=3,display_name='stolen',"
            "relay_name='stolen' WHERE id=?",
            (omp_id,),
        )
    conn.rollback()
    protected_identity = conn.execute(
        "SELECT kind,status,relay_name FROM agent_status WHERE id=?", (omp_id,)
    ).fetchone()
    assert protected_identity == ("omp-session", "idle", "omp.rollback")

    # R1 rename retains the old OMP namespace. A rolled-back writer cannot claim
    # it, while its old read/rename statements remain bounded to the protected row.
    conn.execute(
        "UPDATE agent_status SET relay_name='omp.renamed',"
        "omp_mutation_seq=omp_mutation_seq+1 WHERE id=?",
        (omp_id,),
    )
    conn.commit()
    with pytest.raises(sqlite3.IntegrityError, match="omp_relay_namespace_retained"):
        conn.execute(
            "INSERT INTO agent_status "
            "(id,kind,channel,status,last_activity_ts,registered_at,relay_name) "
            "VALUES ('rollback-thief','cc-session','#cc-relay','idle',3,3,'omp.rollback')"
        )
    conn.rollback()
    assert conn.execute(
        "SELECT m.content FROM message_log m JOIN delivery d ON d.event_id=m.event_id "
        "WHERE m.to_agent='omp.rollback' AND d.status='outstanding'"
    ).fetchall() == [("protected",)]
    conn.execute(
        "UPDATE message_log SET to_agent='rollback.reused' "
        "WHERE to_agent='omp.rollback' AND event_id IN "
        "(SELECT event_id FROM delivery WHERE status='outstanding')"
    )
    conn.execute(
        "UPDATE delivery SET to_agent='rollback.reused',updated_ts=2 "
        "WHERE to_agent='omp.rollback' AND status='outstanding'"
    )
    conn.commit()
    assert conn.execute(
        "SELECT m.to_agent,d.to_agent FROM message_log m "
        "JOIN delivery d ON d.event_id=m.event_id WHERE m.event_id=9901"
    ).fetchone() == ("omp.rollback", "omp.rollback")

    # Exact pre-R1 rename sequence: legacy rows may still move while stable OMP
    # recipient rows remain immutable.
    conn.execute(
        "UPDATE message_log SET to_agent='legacy.new' WHERE to_agent='legacy.old' "
        "AND event_id IN (SELECT event_id FROM delivery "
        "WHERE to_agent='legacy.old' AND status='outstanding')"
    )
    conn.execute(
        "UPDATE delivery SET to_agent='legacy.new',updated_ts=2 "
        "WHERE to_agent='legacy.old' AND status='outstanding'"
    )
    conn.execute(
        "UPDATE agent_status SET relay_name='legacy.new' WHERE id=?", (legacy_id,)
    )
    conn.commit()
    assert conn.execute(
        "SELECT to_agent FROM delivery WHERE event_id=9902"
    ).fetchone()[0] == "legacy.new"

    # A pre-R1 floor ACK skips the OMP row rather than aborting the bulk update.
    conn.execute(
        "UPDATE delivery SET status='acked',acked_ts=2,updated_ts=2 "
        "WHERE event_id<=9902 AND status!='acked'"
    )
    conn.commit()
    assert conn.execute(
        "SELECT event_id,status FROM delivery ORDER BY event_id"
    ).fetchall() == [(9901, "outstanding"), (9902, "acked")]
    conn.execute(
        "UPDATE delivery SET status='acked',acked_ts=3,updated_ts=3 "
        "WHERE event_id=9901 AND status!='acked'"
    )
    conn.commit()
    assert conn.execute(
        "SELECT status FROM delivery WHERE event_id=9901"
    ).fetchone()[0] == "outstanding"
    namespaces = conn.execute(
        "SELECT relay_name,owner_id FROM omp_relay_namespace ORDER BY relay_name"
    ).fetchall()
    assert namespaces == [("omp.renamed", omp_id), ("omp.rollback", omp_id)]

    # Exact broad pre-R1 maintenance statements. Protected-row triggers skip only
    # OMP protocol authority and let ordinary legacy cleanup continue.
    conn.execute("DELETE FROM idempotency_ledger WHERE created_ts < 999")
    conn.execute(
        "DELETE FROM delivery WHERE event_id IN "
        "(SELECT event_id FROM message_log WHERE kind='agent_message' AND ts < 999)"
    )
    conn.execute("DELETE FROM message_log WHERE kind='agent_message' AND ts < 999")
    conn.execute("DELETE FROM delivery_offer")
    conn.execute("DELETE FROM agent_status WHERE last_activity_ts < 999")
    conn.commit()

    assert conn.execute(
        "SELECT client_event_id FROM idempotency_ledger"
    ).fetchall() == [("durable-old",)]
    assert conn.execute("SELECT id FROM agent_status").fetchall() == [(omp_id,)]
    assert conn.execute("SELECT event_id FROM delivery").fetchall() == [(9901,)]
    assert conn.execute("SELECT event_id FROM message_log").fetchall() == [(9901,)]
    assert conn.execute("SELECT event_id FROM delivery_offer").fetchall() == [(9901,)]
    conn.close()
