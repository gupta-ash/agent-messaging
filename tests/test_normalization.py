"""Normalization tests:
- ts accepts ISO-8601 strings; broker stores epoch-ms
- from_agent NOT NULL convention (agent.id, user:<id>, system:broker)
- discord_cursors thread_id null/omitted -> ''
- channel name vs id: broker stores 'channel' as human name, 'channel_id' only in discord_cursors
"""

from __future__ import annotations

import os

import aiosqlite
import pytest

pytestmark = pytest.mark.asyncio


# ----- Timestamp normalization -----

async def test_ts_accepts_iso8601_with_z(client):
    r = await client.post("/api/event", json={
        "from_agent": "a", "channel": "#x", "kind": "user_msg",
        "content": "x", "ts": "2026-05-04T12:00:00Z",
    })
    assert r.status_code == 200, r.text
    # Verify stored as epoch-ms
    async with aiosqlite.connect(os.environ["BROKER_DB_PATH"]) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute("SELECT ts FROM message_log WHERE event_id = ?",
                                 (r.json()["event_id"],))
        row = await cur.fetchone()
    assert isinstance(row["ts"], int)
    # 2026-05-04T12:00:00 UTC == 1777896000000 ms
    assert row["ts"] == 1777896000000


async def test_ts_accepts_iso8601_with_offset(client):
    r = await client.post("/api/event", json={
        "from_agent": "a", "channel": "#x", "kind": "user_msg",
        "content": "x", "ts": "2026-05-04T07:00:00-05:00",
    })
    assert r.status_code == 200
    # 2026-05-04T07:00:00-05:00 == 2026-05-04T12:00:00Z == 1777896000000
    async with aiosqlite.connect(os.environ["BROKER_DB_PATH"]) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute("SELECT ts FROM message_log WHERE event_id = ?",
                                 (r.json()["event_id"],))
        row = await cur.fetchone()
    assert row["ts"] == 1777896000000


async def test_ts_int_passes_through(client):
    r = await client.post("/api/event", json={
        "from_agent": "a", "channel": "#x", "kind": "user_msg",
        "content": "x", "ts": 1746400000000,
    })
    async with aiosqlite.connect(os.environ["BROKER_DB_PATH"]) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute("SELECT ts FROM message_log WHERE event_id = ?",
                                 (r.json()["event_id"],))
        row = await cur.fetchone()
    assert row["ts"] == 1746400000000


async def test_ts_invalid_string_returns_422(client):
    # F6 fix: Pydantic ts validator surfaces ValueError as 422 (validation_error), not 500.
    r = await client.post("/api/event", json={
        "from_agent": "a", "channel": "#x", "kind": "user_msg",
        "content": "x", "ts": "not-a-date",
    })
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "validation_error"


async def test_last_activity_ts_invalid_string_returns_422(client):
    # F6 fix: same validator on agent_status.last_activity_ts.
    r = await client.post("/api/agent/v1/status", json={
        "kind": "demo_research", "channel": "#cc-demo_research",
        "status": "idle", "last_activity_ts": "not-a-date",
    })
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "validation_error"


async def test_discord_cursor_last_check_ts_invalid_returns_422(client):
    # F6 fix: same validator on discord_cursors.last_check_ts.
    r = await client.post("/api/discord/cursors", json={
        "channel_id": "v", "last_seen_message_id": "1",
        "last_check_ts": "garbage",
    })
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "validation_error"


# ----- from_agent convention -----

async def test_from_agent_accepts_user_prefix(client):
    r = await client.post("/api/event", json={
        "from_agent": "user:111111111111111111",
        "channel": "#cc-demo_research", "kind": "user_msg",
        "content": "from Discord catch-up", "ts": 1746400000000,
    })
    assert r.status_code == 200


async def test_from_agent_accepts_system_broker(client):
    # v1.7.1: kind=error now requires envelope content per D38; updated from legacy string.
    import json as _json
    envelope = {
        "type": "system_broker_test",
        "severity": "error",
        "errors": [{"kind": "test", "detail": "broker emitted", "auto_recovered": False}],
        "context": {},
    }
    r = await client.post("/api/event", json={
        "from_agent": "system:broker",
        "channel": "#cc-demo_research", "kind": "error",
        "content": _json.dumps(envelope), "ts": 1746400000000,
    })
    assert r.status_code == 200


async def test_from_agent_accepts_agent_id(client):
    r = await client.post("/api/event", json={
        "from_agent": "demo-research-1",
        "channel": "#cc-demo_research", "kind": "agent_reply",
        "content": "answer", "ts": 1746400000000,
    })
    assert r.status_code == 200


async def test_from_agent_rejects_empty(client):
    r = await client.post("/api/event", json={
        "from_agent": "",
        "channel": "#x", "kind": "user_msg",
        "content": "x", "ts": 1,
    })
    assert r.status_code == 422


# ----- Channel name vs id semantics -----
# Per contract § 3: broker stores `channel` (human name) on agent_status + message_log;
# `channel_id` (snowflake) is only in discord_cursors.

async def test_agent_status_channel_field_stores_human_name(client):
    await client.post("/api/agent/c1/status", json={
        "kind": "demo_research", "channel": "#cc-demo_research",
        "status": "idle", "last_activity_ts": 1,
    })
    async with aiosqlite.connect(os.environ["BROKER_DB_PATH"]) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute("SELECT channel FROM agent_status WHERE id='c1'")
        row = await cur.fetchone()
    assert row["channel"] == "#cc-demo_research"


async def test_discord_cursors_channel_id_stores_snowflake(client):
    snowflake = "1234567890123456789"
    await client.post("/api/discord/cursors", json={
        "channel_id": snowflake, "thread_id": "9876543210987654321",
        "last_seen_message_id": "111", "last_check_ts": 1,
    })
    r = await client.get("/api/discord/cursors", params={"channel_id": snowflake})
    cursors = r.json()["cursors"]
    assert len(cursors) == 1
    assert cursors[0]["channel_id"] == snowflake


# ----- v1.3.1 (D29): display_name field semantics on agent_status -----


async def test_agent_status_display_name_field_persists_across_upsert(client):
    """v1.3.1 (D29) relay-pattern test: relay's hook will POST status with
    display_name on every SessionStart/heartbeat. Verify that re-POSTing
    with display_name supplied again preserves the value across the UPSERT
    (i.e., second POST with status='running' + display_name='Test1' still
    has display_name='Test1' after stored)."""
    initial = {
        "kind": "demo_research",
        "channel": "#cc-demo_research",
        "status": "idle",
        "last_activity_ts": 1746400000000,
        "display_name": "Test1",
    }
    r1 = await client.post("/api/agent/persist-1/status", json=initial)
    assert r1.status_code == 200, r1.text
    # Second POST: status changes to 'running'; display_name supplied again
    second = {
        "kind": "demo_research",
        "channel": "#cc-demo_research",
        "status": "running",
        "last_activity_ts": 1746400000001,
        "display_name": "Test1",
    }
    r2 = await client.post("/api/agent/persist-1/status", json=second)
    assert r2.status_code == 200, r2.text
    # Read back via DB to verify storage layer (not just response shape)
    async with aiosqlite.connect(os.environ["BROKER_DB_PATH"]) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(
            "SELECT status, display_name FROM agent_status WHERE id='persist-1'"
        )
        row = await cur.fetchone()
    assert row["status"] == "running"
    assert row["display_name"] == "Test1"
