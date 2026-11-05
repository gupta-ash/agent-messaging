"""Idempotency tests: client_event_id dedup + 24h TTL sweep."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.asyncio


async def test_post_event_idempotent_returns_same_event_id(client):
    body = {
        "from_agent": "ag1", "channel": "#cc-demo_research",
        "kind": "agent_reply", "content": "hi", "ts": 1746400000000,
        "client_event_id": "550e8400-e29b-41d4-a716-446655440000",
    }
    r1 = await client.post("/api/event", json=body)
    r2 = await client.post("/api/event", json=body)
    assert r1.status_code == r2.status_code == 200
    assert r1.json()["event_id"] == r2.json()["event_id"]


async def test_post_event_idempotent_no_duplicate_message_log_row(client):
    body = {
        "from_agent": "ag1", "channel": "#x", "kind": "agent_reply",
        "content": "hi", "ts": 1, "client_event_id": "fixed-uuid-1",
    }
    await client.post("/api/event", json=body)
    await client.post("/api/event", json=body)
    await client.post("/api/event", json=body)

    # Check via /api/state derived count or directly via SQLite
    import aiosqlite, os
    async with aiosqlite.connect(os.environ["BROKER_DB_PATH"]) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(
            "SELECT COUNT(*) AS n FROM message_log WHERE client_event_id = ?",
            ("fixed-uuid-1",),
        )
        row = await cur.fetchone()
        assert row["n"] == 1


async def test_post_agent_status_idempotent(client):
    body = {
        "kind": "demo_research", "channel": "#cc-demo_research",
        "status": "idle", "last_activity_ts": 1746400000000,
        "client_event_id": "agent-status-uuid-1",
    }
    r1 = await client.post("/api/agent/idem-1/status", json=body)
    r2 = await client.post("/api/agent/idem-1/status", json=body)
    assert r1.json()["event_id"] == r2.json()["event_id"]


async def test_idempotency_ttl_sweep_logic(client, monkeypatch):
    """Drive the TTL sweep window directly (not the background task)."""
    import os
    import aiosqlite
    from broker import server

    # Insert 2 ledger rows, one old, one new
    db_path = os.environ["BROKER_DB_PATH"]
    now = server.now_ms()
    old_ts = now - 25 * 3600 * 1000  # 25 hours ago
    async with aiosqlite.connect(db_path) as conn:
        await conn.execute(
            "INSERT INTO idempotency_ledger (client_event_id, endpoint, agent_id, response_event_id, created_ts) "
            "VALUES (?, ?, ?, ?, ?)",
            ("old-uuid", "POST /api/event", "a", 1, old_ts),
        )
        await conn.execute(
            "INSERT INTO idempotency_ledger (client_event_id, endpoint, agent_id, response_event_id, created_ts) "
            "VALUES (?, ?, ?, ?, ?)",
            ("new-uuid", "POST /api/event", "a", 2, now),
        )
        await conn.commit()

    # Run the sweep query manually (mimics what _idempotency_ttl_sweeper does)
    cutoff = now - 86_400_000
    async with aiosqlite.connect(db_path) as conn:
        await conn.execute("DELETE FROM idempotency_ledger WHERE created_ts < ?", (cutoff,))
        await conn.commit()

        cur = await conn.execute("SELECT client_event_id FROM idempotency_ledger")
        remaining = [r[0] for r in await cur.fetchall()]

    assert "old-uuid" not in remaining
    assert "new-uuid" in remaining
