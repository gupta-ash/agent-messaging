"""War-room v0 (M4 deliverable b): focused coverage for GET /api/agent/{id}/status.

Verifies the existing endpoint (broker/server.py L1015-1047) against the
contract.md § 1 field set, the queue_depth = COUNT(handoffs after last_activity_ts)
math, and the 404 detail string. No endpoint changes — pure gap-fill on top of
the happy-path case in test_endpoints.py L43-56.
"""

from __future__ import annotations

import os

import aiosqlite
import pytest

pytestmark = pytest.mark.asyncio


async def test_war_room_status_returns_all_required_fields(client):
    body = {
        "kind": "demo_research",
        "channel": "#cc-demo_research",
        "thread_id": "thread-xyz",
        "status": "running",
        "current_task": "probing /api/state.agents",
        "last_activity_ts": 1746400000000,
    }
    r = await client.post("/api/agent/wr-fields-1/status", json=body)
    assert r.status_code == 200, r.text

    r = await client.get("/api/agent/wr-fields-1/status")
    assert r.status_code == 200, r.text
    d = r.json()

    expected_fields = {
        "id", "kind", "status", "queue_depth",
        "last_activity_ts", "current_task", "current_thread",
        # v1.3.1 (D29): additive — display_name field on every GET response
        # (None when no display name set, string when set)
        "display_name",
    }
    assert set(d.keys()) == expected_fields, (
        f"field-set mismatch: missing={expected_fields - set(d.keys())} "
        f"extra={set(d.keys()) - expected_fields}"
    )
    assert d["id"] == "wr-fields-1"
    assert d["kind"] == "demo_research"
    assert d["status"] == "running"
    assert d["current_task"] == "probing /api/state.agents"
    assert d["current_thread"] == "thread-xyz"
    assert d["last_activity_ts"] == 1746400000000
    assert d["queue_depth"] == 0
    # v1.3.1 (D29): no display_name supplied -> stored / returned as None
    assert d["display_name"] is None


async def test_war_room_status_queue_depth_counts_handoffs_after_last_activity(client):
    T0 = 1_700_000_000_000
    agent_id = "wr-qd-1"
    body = {
        "kind": "demo_coder", "channel": "#cc-demo_coder",
        "status": "idle", "last_activity_ts": T0,
    }
    r = await client.post(f"/api/agent/{agent_id}/status", json=body)
    assert r.status_code == 200, r.text

    async with aiosqlite.connect(os.environ["BROKER_DB_PATH"]) as conn:
        conn.row_factory = aiosqlite.Row
        await conn.executemany(
            "INSERT INTO message_log "
            "(from_agent, to_agent, channel, kind, content, ts) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [
                ("other", agent_id, "#cc-demo_coder", "handoff", "{}", T0 + 100),
                ("other", agent_id, "#cc-demo_coder", "handoff", "{}", T0 - 50),
                ("other", agent_id, "#cc-demo_coder", "message_logged", "{}", T0 + 200),
            ],
        )
        await conn.commit()

    r = await client.get(f"/api/agent/{agent_id}/status")
    assert r.status_code == 200, r.text
    assert r.json()["queue_depth"] == 1, r.json()

    # Bump last_activity_ts to T0+150 — handoff at T0+100 is now <= last_activity_ts
    # and the strict `ts > last_activity_ts` predicate excludes it.
    r = await client.post(f"/api/agent/{agent_id}/status", json={**body, "last_activity_ts": T0 + 150})
    assert r.status_code == 200, r.text

    r = await client.get(f"/api/agent/{agent_id}/status")
    assert r.status_code == 200, r.text
    assert r.json()["queue_depth"] == 0, r.json()


async def test_war_room_status_queue_depth_follows_bare_ingress_folding(client):
    agent_id = "Wr-Mixed-Case"
    r = await client.post(f"/api/agent/{agent_id}/status", json={
        "kind": "demo_coder", "channel": "#cc-demo_coder",
        "status": "idle", "last_activity_ts": 1,
    })
    assert r.status_code == 200, r.text
    sent = await client.post("/api/event", json={
        "from_agent": "other", "to_agent": "WR-MIXED-CASE",
        "channel": "#cc-demo_coder", "kind": "handoff",
        "content": "{}", "ts": 2,
        "client_event_id": "wr-mixed-case-handoff",
    })
    assert sent.status_code == 200, sent.text

    status = await client.get(f"/api/agent/{agent_id}/status")

    assert status.status_code == 200, status.text
    assert status.json()["queue_depth"] == 1


async def test_war_room_status_returns_404_for_unknown_agent(client):
    # The endpoint raises HTTPException(404, detail="agent not registered") at
    # server.py L1032; the broker-wide _http_exc_handler (L927-942) wraps that
    # into the standard error envelope, surfacing the detail text as
    # error.message. Asserting on the wrapped shape (not raw FastAPI `detail`)
    # is what matches actual broker behavior — same intent as the spec.
    r = await client.get("/api/agent/nonexistent-agent-id-xyz/status")
    assert r.status_code == 404
    body = r.json()
    assert body["error"]["code"] == "http_404"
    assert body["error"]["message"] == "agent not registered"
