"""Iter-2 discriminator fix tests:
- F1: Idempotency-Key header support
- F3: idempotency race condition (concurrent dupes)
- F2: SSE event kind taxonomy (agent_status_change / message_logged / pipeline_event)
- F5: GET /api/state cursor pagination
- F7: Windows path normalization in file watcher
- F8: re-register project returns persisted project_id (not phantom UUID)
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import aiosqlite
import pytest

pytestmark = pytest.mark.asyncio


# ===== F1: Idempotency-Key header support =====

async def test_idempotency_key_header_only(client):
    """Subagents may send Idempotency-Key header instead of body field."""
    body = {
        "from_agent": "a", "channel": "#x", "kind": "user_msg",
        "content": "hi", "ts": 1746400000000,
    }
    r1 = await client.post("/api/event", json=body,
                           headers={"Idempotency-Key": "header-uuid-1"})
    r2 = await client.post("/api/event", json=body,
                           headers={"Idempotency-Key": "header-uuid-1"})
    assert r1.status_code == r2.status_code == 200
    assert r1.json()["event_id"] == r2.json()["event_id"]


async def test_idempotency_key_header_overrides_body_when_equal(client):
    """When both body field and header agree, both work."""
    body = {
        "from_agent": "a", "channel": "#x", "kind": "user_msg",
        "content": "hi", "ts": 1, "client_event_id": "agree-uuid",
    }
    r1 = await client.post("/api/event", json=body,
                           headers={"Idempotency-Key": "agree-uuid"})
    r2 = await client.post("/api/event", json=body)
    assert r1.json()["event_id"] == r2.json()["event_id"]


async def test_idempotency_key_header_disagrees_with_body_400(client):
    """When body and header conflict → 400."""
    body = {
        "from_agent": "a", "channel": "#x", "kind": "user_msg",
        "content": "hi", "ts": 1, "client_event_id": "body-uuid",
    }
    r = await client.post("/api/event", json=body,
                          headers={"Idempotency-Key": "different-uuid"})
    assert r.status_code == 400


async def test_idempotency_key_header_on_agent_status(client):
    body = {
        "kind": "demo_research", "channel": "#cc-demo_research",
        "status": "idle", "last_activity_ts": 1746400000000,
    }
    r1 = await client.post("/api/agent/h-agent/status", json=body,
                           headers={"Idempotency-Key": "agent-header-uuid"})
    r2 = await client.post("/api/agent/h-agent/status", json=body,
                           headers={"Idempotency-Key": "agent-header-uuid"})
    assert r1.json()["event_id"] == r2.json()["event_id"]


# ===== F3: idempotency race (concurrent dupes) =====

async def test_idempotency_concurrent_duplicates_no_500(client):
    """Concurrent retries with same client_event_id should NOT return 500.

    Either both return 200 with same event_id (idempotency held), OR worst case
    one returns 200 and the other returns 200 with same id after IntegrityError
    rollback + re-read. Never 5xx.
    """
    body = {
        "from_agent": "race", "channel": "#x", "kind": "user_msg",
        "content": "hi", "ts": 1, "client_event_id": "race-uuid-1",
    }
    # Fire 5 concurrent identical POSTs
    results = await asyncio.gather(
        *[client.post("/api/event", json=body) for _ in range(5)],
        return_exceptions=True,
    )
    for r in results:
        assert not isinstance(r, BaseException), f"unexpected exception: {r}"
        assert r.status_code == 200, r.text
    event_ids = {r.json()["event_id"] for r in results}
    assert len(event_ids) == 1, f"expected one event_id across all races, got {event_ids}"

    # Verify only one row in message_log
    async with aiosqlite.connect(os.environ["BROKER_DB_PATH"]) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(
            "SELECT COUNT(*) AS n FROM message_log WHERE client_event_id = ?",
            ("race-uuid-1",),
        )
        row = await cur.fetchone()
    assert row["n"] == 1


# ===== F2: SSE event kind taxonomy =====

async def test_sse_kind_status_event_maps_to_agent_status_change(client):
    """Posting agent status creates a message_log row with kind='status'.
    SSE replay should expose it as kind='agent_status_change'."""
    from broker import server
    await client.post("/api/agent/k1/status", json={
        "kind": "demo_research", "channel": "#cc-demo_research",
        "status": "idle", "last_activity_ts": 1,
    })

    class _MockReq:
        async def is_disconnected(self): return False

    gen = server.sse_event_generator(_MockReq(), 0)
    msgs = []
    try:
        async with asyncio.timeout(2.0):
            async for evt in gen:
                if evt.get("event") == "heartbeat":
                    continue
                msgs.append(evt)
                break
    except asyncio.TimeoutError:
        pass
    finally:
        await gen.aclose()

    assert len(msgs) == 1
    payload = json.loads(msgs[0]["data"])
    assert payload["kind"] == "agent_status_change"


async def test_sse_kind_user_msg_maps_to_message_logged(client):
    from broker import server
    await client.post("/api/event", json={
        "from_agent": "a", "channel": "#x", "kind": "user_msg",
        "content": "hi", "ts": 1,
    })

    class _MockReq:
        async def is_disconnected(self): return False

    gen = server.sse_event_generator(_MockReq(), 0)
    msgs = []
    try:
        async with asyncio.timeout(2.0):
            async for evt in gen:
                if evt.get("event") == "heartbeat":
                    continue
                msgs.append(evt)
                break
    except asyncio.TimeoutError:
        pass
    finally:
        await gen.aclose()

    assert len(msgs) == 1
    payload = json.loads(msgs[0]["data"])
    assert payload["kind"] == "message_logged"


async def test_sse_kind_pipeline_event_passes_through(client):
    from broker import server
    await client.post("/api/event", json={
        "from_agent": "system:broker", "channel": "#x", "kind": "pipeline_event",
        "content": json.dumps({"pipeline_id": "p1", "step_index": 0, "transition": "started"}),
        "ts": 1,
    })

    class _MockReq:
        async def is_disconnected(self): return False

    gen = server.sse_event_generator(_MockReq(), 0)
    msgs = []
    try:
        async with asyncio.timeout(2.0):
            async for evt in gen:
                if evt.get("event") == "heartbeat":
                    continue
                msgs.append(evt)
                break
    except asyncio.TimeoutError:
        pass
    finally:
        await gen.aclose()

    payload = json.loads(msgs[0]["data"])
    assert payload["kind"] == "pipeline_event"


async def test_sse_replay_data_shape_matches_live(client):
    """F2 + replay-vs-live data shape known issue: replay should rebuild data
    from columns to mirror live broadcast shape (no longer raw content string)."""
    from broker import server
    await client.post("/api/event", json={
        "from_agent": "ag1", "to_agent": "user:1", "channel": "#cc-demo_research",
        "kind": "agent_reply", "content": "hello", "ts": 1, "thread_id": "t1",
    })

    class _MockReq:
        async def is_disconnected(self): return False

    gen = server.sse_event_generator(_MockReq(), 0)
    msgs = []
    try:
        async with asyncio.timeout(2.0):
            async for evt in gen:
                if evt.get("event") == "heartbeat":
                    continue
                msgs.append(evt)
                break
    except asyncio.TimeoutError:
        pass
    finally:
        await gen.aclose()

    payload = json.loads(msgs[0]["data"])
    # data should be the structured dict, not the bare content string
    assert isinstance(payload["data"], dict)
    assert payload["data"]["content"] == "hello"
    assert payload["data"]["to_agent"] == "user:1"
    assert payload["data"]["channel"] == "#cc-demo_research"
    assert payload["data"]["thread_id"] == "t1"


# ===== F5: GET /api/state cursor pagination =====
# (Iter-3 redesign: opaque composite cursor "<registered_at>:<id>"; broker-time registered_at.)

async def test_state_cursor_pagination(client):
    """Register 5 agents; paginate with limit=2.

    Cursor is opaque (composite registered_at + id), encoded by broker.
    """
    for i in range(5):
        await client.post(f"/api/agent/p{i}/status", json={
            "kind": "demo_coder", "channel": "#cc-demo_coder",
            "status": "idle", "last_activity_ts": 1,
        })
        # Tiny stagger so each registration gets a distinct broker-time registered_at
        await asyncio.sleep(0.005)

    r1 = await client.get("/api/state", params={"limit": 2})
    d1 = r1.json()
    assert len(d1["agents"]) == 2
    assert d1["next_cursor"] is not None
    # Cursor is composite "<ts>:<id>"
    assert ":" in d1["next_cursor"]

    r2 = await client.get("/api/state", params={"limit": 2, "cursor": d1["next_cursor"]})
    d2 = r2.json()
    assert len(d2["agents"]) == 2
    assert {a["id"] for a in d1["agents"]} & {a["id"] for a in d2["agents"]} == set()

    r3 = await client.get("/api/state", params={"limit": 2, "cursor": d2["next_cursor"]})
    d3 = r3.json()
    assert len(d3["agents"]) == 1
    assert d3["next_cursor"] is None


async def test_state_cursor_same_millisecond_no_skip(client, monkeypatch):
    """Iter-3 fix verified under iter-4 monotonic enforcement: even when now_ms()
    returns the same value across all 5 inserts, the broker enforces strictly
    increasing registered_at via max(now, MAX+1). Pagination walks all 5 rows
    exactly once.
    """
    from broker import server
    fixed_ts = 1746000000000
    monkeypatch.setattr(server, "now_ms", lambda: fixed_ts)

    # Register 5 agents — broker enforces monotonic registered_at, NOT all == fixed_ts
    for i in range(5):
        await client.post(f"/api/agent/sm-{i}/status", json={
            "kind": "demo_coder", "channel": "#cc-demo_coder",
            "status": "idle", "last_activity_ts": 1,
        })

    # Verify monotonicity: 5 distinct registered_at values, all >= fixed_ts
    async with aiosqlite.connect(os.environ["BROKER_DB_PATH"]) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(
            "SELECT id, registered_at FROM agent_status WHERE id LIKE 'sm-%' ORDER BY registered_at"
        )
        rows = await cur.fetchall()
    timestamps = [r["registered_at"] for r in rows]
    assert len(set(timestamps)) == 5  # distinct (iter-4 monotonic enforcement)
    assert all(ts >= fixed_ts for ts in timestamps)

    # Paginate with limit=2; expect 3 pages (2 + 2 + 1) with no duplicates
    seen = []
    cursor = None
    for _ in range(5):  # safety cap
        params = {"limit": 2}
        if cursor:
            params["cursor"] = cursor
        r = await client.get("/api/state", params=params)
        d = r.json()
        seen.extend(a["id"] for a in d["agents"] if a["id"].startswith("sm-"))
        if d["next_cursor"] is None:
            break
        cursor = d["next_cursor"]

    sm_seen = [s for s in seen if s.startswith("sm-")]
    assert sorted(sm_seen) == [f"sm-{i}" for i in range(5)]
    assert len(sm_seen) == len(set(sm_seen))


async def test_state_registered_at_immune_to_client_clock(client):
    """N2-fix: registered_at is broker-time, not client-supplied.

    A client sending last_activity_ts=2020 (way in the past) should still get
    a current broker-time registered_at, so pagination cursor walks forward.
    """
    await client.post("/api/agent/clock-back/status", json={
        "kind": "demo_research", "channel": "#x", "status": "idle",
        "last_activity_ts": 1577836800000,  # 2020-01-01 — way in past
    })
    async with aiosqlite.connect(os.environ["BROKER_DB_PATH"]) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(
            "SELECT registered_at, last_activity_ts FROM agent_status WHERE id='clock-back'"
        )
        row = await cur.fetchone()
    # broker-time registered_at must be much greater than client's 2020 last_activity_ts
    assert row["registered_at"] > row["last_activity_ts"]
    assert row["registered_at"] > 1700000000000  # > 2023


async def test_state_cursor_pagination_with_lex_unfriendly_ids(client):
    """N1 fix verification: composite cursor handles non-lex-sortable ids.

    With ids worker-1..worker-11, lexicographic sort would put worker-10 between
    worker-1 and worker-2. Composite cursor (registered_at, id) tracks broker-time
    insertion order, immune to id lex-sort.
    """
    for i in range(1, 12):
        await client.post(f"/api/agent/worker-{i}/status", json={
            "kind": "demo_coder", "channel": "#cc-demo_coder",
            "status": "idle", "last_activity_ts": 1,
        })
        await asyncio.sleep(0.001)

    seen_ids = []
    cursor = None
    for _ in range(10):
        params = {"limit": 3}
        if cursor:
            params["cursor"] = cursor
        r = await client.get("/api/state", params=params)
        d = r.json()
        seen_ids.extend(a["id"] for a in d["agents"])
        if d["next_cursor"] is None:
            break
        cursor = d["next_cursor"]

    assert sorted(seen_ids) == sorted([f"worker-{i}" for i in range(1, 12)])
    assert len(seen_ids) == len(set(seen_ids))


async def test_state_no_pagination_when_under_limit(client):
    await client.post("/api/agent/x/status", json={
        "kind": "demo_research", "channel": "#x", "status": "idle", "last_activity_ts": 1,
    })
    r = await client.get("/api/state", params={"limit": 100})
    assert r.json()["next_cursor"] is None


async def test_state_invalid_cursor_returns_422(client):
    """Malformed cursor should 422 (not crash, not silently ignore)."""
    r = await client.get("/api/state", params={"cursor": "garbage-no-colon"})
    assert r.status_code == 422
    r = await client.get("/api/state", params={"cursor": "notanint:agent-x"})
    assert r.status_code == 422


# ===== F7: Windows path normalization =====

async def test_path_norm_handles_forward_slash(tmp_path, monkeypatch):
    """norm_path() should produce identical output for backslash, forward slash, mixed."""
    from broker import server
    f = tmp_path / "state.db"
    f.write_text("")  # need it to exist for resolve()

    p1 = str(f)  # OS native (Windows: backslash)
    p2 = str(f).replace("\\", "/")  # forward slash
    # Mixed slashes
    p3 = str(f).replace("\\", "/").replace("/", os.sep, 1)

    n1 = server.norm_path(p1)
    n2 = server.norm_path(p2)
    n3 = server.norm_path(p3)
    assert n1 == n2 == n3


async def test_path_norm_handles_case_differences(tmp_path):
    """On Windows: norm_path should be case-insensitive."""
    from broker import server
    f = tmp_path / "state.db"
    f.write_text("")
    upper = str(f).upper()
    lower = str(f).lower()
    if os.name == "nt":
        assert server.norm_path(upper) == server.norm_path(lower)


# ===== F8: re-register returns persisted project_id =====

async def test_reregister_returns_actual_persisted_project_id(client, project_state_db):
    r1 = await client.post("/api/project/register", json=project_state_db)
    pid1 = r1.json()["project_id"]

    # Re-register same name with different root_path
    body2 = {**project_state_db, "root_path": project_state_db["root_path"] + "_v2"}
    r2 = await client.post("/api/project/register", json=body2)
    pid2 = r2.json()["project_id"]

    # On re-register, project_id MUST stay the SAME (per F8 fix; row preserved by ON CONFLICT)
    assert pid1 == pid2

    # Verify in DB
    async with aiosqlite.connect(os.environ["BROKER_DB_PATH"]) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(
            "SELECT project_id FROM project_registry WHERE name = ?",
            (project_state_db["name"],),
        )
        row = await cur.fetchone()
    assert row["project_id"] == pid1


async def test_state_response_has_next_cursor_field(client):
    """Backwards compatibility: GET /api/state always includes next_cursor field."""
    r = await client.get("/api/state")
    assert "next_cursor" in r.json()
