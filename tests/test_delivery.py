"""WS-B-broker-1: delivery shadow-table + POST /api/agent/{name}/ack + cutover backfill.

Covers:
- co-transacted delivery row on directed relay kinds (agent_message/agent_reply/handoff)
- NO delivery row for broadcast (to_agent NULL) or non-relay kinds (e.g. tool_call)
- idempotent-replay does not duplicate the delivery row
- POST /api/agent/{name}/ack marks outstanding->acked, is idempotent, per-recipient
- ack of an unknown name/event is a 200 no-op; empty/invalid body is 422
- ack never echoes the (token-shaped) agent_id (F9)
- one-time cutover backfill seeds historical directed rows as 'acked' (Option A),
  guarded run-once by PRAGMA user_version
"""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.asyncio


async def _delivery_rows(to_agent=None):
    import aiosqlite

    async with aiosqlite.connect(os.environ["BROKER_DB_PATH"]) as conn:
        conn.row_factory = aiosqlite.Row
        if to_agent is None:
            cur = await conn.execute("SELECT * FROM delivery ORDER BY event_id")
        else:
            cur = await conn.execute(
                "SELECT * FROM delivery WHERE to_agent = ? ORDER BY event_id", (to_agent,)
            )
        rows = await cur.fetchall()
        await cur.close()
        return [dict(r) for r in rows]


# ---------------------------------------------------------------- co-txn insert

async def test_directed_agent_message_creates_outstanding_delivery_row(client):
    body = {"from_agent": "a", "to_agent": "bob", "channel": "#x",
            "kind": "agent_message", "content": "hi", "ts": 1}
    r = await client.post("/api/event", json=body)
    assert r.status_code == 200
    eid = r.json()["event_id"]
    rows = await _delivery_rows("bob")
    assert len(rows) == 1
    assert rows[0]["event_id"] == eid
    assert rows[0]["status"] == "outstanding"
    assert rows[0]["attempts"] == 0
    assert rows[0]["acked_ts"] is None
    assert rows[0]["next_push_ts"] is None
    assert rows[0]["created_ts"] > 0 and rows[0]["updated_ts"] > 0


async def test_agent_reply_and_handoff_create_delivery_rows(client):
    for kind in ("agent_reply", "handoff"):
        r = await client.post("/api/event", json={
            "from_agent": "a", "to_agent": "cy", "channel": "#x",
            "kind": kind, "content": "c", "ts": 1})
        assert r.status_code == 200
    rows = await _delivery_rows("cy")
    assert len(rows) == 2
    assert all(x["status"] == "outstanding" for x in rows)


async def test_broadcast_no_to_agent_creates_no_delivery_row(client):
    # agent_reply with to_agent omitted (broadcast) -> no delivery row.
    r = await client.post("/api/event", json={
        "from_agent": "a", "channel": "#x", "kind": "agent_reply",
        "content": "c", "ts": 1})
    assert r.status_code == 200
    assert await _delivery_rows() == []


async def test_non_relay_kind_creates_no_delivery_row(client):
    # tool_call is directed (to_agent set) but is NOT a DELIVERY_KIND -> no row.
    r = await client.post("/api/event", json={
        "from_agent": "a", "to_agent": "bob", "channel": "#x",
        "kind": "tool_call", "content": "c", "ts": 1})
    assert r.status_code == 200
    assert await _delivery_rows() == []


async def test_idempotent_replay_no_duplicate_delivery_row(client):
    body = {"from_agent": "a", "to_agent": "bob", "channel": "#x",
            "kind": "agent_message", "content": "hi", "ts": 1,
            "client_event_id": "cei-dup-1"}
    await client.post("/api/event", json=body)
    await client.post("/api/event", json=body)
    await client.post("/api/event", json=body)
    rows = await _delivery_rows("bob")
    assert len(rows) == 1


# ---------------------------------------------------------------- ack endpoint

async def test_ack_marks_outstanding_acked_and_is_idempotent(client):
    r = await client.post("/api/event", json={
        "from_agent": "a", "to_agent": "bob", "channel": "#x",
        "kind": "agent_message", "content": "hi", "ts": 1})
    eid = r.json()["event_id"]

    a1 = await client.post("/api/agent/bob/ack", json={"event_ids": [eid]})
    assert a1.status_code == 200
    assert a1.json() == {"acked_now": 1, "requested": 1}
    rows = await _delivery_rows("bob")
    assert rows[0]["status"] == "acked"
    assert rows[0]["acked_ts"] is not None

    # Second ack of the same id -> idempotent no-op.
    a2 = await client.post("/api/agent/bob/ack", json={"event_ids": [eid]})
    assert a2.status_code == 200
    assert a2.json() == {"acked_now": 0, "requested": 1}


async def test_ack_batch_mixed_known_and_unknown(client):
    ids = []
    for _ in range(3):
        r = await client.post("/api/event", json={
            "from_agent": "a", "to_agent": "bob", "channel": "#x",
            "kind": "agent_message", "content": "hi", "ts": 1})
        ids.append(r.json()["event_id"])
    # 3 real + 2 unknown ids in one call -> only the 3 real transition.
    a = await client.post("/api/agent/bob/ack", json={"event_ids": ids + [10**9, 10**9 + 1]})
    assert a.status_code == 200
    assert a.json() == {"acked_now": 3, "requested": 5}


async def test_ack_unknown_name_or_event_is_noop_200(client):
    a = await client.post("/api/agent/nobody/ack", json={"event_ids": [999999]})
    assert a.status_code == 200
    assert a.json() == {"acked_now": 0, "requested": 1}


async def test_ack_is_per_recipient(client):
    # message addressed to bob; alice acking the same event_id is a no-op.
    r = await client.post("/api/event", json={
        "from_agent": "a", "to_agent": "bob", "channel": "#x",
        "kind": "agent_message", "content": "hi", "ts": 1})
    eid = r.json()["event_id"]
    a = await client.post("/api/agent/alice/ack", json={"event_ids": [eid]})
    assert a.json() == {"acked_now": 0, "requested": 1}
    assert (await _delivery_rows("bob"))[0]["status"] == "outstanding"


async def test_ack_empty_list_rejected_422(client):
    a = await client.post("/api/agent/bob/ack", json={"event_ids": []})
    assert a.status_code == 422


async def test_ack_non_int_rejected_422(client):
    a = await client.post("/api/agent/bob/ack", json={"event_ids": ["notanint"]})
    assert a.status_code == 422


async def test_ack_does_not_echo_agent_id(client):
    # F9: a token-shaped path param must not cross the response boundary.
    token = "sk-secrettoken-DEADBEEF-END"
    a = await client.post(f"/api/agent/{token}/ack", json={"event_ids": [1]})
    assert a.status_code == 200
    assert token not in a.text


# ------------------------------------------------------- ack floor mirror (#18/#32)

async def test_ack_floor_crosses_off_le_floor_only(client):
    # floor crosses off OUTSTANDING rows with event_id <= floor; rows above stay.
    ids = []
    for _ in range(3):
        r = await client.post("/api/event", json={
            "from_agent": "a", "to_agent": "bob", "channel": "#x",
            "kind": "agent_message", "content": "hi", "ts": 1})
        ids.append(r.json()["event_id"])
    lo, mid, hi = ids  # ascending (sequential event_id)
    a = await client.post("/api/agent/bob/ack", json={"floor": mid})
    assert a.status_code == 200
    assert a.json()["acked_now"] == 2   # lo + mid
    rows = {r["event_id"]: r["status"] for r in await _delivery_rows("bob")}
    assert rows[lo] == "acked"
    assert rows[mid] == "acked"
    assert rows[hi] == "outstanding"    # above floor -> untouched


async def test_ack_floor_preserves_unread_gap_below(client):
    # An unread-gap scenario at the broker layer: an id BETWEEN the floor and an
    # explicitly-acked HIGHER id (the "unread 101" while "102" is read+acked) MUST
    # survive. The client's gap-aware floor stays at the last-handled id BELOW the
    # unread gap, so it posts {floor: lo, event_ids:[hi]} -> lo + hi acked, mid stays.
    ids = []
    for _ in range(3):
        r = await client.post("/api/event", json={
            "from_agent": "a", "to_agent": "bob", "channel": "#x",
            "kind": "agent_message", "content": "m", "ts": 1})
        ids.append(r.json()["event_id"])
    lo, mid, hi = ids  # lo=last-handled (<=floor), mid=UNREAD gap, hi=read+acked
    a = await client.post("/api/agent/bob/ack",
                          json={"floor": lo, "event_ids": [hi]})
    assert a.status_code == 200
    assert a.json()["acked_now"] == 2   # lo (<=floor) + hi (explicit)
    rows = {r["event_id"]: r["status"] for r in await _delivery_rows("bob")}
    assert rows[lo] == "acked"
    assert rows[mid] == "outstanding"   # the UNREAD gap id is NOT crossed off
    assert rows[hi] == "acked"


async def test_ack_floor_only_empty_event_ids(client):
    r = await client.post("/api/event", json={
        "from_agent": "a", "to_agent": "bob", "channel": "#x",
        "kind": "agent_message", "content": "hi", "ts": 1})
    eid = r.json()["event_id"]
    a = await client.post("/api/agent/bob/ack", json={"event_ids": [], "floor": eid})
    assert a.status_code == 200
    assert a.json() == {"acked_now": 1, "requested": 0}
    assert (await _delivery_rows("bob"))[0]["status"] == "acked"


async def test_ack_floor_missing_event_ids_key_ok(client):
    # event_ids is now optional: a floor-only body without the key is valid.
    r = await client.post("/api/event", json={
        "from_agent": "a", "to_agent": "bob", "channel": "#x",
        "kind": "agent_message", "content": "hi", "ts": 1})
    eid = r.json()["event_id"]
    a = await client.post("/api/agent/bob/ack", json={"floor": eid})
    assert a.status_code == 200
    assert a.json()["acked_now"] == 1


async def test_ack_empty_both_rejected_422(client):
    # neither event_ids nor floor -> 422 (empty-both is a caller bug, not a no-op).
    a = await client.post("/api/agent/bob/ack", json={})
    assert a.status_code == 422
    b = await client.post("/api/agent/bob/ack", json={"event_ids": []})
    assert b.status_code == 422


async def test_ack_floor_and_event_ids_no_double_count(client):
    # an id BOTH in event_ids AND <= floor is counted once (disjoint UPDATEs).
    ids = []
    for _ in range(2):
        r = await client.post("/api/event", json={
            "from_agent": "a", "to_agent": "bob", "channel": "#x",
            "kind": "agent_message", "content": "hi", "ts": 1})
        ids.append(r.json()["event_id"])
    lo, hi = ids
    a = await client.post("/api/agent/bob/ack",
                          json={"event_ids": [lo, hi], "floor": hi})
    assert a.status_code == 200
    assert a.json()["acked_now"] == 2   # lo + hi, each once (not 3)


async def test_ack_floor_out_of_range_422(client):
    over = await client.post("/api/agent/bob/ack",
                             json={"floor": 9223372036854775808})
    assert over.status_code == 422
    zero = await client.post("/api/agent/bob/ack", json={"floor": 0})
    assert zero.status_code == 422
    neg = await client.post("/api/agent/bob/ack", json={"floor": -5})
    assert neg.status_code == 422


async def test_ack_floor_is_per_recipient(client):
    # floor only crosses off THIS recipient's rows.
    r = await client.post("/api/event", json={
        "from_agent": "a", "to_agent": "bob", "channel": "#x",
        "kind": "agent_message", "content": "hi", "ts": 1})
    eid = r.json()["event_id"]
    a = await client.post("/api/agent/alice/ack", json={"floor": eid + 1000})
    assert a.json()["acked_now"] == 0
    assert (await _delivery_rows("bob"))[0]["status"] == "outstanding"


# ---------------------------------------------------------------- cutover backfill

async def test_cutover_backfill_seeds_historical_as_acked_run_once(tmp_path, monkeypatch):
    import sqlite3
    import pathlib

    db = tmp_path / "coordination.db"
    monkeypatch.setenv("BROKER_DB_PATH", str(db))
    monkeypatch.setenv("BROKER_HOST", "127.0.0.1")

    from broker import server
    schema = pathlib.Path(server.SCHEMA_PATH).read_text(encoding="utf-8")

    # Seed a pre-cutover message_log BEFORE the backfill runs; leave user_version=0.
    conn = sqlite3.connect(str(db))
    conn.executescript(schema)
    now = 1000
    conn.executemany(
        "INSERT INTO message_log (from_agent, to_agent, channel, kind, content, ts) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        [
            ("a", "bob", "#x", "agent_message", "m1", now),      # directed relay -> seeded
            ("a", "bob", "#x", "agent_reply", "m2", now),        # directed relay -> seeded
            ("a", "carol", "#x", "handoff", "m3", now),          # directed relay -> seeded
            ("a", None, "#x", "agent_reply", "broadcast", now),  # no to_agent -> excluded
            ("a", "bob", "#x", "tool_call", "nonrelay", now),    # non-relay kind -> excluded
        ],
    )
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 0
    conn.commit()
    conn.close()

    await server.ensure_schema()  # 0->1 backfill, 1->2 reaper, 2->5 R1 migrations

    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT to_agent, status FROM delivery ORDER BY event_id").fetchall()
    assert len(rows) == 3                                    # only the 3 directed relay rows
    assert all(r["status"] == "acked" for r in rows)         # Option A: historical = acked
    # R1 advances the prior delivery/reaper schema from user_version 2 to 5 in
    # the same atomic ensure_schema pass.
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 5
    conn.execute(
        "INSERT INTO message_log (from_agent, to_agent, channel, kind, content, ts) "
        "VALUES ('a', 'dave', '#x', 'agent_message', 'late', 1)"
    )
    conn.commit()
    conn.close()

    await server.ensure_schema()  # run-once guard must hold: no re-backfill

    conn = sqlite3.connect(str(db))
    n = conn.execute("SELECT COUNT(*) FROM delivery").fetchone()[0]
    conn.close()
    assert n == 3  # 'dave' was NOT backfilled (guard held)


async def test_fresh_db_backfill_is_empty_and_sets_user_version(client):
    # The `client` fixture ran ensure_schema on an empty message_log: 0 seeded rows,
    # then advanced all run-once migrations through the OMP R1 schema version.
    import aiosqlite
    async with aiosqlite.connect(os.environ["BROKER_DB_PATH"]) as conn:
        cur = await conn.execute("PRAGMA user_version")
        uv = (await cur.fetchone())[0]
        await cur.close()
    assert uv == 5
    assert await _delivery_rows() == []


# ---------------------------------------------------------------- discriminator fixes

async def test_retention_deletes_matching_delivery_rows_no_orphan(client, tmp_path):
    """HIGH fix: the agent_message retention sweep must delete each swept message's
    delivery row in the same txn, so no orphaned delivery row outlives its parent.
    agent_reply/handoff delivery rows (not retention-swept) must survive."""
    import aiosqlite
    from broker import server

    db = os.environ["BROKER_DB_PATH"]
    old = server.now_ms() - 40 * 86400 * 1000   # 40 days old -> swept at 30d retention
    new = server.now_ms()
    async with aiosqlite.connect(db) as conn:
        # event 1: OLD agent_message + its (unacked) delivery row -> both swept
        await conn.execute(
            "INSERT INTO message_log (event_id, from_agent, to_agent, channel, kind, content, ts) "
            "VALUES (1,'a','bob','#x','agent_message','old',?)", (old,))
        await conn.execute(
            "INSERT INTO delivery (to_agent, event_id, status, attempts, created_ts, updated_ts) "
            "VALUES ('bob',1,'outstanding',0,?,?)", (old, old))
        # event 2: RECENT agent_message + delivery -> both survive
        await conn.execute(
            "INSERT INTO message_log (event_id, from_agent, to_agent, channel, kind, content, ts) "
            "VALUES (2,'a','bob','#x','agent_message','new',?)", (new,))
        await conn.execute(
            "INSERT INTO delivery (to_agent, event_id, status, attempts, created_ts, updated_ts) "
            "VALUES ('bob',2,'outstanding',0,?,?)", (new, new))
        # event 3: OLD agent_reply + delivery -> NOT retention-swept (only agent_message is)
        await conn.execute(
            "INSERT INTO message_log (event_id, from_agent, to_agent, channel, kind, content, ts) "
            "VALUES (3,'a','bob','#x','agent_reply','oldreply',?)", (old,))
        await conn.execute(
            "INSERT INTO delivery (to_agent, event_id, status, attempts, created_ts, updated_ts) "
            "VALUES ('bob',3,'outstanding',0,?,?)", (old, old))
        await conn.commit()

    n = await server._agent_message_retention_sweep_once(30, tmp_path / "archive")
    assert n == 1  # only the old agent_message (event 1) archived + deleted

    async with aiosqlite.connect(db) as conn:
        conn.row_factory = aiosqlite.Row
        ml = [r["event_id"] for r in await (await conn.execute(
            "SELECT event_id FROM message_log ORDER BY event_id")).fetchall()]
        dv = [r["event_id"] for r in await (await conn.execute(
            "SELECT event_id FROM delivery ORDER BY event_id")).fetchall()]
    assert ml == [2, 3]   # event 1 message_log deleted
    assert dv == [2, 3]   # event 1 delivery deleted too -> NO orphan; event 3 (agent_reply) kept


async def test_ack_out_of_range_event_id_rejected_422(client):
    """MEDIUM fix: an out-of-i64-range (or <1) event_id must 422, not 500."""
    over = await client.post("/api/agent/bob/ack",
                             json={"event_ids": [9223372036854775808]})  # SQLITE_I64_MAX + 1
    assert over.status_code == 422
    zero = await client.post("/api/agent/bob/ack", json={"event_ids": [0]})
    assert zero.status_code == 422
    neg = await client.post("/api/agent/bob/ack", json={"event_ids": [-5]})
    assert neg.status_code == 422
    # a valid id in the same shape still works
    mixed = await client.post("/api/agent/bob/ack", json={"event_ids": [1, 9223372036854775808]})
    assert mixed.status_code == 422  # whole batch rejected if any id is out of range


async def test_retention_delivery_delete_uses_index_not_full_scan(client):
    """MEDIUM (iter-2) fix: the retention `DELETE FROM delivery WHERE event_id IN (...)`
    must use idx_delivery_event_id (indexed SEARCH), not a full SCAN of delivery held
    under the retention write lock (delivery grows unbounded: reply/handoff never pruned)."""
    import aiosqlite
    async with aiosqlite.connect(os.environ["BROKER_DB_PATH"]) as conn:
        idx = [r[0] for r in await (await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='delivery'"
        )).fetchall()]
        assert "idx_delivery_event_id" in idx
        cur = await conn.execute(
            "EXPLAIN QUERY PLAN DELETE FROM delivery WHERE event_id IN "
            "(SELECT event_id FROM message_log WHERE kind='agent_message' AND ts < ?)",
            (0,),
        )
        plan = " | ".join(str(row[-1]) for row in await cur.fetchall())
        await cur.close()
    # The exact regression codex flagged: the outer delete must not full-scan delivery.
    assert "SCAN delivery" not in plan, plan
