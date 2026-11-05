"""Unit tests for the dashboard data endpoints (agents/pipelines/messages).

All tests use the synchronous TestClient + seeded temp DB from conftest — no
live broker. The dashboard reads the temp DB in mode=ro.
"""

from __future__ import annotations

import time

from broker.dashboard.api import _MESSAGE_KINDS


def _now_ms():
    return int(time.time() * 1000)


# ---------------------------------------------------------------- agents

def test_agents_empty_db(client):
    r = client.get("/api/dashboard/agents")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["agents"] == []
    assert body["counts"] == {"live": 0, "recent": 0, "idle": 0, "stale": 0}
    assert isinstance(body["now"], int)


def test_agents_liveness_boundary_buckets(client, seed_db):
    now = _now_ms()
    rows = [
        ("a-live", now - 60_000),                 # 1m  -> live
        ("a-recent", now - 30 * 60_000),          # 30m -> recent
        ("a-idle", now - 3 * 60 * 60_000),        # 3h  -> idle
        ("a-stale", now - 30 * 60 * 60_000),      # 30h -> stale
    ]
    for sid, ts in rows:
        seed_db.exec(
            "INSERT INTO agent_status (id, kind, channel, status, last_activity_ts, registered_at) "
            "VALUES (?, 'sub-coder', '#c', 'idle', ?, ?)",
            (sid, ts, ts),
        )
    body = client.get("/api/dashboard/agents").json()
    assert body["counts"] == {"live": 1, "recent": 1, "idle": 1, "stale": 1}
    # Sorted effective_ts DESC -> live first, stale last.
    ids = [a["id"] for a in body["agents"]]
    assert ids == ["a-live", "a-recent", "a-idle", "a-stale"]


def test_agents_within_ms_filters_array_not_counts(client, seed_db):
    now = _now_ms()
    seed_db.exec(
        "INSERT INTO agent_status (id, kind, channel, status, last_activity_ts, registered_at) "
        "VALUES ('fresh', 'k', '#c', 'idle', ?, ?)", (now - 60_000, now - 60_000))
    seed_db.exec(
        "INSERT INTO agent_status (id, kind, channel, status, last_activity_ts, registered_at) "
        "VALUES ('old', 'k', '#c', 'idle', ?, ?)", (now - 5 * 60 * 60_000, now - 5 * 60 * 60_000))
    body = client.get("/api/dashboard/agents?within_ms=3600000").json()
    # counts over FULL set (both); array filtered to within 1h (just 'fresh').
    assert body["counts"]["live"] + body["counts"]["idle"] == 2
    assert [a["id"] for a in body["agents"]] == ["fresh"]


def test_agents_registry_enrichment_and_null_name_guard(client, seed_db):
    now = _now_ms()
    # named in registry; display_name NULL -> name comes from registry.
    seed_db.exec(
        "INSERT INTO agent_status (id, kind, channel, status, last_activity_ts, registered_at) "
        "VALUES ('sid-named', 'k', '#c', 'idle', ?, ?)", (now - 1000, now - 1000))
    # registry name is null -> fall back to id[:8].
    seed_db.exec(
        "INSERT INTO agent_status (id, kind, channel, status, last_activity_ts, registered_at) "
        "VALUES ('abcdefgh-nulls', 'k', '#c', 'idle', ?, ?)", (now - 1000, now - 1000))
    seed_db.set_registry({
        "sid-named": {"name": "trading_infra", "last_seen_ts": now - 500},
        "abcdefgh-nulls": {"name": None, "last_seen_ts": now - 500},
    })
    body = client.get("/api/dashboard/agents").json()
    by_id = {a["id"]: a for a in body["agents"]}
    assert by_id["sid-named"]["name"] == "trading_infra"
    assert by_id["abcdefgh-nulls"]["name"] == "abcdefgh"   # id[:8]


def test_agents_display_name_wins_over_registry(client, seed_db):
    now = _now_ms()
    seed_db.exec(
        "INSERT INTO agent_status (id, kind, channel, status, last_activity_ts, registered_at, display_name) "
        "VALUES ('sid-x', 'k', '#c', 'idle', ?, ?, 'stored_name')", (now - 1000, now - 1000))
    seed_db.set_registry({"sid-x": {"name": "registry_name", "last_seen_ts": now - 500}})
    body = client.get("/api/dashboard/agents").json()
    assert body["agents"][0]["name"] == "stored_name"


def test_agents_effective_ts_is_max_of_activity_and_registry(client, seed_db):
    now = _now_ms()
    # last_activity_ts is stale (idle bucket); registry last_seen is fresh (live).
    seed_db.exec(
        "INSERT INTO agent_status (id, kind, channel, status, last_activity_ts, registered_at) "
        "VALUES ('sid-m', 'k', '#c', 'idle', ?, ?)",
        (now - 3 * 60 * 60_000, now - 3 * 60 * 60_000))
    seed_db.set_registry({"sid-m": {"name": "m", "last_seen_ts": now - 30_000}})
    body = client.get("/api/dashboard/agents").json()
    a = body["agents"][0]
    # effective_ts == max -> the fresh registry ts -> liveness live.
    assert a["liveness"] == "live"
    assert a["effective_ts"] == now - 30_000


def test_agents_contradiction_flag(client, seed_db):
    now = _now_ms()
    # stored status 'running' but last activity 30h ago -> stale -> ⚠.
    seed_db.exec(
        "INSERT INTO agent_status (id, kind, channel, status, last_activity_ts, registered_at) "
        "VALUES ('zombie', 'k', '#c', 'running', ?, ?)",
        (now - 30 * 60 * 60_000, now - 30 * 60 * 60_000))
    # stored status 'idle' and stale -> no flag (idle is not an active status).
    seed_db.exec(
        "INSERT INTO agent_status (id, kind, channel, status, last_activity_ts, registered_at) "
        "VALUES ('sleeper', 'k', '#c', 'idle', ?, ?)",
        (now - 30 * 60 * 60_000, now - 30 * 60 * 60_000))
    body = client.get("/api/dashboard/agents").json()
    by_id = {a["id"]: a for a in body["agents"]}
    assert by_id["zombie"]["flag"] == "⚠"
    assert by_id["sleeper"]["flag"] is None


def test_agents_uncapped_250(client, seed_db):
    now = _now_ms()
    for i in range(250):
        seed_db.exec(
            "INSERT INTO agent_status (id, kind, channel, status, last_activity_ts, registered_at) "
            "VALUES (?, 'k', '#c', 'idle', ?, ?)",
            (f"agent-{i:04d}", now - 1000, now - 1000))
    body = client.get("/api/dashboard/agents").json()
    assert len(body["agents"]) == 250
    assert body["counts"]["live"] == 250


def test_agents_registry_missing_degrades(client, seed_db):
    # Default fixture points registry at a non-existent file -> no enrichment, no 500.
    now = _now_ms()
    seed_db.exec(
        "INSERT INTO agent_status (id, kind, channel, status, last_activity_ts, registered_at) "
        "VALUES ('lonely', 'k', '#c', 'idle', ?, ?)", (now - 1000, now - 1000))
    r = client.get("/api/dashboard/agents")
    assert r.status_code == 200
    assert r.json()["agents"][0]["name"] == "lonely"[:8]


def test_agents_registry_malformed_degrades(client, seed_db):
    now = _now_ms()
    seed_db.exec(
        "INSERT INTO agent_status (id, kind, channel, status, last_activity_ts, registered_at) "
        "VALUES ('lonely2', 'k', '#c', 'idle', ?, ?)", (now - 1000, now - 1000))
    seed_db.break_registry()
    r = client.get("/api/dashboard/agents")
    assert r.status_code == 200
    assert r.json()["agents"][0]["name"] == "lonely2"[:8]


def test_agents_sort_is_deterministic_on_ties(client, seed_db):
    # Every row shares ONE timestamp, so effective_ts ties for every pair and the
    # only thing separating them is the `id` tiebreak. Without it the order of
    # tied rows is SQLite's unspecified scan order leaking through Python's
    # stable sort, and two identical requests may disagree.
    now = _now_ms()
    ids = ["agent-c", "agent-a", "agent-d", "agent-b"]
    for sid in ids:
        seed_db.exec(
            "INSERT INTO agent_status (id, kind, channel, status, last_activity_ts, registered_at) "
            "VALUES (?, 'k', '#c', 'idle', ?, ?)", (sid, now - 1000, now - 1000))
    first = [a["id"] for a in client.get("/api/dashboard/agents").json()["agents"]]
    second = [a["id"] for a in client.get("/api/dashboard/agents").json()["agents"]]
    # The whole key tuple is reversed, so the id tiebreak sorts DESC too.
    assert first == sorted(ids, reverse=True)
    assert first == second


def test_agents_payload_unchanged_after_column_narrowing(client, seed_db):
    # `capabilities` and `registered_at` are no longer SELECTed. Every row is
    # read by NAME via aiosqlite.Row, so narrowing cannot shift a field onto the
    # wrong key — this pins that, and pins that neither dropped column leaks
    # into the payload even when both hold distinctive values.
    now = _now_ms()
    seed_db.exec(
        "INSERT INTO agent_status "
        "(id, kind, channel, thread_id, status, current_task, capabilities, "
        " last_activity_ts, registered_at, display_name) "
        "VALUES ('sid-full', 'sub-coder', '#chan', 'thr-1', 'running', 'do a thing', "
        "        'cap-a,cap-b', ?, ?, 'Full Row')",
        (now - 1000, now - 999_000))
    body = client.get("/api/dashboard/agents").json()
    a = body["agents"][0]
    assert set(a) == {
        "id", "name", "kind", "channel", "thread_id", "status", "current_task",
        "effective_ts", "last_activity_ts", "age_ms", "liveness", "flag",
    }
    assert a["id"] == "sid-full"
    assert a["name"] == "Full Row"
    assert a["kind"] == "sub-coder"
    assert a["channel"] == "#chan"
    assert a["thread_id"] == "thr-1"
    assert a["status"] == "running"
    assert a["current_task"] == "do a thing"
    assert a["last_activity_ts"] == now - 1000
    assert a["effective_ts"] == now - 1000
    assert a["liveness"] == "live"
    assert a["flag"] is None
    assert body["counts"]["live"] == 1


# ---------------------------------------------------------------- pipelines

def _seed_pipeline(seed_db, pid, status, started_at, **kw):
    seed_db.exec(
        "INSERT INTO pipelines_view "
        "(project_name, pipeline_id, shape, started_at, completed_at, status, "
        " step_count, steps_done, last_refresh_ts) "
        "VALUES ('demo_project', ?, ?, ?, ?, ?, ?, ?, ?)",
        (pid, kw.get("shape", "implement"), started_at, kw.get("completed_at"),
         status, kw.get("step_count", 3), kw.get("steps_done", 1),
         kw.get("last_refresh_ts", started_at)),
    )


def test_pipelines_in_flight_default_excludes_terminal(client, seed_db):
    now = _now_ms()
    _seed_pipeline(seed_db, "p-open", "open", now - 1000)
    _seed_pipeline(seed_db, "p-inprog", "in_progress", now - 2000)
    _seed_pipeline(seed_db, "p-blocked", "blocked", now - 3000)
    _seed_pipeline(seed_db, "p-done", "done", now - 4000)
    _seed_pipeline(seed_db, "p-failed", "failed", now - 5000)
    _seed_pipeline(seed_db, "p-cancelled", "cancelled", now - 6000)
    body = client.get("/api/dashboard/pipelines").json()
    ids = {p["pipeline_id"] for p in body["pipelines"]}
    assert ids == {"p-open", "p-inprog", "p-blocked"}
    assert body["in_flight_count"] == 3
    assert body["total"] == 6


def test_pipelines_status_all_returns_everything(client, seed_db):
    now = _now_ms()
    _seed_pipeline(seed_db, "p-open", "open", now - 1000)
    _seed_pipeline(seed_db, "p-done", "done", now - 2000)
    body = client.get("/api/dashboard/pipelines?status=all").json()
    assert len(body["pipelines"]) == 2
    assert body["in_flight_count"] == 1
    assert body["total"] == 2


def test_pipelines_invalid_status_422(client):
    r = client.get("/api/dashboard/pipelines?status=bogus")
    assert r.status_code == 422


def test_pipelines_mixed_unit_normalization(client, seed_db):
    now_s = int(time.time())
    now_ms = now_s * 1000
    # 10-digit epoch-seconds row (should up-convert).
    _seed_pipeline(seed_db, "p-seconds", "open", now_s - 100)
    # 13-digit epoch-ms row.
    _seed_pipeline(seed_db, "p-ms", "open", now_ms - 50_000)
    # Anomalous oversized ts (> 1e14, within SQLite i64 range): _norm_ts clamps
    # to None instead of crashing; the row sorts last (None last).
    _seed_pipeline(seed_db, "p-anom", "open", 9_000_000_000_000_000_000)
    body = client.get("/api/dashboard/pipelines").json()
    by_id = {p["pipeline_id"]: p for p in body["pipelines"]}
    # seconds row up-converted to ms (13-digit-ish magnitude).
    assert by_id["p-seconds"]["started_at"] == (now_s - 100) * 1000
    assert by_id["p-ms"]["started_at"] == now_ms - 50_000
    assert by_id["p-anom"]["started_at"] is None
    # Sort DESC with None last -> anomaly is last.
    assert body["pipelines"][-1]["pipeline_id"] == "p-anom"


def test_pipelines_pagination(client, seed_db):
    now = _now_ms()
    for i in range(5):
        _seed_pipeline(seed_db, f"p-{i}", "open", now - i * 1000)
    page1 = client.get("/api/dashboard/pipelines?limit=2&offset=0").json()
    assert page1["returned"] == 2
    assert page1["in_flight_count"] == 5
    page2 = client.get("/api/dashboard/pipelines?limit=2&offset=2").json()
    assert page2["returned"] == 2
    # No overlap between pages.
    p1_ids = {p["pipeline_id"] for p in page1["pipelines"]}
    p2_ids = {p["pipeline_id"] for p in page2["pipelines"]}
    assert p1_ids.isdisjoint(p2_ids)


# ---------------------------------------------------------------- messages

def _seed_message(seed_db, from_a, to_a, content, ts, kind="agent_message"):
    seed_db.exec(
        "INSERT INTO message_log (from_agent, to_agent, channel, kind, content, ts) "
        "VALUES (?, ?, '#c', ?, ?, ?)",
        (from_a, to_a, kind, content, ts))


def test_messages_newest_first(client, seed_db):
    now = _now_ms()
    _seed_message(seed_db, "a", "b", "first", now - 3000)
    _seed_message(seed_db, "a", "b", "second", now - 2000)
    _seed_message(seed_db, "a", "b", "third", now - 1000)
    body = client.get("/api/dashboard/messages").json()
    contents = [m["content"] for m in body["messages"]]
    assert contents == ["third", "second", "first"]   # DESC by event_id


def test_messages_kind_filter(client, seed_db):
    now = _now_ms()
    _seed_message(seed_db, "a", "b", "msg", now - 1000, kind="agent_message")
    _seed_message(seed_db, "a", "b", "stat", now - 1000, kind="status")
    body = client.get("/api/dashboard/messages?kind=agent_message").json()
    assert len(body["messages"]) == 1
    assert body["messages"][0]["content"] == "msg"


def test_messages_within_ms_floor(client, seed_db):
    now = _now_ms()
    _seed_message(seed_db, "a", "b", "recent", now - 60_000)         # within 1h
    _seed_message(seed_db, "a", "b", "ancient", now - 5 * 60 * 60_000)  # outside 1h
    body = client.get("/api/dashboard/messages?within_ms=3600000").json()
    assert [m["content"] for m in body["messages"]] == ["recent"]


def test_messages_before_cursor_and_next_before(client, seed_db):
    now = _now_ms()
    for i in range(5):
        _seed_message(seed_db, "a", "b", f"m{i}", now - (5 - i) * 1000)
    # event_ids 1..5; DESC -> m4,m3,m2,m1,m0. limit=2 -> m4,m3; next_before=event_id of m3.
    page1 = client.get("/api/dashboard/messages?limit=2").json()
    assert [m["content"] for m in page1["messages"]] == ["m4", "m3"]
    assert page1["next_before"] is not None
    cursor = page1["next_before"]
    page2 = client.get(f"/api/dashboard/messages?limit=2&before={cursor}").json()
    assert [m["content"] for m in page2["messages"]] == ["m2", "m1"]
    # last page: 1 row left -> fewer than limit -> next_before null.
    cursor2 = page2["next_before"]
    page3 = client.get(f"/api/dashboard/messages?limit=2&before={cursor2}").json()
    assert [m["content"] for m in page3["messages"]] == ["m0"]
    assert page3["next_before"] is None


def test_messages_empty(client):
    body = client.get("/api/dashboard/messages").json()
    assert body["messages"] == []
    assert body["next_before"] is None


def test_messages_latest_event_id_is_max(client, seed_db):
    # latest_event_id is the GLOBAL MAX(event_id) over message_log (the SSE
    # high-water mark) — independent of the kind/within_ms filter applied to the
    # returned rows. Seed a status row (filtered out of the default agent_message
    # view) with the highest event_id to prove it's a global max, not a filtered one.
    now = _now_ms()
    _seed_message(seed_db, "a", "b", "m-old", now - 2000, kind="agent_message")
    _seed_message(seed_db, "a", "b", "m-new", now - 1000, kind="agent_message")
    _seed_message(seed_db, "a", "b", "s-newest", now - 500, kind="status")  # highest event_id
    body = client.get("/api/dashboard/messages").json()
    # Default kind=agent_message returns only the 2 agent_message rows...
    assert [m["content"] for m in body["messages"]] == ["m-new", "m-old"]
    # ...but latest_event_id is the global max -> the status row's event_id (3).
    assert body["latest_event_id"] == 3


def test_messages_latest_event_id_none_on_empty(client):
    # MAX(event_id) over an empty message_log is NULL -> None.
    body = client.get("/api/dashboard/messages").json()
    assert body["messages"] == []
    assert body["latest_event_id"] is None


# The allowlist, retyped rather than imported. Looping over the constant itself
# would make a future narrowing invisible to CI — the point of this pin is that
# shrinking `_MESSAGE_KINDS` has to be a deliberate two-file edit. `pipeline_event`
# in particular is retired-but-historical and still carries real rows.
# These nine are exactly server.py VALID_EVENT_KINDS. `message_logged` is
# excluded on purpose: it is an SSE wire kind, never a stored one.
_EXPECTED_MESSAGE_KINDS = (
    "agent_message", "agent_reply", "status", "handoff", "error",
    "pipeline_event", "permission_reply", "user_msg", "tool_call",
)


def test_messages_kind_allowlist_membership_is_pinned():
    assert _MESSAGE_KINDS == _EXPECTED_MESSAGE_KINDS


def test_messages_every_allowed_kind_returns_its_rows(client, seed_db):
    now = _now_ms()
    for k in _EXPECTED_MESSAGE_KINDS:
        _seed_message(seed_db, "a", "b", f"body-{k}", now - 1000, kind=k)
    for k in _EXPECTED_MESSAGE_KINDS:
        r = client.get(f"/api/dashboard/messages?kind={k}")
        assert r.status_code == 200, f"{k}: {r.text}"
        assert [m["content"] for m in r.json()["messages"]] == [f"body-{k}"]


def test_messages_invalid_kind_422(client, seed_db):
    now = _now_ms()
    _seed_message(seed_db, "a", "b", "stat", now - 1000, kind="status")

    # The defect this closes: a typo used to return 200 + [], which reads as
    # "nothing happened" rather than "you asked for a kind that does not exist".
    r = client.get("/api/dashboard/messages?kind=agent_mesage")
    assert r.status_code == 422
    detail = r.json()["detail"]
    # Same body shape as the pipelines 422 (a plain string, not FastAPI's list),
    # and it names the legal set so the caller can fix the request.
    assert isinstance(detail, str)
    assert "agent_message" in detail
    assert "pipeline_event" in detail

    assert client.get("/api/dashboard/messages?kind=bogus").status_code == 422
    assert client.get("/api/dashboard/messages?kind=").status_code == 422


def test_messages_rejects_sse_wire_kinds(client, seed_db):
    """Wire kinds are not storage kinds; asking for one must 422, not 200 + [].

    `message_logged` and `agent_status_change` are what server.py sse_kind()
    emits on GET /api/events. Neither is ever written to message_log: they are
    absent from server.py VALID_EVENT_KINDS (the write-side validator enforced
    on POST /api/event), no writer can produce them, and the live table held
    zero such rows across event_id 1..6759 when this was checked.

    The operator most likely to type one is someone who just saw
    "kind":"message_logged" in an SSE frame and went looking for the row. If the
    allowlist accepted it they would get 200 + [] forever and conclude the data
    was missing, rather than being told they crossed two namespaces. That is the
    same silent-empty-page failure test_messages_invalid_kind_422 closes, so it
    is pinned here rather than left to the allowlist's shape.
    """
    for wire_kind in ("message_logged", "agent_status_change"):
        r = client.get(f"/api/dashboard/messages?kind={wire_kind}")
        assert r.status_code == 422, f"{wire_kind} must be rejected, got {r.status_code}"

    # Storage `status` IS accepted — it is the counterpart whose wire name is
    # agent_status_change. Asserting both directions is what makes this a test
    # of the namespace split rather than of one string.
    now = _now_ms()
    _seed_message(seed_db, "a", "b", "stat", now - 1000, kind="status")
    assert client.get("/api/dashboard/messages?kind=status").status_code == 200

    # ...but a non-default ALLOWED kind still works. This is the superset pin:
    # narrowing the allowlist to just the UI's agent_message turns this red.
    r = client.get("/api/dashboard/messages?kind=status")
    assert r.status_code == 200
    assert [m["content"] for m in r.json()["messages"]] == ["stat"]


# ---------------------------------------------------------------- DB degrade

def test_endpoints_degrade_when_db_missing(client, monkeypatch, tmp_path):
    # Point BROKER_DB_PATH at a nonexistent file. mode=ro on an absent DB raises
    # sqlite3.OperationalError ("unable to open database file"); each data endpoint
    # must catch (sqlite3.Error, OSError) and return its empty/degraded payload at
    # HTTP 200 (never propagate to a 500).
    monkeypatch.setenv("BROKER_DB_PATH", str(tmp_path / "does_not_exist.db"))

    r = client.get("/api/dashboard/agents")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["agents"] == []
    assert body["counts"] == {"live": 0, "recent": 0, "idle": 0, "stale": 0}
    assert body["degraded"] is True

    r = client.get("/api/dashboard/pipelines")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["pipelines"] == []
    assert body["total"] == 0
    assert body["in_flight_count"] == 0
    assert body["degraded"] is True

    r = client.get("/api/dashboard/messages")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["messages"] == []
    assert body["next_before"] is None
    assert body["degraded"] is True
