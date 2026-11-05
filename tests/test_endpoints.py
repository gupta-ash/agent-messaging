"""Test all 14 endpoints' happy paths + a few error cases (M1=11 + M2=+3 discord-poll-targets per contract v1.1).

Coverage commitments per plan § Test Strategy item 1.
"""

from __future__ import annotations
import asyncio
import base64
import hashlib
import json
import os
import sqlite3
import uuid

import pytest

pytestmark = pytest.mark.asyncio


# ---------- POST/GET /api/agent/{id}/status ----------

async def test_post_agent_status_creates_row(client):
    body = {
        "kind": "demo_research",
        "channel": "#cc-demo_research",
        "status": "idle",
        "last_activity_ts": 1746400000000,
    }
    r = await client.post("/api/agent/test-1/status", json=body)
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["id"] == "test-1"
    assert isinstance(data["event_id"], int) and data["event_id"] >= 1


async def test_post_agent_status_status_machine(client):
    base = {"kind": "demo_coder", "channel": "#cc-demo_coder", "last_activity_ts": 1746400000000}
    for new_status in ["idle", "queued", "running", "blocked", "done", "failed"]:
        r = await client.post("/api/agent/sm-1/status", json={**base, "status": new_status})
        assert r.status_code == 200


async def test_post_agent_status_invalid_status(client):
    body = {"kind": "x", "channel": "#x", "status": "bogus", "last_activity_ts": 1}
    r = await client.post("/api/agent/x/status", json=body)
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "validation_error"


async def test_get_agent_status_returns_row(client):
    await client.post("/api/agent/g-1/status", json={
        "kind": "demo_writer", "channel": "#cc-demo_writer",
        "status": "running", "current_task": "draft post",
        "last_activity_ts": 1746400000000,
    })
    r = await client.get("/api/agent/g-1/status")
    assert r.status_code == 200
    d = r.json()
    assert d["id"] == "g-1"
    assert d["kind"] == "demo_writer"
    assert d["status"] == "running"
    assert d["queue_depth"] == 0
    assert d["current_task"] == "draft post"


async def test_get_agent_status_404(client):
    r = await client.get("/api/agent/nonexistent/status")
    assert r.status_code == 404


# ---------- POST /api/event ----------

async def test_post_event_creates_row(client):
    body = {
        "from_agent": "ag1",
        "to_agent": "user:1",
        "channel": "#cc-demo_research",
        "kind": "agent_reply",
        "content": "hello",
        "ts": 1746400000000,
    }
    r = await client.post("/api/event", json=body)
    assert r.status_code == 200, r.text
    assert isinstance(r.json()["event_id"], int)


async def test_post_event_invalid_kind(client):
    body = {"from_agent": "a", "channel": "#x", "kind": "wrong",
            "content": "x", "ts": 1}
    r = await client.post("/api/event", json=body)
    assert r.status_code == 422


async def test_post_event_permission_reply_kind(client):
    # Per D16 § (b): permission_reply is a valid event kind for v1.1
    # (M2 broker poller emits these via direct SQL; this test ensures
    # external POSTers can also use the kind for symmetry).
    body = {
        "from_agent": "user:111111111111111111",
        "to_agent": None,
        "channel": "#cc-claude",
        "kind": "permission_reply",
        "content": '{"request_id": "abcde", "behavior": "allow"}',
        "ts": 1746400000000,
    }
    r = await client.post("/api/event", json=body)
    assert r.status_code == 200, r.text
    assert isinstance(r.json()["event_id"], int)


async def test_post_event_empty_from_agent(client):
    body = {"from_agent": "", "channel": "#x", "kind": "user_msg",
            "content": "x", "ts": 1}
    r = await client.post("/api/event", json=body)
    assert r.status_code == 422


async def test_post_event_overlong_names_rejected(client):
    """Ingress bound (reaper robustness root-cause): a multi-hundred-char peer name is
    rejected at /api/event so it never enters delivery and can't drive unbounded per-row
    work in the escalation reaper. 256 is generous (realistic <= 64)."""
    r_from = await client.post("/api/event", json={
        "from_agent": "n" * 300, "channel": "#x", "kind": "user_msg", "content": "x", "ts": 1})
    assert r_from.status_code == 422
    r_to = await client.post("/api/event", json={
        "from_agent": "a", "to_agent": "n" * 300, "channel": "#x", "kind": "agent_message",
        "content": "x", "ts": 1})
    assert r_to.status_code == 422
    # boundary: exactly 256 accepted, 257 rejected
    r_256 = await client.post("/api/event", json={
        "from_agent": "a" * 256, "channel": "#x", "kind": "user_msg", "content": "x", "ts": 1})
    assert r_256.status_code == 200
    r_257 = await client.post("/api/event", json={
        "from_agent": "a" * 257, "channel": "#x", "kind": "user_msg", "content": "x", "ts": 1})
    assert r_257.status_code == 422


async def test_post_agent_status_overlong_agent_id_rejected(client):
    """Parity bound: the peer-authored agent_id path param (stored as message_log.from_agent
    for kind='status') is length-capped at the status endpoint, so no unbounded name enters
    message_log via this second path either. 256 accepted, 257 rejected."""
    body = {"kind": "demo_research", "channel": "#x", "status": "idle", "last_activity_ts": 1}
    r_bad = await client.post("/api/agent/" + "a" * 300 + "/status", json=body)
    assert r_bad.status_code == 422
    r_ok = await client.post("/api/agent/" + "a" * 256 + "/status", json=body)
    assert r_ok.status_code == 200


# ---------- GET /api/state ----------

async def test_get_state_empty(client):
    r = await client.get("/api/state")
    assert r.status_code == 200
    d = r.json()
    assert d["agents"] == []
    assert d["pipelines"] == []


async def test_get_state_with_agent(client):
    await client.post("/api/agent/s1/status", json={
        "kind": "demo_research", "channel": "#cc-demo_research",
        "status": "idle", "last_activity_ts": 1746400000000,
    })
    r = await client.get("/api/state")
    assert r.status_code == 200
    d = r.json()
    assert len(d["agents"]) == 1
    assert d["agents"][0]["id"] == "s1"


async def test_get_state_pagination_params_accepted(client):
    # Pagination accepts limit + opaque cursor; valid no-cursor + valid cursor both 200
    r = await client.get("/api/state?limit=10")
    assert r.status_code == 200
    assert "next_cursor" in r.json()


# ---------- POST /api/project/register + POST /api/project/{name}/refresh ----------

async def test_post_project_register(client, project_state_db):
    r = await client.post("/api/project/register", json={
        "name": project_state_db["name"],
        "root_path": project_state_db["root_path"],
        "state_db_path": project_state_db["state_db_path"],
    })
    assert r.status_code == 200, r.text
    d = r.json()
    assert "project_id" in d
    assert isinstance(d["registered_at"], int)


async def test_post_project_register_missing_state_db(client, tmp_path):
    r = await client.post("/api/project/register", json={
        "name": "ghost",
        "root_path": str(tmp_path),
        "state_db_path": str(tmp_path / "nonexistent.db"),
    })
    assert r.status_code == 400


async def test_post_project_register_relative_path(client, tmp_path):
    r = await client.post("/api/project/register", json={
        "name": "ghost",
        "root_path": str(tmp_path),
        "state_db_path": "relative/path/state.db",
    })
    assert r.status_code == 400


# ---- D27 (v1.2): cross-machine state.db registration via local_validation flag ----

async def test_post_project_register_windows_path_accepted_in_remote_mode(client, tmp_path):
    """D27 (a): a syntactically Windows-shape absolute path (e.g. E:\\dev\\...)
    is accepted by the path-shape check regardless of host OS. The bug that
    motivated D27 was Linux pathlib.Path treating `E:\\...` as relative; the
    PureWindowsPath/PurePosixPath check decouples shape-validation from the
    host OS. local_validation=False is required on a Linux broker because
    the file itself is unreachable, but the SHAPE check passes either way."""
    r = await client.post("/api/project/register", json={
        "name": "winshape",
        "root_path": "C:\\workspaces\\sample",
        "state_db_path": "C:\\workspaces\\sample\\state.db",
        "local_validation": False,
    })
    assert r.status_code == 200, r.text
    d = r.json()
    assert "project_id" in d
    assert isinstance(d["registered_at"], int)


async def test_post_project_register_local_validation_false_skips_fs_checks(client, tmp_path):
    """D27 (b): with local_validation=False, the broker skips Path.exists()
    and the sqlite_master table-presence check. A path that does NOT exist
    on disk + has no readable state.db MUST still return 200 in remote mode."""
    nonexistent = str(tmp_path / "remote_machine_db" / "state.db")
    # Sanity: confirm the file truly doesn't exist (default mode would 400)
    from pathlib import Path as _P
    assert not _P(nonexistent).exists()
    r = await client.post("/api/project/register", json={
        "name": "remote-skip-fs",
        "root_path": str(tmp_path / "remote_machine_db"),
        "state_db_path": nonexistent,
        "local_validation": False,
    })
    assert r.status_code == 200, r.text


async def test_post_project_register_local_validation_strict_bool_rejects_string_false(client, tmp_path):
    """D27 (b) iter-2 L1 split: Pydantic strict bool rejects string coercion.
    The wire-format contract requires JSON bool literal `false`, not string
    `"false"`. Strict bool surfaces the bug as a 422 (validation_error) so
    callers see a fail-fast at the contract boundary instead of silent
    coercion masking a JS-client serialization bug."""
    nonexistent = str(tmp_path / "remote_machine_db" / "state.db")
    r_strict = await client.post("/api/project/register", json={
        "name": "remote-strict-bool",
        "root_path": str(tmp_path),
        "state_db_path": nonexistent,
        "local_validation": "false",
    })
    assert r_strict.status_code == 422, r_strict.text


async def test_post_project_register_rejects_null_byte_in_path(client, tmp_path):
    """D27 iter-2 (M3): path containing a null byte is rejected with 400
    in BOTH local_validation=True (default) and local_validation=False
    modes. The check runs BEFORE the absoluteness gate and BEFORE the
    local_validation branch, so a null-byte-bearing absolute path also
    surfaces the more-specific null-byte error message."""
    bad_path = "/abs/path/with\x00null/state.db"
    # Default local_validation=True (omitted)
    r_default = await client.post("/api/project/register", json={
        "name": "nullbyte-default",
        "root_path": str(tmp_path),
        "state_db_path": bad_path,
    })
    assert r_default.status_code == 400, r_default.text
    assert "null bytes" in r_default.json()["error"]["message"], r_default.text
    # Explicit local_validation=False (remote mode)
    r_remote = await client.post("/api/project/register", json={
        "name": "nullbyte-remote",
        "root_path": str(tmp_path),
        "state_db_path": bad_path,
        "local_validation": False,
    })
    assert r_remote.status_code == 400, r_remote.text
    assert "null bytes" in r_remote.json()["error"]["message"], r_remote.text


async def test_post_project_register_local_validation_true_preserves_existing(client, tmp_path):
    """D27 (c): local_validation=True (the default) preserves existing
    behavior — a missing state.db still returns 400. This is the regression
    guard for backward-compat of all callers that omit the flag."""
    missing_db = str(tmp_path / "definitely_not_here.db")
    # Explicit True
    r = await client.post("/api/project/register", json={
        "name": "preserve-explicit",
        "root_path": str(tmp_path),
        "state_db_path": missing_db,
        "local_validation": True,
    })
    assert r.status_code == 400
    # Omitted (default True)
    r2 = await client.post("/api/project/register", json={
        "name": "preserve-default",
        "root_path": str(tmp_path),
        "state_db_path": missing_db,
    })
    assert r2.status_code == 400


async def test_post_project_register_local_validation_false_still_rejects_relative(client, tmp_path):
    """D27 (d): the path-shape check applies in BOTH modes — even with
    local_validation=False, a non-absolute path MUST be rejected because
    the path is recorded in project_registry.state_db_path and surfaced in
    /api/state responses."""
    r = await client.post("/api/project/register", json={
        "name": "remote-relative",
        "root_path": str(tmp_path),
        "state_db_path": "relative/path/state.db",
        "local_validation": False,
    })
    assert r.status_code == 400


async def test_post_project_register_local_validation_false_records_registry_row(client, tmp_path):
    """D27 (e): with local_validation=False, the project_registry row is
    still UPSERTed and the response shape is identical to the local-mode
    case. Verified by re-issuing the register with same name + observing
    the same project_id (UPSERT-by-name preserves the row)."""
    nonexistent = str(tmp_path / "machine_a" / "state.db")
    r1 = await client.post("/api/project/register", json={
        "name": "remote-record",
        "root_path": str(tmp_path / "machine_a"),
        "state_db_path": nonexistent,
        "local_validation": False,
    })
    assert r1.status_code == 200, r1.text
    d1 = r1.json()
    assert "project_id" in d1 and isinstance(d1["project_id"], str) and len(d1["project_id"]) > 0
    assert isinstance(d1["registered_at"], int) and d1["registered_at"] > 0
    # Re-register with same name → UPSERT preserves project_id (per F8 read-back)
    r2 = await client.post("/api/project/register", json={
        "name": "remote-record",
        "root_path": str(tmp_path / "machine_a"),
        "state_db_path": nonexistent,
        "local_validation": False,
    })
    assert r2.status_code == 200, r2.text
    assert r2.json()["project_id"] == d1["project_id"]


async def test_post_project_refresh(client, project_state_db):
    await client.post("/api/project/register", json=project_state_db)
    r = await client.post(f"/api/project/{project_state_db['name']}/refresh")
    assert r.status_code == 200, r.text
    d = r.json()
    assert d["pipeline_count"] == 1
    assert isinstance(d["refreshed_at"], int)


async def test_post_project_refresh_returns_404_for_unknown_project(client):
    """D27 iter-2 (H2): post_project_refresh consults project_registry first
    so an unknown project name surfaces as a clean 404 instead of a misleading
    200 + pipeline_count=0 from refresh_pipelines_view's missing-file branch."""
    r = await client.post("/api/project/nonexistent-project-zzz/refresh")
    assert r.status_code == 404, r.text
    assert "not registered" in r.json()["error"]["message"]


async def test_post_project_refresh_returns_409_for_remote_registered_project(client, tmp_path):
    """D27 iter-2 (H2): post_project_refresh returns 409 Conflict for a
    project registered with local_validation=False. The broker categorically
    cannot read a remote state.db; previously refresh_pipelines_view's
    missing-file branch silently no-op'd with 200 + pipeline_count=0 so
    operators believed refresh succeeded. 409 surfaces the actual semantic
    ('not applicable in this mode') so callers can react."""
    # Register a remote project (local_validation=False) so the broker
    # records the row but does NOT attempt FS access.
    nonexistent = str(tmp_path / "remote_host_db" / "state.db")
    r_reg = await client.post("/api/project/register", json={
        "name": "remote-refresh-409",
        "root_path": str(tmp_path / "remote_host_db"),
        "state_db_path": nonexistent,
        "local_validation": False,
    })
    assert r_reg.status_code == 200, r_reg.text
    # Now try to refresh — must return 409 with the documented detail message.
    r = await client.post("/api/project/remote-refresh-409/refresh")
    assert r.status_code == 409, r.text
    assert "remote-registered" in r.json()["error"]["message"]


async def test_lifespan_startup_gate_filters_remote_rows():
    """D27 iter-2 (H1) helper-style unit test: the lifespan startup loop
    skips rows where local_validation=0 so the broker doesn't spawn idle
    Observer threads watching CWD for unreachable Windows-shape paths after
    restart. Iter-3 (N4 LOW): tests the canonical `_is_local_registry_row`
    helper that production lifespan + this test both call — eliminates the
    earlier copy-paste predicate and removes any drift risk between the
    production gate and the test's assertion. Lifespan integration is
    verified by the existing happy-path register tests at startup time when
    pytest spins up the app."""
    import os as _os
    import tempfile as _tempfile
    from broker import server as _srv
    from broker.server import _is_local_registry_row
    # Isolate coordination.db so we don't pollute the test harness DB.
    tmp_dir = _tempfile.mkdtemp(prefix="d27-lifespan-")
    coord_db = _os.path.join(tmp_dir, "coordination.db")
    prev = _os.environ.get("BROKER_DB_PATH")
    _os.environ["BROKER_DB_PATH"] = coord_db
    try:
        await _srv.ensure_schema()
        # Insert two rows: one local (local_validation=1), one remote (=0).
        async with _srv.connect_db() as conn:
            await conn.execute(
                "INSERT INTO project_registry (project_id, name, root_path, "
                " state_db_path, registered_at, last_refresh_ts, local_validation) "
                "VALUES (?, ?, ?, ?, ?, NULL, ?)",
                ("p1", "local-row", "/tmp/local", "/tmp/local/state.db", 1, 1),
            )
            await conn.execute(
                "INSERT INTO project_registry (project_id, name, root_path, "
                " state_db_path, registered_at, last_refresh_ts, local_validation) "
                "VALUES (?, ?, ?, ?, ?, NULL, ?)",
                ("p2", "remote-row", "E:\\dev\\proj", "E:\\dev\\proj\\state.db", 2, 0),
            )
            await conn.commit()
            cur = await conn.execute(
                "SELECT name, state_db_path, local_validation FROM project_registry"
            )
            rows = await cur.fetchall()
            await cur.close()
        # Per-row predicate assertions: helper returns the expected bool for each
        # registry-row mode. Drives the same predicate the lifespan loop uses.
        by_name = {r["name"]: r for r in rows}
        assert _is_local_registry_row(by_name["local-row"]) is True
        assert _is_local_registry_row(by_name["remote-row"]) is False
        # Compute kept/skipped via the helper (mirroring the lifespan loop).
        kept = [r for r in rows if _is_local_registry_row(r)]
        skipped_remote = sum(1 for r in rows if not _is_local_registry_row(r))
        kept_names = sorted(r["name"] for r in kept)
        assert kept_names == ["local-row"]
        assert skipped_remote == 1
    finally:
        if prev is None:
            _os.environ.pop("BROKER_DB_PATH", None)
        else:
            _os.environ["BROKER_DB_PATH"] = prev
        try:
            _os.remove(coord_db)
        except OSError:
            pass


# ---------- GET/POST /api/discord/cursors ----------

async def test_post_discord_cursors_with_thread(client):
    r = await client.post("/api/discord/cursors", json={
        "channel_id": "100", "thread_id": "200",
        "last_seen_message_id": "999", "last_check_ts": 1746400000000,
    })
    assert r.status_code == 200
    assert r.json() == {"updated": True}


async def test_post_discord_cursors_no_thread_normalizes_to_empty(client):
    # Omitted thread_id
    r = await client.post("/api/discord/cursors", json={
        "channel_id": "101",
        "last_seen_message_id": "1", "last_check_ts": 1,
    })
    assert r.status_code == 200
    # Verify in GET
    r = await client.get("/api/discord/cursors", params={"channel_id": "101"})
    assert r.status_code == 200
    cursors = r.json()["cursors"]
    assert len(cursors) == 1
    assert cursors[0]["thread_id"] == ""


async def test_post_discord_cursors_null_thread_normalizes_to_empty(client):
    r = await client.post("/api/discord/cursors", json={
        "channel_id": "102", "thread_id": None,
        "last_seen_message_id": "1", "last_check_ts": 1,
    })
    assert r.status_code == 200
    r = await client.get("/api/discord/cursors", params={"channel_id": "102"})
    cursors = r.json()["cursors"]
    assert len(cursors) == 1 and cursors[0]["thread_id"] == ""


async def test_post_discord_cursors_upsert_same_pk(client):
    body = {"channel_id": "200", "thread_id": "300",
            "last_seen_message_id": "1", "last_check_ts": 100}
    await client.post("/api/discord/cursors", json=body)
    body2 = {**body, "last_seen_message_id": "999", "last_check_ts": 200}
    await client.post("/api/discord/cursors", json=body2)
    r = await client.get("/api/discord/cursors",
                         params={"channel_id": "200", "thread_id": "300"})
    cursors = r.json()["cursors"]
    assert len(cursors) == 1
    assert cursors[0]["last_seen_message_id"] == "999"
    assert cursors[0]["last_check_ts"] == 200


async def test_get_discord_cursors_all(client):
    await client.post("/api/discord/cursors", json={
        "channel_id": "1", "thread_id": "1",
        "last_seen_message_id": "a", "last_check_ts": 1,
    })
    await client.post("/api/discord/cursors", json={
        "channel_id": "2",
        "last_seen_message_id": "b", "last_check_ts": 2,
    })
    r = await client.get("/api/discord/cursors")
    cursors = r.json()["cursors"]
    assert len(cursors) == 2


# ---------- GET /api/health ----------

async def test_health_200_and_sane_on_an_empty_database(client):
    """The endpoint ANSWERS on a fresh database and its component check passes.

    The `status` assertion this test used to carry moved to
    `test_health_ok_on_a_fresh_empty_database` when idleness stopped driving
    `status` (2026-08-08). What remains is the part that was always the real
    liveness signal, and the only part any call site on the fleet reads: the
    HTTP status code.
    """
    r = await client.get("/api/health")
    assert r.status_code == 200
    d = r.json()
    assert d["checks"]["sqlite"] is True
    assert d["checks"]["last_event_ts"] == 0


async def test_health_ok_after_event(client):
    await client.post("/api/event", json={
        "from_agent": "h1", "channel": "#h", "kind": "user_msg",
        "content": "x", "ts": 1746400000000,
    })
    r = await client.get("/api/health")
    assert r.json()["checks"]["last_event_ts"] == 1746400000000


# ---------- GET /api/health: the empty-database blind spot (2026-08-07 incident) ----------
#
# `get_health` once computed `age_ok = (last_event_ts == 0) or (...)`. An EMPTY
# message_log has no MAX(ts), which became 0, which short-circuited the age check --
# so a freshly-created 4,096-byte coordination.db reported "ok" while a live broker
# served the WRONG tree for five minutes. Nothing exercised that path, which is the
# whole reason the defect survived.
#
# THE AGE CHECK IS GONE ENTIRELY (2026-08-08), so the exemption it contained can no
# longer be reinstated -- there is no predicate left to exempt anything from. What
# still matters, and what these tests now guard, is that the incident stays
# DETECTABLE: `log_empty` is reported as its own fact, and `db_path` names the
# database this process actually opened. The second is the DIRECT detector and is
# strictly better than inferring a wrong-tree install from an absence of rows.


async def test_health_states_that_the_log_is_empty(client):
    """The empty-log signal must be its OWN fact, not inferred from a sentinel.

    `last_event_ts` deliberately keeps its old wire value of 0 -- broker_client.py
    and the fleet's deploy tooling read that field, so every new key is additive.
    """
    d = (await client.get("/api/health")).json()
    assert d["checks"]["log_empty"] is True
    assert d["checks"]["last_event_ts"] == 0, "wire compatibility must be preserved"


async def test_health_log_empty_can_be_false(client):
    """FIRING CONTROL. Without this, the assertion above passes against a
    hardcoded True and proves nothing -- a check that cannot fail on the fault
    it guards is not a check."""
    before = (await client.get("/api/health")).json()["checks"]["log_empty"]
    await client.post("/api/event", json={
        "from_agent": "h2", "channel": "#h", "kind": "user_msg",
        "content": "x", "ts": 1746400000001,
    })
    after = (await client.get("/api/health")).json()["checks"]["log_empty"]
    assert before is True and after is False, (
        f"log_empty did not discriminate: before={before!r} after={after!r}"
    )


async def test_health_reports_which_database_it_opened(client):
    """The broker CANNOT detect that it is the wrong installation: a second tree
    has the same filename and the same `BROKER_DB_PATH=./coordination.db`, so it
    is self-consistent and indistinguishable from inside the process. Reporting
    the resolved path does not prevent that -- it makes it visible in one glance
    to a human, or to tooling that knows the intended root."""
    import os

    d = (await client.get("/api/health")).json()
    db_path = d["checks"]["db_path"]
    assert os.path.isabs(db_path), f"db_path must be absolute, got {db_path!r}"
    assert db_path == os.path.abspath(os.environ["BROKER_DB_PATH"]), (
        "db_path must name the database this process actually opened"
    )


# ---------- GET /api/health: `status` reports FAULTS, never idleness (2026-08-08) ----------
#
# `status` used to flip to "degraded" from an ABSENCE of traffic: an hour of quiet,
# and its limiting case, an empty log. Neither observes a failing component. On this
# fleet idleness is the steady state -- two of four boxes are routinely quiet for
# weeks with every sub-check passing -- so the alarm state WAS the steady state, and
# humans and peers misread it twice, once because the shipped README told them to.
#
# These three pin the SUBTRACTION, and the third is what makes the first two mean
# anything. Written to fail against the pre-change bytes: today both of the first
# two answer "degraded".


async def test_health_ok_when_the_log_is_stale(client):
    """An IDLE broker whose components are healthy is `ok` -- the GENUINE-AGE arm.

    The timestamp is real and far older than the one-hour threshold the deleted
    heuristic used, so this arm fails against ANY reintroduction of an age
    predicate -- including one carrying the old `(last_event_ts == 0)` exemption.

    THAT IS WHY IT IS NOT ts=0, and the reason is worth keeping: cross-family
    review found that a ts=0 fixture alone lets a SENTINEL-BASED PARTIAL REVERT
    pass. Reinstating `age_ok = (last_event_ts == 0) or (...)` would satisfy a
    ts=0 assertion, satisfy the empty-database assertion, and satisfy both
    surviving fault controls -- so the suite would have been green against
    precisely the exemption the contract's DO-NOT-REINSTATE paragraph exists to
    prevent. A test that cannot fail on the fault it guards is not a test.

    Age is not deleted, it is DEMOTED: `last_event_ts` still carries it, so a
    consumer that wants a staleness alarm can compute one and choose its own
    threshold -- which was always the honest place for that decision.
    """
    from broker.server import now_ms

    stale_ts = now_ms() - (30 * 24 * 60 * 60 * 1000)  # 30d: the idlest real fleet box
    r = await client.post("/api/event", json={
        "from_agent": "h4", "channel": "#h", "kind": "user_msg",
        "content": "x", "ts": stale_ts,
    })
    assert r.status_code == 200, r.text
    d = (await client.get("/api/health")).json()
    assert d["checks"]["log_empty"] is False, "a row exists, so the log is not empty"
    assert d["checks"]["last_event_ts"] == stale_ts, "the age signal must survive the wire"
    assert d["status"] == "ok", (
        "idleness is not a fault -- `status` must report only a failing component"
    )


async def test_health_ok_for_a_ts_zero_event(client):
    """The SENTINEL arm, held as its own case rather than folded into the one above.

    `ts=0` is reachable through the normal API -- the validator rejects only
    `ts < 0` -- and it is the exact value the deleted exemption keyed on. Keeping
    both arms means a partial revert fails whichever half it reintroduces, and
    keeping them SEPARATE means the failure names which half.
    """
    r = await client.post("/api/event", json={
        "from_agent": "h5", "channel": "#h", "kind": "user_msg",
        "content": "x", "ts": 0,
    })
    assert r.status_code == 200, r.text
    d = (await client.get("/api/health")).json()
    assert d["checks"]["log_empty"] is False, "a row exists, so the log is not empty"
    assert d["checks"]["last_event_ts"] == 0
    assert d["status"] == "ok", (
        "a ts=0 row is idle, not faulty -- the old sentinel exemption is gone, not moved"
    )


async def test_health_ok_on_a_fresh_empty_database(client):
    """A fresh install is `ok`, and still SAYS its log is empty.

    This is NOT a reinstatement of the old exemption, and the difference is the
    whole point. The old code let an empty log SATISFY the age check, so emptiness
    was reported as healthy BY THAT CRITERION. Here the criterion is gone; nothing
    is exempted because there is nothing left to be exempt from, and `log_empty`
    stays visible in `checks`.

    The incident that motivated the empty-log rule -- a broker serving a fresh
    4,096-byte coordination.db while the real hub went unserved -- is now detected
    DIRECTLY by `checks.db_path`, observed from the connection that served this
    probe. Emptiness no longer has to stand in for it.
    """
    d = (await client.get("/api/health")).json()
    assert d["checks"]["log_empty"] is True
    assert d["checks"]["sqlite"] is True
    assert d["status"] == "ok", (
        "an empty log is an absence of traffic, not a failing component"
    )


async def test_health_still_degraded_when_sqlite_fails(client, monkeypatch):
    """BLAST-RADIUS CONTROL, and the two tests above are worthless without it.

    A subtraction that removes the last path to "degraded" would make `status`
    a constant, and both assertions above would pass for the wrong reason. A
    genuinely failing component must still degrade.
    """
    from broker import server as _srv

    class _Boom:
        async def __aenter__(self):
            raise RuntimeError("db connect failed")

        async def __aexit__(self, *a):
            return False

    monkeypatch.setattr(_srv, "connect_db", lambda *a, **k: _Boom())
    d = (await client.get("/api/health")).json()
    assert d["checks"]["sqlite"] is False
    assert d["status"] == "degraded", "a real component fault must still degrade"


# ---------- GET /api/health: attached peers, for the deploy/restart decision (2026-08-08) ----------


async def test_health_reports_zero_attached_peers_when_none_are_subscribed(client):
    """The field must EXIST, because the installer keys a restart decision on it.

    Against the pre-change bytes this raises KeyError rather than failing an
    assertion -- the field is absent entirely -- which is what makes the three
    tests in this block discriminate rather than merely pass.
    """
    d = (await client.get("/api/health")).json()
    assert d["checks"]["attached_peers"] == 0


async def test_health_counts_a_live_subscriber(client):
    """A NON-ZERO count must be reachable, or the deferral arm can never fire.

    Asserting only the zero case would be satisfied by a hardcoded 0, which is
    precisely the shape that would make the installer restart a busy broker
    every time while every test stayed green.
    """
    from broker import server as _srv

    sub = _srv._Subscriber(0)
    _srv._subscribers.append(sub)
    try:
        d = (await client.get("/api/health")).json()
        assert d["checks"]["attached_peers"] == 1
    finally:
        _srv._subscribers.remove(sub)

    after = (await client.get("/api/health")).json()
    assert after["checks"]["attached_peers"] == 0, "the count must fall again on detach"


async def test_attached_peers_does_not_affect_status(client):
    """D189's guard, pinned so a future edit cannot quietly undo it.

    `status` reports failing components, never traffic. An attached peer is
    traffic. The whole subject of D189 was removing a traffic observation from
    `overall`; folding a peer count into it would re-create that defect under a
    new field name, and it would look reasonable while doing so.
    """
    from broker import server as _srv

    sub = _srv._Subscriber(0)
    _srv._subscribers.append(sub)
    try:
        d = (await client.get("/api/health")).json()
        assert d["checks"]["attached_peers"] == 1
        assert d["status"] == "ok", "attachment is not a fault and must not degrade"
    finally:
        _srv._subscribers.remove(sub)


# ---------- Error response shape ----------

async def test_error_response_shape(client):
    r = await client.post("/api/event", json={})  # invalid body
    assert r.status_code == 422
    body = r.json()
    assert "error" in body
    assert set(body["error"].keys()) >= {"code", "message", "request_id"}
    assert body["error"]["code"] == "validation_error"
    # request_id is a string (uuid4)
    assert isinstance(body["error"]["request_id"], str)
    assert len(body["error"]["request_id"]) > 0


async def test_request_id_in_response_header(client):
    r = await client.get("/api/health")
    assert "x-request-id" in {k.lower() for k in r.headers.keys()}


# ---------- F8 (iter2 discriminator): X-Request-ID server-generated, never client-echoed ----------

import re as _re

_UUID_RE = _re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


async def test_request_id_response_header_is_server_generated_uuid(client):
    """F8: X-Request-ID response header MUST be a server-generated UUID4,
    not whatever the client supplied. The buggy iter-1 behavior reflected
    a client-supplied X-Request-ID into both header and error body — a token
    leak surface that bypasses the F1 sanitizer."""
    r = await client.get("/api/health")
    rid = r.headers.get("x-request-id")
    assert rid is not None
    assert _UUID_RE.match(rid), f"X-Request-ID must be UUID4-shaped, got {rid!r}"


async def test_request_id_does_not_echo_client_token_in_header(client):
    """F8: token-shaped X-Request-ID MUST NOT be echoed in the response header."""
    fake_token = "MTQ4N_FAKE_TOKEN_VALUE"
    r = await client.get("/api/health", headers={"X-Request-ID": fake_token})
    rid = r.headers.get("x-request-id")
    assert rid is not None
    assert "MTQ4N" not in rid, f"client X-Request-ID leaked into response header: {rid!r}"
    assert _UUID_RE.match(rid), f"X-Request-ID must be UUID4-shaped, got {rid!r}"


async def test_request_id_does_not_echo_client_token_in_422_body(client):
    """F8: token-shaped X-Request-ID MUST NOT appear in 422 response body
    via `error.request_id`. The buggy iter-1 middleware copied client header
    into request.state.request_id which was then echoed by _err_response."""
    fake_token = "MTQ4N_FAKE_TOKEN_VALUE"
    r = await client.post(
        "/api/event",
        json={},  # invalid → 422
        headers={"X-Request-ID": fake_token},
    )
    assert r.status_code == 422
    raw = r.text
    assert "MTQ4N" not in raw, (
        f"client X-Request-ID leaked into 422 body: {raw[:500]!r}"
    )
    body = r.json()
    rid = body["error"]["request_id"]
    assert _UUID_RE.match(rid), f"error.request_id must be UUID4-shaped, got {rid!r}"


async def test_request_id_does_not_echo_client_token_in_500_body(client, monkeypatch):
    """F8: token-shaped X-Request-ID MUST NOT appear in a 500 body either."""
    fake_token = "MTQ4N_FAKE_TOKEN_HEADER"
    from broker import server as _srv

    class _Boom:
        async def __aenter__(self):
            raise RuntimeError("boom")
        async def __aexit__(self, *a):
            return False

    def _bad_connect(*a, **k):
        return _Boom()

    monkeypatch.setattr(_srv, "connect_db", _bad_connect)
    r = await client.get("/api/discord/cursors",
                         headers={"X-Request-ID": fake_token})
    assert r.status_code == 500
    assert "MTQ4N" not in r.text
    rid = r.json()["error"]["request_id"]
    assert _UUID_RE.match(rid), f"error.request_id must be UUID4-shaped, got {rid!r}"


# ---------- F9 (iter2 discriminator): M1 endpoints don't echo user input in HTTPException details ----------

async def test_get_agent_status_404_does_not_echo_agent_id(client):
    """F9: GET /api/agent/{id}/status 404 MUST NOT echo the agent_id —
    a token-shaped path param would otherwise appear in the response body."""
    r = await client.get("/api/agent/MTQ4N_FAKE_TOKEN_AGENT/status")
    assert r.status_code == 404
    assert "MTQ4N" not in r.text, (
        f"agent_id leaked into 404 body: {r.text!r}"
    )


async def test_post_project_register_does_not_echo_state_db_path(client, tmp_path):
    """F9: 400 on missing state_db_path MUST NOT echo the path — a
    token-shaped path would otherwise be reflected back."""
    fake_path = str(tmp_path / "MTQ4N_FAKE_TOKEN_PATH.db")
    r = await client.post("/api/project/register", json={
        "name": "leakcheck",
        "root_path": str(tmp_path),
        "state_db_path": fake_path,
    })
    assert r.status_code == 400
    assert "MTQ4N" not in r.text, (
        f"state_db_path leaked into 400 body: {r.text!r}"
    )


async def test_post_project_register_does_not_echo_sqlite_error(client, tmp_path):
    """F9: 400 on un-readable state.db MUST NOT echo str(exc) — sqlite
    errors can include the offending file path."""
    fake_db = tmp_path / "MTQ4N_FAKE_TOKEN_FILE.db"
    # Write garbage so sqlite raises DatabaseError on open
    fake_db.write_bytes(b"not a sqlite file at all")
    r = await client.post("/api/project/register", json={
        "name": "leakcheck-sqlite",
        "root_path": str(tmp_path),
        "state_db_path": str(fake_db),
    })
    assert r.status_code == 400
    assert "MTQ4N" not in r.text, (
        f"sqlite error leaked path into 400 body: {r.text!r}"
    )


async def test_get_state_bad_cursor_does_not_echo_token(client):
    """F9: 422 on malformed cursor MUST NOT echo the cursor value."""
    fake_cursor = "MTQ4N_FAKE_TOKEN_BAD_FORMAT"
    r = await client.get("/api/state", params={"cursor": fake_cursor})
    assert r.status_code == 422
    assert "MTQ4N" not in r.text, (
        f"cursor value leaked into 422 body: {r.text!r}"
    )


async def test_get_events_bad_last_event_id_does_not_echo_token(client):
    """F9: 422 on non-int Last-Event-ID MUST NOT echo the header value."""
    fake_id = "MTQ4N_NOT_INT"
    r = await client.get("/api/events", headers={"Last-Event-ID": fake_id})
    assert r.status_code == 422
    assert "MTQ4N" not in r.text, (
        f"Last-Event-ID leaked into 422 body: {r.text!r}"
    )


# ---------- F1 (iter1 discriminator): token-leak in error responses ----------

async def test_validation_error_does_not_echo_token_shaped_input(client):
    """F1: a malformed POST /api/event body containing a token-shaped value
    must NOT have that value reflected in the 422 response. Pydantic/FastAPI's
    default behavior includes the bad input in `input` / `input_value` keys —
    we strip those server-side."""
    fake_token = "MTQ4NDY0NDUyNjg5ODI4MjU1Ng.fake_secret_payload"
    body = {
        "from_agent": fake_token,  # valid string, but `kind` is bad → triggers validation error
        "channel": "#x",
        "kind": "wrong",  # invalid: not in VALID_EVENT_KINDS → validation error
        "content": "x",
        "ts": "not-a-number",  # also invalid
    }
    r = await client.post("/api/event", json=body)
    assert r.status_code == 422
    raw_body = r.text
    # The fake-token-shaped substring MUST NOT appear anywhere in the response body.
    assert "MTQ4N" not in raw_body, (
        f"token leak in 422 response: response body contained fake-token substring. "
        f"Body sample: {raw_body[:500]}"
    )
    assert fake_token not in raw_body
    # Sanity: validation error structure is intact (loc/type/msg only)
    errors = r.json()["error"]["details"]["errors"]
    assert isinstance(errors, list) and len(errors) >= 1
    for err in errors:
        assert set(err.keys()) <= {"loc", "type", "msg"}


async def test_validation_error_does_not_echo_token_in_ts_field(client):
    """F1: even when the bad value is the `ts` field, the body must not echo it."""
    fake_token = "MTIzNDU2Nzg5MTIzNDU2Nzg5LfakeSecret123"
    body = {
        "from_agent": "ag1",
        "channel": "#x",
        "kind": "user_msg",
        "content": "x",
        "ts": fake_token,  # invalid ts string → validation error
    }
    r = await client.post("/api/event", json=body)
    assert r.status_code == 422
    raw_body = r.text
    assert fake_token not in raw_body
    assert "fakeSecret" not in raw_body


async def test_delete_poll_target_404_does_not_echo_channel_id(client):
    """F1: DELETE on a non-existent poll target must NOT include the user-supplied
    channel_id in the 404 detail. If a caller mistakenly passed a token as
    channel_id, the previous behavior would echo that token back."""
    secret_marker = "SECRET_TOKEN_HERE_MTUyNQ"
    r = await client.delete("/api/discord/poll-targets",
                            params={"channel_id": secret_marker})
    assert r.status_code == 404
    raw_body = r.text
    assert secret_marker not in raw_body, (
        f"channel_id leaked into 404 response: {raw_body!r}"
    )


async def test_delete_poll_target_404_does_not_echo_thread_id(client):
    """F1: thread_id also must not be echoed back in 404 details."""
    secret_thread = "SECRET_THREAD_MTUyNQ_oops"
    r = await client.delete(
        "/api/discord/poll-targets",
        params={"channel_id": "harmless", "thread_id": secret_thread},
    )
    assert r.status_code == 404
    assert secret_thread not in r.text


async def test_generic_500_returns_fixed_string_only(client, monkeypatch):
    """F1: the generic 500 handler must NOT include str(exc) — only a fixed
    message + request_id. We force a 500 by monkey-patching connect_db to raise
    an exception with a token-shaped message."""
    fake_token = "MTAyMzQ1NjcuTOKENinException"
    from broker import server as _srv

    class _Boom:
        async def __aenter__(self):
            raise RuntimeError(f"db connect failed token={fake_token}")
        async def __aexit__(self, *a):
            return False

    def _bad_connect(*a, **k):
        return _Boom()

    monkeypatch.setattr(_srv, "connect_db", _bad_connect)
    r = await client.get("/api/health")
    # Health endpoint catches the inner exception and returns a "degraded" 200,
    # so use an endpoint that does not catch — fall back to /api/discord/cursors.
    if r.status_code == 200:
        r = await client.get("/api/discord/cursors")
    assert r.status_code == 500
    assert fake_token not in r.text
    body = r.json()
    assert body["error"]["code"] == "internal_error"
    assert body["error"]["message"] == "internal server error"


# ---------- v1.3 (D28): agent_message + GET /api/agent/{id}/messages ----------

async def test_post_event_agent_message_accepted(client):
    r = await client.post("/api/event", json={
        "from_agent": "session-A", "to_agent": "session-B", "channel": "#cc-relay",
        "kind": "agent_message", "content": "hello", "ts": 1746404783000,
        "client_event_id": "msg-1",
    })
    assert r.status_code == 200, r.text
    assert "event_id" in r.json()


async def test_post_event_agent_message_without_to_agent_rejected(client):
    r = await client.post("/api/event", json={
        "from_agent": "session-A", "channel": "#cc-relay",
        "kind": "agent_message", "content": "no recipient", "ts": 1746404783000,
    })
    assert r.status_code == 422
    # body should mention to_agent in the validation error
    assert "to_agent" in r.text.lower()


async def test_get_agent_messages_returns_recipient_inbox(client):
    for i in range(3):
        await client.post("/api/event", json={
            "from_agent": "A", "to_agent": "B", "channel": "#c",
            "kind": "agent_message", "content": f"msg-{i}", "ts": 1746404783000 + i,
            "client_event_id": f"k-{i}",
        })
    r = await client.get("/api/agent/b/messages")
    assert r.status_code == 200
    body = r.json()
    assert len(body["messages"]) == 3
    assert body["next_cursor"] is None
    contents = [m["content"] for m in body["messages"]]
    assert contents == ["msg-0", "msg-1", "msg-2"]


async def test_get_agent_messages_cursor_pagination(client):
    for i in range(5):
        await client.post("/api/event", json={
            "from_agent": "A", "to_agent": "B", "channel": "#c",
            "kind": "agent_message", "content": f"m-{i}", "ts": 1746404783000 + i,
            "client_event_id": f"p-{i}",
        })
    r1 = await client.get("/api/agent/b/messages?limit=2")
    body1 = r1.json()
    assert len(body1["messages"]) == 2
    assert body1["next_cursor"] is not None
    r2 = await client.get(f"/api/agent/b/messages?since={body1['next_cursor']}&limit=2")
    body2 = r2.json()
    assert len(body2["messages"]) == 2
    assert body2["next_cursor"] is not None
    r3 = await client.get(f"/api/agent/b/messages?since={body2['next_cursor']}&limit=2")
    body3 = r3.json()
    assert len(body3["messages"]) == 1
    assert body3["next_cursor"] is None


async def test_get_agent_messages_kind_filter(client):
    await client.post("/api/event", json={
        "from_agent": "A", "to_agent": "B", "channel": "#c",
        "kind": "agent_message", "content": "msg", "ts": 1746404783001,
        "client_event_id": "f-1",
    })
    await client.post("/api/event", json={
        "from_agent": "A", "to_agent": "B", "channel": "#c",
        "kind": "handoff", "content": "handoff", "ts": 1746404783002,
        "client_event_id": "f-2",
    })
    r = await client.get("/api/agent/b/messages?kind=agent_message")
    body = r.json()
    assert len(body["messages"]) == 1
    assert body["messages"][0]["kind"] == "agent_message"


async def test_get_agent_messages_unknown_recipient(client):
    r = await client.get("/api/agent/never-targeted/messages")
    assert r.status_code == 200
    assert r.json() == {"messages": [], "next_cursor": None}


# ---------- M2 (iter-2): Query bound regressions on since + limit ----------


async def test_get_agent_messages_since_overflow_rejected(client):
    """M2 (iter-2): `since` > i64 max returns 422, not 500.

    Without the Query(ge=0, le=SQLITE_I64_MAX) bound, a caller passing a
    value above SQLite's INTEGER capacity would reach the SQL bind step
    and surface as sqlite3.OperationalError -> 500. With the bound, the
    Query validator rejects it at the FastAPI layer with 422.
    """
    r = await client.get(f"/api/agent/B/messages?since={2**63}")
    assert r.status_code == 422
    assert "since" in r.text.lower()


async def test_get_agent_messages_limit_out_of_bounds_rejected(client):
    """M2 (iter-2): limit < 1 or > 100 returns 422 via Query validator.

    Iter-1 used a manual `min(max(limit, 1), 100)` clamp that silently
    coerced both extremes to a valid value. Iter-2 removes the clamp in
    favor of Query(ge=1, le=100) which rejects out-of-range with 422.
    """
    r = await client.get("/api/agent/B/messages?limit=0")
    assert r.status_code == 422
    r = await client.get("/api/agent/B/messages?limit=999")
    assert r.status_code == 422


# ---------- v1.3.1 (D29): display_name field on agent_status ----------


async def test_post_agent_status_with_display_name_persists(client):
    """v1.3.1 (D29): POST with display_name persists; GET returns it."""
    body = {
        "kind": "demo_research",
        "channel": "#cc-demo_research",
        "status": "idle",
        "last_activity_ts": 1746400000000,
        "display_name": "Test1",
    }
    r = await client.post("/api/agent/dn-1/status", json=body)
    assert r.status_code == 200, r.text
    g = await client.get("/api/agent/dn-1/status")
    assert g.status_code == 200
    assert g.json()["display_name"] == "Test1"


async def test_post_agent_status_without_display_name_sets_null(client):
    """v1.3.1 (D29): POST without display_name leaves the column NULL;
    GET returns display_name=null."""
    body = {
        "kind": "demo_research",
        "channel": "#cc-demo_research",
        "status": "idle",
        "last_activity_ts": 1746400000000,
    }
    r = await client.post("/api/agent/dn-2/status", json=body)
    assert r.status_code == 200, r.text
    g = await client.get("/api/agent/dn-2/status")
    assert g.status_code == 200
    assert g.json()["display_name"] is None


async def test_post_agent_status_can_clear_display_name(client):
    """v1.3.1 (D29) full-replace semantics: POST display_name='X' then
    re-POST without it nulls the stored value."""
    body_with = {
        "kind": "demo_research",
        "channel": "#cc-demo_research",
        "status": "idle",
        "last_activity_ts": 1746400000000,
        "display_name": "First",
    }
    r1 = await client.post("/api/agent/dn-3/status", json=body_with)
    assert r1.status_code == 200, r1.text
    g1 = await client.get("/api/agent/dn-3/status")
    assert g1.json()["display_name"] == "First"
    # Re-POST WITHOUT display_name -> should NULL the column
    body_without = {
        "kind": "demo_research",
        "channel": "#cc-demo_research",
        "status": "running",
        "last_activity_ts": 1746400000001,
    }
    r2 = await client.post("/api/agent/dn-3/status", json=body_without)
    assert r2.status_code == 200, r2.text
    g2 = await client.get("/api/agent/dn-3/status")
    assert g2.json()["display_name"] is None


async def test_get_state_includes_display_name(client):
    """v1.3.1 (D29): /api/state returns display_name on every agent dict
    (string when set, null when not)."""
    await client.post("/api/agent/state-with-dn/status", json={
        "kind": "demo_research", "channel": "#cc-demo_research",
        "status": "idle", "last_activity_ts": 1746400000000,
        "display_name": "Alice",
    })
    await client.post("/api/agent/state-without-dn/status", json={
        "kind": "demo_research", "channel": "#cc-demo_research",
        "status": "idle", "last_activity_ts": 1746400000001,
    })
    r = await client.get("/api/state")
    assert r.status_code == 200
    agents_by_id = {a["id"]: a for a in r.json()["agents"]}
    assert agents_by_id["state-with-dn"]["display_name"] == "Alice"
    assert agents_by_id["state-without-dn"]["display_name"] is None


# ---------- v1.3.1 (D29): GET /api/agent-messages/recipients ----------


async def test_get_agent_messages_recipients_returns_distinct(client):
    """v1.3.1 (D29): 3 agent_messages to 2 recipients (one twice) -> 2
    distinct entries with msg_count 2 and 1."""
    # 2 messages to recipient X
    await client.post("/api/event", json={
        "from_agent": "S", "to_agent": "rcpt-X", "channel": "#c",
        "kind": "agent_message", "content": "m1", "ts": 1746404783001,
        "client_event_id": "rd-1",
    })
    await client.post("/api/event", json={
        "from_agent": "S", "to_agent": "rcpt-X", "channel": "#c",
        "kind": "agent_message", "content": "m2", "ts": 1746404783002,
        "client_event_id": "rd-2",
    })
    # 1 message to recipient Y
    await client.post("/api/event", json={
        "from_agent": "S", "to_agent": "rcpt-Y", "channel": "#c",
        "kind": "agent_message", "content": "m3", "ts": 1746404783003,
        "client_event_id": "rd-3",
    })
    r = await client.get("/api/agent-messages/recipients")
    assert r.status_code == 200, r.text
    by_id = {x["to_agent"]: x for x in r.json()["recipients"]}
    assert set(by_id.keys()) == {"rcpt-x", "rcpt-y"}
    assert by_id["rcpt-x"]["msg_count"] == 2
    assert by_id["rcpt-y"]["msg_count"] == 1


async def test_get_agent_messages_recipients_joins_display_name(client):
    """v1.3.1 (D29): when a recipient has POSTed agent_status with display_name,
    the recipients endpoint LEFT JOINs and returns the display_name."""
    # POST 1 agent_message to recipient X
    await client.post("/api/event", json={
        "from_agent": "S", "to_agent": "rj-X", "channel": "#c",
        "kind": "agent_message", "content": "m1", "ts": 1746404783001,
        "client_event_id": "rj-1",
    })
    # POST agent_status for X with display_name='Alice'
    await client.post("/api/agent/rj-x/status", json={
        "kind": "demo_research", "channel": "#cc-demo_research",
        "status": "idle", "last_activity_ts": 1746404783002,
        "display_name": "Alice",
    })
    r = await client.get("/api/agent-messages/recipients")
    assert r.status_code == 200, r.text
    by_id = {x["to_agent"]: x for x in r.json()["recipients"]}
    assert by_id["rj-x"]["display_name"] == "Alice"


async def test_get_agent_messages_recipients_folds_direct_id_for_metadata(client):
    registered = await client.post("/api/agent/Mixed-Recipient/status", json={
        "kind": "demo_research", "channel": "#cc-demo_research",
        "status": "idle", "last_activity_ts": 1746404783001,
        "display_name": "Mixed ID Owner",
    })
    assert registered.status_code == 200, registered.text
    sent = await client.post("/api/event", json={
        "from_agent": "S", "to_agent": "MIXED-RECIPIENT", "channel": "#c",
        "kind": "agent_message", "content": "folded metadata", "ts": 1746404783002,
        "client_event_id": "recipient-folded-direct-id",
    })
    assert sent.status_code == 200, sent.text

    response = await client.get("/api/agent-messages/recipients")

    assert response.status_code == 200, response.text
    by_id = {row["to_agent"]: row for row in response.json()["recipients"]}
    assert by_id["mixed-recipient"]["display_name"] == "Mixed ID Owner"


async def test_get_agent_messages_recipients_keeps_uuid_metadata_after_relay_claim(client):
    agent_id = "recipient-stable-id"
    registered = await client.post(f"/api/agent/{agent_id}/status", json={
        "kind": "codex-session", "channel": "#codex-relay",
        "status": "idle", "last_activity_ts": 1746404783002,
        "display_name": "Stable UUID Owner", "transport": "codex-app-server",
    })
    assert registered.status_code == 200, registered.text
    claimed = await client.put(f"/api/agent/{agent_id}/relay-name", json={
        "relay_name": "recipient-relay-name",
    })
    assert claimed.status_code == 200, claimed.text
    await client.post("/api/event", json={
        "from_agent": "S", "to_agent": agent_id, "channel": "#c",
        "kind": "agent_message", "content": "uuid metadata", "ts": 1746404783003,
        "client_event_id": "recipient-uuid-metadata",
    })

    response = await client.get("/api/agent-messages/recipients")

    assert response.status_code == 200, response.text
    by_id = {row["to_agent"]: row for row in response.json()["recipients"]}
    assert by_id[agent_id]["display_name"] == "Stable UUID Owner"


async def test_get_agent_messages_recipients_prefers_relay_metadata_without_duplication(client):
    direct = await client.post("/api/agent/shared-mailbox/status", json={
        "kind": "demo_research", "channel": "#cc-demo_research",
        "status": "idle", "last_activity_ts": 1746404783001,
        "display_name": "Direct ID Owner",
    })
    assert direct.status_code == 200, direct.text
    relay = await client.post("/api/agent/relay-owner-id/status", json={
        "kind": "codex-session", "channel": "#codex-relay",
        "status": "idle", "last_activity_ts": 1746404783002,
        "display_name": "Relay Name Owner", "transport": "codex-app-server",
    })
    assert relay.status_code == 200, relay.text
    claimed = await client.put("/api/agent/relay-owner-id/relay-name", json={
        "relay_name": "shared-mailbox",
    })
    assert claimed.status_code == 200, claimed.text
    await client.post("/api/event", json={
        "from_agent": "S", "to_agent": "shared-mailbox", "channel": "#c",
        "kind": "agent_message", "content": "one message", "ts": 1746404783003,
        "client_event_id": "recipient-id-relay-collision",
    })

    response = await client.get("/api/agent-messages/recipients")

    assert response.status_code == 200, response.text
    row = next(
        item for item in response.json()["recipients"]
        if item["to_agent"] == "shared-mailbox"
    )
    assert row["msg_count"] == 1
    assert row["display_name"] == "Relay Name Owner"


async def test_get_agent_messages_recipients_left_join_when_no_status(client):
    """v1.3.1 (D29): a recipient that has agent_message rows but NO
    agent_status row gets display_name=null (metadata fallback semantics)."""
    await client.post("/api/event", json={
        "from_agent": "S", "to_agent": "lj-Y", "channel": "#c",
        "kind": "agent_message", "content": "m1", "ts": 1746404783001,
        "client_event_id": "lj-1",
    })
    r = await client.get("/api/agent-messages/recipients")
    assert r.status_code == 200, r.text
    by_id = {x["to_agent"]: x for x in r.json()["recipients"]}
    assert "lj-y" in by_id
    assert by_id["lj-y"]["display_name"] is None


async def test_get_agent_messages_recipients_empty(client):
    """v1.3.1 (D29): no agent_message rows -> 200 with empty list (NOT 404).
    Idempotent read semantics, mirrors GET /api/agent/{id}/messages
    unknown-recipient behavior."""
    r = await client.get("/api/agent-messages/recipients")
    assert r.status_code == 200, r.text
    assert r.json() == {"recipients": []}


async def test_get_agent_messages_recipients_limit_validation(client):
    """v1.3.1 (D29): limit Query bound is [1, 1000]. Out-of-range -> 422."""
    r0 = await client.get("/api/agent-messages/recipients?limit=0")
    assert r0.status_code == 422
    r_over = await client.get("/api/agent-messages/recipients?limit=1001")
    assert r_over.status_code == 422
    r_ok = await client.get("/api/agent-messages/recipients?limit=500")
    assert r_ok.status_code == 200


async def test_get_agent_messages_recipients_orders_newest_first(client):
    """v1.3.1 (D29): recipients are ordered newest-first by MAX(ts) DESC.
    POST 3 agent_messages to distinct recipients with monotonic ts; the
    response's `last_ts` values must descend across the recipients list."""
    await client.post("/api/event", json={
        "from_agent": "S", "to_agent": "ord-A", "channel": "#c",
        "kind": "agent_message", "content": "first", "ts": 1746404780000,
        "client_event_id": "ord-1",
    })
    await client.post("/api/event", json={
        "from_agent": "S", "to_agent": "ord-B", "channel": "#c",
        "kind": "agent_message", "content": "middle", "ts": 1746404780500,
        "client_event_id": "ord-2",
    })
    await client.post("/api/event", json={
        "from_agent": "S", "to_agent": "ord-C", "channel": "#c",
        "kind": "agent_message", "content": "newest", "ts": 1746404781000,
        "client_event_id": "ord-3",
    })
    r = await client.get("/api/agent-messages/recipients")
    assert r.status_code == 200, r.text
    rcpts = r.json()["recipients"]
    # Filter to just the ones we created in this test
    ours = [x for x in rcpts if x["to_agent"].startswith("ord-")]
    assert len(ours) == 3
    # Newest first after canonical mailbox folding.
    assert ours[0]["to_agent"] == "ord-c"
    assert ours[1]["to_agent"] == "ord-b"
    assert ours[2]["to_agent"] == "ord-a"
    # last_ts strictly descending
    assert ours[0]["last_ts"] > ours[1]["last_ts"] > ours[2]["last_ts"]


# ---------- v1.4 (D30): GET /api/agent-messages aggregate ----------


async def test_get_agent_messages_aggregate_returns_all_recipients(client):
    """v1.4 (D30): aggregate endpoint returns rows across multiple recipients
    in event_id ASC order (insertion order preserved).

    POST 3 to rcpt-X and 2 to rcpt-Y; GET /api/agent-messages; assert all 5
    rows present, ordered by event_id ASC matching POST order, contents preserved.
    """
    posts = [
        ("agg-X-1", "rcpt-X", "x-msg-1"),
        ("agg-X-2", "rcpt-X", "x-msg-2"),
        ("agg-Y-1", "rcpt-Y", "y-msg-1"),
        ("agg-X-3", "rcpt-X", "x-msg-3"),
        ("agg-Y-2", "rcpt-Y", "y-msg-2"),
    ]
    for cei, to_agent, content in posts:
        await client.post("/api/event", json={
            "from_agent": "S", "to_agent": to_agent, "channel": "#c",
            "kind": "agent_message", "content": content, "ts": 1746404783000,
            "client_event_id": cei,
        })
    r = await client.get("/api/agent-messages")
    assert r.status_code == 200, r.text
    body = r.json()
    assert len(body["messages"]) == 5
    assert body["next_cursor"] is None
    contents = [m["content"] for m in body["messages"]]
    assert contents == ["x-msg-1", "x-msg-2", "y-msg-1", "x-msg-3", "y-msg-2"]
    # event_id strictly increasing (ASC ordering)
    eids = [m["event_id"] for m in body["messages"]]
    assert eids == sorted(eids)


async def test_get_agent_messages_aggregate_cursor_pagination(client):
    """v1.4 (D30): cursor pagination — peek-ahead detects more pages and the
    cursor walks the result set across recipients in event_id ASC order.

    POST 5 agent_messages mixed across 2 recipients; GET with limit=2 → 2 rows
    + next_cursor; advance with since → 2 rows + next_cursor; advance → 1 row
    + next_cursor=null.
    """
    for i in range(5):
        to_agent = "agg-rcpt-A" if i % 2 == 0 else "agg-rcpt-B"
        await client.post("/api/event", json={
            "from_agent": "S", "to_agent": to_agent, "channel": "#c",
            "kind": "agent_message", "content": f"agg-cur-{i}", "ts": 1746404783000 + i,
            "client_event_id": f"agg-cur-{i}",
        })
    r1 = await client.get("/api/agent-messages?limit=2")
    body1 = r1.json()
    assert len(body1["messages"]) == 2
    assert body1["next_cursor"] is not None
    r2 = await client.get(f"/api/agent-messages?since={body1['next_cursor']}&limit=2")
    body2 = r2.json()
    assert len(body2["messages"]) == 2
    assert body2["next_cursor"] is not None
    r3 = await client.get(f"/api/agent-messages?since={body2['next_cursor']}&limit=2")
    body3 = r3.json()
    assert len(body3["messages"]) == 1
    assert body3["next_cursor"] is None


async def test_get_agent_messages_aggregate_kind_filter(client):
    """v1.4 (D30): kind filter — only rows of the specified kind returned.

    POST 2 agent_message + 2 handoff (all to same recipient). GET defaults to
    kind=agent_message; assert only the 2 agent_message rows surface.
    """
    await client.post("/api/event", json={
        "from_agent": "S", "to_agent": "agg-kf-Z", "channel": "#c",
        "kind": "agent_message", "content": "am-1", "ts": 1746404783001,
        "client_event_id": "agg-kf-1",
    })
    await client.post("/api/event", json={
        "from_agent": "S", "to_agent": "agg-kf-Z", "channel": "#c",
        "kind": "agent_message", "content": "am-2", "ts": 1746404783002,
        "client_event_id": "agg-kf-2",
    })
    await client.post("/api/event", json={
        "from_agent": "S", "to_agent": "agg-kf-Z", "channel": "#c",
        "kind": "handoff", "content": "ho-1", "ts": 1746404783003,
        "client_event_id": "agg-kf-3",
    })
    await client.post("/api/event", json={
        "from_agent": "S", "to_agent": "agg-kf-Z", "channel": "#c",
        "kind": "handoff", "content": "ho-2", "ts": 1746404783004,
        "client_event_id": "agg-kf-4",
    })
    r = await client.get("/api/agent-messages")
    assert r.status_code == 200, r.text
    body = r.json()
    assert len(body["messages"]) == 2
    assert all(m["kind"] == "agent_message" for m in body["messages"])
    contents = sorted(m["content"] for m in body["messages"])
    assert contents == ["am-1", "am-2"]


async def test_get_agent_messages_aggregate_excludes_null_to_agent(client):
    """v1.4 (D30): aggregate endpoint requires `to_agent IS NOT NULL` per the
    partial index — non-agent_message rows with null to_agent must NOT surface
    when callers query other kinds; agent_message kind itself rejects null
    to_agent at POST time (per v1.3 § 8 broker validator), so the partial
    WHERE is informational for the agent_message default.

    POST 1 status row with null to_agent (legal for non-agent_message kinds);
    POST 1 agent_message row with non-null to_agent. GET with the default kind
    returns only the agent_message row. GET with kind=status returns 0 rows
    because the status row has null to_agent (excluded by partial WHERE).
    """
    await client.post("/api/event", json={
        "from_agent": "S", "channel": "#c",
        "kind": "status", "content": "broker-self-status",
        "ts": 1746404783001, "client_event_id": "agg-na-status",
    })
    await client.post("/api/event", json={
        "from_agent": "S", "to_agent": "agg-na-rcpt", "channel": "#c",
        "kind": "agent_message", "content": "am-good",
        "ts": 1746404783002, "client_event_id": "agg-na-am",
    })
    # Default kind=agent_message: only the 1 agent_message row.
    r = await client.get("/api/agent-messages")
    assert r.status_code == 200, r.text
    body = r.json()
    assert len(body["messages"]) == 1
    assert body["messages"][0]["content"] == "am-good"
    assert body["messages"][0]["to_agent"] == "agg-na-rcpt"
    # kind=status: the lone status row has null to_agent and is excluded.
    r2 = await client.get("/api/agent-messages?kind=status")
    assert r2.status_code == 200
    assert r2.json()["messages"] == []


async def test_get_agent_messages_aggregate_empty_returns_200_not_404(client):
    """v1.4 (D30): empty broker returns 200 with empty list (NOT 404).

    Idempotent read semantics — aggregate is conceptually queryable on any
    broker including never-targeted ones. Mirrors GET /api/agent/{id}/messages
    unknown-recipient behavior per D28.
    """
    r = await client.get("/api/agent-messages")
    assert r.status_code == 200
    assert r.json() == {"messages": [], "next_cursor": None}


async def test_get_agent_messages_aggregate_since_overflow_rejected(client):
    """v1.4 (D30): `since` > i64 max returns 422 via FastAPI Query bound,
    not 500 via aiosqlite INTEGER overflow.

    Mirrors test_get_agent_messages_since_overflow_rejected (M2 iter-2).
    """
    r = await client.get(f"/api/agent-messages?since={2**63}")
    assert r.status_code == 422
    assert "since" in r.text.lower()


async def test_get_agent_messages_aggregate_limit_out_of_bounds_rejected(client):
    """v1.4 (D30): limit < 1 or > 200 returns 422 via Query validator.

    Cap is 200 (vs per-recipient inbox cap 100) — aggregate fans across
    recipients so per-fetch yield is naturally higher per D30 ADR. Both
    extremes are rejected at the FastAPI layer; no silent clamp.
    """
    r0 = await client.get("/api/agent-messages?limit=0")
    assert r0.status_code == 422
    r_over = await client.get("/api/agent-messages?limit=201")
    assert r_over.status_code == 422
    r_ok = await client.get("/api/agent-messages?limit=200")
    assert r_ok.status_code == 200


async def test_get_agent_messages_aggregate_limit_200_returns_full_page(client):
    """v1.4 D30 iter-2 #4a: cap-200 boundary — POST 250 rows, assert limit=200
    returns exactly 200 + non-null cursor (proves the cap-200 page returns the
    full 200 and signals there are more rows available)."""
    for i in range(250):
        await client.post("/api/event", json={
            "from_agent": "S",
            "to_agent": f"rcpt-{i:03d}",
            "channel": "#c",
            "kind": "agent_message",
            "content": f"msg-{i}",
            "ts": 1746404783000 + i,
            "client_event_id": f"agg-200-{i:03d}",
        })
    r = await client.get("/api/agent-messages?limit=200")
    assert r.status_code == 200, r.text
    body = r.json()
    assert len(body["messages"]) == 200
    # 250 rows exist; 200 returned; 50 more available => non-null cursor.
    assert body["next_cursor"] is not None


async def test_get_agent_messages_aggregate_uses_partial_index(client, db_path):
    """v1.4 D30 iter-2 #4b: regression guard — confirm partial composite index
    `idx_message_log_kind_event_id_to_agent` is used by the SQLite query planner
    for the aggregate query. EXPLAIN QUERY PLAN should reference the index by name.
    """
    # Seed at least one row so the table has stats; query plan is index-driven
    # regardless but this matches the canonical "ensure DB exists" pattern.
    await client.post("/api/event", json={
        "from_agent": "S",
        "to_agent": "rcpt-X",
        "channel": "#c",
        "kind": "agent_message",
        "content": "msg",
        "ts": 1746404783000,
        "client_event_id": "agg-partial-idx-1",
    })
    import aiosqlite
    async with aiosqlite.connect(db_path) as conn:
        cur = await conn.execute(
            "EXPLAIN QUERY PLAN "
            "SELECT event_id FROM message_log "
            "WHERE event_id > ? AND kind = ? AND to_agent IS NOT NULL "
            "ORDER BY event_id ASC LIMIT ?",
            (0, "agent_message", 50),
        )
        plan = await cur.fetchall()
        await cur.close()
    plan_str = " ".join(str(row) for row in plan).lower()
    assert "idx_message_log_kind_event_id_to_agent" in plan_str, (
        f"Aggregate query did not use partial index. Plan: {plan_str}"
    )


async def test_get_agent_messages_aggregate_status_kind_with_to_agent(client):
    """v1.4 D30 iter-2 #8: positive companion to test #4 from iter-1 — kind=status
    WITH non-null to_agent IS returned (proves the partial-WHERE doesn't drop
    legitimate rows; complements test_get_agent_messages_aggregate_excludes_null_to_agent
    which covers the negative side)."""
    # POST 1 status row WITH to_agent (legitimate non-default kind with recipient).
    await client.post("/api/event", json={
        "from_agent": "S",
        "to_agent": "rcpt-Y",
        "channel": "#c",
        "kind": "status",
        "content": "status-msg",
        "ts": 1746404783100,
        "client_event_id": "agg-status-with-to-1",
    })
    # POST 1 status row WITHOUT to_agent (legal for status kind; broadcast).
    await client.post("/api/event", json={
        "from_agent": "S",
        "channel": "#c",
        "kind": "status",
        "content": "status-broadcast",
        "ts": 1746404783101,
        "client_event_id": "agg-status-with-to-2",
    })
    # GET kind=status; expect ONLY the to_agent='rcpt-Y' row.
    r = await client.get("/api/agent-messages?kind=status")
    assert r.status_code == 200, r.text
    body = r.json()
    assert len(body["messages"]) == 1
    assert body["messages"][0]["to_agent"] == "rcpt-y"
    assert body["messages"][0]["content"] == "status-msg"


# ---------- v1.7 (D38, Wave 6 Track 4): universal error envelope ----------

async def test_post_event_error_envelope_accepted(client):
    """v1.7: valid envelope {type, severity, errors[], context} accepted."""
    import json as _json
    envelope = {
        "type": "subagent_contract_violation",
        "severity": "warn",
        "errors": [
            {"kind": "missing_state_db_row", "detail": "stage 3 silent skip", "auto_recovered": True},
            {"kind": "malformed_envelope", "detail": "JSON parse fail", "auto_recovered": False},
        ],
        "context": {"pipeline_id": "wf-test-123", "stage_index": 3, "teammate_name": "sub-coder"},
    }
    r = await client.post("/api/event", json={
        "from_agent": "test-producer",
        "to_agent": "test-orchestrator-sid",
        "channel": "#test-v17",
        "kind": "error",
        "content": _json.dumps(envelope),
        "ts": 1780000000000,
        "client_event_id": "v17-test-envelope-1",
    })
    assert r.status_code == 200, r.text
    assert "event_id" in r.json()


async def test_post_event_error_envelope_all_severities_accepted(client):
    """v1.7: each of the 4 closed-enum severities accepted (info, warn, error, critical)."""
    import json as _json
    for i, sev in enumerate(["info", "warn", "error", "critical"]):
        envelope = {
            "type": "test_severity",
            "severity": sev,
            "errors": [{"kind": "test", "detail": "sev test", "auto_recovered": False}],
            "context": {},
        }
        r = await client.post("/api/event", json={
            "from_agent": "sev-test",
            "to_agent": "rcpt",
            "channel": "#test",
            "kind": "error",
            "content": _json.dumps(envelope),
            "ts": 1780000000000 + i,
            "client_event_id": "v17-sev-" + sev,
        })
        assert r.status_code == 200, "severity " + sev + " rejected: " + r.text


async def test_post_event_error_envelope_strict_rejects_bad_severity(client):
    """v1.7.1 STRICT: bad severity now 422 (was warn-accept 200 in v1.7)."""
    import json as _json
    envelope = {"type": "test_bad_sev", "severity": "WHATEVER", "errors": []}
    r = await client.post("/api/event", json={
        "from_agent": "bad-sev-test",
        "to_agent": "rcpt",
        "channel": "#test",
        "kind": "error",
        "content": _json.dumps(envelope),
        "ts": 1780000000100,
        "client_event_id": "v17-test-bad-sev-strict",
    })
    assert r.status_code == 422, r.text


async def test_post_event_error_strict_rejects_legacy_string_content(client):
    """v1.7.1 STRICT: legacy pre-v1.7 string content now 422 (was warn-accept 200 in v1.7)."""
    r = await client.post("/api/event", json={
        "from_agent": "legacy-producer",
        "to_agent": "rcpt",
        "channel": "#test",
        "kind": "error",
        "content": "catchup_truncated=true; pre-v1.7 legacy string format",
        "ts": 1780000000200,
        "client_event_id": "v17-test-legacy-strict",
    })
    assert r.status_code == 422, r.text


async def test_post_event_error_envelope_strict_rejects_missing_type(client):
    """v1.7.1 STRICT: envelope missing required `type` field now 422 (was warn-accept 200 in v1.7)."""
    import json as _json
    bad = {"severity": "warn", "errors": []}
    r = await client.post("/api/event", json={
        "from_agent": "test",
        "to_agent": "rcpt",
        "channel": "#test",
        "kind": "error",
        "content": _json.dumps(bad),
        "ts": 1780000000300,
        "client_event_id": "v17-test-no-type-strict",
    })
    assert r.status_code == 422, r.text


async def test_post_event_error_envelope_strict_rejects_errors_not_a_list(client):
    """v1.7.1 STRICT: `errors` field as non-list now 422 (was warn-accept 200 in v1.7)."""
    import json as _json
    bad = {"type": "t", "severity": "warn", "errors": "should-be-list-not-string"}
    r = await client.post("/api/event", json={
        "from_agent": "test",
        "to_agent": "rcpt",
        "channel": "#test",
        "kind": "error",
        "content": _json.dumps(bad),
        "ts": 1780000000400,
        "client_event_id": "v17-test-errors-not-list-strict",
    })
    assert r.status_code == 422, r.text


async def test_post_event_error_envelope_roundtrip_via_inbox(client):
    """v1.7: envelope content round-trips byte-for-byte via inbox GET (broker is opaque)."""
    import json as _json
    envelope = {
        "type": "discord_catchup_truncated",
        "severity": "warn",
        "errors": [{"kind": "catchup_window_exceeded", "detail": "100msg gap", "auto_recovered": False}],
        "context": {"channel_id": "12345", "thread_id": None, "catchup_window_ms": 600000},
    }
    content_str = _json.dumps(envelope)
    r = await client.post("/api/event", json={
        "from_agent": "discord_poller",
        "to_agent": "test-roundtrip-rcpt",
        "channel": "#cc-discord",
        "kind": "error",
        "content": content_str,
        "ts": 1780000000500,
        "client_event_id": "v17-roundtrip",
    })
    assert r.status_code == 200
    r2 = await client.get("/api/agent/test-roundtrip-rcpt/messages?kind=error")
    assert r2.status_code == 200
    body = r2.json()
    msg = next(m for m in body["messages"] if m.get("client_event_id") == "v17-roundtrip")
    assert msg["content"] == content_str
    parsed = _json.loads(msg["content"])
    assert parsed["type"] == "discord_catchup_truncated"
    assert parsed["context"]["channel_id"] == "12345"


async def test_post_event_error_envelope_severity_msg_does_not_echo_token(client):
    """v1.7.1 F1 token-leak safety: bad-severity value must not be echoed in response body even though now 422."""
    import json as _json
    sentinel = "sk-ant-evil-token-shaped-severity"
    bad = {"type": "t", "severity": sentinel, "errors": []}
    r = await client.post("/api/event", json={
        "from_agent": "f1-test",
        "to_agent": "rcpt",
        "channel": "#test",
        "kind": "error",
        "content": _json.dumps(bad),
        "ts": 1780000000600,
        "client_event_id": "v17-test-f1-strict",
    })
    # v1.7.1 strict: now 422 instead of 200 (still token-leak-safe)
    assert r.status_code == 422
    assert sentinel not in r.text


async def test_post_event_error_envelope_does_not_affect_non_error_kinds(client):
    """v1.7: envelope validator is gated on kind='error'; other kinds pass through unchanged."""
    r = await client.post("/api/event", json={
        "from_agent": "test-am",
        "to_agent": "rcpt-am",
        "channel": "#test",
        "kind": "agent_message",
        "content": "any string here, not an envelope",
        "ts": 1780000000700,
        "client_event_id": "v17-test-am-passthrough",
    })
    assert r.status_code == 200, r.text


async def test_post_event_error_envelope_context_is_opaque(client):
    """v1.7: context field is free-form per design; any JSON structure accepted."""
    import json as _json
    envelope = {
        "type": "test_context",
        "severity": "info",
        "errors": [{"kind": "k", "detail": "d", "auto_recovered": True}],
        "context": {
            "nested": {"deep": {"value": [1, 2, 3]}},
            "null_ok": None,
            "unicode_ascii": "ascii-safe-test",
        },
    }
    r = await client.post("/api/event", json={
        "from_agent": "ctx-test",
        "to_agent": "rcpt",
        "channel": "#test",
        "kind": "error",
        "content": _json.dumps(envelope),
        "ts": 1780000000800,
        "client_event_id": "v17-test-ctx-opaque",
    })
    assert r.status_code == 200, r.text


# ---------- OMP R1 broker contract ----------

def _control_token(seed: int = 1) -> tuple[str, str]:
    raw = bytes([seed]) * 32
    token = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
    return token, hashlib.sha256(raw).hexdigest()


async def _bind_omp(
    client,
    recipient_id: str,
    *,
    token_seed: int = 1,
    expected_binding_epoch=None,
    bind_attempt_id=None,
    new_binding_epoch=None,
    process_epoch=None,
    instance_id=None,
    control_epoch=None,
):
    token, token_hash = _control_token(token_seed)
    body = {
        "schema_version": 1,
        "transport": "omp-mcp",
        "kind": "omp-session",
        "channel": "#omp-relay",
        "thread_id": None,
        "status": "idle",
        "current_task": None,
        "last_activity_ts": 1780001000000,
        "display_name": None,
        "operation": "bind",
        "bind_attempt_id": bind_attempt_id or str(uuid.uuid4()),
        "expected_binding_epoch": expected_binding_epoch,
        "new_binding_epoch": new_binding_epoch or str(uuid.uuid4()),
        "omp_process_epoch": process_epoch or str(uuid.uuid4()),
        "instance_id": instance_id or str(uuid.uuid4()),
        "control_epoch": control_epoch or str(uuid.uuid4()),
        "control_token_hash": token_hash,
    }
    response = await client.post(f"/api/agent/{recipient_id}/status", json=body)
    return response, body, token


def _control_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"RelayControl {token}"}


async def _omp_name(client, recipient_id, bind, token, relay_name):
    return await client.put(
        f"/api/agent/{recipient_id}/relay-name",
        headers=_control_headers(token),
        json={
            "schema_version": 1,
            "relay_name": relay_name,
            "takeover": False,
            "binding_epoch": bind["new_binding_epoch"],
            "omp_process_epoch": bind["omp_process_epoch"],
        },
    )


async def _claim_offer(client, recipient_id, bind, token):
    return await client.post(
        f"/api/agent/{recipient_id}/delivery-offer",
        headers=_control_headers(token),
        json={
            "schema_version": 1,
            "binding_epoch": bind["new_binding_epoch"],
            "omp_process_epoch": bind["omp_process_epoch"],
        },
    )


async def test_omp_bind_duplicate_transport_is_closed_and_has_no_side_effect(client):
    agent_id = str(uuid.uuid4())
    response = await client.post(
        f"/api/agent/{agent_id}/status",
        headers={"Content-Type": "application/json"},
        content=(
            b'{"transport":"omp-mcp","transport":"codex-app-server",'
            b'"operation":"bind"}'
        ),
    )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "PROTOCOL_INVALID"
    conn = sqlite3.connect(os.environ["BROKER_DB_PATH"])
    try:
        stored = conn.execute(
            "SELECT COUNT(*) FROM agent_status WHERE id=?", (agent_id,)
        ).fetchone()[0]
    finally:
        conn.close()
    assert stored == 0


async def test_omp_health_capability_requires_operator_configuration(client, monkeypatch):
    monkeypatch.delenv("BROKER_OPERATOR_TOKEN", raising=False)
    monkeypatch.delenv("BROKER_OPERATOR_ID", raising=False)
    unavailable = await client.get("/api/health")
    assert unavailable.json()["checks"]["omp_relay_contract"] == 0
    monkeypatch.setenv("BROKER_OPERATOR_TOKEN", "health-secret")
    monkeypatch.setenv("BROKER_OPERATOR_ID", "health-operator")
    available = await client.get("/api/health")
    assert available.json()["checks"]["omp_relay_contract"] == 1
    monkeypatch.setenv("BROKER_OPERATOR_TOKEN", "health-tökén")
    invalid_config = await client.get("/api/health")
    assert invalid_config.json()["checks"]["omp_relay_contract"] == 0
    unavailable_route = await client.post(
        "/api/operator/delivery/1/reject-unbound",
        headers={"Authorization": "Bearer health-secret"},
        content=b"{}",
    )
    assert unavailable_route.status_code == 503
    assert unavailable_route.json()["error"]["code"] == "BROKER_CAPABILITY_MISSING"
    monkeypatch.setenv("BROKER_OPERATOR_TOKEN", "health-secret")
    restored = await client.get("/api/health")
    assert restored.json()["checks"]["omp_relay_contract"] == 1

    conn = sqlite3.connect(os.environ["BROKER_DB_PATH"])
    conn.executescript(
        "DROP TRIGGER trg_message_log_omp_bounds_r1;"
        "CREATE TRIGGER trg_message_log_omp_bounds_r1 "
        "BEFORE INSERT ON message_log BEGIN SELECT 1; END;"
    )
    conn.close()
    tampered = await client.get("/api/health")
    assert tampered.json()["checks"]["omp_relay_contract"] == 0

    from broker import server
    await server.ensure_schema()
    repaired = await client.get("/api/health")
    assert repaired.json()["checks"]["omp_relay_contract"] == 1
    conn = sqlite3.connect(os.environ["BROKER_DB_PATH"])
    conn.execute("PRAGMA user_version=6")
    conn.close()
    future = await client.get("/api/health")
    assert future.json()["checks"]["omp_relay_contract"] == 0


async def test_omp_health_capability_rejects_stored_invariant_contradictions(
    client, monkeypatch
):
    monkeypatch.setenv("BROKER_OPERATOR_TOKEN", "health-invariant-secret")
    monkeypatch.setenv("BROKER_OPERATOR_ID", "health-invariant-operator")
    recipient_id = str(uuid.uuid4())
    _, _binding, _token = await _bind_omp(client, recipient_id, token_seed=26)
    sent = await client.post("/api/event", json={
        "from_agent": "sender", "to_agent": recipient_id, "channel": "#c",
        "kind": "agent_message", "content": "health invariant",
        "ts": 1780001000001,
    })
    assert sent.status_code == 200
    legacy_id = f"health-legacy-{uuid.uuid4()}"
    assert (await client.post(f"/api/agent/{legacy_id}/status", json={
        "kind": "demo_research", "channel": "#demo", "status": "idle",
        "last_activity_ts": 1780001000002,
    })).status_code == 200
    assert (await client.get("/api/health")).json()["checks"]["omp_relay_contract"] == 1

    db_path = os.environ["BROKER_DB_PATH"]

    def mutate_without_trigger(
        trigger_name: str, sql: str, params: tuple = ()
    ) -> None:
        conn = sqlite3.connect(db_path)
        try:
            trigger_sql = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",
                (trigger_name,),
            ).fetchone()[0]
            conn.execute(f'DROP TRIGGER "{trigger_name}"')
            conn.execute(sql, params)
            conn.execute(trigger_sql)
            conn.commit()
        finally:
            conn.close()

    event_id = sent.json()["event_id"]
    mutate_without_trigger(
        "trg_delivery_omp_ack_guard_r1",
        "UPDATE delivery SET omp_ack_seq=2 WHERE event_id=?",
        (event_id,),
    )
    assert (await client.get("/api/health")).json()["checks"]["omp_relay_contract"] == 0
    mutate_without_trigger(
        "trg_delivery_omp_ack_guard_r1",
        "UPDATE delivery SET omp_ack_seq=0 WHERE event_id=?",
        (event_id,),
    )
    assert (await client.get("/api/health")).json()["checks"]["omp_relay_contract"] == 1

    orphan_recipient = str(uuid.uuid4())
    orphan_event = event_id + 1_000_000
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO delivery_offer "
            "(recipient_id,event_id,offer_token,state,reason,omp_process_epoch,"
            "created_ts,updated_ts) VALUES (?,?,?,'offered',NULL,NULL,1,1)",
            (orphan_recipient, orphan_event, str(uuid.uuid4())),
        )
        conn.commit()
    finally:
        conn.close()
    assert (await client.get("/api/health")).json()["checks"]["omp_relay_contract"] == 0
    mutate_without_trigger(
        "trg_delivery_offer_delete_guard_r1",
        "DELETE FROM delivery_offer WHERE recipient_id=? AND event_id=?",
        (orphan_recipient, orphan_event),
    )
    assert (await client.get("/api/health")).json()["checks"]["omp_relay_contract"] == 1

    mutation_trigger = "trg_agent_omp_binding_update_r1"
    mutate_without_trigger(
        mutation_trigger,
        "UPDATE agent_status SET omp_process_epoch=? WHERE id=?",
        (str(uuid.uuid4()), legacy_id),
    )
    assert (await client.get("/api/health")).json()["checks"]["omp_relay_contract"] == 0
    mutate_without_trigger(
        mutation_trigger,
        "UPDATE agent_status SET omp_process_epoch=NULL WHERE id=?",
        (legacy_id,),
    )
    assert (await client.get("/api/health")).json()["checks"]["omp_relay_contract"] == 1


async def test_relay_control_bypasses_deployment_bearer_only_for_omp_variant(
    client, monkeypatch
):
    legacy_id = str(uuid.uuid4())
    assert (await client.post(f"/api/agent/{legacy_id}/status", json={
        "kind": "cc-session", "channel": "#cc-relay", "status": "idle",
        "last_activity_ts": 1780001000000, "display_name": "auth.legacy",
    })).status_code == 200
    omp_id = str(uuid.uuid4())
    _, binding, token = await _bind_omp(client, omp_id)

    monkeypatch.setenv("BROKER_HOST", "0.0.0.0")
    monkeypatch.setenv("BROKER_BEARER_TOKEN", "deployment-secret")
    malformed_shared_routes = [
        ("put", f"/api/agent/{omp_id}/relay-name"),
        ("post", f"/api/agent/{omp_id}/delivery-ack"),
    ]
    for method, route in malformed_shared_routes:
        request = getattr(client, method)
        missing = await request(route, json=[])
        assert missing.status_code == 401, missing.text
        assert missing.json()["error"]["code"] == "CONTROL_REQUIRED"
        invalid = await request(
            route, headers={"Authorization": "RelayControl invalid"}, json={}
        )
        assert invalid.status_code == 403, invalid.text
        assert invalid.json()["error"]["code"] == "CONTROL_INVALID"

    malformed_refresh = await client.post(
        f"/api/agent/{omp_id}/status",
        json={"transport": "omp-mcp", "operation": "refresh"},
    )
    assert malformed_refresh.status_code == 401, malformed_refresh.text
    assert malformed_refresh.json()["error"]["code"] == "CONTROL_REQUIRED"

    post_bearer_id = str(uuid.uuid4())
    bound_after_bearer, _, _ = await _bind_omp(client, post_bearer_id, token_seed=14)
    assert bound_after_bearer.status_code == 200, bound_after_bearer.text
    leaked = await client.get(
        f"/api/agent/{legacy_id}/status",
        headers={"Authorization": "RelayControl invalid"},
    )
    assert leaked.status_code == 401
    assert leaked.json()["error"]["code"] == "missing_bearer"
    admitted = await client.get(
        f"/api/agent/{legacy_id}/status",
        headers={"Authorization": "Bearer deployment-secret"},
    )
    assert admitted.status_code == 200
    assert (await _omp_name(
        client, omp_id, binding, token, "omp.private-auth"
    )).status_code == 200



async def test_omp_bind_attempt_history_lost_response_conflict_and_aba(client):
    recipient_id = str(uuid.uuid4())
    first, body1, first_token = await _bind_omp(client, recipient_id)
    assert first.status_code == 200, first.text
    assert isinstance(first.json()["event_id"], str)

    replay = await client.post(f"/api/agent/{recipient_id}/status", json=body1)
    assert replay.status_code == 200
    assert replay.json() == first.json()

    changed = dict(body1, display_name="changed")
    conflict = await client.post(f"/api/agent/{recipient_id}/status", json=changed)
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "ATTEMPT_CONFLICT"

    refresh = await client.post(
        f"/api/agent/{recipient_id}/status",
        headers=_control_headers(first_token),
        json={
            "schema_version": 1,
            "transport": "omp-mcp",
            "kind": "omp-session",
            "channel": "#omp-relay",
            "thread_id": None,
            "status": "running",
            "current_task": "before rebind",
            "last_activity_ts": 1780001000001,
            "display_name": None,
            "operation": "refresh",
            "binding_epoch": body1["new_binding_epoch"],
            "omp_process_epoch": body1["omp_process_epoch"],
            "request_id": str(uuid.uuid4()),
        },
    )
    assert refresh.status_code == 200, refresh.text

    second, body2, _ = await _bind_omp(
        client, recipient_id, token_seed=2,
        expected_binding_epoch=body1["new_binding_epoch"],
    )
    assert second.status_code == 200, second.text
    conn = sqlite3.connect(os.environ["BROKER_DB_PATH"])
    try:
        event_content = conn.execute(
            "SELECT content FROM message_log WHERE event_id=?",
            (int(second.json()["event_id"]),),
        ).fetchone()[0]
    finally:
        conn.close()
    assert json.loads(event_content)["old_status"] == "running"

    stale_replay = await client.post(f"/api/agent/{recipient_id}/status", json=body1)
    assert stale_replay.status_code == 409
    assert stale_replay.json()["error"]["code"] == "BINDING_CONFLICT"

    reused_epoch, _, _ = await _bind_omp(
        client, recipient_id, token_seed=3,
        expected_binding_epoch=body2["new_binding_epoch"],
        new_binding_epoch=body1["new_binding_epoch"],
    )
    assert reused_epoch.status_code == 409
    assert reused_epoch.json()["error"]["code"] == "ATTEMPT_CONFLICT"


async def test_omp_refused_bind_attempt_replays_durably_without_reserving_epoch(client):
    recipient_id = str(uuid.uuid4())
    first, active, _ = await _bind_omp(client, recipient_id)
    assert first.status_code == 200
    reused_token_attempt = str(uuid.uuid4())
    same_token, same_token_body, _ = await _bind_omp(
        client,
        recipient_id,
        token_seed=1,
        expected_binding_epoch=active["new_binding_epoch"],
        new_binding_epoch=reused_token_attempt,
    )
    assert same_token.status_code == 409
    assert same_token.json()["error"]["code"] == "ATTEMPT_CONFLICT"
    same_token_replay = await client.post(
        f"/api/agent/{recipient_id}/status", json=same_token_body
    )
    assert same_token_replay.json() == same_token.json()
    reusable_epoch = str(uuid.uuid4())
    refused, refused_body, _ = await _bind_omp(
        client,
        recipient_id,
        token_seed=5,
        expected_binding_epoch=str(uuid.uuid4()),
        new_binding_epoch=reusable_epoch,
    )
    assert refused.status_code == 409
    assert refused.json()["error"]["code"] == "BINDING_CONFLICT"

    replay = await client.post(f"/api/agent/{recipient_id}/status", json=refused_body)
    assert replay.status_code == 409
    assert replay.json() == refused.json()
    changed = await client.post(
        f"/api/agent/{recipient_id}/status",
        json={**refused_body, "display_name": "changed reuse"},
    )
    assert changed.status_code == 409
    assert changed.json()["error"]["code"] == "ATTEMPT_CONFLICT"

    succeeded, current, current_token = await _bind_omp(
        client,
        recipient_id,
        token_seed=6,
        expected_binding_epoch=active["new_binding_epoch"],
        new_binding_epoch=reusable_epoch,
    )
    assert succeeded.status_code == 200, succeeded.text

    refresh = {
        "schema_version": 1,
        "transport": "omp-mcp",
        "kind": "omp-session",
        "channel": "#omp-relay",
        "thread_id": None,
        "status": "running",
        "current_task": "refresh replay",
        "last_activity_ts": 1780001000000,
        "display_name": "refreshed",
        "operation": "refresh",
        "binding_epoch": current["new_binding_epoch"],
        "omp_process_epoch": current["omp_process_epoch"],
        "request_id": str(uuid.uuid4()),
    }
    refreshed = await client.post(
        f"/api/agent/{recipient_id}/status",
        headers=_control_headers(current_token), json=refresh,
    )
    assert refreshed.status_code == 200, refreshed.text
    refresh_replay = await client.post(
        f"/api/agent/{recipient_id}/status",
        headers=_control_headers(current_token), json=refresh,
    )
    assert refresh_replay.json() == refreshed.json()
    refresh_conflict = await client.post(
        f"/api/agent/{recipient_id}/status",
        headers=_control_headers(current_token),
        json={**refresh, "current_task": "changed"},
    )
    assert refresh_conflict.status_code == 409
    assert refresh_conflict.json()["error"]["code"] == "REQUEST_CONFLICT"


async def test_omp_stable_recipient_survives_rename_takeover_and_late_claim(client):
    recipient_id = str(uuid.uuid4())
    bound, binding, token = await _bind_omp(client, recipient_id)
    assert bound.status_code == 200
    assert (await _omp_name(client, recipient_id, binding, token, "omp.one")).status_code == 200

    sent = await client.post("/api/event", json={
        "from_agent": "unknown-sender", "to_agent": "omp.one", "channel": "#c",
        "kind": "agent_message", "content": "before rename", "ts": 1780001000001,
    })
    assert sent.status_code == 200, sent.text
    renamed = await client.put(
        f"/api/agent/{recipient_id}/relay-name", headers=_control_headers(token),
        json={
            "schema_version": 1, "relay_name": "omp.renamed", "takeover": False,
            "binding_epoch": binding["new_binding_epoch"],
            "omp_process_epoch": binding["omp_process_epoch"],
        },
    )
    assert renamed.status_code == 200
    claim = await _claim_offer(client, recipient_id, binding, token)
    assert claim.status_code == 200
    assert claim.json()["delivery"]["to_agent"] == "omp.one"

    late = await client.post("/api/event", json={
        "from_agent": "s", "to_agent": "unowned.name", "channel": "#c",
        "kind": "agent_message", "content": "nobody owned this", "ts": 1780001000002,
    })
    assert late.status_code == 200
    late_owner = str(uuid.uuid4())
    late_bound, late_binding, late_token = await _bind_omp(client, late_owner, token_seed=4)
    assert late_bound.status_code == 200
    assert (await _omp_name(
        client, late_owner, late_binding, late_token, "unowned.name"
    )).status_code == 200
    assert (await _claim_offer(client, late_owner, late_binding, late_token)).json()["code"] == "EMPTY"


async def test_legacy_mailbox_retains_omp_namespace_across_rename(client):
    omp_id = str(uuid.uuid4())
    _, binding, token = await _bind_omp(client, omp_id)
    assert (await _omp_name(client, omp_id, binding, token, "shared.old")).status_code == 200
    omp_message = await client.post("/api/event", json={
        "from_agent": "sender", "to_agent": "shared.old", "channel": "#c",
        "kind": "agent_message", "content": "OMP-owned", "ts": 1780001000003,
    })
    assert omp_message.status_code == 200
    assert (await _omp_name(client, omp_id, binding, token, "omp.moved")).status_code == 200

    retained_message = await client.post("/api/event", json={
        "from_agent": "sender", "to_agent": "shared.old", "channel": "#c",
        "kind": "agent_message", "content": "retained OMP", "ts": 1780001000004,
    })
    assert retained_message.status_code == 200
    stable = await client.get(f"/api/agent/{omp_id}/deliveries")
    assert [row["event_id"] for row in stable.json()["deliveries"]] == [
        omp_message.json()["event_id"], retained_message.json()["event_id"],
    ]
    inherited = await client.get("/api/agent/shared.old/messages")
    assert inherited.status_code == 409
    wrong_ack = await client.post(
        "/api/agent/shared.old/ack",
        json={"event_ids": [retained_message.json()["event_id"]]},
    )
    assert wrong_ack.status_code == 409

    claude_id = str(uuid.uuid4())
    registered = await client.post(f"/api/agent/{claude_id}/status", json={
        "kind": "cc-session", "channel": "#cc-relay", "status": "idle",
        "last_activity_ts": 1780001000005, "display_name": "shared.old",
    })
    assert registered.status_code == 200
    claude_status = await client.get(f"/api/agent/{claude_id}/status")
    assert claude_status.json()["relay_name"] is None

    legacy_takeover = await client.put(
        f"/api/agent/{claude_id}/relay-name",
        json={"relay_name": "shared.old", "takeover": True},
    )
    assert legacy_takeover.status_code == 409

    other_id = str(uuid.uuid4())
    _, other_binding, other_token = await _bind_omp(client, other_id, token_seed=8)
    retained = await _omp_name(
        client, other_id, other_binding, other_token, "shared.old"
    )
    assert retained.status_code == 409
    assert retained.json()["error"]["code"] == "IDENTITY_CONFLICT"

    transferred = await client.put(
        f"/api/agent/{other_id}/relay-name",
        headers=_control_headers(other_token),
        json={
            "schema_version": 1,
            "relay_name": "shared.old",
            "takeover": True,
            "binding_epoch": other_binding["new_binding_epoch"],
            "omp_process_epoch": other_binding["omp_process_epoch"],
        },
    )
    assert transferred.status_code == 200, transferred.text
    conn = sqlite3.connect(os.environ["BROKER_DB_PATH"])
    try:
        retained_recipients = conn.execute(
            "SELECT recipient_id FROM delivery WHERE event_id IN (?,?) "
            "ORDER BY event_id",
            (omp_message.json()["event_id"], retained_message.json()["event_id"]),
        ).fetchall()
    finally:
        conn.close()
    assert retained_recipients == [(omp_id,), (omp_id,)]
    transferred_message = await client.post("/api/event", json={
        "from_agent": "sender", "to_agent": "shared.old", "channel": "#c",
        "kind": "agent_message", "content": "new owner", "ts": 1780001000005,
    })
    assert transferred_message.status_code == 200
    other_inbox = await client.get(f"/api/agent/{other_id}/deliveries")
    assert [row["event_id"] for row in other_inbox.json()["deliveries"]] == [
        transferred_message.json()["event_id"]
    ]

    reclaimed = await client.put(
        f"/api/agent/{omp_id}/relay-name",
        headers=_control_headers(token),
        json={
            "schema_version": 1,
            "relay_name": "shared.old",
            "takeover": True,
            "binding_epoch": binding["new_binding_epoch"],
            "omp_process_epoch": binding["omp_process_epoch"],
        },
    )
    assert reclaimed.status_code == 200
    assert (await _omp_name(
        client, omp_id, binding, token, "omp.moved"
    )).status_code == 200

    unknown = await client.post("/api/event", json={
        "from_agent": "sender", "to_agent": "late.claude", "channel": "#c",
        "kind": "agent_message", "content": "unbound", "ts": 1780001000006,
    })
    assert unknown.status_code == 200
    assert len((await client.get(
        "/api/agent/late.claude/messages"
    )).json()["messages"]) == 1
    late_id = str(uuid.uuid4())
    assert (await client.post(f"/api/agent/{late_id}/status", json={
        "kind": "cc-session", "channel": "#cc-relay", "status": "idle",
        "last_activity_ts": 1780001000007, "display_name": "late.claude",
    })).status_code == 200
    assert (await client.get(
        "/api/agent/late.claude/messages"
    )).json()["messages"] == []

    id_only = "friendly-no-relay"
    assert (await client.post(f"/api/agent/{id_only}/status", json={
        "kind": "demo_research", "channel": "#demo", "status": "idle",
        "last_activity_ts": 1780001000008,
    })).status_code == 200
    id_only_message = await client.post("/api/event", json={
        "from_agent": "sender", "to_agent": id_only, "channel": "#c",
        "kind": "agent_message", "content": "id-only", "ts": 1780001000009,
    })
    assert id_only_message.status_code == 200
    id_only_inbox = await client.get(f"/api/agent/{id_only}/messages")
    assert [row["content"] for row in id_only_inbox.json()["messages"]] == ["id-only"]
    id_only_ack = await client.post(
        f"/api/agent/{id_only}/ack",
        json={"event_ids": [id_only_message.json()["event_id"]]},
    )
    assert id_only_ack.json()["acked_now"] == 1


async def test_legacy_mailbox_prefers_named_holder_over_exact_id_collision(client):
    relay_name = "legacy-name-id-collision"
    named_owner = str(uuid.uuid4())
    assert (await client.post(f"/api/agent/{named_owner}/status", json={
        "kind": "cc-session", "channel": "#cc-relay", "status": "idle",
        "last_activity_ts": 1780001000010, "display_name": relay_name,
    })).status_code == 200
    assert (await client.post(f"/api/agent/{relay_name}/status", json={
        "kind": "demo_research", "channel": "#demo", "status": "idle",
        "last_activity_ts": 1780001000011,
    })).status_code == 200

    sent = await client.post("/api/event", json={
        "from_agent": "sender", "to_agent": relay_name, "channel": "#c",
        "kind": "agent_message", "content": "named owner wins",
        "ts": 1780001000012,
    })
    assert sent.status_code == 200
    conn = sqlite3.connect(os.environ["BROKER_DB_PATH"])
    try:
        id_only_relay_name = conn.execute(
            "SELECT relay_name FROM agent_status WHERE id=?", (relay_name,)
        ).fetchone()[0]
        recipient_id = conn.execute(
            "SELECT recipient_id FROM delivery WHERE event_id=?",
            (sent.json()["event_id"],),
        ).fetchone()[0]
    finally:
        conn.close()
    assert id_only_relay_name is None
    assert recipient_id == named_owner

    inbox = await client.get(f"/api/agent/{relay_name}/messages")
    assert inbox.status_code == 200
    assert [row["content"] for row in inbox.json()["messages"]] == [
        "named owner wins"
    ]
    acked = await client.post(
        f"/api/agent/{relay_name}/ack",
        json={"event_ids": [sent.json()["event_id"]]},
    )
    assert acked.status_code == 200
    assert acked.json()["acked_now"] == 1


async def test_takeover_exact_id_history_without_fallback_is_closed(client):
    id_only = "exact-id-history"
    assert (await client.post(f"/api/agent/{id_only}/status", json={
        "kind": "demo_research", "channel": "#demo", "status": "idle",
        "last_activity_ts": 1780001000013,
    })).status_code == 200
    sent = await client.post("/api/event", json={
        "from_agent": "sender", "to_agent": id_only, "channel": "#c",
        "kind": "agent_message", "content": "stable exact-ID history",
        "ts": 1780001000014,
    })
    assert sent.status_code == 200

    legacy_claimant = str(uuid.uuid4())
    assert (await client.post(f"/api/agent/{legacy_claimant}/status", json={
        "kind": "cc-session", "channel": "#cc-relay", "status": "idle",
        "last_activity_ts": 1780001000015, "display_name": "legacy.claimant",
    })).status_code == 200
    legacy_takeover = await client.put(
        f"/api/agent/{legacy_claimant}/relay-name",
        json={"relay_name": id_only, "takeover": True},
    )
    assert legacy_takeover.status_code == 409
    assert legacy_takeover.json()["error"]["code"] == "http_409"
    assert legacy_takeover.json()["error"]["message"] == (
        "relay name has no rollback-safe fallback"
    )

    omp_claimant = str(uuid.uuid4())
    _, binding, token = await _bind_omp(client, omp_claimant, token_seed=24)
    omp_takeover = await client.put(
        f"/api/agent/{omp_claimant}/relay-name",
        headers=_control_headers(token),
        json={
            "schema_version": 1,
            "relay_name": id_only,
            "takeover": True,
            "binding_epoch": binding["new_binding_epoch"],
            "omp_process_epoch": binding["omp_process_epoch"],
        },
    )
    assert omp_takeover.status_code == 409
    assert omp_takeover.json()["error"]["code"] == "IDENTITY_CONFLICT"

    conn = sqlite3.connect(os.environ["BROKER_DB_PATH"])
    try:
        stored = conn.execute(
            "SELECT to_agent,recipient_id,status FROM delivery WHERE event_id=?",
            (sent.json()["event_id"],),
        ).fetchone()
        claimant_names = conn.execute(
            "SELECT id,relay_name FROM agent_status WHERE id IN (?,?) ORDER BY id",
            (legacy_claimant, omp_claimant),
        ).fetchall()
        retained = conn.execute(
            "SELECT owner_id FROM omp_relay_namespace WHERE relay_name=?",
            (id_only,),
        ).fetchone()
    finally:
        conn.close()
    assert stored == (id_only, id_only, "outstanding")
    assert dict(claimant_names) == {
        legacy_claimant: "legacy.claimant",
        omp_claimant: None,
    }
    assert retained is None


async def test_concurrent_offer_claims_share_one_durable_token(client):
    recipient_id = str(uuid.uuid4())
    _, binding, token = await _bind_omp(client, recipient_id, token_seed=7)
    assert (await _omp_name(
        client, recipient_id, binding, token, "omp.offer-race"
    )).status_code == 200
    sent = await client.post("/api/event", json={
        "from_agent": "sender", "to_agent": "omp.offer-race", "channel": "#c",
        "kind": "agent_message", "content": "one offer", "ts": 1780001000003,
    })
    assert sent.status_code == 200
    first, second = await asyncio.gather(
        _claim_offer(client, recipient_id, binding, token),
        _claim_offer(client, recipient_id, binding, token),
    )
    assert first.status_code == second.status_code == 200
    assert first.json() == second.json()
    conn = sqlite3.connect(os.environ["BROKER_DB_PATH"])
    try:
        offers = conn.execute(
            "SELECT offer_token FROM delivery_offer WHERE recipient_id=? AND event_id=?",
            (recipient_id, sent.json()["event_id"]),
        ).fetchall()
    finally:
        conn.close()
    assert offers == [(first.json()["status"]["offer_token"],)]


async def test_omp_offer_transitions_ack_and_explicit_status_proof(client):
    recipient_id = str(uuid.uuid4())
    _, binding, token = await _bind_omp(client, recipient_id)
    assert (await _omp_name(client, recipient_id, binding, token, "omp.ack")).status_code == 200

    for index in range(2):
        sent = await client.post("/api/event", json={
            "from_agent": "s", "to_agent": "omp.ack", "channel": "#c",
            "kind": "agent_message", "content": f"m{index}",
            "ts": 1780001000100 + index,
        })
        assert sent.status_code == 200
        claim = await _claim_offer(client, recipient_id, binding, token)
        assert claim.status_code == 200
        status = claim.json()["status"]
        assert isinstance(status["event_id"], str)
        target_sequence = ["accepted"] if index == 0 else ["injected", "accepted"]
        for target in target_sequence:
            update = await client.post(
                f"/api/agent/{recipient_id}/delivery-offer-status",
                headers=_control_headers(token),
                json={
                    "schema_version": 1, "event_id": status["event_id"],
                    "offer_token": status["offer_token"],
                    "binding_epoch": binding["new_binding_epoch"],
                    "omp_process_epoch": binding["omp_process_epoch"],
                    "target_state": target, "reason_code": None,
                },
            )
            assert update.status_code == 200, update.text
        ack_body = {
            "schema_version": 1, "event_id": status["event_id"],
            "offer_token": status["offer_token"],
            "binding_epoch": binding["new_binding_epoch"],
            "omp_process_epoch": binding["omp_process_epoch"],
        }
        ack = await client.post(
            f"/api/agent/{recipient_id}/delivery-ack",
            headers=_control_headers(token), json=ack_body,
        )
        assert ack.status_code == 200
        assert ack.json()["code"] == "ACKED"
        again = await client.post(
            f"/api/agent/{recipient_id}/delivery-ack",
            headers=_control_headers(token), json=ack_body,
        )
        assert again.json()["code"] == "ALREADY_ACKED"
        explicit = await client.get(
            f"/api/agent/{recipient_id}/delivery/{status['event_id']}/status",
            headers=_control_headers(token),
            params={
                "schema_version": "1",
                "binding_epoch": binding["new_binding_epoch"],
                "omp_process_epoch": binding["omp_process_epoch"],
            },
        )
        assert explicit.status_code == 200
        assert explicit.json()["status"]["delivery_status"] == "acked"


async def test_omp_ack_requires_current_credential_epochs_token_and_acceptance(client):
    recipient_id = str(uuid.uuid4())
    _, binding, token = await _bind_omp(client, recipient_id)
    await _omp_name(client, recipient_id, binding, token, "omp.fenced")
    await client.post("/api/event", json={
        "from_agent": "s", "to_agent": "omp.fenced", "channel": "#c",
        "kind": "agent_message", "content": "held", "ts": 1780001000200,
    })
    status = (await _claim_offer(client, recipient_id, binding, token)).json()["status"]
    body = {
        "schema_version": 1, "event_id": status["event_id"],
        "offer_token": status["offer_token"],
        "binding_epoch": binding["new_binding_epoch"],
        "omp_process_epoch": binding["omp_process_epoch"],
    }
    missing = await client.post(f"/api/agent/{recipient_id}/delivery-ack", json=body)
    assert missing.status_code == 401
    assert missing.json()["error"]["code"] == "CONTROL_REQUIRED"
    wrong_token = await client.post(
        f"/api/agent/{recipient_id}/delivery-ack",
        headers=_control_headers(_control_token(9)[0]), json=body,
    )
    assert wrong_token.status_code == 403
    unaccepted = await client.post(
        f"/api/agent/{recipient_id}/delivery-ack",
        headers=_control_headers(token), json=body,
    )
    assert unaccepted.status_code == 409
    assert unaccepted.json()["error"]["code"] == "STATE_CONFLICT"
    mismatched = await client.post(
        f"/api/agent/{recipient_id}/delivery-ack", headers=_control_headers(token),
        json={**body, "offer_token": str(uuid.uuid4())},
    )
    assert mismatched.status_code == 409


async def test_omp_recipient_rejection_normalizes_reason_and_notifies_sender(client):
    sender_id = "stable-sender"
    await client.post(f"/api/agent/{sender_id}/status", json={
        "kind": "codex-session", "channel": "#codex-relay", "status": "idle",
        "last_activity_ts": 1780001000300, "transport": "codex-app-server",
    })
    await client.put(f"/api/agent/{sender_id}/relay-name", json={"relay_name": "sender.one"})
    recipient_id = str(uuid.uuid4())
    _, binding, token = await _bind_omp(client, recipient_id)
    await _omp_name(client, recipient_id, binding, token, "omp.reject")
    await client.post("/api/event", json={
        "from_agent": "Sender.One", "to_agent": "omp.reject", "channel": "#c",
        "kind": "agent_message", "content": "sensitive", "ts": 1780001000301,
    })
    status = (await _claim_offer(client, recipient_id, binding, token)).json()["status"]
    rejected = await client.post(
        f"/api/agent/{recipient_id}/delivery-offer-status",
        headers=_control_headers(token),
        json={
            "schema_version": 1, "event_id": status["event_id"],
            "offer_token": status["offer_token"],
            "binding_epoch": binding["new_binding_epoch"],
            "omp_process_epoch": binding["omp_process_epoch"],
            "target_state": "rejected", "reason_code": "USER_REJECTED",
        },
    )
    assert rejected.status_code == 200, rejected.text
    assert rejected.json()["status"]["reject_reason_code"] == (
        "DELIVERY_UNCERTAIN_CONTENT_MAY_HAVE_BEEN_SEEN"
    )
    sender_inbox = await client.get(f"/api/agent/{sender_id}/deliveries")
    notice = sender_inbox.json()["deliveries"]
    assert len(notice) == 1
    assert "sensitive" not in notice[0]["content"]
    assert status["event_id"] in notice[0]["content"]


async def test_omp_rejection_notice_reaches_and_acks_through_claude_mailbox(client):
    sender_id = str(uuid.uuid4())
    registered = await client.post(f"/api/agent/{sender_id}/status", json={
        "kind": "cc-session", "channel": "#cc-relay", "status": "idle",
        "last_activity_ts": 1780001000310, "display_name": "claude.sender",
    })
    assert registered.status_code == 200
    recipient_id = str(uuid.uuid4())
    _, binding, token = await _bind_omp(client, recipient_id)
    assert (await _omp_name(
        client, recipient_id, binding, token, "omp.claude-reject"
    )).status_code == 200
    sent = await client.post("/api/event", json={
        "from_agent": "Claude.Sender", "to_agent": "omp.claude-reject",
        "channel": "#c", "kind": "agent_message", "content": "held",
        "ts": 1780001000311,
    })
    assert sent.status_code == 200
    offer = (await _claim_offer(client, recipient_id, binding, token)).json()["status"]
    rejected = await client.post(
        f"/api/agent/{recipient_id}/delivery-offer-status",
        headers=_control_headers(token),
        json={
            "schema_version": 1, "event_id": offer["event_id"],
            "offer_token": offer["offer_token"],
            "binding_epoch": binding["new_binding_epoch"],
            "omp_process_epoch": binding["omp_process_epoch"],
            "target_state": "rejected", "reason_code": "USER_REJECTED",
        },
    )
    assert rejected.status_code == 200
    inbox = await client.get(
        "/api/agent/claude.sender/messages", params={"kind": "agent_reply"}
    )
    notices = inbox.json()["messages"]
    assert len(notices) == 1
    assert "held" not in notices[0]["content"]
    conn = sqlite3.connect(os.environ["BROKER_DB_PATH"])
    try:
        stored_notice = conn.execute(
            "SELECT m.to_agent,d.recipient_id FROM message_log m "
            "JOIN delivery d ON d.event_id=m.event_id WHERE m.event_id=?",
            (notices[0]["event_id"],),
        ).fetchone()
        assert stored_notice == ("claude.sender", sender_id)
        pre_r1_inbox = conn.execute(
            "SELECT event_id FROM message_log "
            "WHERE (to_agent=? OR to_agent=?) AND kind='agent_reply'",
            ("claude.sender", "claude.sender"),
        ).fetchall()
        assert pre_r1_inbox == [(notices[0]["event_id"],)]
        pre_r1_ack = conn.execute(
            "UPDATE delivery SET status='acked',acked_ts=3,updated_ts=3 "
            "WHERE (to_agent=? OR to_agent=?) AND event_id=? AND status!='acked'",
            ("claude.sender", "claude.sender", notices[0]["event_id"]),
        )
        assert pre_r1_ack.rowcount == 1
        conn.commit()
    finally:
        conn.close()


async def test_rejection_notice_reaches_id_only_sender_or_records_unavailable(client):
    recipient_id = str(uuid.uuid4())
    _, binding, token = await _bind_omp(client, recipient_id, token_seed=10)
    assert (await _omp_name(
        client, recipient_id, binding, token, "omp.id-only-reject"
    )).status_code == 200

    sender_id = f"id-only-{uuid.uuid4()}"
    registered = await client.post(f"/api/agent/{sender_id}/status", json={
        "kind": "demo_research", "channel": "#demo", "status": "idle",
        "last_activity_ts": 1780001000380,
    })
    assert registered.status_code == 200
    sent = await client.post("/api/event", json={
        "from_agent": sender_id, "to_agent": "omp.id-only-reject",
        "channel": "#c", "kind": "agent_message", "content": "held-id",
        "ts": 1780001000381,
    })
    offer = (await _claim_offer(client, recipient_id, binding, token)).json()["status"]
    rejected = await client.post(
        f"/api/agent/{recipient_id}/delivery-offer-status",
        headers=_control_headers(token),
        json={
            "schema_version": 1, "event_id": offer["event_id"],
            "offer_token": offer["offer_token"],
            "binding_epoch": binding["new_binding_epoch"],
            "omp_process_epoch": binding["omp_process_epoch"],
            "target_state": "rejected", "reason_code": "USER_REJECTED",
        },
    )
    assert rejected.status_code == 200
    inbox = await client.get(
        f"/api/agent/{sender_id}/messages", params={"kind": "agent_reply"}
    )
    notices = inbox.json()["messages"]
    assert len(notices) == 1
    conn = sqlite3.connect(os.environ["BROKER_DB_PATH"])
    try:
        notice_owner = conn.execute(
            "SELECT recipient_id FROM delivery WHERE event_id=?",
            (notices[0]["event_id"],),
        ).fetchone()
    finally:
        conn.close()
    assert notice_owner == (sender_id,)
    acked = await client.post(
        f"/api/agent/{sender_id}/ack",
        json={"event_ids": [notices[0]["event_id"]]},
    )
    assert acked.json()["acked_now"] == 1

    missing_id = f"gone-{uuid.uuid4()}"
    assert (await client.post(f"/api/agent/{missing_id}/status", json={
        "kind": "demo_research", "channel": "#demo", "status": "idle",
        "last_activity_ts": 1780001000382,
    })).status_code == 200
    missing_sent = await client.post("/api/event", json={
        "from_agent": missing_id, "to_agent": "omp.id-only-reject",
        "channel": "#c", "kind": "agent_message", "content": "held-gone",
        "ts": 1780001000383,
    })
    conn = sqlite3.connect(os.environ["BROKER_DB_PATH"])
    try:
        deleted = conn.execute(
            "DELETE FROM agent_status WHERE id=?", (missing_id,)
        ).rowcount
        conn.commit()
    finally:
        conn.close()
    assert deleted == 1
    missing_offer = (
        await _claim_offer(client, recipient_id, binding, token)
    ).json()["status"]
    missing_rejected = await client.post(
        f"/api/agent/{recipient_id}/delivery-offer-status",
        headers=_control_headers(token),
        json={
            "schema_version": 1, "event_id": missing_offer["event_id"],
            "offer_token": missing_offer["offer_token"],
            "binding_epoch": binding["new_binding_epoch"],
            "omp_process_epoch": binding["omp_process_epoch"],
            "target_state": "rejected", "reason_code": "USER_REJECTED",
        },
    )
    assert missing_rejected.status_code == 200
    conn = sqlite3.connect(os.environ["BROKER_DB_PATH"])
    try:
        audit = conn.execute(
            "SELECT sender_notice,rejection_notice_event_id "
            "FROM omp_rejection_audit WHERE event_id=?",
            (missing_sent.json()["event_id"],),
        ).fetchone()
    finally:
        conn.close()
    assert audit == ("unavailable", None)


async def test_rejection_notice_retained_name_collision_is_unavailable(client):
    sender_id = "notice-retained-collision"
    assert (await client.post(f"/api/agent/{sender_id}/status", json={
        "kind": "demo_research", "channel": "#demo", "status": "idle",
        "last_activity_ts": 1780001000384,
    })).status_code == 200
    namespace_owner = str(uuid.uuid4())
    _, owner_binding, owner_token = await _bind_omp(
        client, namespace_owner, token_seed=16
    )
    assert (await _omp_name(
        client, namespace_owner, owner_binding, owner_token, sender_id
    )).status_code == 200
    assert (await _omp_name(
        client, namespace_owner, owner_binding, owner_token, "omp.notice-owner"
    )).status_code == 200

    recipient_id = str(uuid.uuid4())
    _, binding, token = await _bind_omp(client, recipient_id, token_seed=17)
    assert (await _omp_name(
        client, recipient_id, binding, token, "omp.notice-retained-target"
    )).status_code == 200
    sent = await client.post("/api/event", json={
        "from_agent": sender_id, "to_agent": "omp.notice-retained-target",
        "channel": "#c", "kind": "agent_message", "content": "held",
        "ts": 1780001000385,
    })
    assert sent.status_code == 200
    offer = (await _claim_offer(client, recipient_id, binding, token)).json()["status"]
    rejected = await client.post(
        f"/api/agent/{recipient_id}/delivery-offer-status",
        headers=_control_headers(token),
        json={
            "schema_version": 1, "event_id": offer["event_id"],
            "offer_token": offer["offer_token"],
            "binding_epoch": binding["new_binding_epoch"],
            "omp_process_epoch": binding["omp_process_epoch"],
            "target_state": "rejected", "reason_code": "USER_REJECTED",
        },
    )
    assert rejected.status_code == 200
    conn = sqlite3.connect(os.environ["BROKER_DB_PATH"])
    try:
        original_sender = conn.execute(
            "SELECT sender_id FROM delivery WHERE event_id=?",
            (sent.json()["event_id"],),
        ).fetchone()
        audit = conn.execute(
            "SELECT sender_notice,rejection_notice_event_id "
            "FROM omp_rejection_audit WHERE event_id=?",
            (sent.json()["event_id"],),
        ).fetchone()
        wrong_owner_notices = conn.execute(
            "SELECT COUNT(*) FROM delivery d JOIN message_log m "
            "ON m.event_id=d.event_id WHERE d.recipient_id=? "
            "AND m.kind='agent_reply'",
            (namespace_owner,),
        ).fetchone()[0]
    finally:
        conn.close()
    assert original_sender == (sender_id,)
    assert audit == ("unavailable", None)
    assert wrong_owner_notices == 0


async def test_rejection_notice_late_name_collision_is_unavailable(client):
    sender_id = "notice-late-name-collision"
    assert (await client.post(f"/api/agent/{sender_id}/status", json={
        "kind": "demo_research", "channel": "#demo", "status": "idle",
        "last_activity_ts": 1780001000386,
    })).status_code == 200
    recipient_id = str(uuid.uuid4())
    _, binding, token = await _bind_omp(client, recipient_id, token_seed=18)
    assert (await _omp_name(
        client, recipient_id, binding, token, "omp.notice-late-target"
    )).status_code == 200
    sent = await client.post("/api/event", json={
        "from_agent": sender_id, "to_agent": "omp.notice-late-target",
        "channel": "#c", "kind": "agent_message", "content": "held",
        "ts": 1780001000387,
    })
    assert sent.status_code == 200
    late_owner = str(uuid.uuid4())
    assert (await client.post(f"/api/agent/{late_owner}/status", json={
        "kind": "cc-session", "channel": "#cc-relay", "status": "idle",
        "last_activity_ts": 1780001000388, "display_name": sender_id,
    })).status_code == 200

    offer = (await _claim_offer(client, recipient_id, binding, token)).json()["status"]
    rejected = await client.post(
        f"/api/agent/{recipient_id}/delivery-offer-status",
        headers=_control_headers(token),
        json={
            "schema_version": 1, "event_id": offer["event_id"],
            "offer_token": offer["offer_token"],
            "binding_epoch": binding["new_binding_epoch"],
            "omp_process_epoch": binding["omp_process_epoch"],
            "target_state": "rejected", "reason_code": "USER_REJECTED",
        },
    )
    assert rejected.status_code == 200
    conn = sqlite3.connect(os.environ["BROKER_DB_PATH"])
    try:
        audit = conn.execute(
            "SELECT sender_notice,rejection_notice_event_id "
            "FROM omp_rejection_audit WHERE event_id=?",
            (sent.json()["event_id"],),
        ).fetchone()
        wrong_owner_notices = conn.execute(
            "SELECT COUNT(*) FROM delivery d JOIN message_log m "
            "ON m.event_id=d.event_id WHERE d.recipient_id=? "
            "AND m.kind='agent_reply'",
            (late_owner,),
        ).fetchone()[0]
    finally:
        conn.close()
    assert audit == ("unavailable", None)
    assert wrong_owner_notices == 0


async def test_rejection_notice_to_nameless_codex_sender_acks_by_stable_id(client):
    sender_id = str(uuid.uuid4())
    registered = await client.post(f"/api/agent/{sender_id}/status", json={
        "kind": "codex-session", "channel": "#codex-relay", "status": "idle",
        "last_activity_ts": 1780001000390, "transport": "codex-app-server",
    })
    assert registered.status_code == 200
    recipient_id = str(uuid.uuid4())
    _, binding, token = await _bind_omp(client, recipient_id, token_seed=15)
    assert (await _omp_name(
        client, recipient_id, binding, token, "omp.nameless-reject"
    )).status_code == 200
    sent = await client.post("/api/event", json={
        "from_agent": sender_id, "to_agent": "omp.nameless-reject",
        "channel": "#c", "kind": "agent_message", "content": "held",
        "ts": 1780001000391,
    })
    assert sent.status_code == 200
    offer = (await _claim_offer(client, recipient_id, binding, token)).json()["status"]
    rejected = await client.post(
        f"/api/agent/{recipient_id}/delivery-offer-status",
        headers=_control_headers(token),
        json={
            "schema_version": 1, "event_id": offer["event_id"],
            "offer_token": offer["offer_token"],
            "binding_epoch": binding["new_binding_epoch"],
            "omp_process_epoch": binding["omp_process_epoch"],
            "target_state": "rejected", "reason_code": "USER_REJECTED",
        },
    )
    assert rejected.status_code == 200
    inbox = await client.get(f"/api/agent/{sender_id}/deliveries")
    notices = inbox.json()["deliveries"]
    assert len(notices) == 1
    assert notices[0]["to_agent"] == sender_id
    acked = await client.post(
        f"/api/agent/{sender_id}/delivery-ack",
        json={"event_ids": [notices[0]["event_id"]]},
    )
    assert acked.json() == {"acked_now": 1, "requested": 1}
    drained = await client.get(f"/api/agent/{sender_id}/deliveries")
    assert drained.json()["deliveries"] == []


async def test_rejected_legacy_rows_are_hidden_and_pre_r1_ack_skips_them(
    client, monkeypatch
):
    monkeypatch.setenv("BROKER_OPERATOR_TOKEN", "legacy-reject-secret")
    monkeypatch.setenv("BROKER_OPERATOR_ID", "legacy-reject-operator")
    event_ids = []
    for index in range(2):
        sent = await client.post("/api/event", json={
            "from_agent": "sender", "to_agent": "rejected.legacy", "channel": "#c",
            "kind": "agent_message", "content": f"legacy-{index}",
            "ts": 1780001000390 + index,
        })
        assert sent.status_code == 200
        event_ids.append(sent.json()["event_id"])
    rejected = await client.post(
        f"/api/operator/delivery/{event_ids[0]}/reject-unbound",
        headers={"Authorization": "Bearer legacy-reject-secret"},
        json={
            "schema_version": 1, "request_id": str(uuid.uuid4()),
            "reason_code": "PROTOCOL_INVALID",
        },
    )
    assert rejected.status_code == 200
    visible = await client.get("/api/agent/rejected.legacy/messages")
    assert [row["event_id"] for row in visible.json()["messages"]] == [event_ids[1]]

    conn = sqlite3.connect(os.environ["BROKER_DB_PATH"])
    try:
        conn.execute(
            "UPDATE delivery SET status='acked',acked_ts=1,updated_ts=1 "
            "WHERE to_agent='rejected.legacy' AND event_id IN (?,?) "
            "AND status!='acked'",
            tuple(event_ids),
        )
        conn.commit()
        states = conn.execute(
            "SELECT event_id,status FROM delivery WHERE event_id IN (?,?) "
            "ORDER BY event_id",
            tuple(event_ids),
        ).fetchall()
    finally:
        conn.close()
    assert states == [(event_ids[0], "rejected"), (event_ids[1], "acked")]
    skipped = await client.post(
        "/api/agent/rejected.legacy/ack", json={"event_ids": [event_ids[0]]}
    )
    assert skipped.status_code == 200
    assert skipped.json()["acked_now"] == 0

    claude_id = str(uuid.uuid4())
    assert (await client.post(f"/api/agent/{claude_id}/status", json={
        "kind": "cc-session", "channel": "#cc-relay", "status": "idle",
        "last_activity_ts": 1780001000393, "display_name": "operator.claude",
    })).status_code == 200
    claude_event = await client.post("/api/event", json={
        "from_agent": "sender", "to_agent": "operator.claude", "channel": "#c",
        "kind": "agent_message", "content": "not operator controlled",
        "ts": 1780001000394,
    })
    refused = await client.post(
        f"/api/operator/agent/{claude_id}/delivery-reject",
        headers={"Authorization": "Bearer legacy-reject-secret"},
        json={
            "schema_version": 1, "request_id": str(uuid.uuid4()),
            "event_id": str(claude_event.json()["event_id"]),
            "reason_code": "PROTOCOL_INVALID",
        },
    )
    assert refused.status_code == 404
    assert refused.json()["error"]["code"] == "DELIVERY_NOT_FOUND"


async def test_explicit_legacy_ack_survives_rename_for_abandoned_rows(client):
    owner_id = str(uuid.uuid4())
    relay_name = "abandoned.legacy"
    assert (await client.post(f"/api/agent/{owner_id}/status", json={
        "kind": "cc-session", "channel": "#cc-relay", "status": "idle",
        "last_activity_ts": 1780001000395, "display_name": relay_name,
    })).status_code == 200
    event_ids = []
    for index in range(2):
        sent = await client.post("/api/event", json={
            "from_agent": "sender", "to_agent": relay_name, "channel": "#c",
            "kind": "agent_message", "content": f"abandoned-{index}",
            "ts": 1780001000396 + index,
        })
        assert sent.status_code == 200
        event_ids.append(sent.json()["event_id"])
    conn = sqlite3.connect(os.environ["BROKER_DB_PATH"])
    try:
        conn.execute(
            "UPDATE delivery SET status='abandoned',updated_ts=2 "
            "WHERE event_id IN (?,?)",
            tuple(event_ids),
        )
        conn.commit()
    finally:
        conn.close()

    renamed = "abandoned.legacy.renamed"
    assert (await client.post(f"/api/agent/{owner_id}/status", json={
        "kind": "cc-session", "channel": "#cc-relay", "status": "idle",
        "last_activity_ts": 1780001000398, "display_name": renamed,
    })).status_code == 200

    visible = await client.get(f"/api/agent/{renamed}/messages")
    assert [row["event_id"] for row in visible.json()["messages"]] == event_ids
    by_name = await client.post(
        f"/api/agent/{renamed}/ack", json={"event_ids": [event_ids[0]]}
    )
    assert by_name.json() == {"acked_now": 1, "requested": 1}
    by_id = await client.post(
        f"/api/agent/{owner_id}/delivery-ack",
        json={"event_ids": [event_ids[1]]},
    )
    assert by_id.json() == {"acked_now": 1, "requested": 1}
    conn = sqlite3.connect(os.environ["BROKER_DB_PATH"])
    try:
        states = conn.execute(
            "SELECT d.event_id,d.to_agent,m.to_agent,d.status,d.acked_ts IS NOT NULL "
            "FROM delivery d JOIN message_log m ON m.event_id=d.event_id "
            "WHERE d.event_id IN (?,?) ORDER BY d.event_id",
            tuple(event_ids),
        ).fetchall()
    finally:
        conn.close()
    assert states == [
        (event_ids[0], renamed, renamed, "acked", 1),
        (event_ids[1], renamed, renamed, "acked", 1),
    ]


async def test_explicit_legacy_ack_repairs_rollback_rename_abandoned_rows(client):
    owner_id = str(uuid.uuid4())
    old_name = "rollback.abandoned.old"
    new_name = "rollback.abandoned.new"
    assert (await client.post(f"/api/agent/{owner_id}/status", json={
        "kind": "cc-session", "channel": "#cc-relay", "status": "idle",
        "last_activity_ts": 1780001000399, "display_name": old_name,
    })).status_code == 200
    event_ids = []
    for index in range(2):
        sent = await client.post("/api/event", json={
            "from_agent": "sender", "to_agent": old_name, "channel": "#c",
            "kind": "agent_message", "content": f"rollback-abandoned-{index}",
            "ts": 1780001000400 + index,
        })
        assert sent.status_code == 200
        event_ids.append(sent.json()["event_id"])
    conn = sqlite3.connect(os.environ["BROKER_DB_PATH"])
    try:
        conn.execute(
            "UPDATE delivery SET status='abandoned',updated_ts=2 "
            "WHERE event_id IN (?,?)",
            tuple(event_ids),
        )
        # Exact released rename shape: only outstanding rows move.
        conn.execute(
            "UPDATE message_log SET to_agent=? WHERE to_agent=? AND event_id IN "
            "(SELECT event_id FROM delivery WHERE status='outstanding')",
            (new_name, old_name),
        )
        conn.execute(
            "UPDATE delivery SET to_agent=?,updated_ts=3 "
            "WHERE to_agent=? AND status='outstanding'",
            (new_name, old_name),
        )
        conn.execute(
            "UPDATE agent_status SET relay_name=? WHERE id=?", (new_name, owner_id)
        )
        conn.commit()
        stale = conn.execute(
            "SELECT d.to_agent,m.to_agent FROM delivery d JOIN message_log m "
            "ON m.event_id=d.event_id WHERE d.event_id IN (?,?) ORDER BY d.event_id",
            tuple(event_ids),
        ).fetchall()
    finally:
        conn.close()
    assert stale == [(old_name, old_name), (old_name, old_name)]

    visible = await client.get(f"/api/agent/{new_name}/messages")
    assert [row["event_id"] for row in visible.json()["messages"]] == event_ids
    by_name = await client.post(
        f"/api/agent/{new_name}/ack", json={"event_ids": [event_ids[0]]}
    )
    assert by_name.json() == {"acked_now": 1, "requested": 1}
    by_id = await client.post(
        f"/api/agent/{owner_id}/delivery-ack",
        json={"event_ids": [event_ids[1]]},
    )
    assert by_id.json() == {"acked_now": 1, "requested": 1}
    conn = sqlite3.connect(os.environ["BROKER_DB_PATH"])
    try:
        repaired = conn.execute(
            "SELECT d.to_agent,m.to_agent,d.status FROM delivery d "
            "JOIN message_log m ON m.event_id=d.event_id "
            "WHERE d.event_id IN (?,?) ORDER BY d.event_id",
            tuple(event_ids),
        ).fetchall()
    finally:
        conn.close()
    assert repaired == [
        (new_name, new_name, "acked"),
        (new_name, new_name, "acked"),
    ]


async def test_operator_rejection_auth_head_idempotency_and_unbound_isolation(
    client, monkeypatch
):
    monkeypatch.setenv("BROKER_OPERATOR_TOKEN", "operator-secret")
    monkeypatch.setenv("BROKER_OPERATOR_ID", "ops-test")
    recipient_id = str(uuid.uuid4())
    _, binding, token = await _bind_omp(client, recipient_id)
    await _omp_name(client, recipient_id, binding, token, "omp.operator")
    event_ids = []
    for index in range(2):
        sent = await client.post("/api/event", json={
            "from_agent": "s", "to_agent": "omp.operator", "channel": "#c",
            "kind": "agent_message", "content": f"op{index}",
            "ts": 1780001000400 + index,
        })
        event_ids.append(str(sent.json()["event_id"]))

    request_id = str(uuid.uuid4())
    body = {
        "schema_version": 1, "request_id": request_id,
        "event_id": event_ids[1], "reason_code": "PROTOCOL_INVALID",
    }
    missing = await client.post(
        f"/api/operator/agent/{recipient_id}/delivery-reject", json=body,
    )
    assert missing.status_code == 401
    invalid = await client.post(
        f"/api/operator/agent/{recipient_id}/delivery-reject", json=body,
        headers={"Authorization": "Bearer wrong"},
    )
    assert invalid.status_code == 403
    assert invalid.json()["error"]["code"] == "OPERATOR_AUTH_INVALID"
    head_conflict = await client.post(
        f"/api/operator/agent/{recipient_id}/delivery-reject", json=body,
        headers={"Authorization": "Bearer operator-secret"},
    )
    assert head_conflict.status_code == 409
    assert head_conflict.json()["error"]["code"] == "HEAD_CONFLICT"
    replay = await client.post(
        f"/api/operator/agent/{recipient_id}/delivery-reject", json=body,
        headers={"Authorization": "Bearer operator-secret"},
    )
    assert replay.json() == head_conflict.json()
    changed = await client.post(
        f"/api/operator/agent/{recipient_id}/delivery-reject",
        json={**body, "reason_code": "RECIPIENT_POLICY_REFUSED"},
        headers={"Authorization": "Bearer operator-secret"},
    )
    assert changed.status_code == 409
    assert changed.json()["error"]["code"] == "REQUEST_CONFLICT"

    first_body = {
        "schema_version": 1, "request_id": str(uuid.uuid4()),
        "event_id": event_ids[0], "reason_code": "PROTOCOL_INVALID",
    }
    first = await client.post(
        f"/api/operator/agent/{recipient_id}/delivery-reject",
        json=first_body, headers={"Authorization": "Bearer operator-secret"},
    )
    assert first.status_code == 200
    assert first.json()["result"]["offer_state"] is None
    assert first.json()["result"]["reject_reason_code"] == "PROTOCOL_INVALID"
    first_replay = await client.post(
        f"/api/operator/agent/{recipient_id}/delivery-reject",
        json=first_body, headers={"Authorization": "Bearer operator-secret"},
    )
    assert first_replay.json() == first.json()
    assert "offer_token" not in first.json()["result"]

    offered_status = (await _claim_offer(client, recipient_id, binding, token)).json()["status"]
    assert offered_status["event_id"] == event_ids[1]
    offered_reject = await client.post(
        f"/api/operator/agent/{recipient_id}/delivery-reject",
        json={
            "schema_version": 1, "request_id": str(uuid.uuid4()),
            "event_id": event_ids[1], "reason_code": "RECIPIENT_POLICY_REFUSED",
        },
        headers={"Authorization": "Bearer operator-secret"},
    )
    assert offered_reject.status_code == 200
    assert offered_reject.json()["result"]["offer_state"] == "rejected"
    assert offered_reject.json()["result"]["reject_reason_code"] == (
        "DELIVERY_UNCERTAIN_CONTENT_MAY_HAVE_BEEN_SEEN"
    )

    injected_send = await client.post("/api/event", json={
        "from_agent": "s", "to_agent": "omp.operator", "channel": "#c",
        "kind": "agent_message", "content": "injected", "ts": 1780001000450,
    })
    injected_status = (await _claim_offer(client, recipient_id, binding, token)).json()["status"]
    injected = await client.post(
        f"/api/agent/{recipient_id}/delivery-offer-status",
        headers=_control_headers(token),
        json={
            "schema_version": 1, "event_id": injected_status["event_id"],
            "offer_token": injected_status["offer_token"],
            "binding_epoch": binding["new_binding_epoch"],
            "omp_process_epoch": binding["omp_process_epoch"],
            "target_state": "injected", "reason_code": None,
        },
    )
    assert injected.status_code == 200
    injected_reject = await client.post(
        f"/api/operator/agent/{recipient_id}/delivery-reject",
        json={
            "schema_version": 1, "request_id": str(uuid.uuid4()),
            "event_id": str(injected_send.json()["event_id"]),
            "reason_code": "PERSISTENCE_UNRECOVERABLE",
        },
        headers={"Authorization": "Bearer operator-secret"},
    )
    assert injected_reject.status_code == 200
    assert injected_reject.json()["result"]["reject_reason_code"] == (
        "DELIVERY_UNCERTAIN_CONTENT_MAY_HAVE_BEEN_SEEN"
    )

    accepted_send = await client.post("/api/event", json={
        "from_agent": "s", "to_agent": "omp.operator", "channel": "#c",
        "kind": "agent_message", "content": "accepted", "ts": 1780001000451,
    })
    accepted_status = (await _claim_offer(client, recipient_id, binding, token)).json()["status"]
    accepted = await client.post(
        f"/api/agent/{recipient_id}/delivery-offer-status",
        headers=_control_headers(token),
        json={
            "schema_version": 1, "event_id": accepted_status["event_id"],
            "offer_token": accepted_status["offer_token"],
            "binding_epoch": binding["new_binding_epoch"],
            "omp_process_epoch": binding["omp_process_epoch"],
            "target_state": "accepted", "reason_code": None,
        },
    )
    assert accepted.status_code == 200
    accepted_reject = await client.post(
        f"/api/operator/agent/{recipient_id}/delivery-reject",
        json={
            "schema_version": 1, "request_id": str(uuid.uuid4()),
            "event_id": str(accepted_send.json()["event_id"]),
            "reason_code": "PROTOCOL_INVALID",
        },
        headers={"Authorization": "Bearer operator-secret"},
    )
    assert accepted_reject.status_code == 409
    assert accepted_reject.json()["error"]["code"] == "TERMINAL_CONFLICT"

    unbound = await client.post("/api/event", json={
        "from_agent": "s", "to_agent": "nobody.yet", "channel": "#c",
        "kind": "agent_message", "content": "unbound", "ts": 1780001000500,
    })
    cross_route = await client.post(
        f"/api/operator/delivery/{unbound.json()['event_id']}/reject-unbound",
        json={
            "schema_version": 1, "request_id": request_id,
            "reason_code": "PERSISTENCE_UNRECOVERABLE",
        },
        headers={"Authorization": "Bearer operator-secret"},
    )
    assert cross_route.status_code == 409
    assert cross_route.json()["error"]["code"] == "REQUEST_CONFLICT"
    unbound_body = {
        "schema_version": 1, "request_id": str(uuid.uuid4()),
        "reason_code": "PERSISTENCE_UNRECOVERABLE",
    }
    unbound_reject = await client.post(
        f"/api/operator/delivery/{unbound.json()['event_id']}/reject-unbound",
        json=unbound_body, headers={"Authorization": "Bearer operator-secret"},
    )
    assert unbound_reject.status_code == 200
    assert unbound_reject.json()["result"]["recipient_id"] is None
    assert unbound_reject.json()["result"]["offer_state"] is None
    assert unbound_reject.json()["result"]["reject_reason_code"] == (
        "PERSISTENCE_UNRECOVERABLE"
    )
    assert (await client.post(
        f"/api/operator/delivery/{unbound.json()['event_id']}/reject-unbound",
        json=unbound_body, headers={"Authorization": "Bearer operator-secret"},
    )).json() == unbound_reject.json()

    late_id = str(uuid.uuid4())
    _, late_binding, late_token = await _bind_omp(client, late_id, token_seed=8)
    assert (await _omp_name(
        client, late_id, late_binding, late_token, "nobody.yet"
    )).status_code == 200
    assert (await _claim_offer(
        client, late_id, late_binding, late_token
    )).json()["code"] == "EMPTY"


async def test_operator_identical_concurrent_rejection_has_one_audit_and_notice(
    client, monkeypatch
):
    monkeypatch.setenv("BROKER_OPERATOR_TOKEN", "concurrent-secret")
    monkeypatch.setenv("BROKER_OPERATOR_ID", "concurrent-operator")
    sender_id = "concurrent-sender"
    assert (await client.post(f"/api/agent/{sender_id}/status", json={
        "kind": "codex-session", "channel": "#codex-relay", "status": "idle",
        "last_activity_ts": 1780001000520, "transport": "codex-app-server",
    })).status_code == 200
    assert (await client.put(
        f"/api/agent/{sender_id}/relay-name", json={"relay_name": "sender.concurrent"}
    )).status_code == 200
    recipient_id = str(uuid.uuid4())
    _, binding, token = await _bind_omp(client, recipient_id, token_seed=12)
    assert (await _omp_name(
        client, recipient_id, binding, token, "omp.concurrent"
    )).status_code == 200
    sent = await client.post("/api/event", json={
        "from_agent": "sender.concurrent", "to_agent": "omp.concurrent",
        "channel": "#c", "kind": "agent_message", "content": "concurrent",
        "ts": 1780001000521,
    })
    body = {
        "schema_version": 1,
        "request_id": str(uuid.uuid4()),
        "event_id": str(sent.json()["event_id"]),
        "reason_code": "PROTOCOL_INVALID",
    }
    first, second = await asyncio.gather(*(
        client.post(
            f"/api/operator/agent/{recipient_id}/delivery-reject",
            headers={"Authorization": "Bearer concurrent-secret"}, json=body,
        )
        for _ in range(2)
    ))
    assert first.status_code == second.status_code == 200
    assert first.json() == second.json()
    conn = sqlite3.connect(os.environ["BROKER_DB_PATH"])
    try:
        audit_count = conn.execute(
            "SELECT COUNT(*) FROM omp_rejection_audit WHERE request_id=?",
            (body["request_id"],),
        ).fetchone()[0]
        notice_count = conn.execute(
            "SELECT COUNT(*) FROM message_log "
            "WHERE from_agent='broker' AND to_agent=? AND kind='agent_reply'",
            ("sender.concurrent",),
        ).fetchone()[0]
    finally:
        conn.close()
    assert audit_count == 1
    assert notice_count == 1


async def test_omp_acceptance_and_operator_rejection_have_one_terminal_winner(
    client, monkeypatch
):
    monkeypatch.setenv("BROKER_OPERATOR_TOKEN", "race-secret")
    monkeypatch.setenv("BROKER_OPERATOR_ID", "race-operator")
    recipient_id = str(uuid.uuid4())
    _, binding, token = await _bind_omp(client, recipient_id, token_seed=9)
    await _omp_name(client, recipient_id, binding, token, "omp.race")
    sent = await client.post("/api/event", json={
        "from_agent": "s", "to_agent": "omp.race", "channel": "#c",
        "kind": "agent_message", "content": "race", "ts": 1780001000550,
    })
    status = (await _claim_offer(client, recipient_id, binding, token)).json()["status"]
    acceptance_body = {
        "schema_version": 1, "event_id": status["event_id"],
        "offer_token": status["offer_token"],
        "binding_epoch": binding["new_binding_epoch"],
        "omp_process_epoch": binding["omp_process_epoch"],
        "target_state": "accepted", "reason_code": None,
    }
    operator_body = {
        "schema_version": 1, "request_id": str(uuid.uuid4()),
        "event_id": str(sent.json()["event_id"]), "reason_code": "PROTOCOL_INVALID",
    }
    accepted, rejected = await asyncio.gather(
        client.post(
            f"/api/agent/{recipient_id}/delivery-offer-status",
            headers=_control_headers(token), json=acceptance_body,
        ),
        client.post(
            f"/api/operator/agent/{recipient_id}/delivery-reject",
            headers={"Authorization": "Bearer race-secret"}, json=operator_body,
        ),
    )
    assert sorted([accepted.status_code, rejected.status_code]) == [200, 409]
    proof = await client.get(
        f"/api/agent/{recipient_id}/delivery/{status['event_id']}/status",
        headers=_control_headers(token),
        params={
            "schema_version": "1", "binding_epoch": binding["new_binding_epoch"],
            "omp_process_epoch": binding["omp_process_epoch"],
        },
    )
    terminal = proof.json()["status"]
    assert terminal["offer_state"] in {"accepted", "rejected"}
    assert (terminal["offer_state"] == "rejected") == (
        terminal["delivery_status"] == "rejected"
    )


async def test_retained_namespace_never_claims_sender_identity(client):
    old_owner = str(uuid.uuid4())
    _, old_binding, old_token = await _bind_omp(client, old_owner, token_seed=14)
    assert (await _omp_name(
        client, old_owner, old_binding, old_token, "retained.sender"
    )).status_code == 200
    assert (await _omp_name(
        client, old_owner, old_binding, old_token, "omp.sender.current"
    )).status_code == 200

    legacy_id = str(uuid.uuid4())
    registered = await client.post(f"/api/agent/{legacy_id}/status", json={
        "kind": "cc-session", "channel": "#cc-relay", "status": "idle",
        "last_activity_ts": 1780001000570, "display_name": "retained.sender",
    })
    assert registered.status_code == 200
    assert (await client.get(f"/api/agent/{legacy_id}/status")).json()["relay_name"] is None

    recipient_id = str(uuid.uuid4())
    _, binding, token = await _bind_omp(client, recipient_id, token_seed=15)
    assert (await _omp_name(
        client, recipient_id, binding, token, "omp.sender-target"
    )).status_code == 200
    sent = await client.post("/api/event", json={
        "from_agent": "retained.sender", "to_agent": "omp.sender-target",
        "channel": "#c", "kind": "agent_message", "content": "not old owner",
        "ts": 1780001000571,
    })
    assert sent.status_code == 200
    conn = sqlite3.connect(os.environ["BROKER_DB_PATH"])
    try:
        sender_id = conn.execute(
            "SELECT sender_id FROM delivery WHERE event_id=?",
            (sent.json()["event_id"],),
        ).fetchone()
    finally:
        conn.close()
    assert sender_id == (None,)

    offer = (await _claim_offer(client, recipient_id, binding, token)).json()["status"]
    rejected = await client.post(
        f"/api/agent/{recipient_id}/delivery-offer-status",
        headers=_control_headers(token),
        json={
            "schema_version": 1, "event_id": offer["event_id"],
            "offer_token": offer["offer_token"],
            "binding_epoch": binding["new_binding_epoch"],
            "omp_process_epoch": binding["omp_process_epoch"],
            "target_state": "rejected", "reason_code": "USER_REJECTED",
        },
    )
    assert rejected.status_code == 200
    assert (await client.get(f"/api/agent/{old_owner}/deliveries")).json()["deliveries"] == []
    conn = sqlite3.connect(os.environ["BROKER_DB_PATH"])
    try:
        audit = conn.execute(
            "SELECT sender_notice,rejection_notice_event_id FROM omp_rejection_audit "
            "WHERE event_id=?",
            (sent.json()["event_id"],),
        ).fetchone()
    finally:
        conn.close()
    assert audit == ("unavailable", None)


async def test_operator_non_ascii_auth_and_abandoned_terminal_are_closed(
    client, monkeypatch
):
    monkeypatch.setenv("BROKER_OPERATOR_TOKEN", "closed-operator")
    monkeypatch.setenv("BROKER_OPERATOR_ID", "closed-operator-id")
    non_ascii = await client.post(
        "/api/operator/delivery/1/reject-unbound",
        headers=[(b"Authorization", b"Bearer \xe9")],
        content=b"{}",
    )
    assert non_ascii.status_code == 403
    assert non_ascii.json()["error"]["code"] == "OPERATOR_AUTH_INVALID"

    sent = await client.post("/api/event", json={
        "from_agent": "sender", "to_agent": "unbound.abandoned", "channel": "#c",
        "kind": "agent_message", "content": "terminal", "ts": 1780001000572,
    })
    event_id = sent.json()["event_id"]
    conn = sqlite3.connect(os.environ["BROKER_DB_PATH"])
    try:
        conn.execute(
            "UPDATE delivery SET status='abandoned',updated_ts=updated_ts+1 "
            "WHERE event_id=?",
            (event_id,),
        )
        conn.commit()
    finally:
        conn.close()
    body = {
        "schema_version": 1, "request_id": str(uuid.uuid4()),
        "reason_code": "PERSISTENCE_UNRECOVERABLE",
    }
    first = await client.post(
        f"/api/operator/delivery/{event_id}/reject-unbound",
        headers={"Authorization": "Bearer closed-operator"}, json=body,
    )
    assert first.status_code == 409
    assert first.json()["error"]["code"] == "TERMINAL_CONFLICT"
    assert first.json()["error"]["details"]["result"]["delivery_status"] == "abandoned"
    replay = await client.post(
        f"/api/operator/delivery/{event_id}/reject-unbound",
        headers={"Authorization": "Bearer closed-operator"}, json=body,
    )
    assert replay.status_code == 409
    assert replay.json() == first.json()


async def test_ever_omp_identity_rejects_legacy_status_name_and_ack(client):
    recipient_id = str(uuid.uuid4())
    _, binding, token = await _bind_omp(client, recipient_id)
    await _omp_name(client, recipient_id, binding, token, "omp.legacy-fence")
    legacy_status = await client.post(f"/api/agent/{recipient_id}/status", json={
        "kind": "cc-session", "channel": "#cc-relay", "status": "idle",
        "last_activity_ts": 1780001000600,
    })
    assert legacy_status.status_code == 409
    assert (await client.put(
        f"/api/agent/{recipient_id}/relay-name", json={"relay_name": "stolen"}
    )).status_code == 409
    assert (await client.post(
        f"/api/agent/{recipient_id}/delivery-ack", json={"event_ids": [1]}
    )).status_code == 409
    assert (await client.get(
        "/api/agent/omp.legacy-fence/messages"
    )).status_code == 409
    assert (await client.post(
        "/api/agent/omp.legacy-fence/ack", json={"event_ids": [1]}
    )).status_code == 409

    shadow = await client.post("/api/agent/omp.legacy-fence/status", json={
        "kind": "cc-session", "channel": "#cc-relay", "status": "idle",
        "last_activity_ts": 1780001000601,
    })
    assert shadow.status_code == 200
    assert (await client.get(
        "/api/agent/omp.legacy-fence/messages"
    )).status_code == 409

    legacy_id = "legacy-takeover-actor"
    registered = await client.post(f"/api/agent/{legacy_id}/status", json={
        "kind": "cc-session", "channel": "#cc-relay", "status": "idle",
        "last_activity_ts": 1780001000601, "display_name": "legacy.actor",
    })
    assert registered.status_code == 200
    takeover = await client.put(
        f"/api/agent/{legacy_id}/relay-name",
        json={"relay_name": "omp.legacy-fence", "takeover": True},
    )
    assert takeover.status_code == 409
    current = await client.get(f"/api/agent/{recipient_id}/status")
    assert current.json()["relay_name"] == "omp.legacy-fence"


async def test_omp_private_routes_reject_duplicates_unknowns_and_noncanonical_types(client):
    recipient_id = str(uuid.uuid4())
    _, binding, token = await _bind_omp(client, recipient_id)
    await _omp_name(client, recipient_id, binding, token, "omp.closed")
    sent = await client.post("/api/event", json={
        "from_agent": "s", "to_agent": "omp.closed", "channel": "#c",
        "kind": "agent_message", "content": "closed", "ts": 1780001000650,
    })
    assert sent.status_code == 200

    missing = await client.post(
        f"/api/agent/{recipient_id}/delivery-offer",
        json={
            "schema_version": 1,
            "binding_epoch": binding["new_binding_epoch"],
            "omp_process_epoch": binding["omp_process_epoch"],
        },
    )
    assert missing.status_code == 401
    assert missing.json()["error"]["code"] == "CONTROL_REQUIRED"

    duplicate = await client.post(
        f"/api/agent/{recipient_id}/delivery-offer",
        headers={**_control_headers(token), "Content-Type": "application/json"},
        content=(
            '{"schema_version":1,"schema_version":1,'
            f'"binding_epoch":"{binding["new_binding_epoch"]}",'
            f'"omp_process_epoch":"{binding["omp_process_epoch"]}"}}'
        ),
    )
    assert duplicate.status_code == 422
    assert duplicate.json()["error"]["code"] == "PROTOCOL_INVALID"

    claim = await _claim_offer(client, recipient_id, binding, token)
    status = claim.json()["status"]
    bad_reason = await client.post(
        f"/api/agent/{recipient_id}/delivery-offer-status",
        headers=_control_headers(token),
        json={
            "schema_version": 1, "event_id": status["event_id"],
            "offer_token": status["offer_token"],
            "binding_epoch": binding["new_binding_epoch"],
            "omp_process_epoch": binding["omp_process_epoch"],
            "target_state": "injected", "reason_code": "USER_REJECTED",
        },
    )
    assert bad_reason.status_code == 422
    numeric_id = await client.post(
        f"/api/agent/{recipient_id}/delivery-ack",
        headers=_control_headers(token),
        json={
            "schema_version": 1, "event_id": int(status["event_id"]),
            "offer_token": status["offer_token"],
            "binding_epoch": binding["new_binding_epoch"],
            "omp_process_epoch": binding["omp_process_epoch"],
        },
    )
    assert numeric_id.status_code == 422
    unchanged = await client.get(
        f"/api/agent/{recipient_id}/delivery/{status['event_id']}/status",
        headers=_control_headers(token),
        params={
            "schema_version": "1", "binding_epoch": binding["new_binding_epoch"],
            "omp_process_epoch": binding["omp_process_epoch"],
        },
    )
    assert unchanged.json()["status"]["offer_state"] == "offered"

    malformed_name = await client.put(
        f"/api/agent/{recipient_id}/relay-name",
        headers=_control_headers(token),
        json=[],
    )
    assert malformed_name.status_code == 422
    assert malformed_name.json()["error"]["code"] == "PROTOCOL_INVALID"


async def test_all_omp_mutation_routes_close_non_object_bodies(client, monkeypatch):
    recipient_id = str(uuid.uuid4())
    _, binding, token = await _bind_omp(client, recipient_id, token_seed=13)
    control = _control_headers(token)

    status = await client.post(
        f"/api/agent/{recipient_id}/status", headers=control, json=[]
    )
    assert status.status_code == 422
    assert status.json()["error"]["code"] == "PROTOCOL_INVALID"
    for raw in (b"null", b"", b"{"):
        closed_status = await client.post(
            f"/api/agent/{recipient_id}/status",
            headers={**control, "Content-Type": "application/json"},
            content=raw,
        )
        assert closed_status.status_code == 422, closed_status.text
        assert closed_status.json()["error"]["code"] == "PROTOCOL_INVALID"

    private_routes = [
        ("post", f"/api/agent/{recipient_id}/delivery-offer"),
        ("post", f"/api/agent/{recipient_id}/delivery-offer-status"),
        ("post", f"/api/agent/{recipient_id}/delivery-ack"),
        ("put", f"/api/agent/{recipient_id}/relay-name"),
    ]
    for method, route in private_routes:
        request = getattr(client, method)
        missing = await request(route, json=[])
        assert missing.status_code == 401, (route, missing.text)
        assert missing.json()["error"]["code"] == "CONTROL_REQUIRED"
        closed = await request(route, headers=control, json=[])
        assert closed.status_code == 422, (route, closed.text)
        assert closed.json()["error"]["code"] == "PROTOCOL_INVALID"
        for raw in (b"null", b"", b"{"):
            missing_raw = await request(
                route, headers={"Content-Type": "application/json"}, content=raw
            )
            assert missing_raw.status_code == 401, (route, missing_raw.text)
            assert missing_raw.json()["error"]["code"] == "CONTROL_REQUIRED"
            closed_raw = await request(
                route,
                headers={**control, "Content-Type": "application/json"},
                content=raw,
            )
            assert closed_raw.status_code == 422, (route, closed_raw.text)
            assert closed_raw.json()["error"]["code"] == "PROTOCOL_INVALID"

    monkeypatch.setenv("BROKER_OPERATOR_TOKEN", "malformed-operator")
    monkeypatch.setenv("BROKER_OPERATOR_ID", "malformed-operator-id")
    operator_routes = [
        f"/api/operator/agent/{recipient_id}/delivery-reject",
        "/api/operator/delivery/1/reject-unbound",
    ]
    for route in operator_routes:
        missing = await client.post(route, json=[])
        assert missing.status_code == 401
        assert missing.json()["error"]["code"] == "OPERATOR_AUTH_REQUIRED"
        closed = await client.post(
            route, headers={"Authorization": "Bearer malformed-operator"}, json=[]
        )
        assert closed.status_code == 422
        assert closed.json()["error"]["code"] == "PROTOCOL_INVALID"
        for raw in (b"null", b"", b"{"):
            missing_raw = await client.post(
                route, headers={"Content-Type": "application/json"}, content=raw
            )
            assert missing_raw.status_code == 401
            assert missing_raw.json()["error"]["code"] == "OPERATOR_AUTH_REQUIRED"
            closed_raw = await client.post(
                route,
                headers={
                    "Authorization": "Bearer malformed-operator",
                    "Content-Type": "application/json",
                },
                content=raw,
            )
            assert closed_raw.status_code == 422
            assert closed_raw.json()["error"]["code"] == "PROTOCOL_INVALID"


async def test_omp_rollback_triggers_bind_legacy_insert_and_reject_poison(client):
    recipient_id = str(uuid.uuid4())
    _, binding, token = await _bind_omp(client, recipient_id)
    poisoned = await client.post("/api/event", json={
        "from_agent": "old-code", "to_agent": recipient_id, "channel": "#c",
        "kind": "agent_message", "content": "bad timestamp",
        "ts": 9_007_199_254_740_992,
    })
    assert poisoned.status_code == 422
    assert poisoned.json()["error"]["code"] == "PROTOCOL_INVALID"
    await _omp_name(client, recipient_id, binding, token, "omp.rollback")
    renamed = await _omp_name(
        client, recipient_id, binding, token, "omp.rollback.renamed"
    )
    assert renamed.status_code == 200, renamed.text
    db_path = os.environ["BROKER_DB_PATH"]
    conn = sqlite3.connect(db_path)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="omp_bound_content"):
            conn.execute(
                "INSERT INTO message_log "
                "(from_agent,to_agent,channel,kind,content,ts) VALUES (?,?,?,?,?,?)",
                ("old-code", "omp.rollback", "#c", "agent_message", "x" * 1_048_577, 1),
            )
        conn.rollback()
        with pytest.raises(sqlite3.IntegrityError, match="omp_bound_ts"):
            conn.execute(
                "INSERT INTO message_log "
                "(from_agent,to_agent,channel,kind,content,ts) VALUES (?,?,?,?,?,?)",
                (
                    "old-code", recipient_id, "#c", "agent_message", "bad ts",
                    9_007_199_254_740_992,
                ),
            )
        conn.rollback()
        assert conn.execute(
            "SELECT COUNT(*) FROM message_log WHERE to_agent='omp.rollback'"
        ).fetchone()[0] == 0
        cursor = conn.execute(
            "INSERT INTO message_log "
            "(from_agent,to_agent,channel,kind,content,ts) VALUES (?,?,?,?,?,?)",
            ("old-code", "omp.rollback", "#c", "agent_message", "ok", 2),
        )
        event_id = cursor.lastrowid
        conn.execute(
            "INSERT INTO delivery "
            "(to_agent,event_id,status,attempts,created_ts,updated_ts) "
            "VALUES (?,?,'outstanding',0,2,2)",
            ("omp.rollback", event_id),
        )
        bound_id = conn.execute(
            "SELECT recipient_id FROM delivery WHERE event_id=?", (event_id,)
        ).fetchone()[0]
        assert bound_id == recipient_id
        conn.commit()
        with pytest.raises(sqlite3.IntegrityError, match="omp_reject_reason_invalid"):
            conn.execute(
                "UPDATE delivery SET status='rejected',reject_reason_code='NOT_CLOSED' "
                "WHERE event_id=?",
                (event_id,),
            )
        conn.rollback()
    finally:
        conn.close()


async def test_omp_private_projection_preserves_event_id_above_js_safe_integer(client):
    recipient_id = str(uuid.uuid4())
    _, binding, token = await _bind_omp(client, recipient_id)
    await _omp_name(client, recipient_id, binding, token, "omp.big-id")
    conn = sqlite3.connect(os.environ["BROKER_DB_PATH"])
    try:
        conn.execute(
            "UPDATE sqlite_sequence SET seq=? WHERE name='message_log'",
            (9_007_199_254_740_992,),
        )
        conn.commit()
    finally:
        conn.close()
    sent = await client.post("/api/event", json={
        "from_agent": "s", "to_agent": "omp.big-id", "channel": "#c",
        "kind": "agent_message", "content": "large id", "ts": 1780001000700,
    })
    assert sent.status_code == 200
    assert sent.json()["event_id"] > 9_007_199_254_740_992
    claim = await _claim_offer(client, recipient_id, binding, token)
    assert claim.status_code == 200
    assert claim.json()["status"]["event_id"] == str(sent.json()["event_id"])
    assert isinstance(claim.json()["delivery"]["event_id"], str)
