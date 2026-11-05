"""POST /api/admin/prune — broker-owned destructive prune (per broker_dashboard handoff).

The dashboard prune button POSTs `application/json` with `{"days": N}`, so the
endpoint reads `days` from an optional JSON body (NOT a query param). These tests
exercise that body contract.

Covers (per the handoff spec verification list):
- days < 1 -> HTTP 422
- the body's days is HONORED (regression guard for the query-vs-body mismatch)
- stale agent_status deleted while fresh preserved (DURABLE prune)
- terminal pipelines_view rows older than cutoff deleted while in-flight + recent
  terminal rows preserved
- mixed-unit started_at normalization: 10-digit epoch-SECONDS rows up-converted by
  the inline CASE before comparison, in BOTH directions (old-seconds pruned,
  recent-seconds kept) alongside 13-digit epoch-ms rows
- returned-count shape {deleted_agents, deleted_pipelines, days, cutoff}
- default days = 7 when no body is sent
- ungated: returns 200 without BROKER_ADMIN_ENABLED (unlike /api/admin/audit-snapshot)

pipelines_view is a derived cache with no insert API, so it is seeded directly via
aiosqlite; a project_registry parent row is inserted first to satisfy the FK.
"""

from __future__ import annotations

import os
import time

import aiosqlite
import pytest

pytestmark = pytest.mark.asyncio

DAY_MS = 86_400_000


async def _seed_agent(conn, agent_id, last_activity_ts, status="idle"):
    await conn.execute(
        "INSERT INTO agent_status (id, kind, channel, status, last_activity_ts, registered_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (agent_id, "demo", "#cc-demo", status, last_activity_ts, last_activity_ts),
    )


async def _seed_project(conn, name="proj_prune"):
    await conn.execute(
        "INSERT INTO project_registry "
        "(project_id, name, root_path, state_db_path, registered_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (name, name, f"/tmp/{name}", f"/tmp/{name}/state.db", 1),
    )


async def _seed_pipeline(conn, pipeline_id, started_at, status, project="proj_prune"):
    await conn.execute(
        "INSERT INTO pipelines_view "
        "(project_name, pipeline_id, shape, started_at, completed_at, status, "
        " step_count, steps_done, last_refresh_ts) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (project, pipeline_id, "office_hours", started_at, None, status, 3, 0, started_at),
    )


async def test_prune_rejects_days_below_one(client):
    for d in (0, -1, -100):
        r = await client.post("/api/admin/prune", json={"days": d})
        assert r.status_code == 422, (d, r.text)


async def test_prune_honors_body_days(client):
    # Regression guard for the query-vs-body mismatch: the body's `days` MUST be
    # used, not silently defaulted to 7. Seed an agent aged 6 days:
    #   days=10 -> 6d row kept (6 < 10);  days=5 -> 6d row pruned (6 >= 5).
    # Against the old query-param endpoint a JSON body was ignored (days==7 always),
    # so the days=5 case would delete 0 and FAIL this test.
    now = int(time.time() * 1000)
    async with aiosqlite.connect(os.environ["BROKER_DB_PATH"]) as conn:
        await _seed_agent(conn, "aged-6d", now - 6 * DAY_MS)
        await conn.commit()

    r = await client.post("/api/admin/prune", json={"days": 10})
    assert r.status_code == 200, r.text
    assert r.json()["deleted_agents"] == 0, r.json()
    assert r.json()["days"] == 10, r.json()

    r = await client.post("/api/admin/prune", json={"days": 5})
    assert r.status_code == 200, r.text
    assert r.json()["deleted_agents"] == 1, r.json()
    assert r.json()["days"] == 5, r.json()


async def test_prune_deletes_stale_agents_preserves_fresh(client):
    now = int(time.time() * 1000)
    async with aiosqlite.connect(os.environ["BROKER_DB_PATH"]) as conn:
        await _seed_agent(conn, "stale-agent", now - 30 * DAY_MS)
        await _seed_agent(conn, "fresh-agent", now)
        await conn.commit()

    r = await client.post("/api/admin/prune", json={"days": 7})
    assert r.status_code == 200, r.text
    assert r.json()["deleted_agents"] == 1, r.json()

    r = await client.get("/api/agent/stale-agent/status")
    assert r.status_code == 404, r.text
    r = await client.get("/api/agent/fresh-agent/status")
    assert r.status_code == 200, r.text


async def test_prune_terminal_pipelines_preserves_inflight_and_recent(client):
    now = int(time.time() * 1000)
    async with aiosqlite.connect(os.environ["BROKER_DB_PATH"]) as conn:
        await _seed_project(conn)
        # terminal + old (epoch-ms)        -> DELETE
        await _seed_pipeline(conn, "done-old-ms", now - 30 * DAY_MS, "done")
        # terminal + old (epoch-SECONDS)   -> DELETE (mixed-unit CASE up-converts then prunes)
        await _seed_pipeline(conn, "cancelled-old-s", (now - 30 * DAY_MS) // 1000, "cancelled")
        # terminal + recent (epoch-ms)     -> KEEP
        await _seed_pipeline(conn, "failed-recent-ms", now, "failed")
        # terminal + recent (epoch-SECONDS)-> KEEP (CASE up-convert stays > cutoff)
        await _seed_pipeline(conn, "done-recent-s", now // 1000, "done")
        # in-flight + old                  -> KEEP (non-terminal status never pruned)
        await _seed_pipeline(conn, "inprogress-old", now - 30 * DAY_MS, "in_progress")
        await conn.commit()

    r = await client.post("/api/admin/prune", json={"days": 7})
    assert r.status_code == 200, r.text
    assert r.json()["deleted_pipelines"] == 2, r.json()

    async with aiosqlite.connect(os.environ["BROKER_DB_PATH"]) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute("SELECT pipeline_id FROM pipelines_view")
        remaining = {row["pipeline_id"] for row in await cur.fetchall()}
        await cur.close()
    assert remaining == {"failed-recent-ms", "done-recent-s", "inprogress-old"}, remaining


async def test_prune_returns_expected_shape_and_default_days(client):
    r = await client.post("/api/admin/prune")  # no body -> default days=7
    assert r.status_code == 200, r.text
    body = r.json()
    assert set(body.keys()) == {"deleted_agents", "deleted_pipelines", "days", "cutoff"}, body
    assert body["days"] == 7
    assert isinstance(body["deleted_agents"], int)
    assert isinstance(body["deleted_pipelines"], int)
    now = int(time.time() * 1000)
    # cutoff = now_ms() - 7d; allow a few minutes of clock slack
    assert abs(body["cutoff"] - (now - 7 * DAY_MS)) < 5 * 60 * 1000, body


async def test_prune_ungated_without_admin_enabled(client):
    # We chose ungated (per spec): the client fixture leaves BROKER_ADMIN_ENABLED
    # unset, yet prune must still return 200 (contrast /api/admin/audit-snapshot,
    # which 403s when admin is disabled).
    r = await client.post("/api/admin/prune", json={"days": 7})
    assert r.status_code == 200, r.text
