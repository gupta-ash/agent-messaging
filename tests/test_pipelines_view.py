"""pipelines_view materialization test (direct call; live watcher skipped per plan)."""

from __future__ import annotations

import os
import sqlite3

import aiosqlite
import pytest

pytestmark = pytest.mark.asyncio


async def test_refresh_pipelines_view_inserts_rows(client, project_state_db):
    # Register first
    r = await client.post("/api/project/register", json=project_state_db)
    assert r.status_code == 200

    # Direct refresh call already happened during register; verify rows
    async with aiosqlite.connect(os.environ["BROKER_DB_PATH"]) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(
            "SELECT * FROM pipelines_view WHERE project_name = ?",
            (project_state_db["name"],),
        )
        rows = [dict(r) for r in await cur.fetchall()]

    assert len(rows) == 1
    assert rows[0]["pipeline_id"] == "p1"
    assert rows[0]["shape"] == "office_hours"
    assert rows[0]["status"] == "in_progress"
    assert rows[0]["step_count"] == 3
    assert rows[0]["steps_done"] == 1


async def test_refresh_pipelines_view_updates_after_state_change(client, project_state_db):
    await client.post("/api/project/register", json=project_state_db)

    # Mutate the project state.db: mark second step done
    conn = sqlite3.connect(project_state_db["state_db_path"])
    conn.execute("UPDATE pipeline_steps SET status='done' WHERE pipeline_id='p1' AND step_index=1")
    conn.commit()
    conn.close()

    # Force refresh
    r = await client.post(f"/api/project/{project_state_db['name']}/refresh")
    assert r.status_code == 200
    assert r.json()["pipeline_count"] == 1

    # Verify updated steps_done
    async with aiosqlite.connect(os.environ["BROKER_DB_PATH"]) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(
            "SELECT steps_done FROM pipelines_view WHERE project_name = ? AND pipeline_id = ?",
            (project_state_db["name"], "p1"),
        )
        row = await cur.fetchone()
    assert row["steps_done"] == 2


async def test_refresh_unregistered_project_noop(client):
    # D27 iter-2 (H2): post_project_refresh now consults project_registry
    # first and returns 404 for an unknown project instead of silently no-op'ing
    # with 200 + pipeline_count=0 (which previously misled operators into
    # believing refresh succeeded). The test name is preserved for git-blame
    # continuity; the assertion was updated as part of the H2 fix.
    r = await client.post("/api/project/ghost/refresh")
    assert r.status_code == 404
    assert "not registered" in r.json()["error"]["message"]


async def test_get_state_includes_pipelines_view(client, project_state_db):
    await client.post("/api/project/register", json=project_state_db)
    r = await client.get("/api/state")
    d = r.json()
    assert len(d["pipelines"]) == 1
    assert d["pipelines"][0]["pipeline_id"] == "p1"
