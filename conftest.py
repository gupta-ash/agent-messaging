"""Pytest fixtures for broker tests.

Each test gets:
- An ephemeral SQLite at tmp_path / coordination.db
- BROKER_HOST forced to 127.0.0.1 (loopback => no auth)
- BROKER_BEARER_TOKEN + BROKER_ADMIN_ENABLED unset
- A schema-bootstrapped DB (ensure_schema called)
- An httpx.AsyncClient bound to the FastAPI app via ASGITransport

Lifespan startup tasks (TTL sweeper, file watchers) are skipped in unit tests
to avoid timing flakiness. Tests that need those exercise them directly.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import httpx
import pytest
import pytest_asyncio

# Collection imports may freeze HOME-dependent library paths. Isolate first.
import tempfile
import shutil
import atexit

SOURCE_ROOT = Path(__file__).resolve().parent
_ISOLATION = tempfile.TemporaryDirectory(prefix="broker-source-tests-")
_ORIGINAL_CWD = Path.cwd()


def _cleanup_isolation():
    import logging
    logging.shutdown()
    os.chdir(_ORIGINAL_CWD)
    _ISOLATION.cleanup()


atexit.register(_cleanup_isolation)
_TEST_HOME = Path(_ISOLATION.name) / "home"
_TEST_HOME.mkdir()
for _key in ("HOME", "USERPROFILE"):
    os.environ[_key] = str(_TEST_HOME)
os.environ["HOMEDRIVE"], os.environ["HOMEPATH"] = os.path.splitdrive(str(_TEST_HOME))
for _key in tuple(os.environ):
    if _key.startswith(("BROKER_", "DISCORD_", "RELAY_")) and _key != "BROKER_TEST_OMP_NODE_MODULES":
        os.environ.pop(_key)
os.environ["BROKER_HOST"] = "127.0.0.1"
os.environ["BROKER_BASE_URL"] = "http://127.0.0.1:1"
os.environ["BROKER_DB_PATH"] = str(Path(_ISOLATION.name) / "coordination.db")
os.environ["BROKER_AUDIT_DIR"] = str(Path(_ISOLATION.name) / "audit")
os.environ["BROKER_AGENT_MESSAGE_ARCHIVE_DIR"] = str(Path(_ISOLATION.name) / "messages")
os.environ["RELAY_HOOKS_DIR"] = str(SOURCE_ROOT / "harness/claude/hooks/relay")
for _relative in ("src", "harness/codex", "harness/omp"):
    sys.path.insert(0, str(SOURCE_ROOT / _relative))
for _relative in (".agents/hooks/relay", ".claude/hooks/relay"):
    shutil.copytree(SOURCE_ROOT / "harness/claude/hooks/relay", _TEST_HOME / _relative)
shutil.copytree(SOURCE_ROOT / "harness/claude/local-relay-channel",
                _TEST_HOME / ".claude/plugins/local-relay-channel")
os.chdir(_ISOLATION.name)


@pytest_asyncio.fixture
async def client(tmp_path, monkeypatch):
    db_path = tmp_path / "coordination.db"
    monkeypatch.setenv("BROKER_DB_PATH", str(db_path))
    monkeypatch.setenv("BROKER_HOST", "127.0.0.1")
    monkeypatch.delenv("BROKER_BEARER_TOKEN", raising=False)
    monkeypatch.delenv("BROKER_ADMIN_ENABLED", raising=False)

    # Import after env is set; ensure_schema reads env at call time
    from broker import server

    await server.ensure_schema()

    transport = httpx.ASGITransport(app=server.app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest_asyncio.fixture
async def db_path(tmp_path, monkeypatch):
    """Just the env + schema setup, no client. For tests that drive DB directly."""
    p = tmp_path / "coordination.db"
    monkeypatch.setenv("BROKER_DB_PATH", str(p))
    from broker import server
    await server.ensure_schema()
    return str(p)


@pytest.fixture
def project_state_db(tmp_path):
    """Build a per-project state.db with the Pattern 1 schema + sample data."""
    import sqlite3
    project_dir = tmp_path / "test_project"
    project_dir.mkdir()
    state_db = project_dir / "state.db"
    conn = sqlite3.connect(str(state_db))
    conn.executescript("""
        CREATE TABLE pipelines (
          id TEXT PRIMARY KEY,
          shape TEXT NOT NULL,
          started_at INTEGER NOT NULL,
          completed_at INTEGER,
          status TEXT NOT NULL CHECK (status IN ('open','in_progress','blocked','done','failed','cancelled'))
        );
        CREATE TABLE pipeline_steps (
          pipeline_id TEXT NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
          step_index INTEGER NOT NULL,
          agent_type TEXT NOT NULL,
          status TEXT NOT NULL,
          output_path TEXT,
          last_task_id TEXT,
          inbox_path TEXT,
          output_artifact_paths TEXT,
          started_at INTEGER NOT NULL,
          completed_at INTEGER,
          PRIMARY KEY (pipeline_id, step_index)
        );
        CREATE TABLE pipeline_dependencies (
          pipeline_id TEXT NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
          depends_on_pipeline_id TEXT NOT NULL REFERENCES pipelines(id) ON DELETE RESTRICT,
          PRIMARY KEY (pipeline_id, depends_on_pipeline_id)
        );
    """)
    # Seed: one pipeline with 3 steps (1 done, 2 in_progress)
    conn.execute(
        "INSERT INTO pipelines (id, shape, started_at, status) VALUES ('p1', 'office_hours', 1000, 'in_progress')"
    )
    conn.executemany(
        "INSERT INTO pipeline_steps (pipeline_id, step_index, agent_type, status, started_at) "
        "VALUES (?, ?, ?, ?, ?)",
        [
            ("p1", 0, "demo_research", "done", 1000),
            ("p1", 1, "demo_writer", "in_progress", 1100),
            ("p1", 2, "demo_comms", "in_progress", 1200),
        ],
    )
    conn.commit()
    conn.close()
    return {"name": "test_project", "root_path": str(project_dir), "state_db_path": str(state_db)}
