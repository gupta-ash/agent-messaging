"""Pytest fixtures for the dashboard package.

Everything runs WITHOUT the live broker:

- A temp coordination.db is seeded with the REAL agent_status / pipelines_view /
  message_log schemas (column names + CHECK enums copied from broker/schema.sql).
  pipelines_view has a FOREIGN KEY to project_registry(name), so a project_registry
  row is seeded first.
- BROKER_DB_PATH is monkeypatched (setenv) to point at that temp DB; the dashboard
  reads it in mode=ro at request time.
- The relay registry path module constant (dashboard.api._REGISTRY_PATH) is
  monkeypatched to a temp file (or left absent to exercise the degrade-to-{} path),
  and the module-level mtime cache is reset between tests.
- A FastAPI test-app factory mounts ONLY our router via include_router(router) and
  returns a synchronous TestClient (drives async routes via its own event loop;
  no live server, no pytest_asyncio needed).

These fixtures live INSIDE the package (dashboard/tests/) per the C-20 ownership
boundary — they never touch broker/tests/.
"""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

# Make the dashboard package importable when pytest is invoked from broker/.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from broker.dashboard import api as dashboard_api
from broker.dashboard import router as dashboard_router


# Real schema (verbatim column names + CHECK enums from broker/schema.sql).
_SCHEMA = """
CREATE TABLE agent_status (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  channel TEXT NOT NULL,
  thread_id TEXT,
  status TEXT NOT NULL CHECK (status IN ('idle','queued','running','blocked','done','failed')),
  current_task TEXT,
  capabilities TEXT,
  last_activity_ts INTEGER NOT NULL,
  registered_at INTEGER NOT NULL,
  display_name TEXT
);

CREATE TABLE message_log (
  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  client_event_id TEXT,
  from_agent TEXT NOT NULL,
  to_agent TEXT,
  channel TEXT NOT NULL,
  thread_id TEXT,
  kind TEXT NOT NULL,
  content TEXT NOT NULL,
  ts INTEGER NOT NULL
);

CREATE TABLE project_registry (
  project_id TEXT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  root_path TEXT NOT NULL,
  state_db_path TEXT NOT NULL,
  registered_at INTEGER NOT NULL,
  last_refresh_ts INTEGER,
  local_validation INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE pipelines_view (
  project_name TEXT NOT NULL,
  pipeline_id TEXT NOT NULL,
  shape TEXT NOT NULL,
  started_at INTEGER NOT NULL,
  completed_at INTEGER,
  status TEXT NOT NULL CHECK (status IN ('open','in_progress','blocked','done','failed','cancelled')),
  step_count INTEGER NOT NULL,
  steps_done INTEGER NOT NULL,
  last_refresh_ts INTEGER NOT NULL,
  PRIMARY KEY (project_name, pipeline_id),
  FOREIGN KEY (project_name) REFERENCES project_registry(name)
);
"""


def _make_db(path: Path) -> None:
    """Create an empty DB with the real schema at `path`."""
    conn = sqlite3.connect(str(path))
    try:
        conn.executescript(_SCHEMA)
        conn.execute(
            "INSERT INTO project_registry "
            "(project_id, name, root_path, state_db_path, registered_at, local_validation) "
            "VALUES ('proj-1', 'demo_project', '/tmp/demo', '/tmp/demo/state.db', 1, 1)"
        )
        conn.commit()
    finally:
        conn.close()


@pytest.fixture
def seed_db(tmp_path, monkeypatch):
    """Return a helper bound to a temp DB; BROKER_DB_PATH points at it.

    The helper exposes `.path` and `.exec(sql, params)` so individual tests can
    insert exactly the rows they need without sharing global seed data.
    """
    db_path = tmp_path / "coordination.db"
    _make_db(db_path)
    monkeypatch.setenv("BROKER_DB_PATH", str(db_path))

    # Default: point the registry constant at a non-existent file so the
    # default behavior is "no enrichment" (degrade to {}). Reset the mtime cache.
    monkeypatch.setattr(dashboard_api, "_REGISTRY_PATH", tmp_path / "no_registry.json")
    dashboard_api._registry_cache["mtime"] = None
    dashboard_api._registry_cache["data"] = {}

    class _Seed:
        path = db_path

        def exec(self, sql, params=()):
            conn = sqlite3.connect(str(db_path))
            try:
                conn.execute(sql, params)
                conn.commit()
            finally:
                conn.close()

        def set_registry(self, mapping):
            """Write a registry.json file and point the module constant at it."""
            reg_path = tmp_path / "registry.json"
            reg_path.write_text(json.dumps(mapping), encoding="utf-8")
            monkeypatch.setattr(dashboard_api, "_REGISTRY_PATH", reg_path)
            dashboard_api._registry_cache["mtime"] = None
            dashboard_api._registry_cache["data"] = {}

        def break_registry(self):
            """Write malformed JSON and point the constant at it (degrade path)."""
            reg_path = tmp_path / "registry.json"
            reg_path.write_text("{ this is not valid json", encoding="utf-8")
            monkeypatch.setattr(dashboard_api, "_REGISTRY_PATH", reg_path)
            dashboard_api._registry_cache["mtime"] = None
            dashboard_api._registry_cache["data"] = {}

    return _Seed()


@pytest.fixture
def client(seed_db):
    """Synchronous TestClient over a FastAPI app mounting only our router."""
    app = FastAPI()
    app.include_router(dashboard_router)
    with TestClient(app) as c:
        yield c
