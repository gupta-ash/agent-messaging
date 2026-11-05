"""Tests for GET /dashboard HTML serve + the read-only / XSS source guards."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from broker.dashboard import api as dashboard_api
from broker.dashboard import router as dashboard_router


def test_serve_dashboard_200(client):
    r = client.get("/dashboard")
    assert r.status_code == 200, r.text
    assert "text/html" in r.headers["content-type"]
    assert "Broker Dashboard" in r.text


def test_serve_dashboard_missing_file_500(seed_db, monkeypatch, tmp_path):
    # Point the package dir at an empty temp dir so dashboard.html is absent.
    monkeypatch.setattr(dashboard_api, "_PKG_DIR", tmp_path)
    app = FastAPI()
    app.include_router(dashboard_router)
    with TestClient(app, raise_server_exceptions=False) as c:
        r = c.get("/dashboard")
    assert r.status_code == 500


def test_dashboard_html_has_no_dangerous_dom_sinks():
    """XSS source-guard: data must go through textContent/createElement only.

    We forbid the assignment/call FORMS of the dangerous sinks (a bare 'innerHTML'
    word appears only in an explanatory comment, which is fine).
    """
    html = (Path(dashboard_api.__file__).parent / "dashboard.html").read_text(encoding="utf-8")
    forbidden = [
        r"\.innerHTML\s*=",
        r"\.outerHTML\s*=",
        r"insertAdjacentHTML\s*\(",
        r"document\.write\s*\(",
    ]
    for pat in forbidden:
        assert re.search(pat, html) is None, f"forbidden DOM sink found: {pat}"


def test_dashboard_html_opens_sse_with_cursor():
    """Replay-pollution guard: the SSE must be opened WITH a cursor param so the
    broker replays nothing historical (it replays only event_id > cursor)."""
    html = (Path(dashboard_api.__file__).parent / "dashboard.html").read_text(encoding="utf-8")
    assert re.search(r"""EventSource\(["'].*?/api/events\?cursor=""", html) is not None, \
        "EventSource must be opened with a ?cursor= param (SSE replay-pollution fix)"


def test_read_only_mode_rejects_writes(seed_db):
    """The dashboard's _connect_ro must open the DB read-only — writes must fail.

    Proves the read-only guarantee is enforced at the SQLite engine level, not
    just by convention.
    """
    import asyncio
    import sqlite3

    import aiosqlite

    async def _try_write():
        async with dashboard_api._connect_ro() as conn:
            await conn.execute(
                "INSERT INTO agent_status "
                "(id, kind, channel, status, last_activity_ts, registered_at) "
                "VALUES ('x', 'k', '#c', 'idle', 1, 1)"
            )
            await conn.commit()

    with pytest.raises((sqlite3.OperationalError, aiosqlite.OperationalError)):
        asyncio.run(_try_write())
