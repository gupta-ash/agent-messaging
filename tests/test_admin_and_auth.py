"""Admin endpoint gating + bearer auth on non-loopback bind."""

from __future__ import annotations

import httpx
import pytest
import pytest_asyncio

pytestmark = pytest.mark.asyncio


# ----- /api/admin/audit-snapshot gating -----

async def test_admin_snapshot_blocked_when_disabled(client):
    r = await client.post("/api/admin/audit-snapshot", json={})
    assert r.status_code == 403


async def test_admin_snapshot_enabled_via_env(client, monkeypatch):
    monkeypatch.setenv("BROKER_ADMIN_ENABLED", "true")
    r = await client.post("/api/admin/audit-snapshot", json={})
    assert r.status_code == 200
    d = r.json()
    assert "output_path" in d and "event_count" in d and "snapshot_window_start_ts" in d


async def test_admin_snapshot_enabled_via_bearer(client, monkeypatch):
    monkeypatch.setenv("BROKER_BEARER_TOKEN", "secret-token-1")
    r = await client.post("/api/admin/audit-snapshot", json={},
                          headers={"Authorization": "Bearer secret-token-1"})
    assert r.status_code == 200


async def test_admin_snapshot_wrong_bearer(client, monkeypatch):
    monkeypatch.setenv("BROKER_BEARER_TOKEN", "secret-token-1")
    r = await client.post("/api/admin/audit-snapshot", json={},
                          headers={"Authorization": "Bearer wrong-token"})
    assert r.status_code == 403


async def test_admin_snapshot_writes_file(client, monkeypatch, tmp_path):
    monkeypatch.setenv("BROKER_ADMIN_ENABLED", "true")
    monkeypatch.setenv("BROKER_AUDIT_DIR", str(tmp_path / "audit"))
    # Seed an event
    await client.post("/api/event", json={
        "from_agent": "a1", "channel": "#x", "kind": "user_msg",
        "content": "hello", "ts": 1746400000000,
    })
    r = await client.post("/api/admin/audit-snapshot", json={})
    assert r.status_code == 200
    output_path = r.json()["output_path"]
    from pathlib import Path
    p = Path(output_path)
    assert p.exists()


# ----- Non-loopback bind requires bearer auth -----
# Note: lifespan startup auth-check is exercised in test_lifespan_auth (separate file path)
# Here we test middleware behavior with non-loopback BROKER_HOST.

@pytest_asyncio.fixture
async def non_loopback_client(tmp_path, monkeypatch):
    db_path = tmp_path / "coordination.db"
    monkeypatch.setenv("BROKER_DB_PATH", str(db_path))
    monkeypatch.setenv("BROKER_HOST", "0.0.0.0")
    monkeypatch.setenv("BROKER_BEARER_TOKEN", "test-bearer")
    monkeypatch.delenv("BROKER_ADMIN_ENABLED", raising=False)
    from broker import server
    await server.ensure_schema()
    transport = httpx.ASGITransport(app=server.app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


async def test_non_loopback_health_no_auth_needed(non_loopback_client):
    # Per contract: health is exempt from auth
    r = await non_loopback_client.get("/api/health")
    assert r.status_code == 200


async def test_non_loopback_missing_bearer_returns_401(non_loopback_client):
    r = await non_loopback_client.post("/api/event", json={
        "from_agent": "a", "channel": "#x", "kind": "user_msg",
        "content": "x", "ts": 1,
    })
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "missing_bearer"


async def test_non_loopback_wrong_bearer_returns_401(non_loopback_client):
    r = await non_loopback_client.post(
        "/api/event",
        json={"from_agent": "a", "channel": "#x", "kind": "user_msg",
              "content": "x", "ts": 1},
        headers={"Authorization": "Bearer wrong"},
    )
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "invalid_bearer"


async def test_non_loopback_correct_bearer_succeeds(non_loopback_client):
    r = await non_loopback_client.post(
        "/api/event",
        json={"from_agent": "a", "channel": "#x", "kind": "user_msg",
              "content": "x", "ts": 1},
        headers={"Authorization": "Bearer test-bearer"},
    )
    assert r.status_code == 200


async def test_lifespan_proceeds_non_loopback_without_token(tmp_path, monkeypatch):
    """Per D26 (2026-05-08; user override extending D21): non-loopback bind
    + no token NO LONGER refuses to start. Bearer is opt-in; absent token
    means the auth middleware passes everything through. This test pins the
    new contract; the old RuntimeError is gone from server.py L820-826."""
    db_path = tmp_path / "coordination.db"
    monkeypatch.setenv("BROKER_DB_PATH", str(db_path))
    monkeypatch.setenv("BROKER_HOST", "0.0.0.0")
    monkeypatch.delenv("BROKER_BEARER_TOKEN", raising=False)
    from broker import server
    # MUST NOT raise. Lifespan completes; broker is ready to serve.
    async with server.lifespan(server.app):
        pass


# ----- F12 (iter3 discriminator): request_id_middleware MUST run on bearer_auth
# short-circuit responses (401 missing/invalid bearer, 500 broker_misconfig).
# Pre-fix, request_id was the INNER middleware so bearer_auth's _err_response
# never had request.state.request_id set AND the response had no X-Request-ID
# header. The fix is registration-order-only: bearer_auth FIRST (inner),
# request_id SECOND (outer). These tests pin the runtime behavior.

import re

_UUID4_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)


async def test_request_id_header_present_on_missing_bearer_401(non_loopback_client):
    """F12: 401 missing_bearer response MUST carry a UUID-shaped X-Request-ID
    header (request_id_middleware ran on the way out)."""
    r = await non_loopback_client.post("/api/event", json={
        "from_agent": "a", "channel": "#x", "kind": "user_msg",
        "content": "x", "ts": 1,
    })
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "missing_bearer"
    rid_header = r.headers.get("X-Request-ID")
    assert rid_header is not None, (
        "F12 regression: 401 response missing X-Request-ID header — "
        "request_id_middleware did not run (likely middleware order bug)"
    )
    assert _UUID4_RE.match(rid_header), (
        f"F12 regression: X-Request-ID header is not a UUID4: {rid_header!r}"
    )


async def test_request_id_in_body_matches_header_on_invalid_bearer_401(non_loopback_client):
    """F12: 401 invalid_bearer body.error.request_id MUST equal the
    X-Request-ID header (NOT 'unknown')."""
    r = await non_loopback_client.post(
        "/api/event",
        json={"from_agent": "a", "channel": "#x", "kind": "user_msg",
              "content": "x", "ts": 1},
        headers={"Authorization": "Bearer wrong"},
    )
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "invalid_bearer"
    rid_header = r.headers.get("X-Request-ID")
    rid_body = r.json()["error"]["request_id"]
    assert rid_header is not None
    assert _UUID4_RE.match(rid_header), (
        f"F12 regression: X-Request-ID not a UUID4: {rid_header!r}"
    )
    assert rid_body != "unknown", (
        "F12 regression: body.error.request_id is 'unknown' — "
        "request.state.request_id was not set when _err_response ran"
    )
    assert rid_body == rid_header, (
        f"F12 regression: body request_id={rid_body!r} != header={rid_header!r}"
    )


async def test_request_id_present_on_broker_misconfig_500_REMOVED_PER_D26(tmp_path, monkeypatch):
    """OBSOLETE per D26 (2026-05-08; user override extending D21).

    Pre-D26: when BROKER_BEARER_TOKEN was unset on non-loopback bind, the
    auth middleware returned 500 broker_misconfig. F12 pinned that the
    request_id middleware ran on that 500 response.

    Post-D26: bearer is opt-in. Unset token = middleware bypasses entirely.
    The 500 broker_misconfig code path is dead; this test no longer has a
    behavior to pin. F12 coverage for request_id on auth-error responses is
    preserved by:
      - test_request_id_header_present_on_missing_bearer_401 (L137)
      - test_request_id_in_body_matches_header_on_invalid_bearer_401 (L159)

    Kept as a renamed no-op marker so a `git log -p -S broker_misconfig`
    audit surfaces this intentional removal rather than appearing as a
    silent test deletion.
    """
    pytest.skip("Removed per D26: broker_misconfig 500 path no longer exists "
                "(unset token = middleware bypass). F12 coverage preserved by "
                "missing_bearer + invalid_bearer 401 tests above.")
