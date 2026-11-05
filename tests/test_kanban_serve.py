"""GET /kanban.html — legacy board RETIRED (OQ-DASH-1, 2026-06-08).

The old kanban board was replaced by the canonical /dashboard (owned by the
broker_dashboard agent, served from the dashboard package). The /kanban.html
route now 307-redirects to /dashboard so existing bookmarks land on the new
board instead of 404. The prior serve/content/XSS/auth tests are obsolete (the
route no longer serves kanban.html content); /dashboard has its own suite in
dashboard/tests/.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.asyncio


async def test_kanban_redirects_to_dashboard(client):
    # AsyncClient does not auto-follow redirects by default, so we observe the
    # 307 + Location directly (the broker's side of the contract).
    r = await client.get("/kanban.html")
    assert r.status_code == 307, r.text
    assert r.headers.get("location") == "/dashboard", dict(r.headers)


async def test_kanban_redirect_target_resolves(client):
    # Following the redirect lands on the canonical board (200 HTML) — guards
    # against the redirect pointing at a broken/missing target.
    r = await client.get("/kanban.html", follow_redirects=True)
    assert r.status_code == 200, r.text
    assert "text/html" in r.headers.get("content-type", ""), dict(r.headers)
