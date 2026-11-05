"""F-CC36 — integration test that DRIVES the lifespan so the connection pool is
live, then exercises the pooled + transient paths through `connect_db`.

Mirrors the lifespan-driving style in tests/test_audit_runner.py:
`async with server.lifespan(server.app):` with env set FIRST via monkeypatch.
No network, no real discord (no token => poller logs WARN + skips per existing
behavior), no extra product code added for the test.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from broker import server

pytestmark = pytest.mark.asyncio


async def test_lifespan_opens_and_closes_pool(tmp_path, monkeypatch):
    db_path = tmp_path / "coordination.db"
    monkeypatch.setenv("BROKER_DB_PATH", str(db_path))
    monkeypatch.setenv("BROKER_DB_POOL_SIZE", "2")
    monkeypatch.setenv("BROKER_HOST", "127.0.0.1")
    monkeypatch.delenv("BROKER_BEARER_TOKEN", raising=False)
    monkeypatch.delenv("BROKER_ADMIN_ENABLED", raising=False)
    # No discord token => poller skips at startup (existing behavior), keeping
    # lifespan startup fast + offline.
    monkeypatch.delenv("DISCORD_BOT_TOKEN", raising=False)

    # Before lifespan: no pool.
    assert server._db_pool is None

    async with server.lifespan(server.app):
        # Pool is live, opened FIRST in startup with the configured size.
        assert server._db_pool is not None
        assert len(server._db_pool._all) == 2
        pooled_connections = tuple(server._db_pool._all)

        # ensure_schema works while the pool is live (it goes through the
        # default-path => pooled connection).
        await server.ensure_schema()

        # Default paths use the live pool and foreign paths stay transient. Object
        # identity proves the branch directly without racing lifespan borrowers.
        async with server.connect_db() as conn:
            assert any(conn is pooled for pooled in pooled_connections)
            cur = await conn.execute("PRAGMA database_list")
            default_rows = await cur.fetchall()
            await cur.close()
            default_path = next(row[2] for row in default_rows if row[1] == "main")
            assert Path(default_path).resolve() == db_path.resolve()
            cur = await conn.execute(
                "SELECT COUNT(*) FROM sqlite_master "
                "WHERE type='table' AND name='agent_status'"
            )
            assert (await cur.fetchone())[0] == 1
            await cur.close()

        foreign_db = tmp_path / "other_state.db"
        async with server.connect_db(str(foreign_db)) as conn:
            assert all(conn is not pooled for pooled in pooled_connections)
            cur = await conn.execute("PRAGMA database_list")
            foreign_rows = await cur.fetchall()
            await cur.close()
            foreign_path = next(row[2] for row in foreign_rows if row[1] == "main")
            assert Path(foreign_path).resolve() == foreign_db.resolve()
            await conn.execute("CREATE TABLE foreign_marker (id INTEGER PRIMARY KEY)")
            await conn.commit()

        async with server.connect_db() as conn:
            assert any(conn is pooled for pooled in pooled_connections)
            cur = await conn.execute(
                "SELECT COUNT(*) FROM sqlite_master "
                "WHERE type='table' AND name='foreign_marker'"
            )
            assert (await cur.fetchone())[0] == 0
            await cur.close()

    # After lifespan exit: pool closed LAST and the global is cleared.
    assert server._db_pool is None
