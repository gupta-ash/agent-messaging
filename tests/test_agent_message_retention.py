"""v1.3 (D28): agent_message retention sweep tests.

Covers the daily background task that archives + deletes `kind='agent_message'`
rows older than `BROKER_AGENT_MESSAGE_RETENTION_DAYS`. The runner is hard to
exercise via lifespan timing (default interval is 86400s); instead we drive
the extracted `_agent_message_retention_sweep_once` helper directly per the
pattern established in test_audit_runner.py.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

pytestmark = pytest.mark.asyncio


async def _seed_message_log(server, *, kind: str, content: str, ts_ms: int,
                            from_agent: str = "A", to_agent: str = "B",
                            channel: str = "#c") -> int:
    async with server.connect_db() as conn:
        cur = await conn.execute(
            "INSERT INTO message_log "
            "(client_event_id, from_agent, to_agent, channel, thread_id, kind, content, ts) "
            "VALUES (NULL, ?, ?, ?, NULL, ?, ?, ?)",
            (from_agent, to_agent, channel, kind, content, ts_ms),
        )
        eid = cur.lastrowid
        await cur.close()
        await conn.commit()
    return eid


async def _count_agent_messages(server) -> int:
    async with server.connect_db() as conn:
        cur = await conn.execute(
            "SELECT COUNT(*) AS n FROM message_log WHERE kind='agent_message'"
        )
        row = await cur.fetchone()
        await cur.close()
    return row["n"]


async def _count_kind(server, kind: str) -> int:
    async with server.connect_db() as conn:
        cur = await conn.execute(
            "SELECT COUNT(*) AS n FROM message_log WHERE kind=?", (kind,)
        )
        row = await cur.fetchone()
        await cur.close()
    return row["n"]


async def test_retention_archives_old_messages(tmp_path, monkeypatch):
    """retention=0 days + an old agent_message row -> row archived to jsonl
    AND deleted from message_log."""
    monkeypatch.setenv("BROKER_DB_PATH", str(tmp_path / "coordination.db"))
    monkeypatch.setenv("BROKER_HOST", "127.0.0.1")
    monkeypatch.delenv("BROKER_BEARER_TOKEN", raising=False)

    from broker import server
    await server.ensure_schema()

    archive_dir = tmp_path / "archive-root"

    # Seed an old agent_message (ts well in the past so cutoff catches it).
    old_ts = server.now_ms() - 10_000
    eid = await _seed_message_log(
        server, kind="agent_message", content="old-message", ts_ms=old_ts,
    )
    assert await _count_agent_messages(server) == 1

    # Manually trigger one sweep iteration via the extracted helper.
    archived_count = await server._agent_message_retention_sweep_once(
        retention_days=0, archive_dir=archive_dir,
    )
    assert archived_count == 1

    # Verify archive jsonl exists and contains the row content.
    jsonl_files = list(archive_dir.glob("agent-messages-*.jsonl"))
    assert len(jsonl_files) == 1, f"expected one archive file, got {jsonl_files}"
    lines = jsonl_files[0].read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    row = json.loads(lines[0])
    assert row["content"] == "old-message"
    assert row["kind"] == "agent_message"
    assert row["event_id"] == eid

    # Verify row removed from message_log.
    assert await _count_agent_messages(server) == 0


async def test_retention_skips_recent_messages(tmp_path, monkeypatch):
    """retention=30 days + a current-ts agent_message row -> no archive file,
    row preserved in message_log."""
    monkeypatch.setenv("BROKER_DB_PATH", str(tmp_path / "coordination.db"))
    monkeypatch.setenv("BROKER_HOST", "127.0.0.1")
    monkeypatch.delenv("BROKER_BEARER_TOKEN", raising=False)

    from broker import server
    await server.ensure_schema()

    archive_dir = tmp_path / "archive-root-recent"

    # Seed a current-ts agent_message.
    await _seed_message_log(
        server, kind="agent_message", content="recent-message", ts_ms=server.now_ms(),
    )
    assert await _count_agent_messages(server) == 1

    archived_count = await server._agent_message_retention_sweep_once(
        retention_days=30, archive_dir=archive_dir,
    )
    assert archived_count == 0

    # No archive file should have been created (the dir may not exist either).
    if archive_dir.exists():
        jsonl_files = list(archive_dir.glob("agent-messages-*.jsonl"))
        assert jsonl_files == [], f"expected no archive files, got {jsonl_files}"

    # Row still present.
    assert await _count_agent_messages(server) == 1


async def test_retention_skips_other_kinds(tmp_path, monkeypatch):
    """retention=0 + an old `handoff` row -> handoff row preserved (only
    `kind='agent_message'` rows are subject to retention)."""
    monkeypatch.setenv("BROKER_DB_PATH", str(tmp_path / "coordination.db"))
    monkeypatch.setenv("BROKER_HOST", "127.0.0.1")
    monkeypatch.delenv("BROKER_BEARER_TOKEN", raising=False)

    from broker import server
    await server.ensure_schema()

    archive_dir = tmp_path / "archive-root-otherkind"

    old_ts = server.now_ms() - 10_000
    await _seed_message_log(
        server, kind="handoff", content="old-handoff", ts_ms=old_ts,
    )
    assert await _count_kind(server, "handoff") == 1

    archived_count = await server._agent_message_retention_sweep_once(
        retention_days=0, archive_dir=archive_dir,
    )
    assert archived_count == 0

    # handoff row preserved.
    assert await _count_kind(server, "handoff") == 1
    # No archive file (no agent_message rows hit cutoff).
    if archive_dir.exists():
        jsonl_files = list(archive_dir.glob("agent-messages-*.jsonl"))
        assert jsonl_files == []


# ---------- M3 (iter-2): partial-write idempotency ----------


async def test_retention_partial_write_failure_truncates_archive(tmp_path, monkeypatch):
    """M3 (iter-2): if file write fails mid-loop, archive is truncated back to
    pre-write size so the next sweep retry doesn't accumulate duplicate rows.

    Setup: seed 2 agent_message rows + write 1 line to the archive first to
    simulate a prior sweep. Then patch json.dumps to raise on the SECOND row
    so the first row is appended to the file BEFORE the failure surfaces. The
    M3 nested try/except must truncate the file back to the pre-write size
    AND roll back the DB so the rows remain available for the next retry.
    """
    monkeypatch.setenv("BROKER_DB_PATH", str(tmp_path / "coordination.db"))
    monkeypatch.setenv("BROKER_HOST", "127.0.0.1")
    monkeypatch.delenv("BROKER_BEARER_TOKEN", raising=False)

    from broker import server
    await server.ensure_schema()

    archive_dir = tmp_path / "audit"
    archive_dir.mkdir()
    archive_path = archive_dir / f"agent-messages-{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.jsonl"
    archive_path.write_text('{"prior": "sweep"}\n', encoding="utf-8")
    pre_size = archive_path.stat().st_size

    old_ts = server.now_ms() - 10_000
    await _seed_message_log(server, kind="agent_message", content="row-1", ts_ms=old_ts)
    await _seed_message_log(server, kind="agent_message", content="row-2", ts_ms=old_ts)

    # Patch json.dumps in the server module to raise on the SECOND row (after
    # row 1 has been written to file). This simulates a mid-loop write failure.
    real_dumps = json.dumps
    call_count = [0]

    def flaky_dumps(*args, **kwargs):
        call_count[0] += 1
        if call_count[0] == 2:
            raise IOError("simulated mid-loop failure")
        return real_dumps(*args, **kwargs)

    monkeypatch.setattr(server.json, "dumps", flaky_dumps)

    with pytest.raises(IOError):
        await server._agent_message_retention_sweep_once(
            retention_days=0, archive_dir=archive_dir,
        )

    # Archive must be truncated back to its pre-write size; rows must remain in DB.
    assert archive_path.stat().st_size == pre_size
    assert archive_path.read_text(encoding="utf-8") == '{"prior": "sweep"}\n'
    # Rows still in DB (rollback succeeded; the next sweep retry can drain them).
    async with server.connect_db() as conn:
        cur = await conn.execute(
            "SELECT COUNT(*) AS c FROM message_log WHERE kind='agent_message'"
        )
        row = await cur.fetchone()
        await cur.close()
        assert row["c"] == 2


# ---------- M4 (iter-2): dedicated archive dir env var ----------


async def test_retention_archive_dir_honors_env_var(monkeypatch, tmp_path):
    """M4 (iter-2): BROKER_AGENT_MESSAGE_ARCHIVE_DIR env var overrides the default."""
    custom_dir = tmp_path / "custom_archive"
    monkeypatch.setenv("BROKER_AGENT_MESSAGE_ARCHIVE_DIR", str(custom_dir))

    from broker import server
    assert server._agent_message_archive_dir() == custom_dir


async def test_retention_archive_dir_default_when_env_unset(monkeypatch):
    """M4 (iter-2): default archive dir is 'audit/agent-messages' when env var unset."""
    monkeypatch.delenv("BROKER_AGENT_MESSAGE_ARCHIVE_DIR", raising=False)

    from broker import server
    assert server._agent_message_archive_dir() == Path("audit/agent-messages")
