"""
Coordination broker: HTTP API, SQLite writer, SSE and lifecycle jobs.

FastAPI + SQLite (aiosqlite) + SSE (sse-starlette) + file watcher (watchdog).

Implementation contracts and current architecture are in docs/ at repository root.
Schema is package-relative schema.sql; runtime DB/log/audit paths use selected cwd.

Source development: python -m broker from an isolated runtime cwd after package setup.
Installed launch: scripts/start-broker.ps1 -> root executable server.py -> main().
Do not launch uvicorn broker.server:app directly: it bypasses main()'s pre-bind
reaper configuration guard. The installed executable exposes no legacy module API.
"""

from __future__ import annotations

import asyncio
import atexit
import base64
import faulthandler
import json
import hashlib
import hmac
import logging
import logging.handlers
import math
import os
import re
import signal
import socket
import sqlite3
import sys
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, AsyncIterator, Optional, Union

import aiosqlite
import httpx
from fastapi import FastAPI, HTTPException, Header, Query, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator
from sse_starlette.sse import EventSourceResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from broker import addressing

# -------------------------------------------------------------------- Windows UTF-8
# Per ~/.agents behavior rules: subprocess Python on Windows must reconfigure stdio.
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

# -------------------------------------------------------------------- Config (env)

def _env_str(key: str, default: str) -> str:
    return os.environ.get(key, default)


def _env_int(key: str, default: int) -> int:
    raw = os.environ.get(key)
    return int(raw) if raw is not None else default


def _env_bool(key: str, default: bool) -> bool:
    raw = os.environ.get(key, "").lower()
    if raw in ("1", "true", "yes", "on"):
        return True
    if raw in ("0", "false", "no", "off"):
        return False
    return default


SCHEMA_PATH = Path(__file__).parent / "schema.sql"
LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1", "0:0:0:0:0:0:0:1"}
SQLITE_I64_MAX = 9223372036854775807  # Largest int that aiosqlite/SQLite can bind as INTEGER.
_OMP_SHARED_PATH_RE = re.compile(
    r"^/api/agent/[^/]+/(?P<operation>status|relay-name|delivery-ack)$"
)
_OMP_OFFER_PATH_RE = re.compile(
    r"^/api/agent/[^/]+/delivery-offer(?:-status)?$"
)
_OMP_DELIVERY_STATUS_PATH_RE = re.compile(
    r"^/api/agent/[^/]+/delivery/[^/]+/status$"
)

# -------------------------------------------------------------------- Logging

logger = logging.getLogger("broker")
if not logger.handlers:
    # F-CC21 (broker stability A1, 2026-05-26): per-process+per-level format so
    # crash forensics include pid. Add RotatingFileHandler so logs survive
    # stderr-redirect fd issues (e.g., post-resume Windows IOCP, autostart
    # Start-Process redirect bad state). 5×10MB rotation = 50MB cap.
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s [%(process)d]: %(message)s")
    log_dir = Path.cwd() / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    file_handler = logging.handlers.RotatingFileHandler(
        log_dir / "broker.log",
        maxBytes=10_485_760,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)
    # Keep stderr handler: preserves live console + autostart-redirect compatibility.
    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setFormatter(fmt)
    logger.addHandler(stream_handler)
    logger.setLevel(_env_str("BROKER_LOG_LEVEL", "INFO").upper())
    logger.propagate = False


# -------------------------------------------------------------------- Crash observability (F-CC21 A4 + A5)

# A4: faulthandler captures C-level crashes (segfault, SIGSEGV, SIGFPE, SIGBUS,
# SIGILL, abort()) that Python try/except cannot see.
# Stage C cleanup (2026-05-27): REMOVED `dump_traceback_later(timeout=300,
# repeat=True)` — that produced 5.9 MB of false-positive stuck-thread dumps
# because watchdog Observer threads legitimately BLOCK FOREVER in the Win32
# `read_directory_changes` syscall waiting for filesystem events. faulthandler
# can't distinguish that pattern from a deadlock. Keep crash-only capture;
# add a one-shot probe via a separate mechanism if event-loop deadlock
# detection becomes a real need.
# Guarded so pytest imports don't accumulate handles in test runs.
_FAULT_LOG_FD: Optional[Any] = None
if "pytest" not in sys.modules:
    try:
        _fault_log_path = Path.cwd() / "logs" / "broker.faulthandler"
        _fault_log_path.parent.mkdir(parents=True, exist_ok=True)
        _FAULT_LOG_FD = open(_fault_log_path, "a", buffering=1, encoding="utf-8")
        faulthandler.enable(file=_FAULT_LOG_FD, all_threads=True)
        # NO dump_traceback_later — see comment above.
    except Exception as e:  # noqa: BLE001 — diagnostic-only; never block startup
        logger.warning("faulthandler setup failed: %s", e, exc_info=True)

# A5: process-exit observability. atexit fires on clean interpreter teardown.
# Signal handlers fire on graceful SIGINT/SIGTERM/SIGBREAK; uvicorn may overwrite
# SIGINT+SIGTERM with its own handlers during server.run() — that's OK because
# (a) atexit ALWAYS fires regardless, (b) SIGBREAK (Windows Ctrl-Break) is NOT
# claimed by uvicorn so this captures it. Anything OS-killed (TerminateProcess
# / kill -9 equivalent) bypasses both — that's also diagnostic data because the
# absence of either log line means "crashed before signal handler could run."
def _on_atexit() -> None:
    logger.warning("broker process exiting (atexit handler fired) — last word")
    try:
        if _FAULT_LOG_FD is not None and not _FAULT_LOG_FD.closed:
            _FAULT_LOG_FD.flush()
    except Exception:  # noqa: BLE001
        pass

if "pytest" not in sys.modules:
    atexit.register(_on_atexit)

    def _on_signal(signum: int, frame) -> None:
        try:
            sig_name = signal.Signals(signum).name
        except (ValueError, AttributeError):
            sig_name = f"signal_{signum}"
        logger.warning("broker received %s (signum=%d) — shutdown initiated", sig_name, signum)
        # Restore default handler + re-raise so process exit proceeds normally.
        try:
            signal.signal(signum, signal.SIG_DFL)
        except (ValueError, OSError):
            pass
        try:
            os.kill(os.getpid(), signum)
        except Exception:  # noqa: BLE001
            pass

    # Windows-supported signals: SIGINT, SIGTERM, SIGBREAK. Best-effort installs
    # — failures (e.g., non-main-thread import) silently degrade.
    for _sig_name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        _sig = getattr(signal, _sig_name, None)
        if _sig is None:
            continue
        try:
            signal.signal(_sig, _on_signal)
        except (ValueError, OSError):
            pass


# -------------------------------------------------------------------- SQLite helpers

async def _apply_pragmas(conn: aiosqlite.Connection) -> None:
    await conn.execute("PRAGMA journal_mode=WAL")
    await conn.execute("PRAGMA busy_timeout=5000")
    await conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = aiosqlite.Row


@asynccontextmanager
async def connect_db(db_path: Optional[str] = None) -> AsyncIterator[aiosqlite.Connection]:
    """Yield an aiosqlite connection with broker PRAGMAs (WAL + busy_timeout + FK).

    F-CC36: for the DEFAULT coordination.db, hand out a POOLED long-lived connection
    when the startup pool is live (no per-request OS-thread spawn — the root-cause fix
    for the 2026-06-08 event-loop wedge). Foreign paths (project state.db) and the
    no-pool case (tests / pre-lifespan / startup) fall back to a transient connection
    — byte-identical to the prior behavior so existing call sites + tests are unaffected.
    """
    default_path = _env_str("BROKER_DB_PATH", "coordination.db")
    path = db_path or default_path
    pool = _db_pool
    if pool is not None and path == default_path:
        async with pool.acquire() as conn:
            yield conn
        return
    conn = await aiosqlite.connect(path)
    try:
        await _apply_pragmas(conn)
        yield conn
    finally:
        await conn.close()


_POOL_SENTINEL = object()


class _ConnectionPool:
    """Fixed-size pool of long-lived aiosqlite connections to the default DB.

    Exclusive checkout via an asyncio.Queue — only one coroutine holds a given
    connection at a time (an aiosqlite Connection serializes ops through ONE
    worker thread and must not be shared concurrently). Connections are opened
    ONCE at startup; aiosqlite spawns the worker thread only at connect() time,
    so steady state has ZERO per-request thread spawns (the wedge path).
    Awaiting an empty pool yields to the loop (never blocks it) — graceful
    degradation, not a wedge.

    Resource safety: `_all` tracks every live connection (including replacements)
    so close() closes them all; partial-open failure cleans up before raising;
    a returned connection is rolled back (no-op when no txn is open) so it never
    re-enters the pool mid-transaction; acquire() rejects use after close()
    begins, and any parked waiter is woken with a sentinel (never hangs through
    shutdown).
    """

    def __init__(self, path: str, size: int):
        self._path = path
        self._size = max(1, size)
        self._queue: asyncio.Queue = asyncio.Queue()
        self._all: list = []
        self._closed = False
        self._replacing = 0  # in-flight broken-conn replacement attempts (guards fail-closed)

    async def _new_conn(self) -> aiosqlite.Connection:
        conn = await aiosqlite.connect(self._path)
        try:
            await _apply_pragmas(conn)
        except BaseException:
            # connect() already spawned the worker thread; close it so a pragma
            # failure does not leak the thread (the very class this fix targets).
            try:
                await conn.close()
            except Exception:
                pass
            raise
        return conn

    async def open(self) -> None:
        try:
            for _ in range(self._size):
                conn = await self._new_conn()
                self._all.append(conn)
                self._queue.put_nowait(conn)
        except BaseException:
            # Boot fails loud (correct), but close any connections already
            # created so a partial open does not leak worker threads/handles.
            for conn in list(self._all):
                try:
                    await conn.close()
                except Exception:
                    pass
            self._all.clear()
            while not self._queue.empty():
                try:
                    self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
            raise
        logger.info("db pool opened: size=%d path=%s", self._size, self._path)

    async def close(self) -> None:
        self._closed = True
        conns = list(self._all)
        self._all.clear()
        for conn in conns:
            try:
                await conn.close()
            except Exception as e:  # noqa: BLE001 — best-effort teardown
                logger.warning("db pool close: %s", e)
        # Poison pill: a SINGLE sentinel suffices — acquire() re-enqueues it
        # before raising, so every parked waiter (any number) wakes and raises.
        self._queue.put_nowait(_POOL_SENTINEL)

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[aiosqlite.Connection]:
        if self._closed:
            raise RuntimeError("connection pool is closed")
        conn = await self._queue.get()
        if conn is _POOL_SENTINEL:
            # close() poison pill — re-enqueue so every other parked waiter wakes.
            self._queue.put_nowait(_POOL_SENTINEL)
            raise RuntimeError("connection pool is closed")
        if self._closed:
            # Pool closed while we were parked and we got a real (now-closed)
            # connection instead of the sentinel. Discard it, keep the pill
            # available, and signal closure.
            try:
                await conn.close()
            except Exception:
                pass
            self._queue.put_nowait(_POOL_SENTINEL)
            raise RuntimeError("connection pool is closed")
        try:
            yield conn
        finally:
            # shield so a second cancellation cannot interrupt the return and
            # silently shrink the pool (the connection is always requeued or
            # closed+replaced).
            await asyncio.shield(self._return(conn))

    async def _return(self, conn: aiosqlite.Connection) -> None:
        """Return a borrowed connection to the pool.

        Always rolls back first (a no-op when no transaction is open) so a
        connection never re-enters the pool mid-transaction — preserving the
        transient-close cleanup semantics regardless of how the borrower exited
        (normal, app-level exception, or cancellation). A connection that cannot
        be rolled back is closed and replaced (the ONLY thread-respawn path;
        rare), keeping `_all` accurate. `_closed` is re-checked after every await
        so a concurrent close() never causes a requeue into a dead pool. If a
        broken connection cannot be replaced AND no live connections remain, the
        pool is failed closed (waiters woken with the sentinel) so acquires fail
        fast instead of hanging forever; while connections remain it degrades
        gracefully (fewer slots). During shutdown the connection is closed.
        """
        if self._closed:
            try:
                await conn.close()
            except Exception:
                pass
            return
        try:
            await conn.rollback()
        except Exception:
            # Broken connection — discard it, then try to replace it.
            try:
                await conn.close()
            except Exception:
                pass
            try:
                self._all.remove(conn)
            except ValueError:
                pass
            if self._closed:
                return
            new = None
            self._replacing += 1
            try:
                new = await self._new_conn()
            except Exception as e:  # noqa: BLE001
                logger.error("db pool: replace broken conn failed: %s", e)
            finally:
                self._replacing -= 1
            if new is not None and not self._closed:
                self._all.append(new)
                self._queue.put_nowait(new)
                return
            if new is not None:
                # close() ran during _new_conn() — do not enqueue into a dead pool.
                try:
                    await new.close()
                except Exception:
                    pass
                return
            # Replacement failed. Fail the pool closed ONLY if no live connections
            # remain AND no other replacement is still in flight (a concurrent
            # replacement may yet restore service). While connections remain — or a
            # replacement is pending — degrade gracefully. Failing closed wakes
            # waiters with the sentinel so acquires fail fast instead of hanging.
            if not self._all and self._replacing == 0 and not self._closed:
                self._closed = True
                self._queue.put_nowait(_POOL_SENTINEL)
                logger.error(
                    "db pool: no live connections left and replacement is failing "
                    "— pool failed closed (restart required)"
                )
            return
        # rollback succeeded; re-check _closed after the await before requeueing.
        if self._closed:
            try:
                await conn.close()
            except Exception:
                pass
            return
        self._queue.put_nowait(conn)


_db_pool: Optional["_ConnectionPool"] = None


_foreign_conns: dict[str, aiosqlite.Connection] = {}
# Per-PATH locks (keyed by the SAME canonical key as _foreign_conns) — serialize all
# access to a given cached connection. The per-project _refresh_locks are NOT sufficient
# on their own: state_db_path is not UNIQUE in project_registry, so two project_names can
# map to ONE file; without a per-path lock their two project-locks would let two coroutines
# use the SAME single-worker-thread aiosqlite connection concurrently (corruption), and a
# concurrent cache-miss would double-open + leak. (Discriminator HIGH, 2026-06-08.)
_foreign_conn_locks: dict[str, asyncio.Lock] = {}


def _foreign_key(path: str) -> str:
    """Canonical cache/lock key for a foreign DB path (case/separator-normalized)."""
    try:
        return norm_path(path)
    except (OSError, ValueError):
        return path


@asynccontextmanager
async def connect_foreign_cached(path: str) -> AsyncIterator[aiosqlite.Connection]:
    """Yield a CACHED long-lived aiosqlite connection for a foreign (non-default) DB.

    F-CC36 follow-up: the per-project pipelines_view refresh reads each project's
    state.db. The transient connect_db(state_db_path) path spawned a fresh OS worker
    thread on the event-loop thread per refresh (the same un-timeout-able Thread.start()
    that wedged the loop under MemoryError). This caches ONE long-lived connection per
    canonical path and reuses it, so the worker thread is spawned at most once per path
    (warmed at startup via the refresh loop) instead of on every debounced refresh.

    SAFETY: a per-path lock (keyed by the same canonical key as the cache) is held across
    the whole block, so a given cached connection is never used concurrently — required by
    aiosqlite (one worker thread per Connection) — EVEN IF two project_names share one
    state.db file (state_db_path is not UNIQUE in the registry). On a connection error the
    connection is closed + evicted so the next call reopens.
    """
    key = _foreign_key(path)
    lock = _foreign_conn_locks.setdefault(key, asyncio.Lock())
    async with lock:
        conn = _foreign_conns.get(key)
        if conn is None:
            conn = await aiosqlite.connect(path)
            try:
                await _apply_pragmas(conn)
            except BaseException:
                try:
                    await conn.close()
                except Exception:
                    pass
                raise
            _foreign_conns[key] = conn
        try:
            yield conn
        finally:
            # Sanitize for reuse: clear any lingering (read) txn. A rollback failure means
            # the connection is broken -> evict + close so the next call reopens (the only
            # re-spawn path, on real failure only).
            try:
                await conn.rollback()
            except Exception:
                _foreign_conns.pop(key, None)
                try:
                    await conn.close()
                except Exception:
                    pass


async def evict_foreign_conn(path: str) -> None:
    """Close + drop the cached foreign connection for `path` (e.g. a re-register pointed a
    project at a different state.db). Acquires the per-path lock so it never closes a
    connection an in-flight refresh is using."""
    key = _foreign_key(path)
    lock = _foreign_conn_locks.get(key)
    if lock is None:
        _foreign_conns.pop(key, None)  # never opened (no lock) -> nothing live to close
        return
    async with lock:
        conn = _foreign_conns.pop(key, None)
        if conn is not None:
            try:
                await conn.close()
            except Exception as e:  # noqa: BLE001 — best-effort
                logger.warning("evict foreign conn (%s): %s", key, e)


async def close_foreign_conns() -> None:
    """Close + clear all cached foreign connections + their locks (lifespan shutdown)."""
    for p, conn in list(_foreign_conns.items()):
        try:
            await conn.close()
        except Exception as e:  # noqa: BLE001 — best-effort teardown
            logger.warning("foreign conn close (%s): %s", p, e)
    _foreign_conns.clear()
    _foreign_conn_locks.clear()


async def with_retry(fn, *args, max_attempts: int = 5, **kwargs):
    """Retry fn on SQLITE_BUSY with bounded backoff: 50/100/200/400/800ms x 5."""
    delays_ms = [50, 100, 200, 400, 800]
    last_err: Optional[BaseException] = None
    for attempt in range(max_attempts):
        try:
            return await fn(*args, **kwargs)
        except (sqlite3.OperationalError, aiosqlite.OperationalError) as e:
            msg = str(e).lower()
            if "locked" in msg or "busy" in msg:
                last_err = e
                if attempt < max_attempts - 1:
                    await asyncio.sleep(delays_ms[attempt] / 1000.0)
                    continue
            raise
    if last_err:
        raise last_err
    raise RuntimeError("retry exhausted with no error captured")


def normalize_ts(value: Union[int, str, None]) -> int:
    """Accept int (epoch-ms) or ISO-8601 string; return int epoch-ms.

    None raises ValueError — callers should provide a default before calling.

    Iter-7 fix: enforces 0 <= ts <= SQLITE_I64_MAX. Larger values would overflow
    aiosqlite's INTEGER bind and surface as 500 instead of 422.

    F1 (iter1 discriminator): error messages MUST NOT include the raw input value
    — Pydantic surfaces ValueError.args[0] in the validation error `msg` field,
    which crosses the response boundary even after we strip `input`/`ctx`.
    A token-shaped value passed as `ts` would otherwise echo back in the 422.
    """
    if value is None:
        raise ValueError("ts is required")
    if isinstance(value, int):
        if value < 0:
            raise ValueError("ts must be >= 0")
        if value > SQLITE_I64_MAX:
            raise ValueError(f"ts exceeds SQLite i64 max ({SQLITE_I64_MAX})")
        return value
    if isinstance(value, str):
        # ISO-8601 with optional fractional seconds + offset
        s = value.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(s)
        except ValueError as e:
            # Do NOT include `value` — token-shaped strings would leak via msg.
            raise ValueError("ts not a valid ISO-8601 string") from e
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        ms = int(dt.timestamp() * 1000)
        if ms < 0 or ms > SQLITE_I64_MAX:
            raise ValueError("ts ISO-8601 expansion out of i64 range")
        return ms
    raise ValueError(f"ts must be int (epoch-ms) or ISO-8601 string, got {type(value).__name__}")


def now_ms() -> int:
    return int(time.time() * 1000)

_OMP_AGENT_COLUMNS = {
    "display_name": "TEXT",
    "relay_name": "TEXT",
    "transport": "TEXT",
    "binding_epoch": "TEXT",
    "omp_process_epoch": "TEXT",
    "omp_instance_id": "TEXT",
    "control_epoch": "TEXT",
    "control_token_hash": "TEXT",
    "bind_attempt_id": "TEXT",
    "omp_mutation_seq": "INTEGER NOT NULL DEFAULT 0",
}

_OMP_R1_TRIGGERS = {
    "trg_delivery_fill_recipient_r1",
    "trg_delivery_binding_immutable_r1",
    "trg_delivery_terminal_immutable_r1",
    "trg_delivery_omp_insert_guard_r1",
    "trg_delivery_omp_status_guard_r1",
    "trg_delivery_omp_ack_guard_r1",
    "trg_delivery_stable_ack_guard_r1",
    "trg_delivery_rejection_check_insert_r1",
    "trg_delivery_rejection_check_update_r1",
    "trg_delivery_offer_immutable_r1",
    "trg_delivery_delete_guard_r1",
    "trg_delivery_offer_delete_guard_r1",
    "trg_delivery_terminal_winner_r1",
    "trg_offer_terminal_winner_r1",
    "trg_agent_ever_omp_update_r1",
    "trg_agent_ever_omp_delete_r1",
    "trg_agent_omp_binding_insert_r1",
    "trg_agent_omp_binding_update_r1",
    "trg_agent_namespace_seed_insert_r1",
    "trg_agent_namespace_seed_update_r1",
    "trg_agent_namespace_insert_r1",
    "trg_agent_namespace_update_r1",
    "trg_agent_stable_namespace_insert_r1",
    "trg_agent_stable_namespace_update_r1",
    "trg_agent_outstanding_delivery_delete_r1",
    "trg_agent_stable_history_rehome_r1",
    "trg_message_log_omp_bounds_r1",
    "trg_message_log_omp_update_guard_r1",
    "trg_message_log_delete_guard_r1",
    "trg_omp_history_update_r1",
    "trg_omp_history_delete_r1",
    "trg_omp_rejection_audit_update_r1",
    "trg_omp_rejection_audit_delete_r1",
}

_OMP_R1_SQL_TABLES = {
    "delivery", "delivery_offer", "idempotency_ledger", "omp_rejection_audit",
    "omp_relay_namespace",
}
_OMP_R1_SQL_INDEXES = {
    "idx_agent_status_relay_name", "idx_delivery_recipient_status_event",
    "idx_delivery_offer_recipient_state_event", "idx_idemp_omp_bind_epoch",
}
_OMP_EXPECTED_SCHEMA_SQL: Optional[dict[str, str]] = None


def _normalize_schema_definition(sql: str) -> str:
    without_create_guard = re.sub(
        r"\bIF\s+NOT\s+EXISTS\b", "", sql, flags=re.IGNORECASE
    )
    return re.sub(r"\s+", "", without_create_guard).lower()


def _expected_omp_schema_sql() -> dict[str, str]:
    """Build the exact R1 SQL-object fingerprint from this binary's schema source."""
    global _OMP_EXPECTED_SCHEMA_SQL
    if _OMP_EXPECTED_SCHEMA_SQL is not None:
        return _OMP_EXPECTED_SCHEMA_SQL
    names = _OMP_R1_SQL_TABLES | _OMP_R1_SQL_INDEXES | _OMP_R1_TRIGGERS
    expected_db = sqlite3.connect(":memory:")
    try:
        expected_db.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        expected_db.execute(
            "CREATE UNIQUE INDEX idx_agent_status_relay_name "
            "ON agent_status(relay_name) WHERE relay_name IS NOT NULL"
        )
        placeholders = ",".join("?" * len(names))
        rows = expected_db.execute(
            f"SELECT name,sql FROM sqlite_master WHERE name IN ({placeholders})",
            tuple(sorted(names)),
        ).fetchall()
    finally:
        expected_db.close()
    definitions = {
        name: _normalize_schema_definition(sql or "") for name, sql in rows
    }
    if definitions.keys() != names:
        missing = sorted(names - definitions.keys())
        raise RuntimeError(f"schema.sql missing R1 definitions: {missing}")
    _OMP_EXPECTED_SCHEMA_SQL = definitions
    return definitions

async def _execute_schema_sql(conn: aiosqlite.Connection, sql: str) -> None:
    """Execute schema.sql statement-by-statement without executescript's implicit COMMIT."""
    statement = ""
    for line in sql.splitlines(keepends=True):
        statement += line
        if sqlite3.complete_statement(statement):
            if statement.strip():
                await conn.execute(statement)
            statement = ""
    if statement.strip():
        raise RuntimeError("schema.sql ended with an incomplete statement")


async def _prepare_omp_r1_schema(conn: aiosqlite.Connection) -> None:
    """Upgrade pre-R1 tables before schema.sql creates R1 indexes/triggers.

    SQLite cannot add table CHECK constraints in place. Delivery and the legacy
    single-key idempotency ledger are therefore copied transactionally. Existing
    rows and numeric legacy projections are preserved; every row whose canonical
    relay name has a current owner receives that stable recipient. Genuinely
    unknown historical names remain unbound.
    """
    cur = await conn.execute(
        "SELECT name,sql FROM sqlite_master WHERE type='table'"
    )
    tables = {row["name"]: (row["sql"] or "") for row in await cur.fetchall()}
    await cur.close()

    # Trigger definitions are versioned code, not one-time CREATE artifacts. Drop and
    # recreate them in this same migration transaction so repaired guards take effect.
    for trigger in _OMP_R1_TRIGGERS:
        await conn.execute(f"DROP TRIGGER IF EXISTS {trigger}")

    if "agent_status" in tables:
        cur = await conn.execute("PRAGMA table_info(agent_status)")
        columns = {row["name"] for row in await cur.fetchall()}
        await cur.close()
        for name, sql_type in _OMP_AGENT_COLUMNS.items():
            if name not in columns:
                await conn.execute(
                    f"ALTER TABLE agent_status ADD COLUMN {name} {sql_type}"
                )

    if "delivery" in tables:
        cur = await conn.execute("PRAGMA table_info(delivery)")
        delivery_columns = {row["name"] for row in await cur.fetchall()}
        await cur.close()
        required = {
            "sender_id", "recipient_id", "omp_ack_seq", "rejected_ts",
            "reject_reason_code", "rejection_notice_event_id", "alerted_ts",
        }
        delivery_sql = tables["delivery"]
        needs_rebuild = (
            not required.issubset(delivery_columns)
            or "'rejected'" not in delivery_sql
            or any(reason not in delivery_sql for reason in OMP_REJECT_REASONS)
            or "reject_reason_code IS NOT NULL" not in delivery_sql
        )
        if needs_rebuild:
            for index in (
                "idx_delivery_outstanding", "idx_delivery_next_push",
                "idx_delivery_event_id", "idx_delivery_recipient_status_event",
            ):
                await conn.execute(f"DROP INDEX IF EXISTS {index}")
            await conn.execute("ALTER TABLE delivery RENAME TO delivery_pre_omp_r1")
            await conn.execute(
                "CREATE TABLE delivery ("
                "to_agent TEXT NOT NULL,event_id INTEGER NOT NULL,"
                "status TEXT NOT NULL DEFAULT 'outstanding' CHECK "
                "(status IN ('outstanding','acked','abandoned','rejected')),"
                "attempts INTEGER NOT NULL DEFAULT 0,next_push_ts INTEGER,"
                "created_ts INTEGER NOT NULL,updated_ts INTEGER NOT NULL,"
                "acked_ts INTEGER,alerted_ts INTEGER,sender_id TEXT,recipient_id TEXT,"
                "omp_ack_seq INTEGER NOT NULL DEFAULT 0,rejected_ts INTEGER,"
                "reject_reason_code TEXT CHECK "
                "(reject_reason_code IS NULL OR reject_reason_code IN "
                "('USER_REJECTED','PROTOCOL_INVALID','RECIPIENT_POLICY_REFUSED',"
                "'PERSISTENCE_UNRECOVERABLE','DELIVERY_UNCERTAIN_CONTENT_MAY_HAVE_BEEN_SEEN')),"
                "rejection_notice_event_id INTEGER,"
                "CHECK ((status='rejected')=(reject_reason_code IS NOT NULL)),"
                "PRIMARY KEY(to_agent,event_id))"
            )
            alerted_expr = "d.alerted_ts" if "alerted_ts" in delivery_columns else "NULL"
            sender_expr = "d.sender_id" if "sender_id" in delivery_columns else "NULL"
            stored_recipient = (
                "d.recipient_id" if "recipient_id" in delivery_columns else "NULL"
            )
            current_recipient_expr = (
                "(SELECT a.id FROM agent_status a "
                "WHERE lower(a.relay_name)=lower(d.to_agent))"
                if "agent_status" in tables else "NULL"
            )
            retained_recipient_expr = (
                "(SELECT n.owner_id FROM omp_relay_namespace n "
                "WHERE n.relay_name=d.to_agent)"
                if "omp_relay_namespace" in tables else "NULL"
            )
            recipient_expr = (
                f"COALESCE({stored_recipient},{retained_recipient_expr},"
                f"{current_recipient_expr})"
            )
            rejected_ts_expr = (
                "d.rejected_ts" if "rejected_ts" in delivery_columns else "NULL"
            )
            reject_reason_expr = (
                "d.reject_reason_code" if "reject_reason_code" in delivery_columns
                else "CASE WHEN d.status='rejected' THEN 'PROTOCOL_INVALID' ELSE NULL END"
            )
            notice_expr = (
                "d.rejection_notice_event_id"
                if "rejection_notice_event_id" in delivery_columns else "NULL"
            )
            ack_seq_expr = (
                "d.omp_ack_seq" if "omp_ack_seq" in delivery_columns else "0"
            )
            await conn.execute(
                "INSERT INTO delivery "
                "(to_agent,event_id,status,attempts,next_push_ts,created_ts,updated_ts,"
                "acked_ts,alerted_ts,sender_id,recipient_id,omp_ack_seq,rejected_ts,"
                "reject_reason_code,rejection_notice_event_id) "
                "SELECT d.to_agent,d.event_id,d.status,d.attempts,d.next_push_ts,"
                f"d.created_ts,d.updated_ts,d.acked_ts,{alerted_expr},{sender_expr},"
                f"{recipient_expr},{ack_seq_expr},{rejected_ts_expr},"
                f"{reject_reason_expr},{notice_expr} FROM delivery_pre_omp_r1 d"
            )
            await conn.execute("DROP TABLE delivery_pre_omp_r1")

    if "idempotency_ledger" in tables:
        cur = await conn.execute("PRAGMA table_info(idempotency_ledger)")
        idemp_info = await cur.fetchall()
        await cur.close()
        idemp_columns = {row["name"] for row in idemp_info}
        idemp_pk = [
            name for _position, name in sorted(
                (row["pk"], row["name"]) for row in idemp_info if row["pk"]
            )
        ]
        required_idemp = {
            "namespace", "client_event_id", "endpoint", "agent_id",
            "response_event_id", "request_hash", "response_status", "response_json",
            "new_binding_epoch", "created_ts",
        }
        if not required_idemp.issubset(idemp_columns) or idemp_pk != [
            "namespace", "client_event_id"
        ]:
            await conn.execute("DROP INDEX IF EXISTS idx_idemp_created_ts")
            await conn.execute("DROP INDEX IF EXISTS idx_idemp_omp_bind_epoch")
            await conn.execute(
                "ALTER TABLE idempotency_ledger RENAME TO idempotency_ledger_pre_omp_r1"
            )
            await conn.execute(
                "CREATE TABLE idempotency_ledger ("
                "namespace TEXT NOT NULL DEFAULT 'legacy',"
                "client_event_id TEXT NOT NULL,endpoint TEXT NOT NULL,agent_id TEXT,"
                "response_event_id INTEGER,request_hash TEXT,response_status INTEGER,"
                "response_json TEXT,new_binding_epoch TEXT,created_ts INTEGER NOT NULL,"
                "PRIMARY KEY(namespace,client_event_id))"
            )
            expressions = {
                "namespace": "COALESCE(namespace,'legacy')" if "namespace" in idemp_columns else "'legacy'",
                "client_event_id": "client_event_id",
                "endpoint": "endpoint",
                "agent_id": "agent_id" if "agent_id" in idemp_columns else "NULL",
                "response_event_id": (
                    "response_event_id" if "response_event_id" in idemp_columns else "NULL"
                ),
                "request_hash": "request_hash" if "request_hash" in idemp_columns else "NULL",
                "response_status": (
                    "response_status" if "response_status" in idemp_columns else "NULL"
                ),
                "response_json": "response_json" if "response_json" in idemp_columns else "NULL",
                "new_binding_epoch": (
                    "new_binding_epoch" if "new_binding_epoch" in idemp_columns else "NULL"
                ),
                "created_ts": "created_ts",
            }
            ordered = [
                "namespace", "client_event_id", "endpoint", "agent_id",
                "response_event_id", "request_hash", "response_status", "response_json",
                "new_binding_epoch", "created_ts",
            ]
            await conn.execute(
                "INSERT INTO idempotency_ledger (" + ",".join(ordered) + ") SELECT "
                + ",".join(expressions[name] for name in ordered)
                + " FROM idempotency_ledger_pre_omp_r1"
            )
            await conn.execute("DROP TABLE idempotency_ledger_pre_omp_r1")



async def ensure_schema(db_path: Optional[str] = None) -> None:
    sql = SCHEMA_PATH.read_text(encoding="utf-8")
    async with connect_db(db_path) as conn:
        await conn.execute("BEGIN IMMEDIATE")
        await _prepare_omp_r1_schema(conn)
        await _execute_schema_sql(conn, sql)
        # Retain every OMP-controlled namespace across rename and rollback. Latest
        # historical stable ownership seeds an unowned alias; an active OMP owner
        # is authoritative when the same name was later taken over.
        namespace_now = now_ms()
        await conn.execute(
            "INSERT OR IGNORE INTO omp_relay_namespace "
            "(relay_name,owner_id,created_ts,updated_ts) "
            "SELECT d.to_agent,d.recipient_id,?,? FROM delivery d "
            "JOIN agent_status a ON a.id=d.recipient_id "
            "WHERE a.binding_epoch IS NOT NULL "
            "AND length(d.to_agent) BETWEEN 1 AND 64 "
            "AND d.to_agent NOT GLOB '*[^A-Za-z0-9._-]*' "
            "ORDER BY d.event_id DESC",
            (namespace_now, namespace_now),
        )
        await conn.execute(
            "INSERT OR IGNORE INTO omp_relay_namespace "
            "(relay_name,owner_id,created_ts,updated_ts) "
            "SELECT relay_name,id,?,? FROM agent_status "
            "WHERE binding_epoch IS NOT NULL AND relay_name IS NOT NULL",
            (namespace_now, namespace_now),
        )
        await conn.execute(
            "UPDATE omp_relay_namespace SET "
            "owner_id=(SELECT a.id FROM agent_status a "
            "          WHERE a.binding_epoch IS NOT NULL "
            "            AND a.relay_name=omp_relay_namespace.relay_name),"
            "updated_ts=? WHERE EXISTS (SELECT 1 FROM agent_status a "
            "WHERE a.binding_epoch IS NOT NULL "
            "AND a.relay_name=omp_relay_namespace.relay_name "
            "AND a.id<>omp_relay_namespace.owner_id)",
            (namespace_now,),
        )
        # D27 iter-2 (M1): in-place migration for existing coordination.db files
        # that were created before the local_validation column was added. Idempotent —
        # PRAGMA table_info returns the live column list so re-running is a no-op.
        # Default 1 preserves all existing rows as local-validation rows (the only
        # mode that existed before D27 v1.2). ALTER TABLE ADD COLUMN is the
        # SQLite-supported additive migration path; DROP/RECREATE would lose rows.
        cur = await conn.execute("PRAGMA table_info(project_registry)")
        cols = {row["name"] for row in await cur.fetchall()}
        await cur.close()
        if "local_validation" not in cols:
            await conn.execute(
                "ALTER TABLE project_registry "
                "ADD COLUMN local_validation INTEGER NOT NULL DEFAULT 1"
            )
        # D29 (v1.3.1): symmetric in-place migration for the new agent_status
        # display_name column. Mirrors the D27 pattern at the same code site —
        # PRAGMA table_info detect + conditional ALTER TABLE ADD COLUMN. Default
        # is NULL (column added without DEFAULT clause), preserving every
        # existing row as "no display name set"; new POSTs that include the
        # field overwrite normally via the existing UPSERT.
        cur = await conn.execute("PRAGMA table_info(agent_status)")
        agent_cols = {row["name"] for row in await cur.fetchall()}
        await cur.close()
        if "display_name" not in agent_cols:
            await conn.execute(
                "ALTER TABLE agent_status ADD COLUMN display_name TEXT"
            )
            agent_cols.add("display_name")
        # R1: product-neutral identity/transport migration. The partial unique index is
        # the completion marker, so an interruption after either additive ALTER resumes
        # the deterministic winner selection before the index is created.
        if "relay_name" not in agent_cols:
            await conn.execute("ALTER TABLE agent_status ADD COLUMN relay_name TEXT")
            agent_cols.add("relay_name")
        if "transport" not in agent_cols:
            await conn.execute("ALTER TABLE agent_status ADD COLUMN transport TEXT")
            agent_cols.add("transport")
        cur = await conn.execute("PRAGMA index_list(agent_status)")
        agent_indexes = {row["name"] for row in await cur.fetchall()}
        await cur.close()
        if "idx_agent_status_relay_name" not in agent_indexes:
            cur = await conn.execute(
                "SELECT id, display_name, last_activity_ts, registered_at "
                "FROM agent_status WHERE kind='cc-session' AND channel='#cc-relay' "
                "ORDER BY last_activity_ts DESC, registered_at DESC, id ASC"
            )
            claude_rows = await cur.fetchall()
            await cur.close()
            winners: list[tuple[str, str]] = []
            displaced: list[str] = []
            claimed: set[str] = set()
            for row in claude_rows:
                relay_name = addressing.canonicalize_relay_name(row["display_name"])
                if relay_name is None:
                    continue
                if relay_name in claimed:
                    displaced.append(row["id"])
                    continue
                claimed.add(relay_name)
                winners.append((relay_name, row["id"]))
            if winners:
                await conn.executemany(
                    "UPDATE agent_status SET relay_name=?, transport='claude-code' "
                    "WHERE id=?",
                    winners,
                )
            if displaced:
                logger.warning(
                    "ensure_schema: relay-name migration displaced duplicate Claude ids: %s",
                    ",".join(displaced),
                )
            await conn.execute(
                "CREATE UNIQUE INDEX idx_agent_status_relay_name "
                "ON agent_status(relay_name) WHERE relay_name IS NOT NULL"
            )
        # APPENDIX 2 reverse migration: project column REMOVED post-cwd-derive-revert.
        # Existing v1.5 DBs that had project added via the old PRAGMA-detect path
        # need to drop it now. PRAGMA-detect: if column present, DROP COLUMN +
        # DROP INDEX (both idempotent). SQLite >= 3.35 supports DROP COLUMN; on
        # older SQLite the DROP fails — caught + logged + column stays as a
        # harmless orphan (always NULL after we removed it from the POST body
        # + Pydantic model + UPSERT clause).
        cur = await conn.execute("PRAGMA table_info(agent_status)")
        agent_cols = {row["name"] for row in await cur.fetchall()}
        await cur.close()
        if "project" in agent_cols:
            try:
                await conn.execute("DROP INDEX IF EXISTS idx_agent_status_project")
                await conn.execute("ALTER TABLE agent_status DROP COLUMN project")
            except Exception as e:
                # Old SQLite (< 3.35) doesn't support DROP COLUMN. Leave column
                # in place; it's a harmless orphan now that nothing reads/writes it.
                logger.warning(
                    "ensure_schema: could not drop agent_status.project column "
                    "(%s: %s); leaving as harmless orphan", type(e).__name__, e
                )
        # WS-B-broker-1: one-time cutover backfill of the delivery table from the
        # historical directed message_log. Guarded by PRAGMA user_version (run-once).
        # Historical rows seed as 'acked' — the
        # pre-cutover client-side floor (D53/D56) still delivers any in-flight tail
        # until WS-B-client, so `outstanding` starts at 0 and the WS-B-broker-2
        # re-push loop only ever re-pushes messages created AFTER cutover. Never
        # blanket-drops a genuinely-unread message.
        cur = await conn.execute("PRAGMA user_version")
        uv_row = await cur.fetchone()
        await cur.close()
        uv = uv_row[0] if uv_row is not None else 0
        # Recipient adoption is a one-time schema migration, including terminal
        # history. Suspend only the runtime immutability guard while all versioned
        # backfills run, then recreate its exact schema definition before commit.
        recreate_binding_guard = uv < 5
        if recreate_binding_guard:
            await conn.execute("DROP TRIGGER IF EXISTS trg_delivery_binding_immutable_r1")
        if uv < 1:
            _bnow = now_ms()
            await conn.execute(
                "INSERT OR IGNORE INTO delivery "
                "(to_agent, event_id, status, attempts, created_ts, updated_ts, acked_ts) "
                "SELECT to_agent, event_id, 'acked', 0, ?, ?, ? FROM message_log "
                "WHERE to_agent IS NOT NULL "
                "AND kind IN ('agent_message', 'agent_reply', 'handoff')",
                (_bnow, _bnow, _bnow),
            )
            await conn.execute("PRAGMA user_version = 1")
            cur = await conn.execute(
                "SELECT COUNT(*) AS n, "
                "SUM(CASE WHEN status='outstanding' THEN 1 ELSE 0 END) AS outstanding "
                "FROM delivery"
            )
            brow = await cur.fetchone()
            await cur.close()
            logger.info(
                "ensure_schema: delivery cutover backfill seeded %s rows "
                "(outstanding=%s) [PRAGMA user_version 0->1]",
                (brow["n"] if brow else 0),
                ((brow["outstanding"] or 0) if brow else 0),
            )
        # WS-B-broker-2 (#18): escalation-reaper column + one-time cutover reconcile.
        # Guarded by PRAGMA user_version (1 -> 2, run-once). Two parts:
        #  (1) additive nullable `alerted_ts` on delivery — idempotent PRAGMA-detect +
        #      conditional ALTER (matching the D27/D29 pattern above). A FRESH DB already
        #      has the column from schema.sql's CREATE TABLE, so the detect skips the
        #      ALTER; an EXISTING delivery table (created before #18) gets it added.
        #  (2) cutover reconcile — stamp every CURRENTLY-outstanding row alerted_ts=now
        #      so the reaper starts clean and never escalates the pre-#18 backlog (rows
        #      accrued between the D71 ship and reaper-enable, incl. any D72
        #      rename-after-delivery residuals present now). Option-A philosophy (D71):
        #      pre-enable delivery was covered by the old client-side floor (D53/D56);
        #      we can't retroactively prove those were processed, so SUPPRESS rather
        #      than false-escalate. A genuine late ack still flips such a row to 'acked'.
        #      The reaper's own `alerted_ts IS NULL` filter then naturally skips them.
        if uv < 2:
            cur = await conn.execute("PRAGMA table_info(delivery)")
            delivery_cols = {row["name"] for row in await cur.fetchall()}
            await cur.close()
            if "alerted_ts" not in delivery_cols:
                await conn.execute("ALTER TABLE delivery ADD COLUMN alerted_ts INTEGER")
            _rnow = now_ms()
            cur = await conn.execute(
                "UPDATE delivery SET alerted_ts = ? "
                "WHERE status = 'outstanding' AND alerted_ts IS NULL",
                (_rnow,),
            )
            _reconciled = cur.rowcount
            await cur.close()
            await conn.execute("PRAGMA user_version = 2")
            logger.info(
                "ensure_schema: reaper cutover reconcile stamped %s pre-#18 outstanding "
                "row(s) alerted_ts=%s [PRAGMA user_version 1->2]",
                _reconciled, _rnow,
            )
        if uv < 3:
            # One migration-time resolution only. Every row whose current canonical
            # mailbox owner is known keeps legacy history continuity; genuinely unknown
            # names stay NULL forever and are never inherited by a later claimant.
            await conn.execute(
                "UPDATE delivery SET recipient_id=("
                "SELECT a.id FROM agent_status a "
                "WHERE lower(a.relay_name)=lower(delivery.to_agent)) "
                "WHERE recipient_id IS NULL AND EXISTS ("
                "SELECT 1 FROM agent_status a "
                "WHERE lower(a.relay_name)=lower(delivery.to_agent))"
            )
            await conn.execute("PRAGMA user_version = 3")
        if uv < 4:
            # R1 credentialed ACK authority begins at this broker version. Earlier
            # rebuild candidates stamped legacy acked rows as 1 even though no R1
            # credential was presented. Reset only never-OMP recipients; accepted
            # OMP ACKs retain their broker-owned sequence marker.
            await conn.execute(
                "UPDATE delivery SET omp_ack_seq=0 "
                "WHERE omp_ack_seq<>0 AND NOT EXISTS ("
                "SELECT 1 FROM agent_status a WHERE a.id=delivery.recipient_id "
                "AND a.binding_epoch IS NOT NULL)"
            )
            await conn.execute("PRAGMA user_version = 4")
        if uv < 5:
            # An exact stable ID is an owned fallback address, not a claimable relay
            # name. Adopt only rows whose address already equals a present identity;
            # genuinely unknown historical names remain unbound forever.
            await conn.execute(
                "UPDATE delivery SET recipient_id=to_agent "
                "WHERE recipient_id IS NULL AND EXISTS ("
                "SELECT 1 FROM agent_status a WHERE a.id=delivery.to_agent)"
            )
            await conn.execute("PRAGMA user_version = 5")
        if recreate_binding_guard:
            await _execute_schema_sql(conn, sql)
        await conn.commit()


# -------------------------------------------------------------------- Pydantic models

VALID_AGENT_STATUSES = {"idle", "queued", "running", "blocked", "done", "failed"}
# DOWNSTREAM MIRROR (2026-08-04): `dashboard/api.py` `_MESSAGE_KINDS` re-declares this exact
# set to gate /api/dashboard/messages, because `message_log.kind` has NO CHECK constraint
# (measured in schema.sql: message_log 0, agent_status 1, pipelines_view 1) so the DB cannot
# be the source of truth -- this set is. The dashboard package deliberately never imports
# `server` (see the mount note at the bottom of this file), so the two cannot be collapsed
# without a third module. WIDENING THIS SET? Tell `broker_dashboard` in the same wave, or
# their endpoint returns 422 for the new kind. The divergence fails LOUD and self-naming,
# which is why the mirror was accepted over a shared import (C-34 duplication vs C-45 scale).
VALID_EVENT_KINDS = {"user_msg", "agent_reply", "status", "handoff", "tool_call", "error", "pipeline_event", "permission_reply", "agent_message"}
# v1.7 (Wave 6 Track 4): severity enum for the universal `error` event envelope.
VALID_ERROR_SEVERITIES = {"info", "warn", "error", "critical"}
# WS-B-broker-1: the point-to-point relay kinds that get a per-recipient delivery
# shadow-row (the 3 inter-session kinds the receive hook polls, D39). Broadcast /
# status / tool_call / etc. never get a delivery row.
DELIVERY_KINDS = {"agent_message", "agent_reply", "handoff"}

OMP_REJECT_REASONS = {
    "USER_REJECTED",
    "PROTOCOL_INVALID",
    "RECIPIENT_POLICY_REFUSED",
    "PERSISTENCE_UNRECOVERABLE",
    "DELIVERY_UNCERTAIN_CONTENT_MAY_HAVE_BEEN_SEEN",
}
OMP_OPERATOR_REASONS = {
    "PROTOCOL_INVALID",
    "RECIPIENT_POLICY_REFUSED",
    "PERSISTENCE_UNRECOVERABLE",
}
OMP_TERMINAL_STATES = {"accepted", "rejected"}
OMP_TRIGGER_ERRORS = {
    "omp_bound_kind", "omp_bound_type", "omp_bound_agent", "omp_bound_relay_name",
    "omp_bound_channel", "omp_bound_thread", "omp_bound_client_event",
    "omp_bound_ts", "omp_bound_content", "omp_reject_reason_invalid",
}


class _OmpFailure(Exception):
    def __init__(self, status_code: int, code: str, details: Any = None):
        super().__init__(code)
        self.status_code = status_code
        self.code = code
        self.details = details


def _omp_error_payload(request: Request, code: str, details: Any = None) -> dict[str, Any]:
    rid = getattr(request.state, "request_id", None) or str(uuid.uuid4())
    return {
        "schema_version": 1,
        "error": {
            "code": code,
            "message": code,
            "request_id": rid,
            "details": details,
        },
    }


def _omp_error(
    request: Request, status_code: int, code: str, details: Any = None
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content=_omp_error_payload(request, code, details),
    )


def _omp_failure_response(request: Request, exc: _OmpFailure) -> JSONResponse:
    return _omp_error(request, exc.status_code, exc.code, exc.details)


def _closed_object(body: Any, fields: set[str]) -> dict[str, Any]:
    if not isinstance(body, dict) or set(body) != fields:
        raise _OmpFailure(422, "PROTOCOL_INVALID")
    return body


class _SharedJsonObject(dict):
    """Last-value mapping plus original pairs for shared-route classification."""

    def __init__(self, pairs: list[tuple[str, Any]]):
        super().__init__(pairs)
        self.pairs = pairs


def _decode_shared_json_bytes(raw: bytes) -> Any:
    """Decode enough to classify a shared route without discarding duplicate hints."""
    try:
        return json.loads(raw, object_pairs_hook=_SharedJsonObject)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


def _decode_json_bytes(raw: bytes) -> Any:
    """Decode one JSON value while rejecting duplicate object members."""
    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise _OmpFailure(422, "PROTOCOL_INVALID")
            result[key] = value
        return result

    try:
        return json.loads(raw, object_pairs_hook=unique_object)
    except _OmpFailure:
        raise
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise _OmpFailure(422, "PROTOCOL_INVALID") from exc


async def _decode_json_body(request: Request) -> Any:
    return _decode_json_bytes(await request.body())


def _is_omp_shared_payload(
    operation: Optional[str], authorization: Optional[str], payload: Any
) -> bool:
    if authorization and authorization.startswith("RelayControl "):
        return True
    if not isinstance(payload, dict):
        return True
    pairs = (
        payload.pairs
        if isinstance(payload, _SharedJsonObject)
        else list(payload.items())
    )
    if operation == "status":
        return any(
            (key == "transport" and value == "omp-mcp")
            or (key == "operation" and value in {"bind", "refresh"})
            for key, value in pairs
        )
    return any(key == "schema_version" for key, _ in pairs)


def _canonical_uuid(value: Any, *, nullable: bool = False) -> Optional[str]:
    if nullable and value is None:
        return None
    if not isinstance(value, str):
        raise _OmpFailure(422, "PROTOCOL_INVALID")
    try:
        parsed = str(uuid.UUID(value))
    except (ValueError, AttributeError, TypeError) as exc:
        raise _OmpFailure(422, "PROTOCOL_INVALID") from exc
    if parsed != value:
        raise _OmpFailure(422, "PROTOCOL_INVALID")
    return value


def _canonical_event_id(value: Any, *, cursor: bool = False) -> int:
    if not isinstance(value, str):
        raise _OmpFailure(422, "PROTOCOL_INVALID")
    if cursor and value == "0":
        return 0
    if not re.fullmatch(r"[1-9][0-9]{0,18}", value):
        raise _OmpFailure(422, "PROTOCOL_INVALID")
    parsed = int(value)
    if parsed > SQLITE_I64_MAX:
        raise _OmpFailure(422, "PROTOCOL_INVALID")
    return parsed


def _canonical_epoch_ms(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise _OmpFailure(422, "PROTOCOL_INVALID")
    if value < 0 or value > 9_007_199_254_740_991:
        raise _OmpFailure(422, "PROTOCOL_INVALID")
    return value


def _is_unicode_scalar_text(value: Any) -> bool:
    return isinstance(value, str) and all(
        not (0xD800 <= ord(character) <= 0xDFFF) for character in value
    )


def _canonical_hash(value: Any) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise _OmpFailure(422, "PROTOCOL_INVALID")
    return value


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _request_digest(method: str, route: str, path: dict[str, Any], body: dict[str, Any]) -> str:
    canonical = _canonical_json(
        {"method": method, "route": route, "path": path, "body": body}
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _relay_control_token_hash(authorization: Optional[str]) -> str:
    if authorization is None:
        raise _OmpFailure(401, "CONTROL_REQUIRED")
    if not authorization.startswith("RelayControl "):
        raise _OmpFailure(403, "CONTROL_INVALID")
    token = authorization[len("RelayControl "):]
    try:
        padding = "=" * ((4 - len(token) % 4) % 4)
        raw = base64.urlsafe_b64decode((token + padding).encode("ascii"))
    except (ValueError, UnicodeEncodeError) as exc:
        raise _OmpFailure(403, "CONTROL_INVALID") from exc
    if len(raw) != 32 or base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=") != token:
        raise _OmpFailure(403, "CONTROL_INVALID")
    return hashlib.sha256(raw).hexdigest()


async def _require_current_control(
    conn: aiosqlite.Connection,
    recipient_id: str,
    presented_hash: str,
    binding_epoch: str,
    omp_process_epoch: str,
) -> aiosqlite.Row:
    cur = await conn.execute("SELECT * FROM agent_status WHERE id=?", (recipient_id,))
    row = await cur.fetchone()
    await cur.close()
    if (
        row is None
        or row["transport"] != "omp-mcp"
        or row["binding_epoch"] is None
        or row["control_token_hash"] is None
        or not hmac.compare_digest(row["control_token_hash"], presented_hash)
        or not hmac.compare_digest(row["binding_epoch"], binding_epoch)
        or not hmac.compare_digest(row["omp_process_epoch"] or "", omp_process_epoch)
    ):
        raise _OmpFailure(403, "CONTROL_INVALID")
    return row


def _delivery_projection(row: aiosqlite.Row) -> dict[str, Any]:
    return {
        "event_id": str(row["event_id"]),
        "client_event_id": row["client_event_id"],
        "from_agent": row["from_agent"],
        "to_agent": row["to_agent"],
        "channel": row["channel"],
        "thread_id": row["thread_id"],
        "kind": row["kind"],
        "content": row["content"],
        "ts": row["ts"],
    }


def _validate_omp_delivery(row: aiosqlite.Row) -> None:
    if not isinstance(row["kind"], str) or row["kind"] not in DELIVERY_KINDS:
        raise _OmpFailure(409, "DELIVERY_INVALID")
    if any(
        not isinstance(row[name], str)
        for name in ("from_agent", "to_agent", "channel", "content")
    ):
        raise _OmpFailure(409, "DELIVERY_INVALID")
    if not (1 <= len(row["from_agent"]) <= 256 and 1 <= len(row["to_agent"]) <= 256):
        raise _OmpFailure(409, "DELIVERY_INVALID")
    if addressing.canonicalize_relay_name(row["to_agent"]) != row["to_agent"]:
        raise _OmpFailure(409, "DELIVERY_INVALID")
    if not (1 <= len(row["channel"]) <= 256):
        raise _OmpFailure(409, "DELIVERY_INVALID")
    if row["thread_id"] is not None and (
        not isinstance(row["thread_id"], str) or len(row["thread_id"]) > 256
    ):
        raise _OmpFailure(409, "DELIVERY_INVALID")
    if row["client_event_id"] is not None and (
        not isinstance(row["client_event_id"], str)
        or len(row["client_event_id"]) > 256
    ):
        raise _OmpFailure(409, "DELIVERY_INVALID")
    try:
        _canonical_epoch_ms(row["ts"])
    except _OmpFailure as exc:
        raise _OmpFailure(409, "DELIVERY_INVALID") from exc
    if len(row["content"].encode("utf-8")) > 1_048_576:
        raise _OmpFailure(409, "DELIVERY_INVALID")


async def _offer_status(
    conn: aiosqlite.Connection,
    recipient_id: str,
    event_id: int,
    binding: aiosqlite.Row,
    *,
    require_delivery: bool = True,
) -> Optional[dict[str, Any]]:
    cur = await conn.execute(
        "SELECT d.status AS delivery_status,d.reject_reason_code,"
        "o.offer_token,o.state AS offer_state,o.reason AS offer_reason "
        "FROM delivery d LEFT JOIN delivery_offer o "
        "ON o.recipient_id=d.recipient_id AND o.event_id=d.event_id "
        "WHERE d.recipient_id=? AND d.event_id=?",
        (recipient_id, event_id),
    )
    row = await cur.fetchone()
    await cur.close()
    if row is None:
        if require_delivery:
            raise _OmpFailure(404, "DELIVERY_NOT_FOUND")
        return None
    delivery_status = row["delivery_status"]
    offer_state = row["offer_state"]
    reason = row["reject_reason_code"]
    contradictory = (
        delivery_status not in {"outstanding", "acked", "rejected"}
        or ((delivery_status == "rejected") != (reason is not None))
        or (offer_state is None) != (row["offer_token"] is None)
        or (offer_state == "accepted" and delivery_status not in {"outstanding", "acked"})
        or (delivery_status == "acked" and offer_state != "accepted")
        or (delivery_status == "rejected" and offer_state is not None
            and (offer_state != "rejected" or row["offer_reason"] != reason))
        or (delivery_status != "rejected" and row["offer_reason"] is not None)
    )
    if contradictory:
        raise _OmpFailure(503, "PROTOCOL_INVALID")
    return {
        "recipient_id": recipient_id,
        "event_id": str(event_id),
        "offer_token": row["offer_token"],
        "offer_generation": 1 if row["offer_token"] is not None else None,
        "binding_epoch": binding["binding_epoch"],
        "omp_process_epoch": binding["omp_process_epoch"],
        "offer_state": offer_state,
        "delivery_status": delivery_status,
        "reject_reason_code": reason,
    }


async def _ledger_row(
    conn: aiosqlite.Connection, namespace: str, key: str
) -> Optional[aiosqlite.Row]:
    cur = await conn.execute(
        "SELECT * FROM idempotency_ledger WHERE namespace=? AND client_event_id=?",
        (namespace, key),
    )
    row = await cur.fetchone()
    await cur.close()
    return row


async def _record_protocol_result(
    conn: aiosqlite.Connection,
    namespace: str,
    key: str,
    endpoint: str,
    agent_id: Optional[str],
    digest: str,
    response_status: int,
    response: dict[str, Any],
    *,
    new_binding_epoch: Optional[str] = None,
    response_event_id: Optional[int] = None,
) -> None:
    await conn.execute(
        "INSERT INTO idempotency_ledger "
        "(namespace,client_event_id,endpoint,agent_id,response_event_id,request_hash,"
        "response_status,response_json,new_binding_epoch,created_ts) "
        "VALUES (?,?,?,?,?,?,?,?,?,?)",
        (
            namespace, key, endpoint, agent_id, response_event_id, digest,
            response_status, _canonical_json(response), new_binding_epoch, now_ms(),
        ),
    )

# ADDR-08 / MACH-05: reason token -> the fixed message a refused recipient renders with.
# The mapping from a parser reason to its HTTP presentation is the INGRESS layer's job:
# `addressing.py` is a pure leaf that must not import fastapi (C21), so it returns a token
# and this dict decides what a client is told.
#
# EVERY VALUE IS A FIXED CONSTANT AND MUST STAY ONE. Never interpolate the submitted
# address into any of them -- `_http_exc_handler` accepts a string detail as-is, so an
# f-string here puts peer-authored, untrusted text straight across the response boundary.
# Each message states the RULE, never the offending value; the address itself reaches only
# the server-side log, truncated, under the request id. Same discipline as the
# `_check_status` / `_check_kind` validators above and the DELETE /api/discord/poll-targets
# handler, which is the canonical no-echo pattern.
#
# PHASE 2 EXTENDS THIS DICT AND NOTHING ELSE: it splits `machine_not_resolvable` into two
# distinct outcomes (known machine with no transport yet vs unknown machine), which is why
# the grammar refusals -- the ones an existing client might actually see -- keep their
# status and their wording across that change.
_REFUSAL_MESSAGES = {
    "wildcard_not_supported":
        "recipient must not contain '*'; wildcard recipients are not supported",
    "ambiguous_multiple_separators":
        "recipient is ambiguous; an address may contain at most one '@'",
    "empty_address_part":
        "recipient must have a non-empty name and a non-empty machine either side of '@'",
    "invalid_machine_label":
        "the machine part of a recipient must be a single letter-digit-hyphen label "
        "containing no dots, at most 63 characters, not starting or ending with '-'",
    "machine_not_resolvable":
        "the machine named in this recipient is not resolvable from this broker; "
        "only 'localhost' is recognised",
}


def _ts_validator(v):
    """Pydantic before-validator: accept int (epoch-ms) or ISO-8601 string; return int.

    Raising ValueError here causes FastAPI to return 422 (validation_error)
    via the RequestValidationError handler — not 500.
    """
    return normalize_ts(v)


class AgentStatusBody(BaseModel):
    kind: str
    channel: str
    thread_id: Optional[str] = None
    status: str
    current_task: Optional[str] = None
    capabilities: Optional[list[str]] = None
    last_activity_ts: int
    client_event_id: Optional[str] = None
    # v1.3.1 (D29): optional human-readable label that consumers (e.g.,
    # claude-relay) can supply at status POST so the dashboard's Agent Messages
    # panel can render friendly names alongside UUID-shaped agent ids. Pure
    # additive — existing v1.3 callers omit the field; broker defaults to NULL
    # in the column AND in /api/state + /api/agent/{id}/status responses.
    # Full-replace semantics: re-POST without display_name nulls the stored
    # value (matches existing UPSERT semantics for kind/channel/status).
    display_name: Optional[str] = None
    # R1: product-neutral transport descriptor. Existing Claude callers omit it;
    # their exact cc-session/#cc-relay request shape is recognized transactionally.
    transport: Optional[str] = Field(default=None, min_length=1, max_length=64)

    @field_validator("status")
    @classmethod
    def _check_status(cls, v: str) -> str:
        if v not in VALID_AGENT_STATUSES:
            # F1: do not echo `v` — Pydantic surfaces it via msg, crossing the
            # response boundary even after `input`/`ctx` are stripped.
            raise ValueError(f"status must be one of {sorted(VALID_AGENT_STATUSES)}")
        return v

    @field_validator("last_activity_ts", mode="before")
    @classmethod
    def _norm_ts(cls, v):
        return _ts_validator(v)


class EventBody(BaseModel):
    # max_length bounds the peer-authored relay names at the /api/event ingress (realistic
    # names are <= 64; 256 is generous headroom incl. `subagent_type@session_id` forms). This
    # is the source of DIRECTED messages -> `delivery` rows the escalation reaper reads, so the
    # bound keeps a multi-MB name out of the delivery table / reaper read-key-log path (and off
    # the delivery index). The OTHER message_log writer carrying a peer-authored name,
    # POST /api/agent/{id}/status, is bounded at that endpoint (agent_id len guard) for parity.
    # The reaper's own name caps (_reaper_sanitize_name[:256] + the from_agent substr) remain
    # as defense-in-depth for any pre-bound / rolling-deploy row.
    from_agent: str = Field(..., min_length=1, max_length=256)
    to_agent: Optional[str] = Field(default=None, max_length=256)
    channel: str = Field(..., min_length=1)
    thread_id: Optional[str] = None
    kind: str
    content: str
    ts: int
    client_event_id: Optional[str] = None

    @field_validator("kind")
    @classmethod
    def _check_kind(cls, v: str) -> str:
        if v not in VALID_EVENT_KINDS:
            # F1: do not echo `v` — see _check_status comment.
            raise ValueError(f"kind must be one of {sorted(VALID_EVENT_KINDS)}")
        return v

    @field_validator("ts", mode="before")
    @classmethod
    def _norm_ts(cls, v):
        return _ts_validator(v)

    @model_validator(mode="after")
    def _check_agent_message_requires_to_agent(self):
        # v1.3 (D28): kind='agent_message' is point-to-point only; broadcast
        # (to_agent=null) is explicitly forbidden per contract section 8 forbidden
        # patterns. Pydantic ValidationError surfaces as HTTP 422 via the
        # RequestValidationError handler -- matches existing pattern.
        if self.kind == "agent_message" and (self.to_agent is None or self.to_agent == ""):
            raise ValueError("to_agent is required for kind='agent_message'")
        return self

    @model_validator(mode="after")
    def _check_error_content_envelope(self):
        """v1.7.1 (Wave 6 Track 4) STRICT MODE: kind='error' content MUST be a JSON
        envelope with shape: {type:str, severity: info|warn|error|critical, errors:[...],
        context:{...}}. Malformed content is 422-rejected.

        Producers must use the strict envelope; legacy unstructured error
        content is not accepted.

        Sub-error item shape (`errors[i]` field types) is explicitly consumer-validated
        per contract v1.7.1 — broker validates only the outer 3 keys + severity enum +
        errors-is-a-list to preserve future extensibility (per D38 + D28 content-opaque
        principle).
        """
        if self.kind != "error":
            return self
        try:
            envelope = json.loads(self.content)
            if not isinstance(envelope, dict):
                raise ValueError("envelope is not a JSON object")
            etype = envelope.get("type")
            sev = envelope.get("severity")
            errors = envelope.get("errors")
            if not isinstance(etype, str) or not etype:
                raise ValueError("envelope.type missing or not a string")
            if sev not in VALID_ERROR_SEVERITIES:
                # F1: do not echo `sev` value into the exception message.
                raise ValueError(
                    f"envelope.severity must be one of {sorted(VALID_ERROR_SEVERITIES)}"
                )
            if not isinstance(errors, list):
                raise ValueError("envelope.errors must be a list")
            # context is optional + free-form; broker stays opaque per D28.
            # sub-error item shape per contract v1.7.1 is consumer-validated; not enforced here.
        except (json.JSONDecodeError, ValueError) as e:
            # v1.7.1 STRICT 422. F1 token-leak safety: type(e).__name__ only.
            raise ValueError(
                f"kind='error' content must be a v1.7 envelope "
                f"{{type, severity, errors[], context}} ({type(e).__name__})"
            )
        return self


class AckBody(BaseModel):
    # WS-B-broker-1: POST /api/agent/{name}/ack body. Bounded list caps the IN(...)
    # size (500 ids + 3 fixed params < SQLite's 999 bound-variable limit).
    # WS-B-broker-2 (#18 mirror fix): event_ids is now OPTIONAL and an additive
    # `floor` mirrors the recipient's gap-aware confirmed-set high-water. The broker
    # crosses off every outstanding row for this recipient with event_id <= floor
    # (in ADDITION to the explicit event_ids). `floor` advances ONLY over a
    # CONTIGUOUS confirmed run (hook _collapse_confirmed) -> it never passes an
    # unread id, so acking <= floor can never cross off a gap/unread message.
    # Loosening event_ids from required-min-1 to optional is backward-compatible
    # (every existing caller sends >= 1 id).
    event_ids: list[int] = Field(default_factory=list, max_length=500)
    floor: Optional[int] = None

    @field_validator("event_ids")
    @classmethod
    def _check_ids_in_i64_range(cls, v: list[int]) -> list[int]:
        # Bound every id to a valid message_log.event_id range [1, SQLITE_I64_MAX].
        # Without this an out-of-i64-range id passes Pydantic int validation but
        # raises OverflowError at aiosqlite bind time -> 500 instead of a clean 422.
        # Mirrors the repo's "bound all SQLite INTEGER inputs" pattern (e.g.
        # _decode_state_cursor). F1: do NOT echo the offending value.
        for x in v:
            if x < 1 or x > SQLITE_I64_MAX:
                raise ValueError("each event_id must be an integer in [1, SQLITE_I64_MAX]")
        return v

    @field_validator("floor")
    @classmethod
    def _check_floor_in_i64_range(cls, v: Optional[int]) -> Optional[int]:
        # Same i64 bound as event_ids (guards the aiosqlite INTEGER bind). None =
        # no floor mirror on this call. Do NOT echo the offending value (F9).
        if v is None:
            return v
        if v < 1 or v > SQLITE_I64_MAX:
            raise ValueError("floor must be an integer in [1, SQLITE_I64_MAX]")
        return v

    @model_validator(mode="after")
    def _require_ids_or_floor(self) -> "AckBody":
        # At least one of {event_ids, floor} must be present. An empty-both body is
        # a caller bug that would otherwise 200 with acked_now=0 and mask itself.
        if not self.event_ids and self.floor is None:
            raise ValueError("provide event_ids and/or floor")
        return self


class RelayNameBody(BaseModel):
    relay_name: str = Field(..., min_length=1, max_length=64)
    takeover: bool = Field(default=False, strict=True)

    @field_validator("relay_name")
    @classmethod
    def _canonical_relay_name(cls, value: str) -> str:
        canonical = addressing.canonicalize_relay_name(value)
        if canonical is None:
            raise ValueError("relay_name must match [A-Za-z0-9._-]{1,64}")
        return canonical


class DeliveryAckBody(BaseModel):
    model_config = {"extra": "forbid"}
    event_ids: list[int] = Field(..., min_length=1, max_length=500)

    @field_validator("event_ids")
    @classmethod
    def _check_ids_in_i64_range(cls, values: list[int]) -> list[int]:
        for value in values:
            if value < 1 or value > SQLITE_I64_MAX:
                raise ValueError("each event_id must be an integer in [1, SQLITE_I64_MAX]")
        return values


class ProjectRegisterBody(BaseModel):
    name: str = Field(..., min_length=1)
    root_path: str
    state_db_path: str
    # D27 (v1.2 additive): when False, broker skips local-FS-dependent checks
    # (Path.exists, sqlite_master tables, refresh_pipelines_view, watcher start)
    # so a Linux broker can register a project whose state.db lives on a
    # Windows host. Default True preserves existing single-machine semantics.
    # strict=True rejects "false"/"true" string coercion → must be a JSON bool.
    local_validation: bool = Field(default=True, strict=True)


class DiscordCursorBody(BaseModel):
    channel_id: str = Field(..., min_length=1)
    thread_id: Optional[str] = None  # null/omitted normalized to '' before insert
    last_seen_message_id: str = Field(..., min_length=1)
    last_check_ts: int

    @field_validator("last_check_ts", mode="before")
    @classmethod
    def _norm_ts(cls, v):
        return _ts_validator(v)


class DiscordPollTargetBody(BaseModel):
    """M2 (D16 § (a)) poll-target registry body. thread_id null/omitted → ''."""
    channel_id: str = Field(..., min_length=1)
    thread_id: Optional[str] = None


class AuditSnapshotBody(BaseModel):
    snapshot_window_start_ts: Optional[int] = None

    @field_validator("snapshot_window_start_ts")
    @classmethod
    def _check_ts(cls, v):
        if v is None:
            return v
        if v < 0:
            raise ValueError("snapshot_window_start_ts must be >= 0")
        if v > SQLITE_I64_MAX:
            raise ValueError(f"snapshot_window_start_ts exceeds SQLite i64 max ({SQLITE_I64_MAX})")
        # Also guard end_ms = ts + 3600000 against i64 overflow
        if v + 3_600_000 > SQLITE_I64_MAX:
            raise ValueError("snapshot_window_start_ts too close to i64 max (would overflow on +1h)")
        return v


# -------------------------------------------------------------------- Helpers (idempotency, SSE kind, paths)

def resolve_client_event_id(body_cei: Optional[str], header_cei: Optional[str]) -> Optional[str]:
    """Per contract § 1 API invariants: POST endpoints accept either body.client_event_id
    OR Idempotency-Key header. If both present and differ, 400."""
    if body_cei and header_cei and body_cei != header_cei:
        raise HTTPException(
            status_code=400,
            detail="client_event_id (body) and Idempotency-Key (header) disagree",
        )
    return body_cei or header_cei or None


def sse_kind(message_log_kind: str) -> str:
    """Map storage kind (message_log.kind) to contract SSE event kind taxonomy.

    Contract § 1 GET /api/events: kind ∈ {agent_status_change, message_logged, pipeline_event}.

    F2 (iter1 discriminator): D16 § (b) requires `permission_reply` to reach SSE
    consumers under that exact kind so subagents can filter for it. Earlier this
    function collapsed every non-status/non-pipeline kind to `message_logged`,
    which masked permission_reply AND error events (e.g. catchup_truncated,
    persistent-open). M2/v1.1 expansion preserves these explicitly.

    F-CC30 (local-relay-channel discriminator iter-1 M1): additive preservation
    of `agent_reply` + `handoff` so SSE consumers (the local-relay-channel MCP
    server filtering on INTER_SESSION_MESSAGE_KINDS) can route them. Pre-fix
    both collapsed to `message_logged` → MCP server's per-kind filter silently
    dropped them, breaking the reply-tool roundtrip + handoff push. Matches
    the precedent set for `agent_message` (preserved verbatim above).
    """
    if message_log_kind == "status":
        return "agent_status_change"
    if message_log_kind == "pipeline_event":
        return "pipeline_event"
    if message_log_kind == "permission_reply":
        return "permission_reply"
    if message_log_kind == "error":
        return "error"
    if message_log_kind == "agent_message":
        return "agent_message"
    if message_log_kind == "agent_reply":
        return "agent_reply"
    if message_log_kind == "handoff":
        return "handoff"
    return "message_logged"


def norm_path(p: str) -> str:
    r"""Normalize a filesystem path for case-insensitive comparison on Windows.

    Resolves symlinks + applies normpath + normcase. Handles forward/backward
    slash mixing, .\ prefixes, and case differences.
    """
    return os.path.normcase(os.path.normpath(str(Path(p).resolve())))


# -------------------------------------------------------------------- SSE broadcaster

class _Subscriber:
    __slots__ = ("queue", "cursor", "dead")

    def __init__(self, cursor: int):
        self.queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=1000)
        self.cursor = cursor
        self.dead = False


_subscribers: list[_Subscriber] = []
_subs_lock = asyncio.Lock()


async def broadcast_event(event: dict[str, Any]) -> None:
    """Push event to all live subscribers; mark slow consumers dead.

    event dict shape: {event_id, ts, kind, agent (=from_agent), data: {...}}
    """
    async with _subs_lock:
        subs_snapshot = list(_subscribers)
    for sub in subs_snapshot:
        if sub.dead:
            continue
        try:
            sub.queue.put_nowait(event)
        except asyncio.QueueFull:
            sub.dead = True
            # best-effort signal; if even this fails, the queue is full so consumer is gone
            try:
                sub.queue.put_nowait({
                    "event_id": -1,
                    "ts": now_ms(),
                    "kind": "error",
                    "agent": "system:broker",
                    "data": {"reason": "slow_consumer_evicted"},
                })
            except asyncio.QueueFull:
                pass


async def sse_event_generator(request: Request, cursor: int) -> AsyncIterator[dict[str, Any]]:
    """Replay from cursor (DB), then subscribe to live broadcaster.

    Replay reconstructs the live broadcast `data` shape from message_log columns
    so consumers see identical envelopes between replay and live (F2 + known
    F-replay-vs-live data shape fix). Status events parse content as JSON;
    other kinds expose {to_agent, content, channel, thread_id}.
    """
    sub = _Subscriber(cursor)
    async with _subs_lock:
        _subscribers.append(sub)
    try:
        # 1. Replay from cursor
        async with connect_db() as conn:
            cur = await conn.execute(
                "SELECT event_id, ts, kind, from_agent, to_agent, channel, thread_id, content "
                "FROM message_log WHERE event_id > ? ORDER BY event_id ASC LIMIT 1000",
                (cursor,),
            )
            rows = await cur.fetchall()
            await cur.close()
        for row in rows:
            data = _reconstruct_data(row["kind"], row["to_agent"], row["channel"],
                                     row["thread_id"], row["content"])
            payload = {
                "ts": row["ts"],
                "kind": sse_kind(row["kind"]),
                "agent": row["from_agent"],
                "data": data,
            }
            yield {"id": str(row["event_id"]), "event": "message", "data": json.dumps(payload)}

        # 2. Subscribe to live with periodic heartbeat
        while not sub.dead:
            if await request.is_disconnected():
                break
            try:
                event = await asyncio.wait_for(sub.queue.get(), timeout=15.0)
            except asyncio.TimeoutError:
                yield {"event": "heartbeat", "data": ""}
                continue
            payload = {
                "ts": event["ts"],
                "kind": sse_kind(event["kind"]),
                "agent": event["agent"],
                "data": event["data"],
            }
            yield {"id": str(event["event_id"]), "event": "message", "data": json.dumps(payload)}

        # 3. If dead, drain final eviction event then end
        if sub.dead:
            try:
                final = sub.queue.get_nowait()
                yield {
                    "id": str(final["event_id"]),
                    "event": "message",
                    "data": json.dumps({
                        "ts": final["ts"], "kind": sse_kind(final["kind"]),
                        "agent": final["agent"], "data": final["data"],
                    }),
                }
            except asyncio.QueueEmpty:
                pass
    finally:
        async with _subs_lock:
            if sub in _subscribers:
                _subscribers.remove(sub)


def _safe_json_load(content: str) -> Any:
    try:
        return json.loads(content)
    except (json.JSONDecodeError, TypeError):
        return content


def _reconstruct_data(kind: str, to_agent: Optional[str], channel: str,
                      thread_id: Optional[str], content: str) -> Any:
    """Rebuild the live-broadcast `data` shape from message_log columns (replay path)."""
    if kind == "status":
        return _safe_json_load(content)
    return {
        "to_agent": to_agent,
        "content": content,
        "channel": channel,
        "thread_id": thread_id,
    }


async def drain_subscribers() -> None:
    async with _subs_lock:
        for sub in _subscribers:
            sub.dead = True
        _subscribers.clear()


# -------------------------------------------------------------------- Idempotency

async def check_idempotent(
    conn: aiosqlite.Connection, client_event_id: Optional[str]
) -> Optional[int]:
    """Return prior response_event_id if client_event_id seen, else None."""
    if not client_event_id:
        return None
    cur = await conn.execute(
        "SELECT response_event_id FROM idempotency_ledger "
        "WHERE namespace='legacy' AND client_event_id=?",
        (client_event_id,),
    )
    row = await cur.fetchone()
    await cur.close()
    return row["response_event_id"] if row else None


async def record_idempotent(
    conn: aiosqlite.Connection,
    client_event_id: Optional[str],
    endpoint: str,
    agent_id: Optional[str],
    response_event_id: int,
) -> None:
    if not client_event_id:
        return
    await conn.execute(
        "INSERT INTO idempotency_ledger "
        "(namespace,client_event_id,endpoint,agent_id,response_event_id,created_ts) "
        "VALUES ('legacy',?,?,?,?,?)",
        (client_event_id, endpoint, agent_id, response_event_id, now_ms()),
    )


async def _idempotency_ttl_sweeper() -> None:
    """Background task: every 60s, delete idempotency_ledger rows older than 24h.
    Wrapped in with_retry per F4 (BEGIN IMMEDIATE + busy-retry coverage)."""
    interval_sec = _env_int("BROKER_IDEMPOTENCY_SWEEP_INTERVAL_SEC", 60)
    ttl_ms = _env_int("BROKER_IDEMPOTENCY_TTL_MS", 86_400_000)  # 24h
    iter_count = 0  # F-CC21 A7
    while True:
        try:
            await asyncio.sleep(interval_sec)
            iter_count += 1
            if iter_count % 60 == 0:  # F-CC21 A7: log once per hour at 60s cadence
                logger.info("ttl_sweeper alive (iter=%d, ~%dh uptime)", iter_count, iter_count // 60)
            cutoff = now_ms() - ttl_ms

            async def _sweep():
                async with connect_db() as conn:
                    await conn.execute("BEGIN IMMEDIATE")
                    try:
                        await conn.execute(
                            "DELETE FROM idempotency_ledger "
                            "WHERE namespace='legacy' AND created_ts < ?",
                            (cutoff,),
                        )
                        await conn.commit()
                    except Exception:
                        await conn.rollback()
                        raise

            await with_retry(_sweep)
        except asyncio.CancelledError:
            return
        except Exception as e:
            # F-CC21 A3: full traceback (was message-only).
            logger.warning("idempotency TTL sweep failed: %s", e, exc_info=True)


# -------------------------------------------------------------------- Relay escalation reaper (#18, WS-B-broker-2)

# Untrusted-text sanitizers for the alert body. BOTH the message `content` (preview)
# AND `to_agent`/`from_agent` are peer-authored and fully untrusted — the broker does
# NOT enforce _is_valid_relay_name on POST, so a name may contain arbitrary chars. We
# therefore sanitize ALL spliced fields: strip control chars (keep the alert one line),
# defang Discord mass-mentions / id-mentions (no @everyone of the alert channel), and
# strip backticks (names + preview sit around the alert's structural inline-code spans;
# a stray backtick would fold the actionable agent name into a code span). Preview is
# also length-bounded (untrusted content is uncapped at POST) + truncated.
_REAPER_MENTION_RE = re.compile(r"@(everyone|here)", re.IGNORECASE)
_REAPER_MENTION_TAG_RE = re.compile(r"<(@[!&]?|#)(\d+)>")
_REAPER_CTRL_RE = re.compile(r"[\x00-\x1f\x7f]")
_ZWSP = "\u200b"  # zero-width space, ASCII-escaped in source; spliced to break Discord mention parsing
# Discord markdown formatting tokens + the backslash itself. The untrusted preview is
# backslash-ESCAPED against these (the DOCUMENTED Discord literal-render mechanism, so it
# is tokenizer-independent \u2014 unlike ZWSP-splitting emphasis, whose exact behavior we would
# have to guess at). Escaping '\' too means a literal backslash in content renders as one
# backslash AND can never combine with a following escape to re-expose a formatting token.
_REAPER_MD_ESCAPE_RE = re.compile(r"[\\*_~|]")


def _reaper_ascii_only(s: str) -> str:
    """Replace every non-ASCII char with a space. This is the ROOT-CAUSE close of the
    homoglyph / bidi / invisible-control whack-a-mole: an ENUMERATED denylist of look-alike
    or format codepoints can never be proven complete — Unicode has dot-like glyphs across
    Letter/Punctuation/Symbol categories (U+00B7, U+2219, U+30FB, U+2E31, U+A78F, ...) and
    dozens of bidi / zero-width controls (U+202A-202E, U+2066-2069, U+061C, U+200B-200F, ...).
    "The untrusted field carries NO non-ASCII" is complete BY CONSTRUCTION against all of
    them at once. The alert's structural tokens (' · ' = U+00B7 separator, '"' quote wrapper,
    backtick code-spans) and Discord's markdown are ALL ASCII, so an ASCII-only field cannot
    homoglyph-imitate any of them; the remaining ASCII-domain steps (control-strip,
    backtick/quote -> ', markdown escape, mention defang) handle the ASCII structural chars.
    The preview is a truncated recognition HINT (the full message is one `relay-check`
    away), so dropping non-ASCII glyphs from it is an acceptable safety trade."""
    return "".join(ch if ord(ch) <= 0x7F else " " for ch in s)


def _reaper_defang(s: str) -> str:
    s = _REAPER_MENTION_RE.sub("@" + _ZWSP + r"\1", s)
    s = _REAPER_MENTION_TAG_RE.sub("<" + _ZWSP + r"\1\2>", s)
    return s


def _reaper_sanitize_name(name: Optional[str], limit: int = 64) -> str:
    if name is None:                    # `is None`, NOT truthiness: a hostile __bool__ must
        return "?"                      # not raise before the guarded str() below
    try:
        # [:256] EARLY cap bounds the O(n) sanitizer work on an adversarially-long name
        # (mirrors the preview's early 2048 bound); the final s[:limit] still applies.
        raw = str(name)[:256]           # untrusted __str__ could raise; contract = never raises
    except Exception:
        return "?"
    s = _reaper_ascii_only(_REAPER_CTRL_RE.sub("", raw))   # drop C0/DEL, then non-ASCII -> space
    # strip backticks: names sit inside the alert's inline-code spans.
    s = _reaper_defang(s).replace("`", "'").strip()
    return s[:limit] or "?"


def _reaper_preview_text(content: Any) -> str:
    """Best-effort unwrap of the broker {"text": ...} send-envelope for a readable
    preview; falls back to the raw content string. NEVER raises — the content is fully
    untrusted, so the json.loads input is BOUNDED and the except is broad: deeply-nested
    JSON raises RecursionError (NOT a ValueError/TypeError), which would otherwise escape
    and, with no per-row guard, wedge the reaper on the oldest un-alerted row forever."""
    if isinstance(content, str):
        try:
            obj = json.loads(content[:8192])   # bounded; broad except => never raises
            if isinstance(obj, dict) and isinstance(obj.get("text"), str):
                return obj["text"]
        except Exception:
            pass
        return content
    if content is None:
        return ""
    try:
        return str(content)     # a non-str object's __str__ could raise; contract = never raises
    except Exception:
        return ""


def _reaper_sanitize_preview(content: Any, limit: int = 100) -> str:
    s = _reaper_preview_text(content)
    if not s:
        return ""
    s = s[:2048]                        # bound untrusted work first
    s = _reaper_ascii_only(s)           # non-ASCII -> space: kills homoglyph/bidi/invisible
    s = _REAPER_CTRL_RE.sub(" ", s)     # ASCII control chars (newlines etc) -> single line
    # The preview is spliced into the alert as ("<preview>") between ' · '-delimited fields.
    # After _reaper_ascii_only the only structural chars it can still carry are ASCII, so a
    # crafted preview cannot homoglyph-imitate the ' · ' separator; neutralize the remaining
    # ASCII structural chars so it can't forge the alert's structure (and inject a fake
    # `action: /mcp reconnect <victim>` line the operator would act on) or apply markdown:
    #   defang @everyone/<@id> (belt; allowed_mentions parse:[] is the hard backstop)
    #   backtick -> ' : can't open/close the alert's inline-code spans (the operator's
    #                   UNFORGEABLE trust anchor is the code-spanned agent name in the real
    #                   action field — the preview can never emit a backtick to fake one)
    #   '"' -> "'"    : can't fake the ("...") preview-quote boundary
    #   \ * _ ~ |     : backslash-escaped so Discord renders them literally (no bold/italic/
    #                   strike/spoiler that could hide or restyle the real action line)
    s = _reaper_defang(s)
    s = s.replace("`", "'").replace('"', "'")
    s = _REAPER_MD_ESCAPE_RE.sub(r"\\\g<0>", s).strip()
    if len(s) > limit:
        # rstrip a trailing '\' first: an odd truncation can sever a '\x' escape pair and
        # leave a dangling backslash that would escape the template's closing '"'.
        s = s[: limit - 1].rstrip("\\") + "…"  # ellipsis
    return s


def _reaper_host() -> str:
    """Broker hostname for the alert. Single-machine assumption: in the current
    intra-machine messaging model broker-host ~= recipient-host; the agent NAME is
    the reliable identifier regardless (flagged for a future multi-broker pass)."""
    try:
        return socket.gethostname() or "?"
    except Exception:
        return "?"


def _build_reaper_alert(*, to_agent: str, event_id: int, from_agent: Optional[str],
                        preview: Any, age_s: int, host: str) -> str:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # broker-local wall clock
    to_s = _reaper_sanitize_name(to_agent)
    from_s = _reaper_sanitize_name(from_agent)
    prev = _reaper_sanitize_preview(preview)
    # Names wrapped in inline-code spans: readable + a second mention-defang layer.
    # THE ACTION FIELD DOES NOT PRESCRIBE A RECONNECT, and that is measured rather
    # than stylistic: reloading the connection re-reads the same records and
    # computes the same delivery key, so for the one cause that never recovers on
    # its own -- a session whose registry row lost its name -- it produces the
    # identical outcome. Re-claiming the name is what actually fixes that.
    # IT STILL NAMES A REMEDY FOR AN AGENT THAT IS NOT RUNNING. The old field
    # ended "(else restart)", and a first draft of this change dropped it -- which
    # left the largest arm of the reaper's own stated population (dead / exited)
    # with a diagnosis and no action. "restart it if not" is that clause back, in
    # the branch it belongs to: you cannot re-claim a name inside a session that
    # has exited. The remaining arms (wedged subscription, broker-was-down) are
    # covered by the same liveness check.
    # The command is inline-code-spanned because it carries underscores, which
    # Discord would otherwise render as italics.
    # KEEP THIS FIELD FREE OF ' · ': the alert's separator count is the security
    # invariant that proves an untrusted preview contributed no field of its own.
    # Parentheses and commas are safe here; a new non-ASCII separator-like glyph
    # would not be (see the homoglyph test).
    return (
        "⚠ relay unacked · " + ts + " · agent `" + to_s + "` · "
        "msg " + str(event_id) + " (\"" + prev + "\") unacked " + str(age_s) + "s · "
        "action: check `" + to_s + "` is live (restart it if not), then re-claim "
        "its relay name there with `/relay-name` · "
        "from `" + from_s + "` · broker " + host
    )


async def _reaper_run_once(*, threshold_sec: int, channel_id: str, host: str) -> int:
    """One reaper cycle (extracted for unit-testability + to keep the loop thin):
    READ outstanding-past-grace rows, POST an alert per row OUTSIDE any txn, and
    stamp `alerted_ts` on POST success in a SEPARATE short write txn. Returns the
    number of rows alerted this cycle.

    Load-bearing txn discipline (single-writer broker): NEVER hold BEGIN IMMEDIATE
    across the network POST — it would stall every inbound relay write.
    """
    cutoff = now_ms() - threshold_sec * 1000

    # (1) READ — short snapshot, released immediately. A plain SELECT in WAL gets a
    # consistent read snapshot WITHOUT a write lock, so it never contends with
    # inbound relay writes (no BEGIN IMMEDIATE here). INNER JOIN message_log: an
    # outstanding row always has its parent (retention co-deletes delivery rows per
    # D71); a pruned/absent parent simply isn't selected. LIMIT bounds per-cycle
    # Discord egress; the rest come next cycle. `content` AND the display-only `from_agent`
    # are bounded AT THE DB READ via substr (both are uncapped at ingest) so a multi-MB body
    # or name is never materialized into Python. `to_agent` is NOT substr'd: it is the write
    # key for the alerted_ts stamp below (a truncated key would fail to match the row), and
    # realistic recipient names are short; its per-cycle exposure is bounded by LIMIT 50.
    async def _read(_cutoff=cutoff):
        async with connect_db() as conn:
            cur = await conn.execute(
                "SELECT d.to_agent AS to_agent, d.event_id AS event_id, "
                "d.created_ts AS created_ts, substr(m.from_agent, 1, 256) AS from_agent, "
                "substr(m.content, 1, 8192) AS content "
                "FROM delivery d JOIN message_log m ON m.event_id = d.event_id "
                "WHERE d.status = 'outstanding' AND d.alerted_ts IS NULL "
                "AND d.created_ts < ? "
                "ORDER BY d.created_ts ASC LIMIT 50",
                (_cutoff,),
            )
            rows = await cur.fetchall()
            await cur.close()
            return [dict(r) for r in rows]

    candidates = await with_retry(_read)
    if not candidates:
        return 0
    if len(candidates) == 50:
        logger.info("relay_reaper: per-cycle cap (50) hit; more stuck rows "
                    "will be alerted next cycle")

    # Lazy import: discord_poller imports server (circular at module scope).
    from broker import discord_poller as _dp

    alerted = 0
    for row in candidates:
        try:
            to_agent = row["to_agent"]
            event_id = row["event_id"]
            age_s = max(0, (now_ms() - row["created_ts"]) // 1000)
            content = _build_reaper_alert(
                to_agent=to_agent, event_id=event_id,
                from_agent=row["from_agent"], preview=row["content"],
                age_s=age_s, host=host,
            )
            # (2a) RE-CHECK status right before the POST (fresh short read, released
            # immediately): a genuine ack may have landed between the batch read and
            # now. This shrinks the late-ack false-page window from the whole ~30s POST
            # down to the ~ms between this check and the POST.
            async def _still_outstanding(_ta=to_agent, _eid=event_id):
                async with connect_db() as conn:
                    cur = await conn.execute(
                        "SELECT 1 FROM delivery WHERE to_agent = ? AND event_id = ? "
                        "AND status = 'outstanding' AND alerted_ts IS NULL",
                        (_ta, _eid),
                    )
                    r = await cur.fetchone()
                    await cur.close()
                    return r is not None

            if not await with_retry(_still_outstanding):
                continue   # acked (or already alerted) since the batch read -> no page
            # (2b) POST — OUTSIDE any txn (up to 30s). Never under a write lock.
            ok = await _dp.post_alert(content, channel_id)
            if not ok:
                # Leave alerted_ts NULL -> retried next cycle (at-least-once).
                continue
            # (3) WRITE — separate short txn, only on POST success. status-aware +
            # `alerted_ts IS NULL` so a genuine ack that lands DURING the POST (status
            # -> 'acked', alerted_ts still NULL) is NOT stamped, and a double-run never
            # re-alerts. (The re-check above already suppresses the POST in the common
            # case; this guards the residual check->POST window's bookkeeping.)
            _anow = now_ms()

            async def _stamp(_ta=to_agent, _eid=event_id, _ts=_anow):
                async with connect_db() as conn:
                    await conn.execute("BEGIN IMMEDIATE")
                    try:
                        await conn.execute(
                            "UPDATE delivery SET alerted_ts = ? "
                            "WHERE to_agent = ? AND event_id = ? "
                            "AND status = 'outstanding' AND alerted_ts IS NULL",
                            (_ts, _ta, _eid),
                        )
                        await conn.commit()
                    except Exception:
                        await conn.rollback()
                        raise

            await with_retry(_stamp)
            alerted += 1
            logger.info("relay_reaper: escalated unacked msg %s -> agent %s "
                        "(age %ss)", event_id, to_agent[:256], age_s)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # Defense-in-depth: one poison row (e.g. pathological content) must NEVER
            # abort the whole cycle. Skip + log; the row stays alerted_ts NULL and is
            # retried next cycle, but newer genuinely-stuck rows behind it still alert.
            logger.warning(
                "relay_reaper: skipping row (to_agent=%r event_id=%r) after error: %s",
                row.get("to_agent"), row.get("event_id"), e, exc_info=True,
            )
            continue
    return alerted


async def _relay_escalation_reaper() -> None:
    """Background task (#18): escalate relay messages stuck 'outstanding' past a
    grace window to the operator via an explicitly configured Discord channel.

    ALERTS ONLY — never auto-acks, auto-reconnects, or auto-restarts anything
    (C-17: observability + alerting, NOT a supervisor/watchdog; the human decides
    the action). Reads the D71/D72 `delivery` ledger: a directed relay row stays
    'outstanding' until the recipient's client mirrors its ack (WS-B-client). A row
    still 'outstanding' AND un-alerted AND older than the grace window means the
    recipient never confirmed receipt (dead / exited / wedged subscription /
    broker-was-down) -> ping the human to check the agent is live, restart it if it
    is not, and -- if it is live but its relay row lost its name -- re-claim the
    name there with `/relay-name`.

    THE ALERT USED TO PRESCRIBE `/mcp reconnect`, AND THAT WAS MEASURED WRONG for
    the one cause in that list that never recovers on its own. A reconnect re-reads
    the same session record and the same registry and derives the same delivery
    key, so a session filing receipts into a mailbox nothing addresses comes back
    from the reconnect in exactly the state it went in. Re-claiming the name
    rewrites the record the derivation reads. The plugin now refuses to subscribe
    under a name it cannot file a receipt under, so that state should no longer be
    constructible at all -- but this page is the operator's last line and must not
    hand them a remedy that does nothing.

    IT MUST ALSO NOT LEAVE THE BIGGEST ARM WITHOUT ONE. The old field's trailing
    "(else restart)" covered "dead / exited", which is the first item in the list
    above, and a draft of this change dropped it while replacing the reconnect --
    trading a wrong remedy for a missing one. You cannot re-claim a name inside a
    session that has exited, so the restart clause is back in the alert.

    The per-cycle work lives in `_reaper_run_once` (testable seam). This wrapper is
    the canonical sleep-first loop idiom (cloned from _idempotency_ttl_sweeper):
    CancelledError before the broad except, hourly liveness log. Dedup: `alerted_ts`
    (D71 uv1->2 column) is stamped once per row on POST success; the SELECT filters
    `alerted_ts IS NULL`, so each stuck episode alerts exactly once. The uv1->2
    cutover reconcile already stamped the pre-#18 backlog, so only NEW post-enable
    stuck messages are ever alerted.
    """
    interval_sec = _env_int("BROKER_REAPER_INTERVAL_SEC", 60)
    threshold_sec = _env_int("BROKER_REAPER_THRESHOLD_SEC", 3600)  # 60 min grace (D111)
    # D82: env-ONLY, no hardcoded default. main()'s _verify_reaper_config_or_die() refuses to
    # start when the reaper is enabled but this is unset, so it is guaranteed present here; a
    # bypass-main edge raises KeyError -> _surface_task_death logs it loudly (fail-loud, never a
    # silently-mute reaper posting to a wrong/hardcoded channel).
    channel_id = os.environ["BROKER_ALERT_DISCORD_CHANNEL_ID"]
    host = _reaper_host()
    iter_count = 0
    while True:
        try:
            await asyncio.sleep(interval_sec)
            iter_count += 1
            if iter_count % 60 == 0:  # hourly liveness at the 60s default cadence
                logger.info("relay_reaper alive (iter=%d, ~%dh uptime)",
                            iter_count, iter_count // 60)
            await _reaper_run_once(
                threshold_sec=threshold_sec, channel_id=channel_id, host=host
            )
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.warning("relay escalation reaper failed: %s", e, exc_info=True)


# -------------------------------------------------------------------- Audit snapshot (Pattern 4)

async def _emit_audit_snapshot(window_ms: int, output_dir: Path,
                               conn: aiosqlite.Connection,
                               overwrite: bool = True) -> Optional[dict]:
    """Snapshot message_log events in [window_ms, window_ms + 3_600_000) to
    `output_dir/{window_ms}.json`. Returns the snapshot dict on success.

    When `overwrite=True` (default — manual POST endpoint contract) the file is
    written unconditionally via `Path.write_text` (replacing any prior content).

    When `overwrite=False` (runner contract) the write uses
    `os.open(..., O_CREAT | O_EXCL | O_WRONLY)` which is atomic at the OS level:
    the file is created only if it does not already exist. On `FileExistsError`
    the function returns None (signal to caller: "skipped, file already existed").
    This eliminates the TOCTOU race a separate `Path.exists()` pre-check would
    introduce when multiple writers race for the same window_ms file."""
    end_ms = window_ms + 3_600_000
    cur = await conn.execute(
        "SELECT event_id, ts, kind, from_agent, to_agent, channel, thread_id, content "
        "FROM message_log WHERE ts >= ? AND ts < ? ORDER BY event_id",
        (window_ms, end_ms),
    )
    rows = [dict(r) for r in await cur.fetchall()]
    await cur.close()

    snapshot = {"snapshot_window_start_ts": window_ms,
                "event_count": len(rows), "events": rows}
    output_path = output_dir / f"{window_ms}.json"
    payload = json.dumps(snapshot, indent=2)

    if overwrite:
        output_path.write_text(payload, encoding="utf-8")
    else:
        # Atomic create-or-fail. The OS check IS the check — no separate exists() call.
        try:
            fd = os.open(str(output_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            return None
        try:
            f = os.fdopen(fd, "w", encoding="utf-8")
        except BaseException:
            # os.fdopen() raised before constructing the TextIOWrapper: close the bare fd
            # ourselves (no file object exists to manage it) and clean up the empty file.
            os.close(fd)
            try:
                output_path.unlink()
            except FileNotFoundError:
                pass
            raise
        try:
            with f:
                f.write(payload)
        except BaseException:
            # If write itself fails, ensure no partial file lingers under our exclusive name.
            try:
                output_path.unlink()
            except FileNotFoundError:
                pass
            raise

    snapshot["output_path"] = str(output_path)
    return snapshot


def _closed_hour_window_ms() -> int:
    """The just-completed hour's window start (epoch-ms), floored to hour boundary.
    Per spec/roadmap.md L42: ((now_ms // 1000 - 3600) // 3600) * 3600 * 1000.

    L-1 clamp: returns 0 if wall-clock is before epoch+3600s (degenerate; only
    triggers on system-clock corruption). Servers run forward — clamp is the
    right call vs raising, which would crash the runner on a misconfigured host."""
    w = ((now_ms() // 1000 - 3600) // 3600) * 3600 * 1000
    return max(0, w)


async def _audit_snapshot_runner() -> None:
    """Background task: every BROKER_AUDIT_INTERVAL_SEC (default 3600s),
    snapshot the just-completed hour's message_log events to
    BROKER_AUDIT_DIR/{window_ms}.json. Idempotent (atomic O_EXCL create-or-skip).
    Startup backfill emits the most recently closed hour ONCE before the loop.

    Per spec § Anti-Patterns: NEVER snapshots the current open hour; never walks
    arbitrary backfill history. Wrapped in with_retry like _idempotency_ttl_sweeper."""
    # Allow float interval for test responsiveness; default is the 3600s int per spec.
    # M-3: validate operator-supplied env var ONCE at task entry (not in loop).
    raw_interval = _env_str("BROKER_AUDIT_INTERVAL_SEC", "3600")
    try:
        interval_sec = float(raw_interval)
        if not (interval_sec > 0) or not math.isfinite(interval_sec):
            raise ValueError(f"interval must be > 0 and finite, got {interval_sec!r}")
    except (ValueError, TypeError) as e:
        logger.warning(
            "invalid BROKER_AUDIT_INTERVAL_SEC=%r (%s); falling back to default 3600",
            raw_interval, e,
        )
        interval_sec = 3600.0
    output_dir = Path(_env_str("BROKER_AUDIT_DIR", "audit/pipeline-runs"))
    output_dir.mkdir(parents=True, exist_ok=True)

    async def _emit_for(window_ms: int) -> Optional[dict]:
        async with connect_db() as conn:
            await conn.execute("BEGIN IMMEDIATE")
            try:
                # Runner uses atomic create-or-fail. The OS check IS the check —
                # no separate exists() pre-check (that would be TOCTOU-racy with
                # the manual POST endpoint or a second runner instance).
                result = await _emit_audit_snapshot(
                    window_ms, output_dir, conn, overwrite=False,
                )
                await conn.commit()
                return result
            except Exception:
                await conn.rollback()
                raise

    # Startup backfill: ONE closed-window emit. O_EXCL inside helper handles
    # the "file already exists" case by returning None.
    try:
        window_ms = _closed_hour_window_ms()
        result = await with_retry(_emit_for, window_ms)
        if result is None:
            logger.debug(
                "audit snapshot for window %d already existed; backfill skipped",
                window_ms,
            )
    except Exception as e:
        # F-CC21 A3: full traceback (was message-only).
        logger.warning("audit snapshot backfill failed: %s", e, exc_info=True)

    iter_count = 0  # F-CC21 A7
    while True:
        try:
            await asyncio.sleep(interval_sec)
            iter_count += 1
            if iter_count % 1 == 0:  # F-CC21 A7: log every iter (3600s = once/hour)
                logger.info("audit_runner alive (iter=%d, ~%dh uptime)", iter_count, iter_count)
            window_ms = _closed_hour_window_ms()
            result = await with_retry(_emit_for, window_ms)
            if result is None:
                logger.debug(
                    "audit snapshot for window %d already existed; skipped",
                    window_ms,
                )
        except asyncio.CancelledError:
            return
        except Exception as e:
            # F-CC21 A3: full traceback (was message-only).
            logger.warning("audit snapshot emit failed: %s", e, exc_info=True)


# -------------------------------------------------------------------- Agent message retention (v1.3 / D28)


def _agent_message_archive_dir() -> Path:
    """Compute archive directory for retention runner.

    Uses dedicated env var BROKER_AGENT_MESSAGE_ARCHIVE_DIR (default
    'audit/agent-messages') decoupled from BROKER_AUDIT_DIR shape. This
    avoids the iter-1 bug where BROKER_AUDIT_DIR.parent produced
    unexpected paths for non-nested operator paths (e.g., a flat root
    BROKER_AUDIT_DIR=/var/lib/broker_audit would put archives in /var/lib/).
    """
    return Path(_env_str("BROKER_AGENT_MESSAGE_ARCHIVE_DIR", "audit/agent-messages"))


async def _agent_message_retention_sweep_once(retention_days: int, archive_dir: Path) -> int:
    """One sweep iteration: archive + delete agent_message rows older than
    `retention_days`. Returns the count of rows archived (0 if nothing to do).

    Atomicity: archive write happens BEFORE DELETE inside the SAME transaction;
    if archive write raises, the transaction rolls back and no rows are deleted.

    M3 (iter-2): partial-write idempotency. We capture the archive file size
    BEFORE the write loop and, on any exception during the write loop OR the
    DELETE, truncate the file back to that pre-write size before re-raising.
    Without this, a mid-loop write failure would leave partial bytes on disk;
    the next sweep retry would re-read the same DB rows and APPEND them again,
    duplicating lines in the archive jsonl. Single-process broker so the
    truncate is safe under the GIL — no concurrent sweep can interleave.
    """
    cutoff_ts = now_ms() - retention_days * 86400 * 1000
    async with connect_db() as conn:
        await conn.execute("BEGIN IMMEDIATE")
        try:
            cur = await conn.execute(
                "SELECT m.* FROM message_log m LEFT JOIN delivery d ON d.event_id=m.event_id "
                "LEFT JOIN agent_status a ON a.id=d.recipient_id "
                "WHERE m.kind='agent_message' AND m.ts<? "
                "AND (a.binding_epoch IS NULL) ORDER BY m.event_id",
                (cutoff_ts,),
            )
            rows = await cur.fetchall()
            await cur.close()
            if not rows:
                await conn.commit()
                return 0
            archive_path = archive_dir / f"agent-messages-{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.jsonl"
            archive_path.parent.mkdir(parents=True, exist_ok=True)
            # M3 (iter-2): capture pre-write file size for partial-failure recovery
            pre_write_size = archive_path.stat().st_size if archive_path.exists() else 0
            try:
                with open(archive_path, "a", encoding="utf-8") as f:
                    for r in rows:
                        f.write(json.dumps(dict(r)) + "\n")
                # WS-B-broker-1: delete the delivery shadow-rows for the agent_message
                # rows about to be retention-deleted, in the SAME txn, so no orphaned
                # delivery row outlives its message_log parent (the co-txn insert keeps
                # them consistent on the write path; this keeps them consistent on the
                # delete path). Runs BEFORE the message_log DELETE (the subquery must
                # still see the rows). agent_reply/handoff delivery rows are untouched
                # (only agent_message is retention-swept per contract § 5).
                await conn.execute(
                    "DELETE FROM delivery WHERE event_id IN ("
                    "SELECT m.event_id FROM message_log m "
                    "LEFT JOIN delivery d2 ON d2.event_id=m.event_id "
                    "LEFT JOIN agent_status a ON a.id=d2.recipient_id "
                    "WHERE m.kind='agent_message' AND m.ts<? AND a.binding_epoch IS NULL)",
                    (cutoff_ts,),
                )
                await conn.execute(
                    "DELETE FROM message_log WHERE kind='agent_message' AND ts<? "
                    "AND event_id NOT IN (SELECT d.event_id FROM delivery d "
                    "JOIN agent_status a ON a.id=d.recipient_id "
                    "WHERE a.binding_epoch IS NOT NULL)",
                    (cutoff_ts,),
                )
                await conn.commit()
            except Exception:
                # Truncate archive back to pre-write size to prevent duplicate
                # appends on retry (M3 fix). Single-process broker, so this
                # is safe under the GIL; no concurrent sweep can interleave.
                try:
                    if archive_path.exists():
                        with open(archive_path, "r+b") as f:
                            f.truncate(pre_write_size)
                except Exception as truncate_err:
                    logger.error(
                        "agent_message retention: archive truncate failed during partial-write recovery: %s",
                        truncate_err, exc_info=True,
                    )
                raise  # re-raise original exception; outer handler does the rollback
            logger.info(
                "agent_message retention swept: %d rows archived to %s",
                len(rows), archive_path,
            )
            return len(rows)
        except Exception:
            await conn.rollback()
            raise


async def _agent_message_retention_runner() -> None:
    """Background task: every BROKER_RETENTION_INTERVAL_SEC (default 86400s = daily),
    archive + delete agent_message rows older than BROKER_AGENT_MESSAGE_RETENTION_DAYS
    (default 30). Mirrors `_audit_snapshot_runner` exception discipline:
    cancellation drops cleanly; per-iter exceptions are logged but don't kill the task.
    """
    interval_sec = _env_int("BROKER_RETENTION_INTERVAL_SEC", 86400)
    retention_days = _env_int("BROKER_AGENT_MESSAGE_RETENTION_DAYS", 30)
    archive_dir = _agent_message_archive_dir()
    logger.info(
        "agent_message retention runner: interval=%ss, retention_days=%s, archive_dir=%s",
        interval_sec, retention_days, archive_dir,
    )
    iter_count = 0  # F-CC21 A7
    while True:
        try:
            await asyncio.sleep(interval_sec)
            iter_count += 1
            logger.info("retention_runner alive (iter=%d, ~%dd uptime)", iter_count, iter_count)
            await with_retry(_agent_message_retention_sweep_once,
                             retention_days, archive_dir)
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.error("agent_message retention runner error: %s", e, exc_info=True)


# -------------------------------------------------------------------- File watcher (pipelines_view)

class _ProjectStateHandler(FileSystemEventHandler):
    def __init__(self, project_name: str, state_db_path: str, loop: asyncio.AbstractEventLoop):
        super().__init__()
        self.project_name = project_name
        self.state_db_path = state_db_path
        self.loop = loop
        # Per F7: normalize once for case-insensitive + slash-tolerant comparison
        self._target_norm = norm_path(state_db_path)

    def on_modified(self, event):  # type: ignore[override]
        if event.is_directory:
            return
        try:
            if norm_path(event.src_path) != self._target_norm:
                return
        except (OSError, ValueError):
            # Path resolution can fail on transient files; treat as miss
            return
        # Bridge to asyncio loop
        asyncio.run_coroutine_threadsafe(
            schedule_pipelines_view_refresh(self.project_name), self.loop
        )


_shared_observer: Optional[Observer] = None  # type: ignore[type-arg]  # one Observer for all projects
_watches: dict[str, tuple] = {}   # project_name -> (norm_dir, handler): this project's handler on its dir's watch
_dir_watch: dict[str, object] = {}  # norm_dir -> shared ObservedWatch (ONE emitter per watched directory)
_pending_refreshes: dict[str, asyncio.Task] = {}
_refresh_locks: dict[str, asyncio.Lock] = {}
_refresh_debounce_sec: float = 5.0
_refreshes_shutting_down: bool = False  # set during lifespan shutdown to refuse new debounced refreshes
_refresh_tasks: set[asyncio.Task] = set()  # ALL live debounced-refresh tasks (incl. orphaned/replaced) for shutdown drain


async def schedule_pipelines_view_refresh(project_name: str) -> None:
    """Debounce: cancel any pending refresh for this project + schedule new one in 5s."""
    if _refreshes_shutting_down:
        return
    pending = _pending_refreshes.get(project_name)
    if pending and not pending.done():
        pending.cancel()

    async def _delayed():
        try:
            await asyncio.sleep(_refresh_debounce_sec)
            await refresh_pipelines_view(project_name)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.warning("debounced refresh failed for %s: %s", project_name, e)

    task = asyncio.create_task(_delayed())
    # Track EVERY live refresh task (not just the per-project current one) so lifespan
    # shutdown can cancel + await orphaned/replaced tasks before closing foreign conns — a
    # replaced (cancelled-but-unwinding) task may still be inside connect_foreign_cached.
    # (Discriminator MEDIUM iter-2, 2026-06-08.) The done-callback self-discards on finish.
    _refresh_tasks.add(task)
    task.add_done_callback(_refresh_tasks.discard)
    _pending_refreshes[project_name] = task


async def refresh_pipelines_view(project_name: str) -> int:
    """Read project state.db, upsert into coordination.db.pipelines_view. Returns row count."""
    lock = _refresh_locks.setdefault(project_name, asyncio.Lock())
    async with lock:
        # Look up state_db_path
        async with connect_db() as conn:
            cur = await conn.execute(
                "SELECT state_db_path FROM project_registry WHERE name = ?", (project_name,)
            )
            row = await cur.fetchone()
            await cur.close()
            if not row:
                logger.warning("refresh_pipelines_view: project %s not registered", project_name)
                return 0
            state_db_path = row["state_db_path"]

        if not Path(state_db_path).exists():
            logger.warning("refresh_pipelines_view: state.db missing at %s", state_db_path)
            return 0

        # Read project state.db (F-CC36 follow-up: cached long-lived conn — no per-refresh thread spawn)
        async with connect_foreign_cached(state_db_path) as project_conn:
            cur = await project_conn.execute(
                "SELECT id, shape, started_at, completed_at, status FROM pipelines"
            )
            pipelines = await cur.fetchall()
            await cur.close()

            # Step counts per pipeline
            pipeline_rows: list[tuple] = []
            now = now_ms()
            for p in pipelines:
                cur2 = await project_conn.execute(
                    "SELECT COUNT(*) AS n, "
                    "  SUM(CASE WHEN status='done' THEN 1 ELSE 0 END) AS done "
                    "FROM pipeline_steps WHERE pipeline_id = ?",
                    (p["id"],),
                )
                step_row = await cur2.fetchone()
                await cur2.close()
                pipeline_rows.append((
                    project_name, p["id"], p["shape"], p["started_at"], p["completed_at"],
                    p["status"], step_row["n"] or 0, step_row["done"] or 0, now,
                ))

        # Upsert into coordination.db (F4: wrapped in with_retry for SQLITE_BUSY backoff)
        async def _commit_view():
            async with connect_db() as conn:
                await conn.execute("BEGIN IMMEDIATE")
                try:
                    # Delete existing rows for project (simpler than upsert when row count may shrink)
                    await conn.execute("DELETE FROM pipelines_view WHERE project_name = ?", (project_name,))
                    await conn.executemany(
                        "INSERT INTO pipelines_view "
                        "(project_name, pipeline_id, shape, started_at, completed_at, status, "
                        " step_count, steps_done, last_refresh_ts) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        pipeline_rows,
                    )
                    await conn.execute(
                        "UPDATE project_registry SET last_refresh_ts = ? WHERE name = ?",
                        (now, project_name),
                    )
                    await conn.commit()
                except Exception:
                    await conn.rollback()
                    raise

        await with_retry(_commit_view)

        logger.info("pipelines_view refreshed for %s: %d rows", project_name, len(pipeline_rows))
        return len(pipeline_rows)


def start_watcher_for_project(project_name: str, state_db_path: str, loop: asyncio.AbstractEventLoop) -> None:
    """Schedule one project's state.db dir on the single shared watchdog Observer.

    F-CC36 follow-up: ONE shared Observer (a single dispatch/run thread) schedules every
    project via one schedule() per project, replacing the prior one-Observer-PER-project
    model (which ran one dispatch thread per project). NB: each watched directory still has
    its OWN OS emitter thread (Windows ReadDirectoryChangesW is per-directory) — emitters
    are NOT merged; what this eliminates is the N-1 redundant per-Observer DISPATCH threads.
    Net ~N-1 fewer persistent threads (defense-in-depth for the memory-pressure root env;
    the wedge itself is fixed by D50). Verified live 2026-06-08 (py-spy): exactly 1
    `dispatch_events` thread + one `read_directory_changes` emitter per watched dir.
    """
    global _shared_observer
    parent_dir = str(Path(state_db_path).parent)
    if not Path(parent_dir).exists():
        logger.warning("start_watcher: parent dir %s missing for %s", parent_dir, project_name)
        return
    if _shared_observer is None:
        _shared_observer = Observer()
        _shared_observer.daemon = True
        _shared_observer.start()
    # Shared-directory-safe (re-)registration. Multiple projects CAN share a parent dir
    # (verified live 2026-06-08: 4 such pairs), and watchdog attaches every project's handler
    # to ONE shared watch/emitter per directory. So track ONE ObservedWatch per normalized
    # dir (_dir_watch) + one handler per project (_watches), and on re-register remove ONLY
    # this project's prior handler (never the whole watch) so co-located projects keep
    # receiving events. Free a dir's emitter only when no project still watches it.
    try:
        new_dir = norm_path(parent_dir)
    except (OSError, ValueError):
        new_dir = parent_dir

    prior = _watches.pop(project_name, None)
    if prior is not None:
        prior_dir, prior_handler = prior
        prior_watch = _dir_watch.get(prior_dir)
        if prior_watch is not None:
            try:
                _shared_observer.remove_handler_for_watch(prior_handler, prior_watch)
            except Exception:
                pass
            # Free the old dir's emitter iff this project moved dirs AND no other project
            # still watches the old dir.
            if prior_dir != new_dir and all(d != prior_dir for (d, _h) in _watches.values()):
                try:
                    _shared_observer.unschedule(prior_watch)
                except Exception:
                    pass
                _dir_watch.pop(prior_dir, None)

    handler = _ProjectStateHandler(project_name, state_db_path, loop)
    watch = _dir_watch.get(new_dir)
    if watch is None:
        watch = _shared_observer.schedule(handler, parent_dir, recursive=False)
        _dir_watch[new_dir] = watch
    else:
        _shared_observer.add_handler_for_watch(handler, watch)
    _watches[project_name] = (new_dir, handler)


def _is_local_registry_row(row) -> bool:
    """Return True iff a project_registry row was registered with local_validation=1.

    Single source of truth for the local-vs-remote registry-row predicate.
    Both the lifespan startup loop (filtering which projects get watchers
    started + initial pipelines_view refresh) and the lifespan-gate test
    (test_lifespan_startup_gate_filters_remote_rows) call this helper to
    avoid copy-paste drift between production and test code.

    `row` is an aiosqlite Row (or any mapping with a 'local_validation'
    integer column); the SQLite INTEGER 0/1 is normalized to a Python bool.
    """
    return bool(row["local_validation"])


def stop_all_watchers() -> None:
    global _shared_observer
    obs = _shared_observer
    if obs is not None:
        try:
            obs.stop()
            obs.join(timeout=2.0)
        except Exception:
            pass
    _shared_observer = None
    _watches.clear()
    _dir_watch.clear()


# -------------------------------------------------------------------- Lifespan task observability + heartbeat (F-CC21 A2 + A6)

def _surface_task_death(name: str, task: asyncio.Task) -> asyncio.Task:
    """Attach done-callback that logs unhandled task exceptions WITH traceback.

    Per F-CC21 (broker stability investigation, 2026-05-26): lifespan background
    tasks were spawned with bare asyncio.create_task — no observability if a
    BaseException escapes the inner while-True loop. After this wrap, any task
    death lands in broker.log with full traceback. Per C-17: does NOT respawn;
    fix the underlying cause, not the symptom.
    """
    def _cb(t: asyncio.Task) -> None:
        if t.cancelled():
            logger.info("background task %r cancelled (clean shutdown)", name)
            return
        exc = t.exception()
        if exc is not None:
            logger.error(
                "background task %r DIED with %s: %s",
                name, type(exc).__name__, exc, exc_info=exc,
            )
        else:
            logger.warning("background task %r exited cleanly (unexpected)", name)
    task.add_done_callback(_cb)
    return task


async def _heartbeat_writer() -> None:
    """Write epoch-ms to logs/broker.heartbeat every 30s.

    F-CC21 (broker stability A6, 2026-05-26): pure observability for time-of-death
    correlation. NO consumer reads this in current scope (no watchdog per C-17 —
    bandaid on top of unknown bug forbidden). Future use: post-mortem timeline
    reconstruction. File mtime == "last known broker alive" timestamp.
    """
    heartbeat_path = Path.cwd() / "logs" / "broker.heartbeat"
    heartbeat_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = heartbeat_path.with_suffix(".tmp")
    iter_count = 0
    while True:
        try:
            await asyncio.sleep(30)
            tmp_path.write_text(str(now_ms()), encoding="utf-8")
            os.replace(tmp_path, heartbeat_path)  # atomic
            iter_count += 1
            if iter_count % 120 == 0:  # log once per hour (every 60 min)
                logger.info("heartbeat alive (iter=%d, ~%dh uptime)", iter_count, iter_count // 120)
            # F-CC21 A8: resource snapshot every 5 min (10 × 30s heartbeat).
            # Catches slow leaks (SSE subscriber queue growth, observer accumulation)
            # that would otherwise only surface as OOM/file-handle exhaustion.
            if iter_count % 10 == 0:
                try:
                    logger.info(
                        "resource snapshot: subscribers=%d watches=%d dirs=%d foreign_conns=%d",
                        len(_subscribers), len(_watches), len(_dir_watch), len(_foreign_conns),
                    )
                except Exception as e:  # noqa: BLE001 — diagnostic; never block heartbeat
                    logger.debug("resource snapshot failed: %s", e, exc_info=True)
        except asyncio.CancelledError:
            return
        except Exception as e:  # noqa: BLE001 — diagnostic; never let heartbeat task die
            logger.warning("heartbeat write failed: %s", e, exc_info=True)


# -------------------------------------------------------------------- Lifespan

@asynccontextmanager
async def lifespan(app: FastAPI):
    # F-CC36: open the coordination.db connection pool FIRST so all startup +
    # request DB access reuses long-lived connections (root-cause fix for the
    # 2026-06-08 event-loop wedge). Created here at boot when memory is freshest.
    global _db_pool, _refreshes_shutting_down
    _refreshes_shutting_down = False  # reset so a re-entered lifespan (tests) re-enables refreshes
    _db_pool = _ConnectionPool(
        _env_str("BROKER_DB_PATH", "coordination.db"),
        _env_int("BROKER_DB_POOL_SIZE", 5),
    )
    await _db_pool.open()

    # Startup: schema, watchers for already-registered projects, TTL sweeper
    await ensure_schema()

    # Bearer authentication is opt-in. The public installer selects loopback;
    # operators choosing a non-loopback bind must configure their network trust
    # and BROKER_BEARER_TOKEN explicitly.
    bind_host = _env_str("BROKER_HOST", "0.0.0.0")  # retained for log-line below

    # Start watchers for already-registered projects + initial refresh
    # D27 iter-2 (H1): gate on local_validation column. Remote-registered rows
    # (local_validation=0) point at paths the broker cannot reach — starting a
    # watcher would create a forever-idle Observer thread watching CWD for a
    # literal-named file that will never appear, and refresh_pipelines_view
    # would early-return 0 silently. Skip both and emit one INFO line so
    # operators can see the count of remote rows at startup.
    loop = asyncio.get_running_loop()
    async with connect_db() as conn:
        cur = await conn.execute(
            "SELECT name, state_db_path, local_validation FROM project_registry"
        )
        rows = await cur.fetchall()
        await cur.close()
    skipped_remote = 0
    for row in rows:
        if not _is_local_registry_row(row):
            skipped_remote += 1
            logger.info("startup: skipping remote-registered project: %s", row["name"])
            continue
        try:
            start_watcher_for_project(row["name"], row["state_db_path"], loop)
            await refresh_pipelines_view(row["name"])
        except Exception as e:
            logger.warning("startup watcher/refresh failed for %s: %s", row["name"], e)
    if skipped_remote:
        logger.info(
            "startup: skipped %d remote-registered project(s) (local_validation=0)",
            skipped_remote,
        )

    ttl_task = _surface_task_death(
        "ttl_sweeper", asyncio.create_task(_idempotency_ttl_sweeper())
    )
    app.state.ttl_task = ttl_task
    audit_task = _surface_task_death(
        "audit_runner", asyncio.create_task(_audit_snapshot_runner())
    )
    app.state.audit_task = audit_task
    retention_task = _surface_task_death(
        "retention_runner", asyncio.create_task(_agent_message_retention_runner())
    )
    app.state.retention_task = retention_task
    heartbeat_task = _surface_task_death(
        "heartbeat_writer", asyncio.create_task(_heartbeat_writer())
    )
    app.state.heartbeat_task = heartbeat_task
    # F-CC27: self-probe task — detects alive-but-frozen request-handler class.
    # Spawn LAST so it cancels FIRST (LIFO shutdown order below).
    self_probe_task = _surface_task_death(
        "self_health_probe", asyncio.create_task(_self_health_probe())
    )
    app.state.self_probe_task = self_probe_task

    # Escalation is opt-in and alerts only; it never restarts or acknowledges
    # anything. Enable it only with configured Discord credentials/destination
    # and working delivery mirrors. Start after the self-probe so its network
    # task cancels before the listener and database pool disappear.
    if _env_bool("BROKER_REAPER_ENABLED", False):
        reaper_task = _surface_task_death(
            "relay_reaper", asyncio.create_task(_relay_escalation_reaper())
        )
        app.state.reaper_task = reaper_task
    else:
        reaper_task = None
        app.state.reaper_task = None
        logger.info("relay escalation reaper disabled (BROKER_REAPER_ENABLED=false)")

    # M2 Discord poller (D16 § (a)-(e)). Graceful: missing token / .env / access.json
    # → log WARN + skip poller; broker continues to serve all other endpoints.
    # Lazy import to avoid circular import (discord_poller imports server).
    try:
        from broker import discord_poller as _dp
        await _dp.maybe_start_poller()
    except Exception as e:
        # F-CC21 A3: full traceback (was type(e).__name__ only — masked every exception class).
        logger.warning("discord_poller startup raised: %s — broker continues", e, exc_info=True)

    logger.info("broker startup complete on %s:%s", bind_host, _env_int("BROKER_PORT", 8787))
    # F-CC22: log active event loop type so we can verify Stage C fix from logs alone.
    _loop_type = type(asyncio.get_running_loop()).__name__
    logger.info("asyncio event loop type: %s (F-CC22: SelectorEventLoop on Windows)", _loop_type)

    try:
        yield
    finally:
        # Shutdown — LIFO order: reaper → self_probe → heartbeat → retention → audit → ttl.
        # The reaper was spawned LAST (after self-probe) and does a network POST, so
        # cancel it FIRST — before the listener/pool tear down. Guarded: it is None
        # when BROKER_REAPER_ENABLED=false (never spawned).
        if reaper_task is not None:
            reaper_task.cancel()
            try:
                await reaper_task
            except (asyncio.CancelledError, Exception):
                pass
        # Self-probe was spawned LAST per F-CC27; cancel FIRST so its httpx
        # client teardown doesn't race against the listener going away.
        self_probe_task.cancel()
        try:
            await self_probe_task
        except (asyncio.CancelledError, Exception):
            pass
        # Heartbeat was spawned next-to-last per F-CC21 A6; cancel next so
        # file-write backpressure doesn't delay other task cancellations.
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except (asyncio.CancelledError, Exception):
            pass
        retention_task.cancel()
        try:
            await retention_task
        except (asyncio.CancelledError, Exception):
            pass
        audit_task.cancel()
        try:
            await audit_task
        except (asyncio.CancelledError, Exception):
            pass
        ttl_task.cancel()
        try:
            await ttl_task
        except (asyncio.CancelledError, Exception):
            pass
        app.state.audit_task = None
        app.state.retention_task = None
        app.state.ttl_task = None
        app.state.heartbeat_task = None
        app.state.self_probe_task = None
        app.state.reaper_task = None
        try:
            from broker import discord_poller as _dp
            await _dp.maybe_stop_poller()
        except Exception as e:
            # F-CC21 A3: full traceback (was type(e).__name__ only).
            logger.warning("discord_poller shutdown raised: %s", e, exc_info=True)
        _refreshes_shutting_down = True
        stop_all_watchers()
        # F-CC36 follow-up: cancel + await EVERY live debounced refresh task (including
        # orphaned/replaced ones no longer in _pending_refreshes) so none is still unwinding
        # inside connect_foreign_cached when we close foreign conns. (Discriminator MEDIUM
        # iter-2.) The flag above refuses any new schedule a late observer event delivers.
        _pending = list(_refresh_tasks)
        for _t in _pending:
            # Don't SECOND-cancel a task a prior re-schedule already cancelled: a 2nd
            # CancelledError could land on the unshielded `await conn.rollback()` in
            # connect_foreign_cached's finally (CancelledError is not caught by its
            # `except Exception`), interrupting cleanup before close_foreign_conns().
            # cancelling()>0 means a cancel is already pending — just gather it. Each task
            # thus gets at most one cancel and still fully unwinds before the gather
            # returns. (Discriminator MEDIUM iter-3, 2026-06-08.)
            if not _t.done() and _t.cancelling() == 0:
                _t.cancel()
        if _pending:
            await asyncio.gather(*_pending, return_exceptions=True)
        _pending_refreshes.clear()
        _refresh_tasks.clear()
        await drain_subscribers()
        # F-CC36 follow-up: close cached foreign (project state.db) connections before the
        # pool — no refresh can run now that watchers + debounce tasks are stopped.
        await close_foreign_conns()
        # F-CC36: close the pool LAST — after every task/watcher/subscriber that
        # could touch the DB has stopped.
        if _db_pool is not None:
            await _db_pool.close()
            _db_pool = None


# -------------------------------------------------------------------- App + middleware

app = FastAPI(title="agent-orchestration-research broker", version="1.0", lifespan=lifespan)
app.state.ttl_task = None
app.state.audit_task = None
app.state.retention_task = None
app.state.heartbeat_task = None  # F-CC21 A6
app.state.self_probe_task = None  # F-CC27
app.state.reaper_task = None  # #18 escalation reaper

# ---- Dashboard router mount (C-20: dashboard package owned by broker_dashboard;
# broker owns ONLY this mount line). Read-only /api/dashboard/* + /dashboard routes.
# The `dashboard` package never imports `server` (no circular import); it opens
# coordination.db read-only (mode=ro). First include_router in the file; no path
# collisions (verified). Fail-loud if the package is missing (broker discipline).
# Dashboard routes are still wrapped by the request_id + bearer middleware below.
from broker.dashboard import router as dashboard_router
app.include_router(dashboard_router)


# F12 (iter3 discriminator) + F-CC27 iter-2 L1 update: MIDDLEWARE REGISTRATION
# ORDER MATTERS.
#
# Per Starlette's `build_middleware_stack`, `app.user_middleware` is consumed
# in reverse — the FIRST `@app.middleware('http')` registered becomes the
# INNERMOST wrapper, and the LAST registered becomes the OUTERMOST.
#
# We need `request_id_middleware` to be the OUTERMOST so that even when
# `bearer_auth_middleware` short-circuits with `_err_response` (401 missing
# bearer / 401 invalid bearer / 500 broker_misconfig), the response still
# passes through `request_id_middleware` on the way out — guaranteeing every
# response carries an `X-Request-ID` header AND that `request.state.request_id`
# is set so `_err_response` can stamp the body's `error.request_id` (instead of
# falling back to the literal string "unknown").
#
# CURRENT registration order (top-to-bottom = first-to-last):
#   1. `bearer_auth_middleware`       (registered first → innermost)
#   2. `request_counter_middleware`   (registered second → middle; F-CC27)
#   3. `request_id_middleware`        (registered last → OUTERMOST)
#
# Execution order (outer → inner → handler → inner → outer):
#   request_id → request_counter → bearer_auth → handler
#
# The F-CC27 counter SITS BETWEEN request_id and bearer_auth. Safe at this
# position: it only mutates module-level counters; does not gate auth; does not
# touch headers; never raises on its own. It reads `request.state.request_id`
# AFTER request_id_middleware sets it, so trace lines include the real UUID
# rid (not '?'). It wraps `await call_next(...)` in try/finally so bearer_auth
# short-circuits still flow through it cleanly (active counter decremented;
# completed counter incremented).
#
# DO NOT REORDER without re-reading this comment + Starlette's stack semantics.


@app.middleware("http")
async def bearer_auth_middleware(request: Request, call_next):
    # Per D26 (2026-05-08; user override extending D21 home LAN trust):
    # bearer is OPT-IN. If BROKER_BEARER_TOKEN is unset, all requests pass —
    # this is the documented home-LAN-trust mode. Setting the token re-enables
    # enforcement (preserved for any future opt-in deployment).
    expected = _env_str("BROKER_BEARER_TOKEN", "")
    if not expected:
        return await call_next(request)
    bind_host = _env_str("BROKER_HOST", "0.0.0.0")
    if bind_host in LOOPBACK_HOSTS:
        return await call_next(request)
    # Health endpoint always exempt per contract
    if request.url.path == "/api/health":
        return await call_next(request)
    # OMP private and operator routes own distinct Authorization schemes. They
    # replace rather than stack the optional deployment bearer on these paths.
    path = request.url.path
    auth = request.headers.get("authorization", "")
    if path.startswith("/api/operator/"):
        return await call_next(request)
    if (
        request.method == "POST" and _OMP_OFFER_PATH_RE.fullmatch(path)
    ) or (
        request.method == "GET" and _OMP_DELIVERY_STATUS_PATH_RE.fullmatch(path)
    ):
        return await call_next(request)
    # Shared legacy/OMP mutation paths must inspect the request variant before
    # applying deployment Bearer auth; an OMP request with a missing RelayControl
    # credential must reach the handler and return CONTROL_REQUIRED.
    shared_match = _OMP_SHARED_PATH_RE.fullmatch(path)
    shared_operation = shared_match.group("operation") if shared_match else None
    is_shared_method = (
        request.method == "POST" and shared_operation in {"status", "delivery-ack"}
    ) or (
        request.method == "PUT" and shared_operation == "relay-name"
    )
    if is_shared_method:
        payload = _decode_shared_json_bytes(await request.body())
        if _is_omp_shared_payload(shared_operation, auth, payload):
            return await call_next(request)
    if not auth.startswith("Bearer "):
        return _err_response(request, 401, "missing_bearer", "Authorization: Bearer <token> required")
    token = auth[len("Bearer "):].strip()
    if token != expected:
        return _err_response(request, 401, "invalid_bearer", "Bearer token does not match")
    return await call_next(request)


# -------------------------------------------------------------------- F-CC27: alive-but-frozen observability
#
# Incident 2026-06-04: broker process alive, background tasks (heartbeat /
# ttl_sweeper / audit / resource_snapshot) still emitting, TCP socket bound +
# LISTENING, but HTTP requests not processed. User had to find the process and
# kill it manually. Prior incidents were process-death (logged via faulthandler
# + atexit + _surface_task_death); this one is wedged-request-handler — a
# different failure class.
#
# Per C-17 (no symptom mitigation without root-cause diagnosis): we add
# observability so the NEXT freeze leaves enough evidence to pick between:
#   H1: uvicorn request-handler stack deadlocked (single bad handler holding
#       the loop) → request count rises, completion count stops, active stays
#       elevated.
#   H2: OS accept-queue full / Windows IOCP-vs-SelectorEventLoop wedge →
#       self-probe times out while heartbeat keeps ticking.
#   H3: SQLite WAL writer wedge → all request handlers block on connect_db /
#       BEGIN IMMEDIATE; show up as slow handlers (when BROKER_REQUEST_TRACE=1).
#   H4: discord_poller asyncio.Lock starvation → same as H2 + visible in
#       per-task heartbeat cadence.
#
# Nothing in this block changes broker behavior. Pure logging + counters.

# Atomic counters (module-level; updated by request_counter_middleware below).
# Asyncio is single-threaded; `+= 1` on a module-level int is safe between
# await points. No lock needed.
_request_active: int = 0
_request_total: int = 0
_request_completed: int = 0


# Counter middleware registered HERE (between bearer_auth above and request_id
# below) so Starlette's reverse-stack semantics give us:
#   execution: request_id (outermost; sets rid) → request_counter (reads rid)
#              → bearer_auth → handler.
# Per F-CC27 iter-1 L1: this preserves the F12 invariant ("request_id outermost
# so bearer_auth short-circuit 401/500 responses still get rid set + header
# stamped") AND lets the counter trace lines include the real rid (not '?').
@app.middleware("http")
async def request_counter_middleware(request: Request, call_next):
    """Maintain (active, total, completed) counters around every HTTP request.

    Position: middle (between bearer_auth and request_id in execution order).
    Safe at this position: only mutates module-level counters; does not gate
    auth; does not touch headers; never raises on its own; ALWAYS decrements
    active in finally even when a downstream handler raises (so a wedged
    handler that never completes shows up as elevated active count rather than
    a leaked counter).

    Optional per-request entry/exit log when env BROKER_REQUEST_TRACE=1.
    Off by default (avoids ~3 log lines per request in steady-state); turn on
    in a diagnostic window when a freeze is suspected.
    """
    global _request_active, _request_total, _request_completed
    trace = _env_bool("BROKER_REQUEST_TRACE", False)
    # request_id is set by request_id_middleware which runs OUTER to us (per
    # F-CC27 iter-1 L1 registration order); rid is therefore populated here.
    rid = getattr(request.state, "request_id", "?")
    method = request.method
    path = request.url.path
    start_ms = time.perf_counter() * 1000.0

    _request_active += 1
    _request_total += 1
    if trace:
        logger.info("req-enter rid=%s active=%d total=%d %s %s",
                    rid, _request_active, _request_total, method, path)
    try:
        response = await call_next(request)
        if trace:
            elapsed = (time.perf_counter() * 1000.0) - start_ms
            logger.info("req-exit  rid=%s elapsed_ms=%.1f status=%s %s %s",
                        rid, elapsed, getattr(response, "status_code", "?"), method, path)
        return response
    except Exception:
        if trace:
            elapsed = (time.perf_counter() * 1000.0) - start_ms
            logger.info("req-raise rid=%s elapsed_ms=%.1f %s %s",
                        rid, elapsed, method, path)
        raise
    finally:
        _request_active -= 1
        _request_completed += 1


@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    """F8 (iter2 discriminator): ALWAYS generate a server-side UUID4 for
    request_id. Never trust X-Request-ID from the client.

    Rationale: a token-shaped X-Request-ID would otherwise be reflected in
    every error response body (`error.request_id`) AND in the response
    header — bypassing F1's body sanitizer because request_id is allowed by
    _SAFE_HTTP_DETAIL_KEYS / _err_response by design.

    Server-generated UUIDs are unpredictable and tie response→log without
    trusting input. A "validate-and-echo-if-safe" approach has zero benefit
    over server-generated UUIDs and only adds attack surface; UUID is faster
    and simpler.

    The client's X-Request-ID MAY be used server-side as a diagnostic
    correlation field, but ONLY length-truncated and ONLY in the server log
    (never in response body or response header).

    F12 (iter3 discriminator) + F-CC27 iter-3 L1: this middleware is registered
    LAST (third, after bearer_auth and request_counter) so it becomes the
    OUTERMOST in Starlette's stack — guaranteeing it runs on EVERY response,
    including bearer_auth short-circuit 401/500s. See the block comment above
    for the full ordering rationale.
    """
    request.state.request_id = str(uuid.uuid4())
    # Diagnostic-only: log client's X-Request-ID truncated to 64 chars under
    # our server-generated request_id. Never echoed.
    client_rid = request.headers.get("X-Request-ID")
    if client_rid:
        client_rid_safe = client_rid[:64]
        logger.debug("client X-Request-ID=%r mapped to server rid=%s",
                     client_rid_safe, request.state.request_id)
    response = await call_next(request)
    # F8: emit server-generated rid as response header so callers can correlate
    # with server logs without ever reflecting attacker-controlled input.
    response.headers["X-Request-ID"] = request.state.request_id
    return response


# -------------------------------------------------------------------- F-CC27: self-health-probe task
# (Counter middleware + module-level counters live above bearer_auth/request_id
# per F-CC27 iter-1 L1 ordering. This self-probe task is independent — it
# spawns in the lifespan and runs in the asyncio loop, not as middleware.)


async def _self_health_probe() -> None:
    """Probe http://127.0.0.1:{port}/api/health every 60s from inside the broker.

    Detects the H2 class (accept-queue / event-loop wedge): if the self-probe
    times out or fails while the heartbeat task is still emitting, the request
    path is wedged but the asyncio loop is alive. Useful diagnostic separator
    vs the H1 class (full event-loop deadlock — both probe AND heartbeat would
    stop in that case).

    NOT a watchdog: never restarts, never escalates, never auto-recovers
    (per C-17). Only logs. Operator sees the WARNING line in broker.log and
    knows the freeze has started in real time rather than discovering it later
    when peers report it.
    """
    port = _env_int("BROKER_PORT", 8787)
    url = f"http://127.0.0.1:{port}/api/health"
    timeout_s = _env_int("BROKER_SELF_PROBE_TIMEOUT_S", 5)
    interval_s = _env_int("BROKER_SELF_PROBE_INTERVAL_S", 60)
    iter_count = 0
    # Reuse one client across iterations — avoids per-iter connection cost.
    async with httpx.AsyncClient(timeout=timeout_s) as client:
        while True:
            try:
                await asyncio.sleep(interval_s)
                t0 = time.perf_counter()
                try:
                    resp = await client.get(url)
                    elapsed_ms = (time.perf_counter() - t0) * 1000.0
                    if resp.status_code != 200:
                        logger.warning(
                            "self-probe: HTTP %d in %.1fms — broker is responding "
                            "but health endpoint reports non-200 (active=%d total=%d completed=%d)",
                            resp.status_code, elapsed_ms,
                            _request_active, _request_total, _request_completed,
                        )
                    else:
                        iter_count += 1
                        if iter_count % 60 == 0:  # once per hour at 60s interval
                            logger.info(
                                "self-probe alive (iter=%d, latency=%.1fms, active=%d total=%d completed=%d)",
                                iter_count, elapsed_ms,
                                _request_active, _request_total, _request_completed,
                            )
                except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError) as e:
                    elapsed_ms = (time.perf_counter() - t0) * 1000.0
                    logger.error(
                        "self-probe: %s after %.1fms — request path appears wedged "
                        "(active=%d total=%d completed=%d). "
                        "Heartbeat task still alive means asyncio loop is fine but "
                        "request handlers are not progressing. Next steps: set "
                        "BROKER_REQUEST_TRACE=1 and restart broker to capture "
                        "per-request entry/exit on next freeze.",
                        type(e).__name__, elapsed_ms,
                        _request_active, _request_total, _request_completed,
                    )
            except asyncio.CancelledError:
                return
            except Exception as e:  # noqa: BLE001 — diagnostic; never let probe task die
                logger.warning("self-probe loop failed (continuing): %s", e, exc_info=True)


def _err_response(request: Request, code_int: int, code_str: str, message: str,
                  details: Optional[dict] = None) -> JSONResponse:
    rid = getattr(request.state, "request_id", None) or "unknown"
    body = {"error": {"code": code_str, "message": message, "request_id": rid}}
    if details:
        body["error"]["details"] = details
    return JSONResponse(status_code=code_int, content=body)


# F1 (iter1 discriminator): error handlers must NOT echo request bodies, headers,
# or any other user-supplied content. Pydantic/FastAPI validation errors include
# the original `input` / `input_value` / `ctx` of the bad field — if the request
# body contained a Discord bot token, returning exc.errors() verbatim would echo
# that token back. We project ONLY {loc, type, msg} into the response.

_SAFE_VALIDATION_KEYS = ("loc", "type", "msg")


def _sanitize_validation_errors(errors: list[Any]) -> list[dict[str, Any]]:
    """Strip user-supplied input/ctx values from Pydantic error dicts; keep only
    {loc, type, msg} which are spec/structural metadata, not raw inputs.
    `loc` is a tuple of field names + indices — never the input value."""
    out: list[dict[str, Any]] = []
    for err in errors:
        if not isinstance(err, dict):
            continue
        out.append({k: err[k] for k in _SAFE_VALIDATION_KEYS if k in err})
    return out


# Whitelist of safe keys when an HTTPException is raised with detail=dict.
# These are the only keys callers may put in detail and have echoed in the response.
_SAFE_HTTP_DETAIL_KEYS = {"code", "message", "request_id"}


@app.exception_handler(StarletteHTTPException)
async def _http_exc_handler(request: Request, exc: StarletteHTTPException):
    code_str = f"http_{exc.status_code}"
    detail = exc.detail
    if isinstance(detail, dict):
        # Only whitelisted keys cross the boundary.
        message = str(detail.get("message", f"HTTP {exc.status_code}"))
        safe_extra = {k: v for k, v in detail.items() if k in _SAFE_HTTP_DETAIL_KEYS and k != "message"}
        return _err_response(request, exc.status_code, code_str, message,
                             details=safe_extra or None)
    if detail:
        # Strings are accepted as-is. Endpoints MUST NOT interpolate user-supplied
        # identifiers (channel_id, thread_id, etc.) into detail strings — see the
        # DELETE /api/discord/poll-targets handler for the canonical pattern.
        return _err_response(request, exc.status_code, code_str, str(detail))
    return _err_response(request, exc.status_code, code_str, f"HTTP {exc.status_code}")


@app.exception_handler(RequestValidationError)
async def _validation_handler(request: Request, exc: RequestValidationError):
    return _err_response(
        request, 422, "validation_error", "request body failed validation",
        details={"errors": _sanitize_validation_errors(exc.errors())},
    )


@app.exception_handler(Exception)
async def _generic_handler(request: Request, exc: Exception):
    # Log includes exception class + traceback (server-side only).
    logger.exception("unhandled exception in %s %s", request.method, request.url.path)
    # Response body is fixed-string only — never str(exc), never class name.
    # The request_id ties the response to the server log for diagnosis.
    return _err_response(request, 500, "internal_error", "internal server error")


# -------------------------------------------------------------------- Routes

def _parse_omp_status_body(agent_id: str, body: Any) -> tuple[str, dict[str, Any]]:
    _canonical_uuid(agent_id)
    base = {
        "schema_version", "transport", "kind", "channel", "thread_id", "status",
        "current_task", "last_activity_ts", "display_name", "operation",
    }
    operation = body.get("operation") if isinstance(body, dict) else None
    if operation == "bind":
        fields = base | {
            "bind_attempt_id", "expected_binding_epoch", "new_binding_epoch",
            "omp_process_epoch", "instance_id", "control_epoch", "control_token_hash",
        }
    elif operation == "refresh":
        fields = base | {"binding_epoch", "omp_process_epoch", "request_id"}
    else:
        raise _OmpFailure(422, "PROTOCOL_INVALID")
    value = _closed_object(body, fields)
    if (
        type(value["schema_version"]) is not int
        or value["schema_version"] != 1
        or value["transport"] != "omp-mcp"
        or value["kind"] != "omp-session"
        or value["channel"] != "#omp-relay"
        or value["thread_id"] is not None
        or not isinstance(value["status"], str)
        or value["status"] not in VALID_AGENT_STATUSES
        or (value["current_task"] is not None
            and not _is_unicode_scalar_text(value["current_task"]))
        or (value["display_name"] is not None and (
            not _is_unicode_scalar_text(value["display_name"])
            or len(value["display_name"]) > 256
        ))
    ):
        raise _OmpFailure(422, "PROTOCOL_INVALID")
    _canonical_epoch_ms(value["last_activity_ts"])
    if operation == "bind":
        if value["status"] != "idle" or value["current_task"] is not None:
            raise _OmpFailure(422, "PROTOCOL_INVALID")
        _canonical_uuid(value["bind_attempt_id"])
        _canonical_uuid(value["expected_binding_epoch"], nullable=True)
        _canonical_uuid(value["new_binding_epoch"])
        _canonical_uuid(value["omp_process_epoch"])
        _canonical_uuid(value["instance_id"])
        _canonical_uuid(value["control_epoch"])
        _canonical_hash(value["control_token_hash"])
    else:
        _canonical_uuid(value["binding_epoch"])
        _canonical_uuid(value["omp_process_epoch"])
        _canonical_uuid(value["request_id"])
    return operation, value


async def _post_omp_status(
    request: Request,
    agent_id: str,
    body: Any,
    authorization: Optional[str],
) -> Union[dict[str, Any], JSONResponse]:
    try:
        presented_hash = (
            _relay_control_token_hash(authorization)
            if isinstance(body, dict) and body.get("operation") == "refresh"
            else None
        )
        operation, value = _parse_omp_status_body(agent_id, body)
    except _OmpFailure as exc:
        return _omp_failure_response(request, exc)

    async def _do() -> Union[dict[str, Any], JSONResponse]:
        async with connect_db() as conn:
            await conn.execute("BEGIN IMMEDIATE")
            try:
                route = "POST /api/agent/{id}/status"
                digest = _request_digest(
                    "POST", route, {"recipient_id": agent_id}, value
                )
                if operation == "bind":
                    attempt_id = value["bind_attempt_id"]
                    previous = await _ledger_row(conn, "omp-bind", attempt_id)
                    if previous is not None:
                        if previous["request_hash"] != digest or previous["agent_id"] != agent_id:
                            await conn.rollback()
                            return _omp_error(request, 409, "ATTEMPT_CONFLICT")
                        stored = json.loads(previous["response_json"])
                        if previous["response_status"] != 200:
                            await conn.commit()
                            return JSONResponse(content=stored, status_code=previous["response_status"])
                        cur = await conn.execute(
                            "SELECT * FROM agent_status WHERE id=?", (agent_id,)
                        )
                        active = await cur.fetchone()
                        await cur.close()
                        exact_active = active is not None and all((
                            active["transport"] == "omp-mcp",
                            active["bind_attempt_id"] == attempt_id,
                            active["binding_epoch"] == value["new_binding_epoch"],
                            active["omp_process_epoch"] == value["omp_process_epoch"],
                            active["omp_instance_id"] == value["instance_id"],
                            active["control_epoch"] == value["control_epoch"],
                            active["control_token_hash"] == value["control_token_hash"],
                        ))
                        if not exact_active:
                            details = {
                                "current_binding_epoch": (
                                    active["binding_epoch"] if active is not None else None
                                )
                            }
                            await conn.commit()
                            return _omp_error(request, 409, "BINDING_CONFLICT", details)
                        await conn.commit()
                        return stored

                    cur = await conn.execute(
                        "SELECT * FROM agent_status WHERE id=?", (agent_id,)
                    )
                    current = await cur.fetchone()
                    await cur.close()
                    current_epoch = current["binding_epoch"] if current is not None else None
                    conflict_code: Optional[str] = None
                    conflict_details: Any = None
                    if current is not None and current["transport"] != "omp-mcp":
                        conflict_code = "IDENTITY_CONFLICT"
                    elif current_epoch != value["expected_binding_epoch"]:
                        conflict_code = "BINDING_CONFLICT"
                        conflict_details = {"current_binding_epoch": current_epoch}
                    elif current is not None and current_epoch is None:
                        # Null expectation is reserved for a never-bound native identity,
                        # not conversion of a pre-existing legacy row.
                        conflict_code = "IDENTITY_CONFLICT"
                    elif value["new_binding_epoch"] == value["expected_binding_epoch"]:
                        conflict_code = "ATTEMPT_CONFLICT"
                    elif current is not None and hmac.compare_digest(
                        current["control_token_hash"] or "", value["control_token_hash"]
                    ):
                        conflict_code = "ATTEMPT_CONFLICT"
                    else:
                        cur = await conn.execute(
                            "SELECT 1 FROM idempotency_ledger "
                            "WHERE namespace='omp-bind' AND new_binding_epoch=?",
                            (value["new_binding_epoch"],),
                        )
                        reused_epoch = await cur.fetchone()
                        await cur.close()
                        if reused_epoch is not None:
                            conflict_code = "ATTEMPT_CONFLICT"
                    if conflict_code is not None:
                        payload = _omp_error_payload(request, conflict_code, conflict_details)
                        await _record_protocol_result(
                            conn, "omp-bind", attempt_id, route, agent_id, digest,
                            409, payload,
                        )
                        await conn.commit()
                        return JSONResponse(content=payload, status_code=409)

                    if current is None:
                        cur = await conn.execute(
                            "SELECT MAX(registered_at) AS m FROM agent_status"
                        )
                        maximum = await cur.fetchone()
                        await cur.close()
                        registered_at = max(now_ms(), (maximum["m"] or 0) + 1)
                        relay_name = None
                    else:
                        registered_at = current["registered_at"]
                        relay_name = current["relay_name"]
                    await conn.execute(
                        "INSERT INTO agent_status "
                        "(id,kind,channel,thread_id,status,current_task,capabilities,"
                        "last_activity_ts,registered_at,display_name,relay_name,transport,"
                        "binding_epoch,omp_process_epoch,omp_instance_id,control_epoch,"
                        "control_token_hash,bind_attempt_id) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,'omp-mcp',?,?,?,?,?,?) "
                        "ON CONFLICT(id) DO UPDATE SET "
                        "kind='omp-session',channel='#omp-relay',thread_id=NULL,status='idle',"
                        "current_task=NULL,last_activity_ts=excluded.last_activity_ts,"
                        "display_name=excluded.display_name,transport='omp-mcp',"
                        "binding_epoch=excluded.binding_epoch,"
                        "omp_process_epoch=excluded.omp_process_epoch,"
                        "omp_instance_id=excluded.omp_instance_id,"
                        "control_epoch=excluded.control_epoch,"
                        "control_token_hash=excluded.control_token_hash,"
                        "bind_attempt_id=excluded.bind_attempt_id,"
                        "omp_mutation_seq=agent_status.omp_mutation_seq+1",
                        (
                            agent_id, "omp-session", "#omp-relay", None, "idle", None,
                            None, value["last_activity_ts"], registered_at,
                            value["display_name"], relay_name, value["new_binding_epoch"],
                            value["omp_process_epoch"], value["instance_id"],
                            value["control_epoch"], value["control_token_hash"], attempt_id,
                        ),
                    )
                    event_content = _canonical_json({
                        "old_status": current["status"] if current is not None else None,
                        "new_status": "idle",
                        "current_task": None,
                    })
                    cur = await conn.execute(
                        "INSERT INTO message_log "
                        "(client_event_id,from_agent,to_agent,channel,thread_id,kind,content,ts) "
                        "VALUES (NULL,?,NULL,'#omp-relay',NULL,'status',?,?)",
                        (agent_id, event_content, value["last_activity_ts"]),
                    )
                    event_id = cur.lastrowid
                    await cur.close()
                    response = {
                        "schema_version": 1,
                        "id": agent_id,
                        "transport": "omp-mcp",
                        "bind_attempt_id": attempt_id,
                        "binding_epoch": value["new_binding_epoch"],
                        "omp_process_epoch": value["omp_process_epoch"],
                        "instance_id": value["instance_id"],
                        "control_epoch": value["control_epoch"],
                        "relay_name": relay_name,
                        "registered_at": registered_at,
                        "event_id": str(event_id),
                    }
                    await _record_protocol_result(
                        conn, "omp-bind", attempt_id, route, agent_id, digest, 200,
                        response, new_binding_epoch=value["new_binding_epoch"],
                        response_event_id=event_id,
                    )
                    await conn.commit()
                    return response

                binding = await _require_current_control(
                    conn, agent_id, presented_hash or "", value["binding_epoch"],
                    value["omp_process_epoch"],
                )
                key = value["request_id"]
                previous = await _ledger_row(conn, "omp-status", key)
                if previous is not None:
                    if previous["request_hash"] != digest or previous["agent_id"] != agent_id:
                        await conn.rollback()
                        return _omp_error(request, 409, "REQUEST_CONFLICT")
                    response = json.loads(previous["response_json"])
                    await conn.commit()
                    return response
                await conn.execute(
                    "UPDATE agent_status SET status=?,current_task=?,last_activity_ts=?,"
                    "display_name=?,omp_mutation_seq=omp_mutation_seq+1 WHERE id=?",
                    (
                        value["status"], value["current_task"], value["last_activity_ts"],
                        value["display_name"], agent_id,
                    ),
                )
                event_content = _canonical_json({
                    "old_status": binding["status"],
                    "new_status": value["status"],
                    "current_task": value["current_task"],
                })
                cur = await conn.execute(
                    "INSERT INTO message_log "
                    "(client_event_id,from_agent,to_agent,channel,thread_id,kind,content,ts) "
                    "VALUES (NULL,?,NULL,'#omp-relay',NULL,'status',?,?)",
                    (agent_id, event_content, value["last_activity_ts"]),
                )
                event_id = cur.lastrowid
                await cur.close()
                response = {
                    "schema_version": 1,
                    "id": agent_id,
                    "transport": "omp-mcp",
                    "binding_epoch": binding["binding_epoch"],
                    "omp_process_epoch": binding["omp_process_epoch"],
                    "relay_name": binding["relay_name"],
                    "registered_at": binding["registered_at"],
                    "event_id": str(event_id),
                }
                await _record_protocol_result(
                    conn, "omp-status", key, route, agent_id, digest, 200, response,
                    response_event_id=event_id,
                )
                await conn.commit()
                return response
            except _OmpFailure as exc:
                await conn.rollback()
                return _omp_failure_response(request, exc)
            except Exception:
                await conn.rollback()
                raise

    return await with_retry(_do)


async def _delivery_and_offer(
    conn: aiosqlite.Connection, recipient_id: str, event_id: int
) -> tuple[Optional[aiosqlite.Row], Optional[aiosqlite.Row]]:
    cur = await conn.execute(
        "SELECT d.*,m.client_event_id,m.from_agent,m.to_agent AS message_to_agent,"
        "m.channel,m.thread_id,m.kind,m.content,m.ts "
        "FROM delivery d JOIN message_log m ON m.event_id=d.event_id "
        "WHERE d.recipient_id=? AND d.event_id=?",
        (recipient_id, event_id),
    )
    delivery = await cur.fetchone()
    await cur.close()
    cur = await conn.execute(
        "SELECT * FROM delivery_offer WHERE recipient_id=? AND event_id=?",
        (recipient_id, event_id),
    )
    offer = await cur.fetchone()
    await cur.close()
    return delivery, offer


async def _reject_delivery_tx(
    conn: aiosqlite.Connection,
    delivery: aiosqlite.Row,
    offer: Optional[aiosqlite.Row],
    requested_reason: str,
    *,
    actor_type: str,
    actor_id: str,
    route: str,
    request_id: Optional[str],
) -> tuple[str, str, Optional[int]]:
    if delivery["status"] != "outstanding":
        raise _OmpFailure(409, "TERMINAL_CONFLICT")
    if offer is not None and offer["state"] in OMP_TERMINAL_STATES:
        raise _OmpFailure(409, "TERMINAL_CONFLICT")
    effective = (
        "DELIVERY_UNCERTAIN_CONTENT_MAY_HAVE_BEEN_SEEN"
        if offer is not None else requested_reason
    )
    timestamp = now_ms()
    if offer is not None:
        await conn.execute(
            "UPDATE delivery_offer SET state='rejected',reason=?,updated_ts=? "
            "WHERE recipient_id=? AND event_id=? AND state IN ('offered','injected')",
            (effective, timestamp, delivery["recipient_id"], delivery["event_id"]),
        )
    cur = await conn.execute(
        "UPDATE delivery SET status='rejected',rejected_ts=?,reject_reason_code=?,"
        "updated_ts=? WHERE recipient_id IS ? AND to_agent=? AND event_id=? "
        "AND status='outstanding'",
        (
            timestamp, effective, timestamp, delivery["recipient_id"],
            delivery["to_agent"], delivery["event_id"],
        ),
    )
    updated = cur.rowcount
    await cur.close()
    if updated != 1:
        raise _OmpFailure(409, "TERMINAL_CONFLICT")
    notice_id: Optional[int] = None
    sender_notice = "unavailable"
    sender = None
    if delivery["sender_id"] is not None:
        cur = await conn.execute(
            "SELECT id,relay_name,transport,binding_epoch FROM agent_status WHERE id=?",
            (delivery["sender_id"],),
        )
        sender = await cur.fetchone()
        await cur.close()
    if sender is not None:
        notice_to = sender["relay_name"] or sender["id"]
        canonical_key, exact_key = _legacy_mailbox_keys(notice_to)
        cur = await conn.execute(
            "SELECT 1 FROM omp_relay_namespace "
            "WHERE relay_name IN (?,?) AND owner_id<>? "
            "UNION ALL "
            "SELECT 1 FROM agent_status "
            "WHERE relay_name IN (?,?) AND id<>? LIMIT 1",
            (
                canonical_key, exact_key, sender["id"],
                canonical_key, exact_key, sender["id"],
            ),
        )
        address_conflict = await cur.fetchone()
        await cur.close()
        if address_conflict is None:
            notice_content = _canonical_json({
                "event_id": str(delivery["event_id"]),
                "reject_reason_code": effective,
            })
            cur = await conn.execute(
                "INSERT INTO message_log "
                "(client_event_id,from_agent,to_agent,channel,thread_id,kind,content,ts) "
                "VALUES (NULL,'broker',?,?,NULL,'agent_reply',?,?)",
                (notice_to, delivery["channel"], notice_content, timestamp),
            )
            notice_id = cur.lastrowid
            await cur.close()
            await conn.execute(
                "INSERT INTO delivery "
                "(to_agent,event_id,status,attempts,created_ts,updated_ts,sender_id,recipient_id) "
                "VALUES (?,?,'outstanding',0,?,?,NULL,?)",
                (notice_to, notice_id, timestamp, timestamp, sender["id"]),
            )
            await conn.execute(
                "UPDATE delivery SET rejection_notice_event_id=? "
                "WHERE to_agent=? AND event_id=?",
                (notice_id, delivery["to_agent"], delivery["event_id"]),
            )
            sender_notice = "created"
    await conn.execute(
        "INSERT INTO omp_rejection_audit "
        "(request_id,actor_type,actor_id,route,recipient_id,event_id,"
        "requested_reason_code,effective_reason_code,offer_token_present,"
        "sender_notice,rejection_notice_event_id,created_ts) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            request_id, actor_type, actor_id, route, delivery["recipient_id"],
            delivery["event_id"], requested_reason, effective,
            1 if offer is not None else 0, sender_notice, notice_id, timestamp,
        ),
    )
    return effective, sender_notice, notice_id


@app.post("/api/agent/{agent_id}/delivery-offer")
async def post_agent_delivery_offer(
    agent_id: str,
    request: Request,
    authorization: Optional[str] = Header(None, alias="Authorization"),
):
    try:
        presented_hash = _relay_control_token_hash(authorization)
        body = await _decode_json_body(request)
        _canonical_uuid(agent_id)
        value = _closed_object(body, {"schema_version", "binding_epoch", "omp_process_epoch"})
        if type(value["schema_version"]) is not int or value["schema_version"] != 1:
            raise _OmpFailure(422, "PROTOCOL_INVALID")
        binding_epoch = _canonical_uuid(value["binding_epoch"])
        process_epoch = _canonical_uuid(value["omp_process_epoch"])
    except _OmpFailure as exc:
        return _omp_failure_response(request, exc)

    async def _do():
        async with connect_db() as conn:
            await conn.execute("BEGIN IMMEDIATE")
            try:
                binding = await _require_current_control(
                    conn, agent_id, presented_hash, binding_epoch or "", process_epoch or ""
                )
                cur = await conn.execute(
                    "SELECT d.*,m.client_event_id,m.from_agent,m.to_agent AS message_to_agent,"
                    "m.channel,m.thread_id,m.kind,m.content,m.ts "
                    "FROM delivery d JOIN message_log m ON m.event_id=d.event_id "
                    "WHERE d.recipient_id=? AND d.status='outstanding' "
                    "ORDER BY d.event_id ASC LIMIT 1",
                    (agent_id,),
                )
                delivery = await cur.fetchone()
                await cur.close()
                if delivery is None:
                    await conn.commit()
                    return {"schema_version": 1, "code": "EMPTY", "status": None, "delivery": None}
                _validate_omp_delivery(delivery)
                cur = await conn.execute(
                    "SELECT * FROM delivery_offer WHERE recipient_id=? AND event_id=?",
                    (agent_id, delivery["event_id"]),
                )
                offer = await cur.fetchone()
                await cur.close()
                if offer is None:
                    timestamp = now_ms()
                    await conn.execute(
                        "INSERT INTO delivery_offer "
                        "(recipient_id,event_id,offer_token,state,reason,omp_process_epoch,"
                        "created_ts,updated_ts) VALUES (?,?,?,'offered',NULL,?,?,?)",
                        (
                            agent_id, delivery["event_id"], str(uuid.uuid4()),
                            binding["omp_process_epoch"], timestamp, timestamp,
                        ),
                    )
                status_value = await _offer_status(conn, agent_id, delivery["event_id"], binding)
                await conn.commit()
                return {
                    "schema_version": 1, "code": "HEAD", "status": status_value,
                    "delivery": _delivery_projection(delivery),
                }
            except _OmpFailure as exc:
                await conn.rollback()
                return _omp_failure_response(request, exc)
            except Exception:
                await conn.rollback()
                raise

    return await with_retry(_do)


@app.post("/api/agent/{agent_id}/delivery-offer-status")
async def post_agent_delivery_offer_status(
    agent_id: str,
    request: Request,
    authorization: Optional[str] = Header(None, alias="Authorization"),
):
    try:
        presented_hash = _relay_control_token_hash(authorization)
        body = await _decode_json_body(request)
        _canonical_uuid(agent_id)
        value = _closed_object(body, {
            "schema_version", "event_id", "offer_token", "binding_epoch",
            "omp_process_epoch", "target_state", "reason_code",
        })
        if type(value["schema_version"]) is not int or value["schema_version"] != 1:
            raise _OmpFailure(422, "PROTOCOL_INVALID")
        event_id = _canonical_event_id(value["event_id"])
        _canonical_uuid(value["offer_token"])
        _canonical_uuid(value["binding_epoch"])
        _canonical_uuid(value["omp_process_epoch"])
        if not isinstance(value["target_state"], str) or value["target_state"] not in {
            "injected", "accepted", "rejected"
        }:
            raise _OmpFailure(422, "PROTOCOL_INVALID")
        if (
            (value["target_state"] == "rejected" and value["reason_code"] != "USER_REJECTED")
            or (value["target_state"] != "rejected" and value["reason_code"] is not None)
        ):
            raise _OmpFailure(422, "PROTOCOL_INVALID")
    except _OmpFailure as exc:
        return _omp_failure_response(request, exc)

    async def _do():
        async with connect_db() as conn:
            await conn.execute("BEGIN IMMEDIATE")
            try:
                binding = await _require_current_control(
                    conn, agent_id, presented_hash, value["binding_epoch"],
                    value["omp_process_epoch"],
                )
                delivery, offer = await _delivery_and_offer(conn, agent_id, event_id)
                if offer is None:
                    raise _OmpFailure(404, "DELIVERY_NOT_FOUND")
                if offer["offer_token"] != value["offer_token"]:
                    raise _OmpFailure(409, "STATE_CONFLICT", {"status": None})
                if delivery is None:
                    raise _OmpFailure(503, "PROTOCOL_INVALID")
                current_state = offer["state"]
                target = value["target_state"]
                if current_state in OMP_TERMINAL_STATES:
                    status_value = await _offer_status(conn, agent_id, event_id, binding)
                    if current_state != target:
                        raise _OmpFailure(409, "TERMINAL_CONFLICT", {"status": status_value})
                    await conn.commit()
                    return {"schema_version": 1, "code": "UNCHANGED", "status": status_value}
                cur = await conn.execute(
                    "SELECT event_id FROM delivery WHERE recipient_id=? "
                    "AND status='outstanding' ORDER BY event_id LIMIT 1",
                    (agent_id,),
                )
                head = await cur.fetchone()
                await cur.close()
                if head is None or head["event_id"] != event_id:
                    status_value = await _offer_status(conn, agent_id, event_id, binding)
                    raise _OmpFailure(409, "HEAD_CONFLICT", {"status": status_value})
                if target == "rejected":
                    await _reject_delivery_tx(
                        conn, delivery, offer, "USER_REJECTED",
                        actor_type="recipient", actor_id=agent_id,
                        route="POST /api/agent/{id}/delivery-offer-status", request_id=None,
                    )
                    code = "UPDATED"
                elif target == "injected":
                    code = "UNCHANGED" if current_state == "injected" else "UPDATED"
                    if current_state == "offered":
                        await conn.execute(
                            "UPDATE delivery_offer SET state='injected',updated_ts=? "
                            "WHERE recipient_id=? AND event_id=? AND state='offered'",
                            (now_ms(), agent_id, event_id),
                        )
                else:
                    await conn.execute(
                        "UPDATE delivery_offer SET state='accepted',updated_ts=? "
                        "WHERE recipient_id=? AND event_id=? AND state IN ('offered','injected')",
                        (now_ms(), agent_id, event_id),
                    )
                    code = "UPDATED"
                status_value = await _offer_status(conn, agent_id, event_id, binding)
                await conn.commit()
                return {"schema_version": 1, "code": code, "status": status_value}
            except _OmpFailure as exc:
                await conn.rollback()
                return _omp_failure_response(request, exc)
            except Exception:
                await conn.rollback()
                raise

    return await with_retry(_do)


@app.get("/api/agent/{agent_id}/delivery/{event_id_text}/status")
async def get_agent_delivery_status(
    agent_id: str,
    event_id_text: str,
    request: Request,
    authorization: Optional[str] = Header(None, alias="Authorization"),
):
    try:
        presented_hash = _relay_control_token_hash(authorization)
        _canonical_uuid(agent_id)
        event_id = _canonical_event_id(event_id_text)
        pairs = request.query_params.multi_items()
        query = dict(pairs)
        if len(query) != len(pairs):
            raise _OmpFailure(422, "PROTOCOL_INVALID")
        value = _closed_object(query, {"schema_version", "binding_epoch", "omp_process_epoch"})
        if value["schema_version"] != "1":
            raise _OmpFailure(422, "PROTOCOL_INVALID")
        _canonical_uuid(value["binding_epoch"])
        _canonical_uuid(value["omp_process_epoch"])
    except _OmpFailure as exc:
        return _omp_failure_response(request, exc)
    async with connect_db() as conn:
        await conn.execute("BEGIN")
        try:
            binding = await _require_current_control(
                conn, agent_id, presented_hash, value["binding_epoch"], value["omp_process_epoch"]
            )
            status_value = await _offer_status(conn, agent_id, event_id, binding)
            await conn.commit()
            return {"schema_version": 1, "status": status_value}
        except _OmpFailure as exc:
            await conn.rollback()
            return _omp_failure_response(request, exc)
        except Exception:
            await conn.rollback()
            raise


def _operator_config() -> Optional[tuple[bytes, str]]:
    expected = _env_str("BROKER_OPERATOR_TOKEN", "")
    operator_id = _env_str("BROKER_OPERATOR_ID", "")
    if not expected or not operator_id:
        return None
    try:
        return expected.encode("ascii"), operator_id
    except UnicodeEncodeError:
        return None


def _operator_auth(authorization: Optional[str]) -> str:
    config = _operator_config()
    if config is None:
        raise _OmpFailure(503, "BROKER_CAPABILITY_MISSING")
    expected_bytes, operator_id = config
    if authorization is None:
        raise _OmpFailure(401, "OPERATOR_AUTH_REQUIRED")
    if not authorization.startswith("Bearer "):
        raise _OmpFailure(403, "OPERATOR_AUTH_INVALID")
    supplied = authorization[len("Bearer "):]
    try:
        supplied_bytes = supplied.encode("ascii")
    except UnicodeEncodeError as exc:
        raise _OmpFailure(403, "OPERATOR_AUTH_INVALID") from exc
    if not hmac.compare_digest(supplied_bytes, expected_bytes):
        raise _OmpFailure(403, "OPERATOR_AUTH_INVALID")
    return operator_id


def _operator_result(
    request_id: str,
    delivery: Optional[aiosqlite.Row],
    offer: Optional[aiosqlite.Row],
    *,
    sender_notice: str = "not_created",
    notice_id: Optional[int] = None,
) -> dict[str, Any]:
    return {
        "request_id": request_id,
        "recipient_id": delivery["recipient_id"] if delivery is not None else None,
        "event_id": str(delivery["event_id"]) if delivery is not None else "0",
        "delivery_status": delivery["status"] if delivery is not None else None,
        "offer_state": offer["state"] if offer is not None else None,
        "reject_reason_code": (
            delivery["reject_reason_code"] if delivery is not None else None
        ),
        "sender_notice": sender_notice,
        "rejection_notice_event_id": str(notice_id) if notice_id is not None else None,
    }


async def _operator_response_tx(
    conn: aiosqlite.Connection,
    request: Request,
    *,
    key: str,
    route: str,
    agent_id: Optional[str],
    digest: str,
    status_code: int,
    code: str,
    result: Optional[dict[str, Any]],
) -> JSONResponse:
    if status_code == 200:
        payload = {"schema_version": 1, "code": code, "result": result}
    else:
        details = {"result": result} if result is not None else None
        payload = _omp_error_payload(request, code, details)
    await _record_protocol_result(
        conn, "omp-operator", key, route, agent_id, digest,
        status_code, payload,
    )
    await conn.commit()
    return JSONResponse(content=payload, status_code=status_code)


async def _operator_reject(
    request: Request,
    authorization: Optional[str],
    *,
    recipient_id: Optional[str],
    event_id_text: Optional[str],
    unbound: bool,
) -> JSONResponse:
    try:
        operator_id = _operator_auth(authorization)
        body = await _decode_json_body(request)
        if unbound:
            fields = {"schema_version", "request_id", "reason_code"}
            event_id = _canonical_event_id(event_id_text)
            route = "POST /api/operator/delivery/{event_id}/reject-unbound"
            path = {"event_id": str(event_id)}
        else:
            fields = {"schema_version", "request_id", "event_id", "reason_code"}
            if not isinstance(recipient_id, str) or not (1 <= len(recipient_id) <= 256):
                raise _OmpFailure(422, "PROTOCOL_INVALID")
            event_id = _canonical_event_id(body.get("event_id") if isinstance(body, dict) else None)
            route = "POST /api/operator/agent/{recipient_id}/delivery-reject"
            path = {"recipient_id": recipient_id}
        value = _closed_object(body, fields)
        if type(value["schema_version"]) is not int or value["schema_version"] != 1:
            raise _OmpFailure(422, "PROTOCOL_INVALID")
        request_key = _canonical_uuid(value["request_id"])
        if not isinstance(value["reason_code"], str) or value["reason_code"] not in (
            OMP_OPERATOR_REASONS
        ):
            raise _OmpFailure(422, "PROTOCOL_INVALID")
        digest = _request_digest("POST", route, path, value)
    except _OmpFailure as exc:
        return _omp_failure_response(request, exc)

    async def _do() -> JSONResponse:
        async with connect_db() as conn:
            await conn.execute("BEGIN IMMEDIATE")
            try:
                previous = await _ledger_row(conn, "omp-operator", request_key or "")
                if previous is not None:
                    if previous["request_hash"] != digest:
                        await conn.rollback()
                        return _omp_error(request, 409, "REQUEST_CONFLICT")
                    payload = json.loads(previous["response_json"])
                    await conn.commit()
                    return JSONResponse(content=payload, status_code=previous["response_status"])

                if unbound:
                    cur = await conn.execute(
                        "SELECT d.*,m.channel FROM delivery d "
                        "JOIN message_log m ON m.event_id=d.event_id WHERE d.event_id=?",
                        (event_id,),
                    )
                    delivery = await cur.fetchone()
                    await cur.close()
                    cur = await conn.execute(
                        "SELECT * FROM delivery_offer WHERE event_id=?", (event_id,)
                    )
                    offer = await cur.fetchone()
                    await cur.close()
                    if delivery is None:
                        return await _operator_response_tx(
                            conn, request, key=request_key or "", route=route,
                            agent_id=None, digest=digest, status_code=404,
                            code="DELIVERY_NOT_FOUND", result=None,
                        )
                    if delivery["recipient_id"] is not None or offer is not None:
                        return await _operator_response_tx(
                            conn, request, key=request_key or "", route=route,
                            agent_id=None, digest=digest, status_code=409,
                            code="RECIPIENT_CONFLICT", result=None,
                        )
                else:
                    cur = await conn.execute(
                        "SELECT binding_epoch FROM agent_status WHERE id=?",
                        (recipient_id,),
                    )
                    recipient = await cur.fetchone()
                    await cur.close()
                    if recipient is None or recipient["binding_epoch"] is None:
                        return await _operator_response_tx(
                            conn, request, key=request_key or "", route=route,
                            agent_id=recipient_id, digest=digest, status_code=404,
                            code="DELIVERY_NOT_FOUND", result=None,
                        )
                    delivery, offer = await _delivery_and_offer(
                        conn, recipient_id or "", event_id
                    )
                    if delivery is None:
                        return await _operator_response_tx(
                            conn, request, key=request_key or "", route=route,
                            agent_id=recipient_id, digest=digest, status_code=404,
                            code="DELIVERY_NOT_FOUND", result=None,
                        )

                if delivery["status"] != "outstanding" or (
                    offer is not None and offer["state"] in OMP_TERMINAL_STATES
                ):
                    terminal_notice = (
                        "created" if delivery["rejection_notice_event_id"] is not None
                        else ("unavailable" if delivery["status"] == "rejected" else "not_created")
                    )
                    result = _operator_result(
                        request_key or "", delivery, offer,
                        sender_notice=terminal_notice,
                        notice_id=delivery["rejection_notice_event_id"],
                    )
                    return await _operator_response_tx(
                        conn, request, key=request_key or "", route=route,
                        agent_id=recipient_id, digest=digest, status_code=409,
                        code="TERMINAL_CONFLICT", result=result,
                    )

                if not unbound:
                    cur = await conn.execute(
                        "SELECT event_id FROM delivery WHERE recipient_id=? "
                        "AND status='outstanding' ORDER BY event_id LIMIT 1",
                        (recipient_id,),
                    )
                    head = await cur.fetchone()
                    await cur.close()
                    if head is None or head["event_id"] != event_id:
                        result = _operator_result(request_key or "", delivery, offer)
                        return await _operator_response_tx(
                            conn, request, key=request_key or "", route=route,
                            agent_id=recipient_id, digest=digest, status_code=409,
                            code="HEAD_CONFLICT", result=result,
                        )

                effective, sender_notice, notice_id = await _reject_delivery_tx(
                    conn, delivery, offer, value["reason_code"],
                    actor_type="operator", actor_id=operator_id, route=route,
                    request_id=request_key,
                )
                # Re-read terminal rows so the serialized result is the stored winner.
                if unbound:
                    cur = await conn.execute(
                        "SELECT d.*,m.channel FROM delivery d JOIN message_log m "
                        "ON m.event_id=d.event_id WHERE d.event_id=?", (event_id,)
                    )
                    delivery = await cur.fetchone()
                    await cur.close()
                else:
                    delivery, offer = await _delivery_and_offer(
                        conn, recipient_id or "", event_id
                    )
                result = _operator_result(
                    request_key or "", delivery, offer,
                    sender_notice=sender_notice, notice_id=notice_id,
                )
                if result["reject_reason_code"] != effective:
                    raise _OmpFailure(503, "PROTOCOL_INVALID")
                return await _operator_response_tx(
                    conn, request, key=request_key or "", route=route,
                    agent_id=recipient_id, digest=digest, status_code=200,
                    code="REJECTED", result=result,
                )
            except _OmpFailure as exc:
                await conn.rollback()
                return _omp_failure_response(request, exc)
            except Exception:
                await conn.rollback()
                raise

    return await with_retry(_do)


@app.post("/api/operator/agent/{recipient_id}/delivery-reject")
async def post_operator_delivery_reject(
    recipient_id: str,
    request: Request,
    authorization: Optional[str] = Header(None, alias="Authorization"),
):
    return await _operator_reject(
        request, authorization, recipient_id=recipient_id,
        event_id_text=None, unbound=False,
    )


@app.post("/api/operator/delivery/{event_id_text}/reject-unbound")
async def post_operator_reject_unbound(
    event_id_text: str,
    request: Request,
    authorization: Optional[str] = Header(None, alias="Authorization"),
):
    return await _operator_reject(
        request, authorization, recipient_id=None,
        event_id_text=event_id_text, unbound=True,
    )


async def _claim_relay_name(
    conn: aiosqlite.Connection,
    agent_id: str,
    relay_name: str,
    *,
    takeover: bool,
    require_agent: bool,
    allow_displace_omp: bool = False,
    omp_authorized: bool = False,
) -> Optional[str]:
    """Claim or rename one canonical mailbox inside the caller's transaction."""
    cur = await conn.execute(
        "SELECT relay_name,binding_epoch FROM agent_status WHERE id=?", (agent_id,)
    )
    current = await cur.fetchone()
    await cur.close()
    if current is None and require_agent:
        raise HTTPException(status_code=404, detail="agent not registered")

    cur = await conn.execute(
        "SELECT owner_id FROM omp_relay_namespace WHERE relay_name=?", (relay_name,)
    )
    retained = await cur.fetchone()
    await cur.close()
    retained_owner = retained["owner_id"] if retained is not None else None
    if retained_owner is not None and retained_owner != agent_id:
        if not takeover:
            raise HTTPException(status_code=409, detail="relay name is retained")
        if not allow_displace_omp:
            raise HTTPException(status_code=409, detail="relay name is OMP-controlled")

    cur = await conn.execute(
        "SELECT id, display_name, binding_epoch FROM agent_status WHERE relay_name=?",
        (relay_name,),
    )
    owner = await cur.fetchone()
    await cur.close()
    displaced_id = owner["id"] if owner is not None and owner["id"] != agent_id else None
    owner_binding = owner["binding_epoch"] if owner is not None else None
    if displaced_id is not None and not takeover:
        raise HTTPException(status_code=409, detail="relay name already claimed")
    if displaced_id is not None and owner_binding is not None and not allow_displace_omp:
        raise HTTPException(status_code=409, detail="relay name is OMP-controlled")

    old_name = current["relay_name"] if current is not None else None
    cur = await conn.execute(
        "SELECT DISTINCT d.recipient_id FROM delivery d "
        "JOIN agent_status a ON a.id=d.recipient_id "
        "WHERE a.binding_epoch IS NULL AND d.recipient_id<>? "
        "AND lower(d.to_agent)=lower(?)",
        (agent_id, relay_name),
    )
    stable_owners = {row["recipient_id"] for row in await cur.fetchall()}
    await cur.close()
    if stable_owners and stable_owners != {displaced_id}:
        if not takeover or len(stable_owners) != 1:
            raise HTTPException(status_code=409, detail="relay name has retained stable history")
        history_owner = next(iter(stable_owners))
        cur = await conn.execute(
            "SELECT relay_name FROM agent_status WHERE id=?", (history_owner,)
        )
        history_identity = await cur.fetchone()
        await cur.close()
        if history_identity is None:
            raise HTTPException(status_code=409, detail="relay name has retained stable history")
        history_fallback = history_identity["relay_name"] or history_owner
        if history_fallback.lower() == relay_name.lower():
            raise HTTPException(
                status_code=409, detail="relay name has no rollback-safe fallback"
            )
        cur = await conn.execute(
            "SELECT 1 FROM agent_status WHERE relay_name=? AND id<>? "
            "UNION ALL SELECT 1 FROM omp_relay_namespace "
            "WHERE relay_name=? AND owner_id<>? LIMIT 1",
            (history_fallback, history_owner, history_fallback, history_owner),
        )
        history_conflict = await cur.fetchone()
        await cur.close()
        if history_conflict is not None:
            raise HTTPException(status_code=409, detail="relay name has no rollback-safe fallback")
        await conn.execute(
            "UPDATE message_log SET to_agent=? WHERE event_id IN "
            "(SELECT event_id FROM delivery WHERE recipient_id=? "
            "AND lower(to_agent)=lower(?))",
            (history_fallback, history_owner, relay_name),
        )
        await conn.execute(
            "UPDATE delivery SET to_agent=?,updated_ts=? "
            "WHERE recipient_id=? AND lower(to_agent)=lower(?)",
            (history_fallback, now_ms(), history_owner, relay_name),
        )
        stable_owners.clear()

    displaced_fallback: Optional[str] = None
    if (
        displaced_id is not None
        and owner_binding is None
        and displaced_id in stable_owners
    ):
        displaced_fallback = displaced_id
        if displaced_fallback == relay_name:
            raise HTTPException(status_code=409, detail="relay name has no rollback-safe fallback")
        cur = await conn.execute(
            "SELECT 1 FROM agent_status WHERE relay_name=? AND id<>? "
            "UNION ALL SELECT 1 FROM omp_relay_namespace "
            "WHERE relay_name=? AND owner_id<>? LIMIT 1",
            (displaced_fallback, displaced_id, displaced_fallback, displaced_id),
        )
        fallback_conflict = await cur.fetchone()
        await cur.close()
        if fallback_conflict is not None:
            raise HTTPException(status_code=409, detail="relay name has no rollback-safe fallback")

    if omp_authorized:
        timestamp = now_ms()
        await conn.execute(
            "INSERT INTO omp_relay_namespace "
            "(relay_name,owner_id,created_ts,updated_ts) VALUES (?,?,?,?) "
            "ON CONFLICT(relay_name) DO UPDATE SET "
            "owner_id=excluded.owner_id,updated_ts=excluded.updated_ts",
            (relay_name, agent_id, timestamp, timestamp),
        )

    if displaced_id is not None:
        if owner_binding is not None:
            await conn.execute(
                "UPDATE agent_status SET relay_name=NULL,status='blocked',"
                "omp_mutation_seq=omp_mutation_seq+1 WHERE id=?",
                (displaced_id,),
            )
        elif displaced_fallback is None:
            await conn.execute(
                "UPDATE agent_status SET relay_name=NULL,status='blocked' WHERE id=?",
                (displaced_id,),
            )
        else:
            await conn.execute(
                "UPDATE message_log SET to_agent=? WHERE event_id IN "
                "(SELECT event_id FROM delivery WHERE recipient_id=? "
                "AND lower(to_agent)=lower(?))",
                (displaced_fallback, displaced_id, relay_name),
            )
            await conn.execute(
                "UPDATE delivery SET to_agent=?,updated_ts=? "
                "WHERE recipient_id=? AND lower(to_agent)=lower(?)",
                (displaced_fallback, now_ms(), displaced_id, relay_name),
            )
            await conn.execute(
                "UPDATE agent_status SET relay_name=?,status='blocked' WHERE id=?",
                (displaced_fallback, displaced_id),
            )

    legacy_source = old_name or agent_id
    if (
        current is not None
        and current["binding_epoch"] is None
        and legacy_source != relay_name
    ):
        await conn.execute(
            "UPDATE message_log SET to_agent=? WHERE event_id IN "
            "(SELECT event_id FROM delivery WHERE recipient_id=? "
            "AND lower(to_agent)=lower(?) "
            "AND status IN ('outstanding','abandoned'))",
            (relay_name, agent_id, legacy_source),
        )
        await conn.execute(
            "UPDATE delivery SET to_agent=?,updated_ts=? "
            "WHERE recipient_id=? AND lower(to_agent)=lower(?) "
            "AND status IN ('outstanding','abandoned')",
            (relay_name, now_ms(), agent_id, legacy_source),
        )

    if current is not None:
        if current["binding_epoch"] is not None:
            if not omp_authorized:
                raise HTTPException(status_code=409, detail="identity requires OMP control")
            await conn.execute(
                "UPDATE agent_status SET relay_name=?,"
                "omp_mutation_seq=omp_mutation_seq+1 WHERE id=?",
                (relay_name, agent_id),
            )
        else:
            await conn.execute(
                "UPDATE agent_status SET relay_name=? WHERE id=?", (relay_name, agent_id)
            )
    return displaced_id




async def _begin_read_snapshot(conn: aiosqlite.Connection) -> None:
    """Keep ownership validation and the following mailbox read consistent."""
    await conn.execute("BEGIN")


def _legacy_mailbox_keys(mailbox: str) -> tuple[str, str]:
    """Return the bounded bare-ingress/exact key pair for a legacy mailbox path."""
    if "@" in mailbox:
        return mailbox, mailbox
    route = addressing.parse_recipient(mailbox)
    if route.disposition != "local" or route.key is None:
        return mailbox, mailbox
    return route.key, mailbox


async def _legacy_mailbox_owner_id(
    conn: aiosqlite.Connection, mailbox: str
) -> Optional[str]:
    """Resolve a legacy mailbox to its current stable owner without late-binding mail."""
    canonical_key, exact_key = _legacy_mailbox_keys(mailbox)
    cur = await conn.execute(
        "SELECT owner_id FROM omp_relay_namespace "
        "WHERE relay_name=? OR relay_name=?",
        (canonical_key, exact_key),
    )
    retained = await cur.fetchone()
    await cur.close()
    if retained is not None:
        raise HTTPException(status_code=409, detail="mailbox requires stable agent endpoint")
    cur = await conn.execute(
        "SELECT id,relay_name,transport,binding_epoch FROM agent_status "
        "WHERE relay_name IN (?,?)",
        (canonical_key, exact_key),
    )
    named_rows = await cur.fetchall()
    await cur.close()
    if named_rows:
        owner = named_rows[0]
        if owner["transport"] == "codex-app-server" or owner["binding_epoch"] is not None:
            raise HTTPException(status_code=409, detail="mailbox requires stable agent endpoint")
        return owner["id"]
    cur = await conn.execute(
        "SELECT id,transport,binding_epoch FROM agent_status WHERE id=?",
        (mailbox,),
    )
    exact_owner = await cur.fetchone()
    await cur.close()
    if exact_owner is None:
        return None
    if (
        exact_owner["transport"] == "codex-app-server"
        or exact_owner["binding_epoch"] is not None
    ):
        raise HTTPException(status_code=409, detail="mailbox requires stable agent endpoint")
    return exact_owner["id"]

async def _relocate_legacy_abandoned_ack_rows(
    conn: aiosqlite.Connection,
    owner_id: str,
    relay_name: Optional[str],
    event_ids: list[int],
) -> None:
    """Move rollback-era abandoned rows to the owner's current readable address."""
    if not event_ids:
        return
    target = relay_name or owner_id
    placeholders = ",".join("?" * len(event_ids))
    params = (owner_id, *event_ids, target)
    await conn.execute(
        "UPDATE message_log SET to_agent=? WHERE event_id IN "
        "(SELECT event_id FROM delivery WHERE recipient_id=? "
        f"AND event_id IN ({placeholders}) AND status='abandoned' "
        "AND lower(to_agent)<>lower(?))",
        (target, *params),
    )
    await conn.execute(
        "UPDATE delivery SET to_agent=?,updated_ts=? WHERE recipient_id=? "
        f"AND event_id IN ({placeholders}) AND status='abandoned' "
        "AND lower(to_agent)<>lower(?)",
        (target, now_ms(), *params),
    )


# ---- POST /api/agent/{id}/status ----
@app.post("/api/agent/{agent_id}/status")
async def post_agent_status(
    agent_id: str,
    request: Request,
    idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
    authorization: Optional[str] = Header(None, alias="Authorization"),
):
    raw = await request.body()
    loose_body = _decode_shared_json_bytes(raw)
    omp_variant = _is_omp_shared_payload("status", authorization, loose_body)
    if omp_variant:
        try:
            if (
                authorization and authorization.startswith("RelayControl ")
            ) or (
                isinstance(loose_body, dict)
                and loose_body.get("operation") == "refresh"
            ):
                _relay_control_token_hash(authorization)
            body = _decode_json_bytes(raw)
        except _OmpFailure as exc:
            return _omp_failure_response(request, exc)
        return await _post_omp_status(request, agent_id, body, authorization)
    body = loose_body
    try:
        legacy_body = AgentStatusBody.model_validate(body)
    except ValidationError as exc:
        raise RequestValidationError(exc.errors()) from exc
    # Bound the peer-authored agent_id path param (stored as message_log.from_agent for
    # kind='status') for parity with EventBody's 256 name cap, so no unbounded name can enter
    # message_log via this path either. Realistic agent ids (session-id / subagent@sid /
    # friendly name) are <= 64.
    if len(agent_id) > 256:
        raise HTTPException(status_code=422, detail="agent_id too long (max 256)")
    last_act = legacy_body.last_activity_ts  # already int via Pydantic validator (F6)
    cei = resolve_client_event_id(legacy_body.client_event_id, idempotency_key)  # F1
    is_claude_relay = legacy_body.kind == "cc-session" and legacy_body.channel == "#cc-relay"
    derived_relay_name = (
        addressing.canonicalize_relay_name(legacy_body.display_name)
        if is_claude_relay else None
    )
    stored_transport = "claude-code" if is_claude_relay else legacy_body.transport
    # The legacy rename producer sends relay-rename-<this session id>-<epoch ms>
    # in the JSON body. A header-only or foreign-session idempotency key still
    # deduplicates the request, but must not grant the destructive takeover action.
    rename_marker = legacy_body.client_event_id or ""
    rename_prefix = f"relay-rename-{agent_id}-"
    rename_suffix = (
        rename_marker[len(rename_prefix):]
        if rename_marker.startswith(rename_prefix)
        else ""
    )
    explicit_takeover = bool(
        rename_suffix and rename_suffix.isascii() and rename_suffix.isdigit()
    )

    async def _do():
        relay_name_for_upsert = derived_relay_name
        async with connect_db() as conn:
            cur = await conn.execute(
                "SELECT binding_epoch FROM agent_status WHERE id=?", (agent_id,)
            )
            legacy_target = await cur.fetchone()
            await cur.close()
            if legacy_target is not None and legacy_target["binding_epoch"] is not None:
                return _omp_error(request, 409, "IDENTITY_CONFLICT")
            # Optimistic pre-check (F3 fast path)
            prior = await check_idempotent(conn, cei)
            if prior is not None:
                # iter-4 HIGH-1 fix: response.registered_at must reflect STORED broker-time,
                # not echo the client-supplied last_activity_ts.
                cur = await conn.execute(
                    "SELECT registered_at FROM agent_status WHERE id = ?", (agent_id,)
                )
                row = await cur.fetchone()
                await cur.close()
                return {"id": agent_id,
                        "registered_at": (row["registered_at"] if row else None),
                        "event_id": prior}

            await conn.execute("BEGIN IMMEDIATE")
            try:
                # Recheck the ever-OMP fence under the same write transaction.
                cur = await conn.execute(
                    "SELECT status,current_task,binding_epoch FROM agent_status WHERE id=?",
                    (agent_id,),
                )
                prior_row = await cur.fetchone()
                await cur.close()
                if prior_row is not None and prior_row["binding_epoch"] is not None:
                    await conn.rollback()
                    return _omp_error(request, 409, "IDENTITY_CONFLICT")
                old_status = prior_row["status"] if prior_row else None

                if derived_relay_name is not None:
                    try:
                        await _claim_relay_name(
                            conn,
                            agent_id,
                            derived_relay_name,
                            takeover=explicit_takeover,
                            require_agent=False,
                        )
                    except HTTPException as exc:
                        if explicit_takeover or exc.status_code != 409:
                            raise
                        # Automatic Claude registration never steals a mailbox. A new
                        # colliding row stays unnamed; an existing row keeps its prior
                        # name through the UPSERT's COALESCE below.
                        relay_name_for_upsert = None

                # Upsert agent_status
                # registered_at is BROKER-TIME on INSERT, MONOTONIC across rows
                # (max(now_ms(), MAX(registered_at)+1)), preserved on UPDATE.
                # Monotonicity rationale: time.time() can return same ms across concurrent
                # writes (resolution + clock-jitter). Cursor pagination requires strict
                # ordering — if two new rows share a registered_at AND the first one's
                # (registered_at, id) <= cursor, the second is silently skipped. The
                # max(now, MAX+1) trick guarantees each new row's registered_at strictly
                # exceeds every prior row, immune to wall-clock collisions.
                cur = await conn.execute("SELECT MAX(registered_at) AS m FROM agent_status")
                max_row = await cur.fetchone()
                await cur.close()
                prev_max = (max_row["m"] or 0)
                broker_now = max(now_ms(), prev_max + 1)
                await conn.execute(
                    "INSERT INTO agent_status (id, kind, channel, thread_id, status, current_task, "
                    " capabilities, last_activity_ts, registered_at, display_name, relay_name, transport) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(id) DO UPDATE SET "
                    " kind=excluded.kind, channel=excluded.channel, thread_id=excluded.thread_id, "
                    " status=excluded.status, current_task=excluded.current_task, "
                    " capabilities=COALESCE(excluded.capabilities, agent_status.capabilities), "
                    " last_activity_ts=excluded.last_activity_ts, "
                    " display_name=excluded.display_name, "
                    " relay_name=COALESCE(excluded.relay_name, agent_status.relay_name), "
                    " transport=COALESCE(excluded.transport, agent_status.transport)",
                    (
                        agent_id, legacy_body.kind, legacy_body.channel, legacy_body.thread_id,
                        legacy_body.status, legacy_body.current_task,
                        json.dumps(legacy_body.capabilities) if legacy_body.capabilities is not None else None,
                        last_act,           # last_activity_ts (client-supplied)
                        broker_now,         # registered_at (broker-time, monotonic; preserved on UPDATE per ON CONFLICT)
                        legacy_body.display_name,  # v1.3.1 (D29) — full-replace semantics: omitted -> NULL on UPDATE
                        relay_name_for_upsert,
                        stored_transport,
                    ),
                )
                # Read back actual stored registered_at (preserved on UPDATE; new on INSERT)
                cur = await conn.execute(
                    "SELECT registered_at FROM agent_status WHERE id = ?", (agent_id,)
                )
                stored = await cur.fetchone()
                await cur.close()
                stored_registered_at = stored["registered_at"]
                # Insert message_log row for SSE/audit (kind='status')
                content = json.dumps({
                    "old_status": old_status,
                    "new_status": legacy_body.status,
                    "current_task": legacy_body.current_task,
                })
                cur = await conn.execute(
                    "INSERT INTO message_log "
                    "(client_event_id, from_agent, to_agent, channel, thread_id, kind, content, ts) "
                    "VALUES (?, ?, ?, ?, ?, 'status', ?, ?)",
                    (cei, agent_id, None, legacy_body.channel, legacy_body.thread_id, content, last_act),
                )
                event_id = cur.lastrowid
                await cur.close()
                await record_idempotent(conn, cei, "POST /api/agent/{id}/status",
                                        agent_id, event_id)
                await conn.commit()
            except (sqlite3.IntegrityError, aiosqlite.IntegrityError):
                # F3: race — another writer won concurrent dedupe. Re-read ledger.
                # Correctness depends on BEGIN IMMEDIATE: the second writer waits for the
                # first to commit before its txn starts, so the post-rollback re-read
                # finds the winner's row. If BEGIN IMMEDIATE is ever removed, this
                # branch may return None and propagate IntegrityError as 500.
                await conn.rollback()
                if cei:
                    prior = await check_idempotent(conn, cei)
                    if prior is not None:
                        # iter-4 HIGH-1: read stored registered_at, not last_act
                        cur = await conn.execute(
                            "SELECT registered_at FROM agent_status WHERE id = ?", (agent_id,)
                        )
                        row = await cur.fetchone()
                        await cur.close()
                        return {"id": agent_id,
                                "registered_at": (row["registered_at"] if row else None),
                                "event_id": prior}
                raise
            except Exception:
                await conn.rollback()
                raise

            # Broadcast SSE
            await broadcast_event({
                "event_id": event_id, "ts": last_act, "kind": "status",
                "agent": agent_id,
                "data": {"old_status": old_status, "new_status": legacy_body.status,
                         "current_task": legacy_body.current_task},
            })
            # iter-4 HIGH-1 fix: return broker-time stored_registered_at, not client last_act
            return {"id": agent_id, "registered_at": stored_registered_at, "event_id": event_id}

    return await with_retry(_do)


# ---- GET /api/agent/{id}/status ----
@app.get("/api/agent/{agent_id}/status")
async def get_agent_status(request: Request, agent_id: str):
    async with connect_db() as conn:
        cur = await conn.execute(
            "SELECT id, kind, status, current_task, last_activity_ts, thread_id, display_name, "
            " relay_name, transport "
            "FROM agent_status WHERE id = ?",
            (agent_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        if not row:
            # F9 (iter2 discriminator): MUST NOT echo agent_id in detail —
            # a token-shaped path param would otherwise appear in the 404 body.
            # Log full agent_id server-side under request_id for diagnosis.
            rid = getattr(request.state, "request_id", "unknown")
            logger.info("agent not registered: rid=%s agent_id=%r", rid, agent_id[:128])
            raise HTTPException(status_code=404, detail="agent not registered")

        # UUID and qualified stable identifiers pass through without folding.
        # Bare IDs fold, but legacy rows retain their exact delivery identifier.
        # Include the owned relay name without dropping stable-ID deliveries.
        canonical_id, exact_id = _legacy_mailbox_keys(agent_id)
        queue_recipients = tuple(dict.fromkeys(
            key for key in (row["relay_name"], canonical_id, exact_id)
            if key is not None
        ))
        placeholders = ",".join("?" * len(queue_recipients))
        cur2 = await conn.execute(
            "SELECT COUNT(*) AS n FROM message_log "
            f"WHERE to_agent IN ({placeholders}) AND kind = 'handoff' AND ts > ?",
            (*queue_recipients, row["last_activity_ts"]),
        )
        qd_row = await cur2.fetchone()
        await cur2.close()

        response = {
            "id": row["id"], "kind": row["kind"], "status": row["status"],
            "queue_depth": qd_row["n"], "last_activity_ts": row["last_activity_ts"],
            "current_task": row["current_task"], "current_thread": row["thread_id"],
            # v1.3.1 (D29): None when no display name set (column NULL); string otherwise
            "display_name": row["display_name"],
        }
        # Preserve the exact legacy war-room response for generic status rows. Relay
        # sessions receive their additive identity/transport fields, including a null
        # relay_name while registered but not yet named.
        if row["transport"] is not None or row["relay_name"] is not None:
            response["relay_name"] = row["relay_name"]
            response["transport"] = row["transport"]
        return response


@app.get("/api/relay-name/{relay_name}")
async def get_relay_name_status(relay_name: str):
    canonical = addressing.canonicalize_relay_name(relay_name)
    if canonical is None:
        raise HTTPException(status_code=422, detail="invalid relay name")
    async with connect_db() as conn:
        cur = await conn.execute(
            "SELECT status, transport FROM agent_status WHERE relay_name=?",
            (canonical,),
        )
        row = await cur.fetchone()
        await cur.close()
    return {
        "relay_name": canonical,
        "registered": row is not None,
        "status": row["status"] if row is not None else None,
        "transport": row["transport"] if row is not None else None,
    }


@app.put("/api/agent/{agent_id}/relay-name")
async def put_agent_relay_name(
    agent_id: str,
    request: Request,
    authorization: Optional[str] = Header(None, alias="Authorization"),
):
    raw = await request.body()
    loose_body = _decode_shared_json_bytes(raw)
    omp_variant = _is_omp_shared_payload("relay-name", authorization, loose_body)
    body = loose_body
    try:
        if omp_variant:
            presented_hash = _relay_control_token_hash(authorization)
            body = _decode_json_bytes(raw)
            _canonical_uuid(agent_id)
            value = _closed_object(body, {
                "schema_version", "relay_name", "takeover",
                "binding_epoch", "omp_process_epoch",
            })
            if (
                type(value["schema_version"]) is not int
                or value["schema_version"] != 1
                or type(value["takeover"]) is not bool
            ):
                raise _OmpFailure(422, "PROTOCOL_INVALID")
            canonical = addressing.canonicalize_relay_name(value["relay_name"])
            if canonical is None or canonical != value["relay_name"]:
                raise _OmpFailure(422, "PROTOCOL_INVALID")
            _canonical_uuid(value["binding_epoch"])
            _canonical_uuid(value["omp_process_epoch"])
            takeover = value["takeover"]
        else:
            try:
                legacy = RelayNameBody.model_validate(body)
            except ValidationError as exc:
                raise RequestValidationError(exc.errors()) from exc
            canonical = legacy.relay_name
            takeover = legacy.takeover
            presented_hash = ""
            value = {}
    except _OmpFailure as exc:
        return _omp_failure_response(request, exc)

    async def _do():
        async with connect_db() as conn:
            await conn.execute("BEGIN IMMEDIATE")
            try:
                cur = await conn.execute(
                    "SELECT * FROM agent_status WHERE id=?", (agent_id,)
                )
                current = await cur.fetchone()
                await cur.close()
                if current is not None and current["binding_epoch"] is not None:
                    if not omp_variant:
                        await conn.rollback()
                        return _omp_error(request, 409, "IDENTITY_CONFLICT")
                    await _require_current_control(
                        conn, agent_id, presented_hash, value["binding_epoch"],
                        value["omp_process_epoch"],
                    )
                elif omp_variant:
                    raise _OmpFailure(403, "CONTROL_INVALID")
                displaced_id = await _claim_relay_name(
                    conn, agent_id, canonical, takeover=takeover, require_agent=True,
                    allow_displace_omp=omp_variant, omp_authorized=omp_variant,
                )
                await conn.commit()
            except _OmpFailure as exc:
                await conn.rollback()
                return _omp_failure_response(request, exc)
            except HTTPException as exc:
                await conn.rollback()
                if omp_variant and exc.status_code == 409:
                    return _omp_error(request, 409, "IDENTITY_CONFLICT")
                raise
            except (sqlite3.IntegrityError, aiosqlite.IntegrityError):
                await conn.rollback()
                if omp_variant:
                    return _omp_error(request, 409, "IDENTITY_CONFLICT")
                raise HTTPException(
                    status_code=409, detail="relay name has no rollback-safe fallback"
                )
            except Exception:
                await conn.rollback()
                raise
            result = {"id": agent_id, "relay_name": canonical, "displaced_id": displaced_id}
            if omp_variant:
                result["schema_version"] = 1
            return result

    return await with_retry(_do)


@app.get("/api/agent/{agent_id}/deliveries")
async def get_agent_deliveries(
    agent_id: str,
    after: int = Query(0, ge=0, le=SQLITE_I64_MAX),
    limit: int = Query(20, ge=1, le=100),
):
    async def _do():
        async with connect_db() as conn:
            await _begin_read_snapshot(conn)
            try:
                cur = await conn.execute(
                    "SELECT id FROM agent_status WHERE id=?", (agent_id,)
                )
                identity = await cur.fetchone()
                await cur.close()
                if identity is None:
                    raise HTTPException(status_code=404, detail="agent not registered")
                cur = await conn.execute(
                    "SELECT m.event_id,m.client_event_id,m.from_agent,m.to_agent,"
                    "m.channel,m.thread_id,m.kind,m.content,m.ts "
                    "FROM delivery d JOIN message_log m ON m.event_id=d.event_id "
                    "WHERE d.recipient_id=? AND d.status='outstanding' "
                    "AND m.kind IN ('agent_message','agent_reply','handoff') "
                    "AND d.event_id>? ORDER BY d.event_id ASC LIMIT ?",
                    (agent_id, after, limit + 1),
                )
                rows = await cur.fetchall()
                await cur.close()
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise
            has_more = len(rows) > limit
            page = rows[:limit]
            return {
                "deliveries": [dict(row) for row in page],
                "next_cursor": page[-1]["event_id"] if has_more and page else None,
            }

    return await with_retry(_do)


@app.post("/api/agent/{agent_id}/delivery-ack")
async def post_agent_delivery_ack(
    agent_id: str,
    request: Request,
    authorization: Optional[str] = Header(None, alias="Authorization"),
):
    raw = await request.body()
    loose_body = _decode_shared_json_bytes(raw)
    omp_variant = _is_omp_shared_payload("delivery-ack", authorization, loose_body)
    body = loose_body
    if omp_variant:
        try:
            presented_hash = _relay_control_token_hash(authorization)
            body = _decode_json_bytes(raw)
            _canonical_uuid(agent_id)
            value = _closed_object(body, {
                "schema_version", "event_id", "offer_token",
                "binding_epoch", "omp_process_epoch",
            })
            if type(value["schema_version"]) is not int or value["schema_version"] != 1:
                raise _OmpFailure(422, "PROTOCOL_INVALID")
            event_id = _canonical_event_id(value["event_id"])
            _canonical_uuid(value["offer_token"])
            _canonical_uuid(value["binding_epoch"])
            _canonical_uuid(value["omp_process_epoch"])
        except _OmpFailure as exc:
            return _omp_failure_response(request, exc)

        async def _omp_do():
            async with connect_db() as conn:
                await conn.execute("BEGIN IMMEDIATE")
                try:
                    binding = await _require_current_control(
                        conn, agent_id, presented_hash, value["binding_epoch"],
                        value["omp_process_epoch"],
                    )
                    delivery, offer = await _delivery_and_offer(conn, agent_id, event_id)
                    if delivery is None:
                        raise _OmpFailure(404, "DELIVERY_NOT_FOUND")
                    if offer is None or offer["offer_token"] != value["offer_token"]:
                        raise _OmpFailure(409, "STATE_CONFLICT", {"status": None})
                    if delivery["status"] == "rejected" or offer["state"] == "rejected":
                        status_value = await _offer_status(conn, agent_id, event_id, binding)
                        raise _OmpFailure(409, "TERMINAL_CONFLICT", {"status": status_value})
                    if offer["state"] != "accepted":
                        status_value = await _offer_status(conn, agent_id, event_id, binding)
                        raise _OmpFailure(409, "STATE_CONFLICT", {"status": status_value})
                    if delivery["status"] == "acked":
                        code = "ALREADY_ACKED"
                    elif delivery["status"] == "outstanding":
                        timestamp = now_ms()
                        cur = await conn.execute(
                            "UPDATE delivery SET status='acked',acked_ts=?,updated_ts=?,"
                            "omp_ack_seq=omp_ack_seq+1 "
                            "WHERE recipient_id=? AND to_agent=? AND event_id=? "
                            "AND status='outstanding'",
                            (
                                timestamp, timestamp, agent_id,
                                delivery["to_agent"], event_id,
                            ),
                        )
                        acked_now = cur.rowcount
                        await cur.close()
                        if acked_now != 1:
                            status_value = await _offer_status(
                                conn, agent_id, event_id, binding
                            )
                            raise _OmpFailure(
                                409, "STATE_CONFLICT", {"status": status_value}
                            )
                        code = "ACKED"
                    else:
                        raise _OmpFailure(503, "PROTOCOL_INVALID")
                    status_value = await _offer_status(conn, agent_id, event_id, binding)
                    await conn.commit()
                    return {"schema_version": 1, "code": code, "status": status_value}
                except _OmpFailure as exc:
                    await conn.rollback()
                    return _omp_failure_response(request, exc)
                except Exception:
                    await conn.rollback()
                    raise

        return await with_retry(_omp_do)

    try:
        legacy = DeliveryAckBody.model_validate(body)
    except ValidationError as exc:
        raise RequestValidationError(exc.errors()) from exc

    async def _legacy_do():
        async with connect_db() as conn:
            await conn.execute("BEGIN IMMEDIATE")
            try:
                cur = await conn.execute(
                    "SELECT binding_epoch,relay_name FROM agent_status WHERE id=?",
                    (agent_id,),
                )
                identity = await cur.fetchone()
                await cur.close()
                if identity is None:
                    raise HTTPException(status_code=404, detail="agent not registered")
                if identity["binding_epoch"] is not None:
                    await conn.rollback()
                    return _omp_error(request, 409, "IDENTITY_CONFLICT")
                await _relocate_legacy_abandoned_ack_rows(
                    conn, agent_id, identity["relay_name"], legacy.event_ids
                )
                timestamp = now_ms()
                placeholders = ",".join("?" * len(legacy.event_ids))
                cur = await conn.execute(
                    f"UPDATE delivery SET status='acked',acked_ts=?,updated_ts=? "
                    f"WHERE recipient_id=? AND event_id IN ({placeholders}) "
                    f"AND status NOT IN ('acked','rejected')",
                    (timestamp, timestamp, agent_id, *legacy.event_ids),
                )
                acked_now = cur.rowcount
                await cur.close()
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise
            return {"acked_now": acked_now, "requested": len(legacy.event_ids)}

    return await with_retry(_legacy_do)


# ---- GET /api/agent/{id}/messages (v1.3 / D28) ----
@app.get("/api/agent/{agent_id}/messages")
async def get_agent_messages(
    agent_id: str,
    request: Request,
    since: int = Query(0, ge=0, le=SQLITE_I64_MAX),
    kind: str = Query("agent_message"),
    limit: int = Query(20, ge=1, le=100),
):
    """Cursor-paginated inbox query (v1.3 / D28).

    Returns the recipient's `agent_message` rows (or any kind if `kind`
    overrides) ordered by event_id ASC. Empty inbox for an unknown
    recipient is returned as 200 with empty list (not 404) -- the inbox
    endpoint is conceptually queryable for any agent_id including
    never-seen ones (idempotent read semantics).

    Security: per F9 iter2 finding, MUST NOT echo agent_id in any error
    detail string (token-shaped path param leak via 404). Log agent_id
    server-side under request_id only.

    M2 (iter-2): `since` and `limit` now have FastAPI Query bounds — `since`
    in [0, SQLITE_I64_MAX] (matches GET /api/events pattern; values above
    i64 max would otherwise overflow aiosqlite's INTEGER bind and surface
    as 500 instead of 422); `limit` in [1, 100] (replaces the prior manual
    `min(max(limit, 1), 100)` clamp — Query validator now enforces, with
    out-of-range producing 422).
    L1 (iter-2): debug-log entry mirrors get_agent_status request_id
    pattern; agent_id truncated to 128 chars to bound log line size.
    """
    rid = getattr(request.state, "request_id", "unknown")
    logger.debug(
        "agent_messages query: rid=%s agent_id=%r kind=%s since=%s limit=%s",
        rid, agent_id[:128], kind, since, limit,
    )

    async def _do():
        async with connect_db() as conn:
            await _begin_read_snapshot(conn)
            try:
                owner_id = await _legacy_mailbox_owner_id(conn, agent_id)
                canonical_key, exact_key = _legacy_mailbox_keys(agent_id)
                # Peek-ahead: fetch limit+1 to detect "more pages" without an extra round-trip.
                peek = limit + 1
                if owner_id is not None:
                    ownership_sql = (
                        "(d.recipient_id=? OR (d.event_id IS NULL AND "
                        "(m.to_agent=? OR m.to_agent=?)))"
                    )
                    ownership_args: tuple[Any, ...] = (
                        owner_id, canonical_key, exact_key,
                    )
                else:
                    ownership_sql = (
                        "(m.to_agent=? OR m.to_agent=?) AND "
                        "(d.event_id IS NULL OR d.recipient_id IS NULL)"
                    )
                    ownership_args = (canonical_key, exact_key)
                cur = await conn.execute(
                    "SELECT m.event_id,m.client_event_id,m.from_agent,m.to_agent,m.channel,"
                    "m.thread_id,m.kind,m.content,m.ts FROM message_log m "
                    "LEFT JOIN delivery d ON d.event_id=m.event_id "
                    f"WHERE {ownership_sql} AND (d.event_id IS NULL OR d.status<>'rejected') "
                    "AND m.event_id>? AND m.kind=? ORDER BY m.event_id ASC LIMIT ?",
                    (*ownership_args, since, kind, peek),
                )
                rows = await cur.fetchall()
                await cur.close()
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise
            if len(rows) > limit:
                next_cursor = rows[limit - 1]["event_id"]
                rows = rows[:limit]
            else:
                next_cursor = None
            return {
                "messages": [dict(r) for r in rows],
                "next_cursor": next_cursor,
            }

    return await with_retry(_do)


@app.post("/api/agent/{agent_id}/ack")
async def post_agent_ack(agent_id: str, body: AckBody, request: Request):
    """Mark delivery shadow-rows acked for this recipient name (WS-B-broker-1/-2).

    Two disjoint cross-off mechanisms in one BEGIN IMMEDIATE txn:
      - explicit `event_ids` (WS-B-broker-1): preserve the legacy transition for
        matching outstanding or abandoned rows, while skipped rejected rows remain
        terminal.
      - `floor` (WS-B-broker-2, #18 mirror): flip every OUTSTANDING row with
        event_id <= floor. The floor is the recipient's gap-aware confirmed-set
        high-water (advances only over a contiguous confirmed run), so it can
        never cross off an unread/gap id above it.

    Idempotent: re-acking an already-acked id is a no-op; an unknown name or
    event_id simply matches no row. `acked_now` = rows that transitioned to
    'acked' on THIS call (the two UPDATEs are disjoint by the status guard, so
    an id that is both in event_ids and <= floor is counted once).

    Security (F9, mirrors get_agent_messages): NEVER echo agent_id into any
    response/error string — a token-shaped path param must not cross the
    response boundary. Logged server-side under request_id, truncated to 128.
    """
    rid = getattr(request.state, "request_id", "unknown")
    logger.debug(
        "agent_ack: rid=%s agent_id=%r n_ids=%s floor=%s",
        rid, agent_id[:128], len(body.event_ids), body.floor,
    )

    async def _do():
        async with connect_db() as conn:
            await conn.execute("BEGIN IMMEDIATE")
            try:
                owner_id = await _legacy_mailbox_owner_id(conn, agent_id)
                canonical_key, exact_key = _legacy_mailbox_keys(agent_id)
                if owner_id is not None:
                    ownership_sql = "recipient_id=?"
                    ownership_args: tuple[Any, ...] = (owner_id,)
                    cur = await conn.execute(
                        "SELECT relay_name FROM agent_status WHERE id=?", (owner_id,)
                    )
                    owner = await cur.fetchone()
                    await cur.close()
                    await _relocate_legacy_abandoned_ack_rows(
                        conn, owner_id, owner["relay_name"], body.event_ids
                    )
                else:
                    ownership_sql = (
                        "recipient_id IS NULL AND (to_agent=? OR to_agent=?)"
                    )
                    ownership_args = (canonical_key, exact_key)
                now = now_ms()
                acked_now = 0
                # Explicit-id acks (WS-B-broker-1). Skipped for floor-only bodies.
                if body.event_ids:
                    placeholders = ",".join("?" * len(body.event_ids))
                    cur = await conn.execute(
                        f"UPDATE delivery SET status='acked',acked_ts=?,updated_ts=? "
                        f"WHERE {ownership_sql} AND event_id IN ({placeholders}) "
                        f"AND status NOT IN ('acked','rejected')",
                        (now, now, *ownership_args, *body.event_ids),
                    )
                    acked_now += cur.rowcount
                    await cur.close()
                # Floor mirror (WS-B-broker-2, #18): cross off every OUTSTANDING row
                # for this recipient with event_id <= floor. A row transitioned by the
                # explicit update is no longer outstanding, so acked_now cannot count it
                # twice. Keeping the floor predicate narrower than explicit IDs prevents
                # inferred cursor progress from reviving an abandoned row. Served by
                # idx_delivery_outstanding(to_agent,status,event_id) as an indexed range.
                #
                # SAFETY (cross-family iter-1 HIGH adjudication) — a floor mirrored from
                # a legacy-cursor-seeded confirmed-set can NOT over-mark an unread row:
                # an OUTSTANDING row is necessarily POST-D71 (the D71 Option-A cutover
                # backfilled every pre-D71 directed row as 'acked'), and a post-D71
                # event_id is only ever cursor-crossed by the CURRENT gap-aware advance
                # (D56, which predates this delivery table) that never passes an
                # unconfirmed id -> a seeded floor stays BELOW any outstanding-unconfirmed
                # row (event_id > floor -> untouched). A pre-D71 message a stale cursor
                # crossed is already 'acked', so the status='outstanding' guard makes it
                # untouchable here. Fail-safe direction preserved (under-mark, never over).
                if body.floor is not None:
                    cur = await conn.execute(
                        "UPDATE delivery SET status='acked',acked_ts=?,updated_ts=? "
                        f"WHERE {ownership_sql} "
                        "AND event_id <= ? AND status='outstanding'",
                        (now, now, *ownership_args, body.floor),
                    )
                    acked_now += cur.rowcount
                    await cur.close()
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise
            return {"acked_now": acked_now, "requested": len(body.event_ids)}

    return await with_retry(_do)


# ---- GET /api/agent-messages/recipients (v1.3.1 / D29) ----
@app.get("/api/agent-messages/recipients")
async def get_agent_messages_recipients(
    limit: int = Query(1000, ge=1, le=1000),
):
    """List distinct recipients of `kind='agent_message'` rows in the message
    log, with msg_count + last_ts + display_name metadata from agent_status.

    Powers the dashboard Agent Messages panel's recipient dropdown (v1.3.1 /
    D29). The earlier dashboard populated this dropdown from /api/state's
    agent registry, which only knows about agents that have POSTed
    /api/agent/{id}/status — claude-relay never POSTs status, so the actual
    UUID conversation pairs from the message_log were invisible. This
    endpoint queries message_log directly so any recipient that has ever
    received an agent_message appears in the dropdown.

    Ordering is newest-first (`MAX(ts) DESC`) so the most recently active
    conversations bubble to the top.

    Metadata resolves a canonical relay-name owner before falling back to a direct
    stable-ID match. When neither row exists, return `null` and the dashboard falls
    back to the recipient prefix only.

    Returns 200 with `{"recipients": []}` for the empty case (NOT 404 —
    idempotent read semantics, mirrors GET /api/agent/{id}/messages
    unknown-recipient handling per D28).

    Security: this endpoint takes no path params and no caller-supplied
    string body — limit is the only input and is validated by FastAPI's
    Query(ge=1, le=1000). FastAPI's default ValidationError envelope is
    used for out-of-range values (preserves F9 discipline; the validation
    error message names the param + bound but never echoes a literal
    user-supplied string beyond what FastAPI already does).
    """
    async def _do():
        async with connect_db() as conn:
            cur = await conn.execute(
                "SELECT m.to_agent, COUNT(*) AS msg_count, MAX(m.ts) AS last_ts, "
                "  CASE WHEN EXISTS "
                "    (SELECT 1 FROM agent_status named WHERE named.relay_name=m.to_agent) "
                "  THEN (SELECT named.display_name FROM agent_status named "
                "        WHERE named.relay_name=m.to_agent) "
                "  ELSE (SELECT direct.display_name FROM agent_status direct "
                "        WHERE direct.id=m.to_agent COLLATE NOCASE) END AS display_name "
                "FROM message_log m "
                "WHERE m.kind = 'agent_message' AND m.to_agent IS NOT NULL "
                "GROUP BY m.to_agent "
                "ORDER BY MAX(m.ts) DESC "
                "LIMIT ?",
                (limit,),
            )
            rows = await cur.fetchall()
            await cur.close()
            recipients = [
                {
                    "to_agent": r["to_agent"],
                    "display_name": r["display_name"],
                    "msg_count": r["msg_count"],
                    "last_ts": r["last_ts"],
                }
                for r in rows
            ]
            return {"recipients": recipients}

    return await with_retry(_do)


# ---- GET /api/agent-messages (v1.4 / D30) ----
@app.get("/api/agent-messages")
async def get_agent_messages_aggregate(
    request: Request,
    since: int = Query(0, ge=0, le=SQLITE_I64_MAX),
    kind: str = Query("agent_message"),
    limit: int = Query(50, ge=1, le=200),
):
    """Cross-recipient aggregate inbox — `agent_message` rows in event_id ASC order (v1.4 / D30).

    Sibling to GET /api/agent/{id}/messages (per-recipient inbox). Returns
    every agent_message row regardless of recipient; intended for the kanban
    dashboard's "All agents" unified view.

    Query params:
      since: cursor (event_id strictly greater than this); default 0 returns from start.
        Validated to [0, SQLITE_I64_MAX] — out-of-range returns 422.
      kind: event kind filter; default 'agent_message'. Other kinds excluded.
      limit: page size; default 50, validated to [1, 200] — out-of-range returns 422.
        (vs inbox cap 100; aggregate fans across recipients so per-fetch yield is
        naturally higher.)

    Empty result for empty broker: 200 with `{"messages": [], "next_cursor": null}`
    (NOT 404). Aggregate is read-only; no agent_message rows = empty inbox is the
    natural answer. Mirrors GET /api/agent/{id}/messages unknown-recipient behavior
    per D28.

    Cursor pagination: peek-ahead (LIMIT limit+1); when an extra row exists,
    `next_cursor` is the `event_id` of the last in-window row (NOT the peek row).
    Callers pass `next_cursor` back as `?since=<event_id>` for the next page;
    `null` indicates last page.

    Note on partial-WHERE exclusion: Backed by partial composite index
    `idx_message_log_kind_event_id_to_agent ... WHERE to_agent IS NOT NULL`. This
    endpoint ONLY returns rows where `to_agent IS NOT NULL`, regardless of `kind`.
    For `kind='agent_message'` (default), the broker's POST validator rejects null
    to_agent at insert time, so this exclusion is informational. For other kinds
    (`status`, `handoff`, etc.), null-to_agent rows are SILENTLY EXCLUDED from
    results — by design, since this is the recipient-side aggregate. Use a
    different endpoint for sender-side or broadcast-style queries.

    Index: backed by partial composite index `idx_message_log_kind_event_id_to_agent`
    per contract.md v1.4 § 2 (leading-equality on kind, range + ORDER BY on event_id;
    partial-WHERE bounds index size to agent_message-eligible rows).

    Security: endpoint has no path parameters; F9 echo-discipline applies trivially.
    debug-log entry mirrors get_agent_messages request_id pattern.
    """
    rid = getattr(request.state, "request_id", "unknown")
    logger.debug(
        "agent_messages_aggregate query: rid=%s kind=%r since=%s limit=%s",
        rid, kind[:128], since, limit,
    )

    async def _do():
        async with connect_db() as conn:
            # Peek-ahead: fetch limit+1 to detect "more pages" without an extra round-trip.
            peek = limit + 1
            cur = await conn.execute(
                "SELECT event_id, client_event_id, from_agent, to_agent, channel, "
                " thread_id, kind, content, ts "
                "FROM message_log "
                "WHERE event_id > ? AND kind = ? AND to_agent IS NOT NULL "
                "ORDER BY event_id ASC LIMIT ?",
                (since, kind, peek),
            )
            rows = await cur.fetchall()
            await cur.close()
            if len(rows) > limit:
                next_cursor = rows[limit - 1]["event_id"]
                rows = rows[:limit]
            else:
                next_cursor = None
            return {
                "messages": [dict(r) for r in rows],
                "next_cursor": next_cursor,
            }

    return await with_retry(_do)


# ---- POST /api/event ----
@app.post("/api/event")
async def post_event(
    body: EventBody,
    request: Request,
    idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
):
    ts = body.ts  # already int via Pydantic validator (F6)
    cei = resolve_client_event_id(body.client_event_id, idempotency_key)  # F1

    # ADDR-08 (2026-08-15): the SINGLE ingress choke point for the recipient grammar.
    # Called exactly once, ABOVE `_do()` ON PURPOSE -- a refusal must open no database
    # connection, so "zero message_log rows and zero delivery rows behind a refusal" holds
    # by construction rather than by inspection, and a request that cannot succeed never
    # consumes a pooled connection.
    route = addressing.parse_recipient(body.to_agent)
    if route.disposition == "refused":
        # No echo of sender-supplied text across the response boundary (see the
        # `_http_exc_handler` comment and the DELETE /api/discord/poll-targets handler).
        # The full address goes to the server-side log only, truncated, under the rid.
        logger.info(
            "address refused: rid=%s reason=%s to_agent=%r",
            getattr(request.state, "request_id", "unknown"),
            route.reason, (body.to_agent or "")[:128],
        )
        raise HTTPException(status_code=422, detail=_REFUSAL_MESSAGES[route.reason])
    # The ONE value bound at all three write sites below. `body.to_agent` is deliberately
    # left unmutated: a mutated Pydantic model is invisible at those three call sites and
    # makes the diff unreviewable.
    to_key = route.key

    async def _do():
        async with connect_db() as conn:
            # Optimistic pre-check (F3 fast path)
            prior = await check_idempotent(conn, cei)
            if prior is not None:
                return {"event_id": prior}

            await conn.execute("BEGIN IMMEDIATE")
            try:
                sender_id = None
                recipient_id = None
                if to_key and body.kind in DELIVERY_KINDS:
                    sender = None
                    sender_name = addressing.canonicalize_relay_name(body.from_agent)
                    if sender_name is not None:
                        cur = await conn.execute(
                            "SELECT id FROM agent_status WHERE relay_name=?",
                            (sender_name,),
                        )
                        sender = await cur.fetchone()
                        await cur.close()
                    if sender is None:
                        cur = await conn.execute(
                            "SELECT id FROM agent_status WHERE id=?", (body.from_agent,)
                        )
                        sender = await cur.fetchone()
                        await cur.close()
                    sender_id = sender["id"] if sender is not None else None
                    cur = await conn.execute(
                        "SELECT COALESCE("
                        "(SELECT owner_id FROM omp_relay_namespace WHERE relay_name=?),"
                        "(SELECT id FROM agent_status WHERE relay_name=?),"
                        "(SELECT id FROM agent_status WHERE id=?)) AS id",
                        (to_key, to_key, to_key),
                    )
                    recipient = await cur.fetchone()
                    await cur.close()
                    recipient_id = (
                        recipient["id"]
                        if recipient is not None and recipient["id"] is not None
                        else None
                    )
                cur = await conn.execute(
                    "INSERT INTO message_log "
                    "(client_event_id, from_agent, to_agent, channel, thread_id, kind, content, ts) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (cei, body.from_agent, to_key, body.channel,
                     body.thread_id, body.kind, body.content, ts),
                )
                event_id = cur.lastrowid
                await cur.close()
                # WS-B-broker-1: co-transacted delivery shadow-row for directed relay
                # kinds (atomic with the message_log insert). Idempotent replay returns
                # before BEGIN IMMEDIATE (above), so no duplicate row is created here.
                if to_key and body.kind in DELIVERY_KINDS:
                    _dnow = now_ms()
                    await conn.execute(
                        "INSERT INTO delivery "
                        "(to_agent,event_id,status,attempts,created_ts,updated_ts,"
                        "sender_id,recipient_id) "
                        "VALUES (?,?,'outstanding',0,?,?,?,?)",
                        (to_key, event_id, _dnow, _dnow, sender_id, recipient_id),
                    )
                await record_idempotent(conn, cei, "POST /api/event",
                                        body.from_agent, event_id)
                await conn.commit()
            except (sqlite3.IntegrityError, aiosqlite.IntegrityError) as exc:
                # Trigger failures are closed OMP protocol violations, not internal
                # errors. Other integrity failures may be a concurrent legacy dedupe.
                await conn.rollback()
                if any(name in str(exc) for name in OMP_TRIGGER_ERRORS):
                    return _omp_error(request, 422, "PROTOCOL_INVALID")
                if cei:
                    prior = await check_idempotent(conn, cei)
                    if prior is not None:
                        return {"event_id": prior}
                raise
            except Exception:
                await conn.rollback()
                raise

            await broadcast_event({
                "event_id": event_id, "ts": ts, "kind": body.kind,
                "agent": body.from_agent,
                # THIRD BIND SITE. Missing it is SILENT: the delivery row would be filed
                # under `to_key` while the envelope carried something else, and the MCP
                # plugin's SSE filter is a bare `!=` that drops a non-matching envelope
                # without a log line (plugin server.py:2046).
                "data": {"to_agent": to_key, "content": body.content,
                         "channel": body.channel, "thread_id": body.thread_id},
            })
            return {"event_id": event_id}

    return await with_retry(_do)


# ---- GET /api/state ----
def _decode_state_cursor(cursor: Optional[str], request: Optional[Request] = None) -> Optional[tuple[int, str]]:
    """Decode opaque cursor `<registered_at>:<id>` to (ts, id) tuple. None if absent.

    Strict input validation (iter-4 MED): empty string -> 422 (caller bug, not absence);
    missing colon -> 422; non-int ts segment -> 422; empty id segment -> 422; ts out of
    practical range -> 422. Returns None ONLY for cursor=None (true absence).

    F1 (iter1) + F9 (iter2): error detail strings MUST NOT include the raw cursor
    value — a malicious caller could pass a token-shaped cursor and have it
    echoed. The full offending cursor is logged server-side under request_id
    for diagnostics.
    """
    def _reject(reason: str) -> None:
        if request is not None:
            rid = getattr(request.state, "request_id", "unknown")
            logger.info("cursor reject: rid=%s reason=%s cursor=%r",
                        rid, reason, (cursor or "")[:128])
        raise HTTPException(status_code=422, detail=reason)

    if cursor is None:
        return None
    if cursor == "":
        _reject("cursor cannot be empty string (omit param to start)")
    parts = cursor.split(":", 1)
    if len(parts) != 2:
        _reject("invalid cursor format (missing ':')")
    ts_str, id_str = parts
    if not ts_str:
        _reject("cursor ts segment empty")
    if not id_str:
        _reject("cursor id segment empty")
    try:
        ts = int(ts_str)
    except ValueError:
        _reject("cursor ts not an int")
    # Reject negative + > SQLite i64 max (binding overflow -> 500 if not caught here).
    # Iter-6 + iter-7: SQLITE_I64_MAX is module-scoped; same guard reused across all int inputs.
    if ts < 0:
        _reject("cursor ts negative")
    if ts > SQLITE_I64_MAX:
        _reject("cursor ts exceeds i64 max")
    return ts, id_str


def _encode_state_cursor(registered_at: int, id_: str) -> str:
    return f"{registered_at}:{id_}"


@app.get("/api/state")
async def get_state(
    request: Request,
    limit: int = Query(100, ge=1, le=1000),
    cursor: Optional[str] = Query(None, description="Opaque pagination cursor"),
):
    """Snapshot of cross-project state.

    Pagination semantics (v1.0; iter-3+iter-4 broker fixes):
    - Cursor is OPAQUE — clients pass back the previous response's `next_cursor`.
    - Composite ordering: ORDER BY registered_at ASC, id ASC.
    - Composite predicate: WHERE (registered_at > cursor_ts)
                              OR (registered_at = cursor_ts AND id > cursor_id).
    - Pipelines collection always returned in full (NOT bounded by `limit`; iter-4 HIGH-4).
      Typically <100 rows across all projects; if this grows unbounded, a separate
      pipeline cursor would be added in v1.1.
    - registered_at is broker-time AND monotonic (max(now, MAX+1)) so concurrent
      inserts during pagination cannot collide.
    - Peek-ahead: SELECT LIMIT+1 → return first `limit`; emit next_cursor only when
      the extra row exists (iter-4 HIGH-3 fix; eliminates spurious cursor on exact-
      multiple-of-limit boundary).
    """
    decoded = _decode_state_cursor(cursor, request=request)

    async with connect_db() as conn:
        # Peek-ahead: fetch limit+1 to detect "more pages" without an extra round-trip
        peek = limit + 1
        if decoded is not None:
            cur_ts, cur_id = decoded
            cur = await conn.execute(
                "SELECT id, kind, channel, thread_id, status, current_task, last_activity_ts, "
                "registered_at, display_name, relay_name, transport "
                "FROM agent_status "
                "WHERE registered_at > ? OR (registered_at = ? AND id > ?) "
                "ORDER BY registered_at, id LIMIT ?",
                (cur_ts, cur_ts, cur_id, peek),
            )
        else:
            cur = await conn.execute(
                "SELECT id, kind, channel, thread_id, status, current_task, last_activity_ts, "
                "registered_at, display_name, relay_name, transport "
                "FROM agent_status ORDER BY registered_at, id LIMIT ?", (peek,),
            )
        rows = await cur.fetchall()
        await cur.close()
        has_more = len(rows) > limit
        agents = [dict(r) for r in rows[:limit]]  # trim peek-row off

        # Pipelines: ALWAYS full (NOT bounded by agent `limit` per HIGH-4 fix).
        # If pipelines_view ever grows beyond ~1000 a separate pipeline cursor is needed.
        cur = await conn.execute(
            "SELECT project_name, pipeline_id, shape, started_at, completed_at, status, "
            " step_count, steps_done, last_refresh_ts FROM pipelines_view "
            "ORDER BY project_name, pipeline_id"
        )
        pipelines = [dict(r) for r in await cur.fetchall()]
        await cur.close()

    next_cursor = (_encode_state_cursor(agents[-1]["registered_at"], agents[-1]["id"])
                   if (has_more and agents) else None)
    return {"agents": agents, "pipelines": pipelines, "next_cursor": next_cursor}


# ---- GET /api/events  (SSE) ----
@app.get("/api/events")
async def get_events(
    request: Request,
    cursor: Optional[int] = Query(None, ge=0, le=SQLITE_I64_MAX),
    last_event_id: Optional[str] = Header(None, alias="Last-Event-ID"),
):
    """SSE event stream. Cursor resolution priority: Last-Event-ID header > ?cursor= query.
    Both are bounded to [0, SQLITE_I64_MAX] to prevent overflow on aiosqlite bind (iter-7 fix)."""
    resolved = 0
    if last_event_id is not None:
        try:
            parsed = int(last_event_id)
        except ValueError:
            # F1 (iter1) + F9 (iter2): do not echo last_event_id — could contain
            # token-shaped value. Log offending input server-side under request_id.
            rid = getattr(request.state, "request_id", "unknown")
            logger.info("Last-Event-ID reject: rid=%s reason=not-int value=%r",
                        rid, last_event_id[:128])
            raise HTTPException(status_code=422, detail="Last-Event-ID not an int")
        if parsed < 0:
            raise HTTPException(status_code=422, detail="Last-Event-ID negative")
        if parsed > SQLITE_I64_MAX:
            raise HTTPException(status_code=422, detail="Last-Event-ID exceeds i64 max")
        resolved = parsed
    elif cursor is not None:
        resolved = cursor  # already validated by FastAPI Query(ge=0, le=SQLITE_I64_MAX)
    return EventSourceResponse(sse_event_generator(request, resolved))


# ---- POST /api/project/register ----
@app.post("/api/project/register")
async def post_project_register(request: Request, body: ProjectRegisterBody):
    state_db_path = body.state_db_path
    rid = getattr(request.state, "request_id", "unknown")
    # D27 iter-2 (M3): null-byte rejection runs FIRST so a path containing
    # a null byte that ALSO fails the absoluteness check still surfaces the
    # more-specific null-byte error. Applies in BOTH local_validation modes
    # (universal — runs before the body.local_validation branch) because the
    # path is recorded as metadata in project_registry.state_db_path either
    # way and a null-byte in stored metadata can break downstream consumers.
    if "\x00" in state_db_path:
        raise HTTPException(status_code=400, detail="state_db_path must not contain null bytes")
    # D27: cross-platform-aware syntactic absoluteness check. The path is
    # recorded as metadata in project_registry.state_db_path and surfaced in
    # /api/state regardless of mode, so the shape check applies to BOTH
    # local_validation=True and =False. PureWindowsPath/PurePosixPath
    # decouple shape-checking from the host OS's pathlib flavor (the bug
    # that motivated D27: Linux pathlib.Path treats `E:\...` as relative).
    is_abs = (
        PureWindowsPath(state_db_path).is_absolute()
        or PurePosixPath(state_db_path).is_absolute()
    )
    if not is_abs:
        raise HTTPException(status_code=400, detail="state_db_path must be absolute")

    # D27: local_validation=False skips all FS-dependent checks (existence,
    # sqlite_master table presence, refresh_pipelines_view, watcher start)
    # so a Linux broker can register a Windows-resident project. Project
    # registry row is still UPSERTed. [CORRECTED 2026-08-04: this used to claim
    # "pipelines_view rows are populated entirely from the caller's subsequent
    # post_event/post_status posts". FALSE — a write census of this file finds
    # pipelines_view touched at exactly three sites: the DELETE+INSERT rebuild inside
    # refresh_pipelines_view(), and one DELETE in post_admin_prune(). post_event and
    # post_status write message_log and the delivery ledger and NEVER touch
    # pipelines_view. So for a local_validation=False project, pipelines_view is
    # populated by nothing at all — which is the actual D27 trade-off and is worth
    # knowing before anyone relies on that path.]
    if body.local_validation:
        if not Path(state_db_path).exists():
            # F9 (iter2 discriminator): MUST NOT echo state_db_path in detail —
            # a token-shaped path would otherwise appear in the 400 body. Log
            # offending value server-side under request_id.
            logger.info("project register reject: rid=%s reason=path_not_readable path=%r",
                        rid, state_db_path[:256])
            raise HTTPException(status_code=400, detail="state_db_path not readable")

        # Validate schema (presence of required tables)
        try:
            async with connect_db(state_db_path) as conn:
                cur = await conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' "
                    "AND name IN ('pipelines','pipeline_steps','pipeline_dependencies')"
                )
                tables = {r["name"] for r in await cur.fetchall()}
                await cur.close()
            missing = {"pipelines", "pipeline_steps", "pipeline_dependencies"} - tables
            if missing:
                # F9: `missing` is computed from a HARDCODED constant set; no user
                # input flows here, so interpolation is static-safe. Keep diagnostic
                # listing in the response since it helps callers fix their schema.
                raise HTTPException(status_code=400,
                                    detail=f"state.db missing required tables: {sorted(missing)}")
        except sqlite3.DatabaseError as e:
            # F9 (iter2 discriminator): MUST NOT echo str(e) — sqlite error strings
            # can include the offending file path. Log full exception server-side.
            logger.info("project register reject: rid=%s reason=sqlite_unreadable exc=%s",
                        rid, type(e).__name__)
            logger.debug("sqlite error detail: %s", e)
            raise HTTPException(status_code=400, detail="state.db unreadable")

    # F-CC36 follow-up: capture the project's PRIOR state_db_path (if any) so a re-register
    # that points it at a DIFFERENT file can evict the now-stale cached foreign connection
    # (otherwise its long-lived aiosqlite worker thread leaks until shutdown).
    prev_state_db_path = None
    if body.local_validation:
        async with connect_db() as _pconn:
            _pcur = await _pconn.execute(
                "SELECT state_db_path FROM project_registry WHERE name = ?", (body.name,)
            )
            _prow = await _pcur.fetchone()
            await _pcur.close()
            if _prow is not None:
                prev_state_db_path = _prow["state_db_path"]

    project_id_new = str(uuid.uuid4())
    ts = now_ms()

    # F4: wrap in with_retry for SQLITE_BUSY backoff coverage
    async def _commit_register():
        async with connect_db() as conn:
            await conn.execute("BEGIN IMMEDIATE")
            try:
                # Upsert project_registry by name
                # D27 iter-2 (M1): persist local_validation so lifespan startup
                # and post_project_refresh can recall registration mode after
                # the request returns. excluded.local_validation in the SET
                # clause means a re-register flips the persisted mode if the
                # caller changes the flag (which is the correct UPSERT semantic).
                await conn.execute(
                    "INSERT INTO project_registry (project_id, name, root_path, state_db_path, "
                    " registered_at, last_refresh_ts, local_validation) "
                    "VALUES (?, ?, ?, ?, ?, NULL, ?) "
                    "ON CONFLICT(name) DO UPDATE SET "
                    " root_path=excluded.root_path, state_db_path=excluded.state_db_path, "
                    " local_validation=excluded.local_validation",
                    (project_id_new, body.name, body.root_path, state_db_path, ts,
                     int(body.local_validation)),
                )
                # F8: read back the actual stored row (project_id may differ on re-register)
                cur = await conn.execute(
                    "SELECT project_id, registered_at FROM project_registry WHERE name = ?",
                    (body.name,),
                )
                row = await cur.fetchone()
                await cur.close()
                await conn.commit()
                return row["project_id"], row["registered_at"]
            except Exception:
                await conn.rollback()
                raise

    actual_project_id, actual_registered_at = await with_retry(_commit_register)

    # D27: only run initial materialization + start watcher when the broker
    # has local FS access to the state.db file. In remote mode these would
    # categorically fail (no FS access) and the project's pipelines_view rows
    # are populated via posted events instead.
    if body.local_validation:
        # F-CC36 follow-up: if a re-register changed the state.db path, drop the stale
        # cached foreign connection for the OLD path before refreshing the new one.
        if prev_state_db_path is not None and prev_state_db_path != state_db_path:
            await evict_foreign_conn(prev_state_db_path)
        # Initial materialization + start watcher
        await refresh_pipelines_view(body.name)
        loop = asyncio.get_running_loop()
        start_watcher_for_project(body.name, state_db_path, loop)

    return {"project_id": actual_project_id, "registered_at": actual_registered_at}


# ---- POST /api/project/{name}/refresh ----
@app.post("/api/project/{project_name}/refresh")
async def post_project_refresh(project_name: str):
    # D27 iter-2 (H2): consult project_registry first so we (a) return 404 for
    # an unknown project (instead of a misleading 200 + pipeline_count=0 from
    # refresh_pipelines_view's missing-file branch), and (b) return 409 for
    # a remote-registered project (the broker categorically cannot read the
    # state.db on a different host; refresh_pipelines_view would silently
    # no-op with 0). Operators saw 200/0 and believed refresh succeeded —
    # 409 surfaces the actual semantic ("not applicable in this mode").
    async with connect_db() as conn:
        cur = await conn.execute(
            "SELECT local_validation FROM project_registry WHERE name = ?",
            (project_name,),
        )
        row = await cur.fetchone()
        await cur.close()
    if row is None:
        raise HTTPException(status_code=404, detail="project not registered")
    if not row["local_validation"]:
        raise HTTPException(
            status_code=409,
            detail="refresh not applicable for remote-registered projects",
        )
    count = await refresh_pipelines_view(project_name)
    return {"refreshed_at": now_ms(), "pipeline_count": count}


# ---- GET /api/discord/cursors ----
@app.get("/api/discord/cursors")
async def get_discord_cursors(
    channel_id: Optional[str] = Query(None),
    thread_id: Optional[str] = Query(None),
):
    async with connect_db() as conn:
        if channel_id is None and thread_id is None:
            cur = await conn.execute(
                "SELECT channel_id, thread_id, last_seen_message_id, last_check_ts FROM discord_cursors"
            )
        elif channel_id is not None and thread_id is None:
            cur = await conn.execute(
                "SELECT channel_id, thread_id, last_seen_message_id, last_check_ts FROM discord_cursors "
                "WHERE channel_id = ?", (channel_id,),
            )
        else:
            cur = await conn.execute(
                "SELECT channel_id, thread_id, last_seen_message_id, last_check_ts FROM discord_cursors "
                "WHERE channel_id = ? AND thread_id = ?", (channel_id or "", thread_id or ""),
            )
        rows = [dict(r) for r in await cur.fetchall()]
        await cur.close()
    return {"cursors": rows}


# ---- POST /api/discord/cursors ----
@app.post("/api/discord/cursors")
async def post_discord_cursors(body: DiscordCursorBody):
    last_check = body.last_check_ts  # already int via Pydantic validator (F6)
    thread_id = body.thread_id if body.thread_id is not None else ""

    async def _do():
        async with connect_db() as conn:
            await conn.execute("BEGIN IMMEDIATE")
            try:
                await conn.execute(
                    "INSERT INTO discord_cursors (channel_id, thread_id, last_seen_message_id, last_check_ts) "
                    "VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(channel_id, thread_id) DO UPDATE SET "
                    " last_seen_message_id=excluded.last_seen_message_id, "
                    " last_check_ts=excluded.last_check_ts",
                    (body.channel_id, thread_id, body.last_seen_message_id, last_check),
                )
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise
        return {"updated": True}

    return await with_retry(_do)


# ---- POST /api/discord/poll-targets (M2 D16 § (a)) ----
@app.post("/api/discord/poll-targets")
async def post_discord_poll_target(body: DiscordPollTargetBody):
    """Register a runtime poll target (per D16 § (a) tier 2). Idempotent: re-POST
    of an existing (channel_id, thread_id) is an upsert.

    M2-pending non-contract addition; absorbed when contract.md bumps to v1.1.
    """
    thread_id = body.thread_id if body.thread_id is not None else ""
    # Try in-memory poller first (so live polling picks up immediately).
    # If poller absent (graceful skip on startup due to missing token / .env),
    # still write to the DB row so a future broker restart finds it.
    from broker import discord_poller as _dp
    poller = _dp.get_poller()
    if poller is not None:
        await poller.register_target(body.channel_id, thread_id)
    else:
        # Persist directly so re-start picks it up.
        async def _do():
            async with connect_db() as conn:
                await conn.execute("BEGIN IMMEDIATE")
                try:
                    await conn.execute(
                        "INSERT INTO discord_poll_targets "
                        "(channel_id, thread_id, registered_at, source, last_polled_ts) "
                        "VALUES (?, ?, ?, 'runtime_register', NULL) "
                        "ON CONFLICT(channel_id, thread_id) DO UPDATE SET "
                        " source='runtime_register'",
                        (body.channel_id, thread_id, now_ms()),
                    )
                    await conn.commit()
                except Exception:
                    await conn.rollback()
                    raise
        await with_retry(_do)
    return {"channel_id": body.channel_id, "thread_id": thread_id, "registered": True}


# ---- GET /api/discord/poll-targets (M2 D16 § (a)) ----
@app.get("/api/discord/poll-targets")
async def get_discord_poll_targets():
    """List all registered poll targets (bootstrap + runtime). Reads directly
    from DB so it works whether or not the poller is running."""
    async with connect_db() as conn:
        cur = await conn.execute(
            "SELECT channel_id, thread_id, registered_at, source, last_polled_ts "
            "FROM discord_poll_targets ORDER BY registered_at, channel_id, thread_id"
        )
        rows = [dict(r) for r in await cur.fetchall()]
        await cur.close()
    return {"poll_targets": rows}


# ---- DELETE /api/discord/poll-targets (M2 D16 § (a)) ----
@app.delete("/api/discord/poll-targets")
async def delete_discord_poll_target(
    channel_id: str = Query(..., min_length=1),
    thread_id: Optional[str] = Query(None),
):
    """Idempotent delete: 200 on success, 404 on unknown (channel_id, thread_id).
    Accepts query params (no body — DELETE bodies are non-portable).
    """
    tid = thread_id if thread_id is not None else ""
    from broker import discord_poller as _dp
    poller = _dp.get_poller()
    if poller is not None:
        deleted = await poller.unregister_target(channel_id, tid)
    else:
        async def _do():
            async with connect_db() as conn:
                await conn.execute("BEGIN IMMEDIATE")
                try:
                    cur = await conn.execute(
                        "DELETE FROM discord_poll_targets "
                        "WHERE channel_id = ? AND thread_id = ?",
                        (channel_id, tid),
                    )
                    n = cur.rowcount or 0
                    await cur.close()
                    await conn.commit()
                    return n
                except Exception:
                    await conn.rollback()
                    raise
        n = await with_retry(_do)
        deleted = n > 0
    if not deleted:
        # F1 (iter1): do NOT interpolate channel_id / thread_id into detail —
        # if a caller passed a token-shaped value, this would echo it back.
        # The 404 status code + endpoint path are sufficient signal; the actual
        # query params are visible to the caller in their own request.
        raise HTTPException(status_code=404, detail="poll target not found")
    return {"channel_id": channel_id, "thread_id": tid, "deleted": True}


# ---- POST /api/admin/audit-snapshot (gated) ----
@app.post("/api/admin/audit-snapshot")
async def post_admin_audit_snapshot(request: Request, body: AuditSnapshotBody):
    # Gate: BROKER_ADMIN_ENABLED OR Authorization: Bearer <BROKER_BEARER_TOKEN>
    enabled = _env_bool("BROKER_ADMIN_ENABLED", False)
    if not enabled:
        expected = _env_str("BROKER_BEARER_TOKEN", "")
        auth = request.headers.get("authorization", "")
        token = auth[len("Bearer "):].strip() if auth.startswith("Bearer ") else ""
        if not expected or token != expected:
            raise HTTPException(status_code=403, detail="admin endpoint disabled")

    # Snapshot window: floor(now / 3600s) * 3600s if not provided
    window_ms = body.snapshot_window_start_ts
    if window_ms is None:
        window_sec = (now_ms() // 1000 // 3600) * 3600
        window_ms = window_sec * 1000

    output_dir = Path(_env_str("BROKER_AUDIT_DIR", "audit/pipeline-runs"))
    output_dir.mkdir(parents=True, exist_ok=True)

    # Manual endpoint always overwrites (non-idempotent by design — explicit re-trigger).
    # Runner path uses overwrite=False (atomic O_EXCL create-or-skip).
    async with connect_db() as conn:
        snapshot = await _emit_audit_snapshot(window_ms, output_dir, conn)

    return {"output_path": snapshot["output_path"],
            "event_count": snapshot["event_count"],
            "snapshot_window_start_ts": snapshot["snapshot_window_start_ts"]}


class PruneBody(BaseModel):
    days: int = Field(default=7, ge=1)


# ---- POST /api/admin/prune (broker-owned; DESTRUCTIVE; per broker_dashboard handoff) ----
@app.post("/api/admin/prune")
async def post_admin_prune(body: Optional[PruneBody] = None):
    """Prune stale rows older than `days`. Destructive. One read-write transaction.

    - agent_status: legacy rows whose last_activity_ts (epoch-ms) < cutoff and
      that own no outstanding stable delivery. Ever-OMP identities are retained.
    - pipelines_view: TERMINAL rows (done/failed/cancelled) whose started_at < cutoff.
      BEST-EFFORT — pipelines_view is a per-project cache re-materialized by
      refresh_pipelines_view() (DELETE+INSERT from each project's state.db), so a
      terminal row still present upstream re-appears on the next refresh. In-flight
      pipelines (open/in_progress/blocked) are NEVER pruned. started_at is mixed-unit
      (10-digit epoch-s for some producers, 13-digit ms for others) -> inline CASE
      normalizes to ms before comparison. message_log retention is left to its
      existing daily runner.

    `days` is read from an optional JSON body {"days": N} (the dashboard prune button
    POSTs application/json); defaults to 7 when the body is absent. days >= 1 (pydantic
    Field ge=1 -> 422 otherwise).
    """
    days = body.days if body is not None else 7
    cutoff = now_ms() - days * 86_400_000

    async def _do():
        async with connect_db() as conn:
            await conn.execute("BEGIN IMMEDIATE")
            try:
                cur_a = await conn.execute(
                    "DELETE FROM agent_status "
                    "WHERE last_activity_ts<? AND binding_epoch IS NULL "
                    "AND NOT EXISTS (SELECT 1 FROM delivery d "
                    "WHERE d.recipient_id=agent_status.id AND d.status='outstanding')",
                    (cutoff,),
                )
                deleted_agents = cur_a.rowcount or 0
                await cur_a.close()
                cur_p = await conn.execute(
                    "DELETE FROM pipelines_view "
                    "WHERE status IN ('done','failed','cancelled') "
                    "AND (CASE WHEN started_at < 1000000000000 "
                    "          THEN started_at * 1000 ELSE started_at END) < ?",
                    (cutoff,),
                )
                deleted_pipelines = cur_p.rowcount or 0
                await cur_p.close()
                await conn.commit()
                return deleted_agents, deleted_pipelines
            except Exception:
                await conn.rollback()
                raise

    deleted_agents, deleted_pipelines = await with_retry(_do)
    return {
        "deleted_agents": deleted_agents,
        "deleted_pipelines": deleted_pipelines,
        "days": days,
        "cutoff": cutoff,
    }


# ---- GET /api/health ----
async def _omp_relay_contract_available(conn: aiosqlite.Connection) -> bool:
    expected_sql = _expected_omp_schema_sql()
    names = tuple(sorted(expected_sql))
    placeholders = ",".join("?" * len(names))
    cur = await conn.execute(
        f"SELECT name,sql FROM sqlite_master WHERE name IN ({placeholders})",
        names,
    )
    objects = await cur.fetchall()
    await cur.close()
    installed_sql = {
        row["name"]: _normalize_schema_definition(row["sql"] or "")
        for row in objects
    }
    if installed_sql != expected_sql:
        return False

    cur = await conn.execute("PRAGMA table_info(agent_status)")
    agent_columns = {row["name"] for row in await cur.fetchall()}
    await cur.close()
    cur = await conn.execute("PRAGMA index_list(agent_status)")
    agent_indexes = {row["name"]: row["unique"] for row in await cur.fetchall()}
    await cur.close()
    cur = await conn.execute("PRAGMA table_info(delivery)")
    delivery_columns = {row["name"] for row in await cur.fetchall()}
    await cur.close()
    cur = await conn.execute("PRAGMA table_info(idempotency_ledger)")
    ledger_info = await cur.fetchall()
    await cur.close()
    ledger_columns = {row["name"] for row in ledger_info}
    ledger_pk = [
        name for _position, name in sorted(
            (row["pk"], row["name"]) for row in ledger_info if row["pk"]
        )
    ]
    cur = await conn.execute("PRAGMA index_list(idempotency_ledger)")
    ledger_indexes = {row["name"]: row["unique"] for row in await cur.fetchall()}
    await cur.close()
    cur = await conn.execute("PRAGMA index_list(delivery)")
    delivery_indexes = {row["name"] for row in await cur.fetchall()}
    await cur.close()
    cur = await conn.execute("PRAGMA index_list(delivery_offer)")
    offer_indexes = {row["name"] for row in await cur.fetchall()}
    await cur.close()
    cur = await conn.execute("PRAGMA user_version")
    version = (await cur.fetchone())[0]
    await cur.close()

    required_agent = set(_OMP_AGENT_COLUMNS)
    required_delivery = {
        "sender_id", "recipient_id", "omp_ack_seq", "rejected_ts",
        "reject_reason_code", "rejection_notice_event_id",
    }
    required_ledger = {
        "namespace", "request_hash", "response_status", "response_json",
        "new_binding_epoch",
    }
    route_methods = {
        (getattr(route, "path", ""), method)
        for route in app.routes
        for method in (getattr(route, "methods", None) or set())
    }
    required_routes = {
        ("/api/agent/{agent_id}/status", "POST"),
        ("/api/agent/{agent_id}/relay-name", "PUT"),
        ("/api/agent/{agent_id}/delivery-offer", "POST"),
        ("/api/agent/{agent_id}/delivery-offer-status", "POST"),
        ("/api/agent/{agent_id}/delivery/{event_id_text}/status", "GET"),
        ("/api/agent/{agent_id}/delivery-ack", "POST"),
        ("/api/operator/agent/{recipient_id}/delivery-reject", "POST"),
        ("/api/operator/delivery/{event_id_text}/reject-unbound", "POST"),
    }
    if not (
        required_agent.issubset(agent_columns)
        and required_delivery.issubset(delivery_columns)
        and required_ledger.issubset(ledger_columns)
        and ledger_pk == ["namespace", "client_event_id"]
        and ledger_indexes.get("idx_idemp_omp_bind_epoch") == 1
        and "idx_delivery_recipient_status_event" in delivery_indexes
        and agent_indexes.get("idx_agent_status_relay_name") == 1
        and "idx_delivery_offer_recipient_state_event" in offer_indexes
        and required_routes.issubset(route_methods)
        and version == 5
        and _operator_config() is not None
    ):
        return False

    cur = await conn.execute(
        "SELECT COUNT(*) AS n FROM ("
        "SELECT d.rowid FROM delivery d "
        "LEFT JOIN delivery_offer o ON o.recipient_id=d.recipient_id "
        "AND o.event_id=d.event_id "
        "LEFT JOIN agent_status a ON a.id=d.recipient_id "
        "WHERE (a.binding_epoch IS NOT NULL AND ("
        "d.status NOT IN ('outstanding','acked','rejected') OR "
        "(d.status='acked' AND (d.omp_ack_seq<>1 OR o.state IS NULL OR o.state<>'accepted')) OR "
        "(d.status<>'acked' AND d.omp_ack_seq<>0))) "
        "OR (d.status='rejected' AND (d.reject_reason_code IS NULL OR "
        "(o.state IS NOT NULL AND (o.state<>'rejected' OR o.reason<>d.reject_reason_code)))) "
        "UNION ALL "
        "SELECT -o.rowid FROM delivery_offer o "
        "LEFT JOIN delivery d ON d.recipient_id=o.recipient_id AND d.event_id=o.event_id "
        "LEFT JOIN agent_status a ON a.id=o.recipient_id "
        "WHERE d.event_id IS NULL OR a.binding_epoch IS NULL OR "
        "(o.state IN ('offered','injected') AND d.status<>'outstanding') OR "
        "(o.state='accepted' AND d.status NOT IN ('outstanding','acked')) OR "
        "(o.state='rejected' AND (d.status<>'rejected' OR o.reason IS NULL "
        "OR o.reason<>d.reject_reason_code)) "
        "UNION ALL "
        "SELECT a.rowid FROM agent_status a WHERE "
        "(a.binding_epoch IS NULL AND (a.omp_process_epoch IS NOT NULL "
        "OR a.omp_instance_id IS NOT NULL OR a.control_epoch IS NOT NULL "
        "OR a.control_token_hash IS NOT NULL OR a.bind_attempt_id IS NOT NULL)) "
        "OR (a.binding_epoch IS NOT NULL AND (a.transport IS NOT 'omp-mcp' "
        "OR a.omp_process_epoch IS NULL OR a.omp_instance_id IS NULL "
        "OR a.control_epoch IS NULL OR a.bind_attempt_id IS NULL))"
        ")"
    )
    contradictions = await cur.fetchone()
    await cur.close()
    return contradictions is not None and contradictions["n"] == 0


@app.get("/api/health")
async def get_health():
    sqlite_ok = True
    last_event_ts = 0
    omp_relay_contract = 0
    # An EMPTY message_log has no MAX(ts). That is a fact about the database and
    # must be established as one -- deriving it from `last_event_ts == 0` conflates
    # "no traffic yet" with "the age check does not apply", and that conflation is
    # what let a freshly-created 4,096-byte coordination.db report 'ok' while the
    # real hub was not being served at all. `last_event_ts` keeps its old wire
    # value (0 when empty): broker_client.py reads it and every new key here is
    # additive, so no consumer breaks.
    log_empty = False
    # db_path is OBSERVED from the connection that actually served this probe, never
    # re-derived from config. `PRAGMA database_list` reports the file THIS connection has
    # attached as `main`, so it cannot disagree with the database the query above just
    # read. Re-deriving it from BROKER_DB_PATH answers a different question -- "what would
    # a NEW connection open?" -- and the two diverge for any relative path once the process
    # no longer has the cwd it started in. `os.path.abspath` is CORRECT today (os.chdir
    # count in this repo is 0 and the launcher pins cwd), so this is a provenance
    # improvement, not a defect fix: it observes instead of recomputing.
    db_path_observed: Optional[str] = None
    try:
        async with connect_db() as conn:
            cur = await conn.execute("SELECT MAX(ts) AS m FROM message_log")
            row = await cur.fetchone()
            await cur.close()
            raw_max_ts = row["m"]
            log_empty = raw_max_ts is None
            last_event_ts = raw_max_ts or 0
            # An in-memory or temporary database reports an EMPTY `file` column. Treat
            # that as "nothing observed" and fall through to the configured path rather
            # than reporting "" as if it were a location.
            cur = await conn.execute("PRAGMA database_list")
            for dbrow in await cur.fetchall():
                if dbrow["name"] == "main" and dbrow["file"]:
                    db_path_observed = os.path.abspath(dbrow["file"])
                    break
            await cur.close()
            omp_relay_contract = (
                1 if await _omp_relay_contract_available(conn) else 0
            )
    except Exception as e:
        logger.warning("health: sqlite probe failed: %s", e)
        sqlite_ok = False

    # Health reports component faults, never traffic age or an empty log. Keep
    # last_event_ts/log_empty as observations for callers choosing a staleness
    # threshold. db_path identifies the observed database, but the caller must
    # compare it with the intended root before inferring a wrong-tree install.
    # These probes are read-only: sqlite_ok does not prove that writes succeed.

    # M2 D16 § (d).3 persistent-open escalation: if any channel has hit the
    # persistent-open threshold, surface as 'degraded' + per-channel circuit
    # state in checks dict. Read metrics defensively (poller may be absent).
    discord_circuits: dict[str, bool] = {}
    persistent_open_any = False
    discord_metrics_ok = True
    try:
        from broker import discord_poller as _dp
        circuits = _dp.broker_metrics.get("discord_rate_limit_circuit_open", {})
        consecutive = _dp.broker_metrics.get("discord_consecutive_capped_failures", {})
        for ch_id, is_open in circuits.items():
            if is_open:
                discord_circuits[ch_id] = True
                if consecutive.get(ch_id, 0) >= _dp.PERSISTENT_OPEN_THRESHOLD:
                    persistent_open_any = True
    except ImportError:
        # The poller is legitimately absent on most boxes -- absent is not a
        # failed check, and reporting it as one would flag every machine.
        pass
    except Exception as e:
        # A probe that RAISED is not a probe that PASSED. The bare `except: pass`
        # this replaces made the two indistinguishable, which is the same class as
        # the empty-log conflation above. Surfaced in checks; deliberately does NOT
        # flip `overall`, because an unreadable metrics dict is not a broker fault.
        discord_metrics_ok = False
        logger.warning("health: discord metrics probe failed: %s", e)

    overall = "ok" if (sqlite_ok and not persistent_open_any) else "degraded"
    # db_path: the database this process actually opened. The broker CANNOT detect that
    # it is the wrong installation -- a second tree has the same filename and the same
    # `BROKER_DB_PATH=./coordination.db`, so it is self-consistent and indistinguishable
    # from inside. Reporting the path does not prevent that; it makes it visible in one
    # glance to a human or to fleet tooling that knows the intended root (the autostart
    # task's own Action).
    checks: dict[str, Any] = {
        "sqlite": sqlite_ok,
        "last_event_ts": last_event_ts,
        "log_empty": log_empty,
        "omp_relay_contract": omp_relay_contract,
        # Observed from the live connection when there was one. The fallback is kept
        # deliberately for the `sqlite_ok=False` arm: there is no connection to
        # interrogate there, and that is precisely the arm where an operator most needs
        # to know WHICH path was attempted. A blank field would answer nothing.
        "db_path": db_path_observed or os.path.abspath(_env_str("BROKER_DB_PATH", "coordination.db")),
        "db_path_observed": db_path_observed is not None,
        "discord_metrics_readable": discord_metrics_ok,
    }
    # Option A (D80): authoritative #18 reaper state. app.state.reaper_task is a Task
    # when the reaper was spawned (BROKER_REAPER_ENABLED=true at lifespan startup), None
    # otherwise (disabled / shutdown / module init); .done() guards a completed/cancelled/
    # crashed task. Provenance-correct BY CONSTRUCTION: only the process that bound :8787
    # can answer this request, so a failed-bind child can never be mistaken for the live
    # broker. Additive JSON field -> no contract break. enable-reaper.ps1 reads
    # checks.reaper_running as the sole reaper-state signal (no log/PID inference).
    _reaper_task = getattr(app.state, "reaper_task", None)
    checks["reaper_running"] = _reaper_task is not None and not _reaper_task.done()
    # reaper_task_present: additive diagnostic (True iff a reaper Task object exists, even if done/crashed)
    # distinguishing "never spawned (disabled at startup)" from "spawned but since exited/crashed". NOTE: the
    # D80-era enable-reaper.ps1 "reconcile" that consumed this signal was DELETED (D81 -- no DB signal can prove
    # a row safe to stamp), so nothing reads this field for reconcile anymore; retained as a harmless diagnostic.
    checks["reaper_task_present"] = _reaper_task is not None
    # ATTACHED PEERS -- how many live SSE subscriptions this broker is currently holding
    # open, i.e. how many peer sessions have a push channel attached right now. It exists
    # for installer restart deferral: a nonzero count proves a peer is attached.
    #
    # THE LIMIT, stated because the name invites over-reading. A subscriber is a live PUSH
    # channel. A peer that reaches this broker ONLY through the per-fire receive hook holds
    # no subscription and is INVISIBLE here. So a non-zero count PROVES someone is attached;
    # a zero count does NOT prove nobody is working. Anything deciding "this box is idle"
    # must also weigh `last_event_ts`, which this same response already carries. Do not
    # write a caller that treats 0 as proof of absence -- that is D100 rule 8, and this
    # field is exactly the shape that invites it.
    #
    # NOT A FAULT, and it must NEVER drive `status` (D189). Attachment is a fact about
    # traffic; `status` reports failing components only. The whole point of D189 was to stop
    # this endpoint inferring a fault from a traffic observation, and adding a peer count to
    # `overall` would re-create that defect with a new field.
    #
    # READ WITHOUT `_subs_lock` DELIBERATELY. `len()` on a list is atomic under the GIL, the
    # value is advisory, and taking the lock here would couple every health probe to
    # subscriber churn. The count can be stale by microseconds; nothing here needs better.
    checks["attached_peers"] = len(_subscribers)
    if discord_circuits:
        checks["discord_rate_limit_circuit_open"] = discord_circuits
    # F-CC27: surface request counters + event-loop scheduled-callback depth.
    # active=0 in steady state; a freeze of class H1 (handler-stack deadlock)
    # would show active > 0 + total-completed gap = active. Loop-scheduled
    # depth is asyncio's pending-callback count — a rising number while
    # broker is otherwise healthy hints at backpressure on the event loop.
    checks["requests"] = {
        "active": _request_active,
        "total": _request_total,
        "completed": _request_completed,
    }
    try:
        loop = asyncio.get_running_loop()
        # _scheduled is asyncio-private but stable across CPython 3.10+; used
        # for diagnostics only. If absent on a non-CPython runtime, fall back
        # to None silently.
        checks["loop_scheduled_depth"] = len(getattr(loop, "_scheduled", []))
    except Exception:
        checks["loop_scheduled_depth"] = None
    return {"status": overall, "checks": checks}


# ---- GET /kanban.html (RETIRED per OQ-DASH-1 -> redirects to /dashboard) ----
@app.get("/kanban.html")
async def get_kanban():
    """Legacy board RETIRED (OQ-DASH-1, 2026-06-08): /dashboard is the canonical
    board. Redirect existing bookmarks to /dashboard rather than serving the old
    kanban.html (the file stays on disk but is no longer served). 307 = temporary,
    method-preserving, not permanently browser-cached (reversible if ever needed).
    """
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/dashboard", status_code=307)


# -------------------------------------------------------------------- Entry point

def _selector_loop_factory() -> asyncio.AbstractEventLoop:
    """Return a SelectorEventLoop on Windows; default on Linux.

    F-CC22 (Stage C fix, 2026-05-27): on Windows we must avoid asyncio's
    ProactorEventLoop because `IocpProactor.accept_coro` raises an unhandled
    OSError(22, ..., WinError 64 ERROR_NETNAME_DELETED) on transient
    socket-level events (peer disconnect during accept, network adapter
    state change, loopback edge cases). uvicorn does not catch this; the
    accept loop dies; HTTP listener silently stops accepting; broker process
    otherwise healthy.

    SelectorEventLoop uses select() syscall — same code path as Linux. The
    accept_coro bug doesn't exist on this path. Trade-off: lower max
    concurrent connections than Proactor. Broker does not use subprocesses on
    Windows, so SelectorEventLoop's subprocess limitation does not apply.

    Passed to uvicorn via `loop="broker.server:_selector_loop_factory"` in main().
    """
    if sys.platform == "win32":
        return asyncio.SelectorEventLoop()
    return asyncio.new_event_loop()  # default on Linux/macOS


# Enabled escalation requires both destination and token in the process
# environment. No hardcoded channel or plugin credential fallback is used here.
_REAPER_REQUIRED_ENV = ("DISCORD_BOT_TOKEN", "BROKER_ALERT_DISCORD_CHANNEL_ID")


def _verify_reaper_config_or_die() -> None:
    """D82 refuse-to-start precondition (called first in main(), before the server binds).

    If BROKER_REAPER_ENABLED is true but the reaper's Discord config is missing/blank in the
    environment, print a FATAL naming the missing var(s) to stderr and exit non-zero BEFORE
    binding rather than starting a silently mute escalation task. Disabled
    escalation needs no Discord configuration. Blank values count as missing.
    """
    if not _env_bool("BROKER_REAPER_ENABLED", False):
        return
    missing = [k for k in _REAPER_REQUIRED_ENV if not (os.environ.get(k) or "").strip()]
    if missing:
        sys.stderr.write(
            "FATAL: BROKER_REAPER_ENABLED=true but the reaper's Discord config is "
            "missing/blank in the environment: " + ", ".join(missing) + ". The broker "
            "refuses to start. Set these in the broker's process environment "
            "or set BROKER_REAPER_ENABLED=false.\n"
        )
        sys.stderr.flush()
        raise SystemExit(78)  # EX_CONFIG (sysexits.h): configuration error


def main() -> None:
    import uvicorn
    _verify_reaper_config_or_die()   # D82: fail loud BEFORE bind if reaper enabled + Discord config absent
    host = _env_str("BROKER_HOST", "0.0.0.0")
    port = _env_int("BROKER_PORT", 8787)
    # F-CC22: loop="broker.server:_selector_loop_factory" forces SelectorEventLoop on
    # Windows to avoid IocpProactor.accept_coro OSError 64 NETNAME_DELETED bug.
    uvicorn.run("broker.server:app", host=host, port=port, log_level="info",
                loop="broker.server:_selector_loop_factory")


if __name__ == "__main__":
    main()
