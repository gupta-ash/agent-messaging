"""Broker-side queue ownership and null-recipient compatibility regressions.

Stable delivery.recipient_id is authoritative when a canonical relay name or
exact registered ID has an owner. Rename follows that owner; deleting it and
claiming its name from another identity does not transfer its mail. Historical
null-recipient rows remain readable by name for compatibility.

Behavioral tests constrain recipient scoping, pagination, ordering and every
directed kind. A schema-only guard cannot catch read-time fragmentation, and
timestamps/client IDs must not accidentally sort in event-id order. Negative
controls distinguish another recipient and non-message traffic. The mutation
harness declares its expected detections rather than relying on a prose table.

These tests do not exercise retention sweeps (covered separately) or the
client-side confirmed-set migration in test_c37_seed_migration.py. Run with
pytest; tests/mutation_proof_c37.py is the separate mutation exercise.
"""

from __future__ import annotations

import ast
import os
import re
import sqlite3
import time
from pathlib import Path

import pytest

# Use the repository-wide module-level asyncio marker convention.
pytestmark = pytest.mark.asyncio

SERVER_PY = Path(__file__).resolve().parent.parent / "src/broker/server.py"

NAME = "t_c37_probe"
OTHER = "t_c37_bystander"
# A third recipient whose messages are INTERLEAVED between ours, so our
# event_ids are non-contiguous. Without interleaving, a regression that pages
# globally first and filters by recipient second is invisible: the global page
# would contain only our rows anyway. With it, other recipients' rows consume
# the page and the queue truncates mid-history.
NOISE = "t_c37_noise"
SID_A = "aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa"
SID_B = "bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb"
SID_C = "cccccccc-3333-4333-8333-cccccccccccc"

# Three eras of two messages each. The recipient NAME never changes; the sid
# holding it does, and every prior holder is DELETED (C-37: "delete all prior
# sessions with that name"), which is the state the queue must survive.
ERAS = ((SID_A, 2), (SID_B, 2), (SID_C, 2))
TOTAL = sum(n for _, n in ERAS)
ERA_SPAN_MS = 10_000

# Anchored to NOW, not to a fixed past date. A frozen literal here was ~455
# days old, i.e. every seeded row sat far beyond the 30-day retention cutoff --
# harmless while conftest skips lifespan tasks, but a landmine for any future
# retention-enabled harness, which would silently delete its own fixture.
BASE_TS = int(time.time() * 1000) - 60_000

# NEITHER ASCENDING NOR DESCENDING -- both degeneracies must be excluded.
#   ts ascending  => a build sorting by `ts` is indistinguishable from one
#                    sorting by `event_id`   (missed ORDER BY ts ASC)
#   ts descending => a build sorting by `ts DESC` returns exactly event_id
#                    order and is equally invisible (missed ORDER BY ts DESC)
# The first revision of this file fixed only the former and shipped the latter.
# Within each era the two messages are swapped; eras advance by ERA_SPAN_MS, so
# every message still POSTDATES its own era's registration (see _seed_three_eras)
# -- which is what makes a registered_at-scoped regression observable.
TS_BY_SEQ = [BASE_TS + o for o in (2000, 1000, 12000, 11000, 22000, 21000)]

# SCRAMBLED, and for the same reason. Minting `client_event_id` and `content`
# in event_id order made `ORDER BY client_event_id` (and `ORDER BY content`)
# indistinguishable from `ORDER BY event_id`. That is the THIRD ordering
# regression to hide behind a co-sorting column -- after ts ASC and ts DESC --
# so the ordering test now asserts non-degeneracy for EVERY distinct-valued
# column returned, instead of this being patched one column at a time.
LABEL_BY_SEQ = ("d", "f", "b", "a", "e", "c")

# The exact column set measured at schema.sql:33-43. Pinned as a literal so an
# ADDITION trips the guard, not just a removal.
EXPECTED_COLUMNS = [
    "event_id", "client_event_id", "from_agent", "to_agent",
    "channel", "thread_id", "kind", "content", "ts",
]

# Tokens that would indicate a recipient-session key has been (re-)introduced.
SESSION_TOKEN_RE = re.compile(r"session|(?<![a-z])sid(?![a-z])", re.IGNORECASE)

# Every directed kind must remain inspectable. Cover all members rather than
# one representative so an incomplete read-path allow-list cannot hide history.
DIRECTED_KINDS = ("agent_message", "agent_reply", "handoff")


def _db() -> sqlite3.Connection:
    """Open the test DB the `client` fixture created. Short-lived by design:
    the server holds an aiosqlite pool against the same file."""
    conn = sqlite3.connect(os.environ["BROKER_DB_PATH"], timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def _hold_name(sid: str, reg_ts: int = BASE_TS) -> None:
    """Register `sid` as the current holder of NAME, and delete every prior
    holder -- the C-37 reclaim, modelled on the real agent_status shape.

    `reg_ts` is per-era on purpose. Giving all three eras one registration
    timestamp made them temporally indistinguishable, which hid a regression
    that scopes the inbox to `ts >= (the current holder's registered_at)` --
    the era-boundary fragmentation C-37(c) exists to forbid.
    """
    conn = _db()
    try:
        conn.execute("DELETE FROM agent_status WHERE display_name = ?", (NAME,))
        conn.execute(
            "INSERT OR REPLACE INTO agent_status "
            "(id, kind, channel, status, last_activity_ts, registered_at, display_name) "
            "VALUES (?, 'session', '#cc-session-relay', 'idle', ?, ?, ?)",
            (sid, reg_ts, reg_ts, NAME),
        )
        conn.commit()
    finally:
        conn.close()


def _drop_all_holders() -> int:
    """Delete every agent_status row for NAME -- the name is now held by NOBODY.
    Returns the count deleted so the caller can assert it was non-empty."""
    conn = _db()
    try:
        cur = conn.execute("DELETE FROM agent_status WHERE display_name = ?", (NAME,))
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


async def _post(client, to: str, content: str, cei: str, ts: int,
                kind: str = "agent_message"):
    r = await client.post("/api/event", json={
        "from_agent": "t_c37_sender", "to_agent": to, "channel": "#cc-session-relay",
        "kind": kind, "content": content, "ts": ts, "client_event_id": cei,
    })
    assert r.status_code == 200, r.text
    return r.json()["event_id"]


async def _seed_three_eras(client) -> list[int]:
    """Post TOTAL messages to NAME across three sid eras, deleting each prior
    holder at the boundary. Returns event_ids in post order.

    Each era registers its holder at its own timestamp and posts messages that
    POSTDATE that registration; timestamps are non-monotonic WITHIN an era.
    Both properties are load-bearing -- see TS_BY_SEQ and _hold_name.
    """
    ids, seq = [], 0
    for era_i, (sid, count) in enumerate(ERAS):
        _hold_name(sid, BASE_TS + era_i * ERA_SPAN_MS)
        for _ in range(count):
            ids.append(await _post(
                client, NAME, f"era-{LABEL_BY_SEQ[seq]}-{sid[:8]}",
                f"c37-{LABEL_BY_SEQ[seq]}", TS_BY_SEQ[seq]))
            # interleave a foreign recipient so our event_ids are NOT contiguous
            await _post(client, NOISE, f"noise-{seq}", f"c37-noise-{seq}",
                        TS_BY_SEQ[seq] + 100)
            seq += 1
    # The fixture is only meaningful if the candidate sort keys DISAGREE, in
    # BOTH directions. Either degeneracy alone hides a real regression.
    assert ids == sorted(ids), "event_ids are not ascending -- fixture broken"
    assert ids != list(range(ids[0], ids[0] + TOTAL)), (
        "VACUOUS FIXTURE: our event_ids are contiguous, so a page-globally-"
        "then-filter regression cannot be distinguished. Interleaving broke.")
    assert TS_BY_SEQ != sorted(TS_BY_SEQ), (
        "VACUOUS FIXTURE: ts is ascending, so a build sorting by ts is "
        "indistinguishable from one sorting by event_id.")
    assert TS_BY_SEQ != sorted(TS_BY_SEQ, reverse=True), (
        "VACUOUS FIXTURE: ts is descending, so a build sorting by ts DESC "
        "returns exactly event_id order and is equally invisible.")
    return ids


# --------------------------------------------------------------------------
# 1. The behavioural criterion.
# --------------------------------------------------------------------------

async def test_queue_is_continuous_after_every_holder_is_deleted(client):
    """(c) stated directly: successive sids still read as ONE queue.

    This fixture uses THREE eras (see ERAS); C-37's wording is "5 sids", which
    is illustrative of "any number", not a count this file reproduces. An
    earlier revision of this docstring said "5 sids" and would have let an
    auditor credit two eras that do not exist.

    The final `_drop_all_holders()` is the sharp part for holder-resolution
    regressions -- the name is held by NOBODY when the inbox is read, so
    nothing the endpoint could resolve through `agent_status` exists any more.
    It is the BLUNT part for era-scoping regressions, which key on a holder
    that this test has just removed; those are caught by the pagination and
    ordering tests, which deliberately leave a holder registered.

    Measured reach: reddens on M1, M3, M4, M5, M6, M8 -- six arms, and not a
    sole detector for any of them (see the module docstring). Pagination
    reaches seven, so this is not the widest clause either; an earlier
    revision of this docstring claimed it was.
    """
    posted = await _seed_three_eras(client)
    assert len(posted) == TOTAL, "fixture did not post what it claims"

    deleted = _drop_all_holders()
    assert deleted == 1, f"expected exactly the last holder to remain, deleted {deleted}"

    r = await client.get(f"/api/agent/{NAME}/messages?since=0&limit=100")
    assert r.status_code == 200, r.text
    got = [m["event_id"] for m in r.json()["messages"]]

    assert got == posted, (
        "C-37(c) VIOLATED: a name's queue fragmented across sid eras.\n"
        f"  posted   {posted}\n  returned {got}"
    )
    assert got == sorted(got), "queue is not in event_id order -- not inspectable"


async def test_r1_outstanding_stable_owner_survives_delete_until_resolution(client):
    first = await client.post(f"/api/agent/{SID_A}/status", json={
        "kind": "cc-session", "channel": "#cc-relay", "status": "idle",
        "last_activity_ts": BASE_TS, "display_name": NAME,
    })
    assert first.status_code == 200, first.text
    event_id = await _post(client, NAME, "stable-first", "c37-stable-first", BASE_TS + 1)
    conn = _db()
    try:
        recipient_id = conn.execute(
            "SELECT recipient_id FROM delivery WHERE event_id=?", (event_id,)
        ).fetchone()[0]
        blocked_delete = conn.execute(
            "DELETE FROM agent_status WHERE id=?", (SID_A,)
        ).rowcount
        conn.commit()
    finally:
        conn.close()
    assert recipient_id == SID_A
    assert blocked_delete == 0

    inbox = await client.get(f"/api/agent/{NAME}/messages")
    assert [row["event_id"] for row in inbox.json()["messages"]] == [event_id]
    acked = await client.post(
        f"/api/agent/{SID_A}/delivery-ack", json={"event_ids": [event_id]}
    )
    assert acked.json()["acked_now"] == 1

    conn = _db()
    try:
        resolved_delete = conn.execute(
            "DELETE FROM agent_status WHERE id=?", (SID_A,)
        ).rowcount
        rehomed = conn.execute(
            "SELECT d.to_agent,m.to_agent FROM delivery d "
            "JOIN message_log m ON m.event_id=d.event_id WHERE d.event_id=?",
            (event_id,),
        ).fetchone()
        conn.commit()
    finally:
        conn.close()
    assert resolved_delete == 1
    assert tuple(rehomed) == (SID_A, SID_A)

    second = await client.post(f"/api/agent/{SID_B}/status", json={
        "kind": "cc-session", "channel": "#cc-relay", "status": "idle",
        "last_activity_ts": BASE_TS + 2, "display_name": NAME,
    })
    assert second.status_code == 200, second.text
    second_status = await client.get(f"/api/agent/{SID_B}/status")
    assert second_status.json()["relay_name"] == NAME
    inherited = await client.get(f"/api/agent/{NAME}/messages")
    assert inherited.status_code == 200
    assert inherited.json()["messages"] == []


async def test_full_history_survives_pagination(client):
    """"Inspectable" includes "walkable". A paginated read must reach the same
    set as a single wide read -- no dupes, no gaps."""
    posted = await _seed_three_eras(client)

    walked, cursor, hops = [], 0, 0
    while True:
        hops += 1
        assert hops <= TOTAL + 2, "pagination did not terminate"
        r = await client.get(f"/api/agent/{NAME}/messages?since={cursor}&limit=2")
        assert r.status_code == 200, r.text
        body = r.json()
        walked.extend(m["event_id"] for m in body["messages"])
        if body["next_cursor"] is None:
            break
        cursor = body["next_cursor"]

    assert walked == posted, f"paginated walk lost history: {walked} != {posted}"
    assert len(set(walked)) == len(walked), "pagination returned duplicates"


# --------------------------------------------------------------------------
# 2. The shape guards -- the part that actually holds the line.
# --------------------------------------------------------------------------

async def test_message_log_has_no_recipient_session_column(client):
    """If this fails, someone added a session column to `message_log`. That is
    ONE way to regress C-37(c) and the only one this guard can see -- it is NOT
    the only way, and an earlier revision of this docstring claimed it was.
    Measured counter-examples that change no schema at all: M1, M4, M5, M6, M7
    and R1 in tests/mutation_proof_c37.py. A schema change here must be a
    deliberate, re-argued decision -- not a silent edit.
    """
    conn = _db()
    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(message_log)").fetchall()]
    finally:
        conn.close()

    assert cols, "VACUOUS: message_log has no columns -- schema did not bootstrap"
    offenders = [c for c in cols if SESSION_TOKEN_RE.search(c)]
    assert not offenders, (
        f"C-37(c) AT RISK: message_log gained session-shaped column(s) {offenders}. "
        "A recipient keyed by session fragments its own history. Re-read "
        "CONSTRAINTS.md#C-37 before proceeding."
    )
    assert cols == EXPECTED_COLUMNS, (
        f"message_log columns changed: {cols} != {EXPECTED_COLUMNS}. Not necessarily "
        "wrong -- but confirm the new column is not a recipient key, then update "
        "EXPECTED_COLUMNS in this file."
    )




# --------------------------------------------------------------------------
# 3. Anti-vacuity: these must FIRE, or nothing above means anything.
# --------------------------------------------------------------------------

def _server_delivery_kinds() -> tuple[str, ...]:
    """Read server.py's DELIVERY_KINDS by parsing the declaration.

    DIRECTED_KINDS is a hand-maintained mirror, and an unenforced mirror of an
    upstream constant is the SAME failure shape as arm X1 one level up: add a
    fourth directed kind to server.py and it would silently fall out of
    coverage here, exactly as `agent_reply` did. So the mirror is checked
    rather than trusted.
    """
    tree = ast.parse(SERVER_PY.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and node.targets[0].id == "DELIVERY_KINDS"):
            v = node.value
            assert isinstance(v, (ast.Set, ast.Tuple, ast.List)), (
                f"DELIVERY_KINDS is no longer a literal collection "
                f"({type(v).__name__}); this guard can no longer read it and "
                "must fail loudly rather than assume the mirror is intact.")
            vals = [e.value for e in v.elts
                    if isinstance(e, ast.Constant) and isinstance(e.value, str)]
            assert len(vals) == len(v.elts), "DELIVERY_KINDS has non-literal members"
            return tuple(sorted(vals))
    raise AssertionError("DELIVERY_KINDS not found in server.py -- renamed?")


async def test_negative_controls_fire(client):
    """Prove the inbox is filtering at all, and that EVERY directed kind
    stays readable.

    The non-default kinds are asserted POSITIVELY as well as negatively, and
    ALL of them are, not one. An earlier revision asserted only that
    `kind=handoff` returns [] -- which an endpoint returning NOTHING for any
    non-default kind also satisfies (arm R4). Fixing that by adding a single
    positive `handoff` case was still not enough: an ALLOWLIST omitting one
    kind (arm X1, modelled on the real `kind IN (...)` idiom already in
    server.py) hid `agent_reply` -- the dominant directed kind live -- while
    every test stayed green. C-37(c) says the history must be inspectable, and
    a kind nobody can read back is not inspectable. SOLE detector for R4 and X1.

    (An earlier docstring here said these assertions would otherwise be
    satisfied by "an endpoint that returns everything to everyone". No longer
    true: `_seed_three_eras` interleaves NOISE, so a return-everything endpoint
    fails the continuity assertions directly. A later one claimed this test
    proved "every kind stays readable" when it covered only `handoff`.)
    """
    await _seed_three_eras(client)
    await _post(client, OTHER, "not-for-us", "c37-bystander", BASE_TS + 900)

    # The mirror must still match its upstream, or the loop below silently
    # stops covering a kind that the broker still treats as directed.
    assert tuple(sorted(DIRECTED_KINDS)) == _server_delivery_kinds(), (
        f"DIRECTED_KINDS {tuple(sorted(DIRECTED_KINDS))} has drifted from "
        f"server.py's DELIVERY_KINDS {_server_delivery_kinds()}. Any kind in "
        "the latter and not the former is uncovered here.")

    # One row per NON-DEFAULT directed kind, each of which must read back.
    extra = {}
    for i, k in enumerate(k for k in DIRECTED_KINDS if k != "agent_message"):
        extra[k] = await _post(client, NAME, f"a-{k}", f"c37-{k}",
                               BASE_TS + 30_000 + i * 1000, kind=k)
    assert extra, "VACUOUS: no non-default directed kinds were seeded"

    r = await client.get(f"/api/agent/{NAME}/messages?since=0&limit=100")
    ours = [m["content"] for m in r.json()["messages"]]
    assert "not-for-us" not in ours, "inbox leaks another recipient's mail"
    for k in extra:
        assert f"a-{k}" not in ours, f"default kind filter is not applied ({k})"

    r = await client.get(f"/api/agent/{OTHER}/messages?since=0&limit=100")
    assert len(r.json()["messages"]) == 1, "bystander control did not fire"

    # POSITIVE, for every NON-DEFAULT directed kind: absent-from-the-default-
    # view is not the same as readable, and an allowlist that drops one kind
    # fails here. (`agent_message` is the endpoint default and so cannot be in
    # this loop -- it legitimately appears in the default view; it is covered
    # by the continuity and bystander assertions instead.)
    for k, eid in extra.items():
        r = await client.get(
            f"/api/agent/{NAME}/messages?since=0&kind={k}&limit=100")
        got = [m["event_id"] for m in r.json()["messages"]]
        assert got == [eid], (
            f"directed kind `{k}` is not inspectable: asked for it, got {got}. "
            f"Its history is unreachable through the inbox.")

    r = await client.get(f"/api/agent/{NAME}/messages?since=0&kind=status&limit=100")
    assert r.json()["messages"] == [], "kind filter is not applied"

    r = await client.get(f"/api/agent/{NAME}/messages?since=999999999&limit=100")
    assert r.json()["messages"] == [], "since cursor is not applied"


async def test_inbox_orders_by_event_id_not_timestamp(client):
    """The sort key must be `event_id`, the same column the cursor advances on.

    Sorting by anything else while paging on `event_id` loses rows
    PERMANENTLY: once a page's last row carries a high event_id, every lower
    id is skipped for good.

    THE ANTI-DEGENERACY CHECK IS GENERALISED, because patching it per column
    failed twice. Three regressions -- ORDER BY ts ASC, ts DESC, and
    client_event_id -- were each invisible for the same reason: some other
    column happened to sort the fixture into event_id order. So rather than
    assert "ts is not ascending" and wait for the next column, this requires
    that NO distinct-valued column reproduces event_id order in either
    direction. Tied columns (from_agent, to_agent, channel, kind, thread_id
    here) are skipped: equal values cannot define a total order, so they are
    not alternative sort keys.
    """
    posted = await _seed_three_eras(client)

    r = await client.get(f"/api/agent/{NAME}/messages?since=0&limit=100")
    msgs = r.json()["messages"]
    assert [m["event_id"] for m in msgs] == posted, (
        f"inbox is not ordered by event_id: {[m['event_id'] for m in msgs]}")

    by_id = {m["event_id"]: m for m in msgs}
    ordered = [by_id[i] for i in posted]
    checked = []
    for col in sorted(ordered[0]):
        if col == "event_id":
            continue
        vals = [m[col] for m in ordered]
        if any(v is None for v in vals) or len(set(vals)) != len(vals):
            continue  # ties / NULLs cannot define a total order
        checked.append(col)
        assert sorted(vals) != vals, (
            f"VACUOUS FIXTURE: `{col}` ascends with event_id, so a build "
            f"ordering by `{col}` is indistinguishable from a correct one.")
        assert sorted(vals, reverse=True) != vals, (
            f"VACUOUS FIXTURE: `{col}` descends as event_id ascends, so a "
            f"build ordering by `{col} DESC` returns exactly event_id order.")
    assert {"ts", "client_event_id", "content"} <= set(checked), (
        f"the three columns known to have hidden a regression must all be "
        f"distinct-valued and checked; only got {checked}")

