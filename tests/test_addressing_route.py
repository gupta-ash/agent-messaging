"""Phase 1 tracer: the `alpha@localhost` slice, through the REAL POST /api/event route.

Covers:
- the tracer's own parser gate: `alpha@localhost` / `alpha@LOCALHOST` / `alpha@LocalHost`
  all resolve `local` with the key `alpha`; a bare relay name is canonicalized;
  a non-str recipient is `passthrough`
- POST /api/event with to_agent=`alpha@localhost` returns 200 and files BOTH the
  `message_log` row and the `delivery` row under the bare key `alpha`
- the message is readable at GET /api/agent/alpha/messages and ackable at
  POST /api/agent/alpha/ack -- the mailbox a bare sender already uses, not a new one
- GET /api/agent/alpha%40localhost/messages is EMPTY. The inbox path param is NOT
  parsed (OQ-5), so the qualified spelling never becomes a second mailbox. That
  emptiness is the behavioural proof the qualifier was stripped before the write.
- every address the grammar cannot parse unambiguously is refused with 422 and leaves
  ZERO message_log rows and ZERO delivery rows behind it
- a refusal body carries the {"error": {code, message, request_id}} envelope and does
  NOT contain the sender-supplied address anywhere in it
- the compatibility floor: a bare name with a dot / underscore / hyphen / digit still
  returns 200, while accepted relay names are ASCII-folded to lowercase; an
  empty-string recipient on a non-directed kind still returns 200 and still creates no
  delivery row

The exhaustive accept/reject table (TEST-01) lives in `tests/test_addressing_grammar.py`
and is owned by plan 01-03. The four parser assertions here are deliberately only the
ones this tracer itself binds, so this file fails on its own if the seam moves.

ADDR-03: the only narrowing this phase applies to the bare-name path is ADDR-09's
wildcard refusal. Do NOT add character-class validation to the local part "for
consistency" -- the local part accepts exactly what the broker accepts today, which is
`EventBody`'s 256-character bound and no character class at all.
"""

from __future__ import annotations

import os

import pytest

from broker import addressing

pytestmark = pytest.mark.asyncio


async def _delivery_rows(to_agent=None):
    import aiosqlite

    async with aiosqlite.connect(os.environ["BROKER_DB_PATH"]) as conn:
        conn.row_factory = aiosqlite.Row
        if to_agent is None:
            cur = await conn.execute("SELECT * FROM delivery ORDER BY event_id")
        else:
            cur = await conn.execute(
                "SELECT * FROM delivery WHERE to_agent = ? ORDER BY event_id", (to_agent,)
            )
        rows = await cur.fetchall()
        await cur.close()
        return [dict(r) for r in rows]


async def _message_log_rows(event_id=None):
    import aiosqlite

    async with aiosqlite.connect(os.environ["BROKER_DB_PATH"]) as conn:
        conn.row_factory = aiosqlite.Row
        if event_id is None:
            cur = await conn.execute("SELECT * FROM message_log ORDER BY event_id")
        else:
            cur = await conn.execute(
                "SELECT * FROM message_log WHERE event_id = ? ORDER BY event_id", (event_id,)
            )
        rows = await cur.fetchall()
        await cur.close()
        return [dict(r) for r in rows]


# ------------------------------------------------------------ the tracer's parser gate

async def test_the_tracer_parser_gate():
    """The four parse_recipient answers the round-trip below actually binds.

    Declared `async` only because this module carries a module-level
    `pytestmark = pytest.mark.asyncio` (the house idiom for its integration tests);
    the parser is pure and awaits nothing.
    """
    # MACH-05 + ADDR-05a: the machine part is folded, the local part is not.
    for qualified in ("alpha@localhost", "alpha@LOCALHOST", "alpha@LocalHost"):
        route = addressing.parse_recipient(qualified)
        assert route.disposition == "local", qualified
        assert route.key == "alpha", qualified

    # R1: an accepted relay name is canonicalized with ASCII A-Z folding.
    bare = addressing.parse_recipient("AlPhA")
    assert bare.disposition == "local"
    assert bare.key == "alpha"

    # Today's broadcast behaviour, untouched.
    absent = addressing.parse_recipient(None)
    assert absent.disposition == "passthrough"
    assert absent.key is None


async def test_empty_name_uuid_is_refused():
    """REGRESSION, cross-family review 2026-08-15 (HIGH). `@<uuid>` must be refused.

    THE DEFECT THIS PINS, stated so nobody re-introduces it by tidying: step 4 answers
    the `<name>@<uuid>` form BEFORE step 7 checks that both parts are non-empty. Without
    a non-empty-name guard IN step 4, `@<uuid>` matched there and returned `passthrough`
    -- HTTP 200, and the malformed key written to all three write sites under a recipient
    no consumer polls. Step 7 could not catch it because step 4 returns first.

    MEASURED before the fix: `parse_recipient("@<uuid>")` returned
    `Route("passthrough", "@<uuid>", None)`.

    THE TWO POSITIVE CONTROLS BELOW ARE THE POINT OF THIS TEST. The obvious over-fix --
    moving the empty-part check above step 4, or dropping the UUID branch -- would make
    the first assertion pass while BREAKING session-id delivery, which works today and is
    a live key-space. A test that only asserted the refusal could not tell the fix from
    that regression.
    """
    uuid = "123e4567-e89b-12d3-a456-426614174000"

    refused = addressing.parse_recipient("@" + uuid)
    assert refused.disposition == "refused", (
        "`@<uuid>` must not be routed: an empty name part violates ADDR-01"
    )
    assert refused.reason == "empty_address_part", refused.reason
    assert refused.key is None, "a refusal must carry no key"

    # CONTROL 1: the qualified session-id form still passes through untouched.
    named = addressing.parse_recipient("alpha@" + uuid)
    assert named.disposition == "passthrough", "name@uuid delivery is live and must not break"
    assert named.key == "alpha@" + uuid, "the key must be byte-identical"

    # CONTROL 2: a raw session id still passes through untouched.
    raw = addressing.parse_recipient(uuid)
    assert raw.disposition == "passthrough", "a raw session id is a live key-space"
    assert raw.key == uuid


# ------------------------------------------------------------------- the round trip

async def test_qualified_localhost_lands_in_the_bare_mailbox(client):
    """Criteria 1 + 3: send qualified, then read and ack BARE.

    This is the assertion the whole stored-form constraint exists to make true. The
    MCP plugin's SSE filter is a bare `!=` (plugin server.py:2046), so a key that
    diverges anywhere along this path is not an error -- it is silence, followed an
    hour later by the escalation reaper paging a human.
    """
    r = await client.post("/api/event", json={
        "from_agent": "sender", "to_agent": "alpha@localhost", "channel": "#x",
        "kind": "agent_message", "content": "hi", "ts": 1})
    assert r.status_code == 200, r.text
    eid = r.json()["event_id"]

    # Bind site (2): the delivery row is filed under the BARE key.
    rows = await _delivery_rows()
    assert [x["to_agent"] for x in rows] == ["alpha"]
    assert rows[0]["event_id"] == eid

    # Bind site (1): so is the durable message_log row.
    logged = await _message_log_rows(eid)
    assert len(logged) == 1
    assert logged[0]["to_agent"] == "alpha"

    # The inbox is readable under the BARE name -- the real route, not a parser unit test.
    inbox = await client.get("/api/agent/alpha/messages")
    assert inbox.status_code == 200, inbox.text
    assert [m["event_id"] for m in inbox.json()["messages"]] == [eid]
    assert inbox.json()["messages"][0]["to_agent"] == "alpha"

    # And it acks under the BARE name.
    ack = await client.post("/api/agent/alpha/ack", json={"event_ids": [eid]})
    assert ack.status_code == 200, ack.text


async def test_the_qualified_spelling_is_not_a_second_mailbox(client):
    """OQ-5, proven behaviourally rather than by inspection.

    `GET /api/agent/{agent_id}/messages` compares its path param for exact equality
    and this phase adds NO parsing there, deliberately: a second place that knows the
    grammar is a second place that can disagree with the first. Do not add it "for
    symmetry" -- this empty list is the requirement.
    """
    r = await client.post("/api/event", json={
        "from_agent": "sender", "to_agent": "alpha@localhost", "channel": "#x",
        "kind": "agent_message", "content": "hi", "ts": 1})
    assert r.status_code == 200, r.text

    other = await client.get("/api/agent/alpha%40localhost/messages")
    assert other.status_code == 200, other.text
    assert other.json()["messages"] == []

    # CONTROL: the bare mailbox is non-empty, so the emptiness above is a real
    # discrimination and not a broken request path returning nothing for everyone.
    inbox = await client.get("/api/agent/alpha/messages")
    assert len(inbox.json()["messages"]) == 1


# ------------------------------------------------------- refused, with nothing behind it

# Each row names the requirement it exercises, so a failing row identifies itself.
REFUSED = [
    ("a@b@c", "ADDR-02: two or more separators is ambiguous, counted before any split"),
    ("@box", "ADDR-01: the name part is empty"),
    ("alpha@", "ADDR-01: the machine part is empty"),
    # REGRESSION, cross-family review 2026-08-15 (HIGH, confirmed by measurement before
    # the fix landed). `@<uuid>` is the ONE empty-name spelling the ordering could not
    # catch: step 4's `<name>@<uuid>` branch answers BEFORE step 7's empty-part check, so
    # this returned `passthrough` and wrote a recipient key nothing polls behind an HTTP
    # 200. `@box` above did NOT catch it -- that row takes a different route through the
    # parser -- which is why the two rows are both here and neither replaces the other.
    ("@123e4567-e89b-12d3-a456-426614174000", "ADDR-01: empty name, UUID machine part"),
    ("alpha@box.domain", "ADDR-04: a dotted machine part is not a single LDH label"),
    ("alpha@local.host", "ADDR-04 beats MACH-05: the reservation is a label, not a suffix"),
    ("alpha@*", "ADDR-09: a wildcard in the machine part"),
    ("*", "ADDR-09: a bare wildcard"),
    ("al*pha", "ADDR-09: a wildcard ANYWHERE, including inside a bare name"),
    # PHASE-1 PLACEHOLDER, NOT THE FINISHED BEHAVIOUR. `alpha@workerbox` is well-formed
    # and simply does not resolve, because Phase 1 recognises exactly one machine name.
    # Phase 2 splits THIS ROW into two rows with two distinct reasons (known machine with
    # no transport yet vs unknown machine). Do not read it as the answer.
    ("alpha@workerbox", "Phase-1 placeholder: well-formed, not resolvable"),
]


@pytest.mark.parametrize("to_agent,label", REFUSED)
async def test_an_unparseable_recipient_is_refused_and_leaves_no_row(client, to_agent, label):
    """Criterion 2 + ADDR-08. The ledgers are asserted as ABSOLUTE counts, not deltas.

    The `client` fixture builds a fresh ephemeral database per test, so "zero rows"
    below means the request wrote nothing at all -- which is the property the choke
    point's placement ABOVE `_do()` exists to make true. A parser called inside the
    transaction could only make it true by inspection.
    """
    r = await client.post("/api/event", json={
        "from_agent": "sender", "to_agent": to_agent, "channel": "#x",
        "kind": "agent_message", "content": "hi", "ts": 1})
    assert r.status_code == 422, f"{label}: expected 422, got {r.status_code} -- {r.text}"

    err = r.json().get("error")
    assert err is not None, f"{label}: refusal did not render the standard envelope: {r.text}"
    for field in ("code", "message", "request_id"):
        assert field in err, f"{label}: envelope is missing {field!r}"
    assert err["message"], f"{label}: a refusal must state a reason (ADDR-08)"

    assert await _message_log_rows() == [], f"{label}: a refusal wrote a message_log row"
    assert await _delivery_rows() == [], f"{label}: a refusal wrote a delivery row"


async def test_a_refusal_never_echoes_the_submitted_address(client):
    """The runtime form of the standing rule that a handler must not interpolate
    sender-supplied identifiers into a detail string.

    A recipient is peer-authored and untrusted, and a token-shaped one must not cross
    the response boundary. The address reaches the server-side log only, truncated,
    under the request id.
    """
    sentinel = "sk-live-7c3f9a2b4d1e"
    to_agent = sentinel + "@box.domain"

    r = await client.post("/api/event", json={
        "from_agent": "sender", "to_agent": to_agent, "channel": "#x",
        "kind": "agent_message", "content": "hi", "ts": 1})
    assert r.status_code == 422, r.text

    body = r.text
    assert sentinel not in body, "the refusal echoed the sender-supplied local part"
    assert to_agent not in body, "the refusal echoed the sender-supplied address"

    # CONTROL: the response is non-empty and really is the refusal, so the two
    # absences above are discriminating rather than vacuous.
    assert r.json()["error"]["message"]


# ------------------------------------------------- the compatibility floor (criterion 3)

@pytest.mark.parametrize("bare,stored,label", [
    ("a.b_c-1", "a.b_c-1", "dot, underscore, hyphen and digit are valid relay-name characters"),
    ("MiXeD", "mixed", "R1: valid relay names are ASCII-folded at recipient ingress"),
    ("legacy name", "legacy name", "invalid legacy bare recipients remain byte-identical"),
])
async def test_a_bare_name_uses_the_product_neutral_canonical_mailbox(client, bare, stored, label):
    """R1 canonicalizes relay names without deleting the legacy bare-name path.

    Every sender in the fleet today addresses a bare name. R1 may apply only the shared
    ASCII fold; it must not reject, trim, or otherwise rewrite that legacy path.
    """
    r = await client.post("/api/event", json={
        "from_agent": "sender", "to_agent": bare, "channel": "#x",
        "kind": "agent_message", "content": "hi", "ts": 1})
    assert r.status_code == 200, f"{label}: {r.text}"
    eid = r.json()["event_id"]

    logged = await _message_log_rows(eid)
    assert [x["to_agent"] for x in logged] == [stored], label
    assert [x["to_agent"] for x in await _delivery_rows()] == [stored], label


async def test_an_empty_recipient_on_a_non_directed_kind_is_still_accepted(client):
    """`""` is a legal to_agent for every kind except agent_message, and the delivery
    guard is already falsy for it, so it creates no delivery row today. This phase must
    not start refusing it: ADDR-01's "both parts non-empty" governs the QUALIFIED form
    only, and step 6 of the grammar returns a bare value verbatim without validating it.
    """
    r = await client.post("/api/event", json={
        "from_agent": "sender", "to_agent": "", "channel": "#x",
        "kind": "tool_call", "content": "hi", "ts": 1})
    assert r.status_code == 200, r.text
    eid = r.json()["event_id"]

    logged = await _message_log_rows(eid)
    assert len(logged) == 1
    assert logged[0]["to_agent"] == ""
    assert await _delivery_rows() == []


async def test_the_not_resolvable_refusal_is_distinct_from_the_grammar_refusals(client):
    """A well-formed address that does not resolve must not be reported as malformed.

    Phase 2 replaces the not-resolvable branch with two distinct outcomes; this
    assertion is what keeps that branch separately addressable until it does.
    """
    async def _message(to_agent):
        r = await client.post("/api/event", json={
            "from_agent": "sender", "to_agent": to_agent, "channel": "#x",
            "kind": "agent_message", "content": "hi", "ts": 1})
        assert r.status_code == 422, r.text
        return r.json()["error"]["message"]

    not_resolvable = await _message("alpha@workerbox")
    malformed = await _message("alpha@box.domain")
    assert not_resolvable != malformed
