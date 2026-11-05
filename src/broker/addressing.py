"""Recipient address grammar for POST /api/event (ADDR-01..ADDR-09, MACH-05).

PURE LEAF MODULE. It imports `__future__`, `re` and `typing` and nothing else --
never `server`, never `fastapi`, never `dashboard`. That is what lets the grammar
table run with no app, no event loop and no database, and it is why `server.py` may
import this while this imports nothing back (the dependency-direction rule stated at
`dashboard/api.py:5-6`).

USED BY: `server.py::post_event`, exactly once per request, ABOVE the `_do()` closure
and therefore above any database connection. A refusal opens no connection, which is
what makes "zero message_log rows and zero delivery rows on a refusal" true by
construction rather than by inspection.

WHAT IT RETURNS: a `Route` -- one of three dispositions plus the key the caller binds
at every write site. It NEVER returns `None` to mean refused: a bare `None` is
indistinguishable from the broadcast passthrough, and being mistaken for one is the
exact fall-through-to-bare-name failure ADDR-08 forbids.

PHASE-1 SEAM CONTRACT: the roster phase replaces the FINAL not-resolvable branch.
R1's product-neutral session contract additionally folds accepted local mailbox
outputs; UUID and refusal branches remain unchanged.

PHASE-1 SCOPE NOTE: exactly one machine name is recognised -- `localhost`. A
well-formed address naming any other machine parses but does not resolve and is
refused generically. That is a placeholder, not the finished behaviour; Phase 2
splits it into REMOTE-01's and REMOTE-02's two distinct, requirement-bearing outcomes.
"""

from __future__ import annotations

import re
from typing import NamedTuple, Optional

# DOWNSTREAM MIRROR (ADDR-05a, 2026-08-15): `_NAME_FOLD` and `_normalize_name` below are
# the THIRD copy of ONE rule. The definition of record is `normalize_name` in the
# agent-config repo at `agents/hooks/relay/shared.py` (ships on Channel B); the second copy
# is `home/.claude/plugins/local-relay-channel/server.py::_normalize_name` (Channel A, this
# repo). No absolute path is written here on purpose -- the portability rule forbids a
# build-host path in shipped source, and this file runs on both Windows and Linux fleet
# machines. An import is impossible in BOTH directions: the three copies live in two
# repositories and ship on two independent channels, and the plugin is deliberately
# standalone. IF THE FOLD RULE CHANGES, ALL THREE CHANGE IN THE SAME WAVE.
#
# THE FAILURE MODE IF THEY DRIFT IS SILENCE, NOT AN ERROR. The plugin's SSE filter is a
# bare inequality -- `if data.get("to_agent") != my_name: return "skipped"`
# (plugin server.py:2046) -- so a key the writer folded differently from the reader is
# dropped without a log line, the sender still got its 200 and its event id, and the
# escalation reaper pages a human on Discord an hour later.
# PINNED BY: `tests/test_name_fold_plugin.py`.
#
# A-Z ONLY, ON PURPOSE -- and this is the reason, measured on Python 3.12: `.lower()`
# and `.casefold()` both map characters from OUTSIDE the validated alphabet INTO it.
# `"\u212a"` (KELVIN SIGN) casefolds to `k`; `"\u0130"` (LATIN CAPITAL I WITH DOT ABOVE)
# lowers to `i` + `"\u0307"`; `"\u1e9e"` (LATIN CAPITAL SHARP S) casefolds to `ss`. Each
# of those merges two genuinely distinct mailboxes into one -- inventing the collision
# this fold exists to prevent. Everything outside A-Z passes through untouched.
_NAME_FOLD = {c: c + 32 for c in range(ord("A"), ord("Z") + 1)}

# R1 product-neutral mailbox grammar. Claims are strict; recipient parsing remains
# backward compatible by folding accepted strings without rejecting legacy spellings.
_RELAY_NAME_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


def _normalize_name(name):
    """Fold ASCII A-Z to a-z. A non-str argument passes through unchanged (ADDR-05a)."""
    if not isinstance(name, str):
        return name
    return name.translate(_NAME_FOLD)


def canonicalize_relay_name(value: object) -> Optional[str]:
    """Return the canonical mailbox for a valid relay-name claim, else ``None``."""
    if not isinstance(value, str) or not _RELAY_NAME_RE.fullmatch(value):
        return None
    return _normalize_name(value)


# DOWNSTREAM MIRROR (ADDR-07, 2026-08-15): this pattern is a VERBATIM copy of
# `home/.claude/plugins/local-relay-channel/server.py::_UUID_RE` (plugin server.py:130-133).
# An import is impossible for the same reason as the fold above: the plugin ships on the
# broker Release and installs per machine, so the two processes cannot share a symbol.
# The two must agree about what the session-id KEY-SPACE is, because the broker decides
# with it what to pass through untouched and the plugin decides with it what to exempt
# from folding -- a disagreement files a message under a key its reader never queries,
# and the plugin's `!=` filter drops it in silence.
# `uuid.UUID(s)` is NOT an equivalent test and must not be substituted: it accepts braced,
# URN and unhyphenated 32-character forms that this pattern rejects.
# PINNED BY: `tests/test_name_fold_plugin.py`.
_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)

# ADDR-04: a single letter-digit-hyphen label, so one machine cannot have two spellings.
# No dot is admitted -- `x.localhost` and `localhost.x` are refused here rather than
# reaching the reservation check below. Strict at both ends (no leading or trailing
# hyphen): widening this later is backward-compatible, narrowing it later is not, and no
# roster machine has a leading or trailing hyphen. LOOKAROUND-FREE deliberately, so it
# can never become an import-time engine error.
_MACHINE_LABEL_RE = re.compile(r"^[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?$")

# ADDR-04: RFC 1123 §2.1's hostname-label ceiling. The input is already bounded to 256
# characters by `EventBody.to_agent` before this module ever sees it; this is the label's
# own bound, not an input bound, and this module deliberately adds no length check to the
# bare-name path (ADDR-03).
MACHINE_LABEL_MAX_LEN = 63

RESERVED_LOCAL_MACHINE = "localhost"  # MACH-05


class Route(NamedTuple):
    """The parser's tagged answer. `key` is what the caller binds at EVERY write site."""

    disposition: str  # "local" | "passthrough" | "refused"
    key: Optional[str]  # the value to bind at the message_log insert, the delivery
    # insert and the broadcast_event envelope -- all three
    reason: Optional[str]  # a fixed token; NEVER contains sender-supplied text


def parse_recipient(value: Optional[str]) -> Route:
    """Resolve a sender-supplied recipient to one of three answers.

    The order of the steps below IS the requirement set, in the order the requirements
    themselves specify. Reordering any of them changes behaviour -- see the per-step
    comments for which reordering breaks what.
    """
    # 1. Broadcast / absent recipient. `to_agent` is Optional[str] and a broadcast must
    #    keep working exactly as it does today.
    if not isinstance(value, str):
        return Route("passthrough", value, None)

    # 2. ADDR-09 -- a `*` ANYWHERE in the address, so before any split. A wildcard that
    #    is accepted as a literal name is a delivery semantic arriving by accident.
    if "*" in value:
        return Route("refused", None, "wildcard_not_supported")

    # 3. ADDR-07 -- the session-id key-space, tested BEFORE the split. These keys are
    #    matched exactly as they are today.
    if _UUID_RE.match(value):
        return Route("passthrough", value, None)

    # 4. ADDR-07 -- the `<name>@<uuid>` form. Steps 3 and 4 MUST precede step 8: a session
    #    id is itself a syntactically valid LDH label (hex digits and hyphens, no dots), so
    #    a label check placed above them would read the session id as a machine name and
    #    refuse a delivery that works today.
    #
    #    THE NON-EMPTY `head` IS LOAD-BEARING; DO NOT SIMPLIFY IT AWAY (2026-08-15).
    #    Without it this branch answers BEFORE step 7's empty-part check, and `@<uuid>` --
    #    a well-formed session id with NO name in front of it -- returns `passthrough`
    #    instead of the `empty_address_part` refusal ADDR-01 requires. That writes a
    #    recipient key no consumer polls to all three write sites behind an HTTP 200.
    #    Step 7 cannot catch it: this branch returns first, which is exactly why the
    #    guard belongs here and not there.
    #    MEASURED, not reasoned: before this guard, `parse_recipient("@<uuid>")` returned
    #    `Route("passthrough", "@<uuid>", None)`; after it, `empty_address_part`. Control:
    #    `"@box"` refused correctly both before and after, so the empty-part branch was
    #    always live and merely unreachable for the UUID spelling.
    #    PINNED BY: `tests/test_addressing_route.py::test_empty_name_uuid_is_refused`.
    if value.count("@") == 1:
        head, _, tail = value.partition("@")
        if head and _UUID_RE.match(tail):
            return Route("passthrough", value, None)

    # 5. ADDR-02 -- ambiguity, counted BEFORE any split is attempted. A right-split of a
    #    three-part address returns a plausible-looking pair, so the parser must refuse
    #    rather than guess which `@` was the separator.
    if value.count("@") > 1:
        return Route("refused", None, "ambiguous_multiple_separators")

    # 6. R1 -- recipient ingress uses the shared ASCII fold, without adding claim-time
    #    validation to the legacy bare-name path.
    if "@" not in value:
        return Route("local", _normalize_name(value), None)

    local, _, machine = value.partition("@")

    # 7. ADDR-01 -- both parts non-empty. Covers `@box` and `alpha@`.
    if not local or not machine:
        return Route("refused", None, "empty_address_part")

    # 8. ADDR-04 -- a single LDH label within its length bound.
    if len(machine) > MACHINE_LABEL_MAX_LEN or not _MACHINE_LABEL_RE.match(machine):
        return Route("refused", None, "invalid_machine_label")

    # 9. MACH-05 -- reserved, checked BEFORE any roster lookup (Phase 1 has no roster, so
    #    this is the only accept). R1 folds the accepted local mailbox too.
    if _normalize_name(machine) == RESERVED_LOCAL_MACHINE:
        return Route("local", _normalize_name(local), None)

    # 10. PHASE-1 PLACEHOLDER. Phase 2 replaces THIS BRANCH AND ONLY THIS BRANCH with
    #     REMOTE-01 ("known machine, not routable yet") and REMOTE-02 ("unknown machine")
    #     as two distinct outcomes, never collapsed into one.
    return Route("refused", None, "machine_not_resolvable")
