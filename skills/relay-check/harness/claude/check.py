# -*- coding: utf-8 -*-
"""check.py — cc-session-relay on-demand message inspection skill.

Reads messages from the local broker via HTTP API. Read-only, non-destructive:
does NOT advance the receive-hook cursor at ~/.claude/relay/cursors/.

Two modes:
  default — GET /api/agent/{my_sid}/messages — per-session inbox
  --all   — GET /api/agent-messages — cross-recipient aggregate (v1.4 endpoint)

Multi-kind: --kind=all fans out across INTER_SESSION_MESSAGE_KINDS, merges by
event_id, dedupes. Per-kind failure isolated (one kind's fetch failure does NOT
block other kinds in the same invocation; per contract.md v1.8).

Fail-loud on broker-unreachable per send.py L210-232 discipline (user invoked
this skill explicitly; surface outage instead of silent {}+exit-0).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Import broker_client helpers (per plan: cleaner than raw urllib since this
# is a read-only skill with no skill-specific HTTP wrapping needs).
sys.path.insert(0, str(Path.home() / ".claude" / "hooks" / "relay"))

from broker_connection import resolve_broker_base_url

DEFAULT_BROKER_BASE_URL = resolve_broker_base_url()
os.environ.setdefault("BROKER_BASE_URL", DEFAULT_BROKER_BASE_URL)
from broker_client import get_agent_messages, get_all_agent_messages  # type: ignore

# Mirror contract.md v1.8 INTER_SESSION_MESSAGE_KINDS tuple (also defined in
# ~/.claude/hooks/relay/shared.py:505). Kept as a local constant so the skill
# runs standalone without depending on relay-hooks paths.
INTER_SESSION_MESSAGE_KINDS = ("agent_message", "agent_reply", "handoff")

# Forbidden kinds per contract.md v1.8 § 8.
FORBIDDEN_KINDS = frozenset({"error"})


HOME = Path.home()
REGISTRY_PATH = HOME / ".claude" / "relay" / "registry.json"


def _broker_base_url() -> str:
    return os.environ.get("BROKER_BASE_URL", DEFAULT_BROKER_BASE_URL).rstrip("/")


def _resolve_session_id() -> str:
    """Resolve session_id via CLAUDE_CODE_SESSION_ID env var.

    Per C-13 (no auto-derive) + F-CC11-F-CC16: CC injects this env var into
    every subprocess at process-spawn time. Fail loud if absent. Parity with
    send.py L210-232 / set_name.py.
    """
    sid = os.environ.get("CLAUDE_CODE_SESSION_ID")
    if sid:
        return sid
    sys.stderr.write(
        "relay-check: CLAUDE_CODE_SESSION_ID env var not set. This script must "
        "run inside a Claude Code session (CC injects this env var into every "
        "subprocess at spawn time per F-CC12). If running outside CC, set "
        "CLAUDE_CODE_SESSION_ID=<sid> explicitly before invoking.\n"
    )
    sys.exit(1)


def _resolve_my_name(registry: Dict[str, Any]) -> str:
    """Resolve current session's user-set friendly_name from registry.

    Per ACTIVE PLAN (friendly-name primitive): broker inbox is keyed by
    friendly_name. session_id is only the env-var bootstrap key. Fail-loud
    if registry has no `name` for this session OR naming_status != 'user_set'
    (matches MCP server discipline at server.py:_resolve_my_name).
    """
    sid = _resolve_session_id()
    entry = registry.get(sid) or {}
    name = entry.get("name") if isinstance(entry, dict) else None
    ns = entry.get("naming_status") if isinstance(entry, dict) else None
    if isinstance(name, str) and name and ns == "user_set":
        return name
    sys.stderr.write(
        f"relay-check: session {sid[:8]}… has no user-set friendly_name in registry.\n"
        f"  Run `/relay-name <name>` first to claim a friendly name "
        f"for cross-session messaging.\n"
    )
    sys.exit(2)


def _load_registry() -> Dict[str, Any]:
    if not REGISTRY_PATH.exists():
        return {}
    try:
        data = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _resolve_from_filter(from_arg: str, registry: Dict[str, Any]) -> List[str]:
    """Return the list of identifiers (names + legacy sids) that match `from_arg`.

    Per ACTIVE PLAN friendly-name primitive: senders POST with from_agent=name,
    so the primary match is direct friendly_name equality. Legacy support: if
    from_arg is a UUID, look up the registry for any historical name(s) and
    include both. Filter is OR-of-candidates client-side.
    """
    candidates = [from_arg]  # primary: direct friendly_name match
    # Legacy: UUID-shaped from_arg → expand to friendly_name(s) from registry
    if len(from_arg) == 36 and from_arg.count("-") == 4:
        for sid, entry in registry.items():
            if sid == from_arg and isinstance(entry, dict):
                name = entry.get("name")
                if isinstance(name, str) and name and name not in candidates:
                    candidates.append(name)
    return candidates


def _filter_by_from(messages: List[Dict[str, Any]], from_candidates: List[str]) -> List[Dict[str, Any]]:
    if not from_candidates:
        return []
    cand_set = set(from_candidates)
    return [m for m in messages if m.get("from_agent") in cand_set]


def _fetch_per_kind(
    *,
    use_all: bool,
    agent_id: str,
    since: int,
    kind: str,
    limit: int,
    broker_url: str,
) -> Optional[Dict[str, Any]]:
    """Call the right broker_client helper based on `use_all`. Returns the
    helper's return value (dict on success; None on broker-down/4xx/5xx).
    """
    if use_all:
        return get_all_agent_messages(
            since=since, kind=kind, limit=limit, broker_url=broker_url
        )
    return get_agent_messages(
        agent_id, since=since, kind=kind, limit=limit, broker_url=broker_url
    )


# Cap on tail-walk iterations to prevent runaway pagination on a degenerate
# broker response (e.g. cursor not advancing). 100 × 100 = 10,000 events ceiling
# per tail invocation; current broker has ~1,800 events so plenty of headroom.
# Page size = 100 chosen to be safe across both endpoints: per-session
# /api/agent/{sid}/messages caps `limit` at 100 (Pydantic Field le=100); aggregate
# /api/agent-messages caps at 200. 100 is the common safe ceiling.
_TAIL_WALK_MAX_PAGES = 100
_TAIL_WALK_PAGE_SIZE = 100


def _fetch_per_kind_tail(
    *,
    use_all: bool,
    agent_id: str,
    kind: str,
    tail_n: int,
    broker_url: str,
    from_candidates: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    """Walk the message_log forward in pages of 100 to discover the newest
    `tail_n` events of `kind`. Mirrors SQL `ORDER BY event_id DESC LIMIT N`.

    Per HIGH iter-1 finding: the broker's GET endpoints return ASC pages with
    no DESC option; the only way to get the tail is to paginate to the end.
    Cost is O(ceil(N_total/100)) HTTP calls per kind; capped at 100 pages
    (10,000 events ceiling per invocation).

    Per MEDIUM iter-2 finding: if `from_candidates` is supplied (non-empty),
    pages are filtered inline as they arrive, so the final tail is "newest N
    from those senders" rather than "newest N globally, then filter (which
    would silently lose recent messages from the target sender if they fall
    outside the global newest N)."

    Returns a dict in the same shape as broker's response:
      {"messages": [...newest tail_n in ASC order...], "next_cursor": None}
    or None on broker-down (any page failing aborts the walk → fail-loud).
    """
    filter_set = set(from_candidates) if from_candidates else None
    collected: List[Dict[str, Any]] = []
    cursor = 0
    for _ in range(_TAIL_WALK_MAX_PAGES):
        result = _fetch_per_kind(
            use_all=use_all,
            agent_id=agent_id,
            since=cursor,
            kind=kind,
            limit=_TAIL_WALK_PAGE_SIZE,
            broker_url=broker_url,
        )
        if result is None:
            return None
        if not isinstance(result, dict):
            return None
        msgs = result.get("messages") or []
        if not isinstance(msgs, list) or not msgs:
            break
        # Apply --from filter inline (per iter-2 MEDIUM fix): only retain matches.
        if filter_set is not None:
            page_filtered = [m for m in msgs if m.get("from_agent") in filter_set]
            collected.extend(page_filtered)
        else:
            collected.extend(msgs)
        # Advance cursor by max event_id we just received (per the receive-hook
        # discipline at shared.py:do_receive — drop broker's next_cursor, use
        # max(event_id) actually received, to defend against contract drift).
        max_eid = 0
        for m in msgs:
            eid = m.get("event_id")
            if isinstance(eid, int) and eid > max_eid:
                max_eid = eid
        if max_eid <= cursor:
            # Cursor didn't advance — broker bug or empty page; stop to avoid loop
            break
        cursor = max_eid
        # If we got less than a full page, we've reached the end.
        if len(msgs) < _TAIL_WALK_PAGE_SIZE:
            break

    # Sort ASC (defensive — broker should already return ASC) and tail
    collected.sort(key=lambda m: m.get("event_id", 0))
    tail = collected[-tail_n:] if len(collected) > tail_n else collected
    return {"messages": tail, "next_cursor": None, "_walked_pages": len(collected) // _TAIL_WALK_PAGE_SIZE + 1}


def _format_text(payload: Dict[str, Any]) -> str:
    """Human-readable table for --format text. Mirrors plan § Output format."""
    msgs = payload.get("messages", [])
    since = payload.get("since", 0)
    tail = payload.get("tail")
    count = payload.get("count", len(msgs))
    if tail is not None:
        header_qualifier = f"newest {tail} per kind"
    else:
        header_qualifier = f"since event_id {since}"
    if not msgs:
        return f"0 messages ({header_qualifier}).\n"
    lines = [f"{count} messages ({header_qualifier}):"]
    for m in msgs:
        event_id = m.get("event_id", "?")
        ts_ms = m.get("ts")
        ts_str = "?"
        if isinstance(ts_ms, (int, float)):
            try:
                ts_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts_ms / 1000))
            except (OSError, OverflowError, ValueError):
                ts_str = f"ts={ts_ms}"
        kind = m.get("kind", "?")
        frm = m.get("from_agent", "?")
        to = m.get("to_agent", "?")
        # Truncate UUIDs for readability
        frm_s = frm[:14] + "…" if isinstance(frm, str) and len(frm) > 16 else frm
        to_s = to[:14] + "…" if isinstance(to, str) and len(to) > 16 else to
        content = m.get("content", "")
        if isinstance(content, str) and len(content) > 120:
            content = content[:117] + "..."
        lines.append(f"  {event_id}  {ts_str}  {kind:<15} {frm_s} → {to_s}")
        lines.append(f"        {content!r}")
    return "\n".join(lines) + "\n"


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")

    p = argparse.ArgumentParser(
        prog="relay-check",
        description=(
            "On-demand inspection of inter-session messages via the local broker. "
            "Read-only; does NOT advance the receive-hook cursor."
        ),
        # allow_abbrev=False per discriminator iter-1 M1: argparse's default
        # abbreviation support (e.g. `--sinc 0` accepted as `--since 0`) would
        # silently bypass the sys.argv-based `user_passed_since` detection
        # below, causing the strengthened mutex + default-injection to misfire.
        # Disabling abbreviation aligns argparse's accepted spellings with the
        # literal-string detection. Future-proofs other flags too.
        allow_abbrev=False,
    )
    p.add_argument("--since", type=int, default=0,
                   help=("Cursor (event_id strictly greater than this). Pass --since 0 "
                         "explicitly for full history (full-dump cron use case). When "
                         "neither --since nor --tail is passed, default is --tail 20 "
                         "(NOT --since 0; see --tail). Mutually exclusive with --tail."))
    p.add_argument("--tail", type=int, default=None,
                   help=("Return the NEWEST N messages (per kind in --kind=all mode; "
                         "or N total in single-kind mode). Walks the message_log forward "
                         "in pages of 100 to discover the tail; cost ~ceil(N_total/100) "
                         "HTTP calls per kind, capped at 100 pages. Mutually exclusive "
                         "with --since (use one or the other). Mirrors SQL "
                         "`ORDER BY event_id DESC LIMIT N` semantics. When combined "
                         "with --from, filters inline during the walk so 'newest N from "
                         "<sender>' is returned, not 'newest N globally then filter'. "
                         "Default when --since not passed: --tail 20 (inbox-viewer semantics; "
                         "mirrors tail / journalctl -n / git log -n)."))
    p.add_argument("--kind", type=str, default="all",
                   help=("Single kind (agent_message | agent_reply | handoff) OR "
                         "'all' (fans out across all 3 inter-session kinds). "
                         "Default: all. 'error' rejected per contract v1.8 § 8."))
    p.add_argument("--limit", type=int, default=20,
                   help="Per-kind page size (max 200 server-side). Default 20.")
    p.add_argument("--from", dest="from_filter", type=str, default=None,
                   help="Client-side filter on sender (friendly name or UUID).")
    p.add_argument("--all", action="store_true",
                   help=("Use cross-recipient aggregate endpoint "
                         "GET /api/agent-messages (v1.4) instead of per-session inbox."))
    p.add_argument("--format", dest="output_format", choices=("json", "text"),
                   default="json", help="Output format. Default: json.")
    args = p.parse_args()

    # Detect whether user explicitly passed --since (vs argparse default of 0).
    # sys.argv inspection is the only way since argparse can't distinguish
    # "user passed --since 0" from "argparse default of 0".
    user_passed_since = any(a == "--since" or a.startswith("--since=") for a in sys.argv[1:])

    # --tail and --since are mutually exclusive (per docstring contract).
    if args.tail is not None and user_passed_since:
        sys.stderr.write(
            "relay-check: --tail and --since are mutually exclusive. "
            "--tail walks forward to discover the newest events; --since is a fixed cursor.\n"
        )
        sys.exit(1)
    if args.tail is not None and args.tail <= 0:
        sys.stderr.write(f"relay-check: --tail must be > 0; got {args.tail}.\n")
        sys.exit(1)

    # Apply default: if neither --tail nor --since was explicitly passed, default
    # to --tail 20 (per mtr_wiki handoff; inbox-viewer semantics). Per C-17 root-
    # cause discipline: changes the cause (unbounded default), not the symptom.
    # Explicit --since 0 still works (full-dump cron use case preserved).
    if args.tail is None and not user_passed_since:
        args.tail = 20

    # Validate --kind early
    if args.kind in FORBIDDEN_KINDS:
        sys.stderr.write(
            f"relay-check: --kind={args.kind!r} is forbidden per contract.md v1.8 § 8 "
            f"(auto-injected by ~/.claude/hooks/relay/receive_errors.py on every "
            f"UserPromptSubmit + SessionStart fire; do not double-poll).\n"
        )
        sys.exit(1)
    valid_kinds = set(INTER_SESSION_MESSAGE_KINDS) | {"all"}
    if args.kind not in valid_kinds:
        sys.stderr.write(
            f"relay-check: --kind={args.kind!r} not recognized. "
            f"Valid: {sorted(INTER_SESSION_MESSAGE_KINDS)} OR 'all'.\n"
        )
        sys.exit(1)
    if args.limit <= 0:
        sys.stderr.write(f"relay-check: --limit must be > 0; got {args.limit}.\n")
        sys.exit(1)
    if args.since < 0:
        sys.stderr.write(f"relay-check: --since must be >= 0; got {args.since}.\n")
        sys.exit(1)

    session_id = _resolve_session_id()
    broker_url = _broker_base_url()
    registry = _load_registry()
    # ACTIVE PLAN: broker inbox keyed by friendly_name. Resolve here; fail-loud
    # if no user-set name (parity with MCP server + send.py sender gate).
    my_name = _resolve_my_name(registry)

    # Resolve --from filter to candidate session_ids (if applicable)
    from_candidates: List[str] = []
    if args.from_filter:
        from_candidates = _resolve_from_filter(args.from_filter, registry)
        if not from_candidates:
            # Soft warning to stderr; still proceed (broker may have messages
            # from agents not in our registry — UUID-direct case is handled by
            # _resolve_from_filter; a registry-absent friendly name = no match)
            sys.stderr.write(
                f"relay-check: --from {args.from_filter!r} not found in registry; "
                f"no client-side filter will match.\n"
            )

    # Determine kinds to poll
    if args.kind == "all":
        kinds_to_poll = list(INTER_SESSION_MESSAGE_KINDS)
    else:
        kinds_to_poll = [args.kind]

    # Fan out across kinds (per-kind failure isolated)
    all_messages: List[Dict[str, Any]] = []
    fetch_errors: List[Dict[str, Any]] = []
    successful_kinds: List[str] = []

    for kind in kinds_to_poll:
        if args.tail is not None:
            # Tail mode: filter inline during walk (per iter-2 MEDIUM fix) so
            # --tail N + --from X returns "newest N from X" not "newest N
            # globally, then filter (which loses recent X messages if they fall
            # outside the global newest N)."
            result = _fetch_per_kind_tail(
                use_all=args.all,
                agent_id=my_name,
                kind=kind,
                tail_n=args.tail,
                broker_url=broker_url,
                from_candidates=from_candidates if args.from_filter else None,
            )
        else:
            result = _fetch_per_kind(
                use_all=args.all,
                agent_id=my_name,
                since=args.since,
                kind=kind,
                limit=args.limit,
                broker_url=broker_url,
            )
        if result is None:
            fetch_errors.append({"kind": kind, "error": "broker_returned_none"})
            continue
        successful_kinds.append(kind)
        msgs = result.get("messages") if isinstance(result, dict) else None
        if isinstance(msgs, list):
            all_messages.extend(msgs)

    # If ALL kinds failed: broker-down / 4xx; fail-loud per send.py discipline
    if not successful_kinds:
        sys.stdout.write(json.dumps({
            "error": f"broker unreachable or returned 4xx/5xx at {broker_url}",
            "status": 0,
            "kinds_attempted": kinds_to_poll,
            "fetch_errors": fetch_errors,
        }))
        sys.stdout.write("\n")
        sys.exit(2)

    # Dedupe by event_id (each event has a unique global event_id per
    # message_log schema; same event surfacing across multiple per-kind fetches
    # would be unusual but defensive). Sort ASC by event_id.
    seen_event_ids: set = set()
    deduped: List[Dict[str, Any]] = []
    for m in all_messages:
        eid = m.get("event_id")
        if eid in seen_event_ids:
            continue
        seen_event_ids.add(eid)
        deduped.append(m)
    deduped.sort(key=lambda m: m.get("event_id", 0))

    # Apply --from filter client-side
    if args.from_filter:
        filtered = _filter_by_from(deduped, from_candidates)
    else:
        filtered = deduped

    endpoint = "/api/agent-messages" if args.all else f"/api/agent/{my_name}/messages"
    payload = {
        "session_id": session_id,
        "name": my_name,
        "endpoint": endpoint,
        "mode": "tail" if args.tail is not None else "since",
        "since": args.since,
        "tail": args.tail,
        "kinds_polled": successful_kinds,
        "kinds_failed": [e["kind"] for e in fetch_errors] if fetch_errors else [],
        "messages": filtered,
        "count": len(filtered),
        "filtered_by_from": args.from_filter,
    }

    if args.output_format == "text":
        sys.stdout.write(_format_text(payload))
    else:
        sys.stdout.write(json.dumps(payload))
        sys.stdout.write("\n")
    sys.exit(0)


if __name__ == "__main__":
    main()
