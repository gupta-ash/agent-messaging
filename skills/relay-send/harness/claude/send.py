# -*- coding: utf-8 -*-
"""send.py — cc-session-relay sender skill entry-point.

Per task spec § relay-send.

Resolves the recipient (name -> UUID via registry; or accepts UUID directly),
resolves the sender's own session_id (env -> state marker -> sessions file),
builds a JSON envelope (`{text, sender_name, reply_to_event_id?}`), POSTs to
`/api/event` with `kind='agent_message'`, prints a receipt to stdout.

Send-side errors surface to the user (the user invoked this skill explicitly)
— DO NOT silent-degrade like the receive hooks.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import uuid
from pathlib import Path
from typing import Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

# Import shared broker helper from the relay hooks package.
# Per C-13 + F-CC11-F-CC16: pid-walk removed; session_id resolved via
# CLAUDE_CODE_SESSION_ID env var instead (injected by claude.exe per F-CC12).
sys.path.insert(0, str(Path.home() / ".claude" / "hooks" / "relay"))

try:
    # canonical liveness (dual-key + daemon-aware) + broker-down marker writer;
    # supersedes the local _is_session_active. `normalize_name` is the C-48
    # relay-name fold — imported rather than copied so there is exactly ONE
    # definition on this side of the fleet; shared.py and this file ship in the
    # same Channel-B commit, so they cannot skew. If it is ever missing, the
    # except below exits 1 loudly rather than sending to an unfolded mailbox.
    from shared import (
        DEFAULT_BROKER_BASE_URL,
        _is_sid_live,
        _sid_liveness,
        _mark_broker_down,
        normalize_name,
    )
except Exception as _e:  # pragma: no cover
    sys.stderr.write(f"relay-send: cannot import from relay shared lib: {_e}\n")
    sys.exit(1)

HOME = Path.home()
RELAY_DIR = HOME / ".claude" / "relay"
REGISTRY_PATH = RELAY_DIR / "registry.json"

BROKER_TIMEOUT_S = 5.0

# --- #24 sender-side broker-down watchdog (WS-B sender) ---------------------
# When a send POST never reaches the broker (URLError), NO delivery row is ever
# created, so the recipient-side reaper (#18) can never alert. The sender is the
# only actor that can see this, and Discord (external) is the only alert path
# alive during a local-broker outage. So: persist -> mark down -> alert (deduped).
DISCORD_API_BASE = "https://discord.com/api/v10"
ALERT_TIMEOUT_S = 10.0
DEFAULT_SENDER_ALERT_COOLDOWN_S = 300.0
SENDER_ALERT_MARKER_PATH = RELAY_DIR / "state" / "sender-alert.json"
# Bounded recovery-flush: at most K queued sends replayed per successful send,
# single-shot each, so a large backlog never stalls the send before its receipt
# (bounded foreground work). The broker is proven-reachable here (the send just 2xx'd),
# so these POSTs are fast; the bound is a safety cap. Rest drains on later sends.
SENDER_FLUSH_MAX = 5
SENDER_FLUSH_TIMEOUT_S = 1.5

# broker_client provides the generic outbox (persist failed send + replay on
# recovery). It RuntimeErrors at IMPORT if BROKER_BASE_URL is unset (it bounds
# the var), which send.py otherwise tolerates via its default -> seed the env
# FIRST (mirrors the plugin server's setdefault), then import; degrade to None
# (still alert + mark-down) if the import fails for any other reason.
os.environ.setdefault("BROKER_BASE_URL", DEFAULT_BROKER_BASE_URL)
sys.path.insert(0, str(HOME / ".claude" / "hooks" / "relay"))
try:
    import broker_client  # deployed ~/.claude/hooks/relay/broker_client.py
except Exception as _bc_e:  # pragma: no cover
    broker_client = None
    sys.stderr.write(f"relay-send: broker_client unavailable ({_bc_e}); sender-outbox disabled\n")

# Canonical UUID4 (36 chars: 8-4-4-4-12 hex with hyphens)
UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


def _broker_base_url() -> str:
    return os.environ.get("BROKER_BASE_URL", DEFAULT_BROKER_BASE_URL).rstrip("/")


# --- #24 broker-down watchdog helpers (stdlib urllib only; NEVER import the ---
# --- Channel-A broker tree; every helper is best-effort + never raises) ------

def _sender_alert_cooldown_s() -> float:
    """Dedup window so a burst of sends during one outage doesn't spam Discord."""
    try:
        v = float(os.environ.get("BROKER_SENDER_ALERT_COOLDOWN_S", ""))
        return v if v > 0 else DEFAULT_SENDER_ALERT_COOLDOWN_S
    except (ValueError, TypeError):
        return DEFAULT_SENDER_ALERT_COOLDOWN_S


def _alert_channel_id() -> str:
    return os.environ.get("BROKER_ALERT_DISCORD_CHANNEL_ID", "").strip()


def _discord_token() -> Optional[str]:
    """Bot token for the broker-down alert. Mirrors discord_poller._load_env_token:
    env DISCORD_BOT_TOKEN first, else parse ~/.claude/channels/discord/.env
    (honor DISCORD_STATE_DIR). Readable by any same-user process. Never raises."""
    tok = os.environ.get("DISCORD_BOT_TOKEN")
    if tok:
        return tok
    state_dir = os.environ.get("DISCORD_STATE_DIR") or str(
        HOME / ".claude" / "channels" / "discord"
    )
    try:
        text = (Path(state_dir) / ".env").read_text(encoding="utf-8")
    except Exception:
        # OSError/FileNotFoundError AND UnicodeDecodeError (a ValueError) on a
        # non-UTF-8 / BOM'd .env — must never propagate (H1: would crash the
        # broker-down send path before the receipt is emitted).
        return None
    pat = re.compile(r"^(\w+)=(.*)$")
    for line in text.split("\n"):
        m = pat.match(line)
        if m and m.group(1) == "DISCORD_BOT_TOKEN":
            v = m.group(2).strip()
            return v or None
    return None


def _sanitize_alert_field(s, limit: int = 200) -> str:
    """Printable-ASCII, single-line, code-span-safe field for the Discord alert.
    The fields here are the sender's OWN name/text (trusted-ish); this is
    defense-in-depth atop the hard allowed_mentions.parse:[] backstop."""
    try:
        s = str(s)
    except Exception:
        return "?"
    s = "".join(ch if 0x20 <= ord(ch) <= 0x7E else " " for ch in s)  # printable ASCII, one line
    s = s.replace("`", "'").replace('"', "'")  # can't open code spans or the ("...") preview quote
    s = re.sub(r"[*_~|\\]", "", s)  # drop markdown emphasis/spoiler/escape so it can't restyle the alert
    s = " ".join(s.split())  # collapse whitespace runs
    return s[:limit]


def _post_discord_alert(content: str) -> bool:
    """Best-effort direct Discord POST (the ONLY alert path alive when the local
    broker is down — Discord is external). Stdlib urllib only. Returns True only
    on 2xx; never raises; False without an explicitly configured destination/token."""
    channel_id = _alert_channel_id()
    if not channel_id:
        return False
    tok = _discord_token()
    if not tok:
        return False
    url = f"{DISCORD_API_BASE}/channels/{channel_id}/messages"
    headers = {
        "Authorization": f"Bot {tok}",
        "Content-Type": "application/json",
        "User-Agent": "relay-send (broker-down watchdog)",
    }
    body = json.dumps({
        "content": content[:1900],
        "allowed_mentions": {"parse": []},  # Discord parses NO mentions from content
        "flags": 4,                          # SUPPRESS_EMBEDS: no URL unfurl
    }).encode("utf-8")
    for attempt in range(2):  # one 429 retry
        try:
            req = Request(url, data=body, headers=headers, method="POST")
            with urlopen(req, timeout=ALERT_TIMEOUT_S) as resp:
                return 200 <= resp.status < 300
        except HTTPError as e:
            if e.code == 429 and attempt == 0:
                try:
                    retry_after = float(e.headers.get("Retry-After", "0") or 0)
                except (ValueError, TypeError):
                    retry_after = 0.0
                if 0 < retry_after <= 30:
                    time.sleep(retry_after)
                    continue
            return False
        except Exception:
            return False
    return False


def _sender_alert_due() -> bool:
    """True iff the last sender-side Discord alert is older than the cooldown.
    Silent-degrade: any read failure -> due (fail TOWARD alerting)."""
    try:
        data = json.loads(SENDER_ALERT_MARKER_PATH.read_text(encoding="utf-8"))
        last = float(data.get("last_sender_alert_ts", 0))
    except Exception:
        return True
    now_ms = time.time() * 1000.0
    if last > now_ms + 5000:  # future-ts (clock skew) guard -> treat as due
        return True
    return (now_ms - last) > _sender_alert_cooldown_s() * 1000.0


def _stamp_sender_alert() -> None:
    """Record the alert time (best-effort atomic write; never raises)."""
    try:
        SENDER_ALERT_MARKER_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = SENDER_ALERT_MARKER_PATH.parent / (".sender-alert." + uuid.uuid4().hex + ".tmp")
        tmp.write_text(
            json.dumps({"last_sender_alert_ts": int(time.time() * 1000)}),
            encoding="utf-8",
        )
        os.replace(str(tmp), str(SENDER_ALERT_MARKER_PATH))
    except Exception:
        pass


def _clear_sender_alert_stamp() -> None:
    """Release a just-claimed cooldown stamp when the Discord post FAILED, so the
    next send is due again (preserves fail-toward-alerting; never raises)."""
    try:
        os.unlink(str(SENDER_ALERT_MARKER_PATH))
    except Exception:
        pass


def _load_registry() -> dict:
    if not REGISTRY_PATH.exists():
        return {}
    try:
        data = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _check_sender_gate(sender_sid: str, registry: dict) -> None:
    """Per APPENDIX 3 sender-side gate: sender's session must have
    naming_status='user_set' (i.e., user has run `/relay-name`).

    If gate fails: exit 2 with diagnostic. Sender keeps default
    `<reponame>-<short-sid-8>` name → must claim a friendly name first.
    """
    entry = registry.get(sender_sid)
    naming_status = entry.get("naming_status") if isinstance(entry, dict) else None
    if naming_status == "user_set":
        return
    current_default = entry.get("name") if isinstance(entry, dict) else None
    if not current_default:
        current_default = sender_sid[:8] + "…"
    sys.stderr.write(
        f"relay-send: your session has no friendly name set "
        f"(current default: '{current_default}').\n"
        f"  Run `/relay-name <name>` first to claim a friendly name "
        f"for cross-session messaging.\n"
    )
    sys.exit(2)


def _resolve_recipient(to_arg: str, registry: dict, sender_sid: str) -> Tuple[str, Optional[str]]:
    """Return (recipient_friendly_name, recipient_sid_or_None).

    Per ACTIVE PLAN friendly-name primitive: recipient is addressed by
    friendly_name in the broker POST body. UUID-shaped --to args are STILL
    accepted for backward-compat (look up registry to convert; if found and
    active, return its name; else exit 1).

    Exits 1 with diagnostic when:
      - friendly_name not found in any registry entry's 'name' field
      - friendly_name found but session not active (ppid dead AND last_seen stale)
      - UUID supplied but session not in registry OR not active
      - UUID supplied but session has no friendly_name set

    Multi-match (rare): pick most-recent-active by last_seen_ts; warn to stderr.

    Returned `recipient_sid` is informational only — used in the JSON receipt
    for operator traceability. Broker POST uses friendly_name as to_agent.
    """
    if UUID_RE.match(to_arg):
        if to_arg == sender_sid:
            sys.stderr.write(
                f"relay-send: cannot send to your own session ('{to_arg[:14]}...').\n"
            )
            sys.exit(1)
        # Legacy UUID path: convert to friendly_name via registry.
        entry = registry.get(to_arg)
        if not isinstance(entry, dict):
            sys.stderr.write(
                f"relay-send: session_id '{to_arg[:14]}…' not in registry. "
                f"Use friendly_name (post-friendly-name-primitive migration).\n"
            )
            _print_active_names_diagnostic(registry, sender_sid)
            sys.exit(1)
        name = entry.get("name")
        if not isinstance(name, str) or not name:
            sys.stderr.write(
                f"relay-send: session '{to_arg[:14]}…' has no user-set "
                f"friendly_name. Recipient must run `/relay-name <name>` first.\n"
            )
            _print_active_names_diagnostic(registry, sender_sid)
            sys.exit(1)
        live = _sid_liveness(to_arg)
        if live == "dead":
            sys.stderr.write(
                f"relay-send: session '{to_arg[:14]}…' (name='{name}') "
                f"is not live (no live foreground process or background daemon job-state).\n"
            )
            _print_active_names_diagnostic(registry, sender_sid)
            sys.exit(1)
        if live == "unknown":
            sys.stderr.write(f"relay-send: WARNING - liveness of session '{to_arg[:14]}...' (name='{name}') could not be confirmed (transient probe); sending anyway.\n")
        return normalize_name(name), to_arg

    # Friendly-name path (preferred post-ACTIVE PLAN).
    # ENTRY-POINT FOLD (C-48) — `--to` is the second of the two ARGUMENTS a relay
    # name enters through, and the value returned from here becomes the broker's
    # `to_agent`, i.e. the mailbox the message is filed under. Fold it once, at
    # the top of this path, and the comparison below, the diagnostics and the
    # return value are all canonical from one edit.
    #
    # THE UUID BRANCH ABOVE IS DELIBERATELY NOT FOLDED HERE: a session_id is a
    # different key-space and folding it would be a change to sid addressing,
    # which this work does not touch. What IS folded there is the NAME read out
    # of the registry row, because that is a name entering the process.
    #
    # The diagnostics below now quote the canonical form rather than what was
    # typed. That is intended: post-fold the canonical form IS the name, and the
    # active-name list printed alongside makes the mapping obvious.
    to_arg = normalize_name(to_arg)
    matches = []  # (sid, entry, last_seen_or_0, liveness)
    for sid, entry in registry.items():
        if sid == sender_sid:
            continue
        # BOTH OPERANDS FOLDED: `to_arg` at the entry point above, the STORED
        # name here because rows written before this change carry their original
        # casing and this function reads the registry JSON raw.
        if isinstance(entry, dict) and normalize_name(entry.get("name")) == to_arg:
            live = _sid_liveness(sid)
            if live == "dead":
                continue
            last_seen = entry.get("last_seen_ts")
            matches.append((sid, entry, last_seen or 0, live))

    if not matches:
        sys.stderr.write(
            f"relay-send: no active session named '{to_arg}'.\n"
            f"  Run `/relay-name <name>` from the target session to claim a name.\n"
        )
        _print_active_names_diagnostic(registry, sender_sid)
        sys.exit(1)

    matches.sort(key=lambda m: (m[3] == "live", m[2]), reverse=True)
    chosen_sid = matches[0][0]
    if matches[0][3] == "unknown":
        sys.stderr.write(f"relay-send: WARNING - liveness of '{to_arg}' (sid {chosen_sid[:8]}) could not be confirmed (transient probe); sending anyway.\n")
    live_ct = sum(1 for m in matches if m[3] == "live")
    if live_ct > 1:
        sys.stderr.write(f"relay-send: WARNING - {live_ct} live sessions named '{to_arg}'; picking most recent (sid={chosen_sid[:14]}...)\n")
    return to_arg, chosen_sid


def _print_active_names_diagnostic(registry: dict, sender_sid: str) -> None:
    """Stderr-print the list of currently-active named sessions for the user."""
    active = []
    for sid, entry in registry.items():
        if sid == sender_sid:
            continue
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        if not name:
            continue
        last_seen = entry.get("last_seen_ts")
        if _is_sid_live(sid):
            active.append((name, sid, last_seen or 0))
    if not active:
        sys.stderr.write("  (no active named sessions in registry)\n")
        return
    active.sort(key=lambda x: x[2], reverse=True)
    sys.stderr.write("  Active named sessions:\n")
    for name, sid, last_seen_ms in active[:20]:
        if last_seen_ms:
            age_s = int(time.time()) - (last_seen_ms // 1000)
            if age_s < 60:
                age_str = f"{age_s}s ago"
            elif age_s < 3600:
                age_str = f"{age_s // 60}m ago"
            else:
                age_str = f"{age_s // 3600}h ago"
        else:
            age_str = "(no last_seen)"
        sys.stderr.write(f"    - {name}  (sid={sid[:14]}…, last_seen={age_str})\n")


def _resolve_sender_session_id() -> str:
    """Resolve sender session_id via CLAUDE_CODE_SESSION_ID env var.

    Per C-13 (no auto-derive) + F-CC11-F-CC16: CC injects CLAUDE_CODE_SESSION_ID
    into every subprocess env at process-spawn time. Verified empirically on
    Windows Git Bash via 9-test deep-inheritance probe.

    Pre-C-13 used pid-walk. Removed (was undocumented + broken on Windows
    where $PPID=1 in orphaned subprocess per F-CC11; was confabulated invention
    per F-CC5/F-CC8).

    Fail loud if env var unset.
    """
    sid = os.environ.get("CLAUDE_CODE_SESSION_ID")
    if sid:
        return sid
    sys.stderr.write(
        "relay-send: CLAUDE_CODE_SESSION_ID env var not set. This script must "
        "run inside a Claude Code session (CC injects this env var into every "
        "subprocess at spawn time per F-CC12). If running outside CC, set "
        "CLAUDE_CODE_SESSION_ID=<sid> explicitly before invoking.\n"
    )
    sys.exit(1)


def _resolve_sender_name(sender_sid: str, registry: dict) -> str:
    entry = registry.get(sender_sid)
    if isinstance(entry, dict):
        name = entry.get("name")
        if isinstance(name, str) and name:
            # ENTRY-POINT FOLD (C-48), SENDER SIDE — and this one is load-bearing
            # rather than cosmetic. MEASURED: the value returned here becomes
            # `sender_name`, which is posted to the broker as the event's
            # `from_agent`. The recipient renders that as the `from` attribute
            # and is instructed to reply by addressing it, so an unfolded value
            # here hands a peer a name in a casing the registry may not hold.
            #
            # NOT IN THE DESIGN SPEC'S SITE LIST. The spec said "two files and
            # two arguments" and enumerated only `_resolve_recipient` in this
            # file; this second registry-name read was found by tracing where
            # each remaining raw read actually goes. The PRINCIPLE (fold at
            # entry) held; the enumeration was short by two.
            return normalize_name(name)
    # WAS `return sender_sid[:8] + "…"`. That value is posted as the event's
    # `from_agent` — the recipient renders it as the `from` attribute and is
    # instructed to reply by addressing it — so it is a REPLY ADDRESS, and a
    # truncated session id is valid in NEITHER key-space: `peer_reply` and
    # `_resolve_recipient` both resolve a friendly_name or a WHOLE registered
    # session_id and nothing else. A peer answering `a0000000…` reaches nobody,
    # and the refusal lands on the peer rather than on whoever wrote the address.
    #
    # `"<unknown>"` is this codebase's existing word for "no identity
    # established" — shared.py::resolve_sender_name returns exactly that literal
    # from its own no-identity arms — so this follows a convention instead of
    # inventing one, and it is honest in the only way that matters here: it does
    # not look like something a peer can answer.
    #
    # THE SEND STILL PROCEEDS. This arm fires only when the SENDER has no
    # registry row of its own, which is a state the recipient can do nothing
    # about; refusing to send would be a new failure rather than a fix.
    # MEASURED before changing it: ZERO ellipsis-bearing values exist in any of
    # message_log.from_agent, message_log.to_agent or agent_status.display_name
    # across 6,174 rows, so this arm has never actually reached the broker — the
    # defect is latent, and this closes it before it fires rather than after.
    return "<unknown>"


def _post_event(payload: dict) -> Tuple[int, dict]:
    """Single-shot POST; returns (status_code, parsed_response_dict).

    Raises URLError / connection errors to caller.
    """
    base = _broker_base_url()
    url = f"{base}/api/event"
    bearer = os.environ.get("BROKER_BEARER_TOKEN")
    headers = {
        "Content-Type": "application/json",
        "Idempotency-Key": payload["client_event_id"],
    }
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"
    body_bytes = json.dumps(payload).encode("utf-8")
    req = Request(url, data=body_bytes, headers=headers, method="POST")
    try:
        with urlopen(req, timeout=BROKER_TIMEOUT_S) as resp:
            try:
                data = json.loads(resp.read().decode("utf-8"))
            except (json.JSONDecodeError, ValueError):
                data = {}
            if not isinstance(data, dict):
                data = {}
            return resp.status, data
    except HTTPError as e:
        try:
            err_body = e.read().decode("utf-8")
        except Exception:
            err_body = ""
        try:
            err_json = json.loads(err_body) if err_body else {}
            if not isinstance(err_json, dict):
                err_json = {"raw": err_body}
        except (json.JSONDecodeError, ValueError):
            err_json = {"raw": err_body}
        return e.code, err_json


def _is_pre_v13_kind_rejection(err_json: dict) -> bool:
    """Detect a 422 indicating the broker doesn't recognize 'agent_message'
    in its `kind` enum (i.e., pre-v1.3 broker).

    Distinguishes from genuine validation errors (e.g., missing `to_agent`,
    out-of-range `since`/`limit`) where the broker IS v1.3+ but the caller
    has a real bug. Per peer D28 / contract.md v1.3 + handoff-broker-v1.3-cc-session-relay.md:

      pre-v1.3 broker rejects unknown kind via Pydantic Literal mismatch:
        {"loc": ["body", "kind"], "type": "literal_error",
         "msg": "Input should be 'user_msg', 'agent_reply', ..."}
      v1.3+ broker rejects missing to_agent via @model_validator:
        {"loc": ["body"], "type": "value_error",
         "msg": "Value error, to_agent is required for kind='agent_message'"}

    The discriminating signal is `"kind"` in `loc` AND `type == "literal_error"`.
    """
    try:
        details = err_json.get("error", {}).get("details", {})
        errors = details.get("errors", [])
        if not isinstance(errors, list):
            return False
        for err in errors:
            if not isinstance(err, dict):
                continue
            loc = err.get("loc") or []
            err_type = err.get("type", "")
            if "kind" in loc and err_type == "literal_error":
                return True
        return False
    except Exception:
        return False


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")

    p = argparse.ArgumentParser(
        prog="relay-send",
        description="Send an inter-session message via the local broker.",
    )
    p.add_argument("--to", required=True, help="Recipient name or session_id UUID")
    p.add_argument("--text", required=True, help="Message body")
    p.add_argument("--reply-to", type=int, default=None,
                   help="Optional event_id of an earlier message this replies to")
    args = p.parse_args()

    registry = _load_registry()
    sender_sid = _resolve_sender_session_id()
    # APPENDIX 3 sender-side gate: must run /relay-name first.
    _check_sender_gate(sender_sid, registry)
    # Returns (recipient_friendly_name, recipient_sid_or_None) per ACTIVE PLAN
    # friendly-name primitive — sid is informational only (used in receipt).
    recipient_name, recipient_sid = _resolve_recipient(args.to, registry, sender_sid)
    # Non-blocking staleness advisory: if the resolved recipient's registry
    # last_seen_ts is >12h old, warn the user (may be a stale same-name match)
    # but proceed to send anyway.
    if recipient_sid:
        _entry = registry.get(recipient_sid)
        _last_seen = _entry.get("last_seen_ts") if isinstance(_entry, dict) else None
        if _last_seen is not None:
            try:
                _age_ms = int(time.time() * 1000) - int(_last_seen)
                if _age_ms > 12 * 60 * 60 * 1000:
                    _age_h = _age_ms // (60 * 60 * 1000)
                    sys.stderr.write(
                        f"relay-send: WARNING - recipient '{recipient_name}' "
                        f"(sid {recipient_sid[:8]}) last seen {_age_h}h ago; "
                        f"verify this is the intended live session.\n"
                    )
            except (TypeError, ValueError):
                pass
    sender_name = _resolve_sender_name(sender_sid, registry)

    # Sanitize lone Unicode
    # surrogates in user input (Windows clipboard paste edge case can produce
    # unpaired surrogate halves in sys.argv). Round-trip through utf-8/replace
    # converts any lone surrogate to U+FFFD (visible replacement char). Outer
    # json.dumps at L258 uses default ensure_ascii=True so this is defense-in-
    # depth — guarantees the str body never carries surrogates regardless of
    # downstream encoding strictness.
    args.text = args.text.encode("utf-8", errors="replace").decode("utf-8")

    envelope = {
        "text": args.text,
        "sender_name": sender_name,
    }
    if args.reply_to is not None:
        envelope["reply_to_event_id"] = args.reply_to

    client_event_id = str(uuid.uuid4())
    posted_at = int(time.time() * 1000)

    payload = {
        "from_agent": sender_name,
        "to_agent": recipient_name,
        "channel": "#cc-session-relay",
        "kind": "agent_message",
        "content": json.dumps(envelope, ensure_ascii=False),
        "ts": posted_at,
        "client_event_id": client_event_id,
    }

    try:
        status, resp = _post_event(payload)
    except URLError as e:
        # #24 broker-down watchdog: the POST never reached the broker, so NO
        # delivery row exists and the recipient-side reaper (#18) can NEVER
        # alert. Persist (never lose) -> unify the down-state -> alert the operator
        # directly on Discord (the only path alive during a local-broker
        # outage), deduped by a cooldown stamp. Best-effort throughout.
        base = _broker_base_url()
        preview = args.text[:80] + ("..." if len(args.text) > 80 else "")
        persisted = False
        if broker_client is not None:
            try:
                broker_client._enqueue_outbox("/api/event", payload, client_event_id, posted_at)
                persisted = True
            except Exception:
                persisted = False
        try:
            _mark_broker_down(f"send URLError: {e.reason}")
        except Exception:
            pass
        alerted = False
        try:
            if _sender_alert_due():
                # Claim the cooldown window BEFORE posting so concurrent sends at
                # outage onset don't each fire a duplicate alert; release the claim
                # if the post fails so the next send is due again (fail toward
                # alerting). The whole block is wrapped so a helper raising (e.g. a
                # malformed .env) can NEVER break the receipt/exit contract below.
                _stamp_sender_alert()
                content = (
                    "⚠ relay send undelivered · "
                    + time.strftime("%Y-%m-%d %H:%M:%S") + " · broker unreachable at "
                    + _sanitize_alert_field(base, 80) + " · queued "
                    + _sanitize_alert_field(sender_name, 64) + " -> "
                    + _sanitize_alert_field(recipient_name, 64) + ": (\""
                    + _sanitize_alert_field(preview, 120) + "\") · action: check/restart "
                    + "the broker (message queued, retries on recovery)"
                )
                if _post_discord_alert(content):
                    alerted = True
                else:
                    _clear_sender_alert_stamp()
        except Exception:
            alerted = False
        sys.stdout.write(json.dumps({
            "status": "queued_broker_down",
            "queued": persisted,
            "delivered": False,
            "client_event_id": client_event_id,
            "from_name": sender_name,
            "to_name": recipient_name,
            "recipient_session_id": recipient_sid,
            "text_preview": preview,
            "broker_url": base,
            "discord_alerted": alerted,
            "error": f"broker unreachable at {base}: {e.reason}",
            "hint": (
                "broker unreachable; message "
                + ("persisted to the relay outbox and will retry on broker recovery"
                   if persisted else "NOT persisted (broker_client unavailable) — re-send when the broker is back")
                + ("; operator alerted on Discord" if alerted else "")
            ),
        }))
        sys.stdout.write("\n")
        sys.exit(4)
    except Exception as e:
        sys.stdout.write(json.dumps({
            "error": f"unexpected {type(e).__name__}: {e}",
            "status": 0,
        }))
        sys.stdout.write("\n")
        sys.exit(2)

    if 200 <= status < 300:
        # #24 recovery trigger #1: the broker just proved reachable (this POST 2xx'd)
        # -> drain a BOUNDED prefix of any sends queued during a prior outage. Bounded
        # (<=K entries, single-shot) so a large backlog never stalls the send before
        # the receipt (bounded foreground work); the rest drain on later sends + the
        # MCP-reconnect background flush. NOTE: this new message was already posted
        # above, so queued replays land AFTER it -> replay is UNORDERED relative to a
        # concurrent live send (documented in SKILL.md).
        if broker_client is not None:
            try:
                if os.path.getsize(str(broker_client._outbox_path())) > 0:
                    broker_client.flush_outbox(
                        max_entries=SENDER_FLUSH_MAX, attempts=1, timeout_s=SENDER_FLUSH_TIMEOUT_S)
            except Exception:
                pass
        preview = args.text[:80] + ("..." if len(args.text) > 80 else "")
        receipt = {
            "event_id": resp.get("event_id"),
            "posted_at": posted_at,
            "recipient_session_id": recipient_sid,
            "recipient_name": recipient_name,
            "from_name": sender_name,
            "to_name": recipient_name,
            "text_preview": preview,
            "summary": (
                f"eventid {resp.get('event_id')} | from {sender_name} -> "
                f"to {recipient_name} ({(recipient_sid or '?')[:8]}) | {preview}"
            ),
            "client_event_id": client_event_id,
        }
        sys.stdout.write(json.dumps(receipt))
        sys.stdout.write("\n")
        sys.exit(0)

    if status == 422:
        # Distinguish pre-v1.3 broker (rejects 'agent_message' kind itself)
        # from a v1.3+ broker reporting a real validation failure (e.g., the
        # caller built a payload missing to_agent, or out-of-range since/limit).
        if _is_pre_v13_kind_rejection(resp):
            receipt = {
                "stub_mode": True,
                "hint": (
                    "Broker is pre-v1.3 — it doesn't recognize "
                    "kind='agent_message'. Restart the broker to pick up the "
                    "v1.3 code (see peer's handoff-broker-v1.3-cc-session-relay.md)."
                ),
                "broker_status": status,
                "broker_response": resp,
                "would_have_posted": {
                    "from_agent": sender_name,
                    "to_agent": recipient_name,
                    "kind": "agent_message",
                    "client_event_id": client_event_id,
                    "posted_at": posted_at,
                    "recipient_session_id": recipient_sid,
                    "recipient_name": recipient_name,
                    "from_name": sender_name,
                    "to_name": recipient_name,
                },
            }
            sys.stdout.write(json.dumps(receipt))
            sys.stdout.write("\n")
            sys.exit(0)

        # Real validation error from a v1.3+ broker — caller bug, surface it.
        sys.stdout.write(json.dumps({
            "error": (
                "broker rejected POST /api/event with HTTP 422 "
                "(validation error from v1.3+ broker)"
            ),
            "status": status,
            "broker_response": resp,
        }))
        sys.stdout.write("\n")
        sys.exit(3)

    # Other HTTP failure
    sys.stdout.write(json.dumps({
        "error": f"broker rejected POST /api/event (status {status})",
        "status": status,
        "broker_response": resp,
    }))
    sys.stdout.write("\n")
    sys.exit(3)


if __name__ == "__main__":
    main()
