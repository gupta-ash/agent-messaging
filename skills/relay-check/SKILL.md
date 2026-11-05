---
name: relay-check
description: |
  On-demand inspection of inter-session messages in the local broker. Read-only
  inspection for the current OMP, Claude Code or Codex relay identity. In
  Claude Code it can also query cross-recipient history with --all. Use this
  when you need to check whether a teammate replied or inspect relay traffic.

  Use when the user asks "did <session> reply yet?", "show me recent inter-agent
  traffic", "what messages did I get from <session>?", or phrases like
  "/relay check", "check messages", "show inbox", "inspect broker traffic".

  Read-only by design. It does not acknowledge a delivery or advance a receive
  cursor. Canonical delivery remains host-native: the OMP adapter/extension,
  Claude receive hooks or the R5B Codex adapter.
---

# relay-check

On-demand inspection of inter-session messages through the current host's broker adapter.

Replaces the prior pattern of raw `sqlite3 broker/coordination.db "SELECT ... FROM message_log"` for message inspection (raw SQL bypasses broker validation; the skill is the canonical path).

## Host selection

OMP must use only the OMP native adapter.
Codex must use only the Codex adapter.
Claude Code must use only the Claude Code adapter.
Never cross-fallback between host adapters.

### OMP native adapter

Call the mounted `check` tool from the existing `omp-relay` MCP server,
exposed as `mcp__omp_relay_check`. If mounted as an XD device, read
`xd://mcp__omp_relay_check` for its schema, then write the argument JSON there.
This is the same native tool, not a shell command or another transport.

- Map an explicit `--since` -> `after` as a decimal **string**, and `--limit`
  -> `limit` as a number. Omit unspecified values to retain native defaults.
- This reads the bound OMP identity's projected health, queue and delivery
  state; it does not acknowledge deliveries or advance a receive cursor.
  It is not Claude's cross-recipient historical-message browser.
- Claude-only `--tail`, `--kind`, `--from`, `--all` and `--format` views are
  unsupported. Fail loud if the request requires one; do not change its query.
- Return the native result or ToolError unchanged. Do not wrap it in Claude's
  historical `messages` payload or infer receipt from a read-only check.
- OMP's adapter/extension owns identity, delivery and internal ACK. Do not
  call `broker_ack`, supply an agent ID, set `CLAUDE_CODE_SESSION_ID`, inspect
  the Claude registry, run another host's script/launcher or call broker HTTP.
- If this native tool is absent, stop and report missing managed OMP relay
  discovery. Do not install an adapter, restart a service or cross-fallback.

### Claude Code adapter

Run the existing `python ~/.agents/skills/relay-check/scripts/check.py`
command documented below. This preserves the current Claude Code JSON payload
(including `"messages"`), text rendering, filters, cursor behavior, and **Exit codes** unchanged.

### Codex adapter

Call the `check` tool from the session-scoped `relay` MCP server; in Codex
tool notation this is `mcp__relay__check`.

- Map `--since` -> `after` and `--limit` -> `limit`. With neither value
  supplied, use the R5B defaults (`after=0`, `limit=20`).
- The R5B check returns this stable Codex identity's broker status and
  outstanding deliveries. It is read-only and does not acknowledge them.
- The R5B tool does not implement the Claude-only `--tail`, `--kind`, `--from`,
  `--all`, or `--format` views. If the request depends on one of those options,
  fail loud rather than silently changing the query.
- Return the MCP result or ToolError unchanged. Do not fabricate the Claude
  script's historical-message wrapper around the R5B delivery response.
- The R5B launcher binds the stable Codex agent ID. Never set or fabricate `CLAUDE_CODE_SESSION_ID`,
  pass an agent ID, read the Claude registry, run the Claude Python entry point,
  or implement a second transport. Do not call the broker HTTP API directly.
- If the session-scoped `relay` MCP server or its `check` tool is absent,
  stop and report that this Codex session must be launched through
  `<USERPROFILE>/.agents/services/broker/scripts/start-codex-relay-session.ps1`
  under PowerShell 7 with the session's explicit private `-StateDirectory`;
  retain the prior directory for existing identities. Do not fall back to the Claude adapter.

## Claude Code invocation

```bash
python ~/.agents/skills/relay-check/scripts/check.py \
  [--since <event_id> | --tail <N>] \
  [--kind <kind|all>] \
  [--limit <N>] \
  [--from <name-or-uuid>] \
  [--all] \
  [--format json|text]
```

**Default when neither `--since` nor `--tail` is passed: `--tail 20`** (newest 20 messages per kind; inbox-viewer semantics matching `tail` / `journalctl -n` / `git log -n`). Explicit `--since 0` still returns full history for full-dump callers.

| Arg | Default | Notes |
|---|---|---|
| `--since`  | (none; see Default note) | Cursor (event_id strictly greater than this). Pass `--since 0` explicitly for full history (full-dump cron use case). Mutually exclusive with `--tail`. |
| `--tail`   | **20** when neither `--since` nor `--tail` is passed; else (none) | Return the NEWEST N messages (per kind in --kind=all mode; or N total in single-kind mode). Walks the message_log forward in pages of 100 to discover the tail; cost ~ceil(N_total/100) HTTP calls per kind, capped at 100 pages. Mirrors SQL `ORDER BY event_id DESC LIMIT N` semantics. Mutually exclusive with `--since`. |
| `--kind`   | `all`       | Single kind (`agent_message` / `agent_reply` / `handoff`) OR `all` → fans out across all 3 inter-session kinds, merges by event_id, dedupes. Per contract.md v1.8 `INTER_SESSION_MESSAGE_KINDS`. |
| `--limit`  | 20          | Per-kind page size (broker contract: 100 cap on per-session endpoint, 200 cap on aggregate). Default 20. Used in `--since` mode; ignored in `--tail` mode (which uses internal page size 100). |
| `--from`   | (none)      | Client-side filter on sender. Matches against `from_agent` field (UUID OR friendly name via registry lookup). In `--tail` mode the filter is applied inline as pages are walked (so `--tail N --from X` returns "newest N from X", not "newest N globally then filter"). In `--since` mode the filter is applied to the single page returned. |
| `--all`    | false       | Use `GET /api/agent-messages` (cross-recipient aggregate, v1.4 endpoint) instead of `GET /api/agent/{sid}/messages` (per-session). |
| `--format` | `json`      | `json` = raw response + metadata. `text` = human-readable table. |

## Claude Code broker-API contract caveat (read this if you expect ALL events)

Both `GET /api/agent/{sid}/messages` (default mode) and `GET /api/agent-messages` (--all mode) only surface message_log rows where `to_agent` is set. **Broadcast events with `to_agent IS NULL` are NOT accessible via either endpoint.** For raw broadcast inspection (rare; mostly status pings or unrouted handoffs), the only path is direct SQL: `sqlite3 broker/coordination.db "SELECT * FROM message_log WHERE to_agent IS NULL ORDER BY event_id DESC LIMIT N"`. This skill does not bridge that gap.

## Claude Code output

**JSON (default):**

```json
{
  "session_id": "39f2451d-...",
  "endpoint": "/api/agent/<sid>/messages",
  "mode": "tail",
  "since": 0,
  "tail": 20,
  "kinds_polled": ["agent_message", "agent_reply", "handoff"],
  "messages": [
    {"event_id": 1247, "ts": 1746404783000,
     "from_agent": "subagents-uuid", "to_agent": "broker-uuid",
     "kind": "agent_message", "content": "..."}
  ],
  "count": 3,
  "filtered_by_from": null
}
```

**Text (--format text):**

```
3 messages since event_id 1200:
  1247  2026-06-04 14:32  agent_message   subagents → broker
        "ack the bug fix"
  1248  2026-06-04 14:33  handoff         broker → research
        "{...envelope...}"
  ...
```

## Claude Code exit codes

| Code | Meaning |
|---|---|
| 0 | Success (incl. empty result set). |
| 1 | Caller error: unresolved session_id (CLAUDE_CODE_SESSION_ID env var unset), invalid `--kind` (e.g. `error` forbidden per contract.md v1.8 § 8), invalid arg combination (e.g. `--tail` + `--since`), or out-of-range values. |
| 2 | Broker unreachable / 4xx / 5xx (the underlying `broker_client._get()` collapses URLError + HTTPError into a single `None` return; this skill cannot distinguish them). Surfaces to user — does NOT silent-degrade like receive hooks. |

## Claude Code examples

```bash
# Default (no args): newest 20 messages addressed to me, all 3 kinds
python ~/.agents/skills/relay-check/scripts/check.py --format text

# Explicit full history (old default; for full-dump cron callers)
python ~/.agents/skills/relay-check/scripts/check.py --since 0 --limit 5

# Newest 5 messages addressed to me, all 3 kinds (mirrors SQL DESC LIMIT 5)
python ~/.agents/skills/relay-check/scripts/check.py --tail 5 --format text

# Did my teammate "subagents" reply yet? (newest 5 from them, all kinds)
python ~/.agents/skills/relay-check/scripts/check.py --tail 5 --from subagents --format text

# Cross-recipient view: newest 10 handoffs across all sessions (DIRECTED only;
# see broker-API caveat above for broadcast handoffs with to_agent IS NULL)
python ~/.agents/skills/relay-check/scripts/check.py --all --kind handoff --tail 10 --format text

# Forward poll from a specific cursor (e.g., resume from receive-hook cursor)
python ~/.agents/skills/relay-check/scripts/check.py --since 1500 --format text
```

## Claude Code when NOT to use this skill

- **Default message delivery** — that's the receive hooks at `~/.claude/hooks/relay/receive_*.py`. They auto-inject on UserPromptSubmit / SessionStart / PostToolUse / Notification. This skill is for ON-DEMAND inspection, not canonical receipt.
- **Polling kind='error'** — FORBIDDEN per contract.md v1.8 § 8 (auto-injected on every UserPromptSubmit + SessionStart via `~/.claude/hooks/relay/receive_errors.py`). Double-polling wastes broker round-trips. This skill's `--kind` arg rejects `error`.
- **Cursor management** — out of scope. This skill does NOT write or advance the receive-hook cursor. If you read 20 messages here, the next UserPromptSubmit will still auto-inject the same 20 (by design — non-destructive read).

## Claude Code environment

- `BROKER_BASE_URL` — explicit broker URL, authoritative when set. Otherwise read the invoking user's `~/.agents/installations/agent-messaging/connection.json`; malformed existing configuration fails loudly. Only an absent file uses the portable `http://127.0.0.1:8787` default.
- `BROKER_BEARER_TOKEN` — optional bearer token (required when broker bound non-loopback).
- `CLAUDE_CODE_SESSION_ID` — session_id source (per F-CC12; CC injects this env var into every subprocess at process-spawn time). Required. If unset, script fails loud (per C-13: no auto-derive, no pid-walk fallback). When running outside CC, set explicitly.
