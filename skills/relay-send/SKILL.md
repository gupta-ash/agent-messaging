---
name: relay-send
description: |
  Send a directed message to another registered relay session via the local
  broker from OMP, Claude Code or Codex.

  Use when the user asks to talk to a sibling session — phrases like
  "send to <session>: <text>", "@<session> <text>", "/relay send <to> <text>",
  or natural-language equivalents like "tell research-fork-A about X".
  Delivery is handled by the recipient's own host adapter.

  Claude Code resolves recipients through `~/.claude/relay/registry.json`.
  Codex uses the broker-owned identity and mailbox established by the R5B
  launcher. UUIDs are also accepted directly.
  OMP uses its existing session-bound native omp-relay MCP tools.
---

# relay-send

Send an inter-session message through the local broker. Select the active
host's adapter below; never impersonate another harness.

## Host selection

OMP must use only the OMP native adapter.
Codex must use only the Codex adapter.
Claude Code must use only the Claude Code adapter.
Never cross-fallback between host adapters.

### OMP native adapter

Call the mounted `send` tool from the existing `omp-relay` MCP server,
exposed as `mcp__omp_relay_send`. If mounted as an XD device, read
`xd://mcp__omp_relay_send` for its schema, then write the argument JSON there.
This is the same native tool, not a shell command or another transport.

- Map `--to` -> `to_agent` and `--text` -> `content`. Omit optional `channel`
  and `kind` unless requested; use only values accepted by the native schema.
- The native tool has no `reply_to_event_id`. If `--reply-to` was requested,
  fail loud before sending; do not silently discard the threading request.
- Return the native result or ToolError unchanged. Its event ID is a string;
  do not manufacture the Claude script's numeric-ID/posted-at receipt.
  Storing an event does not prove the recipient read or replied to it.
- OMP's adapter/extension owns session identity, delivery and internal ACK.
  Do not call Claude's `peer_reply` or `broker_ack`, supply another agent ID,
  set `CLAUDE_CODE_SESSION_ID`, read the Claude registry, run either host's
  launcher/script, or call broker HTTP directly.
- If this native tool is absent, stop and report missing managed OMP relay
  discovery. Do not install an adapter, restart a service or cross-fallback.

### Claude Code adapter

Run the existing `python ~/.agents/skills/relay-send/scripts/send.py`
command documented below. This keeps the current Claude Code registry,
outbox/watchdog, JSON receipt (including `"event_id"`), and **Exit codes**
unchanged.

**This is the `peer_reply` / send path — it delivers an actual MESSAGE the peer
receives.** It is NOT `broker_ack`. `broker_ack` only confirms that YOU received
a message (advances your local receive cursor) and sends nothing to anyone. If a
peer is waiting to hear from you, use this skill (or the `peer_reply` MCP tool);
to confirm a message you received so it stops being re-delivered, use `broker_ack`.

### Codex adapter

Call the `send` tool from the session-scoped `relay` MCP server; in Codex
tool notation this is `mcp__relay__send`.

- Map `--to` -> `to_agent` and `--text` -> `content`.
- Use `channel="#codex-relay"` and `kind="agent_message"` unless the caller
  explicitly requested another R5B-supported kind.
- The R5B tool does not expose `reply_to_event_id`. If the caller supplied
  `--reply-to`, fail loud before sending rather than silently dropping the
  threading request.
- Return the MCP result or ToolError unchanged. Do not invent legacy-script
  receipt fields that the broker did not return.
- The R5B launcher binds a stable Codex agent ID and the broker-owned relay
  name. Never set or fabricate `CLAUDE_CODE_SESSION_ID`, pass an agent ID, read
  the Claude registry, run the Claude Python entry point, or implement a second
  transport. Do not call the broker HTTP API directly.
- If the session-scoped `relay` MCP server or its `send` tool is absent,
  stop and report that this Codex session must be launched through
  `<USERPROFILE>/.agents/services/broker/scripts/start-codex-relay-session.ps1`
  under PowerShell 7 with the session's explicit private `-StateDirectory`;
  retain the prior directory for existing identities. Do not fall back to the Claude adapter.

## Claude Code invocation

```bash
python ~/.agents/skills/relay-send/scripts/send.py \
  --to "<name-or-uuid>" \
  --text "<message body>" \
  [--reply-to <event_id>]
```

| Arg | Required | Notes |
|---|---|---|
| `--to`       | yes | Friendly name (resolved via `~/.claude/relay/registry.json`) OR a session_id UUID4 (36-char canonical form). |
| `--text`     | yes | Message body. Will be wrapped in a small JSON envelope `{text, sender_name, reply_to_event_id?}` for the broker. |
| `--reply-to` | no  | Optional `event_id` of an earlier inter-session message this reply threads under. Stored inside the envelope. |

## Claude Code output

On success: prints a JSON receipt to stdout, exit 0:

```json
{"event_id": 1247, "posted_at": 1746404783000,
 "recipient_session_id": "c0000000-0000-4000-8000-000000000003",
 "recipient_name": "research-fork-A"}
```

If the broker rejects `kind='agent_message'` itself (HTTP 422 with a Pydantic
Literal mismatch on the `kind` field), the script surfaces the 422 with a
`stub_mode: true` marker (exit 0) — indicates the broker lacks `agent_message`
support and needs a restart/upgrade.

If the call has a real validation failure (e.g., missing `to_agent`,
out-of-range `since`/`limit`), the script reports a real failure receipt and
exits 3 — no stub_mode mask.

On other failures: prints `{"error": "...", "status": <code>}` to stdout and
exits non-zero.

**Broker unreachable (broker down) — #24 sender-side watchdog.** If the broker
cannot be reached (`URLError`), the message is NOT lost: it is persisted to the
relay outbox (`~/.claude/channels/discord/outbox.jsonl`, replayed automatically
when the broker recovers). If an alert destination and token were explicitly
configured, the operator is alerted on Discord with a 5-minute cooldown. No
notification destination is configured by default. The script prints a
`queued_broker_down` receipt and exits **4**:

```json
{"status": "queued_broker_down", "queued": true, "delivered": false,
 "client_event_id": "…", "to_name": "research-fork-A",
 "discord_alerted": false, "broker_url": "http://127.0.0.1:8787",
 "hint": "broker unreachable; message persisted to the relay outbox and will retry on broker recovery"}
```

Exit codes: **0** = delivered (or pre-v1.3 stub); **2** = sender-gate not set /
unexpected error (nothing persisted); **3** = broker validation failure (caller
bug); **4** = broker unreachable, message QUEUED for retry (not delivered).

Queued sends replay automatically once the broker recovers — on your next send,
via the receive hook, and via the MCP-reconnect background flush — a **bounded**
number per turn so recovery never stalls a turn (a large backlog drains over
several turns). **Replay is not strictly ordered** relative to a live send during
recovery: a message queued while the broker was down may arrive *after* a message
you send once the broker is back.

## Claude Code examples

```bash
# By friendly name
python ~/.agents/skills/relay-send/scripts/send.py \
  --to "research-fork-A" \
  --text "the result of your fork: <stuff>"

# By UUID, threaded reply
python ~/.agents/skills/relay-send/scripts/send.py \
  --to "c0000000-0000-4000-8000-000000000003" \
  --text "here is the follow-up data" \
  --reply-to 1247
```

## Claude Code environment

- `BROKER_BASE_URL` — explicit broker URL, authoritative when set. Otherwise read the invoking user's `~/.agents/installations/agent-messaging/connection.json`; malformed existing configuration fails loudly. Only an absent file uses the portable `http://127.0.0.1:8787` default.
- `BROKER_BEARER_TOKEN` — optional bearer token (required when broker bound non-loopback).
- `CLAUDE_CODE_SESSION_ID` — sender session_id source (per F-CC12; CC injects this env var into every subprocess at process-spawn time). Required. If unset, script fails loud (per C-13: no auto-derive, no pid-walk fallback). When running outside CC, set explicitly.
- `DISCORD_BOT_TOKEN` — optional credential for configured broker-down alerts. If unset, read from `~/.claude/channels/discord/.env` (honoring `DISCORD_STATE_DIR`). An absent token skips the alert without affecting persistence/retry.
- `BROKER_ALERT_DISCORD_CHANNEL_ID` — explicit Discord destination for broker-down alerts; no default. Missing or blank means no notification is sent and no token is read. Shared with the broker-side escalation reaper.
- `BROKER_SENDER_ALERT_COOLDOWN_S` — minimum seconds between broker-down Discord alerts (dedup during one outage); defaults to 300.
