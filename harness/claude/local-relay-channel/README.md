# Claude Code relay channel

Optional Claude Code MCP plugin for agent-messaging. It subscribes to the broker's
SSE stream and pushes directed `agent_message`, `agent_reply` and `handoff` events
into the owning session through `notifications/claude/channel`.

## Requirements and activation

- A broker reachable at the explicitly configured or installed selected URL.
- Python with `requirements-mcp.txt` installed in the interpreter used by the MCP
  registration, plus the shared relay library at `~/.claude/hooks/relay/`.
- A Claude Code version and account configuration that supports development
  channels. Channel availability is controlled by Claude Code.
- A user-set relay name. Claim it with `/relay-name`; the plugin does not invent
  an identity from a directory, conversation title or process ID.

Use the repository's public installer with the Claude harness selected. Installing
files does not register receive hooks or activate channels by default. When you
explicitly want a channel-enabled launch, the optional wrappers run:

```text
claude --dangerously-load-development-channels server:local-relay-channel
```

`cc_channel` uses the Claude command on PATH on POSIX; `cc_channel.bat` does the
same on Windows. Both forward arguments. The plugin's `.mcp.json` launches
`python ${CLAUDE_PLUGIN_ROOT}/server.py`; choose the intended Python interpreter
in your explicit MCP registration if `python` on PATH is not that interpreter.

## Identity and delivery

The adapter reads `sessionId` from the direct parent Claude Code process's
`~/.claude/sessions/<pid>.json` record, falling back to `CLAUDE_CODE_SESSION_ID`
only when that record cannot be read. A process ID locates the record and never
becomes a relay identity. The session record's mutable conversation title is not
a routing name.

Routing uses the explicitly set friendly name in the relay registry or daemon
job state. A nameless session remains inactive until a valid name is available.
The adapter watches name/session changes, reconnects the stream when needed, and
keeps separate SSE-resume and confirmed-delivery cursors under `~/.claude/relay/`.
Initial startup without a cursor starts live-only rather than replaying all
broker history. Use `/relay-check` for read-only inspection of historical traffic.

The tool surface separates two different actions:

- `peer_reply(to_session_id, content, channel?)` sends a message a peer receives.
- `broker_ack(event_ids)` confirms receipt locally and mirrors eligible delivery
  acknowledgements to the broker. It sends no reply to the peer. Returned per-ID
  reasons explain refusals; subagents must not acknowledge an orchestrator's mail.

Inspection with `/relay-check` does not acknowledge messages or advance delivery
cursors. Sender-side outage handling preserves queued messages; it is not proof
that a recipient read a message.

## Configuration and privacy

`BROKER_BASE_URL` explicitly selects the broker and takes precedence. Without
that environment variable, the Claude helpers read the invoking user's
`~/.agents/installations/agent-messaging/connection.json`. This installer-managed,
nonsecret file has exactly `schema_version: 1` and `broker_url` containing the
selected `http://127.0.0.1:<port>` URL. An existing unreadable or malformed file
fails loudly instead of contacting an older endpoint. Only an absent file uses
the portable source default `http://127.0.0.1:8787`.

The optional `CLAUDE_CODE_SESSION_ID` environment value is an identity fallback,
not an override of an available owning-process record. Shared HTTP helpers honor
`BROKER_BEARER_TOKEN` for their authenticated requests.

The public release includes no user registry, conversation history, credentials,
mailbox state or notification destination. Discord sender alerts require an
explicit `BROKER_ALERT_DISCORD_CHANNEL_ID` and a configured token. Broker-side
escalation is disabled unless explicitly enabled and configured.

## Troubleshooting

Diagnostics go to stderr; stdout is reserved for MCP JSON-RPC. Check Claude Code's
plugin logs for identity resolution, inactive-name and SSE reconnection messages.
Confirm the broker health endpoint, the selected interpreter's MCP dependencies,
the installed shared library and the session's explicit relay name. If channels
are unavailable in the host, report that prerequisite rather than silently
registering receive hooks or switching to another harness's identity.
