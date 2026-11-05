---
name: relay-name
description: |
  Set or update the human-readable broker relay name for the current OMP,
  Claude Code or Codex session.

  Use when the user asks to override the auto-generated session name —
  triggers like "/relay name <name>", "rename this session to <name>",
  "set my session name to <name>".
---

# relay-name

Set the current session's friendly relay name so other registered sessions can
address it through `relay-send`.

## Host selection

OMP must use only the OMP native adapter.
Codex must use only the Codex adapter.
Claude Code must use only the Claude Code adapter.
Never cross-fallback between host adapters.

### OMP native adapter

Call the mounted `name` tool from the existing `omp-relay` MCP server,
exposed as `mcp__omp_relay_name`. If mounted as an XD device, read
`xd://mcp__omp_relay_name` for its schema, then write the argument JSON there.
This is the same native tool, not a shell command or another transport.

- Map `--name` -> `relay_name`. This is the tool's only argument; OMP's
  existing native session binding supplies identity.
- A call claims or renames this session's relay name, taking it over from any
  previous owner by default. Only `relay_name` is required: no force flag or
  separate confirmation. The latest successful explicit claim owns the name;
  earlier queued messages stay bound to their original session identities.
- Return the native result or ToolError unchanged. Do not fabricate Claude's
  `session_id`, relay-name-file or reconnect receipt fields.
- Do not supply an agent ID, set `CLAUDE_CODE_SESSION_ID`, edit the Claude
  registry, run either host's script/launcher or call broker HTTP directly.
- If this native tool is absent, stop and report missing managed OMP relay
  discovery. Do not install an adapter, restart a service or cross-fallback.

### Claude Code adapter

Run the existing `python ~/.agents/skills/relay-name/scripts/set_name.py`
command documented below. This preserves the current Claude Code registry and
project-name-file writes, the `"session_id"` receipt, reclaim-by-default
behavior, and **exit 0** success contract unchanged.

### Codex adapter

Call the `name` tool from the session-scoped `relay` MCP server; in Codex
tool notation this is `mcp__relay__name`.

- Map `--name` -> `relay_name`.
- Use `takeover=true`: invoking this skill to set a name is explicit user authorization
  to reclaim it, matching the Claude Code public behavior.
- Return the MCP result or ToolError unchanged. The broker owns the Codex name;
  do not fabricate Claude-only `relay_name_file` or reconnect fields.
- The R5B launcher binds the stable Codex agent ID. Never set or fabricate `CLAUDE_CODE_SESSION_ID`,
  pass an agent ID, edit the Claude registry, run the Claude Python entry point,
  or implement a second transport. Do not call the broker HTTP API directly.
- If the session-scoped `relay` MCP server or its `name` tool is absent,
  stop and report that this Codex session must be launched through
  `<USERPROFILE>/.agents/services/broker/scripts/start-codex-relay-session.ps1`
  under PowerShell 7 with the session's explicit private `-StateDirectory`;
  retain the prior directory for existing identities. Do not fall back to the Claude adapter.

## Claude Code invocation

```bash
python ~/.agents/skills/relay-name/scripts/set_name.py --name "<new-name>"
```

| Arg | Required | Notes |
|---|---|---|
| `--name` | yes | New name. Max 64 chars. Allowed chars: alphanumeric, `.`, `_`, `-`. Spaces and other punctuation rejected. |
| `--reclaim` / `--force` | no | **Redundant post-C-25** — reclaim is now the DEFAULT. Accepted as a back-compat alias; has no additional effect. |

## Claude Code output

On success, prints to stdout (exit 0):

```json
{"session_id": "c0000000-0000-4000-8000-000000000003",
 "old_name": "previous-name",
 "new_name": "research-fork-A"}
```

On invalid name, exits 2 with diagnostic.
On unresolvable session_id (not running inside a CC session), exits 1 with diagnostic.
On lock-acquisition failure, exits **4** (registry left untouched).
A claim always SUCCEEDS (exit 0) and cleans up every other row holding the same name — see below. (The old exit-**3** conflict is RETIRED per C-25.)

## Claude Code examples

```bash
python ~/.agents/skills/relay-name/scripts/set_name.py --name "research-fork-A"
python ~/.agents/skills/relay-name/scripts/set_name.py --name "main_orch"
```

## Claude Code environment

- `CLAUDE_CODE_SESSION_ID` — session_id source (per F-CC12; CC injects this env var into every subprocess at process-spawn time). Required. If unset, script fails loud (per C-13: no auto-derive, no pid-walk fallback). When running outside CC, set explicitly.

## Claude Code reclaiming — the default behavior (C-25)

An explicit `/relay-name <name>` **is** the user's authorization to own the name. The claim always succeeds and cleans up every other row holding that name (scope = the NAME field, so stale non-user_set duplicate rows are removed too):

- **Provably-dead holder** (`shared._is_sid_live` is False — no live process, no non-terminal daemon job-state) → the row is **REMOVED** entirely. This is what stops duplicate rows accumulating across CC's sid churn.
- **Live holder** (foreground process, or non-terminal bg job-state) → its registry row is **demoted** to no-name and a loud **WARNING** is printed. A live background-agent daemon re-asserts its name on respawn and is NOT registry-deletable — that residual is surfaced in a NOTE, never hidden.

`_is_sid_live` **fails toward LIVE** on a probe error, so uncertainty → demote+warn, never a silent delete. The receipt's `reclaimed_from` lists the sids whose registry row was mutated (removed or demoted).

**No agent confirm step for the user's command.** Because typing `/relay-name` is the authorization, the agent does NOT need to AskUserQuestion first (the old exit-3 gate is gone). C-24's warn+confirm still applies only when an AGENT wants to claim a name that a genuinely LIVE session holds — but for the user's own explicit command, reclaim is the default.

**Name continuity across session-id changes:** a fresh registry row is nameless. SessionStart may display the explicit name remembered in `<project>/.relay-name.json`, but never inherits a sibling's name or automatically applies that reminder. Run `/relay-name <name>` to reclaim it explicitly. The channel adapter detects a valid user-set name and activates push without inventing an identity.
