# agent-messaging

A local, SQLite-backed coordination API with Claude Code, Codex and Oh My Pi (OMP) relay adapters. Durable message queues, stable session identities, binding epochs, application receipts, a read-only dashboard and optional explicitly configured Discord notifications are separate concerns. Sending a message is not proof its recipient read it.

Python distribution and console command: **`agent-messaging`**. The import package is `broker`; existing service root `.agents/services/broker`, database schemas, agent IDs and OMP tool identities are unchanged.

## Components

| Component | Behavior |
|---|---|
| Broker | Loopback HTTP/SSE API, SQLite queues/status, project-state readers and `/dashboard` |
| `relay-check` | Read-only queue/session/delivery inspection; no receive-cursor advance |
| `relay-name` | Bind/rename this session's relay name; use the invoking harness's tool |
| `relay-send` | Durable send; retains native errors and returned event identity |
| Claude | Six-file `local-relay-channel` plugin, shared relay library, selected skill fronts and user-scope MCP registration |
| Codex | Session-scoped `relay` MCP launcher/adapter; no global MCP registration or automatic session launch |
| OMP | Native `omp-relay` MCP server and extension; native tools remain `mcp__omp_relay_*` |

Read [operations](docs/operations.md), [architecture](docs/architecture.md), [release contract](docs/release.md) and [implementation/testing](docs/implementation.md).

## Prerequisites and boundaries

The scripted installer supports **Windows PowerShell 5.1+**, the actual invoking user's profile, and an existing **base Python 3.12** with pip/venv/ensurepip. It never installs Python, a harness, Bun, Node, or machine policy. Codex's session launcher requires **PowerShell 7**. Select only harnesses already installed. No hook activation, enterprise channel policy or organization entitlement setup is included.

For OMP, explicitly select its existing public CLI, actual selected agent directory, base Python and two private, separate state/runtime directories. The selected OMP Python must already have the dependencies in `requirements-omp-mcp.txt` (`mcp`, `httpx`, `cryptography`, `PyYAML`); this installer does not modify a shared base interpreter. OMP's ordinary upstream/package-manager updates are independent. `OmpProfile` is optional and is never defaulted to a private profile. `OmpAgentRoot` is the actual agent directory for that profile, not a parent from which the installer guesses a profile layout.

Core Python source can be used on other supported Python platforms; the POSIX launcher and systemd operator are explicit source-use utilities, **not a tested Linux/macOS harness installer**.

## Exact-version install or upgrade

Choose an exact published release tag and an absolute user-owned WorkRoot outside source, installation and state. Download **that release's `install.ps1` asset** through GitHub Releases. Do not pipe downloaded text into `iex`. A changed frontdoor requires explicitly downloading the new release script; the script never self-replaces. Public downloads require HTTPS, not private-repository access or a token.

```powershell
$Version = '<exact-release-tag>'
$Work = Join-Path $env:USERPROFILE 'installer-work/agent-messaging-operation'
$Python = '<absolute-existing-base-python.exe>'
& ./install.ps1 -Version $Version -WorkRoot $Work -PythonExe $Python -Harness @('claude','codex') -Mode Check
& ./install.ps1 -Version $Version -WorkRoot $Work -PythonExe $Python -Harness @('claude','codex') -Mode Install
& ./install.ps1 -Version $Version -WorkRoot $Work -Mode Verify
```

Invoke the script directly for PowerShell arrays; do not pass a comma-containing native `-File` argument. `Harness @('none')` (fresh default) installs only the service; it cannot be combined with a harness. Omitted selectors preserve existing selections. Explicit additions expand their union; omission is not uninstall. `InstallRoot`, `PythonExe`, `Port` (fresh default 8787), `UserRoot` and OMP identity/root arguments must agree with the retained record on upgrades/Verify; state relocation is not an upgrade. Existing private `broker.env` supplies its port during explicit adoption and is never overwritten. There is no `latest`, downgrade or automatic retry.

`Check` may download wheels/archive and use isolated WorkRoot config copies, but makes no live change. `Install` repeats complete preflight before its first live write. `Verify` is a separate **read-only, no-network** operation using retained metadata and actual installed files, even after download/extraction is removed. It does not start MCP servers, query a service, repair prerequisites or prove activation.

OMP example (replace every placeholder with an existing validated local selection):

```powershell
& ./install.ps1 -Version $Version -WorkRoot $Work -PythonExe $Python -Harness @('omp') `
  -OmpCommand '<absolute-omp-command>' -OmpAgentRoot '<absolute-selected-agent-directory>' `
  -OmpPythonExe '<absolute-existing-base-python.exe>' `
  -RelayStateRoot (Join-Path $env:USERPROFILE '.omp-relay/state') `
  -RelayRuntimeRoot (Join-Path $env:USERPROFILE '.omp-relay/runtime') -Mode Check
```

A fresh installation creates only these two explicitly selected private roots. Adoption requires both to exist, so it cannot silently regenerate session identity. The installer uses OMP's public typed config API against an isolated copy, then atomically merges only the selected extension and `mcpServers["omp-relay"]`. It preserves unrelated servers, lists, order, disabled entries and settings. A foreign same-name registration or disabled selected relay is a conflict, not permission to override it.

## Existing installations, ownership and recovery

Use **`-AdoptExisting`** for the first takeover of recognizable same-user code without a new receipt. Prior code must match an explicit known source hash or selected payload; arbitrary same-name code/registrations are refused. Every overwritten owned file is privately backed up under the operation WorkRoot. Backups and failure diagnostics are retained preservation, not disposable extraction; their location is returned in private operation output. The private `operation.json`/`failure.json` map original paths to backups and code/configuration/environment kinds; only immutable code has old/candidate digest bindings. Do not purge them with downloads. Follow the [bounded manual code-only recovery procedure](deploy/RUNBOOK.md), never a wholesale configuration/venv/database restore.

Only explicit declaration members are installed, never a home-tree mirror. Locally edited immutable code is a conflict, including on the same version. Unknown neighbors, credentials, `broker.env`, DB/WAL/SHM/journals, logs, audit, sessions, stable IDs and Task Scheduler registrars are preserved. Obsolete code is removed only when explicitly hash-bound in `deploy/retirements.json`, previously owned and still unchanged. No database or automatic code rollback occurs after a partial failure.

Claude's existing same-user `.claude/skills -> .agents/skills` and `.claude/hooks -> .agents/hooks` junctions are preserved, not replaced. Without the shared skills junction, only selected native fronts are exposed in `.claude/skills`; helpers stay canonical. The plugin and library are installed before registration. A fresh registration uses the installed venv Python, with MCP provisioned/probed there; a correct adopted registration using the explicitly selected base Python is retained only after that actual interpreter's MCP prerequisite is checked:

```powershell
claude mcp add --scope user local-relay-channel -- <installed-venv-python> <installed-plugin-server.py>
```

The selected loopback URL is retained in nonsecret `.agents/installations/agent-messaging/connection.json`. Claude helpers/plugin use it when no explicit `BROKER_BASE_URL` is configured, so a nondefault port is not silently replaced by 8787. A conflicting provisioned URL refuses; no settings/credential value is overwritten.

A correct registration is preserved, a foreign one refuses. Copying files or writing the registration is not a real MCP initialize/tools-list readiness result; use a fresh selected-harness session for that check.

## Activation and autostart are separate choices

Install never starts/stops a service or changes autostart by default. A running broker blocks executable changes. To authorize this specific stop/start operation, add `-Activate` to **Install** and type the displayed component/version/current PID/root exactly. The installer uses the owned stop/start operators without `-Force`, waits on the detached launcher handle (never drains its inherited output pipe), and checks a new PID/start identity, selected root and real health. Failure is reported separately from installed bytes; an old healthy listener is not new-code activation. **First adoption of a legacy relative `python server.py` process requires independently proving PID/start/cwd/root and explicitly stopping that owned installation without Force first.** A bare argv/healthy port cannot prove its root; Activate cannot override that refusal. See the [exact adoption prerequisite](deploy/RUNBOOK.md).

Manual activation after installation:

```powershell
& "$env:USERPROFILE/.agents/services/broker/scripts/restart-broker.ps1" -Port 8787
```

Use the actual retained port. Do not pipe/drain the detached restart script. For a scripted invocation use `Start-Process -PassThru` and `WaitForExit()` on its launcher handle.
All Windows operators and the backend use the same validated `broker.env` selection, including whitespace/matching quotes. Invalid port syntax, malformed/invalid duplicates, leading zeros and out-of-range values refuse before process effects instead of silently probing 8787; an explicit port must agree. Metadata/config-only changes report `reconciled` without advancing code-change time or claiming activation; genuinely unchanged installations report `verified-no-op`.


Explicit autostart registration, only if desired:

```powershell
& "$env:USERPROFILE/.agents/services/broker/scripts/install-windows-autostart.ps1" -Trigger AtLogon
```

Upgrade preserves any existing registrar, including a no-rollback variant; it does not call this command. Autostart registration does not itself prove a new process is healthy.

## Runtime configuration and API

Fresh `broker.env` is private UTF-8 without BOM, generated from selected generic settings, not copied from the build host:

```text
BROKER_HOST=127.0.0.1
BROKER_PORT=8787
BROKER_DB_PATH=coordination.db
```

There is no notification destination by default. Optional `DISCORD_BOT_TOKEN`, `DISCORD_NOTIFY_CHANNEL_ID`, and `BROKER_ALERT_DISCORD_CHANNEL_ID` are private user configuration; never put their values in releases or command-line install arguments. Reaper enablement is an explicit separately authorized operator action and requires its own valid private configuration.

`GET /api/health` reports SQLite status and the database actually opened. `POST /api/event`, `GET /api/agent/<id>/messages`, `/api/events` and native adapter endpoints retain their existing schemas; `/dashboard` is read-only. This is a local coordination service, not an authentication gateway or hosted mailbox. Loopback health does not prove a successful database write; see the disposable write/read smoke in the testing guide.

## Codex session launcher

```powershell
pwsh -NoProfile -File "$env:USERPROFILE/.agents/services/broker/scripts/start-codex-relay-session.ps1" `
  -BrokerDirectory "$env:USERPROFILE/.agents/services/broker" `
  -StateDirectory '<existing-private-codex-relay-state>' -SessionKey '<key>' `
  -WorkingDirectory '<project>' -BrokerUrl http://127.0.0.1:8787
```

Preserve the exact `SessionKey` and `StateDirectory` on resume. `-NewThread` is explicit. The short `scripts/codex-relay.ps1` wrapper also requires explicit broker and state roots; there is no pilot drive, hidden root search, forced model or global MCP registration.

## Owned-file uninstall

Stop only your owned process through `scripts/stop-broker.ps1` with the retained port. Explicitly unregister autostart only if you own/want to remove it. Remove the owned Claude entry with `claude mcp remove --scope user local-relay-channel`; remove only the owned OMP contribution through its public configuration interface. Review `.agents/installations/agent-messaging/installed.json`; delete listed immutable files only when their actual hashes still match. Preserve edited/foreign files, settings, state, credentials and recovery backups. Selection omission never performs this operation. There is no whole-directory or database deletion command.
