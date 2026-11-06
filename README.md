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

The scripted installer supports **Windows PowerShell 5.1+**, the actual invoking Windows user's profile, and an existing **base Python 3.12** with pip/venv/ensurepip. On a fresh installation it discovers Python from optional `-PythonExe`, then `PYTHON_EXE`, then PATH `python`/`python.exe`; invalid explicit/environment selections and venvs refuse rather than falling through to another interpreter. Updates retain the installed interpreter. For a recognizable legacy venv, the base is specifically `<pyvenv.cfg home>/python.exe`; optional `executable` metadata must agree with that path, and the actual version/base/executable/prefix probe must match. Inconsistent metadata, aliases, a missing declared base or a nested-venv executable refuse; no alternate interpreter is guessed or repaired. It never installs Python, a harness, Bun, Node, or machine policy. Codex's session launcher requires **PowerShell 7**. No hook activation, enterprise channel policy or organization entitlement setup is included.

Available Claude, Codex and OMP executables are discovered locally on first install. With no supported harness installed, the fresh service-only installation is valid. OMP discovery uses its existing command (`-OmpCommand`, `OMP_COMMAND`, then PATH `omp`) and public **`config path`** API to obtain the actual selected current-user agent directory. `OMP_PROFILE` takes precedence over `PI_PROFILE`, including an empty/default selection; `PI_CODING_AGENT_DIR` is respected. No `multi_machine` profile or guessed profile layout is required. The existing `omp-relay` declaration supplies its base Python and private state/runtime roots when present. Otherwise an observed local runtime.json OMP interpreter, `OMP_PYTHON_EXE`, or the selected broker base Python is used, and fresh roots default to `.omp-relay/state` and `.omp-relay/runtime` below the actual profile. The selected OMP Python must already have `mcp`, `httpx`, `cryptography`, and `PyYAML` (`requirements-omp-mcp.txt`); a shared base interpreter is not modified. Missing, unsafe, conflicting or disabled existing inputs still refuse.

Discovery selects installed executable candidates, not a promise that every adapter is usable. If an auto-selected harness has missing/unsafe prerequisites (for example OMP with no actual agent directory or missing base-Python dependencies), the complete installation refuses before live writes rather than silently skipping that adapter. On a **fresh** installation, use `-Harness @('claude','codex')` to exclude an unavailable OMP adapter, another supported subset, or `-Harness @('none')` for service-only. Retained selections on updates are not implicitly removed by narrowing or omission; restore their genuine prerequisites instead. Immediately knowable user/work/install paths are validated before external Python/OMP discovery. OMP's `config path` is a read-only directory query; live discovery never uses `config set`. Typed config-set projection happens only against isolated WorkRoot copies.

Core Python source can be used on other supported Python platforms; the POSIX launcher and systemd operator are explicit source-use utilities, **not a tested Linux/macOS harness installer**.

## Install or update locally

Download the component frontdoor once, then run it locally to install or update the latest published **stable** release:

```powershell
Invoke-WebRequest -UseBasicParsing `
  -Uri 'https://github.com/gupta-ash/agent-messaging/releases/latest/download/install.ps1' `
  -OutFile ./install.ps1
& ./install.ps1
& ./install.ps1 -Mode Verify
```

There is no required version, work-directory, user/profile, Python or OMP coordinate matrix, source checkout, fleet-deployment package, staging checkout or remote coordinator. The actual OS user is automatic; another user's profile cannot be selected. Public HTTPS downloads need no private-repository access or token. Do not pipe remote text into `iex`.

The saved frontdoor validates the resolved exact tag/full commit, archive checksum, exact source-bound members/modes and matching committed frontdoor asset. If the selected release has a newer frontdoor, it hands off **once** to those verified extracted bytes with the same selections and resolved exact version. The selected frontdoor independently repeats admission and rejects changed bindings or recursion. The saved/running script is never overwritten, and script mismatch is never bypassed.

Optional controls:

```powershell
& ./install.ps1 -Mode Check                    # plan the latest stable release without live writes
& ./install.ps1 -Version '<exact-release-tag>' # pin this operation, still refusing downgrade/source divergence
& ./install.ps1 -Harness @('claude','codex')    # explicit selection or retained-selection additions
& ./install.ps1 -Harness @('none')             # service-only on a fresh installation
```

Invoke the script directly for PowerShell arrays; do not pass a comma-containing native `-File` argument. Omitted selectors preserve existing selections; explicit additions expand their union and omission is not uninstall. `none` cannot be combined with a harness. `InstallRoot`, `PythonExe`, `Port` (fresh default 8787) and optional OMP coordinate flags must agree with retained selections on updates/Verify; root/state relocation is not an upgrade. Existing private `broker.env` supplies its port on adoption and is never overwritten. Advanced `-WorkRoot <absolute-owned-directory>` is optional; otherwise a fresh private operation directory is derived under the invoking account's LocalApplicationData, outside source/install/config/state roots. Downloads, extraction and child temp/cache/build activity stay there. Backups are retained, not automatically purged.

A frontdoor handoff forwards the already resolved **same WorkRoot**; it does not select a second default. Each validation pass retains a separate `release-*` download/extraction directory there. The backend's private output identifies `work_root`, its `operation-*` temporary/build directory as `operation`, and the exact separate `backups-*` directory as `backups`. Preserve the returned backups and its operation/failure map; neither `release-*` directory is the recovery-backup directory. No directory is automatically purged after handoff.

`Check` may download wheels/archive and use isolated WorkRoot config copies, but makes no live change. `Install` repeats complete preflight, writes only its owned selected surface and exercises the independent retained file/config verifier before reporting success. Separate **read-only, no-network** `Verify` uses the retained installed version when `Version` is omitted, even after downloads/extraction are removed. It does not repair prerequisites, start MCP servers, query a service or prove activation/native readiness.

OMP configuration projection uses its public typed config API against an isolated copy, then atomically merges only the selected extension and `mcpServers["omp-relay"]`. The actual root/profile, base Python, existing state/runtime roots, entry shape and unrelated servers/lists/order/settings are retained. Before choosing fresh roots, discovery checks retained/current registrations, installed relay files, selected config/extensions, local runtime declarations, relay environment inputs and the existing `.omp-relay` namespace/selected directories. Any prior identity evidence requires both observed roots to exist; ambiguous/incomplete legacy identity refuses rather than silently creating replacements. Only a genuinely new relay with no such evidence may initialize fresh roots. A foreign same-name registration, conflicting selected root/port or disabled relay is a conflict, not permission to override it.

## Existing installations, ownership and recovery

Recognizable **same-user** legacy broker installations are adopted automatically only when their broker/schema anchors and every overwritten selected file match declared known source hashes or selected payload bytes. There is no adoption override: absent/unrecognized anchors, edited code and foreign registrations still refuse. Private `broker.env`, interpreter, port and bound adapter state inputs are preserved. Every overwritten owned file is privately backed up under the operation WorkRoot. Backups/failure diagnostics are retained preservation, not disposable extraction; their location is returned in private operation output. `operation.json`/`failure.json` map original paths to backups and code/configuration/environment kinds; only immutable code has old/candidate digest bindings. Do not purge them with downloads. Follow the [bounded manual code-only recovery procedure](deploy/RUNBOOK.md), never a wholesale configuration/venv/database restore.

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
