# Development and verification entrypoints

Python 3.12 is the supported baseline. Create a non-live venv and install `-e '.[dev,claude-mcp,omp-mcp]'`. Set all temp/cache paths to an owned session subtree. Tests set synthetic HOME/USERPROFILE before imports, clear broker/Discord credentials and use synthetic DB/audit/message paths. Never use a live mailbox, production state or canonical harness session.

The backend prepares wheels and a source-bound `agent-messaging` console wheel entirely below WorkRoot, then installs into the owned service venv offline after complete preflight. It does not install a global runtime. OMP's selected base Python is a prerequisite, not globally modified. Read-only Verify uses retained templates/selection/source bindings and actual files; no network, provisioning or service/registrar calls.

The write phase owns the confirmed prior-process stop and refuses a still-running/restarted listener before copying code. Activation starts only after that boundary and retains the original PID/start identity for its new-process proof; it never re-stops a saved PID or accepts the same old healthy instance. Different-version upgrades require the packager's fixed-width timestamp coordinates before ordering them.

Native Windows PowerShell children use the explicit SystemRoot executable with only its matching built-in Modules directory and a disabled module-analysis cache. Direct frontdoor/backend/packager invocations similarly select the executing shell's built-ins and restore the caller's process module environment in finally blocks. This avoids inherited PowerShell 7 module incompatibility without weakening ownership checks or making Verify write cache files. Release fixture snippets independently use the same native module boundary; the packager has a direct poisoned-inheritance behavior case.

Integrator commands (implementation workers do not run them):

```powershell
python -B -m pytest -q tests skills
python -B -m pytest -q tests/test_release_asset_contract.py tests/test_packager_root_modules.py tests/test_installer_restart_deferral.py tests/test_installer_omp_selection.py tests/test_codex_relay_mcp.py tests/test_package_loopback.py
pwsh -NoProfile -File tests/test-start-codex-relay-session.ps1
pwsh -NoProfile -File tests/test-codex-relay-wrapper.ps1
```

Run `pwsh -NoProfile -File tests/test-broker-env.ps1` for the shared strict environment grammar and effect-guarded actual operator consumers (that fixture requires PowerShell 7; runtime helper/operators remain PS5.1+). Backend fixtures also cover adopted whitespace/quoted ports, invalid-before-effect refusals, private original-to-backup mappings and metadata-only reconciliation without code-time advancement. Codex fixtures load the real URL resolver as an independent scriptblock; its absent script-directory context uses only the explicitly selected broker root's packaged parser, never an empty Join-Path or alternate grammar.

`tests/test_installer_restart_deferral.py` now exercises actual owned-file planning, drift, union selection, private backup, interruption and explicit hash-bound retirement, rather than asserting installer source text. The obsolete extension-only bypass/text-structure tests are removed; `test_installer_omp_selection.py` exercises selected MCP/config ownership and optional actual public config projection.

For the real built-package installation lane set `AGENT_MESSAGING_TEST_BUNDLE`, `AGENT_MESSAGING_TEST_RECEIPT` and `AGENT_MESSAGING_TEST_BASE_PYTHON` to an extracted actual archive, its external receipt and the selected existing base Python, then run that suite. It explicitly intercepts profile binding and the no-listener process snapshot for a synthetic local fixture; this is not proof of actual target-account or live-listener acceptance. It performs real wheel/dependency installation and retained Verify after deleting downloads, with no service activation.

For actual OMP public-config projection set `AGENT_MESSAGING_TEST_OMP_COMMAND` and `AGENT_MESSAGING_TEST_OMP_PYTHON` explicitly. For installed `python -m broker` and console loopback write/read/dashboard smoke, set `AGENT_MESSAGING_TEST_INSTALLED_ROOT` to a real owned disposable installation and run `test_package_loopback.py`. Smoke processes use ephemeral loopback ports, explicit synthetic state, unrelated cwd, no notification credentials and stop only their owned PID. Existing source-only flat-package smoke remains separate.

OMP extension suites require the real selected OMP dependency installation via `BROKER_TEST_OMP_NODE_MODULES`. A fresh harness MCP initialize/tools-list/read-only check against an owned fixture remains a separate integration gate. No source test proves actual production activation, auth, autostart or real target-account readiness.

## Simple installer integration gates

The implementation worker does not run these gates. Rebuild the real release after committing the changed frontdoor/backend/runbook; the existing explicit declaration already contains that entire closure, so no new package member or second installer engine is needed.

Run the file/adoption/discovery fixtures and actual OMP default/named-profile config-path cases:

```powershell
$env:AGENT_MESSAGING_TEST_OMP_COMMAND = '<actual-existing-omp-command>'
$env:AGENT_MESSAGING_TEST_OMP_PYTHON = '<actual-base-python-with-OMP-MCP-dependencies>'
& $Python -B -m pytest -q tests/test_installer_restart_deferral.py tests/test_installer_omp_selection.py
```

The optional public-API tests initialize only owned synthetic config/profile fixtures; they do not launch sessions/models or mutate a production registration. They compare discovery to the actual `omp [--profile <fixture>] config path` output, never an expected inferred directory layout. The ordinary package fixture still needs `AGENT_MESSAGING_TEST_BUNDLE`, `AGENT_MESSAGING_TEST_RECEIPT`, and `AGENT_MESSAGING_TEST_BASE_PYTHON`; it now requires Install's independent verification result as well as separate Verify after deleting downloads. Its account/listener interceptions remain explicit, not actual Windows-account proof.

Root-preflight fixtures refuse unsafe UserRoot/WorkRoot/InstallRoot before either Python prerequisite execution or OMP discovery. Windows legacy-venv fixtures derive home/python.exe, probe the real supported base, and refuse conflicting venv/alias/other-base metadata without executing it. The optional profile fixture uses config set only to initialize its owned synthetic test profile; production discovery itself issues only the public read-only config path query. Parent integration should compare fixture files/directories before/after that query for both default and named profiles.

For end-to-end frontdoor/account/latest/handoff checks use a **disposable Windows account/VM** with real installed prerequisites and no production product receipt/listener. The frontdoor intentionally cannot redirect its fixed metadata namespace to a spoofed HOME. Save the revised released frontdoor A, run `& ./install.ps1` with no options, inspect the retained resolved version/Python/Harness/port/roots, and run `& ./install.ps1 -Mode Verify` after removing downloads (not backups). In another fresh fixture with no harness executable, require service-only success. Invalid PYTHON_EXE/non-base Python, foreign OMP config-path root, edited legacy code and foreign/disabled MCP entries must refuse before live writes. Known-byte legacy adoption must preserve private broker.env, DB/WAL, state roots, Codex SessionKey files, parent junctions and unrelated configuration, with real backup mappings for overwritten files.

Publish a second real descendant stable release B with a deliberately changed committed frontdoor. Invoke the **unchanged saved A script** without options: require verified one-hop execution of B, B's installed receipt and independent Verify, and unchanged A bytes. Exercise pinned downgrade/source divergence, wrong asset/archive/member/source bindings, tampering and changed B bindings during handoff; each must refuse without bypass, recursive handoff or successful-install stamp. A marker is only a bound recursion guard, never a trust shortcut.

Installed broker write/read/dashboard smoke:

```powershell
$env:AGENT_MESSAGING_TEST_INSTALLED_ROOT = '<real-owned-disposable-installed-broker-root>'
& $Python -B -m pytest -q tests/test_package_loopback.py
```

This runs the installed venv's `python -m broker` and `agent-messaging.exe` from unrelated disposable runtime cwd, chooses ephemeral loopback ports and synthetic DB/audit/message paths, verifies `/api/health` SQLite/actual DB, POST `/api/event` → recipient GET roundtrip and HTML dashboard, and stops only its owned process. Never point it at a production installation/state.

Separately launch fresh selected Claude/Codex/OMP fixture sessions against an owned disposable broker. Require real MCP initialize/tools-list and native read-only relay-check, not installation/parser success. Claude uses the actual retained registered interpreter/plugin; Codex uses the installed session launcher with explicit private StateDirectory/SessionKey and creates no global entry; OMP uses the discovered fixture agent/profile and its retained base Python/state/runtime roots with `mcp__omp_relay_check`. Compare unrelated config, junctions and identity files before/after and preserve Codex resume coordinates. Production activation, existing-peer consent and legacy relative-process refusal remain separate user-authorized gates.
