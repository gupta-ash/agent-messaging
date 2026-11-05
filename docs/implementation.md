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
