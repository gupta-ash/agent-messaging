# Implementation

The executable is `harness/claude/send.py` under this skill. It installs as `.agents/skills/relay-send/scripts/send.py` and uses standard-library CLI/JSON/path facilities plus the common relay library. Server dependencies are in requirements.txt; MCP adapters use requirements-mcp.txt or requirements-omp-mcp.txt.

From a non-live checkout, run `python -B -m pytest -q tests skills` with temporary/cache locations set to a disposable directory. Root conftest establishes synthetic HOME/USERPROFILE and source-derived hook/plugin dependencies before collection. Never import these tests against a live installed HOME. PowerShell wrapper and launcher lifecycle tests run separately.

`deploy/release-files.json` defines the release closure. Runtime skill exports include SKILL.md and the selected script; engineering docs and tests remain source-only. The public installer handles harness selection and explicit activation. Examples in SKILL.md name the final installed paths.
