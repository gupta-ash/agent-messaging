# relay-check source

Read broker inbox/aggregate messages without advancing delivery cursors.

Read [SKILL.md](SKILL.md) for the installed invocation and harness-selection contract. Engineering: [architecture](docs/architecture.md), [design](docs/design.md), [implementation](docs/implementation.md). The Claude executable is authored under `harness/claude` and installs to `.agents/skills/relay-check/scripts/`. Source-only docs/tests do not ship as runtime skills.

The agent-messaging repository owns this skill and the shared relay library. The public installer selects the requested harness; it does not register receive hooks or activate messaging unless explicitly requested.
