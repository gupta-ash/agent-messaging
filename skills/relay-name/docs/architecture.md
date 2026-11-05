# Architecture

Claim or rename the Claude relay name while preserving epoch, liveness and queue ownership.

SKILL.md routes by the invoking harness. The Claude executable is `../harness/claude/set_name.py` relative to the skill root. OMP/Codex native relay tools are provided by the owning broker adapters, not a second Python implementation in this skill. There is no independent skill MCP server or daemon.

The executable imports the single authored `harness/claude/hooks/relay` library through its installed `.claude/hooks/relay` view. HTTP goes to the configured broker. Registry/session/job/outbox/cursor state stays in the selected user's `.claude` namespaces, never in the release tree. Modules freeze home-derived paths during import; tests set synthetic HOME and USERPROFILE before loading them.

The Claude executable owns liveness checks, registry mutation, reclaim rules and job-state mirroring. The broker remains the coordination writer. The release includes the selected skill's frontmatter and script plus the common relay library and harness adapter.
