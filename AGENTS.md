# agent-messaging engineering

Read README.md and docs/{architecture,design,implementation,operations,release}.md before changing source. Update contracts and meaningful consumer-visible tests together. Preserve endpoints, SQLite/state identities, optional dependency boundaries and the dashboard's read-only access. No aliases for removed root modules: import broker.server, broker.addressing and broker.discord_poller.

Use a non-live worktree and a session-owned scratch/build/cache subtree. Never use a real mailbox, live HOME, production DB or deployed plugin as a fixture. Set synthetic HOME/USERPROFILE/APPDATA/LOCALAPPDATA/temp before imports, clear credentials and use an owned loopback port. Source publication does not authorize service activation, autostart, hooks or remote execution. Reviewed versioned local packages and separate read-only Verify replace any central staging prerequisite.
