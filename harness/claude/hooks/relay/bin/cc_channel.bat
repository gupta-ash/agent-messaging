@echo off
REM cc_channel.bat — launch Claude Code with local-relay-channel enabled.
REM Forwards all args to claude. Resolves `claude` via PATH (the claude shim
REM lives in ~/.local/bin). This script deploys to ~/.agents/hooks/relay/bin, which
REM update.ps1 and bootstrap add to PATH, so `cc_channel` runs from any cwd.
claude --dangerously-load-development-channels server:local-relay-channel --effort ultracode %*
