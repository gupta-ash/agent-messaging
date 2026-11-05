@echo off
REM cc_channel.bat — launch Claude Code with local-relay-channel enabled.
REM Forwards all arguments to the Claude Code command on PATH.
claude --dangerously-load-development-channels server:local-relay-channel %*
