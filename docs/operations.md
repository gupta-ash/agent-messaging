# Runtime and harness operations

Use the root README's explicit install, activation, autostart and uninstall instructions. Keep the actual service root, port and private broker.env. Relative DB/log/audit paths belong to the selected runtime cwd; code resources belong to the package. Generic loopback defaults are not authority to replace provisioned settings.

Claude registers `local-relay-channel` with the installed venv Python and plugin server on a fresh install. A correct adopted registration using the explicitly selected base Python is retained after its actual MCP dependencies are checked. The library resolves through the retained same-user hooks junction when present; no receive hooks or enterprise policy is installed. Launch its documented channel wrapper explicitly only when using that feature. A foreign registration refuses. The nonsecret connection.json selects the retained loopback port unless an explicit matching BROKER_BASE_URL is configured.

Codex uses the installed PowerShell 7 session launcher with explicit BrokerDirectory, StateDirectory, WorkingDirectory and SessionKey. Preserve exact state/session coordinates on resume. The short wrapper never searches a hidden second root or forces a private model. Its eleven overrides register `relay` only for that session, not globally.

OMP selection contributes `omp-relay` and the selected extension to the explicitly selected actual agent directory/profile. Use mounted `mcp__omp_relay_send`, `mcp__omp_relay_check`, `mcp__omp_relay_name` tools; do not infer a Claude session ID, run a Codex launcher or call raw HTTP as an alternate identity. Native results/errors retain their meaning. The base-Python guard remains; OMP relay state/runtime roots cannot be silently replaced during adoption.

Use a fresh selected-harness session against an owned disposable broker to prove actual MCP initialize/tools-list/read-only behavior. Neither installation, parser support, historical evidence nor a production port's response is such proof. Activation/autostart/notification enablement always require separate authorization.
