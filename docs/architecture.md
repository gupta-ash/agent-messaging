# Architecture

`src/broker/server.py` is the HTTP/SSE writer boundary: FastAPI middleware, SQLite pooling/schema, status/messages, watchers, pruning and audit. Pure addressing lives in `broker.addressing`; Discord polling remains a lazy lifecycle import. SQL and dashboard HTML are package-relative resources. The dashboard opens SQLite read-only and never imports the server to obtain a writer.

Claude's plugin is separate from its shared relay library; the six plugin files and six library/bin files (including the nonsecret connection resolver) plus three fronts/helpers are one explicit release declaration. Codex's websocket/app-server adapter and MCP facade are session-scoped. OMP's stdio MCP adapter and extension preserve persistent identity state separately from temporary rendezvous state. None is a second broker database implementation.

The root frontdoor admits an exact public release, dereferenced commit, archive checksum, exact normalized members/modes, committed source blobs and executing frontdoor before executing the downloaded backend. The backend projects only selected declaration entries, independently preflights the complete set and retains its self-contained read-only verifier. There is no central deployment service, persistent staging tree or source checkout prerequisite.
