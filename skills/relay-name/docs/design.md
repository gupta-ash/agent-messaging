# Design

The public skill uses its `relay-*` name and matching installed script directory. Preserve JSON receipt fields, documented exit codes, canonical-name handling, strict session identity and error reporting. A transport ACK is not an application receipt; process guesses never supply another session's identity.

OMP instead uses the existing session-bound native `mcp__omp_relay_name` tool with only `relay_name`: claim or rename this session's relay name, taking it over from any previous owner by default, without a force flag or separate confirmation. The latest successful explicit claim owns the name; earlier queued messages remain bound to their original session identities. Native results/errors, including unsafe legacy-history refusals, propagate unchanged. Codex uses the session-scoped `relay` MCP server. [SKILL.md](../SKILL.md#host-selection) owns exact invocation/failure behavior; [public setup](../../../README.md) explains the shared native boundary. No additional transport, identity lookup, registry mutation or cross-host fallback belongs in this skill.

The common library has one source in this repository. Delivery, liveness and epoch semantics stay compatible with the plugin and broker. Frozen home dependencies mean patching Path.home after import is not isolation. Test in a disposable installed-layout closure. The public installer does not register receive hooks or implicitly activate a harness.

Engineering docs/tests are source-only; SKILL.md is the runtime invocation authority. Installation and activation are explicit operator actions, separate from source editing.
