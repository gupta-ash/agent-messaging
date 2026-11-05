# Exact-source release contract

`deploy/package-broker-stack.ps1` packages one literal committed revision using `deploy/release-files.json`, never installed homes or dirty adjacent bytes. Its executing generation policy must match that commit. Output must be a fresh absent absolute local directory outside source/installation/state, with no unsafe reparse ancestry. No overwrite/adoption of package output is supported.

Git archive attributes are not raw-blob authority: `export-subst` may transform bytes and `export-ignore` may omit members. Before publication the builder independently binds every declared static export's materialized and staged bytes/mode to the selected commit's raw blob OID/mode, including the frontdoor asset and metadata. A transformed/missing member refuses rather than relying on clean filters, global Git settings, or disabling attributes. Normal EOL working-tree normalization is not a claim about archive transformation.
The raw OID comparison streams `blob <byte-length>\0` followed by the exact file bytes through SHA-1, matching the captured Git tree's blob identity without a materialized directory needing Git metadata or native batch stdout framing. Mode checks remain bound to the committed tree and archive entries. This is separate from the SHA-256 archive/member receipts.


```powershell
& ./deploy/package-broker-stack.ps1 -SourceBrokerRoot '<accepted-checkout>' `
  -ReleaseTarget '<full-40-lowercase-commit>' -BundleVersion '<yyyy.MM.dd.HHmmss>' `
  -OutDir '<fresh-owned-output>' -Zip -NoRelease -ReleaseRepo gupta-ash/agent-messaging
```

The tag is exactly BundleVersion. Publication is a separate authorized invocation without NoRelease, after source/privacy/legal gates; there is no implicit latest/downgrade/retry selection. The builder publishes `agent-broker-deploy-<tag>.zip`, the same name plus `.json`, and the committed root `install.ps1`. Literal source materialization, exact staged and ZIP membership, case-sensitive names, per-member bytes and source modes remain bound. The plugin count applies only to its six-file subtree, not the rest of `home`.

The release receipt has exactly `schema_version:1`, `component:"agent-messaging"`, `repository:"gupta-ash/agent-messaging"`, `version`, `source_commit`, `archive_sha256`, and `files:[{path,sha256,mode}]`. Paths are normalized package-relative names; mode is 100644/100755. It covers every ZIP member including generated `.gitignore`, `manifest.json` and `VERSION.txt`; the archive digest is outside the archive to avoid a self-hash. The manifest binds packagedCommit, bundleVersion, release declaration digest and the existing tree hash. The root frontdoor also binds archive members to the selected committed Git tree's actual blob hashes/modes before downloaded code executes.

Service/adapters/operators, the six plugin files, six fronts/helpers, six library/bin files (including the connection resolver), LICENSE, frontdoor and bounded verifier closure share this single declaration. No exports.json, hook-settings fragment, channel-policy/setup/GrowthBook/vps wrapper, environment values, runtime state, private evidence or history is included. Only selected harness surfaces are installed.

Installed receipts have closed schema fields `schema_version`, `component`, `repository`, `version`, `source_commit`, `archive_sha256`, `parameters`, `files`, `configuration_paths`, `held_paths`, `last_code_change_utc`. Parameters use resolved nonsecret CLI option names, excluding Version/Mode/WorkRoot/consent. Immutable files include absolute path/hash/mode; independently projected configuration is separate. Code-change time is not activation evidence. Backups and possible partial effects are private operation output, not public receipt data.
