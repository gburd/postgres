# Session-owned GUC prefix reservations

## Repair

A session that loaded PL/pgSQL reserved `plpgsql` in a runtime-global list,
although custom GUC descriptors were session-owned. A new session then rejected
valid `SET plpgsql.extra_warnings = 'all'` before its own LOAD. Fork mode and a
fresh B-only control accepted the placeholder.

Move the effective reservation list into `PgSessionGUCState`. Both list storage
and strings allocate in that session's `GUCMemoryContext`; existing bucket
adoption, context teardown, and initialization move or clear ownership together.
RESET ALL does not remove reservations. Placeholder conversion, typo warnings,
name validation, and permission checks are unchanged.

True shared preload remains inherited. At postmaster startup, capture an
immutable deep copy in the existing runtime extension context, independently of
whether any custom descriptors exist. New threaded sessions copy it after
seeding descriptors and before replaying startup settings. Client LOAD cannot
change that snapshot. Fork inheritance and exec-child preload reconstruction
retain their existing paths.

Delete `guc_0.out`: it encoded the incorrect early rejection and lacked an
upstream octal-display block. The canonical expected output is unchanged.

## Controls

All execution ran on the dedicated Debian 13 EC2 correctness host, with the
same explicit-uring libxtc pin, assertions/debug symbols, injection points,
C locale, and nofile 131072. Builds and tests did not run locally.

TAP 022 uses three fresh clusters per lane: no preload, reservation-only preload,
and supported pg_stat_statements descriptor preload. The small reservation-only
fixture has no custom descriptors or mutable globals. The test does not invoke
a PL helper before the pre-LOAD probes.

| Lane | Old prefix policy | Fixed |
| --- | ---: | ---: |
| Fork | 81/81 | 81/81 |
| Session fibers, one executor loop | 73/86 | 86/86 |
| Session fibers, two executor loops | 73/86 | 86/86 |

All 253 assertions completed in each run, without skips or timeout. The 26 old
failures reproduce foreign-LOAD contamination: valid and typo placeholders,
conversion value/warning, connection startup options, and reconnect. Both
preload positives pass before and after the repair. Unsupported PL/pgSQL shared
preload still fails at the custom-GUC safety check; no admission metadata changed.

TAP waits for removal from `pg_stat_activity`, not completed memory reclamation.
The split runtime GUC test supplies the stronger lifetime check: actual
`MarkGUCPrefixReserved()` allocations have distinct session-owned lists, cells,
and strings; synchronous reset deletes A's context before B uses its list.
It also checks early move-and-clear and fresh/reset NIL state. Synthetic owners
clear borrowed authentication pointers before full reset, so the test cannot
free the live session's `system_user_context`. Independent review caught this
and the original overly strong TAP teardown claim before execution.

## Validation

Runtime structure layout changed. The previous complete installation and build
were retained and hashed; a new empty build and installation rebuilt every
backend and module with matching options and installed headers.

| Check | Result |
| --- | --- |
| Clean build/install/test-module install; fresh setup | PASS; setup 3/3 |
| Backend-runtime regression, including allocation/reset checks | PASS |
| TAP 022 / TAP 021 | 253/253 / 94/94 |
| TAP 019 / TAP 020 | 13/13 / 9/9 |
| Process regression | 239/239 |
| Original persistent-session probes | 469/469 |
| Full four-loop fiber regression, three fresh clusters | 239/239 each |
| Dead-end / startup-storm / migration TAP | 16/16 / 5/5 / 6/6, no skips |
| Lifecycle checker | PASS: 180 fields, 180 buckets, 36 resets, 431 owners |
| Global lifetime checker | RED: unchanged 39 declarations / 36 symbols |

The full fiber suite previously finished 238/239 three times with only the GUC
prefix difference. All three fixed runs use canonical `guc.out`; no EXPLAIN or
other difference occurred. Startup-storm logs contain 337 migratable spawns;
the migration run records 280 cross-loop steals. Actual io_uring descriptors,
matching live executable hashes, and intended library maps were captured.
No hang, crash, or retained-root warning was observed. Expected unsafe-preload
startup failures remain explicit negative controls, not crashes.

## Identities and evidence

Source: `e15b12ed21253c315d92bbb5f1e455b9a772d58b` plus the reviewed prefix
repair. This audit was added after execution. The transferred delta retained
GUC3 fields/tests and included deletion of the obsolete alternate output.

- Tested merged patch SHA256: `a75652095fa7902b7898a58cfb23948ef790a865f91199a06533198d648e8a3c`
- Exact old/fixed TAP SHA256: `c098a5e1e0ccea9258828cb47065d87cc54eff38b1b9c043b622c15e9b5e2c23`
- Old installed postgres: `1702d6fb7e2bce84d1762e83984aa71814ddb1106e4a745d6d68acddd47a8119`
- Fixed installed postgres: `80ae00526d8bee8e7e5af254dbf84515e5a3906019fe79f8995a65557d070fae`
- libxtc source: `95041727ab08dca6b1474aabca38e2738c66e6ed`
- Installed libxtc: `3d8876b3fa20de5ed9af4345531a912bbd19a81d5289b49602a76d7fea72da7e`
- Old TAP archive: `675cb2b77192c410eb8414c5897fbf45dde7493cde85e19f0cd837366cfccbe3`
- Fixed archive: `944184fb131c448f25a9dbca0be9d857fedc4d867f4c153f5c4f128fa7d21a96`

Archives were fetched and remote/local checksums compared. Full source/build
manifests, retained test data, logs, live identities, ring snapshots, and old
prefix verification are under `/scratch/postgres-xtc/baseline-20261028/` in
`prefix-tap022-old/`, `prefix-fixed/`, and their corresponding evidence archives.
Known build warnings remain; this is not a warning-free build claim.

## Limits

Core fiber regression is now green at this candidate, but stage one is not
closed. Global ownership remains red; worker/world, TLS, cancellation, crash,
and connection-churn qualification are separate work. No scheduler cutover or
performance acceptance follows from these passes.

Parallel custom-GUC/library serialization is unchanged. Runtime-global library
selection and heap-backed custom-value ownership remain Phase 16 /
Gate E2-Extensions work. The client/preload tests do not establish parallel
custom-GUC parity. Dedicated EXEC_BACKEND, logical-FATAL, and session/local
preload matrices were not run. Unsupported writable preload descriptors remain
rejected by the existing safety check.
