# Three session GUC ownership repairs

## Change and scope

Move `enable_groupagg`, `log_statement_max_length`, and
`output_plugin_libraries_string` out of process globals into their existing
session buckets. Preserve public lvalues through fallback-aware accessors and
bind each GUC registry to its session's backing address. Whole-bucket adoption
and reset cover the new fields; string lifetime remains with GUC machinery.
There is no new hot mirror, TLS slot, string destructor, or module admission.

The failure was observable beyond SHOW: another session could change grouped
aggregation's Disabled state, logged statement truncation, and the output-plugin
permission decision. Admin-session probes establish isolation defects, not a
privilege exploit. The process-only loader guard still rejects pgoutput in the
threaded address space even when the wrongly shared allowlist permits it.

## Fail-first and fixed controls

All execution used the dedicated Debian 13 EC2 correctness host, explicit uring,
libxtc `95041727ab08dca6b1474aabca38e2738c66e6ed`, assertions, debug symbols,
injection points, C locale, and nofile 131072. No local builds or tests.

The exact final TAP 021 ran against both binaries:

| Lane | Before | After |
| --- | ---: | ---: |
| Fork | 30/30 | 30/30 |
| Session fibers, one executor loop | 18/32 | 32/32 |
| Stackless protocol pool, four carriers | 18/32 | 32/32 |

All 94 assertions completed in both runs, without skips or timeout. Five live
connections exercise the four-carrier pool; this is not a general fairness
result. Tests cover alternating SET, SET LOCAL, rollback, RESET, reconnect/role
defaults, actual planner and logging behavior, SET privilege, and plugin policy.
Threaded allow-path checks require the existing process-only loader rejection
(`XX000`); deny-path checks require policy rejection (`42501`) before loading.
Neither path claims threaded pgoutput support.

The startup isolation assertion reads A before A repeats its SET. Old controls
with three incorrect empty-list display expectations and an early test abort
were retained separately; they are not the authoritative comparison above.

Independent review corrected duplicate synthetic-session GUC initialization
and missing static early-fallback scalar defaults before integration. Split C
tests check backing addresses, alternate-session values, string reset, and
fresh-thread fallback defaults. The existing helper initializes synthetic GUC
registries; tests do not initialize them again.

## Validation

A new empty build and install replaced the old build, with matching Meson
options. All backend and test-module objects were rebuilt because runtime
structure layout changed. The complete old installation was copied and hashed
before replacement and reverified afterward.

| Check | Result |
| --- | --- |
| Clean build/install/install-test-files; fresh setup | PASS; setup 3/3 |
| TAP 021 | 94/94 |
| Backend-runtime regression | PASS |
| Process regression | 239/239 |
| Same-loop latch TAP 019 | 13/13 |
| Parallel attachment TAP 020 | 9/9 |
| Persistent session probes | 465/469; only four known reserved-prefix failures |
| Full four-loop fiber regression, three fresh runs | 238/239 each; only `guc` reserved-prefix difference |
| Runtime lifecycle checker | PASS: 180 fields, 180 buckets, 36 resets, 430 owner mappings |
| Global lifetime checker | RED: 39 unclassified declarations / 36 symbols |

All three GUC families pass the original persistent probes (321/321). No EXPLAIN
differences occurred in the three full fiber runs. This does not prove that all
possible plan variation is eliminated. Every full run captured the intended
live binary, libxtc mapping, effective settings, and actual io_uring descriptors.
No hang, crash, or retained-root warning was observed.

The global checker previously reported 45 declarations / 39 symbols. Exactly
the three migrated symbols disappeared; no new unclassified symbol appeared.
The scanner still exits before certifying later boundary checks. Known format
and hstore_plperl build warnings remain.

## Identities and evidence

Source base: `52f433790e813c28a81dd3543841744fb9b474b7`, plus the reviewed owner
patch and TAP 021. This audit was added after execution; no prefix repair or
expected-output change was in the tested source.

- Tested patch SHA256: `dab54d5b739673c1178be5b6d8a13f8fa5ed012b869e466a624d215f8bb1e4a7`
- Exact TAP SHA256: `1301fbf006bbc947778f95b3fb05d461f054fa8ce680398b77f907ae3af8ebf1`
- Old installed postgres: `7f0ea9d764a2e37e719842dcdea8e2bcd1f17626efe323bf248a4e74e9113b1a`
- Fixed installed postgres: `1702d6fb7e2bce84d1762e83984aa71814ddb1106e4a745d6d68acddd47a8119`
- Installed libxtc: `3d8876b3fa20de5ed9af4345531a912bbd19a81d5289b49602a76d7fea72da7e`
- Final old TAP archive: `7044fa6149b1ff0cf80031fcb56524e108eed1afa9f073a4e9a9ab9e16116e00`
- Fixed evidence archive: `ea60b02e88c19def95d0a28b893f4a724abb1be8c2d468866ebc6042c5552aec`

Archives were fetched and remote/local checksums compared. Reports, source and
build manifests, live identities, full logs, diffs, and harness corrections are
under `/scratch/postgres-xtc/baseline-20261028/{tap021-final-old,guc3-fixed}/`.
The fixed archive is `guc3-fixed-evidence.tar.gz` in that parent directory.

## Remaining gate failures

Foreign-session PL/pgSQL LOAD still reserves its prefix globally, incorrectly
rejecting a valid setting before B's own LOAD. That separate repair is pending.
No alternate expected output was refreshed to hide it. Full threaded
qualification and global ownership remain red. Broader worker, TLS,
cancellation, crash, and churn gates are not covered here. This is stage-one
stabilization, not scheduler cutover or performance acceptance.
