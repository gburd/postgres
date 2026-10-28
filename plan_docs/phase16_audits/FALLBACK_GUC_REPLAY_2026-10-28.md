# Replay postmaster settings in exec fallback children

## Failure and cause

The wire-cancellation matrix exposed a separate startup failure in its genuine
process-fallback control. With `multithreaded=on` followed by
`max_wal_senders=0`, the exec child failed before its first query:

```
shared memory struct "Wal Sender Ctl" was created with different size:
existing 112, requested 1072
```

`read_nondefault_variables()` excluded postmaster/internal settings when
`multithreaded && IsUnderPostmaster`. That exclusion protects shared-address-space
carriers from replaying assignments into the postmaster's globals, including
freeing its strings. But an exec child has fresh globals. Replaying
`multithreaded=true` made it skip subsequent postmaster settings, leaving
`max_wal_senders` at its compiled default of ten.

The saved `config_exec_params` contained zero, after multithreaded. The parent
had not lost the configuration. A fresh child debugger capture then observed:

| Point | Direct max_wal_senders |
| --- | ---: |
| Before replay; exec flag true, multithreaded false | 10 |
| Parsed max_wal_senders record: value 0, postmaster context, multithreaded true | 10 |
| Immediately after replay | 10 |
| WAL sender shared-memory request | 10 |

The same child measured a 112-byte header and 96-byte sender: ten senders
explain the exact 1072-byte request. Child executable identity and the input
file reached through its cwd matched the retained parent input. This is a
direct integer global, not a session-root accessor failure.

One debugger event was misleadingly named `skip_record`: optimized line
information relocated its requested breakpoint to the assignment call. The
report corrects that label. It is not evidence of a stop inside the skip body.
The parsed input, guard inputs, assignment-path observations and before/after
values establish the replay defect together.

An order-only control, with unchanged values and multithreaded moved last,
restored zero, configuration-file provenance, and a 112-byte allocation. Its
reconnection ran as `process|process|4|1`, with an OS PID distinct from the
postmaster. This was a diagnostic control, not a change to the failing wire
fixture. Plain Cluster initialization supplies zero early; a later duplicate
in the wire test changed effective insertion order and exposed the defect.

## Repair boundary

Add `!pg_backend_was_forkexeced` to the exclusion. Its sole true assignment is
in `SubPostmasterMain()`, before GUC initialization/replay. It stays false in
the postmaster and its carriers. The exec child's private write cannot change
the parent's value.

Use the existing boolean, not `PG_BACKEND_WAS_FORKEXECED`: the macro is constant
true under EXEC_BACKEND and would also bypass protection in any postmaster
carrier in such a build. The direct flag describes this particular process.

Both carrier callers keep their protection; exec children replay all settings.
No runtime layout, owner, scheduling, parallel serialization, IO remap or
hba/ident workaround changes. No resize or suppression of the shared-memory
consistency check. The production change is one condition and its comment.

## Independent regression and controls

TAP 026 uses five fresh clusters: fork with zero/ten after multithreaded=off,
fallback with zero/ten after on, and fallback zero before on. All other
explicit settings precede multithreaded; wal_level=replica permits ten senders.
The test checks saved replay order, exact values and configuration-file
provenance, actual process identity, allocation presence, reconnect and fast
shutdown. The boot-default ten case must not pass by value alone.

| Case | Old | Fixed |
| --- | ---: | ---: |
| Fork, zero after | 67/67 | 67/67 |
| Fork, ten after | 67/67 | 67/67 |
| Fallback, zero after | 6/7 | 71/71 |
| Fallback, ten after | 68/71 | 71/71 |
| Fallback, zero before | 71/71 | 71/71 |
| Total | 279/283 | 347/347 |

Old zero-after fails its first connection; dependent assertions return without
aborting the other cases. Ten-after fails only source/file/line provenance.
No skip, timeout or harness exception occurs in the authoritative runs. Fixed
execution reaches every dependent assertion.

An initial fixture incorrectly called a nonexistent Cluster `pid` method.
It was replaced with the existing TAP001/003 postmaster.pid read pattern.
Review explicitly corrected that missed API and rechecked all Cluster calls.
That initial run and an observer permission/capture-order failure were retained
separately, not used to qualify controls. The identical corrected fixture was
used for the old and fixed results above.

All execution ran on the dedicated Debian 13 EC2 correctness host with
assertions, debug symbols, injection points, explicit uring, C locale and
nofile 131072. No local tests/builds. All five old/fixed postmaster executable
identities, maps and limits, and all three fallback ring descriptors, were
captured. Candidate installation changed only bin/postgres; the helper and
SQL stayed unchanged. Only guc.c and the backend executable rebuilt.

## Regression results and remaining wire defect

- Fresh setup, backend-runtime regression, process core 239/239 and fresh
  four-loop fiber core 239/239: PASS.
- TAP 019/020/021/022/023: 13/13, 9/9, 94/94, 253/253, 173/173.
- TAP 001/003: 128/128, 43/43.
- Lifecycle checker: PASS, 431 owner mappings.
- Global lifetime checker: still RED, 39 declarations / 36 symbols.

The unchanged full wire TAP 025 now completes its old-policy negative control:

| Lane | Passed |
| --- | ---: |
| Fork | 181/181 |
| Fiber, one loop | 159/182 |
| Fiber, two loops | 159/182 |
| Pooled four | 147/169 |
| Genuine process fallback | 183/183 |
| Pooled eight, saturated dead-end route | 22/23 |

Exactly 69 assertions fail, all missing SQLSTATE 57014 under the still-unfixed
logical wire cancellation route. Wrong-key/malformed/replay checks, subsequent
usability, crash guards and fast shutdown pass. The separate pooled-eight
fixture proves eight native carriers started before saturation. No CPU skip
or debugger intervention. Total: 851/920 in 286.7 seconds. This is qualified
negative evidence for the separate wire repair, not a green cancellation gate.

## Provenance

Base: `034bb15ee2df12b48c59d6701f6fc2c916da96ce`; audit added after execution.

- Old installed backend: `317c06f9bda3aedd39d876f9582ce7fcf16e3d1831f7d5dbb2c6f551bf530830`
- Fixed installed backend: `7c802ed789ea2b7b94a50796af0d813f2489ba363a92970fce9264fdb9164384`
- Exact TAP 026: `2c08a1a1b725c9f09f810080974ed43a00c75ea52ce5ddd7ecb3a0c6399054bd`
- libxtc source: `95041727ab08dca6b1474aabca38e2738c66e6ed`
- Installed libxtc: `3d8876b3fa20de5ed9af4345531a912bbd19a81d5289b49602a76d7fea72da7e`
- Live diagnosis archive: `395ba88d319b20db33bf7ff281e412548e6d73bb1f641bfdf74446bb75b8675b`
- Corrected old TAP archive: `9f0b18afd41af103e2266dba03e45dd5920c670c3587deac84794acdcf568386`
- Fixed candidate archive, including full old-wire control: `9013a803c65fab425334508598cd95cddfd0735d33c70c289997401051af053a`

All archives fetched and checked, including 1997 diagnosis, 9863 old-control
and 35458 fixed manifest entries. Reports and raw artifacts:
`/scratch/postgres-xtc/baseline-20261028/{fallback-guc-diagnosis,fallback-tap026-old,fallback-fixed}/`.
The complete old installation remains at
`/mnt/nvme/control-pre-fallback-guc/pg`.

Windows/native EXEC_BACKEND runtime, sanitizers, full worker/world suites,
TLS and churn are not newly qualified. Wire cancellation and hard-wait
fail-stop candidates remain separate. Stage one remains open; no scheduler
cutover or performance acceptance follows from this fix.
