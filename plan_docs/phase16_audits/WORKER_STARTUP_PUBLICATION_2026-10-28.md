# Background-worker startup publication

## Repair

`StartBackgroundWorker()` excluded native threads from immediate PID reporting,
but not session fibers: fibers use `PM_CHILD_CARRIER_POOLED_LOGICAL`. Dynamic
waiters could therefore observe STARTED while the worker was still queued,
before common initialization or entry-point lookup.

Use `PostmasterChildHasLogicalBackendPublication()` for that exclusion. Logical
workers publish STARTED at the existing common startup-complete boundary. Keep
the private `rw_pid` and logical signal target available from launch so that
termination can still reach a worker whose startup is incomplete.

STARTED remains distinct from worker-specific readiness. In particular, parallel
MQ attachment still requires the scan-then-wait repair in `parallel.c`; no
attachment, exit, generation, or libxtc behavior changes here. Process workers
retain their historical early visibility after successful fork/exec.

## Deterministic control

TAP 023 uses an inert injection point after `BaseInit()` and before entry-point
lookup. The existing thread-compatible test module supplies a cooperative DSM
gate, not the process-only injection_points extension. After the target enters
the gate, a later sentinel registration/startup proves the postmaster finished
the target's launch path, including the disputed immediate reporter. Gate entry
alone would not prove that ordering.

Four cases cover normal startup, a missing entry point, clean exit before
publication, and termination while held. The helper returns observations only
after cleanup. Each lane also tests SQL-driver ERROR and FATAL while the cleanup
guard owns the gate, DSM mapping, injection attachment, and two workers. A
PID-specific readiness NOTICE precedes SQL cancellation or termination; the
test requires `57014` or `57P01`, not the hold-timeout error. Cancellation checks
same-session recovery. Both worker unregister messages precede successful
reattachment of the same injection name.

Those log barriers prove worker-registration slots were relinquished:
`ForgetBackgroundWorker()` clears `in_use` before logging. They do not prove
complete DSM, PGPROC, runtime-root, or libxtc memory reclamation. The test does
not rely on activity disappearance or PID-matched fiber exit logs.

## Fail-first and fixed evidence

All execution used the existing Debian 13 EC2 correctness host, assertions,
debug symbols, injection points, explicit uring, C locale and nofile 131072.
No local builds or tests.

| Exact expanded TAP 023 | Old | Fixed |
| --- | ---: | ---: |
| All process, fiber, and pooled cases | 157/173 | 173/173 |

The old run fails exactly 16 assertions: held startup status and PID, four cases
in each logical lane. Process behavior, actual worker carrier, completion,
cleanup, usability, and fast shutdown pass. All 48 new exceptional-cleanup
assertions and three final log guards pass on both binaries. Neither run skips
tests or uses timeout cleanup to satisfy an assertion.

The initial 122-assertion fixture independently passed 122/122 after failing
only the same 16 old-policy assertions. Review removed an unnecessary shared
preload of the test module: its custom GUC lacks a preload session accessor.
Dynamic loading remains; admission was not widened. An earlier pooled-log
regex mismatch and runner metadata errors were retained separately, not counted
as valid old-policy failures.

| Validation | Result |
| --- | --- |
| Fresh setup | 3/3 |
| Process backend-runtime regression | PASS |
| Process core regression | 239/239, repeated after addon |
| Fresh four-loop fiber core | 239/239 in three primary runs, plus one addon run |
| TAP 019 / 020 | 13/13 / 9/9 |
| TAP 001 / 003 | 128/128 / 43/43 |
| TAP 021 / 022, primary candidate | 94/94 / 253/253 |
| Lifecycle checker | PASS: 180 fields, 180 buckets, 36 resets, 431 owners |
| Global lifetime checker | RED: unchanged 39 declarations / 36 symbols |
| Separate non-injection backend/helper compile | PASS; not installed or runtime-tested |

The addon rebuilt only the test helper; backend bytes were unchanged. No
unexpected failure, hang, crash, retained-root warning, cleanup warning, or
shutdown fallback occurred. SQL termination completed within its requested
5000ms limit. Live executable identities and actual io_uring descriptors were
captured for every old/fixed TAP postmaster and full fiber run. Existing format
warnings remain in the full non-injection compile.

## Provenance

Source base: `8514cda9f7a6e3858332af582b386559e29a1701` plus the reviewed startup
repair and fixtures. This audit was added after execution. Runtime structure
layout did not change. The primary candidate rebuilt the two touched backend
objects; old and fixed addon runs used identical rebuilt helper and SQL bytes.
Original complete control installations were preserved and rehashed; addon
fixtures were installed into a separate copy of the old installation.

- Old installed backend: `2958dc73590463c46c33678d83fdec5228d3eafc006426555152157f723ee230`
- Fixed installed backend: `317c06f9bda3aedd39d876f9582ce7fcf16e3d1831f7d5dbb2c6f551bf530830`
- Identical addon helper: `bafb45b3cfb297127d2f38b1db9bfa82f475e93d58a67bd7d0aaa202d9c068b7`
- Exact expanded TAP: `6e9f1efd49c96bbbfa8e752bf488d6b67ac7e6d757a0d6b877286ab61c8df891`
- libxtc source: `95041727ab08dca6b1474aabca38e2738c66e6ed`
- Installed libxtc: `3d8876b3fa20de5ed9af4345531a912bbd19a81d5289b49602a76d7fea72da7e`
- Primary fixed archive: `d041faf9b0aad1a866b388c2dec7622a51e9953c45217c5e7eabc22b1154b72c`
- Expanded old archive: `9841e413b938768288087999d370907ad6469a9763eccebbeec2efb904ab4f37`
- Expanded fixed archive: `767acd651c1c66ecb9b4933bcf7497906e7b76947cc26a00e6c8cc7bfbbaf38e`

Archives were fetched and checksums verified, including all 2966 old and 9573
fixed addon manifest files. Reports, source/build/install manifests, raw logs,
retained test data and live captures are under
`/scratch/postgres-xtc/baseline-20261028/startup-{tap023-old,fixed,cleanup-old,cleanup-fixed}/`.

## Limits and remaining stage-one work

Global ownership remains red. Wire-protocol cancellation independently fails
in fiber mode; SQL cancellation is not a substitute for that gate. Hard-wait
error policy, full worker/pooled suites, TLS, crash and churn qualification
remain separate work. No scheduler cutover or performance acceptance follows
from these results.

Saved exit-PID handling for pooled logical backends and CleanupBackend's exit
identity are unchanged. This test deliberately avoids those log identities.
Stale-handle replacement, immediate post-boundary exit, arbitrary crash and
postmaster death are not newly qualified here. Existing generation checks and
restart behavior remain; TAP 001's restart control passes.
