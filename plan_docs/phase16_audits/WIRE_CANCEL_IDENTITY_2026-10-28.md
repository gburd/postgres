# Authenticate and route wire cancellation by backend identity

## Failure and identity join

SQL cancellation passed, but psql wire cancellation did not reach session
fibers. The original psql020 test ran its full 180-second sleep and failed;
this was not an external timeout. The log said the requested PID matched no
process.

A fresh live join established the mismatch. BackendKeyData advertised logical
ID 6 with a 32-byte key. Its ProcSignal slot held backendId 6 but pss_pid 69866,
the postmaster OS PID. The keys compared equal. The registry and
PM_CHILD_CARRIER_POOLED_LOGICAL entry referred to the same backend pointer;
MyProc agreed on slot, OS PID and backendId. Old SendCancelRequest searched
only pss_pid, so it never found the authenticated logical target. A fork
control advertised its actual OS PID and returned SQLSTATE 57014.

No raw key was printed. The diagnostic session was deliberately stopped after
observation, not counted as a completed acceptance run.

## Routing boundary

SendCancelRequest now validates positive identity and bounded nonempty key
length, then authenticates the target under its ProcSignal slot spinlock.
Identity matching uses the existing namespace-aware helper. The key must have
exactly the stored length and pass timingsafe_bcmp. A wrong-key numeric match
does not stop the scan: an OS PID and logical ID may share a number.

Capture full backend ID, routing kind and OS PID under the lock; release it
before logging, registry access or wakeup. Logical cancellation uses the
existing QUERY_CANCEL registry path, only in the owning postmaster address
space. Never pass a logical ID to kill(), or fall back to OS signaling on a
registry miss. Genuine process/fallback targets retain process-group SIGINT.

This changes only SendCancelRequest. It does not change cancellation-key
creation, the registry, teardown ownership or backend publication. Pre-auth
callers still use no LWLocks.

## Same-fixture negative and candidate controls

TAP 025 speaks protocol 3.0 and 3.2 directly. It covers correct, wrong,
cross-paired, truncated, long and malformed keys; invalid IDs; replay after
close; observed PGPROC-slot reuse; overlapping cancellation and disconnect;
unrelated-session usability; ReadyForQuery and same-session recovery. Keys
stay opaque in memory.

The four-carrier authentication lane remains four. A separate saturated
pooled-eight lane first opens eight unfinished SSLRequest startups to grow
native carriers, completes observer and target on two of those same sockets,
and retains six startup holders. It asserts eight carriers actually started
before testing the dead-end cancel route. Merely configuring eight would not
establish that capacity. Established sessions alone cannot fill the doubled
PMChild pool.

The first full old run exposed an independent fallback startup GUC replay
failure. That was fixed separately in bc347bbe4d, without changing the wire
fixture's settings. The subsequent full old-wire run is the authoritative
negative control:

| Lane | Old | Fixed |
| --- | ---: | ---: |
| Fork | 181/181 | 181/181 |
| Fiber, one loop | 159/182 | 182/182 |
| Fiber, two loops | 159/182 | 182/182 |
| Pooled four | 147/169 | 169/169 |
| Genuine process fallback | 183/183 | 183/183 |
| Pooled eight, saturated dead-end route | 22/23 | 23/23 |
| Total | 851/920 | 920/920 |

Old fails exactly 69 missing-57014 assertions, with normal finite query
completion. All other nested assertions pass. Candidate passes all 920,
without skips, bailout, timeout, debugger intervention or changed expectations.
Old takes 286.7 seconds and candidate 32.4 seconds because valid cancellations
now interrupt the sleeps; this is not a throughput benchmark.

An initial POSIX CPU-count import was unavailable on Debian Perl. The fixture
now uses checked getconf output; the 16-CPU host skips no saturation coverage.
A separate early runner falsely aborted after reading buffered output in the
wrong order. Both excluded attempts were retained. Neither changes the exact
old/fixed fixture identity above.

## Actual psql and regression gates

Unmodified psql020 passes 2/2 in each of fork, fiber one-loop, fiber two-loop
and pooled-four mode, each under one second. Its 180-second timeout default
was retained. Both query failure and the expected user-request cancellation
message are required; an unrelated connection error cannot satisfy the test.
Live hashes, configuration, startup/routing logs and threaded ring descriptors
establish the lanes. An optional observer SQL query raced socket readiness;
that failed probe was not counted as SQL evidence of effective settings.

| Guard | Result |
| --- | --- |
| Fresh setup | 3/3 |
| Backend-runtime regression | PASS |
| Process core | 239/239 |
| Fresh fiber core, four loops, pool0/sync | 239/239 |
| Fresh pooled core, pool4/sync | 239/239 |
| TAP 019 / 020 | 13/13 / 9/9 |
| TAP 021 / 022 | 94/94 / 253/253 |
| TAP 023 / 026 | 173/173 / 347/347 |
| TAP 001 / 003 | 128/128 / 43/43 |
| Lifecycle | PASS, 431 mappings |
| Global lifetimes | Still RED, 39 declarations / 36 symbols |

All execution used the existing EC2 Debian 13 correctness host, assertions,
debug symbols, injection points, explicit uring, C locale and nofile 131072.
No local tests/builds. Six wire-test postmasters have live candidate identities,
maps, limits and configuration; all five threaded configurations have actual
ring descriptors. The full fiber/pooled runs also have successful live SQL
mode/IO/loop checks. No expected output was changed.

## Provenance

Base: bc347bbe4ddb54e8fa4636bf109bbaae3af3b946. This audit was added afterward.
Only procsignal.c and the backend executable rebuilt; installation changed
only bin/postgres. Existing helper, SQL and library bytes stayed unchanged.

- Old installed backend: `7c802ed789ea2b7b94a50796af0d813f2489ba363a92970fce9264fdb9164384`
- Fixed installed backend: `85134032ae4cdc943e11a709020c684f3791977ba2016f750b9db39cd160feba`
- Exact TAP 025: `a63c700425f9408e8c31bd9fb1adca32c0a4d1c98a232bfdcf6e137d1f831841`
- libxtc source: `95041727ab08dca6b1474aabca38e2738c66e6ed`
- Installed libxtc: `3d8876b3fa20de5ed9af4345531a912bbd19a81d5289b49602a76d7fea72da7e`
- Full old-wire archive: `9013a803c65fab425334508598cd95cddfd0735d33c70c289997401051af053a`
- Fixed archive: `d6976f955aa2df77db14709b2ce524e2232f9cd26329922c7e07ebb6592d6bd6`

Archives fetched and checked, including all 39495 candidate manifest files.
Reports and raw artifacts are under
`/scratch/postgres-xtc/baseline-20261028/{wire-cancel-identity,fallback-fixed,wire-fixed}/`.
The complete old installation remains at `/mnt/nvme/control-pre-wire-cancel/pg`.
Preparation's quadratic manifest hashing was stopped after confirming no build
or PG workload remained; verification resumed against the preserved control.
That delay was runner overhead, not a PostgreSQL stall.

## Limits

The inherited registry/my_proc/latch teardown race is not sanitizer-qualified;
no failure appeared in the overlap tests. Numeric OS/logical collisions, OS
PID reuse, mixed process-receiver/logical-target topology, Windows/non-XTC and
native regular THREAD builds were not executed. Locked capture and fail-closed
routing are the source-reviewed boundaries, not claims of exercised cases.

Hard-wait fail-stop, broader worker/world/TLS/crash/churn qualification and
remaining global ownership are separate stage-one work. No scheduler cutover
or sustained performance acceptance follows from these results.
