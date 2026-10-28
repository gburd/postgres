# Same-loop fiber latch wake, 2026-10-28

`SetLatch` used pthread equality to choose `WakeupMyProc` and suppress an
explicit libxtc wake. Two fibers on the same loop have separate selfpipes;
that path can write the caller's pipe instead of the sleeping owner's.

For an in-process fiber owner, write its recorded wakeup fd regardless of
pthread equality, then call `xtc_proc_wake` with its generation-bearing ID.
Non-fiber and foreign-process routing is unchanged. This also avoids using an
owner pthread recorded before migration to select the current fiber's pipe.

## Reproducer and controls

New `test_backend_runtime/t/019_same_loop_latch.pl` runs fresh fork and one-loop
fiber servers. It verifies the logical launcher is waiting, checks a separate
one-second sleep deadline without a test setter, then creates a tablespace and
database and moves the database. The move must finish before a ten-second
statement timeout; an external bound prevents a failed timer hanging the test.
The final catalog check verifies the move actually happened.

On Debian 13 EC2, with assertions and injection points, exact libxtc
`95041727ab08dca6b1474aabca38e2738c66e6ed`:

| Candidate | Result |
| --- | --- |
| Before latch fix, code from `3cbf38e7dc` | 12/13; only fiber database move fails with statement timeout |
| Latch fix | 13/13, no skips |
| Fork control in both runs | 6/6 |
| No-setter sleep deadline in both models, both runs | pass |
| Separate tiny DDL reproduction, before fix | fiber timeout twice; fork pass |
| Same DDL after fix | fiber 3/3; fork pass |
| Core process regression after fix | 239/239 |
| Backend-runtime regression after fix | pass |

The TAP differential captured live executable paths and hashes for all four
servers. Old postgres SHA256 starts `50dcc4`; fixed postgres SHA256 is
`622e5a37b35e60161e95a04c764273974207f07298e6df17dd1b22893ec1225f`.
Both loaded the same libxtc. Tests completed in about 14 seconds before the fix
and four seconds afterward; no outer timeout supplied the failure.

## Remaining limits

The full four-loop fiber regression passed the earlier database-barrier stage
but stalled later, after 95 emitted verdicts, building `btree_test_expr_idx`.
Early and final captures show a carrier inside `WaitForParallelWorkersToAttach`,
with different inner frames (`shm_mq_get_sender`, `GetBackgroundWorkerPid`).
This does not prove a particular spinlock is held. Other carriers were polling
and a new SQL connection timed out. The harness captured before ending the run;
fast shutdown succeeded. Full threaded correctness remains red.

The original pre-fix barrier capture lasted longer than the launcher's normal
180-second deadline. The focused timeout control passes, but it does not
explain that longer run's missing timeout recovery. No libxtc defect is claimed.

`xtc_proc_wake` takes a library mutex and is not async-signal-safe. Independent
review found no supported asynchronous handler newly reaching this fiber wake
path: threaded backends skip normal backend handler installation, postmaster
handlers use the non-fiber postmaster latch, and SIGURG only writes a pipe.
Do not claim carriers always mask signals: existing abort/error paths restore
`UnBlockSig`; that broader signal-mask debt remains outside this fix.

Evidence is retained under `/scratch/postgres-xtc/baseline-20261028/`, including
`evidence-barrier-focused`, `evidence-latch-candidate`, and the TAP differential
bundle. No performance measurements or model-default changes were made.
