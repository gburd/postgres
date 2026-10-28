# Parallel attachment wait, 2026-10-28

`WaitForParallelWorkersToAttach` waited only for workers not yet marked started.
A started worker with no sender on its error queue caused an immediate retry.
On a fiber carrier, that polling can prevent the launch supervisor or worker
needed for attachment from running.

Scan every worker, preserve the existing stopped-worker/error checks, then wait
once if attachment is incomplete. Queue attachment and worker state changes
already set the leader's latch. Remove the old inner wait: retaining it alongside
a second wait could consume a notification and sleep again before rescanning.
This follows the existing finish-wait structure and applies to both fork and
fiber callers.

## Focused failure and identity capture

The exact empty-table expression-index case from `btree_index.sql`, with
`min_parallel_table_scan_size=0` and `max_parallel_maintenance_workers=4`,
reproduced twice on fresh one-loop fiber servers before the fix. CREATE INDEX
never completed; a ten-second SQL timeout did not rescue it, and the external
60-second limit ended each run. Four-loop focused controls passed three times;
fork passed. All requested and launched one worker for the empty table.

Read-only GDB captured this chain on the second one-loop run:

- Parallel handle slot 1, generation 1 matched shared worker PID 9, STARTED,
  error-queue sender NULL, one launched worker and zero attached.
- The supervisor's queued spawn message referenced a `BackendThreadStart`
  matching the worker's DSM handle, notify ID, ordinal and name.
- That backend was preconstructed but not entered: `fiber_entered=0`,
  `start_claimed=0`, no PGPROC and no latch. It had no libxtc worker proc yet.
- The pinned supervisor task was SCHEDULED in the same loop's inbound wake
  queue; the leader was RUNNING in the attachment scan.
- Loop dispatch count remained 83 across two captures separated by a resumed
  one-second interval. The same spawn payload remained queued. Both tail
  captures had zero dropped events.

This identifies the queued supervisor, not an already-entered worker paused
before attachment. Sampling `SpinLockAcquire` in one stack did not prove a
particular spinlock was held; later samples reached other polling instructions.

## Separate startup-publication defect

`start_bgworker` excludes `PostmasterChildIsThread` from immediate PID
publication, but fiber children use `PM_CHILD_CARRIER_POOLED_LOGICAL`. They
therefore publish STARTED immediately after enqueue. The explicit logical
startup-complete path already supports both kinds.

That predicate is unchanged here, so the attachment test still exercises the
observed failure. Startup visibility needs a separate repair and control.
Even correct publication precedes sender attachment, so it does not replace
the wait in this patch.

## Validation

Dedicated Debian 13 EC2, matching libxtc
`95041727ab08dca6b1474aabca38e2738c66e6ed`, assertions and injection points.
New TAP `020_parallel_attach.pl` uses fresh fork and one-loop fiber clusters,
an external deadline and launch-log evidence to reject serial fallback.

| Check | Result |
| --- | --- |
| TAP 020 on old attachment loop, latch fixed (`1a5c325d3c`) | 8/9; only fiber completion assertion fails |
| TAP 020 after attachment wait repair | 9/9 |
| Core process regression after repair | 239/239 |
| Backend-runtime regression | pass |
| TAP 019, same-loop latch wake | 13/13 |

Independent source review found no blocker in notification ordering, worker
failure handling, sender detachment, generation checks or process callers.
All builds/tests ran on EC2; no local build was used. Full threaded regression
is still being collected and is not green at this checkpoint. Early collection
reached actual EXPLAIN output differences rather than the old attachment stall;
those differences remain failures until evaluated, not waived by this fix.

Raw evidence and scripts are under
`/scratch/postgres-xtc/baseline-20261028/`. Focused identity archive SHA256:
`de7e04c46218ac06e8ec4f2460675c23fd142cc19ac00e6bfa5a2f800c57bdc4`.
Authoritative old TAP evidence is `evidence-parallel-tap-red`, run `red2`
(5 ms live-identity polling captured both postmasters; the first run missed the
brief fork process). Startup-publication, hard wait-error handling and remaining
ownership failures are still open. No performance acceptance or model cutover
is claimed.
