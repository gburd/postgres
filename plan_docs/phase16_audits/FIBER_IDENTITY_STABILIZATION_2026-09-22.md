# Fiber identity stabilization, 2026-09-22

## Landed

`c5fb2b8a35` replaces the PG-owned TLS backend-fiber boolean with the documented
current-proc userdata predicate. All executable consumers were audited. Userdata
is cleared before releasing its embedded PgCarrier owner; final exit retains the
existing durable classification. The non-backend xtc_self wait fallback remains.
Independent source review found no blocker.

A deterministic ordered sibling/supervisor test demonstrates why TLS is wrong:
A parks with the flag true, the supervisor inherits true, B exits and clears it,
then A resumes with false. The new split carrier-family test calls the actual
predicate and checks identity after sibling exit and after owner release. The
agent ran an actual-predicate negative control; its report and runnable probes
are in `/tmp/xtc-fiber-identity-audit-20260922.md`.

## Validation

- Integrated build and Meson setup: passed.
- `test_backend_runtime/regress`: passed, including the new predicate test.
- `018_dead_end_backend`: 16/16 passed.
- Process regress: 239/239 passed in 114 seconds on the final retry. An earlier
  invocation hit the tool's 180-second deadline on a busy shared host; its server
  was explicitly stopped before retrying. Do not count that attempt as green.
- Lifecycle guard: passed (180 fields/buckets, 36 resets, 427 owners).
- Global guard: still red, 42 unclassified declarations (down from 44 because
  the stale TLS definition/extern were removed). No baseline changes.
- Full fiber suite: still NOT green.

Both fiber attempts used fresh clusters, four libxtc loops, the full parallel
schedule and `threaded_smoke.conf`, with nofile raised to 131072. The first
completed numeric (3905 ms) and 78 tests before the 240-second limit. The second
completed 95 tests before the 600-second limit. No completed test was reported
`not ok` in these partial outputs, but neither attempt finished its active group.
These runs show progress beyond the prior numeric stop, not proof of its cause
or resolution of the historical write wedge.

## New diagnostic lead and limits

After the longer run timed out, a GDB stack captured a carrier in
`shm_mq_get_queue -> WaitForParallelWorkersToAttach -> _bt_begin_parallel`.
The BGWH_STARTED branch in WaitForParallelWorkersToAttach rechecks the queue
sender without a wait when the sender is still NULL. This is a reachable busy
retry and a candidate carrier-starvation mechanism. A single stack does NOT
prove repeated execution, the blocking worker's loop placement, or root cause.
Other executor stacks were inside liburing polling.

Capture occurred AFTER the timeout had sent smart shutdown, so it is not a
pristine pre-shutdown capture. The libxtc commands explicitly failed due to
missing debug information; no proc/park/queue conclusion can be inferred from
them. The SQL inspection also failed: the Nix shell's temporary socket directory
was gone after shell exit. It is not evidence of an unresponsive SQL executor.

Explicit immediate shutdown stopped both test servers. This is not a crash
recovery or shutdown-cleanliness certification. No matching test server was
intentionally left running.

Artifacts:
- `/tmp/xtc-identity-integrated.log`
- `/tmp/xtc-identity-process-retry.log`
- `/tmp/xtc-identity-fiber-regress.log` and corresponding directory
- `/tmp/xtc-identity-fiber-long.log` and corresponding directory
- `/tmp/xtc-identity-long-gdb.txt`
- `/tmp/xtc-identity-global-check.log`

## Next bounded work

1. Reproduce parallel startup on a small standalone query with a stable socket
   directory, capture before any timeout signal, and use matching unstripped
   libxtc debug information. Verify queue-sender notification and worker loop
   dependency before changing the wait. Keep fork as control.
2. Complete the two-flag WAL/WAIT FOR LSN owner migration. The earlier agent
   stopped without a patch; its worktree remains clean, nothing integrated.
3. Remaining ownership, sustained churn and EC2 acceptance remain outstanding.
   AWS last returned InvalidClientTokenId; this checkpoint made no AWS resource
   changes and no performance claim. The libxtc requirements report is unchanged.
