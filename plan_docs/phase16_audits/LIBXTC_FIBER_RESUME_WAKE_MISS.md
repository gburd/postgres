# libxtc bug: parked backend FIBER stranded holding WALWriteLock after its fsync completes -- lost fiber-resume wakeup (both native io_uring AND forced-offload paths)

Date: 2026-08-29
libxtc: v1.40.0 (commit 499773135c / tag v1.40.0; local checkout c55c144)
Consumer: PostgreSQL "xtc" branch, sessions-as-fibers path (pooled_protocol_carriers=0),
adopting your option-A recommendation (run pooled sessions as fibers on the loop pool).
Classification: this LOOKS like a genuine libxtc lost-wakeup on the fiber-RESUME path.
You explicitly asked us to send this: "if a session fiber's xtc_aio_fdatasync is NOT
parking/resuming (carrier still freezes with a FIBER stuck), that WOULD be a libxtc
issue -- send the gdb." Here it is. The fiber DOES park correctly; it is NEVER RESUMED
after its fsync completes.

--------------------------------------------------------------------------------
## TL;DR
A PostgreSQL client backend running as a libxtc fiber acquires an exclusive LWLock
(WALWriteLock), writes WAL, and calls xtc_aio_fdatasync(fd) which parks the fiber. The
fsync COMPLETES (the io_uring CQE arrives / the blocking-pool worker finishes) but the
parked fiber is NEVER RESUMED. It sits suspended off every OS thread, still holding
WALWriteLock. Every other committer (the WAL writer fiber, checkpointer, the load) then
blocks forever on WALWriteLock. All carrier loops sit idle in xtc_io_poll. Permanent
wedge.

- NATIVE io_uring fsync path: wedges INTERMITTENTLY -- 1 in 8 runs of a pgbench scale-100
  init (a single-connection heavy-write COPY + index build + vacuum).
- FORCED-OFFLOAD path (XTC_AIO_FORCE_OFFLOAD=1, blocking-pool fsync): wedges
  DETERMINISTICALLY (every run) -- easier to debug.
Same end state in both: WALWriteLock holder fiber parked and unresumed after fsync done.

This is distinct from the v1.39 xtc_loop_wake fix (that was the cross-thread PRODUCER
side making a peer's queued work runnable). This is the fiber's OWN completion resume:
the work it parked ON finished, and it was not scheduled back.

--------------------------------------------------------------------------------
## Evidence (symbolized gdb, both paths)

### FORCED-OFFLOAD wedge (deterministic), XTC_AIO_FORCE_OFFLOAD=1
Thread census at the wedge (37-loop exec pool):
  - 33x  blk_worker()  in __futex_abstimed_wait   <- blocking-pool workers IDLE (no work)
  - 31x  xtc_io_poll / __xtc_loop_step / __xtc_exec_worker   <- carrier loops IDLE
  -  1x  WalWriterMain -> XLogBackgroundFlush -> LWLockAcquire(WALWriteLock, EXCLUSIVE)
             -> ProcSemaphoreWaitCallback -> PGSemaphoreLock   <- BLOCKED on WALWriteLock
  -  0x  exec_simple_query / PostgresMain / XLogWrite / xtc_aio / fdatasync ANYWHERE
The WALWriteLock holder (the COPY backend fiber) is on NO OS thread. It parked in
aio_offload -> xtc_blocking_run's completion-pipe wait; the 33 pool workers are idle
(its fsync finished) yet the fiber was not resumed.

### NATIVE io_uring wedge (1/8), default
Same census, minus the pool workers:
  -  2x  iou-wrk-*   (io_uring worker threads; the fsync SQE was serviced)
  - 31x  xtc_io_poll  (carrier loops IDLE)
  -  1x  WalWriterMain -> XLogBackgroundFlush -> LWLockAcquire(WALWriteLock) -> parked
  -  0x  exec_simple_query / XLogWrite / xtc_aio / fdatasync ANYWHERE
The holder COPY fiber parked in xtc_aio_fdatasync's `while(!a.done) xtc_yield()` loop;
the io_uring FDATASYNC CQE was produced (iou-wrk serviced it) but the loop did not
resume the fiber (a.done never observed / the fiber not re-scheduled).

### The lock, from gdb
  p *(LWLock*)0x...b80  ->  state.value = 0x80042000  (LW_VAL_EXCLUSIVE set: held)
                            waiters = {head=406, tail=436}  (waiters queued behind holder)
Held exclusive by a fiber that is on no runnable queue and no OS thread.

--------------------------------------------------------------------------------
## Why we believe it is a libxtc resume-side lost wakeup (not PG)

The PG side parks EXACTLY as your API prescribes and does nothing after parking:
- Native: pg_fdatasync (fd.c) -> xtc_aio_fdatasync(fd) on the backend fiber. Your aio_do
  submits io_uring_prep_fsync and parks `while(!a.done){ t->park_requested=1;
  atomic_store(&t->wake_revents,0); xtc_yield(); }`. The completion is on the fiber's OWN
  loop; PG has no hook in that loop. If the CQE is drained but the fiber is not scheduled
  (or a.done's store is not observed on resume), that is inside libxtc.
- Offload: XTC_AIO_FORCE_OFFLOAD=1 -> aio_offload -> xtc_blocking_run. That parks the
  fiber on the completion pipe and a pool worker runs the fsync. Workers are idle at the
  wedge (work done) and the fiber is not resumed -- again inside libxtc.

In both cases the fiber is holding a PG LWLock across the parked fsync. That is BY YOUR
DESIGN advice ("make the ONE fiber that wins WALWriteLock and performs the flush do it
via xtc_aio_fdatasync (parking, so its carrier runs siblings)"). The park is correct; the
non-resume is the bug. Once the holder is not resumed, everything waiting on the lock
wedges -- which is why it presents as a whole-server hang.

Note the level-triggered / a.done invariants look right in the source; the bug is likely
a scheduling-edge race: the completion fires in a window where the parked fiber is
between clearing wake_revents and actually yielding, or the loop drains the completion
CQE but does not mark THAT task runnable (single-shot re-arm interaction on a loop that
then goes idle in io_poll). The intermittent 1/8 native rate + deterministic offload rate
fits a narrow scheduling race that the offload pipe path hits every time.

--------------------------------------------------------------------------------
## Reproduction (deterministic via offload)
1. PostgreSQL "xtc" branch on libxtc v1.40.0, multithreaded=on,
   pooled_protocol_carriers=0 (sessions-as-fibers), fsync=on, synchronous_commit=on,
   PGDATA on local NVMe (xfs), shared_buffers ~40% RAM, 32-vCPU box (c6id.8xlarge).
2. XTC_AIO_FORCE_OFFLOAD=1 in the postgres environment (forces the blocking-pool fsync).
3. `pgbench -i -s 100` (single connection, heavy write). It hangs; `select 1` from a new
   connection hangs too. gdb "thread apply all bt": WALWriteLock held by an off-thread
   fiber, WAL writer fiber blocked on it, all loops idle in xtc_io_poll.
Intermittent native repro: same without XTC_AIO_FORCE_OFFLOAD; ~1/8 inits wedge.
We can share the full backtraces (bt_native_wedge.txt, bt_offload_wedge.txt) and a
scripted repro.

--------------------------------------------------------------------------------
## What we ruled out
- NOT the inline-blocking-fsync wedge of the STACKLESS pooled path (that was our bare
  pthread; this is the fiber path you recommended, and the fsync DOES park).
- NOT the v1.39 producer-side cross-loop wake miss (fixed; we call xtc_loop_wake on the
  pooled queue signal). This is the RESUME of the fiber's own completion.
- NOT a PG post-park action: PG does nothing between xtc_aio_fdatasync parking and its
  return; the resume is entirely libxtc's.

## Ask
Please look at the fiber-resume path for a completed aio/blocking op when the owning loop
is (or is about to go) idle in xtc_io_poll:
1. Native: does draining the FDATASYNC CQE reliably mark the parked task runnable AND, if
   the loop was about to sleep, prevent it sleeping (or wake it)? The 1/8 rate suggests a
   drain-vs-repark or CQE-vs-idle race.
2. Offload: after a blocking-pool worker completes and writes the completion pipe, is the
   parked fiber's loop guaranteed to observe it and reschedule the fiber? The
   deterministic wedge suggests the pipe-readiness is not resuming the fiber (the
   xtc_blocking_run park-side of the same "foreign thread must nudge the loop" contract
   from your v1.39 reply -- the pool worker is a foreign thread w.r.t. the parked fiber's
   loop).
If (2) is "the pool worker must xtc_loop_wake the fiber's loop after writing the pipe,"
that is a libxtc-internal fix (the pool worker is libxtc's own thread). We can also test
any patch quickly on this repro.
