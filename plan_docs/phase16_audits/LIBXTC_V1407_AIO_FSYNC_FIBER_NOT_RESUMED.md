# libxtc report: WAL-fsync fiber parked on xtc_aio_fdatasync (park=-) is never resumed, holding WALWriteLock -- surface #7 on v1.40.7

Date: 2026-09-04
libxtc: v1.40.7 (rev b3baee7 / fix commit c4adc77).  All six prior cross-loop fixes confirmed
in and their signatures GONE (notably: 0 io_del_fd/__find_fd frames -- the v1.40.7 io->fds
spin is fixed).
Consumer: PostgreSQL "xtc" branch, sessions-as-fibers (pooled_protocol_carriers=0).
PG-side: includes our hot-cell thread-safety fix and the ProcSleep bounded-wait guard you
endorsed.

--------------------------------------------------------------------------------
## TL;DR
v1.40.7 fixed the io->fds cycle/spin (thank you -- confirmed, 0 spin frames).  A DIFFERENT,
intermittent hang remains (~50-60% of runs): the ONE backend fiber doing the WAL fsync holds
WALWriteLock, submits xtc_aio_fdatasync, PARKS (xtc_dump shows it as `park=-`: no fd, no
timer -- an aio-completion park), the io_uring worker services the fsync, and the fiber is
NEVER RESUMED.  19 committers then pile on LWLock/WALWrite, they hold row locks while stuck,
and every other session queues behind those rows -> global stall.

This is the aio-completion resume path -- which you verified sound in isolation (0/20 in your
harness).  It fails in this workload, so the trigger is something our shape adds (many
migratable fibers + WALWriteLock held across the parked fsync + concurrent socket fd parks).

--------------------------------------------------------------------------------
## Decisive evidence (all from one caught hang, v1.40.7)

WALWriteLock state:  p/x ((LWLock*)0x7fa34c560b80)->state.value  =>  0x80040000
                     bit31 LW_VAL_EXCLUSIVE set => HELD EXCLUSIVE.

PG wait events (client backends):
    LWLock/WALWrite      19     <-- committers queued behind the lock
    Lock/transactionid    6
    Client/ClientRead     4
    Lock/tuple            2
    IO/WalSync            1     <-- EXACTLY ONE backend is in issue_xlog_fsync == the holder
    -/-                   1

gdb `thread apply all bt`, THREE samples 1s apart -- byte-identical each time:
    XLogWrite=0  issue_xlog_fsync=0  fdatasync=0  xtc_aio=0  XLogFlush=0
    LWLockAcquire=1   iou-wrk=19   xtc_io_poll=31
=> The fsync'ing fiber is on NO OS thread (parked, not spinning -- identical samples with no
   progress, and zero io_del_fd/__find_fd frames so the v1.40.7 spin is not involved).
   io_uring workers (19) are present, i.e. the FDATASYNC was submitted and serviced.

xtc_dump park-kind histogram at the hang:
    park=-      34     <-- parked with NO fd and NO timer == the aio-completion park shape
    park=fd     33     <-- the idle sessions on their client sockets
    park=timer   1
All procs "parked"; no proc `scheduled`/`running`; all 32 loops alive with tasks_run+steals
climbing (the scheduler is healthy, it simply never re-runs this one fiber).

## Why we are confident it is the aio-completion resume
- Exactly one backend reports IO/WalSync (it entered issue_xlog_fsync -> pg_fdatasync ->
  xtc_aio_fdatasync) and that fiber is off every OS thread while its io_uring workers exist.
- Its park kind is `park=-` (no fd, no timer), which is the aio park, not a wait_fd park.
- PG does NOTHING between xtc_aio_fdatasync parking and its return; the resume is libxtc's.
- WALWriteLock stays held exclusive for the whole (multi-minute) stall, so the fiber never
  returns from the fsync call.

## What is DIFFERENT from your 0/20-clean aio harness (likely trigger)
Our shape adds three things at once, which your pure-aio stress may not combine:
 1. The fsync'ing fiber holds a PG LWLock (WALWriteLock) ACROSS the parked
    xtc_aio_fdatasync -- so it cannot be retried/abandoned, and its non-resume is fatal
    rather than merely slow.
 2. ~33 OTHER migratable fibers are simultaneously parked on their own client-socket fds
    (park=fd) on the same loops, with steal churn ongoing (all loops show steals).
 3. The submitting fiber may be work-stolen between submit and completion, so the completion
    lands on a loop other than the one it submitted from (the same class as #1-#6, now on the
    aio completion rather than the fd registry / task->state / timer).
Suggested harness delta: N migratable fibers, each looping {xtc_aio_pwrite; xtc_aio_fdatasync}
on real disk, while a shared "lock" is held across the fdatasync by whichever fiber is
flushing, AND the other fibers are concurrently parked in xtc_proc_wait_fd on their own pipes,
with eager rebalance on.  We expect the stolen-between-submit-and-completion case to strand.

## Reproduction (PG)
1. PG "xtc" HEAD on libxtc v1.40.7, multithreaded=on, pooled_protocol_carriers=0, fsync=on,
   PGDATA on NVMe, 32-vCPU (c6id.8xlarge).
2. pgbench -i -s 50 ; pgbench -c 32 -j 8 -T 50   (also hangs at -c 64, and at -c 16).
3. ~50-60% of runs: commits freeze within ~15s (delta 2-3 per 4s vs ~1600/s healthy) and never
   recover.  Then the signature above.
NOT a workload artifact -- falsifying test: the hang persists at scale=300 (10x fewer row
conflicts) and at c=16 (4x fewer clients), while FORK completes the identical scale=50/c=64
workload at 42,282 tps.

## Ruled out (this session, instrumented)
- v1.40.7's io->fds spin: GONE (0 frames).
- Deadlock detector / timeouts: works (fires correctly; and the hang has no LWLock-timeout
  dependency -- the holder is in fsync, not waiting).
- PG hot current-work cells: fixed (thread-local mode-state).
- Row-lock contention as a cause: refuted (see falsifying test) -- the row-lock convoy is a
  SYMPTOM of the stranded holder.

## Note
Our ProcSleep bounded-wait guard cannot help here: the stuck fiber is not in ProcSleep, it is
inside xtc_aio_fdatasync holding the lock.  Happy to run any instrumented libxtc build or a
patch candidate on this repro -- it reproduces in ~2 minutes per attempt.
