# Write-wedge keystone root cause: an aux-process raw-semaphore wait starves the carrier OS thread that owns a backend fiber's io_uring poll

Date: 2026-09-12. Diagnosis by agent 082077eb (verified-io_uring Debian, ran out of turns before
committing a fix; box torn down; this preserves its findings + the verified fix location).
Root cause independently re-verified in source (line numbers below).

## What was ruled OUT (both prior hypotheses for "finding 2")
Neither of the suspected mechanisms is the bug:
- **NOT the `LW_FLAG_WAKE_IN_PROGRESS` CAS race** and **NOT proclist corruption.** `LWLockWakeup`'s
  waiter walk and `ProcWakeSemaphore`'s fd hand-off both worked correctly.
- The write to `sem_wake_fd=7` landed, and the io_uring poll completion for it was sitting in the CQ
  (`res=1`, ready), stable across a 5s resample. So the LWLock wake FIRED and the CQE is READY.

## The actual root cause (a FOURTH distinct mechanism)
The stuck COPY backend (`pgbench -i` `COPY pgbench_accounts FROM STDIN`, backendId 8) is a migratable
fiber whose fd-readiness poll (fd 7) was registered on **loop 0**. Loop 0's polling OS thread is the
SAME carrier thread that also runs **WalWriter** as a fiber.

WalWriter is an auxiliary process, and `InitAuxiliaryProcess` (proc.c:721) unconditionally sets
`MyProc->sem_fiber_backed = false` (**proc.c:842** -- verified; confirmed at runtime:
`PGPROC[136] WAL_WRITER: sem_fiber_backed=False`). So when WalWriter does
`LWLockAcquire(WALWriteLock)` and blocks, `ProcWaitOnSemaphore` takes the **raw blocking
`PGSemaphoreLock`** path (`ProcSemaphoreWaitCallback`), which blocks the ENTIRE OS THREAD, not just
the fiber (confirmed: that thread's utime/stime were flat in `/proc/<pid>/task/<tid>/stat` --
genuinely blocked, not spinning).

With loop 0's OS thread frozen in a raw semaphore wait, `xtc_io_poll` never runs again on that loop,
so the COPY backend's already-ready completion (sitting in loop 0's CQ) is **never reaped**, and its
fiber never resumes -- even though the LWLock is free and every libxtc wake mechanism fired
correctly. The wedge is a **carrier-OS-thread starvation via aux-process/backend-fiber colocation**,
not a lost wake.

Relation to the prior diagnosis's "finding 3" (raw-semaphore pooled carriers): same family (a raw
`PGSemaphoreLock` blocking a carrier thread) but a different trigger -- this one hits
THREAD-PER-SESSION mode via an AUX process colocated with a backend fiber on one carrier thread, not
just pooled-protocol sessions.

## The fix (verified location; NOT yet implemented)
`InitProcess` sets `sem_fiber_backed = xtc_in_backend_fiber` (**proc.c:613**), so a regular backend
fiber's LWLock wait goes through the fiber-aware `ProcSemaphoreWaitFiber` (park, don't block the OS
thread). `InitAuxiliaryProcess` (proc.c:842) instead hardcodes `false`, so aux fibers block the
whole carrier thread.

**Fix:** make aux processes that run as fibers and can block on contended LWLocks (WalWriter,
BgWriter, Checkpointer -- all seen blocked in `PGSemaphoreLock`) fiber-backed too: at proc.c:842,
set `sem_fiber_backed` from `xtc_in_backend_fiber` the same way InitProcess does (mind the
`sem_wake_fd >= 0 && !PG_BACKEND_WAS_FORKEXECED` guard InitProcess uses). Then their LWLock waits
park instead of freezing the carrier thread that another fiber's poll depends on.

Care required: confirm aux fibers are spawned with proper loop-pinning/migratability semantics
before flipping this; process mode is untouched (guarded by `sem_fiber_backed`/`USE_XTC_CARRIER`).
Re-run the c=8 `pgbench -i -s 300` repro before/after; add a regression test.

## Why this matters for the north star
This is the write-path keystone. It gates the write benchmark on BOTH schedulers (thread-per-session
via aux colocation; pooled additionally via finding 3). Fixing it is the write-side analogue of the
starvation fix on the read side -- together they are what could take fiber from at/below fork to
ahead of it.
