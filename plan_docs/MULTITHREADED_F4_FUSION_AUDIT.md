# Fusion increment F4+ -- audit + xtc_pool evaluation (2026, branch base origin/xtc c7388d73fee)

## Executive summary

- **Part 1 (audit): the primitive dedup is COMPLETE.** Every wait/block primitive
  reachable from a client-backend fiber hot path yields its carrier OS thread
  (via the xtc seam) instead of blocking it. No genuine carrier-blocking gap
  found. The two known non-yield sites (spinlock `pg_usleep`, standby throttle)
  are intentional and out of scope.
- **Part 2 (xtc_pool): NOT a clean fit -- no code written.** `xtc_pool`'s
  contract ("checkout blocks the calling FIBER until a caller-owned resource is
  free; fibers check out and RETURN") is a fiber-side concurrency limiter for
  borrow/return resources. The carrier pool is a set of long-lived, never-returned
  process-lifetime OS threads whose count is elastically grown to a cap -- the
  "spawn-workers half", which `xtc_pool.h` itself says is the SUPERVISOR
  (`xtc_svr`/`xtc_orc`, `xtc_sup_opts_t.max_children`), not `xtc_pool`. Forcing it
  is exactly the "loop-resident checkout / lifecycle mismatch" the task warned
  against. Left the hand-rolled carrier bookkeeping in place.

Because Part 2 implements nothing, there is nothing to build/A-B, and no EC2
instance was launched (the audit is a static source trace; all evidence is
file:line in the tree at c7388d73fee).

---

## Part 1 -- fusion-completeness AUDIT

Method: for each wait/block primitive reachable from a client-backend fiber hot
path, trace whether, when the caller runs as an xtc backend fiber
(`xtc_in_backend_fiber` / migration-stable `sem_fiber_backed`), it yields the
fiber via the xtc seam, or blocks the carrier pthread.

Two seams carry essentially every backend wait:

- **Latch/WaitEventSet seam** -- `WaitEventSetWaitBlock()`
  (`waiteventset.c:1454`): when running as a fiber (`xtc_in_backend_fiber ||
  !xtc_pid_is_none(xtc_self())`) it parks on `set->epoll_fd` via
  `xtc_pg_wait_fd()` and then harvests with a non-blocking `epoll_wait(...,0)`,
  instead of blocking the carrier in `epoll_wait(..., cur_timeout)`. Wake side:
  `SetLatch()` (`latch.c:384`) writes `owner_wakeup_fd` (the epoll-registered
  self-pipe/eventfd that unparks the fiber) AND nudges the owning loop
  cross-thread via `xtc_proc_wake(owner_fiber)` (`latch.c` ~132). Bidirectional
  and complete. `WaitLatch()` always funnels through `WaitEventSetWait()`
  (`latch.c:291`), so there is no latch bypass.
- **PGSemaphore (LWLock) seam** -- `ProcWaitOnSemaphore()` (`proc.c:2358`): when
  `proc == MyProc && proc->sem_fiber_backed && sem_wake_fd >= 0`, it calls
  `ProcSemaphoreWaitFiber()` (`proc.c:2302`) which arms under a spinlock and
  parks on the eventfd via `xtc_pg_wait_fd(sem_wake_fd, ...)` -- the carrier
  thread is freed. Wake side `ProcWakeSemaphore()` (`proc.c` ~2400) wakes the
  fiber via the eventfd and does NOT post `proc->sem` (no over-post of the
  counting semaphore). The channel decision keys off migration-stable
  `sem_fiber_backed`, not the per-thread `xtc_in_backend_fiber`, so a
  work-stolen fiber and its waker never split-brain the wake channel.
  `ProcSemaphoreAbsorbExtraWaits()` (`proc.c:2532`) is centrally guarded to
  no-op for a fiber-backed proc (`proc.c` ~2527), covering all 8 spurious-wake
  call sites.

### Audit table

| primitive | fiber-yields? | evidence (file:line) | gap? |
|---|---|---|---|
| Latch / WaitEventSet (`WaitEventSetWaitBlock`) | YES -- parks on `epoll_fd` via `xtc_pg_wait_fd`, non-blocking harvest | `waiteventset.c:1454-1490` (park); `latch.c:291` (WaitLatch->WaitEventSetWait); `latch.c:384,~101,~132` (SetLatch fd write + `xtc_proc_wake`) | none |
| LWLock contended sleep (`LWLockAcquire`/`WaitForVar`/`AcquireOrWait`) | YES -- all sleeps route through `ProcWaitOnSemaphore` -> `ProcSemaphoreWaitFiber` (eventfd park) | `lwlock.c:1308,1484,1702` -> `proc.c:2358` -> `proc.c:2302` | none |
| ConditionVariable (`ConditionVariableTimedSleep`) | YES -- waits via `WaitLatch(MyLatch,...)` -> Latch seam | `condition_variable.c:166` | none |
| ProcSleep / heavyweight lock wait | YES -- waits via `WaitLatch(MyLatch,...)` -> Latch seam | `proc.c:1682` | none |
| ProcWaitForSignal | YES -- `WaitLatch(MyLatch,...)` -> Latch seam | `proc.c:2246` | none |
| ProcArrayGroupClearXid (group XID clear) | YES -- follower sleeps via `ProcWaitOnSemaphore` -> fiber eventfd park | `procarray.c:815` -> `proc.c:2358` | none |
| XactLockTableWait | YES -- implemented as a heavyweight `LockAcquire` on the xact tag; contended wait is ProcSleep/Latch seam | `lmgr.c:116,160,194` (LockAcquireExtended) -> `proc.c:1682` | none |
| buffer-IO wait (`WaitIO`, BM_IO_IN_PROGRESS) | YES -- `pgaio_wref_wait` (AIO CV -> WaitLatch seam) or `ConditionVariableSleep` on the buffer IO CV -> WaitLatch seam | `bufmgr.c:7376` (`WaitIO`); `aio.c:645` (`pgaio_io_wait` CV sleep) | none |
| buffer pin-cleanup wait (`LockBufferForCleanup`) | YES -- `ProcWaitForSignal`/latch wait; pin release sets the latch | `bufmgr.c:6919` -> `proc.c:2246` -> Latch seam | none |
| AIO fiber read/write (io_method=xtc from fiber) | YES -- `xtc_aio_preadv/pwritev` PARK the issuing fiber on the xtc loop; IO completes synchronously in submit(), so no CV wait even reached | `method_xtc.c:66,106` (`pgaio_xtc_needs_synchronous_execution` false only in-fiber; submit parks) | none |
| WAL flush / insertion wait (`WaitXLogInsertionsToFinish`) | YES -- `LWLockWaitForVar` -> `ProcWaitOnSemaphore` -> fiber eventfd park | `xlog.c:1621` -> `lwlock.c:1484` -> `proc.c:2358` | none |
| SyncRep (`SyncRepWaitForLSN`) | YES -- `WaitLatch(MyLatch,...)` -> Latch seam | `syncrep.c:342` | none (walsender is aux, not a pooled client fiber; still yields if it were) |
| ProcSignalBarrier (`WaitForProcSignalBarrier`) | YES -- `ConditionVariableTimedSleep(pss_barrierCV,...)` -> WaitLatch -> Latch seam | `procsignal.c:720` -> `condition_variable.c:166` | none |

### Intentional non-yield sites (noted, NOT fixed -- per task)

- **Spinlock backoff** (`s_lock` -> `pg_usleep`): a spinlock is held for a
  handful of instructions; parking a fiber there would cost far more than the
  spin. Correctly left as a carrier-level micro-sleep.
- **Standby recovery-conflict / throttle `pg_usleep`**: coarse policy sleeps,
  not a wait-for-wake boundary. Out of scope by directive.

### Runtime-plumbing mutexes (F2 territory, not a wait boundary)

`reloptions.c:638` (`ThreadedRelOptionsMutex`) and `pg_locale.c:259`
(`PgLocaleMutex`) are raw `pthread_mutex_lock` over shared caches. They are NOT
wait/wake boundaries: they hold the lock for a microsecond cache
lookup/insert, so a carrier is blocked only for that tiny critical section, not
a wait. They are the exact "reloptions / pg_locale guards" the roadmap files
under F2 (lock/CV plumbing dedup, `xtc_lwlock`/`xtc_sync`), not F4. No fiber
wait-boundary gap.

### Audit conclusion

The coordinator's belief holds: SetLatch/WaitEventSet, LWLock-via-PGSemaphore,
ConditionVariable-via-WaitLatch, and AIO-via-xtc_aio are all already fused, and
every other backend wait boundary funnels into one of those two seams. **No
client-backend fiber wait boundary blocks its carrier OS thread. No genuine gap
to close in F4.** Re-converting any of these would duplicate work and risk the
working wake machinery, exactly as the task warned.

---

## Part 2 -- xtc_pool for the carrier pool: evaluated, NOT a clean fit

### xtc_pool contract (from installed header, v1.32.0 @ 563329f)

`xtc_pool.h` (verbatim intent):

> "A bounded resource pool: a fixed set of **caller-owned resources**
> (connections, buffers, handles) that **fibers check out and return**.
> Checkout **blocks the calling fiber** (up to a timeout) when all resources are
> busy... This is the checkout/return half of the connection-pool pattern; the
> supervisor's bounded dynamic pool (`xtc_sup_opts_t.max_children`) is the
> **spawn-workers half**."

API: `xtc_pool_create(capacity)`, `xtc_pool_add(resource)`,
`xtc_pool_checkout(timeout_ns) -> resource` (blocks the fiber),
`xtc_pool_checkin(resource)`, `available()`, `capacity()`.

### What the hand-rolled carrier pool actually is (launch_backend.c)

- `pooled_protocol_carrier_count` (line 324): a monotonically-incremented
  **count** of carrier threads spawned, capped at
  `PgRuntimePooledProtocolCarrierLimit()`.
- `backend_pooled_protocol_start_one_carrier()` (line 1203): creates ONE
  long-lived carrier OS thread with `pg_thread_create` (line 1311), then
  `pooled_protocol_carrier_count++` (line 1328). Elastic growth:
  `backend_pooled_protocol_maybe_start_carrier_for_work()` spawns another
  carrier when the session queue outgrows the idle carriers, up to the cap.
- Carriers are **process-lifetime** threads that run an `xtc_exec` loop; they
  are **never returned, never checked in, never destroyed** during normal
  operation. What they consume is *sessions*, which flow through a
  producer/consumer **queue** (`backend_pooled_protocol_dequeue`, already
  F2-fused onto `xtc_amutex` + `xtc_notify`), not a checkout/checkin slot pool.

### Why it does not map (the three hard mismatches)

1. **Checkout blocks a FIBER; a carrier is not a fiber.** `xtc_pool_checkout`'s
   blocking-until-free is implemented by parking the calling fiber on an xtc
   loop. A carrier is the raw pthread that *hosts* the loop -- it has no fiber
   to park, and nothing on the carrier-creation path runs on a loop. This is
   precisely the "xtc_pool assumes loop-resident checkout" disqualifier called
   out in the task.
2. **No checkin in the carrier lifecycle.** xtc_pool is borrow -> use ->
   return. Carriers are spawn-once, run-forever. There is no return step to map
   to `xtc_pool_checkin`; modeling a never-returned resource as a pool member
   just leaves capacity permanently checked out -- the pool would add nothing
   over the existing counter and would break the moment a carrier did exit.
3. **The carrier "pool" is the SPAWN half, which the header assigns to the
   SUPERVISOR, not xtc_pool.** `xtc_pool.h` explicitly says elastic
   grow-to-a-cap of workers is `xtc_sup_opts_t.max_children`
   (`xtc_svr`/`xtc_orc`). That is the roadmap's later, separate "xtc_svr/xtc_orc
   supervision" increment -- not F4, and not xtc_pool.

### If anything were pooled, it would be sessions -- and those already have a queue

The leased/returned resource in this design is the **session** (enqueue ->
dequeue -> run-as-fiber -> exit), not the carrier. That is a producer/consumer
**queue**, and it is already xtc-fused (F2: `xtc_amutex` + `xtc_notify`). It is
not a bounded borrow/return slot pool either, so xtc_pool is not the right shape
for the session layer any more than the carrier layer.

### Decision

**Leave the hand-rolled carrier bookkeeping in place. No code.** The honest
answer: xtc_pool solves a different problem (bounded borrow/return of
caller-owned resources by fibers). The carrier layer's natural libxtc fusion is
the supervisor behaviour (`xtc_svr`/`xtc_orc`, `max_children`) -- a distinct,
larger, later increment that owns spawn/restart/backoff -- when the runtime is
ready for it. Forcing xtc_pool here would be a fusion for its own sake, against
the roadmap's own methodology.

---

## Validation

Part 2 wrote no code, so there is nothing to build, regress, or A/B. No EC2
instance was launched (static source audit only). Process mode and the threaded
runtime are untouched at c7388d73fee.
