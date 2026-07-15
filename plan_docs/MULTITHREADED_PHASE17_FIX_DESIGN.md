# Phase 17 fix design: fiber-aware ProcWaitOnSemaphore

## Problem (measured, 2026-07-15)
`ProcWaitOnSemaphore(proc, wait_event)` is the scheduler-visible wrapper for
semaphore-backed deep waits (LWLock acquire slow path, buffer content-lock wait,
CLOG/ProcArray group updates). In a pooled backend fiber it currently calls
`ProcSemaphoreWaitCallback` -> `PGSemaphoreLock(proc->sem)` -> raw `sem_wait()`,
which **blocks the whole carrier OS thread**. With N carriers < clients, a
contended-write workload (TPC-B) flatlines at N-wide throughput: EC2 32-vCPU, 8
carriers, TPC-B is pinned at ~24.7k tps across 8/16/32/64 clients while process
scales to ~74k. (Full data: MULTITHREADED_PHASE18_PROFILE.md.)

## Invariants established by reading the callers
- The waiter ALWAYS waits on its OWN sem: every `ProcWaitOnSemaphore(proc,...)`
  callsite passes the caller's own PGPROC (`MyProc`, or the local `proc` alias of
  it). It never blocks waiting on another proc's sem.
- A DIFFERENT process wakes it via `ProcWakeSemaphore(waiter)` ->
  `PGSemaphoreUnlock(waiter->sem)`, after setting the predicate
  (`proc->lwWaiting = LW_WS_NOT_WAITING`, buffer pin flag, group-update done).
- The caller re-checks the predicate in a loop and re-waits on spurious wake
  (`for(;;){ ProcWaitOnSemaphore(...); if (predicate_satisfied) break; extraWaits++; }`).
  This loop is what makes a lost/spurious wake SAFE: a missed wake just means one
  more iteration; an extra wake is absorbed by `extraWaits` re-incrementing the
  sema. THIS EXISTING TOLERANCE is central to the fix's safety.

## The fix (PG-side; no libxtc change -- xtc_notify already exists)
Give each PGPROC a fiber-wake handle + an `xtc_notify`-style parkable signal.
When the waiter is in a backend fiber, PARK THE FIBER instead of blocking:

1. Waiter side (`ProcWaitOnSemaphore`, only when `xtc_in_backend_fiber`):
   a. Publish self's fiber id on `proc` (loop_id/local_id/gen), like the latch
      owner_fiber capture -- at the actual park point (authoritative xtc_self()).
   b. Park via the proc's `xtc_notify` (or `xtc_proc_wake`-park) with an
      infinite/interruptible wait.
   c. On wake, clear the published handle and RETURN (the caller's existing
      predicate re-check loop decides whether to re-park -- we do NOT re-check
      the predicate here; we keep the exact same "return, caller loops" contract
      as the raw sem_wait).
2. Waker side (`ProcWakeSemaphore`): if the target proc has a valid fiber handle,
   `xtc_notify_signal` / `xtc_proc_wake` it (cross-carrier safe). ALSO still
   `PGSemaphoreUnlock(proc->sem)` so a process-mode or non-fiber waiter (or a
   waiter that already fell back to raw sem_wait) is woken too.
3. Process / non-fiber path: unchanged raw `PGSemaphoreLock`/`Unlock`.

## Why xtc_notify (stored-signal) removes the wake race
The classic hazard: waker fires between the waiter's "publish handle" and its
"park" -> lost wake -> hang. `xtc_notify` has STORED-SIGNAL semantics: a signal
delivered before the wait causes the next wait to return immediately. So the
ordering is safe WITHOUT an arm/re-check/park interlock:
  - publish handle (release)  ->  park (xtc_notify_wait)
  - a signal in between is stored; the wait returns at once.
Backstop: even if xtc_notify's stored-signal didn't cover a corner, the CALLER's
predicate re-check loop turns a lost wake into at most one extra park that the
NEXT ProcWakeSemaphore (or the retained PGSemaphoreUnlock, which still posts the
sem) resolves -- but we must confirm the fiber park is also woken by that later
signal, not only by the sema. Design keeps BOTH wake channels live to be safe.

## Open design questions for reviewers (adversarial, please break this)
Q1. WAKE-RACE / LOST-WAKEUP: is publish-then-notify_wait truly safe given
    xtc_notify stored-signal, across CARRIERS (waiter parked on carrier A, waker
    running on carrier B)? Does xtc_notify's stored signal survive a cross-loop
    signal, or is it per-loop? If per-loop, the handle must encode the waiter's
    loop and the signal must target that loop (like owner_fiber_loop).
Q2. HANDLE LIFETIME / ABA: the waiter publishes {loop,local,gen}; between park
    and wake the fiber id is stable (same fiber). But `proc` is a shared PGPROC
    -- could a DIFFERENT backend reuse this PGPROC while a stale handle is
    published? (Waiter clears on wake; but on ERROR/cancel unwind out of the
    park, who clears it?) Need a clear "handle valid only while this fiber is
    parked here" rule + clear-on-unwind.
Q3. CANCELLATION / INTERRUPTS: raw sem_wait is uninterruptible; the LWLock loop
    relies on that (it counts extraWaits, never checks interrupts inside the
    loop). If the fiber park is interruptible (xtc kill/cancel), does that break
    the LWLock protocol (return without the lock, predicate not satisfied ->
    loop re-parks -- probably fine, but confirm no CHECK_FOR_INTERRUPTS is newly
    reachable mid-LWLock).
Q4. Is xtc_notify per-PGPROC allocatable in SHARED MEMORY? PGPROC lives in shmem;
    xtc_notify_t is a heap/loop object. We likely CANNOT store an xtc_notify_t in
    PGPROC directly. Alternative: store only the {loop,local,gen} handle in
    PGPROC (POD, shmem-safe) and use `xtc_proc_wake(pid)` to wake the parked
    fiber -- but then the waiter must PARK in something xtc_proc_wake can wake.
    xtc_proc_wake wakes a fiber parked in xtc_proc_wait_fd or xtc_recv. Neither
    is a clean fd-less park (established last session). SO: either
    (a) eventfd-per-PGPROC parked via xtc_pg_wait_fd (POD fd in shmem, works,
        costs 1 fd/proc), OR
    (b) a private xtc_notify created per-waiter on the carrier (not in PGPROC),
        with the PGPROC holding only the wake handle -- needs the waker to reach
        that notify, which lives on another carrier => back to xtc_proc_wake.
    This Q4 likely DECIDES the mechanism. Reviewers: which is correct for a
    shmem PGPROC woken cross-carrier?
Q5. Does making this fiber-aware REINTRODUCE the hot-row regression? The hot-row
    win came from carriers serializing. If parking frees the carrier, do we lose
    the 2.8x? (Hypothesis: no -- hot-row contention is on a tuple lock resolved
    without a semaphore deep-wait; but MEASURE both workloads in the A/B.)

## A/B gate (must pass before landing)
Re-run the exact MULTITHREADED_PHASE18_PROFILE.md matrix (process vs threaded,
TPC-B + hot-row, 8/16/32/64 clients, 8 carriers): threaded TPC-B must scale
toward process (not flatline); hot-row must stay >= current threaded. Plus: TAP
007 pooled (46/46), a cancellation/timeout stress test, and check-threaded green.

---

## REVIEW OUTCOME (two independent adversarial reviews, 2026-07-15): GO-WITH-CHANGES

Both reviewers traced the ACTUAL libxtc source (not headers) and converged:

### Q4 mechanism DECIDED: per-PGPROC eventfd (primary) + xtc_proc_wake (secondary nudge), mirroring the proven latch/SetLatch path. NOT xtc_notify, NOT bare xtc_proc_wake.
- xtc_notify_t is heap + live pthread_mutex/cond + stack-pointer waiter queue -> CANNOT live in shmem PGPROC. REJECTED.
- bare xtc_proc_wake HANGS: it only fires recv_waker (armed by xtc_proc_wait_fd/xtc_recv), never the private per-call fiber_waiter that notify/sem park on, and it has NO stored-signal -> a wake between publish and the yield inside the park is DROPPED. The caller's for(;;) predicate loop does NOT save this: for LWLock/buffer the releaser wakes exactly once and is gone; the re-check loop only absorbs SPURIOUS wakes, never a LOST one. REJECTED as sole channel.
- eventfd readiness is LEVEL-TRIGGERED/persistent = the real stored-signal: a write before the waiter parks leaves the fd readable so the park returns at once. This closes the publish/park race by construction. The waker MUST write() the fd (load-bearing, also the memory barrier that publishes the predicate); xtc_proc_wake(pid) is only the loop-poke. This is byte-for-byte what SetLatch already does (latch.c:456-483). Cost ~1 fd/proc (few hundred) -- negligible.
- Laziest correct variant (reviewer 2): for a fiber waiter, ProcWakeSemaphore just SetLatch(&waiter->procLatch) and the waiter parks on its latch -> inherits PM-death multiplexing, cancellation, fd-budget-of-one, and already-green code. Prototype this first; fall back to a dedicated per-PGPROC eventfd only if latch re-entrancy in the LWLock call stack is a problem.

### MUST-FIX before landing (union of both reviews):
1. eventfd/latch primary + xtc_proc_wake secondary; waker MUST write the fd. Reject b-alone (hang).
2. Publish only gen-checked {loop,local,gen} in PGPROC, NEVER a raw xtc_waker_t (xtc_waker_wake derefs w->task->state with zero validation -> UAF on a recycled task). The __table_lookup gen check kills ABA.
3. ELIMINATE the +1 sem leak: a fiber waiter parks on the fd, NOT proc->sem. If ProcWakeSemaphore also PGSemaphoreUnlocks, the post is never drained -> sem.count corrupts a later process-mode sem_wait on the recycled PGPROC. Gate so EXACTLY ONE channel delivers to a fiber waiter (per-proc POD "fiber-armed" flag, published release before fd registration, read acquire by waker). Do NOT "keep both live."
4. The fiber park MUST be UNINTERRUPTIBLE under the LWLock/buffer protocol: LWLock waits under HOLD_INTERRUPTS (lwlock.c:1229), and xtc_proc_wait_fd checks kill_pending -> xtc_exit_self longjmp (proc.c:1900/1990). A kill longjmp while lwWaiting!=NOT_WAITING and the proc is still on the wait queue TEARS the LWLock protocol. Defer any pending kill until the lock protocol completes; add NO new CHECK_FOR_INTERRUPTS inside the loop.
5. eventfd (if dedicated) created EAGERLY at PGPROC init (InitProcGlobal), EMFILE handled at startup, fd budget reserved in set_max_safe_fds. No per-wait fd churn.
6. Audit ALL ProcWakeSemaphore cross-backend callers for publish-before-signal: lwlock.c:1043/1806, bufmgr.c:6627, clog.c:657, procarray.c:878. (clog.c:558 / procarray.c:826 raw PGSemaphoreUnlock are same-proc extraWaits fixups -- harmless, but audit vs the #3 rebalance.)

### MUST-VERIFY (hard A/B gate):
7. Re-run the exact TPC-B + hot-row x {8,16,32,64} x {process,threaded,8 carriers} matrix: TPC-B must scale toward process; hot-row must NOT regress. Add sem-post/eventfd-write balance counters (prove #3 under load) + a kill/cancel stress test (prove #4). TAP 007 46/46; check-threaded green.
8. Carrier-teardown-with-parked-fiber must not UAF the loop proc table (supervisor must drain/kill parked backend fibers before xtc_loop_fini).

### DOC FIX: this file cited xtc-1.3.0; the branch links xtc-1.22.0. All libxtc claims re-verified against 1.22 source in the reviews (sync.c/proc.c/task.c). The 1.3.0 path was only a header-doc read; semantics confirmed unchanged for notify/sem/proc_wake/wait_fd.

### Nice-to-have: hook the fiber park INSIDE the existing PgSuspend/ProcSemaphoreWaitCallback seam (proc.c:2130) rather than branching in ProcWaitOnSemaphore -- reuses the wait-completion publication already wired to ProcWakeSemaphore.

### DECISION: implement the SetLatch-reuse variant first (laziest correct, max code reuse), with the must-fixes; if LWLock-call-stack latch re-entrancy fails, fall back to a dedicated per-PGPROC eventfd. Re-review the implementation diff (not just the design) with the same two-reviewer gate before merge.

---

## DIFF-LEVEL RE-REVIEW (two independent, 2026-07-15): NO-GO -- 3 blockers, redesign required

Both reviewers independently reached NO-GO on the first implementation (commit
5eb4c1194d0).  Convergent critical defects:

### BLOCKER 1 (both) -- kill-longjmp tears the LWLock protocol [my design claim was FALSE]
xtc_pg_wait_fd -> xtc_proc_wait_fd LONGJMPS via xtc_exit_self on kill_pending, at
TWO points (libxtc proc.c:1897 up-front, :1990 after yield).  A supervisor/cancel
kill while parked unwinds ProcSemaphoreWaitFiber with lwWaiting!=NOT_WAITING, the
proc still on lock->waiters, HOLD_INTERRUPTS leaked -> wait-list corruption + a
later LWLockWakeup proclist_delete of a gone proc.  HOLD_INTERRUPTS gates PG's
CHECK_FOR_INTERRUPTS, NOT libxtc's xtc_exit_self (orthogonal).  My header comment
asserting "does not longjmp" was wrong -- verified against libxtc source.
FIX: park must be UNINTERRUPTIBLE -- either an xtc_proc_wait_fd variant that does
not consult kill_pending, or defer the xtc kill across the wait (honor crit_depth).
Likely needs a libxtc primitive (verify-before-requesting: check if xtc already
has a no-cancel park or a crit-section that suppresses xtc_exit_self).

### BLOCKER 2 (both) -- stale sem_fiber_armed => process-mode HANG + fd leak
"Waker owns disarm" is UNSOUND: the waker sets the predicate under pg_write_barrier
BEFORE its ProcWakeSemaphore body runs, so the waiter can observe the predicate
(via a spurious wake) and BREAK the caller loop while armed is STILL true and no
waker has disarmed.  Then: (2a) a late waker's write(fd) leaks the eventfd counter
across PGPROC recycle; (2b) worse -- if the recycled PGPROC is later used by a
NON-FIBER (process-mode) backend, ProcWakeSemaphore sees the stale armed==true,
takes the fd branch, and does NOT PGSemaphoreUnlock(proc->sem) -> the process-mode
sem_wait() is never posted -> HANGS FOREVER.  InitProcess resets sem but NOT
sem_fiber_armed/handle/fd.
FIX: waiter disarms + drains on predicate-loop exit (carefully, not reopening the
race); InitProcess AND InitAuxiliaryProcess reset sem_fiber_armed=false + drain
sem_wake_fd next to PGSemaphoreReset.

### BLOCKER 3 (reviewer 1) -- extraWaits over-posts the sem for fiber waiters
The fiber never posts proc->sem, but SPURIOUS wakes make extraWaits>0, and the
callers' tail loop `while(extraWaits-- >0) PGSemaphoreUnlock(proc->sem)`
(lwlock.c:1347, bufmgr.c:6648, clog.c:557, procarray.c:825) posts it anyway ->
phantom count -> lost wait when the recycled PGPROC later takes the non-fiber path.
My ProcWakeSemaphore gate did not cover this caller-side re-post.
FIX: guard the four post-loops with `if (!xtc_in_backend_fiber)`.

### HIGH/MEDIUM (reviewer 2) -- arm/disarm race + ordering
Non-atomic arm-check(unbarriered proc.c:2218)/park vs disarm/write reopens a lost
wake on re-park (single-wake group-update paths have no retry to rescue it).  Make
sem_fiber_armed a pg_atomic flag with acq/rel; publish the {loop,local,gen} handle
as a versioned unit; consider a per-PGPROC spinlock serializing arm/disarm/drain.

### CONFIRMED OK: prepared-xact sem_wake_fd=-1 guard (self-caught fix); gen-check
makes a stale xtc_proc_wake a safe no-op; eventfd drain vs concurrent write is safe
in isolation (level-triggered).

### VERDICT / PLAN
Do NOT ship 5eb4c1194d0.  The lock-free "waker owns disarm" hand-off is the root
of B2/B3/HIGH.  REDESIGN toward a serialized arm/disarm (per-PGPROC spinlock around
the arm-check+park-decision and the disarm+wake), + waiter-side disarm-on-exit, +
InitProcess/InitAuxiliaryProcess reset, + the extraWaits guard, + an uninterruptible
park (B1 -- the hard one; may need a libxtc no-cancel park, verify first).  Re-run
the two-reviewer diff gate after the redesign, plus a cancel-while-parked TAP and
the A/B matrix.  The commit stays local (unpushed) until it passes.
