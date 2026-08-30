# libxtc v1.40.3 TSan follow-up: io_del_fd race CONFIRMED GONE, but a task->state cross-loop race remains (still the concurrent-commit collapse)

Date: 2026-08-30
libxtc: v1.40.3 (rev d0adff7e / fix commit f5b5ca0). Both fixes confirmed compiled in
(__xtc_io_defer_del_fd io_uring.c:374; reap-own-io-before-steal). PG "xtc" +
libxtc v1.40.3 both -fsanitize=thread (clang18/compiler-rt18/system glibc).

--------------------------------------------------------------------------------
## TL;DR
Your two v1.40.3 fixes work: the xtc_io_del_fd / __find_fd / io->fds cross-loop race is
GONE (139 TSan reports on v1.40.2 -> 0 on v1.40.3). Thank you. BUT the 64-client
concurrent-commit collapse PERSISTS (both native and forced-offload, 0-2 nonzero tps
samples out of 15), and a fresh TSan run surfaces the next layer: a NON-ATOMIC
cross-loop data race on `task->state` (and the run-queue link) between a loop's
inbox-WAKE handler and the task's owning loop's dispatch. This is the same
"cross-loop step is unsafe" family, one level up from the fd registry.

## The remaining race (TSan, both stacks) -- task struct at 0x722000038080 (size 120)

### Field +0x18 = task->state (loop.c:637 vs :137)
  WRITE thread T48:  __xtc_loop_step  loop.c:637  `t->state = XTC_TS_RUNNING;`
                       <- __xtc_loop_step_once:969 <- __xtc_exec_worker  (dispatch/run)
  PREV WRITE thread T46:  __xtc_inbox_drain  loop.c:137  `m->task->state = XTC_TS_SCHEDULED;`
                       <- __xtc_loop_step:596 <- __xtc_loop_step_once:969 <- __xtc_exec_worker
  (the XTC_INB_WAKE handler: `if (m->task->state == XTC_TS_PARKED) { m->task->state =
   XTC_TS_SCHEDULED; __xtc_loop_enqueue(loop, m->task); }`)

### Field +0x30 = run-queue link (loop.c:834 vs :357)
  WRITE T48:  __xtc_loop_step  loop.c:834
  PREV WRITE T46:  __xtc_loop_enqueue  loop.c:357  <- __xtc_inbox_drain:138 (same WAKE path)

Two DIFFERENT carrier-loop OS threads write the same task's `state` and queue-link with no
synchronization: loop T46 is draining a cross-thread XTC_INB_WAKE that targets the task,
while loop T48 is dispatching/running that same task.

## Why this happens (our reading)
`task->state` is a plain `int`, written non-atomically by BOTH:
  - the owning loop during dispatch (loop.c:616 `= XTC_TS_SCHEDULED`, :637 `= XTC_TS_RUNNING`), and
  - a PEER loop's inbox-WAKE handler (loop.c:137 `= XTC_TS_SCHEDULED`) when a cross-thread
    xtc_proc_wake / mailbox wake for that task lands in the peer loop's inbox.
You already made the "wake races park" window safe with an ATOMIC wake_pending
(loop.c:146, memory_order_release) -- but the `state` read-check-write at loop.c:136-137
and the loop.c:637 dispatch write are plain, so a WAKE delivered to one loop while the
task runs/dispatches on another races `state` (and then double-touches the run-queue link
via __xtc_loop_enqueue). Net effect: the task's state/queue is corrupted -> a
WALWriteLock-holder fiber's wake is lost or it is left mis-stated -> stranded ->
concurrent-commit collapse (our unchanged gdb signature: WALWriteLock held exclusive by an
off-thread fiber, all loops idle in xtc_io_poll, commits frozen).

## How a WAKE for a running/dispatching task lands on a PEER loop
Under work-stealing + eager rebalance a migratable client fiber runs on a loop other than
the one a cross-thread waker targets (or is mid-migration). The waker posts XTC_INB_WAKE to
one loop's inbox; that loop drains it and mutates task->state while the task is being
dispatched on the loop that currently holds it. (This is the same ownership ambiguity that
caused the fd-registry race; the fd registry is now owner-routed, but task->state is still
touched by whichever loop drains the WAKE.)

## What we RULED OUT (same run)
- xtc_io_del_fd / __find_fd / io->fds: 0 reports (your v1.40.3 fix -- confirmed gone).
- ProcWakeSemaphore proc.c:2464/2476 (25): both under SpinLockAcquire(&proc->sem_fiber_lock)
  -> PG-spinlock-blind FALSE POSITIVE.
- PgRuntimeInstallHotCurrentCells / ClearHotCurrentRootRefs backend_runtime.c (525+): our
  known PG-side hot-current-work-cell item (separate; not this collapse).
- lock.c GrantLock/UnGrantLock/SetupLockInTable/LockAcquireExtended + dlist_* + liburing:
  known-benign PG shmem lock-table / lock-free buckets.
The `__xtc_loop_step` / `__xtc_inbox_drain` task->state race is the one un-suppressed,
non-spinlock, libxtc-internal finding, and it is on the wake/dispatch path.

## Fix direction (your call)
Make task->state transitions cross-loop safe: either (a) make `state` atomic and use a CAS
for the PARKED->SCHEDULED transition in the inbox-WAKE handler (so a concurrent
dispatch write can't be lost), or (b) route the WAKE's state change + enqueue to the task's
OWNING loop (the same owner-thread-routing you used for the fd unregister in v1.40.3),
rather than mutating task->state from whichever loop drains the WAKE. Given v1.40.3 already
established owner-routing for the fd registry, extending that to task->state/enqueue is the
consistent shape. The wake_pending atomic latch is already the right idea; the plain
`state` store beside it is the gap.

## Reproduction
1. PG "xtc" on libxtc v1.40.3, both -fsanitize=thread; multithreaded=on,
   pooled_protocol_carriers=0, fsync=on; TSAN_OPTIONS suppressions = the benign buckets
   above.
2. pgbench -i -s 20; pgbench -c 32 -j 8 -T 40. Collapses to ~0 tps after ~15s; TSan emits
   the __xtc_loop_step/__xtc_inbox_drain task->state race within seconds.
   Non-TSan repro: pgbench -c 64 collapses to 0 tps after ~2s on both native and offload.
We can share tsan_loop_race.txt (full both-stack reports) + the summary table.

## Note
Consistent with the pattern of the last four fixes (v1.39 producer-nudge, v1.40.2
done-flag, v1.40.3 fd owner-routing + reap-before-steal): the cross-loop step is the unsafe
one. This task->state race is the next instance. We expect fixing it closes the collapse on
both paths.
