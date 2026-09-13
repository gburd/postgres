# Decision: the pooled protocol scheduler is a hand-rolled scheduler-on-libxtc and MUST be transformed to real fibers (Option A, now unblocked)

Date: 2026-09-13. Standing directive: build to the libxtc contract as if bug-free; transform
everything in Postgres that should use libxtc properly; no half-measures/workarounds; file /tmp bug
reports when a documented contract genuinely fails.

## The finding
The pooled protocol scheduler (the DEFAULT under multithreaded=on) is PG hand-rolling a fair,
fd-aware, work-stealing coroutine scheduler ON TOP OF libxtc -- which already IS exactly that. The
hand-rolled layer comprises:
- `runnable_queue` + `parked_protocol_queue` (a bespoke PG run queue)
- `PgRuntimeProtocolSchedulerWaitParkedReads` (a bespoke readiness poll() over parked client fds)
- `PgCarrierLeaseRunnableProtocolBackend` / `PgCarrierYieldRunnableOnBudget` (bespoke lease/preempt)
- `pooled_protocol_carrier_message_budget` + `PG_STEP_YIELD_BUDGET` (bespoke time-slicing)
- the 2.85x-slower "eager sweep every lease" (bespoke fairness)

A pooled carrier is a BARE PTHREAD running a `for(;;)` lease loop; each session runs INLINE to a
STACKLESS protocol-read boundary. `xtc_in_backend_fiber` is FALSE on that pthread.

## Both bugs I chased this session are ARTIFACTS of this hand-rolled layer, not of libxtc
- **Read starvation / lease unfairness** (measured monopoly: 6 of 8 clients got 1 txn, 2 monopolized;
  trace-confirmed: every lease saw `runnable_count=0 parked_count=6`). libxtc's default is plain-FIFO
  zero-overhead scheduling and it offers a proportional-share class (`xtc_exec_class_create`,
  Glommio-inspired vruntime) + work-stealing migration -- i.e. the fairness the hand-rolled sweep
  reinvents at 2.85x cost is a LIBXTC PRIMITIVE we are not using.
- **Write wedge finding #3** (pooled sessions block the whole carrier OS thread on a raw semaphore
  because they are not fibers). This is the exact root cause the Option-A plan
  (MULTITHREADED_SESSIONS_AS_FIBERS_PLAN.md, endorsed by the libxtc team 2026-08-28) was written to
  fix: a real session fiber PARKS in place on socket read / WAL fsync / lock wait, and the loop runs
  the OTHER ready session fibers -- by construction, no cohort-freeze, no starvation.

## The contract-honest design (Option A -- already staged behind `pooled_protocol_fiber_sessions`)
Each pooled session is a long-lived libxtc FIBER on the carrier's exec loop:
- spawn on connect (`xtc_proc_spawn`, migratable => work-stealable across loops for free),
- park IN PLACE on socket read (`xtc_pg_wait_fd`) and on WAL fsync (`xtc_aio_fdatasync`) and on
  LWLock/buffer waits (`ProcSemaphoreWaitFiber`), holding its stack,
- exit on disconnect.
`xtc_self() != none` on the session fiber => `xtc_in_backend_fiber` TRUE => `pg_fdatasync` parks
(already implemented in fd.c). Scheduling, fairness, readiness wakeups, and load-balancing are ALL
libxtc's job. The hand-rolled run queue / readiness poll / message budget / eager sweep are DELETED
(or reduced to the thin spawn/park/exit bridge).

## Why NOW: the blocker is GONE
The Option-A plan was blocked on "the libxtc cross-loop task->state resume race." That is the
lost-wake bug **libxtc v1.44.1 FIXED this session** (verified 0/12 write-heavy fiber-path hangs). Per
the standing directive we build to the libxtc contract as bug-free; the documented contract for
`xtc_proc_spawn` + `xtc_proc_wait_fd` + migratable work-stealing + `xtc_exec_class` fully supports
this design. So Option A is UNBLOCKED and is the correct path -- NOT the 2.85x hand-rolled fairness
sweep, which is exactly the "half-measure / workaround" the directive forbids.

## What this replaces
- The interim eager-sweep fairness fix (branch fairness-fix, WIP 574cfc37f3) is a REFERENCE for the
  confirmed mechanism, NOT the ship path. It stays on its branch, unmerged.
- The message-budget (pooled-gated, extracted at fairness-fix 041dc78f7a) becomes unnecessary once
  sessions are fibers scheduled by libxtc (a fiber yields naturally at every park; time-slicing among
  CPU-bound fibers is `xtc_exec_class`'s job, not a PG message counter). Keep it only if a measured
  need survives the transform.

## Plan / next
Resume the STAGED Option A (MULTITHREADED_SESSIONS_AS_FIBERS_PLAN.md), starting at P-A1 (prototype one
session as a fiber, prove xtc_in_backend_fiber=true and in-place fsync park), and FIX the
thread-per-session PANIC that converges here (postgres.c:7058 "could not lease protocol read park for
same carrier resume", the c>=192 failure). Then P-A2 stack sizing/RSS, P-A3 WAL fsync on the fiber,
P-A4 the write-wedge repro + full apples-to-apples matrix (bar: outright win), P-A5 escalate any
in-place-park failure to libxtc as a /tmp bug. Each phase two-review-gated, process mode byte-for-byte,
A/B neutral-or-better on read-S/CPU.

This is THE north-star move: stop competing with libxtc's scheduler and fuse onto it.
