# Remove hand-rolled reimplementations of libxtc; use libxtc directly

Date: 2026-09-13. Directive: the pooled protocol scheduler (a PG-hand-rolled fair, fd-aware,
work-stealing coroutine scheduler on top of libxtc) should never have been built -- remove it and use
libxtc. Find and remove every other place PG re-implemented a libxtc feature, using the libxtc feature.

This is the inventory + removal plan. Ground rule (standing directive): build to the libxtc contract as
if bug-free; a genuine contract failure gets a /tmp bug report, NOT a PG-side workaround. Process mode
stays byte-for-byte; each removal A/B neutral-or-better.

## libxtc surface we should be USING (v1.45.0), so we do not re-implement it
- Scheduler / fibers: xtc_proc_spawn(+_link/_monitor), migratable work-stealing, xtc_exec_class
  (proportional-share, Glommio-inspired), xtc_proc_wait_fd (per-fiber fd park), xtc_yield*, xtc_loop_wake.
- OTP behaviours: xtc_svr (gen_server), xtc_orc/xtc_sup (supervisor), xtc_pool (resource pool),
  xtc_fsm (state machine), xtc_reg (name registry), xtc_pg (process groups), xtc_xproc (external proc).
- Sync: xtc_amutex, xtc_arwlock, xtc_rwlock, xtc_sem, xtc_barrier, xtc_notify, xtc_gate, xtc_credit
  (backpressure), xtc_abort_source/token, xtc_lrlock (left-right), xtc_lockmgr (deadlock-detecting).
- Data: xtc_chan (mpmc/mpsc/oneshot/broadcast/watch/demand channels), xtc_chash, xtc_cskip, xtc_future.
- IO: xtc_aio (pread/pwrite/fsync/fdatasync), xtc_io, xtc_iosched, xtc_dio_sched, xtc_bdev, xtc_blocking.
- Observability: xtc_tail (dial9), xtc_inspect, xtc_dump/xtc_panic, xtc_alloc_audit, xtc_log.

## ALREADY fused (do NOT touch -- these correctly use libxtc)
- Pooled queue LOCK + idle-wait wake: already routed to xtc_amutex + xtc_notify (F2, pg_xtc_carrier.c
  xtc_pg_pooled_queue_*). The PRIMITIVE is libxtc; the ABSTRACTION on top (the run queue itself) is not.
- dfmgr rendezvous critical section: xtc_amutex_static (dfmgr.c:283) -- correct.
- Threaded GUC critical section: xtc_amutex_static (guc.c:183) -- correct.
- WAL fsync from a fiber: pg_fdatasync -> xtc_aio_fdatasync when xtc_in_backend_fiber (fd.c) -- correct.
- Backend/aux fibers: xtc_proc_spawn_monitor + per-loop supervisor (pg_xtc_carrier.c) -- correct
  (thread-per-session already runs real fibers).
- LWLock/sem wait as a fiber park: ProcSemaphoreWaitFiber (proc.c) -- correct.

## TARGET 1 (PRIMARY): the hand-rolled pooled protocol scheduler -- REMOVE, replace with libxtc fibers
The entire `backend_pooled_protocol_*` family + the PgRuntimeProtocolScheduler run-queue is PG
re-implementing libxtc's scheduler. Files: launch_backend.c, tcop/postgres.c,
utils/init/backend_runtime_backend.c, include/utils/backend_runtime.h. What to delete once sessions are
libxtc fibers (Option A / MULTITHREADED_SESSIONS_AS_FIBERS_PLAN.md):
- runnable_queue + parked_protocol_queue (PgRuntimeProtocolScheduler*) -> libxtc schedules parked fibers
  directly; the "queue" is libxtc's runnable set. DELETE PopRunnable/Collect/MarkRunnable/Lease/Yield.
- PgRuntimeProtocolSchedulerWaitParkedReads (a bespoke poll() over parked client fds) -> each session
  fiber parks on its OWN fd via xtc_pg_wait_fd; libxtc's io_poll does readiness. DELETE.
- pooled_protocol_carrier_message_budget + PG_STEP_YIELD_BUDGET (bespoke preemption/time-slicing) ->
  a fiber yields naturally at every park; CPU-bound time-slicing is xtc_exec_class's job. DELETE
  (keep ONLY if a measured need survives the fiber transform).
- the eager-sweep "fairness" (fairness-fix branch, 2.85x) -> NEVER MERGE; libxtc work-stealing +
  xtc_exec_class provide fairness. Already flagged do-not-merge.
- backend_pooled_protocol_enqueue/dequeue (bespoke work queue) -> a new connection spawns a session
  fiber directly (xtc_proc_spawn on a chosen loop); no PG queue. Consider xtc_chan demand-channel ONLY
  if a postmaster->loop hand-off queue is still needed for accept; otherwise delete.
- the stackless PG_STEP_PARK_PROTOCOL_READ lease/resume machinery (PgSessionStaging*,
  PgCarrier*ProtocolReadPark, the postgres.c:7058 PANIC) -> a fiber HOLDS its stack and parks in place;
  no lease-on-resume. DELETE the stackless park; the PANIC disappears with it.
ORDER: land the fiber-per-session model first (P-A1..P-A4), THEN delete this. You cannot remove the
scheduler before its replacement runs, or the pooled default has nothing to run on. Owned by the
optionA-fibers worktree.

## TARGET 2: demand-grow carrier pool -> xtc_pool (candidate, evaluate during Option A)
backend_pooled_protocol_{start_pool,start_one_carrier,maybe_start_carrier_for_work,maybe_request_grow}
+ pooled_protocol_carrier_count is a hand-rolled resource pool with demand growth. libxtc has xtc_pool
(create/checkout/checkin/add/available/capacity). EVALUATE: once sessions are fibers on the exec loop,
is a PG "carrier pool" even needed, or does xtc_exec's N-loop pool + work-stealing subsume it? If a pool
is still needed (e.g. bounding concurrent sessions), use xtc_pool or xtc_credit rather than the
hand-rolled counter+grow. Decide with evidence in P-A2/P-A3. Postmaster-thread-affinity constraints
(carrier_count/wake-fd are postmaster-owned) must be re-checked against xtc_pool's model.

## TARGET 3: audit sweep for other reimplementations (do AFTER Target 1, lower risk)
Candidates to check against libxtc, remove if hand-rolled:
- Any PG-side name->pid/worker registry that duplicates xtc_reg.
- Any hand-rolled backpressure/credit counter that duplicates xtc_credit.
- Any bespoke request/reply RPC between postmaster and workers that duplicates xtc_svr/xtc_fsm.
- Latch/LWLock/CV/AIO dedup onto xtc primitives (the north-star F4+ item) -- each A/B-measured,
  one behaviour at a time, kept only if neutral-or-better.
These are the aggressive-fusion F4+ items; gate them on Option A being live so we measure against the
fiber baseline, not the hand-rolled one.

## Sequencing
1. Option A P-A1..P-A4 makes pooled sessions real libxtc fibers (in flight: P-A1, agent on optionA-fibers).
2. THEN delete TARGET 1 (the hand-rolled scheduler) -- it becomes dead code the moment the fiber path
   is the default. This is the bulk of the "remove it" mandate; it is a DELETION, the laziest and best fix.
3. Evaluate TARGET 2 (xtc_pool) during/after the transform.
4. TARGET 3 audit sweep once the fiber baseline exists.
Do NOT delete Target 1 before step 1 lands (nothing to run on). Do NOT merge the fairness-sweep or the
message-budget as permanent -- they are the workarounds this directive removes.
