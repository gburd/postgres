# XTC_AIO_DESIGN: routing backend disk I/O through libxtc's async file path

Status: DESIGN / ANALYSIS ONLY. No source, build, or GUC changes are made by
this document. This is item #6 from AGENTS_XTC.md and the "async I/O" candidate
in Phase 18 (libxtc Deduplication Audit) of plan_docs/MULTITHREADED_PLAN.md.

Scope: analyze whether/how a PostgreSQL backend running as an xtc fiber
(USE_XTC_CARRIER, B_BACKEND on a carrier loop) can route its disk I/O through
libxtc's async file path (xtc_aio_* / xtc_io) instead of PostgreSQL's own AIO
method layer, while preserving process-mode PostgreSQL unchanged.

Phase 18 rule that governs every verdict here: prefer `wrap` (PostgreSQL API
unchanged, xtc under the covers) over changing call sites; keep process mode
green; one primitive family per commit, each with a focused equivalence test.
The recommended shape below is a new `io_method` value, which is a `wrap` at
the method-vtable seam, not a change to any `pgaio_io_*` call site.

--------------------------------------------------------------------------------
## 1. PostgreSQL AIO surface inventory

Source read: src/backend/storage/aio/{aio.c, aio_init.c, aio_io.c,
aio_callback.c, aio_target.c, method_sync.c, method_worker.c,
method_io_uring.c, read_stream.c, README.md} and
src/include/storage/{aio.h, aio_internal.h, aio_types.h}.

### 1.1 The pluggable IO method abstraction

The vtable is `IoMethodOps` (src/include/storage/aio_internal.h). Its members:

- properties: `wait_on_fd_before_close` (bool).
- `shmem_callbacks` (ShmemCallbacks: request_fn / init_fn / attach_fn).
- `init_backend` (per-backend init, optional).
- `needs_synchronous_execution(PgAioHandle *)` (optional; forces sync path).
- `submit(uint16 num_staged_ios, PgAioHandle **staged_ios)` -> int. Advances
  each handle to at least PGAIO_HS_SUBMITTED. Always called in a critical
  section.
- `wait_one(PgAioHandle *, uint64 ref_generation)` (optional). On return the
  handle is in one of PGAIO_HS_COMPLETED_IO / _SHARED / _LOCAL, or was
  recycled. Must not block if already completed/recycled.
- `check_one(PgAioHandle *, uint64 ref_generation)` (optional). Non-blocking
  poll for background completions.

Three concrete implementations register const `IoMethodOps` instances:

- `pgaio_sync_ops` (method_sync.c): `needs_synchronous_execution` always true;
  `submit` is an error path (IO already ran inline). The real synchronous work
  is `pgaio_io_perform_synchronously()` in aio_io.c (pg_preadv / pg_pwritev in
  a START_CRIT_SECTION, then `pgaio_io_process_completion()`).
- `pgaio_worker_ops` (method_worker.c): submit pushes handle IDs onto a
  shared-memory ring (`PgAioWorkerSubmissionQueue`) and wakes an idle
  `B_IO_WORKER` via `SetLatch`; no `wait_one` (completion is done in the worker
  and observed via the handle's ConditionVariable). `needs_synchronous_execution`
  returns true when `!IsUnderPostmaster`, the IO references process-local memory
  (PGAIO_HF_REFERENCES_LOCAL), or the target cannot be reopened in another
  process (`pgaio_io_can_reopen`).
- `pgaio_uring_ops` (method_io_uring.c): submit fills io_uring SQEs and calls
  `io_uring_submit`; `wait_one` and `check_one` drain the owner's CQ under a
  per-context `completion_lock` LWLock, calling `pgaio_io_process_completion()`
  for each CQE. One io_uring instance per backend, created in postmaster during
  startup so any backend can drain any instance. `wait_on_fd_before_close =
  true` (IOSQE_ASYNC IOs must not have their fd closed underneath them).

Selection: `io_method` GUC (aio.h `IoMethod` enum: IOMETHOD_SYNC,
IOMETHOD_WORKER, and IOMETHOD_IO_URING when USE_LIBURING && !EXEC_BACKEND).
`io_method_options[]` in aio.c maps GUC strings; `pgaio_method_ops_table[]`
maps the enum to the const vtable; `assign_io_method()` sets the runtime global
`pgaio_method_ops`. DEFAULT_IO_METHOD is IOMETHOD_WORKER.

Guard already in the tree: a StaticAssertDecl that `io_method_options` and
`pgaio_method_ops_table` stay the same length. Any new method must keep both in
sync or the build fails.

### 1.2 How a backend submits and reaps AIO

Handle lifecycle (states in aio_internal.h `PgAioHandleState`, all transitions
through `pgaio_io_update_state()`):

IDLE -> HANDED_OUT -> DEFINED -> STAGED -> SUBMITTED -> COMPLETED_IO ->
COMPLETED_SHARED -> COMPLETED_LOCAL -> (recycled) IDLE.

- Acquire: `pgaio_io_acquire()` / `pgaio_io_acquire_nb()` (aio.c). Only ONE
  handle may be handed out per backend at a time (`handed_out_io`), so a
  backend can always wait for in-flight IO to free a handle. Handles and their
  iovecs live in shared memory (`PgAioCtl`, `PgAioBackend` per-proc state in
  aio_init.c), sized `AioProcs() = MaxBackends + NUM_AUXILIARY_PROCS` times
  `io_max_concurrency`.
- Define: `pgaio_io_start_readv()` / `pgaio_io_start_writev()` (aio_io.c) set
  `op_data.{read,write}.{fd,offset,iov_length}` then call `pgaio_io_stage()`.
- Stage + submit: `pgaio_io_stage()` (aio.c) runs stage callbacks, then either
  runs synchronously (`pgaio_io_perform_synchronously`) if
  `pgaio_io_needs_synchronous_execution()`, or appends to `staged_ios[]` and
  (outside batchmode) calls `pgaio_submit_staged()`, which calls
  `pgaio_method_ops->submit()` inside START_CRIT_SECTION.
- Batching: `pgaio_enter_batchmode()` / `pgaio_exit_batchmode()` /
  `pgaio_submit_staged()`; up to PGAIO_SUBMIT_BATCH_SIZE (32) unsubmitted.
- Wait: callers hold a `PgAioWaitRef` (from `pgaio_io_get_wref()` BEFORE
  staging) and call `pgaio_wref_wait()`. Internally `pgaio_io_wait()` (aio.c)
  loops on the handle state: in SUBMITTED it calls `pgaio_method_ops->wait_one`
  if present (io_uring) else sleeps on the handle's ConditionVariable (worker
  mode, where the io_worker or another backend completes it).
- Result: the issuer passes a backend-local `PgAioReturn *` to acquire; on
  reclaim the distilled result is copied there. Errors are NOT thrown at
  completion time (completion may run in a critical section in any backend);
  the issuer later calls `pgaio_result_report()` on its PgAioReturn.
- Completion: `pgaio_io_process_completion()` (aio.c) sets result, runs shared
  completion callbacks, broadcasts the CV, and if the completing backend is the
  owner, reclaims (runs local callbacks). It asserts CritSectionCount > 0.
- fd close hygiene: `pgaio_closing_fd()` (aio.c) submits staged IOs on that fd
  and, if `wait_on_fd_before_close`, waits for in-flight IOs using that fd.
- Shutdown: `pgaio_shutdown()` (before_shmem_exit) drains all in-flight IOs.

### 1.3 The io_method GUC and the B_IO_WORKER carriers

`io_method=worker` is the default and the only method that uses separate
processes. IO workers are `B_IO_WORKER` auxiliary processes running
`IoWorkerMain()` (method_worker.c). They register a slot
(`pgaio_worker_register`), then loop: consume a handle ID from the shared
submission queue, `pgaio_io_reopen()` the target's fd in the worker, and run
`pgaio_io_perform_synchronously()`. The pool auto-sizes between io_min_workers
and io_max_workers; growth is signalled to the postmaster via
PMSIGNAL_IO_WORKER_GROW.

Important for xtc: `IoWorkerMain` already has a `threaded_worker` branch
(`PgRuntimeIsThreadBacked(CurrentPgRuntime)`) that skips per-process signal
setup and routes config reload through logical interrupts. Per AGENTS_XTC.md
these workers currently run on the BASE tree's pthread carriers, NOT on xtc.
Also note `pgaio_init_backend()` early-returns for B_IO_WORKER (a worker has no
AIO backend state of its own; it only executes others' handles).

--------------------------------------------------------------------------------
## 2. libxtc async-file API inventory

Source read: /home/gburd/ws/xtc/src/inc/{xtc_aio.h, xtc_io.h, xtc_fs.h,
xtc_iosched.h, xtc_dio_sched.h, xtc_proc.h, xtc_blocking.h} and
man/man3/xtc_aio.3, docs/M16_PG_ADAPTER.md.

### 2.1 The high-level fiber-blocking API (xtc_aio_*)

Four calls (xtc_aio.h), each valid from a fiber running on a loop:

- `int xtc_aio_pread(int fd, void *buf, uint32_t len, int64_t off)`
- `int xtc_aio_pwrite(int fd, const void *buf, uint32_t len, int64_t off)`
- `int xtc_aio_fsync(int fd)`      (data + metadata)
- `int xtc_aio_fdatasync(int fd)`  (data only; the WAL/page flush hot path)

Semantics (xtc_aio.3): the call SUSPENDS the calling fiber and RESUMES it on
completion, keeping the loop live for other fibers. It returns only once the op
has fully completed or failed. Return value is bytes transferred (>= 0; a short
pread at EOF is the partial count, not an error) or a negative errno; fsync
returns 0 or negative errno. The buffer must stay valid until the call
returns, which is guaranteed by simply not returning early -- the fiber parks
on this one op's completion and arms no other waker.

Mechanism chosen at run time, per the loop's io backend:
- io_uring loop: submitted natively (IORING_OP_READ/WRITE/FSYNC), no worker
  thread; the kernel completion re-enqueues the fiber.
- kqueue (FreeBSD/macOS): POSIX AIO with SIGEV_KEVENT, reaped as EVFILT_AIO.
- IOCP (Windows): overlapped ReadFile/WriteFile; fsync has no async form and
  offloads.
- readiness-only backend (epoll, poll, select, event ports, AIX): a regular
  file is not pollable, so the op is offloaded to the blocking thread pool
  (xtc_blocking) -- same API, same observable behavior, but concurrency is
  bounded by the pool thread count (default max(4, CPUs), grows to 64).
- Off a loop entirely: runs inline on the calling thread (always correct).

`__xtc_aio_force_offload(int on)` (and XTC_AIO_FORCE_OFFLOAD=1) forces the
offload path even where a native engine exists -- a test hook to prove the
fallback is behavior-identical.

### 2.2 The low-level engine (xtc_io / xtc_aio_t)

xtc_io.h is the L1 event-notification engine, one backend compiled per binary.
`xtc_io_aio_submit(xtc_io_t *io, xtc_aio_t *a)` submits one async file op
described by `xtc_aio_t { fd, op (XTC_AIO_*), buf, len, off, tag, done, res }`.
Returns XTC_OK if queued (the caller parks until the completion event, tagged
with `a->tag`, arrives on `xtc_io_poll`, then reads `a->res`), or XTC_E_NOSYS
if the backend has no native file completion (caller should offload). The
completion is delivered on the SAME poll loop as fd readiness (event flag
XTC_IO_AIO, tag = the xtc_aio_t's tag, which is an xtc_task_t *). This is what
xtc_aio_* is built on; it is still fundamentally per-fiber (the tag is the
single parking task), not a shared queue any fiber can reap.

### 2.3 Adjacent helpers (not on the critical path yet)

- xtc_fs.h: portable synchronous open/close/stat/rename/dir plus
  `xtc_fs_pread/pwrite/fsync/fdatasync` (BLOCKING; use xtc_aio_* on a loop for
  the hot path). Also direct-I/O alignment/alloc helpers (`xtc_fs_dio_align`,
  `xtc_fs_dio_alloc`) and XTC_FS_DIRECT. Relevant only if PG ever routes
  file OPEN through xtc; today PG opens its own fds via fd.c.
- xtc_iosched.h / xtc_dio_sched.h: an adaptive single-writer write-batching
  scheduler over xtc_aio_pwrite with a genetic batch-size tuner. This overlaps
  conceptually with PG's own read_stream / batchmode and with a WAL writer; it
  is a `defer` candidate, not part of the first step.
- xtc_blocking.h: the thread-pool offload that xtc_aio_* falls back to; also
  directly usable to park a fiber across an arbitrary blocking call.

### 2.4 The one structural mismatch that drives everything below

PG's AIO engine is submit/reap-DECOUPLED, shared-memory, and
COMPLETABLE-BY-ANY-BACKEND: backend A can submit, do other work, and backend B
(or an io_worker) can run the completion; the issuer only later observes it via
a wait-ref and CV. This exists specifically to avoid the deadlock/starvation
described in aio/README.md ("Deadlock and Starvation Dangers due to AIO") and
to allow completion inside critical sections (WAL).

xtc_aio_* is the OPPOSITE model: it is a per-fiber PARK-UNTIL-DONE call. The
issuing fiber cannot hand its in-flight op to another fiber to complete, cannot
start N ops and reap them out of order without N fibers, and cannot complete
another backend's op. `xtc_io_aio_submit` is only slightly lower-level: the
completion is still tagged to exactly one parking task.

Consequence: an "xtc" io_method is a natural fit for the SYNCHRONOUS-shaped
part of PG's API (issuer issues, then issuer waits, on the SAME backend, for
its OWN handle), but it is NOT a drop-in for the cross-backend-completion
contract that io_uring/worker modes provide. This is the central `keep`/`defer`
line in the mapping.

--------------------------------------------------------------------------------
## 3. Mapping table: PostgreSQL AIO concept -> nearest xtc API

Verdict legend: replace (call site changes to xtc), wrap (PG API unchanged, xtc
under the covers), keep (semantics diverge; do not touch), defer (needs a later
phase). "Guard" = the check that fails if the equivalence assumption is wrong.

| # | PostgreSQL concept | Nearest xtc API | Verdict | Guard against wrong equivalence |
|---|---|---|---|---|
| 1 | The `io_method` GUC + `pgaio_method_ops_table[]` selection | new enum value `IOMETHOD_XTC` -> new `pgaio_xtc_ops` | wrap (add a method; no call site changes) | StaticAssertDecl in aio.c that options[] and ops_table[] lengths match; a GUC round-trip test that `io_method=xtc` is only accepted where the carrier is compiled in |
| 2 | `IoMethodOps.submit(batch)` (advance to SUBMITTED, crit section) | `xtc_aio_pread/pwrite/fdatasync` per handle, on the fiber | wrap, but effectively SYNCHRONOUS-on-issuer | Assert `xtc_in_backend_fiber` at submit; assert each handle reaches PGAIO_HS_COMPLETED_* before submit returns (xtc_aio blocks to completion). A test that submit of a batch >1 still completes every handle |
| 3 | `IoMethodOps.needs_synchronous_execution` | n/a (xtc method always "completes at submit") | wrap | Return true for `!xtc_in_backend_fiber` OR `PGAIO_HF_REFERENCES_LOCAL` OR `!pgaio_io_can_reopen` is NOT required here (xtc uses the issuer's own fd, no reopen), but MUST return true when not on a fiber so process-mode aux paths fall back |
| 4 | `IoMethodOps.wait_one(handle, gen)` cross-backend drain | none (xtc parks the issuing fiber only) | keep/defer | If a foreign backend ever waits on an xtc-submitted handle, it must fall through to the CV path; guard = assert the xtc method never leaves a handle in SUBMITTED after submit returns, so no foreign wait_one is ever needed |
| 5 | `IoMethodOps.check_one` background poll | none needed (xtc completes inline at submit) | keep | Assert xtc method has no in-flight-after-submit handles for check_one to find |
| 6 | `pgaio_io_process_completion()` (crit-section completion, any backend) | called by the xtc method in the ISSUING fiber right after xtc_aio returns | wrap | Keep the existing START_CRIT_SECTION around the process_completion call; assert `ioh->owner_procno == MyProcNumber` in the xtc path (issuer completes its own IO) |
| 7 | Short read / partial completion (PGAIO_RS_PARTIAL) | xtc_aio_pread returns partial byte count at EOF, negative errno on failure | wrap | Feed xtc_aio return straight into `ioh->result = (n<0 ? -errno : n)` exactly as `pgaio_io_perform_synchronously` does; a torn-read equivalence test comparing xtc vs sync method result codes |
| 8 | Error encoding (no ereport at completion; PgAioResult) | xtc returns negative errno; PG callbacks encode it | wrap | Reuse the md.c/bufmgr.c completion callbacks unchanged; guard = the same PgAioResult error_data path as sync/worker (do not raise in the xtc method) |
| 9 | `PgAioWaitRef` / `pgaio_wref_wait()` | issuer already blocked inside submit; wref_wait sees COMPLETED | wrap | wref_wait must be a no-op-fast-path when the handle is already COMPLETED_LOCAL; existing aio.c code already handles this |
| 10 | `wait_on_fd_before_close` + `pgaio_closing_fd()` | xtc holds no in-flight op past submit; fd owned by issuer | keep=false | Set `.wait_on_fd_before_close = false` for the xtc method (nothing in flight to wait for); guard = the "no SUBMITTED after submit" assert (#4) makes this sound |
| 11 | fd ownership / reopen in another process (`pgaio_io_reopen`) | xtc uses the issuer fiber's own open fd | keep (no reopen) | The xtc method must NOT set PGAIO_TID targets expecting reopen; it uses op_data.{read,write}.fd directly, same as sync method. Guard = never route an xtc-submitted IO through a foreign executor |
| 12 | io_uring method (`pgaio_uring_ops`) | xtc_aio on an io_uring loop ALSO uses io_uring | keep both | These are two independent io_uring users. Guard = do NOT let a backend use both PG's own ring and xtc's ring for the same fds concurrently; pick one via io_method. Cross-check: process mode keeps IOMETHOD_IO_URING; fiber mode selects IOMETHOD_XTC |
| 13 | worker method (`pgaio_worker_ops`) + B_IO_WORKER carriers | xtc offload pool (on epoll loops) is the moral analogue | keep for now | Keep method_worker path intact for process mode and for aux/threaded workers not on xtc. Guard = process-mode default remains IOMETHOD_WORKER; the xtc method is opt-in and fiber-only |
| 14 | `read_stream.c` batching / prefetch | xtc_iosched (write side) / batchmode | defer | read_stream sits ABOVE the method layer and is method-agnostic; it needs no change. Guard = read_stream regression tests pass unchanged under io_method=xtc |
| 15 | WAL fsync ordering / O_DSYNC path | `xtc_aio_fsync` / `xtc_aio_fdatasync` | defer | WAL write+flush ordering is load-bearing; only route WAL through xtc after data-file reads are proven. Guard = a WAL-ordering test (crash-recovery consistency) under io_method=xtc |
| 16 | shared-memory handle state reap-by-any-backend | none (xtc is per-fiber) | keep | This is the model divergence in section 2.4; do not attempt to make xtc handles foreign-completable |

### 3.1 Can an "xtc" io_method be a new PgAioMethodOps that dispatches through xtc_aio when on a fiber carrier, falling back otherwise?

Yes -- and this is the recommended shape. It fits the vtable cleanly BECAUSE it
maps onto the SYNCHRONOUS-on-issuer subset of the contract:

- `pgaio_xtc_ops.needs_synchronous_execution` returns true whenever
  `!xtc_in_backend_fiber` (i.e. the backend is NOT on an xtc carrier). That
  routes process-mode backends, aux processes, and any non-fiber path straight
  to `pgaio_io_perform_synchronously()` -- the existing correct fallback, no new
  code. It can also just fall to the configured process-mode method by leaving
  the GUC alone in process mode (see section 4).
- `pgaio_xtc_ops.submit` runs, for each staged handle, the matching xtc_aio_*
  call on the current fiber, sets `ioh->result`, and calls
  `pgaio_io_process_completion()` inside a critical section -- structurally
  identical to `pgaio_io_perform_synchronously()` but yielding the carrier loop
  to other fibers while the kernel/pool does the IO. Because the fiber parks
  (not the carrier thread), other backends on the same loop keep running, which
  is exactly the property that made the sync method unacceptable under fork but
  acceptable under fibers.
- No `wait_one` / `check_one`: the handle is COMPLETED before submit returns,
  so `pgaio_wref_wait()` takes its already-done fast path.

The deep limitation this accepts on the first step: the AIO becomes
"asynchronous with respect to the LOOP" (other fibers run) but "synchronous
with respect to the ISSUING BACKEND" (the issuer does not proceed past submit
until its own IO completes). PG's true-async benefits (issue N, do unrelated
work, reap later; cross-backend completion) are NOT obtained yet. Getting them
requires the multi-fiber-per-handle or xtc_io_aio_submit-with-deferred-reap
work that is a later step (section 4, step 3+). This is a deliberate
"defer with invariant": the invariant is "the xtc method never leaves a handle
in SUBMITTED after submit returns", which keeps items #4, #5, #10 sound.

--------------------------------------------------------------------------------
## 4. Concrete staged plan

Overriding constraints (from AGENTS_XTC.md and Phase 18): keep process mode
green; prefer wrap over call-site changes; one primitive family per commit with
a focused equivalence test; do not run this until backends genuinely own an xtc
carrier (they do, for B_BACKEND concurrent-on-pool, per M16_XTC_CARRIER_FINDINGS.md).

### Step 0 (prerequisite, no code): pin the equivalence oracle

The correctness oracle for every step is `io_method=sync`
(`pgaio_io_perform_synchronously`): same fd, same iovec, same
`ioh->result = (n<0 ? -errno : n)`, same `pgaio_io_process_completion`. The xtc
method must produce byte-for-byte identical PgAioResult / PgAioReturn for the
same handle. Write the equivalence test as "run the read_stream / bufmgr read
path under io_method=sync and io_method=xtc and diff the results and buffer
contents." This is the test that ships with Step 1.

### Step 1 (smallest first step): io_method="xtc" that wraps xtc_aio_* on fiber carriers

STATUS: DONE (libxtc v1.1.0).  Implemented in src/backend/storage/aio/method_xtc.c
(IOMETHOD_XTC / pgaio_xtc_ops, gated on USE_XTC_CARRIER).  Verified on meh:
io_method=xtc + shared_buffers=1MB, a 200k-row table scan issues real data-file
reads through xtc_aio_pread on the backend fiber and returns the correct
count/sum with no PANIC/corruption; process mode and non-fiber backends fall to
the synchronous method unchanged.  Covered by scripts/xtc_smoke.sh.  One bug
found and fixed during bringup: submit() must call pgaio_io_prepare_submit()
(advance to SUBMITTED) before running the IO and completing it, exactly like
the io_uring/worker methods -- otherwise pgaio_io_process_completion() PANICs
("waiting for own IO in wrong state: IDLE").

New `IOMETHOD_XTC` enum value + `pgaio_xtc_ops` (a new method_xtc.c), added to
`io_method_options[]` and `pgaio_method_ops_table[]` (keeping the StaticAssert
happy). Behavior:

- `needs_synchronous_execution(ioh)`: return true if not on an xtc backend
  fiber (`!xtc_in_backend_fiber`), OR if `ioh->flags & PGAIO_HF_REFERENCES_LOCAL`
  is set AND the fiber path cannot handle it (it can -- xtc uses the issuer's
  own memory and fd -- so local references are fine; still return true off a
  fiber). Off a fiber, this yields the exact existing sync behavior.
- `submit(n, ios)`: for each handle, map PgAioOp -> xtc call
  (PGAIO_OP_READV/WRITEV over the handle's single-or-multi iovec). xtc_aio_*
  takes one (buf,len,off); for a multi-element iovec either (a) loop per iovec
  element accumulating bytes, or (b) for the first cut, restrict the xtc method
  to iov_length==1 and let >1 fall to `needs_synchronous_execution` returning
  true. Prefer (b) for Step 1 (smallest diff), lift to (a) in Step 2. Set
  `ioh->result`, then `START_CRIT_SECTION(); pgaio_io_process_completion(ioh,
  ioh->result); END_CRIT_SECTION();` -- same as the sync method.
- No shmem callbacks, no init_backend, no wait_one/check_one.
  `.wait_on_fd_before_close = false`.

What Step 1 MUST preserve (and how):
- Process-mode fallback: process-mode backends are not on a fiber, so
  `needs_synchronous_execution` -> true -> `pgaio_io_perform_synchronously`.
  Better still, do not change the process-mode default GUC at all; only fiber
  backends opt in (see "GUC wiring" below).
- The existing method_worker path: untouched; still the default. B_IO_WORKER
  carriers keep running as today (base pthreads, NOT xtc).
- Completion ordering: the issuer completes its own handle immediately at
  submit; `pgaio_wref_wait` sees COMPLETED. No reordering possible because
  nothing is left in flight.
- Error / short-read semantics: identical to sync method by construction
  (item #7, #8).
- WAL fsync ordering: NOT touched in Step 1. WAL still uses whatever the
  process-mode method does; the xtc method only handles data-file readv/writev
  issued by fiber backends. (fsync/fdatasync ops are not even PgAioOp values
  today -- PGAIO_OP only has READV/WRITEV -- so WAL flush does not flow through
  this method yet. See item #15 / Step 4.)

GUC wiring (design note, not implemented here): the cleanest "wrap" is to leave
`io_method` as the user-facing GUC and have the xtc CARRIER select
`IOMETHOD_XTC` for its own backends at fiber entry only if the operator opted
in, defaulting fiber backends to the same method the rest of the cluster uses.
Because `pgaio_method_ops` is a PG_GLOBAL_RUNTIME pointer, a fiber backend can
carry its own method selection without disturbing process-mode backends. The
guard is a check function that rejects `io_method=xtc` when the carrier is not
compiled in (USE_XTC_CARRIER off), mirroring how IOMETHOD_IO_URING is gated on
USE_LIBURING.

Invariants + test for Step 1:
- Invariant A: an xtc-submitted handle is COMPLETED_* before submit returns
  (assert in submit). Protects items #4/#5/#10.
- Invariant B: `xtc_in_backend_fiber` is true for every handle the xtc method
  actually executes (assert). Protects process-mode fallback.
- Test: the Step 0 equivalence test (sync vs xtc, buffer + result diff) plus a
  concurrent-fibers test: two backend fibers on the same carrier loop each
  issue a data-file read via io_method=xtc and both complete correctly (this is
  the property that distinguishes fiber-async from the old fork-sync method and
  is exactly the class the concurrent lost-wakeup work in
  M16_XTC_CARRIER_FINDINGS.md validated for socket waits -- now for file IO).

### Step 2: multi-iovec + writev, still issuer-synchronous

STATUS: DONE (libxtc v1.1.0).  method_xtc.c now handles any-length READV/WRITEV
on a fiber: needs_synchronous_execution() returns false for both ops, and
pgaio_xtc_run_vectored() loops xtc_aio_pread/pwrite per iovec element at
offset+bytes_done (libxtc v1.1.0 has no readv/writev), mirroring
pg_preadv/pg_pwritev short-transfer/error semantics.  Verified on meh: a
500k-row wide-table seqscan through combined multi-buffer readv returns the
correct count/sums with 0 iovec-misassembly rows (per-row md5 check), and the
bulk-update writev path is correct.  Covered by scripts/xtc_smoke.sh
("xtc_aio multi-iovec no misassembly").

Lift the iov_length==1 restriction: loop xtc_aio_pread/pwrite per iovec element,
or use `xtc_io_aio_submit` with an iovec-aware xtc op if/when available. Add
PGAIO_OP_WRITEV coverage. Same invariants; extend the equivalence test to
scatter/gather and writes. Force-offload variant of the test
(`__xtc_aio_force_offload(1)`) to prove the epoll fallback path matches the
io_uring path on the CI host.

### Step 3 (larger, deferred): true issuer-async via deferred reap

To get PG's real async benefit (issue, do other work, reap later) on a fiber,
the xtc method would submit via `xtc_io_aio_submit` tagged to a per-handle
parking task and return with the handle in SUBMITTED, then implement `wait_one`
to park the ISSUING fiber on that op's completion. This reintroduces the
cross-backend-completion question (item #4/#16): another backend waiting on the
handle still cannot drain the issuer's xtc completion, so `wait_one` for a
FOREIGN handle must fall back to the CV and rely on the issuer eventually
reaping. This step is gated behind a design decision and its own deadlock/
starvation test (mirroring README.md's dangers). Defer with invariant: until
this lands, Invariant A (no SUBMITTED after submit) holds and the foreign-wait
question does not arise.

#### Step 3 review against libxtc v1.3.0 (2026-07-06): DEFER, invariant holds

Re-examined with the v1.3.0 API in hand. Conclusion: keep deferring; do NOT
build the full deferred-reap machinery yet. Rationale:

- libxtc offers no batch-submit convenience over the high-level AIO surface:
  `xtc_aio_preadv`/`pwritev` are one-op-per-park (each call parks the issuer to
  completion). True batch-parallel submission would drop to the low-level
  `xtc_io_aio_submit` + `xtc_io_poll` + per-handle `xtc_aio_t.tag` plumbing,
  reaching into the CARRIER LOOP's `xtc_io_t` and managing a reap loop -- exactly
  the deferred-reap surface this step gates.
- It reopens the foreign-drain problem (risk #1) AND the async-kill-mid-read
  problem (risk #2: an `xtc_exit_pid` at the `xtc_io_aio_submit` yield point
  must not let the buffer/fd be reused before the op completes). Both need
  dedicated deadlock/starvation and fault-injection tests before any code.
- The benefit is PERFORMANCE ONLY; correctness is complete today under
  Invariant A. There is no benchmark yet showing the sequential-per-op park
  cost is a bottleneck for the current fiber-backed backends, so building the
  async path now is speculative. (The autovac worker-start-timeout race found
  in item #5 is a reminder that fiber lifecycle corners are subtle and deserve
  a real driving need + test before adding more of them.)

SCOPED FIRST SLICE when this is taken up (smallest real win, no foreign drain):
batch-parallel reap WITHIN a single issuer's `submit(batch)` -- submit all N
staged ops to the loop ring tagged to the ISSUER's own task, park once, reap
all N. The issuer still reaps its own ops (no foreign backend ever drains a
SUBMITTED handle), so Invariant A's spirit is preserved while N ops go on the
ring at once instead of N sequential parks. Gate: a batch>1 correctness test
plus the async-kill-mid-read fault-injection test. Full deferred reap (return
SUBMITTED, `wait_one` on a foreign handle) stays behind the separate design
decision.

### Step 4 (deferred): WAL / fsync path

Add fsync/fdatasync PgAioOp support and route WAL flush through
`xtc_aio_fdatasync` only after Steps 1-3 are proven, with a crash-recovery
WAL-ordering test as the gate (item #15). This is where xtc_iosched (adaptive
write batching) could be evaluated against PG's own batchmode -- but only as a
measured tradeoff, not a default.

--------------------------------------------------------------------------------
## 5. Risks

1. Completion ordering. PG allows completion in any backend and inside critical
   sections; the xtc method sidesteps this by completing on the issuer at
   submit (Invariant A). The risk is a future change (Step 3) that leaves a
   handle in SUBMITTED and then relies on a FOREIGN backend to drain it -- xtc
   cannot do that. Mitigation: the "no SUBMITTED after submit" assert; any
   attempt to relax it must add a real foreign-drain mechanism first.

2. Cancellation on backend exit. `pgaio_shutdown()` drains all in-flight IOs
   before a backend exits. With the issuer-synchronous xtc method there are no
   in-flight IOs at exit, so this is trivially satisfied. But an xtc fiber can
   be killed asynchronously (`xtc_exit_pid`) at a yield point -- and xtc_aio_*
   parks at exactly such a point. If a backend fiber is killed while parked in
   xtc_aio_pread, the buffer and fd must not be reused until the kernel/pool op
   finishes. Mitigation: xtc_aio.3 guarantees the buffer stays valid because
   the call does not return early; combine with
   `xtc_proc_recovery_track_fd` / `xtc_proc_at_exit` so a contained fault
   releases the fd only AFTER the op is accounted. Guard: a fault-injection test
   that kills a backend fiber mid-read and checks no buffer/fd reuse race.

3. fd ownership between the fiber and the io ring. In io_uring mode PG creates
   one ring per backend in POSTMASTER shared memory (method_io_uring.c comment),
   so any backend can drain any ring. xtc's io ring belongs to the CARRIER LOOP,
   not the backend, and multiple backend fibers share one loop's ring. The fd,
   however, is the issuing backend's own (opened via fd.c, dup'd into the
   backend as with the client socket per M16 findings). Risk: two fibers on the
   same loop issuing ops on the same fd, or a fiber migrating loops mid-op.
   Mitigation: M16_PG_ADAPTER.md already pins each backend to ONE xtc_loop for
   its life ("matches the historical fork-per-backend model"); keep that pin.
   xtc_aio parks on one op and arms no other waker, so no migration mid-op.
   Guard: assert the backend does not change loop_id between submit and
   completion (it cannot, since submit blocks to completion in Step 1).

4. Interaction with the existing B_IO_WORKER carriers. io_method=worker and
   io_method=xtc are mutually exclusive per backend (the method vtable is a
   single runtime pointer). The danger is a mixed cluster where fiber backends
   use xtc and everything else uses worker, and an io_worker tries to complete
   an xtc-submitted handle. It cannot, because the xtc method never enqueues
   into the worker submission queue and never leaves a handle SUBMITTED for a
   worker to pick up. Guard: the xtc method must never touch
   `io_worker_submission_queue`; assert Invariant A. Also: io_uring's per-proc
   ring count subtracts MAX_IO_WORKERS on the assumption workers and io_uring
   are never used together (pgaio_uring_procs); the xtc method must not perturb
   AioProcs()/ring sizing (it allocates no shmem), so this stays valid.

5. Buffer lifetime across fiber yields. This is the subtlest. PG shared-buffer
   reads target a pinned buffer in shared memory; local-buffer reads
   (PGAIO_HF_REFERENCES_LOCAL) target process-local memory. xtc_aio parks the
   fiber for the whole op, so the buffer must survive the yield. For shared
   buffers this is fine (pinned, shared memory outlives any fiber). For
   local/stack buffers, xtc_aio.3's contract (buffer valid because the call
   does not return until done) holds AS LONG AS the fiber is not resumed with a
   different stack. Since Step 1 completes on the issuer with no intervening
   resume of THAT fiber on other work, the stack is stable. Risk appears only
   in Step 3 (deferred reap), where the issuer fiber runs other code between
   submit and reap while a local-buffer op is outstanding -- exactly the case
   PGAIO_HF_REFERENCES_LOCAL + `needs_synchronous_execution` exists to forbid
   for methods that cannot handle it. Mitigation: in Step 3, force
   PGAIO_HF_REFERENCES_LOCAL IOs to the issuer-synchronous path (Invariant A)
   even when other IOs go async. Guard: a test that a local-buffer read under
   io_method=xtc never yields the issuer fiber to unrelated work while
   outstanding.

--------------------------------------------------------------------------------
## Appendix: file/function reference index

PostgreSQL (paths under src/):
- backend/storage/aio/aio.c: pgaio_io_acquire[_nb], pgaio_io_stage,
  pgaio_io_needs_synchronous_execution, pgaio_submit_staged,
  pgaio_io_process_completion, pgaio_io_wait, pgaio_wref_wait,
  pgaio_closing_fd, pgaio_shutdown, assign_io_method, io_method_options[],
  pgaio_method_ops_table[], pgaio_method_ops.
- backend/storage/aio/aio_io.c: pgaio_io_start_readv/writev,
  pgaio_io_perform_synchronously, pgaio_io_uses_fd.
- backend/storage/aio/aio_init.c: AioProcs, AioShmemInit, pgaio_init_backend
  (B_IO_WORKER early-return).
- backend/storage/aio/method_sync.c: pgaio_sync_ops.
- backend/storage/aio/method_worker.c: pgaio_worker_ops, IoWorkerMain
  (threaded_worker branch), submission queue, B_IO_WORKER slots.
- backend/storage/aio/method_io_uring.c: pgaio_uring_ops, per-backend ring,
  pgaio_uring_submit/wait_one/check_one, wait_on_fd_before_close.
- include/storage/aio.h: IoMethod enum, DEFAULT_IO_METHOD, PgAioOp,
  PgAioHandleFlags, pgaio_io_* prototypes.
- include/storage/aio_internal.h: IoMethodOps, PgAioHandle, PgAioBackend,
  PgAioCtl, PgAioHandleState, pgaio_*_ops externs.
- include/storage/aio_types.h: PgAioWaitRef, PgAioResult, PgAioReturn,
  PgAioTargetData.

libxtc (paths under /home/gburd/ws/xtc/src/inc, docs under docs/, man under man/):
- xtc_aio.h + man/man3/xtc_aio.3: xtc_aio_pread/pwrite/fsync/fdatasync,
  __xtc_aio_force_offload.
- xtc_io.h: xtc_io_aio_submit, xtc_aio_t, XTC_AIO_* ops, XTC_IO_AIO event flag,
  xtc_io_poll.
- xtc_fs.h: xtc_fs_pread/pwrite/fsync/fdatasync, direct-I/O alignment/alloc.
- xtc_iosched.h / xtc_dio_sched.h: adaptive write-batching scheduler + GA tuner
  (defer candidate).
- xtc_blocking.h: xtc_blocking_run/submit (the offload fallback).
- xtc_proc.h: xtc_proc_wait_fd, xtc_proc_recovery_track_fd, xtc_proc_at_exit,
  loop-pinned fiber identity (xtc_pid_t: loop_id/local_id/gen).
- docs/M16_PG_ADAPTER.md: M16.2 (aio integration), the per-backend loop-pin
  mitigation, and the 16.2a (wrap pgaio over xtc_aio) / 16.2b (io_uring method)
  split that this plan's Step 1 / Step 3 mirror.
