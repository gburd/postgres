# v1.49.2: same-task duplicate fd registration with matching deferred delete

## Observed failure, not a complete historical RCA

On a fresh Debian 13 EC2 host, PostgreSQL's fiber regression workload still
stalls with libxtc v1.49.2. GDB now identifies the original registration error:
`xtc_io_reg_fd` returns **XTC_E_INVAL (-1)** with an existing live registration
for the **same fd and same task tag**. `xtc_proc_wait_fd` translates it to
**XTC_E_INTERNAL (-6)** for the infinite wait. PG incorrectly treats that hard
error as a benign wake and retries indefinitely.

In the final capture, the exact fd is also queued for deferred unregister on
that same I/O object. This is stronger evidence than our September 17 sampled
return-code report, but it is not a captured history of how the entry was left
behind. Please review the cross-loop unregister/re-register contract.

## Exact evidence

Run `full5`, before any test timeout or server shutdown signal:

```
WAIT_RESULT fd=447 rc=-6 revents=0 timeout_ms=-1
REG_INPUT fd=447 interest=13 io=0x563a58fef120 tag=0x563a5910d520
io->tail_loop_id = 3
io->n_pending_del = 1
io->has_pending_del = 1
io->pending_del[0] = 447
__current_proc->pid = {loop_id=3, local_id=5, gen=1}
existing node = {fd=447, interest=13, tag=0x563a5910d520,
                 is_wakeup=0, dead=0, ...}
xtc_io_reg_fd return = -1
```

The stack is `WaitLatch -> WaitEventSetWaitBlock -> xtc_pg_wait_fd ->
xtc_proc_wait_fd -> xtc_io_reg_fd`. A prior independent run (`full4`) showed
the same-tag duplicate for fd446 and one pending deletion, but did not inspect
that deletion's fd; do not claim it was fd446. Run `full3` stopped at the PG
return site four times: fd61, rc=-6, revents=0, timeout=-1, via
`LWLockAcquireOrWait -> ProcWaitOnSemaphore -> ProcSemaphoreWaitFiber`.
These are different runs, not one simultaneous complete state capture.

GDB breakpoints observed values without editing source or forcing deletion.
After the first error breakpoint we disabled it, stopped at the next
registration, walked `io->fds`, and used `finish` to observe the actual return.
The full5 pending-delete entry and registration were read in the same stopped
process. There was no scheduler-drain intervention or standalone reproducer.

## Relevant source and hypothesis

Reviewed revision: 542a67d9ea37a425a2ab7d6b11dff8993bd42871.

- `src/io/io_uring.c:404-414`: registration rejects `__find_fd(io, fd)` as a
  duplicate before adding a node.
- `__xtc_io_del_fd_async`: foreign-loop deletion queues the fd and wakes the
  owner; the queued deletion is drained by `__drain_pending_del` at the start
  of `xtc_io_poll` (line686).
- `src/ptc/proc.c:2520`: registration failure is translated to INTERNAL on the
  infinite-timeout path.

Candidate sequence: a migrated wait returns after queuing deletion on the old
owner; the task later attempts registration on that owner before its next poll
has drained the queue. The same-task pending registration is rejected; PG's
error-as-wake retry prevents the owner from reaching the drain. The captured
state is consistent with this, but the prior migration/unregister chronology
has not been traced end-to-end. No claim that simply draining by fd is a safe
library fix: registration generations and fd reuse need review.

## Controls and limits

- Explicit uring configure; matching pinned headers/library; unstripped debug
  library and matching source. Source tar SHA256 verified before extraction.
- PG process suite: 239/239 passed on this EC2 build. Backend-runtime regression
  and dead-end connection TAP (16 checks) passed.
- Same first parallel group in process mode: 21/21 passed.
- Fiber `test_setup + numeric` alone: 2/2 passed.
- Fiber first-group attempts: two captured stalls (group1/group2), then two
  successful 21-test runs (group3/group4). Scheduling-sensitive, not a claim of
  deterministic failure on every run.
- Full-suite attempts fiber2/full3/full4/full5 stalled at different points;
  the bounded runner collected state first, then terminated pg_regress and
  requested immediate server shutdown. These are incomplete suites, not
  hundreds of independent failures.
- An initial harness attempt (`fiber1`) forgot to create the regression
  database under `--use-existing`; all errors were missing-database errors.
  Discarded as a harness failure, retained in artifacts.
- libc debug symbols established the hot epoll sample used **timeout=0**,
  returning 0; it was not proof of a blocking epoll syscall. Earlier backtrace
  names alone could not make that distinction.
- Tail buffers overwrote old records (e.g. fiber2: 17665 dropped). No lost-wake
  or absent-event claim is based on these tails. `xtc-stranded` labels are not
  used as proof; library-sync parks may have no fd/timer label.
- PG's mapping of hard errors to WL_LATCH_SET is our integration defect. The
  public wait cleanup contract does not excuse that unbounded retry.
- No stock performance measurement, fixed-core threshold, library-exclusive
  blame, or proof that every historical wedge has this cause.

## Narrow request

Please test the documented auto-unregister contract across native migration,
with immediate same-fd reuse and a pending foreign-loop delete on the target
owner. Require a control that distinguishes same-task prior registration from
another live waiter and covers fd close/reuse and generations. Can a successful
wait return while a later same-task wait can fail solely because its earlier
registration is pending deletion? If this is intended, document the required
caller protocol; PG should not have to call private poll/drain internals.

Also preserve/document the underlying registration failure rather than hiding
all causes behind INTERNAL. We will separately fix PG's hard-error policy at
an error-safe boundary, retaining process-wide fail-stop for unsafe shared
state rather than retrying or performing arbitrary cleanup.

## Environment and retained artifacts

- AWS hotdog account585335547908, us-east-1, c6id.4xlarge (16 vCPU), Debian13,
  kernel6.12.107+deb13-cloud-amd64, local instance-store XFS.
- PG33bb489bbb7e10c5e67ac371a5c68db017d89d0f, assert-enabled debugoptimized.
- libxtc1.49.2 pinned revision above, configure `--with-io-backend=uring
  --with-tls=openssl --enable-shared`, CFLAGS `-O1 -g3 -fno-omit-frame-pointer`.
- Four executor loops, multithreaded=on, pooled_protocol_carriers=0,
  io_method=sync, summarize_wal=off, nofile131072, PG_XTC_TAIL=1; default
  shared_buffers/fsync/autovacuum. This is correctness diagnosis, not the
  85%-RAM performance acceptance setup. Client driver on SUT, not a benchmark.
- Test first-group schedule: test_setup; then boolean char name varchar text
  int2 int4 int8 oid float4 float8 bit numeric txid uuid enum money rangetypes
  pg_lsn regproc concurrently. Full runs use in-tree parallel_schedule.
- Captures at20s for first group,60s for later full runs,90s for fiber2; further
  GDB breakpoints after that. No absence inference from late captures.
- Local archive: `/tmp/xtc-stabilize-20260922-165131/evidence-final.tar.gz`
  SHA256 `b60f885a3e44802a4ce4d19c6d616d78d39dfbdc3f720d4e794b6e323cc060ed`.
  Expanded under `.../final/results/`; full5/wait-gdb.txt is the key record.
- EC2 instance terminated after checksum-verified collection; dedicated SG,
  AWS key pair and local PEM removed. Report written locally, not sent.
