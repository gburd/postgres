# Option A: pooled sessions as libxtc fibers (the write-wedge fix)

Decision (2026-08-28, endorsed by the libxtc team): the pooled write-load WEDGE
(.ec2/wedge-rootcause-2026-08-28.md) is fixed ONLY by running pooled client sessions as
real libxtc fibers on the carrier's exec loop -- NOT by offloading fsync from the bare
carrier pthread.  libxtc v1.40 added xtc_blocking_run_off_loop (the option-B primitive we
asked for), but the team confirmed it alone does NOT un-wedge (a bare pthread still can't
be parked/resumed, and offloading fsync doesn't shorten the WALWriteLock hold).  So we
adopt option A.

## Why A (from the libxtc reply)
A fiber that hits a blocking point (socket read, WAL fsync via xtc_aio_fdatasync, lock
wait) PARKS and the loop immediately runs the OTHER ready session fibers on that carrier.
That breaks the "one blocking syscall on the shared carrier pthread freezes the whole
cohort" coupling -- by construction, and uniformly for reads/fsync/lock-waits.

## Current shape (what changes)
- Today: the pooled carrier is a bare pthread (pg_thread_create, launch_backend.c:1372)
  running a for(;;) lease loop; each session runs INLINE via
  PgSessionRunProtocolSchedulerUntilBoundary to a STACKLESS PG_STEP_PARK_PROTOCOL_READ
  boundary, then returns and the carrier leases the next session.  xtc_in_backend_fiber is
  FALSE on the carrier pthread -> pg_fdatasync takes the raw blocking path.  A blocking
  fsync freezes the carrier + all its leased siblings + holds WALWriteLock.
- Target: each session is a long-lived libxtc FIBER on the carrier's exec loop (spawn on
  connect, park IN PLACE on socket read via xtc_aio/xtc_pg_wait_fd AND on fsync via
  xtc_aio_fdatasync, exit on disconnect).  xtc_self()!=none on the session fiber ->
  xtc_in_backend_fiber TRUE -> pg_fdatasync parks (already implemented in fd.c).  The
  stackless protocol-read-park mechanism is replaced by (or coexists with) a stack-holding
  fiber park.

## Plan (phased, two-review-gated, process mode byte-for-byte, A/B neutral-or-better on read-S/CPU)

### STAGED (2026-08-31, dormant behind `pooled_protocol_fiber_sessions`, default off)
The default-flip machinery is now wired and byte-for-byte dormant so it is READY to
activate the moment the libxtc cross-loop task->state resume race is fixed:
- New GUC `pooled_protocol_fiber_sessions` (bool, PGC_POSTMASTER, default off).  When on,
  the `pooled_protocol_carriers = -1` (auto) resolution in postmaster.c resolves to 0
  (fiber-per-session) instead of one stackless carrier per core.  carriers=0 already routes
  every B_BACKEND through postmaster_backend_thread_launch -> xtc_pg_launch_backend_fiber
  (a fiber on the carrier-loop pool that parks in place on WAL fsync / LWLock / buffer pin),
  which is exactly the Option-A model.  An explicit carriers value overrides and ignores
  the knob.  Default off => today's auto (stackless carrier-per-core) is unchanged.
- TO FLIP when libxtc lands the task->state fix: set boot_val => 'true' on the GUC (one
  line), re-run P-A4 validation (no wedge at 64/192/256 + full matrix), two-review gate.

### Remaining P-A steps (blocked on the libxtc task->state cross-loop fix)
P-A1. Prototype: spawn ONE session as a fiber on the exec loop (xtc_proc_spawn on
      g_xtc_loop / xtc_exec_loop), run PostgresMain-equivalent to the read boundary,
      park via xtc_pg_wait_fd holding its stack, resume on readable.  Prove
      xtc_in_backend_fiber=true so pg_fdatasync -> xtc_aio_fdatasync parks.  This is the
      thread-per-session path (pooled_protocol_carriers=0) done RIGHT -- study why that
      path PANICs at c>=192 ("could not lease protocol read park for same carrier
      resume") and fix that as part of this, since it is the same fiber-per-session shape.
P-A2. Stack sizing: size the fiber stack from PG's deepest planner/executor recursion
      (guard-page stacks -> clean SIGSEGV on overflow).  Measure RSS at 100s-1000s of
      parked session fibers; use xtc_stack_reclaim for idle sessions if RSS is tight
      (libxtc team flagged this as the scaling axis).
P-A3. WAL fsync on the fiber: the group-commit flusher fiber does xtc_aio_fdatasync
      (parks -> carrier runs siblings); other committers park on WALWriteLock as fibers
      and take PG's existing "someone flushed past my LSN" fast path on wake.  KEEP PG's
      group commit; add NO coalescing (libxtc team: coalescing is a WAL-LSN concern, stays
      in XLogFlush).  Ensure NO inline fdatasync branch is reachable from a fiber.
P-A4. Validate on the write-wedge repro (64-client TPC-B, fsync on): no wedge; gdb shows a
      fiber (not a bare pthread) parked in xtc_aio_fdatasync while the carrier runs
      siblings.  Then the full apples-to-apples matrix (mtpg_matrix.sh): TPROC-C + -S +
      CPU + TPROC-H, fork vs xtc, 85% RAM, carriers=auto, separate loadgen.  Bar: outright
      win.
P-A5. If a session fiber's xtc_aio_fdatasync does NOT park (carrier still freezes with a
      FIBER stuck inline in fdatasync), that is a libxtc issue -> send them the gdb.

## Scope note
This is a large, architectural change (the carrier scheduler + protocol-read-park +
thread-per-session PANIC fix converge here).  It is the B2 write-path item and THE gate to
the write-heavy win.  The libxtc team offered to sketch the fiber-per-session conversion
against our lease-loop + protocol-read-park code -- take them up on it before P-A1.
