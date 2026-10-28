# libxtc v2 consumer migration: source audit

## Scope and evidence

Initially audited against PostgreSQL
`65cf619a1f4c9b5fcb77100be2b65235510405cd`; integrated after the rebase
onto origin/master at PostgreSQL
`17d9c54f44c8ab6ac1d16a6f9110d62da65b6591`.
libxtc target: tagged v2.0.0,
`95041727ab08dca6b1474aabca38e2738c66e6ed`. GitHub `ls-remote`
confirmed the peeled tag. Source was inspected in a separate clean clone,
not the existing libxtc working directories. Library references below are
at that exact revision.

This is source inspection and a consumer migration, with **NO build or
runtime validation**. It does not establish that the PostgreSQL write
wedge is fixed. EC2 validation remains blocked on AWS hotdog credentials.

The September 22 replies about C-runtime supervision and duplicate fd
registration were read against the current source. Their short commit IDs
are absent from this clone's history. GitHub now peels v1.49.2 to
`40f27dec0efdfe8fe5fc1659ac2bb50e1ccd38e0`, not PostgreSQL's old pin
`542a67d9ea37a425a2ab7d6b11dff8993bd42871`. The findings below do not
claim ancestry from the reply commits.

## Consumer changes

- Public caller-allocated structs start with `size_t size`; zero is
  invalid. `src/inc/xtc_int.h:173-215` implements sized input/output;
  `src/ptc/proc.c:1750-1754,4746-4770` checks spawn options and DOWN
  output, and `src/evt/exec.c:1271-1285` checks loop-stat output.
  PostgreSQL initializes three spawn options, one DOWN output and two
  loop-stat outputs in `pg_xtc_carrier.c`. TLS options and the custom
  transport in `be-secure-openssl.c` use their named initializers too
  (`src/io/tls_common.h:151-167`, `src/io/tls_openssl.c:978-989`).
- `xtc_pid_t` is `{ uint32_t loop_id, uint32_t local_id, uint64_t gen; }`
  (`src/inc/xtc_proc.h:33-46`). PostgreSQL's loop/local fields already
  have the right width. `Latch.owner_fiber_gen` and `PGPROC.sem_fiber_gen`
  become uint64; their assignment/read callers retain all bits. Nine
  diagnostic formats now use UINT64_FORMAT and an explicit PG uint64
  cast. The two 96-byte diagnostic buffers grow to 128 bytes to fit the
  wider identity.
- The existing carrier test now passes XTC_PROC_OPTS_INIT for one
  monitored spawn and uses XTC_DOWN_INFO_INIT with xtc_down_decode_ex
  for the same zero-reason assertions. Its other spawns retain NULL
  options, which still mean defaults. Two compile-time assertions cover
  the stored generation widths; neither these assertions nor the test
  have been compiled or run. Existing app/log options already use default
  macros. PostgreSQL's AIO callers use the convenience functions, not
  xtc_aio_t.
- Meson requires libxtc >=2.0.0 when enabling the carrier. For both build
  systems, pg_xtc_carrier.c checks for XTC_PROC_OPTS_INIT, the sized
  public-struct API introduced in v2. The installed headers omit generated
  xtc_config.h, so XTC_VERSION_MAJOR defaults to zero even for installed
  v2; a version-macro guard would wrongly reject the flake package.
  No configure.ac integration was added. The no-libxtc build remains
  optional; source changes stay under USE_XTC_CARRIER. The
  process-backend selection and wait/error policies are unchanged.

## What the library source now contains

- The duplicate-registration repair is present: `src/io/io_uring.c:1247`
  removes the matching queued delete under its lock; `:1357` applies it
  before checking for a duplicate. `src/ptc/proc.c:3483-3527` propagates
  registration errors. `test/m2/test_io_register.c` still contains
  `R4c_reg_with_pending_del`. This test was inspected, not run.
- Finished proc fibers are marked reclaimable in `src/evt/coro_fctx.c:405`
  (also ucontext/Windows), reclaimed from the scheduler in
  `src/evt/loop.c:1090-1092`, and released/recycled by
  `src/evt/task.c:286-347`. Bare xtc_async task handles remain live until
  loop teardown. The cache bound matters for PG: up to 64 stacks per
  thread plus 64 spare stacks per loop (`src/inc/coro_common.h:108-240`,
  `src/inc/loop_int.h:334`). At 8 MiB each, either full cache reserves
  512 MiB plus guards; this is not a measurement of resident memory.
  Churn must measure mappings and RSS, including migration.
- `alive` and proc-table `n_used` are atomic (`src/ptc/proc.c:587,690`),
  as is `park_timer` (`src/inc/loop_int.h:97`). These are source changes,
  not evidence of a race-free PG workload.
- DOWN delivery now bypasses mailbox capacity using `__send_sys`
  (`src/ptc/proc.c:2541,5133-5148`). Allocation failure or a gone watcher
  can still prevent delivery and emits LIFECYCLE_DROP. Monitored-registry
  enrollment failure rolls back and returns failure
  (`src/orc/reg.c:468-501`). Neither is an unconditional delivery guarantee.
- Supervisor group restarts wait for old children to leave their owner
  tables, with a five-second observation budget (`src/orc/sup.c:276-338`).
  DELIVERED still does not mean exit hooks completed or storage was
  reclaimed (`src/inc/xtc_proc.h:278-313`); no new general cleanup-complete
  status primitive was found. PG's custom supervisor is not replaced.
- xproc has typed death handling and TERM/KILL reap attempts, but the
  second timed wait's result is ignored (`src/orc/xproc.c:395-440,639-649`).
  Safe re-exec requires the child-entry hook; otherwise entry spawning
  retains the fork path (`:520-544`). This migration does not adopt xproc
  or change PG's shared-address-space fail-stop policy.

## Packaging and remaining work

The flake still exports `packages.${system}.xtc`; dist/configure is present,
xtc.pc still supplies `-lxtc`, and the shared SONAME becomes libxtc.so.2.
The library flake disables its check phase. Updating the lock fetched
metadata only: libxtc changed, uringscope was added as its input, and the
existing root nixpkgs/flake-utils pins stayed unchanged. The locked libxtc
NAR hash is `sha256-RAZAt2b7n717SLjHxZgPbr33N5DYqmbdxD5IVl1nv6w=`.

Static checks cover whitespace, all PG C/header libxtc struct declarations
(including tests), generation storage/formats, and lockfile scope. They do
not substitute for compilation. Rebuild PG and all modules from clean
outputs: Latch/PGPROC layout and the library ABI changed. Independent
source review is complete; EC2 validation must cover process mode,
threaded/pooled mode, TLS custom transport, DOWN/crash handling, carrier
stats, and fixed-concurrency connection churn.

The pre-existing `xtc_pg_wait_fd` hard-error-as-latch-wake mapping remains
unchanged by request (`pg_xtc_carrier.c`, rc handling after restore). It
can still turn registration failures into retry loops. Treat that as a
separate follow-up, not as fixed by this pin.
