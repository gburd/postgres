# Multithreaded Agent Reference

This file holds source-orientation, local build/test, platform, and terminology
notes split out of `AGENTS.md`. Use it when working in a specific subsystem or
when local build/test friction repeats.

## Source Orientation

Important current files:

- `src/backend/tcop/postgres.c`: `PostgresMain()`, the top-level backend loop,
  error recovery, command read, command dispatch, and `ProcessInterrupts()`.
- `src/backend/tcop/backend_runtime_tcop.c`: fork-owned runtime bridge
  accessors for top-level command-loop state, including session `tcop` state,
  command-read state, current debug-query-string state, and query-error
  Valgrind state. Add future `tcop/postgres.c` compatibility shims here rather
  than growing `backend_runtime.c`.
- `src/include/access/session.h` and `src/backend/access/common/session.c`:
  existing `Session` abstraction for session-scoped DSM/DSA state. Treat this
  as a seed for the broader session object unless there is a strong reason not
  to.
- `src/backend/utils/cache/backend_runtime_cache.c`: fork-owned runtime bridge
  accessors for session- and execution-owned catalog/cache roots, including
  the fallback-aware execution catalog, catcache, relmap, and invalidation
  selectors. Add future catalog/cache accessor shims here rather than growing
  `backend_runtime.c`.
- `src/backend/utils/init/backend_runtime_session.c`: fork-owned runtime
  bridge accessors for broad session-owned compatibility state that does not
  yet have a narrower owner file, including datetime/timezone, namespace,
  locale, database, tablespace, binary-upgrade, text-search, extension,
  invalidation, relmap, prepared-statement, on-commit, and sequence shims. Keep
  fallback-aware current-bucket selectors here only for the broad session
  bridge or in narrower owner-adjacent files, and expose shared
  current-or-early helpers only through `backend_runtime_internal.h`.
- `src/backend/utils/init/backend_runtime_teardown.c`: fork-owned closed-state
  reset/teardown owner for `PgBackend`, `PgSession`, and `PgExecution`.
  Keep root object construction, current-pointer installation, and early
  fallback adoption in `backend_runtime.c`; move semantic closed-reset work
  here or to a narrower owner-adjacent runtime file.
- `src/backend/utils/activity/backend_runtime_pgstat.c`: fork-owned runtime
  bridge accessors for pgstat-owned backend/session state. Add future pgstat
  accessor shims here rather than growing `backend_runtime.c`.
- `src/backend/utils/error/backend_runtime_error.c`: fork-owned runtime bridge
  accessors for error-reporting, logging, and elog-owned compatibility state,
  including the fallback-aware execution error selector.
- `src/backend/utils/adt/backend_runtime_ri.c`: fork-owned runtime bridge
  accessors for RI trigger session globals and execution cleanup state. Add
  future RI trigger compatibility shims here rather than growing
  `backend_runtime.c` or `backend_runtime_session.c`.
- `src/backend/regex/backend_runtime_regex.c`: owner-adjacent runtime bridge
  for session and execution regex compatibility state. Add future regex
  compatibility shims here rather than growing `backend_runtime.c`.
- `src/backend/utils/adt/backend_runtime_pseudorandom.c`: fork-owned runtime
  bridge accessors for SQL random-function session state. Add future
  pseudorandom-function compatibility shims here rather than growing
  `backend_runtime.c`.
- `src/backend/utils/fmgr/backend_runtime_extension.c`: fork-owned runtime
  bridge accessors for extension and dynamic-library module state, including
  the fallback-aware execution extension selector. Keep root runtime/session
  selection and extension-module lifecycle orchestration in `backend_runtime.c`.
- `src/backend/optimizer/util/backend_runtime_optimizer.c`: fork-owned
  runtime bridge accessors for optimizer session state. Add future optimizer
  compatibility shims here rather than growing `backend_runtime.c`.
- `src/backend/utils/misc/backend_runtime_guc.c`: fork-owned runtime bridge
  accessors for GUC compatibility state that lives in session/backend/runtime
  buckets, including server/runtime GUCs, connection GUCs, core GUC registry
  pointers/lists, miscellaneous GUCs, threaded GUC mutex depth, and GUC
  error-reporting state, including the fallback-aware execution GUC-error
  selector. Add future GUC backing-variable shims here rather than growing
  `backend_runtime.c` or `guc_tables.c`.
- `src/backend/utils/misc/backend_runtime_utility.c`: fork-owned runtime
  bridge accessors for backend-local utility, formatting, sampling, superuser,
  and resource-owner callback state. Add small utility compatibility shims here
  rather than growing `backend_runtime.c`.
- `src/backend/commands/backend_runtime_vacuum.c`: fork-owned runtime bridge
  accessors for vacuum/analyze session and execution state, including the
  fallback-aware lazy session vacuum and execution vacuum/analyze selectors.
  Add future vacuum, analyze, and parallel-vacuum compatibility shims here
  rather than growing `backend_runtime.c`.
- `src/backend/commands/backend_runtime_async.c`: fork-owned runtime bridge
  accessors for LISTEN/NOTIFY async session and execution state, including the
  fallback-aware execution async selector. Add future async compatibility
  shims here rather than growing `backend_runtime.c`.
- `src/backend/commands/backend_runtime_event_trigger.c`: fork-owned runtime
  bridge accessors for event-trigger execution state. Add future event-trigger
  compatibility shims here rather than growing `backend_runtime.c`.
- `src/backend/commands/backend_runtime_matview.c`: fork-owned runtime bridge
  accessors for materialized-view execution state, including the
  fallback-aware materialized-view execution selector. Add future
  materialized-view compatibility shims here rather than growing
  `backend_runtime.c`.
- `src/backend/commands/backend_runtime_trigger.c`: fork-owned runtime bridge
  accessors for trigger execution state, including the fallback-aware trigger
  execution selector. Add future trigger compatibility shims here rather than
  growing `backend_runtime.c`.
- `src/backend/executor/backend_runtime_executor.c`: fork-owned runtime bridge
  accessors for executor-owned SPI state, including the fallback-aware SPI
  execution bucket selector, plus executor instrumentation compatibility
  shims. Add future executor compatibility shims here rather than growing
  `backend_runtime.c`.
- `src/backend/replication/logical/backend_runtime_logical.c`: fork-owned
  runtime bridge accessors for logical-replication execution scratch and
  snapbuild export state, including the fallback-aware replication scratch and
  snapbuild execution selectors. Add future logical-replication compatibility
  shims here rather than growing `backend_runtime.c`.
- `src/backend/access/transam/backend_runtime_parallel.c`: fork-owned runtime
  bridge accessors for backend-local parallel-query state. Add parallel-query
  compatibility shims here rather than growing `backend_runtime.c`.
- `src/backend/access/transam/backend_runtime_xact.c`: fork-owned runtime
  bridge accessors for transaction and two-phase execution/session state,
  including the fallback-aware XLog-insert, transaction, transaction-cleanup,
  and two-phase execution bucket selectors. Add future transaction
  compatibility shims here rather than growing `backend_runtime.c`.
- `src/backend/jit/backend_runtime_jit.c`: fork-owned runtime bridge accessors
  for provider-independent and LLVM-provider JIT session state. Keep
  LLVM-provider-private semantic lifecycle work under `src/backend/jit/llvm`
  when that state needs provider-specific cleanup.
- `src/backend/libpq/backend_runtime_connection.c`: fork-owned runtime bridge
  accessors for frontend/backend connection state. Add backend libpq,
  protocol, startup, and client-connection compatibility accessors here rather
  than growing `backend_runtime.c`.
- `src/backend/utils/mmgr/backend_runtime_memory.c`: fork-owned runtime bridge
  accessors for memory-manager and execution memory-context state, including
  the fallback-aware execution memory-context selector. Add memory-context
  compatibility shims here rather than growing `backend_runtime.c`.
- `src/backend/utils/mmgr/backend_runtime_portal.c`: fork-owned runtime
  bridge accessors for portal manager and active-portal state, including the
  fallback-aware session portal-manager and execution portal selectors. Add
  portal compatibility shims here rather than growing `backend_runtime.c`.
- `src/backend/utils/resowner/backend_runtime_resowner.c`: fork-owned runtime
  bridge accessors for execution resource-owner state, including the
  fallback-aware resource-owner execution bucket selector. Add resource-owner
  compatibility shims here rather than growing `backend_runtime.c`.
- `src/backend/utils/time/backend_runtime_time.c`: fork-owned runtime bridge
  accessors for execution snapshot and combo-CID state, including the
  fallback-aware snapshot and combo-CID execution bucket selectors. Add
  snapshot and combo-CID compatibility shims here rather than growing
  `backend_runtime.c`.
- `src/backend/nodes/backend_runtime_nodes.c`: fork-owned runtime bridge
  accessors for node read/write execution state. Add node serializer/parser
  compatibility shims here rather than growing `backend_runtime.c`.
- `src/backend/parser/backend_runtime_parser.c`: fork-owned runtime bridge
  accessors for parser-owned session state, including the fallback-aware
  parser bucket selector. Add future parser compatibility shims here rather
  than growing `backend_runtime.c`.
- `src/backend/storage/buffer/backend_runtime_buffer.c`: fork-owned runtime
  bridge accessors for backend-local buffer state. Add buffer-manager
  compatibility accessors here rather than growing `backend_runtime.c`.
- `src/backend/storage/file/backend_runtime_file.c`: fork-owned runtime bridge
  accessors for backend-local fd/storage state. Add fd, smgr, and pending-fsync
  compatibility accessors here rather than growing `backend_runtime.c`.
- `src/backend/storage/lmgr/backend_runtime_lmgr.c`: fork-owned runtime bridge
  accessors for backend-local lock-manager state. Add lock, LWLock, predicate,
  and deadlock-detector compatibility accessors here rather than growing
  `backend_runtime.c`.
- `src/backend/storage/ipc/backend_runtime_ipc.c`: fork-owned runtime bridge
  accessors for backend-local IPC, sinval, DSM, and latch state. Add IPC
  compatibility accessors here rather than growing `backend_runtime.c`.
- `src/backend/postmaster/interrupt.c`: owner-adjacent runtime bridge for
  backend pending-interrupt and interrupt-holdoff compatibility accessors plus
  the logical backend interrupt mailbox helpers.
- `src/backend/utils/init/backend_runtime_internal.h`: backend-private runtime
  declarations shared by fork-owned runtime support files. Do not expose these
  helpers in installed headers unless an upstream-owned caller truly needs
  them.
- `src/test/modules/test_backend_runtime/test_backend_runtime.h`: shared
  declarations for the backend runtime test extension.
- `src/test/modules/test_backend_runtime/test_backend_runtime_backend.c`:
  backend pgstat-pending, activity, memory-manager, utility, and reset-state
  tests that have not yet earned a narrower owner file.
- `src/test/modules/test_backend_runtime/test_backend_runtime_backend_core.c`:
  core backend identity, command/log, expression-interpreter, and latch
  interrupt tests.
- `src/test/modules/test_backend_runtime/test_backend_runtime_backend_interrupt.c`:
  backend interrupt-holdoff, pending-interrupt, and exit-state tests.
- `src/test/modules/test_backend_runtime/test_backend_runtime_backend_subsystems.c`:
  backend parallel, instrumentation, buffer, storage, lock, IPC, wait,
  transaction, timeout, replication, recovery, maintenance-worker,
  autovacuum, repack, AIO, and extension-module tests split from the broad
  backend test family.
- `src/test/modules/test_backend_runtime/test_backend_runtime_pmchild.c`:
  PMChild thread-backend signal and publication-race tests.
- `src/test/modules/test_backend_runtime/test_backend_runtime_session.c`:
  core session, SQL loop, tcop, xact callback, database, tablespace, locale,
  and shared user-identity helper test functions.
- `src/test/modules/test_backend_runtime/test_backend_runtime_session_cache.c`:
  session catalog/cache, extension-module, prepared-statement, invalidation,
  RI, relmap, and session-reset tests split from the broader session test
  family.
- `src/test/modules/test_backend_runtime/test_backend_runtime_session_guc.c`:
  runtime/server GUC, generated GUC rebind, connection, parser, vacuum,
  buffer, xact-default, lock-wait, and logging GUC tests.
- `src/test/modules/test_backend_runtime/test_backend_runtime_session_guc_core.c`:
  session pgstat, query-id, storage, user, command, replication, general,
  compatibility, access/WAL, miscellaneous, and aggregate GUC-state tests split
  from the broader session GUC test family.
- `src/test/modules/test_backend_runtime/test_backend_runtime_session_guc_planner.c`:
  session sort, JIT, query-memory, planner-cost, and planner-method GUC-state
  tests split out of the broader session GUC test family.
- `src/test/modules/test_backend_runtime/test_backend_runtime_connection.c`:
  connection, socket, protocol, startup, and security test functions.
- `src/test/modules/test_backend_runtime/test_backend_runtime_execution.c`:
  execution, transaction, snapshot, resource-owner, and query-work test
  functions.
- `src/test/modules/test_backend_runtime/test_backend_runtime.c`: keep this as
  the small module entry point and native-thread/runtime smoke holder; add new
  object-family tests to the split files above.
- `src/include/miscadmin.h`: widely visible process/session globals and the
  interrupt macros.
- `src/backend/storage/ipc/procsignal.c`: process-signal-style backend
  communication.
- `src/backend/storage/ipc/latch.c` and `src/backend/storage/ipc/waiteventset.c`:
  wait/wakeup infrastructure.
- `PgBackendRecoveryState` in `src/include/utils/backend_runtime.h` now owns
  backend-local recovery/startup/standby state bridged from `startup.c`,
  `standby.c`, and `xlogrecovery.c`. Use
  `PG_BACKEND_STANDBY_INITIAL_WAIT_US` for the standby conflict-wait default
  so early fallback and initialized thread backends stay aligned.
- `PgBackendMaintenanceWorkerState` in `src/include/utils/backend_runtime.h`
  now owns backend-local state bridged from archiver, checkpointer, bgwriter,
  WAL summarizer, and data-checksum workers. Avoid object-like compatibility
  macros named `arch_files` or `operation`: those collide with struct members
  in `pgarch.c` and `datachecksum_state.c`.
- `PgBackendRepackState` in `src/include/utils/backend_runtime.h` now owns
  backend-local repack leader/worker state bridged from
  `src/backend/commands/repack.c`, `src/backend/commands/repack_worker.c`,
  and `src/include/commands/repack.h`. Keep `DecodingWorker` private to
  `repack.c`; the runtime header should only forward-declare its struct tag.
- `PgBackendAioState` in `src/include/utils/backend_runtime.h` now owns
  backend-local AIO state bridged from `src/include/storage/aio_internal.h`,
  `src/backend/storage/aio/aio.c`,
  `src/backend/storage/aio/method_worker.c`, and
  `src/backend/storage/aio/method_io_uring.c`. Keep `PgAioUringContext`
  private to `method_io_uring.c`; the runtime header should only
  forward-declare its struct tag.
- `src/backend/utils/mb/backend_runtime_mb.c`: owner-adjacent runtime bridge
  for session encoding conversion cache accessors and the fallback-aware
  current encoding bucket selector.
- `src/backend/postmaster/launch_backend.c` and
  `src/backend/postmaster/postmaster.c`: backend launch and supervision.
- `src/backend/postmaster/autovacuum.c`,
  `src/backend/postmaster/auxprocess.c`,
  `src/backend/postmaster/bgworker.c`, and the individual auxiliary worker
  files under `src/backend/postmaster/`: worker launch, supervision, and
  server-owned worker lifecycles.
  `PgBackendAutovacuumState` owns autovacuum launcher/worker backend-local
  state bridged from `autovacuum.c`; keep private `avl_dbase` and
  `WorkerInfoData` pointers typed through forward-declared struct tags.
- `src/backend/replication/walreceiver.c`,
  `src/backend/replication/logical/launcher.c`,
  `src/backend/replication/logical/worker.c`, and
  `src/backend/storage/aio/method_worker.c`: replication and AIO worker
  lifecycles that must eventually use the threaded worker runtime.
- `src/include/fmgr.h` and `src/backend/utils/fmgr/dfmgr.c`: extension module
  ABI checks.
- `src/pl/plpgsql`: PL/pgSQL implementation.

## Local Build And Test Notes

- This checkout is commonly built with GNU make on macOS. Use `gmake`, not the
  BSD `make`. In the Codex desktop shell, Homebrew's bin directory may be
  absent from `PATH`; if `gmake` is not found, use `/opt/homebrew/bin/gmake` or
  export `PATH="/opt/homebrew/bin:$PATH"` before building.
- Do not use parallel `gmake -B ...` as a shortcut to force touched-object
  compiles in this checkout. It can trigger concurrent `config.status
  --recheck` runs, which race through temporary `conftest` files and produce
  misleading configure failures. To force coverage for edited sources, remove
  the specific `.o` files and rebuild the ordinary targets serially, or use the
  documented clean rebuild path below when installed headers or runtime object
  layouts changed.
- After cleaning under `src/backend`, generated backend-side files can be
  missing while include-side `header-stamp` files and symlinks still exist. If
  the build fails with a missing header such as `utils/errcodes.h`, regenerate
  the backend-side utility outputs explicitly:

  ```sh
  gmake -C src/backend/utils fmgr-stamp errcodes.h probes.h guc_tables.inc.c pgstat_wait_event.c wait_event_funcs_data.c wait_event_types.h
  ```

  If include-side symlinks are also missing or stale, remove the stamp and
  rebuild the symlinks:

  ```sh
  rm -f src/include/utils/header-stamp
  gmake -C src/backend/utils generated-header-symlinks
  ```

  If the missing generated header is `nodes/nodetags.h`, use the equivalent
  node-header recovery:

  ```sh
  rm -f src/backend/nodes/node-support-stamp
  gmake -C src/backend/nodes node-support-stamp
  rm -f src/include/nodes/header-stamp
  gmake -C src/backend/nodes generated-header-symlinks
  ```

- After changing exported backend globals or their `PG_THREAD_LOCAL`
  declarations in installed headers, clean and rebuild any in-tree extension
  under test before trusting its regression result. At minimum, do this for
  PL/pgSQL when touching GUC backing variables used by PL/pgSQL:

  ```sh
  gmake -C src/pl/plpgsql/src clean
  gmake -C src/pl/plpgsql/src all
  gmake -C src/pl/plpgsql/src DESTDIR="$PWD/tmp_install" install
  ```

  A stale PL/pgSQL build after SPI exported-state changes can fail during
  `initdb` post-bootstrap initialization with `Symbol not found:
  _SPI_processed` while loading `plpgsql.dylib`. Treat that as a stale module
  build, not as a SQL regression: clean, rebuild, and reinstall PL/pgSQL into
  the current `tmp_install`.

  `pg_global_prng_state` is also exported through an installed common header
  and is referenced by some contrib/test modules, including `amcheck`,
  `auto_explain`, `tablefunc`, and several `src/test/modules` tests. Clean and
  reinstall any of those modules before testing them after PRNG TLS changes.

  After changing `src/include/utils/backend_runtime.h`, clean and relink
  `src/test/modules/test_backend_runtime` before running its regression check.
  The extension allocates runtime structs by value, so a stale
  `test_backend_runtime.dylib` can crash during early tests even when the
  backend itself rebuilt successfully:

  ```sh
  gmake -C src/test/modules/test_backend_runtime clean
  gmake -C src/test/modules/test_backend_runtime all
  gmake -C src/test/modules/test_backend_runtime check
  ```

  Pending interrupt globals such as `InterruptPending` can be referenced from
  server-side common objects and loadable modules. After converting one of
  these exported names to an object-backed compatibility macro, rebuild
  `src/common`, PL/pgSQL, `src/test/regress`, and `libpqwalreceiver` before
  trusting `initdb` or core regression results:

  ```sh
  gmake -C src/common clean all
  gmake -C src/pl/plpgsql/src clean all DESTDIR="$PWD/tmp_install" install
  gmake -C src/test/regress clean all
  gmake -C src/backend/replication/libpqwalreceiver clean all DESTDIR="$PWD/tmp_install" install
  ```

  Memory-context globals exported through `palloc.h` or `memutils.h` are also
  referenced by backend loadable modules needed during `initdb`
  post-bootstrap initialization. After converting one of these names to an
  object-backed compatibility macro, rebuild and reinstall `src/backend/snowball`
  before trusting temp-instance tests. A stale `dict_snowball.dylib` fails
  `initdb` with `Symbol not found: _CurrentMemoryContext`.

  ```sh
  gmake -C src/backend/snowball clean all DESTDIR="$PWD/tmp_install" install
  ```

  Direct-pointer GUC globals exported through `miscadmin.h` can be referenced
  by backend loadable modules too. After converting one of these names to an
  object-backed compatibility macro, force-clean and reinstall
  `libpqwalreceiver` before trusting subscription tests or the core
  `parallel_schedule`; a stale `libpqwalreceiver.dylib` failed to load with
  `Symbol not found: _work_mem` after the query-memory GUC migration.

  ```sh
  gmake -C src/backend/replication/libpqwalreceiver clean all
  gmake -C src/backend/replication/libpqwalreceiver DESTDIR="$PWD/tmp_install" install
  ```

  Logical-decoding output plugins can also keep stale references to moved
  backend globals. After moving memory-context or replication GUC globals,
  clean and reinstall `pgoutput` and `pgrepack` before trusting
  `contrib/test_decoding`; stale copies have failed with
  `Symbol not found: _CurrentMemoryContext`.

  ```sh
  gmake -C src/backend/replication/pgoutput clean all DESTDIR="$PWD/tmp_install" install
  gmake -C src/backend/replication/pgrepack clean all DESTDIR="$PWD/tmp_install" install
  ```

  Core backend globals such as `MyProcPid` can also be referenced from
  server-side port objects. If a clean backend link fails with a removed
  backend-global symbol from `libpgport_srv.a`, clean and rebuild `src/port`
  as well:

  ```sh
  gmake -C src/port clean all
  ```

  Server-side common objects can have the same stale-symbol problem. If a
  clean backend link fails with a removed backend-global symbol from
  `libpgcommon_srv.a`, clean and rebuild `src/common` as well:

  ```sh
  gmake -C src/common clean all
  ```

- After changing a contrib/test module header to expose `PG_THREAD_LOCAL`
  declarations, clean and rebuild every object in that module before running a
  threaded smoke. Stale objects can still see the old plain-global symbol while
  freshly compiled objects use TLS, producing crashes that look like missing
  initialization. This was observed in `pg_stash_advice` after changing its
  DSM attachment pointers in `pg_stash_advice.h`.

- If an installed header changes a global from plain storage to
  `PG_THREAD_LOCAL`, do not trust a purely incremental backend build. Stale
  backend objects can still compile and link but then crash during `initdb`
  post-bootstrap single-user startup. Use the backend clean plus generated-file
  recovery above, then rebuild with `gmake -j8`.

- If a shared-memory struct layout changes, especially `Latch`, `PGPROC`, or
  fields embedded immediately beside semaphores/latches, do not trust a purely
  incremental backend build. Stale objects can corrupt adjacent shared-memory
  fields; one observed failure after changing `Latch` was a bootstrap segfault
  in `PGSemaphoreReset()` because stale `proc.o` still used the old
  `PGPROC.procLatch` size. Use the backend clean plus generated-file recovery
  above, then rebuild with `gmake -j8`.

- If `src/include/utils/backend_runtime.h` changes the layout of embedded
  runtime structs such as `PgThreadBackendRuntimeState`, do not trust a purely
  incremental backend build. Stale objects can keep old field offsets while
  freshly compiled runtime code zeros or writes the new, larger struct. One
  observed failure after adding connection socket I/O state was threaded
  startup corrupting adjacent `BackendThreadStart` timezone fields and
  segfaulting in `StartupXLOG()` before readiness. Use the backend clean plus
  generated-file recovery above, then rebuild with `gmake -j8`.

  Another observed failure after expanding `PgThreadBackendRuntimeState` was
  `001_threaded_runtime.pl` failing to start a thread-model background worker
  with `could not access file "": No such file or directory`; stale
  `src/backend/postmaster/launch_backend.o` had allocated the old embedded
  runtime-state size, so initialization overwrote the background-worker
  startup data. At minimum remove and rebuild
  `src/backend/postmaster/launch_backend.o`, then rerun `gmake -j8`. Header
  changes that remove execution globals may also need stale users removed and
  rebuilt, including `src/backend/access/transam/xact.o`,
  `src/backend/access/transam/twophase.o`,
  `src/backend/access/transam/xloginsert.o`,
  `src/backend/replication/logical/applyparallelworker.o`, and
  `src/backend/replication/logical/tablesync.o`.

  Another observed failure after adding a field to `PgCarrier` was
  `001_threaded_runtime.pl` failing at `pg_ctl start`, with lldb showing
  `PgBackendGetSignalPid()` dereferencing address `0x1`. Stale
  `src/backend/postmaster/launch_backend.o` had passed a `PgBackend *` offset
  from the old embedded `PgThreadBackendRuntimeState` layout into
  `PostmasterChildSetThreadBackend()`. Force-rebuild that object before
  reinstalling and rerunning threaded TAP.

  The same rule applies when adding fields to `PgBackend` itself. A stale
  incremental build after adding AIO runtime state linked, but `initdb`
  post-bootstrap startup spun during shutdown cleanup in
  `dsm_backend_shutdown()`/`dsm_detach()` because stale backend objects still
  used the old `PgBackend` layout. Treat that as stale-object fallout: kill the
  stuck temp bootstrap, run the backend clean plus generated-file recovery,
  rebuild with `gmake -j8`, and reinstall before rerunning temp-instance tests.

  The same rule applies when adding fields to embedded `PgSession` state. One
  observed stale-object symptom after expanding `PgSessionCatalogLookupState`
  was threaded `CREATE EXTENSION test_backend_runtime_threaded` crashing in
  `list_member_ptr()` from `dfmgr.c` while recording `_PG_init()` session
  state. Rebuild at least `src/backend/utils/fmgr/dfmgr.o` and reinstall the
  temp tree; prefer a full backend clean plus generated-file recovery when the
  layout change is broad.

- When moving WAL/XLog state out of `src/backend/access/transam/xlog.c`, avoid
  object-like compatibility macros that reuse names of shared WAL struct
  fields. In particular, do not define a `RedoRecPtr` macro: it also expands
  inside `Insert->RedoRecPtr` and `XLogCtl->RedoRecPtr`. Use the existing
  `XLogLocalRedoRecPtr` compatibility name for the backend-local redo-pointer
  cache and leave shared struct member references untouched.

- If `src/include/replication/worker_internal.h` changes the layout of
  `LogicalRepWorker`, clean and rebuild the whole logical replication backend
  directory before running logical replication smokes. Incremental builds in
  this checkout have left objects such as `syncutils.o`, `tablesync.o`, and
  `applyparallelworker.o` built against the previous struct layout:

  ```sh
  gmake -C src/backend/replication/logical clean
  gmake -j8
  gmake -j8 install DESTDIR="$PWD/tmp_install"
  ```

  A stale `syncutils.o` can read `LogicalRepWorker.userid` from the old offset
  and make table-sync workers fail during startup with errors like
  `role with OID 119 does not exist`, where `119` is the ASCII value of a
  subscription relation-state byte.

- If `PMChild` layout changes in `src/include/postmaster/postmaster.h`, do not
  trust an incremental build of postmaster objects. Stale postmaster objects can
  corrupt the PMChild freelists or crash auxiliary children during temp-instance
  startup. Use:

  ```sh
  gmake -C src/backend/postmaster clean
  gmake -C src/backend -j8
  ```

- If a shared enum in an installed or widely included header changes numeric
  values, do not trust a purely incremental backend build. For example,
  inserting a new `PMSignalReason` before existing values can leave stale
  objects such as `checkpointer.o` signaling one numeric reason while
  `postmaster.o` interprets another, causing shutdown hangs. Prefer appending
  new signal reasons to preserve existing values, and force rebuild affected
  objects or use a clean backend rebuild before testing.

- This checkout is currently configured with `with_gssapi = no`. A direct
  `gmake -C src/backend/libpq be-secure-gssapi.o` can fail before reaching
  project changes because the GSSAPI types and functions are unavailable in
  this configuration. For GSSAPI-only source annotations, use static lifetime
  scan coverage plus a full non-GSS build here, and use a GSSAPI-enabled build
  when compile coverage for that file is required.

- This checkout is currently configured with `with_ssl = no`. A direct
  `gmake -C src/backend/libpq be-secure-openssl.o` can fail before reaching
  project changes because OpenSSL support macros are not enabled in
  `pg_config.h`. For OpenSSL-only source annotations, use static lifetime scan
  coverage plus a full non-SSL build here, and use an SSL-enabled build when
  compile coverage for that file is required.

  `contrib/pgcrypto` is different: this makefile still builds OpenSSL-backed
  object files even when the core tree is configured `with_ssl = no`. On this
  macOS checkout, use Homebrew OpenSSL explicitly for focused pgcrypto builds
  and checks:

  ```sh
  PG_CPPFLAGS="-I/opt/homebrew/opt/openssl@3/include" \
  PG_LDFLAGS="-L/opt/homebrew/opt/openssl@3/lib" \
  SHLIB_LINK="-L/opt/homebrew/opt/openssl@3/lib -lcrypto" \
  gmake -C contrib/pgcrypto clean all check
  ```

- This macOS checkout is not a normal compile target for `contrib/sepgsql`.
  PostgreSQL builds `sepgsql` only with SELinux support, Meson disables the
  SELinux dependency automatically when the host system is not Linux, and this
  machine does not currently have Homebrew `libselinux` headers. A direct
  `gmake -C contrib/sepgsql label.o uavc.o` can fail before reaching project
  changes with `fatal error: 'selinux/label.h' file not found`. For sepgsql
  Phase 12 state migrations here, use static lifetime scan coverage,
  manifest/lifecycle checks, source review against the owning files, and a
  Linux SELinux-enabled build for compile/runtime coverage.

- This checkout is currently configured with `with_python = no`. Direct
  PL/Python builds and regression tests under `src/pl/plpython` are not
  available in this configuration. For PL/Python-only Phase 12 migrations,
  use runtime lifecycle checks, global lifetime scans, source review, and the
  full non-Python build here, then use a Python-enabled build before claiming
  PL/Python runtime coverage for Gate E2. If local Python headers are
  installed through Homebrew, object-level compile coverage is still possible
  with:

  ```sh
  PYINCLUDES="$(python3-config --includes)"
  gmake -C src/pl/plpython plpy_procedure.o plpy_spi.o plpy_cursorobject.o plpy_main.o CPPFLAGS="$PYINCLUDES"
  ```

- This checkout is currently configured with `with_tcl = no`. A direct
  `gmake -C src/pl/tcl pltcl.o` can still compile the PL/Tcl source on this
  machine, but top-level install omits the `pltcl` extension and
  `gmake -C src/pl/tcl check` fails at SQL startup with
  `extension "pltcl" is not available`. For PL/Tcl-only Phase 12 migrations,
  use runtime lifecycle checks, global lifetime scans, source review,
  stale-symbol scans, the `pltcl.o` object build, and the full non-Tcl build
  here, then use a Tcl-enabled build before claiming PL/Tcl runtime coverage
  for Gate E2.

- This checkout is currently configured and validated with `with_perl = yes`.
  Direct PL/Perl builds and regression tests under `src/pl/plperl` are
  available here and should be used for PL/Perl or bundled-language Phase 12
  work. After `gmake -C src/pl/plperl check`, the shared `tmp_install` can be
  recreated; reinstall any test module needed by later direct TAP commands
  before rerunning those TAP checks.

- This checkout is currently validated with LLVM and Perl enabled for Phase 12
  JIT provider and bundled-language work:

  ```sh
  ./configure --without-icu --disable-rpath --with-llvm --with-perl LLVM_CONFIG=/opt/homebrew/opt/llvm@21/bin/llvm-config
  ```

  After switching LLVM configuration or changing installed runtime/JIT
  headers, do not trust stale incremental objects. Use the backend clean plus
  generated-file recovery above before a full `gmake -j8`; otherwise
  `llvmjit.dylib` can fail to link or runtime JIT smoke can crash against an
  older `postgres` binary missing new runtime accessors such as
  `PgCurrentLLVMJitState`.

  LLVM 21 headers collide with PostgreSQL's short historical macros. Keep the
  macro cleanup in `src/include/jit/llvmjit.h` for LLVM C headers and in
  `src/backend/jit/llvm/llvmjit_inline.cpp` for later LLVM C++ headers. If
  a clean LLVM provider build fails inside LLVM headers with names such as
  `Mode`, `PM`, `AM`, or `TZ`, fix the boundary cleanup rather than patching
  generated LLVM headers.

- This checkout is currently configured without `--enable-injection-points`.
  `src/test/modules/injection_points` intentionally skips checks in that
  configuration, and injection-point TAP/regression coverage requires a build
  configured with injection points enabled. For injection-point-only source
  annotations in this checkout, use object compile coverage where reachable,
  static lifetime scan coverage, and a full non-injection build/install.

- This checkout does not define `LWLOCK_STATS` in normal builds. Changes inside
  the optional LWLock stats debug block in `src/backend/storage/lmgr/lwlock.c`
  are therefore covered here by normal surrounding-object builds, runtime
  accessor tests, and `gmake check-global-lifetimes`; direct compile coverage
  for that debug block requires an `LWLOCK_STATS`-enabled build.

- For manual temp-cluster smokes, especially threaded-mode smokes launched from
  this deep checkout path, use a short Unix socket directory under `/tmp` with
  `pg_ctl -o "-k /tmp/..."` or an explicit `unix_socket_directories` setting.
  Nested workspace paths can exceed the platform Unix socket path length before
  SQL starts.

- Some `gmake ... check` runs fail on macOS because temporary-install binaries
  still refer to `/usr/local/pgsql/lib/libpq.5.dylib`. The repository-level
  `temp-install` rule now rewrites `libpqwalreceiver.dylib` to load libpq from
  its own directory on Darwin. For hand-built/direct `tmp_install` runs, patch
  remaining frontend binaries before running direct `pg_regress` commands:

  ```sh
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" "$PWD/tmp_install/usr/local/pgsql/bin/initdb" || true
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" "$PWD/tmp_install/usr/local/pgsql/bin/psql"
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" "$PWD/tmp_install/usr/local/pgsql/bin/pg_ctl" || true
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" "$PWD/tmp_install/usr/local/pgsql/bin/pg_basebackup" || true
  ```

  Contrib extensions linked against libpq can have the same stale install name
  in their temp-installed `.dylib`. If `CREATE EXTENSION dblink` or
  `CREATE EXTENSION postgres_fdw` fails before SQL behavior with
  `Library not loaded: /usr/local/pgsql/lib/libpq.5.dylib`, patch the
  recreated extension library and rerun the direct driver:

  ```sh
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" "$PWD/tmp_install/usr/local/pgsql/lib/dblink.dylib" || true
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" "$PWD/tmp_install/usr/local/pgsql/lib/postgres_fdw.dylib" || true
  ```

  Do not run two `gmake ... check` targets that recreate the repository-level
  `tmp_install` in parallel. They race on `rm -rf tmp_install` and temp-install
  creation, producing misleading failures such as `Directory not empty` while
  deleting `tmp_install` or an install-log failure before SQL starts. Run those
  checks sequentially, or give one check an isolated temp-install root if the
  make target supports it.

  Tests that create subscriptions can reach `libpqwalreceiver.dylib` in normal
  recursive checks. If a direct custom temp install bypasses the repository
  `temp-install` rule, patch that module's libpq dependency before trusting
  subscription failures as threaded runtime signal.

  After rebuilding backend/postmaster code, reinstall before direct TAP runs
  that use `tmp_install`; otherwise an old `postgres` binary can run with new
  test modules or headers. One observed stale-install symptom was
  `001_threaded_runtime.pl` failing at
  `test_backend_runtime_launch_thread_bgworker()` with
  `thread-model background worker did not start: status 2` and server log
  `could not access file ""`. Reinstall with
  `gmake -j8 install DESTDIR="$PWD/tmp_install"` and reinstall
  `src/test/modules/test_backend_runtime`, then patch install names again
  before rerunning TAP.

  Direct isolation runs can fail the same way from build-tree binaries. Patch
  `src/test/isolation/isolationtester` and
  `src/test/isolation/pg_isolation_regress` to the same temp-install
  `libpq.5.dylib` before rerunning them. Direct TAP runs that pass
  `PG_REGRESS="$PWD/src/test/regress/pg_regress"` can fail during
  `pg_regress --config-auth` with signal 6 for the same reason after
  rebuilding `src/test/regress`; patch `src/test/regress/pg_regress` too:

  ```sh
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" src/test/regress/pg_regress
  ```

  `gmake -C src/test/regress check-tests` recreates `tmp_install`, so a
  previously patched `psql` can become unpatched again. If that target fails
  before SQL starts with a `dyld` `libpq.5.dylib` loader error, patch the new
  temp-install binaries and rerun the equivalent `pg_regress` command directly.

  Do not run two `gmake ... check` targets that create a temp install in
  parallel from the same checkout. They share `$PWD/tmp_install`, and parallel
  temp-install setup can fail before SQL starts with `rm: ... tmp_install:
  Directory not empty` or a missing `tmp_install/log/install.log`. Run those
  regression targets sequentially, or use a separate checkout/build directory
  for parallel test work.

  Top-level `gmake check-world` and recursive targets such as
  `gmake -C src/test check` also recreate `tmp_install` on this checkout. They
  can therefore fail before SQL starts even if a previous temp install was
  patched. If the failure is a `dyld` lookup for
  `/usr/local/pgsql/lib/libpq.5.dylib`, patch the recreated temp install and run
  the reached test driver directly. For example, after a `check-world` failure
  in `src/test/isolation`, patch `psql`, `pg_ctl`, `pg_isolation_regress`, and
  `isolationtester`, then rerun:

  ```sh
  cd src/test/isolation
  PATH="$PWD/../../../tmp_install/usr/local/pgsql/bin:$PWD:$PATH" \
  DYLD_LIBRARY_PATH="$PWD/../../../tmp_install/usr/local/pgsql/lib" \
  INITDB_TEMPLATE="$PWD/../../../tmp_install/initdb-template" \
  ./pg_isolation_regress --temp-instance=./tmp_check_iso --inputdir=. --outputdir=output_iso --bindir= --schedule=./isolation_schedule
  ```

  The core regression equivalent after patching is:

  ```sh
  cd src/test/regress
  PATH="$PWD/../../../tmp_install/usr/local/pgsql/bin:$PWD:$PATH" \
  DYLD_LIBRARY_PATH="$PWD/../../../tmp_install/usr/local/pgsql/lib" \
  INITDB_TEMPLATE="$PWD/../../../tmp_install/initdb-template" \
  ./pg_regress --temp-instance=./tmp_check --inputdir=. --bindir= --dlpath=. --schedule=./parallel_schedule
  ```

  The focused threaded core-regression smoke is available from the repository
  root:

  ```sh
  gmake check-threaded-smoke
  ```

  It uses `src/test/regress/threaded_smoke.conf` to start the pg_regress temp
  cluster with `multithreaded = on` and runs
  `src/test/regress/threaded_schedule`. The initial schedule is intentionally
  helper-free: it excludes `test_setup` and any SQL tests that depend on
  `src/test/regress/regress.dylib`, because a full threaded `gmake check
  TEMP_CONFIG=...` currently fails at setup with the expected backend-model
  mismatch for that process-only regression helper library. Grow
  `threaded_schedule` when a candidate test either has no helper/setup
  dependency or the dependency has been audited for thread-per-session mode.
  `pg_regress` prints the useful pass/fail count directly, e.g. `All 10 tests
  passed` for the first helper-free schedule.

  The full threaded core-regression visibility target is:

  ```sh
  gmake check-threaded
  ```

  It runs `src/test/regress/parallel_schedule` with the same threaded temp
  config. Current baseline: `gmake check-threaded` completes all 245 core
  regression tests in threaded mode with default pg_regress concurrency.
  Prepared transactions are enabled through pg_regress' normal temporary
  instance configuration and `prepared_xacts` is admitted to the threaded
  schedule. Regular dynamic parallel workers are admitted; `select_parallel`
  should match the normal expected output, not a leader-only threaded
  alternate. Anonymous temp-file names include the logical backend id in
  threaded mode so parallel worker threads with the same OS PID cannot truncate
  each other's private spill files. The remaining threaded expected-output
  shim is `guc_0.out`, where `plpgsql` has already reserved its GUC prefix in
  the shared runtime before the later `guc` test.

  The worker-settings threaded core-regression visibility target is:

  ```sh
  gmake check-threaded-workers
  ```

  It also runs the full `src/test/regress/parallel_schedule`, but uses
  `src/test/regress/threaded_workers.conf`: `multithreaded = on`,
  `io_method = worker`, and `summarize_wal = on`. Use it to expose AIO worker
  and WAL summarizer startup/teardown issues that the stable
  `check-threaded` baseline intentionally avoids with `io_method = sync` and
  `summarize_wal = off`.

  The broader Phase 12 world-core threaded validation target is:

  ```sh
  gmake check-threaded-world-core
  ```

  It composes the worker-settings core regression baseline, PL/pgSQL
  regression under `threaded_workers.conf`, full `src/test/isolation`
  regression under `threaded_workers.conf`,
  `src/test/modules/test_backend_runtime` process-mode regression plus direct
  `001_threaded_runtime.pl`, `002_threaded_bgworker_crash.pl`, and
  `003_milestone_w_core_smoke.pl` TAP runs, and the lifecycle/global
  guardrails. The direct TAP leg uses the repo-local `.perl5` module path so
  the target still exercises threaded TAP in checkouts configured without
  `--enable-tap-tests`. It deliberately does not recurse through all of
  `check-world`:
  contrib-wide threaded support, bundled procedural languages beyond PL/pgSQL,
  broad `src/bin`/interfaces/TAP coverage, and the full custom/extension GUC
  matrix remain Phase 16 / Gate E2-Extensions unless the focused target,
  threaded TAP log guard, retained-root warnings, lifecycle checker, or global
  lifetime scan exposes a core runtime dependency. Isolation is included after
  clearing the threaded safe-snapshot wait, parallel-deadlock timeout, and
  async notification routing blockers found during world-core discovery.

  The interim 150-pass threaded visibility target is:

  ```sh
  gmake check-threaded-150
  ```

  It runs `src/test/regress/threaded_150_schedule` with the same threaded temp
  config. The schedule is intentionally narrower than full `check-threaded`:
  it is retained as a quick historical visibility target, but the authoritative
  core-regression threaded baseline is now the full `check-threaded` target.

  The interim 200-pass threaded visibility target is:

  ```sh
  gmake check-threaded-200
  ```

  It runs `src/test/regress/threaded_200_schedule` with the same threaded temp
  config. The schedule is serialized: one test per schedule line. This keeps
  threaded backend startup/teardown and later SQL feature surfaces visible
  with a shorter run than the full target, but the authoritative
  core-regression threaded baseline is now the full `check-threaded` target.

  If the same recursive target needs to be rerun, patch the build-tree binaries
  that are copied into each recreated temp install before rerunning. This has
  allowed recursive checks such as
  `gmake -C src/test/modules/test_extensions check` to run normally after an
  initial temp-install `initdb` loader failure:

  ```sh
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" src/bin/initdb/initdb || true
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" src/bin/psql/psql || true
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" src/bin/pg_ctl/pg_ctl || true
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" src/bin/pg_basebackup/pg_basebackup || true
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" src/backend/replication/libpqwalreceiver/libpqwalreceiver.dylib || true
  ```

  `gmake check-world` also builds ECPG test executables that can record
  `/usr/local/pgsql/lib/libecpg.6.dylib`, `libpgtypes.3.dylib`, and
  `libecpg_compat.3.dylib` from build-tree library IDs. If all ECPG tests abort
  with signal 6 and stderr says `Library not loaded: /usr/local/pgsql/lib/...`,
  patch the build-tree dynamic-library IDs before rerunning:

  ```sh
  install_name_tool -id "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" src/interfaces/libpq/libpq.5.dylib
  install_name_tool -id "$PWD/tmp_install/usr/local/pgsql/lib/libecpg.6.dylib" src/interfaces/ecpg/ecpglib/libecpg.6.dylib
  install_name_tool -id "$PWD/tmp_install/usr/local/pgsql/lib/libpgtypes.3.dylib" src/interfaces/ecpg/pgtypeslib/libpgtypes.3.dylib
  install_name_tool -id "$PWD/tmp_install/usr/local/pgsql/lib/libecpg_compat.3.dylib" src/interfaces/ecpg/compatlib/libecpg_compat.3.dylib
  ```

  Also patch inter-library references in `src/interfaces/ecpg/ecpglib` and
  `src/interfaces/ecpg/compatlib`, and patch any already-built ECPG test
  executables if rerunning `src/interfaces/ecpg/test` without rebuilding them.

- Do not run temp-instance smokes that use `tmp_install` in parallel with
  recursive check targets that recreate `tmp_install`. In particular,
  `gmake -C src/test/modules/test_backend_runtime check` deletes and
  reinstalls `tmp_install`; any concurrent smoke using
  `$PWD/tmp_install/usr/local/pgsql/bin` can fail for environmental reasons
  before it reaches the code being tested.

- For focused process-mode regression checks, run the test driver directly with
  the temp install first on `PATH`, for example:

  ```sh
  cd src/test/regress
  PATH="$PWD/../../../tmp_install/usr/local/pgsql/bin:$PWD:$PATH" \
  DYLD_LIBRARY_PATH="$PWD/../../../tmp_install/usr/local/pgsql/lib" \
  INITDB_TEMPLATE="$PWD/../../../tmp_install/initdb-template" \
  ./pg_regress --temp-instance=./tmp_check --inputdir=. --bindir= --dlpath=. --dbname=regression guc
  ```

  If `$PWD/../../../tmp_install/initdb-template` or the equivalent relative
  path for the current test directory does not exist, omit `INITDB_TEMPLATE`
  and let `pg_regress` run a fresh `initdb`. A missing template fails before
  SQL starts with a `cp ... initdb-template: No such file or directory` error.

  On macOS, Unix-domain socket paths are limited. Live smokes from this long
  checkout path can fail before SQL starts with `Unix-domain socket path ... is
  too long (maximum 103 bytes)`. Use a short `mktemp -d /tmp/...` directory for
  ad hoc temp clusters that need Unix sockets.

  Threaded temp clusters currently require the database locale to match the
  postmaster process locale. In this checkout the shell commonly reports
  `LC_CTYPE=C.UTF-8`, so direct threaded smokes should initialize clusters with
  `initdb --locale=C.UTF-8` rather than `--no-locale`; otherwise threaded
  client backends fail before SQL starts with
  `database locale is incompatible with threaded backend mode`.

  If killed threaded temp clusters leave SysV shared-memory IDs behind,
  follow-up `initdb` runs can fail during bootstrap with
  `could not create shared memory segment: No space left on device` even when
  disk space is fine. First confirm no PostgreSQL server processes from this
  checkout are still running, then inspect and remove stale segments with
  `ipcs -m` and `ipcrm -m <id>`.

  Many individual regression tests assume fixture objects created by earlier
  `parallel_schedule` groups. If a direct focused run fails with missing tables
  such as `onek` or `tenk1`, rerun with the schedule prefix that builds the
  fixture state, for example:

  ```sh
  ./pg_regress --temp-instance=./tmp_check --inputdir=. --bindir= --dlpath=. --dbname=regression \
    test_setup copy copyselect copydml copyencoding insert insert_conflict \
    create_function_c create_misc create_operator create_procedure create_table create_type create_schema \
    create_index create_index_spgist create_view index_including index_including_gist \
    create_aggregate create_function_sql create_cast constraints triggers select vacuum sanity_check guc
  ```

  The `horology` test has its own date/time fixture dependencies. Run
  `date time timetz timestamp timestamptz interval` before `horology` in direct
  focused runs, matching `parallel_schedule`.

  The `select_parallel` test can produce plan-shape diffs if the direct run
  only includes `create_misc`; include the schedule prefix through
  `create_index`, `vacuum`, `guc`, and `sysviews` before `select_parallel`.

  The `privileges` test has an opening large-object cleanup query whose
  expected output assumes no matching leftover objects. In direct focused runs
  that include both files, run `privileges` before `largeobject`, or run them
  in separate temp instances.

  The `stats` test expects helper objects from `stats_ext`; include
  `stats_ext` before `stats` in direct focused runs.

  The `float8` test expects the permanent `FLOAT8_TBL` fixture from
  `test_setup` after it drops its temporary table. Direct focused runs should
  use at least:

  ```sh
  ./pg_regress --temp-instance=./tmp_check --inputdir=. --bindir= --dlpath=. --dbname=regression \
    test_setup float8
  ```

- `guc_privs` is not a core `src/test/regress` test. It lives under
  `src/test/modules/unsafe_tests`.
- Built-in GUCs whose direct backing variables moved into `PgSession` must
  carry `threaded_accessor => 'PgCurrent...Ref'` in
  `src/backend/utils/misc/guc_parameters.dat`. Do not add a hand-written
  rebind row in `guc.c`; `gen_guc_tables.pl` emits
  `ThreadedSessionGUCRebinds` from the data file.
- `analyze` is not a core `src/test/regress` test file in this checkout. For
  focused sampling/ANALYZE validation, use a live temp-cluster smoke that
  creates a table, inserts enough rows, runs `ANALYZE`, and verifies visible
  `pg_stats` rows.
- `create_role` is not reliable as a standalone direct `pg_regress` test in
  this checkout. It appears late in `parallel_schedule` and assumes earlier
  fixture/public-schema state; for focused superuser/role-cache validation,
  prefer `roleattributes` plus a live temp-cluster role privilege smoke unless
  you are intentionally running the larger schedule prefix.
- The extension backend-model tests need the test extension module installed
  into the current temp install before direct `pg_regress` runs:

  ```sh
  gmake -C src/test/modules/test_extensions DESTDIR="$PWD/tmp_install" install
  ```

- The threaded backend-runtime TAP fixture uses
  `CREATE EXTENSION test_backend_runtime_threaded`. After changing
  `src/test/modules/test_backend_runtime/test_backend_runtime_threaded.c`, its
  extension control/SQL files, or the module Makefile/meson metadata, reinstall
  that module before manual threaded smokes:

  ```sh
  gmake -C src/test/modules/test_backend_runtime DESTDIR="$PWD/tmp_install" install
  ```

- The direct threaded backend-runtime TAP also installs representative contrib
  modules into `tmp_install` before starting its threaded node: `hstore`,
  `pg_trgm`, `btree_gist`, `pageinspect`, and `pg_plan_advice`. Keep this as
  a Gate E2 representative extension smoke. These modules opt in with
  thread-per-session backend-model metadata; `pg_trgm` also moves its custom
  GUC backing variables into `PgSession.extension_modules`, and
  `pg_plan_advice` routes its module-wide context/hook-list state through
  `PgRuntime.extension_modules` before opting in. `pg_plan_advice` is a
  loadable module, not a SQL extension with a control file, so TAP coverage
  must use `LOAD 'pg_plan_advice'`. Keep that load near the end of
  `001_threaded_runtime.pl` until its planner-hook behavior with threaded
  parallel query has a dedicated audit; loading it before the parallel-query
  smoke has caused the server connection to drop during that later smoke.
  Future contrib opt-ins need the same mutable-state audit. Phase 16 still
  owns contrib-wide threaded regression.

- The backend-runtime state/PMChild regression is expected to be runnable as a
  focused process-mode control after the same module install. The fake
  thread-runtime helper tests should construct `PgThreadBackendRuntimeState`
  objects without installing them into the active SQL backend:

  ```sh
  cd src/test/modules/test_backend_runtime
  PATH="$PWD/../../../../tmp_install/usr/local/pgsql/bin:$PATH" \
  DYLD_LIBRARY_PATH="$PWD/../../../../tmp_install/usr/local/pgsql/lib" \
  ../../../../src/test/regress/pg_regress --temp-instance=./tmp_check \
    --inputdir=. --outputdir=output \
    --bindir="$PWD/../../../../tmp_install/usr/local/pgsql/bin" \
    --dlpath=. test_backend_runtime
  ```

- GUC custom-prefix smoke tests that preload `test_oat_hooks` need that module
  installed into the current temp install first:

  ```sh
  gmake -C src/test/modules/test_oat_hooks DESTDIR="$PWD/tmp_install" install
  ```

- Threaded runtime GUC stack coverage in
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` verifies
  built-in database/role/startup defaults, `SET LOCAL` rollback/commit
  behavior, `RESET` back to database and startup-packet sources, and custom
  extension GUC `SET LOCAL`/`RESET` semantics through the superuser `LOAD`
  path. Unprivileged `LOAD 'test_backend_runtime_threaded'` is expected to
  fail with the normal library-access policy error unless explicit load
  privileges are granted.

  Manual concurrent threaded GUC smokes should capture background client PIDs
  with `$!` and wait for that explicit list. Do not rely on `jobs -p` in the
  non-interactive zsh shell; it can produce an empty list, causing the harness
  to stop the temp postmaster before the background clients finish.

- Abandoned-client teardown smokes should leave the backend idle in
  transaction before killing the client, matching `background_psql` behavior.
  Do not use `SELECT pg_sleep(...)` as the wait point for that fixture: killing
  the frontend while the backend is inside `pg_sleep` can leave the advisory
  lock visible until the running query observes an interrupt or finishes,
  which tests a different path from idle-client abandonment.

- Direct logical replication parallel-apply smokes should use the upstream
  `src/test/subscription/t/015_stream.pl` interleaved transaction shape:
  start one large transaction, run and commit a second large transaction while
  the first remains open, then commit the first. A single large transaction
  followed by `pg_sleep()` can replicate successfully without proving the
  `STREAM_START`/parallel apply path. The parallel worker is pooled and can be
  hard to catch by polling `pg_stat_activity`; use the subscriber log marker
  for `logical replication parallel apply worker for subscription`, the final
  replicated row/default counts, and a postmaster child-process check as the
  primary smoke evidence.

- Manual threaded slot-sync smokes that use `pg_basebackup -R` should write
  the final `primary_conninfo` containing `dbname=postgres` into
  `postgresql.auto.conf`, not only `postgresql.conf`. The `-R` generated
  `primary_conninfo` in `postgresql.auto.conf` otherwise overrides the later
  config-file value and makes the slot sync worker restart with
  `replication slot synchronization requires "dbname" to be specified in
  "primary_conninfo"` before testing the intended threaded path.

- Threaded checkpointer/background-writer smokes should wait for the
  post-startup handoff. In threaded mode those workers intentionally start as
  processes before recovery forks the startup process, then exit and relaunch
  as thread carriers after `PM_RUN` and after another thread carrier exists.
  Good smoke evidence is one logical `checkpointer` and one logical
  `background writer` in `pg_stat_activity`, no OS child command containing
  `checkpointer` or `background writer` under the postmaster, a successful
  `CHECKPOINT`, and clean fast shutdown. In process-mode compatibility smokes,
  the same workers should still appear as OS child processes.

- Plain `multithreaded=on` temp clusters should complete
  `pg_ctl -m fast stop` cleanly. If a Phase 11 worker smoke hangs during fast
  stop, sample the postmaster before cleanup; a previous blocker was a
  thread-backed worker that consumed logical interrupts without routing
  shutdown requests through `ProcessMainLoopInterrupts()`.

- PostgreSQL TAP tests require the non-core Perl module `IPC::Run`. Use the
  repo-local `.perl5` paths in direct TAP commands; this checkout currently has
  a usable `IPC::Run` there. The system Perl may not provide `IPC::Run`, and
  older CPAN attempts through the normal system/local-lib path stalled while
  installing `IO::Tty`. If the repo-local path is unavailable, the unpacked
  pure-Perl IPC::Run build can also be used directly from:

  ```sh
  /Users/samwillis/.cpan/build/IPC-Run-20260402.0-5/blib/lib
  ```

  To retry a normal local install without relying on system Perl paths, use:

  ```sh
  PERL_MM_USE_DEFAULT=1 \
  PERL_MM_OPT="INSTALL_BASE=$HOME/perl5" \
  PERL_MB_OPT="--install_base $HOME/perl5" \
  cpan -T -i IPC::Run
  ```

  A currently verified local install uses
  `$HOME/.local/perl5-postgres-tests`. If system `cpan` hangs while installing
  `IO::Tty`, reuse the unpacked CPAN build directories with local
  `INSTALL_BASE`:

  ```sh
  PERL_PREFIX="$HOME/.local/perl5-postgres-tests"
  cd "$HOME/.cpan/build/IO-Tty-1.31-7"
  /usr/bin/perl Makefile.PL INSTALL_BASE="$PERL_PREFIX"
  /usr/bin/make
  /usr/bin/make install
  cd "$HOME/.cpan/build/IPC-Run-20260402.0-7"
  PERL5LIB="$PERL_PREFIX/lib/perl5/darwin-thread-multi-2level:$PERL_PREFIX/lib/perl5" /usr/bin/perl Makefile.PL INSTALL_BASE="$PERL_PREFIX"
  PERL5LIB="$PERL_PREFIX/lib/perl5/darwin-thread-multi-2level:$PERL_PREFIX/lib/perl5" /usr/bin/make
  PERL5LIB="$PERL_PREFIX/lib/perl5/darwin-thread-multi-2level:$PERL_PREFIX/lib/perl5" /usr/bin/make install
  ```

  Keep the repo-local `.perl5` paths in direct TAP commands. This checkout is
  still configured without `--enable-tap-tests`, so
  recursive `gmake ... check` targets report `TAP tests not enabled`. Do not
  treat that configure-time message as a reason to skip TAP coverage; run the
  direct `prove` command with the local `PERL5LIB` path. Direct `prove` runs
  also need the same harness environment that
  `gmake check` supplies, especially `PG_REGRESS`; if `PG_REGRESS` is missing,
  `PostgreSQL::Test::Cluster->init` can call `system_or_bail()` with an
  undefined command and `prove` may report an empty skip reason before the
  server starts. A minimal direct environment is:

  ```sh
  PATH="/opt/homebrew/bin:$PWD/tmp_install/usr/local/pgsql/bin:$PATH" \
  GMAKE="/opt/homebrew/bin/gmake" \
  DYLD_LIBRARY_PATH="$PWD/tmp_install/usr/local/pgsql/lib" \
  INITDB_TEMPLATE="$PWD/tmp_install/initdb-template" \
  PG_REGRESS="$PWD/src/test/regress/pg_regress" \
  PERL5LIB="$PWD/.perl5/lib/perl5:$PWD/.perl5/lib/perl5/darwin-thread-multi-2level:$PWD/src/test/perl" \
  prove -I src/test/perl src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl
  ```

  The direct threaded backend-runtime TAP can be run with the verified local
  install like this after installing `test_backend_runtime` into `tmp_install`
  and patching the temp-install dylib paths:

  ```sh
  PERL_PREFIX="$HOME/.local/perl5-postgres-tests"
  ROOT="$PWD"
  TESTDIR="$ROOT/src/test/modules/test_backend_runtime"
  rm -rf "$TESTDIR/tmp_check"
  mkdir -p "$TESTDIR/tmp_check/log"
  cd "$TESTDIR"
  export PERL5LIB="$PERL_PREFIX/lib/perl5/darwin-thread-multi-2level:$PERL_PREFIX/lib/perl5:$ROOT/src/test/perl:$TESTDIR"
  export PATH="$ROOT/tmp_install/usr/local/pgsql/bin:$TESTDIR:/opt/homebrew/bin:$PATH"
  export DYLD_LIBRARY_PATH="$ROOT/tmp_install/usr/local/pgsql/lib"
  export INITDB_TEMPLATE="$ROOT/tmp_install/initdb-template"
  export TESTLOGDIR="$TESTDIR/tmp_check/log"
  export TESTDATADIR="$TESTDIR/tmp_check"
  export PGPORT=65432
  export top_builddir="$ROOT"
  export PG_REGRESS="$ROOT/src/test/regress/pg_regress"
  export share_contrib_dir="$ROOT/tmp_install/usr/local/pgsql/share/extension"
  export GMAKE=/opt/homebrew/bin/gmake
  prove -v -I "$ROOT/src/test/perl" -I "$TESTDIR" t/001_threaded_runtime.pl
  ```

  If another check, such as `gmake -C src/pl/plperl check`, recreates
  `tmp_install` before direct backend-runtime TAP, reinstall
  `test_backend_runtime` into that temp install first:

  ```sh
  gmake -C src/test/modules/test_backend_runtime DESTDIR="$PWD/tmp_install" install
  ```

  Otherwise `001_threaded_runtime.pl` can start the server but fail at
  `CREATE EXTENSION test_backend_runtime_threaded`, and
  `002_threaded_bgworker_crash.pl` can fail to load
  `test_backend_runtime_threaded`.

  Module TAP tests that normally run through `prove_check` may need the full
  makefile harness environment. For worker SPI, run from the module directory
  after refreshing `tmp_install` and patching the temp-install dynamic library
  names:

  ```sh
  cd src/test/modules/worker_spi
  rm -rf tmp_check && mkdir -p tmp_check/log
  PERL5LIB="/Users/samwillis/.cpan/build/IPC-Run-20260402.0-5/blib/lib:$OLDPWD/src/test/perl" \
  TESTLOGDIR="$PWD/tmp_check/log" \
  TESTDATADIR="$PWD/tmp_check" \
  PATH="$OLDPWD/tmp_install/usr/local/pgsql/bin:$PWD:$PATH" \
  DYLD_LIBRARY_PATH="$OLDPWD/tmp_install/usr/local/pgsql/lib" \
  INITDB_TEMPLATE="$OLDPWD/tmp_install/initdb-template" \
  PGPORT=65432 \
  top_builddir="$PWD/../../../.." \
  PG_REGRESS="$PWD/../../../../src/test/regress/pg_regress" \
  share_contrib_dir="$OLDPWD/tmp_install/usr/local/pgsql/share/extension" \
  enable_injection_points=no \
  prove -I "$OLDPWD/src/test/perl" -I . t/001_worker_spi.pl t/002_worker_terminate.pl
  ```

- In the managed Codex sandbox, PostgreSQL temp-instance tests can fail during
  `initdb` with `could not create shared memory segment: Operation not
  permitted` from `shmget()`. Treat that as a sandbox restriction, not a
  PostgreSQL regression. Rerun the same test outside the sandbox/with
  escalation, or force a POSIX DSM configuration when that is sufficient for
  the check.

- Repeated crash-debugging of threaded temp clusters on macOS can leave stale
  SysV shared-memory segments and semaphore sets even when no `postgres`
  process remains. If `initdb` fails with `shmget(...): No space left on
  device`, first confirm that no PostgreSQL server process is still running:

  ```sh
  ps -axo pid,ppid,stat,command | rg '[p]ostgres|[p]ostmaster|[i]nitdb' || true
  ```

  Prefer removing only detached shared-memory segments (`NATTCH` is zero in
  `ipcs -ma`). Do not remove attached segments from unrelated running
  PostgreSQL instances. Start with detached shared memory owned by the current
  user:

  ```sh
  for id in $(ipcs -ma | awk '$1 == "m" && $9 == 0 && $5 == "'$USER'" {print $2}'); do ipcrm -m "$id" || true; done
  ```

  Remove semaphore sets only after identifying them as stale test leftovers;
  they do not expose attachment counts in the same way as shared-memory
  segments.

- This shell is zsh. Cleanup commands with unmatched globs, such as
  `rm -rf tmp_check_*`, can fail with `no matches found` before the test command
  runs. Use a matched path, `find`, or enable null-glob behavior when cleaning
  optional TAP/regression scratch directories.

## Terminology

- Runtime: one server runtime inside an address space. In process mode, each
  backend process has its own private address space plus shared memory. In
  threaded mode, many backends share one address space.
- Carrier: the physical execution vehicle, such as an OS process, OS thread,
  or future host scheduler worker.
- Backend: a logical PostgreSQL backend identity visible to cancellation,
  statistics, lock ownership, and monitoring.
- Session: SQL session state for a client or pooled logical session.
- Execution: active transaction/query/portal work currently consuming backend
  resources.
- Connection: frontend/backend protocol transport. It is usually a socket in
  native PostgreSQL, but should not be architecturally identical to a session.
