# Multithreaded PostgreSQL Agent Guide

This repository is an experimental branch for making PostgreSQL capable of
running backend sessions in a multithreaded runtime. The branch is allowed to
be ambitious and is not currently optimized for upstream patch shape.

Implementation is now underway. Keep the plan and architecture notes current as
the code evolves.

## Project Docs

- [MULTITHREADED_ARCHITECTURE.md](MULTITHREADED_ARCHITECTURE.md) describes the
  desired end-state architecture.
- [MULTITHREADED_PLAN.md](MULTITHREADED_PLAN.md) describes the staged
  implementation plan, validation strategy, and risk register.
- [MULTITHREADED_PHASE5_INTERRUPTS.md](MULTITHREADED_PHASE5_INTERRUPTS.md)
  records the logical interrupt boundary and recovery-conflict fixture
  decision.
- [MULTITHREADED_PHASE6_EXIT.md](MULTITHREADED_PHASE6_EXIT.md) records the
  current backend lifecycle/exit boundary and deferred thread-runtime proof.
- [MULTITHREADED_PHASE7_EXTENSIONS.md](MULTITHREADED_PHASE7_EXTENSIONS.md)
  records the extension backend-model gate and PL/pgSQL audit result.
- [MULTITHREADED_PHASE8_THREAD_SAFETY.md](MULTITHREADED_PHASE8_THREAD_SAFETY.md)
  records the first thread-local bridge for backend-local state and the
  remaining Phase 8 thread-safety floor.
- [MULTITHREADED_PHASE9_WAIT_BOUNDARY.md](MULTITHREADED_PHASE9_WAIT_BOUNDARY.md)
  records the current logical wait/suspend boundary work.
- [MULTITHREADED_PHASE10_THREAD_RUNTIME.md](MULTITHREADED_PHASE10_THREAD_RUNTIME.md)
  records the thread-per-session runtime work.
- [refs/REFERENCES.md](refs/REFERENCES.md) lists external references.
- [refs/pgconf-2025-multithreading-transcript.md](refs/pgconf-2025-multithreading-transcript.md)
  is the local transcript of the PgConf.dev 2025 talk that motivates this work.

## Working Assumptions

- Use Heikki Linnakangas's multithreading branch as reference material, not as
  a base to merge wholesale.
- Preserve multiprocess PostgreSQL as a supported backend model.
- The first native threading target should be thread-per-session. The longer
  term target is an explicit scheduler that can map sessions/executions to a
  pool of carriers.
- Thread-per-session for regular client backends is not the final normal-mode
  target. Normal threaded server mode should eventually run in-tree
  server-owned workers, including autovacuum and auxiliary worker families, as
  threaded runtime-owned workers rather than forked subprocesses.
- Single-user mode, bootstrap mode, frontend command-line utilities,
  postmaster/control-plane process lifetime, and crash-escalation paths are
  deliberate process-lifetime exceptions.
- Do not overfit the design to WASM. Keep the main-loop and wait-boundary
  abstractions clean enough that a future host-driven runtime can use them.
- Existing third-party C extensions may be process-backend-only. That is an
  acceptable compatibility break for threaded mode.
- Existing third-party background workers may remain process-only or be
  rejected in threaded mode unless explicit worker-runtime metadata opts them
  in.
- In-tree modules and important bundled languages, especially PL/pgSQL, should
  have a plausible path to work in threaded mode.

## Source Orientation

Important current files:

- `src/backend/tcop/postgres.c`: `PostgresMain()`, the top-level backend loop,
  error recovery, command read, command dispatch, and `ProcessInterrupts()`.
- `src/include/access/session.h` and `src/backend/access/common/session.c`:
  existing `Session` abstraction for session-scoped DSM/DSA state. Treat this
  as a seed for the broader session object unless there is a strong reason not
  to.
- `src/include/miscadmin.h`: widely visible process/session globals and the
  interrupt macros.
- `src/backend/storage/ipc/procsignal.c`: process-signal-style backend
  communication.
- `src/backend/storage/ipc/latch.c` and `src/backend/storage/ipc/waiteventset.c`:
  wait/wakeup infrastructure.
- `src/backend/postmaster/launch_backend.c` and
  `src/backend/postmaster/postmaster.c`: backend launch and supervision.
- `src/backend/postmaster/autovacuum.c`,
  `src/backend/postmaster/auxprocess.c`,
  `src/backend/postmaster/bgworker.c`, and the individual auxiliary worker
  files under `src/backend/postmaster/`: worker launch, supervision, and
  server-owned worker lifecycles.
- `src/backend/replication/walreceiver.c`,
  `src/backend/replication/logical/launcher.c`,
  `src/backend/replication/logical/worker.c`, and
  `src/backend/storage/aio/method_worker.c`: replication and AIO worker
  lifecycles that must eventually use the threaded worker runtime.
- `src/include/fmgr.h` and `src/backend/utils/fmgr/dfmgr.c`: extension module
  ABI checks.
- `src/pl/plpgsql`: PL/pgSQL implementation.

## Development Rules For This Branch

- Keep documentation and code commits coherent. Prefer one conceptual change
  per commit.
- Before editing core code, read the surrounding implementation and current
  comments. PostgreSQL has many invariants that are documented only locally.
- Keep process-mode behavior working after each implementation phase.
- Use static annotations and tools to classify globals before moving large
  amounts of state.
- Do not attempt thread launch until the thread-safety floor is in place:
  backend-local globals must not be shared plain process globals, backend exit
  must not terminate the whole runtime, and timeout/interrupt delivery must be
  per logical backend.
- Prefer introducing compatibility wrappers around current globals before
  changing all call sites.
- Avoid broad mechanical churn unless it unlocks a specific migration step.
- Do not remove process isolation paths merely because threaded mode exists.

## Local Build And Test Notes

- This checkout is commonly built with GNU make on macOS. Use `gmake`, not the
  BSD `make`. In the Codex desktop shell, Homebrew's bin directory may be
  absent from `PATH`; if `gmake` is not found, use `/opt/homebrew/bin/gmake` or
  export `PATH="/opt/homebrew/bin:$PATH"` before building.
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

  `pg_global_prng_state` is also exported through an installed common header
  and is referenced by some contrib/test modules, including `amcheck`,
  `auto_explain`, `tablefunc`, and several `src/test/modules` tests. Clean and
  reinstall any of those modules before testing them after PRNG TLS changes.

- If an installed header changes a global from plain storage to
  `PG_THREAD_LOCAL`, do not trust a purely incremental backend build. Stale
  backend objects can still compile and link but then crash during `initdb`
  post-bootstrap single-user startup. Use the backend clean plus generated-file
  recovery above, then rebuild with `gmake -j8`.

- If `PMChild` layout changes in `src/include/postmaster/postmaster.h`, do not
  trust an incremental build of postmaster objects. Stale postmaster objects can
  corrupt the PMChild freelists or crash auxiliary children during temp-instance
  startup. Use:

  ```sh
  gmake -C src/backend/postmaster clean
  gmake -C src/backend -j8
  ```

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

- This checkout is currently configured with `with_llvm = no`. Direct builds
  under `src/backend/jit/llvm` fail before reaching project changes because
  the LLVM Makefile requires an LLVM-enabled configuration. For LLVM-only
  source annotations, use static lifetime scan coverage plus a full non-LLVM
  build here, and use an LLVM-enabled build when compile or runtime JIT
  coverage for those files is required.

- This checkout is currently configured without `--enable-injection-points`.
  `src/test/modules/injection_points` intentionally skips checks in that
  configuration, and injection-point TAP/regression coverage requires a build
  configured with injection points enabled. For injection-point-only source
  annotations in this checkout, use object compile coverage where reachable,
  static lifetime scan coverage, and a full non-injection build/install.

- Some `gmake ... check` runs fail on macOS because temporary-install binaries
  still refer to `/usr/local/pgsql/lib/libpq.5.dylib`. Patch the temp install
  before running direct `pg_regress` commands:

  ```sh
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" "$PWD/tmp_install/usr/local/pgsql/bin/initdb" || true
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" "$PWD/tmp_install/usr/local/pgsql/bin/psql"
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" "$PWD/tmp_install/usr/local/pgsql/bin/pg_ctl" || true
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" "$PWD/tmp_install/usr/local/pgsql/bin/pg_basebackup" || true
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" "$PWD/tmp_install/usr/local/pgsql/lib/libpqwalreceiver.dylib" || true
  ```

  Tests that create subscriptions can reach `libpqwalreceiver.dylib`; patch it
  along with the frontend binaries after reinstalling or recreating
  `tmp_install`.

  Direct isolation runs can fail the same way from build-tree binaries. Patch
  `src/test/isolation/isolationtester` and
  `src/test/isolation/pg_isolation_regress` to the same temp-install
  `libpq.5.dylib` before rerunning them.

  `gmake -C src/test/regress check-tests` recreates `tmp_install`, so a
  previously patched `psql` can become unpatched again. If that target fails
  before SQL starts with a `dyld` `libpq.5.dylib` loader error, patch the new
  temp-install binaries and rerun the equivalent `pg_regress` command directly.

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

- GUC custom-prefix smoke tests that preload `test_oat_hooks` need that module
  installed into the current temp install first:

  ```sh
  gmake -C src/test/modules/test_oat_hooks DESTDIR="$PWD/tmp_install" install
  ```

- Direct logical replication parallel-apply smokes should poll while the
  publisher transaction is still open. The parallel worker can be transient,
  and `pg_stat_activity.backend_type` reports it as
  `logical replication parallel worker`.

- PostgreSQL TAP tests require the non-core Perl module `IPC::Run`. The system
  Perl on this macOS checkout may not have it, in which case direct `prove`
  invocations fail before starting PostgreSQL with `Can't locate IPC/Run.pm`.
  Install `IPC::Run` into the Perl used for the build before treating TAP
  coverage as runnable.

- In the managed Codex sandbox, PostgreSQL temp-instance tests can fail during
  `initdb` with `could not create shared memory segment: Operation not
  permitted` from `shmget()`. Treat that as a sandbox restriction, not a
  PostgreSQL regression. Rerun the same test outside the sandbox/with
  escalation, or force a POSIX DSM configuration when that is sufficient for
  the check.

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
