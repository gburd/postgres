# Phase 6 Backend Lifecycle And Exit Notes

This note records the current Phase 6 exit boundary so remaining `proc_exit()`
and `exit()` call sites are intentional rather than an unexplained search
result.

## Current Boundary

- `PgBackendExit(int code)` is the logical backend exit API. In process mode it
  preserves the existing behavior by running backend cleanup and then exiting
  the process.
- `PgBackendExitCleanup(int code)` runs the backend-local cleanup sequence:
  `before_shmem_exit`, DSM detach callbacks, `on_shmem_exit`, then
  `on_proc_exit`.
- After cleanup, `PgBackendExit()` calls `PgBackendExitComplete(int code)`.
  That function dispatches through the current runtime's optional
  `exit_backend` continuation. Process mode leaves this unset and falls through
  to `PgBackendExitProcess(int code)`, the private tail that still calls
  `exit(code)`.
- `proc_exit(int code)` remains as a compatibility wrapper for process-mode
  code and unmigrated callers.
- `PgBackendExitState` is stored on `PgBackend` and owns the exit callback
  stacks plus backend-local exit-in-progress flags.
- Dynamic shared memory mapping descriptors are stored on `PgBackend`.
  `dsm_backend_shutdown()`, `dsm_detach_all()`, `dsm_find_mapping()`, and
  `reset_on_dsm_detach()` operate on the current logical backend's mapping
  list rather than a process-global list. This keeps DSM detach callbacks and
  mapping reference cleanup local to the backend that is exiting.
- A threaded runtime must install a non-returning `exit_backend` continuation
  that removes the logical backend from its scheduler without returning to the
  cleaned-up backend stack.
- Early exit registrations made before a `PgBackend` exists use a small
  fallback state. `InitializePgProcessRuntime()` adopts that fallback state into
  the process backend so postmaster-child cleanup callbacks, including
  `MarkPostmasterChildInactive`, still run at normal backend exit.
- DSM calls made before `CurrentPgBackend` exists use a small runtime fallback
  list. Normal backend DSM mappings are expected to exist under an initialized
  `PgBackend`; the fallback exists for reset/detach paths that run before
  process runtime installation.

Core code that needs to test exit progress should call
`PgBackendExitInProgress()` or `PgBackendShmemExitInProgress()` rather than
reading the legacy exported globals directly. The globals remain as process-mode
compatibility mirrors.

## Migrated Logical Exits

These paths now go through `PgBackendExit()`:

- frontend EOF / Terminate handling in the main backend loop;
- FATAL and FATAL_CLIENT_ONLY error termination;
- startup/authentication early disconnect paths;
- logical launcher and I/O worker shutdown paths reached through
  `ProcessInterrupts()`;
- WAL sender connection, protocol, shutdown, timeout, and postmaster-death
  exits;
- logical replication apply, table sync, slot sync, sync utility, and parallel
  apply worker exits.

## Remaining Process Or Runtime Exits

The remaining direct `proc_exit()` and `exit()` call sites have the following
Phase 6 ownership decisions:

- `PgBackendExitProcess()` and the `proc_exit()` wrapper are the process-mode
  compatibility tail. They are the only normal backend-exit path that should
  call `exit()` after logical backend cleanup.
- Postmaster, launch-backend, bootstrap, single-user, frontend command-line,
  help/version, and configuration-file startup failures are process lifetime
  paths, not logical backend exits.
- Startup, checkpointer, bgwriter, walwriter, archiver, syslogger, WAL
  receiver, WAL summarizer, and AIO method workers remain auxiliary
  process-owned workers for Phase 6. Phase 11, Auxiliary Worker Thread
  Runtime, must add an explicit worker/auxiliary runtime owner before any of
  these are converted to threaded carriers.
- Generic background workers remain process-owned for Phase 6 so third-party
  extension workers preserve current behavior. Extension/background-worker
  thread compatibility is gated by Phase 11 worker runtime work and Phase 16
  extension metadata hardening.
- Autovacuum launcher and workers remain process-owned workers for Phase 6.
  Phase 11 must migrate them through the worker runtime, but they are not user
  sessions and should not be silently folded into the session backend lifecycle.
- Low-level postmaster-death wait paths, recovery-target shutdown, archive
  restore signal handling, scanner fatal exits, spinlock hard failures,
  pre-error-system crashes, and signal-handler paths are escalation paths. They
  intentionally terminate the process or runtime rather than returning to a
  scheduler.

These decisions make the remaining `proc_exit()` search results intentional.
Phase 11 is responsible for defining which runtime object owns each threaded
worker's cleanup and scheduler continuation.

## Validation So Far

- Focused object builds passed for touched backend lifecycle, tcop, error,
  libpq, portal, WAL sender, and logical replication worker files.
- `gmake -C src/backend -j8` passed after the backend-local exit-state changes
  and after the replication worker migration.
- `gmake -C src/backend postgres` passed after moving DSM mapping ownership
  onto `PgBackend`.
- `perl src/tools/global_lifetime/scan_global_lifetimes.pl --baseline
  src/tools/global_lifetime/global_lifetime_baseline.tsv` reported no new
  unclassified mutable globals.
- `gmake -C src/test/regress check` passed 245/245 after the backend-local
  exit-state change, after the replication worker migration, and after adding
  the runtime exit continuation.
- `gmake -C src/test/isolation check` passed 129/129 during Gate B validation.
- `gmake check-world` passed for the configured test coverage during Gate B
  validation. TAP-only subtrees were skipped by the build system because this
  checkout is not configured with `--enable-tap-tests`.
- `gmake -C src/test/modules/test_dsm_registry check` passed after adding a
  fixture that registers an `on_dsm_detach` callback, leaves its DSM mapping
  pinned for backend-exit cleanup, reconnects, and verifies the callback ran.
- The same `test_dsm_registry` fixture now records callback ordering across an
  actual backend disconnect and verifies `before_shmem_exit` callbacks run in
  LIFO order, followed by DSM detach callbacks, `on_shmem_exit` callbacks in
  LIFO order, and then `on_proc_exit` callbacks in LIFO order.
- The same fixture creates an inter-transaction temporary file, leaves it open
  across the backend disconnect, reconnects, and verifies backend-exit cleanup
  closed and unlinked the temporary file.
- Gate B initially exposed a race in this fixture: `pg_regress` can reconnect
  before the previous backend has finished `proc_exit()` callbacks. The fixture
  now waits briefly for the final `on_proc_exit` marker before comparing the
  callback trace.
- `gmake -C src/test/modules/test_backend_runtime check` passed after adding a
  fixture that installs a runtime `exit_backend` continuation, calls
  `PgBackendExitComplete(17)`, and verifies control transfers to the
  scheduler-like continuation instead of falling through to process exit.
- The same `test_backend_runtime` module now simulates two logical backends in
  one address space, creates a pinned DSM mapping under one backend, runs
  `dsm_backend_shutdown()` under the other backend, and verifies the first
  backend's mapping remains attached.
- A focused Gate B smoke test passed timeout routing, active-query
  cancellation, config reload, LISTEN/NOTIFY, client disconnect during an open
  transaction, and backend termination/FATAL-path responsiveness checks.

## Deferred Thread Runtime Proof

There is not yet a real thread-per-session runtime running full backend exit
cleanup while another in-process backend continues. That is not a Phase 6
blocker; it is the end-to-end proof for Phase 10, where threaded backend launch
first exists.

Phase 6's boundary is the lifecycle split and ownership model needed before
thread launch: logical backend exit no longer has to mean direct process exit,
cleanup state and DSM mappings are owned by `PgBackend`, process-mode behavior
is preserved, and the post-cleanup runtime handoff contract is tested.
