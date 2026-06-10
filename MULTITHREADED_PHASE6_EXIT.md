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
- `PgBackendExitProcess(int code)` is the private process-mode tail that still
  calls `exit(code)`.
- `proc_exit(int code)` remains as a compatibility wrapper for process-mode
  code and unmigrated callers.
- `PgBackendExitState` is stored on `PgBackend` and owns the exit callback
  stacks plus backend-local exit-in-progress flags.
- Early exit registrations made before a `PgBackend` exists use a small
  fallback state. `InitializePgProcessRuntime()` adopts that fallback state into
  the process backend so postmaster-child cleanup callbacks, including
  `MarkPostmasterChildInactive`, still run at normal backend exit.

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

The remaining direct `proc_exit()` and `exit()` call sites are currently treated
as process/runtime exits or as not-yet-migrated background-worker families:

- postmaster, launcher, frontend command-line, bootstrap, and single-user
  startup/shutdown paths;
- auxiliary process families such as startup, checkpointer, bgwriter,
  walwriter, archiver, syslogger, WAL receiver, WAL summarizer, and AIO worker;
- autovacuum launcher and worker paths, which need a separate decision on
  whether they become logical backends or remain process-managed workers;
- low-level crash, recovery-target, signal-handler, and wait-event error paths
  that intentionally terminate the process or avoid normal cleanup.

These paths should not be mechanically replaced without deciding which runtime
object owns them in the threaded model.

## Validation So Far

- Focused object builds passed for touched backend lifecycle, tcop, error,
  libpq, portal, WAL sender, and logical replication worker files.
- `gmake -C src/backend -j8` passed after the backend-local exit-state changes
  and after the replication worker migration.
- `perl src/tools/global_lifetime/scan_global_lifetimes.pl --baseline
  src/tools/global_lifetime/global_lifetime_baseline.tsv` reported no new
  unclassified mutable globals.
- `gmake -C src/test/regress check` passed 245/245 after the backend-local
  exit-state change and again after the replication worker migration.
- `gmake -C src/test/subscription check` did not run TAP tests because this
  checkout is not configured with `--enable-tap-tests`.

## Remaining Phase 6 Gaps

- `PgBackendExit()` still reaches `exit()` in process mode. A threaded runtime
  needs a carrier-aware completion path that returns control to the scheduler
  after `PgBackendExitCleanup()`.
- There is not yet a thread-per-session runtime, so the branch cannot directly
  prove that one logical backend exits while other in-process backends continue.
- DSM/DSA cleanup is still invoked through the existing backend shutdown hooks.
  Core regression covers common paths, but a targeted DSM/DSA exit fixture is
  still needed.
- FATAL during an active transaction is indirectly exercised by regression
  coverage, but a focused lifecycle fixture would make the Phase 6 gate more
  explicit.
- Remaining background-worker and auxiliary-process exits need an ownership
  decision before they are migrated or documented as permanently process-only.
