# Phase 19 Increment 2 -- fork+exec process-fallback route: implementation design

Status: design (2026-07-12).  Increment 1 (classification) landed as
`66dc4d18674`.  This document is the concrete mechanism for Increment 2 -- how a
session that needs a process-only extension gets a real, isolated backend under
`multithreaded=on`, without flipping PostgreSQL into global EXEC_BACKEND mode.

## The constraint, restated from code

- The carriers run **in the postmaster process itself** (`xtc_pg_carrier_start`
  runs on the postmaster thread and captures the postmaster's own latch;
  `pg_xtc_carrier.c`).  So once `postmaster_thread_carriers_started` is true, the
  postmaster process is multithreaded.
- `fork()` without `exec()` in a multithreaded process is unsafe (only the
  forking thread survives; mutexes held by other threads stay locked forever).
  `postmaster_child_launch_carrier` already refuses this with `ENOSYS`
  (launch_backend.c) once carriers exist.
- Therefore a process-fallback backend on Linux MUST be **fork+exec**: the
  exec'd child starts from a clean single-threaded image and re-derives all the
  state it would otherwise have inherited.
- The fork+exec machinery already exists but is `#ifdef EXEC_BACKEND`
  (Windows-only in this build): `internal_forkexec`,
  `save_backend_variables` / `restore_backend_variables` (~70 state transfers),
  `SubPostmasterMain`, the `main.c:219` child dispatch, and the cross-file
  `PGSharedMemoryReAttach` (sysv_shmem.c) + `read/write_nondefault_variables`
  (guc.c).

## Why we can't just `#define EXEC_BACKEND`

Defining EXEC_BACKEND globally makes **every** backend fork+exec, which defeats
the carrier model (the whole point is that threaded backends are fibers on
carrier threads, not processes).  We need *selective* fork+exec: carriers for the
common case, fork+exec only for the fallback.

The behaviour-flipping sites are few and now enumerated (all `#ifndef
EXEC_BACKEND`):

1. `postmaster.c:2102` -- child closes inherited listen sockets.  Only matters
   in the child; a fork+exec child re-derives sockets, a fork child inherits
   them.  Per-child, decidable at launch.
2. `shmem.c:644` -- an `Assert(!IsUnderPostmaster)` relaxation.  Assert-only.
3. `guc_tables.c:462` -- removes `mmap` from the allowed `shared_memory_type`
   list.  **This is the load-bearing one** (see below).
4. `syslogger.c:191` -- syslogger readiness timing.  Child-side only.

## The load-bearing detail: shared memory must be re-attachable

An exec'd child loses the postmaster's anonymous `mmap(MAP_SHARED)` mapping and
must re-attach the main shared segment by key.  EXEC_BACKEND handles this via
**sysv** shared memory (`UsedShmemSegID`, `PGSharedMemoryReAttach`).  The default
non-EXEC_BACKEND build uses **mmap anonymous** shmem, inherited only via fork.

So a fork+exec fallback child requires the server's main shared memory to be
sysv (or another named/re-attachable type), NOT anonymous mmap.

Design decision: **when `multithreaded=on` AND process-fallback is enabled,
require/force `shared_memory_type=sysv`** (the same requirement EXEC_BACKEND
already imposes), so the fallback child can re-attach.  This is a documented
constraint, checked at startup, not a silent behaviour change.  Servers that
never need a fallback and never set it pay nothing.

## Proposed build + code shape

1. New build symbol `USE_XTC_PROCESS_FALLBACK`, defined by meson when
   `USE_XTC_CARRIER` is set and the platform is non-Windows.  It gates the
   *compilation* of the fork+exec machinery on Linux; it does NOT change any
   runtime default.

2. Change the machinery guards from `#ifdef EXEC_BACKEND` to
   `#if defined(EXEC_BACKEND) || defined(USE_XTC_PROCESS_FALLBACK)` in exactly
   the files that must compile the fork+exec path:
   - launch_backend.c (the 90-191 decls, 2183-2953 machinery; NOT the 2125-2179
     dispatch, which stays EXEC_BACKEND for the all-backends case),
   - main.c (the `SubPostmasterMain` dispatch at :219),
   - sysv_shmem.c / posix_sema.c (`PGSharedMemoryReAttach`, sema reattach),
   - guc.c (`read/write_nondefault_variables`),
   - the headers that declare these (postmaster.h, pg_shmem.h, ...).
   The 4 `#ifndef EXEC_BACKEND` behaviour sites become
   runtime-conditional on the *child's* launch model where they are child-side
   (1, 4), stay assert-only (2), and the shmem-type restriction (3) becomes "if
   process-fallback is enabled, sysv is required" rather than compile-time.

3. New launch model `PG_BACKEND_LAUNCH_PROCESS_FALLBACK` and a route in
   `postmaster_pooled_protocol_launch` (and the thread-per-session launch):
   when a session's demand is `PROCESS`, call an
   `internal_forkexec`-based launch instead of enqueuing on the carrier pool.
   The child arrives in `SubPostmasterMain`, re-attaches shmem, restores
   backend variables, and runs as an ordinary supervised process backend.

4. Supervision: the fallback child is a normal `PMChild`, counted against
   `MaxConnections`, reaped by the postmaster, subject to the process-mode
   crash-restart policy.  A crash in it is a single-backend crash (it does NOT
   share the carrier address space) -- strictly better isolation than an
   in-carrier extension crash.

## Sub-steps (each its own commit, each validated)

- (a) Add `USE_XTC_PROCESS_FALLBACK` + widen the guards so the machinery
  COMPILES on Linux, with NO runtime path change.  Validate: process mode and
  threaded mode byte-for-byte identical (`gmake check`, `check-threaded`);
  fork+exec functions exist but are never called.
- (b) Startup check: if process-fallback is enabled and shmem is not
  re-attachable (mmap), either force sysv or refuse with a clear message.
  Validate: threaded server starts with sysv; mmap+fallback is rejected clearly.
- (c) Add `PG_BACKEND_LAUNCH_PROCESS_FALLBACK` + the fork+exec launch route,
  gated behind a **forced test knob** (a session/developer GUC, e.g.
  `xtc_force_process_fallback`) so a session can be deterministically routed to
  a process backend regardless of extension needs.  Validate on EC2: a normal
  session forced to fallback runs correctly as a process backend under
  `multithreaded=on`; carriers still serve un-forced sessions.
- (d) Wire real detection (Increment 3): a process-only extension in the
  session's preload GUCs sets the demand to PROCESS -> routed to fallback.
  Validate: a process-only test extension runs in a fallback backend; the same
  extension marked threaded runs on a carrier.
- (e) Crash isolation: validate a crash in a fallback backend is a normal
  single-backend crash, not a whole-server fail-stop.

## Why this is its own series, not one commit

Sub-step (a) alone touches ~6 files across the shmem/sema/guc reattach
subsystem and is the highest-blast-radius area in PostgreSQL (a mistake breaks
ALL backend startup, process mode included).  Each sub-step must keep process
mode green and be validated on a real pooled threaded server.  Increment 2 is
therefore sequenced as (a)->(e) above, each landing only when its validation is
green.
