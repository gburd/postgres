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

Design decision (refined during Increment 2(a), 2026-07-12): do NOT force a
shmem type at startup, and do NOT change any default.  The default threaded
build keeps `shared_memory_type=mmap`.  Instead, the fallback ROUTE (sub-step c)
checks `shared_memory_type` at the moment a session would be routed to a
fork+exec fallback: if the segment is re-attachable (sysv), route to the
fallback; if it is anonymous mmap, keep the current fail-closed `ERROR`
(Increment 1's message) with an added hint to set `shared_memory_type=sysv` to
enable the process-fallback.  This is smaller, changes no defaults, needs no
global sysv-vs-mmap perf re-validation, and puts the constraint exactly where it
bites.  Forcing sysv globally under multithreaded=on was rejected as
too broad (it would change shmem behaviour for every threaded server, including
those that never use a fallback).  Confirmed the default is
`DEFAULT_SHARED_MEMORY_TYPE = SHMEM_TYPE_MMAP` on non-EXEC_BACKEND Linux
(pg_shmem.h).

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
- (a) [DONE, commit 9e567171c64] Add `USE_XTC_PROCESS_FALLBACK` +
  `FORKEXEC_BACKEND` and widen the compile guards so the machinery COMPILES on
  Linux, with NO runtime path change.  Validated: full build links clean; both
  multithreaded=off and =on start and run identically; fork+exec functions exist
  but are never called.
- (b) [folded into (c)] shmem re-attachability is checked at the fallback ROUTE,
  not forced at startup (see the design decision above): default stays mmap; the
  route requires sysv and otherwise keeps the fail-closed ERROR with a hint.
- (c) Add `PG_BACKEND_LAUNCH_PROCESS_FALLBACK` + the fork+exec launch route,
  gated behind a **forced test knob** (a session/developer GUC, e.g.
  `xtc_force_process_fallback`) so a session can be deterministically routed to
  a process backend regardless of extension needs.  The route checks
  `shared_memory_type`: sysv -> fork+exec fallback; mmap -> fail-closed ERROR +
  "set shared_memory_type=sysv" hint.  Validate on EC2: a normal
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

## Increment 2(c) progress + remaining exec'd-child restore gaps (2026-07-13)

Committed dbae3d31965.  The fork+exec route works and the exec'd child boots
deep into startup.  Resolved, in order, by EC2-validated iteration:

1. postgres_exec_path def + assignment (2(a) closure, f996910054a).
2. instrument.c timing-init assert (child inits TSC via restore_backend_variables).
3. shmem re-attach (AttachSharedMemoryStructs) + fastpath-lock assert -- gated on
   the new PG_BACKEND_WAS_FORKEXECED flag so ONLY the exec'd child re-attaches
   (forked aux procs inherit and must not).
4. shmem.c InitShmemAllocator !IsUnderPostmaster assert relaxed for the child.
5. io_method worker->xtc remap re-applied in SubPostmasterMain (the PGC_S_OVERRIDE
   remap does not survive serialization; child restored worker and looked up the
   AioWorkerSubmissionQueue the xtc parent never allocated).

REMAINING (next session), the child now dies at:
  FATAL: could not open file "(null)" / could not load (null)
in process_shared_preload_libraries() (SubPostmasterMain).  With an EMPTY
shared_preload_libraries, load_libraries() should early-return; a non-empty
"(null)" means a PGC_POSTMASTER preload GUC string (shared_preload_libraries_
string and/or the local/session preload lists) is dangling/garbage in the exec'd
child -- a GUC-string restore gap in read_nondefault_variables /
restore_backend_variables for string GUCs.  Root-cause which string is not
restored (likely a *_preload_libraries char* whose value pointer isn't
serialized), fix the restore, and continue iterating (expect a few more
exec'd-child state-restore gaps after this one).  Each fix must keep the
PG_BACKEND_WAS_FORKEXECED discipline: only the exec'd child re-derives; forked
children inherit.

Definition of done for 2(c): with xtc_force_process_fallback=on + sysv, a client
connection runs a query in a fork+exec'd process backend; carriers still serve
un-forced sessions; process mode and threaded-fallback-off remain byte-identical.

## Increment 2(c) COMPLETE (2026-07-13, commit 1a12a49ab81)

The fork+exec process-fallback route runs queries end-to-end.  Final two
exec'd-child gaps resolved after the io_method remap:

6. hba_file / ident_file were NULL in the child (set as PGC_S_OVERRIDE inside
   SelectConfigFiles, which the child skips; they do not survive GUC
   serialization -- unlike config_file/data_directory, which are carried via
   read_backend_variables).  Re-derived from configdir in SubPostmasterMain.
7. DSM control segment: dsm_backend_startup's attach was #ifdef EXEC_BACKEND
   only -> dsm_control NULL -> SIGSEGV in dsm_attach/pgstat(DSA) at auth.
   Widened to FORKEXEC_BACKEND + gated on PG_BACKEND_WAS_FORKEXECED.

Validated (EC2, cassert, sysv, force_fallback=on): SELECT + table round-trip in
a fork+exec'd process backend.  Safety matrix green: process / threaded-fallback
-off / threaded-fallback-on all OK.

All the state-restore gating follows one discipline: only the exec'd child
(PG_BACKEND_WAS_FORKEXECED) re-derives; normally-forked children inherit.  Under
EXEC_BACKEND the macro is a constant true, so upstream behaviour is byte-for-byte
unchanged.

REMAINING for Phase 19:
- Increment 3: real per-session detection -- set the session demand to PROCESS
  when a session needs a process-only extension (preload GUCs first, catalog
  later), so ONLY those sessions take the fallback; drop the force knob to a
  test-only aid.
- Increment 2(e): crash isolation -- prove a crash in a process-fallback backend
  is a normal single-backend crash, not a whole-server fail-stop (it should be,
  since it does not share the carrier address space -- needs a TAP guard).
- The route still requires shared_memory_type=sysv; document it and, for
  Increment 3, verify the demand-driven routing refuses cleanly (not crash) when
  a process-only extension is needed but shmem is mmap.

## Increment 3 done; 2(e) blocked by a config-application bug (2026-07-13)

Increment 3 (23e99a9b159): process-only shared_preload_libraries are now
rejected at startup under multithreaded=on (validated: multithreaded=off loads
it; multithreaded=on refuses with the Increment-1 message).  Also landed:
io workers are never started under multithreaded=on (f49e0dcf5bd).

Increment 2(e) (013 crash-isolation guard) is written and committed but SKIPS,
because of a newly-found config-application bug that blocks forcing the route:

  BUG: under multithreaded=on, `xtc_force_process_fallback = on` set in
  postgresql.conf does NOT take effect (SHOW returns off, pg_settings source =
  default), and it also reverts `shared_memory_type = sysv` in the same file
  back to mmap.  Isolated on EC2 (m6id.8xlarge, cassert, v1.20.1):
    [sysv_only]  shared_memory_type=sysv                      -> shm=sysv        (ok)
    [mt_only]    multithreaded=on                             -> mt=on           (ok)
    [mt_sysv]    multithreaded=on + sysv                      -> shm=sysv mt=on  (ok)
    [all3]       multithreaded=on + sysv + fallback=on        -> shm=mmap fb=off  (BUG)
    [solo]       xtc_force_process_fallback=on (only)         -> fb=on            (ok)
  So the GUC works alone (fb=on, source=configuration file) and sysv works with
  multithreaded; only the THREE together revert.  The postmaster then launches
  backends as carrier fibers (log: "spawned backend fiber"), not fork+exec, so
  013 cannot exercise crash isolation.

  Lead: the fallback GUC + sysv are set in the config block AFTER `multithreaded`;
  something in the multithreaded startup path (io_method remap / pooled-carrier
  resolution / a config re-read) appears to snapshot or re-apply GUCs such that
  config-file values after `multithreaded` are dropped or reverted to default in
  the [all3] combination.  The Increment 2(c) fork+exec route itself was
  validated working earlier this session via a direct `postgres -D &` run; this
  is specifically a config-APPLICATION regression in the multithreaded+sysv+
  fallback combination, not the route logic.  NEXT: trace where these
  PGC_POSTMASTER values are lost after `multithreaded` is finalized (likely
  around the io_method SetConfigOption(PGC_S_OVERRIDE) block or a
  ProcessConfigFile re-read), fix it, then 013 runs the isolation assertions.

The route + all state-restore fixes remain committed, default-off, and
process-mode / normal-threaded-mode safe.

## Two precise follow-ups isolated (2026-07-13, EC2 m6id.8xlarge)

### Bug A: config line ORDER -- GUCs after `multithreaded` in the file are dropped
  [mt_fb]    multithreaded=on THEN xtc_force_process_fallback=on  -> server did not
             even come up (fb empty)
  [fb_first] xtc_force_process_fallback=on + sysv THEN multithreaded=on -> fb=on
             shm=sysv, route ENGAGES (no "spawned backend fiber")
So putting the fallback/sysv GUCs BEFORE `multithreaded` in postgresql.conf is a
working order; the bug is that `multithreaded`'s processing (or the io_method
PGC_S_OVERRIDE remap / pooled resolution it triggers during config read) drops or
reverts config-file GUC lines that come AFTER it.  Fix: find why subsequent
config-file lines are lost once `multithreaded` is assigned/finalized.  013 can be
made to pass sooner by ordering its append_conf with fallback+sysv before
multithreaded, but the underlying order-sensitivity should be fixed.

### Bug B (the real Increment 2(e) gap): process-fallback crash still FAIL-STOPS
With the route engaged (fb=on, fork+exec backend), crashing that backend still
logs "terminating threaded server runtime after child crash" and brings the whole
server down (server_alive_after_crash=0).  So a process-fallback backend -- though
a real isolated process that does NOT share the carrier address space -- is
currently treated by the crash handler exactly like a carrier-fiber crash.
Fix for 2(e): in the postmaster crash path (postmaster.c ~1861/1922 and
HandleChildCrash), distinguish a process-fallback B_BACKEND (a real reaped
process) from a carrier-fiber backend, and give the former normal single-backend
crash handling (reap + optional restart_after_crash) instead of the whole-server
fail-stop.  Only THEN does the isolation contract 013 pins actually hold.

Both are narrow, well-characterized, and independent of the (working) route +
state-restore logic.  Nothing shipped is broken: the route is default-off and
process / normal-threaded modes are unaffected.

## Bug B REFRAMED: process-fallback crash SHOULD fail-stop (2026-07-13)

On closer analysis Bug B was mis-stated.  A process-fallback backend is isolated
at the ADDRESS-SPACE level (its crash cannot scribble on carrier fibers' stacks)
-- but NOT at the SHARED-MEMORY level.  Like ANY backend (process mode included),
a crashing backend may leave shared buffers/locks inconsistent.  Process mode
handles that by SIGQUIT'ing all backends and reinitializing shared memory
(HandleChildCrash -> HandleFatalError(PMQUIT_FOR_CRASH)).  Under multithreaded=on
the postmaster cannot safely SIGQUIT+reinit while carrier threads run inside its
own process, so the correct policy is the SAME fail-stop as a carrier crash.

So the current behavior (process-fallback crash -> "terminating threaded server
runtime after child crash" -> fail-stop) is CORRECT, not a bug.  013 was rewritten
to pin that contract (fail-stop + committed data survives + external restart
recovers), mirroring 010 for carrier crashes.  The address-space isolation of the
fallback process remains valuable (unsafe extension containment) but does not
change the shared-memory crash-recovery policy.

Net: Increment 2(e) is "fail-stop, like all backend crashes under multithreaded",
guarded by 013.  Only Bug A (config-order: GUC lines after `multithreaded` in the
file are dropped) remains an open correctness nuisance -- workaround is to place
fallback/sysv BEFORE multithreaded in postgresql.conf (013 does this).  Bug A root
cause not yet found (not in the ThreadedGUCLock mutex nor the IsUnderPostmaster
backend-replay paths; those don't run in the postmaster during SelectConfigFiles).
