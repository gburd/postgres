# Preload custom-GUC accessor API — validated + landed (2026-08-26)

Branch preload-custom-guc. Fixes the confirmed threaded gap: shared_preload_libraries
custom GUCs were invisible per-session under multithreaded=on.

## What landed
Core (guc.c/guc_tables.h/guc.h/miscinit.c):
 - RegisterCustomGUCSessionAccessor{,Int,Real,String,Enum}(name, accessor) extension API.
 - SnapshotPreloadCustomGUCs() (postmaster, end of process_shared_preload_libraries):
   shared registry; FAIL-CLOSED FATAL for a session-writable (context>=PGC_SU_BACKEND)
   custom GUC with no accessor.
 - SeedPreloadCustomGUCs() (per session, in InitializeThreadedSessionGUCOptions):
   fresh per-session config_generic+state; accessor-arm rebinds GUC_VARIABLE_<T>=accessor()
   + InitializeOneGUCOption; no-accessor-arm carries the frozen process-wide pointer
   forward (var->state->variable = tmpl->state->variable) + reset-metadata only.
 - pg_stat_statements registers accessors for track/track_utility/track_planning.

## Two-review gate + fixes
Adversarial implementation review (BLOCK) caught: F1 NULL-deref crash on runtime-scoped
no-accessor customs (SHOW pgss.max / pg_settings -> whole-process fail-stop), F2
fail-closed hole (PGC_BACKEND/PGC_SU_BACKEND escaped), F3 accessor cell not booted.
All three fixed (commit 61a490f69e); items 1/2/3(session-live)/5/6/7 confirmed clean.

## Validation (chiuso c7i.4xlarge, cassert)
- process regress 245/245, 0 diffs (byte-for-byte).
- preloaded pgss under mt=on: pgss.track='top' (was 'unrecognized parameter'!),
  pgss.max=5000, pgss.save=on, pg_settings full scan works, 5 pgss rows in pg_settings
  -- the F1 crash is GONE.
- per-session track isolation PASS (A set none, B set all, C default top).
- server alive after all, 0 crashes.
- OLD (pre-fix) build reproduced the F1 crash exactly (SHOW pgss.max -> connection lost),
  confirming the review + the fix target the same thing.

## DEFERRED (separate pgss-shmem-under-threading gap, NOT this fix)
pg_stat_statements' AFFINE marker is HELD: its VIEW doesn't work under mt=on
('SELECT * FROM pg_stat_statements' -> 'must be loaded via shared_preload_libraries';
mt=off works fine on the same build).  Root: pgss's shmem pointers
(pgss/pgss_hash = pgss_runtime_state()->shared_state/hash, set by its shmem_startup
callback via RegisterShmemCallbacks) are not visible to the querying session under
threading.  thread_runtime is process-wide, so the runtime-state accessor should share
-- needs investigation of the pgss shmem_startup/RegisterShmemCallbacks threaded path.
The GUC accessor infrastructure (the deliverable) is correct and lands regardless.
