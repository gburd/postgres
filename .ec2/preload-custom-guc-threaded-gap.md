# FINDING: shared_preload_libraries custom GUCs don't reach per-session GUC state under multithreaded=on (2026-08-25)

## Symptom (measured, chiuso c7i.4xlarge, origin/xtc b9e75754be = v1.37 build)
shared_preload_libraries='pg_stat_statements', compute_query_id=on:
  multithreaded=off: SHOW pg_stat_statements.track -> 'top'  (preload OK, GUC registered)
  multithreaded=on:  SHOW pg_stat_statements.track -> ERROR: unrecognized configuration
                     parameter "pg_stat_statements.track"  (GUC NOT visible to the session)
Reproduced cleanly A/B (same build, only multithreaded differs).  0 crashes, no
error at startup -- the module IS dlopen'd (postmaster.c:1097
process_shared_preload_libraries + the multithreaded check_loaded_modules_backend_model
at :1113), and its _PG_init runs in the postmaster and calls DefineCustom*.

## Root cause (analysis, needs confirmation)
Custom GUCs and placeholders live in a PER-SESSION guc_hashtab
(guc.c:100 `#define guc_hashtab (*PgCurrentGUCHashTableRef())`, and the comment at
guc.c:826 "Custom GUCs and placeholders remain per-session in guc_hashtab").
Built-in GUCs use the shared immutable ConfigureNames[] table; custom GUCs do NOT.
The preload module's _PG_init runs ONCE in the postmaster and registers its custom
GUCs into the POSTMASTER's guc_hashtab.  In process mode a forked backend inherits
that hashtab via fork().  In threaded mode each pooled session builds a FRESH
per-session guc_hashtab that does not include the postmaster's preload-registered
custom GUCs -> the session cannot see pg_stat_statements.track etc.

## Impact
Every shared_preload_libraries module's CUSTOM GUCs are invisible per-session under
multithreaded=on.  The module's hooks (set once in the postmaster, in the shared
address space) DO fire, but its GUCs can't be read/SET by sessions.  pg_stat_statements
specifically errors "must be loaded via shared_preload_libraries" from a session even
though it IS loaded in the process -- because the session's GUC/state view is missing.

## Scope / not-yet-affected
- postgres_fdw: NOT preloaded (loads on first use / CREATE EXTENSION), its _PG_init
  runs in the session, so its GUCs register in the session's hashtab -> works
  (validated: loopback FDW scan=100, pushdown=10 under mt=on).
- auto_explain: works via LOAD (session-time _PG_init), validated earlier.
- The gap is specifically PRELOADED custom GUCs.

## Fix direction (core runtime, NOT a contrib change) -- design-first, do carefully
Options: (a) when building a session guc_hashtab, seed it from the postmaster's
custom-GUC registrations (a shared "preload custom GUC registry" the postmaster
populates and sessions copy, analogous to the builtin ConfigureNames[] sharing);
(b) re-run the custom-GUC registration part of preloaded modules' registration per
session (invasive, re-entrancy concerns); (c) make the custom-GUC hashtab entries
for preload-registered GUCs shared (with per-session VALUE storage via the existing
PgSessionEnsureExtensionPrivateState pattern the extensions already use).  (a)/(c)
look right; needs an independent design pass + the per-session-value interplay with
the confirmed custom-GUC design (CUSTOM_GUC_FIX_DESIGN.md).

## Consequence for Phase 16 markers
- postgres_fdw AFFINE: LANDS (branch phase16-postgres-fdw, validated).
- pg_stat_statements AFFINE: marker is correct + module is per-session-converted, but
  it cannot be VALIDATED until this preload gap is fixed (a preloaded pg_stat_statements
  session can't see its own GUCs).  HOLD the marker on branch phase16-contrib-tier2
  until the preload gap is resolved.
