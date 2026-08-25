# Phase 16 audit summary — contrib, PL/Python, GUC/hooks (2026-08-25)

Three read-only audits run by agents to produce a scoped, ranked worklist for
"Bundled Extension Completion And Hardening" (Gate E2-Extensions).  Detailed GUC
reports are in this directory (GUC_THREADING_AUDIT.md / _QUICKREF / _INDEX).

## Backend-model marker mechanism (confirmed)
- Defined in `src/include/fmgr.h`: `PG_MODULE_MAGIC_EXT(..., PG_MODULE_MAGIC_BACKEND_MODEL_*)`.
- Models: PROCESS, THREAD_PER_SESSION, POOLED_SCHEDULER, POOLED_PROTOCOL_AFFINE,
  POOLED_PROTOCOL_MIGRATABLE, TASK_REENTRANT.
- Default = PROCESS (fail-closed).  Gate: `check_module_backend_model()` in
  `src/backend/utils/fmgr/dfmgr.c:593` at dlopen time.

## 1) contrib (55 extensions)
Already marked: ~17 AFFINE + ~9 THREAD_PER_SESSION.  Remaining worklist:
- **Tier 1 (16 quick wins, marker-only, verified safe):** auto_explain, dblink,
  postgres_fdw, pg_stat_statements, uuid-ossp, pgcrypto (OpenSSL init state),
  pg_buffercache, sslinfo, file_fdw, xml2, lo, test_decoding, pg_logicalinspect,
  pg_freespacemap, pg_visibility, sepgsql.  (Most already use
  PgRuntimeEnsureExtensionPrivateState / PgSessionEnsureExtensionPrivateState.)
- **Tier 2 (minor audit):** bloom, amcheck, pg_overexplain, pgrowlocks, intagg.
- **Tier 3 (Python bridges -> PROCESS):** hstore_plpython, ltree_plpython,
  jsonb_plpython (must match host PL/Python model -- see below).
- **Tier 4 (Perl bridges -> inherit AFFINE):** bool_plperl, hstore_plperl,
  jsonb_plperl.
- **Tier 5 (defer):** tcn.
NOTE: verify each Tier-1 marking with a threaded load+exec test; the agent's
"verified safe" is a source audit, not a run.  Two of the Tier-1 (pgcrypto
OpenSSL, uuid-ossp) touch C-library init state -- re-check those first.

## 2) PL/Python (currently PROCESS-only)
Recommendation: **Option C -- single embedded interpreter + per-session
PyThreadState re-stamp on entry, matching plperl's activate_interpreter()**.
- Move PLy_execution_contexts + explicit_subtransactions to per-session buckets;
  add activate_plpython_interpreter() (PyThreadState_Swap) at the 3 entry points.
- The GIL stays process-global -> no fiber parallelism gain, but sessions
  interleave correctly (cooperative fibers don't preempt mid-bytecode); this is
  the same model plperl uses and unlocks pooled-affine instead of forced PROCESS.
- Sub-interpreters/PEP-684 per-interpreter GIL (Option B) needs Python 3.12+ and
  is a later hardening, not Phase 16.
- Guard: revert to PROCESS if any refcount corruption shows under the interleave
  TAP.  This is real implementation work (a single-owner task), not a marker.

## 3) GUC + extension hooks
- **Core GUC storage: per-backend-safe** already (per-session buckets +
  ThreadedGUCLock/xtc_amutex seam).  READY.
- **CRITICAL BUG -- custom-GUC valueAddr is shared across sessions:**
  DefineCustomInt/Bool/Real/EnumVariable store the extension's single global
  `valueAddr`; under threading all sessions on all carriers write the SAME
  memory, so two sessions SET-ing a custom GUC corrupt each other.  STRING
  already has per-session shadow storage (guc.c:5607-5608) -- the model to copy.
  Unguarded writes: guc.c:3063 (BOOL), 3079 (INT), 3095 (REAL), 3130 (ENUM);
  registration guc.c:6809-6913.  **This is a real correctness bug, blocker for
  Phase 16 completion, and a good standalone fix.**  (Independent-verify the
  claim before implementing -- confirm the STRING shadow path and that
  BOOL/INT/REAL/ENUM truly lack it.)
- **Extension hook pointers (~25: ExecutorStart_hook, ProcessUtility_hook,
  planner_hook, ...):** PG_GLOBAL_RUNTIME, set-once at preload -> safe now;
  only a hazard if an extension is loaded/unloaded mid-session.  Defer-with-
  invariant: guard CREATE EXTENSION mid-session hook mutation.
- assign/check/show hooks run inside ThreadedGUCLock (serialized) -> safe; add
  stress tests.

## Ranked Phase 16 worklist (merged)
1. **Custom-GUC per-session shadow storage (BOOL/INT/REAL/ENUM)** -- CRITICAL
   correctness bug; standalone; STRING is the template.  (independent-verify first)
2. contrib Tier-1 markers (16) -- quick wins; each needs a threaded load+exec check.
3. PL/Python Option C -- real implementation (per-session PyThreadState).
4. contrib Tier-2 audits (5) + Perl bridges (3, inherit AFFINE) + Python bridges
   (3, mark PROCESS).
5. Extension-hook mid-session-load guard (defer-with-invariant).
6. assign/check/show hook stress tests; threaded contrib regression per extension.
7. TSan/ASan where feasible; crash/FATAL tests; perf baselines.

## Caveats on these audits
Agent source-audits, not runs.  Every "safe/verified" is a static read.  Before
landing any marker, run the extension under multithreaded=on with >=2 sessions on
<2 carriers and confirm correct results + isolation.  The custom-GUC bug and the
PL/Python design both need an independent second look before implementing.
