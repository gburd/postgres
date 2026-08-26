# pg_stat_statements fully threaded-safe + AFFINE — validated + landed (2026-08-26)

Fix (a): moved pgss/pgss_hash (process-wide shmem singletons) from
pgss_runtime_state() back to plain PG_GLOBAL_SHMEM statics, so the pointers set by
pgss_shmem_startup (which runs under the early-fallback runtime) are visible to every
carrier session (shared address space) -- fixing the view "must be loaded via
shared_preload_libraries" error under mt=on.  Re-marked AFFINE.

Validated (chiuso c7i.4xlarge, cassert):
- process regress 245/245 0-diffs (byte-for-byte); pg_stat_statements own regression PASS.
- preloaded pgss under mt=on: view rows>0 (was the error!), a tracked query shows
  calls>=1, pg_stat_statements_reset() works, track=top/max=5000, per-session track
  isolation PASS (A none / B all), server alive, 0 crashes.

pg_stat_statements is now fully functional under multithreaded=on: session GUCs
per-session (via the preload custom-GUC accessor API) + the stats view via process-wide
shmem pointers.  The general extension-shmem-under-threading gap (fix (c): core adopts
early extension-module state into the threaded runtime) remains a documented follow-up
for any future extension whose shmem_startup stashes state in
PgRuntimeEnsureExtensionPrivateState; pgss was the only exposed contrib case and uses
the small local fix (a).
