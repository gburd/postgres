# xtc_loop_wake nudge (libxtc v1.39) — validation (2026-08-28, mala)

libxtc v1.38->v1.39 (adds public xtc_loop_wake, the lost-wake-free any-thread loop
nudge).  PG fix: xtc_pg_pooled_queue_signal nudges every carrier loop AFTER the
enqueue, per the libxtc producer-must-nudge contract (their reply confirmed our
option B is the whole fix).

Repro: threaded pooled server + 8 pgbench TPC-B write workers, then the query that
hung before (`select sum(xact_commit+xact_rollback) from pg_stat_database`) x10 @ 3s,
20s timeout each.
- carriers=8 (artificially starved: 8 carriers vs 8 busy workers + monitor):
  ok=6 hung=4 -- the nudge lets it SELF-RECOVER (was hung=10 before the fix) but the
  monitor still starves for a carrier when the pool == worker count (that is the B2
  write-path carrier-provisioning gap, not the wake miss).
- carriers=-1 auto=32 (PRODUCTION config, cores):  ok=10 hung=0, every query 6-7ms
  under sustained write load.  The lost-wakeup stall is CLOSED at correct provisioning.

Conclusion: the write-load stall + HammerDB monitor-VU hang was (1) our missing
producer nudge [fixed here with xtc_loop_wake] and (2) carrier under-provisioning when
pooled_protocol_carriers is set below core count [use the -1 auto default].  At
auto=cores the monitor path no longer hangs -> threaded TPROC-C is now measurable.
