# P3 apples-to-apples matrix — status (2026-08-28, mala)

Config (both lanes identical, per plan): c6id.8xlarge 32 vCPU/61GB, PGDATA on NVMe xfs,
shared_buffers=53769MB (85% RAM), autovacuum/fsync/synchronous_commit/full_page_writes
on, huge_pages on, io_method=sync.  fork=mt off; xtc=mt on, pooled_protocol_carriers=-1
(auto=32).  HammerDB TPROC-C 200wh, driver on a SEPARATE loadgen.  libxtc v1.39 +
xtc_loop_wake nudge (cd305a23e6) + P1 harness (mtpg_matrix.sh).

## Result so far
- fork VU=32: HammerDB NOPM = 864,892 (monitor captured cleanly).
- xtc  VU=32: server WEDGED under the sustained HammerDB write load -- `select 1` hangs
  3x in a row, CPU pinned ~44% (I/O-wait, not progressing).  The lane did not complete.

## Key finding: the xtc_loop_wake fix is NECESSARY but NOT SUFFICIENT for heavy write load
- The light repro (pg_stat_database x10 under 8 pgbench writers) is FIXED at carriers=32
  (ok=10/hung=0) by the wake nudge.
- But the FULL HammerDB TPROC-C write workload (200wh, VU=32, stored-proc neword/payment,
  many concurrent sessions) STILL wedges the pooled scheduler.  So there is a residual
  stall beyond the single producer-nudge fix -- it only manifests under sustained
  many-session write pressure, not the light case.
- HammerDB's monitor now works for FORK (864,892 NOPM captured), confirming the harness +
  the metric are sound; the xtc side can't be measured because it wedges first.

## Interpretation
Two effects compound on the write path:
1. Residual wake/scheduling stall under heavy write concurrency (deeper than the single
   xtc_pg_pooled_queue_signal nudge -- possibly other cross-thread MarkRunnable sites, or
   the F2-layer pooled-queue lock/notify contention, need the same nudge discipline /
   dedup).
2. The B2 write-path carrier-feeding gap.
This is the real remaining work to hit the "outright win on write-heavy" bar; the wake
fix moved us from "hangs immediately / near-zero" toward "works under light load", but
heavy sustained write load still wedges.

## Next (per plan, B1-residual + B2)
- Instrument the heavy-write wedge with gdb (all-carrier bt) to name whether it is (a)
  another un-nudged cross-thread MarkRunnable site, (b) the pooled-queue lock/notify
  serializer, or (c) a genuine libxtc gap under this pattern -- then fix the named cause.
- Harness nit: the server-side srv_nopm xcheck sampler mis-times (reads 0); HammerDB's
  own NOPM is authoritative now (monitor works post-fix) -- rely on it, drop/repair the
  xcheck.
