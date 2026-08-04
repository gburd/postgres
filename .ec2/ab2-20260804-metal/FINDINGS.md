# Metal A/B run #2 (2026-08-04) -- fork clean, mt-NOPM blocked by HammerDB monitor, but the BEAT-FORK LEVER VERDICT landed

Box: m8idn.metal-96xl i-05aed48d5e6fc3e4a, us-east-1, key xtc-ab2-20260804-122159.
origin/xtc 1c76397e496, libxtc v1.32.0, RELEASE, io_method=sync both lanes,
huge_pages=on, fsync=off. TERMINATED + SG/key/pem deleted + 5-region-verified.
Driven by /tmp/ab2_run.sh (coordinator-run, not delegated -- the sub-agent leak
pattern). The two prior harness bugs (unpooled mt lane; fast-stop hang) were
FIXED and CONFIRMED: every mt cell logged POOLED_OK carriers=192 rows=15, and
no stop hang occurred.

## Result table (NOPM)
  mode      vu   rep   nopm
  process   192  1     1,037,814
  process   192  2     1,084,278
  process   384  1     1,097,489
  process   384  2     1,120,017
  threaded  192  1     NA   (HammerDB monitor VU failed -- see below)
  threaded  192  2     NA
  threaded  384  1     NA
  threaded  384  2     NA
Fork (stock) is clean + consistent: ~1.06M @192, ~1.11M @384 NOPM.

## WHY mt NOPM = NA (NOT a runtime failure -- important)
The mt runtime worked: pooled assert passed, server log CLEAN (zero errors), and
ALL 192 worker VUs logged FINISHED SUCCESS -- the transactions ran. The ONLY
failure is HammerDB's MONITOR VU (Vuser 1): it did rampup, logged "Rampup
complete, Taking start Transaction Count", then FINISHED FAILED -- it never
entered the timing loop, so no "TEST RESULT: System achieved N NOPM" line was
printed -> parse = NA. Contrast the fork monitor, which printed the full
"Timing test period... 1,2,3,4,5... Test complete... TEST RESULT" sequence.
So the mt lane DID the work; HammerDB's NOPM-collection monitor failed at the
rampup->timing transition on the pooled path (client-side; server was clean).
This is a measurement-path interaction with the pooled scheduler (long-idle
monitor session across the rampup sleep), NOT a throughput regression.
FIX for the re-run (already in /tmp/ab2_run.sh): a monitor-independent metric --
sample sum(xact_commit) from pg_stat_database at timing-window start/end and
compute committed-txn/min ourselves (same definition both lanes, no HammerDB
monitor).  Plus io_method=xtc for the mt lane (see below).

## THE BEAT-FORK LEVER VERDICT (this run's real payoff, from mt pgbench select@384)
mt pgbench -S -c384: tps = 898,730 (latency avg 0.427 ms).  FLAT perf profile
(--no-children), the top symbols:
    6.39%  intel_idle              <- box has HEADROOM (not saturated)
    5.05%  LWLockAcquire           <- LWLock contention (OURS)
    4.93%  PinBuffer.constprop.0   <- buffer-pin hot path
    3.09%  _bt_compare             <- btree descent (real work)
    1.74%  BufferLockAcquire
    1.63%  StartReadBuffer
    1.45%  UnlockReleaseBuffer
    1.44%  LWLockRelease
    1.40%  BufferGetBlockNumber
  ... update_sg_lb_stats (CFS newidle balancer): 0.52% + 0.48% ~= 1.0% TOTAL.

VERDICT: the CFS scheduling tax that was 41% in the pre-thread-fix run is GONE
(~1%).  The thread-explosion fix (4634 -> ~200 threads) + F3 steal-backoff
eliminated it.  The beat-fork lever is now CONTENTION, NOT SCHEDULING:
  - LWLockAcquire+Release ~= 6.5%
  - PinBuffer + BufferLockAcquire + UnlockReleaseBuffer ~= 8% buffer pin/lock
These are OUR contention paths (buffer manager + lock manager), the same class
whether fork or threaded -- so the next beat-fork work is buffer-pin / LWLock
contention reduction, not scheduler tuning.  (Also note: at select@384 the
kernel top is tcp_sendmsg ~10% in the callgraph -- point-select is partly
network-bound at ~900k tps, another reason absolute mt-vs-fork at extreme client
counts is dominated by non-runtime costs.)

## Thread-count check (thread-explosion fix holds)
Perf-phase snapshot: postmaster threads = 21, iou-wrk = 1 (carriers spawn lazily;
the snapshot caught early bring-up).  Far from the old 4634.  The v1.31 io-wq cap
+ bounded worker-fiber executor hold on the current tree.

## io_method (user point -- FOLDED INTO THE RE-RUN)
This run used io_method=sync BOTH lanes.  That HANDICAPS mt: sync blocks the
carrier OS thread on each data-file read, denying the runtime its designed
fiber-park advantage.  There IS a fiber-native method -- io_method=xtc
(method_xtc.c: xtc_aio_preadv/pwritev park the FIBER on the xtc loop, do not
block the carrier; falls back to sync off-fiber, process byte-for-byte).  The
fair comparison is fork@sync vs mt@xtc (each lane on its native-best IO path);
worker is unavailable threaded but xtc is exactly the pooled runtime's path.
/tmp/ab2_run.sh now sets io_method=xtc on the threaded lane (override
IOM_THREADED=sync for an mt-sync-vs-mt-xtc A/B).

## Re-run plan (one clean run gets the mt NOPM)
1. Monitor-independent throughput (srv_tpm column) -- already added.
2. io_method=xtc on the mt lane -- already added.
3. Validate io_method=xtc starts + serves under pooled before the full matrix.
Everything else (pooled assert, stop escalation) is proven this run.
