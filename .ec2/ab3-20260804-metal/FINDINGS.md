# Metal A/B run #3 (2026-08-04) -- io_method=xtc FUTEX-STORMS under OLTP; the beat-fork trigger found

Box: m8idn.metal-96xl i-03ae45c0f589a33ea, us-east-1, key xtc-ab3. origin/xtc
158e7e6e283, libxtc v1.32.0, RELEASE. THIS RUN put the mt lane on io_method=xtc
(the fiber-native path) + added a monitor-independent throughput metric.
TERMINATED + SG/key/pem deleted + 5-region-verified clean.

## Two harness upgrades this run PROVED OUT
1. Monitor-independent throughput (srv_tpm = pg_stat_database.xact_commit delta
   over the window): WORKS.  process@192 HammerDB TPM=2,455,403 vs my
   srv_tpm=2,319,077 -- tracks within ~6%, validating the metric.  So we can now
   measure mt throughput even when HammerDB's monitor VU fails.
2. io_method=xtc preflight under pooled: PASSED (server starts, serves) -- the
   xtc AIO path is functionally fine.

## THE HEADLINE FINDING: io_method=xtc collapses mt under concurrent OLTP
  mode      vu   rep   HammerDB_NOPM   srv_tpm(committed/min)
  process   192  1     1,067,612       2,319,077
  process   192  2     1,044,344       2,263,731   <- fork stable ~2.3M
  threaded  192  1     NA              21,136       <- mt = 0.9% of fork  (!!)
The mt lane's 192 HammerDB workers all logged FINISHED SUCCESS and the server
log was CLEAN, yet committed ~1% of fork's transactions.  Live probe: the mt
postgres burned ~14 cores (1444% CPU) but a monitoring psql could not even
connect, and commits/sec ~= 0.

## ROOT CAUSE (perf on the busy mt postgres): a FUTEX STORM in carrier dispatch
36.71% of busy time in __x64_sys_futex (27.93% futex_wait + futex_wake +
run-queue spinlock contention via try_to_wake_up).  The callers are NOT the AIO
path -- they are the CARRIER-DISPATCH machinery:
  backend_pooled_protocol_carrier_entry  2.29%
  xtc_counter_add (F1 counters)          1.47%
  xtc_pg_pooled_queue_lock/_wait (F2)    ~1.7%
  xtc_notify_wait / xtc_amutex_lock (F2) ~1.4%
Interpretation: io_method=xtc PARKS the fiber on EVERY data-file IO.  OLTP does
many tiny (mostly cached) IOs per transaction, so each becomes a fiber
park+resume cycle through the carrier scheduler's wake path (F1 counters + F2
queue notify + carrier re-lease) -> a futex wait/wake per IO -> at 192 fibers x
many IOs/txn, a scheduler-wide futex storm that livelocks throughput.
io_method=SYNC avoids it: a cached pread returns immediately with NO park, NO
futex -- which is why the PRIOR run (sync) hit 98.6% of fork on the SAME OLTP.

## THE TENSION (measured, not assumed)
- io_method=xtc is the "fiber-native" path and is RIGHT when IO genuinely blocks
  (cold reads, slow storage) -- it keeps the carrier busy instead of blocking.
- But under OLTP's many-small-cached-IO pattern, the per-IO park+wake overhead
  (futex round trip) DWARFS the (near-zero) benefit of not blocking on a cached
  pread.  Net: io_method=xtc is currently a catastrophic regression for
  high-concurrency OLTP, and a likely win only for IO-bound scans/cold data.

## DECISION / PATH FORWARD
1. The stock-vs-mt OLTP number should be taken with io_method=SYNC on the mt lane
   (prior run: 98.6% OLTP VU=192, 100% pgbench-select@192, 115.7% update@192).
   The monitor-independent srv_tpm now makes that a clean, HammerDB-monitor-proof
   measurement.  <- this is the "get the number" path, ready now.
2. io_method=xtc needs a "don't park for a hot/cached IO" fast path before it is
   OLTP-viable: only park the fiber when the IO will ACTUALLY block (e.g. try a
   non-blocking/cached completion first, fall back to sync for a buffer already
   in OS cache).  That, or batch the AIO completions so N IOs share one wake
   instead of N wakes.  Until then, keep io_method=sync as the mt OLTP default
   and use xtc for IO-bound analytic workloads only.
3. SEPARATE lever (independent of io_method): the F1 counters (xtc_counter_add
   1.47%) and F2 queue notify appear on the hot dispatch path.  Check whether
   xtc_counter_add takes a lock/atomic that contends at 192 fibers -- if so, a
   per-carrier counter (summed on read) removes it from the hot path.  This helps
   BOTH io_methods.

Artifacts: .ec2/ab3-20260804-metal/ (hresults, ab3_flat.txt perf, futex_storm.txt,
ab3_run.sh with the io_method=xtc lane + srv_tpm metric).
