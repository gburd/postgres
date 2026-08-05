# Metal A/B run #4 (2026-08-04) -- THE beat-fork blocker isolated: pooled-scheduler per-transaction dispatch, NOT io_method

Box: m8idn.metal-96xl i-0b66934547e55a7ee, us-east-1, key xtc-ab4. origin/xtc
5827568af40 (has the preadv2 cached-read fast path). libxtc v1.32.0, RELEASE.
Matrix: fork(sync) vs mt(io_method=sync) vs mt(io_method=xtc + fast path), VU
192 & 384, srv_tpm = pg_stat_database xact_commit/min (monitor-independent).
TERMINATED + verified clean 5 regions.

## Result table (srv_tpm = committed txns/min)
  lane            vu    srv_tpm       % of fork
  fork            192   2,367,218     100%
  mt io=sync      192      35,948     1.5%
  mt io=xtc(fast) 192      22,155     0.9%
  fork            384   2,353,326     100%
  mt io=sync      384           7     ~0%
  mt io=xtc(fast) 384          75     ~0%

## THE FINDING: it is NOT io_method, it is the per-transaction dispatch path
Both io_method=sync AND io_method=xtc collapse mt to ~1% of fork under HammerDB
OLTP.  So io_method is NOT the blocker (the run #3 conclusion was incomplete).
The preadv2 cached-read fast path (commit 5827568af40) is still a correct
micro-opt for cold-read analytics, but it does not touch this blocker.

## The DECISIVE contrast: pgbench is HEALTHY, HammerDB is not
Same mt-xtc(fast-path) server, same box:
  - pgbench -S -c384 (persistent conns, back-to-back selects): 929,061 tps,
    CLEAN profile (LWLockAcquire 4.72%, PinBuffer 3.70%, _bt_compare 3.11%),
    ZERO futex, no carrier_entry storm.  mt is at parity here.
  - HammerDB TPROC-C @192: ~1% of fork, live perf = 36%+ __x64_sys_futex, top
    callers backend_pooled_protocol_carrier_entry, PgRuntimeProtocolSchedulerPop
    Runnable, xtc_counter_add, and glibc pthread_mutex_lock/__condvar_cancel_
    waiting/__futex_abstimed_wait (INSIDE xtc_amutex/xtc_notify -- the F2
    primitives are pthread/futex-backed).
Difference: pgbench holds a session leased on ONE carrier and loops queries with
no re-dispatch.  HammerDB drives many SHORT stored-proc transactions and cycles
each session through the pooled scheduler's enqueue -> notify -> wait -> lease
-> park -> wake path PER TRANSACTION.  At 192-384 sessions x thousands of txns/s
that per-transaction re-dispatch is a futex storm.  Fork has none of this (each
backend just loops on its own socket).

## ROOT CAUSE (precise)
The pooled protocol scheduler re-dispatches a session through the carrier queue
(lock + notify + wait + lease) and the eventfd park/wake on EVERY transaction
boundary.  The synchronization is glibc-pthread/futex-backed (xtc_amutex/
xtc_notify wrap pthread mutex/condvar), and the per-transaction frequency makes
the futex traffic dominate.  This is ARCHITECTURAL, not a primitive swap or an
io_method choice.

## FIX DIRECTIONS (design-first; each its own increment, A/B-gated)
1. STICKY SESSION AFFINITY: keep a session leased on its carrier across
   transaction boundaries (like pgbench's persistent-conn path that hits 929k
   tps) instead of re-queuing per transaction.  Re-dispatch only when the
   session actually parks on a slow wait, not on every commit.  This is the
   highest-leverage change -- it makes the OLTP path behave like the healthy
   pgbench path.
2. WAKELESS FAST PATH: when a carrier has a runnable session ready, resume it
   WITHOUT going through the queue-notify/eventfd (no futex) -- only use the
   notify/park when the carrier would genuinely idle.
3. Reduce F1 counter cost on the hot path (xtc_counter_add ~1.5%): per-carrier
   counters summed on read, off the dispatch path.
4. The preadv2 fast path (landed) stays for cold-read analytics; re-measure it
   on an IO-bound scan workload where it should help.

## Harness upgrades PROVEN this run (keep)
- srv_tpm (monitor-independent) validated (fork srv_tpm 2.37M vs HammerDB TPM
  2.51M, ~6%).
- io_method sweep (sync/xtc) on the mt lane.
- pooled-in-effect assert + stop escalation held all run.

Artifacts: .ec2/ab4-20260804-metal/ (hresults.tsv, ab4_hdb_mt_futex.txt live
HammerDB-mt storm profile, ab4_sel384_clean.txt healthy pgbench profile,
ab4_run.sh, STATUS).
