# ROOT CAUSE: pooled carrier pool under-grows for CPU-bound work (2026-08-24, rebased tree, c7i.metal-48xl/192-core)

## The real problem (NOT a livelock, NOT locks, NOT sockets)
Definitive parity, carriers=192 limit, 192 clients:

  workload            fork        fiber       ratio
  trivial -S c=128    2,088k      1,080k      0.52x   (socket-syscall bound; M:N tax)
  compute  c=128      60,842      5,199       0.085x  (CPU-bound: CATASTROPHIC)
  compute  c=192      53,741      3,923       0.073x

The compute query (generate_series(1,20000) + sqrt/sin per call; ~real CPU, tiny
result) is ~12x SLOWER on fibers.

## Why: the pool grows to ~15 carriers and STOPS, though the limit is 192
Live sampling under 192 CPU-bound clients (pg_stat_xtc_carriers + pg_stat_activity):
  t=3s:  carriers=15  active_backends=5
  t=8s:  carriers=15  active_backends=1
  t=15s: carriers=15  active_backends=1
  tps=4,080
Only 15 carrier OS threads on a 192-core box; pg_stat_activity shows only 1-5
sessions ACTIVE at once -> 192 sessions serialized onto 15 carriers.

## The mechanism (launch_backend.c)
backend_pooled_protocol_maybe_start_carrier_for_work() is the ONLY pool-growth
trigger, called only at logical-start/connection time, and grows by ONE carrier
only when queue_length > idle_carriers.  Once a session is DISPATCHED and
executing a CPU-bound query, it is neither "queued" nor on an "idle" carrier --
it MONOPOLIZES its carrier for the whole query (a pure-CPU query has no
client-read / I/O / lock-wait yield boundary to release the carrier
cooperatively).  So:
  - short/IO-bound queries: a session parks quickly -> a small pool keeps the
    dispatch queue drained -> pool stabilizes small (15) -> fine.
  - CPU-bound queries: each of 15 carriers runs one query to completion; the
    other 177 sessions wait; no NEW work enqueues while they wait, so the
    queue-vs-idle growth signal never fires -> pool never grows toward 192.
The pool is sized by dispatch latency, not by runnable parallelism.  Fork has no
such cap (1 process/1 kernel thread per client, preemptively scheduled across
192 cores), so it scales linearly.

## Fix direction (hot-path scheduler change -> A/B + two-review gate, do NOT rush)
The pool must grow toward min(carrier_limit, ncpus) based on RUNNABLE demand,
not just dispatch-queue backlog.  Options:
  (A) Track BUSY carriers; when a session is about to run and
      (busy_carriers + queue_length) > carrier_count and
      carrier_count < min(limit, ncpus), start another carrier.  Grow on the
      "all carriers busy + more demand" signal, which CPU-bound load DOES produce.
  (B) Preemption: give libxtc's preempt (preempt_int.h) a timeslice so a
      CPU-bound fiber yields its carrier periodically -> more sessions become
      runnable on the existing carriers.  This is the deeper libxtc-native fix
      (cooperative -> preemptive-ish), but affects every fiber and needs its own
      A/B (preempt overhead vs fork).
  (C) The thread-per-session model (pooled_protocol_carriers=0) already sizes the
      executor to ncpus -- re-measure it on this CPU-bound workload; the
      oversubscription doc argues it IS the libxtc-native path.  If TPS>=(A), it
      may be the simpler answer than growing the pooled pool.

Guardrail: whatever grows the pool MUST NOT reintroduce the 2026-07-23
thread-explosion (fiber-executor loops stacked on pooled carriers -> CFS became
top CPU consumer).  Cap total OS threads at ~ncpus, not limit+executor+workers.

## Also confirmed this session
- NO fiber LWLock livelock (the 2026-08-06 finding was a misdiagnosis: -S connect
  collapse = the now-fixed connect-drop chain; -N low tps = EBS fdatasync 4.3MB/s
  ceiling, fork is equally disk-bound there).
- Fix A + reroute survived the rebase and function (0 connect failures to c=256 -S).
- io=sync, no futex storm in the saturated -S profile (xtc symbols ~0%,
  LWLockAcquire 3.6%, PinBuffer 1.5% -- normal).
