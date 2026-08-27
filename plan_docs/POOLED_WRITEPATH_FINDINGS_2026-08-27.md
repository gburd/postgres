# Threaded write-path + pooled-monitor findings from HammerDB TPROC-C on NVMe (2026-08-27)

For the pooled-protocol-scheduler / libxtc-write-path owners.  Measured on
c6id.8xlarge (32 vCPU), PGDATA on local NVMe (xfs), HammerDB 4.11 TPROC-C, 200
warehouses, full durability (fsync+synchronous_commit+full_page_writes ON),
origin/xtc @ ae9b11b5a1 (libxtc v1.37), pooled_protocol_carriers=auto(32).

## 1. Write-heavy durable OLTP: pooled-threaded is ~24% of fork  [PRIORITY]

| VU | fork NOPM | threaded NOPM | ratio |
|----|-----------|---------------|-------|
| 32 | 623,787   | 147,410       | 0.24x |
| 64 | 931,984   | 226,308       | 0.24x |

Threaded also burns MORE CPU per unit work (66% vs 41% at VU=16).  This is the
inverse of read-only pgbench -S (fiber beats fork 1.02-1.04x at c>=192) and of
CPU-bound compute (1.53x fork).  The write path -- WAL insert/flush, XLogFlush
fsync, commit, checkpoint -- is where the shared-address-space pooled runtime
loses to fork: fork gives every backend an independent kernel-scheduled commit
path, while the carrier pool funnels commits/fsync through N carriers.  Symptoms:
"checkpoints are occurring too frequently (5 seconds apart)" at max_wal_size=64GB
on the threaded lane, high I/O-wait, and monitoring-connection starvation.

This is the concrete write-path target for the libxtc fusion (dedup WAL/commit/
fsync onto xtc primitives; xtc_credit / batched fsync via xtc_io_aio; per-carrier
commit pipelining).  A/B each change on check-threaded-pooled + this HammerDB
TPROC-C lane.  Fork parity on write-heavy durable OLTP is the open north-star gap.

## 2. HammerDB monitor VU (Vuser 1) fails on the pooled server  [MEASUREMENT BLOCKER]

Most threaded TPROC-C lanes could not be measured because HammerDB's monitor VU
fails at "Rampup complete, Taking start Transaction Count" while every worker VU
"FINISHED SUCCESS" and the server does 85-88% CPU of real work.  The monitor runs,
on a dedicated pgtcl connection:
    select sum(xact_commit + xact_rollback) from pg_stat_database
    select sum(d_next_o_id) from district
Ruled OUT (all verified to WORK via psql on the same pooled server under load):
the queries themselves, DECLARE/FETCH cursor + async single-row protocol, and a
130-second-idle-then-reuse (no idle reaping).  No server-side ERROR/FATAL/PANIC.
The failure is INTERMITTENT (a few early runs captured threaded NOPM fine), which
points at a timing/protocol interaction: the monitor connection may starve for a
carrier slot while worker VUs saturate the pool (pool=32, VUs=16..128+monitor), or
the pgtcl monitor's PQsendQuery/PQgetResult single-row flow is serviced slowly by
the carrier under contention.  Repro: run the harness
(src/tools/benchmark/mtpg_hammerdb_bench.sh) with multithreaded=on; the threaded
lanes NA while process lanes capture NOPM every time.

Hypothesis to check first: does a fresh client connection opened AFTER the pool is
saturated with long-lived worker sessions get a carrier promptly, or does it block/
time out?  If the monitor's connect or first query waits > HammerDB's tolerance,
that is the bug.  A dedicated non-pooled (or reserved-carrier) admin/monitor lane
would both fix measurement and matter for real monitoring tools (Datadog,
pg_stat_statements pollers) against a saturated pooled server.

## 3. Startup-window fork-fail transient (benign, noted)

"could not fork new process for connection: Function not implemented" appears for
~4s during startup/recovery BEFORE "database system is ready to accept
connections"; harmless (the harness already tolerates it), but a client connecting
in that window gets a hard fork-fail rather than a retry/wait.  Consider holding
early client connections until the pool is ready instead of falling through to
fork under multithreaded=on.

## UPDATE (2026-08-27 pm): root-cause profiled -- it's carrier IDLE, not WAL

Clean pgbench TPC-B (scale-200, c=64, durability ON, NVMe, -c overrides):
fork 54,215 tps @ 6.9% idle  vs  pooled(32 car) 38,606-39,251 tps @ 25.8-28.5% idle
= pooled is 0.71-0.72x fork and leaves ~27% of the machine IDLE under load.

perf (system-wide): threaded lane = __cpuidle 44% self; all non-idle top symbols
are TCP loopback (send/tcp_sendmsg/epoll_wait).  NO insertpos_lck, NO
perform_spin_delay, NO LWLock, NO XLogFlush in top-30.  => the 2026-07-20
WAL-spinlock hypothesis is REFUTED for write-heavy OLTP; the bottleneck is the
scheduler leaving carriers idle, same class as the 2026-07-20 durability-off
"__xtc_exec_try_steal + 54% idle" finding.

Verified the write path already yields: pg_fdatasync/pg_fsync -> xtc_aio_* (park,
fd.c); WAL/xact waits -> Phase-17 eventfd park.  So it is NOT a serialized fsync
or a spinning carrier.  It is latency-overlap: sessions run to a protocol-read
boundary then park for the client's network RTT; at any instant only ~1 session
is runnable (pg_stat_activity active=1), so the woken carrier drains it and
re-parks while cores idle.  Fork overlaps 64 clients' RTT across 64 kernel
processes; pooled cannot manufacture more concurrent runnable work than clients
supply, and the dispatch concentrates on loop 0 (pg_stat_xtc_carriers: loop0
tasks_run=864, loops1-14 ~1; steals=0, carriers pinned migratable=0).

### Two concrete BUGS found (separate tickets, both HANGS):
1. pooled_protocol_carriers set EXPLICITLY > cores (48/64/96 on 32 vCPU) WEDGES:
   pgbench stalls at "starting vacuum...end", server stops answering.  Auto-sizing
   (caps effective at ~ncpu) avoids it, so default users don't hit it, but an
   explicit over-core value should clamp or work, not hang.  Limit is the raw GUC
   (PgRuntimePooledProtocolCarrierLimit == pooled_protocol_carriers, no ncpu clamp).
2. thread-per-session (pooled_protocol_carriers=0) WEDGES at c=64 write load
   (pgbench stuck, 0 tps) -- the known "could not lease protocol read park for
   same carrier resume" class, now also reproduced on plain pgbench TPC-B.

### Fix directions (A/B-gated, neutral-or-better on read-S/CPU, per Phase-18 rule):
- Keep >1 session runnable per carrier: prefetch/pipeline the next protocol
  message for parked sessions (xtc_io batching) so a carrier isn't idle waiting
  for the sole runnable session's RTT.
- Spread leased sessions across exec loops (round-robin the session onto the
  least-busy loop, or make pooled sessions migratable so idle loops steal) --
  today loop 0 does ~all the work.
- Fix the two wedges above (they block any oversubscription experiment).
No code changed: profiling only, per "name the symbol before converting."  Symbol
named = carrier IDLE -> fix is scheduler feeding, not a lock conversion.
