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
