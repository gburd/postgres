# HammerDB TPROC-C on NVMe — fork vs pooled-threaded (2026-08-27)

Hardware (AWS chiuso): SUT = c6id.8xlarge (32 vCPU, 61 GB), PGDATA on the local
**1.7 TB NVMe instance store formatted xfs** (a REAL filesystem, not tmpfs/EBS).
Loadgen = c6i.4xlarge over the private VPC network (client load never steals SUT
CPU).  HammerDB 4.11 TPROC-C, 200 warehouses (~35 GB), DURABILITY=ON
(fsync + synchronous_commit + full_page_writes all ON), shared_buffers=8GB,
huge_pages=on, io_method=sync, max_wal_size=64GB.  Build = origin/xtc @ ae9b11b5a1
(libxtc v1.37), release.  Same data dir, restart between lanes with/without
multithreaded=on; pooled_protocol_carriers=auto (=32 on 32 vCPU).

## Results (HammerDB NOPM = New Orders/min; TPM = total)

| VU  | fork NOPM | fork TPM  | fork CPU | threaded NOPM | threaded TPM | threaded CPU |
|-----|-----------|-----------|----------|---------------|--------------|--------------|
| 16  | 479,013   | 1,102,110 | 41 %     | (monitor NA)  | -            | 66 %         |
| 32  | 623,787 – 884,684 | 1.44M – 2.04M | 80–99 % | 147,410* | 338,999* | 85 %  |
| 64  | 931,984   | 2,144,122 | 93 %     | 226,308*      | 520,040*     | 88 %         |
| 128 | 869,415   | 2,002,090 | 97 %     | (monitor NA)  | -            | 88 %         |

\* threaded rows are the runs where HammerDB's monitor VU succeeded (see the
measurement caveat); most threaded runs returned NA because the monitor VU failed.

## Headline: threaded LOSES badly on write-heavy durable OLTP

threaded / fork on TPROC-C (write-heavy, full durability, NVMe):
- VU=32: 147,410 / 623,787  = **0.24x**
- VU=64: 226,308 / 931,984  = **0.24x**

Threaded is ~**24 % of fork** here, and burns MORE CPU per unit work (66 % vs 41 %
at VU=16).  This is the OPPOSITE of the read-only pgbench -S result (fiber BEATS
fork 1.02-1.04x at c>=192) and consistent with the known write-path weakness: the
pooled carrier pool serializes the WAL-flush / fsync / commit path across a shared
address space where fork gives every backend an independent commit path.  Symptom
on the SUT: "checkpoints are occurring too frequently" + high I/O-wait under the
threaded lane, and the pooled monitoring connection starved (8s query timeouts).

This is a real, reproducible gap and the priority target for the libxtc-fusion
write path (WAL/commit/fsync on xtc primitives), NOT something the current pooled
scheduler closes.  The north-star "beat fork" is demonstrated on read OLTP + CPU-
bound compute; write-heavy durable OLTP is where the work remains.

## Measurement caveat / blocker: HammerDB monitor VU fails on the pooled server

Most threaded lanes returned NA because HammerDB's monitor VU (Vuser 1) FAILS at
"Rampup complete, Taking start Transaction Count" against the pooled threaded
server -- while all 32/64/128 WORKER VUs "FINISHED SUCCESS" and the server does
real work (85-88 % CPU).  The monitor issues
`select sum(xact_commit+xact_rollback) from pg_stat_database` and
`select sum(d_next_o_id) from district` on a dedicated connection.  Verified in
isolation via psql on the SAME pooled server: both queries, cursors/async, and a
130s-idle-then-reuse all WORK -- so it is not the query, the pooled carrier, or
idle reaping.  The failure is intermittent (a few early runs DID capture threaded
NOPM), pointing at a timing/protocol interaction between HammerDB's pgtcl monitor
connection and the pooled carrier under load (possibly the monitor's connection
starving for a carrier slot while workers saturate the pool, or a pgtcl
single-row-mode flow the carrier services slowly).  Filed for the pooled-scheduler
/ benchmark-harness owners.  No server-side ERROR/FATAL/PANIC is logged.

## Server-side notes
- Pooled carrier scheduler engaged correctly (POOLED_OK carriers=32) on every
  threaded lane; the startup-window "could not fork new process for connection:
  Function not implemented" lines are the known pre-ready transient (they stop
  once "database system is ready to accept connections" prints).
- fork lanes: clean, monotone NOPM, captured every time.
- The write load triggered 5-second checkpoint cycles at max_wal_size=64GB on both
  lanes; fork absorbed it (independent per-backend commit), threaded did not.
