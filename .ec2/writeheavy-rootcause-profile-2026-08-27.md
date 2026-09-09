> **CAVEAT ADDED 2026-09-07 -- LOADGEN WAS CO-LOCATED ON THE SUT.**
> This study ran pgbench on the SUT itself: TCP loopback symbols (__send, tcp_sendmsg,
> tcp_write_xmit, epoll_wait) dominate the non-idle profile in BOTH lanes, which is only
> possible with a local driver.  That violates the apples-to-apples methodology later
> fixed in plan_docs/FORK_TO_XTC_PERF_PLAN.md section 1 (driver on a SEPARATE host).
>
> Why it matters directionally, not just formally: pgbench with 64 clients competes for
> the same 32 vCPUs as the server, so the headline "44% swapper/__cpuidle => carriers
> are starved" reading is CONFOUNDED.  Co-located load can manufacture an idle-core
> signature (driver descheduled -> server has nothing to do), and it also depresses both
> lanes' absolute tps.  The 0.71-0.72x RATIO is more trustworthy than the idle% is,
> because both lanes paid the same driver tax -- but fork and threaded do not
> necessarily pay it EQUALLY (fork's per-backend processes and the threaded carriers
> interleave with the driver differently), so even the ratio is not airtight.
>
> What still stands, because it is a NEGATIVE result robust to the confound: no
> WAL-insert spinlock (insertpos_lck), no perform_spin_delay, no LWLock, no XLogFlush
> in the top 30 -- co-locating a driver cannot REMOVE server-side lock contention from
> a profile.  So "the write gap is not WAL/lock contention" holds; "the write gap IS
> carrier under-utilization" is downgraded to a hypothesis pending a re-run with a
> separate loadgen.
>
> ACTION: P3 must re-establish this profile under section-1 methodology before P4
> commits to the scheduler-feeding fix.  The dispatch-affinity finding below (loop 0
> gets 29/23 of the work, steals=0) is independent of the driver placement and stands.

# HammerDB/pgbench write-heavy disparity — root-cause profiling (2026-08-27)

Follow-up to hammerdb-nvme-fork-vs-threaded-2026-08-27.md.  Goal: find WHY pooled
threaded loses to fork on write-heavy durable OLTP, before changing code.
SUT c6id.8xlarge (32 vCPU), PGDATA on local NVMe (xfs), pgbench TPC-B scale-200,
64 clients, fsync+synchronous_commit+full_page_writes ON, io_method=sync,
huge_pages=on, origin/xtc @ b2a580d59c (libxtc v1.37), release build.

## Clean A/B (postgres -c overrides, hardstop-by-PID between lanes, mode-verified)

| lane                        | tps    | idle under load | vs fork |
|-----------------------------|--------|-----------------|---------|
| fork (mt=off)               | 54,215 | 6.9 %           | 1.00x   |
| pooled (mt=on, auto=32 car) | 38,606 | 28.5 %          | 0.71x   |
| pooled (explicit 32 car)    | 39,251 | 25.8 %          | 0.72x   |
| thread-per-session (car=0)  | WEDGES at c=64 (pgbench stuck "starting vacuum...end", 0 tps) |
| pooled explicit 48/64/96 car | WEDGES (carriers>cores hangs; server stops answering) |

Reproducible: pooled threaded is ~71-72% of fork on this write workload, and
LEAVES ~26-28% OF THE MACHINE IDLE under load while fork saturates it (6.9% idle).

## Root cause: NOT WAL/lock contention -- carrier under-utilization (idle cores)

perf (system-wide, 40s, during the write load):
- fork lane: box busy (~8% swapper/idle); top self = TCP loopback send/recv + syscall.
- threaded lane: **swapper/__cpuidle = 44% self-time** -- nearly half the cores
  IDLE under load.  Top non-idle symbols are ALL TCP loopback (__send, tcp_sendmsg,
  tcp_write_xmit, epoll_wait).  NO WAL-insert spinlock (insertpos_lck), NO
  perform_spin_delay, NO LWLock, NO XLogFlush in the top 30.  The 2026-07-20 WAL-
  contention hypothesis is REFUTED for this workload.

Carrier loop stats mid-run (pg_stat_xtc_carriers): loop 0 tasks_run=864, loops
1-2 = 29/23, loops 3-14 = 1 each; steals=0 on every loop.  pg_stat_xtc_runtime:
sessions_leased=67, sessions_resumed=38, protocol_parks=46, wakes_delivered=46,
carriers_started=31, queue_waits=54.  pg_stat_activity sampled during load:
active=1 almost always -- i.e. at any instant only ~1 session is runnable in the
pool; the rest are parked on protocol-read (waiting for the client's next
statement over the network RTT).

## The mechanism (why idle, and why fsync is NOT the culprit)

Verified the write path already yields the carrier where it matters:
- pg_fdatasync / pg_fsync on a backend fiber route through xtc_aio_fdatasync/fsync
  (fd.c) which PARK the fiber (io_uring/offload) -- the carrier stays live for
  siblings.  NOT a blocking carrier stall.
- WAL insertion wait / xact-lock wait use the Phase-17 ProcWaitOnSemaphore eventfd
  park (F4 audit) -- fiber-aware.

So the loss is NOT a serialized fsync or a spinning carrier.  It is that the
pooled model runs sessions to a protocol-read boundary then PARKS them; a
synchronous pgbench client then has network-RTT dead time before its next
statement.  Fork overlaps 64 clients' RTT across 64 kernel-scheduled processes
(kernel keeps cores warm).  Pooled multiplexes onto ~32 carriers but at any
instant only ~1 session is runnable -> the woken carrier drains it in microseconds
and re-parks, and the pool cannot manufacture more concurrent runnable work than
the clients supply -> cores idle -> ~71% of fork.  It is a LATENCY-OVERLAP /
scheduler-feeding gap, not a lock/IO gap.

## What does NOT fix it (measured)
- More carriers (oversubscribe 48/64/96 on 32 cores): WEDGES (explicit carriers >
  cores hangs -- a separate pooled-scheduler bug worth its own ticket).
- thread-per-session (car=0): WEDGES at c=64 (the known "could not lease protocol
  read park for same carrier resume" class).

## Where the real fix lives (for the pooled-scheduler / libxtc-fusion owners)
The gap is the pooled protocol scheduler's ability to keep many sessions
concurrently in-flight and spread across carrier loops, NOT WAL.  Candidate
directions (each A/B on this exact lane, neutral-or-better gate):
1. Pipeline/batch client reads so a carrier has >1 runnable session (prefetch the
   next protocol message for parked sessions; the F-series xtc_io batching).
2. Spread leased sessions across loops instead of loop-0 concentration (the
   dispatch hands almost all work to loop 0; steals=0 because pooled carriers are
   pinned migratable=0).  Either round-robin the *session* onto the least-busy
   carrier's loop, or make pooled sessions migratable so idle loops steal.
3. Fix the explicit-carriers>cores wedge + the thread-per-session c=64 wedge
   (both hang; they mask any oversubscription test).
This is the open north-star write-path gap; read -S (fiber beats fork) and CPU-
bound (1.53x) already win.  No code changed this pass -- profiling only, per the
"name the symbol before converting" discipline; the symbol named is carrier IDLE,
so the fix is scheduler feeding, not a lock conversion.
