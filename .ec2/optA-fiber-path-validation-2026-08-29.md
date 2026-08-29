# Option A (sessions-as-fibers) -- validated model, blocked on a libxtc fiber-resume wake-miss

Date 2026-08-29 (mala, c6id.8xlarge NVMe, libxtc v1.40, PG debugoptimized).

## What option A IS, in this tree
The fiber-per-session execution model already exists as pooled_protocol_carriers=0:
launch_backend.c routes B_BACKEND to xtc_pg_launch_backend_fiber -> a libxtc fiber on the
exec-loop pool running backend_thread_entry/BackendMain, xtc_in_backend_fiber=true, so
pg_fdatasync -> xtc_aio_fdatasync PARKS the fiber (carrier runs siblings).  The DEFAULT
(carriers=-1 -> resolved to core count) is the STACKLESS pooled scheduler (raw carrier
pthreads, N sessions inline, inline blocking fsync) -- that is the wedging path.  So
"option A" = make the default use the fiber model.

## Pivotal experiment (the wedge repro, fork vs both threaded models)
64-client TPC-B write load, fsync=on, NVMe:
 - pooled32 (stackless, current default): WEDGES -- select 1 times out 13+ consecutive,
   never recovers.  bt: a raw carrier pthread in BLOCKING fdatasync (fd.c:617) under
   XLogFlush holding WALWriteLock, 14 waiters parked behind it.  No tps.
 - fibers0 (option A): does NOT wedge -- select 1 only ever blips 1 probe and RECOVERS.
   Completed the full 64-client write load at tps=34811.  The fiber parks on fsync via
   xtc_aio_fdatasync and the carrier runs siblings.  <-- option A works for the fsync
   coupling, exactly as the libxtc team predicted.

## The residual blocker (a DIFFERENT, libxtc bug)
fibers0 has an INTERMITTENT lost-wakeup: a backend fiber that acquires WALWriteLock, WAL-
writes, and parks in xtc_aio_fdatasync is sometimes NEVER RESUMED after its fsync
completes -> stays parked off-thread holding WALWriteLock -> WAL writer + checkpointer +
load all block on WALWriteLock -> permanent wedge; all loops idle in xtc_io_poll.
 - NATIVE io_uring fsync: 1/8 scale-100 inits wedge.
 - XTC_AIO_FORCE_OFFLOAD=1 (blocking-pool fsync): wedges EVERY run (deterministic).
Both: WALWriteLock state=0x80042000 (held exclusive), holder on no OS thread, no
exec_simple_query/XLogWrite/xtc_aio/fdatasync frame anywhere.  This is the fiber's OWN
completion resume being lost -- NOT the v1.39 producer-side cross-loop wake (that is
fixed).  Filed: plan_docs/phase16_audits/LIBXTC_FIBER_RESUME_WAKE_MISS.md +
/tmp/libxtc-fiber-resume-wake-miss-2026-08-29.md.  Backtraces:
.ec2/fiber-resume-wedge-backtraces-2026-08-29.txt.

## Decision
Option A is the correct model and is validated for the write-fsync coupling.  Do NOT flip
the default to the fiber path until the libxtc fiber-resume wake-miss is fixed -- a path
that wedges 1/8 fails the "outright win, stability at scale" bar.  Sequence:
 1. libxtc fixes the fiber-resume wake-miss (report filed; deterministic offload repro
    hands them a fast test).  Re-validate: 0 wedges across many inits + sustained write
    load + high concurrency (64/192/256).
 2. THEN flip the -1 auto default to the fiber model (postmaster.c resolution), keep the
    stackless pool available via an explicit positive carriers value, keep process mode
    byte-for-byte.  Two-review gate; A/B neutral-or-better on read-S/CPU.
 3. Full matrix (pgbench -S, CPU, HammerDB TPROC-C/H) at 85% RAM vs fork -> the win.

## Fork baseline (for when the fiber path is un-wedged)
Same box/config, 64-client TPC-B, 90s, fsync=on, NVMe, SB=40% RAM:
 - FORK (mt=off): tps = 41,058 (3.69M txns, 0 failed, latency avg 1.56ms).  <-- the number
   option A must beat.
 - fibers0 (option A): could not get a clean 64-client throughput number -- the -c 64 run
   hit the fiber-resume wake-miss and wedged mid-bench (log stops at vacuum).  A clean
   fibers0 throughput comparison is BLOCKED on the libxtc fiber-resume fix.
The earlier fibers0 = 34,811 tps datapoint (first pivotal experiment, 100s run) shows the
model is in the right ballpark and, once wedge-free, is a credible base to push past fork
with libxtc-fused primitives; but it is not yet a clean win and not yet trustworthy until
the wake-miss is gone.
