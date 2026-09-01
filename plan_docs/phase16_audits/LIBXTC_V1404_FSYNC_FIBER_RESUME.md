# libxtc v1.40.4 follow-up: the WAL-fsync fiber (xtc_aio_fdatasync) is STILL not resumed under 64-client concurrent commit -- the 5th surface

Date: 2026-09-01
libxtc: v1.40.4 (rev e3b77af / fix commit 9c41529). All FOUR prior cross-loop fixes
confirmed in: fd registry defer (v1.40.3), reap-before-steal (v1.40.3), atomic task->state
CAS (v1.40.4), timer-arm-on-running-loop (v1.40.4).
PG "xtc" branch HEAD includes a PG-SIDE fix landed this session (hot current-work cell
mode-state + process cells made per-carrier-thread; see below) -- so this is NOT that.

--------------------------------------------------------------------------------
## TL;DR
The task->state fix (v1.40.4) is confirmed applied and the task->state TSan race is GONE.
We ALSO found and fixed a PG-side process-global race (the "hot current-work cells").
With both in, the 64-client concurrent-commit collapse STILL reproduces on the FIBER path
(pooled_protocol_carriers=0), and a fresh clean diagnostic pins it: the WAL-fsync fiber
that parked in xtc_aio_fdatasync (PG wait_event IO/WalSync) while holding WALWriteLock is
not resumed after its io_uring FDATASYNC completion -- so 16+ committer fibers pile up on
LWLock/WALWrite and commits freeze (delta 3 / 3s vs 115/3s healthy).

IMPORTANT SCOPE CORRECTION (for us and you): the STACKLESS pooled default
(pooled_protocol_carriers=-1, sessions run inline on carrier pthreads) is HEALTHY -- it
completes pgbench -c 64 at 37.4k tps (fork = 45.0k) with no hang. ONLY the sessions-as-
fibers path (carriers=0, each session an xtc fiber that parks in place on fsync) hangs.
That is expected: only the fiber path calls xtc_aio_fdatasync from a migratable fiber, so
only it exercises this resume path.

--------------------------------------------------------------------------------
## Clean diagnostic (v1.40.4, fiber path, 64-client TPC-B, scale-100, NVMe, cassert off)
Mid-run, WHILE collapsed (no shutdown contamination):
- 128 conns established; psql/select 1 respond (rc=0) -- runtime not dead.
- pg_stat_activity (client backends): 16 active on LWLock/WALWrite, 1 active on IO/WalSync,
  25 on Lock/transactionid, 22 on Lock/tuple.
- commits delta = 3 in 3s (frozen; the healthy pool path shows ~115/3s).
- pgbench progress: healthy 2200-4000 tps for ~18s, then 0.0 and never recovers.
- gdb "thread apply all bt" at the freeze: xtc_aio=0, fdatasync=0, issue_xlog_fsync=0
  (NO fiber on any OS thread stack is in the fsync), iou-wrk=23 (io_uring workers present,
  the FDATASYNC was serviced), io_poll=30 (all carrier loops idle), XLogFlush=1 (the WAL
  writer waiting on WALWriteLock, addr 0x7f09c1960b80, held exclusive).
So: the fsync fiber set PG wait_event IO/WalSync, called xtc_aio_fdatasync, PARKED, its
io_uring FDATASYNC completion was produced -- and the fiber was NOT rescheduled. It stays
suspended holding WALWriteLock; every committer piles up behind it; commits freeze.

## Why we believe it is a libxtc resume miss (not PG, not the hot-cell race)
- PG parks via xtc_aio_fdatasync and does nothing after; the resume is libxtc's.
- All four prior cross-loop fixes are in and their TSan races are gone.
- We fixed the one PG-side process-global race a fresh TSan run flagged (hot current-work
  cells -- details below), and the collapse is unchanged, so it is not that either.
- The fiber is genuinely not-runnable (off every OS thread, all loops idle in xtc_io_poll),
  which is a scheduler resume state, not a PG lock cycle (the WALWriteLock waiters are
  correctly parked BEHIND the stuck holder).
This is the same END STATE as the four you already fixed, now on the xtc_aio_fdatasync
completion-resume path specifically, and only reachable from a MIGRATABLE fiber doing a WAL
fsync under high concurrency -- the 5th surface you anticipated.

## The PG-side race we fixed this session (so you can rule it out)
A fresh v1.40.4 TSan run's top un-suppressed finding was PG-side:
PgRuntimeInstallHotCurrentCells / PgRuntimeClearHotCurrentRootRefs (backend_runtime.c) --
our process-global "hot current-work cell" mode-state + *ProcessCell/*ProcessRef pointers
were written by every carrier thread on every current-work switch. We made the mode-state
PG_THREAD_LOCAL and stopped carriers from clearing the process-global cells. That removed
those ~530 TSan reports. It did NOT change the collapse -- confirming the residual is the
xtc_aio_fdatasync resume, not the hot cells.

## Reproduction
1. PG "xtc" (HEAD, includes the hot-cell fix) on libxtc v1.40.4, multithreaded=on,
   pooled_protocol_carriers=0 (fiber-per-session), fsync=on, PGDATA on NVMe/xfs, 32-vCPU.
2. pgbench -i -s 100; pgbench -c 64 -j 16 -T 60 -P 2. Healthy for ~15-18s, then 0.0 tps.
   gdb: WALWriteLock held exclusive by an off-thread fiber, all loops idle in xtc_io_poll,
   iou-wrk present, no fiber in xtc_aio on any stack.
Contrast: carriers=-1 (stackless pool) completes at 37.4k tps -- the fiber path is the only
one that hangs.
We can share cf_bt_v1404.txt + the clean diagnostic and a scripted repro.

## When we come back
This matches your "if there is a fifth surface, the same method will find it." The gdb
here is the fiber-off-thread-holding-WALWriteLock + all-loops-idle signature you asked for.
If it would help, we can build a focused libxtc-only harness (a migratable fiber doing
xtc_aio_fdatasync in a loop while a foreign thread hammers xtc_proc_wake + eager rebalance)
-- the same shape that reproduced #1-#4 -- to give you a TSan/standalone repro without PG.

## UPDATE 2026-09-01: v1.40.6 (timer-park fix) applied -- INTERMITTENT strand remains; OWNERSHIP NOT YET PROVEN

libxtc v1.40.6 (rev 3edc2e9, the cancel-before-re-arm timer-park fix) is in.  The fiber
path (pooled_protocol_carriers=0) at pgbench -c 64 is now INTERMITTENT, not a hard 100%
hang:
- One capture progressed at ~1730 commits/s (8658 commits / 5s) mid-run -- healthy.
- Another run hung at "starting vacuum...end" (the very first transactions), timeout-killed
  at 120s on a -T 30 bench (rc=124).

So v1.40.6 HELPED (some runs now make full-rate progress where v1.40.4 always stalled) but
a residual INTERMITTENT strand remains on the fiber path.  The stackless pool default stays
100% healthy (37.9k tps, always completes).

WHAT IS NOT YET PROVEN (do NOT file a libxtc report until this is closed):
- The xtc_dump at one stalled moment showed 64 park=fd (idle clients) + 35 park=- + exactly
  ONE park=timer proc.  I DID NOT capture that timer proc's STACK, so I cannot say it is the
  strand.  Static analysis says it may well be a RED HERRING: bgwriter/checkpointer do a
  normal periodic WaitLatch(timeout=100/200ms) (seen in the bt as wait classes 0x05/0x09),
  which is a legitimate park=timer that re-arms every wake -- NOT a strand.
- I do NOT have the stranded WALWriteLock holder's fiber stack on v1.40.6.  Earlier captures
  that showed the holder were on v1.40.3/v1.40.4 (pre-timer-fix) and are stale.
- Ownership (libxtc vs PG) is therefore UNDETERMINED.  The one PG-side thing the team's
  reply named -- a hand-rolled xtc_task_park_on_timer re-park loop -- is ABSENT in PG (our
  timed waits route through xtc_proc_sleep / xtc_proc_wait_fd, both v1.40.6-fixed library
  primitives).  That points AWAY from a trivial PG fix but does NOT prove a libxtc gap.

CORRECTION to earlier framing: this is NOT confirmed to be the xtc_aio_fdatasync completion
(team verified that path sound; our progressing runs release WALWriteLock fine).  Nor is it
confirmed to be a timer-park.  It is an INTERMITTENT strand of undetermined mechanism.

NEXT (required before any libxtc report): reproduce a stall on v1.40.6 and capture the
STRANDED HOLDER's fiber stack + park kind (find the PGPROC holding WALWriteLock, read its
task->park_fd / park_timer / state).  Only then decide PG-vs-libxtc and write the report.
Option A flip stays staged (dormant) until the fiber path is 0-hang across many runs at
64/192/256.

