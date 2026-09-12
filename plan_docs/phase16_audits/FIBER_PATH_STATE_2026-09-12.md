# Fiber-path state after libxtc v1.44.1 (2026-09-12) -- consolidated, honest

The lost-wake blocker chased for the whole investigation IS FIXED (v1.44.1). But the fiber path is
NOT yet performance-competitive, and there are distinct open issues. This consolidates them so the
next session does not re-derive it.

## What is fixed / done
- **libxtc cross-loop aio lost-wake: FIXED** (v1.44.1 `e9a2e14`, `2c851a5`). The fdatasync strand
  (res=0 CQE on the submit loop's ring, fiber homed elsewhere, never reaped) no longer occurs;
  hang rate 0/12 vs the prior 22-60 %. Init (which fsyncs) now completes cleanly every time.
- **HEAD build break: FIXED** (`afae905889`). `f4ec2a937b` had committed the counter *name* while
  the enum *member* lived only in the uncommitted starvation WIP -> HEAD did not compile with
  `-Dxtc=enabled`. The one-line enum member is now committed.
- **The "pmchild double-release crash" was a PHANTOM** (`ba0c145367`): it does not reproduce on a
  genuinely clean HEAD build (4 independent local checks, incl. all three ADD PRIMARY KEY
  statements + a concurrent write run). It was the starvation WIP's mid-session carrier yield
  (which also produces a pgstat_is_initialized assert). My earlier f4ec2a937b crash-bisection is
  retracted (`72b9c653a7`) -- those builds silently linked the WIP.

## What is OPEN (the real remaining work, all OURS not libxtc's)
1. **Write-heavy WEDGE under concurrency -- CHARACTERIZED 2026-09-12** (see
   `plan_docs/phase16_audits/fiber-write-wedge-diagnosis-2026-09-12.md`). Confirmed to wedge on
   BOTH `carriers=0` (thread-per-session) and `carriers=-1` (pooled default, the shipped config --
   this is NOT a thread-per-session-only artifact). Three distinct ours-side mechanisms found: (1)
   a WalWriter livelock (`ProcSemaphoreWaitFiber`'s retry loop re-running a 231-entry GUC rebind on
   every park/wake, spinning 100% CPU and never returning to poll its own io_uring ring); (2) a
   genuine lost LWLock wakeup (`PGPROC.lwWaiting` frozen at `LW_WS_WAITING` after `WALWriteLock`
   release -- reproduces with a SINGLE backend, zero concurrency, at `pgbench -i`'s own `ADD PRIMARY
   KEY` step); (3) pooled-protocol sessions are never libxtc fibers, so their LWLock waits block the
   whole carrier OS thread via raw `PGSemaphoreLock` (18/32 carriers blocked simultaneously at
   c=32). No libxtc bug -- all three are PG-side. Fix directions are named in that doc but not yet
   implemented; that is the next real work on this axis.
2. **Read-path 5.6x gap.** Fiber `-S` completes cleanly (0 starved) but at 84,757 tps vs fork's
   473,830 = 0.18x, at c=32. A pure scheduler/overhead gap.
3. **Earlier win claims need re-establishing.** In-repo notes claim read -S beats fork 1.02-1.04x
   at c>=192 and CPU-bound 1.53x. This run (c=32) shows 0.18x read. Not necessarily a contradiction
   -- may be a different regime (high client count where fork's per-process cost dominates) -- but
   UNVERIFIED here. Re-run the c>=192 / 2x-oversubscription lane with a SEPARATE loadgen before
   trusting any win claim.
4. **Pooled-session starvation** (`working = min(clients, carriers)`): still open. Candidate fix is
   the stashed `PG_STEP_YIELD_BUDGET` WIP (`stash@{0}` + `.ec2/orphaned-wip/`), which is UNVALIDATED
   and currently causes a pgstat_is_initialized assert on resume -- fix that before landing it.
   Does NOT affect the fiber path (one fiber per session; starved=0 measured).
5. **Checkpointer/fiber buffer-lock ABBA deadlock** (baseline bug #4): still open; the sub-agent
   assigned to it did not land a fix. All threaded regress targets remain RED from it.

## Methodology debt to fix before the next benchmark
- **`io-backend=auto` silently degrades to epoll without `liburing`-dev installed at configure
  time** (`.ec2/loop-poll-2026-09-07/xtcpg_build.sh` never passed `-Dio-backend=uring`). The
  2026-09-12 v1.44.1 bench run below did not archive its build script or keep server logs, so its
  I/O backend is UNCONFIRMED -- treat its write-wedge numbers as directionally corroborated (the
  wedge itself reproduces on a verified-uring build too, per the 2026-09-12 diagnosis doc) but not
  as evidence about io_uring's cross-loop-submit machinery specifically. Force `-Dio-backend=uring`
  and verify at RUNTIME (`/proc/<pid>/fdinfo` showing `SqMask`/`CqMask`, plus `ldd` showing
  `liburing.so`), not just the meson log, on every future run.
- **Separate loadgen host.** This run co-located pgbench on the SUT (a known confound). The 5.6x
  and the wedge are too large to be driver placement alone, but a headline needs a remote driver.
- **Build from a committed archive, never the working tree** -- a dirty tree silently supplied a
  missing symbol and produced a build I mis-bisected. `git archive <commit>` is now the rule.
- **Distinguish WEDGE (server alive, no tps) from CRASH (server gone) from teardown-SIGKILL.** The
  harness must not report a timeout-kill as a crash.

## The honest bottom line
The north-star "beat fork by a significant margin" is NOT met at c=32 on the standard config today.
The libxtc-side blocker is genuinely closed; the remaining gaps are PG-side scheduler/perf problems,
now diagnosable with the mature xtc_tail instrument suite. That is a materially better position than
being blocked on an upstream lost-wake -- but it is not yet a win, and should not be reported as one.
