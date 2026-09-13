# Open-issues dispatch plan (2026-09-12)

Five open issues from FIBER_PATH_STATE_2026-09-12.md, dispatched by dependency + file-ownership.

## In flight (parallel — no file collisions)
- **#1 write-heavy WEDGE at c=32** -> agent 235c046c (DIAGNOSIS only, touches no code).
  Characterize with xtc_tail: lost-wake vs deadlock vs contention, which primitive/loop, ours vs libxtc.
- **#5 checkpointer/fiber ABBA deadlock** -> agent 0e7398c4 (FIX). Owns bufmgr.c/proc.c/checkpointer.c/
  launch_backend.c aux side. Blocks ALL threaded regress (77/168 RED).
- **#2 read -S 5.6x gap** -> agent be16cc5a (DIAGNOSIS + client-count sweep). Touches no code.
  Reconciles the prior "wins at c>=192" claim by sweeping c=8..384 fiber-vs-fork.

## Queued (sequential — proc.c collision with #5)
- **#4 pooled starvation**: validate the stashed PG_STEP_YIELD_BUDGET WIP (stash@{0} /
  .ec2/orphaned-wip/), fix its pgstat_is_initialized-on-resume assert. Touches postgres.c +
  backend_runtime.h -- but the fairness logic is adjacent to proc.c wait paths #5 edits, so hold it
  until #5 lands to avoid a merge collision and a confounded validation.

## Ordering rationale
- #1 and #2 are diagnosis (no code) -> safe alongside anything.
- #5 is the only code-fix in flight; #4 waits for it (both near proc.c).
- #3 (verify win claims at c>=192) is FOLDED INTO #2's client-count sweep -- if #2 shows a crossover,
  #3 is answered; if not, the old claims are refuted and #3 is moot.

So the 5 issues collapse to: 2 diagnoses running, 1 fix running, 1 fix queued, 1 folded in.

## Update 2026-09-12 (later): ABBA agent hit its limit without a fix
- **#5 ABBA deadlock: STILL OPEN.** Agent 0e7398c4 ran ~2h, ended mid-diagnosis (limit reached),
  committed NO fix, and left a Debian box (xtc-abba3, us-west-2) which I terminated + cleaned
  (verified KeyName+Name tag; SG+key deleted, pem shredded).  No salvageable artifacts on it (it was
  still in Debian setup).  Needs a fresh, tightly-scoped agent -- the deadlock is deterministic so a
  focused repro-fix-verify pass should be quick, but it must be given a HARD turn budget and told to
  commit incrementally so partial progress survives a limit hit.
- Live and progressing: #2 read-gap (be16cc5a, 2 us-east-2 boxes = SUT+separate driver, as intended)
  and #1 write-wedge (235c046c), both restarting on Debian + forced-uring.
- Reminder for the ABBA re-dispatch: it edits proc.c, so keep #4 starvation (also near proc.c) queued
  behind it still.

## Update 2026-09-12 (write-wedge diagnosed -> fix dispatched)
- **#1 write wedge: DIAGNOSED** (fiber-write-wedge-diagnosis-2026-09-12.md) = 3 PG-side bugs on BOTH
  schedulers. Dispatched a FIX for the keystone (finding 2, lost LWLock wakeup, c=8 single-backend)
  as agent 082077eb, with a HARD 300-turn budget + commit-incrementally mandate (prior agents kept
  hitting limits mid-task). It owns lwlock.c + proc.c lwlock-wait paths.
- Findings 1 (WalWriter livelock) and 3 (raw-semaphore carriers) remain to fix AFTER the keystone --
  #2 may clear the write path on its own; re-measure before fixing them.
- **#5 ABBA: still OPEN, still un-dispatched** (its agent hit a limit). Its files (bufmgr/checkpointer)
  do NOT overlap the lwlock fix, so it CAN run in parallel -- but hold it one beat so the lwlock agent
  establishes proc.c ground truth first (the lwlock fix touches proc.c wait paths; ABBA touches
  proc.c semaphore paths -- adjacent). Re-dispatch ABBA once the lwlock agent has committed its repro.
- **#4 starvation: still queued** behind both proc.c-touching fixes.
- **#2 read-gap sweep: in flight** (be16cc5a, SUT+driver in us-east-2, Debian+uring, pooled default).

## Live agents right now
- 082077eb  lwlock-wakeup fix (keystone)   -- dispatching, Debian+uring
- be16cc5a  read-gap 3-lane c=8..384 sweep  -- running, SUT+driver us-east-2

## Update 2026-09-12 (north-star push: starvation fix dispatched)
- **#4 pooled starvation -> the north-star fix, DISPATCHED** as agent de68f552 (320-turn budget,
  commit-incrementally).  Rationale: the verified read sweep shows pooled = parity at c=32 then
  PLATEAUS (starvation) above it while fork climbs.  Fixing starvation is what lets pooled scale
  past the carrier count and BEAT fork at high concurrency = the north star.  It validates the
  stashed PG_STEP_YIELD_BUDGET WIP, fixes its pgstat-on-resume assert (normal resume calls
  pgstat_ensure_shmem_attached at postgres.c:7061; the yield path skips it), picks a positive default
  budget, and MEASURES the before/after scaling curve vs fork with a separate driver.
  Files: postgres.c/launch_backend.c/backend_runtime.h/walwriter.c/guc -- NO overlap with the lwlock
  agent's lwlock.c/proc.c, so safe in parallel; told to STOP if it hits a conflict near those.
- **#1 keystone lwlock fix: in flight** (082077eb, box up us-east-1).
- Two fix agents now running in parallel (disjoint files): lwlock (write path) + starvation (read
  scaling).  Between them they target the two things keeping fiber at/below fork.

## Live agents
- 082077eb  lost-LWLock-wakeup fix (write keystone)  -- us-east-1
- de68f552  pooled-starvation yield-budget fix + scaling sweep (THE north-star fix)


## Update 2026-09-12 (keystone diagnosed as a 4th bug; fix re-dispatched)
- **#1 keystone**: agent 082077eb hit its limit mid-diagnosis (no commit, LEAKED its box -> I
  terminated + cleaned it, verified).  But its diagnosis was gold: RULED OUT the CAS-race and
  proclist hypotheses, and found the REAL cause -- a FOURTH bug: an aux process (WalWriter,
  sem_fiber_backed=false at proc.c:842) blocking on a raw PGSemaphoreLock freezes the carrier OS
  thread that runs xtc_io_poll for a colocated backend fiber, so that fiber's READY completion is
  never reaped.  Preserved in LWLOCK_WEDGE_ROOTCAUSE_AUX_CARRIER_STARVATION.md, root cause + fix
  location re-verified in source.
- **Fix re-dispatched** as agent 0127e37b (220-turn budget): make aux fibers sem_fiber_backed from
  xtc_in_backend_fiber (like InitProcess:613) so their LWLock waits PARK instead of freezing the
  thread.  Scoped to proc.c InitAuxiliaryProcess ONLY -- disjoint from de68f552's
  postgres.c/launch_backend.c starvation fix.

## Two north-star fix agents live in parallel (disjoint files)
- 0127e37b  aux-fiber sem_fiber_backed fix (WRITE keystone)  -- proc.c InitAuxiliaryProcess
- de68f552  pooled-starvation yield-budget fix + scaling sweep (READ scaling) -- postgres.c etc.


## HAZARD LEARNED 2026-09-12: parallel agents SHARE the working tree -- a checkout/reset nukes a sibling's uncommitted edits
Two fix agents ran in parallel in the SAME checkout (/home/gburd/ws/postgres/xtc).  The starvation
agent (de68f552) drifted out of scope and left an uncommitted proc.c aux-fiber fix in the tree.  To
let the aux-fiber agent (0127e37b) land ITS proc.c fix cleanly, I ran
`git checkout -- src/backend/storage/lmgr/proc.c` to reset -- which silently REVERTED 0127e37b's OWN
uncommitted proc.c edit (it had an edit live in the shared tree).  0127e37b noticed its fix "vanished
to HEAD with no stash/commit trace" and correctly stopped chasing it as shared-worktree interference.

Root cause: multiple background agents operate on ONE working tree.  Any `git checkout`/`git reset`/
`git stash` by me OR by an agent affects EVERY agent's uncommitted work in that tree.  This is the
SECOND time uncommitted state crossed between contexts (cf. the dirty-tree-supplied-a-symbol
bisection trap).

RULES going forward:
1. NEVER `git checkout -- <file>` / reset / stash to clean up one agent's mess while another agent
   is live in the same checkout and might have uncommitted edits to that file.  Preserve as a patch,
   then let the OWNING agent reset its own tree, or wait until all siblings have committed.
2. Prefer separate git worktrees (`git worktree add`) per parallel agent when they might touch
   nearby files -- disjoint FILE lists is not enough; the WORKING TREE is shared.
3. Agents must commit within one turn of editing (already mandated) -- the shared tree makes the
   edit->commit window a real data-loss window, not just a crash window.
4. When two agents' file scopes are truly disjoint AND both commit-fast, parallel-in-one-tree is
   tolerable; when scopes can overlap (both touched proc.c here), serialize or use worktrees.


## Update 2026-09-13: write keystone FIXED (ef06a6b07c); NO live agents; EC2 clean
- **#1 write keystone (aux-fiber sem_fiber_backed) FIXED + validated** by agent 0127e37b (commit
  ef06a6b07c): c=8 pgbench -i -s 300 wedge reproduced pre-fix, GONE post-fix 3/3, gmake check 245/245
  (isolated worktree).  Two agents independently converged on this exact fix -- strong evidence.
  Validated LOCALLY (uring-backed) -- creds AuthFailure blocked EC2, re-confirm on Debian/EC2 later.
  Doc: WRITE_KEYSTONE_FIXED_AUX_FIBER_2026-09-13.md.
- **The NEXT write blocker is finding #1-livelock**: c>=32 write hits WalWriter livelock in
  ProcSemaphoreWaitFiber's GUC-rebind retry (100% CPU spin).  Fix = GUC lazy-rebind
  (PgRuntimeRestoreCurrentWorkLazy exists at that site, unwired).
- **Read starvation = TWO bugs**: budget (necessary, done-in-WIP, pooled-gated) + lease fairness
  (open, the 33501-vs-1 spread).  Next read fix = FIFO round-robin fairness in the yield/re-enqueue.
- NO live agents.  EC2 clean across 6 regions.  Working tree clean.  A creds AuthFailure was seen
  mid-agent-run -- check `lava` creds freshness before the next EC2 dispatch.

## Live agents
- (none)

## Ready-to-dispatch queue (ordered)
1. Write finding #1: WalWriter GUC-rebind livelock -> lazy-rebind.  Then re-measure c>=32 write.
2. Read fairness: FIFO round-robin in PgCarrierYieldRunnableOnBudget/PopRunnable.  Then re-measure
   the read -S scaling sweep vs fork (the north-star curve).
3. ABBA checkpointer/fiber deadlock (#5) -- still open, blocks threaded regress.
Use separate git worktrees per parallel agent (shared-tree hazard learned this session).


## Update 2026-09-13b: two fixes dispatched in ISOLATED WORKTREES (hazard-mitigated), disjoint files
Per the shared-worktree hazard, each parallel agent now gets its OWN git worktree + branch:
- **fairness** (agent 06c5ea80, worktree /home/gburd/ws/postgres/xtc-fairness, branch fairness-fix):
  the READ north-star fix.  Extract budget (pooled-gated) from the preserved patch + fix the LEASE
  FAIRNESS (the 33501-vs-1 monopoly).  Mechanism to confirm by instrumentation: 2 hot sessions
  ping-pong across 2 carriers while 6 parked siblings never get leased, despite pop-head/push-tail
  looking FIFO.  Files: launch_backend.c (carrier loop), backend_runtime_backend.c
  (Yield/PopRunnable), postgres.c (budget gate), guc/globals.  Deliver the EC2 read -S scaling curve
  vs fork (does pooled scale past c=32 now?).
- **livelock** (agent 215888e8, worktree /home/gburd/ws/postgres/xtc-livelock, branch livelock-fix):
  the WRITE finding #1.  Switch the eager PgRuntimeRestoreCurrentWork to the LAZY variant
  (PgRuntimeRestoreCurrentWorkLazy, exists, unwired) at fiber fd-resume sites -- THE site is
  pg_xtc_carrier.c:1420 (xtc_pg_wait_fd resume runs a 231-entry GUC rebind every park/wake -> 100%
  CPU spin, never re-polls the ring).  Per-site safety check + a stale-GUC data-integrity test (a
  wrong lazy switch = silent stale GUC read = the one class we must NOT trade away).  Files:
  pg_xtc_carrier.c/method_xtc.c/fd.c/dfmgr.c/guc.c -- DISJOINT from fairness.

Both: 260-300 turn budgets, commit+push-per-step to their own branch, tear-down-on-any-exit.
Merge each branch to xtc after it validates.  Disjoint files + separate worktrees => truly parallel.

## Live agents
- 06c5ea80  pooled lease FAIRNESS (read north-star curve)   -- worktree xtc-fairness / fairness-fix
- 215888e8  WalWriter GUC-rebind LIVELOCK (write finding #1) -- worktree xtc-livelock / livelock-fix


## Update 2026-09-13c: livelock REPRODUCED + fix built (unvalidated); validation re-dispatched
- Livelock agent 215888e8 REPRODUCED write finding #1 live (gdb: RebindSessionGUCVariablePointers <-
  PgRuntimeRestoreCurrentWork <- xtc_pg_wait_fd:1420 <- ProcSemaphoreWaitFiber <- XLogFlush; 100% of a
  core; 14 unreaped ring completions) and switched the 2 HOT fiber-resume sites (pg_xtc_carrier.c
  :1457/:1405) to PgRuntimeRestoreCurrentWorkLazy -- leaving the 5 lower-freq sites eager (correct
  conservative call).  Builds clean.  ABORTED right before validating the AFTER.
- Committed as UNVALIDATED WIP 9e6167edac on branch livelock-fix (pushed).  NOT merged to xtc.
- Validation re-dispatched (agent 08a8cb80, 160-turn budget, livelock-fix worktree): prove
  livelock-GONE at c=32 + the STALE-GUC data-integrity test (the critical safety gate) + gmake check
  245/245.  A stale GUC read on the lazy path = STOP-and-report bug, not ship-around.
- No EC2 leaked (livelock work was local).  EC2 clean.

## Live agents
- 06c5ea80  pooled lease FAIRNESS (read north-star curve)  -- worktree xtc-fairness / fairness-fix
- 08a8cb80  livelock fix VALIDATION (write finding #1)      -- worktree xtc-livelock / livelock-fix
Branches: fairness-fix, livelock-fix (WIP pushed) -- merge each to xtc after it validates.
