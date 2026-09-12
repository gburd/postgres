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
