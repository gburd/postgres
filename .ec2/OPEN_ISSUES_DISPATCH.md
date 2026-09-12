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
