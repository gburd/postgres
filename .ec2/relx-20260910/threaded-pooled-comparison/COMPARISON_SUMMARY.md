# check-threaded-pooled: patched tree vs unmodified HEAD, 5 vs 4 runs (2026-09-10)

## Context / correction

An earlier version of this report (see git history of this directory) treated
`check-threaded-pooled` RED as a possible regression from Fix A/Fix B. That was
wrong: a baseline agent (commit `d4f238ebaf`,
`plan_docs/phase16_audits/THREADED_TEST_BASELINE_2026-09.md`) subsequently
measured pristine HEAD (`563e490870`) and found ALL threaded targets
deterministically RED at **77 ok / 168 not ok**, triggered by a genuine,
pre-existing checkpointer/backend-fiber buffer-lock deadlock during
`create_index` (`REINDEX TABLE CONCURRENTLY`), unrelated to anything in this
task. The corrected instruction: 77/168 is the neutral baseline; only a
*different* count is the interesting signal.

This document records what was actually measured on THIS EC2 instance
(`c6id.4xlarge`, different hardware from the baseline's `c6id.8xlarge`) when
re-running that comparison directly, since the signal turned out to be more
interesting than "neutral."

## What was measured

Two autoconf trees, both `USE_XTC_CARRIER=1`, both built against the same
libxtc v1.43.0 static lib, both installed fresh, run back-to-back on the same
box:

- **`pg-prefix-autoconf`**: unmodified `HEAD` (`git diff --stat HEAD` empty).
- **`pg-autoconf`**: `HEAD` + Fix A (`reloptions.c`) + Fix B (`xpath.c`,
  `xslt_proc.c`, `xml.c`, `xml.h`) + the `guc.c` sibling fix (see
  `RELOPTIONS_THREADED_UNWIND_AUDIT.md` section 4) -- the exact tree these two
  fixes are being landed from.

`gmake -C src/test/regress check-threaded-pooled` run 4x on unmodified HEAD and
5x on the patched tree, `threaded_pooled.conf` (`multithreaded=on
pooled_protocol_carriers=4`), full `parallel_schedule`.

## Results

| Run | Tree | Outcome | `create_index` | Notes |
|---|---|---|---|---|
| 1 | unmodified HEAD | 77 ok / 168 not ok | **DEADLOCK** | matches documented baseline exactly |
| 2 | unmodified HEAD | 77 ok / 168 not ok | **DEADLOCK** | |
| 3 | unmodified HEAD | 77 ok / 168 not ok | **DEADLOCK** | |
| 4 | unmodified HEAD | 77 ok / 168 not ok | **DEADLOCK** | |
| 1 | patched | hung, killed manually | passed (555ms) | wedged at test 171, `publication`/`subscription` group |
| 2 | patched | 244 ok / 1 not ok | passed (535ms) | `select_distinct` plan-shape diff only, clean finish |
| 3 | patched | hung, killed manually | passed (545ms) | identical hang location/stack to run 1 |
| 4 | patched | 242 ok / 3 not ok | passed (555ms) | `subselect`/`union`/`join` plan-shape diffs, clean finish |
| 5 | patched | 244 ok / 1 not ok | passed (549ms) | `select_distinct` plan-shape diff only, clean finish |

**`create_index` deadlock rate: unmodified HEAD 4/4 (100%); patched tree 0/5
(0%).**

Note: the very first `check-threaded-pooled` run on this task (Fix A + Fix B
applied, BEFORE the `guc.c` sibling fix was found/applied) also passed
`create_index` and progressed to ~test 218 (the `xml` test, deep past
`create_index`) before crashing on the then-unfixed `guc.c` Assert at
`subselect` (see `RELOPTIONS_THREADED_UNWIND_AUDIT.md` section 4 and
`guc_c_sibling_bug_before_crash.log`). So the `create_index` pass is not an
artifact of the `guc.c` fix specifically -- it was already passing with only
Fix A + Fix B in the tree.

**New hang location on the patched tree (2/5 runs): `publication`/`subscription`
test group, NOT `create_index`.** Identical stack both times:

```
#3  WaitEventSetWait (..., timeout=10, ...)
#4  WaitLatch (...)
#5  logicalrep_worker_stop_internal (worker=..., signo=15) at launcher.c:663
#6  logicalrep_worker_stop (wtype=WORKERTYPE_APPLY, subid=..., relid=0) at launcher.c:711
#7  DropSubscription (stmt=..., isTopLevel=true) at subscriptioncmds.c:2733
#8  ProcessUtilitySlow (...) at utility.c:1885
...
STATEMENT: DROP SUBSCRIPTION regress_testsub;
```

This is `DROP SUBSCRIPTION` waiting (10ms poll loop, `WaitLatch`) for a logical
replication apply worker to observe its stop signal and exit -- and the worker
never does, in these two runs. The postmaster process itself survives in this
state (confirmed alive via `ps`/`gdb`); only the `pg_regress` driver eventually
loses its connection and the run is reported as hung/bailed. Full backtraces:
`patched_run1_hang_gdb_bt.txt`, `patched_run1_hang_gdb_bt2.txt` (5+ minutes
apart, identical location), `patched_run3_hang_gdb_bt.txt`.

## What this evidence supports, and what it does NOT support

**Supported, directly observed:**
- On this hardware, unmodified HEAD's `create_index`/`REINDEX CONCURRENTLY`
  buffer-lock deadlock reproduced 4/4 -- consistent with (though on different
  hardware than) the documented baseline's 3/3.
- On this hardware, the patched tree (Fix A + Fix B + the `guc.c` fix) did NOT
  hit that deadlock in 5/5 runs.
- The patched tree instead hit a DIFFERENT, previously-undocumented hang
  (logical replication worker stop, `publication`/`subscription` test group)
  in 2/5 runs, and finished with only cosmetic query-plan-shape diffs
  (`select_distinct`, `subselect`, `union`, `join` -- all EXPLAIN-output
  diffs, not wrong-answer diffs; consistent with autovacuum/ANALYZE-timing-
  dependent planner choices under threaded scheduling, a known category of
  test flakiness unrelated to correctness) in the other 3/5.

**NOT supported -- explicitly not claimed:**
- **This is NOT a claim that Fix A, Fix B, or the `guc.c` fix "fixed" the
  `create_index` deadlock.** None of the three touch buffer locking,
  checkpointing, `REINDEX`, or anything on that deadlock's call path
  (confirmed: `git diff` touches only `reloptions.c`, `xml.c`, `xml.h`,
  `xpath.c`, `xslt_proc.c`, `guc.c`; the deadlock's stack is
  `CheckPointBuffers`/`heap_index_delete_tuples`/`_bt_*`, entirely disjoint).
  The far more likely explanation is that this is a genuinely racy,
  timing-sensitive deadlock (an ABBA lock-order race depends on exact
  scheduling), and rebuilding the binary at all (different code layout,
  different addresses, different branch-predictor/cache behavior) is enough
  to perturb its timing -- on EITHER tree. This report does not have evidence
  to distinguish "the patch set changed something relevant" from "this
  deadlock's window is narrow and reproducibility is hardware/build-address
  sensitive, and I got a different draw from the RNG of scheduling."
- **This is NOT a claim that the `publication`/`subscription` hang is caused
  by Fix A/B/guc.c either.** None of the three touch logical replication,
  worker signaling, or `launcher.c`. It was simply not observed on unmodified
  HEAD because unmodified HEAD never got far enough into the schedule (it died
  at test 74, long before test 172) to reach that code at all in any of the 4
  runs -- so "unmodified HEAD doesn't show it" is uninformative; the schedule
  never exercised that path on HEAD in this experiment.
- **This is not proof that the patched tree is "better" or "worse" than
  HEAD for check-threaded-pooled overall.** Both trees are unreliable
  (deadlock/hang somewhere in the schedule); this data changes WHERE, not
  WHETHER.

## Bottom line for this task

Neither finding changes the verdict for Fix A or Fix B specifically:
- Fix A's reloptions.c assert bug is proven fixed by direct fault injection
  (see `RELOPTIONS_THREADED_UNWIND_AUDIT.md`, `.ec2/relx-20260910/fixA/`),
  independent of this suite-level exercise.
- Fix B's xml2 concurrency claim is proven by the standalone pgbench hammer
  (`.ec2/relx-20260910/fixB/`), independent of this suite-level exercise.
- `gmake check` (process mode) is unaffected either way: 245/245, both before
  and after, every time it was run (see the main report).
- The `create_index` deadlock and the `publication`/`subscription` hang are
  both recorded here as open items for whoever owns threaded-suite triage next
  (the former already is, per `THREADED_TEST_BASELINE_2026-09.md`; the latter
  is new and unrecorded elsewhere as of this writing). Neither is chased
  further in this task, per the explicit instruction to record and move on.

## Honest uncertainty

I did not have time/scope in this task to run enough trials on both trees to
statistically distinguish "these two deadlocks are both draws from noisy,
timing-sensitive races and my n=4/n=5 samples are not representative" from "the
patch set specifically shifted the failure mode." A rigorous answer would need
at least 10+ runs per tree, ideally alternated (A/B/A/B...) on identical
hardware in one sitting to control for thermal/neighbor drift, which is beyond
this task's remaining budget. I am reporting the n=4/n=5 data honestly with
its limits rather than either suppressing it or overclaiming a causal fix.
