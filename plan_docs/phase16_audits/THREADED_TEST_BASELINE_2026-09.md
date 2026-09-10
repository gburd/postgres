# Threaded regression test baseline (clean HEAD, 2026-09-09/10)

HEAD: `563e490870` (pristine `origin/xtc`, no stashed work applied).
libxtc: **v1.43.0** (`b977d21`). SUT: EC2 c6id.8xlarge (32 vCPU), autoconf build, `/mnt/nvme` (XFS).
Method: each target run 3x. Raw logs in `.ec2/baseline-2026-09-09/`.

--------------------------------------------------------------------------------
## The matrix

| target | config | r1 | r2 | r3 | verdict |
|---|---|---|---|---|---|
| `gmake check` (process) | — | **245/245** | **245/245** | — | **GREEN** |
| `check-threaded` | `carriers=0` (fiber), `io_method=sync` | 77 ok / 168 not ok | 77/168 | 77/168 | **RED, deterministic** |
| `check-threaded-pooled` | `carriers=4` (pooled default) | 77/168 | 77/168 | 77/168 | **RED, deterministic** |
| `check-threaded-workers` | `threaded_workers.conf` | 77/168 | 73/172 | 77/168 | **RED, deterministic** |

Process mode is green and unaffected. **All three threaded targets fail identically and
deterministically at the same place**, which is the important and previously unrecorded fact.

## This corrects two assumptions I was operating on

1. **I expected `check-threaded` to be intermittent** (the fiber lost-wake hang is ~29-60 % per
   run). It is not: 3/3 identical. So the threaded-target failure is **not** the libxtc lost-wake
   bug we are blocked on.
2. **I expected `check-threaded-pooled` to be the healthy lane** and told another agent to gate its
   work on it. **It fails identically.** That guidance was wrong, and any "validated on
   check-threaded-pooled" claim on this branch needs re-reading against this baseline.

## Anatomy of the failure: one trigger, then a cascade

```
ok 73        ...
not ok 74    + create_index        1426 ms     <-- THE TRIGGER (real diff)
ok 75        + create_index_spgist 1056 ms
ok 76..78    (still fine)
not ok 79    + create_aggregate       3 ms     <-- cascade begins
not ok 80    + create_function_sql    3 ms
... ~155 tests all "not ok" at 3 ms ...
not ok 245   - tablespace             3 ms
pg_ctl: could not send stop signal (PID: 67577): No such process
Bail out!
```

The ~155 failures at **3 ms** never ran: the postmaster is gone by then. So this is **1 real
failure + 167 collateral**, not 168 problems. `create_index`'s diff is entirely `-` lines (expected
output absent), i.e. the session died mid-file; the last statements before the gap are
`REINDEX TABLE CONCURRENTLY` / partitioned-index REINDEX.

## Root cause: an ABBA deadlock on buffer 8791 between the checkpointer and a backend fiber

From the captured `gdb thread apply all bt` (`hang-evidence-noassert-check-threaded-r1/`):

**Thread A — checkpointer, a raw pthread, blocking in a POSIX semaphore:**
```
PGSemaphoreLock (pg_sema.c:320)
ProcSemaphoreWaitCallback (proc.c:2293)
ProcWaitOnSemaphore (proc.c:2444)
BufferLockAcquire (buffer=8791, mode=BUFFER_LOCK_SHARE_EXCLUSIVE)  bufmgr.c:6190
FlushUnlockedBuffer -> SyncOneBuffer -> BufferSync -> CheckPointBuffers
CreateCheckPoint -> CheckpointerMain
  ... backend_thread_run_worker (launch_backend.c:2368) -> pg_thread_start
```

**Thread B — a backend fiber, parked on its eventfd:**
```
xtc_pg_wait_fd (fd=200, interest=2, timeout_ms=-1)  pg_xtc_carrier.c:1420
ProcSemaphoreWaitFiber (proc.c:2364)
ProcWaitOnSemaphore (proc.c:2429)
BufferLockAcquire (buffer=8791, mode=BUFFER_LOCK_SHARE)  bufmgr.c:6190
LockBuffer -> heap_index_delete_tuples (heapam.c:8513)
_bt_delitems_delete_check -> _bt_simpledel_pass -> _bt_delete_or_dedup_one_page
_bt_findinsertloc -> _bt_doinsert -> btinsert -> index_insert
CatalogIndexInsert -> CatalogTupleInsert -> CreateConstraintEntry -> StoreRelCheck
```

**Same buffer, 8791. Both waiting. Nobody releases.** Meanwhile all 31 carrier loops sit idle in
`io_uring_wait_cqes` — the runtime is alive and has nothing to do, which is exactly what a mutual
wait looks like from the loop's perspective.

Thread-frame census across the dump: 31x `__xtc_loop_step`/`xtc_io_poll`/`_io_uring_get_cqe`
(idle loops), 2x `BufferLockAcquire`+`ProcWaitOnSemaphore` (the deadlock pair), 3x
`backend_thread_entry`, 2x `WaitEventSetWait`.

### Why this is a distinct bug from the fsync lost-wake
* Different primitive: **buffer lock**, not `xtc_aio_fdatasync`.
* Different waiters: one is a **non-fiber pthread** (checkpointer) in a real `sem_wait`; the
  fdatasync bug is fiber-only.
* **Deterministic**, not ~29-60 %.
* Independent of `io_method` (these configs use `sync`, not `xtc`).

The asymmetry is the suspicious part: `ProcWaitOnSemaphore` forks by waiter shape —
`ProcSemaphoreWaitFiber` (eventfd park) for a fiber, `PGSemaphoreLock` for anything else — and
`ProcWakeSemaphore` does handle both shapes deliberately (fd-write for `sem_fiber_backed`, never
over-posting `proc->sem`; `PGSemaphoreUnlock` otherwise). So this is likely a genuine **lock-order
inversion** rather than a plain missed wake. I am stating that as the leading hypothesis, not a
conclusion: I have not yet established which side acquired first, nor whether
`BUFFER_LOCK_SHARE_EXCLUSIVE` vs `SHARE` ordering here is legal under upstream's buffer-lock
contract.

## Caveat on part of the evidence (recorded, not hidden)
Two of the captured `hang-evidence-*` postmaster logs contain
`could not fork new process for connection: Function not implemented`, the documented
server-reuse-after-timeout-kill signature. Those two captures are therefore **not admissible** as
independent evidence. The four main-run logs (`check_threaded_r1`, `check_threaded_pooled_r1`,
`check_threaded_workers_r1`) contain **zero** occurrences, so the 77/168 baseline itself is clean;
only the affected hang dumps are suspect. The backtrace quoted above is from a capture without
that signature.

## Consequences for how we work
1. **`gmake check` (process) is the only currently trustworthy green gate.** Use it as the
   must-not-regress bar.
2. **No threaded target can serve as a pass/fail gate until this deadlock is fixed** — everything
   downstream inherits a RED baseline. A change that leaves it at 77/168 is *neutral*, not broken;
   a change that moves it is what matters. Compare against 77/168 explicitly.
3. This is now the top non-libxtc blocker: it is ours, deterministic, and reproducible in ~2
   minutes, unlike the libxtc hang where we wait on an upstream instrument.

## Next steps (not done here)
* Determine acquisition order for buffer 8791 and whether checkpointer's
  `FlushUnlockedBuffer`/`BufferLockAcquire(SHARE_EXCLUSIVE)` can legally block behind a fiber
  holding `SHARE`.
* Decide whether an in-tree server-owned worker like the checkpointer should be a fiber (AGENTS.md
  already states normal threaded mode should eventually run server-owned workers as runtime-owned
  worker fibers, which would remove the raw-pthread-in-sem_wait shape entirely).
* Narrow the repro below full `check-threaded` -- `create_index` alone, or just the
  `REINDEX CONCURRENTLY` + concurrent-checkpoint pair.


--------------------------------------------------------------------------------
## UPDATE 2026-09-10: the 77/168 baseline is a STACK of at least five independent bugs

Validating unrelated fixes on a second box (`c6id.4xlarge`, vs this baseline's `c6id.8xlarge`) peeled
the failure open. The threaded suite does not have "a" bug; it has a queue of them, each masking the
next. Fixing one just moves the wall.

| # | bug | status |
|---|---|---|
| 1 | `guc.c` `ThreadedGUCUnlock()` unconditional `RESUME_INTERRUPTS()` -- **any invalid `SET` crashes the server** in threaded mode | **FIXED** `491c47d65e` |
| 2 | `reloptions.c` `ThreadedRelOptionsUnlock()` same unwind hazard | **FIXED** `a6e6d00abc` |
| 3 | `dfmgr.c` rendezvous-hash/file_list race (needed the same unwind guard) | **FIXED** `9db33b7674` |
| 4 | checkpointer/backend-fiber **buffer-lock ABBA deadlock** on `create_index` (`REINDEX CONCURRENTLY`) | **OPEN** -- documented above |
| 5 | `mcxt.c` `Assert("parent->firstchild == context")` in `MemoryContextSetParent` <- `MemoryContextDelete` <- `AtEOXact_RI` (RI trigger teardown), hit in the `plpgsql` group | **OPEN, NEW** -- `.ec2/relx-20260910/fixA/UNRELATED_mcxt_ri_triggers_crash_not_fixed.md` |
| 6 | `DROP SUBSCRIPTION` hangs in `logicalrep_worker_stop_internal` (worker never observes SIGTERM), `publication`/`subscription` group | **OPEN, NEW** -- 2/5 runs, identical stack 5 min apart |

Bugs 1-3 are the SAME defect in three places: a threaded critical section pairing
`HOLD_INTERRUPTS()` with an unconditional `RESUME_INTERRUPTS()`, while `errfinish()` resets
`InterruptHoldoffCount` to 0 during any error unwind -- so an ordinary SQL error crashes the
server. All three now use the identical `if (InterruptHoldoffCount > 0) InterruptHoldoffCount--;`
guard. **Bug 1 was pre-existing since `da1a57e156` (2026-07-16), ~2 months.** That single fix is
why the suite can now reach test ~172-218 instead of dying at 74.

### A striking correlation that I am NOT treating as causal
On the second box, unmodified HEAD hit the `create_index` deadlock **4/4**; the patched tree
**0/5**. Tempting, and wrong to bank: none of the three fixes touch buffer locking, checkpointing,
`REINDEX`, or `launcher.c` (verified -- `git diff` over those commits touches
`reloptions.c`/`guc.c`/`xml*`/`xpath.c`/`xslt_proc.c` and nothing on the deadlock's
`CheckPointBuffers`/`heap_index_delete_tuples`/`_bt_*` path). Bug 4 is an ABBA lock-order race, so
its window is scheduling-sensitive; **rebuilding the binary at all changes code layout and
timing**, which is sufficient to explain a different draw. The honest statement: this data changes
**where** the suite dies, not **whether**.

Likewise "unmodified HEAD never showed bugs 5 and 6" is uninformative -- HEAD died at test 74 and
never reached tests 172/218 in any run, so those paths were simply never exercised.

### Revised guidance
* **`gmake check` (process, 245/245) remains the only trustworthy gate.**
* Do not compare threaded runs against `77/168` any more -- with bugs 1-3 fixed the wall has moved.
  Re-baseline before using any threaded target as a gate.
* Expect **plan-shape diffs** (`select_distinct`, `subselect`, `union`, `join`) on runs that now get
  far enough to finish. These are EXPLAIN-output differences, not wrong answers -- consistent with
  autovacuum/ANALYZE timing under threaded scheduling. Do not chase them as correctness bugs
  without checking the diff is cosmetic.
