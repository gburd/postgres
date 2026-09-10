# reloptions.c Threaded Critical Section -- Unwind Assert Fix + Raw-pthread-vs-amutex Audit

Status: fix landed + audit. Companion to the `dfmgr.c` fix (commit `9db33b7674`)
and the sibling `guc.c` fix landed alongside it (same class of bug, found while
validating this one under `gmake check-threaded-pooled`).

## 1. The bug (confirmed, with one correction to the original diagnosis)

`ThreadedRelOptionsUnlock()` (pre-fix) had `Assert(ThreadedRelOptionsMutexDepth
> 0)` followed by an unconditional `RESUME_INTERRUPTS()`. Both trip because
`errfinish()` (elog.c) unconditionally resets `InterruptHoldoffCount` to 0
while unwinding ANY error, and that reset can run BEFORE the `PG_CATCH` unlock
does.

The original diagnosis named four candidate trigger sites. Reproduction (EC2,
`c6id.4xlarge`, cassert build, `multithreaded=on pooled_protocol_carriers=4`,
libxml2 2.13.8 / libxslt 1.1.43 / libxtc v1.43.0) via a throwaway probe
extension (`reloptprobe`, not shipped) confirmed:

- **Site 1** (`add_reloption_kind()`'s "user-defined relation parameter types
  limit exceeded" ereport at line ~919) does **NOT** trigger the bug. That
  code calls `ThreadedRelOptionsUnlock(locked)` as an ordinary function call
  BEFORE `ereport(ERROR)`, not during error unwind -- `InterruptHoldoffCount`
  is already back to its pre-lock value by the time `errfinish()` runs, so
  there is nothing to trip. Exercised directly (25 iterations past
  `RELOPT_KIND_MAX`) on the pre-fix build: raised the expected SQL error,
  no crash, both before and after the fix. **This corrects the original
  diagnosis, which listed this as a trigger site.**
- **Site 3/4** (a palloc/pstrdup failure inside `allocate_reloption()`'s
  `PG_TRY`, line ~1083, whose `PG_CATCH` calls `ThreadedRelOptionsUnlock(locked)`
  during unwind, line ~1106) **DOES** trigger the bug. Reproduced live:
  `TRAP: failed Assert("InterruptHoldoffCount > 0"), File:
  "../src/backend/access/common/reloptions.c", Line: 670` via a 2GB reloption
  name forcing `pstrdup()` past `MaxAllocSize` inside the lock. Confirms the
  diagnosis for this site precisely (matched down to the exact source line of
  the `RESUME_INTERRUPTS()` call).
- **Site 2** (`allocate_reloption()`'s `elog(ERROR, "unsupported reloption
  type %d")` in the `switch` default case) is **unreachable under the lock**:
  that `elog` runs in the `switch` statement, which is BEFORE
  `ThreadedRelOptionsLock()` is even called a few lines later in the same
  function. Not a trigger site; the lock is not yet held. This also corrects
  the original diagnosis.
- The other two `elog(ERROR, "unsupported reloption type %d")` sites
  (`parse_one_reloption()` line ~2074, `fillRelOptions()` lines ~2213/2222) are
  in functions that never call `ThreadedRelOptionsLock()`/`Unlock()` at all --
  irrelevant to this bug.

Net: **one of the four named sites (palloc/pstrdup-in-`PG_CATCH`) is a
confirmed, reproduced trigger; one (the limit-exceeded ereport) is not, because
it unlocks before erroring rather than during unwind; one (the type-switch
elog) is unreachable under the lock at all.** The fix itself (drive off the
depth counter; tolerate `InterruptHoldoffCount` already at 0) is correct and
general regardless of which specific site fires it -- it is not scoped to only
the sites enumerated above, and covers `add_reloption()`'s own `PG_CATCH`
(custom_options array growth failure) and `init_string_reloption()`'s
`PG_CATCH` (default-value strdup failure) the same way, neither of which were
separately probed but share the identical unlock-during-unwind shape.

## 2. `RelOptionsGlobalMemoryContext()`'s remaining `Assert(...MutexDepth > 0)`

Verified NOT an unwind-path assert, so correctly left in place. All five call
sites (`initialize_reloptions()` at line 856, `add_reloption()` at 972,
`allocate_reloption()` at 1084, `init_string_reloption()` at 1420, and the two
callers of `initialize_reloptions()` itself -- `parseRelOptions()` at 1877 and
`AlterTableGetRelOptionsLockLevel()` at 2529) are reached ONLY from inside a
still-active (non-unwinding) `ThreadedRelOptionsLock()`/`PG_TRY` region, never
from a `PG_CATCH` block. The assert fires only if that invariant is violated
by a future bug elsewhere, which is exactly what an assert should catch.

## 3. Raw pthread_mutex vs fiber-aware xtc_amutex -- can any reloptions locked region park?

**Answer: No. The raw `pthread_mutex_t` in `reloptions.c` is correct as-is; it
does NOT need to become an `xtc_amutex` like `guc.c`/`dfmgr.c` did.**

`dfmgr.c`'s critical section had to move to `xtc_amutex` because its locked
region calls `call_module_init_function()`, which runs a loaded module's
arbitrary `_PG_init()` -- and nearly every real extension's `_PG_init()` calls
`DefineCustomXXXVariable()`, entering the GUC critical section, which itself
parks on contention under `USE_XTC_CARRIER`. A raw pthread mutex held across
that nested park would starve another fiber on the same carrier.

Every one of the five locked regions in `reloptions.c` was read line-by-line
(`add_reloption_kind()`, `add_reloption()`, `allocate_reloption()`,
`parseRelOptions()`'s `initialize_reloptions()` call,
`AlterTableGetRelOptionsLockLevel()`'s `initialize_reloptions()` call, and
`init_string_reloption()`'s default-value strdup). Their bodies consist
exclusively of:

- pointer/array bookkeeping (`relOpts[j] = ...`, `custom_options[n++] = ...`);
- `palloc()` / `repalloc()` / `pstrdup()` / `MemoryContextAlloc()` /
  `MemoryContextStrdup()` / `MemoryContextSwitchTo()` -- confirmed by grep of
  `src/backend/utils/mmgr/{aset,generation,mcxt}.c` to contain no
  `USE_XTC_CARRIER`, `xtc_yield`, `xtc_park`, or `xtc_amutex` reference
  anywhere; these are pure CPU allocation with no lock that yields;
- `strlen()` / `strncmp()` / a `DoLockModesConflict()` table lookup
  (`src/backend/storage/lmgr/lock.c`) -- a plain array/bitmask check, no I/O,
  no lock acquisition, no wait.

None of the five call `add_reloption_kind()`-family functions, load a shared
library, run extension code, touch SPI, acquire an LWLock/heavyweight lock, or
call `WaitLatch`/`WaitEventSetWait`. `add_reloption_kind()` and
`add_reloption()` ARE themselves called from extension `_PG_init()` functions
(`contrib/bloom/blutils.c`, `src/test/modules/dummy_index_am/dummy_index_am.c`)
-- but that is the OUTSIDE-in direction (dfmgr's locked region calls into
`_PG_init()`, which calls into reloptions' lock); reloptions' own locked region
never calls back into dfmgr, the GUC critical section, or anything else that
parks. There is no nesting hazard in this direction.

**Conclusion: the raw `pthread_mutex_lock`/`unlock` in `reloptions.c` is fine
as shipped.** Converting it to `xtc_amutex` would be speculative hardening
against a hazard that does not exist today, contradicting the task's explicit
instruction not to convert speculatively. If a future reloptions locked region
is extended to call something that parks (a hypothetical validator callback
that itself takes the GUC lock, say), that would need to be re-audited at that
time -- but no such call exists today.

## 4. A third, sibling instance of the same bug class found (and fixed) while validating this one

While running `gmake check-threaded-pooled` to validate this fix (and the
independent xml2 fix, Fix B), the identical bug shape was found in
`ThreadedGUCUnlock()` (`guc.c`), UNRELATED to reloptions.c or xml2 and
pre-existing on `HEAD` (confirmed by reverting the reloptions/xml/guc diff on
the EC2 tree and reproducing the identical crash on unmodified `HEAD`). Any
ordinary `SET` statement whose value fails validation (which the regression
suite's `guc.sql`/`subselect.sql`/etc. exercise routinely) crashed the server
with `TRAP: failed Assert("InterruptHoldoffCount > 0"), File: "guc.c", Line:
245` -- blocking `check-threaded-pooled` entirely, since virtually every SQL
statement touches a GUC nesting level somewhere. Fixed with the identical
established pattern (tolerate `InterruptHoldoffCount` already at 0). See
`.ec2/relx-20260910/fixA/guc_c_sibling_bug_before_crash.log` for the full
crash evidence and `src/backend/utils/misc/guc.c`'s `ThreadedGUCUnlock()` for
the fix. This is now the third of three known instances of this bug class
(`dfmgr.c` fixed in 9db33b7674, `reloptions.c` fixed here, `guc.c` fixed here);
a grep for the pattern (`RESUME_INTERRUPTS();` co-located with a
`Threaded*Lock`/`Threaded*Unlock` pair) across `src/backend/` finds no further
unfixed instances as of this writing.

## 5. A third, DIFFERENT, unrelated bug found and NOT fixed

`gmake check-threaded-pooled`'s full `parallel_schedule` also crashes on an
unrelated, pre-existing bug: `TRAP: failed Assert("parent->firstchild ==
context"), File: "mcxt.c", Line: 694` via `AtEOXact_RI() ->
MemoryContextDelete() -> MemoryContextSetParent()` -- a memory-context
lifecycle assertion in the RI (foreign-key trigger) fast-path metadata
cleanup, hit by the `plpgsql`/`domain`/`rangefuncs` parallel test group. This
is NOT the `InterruptHoldoffCount` bug class, is unrelated to any file touched
by this fix or Fix B, and is out of scope. It blocks the FULL threaded
`parallel_schedule` from completing but does not block validation of Fix A or
Fix B specifically (a narrower schedule of
`test_setup`+`reloptions`+`guc`+`xml`, and xml2's own contrib check, both ran
clean under the identical `threaded_pooled.conf`). See
`.ec2/relx-20260910/fixA/UNRELATED_mcxt_ri_triggers_crash_not_fixed.md` for the
full trace and analysis. Not chased further here, per the task's explicit
instruction to note known/unrelated bugs rather than chase them.

## 6. Correction: check-threaded-pooled is not a reliable pass/fail gate at all

A subsequent baseline measurement (commit `d4f238ebaf`,
`plan_docs/phase16_audits/THREADED_TEST_BASELINE_2026-09.md`) established that
**all three threaded regress targets fail deterministically on unmodified
HEAD** at 77 ok / 168 not ok, root-caused to a pre-existing checkpointer/
backend-fiber buffer-lock deadlock during `create_index`'s `REINDEX
CONCURRENTLY`, unrelated to reloptions.c, xml2, or guc.c. That correction
retroactively changes how to read Section 4 above: the `guc.c` bug was a real,
independently-confirmed bug (crashes on an ordinary `SET` statement, with or
without the suite's pre-existing deadlock), but "blocking check-threaded-pooled
entirely" overstated its role -- the suite was already unable to complete
end-to-end on unmodified HEAD for an unrelated reason before this fix existed.

A direct comparison (unmodified HEAD vs the patched tree, 4 vs 5 runs, same
EC2 instance) found: unmodified HEAD hit the documented `create_index`
deadlock 4/4; the patched tree (Fix A + Fix B + this `guc.c` fix) did not hit
it in 5/5, instead sometimes (2/5) hitting a different, previously
undocumented hang in logical-replication worker shutdown
(`publication`/`subscription` test group), and otherwise (3/5) finishing with
only cosmetic query-plan-shape diffs. None of the three fixed files touch
buffer locking, `REINDEX`, or logical replication, so this is recorded as an
observed correlation, explicitly NOT a claimed causal fix for either
suite-level issue. Full data:
`.ec2/relx-20260910/threaded-pooled-comparison/COMPARISON_SUMMARY.md`.
