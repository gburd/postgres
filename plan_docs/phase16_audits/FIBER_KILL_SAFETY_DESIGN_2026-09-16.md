# Design: making a backend fiber safe to async-kill (the quickdie-equivalent resource tracking)

Date: 2026-09-16. Written while blocked on the libxtc io_uring ring bug. This is the prerequisite the
owner asked for after the `quickdie()` discussion, and the prerequisite `xtc_proc(3)` now documents
("do not async-kill a fiber that can be interrupted mid-mutation of shared state").

## The good news: PostgreSQL already has the release list
I expected to have to invent an inventory of abandonable resources. It already exists, ordered and
battle-tested, in `AbortTransaction()` (xact.c ~2890-2930) -- the path a normal `ereport(ERROR)` unwind
takes:

```
AtAbort_Memory();              AtAbort_ResourceOwner();
LWLockReleaseAll();            /* "as quickly as possible" */
WaitLSNCleanup();
UnlockBuffers();               /* buffer content locks + pins */
XLogResetInsertion();          /* WAL record-construction buffers */
ConditionVariableCancelSleep();
LockErrorCleanup();            /* heavyweight lock wait cleanup */
```

So the resources a killed fiber would abandon are exactly the ones PG already knows how to release. That
makes the `xtc_proc_at_exit` hook straightforward: run this same sequence. It is NOT a new mechanism we
must invent and maintain -- it is the existing abort path, reached from a different trigger.

## The bad news: one window is genuinely unrecoverable, and it is not on that list
`XLogResetInsertion()` resets the *backend-local record-construction staging* (`registered_buffers`,
`rdatas`). It does NOT undo a record already being copied into the shared WAL buffers. Once
`XLogInsertRecord()` has reserved space and started `CopyXLogRecordToWAL()`, the bytes are in shared
memory and no cleanup callback can retract them -- a killed fiber there leaves a torn record that every
other session and recovery will read.

The same is true of a buffer page halfway through modification: releasing the content lock (which
`UnlockBuffers()` does) publishes an inconsistent page rather than fixing it.

**This is why the answer cannot be "register more finalizers".** Release-on-exit handles ownership; it
cannot handle mid-mutation.

## The insight: PostgreSQL already MARKS every such window
The unrecoverable windows are exactly PG's critical sections. The WAL case is fully bracketed:

```
xlog.c:904   START_CRIT_SECTION();
xlog.c:908     WALInsertLockAcquire();
xlog.c:1007    CopyXLogRecordToWAL(...);     <-- the unrecoverable copy
xlog.c:942/…  END_CRIT_SECTION();
```

`CritSectionCount` is a nesting **counter** -- structurally identical to libxtc's `mask_depth` -- and in
our tree it is already per-session (`PgCurrentCritSectionCountRef()`), so it is fiber-correct today.
There are 124 `START_CRIT_SECTION()` sites in `src/backend`.

So the correct design is: **bridge PG's existing critical-section counter to libxtc's cancellation
mask.** Every window PG already declares unsafe becomes a window in which libxtc defers the kill. The
guarantee becomes mechanical instead of aspirational, with no new invariant to maintain and no new list
to keep in sync.

## Blocked on one small libxtc addition (requested)
`xtc_uncancelable()` is callback-shaped (`body(void *ud)`), but PG's critical sections are macro pairs
around arbitrary straight-line code that `goto`s, `return`s and `longjmp`s (via `ereport`). Converting
124 sites into callbacks would mean hoisting frame state into structs and rewriting control flow in the
most safety-critical code in the tree -- a change I should not make and that upstream would not take.

Internally the mask is already just a counter (`__mask_depth_inc` / `body()` / `__mask_depth_dec` /
`__mask_drain`), so I requested the paired form in
`/tmp/libxtc-paired-cancellation-mask-api-request-2026-09-16.md`:

```c
xtc_mask_enter();   /* ++mask_depth */
xtc_mask_leave();   /* --mask_depth, then drain any latched kill */
```

With it the bridge is two macros and covers all 124 sites with zero code motion:

```c
#define START_CRIT_SECTION() \
    do { CritSectionCount++; if (xtc_in_backend_fiber) xtc_mask_enter(); } while (0)
#define END_CRIT_SECTION() \
    do { Assert(CritSectionCount > 0); CritSectionCount--; \
         if (xtc_in_backend_fiber) xtc_mask_leave(); } while (0)
```

## The resulting supervisor decision procedure
With the bridge plus v1.47.0's `xtc_exit_pid_deadline` and `mask_depth`/`mask_deferred` in
`xtc_proc_info`:

| observed | meaning | action |
|---|---|---|
| `mask_depth == 0` | not mid-mutation | `xtc_exit_pid` -- safe to kill this fiber alone |
| `mask_depth > 0` | inside a critical section | wait; the kill is honoured on exit |
| `mask_depth > 0`, `mask_deferred != 0`, no progress | wedged inside a critical section | escalate to the process-level fail-stop |

That third row is the case our current `ExitPostmaster(1)` backstop already handles, and it is the stance
`xtc_proc(3)` now recommends -- so the backstop stays as the final tier rather than being replaced.

## Plan (once the paired API lands)
1. Bridge `START_CRIT_SECTION`/`END_CRIT_SECTION` (2 macros). Guard on `xtc_in_backend_fiber` so process
   mode is byte-for-byte unchanged.
2. Register an `xtc_proc_at_exit` handler running the `AbortTransaction` release sequence above, so a
   kill delivered *outside* a critical section releases ownership correctly.
3. Regression test: a fiber inside `START_CRIT_SECTION()` must report `mask_depth > 0` and
   `xtc_exit_pid_deadline` must return `XTC_KILL_DEFERRED`; outside, `XTC_KILL_DELIVERED`. That converts
   "never killed mid-mutation" from a documented hope into an enforced invariant.
4. Only then teach the postmaster to try per-fiber kill before the process-level fail-stop.

## Why this is worth doing even though the wedge is a libxtc bug
The current wedge will be fixed upstream, but *any* future bug that parks a fiber indefinitely recreates
the same operator problem: a cluster that cannot be stopped and a session that cannot be killed. This
work makes the runtime able to shed one bad session instead of losing the process -- which is the
BEAM-style self-healing property this project is aiming for, and it is independent of whatever caused the
park.
