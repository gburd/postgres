# Diagnosis: immediate shutdown (SIGQUIT/SIGKILL) cannot terminate a wedged fiber backend

Date: 2026-09-15. Diagnosed from the observed symptom (seven wedged servers survived SIGTERM *and*
SIGQUIT and needed an external SIGKILL) plus the shutdown-delivery code path and the existing
deferral note in `proc.c`.

## Symptom, with log proof
Seven wedged threaded servers ignored `pg_ctl -m fast` (SIGTERM) and `-m immediate` (SIGQUIT). One
server's log shows PostgreSQL's own last-resort escalation running to completion and still not working:

```
LOG:  received smart shutdown request
LOG:  received immediate shutdown request
LOG:  issuing SIGKILL to recalcitrant children      <-- the "last measure to get them unwedged"
```
The postmaster then stayed alive until I sent SIGKILL from outside. So the escalation designed
specifically for stuck children (`postmaster.c` ~2072-2090, "This is a last measure to get them
unwedged") is INEFFECTIVE in threaded mode.

## Root cause: every forceful signal degrades to a cooperative request
For a fiber-backed child, `signal_child()` does not signal anything. It takes this branch
(`postmaster.c`):

```c
if (PostmasterChildHasLogicalBackendPublication(pmchild))
{
    if (thread_child_signal_interrupt(pmchild, signal, &interrupt))
        (void) PostmasterChildRaiseThreadInterrupt(pmchild, interrupt);
    return;                       /* <-- never reaches kill() */
}
```

and `thread_child_signal_interrupt()` maps the three *forceful* signals to the same **cooperative**
interrupt as everything else:

```c
case SIGQUIT:
case SIGKILL:
case SIGABRT:
    *interrupt = PG_BACKEND_INTERRUPT_PROC_DIE;
    return true;
```

`PostmasterChildRaiseThreadInterrupt()` then just calls `SendInterrupt(logical_backend, interrupt)` --
it sets a flag and wakes the target.

That is a **category error for immediate shutdown**. In process mode the semantics differ by kind:
- SIGTERM = "please wind down" -> cooperative, requires the child to reach `CHECK_FOR_INTERRUPTS()`;
- SIGQUIT = "die now, skip cleanup" -> the child's quickdie handler runs, and even if the child is
  compromised the kernel guarantees delivery;
- SIGKILL = "the kernel removes you, no cooperation possible".

In threaded mode all three become "set a flag and hope the fiber notices". A fiber wedged inside a lock
wait (our current write-path wedge: parked in `ProcSemaphoreWaitFiber` on its `sem_wake_fd`, waiting on a
buffer content lock nobody releases) never returns to a `CHECK_FOR_INTERRUPTS()` and so never observes
the flag. There is no layer beneath it that can force the issue, because `kill()` is unreachable for a
fiber and nothing calls libxtc's async-kill.

Consequences:
1. `pg_ctl -m immediate` cannot stop the cluster; operators must SIGKILL the postmaster, which is
   exactly the outcome immediate shutdown exists to avoid.
2. The SIGKILL-after-`SIGKILL_CHILDREN_AFTER_SECS` safety net is dead code for fiber children.
3. Crash-escalation paths that rely on "SIGQUIT everyone, then reinitialise" cannot make progress if any
   participant is wedged -- so one stuck fiber can block recovery for the whole cluster, not just itself.

## Why it was left this way (and it was a considered choice, not an oversight)
`proc.c`'s `ProcSemaphoreWaitFiber` header carries an explicit deferral:

> *DEFER-WITH-INVARIANT (Blocker 1): a backend fiber is never `xtc_exit_pid`'d mid-wait in this phase
> (nothing async-kills a backend fiber; verified), so `xtc_proc_wait_fd`'s
> `kill_pending -> xtc_exit_self` longjmp cannot fire here. A future supervisor-cancellation phase MUST
> make this park kill-safe (uninterruptible park or `xtc_proc_at_exit` LWLockReleaseAll) before it may
> async-kill a parked backend fiber.*

So the invariant "nothing async-kills a backend fiber" is what makes the current park correct, and it is
still true (`xtc_exit_pid` appears nowhere in `src/` except that comment). The gap is that the invariant
was never revisited, and immediate shutdown quietly inherited the cooperative-only path.

## The primitive we need already exists in libxtc
`xtc_exit_pid(xtc_pid_t target, int reason)` -- libxtc's async kill -- is exactly the missing forceful
mechanism, and notably it is **scope-aware**: `xtc_scope` finalizers run LIFO "on the normal
`xtc_scope_close`, AND on an error return, `xtc_exit_self`, an async kill (`xtc_exit_pid`), or a
cancellation". So libxtc already provides the "resources are released even on a forced kill" guarantee
that a safe implementation requires. We do not use it anywhere today.

## What a fix requires (in dependency order)
1. **Make the fiber park kill-safe** -- the prerequisite `proc.c` names. Either mark the park
   uninterruptible for the window where it holds the arm/disarm invariant, or register an
   `xtc_proc_at_exit` handler that performs `LWLockReleaseAll()` + buffer-lock release + the
   `sem_fiber_armed` disarm, so an async kill cannot leave shared state corrupt. This is the real work;
   an async kill of a fiber holding a buffer content lock or a spinlock would be worse than the hang.
2. **Record each backend fiber's `xtc_pid_t` where the postmaster can see it.** `PMChild` currently has
   no `xtc_pid_t` (only the supervisor's own pids are tracked, in `g_xtc_sup_pid[]`), so the postmaster
   cannot currently name the fiber it wants to kill.
3. **Split the signal mapping** in `thread_child_signal_interrupt()`: keep the cooperative interrupt for
   SIGTERM/SIGINT, but make SIGQUIT/SIGKILL/SIGABRT escalate to `xtc_exit_pid()` on the target fiber
   (after 1 and 2). SIGABRT should additionally keep its "dump core" character where feasible.
4. **Keep a hard backstop.** If a fiber cannot be killed even by `xtc_exit_pid` (e.g. it is spinning in
   an uninterruptible section), the postmaster must escalate to taking the whole process down --
   `_exit()`/`abort()` of the carrier process is the threaded analogue of "the kernel removes you", and
   is consistent with the project's existing stance that a genuine crash in threaded mode fail-stops the
   process.

## Recommended sequencing
Do **not** wire up `xtc_exit_pid` before step 1. Today's hang is ugly but safe (state is intact and
`SIGKILL` of the process still works); an async kill that interrupts a lock-holding fiber risks shared
memory corruption, which is strictly worse. Step 4 alone is cheap and would restore the operator
guarantee that `-m immediate` always terminates the cluster: after
`SIGKILL_CHILDREN_AFTER_SECS`, if any fiber child is still alive, the postmaster fail-stops the process
rather than waiting forever. That is a small, safe change and I would land it first.

## Note on the relationship to the current wedge
This is independent of the buffer-lock wedge and worth fixing on its own merits: even with the wedge
fixed, any future bug that parks a fiber indefinitely would again make the cluster unstoppable. The
wedge is the trigger; this is the missing safety net.


--------------------------------------------------------------------------------
## Correction 2026-09-15: libxtc is NOT missing the machinery; the blocker is PG's crash contract

My original text above said the park "is not kill-safe" and implied libxtc lacked what we need. That was
wrong on the first count and unfair on the second. Correcting it, and recording the real blocker.

**libxtc v1.45.0 already provides everything needed for cancellation-SAFE release:**
- `__xtc_proc_kill_deliver()` honours a mask -- `mask_depth == 0` unwinds via `xtc_exit_self`;
  `mask_depth > 0` DEFERS and latches the reason, firing when the mask drops to 0.
- `xtc_uncancelable()` IS the "uninterruptible park" this doc asked for.
- `xtc_cancel_poll()`, `xtc_cancel_requested()`, `xtc_proc_at_exit()` (where LWLockReleaseAll would go),
  and `xtc_scope` finalizers that run on async kill.

**The actual blocker is architectural, on our side.** In process mode `quickdie()` deliberately abandons
shared state -- it does NOT release locks or unwind -- and that is safe ONLY because the postmaster then
does `all server processes terminated; reinitializing`: the whole shared memory segment is discarded and
the cluster replays WAL. Nothing ever reads the abandoned state.

Threaded mode has no such boundary. Killing ONE fiber leaves the same address space and every other
session running, so an async kill at an arbitrary park point can leave a partially-written WAL record (a
fiber killed inside XLogInsertRecord holding a WAL insertion slot), a partially-updated shared buffer
(killed mid-heap_update), or a broken cross-structure invariant. `LWLockReleaseAll()` fixes lock
OWNERSHIP; it cannot fix data MID-MUTATION.

And masking cannot rescue the wedged case: for a stuck fiber the dangerous window is exactly where it is
stuck, so `xtc_uncancelable()` would defer the kill FOREVER -- the hang we are trying to break. Masking
makes cancellation safe; it does not make a stuck critical section killable. Different problems.

**Therefore the process-level fail-stop we shipped is not a workaround -- it is the threaded analogue of
PostgreSQL's own contract.** Process mode's answer to "a backend is stuck and must die now" is also "take
the process down and recover from WAL"; we do the same at the granularity a shared address space forces.
It stays as the final backstop regardless of what per-fiber kill later becomes possible.

Filed `/tmp/libxtc-async-kill-of-wedged-pg-backend-fiber-2026-09-15.md` asking for the things that would
let a per-fiber kill beat a process fail-stop: (1) a kill deadline / delivered-vs-deferred-vs-timed-out
RETURN from `xtc_exit_pid` (today it cannot tell us which happened); (2) supervisor-side visibility of
`mask_depth`/`mask_deferred` via `xtc_proc_info`/`xtc_inspect_procs`, so a supervisor can distinguish
"unwinding" from "wedged inside a mask" -- useful on its own, not just for killing; (3) documented
guidance on cancelling a fiber that mutates state other fibers keep using; (4) noted-only: a fiber
group/arena whose shared allocations can be discarded wholesale on kill.

So the earlier "obvious fix" is NOT available, and the reason is worth understanding rather than
routing around: it is a genuine impedance mismatch between PG's crash-recovery model and per-fiber
cancellation, not a gap in libxtc's API.
