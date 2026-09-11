# Why the supervision tree does NOT respawn aux workers on a genuine crash

Date: 2026-09-11. Verified in source on this branch (`17ca62818b`).

This records a boundary that will otherwise be re-litigated, because "BEAM-style self-healing"
naturally invites "so why doesn't the supervisor just restart the crashed walwriter?"

## What process-mode PostgreSQL actually does

`cleanup_wal_writer_child()` in `src/backend/postmaster/postmaster.c`, in full:

```c
	ReleasePostmasterChildSlot(WalWriterPMChild);
	WalWriterPMChild = NULL;
	if (!EXIT_STATUS_0(exitstatus))
		HandleChildCrash(0, exitstatus, _("WAL writer process"));
	return true;
```

So **any** exit that is not a clean status-0 goes to `HandleChildCrash`, whose non-clean path is
`HandleFatalError(PMQUIT_FOR_CRASH, true)` — SIGQUIT every child, wait, reinitialise shared
memory, restart from STARTUP. Process mode's "self-heal" for a genuine aux-worker crash is
therefore **"the whole server cycles"**, not "that worker respawns while its siblings keep
serving". There is no narrow crash-respawn to mirror.

The only narrow, side-effect-free relaunch that exists is the **clean exit** case (e.g. an
unsolicited `SIGTERM` to just that worker): the PMChild pointer becomes NULL, and with
`pmState == PM_RUN` the missing singleton is relaunched. That path **already works on this branch
unmodified** for a fiber-backed walwriter, because `cleanup_wal_writer_child` skips
`HandleChildCrash` on `EXIT_STATUS_0`.

## What the threaded branch already does, which is stricter still

`HandleChildCrash` does not even reach process-mode crash recovery when carriers exist:

```c
	/*
	 * Once threaded carriers exist, a child crash means the postmaster's own
	 * address space may be compromised.  The process-mode crash-recovery path
	 * ... cannot safely recover from a failed thread inside this process.
	 */
	if (multithreaded && PostmasterThreadCarriersStarted())
	{
		ereport(LOG, (errmsg("terminating threaded server runtime after child crash")));
		ExitPostmaster(1);
	}
```

**On this branch, an aux-worker genuine crash already fail-stops the whole server.** That is a
deliberate decision with a stated rationale: a failed thread may have corrupted the address space
shared by every carrier.

## Therefore

Adding "genuine crash ⇒ respawn just that worker, siblings keep serving" would be:

* **more** self-healing than process-mode PostgreSQL has ever been for these singletons, and
* an override of an explicit safety decision in the threaded path,

on the basis that a crashed thread's shared state is trustworthy — which is exactly what both
comments above deny. It is not a conservative gap to be closed; it is a boundary.

## What the supervision tree therefore does

* **Client backends:** `XTC_RESTART_TEMPORARY`. Crash ⇒ fail-stop, never restart.
* **Safely-relaunchable singletons (walwriter first):** registered as supervised children whose
  restart covers the **clean-exit** case only — formalising the already-correct
  missing-singleton relaunch through the supervisor pipeline instead of an ad-hoc NULL check.
* **Genuine crash:** classified from the documented `XTC_DOWN_KIND_SIGNAL` and routed into the
  existing `HandleChildCrash` → `ExitPostmaster(1)` fail-stop. Unchanged behaviour, now expressed
  through the tree.

That is a real OTP supervision tree with real restart policy, intensity and escalation, and it
delivers genuine self-healing exactly where self-healing is safe — without inventing recovery
semantics upstream deliberately refuses.

## What would change this
A future design that makes an aux worker's shared-state contribution recoverable — e.g. a worker
whose state is fully reconstructible, or an `xtc_xproc`-supervised out-of-address-space worker
whose crash cannot corrupt carrier memory. At that point crash-respawn becomes discussable **for
that worker**, on evidence, not as a general policy. Phase 19's process-fallback work is the
natural home for it.


--------------------------------------------------------------------------------
## Addendum 2026-09-11: restart ownership cannot move to the supervisor thread

A second structural constraint, verified in source, which fixes the *division of labour* in the
tree rather than its scope.

**`xtc_orc` restarts a child from the supervisor's own thread.** `src/orc/sup.c:293` is the
supervisor's `xtc_recv` loop; the restart at `:342` calls `__spawn_child()` directly from inside
it. So a restarted child's `spec.fn` runs on the supervisor's carrier-loop OS thread. For a
pure-libxtc application that is correct and expected.

**But every PMChild slot assignment in this codebase is postmaster-thread-affine.** All
`AssignPostmasterChildSlot()` call sites live in postmaster context
(`postmaster.c:4204, 4666, 4710, 4862`), as do the `ActiveChildList` mutations.

Therefore, letting `xtc_orc` own an aux worker's restart would mean calling
`AssignPostmasterChildSlot()` from a carrier thread — **a new cross-thread-safety bug of our own
making**, not a workaround for anyone else's. Declining to do that is not a half-measure; it is
declining to write a bug.

**And PostgreSQL's own relaunch already covers fiber-backed workers.**
`LaunchMissingBackgroundProcesses()` contains
`if (WalWriterPMChild == NULL && pmState == PM_RUN) WalWriterPMChild = StartChildProcess(B_WAL_WRITER);`
and is called from `ServerLoop()` (`postmaster.c:2033`) every tick.
`reap_aux_or_backend_child()` routes process *and* fiber/thread exits through the same
`cleanup_wal_writer_child()`, which NULLs the pointer on any exit — so the next tick relaunches it
by the existing, postmaster-thread-affine, already-safe path.

### The resulting division of labour

| responsibility | owner | why |
|---|---|---|
| **detection** — classify the DOWN | `xtc_orc` | it holds the monitor |
| **escalation** — fail-stop on a genuine `SIGNAL` | `xtc_orc` -> existing `HandleChildCrash` | correctness property, must not regress |
| **restart** of aux workers | **existing postmaster polling** | PMChild assignment is postmaster-thread-affine |
| **restart intensity / flap escalation** | ours, alongside the postmaster relaunch | `xtc_orc` cannot track restarts it does not perform |

Aux workers are therefore registered `XTC_RESTART_TEMPORARY` — "never restart" is the
contract-accurate expression of *"this tree observes and escalates; someone else relaunches"*.
Note this also sidesteps the `reason != 0` / `NOPROC` defect with **no compensating logic at all**,
purely by selecting the correct documented policy. That is a consequence, not the motivation.

Restart intensity is not lost, it moves: we count relaunches per singleton in a window and
fail-stop a flapping worker. A walwriter that dies and relaunches 20 times a minute is a sick
server and should stop rather than spin — real OTP escalation, owned by whoever owns the restart.

### Prerequisite for full fusion (roadmap item)
Giving `xtc_orc` restart ownership requires a safe way for a carrier thread to obtain a PMChild
slot and mutate `ActiveChildList` — most plausibly by *asking* the postmaster thread, which is
exactly what our existing `XTC_SUP_SPAWN` mailbox hop already does for backend fibers. That is
lifecycle-critical, AGENTS.md two-review territory, and is written up separately. Until it exists,
detection-and-escalation is the correct and complete shape of the tree.
