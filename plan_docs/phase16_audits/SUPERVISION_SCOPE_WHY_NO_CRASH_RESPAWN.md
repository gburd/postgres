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
