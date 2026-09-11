# BUG: `xtc_orc`'s supervisor uses the spawn-then-monitor idiom your own header warns against

Date: 2026-09-11
libxtc: **c1a7bda** (checkout verified pristine — `git status --short` empty)
Severity: **blocks adoption of `xtc_orc` entirely for our use case**, and it is a correctness
bug in your library independent of us.
Reporter context: PostgreSQL-on-libxtc branch. We do **not** currently call `xtc_orc` — this was
found while evaluating it to replace our hand-rolled supervisor.

--------------------------------------------------------------------------------
## Provenance first: this is in libxtc, not in our tree

Stated plainly because it matters for where to look:

* our PostgreSQL branch contains **zero** references to `xtc_orc` / `xtc_sup_*` / `xtc_child_spec`
  (`grep -rn ... src/ contrib/ | wc -l` → `0`);
* `src/orc/sup.c` exists only in the libxtc checkout, not in our tree;
* the checkout is pristine at `c1a7bda` — no local edits of ours;
* the lines are upstream-authored.

So nothing we did produced this, and nothing we can do in our branch works around it.

--------------------------------------------------------------------------------
## The bug: your header names the exact idiom, and your supervisor is it

`src/inc/xtc_proc.h`, documenting `xtc_proc_spawn_monitor`:

> *"Atomic spawn + link / spawn + monitor (Erlang spawn_link / spawn_monitor). Identical to
> `xtc_proc_spawn`, but the parent<->child relationship is established **BEFORE** the child is
> made runnable, so there is **no window in which the child exists but is not yet
> linked/monitored** — even if the child runs and exits immediately, its EXIT/DOWN is delivered
> (**no `XTC_DOWN_NOPROC` race that a spawn-then-link/monitor idiom can hit**)."*

`src/orc/sup.c:118-142`, `__spawn_child()`:

```c
	rc = xtc_proc_spawn(target, c->spec.fn, c->spec.arg, &pop, &c->pid);
	if (rc != XTC_OK) return rc;
	c->alive = 1;
	rc = xtc_monitor(c->pid, &c->monitor_ref);
	return rc;
```

**Spawn at `:137`, monitor at `:140`.** That is precisely the spawn-then-monitor idiom, hitting
precisely the `XTC_DOWN_NOPROC` race your own documentation says the atomic call exists to avoid.
`xtc_proc_spawn_monitor()` is public (`xtc_proc.h:163`) and unused in `sup.c`.

`:426` spawns the supervisor proc itself the same way, though the consequences there are yours to
assess.

The window is **widened by design** in the multi-loop case: the comment at `:128-130` says the
supervisor runs on loop 0 and monitors children **cross-loop**, so a child placed on another loop
can be made runnable, run, and exit on a different OS thread while `xtc_monitor()` has not yet
executed.

## Consequence 1 — crashes are misreported as benign

`src/ptc/proc.c:3456-3459` documents that `xtc_monitor()` on an already-dead target does not
fail; it delivers an immediate DOWN "so report it as `XTC_DOWN_NOPROC` — a DISTINCT reason".

`src/inc/xtc_proc.h:691-695`:
```
XTC_DOWN_KIND_CLEAN  = 0   target returned or xtc_exit_self(0)
XTC_DOWN_KIND_EXIT   = 1   xtc_exit_self(code)
XTC_DOWN_KIND_SIGNAL = 2   R1 contained fault, signal in .signal
XTC_DOWN_KIND_NOPROC = 3   monitor raced a dead target (benign)
```

So a child that **faults** inside the window is delivered as `NOPROC` — annotated *benign* —
rather than `SIGNAL`. A supervisor cannot distinguish "my child segfaulted" from "I raced a
already-exited child".

## Consequence 2 — `TRANSIENT` children are restarted after a CLEAN exit

`src/orc/sup.c:85-89`:
```c
	case XTC_RESTART_PERMANENT:  return 1;
	case XTC_RESTART_TEMPORARY:  return 0;
	case XTC_RESTART_TRANSIENT:  return reason != 0;
```

`TRANSIENT` restarts on any nonzero reason, and `NOPROC` is reason **3**. So a child that exits
**cleanly but fast** can be misclassified and restarted — triggered by a condition your own
header calls benign. That is an OTP semantics violation: `transient` must restart only on
abnormal termination.

These two are coupled: (2) is (1) reaching a policy that treats any nonzero reason as abnormal.
Whether fixing the atomicity fully closes (2) is your call — there may be other routes to
`NOPROC` — but the coupling is visible in these five lines.

## Reproduction

A colleague of mine built standalone C repros against `c1a7bda` and is preparing them for you with
exact compile lines; I verified the *mechanism* independently at the source level, as above, and
the two readings agree. Their headline results, as reported to me:

* a child that faults instantly is classified `XTC_DOWN_KIND_NOPROC` rather than
  `XTC_DOWN_KIND_SIGNAL` in **2000 of 2000 trials**, using same-loop cooperative scheduling — i.e.
  **no OS-thread race is needed to hit this**, which makes it deterministic rather than rare;
* spurious restarts of `TRANSIENT` children that exited cleanly, observed on cross-loop placement.

I am deliberately not quoting a figure for the second one: their first-pass number was garbled in
transit and I sent it back for restatement rather than forward a number I could not stand behind.
Treat the 2000/2000 as the load-bearing one until their repro lands.

## The fix, as we see it

`__spawn_child()` should use `xtc_proc_spawn_monitor()` — the primitive you already ship for
exactly this. Note the constraint in your own doc: *"The CALLER MUST be a process
(`xtc_self() != NONE`)"*. `__spawn_child` is called from `__handle_add_child`, which the comment at
`:144-146` says runs **inside the supervisor proc** ("so the spawn + monitor are owned by it"), so
that precondition appears already satisfied on the ADD_CHILD path — worth confirming for the
`xtc_sup_start` path.

Separately, and regardless of the atomicity fix: consider whether `NOPROC` should ever reach the
restart-policy decision at all. If it is genuinely benign, `__should_restart` arguably should
treat it as non-abnormal rather than "nonzero ⇒ restart".

## Why this blocks us specifically, and why we care a lot

We are trying to put PostgreSQL under a **real OTP-style supervision tree** — the goal is
BEAM-style self-healing where it is safe. Two hard requirements collide with this bug:

1. **Crash ⇒ fail-stop for client backends.** A crashed PostgreSQL backend may have corrupted
   shared memory, so it must **never** be silently restarted; our supervisor classifies the DOWN
   and escalates a genuine crash to a whole-process fail-stop. If a crash arrives as benign
   `NOPROC`, **we lose the fail-stop trigger** and would continue running on possibly-corrupted
   shared state. That is a data-integrity risk, not a preference.
2. **Safe self-healing for in-tree aux workers.** Workers like walwriter, bgwriter, archiver and
   the autovacuum/logical-replication launchers are already restarted by process-mode PostgreSQL
   when they die, so registering them `TRANSIENT`/`PERMANENT` under a supervisor is a faithful
   translation rather than new risk. But a supervisor that **spuriously restarts a worker that
   exited cleanly** is worse than one that never restarts: it would fight our own orderly
   shutdown, and restart-intensity escalation could then take down a healthy server.

So this single defect blocks both halves of the supervision work. Our hand-rolled per-loop
supervisor already uses `xtc_proc_spawn_monitor()` (behind an `XTC_SUP_SPAWN` mailbox hop,
specifically so the spawn and monitor are owned atomically by the target loop) and therefore has
**no such window** — which is the awkward position of having to keep ~260 lines of our own code
because the library's equivalent is less safe than the primitive the library ships.

We would very much rather delete our version. Please fix `__spawn_child` and we will re-test
immediately; the repro is deterministic, so verification should be quick on both sides.

## Environment
Found by source inspection plus standalone C programs linked against a pristine `c1a7bda` build
(meson, `-Dbuildtype=debugoptimized -Dtls=openssl -Dshared=true`), Linux x86_64, glibc.
No PostgreSQL involvement in the repro path at all.
