# Independent verification: xtc_orc's spawn/monitor race (source-level)

Date: 2026-09-11
libxtc: **c1a7bda**, checkout verified pristine (`git status --short` empty).

A sub-agent building standalone C repros against `xtc_orc` reported two defects. Because a
"bug" that turns out to be our own instrumentation would be the fifth such retraction in this
collaboration, I verified the mechanism independently **at the source level** before endorsing
it. Both check out.

## 1. The spawn/monitor window is real

`src/orc/sup.c` — every spawn/monitor call in the file:

```
137:  rc = xtc_proc_spawn(target, c->spec.fn, c->spec.arg, &pop, &c->pid);
138:  if (rc != XTC_OK) return rc;
139:  c->alive = 1;
140:  rc = xtc_monitor(c->pid, &c->monitor_ref);
426:  rc = xtc_proc_spawn(loop, __sup_entry, sup, NULL, &pid)      /* the supervisor itself */
```

Spawn at `:137`, monitor at `:140` — **two separate steps with a window between them.**

The atomic primitive exists and is public:
```
src/inc/xtc_proc.h:163   XTC_API int xtc_proc_spawn_monitor(xtc_loop_t *, xtc_proc_fn, void *,
                                     const xtc_proc_opts_t *, xtc_pid_t *, uint64_t *);
```

**libxtc ships an atomic spawn+monitor and its own supervisor does not use it.** That is the
actionable sentence. Our hand-rolled supervisor *does* use it (via the `XTC_SUP_SPAWN_MAGIC`
mailbox hop, precisely so the spawn and the monitor are owned by the loop atomically), so
adopting `xtc_orc` as-is would be a **regression**, not a dedup.

## 2. The misclassification is by design, and it is load-bearing for us

`xtc_monitor()` on an already-dead target does not fail; `src/ptc/proc.c:3456-3459` says it
"delivers an immediate DOWN rather than failing... so report it as `XTC_DOWN_NOPROC` -- a
DISTINCT reason".

`src/inc/xtc_proc.h:691-695`:
```
XTC_DOWN_KIND_CLEAN  = 0   target returned or xtc_exit_self(0)
XTC_DOWN_KIND_EXIT   = 1   xtc_exit_self(code)
XTC_DOWN_KIND_SIGNAL = 2   R1 contained fault, signal in .signal
XTC_DOWN_KIND_NOPROC = 3   monitor raced a dead target (benign)
XTC_DOWN_KIND_NOCONNECTION = 4
```

So a child that faults *inside the window* is reported `NOPROC` ("benign") instead of `SIGNAL`
("contained fault"). **For us that is a correctness regression, not a cosmetic one:** our DOWN
classifier distinguishes genuine-crash from clean logical exit from FATAL, and the
genuine-crash class is what triggers **fail-stop**. A crash flattened to "benign NOPROC" would
not escalate — a backend with possibly-corrupted shared memory would be treated as a benign
race.

## 3. The spurious-restart bug follows from (2), via a one-line policy test

`src/orc/sup.c:85-89`:
```c
case XTC_RESTART_PERMANENT:  return 1;
case XTC_RESTART_TEMPORARY:  return 0;
case XTC_RESTART_TRANSIENT:  return reason != 0;
```

`TRANSIENT` restarts whenever `reason != 0`. `NOPROC` is reason **3**. So a child that exits
*cleanly but fast* can be misread as `NOPROC` and **restarted, despite having exited cleanly** —
and the library's own comment calls that condition benign. The two bugs are therefore linked:
(3) is a consequence of (2) reaching a policy that treats any nonzero reason as abnormal.

Whether fixing the atomicity fully fixes (3) is for them to say — there may be other paths to
`NOPROC` — but the coupling is clear from these five lines.

## 4. Consequence for the north star

This blocks BOTH halves of the supervision work:

* **Mechanical replacement:** cannot adopt `xtc_orc` for backend children without losing the
  fail-stop trigger. Our hand-rolled supervisor stays.
* **BEAM-style self-healing aux workers:** the plan was to register safely-restartable in-tree
  workers (walwriter, bgwriter, archiver, autovacuum/logical launchers) as `TRANSIENT` or
  `PERMANENT`, which is a faithful translation of what process-mode PostgreSQL already does on
  their death. But a supervisor that **spuriously restarts a worker that exited cleanly** is
  arguably worse than not restarting at all. So self-heal is blocked on the same defect.

## Defer with invariant (per AGENTS.md)

* **Why the current scope is safe now:** we keep the hand-rolled per-loop supervisor, which uses
  `xtc_proc_spawn_monitor()` and has no such window; backend children remain effectively
  TEMPORARY with crash => fail-stop. Behaviour is unchanged from today.
* **Which guard catches a wrong assumption:** the genuine-crash path is exercised by
  `PG_XTC_INJECT_CRASH` plus the existing crash TAP tests (`002_threaded_bgworker_crash.pl`,
  `010_phase16_pooled_crash_recovery.pl`, `013_phase19_process_fallback_crash.pl`); F0d's
  `xtc_dump` must precede "terminating threaded server runtime". If a future change lets a
  crash be reclassified as benign, those tests stop seeing the fail-stop.
* **Which gate owns completion:** F4-SUP, reopened once libxtc makes `xtc_orc` use the atomic
  spawn+monitor (or exposes a way to keep our own spawn path under their supervisor tree).

## Note on method
The sub-agent's first report contained a garbled figure ("5253/3000") which I sent back for
correction rather than forwarding. Numbers go to this team exactly or not at all — they read
them closely, and a bad headline number would discredit a real finding.
