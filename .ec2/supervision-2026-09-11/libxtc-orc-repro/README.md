# xtc_orc spawn-then-monitor race: runnable repros

Filed as `/tmp/libxtc-orc-spawn-monitor-race-BUG-2026-09-11.md` (also in
`plan_docs/phase16_audits/LIBXTC_ORC_SPAWN_MONITOR_RACE_BUG.md`).

## Build (against a pristine libxtc checkout)

```sh
cd <libxtc-checkout>
git status --short   # MUST be empty
meson setup /tmp/xtc-supcheck-build -Dtls=none -Dshared=false -Dbuildtype=debugoptimized
ninja -C /tmp/xtc-supcheck-build libxtc.a

cc -I<libxtc-checkout>/src/inc -O0 -g \
   -o test_orc_spawn_monitor_race test_orc_spawn_monitor_race.c \
   /tmp/xtc-supcheck-build/libxtc.a -lpthread -lrt -ldl -luring
```
(repeat for the other two .c files; same compile line, different source.)

## Files

- `test_orc_spawn_monitor_race.c` -- THE bug repro.  Two modes:
  - `classify same|cross <trials>`: a TEMPORARY child that faults instantly,
    added via the real `xtc_sup_add_child()`, then independently monitored
    by the caller.  Reports how many of the DOWNs were correctly classified
    `XTC_DOWN_KIND_SIGNAL` vs lost to `XTC_DOWN_KIND_NOPROC`.
    Same-loop: **deterministic 100% loss** (0/N correct, N/N lost, every run).
    Cross-loop: probabilistic, observed 18.7%-47.3% loss across repeated runs.
  - `restart same|cross <trials>`: a TRANSIENT child that exits CLEANLY
    (reason 0, must never restart).  Reports `xtc_sup_n_restarts()` deltas.
    Same-loop: deterministic 0 spurious restarts (the bug does not fire here
    for a CLEAN exit -- see mechanism note below).
    Cross-loop: probabilistic spurious restarts, observed 238-1974 per 1000
    trials across repeated runs.

- `test_spawn_monitor_atomic.c` / `test_spawn_monitor_atomic_cross.c` --
  NOT bug repros; sanity checks that the ATOMIC `xtc_proc_spawn_monitor()`
  primitive (which `xtc_orc`'s own `__spawn_child` does NOT use, but which
  our hand-rolled PostgreSQL supervisor DOES use) is race-free: same-loop
  3000/3000 correct, cross-loop 3000/3000 correct, zero losses in both.
  This is the evidence that the fix is available and already proven safe
  by construction, not just asserted.

## Mechanism note (why same-loop CLASSIFY is deterministic but same-loop
RESTART shows 0 spurious restarts)

`xtc_sup_add_child()` is a blocking round trip: it sends an ADD_CHILD
control message to the supervisor and blocks in `xtc_recv()` (a yield
point) for the reply.  Inside the supervisor, on a SINGLE loop, the child
is spawned (enqueued) before the reply is sent.  A crashing child with NO
yield of its own runs to completion (fault) while the *caller* is still
blocked in `xtc_recv()` waiting for the ADD_CHILD reply -- so by the time
the caller's own follow-up `xtc_monitor()` call executes, the child is
already gone.  This is deterministic on a single OS thread because the
scheduler always has an opportunity to run the newly-enqueued child before
returning control to a fiber that is parked in `xtc_recv`.

A CLEANLY-exiting child (the RESTART-mode probe) hits the SAME window in
principle, but our specific same-loop RESTART test observed 0/N because
`xtc_sup_add_child`'s OWN internal monitor (inside `__handle_add_child`,
which runs BEFORE the reply is sent) is established before the reply
unblocks the caller -- so the supervisor's own bookkeeping already caught
the exit correctly by the time an external caller could race it a second
time. The two measurements probe different observers: CLASSIFY probes a
SEPARATE caller's own monitor (added after add_child returns); RESTART
probes the supervisor's OWN internal monitor's restart decision.  Both are
instances of the same defect (`__spawn_child`'s spawn-then-monitor split);
they simply expose it via different racers, which is why they behave
differently under same-loop scheduling.  Cross-loop widens the window for
both because the child can now run to completion, concurrently, on a
genuinely different OS thread while EITHER caller is off doing other work.
