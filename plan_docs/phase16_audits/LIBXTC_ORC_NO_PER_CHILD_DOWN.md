# GAP (not a bug): `xtc_orc`'s public API has no per-child DOWN classification

Date: 2026-09-11
libxtc: **v1.44.0** (tag `2c6d4db`), checkout verified pristine (`git status --short` empty).
Reporter context: PostgreSQL-on-libxtc branch, building a real supervision tree on `xtc_orc`.

--------------------------------------------------------------------------------
## Up front: this is NOT the bug you just fixed, and the fix works

We re-verified `/tmp/libxtc-orc-spawn-monitor-race-BUG-2026-09-11.md` against v1.44.0 with an
independent repro, same-loop (the deterministic-loss topology):

```
RESULT classify: correct(SIGNAL)=500 lost(NOPROC)=0 of 500 trials   (was 0/500 before the fix)
RESULT restart:   spurious_restarts=0 of 500 TRANSIENT clean-exit trials  (was already 0 here)
```

**500/500 `SIGNAL`, zero `NOPROC`.** Confirmed independently, matches your own 200/200 result.
`sup.c:188`'s atomic `xtc_proc_spawn_monitor()` and `sup.c:107`'s `NOPROC`-is-not-abnormal guard
both do exactly what the fix note says. We are adopting v1.44.0 on the strength of this. Thank you
for turning that around fast — this report is additive, not a regression on the fix.

This report is about a **different, narrower problem** we found while wiring `xtc_orc` into our
runtime for real: even with the internal classification now correct, **nothing in the public API
lets an external caller learn what that classification was, for a specific child.**

--------------------------------------------------------------------------------
## The gap

`src/inc/xtc_orc.h`'s entire public surface:

```c
XTC_API int  xtc_sup_start(...);
XTC_API int  xtc_sup_stop(xtc_supervisor_t *sup);
XTC_API int  xtc_sup_add_child(xtc_supervisor_t *sup, const xtc_child_spec_t *spec, xtc_pid_t *out_pid);
int          xtc_sup_join(xtc_supervisor_t *sup, int64_t timeout_ns);
XTC_API int  xtc_sup_n_children(const xtc_supervisor_t *sup);
XTC_API int  xtc_sup_n_alive(const xtc_supervisor_t *sup);
XTC_API int  xtc_sup_n_restarts(const xtc_supervisor_t *sup);
XTC_API int  xtc_sup_alive(const xtc_supervisor_t *sup);
```

Every accessor is an **aggregate counter** over the whole supervisor. `xtc_child_spec_t` has
`name`, `fn`, `arg`, `policy`, `loop`, `mailbox_cap` — no callback field, no notify target, no
DOWN-forwarding hook. The supervisor decodes each child's DOWN inside its own `xtc_recv` loop
(`sup.c` `__sup_entry`) purely to drive `__should_restart`; the classification is consumed and
discarded, never surfaced.

### Repro 1: two `TEMPORARY` children, one crashes, one exits cleanly — the public counters cannot
tell them apart

`test_orc_temporary_observability.c` (attached, compile line below). Registers one `TEMPORARY`
child that SIGSEGVs and one `TEMPORARY` child that returns cleanly, waits for both to finish, then
reads every public accessor:

```
add crash-temp child: rc=0 pid=(0,3,1)
add clean-temp child: rc=0 pid=(0,3,2)

--- PUBLIC xtc_orc counters after both children are dead ---
n_children delta = 2 (expect 2)
n_alive          = 1 (supervisor's own bookkeeping proc; irrelevant to the two children, both dead)
n_restarts delta = 0 (TEMPORARY never restarts, so 0 for BOTH the crash and the clean exit)
alive            = 1 (the supervisor itself, not per-child)
```

`n_restarts` is 0 for both because `TEMPORARY`'s restart decision is "never" regardless of reason
— which is exactly the policy an embedder with a fail-stop-on-crash requirement needs to use (see
"why it matters" below), and exactly the policy for which the public API retains **zero**
externally visible trace of what happened to a specific child.

--------------------------------------------------------------------------------
## Why a caller-side follow-up `xtc_monitor()` is not a substitute (three reasons, all measured)

The obvious workaround is: call `xtc_monitor()` yourself on the `pid` `xtc_sup_add_child` hands
back, and read the DOWN it delivers to you. Three independent reasons this does not work.

### (i) It is a second monitor racing the supervisor's own — measured 1000/1000 lost, same-loop

`test_addchild_followup_monitor_rate.c`: register a `TEMPORARY` child that faults instantly via
`xtc_sup_add_child`, then immediately `xtc_monitor()` the returned pid ourselves, on the SAME
single loop as the supervisor (no cross-thread race needed):

```
SAME-LOOP add_child + OUR OWN follow-up monitor: correct(SIGNAL)=0 lost(NOPROC)=1000 of 1000
```

Mechanism: `xtc_sup_add_child` is a blocking round trip (`ADD_CHILD` message to the supervisor,
`xtc_recv` for the reply). Inside the supervisor, `__handle_add_child` spawns the child (now
correctly atomic-with-its-OWN-monitor, per the v1.44.0 fix) and replies. On a single cooperative
loop, a no-yield crashing child can run to completion while the CALLER is still blocked in
`xtc_recv` waiting for that reply — so by the time the caller's own follow-up `xtc_monitor()`
executes, the child is already gone, and `xtc_monitor` on a dead target is documented, correct
`NOPROC` behaviour. This is a DIFFERENT window than the one you just fixed: it is not inside
`__spawn_child`, it is the gap between "child is running" and "caller can possibly react",
which `add_child`'s synchronous contract cannot close for a caller outside the supervisor.

### (ii) `out_pid` is written once and never updated on restart — the handle goes stale

Verified at the two source sites:

* `sup.c:189` — `__spawn_child` sets `c->pid` (the supervisor's OWN bookkeeping) on every spawn,
  **including every restart**.
* `sup.c:548` (`xtc_sup_add_child`) — writes the caller's `*out_pid` exactly ONCE, from the
  ADD_CHILD reply, at the original add time.

So after the first `PERMANENT`/`TRANSIENT` restart, the supervisor's internal `c->pid` has moved
on to the new child's pid, but the caller's `out_pid` still names the ORIGINAL, now-dead one.
`test_stale_outpid.c` confirms this concretely: a `PERMANENT` child that exits every 5ms is
restarted 3 times (`n_restarts=3`, `run_count=4`), then we `xtc_monitor()` the ORIGINAL `out_pid`
from the initial `add_child` call:

```
add_child rc=0 original_pid=(0,3,1)
after 10ms: run_count=4 n_restarts=3
xtc_monitor(original_pid) rc=0
DOWN on original_pid: kind=3 reason=-100000
CONFIRMED STALE: original_pid's specific generation is already gone (NOPROC)
```

`original_pid`'s specific generation is gone; the live child today has a different `pid.gen` that
`out_pid` was never updated to. Nothing in the public API signals that the pid changed.

### (iii) Even if (i) and (ii) were both solved, timing is still wrong

The supervisor may already have RESTARTED the child (for `PERMANENT`/`TRANSIENT`) before an
external caller's own monitor call even gets scheduled — so "the caller learns the classification"
and "the supervisor has already acted on it" are not ordered with respect to each other. An
embarrassingly simple example: an embedder wanting to log "child X crashed with signal 11" before
a restart happens has no way to guarantee that ordering today.

--------------------------------------------------------------------------------
## Why this matters beyond us

Any embedder that must **act differently** depending on whether a child crashed vs. exited
cleanly — not just decide whether to restart it, but log it, alert on it, refuse to restart a
process whose shared state might be corrupt, or escalate up its own supervision tree — needs the
**event**, not a counter. That is the difference between an OTP supervisor (which is exactly this
crash-vs-clean distinction, surfaced to `Logger`/telemetry/alarms in the BEAM ecosystem) and a
plain restart loop. `xtc_orc` gets the restart-loop half right (and, as of v1.44.0, gets the
INTERNAL classification right too) but currently cannot export the classification at all.

Concretely for us: our runtime's crash policy is "a crashed backend/worker may have corrupted
shared memory, so escalate to a whole-process fail-stop; a clean exit does not." That decision
lives OUTSIDE any supervisor (in our postmaster), by necessity — a supervisor cannot safely decide
to tear down a shared-address-space host process from inside itself. We therefore cannot move any
spawn whose death must be classified onto `xtc_sup_add_child`, and are instead using `xtc_orc` for
what its API actually provides well: child-spec bookkeeping, restart policy declaration, and the
aggregate counters as observability. Our own `xtc_proc_spawn_monitor()` + a classifier stays the
source of truth for crash detection, in parallel with (not through) the supervisor.

--------------------------------------------------------------------------------
## Two concrete API shapes, either of which unblocks us

We would take either of these; (2) may fit your message-passing idiom and hot-path discipline
better than a callback:

**(1) An optional callback in `xtc_child_spec_t`:**
```c
typedef struct xtc_child_spec {
    ...
    void (*on_down)(const struct xtc_child_spec *spec, const xtc_down_info_t *info, void *ud);
    void  *on_down_ud;
} xtc_child_spec_t;
```
Invoked from the supervisor's own `xtc_recv` loop, BEFORE the restart decision, so an embedder can
observe (and even veto/escalate) ahead of any restart. Downside: runs on the supervisor's own
thread, so it must be cheap and non-blocking — worth stating as a documented constraint (like
`xtc_reg_members`'s callback-under-lock contract).

**(2) An opt-in "forward the DOWN" registration**, closer to your existing primitives:
```c
XTC_API int xtc_sup_forward_down(xtc_supervisor_t *sup, xtc_pid_t to);
```
Every child DOWN the supervisor observes gets re-sent (as the same `xtc_down_info_t`-shaped
message, addressed by the ORIGINAL child spec's identity — e.g. `spec->name` or a stable per-child
index, NOT the pid, given (ii) above) to `to`'s mailbox, in addition to driving the restart
decision. This keeps the hot path in the supervisor's existing `xtc_send`, avoids a callback
crossing an ABI/calling-convention boundary, and lets the receiver process the event at its own
pace via ordinary `xtc_recv` — which is exactly the idiom the rest of `xtc_proc` already uses.

Either shape needs a STABLE per-child identity that survives a restart (name or index), since (ii)
above shows the pid is not that identity.

--------------------------------------------------------------------------------
## Reproduction

Three files, same build as the original report:

```sh
cd <libxtc-checkout>
git status --short   # MUST be empty
meson setup /tmp/xtc-v1440-build -Dtls=none -Dshared=false -Dbuildtype=debugoptimized
ninja -C /tmp/xtc-v1440-build libxtc.a

cc -I<libxtc-checkout>/src/inc -O0 -g \
   -o test_orc_temporary_observability test_orc_temporary_observability.c \
   /tmp/xtc-v1440-build/libxtc.a -lpthread -lrt -ldl -luring

cc -I<libxtc-checkout>/src/inc -O0 -g \
   -o test_addchild_followup_monitor_rate test_addchild_followup_monitor_rate.c \
   /tmp/xtc-v1440-build/libxtc.a -lpthread -lrt -ldl -luring

cc -I<libxtc-checkout>/src/inc -O0 -g \
   -o test_stale_outpid test_stale_outpid.c \
   /tmp/xtc-v1440-build/libxtc.a -lpthread -lrt -ldl -luring
```

(repeat for each; same compile line, different source file.)

## Environment
Standalone C, no PostgreSQL involvement, Linux x86_64, glibc, libxtc v1.44.0 (`2c6d4db`).
