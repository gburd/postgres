# Request: xtc_orc needs a per-child DOWN hook (crash-vs-clean, before the restart decision)

Date: 2026-11-04
libxtc: fix/stale-readiness-dispatch @ `6dd001e0` (`v2.1.1-6-g6dd001e0`), the
commit we currently pin (it carries the stale-readiness gen-token fix + the OTP
and TLS-hook headers). Gap re-verified against this exact source.
Reporter context: PostgreSQL-on-libxtc branch, wiring `xtc_orc` into the carrier
supervision tree for real.

This re-raises and updates `/tmp/libxtc-orc-spawn-monitor-race-...`-era report
`LIBXTC_ORC_NO_PER_CHILD_DOWN.md` (2026-09-11, against v1.44.0). That older
report's analysis still holds verbatim; this one confirms the gap is unchanged
in the pinned commit and adds a concrete consequence: a feature we shipped this
week is forced to carry **zero children** purely because of this gap.

## The answer up front

`xtc_orc`'s public API still exposes only aggregate counters. There is no way
for an embedder to learn *what happened to a specific child* (crashed on a
signal vs. exited cleanly vs. nonzero exit) — the supervisor decodes exactly
that, uses it for `__should_restart`, and discards it. Re-verified in the
pinned source:

- `src/inc/xtc_orc.h`: `xtc_child_spec_t` fields are `size, name, fn, arg,
  policy, mailbox_cap, loop` — no callback, no notify target, no DOWN-forward.
  The public functions are `xtc_sup_{start,stop,add_child,join,n_children,
  n_alive,n_restarts,alive}` — all aggregate.
- `src/orc/sup.c:631-657`: the supervisor's `xtc_recv` loop does
  `memcpy(&d, msg, sizeof d)` (the `xtc_down_info_t`), `__find_child_by_pid`,
  then `__should_restart(sup, &children[idx], d.reason)`. The decoded `d`
  (kind/signal/reason) is consumed for the restart branch and never surfaced.

So nothing has changed since the 2026-09-11 report, and the three measured
reasons a caller-side follow-up `xtc_monitor()` cannot substitute all still
apply (add_child's synchronous round-trip loses the DOWN 1000/1000 same-loop;
`out_pid` is written once and goes stale across a restart; and the supervisor
may restart before an external monitor is even scheduled).

## Why we need the event, not a counter — the concrete consequence this week

Our runtime's crash policy is non-negotiable and lives OUTSIDE any supervisor:
a crashed backend/worker may have corrupted the shared address space, so a
genuine crash (a SIGNAL DOWN) must escalate to a **whole-process fail-stop**;
a clean exit must not. A supervisor cannot safely decide to tear down its own
shared-address-space host process from inside itself, so that decision stays in
our postmaster. Our hand-rolled `xtc_proc_spawn_monitor()` + DOWN classifier is
the source of truth that sets a genuine-crash flag and kicks the postmaster
latch.

This week we wired `xtc_orc` into the carrier path anyway (commit on our branch:
"Stand up an optional per-loop xtc_orc supervisor for carriers", GUC-gated,
default off). Because of this gap we could add **no children** to that
supervisor: any child whose death must be classified cannot go through
`xtc_sup_add_child` without silently losing the SIGNAL-DOWN -> fail-stop
escalation. The supervisor therefore stands up with `xtc_sup_start(loop, &opts,
NULL, 0, &sup)` — zero children — and exists only as scaffolding and
bookkeeping, with a long code comment explaining that backend fibers are
deliberately NOT orc children *because the classification cannot be observed*.
That is the whole value of `xtc_orc` (spawn+monitor atomicity, restart policy,
the child registry) left on the table for exactly the children we most want it
to own.

With a per-child DOWN hook we would move real children onto `xtc_orc`: backend
and worker fibers as `TEMPORARY` children (never auto-restarted — our policy),
with the hook delivering each child's classified DOWN to our postmaster-facing
escalator *before* any restart decision. That is the one missing piece between
"xtc_orc as a bookkeeping shell" and "xtc_orc as our actual supervision tree."

## What we are NOT asking for

- Not auto-restart of these children. Our policy forbids it; `TEMPORARY` is
  correct. We need the *observation*, not a restart behaviour change.
- Not a change to the internal classification — the v1.44.0 spawn-monitor-race
  fix is correct and we rely on it (500/500 SIGNAL, 0 NOPROC, re-confirmed).
- Not a way to tear down the host from inside the supervisor. The escalation
  stays in our code; we only need the supervisor to hand us the event.

## Two concrete API shapes, either unblocks us (unchanged from the 2026-09-11 ask)

Either is fine; (2) likely fits your message-passing idiom and hot-path
discipline better.

**(1) Optional callback in `xtc_child_spec_t`:**
```c
typedef struct xtc_child_spec {
    ...
    /* Invoked from the supervisor's own recv loop when THIS child goes DOWN,
     * BEFORE the restart decision.  Must be cheap and non-blocking (runs on
     * the supervisor's thread) -- same contract as xtc_reg_members' callback.
     * info is borrowed for the call's duration. */
    void (*on_down)(const struct xtc_child_spec *spec,
                    const xtc_down_info_t *info, void *ud);
    void  *on_down_ud;
} xtc_child_spec_t;
```
Attach point is exactly `sup.c` ~line 636 (right after `__find_child_by_pid`
resolves the child, before `__should_restart`). The `.size`-gated struct makes
this ABI-additive.

**(2) Opt-in DOWN forwarding, closer to your existing primitives:**
```c
XTC_API int xtc_sup_forward_down(xtc_supervisor_t *sup, xtc_pid_t to);
```
Every child DOWN the supervisor observes is re-sent (as the same
`xtc_down_info_t`-shaped message) to `to`'s mailbox, in addition to driving the
restart decision — addressed by a STABLE per-child identity (the spec `name` or
a per-child index), NOT the pid, since the pid goes stale across a restart. The
receiver processes it at its own pace via ordinary `xtc_recv`, which is the
idiom the rest of `xtc_proc` already uses. This keeps the hot path in the
supervisor's existing `xtc_send` and avoids a callback crossing the ABI.

Either shape needs a stable per-child identity surviving restart (name or
index); the 2026-09-11 report's repro `test_stale_outpid.c` shows the pid is not
that identity.

## Ordering requirement (the one hard constraint)

The hook/forward must fire BEFORE the supervisor acts on the restart decision
for that child, so an embedder can observe (and, for shape (1), potentially veto
or escalate) ahead of any restart. "The caller learns the classification" and
"the supervisor has already restarted" must be ordered. For our `TEMPORARY`
children there is no restart, but the ordering guarantee is what makes the hook
usable for `PERMANENT`/`TRANSIENT` embedders too.

## Reproduction

The three programs from `LIBXTC_ORC_NO_PER_CHILD_DOWN.md`
(`test_orc_temporary_observability.c`, `test_addchild_followup_monitor_rate.c`,
`test_stale_outpid.c`) still demonstrate the gap unchanged; rebuild against
`6dd001e0`:

```sh
cd <libxtc-checkout>           # at 6dd001e0, git status --short empty
meson setup /tmp/xtc-b -Dtls=none -Dshared=false -Dbuildtype=debugoptimized
ninja -C /tmp/xtc-b libxtc.a
cc -I<libxtc>/src/inc -O0 -g -o t test_orc_temporary_observability.c \
   /tmp/xtc-b/libxtc.a -lpthread -lrt -ldl -luring
./t   # the public counters cannot distinguish the crashed child from the clean one
```

## Not established / scope

We have not prototyped the hook inside `sup.c` ourselves (we do not want to fork
your supervisor). The attach line is our read of the pinned source, not a tested
patch. Everything about our own escalation path is verified in our tree; the ask
is solely the one-way export of the classification the supervisor already
computes.

## Environment

libxtc `6dd001e0` (v2.1.1-6), io_uring backend, Linux. Consumer: PostgreSQL xtc
branch, carrier supervision on a multi-loop `xtc_exec`, `xtc_sup` one per loop,
`ONE_FOR_ONE`, children intended as `TEMPORARY` (fail-stop-on-crash policy).
