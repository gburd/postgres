# Request: an off-thread / async xtc_sup_cast_add_child

Date: 2026-11-13
libxtc: `8a7e9c06` (2.2 dev line, what we pin). Verified against this source.
Reporter context: PostgreSQL-on-libxtc branch, moving client-backend fibers
onto the xtc_orc supervisor (using the on_child_down hook you added).

## The answer up front

To make the xtc_orc supervisor OWN our client-backend fibers -- so your
`on_child_down` hook fires for them -- we need to add a child to a supervisor
FROM A THREAD THAT IS NOT A FIBER. `xtc_sup_add_child` cannot do that: it
requires `xtc_self()` (sends ADD_CHILD and awaits the reply), so it returns
XTC_E_INVAL off a proc (src/orc/sup.c: `if (xtc_pid_is_none(xtc_self())) return
XTC_E_INVAL`). Please add a fire-and-forget variant callable from any thread:

```c
/* Enqueue an ADD_CHILD to the supervisor from ANY thread (no reply awaited).
 * The supervisor spawns+monitors+tables the child on its own loop exactly as
 * xtc_sup_add_child does, so on_child_down fires for it; the caller does not
 * get the pid back.  Returns XTC_OK once enqueued, XTC_E_AGAIN if the
 * supervisor mailbox is soft-full (retry), XTC_E_INVAL for a dead/invalid
 * supervisor, XTC_E_RESOURCE at max_children.  Safe from a plain thread. */
XTC_API int xtc_sup_cast_add_child(xtc_supervisor_t *sup,
                                   const xtc_child_spec_t *spec);
```

It is to `xtc_sup_add_child` what `xtc_sup_cast` is to `xtc_sup_call`: the same
internal ADD_CHILD path the supervisor already runs in `__handle_add_child`,
reached by an off-thread enqueue instead of a from-fiber call+await.

## Why we need off-thread specifically

Our client backends are launched from the POSTMASTER SPAWN THREAD
(`postmaster_backend_thread_launch`), which is control-plane and deliberately
NOT a carrier fiber (a process-lifetime exception in our design). That thread
already does a fire-and-forget cross-thread `xtc_send` to a per-loop supervisor
today (our hand-rolled XTC_SUP_SPAWN message); we just want that to be the
supervisor's OWN ADD_CHILD path so the child lands in its table and your hook
sees its DOWN. We never use the returned pid (the launch is fire-and-forget),
so awaiting a reply is pure cost -- the cast is a strictly better fit than
add_child for us even ignoring the thread constraint.

## Why the alternatives are worse

- A plain `xtc_proc_spawn_monitor` (what we do now) does NOT register the child
  with the supervisor, so `on_child_down` never fires -- defeating the whole
  point of adopting the hook.
- Bridging with a dedicated per-loop "adder" fiber that calls
  `xtc_sup_add_child` works without a libxtc change, but adds a second proc per
  loop purely to cross the thread->fiber boundary AND serializes adds behind the
  synchronous add_child await -- a connect-burst bottleneck the cast avoids.
- Making the spawn thread a fiber is not an option (control-plane lifetime).

## Ordering / semantics we rely on

- The child must be TABLED by the supervisor (so the hook and the restart policy
  apply) and spawned+monitored atomically on the supervisor's loop, exactly as
  `xtc_sup_add_child` does internally. We use `XTC_RESTART_TEMPORARY` (a crashed
  backend must never auto-restart; it escalates to our fail-stop), so no restart
  behavior is exercised -- we only need the DOWN surfaced via on_child_down.
- AGAIN/INVAL/RESOURCE return codes like the enqueue path so our existing
  rotate-on-AGAIN admission loop (never drop an accepted connection under a
  connect burst) carries over unchanged.
- `spec` is copied by the enqueue (the caller's stack spec need not outlive the
  call), matching `xtc_sup_add_child`'s copy semantics.

## Not established / scope

We have not prototyped it in sup.c (we do not fork your supervisor). The
`xtc_pid_is_none(xtc_self())` guard and the `__handle_add_child` internal path
are read from the 8a7e9c06 source; the cast is a consumer-vantage proposal. Our
launch path's off-thread, fire-and-forget, pid-not-needed shape is verified in
our tree (postmaster_backend_thread_launch -> xtc_pg_launch_backend_fiber).

## Environment

libxtc `8a7e9c06` (2.2 dev), io_uring, Linux. Consumer: PostgreSQL xtc branch,
one xtc_sup per carrier loop (ONE_FOR_ONE), on_child_down installed, children
intended TEMPORARY, added from the postmaster spawn thread under a connect-burst
admission loop.
