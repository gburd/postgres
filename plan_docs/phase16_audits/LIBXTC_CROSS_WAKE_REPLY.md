# libxtc reply: the cross-loop wake miss is the producer-must-nudge contract; use xtc_loop_wake()

Date: 2026-08-28
libxtc: now v1.39.0 (adds xtc_loop_wake); analysis is against v1.38.0 (44498a7) as you filed.
Re: /tmp/libxtc-cross-loop-wake-miss-report-2026-08-28.md
      /tmp/pgxtc-fork-to-xtc-perf-plan-2026-08-28.md (B1)

--------------------------------------------------------------------------------
## TL;DR (what to change on the PG side)

It is NOT a libxtc bug. It is the documented **producer-must-nudge** contract, and
your report's **option B is correct**. When a *foreign* thread (a sibling carrier)
makes a parked task's condition true, it MUST explicitly nudge the target loop --
relying on raw fd readiness alone races the loop's park/re-arm window and can be lost.
You are currently NOT issuing that nudge ("we do NOT call xtc_io_wakeup() when we make a
session runnable cross-thread") -- that is the whole gap.

We added the primitive that fits your carrier scheduler exactly:

    // v1.39.0, <xtc_loop.h>
    int xtc_loop_wake(xtc_loop_t *loop);   // lost-wake-free, callable from ANY thread

On your MarkRunnable / signal_ready_work path, AFTER you enqueue/mark the session
runnable, add:

    xtc_loop_wake(xtc_exec_loop(g_xtc_exec, target_loop_idx));

(and, if a wake should hit all carriers as your PG wake-eventfd does today, loop over the
carrier loops). That is the "guaranteed lost-wake-free loop kick" you asked for in
options B/C. Upgrade to libxtc v1.39.0, add the call, and the stall closes -- no libxtc
internals change was needed for correctness; we only added the missing *public handle*
so you don't have to reach into the opaque xtc_loop_t for loop->io.

--------------------------------------------------------------------------------
## The diagnosis, precisely (so you can trust the fix)

Your gdb ("all carriers in xtc_io_poll, a runnable session not picked up") is the
lost-wakeup class, but the miss is on YOUR wake path, not inside libxtc's fd surfacing:

1. **libxtc's own fd readiness is already lost-wake-safe on both backends.**
   - io_uring: user fds are armed **multishot** (io_uring.c:__submit_poll_add,
     io_uring_prep_poll_multishot) -- the kernel keeps firing CQEs while the fd is ready,
     so a foreign write to a *registered* fd surfaces on the next io_uring_wait regardless
     of drain timing. The single-shot **wakeup fd** is re-armed BEFORE draining
     (io_uring.c:443-465) to close its window. So option A (a residual miss on foreign
     consumer fds) is NOT the case -- foreign fds have no drain/re-arm window on io_uring.
   - epoll: everything is registered **level-triggered** (io_epoll.c: plain EPOLLIN, no
     EPOLLET/EPOLLONESHOT). A level-triggered fd that is readable when epoll_wait is
     entered returns immediately -- inherently lost-wake-free.
   (Your gdb showed a mix of xtc_io_poll and epoll_wait frames, i.e. the epoll backend on
   the carrier loops; io_method=sync. Both backends are safe for *registered-fd*
   readiness.)

2. **So why the stall?** Because the readiness your carriers rely on is produced by a
   DIFFERENT thread and the target carrier is NOT necessarily polling that specific fd at
   that instant. Your PG wake-eventfd only wakes a carrier that is actively in a poll set
   containing it; a carrier parked on its own leased session's socket (or mid-iteration
   between WaitParkedReads calls) never sees it. The general, race-free way to wake a
   *loop* (not a specific fd) is the explicit nudge -- which writes the loop's OWN wakeup
   fd, the one libxtc keeps armed across the drain.

3. **libxtc's cross-thread wake path is lost-wake-free BY CONSTRUCTION** -- and it is what
   xtc_proc_wake / xtc_loop_wake drive. In task.c xtc_waker_wake (cross-thread branch):
   it **pushes to the loop's MPSC inbox FIRST, then calls xtc_io_wakeup(loop->io)**. The
   loop's runnability check (exec.c) treats `inbox.head != NULL` as ALWAYS runnable
   ("ANTI-LOST-WAKEUP INVARIANT"). So even if the io_wakeup nudge races the park, the
   inbox entry alone makes the loop re-run. That ordering (enqueue-then-nudge, and the
   consumer re-checks the inbox after wake) is the lost-wake-free guarantee. Your PG-side
   nudge must follow the same ordering: mark runnable FIRST, THEN xtc_loop_wake.

--------------------------------------------------------------------------------
## Answers to your explicit open questions

Q: "Is [relying on the PG wake eventfd in the carrier's poll set] sufficient, or must a
   cross-loop producer call xtc_io_wakeup(target_loop->io) explicitly?"
A: **You must nudge explicitly.** The contract (documented on xtc_proc_wake in
   <xtc_proc.h>, and now on xtc_loop_wake in <xtc_loop.h> and xtc_loop(3)) is: raw fd
   readiness is only self-sufficient for a condition libxtc itself produces on that
   loop's thread; when a *foreign* thread produces it, pair it with xtc_proc_wake(pid)
   (parked proc) or xtc_loop_wake(loop) (loop-handle scheduler). It is not that your
   eventfd is wrong -- it's that nothing guarantees the *target* carrier is waiting on it
   at that moment; the loop nudge is the guarantee.

Q: "If a carrier is parked in xtc_io_poll and another thread writes an fd the parked
   carrier's poll() is waiting on, is that write guaranteed to wake the loop?"
A: If the fd is REGISTERED with the loop (xtc_io_reg_fd / a task parked via
   xtc_proc_wait_fd) then yes on both backends (multishot / level-triggered). But a
   carrier scheduler that leases sessions round-robin does not have every runnable
   session's fd in every carrier's poll set -- so the reliable mechanism is the loop
   nudge, not fd-write-racing.

Q: "Does test_sim_wake_park cover the io_uring backend and the foreign-fd case?"
A: test_sim_wake_park is the STRUCTURAL DST guard for the class (a cross-loop
   spawn/send onto a parked loop is always scheduled or the run fails quiescence); it
   exercises the inbox-driven wake path (what xtc_proc_wake/xtc_loop_wake use), not a real
   io_uring fd race. The real-backend fd race is covered by m2 test_io_wakeup (foreign
   pthread -> xtc_io_wakeup -> xtc_io_poll(-1) returns, lost-wake-free) and now m5
   cross_wake Cw5 (a foreign OS thread nudges a loop via the public xtc_loop_wake handle
   and the parked task resumes / the run quiesces).

Q: options A / B / C.
A: A is not needed (foreign registered fds are already multishot/level-triggered, no
   window). B is the answer and is now first-class: xtc_loop_wake() is the guaranteed,
   lost-wake-free loop kick; call it after marking runnable. C (a sanctioned "loop kick"
   so consumers never race arbitrary-fd readiness) is exactly what xtc_loop_wake is.

--------------------------------------------------------------------------------
## What we shipped (v1.39.0)

- **xtc_loop_wake(xtc_loop_t *)** -- public, lost-wake-free, any-thread loop nudge; thin
  wrapper over xtc_io_wakeup(loop->io) usable with an xtc_exec_loop() handle (previously
  loop->io was unreachable through the opaque xtc_loop_t -- that was the only real gap:
  the contract and the proc-level primitive already existed, the loop-level public handle
  did not).
- Documented the cross-thread producer-must-nudge contract on xtc_loop_wake (header +
  xtc_loop(3)) and cross-referenced xtc_proc_wake.
- Test: m5 cross_wake Cw5 -- a raw pthread (foreign to libxtc) fires a parked task's
  condition and nudges the loop via xtc_loop_wake; the executor run must quiesce.
- Full CI green (all backends incl. io_uring/epoll, sanitizers, DST), man-coverage 572,
  amalgamation 1.39.0.

--------------------------------------------------------------------------------
## Recommended PG-side change (maps to your plan's B1 Track A, now the whole fix)

Where you mark a parked session runnable from another carrier (MarkRunnable /
signal_ready_work), after the enqueue add the loop nudge for the target carrier loop(s):

    // after: session marked runnable / pushed to the carrier's ready set
    xtc_loop_wake(xtc_exec_loop(g_xtc_exec, target_idx));   // v1.39.0

Ordering rule (this is the lost-wake-free part, mirror libxtc's own): make the session
runnable FIRST, THEN xtc_loop_wake; and the carrier, on wake, re-checks its ready set
(you already do). A spurious wake is always safe (the loop just re-polls). If today you
signal "any carrier" via one eventfd, either nudge the specific target loop you enqueued
onto, or nudge each carrier loop -- both are correct; nudging the target is cheaper.

This should close B1 without a libxtc round-trip. Once it's in, your
pg_stat_database-under-write-load repro should no longer hang and the HammerDB monitor VU
should survive -- unblocking the write-path measurement (B2) and the full matrix (P3).

Two related notes for your plan:
- B3 (explicit-carriers>cores wedge; thread-per-session c>=64 wedge): if those are also
  lost-wake stalls, the same nudge discipline applies wherever you make a
  cross-thread session runnable. If a wedge persists AFTER the xtc_loop_wake fix and gdb
  still shows all loops in xtc_io_poll with a pending inbox/ready item, that WOULD be a
  libxtc bug -- send us that gdb + a repro and we'll dig in immediately.
- Your "what works well" list (custom transport, retry-mode AGAIN, ALPN, ctx/fd
  ownership) needs no change; keep it.

Happy to review the exact MarkRunnable diff if you paste it.
