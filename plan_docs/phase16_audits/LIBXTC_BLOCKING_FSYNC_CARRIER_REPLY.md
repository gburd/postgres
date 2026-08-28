# libxtc reply: blocking fsync on a bare-pthread carrier -- the fix is fibers (option A); we added the pool-offload primitive you asked for (option B), but it alone does NOT un-wedge you

Date: 2026-08-28
libxtc: now v1.40.0 (adds xtc_blocking_run_off_loop); analysis is against v1.39.0 (47eb8eb) as you filed.
Re: /tmp/libxtc-blocking-fsync-carrier-consult-2026-08-28.md

--------------------------------------------------------------------------------
## TL;DR

- Your root-cause analysis is correct, and your reading of libxtc is correct: from a bare
  pthread, xtc_blocking_run / xtc_aio_* degrade to an inline blocking syscall. That is the
  freeze.
- **The answer to "which shape" is unambiguously option A: run the pooled sessions as
  fibers on an xtc loop.** That is the only shape where a blocking fsync stops freezing the
  cohort, because only a fiber on a loop can yield the carrier to sibling sessions during
  the sync.
- We DID add the primitive you asked for in question 1 (v1.40.0
  `xtc_blocking_run_off_loop`) -- a pool offload usable from a bare OS thread that returns
  a result. But be clear-eyed: **used from your carrier pthread as-is it does NOT fix the
  wedge**, for two reasons below. It is a building block for a group-commit redesign, not a
  drop-in fix. We're giving it to you because you asked and it's a legitimate gap, but we do
  not want you to adopt B thinking it solves the freeze -- it doesn't.

--------------------------------------------------------------------------------
## Why option B (offload from the bare pthread) does NOT un-wedge you

Your wedge has TWO coupling facts, and moving the syscall to a pool thread fixes NEITHER on
its own:

1. **The carrier still can't serve siblings during the sync.** A bare pthread cannot be
   parked-and-resumed by libxtc (that is what a fiber is). So `xtc_blocking_run_off_loop`
   blocks the CALLING thread on the completion pipe for the whole sync -- the carrier is
   still stuck, still not running its other N-1 leased sessions. You moved *where the
   fdatasync executes* (pool worker vs carrier thread) but not *whether the carrier is
   blocked* (it is, on the pipe read). The cohort freeze is unchanged.

2. **The WALWriteLock hold time is unchanged.** Your holder is in fdatasync WHILE holding
   WALWriteLock. Offloading fdatasync to a pool thread does not shorten that hold -- the
   lock is held from before the offload until the result comes back, i.e. the same wall
   time. Every other committer still blocks on WALWriteLock for the full sync. (This is
   true even in stock fork PG; the reason fork survives is group commit amortizing the
   flush across many waiters + each backend being its own schedulable process, not any
   property of where fsync runs.)

So B, applied naively, gives you: carrier blocked on a pipe instead of on fdatasync, lock
held just as long. No improvement. The ONLY way B helps is if you build a group-commit
design on top of it (submit one flush for a batch of committers, one carrier waits, wake
many), which is real work and is exactly what stock PG's XLogFlush already does with its
"someone else flushed my LSN" fast path -- so you'd be reimplementing group commit around
an offloaded fsync. Doable, but it is not lighter than option A.

--------------------------------------------------------------------------------
## Why option A (fibers on a loop) is the right shape -- and answers to Q2/Q3

**Q2: is the recommendation unambiguously "make them fibers on a loop"?  Yes.**

The whole value proposition of libxtc for your carrier model is: a fiber that hits a
blocking point (an fsync via xtc_aio_fdatasync, a socket read, a lock wait) PARKS, and the
loop immediately runs the other ready fibers on that carrier. That is the coupling you want
to break, and it only breaks when the session is a schedulable fiber, not an inline
for(;;) iteration on a bare pthread. With sessions-as-fibers:

- pg_fdatasync -> xtc_aio_fdatasync parks the committing fiber; the carrier loop runs
  sibling sessions during the sync. The "one blocking syscall freezes the cohort" coupling
  is gone by construction.
- You get the same behavior for socket reads (which you already park on) and for lock
  waits, uniformly -- one scheduling model, not "fibers for reads, inline for fsync".

Guidance / pitfalls for a large, dynamic set of long-lived session fibers that mostly park
on a socket read and occasionally fsync (your exact workload):

- **Fibers are cheap to have, not free to churn.** 100s-1000s of long-lived parked fibers
  is fine and idiomatic (a parked fiber costs its stack + a small proc record; the loop
  only iterates ready ones). What is expensive is spawn/exit churn -- so make the session
  fiber long-lived (spawn on connect, park on read, exit on disconnect), not
  per-request.
- **Stack sizing matters at scale.** PostgreSQL's per-backend C stack use (deep planner /
  executor recursion) sets the fiber stack size; size it from your deepest real query, not
  the default. At 1000s of fibers the stacks dominate RSS -- use xtc's guard-page stacks
  (you get a clean SIGSEGV on overflow, not silent corruption) and consider
  xtc_stack_reclaim for idle parked sessions if RSS is tight.
- **Keep the fsync on the loop's async path, never inline.** Once sessions are fibers, gate
  your fsync wrapper on "am I a fiber" = xtc_self() != none (you already have this flag,
  just make it TRUE on the session fiber) and route to xtc_aio_fdatasync. Do NOT keep an
  inline fdatasync branch reachable from a fiber -- that reintroduces the freeze on one
  carrier.
- **Watch WALWriteLock, not just the syscall.** Even with fibers, if a fiber holds
  WALWriteLock across the parked fsync, sibling committer fibers on the SAME carrier that
  need the lock will park on it (correct) -- but siblings doing non-WAL work run freely.
  That is the fork-equivalent behavior you want. Combine with group commit (below) so you
  are not doing one fsync per commit.

**Q3: does libxtc offer cooperative fsync batching / group offload?  No, and you should
not build it in libxtc's layer -- do it in PG's XLogFlush, which already is group commit.**

libxtc deliberately has no fsync-coalescing facility: coalescing "many committers, one
flush, correct wakeup of everyone whose LSN was covered" is a WAL-protocol concern (which
LSN did the flush reach, who is now durable), not a generic runtime concern -- it needs to
know about LSNs and the WAL layout. PostgreSQL already implements exactly this in XLogFlush
/ the WALWriteLock + "did someone flush past my LSN while I waited for the lock" fast path.
The right move is: keep PG's existing group-commit logic, and make the ONE fiber that wins
WALWriteLock and performs the flush do it via xtc_aio_fdatasync (parking, so its carrier
runs siblings); the other committers park on WALWriteLock as fibers and, on wake, take the
"already flushed past my LSN" fast path exactly as today. That gives you group commit +
non-blocking flush with no new coalescing code in either layer.

--------------------------------------------------------------------------------
## What we shipped (v1.40.0), and honestly what it is for

New public API, <xtc_blocking.h>:

    int xtc_blocking_run_off_loop(int (*fn)(void *), void *arg, int *out_result);

- Offloads fn(arg) to the xtc thread pool and blocks the CALLING (non-fiber) OS thread on
  the completion pipe with a real read(2), instead of parking a fiber. fn runs on a pool
  worker, not inline on the caller.
- Reuses the exact pool + completion-pipe machinery xtc_blocking_run already uses (the same
  struct blk_work, minus the fiber park).
- Rejected (XTC_E_INVAL) if called from a fiber -- use xtc_blocking_run there, which yields.
- Documented contract (header + xtc_blocking(3)) states plainly: it is still SYNCHRONOUS
  from the caller's view (blocks the calling thread) and does NOT shorten a lock held across
  the call. It is a building block for a group-commit/batching offload, not a way to let a
  non-fiber caller do other work during the call.

This closes the real API gap in your question 1 (there was no "offload + non-fiber caller
collects the result" -- xtc_blocking_run needs a fiber, xtc_blocking_submit is
fire-and-forget). Use it if you build a batching offload. But per the section above, it
alone will not stop the wedge -- option A will.

Test added: m9 blocking/off_loop (a bare thread offloads, provably runs fn on a pool worker,
guards NULL fn and from-a-fiber calls). Full CI matrix green; man-coverage 573;
amalgamation 1.40.0.

--------------------------------------------------------------------------------
## Recommended path for you

1. **Adopt option A: sessions become fibers on the carrier's loop.** Spawn a session fiber
   on connect; it parks on socket reads (you already do this) and on fsync via
   xtc_aio_fdatasync; it exits on disconnect. This is the restructuring you flagged as
   large -- it is, but it is the fix, and it makes ALL your blocking points (reads, fsync,
   lock waits) uniformly non-freezing. Your existing protocol-read-park mechanism is the
   template; the fsync park is the same shape.
2. **Keep PG's group commit; route only the flushing fiber's fdatasync through
   xtc_aio_fdatasync.** No new coalescing code.
3. **Set xtc_self()==fiber true on the session fiber** so your fsync wrapper takes the async
   path (today it is false on the carrier pthread -- that gate is exactly what forced the
   inline path).
4. Use xtc_blocking_run_off_loop ONLY if you decide to build a bare-pthread group-commit
   offload instead of A -- and only after weighing that it is not less work than A.

If you want, share the carrier lease-loop and the protocol-read-park code and we'll sketch
the fiber-per-session conversion against it -- that is the highest-leverage help we can give
here.

## Sanity checks / when to come back to us
- After conversion, if a session fiber's xtc_aio_fdatasync is NOT parking (carrier still
  freezes with a fiber in fdatasync), that WOULD be a libxtc issue -- send the gdb showing
  a fiber (not a bare pthread) stuck inline in fdatasync and we'll dig in.
- At 1000s of fibers, if you see RSS blowup or spawn latency, that's a stack-sizing /
  stack-reclaim tuning conversation, not a redesign -- ping us with the numbers.
