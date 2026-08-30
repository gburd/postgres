# libxtc reply: CONFIRMED second bug -- close-vs-write race on the blocking completion pipe under concurrent commit. Fixed in v1.40.2.

Date: 2026-08-29
libxtc: fixed in v1.40.2 (commit dec8428); analysis against v1.40.1 (ebc430c) as you filed.
Re: /tmp/libxtc-fiber-resume-concurrent-commit-2026-08-29.md

--------------------------------------------------------------------------------
## TL;DR

You were right again -- v1.40.1 fixed the single-fiber strand, but a SECOND,
concurrency-gated resume bug remained, and it is a genuine libxtc data race (not PG).
Fixed in v1.40.2. Upgrade and re-run your 64-client TPC-B; no PG-side change needed.

Your hypothesis 2 was the closest: it is a completion-vs-teardown race on the offload path,
surfaced only under high steal churn / spurious wakes. ThreadSanitizer caught it precisely.

--------------------------------------------------------------------------------
## Root cause (what v1.40.1 did not cover)

The stranded holder in your gdb parked via xtc_aio_fdatasync. On a build/config where the
native io_uring FSYNC engine is not used (io_method=sync, or the ring declines the op),
xtc_aio_* falls back to xtc_blocking_run -- the thread-pool offload. That is the path that
raced.

xtc_blocking_run parked the fiber on its completion pipe with xtc_proc_wait_fd, and then
UNCONDITIONALLY did: read(pipe) + close(pipe) + return -- trusting that one wake meant "my
completion arrived." But xtc_proc_wait_fd wakes are explicitly allowed to be SPURIOUS: a
cross-thread nudge, an eager-rebalance poke of an idle peer, or a stray xtc_proc_wake all
resume a parked fiber, and the contract is that the waiter must RE-EVALUATE its condition
(this is the same producer-must-nudge / spurious-wake-safe contract from the v1.39 reply).
xtc_blocking_run violated its own side of that contract: it did not re-check that the work
had actually completed.

Under your workload (many migratable client fibers, eager rebalance, high steal churn,
64 concurrent committers) spurious wakes are frequent. Sequence at the freeze:

  1. Committer fiber F wins WALWriteLock, calls xtc_aio_fdatasync -> xtc_blocking_run,
     enqueues the flush to a pool worker, parks on its completion pipe.
  2. Before the pool worker writes the completion byte, a spurious wake (an
     eager-rebalance nudge, or a cross-thread wake meant for F's steal/rebalance) resumes F.
  3. F, trusting the wake, read()s + close()s BOTH pipe fds and returns from
     xtc_blocking_run -- freeing the on-stack struct blk_work.
  4. The pool worker, still executing, write()s the (now closed / possibly fd-reused) pipe
     and stores its result into the freed on-stack work item.

That is a close-vs-write data race on the completion pipe plus a use-after-free of the
caller's stack. ThreadSanitizer flags it exactly:

    Write (close)  xtc_blocking_run  blocking.c   [fiber thread]
    Read  (write)  blk_worker        blocking.c   [pool-worker thread]
    Location is file descriptor NN created by the fiber's pipe()

Functionally, F returns from its flush "successfully" without the flush's result being
correctly synchronized (and having corrupted/closed the completion channel), so it
proceeds to release WALWriteLock incorrectly or, in the observed production case, the
completion accounting is lost and F is treated as still-in-flight -- either way the WAL
commit pipeline stalls behind it after the first burst. It is concurrency-gated (needs the
spurious-wake timing), which is why single-connection init is now 0/8 (v1.40.1) but 64-way
commit collapses after ~2 s.

This is distinct from and complementary to the v1.40.1 idle-poll-dispatch fix: that one
was "the loop dropped a reaped completion event"; this one is "the fiber acted on a
spurious wake as if it were its completion, and tore down the channel out from under the
worker."

--------------------------------------------------------------------------------
## The fix (v1.40.2)

Gate completion on an explicit flag, not on the wake. struct blk_work gains a
`_Atomic int done`:

  - The pool worker stores done = 1 with release AFTER its wakeup write (its last touch of
    the work item).
  - xtc_blocking_run re-parks until it observes done == 1 (acquire); a spurious wake simply
    re-parks. Only after done is observed does it read the byte and close the pipe.

Because done is stored after the write, the caller's done==1 observation strictly follows
the worker's write, so the read + close (and the release of the on-stack work item) can no
longer race the worker. A spurious wake before the worker runs no longer tears the channel
down. (xtc_blocking_run_off_loop -- the bare-thread variant -- already blocked on a real
read() and was race-free; it just shares the worker, which now always publishes done.)

Verified: on a concurrent-commit stress (many migratable fibers doing repeated
aio-write+fdatasync under a shared flush lock held across the park, with eager rebalance),
the blk_worker-write-vs-close ThreadSanitizer race goes from present to zero. Full make
check green. Regression exercise added: m5 exec/Blk2_concurrent_commit.

--------------------------------------------------------------------------------
## Answers to your hypotheses

1. "A completion reaped between step_once-idle and the idle poll, or during rebalance, that
   neither loop dispatches." Not this -- v1.40.1 closed the idle-poll drop, and the three
   poll sites (step, fairness-quantum, idle) all dispatch now. The remaining bug was on the
   OFFLOAD completion channel, not the loop's event dispatch.
2. "Other xtc_io_poll sites that reap without dispatching." Audited: no. The idle poll was
   the only one; it dispatches since v1.40.1.
3. "A migratable fiber parked on an eventfd (xtc_pg_wait_fd) woken cross-thread while its
   loop is idle -- does it resume post-1.40.1?" Yes, that path is sound: xtc_task_waker
   captures the CURRENT loop (the steal loop), so xtc_proc_wake nudges the correct loop's
   inbox, and fd readiness on that loop is level-triggered / multishot. Your
   LWLockAcquireOrWait waiters were stuck as SECONDARY victims -- blocked behind the
   stranded holder, not independently lost. Once the holder resumes (this fix), they drain.

--------------------------------------------------------------------------------
## What you should do

1. Upgrade to libxtc v1.40.2 and re-run: pgbench -c 64 -j 16 -T 60 -P 5. The post-2s
   collapse should be gone. Your parking code is correct as written; this was ours.
2. If you run with the native io_uring FSYNC engine (io_method != sync and the ring accepts
   IORING_OP_FSYNC), that path does NOT go through xtc_blocking_run and was not subject to
   this specific race -- but v1.40.2 is still the right baseline (the fallback offload is
   reachable whenever the ring declines an op, and your forced-offload runs hit it every
   time).

## A note for your gdb tooling
You asked for a way to enumerate parked procs / find the holder's park_fd loop. We don't
ship a gdb helper for the proc table yet; if you want one we can add a
tools/gdb/xtc-gdb.py command that walks the executor loops and lists each loop's parked
tasks + their park_fd. Say the word and we'll add it -- it would have let you point at the
exact loop here, though TSan got us there faster.

## When to come back to us
If v1.40.2 still collapses under 64-way commit with a fiber off-thread holding WALWriteLock,
capture: (a) whether you're on the native io_uring fsync path or the offload path, and
(b) a ThreadSanitizer run of your workload if you can build PG under -fsanitize=thread (even
partial). TSan found this one in minutes where reasoning did not. Thank you again for the
crisp, reproducible report.
