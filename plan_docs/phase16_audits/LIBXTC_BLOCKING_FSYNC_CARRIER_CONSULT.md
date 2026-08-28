# libxtc consult: blocking fsync on a bare-pthread "carrier" freezes co-hosted work — is there a pool-offload for non-fiber callers, or should carriers be fibers?

Date: 2026-08-28
libxtc: v1.39.0 (tag f5b6e98 / commit 47eb8eb)
Consumer: PostgreSQL "xtc" branch, pooled-protocol carrier scheduler (multithreaded=on).
Classification: NOT a libxtc bug (libxtc behaves per its documented contract). This is a
DESIGN CONSULT + a possible small feature request. We want your read on the right shape
before we implement, since it touches how we use xtc_aio_* / xtc_blocking_run.

--------------------------------------------------------------------------------
## The problem we hit (PostgreSQL side)

Under a write-heavy OLTP load (64 concurrent clients, pgbench TPC-B, fsync=on, local
NVMe) the pooled scheduler WEDGES within seconds: throughput collapses, sessions hang,
CPU pinned ~70% then stuck, no recovery. Stock fork PostgreSQL on the same box does not.

Symbolized gdb (all threads) at the wedge, the load-bearing stacks:
  HOLDER:  fdatasync > pg_fdatasync > issue_xlog_fsync > XLogWrite > XLogFlush >
           RecordTransactionCommit > CommitTransaction > ... > exec_simple_query
  WAITERS: __futex_abstimed_wait > PGSemaphoreLock > ProcSemaphoreWaitCallback >
           LWLockAcquireOrWait > XLogFlush > RecordTransactionCommit > CommitTransaction

I.e. one "carrier" is in a BLOCKING fdatasync() inside the WAL-write critical section
(holding an exclusive lock, WALWriteLock), and the others are parked waiting for that
lock.

--------------------------------------------------------------------------------
## Why it wedges (root cause, on OUR side)

Our pooled carrier is a **bare OS pthread** (pthread_create), not a libxtc fiber. It runs
MANY PostgreSQL client sessions **inline** in a `for(;;)` loop (one carrier multiplexes N
sessions, running each to a protocol-read boundary, then the next). PostgreSQL's WAL
commit path takes WALWriteLock and calls fdatasync().

On that bare pthread, our fsync wrapper takes the plain BLOCKING fdatasync() path (we gate
the async path on an "in a backend fiber" flag, which is false on the carrier pthread). So
the carrier pthread blocks in fdatasync() for the whole disk sync WHILE:
  1. holding WALWriteLock (all other committers block on it), AND
  2. unable to return to its loop to run the OTHER sessions multiplexed onto it.
Under concurrency this starves the pool and wedges. Fork does not wedge because each
backend is its own process: a blocking fsync blocks only that process, the kernel runs the
rest, and group-commit amortizes the flush. The pooled model's "N sessions share one OS
thread" coupling is what turns one blocking syscall into a cohort freeze.

--------------------------------------------------------------------------------
## What we found in libxtc (v1.39), so you can confirm we read it right

We considered routing the fsync through xtc_aio_fdatasync / xtc_blocking_run so the fsync
runs on a pool thread and the caller yields. But both require a LOOP/FIBER context:

- aio.c aio_do(): `if (t == NULL || loop == NULL || loop->io == NULL) return
  aio_offload(...)`. aio_offload(): `if (xtc_blocking_run(fn,arg,&rc) != XTC_OK) rc =
  fn(arg); /* off a loop: run inline */` -- i.e. off a loop it runs the fsync INLINE
  (blocking) on the caller.
- blocking.c xtc_blocking_run(): `if (xtc_pid_is_none(xtc_self())) goto run_sync;` -- when
  the caller is NOT a loop process (our bare carrier pthread, xtc_self() is none), it runs
  the work SYNCHRONOUSLY on the caller. Documented and correct: it cannot park a
  non-fiber, so it degrades to inline.

So from a bare pthread, every xtc_aio_* / xtc_blocking_run degrades to an inline blocking
syscall on that pthread. That is exactly our freeze. libxtc is behaving as specified; the
mismatch is that our carrier is not a fiber.

--------------------------------------------------------------------------------
## Our two candidate fixes, and where we want your input

A. **Make the carrier a real libxtc fiber / run pooled sessions on an exec loop** (instead
   of inline on a bare pthread). Then pg_fdatasync -> xtc_aio_fdatasync PARKS the fiber and
   the loop runs sibling sessions during the sync -- the coupling disappears. This is the
   "right" shape and aligns with your model, but it is a large restructuring of our
   scheduler (today the carrier owns a for(;;) inline lease loop; making each leased
   session a schedulable fiber on the loop is significant, and touches our
   protocol-read-park mechanism).

B. **Offload just the blocking syscall to a pool thread from the bare pthread**, keeping
   the carrier a pthread. This needs a "run this blocking fn on a pool worker and let ME
   (a non-fiber OS thread) wait on a completion fd" primitive -- i.e. xtc_blocking_run's
   pool path, but callable from a NON-loop thread where the caller blocks on the
   completion pipe/eventfd rather than parking a fiber. Then the carrier pthread is only
   blocked for the handoff, and (more importantly) we could restructure so the carrier
   returns to serve siblings while a DIFFERENT mechanism owns the in-flight fsync.

### Questions for the libxtc team
1. Is there (or would you add) a **pool-offload usable from a bare OS thread** -- submit a
   blocking fn to the xtc thread pool and get a completion handle the CALLER can wait on
   (or poll) without being a fiber? Today xtc_blocking_run requires xtc_self() != none.
   Even a "detached submit + caller-supplied eventfd" would let us decouple the fsync from
   the carrier pthread.
2. For a consumer that multiplexes many logical tasks onto one OS thread and must
   occasionally do a blocking syscall: is your recommendation unambiguously "make them
   fibers on a loop" (option A)? If so, any guidance/pitfalls for running a large,
   dynamic set of fibers where each is a long-lived session that mostly parks on a socket
   read and occasionally does WAL fsync?
3. Is there any libxtc facility for **cooperative fsync batching / group offload** (many
   fibers wanting fdatasync on the same fd coalesced into one pool op) that we should use
   rather than building our own group-commit-aware offload?

We suspect the answer is "option A: sessions must be fibers," but before we commit to that
restructuring we want to confirm there isn't a lighter pool-offload path (option B) you'd
recommend or add, and to sanity-check the fiber-per-session direction for a
socket-parking, occasionally-fsyncing workload at 100s-1000s of sessions.

--------------------------------------------------------------------------------
## Reproduction (PostgreSQL, deterministic)
1. PG "xtc" branch (origin/xtc) on libxtc v1.39; multithreaded=on, pooled_protocol_carriers
   =-1 (auto = cores); fsync=on, synchronous_commit=on, PGDATA on local NVMe (xfs).
2. pgbench -i -s 100; then pgbench -c 64 -j 8 -T 300 (TPC-B, write-heavy).
3. Within ~8s: `select 1` from a fresh connection hangs; gdb -p <postmaster> "thread apply
   all bt" shows one carrier in fdatasync under XLogFlush/WALWriteLock and the rest parked
   in LWLockAcquireOrWait(WALWriteLock). No recovery.
Happy to share the full backtrace file and a scripted repro.

## What is NOT the issue (already handled)
- The cross-loop wake miss you helped us fix (v1.39 xtc_loop_wake) is in and works for the
  idle-session case. This wedge is a different, blocking-syscall-coupling problem.
- The custom TLS transport, retry-mode AGAIN, ALPN, ctx/fd ownership: all fine, no change.
