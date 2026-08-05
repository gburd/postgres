# Oversubscription A/B thesis + the libxtc-native path (2026-08-05)

## The reframe (from the user, and confirmed by tracing the code)

The fork-vs-fiber win is NOT expected at 1x cores (one runnable unit per core --
Linux CFS and xtc_exec both cope).  It is expected under OVERSUBSCRIPTION: 2x
clients = 2x runnable units.  Stock forks 2x-cores OS processes and hands the
whole N:M scheduling problem to the kernel (context switches, run-queue
contention, per-process RSS, TLB flushes).  libxtc keeps it in ONE process and
xtc_exec does M:N fiber scheduling in userspace (cheap switches, no per-process
overhead, cache-warm carriers).  Every A/B so far ran at 1x (192 clients / ~192
units), where the fiber advantage is invisible or negative -- we never tested
the thesis.

## What the code actually is (traced 2026-08-05 -- decisive)

There are TWO threaded modes:

1. THREAD-PER-SESSION (pooled_protocol_carriers=0): each client backend is a
   REAL, SUPERVISED libxtc fiber -- xtc_proc_spawn_monitor(loop, xtc_carrier_proc,
   ...) one per backend (pg_xtc_carrier.c), running the NORMAL unmodified
   PostgresMain.  At the client-read boundary secure_read -> pq_recvbuf ->
   WaitEventSetWaitBlock detects xtc_in_backend_fiber and calls
   xtc_pg_wait_fd(epoll_fd) -> xtc_proc_wait_fd -> YIELDS to xtc_exec.  The
   fiber-executor loop count is sized to core count (capped 256), so N backend
   fibers run over ~ncore carrier loops and xtc_exec does the M:N scheduling,
   with supervision, work-stealing, and locality.  THIS IS ALREADY THE FULL
   libxtc-NATIVE MODEL, including supervision.  No new code.

2. POOLED (the current default): backend_pooled_protocol_carrier_entry -- one
   carrier for(;;) loop leases many sessions from PG-side queues
   (parked_protocol_queue / runnable_queue) with a shared wake eventfd, runs
   each session inline to a PG_STEP_PARK_PROTOCOL_READ boundary, session RETURNS
   (stackless), carrier picks the next.  A hand-rolled PG-side cooperative
   scheduler ON TOP of xtc_exec -- a SECOND scheduler.  This is the path that
   futex-stormed to ~1% of fork at 384 VUs (ab4).  It exists to BOUND fiber
   count (a fiber holds a C stack; the pooled model reuses ~ncore stacks for
   many sessions).

So "make backends real fibers / lean fully into libxtc including supervision"
is ALREADY DONE in thread-per-session mode.  The pooled scheduler is the
deviation.  The affinity_runnable_queue (removed) was patching the deviation.

## The test the thesis calls for (NO new code)

Benchmark thread-per-session-as-fibers vs fork, at 1x AND 2x oversubscription:
  - fork (multithreaded=off): stock spawns N backend processes; Linux schedules.
  - mt thread-per-session (multithreaded=on, pooled_protocol_carriers=0): N
    backend fibers over ~ncore carrier loops; xtc_exec schedules in-process.
  - (keep mt pooled as a third lane for contrast -- expected to lose, the ab4
    futex storm.)
Oversubscription increment: if the box has C cores, test client counts at ~C
(1x) AND ~2*C (2x).  Metrics per lane: srv_tpm (committed txn/min, monitor-
independent), p95/p99 latency, RSS (fork's 2x-process memory vs the single
threaded process -- a key efficiency axis the user named), context-switch rate
(vmstat cs / perf sched), and futex%.

Hypothesis to confirm or kill:
  - At 1x: fork ~ mt (parity or fork slightly ahead).
  - At 2x: mt thread-per-session BEATS fork on srv_tpm and/or RSS and/or p99,
    because xtc_exec's userspace M:N scheduling beats the kernel scheduling 2x-C
    processes.  IF SO, thread-per-session-as-fibers is the answer and the pooled
    scheduler was a premature fiber-count optimization that xtc_exec makes
    unnecessary at these scales.  IF NOT (mt loses even at 2x), the fiber-stack
    memory or per-fiber overhead dominates and the pooled model earns its
    complexity -- then the real work is fixing the pooled scheduler's futex storm
    (fuse its dispatch onto xtc primitives), not deleting it.

## Guardrails for the run
- thread-per-session cannot fork IO workers -> io_method=sync on the mt lane
  (NOT xtc: ab3 showed xtc futex-storms OLTP; and sync is the fair
  thread-per-session default).  fork lane io_method=sync too.
- Watch the thread count (the 4634-explosion history): thread-per-session sizes
  the fiber executor to core count; confirm ~ncore loops, not a multiplier.
- Oversubscribing fibers > loops is COOPERATIVE (fibers yield at every wait), so
  no deadlock; the risk is a latency ceiling if a fiber CPU-hogs between yields
  -- OLTP yields constantly (every client read), so this is the friendly case.
- Coordinator runs the metal A/B (sub-agents leaked boxes 6x); verify teardown.
