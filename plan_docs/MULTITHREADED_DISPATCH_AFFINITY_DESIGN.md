# Per-transaction re-dispatch fix -- design (2026-08-05)

> DECISION (2026-08-05): Increment 1's affinity_runnable_queue was IMPLEMENTED,
> twice-reviewed, then REMOVED WITHOUT MEASURING -- it is the wrong layer.  A
> pooled session is NOT a libxtc fiber that yields; it runs to a
> PG_STEP_PARK_PROTOCOL_READ boundary and RETURNS to the carrier's for(;;) loop,
> which re-dispatches it from PG-side queues.  That is a SECOND, hand-rolled
> cooperative scheduler on top of the carrier's xtc_exec loop, and the affinity
> queue just made that hand-rolled scheduler more elaborate -- the opposite of
> fusing with libxtc.  See "ARCHITECTURAL CONCLUSION" at the bottom.  The
> increments below are kept as the RECORD of what the hand-rolled path would
> need; the actual plan is the libxtc-native redirection.

Fixes the beat-fork blocker isolated in .ec2/ab4-20260804-metal/FINDINGS.md:
under HammerDB OLTP mt runs at ~1% of fork because every transaction re-cycles
a session through the pooled scheduler's park -> wake -> re-lease path (36%+
__x64_sys_futex).  pgbench (persistent conns) is at parity (929k tps, clean
profile), so the healthy path already exists -- OLTP just needs to reach it.

## Current mechanism (traced, HEAD 09f87ae986f)

Per transaction/command on a pooled session:
1. Session finishes a command, has no buffered next command -> parks:
   PgRuntimeProtocolSchedulerParkBackend() pushes park_state->scheduler_node onto
   the GLOBAL scheduler->parked_protocol_queue (spinlock), records
   park_state->parked_carrier = the carrier it was on.
2. A carrier polls parked session fds via PgRuntimeProtocolSchedulerWaitParkedReads
   (poll() over the parked fds + the shared pooled_protocol_wake_fd eventfd).
3. Client's next command arrives -> the fd is readable -> PgRuntimeProtocolScheduler
   MarkRunnable() moves the node to the GLOBAL scheduler->runnable_queue, and
   backend_pooled_protocol_wake_signal() writes the SHARED eventfd to wake a
   carrier blocked in poll().
4. SOME carrier wakes, drains the eventfd, and PgCarrierLeaseRunnableProtocolBackend
   -> PgRuntimeProtocolSchedulerPopRunnable() pops the GLOBAL FIFO runnable queue
   -- IGNORING park_state->parked_carrier -- and runs the session.

Two costs, per transaction, that fork does not pay:
- (A) a futex/eventfd wake (write + poll wakeup + read-drain) to hand the session
  to a carrier;
- (B) NO affinity: PopRunnable is a global FIFO, so the session usually resumes
  on a DIFFERENT carrier than parked_carrier -> cache-cold PGPROC/session state
  + contention on the single global runnable queue spinlock + shared eventfd
  thundering herd across all idle carriers.

The affinity hook ALREADY EXISTS: PgBackendProtocolParkState.parked_carrier is
recorded at park time and simply not consulted at lease time.

## The fix -- three layered increments, each A/B-gated on the metal HammerDB run

### Increment 1 -- STICKY SESSION-TO-CARRIER AFFINITY  (highest leverage)
Goal: a session resumes on the SAME carrier it parked on whenever that carrier
is available, so it behaves like pgbench's persistent-conn path (no cross-carrier
bounce, no global-queue contention).

Design:
- Add a per-carrier runnable sub-list: PgCarrier gains an affinity_runnable
  dlist + count (or the scheduler keeps a small map loop_id -> runnable sublist).
  Simpler + lower-risk: keep the single global runnable_queue but have
  MarkRunnable, when park_state->parked_carrier is non-NULL and still registered/
  alive, push the node onto that carrier's OWN affinity_runnable list instead of
  the global queue.  PopRunnable(carrier) then checks its own affinity list FIRST,
  falls back to the global runnable_queue (work-stealing) so a dead/busy carrier's
  sessions are not stranded.
- Invariants to preserve (these are the load-bearing bits -- a mistake = hang or
  cross-fiber corruption):
  * All queue-state transitions stay under scheduler->lock (the spinlock);
    the affinity sublist is part of that protected state.
  * scheduler_queue_state stays the single source of truth; add no new state,
    reuse RUNNABLE (the node is RUNNABLE whether on the global or an affinity
    list -- carry a 1-bit "on_affinity_list" or a back-pointer so
    MarkRunnable/Lease/Remove/PopRunnable dequeue from the RIGHT list).
  * parked_carrier may have EXITED between park and wake (carrier crash / pool
    shrink): if the recorded carrier is no longer registered, fall back to the
    global queue.  Never deref a freed carrier -- validate against the scheduler's
    registered-carrier set under the lock.
  * Work-stealing fallback MUST remain: if a carrier finds its affinity list empty
    it must still drain the global queue, or a session whose home carrier is busy
    on a long query would starve.  Balance: prefer affinity, never at the cost of
    a runnable session sitting idle while a carrier spins.
- Files: backend_runtime_backend.c (MarkRunnable, PopRunnable, Lease, Remove,
  Park); backend_runtime.h (PgCarrier affinity list fields; park_state list
  tag); the carrier loop in launch_backend.c only if PopRunnable's signature
  changes (it already takes the carrier via CurrentPgCarrier).
- Expected effect: removes cost (B).  Should recover most of the gap on its own
  (pgbench parity proves same-carrier resume is cheap).

### Increment 2 -- WAKELESS RESUME  (removes cost A when possible)
Goal: skip the eventfd write when the target carrier will notice the runnable
session on its own next poll iteration.

Design:
- The home carrier is, in steady OLTP, sitting in WaitParkedReads' poll() with
  its OWN parked session fds in the poll set (including the just-readable one).
  When a session becomes runnable BECAUSE ITS OWN FD polled readable on its home
  carrier, that carrier already knows -- no eventfd write is needed at all (the
  poll already returned).  The eventfd write is only needed to wake a DIFFERENT,
  idle-blocked carrier (the affinity-miss / work-steal case).
- So: backend_pooled_protocol_wake_signal() becomes conditional -- only fire the
  shared eventfd when the newly-runnable session is NOT going to the currently
  polling carrier, i.e. only on the affinity-miss path or when a carrier is
  known idle-blocked.  When MarkRunnable happens on the same carrier that is
  actively draining its poll result, set the node runnable and let the carrier's
  existing loop pick it up with NO syscall.
- Invariant: NEVER lose a wakeup.  The safe rule: default to waking (current
  behavior) and ELIDE the wake only when we can prove the target carrier is
  already running/polling and will re-check the runnable list before blocking.
  A conservative elision (only when the carrier is mid-poll-drain on its own fd)
  cannot lose a wake; anything less certain keeps the eventfd write.
- Files: launch_backend.c (wake_signal call sites, the carrier loop's
  drain/recheck ordering), backend_runtime_backend.c (MarkRunnable returns
  enough info -- "did this go to the running carrier?" -- to decide).
- Gate this SEPARATELY from Inc 1: a lost-wakeup here is a hang.  Soak with the
  concurrency test + the notify/LISTEN test before trusting it.

### Increment 3 -- F1 COUNTERS OFF THE HOT DISPATCH PATH
xtc_counter_add showed ~1.5% on the hot path (sessions_leased/resumed/wakes are
incremented per transaction).  xtc_counter_add is per-CPU-sharded but still a
function call + shard index + atomic on the hottest path.
Design: keep a plain uint64 per-carrier local counter (no atomic -- only the
owning carrier writes it) for the per-transaction counters (leased, resumed,
wakes); sum the per-carrier locals when pg_stat_xtc_runtime is read (the SRF is
already the slow path).  Leave the rare counters (carriers_started,
process_fallbacks) on xtc_counter_add.
- Files: pg_xtc_carrier.c (the counter add helpers + snapshot), PgCarrier
  (per-carrier counter array).
- Lowest risk, smallest win; do it last or fold into Inc 1's PgCarrier change.

## Validation (each increment, before the next)
- Build -Dxtc=enabled -Dcassert=on: gmake check-threaded-pooled +
  test_backend_runtime (005 notify, 007/009 pooled deep waits, 015 storm) +
  gmake check (process byte-for-byte).
- Correctness soak: the concurrency/notify tests under load (lost-wakeup hunt).
- Metal A/B (coordinator-run): HammerDB srv_tpm mt vs fork @192/@384 -- the
  number must move toward parity and pgbench@384 must stay ~929k (no regression
  to the healthy path).  KEEP only if neutral-or-better.

## Sequencing
Inc 1 first (biggest win, self-contained), measure.  Then Inc 2 (needs Inc 1's
affinity to know "same carrier"), measure.  Inc 3 anytime (fold into Inc 1's
PgCarrier struct change).  Sequential -- all three touch the same hot files and
a lost-wakeup/queue-state bug is a hang.

## ARCHITECTURAL CONCLUSION (2026-08-05) -- remove the affinity queue; fuse with xtc_exec instead

Inc 1's affinity_runnable_queue was removed (never A/B'd) because it deepens the
wrong architecture rather than fixing it.  The root problem is that the pooled
protocol scheduler is a PG-SIDE cooperative scheduler:
  PgSessionStepUnprotected runs the protocol until PG_STEP_PARK_PROTOCOL_READ,
  then RETURNS; the carrier's for(;;) loop commits the park to a PG queue
  (parked_protocol_queue / runnable_queue), polls parked fds, and re-leases
  sessions by hand -- with a shared wake eventfd and (in Inc 1) a per-carrier
  affinity list.  That is xtc_exec's job, re-implemented in PostgreSQL.

The libxtc-native design (the actual north-star fusion): a pooled session is a
real libxtc fiber.  At the protocol-read boundary it calls xtc_pg_wait_fd on its
client socket and YIELDS.  xtc_exec resumes it when the socket is readable.
xtc_exec ALREADY provides run queues, work-stealing, loop locality (affinity),
and wakeless resume -- so this DELETES the PG-side parked_protocol_queue,
runnable_queue, the shared pooled_protocol_wake_fd, WaitParkedReads,
MarkRunnable/PopRunnable/Lease, and the whole per-transaction re-dispatch loop,
rather than adding an affinity list to them.  It is less code, and it is the
fusion the north star asks for (adopt xtc behaviours, dedup PG plumbing onto
xtc).

Why this was not obvious up front, and the risk to weigh before doing it: the
PG-side scheduler exists because a stackless boundary (run-to-park-then-return)
lets the carrier drop the session's C stack between transactions, and because
the protocol-read park has to compose with PG's interrupt/latch/timeout
machinery (CHECK_FOR_INTERRUPTS, ProcSignal, statement/idle timeouts) that a
naive xtc_pg_wait_fd yield must still honor.  A fiber that yields at the read
boundary holds a full C stack parked per idle session -- 192-384 idle sessions x
stack = memory the stackless design avoids.  So the redirection is real work
with a real tradeoff (stack memory per parked session vs. deleting the
hand-rolled scheduler + getting xtc_exec's locality for free), and it must be
MEASURED: does a fiber-yield-at-read-boundary session cut the 36% futex storm
and move HammerDB mt toward fork, at an acceptable parked-stack memory cost?

NEXT STEP (replaces Inc 1/2/3): design + prototype the fiber-yield-at-
protocol-read-boundary session on a SPIKE branch, A/B it against the current
PG-side scheduler on the metal HammerDB workload (srv_tpm mt vs fork, futex%,
RSS per idle session).  Keep the current PG-side scheduler until the fiber-yield
variant is measured neutral-or-better on throughput AND acceptable on memory.
The Inc 2 (wakeless resume) and Inc 3 (F1 counters) items above become moot if
the redirection lands (xtc_exec does both); they stay only as fallback tuning
for the PG-side scheduler if the redirection proves too costly.
