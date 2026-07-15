# Phase 18 profiling: the gap is contention/idle, NOT per-command CPU

EC2 m6id.8xlarge (32 vCPU), release build + frame pointers, CPU-bound prepared
SELECT (`pgbench -M prepared -S`), 16 clients, `fsync=off`, 1GB shared_buffers,
system-wide `perf record`.  Same build, two lanes.

## Headline

    process         402k tps
    threaded_pooled 166k tps   (~41% of process)

But the profile shows the threaded lane is **CPU-STARVED, not CPU-heavy**:

    lane             swapper/idle    postgres on-CPU
    process          55%             32%   (PgSessionRun 31.7%)
    threaded_pooled  79%             ~9%   (carrier entry 8.6%)

The threaded server leaves **79% of a 32-core machine idle** while running at
41% of process throughput.  The bottleneck is NOT extra per-command work -- it is
**serialization**: work is not being dispatched to the idle CPUs.

## Evidence it is a single hot lock, not carrier starvation

Carrier-count sweep at 16 clients:

    carriers=8   161k tps
    carriers=16  172k tps   (+7% only)
    carriers=32  121k tps   (WORSE)

More carriers barely help and over-provisioning HURTS -- the classic signature
of a single contended lock every command must take: more threads just contend
harder.  The profile confirms kernel-side contention in the threaded lane that
is absent in process:

    __x64_sys_futex / do_futex     3.26%
    _raw_spin_unlock_irqrestore    3.07%
    __lll_lock_wake_private        2.51%
    __lll_lock_wait_private        ~1.2%

(These are low as CPU% precisely because contenders block OFF-CPU -- hence the
79% idle.  A quick-held but universally-taken mutex serializes everyone.)  The
lock-wait frames sit next to the per-command socket path (`__send`/`__poll`),
i.e. the contention is on the per-command protocol/wait-boundary, not per-session
setup.

## This OVERTURNS the plan's hypothesis

MULTITHREADED_PLAN.md Phase 18 assumed the gap was "per-command scheduling +
current-work (TLS bridge) indirection layered on top of libxtc."  The TLS bridge
accessors (`PgCurrent*Ref`) **do not appear** in the profile at all.  The gap is
lock/serialization contention starving the machine, so the right Phase 18 levers
are:

1. Find and de-contend the single per-command lock every carrier takes
   (candidates, in order of suspicion, to confirm with `perf lock record` /
   off-CPU BPF next: the per-command wait-boundary/latch lock around
   send/recv; the pooled-protocol queue mutex/cond on the dispatch path; a
   process-wide mutex such as `ThreadedGUCMutex`; a shared LWLock).
2. THEN, if useful, the libxtc-primitive fusion (sharded/lock-free dispatch,
   xtc-native wait) -- but pointed at the contention, not at CPU indirection.

The pooled-queue-vs-xtc_chan swap I earlier deprioritized as "per-session, not a
perf lever" may actually matter IF the queue mutex is the contended lock -- but
only measurement will say; do not guess.

## Next step

`perf lock record`/`bpftrace` off-CPU trace to NAME the contended lock exactly,
then de-contend it and A/B with mtpg_ab.  Everything before this was setup; this
is the finding that directs Phase 18.

## Follow-up: named + fixed the top lock; next layer is the kernel wait path

An off-CPU mutex trace (bpftrace uprobe on pthread_mutex_lock, counting by mutex
address, resolved against the postgres symbol table) named the exact contended
lock: **ps_status_mutex** (postgres+0x1105f40), taken **~4.15M times in 10s** --
orders of magnitude above any other mutex (next was ~450).  Every carrier
serialized on it to update the one shared process title on every command, which
is meaningless in a threaded server.

Fix (7af1ca5a5e1): update_ps_display_precheck() skips per-command updates under
multithreaded.  Result: 161k -> 192k tps (+19%); the mutex trace now tops out at
~450 (contention eliminated).

But the machine is STILL ~78% idle at 190k vs 400k process, and there is NO
remaining userspace-mutex contention.  The residual kernel cost is
`_raw_spin_unlock_irqrestore` (~2.9%) with `do_syscall_64` (~6.8%) -- the
futex/poll wait+wakeup path.  The carriers are parking at protocol boundaries
(waiting for the next command's socket data / a wakeup) and the machine is
starved because that wait/wakeup dispatch does not keep the CPUs fed, NOT because
of a hot userspace lock.

## Revised Phase 18/17 direction

The remaining gap is the **wait/wakeup boundary**, not a userspace lock and not
per-command CPU indirection:

- carriers block in the kernel (futex/poll) at each protocol boundary;
- more carriers do not help (sweep after the fix: 8=192k, 16=133k, 32=162k --
  still erratic, still far from process), consistent with a dispatch/wakeup
  bottleneck rather than compute or a single lock.

This is Phase 17 (wait-boundary) + the libxtc wait-fusion part of Phase 18:
make the carrier wait/wakeup at protocol boundaries cheap and scalable (xtc-native
readiness / batched wakeups / avoiding a kernel round-trip per command).  The
next investigation is an off-CPU / `perf sched` trace of WHERE the carriers block
and how wakeups propagate, then target that path.  ps_status was the first,
clearest win; the wait path is the bigger structural one.

## Root cause fully isolated: the pooled carrier idle loop (2026-07-13/14)

Latency isolation (EC2 m6id.8xlarge, prepared SELECT, ps_status fix in place):

    1 client:   process 0.030ms/33.5k tps  ==  threaded 0.030ms/33.3k tps  (IDENTICAL)
    16 clients: process 0.039ms/414k       vs  threaded 0.095ms/168k        (2.4x worse)

So there is NO per-command overhead -- at c=1 threaded latency equals process to
the microsecond.  The gap appears ONLY under concurrency, and crucially MORE
carriers make it WORSE:

    8 carriers / 16 clients   168k, 0.095ms
    16 carriers / 16 clients  105k, 0.152ms   (worse!)
    24 carriers / 16 clients  122k, 0.131ms

"More carrier threads -> worse" rules out carrier starvation and points at
contention that scales with the number of active carrier OS threads.

perf (16 carriers) named the kernel contention: `futex_wake ->
__lll_lock_wake_private` (a pthread cond/mutex WAKE) at ~5.8%, and
`try_to_wake_up -> wake_up_q -> futex_wake` on `_raw_spin_unlock_irqrestore`
(~5.3%), plus `sock_def_readable <- unix_stream_sendmsg <- __send`.  It is a
WAKE storm, not lock-hold contention (the mutex-lock trace was cold).

Source: the pooled carrier idle loop
(backend_pooled_protocol_carrier_entry, launch_backend.c ~1395):

    for (;;) {
        lease runnable backend; if found -> resume; continue;
        dequeue new session;     if found -> run; continue;
        nready = WaitParkedReads(..., 10L);   // poll() this carrier's parked fds, 10ms timeout
        if (nready) { signal_ready_work(nready); continue; }
        wait_for_work(10000L);                // pthread_cond_timedwait on the SHARED queue, 10ms
    }

Two scaling problems:
1. **Per-carrier busy-ish poll**: every carrier independently `poll()`s its parked
   fds with a 10ms timeout, so idle carriers wake ~100x/s and re-touch shared
   state; with N carriers that is N wake+recheck cycles hammering the one shared
   queue mutex/cond.
2. **Shared-queue cond wake storm**: `signal_work` (`pthread_cond_signal`) on each
   session launch + all carriers parked on the SAME `pooled_protocol_queue_cond`
   => futex_wake / try_to_wake_up storms that grow with carrier count.

### Fix direction (Phase 17/18 core -- the real gap-closer)

Restructure the carrier wait so it does NOT busy-poll and does NOT thundering-herd
on one shared cond:
- one BLOCKING wait per loop that batches all of that loop's parked-session fds
  (the loop's epoll already holds them via xtc_proc_wait_fd) with NO short
  timeout -- wake on actual socket readiness, not a 10ms timer;
- targeted / demand wakeup for new sessions (an xtc_chan mpsc or a per-carrier
  waker) instead of broadcasting on one shared cond that all carriers re-contend;
- this is exactly where libxtc wait-fusion (xtc-native readiness + per-loop waker)
  earns its keep -- and it must be A/B'd with mtpg_ab (target: threaded p50/p95/p99
  and tps approach process; machine no longer 78% idle at load).

This is the structural work to reach the goal (threaded >= fork on tps AND
p95/p99 latency AND resource footprint).  It is a scheduler-loop change, so it
must land incrementally with the A/B gate and the latency percentiles, not as a
big-bang rewrite.  The ps_status fix (+19%) was the first, isolated win; this
loop is the main event.

## eventfd wake fix landed (d1320cbdae0): real but partial (2026-07-14)

Added a shared wake eventfd to the carrier poll set so signal_work interrupts the
poll() directly; the per-carrier poll timeout dropped 10ms -> 1000ms (safety net).
Measured on EC2 (m6id.8xlarge, prepared SELECT, ps_status fix + this fix):

    lane          c1 latency/tps        c16 latency/tps
    process       0.030ms / 33.3k       0.038ms / 416k
    threaded  8c  0.030ms / 33.2k       0.090ms / 177k
    threaded 16c  0.030ms / 33.2k       0.089ms / 180k

Wins:
- The "more carriers = WORSE" collapse is GONE: 16 carriers was 105k before the
  fix, 180k now.  The 10ms self-wake storm is eliminated.
- Futex contention dropped from ~5.8% to ~2.7%.

But the ~2.3x gap remains (threaded ~180k vs process 416k) and the machine is now
~83% IDLE with LOW contention.  So the residual bottleneck is NOT lock contention
anymore -- it is **per-command carrier-cycle latency / insufficient parallelism**:
8 carriers round-robining 16 clients cannot saturate 32 cores because each
carrier's cycle (recv command -> execute -> send reply -> fiber park -> resume on
next) has more per-command latency than a dedicated process backend, and there
are only 8 of them.  c1 latency is identical to process (0.030ms), so the cost is
purely in the concurrency/dispatch cycle, not the command itself.

### Remaining direction (to reach process parity)

Two orthogonal levers, both to be A/B'd with mtpg_ab + p95/p99:
1. **More carriers by default** now that they no longer collapse: the auto-tuner
   caps at Max(8, cpus/4)=8 here; with the collapse fixed, carriers ~= clients
   (or ~= cpus) should recover parallelism.  Cheapest test first.  (Earlier
   "more carriers worse" was the busy-poll storm, now fixed -- re-tune the
   default.)
2. **Cut per-command cycle latency**: the fiber park/resume + epoll re-arm +
   reply-send wakeup per command.  This is the libxtc wait-fusion depth (batched
   readiness, avoid a kernel round-trip per command) -- the structural item.

Next: re-run the carrier sweep (now that collapse is fixed) to see if simply
raising the carrier default recovers most of the gap, before investing in
cycle-latency surgery.

## Carrier sweep after the eventfd fix: NOT a carrier-count problem (2026-07-14)

    carriers=16 clients=16  172k, 0.093ms
    carriers=32 clients=16  156k  (worse)
    carriers=32 clients=32  137k
    carriers=64 clients=32  124k

Raising carriers does NOT recover the gap -- throughput plateaus ~170-180k and
degrades with more carriers/clients, even though contention is now low (2.7%
futex) and the machine is ~83% idle.  So the residual is a **single shared
serialization point** every command touches that caps aggregate throughput at
roughly a fixed level regardless of parallelism -- classic Amdahl serial section.

Ruled out this session: per-command CPU (c1 == process), the ps_status mutex
(fixed), the 10ms busy-poll + shared-cond wake storm (fixed via eventfd).
Remaining suspect: a shared LWLock/spinlock or a libxtc loop-level lock on the
per-command execution or socket-I/O path (the residual _raw_spin_unlock_irqrestore
sits under sock_def_readable <- unix_stream_sendmsg, i.e. the reply-send wakeup,
and under futex_wake).  NEXT investigation: `perf lock` / LWLock-wait tracing (or
a bpftrace off-CPU trace keyed on the PG LWLock tranche) under load to name the
serial section, then de-contend/sharded it and A/B.

State of the gap after this session's fixes:
  process 416k / 0.038ms  vs  threaded ~180k / 0.089ms  (c16)
  -- collapse fixed, contention halved; the ~2.3x aggregate-throughput plateau is
     the next (and likely final major) structural target for process parity.

## Serial section localized to a HEAP-allocated mutex (2026-07-14)

Off-CPU trace (bpftrace sched_switch, prev_comm=postgres) under 16c/8carriers:
  do_poll        1,916,533   (carriers parked on parked-session fds -- normal)
  futex_wait     1,194,459   (blocked on a futex -- THE contention)
  do_nanosleep   4,856       (PG spinlock perform_spin_delay fallback -- small,
                              so the scheduler->lock SpinLock is NOT the issue)

So it is a futex/mutex WAIT, not the scheduler spinlock (which would spin/burn
CPU, but the machine is idle).  The futex-wait ustack is always
__lll_lock_wait_private (a pthread MUTEX slow path); the FP chain breaks in libc
so the PG caller is not visible from the stack.

Mutex-address counting (uprobe pthread_mutex_lock, arg0) shows the hot addresses
are 0x11c40a0 / 0x11e7bb0 / 0x1214680 -- and critically these are NOT any named
PG mutex:
  ThreadedBackendRegistryMutex 0x1104f00   ThreadedGUCMutex        0x1105dc0
  pooled_protocol_queue_mutex  0x10f9360   backend_thread_malloc_trim 0x10f92c0
  (none hot; cond_timedwait did not register -> wait_for_work is not the path)

The postgres binary's data/bss ends at 0x10f9000 and libxtc is mapped at
0x7ffff73xxxxx, so the hot addresses (~0x11c_-0x121_) are in the PROGRAM
BREAK / HEAP -- i.e. malloc'd pthread_mutex_t objects.  low pthread_mutex_lock
ENTRY counts + huge __lll_lock_wait_private = a mutex held for relatively long
and universally contended (few acquires, each waiter blocks a while).

### Conclusion + next step
The remaining ~2.3x plateau is a HEAP-allocated mutex on the per-command path
that every carrier serializes on -- not a PG named global, not the scheduler
SpinLock, not libxtc BSS.  Candidates: a mutex embedded in a SHARED heap runtime
object (a per-runtime/per-loop/registry object, or a libxtc heap object such as a
loop/slab/proc lock).  NEXT: name the object -- `perf probe` or a bpftrace
uretprobe on the malloc that returns these addresses, or attach the mutex address
back to its allocation via a uprobe on pthread_mutex_init recording arg0+ustack.
Then either shard it (per-carrier/per-loop) or make the hot path lock-free, and
A/B with mtpg_ab (target: kill the futex_wait, fill the 83% idle).

This is the single remaining major structural target for process parity.  All
three earlier serializers (ps_status mutex, 10ms busy-poll, shared-cond wake
storm) are fixed; this heap mutex is the last one standing between ~180k and
~416k.

## Definitive: the heap mutex is INSIDE libxtc (2026-07-14)

Code audit: PostgreSQL has NO pthread_mutex_init call sites and NO pthread_mutex_t
embedded in any malloc'd runtime/session/backend/pmchild struct -- every PG mutex
is a static PG_GLOBAL_RUNTIME ...= PTHREAD_MUTEX_INITIALIZER (which lives in bss
<= 0x10f9000, and those were all cold in the trace).  So the hot HEAP mutex
(~0x11c_-0x121_, in the program break) is allocated + pthread_mutex_init'd by
**libxtc** internally -- one of its per-operation locks (candidates: the per-loop
lock, the proc/park registry lock, or the slab/allocator lock that a fiber
touches on every park/unpark via xtc_proc_wait_fd).

So the last serializer is a libxtc-internal lock taken per fiber park/unpark, i.e.
per command boundary.  Every carrier hits it each command -> serialization ->
~180k plateau + 83% idle.

### Actions (next session)
1. Build libxtc WITH symbols (`./dist/configure CFLAGS='-g -fno-omit-frame-pointer'`)
   and re-run the mutex-address trace: the hot addr now resolves to a libxtc
   symbol / init site, naming the exact lock.
2. If it is a global libxtc lock on the park/unpark hot path, this is a libxtc
   design issue to raise with the libxtc team (detailed repro: 16 clients / 8
   fiber carriers, prepared SELECT, ~1.2M __lll_lock_wait_private/8s on a
   heap-allocated libxtc mutex; process-mode PG at 416k vs fiber-carrier PG at
   180k, machine 83% idle) -- OR a usage change on our side (e.g. per-loop
   affinity so backends on one loop do not cross-contend a shared registry lock,
   sharding fibers across the 32 loops instead of funneling park state through a
   shared structure).
3. Re-tune / re-measure with mtpg_ab once the libxtc lock is addressed.

### Session tally toward process parity
Fixed (measured): ps_status mutex (+19%); 10ms busy-poll + shared-cond wake storm
(eventfd, collapse gone + contention halved).  Localized + attributed: the final
~2.3x plateau = a libxtc-internal heap mutex on the per-park hot path.  This is
the deep-fusion boundary the north star anticipated: closing it needs libxtc
cooperation (symbols -> name -> shard/lock-free the park path), not more PG-side
changes.

## NAMED (code-level): xtc_proc_wake -> __resolve -> __lt_lock + cross-proc mbox_lock

Read libxtc v1.20.1 src/ptc/proc.c directly (local checkout).  The per-command
serial section is the cross-thread WAKE path:

  xtc_proc_wake(target)  proc.c:1386
    -> __resolve(target)  proc.c:1392
         -> pthread_mutex_lock(&__lt_lock)  (~1243) -- ONE global loop-table lock,
            scans LOOP_TABLE_MAX entries (Strategy-2 fallback for a wake issued
            from a thread that is not the target's own loop)
    -> p->mbox_lock  proc.c:1396 (per-proc, HEAP, pthread_mutex_init proc.c:965)
       -- matches the heap mutex the off-CPU/address trace found hot.

PostgreSQL wires SetLatch/async-read completion -> xtc_proc_wake (latch.c:482,
waiteventset.c:1383), so every command boundary that wakes a fiber parked on
ANOTHER carrier's loop hits the global __lt_lock.  N carriers -> serialize.  This
is bss (__lt_lock) + heap (mbox_lock) contention on the wake path -- consistent
with all measurements.

### Two tracks (next session)
A. Report to libxtc (done: /tmp/libxtc-proc-mutex-contention-report.md): shard/RCU
   the loop table so __resolve has no global lock on the wake fast path; lock-free
   "fire armed waker" so cross-thread wake skips the target mbox_lock.
B. Our side, testable NOW without libxtc changes: LOOP AFFINITY.  If we keep a
   session's fiber and its usual waker on the SAME xtc loop, __resolve takes the
   fast (Strategy-1) path and skips __lt_lock.  We currently spread backends
   across loops (pg_xtc_carrier.c "concurrent backends run on distinct loops");
   the wake almost always crosses loops.  Experiment: pin a session's fiber and
   the carrier that wakes it to one loop (or reduce loop count so the table scan
   is cheap / same-loop), measure with mtpg_ab + latency.  A/B target: eliminate
   the futex_wait, fill the 83% idle, approach process tps + p95/p99.

This fully closes the diagnosis arc.  The last ~2.3x is the libxtc cross-thread
wake lock; closing it is track A (upstream) and/or track B (loop affinity on our
side) -- both concrete and measurable, both preserving process mode.

## Track B premise FALSIFIED: it is NOT the cross-thread wake path (2026-07-14)

Before building loop affinity, verified the premise by counting the actual
libxtc calls under load (uprobe on the public symbols, 16c/8carriers, 10s):

    xtc_proc_wake   = 0-1 calls      (NOT per-command!)
    xtc_proc_wait_fd = 39 calls      (NOT per-command!)

So the per-command path does NOT call xtc_proc_wake and the backends do NOT
re-park via xtc_proc_wait_fd each command -- the carrier cycles resumable
backends in USERSPACE (PgCarrierLeaseRunnableProtocolBackend) without a
syscall-park per command.  Therefore __resolve/__lt_lock and the cross-thread
wake path are NOT the per-command serial section.  Loop affinity (Track B) would
not help -- do NOT build it.  (The earlier code-level attribution to
xtc_proc_wake was a plausible-but-unverified inference; this measurement corrects
it.  Lesson: always count the call before attributing.)

What IS true and still unexplained:
- ~367k __lll_lock_wait_private (pthread mutex slow-path) in 10s -- heavy mutex
  contention remains;
- the hot mutex is HEAP-allocated (addresses ~0x11c_-0x121_, in the program
  break), NOT any PG named global (all cold) and NOT libxtc BSS;
- but wake/park SYSCALLS are near-zero, so the hot mutex is taken on a path that
  is mostly fast (contended but rarely blocking to a syscall) EXCEPT under load
  it slow-paths ~367k/10s.
- the __lll_lock_wait_private caller is unrecoverable from the stack (libc lock
  internals, no FP chain), so naming it needs either libxtc built WITH full
  debuginfo, or a pthread_mutex_lock-entry uprobe capturing arg0 AND resolving
  arg0 -> symbol/allocation (partially done: arg0 in heap, not PG).

### Corrected next step
Do NOT build loop affinity.  To NAME the heap mutex definitively: build libxtc
with `-g3 -fno-omit-frame-pointer` + a debug PG, then either (a) perf/bpftrace
with the libxtc debuginfo so the lock-wait caller resolves, or (b) a
pthread_mutex_lock uprobe recording arg0 + a pthread_mutex_init uprobe recording
(addr->ustack) so the hot arg0 maps to its init site.  The libxtc team is
already reworking the proc-layer locking for the next release; the cleanest path
may be to re-measure on that release rather than reverse-engineer the current
heap mutex.  Either way: measure/name before fixing -- this session's value was
FALSIFYING the wake-path hypothesis so no wrong fix gets built.

## RESOLVED: it was glibc malloc-arena contention from arena_max=1 (2026-07-14, v1.21.0)

Both prior lock hypotheses (wake-path __lt_lock; t->lock via xtc_send) were
FALSIFIED by call-count measurement: under load xtc_proc_wake=0-1/10s,
xtc_send=0/10s, xtc_proc_wait_fd=39/10s, pthread_mutex_lock uprobe=0.  The real
cause:

- malloc/free = ~2.6M/sec under a pooled prepared-SELECT load (per-command
  StringInfo churn: postgres.c:4940 initStringInfo(&input_message) after
  MemoryContextReset(message_context), each command).
- The ~367k __lll_lock_wait_private/10s is glibc ARENA-LOCK contention (arena
  lll_lock, not pthread_mutex -- hence the pthread_mutex_lock uprobe read 0).
- PgRuntimeConfigureThreadedAllocator pinned pooled mode to M_ARENA_MAX=1 (a
  deliberate idle-footprint choice), so all 8 carrier threads serialized on ONE
  arena lock.  Process mode never hits this (private per-process arenas).

Fix (e0880ddb823): scale M_ARENA_MAX with carrier count (one arena per carrier).
Measured (EC2 m6id.8xlarge, 32 vCPU, prepared SELECT):
    arena_max=1        8c/16cl  199k tps
    per-carrier        8c/16cl  262k tps   (+32%)
    per-carrier       16c/16cl  406k tps   (process 416k -- ~98%)
Footprint preserved: 100 idle conns add only ~8MB RSS (M_TRIM_THRESHOLD/M_TOP_PAD
kept).  Correctness: pooled TAP 007 46/46.

### Where the gap stands now
Threaded is at ~98% of process throughput at matched carriers/clients, footprint
comparable, correctness intact.  Remaining small residual + p95/p99 parity
(measure with pgbench_pctl) and the libxtc proc-table RCU work (their PLAN 19.5c)
are further levers but NO LONGER the gating bottleneck.  Session tally of fixed
serializers: ps_status mutex (+19%), 10ms busy-poll/wake-storm (eventfd),
malloc-arena cap (+32-100%, the big one).

### Methodology note
Three hypotheses, three measurements: per-command-CPU (falsified), wake-path lock
(falsified), t->lock/xtc_send (falsified) -- then malloc call-count + arena A/B
NAILED it.  Count the call / A/B the change before attributing or requesting a
fix.  The libxtc team's t->lock root-cause was also an unverified inference; our
xtc_send=0 measurement saved them from building an RCU fix for the wrong lock.

## PARITY CLOSE-OUT: threaded ~= process on tps, p95/p99, AND footprint (2026-07-14)

With the arena-per-carrier fix (e0880ddb823), full measurement on EC2
(m6id.8xlarge, 32 vCPU; prepared SELECT; 16 carriers / 16 clients; --log ->
pgbench_pctl for percentiles; PSS for fair memory):

    metric      process      threaded     delta
    tps         394.5k       389.8k       -1.2%
    p50         0.039ms      0.040ms      +2.6%
    p95         0.043ms      0.044ms      +2.3%
    p99         0.048ms      0.050ms      +4.2%
    max         1.17ms       3.49ms       (rare tail spike, <1% of txns)
    footprint   PSS 415MB    PSS 423MB    +2%   (24 procs vs 1)

Throughput, p50/p95/p99, and fair (PSS) memory are all within a few percent of
the fork model.  This closes the stated goal to PARITY on the axes named
(TPS, p95/p99 latency, RAM footprint) for the cached read-only workload.

Caveats / follow-ups (not gating):
- max latency is worse (3.49 vs 1.17ms) -- a rare tail spike (<1% of txns, p99
  unaffected); likely a carrier scheduling/arena-growth hiccup.  Worth a p99.9
  look later.
- PSS parity at 16 sessions; threaded's per-process-overhead advantage widens at
  higher connection counts (1 process vs N) -- the classic thread-vs-fork memory
  win, not yet quantified at high N.
- Workload is cached prepared -S; write/mixed (TPC-B) and higher-concurrency
  sweeps are the remaining perf-characterization surface, but the dominant
  serial-section that caused the 2.3x plateau is fixed.

Session-18 arc complete: three serializers fixed (ps_status mutex, 10ms
busy-poll/wake-storm eventfd, glibc arena_max cap -- the big one), gap closed
from ~41% to ~99% of process.  Not a libxtc fix -- our own allocator tuning,
proven by A/B.

## Phase 17 measurement: contended-write carrier starvation CONFIRMED (2026-07-15)

EC2 m6id.8xlarge (32 vCPU), release+FP build, libxtc v1.22.0, HEAD ed5c6e7eb07.
shared_buffers=4GB (dataset in cache -> isolates LOCK contention, not I/O),
fsync/synchronous_commit/full_page_writes off.  Pooled carriers pinned to 8
(pooled_protocol_carriers=8) DELIBERATELY < the higher client counts, so the
carrier-blocking sem_wait shows.  30s/run, -j = -c.

Process vs threaded-pooled, TPS by client count:

  TPC-B (tpcb-like: write-heavy, branch/teller-row + WAL + buffer LWLock contention)
    clients   process    threaded   threaded/process
       8       24763       24755      100%  (parity: clients == carriers)
      16       38457       24787       64%
      32       73985       24631       33%
      64       70459       24591       35%

  hot-row UPDATE (every client UPDATEs the SAME one row: extreme tuple-lock/WAL contention)
    clients   process    threaded
       8       43980       42247
      16       35147       46826   (threaded 1.33x process)
      32       25745       46156   (threaded 1.79x)
      64       16366       45790   (threaded 2.80x)

### Verdict
CONFIRMED, decisively.  Threaded TPC-B FLATLINES at ~24.7k tps across 8/16/32/64
clients while process scales to ~74k.  With 8 carriers, threaded cannot do more
than ~8 clients' worth of scalable work: a carrier that enters
ProcWaitOnSemaphore -> PGSemaphoreLock -> raw sem_wait BLOCKS THE WHOLE CARRIER OS
THREAD, so it cannot serve any other session while parked on an LWLock/WAL wait.
Eight blocked carriers == eight-wide throughput ceiling, exactly the Phase 17
audit prediction (invisible on the cached read-only workload of Phase 18, which
never takes those waits).  Confirmed the cap is the pooled-carrier count (8), not
the executor loop count (32): pooled_protocol_carriers gates concurrent session
fibers in launch_backend.c:1153.

### Surprise (north-star relevant, keep it)
On the PATHOLOGICALLY contended hot-row UPDATE, threaded BEATS process by up to
2.8x at 64 clients (45.8k vs 16.4k).  When 64 processes all fight one tuple lock,
fork mode thrashes on scheduling/context-switch; 8 carriers serialize the same
inherent contention with far less OS overhead.  This is a genuine "beat the fork
model" datapoint -- the Phase 17 fix must RECOVER TPC-B scaling WITHOUT losing
this hot-row win (i.e. don't just raise carrier count; make the wait fiber-aware
so a parked session frees its carrier).

### Unblocks
Phase 17 implementation is now measurement-justified: make ProcWaitOnSemaphore
fiber-aware (park the fiber via xtc_sem/xtc_notify, wake in ProcWakeSemaphore),
keeping raw sem_wait for the process/non-fiber path.  Re-run THIS matrix as the
A/B gate: threaded TPC-B should scale toward process; hot-row should stay >= now.
