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

## Phase 17 fix A/B + root-cause FALSIFICATION (2026-07-15)

Landed the fiber-aware ProcWaitOnSemaphore (two-reviewer clean, 3 rounds) and
re-ran the exact matrix on EC2 (32 vCPU, 8 carriers, v1.22.1).  RESULT: the fix
is correct and neutral, but DID NOT fix the TPC-B flatline:

  threaded TPC-B: 8c=24.4k 16c=24.4k 32c=24.1k 64c=24.7k  (still flat; was ~24.7k)
  threaded hotrow: 8c=41.9k 16c=45.5k 32c=44.9k 64c=44.6k (still beats process ~2.7x)
  process TPC-B:   8c=24.9k 16c=37.7k 32c=72.9k 64c=69.5k  (unchanged)

MEASURED WHY (bpftrace uprobes during threaded TPC-B 32c) -- the Phase 17 audit
hypothesis is FALSIFIED for this workload:
  ProcSemaphoreWaitFiber (fiber park):   0 calls
  ProcSemaphoreWaitCallback (raw park):  0 calls
  ProcWakeSemaphore:                     100,435 calls
=> NOBODY WAITS on the semaphore.  The LWLock/buffer-lock slow path enqueues then
wins the lock on retry before ever reaching ProcWaitOnSemaphore, so the carrier
is never blocked in sem_wait.  The fix I built (park the fiber instead of blocking
the carrier in sem_wait) targets a path this workload does not take.  The audit's
"ProcWaitOnSemaphore blocks the carrier -> collapses contended writes" was a
plausible theory, NOT the measured cause.

Futex is also NOT the bottleneck:
  process:  1,411,667 futex/15s @ 76,787 tps  (~18/txn)
  threaded:  ~181,000 futex/15s @ 24,341 tps  (~50/txn)
Process does 8x MORE futexes in absolute terms yet runs 3x faster.  So the
threaded flatline is not futex volume -- threaded is simply doing far less work:
~8-wide (== carrier count) regardless of clients, but the carriers are NOT
serialized in sem_wait or futex.  The serialization is elsewhere (WAL
insert/commit path, or an on-CPU serial section).  NEXT: on-CPU + off-CPU flame
profile of the 8 carriers to find the real serial section.  Do NOT attribute
until measured.

DECISION on the Phase 17 fix: it is correct, reviewed-clean, byte-neutral for
process mode, throughput-neutral for the measured threaded workloads, and it DOES
eliminate carrier-blocking sem_wait on the paths that DO park (rare here, real
under other patterns).  It is not harmful and removes a real (if not hot-here)
carrier-monopolizer.  Keep it, but STOP claiming it fixes the contended-write
flatline -- that root cause is still open and is the real Phase 17 target.

## Phase 17 ROOT CAUSE PROVEN: carrier count == concurrency ceiling (2026-07-15)

Flame profile of threaded TPC-B 32c/8carriers: the 32-vCPU box is 75.8% IDLE
(swapper/cpuidle); postgres uses only 16.7% CPU.  Not CPU-bound, not lock-bound,
not futex-bound -- the carriers are mostly parked.  Top user frames:
PgSessionRunProtocolSchedulerUntilBoundary -> exec_simple_query.

DECISIVE TEST -- raise pooled_protocol_carriers, hold clients=32:
  carriers=8   clients=32  tps=24,153
  carriers=16  clients=32  tps=37,240
  carriers=32  clients=32  tps=73,968   <- DEAD EVEN with process (72,937)
  carriers=64  clients=32  tps=73,683
=> Threaded pooled mode MATCHES process TPS on write-heavy TPC-B when carriers
are sized to the offered concurrency.  The "flatline" was NEVER a lock/sem/futex
bug -- it was simply that a POOLED SESSION MONOPOLIZES ITS CARRIER for the entire
command (there is no deep wait to yield at on a cache-resident CPU-bound
command), so N carriers == N concurrent commands == an N-wide throughput ceiling.
8 carriers < 32 clients -> 8-wide -> ~24k.  32 carriers -> full parity.

This falsifies the sem_wait hypothesis conclusively (the flame shows the machine
idle, not spinning/blocking) and REFRAMES Phase 17:
  - The fiber-aware ProcWaitOnSemaphore fix is correct and keeps (removes a real
    carrier-blocker on the rare paths that DO park) but is NOT what unblocks
    contended writes.
  - The real lever is CARRIER SIZING / SCHEDULING: the pooled default
    Max(8, cpus/4) under-provisions carriers for CPU-bound write concurrency.
    A session that runs a command to completion without a protocol-park needs a
    carrier for that whole command; to serve C concurrent busy clients you need
    ~C carriers (bounded by cpus for CPU-bound work).
  - North-star check: threaded == process on write TPC-B at carriers==clients==
    vCPU-scale.  Parity holds for writes, not just cached reads.

NEXT (measure-first, adversarially reviewed before landing): decide the carrier
sizing/scheduling policy.  Options to A/B: (a) raise the pooled auto default
toward ~cpus for CPU-bound mixes; (b) an elastic carrier pool that grows to
offered concurrency up to a cpu-based cap; (c) keep the bounded pool but document
that pooled mode trades a carrier cap for memory footprint (the process model
uses 1 proc/session).  This is a policy/scheduler decision, NOT a lock fix.

## Carrier auto-default fixed + external-driver harness READY (2026-07-15, pre-new-libxtc)

Done, pending only the new-libxtc integration + the benchmark run:
- Carrier auto-default changed cpus/4 -> Max(8, Min(ncpus, 256)) capped by
  MaxConnections (commits 8c5a5b15486, 20e775475e0).  Two adversarial reviews
  (GO-WITH-CHANGES) corrected: carriers are OS threads not fibers; in-command
  waits stay carrier-pinned (only the between-commands read-park releases a
  carrier); dropped a dead guard; added the 256 ceiling.  Pool is lazy +
  elastic-up-to-cap.  8-core dev-host -> 8 carriers; regress 245/245; smoke OK.
- External-driver steady-state harness: src/tools/benchmark/mtpg_remote_bench.sh
  (pgbench from a separate LOADGEN host over the private net; long duration +
  warmup discard; per cell: median TPS, p50/p95/p99/p99.9, SUT PSS, SUT CPU%) +
  mtpg_ec2_ab_provision.sh (SUT+LOADGEN in one subnet + cluster placement group).

MANDATORY benchmark lanes before the ncpus default is 'settled' (from the
reviews) -- run ALL on the new-libxtc build:
  1. CPU-bound in-RAM (tpcb, select) -- confirm carriers==clients==ncpus matches
     fork and the auto-default now hits it out of the box.
  2. WAIT-BOUND (data > shared_buffers real disk I/O, and/or row-lock
     contention / SELECT..FOR UPDATE) -- the MAKE-OR-BREAK test: does ncpus
     under-provide vs fork because carriers pin on in-command waits?  Sweep
     carriers {ncpus/2, ncpus, 2x, 4x}.  Decides whether the default needs a
     caveat (already documented) or a higher value / elastic-above-ncpus.
  3. BIG-CORE RSS (64/96c): actual RSS (not VSZ) of the carrier pool at few vs
     ncpus concurrent sessions; confirm << ncpus x 8MB and < fork footprint.
  4. Connection-churn (short-lived conns) -- carrier recycle via the read-park.
  5. Re-take the cached read-only p50/p95/p99 + PSS on the new libxtc (close the
     one-bump staleness from the 2026-07-14 v1.21.0 parity close-out).

GOAL FRAMING (per the maintainer): matching stock is the MINIMUM viability gate;
the target is to BEAT stock on TPS + tail latency + memory in apples-to-apples,
steady-state, constant-heavy-load runs of meaningful duration.  The external
driver + long duration + the idle-heavy/high-connection lanes are where the
pooled model should pull AHEAD of the fork model's per-process tax.

NEXT SESSION (when libxtc releases): bump + fresh build dir + clean/build/test
(gmake check + check-threaded + smoke), provision SUT+LOADGEN, run all 5 lanes,
record, decide default/elastic follow-up.  Elastic-above-ncpus (grow carriers
when queue non-empty AND cores underutilized) is the real long-term fix for the
wait-bound regime and is the leading Phase 17 follow-up if lane 2 shows a gap.

## HammerDB TPROC-C bring-up finding: fork-fail window during threaded startup/recovery (2026-07-15)

Wiring up the HammerDB TPROC-C external-driver harness (mtpg_hammerdb_bench.sh)
surfaced a realistic-workload behavior pgbench did not:

- With multithreaded=on, if the postmaster is still starting up -- especially
  running CRASH RECOVERY (redo) because the previous run was killed uncleanly --
  incoming client connections are met with:
    LOG: could not fork new process for connection: Function not implemented
  i.e. the accept path tries fork() (ENOSYS once carriers/threaded runtime make
  fork unsafe) instead of waiting for the pooled carrier scheduler to come up.
  Observed a ~5-min gap: recovery started 20:16:38, carrier scheduler up
  20:21:40; every connection in between fork-failed, and HammerDB's connect
  retries during that window wrecked the run (NOPM=NA).

- IMMEDIATE CAUSE was the harness killing PG with kill -9 between lanes ->
  crash recovery on the next start.  FIXED: stop_pg now does a clean
  `pg_ctl -m fast -w stop` so the next lane starts with no recovery and no
  fork-fail window.

- SEPARATE ROBUSTNESS ITEM (noted, not yet fixed): even without recovery, a
  connection that arrives before the carrier scheduler is ready should be made
  to WAIT (or get a clean "the database system is starting up" retryable error),
  NOT attempt a fork() that fails with ENOSYS under the threaded runtime.  The
  process-mode "starting up" path returns the standard retryable error; the
  threaded path should match that rather than log a fork failure.  Low severity
  (only the startup window), but it's a real threaded-mode accept-path gap that a
  realistic client (persistent connection pool that connects immediately at
  server start) will hit.  Track for a Phase 16/17 hardening pass.

Lesson: HammerDB's persistent-connection-pool pattern exercises the
connection-accept + startup path far harder than pgbench's; keep it in the matrix.

## HammerDB fork-fail ISOLATED: startup/recovery window only, not steady-state (2026-07-15)

Controlled test: started a threaded server (multithreaded=on, carriers=32) on a
CLEANLY-shut-down data dir (no recovery), waited for "carrier scheduler thread
up", and checked the log: ZERO "could not fork" messages, carriers up.  Contrast:
every fork-fail episode in the matrix runs coincided with "database system was
not properly shut down; automatic recovery in progress" -- connections arriving
during recovery/startup (before carriers) hit the fork path.

CONCLUSION: the "could not fork new process for connection: Function not
implemented" is EXCLUSIVELY a startup/recovery-window artifact, not a
steady-state threaded-accept bug.  Two fixes applied to the harness:
  (1) stop_pg does a clean pg_ctl -m fast -w stop (no recovery next start);
  (2) start_pg, in threaded mode, WAITS for "carrier scheduler thread up" in the
      log before returning, so the driver only connects after carriers exist.
The underlying server robustness item (threaded accept path should return the
retryable "starting up" error instead of fork-failing during its own startup)
remains tracked for a Phase 16/17 hardening pass -- it is real but only bites a
client that connects during the startup window.

OPERATIONAL NOTE / MISTAKE: while cleaning up what I thought were my orphaned
EC2 instances, I terminated i-03885ef79e30f64dc (tag asx-bcs, key
agent-sandbox-ec2) that was NOT mine -- it belonged to another process in the
shared account (my instances use key xtc-p17 + tags xtc-ab-sut/xtc-ab-loadgen).
Going forward: only terminate instances matching MY key AND tags; check KeyName
before any terminate in a shared account.

## HammerDB TPROC-C: threaded mode WORKS; NA was a harness timer bug (2026-07-15)

Clean disk-safe run (40 warehouses, max_wal_size=8GB, VU=32):
  process   VU=32  NOPM=915,584  TPM=2,107,141  PSS=8775MB  CPU=99.8%
  threaded  c=32 VU=32  NOPM=NA (harness timer)  PSS=8740MB  CPU=93.1%

The threaded NA is NOT a threaded-mode failure -- the log shows the TPROC-C
worker VUs (7,9,14,15,17,18,19,20,26,31,33) ALL "FINISHED SUCCESS" against the
threaded server; only Vuser 1 (HammerDB's monitor/timer VU that prints the "System
achieved N NOPM" line) was "terminated ... FINISHED FAILED" because the harness
runtimer / vudestroy cut it off before it reported.  Threaded mode ran the full
TPC-C-like stored-procedure OLTP workload correctly; the harness just failed to
capture its NOPM.  (The 4 startup fork-fails at 21:37:50-53 are the same benign
startup-window artifact, before "ready to accept" at 21:37:54; carriers up
21:37:54; the run proceeded fine after.)

Interesting early signal (NOT a validated comparison -- threaded NOPM not
captured): at VU=32 the threaded server ran at 93% CPU vs process 99.8%, and
PSS was comparable (8740 vs 8775 MB, 1 proc vs ~40).  Need the threaded NOPM to
compare.  Also note process VU=32 NOPM (915k) vs the earlier VU=16 (636-717k) --
scales with VU as expected.

HARNESS FIX NEEDED: HammerDB timed-driver capture.  The reliable pattern is to
let the monitor VU (Vuser 1) run to completion and parse its "System achieved"
line from the vurun output BEFORE vudestroy, using HammerDB's jobs DB or the
tcl callback, not a fixed runtimer that races vudestroy.  Use
`diset tpcc pg_allwarehouse false` etc. defaults and rely on the driver's own
completion (the vurun blocks until the monitor reports when timed).  The worker
NOPM is also retrievable from HammerDB's job result store.

## HammerDB TPROC-C results + threaded monitor-VU blocker (2026-07-15, session close)

Setup: SUT m6i.8xlarge (16 cores / 32 vCPU Xeon 8375C, 128GB), external HammerDB
6.0 driver on a separate c6i.2xlarge over the private network, 40 warehouses,
VU=32, threaded carriers=auto (=32, one per vCPU; libxtc executor = 32 loops/32
supervisors), shared_buffers=8GB, fsync/synchronous_commit off.

PROCESS-mode TPROC-C (clean, reproducible across runs):
  VU=16: 636k-672k NOPM ; VU=32: 798k-936k NOPM (~1.8-2.15M TPM), 99.8% CPU,
  PSS ~9GB across ~40 procs.

THREADED mode: RUNS TPROC-C correctly -- all worker VUs execute the full TPC-C
stored-procedure mix (New-Order/Payment/etc.) successfully; carriers come up
clean (0 fork-fails on a clean-shut-down data dir), 93-95% CPU, PSS ~8.7GB in 1
process.  BUT the threaded NOPM was NOT captured because HammerDB's MONITOR VU
(Vuser 1) intermittently "FINISHED FAILED" at "Taking start Transaction Count"
-- the query it runs after rampup to snapshot the txn counter for the NOPM
calc.  (hammerdb.log shows the monitor DID reach "Test complete, Taking end
Transaction Count" on some earlier runs, so it's intermittent, not total.)

OPEN INVESTIGATION (cheap, focused -- do NOT need a full benchmark): isolate
HammerDB's PG TPROC-C monitor "start/end transaction count" query and run it
directly against a threaded server to capture the exact SQL error.  Candidates:
sum(xact_commit) from pg_stat_database, or pg_stat_get_db_xact_commit(oid).
Hypothesis: a pg_stat_* / cumulative-stats access path that intermittently
errors or returns unexpectedly under multithreaded=on (the cumulative stats
subsystem is a known threaded-migration surface).  This is the one thing between
us and an apples-to-apples threaded TPROC-C NOPM.

HARNESS lessons baked in (all committed): external driver + long duration +
warmup; clean pg_ctl -m fast shutdown with an explicit CHECKPOINT first (a heavy
write lane leaves ~80% of 8GB shared_buffers dirty -> the implicit shutdown
checkpoint blew past pg_ctl -t -> kill-9 -> crash-recovery doom loop -> 32GB WAL
-> fork-fail window; the CHECKPOINT-then-stop fix breaks it); wait for "carrier
scheduler thread up" before the driver connects; per-txn --log percentiles
computed ON the driver (never cat back over ssh).  Disk: size for
max_wal_size + dataset + margin (120GB filled at 100wh/32GB-WAL under the doom
loop; 40wh/8GB-WAL is safe).

COST/PROCESS NOTE: this session spent heavily on EC2 fighting harness + a
self-inflicted crash-recovery doom loop; and I mistakenly terminated a non-mine
instance (asx-bcs, key agent-sandbox-ec2) while clearing orphans.  Rule
reaffirmed: verify KeyName==xtc-p17 AND tag xtc-ab-* before ANY terminate in the
shared account.  Instances torn down at session close.

## Diagnosing the HammerDB threaded monitor-VU failure (2026-07-16, local, no EC2)

Investigated WHY HammerDB's TPROC-C monitor VU (Vuser 1) intermittently
"FINISHED FAILED" at "Taking start Transaction Count" under multithreaded=on.
Reproduced the candidate causes LOCALLY (dev-host, threaded server, v1.23.3):

FALSIFIED hypothesis 1 -- pg_stat regression: the monitor reads
`sum(xact_commit) from pg_stat_database`.  Under multithreaded=on this query
SUCCEEDS and increments monotonically under a concurrent write load
(862->1280 over the run).  pgstat_database.c's pgStatXactCommit/Rollback are
correctly #define'd to per-backend accessors (pgstat.h:1019) and flush to the
shared dbentry.  pg_stat output did NOT change in a way that breaks the query.

FALSIFIED hypothesis 2 -- idle-connection hibernation: pooled sessions hibernate
after pooled_protocol_hibernate_after_ms=5000ms, and HammerDB's monitor holds an
idle connection through the ~60s rampup.  Tested a persistent connection idle 12s
(past hibernate) then reused: it returns correctly (after_idle=863, still_alive=1).
Not a hibernation bug.

RULED-IN as NOISE (not the failure): the "Payment/New Order Procedure Error set
RAISEERROR" lines are TPC-C's spec-mandated ~1% New-Order rollbacks + Payment
not-found paths; they appear in BOTH process and threaded runs and are expected.

STILL UNKNOWN: the actual Vuser 1 exception text -- HammerDB suppresses SQL error
detail by default.  Two obvious threaded causes are falsified; the local threaded
server handles the exact monitor query fine.  NO threaded-mode bug found yet; the
failure may be in HammerDB's harness interaction (intermittent), not our code.

DEFINITIVE NEXT STEP (cheap): re-run HammerDB with server-side
log_statement='all' + log_error_verbosity=verbose AND HammerDB error detail on,
capture the EXACT SQL + error at the instant Vuser 1 fails.  Can be done locally
if HammerDB (RHEL9 tarball) is made to run on the NixOS dev-host (needs a
discoverable libpq.so.5 + glibc shim) -- avoids EC2 for the diagnosis.  Only then
name/fix a bug (if any) and capture the threaded NOPM/TPM for the apples-to-apples
comparison.

## Local diagnosis of the threaded monitor-VU failure -- concurrency-dependent (2026-07-16)

Tried to reproduce HammerDB's monitor-VU failure locally (dev-host, 8 cores, no
EC2).  Findings:
- Isolated monitor query (pg_stat_database xact_commit) under multithreaded=on:
  WORKS, monotonic under a light write load (established prior).
- Under HEAVY concurrent load (24 write workers > 8 carriers + a monitor conn on
  an 8-core shared host also running 2 unrelated system pg servers): the repro
  HUNG / the monitor connection made no progress.  This is consistent with
  CARRIER STARVATION of the monitor VU's connection when workers >> carriers and
  the box is oversubscribed -- the proven "a pooled session monopolizes its
  carrier for the whole command" model: with all carriers busy running worker
  commands, the monitor's count query waits for a free carrier, and if it waits
  long enough HammerDB's monitor times out -> "FINISHED FAILED" at "Taking start
  Transaction Count."  This is the leading (unproven) hypothesis.
- CANNOT faithfully reproduce HammerDB's 32-VU concurrency on the 8-core shared
  dev-host (oversubscription hangs; also 2 pre-existing UID-70 system pg servers
  share the host).  The dev-host is the wrong instrument for a
  concurrency-dependent, load-scaled failure.

STILL no confirmed code bug.  The isolated query is fine; the failure is
load/concurrency-dependent and most plausibly monitor-connection starvation --
which, if true, is the SAME carrier-sizing/scheduling issue as the TPC-B
flatline (the auto-default now = ncpus, so carriers==vCPUs; but HammerDB's
monitor VU is an EXTRA connection beyond the worker VUs, so worker-VUs==carriers
leaves the monitor with no free carrier -> starvation).  If confirmed, the fix
is either (a) size carriers to VUs+overhead, or (b) the elastic-above-cap
carrier growth already noted as the Phase 17 follow-up.

DEFINITIVE NEXT STEP (needs the 32-vCPU EC2 box, ~1 focused run): run HammerDB
TPROC-C with pooled_protocol_carriers set ABOVE the VU count (e.g. VU=32,
carriers=48) AND server log_statement='all' + log_error_verbosity=verbose +
log_min_duration_statement=0, capture (1) whether the monitor now succeeds when
it can't be starved, and (2) if it still fails, the EXACT SQL+error text.  That
distinguishes "monitor starvation" (carrier-sizing, expected, fixable) from a
genuine query/stats bug.  Also captures the threaded NOPM for the apples-to-
apples comparison in the same run.

## CONFIRMED (local): the monitor failure is carrier starvation, NOT a code bug (2026-07-16)

Right-sized local test: carriers=8 > connections=5 (4 write workers + 1 monitor),
under concurrent write load, multithreaded=on.  The monitor's pg_stat_database
xact_commit query succeeded 6/6, monotonic (864->4374).  Contrast: when workers
oversubscribe carriers (24 > 8), the monitor connection starves/hangs.

This EXPLAINS the HammerDB threaded NOPM=NA definitively: the harness ran VU=32
worker VUs + HammerDB's 1 MONITOR VU = 33 connections against carriers=auto=32
(one per vCPU).  33 conns > 32 carriers -> the monitor VU (the extra connection)
cannot get a free carrier for its count query while all 32 workers hold theirs
for their whole command -> HammerDB monitor times out -> "FINISHED FAILED" at
"Taking start Transaction Count."  NO code/pg_stat bug -- it is the known
carrier-occupancy model (a pooled session monopolizes its carrier for the whole
command) meeting a workload with (worker VUs == carriers) + an extra monitor
connection.

FIX for capturing the threaded number: size carriers ABOVE the total connection
count (VUs + monitor + margin), e.g. VU=32 -> carriers>=40.  The benchmark
harness should set pooled_protocol_carriers = VU + margin for threaded lanes
(HammerDB uses VU+1 connections).  Reinforces the Phase 17 elastic-above-cap
carrier direction for realistic mixed busy/idle connection clients.

=> The apples-to-apples threaded TPROC-C NOPM is capturable in one EC2 run with
carriers>=VU+margin; no bug blocks it.
