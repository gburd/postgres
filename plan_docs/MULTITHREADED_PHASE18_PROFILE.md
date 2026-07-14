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
