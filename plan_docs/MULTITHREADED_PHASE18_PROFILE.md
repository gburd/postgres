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
