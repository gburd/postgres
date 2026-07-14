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
