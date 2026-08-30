# libxtc v1.40.2 follow-up: the OFFLOAD `_Atomic done` fix is confirmed, but the 64-concurrent-committer collapse REMAINS on BOTH the native io_uring AND offload paths

Date: 2026-08-30
libxtc: v1.40.2 (commit dec8428 / tag rev ed60a396). Fix confirmed compiled in
(blocking.c: `_Atomic int done` at :49, re-park `while(!atomic_load_explicit(&w.done,
acquire))` at :297).
Consumer: PostgreSQL "xtc" branch, sessions-as-fibers (pooled_protocol_carriers=0).
Re: your v1.40.2 reply, which asked, if it still collapses, for (a) native-vs-offload and
(b) a TSan run. Answering (a) here; (b) is a bigger build we're queuing.

--------------------------------------------------------------------------------
## What v1.40.2 DID fix (confirmed)
The single-connection init strand is gone on both paths (as of v1.40.1 for native, and
your v1.40.2 `_Atomic done` for the offload close-vs-write race). The single-fiber cases
we reported are resolved.

## What still collapses: 64 concurrent committers, on BOTH paths
Workload: pgbench -c 64 -j 16 TPC-B write, fsync=on, synchronous_commit=on, sessions-as-
fibers on a 37-loop pool, migratable fibers + eager rebalance, 32-vCPU c6id NVMe, PG
debugoptimized on libxtc v1.40.2.

Result (each ~1000 tps for ~2s, then 0.0 tps for the rest of the run, every time):
  - NATIVE io_uring FSYNC path (default):        COLLAPSED, 1/15 nonzero progress samples.
  - XTC_AIO_FORCE_OFFLOAD=1 (the path you fixed): COLLAPSED, 0/0 nonzero samples.
  - c=192 and c=256: same collapse, NO PANIC.
So the concurrency-gated resume wedge is NOT specific to the offload completion pipe your
`_Atomic done` fix addressed -- it reproduces on the native io_uring FSYNC completion path
too, and still on offload under 64-way concurrency (the done-flag fix closed the single-
fiber strand but not the many-concurrent-committer collapse).

## gdb at the native collapse (the signature you asked about)
- Whole backtrace: BackendMain=0, exec_simple_query=0, XLogWrite=0, xtc_aio=0,
  xtc_blocking=0, blk_worker=0  -> ALL 64 client fibers are libxtc-parked (invisible to
  gdb); none are running.
- iou-wrk = 25  -> we are on the NATIVE io_uring path (io_method left default; the ring
  accepts IORING_OP_FSYNC).
- 32 of 37 carrier loops idle in xtc_io_poll / __xtc_loop_step_once / __xtc_exec_worker.
- Server-side commits frozen (delta ~2 commits / 4 s).
- In prior snapshots the reached-commit fibers were parked in
    exec_simple_query("END;") -> CommitTransaction -> XLogFlush ->
    LWLockAcquireOrWait(WALWriteLock) -> ProcSemaphoreWaitFiber -> xtc_pg_wait_fd
  and WALWriteLock read HELD EXCLUSIVE (state=0x80042000) with the holder off-thread --
  i.e. a fiber that acquired WALWriteLock and parked (mid-flush) is not resumed; everyone
  behind it stalls.

So the end state is the SAME stranded-WALWriteLock-holder shape as before, now on the
native path, surviving v1.40.2, and only under concurrent commit (single-fiber init is
0/8).

## Our reading
Your v1.40.2 reply said the native io_uring FSYNC path "was not subject to this specific
[offload close-vs-write] race" -- consistent with our data: native collapses too, so this
is a SEPARATE resume gap, not the offload pipe race. Candidates (your call which to chase;
you noted TSan found the last one fast):
  1. A native io_uring FDATASYNC one-shot CQE completion for a MIGRATABLE fiber that has
     been (or is being) work-stolen across loops under high churn -- reaped by a loop that
     does not mark THAT task runnable (a residual of the reap/dispatch/steal interaction,
     even though the three poll sites now dispatch), or a completion arriving during the
     steal/rebalance transition window.
  2. A cross-loop resume-nudge (xtc_proc_wake / task waker) that targets the wrong loop
     when the fiber's owning loop changed due to a steal between park and completion.
  3. Something in how many simultaneous parked-then-completing fibers interact with the
     idle poll's single reap buffer (evs[8]) -- >8 completions in one idle poll?

## Reproduction
1. PG "xtc" branch on libxtc v1.40.2, multithreaded=on, pooled_protocol_carriers=0,
   fsync=on, synchronous_commit=on, PGDATA on NVMe/xfs, shared_buffers ~40% RAM, 32 vCPU.
2. pgbench -i -s 100 (succeeds); pgbench -c 64 -j 16 -T 45 -P 5.
3. Progress: ~1000 tps for ~2 s, then 0.0 tps forever. gdb: WALWriteLock held by an
   off-thread fiber, all loops idle in xtc_io_poll, zero client backends running.
   Deterministic. Same at c=192, c=256.
We have bt_w64d (native) captured; can share it + a scripted repro.

## Next from our side (the (b) you asked for)
We will attempt a ThreadSanitizer build of PG + libxtc under this workload -- you found the
offload race with TSan in minutes; a native-path TSan run may localize this one similarly.
That is a larger build for us (PG under -fsanitize=thread + a TSan libxtc); we're queuing
it. If you also have (or can add) the tools/gdb/xtc-gdb.py parked-proc enumerator you
offered, that would let us name the exact loop owning the stranded holder's park_fd and
confirm the steal-window hypothesis directly.

## Not the issue
Not the stackless/bare-pthread path. Not a PG post-park action (PG parks per your API,
does nothing after). The v1.40.1 idle-poll fix and v1.40.2 offload `_Atomic done` fix are
both in and both effective for the single-fiber cases; this is the remaining
concurrent-commit case on both completion paths.
