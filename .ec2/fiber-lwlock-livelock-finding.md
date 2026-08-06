# The real blocker is fiber LWLock contention (BufferMapping/WALInsert), not connect (2026-08-06)

## The c-sweep pinned it (m8idn.metal-96xl, A + reroute build)
  c    pgbench_tps   connfail   note
  1    21,969        0
  8    152,086       0
  32   324,576       0
  64   342,515       0          (in the sweep) -- flows
  128  NA            21         collapses
  256  NA            1
  384  NA            1
Flows cleanly to c=64, collapses at c>=128.

## But c=64 is INTERMITTENT -- and the stall is LWLock contention, not connect
A manual c=64 -S re-run HUNG (pgbench connected, connfail=0, but ~0 commits).
pg_stat_activity of the hung run: backends ACTIVE (not idle) waiting on
  BufferMapping (52), BufferShared (9), WALInsert (3)  -- buffer-manager + WAL
LWLocks -- with commits ~0/3s.  So it is NOT a client-read wake miss (backends
are active, not idle in ClientRead) and NOT primarily the connect burst.  It is a
FIBER LWLOCK CONTENTION LIVELOCK: fibers pile on BufferMapping/WALInsert and make
no progress.  Intermittent (the sweep's c=64 flowed at 342k; the re-run
livelocked) -- a race.

## Likely root cause (the real one, deepest layer)
A fiber that acquires an LWLock and then PARKS (yields its carrier at a wait
boundary) while holding it, combined with the other fibers piling up on that
LWLock, can livelock if the holder is not promptly rescheduled to release --
especially the high-traffic BufferMapping partition and the WALInsert locks under
c>=64 concurrency.  This is the fiber<->LWLock interaction, the heart of "use
libxtc to the fullest": LWLock acquire/release must compose correctly with fiber
park/resume so a lock holder always makes progress.  The connect-burst fixes
(A + reroute) were real and necessary (they removed the spawn-drop layer -- the
server log now shows NO "could not fork"/"hand spawn" under the burst), but the
throughput ceiling is this LWLock livelock, which appears at moderate concurrency
and is intermittent.

## What is proven vs not
PROVEN: fork services huge tps (c=64 -S = 1,253,761 tps; fork ctx-switch ~76M
over a 20s run ~= 3.8M/s).  The fiber runtime DID hit 342k tps at c=64 once, and
low-c (1-32) flows cleanly.
NOT PROVEN / BLOCKED: a STABLE fiber tps at c>=64, because the BufferMapping/
WALInsert LWLock livelock intermittently stalls it.  The "fibers beat fork"
thesis cannot be measured until this LWLock-fiber livelock is fixed -- and it is
also the thing that, once fixed, determines whether fibers actually win (LWLock
contention was ALREADY the ab4 beat-fork lever finding; this is the same lever,
now seen as an outright livelock under fiber park/resume).

## Next (focused, its own effort)
Instrument the fiber LWLock path under c=64-128: identify whether a fiber parks
while holding BufferMapping/WALInsert (it should NOT -- affine-section audit said
no lock is held across a park; verify that holds under real contention), or
whether the LWLock WAIT-and-wake among fibers has a lost-wake/ordering bug that
livelocks.  This is the deepest and most important remaining item -- it is the
actual "fibers done right beat fork" gate.  The connect fixes (A 4af3e0f0084 +
reroute 3432ceb8fb8) stay LOCAL pending this + the two-review gate (landing them
alone does not unblock throughput).

## Blocker chain (final, updated)
unpooled-ENOSYS -> HammerDB-monitor -> io_method=xtc-futex -> pooled-carrier-
starve -> accept-serialization (FIXED) -> socket leak (FIXED) -> eventfd-leak
(not real) -> SSLRequest latency (FIXED, A) -> spawn-send drop (FIXED, reroute)
-> **fiber LWLock livelock on BufferMapping/WALInsert (THE real blocker, next)**.

All MY EC2 torn down + verified (xtc-numa-bench us-east-2 = another owner's).
