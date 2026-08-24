# The "fiber LWLock livelock" was a MISDIAGNOSIS — corrected (2026-08-24, rebased tree, c7i.metal-48xl/192-core, chiuso)

## What the earlier finding (2026-08-06, pre-rebase) claimed
"Fiber LWLock contention livelock on BufferMapping/WALInsert, intermittent at
c>=64; -S collapses at c>=128."  It named two hypotheses (holder-parks-while-held,
or LWLock wake/ordering bug).

## What the rebased tree actually shows (Fix A + reroute now in-tree)
NO LIVELOCK.  Stable, 0 failed transactions, flat tps.  The old collapse is gone.
The finding conflated THREE unrelated things:

1. **-S (read-only) connect collapse at c>=128 = the connect-drop bug, now FIXED.**
   -S c-sweep on the rebased tree flows to c=256 with 0 failures:
     carriers=32:  ~757k tps flat c=32..192
   The collapse the finding saw was the spawn-drop / accept-serialization chain
   (fixed by accept-drain + fd-leak + Fix A + reroute), not a lock livelock.

2. **-N (write) low throughput = EBS fsync ceiling, NOT fibers.**
   The box's gp3 root volume fdatasyncs at 4.3 MB/s (~1.9 ms/8KB).  -N wait
   events = LWLock|WALWrite + IO|WalSync (backends correctly queued behind
   group-commit fsync); perf shows ~73% idle (swapper) — CPUs waiting on disk,
   not spinning on a lock.  fork on the SAME disk is also WAL-bound (fork -N
   c=192 = 28k; fiber -N c=192 = 10k — both disk-limited, fiber lower for the
   real reason below).

3. **The REAL gap = fiber -S saturates ~1.05M while fork scales to ~1.95M.**
   Definitive parity (carriers matched to load = 192, -S, 15s cells):
     c     fork        fiber(car=192)   ratio
     64    1,695k      1,051k           0.62x
     128   1,949k      1,037k           0.53x
     192   1,814k      1,067k           0.59x
     256   1,783k      1,035k           0.58x
   Raising carriers 32->192 lifted fiber from 757k to 1,067k (+41%), so carrier
   count IS the concurrency lever — but fiber then PLATEAUS at ~1.05M regardless
   of client count, while fork keeps climbing.  This is a per-transaction
   pooled-dispatch/scheduling SATURATION, not a livelock — the same
   dispatch-overhead lever ab4 flagged and the "pooled per-transaction
   re-dispatch is the wrong layer" conclusion.  This is the north-star gap to close.

## Corrected next step
Profile the SATURATED fiber -S run (car=192, c=192) to localize the ~1.05M
ceiling: carrier dispatch/futex (backend_pooled_protocol_carrier_entry, xtc
notify/amutex) vs PG lock (PinBuffer/LWLock).  ab4 saw ~36% futex on the pooled
per-transaction path — if that recurs, the fix is reducing per-transaction
park/redispatch (session affinity to a carrier for back-to-back queries, or the
thread-per-session model which the oversubscription doc argues IS the
libxtc-native path).  The lever is dispatch cost, not a lock bug.

## Fix A re-verified (survived the rebase)
pg_prenegotiate_ssl_request present in pqcomm.c (x3) + libpq-be.h ssl_prenegotiated
(x2); commits a48d157e66 (A) + b455231670 (reroute) are in origin/xtc after the
rebase.  -S c-sweep with 0 connect failures to c=256 is the functional proof A +
reroute still work on the rebased build.
