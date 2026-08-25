# Wave 2 — Why io_method=xtc isn't the best choice (2026-08-25, chiuso c7i.metal-48xl/192-core, origin/xtc 235909ccf0 = v1.37 + perf fixes)

## TL;DR
On the CURRENT tree io=xtc is NOT the futex-storm disaster the old finding
recorded -- that's stale.  It's at parity-or-slightly-better for reads and
WORSE for writes.  It isn't "best" because (a) the read hot path is already
solved by a synchronous fast path, (b) its theoretical cold-read advantage
doesn't manifest at normal carrier counts, and (c) the write path regresses
(per-IO park, no fast path).  A true batched submit/reap would help, but
`xtc_aio` doesn't expose one and PG OLTP issues IO one-at-a-time anyway.

## Measured (io=sync vs io=xtc, same box/tree)
  workload                         io=sync     io=xtc      delta
  A cached -S (192c, sb=8GB)       1,915k      2,023k      xtc +5.6%   (futex ~0% both)
  B write  -N (192c, sb=8GB)       64,246      47,208      xtc -36%    (futex ~5% both)
  C cold  -S (64c, sb=2GB, drop$)  4,810       4,800       parity

## Why (mechanism)
method_xtc.c is ISSUER-SYNCHRONOUS: submit() runs each staged IO to completion,
parking the fiber for its OWN IO before returning (mirrors method_sync, only the
call differs: xtc_aio_preadv/pwritev vs pg_preadv/pwritev).  Header says so:
"Issuer-async reap is Step 3; WAL/fsync is Step 4" -- i.e. the batched/async path
is acknowledged UNBUILT.

1. Cached reads: the preadv2(RWF_NOWAIT) fast path (already in method_xtc.c)
   serves cache hits with a plain syscall, NO fiber park, NO carrier-scheduler
   wake, NO futex.  That's why the old "36% futex, 1% of fork" collapse is gone
   and io=xtc even edges sync (+5.6%) -- it avoids sync's blocking on the rare
   miss while never parking on the common hit.
2. Cold random reads: io=xtc PARKS the fiber (frees the carrier) during the real
   disk read, which SHOULD win when carriers are scarce vs in-flight IO.  At
   c=64 with 192 carriers the disk is the bottleneck and there are plenty of
   carriers, so parking-vs-blocking is a wash -> parity.  The advantage would
   show at (clients >> carriers) with deep IO; not this config.
3. Writes: xtc_aio_pwritev parks per write and there is NO cached-write fast
   path (only READV has preadv2(RWF_NOWAIT)).  Every buffer/WAL write round-trips
   fiber park->loop->wake -> the -36% regression.

## libxtc API reality (v1.37)
xtc_aio.h exposes only per-call-parking ops (pread/pwrite/preadv/pwritev/fsync/
fdatasync) -- NO submit-many/reap-many.  A batched path COULD be built on
xtc_io_aio_submit + xtc_future_when_all (submit N, park once), but PG's OLTP
buffer manager issues one IO per ReadBuffer (num_staged_ios usually 1), so there
is no batch to amortize a single park over on the hot path.  xtc_iosched exists
(adaptive write-batching, single-writer) -- relevant to a WAL-writer redesign,
not per-backend buffer IO.

## Recommendation
- KEEP io=sync as the OLTP default.  It wins writes decisively and reads are a
  wash-or-slight-loss.
- io=xtc is a valid choice for READ-heavy / cache-friendly workloads (neutral-to-
  +5.6%) and is no longer a hazard -- the stale "futex storm" warning can be
  retired from the docs (it was pre-fast-path + pre-perf-fixes).
- Concrete improvement to make io=xtc win writes: add a NON-parking fast path for
  writes analogous to the read one -- pwritev2(RWF_NOWAIT) returns without
  blocking when the write can be buffered immediately (page cache dirtying),
  parking only when it would block (e.g. stable-storage pressure / O_DIRECT).
  This is the single highest-value io=xtc change and mirrors the proven read
  fast path.  A/B it on -N.
- The batched submit/reap path (Step 3) is NOT worth it for per-backend buffer IO
  (no batch on the hot path); it's only interesting for readahead/prefetch
  (pgaio's staged-read batches) and the WAL writer (xtc_iosched) -- separate,
  later, and measurement-gated.

## Doc cleanup owed
Retire/annotate the stale "io_method=xtc futex-storms OLTP" claim in
plan_docs/XTC_AIO_DESIGN.md and the session summaries -- it was true pre-fixes,
false now.  io=xtc reads are neutral-or-better; only writes regress, and that has
a clear fix (pwritev2 NOWAIT).

## UPDATE: write fast path (pwritev2 RWF_NOWAIT) A/B result (2026-08-25)
Added the symmetric write fast path and re-measured -N c192 (sb=8GB, 2 runs):
  io=sync:                    56,512 / 56,651
  io=xtc + write fast path:   51,957 / 60,006   (was 47,208 = -36% before the fix)
The -36% write regression is CLOSED -- io=xtc is now at parity with sync on -N
(within run-to-run noise; run 2 beat sync).  Combined with reads (neutral/+5.6%),
io=xtc is now neutral-or-better across cached-read, cold-read, and write OLTP.
Change on branch io-xtc-write-fastpath (a0503e0537); pending two reviews + a
wider A/B (more runs, mixed tpcb) before landing.  KEEP io=sync as the documented
default until the review, but the "io=xtc regresses writes / futex-storms OLTP"
warning is now obsolete.
