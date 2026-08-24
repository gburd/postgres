# CPU-bound collapse ROOT CAUSE + FIX: shared-address-space malloc policy (2026-08-24, chiuso c7i.metal-48xl/192-core)

## The user's lead (off-pool threads / xtc_blocking) -- investigated, not the fix
libxtc DOES have off-loop offload: xtc_blocking_run(fn, arg, &res) runs fn on a
CPU-scaled blocking-thread pool (max(4,cpus) cap 64, grows on demand) and parks
the calling fiber.  BUT running a whole PG query on a blocking-pool thread is
UNSAFE here: the per-backend "current" state (CurrentPgBackend etc.) is resolved
via PgCurrentBackendHotRefThreadRef, which is PG_THREAD_LOCAL (__thread) -- it
does not follow the work onto a pool thread, so the query would see a NULL/wrong
backend.  xtc_blocking is right for self-contained leaf calls with no per-backend
PG state; it is not a drop-in for query execution.  (xtc_preempt is Phase-0/off;
not usable yet.)

## The real root cause (measured, not the scheduler)
Under a CPU-bound query (generate_series(1,20000)+sqrt/sin, tuplestore) at c=192:
 - fiber default: ~3,700 tps vs fork ~51,000 (0.07x).
 - The pooled pool DID grow (carriers_started 2 -> 43; the pg_stat_xtc_carriers
   view shows the 15 worker-fiber EXECUTOR loops, NOT the pooled carriers -- use
   the pg_stat_xtc_runtime "carriers_started" counter).  So NOT pool sizing.
 - perf of the grown run: ~35% __memset_avx512 + mmap/munmap/mremap, real query
   work otherwise, ~0% futex.  The carriers share ONE address space; glibc mmaps
   each large tuplestore block and munmaps on free, and across dozens of carrier
   threads the kernel mmap_lock + cross-core TLB shootdowns serialize them.
   Process mode never hits this: each backend is its own process (private VA).

## The fix (backend_runtime.c PgRuntimeConfigureThreadedAllocator, pooled path)
The prior tuning had M_TRIM_THRESHOLD=128KB and left glibc's mmap path on -- the
worst case (constant mmap+munmap of large blocks).  Change to:
   M_MMAP_MAX = 0          (never mmap allocations; keep large blocks in-arena)
   M_TRIM_THRESHOLD = 64MB (retain freed blocks for reuse, don't munmap to OS)
   M_TOP_PAD = 0           (unchanged)
(M_ARENA_MAX=carriers stays; operator MALLOC_* env still wins.)

## Result (stable, 3-run median, in-code fix, NO env)
                 compute tps        vs fork
  fork           50,940 / 50,956 / 51,409
  fiber (fix)    78,697 / 78,797 / 79,203    1.55x  <- BEATS fork by 55%
  -S read OLTP   fiber ~2.0M vs fork ~1.8M   ~1.1x  (unchanged / still ahead)
  RSS after compute: 946MB -> ~1124MB (+19%, bounded)  Env A/B: 128KB=3.7k,
  ARENA_MAX only=4.3k, mmap0+trim64MB=68.5k, mmap0+trim-1=33.6k (64MB is best).

## Bottom line
Fiber now BEATS fork on BOTH read OLTP (-S ~1.1x) AND CPU-bound compute (~1.55x)
-- the north-star result.  The lever was the shared-address-space allocator
policy that threads incur and fork does not, NOT the scheduler and NOT off-pool
offload.  Commit 1994f9dea2 on branch pooled-demand-grow.

## Before landing (required, unchanged discipline)
Startup/allocator + hot-path scheduler changes -> clean-box world tests (process
byte-for-byte, threaded test_backend_runtime, check-threaded-pooled) + two
adversarial reviews before origin/xtc.  Watch RSS under real mixed workloads
(the +19-24% is a tuplestore-heavy worst case; confirm it does not balloon under
sustained large-sort/hash load -- M_TRIM_THRESHOLD=64MB caps per-arena retention
but total across many arenas is arenas*64MB worst case).
