# TSan on the multithreaded carrier -- findings (2026-08-04)

Ran ThreadSanitizer end-to-end against the pooled threaded carrier runtime on a
fresh EC2 AL2023 box (system clang-18 + compiler-rt-18 + system glibc 2.34 --
NOT the nix devshell, which has the compiler-rt<->glibc GLIBC_PRIVATE skew).
origin/xtc = 2b41b4883f4, libxtc v1.32.0 built with -fsanitize=thread
-DXTC_TSAN_FIBERS=1 (all four __tsan_*_fiber annotations compiled in). Drove
pgbench -S/-N, LISTEN/NOTIFY, connect-storm, and the test_backend_runtime suite
under TSAN_OPTIONS=halt_on_error=0.

## Toolchain result: TSan WORKS on the carrier now
The nix compiler-rt<->glibc skew is ABSENT on system clang+glibc. TSan is
genuinely runnable -- NOT a libxtc gap (libxtc's fiber-identity annotations are
present and live). Build recipe, not a code owe: AL2023 default clang-15 fails to
compile libxtc (os_errno.c:30 _Atomic non-constant initializer) -- use dnf
clang18 + compiler-rt18 (still system glibc). PG built -Db_sanitize=thread
-shared-libsan (libpq's --no-undefined shlib link needs the shared TSan runtime).

## Race triage (thousands of warnings -> a small set of distinct locations)

### A. LIKELY TSan-BLIND-TO-SPINLOCK false positives (coordinator reclassified)
The pooled-scheduler carrier-table "race" (thread_runtime /
scheduler->idle_carrier_count/registered_carrier_count):
  WRITE PgRuntimeProtocolSchedulerRegisterCarrier backend_runtime_backend.c:1747
  READ  PgRuntimePooledProtocolIdleCarrierCount   backend_runtime.c:1434
BOTH accesses are inside SpinLockAcquire(&scheduler->lock). PG spinlocks are
custom inline asm, NOT pthread mutexes TSan intercepts, so TSan cannot see the
happens-before and reports a race on the lock-protected counter -- the classic
"TSan is blind to PG spinlocks" false positive (same reason the whole '??'
shmem/pg_atomic/SpinLock bucket lights up). Very likely NOT a real race. Fix =
a TSan suppression, not code (after confirming both accesses are always under
scheduler->lock -- they are at these sites).

### B. THE REAL DESIGN QUESTION -- process-global "current work" hot cells
  PgRuntimeInstallHotCurrentCells        backend_runtime.c:360
  PgRuntimeClearHotCurrentRootRefs       backend_runtime.c:228-238
  PgRuntimeLoadHotCurrentRootRefs / PgRuntimeClearHotFieldPointers
  cells: PgRuntimeHotCurrentCellModeState (4B); PgCurrent{Runtime,Carrier,
    Backend,Session,Connection,Execution}HotRefProcessRef (8B ea); PgTop*/
    PgPortal*/PgMessage* hot-ref cells.
Process-globals (PG_GLOBAL_RUNTIME expands to NOTHING -- a plain global) holding
the "current work" hot-pointer scratch, stamped per-backend at bringup. Under
thread-per-session this was serialized; under the POOLED SCHEDULER backends are
brought up/switched on MULTIPLE carriers concurrently, so these cells are now
written from multiple carrier threads. DESIGN QUESTION: genuine bug or
benign-by-design (each carrier only reads back its own last write in a
fiber-affine window)? Likely wants per-carrier/thread-local cells OR a proof the
pooled scheduler's affinity forbids a cross-carrier stale read. This is the
genuinely-new-to-threading, worth-investigating finding. Whether observable vs
just-writes needs the two-stack analysis in
.ec2/tsan-2026-08-04/real_race_reports.txt.

### C. BENIGN (known PG patterns; appear on any TSan-on-PG run, process too)
LWLock state / buffer headers / xlog buffers / dynahash / lock table ('??'
shmem), pg_atomic_*, SpinLock spin-backoff (s_lock.c), WAL insert lock-free
reservation, GrantLock/dynahash, SetLatch, ComputeXidHorizons, pgstat
pending_since, data_checksums, pqsignal handlers. PG's documented lock-free
discipline TSan structurally can't verify.

### D. libxtc-internal
coro_uctx.c __xtc_async_ex (thread-creation context frame under nearly every
backend-fiber report -- not a distinct bug), xtc_yield, __xtc_coro_step, exec.c
steal/worker, __xtc_fiber_ctx_save/restore, g_xtc_ready, __fault_guard_installed.

## Follow-ups (each its own scoped task, NOT done here)
1. Item B (hot-cell family): decide per-carrier/thread-local vs prove-affinity-
   safe for the PgRuntime*HotCurrent*/Hot*Ref cells under the pooled scheduler.
   HIGHEST-value TSan follow-up.
2. Item A: TSan spinlock suppression (or annotate PG spinlocks) so lock-protected
   scheduler counters stop dominating the report.
3. Standing recipe: TSan-on-carrier = system clang18 + system glibc, NOT nix.
Raw logs: .ec2/tsan-2026-08-04/ (real_race_reports.txt, racing_locations.txt,
our_code_frame0.txt, all_summaries.txt).
