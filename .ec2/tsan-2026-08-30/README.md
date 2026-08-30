# TSan run 2026-08-30: root-caused the native-path concurrent-commit wedge

PG xtc + libxtc v1.40.2 both -fsanitize=thread (clang18/compiler-rt18/system glibc).
Workload: pgbench -c 32 -j 8 TPC-B write, mt=on, pooled_protocol_carriers=0 (fibers),
fsync=on.  Collapsed to 0 tps after ~20s (reproduced the wedge under TSan); 2361 warnings.

REAL BUG (un-suppressed, non-spinlock, libxtc-internal, on the resume path):
  xtc_io_del_fd / __find_fd mutate the per-io fd list io->fds (io_uring.c:80,326,331,
  342,343) from TWO carrier-loop threads concurrently -- no lock.  Called from
  xtc_proc_wait_fd:2219 cleanup ("use wl, the loop we registered on") on fiber resume;
  work-stealing migrates a parked-fd-owning fiber so the del_fd runs cross-loop.  Corrupts
  the registry -> lost fd/completion -> stranded WALWriteLock holder -> wedge.
  Report: plan_docs/phase16_audits/LIBXTC_TSAN_IO_DEL_FD_CROSS_LOOP_RACE.md
  Raw two-stack reports: tsan_xtc_races.txt ; full summary table: tsan_summary_table.txt

RULED OUT (same run):
 - ProcWakeSemaphore proc.c:2464/2476 vs ProcSemaphoreWaitFiber:2318 (78): both under
   SpinLockAcquire(&proc->sem_fiber_lock) -> PG-spinlock-blind FALSE POSITIVE.
 - PgRuntimeInstallHotCurrentCells/ClearHotCurrentRootRefs (268+): known hot-cell item.
 - dlist_*/lock.c/liburing _io_uring_get_sqe: known-benign shmem/lock-free/liburing.

Build recipe (standing): system clang18 + compiler-rt18 + system glibc (NOT nix); libxtc
meson -Db_sanitize=thread (auto XTC_TSAN_FIBERS); PG -Db_sanitize=thread -shared-libsan;
add /usr/lib/clang/18/lib/x86_64-amazon-linux-gnu (libclang_rt.tsan.so) to ld.so.conf +
LD_LIBRARY_PATH or zic/the build tools fail to load the TSan runtime.
