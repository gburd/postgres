# libxtc TSan finding: `xtc_io_del_fd` / `io->fds` registry is mutated cross-loop WITHOUT synchronization -> the native-path concurrent-commit resume wedge

Date: 2026-08-30
libxtc: v1.40.2 (rev ed60a396), built -fsanitize=thread (XTC_TSAN_FIBERS auto-on).
PG "xtc" branch built -fsanitize=thread -shared-libsan (system clang18 + compiler-rt18 +
system glibc, per our standing recipe; NOT nix).
This is the TSan run you asked for in the v1.40.2 reply. TSan found it in one workload run,
exactly as you predicted.

--------------------------------------------------------------------------------
## TL;DR
The native-path concurrent-commit collapse is a genuine DATA RACE in libxtc's io_uring
backend: `xtc_io_del_fd()` unlinks from the per-`xtc_io_t` fd list `io->fds` (and
`__find_fd()` walks it) with NO synchronization, and it is called from TWO DIFFERENT
carrier-loop OS threads concurrently against the SAME `io`. The list corrupts, a parked
fiber's fd registration/completion is lost, the fiber (holding WALWriteLock) is never
resumed, and the commit pipeline wedges. TSan pins it precisely.

## The race (TSan, both stacks) -- address 0x7250000026e0, an 8-byte pointer in io->fds
READ  (thread T91, a carrier loop):
    __find_fd            io_uring.c:80      (walks io->fds: `for (p=io->fds; p; p=p->next)`)
    xtc_io_del_fd        io_uring.c:326     (`uf = __find_fd(io, fd)`)
    xtc_proc_wait_fd     proc.c:2219        (cleanup: `xtc_io_del_fd(wl->io, park_fd)` on resume)
    xtc_pg_wait_fd       pg_xtc_carrier.c:1354
    WaitEventSetWaitBlock waiteventset.c:1483
    ... PgSessionStagingWaitProtocolRead -> BackendMainWithStartupData -> xtc_carrier_proc

PREVIOUS WRITE (thread T92, a DIFFERENT carrier loop):
    xtc_io_del_fd        io_uring.c:331     (unlink: `for(pp=&io->fds;...) if(*pp==uf){*pp=uf->next;...}`)
    xtc_proc_wait_fd     proc.c:2219        (same cleanup path)
    ... (another backend fiber resuming on another loop)

Also seen (same list): io_uring.c:342/343 (`uf->next = io->zombies; io->zombies = uf;`)
and io_uring.c:80 (`__find_fd`) -- 139 reports total across these lines. Two carrier-loop
threads mutate the same `io->fds` / `io->zombies` list at once.

## Why two threads touch the same io->fds
`xtc_proc_wait_fd`'s cleanup (proc.c:2219) does, on resume:
    if (self->task->park_fd >= 0) { xtc_io_del_fd(wl->io, park_fd); park_fd = -1; }
with the comment "Use wl (the loop we registered on), not the home loop." Under
work-stealing / eager rebalance, migratable client-backend fibers register their park fd on
whichever loop they run on and delete it on resume from whichever loop they resume on. When
a fiber is stolen between park and resume (or two fibers that share a target `io` run on
different OS threads during a rebalance window), `xtc_io_del_fd` mutates one `io`'s fd list
from a thread that is not that io's single owner. `io->fds` is a plain singly-linked list
with no lock, built on the "one loop = one thread" invariant -- which work-stealing
migration of a parked-fd-owning fiber violates.

## Consequence (matches our gdb wedge)
A corrupted `io->fds` / lost multishot-poll registration means the parked fiber's fd
readiness (the WAL fsync completion, or the sem_wake_fd) is never delivered -> the fiber is
never marked runnable -> it stays suspended holding WALWriteLock -> WAL writer + every
committer block behind it -> commits collapse to 0 tps after the first burst (our
64-client TPC-B result). It is concurrency-gated because it needs the steal-between-park-
and-resume window, which is why single-fiber init is clean but 64-way commit collapses.

## What we RULED OUT with the same TSan run (so you can ignore these)
- ProcWakeSemaphore proc.c:2464/2476 vs ProcSemaphoreWaitFiber proc.c:2318 (78 reports):
  both accesses are inside SpinLockAcquire(&proc->sem_fiber_lock). PG spinlocks are custom
  inline asm TSan cannot see -> spinlock-blind FALSE POSITIVE. The sem_fiber wake hand-off
  is correctly locked; the resume bug is NOT there.
- PgRuntimeInstallHotCurrentCells / PgRuntimeClearHotCurrentRootRefs backend_runtime.c
  (268+ reports): our known process-global "hot current-work cells" item; separate PG-side
  design question, not this wedge.
- dlist_*, lock.c GrantLock/SetupLockInTable/UnGrantLock, liburing _io_uring_get_sqe:
  known-benign PG shmem/lock-free + liburing-internal buckets (appear on any TSan-on-PG
  run).
The `xtc_io_del_fd`/`__find_fd`/`io->fds` race is the one un-suppressed, non-spinlock,
libxtc-internal finding -- and it is on the exact resume path.

## Fix direction (your call)
The per-`io` fd registry (io->fds, io->zombies, __find_fd, xtc_io_del_fd, and presumably
xtc_io_add_fd) needs to be safe against cross-loop callers, OR xtc_proc_wait_fd's cleanup
must guarantee the del_fd runs on the fd's owning-loop thread (e.g. route the del_fd to the
registering loop via that loop's inbox rather than calling it directly from the resuming
thread). Given your architecture, "post the fd-unregister to the owning loop" is likely
cleaner than locking the hot registry. This mirrors the v1.39 producer-must-nudge and
v1.40.2 done-flag fixes: the cross-loop step is the unsafe one.

## Reproduction
1. PG "xtc" on libxtc v1.40.2, both -fsanitize=thread (clang18/compiler-rt18/system glibc;
   libxtc via meson -Db_sanitize=thread which auto-enables XTC_TSAN_FIBERS; PG
   -Db_sanitize=thread -shared-libsan; add clang's libclang_rt.tsan.so dir to ld.so.conf).
2. multithreaded=on, pooled_protocol_carriers=0 (sessions-as-fibers), fsync=on,
   TSAN_OPTIONS="halt_on_error=0 suppressions=<benign buckets>".
3. pgbench -i -s 20; pgbench -c 32 -j 8 -T 40. Collapses to 0 tps after ~20s; TSan emits
   the xtc_io_del_fd/__find_fd race within seconds of load.
We can share tsan_xtc_races.txt (full both-stack reports) and the summary table.

## Note
This is fully consistent with your v1.40.2 reply: the native io_uring FSYNC path "was not
subject to the [offload close-vs-write] race" -- correct; it is subject to THIS separate
io->fds cross-loop race. Fixing it should close the concurrent-commit collapse on both
paths (offload's completion pipe fd goes through the same xtc_proc_wait_fd/xtc_io_del_fd
cleanup).
