<!--
ARCHIVAL RECORD (2026-07-24).  This is the findings doc from the original
M16.1b out-of-tree spike -- the FIRST attempt to run a PostgreSQL backend as an
xtc_proc fiber -- which lived on branch xtc-m16-1b-spike in the ~/ws/postgres/
master worktree (that worktree was accidentally used for the spike; it has been
restored to origin/master).  The full spike is preserved at git tag
`archive/xtc-m16-1b-spike` (tip 1413136fe17) if the code is ever needed.

This work is SUPERSEDED by the mature `xtc` branch: the spike's pg_xtc_sched.c
is the ancestor of today's pg_xtc_carrier.c, and the exact blocker it documents
(the dup'd client read-fd diverging from the fd registered in the WaitEventSet)
is part of the fiber wait-boundary seam the xtc branch resolved and built far
past (pooled protocol scheduler, migration, thread-explosion fix, ~98.6% of fork
on OLTP).  Kept only as the project's origin-story / seam-discovery record.
-->

# M16.1b Spike Findings -- PostgreSQL backend as an xtc_proc fiber

Status: **(B) BLOCKED at a precise, well-understood point** -- one small
seam bug away from a plausible "select 1" round-trip.  OUT-OF-TREE spike in
`/home/gburd/ws/postgres/master` on branch `xtc-m16-1b-spike`.  Nothing here
touches the libxtc repo.

## How far it got (the good news)

The backend really runs as an xtc_proc fiber on a postmaster-hosted xtc
scheduler.  End to end, verified from logs:

1. libxtc.a links into the PG backend; xtc seam symbols present in the binary
   (`xtc_pg_spawn_backend`, `xtc_pg_sched_start`, `xtc_proc_spawn`, ...).
2. `postmaster_child_launch` routes ONLY `B_BACKEND` (child_type=1) to
   `xtc_pg_spawn_backend`; every aux/bgworker type still forks (confirmed:
   child_type 5,10,11,12,13,16 fork; only type 1 becomes a fiber).
3. The scheduler thread comes up; the backend fiber is spawned and enters:
   `xtc: backend fiber entered, running main_fn as an xtc_proc`.
4. The fiber runs BackendMain -> PostgresMain -> InitPostgres -> StartupXLOG
   (recovery + checkpoint), reaches `connection received: host=[local]`, and
   yields to the xtc loop at the client-read wait -- the scheduler thread
   parks idle in `io_uring_wait_cqes` (NOT busy-spinning, NOT blocking).

So the fiber path executes real backend code and the runtime seam
(WaitEventSetWait -> xtc_proc_wait_fd) fires.

## The precise blocker (the wall)

The fiber parks forever in `xtc_proc_wait_fd(fd=3, READABLE, timeout=-1)` at
the startup-packet read and never wakes, because it is waiting on the WRONG
file descriptor.  Proven by an fstat probe inside the seam:

```
xtc: backend fiber MyClientSocket->sock=12 (dup'd client fd)
xtc: WES enter nevents=2 ... fds=[3,4,..] ev=[1(READABLE),16(LATCH),..]
     fstat3=0(ino=2064)  fstat12=0(ino=1846324)
xtc: wait_fd ENTER fd=3 pg_events=2 interest=0xd timeout_ms=-1   <-- never returns
```

- The real client connection is fd **12** (inode 1846324) -- the descriptor
  `xtc_pg_spawn_backend` created with `dup()` and stored in
  `MyClientSocket->sock`.  PG reads/writes the client on fd 12.
- The WaitEventSet the fiber blocks on has its socket event registered with
  fd **3** (inode 2064 -- a different open file, the pre-dup / listen-side
  descriptor).  fd 3 never becomes readable, so the fiber hangs and psql
  times out.

Root cause: the `dup()` workaround (added so the postmaster's
`closesocket(s.sock)` in ServerLoop, postmaster.c:1733, does not kill the
fiber's socket) makes the READ fd (12, via MyClientSocket/port->sock) diverge
from the fd captured into the WaitEventSet (3).  The wait-registration path and
the read path end up on different descriptors.

## Concrete next steps to get past the wall

In priority order (each is small and local to the spike files):

1. **Eliminate the read/wait fd divergence.**  Two candidate fixes:
   a. **Drop the `dup()`** in `xtc_pg_spawn_backend` (keep client_sock->sock =
      original fd 3) AND stop the postmaster from closing it for the xtc
      backend: in `postmaster.c` ServerLoop, guard the
      `closesocket(s.sock)` at line ~1733 with `#ifdef USE_XTC_BACKENDS ... if
      (child_type != B_BACKEND) ...` so the fiber keeps the one true fd.
      Then read fd and wait fd are both 3.  RISK: the postmaster now leaks the
      client fd for non-fiber paths -- gate strictly on the xtc backend.
   b. If keeping dup: ensure EVERYTHING uses the dup'd fd.  The 2-event set
      with fd=3 is created before/around pq_init from a stale ClientSocket
      copy; make the dup happen BEFORE any ClientSocket copy is captured, and
      confirm `port->sock` == the fd registered in FeBeWaitSet.  Verify with
      the existing fstat probe that fds match.

2. **Wire SetLatch -> fiber wakeup.**  Pure-latch waits currently park on the
   latch signalfd (fd 4, WAIT_USE_SIGNALFD) for readability.  That works ONLY
   if `SetLatch/WakeupMyProc` (raises SIGURG to self, feeding the process
   signalfd) is actually delivered so the signalfd becomes readable on the
   scheduler thread.  Confirm SIGURG reaches the signalfd in the shared
   postmaster process; if signal delivery races between postmaster threads,
   replace the latch path with the proven mailbox glue
   (examples/09_pgmock/pg_latch.h: SetLatch -> xtc_send into the fiber's
   mailbox, ResetLatch -> drain) by intercepting SetLatch under
   USE_XTC_BACKENDS.

3. After 1+2, re-run `run_spike.sh`; expect the fiber to read the startup
   packet, authenticate (trust), and answer `select 1`.

## Known remaining walls beyond this one (from the plan's 5 risks)

- **on_shmem_exit list** is shared: the fiber calls `on_exit_reset()`, which
  ABANDONS the postmaster's own exit handlers (M16.3b globals wall).  OK for a
  single short-lived backend; fatal for N.
- **PG_exception_stack / sigsetjmp**: not yet exercised (we never reached an
  ereport(ERROR) inside the fiber).  The sigsetjmp error stack is a per-process
  global; on the fiber stack it should be fine for ONE backend but must be
  saved/restored per fiber for N.  UNTESTED -- next milestone.
- **Signals to the scheduler thread**: SIGQUIT/SIGTERM/SIGALRM (startup-packet
  timeout) are process-directed; which thread services them in the shared
  postmaster is unverified.
- **InitPostmasterChild skipped**: we run only the safe subset
  (IsUnderPostmaster, on_exit_reset, InitializeWaitEventSupport,
  InitProcessLocalLatch, InitializeLatchWaitSet).  Deliberately skipped:
  InitProcessGlobals (overwrites MyStartTime + reseeds PRNG), setsid(),
  pqsignal(SIGQUIT)+sigprocmask, PostmasterDeathSignalInit -- all destructive
  to the shared postmaster.

## Exact build recipe (reproducible)

Prereqs: stock PG already configured with
`./configure --prefix=/home/gburd/pgxtc-spike --without-icu --without-readline
--without-zlib --enable-debug`; libxtc built at
`/home/gburd/ws/xtc/build_unix/libxtc.a` (`mkdir -p build_unix && cd build_unix
&& ../dist/configure && make -j`).

Wiring (already applied on the branch, in `src/Makefile.global`):

```
CPPFLAGS = ... -DUSE_XTC_BACKENDS -I/home/gburd/ws/xtc/src/inc
LIBS = -lm /home/gburd/ws/xtc/build_unix/libxtc.a -lpthread -ldl \
       -L/nix/store/grrnp6y64yv2ip3pb7a9hvz7h89vfgzg-liburing-2.12/lib \
       -luring -lssl -lcrypto
```

libxtc pulls in liburing (headers + -luring) and openssl (-lssl -lcrypto).
The link MUST run inside a shell that provides openssl so -lssl/-lcrypto
resolve via NIX_LDFLAGS:

```
cd /home/gburd/ws/postgres/master
nix-shell -p gcc gnumake bison flex perl openssl \
  --run "make -C src/backend && make -C src/backend install"
```

Run the spike (single backend at a time):

```
$HOME/pgxtc-spike/bin/initdb -D /scratch/xtc-test/pgdata -U postgres --no-locale
# postgresql.conf: listen_addresses='' unix_socket_directories='/scratch/xtc-test'
#                  autovacuum=off logging_collector=off
bash /scratch/xtc-test/run_spike.sh    # starts server, runs psql 'select 1'
```

Helper scripts (in /scratch/xtc-test, NOT in the repo):
- `run_spike.sh`  -- start postmaster, connect once, dump xtc log lines.
- `gdb_spike.sh`  -- same under gdb with an 18s self-interrupt to capture the
  parked-fiber backtrace (yama ptrace_scope=1 forbids attaching to a running
  pid, so gdb must launch the inferior).  Both scripts clear stale SysV shmem
  (48-byte PG control segs) and /dev/shm/PostgreSQL.* left by `kill -9`.

## The seam (files changed on the branch)

- `src/backend/postmaster/pg_xtc_sched.c` (new) -- xtc app on a pthread;
  `xtc_pg_spawn_backend` runs BackendMain as a fiber; `xtc_pg_wait_fd` bridges
  WL_* <-> XTC_IO_*.  Runs the safe InitPostmasterChild subset in the fiber.
- `src/backend/storage/ipc/waiteventset.c` -- WaitEventSetWait intercept:
  when `xtc_in_backend_fiber`, pick the CLIENT SOCKET event (must skip
  WL_LATCH_SET / WL_EXIT_ON_PM_DEATH events, which also carry real fds), wait
  on it via the xtc loop; pure-latch waits park on the latch signalfd.
- `src/backend/postmaster/launch_backend.c` -- route B_BACKEND to the fiber.
- `src/backend/port/pg_xtc_glue.{c,h}` -- ported Latch glue (from
  examples/09_pgmock/pg_latch.*), not yet on the SetLatch hot path.
- `src/backend/postmaster/Makefile` -- pg_xtc_sched.o.
- `src/Makefile.global` -- libxtc link + include + USE_XTC_BACKENDS.

Diagnostic elogs (rate-limited to 40 lines) are still in the seam; remove them
once the fd fix lands.
