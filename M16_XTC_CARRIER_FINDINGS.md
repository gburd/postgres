# M16 xtc-carrier Findings -- threaded PostgreSQL on the xtc scheduler

Status: **(A) SUCCESS** -- `psql -c "select 1"` round-trips with the threaded
backend running as an xtc_proc fiber, and its client-socket waits driven by
`xtc_proc_wait_fd` on the xtc scheduler loop.  OUT-OF-TREE, throwaway, on
branch `xtc-carrier` in `/home/gburd/src/multithreaded-postgres`.  Nothing
here touches the libxtc repo.

## The result (proof from the postmaster log)

```
LOG:  database system is ready to accept connections
LOG:  xtc: carrier scheduler thread up (single-loop app)
LOG:  xtc: spawned backend fiber pid=(loop=0,local=1,gen=1)
LOG:  xtc: B_BACKEND launched as xtc fiber (child_slot=1)
xtc: backend fiber entered; running backend_thread_entry
xtc: fiber wait_fd fd=34 interest=0xd timeout_ms=-1 (via xtc_proc_wait_fd)   <-- repeated
```
```
$ psql -c "select 1 as xtc_carrier"
 xtc_carrier
-------------
           1
(1 row)
psql rc=0
```

- `fd=34` is the backend's client socket (`MyClientSocket->sock`).
- `interest=0xd` = READABLE|HUP|ERR; `timeout_ms=-1` = wait forever.
- Every top-level protocol read parks the fiber on the xtc loop instead of
  blocking the carrier thread in `epoll_wait`.  This is threaded PostgreSQL
  cooperatively scheduled by xtc.

## Why this worked where the fork spike did not

The fork spike (xtc-m16-1b-spike) died at the read/wait **fd divergence**: a
`dup()` made the backend read fd (12) differ from the fd registered in the
WaitEventSet (3), so the fiber parked forever on the wrong descriptor.

The multithreaded tree already solved that: it keeps the backend **in-thread**.
`postmaster_backend_thread_launch` dup()s the client fd ONCE into
`thread_start->client_sock.sock`; `backend_thread_run_backend` sets
`MyClientSocket = &thread_start->client_sock`; `FeBeWaitSet` is later built from
that same `MyProcPort->sock`.  Read fd == wait fd, always.  The PG-globals wall
that blocked the fork approach is also gone -- the tree runs the full
thread-per-session init.  We reuse ALL of it and change only the carrier.

## The seam (files changed on the branch)

1. `src/backend/postmaster/pg_xtc_carrier.c` / `.h` (new)
   - Single-loop `xtc_app` on a dedicated pthread (`xtc_pg_carrier_start`).
   - `xtc_pg_launch_backend_fiber(entry, arg)` spawns the tree's
     `backend_thread_entry` as an `xtc_proc` fiber on the carrier loop.
   - `xtc_pg_wait_fd(fd, WL_*, timeout_ms)` bridges WL_* <-> XTC_IO_* and calls
     `xtc_proc_wait_fd`.
   - `__thread bool xtc_in_backend_fiber` -- true only on the carrier thread
     while a backend fiber runs.
   - `xtc_set_stack_size(8 MiB)` -- CRITICAL: xtc's default fiber stack is 64
     KiB, which overflows instantly in PG parser/planner recursion.  Matched to
     pg_thread.c's 8 MiB pthread stack.  (Calibration knob -- deepen if a path
     faults.)

2. `src/backend/postmaster/launch_backend.c`
   - For `USE_XTC_CARRIER` + `B_BACKEND`, route to
     `xtc_pg_launch_backend_fiber(backend_thread_entry, thread_start)` instead
     of `pg_thread_create`.  Still publishes the logical backend.

3. `src/backend/storage/ipc/waiteventset.c`
   - `WaitEventSetWaitBlock` (WAIT_USE_EPOLL): when `xtc_in_backend_fiber`, wait
     on `set->epoll_fd` (itself pollable) via `xtc_pg_wait_fd`, then a
     non-blocking `epoll_wait(...,0)` to harvest.  ALL event decoding below is
     reused unchanged -- the smallest possible intercept.

4. `src/Makefile.global.in` (template, so configure regenerates it)
   - `USE_XTC_CARRIER` gate: `-DUSE_XTC_CARRIER -I$(XTC_ROOT)/src/inc` and
     `LIBS += $(XTC_ROOT)/build_unix/libxtc.a -luring -lssl -lcrypto`.

5. `src/backend/postmaster/Makefile`
   - `pg_xtc_carrier.o` when `USE_XTC_CARRIER`.

## Baseline-build fixes (pre-existing multithreaded-tree bugs, gcc 14 + cassert)

Committed separately (commit "fix multithreaded-tree baseline build ...").
These are NOT xtc; they block a clean build of the stock branch under gcc 14.3:

- `nodes/read.c`, `nodes/readfuncs.c`: include `utils/backend_runtime.h`
  (defines `struct PgExecutionNodeIOState`) BEFORE `nodes/readfuncs.h` aliases
  `restore_location_fields` -> accessor macro.  Wrong order lets the object
  macro poison the struct field declaration / recurse through the generated
  `.def` accessor ("declared as a function" / "has no member").
- `nodes/readfuncs.h`, `nodes/outfuncs.c`: pin `restore/write_location_fields`
  to a stable inline over the extern accessor, immune to a later transitive
  `backend_runtime.h` re-expanding the recursive `.def` macro.
- `utils/activity/pgstat_io.c`: include `access/xlog.h` for the
  `track_wal_io_timing` accessor (GUC moved into session runtime state).

The baseline was built **without `--enable-cassert`** for the xtc bringup,
because cassert also trips a pre-existing bootstrap-mode assertion
(`Assert("GUCMemoryContext == NULL")` in guc.c during initdb -- the tree's
GUC-into-session-state refactor is not bootstrap-safe under cassert with gcc
14).  That assertion is orthogonal to xtc; documented as a known gap.

## Exact build recipe (reproducible)

Prereqs: `libxtc.a` at `/home/gburd/ws/xtc/build_unix/libxtc.a`.

```sh
cd /home/gburd/src/multithreaded-postgres     # branch xtc-carrier
export TMPDIR=/scratch/xtc-test; mkdir -p /scratch/xtc-test

nix-shell -p gcc gnumake bison flex perl readline zlib openssl liburing --run '
  ./configure --prefix=/scratch/xtc-test/inst --without-icu --enable-debug \
              CFLAGS="-O1 -g"
  make USE_XTC_CARRIER=1 -j"$(nproc)"
  make USE_XTC_CARRIER=1 install
'
```

Run (fresh cluster, LC_CTYPE=C -- threaded mode requires the db LC_CTYPE to
match the postmaster process LC_CTYPE):

```sh
export PGDATA=/scratch/xtc-test/pgdata LC_ALL=C LC_CTYPE=C LANG=C
export LD_LIBRARY_PATH=<liburing-2.12>/lib   # or run inside the nix-shell
/scratch/xtc-test/inst/bin/initdb -D "$PGDATA" -U postgres --no-locale -E UTF8
cat >> "$PGDATA/postgresql.conf" <<EOF
listen_addresses=''
unix_socket_directories='/scratch/xtc-test'
autovacuum=off
logging_collector=off
EOF
/scratch/xtc-test/inst/bin/pg_ctl -D "$PGDATA" -l /scratch/xtc-test/pm_xtc.log \
    -o "-c multithreaded=on" -w start
LC_ALL=C /scratch/xtc-test/inst/bin/psql -h /scratch/xtc-test -U postgres \
    -d postgres -c "select 1 as xtc_carrier;"
grep -aE "xtc:" /scratch/xtc-test/pm_xtc.log
```

Helper: `/scratch/xtc-test/run_xtc.sh` (NOT in the repo).

Stock baseline confirmed BEFORE xtc: process mode + threaded mode
(`multithreaded=on`) both answer `select 1`; the backend runs as an OS thread
inside the postmaster process (8 threads, zero child processes -- verified via
/proc/PID/task and ps --ppid).

## Known walls beyond this milestone (concrete next steps)

1. **Fiber lifecycle / multiple backends.**  A second connection spawns a
   second fiber (`local=2`) on the single loop, but the first fiber does not
   exit when its psql disconnects, and a second query then hung.  Next: wire
   fiber teardown -- when the backend's `proc_exit` runs inside the fiber,
   `xtc_exit_self` and release the loop slot; verify N sequential and N
   concurrent backends.  Single-loop = cooperative; a real carrier POOL
   (many loops/threads) is the Phase-15 shape.

2. **Startup-process teardown crash (pre-existing, flaky, NOT xtc).**  After a
   non-clean shutdown, recovery runs and the StartupProcess thread SIGSEGVs in
   `proc_exit -> PgBackendResetXLogClosedState -> MemoryContextDelete`
   (backend_runtime_teardown.c:569).  This is in a normal pthread carrier, not
   the xtc fiber.  It corrupts the cluster and forces a re-initdb.  Reproduce on
   the pristine `multithreaded` branch and report upstream; use clean
   `pg_ctl -m fast stop` to avoid triggering recovery meanwhile.

3. **Latch/SetLatch wakeups.**  The epoll-fd intercept already covers latch
   wakeups (the latch signalfd is a registered epoll event, so SetLatch makes
   the epoll fd readable and unparks the fiber).  Not separately exercised for
   cross-fiber SetLatch; verify under LISTEN/NOTIFY.

4. **sigsetjmp / ereport(ERROR) inside the fiber.**  Not yet exercised (no
   error path hit).  `PG_exception_stack` is per-thread in this tree; on the
   fiber stack it should hold for one backend, but confirm an ERROR unwinds
   cleanly and the fiber survives.

5. **cassert build.**  Fix the bootstrap `GUCMemoryContext == NULL` assertion so
   the xtc carrier can run under `--enable-cassert` (better crash diagnostics).

6. **Remove diagnostic writes.**  The rate-limited raw `write()` proofs in
   `pg_xtc_carrier.c` (fiber-entered, wait_fd) should be dropped once the
   lifecycle work lands.

## Commits on branch xtc-carrier

- `xtc-carrier: fix multithreaded-tree baseline build under gcc 14 + cassert`
- `xtc-carrier: wire xtc fiber carrier for threaded B_BACKEND (USE_XTC_CARRIER)`
- `xtc-carrier: SUCCESS - select 1 round-trips through xtc scheduler`
