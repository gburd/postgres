# Phase 11 Auxiliary Worker Thread Runtime Notes

Phase 11 is in progress. The goal is to make normal threaded server mode fully
threaded for in-tree server-owned worker families, so the runtime does not fork
subprocesses for ordinary server operation.

## Autovacuum Worker Thread Slice

The first Phase 11 slice converts late autovacuum workers from the Phase 10
deferral into thread carriers:

- `PgRuntimeShouldThreadBackend()` now selects `PG_BACKEND_LAUNCH_THREAD` for
  `B_AUTOVAC_WORKER` when `multithreaded=on`;
- `postmaster_backend_thread_launch()` accepts `B_AUTOVAC_WORKER` launches
  without client startup data and runs the worker main function inside the
  carrier thread;
- thread runtime state initialization is split from TLS installation, allowing
  the postmaster-owned `PMChild` entry to keep a pointer to the logical
  `PgBackend` for a thread-backed child;
- `signal_child()` can route postmaster `SIGINT`, `SIGTERM`, `SIGQUIT`,
  `SIGKILL`, `SIGABRT`, and `SIGHUP` requests to thread-backed children as
  logical backend interrupts instead of assuming every `PMChild` has a PID;
- `AutoVacWorkerMain()` avoids process-global signal handler installation in
  threaded mode, initializes logical timeouts, keeps the postmaster memory
  context owned by the runtime, checks for a valid autovacuum worker entry
  before touching worker-local GUC state, and releases the temporary threaded
  startup gate after `InitPostgres()` completes;
- same-process `SendPostmasterSignal()` calls now mark the postmaster PMSignal
  flag and wake the postmaster latch directly instead of sending `SIGUSR1` to
  the containing threaded process;
- the Phase 10 threaded TAP smoke now uses a thread-safe test helper module to
  request an autovacuum worker deterministically, with ordinary autovacuum
  scheduling disabled so the fixture proves the explicit carrier path rather
  than launcher heuristics.

This slice deliberately does not convert the autovacuum launcher. In threaded
mode it remains a startup-time process carrier for now, while its late workers
are thread-backed. The remaining Phase 11 work must convert the launcher and
the other in-tree server-owned worker families before normal threaded server
mode can claim to be no-fork for ordinary operation. A real scheduled
autovacuum worker with a valid worker entry is also still pending; the current
deterministic proof covers carrier launch and worker-main entry, not useful
vacuum/analyze work.

## Late AIO Worker Thread Slice

The next Phase 11 slice lets dynamically launched AIO method workers use
thread carriers once the threaded runtime has already started:

- startup-time `B_IO_WORKER` children remain process-backed for now, because
  the postmaster still starts other process-backed worker families during
  startup and must not fork after becoming multithreaded;
- after `postmaster_thread_carriers_started` is true, new
  `StartChildProcess(B_IO_WORKER)` requests are routed to the backend thread
  carrier launcher;
- `AuxiliaryProcessMainCommon()` now preserves `PostmasterContext` for
  thread-backed auxiliary workers instead of deleting memory still owned by
  the postmaster;
- `IoWorkerMain()` skips process-wide signal handler and signal-mask changes
  when running as a thread carrier, relying on logical backend interrupts
  delivered through the postmaster signal bridge;
- the postmaster maps IO-worker `SIGUSR2` to a logical shutdown request,
  preserves the historical ignored `SIGTERM` behavior, and treats `SIGINT`
  as the manual-restart/proc-die path;
- thread-backed IO worker exits are reaped through the existing IO worker
  accounting path so `io_worker_count` and `io_worker_children[]` stay
  consistent.

The local smoke for this slice starts `multithreaded=on` with
`io_method=worker` and `io_min_workers=1`, then raises `io_min_workers` to 2
after a threaded client backend exists. The server reported one IO worker
before reload and two after reload, while the postmaster OS child-process
count stayed unchanged at six. That proves the second, late IO worker used a
thread carrier instead of a forked subprocess. The same proof has been added
to the threaded runtime TAP test for TAP-enabled environments.

## Remaining Worker Families

- autovacuum launcher;
- checkpointer, background writer, WAL writer, archiver, and syslogger;
- startup/recovery worker paths that are part of normal server operation;
- WAL receiver and WAL summarizer;
- startup-time AIO method workers;
- logical replication launcher, apply, table sync, slot sync, sync utility,
  and parallel apply workers;
- a narrow allowlist path for in-tree generic background worker tests and
  examples;
- third-party background worker rejection or explicit thread-worker metadata.

## Validation

Validation run for this slice:

- touched-object builds passed for `backend_runtime.o`, `launch_backend.o`,
  `pmchild.o`, `postmaster.o`, and `autovacuum.o`;
- full `gmake -C src/backend -j8` passed because installed headers and the
  PMChild layout changed;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed its SQL
  regression and skipped TAP in this checkout because it is not configured
  with `--enable-tap-tests`;
- direct syntax check for `t/001_threaded_runtime.pl` passed;
- direct threaded TAP smoke passed with 23 tests after patching the known
  macOS temp-install `libpq` references.

Additional validation for the late AIO worker slice:

- touched-object builds passed for `launch_backend.o`, `postmaster.o`,
  `auxprocess.o`, and `method_worker.o`;
- full `gmake -j8` passed;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed;
- a direct threaded temp-cluster smoke passed: `multithreaded=on`,
  `io_method=worker`, `io_min_workers=1`, `io_max_workers=4`,
  `io_worker_launch_interval=0`, then `ALTER SYSTEM SET io_min_workers = 2`
  and `pg_reload_conf()`. The smoke observed `io_before=1`, `io_after=2`,
  `children_before=6`, and `children_after=6`, then stopped the server
  cleanly with fast shutdown.

An attempted TAP fixture that relied on ordinary autovacuum scheduling did not
start a worker reliably within a short poll window, even with aggressive table
thresholds. The live worker proof should use a deterministic trigger or a
dedicated test hook rather than depending on launcher heuristics.
