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
  context owned by the runtime, and releases the temporary threaded startup
  gate after `InitPostgres()` completes;
- the Phase 10 threaded TAP smoke no longer expects the old autovacuum
  deferral log. A deterministic live autovacuum-worker proof remains pending.

This slice deliberately does not convert the autovacuum launcher. In threaded
mode it remains a startup-time process carrier for now, while its late workers
are thread-backed. The remaining Phase 11 work must convert the launcher and
the other in-tree server-owned worker families before normal threaded server
mode can claim to be no-fork for ordinary operation.

## Remaining Worker Families

- autovacuum launcher;
- checkpointer, background writer, WAL writer, archiver, and syslogger;
- startup/recovery worker paths that are part of normal server operation;
- WAL receiver and WAL summarizer;
- AIO method workers;
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
- direct threaded TAP smoke passed with 22 tests after patching the known
  macOS temp-install `libpq` references.

An attempted TAP fixture that relied on ordinary autovacuum scheduling did not
start a worker reliably within a short poll window, even with aggressive table
thresholds. The live worker proof should use a deterministic trigger or a
dedicated test hook rather than depending on launcher heuristics.
