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
- thread carriers now initialize their own narrow TLS GUC table with
  `InitializeThreadedSessionGUCOptions()` before entering backend or worker
  main code. The initialized set includes the early `InitPostgres()` GUCs and
  the worker-local autovacuum override GUCs exercised by this slice;
- `signal_child()` can route postmaster `SIGINT`, `SIGTERM`, `SIGQUIT`,
  `SIGKILL`, `SIGABRT`, and `SIGHUP` requests to thread-backed children as
  logical backend interrupts instead of assuming every `PMChild` has a PID;
- `AutoVacWorkerMain()` avoids process-global signal handler installation in
  threaded mode, initializes logical timeouts, keeps the postmaster memory
  context owned by the runtime, checks for a valid autovacuum worker entry,
  applies worker-local GUC overrides after `InitPostgres()` when running as a
  thread carrier, and releases the temporary threaded startup gate after
  `InitPostgres()` completes;
- same-process `SendPostmasterSignal()` calls now mark the postmaster PMSignal
  flag and wake the postmaster latch directly instead of sending `SIGUSR1` to
  the containing threaded process;
- the Phase 10 threaded TAP smoke uses a thread-safe test helper module to
  request an autovacuum worker deterministically, so the fixture proves the
  explicit carrier path rather than launcher scheduling heuristics.

A real scheduled autovacuum worker smoke now covers the launcher-created worker
entry and database connection path. Useful vacuum/analyze work should still get
broader coverage once Phase 11 worker families are closer to complete.

## Autovacuum Launcher Thread Slice

The autovacuum launcher is now opted into the thread carrier path in threaded
mode:

- `PgRuntimeShouldThreadBackend()` selects `PG_BACKEND_LAUNCH_THREAD` for
  `B_AUTOVAC_LAUNCHER` when `multithreaded=on`;
- `postmaster_backend_thread_launch()` accepts launcher starts without client
  startup data;
- `AutoVacLauncherMain()` skips process-wide signal handlers and signal-mask
  changes when running as a thread carrier, initializes logical timeouts, and
  preserves the postmaster-owned memory context;
- thread-backed launcher startup releases the temporary threaded startup gate
  after local initialization and before entering the scheduling loop;
- launcher-only defensive `SetConfigOption()` overrides remain process-mode
  only for now, because the launcher does not call `InitPostgres()` and does
  not yet have a private backend GUC option hash in threaded mode;
- `ProcessAutoVacLauncherInterrupts()` drains logical backend interrupts and
  converts the launcher-specific wakeup interrupt into the existing
  `SIGUSR2`/`got_SIGUSR2` path;
- thread-backed launcher config reloads observe the postmaster's shared GUC
  reload result instead of running `ProcessConfigFile()` concurrently in a
  worker thread;
- autovacuum workers notify a threaded launcher through
  `PostmasterSignalAutoVacLauncher()` instead of sending `SIGUSR2` to the
  containing process PID;
- thread-backed launcher exit is reaped by the postmaster thread-exit path,
  clearing `AutoVacLauncherPMChild` and preserving crash escalation for
  abnormal exits.

The threaded runtime TAP smoke now enables autovacuum with a long nap time,
waits for one logical `autovacuum launcher` backend, verifies that no
postmaster child process is titled as an autovacuum launcher, and still runs
the deterministic autovacuum-worker entry test. The deterministic worker is a
no-entry launch-path smoke; stable `pg_stat_activity` accounting across
launcher-created useful workers remains part of the broader real autovacuum
coverage still needed in Phase 11.

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

The thread-backed child signal bridge has been factored so each worker family
can map postmaster signals to logical interrupts in one place. Today that
preserves the existing backend/autovacuum behavior and the IO-worker-specific
`SIGINT`, ignored `SIGTERM`, and `SIGUSR2` shutdown semantics; future worker
families should extend that helper before opting into thread carriers.

The local smoke for this slice starts `multithreaded=on` with
`io_method=worker` and `io_min_workers=1`, then raises `io_min_workers` to 2
after a threaded client backend exists. The server reported one IO worker
before reload and two after reload, while the postmaster OS child-process
count stayed unchanged at six. That proves the second, late IO worker used a
thread carrier instead of a forked subprocess. The same proof has been added
to the threaded runtime TAP test for TAP-enabled environments.

## Generic Background Worker Compatibility Gate

Generic background workers still expose a process-oriented registration ABI and
do not yet have metadata for declaring thread compatibility. Once the
postmaster has created any thread carrier, `StartBackgroundWorker()` now
rejects generic `B_BG_WORKER` launches explicitly in threaded mode instead of
letting them fall through to an unsafe post-thread fork attempt.

Rejected workers are reported as stopped to their shared-memory registration
slot, with the usual notification sent when the requester has a valid
process-backed notification PID. This avoids the previous behavior where a
dynamic background worker could look like a transient fork failure and be
retried by the postmaster. Static background workers may still start as
process-backed workers before the first thread carrier is created; converting
or rejecting those startup-time workers remains part of the later coordinated
worker-family conversion.

The threaded runtime TAP smoke now registers a deliberately unreachable dynamic
background worker after threaded client carriers exist and verifies that the
worker is rejected with an explicit server-log message while the server remains
usable.

## WAL Summarizer Thread Slice

The WAL summarizer is now opted into the thread carrier path in threaded mode:

- `PgRuntimeShouldThreadBackend()` selects `PG_BACKEND_LAUNCH_THREAD` for
  `B_WAL_SUMMARIZER` when `multithreaded=on`;
- `postmaster_backend_thread_launch()` accepts WAL summarizer launches without
  client startup data, matching the other fixed server-owned worker entries;
- `WalSummarizerMain()` skips process-wide signal handler and signal-mask
  changes when running as a thread carrier;
- `ProcessWalSummarizerInterrupts()` first drains logical backend interrupts
  with `PgCurrentBackendApplyInterrupts()`, so postmaster-directed SIGHUP and
  shutdown requests arrive through the backend-runtime bridge;
- thread-backed WAL summarizer SIGHUP observes the postmaster's shared GUC
  reload result instead of calling `ProcessConfigFile()` itself. The
  postmaster owns parsing and applying config files for the shared address
  space; running the parser concurrently in the worker thread crashed during
  the first reload smoke;
- thread-backed WAL summarizer exit is reaped by the postmaster thread-exit
  path, clearing `WalSummarizerPMChild` and preserving the existing crash
  escalation behavior for abnormal exits.

The live smoke for this slice starts a threaded temp cluster with
`summarize_wal=on`, waits until `pg_stat_activity` reports one `walsummarizer`
logical backend, reloads config to exercise the SIGHUP path, then sets
`summarize_wal=off` and reloads again. The summarizer count drops to zero and
the postmaster OS child-process count stays unchanged across the summarizer
exit, proving the summarizer was a thread carrier rather than a forked child.

## WAL Receiver Thread Slice

The WAL receiver is now opted into the thread carrier path in threaded mode:

- `PgRuntimeShouldThreadBackend()` selects `PG_BACKEND_LAUNCH_THREAD` for
  `B_WAL_RECEIVER` when `multithreaded=on`;
- `postmaster_backend_thread_launch()` accepts WAL receiver launches without
  client startup data;
- `WalReceiverMain()` skips process-wide signal handler and signal-mask
  changes when running as a thread carrier;
- `WalRcvData.threaded` records whether the active receiver is thread-backed.
  Startup-process shutdown uses that flag to wake the WAL receiver proc latch
  and let it observe `WALRCV_STOPPING`, instead of sending `SIGTERM` to the
  process PID shared by the postmaster runtime;
- WAL receiver wait loops drain logical backend interrupts with
  `PgCurrentBackendApplyInterrupts()` before `CHECK_FOR_INTERRUPTS()` and also
  exit when shared WAL receiver state reaches `WALRCV_STOPPING`;
- thread-backed WAL receiver config reloads observe the postmaster's shared
  GUC reload result instead of running `ProcessConfigFile()` concurrently;
- `libpqwalreceiver` is marked as compatible with the thread-per-session
  backend model so the in-tree receiver transport can be loaded by the
  thread-backed worker;
- thread-backed WAL receiver exit is reaped by the postmaster thread-exit
  path, clearing `WalReceiverPMChild` while preserving the existing process
  behavior that treats normal exit and FATAL exit as non-crash exits.

The live smoke for this slice starts a process-backed primary and a
threaded-mode standby, waits until hot standby accepts connections, verifies
that `pg_stat_activity` reports one `walreceiver` logical backend, verifies no
postmaster OS child process is titled as a WAL receiver, writes a row on the
primary, waits for standby replay to catch up, and reads the replicated row
from the standby.

## Slot Sync Worker Thread Slice

The logical replication slot sync worker is now opted into the thread carrier
path in threaded mode:

- `PgRuntimeShouldThreadBackend()` selects `PG_BACKEND_LAUNCH_THREAD` for
  `B_SLOTSYNC_WORKER` when `multithreaded=on`;
- `postmaster_backend_thread_launch()` accepts slot sync worker launches
  without client startup data;
- `ReplSlotSyncWorkerMain()` skips process-wide signal handler and signal-mask
  changes when running as a thread carrier and releases the temporary threaded
  startup gate after `InitPostgres()` completes;
- thread-backed slot sync workers record both the containing process PID and
  their logical backend `ProcNumber`. Promotion shutdown wakes the worker's
  PGPROC latch instead of signaling the shared postmaster PID;
- slot sync wait and retry loops drain logical backend interrupts and check the
  shared stop flag so promotion and postmaster-directed shutdown can be
  observed without Unix process signals;
- thread-backed config reloads observe the postmaster's shared GUC reload
  result instead of running `ProcessConfigFile()` concurrently. A
  backend-local config snapshot lets the worker still detect slot-sync
  parameter changes even though the shared GUC storage has already been
  updated by the postmaster;
- `libpqwalreceiver` initialization is idempotent for its own function table,
  which lets a thread-backed WAL receiver and a thread-backed slot sync worker
  share the same in-process walreceiver transport module while still rejecting
  a different walreceiver provider;
- thread-backed slot sync worker exit is reaped by the postmaster thread-exit
  path, clearing `SlotSyncWorkerPMChild` and preserving crash escalation for
  abnormal exits.

The live smoke for this slice starts a process-backed primary and a
threaded-mode standby with `sync_replication_slots=on`, waits until the slot
sync worker starts, verifies that `pg_stat_activity` reports one `slotsync
worker` logical backend, verifies no postmaster OS child process is titled as a
slot sync worker, then changes `hot_standby_feedback` to `off` and reloads
config. The worker logs the expected parameter-change restart message and the
logical slot sync worker count drops to zero. The focused teardown stops the
primary before the standby; stopping the standby while the primary remains
live exposed a separate WAL receiver shutdown wait that should be covered by a
WAL receiver hardening follow-up rather than this slot sync carrier slice.

## Logical Replication Launcher Thread Slice

The static logical replication launcher is now allowed onto the background
worker thread-carrier path in threaded mode:

- `BackgroundWorkerCanUseThreadCarrier()` provides a narrow allowlist for
  audited in-tree `B_BG_WORKER` entrypoints. The first allowlisted entry is
  the static `postgres`/`ApplyLauncherMain` worker;
- `postmaster_child_launch_carrier()` routes allowlisted background workers to
  `postmaster_backend_thread_launch()` while generic and third-party
  background workers remain rejected after thread carriers exist;
- `postmaster_backend_thread_launch()` copies the `BackgroundWorker` startup
  descriptor into the thread-start record and invokes `BackgroundWorkerMain()`
  with that descriptor inside the carrier thread;
- `BackgroundWorkerMain()` skips process-wide signal handler and process-title
  mutation when running as a thread carrier, keeps the postmaster memory
  context owned by the runtime, and releases the temporary threaded startup
  gate after common shared-memory setup and trusted entrypoint lookup;
- `PMChild` now retains a stable visible signal/stat ID for thread-backed
  children so background-worker registration, `pg_stat_activity`, and cleanup
  logs can refer to the logical backend ID even after the carrier exits;
- logical interrupt consumption now arms the legacy `InterruptPending` flag.
  This lets workers that drain the backend-runtime mailbox immediately before
  `CHECK_FOR_INTERRUPTS()` still run the old `ProcessInterrupts()` path for
  shutdown and cancellation;
- `ApplyLauncherMain()` records its logical `ProcNumber` in shared launcher
  state, wakes via the launcher PGPROC latch in threaded mode, avoids
  concurrent config-file parsing on SIGHUP, and drains logical backend
  interrupts around latch waits.

The live smoke for this slice starts a threaded temp cluster with
`wal_level=logical`, waits until `pg_stat_activity` reports one `logical
replication launcher` logical backend with a thread-style logical PID, then
performs a fast shutdown. The first smoke exposed two useful lifecycle gaps:
the background-worker shared slot must use the thread backend's visible ID
rather than the containing process PID, and consumed logical interrupts must
arm legacy interrupt processing before `CHECK_FOR_INTERRUPTS()`.

## Logical Replication Apply And Table-Sync Thread Slice

Logical replication apply workers and table-sync workers are now allowlisted
for thread carriers in threaded mode:

- `BackgroundWorkerCanUseThreadCarrier()` accepts the in-tree
  `postgres`/`ApplyWorkerMain` and `postgres`/`TableSyncWorkerMain`
  entrypoints, in addition to the already audited logical replication
  launcher;
- `LogicalRepWorker` slots now record both the historical `PGPROC *`/OS PID
  view and the SQL-visible logical signal PID used by thread-backed workers;
- `logicalrep_worker_attach()` stores the worker's logical signal PID,
  `ProcNumber`, and carrier model at attach time, so later stop and stats
  paths do not need to infer them from the containing process PID;
- `logicalrep_worker_stop_internal()` preserves the existing `kill()` path for
  process-backed workers and routes stop requests for thread-backed workers
  through `SendBackendInterrupt()`;
- `pg_stat_get_subscription()` and the parallel-apply leader lookup use the
  logical signal PID, keeping `pg_stat_subscription.pid` aligned with
  `pg_stat_activity.pid` in thread mode;
- common apply/table-sync worker startup avoids installing process-wide
  SIGHUP handlers or changing the process signal mask when running as a
  thread carrier;
- apply-worker config reload handling consumes `ConfigReloadPending` in
  thread mode but leaves config-file parsing to the postmaster-owned reload
  path for the shared address space;
- the sequence-sync config reload site has the same guard, and the follow-on
  sequence-sync slice allowlists `SequenceSyncWorkerMain` after validating the
  launch and copy path.

## Logical Replication Sequence-Sync Thread Slice

Logical replication sequence-sync workers are now allowlisted for thread
carriers in threaded mode:

- `BackgroundWorkerCanUseThreadCarrier()` accepts the in-tree
  `postgres`/`SequenceSyncWorkerMain` entrypoint;
- sequence-sync workers already use the shared `LogicalRepWorker` attach
  path, so they publish the same logical signal PID, `ProcNumber`, and
  carrier model as apply/table-sync workers;
- `SetupApplyOrSyncWorker()` covers sequence-sync startup, avoiding
  process-wide SIGHUP handler installation and signal-mask changes when the
  worker runs inside a carrier thread;
- `ProcessSequenceSyncConfigReload()` consumes `ConfigReloadPending` in
  thread mode but leaves config-file parsing to the postmaster-owned reload
  path for the shared address space.

## Logical Replication Parallel Apply Thread Slice

Logical replication parallel apply workers are now allowlisted for thread
carriers in threaded mode:

- `BackgroundWorkerCanUseThreadCarrier()` accepts the in-tree
  `postgres`/`ParallelApplyWorkerMain` entrypoint;
- `SendProcSignal()` can deliver proc-number-targeted same-process
  notifications through the logical backend interrupt mailbox when the target
  slot belongs to a thread-backed backend sharing the postmaster PID;
- same-process `SendProcSignal()` calls that cannot be mapped to a logical
  backend interrupt now fail instead of signaling the containing postmaster
  process;
- parallel apply worker shutdown and `pqmq.c` leader wakeups now pass the
  leader worker's `ProcNumber`, preserving the existing process-mode PID path
  while giving thread-backed workers a logical destination;
- `ParallelApplyWorkerMain()` avoids installing process-wide SIGHUP/SIGUSR2
  handlers or changing the process signal mask when running as a thread
  carrier;
- `ProcessParallelApplyInterrupts()` drains queued logical backend interrupts
  before legacy interrupt checks, and consumes config reload requests in
  threaded mode without running a second shared-address-space
  `ProcessConfigFile()`.

The current same-process `SendProcSignal()` bridge is intentionally limited to
`ProcSignalReason` values that already have `PgBackendInterruptType`
equivalents. Future worker conversions that need additional proc-signal
reasons should add explicit interrupt types rather than falling back to
process signals.

## WAL Writer Thread Slice

The WAL writer is now opted into the thread carrier path in threaded mode:

- `PgRuntimeShouldThreadBackend()` selects `PG_BACKEND_LAUNCH_THREAD` for
  `B_WAL_WRITER` when `multithreaded=on`;
- `postmaster_backend_thread_launch()` accepts WAL writer launches without
  client startup data;
- `WalWriterMain()` skips process-wide signal handler and signal-mask changes
  when running as a thread carrier;
- `ProcessMainLoopInterrupts()` now avoids concurrent `ProcessConfigFile()`
  calls in thread-backed workers, relying on the postmaster's shared GUC
  reload for thread-mode config changes;
- the postmaster maps thread-backed WAL writer `SIGTERM` to a logical
  shutdown request and preserves the process-backed behavior that ignores
  `SIGINT`;
- thread-backed WAL writer exit is reaped by the postmaster thread-exit path,
  clearing `WalWriterPMChild` and preserving crash escalation for abnormal
  exits.

This slice deliberately leaves checkpointer and background writer on their
startup-time process paths. They are launched before the startup process, so
converting them directly to startup-time threads would make the later startup
process fork unsafe. They need either startup/recovery worker conversion first
or an explicit process-to-thread handoff after recovery reaches normal running.

## Archiver Thread Slice

The WAL archiver is now opted into the thread carrier path in threaded mode:

- `PgRuntimeShouldThreadBackend()` selects `PG_BACKEND_LAUNCH_THREAD` for
  `B_ARCHIVER` when `multithreaded=on`;
- `postmaster_backend_thread_launch()` accepts archiver launches without
  client startup data;
- `PgArchiverMain()` skips process-wide signal handler and signal-mask changes
  when running as a thread carrier;
- a logical `PG_BACKEND_INTERRUPT_WAKEUP_STOP` interrupt preserves the
  archiver's `SIGUSR2` semantics: wake, do one final archive cycle, and exit;
- thread-backed archiver `SIGTERM` remains a delayed shutdown request, matching
  the process-backed behavior where random `SIGTERM` should not immediately
  disable archiving;
- `ProcessPgArchInterrupts()` drains logical backend interrupts and avoids
  concurrent `ProcessConfigFile()` calls in thread-backed archivers, relying on
  the postmaster's shared GUC reload;
- the archiver records the loaded `archive_library` value so a thread-backed
  config reload can still restart the archiver if the configured archive module
  changes;
- thread-backed archiver exit is reaped by the postmaster thread-exit path,
  clearing `PgArchPMChild` and preserving existing normal/FATAL/crash handling.

## Remaining Worker Families

- checkpointer, background writer, and syslogger;
- startup/recovery worker paths that are part of normal server operation;
- startup-time AIO method workers;
- a narrow allowlist path for in-tree generic background worker tests and
  examples;
- explicit thread-worker metadata for third-party background workers that can
  eventually opt into threaded mode.

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

Additional validation for the logical replication sequence-sync slice:

- touched-object builds passed for `sequencesync.o`, `launcher.o`, and
  `bgworker.o`;
- full `gmake -j8` passed;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed;
- a direct process-publisher/threaded-subscriber sequence smoke passed:
  `multithreaded=on`, `wal_level=logical`, logical replication launcher/apply
  workers and sequence-sync worker running as thread carriers, sequence value
  copied to the subscriber, no logical replication child process observed, and
  no subscriber log `ERROR`/`FATAL`/`PANIC`.

Additional validation for the logical replication parallel-apply slice:

- touched-object builds passed for `procsignal.o`, `applyparallelworker.o`,
  `launcher.o`, `worker.o`, and `bgworker.o`;
- full `gmake -j8` passed;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed;
- a direct process-publisher/threaded-subscriber parallel streaming smoke
  passed using the upstream `t/015_stream.pl` interleaved transaction shape:
  `multithreaded=on`, `streaming = parallel`,
  `max_parallel_apply_workers_per_subscription = 2`, a logical replication
  parallel apply worker launched as a thread carrier, the subscriber reached
  the expected final row/default counts `3334|3334|3334`, no logical
  replication child process was observed, and the subscriber log had no
  `ERROR`/`FATAL`/`PANIC`.
- a follow-up direct threaded AIO smoke on the committed branch reproduced the
  same late-worker proof and fast shutdown: `io_after=2`,
  `children_before=6`, and `children_after=6`.

Additional validation for the generic background worker compatibility gate:

- touched-object builds passed for `launch_backend.o` and `postmaster.o`;
- `gmake -j8` passed after the postmaster helper/header change;
- `gmake -C src/test/modules/test_backend_runtime all` passed after adding the
  threaded bgworker rejection helper;
- `gmake -C src/test/modules/test_backend_runtime check` passed its SQL
  regression and skipped TAP because this checkout is not configured with
  `--enable-tap-tests`;
- direct `perl -c src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl`
  is blocked in this local Perl by the missing non-core `IPC::Run` module
  before syntax is checked.

Additional validation for the WAL summarizer thread slice:

- touched-object builds passed for `backend_runtime.o`, `launch_backend.o`,
  `postmaster.o`, and `walsummarizer.o`;
- full `gmake -j8` passed after the final reload fix;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed after the reload fix;
- `gmake -C src/test/modules/test_backend_runtime check` passed its SQL
  regression and skipped TAP because this checkout is not configured with
  `--enable-tap-tests`;
- direct threaded temp-cluster smoke passed with `multithreaded=on`,
  `autovacuum=off`, and `summarize_wal=on`. The smoke observed
  `show_summarize=on`, `summarizers_before=1`, and `summarizers_after=0`
  after `ALTER SYSTEM SET summarize_wal = off` plus `pg_reload_conf()`.
  The postmaster OS child count remained `5` before and after the logical
  summarizer exited, and fast shutdown completed cleanly.

Additional validation for the WAL writer thread slice:

- touched-object builds passed for `backend_runtime.o`, `interrupt.o`,
  `launch_backend.o`, `postmaster.o`, and `walwriter.o`;
- full `gmake -j8` passed;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed its SQL
  regression and skipped TAP because this checkout is not configured with
  `--enable-tap-tests`;
- direct threaded temp-cluster smoke passed with `multithreaded=on`,
  `autovacuum=off`, and `summarize_wal=off`. The smoke observed
  `walwriters=1`, `walwriters_after_work=1`, `children_with_walwriter=4`,
  and `children_after_work=4` after `pg_reload_conf()`, a small WAL-writing
  workload, `CHECKPOINT`, and clean fast shutdown.

Additional validation for the WAL receiver thread slice:

- touched-object builds passed for `backend_runtime.o`, `launch_backend.o`,
  `postmaster.o`, `walreceiver.o`, `walreceiverfuncs.o`, and
  `libpqwalreceiver`;
- full `gmake -j8` passed;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed;
- direct threaded primary/standby smoke passed after patching the known macOS
  temp-install `libpq` references. The smoke observed `walreceiver_count=1`,
  `walreceiver_children=0`, `replayed=t`, and `standby_count=1`, then stopped
  both servers cleanly.

Additional validation for the slot sync worker thread slice:

- touched-object builds passed for `slotsync.o` and `libpqwalreceiver.o`;
- full `gmake -j8` passed;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed;
- direct threaded primary/standby smoke passed after patching the known macOS
  temp-install `libpq` references. The smoke observed `slotsync_count=1`,
  `slotsync_children=0`, `reload_seen=yes`, and `slotsync_after_reload=0`.
  The smoke stops the primary before the standby to avoid conflating the slot
  sync proof with the WAL receiver live-primary shutdown follow-up noted
  above.

Additional validation for the logical replication launcher thread slice:

- touched-object builds passed for `bgworker.o`, `launch_backend.o`,
  `postmaster.o`, `pmchild.o`, `backend_runtime.o`, and `launcher.o`;
- full `gmake -j8` passed;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed;
- a direct threaded temp-cluster smoke passed: `multithreaded=on`,
  `wal_level=logical`, `max_logical_replication_workers=4`,
  `autovacuum=off`, `summarize_wal=off`, and `io_method=sync`. The smoke
  verified `pg_stat_activity` contained `logical replication launcher` with a
  logical thread PID, and `pg_ctl -m fast stop` completed without hanging.

Additional validation for the logical replication apply/table-sync thread
slice:

- touched-object builds passed for `bgworker.o`, `launcher.o`, `worker.o`,
  `sequencesync.o`, and `tablesync.o`;
- full `gmake -j8` and `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed;
- after changing the `LogicalRepWorker` shared struct layout, stale logical
  replication objects left from the previous build caused table-sync startup
  failures with `role with OID 119 does not exist`; cleaning
  `src/backend/replication/logical` and rebuilding fixed the stale-layout
  failure and is now recorded in `AGENTS.md`;
- a direct process-publisher/threaded-subscriber smoke passed after patching
  the known macOS temp-install `libpq` references. The subscriber used
  `multithreaded=on`, `wal_level=logical`, `max_logical_replication_workers=8`,
  `max_sync_workers_per_subscription=4`, and
  `max_parallel_apply_workers_per_subscription=0`. The smoke observed both
  `logical replication tablesync worker` and `logical replication apply
  worker`, copied 20,000 rows through initial table sync, replicated one
  later insert for a final count of 20,001, reported
  `pg_stat_subscription` as `mt_sub|apply|5`, found no logical-replication OS
  child processes under the threaded subscriber postmaster, and stopped both
  servers cleanly.

Additional validation for the logical replication sequence-sync thread slice:

- touched-object build passed for `bgworker.o`;
- full `gmake -j8` and `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed;
- a direct process-publisher/threaded-subscriber sequence smoke passed after
  patching the known macOS temp-install `libpq` references. The publisher
  used `CREATE PUBLICATION ... FOR ALL SEQUENCES`; the threaded subscriber
  used `multithreaded=on`, `wal_level=logical`,
  `max_logical_replication_workers=8`,
  `max_sync_workers_per_subscription=4`, and
  `max_parallel_apply_workers_per_subscription=0`. The smoke verified the
  apply worker and sequence-sync worker start messages, the sequence-sync
  worker finish message, 50 `pg_subscription_rel` rows in READY state, synced
  sequence values `1001|true,1050|true`, no logical-replication OS child
  processes under the threaded subscriber postmaster after sync, and clean
  shutdown of both servers. A larger 500-sequence dry run also synced all
  rows, but the initial assertion expected `t`/`f` booleans where SQL string
  concatenation returned `true`/`false`.

Additional validation for the archiver thread slice:

- touched-object builds passed for `backend_runtime.o`, `interrupt.o`,
  `launch_backend.o`, `pgarch.o`, and `postmaster.o`;
- full `gmake -j8` passed after adding the wake/stop interrupt;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed its SQL
  regression and skipped TAP because this checkout is not configured with
  `--enable-tap-tests`;
- direct threaded temp-cluster smoke passed with `multithreaded=on`,
  `archive_mode=on`, shell `archive_command`, `autovacuum=off`, and
  `summarize_wal=off`. The smoke observed `archivers=1`,
  `archivers_after_archive=1`, `archived_files=1`,
  `children_with_archiver=4`, and `children_after_archive=4` after
  `pg_reload_conf()`, a WAL-writing workload, `pg_switch_wal()`, and clean
  fast shutdown.

An attempted TAP fixture that relied on ordinary autovacuum scheduling did not
start a worker reliably within a short poll window, even with aggressive table
thresholds. The live worker proof should use a deterministic trigger or a
dedicated test hook rather than depending on launcher heuristics.
