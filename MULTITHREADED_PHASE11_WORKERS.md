# Phase 11 Auxiliary Worker Thread Runtime Notes

Phase 11 is complete for the current thread-per-session worker-runtime stage.
The goal was to make normal threaded server mode fully threaded for in-tree
server-owned worker families, so the runtime does not fork subprocesses for
ordinary server operation.

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
  the worker-local autovacuum override GUCs exercised by this slice. It also
  includes `track_counts`, which autovacuum launcher startup reads before
  entering its steady-state scheduling loop;
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
  only for now. Threaded launcher startup has a narrow private GUC table, but
  full worker-local GUC adoption and override policy are later work;
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

## AIO Worker Thread Slice

The next Phase 11 slice lets AIO method workers use thread carriers in normal
threaded server operation:

- startup-time `B_IO_WORKER` children remain process-backed until the
  postmaster reaches `PM_RUN`, because they can be needed before regular
  backend thread carriers exist;
- once `PM_RUN` has been reached and thread carriers exist, the postmaster
  signals any startup-era process AIO workers with the existing `SIGUSR2`
  shutdown request and lets `maybe_start_io_workers()` refill the minimum
  worker count through the thread-carrier path;
- after `postmaster_thread_carriers_started` is true, new
  `StartChildProcess(B_IO_WORKER)` requests are routed to the backend thread
  carrier launcher;
- `AuxiliaryProcessMainCommon()` now preserves `PostmasterContext` for
  thread-backed auxiliary workers instead of deleting memory still owned by
  the postmaster;
- `IoWorkerMain()` skips process-wide signal handler and signal-mask changes
  when running as a thread carrier, relying on logical backend interrupts
  delivered through the postmaster signal bridge;
- AIO worker thread carriers bypass the temporary serialized backend startup
  gate, because regular backend startup can need worker-backed catalog reads
  before it is ready to release that gate, and AIO workers do not perform
  catalog/session startup of their own;
- the postmaster maps IO-worker `SIGUSR2` to a logical shutdown request,
  preserves the historical ignored `SIGTERM` behavior, and treats `SIGINT`
  as the manual-restart/proc-die path;
- `IoWorkerMain()` drains pending logical backend interrupts before checking
  the normal interrupt flags, so latch wakeups delivered by the postmaster
  signal bridge become visible to the worker loop;
- thread-backed IO workers observe postmaster-owned config reload decisions
  instead of running `ProcessConfigFile()` concurrently in the shared address
  space;
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

The startup-handoff follow-up starts `multithreaded=on` with
`io_method=worker` and `io_min_workers=2`, waits for normal running, and checks
that the two logical IO workers remain present while no postmaster child
process is still titled as an IO worker. That proves startup-time process
workers were signaled out and replaced by thread carriers after `PM_RUN`.

## Generic Background Worker Backend Model Metadata

Generic background workers now carry explicit backend-model metadata in their
`BackgroundWorker` registration. The default zero-initialized value is
`BgWorkerBackendProcess`, so existing third-party workers remain process-only
and are rejected in threaded mode once a thread carrier is required. A worker
must set `bgw_backend_model = BgWorkerBackendThreadPerSession` before the
postmaster may place it on a thread carrier.

`BackgroundWorkerCanUseThreadCarrier()` now checks the registration metadata
instead of maintaining a hard-coded function-name allowlist. The logical
replication launcher, apply worker, table-sync worker, sequence-sync worker,
and parallel apply worker registrations explicitly opt into the thread-per-
session worker model because those entrypoints have already been audited in
earlier Phase 11 slices.

Dynamic background worker metadata is preserved across the shared-memory slot
handoff into the postmaster-owned `RegisteredBgWorker`. That copy is required
because the postmaster makes the carrier decision from its private worker
record after accepting a dynamic registration.

Rejected process-model workers are reported as stopped to their shared-memory
registration slot, with the usual notification sent when the requester has a
valid logical backend signal PID. Notification and termination now route
through helpers that can wake or signal thread-backed requesters and workers by
logical backend id, while preserving the historical Unix signal path for
process-backed children.

The threaded runtime TAP smoke now covers both sides of this contract: a
deliberately unreachable default/process-model dynamic worker is rejected with
an explicit server-log message, and an explicitly opted-in test worker starts
as a background-worker thread carrier, receives a terminate request, reports
clean shutdown to the requester, and leaves the SQL backend usable.

## Parallel Worker Thread Slice

Core parallel query, parallel index build, and parallel vacuum workers use the
generic dynamic background-worker mechanism. They are now opted into the
thread-carrier path in threaded mode:

- `InitializeParallelDSM()` no longer suppresses parallel workers merely
  because `multithreaded=on`;
- `LaunchParallelWorkers()` sets
  `bgw_backend_model = BgWorkerBackendThreadPerSession` for
  `ParallelWorkerMain`, so the postmaster can route the worker through the
  explicit background-worker backend-model gate;
- the dynamic background-worker notification PID now uses
  `PgCurrentBackendSignalPid()`, which lets a thread-backed leader wait for
  worker startup through the logical backend id rather than the containing
  process PID;
- the leader's serialized `FixedParallelState.parallel_leader_pid` remains
  the carrier process PID. That value is still the interlock used by
  `BecomeLockGroupMember()` and by `SendProcSignal()` when paired with the
  leader's `ProcNumber`; the proc-signal bridge maps same-process
  `PROCSIG_PARALLEL_MESSAGE` delivery to a logical backend interrupt.

The live smoke for this slice starts a threaded temp cluster, forces a simple
parallel aggregate with `debug_parallel_query=on`, verifies a `Gather` plan
with workers, verifies the aggregate result, and checks the server log for
`starting background worker thread carrier "parallel worker ..."` entries.
A process-mode control smoke with the same query shape still starts process
parallel workers and returns the expected result.

## test_shm_mq Thread Slice

The in-tree `test_shm_mq` dynamic background-worker module is now opted into
the thread-carrier path in threaded mode:

- the module uses `PG_MODULE_MAGIC_EXT()` with
  `PG_MODULE_MAGIC_BACKEND_MODEL_THREAD_PER_SESSION`, so threaded backends can
  load it through the Phase 7 extension backend-model gate;
- `setup_background_workers()` sets
  `bgw_backend_model = BgWorkerBackendThreadPerSession` for the workers it
  dynamically registers;
- the requester notification id now uses `PgCurrentBackendSignalPid()` rather
  than `MyProcPid`, so a thread-backed SQL backend can wait on worker startup
  through its logical backend identity;
- worker startup uses `BackendSignalPidGetProc()` to wake the registrant,
  preserving the process-mode PID path while resolving logical backend ids in
  threaded mode.

This slice also exposed a threaded extension-loading gap: the per-carrier GUC
initialization bridge did not initialize `extension_control_path`, so
`CREATE EXTENSION test_shm_mq` could dereference a NULL thread-local copy while
searching extension control directories. `InitializeThreadedSessionGUCOptions()`
now initializes `extension_control_path` alongside `dynamic_library_path`.

## worker_spi Thread Slice

The in-tree `worker_spi` background-worker example is now opted into the
thread-carrier path in threaded mode:

- the module uses `PG_MODULE_MAGIC_EXT()` with
  `PG_MODULE_MAGIC_BACKEND_MODEL_THREAD_PER_SESSION`, so threaded backends can
  load it through the Phase 7 extension backend-model gate;
- static preload workers and SQL-launched dynamic workers set
  `bgw_backend_model = BgWorkerBackendThreadPerSession`;
- dynamic worker startup notifications use `PgCurrentBackendSignalPid()` so a
  thread-backed SQL backend can wait through its logical backend identity;
- worker entry avoids process-wide SIGHUP/SIGTERM handler changes when running
  as a thread carrier, relying on the background-worker logical interrupt
  bridge;
- the main wait loop drains `ProcessMainLoopInterrupts()` after latch wakeups,
  so config reload and shutdown requests are handled consistently by process
  workers and thread-backed workers;
- the custom wait-event id cache is backend-local TLS, preserving the old
  per-process storage semantics when multiple `worker_spi` workers share one
  threaded address space.

Direct validation covered both carrier models. A threaded temp-cluster smoke
with `multithreaded=on`, `shared_preload_libraries='worker_spi'`, one static
worker, and one `worker_spi_launch()` dynamic worker observed logical
`worker_spi` and `worker_spi dynamic` backends, thread-carrier start logs for
both workers, no postmaster child process matching `worker_spi`, no log
`ERROR`/`FATAL`/`PANIC`, and clean fast shutdown. A process-mode control smoke
with the same static preload worker observed a postmaster child process for
`worker_spi`, no log `ERROR`/`FATAL`/`PANIC`, and clean fast shutdown.

## pg_stash_advice Thread Slice

The bundled `pg_stash_advice` persistence worker is now opted into the
thread-carrier path in threaded mode. Because `pg_stash_advice` registers an
advisor with `pg_plan_advice`, the dependency module also declares
thread-per-session compatibility:

- `pg_plan_advice` and `pg_stash_advice` use `PG_MODULE_MAGIC_EXT()` with
  `PG_MODULE_MAGIC_BACKEND_MODEL_THREAD_PER_SESSION`;
- obvious `pg_plan_advice` session GUC backing variables are backend/session
  TLS, and the exported planner-generation counter is execution TLS. The
  planner/advisor hook lists remain runtime-global because they are installed
  at module load and then read by all sessions;
- `pg_stash_advice` session-local stash-name state, DSM/DSA/dshash attachment
  pointers, dshash parameter copies, and module memory context are backend TLS;
- the persistence worker sets
  `bgw_backend_model = BgWorkerBackendThreadPerSession`, uses
  `PgCurrentBackendSignalPid()` for dynamic worker startup notification and
  shared worker identity, and avoids process-wide signal handler changes when
  running as a thread carrier;
- the worker loop drains logical interrupts explicitly but does not use
  `ProcessMainLoopInterrupts()`, because `pg_stash_advice` must perform a
  final dump after a shutdown request instead of exiting immediately.

This is an initial worker-runtime slice, not the final Phase 16 contrib GUC
story. The threaded GUC bridge still initializes only a narrow set of core
records, so broader custom-GUC session isolation for contrib modules remains a
later hardening task.

The first threaded smoke found a stale-object failure after the header changed
the DSM attachment pointers to TLS: `stashfuncs.o` still saw the old plain
global declaration and skipped `pgsa_attach()`, while `pgsa_check_lockout()`
used the new TLS `pgsa_state` and crashed. A clean rebuild of both
`pg_stash_advice` and `pg_plan_advice` fixed the mismatch.

Direct validation covered both carrier models. A threaded temp-cluster smoke
with `multithreaded=on`,
`shared_preload_libraries='pg_plan_advice, pg_stash_advice'`,
`pg_stash_advice.persist=true`, and `pg_stash_advice.persist_interval=0`
created a stash, stored one advice entry, observed a logical
`pg_stash_advice worker`, found no postmaster child process matching
`pg_stash_advice`, saw the thread-carrier start log, completed clean fast
shutdown, and verified the final `pg_stash_advice.tsv` dump. A process-mode
control smoke with the same persistence path observed a postmaster child
process for the worker, completed clean fast shutdown, and verified the dump
file. Process-mode SQL regression checks passed for both
`contrib/pg_plan_advice` and `contrib/pg_stash_advice`; the latter skipped TAP
because this checkout is not configured with TAP tests.

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

The static logical replication launcher is now opted into the background
worker thread-carrier path in threaded mode:

- the `postgres`/`ApplyLauncherMain` registration sets
  `bgw_backend_model = BgWorkerBackendThreadPerSession`;
- `postmaster_child_launch_carrier()` routes metadata-opted background
  workers to `postmaster_backend_thread_launch()` while default/process-model
  third-party background workers remain rejected when a thread carrier is
  required;
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

Logical replication apply workers and table-sync workers are now opted into
thread carriers in threaded mode:

- the in-tree `postgres`/`ApplyWorkerMain` and
  `postgres`/`TableSyncWorkerMain` registrations set
  `bgw_backend_model = BgWorkerBackendThreadPerSession`, in addition to the
  already audited logical replication launcher;
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
  sequence-sync slice opts `SequenceSyncWorkerMain` into the explicit
  background-worker backend model after validating the launch and copy path.

## Logical Replication Sequence-Sync Thread Slice

Logical replication sequence-sync workers are now opted into thread carriers
in threaded mode:

- the in-tree `postgres`/`SequenceSyncWorkerMain` registration sets
  `bgw_backend_model = BgWorkerBackendThreadPerSession`;
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

Logical replication parallel apply workers are now opted into thread carriers
in threaded mode:

- the in-tree `postgres`/`ParallelApplyWorkerMain` registration sets
  `bgw_backend_model = BgWorkerBackendThreadPerSession`;
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

## Checkpointer And Background Writer Thread Handoff Slice

The checkpointer and background writer now move to thread carriers after
normal threaded operation begins:

- `PgRuntimeShouldThreadBackend()` selects thread carriers for
  `B_CHECKPOINTER` and `B_BG_WRITER` once they are eligible to relaunch;
- `postmaster_child_launch_carrier()` preserves the startup-time process
  launch for these two workers while no thread carrier has been created, so
  the startup process can still be forked safely for recovery;
- after `PM_RUN` and after another thread carrier exists, the postmaster asks
  the startup-era process checkpointer/background writer to exit normally and
  then relaunches them as thread carriers;
- the checkpointer has a dedicated logical
  `PG_BACKEND_INTERRUPT_CHECKPOINTER_SHUTDOWN_XLOG` interrupt so postmaster
  `SIGINT` still means "write the shutdown checkpoint" for thread-backed
  checkpointers;
- thread-backed checkpointer `SIGUSR2` maps to the existing shutdown request,
  and thread-backed background writer `SIGTERM` maps to the existing shutdown
  request;
- `CheckpointerMain()` and `BackgroundWriterMain()` avoid process-wide signal
  handler installation and signal-mask changes when running as thread
  carriers;
- `ProcessCheckpointerInterrupts()` drains logical backend interrupts and
  avoids a second shared-address-space `ProcessConfigFile()` in thread mode,
  while still updating the checkpointer-owned shared-memory config copies.

This slice was intentionally implemented as a handoff rather than a
startup-time thread launch because, at that point in the phase, the startup
process still forked during recovery. The later startup/recovery thread slice
removed that startup-era process carrier in threaded mode.

## Startup/Recovery Interrupt Boundary Prep

At this point in the phase, startup/recovery was the remaining auxiliary
carrier-conversion family. It was harder than the handoff workers because the
startup process owns in-flight recovery state; in hot standby it can overlap
with client backends, so a simple "ask the process child to exit and relaunch
it as a thread" handoff was not equivalent.

The first preparatory slice makes the startup process targetable through the
logical thread-interrupt boundary before switching its carrier:

- `PG_BACKEND_INTERRUPT_STARTUP_PROMOTE` represents the startup process'
  `SIGUSR2` promotion trigger explicitly instead of overloading a generic wake
  or shutdown bit;
- `ProcessStartupProcInterrupts()` consumes logical backend interrupts and maps
  config reload, startup promotion, shutdown, proc-die, barrier, and
  log-memory-context requests onto the existing startup-local state;
- the postmaster's thread-child signal bridge preserves startup `SIGINT`
  ignore behavior, maps startup `SIGTERM` to the existing shutdown request,
  and maps startup `SIGUSR2` to the startup promotion interrupt.

This preparatory slice did not launch `B_STARTUP` on a thread carrier. The
follow-up startup/recovery thread slice added that carrier conversion.

## Startup/Recovery Thread Slice

Startup/recovery now has an initial thread-carrier slice in threaded mode:

- `PgRuntimeShouldThreadBackend()` selects `PG_BACKEND_LAUNCH_THREAD` for
  `B_STARTUP` when `multithreaded=on`;
- `postmaster_backend_thread_launch()` accepts startup launches without client
  or background-worker startup data;
- `StartupProcessMain()` uses logical timeouts instead of installing a
  process-wide `SIGALRM` handler when running as a thread carrier;
- startup skips process-wide signal handler and signal-mask changes in thread
  mode, relying on the logical interrupt mappings added by the boundary-prep
  slice;
- startup config reload avoids a second shared-address-space
  `ProcessConfigFile()` call in thread mode and still compares the recovery
  connection settings to request a WAL receiver restart when needed;
- startup child cleanup is now shared by process and thread reaping paths, so
  successful recovery, recovery-target shutdown, shutdown-request exits, and
  startup failure/crash escalation update postmaster state through one helper;
- the exit-time macOS `pthread_is_threaded_np()` diagnostic no longer warns in
  explicit threaded mode, where postmaster-owned threads are expected.

This remains an initial carrier slice, but hot-standby recovery, physical
basebackup, and promotion now have direct smoke coverage. Crash/restart
recovery paths still need explicit stress validation before Phase 11's
startup/recovery work should be considered fully hardened.

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

## Syslogger Thread Handoff Slice

The syslogger now moves to a thread carrier after normal threaded operation
begins:

- `PgRuntimeShouldThreadBackend()` selects thread carriers for `B_LOGGER` once
  the logger is eligible to relaunch;
- `postmaster_child_launch_carrier()` preserves the startup-time process
  launch for `B_LOGGER` while no thread carrier has been created, because the
  syslogger is started before the startup process and must not make later
  startup-era `fork()` calls unsafe;
- after `PM_RUN` and after another thread carrier exists, the postmaster asks
  the startup-era process syslogger to exit with `SIGUSR2`. The process
  logger flushes any buffered pipe input and exits normally; the postmaster
  child-exit path then relaunches the logger through the carrier-aware path,
  which selects a thread carrier;
- `SysLoggerMain()` avoids process-wide signal handler installation and
  signal-mask changes when running as a thread carrier, keeps
  `PostmasterContext` owned by the runtime, and does not redirect process
  stdout/stderr to `/dev/null`;
- thread-backed syslogger reload and rotation requests are delivered through
  logical backend interrupts. A dedicated
  `PG_BACKEND_INTERRUPT_LOG_ROTATE` interrupt preserves `SIGUSR1` rotation
  semantics for the thread carrier;
- thread-backed syslogger config reloads observe the postmaster-owned shared
  GUC reload result instead of running `ProcessConfigFile()` concurrently in
  the shared address space;
- when a logger is thread-backed, the postmaster leaves the shared `FILE`
  objects open because the logger thread owns them in the same address space.
  Process-backed loggers keep the historical behavior where the postmaster
  closes its copies after fork or exec;
- thread-backed syslogger exit is reaped by the postmaster thread-exit path,
  clearing `SysLoggerPMChild` and restarting the logger when
  `logging_collector` remains enabled.

This slice is a handoff rather than startup-time thread launch for the same
reason as the checkpointer/background writer slice: recovery startup still
depends on safe startup-era process creation. Startup/recovery conversion can
remove the temporary process logger later.

## Generic ProcSignal Wakeup Follow-up

An attempted online data-checksum launcher/worker conversion exposed a generic
logical ProcSignal gap before the worker itself could be assessed: barrier
delivery to thread-backed auxiliary ProcSignal slots only set latches for
slots below `MaxBackends`. The checkpointer is an auxiliary slot, so a checksum
state barrier could wait indefinitely for the thread-backed checkpointer to
acknowledge it.

`SendBackendInterrupt()`, `SendProcSignal()`, and
`EmitProcSignalBarrier()` now wake every ProcSignal slot that owns a real
`PGPROC` latch, including auxiliary slots and excluding prepared-transaction
dummy slots. `EmitProcSignalBarrier()` also routes same-process threaded slots
through the logical backend interrupt mailbox instead of sending `SIGUSR1` to
the postmaster process.

`WaitForProcSignalBarrier()` drains the current backend's logical interrupt
mailbox while waiting, so a thread-backed backend that emits a barrier can
absorb its own barrier before waiting for all slots to advance. Dynamic
background-worker startup/shutdown waiters also drain logical interrupts before
the legacy interrupt check.

## Online Data-Checksum Worker Thread Slice

The online data-checksum launcher and per-database workers are now opted into
the dynamic background-worker thread-carrier path in threaded mode:

- `StartDataChecksumsWorkerLauncher()` marks the launcher registration as
  `BgWorkerBackendThreadPerSession` and uses `PgCurrentBackendSignalPid()` for
  dynamic-worker startup/exit notification, so a thread-backed SQL backend can
  wait on the launcher through its logical signal ID;
- `ProcessDatabase()` marks each `DataChecksumsWorkerMain` registration as
  `BgWorkerBackendThreadPerSession`, uses the launcher's logical signal ID for
  notification, and records whether the child worker is thread-backed;
- launcher cleanup preserves the process-mode `SIGTERM` path for process
  workers and routes thread-backed worker termination through
  `SendBackendInterrupt(..., PG_BACKEND_INTERRUPT_PROC_DIE, ...)`;
- the launcher and worker entrypoints avoid installing process-wide signal
  handlers when they are running inside thread carriers, relying on the
  background-worker logical interrupt bridge instead;
- `InitBufferManagerAccess()` now initializes the backend-local
  `BackendWritebackContext`. The first threaded checksum smoke exposed this
  generic gap: after the worker flushed a dirty victim buffer,
  `ScheduleBufferTagForWriteback()` dereferenced the thread-local writeback
  context before it had been initialized for that backend thread.

The live threaded smoke starts a `multithreaded=on` cluster with checksums off,
creates a 3,000-row table, calls `pg_enable_data_checksums(0, 1000)`, waits
until `SHOW data_checksums` reports `on`, verifies the table still has 3,000
rows, and checks the log for thread-carrier starts for the checksum launcher
and per-database workers. No postmaster child process matching
`datachecksums` remains after completion.

## REPACK Decoding Worker Thread Slice

The in-core `REPACK (CONCURRENTLY)` decoding worker is now opted into the
dynamic background-worker thread-carrier path in threaded mode:

- `start_repack_decoding_worker()` marks the dynamic worker registration as
  `BgWorkerBackendThreadPerSession`;
- the launcher uses `PgCurrentBackendSignalPid()` for dynamic-worker
  startup/exit notification, so a thread-backed backend can wait for the
  worker through its logical signal ID;
- the shared `backend_pid` remains the leader PGPROC pid interlock used by
  `BecomeLockGroupMember()` and same-process `SendProcSignal()` with
  `backend_proc_number`. That remains correct for thread-backed leaders
  because the ProcSignal table is still keyed by `MyProcPid` plus
  `MyProcNumber`, while the bgworker notification path is keyed by the logical
  signal ID;
- `RepackWorkerMain()` already avoids process-global mutable state by keeping
  worker flags, current WAL segment, DSM ownership, and relation locators in
  backend-local TLS, and `PROCSIG_REPACK_MESSAGE` already has a logical
  backend-interrupt mapping;
- the in-tree `pgrepack` logical decoding output plugin is marked
  `PG_MODULE_MAGIC_BACKEND_MODEL_THREAD_PER_SESSION`. It has no `_PG_init`
  hook, no mutable file-scope state, and keeps per-run state in the logical
  decoding context supplied by the REPACK worker;
- the first threaded REPACK smoke exposed another narrow GUC-bootstrap gap:
  `logical_decoding_work_mem` is a backend-local GUC backing variable with no
  C initializer, so a fresh thread carrier saw zero and crashed while logical
  decoding tried to evict reorder-buffer changes immediately. The threaded
  session GUC bootstrap now initializes `logical_decoding_work_mem` and
  `debug_logical_replication_streaming` before logical-decoding workers can
  enter the reorder buffer.

Validation for this slice included threaded and process-mode temp-cluster
`REPACK (CONCURRENTLY)` smokes over a 5,000-row table. The threaded smoke
verified the worker started as a thread carrier, table contents were preserved,
the server log contained no `ERROR`, `FATAL`, or `PANIC`, and no postmaster
child process matching `REPACK decoding worker` remained after completion.
The process-mode control verified the same command still succeeds with
`multithreaded = off`.

## pg_prewarm Autoprewarm Worker Thread Slice

The bundled `pg_prewarm` extension is now marked compatible with
thread-per-session backends, and its autoprewarm workers can run on thread
carriers in threaded mode:

- the module magic block declares
  `PG_MODULE_MAGIC_BACKEND_MODEL_THREAD_PER_SESSION`;
- the SQL-callable `pg_prewarm(..., 'read')` scratch buffer is backend-local
  TLS instead of mutable file-scope storage shared by all sessions;
- the autoprewarm leader and per-database dynamic workers declare
  `BgWorkerBackendThreadPerSession` and use `PgCurrentBackendSignalPid()` for
  dynamic-worker notifications;
- the leader and per-database worker avoid installing process-wide signal
  handlers when running on thread carriers, relying on the background-worker
  logical interrupt bridge;
- normal postmaster `SIGTERM` delivery to generic thread-backed background
  workers now maps to `PG_BACKEND_INTERRUPT_SHUTDOWN_REQUEST`, matching worker
  main loops that watch `ShutdownRequestPending`;
- the autoprewarm DSM state now records logical backend signal IDs for
  ownership checks. The backend-local `apw_state` pointer is TLS;
- the first threaded autoprewarm smoke exposed a DSM lifetime difference:
  in process mode the per-database worker can detach the block-info DSM
  mapping independently, but in threaded mode detaching unmaps it for the
  leader too. Thread-backed per-database workers now use the leader's
  in-address-space block-info pointer and leave DSM detach ownership with the
  leader.

Validation for this slice used a two-stage temp-cluster smoke. Process mode
created a 5,000-row table, exercised `pg_prewarm(..., 'read')`,
`pg_prewarm(..., 'buffer')`, and `autoprewarm_dump_now()`, then shut down
cleanly. The same cluster was restarted with `multithreaded = on` and
`shared_preload_libraries = 'pg_prewarm'`; the log showed thread-carrier
starts for both `autoprewarm leader` and `autoprewarm worker`,
`autoprewarm successfully prewarmed`, no `ERROR`, `FATAL`, or `PANIC`, and no
postmaster child process matching `autoprewarm`. The smoke then verified the
table still contained the expected 5,000 rows. This first run used bounded
cleanup because plain threaded fast shutdown later proved to be blocked by the
thread-backed logical replication launcher, independent of `pg_prewarm`.

Follow-up shutdown validation showed the hang was caused by the launcher loop
applying logical interrupts but not routing `ShutdownRequestPending` through
the common main-loop interrupt handler. `ApplyLauncherMain()` now uses
`ProcessMainLoopInterrupts()` in its loop and latch wake path. A direct plain
`multithreaded=on` temp-cluster smoke now starts, executes `select 1`, and
completes `pg_ctl -m fast stop` cleanly.

## Remaining Worker Families

No remaining in-tree server-owned worker family currently lacks an initial
thread-carrier path in threaded mode. The remaining Phase 11 work is hardening
and validation, especially crash restart, worker shutdown/restart, and Gate E
coverage.

The in-tree generic background-worker registration audit is complete for the
current tree. A source scan of `RegisterBackgroundWorker()`,
`RegisterDynamicBackgroundWorker()`, and `BgWorkerBackend` under `src/`,
`contrib/`, and `src/test/modules` finds only the audited registrations for
logical replication, core parallel workers, online checksums, in-core
`REPACK (CONCURRENTLY)`, `pg_prewarm`, `pg_stash_advice`, `worker_spi`,
`test_shm_mq`, and `test_backend_runtime`. The one default/process-model
registration in `test_backend_runtime` is intentional negative coverage for
unsafe extension worker loading in threaded mode.

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
- the startup-handoff follow-up initially exposed a bootstrap dependency cycle:
  regular backend authentication was waiting on worker AIO catalog reads while
  replacement AIO threads were waiting behind the temporary backend startup
  gate. Letting AIO worker thread carriers bypass that gate fixed the cycle;
- after that fix, a direct threaded startup-handoff smoke passed:
  `multithreaded=on`, `io_method=worker`, `io_min_workers=2`,
  `io_max_workers=4`, and `io_worker_launch_interval=0`. The smoke observed
  `io_workers=2`, no postmaster child command matching `io worker|ioworker`,
  successful 5,000-row DDL/insert plus `CHECKPOINT`, no log
  `ERROR`/`FATAL`/`PANIC`, and clean fast shutdown;
- a direct process-mode control smoke passed with `io_method=worker` and
  `io_min_workers=2`: the smoke observed `io_workers=2`, two postmaster child
  IO worker processes, successful 1,000-row DDL/insert plus `CHECKPOINT`, no
  log `ERROR`/`FATAL`/`PANIC`, and clean fast shutdown;
- the threaded runtime TAP smoke now starts with two AIO workers to cover
  startup handoff, verifies no startup IO worker remains as a postmaster child
  process, then raises `io_min_workers` to 3 to keep the late-launch proof.
  Direct TAP execution now passes in this checkout when run with
  `PERL5LIB="$HOME/perl5/lib/perl5:$PWD/src/test/perl"` and the usual
  `PG_REGRESS`/temp-install harness environment;
- `gmake -C src/test/modules/test_backend_runtime check` passed its SQL
  regression and skipped TAP because this checkout is not configured with
  `--enable-tap-tests`.

Validation for the startup/recovery interrupt-boundary prep:

- touched-object builds passed for `startup.o` and `postmaster.o`;
- full `gmake -j8` passed;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed;
- `git diff --check` passed;
- the in-tree generic background-worker audit scan used:
  `rg -n "Register(Dynamic)?BackgroundWorker\\(|BgWorkerBackend" contrib src -g '*.[ch]'`;
- a direct threaded temp-cluster smoke passed after patching the known macOS
  temp-install `libpq` references: `multithreaded=on`, startup, `select
  current_setting('multithreaded'), 1` returned `on|1`, fast shutdown
  completed, and the server log contained no `ERROR`, `FATAL`, or `PANIC`.

Validation for the startup/recovery thread slice:

- touched-object builds passed for `startup.o`, `launch_backend.o`,
  `postmaster.o`, and `backend_runtime.o`;
- full `gmake -j8` passed;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed;
- `git diff --check` passed;
- a direct threaded temp-cluster smoke passed after patching the known macOS
  temp-install `libpq` references: `multithreaded=on`, `autovacuum=off`,
  startup, `select current_setting('multithreaded'), 1` returned `on|1`, fast
  shutdown completed, the startup recovery log line used the postmaster PID,
  the obsolete `postmaster became multithreaded` exit warning was absent, and
  the server log contained no `WARNING`, `ERROR`, or `PANIC`;
- a direct process-mode control smoke passed with `autovacuum=off`: startup,
  `select 1`, and fast shutdown completed, the startup recovery log line used
  a separate startup child PID, and the server log contained no `WARNING`,
  `ERROR`, `PANIC`, or `postmaster became multithreaded`.

Additional validation for threaded physical basebackup and hot-standby
promotion exposed and fixed a walsender thread-safety issue:

- `exec_replication_command()` used a function-local static
  `MemoryContext` for replication commands. That storage was per-walsender in
  process mode but shared by all walsender threads in threaded mode. A
  threaded primary serving `pg_basebackup -X stream` could silently exit while
  concurrent physical walsender connections processed `CREATE_REPLICATION_SLOT`
  or `IDENTIFY_SYSTEM`. The context is now a thread-local
  `PG_GLOBAL_BACKEND` walsender variable;
- touched-object build passed for `walsender.o`;
- full `gmake -j8` passed;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed;
- `git diff --check` passed;
- a direct threaded primary/threaded standby no-slot smoke passed after
  patching the known macOS temp-install `libpq` references: `pg_basebackup
  -X stream -R --no-slot --no-sync`, standby start with `hot_standby=on`,
  initial read returned `pg_is_in_recovery()=true` and one row, replay caught
  up to two rows, `pg_ctl promote` completed, a post-promotion insert
  succeeded, and the promoted standby returned `f|3`;
- a direct threaded primary/threaded standby temporary-slot smoke passed with
  the default `pg_basebackup -X stream -R --no-sync` path. The primary and
  standby startup/recovery log lines used the postmaster PIDs, replay caught
  up, promotion completed, post-promotion writes succeeded, and primary plus
  standby logs contained no `WARNING`, `ERROR`, `PANIC`, or
  `postmaster became multithreaded`;
- a direct process-mode primary/process-mode standby temporary-slot control
  smoke passed for the same basebackup, replay, promotion, post-promotion
  write, and clean shutdown sequence, with no log `WARNING`, `ERROR`,
  `PANIC`, or `postmaster became multithreaded`.

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

Additional validation for the checkpointer/background writer handoff slice:

- touched-object builds passed for `bgwriter.o`, `checkpointer.o`,
  `launch_backend.o`, `postmaster.o`, and `backend_runtime.o`;
- full `gmake -j8` passed;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed;
- a direct threaded temp-cluster smoke passed with `multithreaded=on`,
  `autovacuum=off`, `summarize_wal=off`, and `io_method=sync`. The smoke
  observed one logical `background writer`, one logical `checkpointer`, and
  one logical `walwriter` in `pg_stat_activity`, no OS child process for
  checkpointer/background writer after handoff, successful DDL/insert plus
  `CHECKPOINT`, final row count `2000`, no log `ERROR`/`FATAL`/`PANIC`, and
  clean fast shutdown;
- a direct process-mode temp-cluster smoke passed with the same
  checkpointer/background writer visible as OS child processes, successful
  DDL/insert plus `CHECKPOINT`, final row count `100`, no log
  `ERROR`/`FATAL`/`PANIC`, and clean fast shutdown.
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
  passed when run with
  `PERL5LIB="$HOME/perl5/lib/perl5:$PWD/src/test/perl"`.

Additional validation for the explicit background-worker backend-model
metadata slice:

- touched-object builds passed for `bgworker.o`, `postmaster.o`, and the
  `test_backend_runtime` module;
- full `gmake -j8` passed;
- full `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed, followed by
  reinstalling the `test_backend_runtime` module into the temp install;
- a direct threaded temp-cluster smoke passed with `multithreaded=on`,
  `autovacuum=off`, `summarize_wal=off`, and `io_method=sync`. The smoke
  observed `current_setting('multithreaded') = on`, verified that the default
  process-model test background worker was rejected, verified that the
  explicit thread-model test background worker launched and stopped
  successfully, and verified the SQL backend stayed usable with `SELECT 42`;
- the same smoke log included `starting background worker thread carrier` for
  both the logical replication launcher and the explicit test background
  worker;
- `gmake -C src/test/modules/test_backend_runtime check` passed its SQL
  regression and skipped TAP because this checkout is not configured with
  `--enable-tap-tests`.

Additional hardening for generic background-worker shutdown and crash
escalation:

- a direct threaded smoke exposed that the explicit test thread-model
  background worker could start but then hang in
  `WaitForBackgroundWorkerShutdown()`. The postmaster's thread signal bridge
  maps generic background-worker `SIGTERM` to
  `PG_BACKEND_INTERRUPT_SHUTDOWN_REQUEST`, so the test worker now treats
  `ShutdownRequestPending` as a stop request alongside `ProcDiePending`;
- a direct threaded smoke then passed with `multithreaded=on`,
  `autovacuum=off`, `summarize_wal=off`, and `io_method=sync`: the explicit
  test background worker launched as a thread carrier, `TerminateBackgroundWorker()`
  stopped it cleanly, SQL returned `42` afterward, fast shutdown completed,
  and the log contained no `PANIC`, crash signature, or threaded-runtime crash
  escalation message;
- the threaded runtime TAP now also covers background-worker restart without
  runtime escalation: a restartable thread-model test worker exits once with
  code 1, the postmaster restarts it on a thread carrier, the second run stays
  alive until `TerminateBackgroundWorker()`, and the fixture verifies the
  second run appeared in the server log;
- a direct threaded crash-escalation smoke exposed that a nonzero exit from a
  thread-backed background worker entered process-mode crash recovery and could
  wedge in `PM_WAIT_BACKENDS` waiting for in-address-space siblings. Once
  thread carriers have started, `HandleChildCrash()` now treats any child
  crash as a runtime-level failure and exits the postmaster process instead of
  attempting in-place crash recovery in the shared address space;
- the direct crash-escalation smoke now passes: a test thread-model background
  worker exits with code 17, `psql` loses the connection, the postmaster
  process exits, the log contains `terminating threaded server runtime after
  child crash`, and the previous `issuing SIGKILL to recalcitrant children`
  wedge marker is absent. This is now covered by
  `t/002_threaded_bgworker_crash.pl`;
- touched-object builds passed for `postmaster.o` and the
  `test_backend_runtime` module;
- full `gmake -j8` passed;
- full `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed, followed by
  reinstalling the `test_backend_runtime` module into the temp install;
- `gmake -C src/test/modules/test_backend_runtime check` passed its SQL
  regression and skipped TAP because this checkout is not configured with
  `--enable-tap-tests`.

Additional validation for the parallel worker thread slice:

- touched-object build passed for `parallel.o`;
- full `gmake -j8` passed;
- full `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed;
- a direct threaded temp-cluster smoke passed with `multithreaded=on`,
  `max_parallel_workers=8`, `max_parallel_workers_per_gather=4`,
  `autovacuum=off`, `summarize_wal=off`, and `io_method=sync`. The smoke
  forced a parallel aggregate with `debug_parallel_query=on`, observed a
  `Gather` plan with `Workers Planned: 4`, returned the expected
  `sum=200010000` and `count=20000`, and the server log showed parallel
  workers launched as background-worker thread carriers with no
  `ERROR`/`FATAL`/`PANIC`;
- a direct process-mode control smoke passed with `multithreaded=off`,
  `max_parallel_workers=8`, and `max_parallel_workers_per_gather=4`. The
  smoke observed a `Gather` plan with `Workers Planned: 2`, returned the
  expected `sum=50005000` and `count=10000`, and the server log showed
  process-backed parallel workers;
- `gmake -C src/test/modules/test_backend_runtime check` passed its SQL
  regression and skipped TAP because this checkout is not configured with
  `--enable-tap-tests`;
- direct `perl -c src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl`
  passed when run with
  `PERL5LIB="$HOME/perl5/lib/perl5:$PWD/src/test/perl"`.

Additional validation for the `test_shm_mq` thread slice:

- full `gmake -j8` passed;
- full `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed, followed by
  reinstalling the `test_shm_mq` module into the temp install;
- `gmake -C src/test/modules/test_shm_mq check` passed its process-mode SQL
  regression;
- a direct threaded temp-cluster smoke passed with `multithreaded=on`,
  `max_worker_processes=16`, `autovacuum=off`, `summarize_wal=off`, and
  `io_method=sync`. The smoke verified `current_setting('multithreaded') =
  'on'`, created the `test_shm_mq` extension, ran both `test_shm_mq(...)` and
  `test_shm_mq_pipelined(...)` with thread-backed dynamic workers, and
  verified the SQL backend stayed usable with `SELECT 42`;
- the first threaded `CREATE EXTENSION test_shm_mq` attempt crashed in
  `get_extension_control_directories()` because the threaded GUC bridge had
  not initialized `extension_control_path`. Initializing that GUC in
  `InitializeThreadedSessionGUCOptions()` fixed the smoke.

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

Additional validation for the syslogger handoff slice:

- touched-object builds passed for `syslogger.o`, `postmaster.o`,
  `launch_backend.o`, and `backend_runtime.o`;
- full `gmake -j8` passed;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed;
- direct threaded temp-cluster smoke passed with `multithreaded=on`,
  `logging_collector=on`, explicit `pg_rotate_logfile()`, and a warning
  emitted after rotation. The smoke observed `current_setting('multithreaded')
  = 'on'`, no postmaster child process matching the syslogger after handoff,
  the warning marker in `pg_log`, no log `ERROR`/`FATAL`/`PANIC`, and clean
  fast shutdown;
- direct process-mode control smoke passed with `logging_collector=on`,
  explicit `pg_rotate_logfile()`, and a warning emitted after rotation. The
  smoke observed one postmaster syslogger child process, verified the process
  logger still ignores `SIGUSR2`, found the warning marker in `pg_log`, saw no
  log `ERROR`/`FATAL`/`PANIC`, and completed a clean fast shutdown.

Additional validation for the online data-checksum worker thread slice:

- touched-object builds passed for `datachecksum_state.o` and `bufmgr.o`;
- full `gmake -j8` passed;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed;
- direct threaded temp-cluster smoke passed with `multithreaded=on`,
  checksums initially off, `autovacuum=off`, `summarize_wal=off`, and
  `io_method=sync`. The smoke created a 3,000-row table, called
  `pg_enable_data_checksums(0, 1000)`, observed `data_checksums=on`,
  verified the table still had 3,000 rows, saw thread-carrier start logs for
  the checksum launcher and per-database workers, found no datachecksums OS
  child process after completion, saw no log `ERROR`/`FATAL`/`PANIC`, and
  completed a clean fast shutdown;
- direct process-mode control smoke passed with the same checksum workload and
  settings except `multithreaded=off`. The smoke observed
  `data_checksums=on`, verified the table still had 3,000 rows, saw
  process-carrier start logs for the checksum launcher and workers, saw no log
  `ERROR`/`FATAL`/`PANIC`, and completed a clean fast shutdown.

Additional Gate E hardening validation:

- a direct threaded autovacuum-launcher smoke exposed that launcher threads
  were seeing the default false TLS value for `track_counts`, so
  `AutoVacuumingActive()` returned false inside the launcher even though the
  postmaster had correctly decided to start it. The launcher then took the
  emergency one-shot worker path and exited, leaving no stable
  `autovacuum launcher` row in `pg_stat_activity`;
- adding `track_counts` to `InitializeThreadedSessionGUCOptions()` fixed that
  startup divergence. A clean threaded temp-cluster smoke with
  `autovacuum=on`, `autovacuum_naptime='1h'`, `io_method=sync`, and
  `summarize_wal=off` observed one logical `autovacuum launcher` backend and
  no stray autovacuum worker row;
- touched-object build passed for `guc.o`;
- full `gmake -j8` passed;
- full `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed, followed by
  reinstalling `src/test/modules/test_backend_runtime` into the temp install;
- `gmake -C src/test/modules/test_backend_runtime check` passed its
  process-mode SQL regression after recreating `tmp_install`; this checkout is
  still not configured with `--enable-tap-tests`, so the recursive target
  skipped TAP;
- direct `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl`
  passed all 40 tests with the local `PERL5LIB` TAP environment. This covers
  the threaded autovacuum launcher, deterministic autovacuum worker entry,
  startup and late AIO workers, unsafe/process-model background-worker
  rejection, explicit thread-model background-worker startup/shutdown,
  restartable thread-model background-worker relaunch after exit code 1,
  cancellation/termination, PL/pgSQL, representative threaded SQL usability
  checks, and the broader invariant that the configured threaded runtime has
  no postmaster OS child processes after startup handoff or after dynamic
  worker activity.
- direct `prove` over both `test_backend_runtime` threaded TAP files passed
  all 46 tests. The second fixture,
  `t/002_threaded_bgworker_crash.pl`, starts a separate threaded cluster,
  launches a thread-model background worker that exits with code 17, verifies
  the client connection is lost, verifies the postmaster/runtime exits with
  `terminating threaded server runtime after child crash`, and verifies the
  old process-mode crash-recovery wedge marker is absent.
- an attempted broad process-mode `gmake check-world` passed
  `src/test/isolation` with all 129 tests and progressed through early
  `src/test/modules` targets before stopping in
  `src/test/modules/test_extensions` before SQL started. The failure was the
  known macOS temp-install loader issue: recreated `tmp_install` binaries still
  referenced `/usr/local/pgsql/lib/libpq.5.dylib`;
- after patching the recreated temp-install binaries, the reached
  `test_extensions` regression driver passed all four tests:
  `test_extensions`, `test_extdepend`, `test_ext_backend_model`, and
  `test_ext_backend_model_pooled`;
- after patching the build-tree frontend binaries copied into recreated temp
  installs, `gmake -C src/test/modules/test_extensions check` passed normally,
  including recreating `tmp_install` and rerunning the same four SQL tests.
- after the build-tree install-name patch, literal `gmake check-world` passed
  in this checkout. Recursive TAP targets were skipped because the checkout is
  not configured with `--enable-tap-tests`, so direct TAP remains the threaded
  runtime evidence for this phase;
- a fresh direct `prove` over both `test_backend_runtime` threaded TAP files
  after the successful `check-world` passed all 46 tests again.

An attempted TAP fixture that relied on ordinary autovacuum scheduling did not
start a worker reliably within a short poll window, even with aggressive table
thresholds. The live worker proof should use a deterministic trigger or a
dedicated test hook rather than depending on launcher heuristics.
