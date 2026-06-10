# Phase 8 Thread-Safety Floor Notes

Phase 8 is not complete yet. This note records the implementation slices that
now use the explicit `PG_THREAD_LOCAL` storage qualifier from
`src/include/utils/global_lifetime.h` as a compatibility bridge for
thread-per-session launch.

## Completed Slice

The `PG_GLOBAL_*` annotations remain classification-only. Do not make those
macros expand to TLS. Backend-local state that needs process-per-session
semantics in the initial threaded runtime uses explicit `PG_THREAD_LOCAL`
storage until it can move behind an owned runtime, backend, session, or
execution object.

The following state now uses explicit `PG_THREAD_LOCAL` storage:

- current runtime carrier pointers: `CurrentPgCarrier`, `CurrentPgBackend`,
  `CurrentPgSession`, `CurrentPgConnection`, and `CurrentPgExecution`;
- memory context globals: `CurrentMemoryContext`, `TopMemoryContext`,
  `ErrorContext`, `CacheMemoryContext`, `MessageContext`,
  `TopTransactionContext`, `CurTransactionContext`, and `PortalContext`;
- resource owner globals: `CurrentResourceOwner`,
  `CurTransactionResourceOwner`, `TopTransactionResourceOwner`, and
  `AuxProcessResourceOwner`;
- `MyProc` and `got_deadlock_timeout`;
- error stack state: `error_context_stack`, `PG_exception_stack`, `errordata`,
  `errordata_stack_depth`, and `recursion_depth`;
- timeout registration and pending-delivery state in `timeout.c`;
- virtual fd and temporary-file owner state in `fd.c`;
- portal manager session state;
- logical apply-worker memory/error context state;
- regexp cache memory context;
- frontend protocol and connection state: `FrontendProtocol`, `MyProcPort`,
  `MyClientSocket`, `MyCancelKey`, `MyCancelKeyLength`, `PqCommMethods`,
  `FeBeWaitSet`, `whereToSendOutput`, `debug_query_string`, and the libpq
  send/receive buffers in `pqcomm.c`;
- interrupt pending flags and holdoff counters, including async notify, sinval
  catchup, config reload/shutdown, parallel query, parallel logical apply,
  slot sync, and repack interrupt flags;
- backend/session identity globals: `MyProcPid`, `MyStartTime`,
  `MyStartTimestamp`, `MyLatch`, `MyPMChildSlot`, `MyProcNumber`,
  `ParallelLeaderProcNumber`, `MyDatabaseId`, `MyDatabaseTableSpace`,
  `MyDatabaseHasLoginEventTriggers`, `DatabasePath`, `MyBackendType`, `Mode`,
  and `OutputFileName`;
- vacuum execution state: `VacuumCostBalance` and `VacuumCostActive`;
- transaction execution state in `xact.c`, including current transaction
  state, subtransaction/command counters, transaction timestamps, parallel
  current-XID state, unreported subtransaction XIDs, transaction abort context,
  transaction flags, logical-streaming system-scan state, and transaction
  sampling state;
- transaction callback lists in `xact.c`, now session-local TLS state;
- snapshot manager execution state in `snapmgr.c`, including current,
  secondary, catalog, historic, registered, active, exported, and first-xact
  snapshots, plus `TransactionXmin`, `RecentXmin`, tuple CID mapping, and
  `FirstSnapshotSet`;
- GUC manager state in `guc.c`: `GUCMemoryContext`, the session-local mutable
  `guc_variables` copy, `guc_hashtab`, `guc_nondef_list`, `guc_stack_list`,
  `guc_report_list`, `reporting_enabled`, and `GUCNestLevel`;
- GUC check-hook error state: `GUC_check_errcode_value`,
  `GUC_check_errmsg_string`, `GUC_check_errdetail_string`, and
  `GUC_check_errhint_string`;
- exported GUC backing variables that are heavily used by session-local code,
  including the timeout and lock-wait GUCs in `proc.c`, startup and resource
  GUCs in `globals.c` and `miscinit.c`, tcop logging/connection GUCs, RLS
  state, and the exported logging/debug GUCs in `guc_tables.c` including
  `check_function_bodies`.

`ConfigureNames[]` is now classified as an immutable generated template. The
generator emits `NULL` backing-variable pointers into that template, and emits
`InitializeGUCVariablePointers()` beside it. Each backend session copies the
template into `guc_variables` during GUC initialization, then calls
`InitializeGUCVariablePointers()` to bind the copied records to the current
thread's backing variables before `guc.c` mutates stack, reset, report, and
source state. This removes the static-initializer blocker for TLS GUC backing
variables.

`CurrentTransactionState` cannot use a static initializer that points at
`TopTransactionStateData`, because both are thread-local objects. It is
initialized by `InitializeTransactionState()`, called from `main()` immediately
after memory-context initialization so bootstrap, check-only, single-user,
postmaster, and regular backend paths can safely inspect transaction nesting
before `BaseInit()`. `BaseInit()` also calls the same function idempotently for
normal backend startup.

`PostmasterContext` remains runtime-global. The GUC static-initializer
constraint is now removed for generated built-in GUC records, but many GUC
backing variables outside the exported first slice still need to be converted
or explicitly classified according to their real owner.

Any dynamically loaded module that references an exported global after it gains
`PG_THREAD_LOCAL` must be rebuilt against the updated headers. Stale modules can
still link but may crash because they use the old non-TLS symbol access pattern.
During validation this affected `test_ext_backend_model.dylib` and
`plpgsql.dylib`; cleaning and rebuilding those modules fixed the crashes.

## Remaining Phase 8 Work

Phase 8 still needs to cover at least:

- remaining GUC backing variables now that the generated runtime rebind layer
  exists;
- the rest of the required-floor audit from `MULTITHREADED_PLAN.md`.

Before Phase 8 can be marked complete, Gate C must pass: `check-world`, static
global report checks, extension load tests using the test-only threaded backend
model, and PL/pgSQL process-mode regression tests. Gate C also fails if any
Phase 8 required-floor global remains unsafe and unclassified.

## Validation So Far

Validation for this slice:

- explicit generated-header recovery for `src/backend/utils` and
  `src/backend/nodes`, followed by `gmake -j8`;
- incremental `gmake -j8` after moving transaction-state initialization into
  `main()`;
- clean `gmake -j8` after making `ConfigureNames[]` an immutable template and
  rebinding GUC backing-variable pointers at runtime;
- focused core GUC regression test: `guc`;
- unsafe test module GUC privilege regression test: `guc_privs`;
- `perl src/tools/global_lifetime/scan_global_lifetimes.pl --baseline
  src/tools/global_lifetime/global_lifetime_baseline.tsv`;
- `perl src/tools/global_lifetime/scan_global_lifetimes.pl --write-baseline
  src/tools/global_lifetime/global_lifetime_baseline.tsv`;
- regenerated `src/tools/global_lifetime/global_lifetime_baseline.tsv` so
  previously classified Phase 8 globals are no longer carried as stale
  unclassified debt;
- filtered static scan for the touched required-floor names;
- `git diff --check`;
- extension backend-model regression tests:
  `test_extensions`, `test_extdepend`, `test_ext_backend_model`, and
  `test_ext_backend_model_pooled`;
- PL/pgSQL process-mode regression tests.

On macOS, the temp install still records `/usr/local/pgsql/lib/libpq.5.dylib`
in frontend binaries. The extension and PL/pgSQL checks above were run after
patching the temp-installed `initdb` or `psql` with `install_name_tool` to point
at `tmp_install/usr/local/pgsql/lib/libpq.5.dylib`; the unpatched failures were
dynamic loader failures before SQL tests ran.
