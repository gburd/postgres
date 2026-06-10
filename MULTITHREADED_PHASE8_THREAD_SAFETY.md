# Phase 8 Thread-Safety Floor Notes

Phase 8 is not complete yet. This note records the implementation slices that
now use the explicit `PG_THREAD_LOCAL` storage qualifier from
`src/include/utils/global_lifetime.h` as a compatibility bridge for
thread-per-session launch.

## Completed Slice

The `PG_GLOBAL_*` annotations remain classification-only. Do not make those
macros expand to TLS. Several annotated variables are GUC backing variables
whose addresses are embedded in static GUC tables, and C does not permit a TLS
variable address in a static initializer.

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
  `GUC_check_errhint_string`.

`ConfigureNames[]` is now classified as an immutable generated template. Each
backend session copies the template into `guc_variables` during GUC
initialization before `guc.c` mutates stack, reset, report, and source state.

`CurrentTransactionState` cannot use a static initializer that points at
`TopTransactionStateData`, because both are thread-local objects. It is
initialized by `InitializeTransactionState()`, called from `main()` immediately
after memory-context initialization so bootstrap, check-only, single-user,
postmaster, and regular backend paths can safely inspect transaction nesting
before `BaseInit()`. `BaseInit()` also calls the same function idempotently for
normal backend startup.

`PostmasterContext` remains runtime-global. The timeout and lock-wait GUC
backing variables in `proc.c` remain classified as session-owned but are not
TLS yet because of the GUC static-initializer constraint. The same constraint
keeps `ExitOnAnyError`, `IgnoreSystemIndexes`, and the core GUC backing
variables in `globals.c` as non-TLS annotated globals for now. These need a GUC
indirection layer before they can become per-session state.

Any dynamically loaded module that references an exported global after it gains
`PG_THREAD_LOCAL` must be rebuilt against the updated headers. Stale modules can
still link but may crash because they use the old non-TLS symbol access pattern.
During validation this affected `test_ext_backend_model.dylib` and
`plpgsql.dylib`; cleaning and rebuilding those modules fixed the crashes.

## Remaining Phase 8 Work

Phase 8 still needs to cover at least:

- GUC backing variables, likely by introducing GUC indirection rather than
  direct TLS globals;
- the rest of the required-floor audit from `MULTITHREADED_PLAN.md`.

Before Phase 8 can be marked complete, Gate C must pass: `check-world`, static
global report checks, extension load tests using the test-only threaded backend
model, and PL/pgSQL process-mode regression tests. Gate C also fails if any
Phase 8 required-floor global remains unsafe and unclassified.

## Validation So Far

Validation for this slice:

- `gmake clean` followed by `gmake -j8`;
- explicit generated-header recovery for `src/backend/utils` and
  `src/backend/nodes`, followed by `gmake -j8`;
- incremental `gmake -j8` after moving transaction-state initialization into
  `main()`;
- focused core GUC regression test: `guc`;
- unsafe test module GUC privilege regression test: `guc_privs`;
- `perl src/tools/global_lifetime/scan_global_lifetimes.pl --baseline
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
