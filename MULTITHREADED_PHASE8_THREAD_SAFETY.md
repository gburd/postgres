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
- vacuum tuning GUC backing variables in `vacuum.c`: `vacuum_freeze_min_age`,
  `vacuum_freeze_table_age`, `vacuum_multixact_freeze_min_age`,
  `vacuum_multixact_freeze_table_age`, `vacuum_failsafe_age`,
  `vacuum_multixact_failsafe_age`, `vacuum_max_eager_freeze_failure_rate`,
  `track_cost_delay_timing`, and `vacuum_truncate`;
- transaction execution state in `xact.c`, including current transaction
  state, subtransaction/command counters, transaction timestamps, parallel
  current-XID state, unreported subtransaction XIDs, transaction abort context,
  transaction flags, logical-streaming system-scan state, and transaction
  sampling state;
- transaction characteristic GUC backing variables in `xact.c`: the
  session-local `DefaultXact*` defaults and the execution-local current
  `Xact*` isolation, read-only, and deferrable state;
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
  `check_function_bodies`;
- planner, analyze, GEQO, and JIT GUC backing variables, including
  `default_statistics_target`, the `jit_*` cost and feature toggles,
  `enable_geqo`, the GEQO tuning variables, planner cost constants, path
  enablement toggles, parallel planner toggles, partition-pruning toggles,
  collapse limits, `constraint_exclusion`, and the eager/distinct/self-join
  planner toggles;
- exported session-facing GUC backing variables in `guc_tables.c`, including
  `application_name`, `role_string`, `tcp_keepalives_idle`,
  `tcp_keepalives_interval`, `tcp_keepalives_count`, and `tcp_user_timeout`.
  `in_hot_standby_guc` remains deliberately separate because it reflects
  recovery/runtime state rather than per-session user state;
- session SQL-behavior GUC backing variables outside `guc_tables.c`, including
  `Array_nulls`, `backslash_quote`, `bytea_output`, `extra_float_digits`,
  `quote_all_identifiers`, `Transform_null_equals`, `xmlbinary`, and
  `xmloption`. The frontend `fe_utils` `quote_all_identifiers` variable is a
  separate client-side option and remains plain frontend state.

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
- backend clean plus generated-header recovery, followed by clean `gmake -j8`
  after converting installed-header planner/JIT declarations to
  `PG_THREAD_LOCAL`;
- focused core GUC regression test: `guc`;
- fixture-backed planner/JIT regression coverage:
  `test_setup copy copyselect copydml copyencoding insert insert_conflict
  create_function_c create_misc create_operator create_procedure create_table
  create_type create_schema create_index create_index_spgist create_view
  index_including index_including_gist create_aggregate create_function_sql
  create_cast constraints triggers select vacuum sanity_check guc join
  aggregates incremental_sort plancache limit plpgsql copy2 temp domain
  rangefuncs prepare conversion truncate alter_table sequence polymorphism
  rowtypes returning largeobject with xml partition_merge partition_split
  partition_join partition_prune reloptions hash_part indexing
  partition_aggregate partition_info tuplesort explain memoize predicate numa
  eager_aggregate planner_est`;
- fixture-backed transaction regression coverage:
  `test_setup copy copyselect copydml copyencoding insert insert_conflict
  create_function_c create_misc create_operator create_procedure create_table
  create_type create_schema create_index create_index_spgist create_view
  index_including index_including_gist create_aggregate create_function_sql
  create_cast constraints triggers select vacuum sanity_check guc
  transactions`;
- fixture-backed exported session GUC regression coverage:
  `test_setup copy copyselect copydml copyencoding insert insert_conflict
  create_function_c create_misc create_operator create_procedure create_table
  create_type create_schema create_index create_index_spgist create_view
  index_including index_including_gist create_aggregate create_function_sql
  create_cast constraints triggers select vacuum sanity_check guc create_role
  roleattributes`;
- live temp-cluster smoke coverage for `application_name`, `role`,
  `tcp_keepalives_idle`, `tcp_keepalives_interval`, `tcp_keepalives_count`, and
  `tcp_user_timeout`;
- fixture-backed SQL-behavior GUC regression coverage:
  `test_setup boolean char name varchar text float4 float8 strings arrays copy
  copyselect copydml copyencoding insert insert_conflict create_function_c
  create_misc create_operator create_procedure create_table create_type
  create_schema create_index create_index_spgist create_view index_including
  index_including_gist create_aggregate create_function_sql create_cast
  constraints triggers select vacuum sanity_check xml`;
- live temp-cluster smoke coverage for `array_nulls`, `backslash_quote`,
  `bytea_output`, `extra_float_digits`, `quote_all_identifiers`,
  `transform_null_equals`, `xmlbinary`, and `xmloption`;
- fixture-backed vacuum GUC regression coverage:
  `test_setup copy copyselect copydml copyencoding insert insert_conflict
  create_function_c create_misc create_operator create_procedure create_table
  create_type create_schema create_index create_index_spgist create_view
  index_including index_including_gist create_aggregate create_function_sql
  create_cast constraints triggers select vacuum sanity_check guc`;
- live temp-cluster smoke coverage for `vacuum_truncate`,
  `vacuum_freeze_min_age`, `vacuum_freeze_table_age`, `vacuum_failsafe_age`,
  `vacuum_multixact_freeze_min_age`,
  `vacuum_multixact_freeze_table_age`,
  `vacuum_multixact_failsafe_age`,
  `vacuum_max_eager_freeze_failure_rate`, and
  `track_cost_delay_timing`;
- targeted isolation regression coverage:
  `read-only-anomaly read-only-anomaly-2 read-only-anomaly-3
  serializable-parallel-2`;
- unsafe test module GUC privilege regression test: `guc_privs`;
- `perl src/tools/global_lifetime/scan_global_lifetimes.pl --baseline
  src/tools/global_lifetime/global_lifetime_baseline.tsv`;
- `perl src/tools/global_lifetime/scan_global_lifetimes.pl --write-baseline
  src/tools/global_lifetime/global_lifetime_baseline.tsv`;
- regenerated `src/tools/global_lifetime/global_lifetime_baseline.tsv` so
  previously classified Phase 8 globals are no longer carried as stale
  unclassified debt;
- filtered static scan for the touched required-floor names;
- filtered non-TLS extern mismatch search for the planner/JIT/analyze,
  exported session, session SQL-behavior, and vacuum tuning GUC backing
  variables;
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
