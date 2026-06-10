# Phase 8 Thread-Safety Floor Notes

Phase 8 is not complete yet. This note records the first implementation slice:
the branch now has an explicit `PG_THREAD_LOCAL` storage qualifier in
`src/include/utils/global_lifetime.h`, and a set of high-risk backend-local
globals have been moved to thread-local storage as a compatibility bridge for
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
- regexp cache memory context.

`PostmasterContext` remains runtime-global. The timeout and lock-wait GUC
backing variables in `proc.c` remain classified as session-owned but are not
TLS yet because of the GUC static-initializer constraint.

Any dynamically loaded module that references an exported global after it gains
`PG_THREAD_LOCAL` must be rebuilt against the updated headers. Stale modules can
still link but may crash because they use the old non-TLS symbol access pattern.
During validation this affected `test_ext_backend_model.dylib` and
`plpgsql.dylib`; cleaning and rebuilding those modules fixed the crashes.

## Remaining Phase 8 Work

Phase 8 still needs to cover at least:

- `MyProcPort` and frontend protocol buffers;
- interrupt pending flags and interrupt holdoff counters;
- timeout pending flags outside the registration machinery;
- GUC backing variables and GUC nesting state, likely by introducing GUC
  indirection rather than direct TLS globals;
- current transaction and session identity globals;
- the rest of the required-floor audit from `MULTITHREADED_PLAN.md`.

Before Phase 8 can be marked complete, Gate C must pass: `check-world`, static
global report checks, extension load tests using the test-only threaded backend
model, and PL/pgSQL process-mode regression tests. Gate C also fails if any
Phase 8 required-floor global remains unsafe and unclassified.

## Validation So Far

Validation for this slice:

- focused object builds for the touched files;
- `perl src/tools/global_lifetime/scan_global_lifetimes.pl --baseline
  src/tools/global_lifetime/global_lifetime_baseline.tsv`;
- filtered static scan for the touched required-floor names;
- `gmake -C src/backend all` after a clean backend rebuild.
