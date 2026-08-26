# General extension-shmem-under-threading fix (c) — validated + landed (2026-08-26)

PgRuntimeCopyEarlyExtensionModuleState: the postmaster now copies (not moves) the
early-runtime extension-module state into thread_runtime, so any preloaded extension's
runtime-scoped private state (registered under CurrentPgRuntime==NULL during
_PG_init/shmem_startup) reaches carrier sessions -- not just pgss (which also has its
local fix (a)).

Two-review: SHIP-WITH-NITS.  Both risk questions resolved safe (empty-early Ensure
does not error -- guard tests PgRuntimeIsThreadBacked(CurrentPgRuntime); shallow
list_copy + frozen-early = no aliasing/late-registration gap).  Nit F3 (wrong
COW-inherited comment) fixed -- process-fallback is fork+EXEC, rebuilds its own early;
COPY chosen because idempotent + nothing else consumes early.  F6 (pre-existing
rendezvous_hash unlocked-insert race) tracked separately.

A CurrentPgRuntime==NULL assert I added per the nit was OVER-STRICT (the
test_backend_runtime unit tests call InitializePgThreadRuntime from a backend context
with a runtime installed) -- it tripped test_backend_runtime/regress; removed (behavior
was always correct).  My own assert caught its own over-strictness = good.

Validated (chiuso c7i.4xlarge, cassert): process regress 245/245 0-diffs (core
backend_runtime.c byte-for-byte); test_backend_runtime FULL PASS (0 fail); pgss view
under mt=on works (fix a + c belt-and-suspenders), server alive, 0 crashes.
