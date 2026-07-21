# Cassert findings: test_backend_runtime threaded suite under USE_ASSERT_CHECKING

Investigation of pre-existing cassert assertion failures blocking the threaded
`test_backend_runtime` suite from running green under a `USE_ASSERT_CHECKING`
build. All findings below are pre-existing (not caused by Phase A/B) and
invisible to the release gate (asserts off). Reproduced against a separate
`build-cassert` (`-Dcassert=true -Dxtc=enabled`, libxtc 1.24.0).

## Fixed this pass (four issues, all landed)

### 1. RUNTIME bug (genuine): async LISTEN/NOTIFY DSA double-detach / use-after-free at process exit

- Assert seen: `GetMemoryChunkContext(ptr) == GUCMemoryContext` (guc.c) was the
  task's flagged line, but the real crash that surfaces first is a SIGSEGV in
  `slist_pop_head_node` / `dsm_detach` (dsm.c) on a `dsm_segment` already
  clobbered with the cassert `CLOBBER_FREED_MEMORY` poison (`0x7f...`).
- Trigger: `LISTEN` then backend exit. Reproduced with a bare `LISTEN; UNLISTEN`
  and NO extension loaded at all, in BOTH process and threaded mode — entirely
  independent of the test module.
- Root cause: `proc_exit()` runs `shmem_exit()` first, which calls
  `dsm_backend_shutdown()` — detaching every mapped DSM segment (including the
  `dsa_pin_mapping`'d LISTEN/NOTIFY global-channel DSA's segment) and freeing
  the `dsm_segment` struct. `proc_exit()` then continues to
  `PgBackendResetClosedState` -> `PgBackendResetUtilityClosedState`, which
  called `dsa_detach(async_global_channel_dsa)` a SECOND time on the freed
  segment. Release survives because freed memory isn't poisoned; cassert
  crashes on the `0x7f` pattern.
- Why release is unaffected: uncontended/stale-memory paths never trip; the
  release `test_backend_runtime` gate and process regress stay green.
- Fix (`src/backend/utils/init/backend_runtime_teardown.c`): guard the explicit
  `dshash_detach`/`dsa_detach` with `if (!PgBackendExitInProgress())`. On the
  process/thread-exit path (`proc_exit_inprogress == true`, the only current
  non-test caller, via `ipc.c:366`) `dsm_backend_shutdown()` already owns the
  teardown, so just drop the runtime pointers. This matches upstream, which
  never `dsa_detach()`es the pinned global-channel DSA at exit. The guarded
  branch remains correct for the future logical-session-reset path (no
  `proc_exit`, segment still live), which is the reason the explicit detach
  exists.
- Verified: `dsm_pin_mapping` only clears `seg->resowner`; it leaves the segment
  on the backend `dsm_segment_list`, so `dsm_backend_shutdown` reclaims it — no
  leak from skipping the redundant detach. LISTEN/NOTIFY clean over 25
  connect/disconnect cycles in both process and threaded mode; no cores.

### 2. `check_GUC_init` mismatch in the pg_trgm session-state port (guc.c assert)

- Assert: `check_GUC_init(variable)` (guc.c ~6512), tripped by
  `pg_trgm`'s `_PG_init` on `CREATE EXTENSION pg_trgm`. Reproduces in BOTH
  process and threaded mode under cassert (not threaded-specific).
- Root cause: the xtc port moved pg_trgm's GUC-backing C-vars into a
  `PgTrgmSessionState` struct and initialized them with double literals
  (`0.3`, `0.6`, `0.5`), while `_PG_init` passes float boot_vals
  (`0.3f`, `0.6f`, `0.5f`). `check_GUC_init` does an EXACT float comparison and
  `(double) 0.3 != (double) 0.3f`, so the assert fires. Both print as "0.3".
- Fix (`contrib/pg_trgm/trgm_op.c`): initialize the session-state fields with
  `0.3f`/`0.6f`/`0.5f` to match the boot_val bit-for-bit (as upstream's C-global
  initializer does).
- No runtime behavior change: the effective value is governed by the boot_val
  (already `0.3f`) once `InitializeOneGUCOption` runs; and `show_limit`/
  `set_limit` return `float4`, so `0.3` and `0.3f` are identical to users.
- Scope check: pg_trgm is the only contrib extension with this double-vs-float
  custom-real-GUC mismatch (auto_explain's real boot_val is `0.0`, exact).

### 3/4. TEST-setup gaps in the test_backend_runtime module (assert-legit, test-only)

- `test_backend_runtime_dsm.c` (`test_backend_dsm_shutdown_is_backend_local`):
  installed zeroed fake backends then called `dsm_create` ->
  `LWLockAcquire(DynamicSharedMemoryControlLock)`, tripping
  `Assert(!(MyProc == NULL && IsUnderPostmaster))` (lwlock.c ~1214) because the
  fake backend had `my_proc == NULL`. Release survives: the immediate
  uncontended lock acquisition never dereferences `proc`. Fix: set the fake
  backends' `my_proc = MyProc` (the real process PGPROC); the test isolates DSM
  mapping ownership, not proc identity, so this preserves coverage.
- `test_backend_runtime_carrier.c` (`test_carrier_protocol_park_prepare_commit`):
  called `ResetLatch(&fake_latch)` while `CurrentPgBackend == NULL`, tripping
  `Assert(latch->owner_pid == MyProcPid)` (latch.c ~522) because `MyProcPid`
  resolves through the NULL-backend fallback core state (`early_backend_core`,
  pid 0), not the fake latch's owner (the real pid recorded by `InitLatch`).
  The latch is only used to observe `PgBackendRaiseInterrupt` setting `is_set`;
  fix clears `is_set` directly. Coverage of the tested behavior is preserved.

These two are TEST bugs: the fake-state harness didn't satisfy cassert
invariants that legitimately hold for any real backend under the postmaster
(a backend has a PGPROC; a latch is reset by its owner in the owner's process).
The asserts correctly caught the missing setup, not a runtime defect.

## NOT fixed this pass — LOUD pre-existing finding (out of scope, needs its own task)

### Broader cassert process-core-regression breakage: plan-cache teardown assert

- Assert: `dlist_is_empty(&session->plan_cache.saved_plan_list)`
  (`src/backend/utils/init/backend_runtime_teardown.c`, in
  `PgSessionResetPlanCacheClosedState`), fired at backend exit.
- Scope: a FULL `meson test --suite regress` (the 245 core process-mode
  regression tests) under cassert crashes with ~15-18 cores on this single
  assert. Confirmed PRE-EXISTING and independent of this pass: reproduced on a
  clean, stashed (unmodified) tree. This is much broader than the
  test_backend_runtime suite and is NOT required for the threaded
  test_backend_runtime gate (which is green with the four fixes above).
- Likely nature: a session exits with cached plans still linked on
  `saved_plan_list` — a plan-cache/session teardown-ordering or
  unregister-on-exit gap in the runtime port. It fires in process mode too, so
  it is not threaded-specific. It needs its own investigation: identify who
  must drain/unlink `saved_plan_list` before
  `PgSessionResetPlanCacheClosedState`, and whether the invariant or the
  teardown ordering is wrong. Do NOT weaken the assert without that analysis.
- Impact: blocks a full `check`/`check-threaded` under cassert, but does NOT
  block the scoped threaded `test_backend_runtime` suite under cassert.

## Validation after the four fixes

- Release process core regress: 245/245, 0 fail, no cores.
- Release `test_backend_runtime` suite: 12 meson entries OK / 0 fail / 2 SKIP,
  no cores.
- Cassert `test_backend_runtime` suite: 12 OK / 0 fail / 2 SKIP, no cores
  (001_threaded_runtime 128 subtests pass; the pg_trgm CREATE EXTENSION path is
  exercised there).
- LISTEN/NOTIFY clean under cassert in process and threaded mode.
- Global-lifetime baseline check clean (no new unclassified globals).
- Runtime-lifecycle checker has a pre-existing unrelated failure
  (`contrib/spi/refint.c does not exist` in the owner map); none of the four
  touched files are in that checker's source or owner-map scope.
