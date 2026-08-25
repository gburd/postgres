# Multithreaded PostgreSQL GUC and Extension-Hook Audit Report

## Executive Summary

This fork's GUC subsystem has partially migrated to per-backend state, but custom GUCs from extensions still use process-global `valueAddr` pointers that are shared across all sessions on all carriers. Extension hook pointers (function pointers set by extensions) are process-global and unserialized. Both represent Phase 16 open items under "finish custom/extension GUC ownership and hook semantics for threaded mode."

**Critical Finding**: Under `multithreaded=on` with two sessions on the same carrier:
- Setting a custom GUC in Session A may corrupt Session B's view if both point to the same extension-owned C variable (`valueAddr`).
- Extension hooks like `ExecutorStart_hook` are shared—if extension A installs a hook and later unloads while Session B is executing, Session B may call a freed/invalid function pointer.

---

## Part 1: Core GUC Storage Architecture

### Built-in GUCs: Per-Backend Safe

Built-in GUC values are **per-backend-safe** under threading. The architecture:

1. **Backing storage**: Each session has its own copy of the GUC variable table:
   - `PgCurrentGUCVariablesRef()` → thread-local via `PgCurrentSessionGUCState()->variables` (per-session array)
   - `PgCurrentGUCVariableStatesRef()` → per-session state array
   - Example: `*PgCurrentMaxStackDepthRef()` returns a pointer to the current session's copy

2. **Accessor pattern** (src/backend/utils/misc/backend_runtime_guc.c):
   ```c
   int *PgCurrentMaxStackDepthRef(void)
   {
       return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(
           CurrentPgSessionMiscGUCRuntimeState, 
           PgCurrentSessionMiscGUCState)->max_stack_depth_kb;
   }
   ```
   Each accessor reaches into the **current session's** runtime state, never a process-global.

3. **State macro system** (src/backend/utils/misc/guc.c):
   ```c
   #define GUC_VARIABLE_BOOL(record)   (GUC_STATE(record)->variable.boolvar)
   #define GUC_STATE(record)           (GUCRecordState(record))
   ```
   `GUCRecordState()` returns `record->state` for custom GUCs (where `state != NULL`), or for built-ins it indexes into the current session's array at `guc_variable_states[index]`.

4. **Threaded lock coordination** (guc.c lines 87–149):
   - `ThreadedGUCLock()`: Uses a fiber-aware `xtc_amutex` on carriers to serialize GUC startup/SET/RESET across fibers.
   - **SEAM**: Before xtc_amutex_lock() (which may yield the fiber), saves the six `CurrentPg*` roots on the stack. After lock acquire, restores them so the woken fiber's carrier thread sees this fiber's actual state, not the last fiber that ran there (line 120–135).
   - Prevents cross-session corruption during GUC mutations in multithreaded mode.

### Verdict

**Core GUC storage is per-backend-safe.** The fork correctly moved built-in GUC values into per-session buckets and provided accessor functions that reach into the current session's runtime object. The threaded lock + seam protects against carrier thread hijacking during that operation.

---

## Part 2: Custom GUC valueAddr Sharing Problem

### The Hazard

When an extension calls `DefineCustomIntVariable(..., valueAddr, ...)` (guc.c line 6819), the extension passes a **pointer to an extension-owned C global**:

```c
// In extension code, somewhere:
static int my_param = 0;  // extension global

// In extension _PG_init():
DefineCustomIntVariable("myext.param", ..., &my_param, ...);
```

What happens in the GUC subsystem:

1. **Registration** (guc.c lines 6819–6835):
   ```c
   var = init_custom_variable(name, ..., PGC_INT);
   GUC_VARIABLE_INT(var) = valueAddr;  // valueAddr = &my_param
   define_custom_variable(var);
   ```
   The `config_generic_state` for this custom GUC stores:
   ```c
   state->variable.intvar = &my_param  // PROCESS-GLOBAL pointer
   ```

2. **Per-session GUC table in threaded mode** (backend_runtime.c):
   When a threaded session initializes, it builds a private copy of the GUC registry and assigns per-session state for **built-in** GUCs. But custom GUCs:
   ```c
   // From GUCRecordState() in guc.c line 565:
   if (GUCRecordIsCurrentSessionBuiltin(record))
   {
       // Built-in: use current session's state array
       return &guc_variable_states[index];
   }
   Assert(record->state != NULL);
   return record->state;  // Custom: returns the SHARED state from registry
   ```

3. **The corruption scenario**: Sessions A and B both read/write through `valueAddr`:
   - Session A: `*PgCurrentIntVariableRef(custom_guc)` → looks up `GUC_STATE(custom_guc)->variable.intvar` → returns `&my_param` (process global)
   - Session B: `*PgCurrentIntVariableRef(custom_guc)` → same lookup → **same `&my_param`**
   - Session A does `SET myext.param = 10` → writes to `my_param`
   - Session B does `SET myext.param = 20` → **overwrites the same `my_param` that Session A is reading**

### Where It Breaks

**File**: `src/backend/utils/misc/guc.c`
- **Line 551–576**: `GUCRecordVariableIsCurrentSessionOwned()` checks if a GUC's variable pointer is owned by the current session. For custom GUCs with extension-owned `valueAddr`, this returns **false** (the address is not in session memory).
- **Line 5594**: Comment acknowledges the issue: "Ordinary SET processing must still assign custom and extension GUCs."
- **Line 5607–5608**: For STRING GUCs, there's a guard: `PgCurrentOrEarlySessionOwnsPointer(GUC_VARIABLE_STRING(record))` allows assignment IF the string is session-owned OR during GUCThreadedBackendReplayActive. **For non-string custom GUCs (BOOL, INT, REAL, ENUM), NO such guard exists.**

### Specific Breakage Locations

1. **guc.c line 3063** (PGC_BOOL in InitializeOneGUCOption):
   ```c
   *GUC_VARIABLE_BOOL(gconf) = GUC_RESET_BOOL(gconf) = newval;
   ```
   If a bool custom GUC is initialized, it writes directly through the extension's pointer—no check for session ownership, no per-session copy. Two sessions race on the same write.

2. **guc.c line 3079** (PGC_INT):
   ```c
   *GUC_VARIABLE_INT(gconf) = GUC_RESET_INT(gconf) = newval;
   ```
   Same problem for int custom GUCs.

3. **guc.c line 3095** (PGC_REAL):
   ```c
   *GUC_VARIABLE_REAL(gconf) = GUC_RESET_REAL(gconf) = newval;
   ```
   Same for real custom GUCs.

4. **guc.c line 3130** (PGC_ENUM):
   ```c
   *GUC_VARIABLE_ENUM(gconf) = GUC_RESET_ENUM(gconf) = newval;
   ```
   Same for enum custom GUCs.

5. **guc.c lines 5730–5738** (set_config_with_handle_internal, PGC_BOOL during SET):
   ```c
   if (changeVal)
   {
       if (!makeDefault)
           push_old_value(record, action);
       if (conf->assign_hook)
           conf->assign_hook(newval, newextra);
       *GUC_VARIABLE_BOOL(record) = newval;  // Unguarded write through extension pointer
       ...
   }
   ```

### Example Crash Scenario (Hypothetical Test)

```
Thread 1 (Session A):
  SET myext.param = 10
    → ThreadedGUCLock() acquires amutex
    → *GUC_VARIABLE_INT(custom_guc) = 10
    → writes to extension's static int my_param
    → ThreadedGUCUnlock()

Thread 2 (Session B):
  SET myext.param = 20
    → ThreadedGUCLock() acquires amutex  
    → *GUC_VARIABLE_INT(custom_guc) = 20
    → writes to **THE SAME** my_param
    → ThreadedGUCUnlock()
  
  Now Session A reads: SELECT current_setting('myext.param')
    → Sees 20 (set by Session B), not its own value 10
    → Corruption: session-local GUC isolation broken
```

### Evidence Locations

| File:Line | Code | Issue |
|-----------|------|-------|
| guc.c:6809 | `GUC_VARIABLE_BOOL(var) = valueAddr;` | Extension pointer stored directly in state |
| guc.c:6835 | `GUC_VARIABLE_INT(var) = valueAddr;` | Same for int |
| guc.c:3063 | `*GUC_VARIABLE_BOOL(gconf) = ... = newval;` | Unguarded write (non-string BOOL) |
| guc.c:3079 | `*GUC_VARIABLE_INT(gconf) = ... = newval;` | Unguarded write (non-string INT) |
| guc.c:3095 | `*GUC_VARIABLE_REAL(gconf) = ... = newval;` | Unguarded write (non-string REAL) |
| guc.c:3130 | `*GUC_VARIABLE_ENUM(gconf) = ... = newval;` | Unguarded write (non-string ENUM) |
| guc.c:551–576 | `GUCRecordVariableIsCurrentSessionOwned()` | No check for custom-GUC session ownership in non-string case |

---

## Part 3: Extension Hook-Pointer Globals

### Hook Pointer Inventory

Searched: `grep -r "PG_GLOBAL_RUNTIME.*_hook" src/backend --include="*.c"`

**All are process-global function pointers**, uninitialized to NULL or a default implementation.

| Hook Name | File:Line | Kind | Mutated? | Thread-Safe? |
|-----------|-----------|------|----------|--------------|
| `ExecutorStart_hook` | executor/execMain.c:70 | Custom plugin | Yes (set-once at load) | NO (shared, unguarded) |
| `ExecutorRun_hook` | executor/execMain.c:71 | Custom plugin | Yes (set-once) | NO |
| `ExecutorFinish_hook` | executor/execMain.c:72 | Custom plugin | Yes (set-once) | NO |
| `ExecutorEnd_hook` | executor/execMain.c:73 | Custom plugin | Yes (set-once) | NO |
| `ExecutorCheckPerms_hook` | executor/execMain.c:76 | Custom plugin | Yes (set-once) | NO |
| `ProcessUtility_hook` | tcop/utility.c:77 | Custom plugin | Yes (set-once) | NO |
| `planner_hook` | optimizer/plan/planner.c:38 | Custom plugin | Yes (set-once) | NO |
| `planner_setup_hook` | optimizer/plan/planner.c:39 | Custom plugin | Yes (set-once) | NO |
| `planner_shutdown_hook` | optimizer/plan/planner.c:40 | Custom plugin | Yes (set-once) | NO |
| `create_upper_paths_hook` | optimizer/plan/planner.c:41 | Custom plugin | Yes (set-once) | NO |
| `post_parse_analyze_hook` | parser/analyze.c:27 | Custom plugin | Yes (set-once) | NO |
| `set_rel_pathlist_hook` | optimizer/path/allpaths.c:39 | Custom plugin | Yes (set-once) | NO |
| `join_search_hook` | optimizer/path/allpaths.c:40 | Custom plugin | Yes (set-once) | NO |
| `set_join_pathlist_hook` | optimizer/path/joinpath.c:37 | Custom plugin | Yes (set-once) | NO |
| `join_path_setup_hook` | optimizer/path/joinpath.c:38 | Custom plugin | Yes (set-once) | NO |
| `emit_log_hook` | utils/error/elog.c:31 | Custom plugin | Yes (set-once) | NO |
| `object_access_hook` | catalog/objectaccess.c:30 | Custom plugin | Yes (set-once) | NO |
| `check_password_hook` | commands/user.c:39 | Custom plugin | Yes (set-once) | NO |
| `shmem_startup_hook` | storage/ipc/ipci.c:43 | Custom plugin | Yes (set-once) | NO |
| `shmem_request_hook` | utils/init/miscinit.c:72 | Custom plugin | Yes (set-once) | NO |
| `needs_fmgr_hook` | utils/fmgr/fmgr.c:38 | Custom plugin | Yes (set-once) | NO |
| `fmgr_hook` | utils/fmgr/fmgr.c:39 | Custom plugin | Yes (set-once) | NO |
| `get_attavgwidth_hook` | utils/cache/lsyscache.c:34 | Custom plugin | Yes (set-once) | NO |
| `get_relation_stats_hook` | utils/adt/selfuncs.c:43 | Custom plugin | Yes (set-once) | NO |
| `get_index_stats_hook` | utils/adt/selfuncs.c:44 | Custom plugin | Yes (set-once) | NO |

**Total: ~25 hook pointers**

### Hook Safety Analysis

**Set-Once, Module-Lifetime Hooks** (lowest risk):
- Hooks loaded in `shared_preload_libraries` (postmaster startup, single-threaded).
- Set exactly once at `_PG_init()`, never changed until postmaster exit.
- Example: extension loads → hook = my_executor_start → runs on all queries
- **Risk**: If extension is unloaded mid-session, hook still points at freed code. **BUT** in normal operation, extensions don't unload; only at server restart.
- **Current status**: NOT threaded-safe-checked, but acceptable if extensions are stable/preloaded.

**Session-Local Hook Manipulation** (hypothetical, high risk):
- If a threaded session could install its own hook for that session only → would need serialization.
- Currently no such feature exists.
- **Risk**: LOW for now (not implemented), but the pattern is not prevented.

### Verdict

All extension hooks are **process-global function pointers with implicit coupling to extension load/unload**. Under `multithreaded=on`:
- **Safe**: Hooks loaded at postmaster startup (before threading) and never unloaded.
- **Hazard**: If an extension is loaded/unloaded mid-session or if per-session hooks are ever implemented without proper locking, two sessions could race on the hook pointer and call a freed function.
- **Not guarded**: No per-session hook table, no mutex around hook pointer mutations, no carrier-aware check.

---

## Part 4: GUC Assign/Check/Show Hooks

### Hook Types

1. **Assign hooks** (e.g., `GucBoolAssignHook`): Called after value is validated; applies side effects.
2. **Check hooks** (e.g., `GucBoolCheckHook`): Validates proposed value; can reject or canonicalize.
3. **Show hooks** (e.g., `GucShowHook`): Returns display string for SHOW command.

### Thread-Safety Assumptions in Hooks

**Current assumption in guc.c** (line 5732):
```c
if (conf->assign_hook)
    conf->assign_hook(newval, newextra);
*GUC_VARIABLE_BOOL(record) = newval;
```

The hook is called **inside the ThreadedGUCLock()** (lines 4954–4971), so:
- Concurrent hook calls from different sessions are **serialized** by the amutex.
- BUT: The hook is an extension-provided function. If the hook reads/writes process-global state (beyond its assigned GUC), it can still race with unrelated code.

### Risk: Hooks with Side Effects

Example (hypothetical):
```c
void my_assign_hook(bool newval, void *extra)
{
    // Update extension's global state:
    extension_flag = newval;
    if (newval)
        background_worker_start();  // Race? Already running?
}
```

Two sessions SET the same GUC to different values:
- Session A: lock acquired, hook called → background_worker_start()
- Session B: lock acquired, hook called → background_worker_start() again (race condition in the extension, not GUC's fault, but GUC doesn't prevent it).

**Verdict**: Hooks themselves are not unsafe in the GUC lock, but **hooks must be carefully designed to avoid process-global races**. Not a GUC architecture problem; an extension contract issue.

---

## Part 5: Ranked Worklist for Phase 16 Completion

### Priority 1: Custom GUC valueAddr Per-Backend Binding (CRITICAL)

**Why**: Cross-session corruption of custom GUC values is a correctness blocker.

**Work**:
1. Detect custom GUCs at session startup: walk GUC registry, identify entries where `state != NULL` (custom GUC marker).
2. For each custom GUC with non-session-owned `valueAddr`:
   - Allocate per-session shadow storage (bool, int, real, char*, int array).
   - Copy initial value from extension's `valueAddr`.
   - Repoint `state->variable` to shadow storage.
   - On RESET, copy back from `reset_val` (stored in session state, not extension global).
3. Add guards in InitializeOneGUCOption and set_config_with_handle_internal for non-string types, symmetric to STRING case (guc.c line 5594).

**Files to modify**:
- `src/backend/utils/misc/guc.c`: InitializeOneGUCOption (lines 3063, 3079, 3095, 3130), set_config_with_handle_internal (line 5732 etc.)
- `src/backend/utils/init/backend_runtime_guc.c` or `src/backend/utils/init/backend_runtime.c`: Add session-local custom-GUC shadow storage allocation/reset.
- `src/include/utils/guc_tables.h`: Extend `config_generic_state` or add a custom-GUC companion structure.

**Evidence**:
- Line 6809–6913 (DefineCustom functions store unguarded extension pointers)
- Line 3063, 3079, 3095, 3130 (unguarded writes)
- Line 5594 (acknowledgment of the issue for STRING only)

---

### Priority 2: Custom GUC Registry Per-Session Isolation

**Why**: Extensions' `_PG_init()` runs once per process; threaded sessions should not share a mutable GUC registry.

**Work**:
1. In threaded mode at session startup, build a **private copy** of the GUC registry (not just the variable values, but the config_generic entries themselves if they're custom GUCs).
2. Ensure session-local GUC variable state arrays are initialized for custom GUCs.
3. Test that two sessions can have different custom-GUC sets (e.g., one extension loaded in Session A but not B, or vice versa).

**Files to modify**:
- `src/backend/utils/init/backend_runtime_session.c`: Session bootstrap, GUC initialization.
- `src/backend/utils/misc/guc.c`: GUC registry lookup/initialization logic.

**Evidence**:
- guc.c line 565 (GUCRecordState returns shared state for custom GUCs, not per-session).

---

### Priority 3: Extension Hook Per-Session Decoration (DEFER WITH INVARIANT)

**Why**: Hooks are set-once at load time; per-session hook modification is not yet required.

**Defer Rationale**:
- Current extensions do not unload mid-session in normal operation.
- Preloaded extensions are loaded before threading starts (single-threaded startup).
- If a hook is needed per-session (rare), a future design would add a session-local hook table, but that's not Phase 16 scope.

**Invariant to preserve**:
- No extension is loaded/unloaded while a threaded session is active (catch at load time: `if (multithreaded && !process_shared_preload_libraries_in_progress) ereport(ERROR, "cannot load extension during threaded operation")`).
- Hooks are immutable after session startup.
- If violation occurs, crash predictably (assertion or guard).

**Files to update**:
- `src/backend/utils/fmgr/extension.c` or `src/backend/catalog/pg_require.c` (wherever CREATE EXTENSION runs): Add check for multithreaded + mid-session loading.

---

### Priority 4: Extension Assign/Check/Show Hook Testing

**Why**: Hooks run inside ThreadedGUCLock, so hook-level races are mitigated, but hooks must not assume process-wide correctness.

**Work**:
1. Add stress test: two threaded sessions SET the same custom GUC (with a hook) to different values concurrently.
2. Verify assign hook is called exactly once per SET, in the correct order (serialized by lock).
3. Verify hook side effects don't race (e.g., if hook starts a background task, verify no duplicate launches).

**Files to add**:
- `src/test/modules/test_guc_hooks_threaded/`: New test module.
  - `test_guc_hooks_threaded.c`: Extension that defines a custom GUC with hooks.
  - `sql/test_custom_guc_threaded.sql`: Test script.

**Evidence**:
- guc.c line 5732 (hook called inside lock, but extension-provided so contract is unclear).

---

### Priority 5: Custom GUC and Extension Validation Gates

**Why**: Phase 16 exit gate requires "custom/extension GUC stress" and "threaded contrib regression."

**Work**:
1. Run full TAP suite with `multithreaded=on` against all contrib modules that define custom GUCs (e.g., pgstattuple, postgres_fdw, pg_stat_statements).
2. For each custom GUC in contrib:
   - Verify value changes in one session don't affect another.
   - Verify RESET works correctly per-session.
   - Verify GUC assign hooks execute without corruption.
3. Document any custom-GUC module that cannot run in threaded mode as process-only (Phase 19 fallback).

**Test coverage**:
- `src/test/isolation/specs/custom_guc_threaded_*.spec`: Isolation tests for concurrent custom-GUC changes.
- CI: Add `gmake check-threaded` variant that loads representative contrib extensions.

**Evidence**:
- MULTITHREADED_PLAN.md line 1044: "custom/extension GUC stress" is a Gate E2-Extensions validation item.

---

### Priority 6: GUC String Custom Variable Mitigation Extension

**Why**: String custom GUCs already have a guard (guc.c line 5594); ensure non-string types have equivalent.

**Work**:
1. Audit every custom-GUC definition in the codebase to find those with non-session-owned variables.
2. For each custom GUC in a system extension (e.g., pgstattuple, contrib):
   - If variable is global in extension, migrate to per-session shadow.
   - If variable is config-file-set (PGC_POSTMASTER), document as process-only.
3. Add a migration guide for third-party extensions.

**Files to update**:
- `src/backend/utils/misc/README`: Document custom-GUC threading model.
- `doc/src/sgml/extend.sgml`: Extension author guide for custom GUCs in threaded mode.

**Evidence**:
- guc.c line 5607–5608: STRING custom GUCs have `PgCurrentOrEarlySessionOwnsPointer` guard; non-string don't.

---

### Priority 7: Backpressure and Async GUC Changes

**Why**: Long-term; not Phase 16 critical.

**Defer**: Until Phase 17 or later scheduler-boundary work. Async GUC changes (e.g., changing a GUC from a signal handler or callback without holding the GUC lock) need careful sequencing with fiber park/resume. Current synchronous SET inside GUCLock is correct; no deferred work in Phase 16.

---

## Summary Table

| Issue | Severity | Location | Phase 16 Target | Status |
|-------|----------|----------|-----------------|--------|
| Custom GUC valueAddr sharing | CRITICAL | guc.c:3063,3079,3095,3130 | Fix per-session shadow storage | Not yet implemented |
| Custom GUC registry per-session | HIGH | guc.c:565, backend_runtime_session.c | Isolate session GUC tables | Partially done (strings only) |
| Extension hook unload race | MEDIUM | utils/fmgr/extension.c | Prevent mid-session load/unload | Not guarded |
| Custom GUC assign-hook testing | MEDIUM | (new test module) | Add stress tests | No coverage yet |
| Contrib custom-GUC validation | MEDIUM | src/test/isolation/ | Run under multithreaded=on | Ad-hoc only |
| String custom-GUC mitigation | LOW | (extends guc.c logic) | Document / extend guard | Exists for strings, not others |
| Async GUC change safety | LOW | (future) | Defer to Phase 17 | Not in scope |

---

## Recommended Next Steps

1. **Immediate** (Sprint 1): Implement Priority 1 (custom GUC shadow storage for non-string types). This is the correctness blocker.
2. **Follow-up** (Sprint 2): Add Priority 2 (per-session GUC registry isolation for custom GUCs).
3. **Testing** (Sprint 2–3): Implement Priority 4 and 5 (stress tests + contrib regression under threading).
4. **Hardening** (Sprint 3+): Priority 6 (audit all custom GUCs in system; document Phase 19 fallback for process-only extensions).
5. **Defer**: Priority 3 and 7 (hooks and async GUC changes) to Phase 17 or later.

