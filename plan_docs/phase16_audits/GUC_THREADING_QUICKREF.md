# GUC + Extension-Hook Threading Audit: Quick Reference

## Status Summary

| Component | Status | Risk | Phase 16 Work |
|-----------|--------|------|--------------|
| **Core GUC Storage** | ✅ Per-session safe | LOW | None needed |
| **Custom GUC valueAddr** | ❌ Process-global shared | CRITICAL | Priority 1: shadow storage |
| **Extension Hooks** | ⚠️ Shared, set-once | MEDIUM | Priority 3: defer with guard |
| **GUC Assign Hooks** | ✅ Serialized by lock | LOW | Priority 4: testing only |

---

## The Core Problem

**Custom GUC valueAddr** (extension-provided pointer) is shared across all sessions:

```
Extension registers:  static int my_param = 0;
                     DefineCustomIntVariable(..., &my_param, ...);

Session A: SET myext.param = 10  →  writes &my_param
Session B: SET myext.param = 20  →  writes &my_param (SAME ADDRESS!)
Session A: SHOW myext.param      →  reads &my_param = 20 (WRONG!)
```

**Why it breaks**:
- Both sessions point at the same process-global variable
- GUC state is supposed to be per-session
- String custom GUCs have a guard (line 5607-5608); BOOL/INT/REAL/ENUM don't

---

## What Needs Fixing (Priority 1)

**File**: `src/backend/utils/misc/guc.c`

**Add per-session shadow storage** for custom GUCs (like strings already have):

```c
// Before (line 3063, 3079, 3095, 3130):
*GUC_VARIABLE_BOOL(gconf) = GUC_RESET_BOOL(gconf) = newval;  // writes extension pointer

// After (symmetric to STRING case):
if (is_custom_guc && !PgCurrentSessionOwnsPointer(GUC_VARIABLE_BOOL(gconf)))
    allocate_custom_guc_shadow_for_session(gconf);  // allocate session copy
*GUC_VARIABLE_BOOL(gconf) = GUC_RESET_BOOL(gconf) = newval;  // now writes session copy
```

---

## Extension Hooks: Safe But Unguarded

**All ~25 hooks are process-global function pointers**:
```c
PG_GLOBAL_RUNTIME ExecutorStart_hook_type ExecutorStart_hook = NULL;
PG_GLOBAL_RUNTIME ProcessUtility_hook_type ProcessUtility_hook = NULL;
PG_GLOBAL_RUNTIME planner_hook_type planner_hook = NULL;
```

**Current safety**: Loaded at postmaster startup, never unloaded → safe.
**Future risk**: If extensions can be loaded/unloaded mid-session → need guard.

**Phase 16 action** (Priority 3, defer):
```c
if (multithreaded && !process_shared_preload_libraries_in_progress)
    ereport(ERROR, "cannot load extension during threaded operation");
```

---

## Testing Needed (Priority 4)

Concurrent custom-GUC test:
```sql
-- Session A
SET myext.param = 10;
SELECT current_setting('myext.param');  -- must be '10', not '20'

-- Session B (concurrent)
SET myext.param = 20;
SELECT current_setting('myext.param');  -- must be '20', not '10'
```

---

## File Locations & Evidence

| Issue | File:Line | Code | Fix |
|-------|-----------|------|-----|
| BOOL unguarded | guc.c:3063 | `*GUC_VARIABLE_BOOL(gconf) = ...` | Add session check |
| INT unguarded | guc.c:3079 | `*GUC_VARIABLE_INT(gconf) = ...` | Add session check |
| REAL unguarded | guc.c:3095 | `*GUC_VARIABLE_REAL(gconf) = ...` | Add session check |
| ENUM unguarded | guc.c:3130 | `*GUC_VARIABLE_ENUM(gconf) = ...` | Add session check |
| STRING already guarded | guc.c:5607–5608 | `PgCurrentOrEarlySessionOwnsPointer()` | Copy to others |
| Extension pointer stored | guc.c:6809–6913 | `GUC_VARIABLE_*(var) = valueAddr` | Shadow storage |

---

## Phase 16 Checklist

- [ ] Priority 1: Custom-GUC shadow storage (BOOL/INT/REAL/ENUM)
- [ ] Priority 2: Per-session GUC state arrays (custom GUCs)
- [ ] Priority 3: Extension-load guard (defer with invariant)
- [ ] Priority 4: Concurrent stress test
- [ ] Priority 5: Contrib TAP with multithreaded=on
- [ ] Priority 6: Documentation (README + extend.sgml)

---

See: **GUC_EXTENSION_THREADING_AUDIT.md** for full 437-line report
