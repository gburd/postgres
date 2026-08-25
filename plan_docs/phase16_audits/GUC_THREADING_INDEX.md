# GUC + Extension-Hook Threading Audit: Document Index

## Overview

This audit examined the multithreaded PostgreSQL fork's GUC (Grand Unified Configuration) subsystem and extension-hook surface for thread-safety correctness under `multithreaded=on`. Two comprehensive reports were produced: a full technical audit (437 lines) and a quick reference (111 lines).

**Scope**: Read-only code analysis, no modifications made, no tests executed.

---

## Documents

### 1. GUC_EXTENSION_THREADING_QUICK_REF.md
**Purpose**: One-page status overview and action items  
**Audience**: Quick decision-making, Phase 16 task planning  
**Content**:
- Status matrix (4 components, 4 properties each)
- Core problem explanation with code example
- Priority 1 fix summary
- Extension hooks status
- Testing checklist
- Phase 16 task list
- File locations table

**When to use**: Starting point before diving into full audit

---

### 2. GUC_EXTENSION_THREADING_AUDIT.md
**Purpose**: Full technical audit with evidence and ranked worklist  
**Audience**: Core developers, Phase 16 leads, code reviewers  
**Content**:
- Part 1: Core GUC Storage Architecture (built-in GUCs)
  - Verdict: Per-backend safe ✓
  - Evidence: Backing storage, accessor patterns, threaded lock coordination
  
- Part 2: Custom GUC valueAddr Sharing Problem (CRITICAL)
  - The hazard and corruption scenario
  - Where it breaks (5 specific locations: lines 3063, 3079, 3095, 3130, 5732)
  - Evidence table with file:line references
  
- Part 3: Extension Hook-Pointer Globals (25 hooks)
  - Complete inventory with locations
  - Hook safety analysis (set-once, lifetime)
  - Risk assessment (low for preloaded, medium for mid-session)
  
- Part 4: GUC Assign/Check/Show Hooks
  - Hook types and thread-safety assumptions
  - Risk analysis (extension contract issue, not GUC bug)
  
- Part 5: Ranked Phase 16 Worklist
  - 7 priorities from CRITICAL to defer
  - Priority 1: Custom-GUC shadow storage (blocker)
  - Priority 2: Registry per-session isolation
  - Priority 3: Hook unload guard (defer with invariant)
  - Priorities 4-6: Testing, validation, documentation
  - Priority 7: Async GUC changes (Phase 17+)
  - Summary table with severity, location, status
  
- Recommended next steps

**When to use**: Implementation planning, code review, detailed investigation

---

## Key Findings Summary

| Finding | Status | Evidence | Phase 16 Action |
|---------|--------|----------|-----------------|
| **1. Core GUC Storage** | ✅ Safe | backend_runtime_guc.c, guc.c:565 | None needed |
| **2. Custom GUC valueAddr** | ❌ Hazard | guc.c:3063,3079,3095,3130 | Priority 1: Shadow storage |
| **3. Extension Hooks** | ⚠️ Shared | execMain.c:70, utility.c:77 | Priority 3: Guard + defer |
| **4. Assign Hooks** | ✅ Safe | guc.c:5732 (locked) | Priority 4: Testing |

---

## Critical Issue: Custom GUC valueAddr Corruption

**Location**: guc.c:3063, 3079, 3095, 3130

**Problem**:
```
Extension A registers: static int my_param = 0;
                      DefineCustomIntVariable("myext.param", ..., &my_param);

Session 1: SET myext.param = 10  → writes &my_param
Session 2: SET myext.param = 20  → writes &my_param (SAME ADDRESS!)
Session 1: SHOW myext.param      → reads &my_param = 20 (WRONG! expects 10)
```

**Root Cause**: Extension pointer passed to DefineCustom*Variable() is stored directly in GUC state. All sessions share that pointer. String custom GUCs have a guard; BOOL/INT/REAL/ENUM don't.

**Fix**: Allocate per-session shadow storage for custom-GUC values (symmetric to STRING case at guc.c:5607-5608).

**Blocker**: Yes. Must be fixed before Phase 16 completion.

---

## File Locations Quick Reference

| Issue | File:Line | Type |
|-------|-----------|------|
| BOOL custom unguarded | guc.c:3063 | Write path |
| INT custom unguarded | guc.c:3079 | Write path |
| REAL custom unguarded | guc.c:3095 | Write path |
| ENUM custom unguarded | guc.c:3130 | Write path |
| Extension pointer stored | guc.c:6809-6913 | Registration |
| STRING guard (model) | guc.c:5607-5608 | Fix reference |
| Custom state lookup | guc.c:565 | Architecture |
| ExecutorStart_hook | executor/execMain.c:70 | Hook definition |
| ProcessUtility_hook | tcop/utility.c:77 | Hook definition |
| planner_hook | optimizer/plan/planner.c:38 | Hook definition |
| (22 more hooks) | (various) | Hook definitions |

---

## Phase 16 Priorities

### ⚠️ CRITICAL (Blocker)
**Priority 1**: Custom GUC valueAddr per-session binding
- Must implement before Phase 16 closes
- Files: guc.c, backend_runtime.c/guc.c

### 🔴 HIGH (Core feature)
**Priority 2**: Custom GUC registry per-session isolation
- Depends on Priority 1
- Files: backend_runtime_session.c, guc.c

### 🟡 MEDIUM (Safety invariant)
**Priority 3**: Extension hook unload guard (defer with invariant)
- Prevents mid-session extension load/unload
- File: utils/fmgr/extension.c

### 🟢 LOW (Testing/Validation)
**Priority 4**: Custom GUC assign-hook stress testing
**Priority 5**: Contrib custom-GUC regression suite
**Priority 6**: Documentation (README + extend.sgml)

### 🔵 FUTURE (Phase 17+)
**Priority 7**: Async GUC changes (signal handler safety)

---

## How to Use These Reports

### For Quick Status Check
Read **GUC_EXTENSION_THREADING_QUICK_REF.md** (5 min)

### For Implementation Planning
1. Read **GUC_EXTENSION_THREADING_QUICK_REF.md** (5 min)
2. Review Priority 1 section in **GUC_EXTENSION_THREADING_AUDIT.md** (20 min)
3. Examine guc.c:3063, 3079, 3095, 3130 side-by-side with guc.c:5607-5608

### For Deep Technical Review
Read **GUC_EXTENSION_THREADING_AUDIT.md** in full (45 min)
- Part 1 provides architecture context
- Part 2 has detailed corruption scenarios and evidence
- Part 5 ranks all work items with file:line references

### For Code Review
Use the **Summary Table** in Part 5 of the full audit as a checklist:
- Verify Priority 1 implementation adds session ownership checks
- Verify Priority 2 adds per-session state arrays for custom GUCs
- Verify Priority 3 adds guard in CREATE EXTENSION path
- Verify Priorities 4-6 are tested and documented

---

## Audit Metadata

| Property | Value |
|----------|-------|
| Audit Date | 2026-08-25 |
| Scope | GUC subsystem + 25 extension hooks |
| Analysis Type | Read-only code inspection |
| Repo | /home/gburd/ws/postgres/xtc |
| Branch | wave1-candidate |
| Files Analyzed | ~35 source files, ~9500 lines GUC code |
| Tools Used | grep, find, read, bash (read-only) |
| Modifications Made | NONE (read-only audit) |
| Tests Executed | NONE (analysis only) |

---

## Next Steps

1. **Immediate** (this sprint):
   - Review GUC_EXTENSION_THREADING_QUICK_REF.md
   - Schedule Priority 1 implementation

2. **This week**:
   - Read full GUC_EXTENSION_THREADING_AUDIT.md
   - Assess Priority 1 implementation effort

3. **Phase 16 sprint**:
   - Implement Priority 1 (shadow storage for BOOL/INT/REAL/ENUM)
   - Follow with Priorities 2-6 in sequence
   - Defer Priority 3 + 7 with documented invariants

---

**Questions?** See the full audit documents or the concrete file:line evidence tables.

