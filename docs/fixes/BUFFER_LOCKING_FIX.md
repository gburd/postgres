# Buffer Locking Fix for Orvos Chunk Allocation

**Date**: 2026-03-04
**Commit**: 1936e9ef104
**Status**: ✅ FIXED

---

## Problem

Tests were failing with assertion failures and core dumps:

```
TRAP: failed Assert("entry->data.lockmode == BUFFER_LOCK_UNLOCK"), File: "bufmgr.c", Line: 5770
```

**Failing tests**:
- `postgresql:regress / regress/regress`
- `postgresql:recovery / recovery/027_stream_regress`
- `postgresql:pg_upgrade / pg_upgrade/002_pg_upgrade`

**Crash location**: `ovpage_extendrel_newbuf()` at orvos_freepagemap.c:220

---

## Root Cause

The code used the legacy PostgreSQL buffer extension pattern:

```c
/* OLD CODE - BROKEN */
buf = ReadBuffer(rel, P_NEW);
LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);  // ASSERTION FAILS HERE
```

In modern PostgreSQL, `ReadBuffer(rel, P_NEW)` may return buffers in an inconsistent lock state, especially during concurrent extension operations.

---

## Solution

Updated to use the modern `ExtendBufferedRelBy()` API:

```c
/* NEW CODE - CORRECT */
Buffer buffers[513];
uint32 extend_by = 1 + num_extra_pages;
uint32 extended_by = extend_by;
uint32 flags = EB_LOCK_FIRST;

if (RELATION_IS_LOCAL(rel))
    flags |= EB_SKIP_EXTENSION_LOCK;

ExtendBufferedRelBy(BMR_REL(rel), MAIN_FORKNUM, NULL,
                    flags, extend_by, buffers, &extended_by);

buf = buffers[0];  /* First buffer returned locked */
```

**Benefits**:
- Proper lock state management (EB_LOCK_FIRST flag)
- Automatic extension lock handling
- Better performance (single API call for all pages)
- Aligns with modern heap implementation

---

## Files Modified

1. **src/backend/access/orvos/orvos_freepagemap.c**
   - Replaced `ReadBuffer(P_NEW)` loop with `ExtendBufferedRelBy()`
   - Removed manual extension lock management
   - Fixed type mismatches and removed unused variables

2. **src/backend/access/orvos/orvos_meta.c**
   - Updated metapage initialization to use `ExtendBufferedRel()`
   - Simpler and safer for single-page extension

---

## Testing Results

**Before Fix**: 3 test suites failing, core dumps, assertion failures

**After Fix**: ✅ All tests passing (exit code 0)

```bash
cd build && meson test --suite postgresql:regress --suite postgresql:recovery --suite postgresql:pg_upgrade
# Result: PASSED
```

---

## Technical Details

### Buffer Lock States

PostgreSQL buffers have these lock states:
- `BUFFER_LOCK_UNLOCK`: No content lock
- `BUFFER_LOCK_SHARED`: Shared (read) lock
- `BUFFER_LOCK_EXCLUSIVE`: Exclusive (write) lock

**Critical rule**: Cannot call `LockBuffer()` on an already-locked buffer.

### ExtendBufferedRelBy Flags

- `EB_LOCK_FIRST`: First buffer returned locked
- `EB_SKIP_EXTENSION_LOCK`: For local relations (no concurrency)

### Why This Pattern is Better

**Old**: Multiple `ReadBuffer` calls, manual lock management, error-prone
**New**: Single API call, automatic lock handling, modern and safe

---

## Recommendations

1. **Never use `ReadBuffer(rel, P_NEW)` + `LockBuffer()`**: Use `ExtendBufferedRel` APIs instead
2. **Check heap implementation**: When updating buffer code, see how heap does it
3. **Test with assertions**: Build with `--enable-cassert` to catch issues early

---

**Status**: ✅ PRODUCTION-READY
**Commit**: 1936e9ef104
