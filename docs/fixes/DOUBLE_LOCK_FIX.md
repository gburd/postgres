# Fix Buffer Double-Lock in Chunk Allocation

**Date**: 2026-03-04
**Issue**: Test failures with buffer double-lock assertion
**Status**: ✅ FIXED

---

## Problem

Tests were crashing with assertion failure during INSERT operations:

```
TRAP: failed Assert("entry->data.lockmode == BUFFER_LOCK_UNLOCK"),
File: "../src/backend/storage/buffer/bufmgr.c", Line: 5770, PID: 1222804

postgres: gburd regression [local] INSERT(ovpage_extendrel_newbuf+0x1a4)
postgres: gburd regression [local] INSERT(ovpage_getnewbuf+0xde)
postgres: gburd regression [local] INSERT(ovmeta_get_root_for_attribute+0x220)
```

Error output:
```
server closed the connection unexpectedly
	This probably means the server terminated abnormally
	before or while processing the request.
connection to server was lost
```

This occurred during the first INSERT after creating an orvos table.

---

## Root Cause

**Double-locking the metapage buffer**:

1. `ovpage_getnewbuf()` locks the metapage buffer (line 113):
   ```c
   metabuf = ReadBuffer(rel, OV_META_BLK);
   LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
   ```

2. When the FPM is empty (`blk == InvalidBlockNumber`), it calls:
   ```c
   buf = ovpage_extendrel_newbuf(rel);  // Only passes relation
   ```

3. `ovpage_extendrel_newbuf()` tries to lock the metapage AGAIN (line 230):
   ```c
   metabuf = ReadBuffer(rel, OV_META_BLK);
   LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);  // DOUBLE LOCK!
   ```

This causes PostgreSQL's assertion to fail because we're trying to lock a buffer that this process already has locked.

---

## Solution

Pass the already-locked metabuf to `ovpage_extendrel_newbuf()` so it doesn't try to acquire the lock again.

### Changes Made

**1. Function Signature**

Changed from:
```c
Buffer ovpage_extendrel_newbuf(Relation rel)
```

To:
```c
Buffer ovpage_extendrel_newbuf(Relation rel, Buffer metabuf)
```

**2. Buffer Management Logic**

```c
Buffer local_metabuf = InvalidBuffer;
bool release_metabuf = false;

if (extended_by > 1)
{
    /* Get the metapage to update the FPM */
    if (metabuf == InvalidBuffer)
    {
        /* Caller didn't provide metabuf, we need to read it */
        local_metabuf = ReadBuffer(rel, OV_META_BLK);
        LockBuffer(local_metabuf, BUFFER_LOCK_EXCLUSIVE);
        release_metabuf = true;
    }
    else
    {
        /* Caller already has metabuf locked, use it */
        local_metabuf = metabuf;
        release_metabuf = false;
    }

    metapage = BufferGetPage(local_metabuf);
    metaopaque = (OVMetaPageOpaque *) PageGetSpecialPointer(metapage);

    // ... process extra pages ...

    MarkBufferDirty(local_metabuf);
    if (release_metabuf)
        UnlockReleaseBuffer(local_metabuf);
}
```

**3. Call Site Update**

In `ovpage_getnewbuf()`:
```c
if (blk == InvalidBlockNumber)
{
    /* No free pages. Have to extend the relation. */
    buf = ovpage_extendrel_newbuf(rel, metabuf);  // Now passes metabuf
    blk = BufferGetBlockNumber(buf);
}
```

**4. Header File Update**

Updated declaration in `orvos_internal.h`:
```c
extern Buffer ovpage_extendrel_newbuf(Relation rel, Buffer metabuf);
```

---

## Files Modified

1. **src/backend/access/orvos/orvos_freepagemap.c**:
   - Changed function signature (line 166)
   - Added `local_metabuf` and `release_metabuf` variables
   - Updated metabuf acquisition logic to check if already provided
   - Only release metabuf if we acquired it ourselves
   - Updated call site to pass metabuf (line 132)

2. **src/include/access/orvos_internal.h**:
   - Updated function declaration (line 985)

---

## Why This Fix Works

### Buffer Lock States

When `ovpage_getnewbuf()` is called:
- If `metabuf == InvalidBuffer`: Function reads and locks the metapage
- If `metabuf != InvalidBuffer`: Caller already has it locked

When FPM is empty and we need to extend:
- Before fix: `ovpage_extendrel_newbuf()` blindly tried to lock metapage again
- After fix: `ovpage_extendrel_newbuf()` checks if metabuf was provided and reuses it

### Lock Ownership

PostgreSQL tracks buffer locks per-process. The assertion checks that we don't try to lock a buffer we already have locked:

```c
Assert(entry->data.lockmode == BUFFER_LOCK_UNLOCK);
```

By passing the already-locked buffer instead of trying to acquire it again, we avoid the double-lock scenario.

---

## Testing

### Before Fix
```sql
CREATE TABLE t_orvos(c1 int, c2 int, c3 int) USING orvos;
INSERT INTO t_orvos SELECT i,i+1,i+2 FROM generate_series(1, 10)i;
-- ERROR: server closed connection unexpectedly
-- Backend crashed with assertion failure
```

### After Fix
```sql
CREATE TABLE t_orvos(c1 int, c2 int, c3 int) USING orvos;
INSERT INTO t_orvos SELECT i,i+1,i+2 FROM generate_series(1, 10)i;
INSERT 0 10  -- Success!

SELECT * FROM t_orvos;
-- Returns all 10 rows correctly
```

---

## Related Issues

This is different from the previous buffer locking fix (commit 1936e9ef104):
- **Previous fix**: Changed ReadBuffer(P_NEW) to ExtendBufferedRelBy()
- **This fix**: Prevents double-locking an already-locked metapage buffer

Both fixes address buffer management issues, but in different contexts:
- Previous: New buffer allocation and locking
- Current: Reusing an already-locked buffer

---

## Design Notes

### Why Not Always Read Fresh?

We could have `ovpage_extendrel_newbuf()` always read its own metabuf, but that would be inefficient:
- Caller already has the metabuf locked
- Reading it again would cause unnecessary buffer lookups
- We'd need to unlock the caller's buffer first (complex coordination)

### Why Check InvalidBuffer?

Some callers might call `ovpage_extendrel_newbuf()` without having a metabuf yet (though currently none do). By checking for `InvalidBuffer`, we make the function more flexible and self-contained.

### Buffer Lifecycle

```
ovpage_getnewbuf(rel, metabuf_from_caller)
├─ If metabuf_from_caller == InvalidBuffer:
│  ├─ ReadBuffer(OV_META_BLK) → metabuf [LOCKED]
│  └─ release_metabuf = true
├─ Else:
│  └─ metabuf = metabuf_from_caller [ALREADY LOCKED]
│
├─ Check FPM for free pages
│
├─ If FPM empty:
│  ├─ ovpage_extendrel_newbuf(rel, metabuf) [PASSES LOCKED BUFFER]
│  │  ├─ ExtendBufferedRelBy() to get new pages
│  │  └─ Uses provided metabuf (already locked)
│  └─ Returns new buffer
│
└─ If release_metabuf: UnlockReleaseBuffer(metabuf)
```

---

## Verification

Confirmed fix by:
1. Rebuilding: `ninja -C build`
2. Running regression test: `meson test postgresql:regress/regress/regress`
3. Checking no assertion failures in postmaster.log
4. Verifying INSERT operations complete successfully

---

## Confidence Assessment

**Confidence**: 100% ✅

**Rationale**:
1. Root cause clearly identified from backtrace
2. Fix directly addresses the double-lock scenario
3. Follows PostgreSQL buffer management patterns
4. No change to external behavior, only internal buffer handling

---

**Status**: ✅ FIXED
**Commit**: 940006a7d0e
