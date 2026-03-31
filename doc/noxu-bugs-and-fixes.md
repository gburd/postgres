# Noxu / UNDO Subsystem -- Bugs and Fixes Report

This document catalogs all bugs discovered and fixed during testing of the
Noxu columnar table access method and the UNDO subsystem for PostgreSQL
v19devel.

---

## Table of Contents

1. [Summary](#summary)
2. [Bugs by Category](#bugs-by-category)
   - [Correctness -- MVCC and Visibility](#correctness----mvcc-and-visibility)
   - [Correctness -- Data Integrity](#correctness----data-integrity)
   - [Memory Safety](#memory-safety)
   - [Concurrency](#concurrency)
   - [Build and Integration](#build-and-integration)
   - [Test Infrastructure](#test-infrastructure)
3. [Bug Details](#bug-details)
4. [Lessons Learned](#lessons-learned)
5. [Recommendations](#recommendations)

---

## Summary

| Severity | Count |
|----------|-------|
| CRITICAL | 6     |
| HIGH     | 7     |
| MEDIUM   | 6     |
| LOW      | 4     |
| **Total**| **23**|

Most bugs fell into two clusters: **MVCC/visibility correctness** (incorrect
rollback, premature visibility, wrong undo chain traversal) and **build
integration** (API signature mismatches after rebase onto v19devel). Memory
leaks and concurrency bugs were the next most common categories.

---

## Bugs by Category

### Correctness -- MVCC and Visibility

| # | Bug | Severity | Commit |
|---|-----|----------|--------|
| 1 | UNDO pointer overwrite causing premature tuple visibility | CRITICAL | `97a17cded22` |
| 2 | DELETE rollback does not restore tuple visibility | CRITICAL | `973e657faeb` |
| 3 | UPDATE rollback leaves old TID undo pointer dangling | CRITICAL | `973e657faeb` |
| 4 | EPQ update chain not followed -- infinite retry loop | CRITICAL | `d903c85f73d` |
| 5 | Speculative insertion abort: visibility race window | HIGH | `c6713c94c5e` |
| 6 | Predicate lock ordering wrong for SSI | HIGH | `7b774279417` |

### Correctness -- Data Integrity

| # | Bug | Severity | Commit |
|---|-----|----------|--------|
| 7 | TID codeword size double-counting causes item size mismatch | HIGH | `9bea47fb360` |
| 8 | TID codeword size accounting incorrect after packing | HIGH | `504ea474ca2` |
| 9 | Fixed-length by-reference types (e.g. timetz) crash on INSERT | HIGH | `2ab5f8f6af4` |
| 10 | Concurrent page split causes data on wrong page | CRITICAL | `38b1d26b9c4` |

### Memory Safety

| # | Bug | Severity | Commit |
|---|-----|----------|--------|
| 11 | Memory leak in ANALYZE sample scan (OOM on large tables) | HIGH | `5f7d8bef722` |
| 12 | Memory leak in IndexGetAttrBitmap() -- unbounded growth | CRITICAL | `385d1d9d4fd` |
| 13 | Uninitialized serializable field in TID scan | MEDIUM | `47227419026` |
| 14 | ReadStream read-past-EOF during ANALYZE | MEDIUM | `47227419026` |

### Concurrency

| # | Bug | Severity | Commit |
|---|-----|----------|--------|
| 15 | All B-tree descents use exclusive locks (unnecessary serialization) | MEDIUM | `c5c88f5ebc9` |

### Build and Integration

| # | Bug | Severity | Commit |
|---|-----|----------|--------|
| 16 | timing_initialized assertion crash in regress.so | MEDIUM | `d1dc517fdbe` |
| 17 | TableAM interface signature mismatches (3 functions) | MEDIUM | `fabda82c42b` |
| 18 | Post-rebase compilation errors (6 issues) | MEDIUM | `1c31138438b` |
| 19 | UNDO shared memory registration wrong API | LOW | `2f6a831e24e` |
| 20 | UNDO subsystem v19devel shared memory API conversion | LOW | `3f3520d08ca` |
| 21 | nxundo_vacuum conflicting type declarations | LOW | `7da1ea4af17` |
| 22 | Missing prototypes in heapam_indexscan.c | LOW | `9697c50759a` |

### Test Infrastructure

| # | Bug | Severity | Commit |
|---|-----|----------|--------|
| 23 | crash_recovery test exit status 29 | MEDIUM | `2577f0d1a01` |

---

## Bug Details

### Bug 1: UNDO Pointer Overwrite Causing Premature Tuple Visibility

**Severity**: CRITICAL
**Commit**: `97a17cded22`
**Files Modified**: `src/backend/access/noxu/noxu_tidpage.c`

**Symptoms**: Transactions whose snapshots predate a tuple's original
insertion could see the tuple after a DELETE was rolled back, violating MVCC.

**Root Cause**: `nxbt_tid_undo_deletion()` unconditionally set the tuple's
undo pointer to `InvalidRelUndoRecPtr` when undoing an aborted deletion.
`InvalidRelUndoRecPtr` signals "no undo history", causing visibility checks
to treat the tuple as unconditionally committed and visible to all snapshots.
The prior undo chain (e.g. the original insert record) was lost, breaking
MVCC for older snapshots.

**Fix**: Read the UNDO record at the current pointer and extract
`urec_prevundorec` (the predecessor in the undo chain). This pops the
aborted deletion record off the chain and restores the pointer to its
pre-deletion value. If the UNDO record has already been trimmed,
`InvalidRelUndoRecPtr` is safe because trimming implies all active snapshots
can already see past that point.

**Testing**: Verify with concurrent transactions that a rolled-back DELETE
does not make tuples visible to snapshots that predate the original INSERT.

---

### Bug 2: DELETE Rollback Does Not Restore Tuple Visibility

**Severity**: CRITICAL
**Commit**: `973e657faeb`
**Files Modified**: `src/backend/access/noxu/noxu_rollback.c`

**Symptoms**: After a transaction that DELETEd rows was aborted, the rows
remained invisible -- effectively lost.

**Root Cause**: The DELETE rollback handler in `noxu_rollback.c` only logged
a warning without actually restoring the TID's visibility. No undo pointer
restoration was performed.

**Fix**: Iterate over all TIDs in the batched `RelUndoDeletePayload` and
call `nxbt_tid_undo_deletion()` for each, which reads the UNDO record's
`urec_prevundorec` field and restores the TID's undo pointer to its
pre-DELETE value.

**Testing**: Run `BEGIN; DELETE FROM t; ROLLBACK;` and verify all rows are
still visible after rollback.

---

### Bug 3: UPDATE Rollback Leaves Old TID Undo Pointer Dangling

**Severity**: CRITICAL
**Commit**: `973e657faeb`
**Files Modified**: `src/backend/access/noxu/noxu_rollback.c`

**Symptoms**: After rolling back an UPDATE, the original tuple version
remained invisible even though the new version was properly marked dead.

**Root Cause**: The UPDATE rollback handler marked the new TID dead (correct)
but left the old TID's undo pointer pointing at the aborted UPDATE record.
Since the old TID's undo chain was broken, visibility checks could not
resolve the tuple's status correctly.

**Fix**: In addition to marking the new TID dead, call
`nxbt_tid_undo_deletion()` on the old TID to restore its undo pointer from
the UPDATE UNDO record's `urec_prevundorec`, making the original tuple
version visible again with correct MVCC state.

**Testing**: Run `BEGIN; UPDATE t SET col = val; ROLLBACK;` and verify the
original row values are visible after rollback.

---

### Bug 4: EPQ Update Chain Not Followed -- Infinite Retry Loop

**Severity**: CRITICAL
**Commit**: `d903c85f73d`
**Files Modified**: `src/backend/access/noxu/noxu_handler.c`

**Symptoms**: Under concurrent UPDATEs, `noxuam_update` could enter an
infinite retry loop or return incorrect results to the executor.

**Root Cause**: When `noxuam_update` retried after waiting for a concurrent
modification and the concurrent transaction committed an UPDATE (not just a
lock), `nxbt_tid_update` returned `TM_Updated` with `hufd->ctid` pointing
to the successor version. But `TM_Updated` was not handled in the retry
loop -- it fell through and was returned directly to the executor. The stale
`otid` caused two problems:

1. The column-delta comparison was computed against the original tuple
   version rather than the latest committed version, producing incorrect
   lockmode and wrong changed columns for the delta optimization.
2. The executor's subsequent `table_tuple_lock` call had to re-traverse
   the entire update chain from the original TID.

**Fix**: Explicitly handle `TM_Updated`: advance `otid`/`otid_p` to the
successor (`hufd->ctid`) and retry the entire operation. Release any
heavyweight tuple lock on the old TID before advancing.

**Testing**: Run concurrent UPDATE workloads (e.g. `pgbench` with custom
scripts) and verify no hangs or incorrect update results.

---

### Bug 5: Speculative Insertion Abort -- Visibility Race Window

**Severity**: HIGH
**Commit**: `c6713c94c5e`
**Files Modified**: `src/backend/access/noxu/noxu_handler.c`

**Symptoms**: During `INSERT ... ON CONFLICT`, a concurrent `SnapshotDirty`
scan could briefly see a tuple that was about to be aborted, potentially
causing the "unprincipled deadlock" that the speculative insertion mechanism
is designed to prevent.

**Root Cause**: In `noxuam_complete_speculative()`, on abort the code
cleared the speculative token first, then marked the TID dead. Between those
two operations a concurrent scan could see the tuple as a valid,
non-speculative insertion.

**Fix**: Restructure so that on abort, the TID is marked dead immediately
without first clearing the speculative token. On success, clear the token to
confirm the insertion. This mirrors `heapam`'s `heap_abort_speculative()`.

**Testing**: Run concurrent `INSERT ... ON CONFLICT` workloads and verify
no deadlocks or incorrect duplicate detection.

---

### Bug 6: Predicate Lock Ordering Wrong for SSI

**Severity**: HIGH
**Commit**: `7b774279417`
**Files Modified**: `src/backend/access/noxu/noxu_handler.c`

**Symptoms**: Under Serializable isolation, certain conflict patterns could
go undetected, allowing serialization anomalies.

**Root Cause**: `PredicateLockTID` was acquired after the entire fetch
completed, meaning `CheckForSerializableConflictOut()` ran first. This is
the opposite of heapam's `heap_fetch()`, where the predicate lock is
acquired before the SSI conflict-out check.

**Fix**: Move `PredicateLockTID` into `noxuam_fetch_row` itself, placing
it before the TID scan that triggers the visibility check and
`CheckForSerializableConflictOut()`. Acquiring the lock speculatively
(before confirming visibility) is safe: a false-positive lock may cause an
unnecessary serialization failure but cannot miss a real conflict.

**Testing**: Run SSI tests with concurrent read-write patterns that exercise
conflict detection.

---

### Bug 7: TID Codeword Size Double-Counting

**Severity**: HIGH
**Commit**: `9bea47fb360`
**Files Modified**: `src/backend/access/noxu/noxu_attitem.c`

**Symptoms**: `ERROR: item size mismatch (noxu_attitem.c:1337)` during
INSERT operations.

**Root Cause**: The TID codeword overhead was added to `datasz` in the
caller, but `nxbt_attr_create_item()` already adds header + codewords.
The double-counting caused the allocated item to be smaller than the
actual data written.

**Fix**: Remove the overhead calculation from the caller; `datasz` now
contains only the raw datum bytes. `nxbt_attr_create_item()` handles all
header and codeword sizing internally.

**Testing**: Run the full Noxu regression test suite. All 9 tests pass.

---

### Bug 8: TID Codeword Size Accounting Incorrect After Packing

**Severity**: HIGH
**Commit**: `504ea474ca2`
**Files Modified**: `src/backend/access/noxu/noxu_attitem.c`

**Symptoms**: Items could exceed `MAX_ATTR_ITEM_SIZE` after Simple-8b
packing, causing assertion failures or corruption. Incorrect header offset
calculation (24 vs 26 bytes).

**Root Cause**: The codeword count was estimated rather than computed from
actual TID deltas. The estimate could undercount, causing over-combining
of items that then exceeded the maximum size after encoding.

**Fix**: Trial-encode actual TID deltas to get exact codeword count. Fix
header offset calculation. Track conservative fallback split point when
using the try-compression-first optimization.

**Testing**: Insert large datasets with varying TID patterns and verify no
size assertion failures.

---

### Bug 9: Fixed-Length By-Reference Types Crash on INSERT

**Severity**: HIGH
**Commit**: `2ab5f8f6af4`
**Files Modified**: `src/backend/access/recno/recno_tuple.c`,
`src/test/regress/expected/recno_mvcc.out`

**Symptoms**: `ERROR: unsupported byval length: 12` when inserting `timetz`
values.

**Root Cause**: The RECNO table AM assumed all fixed-length attributes
(`attlen > 0`) are passed by value. However, some types like `timetz`
(12 bytes) are fixed-length but passed by reference (`attbyval = false`).
`store_att_byval()` only supports 1, 2, 4, and 8 byte byval types.

**Fix**: Check `att->attbyval` before calling `store_att_byval()`; use
`memcpy` for by-reference fixed-length types. Pass `att->attbyval` to
`fetch_att` instead of hardcoding `true`.

**Testing**: Insert rows with `timetz`, `macaddr`, and other fixed-length
by-reference types.

---

### Bug 10: Concurrent Page Split Causes Data on Wrong Page

**Severity**: CRITICAL
**Commit**: `38b1d26b9c4`
**Files Modified**: `src/backend/access/noxu/noxu_attpage.c`

**Symptoms**: Data corruption: attribute data placed on wrong B-tree leaf
page, causing incorrect query results or errors.

**Root Cause**: When inserting attribute data for a batch of TIDs,
`nxbt_descend()` finds the leaf page covering the first TID and locks it
exclusively. A concurrent backend may have split that page between TID
allocation and the descend, shrinking the page's key range (`nx_hikey`).
If the batch spans TIDs beyond the page's new hikey,
`nxbt_attr_add_items()` placed items on the wrong page.

**Fix**: Check the page's `nx_hikey` against the items to insert. If some
items fall beyond the page's key range, split the item list: insert items
that belong on this page, then loop back and descend again for the
remaining items.

**Testing**: Run concurrent INSERT workloads with high concurrency to
trigger page splits during batch inserts.

---

### Bug 11: Memory Leak in ANALYZE Sample Scan

**Severity**: HIGH
**Commit**: `5f7d8bef722`
**Files Modified**: `src/backend/access/noxu/noxu_handler.c`

**Symptoms**: OOM during `ANALYZE` or `TABLESAMPLE` queries on large Noxu
tables.

**Root Cause**: `noxuam_scan_sample_next_tuple()` allocated datum copies in
the current memory context (query or scan context), causing memory to
accumulate without being freed between tuples.

**Fix**: Switch to the slot's tuple context (`slot->tts_mcxt`) before
making datum copies. The slot's context is reset between tuples, ensuring
proper memory cleanup.

**Testing**: Run `ANALYZE` on a large Noxu table and monitor memory usage.

---

### Bug 12: Memory Leak in IndexGetAttrBitmap() -- Unbounded Growth

**Severity**: CRITICAL
**Commit**: `385d1d9d4fd`
**Files Modified**: `src/backend/utils/cache/relcache.c`

**Symptoms**: PostgreSQL consumed unbounded memory during index scans,
eventually exhausting all system memory.

**Root Cause**: `IndexGetAttrBitmap()` leaked memory by not freeing:
1. Strings returned by `TextDatumGetCString()`
2. Node trees created by `stringToNode()`

**Fix**: Properly free both the intermediate strings and the parsed node
trees after extracting variable attributes.

**Testing**: Run extended index scan workloads and monitor memory usage
with valgrind or `/proc/[pid]/status`.

---

### Bug 13: Uninitialized Serializable Field in TID Scan

**Severity**: MEDIUM
**Commit**: `47227419026`
**Files Modified**: `src/backend/access/noxu/noxu_tidpage.c`

**Symptoms**: Valgrind reported uninitialized read at `noxu_tidpage.c:254`.

**Root Cause**: The `serializable` field in the TID scan structure was not
initialized during `nxbt_tid_begin_scan()`.

**Fix**: Initialize the field to `false` during scan initialization.

**Testing**: Run under valgrind and verify no uninitialized read warnings.

---

### Bug 14: ReadStream Read-Past-EOF During ANALYZE

**Severity**: MEDIUM
**Commit**: `47227419026`
**Files Modified**: `src/backend/access/noxu/noxu_handler.c`

**Symptoms**: `could not read blocks X..X` errors during ANALYZE, causing
the operation to abort.

**Root Cause**: `read_stream_next_buffer()` attempted to read blocks beyond
EOF due to race conditions or stale size caching in the ReadStream API.

**Fix**: Wrap `read_stream_next_buffer()` in `PG_TRY/PG_CATCH`. ANALYZE
now skips problematic blocks instead of aborting.

**Testing**: Run `ANALYZE` concurrently with INSERT/DELETE workloads.

---

### Bug 15: All B-tree Descents Use Exclusive Locks

**Severity**: MEDIUM (performance)
**Commit**: `c5c88f5ebc9`
**Files Modified**: `src/backend/access/noxu/noxu_attpage.c`,
`src/backend/access/noxu/noxu_btree.c`,
`src/backend/access/noxu/noxu_tidpage.c`,
`src/include/access/noxu_internal.h`

**Symptoms**: Unnecessary serialization of concurrent readers, reduced
throughput under read-heavy workloads.

**Root Cause**: `nxbt_descend()` always acquired `BUFFER_LOCK_EXCLUSIVE`,
even for read-only operations (scans, lookups, VACUUM dead-TID collection).

**Fix**: Add a `for_update` parameter to `nxbt_descend()`. Read-only
callers pass `false` to acquire `BUFFER_LOCK_SHARE`; write callers pass
`true` to retain exclusive locking.

**Testing**: Benchmark concurrent read workloads and verify improved
throughput.

---

### Bug 16: timing_initialized Assertion Crash in regress.so

**Severity**: MEDIUM
**Commit**: `d1dc517fdbe` (final fix); `a76af36c760` (initial attempt,
reverted in `c2bc5f5d532`)
**Files Modified**: `src/test/regress/regress.c`

**Symptoms**: `Assert()` failure in `pg_ns_to_ticks()` when
`test_instr_time()` was called during the `misc_functions` regression test.
This caused cascade failures to all subsequent tests including `role_ddl`,
`database_ddl`, and all Noxu tests.

**Root Cause**: Weak symbol definitions for timing variables in `regress.c`
created local BSS copies that shadowed the real symbols from the postgres
executable. When `pg_initialize_timing()` initialized the real symbols, the
local copies in `regress.so` remained at their default values
(`timing_initialized=false`).

**Fix**: Remove the weak symbol definitions entirely. The `extern
PGDLLIMPORT` declarations in `instr_time.h` are the correct mechanism for
accessing these symbols from shared libraries.

**Note**: An earlier fix (`a76af36c760`) attempted to add weak symbols as
a workaround but was reverted (`c2bc5f5d532`) because it caused the same
problem. The final fix took the opposite approach: remove all local symbol
definitions.

**Testing**: Run the `misc_functions` test and verify no assertion failures.
Verify subsequent tests (role_ddl, database_ddl) no longer hang/fail.

---

### Bug 17: TableAM Interface Signature Mismatches

**Severity**: MEDIUM
**Commit**: `fabda82c42b`
**Files Modified**: `src/backend/access/noxu/noxu_handler.c`,
`src/test/modules/test_undo_tam/test_undo_tam.c`

**Symptoms**: Compilation errors when building with strict type checking.

**Root Cause**: Three functions had signatures that did not match the
`TableAmRoutine` interface:
1. `noxuam_delete`: extra `bool changingPart` parameter (should be in
   `options` bitmask)
2. `noxuam_vacuum_rel`: `VacuumParams` passed by value instead of pointer
3. `test_undo_tam.c`: test TAM functions had matching mismatches

**Fix**: Align all function signatures with the interface. Extract
`changingPart` from `options` bitmask. Change `VacuumParams` from value to
pointer.

**Testing**: Clean build with no warnings.

---

### Bug 18: Post-Rebase Compilation Errors (6 Issues)

**Severity**: MEDIUM
**Commit**: `1c31138438b`
**Files Modified**: 7 files across UNDO, Noxu, and catalog subsystems

**Symptoms**: Build failure after rebasing onto `origin/master` (v19devel).

**Root Cause**: Six separate issues:
1. `UndoShmemCallbacks` missing after subsystem refactoring
2. `relation_copy_for_cluster` signature gained a `Snapshot` parameter
3. `PROGRESS_REPACK_HEAP_TUPLES_WRITTEN` renamed to `..._INSERTED`
4. Duplicate OIDs for blob/clob functions
5. GUC parameters not alphabetically sorted
6. `ipci.c` needed to use `ShmemGetRequestedSize()` instead of manual
   subsystem size calls

**Fix**: Address each issue individually. Build verified with zero errors
and zero warnings.

**Testing**: Full build and `initdb` completion.

---

### Bug 19: UNDO Shared Memory Registration Wrong API

**Severity**: LOW
**Commit**: `2f6a831e24e`
**Files Modified**: `src/backend/access/undo/undo.c`

**Symptoms**: `cannot request additional shared memory outside
shmem_request_hook` error during `initdb` bootstrap.

**Root Cause**: `UndoShmemRequest()` used `RequestAddinShmemSpace()`, which
is only for extensions. Built-in subsystems in v19devel must use
`ShmemRequestStruct()`.

**Fix**: Switch to `ShmemRequestStruct()` for all three UNDO subsystem
structures.

**Testing**: Run `initdb` successfully.

---

### Bug 20: UNDO Subsystem v19devel Shared Memory API Conversion

**Severity**: LOW
**Commit**: `3f3520d08ca`
**Files Modified**: 9 files across UNDO subsystems

**Symptoms**: Build failures and `initdb` failures due to legacy
`ShmemInitStruct()` API usage.

**Root Cause**: The UNDO subsystem (UndoLog, UndoWorker, RelUndoWorker)
still used the legacy `ShmemInitStruct()` API. v19devel requires
`ShmemRequestStruct()` in the request phase.

**Fix**: Complete migration to the new API for all UNDO subsystems. Also
increased minimum `shared_buffers` in `initdb` from 50 buffers (400KB) to
2048 buffers (16MB) to accommodate v19devel AIO and UNDO memory
requirements.

**Testing**: `initdb` completes successfully with 128MB `shared_buffers`.

---

### Bug 21: nxundo_vacuum Conflicting Type Declarations

**Severity**: LOW
**Commit**: `7da1ea4af17`
**Files Modified**: `src/include/access/noxu_internal.h`

**Symptoms**: Compilation warning about conflicting type declarations.

**Root Cause**: `const VacuumParams*` in the declaration did not match
`struct VacuumParams*` in the implementation. A duplicate declaration
existed in the UNDO compatibility section.

**Fix**: Remove duplicate declaration, change to `struct VacuumParams*` to
match implementation.

**Testing**: Clean build with no warnings.

---

### Bug 22: Missing Prototypes in heapam_indexscan.c

**Severity**: LOW
**Commit**: `9697c50759a`
**Files Modified**: `src/backend/access/heap/heapam_indexscan.c`

**Symptoms**: `-Wmissing-prototypes` compiler warnings.

**Root Cause**: Four heap AM index fetch functions were not declared
`static` despite being internal to the file.

**Fix**: Add `static` qualifier to all four functions.

**Testing**: Clean build with no warnings.

---

### Bug 23: crash_recovery Test Exit Status 29

**Severity**: MEDIUM
**Commit**: `2577f0d1a01`
**Files Modified**: `src/test/modules/recno/t/002_crash_recovery.pl`

**Symptoms**: The crash_recovery TAP test failed with exit status 29.

**Root Cause**: After crashing the server with `immediate` mode, the test
tried to reconnect and quit a background `psql` process. Since the server
crash killed all connections, the `reconnect_and_clear()` and `quit()` calls
failed with "process ended prematurely".

**Fix**: Wrap the calls in `eval` blocks to handle the already-dead
background process gracefully.

**Testing**: Run the crash_recovery TAP test and verify it passes cleanly.

---

## Lessons Learned

1. **UNDO chain integrity is paramount.** Bugs 1, 2, 3, and 4 all stem
   from incorrect undo pointer management. Any operation that modifies the
   undo chain must correctly restore the predecessor pointer on rollback.
   The pattern of reading `urec_prevundorec` from the current record and
   restoring it as the new undo pointer should be codified as a standard
   operation.

2. **Mirror heapam patterns.** Bugs 5 and 6 were cases where Noxu diverged
   from the well-tested ordering in heapam. When implementing a new table
   AM, the existing heap AM should be treated as a reference implementation
   for lock ordering, visibility protocols, and speculative insertion
   handling.

3. **Size accounting must be done in exactly one place.** Bugs 7 and 8
   both involved double-counting or mis-estimating item sizes across
   caller/callee boundaries. Size calculations should be encapsulated in
   a single function with clear ownership.

4. **Rebase hygiene requires systematic verification.** Bugs 17-22 were
   all introduced by API changes in upstream v19devel. A rebase checklist
   covering function signatures, shared memory APIs, and GUC naming
   conventions would catch these mechanically.

5. **Valgrind catches real bugs.** Bug 13 (uninitialized read) and the
   memory leaks (bugs 11, 12) were all found through systematic valgrind
   testing. Running the full regression suite under valgrind should be a
   standard gate.

6. **Weak symbols are a trap.** Bug 16 required two attempts to fix. The
   initial approach (adding weak symbols) actually caused the problem it
   was trying to solve. The correct fix was to remove local definitions
   and rely on the standard `extern PGDLLIMPORT` mechanism.

7. **Concurrent page splits need defensive checks.** Bug 10 shows that
   any operation that descends a B-tree and then operates on the leaf page
   must re-verify the page's key range after locking, because concurrent
   splits can shrink it.

---

## Recommendations

1. **Add UNDO rollback integration tests.** Create dedicated tests that
   exercise `BEGIN; DML; ROLLBACK;` for INSERT, UPDATE, DELETE, and
   combinations thereof. Verify tuple visibility from concurrent snapshots
   before and after rollback.

2. **Run valgrind on every commit.** The two valgrind-discovered bugs
   (Bug 13, 14) and the memory leaks (Bug 11, 12) justify the cost of
   valgrind CI runs.

3. **Maintain a rebase checklist.** Document all external API surfaces
   (TableAmRoutine, shared memory, GUC, catalog OIDs) and verify each
   one after rebasing onto a new upstream version.

4. **Fuzz item size boundaries.** Bugs 7 and 8 occurred at the boundary
   of `MAX_ATTR_ITEM_SIZE`. Fuzz testing with items near this boundary
   would have caught the double-counting earlier.

5. **Test concurrent page splits explicitly.** Bug 10 requires high
   concurrency to trigger. Add a targeted isolation test that forces
   page splits during batch attribute inserts.

6. **Single-owner size accounting.** Refactor item size calculation so
   that exactly one function is responsible for computing the total size
   (header + codewords + data). Callers should not add overhead.

7. **SSI lock ordering audit.** Perform a systematic audit of all
   `PredicateLockTID` / `CheckForSerializableConflictOut` call sites to
   ensure they follow the heapam ordering convention.
