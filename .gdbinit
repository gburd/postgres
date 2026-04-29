# HOT-indexed updates (HOT/SIU) — GDB breakpoints for code review
#
# Usage:  gdb -x .gdbinit <postgres-binary>
# Or from gdb:  source .gdbinit
#
# These breakpoints cover the major code paths of the HOT-indexed updates
# patch series, organized by subsystem so groups can be enabled/disabled
# during debugging.  They are set on FUNCTION NAMES (not line numbers) so
# they survive code churn; set finer-grained line breakpoints by hand once
# you are stopped in the relevant function.
#
# Tip: to focus on one subsystem, disable all then enable selectively:
#   disable breakpoints
#   enable 1 2 3

# =========================================================================
# 1. UPDATE DECISION — heap_update() HOT / HOT-indexed / non-HOT choice
#    src/backend/access/heap/heapam.c
# =========================================================================

# Main entry: heap_update (chooses classic-HOT, HOT-indexed, or non-HOT)
break heap_update

# Eligibility classifier: HEAP_HOT_MODE_NO / _CLASSIC / _INDEXED
break HeapUpdateHotAllowable

# Identify the modified indexed attributes from old/new heap tuples
# (the simple_heap_update / catalog-update path; the executor path uses
# ExecUpdateModifiedIdxAttrs, see group 4)
break HeapUpdateModifiedIdxAttrs

# =========================================================================
# 2. ON-DISK FORMAT — building the new tuple with the inline trailing bitmap
#    src/backend/access/heap/heapam.c, src/include/access/hot_indexed.h
# =========================================================================

# Form the stored heap-only tuple carrying the inline modified-attrs bitmap
break heap_form_hot_indexed_tuple

# The bitmap accessors/stub predicate are static inlines in hot_indexed.h
# (HotIndexedGetModifiedBitmap, HotIndexedBitmapUnion, HotIndexedHeaderIsStub,
# HotIndexedStubGetForward); break on their callers below rather than here.

# =========================================================================
# 3. WRITE PATH — executor side: which indexed attrs changed, which indexes
#    to maintain.  src/backend/executor/{nodeModifyTable,execTuples,execIndexing}.c
# =========================================================================

# Compute modified_idx_attrs for an UPDATE (executor path)
break ExecUpdateModifiedIdxAttrs

# Attribute-by-attribute old-vs-new comparison over the indexed-attr set
break ExecCompareSlotAttrs

# Mark each index unchanged/needs-update ahead of ExecInsertIndexTuples
break ExecSetIndexUnchanged

# Insert index tuples; skips indexes whose attrs did not change (HOT-indexed)
break ExecInsertIndexTuples

# =========================================================================
# 4. READ PATH — HOT chain walk and crossed-attribute staleness
#    src/backend/access/heap/heapam_indexscan.c, access/index/indexam.c
# =========================================================================

# table AM index fetch (takes the buffer share-lock, calls the chain walk)
break heapam_index_fetch_tuple

# Chain walk: unions each crossed hop's/stub's modified-attrs bitmap and
# arms xs_hot_indexed_recheck
break heap_hot_search_buffer

# index_fetch_heap: bitmap-overlap staleness verdict (xs_hot_indexed_stale)
# and the kill_prior_tuple decision (incl. stale-leaf kill)
break index_fetch_heap

# Per-relation/per-index indexed-attr set used by the overlap test
break RelationGetIndexedAttrs

# =========================================================================
# 5. UNIQUE / EXCLUSION — value-confirmed stale-entry skip
#    src/backend/access/nbtree/nbtinsert.c, executor/execIndexing.c
# =========================================================================

# btree unique check: SnapshotDirty fetch + crossed-hop recheck
break _bt_check_unique

# Opclass-correct key comparison of the live tuple vs the arriving leaf
# (the value confirmation behind the bitmap verdict for unique inserts)
break _bt_heap_keys_equal_leaf

# Exclusion / unique constraint check (uses index_recheck_constraint to
# value-confirm a stale chain hit)
break check_exclusion_or_unique_constraint

# =========================================================================
# 6. PRUNE / COLLAPSE — dead-chain collapse to xid-free stubs
#    src/backend/access/heap/pruneheap.c
# =========================================================================

# Prune+freeze entry (forward path)
break heap_page_prune_and_freeze

# Per-chain pruning: collapses a dead prefix, steps through stubs
break heap_prune_chain

# Find the first live tuple of a (possibly stubbed) chain; drives the
# root-redirect re-point that collapses back to classic HOT
break heap_prune_chain_find_live

# Apply the planned prune actions on the page (shared forward + redo path;
# writes the stub headers/forward links)
break heap_page_prune_execute

# =========================================================================
# 7. VACUUM — all-visible suppression for HOT-indexed chains and stubs
#    src/backend/access/heap/vacuumlazy.c
# =========================================================================

# Decides all-visible; keeps a page non-all-visible if it carries a live
# HOT-indexed member, a redirect-to-SIU, or a collapse-survivor stub
break heap_page_would_be_all_visible

# =========================================================================
# 8. WAL — logging and replay (reuses existing UPDATE and prune/freeze
#    records; no new record type).  src/backend/access/heap/heapam*.c
# =========================================================================

# Log a heap update (the on-page heaptup carries the bitmap; flag is logged)
break log_heap_update

# Replay of UPDATE records (HOT and non-HOT)
break heap_xlog_update

# Replay of prune/freeze records (carries the stub (offset, forward) pairs)
break heap_xlog_prune_freeze

# =========================================================================
# 9. STATISTICS — per-relation HOT-indexed chain shape
#    src/backend/access/heap/hot_indexed_stats.c
# =========================================================================

# pg_relation_hot_indexed_stats(regclass)
break pg_relation_hot_indexed_stats
