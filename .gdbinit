# HOT Indexed Updates — GDB breakpoints for code review
#
# Usage:  gdb -x .gdbinit <postgres-binary>
# Or from gdb:  source .gdbinit
#
# These breakpoints cover the major code paths introduced or modified by
# the HOT indexed updates patch series.  They are organized by subsystem
# to make it easy to enable/disable groups during debugging.
#
# Tip: to skip to a specific subsystem, disable all then enable selectively:
#   disable breakpoints
#   enable 1 2 3          # just the update-decision group

# =========================================================================
# 1. UPDATE DECISION — heap_update() HOT/HOT-indexed/non-HOT choice
#    src/backend/access/heap/heapam.c
# =========================================================================

# Main entry: heap_update
break heapam.c:3210

# HOT decision block: pure HOT vs HOT indexed vs non-HOT
# Line 4019: pure HOT (no indexed columns changed)
# Line 4024: HOT indexed path (non-catalog, some indexed columns changed)
# Line 4031: predict augmented tuple size
# Line 4033: size+space check before creating augmented tuple
break heapam.c:4019
break heapam.c:4024
break heapam.c:4033

# Set HEAP_INDEXED_UPDATED flag on new tuple before page insertion
break heapam.c:4101

# Restore HEAP_INDEXED_UPDATED on old tuple (only if it previously had it)
break heapam.c:4147

# =========================================================================
# 2. TUPLE CREATION — building the augmented tuple with embedded bitmap
#    src/backend/access/heap/heapam.c
# =========================================================================

# Predict augmented tuple size (returns 0 if t_hoff would overflow)
break heap_hot_indexed_tuple_size

# Create augmented tuple with embedded modified-column bitmap
break heap_hot_indexed_create_tuple

# Serialize Bitmapset into raw bytes in tuple header
break heap_hot_indexed_serialize_bitmap

# =========================================================================
# 3. BITMAP UTILITIES — raw bitmap operations for chain following
#    src/backend/access/heap/heapam.c
# =========================================================================

# Compute raw bitmap byte size from natts
break heap_hot_indexed_bitmap_raw_size

# Check if tuple header has room for bitmap between null bitmap and data
break heap_hot_indexed_has_bitmap_space

# Read HOT indexed bitmap from tuple header (returns Bitmapset)
break heap_hot_indexed_read_bitmap

# Fast overlap check: does tuple's raw bitmap overlap with indexed_attrs?
break heap_hot_indexed_bitmap_overlaps_raw

# OR a tuple's raw bitmap into an accumulator buffer
break heap_hot_indexed_bitmap_or_raw

# Check if accumulated raw bitmap overlaps with indexed_attrs
break heap_hot_indexed_accum_overlaps

# Merge bitmaps from dead tuples into a target tuple on the page
break heap_hot_indexed_merge_bitmaps_raw

# Deserialize raw bytes back to Bitmapset
break heap_hot_indexed_deserialize_bitmap

# =========================================================================
# 4. INDEX SCAN — HOT chain following with stale-entry detection
#    src/backend/access/heap/heapam_indexscan.c
# =========================================================================

# Main HOT chain search with indexed update awareness
break heap_hot_search_buffer

# Redirect-with-data: initialize bitmap accumulator from collapsed redirect
break heapam_indexscan.c:182

# Accumulate bitmap from INDEXED_UPDATED tuple in chain
break heapam_indexscan.c:250

# Stale entry detection: accumulated bitmap overlaps this index's attrs
break heapam_indexscan.c:297

# =========================================================================
# 5. INDEX SCAN SETUP — indexed_attrs bitmap computation
#    src/backend/access/index/indexam.c
# =========================================================================

# Compute indexed_attrs for HOT indexed update chain following
break indexam.c:299

# =========================================================================
# 6. INDEX INSERTION — skip unchanged indexes for HOT indexed updates
#    src/backend/executor/execIndexing.c
# =========================================================================

# Entry: insert/update index tuples
break ExecInsertIndexTuples

# Index skip decision: skip indexes whose attrs don't overlap modified set
break execIndexing.c:370

# =========================================================================
# 7. PRUNING — chain collapsing and redirect-with-data
#    src/backend/access/heap/pruneheap.c
# =========================================================================

# Main prune function
break heap_page_prune_and_freeze

# Per-chain pruning entry
break heap_prune_chain

# Chain collapsing: collect bitmaps from dead INDEXED_UPDATED intermediates
break pruneheap.c:1802

# OR dead tuple bitmaps into combined bitmap
break pruneheap.c:1836

# Record redirect-with-data for execute phase
break pruneheap.c:1863

# Execute phase: apply redirect-with-data entries on the page
break pruneheap.c:1287

# =========================================================================
# 8. WAL REPLAY — recovery of HOT indexed updates
#    src/backend/access/heap/heapam_xlog.c
# =========================================================================

# WAL replay for XLOG_HEAP2_INDEXED_UPDATE
break heap_xlog_indexed_update

# =========================================================================
# 9. WAL LOGGING — writing HOT indexed update records
#    src/backend/access/heap/heapam.c
# =========================================================================

# WAL logging for heap updates (handles indexed_update flag)
break log_heap_update

# Serialize redirect-with-data into WAL record (pruneheap.c)
break pruneheap.c:2936
