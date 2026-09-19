# Phase 5: Delete-marking in nbtree indexes

## Goal
Let an in-place-MVCC table AM UPDATE an indexed column WITHOUT allocating a new
TID: instead of delete+insert-at-new-TID, the AM inserts the NEW (key,TID) index
entry and DELETE-MARKS the OLD (key,TID) entry in place. The stable TID is
preserved; the old entry lingers as a tombstone until the old version is
undiscoverable, then VACUUM removes it. zheap's README names this as the
mechanism it intended but never implemented; it is what unlocks fully in-place
UPDATE (Phases 8/9/10 use it).

## On-disk representation (CORRECTED)
There is NO free flag bit in a standard non-pivot nbtree leaf tuple: it stores
its heap TID directly in t_tid with INDEX_ALT_TID_MASK UNSET, and t_info is full
(SIZE_MASK 0x1FFF | ALT_TID 0x2000 | VAR 0x4000 | NULL 0x8000). This is exactly
why zheap never implemented delete-marking.

Solution: represent a delete-marked leaf entry as an ALT-TID non-posting tuple,
reusing nbtree's sanctioned t_tid-redefinition mechanism. Set INDEX_ALT_TID_MASK
in t_info and a NEW status bit in the BT_STATUS_OFFSET_MASK (0xF000) range of
t_tid's offset number:
    #define BT_IS_DELETE_MARKED   0x4000   /* free; 0x1000=PIVOT_HEAP_TID,
                                              0x2000=IS_POSTING already taken */
The entry's single heap TID moves into the alt-TID heap-TID trailer (as a pivot
tuple carries its heap TID), retrieved via BTreeTupleGetHeapTID. Accessors:
    BTreeTupleIsDeleteMarked(itup):
        (t_info & INDEX_ALT_TID_MASK) && !IsPosting &&
        (ItemPointerGetOffsetNumberNoCheck(&t_tid) & BT_IS_DELETE_MARKED)
    BTreeTupleSetDeleteMarked(itup, heaptid): promote to alt-TID form, set the
        status bit, store heaptid in the trailer. WAL-logged.
    BTreeTupleClearDeleteMarked: demote back to plain leaf tuple (or leave for
        VACUUM).
CAUTION: BTreeTupleIsPivot must still return false for a delete-marked leaf
(it has ALT_TID set) -- update BTreeTupleIsPivot/IsPosting so the DELETE_MARKED
status is recognized as a third alt-TID subtype (pivot | posting | delete-marked)
and non-pivot classification stays correct on the leaf. Verify every
BTreeTupleIs{Pivot,Posting}/GetHeapTID/GetNAtts call site handles the new subtype.
This is the delicate part: audit all alt-TID readers.

## Correctness invariants (MUST hold)
I1 (visibility completeness): a snapshot that can see version V of a row must be
   able to REACH V through every index whose key equals V's key. When an indexed
   column changes k_old->k_new in place at TID T:
     - INSERT a live entry (k_new, T) so new-snapshot readers find T under k_new.
     - DELETE-MARK the old entry (k_old, T) but KEEP it, so old-snapshot readers
       (who see the before-image via UNDO) still find T under k_old.
   Neither entry is removed until no snapshot needs the corresponding version
   (VACUUM + undo horizon).
I2 (no false hits): an index scan must not return T under k_old to a reader who
   should see k_new. Enforced by RECHECK: a scan that returns a heap TID reached
   via a delete-marked entry sets xs_recheck, and the executor re-evaluates the
   qual against the visible version. (FLUX already sets recheck for in-place
   UPDATE tuples in bitmap scans; extend to index scans through delete-marked
   entries.)
I3 (uniqueness): _bt_check_unique must NOT treat a delete-marked entry as a live
   duplicate (its key no longer describes the live tuple), and must NOT miss a
   real duplicate. Rule: skip delete-marked entries when judging uniqueness of a
   NEW key; the live (k_new,T) entry is the authority. A live entry whose TID
   points at a tuple whose CURRENT key != the index key is treated as
   already-delete-marked-in-progress -> consult the live tuple (heap-fetch) to
   decide, exactly as heap does for in-progress updates.
I4 (index-only scans): a delete-marked entry's key may not match the live tuple,
   so IOS cannot trust it. Options: (a) IOS must visit the table + recheck for
   delete-marking AMs, or (b) disable IOS (amcanreturn=false) for indexes on
   delete-marking tables. v1 DECISION: (a) -- force recheck/visit for entries
   from a delete-marking-capable index; keep IOS available for non-marked entries
   via the VM as usual. If (a) proves too invasive, fall back to (b).
I5 (VACUUM/undo horizon): a delete-marked (k_old,T) tombstone is removable only
   when no snapshot can still need the before-image at T under k_old -- i.e. the
   UNDO for that key change is discardable. Reconcile with the existing
   undo-informed index pruning (index_prune) + the per-AM VACUUM. A live
   (k_new,T) entry is removable only when T itself is dead (normal rule).

## Scan/read path
- _bt_readpage / _bt_checkkeys: when returning a heap TID from a delete-marked
  entry, set scan->xs_recheck = true (so amgetnextslot/bitmap consumer rechecks).
  Do NOT skip the entry outright (old-snapshot readers legitimately need it);
  visibility is decided by the table AM's fetch, not by the index.
- kill_prior_tuple / LP_DEAD: a delete-marked entry is NOT dead; never set LP_DEAD
  on it based on the marking alone.

## Write path (who sets the mark)
The TABLE AM drives it during in-place indexed-column UPDATE:
  1. aminsert the new (k_new, T) entry (heap-style).
  2. call a new index-AM entry point to delete-mark the old (k_old, T) entry:
     index_delete_mark(indexRel, k_old, T) -> nbtree btdeletemark: descend by
     (k_old,T), set INDEX_AM_RESERVED_BIT on the exact matching entry, WAL-log it.
     Re-descend by key+TID (like the Phase-3 undo handler) so a concurrent split
     doesn't mark the wrong entry.
  This is gated to delete-marking-capable AMs (a new tableam capability, or reuse
  RelationUndoEngine != NONE + a new amflag am_index_delete_marking).

## WAL + redo + amcheck
- New nbtree WAL record XLOG_BTREE_DELETE_MARK (offset + set/clear) or piggyback
  on an existing tuple-rewrite record; redo re-sets the bit. wal_consistency_checking
  must mask nothing new (the bit is deterministic).
- amcheck (verify_nbtree): teach bt_index_check that a delete-marked entry whose
  key mismatches the heap tuple is EXPECTED (not corruption) for delete-marking
  indexes; still verify heap TID validity + key ordering.

## Testing (heavy -- this is the risky phase)
- Unit: set/clear bit round-trips; posting-list handling.
- Regress (new sql/index_deletemark): create a delete-marking-capable test
  scaffold (a test hook or reuse FLUX once Phase 8 wires it) exercising:
  in-place key UPDATE keeps TID; old+new snapshots see the right versions through
  the index; uniqueness accepts k_new and rejects a real dup; IOS rechecks.
- Isolation: concurrent key UPDATE vs reader vs unique insert vs VACUUM.
- Recovery TAP: crash mid-key-update, recover, amcheck clean, both versions
  reachable.
- amcheck: bt_index_check + bt_index_parent_check clean on a delete-marked index.

## Phasing note
v1 lands the nbtree mechanism + capability + recheck + unique + IOS + VACUUM +
WAL + amcheck, with a MINIMAL synthetic exerciser (test hook) since no in-place
AM uses it until Phase 8 (FLUX) / 9 (ZHEAP) / 10 (RECNO). Hash indexes: defer
delete-marking (hash rarely used for the target workloads); document as future.

## Attribution: Greg Burd. (Completes the index-side mechanism zheap's README
## described but never implemented.)
