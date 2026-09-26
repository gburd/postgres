# nbtree + hash index UNDO — design notes

**Status: historical, and superseded in its central decision.** This file is the
forward-looking plan ("Phase 3") for index UNDO.  Its description of the *apply*
path (the pluggable rmgr, `rm_undo` callbacks keyed by `urec_rmid`) is still
accurate.  Its description of how the *write* path is selected is not, and the
difference matters, so read this note before the body:

* The plan routes index UNDO on the parent table AM's UNDO engine, via a
  three-valued `UndoEngine { NONE, FORK, PERBACKEND }`.  That is not what
  shipped.  `UNDO_ENGINE_FORK` does not exist: the enum is `{ UNDO_ENGINE_NONE,
  UNDO_ENGINE_PERBACKEND }`, because the per-relation fork engine was removed and
  per-backend is the only store a table AM may declare.
* Index UNDO does **not** route on the table AM's engine at all.  It is gated by
  `RelationUsesIndexUndo(indexrel, heaprel)`, a per-RELATION property driven by
  the `index_undo` reloption on the table (default **on**), whose in-tree
  consumer is plain **heap**.  The gate refuses a delete-marking parent (such an
  AM reverses its own index changes from its table UNDO, so a second record would
  double-apply), non-WAL relations, and indexes created in the current
  transaction.
* Consequently the "DORMANT until Phase 9/10" section below is wrong in both
  directions: the per-backend index-UNDO write arms are never reached for the
  in-tree AMs (both set `am_index_delete_marking`), while the cluster-wide path
  *is* reachable and exercised, because its consumer is heap.

For what shipped, read the two `INDEX: ... UNDO for rollback without VACUUM`
commit messages, which also document the three defects that had to be fixed
before any of it worked.

---

## Current state (what exists)
The nbtree/hash index UNDO handlers already use the AM-agnostic pluggable rmgr
API: they register via RegisterUndoRmgr (UNDO_RMID_NBTREE=1, UNDO_RMID_HASH=3),
their rm_undo(rmid,info,xid,reloid,payload,payload_len,UndoRecPtr) callbacks are
generic, and UndoRecPtr is already the shared uint64 (undodefs.h).

BUT the WRITE side is bound to the cluster-wide UNDO-in-WAL / per-relation-fork
engine: NbtreeUndoLogInsert/HashUndoLogInsert call
UndoRecordSetCreate(xid, GetCurrentTransactionUndoRecPtr()) which chains off the
fork engine's TransactionState.undoRecPtr and is applied on abort by
undoapply.c's ApplyUndoChainFromWAL (WAL path). Gated by
RelationAmSupportsUndo(heaprel) on the parent table.

## Constraint (coexistence)
FLUX (still on the fork engine until Phase 8) parents indexes whose index UNDO
shares the TABLE transaction's UNDO context. The flux regress test exercises
key-change ROLLBACK restoring the old index entry via this path. Moving index
UNDO unconditionally to per-backend NOW would split a single FLUX xact's undo
across two engines and break that test.

## Phase 3 design: engine-selectable index UNDO
Route the index-UNDO write path to the SAME engine the parent table AM uses:
  - Add a table-AM capability distinguishing "uses per-backend undo" from
    "uses fork undo" from "no undo" (extend the existing RelationAmSupportsUndo /
    amflags mechanism; e.g. RelationUndoEngine(rel) -> {NONE, FORK, PERBACKEND}).
  - Index UNDO write:
      FORK       -> UndoRecordSetCreate(xid, GetCurrentTransactionUndoRecPtr())  [unchanged]
      PERBACKEND -> PrepareUndoInsert/InsertPreparedUndo chaining off pbuUndoLatestPtr,
                    tagged with UNDO_RMID_NBTREE / UNDO_RMID_HASH.
  - Apply: unchanged rm_undo callbacks. Fork undo applied by ApplyUndoChainFromWAL;
    per-backend undo applied by execute_undo_actions (Phase 2b dispatch, already
    routes uur_rmid -> GetUndoRmgr->rm_undo). Both call the SAME nbtree/hash
    rm_undo, so the apply logic is shared.
During coexistence: FLUX=FORK (flux test stays green); ZHEAP/RECNO (Phase 9/10)
declare PERBACKEND. Phase 8 flips FLUX to PERBACKEND and the FORK branch retires
with the fork engine.

## Deliverables
- RelationUndoEngine (or equivalent) capability + wiring.
- nbtree_undo.c / hash_undo.c write path selects engine; apply unchanged.
- Tests: FLUX index-rollback regress stays green (FORK path). Add a per-backend
  index-UNDO apply test once a per-backend AM exists (defer real coverage to
  Phase 9/10; add a targeted unit/TAP if feasible now with a synthetic producer).
- Docs: update the index-UNDO README/comments to describe engine selection.

## Attribution: Greg Burd (index UNDO handlers + engine routing).

## Phase 3 status: DELIVERED (engine-selectable index UNDO write path)

Implemented on top of the existing pluggable-rmgr apply path.  Summary of the
wiring as built:

### Capability API (src/include/access/tableam.h, tableam.c)
  typedef enum UndoEngine { UNDO_ENGINE_NONE, UNDO_ENGINE_FORK,
                            UNDO_ENGINE_PERBACKEND };
  TableAmRoutine.am_undo_engine  -- new field, default 0.
  UndoEngine RelationUndoEngine(Relation rel);

  Mapping (RelationAmSupportsUndo() kept working, unchanged):
    heap  (am_supports_undo = false)                       -> NONE
    FLUX  (am_supports_undo = true, am_undo_engine unset=0) -> FORK   (unchanged)
    future per-backend AM (am_undo_engine = PERBACKEND)     -> PERBACKEND

  FLUX is untouched: with am_undo_engine left 0, an am_supports_undo=true AM
  maps to FORK, so FLUX's behavior and the flux regress test are unaffected.

### Write-path routing (nbtree_undo.c, hash_undo.c, hashinsert.c)
  Each index-UNDO write point (nbtree leaf/upper insert, nbtree dedup, hash
  insert) now begins with:
      if (RelationUndoEngine(heaprel) == UNDO_ENGINE_PERBACKEND) {
          <AM>_undo_write_perbackend(...); return;
      }
      /* FORK engine (default, incl. FLUX): unchanged. */
      ... original UndoRecordSetCreate(xid, GetCurrentTransactionUndoRecPtr())
          / UndoBufferAddRecordParts() code, byte-for-byte ...
  hashinsert.c's write gate additionally allows the PERBACKEND branch through
  (it must not require the FORK-only active shared UNDO buffer).

### Apply path (unchanged)
  rm_undo callbacks (nbtree_undo_apply / hash_undo_apply) are untouched.  FORK
  undo is applied by undoapply.c's WAL path; per-backend undo by
  execute_undo_actions() -> GetUndoRmgr(uur_rmid) -> rm_undo.  Both reach the
  SAME callback, so apply logic is shared.

### DORMANT until Phase 9/10 (deliberate, documented in-code)
  No per-backend table AM exists yet, so RelationUndoEngine() never returns
  PERBACKEND and the <AM>_undo_write_perbackend() helpers are unreachable at
  runtime.  They compile and fail-closed (elog ERROR) rather than silently drop
  index UNDO.  Two pieces are deferred to Phase 9/10, where a real per-backend
  AM can exercise them end to end (see the notes in nbtree_undo.c):
    1. The full PrepareUndoInsert/InsertPreparedUndo WAL choreography.  It must
       live in a small pbu-side shim TU, because the per-backend headers
       (pbu_undorecord.h/pbu_undoinsert.h) redefine UndoRecordHeader /
       UndoLogControl and use the pre-RelFileLocator RelFileNode type, so they
       cannot be included in the nbtree/hash TUs that already pull in the fork
       engine's access/undorecord.h.  The helper documents the exact record
       contract (uur_rmid, uur_type=subtype, uur_reloid, payload) and the
       SetCurrentTransactionPbuUndoLocation() chain hook the shim must honor.
    2. execute_undo_actions_page() currently passes rec->uur_info (UREC_INFO_*
       flag bits) as the apply callback's "info" arg, but the nbtree/hash
       subtype lives in uur_type.  Reconciling that is a one-line pbu-engine
       dispatch fix (out of scope for Phase 3, which must not modify pbu
       internals); the write side already stores the subtype in uur_type.

### Tests
  FORK path: flux regress test stays green (ok 1) -- exercises key-change
  ROLLBACK restoring the old index entry through RelationUndoEngine()==FORK.
  fileops: ok 1.  isolation: all 135 ok.
  PERBACKEND path: no synthetic test added -- it is provably unreachable
  without a per-backend table AM; real coverage lands in Phase 9/10 with the
  shim from item (1) above.
