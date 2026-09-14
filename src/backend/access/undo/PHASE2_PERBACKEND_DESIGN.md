# Per-backend UNDO logs (zheap model) — design notes

**Status: historical.** This file records the design of the per-backend UNDO
engine and the reasoning behind it.  It was written as a forward-looking plan
("Phase 2") and its plan-shaped statements no longer describe the tree.  Two in
particular were not carried out as written, so do not read them as current:

* The plan said the per-backend engine would **replace** the cluster-wide
  UNDO-in-WAL engine, and that `atm.c` would be deleted.  It did not.  Both
  ship: `undolog.c`, `undorecord.c`, `undoapply.c`, `undo_xlog.c` and `atm.c`
  are all present and in use, because the two stores serve different consumers
  (see `README`, "The UNDO Store").  What *was* removed is the third design, the
  per-relation UNDO fork.
* The sequencing and gating described below (coexistence stages 2a-2d, a later
  "Phase 8" FLUX migration) is how the work was organized at the time, not a
  description of the shipped commits.  For what the shipped series contains, read
  the commit messages.

The engine itself, and the attribution, are accurate.

## Why per-backend

Each backend appends to its own log, so the hot path has no shared
insertion-point contention.  Undo is discarded once no snapshot needs it, by a
background discard worker.  This suits a high-churn in-place-update table AM,
where UNDO volume is dominated by rows being updated repeatedly.  It is the only
store a table AM may declare (`am_undo_engine = UNDO_ENGINE_PERBACKEND`).

Two upper layers are shared with the cluster-wide store and are what downstream
consumers actually bind to:

  - the pluggable UNDO resource-manager API (`RegisterUndoRmgr` / `GetUndoRmgr`,
    `undormgrlist.h`) used by FILEOPS, nbtree, hash, FLUX and RECNO;
  - AM-agnostic buffer helpers plus subtransaction tracking.

## What the engine provides

Files live in `src/backend/access/undo/perbackend/` with a `pbu_` prefix:

    pbu_undolog.c       per-backend undo log lifecycle, UndoLogControl banks,
                        file-backed segments under base/undo, 64-bit
                        UndoRecPtr = 24-bit logno | 40-bit offset.
    pbu_undoinsert.c    PrepareUndoInsert / InsertPreparedUndo / UndoFetchRecord.
    pbu_undorecord.c    pack/unpack UnpackedUndoRecord.
    pbu_undodiscard.c   } background discard of undo no snapshot needs.
    pbu_discardworker.c }
    pbu_undoaction.c    rollback apply (execute_undo_actions), CLR-style.
    pbu_undorequest.c   rollback request queue (RollbackHT).
    pbu_recovery.c      crash-recovery re-drive of per-backend undo.
    pbu_undofile.c      segment file access.
    pbu_helpers.c       shared helpers.

A tuple's version pointer is an opaque `UndoRecPtr` (uint64): the read path
fetches a record and follows the chain through the pointer in each reconstructed
record, without decoding log/offset arithmetic.

## API drift reconciled from the original zheap work

- `RelFileNode` -> `RelFileLocator` (PG16); buffer/smgr/WAL prototypes evolved.
- shmem registration (now via `PG_SHMEM_SUBSYSTEM` in `storage/subsystemlist.h`),
  background-worker registration, checkpoint hooks.
- `UndoPersistence` (PERMANENT / UNLOGGED / TEMP) retained.

## Attribution

Per-backend undo engine derives from EnterpriseDB zheap
(Amit Kapila, Dilip Kumar, Kuntal Ghosh, Mahendra Singh Thalor, Rafia Sabih,
Thomas Munro et al.). Adaptation/AM-agnostic layer/subxact: Greg Burd.
