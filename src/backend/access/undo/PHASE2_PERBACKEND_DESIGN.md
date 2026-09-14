# Phase 2: Per-backend UNDO logs (zheap model) — design

## Goal
Replace the cluster-wide UNDO-in-WAL / per-relation-fork engine with per-backend
undo logs, matching zheap. Preserve the two upper layers the current tree already
has and that downstream AMs depend on:
  - the pluggable UNDO resource-manager API (RegisterUndoRmgr/GetUndoRmgr,
    undormgrlist.h) used by FILEOPS/nbtree/hash/flux,
  - AM-agnostic Tier-2 buffer helpers + robust subtransaction tracking.

## What is removed (per-relation fork era)
relundo.c, relundo_apply.c, relundo_discard.c, relundo_page.c,
relundo_recovery.c, relundo_worker.c, relundo_xlog.c, atm.c (+ headers).
The RelUndoRecPtr type (counter|blkno|offset into a per-relation fork) goes away.

## What is added (per-backend, from edb/zheap, drift-reconciled to master)
undolog.c    — per-backend undo log lifecycle, UndoLogControl banks, file-backed
               segments under base/undo, 64-bit UndoRecPtr = 24-bit logno|40-bit off.
undoinsert.c — PrepareUndoInsert / InsertPreparedUndo / UndoFetchRecord.
undorecord.c — pack/unpack UnpackedUndoRecord.
undodiscard.c + discardworker.c — background discard of undo no snapshot needs.
undoaction.c — rollback apply (execute_undo_actions), CLR-style.
undorequest.c — rollback request queue (RollbackHT).

## API-drift reconciliation (2019 zheap -> current master)
- RelFileNode -> RelFileLocator (PG16). Buffer/smgr/WAL prototypes evolved.
- shmem registration, background-worker registration, checkpoint hooks.
- UndoPersistence (PERMANENT/UNLOGGED/TEMP) retained.

## Downstream contract change (the hard part; consumed in later phases)
FLUX today calls RelUndoReserve/Stage/Finish/ReadRecord/ApplyChain and stores
RelUndoRecPtr t_verptr per tuple. Per-backend UNDO changes the pointer TYPE
(UndoRecPtr uint64) and storage (per-backend log, not per-relation fork). Phase 8
(FLUX rework) migrates FLUX to PrepareUndoInsert/UndoFetchRecord + a uint64 verptr.
Index UNDO apply (Phase 3) and sLog (Phase 4-rework) likewise rebase.

## Sequencing within Phase 2
2a. Vendor zheap undo files, reconcile compile-level drift (RelFileLocator etc.)
    so the engine builds standalone (not yet wired to any AM).
2b. Wire the pluggable rmgr + subxact + Tier-2 AM-agnostic layers onto it.
2c. COEXISTENCE (revised decision -- no shim): keep the OLD relundo*.c engine
    physically present and building during Phases 2-7. FLUX keeps calling the real
    RelUndo* API unchanged, so every commit builds + tests green. The new
    per-backend engine is added alongside (new names). No compat shim -- a shim with
    one caller is hidden Phase-8 work under old names; avoid it. Phase 8 migrates FLUX
    to the native per-backend API (UndoRecPtr verptr, PrepareUndoInsert/UndoFetchRecord)
    AND removes relundo*.c/atm.c as part of that same commit cleanup. ZHEAP (Phase 9)
    and RECNO (Phase 10) are written natively against per-backend from the start.
2d. undo-log unit tests + rollback + crash-recovery + discard-under-load.

## Attribution
Per-backend undo engine derives from EnterpriseDB zheap
(Amit Kapila, Dilip Kumar, Kuntal Ghosh, Mahendra Singh Thalor, Rafia Sabih,
Thomas Munro et al.). Adaptation/AM-agnostic layer/subxact: Greg Burd.
