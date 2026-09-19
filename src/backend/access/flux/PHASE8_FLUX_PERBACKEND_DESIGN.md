# Phase 8: FLUX rework onto per-backend UNDO + always-in-place UPDATE

## Magnitude
FLUX = ~21K lines, ~160 RelUndo* call sites, t_verptr (RelUndoRecPtr) embedded
in the on-disk tuple header. This is the deepest coupling in the arc. Done in
GATED STAGES; each stage builds + passes the FLUX suite before the next.

## Good news (scoping)
t_verptr is used mostly OPAQUELY: the hot path reads a record via
RelUndoReadRecord(rel, verptr,...) and follows the chain through a verptr in each
reconstructed record; it does NOT decode fork blkno/offset arithmetic on the read
path. Both RelUndoRecPtr and per-backend UndoRecPtr are uint64. So the migration
is: swap the READ api (RelUndoReadRecord->UndoFetchRecord), the WRITE api
(RelUndoReserve/Stage/Finish -> PrepareUndoInsert/InsertPreparedUndo), and keep
verptr opaque (retype RelUndoRecPtr->UndoRecPtr). On-disk header size is unchanged
(8B either way).

## MANDATORY PREREQUISITE (Stage 8a): per-backend crash-recovery + 2PC
FLUX cannot move to per-backend undo until per-backend undo is CRASH-SAFE and
2PC-safe (FILEOPS dual-writes today precisely because pbu lacks this). So FIRST:
  8a. WAL-log per-backend undo pages (RegisterUndoLogBuffers/UndoLogBuffersSetLSN
      already stubbed in Phase 2/6). Wire CheckPointUndoLogs into CheckPointGuts
      and StartupUndoLogs into recovery (the Phase-2 no-ops). Implement undofile
      exists/extend at recovery. Apply per-backend undo of aborted-at-crash xacts
      during recovery (RollbackHT replay / execute_undo_actions from the recovered
      undo logs). 2PC: persist the per-backend undo location in the 2PC state file
      and apply on ROLLBACK PREPARED.
      Gate: a NEW recovery TAP that crashes with in-flight per-backend undo and
      verifies revert; FILEOPS 054/068 pass with FILEOPS switched to
      per-backend-ONLY (collapse the dual-write) -> proves pbu crash+2PC.

## Stage 8b: FLUX read/write/redo migration
  - Retype t_verptr and all FLUX RelUndoRecPtr locals to UndoRecPtr.
  - Write path: replace RelUndoReserve/Stage/Finish/FinishWithTuple with the
    per-backend insert protocol (PrepareUndoInsert before crit; InsertPreparedUndo
    + buffer LSNs in crit; chain via SetCurrentTransactionPbuUndoLocation). The
    UNDO record carries the before-image tuple (unchanged payload) tagged with a
    FLUX undo rmid (register UNDO_RMID_FLUX descriptor's rm_undo = the existing
    flux reverse-apply).
  - Read path: FluxReconstructVisibleVersion + all RelUndoReadRecord ->
    UndoFetchRecord/UndoRecordRelease; chain-follow via the verptr in each record.
  - Redo: FLUX xlog records that referenced the fork (CAS_UPDATE_UNDO block 1/2)
    now reference per-backend undo; rework flux_xlog.c accordingly. Verify under
    wal_consistency_checking=flux + crash-recover.
  - FLUX declares am_undo_engine = PERBACKEND (Phase-3 capability) so index UNDO
    routes per-backend too, and enables am_index_delete_marking.

## Stage 8c: always-in-place UPDATE via delete-marking
  - Remove flux_update_out_of_place for the indexed-column case: an indexed-column
    UPDATE now stays in place (stable TID), inserts the new index entry, and
    delete-marks the old via index_delete_mark (Phase 5). Wire the Phase-5
    IOS-suppression (btcanreturn) now that a delete-marking AM exists.
  - Keep out-of-place only where genuinely required (e.g. no room on page ->
    same as heap cross-page); document.
  - flux regress line 46 (key-change rollback) must still restore old key+entry,
    now through delete-marked entry + per-backend undo.

## Stage 8d: remove the fork engine + collapse FILEOPS dual-write
  - Delete relundo*.c, atm.c, undo_xlog.c (UNDO-in-WAL), and the xactundo.c
    fork paths FLUX no longer uses. Remove the RM_RELUNDO_ID/RM_UNDO_ID(-in-WAL)
    rmgrs if now unused. Collapse FILEOPS to per-backend-only (drop dual-write).
  - This is the commit that in the FINAL series replaces 'UNDO-in-WAL' at
    position 3 with per-backend; here we do it as the FLUX-rework cleanup.

## Gates (every stage)
flux regress, 135 isolation (incl flux_conflict), flux crash-recovery TAP,
wal_consistency_checking=flux clean, fileops (per-backend-only) 054/068,
delete-marking tests. Full post-recovery workload (the test that caught the
earlier bad redo). amcheck on flux-parented indexes with delete-marks.

## Attribution: Greg Burd (FLUX + per-backend recovery/2PC + delete-marking use).
