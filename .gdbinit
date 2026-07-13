# ============================================================
# UNDO 11-commit series: layered breakpoints (auto-generated)
# Enable a layer with its macro, e.g.  undo-bp-fileops-entry
# '-entry' = callbacks/redo/undo/DML only; '-all' = + helpers.
# ============================================================
set breakpoint pending on

define undo-bp-fileops-entry
  # 7 entry points (fileops)
  break FileOpsAtPrepare
  break FileopsUndoRmgrInit
  break PostPrepare_FileOps
  break fileops_desc
  break fileops_redo
  break fileops_undo_apply
  break fileops_undo_desc
end
define undo-bp-fileops-all
  undo-bp-fileops-entry
  # 29 helpers (fileops)
  break AddPendingFileOp
  break AddPendingFileOpWithData
  break AtSubAbort_FileOps
  break AtSubCommit_FileOps
  break FileOpsCancelPendingDelete
  break FileOpsChmod
  break FileOpsChown
  break FileOpsCreate
  break FileOpsDelete
  break FileOpsDoPendingOps
  break FileOpsExecOne
  break FileOpsLink
  break FileOpsMkdir
  break FileOpsRemoveXattr
  break FileOpsRename
  break FileOpsRmdir
  break FileOpsRmdirRecursive
  break FileOpsRmtree
  break FileOpsSetXattr
  break FileOpsSymlink
  break FileOpsSync
  break FileOpsTruncate
  break FileOpsWrite
  break FreePendingFileOp
  break fileops_fsync_parent
  break fileops_identify
  break fileops_twophase_do
  break fileops_twophase_postabort
  break fileops_twophase_postcommit
end

define undo-bp-undo_core-entry
  # 108 entry points (undo_core)
  break ApplyOneUndoRecord
  break ApplyUndoChainFromWAL
  break ApplyUndoChainFromWALBounded
  break AtAbort_XactUndo
  break AtCommit_XactUndo
  break AtPrepare_XactUndo
  break AtProcExit_Undo
  break AtProcExit_XactUndo
  break AtSubAbort_XactUndo
  break AtSubCommit_XactUndo
  break CheckPointUndoLog
  break CleanupXactUndoInsertion
  break CollapseXactUndoSubTransactions
  break DeferXactUndoData
  break ExtendUndoLogFile
  break ExtendUndoLogSmgrFile
  break FlushDeferredUndoXacts
  break GetCurrentXactUndoRecPtr
  break GetUndoBufferStats
  break GetUndoLogStats
  break GetUndoPersistenceLevel
  break GetUndoRmgr
  break InitUndoRmgrs
  break InitializeUndo
  break InitializeXactUndo
  break InsertXactUndoData
  break InvalidateUndoBufferRange
  break InvalidateUndoBuffers
  break MarkUndoBufferDirty
  break PerformUndoRecovery
  break PrepareXactUndoData
  break PrepareXactUndoDataParts
  break ReadUndoBuffer
  break ReadUndoBufferExtended
  break RegisterUndoRmgr
  break RegisterUndoRmgrs
  break ReleaseUndoBuffer
  break ResetXactUndo
  break UndoClearBatchLSN
  break UndoFlushGetMaxWritePtr
  break UndoFlushResetMaxWritePtr
  break UndoFreeBatchData
  break UndoGetDiscardHorizon
  break UndoGetOldestBatchLSN
  break UndoLogCloseFiles
  break UndoLogDeleteSegmentFile
  break UndoLogDiscard
  break UndoLogGetDiscardPtr
  break UndoLogGetInsertPtr
  break UndoLogGetOldestDiscardPtr
  break UndoLogPath
  break UndoLogSealAndRotate
  break UndoLogShmemInit
  break UndoLogShmemSize
  break UndoLogStateToString
  break UndoLogSync
  break UndoLogTryPressureDiscard
  break UndoMakeBufferTag
  break UndoReadBatchFromWAL
  break UndoRecordAddPayload
  break UndoRecordAddPayloadParts
  break UndoRecordDeserialize
  break UndoRecordEnsureCapacity
  break UndoRecordGetPayloadSize
  break UndoRecordSerialize
  break UndoRecordSetCreate
  break UndoRecordSetFree
  break UndoRecordSetGetNumRecords
  break UndoRecordSetGetSize
  break UndoRecordSetInsert
  break UndoRecordSetReset
  break UndoRecordSetResetCache
  break UndoRecoveryNeeded
  break UndoRecoveryRemoveXid
  break UndoRecoveryTrackBatch
  break UndoRegisterBatchLSN
  break UndoResetBatchReader
  break UndoSetDiscardHorizon
  break UndoShmemAttach_internal
  break UndoShmemInit
  break UndoShmemInit_internal
  break UndoShmemRequest_internal
  break UndoShmemSize
  break UndoValidateBatchLSN
  break UndoWalBatchFlush
  break UndoWalBatchReset
  break UndoWorkerGetOldestXid
  break UndoWorkerMain
  break UndoWorkerRegister
  break UndoWorkerRequestShutdown
  break UndoWorkerShmemInit
  break UndoWorkerShmemSize
  break UnlockReleaseUndoBuffer
  break WakeUndoDiscardWorker
  break XActUndoUpdateLastBatchLSN
  break XactUndoFlushPending
  break XactUndoHasPendingData
  break XactUndoHasUnrecoverableUndo
  break XactUndoShmemInit
  break XactUndoShmemSize
  break XactUndo_SubXactCallback
  break perform_undo_discard
  break pg_stat_get_undo_buffers
  break pg_stat_get_undo_logs
  break pg_undo_force_discard
  break undo_redo
  break undo_worker_sighup
  break undo_worker_sigterm
end
define undo-bp-undo_core-all
  undo-bp-undo_core-entry
  # 2 helpers (undo_core)
  break EnsureSubxactStackCapacity
  break GetCurrentXactLastBatchLSN
end

define undo-bp-atm_slog-entry
  # 29 entry points (atm_slog)
  break ATMCollectUnrevertedDatabases
  break ATMGetNextUnreverted
  break ATMGetOldestUnrevertedLSN
  break ATMMarkReverted
  break LogicalRevertLauncherMain
  break LogicalRevertLauncherRegister
  break LogicalRevertShmemInit
  break LogicalRevertShmemSize
  break LogicalRevertWorkerMain
  break SLogFlatHashApply
  break SLogPartApplyOne
  break SLogTxnCollectUnrevertedDatabases
  break SLogTxnGetNextUnreverted
  break SLogTxnGetOldestUnrevertedLSN
  break SLogTxnMarkReverted
  break StartLogicalRevertWorker
  break atm_redo
  break flat_hash_apply_cleanup_retained
  break flat_hash_apply_commit_xid
  break flat_hash_apply_create_aborted
  break flat_hash_apply_insert
  break flat_hash_apply_mark_aborted
  break flat_hash_apply_remove_entry
  break flat_hash_apply_remove_xid
  break flat_hash_apply_update_op
  break get_revertable_database_list
  break logical_revert_sighup
  break logical_revert_sigterm
  break process_revert_entry
end
define undo-bp-atm_slog-all
  undo-bp-atm_slog-entry
  # 88 helpers (atm_slog)
  break ATMAddAborted
  break ATMAddAbortedInternal
  break ATMForget
  break ATMForgetInternal
  break ATMGetLastBatchLSN
  break ATMRecoveryFinalize
  break ATMReloadFromCheckpoint
  break ATMShmemInit
  break ATMShmemSize
  break CheckPointATM
  break SLogCollectTrackedPartitions
  break SLogComputeNumPartitions
  break SLogEnsureDsaAttached
  break SLogFlatHashCapacity
  break SLogFlatHashComputeHash
  break SLogFlatHashDataSize
  break SLogFlatHashHasOpForXid
  break SLogFlatHashInit
  break SLogFlatHashPartitionedShmemSize
  break SLogFlatHashProbe
  break SLogFlatHashProbeForInsert
  break SLogFlatHashScanInit
  break SLogFlatHashScanNext
  break SLogFlatHashShmemSize
  break SLogGetPartition
  break SLogGetPartitionByIndex
  break SLogRecoveryFinalize
  break SLogRegisterAmDescriptor
  break SLogShmemInit
  break SLogShmemInit_cb
  break SLogShmemRequest
  break SLogShmemRequest_cb
  break SLogShmemSize
  break SLogTupleAnyTracked
  break SLogTupleCleanupRetained
  break SLogTupleCollectTrackedKeys
  break SLogTupleCommitByXid
  break SLogTupleEvictCommitted
  break SLogTupleGetBeforeImage
  break SLogTupleGetDirtyWriterXid
  break SLogTupleGetDirtyXid
  break SLogTupleGetLockConflictXid
  break SLogTupleGetWriteConflictXid
  break SLogTupleHasAbortedEntry
  break SLogTupleHasEntry
  break SLogTupleHasLockConflict
  break SLogTupleInsert
  break SLogTupleInsertRecovery
  break SLogTupleIsDeletedByMe
  break SLogTupleIsInsertedByMe
  break SLogTupleIterateByTid
  break SLogTupleIterateTrackedKeys
  break SLogTupleIterateTrackedKeysExt
  break SLogTupleIterateTrackedKeysForSubXid
  break SLogTupleLookup
  break SLogTupleLookupFiltered
  break SLogTupleMarkAborted
  break SLogTupleMarkAbortedSingle
  break SLogTupleMaybeCleanupRetained
  break SLogTupleNumEntries
  break SLogTupleRemove
  break SLogTupleRemoveBySubXid
  break SLogTupleRemoveByXid
  break SLogTupleRemoveByXidGlobal
  break SLogTupleRemoveByXidSingle
  break SLogTupleResetTracking
  break SLogTupleShmemInit
  break SLogTupleShmemRequest
  break SLogTupleShmemSize
  break SLogTupleStoreBeforeImage
  break SLogTupleTrackKey
  break SLogTupleTrackLocalOnly
  break SLogTupleUntrackLocalOnly
  break SLogTupleUpdateSubXid
  break SLogTxnInsert
  break SLogTxnLookup
  break SLogTxnLookupByXid
  break SLogTxnRemove
  break SLogTxnRemoveByXid
  break SLogTxnSnapshotForCheckpoint
  break slog_atm_key
  break slog_atm_key_reloid
  break slog_atm_key_xid
  break slog_atm_tree
  break slog_insert_tid_hash
  break slog_insert_tids_ensure
  break slog_num_cpus
  break slog_tuplock_to_lockmode
end

define undo-bp-perbackend-entry
  # 178 entry points (perbackend)
  break AmAttachedToUndoLog
  break CanPushReqToUndoWorker
  break CaptureCurrentUndoLogMeta
  break CheckPointUndoLogs
  break CleanUpUndoCheckPointFiles
  break DiscardWorkerMain
  break DiscardWorkerRegister
  break DropUndoLogsInTablespace
  break FindUndoEndLocationAndSize
  break GetRollbackHashKeyFromQueue
  break INIT_UNDOFILETAG
  break InsertPreparedUndo
  break InsertRequestIntoErrorUndoQueue
  break InsertRequestIntoUndoQueues
  break InsertUndoBytes
  break InsertUndoRecord
  break IsDiscardProcess
  break IsUndoWorkerAvailable
  break LogUndoMetaData
  break LogUndoMetaDataNow
  break NeedUndoMetaLog
  break PbuAtAbort_ApplyUndo
  break PbuAtPrepare_Undo
  break PbuEnsureUndoLauncher
  break PbuPerformUndoRecovery
  break PbuRegisterRecoveredRollbackReq
  break PbuUndoLogDiscard
  break PbuUndoLogShmemInit
  break PbuUndoLogShmemRequest
  break PbuUndoLogShmemSize
  break PbuUndoWorkerMain
  break PendingUndoShmemInit
  break PendingUndoShmemRequest
  break PendingUndoShmemSize
  break PrefetchUndoPages
  break PrepareUndoInsert
  break PrepareUpdateUndoActionProgress
  break ReadUndoBytes
  break RegisterRollbackReq
  break RegisterUndoLogBuffers
  break RegisterUndoLogBuffersForImage
  break RegisterUndoLogBuffersWithData
  break ResetUndoBuffers
  break ResetUndoLogs
  break ResetUndoRecord
  break RollbackHTCleanup
  break RollbackHTIsFull
  break RollbackHTRemoveEntry
  break SetCurrentUndoLocation
  break SetUndoWorkerQueueStart
  break StartupUndoLogs
  break TempUndoDiscard
  break UndoDiscard
  break UndoDiscardOneLog
  break UndoErrorQueueElemsShmSize
  break UndoErrorQueueGetFreeElem
  break UndoFetchRecord
  break UndoGetBufferSlot
  break UndoGetOneRecord
  break UndoGetPrevRecordLen
  break UndoGetPrevUndoRecptr
  break UndoGetWork
  break UndoLauncherMain
  break UndoLauncherOnExit
  break UndoLauncherRegister
  break UndoLauncherShmemInit
  break UndoLauncherShmemRequest
  break UndoLauncherShmemSize
  break UndoLauncherSighup
  break UndoLogAdvance
  break UndoLogAllocate
  break UndoLogAllocateInRecovery
  break UndoLogAmAttachedTo
  break UndoLogBuffersSetLSN
  break UndoLogDirectory
  break UndoLogDiscardAll
  break UndoLogGet
  break UndoLogGetFirstValidRecord
  break UndoLogGetLastXactStartPoint
  break UndoLogGetNextInsertPtr
  break UndoLogInit
  break UndoLogIsDiscarded
  break UndoLogNewSegment
  break UndoLogNext
  break UndoLogRewind
  break UndoLogSegmentPath
  break UndoLogSetLSN
  break UndoLogSetLastXactStartPoint
  break UndoLogStateGetAndClearPrevLogXactUrp
  break UndoLogStateGetDatabaseId
  break UndoRecPtrGetTablespace
  break UndoRecordAllocate
  break UndoRecordBulkFetch
  break UndoRecordExpectedSize
  break UndoRecordIsValid
  break UndoRecordOnUndoLogChange
  break UndoRecordPrepareTransInfo
  break UndoRecordRelease
  break UndoRecordSetInfo
  break UndoRecordUpdateTransInfo
  break UndoRollbackHashTableSize
  break UndoSetPrepareSize
  break UndoSizeQueueElemsShmSize
  break UndoSizeQueueGetFreeElem
  break UndoWorkerAttach
  break UndoWorkerCleanup
  break UndoWorkerDetach
  break UndoWorkerGetSlotInfo
  break UndoWorkerIsLingering
  break UndoWorkerLaunch
  break UndoWorkerOnExit
  break UndoWorkerPerformRequest
  break UndoWorkerQueuesEmpty
  break UndoXidQueueElemsShmSize
  break UndoXidQueueGetFreeElem
  break UndoworkerSigtermHandler
  break UnlockReleaseUndoBuffers
  break UnpackUndoRecord
  break UnpackedUndoRecordSize
  break WaitForUndoWorkerAttach
  break WakeupUndoWorker
  break allocate_empty_undo_segment
  break assign_undo_tablespaces
  break attach_undo_log
  break check_undo_tablespaces
  break choose_undo_tablespace
  break detach_current_undo_log
  break ensure_undo_log_number
  break execute_undo_actions
  break execute_undo_actions_page
  break extend_undo_log
  break forget_undo_buffers
  break get_undo_log_by_number
  break initialize_undo_log_bank
  break pbu_pg_stat_get_undo_logs
  break undo_age_comparator
  break undo_err_time_comparator
  break undo_log_before_exit
  break undo_record_comparator
  break undo_size_comparator
  break undo_xlog_apply_progress
  break undoaction_redo
  break undofile_close
  break undofile_create
  break undofile_exists
  break undofile_extend
  break undofile_fd
  break undofile_forget_sync
  break undofile_get_segment_file
  break undofile_immedsync
  break undofile_init
  break undofile_maxcombine
  break undofile_nblocks
  break undofile_open
  break undofile_open_segment_file
  break undofile_prefetch
  break undofile_readv
  break undofile_registersync
  break undofile_shutdown
  break undofile_startreadv
  break undofile_syncfiletag
  break undofile_truncate
  break undofile_unlink
  break undofile_writeback
  break undofile_writev
  break undofile_zeroextend
  break undolog_bank_gc
  break undolog_redo
  break undolog_xid_map_add
  break undolog_xid_map_gc
  break undolog_xlog_attach
  break undolog_xlog_create
  break undolog_xlog_discard
  break undolog_xlog_extend
  break undolog_xlog_meta
  break undolog_xlog_rewind
  break undolog_xlog_switch
  break undoworker_sigterm_handler
end
define undo-bp-perbackend-all
  undo-bp-perbackend-entry
  # 20 helpers (perbackend)
  break GetEpochForXid
  break IsTransactionFirstRec
  break PopErrorQueueNthElem
  break PopSizeQueueNthElem
  break PopXidQueueNthElem
  break PushErrorQueueElem
  break PushSizeQueueElem
  break PushXidQueueElem
  break RemoveOldElemsFromErrorQueue
  break RemoveOldElemsFromSizeQueue
  break RemoveOldElemsFromXidQueue
  break RemoveRequestFromQueue
  break binaryheap_init_shm
  break binaryheap_remove_nth
  break binaryheap_remove_nth_unordered
  break binaryheap_shmem_size
  break dbid_exists
  break err_out_to_client
  break pbu_twophase_postabort
  break pbu_twophase_postcommit
end

define undo-bp-index_undo-entry
  # 15 entry points (index_undo)
  break HashUndoLogInsert
  break HashUndoRmgrInit
  break NbtreeUndoLogInsert
  break NbtreeUndoRmgrInit
  break hash_undo_apply
  break hash_undo_desc
  break hash_undo_find_entry
  break hash_undo_key_digest
  break hash_undo_verify_entry
  break nbtree_undo_apply
  break nbtree_undo_desc
  break nbtree_undo_find_leaf_entry
  break nbtree_undo_key_digest
  break nbtree_undo_verify_leaf_entry
  break nbtree_undo_write
end
define undo-bp-index_undo-all
  undo-bp-index_undo-entry
  # 0 helpers (index_undo)
end

define undo-bp-flux-entry
  # 75 entry points (flux)
  break AtPrepare_Flux
  break FluxLogRelationStats
  break FluxPbuApplySubxactUndo
  break FluxPbuBlockIsUndo
  break FluxPbuCancelUndo
  break FluxPbuFinishUndo
  break FluxPbuGetCurrentUndoLatest
  break FluxPbuGetCurrentUndoStart
  break FluxPbuInSubxactApply
  break FluxPbuInsertUndo
  break FluxPbuPrepareInsert
  break FluxPbuRedoUndo
  break FluxPbuRegisterUndoBuffers
  break FluxPbuUndoPending
  break FluxPrepareReassignSlot
  break FluxResolvePreparedSlot
  break FluxUndoRmgrInit
  break FluxXLogCasUpdate
  break FluxXLogCasUpdateUndo
  break FluxXLogCompress
  break FluxXLogCrossPageDefrag
  break FluxXLogDefrag
  break FluxXLogDelete
  break FluxXLogInitPage
  break FluxXLogInsert
  break FluxXLogMultiInsert
  break FluxXLogOverflowWrite
  break FluxXLogPrepareLogicalImage
  break FluxXLogRegisterLogicalImage
  break FluxXLogReleaseLogicalImage
  break FluxXLogUpdate
  break FluxXLogWriteDict
  break apply_flux_undo_insert
  break apply_flux_undo_restore_tuple
  break emit_flux_undo_clr
  break flux_inplace_index_undo
  break flux_multi_insert
  break flux_prepare_pagescan
  break flux_redo
  break flux_relation_copy_data
  break flux_relation_copy_for_cluster
  break flux_relation_estimate_size
  break flux_relation_needs_toast_table
  break flux_relation_nontransactional_truncate
  break flux_relation_set_new_filelocator
  break flux_relation_toast_am
  break flux_relation_vacuum
  break flux_scan_getnextslot
  break flux_scan_getnextslot_tidrange
  break flux_tableam_handler
  break flux_toast_delete
  break flux_tuple_delete
  break flux_tuple_fetch_row_version
  break flux_tuple_insert
  break flux_tuple_insert_speculative
  break flux_tuple_lock
  break flux_tuple_update
  break flux_tuple_update_inplace
  break flux_undo_apply
  break flux_undo_desc
  break flux_xlog_cas_update_redo
  break flux_xlog_cas_update_undo_redo
  break flux_xlog_compress_redo
  break flux_xlog_cross_page_defrag_redo
  break flux_xlog_defrag_redo
  break flux_xlog_delete_redo
  break flux_xlog_init_page_redo
  break flux_xlog_insert_redo
  break flux_xlog_lock_redo
  break flux_xlog_multi_insert_redo
  break flux_xlog_overflow_write_redo
  break flux_xlog_update_inplace_redo
  break flux_xlog_vm_clear_redo
  break flux_xlog_vm_set_redo
  break flux_xlog_write_dict_redo
end
define undo-bp-flux-all
  undo-bp-flux-entry
  # 171 helpers (flux)
  break FluxAbortTransaction
  break FluxCheckForSerializableConflictOut
  break FluxCheckSelfModified
  break FluxCleanupTransactionState
  break FluxClearUncommittedFlags
  break FluxCollectRelationStats
  break FluxCommitTransaction
  break FluxComputeDataSize
  break FluxCountNondeletablePages
  break FluxDeformTuple
  break FluxDeformTupleUpTo
  break FluxDirtyMapCheck
  break FluxDirtyMapMark
  break FluxDirtyMapShmemInit
  break FluxDirtyMapShmemInit_cb
  break FluxDirtyMapShmemRequest
  break FluxDirtyMapShmemSize
  break FluxEnsureSLogCallbacks
  break FluxEpqReconcileMark
  break FluxEpqReconcileMatches
  break FluxForgetSerializeLocks
  break FluxFormTuple
  break FluxFormTupleForceShrink
  break FluxFormTupleUpdate
  break FluxFreeTuple
  break FluxGetCommitTimestamp
  break FluxGetDmlTimestamp
  break FluxGetMvccStats
  break FluxGetOldestActiveTimestamp
  break FluxGetOldestXminHorizon
  break FluxGetPageWithFreeSpace
  break FluxGetSnapshotTimestamp
  break FluxGetTransactionTimestamp
  break FluxGetUpdateStats
  break FluxHoldsTupleLock
  break FluxInitPage
  break FluxInitTransactionState
  break FluxLocalCidGet
  break FluxLocalCidReset
  break FluxLocalCidSet
  break FluxLocalDirtyPageMark
  break FluxLocalDirtyPagesReset
  break FluxLockMultipleTuples
  break FluxLockPage
  break FluxLockRelationForDDL
  break FluxLockTuple
  break FluxMvccShmemInit
  break FluxMvccShmemInit_cb
  break FluxMvccShmemRequest
  break FluxMvccShmemSize
  break FluxPageAddTuple
  break FluxPageDefragment
  break FluxPageGetLiveTuples
  break FluxPageIndexTupleDelete
  break FluxPageIsEmpty
  break FluxPagePruneOpt
  break FluxPageUpdateTuple
  break FluxPbuFetchVersion
  break FluxPbuFetchXid
  break FluxPbuRecordExists
  break FluxProcessAbortedEntries
  break FluxReconstructVisibleVersion
  break FluxRecordFreeSpace
  break FluxReleaseSerializeLocks
  break FluxRestoreBeforeImages
  break FluxSLogSubXactCallback
  break FluxSLogXactCallback
  break FluxSetHintBits
  break FluxShmemExit
  break FluxSlotCacheSysAttrs
  break FluxSlotStoreMaterializedTuple
  break FluxSlotStoreTuple
  break FluxTargetFreeSpace
  break FluxTrackSerializeLock
  break FluxTruncateRelation
  break FluxTupleDeadToAll
  break FluxTupleHasCommittedUpdateAfter
  break FluxTupleSatisfiesMVCC
  break FluxTupleToSlot
  break FluxTupleToSlotWithOverflow
  break FluxTupleVisibleToSnapshot
  break FluxTupleVisibleToSnapshotDual
  break FluxUnlockPage
  break FluxUnlockTuple
  break FluxUpdateOldestActiveTimestamp
  break FluxVMCheck
  break FluxVMCheckCached
  break FluxVMClear
  break FluxVMExtend
  break FluxVMGetPageSize
  break FluxVMInit
  break FluxVMMapHeapToVM
  break FluxVMPinBuffer
  break FluxVMSet
  break FluxVMTruncate
  break FluxVMUpdateForDelete
  break FluxVMUpdateForInsert
  break FluxVMUpdateForUpdate
  break FluxVMVacuumPage
  break FluxVacuumCrossPageDefrag
  break FluxVacuumFSM
  break FluxXactCallback
  break GetFluxTableAmRoutine
  break flux_analyze_accumulate_sample
  break flux_batch_clear_uncommitted
  break flux_bitmap_stream_read_next
  break flux_clear_uncommitted_by_page
  break flux_cmp_tracked_key_by_block
  break flux_dirtymap_mix
  break flux_dirtymap_part
  break flux_dirtymap_slot0
  break flux_fetch_tid
  break flux_form_tuple_internal
  break flux_header_dirty_xid
  break flux_index_build_range_scan
  break flux_index_delete_tuples
  break flux_index_entry_key_mismatch
  break flux_index_fetch_tuple
  break flux_index_getnext_slot
  break flux_index_key_changed
  break flux_index_scan_begin
  break flux_index_scan_end
  break flux_index_scan_reset
  break flux_index_validate_scan
  break flux_indexed_attr_changed
  break flux_inplace_index_maint_possible
  break flux_inplace_index_maintenance
  break flux_mask
  break flux_page_clear_uncommitted
  break flux_process_aborted_cb
  break flux_register_twophase_cb
  break flux_release_update_overflow_buffers
  break flux_restore_before_image_cb
  break flux_scan_analyze_next_block
  break flux_scan_analyze_next_tuple
  break flux_scan_begin
  break flux_scan_bitmap_next_tuple
  break flux_scan_end
  break flux_scan_nextblock
  break flux_scan_rescan
  break flux_scan_sample_next_block
  break flux_scan_sample_next_tuple
  break flux_scan_set_tidrange
  break flux_scan_stream_read_next
  break flux_slot_callbacks
  break flux_stamp_tuple_committed
  break flux_toast_cleanup
  break flux_toast_tuple
  break flux_tuple_complete_speculative
  break flux_tuple_get_latest_tid
  break flux_tuple_satisfies_snapshot
  break flux_tuple_tid_valid
  break flux_twophase_postabort
  break flux_twophase_postcommit
  break flux_twophase_recover
  break flux_update_out_of_place
  break flux_vac_scan_next_block
  break flux_vm_extend
  break flux_vm_readbuf
  break tts_flux_clear
  break tts_flux_copy_heap_tuple
  break tts_flux_copy_minimal_tuple
  break tts_flux_copyslot
  break tts_flux_deform
  break tts_flux_getsomeattrs
  break tts_flux_getsysattr
  break tts_flux_init
  break tts_flux_is_current_xact_tuple
  break tts_flux_materialize
  break tts_flux_materialize_values
  break tts_flux_release
end

define undo-bp-recno-entry
  # 66 entry points (recno)
  break RecnoApplyDiffReverse
  break RecnoApplyInlineDiffReverse
  break RecnoPbuApplySubxactUndo
  break RecnoPbuBlockIsUndo
  break RecnoPbuCancelUndo
  break RecnoPbuFinishUndo
  break RecnoPbuGetCurrentUndoLatest
  break RecnoPbuGetCurrentUndoStart
  break RecnoPbuInSubxactApply
  break RecnoPbuInsertUndo
  break RecnoPbuPrepareInsert
  break RecnoPbuRedoUndo
  break RecnoPbuRegisterUndoBuffers
  break RecnoPbuUndoPending
  break RecnoUndoRmgrInit
  break RecnoXLogCompress
  break RecnoXLogCrossPageDefrag
  break RecnoXLogDefrag
  break RecnoXLogDelete
  break RecnoXLogDeleteHLC
  break RecnoXLogEnsureLogicalImageCxt
  break RecnoXLogInitPage
  break RecnoXLogInsert
  break RecnoXLogInsertHLC
  break RecnoXLogMaybeAppendLogicalTuple
  break RecnoXLogOverflowWrite
  break RecnoXLogResetLogicalImages
  break RecnoXLogUpdate
  break RecnoXLogUpdateHLC
  break apply_recno_undo_insert
  break apply_recno_undo_restore_tuple
  break emit_recno_undo_clr
  break recno_multi_insert
  break recno_prepare_pagescan
  break recno_redo
  break recno_redo_handle_hlc
  break recno_relation_copy_data
  break recno_relation_copy_for_cluster
  break recno_relation_estimate_size
  break recno_relation_needs_toast_table
  break recno_relation_nontransactional_truncate
  break recno_relation_set_new_filelocator
  break recno_relation_size
  break recno_relation_vacuum
  break recno_scan_getnextslot
  break recno_scan_getnextslot_tidrange
  break recno_tableam_handler
  break recno_tuple_delete
  break recno_tuple_fetch_row_version
  break recno_tuple_insert
  break recno_tuple_insert_speculative
  break recno_tuple_lock
  break recno_tuple_update
  break recno_undo_apply
  break recno_undo_desc
  break recno_xlog_compress_redo
  break recno_xlog_cross_page_defrag_redo
  break recno_xlog_defrag_redo
  break recno_xlog_delete_redo
  break recno_xlog_init_page_redo
  break recno_xlog_insert_redo
  break recno_xlog_lock_redo
  break recno_xlog_overflow_write_redo
  break recno_xlog_update_inplace_redo
  break recno_xlog_vm_clear_redo
  break recno_xlog_vm_set_redo
end
define undo-bp-recno-all
  undo-bp-recno-entry
  # 244 helpers (recno)
  break GetRecnoTableAmRoutine
  break HLCCompare
  break HLCFromTimestampTz
  break HLCGetDriftStats
  break HLCGetGlobal
  break HLCGetLogical
  break HLCGetPhysical
  break HLCGetUncertaintyInterval
  break HLCInUncertaintyWindow
  break HLCMake
  break HLCNow
  break HLCToString
  break HLCToTimestampTz
  break RecnoAbortTransaction
  break RecnoAddDictEntry
  break RecnoBatchDefrag
  break RecnoCanPruneHLC
  break RecnoCanVacuumTimestamp
  break RecnoCheckClockHealth
  break RecnoCheckDeadlock
  break RecnoCheckForSerializableConflictOut
  break RecnoCheckUncommittedInsert
  break RecnoChooseCompressionType
  break RecnoClassifyFreeSpace
  break RecnoCleanupTransactionState
  break RecnoClockGetStats
  break RecnoClockMonitorMain
  break RecnoClockShmemInit
  break RecnoClockShmemInit_cb
  break RecnoClockShmemRequest
  break RecnoClockShmemSize
  break RecnoClockShutdown
  break RecnoClockStartMonitor
  break RecnoCollectOverflowPtrs
  break RecnoCollectRelationStats
  break RecnoCommitTransaction
  break RecnoCompactPage
  break RecnoCompressAttribute
  break RecnoCompressDelta
  break RecnoCompressDictionary
  break RecnoCompressLZ4
  break RecnoCompressLZ4Dict
  break RecnoCompressZSTD
  break RecnoCompressZSTDDict
  break RecnoComputeDataSize
  break RecnoComputeSlotSize
  break RecnoComputeTupleDiff
  break RecnoDecompressAttribute
  break RecnoDecompressDelta
  break RecnoDecompressDictionary
  break RecnoDecompressLZ4
  break RecnoDecompressLZ4Dict
  break RecnoDecompressZSTD
  break RecnoDecompressZSTDDict
  break RecnoDeformTuple
  break RecnoDefragmentPage
  break RecnoDeleteOverflow
  break RecnoDeleteOverflowChain
  break RecnoDeleteTupleOverflows
  break RecnoDiffIsCompact
  break RecnoFetchOverflow
  break RecnoFetchOverflowColumn
  break RecnoFillHLCInfo
  break RecnoFindDictEntry
  break RecnoFindOverflowPageForReuse
  break RecnoForgetSerializeLocks
  break RecnoFormTuple
  break RecnoFormTupleFromSlot
  break RecnoFreeTuple
  break RecnoGetCommitHLC
  break RecnoGetCommitTimestamp
  break RecnoGetCompressionStats
  break RecnoGetDictForRelation
  break RecnoGetDmlTimestamp
  break RecnoGetFSMState
  break RecnoGetFSMStats
  break RecnoGetInsertFreeSpace
  break RecnoGetMvccStats
  break RecnoGetNextDefragPage
  break RecnoGetOldestActiveHLC
  break RecnoGetOldestActiveTimestamp
  break RecnoGetOverflowStats
  break RecnoGetPageWithFreeSpace
  break RecnoGetPhysicalTimeMs
  break RecnoGetSnapshotHLC
  break RecnoGetSnapshotTimestamp
  break RecnoGetTimestampBounds
  break RecnoGetTransactionHLC
  break RecnoGetTransactionTimestamp
  break RecnoGetUpdateStats
  break RecnoHLCShmemInit
  break RecnoHLCShmemInit_cb
  break RecnoHLCShmemRequest
  break RecnoHLCShmemSize
  break RecnoHoldsTupleLock
  break RecnoInitCompressionDict
  break RecnoInitFSM
  break RecnoInitPage
  break RecnoInitTransactionState
  break RecnoIsOverflowRecord
  break RecnoLockMultipleTuples
  break RecnoLockPage
  break RecnoLockRelationForDDL
  break RecnoLockTuple
  break RecnoLogRelationStats
  break RecnoMarkPageForDefrag
  break RecnoMvccShmemInit
  break RecnoMvccShmemInit_cb
  break RecnoMvccShmemRequest
  break RecnoMvccShmemSize
  break RecnoOpportunisticDefrag
  break RecnoPageAddTuple
  break RecnoPageDefragment
  break RecnoPageDeleteTuple
  break RecnoPageGetLiveTuples
  break RecnoPageIndexTupleDelete
  break RecnoPagePruneOpt
  break RecnoPageUpdateTuple
  break RecnoPbuFetchVersion
  break RecnoPbuFetchXid
  break RecnoPruneDecision
  break RecnoReadClockBound
  break RecnoReconstructVisibleVersion
  break RecnoRecordFreeSpace
  break RecnoReleaseSerializeLocks
  break RecnoReplicaAdvanceHLC
  break RecnoReplicaHandleUncertainty
  break RecnoResetCompressionDict
  break RecnoSLogBuildKey
  break RecnoSLogClearUncommittedFlags
  break RecnoSLogEnsureCallbacks
  break RecnoSLogGetDirtyXid
  break RecnoSLogHasAbortedEntry
  break RecnoSLogHasEntry
  break RecnoSLogHasLockConflict
  break RecnoSLogInsert
  break RecnoSLogIsDeletedByMe
  break RecnoSLogIsInsertedByMe
  break RecnoSLogLocalTrack
  break RecnoSLogLockPartition
  break RecnoSLogLookup
  break RecnoSLogLookupAll
  break RecnoSLogMarkAborted
  break RecnoSLogNumEntries
  break RecnoSLogPartition
  break RecnoSLogProcessAbortedEntries
  break RecnoSLogRemove
  break RecnoSLogRemoveBySubXid
  break RecnoSLogRemoveByXid
  break RecnoSLogRemoveByXidGlobal
  break RecnoSLogShmemInit
  break RecnoSLogShmemInit_cb
  break RecnoSLogShmemRequest
  break RecnoSLogShmemSize
  break RecnoSLogSortCmp
  break RecnoSLogSubXactCallback
  break RecnoSLogTrackSubXact
  break RecnoSLogUnlockPartition
  break RecnoSLogUpdateSubXid
  break RecnoSLogXactCallback
  break RecnoScheduleDefrag
  break RecnoShmemExit
  break RecnoShouldDefragPage
  break RecnoSlotStoreMaterializedTuple
  break RecnoSlotStoreTuple
  break RecnoStoreOverflow
  break RecnoStoreOverflowColumn
  break RecnoTrackSerializeLock
  break RecnoTupleHasCommittedUpdateAfter
  break RecnoTupleToSlot
  break RecnoTupleToSlotWithOverflow
  break RecnoTupleVisible
  break RecnoTupleVisibleHLC
  break RecnoTupleVisibleToSnapshot
  break RecnoTupleVisibleToSnapshotDual
  break RecnoTupleVisibleWithUncertainty
  break RecnoUnlockPage
  break RecnoUnlockTuple
  break RecnoUpdateOldestActiveTimestamp
  break RecnoVMCheck
  break RecnoVMClear
  break RecnoVMExtend
  break RecnoVMGetPageSize
  break RecnoVMInit
  break RecnoVMMapHeapToVM
  break RecnoVMPinBuffer
  break RecnoVMSet
  break RecnoVMTruncate
  break RecnoVMUpdateForDelete
  break RecnoVMUpdateForInsert
  break RecnoVMUpdateForUpdate
  break RecnoVMVacuumPage
  break RecnoVacuumCrossPageDefrag
  break RecnoVacuumFSM
  break RecnoVacuumOverflowRecords
  break RecnoWaitForClockBound
  break RecnoXactCallback
  break assign_recno_clock_check_interval
  break assign_recno_enable_clock_bound
  break assign_recno_fatal_on_clock_drift
  break assign_recno_max_clock_offset
  break assign_recno_node_id
  break recno_fetch_tid
  break recno_index_build_range_scan
  break recno_index_delete_tuples
  break recno_index_entry_key_mismatch
  break recno_index_fetch_tuple
  break recno_index_getnext_slot
  break recno_index_scan_begin
  break recno_index_scan_end
  break recno_index_scan_reset
  break recno_index_validate_scan
  break recno_init_mvcc_state
  break recno_inplace_index_maintenance
  break recno_mask
  break recno_scan_analyze_next_block
  break recno_scan_analyze_next_tuple
  break recno_scan_begin
  break recno_scan_bitmap_next_tuple
  break recno_scan_end
  break recno_scan_nextblock
  break recno_scan_rescan
  break recno_scan_sample_next_block
  break recno_scan_sample_next_tuple
  break recno_scan_set_tidrange
  break recno_scan_stream_read_next
  break recno_slot_callbacks
  break recno_tuple_complete_speculative
  break recno_tuple_get_latest_tid
  break recno_tuple_satisfies_snapshot
  break recno_tuple_tid_valid
  break recno_vm_extend
  break recno_vm_readbuf
  break tts_recno_clear
  break tts_recno_copy_heap_tuple
  break tts_recno_copy_minimal_tuple
  break tts_recno_copyslot
  break tts_recno_deform
  break tts_recno_getsomeattrs
  break tts_recno_getsysattr
  break tts_recno_init
  break tts_recno_is_current_xact_tuple
  break tts_recno_materialize
  break tts_recno_release
end

define undo-bp-rmgrdesc-entry
  # 9 entry points (rmgrdesc)
  break atm_desc
  break flux_desc
  break recno_desc
  break undo_desc
  break undo_identify
  break undoaction_desc
  break undoaction_identify
  break undolog_desc
  break undolog_identify
end
define undo-bp-rmgrdesc-all
  undo-bp-rmgrdesc-entry
  # 4 helpers (rmgrdesc)
  break atm_identify
  break flux_comp_type_name
  break flux_identify
  break recno_identify
end

define undo-bp-basics
  # foundation: fileops + undo core + atm/slog + perbackend entry points
  undo-bp-fileops-entry
  undo-bp-undo_core-entry
  undo-bp-atm_slog-entry
  undo-bp-perbackend-entry
end

# --- new code embedded in existing functions (file:line) ---
define undo-bp-embedded
  break src/backend/access/common/reloptions.c:2129  # fillRelOptions()
  break src/backend/access/hash/hashinsert.c:242  # _hash_doinsert()
  break src/backend/access/index/genam.c:106  # RelationGetIndexScan()
  break src/backend/access/index/indexam.c:241  # index_insert()
  break src/backend/access/nbtree/nbtdedup.c:152  # _bt_dedup_pass()
  break src/backend/access/nbtree/nbtdedup.c:160  # _bt_dedup_pass()
  break src/backend/access/nbtree/nbtdedup.c:383  # _bt_bottomupdel_pass()
  break src/backend/access/nbtree/nbtdedup.c:404  # _bt_bottomupdel_pass()
  break src/backend/access/nbtree/nbtdedup.c:413  # _bt_bottomupdel_pass()
  break src/backend/access/nbtree/nbtdedup.c:421  # _bt_bottomupdel_pass()
  break src/backend/access/nbtree/nbtinsert.c:162  # _bt_doinsert()
  break src/backend/access/nbtree/nbtinsert.c:265  # _bt_doinsert()
  break src/backend/access/nbtree/nbtinsert.c:525  # _bt_check_unique()
  break src/backend/access/nbtree/nbtinsert.c:732  # _bt_check_unique()
  break src/backend/access/nbtree/nbtinsert.c:1435  # _bt_insertonpg()
  break src/backend/access/nbtree/nbtinsert.c:2854  # _bt_delete_or_dedup_one_page()
  break src/backend/access/nbtree/nbtinsert.c:2935  # _bt_simpledel_pass()
  break src/backend/access/nbtree/nbtreadpage.c:1039  # _bt_saveitem()
  break src/backend/access/nbtree/nbtreadpage.c:1087  # _bt_setuppostingitems()
  break src/backend/access/nbtree/nbtreadpage.c:1124  # _bt_savepostingitem()
  break src/backend/access/nbtree/nbtree.c:169  # bthandler()
  break src/backend/access/nbtree/nbtree.c:312  # btgetbitmap()
  break src/backend/access/nbtree/nbtree.c:329  # btgetbitmap()
  break src/backend/access/nbtree/nbtree.c:336  # btgetbitmap()
  break src/backend/access/nbtree/nbtree.c:431  # btrescan()
  break src/backend/access/nbtree/nbtree.c:448  # btrescan()
  break src/backend/access/nbtree/nbtree.c:1589  # btvacuumpage()
  break src/backend/access/nbtree/nbtree.c:1833  # btreevacuumposting()
  break src/backend/access/nbtree/nbtree.c:1865  # btgettreeheight()
  break src/backend/access/nbtree/nbtsearch.c:558  # _bt_binsrch_insert()
  break src/backend/access/nbtree/nbtsearch.c:1640  # _bt_returnitem()
  break src/backend/access/nbtree/nbtsort.c:1156  # _bt_load()
  break src/backend/access/nbtree/nbtutils.c:336  # _bt_killitems()
  break src/backend/access/nbtree/nbtutils.c:713  # _bt_truncate()
  break src/backend/access/nbtree/nbtutils.c:721  # _bt_truncate()
  break src/backend/access/nbtree/nbtutils.c:775  # _bt_truncate()
  break src/backend/access/nbtree/nbtutils.c:859  # _bt_truncate()
  break src/backend/access/nbtree/nbtxlog.c:1003  # btree_xlog_reuse_page()
  break src/backend/access/nbtree/nbtxlog.c:1110  # btree_redo()
  break src/backend/access/rmgrdesc/nbtdesc.c:135  # btree_desc()
  break src/backend/access/rmgrdesc/nbtdesc.c:199  # btree_identify()
  break src/backend/access/table/tableam.c:785  # table_block_relation_estimate_size()
  break src/backend/access/table/tableamapi.c:39  # GetTableAmRoutine()
  break src/backend/access/transam/twophase.c:527  # MarkAsPreparingGuts()
  break src/backend/access/transam/twophase.c:1014  # TwoPhaseFilePath()
  break src/backend/access/transam/twophase.c:1027  # TwoPhaseFilePath()
  break src/backend/access/transam/twophase.c:1149  # StartPrepare()
  break src/backend/access/transam/twophase.c:1206  # EndPrepare()
  break src/backend/access/transam/twophase.c:1568  # StandbyTransactionIdIsPrepared()
  break src/backend/access/transam/twophase.c:1696  # FinishPreparedTransaction()
  break src/backend/access/transam/twophase.c:1704  # FinishPreparedTransaction()
  break src/backend/access/transam/twophase.c:2282  # RecoverPreparedTransactions()
  break src/backend/access/transam/twophase.c:2742  # PrepareRedoAdd()
  break src/backend/access/transam/twophase.c:3024  # TwoPhaseGetOldestXidInCommit()
  break src/backend/access/transam/xact.c:447  # IsAbortedTransactionBlockState()
  break src/backend/access/transam/xact.c:1183  # IsInParallelMode()
  break src/backend/access/transam/xact.c:2284  # StartTransaction()
  break src/backend/access/transam/xact.c:2593  # CommitTransaction()
  break src/backend/access/transam/xact.c:2640  # CommitTransaction()
  break src/backend/access/transam/xact.c:2818  # PrepareTransaction()
  break src/backend/access/transam/xact.c:2967  # PrepareTransaction()
  break src/backend/access/transam/xact.c:3007  # PrepareTransaction()
  break src/backend/access/transam/xact.c:3125  # AbortTransaction()
  break src/backend/access/transam/xact.c:3179  # AbortTransaction()
  break src/backend/access/transam/xact.c:3271  # AbortTransaction()
  break src/backend/access/transam/xact.c:3273  # AbortTransaction()
  break src/backend/access/transam/xact.c:5463  # CommitSubTransaction()
  break src/backend/access/transam/xact.c:5657  # AbortSubTransaction()
  break src/backend/access/transam/xact.c:6686  # xact_redo()
  break src/backend/access/transam/xact.c:6706  # xact_redo()
  break src/backend/access/transam/xact.c:6718  # xact_redo()
  break src/backend/access/transam/xact.c:6739  # xact_redo()
  break src/backend/access/transam/xact.c:6745  # xact_redo()
  break src/backend/access/transam/xact.c:6750  # xact_redo()
  break src/backend/access/transam/xact.c:6763  # xact_redo()
  break src/backend/access/transam/xlog.c:759  # PrimaryFlushWakeupProcessRequests()
  break src/backend/access/transam/xlog.c:6332  # StartupXLOG()
  break src/backend/access/transam/xlog.c:6395  # StartupXLOG()
  break src/backend/access/transam/xlog.c:6870  # StartupXLOG()
  break src/backend/access/transam/xlog.c:7749  # CreateCheckPoint()
  break src/backend/access/transam/xlog.c:7789  # CreateCheckPoint()
  break src/backend/access/transam/xlog.c:7791  # CreateCheckPoint()
  break src/backend/access/transam/xlog.c:8067  # CreateCheckPoint()
  break src/backend/access/transam/xlog.c:8087  # CreateCheckPoint()
  break src/backend/access/transam/xlog.c:8093  # CreateCheckPoint()
  break src/backend/access/transam/xlog.c:8108  # CreateCheckPoint()
  break src/backend/access/transam/xlog.c:8134  # CreateCheckPoint()
  break src/backend/access/transam/xlog.c:8136  # CreateCheckPoint()
  break src/backend/access/transam/xlog.c:8172  # CreateCheckPoint()
  break src/backend/access/transam/xlog.c:8226  # CreateCheckPoint()
  break src/backend/access/transam/xlog.c:8258  # CreateCheckPoint()
  break src/backend/access/transam/xlog.c:8260  # CreateCheckPoint()
  break src/backend/access/transam/xlog.c:8291  # CreateCheckPoint()
  break src/backend/access/transam/xlog.c:8294  # CreateCheckPoint()
  break src/backend/access/transam/xlog.c:8312  # CreateCheckPoint()
  break src/backend/access/transam/xlog.c:8315  # CreateCheckPoint()
  break src/backend/access/transam/xlog.c:8503  # CheckPointGuts()
  break src/backend/access/transam/xlog.c:8548  # CheckPointGuts()
  break src/backend/access/transam/xlog.c:9094  # KeepLogSeg()
  break src/backend/access/transam/xlogrecovery.c:1881  # PerformWalRecovery()
  break src/backend/commands/dbcommands.c:477  # CreateDirAndVersionFile()
  break src/backend/commands/dbcommands.c:479  # CreateDirAndVersionFile()
  break src/backend/commands/dbcommands.c:494  # CreateDirAndVersionFile()
  break src/backend/commands/dbcommands.c:498  # CreateDirAndVersionFile()
  break src/backend/commands/dbcommands.c:518  # CreateDirAndVersionFile()
  break src/backend/commands/dbcommands.c:526  # CreateDirAndVersionFile()
  break src/backend/commands/dbcommands.c:528  # CreateDirAndVersionFile()
  break src/backend/commands/dbcommands.c:535  # CreateDirAndVersionFile()
  break src/backend/commands/dbcommands.c:540  # CreateDirAndVersionFile()
  break src/backend/commands/dbcommands.c:673  # CreateDatabaseUsingFileCopy()
  break src/backend/commands/dbcommands.c:677  # CreateDatabaseUsingFileCopy()
  break src/backend/commands/dbcommands.c:2263  # movedb()
  break src/backend/commands/dbcommands.c:2265  # movedb()
  break src/backend/commands/dbcommands.c:2297  # movedb()
  break src/backend/commands/dbcommands.c:2303  # movedb()
  break src/backend/commands/dbcommands.c:3463  # dbase_redo()
  break src/backend/commands/dbcommands.c:3468  # dbase_redo()
  break src/backend/commands/tablecmds.c:964  # DefineRelation()
  break src/backend/commands/tablespace.c:604  # create_tablespace_directories()
  break src/backend/commands/tablespace.c:626  # create_tablespace_directories()
  break src/backend/commands/tablespace.c:633  # create_tablespace_directories()
  break src/backend/commands/tablespace.c:635  # create_tablespace_directories()
  break src/backend/commands/tablespace.c:640  # create_tablespace_directories()
  break src/backend/commands/tablespace.c:672  # create_tablespace_directories()
  break src/backend/commands/tablespace.c:700  # create_tablespace_directories()
  break src/backend/commands/tablespace.c:807  # destroy_tablespace_directories()
  break src/backend/commands/tablespace.c:831  # destroy_tablespace_directories()
  break src/backend/commands/tablespace.c:838  # destroy_tablespace_directories()
  break src/backend/commands/tablespace.c:876  # destroy_tablespace_directories()
  break src/backend/commands/tablespace.c:883  # destroy_tablespace_directories()
  break src/backend/commands/tablespace.c:887  # destroy_tablespace_directories()
  break src/backend/commands/tablespace.c:900  # destroy_tablespace_directories()
  break src/backend/commands/tablespace.c:910  # destroy_tablespace_directories()
  break src/backend/commands/tablespace.c:914  # destroy_tablespace_directories()
  break src/backend/commands/trigger.c:3190  # ExecARUpdateTriggers()
  break src/backend/commands/trigger.c:3228  # ExecARUpdateTriggers()
  break src/backend/commands/trigger.c:3745  # TriggerEnabled()
  break src/backend/commands/trigger.c:3798  # TriggerEnabled()
  break src/backend/commands/trigger.c:3816  # TriggerEnabled()
  break src/backend/commands/trigger.c:3959  # TriggerEnabled()
  break src/backend/commands/trigger.c:4509  # AfterTriggerExecute()
  break src/backend/commands/trigger.c:4894  # afterTriggerInvokeEvents()
  break src/backend/commands/trigger.c:5523  # AfterTriggerEndXact()
  break src/backend/commands/trigger.c:6394  # AfterTriggerSaveEvent()
  break src/backend/commands/trigger.c:6574  # AfterTriggerSaveEvent()
  break src/backend/commands/trigger.c:6582  # AfterTriggerSaveEvent()
  break src/backend/commands/trigger.c:6807  # AfterTriggerSaveEvent()
  break src/backend/executor/execReplication.c:951  # ExecSimpleRelationUpdate()
  break src/backend/executor/execReplication.c:989  # ExecSimpleRelationUpdate()
  break src/backend/executor/nodeModifyTable.c:1295  # ExecInsert()
  break src/backend/executor/nodeModifyTable.c:1570  # ExecDeleteEpilogue()
  break src/backend/executor/nodeModifyTable.c:1759  # ExecDelete()
  break src/backend/executor/nodeModifyTable.c:2368  # ExecUpdateEpilogue()
  break src/backend/executor/nodeModifyTable.c:2389  # ExecUpdateEpilogue()
  break src/backend/executor/nodeModifyTable.c:2478  # ExecCrossPartitionUpdateForeignKey()
  break src/backend/executor/nodeModifyTable.c:2596  # ExecUpdate()
  break src/backend/executor/nodeModifyTable.c:2682  # ExecUpdate()
  break src/backend/executor/nodeModifyTable.c:2777  # ExecUpdate()
  break src/backend/executor/nodeModifyTable.c:3464  # ExecMergeMatched()
  break src/backend/executor/nodeModifyTable.c:3493  # ExecMergeMatched()
  break src/backend/optimizer/util/plancat.c:287  # get_relation_info()
  break src/backend/postmaster/postmaster.c:929  # PostmasterMain()
  break src/backend/replication/logical/decode.c:571  # heap_decode()
  break src/backend/storage/buffer/bufmgr.c:3221  # MarkBufferDirty()
  break src/backend/storage/buffer/bufmgr.c:4961  # DropRelationBuffers()
  break src/backend/storage/buffer/bufmgr.c:5996  # UnlockBuffers()
  break src/backend/storage/file/copydir.c:58  # copydir()
  break src/backend/storage/file/copydir.c:71  # copydir()
  break src/backend/storage/file/copydir.c:101  # copydir()
  break src/backend/storage/file/copydir.c:109  # copydir()
  break src/backend/storage/smgr/smgr.c:305  # smgropen()
  break src/backend/tcop/utility.c:1190  # ProcessUtilitySlow()
  break src/backend/utils/init/postinit.c:861  # InitPostgres()
end

# ============================================================
# Enable ALL layers at startup (every new function + embedded block).
# Comment out any line below to trace a subset instead.
# ============================================================
undo-bp-fileops-all
undo-bp-undo_core-all
undo-bp-atm_slog-all
undo-bp-perbackend-all
undo-bp-index_undo-all
undo-bp-flux-all
undo-bp-recno-all
undo-bp-rmgrdesc-all
undo-bp-embedded
echo [undo] all breakpoint layers enabled\n
