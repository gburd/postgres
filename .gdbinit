set tui tab-width 4
set tui mouse-events off

#b ExecOpenIndicies
b ExecInsertIndexTuples
b heapam_tuple_update
b simple_heap_update
b heap_update
b ExecUpdateModIdxAttrs
b HeapUpdateModIdxAttrs
b ExecCompareSlotAttrs
b HeapUpdateHotAllowable
b HeapUpdateDetermineLockmode
b heap_page_prune_opt

b InitMixTracking
b RelationGetIdxSubpaths

b jsonb_idx_extract
b jsonb_idx_compare
b extract_jsonb_path_from_expr

#b fork_process
#b ParallelWorkerMain
#set follow-fork-mode child
#b initdb.c:3105

