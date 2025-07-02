set tui tab-width 4
set tui mouse-events off


#b tts_heap_check_idx_attrs
#b ExecCheckTupleForChanges
#b ExecOpenIndicies
#b ExecInsertIndexTuples
#b simple_heap_update

#b fork_process
#b ParallelWorkerMain
#set follow-fork-mode child
#b initdb.c:3105

