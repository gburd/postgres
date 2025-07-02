set tui tab-width 4
set tui mouse-events off

#b ExecOpenIndicies
#b ExecInsertIndexTuples
#b heap_update
b ExecCheckIndexedAttrsForChanges

#b fork_process
#b ParallelWorkerMain
#set follow-fork-mode child
#b initdb.c:3105

