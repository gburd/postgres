/*-------------------------------------------------------------------------
 *
 * pbu_compat.h
 *	  API-drift compatibility shims for the per-backend UNDO engine.
 *
 * The per-backend UNDO engine is vendored from EnterpriseDB zheap (2019,
 * ~PG12-devel) and reconciled to current master (~PG18/19-devel).  This
 * header centralizes the mechanical drift fixes that would otherwise be
 * duplicated across every vendored translation unit, so that all pbu_*.c
 * files reconcile the same drift the same way.
 *
 * Phase 2a scope: the engine must COMPILE and LINK into the backend but is
 * not yet wired to any resource manager, background worker, or access
 * method.  Where a shim maps to a "close enough" master API (wait-event
 * labels, resource-manager IDs that are never dispatched in this phase),
 * that is called out inline; those are tightened when the engine is wired
 * up in Phase 2b.
 *
 * Per-backend UNDO engine derives from EnterpriseDB zheap
 * (Amit Kapila, Dilip Kumar, Kuntal Ghosh, Mahendra Singh Thalor,
 *  Rafia Sabih, Thomas Munro et al.).
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/perbackend/pbu_compat.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PBU_COMPAT_H
#define PBU_COMPAT_H

#include "access/rmgr.h"
#include "access/transam.h"
#include "access/xlogutils.h"		/* InRecovery, InHotStandby */
#include "nodes/primnodes.h"		/* OnCommitAction */
#include "storage/bufmgr.h"		/* ReadBufferMode */
#include "storage/lwlock.h"
#include "storage/procarray.h"
#include "utils/wait_event.h"

/*
 * INT64_MODIFIER was removed from the tree; UndoRecPtrFormat/UndoLogOffset
 * still reference it.  The width modifier for a 64-bit integer is "ll" on all
 * supported platforms (int64 is long long via pg_config).
 */
#ifndef INT64_MODIFIER
#define INT64_MODIFIER "ll"
#endif

/*
 * ShmemVariableCache was renamed to TransamVariables.
 */
#ifndef ShmemVariableCache
#define ShmemVariableCache TransamVariables
#endif

/*
 * Resource-manager IDs.  zheap allocated dedicated rmgr IDs (RM_UNDOLOG_ID,
 * RM_UNDOACTION_ID) in rmgrlist.h.  Phase 2a explicitly does NOT wire the
 * engine into rmgrlist.h (that is Phase 2b), and no pbu redo routine is
 * registered or dispatched yet, so these only need to be valid RmgrId-typed
 * constants for the XLogInsert() call sites to compile.  We alias them to the
 * existing RM_UNDO_ID; the alias is inert until Phase 2b assigns real IDs.
 *
 * Phase 2b: RM_UNDOLOG_ID and RM_UNDOACTION_ID are now dedicated resource
 * manager IDs registered in rmgrlist.h (with the pbu undolog_redo/desc/
 * identify and undoaction_redo/desc/identify handlers), so the alias is gone.
 * The existing RM_UNDO_ID (undo_redo) engine is untouched and keeps its own
 * WAL stream.
 */

/*
 * Wait-event labels.  Several zheap-specific wait events were never merged
 * upstream.  They are observability labels only.  The per-backend undo file
 * I/O paths (pbu_undofile.c) use the existing DATA_FILE_* events directly, as
 * undo segments are ordinary data files; only the checkpoint-snapshot and
 * background-worker labels below remain aliased to the closest existing event.
 * These are purely cosmetic (they change the string shown in
 * pg_stat_activity.wait_event), so dedicated wait_event_names.txt entries are
 * a nice-to-have, not a correctness item; add them alongside the checkpoint
 * snapshot wiring (Phase 2d).
 */
#ifndef WAIT_EVENT_UNDO_CHECKPOINT_WRITE
#define WAIT_EVENT_UNDO_CHECKPOINT_WRITE	WAIT_EVENT_UNDO_FLUSH_SYNC
#endif
#ifndef WAIT_EVENT_UNDO_CHECKPOINT_SYNC
#define WAIT_EVENT_UNDO_CHECKPOINT_SYNC		WAIT_EVENT_UNDO_FLUSH_SYNC
#endif
#ifndef WAIT_EVENT_UNDO_CHECKPOINT_READ
#define WAIT_EVENT_UNDO_CHECKPOINT_READ		WAIT_EVENT_UNDO_FLUSH_SYNC
#endif
#ifndef WAIT_EVENT_UNDO_LAUNCHER_MAIN
#define WAIT_EVENT_UNDO_LAUNCHER_MAIN		WAIT_EVENT_UNDO_WORKER_MAIN
#endif
#ifndef WAIT_EVENT_UNDO_DISCARD_WORKER_MAIN
#define WAIT_EVENT_UNDO_DISCARD_WORKER_MAIN	WAIT_EVENT_UNDO_WORKER_MAIN
#endif
#ifndef WAIT_EVENT_BGWORKER_STARTUP
#define WAIT_EVENT_BGWORKER_STARTUP			WAIT_EVENT_BGWORKER_SHUTDOWN
#endif

/*
 * PROCARRAY_FLAGS_* and the two-argument GetOldestXmin() were replaced by
 * GetOldestNonRemovableTransactionId(Relation).  The flags selected which
 * process classes to ignore when computing the horizon.  The discard worker
 * calls this with a NULL relation, i.e. it wants the global oldest xid that
 * any snapshot might still need; GetOldestNonRemovableTransactionId(NULL) is
 * exactly that conservative global horizon.  Being conservative only makes
 * discard retain MORE undo than strictly necessary, never less, so it is safe:
 * no snapshot can ever observe a prematurely discarded undo record.  The alias
 * therefore preserves correctness (the zheap flags were a discard-aggressiveness
 * tuning knob, not a safety property).
 */
#ifndef PROCARRAY_FLAGS_DEFAULT
#define PROCARRAY_FLAGS_DEFAULT		0
#define PROCARRAY_FLAGS_VACUUM		0
#define PROCARRAY_FLAGS_AUTOVACUUM	0
#define GetOldestXmin(rel, flags)	GetOldestNonRemovableTransactionId(rel)
#endif

/*
 * Per-backend UNDO log freelist lock.  zheap used a built-in named LWLock
 * (UndoLogLock) from the main LWLock array.  To avoid touching the shared
 * lwlocklist.h in Phase 2a, the engine allocates its own singleton lock in
 * shared memory (see pbu_undolog.c PbuUndoLogShmemInit) and exposes it through
 * this global.
 */
#ifndef FRONTEND
extern PGDLLIMPORT LWLock *UndoLogLock_pbu;
#define UndoLogLock		UndoLogLock_pbu

/*
 * zheap used two more built-in named LWLocks: RollbackRequestLock (guards the
 * rollback request queues / hash table) and UndoWorkerLock (guards the undo
 * worker shmem slots).  Like UndoLogLock, the pbu engine allocates these as
 * its own singletons (see pbu_undolog.c PbuUndoLogShmemInit) to avoid touching
 * the shared lwlocklist.h in Phase 2a.
 */
extern PGDLLIMPORT LWLock *RollbackRequestLock_pbu;
extern PGDLLIMPORT LWLock *UndoWorkerLock_pbu;
#define RollbackRequestLock	RollbackRequestLock_pbu
#define UndoWorkerLock		UndoWorkerLock_pbu

/*
 * Oldest full XID that still has undo not yet discarded, published by the
 * discard worker and read by other backends.  zheap kept this as a
 * pg_atomic_uint64 field (oldestXidWithEpochHavingUndo) on the shared PROC_HDR
 * (ProcGlobal).  To avoid widening that shared struct in Phase 2a, the pbu
 * engine owns its own atomic in its shmem region (see pbu_undolog.c).
 */
#include "port/atomics.h"
extern PGDLLIMPORT pg_atomic_uint64 *UndoOldestXidHavingUndo_pbu;
#endif

/*
 * pbu-private LWLock tranche IDs.  Registered dynamically via
 * LWLockRegisterTranche() in pbu_undolog.c so we do not touch the shared
 * built-in tranche enum.  These IDs live above LWTRANCHE_FIRST_USER_DEFINED.
 */
#define LWTRANCHE_UNDOLOG		(LWTRANCHE_FIRST_USER_DEFINED + 1001)
#define LWTRANCHE_UNDODISCARD	(LWTRANCHE_FIRST_USER_DEFINED + 1002)
#define LWTRANCHE_DISCARD_UPDATE (LWTRANCHE_FIRST_USER_DEFINED + 1003)

/*
 * ReadBufferMode: zheap's bare RBM_ZERO ("zero the page, do not read from
 * disk, do not lock") was removed.  Master's RBM_ZERO_AND_LOCK has the same
 * meaning we need here -- do not read from disk, caller initializes the page --
 * and additionally takes the exclusive content lock.  pbu_undoinsert.c guards
 * every LockBuffer() with "if (rbm != RBM_ZERO_AND_LOCK)", so the alias is the
 * correct master mapping: the page is zeroed-not-read exactly as before and is
 * locked exactly once.  This is resolved, not a deferral.
 */
#ifndef RBM_ZERO
#define RBM_ZERO				RBM_ZERO_AND_LOCK
#endif

/*
 * OnCommitAction: zheap added ONCOMMIT_TEMP_DISCARD to discard a TEMP-persistence
 * undo log at commit.  Master's on-commit machinery (pg_on_commit_actions,
 * keyed by relation OID) has no such action, and only a table AM that stores
 * temp-table data in per-backend undo would ever register it.  No such AM
 * exists yet (FLUX migrates to per-backend undo in Phase 8; a temp-undo table
 * AM is later still), so aliasing to the no-op action is behaviourally exact
 * for this phase -- no temp undo is produced, so there is nothing to discard.
 *
 * ONCOMMIT_TEMP_DISCARD is therefore aliased to the no-op action.  Real
 * temp-undo discard-on-commit is needed once an AM stores temp data in
 * per-backend undo, which is also when the on-commit hook must key on undo log
 * number rather than relation OID.
 */
#ifndef ONCOMMIT_TEMP_DISCARD
#define ONCOMMIT_TEMP_DISCARD	ONCOMMIT_NOOP
#endif

/*
 * target_prefetch_pages (a global derived from effective_io_concurrency) was
 * removed; use effective_io_concurrency directly as the prefetch depth.
 */
#ifndef target_prefetch_pages
#define target_prefetch_pages	effective_io_concurrency
#endif

/*
 * RelFileNode was renamed to RelFileLocator (PG16); its fields spcNode/
 * dbNode/relNode became spcOid/dbOid/relNumber.  The zheap engine uses
 * RelFileNode purely as the physical storage locator, which is exactly what
 * RelFileLocator now is, so aliasing the type name lets the vendored code
 * compile unchanged.  Field accesses are fixed at their (few) sites.
 */
#ifndef FRONTEND
#define RelFileNode RelFileLocator
#endif

#endif							/* PBU_COMPAT_H */
