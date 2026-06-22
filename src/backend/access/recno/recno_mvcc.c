/*-------------------------------------------------------------------------
 *
 * recno_mvcc.c
 *	  RECNO heap-compatible xmin/xmax MVCC implementation
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/recno/recno_mvcc.c
 *
 * NOTES
 *	  RECNO uses ordinary heap-compatible xmin/xmax MVCC visibility, the same
 *	  model as HeapTupleSatisfiesMVCC: a tuple is visible to a snapshot iff its
 *	  inserter (t_xmin) is committed-and-visible-to-the-snapshot AND its
 *	  deleter/updater (t_xmax) is invalid, not committed, or not visible.  CLOG
 *	  is the commit-status authority; the snapshot's xmin/xmax/xip decide
 *	  visibility of committed XIDs; subtransaction aborts (ROLLBACK TO
 *	  savepoint) are handled by stamping t_xmin/t_xmax with the current subxid
 *	  so CLOG marks a rolled-back subxact's tuples aborted (plus a transient
 *	  sLog ABORTED marker consulted for the still-in-progress window).
 *
 *	  In-place UPDATE keeps the NEWEST version on the page (new t_xmin) and
 *	  pushes the pre-update image to the per-relation UNDO fork via t_verptr
 *	  (zheap style).  A snapshot that cannot see the updater's xmin reads the
 *	  old version back from the fork with RecnoReconstructVisibleVersion()
 *	  (recno_pvs.c), which walks t_verptr and stops at the version whose
 *	  producing xid is visible (XidInMVCCSnapshot).  The single visibility
 *	  function is RecnoTupleSatisfiesMVCC(), reached via
 *	  RecnoTupleVisibleToSnapshotDual() from every read site.
 *
 *	  The pre-commit "dirty read" window closes for free: a reader resolves
 *	  t_xmin against CLOG, which reports committed only at the durability point
 *	  (RecordTransactionCommit flushes the commit record).  A not-yet-committed
 *	  (hence not-yet-durable) inserter is simply invisible -- no HLC, no
 *	  timestamp rewind, no recno_cts durable map.
 *
 *	  The RECNO_TUPLE_UNCOMMITTED flag (0x0080) is set on insert/delete/update
 *	  and cleared (as a hint bit) at commit.  It no longer drives read
 *	  visibility (CLOG does); it is retained for the sLog write-conflict and
 *	  defrag paths.
 *
 * ISOLATION LEVEL SEMANTICS
 *
 *	  RECNO integrates with PostgreSQL's Serializable Snapshot Isolation
 *	  (SSI) infrastructure in predicate.c.  The scan path acquires SIREAD
 *	  predicate locks via PredicateLockTID(), and the DML paths (INSERT,
 *	  UPDATE, DELETE) call CheckForSerializableConflictIn() to detect
 *	  rw-antidependencies.  The RecnoCheckForSerializableConflictOut()
 *	  function handles the reverse direction (reader encounters a tuple
 *	  written by a concurrent transaction) by looking up the writer's
 *	  XID via the sLog and delegating to predicate.c.
 *
 *	  BEFORE-IMAGE SERVING:
 *
 *	  In-place UPDATEs destroy the pre-image on the page.  Under WS-PVS3 the
 *	  visible prior version is reconstructed by walking the durable UNDO fork
 *	  chain (RecnoReconstructVisibleVersion), not from any shared sLog DSA
 *	  entry: readers whose snapshot cannot see the committing xid follow the
 *	  on-page head verptr into the UNDO fork.  This restores correct snapshot
 *	  semantics for concurrent readers under REPEATABLE READ and SERIALIZABLE.
 *
 *	  CONCURRENCY:
 *
 *	  1. Same-tuple write-write conflicts serialize correctly: the
 *	     second writer blocks (via XactLockTableWait on the sLog dirty
 *	     XID) until the first commits or aborts.
 *
 *	  2. Write Skew (A5B) on disjoint tuples IS detected via predicate
 *	     locking (SIREAD locks on tuples read + conflict-in checks on
 *	     writes).
 *
 *	  In summary, RECNO's isolation guarantees are:
 *	  - READ COMMITTED: Correct (no dirty reads, each statement gets
 *	    fresh visibility via a per-statement snapshot)
 *	  - REPEATABLE READ: Full Snapshot Isolation; concurrent committed
 *	    UPDATEs are reconstructed from the UNDO fork per reader snapshot
 *	  - SERIALIZABLE: Full SSI via predicate.c integration; write skew
 *	    and phantom anomalies are prevented through predicate locking
 *	    and rw-antidependency cycle detection
 *
 *	  References:
 *	  - Berenson et al., "A Critique of ANSI SQL Isolation Levels" (1995)
 *	  - Adya, "Weak Consistency: A Generalized Theory and Optimistic
 *	    Implementations for Distributed Transactions" (2000)
 *	  - Cahill et al., "Serializable Isolation for Snapshot Databases" (2009)
 *	  - Ports & Grittner, "Serializable Snapshot Isolation in PostgreSQL" (2012)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/atm.h"
#include "access/recno.h"
#include "access/slog.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "utils/snapmgr.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "utils/memutils.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/predicate.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/timestamp.h"

/*
 * Total number of PGPROC slots, matching the allProcs array size in proc.c.
 * This must cover regular backends, auxiliary procs, and prepared transactions
 * since GetNumberFromPGProc() can return indices up to TotalProcs - 1.
 */
#define RECNO_TOTAL_PROCS \
	(MaxBackends + NUM_AUXILIARY_PROCS + max_prepared_xacts)

/*
 * Shared memory structures for MVCC
 */
typedef struct RecnoMvccShmemData
{
	LWLock		mvcc_lock;		/* Protects serializable horizon only */
	uint64		oldest_active_ts;	/* Cached oldest active transaction ts;
									 * backs RecnoGetOldestActiveTimestamp(), which
									 * still feeds the defrag WAL record and the VM
									 * all-visible check.  NOT a vacuum horizon. */
	uint64		serializable_horizon;	/* Serializable isolation horizon */

	pg_atomic_uint32 oldest_active_generation;	/* Bumped when cache is
												 * invalidated */
	pg_atomic_uint32 active_xact_count; /* Number of active transactions
										 * (atomic) */

	/*
	 * Per-backend active transaction start timestamps.  Each backend slot
	 * stores the start timestamp of its current RECNO transaction, or 0 if
	 * idle.  This array is indexed by pgprocno (the offset into
	 * ProcGlobal->allProcs) and is sized to RECNO_TOTAL_PROCS so that
	 * auxiliary procs and prepared transactions are covered.
	 *
	 * Each slot is written only by its owning backend and read by VACUUM, so
	 * no lock is needed — just a compiler barrier via volatile access.
	 */
	int			num_xact_slots; /* Number of slots (== RECNO_TOTAL_PROCS) */
	uint64		xact_start_ts_slots[FLEXIBLE_ARRAY_MEMBER];

}			RecnoMvccShmemData;

static RecnoMvccShmemData * RecnoMvccShmem = NULL;


/*
 * Per-transaction MVCC state.
 *
 * SSI (Serializable Snapshot Isolation) conflict detection is now delegated
 * entirely to PostgreSQL's predicate.c infrastructure.  RECNO integrates
 * with it by calling PredicateLockTID() in the scan path and
 * CheckForSerializableConflictIn/Out() in the DML paths.  The private
 * rw-conflict graph that was previously here has been removed.
 *
 * Commit visibility uses CLOG (heap-shaped xmin/xmax); the timestamp fields
 * below are page-level bookkeeping only.
 */
struct RecnoTransactionState
{
	uint64		xact_start_ts;	/* Transaction start timestamp */
	uint64		xact_commit_ts; /* Transaction commit timestamp */

	/*
	 * READ COMMITTED per-command read point.  Under RC the visibility read
	 * point is "now", refreshed each command so a scan sees concurrent
	 * commits.  Captured once per command (keyed by rc_read_point_cid) and
	 * reused for every tuple.
	 */
	CommandId	rc_read_point_cid;
	uint64		rc_read_point_xcc;	/* snapXactCompletionCount at capture */
	bool		is_serializable;	/* Serializable isolation level */
	bool		is_read_only;	/* Transaction has not performed writes */

	/*
	 * EPQ reconcile identity for the in-place write-write conflict path.
	 *
	 * When recno_tuple_update detects a committed concurrent UPDATE (head
	 * verptr's committer is invisible to snapshot) it returns TM_Updated
	 * and the executor runs EvalPlanQual: table_tuple_lock re-reads the
	 * latest on-page value (which, because RECNO updates in place, already
	 * reflects every commit so far) and the quals/SET expression recompute
	 * on top of it.  But EPQ re-evaluates against the same statement
	 * snapshot (same xid xip / curcid), so a naive probe would re-detect
	 * the same committed marker and livelock.  Heap escapes this because
	 * EPQ advances tupleid to a new tuple version with a clean xmax; RECNO
	 * has no new version to advance to.
	 *
	 * EPQ dedup: identity of the (verptr, xid) marker we bounced on to
	 * EvalPlanQual for this exact (relid, tid, cid).  If the next probe on
	 * the same tuple in the same EPQ retry finds an unchanged head verptr
	 * and xid, we already reconciled it -- skip to prevent livelock.  A
	 * strictly-newer committer stamps a NEW record with a fresh verptr, so
	 * dedup only suppresses re-firing on the identical marker.  No temporal
	 * ordering is required: this is a physical identity check.
	 */
	Oid			epq_reconcile_relid;
	ItemPointerData epq_reconcile_tid;
	CommandId	epq_reconcile_cid;
	RelUndoRecPtr epq_reconcile_verptr;
	TransactionId epq_reconcile_xid;
};

/* Restart reasons */
#define RECNO_RESTART_NONE				0
#define RECNO_RESTART_UNCERTAINTY		1
#define RECNO_RESTART_SERIALIZABLE		2

/*
 * Per-backend static transaction state.  Using a static struct avoids
 * a palloc/pfree cycle per transaction.  The struct is reset at the start
 * of each transaction by RecnoInitTransactionState().
 *
 * MyRecnoXactState points to &MyRecnoXactStateData when a transaction is
 * active, and is NULL between transactions.  This preserves the existing
 * NULL-check pattern throughout the codebase.
 */
static RecnoTransactionState MyRecnoXactStateData;
static RecnoTransactionState *MyRecnoXactState = NULL;

/*
 * GUC variables (recno_enable_serializable and recno_max_transactions removed;
 * SSI is unconditionally provided via predicate.c)
 */

/*
 * Function prototypes
 */
static void RecnoInitTransactionState(void);
static void RecnoCleanupTransactionState(void);
static void RecnoShmemExit(int code, Datum arg);

/* RecnoCheckUncommittedDelete removed -- logic inlined in visibility checks */


/*
 * Shared memory size calculation
 *
 * Per-transaction state (RecnoTransactionState) is allocated in
 * backend-local TopTransactionContext, NOT in shared memory, so it
 * does not appear here.  The only shared-memory array is the
 * per-backend xact_start_ts_slots[], which scales naturally with
 * RECNO_TOTAL_PROCS (and therefore MaxBackends).  During bootstrap
 * MaxBackends is ~4, keeping this allocation tiny.
 */
Size
RecnoMvccShmemSize(void)
{
	Size		size;

	/*
	 * Base struct (includes the flexible array header but not the array
	 * elements), plus one uint64 slot per PGPROC (regular backends, auxiliary
	 * procs, prepared transactions) for tracking active transaction start
	 * timestamps.
	 */
	size = offsetof(RecnoMvccShmemData, xact_start_ts_slots);
	size = add_size(size, mul_size(RECNO_TOTAL_PROCS, sizeof(uint64)));

	return size;
}

/*
 * Initialize shared memory for MVCC
 */
void
RecnoMvccShmemInit(void)
{
	bool		found;

	RecnoMvccShmem = (RecnoMvccShmemData *)
		ShmemInitStruct("RECNO MVCC Data",
						RecnoMvccShmemSize(),
						&found);

	if (!found)
	{
		int			total_procs = RECNO_TOTAL_PROCS;

		/* Initialize shared memory */
		LWLockInitialize(&RecnoMvccShmem->mvcc_lock, LWTRANCHE_BUFFER_MAPPING);
		RecnoMvccShmem->oldest_active_ts = 1;
		pg_atomic_init_u32(&RecnoMvccShmem->oldest_active_generation, 0);
		RecnoMvccShmem->serializable_horizon = 1;
		pg_atomic_init_u32(&RecnoMvccShmem->active_xact_count, 0);

		/* Initialize per-backend active timestamp slots to 0 (idle) */
		RecnoMvccShmem->num_xact_slots = total_procs;
		memset(RecnoMvccShmem->xact_start_ts_slots, 0,
			   total_procs * sizeof(uint64));
	}

	/* Register cleanup function */
	on_shmem_exit(RecnoShmemExit, 0);
}

/*
 * RecnoGetDmlTimestamp -- return the transaction's start timestamp for DML
 * page-level bookkeeping.
 *
 * Within a single transaction, all DML operations (INSERT, UPDATE, DELETE)
 * stamp the page-level commit-ts word (RecnoPageSetCommitTs) with the
 * transaction's start timestamp.
 *
 * This value is NOT a visibility timestamp.  Commit visibility comes from
 * CLOG via heap-shaped xmin/xmax: while the transaction is in-flight the
 * RECNO_TUPLE_UNCOMMITTED flag is set and self-visibility is determined by
 * matching the inserter's XID in the sLog; after commit, t_xmin/t_xmax + CLOG
 * decide visibility.  Intra-transaction ordering (multiple DMLs in the same
 * txn) is handled by CID (command ID) from the sLog entry.
 */
uint64
RecnoGetDmlTimestamp(void)
{
	/*
	 * Callers must have already called RecnoGetTransactionTimestamp() or
	 * equivalent, which initializes the transaction state.  We assert rather
	 * than lazily initializing, keeping this function as lean as possible on
	 * the hot path.
	 */
	Assert(MyRecnoXactState != NULL);

	return MyRecnoXactState->xact_start_ts;
}

/*
 * RecnoGetCommitTimestamp
 *
 * Return a wall-clock timestamp (microseconds since the PostgreSQL epoch)
 * used only for page-level commit-ts bookkeeping and the VM all-visible
 * hint.  Commit visibility comes from CLOG (heap-shaped xmin/xmax); nothing
 * reads this value for a correctness/visibility DECISION, so it does not
 * need strict cross-backend monotonicity.
 *
 * Historically this walked an atomic compare-and-swap loop over a single
 * shared global_commit_ts counter to keep timestamps strictly increasing.
 * That shared RMW was a hot-path serialization point (one cache line CAS'd
 * ~2x per write txn) with no surviving consumer of strict monotonicity, so
 * it has been replaced with a plain GetCurrentTimestamp() read: no shared
 * counter, no CAS, no spin.  Wall-clock microseconds are already
 * monotonic-enough for a "min over active start-stamps" horizon and a
 * ">= oldest" VM hint (both use non-strict comparisons).
 */
uint64
RecnoGetCommitTimestamp(void)
{
	if (RecnoMvccShmem == NULL)
		elog(ERROR, "RECNO MVCC not initialized");

	return (uint64) GetCurrentTimestamp();
}

/*
 * RecnoGetTransactionTimestamp
 *
 * Return the start timestamp of the current transaction.  Initializes
 * per-transaction MVCC state on first call within a transaction.
 *
 * Returns the transaction's start timestamp (uint64).
 */
uint64
RecnoGetTransactionTimestamp(void)
{
	if (MyRecnoXactState == NULL)
		RecnoInitTransactionState();

	return MyRecnoXactState->xact_start_ts;
}

/*
 * Subsystem callback wrappers for PG_SHMEM_SUBSYSTEM infrastructure
 */
static void
RecnoMvccShmemRequest(void *arg)
{
	ShmemRequestStruct(.name = "RECNO MVCC Data",
					   .size = RecnoMvccShmemSize(),
					   .ptr = (void **) &RecnoMvccShmem);
}

static void
RecnoMvccShmemInit_cb(void *arg)
{
	int			total_procs = RECNO_TOTAL_PROCS;

	/* RecnoMvccShmem is already set by the ShmemRequestStruct .ptr mechanism */
	Assert(RecnoMvccShmem != NULL);

	/* Initialize shared memory fields */
	LWLockInitialize(&RecnoMvccShmem->mvcc_lock, LWTRANCHE_BUFFER_MAPPING);
	RecnoMvccShmem->oldest_active_ts = 1;
	pg_atomic_init_u32(&RecnoMvccShmem->oldest_active_generation, 0);
	RecnoMvccShmem->serializable_horizon = 1;
	pg_atomic_init_u32(&RecnoMvccShmem->active_xact_count, 0);

	/* Initialize per-backend active timestamp slots to 0 (idle) */
	RecnoMvccShmem->num_xact_slots = total_procs;
	memset(RecnoMvccShmem->xact_start_ts_slots, 0,
		   total_procs * sizeof(uint64));

	/* Register cleanup function */
	on_shmem_exit(RecnoShmemExit, 0);
}

const ShmemCallbacks RecnoMvccShmemCallbacks = {
	.request_fn = RecnoMvccShmemRequest,
	.init_fn = RecnoMvccShmemInit_cb,
};

/*
 * Initialize per-transaction MVCC state
 *
 * The start timestamp is a plain wall-clock value from
 * RecnoGetCommitTimestamp() held in the uint64 xact_start_ts field for
 * per-backend slot tracking.
 */
/*
 * Transaction callback for RECNO MVCC cleanup.
 *
 * This is registered once per backend via RegisterXactCallback.
 * On transaction commit or abort, it calls RecnoCommitTransaction()
 * or RecnoCleanupTransactionState() to reset MyRecnoXactState,
 * ensuring the next transaction in this backend gets a fresh start
 * timestamp from RecnoGetCommitTimestamp().
 */
static bool recno_xact_callback_registered = false;

static void
RecnoXactCallback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
			RecnoCommitTransaction();
			break;

		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			RecnoCleanupTransactionState();
			break;

		case XACT_EVENT_PREPARE:

			/*
			 * At PREPARE, the transaction is still "in progress" for
			 * visibility purposes.  We must NOT clear the shared-memory
			 * xact_start_ts slot or decrement active_xact_count -- doing so
			 * would allow VACUUM to advance the oldest-active horizon past
			 * this prepared transaction's start timestamp, risking premature
			 * tuple pruning between PREPARE and COMMIT PREPARED.
			 *
			 * AtPrepare_Recno() has already relocated the pin from this
			 * backend's proc slot onto the prepared xact's dummy-proc slot
			 * (RecnoPrepareReassignSlot), so it survives this backend running
			 * new transactions or exiting; the backend that runs COMMIT/
			 * ROLLBACK PREPARED clears the dummy slot via
			 * RecnoResolvePreparedSlot() in the RECNO 2PC callbacks.  Here we
			 * only drop the backend-local pointer so this backend can start
			 * fresh transactions.
			 */
			MyRecnoXactState = NULL;
			break;

		default:
			/* Pre-commit, pre-prepare -- nothing to do */
			break;
	}
}

static void
RecnoInitTransactionState(void)
{
	if (MyRecnoXactState != NULL)
		return;

	/* Register cleanup callback on first use in this backend */
	if (!recno_xact_callback_registered)
	{
		RegisterXactCallback(RecnoXactCallback, NULL);
		recno_xact_callback_registered = true;
	}

	/* Use the static per-backend struct; zero it to start fresh */
	memset(&MyRecnoXactStateData, 0, sizeof(RecnoTransactionState));
	MyRecnoXactState = &MyRecnoXactStateData;

	/*
	 * Stamp the transaction start timestamp (monotonic wall-clock via
	 * RecnoGetCommitTimestamp).  This is a within-transaction bookkeeping
	 * value only; commit visibility comes from CLOG (heap-shaped xmin/xmax).
	 */
	MyRecnoXactState->xact_start_ts = RecnoGetCommitTimestamp();

	MyRecnoXactState->xact_commit_ts = 0;

	/* No RC per-command read point captured yet this transaction. */
	MyRecnoXactState->rc_read_point_cid = InvalidCommandId;
	MyRecnoXactState->rc_read_point_xcc = 0;
	MyRecnoXactState->is_serializable = (XactIsoLevel == XACT_SERIALIZABLE);
	MyRecnoXactState->is_read_only = true;	/* Until first write */

	/* Register in shared memory for oldest-active-timestamp tracking */
	if (RecnoMvccShmem != NULL)
	{
		int			my_slot = MyProc ? (int) GetNumberFromPGProc(MyProc) : -1;

		/*
		 * Write our start timestamp into our per-backend slot.  This is a
		 * single-writer/multi-reader pattern (only we write our slot, VACUUM
		 * reads it), so no lock is needed — just a write barrier.
		 */
		if (my_slot >= 0 && my_slot < RecnoMvccShmem->num_xact_slots)
		{
			pg_write_barrier();
			RecnoMvccShmem->xact_start_ts_slots[my_slot] =
				MyRecnoXactState->xact_start_ts;
		}

		pg_atomic_fetch_add_u32(&RecnoMvccShmem->active_xact_count, 1);

		/*
		 * If our start timestamp is older than the cached oldest, invalidate
		 * the cache by bumping the generation counter.
		 */
		if (MyRecnoXactState->xact_start_ts < RecnoMvccShmem->oldest_active_ts)
			pg_atomic_fetch_add_u32(&RecnoMvccShmem->oldest_active_generation, 1);
	}
}

/*
 * Cleanup per-transaction MVCC state
 */
static void
RecnoCleanupTransactionState(void)
{
	if (MyRecnoXactState == NULL)
		return;

	/*
	 * Clear our slot in shared memory.  No lock needed: each backend only
	 * writes its own slot, and the generation counter invalidates the cached
	 * oldest_active_ts when needed.
	 */
	if (RecnoMvccShmem != NULL)
	{
		int			my_slot = MyProc ? (int) GetNumberFromPGProc(MyProc) : -1;
		uint64		my_ts = MyRecnoXactState->xact_start_ts;

		/* Clear our per-backend slot */
		if (my_slot >= 0 && my_slot < RecnoMvccShmem->num_xact_slots)
		{
			RecnoMvccShmem->xact_start_ts_slots[my_slot] = 0;
			pg_write_barrier();
		}

		pg_atomic_fetch_sub_u32(&RecnoMvccShmem->active_xact_count, 1);

		/*
		 * Invalidate the cached oldest_active_ts if we might have been the
		 * oldest.  Bump the generation counter so that
		 * RecnoGetOldestActiveTimestamp() rescans on the next call. If no
		 * transactions remain, advance the cached value cheaply.
		 */
		if (pg_atomic_read_u32(&RecnoMvccShmem->active_xact_count) == 0)
		{
			RecnoMvccShmem->oldest_active_ts = (uint64) GetCurrentTimestamp();
			pg_atomic_fetch_add_u32(&RecnoMvccShmem->oldest_active_generation, 1);
		}
		else if (my_ts == RecnoMvccShmem->oldest_active_ts)
		{
			/*
			 * Only invalidate the cache when we were the actual oldest active
			 * transaction.  If my_ts < oldest_active_ts, the cached value was
			 * already advanced past us by another backend's rescan, so our
			 * departure cannot change the oldest.  Using strict equality
			 * instead of <= dramatically reduces invalidation frequency under
			 * high concurrency.
			 */
			pg_atomic_fetch_add_u32(&RecnoMvccShmem->oldest_active_generation, 1);
		}
	}

	MyRecnoXactState = NULL;
}

/*
 * RecnoPrepareReassignSlot -- move this backend's oldest-active-timestamp slot
 *		to the prepared transaction's dummy-proc slot.
 *
 * Called from AtPrepare_Recno() in the preparing backend.  A prepared xact is
 * still "active" for visibility until COMMIT/ROLLBACK PREPARED, so its start
 * timestamp must keep pinning the vacuum horizon -- but the preparing backend
 * is about to become free to run new transactions (and may exit entirely).
 * Keying the pin by the backend's own proc slot (as the in-progress path does)
 * would orphan the pin the moment the backend exits, or let the resolving
 * backend -- a different proc slot -- fail to find it.  Relocate the pin to
 * the dummy PGPROC slot the gxact owns for its whole prepared lifetime;
 * RecnoResolvePreparedSlot() clears exactly that slot when the xact resolves.
 *
 * active_xact_count is left unchanged: the transaction is still active, the
 * pin simply moved slots.  No-op if this backend never opened a RECNO xact.
 */
void
RecnoPrepareReassignSlot(int dummy_slot)
{
	int			my_slot;
	uint64		my_ts;

	if (RecnoMvccShmem == NULL || MyRecnoXactState == NULL)
		return;

	my_slot = MyProc ? (int) GetNumberFromPGProc(MyProc) : -1;
	my_ts = MyRecnoXactState->xact_start_ts;

	if (dummy_slot < 0 || dummy_slot >= RecnoMvccShmem->num_xact_slots)
		return;

	/* Publish the pin at the dummy slot, then release the backend slot. */
	RecnoMvccShmem->xact_start_ts_slots[dummy_slot] = my_ts;
	pg_write_barrier();
	if (my_slot >= 0 && my_slot < RecnoMvccShmem->num_xact_slots)
		RecnoMvccShmem->xact_start_ts_slots[my_slot] = 0;

	/* Force a horizon rescan so both moves are observed. */
	pg_atomic_fetch_add_u32(&RecnoMvccShmem->oldest_active_generation, 1);
}

/*
 * RecnoResolvePreparedSlot -- clear a prepared xact's dummy-proc slot.
 *
 * Called from the resolving backend (recno_twophase_postcommit /
 * recno_twophase_postabort) once COMMIT/ROLLBACK PREPARED finishes.  Mirrors
 * the tail of RecnoCleanupTransactionState() but operates on the gxact's dummy
 * slot rather than the resolver's own proc slot.  Idempotent: clearing an
 * already-zero slot only decrements the active count and bumps the generation
 * when the slot actually held a pin, so it is safe to call once per 2PC record.
 */
void
RecnoResolvePreparedSlot(int dummy_slot)
{
	uint64		slot_ts;

	if (RecnoMvccShmem == NULL)
		return;
	if (dummy_slot < 0 || dummy_slot >= RecnoMvccShmem->num_xact_slots)
		return;

	slot_ts = RecnoMvccShmem->xact_start_ts_slots[dummy_slot];
	if (slot_ts == 0)
		return;					/* already cleared -- nothing to do */

	RecnoMvccShmem->xact_start_ts_slots[dummy_slot] = 0;
	pg_write_barrier();

	pg_atomic_fetch_sub_u32(&RecnoMvccShmem->active_xact_count, 1);

	if (pg_atomic_read_u32(&RecnoMvccShmem->active_xact_count) == 0)
	{
		RecnoMvccShmem->oldest_active_ts = (uint64) GetCurrentTimestamp();
		pg_atomic_fetch_add_u32(&RecnoMvccShmem->oldest_active_generation, 1);
	}
	else if (slot_ts <= RecnoMvccShmem->oldest_active_ts)
		pg_atomic_fetch_add_u32(&RecnoMvccShmem->oldest_active_generation, 1);
}

/*
 * SSI conflict detection is handled by PostgreSQL's predicate.c infrastructure
 * via CheckForSerializableConflictIn/Out calls in the DML and scan paths.
 * The RecnoCheckSerializableConflict compatibility stub has been removed.
 */

/*
 * RecnoCheckForSerializableConflictOut -- detect rw-conflicts where a
 * serializable reader encounters a tuple written by a concurrent transaction.
 *
 * This is the RECNO equivalent of HeapCheckForSerializableConflictOut.
 * It determines the XID of the concurrent writer via the sLog and delegates
 * to the core CheckForSerializableConflictOut() in predicate.c.
 *
 * Called when a serializable transaction encounters a tuple that is not
 * visible to our snapshot (concurrent insert or concurrent delete/update
 * that made the tuple disappear).
 */
void
RecnoCheckForSerializableConflictOut(Relation relation,
									 RecnoTupleHeader *tuple,
									 Buffer buffer,
									 Snapshot snapshot)
{
	TransactionId xid;
	bool		is_insert;

	if (!CheckForSerializableConflictOutNeeded(relation, snapshot))
		return;

	/*
	 * Determine the writer's XID.  For RECNO, the tuple header doesn't store
	 * XIDs — we get them from the sLog.
	 */
	xid = SLogTupleGetDirtyXid(RelationGetRelid(relation),
							   &tuple->t_ctid, &is_insert);

	if (!TransactionIdIsValid(xid))
	{
		/*
		 * No in-progress writer found.  The writer already committed and its
		 * sLog entries were cleaned up.  In this case, the conflicting
		 * transaction committed so long ago that it's no longer tracked.  No
		 * conflict to report — analogous to heap's HEAPTUPLE_DEAD case.
		 */
		return;
	}

	/* Skip conflicts with our own transaction */
	if (TransactionIdIsCurrentTransactionId(xid))
		return;

	/* Get top-level XID for subtransaction support */
	xid = SubTransGetTopmostTransaction(xid);

	/* Skip if too old to be a concurrent transaction */
	if (TransactionIdPrecedes(xid, TransactionXmin))
		return;

	CheckForSerializableConflictOut(relation, xid, snapshot);
}

/*
 * Commit the current transaction and assign commit timestamp.
 *
 * The commit timestamp is a monotonic wall-clock value (RecnoGetCommitTimestamp)
 * used only for page-level bookkeeping; commit visibility comes from CLOG
 * (heap-shaped xmin/xmax MVCC).
 */
void
RecnoCommitTransaction(void)
{
	if (MyRecnoXactState == NULL)
		return;

	MyRecnoXactState->xact_commit_ts = RecnoGetCommitTimestamp();

	/* Update serializable horizon (only for serializable transactions) */
	if (RecnoMvccShmem != NULL && MyRecnoXactState->is_serializable)
	{
		LWLockAcquire(&RecnoMvccShmem->mvcc_lock, LW_EXCLUSIVE);
		RecnoMvccShmem->serializable_horizon =
			Min(RecnoMvccShmem->serializable_horizon,
				MyRecnoXactState->xact_commit_ts);
		LWLockRelease(&RecnoMvccShmem->mvcc_lock);
	}

	RecnoCleanupTransactionState();
}

/*
 * Abort the current transaction
 */
void
RecnoAbortTransaction(void)
{
	if (MyRecnoXactState == NULL)
		return;

	RecnoCleanupTransactionState();
}

/*
 * Get snapshot timestamp for reads
 */
uint64
RecnoGetSnapshotTimestamp(Snapshot snapshot)
{
	if (IsMVCCSnapshot(snapshot))
	{
		if (MyRecnoXactState == NULL)
			RecnoInitTransactionState();

		/*
		 * REPEATABLE READ / SERIALIZABLE: return transaction-start timestamp
		 * for a consistent point-in-time snapshot across all statements.
		 */
		if (IsolationUsesXactSnapshot())
			return MyRecnoXactState->xact_start_ts;

		/*
		 * READ COMMITTED: return current timestamp so each visibility check
		 * sees the latest committed state.
		 */
		return (uint64) RecnoGetCommitTimestamp();
	}
	else
	{
		/* SnapshotAny or other non-MVCC snapshots */
		return 0;
	}
}

/*
 * Check if tuple is visible to the given snapshot
 */
bool
RecnoTupleVisibleToSnapshot(RecnoTupleHeader *tuple, Snapshot snapshot,
							Oid relid, Buffer buffer)
{
	/* Heap-shaped: forward to the single xmin/xmax visibility entry point. */
	return RecnoTupleVisibleToSnapshotDual(tuple, snapshot, relid, buffer);
}

/*
 * Invalidate the cached oldest active timestamp, forcing the next call
 * to RecnoGetOldestActiveTimestamp() to rescan all per-backend slots.
 *
 * Also callable from VACUUM or any code that needs to force a refresh.
 */
void
RecnoUpdateOldestActiveTimestamp(void)
{
	if (RecnoMvccShmem == NULL)
		return;

	pg_atomic_fetch_add_u32(&RecnoMvccShmem->oldest_active_generation, 1);
}

/*
 * Per-backend cache of the oldest-active-timestamp computation.
 * Avoids rescanning all per-backend slots on every call; only rescans
 * when the global generation counter has been bumped.
 */
static uint32 my_oldest_active_gen = 0;
static uint64 my_oldest_active_cached = 0;

/*
 * RecnoGetOldestActiveTimestamp -- return the oldest active transaction's
 * start timestamp.
 *
 * This is the RECNO analog of PostgreSQL's GetOldestNonRemovableTransactionId.
 * VACUUM uses this to determine which deleted tuples can be safely removed:
 * a deleted tuple whose commit timestamp is older than this value is no
 * longer visible to any running transaction and can be reclaimed.
 *
 * If no transactions are active, returns the current global commit timestamp,
 * meaning all committed deletions are eligible for cleanup.
 *
 * Uses a per-backend cache that is invalidated when the global generation
 * counter changes.  No LWLock acquisition needed in the common case.
 */
uint64
RecnoGetOldestActiveTimestamp(void)
{
	uint32		current_gen;

	if (RecnoMvccShmem == NULL)
		elog(ERROR, "RECNO MVCC not initialized");

	/* Fast path: check if our cached value is still valid */
	current_gen = pg_atomic_read_u32(&RecnoMvccShmem->oldest_active_generation);
	if (current_gen == my_oldest_active_gen && my_oldest_active_cached != 0)
		return my_oldest_active_cached;

	/* Slow path: rescan all per-backend slots (lockless) */
	{
		uint64		oldest = 0;
		int			i;

		pg_read_barrier();

		for (i = 0; i < RecnoMvccShmem->num_xact_slots; i++)
		{
			uint64		ts = RecnoMvccShmem->xact_start_ts_slots[i];

			if (ts != 0 && (oldest == 0 || ts < oldest))
				oldest = ts;
		}

		if (oldest == 0)
			oldest = (uint64) GetCurrentTimestamp();

		/* Update the shared cached value (benign race with other backends) */
		RecnoMvccShmem->oldest_active_ts = oldest;

		/* Cache locally */
		my_oldest_active_cached = oldest;
		my_oldest_active_gen = current_gen;

		return oldest;
	}
}

/*
 * Get MVCC statistics
 */
void
RecnoGetMvccStats(uint64 *current_ts, uint64 *oldest_ts, int *active_xacts)
{
	if (RecnoMvccShmem == NULL)
	{
		*current_ts = 0;
		*oldest_ts = 0;
		*active_xacts = 0;
		return;
	}

	*current_ts = (uint64) GetCurrentTimestamp();
	*oldest_ts = RecnoMvccShmem->oldest_active_ts;
	*active_xacts = (int) pg_atomic_read_u32(&RecnoMvccShmem->active_xact_count);
}

/*
 * Shared memory exit cleanup
 */
static void
RecnoShmemExit(int code, Datum arg)
{
	RecnoCleanupTransactionState();
}


/*
 * RecnoTupleHasCommittedUpdateAfter -- fork-driven lost-update conflict probe.
 *
 * A RECNO in-place UPDATE stamps a trailing verptr on the new on-page image
 * (WS-PVS1) pointing at the UNDO-fork record it just wrote (WS-PVS3 PVS).
 * That verptr is the head of the tuple's version chain: the record it
 * refers to describes the update that PRODUCED the current on-page image,
 * so its urec_xid IS the last committer of this tuple.  Older commits have
 * already been absorbed into the on-page bytes we are about to overwrite;
 * they are not lost-update candidates.  A single-step probe is therefore
 * sufficient.
 *
 * Conflict iff:
 *   1) the head verptr resolves (RelUndoReadRecordHeader returns true), and
 *   2) urec_xid is invisible to snapshot (XidInMVCCSnapshot returns true),
 *      and
 *   3) urec_xid is not our own xid, and
 *   4) urec_xid did commit (guarding against in-progress/aborted xids that
 *      the writer has yet to reach the wait path for).
 *
 * If RelUndoReadRecordHeader returns false the record was discarded --
 * WS-PVS4's oldest_xmin discard gate guarantees the committer xid then
 * precedes every live snapshot's xmin, so it is visible to every reader and
 * cannot be a lost-update candidate.  Safe terminator.
 *
 * *out_head_verptr and *out_head_xid receive the observed (verptr, xid)
 * identity so the caller can pass them to RecnoEpqReconcileMark before
 * returning TM_Updated, and to RecnoEpqReconcileMatches to skip a
 * previously-bounced marker.
 */
bool
RecnoTupleHasCommittedUpdateAfter(Relation rel,
								  const RecnoTupleHeader *tuple,
								  Size tuple_len,
								  Snapshot snapshot,
								  TransactionId exclude_xid,
								  RelUndoRecPtr *out_head_verptr,
								  TransactionId *out_head_xid,
								  bool *out_inprogress)
{
	RelUndoRecPtr head;
	RelUndoRecordHeader hdr;

	if (out_head_verptr != NULL)
		*out_head_verptr = InvalidRelUndoRecPtr;
	if (out_head_xid != NULL)
		*out_head_xid = InvalidTransactionId;
	if (out_inprogress != NULL)
		*out_inprogress = false;

	if (snapshot == NULL || !IsMVCCSnapshot(snapshot))
		return false;

	head = RecnoTupleGetVersionPtr(tuple, tuple_len);
	if (!RelUndoRecPtrIsValid(head))
		return false;			/* never updated -- no committed conflict */

	if (!RelUndoReadRecordHeader(rel, head, &hdr))
		return false;			/* discarded -> visible to every snapshot */

	if (out_head_verptr != NULL)
		*out_head_verptr = head;
	if (out_head_xid != NULL)
		*out_head_xid = hdr.urec_xid;

	if (TransactionIdIsValid(exclude_xid) &&
		TransactionIdEquals(hdr.urec_xid, exclude_xid))
		return false;			/* our own update, not a conflict */

	if (!TransactionIdIsValid(hdr.urec_xid))
		return false;

	/*
	 * XidInMVCCSnapshot(xid, snap) returns true iff the xid is NOT visible to
	 * snap -- i.e. in-progress or later than the snapshot's xmax.  A conflict
	 * requires that the head committer be invisible to the writer's snapshot
	 * AND actually committed (an in-progress or aborted xid is handled by the
	 * writer's dirty-xid / abort path, not here).
	 */
	if (!XidInMVCCSnapshot(hdr.urec_xid, snapshot))
		return false;

	/*
	 * The head committer is invisible to our snapshot and is not our own xid.
	 * Normally it must have committed to be a lost-update conflict.  But there
	 * is a commit-visibility window: the committer's PRE_COMMIT callback
	 * removes its in-progress sLog marker and clears the on-page UNCOMMITTED
	 * flag BEFORE RecordTransactionCommit() marks CLOG and BEFORE
	 * ProcArrayEndTransaction() clears it from the running list.  In that
	 * window TransactionIdDidCommit() is still false while
	 * TransactionIdIsInProgress() is still true, and the sLog marker the
	 * writer-wait path keys off is already gone.  If we returned "no conflict"
	 * here, a concurrent updater would clobber the just-committed value with
	 * no 40001 / EPQ -- a lost update.  Signal the caller to wait on the
	 * in-flight head_xid (heap semantics: see live modifier -> wait -> re-check
	 * CLOG) rather than treating not-yet-committed as no-conflict.
	 */
	if (!TransactionIdDidCommit(hdr.urec_xid))
	{
		if (out_inprogress != NULL &&
			TransactionIdIsInProgress(hdr.urec_xid))
			*out_inprogress = true;
		return false;
	}

	return true;
}

/*
 * RecnoEpqReconcileMatches -- true iff we already bounced this exact
 * (relid, tid, cid, head_verptr, head_xid) marker to EvalPlanQual.
 *
 * A strictly-newer committer stamps a NEW UNDO record with a fresh
 * (blkno, offset, counter) triple, so an unchanged head verptr and xid
 * imply the observed marker is the identical one EPQ already reconciled.
 * Physical identity, no temporal ordering.
 */
bool
RecnoEpqReconcileMatches(Snapshot snapshot, Oid relid, ItemPointer tid,
						 RelUndoRecPtr head_verptr, TransactionId head_xid)
{
	return MyRecnoXactState != NULL &&
		TransactionIdIsValid(MyRecnoXactState->epq_reconcile_xid) &&
		MyRecnoXactState->epq_reconcile_relid == relid &&
		MyRecnoXactState->epq_reconcile_cid == snapshot->curcid &&
		ItemPointerEquals(&MyRecnoXactState->epq_reconcile_tid, tid) &&
		MyRecnoXactState->epq_reconcile_verptr == head_verptr &&
		TransactionIdEquals(MyRecnoXactState->epq_reconcile_xid, head_xid);
}

/*
 * RecnoEpqReconcileMark -- record the (verptr, xid) marker identity we are
 * bouncing to EvalPlanQual so the retry does not re-report it.
 */
void
RecnoEpqReconcileMark(Snapshot snapshot, Oid relid, ItemPointer tid,
					  RelUndoRecPtr head_verptr, TransactionId head_xid)
{
	if (MyRecnoXactState == NULL)
		RecnoInitTransactionState();

	MyRecnoXactState->epq_reconcile_relid = relid;
	ItemPointerCopy(tid, &MyRecnoXactState->epq_reconcile_tid);
	MyRecnoXactState->epq_reconcile_cid = snapshot->curcid;
	MyRecnoXactState->epq_reconcile_verptr = head_verptr;
	MyRecnoXactState->epq_reconcile_xid = head_xid;
}

/*
 * RecnoTupleSatisfiesMVCC -- heap-compatible xmin/xmax visibility check.
 *
 * This is the single visibility function for RECNO's heap-shaped MVCC model.
 * It mirrors HeapTupleSatisfiesMVCC: a tuple is visible to `snapshot` iff its
 * inserter (t_xmin) is committed-and-visible-to-the-snapshot AND its
 * deleter/updater (t_xmax) is invalid, not committed, or not visible to the
 * snapshot.  CLOG is the authoritative commit-status oracle; the snapshot's
 * xmin/xmax/xip decide visibility of committed XIDs.  The sLog is consulted
 * ONLY for the command-id (cid) of the current transaction's own uncommitted
 * work, which RECNO stores there instead of in the tuple header.
 *
 * The pre-commit "dirty read" window closes for free here: a reader resolves
 * t_xmin against CLOG, and CLOG marks a transaction committed only at the
 * durability point (RecordTransactionCommit flushes the commit record before
 * ProcArrayEndTransaction / TransactionIdDidCommit reports true for a
 * crash-safe reader path).  A not-yet-committed inserter -- which includes a
 * not-yet-durable one -- is simply invisible.  No HLC, no recno_cts durable
 * map, no timestamp rewind.
 *
 * `curcid` is the reader's command id for MVCC snapshots (InvalidCommandId
 * for SELF/ANY, which see all of the current transaction's work).
 */

/*
 * RecnoSetHintBits -- cache a CLOG commit result on the tuple as a hint bit.
 *
 * Mirrors heap's SetHintBits: once TransactionIdDidCommit() has confirmed a
 * tuple's xmin (or xmax) committed, record it in t_flags so subsequent
 * visibility checks skip the CLOG SLRU lookup entirely.  This is the
 * optimization that keeps a hot-set scan off the CLOG buffer LWLock (the
 * profiled cause of RECNO's post-HLC write-scaling regression: without it,
 * every tuple examined by every scan did a TransactionIdDidCommit -> CLOG
 * lookup, serializing on the CLOG buffer lock).  Non-WAL hint write: the
 * flag is reconstructible from CLOG, so a torn/lost hint is harmless.
 */
static inline void
RecnoSetHintBits(RecnoTupleHeader *tuple, Buffer buffer, uint16 hint)
{
	/*
	 * Cache the CLOG result as a hint bit, but ONLY when we hold the buffer
	 * content lock: BufferSetHintBits16 (like MarkBufferDirtyHint) errors on an
	 * unlocked buffer, and RecnoTupleSatisfiesMVCC is reached from some paths
	 * (post-VACUUM reads, replication reads, detached-tuple checks) where the
	 * buffer is pinned but not lock-held.  If we can't safely set the shared
	 * hint, just skip it -- the result is still correct, the CLOG lookup simply
	 * isn't cached on this call; the next scan holding the lock will cache it.
	 * Setting the in-memory bit without the lock would race a concurrent
	 * writer's t_flags update, so we do nothing rather than a bare |=.
	 */
	if (BufferIsValid(buffer) && BufferIsLockedByMe(buffer))
		BufferSetHintBits16(&tuple->t_flags,
							tuple->t_flags | hint, buffer);
}

static bool
RecnoTupleSatisfiesMVCC(RecnoTupleHeader *tuple, Snapshot snapshot,
						Oid relid, CommandId curcid, Buffer buffer)
{
	TransactionId xmin;
	TransactionId xmax;

	if (tuple == NULL)
		return false;

	xmin = RecnoTupleGetXmin(tuple);
	xmax = RecnoTupleGetXmax(tuple);

	/*
	 * SnapshotAny / non-MVCC "see everything": show every non-deleted tuple.
	 * A committed-and-visible xmax still hides it (the row is gone), but for
	 * SnapshotAny the caller wants raw existence, so only the DELETED flag
	 * matters.  Callers that need SNAPSHOT_DIRTY semantics handle that
	 * separately in recno_handler.c before reaching here.
	 */
	if (!IsMVCCSnapshot(snapshot))
		return !(tuple->t_flags & RECNO_TUPLE_DELETED) ||
			!TransactionIdIsValid(xmax);

	/* ---- xmin side: is the inserter committed-and-visible? ---- */
	if (!TransactionIdIsValid(xmin))
		return false;			/* no inserter -- cannot be visible */

	if (TransactionIdIsCurrentTransactionId(xmin))
	{
		/*
		 * Inserted by our own transaction.  Visible unless: (a) it was
		 * inserted by a later command (curcid ordering), or (b) the
		 * subtransaction that inserted it was rolled back to a savepoint --
		 * RECNO records that as an SLOG_OP_ABORTED entry on the tuple's TID
		 * (RECNO stamps t_xmin with the TOP xid, so TransactionIdIsCurrent is
		 * still true for a rolled-back subxact's tuple; the sLog ABORTED marker
		 * is the authoritative subxact-abort signal).  Consult the sLog while
		 * the insert is still uncommitted; a committed self-insert from an
		 * earlier command is unconditionally visible.
		 */
		if (tuple->t_flags & RECNO_TUPLE_UNCOMMITTED)
		{
			SLogTupleOp e[SLOG_MAX_TUPLE_OPS];
			int			n = SLogTupleLookupFiltered(relid, &tuple->t_ctid,
													InvalidTransactionId,
													e, SLOG_MAX_TUPLE_OPS);
			int			i;

			for (i = 0; i < n; i++)
			{
				/* Savepoint rollback marked this op aborted -> invisible. */
				if (e[i].op_type == SLOG_OP_ABORTED)
					return false;
				/* Our own later-command insert is not yet visible. */
				if (e[i].op_type == SLOG_OP_INSERT &&
					TransactionIdEquals(e[i].xid, xmin) &&
					curcid != InvalidCommandId && e[i].cid >= curcid)
					return false;
			}
		}
		/* our insert, visible so far -- fall through to xmax check */
	}
	else if (XidInMVCCSnapshot(xmin, snapshot))
		return false;			/* inserter not yet visible to snapshot */
	else if (tuple->t_flags & RECNO_TUPLE_XMIN_COMMITTED)
	{
		/* CLOG hint already set: inserter is committed, skip the CLOG lookup */
	}
	else if (!TransactionIdDidCommit(xmin))
		return false;			/* inserter aborted / in-flight-not-durable */
	else
	{
		/*
		 * Inserter committed.  Cache it as a hint bit (like heap's
		 * HEAP_XMIN_COMMITTED) so subsequent visibility checks skip the CLOG
		 * SLRU lookup -- this is what keeps a hot-set scan off the CLOG buffer
		 * LWLock.  Non-WAL hint write via MarkBufferDirtyHint.
		 */
		RecnoSetHintBits(tuple, buffer, RECNO_TUPLE_XMIN_COMMITTED);
	}

	/* Inserter is committed-and-visible.  Now the xmax (deleter) side. */

	if (!TransactionIdIsValid(xmax) ||
		!(tuple->t_flags & (RECNO_TUPLE_DELETED | RECNO_TUPLE_UPDATED)))
	{
		/*
		 * No deleter, or the tuple carries no delete/supersede marker: the
		 * on-page image is live for this snapshot.
		 *
		 * NOTE (in-place-UPDATE / zheap read path): when this tuple was
		 * updated in place, the on-page image is the NEWEST version and its
		 * xmin is the updater.  If that updater is invisible to `snapshot`,
		 * the xmin check above already returned false, and the caller
		 * (recno_handler.c) reconstructs the older visible version from the
		 * UNDO fork via RecnoReconstructVisibleVersion().  So an old snapshot
		 * never sees the wrong (too-new) bytes: it is either hidden here and
		 * reconstructed, or the updater is visible and the new image is
		 * correct.
		 */
		return true;
	}

	/* There is a deleter/updater xmax; resolve it heap-style. */
	if (TransactionIdIsCurrentTransactionId(xmax))
	{
		/*
		 * Deleted/superseded by our own transaction.  Invisible to us unless
		 * the delete happened in a later command (then we still see the row).
		 * Consult the sLog for the delete cid while it is uncommitted.
		 */
		if (curcid != InvalidCommandId &&
			(tuple->t_flags & RECNO_TUPLE_UNCOMMITTED))
		{
			SLogTupleOp e[SLOG_MAX_TUPLE_OPS];
			int			n = SLogTupleLookupFiltered(relid, &tuple->t_ctid,
													xmax, e, SLOG_MAX_TUPLE_OPS);
			int			i;

			for (i = 0; i < n; i++)
			{
				if ((e[i].op_type == SLOG_OP_DELETE ||
					 e[i].op_type == SLOG_OP_UPDATE) && e[i].cid >= curcid)
					return true; /* deleted after our scan started */
			}
		}
		return false;			/* our own delete, visible to this command */
	}

	if (XidInMVCCSnapshot(xmax, snapshot))
		return true;			/* deleter not yet visible -- row still here */

	if (tuple->t_flags & RECNO_TUPLE_XMAX_COMMITTED)
		return false;			/* CLOG hint: deleter committed -- gone */

	if (!TransactionIdDidCommit(xmax))
		return true;			/* deleter aborted / in-flight -- row still here */

	/* deleter committed-and-visible -- gone.  Cache the CLOG result. */
	RecnoSetHintBits(tuple, buffer, RECNO_TUPLE_XMAX_COMMITTED);
	return false;
}

/*
 * RecnoTupleVisibleToSnapshotDual -- primary visibility entry point.
 *
 * Formerly routed to HLC vs legacy timestamp visibility; RECNO now uses one
 * heap-compatible xmin/xmax model, so this just forwards to
 * RecnoTupleSatisfiesMVCC.  The name and signature are kept because ~35 call
 * sites route through here.
 */
bool
RecnoTupleVisibleToSnapshotDual(RecnoTupleHeader *tuple, Snapshot snapshot,
								Oid relid, Buffer buffer)
{
	/*
	 * Only apply CID filtering for MVCC snapshots.  SNAPSHOT_SELF and
	 * SNAPSHOT_ANY must see all of the current transaction's work.
	 */
	return RecnoTupleSatisfiesMVCC(tuple, snapshot, relid,
								   (snapshot != NULL &&
									snapshot->snapshot_type == SNAPSHOT_MVCC)
								   ? snapshot->curcid : InvalidCommandId,
								   buffer);
}

/*
 * RecnoGetOldestXminHorizon -- XID retention horizon for VACUUM/prune.
 *
 * Heap-shaped replacement for the former HLC oldest_snapshot_hlc horizon.  A
 * committed-deleted tuple whose deleter (t_xmax) precedes this horizon is
 * invisible to every current and future snapshot and can be physically
 * removed.  Uses the standard non-removable-transaction horizon so it tracks
 * the cluster's oldest snapshot xmin exactly like heap VACUUM.
 */
TransactionId
RecnoGetOldestXminHorizon(Relation rel)
{
	return GetOldestNonRemovableTransactionId(rel);
}

/*
 * RecnoTupleDeadToAll -- true iff a committed-deleted tuple can be reclaimed.
 *
 * Heap-shaped: the tuple must carry a committed DELETE marker (DELETED set,
 * UNCOMMITTED clear) whose deleter XID (t_xmax) is committed and older than
 * the supplied oldest-xmin horizon, so no active or future snapshot can see
 * it.  Replaces the old `t_commit_ts < oldest_ts` timestamp comparison, which
 * is meaningless now that the t_commit_ts word holds an XID, not a timestamp.
 */
bool
RecnoTupleDeadToAll(RecnoTupleHeader *tuple, TransactionId oldest_xmin)
{
	TransactionId xmax;

	if (!(tuple->t_flags & RECNO_TUPLE_DELETED))
		return false;
	if (tuple->t_flags & RECNO_TUPLE_UNCOMMITTED)
		return false;

	xmax = RecnoTupleGetXmax(tuple);
	if (!TransactionIdIsValid(xmax))
		return false;
	if (!TransactionIdDidCommit(xmax))
		return false;			/* delete aborted / in flight: keep */

	return TransactionIdPrecedes(xmax, oldest_xmin);
}
