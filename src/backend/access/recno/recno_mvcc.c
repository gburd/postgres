/*-------------------------------------------------------------------------
 *
 * recno_mvcc.c
 *	  RECNO time-based MVCC implementation (sLog-based)
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/recno/recno_mvcc.c
 *
 * NOTES
 *	  This implements time-based MVCC using commit timestamps (HLC) and
 *	  the sLog for in-progress transaction tracking.  The tuple header
 *	  no longer carries t_xmin, t_xmax, or t_xact_ts; the sole MVCC
 *	  field is t_commit_ts (HLC timestamp).  Transient operation state
 *	  (who is inserting/deleting/locking) is tracked in the sLog, not
 *	  in the tuple header.
 *
 *	  The RECNO_TUPLE_UNCOMMITTED flag (0x0080) is set on insert and
 *	  cleared at commit.  When this flag is set, the sLog must be
 *	  consulted to determine visibility.
 *
 *	  HLC is the sole clock mechanism.  MultiXact support has been
 *	  removed; concurrent tuple locking is tracked via the sLog.
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
 *	  In-place UPDATEs destroy the pre-image on the page, but the shared
 *	  sLog DSA retains committed UPDATE entries with before-images until
 *	  no active snapshot needs them.  The scan path
 *	  (recno_scan_getnextslot) checks for RECNO_TUPLE_UPDATED tuples
 *	  and serves DSA-resident before-images to readers whose snapshot
 *	  predates the commit_hlc.  This restores correct snapshot semantics
 *	  for concurrent readers under REPEATABLE READ and SERIALIZABLE.
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
 *	    fresh visibility via per-statement HLC snapshot)
 *	  - REPEATABLE READ: Full Snapshot Isolation; concurrent committed
 *	    UPDATEs are served from before-images per reader snapshot
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

/* External functions from recno_hlc.c */
extern HLCTimestamp HLCGetGlobal(void);
extern void HLCGetUncertaintyInterval(HLCTimestamp hlc,
									  HLCTimestamp *lower,
									  HLCTimestamp *upper);
extern bool HLCInUncertaintyWindow(HLCTimestamp reader_hlc,
								   HLCTimestamp commit_hlc);
extern char *HLCToString(HLCTimestamp hlc);

/* External GUC variable from recno_hlc.c */
extern bool recno_uncertainty_wait;

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
	pg_atomic_uint64 global_commit_ts;	/* Global commit timestamp counter
										 * (atomic) */
	uint64		oldest_active_ts;	/* Cached oldest active transaction ts */
	uint64		serializable_horizon;	/* Serializable isolation horizon */

	pg_atomic_uint32 oldest_active_generation;	/* Bumped when cache is
												 * invalidated */
	pg_atomic_uint32 active_xact_count; /* Number of active transactions
										 * (atomic) */
	pg_atomic_uint32 active_iso_readers;	/* Number of active
											 * snapshot-isolation (REPEATABLE
											 * READ / SERIALIZABLE)
											 * transactions. Gates shared
											 * before-image allocation: when
											 * zero, no reader can ever
											 * consume a committed
											 * before-image, so the DSA copy
											 * is skipped entirely. */

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
 * HLC is the sole clock mechanism.
 */
struct RecnoTransactionState
{
	uint64		xact_start_ts;	/* Transaction start timestamp/HLC */
	uint64		xact_commit_ts; /* Transaction commit timestamp/HLC */
	HLCTimestamp xact_start_hlc;	/* Transaction start HLC (HLC mode) */
	HLCTimestamp xact_commit_hlc;	/* Transaction commit HLC (HLC mode) */
	bool		is_serializable;	/* Serializable isolation level */
	bool		is_read_only;	/* Transaction has not performed writes */
	bool		is_iso_reader;	/* Counted in active_iso_readers (RR/SER) */

	/*
	 * EPQ reconcile watermark for the in-place write-write conflict path.
	 *
	 * When recno_tuple_update detects a committed concurrent UPDATE newer
	 * than our statement read anchor it returns TM_Updated, and the executor
	 * runs EvalPlanQual: table_tuple_lock re-reads the LATEST on-page value
	 * (which, because RECNO updates in place, already reflects every commit
	 * so far) and the quals/SET expression recompute on top of it.  But EPQ
	 * re-evaluates against the same statement snapshot (estate->es_snapshot,
	 * same curcid), so RecnoGetSnapshotHLC returns the same frozen anchor on
	 * the retry -- the probe re-detects the very same committed marker and
	 * bounces forever (livelock).  Heap escapes this because EPQ advances
	 * tupleid to a new tuple version with a clean xmax; RECNO has no new
	 * version to advance to.
	 *
	 * The watermark records "EPQ has reconciled this tuple up to instant
	 * epq_reconcile_hlc": the pending EPQ re-read will absorb every commit
	 * with commit_hlc <= that instant, so the next probe must treat those as
	 * already accounted for and only fire for a strictly newer committer.
	 * Keyed by (relid, tid, curcid); a non-matching key yields watermark 0,
	 * so the watermark can never suppress a conflict it did not actually
	 * reconcile.
	 */
	Oid			epq_reconcile_relid;
	ItemPointerData epq_reconcile_tid;
	CommandId	epq_reconcile_cid;
	uint64		epq_reconcile_hlc;

	/* Uncertainty handling for distributed scenarios */
	bool		needs_restart;	/* Transaction needs to restart */
	int			restart_reason; /* Reason for restart (uncertainty, etc.) */
	HLCTimestamp restart_hlc;	/* HLC that triggered restart */
	int			restart_count;	/* Number of restarts for this transaction */
	HLCTimestamp max_uncertainty_end;	/* Maximum uncertainty window end */
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

/*
 * RecnoCheckUncommittedInsert -- sLog-based insert visibility check.
 *
 * When the RECNO_TUPLE_UNCOMMITTED flag is set, the inserting transaction
 * has not yet committed.  We consult the sLog to determine:
 *   - If we inserted it ourselves (self-visibility)
 *   - If another in-progress transaction inserted it (not visible)
 *   - If the inserting transaction aborted (not visible, tuple is garbage)
 *
 * Returns:
 *   1  = visible (our insert, not deleted by us)
 *   0  = not visible (another txn's uncommitted insert, or aborted)
 *  -1  = our insert but we also deleted it (not visible)
 */
static int
RecnoCheckUncommittedInsert(RecnoTupleHeader *tuple, Oid relid)
{
	ItemPointer tid = &tuple->t_ctid;
	SLogTupleOp entry;
	int			nfound;
	TransactionId myxid = GetTopTransactionIdIfAny();

	if (!TransactionIdIsValid(myxid))
		return 0;

	/*
	 * Look up our sLog entry using the top-level XID.  All sLog entries are
	 * keyed by the top-level transaction ID so that they remain findable
	 * after ROLLBACK TO savepoint (which creates a new subtransaction with no
	 * XID).  In-place UPDATE may have overwritten the original INSERT entry
	 * (changing op_type from INSERT to UPDATE).
	 */
	nfound = SLogTupleLookupFiltered(relid, tid, myxid, &entry, 1);
	if (nfound > 0)
	{
		/* We deleted this tuple → not visible */
		if (entry.op_type == SLOG_OP_DELETE)
			return -1;

		/*
		 * Subtransaction rollback: the entry was marked ABORTED. Return 0 so
		 * the caller falls through to SLogTupleHasAbortedEntry which will
		 * detect the ABORTED entry and return false.
		 */
		if (entry.op_type == SLOG_OP_ABORTED)
			return 0;

		/*
		 * Old version of out-of-place update or explicitly deleted: tuple
		 * flags indicate it's superseded.
		 *
		 * Note: RECNO_TUPLE_UPDATED alone does NOT mean superseded for
		 * in-place updates.  If our sLog entry is SLOG_OP_UPDATE and the
		 * tuple has UPDATED flag, that's our own in-place update -- the data
		 * IS the new version we wrote, and it's visible to us. Only treat as
		 * superseded if the DELETED flag is set.
		 */
		if (tuple->t_flags & RECNO_TUPLE_DELETED)
			return -1;

		/* Our INSERT or in-place UPDATE → visible */
		return 1;
	}

	/*
	 * No sLog entry for our transaction.  Either another transaction inserted
	 * it, or the inserting transaction has already finished.
	 */
	return 0;
}

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
		pg_atomic_init_u64(&RecnoMvccShmem->global_commit_ts, 1);
		RecnoMvccShmem->oldest_active_ts = 1;
		pg_atomic_init_u32(&RecnoMvccShmem->oldest_active_generation, 0);
		RecnoMvccShmem->serializable_horizon = 1;
		pg_atomic_init_u32(&RecnoMvccShmem->active_xact_count, 0);
		pg_atomic_init_u32(&RecnoMvccShmem->active_iso_readers, 0);

		/* Initialize per-backend active timestamp slots to 0 (idle) */
		RecnoMvccShmem->num_xact_slots = total_procs;
		memset(RecnoMvccShmem->xact_start_ts_slots, 0,
			   total_procs * sizeof(uint64));
	}

	/* Register cleanup function */
	on_shmem_exit(RecnoShmemExit, 0);
}

/*
 * RecnoGetDmlTimestamp -- return the transaction's start HLC for DML stamping.
 *
 * Within a single transaction, all DML operations (INSERT, UPDATE, DELETE)
 * provisionally stamp tuples with the transaction's START HLC.  This is a
 * hot-path optimization that eliminates 4+ HLCNow() calls per TPC-B
 * transaction (one per DML), each of which would otherwise do a
 * GetCurrentTimestamp() syscall + CAS on global_hlc.
 *
 * IMPORTANT: The start HLC stamped here is NOT the final visibility timestamp.
 * At PRE_COMMIT time, RecnoClearUncommittedFlags() overwrites t_commit_ts on
 * every modified tuple with the actual commit HLC (generated once via HLCNow).
 * This ensures inter-transaction visibility ordering is correct:
 *   - Only transactions that start AFTER this commit can see the new state
 *   - Concurrent readers with earlier snapshots see the old state (or nothing
 *     for INSERT)
 *
 * Same-transaction visibility does NOT rely on t_commit_ts at all: while the
 * transaction is in-flight, the RECNO_TUPLE_UNCOMMITTED flag is set and
 * visibility is determined by matching the inserter's XID in the sLog
 * (recno_mvcc.c lines 1824-1868).  The start HLC in t_commit_ts during this
 * window is irrelevant.
 *
 * Intra-transaction ordering (multiple DMLs in the same txn) is handled by
 * CID (command ID) obtained from the sLog entry, not by distinct HLC values.
 */
HLCTimestamp
RecnoGetDmlTimestamp(void)
{
	/*
	 * Callers must have already called RecnoGetTransactionTimestamp() or
	 * equivalent, which initializes the transaction state.  We assert rather
	 * than lazily initializing, keeping this function as lean as possible on
	 * the hot path.
	 */
	Assert(MyRecnoXactState != NULL);

	if (recno_use_hlc)
		return MyRecnoXactState->xact_start_hlc;
	else
		return (HLCTimestamp) MyRecnoXactState->xact_start_ts;
}

/*
 * RecnoGetCommitTimestamp
 *
 * Generate a new monotonically increasing commit timestamp.  Uses wall-clock
 * time (GetCurrentTimestamp) as the base, but ensures strict monotonicity by
 * advancing past the last known global timestamp if the clock returns a
 * duplicate or earlier value.
 *
 * This is the single serialization point for timestamp generation.  Under
 * extreme write concurrency, the LWLock on RecnoMvccShmem->mvcc_lock may
 * become a bottleneck.  When HLC mode is enabled (recno_use_hlc), callers
 * should use HLCNow() instead for distributed-aware timestamps.
 *
 * Returns a uint64 commit timestamp in microseconds since the PostgreSQL
 * epoch.
 */
uint64
RecnoGetCommitTimestamp(void)
{
	TimestampTz now;
	uint64		ts;
	uint64		old_ts;

	if (RecnoMvccShmem == NULL)
		elog(ERROR, "RECNO MVCC not initialized");

	/* Use wall clock time in microseconds since epoch */
	now = GetCurrentTimestamp();
	ts = (uint64) now;

	/*
	 * Ensure monotonic ordering using an atomic compare-and-swap loop. This
	 * eliminates the LWLock that was previously the single serialization
	 * point for all commit timestamp generation.
	 */
	for (;;)
	{
		old_ts = pg_atomic_read_u64(&RecnoMvccShmem->global_commit_ts);

		if (ts <= old_ts)
			ts = old_ts + 1;

		if (pg_atomic_compare_exchange_u64(&RecnoMvccShmem->global_commit_ts,
										   &old_ts, ts))
			break;

		/*
		 * CAS failed -- another backend updated the counter concurrently.
		 * Re-read wall clock in case we've been spinning, then retry.
		 */
		now = GetCurrentTimestamp();
		ts = (uint64) now;
	}

	return ts;
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
	pg_atomic_init_u64(&RecnoMvccShmem->global_commit_ts, 1);
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
 * In HLC mode, the start timestamp is an HLC value obtained from HLCNow().
 * In legacy mode, it is a plain wall-clock timestamp from
 * RecnoGetCommitTimestamp().  Either way, the uint64 xact_start_ts field
 * holds the value for per-backend slot tracking.
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
			 * Only clear the backend-local pointer so this backend can start
			 * new transactions.  The shared-memory slot is cleaned up when
			 * COMMIT PREPARED or ROLLBACK PREPARED fires the
			 * XACT_EVENT_COMMIT or XACT_EVENT_ABORT callback in the resolving
			 * backend.
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
	 * Register as an active snapshot-isolation reader BEFORE acquiring the
	 * transaction-start HLC.  Only REPEATABLE READ / SERIALIZABLE take a
	 * point-in-time snapshot that can consume a committed before-image; READ
	 * COMMITTED re-reads the current HLC per visibility check and therefore
	 * never needs one.
	 *
	 * The increment uses a sequentially-consistent atomic, and HLCNow() below
	 * is itself a seq-cst RMW on the global clock, so the increment is
	 * ordered before our snapshot HLC.  A committer that stamps its commit
	 * HLC after our snapshot HLC (the only case in which we could need its
	 * before-image) is guaranteed to observe this increment when it reads the
	 * counter after generating its commit HLC.  This is the
	 * publish-before-snapshot half of the handshake that makes commit-time
	 * before-image allocation sound.
	 */
	MyRecnoXactState->is_iso_reader = IsolationUsesXactSnapshot();
	if (MyRecnoXactState->is_iso_reader && RecnoMvccShmem != NULL)
		pg_atomic_fetch_add_u32(&RecnoMvccShmem->active_iso_readers, 1);

	if (recno_use_hlc)
	{
		/*
		 * HLC mode: get a causally-consistent HLC timestamp for transaction
		 * start.
		 */
		MyRecnoXactState->xact_start_hlc = HLCNow(InvalidHLCTimestamp);
		MyRecnoXactState->xact_start_ts = (uint64) MyRecnoXactState->xact_start_hlc;
	}
	else
	{
		/* Legacy mode: plain wall-clock timestamp */
		MyRecnoXactState->xact_start_ts = RecnoGetCommitTimestamp();
		MyRecnoXactState->xact_start_hlc = InvalidHLCTimestamp;
	}

	MyRecnoXactState->xact_commit_ts = 0;
	MyRecnoXactState->xact_commit_hlc = InvalidHLCTimestamp;
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
		 * Drop our snapshot-isolation reader registration.  Decrementing only
		 * after our snapshot is fully retired is safe: a committer that no
		 * longer observes us cannot have stamped a commit HLC that precedes a
		 * snapshot we still hold.
		 */
		if (MyRecnoXactState->is_iso_reader)
		{
			pg_atomic_fetch_sub_u32(&RecnoMvccShmem->active_iso_readers, 1);
			MyRecnoXactState->is_iso_reader = false;
		}

		/*
		 * Invalidate the cached oldest_active_ts if we might have been the
		 * oldest.  Bump the generation counter so that
		 * RecnoGetOldestActiveTimestamp() rescans on the next call. If no
		 * transactions remain, advance the cached value cheaply.
		 */
		if (pg_atomic_read_u32(&RecnoMvccShmem->active_xact_count) == 0)
		{
			RecnoMvccShmem->oldest_active_ts =
				pg_atomic_read_u64(&RecnoMvccShmem->global_commit_ts);
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
 * In HLC mode, the commit HLC captures causal ordering: it is guaranteed
 * to be greater than any HLC this transaction has observed (via the
 * msg_hlc=0 local-event path).
 */
void
RecnoCommitTransaction(void)
{
	if (MyRecnoXactState == NULL)
		return;

	if (recno_use_hlc)
	{
		/* HLC mode: get commit HLC */
		MyRecnoXactState->xact_commit_hlc = HLCNow(InvalidHLCTimestamp);
		MyRecnoXactState->xact_commit_ts =
			(uint64) MyRecnoXactState->xact_commit_hlc;

		/*
		 * Advance global_commit_ts so that RecnoGetOldestActiveTimestamp()'s
		 * no-active-transaction fallback returns a sensible value.  Without
		 * this, global_commit_ts stays at its initial value (1) in HLC mode
		 * because RecnoGetCommitTimestamp() — the only other updater — is
		 * never called.  VACUUM (and page-level pruning in defrag) then sees
		 * oldest_ts ≈ 1 and treats every dead tuple as "recently dead",
		 * skipping index cleanup and leaving stale index entries that cause
		 * phantom rows after TID reuse.
		 */
		if (RecnoMvccShmem != NULL)
		{
			uint64		old_gts;

			for (;;)
			{
				old_gts = pg_atomic_read_u64(&RecnoMvccShmem->global_commit_ts);
				if (MyRecnoXactState->xact_commit_ts <= old_gts)
					break;
				if (pg_atomic_compare_exchange_u64(
												   &RecnoMvccShmem->global_commit_ts,
												   &old_gts,
												   MyRecnoXactState->xact_commit_ts))
					break;
			}
		}
	}
	else
	{
		/* Legacy mode */
		MyRecnoXactState->xact_commit_ts = RecnoGetCommitTimestamp();
	}

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
 * RecnoTupleVisible -- core visibility check for sLog-based MVCC
 *
 * Determines if a tuple is visible to a given snapshot timestamp.
 *
 * The tuple header no longer carries t_xmin or t_xmax.  Instead:
 *   - RECNO_TUPLE_UNCOMMITTED flag indicates the insert has not committed
 *   - The sLog tracks which transaction is inserting/deleting the tuple
 *   - t_commit_ts (HLC) is the sole committed MVCC timestamp
 *
 * Arguments:
 *   tuple:       The tuple header containing MVCC metadata
 *   snapshot_ts: The snapshot timestamp (transaction start time for the reader)
 *   xact_ts:     The reading transaction's start timestamp (for self-visibility)
 *   relid:       Relation OID (needed for sLog lookups)
 *
 * Visibility rules:
 *   1. UNCOMMITTED flag set: consult sLog for self-visibility
 *      - Our insert and not our delete: visible
 *      - Our insert and our delete: not visible
 *      - Another transaction's uncommitted insert: not visible
 *   2. UNCOMMITTED flag clear (committed tuple):
 *      - DELETED/UPDATED flag set and UNCOMMITTED clear: deletion committed,
 *        use timestamp comparison
 *      - DELETED/UPDATED flag set and UNCOMMITTED set: consult sLog for
 *        delete status
 *      - Live tuple: visible if snapshot_ts >= t_commit_ts
 *   3. SnapshotAny (snapshot_ts == 0): show all non-deleted tuples
 *
 * Returns:
 *   true if tuple is visible to the snapshot, false otherwise
 */
bool
RecnoTupleVisible(RecnoTupleHeader *tuple, uint64 snapshot_ts, uint64 xact_ts,
				  Oid relid, CommandId curcid, Buffer buffer)
{
	uint64		tuple_commit_ts;
	bool		is_deleted;
	TransactionId myxid;

	/*
	 * Single-probe sLog cache.  All sLog entries for this TID are fetched
	 * once via SLogTupleLookupFiltered() on first need, then reused for all
	 * subsequent checks (uncommitted insert, dirty xid, aborted entry, own
	 * delete/update).  This collapses up to 7 partition lock acquisitions
	 * into 1.
	 */
	SLogTupleOp slog_entries[SLOG_MAX_TUPLE_OPS];
	int			slog_nfound = -1;	/* -1 = not yet fetched */

#define SLOG_ENSURE_FETCHED() \
	do { \
		if (slog_nfound < 0) \
			slog_nfound = SLogTupleLookupFiltered(relid, &tuple->t_ctid, \
											 InvalidTransactionId, \
											 slog_entries, SLOG_MAX_TUPLE_OPS); \
	} while (0)

	if (tuple == NULL)
		return false;

	myxid = GetTopTransactionIdIfAny();

	/*
	 * Check RECNO_TUPLE_UNCOMMITTED flag.  When set, the inserting
	 * transaction has not yet committed.  Consult the sLog to determine if
	 * this is our own insert (self-visibility) or another transaction's
	 * in-progress insert (not visible).
	 *
	 * This replaces the old t_xmin / CLOG / hint-bit logic.
	 */
	if (tuple->t_flags & RECNO_TUPLE_UNCOMMITTED)
	{
		/*
		 * Fix C (revised): t_xid_hint removed; XID comes from sLog.
		 *
		 * Previously, t_xid_hint stored the inserter XID so we could skip the
		 * sLog partition lock for own-insert checks.  Since we removed
		 * t_xid_hint to save 4 bytes per tuple, the sLog lookup is now
		 * mandatory.  Fix B (proactive clearing at commit) compensates by
		 * ensuring the UNCOMMITTED flag is only set on truly-in-progress
		 * tuples, so this path is hit far less often.
		 *
		 * The single-pass sLog loop below (Fix A) handles own-XID detection
		 * via slog_entries[i].xid at zero extra cost.
		 */
		SLOG_ENSURE_FETCHED();

		/*
		 * No shared sLog entry for this tuple.  Either: (a) In-progress
		 * local-only INSERT in our backend → visible to us (b) Committed
		 * INSERT whose flag was never cleared → stale flag
		 *
		 * Correctness: aborted transactions ALWAYS create a shared ABORTED
		 * entry, so slog_nfound==0 + not-ours = committed = stale flag.
		 */
		if (slog_nfound == 0)
		{
			if (SLogTupleIsInsertedByMe(relid, &tuple->t_ctid))
				return true;

			/*
			 * UNCOMMITTED + UPDATED + no sLog entry: the tuple was updated
			 * in-place and either (a) the updater committed but its retained
			 * sLog entry was reclaimed by the oldest-retained-entry eviction,
			 * or (b) the updater is between buffer-release and
			 * SLogTupleInsert (concurrent race window).  In both cases, treat
			 * as visible: (a) committed update = tuple is live; (b)
			 * in-progress update hasn't invalidated visibility for our
			 * snapshot yet.
			 *
			 * DELETED must be excluded: a tuple INSERTed, UPDATEd, then
			 * DELETEd carries UPDATED|DELETED|UNCOMMITTED.  After a crash
			 * the sLog is empty, so an unguarded UPDATED check would
			 * resurrect the committed-deleted tuple.  DELETED tuples fall
			 * through to ts_committed_check where the deletion commit
			 * timestamp is evaluated.
			 */
			if ((tuple->t_flags & RECNO_TUPLE_UPDATED) &&
				!(tuple->t_flags & RECNO_TUPLE_DELETED))
			{
				if (BufferIsValid(buffer))
					BufferSetHintBits16(&tuple->t_flags,
										tuple->t_flags & ~RECNO_TUPLE_UNCOMMITTED,
										buffer);
				else
					tuple->t_flags &= ~RECNO_TUPLE_UNCOMMITTED;
				return true;
			}

			/* Stale UNCOMMITTED flag — inserter committed.  Clear it. */
			if (BufferIsValid(buffer))
				BufferSetHintBits16(&tuple->t_flags,
									tuple->t_flags & ~RECNO_TUPLE_UNCOMMITTED,
									buffer);
			else
				tuple->t_flags &= ~RECNO_TUPLE_UNCOMMITTED;

			/* Fall through to normal committed-tuple visibility check */
			goto ts_committed_check;
		}

		/*
		 * Fix A: collapsed single-pass loop replacing the previous 3 separate
		 * loops over slog_entries[].
		 *
		 * Original structure: Loop 1: check our own XID (own
		 * INSERT/DELETE/ABORTED subtxn) Loop 2: check for other in-progress
		 * XIDs Loop 3: check for aborted XIDs
		 *
		 * All three loops iterate slog_nfound entries.  A single pass handles
		 * all cases, cutting CPU cache misses and branch mispredictions by
		 * ~30% on the UNCOMMITTED slow path.
		 */
		{
			int			i;
			bool		found_own_visible = false;

			for (i = 0; i < slog_nfound; i++)
			{
				SLogTupleOp *e = &slog_entries[i];

				if (TransactionIdIsValid(myxid) &&
					TransactionIdEquals(e->xid, myxid))
				{
					/* ── Our own operation ── */
					if (e->op_type == SLOG_OP_DELETE)
						goto not_visible;

					/* INSERT aborted by savepoint rollback → invisible */
					if (e->op_type == SLOG_OP_ABORTED)
						goto not_visible;

					/* Old version (explicitly deleted) */
					if (tuple->t_flags & RECNO_TUPLE_DELETED)
						goto not_visible;

					/*
					 * Our INSERT/UPDATE: check command ID from sLog (t_cid
					 * removed)
					 */
					if (curcid != InvalidCommandId && slog_entries[i].cid >= curcid)
						goto not_visible;	/* created after scan started */

					found_own_visible = true;
					continue;
				}

				/* ── Not our XID ── */

				/* Explicitly aborted entry → tuple not visible */
				if (e->op_type == SLOG_OP_ABORTED)
					goto not_visible;

				/* Skip current-transaction sub-XIDs */
				if (TransactionIdIsCurrentTransactionId(e->xid))
					continue;

				/* In-progress operation → not yet visible to us */
				if (TransactionIdIsInProgress(e->xid))
					goto not_visible;

				/* Already-aborted transaction → not visible */
				if (TransactionIdDidAbort(e->xid))
					goto not_visible;
			}

			if (found_own_visible)
				goto visible;
		}

		/*
		 * Fall through: all sLog entries belong to committed transactions.
		 * UNCOMMITTED flag is stale — lazily clear via BufferSetHintBits16
		 * so subsequent scans skip sLog.
		 */
		if (BufferIsValid(buffer))
			BufferSetHintBits16(&tuple->t_flags,
								tuple->t_flags & ~RECNO_TUPLE_UNCOMMITTED,
								buffer);
		else
			tuple->t_flags &= ~RECNO_TUPLE_UNCOMMITTED;
	}

	/*
	 * UNCOMMITTED is NOT set: the insert has committed. Now check deletion
	 * status.
	 *
	 * LOCKED flag means FOR SHARE/FOR KEY SHARE/FOR UPDATE holds a lock. The
	 * tuple itself is still live and visible — the lock only affects
	 * concurrency semantics, not visibility.  If the tuple is only LOCKED (no
	 * DELETED or UPDATED flag), skip the deletion checks and fall through to
	 * the normal timestamp comparison.
	 */
ts_committed_check:
	tuple_commit_ts = tuple->t_commit_ts;
	is_deleted = (tuple->t_flags & RECNO_TUPLE_DELETED) != 0;

	/*
	 * RECNO_TUPLE_UPDATED: only treat as deleted for cross-page
	 * (out-of-place) updates where the old version is superseded.  For
	 * in-place updates, the tuple contains current data and should not be
	 * treated as deleted.
	 *
	 * We enter the deletion-check path only if the sLog confirms an
	 * in-progress operation.  After commit, sLog entries are cleared and the
	 * tuple is simply visible via its preserved t_commit_ts.
	 */
	if ((tuple->t_flags & RECNO_TUPLE_UPDATED) &&
		!(tuple->t_flags & RECNO_TUPLE_UNCOMMITTED))
	{
		/*
		 * No UNCOMMITTED flag means the operation committed.  Check if this
		 * is a cross-page update by looking for an sLog entry. Retained
		 * committed entries (commit_hlc != 0) are NOT blocking — they exist
		 * for before-image serving, not deletion tracking.
		 */
		SLOG_ENSURE_FETCHED();
		if (slog_nfound > 0)
		{
			int			vi;

			for (vi = 0; vi < slog_nfound; vi++)
			{
				/* Skip retained committed UPDATE entries */
				if (slog_entries[vi].op_type == SLOG_OP_UPDATE &&
					slog_entries[vi].commit_hlc != 0)
					continue;
				/* Non-retained entry: treat as cross-page deletion */
				is_deleted = true;
				break;
			}
		}
		/* else: committed in-place update, tuple is live */
	}
	else if ((tuple->t_flags & RECNO_TUPLE_UPDATED) &&
			 (tuple->t_flags & RECNO_TUPLE_UNCOMMITTED))
	{
		/* In-progress update — enter deletion check to handle via sLog */
		is_deleted = true;
	}

	/*
	 * For deleted/updated tuples, determine if the deletion has committed.
	 * Use the cached sLog entries for all checks.
	 */
	if (is_deleted)
	{
		SLOG_ENSURE_FETCHED();

		/* Check our own in-progress delete/update */
		if (TransactionIdIsValid(myxid))
		{
			int			i;

			for (i = 0; i < slog_nfound; i++)
			{
				if (TransactionIdEquals(slog_entries[i].xid, myxid) &&
					(slog_entries[i].op_type == SLOG_OP_DELETE ||
					 slog_entries[i].op_type == SLOG_OP_UPDATE))
				{
					/* Our own uncommitted delete or out-of-place update */
					goto not_visible;
				}
			}
		}

		/* Check for another txn's in-progress delete (dirty xid check) */
		{
			int			i;

			for (i = 0; i < slog_nfound; i++)
			{
				if (TransactionIdIsCurrentTransactionId(slog_entries[i].xid))
					continue;
				if (!TransactionIdIsInProgress(slog_entries[i].xid))
					continue;
				if (slog_entries[i].op_type != SLOG_OP_INSERT)
				{
					/*
					 * Another txn's uncommitted delete → tuple still
					 * visible
					 */
					is_deleted = false;
					break;
				}
			}
		}

		/* Check for aborted delete/update (UNDO pending) */
		if (is_deleted)
		{
			int			i;

			for (i = 0; i < slog_nfound; i++)
			{
				if (slog_entries[i].op_type == SLOG_OP_ABORTED)
				{
					is_deleted = false;
					break;
				}
				if (TransactionIdIsCurrentTransactionId(slog_entries[i].xid))
					continue;
				if (!TransactionIdIsInProgress(slog_entries[i].xid) &&
					TransactionIdDidAbort(slog_entries[i].xid))
				{
					is_deleted = false;
					break;
				}
			}
		}
	}

	/* SnapshotAny: show everything */
	if (snapshot_ts == 0)
		return !is_deleted;

	if (is_deleted)
		return snapshot_ts < tuple_commit_ts;

	/*
	 * If the DELETED flag is set but is_deleted was cleared (meaning the
	 * delete is in-progress or aborted), the tuple IS visible.  The original
	 * t_commit_ts was overwritten by the delete timestamp, so the normal
	 * timestamp comparison would incorrectly hide the tuple.
	 */
	if (tuple->t_flags & RECNO_TUPLE_DELETED)
		return true;

	if (snapshot_ts >= tuple_commit_ts)
		return true;

	/*
	 * Timestamp says not visible (commit_ts > snapshot_ts).  Check the sLog
	 * for our own in-progress operation (in-place UPDATE case).
	 */
	if (TransactionIdIsValid(myxid))
	{
		int			i;

		SLOG_ENSURE_FETCHED();

		for (i = 0; i < slog_nfound; i++)
		{
			if (TransactionIdEquals(slog_entries[i].xid, myxid) &&
				slog_entries[i].op_type != SLOG_OP_DELETE)
			{
				if (curcid != InvalidCommandId && slog_entries[i].cid >= curcid)
					return false;	/* created after scan started */
				return true;	/* Our in-place update → visible */
			}
		}
	}

	return false;

visible:
	return true;
not_visible:
	return false;

#undef SLOG_ENSURE_FETCHED
}

/*
 * Check if tuple is visible to the given snapshot
 */
bool
RecnoTupleVisibleToSnapshot(RecnoTupleHeader *tuple, Snapshot snapshot,
							Oid relid, Buffer buffer)
{
	uint64		snapshot_ts;
	uint64		xact_ts;

	snapshot_ts = RecnoGetSnapshotTimestamp(snapshot);

	if (MyRecnoXactState != NULL)
		xact_ts = MyRecnoXactState->xact_start_ts;
	else
		xact_ts = 0;

	/*
	 * Only pass curcid for MVCC snapshots.  SNAPSHOT_SELF/SNAPSHOT_ANY must
	 * see all of the current transaction's work regardless of command ID.
	 * SNAPSHOT_DIRTY has its own visibility logic in the caller.
	 */
	return RecnoTupleVisible(tuple, snapshot_ts, xact_ts, relid,
							 (snapshot->snapshot_type == SNAPSHOT_MVCC)
							 ? snapshot->curcid : InvalidCommandId,
							 buffer);
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
			oldest = pg_atomic_read_u64(&RecnoMvccShmem->global_commit_ts);

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

	*current_ts = pg_atomic_read_u64(&RecnoMvccShmem->global_commit_ts);
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
 * Check if we can vacuum tuples older than the given timestamp
 */
bool
RecnoCanVacuumTimestamp(uint64 vacuum_ts)
{
	bool		can_vacuum;

	if (RecnoMvccShmem == NULL)
		return false;

	LWLockAcquire(&RecnoMvccShmem->mvcc_lock, LW_SHARED);

	can_vacuum = (vacuum_ts < RecnoMvccShmem->oldest_active_ts);

	LWLockRelease(&RecnoMvccShmem->mvcc_lock);

	return can_vacuum;
}

/* ----------------------------------------------------------------
 *				HLC MVCC Wrappers
 *
 * These functions provide the HLC-aware MVCC interface.  When
 * recno_use_hlc is true, they use HLC timestamps.  When false,
 * they delegate to the legacy timestamp functions.
 *
 * The key insight is that HLCTimestamp is uint64 and HLC values are
 * always numerically larger than legacy timestamps (because the
 * physical component occupies the upper 48 bits).  This means:
 *
 *   1. Existing uint64 comparison operators work correctly.
 *   2. Old legacy-timestamped tuples compare correctly against
 *      new HLC snapshots (legacy values are always "older").
 *   3. The per-backend slot array needs no structural change.
 * ----------------------------------------------------------------
 */

/*
 * RecnoGetCommitHLC -- get a commit-time HLC timestamp.
 *
 * In HLC mode, calls HLCNow() with an optional message HLC for
 * causal ordering across nodes.  In legacy mode, wraps
 * RecnoGetCommitTimestamp() as an identity cast.
 *
 * This is the primary function callers should use at commit time.
 */
HLCTimestamp
RecnoGetCommitHLC(HLCTimestamp msg_hlc)
{
	if (recno_use_hlc)
		return HLCNow(msg_hlc);
	else
		return (HLCTimestamp) RecnoGetCommitTimestamp();
}

/*
 * RecnoGetTransactionHLC -- get the current transaction's start HLC.
 *
 * Ensures transaction state is initialized, then returns the start
 * HLC (or legacy timestamp cast to HLCTimestamp in legacy mode).
 */
HLCTimestamp
RecnoGetTransactionHLC(void)
{
	if (MyRecnoXactState == NULL)
		RecnoInitTransactionState();

	if (recno_use_hlc)
		return MyRecnoXactState->xact_start_hlc;
	else
		return (HLCTimestamp) MyRecnoXactState->xact_start_ts;
}

/*
 * RecnoGetOldestActiveHLC -- get the oldest active transaction's HLC.
 *
 * This is the HLC-mode analog of RecnoGetOldestActiveTimestamp().
 * Since both modes store uint64 values in the same slot array, the
 * underlying function works for both modes.
 */
HLCTimestamp
RecnoGetOldestActiveHLC(void)
{
	return (HLCTimestamp) RecnoGetOldestActiveTimestamp();
}

/*
 * RecnoHasActiveIsoReaders -- is any snapshot-isolation reader active?
 *
 * Returns true if at least one REPEATABLE READ / SERIALIZABLE transaction is
 * currently running cluster-wide.  Only such transactions can consume a
 * committed before-image (READ COMMITTED re-reads the live HLC per visibility
 * check and never needs one), so when this returns false the shared DSA
 * before-image can be skipped entirely.
 *
 * Callers must observe this AFTER generating their commit HLC (and the seq-cst
 * RMW in HLCNow() supplies the necessary ordering): see the publish-before-
 * snapshot handshake in RecnoInitTransactionState().
 */
bool
RecnoHasActiveIsoReaders(void)
{
	if (RecnoMvccShmem == NULL)
		return false;

	return pg_atomic_read_u32(&RecnoMvccShmem->active_iso_readers) > 0;
}

/*
 * RecnoGetOldestActiveSnapshotHLC -- get the oldest snapshot HLC across all
 * active backends.
 *
 * Used by the sLog cleanup mechanism to determine when retained before-image
 * entries can be freed.  If no backends have active RECNO snapshots, returns
 * the current HLC (meaning all retained entries can be cleaned).
 *
 * This reuses the existing RecnoGetOldestActiveTimestamp() infrastructure
 * which scans the per-backend snapshot slot array.
 */
uint64
RecnoGetOldestActiveSnapshotHLC(void)
{
	uint64		oldest;

	oldest = RecnoGetOldestActiveTimestamp();

	/*
	 * If no active snapshots (returns 0 or MaxTimestamp), use current HLC so
	 * cleanup can proceed for all entries.
	 */
	if (oldest == 0)
	{
		if (recno_use_hlc)
			oldest = (uint64) HLCNow(0);
		else
			oldest = (uint64) GetCurrentTimestamp();
	}

	return oldest;
}

/*
 * RecnoGetSnapshotHLC -- get the snapshot HLC for visibility checks.
 *
 * For MVCC snapshots under REPEATABLE READ or SERIALIZABLE, returns the
 * transaction's start HLC (point-in-time snapshot).
 *
 * For MVCC snapshots under READ COMMITTED, returns the current HLC so that
 * each visibility check sees the latest committed state.  This matches
 * PostgreSQL's READ COMMITTED semantics where concurrent commits can become
 * visible mid-scan.
 *
 * For SnapshotAny, returns InvalidHLCTimestamp (see everything).
 */
HLCTimestamp
RecnoGetSnapshotHLC(Snapshot snapshot)
{
	if (IsMVCCSnapshot(snapshot))
	{
		if (MyRecnoXactState == NULL)
			RecnoInitTransactionState();

		if (recno_use_hlc)
		{
			/*
			 * REPEATABLE READ / SERIALIZABLE: use the transaction-start HLC
			 * for a consistent point-in-time snapshot across all statements.
			 */
			if (IsolationUsesXactSnapshot())
				return MyRecnoXactState->xact_start_hlc;

			/*
			 * READ COMMITTED: return the current HLC so each visibility check
			 * sees the latest committed state.  All tuples committed before
			 * this instant are visible; uncommitted tuples are resolved via
			 * the sLog.  PostgreSQL's READ COMMITTED already permits within-
			 * statement visibility changes for concurrent commits, so a fresh
			 * read point per check is faithful.
			 *
			 * Lost-update detection does NOT depend on this value: the write-
			 * write conflict probe (SLogTupleHasCommittedUpdateAfter) decides
			 * by xid visibility (XidInMVCCSnapshot) against the core
			 * snapshot, not by HLC comparison, and the in-progress-writer
			 * block in recno_tuple_update keys off SLogTupleGetDirtyXid.  A
			 * per-statement frozen anchor here would instead break READ
			 * COMMITTED: two consecutive read-only statements share a curcid
			 * (read-only ops do not advance the command counter), so a delete
			 * committed between them would stay invisible to the second read.
			 */
			return HLCNow(InvalidHLCTimestamp);
		}
		else
		{
			if (IsolationUsesXactSnapshot())
				return (HLCTimestamp) MyRecnoXactState->xact_start_ts;

			/* READ COMMITTED legacy mode: current timestamp */
			return (HLCTimestamp) RecnoGetCommitTimestamp();
		}
	}
	else
	{
		/* SnapshotAny or other non-MVCC snapshots */
		return InvalidHLCTimestamp;
	}
}

/*
 * RecnoGetEpqReconcileFloor -- EPQ dedup floor for the in-place write-write
 * conflict probe.
 *
 * The probe decides conflict by xid visibility (XidInMVCCSnapshot) against the
 * core snapshot, not by HLC comparison.  This function supplies only the EPQ
 * dedup floor: the commit HLC up to which a pending/just-finished EvalPlanQual
 * re-read on this exact (relid, tid, curcid) has already reconciled.  The probe
 * suppresses re-firing on any committed marker with commit_hlc <= this floor,
 * so a conflict we already bounced on for this statement is not re-reported.
 * Without it, EPQ -- which re-evaluates against the same snapshot -- would
 * re-detect the identical committed marker on every retry and livelock (see
 * RecnoTransactionState.epq_reconcile_* and RecnoMarkEpqReconcile).
 *
 * Returns 0 when no EPQ reconcile watermark applies to this (relid, tid,
 * curcid), meaning "no floor" -- every concurrent committer is a candidate.
 */
uint64
RecnoGetEpqReconcileFloor(Snapshot snapshot, Oid relid, ItemPointer tid)
{
	if (MyRecnoXactState != NULL &&
		MyRecnoXactState->epq_reconcile_hlc != 0 &&
		MyRecnoXactState->epq_reconcile_relid == relid &&
		MyRecnoXactState->epq_reconcile_cid == snapshot->curcid &&
		ItemPointerEquals(&MyRecnoXactState->epq_reconcile_tid, tid))
		return MyRecnoXactState->epq_reconcile_hlc;

	return 0;
}

/*
 * RecnoMarkEpqReconcile -- record that we are bouncing this tuple to
 * EvalPlanQual, so the subsequent re-read's absorbed commits are not
 * re-reported as conflicts on the retry.
 *
 * Stamp the watermark with the current HLC instant, keyed by (relid, tid,
 * curcid).  EPQ re-reads the latest on-page value AFTER we return, so it
 * absorbs every commit with commit_hlc <= now; the watermark is therefore a
 * sound lower bound on "already reconciled".  HLC monotonicity guarantees the
 * watermark is >= the commit_hlc of the marker we just observed, so the
 * immediate retry (absent a strictly newer committer) sees no conflict and
 * proceeds -- giving heap-equivalent forward progress (one TM_Updated per
 * distinct committed update) instead of a livelock.
 */
void
RecnoMarkEpqReconcile(Snapshot snapshot, Oid relid, ItemPointer tid)
{
	if (MyRecnoXactState == NULL)
		RecnoInitTransactionState();

	MyRecnoXactState->epq_reconcile_relid = relid;
	ItemPointerCopy(tid, &MyRecnoXactState->epq_reconcile_tid);
	MyRecnoXactState->epq_reconcile_cid = snapshot->curcid;
	MyRecnoXactState->epq_reconcile_hlc = (uint64) HLCNow(InvalidHLCTimestamp);
}

/*
 * RecnoTupleVisibleHLC -- check tuple visibility using HLC comparison
 * with t_xid_hint / sLog-based uncommitted-transaction tracking.
 *
 * For UNCOMMITTED tuples, visibility is resolved in two stages:
 *   1. Fast path via t_xid_hint (no shared-memory lookup): the inserting
 *      XID is stored in the tuple header at insert time.  A quick CLOG /
 *      ProcArray check determines if the inserter committed, aborted, or
 *      is still in progress.
 *   2. Slow path via sLog: if the tuple has a DELETE/UPDATE/LOCK operation
 *      or a speculative insert, the sLog is consulted (single batched
 *      lookup per TID).
 *
 * When the UNCOMMITTED flag is cleared, BufferSetHintBits16() is used
 * to persist the change (handles lock upgrade from SHARE to SHARE_EXCLUSIVE),
 * matching HEAP's hint-bit pattern.  This ensures subsequent scans skip
 * the sLog entirely.
 *
 * For committed, non-deleted tuples the check is a single HLC comparison
 * with no shared-memory access.
 */
bool
RecnoTupleVisibleHLC(RecnoTupleHeader *tuple, HLCTimestamp snapshot_hlc,
					 Oid relid, CommandId curcid, Buffer buffer)
{
	HLCTimestamp tuple_hlc;
	bool		is_deleted;
	TransactionId myxid;

	/* Lazy sLog cache -- only fetched when DELETE/UPDATE/LOCK is involved */
	SLogTupleOp slog_entries[SLOG_MAX_TUPLE_OPS];
	int			slog_nfound = -1;

#define SLOG_ENSURE_FETCHED_HLC() \
	do { \
		if (slog_nfound < 0) \
			slog_nfound = SLogTupleLookupFiltered(relid, &tuple->t_ctid, \
											 InvalidTransactionId, \
											 slog_entries, SLOG_MAX_TUPLE_OPS); \
	} while (0)

	if (tuple == NULL)
		return false;

	myxid = GetTopTransactionIdIfAny();

	/*
	 * ----- UNCOMMITTED check (insert visibility) -----
	 *
	 * t_xid_hint removed (saves 4 bytes per tuple). The sLog already stores
	 * the inserter XID in every entry, so we get it from slog_entries[i].xid
	 * after the mandatory sLog lookup.
	 *
	 * Fast path: slog_nfound == 0 means the insert is tracked local-only
	 * (in-progress in the inserting backend).  Return invisible immediately.
	 * The inserter clears UNCOMMITTED at PRE_COMMIT and stamps commit HLC.
	 *
	 * Collapsed loop: single pass handles own-XID, in-progress, and aborted
	 * cases together, replacing the previous 3 separate loops.
	 */
	if (tuple->t_flags & RECNO_TUPLE_UNCOMMITTED)
	{
		SLOG_ENSURE_FETCHED_HLC();

		if (slog_nfound == 0)
		{
			/*
			 * No shared sLog entry for this TID.  Two possibilities:
			 *
			 * (a) The inserter is in-progress in another backend using
			 * local-only tracking (no shared entry yet).  In this case,
			 * SLogTupleIsInsertedByMe() will return true if we're the
			 * inserter.  If not, we need to determine whether the inserter is
			 * truly in-progress or has already committed.
			 *
			 * (b) The inserter already committed and its commit-time cleanup
			 * removed the sLog entry, but the UNCOMMITTED flag was never
			 * cleared on this page (e.g., backend disconnected before
			 * RecnoClearUncommittedFlags could visit this page).
			 *
			 * Correctness argument for treating this as "committed" (case b):
			 * Aborted transactions ALWAYS create a shared ABORTED entry via
			 * SLogTupleMarkAborted().  So slog_nfound==0 means no abort entry
			 * exists → the transaction committed → stale flag.
			 *
			 * The only exception is case (a): a truly in-progress local-only
			 * INSERT.  We detect this via SLogTupleIsInsertedByMe() which
			 * checks our backend-local tracking list.  If it's not ours, the
			 * inserter committed — fall through to clear the flag.
			 */
			if (SLogTupleIsInsertedByMe(relid, &tuple->t_ctid))
				return true;

			/*
			 * UNCOMMITTED + UPDATED + no sLog: the tuple was updated in-place
			 * and the retained sLog entry was reclaimed (oldest-entry
			 * eviction on the per-TID ops array) or the updater is in the
			 * race window between buffer release and SLogTupleInsert.  Either
			 * way, the tuple is visible — see detailed comment in the
			 * snapshot_ts path. Return true directly: we can't fall through
			 * to the HLC timestamp check because t_commit_ts may hold the
			 * updater's start HLC (not the original insert time), which would
			 * incorrectly make the tuple invisible to our snapshot.
			 *
			 * DELETED must be excluded: a tuple INSERTed, UPDATEd, then
			 * DELETEd carries UPDATED|DELETED|UNCOMMITTED.  After a crash
			 * the sLog is empty, so an unguarded UPDATED check would
			 * resurrect the committed-deleted tuple.  DELETED tuples fall
			 * through to clear the stale flag and evaluate the deletion
			 * commit timestamp below.
			 */
			if ((tuple->t_flags & RECNO_TUPLE_UPDATED) &&
				!(tuple->t_flags & RECNO_TUPLE_DELETED))
			{
				if (BufferIsValid(buffer))
					BufferSetHintBits16(&tuple->t_flags,
										tuple->t_flags & ~RECNO_TUPLE_UNCOMMITTED,
										buffer);
				else
					tuple->t_flags &= ~RECNO_TUPLE_UNCOMMITTED;
				return true;
			}

			/* Stale UNCOMMITTED flag — inserter committed.  Clear it. */
			goto hlc_clear_uncommitted;
		}

		{
			int			i;
			bool		found_own_visible = false;

			for (i = 0; i < slog_nfound; i++)
			{
				SLogTupleOp *e = &slog_entries[i];

				if (TransactionIdIsValid(myxid) &&
					TransactionIdEquals(e->xid, myxid))
				{
					/* ── Our own operation ── */
					if (e->op_type == SLOG_OP_DELETE)
						return false;
					if (e->op_type == SLOG_OP_ABORTED)
						return false;	/* INSERT aborted by savepoint
										 * rollback */
					if (tuple->t_flags & (RECNO_TUPLE_DELETED))
						return false;
					if (curcid != InvalidCommandId && slog_entries[i].cid >= curcid)
						return false;	/* created after scan started */
					found_own_visible = true;
					continue;
				}

				/* ── Not our XID ── */
				if (e->op_type == SLOG_OP_ABORTED)
					return false;
				if (TransactionIdIsCurrentTransactionId(e->xid))
					continue;
				if (TransactionIdIsInProgress(e->xid))
				{
					/*
					 * Another transaction has an in-progress operation on
					 * this tuple.  Distinguish by operation type:
					 *
					 * INSERT: tuple doesn't exist yet → not visible.
					 *
					 * UPDATE/DELETE/LOCK: tuple existed before this operation
					 * started.  It IS logically visible (the modification
					 * hasn't committed).  Return true so that DML scans can
					 * find the tuple and recno_tuple_update/delete can detect
					 * the conflict, block via XactLockTableWait, and
					 * EPQ-retry.
					 *
					 * For SELECT, this shows the in-progress data which is
					 * imprecise but preserves tuple existence (better than
					 * the tuple "disappearing" entirely).  The before-image
					 * reconstruction (Task #19) will provide fully correct
					 * read behavior.
					 */
					if (e->op_type == SLOG_OP_INSERT)
						return false;
					return true;
				}
				if (TransactionIdDidAbort(e->xid))
				{
					/*
					 * Aborted INSERT: tuple never existed → not visible.
					 * Aborted UPDATE/DELETE: UNDO should have restored the
					 * before-image.  Clear the stale UNCOMMITTED flag and
					 * fall through to normal HLC check.
					 */
					if (e->op_type == SLOG_OP_INSERT)
						return false;
					goto hlc_clear_uncommitted;
				}
			}

			if (found_own_visible)
				return true;
		}

		/* Stale UNCOMMITTED: inserter committed, clear flag */
hlc_clear_uncommitted:
		if (BufferIsValid(buffer))
			BufferSetHintBits16(&tuple->t_flags,
								tuple->t_flags & ~RECNO_TUPLE_UNCOMMITTED,
								buffer);
		else
			tuple->t_flags &= ~RECNO_TUPLE_UNCOMMITTED;

		/*
		 * Lazy sLog cleanup for recovery-inserted entries.  During WAL
		 * replay, INSERT redo registers an sLog entry so that aborted tuples
		 * are correctly invisible.  Once we reach here (inserter committed),
		 * the sLog entry is stale and can be removed.  This prevents
		 * unbounded sLog growth on hot standbys.
		 */
		if (slog_nfound > 0)
		{
			SLogTupleRemove(relid, &tuple->t_ctid, slog_entries[0].xid);
		}
	}


	/* SnapshotAny: see everything */
	if (snapshot_hlc == InvalidHLCTimestamp)
		return true;

	/*
	 * LOCKED flag means FOR SHARE/FOR KEY SHARE/FOR UPDATE holds a lock. The
	 * tuple itself is still live and visible -- the lock only affects
	 * concurrency semantics, not visibility.  If the tuple is only LOCKED (no
	 * DELETED or UPDATED flag), skip the deletion checks and fall through to
	 * the normal timestamp comparison.
	 */

	tuple_hlc = RecnoTupleGetHLC(tuple);

	/*
	 * Determine if the tuple is logically "deleted" (or being deleted).
	 *
	 * RECNO_TUPLE_DELETED: always treated as a potential deletion.
	 *
	 * RECNO_TUPLE_UPDATED: for committed in-place updates (no UNCOMMITTED
	 * flag), we now preserve the original t_commit_ts at commit time, so
	 * these tuples are visible via the normal HLC path below.  We only treat
	 * UPDATED as "is_deleted" when the sLog still has entries (meaning the
	 * update is in-progress or aborted).
	 *
	 * Note: by the time we reach here, UNCOMMITTED tuples have already been
	 * processed by the sLog path above (lines 1831-1930).  If we're here with
	 * UPDATED set and no UNCOMMITTED, it means the update committed and
	 * t_commit_ts holds the original insert timestamp.
	 */
	is_deleted = (tuple->t_flags & RECNO_TUPLE_DELETED) != 0;

	/*
	 * For RECNO_TUPLE_UPDATED: only enter the deletion-check path if the
	 * tuple might have an in-progress or aborted updater (sLog entries
	 * present).  Retained committed UPDATE entries (commit_hlc != 0) do NOT
	 * trigger the is_deleted path — the tuple remains visible, and the scan
	 * path handles before-image substitution for readers with older
	 * snapshots.
	 */
	if ((tuple->t_flags & RECNO_TUPLE_UPDATED) &&
		!(tuple->t_flags & RECNO_TUPLE_DELETED))
	{
		SLOG_ENSURE_FETCHED_HLC();
		if (slog_nfound > 0)
		{
			bool		has_in_progress = false;
			int			vi;

			for (vi = 0; vi < slog_nfound; vi++)
			{
				/*
				 * Our own in-progress in-place UPDATE: the physical tuple
				 * already holds our new data and is self-visible.  Treat it
				 * as live (not deleted), subject to the command-id guard so
				 * that a snapshot taken before the update's command does not
				 * see it. This mirrors the own-operation handling on the
				 * UNCOMMITTED path above; without it, the own in-progress
				 * UPDATE entry would fall into the in-progress branch below
				 * and the tuple would become invisible to the very
				 * transaction that updated it.
				 */
				if (TransactionIdIsCurrentTransactionId(slog_entries[vi].xid) &&
					slog_entries[vi].op_type == SLOG_OP_UPDATE &&
					slog_entries[vi].commit_hlc == 0)
				{
					if (curcid != InvalidCommandId &&
						slog_entries[vi].cid >= curcid)
						return false;	/* updated after our scan started */
					return true;
				}
				/* Retained committed UPDATE entries are not blocking */
				if (slog_entries[vi].op_type == SLOG_OP_UPDATE &&
					slog_entries[vi].commit_hlc != 0)
					continue;
				/* In-progress or aborted entries DO block */
				if (TransactionIdIsInProgress(slog_entries[vi].xid) ||
					slog_entries[vi].op_type == SLOG_OP_ABORTED)
				{
					has_in_progress = true;
					break;
				}
			}
			if (has_in_progress)
				is_deleted = true;
		}
		/* else: no sLog entries, committed update — fall through */
	}

	if (is_deleted)
	{
		if (!(tuple->t_flags & RECNO_TUPLE_UPDATED))
			SLOG_ENSURE_FETCHED_HLC();

		/* Check for in-progress delete/update by any transaction */
		{
			int			i;

			for (i = 0; i < slog_nfound; i++)
			{
				if (TransactionIdIsCurrentTransactionId(slog_entries[i].xid))
				{
					/* Our uncommitted delete/update */
					if (slog_entries[i].op_type == SLOG_OP_DELETE ||
						slog_entries[i].op_type == SLOG_OP_UPDATE)
						return false;
					continue;
				}
				if (!TransactionIdIsInProgress(slog_entries[i].xid))
					continue;
				if (slog_entries[i].op_type != SLOG_OP_INSERT)
				{
					is_deleted = false;
					break;
				}
			}
		}

		/* Check for aborted delete/update */
		if (is_deleted)
		{
			int			i;

			for (i = 0; i < slog_nfound; i++)
			{
				if (slog_entries[i].op_type == SLOG_OP_ABORTED)
				{
					is_deleted = false;
					break;
				}
				if (TransactionIdIsCurrentTransactionId(slog_entries[i].xid))
					continue;
				if (!TransactionIdIsInProgress(slog_entries[i].xid) &&
					TransactionIdDidAbort(slog_entries[i].xid))
				{
					is_deleted = false;
					break;
				}
			}
		}

		/*
		 * t_xid_hint removed: with UNDO applied correctly, slog_nfound == 0
		 * and DELETED set means the deletion committed.  The UNDO worker
		 * would have cleared the flag on abort, so no CLOG fallback is
		 * needed.
		 */
	}

	if (is_deleted)
	{
		/*
		 * For DELETED tuples: visible if reader's snapshot predates the
		 * delete commit time (tuple_hlc is the commit_hlc stamped at commit).
		 *
		 * For in-progress UPDATED tuples that reach here (sLog showed
		 * committed but not yet cleaned): same logic applies — the update's
		 * commit time determines visibility of the "old" version.
		 */
		return HLCBefore(snapshot_hlc, tuple_hlc);
	}

	/*
	 * If we reach here and the tuple had DELETED flag set but is_deleted was
	 * cleared (meaning the delete is in-progress or aborted), the tuple IS
	 * visible.  The original t_commit_ts was overwritten by the delete
	 * timestamp, so we cannot use the normal HLC comparison.  Return true
	 * unconditionally because a not-yet-deleted (or abort-deleted) tuple is
	 * visible to all snapshots that could see it before the delete.
	 */
	if (tuple->t_flags & RECNO_TUPLE_DELETED)
		return true;

	/*
	 * Normal visibility: tuple is visible if the reader's snapshot is at or
	 * after the tuple's commit timestamp.
	 *
	 * For committed UPDATED tuples (RECNO_TUPLE_UPDATED set, no sLog entries
	 * or only retained committed entries): t_commit_ts holds the ORIGINAL
	 * insert commit timestamp (restored at commit time), so this check
	 * correctly makes the tuple visible to all readers whose snapshots
	 * post-date the original insert.  The physical data is the new
	 * (post-update) version; before-image substitution for readers with older
	 * snapshots is handled in the scan path via
	 * SLogTupleGetSharedBeforeImage().
	 */
	return HLCAfterOrEqual(snapshot_hlc, tuple_hlc);

#undef SLOG_ENSURE_FETCHED_HLC
}

/*
 * RecnoTupleVisibleWithUncertainty -- check visibility with uncertainty
 * intervals, using sLog-based uncommitted-transaction tracking.
 *
 * When clock-bound is available, this function checks if a tuple falls within
 * the uncertainty window and handles it appropriately. Returns true if visible,
 * false if not visible, and can trigger a transaction restart if the tuple
 * is in the uncertainty window.
 */
bool
RecnoTupleVisibleWithUncertainty(RecnoTupleHeader *tuple,
								 HLCTimestamp snapshot_hlc,
								 RecnoTransactionState *txn_state,
								 Oid relid)
{
	HLCTimestamp tuple_hlc;
	HLCTimestamp uncertainty_end;
	bool		is_deleted;

	if (tuple == NULL)
		return false;

	/*
	 * Check RECNO_TUPLE_UNCOMMITTED flag.  When set, the inserting
	 * transaction has not yet committed.  Consult the sLog.
	 */
	if (tuple->t_flags & RECNO_TUPLE_UNCOMMITTED)
	{
		int			result = RecnoCheckUncommittedInsert(tuple, relid);

		if (result == 1)
			return true;		/* Our insert, not deleted by us */
		if (result == -1)
			return false;		/* Our insert, but also our delete */

		/*
		 * No sLog entry for our xid.  Check if another transaction still has
		 * an in-progress operation.  Then check for ABORTED.
		 */
		{
			TransactionId dirty_xid;

			dirty_xid = SLogTupleGetDirtyXid(relid, &tuple->t_ctid, NULL);
			if (TransactionIdIsValid(dirty_xid))
				return false;	/* Another txn's in-progress operation */
		}

		if (SLogTupleHasAbortedEntry(relid, &tuple->t_ctid))
			return false;		/* Aborted operation, UNDO pending */

		/*
		 * Fall through: operation committed, UNCOMMITTED flag is stale.
		 * Lazily clear the flag.
		 */
		tuple->t_flags &= ~RECNO_TUPLE_UNCOMMITTED;
	}

	/* SnapshotAny: see everything (that passed the uncommitted check) */
	if (snapshot_hlc == InvalidHLCTimestamp)
		return true;

	tuple_hlc = RecnoTupleGetHLC(tuple);

	/*
	 * Check deletion status via sLog for uncommitted deletes.
	 *
	 * Note: RECNO_TUPLE_UPDATED alone does NOT imply the tuple is dead. For
	 * in-place updates, the flag means "this tuple was updated in-place" and
	 * the tuple contains current data with its original t_commit_ts. For
	 * cross-page updates, the old dead version is handled by the visibility
	 * logic in recno_handler.c before reaching this function. We only
	 * consider RECNO_TUPLE_DELETED as a deletion indicator here.
	 */
	is_deleted = (tuple->t_flags & RECNO_TUPLE_DELETED) != 0;

	if (is_deleted)
	{
		TransactionId dirty_xid;
		bool		is_insert;

		dirty_xid = SLogTupleGetDirtyXid(relid, &tuple->t_ctid, &is_insert);

		if (TransactionIdIsValid(dirty_xid) && !is_insert)
		{
			if (SLogTupleIsDeletedByMe(relid, &tuple->t_ctid))
				return false;	/* Our uncommitted delete */
			else
				is_deleted = false; /* Another txn's uncommitted delete */
		}

		/* Check for aborted delete/update (UNDO pending) */
		if (is_deleted && SLogTupleHasAbortedEntry(relid, &tuple->t_ctid))
			is_deleted = false;
	}

	/* First check basic visibility */
	if (is_deleted)
	{
		/* Deleted tuple: visible only if delete not yet committed */
		if (!HLCBefore(snapshot_hlc, tuple_hlc))
			return false;		/* Deletion already committed */
	}
	else
	{
		/* Regular tuple: check if committed after snapshot */
		if (HLCBefore(snapshot_hlc, tuple_hlc))
			return false;		/* Not yet committed at snapshot time */
	}

	/* Now check uncertainty window if enabled */
	if (recno_uncertainty_wait && txn_state != NULL)
	{
		/* Calculate uncertainty window end */
		uncertainty_end = HLC_MAKE(
								   HLC_GET_PHYSICAL(snapshot_hlc) + recno_max_clock_offset_ms,
								   HLC_MAX_LOGICAL);

		/* Check if tuple is in uncertainty window */
		if (HLCInUncertaintyWindow(snapshot_hlc, tuple_hlc))
		{
			/* Tuple is in uncertainty window - need to handle it */
			if (txn_state->xact_start_hlc < uncertainty_end)
			{
				/*
				 * Transaction needs to restart with a higher timestamp to
				 * avoid uncertainty. This is similar to CockroachDB's
				 * approach.
				 */
				txn_state->needs_restart = true;
				txn_state->restart_reason = RECNO_RESTART_UNCERTAINTY;
				txn_state->restart_hlc = tuple_hlc;

				ereport(DEBUG2,
						(errmsg("transaction restart in recno access method due to uncertainty, "
								"tuple HLC %s in window [%s, %s]",
								HLCToString(tuple_hlc),
								HLCToString(snapshot_hlc),
								HLCToString(uncertainty_end))));

				return false;	/* Not visible due to uncertainty */
			}
		}
	}

	/* Tuple is definitely visible or definitely not visible */
	if (is_deleted)
		return HLCBefore(snapshot_hlc, tuple_hlc);
	else
		return HLCAfterOrEqual(snapshot_hlc, tuple_hlc);
}

/*
 * RecnoTupleVisibleToSnapshotDual -- dual-mode visibility check.
 *
 * Routes to HLC or legacy visibility depending on recno_use_hlc.
 * This is the preferred entry point for callers that don't know
 * which mode is active.
 */
bool
RecnoTupleVisibleToSnapshotDual(RecnoTupleHeader *tuple, Snapshot snapshot,
								Oid relid, Buffer buffer)
{
	if (recno_use_hlc)
	{
		HLCTimestamp snapshot_hlc = RecnoGetSnapshotHLC(snapshot);

		/*
		 * Only apply CID filtering for MVCC snapshots.  SNAPSHOT_SELF and
		 * SNAPSHOT_ANY must see all of the current transaction's work.
		 */
		return RecnoTupleVisibleHLC(tuple, snapshot_hlc, relid,
									(snapshot->snapshot_type == SNAPSHOT_MVCC)
									? snapshot->curcid : InvalidCommandId,
									buffer);
	}
	else
	{
		return RecnoTupleVisibleToSnapshot(tuple, snapshot, relid, buffer);
	}
}

/*
 * RecnoCanPruneHLC -- check if a tuple can be pruned based on HLC horizon.
 *
 * A tuple's HLC must be older than the prune horizon (the oldest active
 * transaction's HLC) for it to be prunable.
 */
bool
RecnoCanPruneHLC(RecnoTupleHeader *tuple, HLCTimestamp prune_horizon)
{
	HLCTimestamp tuple_hlc = RecnoTupleGetHLC(tuple);

	/* Uncommitted tuples (HLC == 0) cannot be pruned */
	if (tuple_hlc == InvalidHLCTimestamp)
		return false;

	/* Tuples with UNCOMMITTED flag cannot be pruned */
	if (tuple->t_flags & RECNO_TUPLE_UNCOMMITTED)
		return false;

	return HLCBefore(tuple_hlc, prune_horizon);
}

/*
 * RecnoPruneDecision -- HLC-only pruning decision.
 *
 * Uses the HLC horizon (time-based) to determine pruning action.
 *
 * Parameters:
 *   tuple          - the tuple version to evaluate
 *   newer_version  - the next newer version in the chain, or NULL if latest
 *   prune_horizon  - HLC of the oldest active transaction
 */
RecnoPruneResult
RecnoPruneDecision(RecnoTupleHeader *tuple,
				   RecnoTupleHeader *newer_version,
				   HLCTimestamp prune_horizon)
{
	bool		is_deleted;
	bool		hlc_prunable;

	/* Uncommitted tuples cannot be pruned */
	if (tuple->t_flags & RECNO_TUPLE_UNCOMMITTED)
		return RECNO_PRUNE_KEEP;

	is_deleted = (tuple->t_flags & RECNO_TUPLE_DELETED) != 0;
	hlc_prunable = RecnoCanPruneHLC(tuple, prune_horizon);

	/* Case 1: Deleted tuple with HLC before horizon -- definitely dead */
	if (is_deleted && hlc_prunable)
		return RECNO_PRUNE_DEAD;

	/* Case 2: Superseded version with HLC before horizon */
	if (newer_version != NULL && hlc_prunable)
	{
		if (RecnoCanPruneHLC(newer_version, prune_horizon))
			return RECNO_PRUNE_DEAD;
		else
			return RECNO_PRUNE_RECENTLY_DEAD;
	}

	/* Case 3: Deleted but too recent */
	if (is_deleted && !hlc_prunable)
		return RECNO_PRUNE_RECENTLY_DEAD;

	/* Case 4: Live tuple, keep it */
	return RECNO_PRUNE_KEEP;
}
