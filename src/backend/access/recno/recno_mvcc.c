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
 *	  DVV (Dotted Version Vectors) have been removed.  HLC is the sole
 *	  clock mechanism.  MultiXact support has been removed; concurrent
 *	  tuple locking is tracked via the sLog.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/atm.h"
#include "access/recno.h"
#include "access/recno_slog.h"
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
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/timestamp.h"

/* External functions from recno_hlc.c */
extern HLCTimestamp HLCGetGlobal(void);
extern void HLCGetUncertaintyInterval(HLCTimestamp hlc,
									  HLCTimestamp * lower,
									  HLCTimestamp * upper);
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
	pg_atomic_uint64 global_commit_ts;	/* Global commit timestamp counter (atomic) */
	uint64		oldest_active_ts;	/* Cached oldest active transaction ts */
	uint64		serializable_horizon;	/* Serializable isolation horizon */

	/* Anti-dependency tracking for serializable isolation */
	bool		anti_deps_enabled;	/* Anti-dependency tracking enabled */
	pg_atomic_uint32 oldest_active_generation;	/* Bumped when cache is invalidated */
	int			max_transactions;	/* Maximum concurrent transactions */
	pg_atomic_uint32 active_xact_count;	/* Number of active transactions (atomic) */

	/*
	 * Per-backend active transaction start timestamps.  Each backend slot
	 * stores the start timestamp of its current RECNO transaction, or 0 if
	 * idle.  This array is indexed by pgprocno (the offset into
	 * ProcGlobal->allProcs) and is sized to RECNO_TOTAL_PROCS so that
	 * auxiliary procs and prepared transactions are covered.
	 *
	 * Each slot is written only by its owning backend and read by VACUUM,
	 * so no lock is needed — just a compiler barrier via volatile access.
	 */
	int			num_xact_slots; /* Number of slots (== RECNO_TOTAL_PROCS) */
	uint64		xact_start_ts_slots[FLEXIBLE_ARRAY_MEMBER];

}			RecnoMvccShmemData;

static RecnoMvccShmemData * RecnoMvccShmem = NULL;

/*
 * RW-conflict edge for SSI cycle detection.
 *
 * Each edge records a rw-antidependency: the reader read a version that
 * was overwritten by the writer.  The edge direction is reader -> writer
 * ("reader has an outgoing conflict to writer", or equivalently "writer
 * has an incoming conflict from reader").
 *
 * We track these edges per-transaction using local Lists (not shared
 * memory), because RECNO's timestamp-based MVCC can detect conflicts
 * locally by comparing commit timestamps.  This avoids the complexity
 * of a shared-memory conflict pool while still implementing the SSI
 * "dangerous structure" heuristic.
 */
typedef struct RecnoRWConflict
{
	uint64		other_xact_ts;	/* Start timestamp of the other transaction */
	uint64		other_commit_ts;	/* Commit timestamp (0 if still running) */
	bool		other_committed;	/* Has the other transaction committed? */
	bool		other_is_read_only; /* Is the other transaction read-only? */
}			RecnoRWConflict;

/*
 * Per-transaction MVCC state with SSI conflict tracking.
 *
 * The rw_conflicts_in list records transactions that have a rw-conflict
 * pointing TO this transaction (i.e., they read something we overwrote).
 * The rw_conflicts_out list records transactions that have a rw-conflict
 * pointing FROM this transaction (i.e., we read something they overwrote).
 *
 * A "dangerous structure" exists when:
 *   T_in --rw--> T_pivot --rw--> T_out
 * and T_out committed before T_pivot (or T_pivot committed before T_in
 * in the reverse direction).  T_pivot has both an incoming AND outgoing
 * rw-antidependency edge from/to committed or prepared transactions.
 *
 * This is the "two consecutive rw-antidependency edges" heuristic from
 * the SSI paper (Cahill et al., "Serializable Isolation for Snapshot
 * Databases") and the PostgreSQL implementation paper (Ports & Grittner,
 * "Serializable Snapshot Isolation in PostgreSQL").
 *
 * DVV fields (xact_start_dvv, xact_commit_dvv) have been removed.
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

	/* SSI rw-antidependency tracking */
	List	   *rw_conflicts_in;	/* List of RecnoRWConflict: T --rw--> me */
	List	   *rw_conflicts_out;	/* List of RecnoRWConflict: me --rw--> T */
	bool		has_conflict_in;	/* Quick flag: has any incoming conflict */
	bool		has_conflict_out;	/* Quick flag: has any outgoing conflict */
	bool		doomed;			/* Marked for abort by another transaction */

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
 *
 * The rw_conflicts_in/out Lists are allocated in TopTransactionContext and
 * freed when that context is destroyed.  We reset the pointers to NIL in
 * RecnoCleanupTransactionState().
 */
static RecnoTransactionState MyRecnoXactStateData;
static RecnoTransactionState *MyRecnoXactState = NULL;

/* GUC variables */
bool		recno_enable_serializable = true;
int			recno_max_transactions = 1000;

/*
 * Function prototypes
 */
static void RecnoInitTransactionState(void);
static void RecnoCleanupTransactionState(void);
static bool RecnoCheckForDangerousStructure(void);
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
	RecnoSLogEntry entry;
	int			nfound;
	TransactionId myxid = GetTopTransactionIdIfAny();

	if (!TransactionIdIsValid(myxid))
		return 0;

	/*
	 * Look up our sLog entry using the top-level XID.  All sLog entries are
	 * keyed by the top-level transaction ID so that they remain findable
	 * after ROLLBACK TO savepoint (which creates a new subtransaction with
	 * no XID).  In-place UPDATE may have overwritten the original INSERT
	 * entry (changing op_type from INSERT to UPDATE).
	 */
	nfound = RecnoSLogLookup(relid, tid, myxid, &entry, 1);
	if (nfound > 0)
	{
		/* We deleted this tuple → not visible */
		if (entry.op_type == RECNO_SLOG_DELETE)
			return -1;

		/*
		 * Subtransaction rollback: the entry was marked ABORTED.
		 * Return 0 so the caller falls through to RecnoSLogHasAbortedEntry
		 * which will detect the ABORTED entry and return false.
		 */
		if (entry.op_type == RECNO_SLOG_ABORTED)
			return 0;

		/*
		 * Old version of out-of-place update or explicitly deleted:
		 * tuple flags indicate it's superseded.
		 */
		if (tuple->t_flags & (RECNO_TUPLE_DELETED | RECNO_TUPLE_UPDATED))
			return -1;

		/* Our INSERT or in-place UPDATE → visible */
		return 1;
	}

	/*
	 * No sLog entry for our transaction.  Either another transaction
	 * inserted it, or the inserting transaction has already finished.
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
		RecnoMvccShmem->anti_deps_enabled = recno_enable_serializable;
		RecnoMvccShmem->max_transactions = recno_max_transactions;
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
 * RecnoGetDmlTimestamp -- return the transaction's start HLC for DML operations.
 *
 * Within a single transaction, all DML operations (INSERT, UPDATE, DELETE)
 * use the same timestamp: the transaction's start HLC.  Intra-transaction
 * ordering is handled by CID, not by distinct HLC values per operation.
 * The final commit HLC (assigned at commit time) determines inter-transaction
 * visibility ordering.
 *
 * This eliminates 4 HLCNow() calls per TPC-B transaction (one per DML),
 * each of which would otherwise do a GetCurrentTimestamp() syscall + CAS
 * on global_hlc.
 *
 * Correctness: RecnoTupleVisibleHLC compares tuple_hlc <= snapshot_hlc for
 * committed tuples.  All tuples in tx T1 share T1's start HLC.  When T2
 * starts after T1 commits, T2's snapshot_hlc > T1's start HLC (because
 * T1's commit HLC >= T1's start HLC, and T2's start HLC > T1's commit HLC),
 * so T1's tuples are correctly visible to T2.
 */
HLCTimestamp
RecnoGetDmlTimestamp(void)
{
	/*
	 * Callers must have already called RecnoGetTransactionTimestamp() or
	 * equivalent, which initializes the transaction state.  We assert
	 * rather than lazily initializing, keeping this function as lean as
	 * possible on the hot path.
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
	 * Ensure monotonic ordering using an atomic compare-and-swap loop.
	 * This eliminates the LWLock that was previously the single
	 * serialization point for all commit timestamp generation.
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
	RecnoMvccShmem->anti_deps_enabled = recno_enable_serializable;
	RecnoMvccShmem->max_transactions = recno_max_transactions;
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
 *
 * DVV has been removed; HLC is the sole clock mechanism.
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
			RecnoCleanupTransactionState();
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

	if (recno_use_hlc)
	{
		/*
		 * HLC mode: get a causally-consistent HLC timestamp for transaction
		 * start.  DVV has been removed; HLC is the sole clock.
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

	/* SSI conflict tracking */
	MyRecnoXactState->rw_conflicts_in = NIL;
	MyRecnoXactState->rw_conflicts_out = NIL;
	MyRecnoXactState->has_conflict_in = false;
	MyRecnoXactState->has_conflict_out = false;
	MyRecnoXactState->doomed = false;

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
	 * writes its own slot, and the generation counter invalidates the
	 * cached oldest_active_ts when needed.
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
		 * Invalidate the cached oldest_active_ts if we might have been
		 * the oldest.  Bump the generation counter so that
		 * RecnoGetOldestActiveTimestamp() rescans on the next call.
		 * If no transactions remain, advance the cached value cheaply.
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
			 * Only invalidate the cache when we were the actual oldest
			 * active transaction.  If my_ts < oldest_active_ts, the
			 * cached value was already advanced past us by another
			 * backend's rescan, so our departure cannot change the
			 * oldest.  Using strict equality instead of <= dramatically
			 * reduces invalidation frequency under high concurrency.
			 */
			pg_atomic_fetch_add_u32(&RecnoMvccShmem->oldest_active_generation, 1);
		}
	}

	/* Free SSI conflict lists */
	if (MyRecnoXactState->rw_conflicts_in)
		list_free_deep(MyRecnoXactState->rw_conflicts_in);
	if (MyRecnoXactState->rw_conflicts_out)
		list_free_deep(MyRecnoXactState->rw_conflicts_out);

	MyRecnoXactState = NULL;
}

/*
 * RecnoCheckSerializableConflict -- check if the current transaction should
 * be aborted due to serializable isolation conflicts.
 *
 * This is called during tuple operations (read, write, delete) to check
 * whether the current transaction has been doomed by another transaction's
 * conflict detection.
 *
 * Returns true if the caller should proceed (no conflict), false is never
 * returned because we ereport on failure.
 */
bool
RecnoCheckSerializableConflict(Relation rel, ItemPointer tid)
{
	if (MyRecnoXactState == NULL)
		RecnoInitTransactionState();

	if (!MyRecnoXactState->is_serializable || !RecnoMvccShmem->anti_deps_enabled)
		return false;

	/* Check if another transaction has doomed us */
	if (MyRecnoXactState->doomed)
	{
		ereport(ERROR,
				(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
				 errmsg("could not serialize access due to read/write dependencies among transactions"),
				 errdetail_internal("Reason code: Canceled on identification as a pivot, during conflict check."),
				 errhint("The transaction might succeed if retried.")));
	}

	return true;
}

/*
 * RecnoRecordReadConflict -- record that the current transaction read a
 * tuple version that was subsequently overwritten by another transaction
 * with the given commit timestamp.
 *
 * This creates an outgoing rw-conflict edge: me --rw--> writer.
 * It means the current transaction read a version that the writer
 * overwrote, and the writer committed.
 *
 * After recording the conflict, we check for a dangerous structure.
 *
 * Parameters:
 *   writer_start_ts  - start timestamp of the writing transaction
 *   writer_commit_ts - commit timestamp of the writing transaction (0 if
 *                      still running)
 *   writer_committed - whether the writer has committed
 */
void
RecnoRecordReadConflict(uint64 writer_start_ts, uint64 writer_commit_ts,
						bool writer_committed)
{
	RecnoRWConflict *conflict;
	MemoryContext oldcxt;
	ListCell   *lc;

	if (MyRecnoXactState == NULL)
		RecnoInitTransactionState();

	if (!MyRecnoXactState->is_serializable || !RecnoMvccShmem->anti_deps_enabled)
		return;

	/* Don't record conflicts with ourselves */
	if (writer_start_ts == MyRecnoXactState->xact_start_ts)
		return;

	/* Check for duplicate -- avoid recording the same conflict twice */
	foreach(lc, MyRecnoXactState->rw_conflicts_out)
	{
		RecnoRWConflict *existing = (RecnoRWConflict *) lfirst(lc);

		if (existing->other_xact_ts == writer_start_ts)
		{
			/* Update commit info if newly known */
			if (!existing->other_committed && writer_committed)
			{
				existing->other_committed = true;
				existing->other_commit_ts = writer_commit_ts;
			}
			return;
		}
	}

	/* Record the new outgoing conflict */
	oldcxt = MemoryContextSwitchTo(TopTransactionContext);

	conflict = (RecnoRWConflict *) palloc(sizeof(RecnoRWConflict));
	conflict->other_xact_ts = writer_start_ts;
	conflict->other_commit_ts = writer_commit_ts;
	conflict->other_committed = writer_committed;
	conflict->other_is_read_only = false;	/* Writer is not read-only */

	MyRecnoXactState->rw_conflicts_out =
		lappend(MyRecnoXactState->rw_conflicts_out, conflict);
	MyRecnoXactState->has_conflict_out = true;

	MemoryContextSwitchTo(oldcxt);

	/*
	 * Check for dangerous structure.  We might be the pivot (have both
	 * incoming and outgoing conflicts), or the "Tin" transaction in a Tin ->
	 * Tpivot -> Tout structure.
	 */
	if (RecnoCheckForDangerousStructure())
	{
		ereport(ERROR,
				(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
				 errmsg("could not serialize access due to read/write dependencies among transactions"),
				 errdetail_internal("Reason code: Canceled on identification as a pivot, during read."),
				 errhint("The transaction might succeed if retried.")));
	}
}

/*
 * RecnoRecordWriteConflict -- record that the current transaction is
 * overwriting a tuple version that was read by another transaction with
 * the given start timestamp.
 *
 * This creates an incoming rw-conflict edge: reader --rw--> me.
 * It means the reader read a version that we are now overwriting.
 *
 * After recording the conflict, we check for a dangerous structure.
 *
 * Parameters:
 *   reader_start_ts  - start timestamp of the reading transaction
 *   reader_commit_ts - commit timestamp of the reading transaction (0 if
 *                      still running)
 *   reader_committed - whether the reader has committed
 *   reader_is_read_only - whether the reader is a read-only transaction
 */
void
RecnoRecordWriteConflict(uint64 reader_start_ts, uint64 reader_commit_ts,
						 bool reader_committed, bool reader_is_read_only)
{
	RecnoRWConflict *conflict;
	MemoryContext oldcxt;
	ListCell   *lc;

	if (MyRecnoXactState == NULL)
		RecnoInitTransactionState();

	if (!MyRecnoXactState->is_serializable || !RecnoMvccShmem->anti_deps_enabled)
		return;

	/* Don't record conflicts with ourselves */
	if (reader_start_ts == MyRecnoXactState->xact_start_ts)
		return;

	/* Mark that we have performed a write */
	MyRecnoXactState->is_read_only = false;

	/* Check for duplicate */
	foreach(lc, MyRecnoXactState->rw_conflicts_in)
	{
		RecnoRWConflict *existing = (RecnoRWConflict *) lfirst(lc);

		if (existing->other_xact_ts == reader_start_ts)
		{
			if (!existing->other_committed && reader_committed)
			{
				existing->other_committed = true;
				existing->other_commit_ts = reader_commit_ts;
			}
			return;
		}
	}

	/* Record the new incoming conflict */
	oldcxt = MemoryContextSwitchTo(TopTransactionContext);

	conflict = (RecnoRWConflict *) palloc(sizeof(RecnoRWConflict));
	conflict->other_xact_ts = reader_start_ts;
	conflict->other_commit_ts = reader_commit_ts;
	conflict->other_committed = reader_committed;
	conflict->other_is_read_only = reader_is_read_only;

	MyRecnoXactState->rw_conflicts_in =
		lappend(MyRecnoXactState->rw_conflicts_in, conflict);
	MyRecnoXactState->has_conflict_in = true;

	MemoryContextSwitchTo(oldcxt);

	/*
	 * Check for dangerous structure.
	 */
	if (RecnoCheckForDangerousStructure())
	{
		ereport(ERROR,
				(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
				 errmsg("could not serialize access due to read/write dependencies among transactions"),
				 errdetail_internal("Reason code: Canceled on identification as a pivot, during write."),
				 errhint("The transaction might succeed if retried.")));
	}
}

/*
 * RecnoMarkTransactionWrite -- mark that the current serializable transaction
 * has performed a write operation.  Called from insert/update/delete paths.
 */
void
RecnoMarkTransactionWrite(void)
{
	if (MyRecnoXactState == NULL)
		RecnoInitTransactionState();

	MyRecnoXactState->is_read_only = false;
}

/*
 * RecnoCheckForDangerousStructure -- check if the current transaction is
 * part of a "dangerous structure" that could lead to a serialization anomaly.
 *
 * The dangerous structure from the SSI paper is:
 *
 *     T_in --rw--> T_pivot --rw--> T_out
 *
 * where T_out committed first.  This is detected by checking whether the
 * current transaction (as T_pivot) has BOTH:
 *   - at least one incoming rw-conflict from a non-doomed transaction
 *   - at least one outgoing rw-conflict to a committed (or prepared)
 *     transaction
 *
 * The key optimizations from the SSI paper:
 *
 * 1. A read-only T_in that acquired its snapshot after T_out committed
 *    cannot be part of a real anomaly, because it would see T_out's writes.
 *
 * 2. If T_out committed before T_pivot (which we are), then the structure
 *    Tin -> Tpivot -> Tout is dangerous.
 *
 * 3. If T_in committed before T_out, the structure is not dangerous.
 *
 * Returns true if a dangerous structure is detected and the current
 * transaction should be aborted.
 */
static bool
RecnoCheckForDangerousStructure(void)
{
	ListCell   *lc_in;
	ListCell   *lc_out;

	/* Need both incoming and outgoing conflicts to be a pivot */
	if (!MyRecnoXactState->has_conflict_in ||
		!MyRecnoXactState->has_conflict_out)
		return false;

	/*
	 * Check each outgoing conflict (me --rw--> T_out) to see if T_out has
	 * committed or prepared.  If so, we are a potential pivot.
	 */
	foreach(lc_out, MyRecnoXactState->rw_conflicts_out)
	{
		RecnoRWConflict *out_conflict = (RecnoRWConflict *) lfirst(lc_out);

		if (!out_conflict->other_committed)
			continue;			/* T_out hasn't committed yet */

		/*
		 * T_out committed.  Now check each incoming conflict (T_in --rw-->
		 * me) to see if there's a valid T_in that makes this dangerous.
		 */
		foreach(lc_in, MyRecnoXactState->rw_conflicts_in)
		{
			RecnoRWConflict *in_conflict = (RecnoRWConflict *) lfirst(lc_in);

			/*
			 * Optimization 1: If T_in is read-only and it started after T_out
			 * committed, T_in would see T_out's writes and there's no
			 * anomaly.
			 */
			if (in_conflict->other_is_read_only &&
				in_conflict->other_xact_ts >= out_conflict->other_commit_ts)
				continue;

			/*
			 * Optimization 2: If T_in already committed before T_out
			 * committed, the serialization order T_in < T_pivot < T_out is
			 * valid and there's no anomaly.  This is because T_in's reads
			 * were consistent with a serial order where T_in precedes T_out.
			 */
			if (in_conflict->other_committed &&
				in_conflict->other_commit_ts < out_conflict->other_commit_ts)
				continue;

			/*
			 * We have a dangerous structure: T_in --rw--> me(pivot) --rw-->
			 * T_out(committed)
			 *
			 * and T_in either hasn't committed yet or committed after T_out.
			 * We must abort to prevent a potential serialization anomaly.
			 */
			ereport(DEBUG2,
					(errmsg("RECNO SSI: dangerous structure detected, "
							"T_in(start=" UINT64_FORMAT ") -> me(start=" UINT64_FORMAT
							") -> T_out(start=" UINT64_FORMAT ", commit=" UINT64_FORMAT ")",
							in_conflict->other_xact_ts,
							MyRecnoXactState->xact_start_ts,
							out_conflict->other_xact_ts,
							out_conflict->other_commit_ts)));

			return true;
		}
	}

	return false;
}

/*
 * Commit the current transaction and assign commit timestamp.
 *
 * In HLC mode, the commit HLC captures causal ordering: it is guaranteed
 * to be greater than any HLC this transaction has observed (via the
 * msg_hlc=0 local-event path).
 *
 * DVV has been removed; HLC is the sole clock mechanism.
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
		/* Use transaction start timestamp for MVCC snapshots */
		if (MyRecnoXactState == NULL)
			RecnoInitTransactionState();
		return MyRecnoXactState->xact_start_ts;
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
	 * once via RecnoSLogLookupAll() on first need, then reused for all
	 * subsequent checks (uncommitted insert, dirty xid, aborted entry,
	 * own delete/update).  This collapses up to 7 partition lock
	 * acquisitions into 1.
	 */
	RecnoSLogEntry slog_entries[RECNO_SLOG_MAX_OPS];
	int			slog_nfound = -1;	/* -1 = not yet fetched */

#define SLOG_ENSURE_FETCHED() \
	do { \
		if (slog_nfound < 0) \
			slog_nfound = RecnoSLogLookupAll(relid, &tuple->t_ctid, \
											 slog_entries, RECNO_SLOG_MAX_OPS); \
	} while (0)

	if (tuple == NULL)
		return false;

	myxid = GetTopTransactionIdIfAny();

	/*
	 * Check RECNO_TUPLE_UNCOMMITTED flag.  When set, the inserting
	 * transaction has not yet committed.  Consult the sLog to determine
	 * if this is our own insert (self-visibility) or another transaction's
	 * in-progress insert (not visible).
	 *
	 * This replaces the old t_xmin / CLOG / hint-bit logic.
	 */
	if (tuple->t_flags & RECNO_TUPLE_UNCOMMITTED)
	{
		SLOG_ENSURE_FETCHED();

		/*
		 * Check for our own insert (replaces RecnoCheckUncommittedInsert).
		 */
		if (TransactionIdIsValid(myxid))
		{
			int		i;

			for (i = 0; i < slog_nfound; i++)
			{
				if (!TransactionIdEquals(slog_entries[i].xid, myxid))
					continue;

				/* Our DELETE → not visible */
				if (slog_entries[i].op_type == RECNO_SLOG_DELETE)
					goto not_visible;

				/* Subtransaction rollback marked ABORTED → fall through */
				if (slog_entries[i].op_type == RECNO_SLOG_ABORTED)
					break;

				/* Old version of out-of-place update or explicitly deleted */
				if (tuple->t_flags & (RECNO_TUPLE_DELETED | RECNO_TUPLE_UPDATED))
					goto not_visible;

				/* Our INSERT or in-place UPDATE */
				if (curcid != InvalidCommandId && tuple->t_cid >= curcid)
					goto not_visible;	/* created after scan started */
				goto visible;
			}
		}

		/*
		 * No sLog entry for our xid.  Check if another transaction still
		 * has an in-progress operation (replaces RecnoSLogGetDirtyXid).
		 */
		{
			int		i;

			for (i = 0; i < slog_nfound; i++)
			{
				if (TransactionIdIsCurrentTransactionId(slog_entries[i].xid))
					continue;
				if (!TransactionIdIsInProgress(slog_entries[i].xid))
					continue;
				/* Another txn's in-progress operation */
				goto not_visible;
			}
		}

		/*
		 * Check for aborted entries (replaces RecnoSLogHasAbortedEntry).
		 */
		{
			int		i;

			for (i = 0; i < slog_nfound; i++)
			{
				if (slog_entries[i].op_type == RECNO_SLOG_ABORTED)
					goto not_visible;

				if (TransactionIdIsCurrentTransactionId(slog_entries[i].xid))
					continue;

				if (!TransactionIdIsInProgress(slog_entries[i].xid) &&
					TransactionIdDidAbort(slog_entries[i].xid))
					goto not_visible;
			}
		}

		/*
		 * Fall through: operation committed, UNCOMMITTED flag is stale.
		 * Lazily clear via BufferSetHintBits16 (handles lock upgrade
		 * from SHARE to SHARE_EXCLUSIVE) so subsequent scans skip sLog.
		 */
		if (BufferIsValid(buffer))
			BufferSetHintBits16(&tuple->t_flags,
								tuple->t_flags & ~RECNO_TUPLE_UNCOMMITTED,
								buffer);
		else
			tuple->t_flags &= ~RECNO_TUPLE_UNCOMMITTED;
	}

	/*
	 * UNCOMMITTED is NOT set: the insert has committed.
	 * Now check deletion status.
	 *
	 * LOCKED flag means FOR SHARE/FOR KEY SHARE/FOR UPDATE holds a lock.
	 * The tuple itself is still live and visible — the lock only affects
	 * concurrency semantics, not visibility.  If the tuple is only
	 * LOCKED (no DELETED or UPDATED flag), skip the deletion checks
	 * and fall through to the normal timestamp comparison.
	 */
	tuple_commit_ts = tuple->t_commit_ts;
	is_deleted = (tuple->t_flags & RECNO_TUPLE_DELETED) != 0;

	/*
	 * Treat RECNO_TUPLE_UPDATED (old version of an out-of-place update) as
	 * effectively deleted.
	 */
	if (tuple->t_flags & RECNO_TUPLE_UPDATED)
		is_deleted = true;

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
			int		i;

			for (i = 0; i < slog_nfound; i++)
			{
				if (TransactionIdEquals(slog_entries[i].xid, myxid) &&
					(slog_entries[i].op_type == RECNO_SLOG_DELETE ||
					 slog_entries[i].op_type == RECNO_SLOG_UPDATE))
				{
					/* Our own uncommitted delete or out-of-place update */
					goto not_visible;
				}
			}
		}

		/* Check for another txn's in-progress delete (dirty xid check) */
		{
			int		i;

			for (i = 0; i < slog_nfound; i++)
			{
				if (TransactionIdIsCurrentTransactionId(slog_entries[i].xid))
					continue;
				if (!TransactionIdIsInProgress(slog_entries[i].xid))
					continue;
				if (slog_entries[i].op_type != RECNO_SLOG_INSERT)
				{
					/* Another txn's uncommitted delete → tuple still visible */
					is_deleted = false;
					break;
				}
			}
		}

		/* Check for aborted delete/update (UNDO pending) */
		if (is_deleted)
		{
			int		i;

			for (i = 0; i < slog_nfound; i++)
			{
				if (slog_entries[i].op_type == RECNO_SLOG_ABORTED)
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

	if (snapshot_ts >= tuple_commit_ts)
		return true;

	/*
	 * Timestamp says not visible (commit_ts > snapshot_ts).  Check the sLog
	 * for our own in-progress operation (in-place UPDATE case).
	 */
	if (TransactionIdIsValid(myxid))
	{
		int		i;

		SLOG_ENSURE_FETCHED();

		for (i = 0; i < slog_nfound; i++)
		{
			if (TransactionIdEquals(slog_entries[i].xid, myxid) &&
				slog_entries[i].op_type != RECNO_SLOG_DELETE)
			{
				if (curcid != InvalidCommandId && tuple->t_cid >= curcid)
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
 * GUC assign hooks
 */
void
assign_recno_enable_serializable(bool newval, void *extra)
{
	if (RecnoMvccShmem != NULL)
	{
		LWLockAcquire(&RecnoMvccShmem->mvcc_lock, LW_EXCLUSIVE);
		RecnoMvccShmem->anti_deps_enabled = newval;
		LWLockRelease(&RecnoMvccShmem->mvcc_lock);
	}
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
 * DVV has been removed; HLC is the sole clock mechanism.
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
 * RecnoGetSnapshotHLC -- get the snapshot HLC for visibility checks.
 *
 * For MVCC snapshots, returns the transaction's start HLC.
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
			return MyRecnoXactState->xact_start_hlc;
		else
			return (HLCTimestamp) MyRecnoXactState->xact_start_ts;
	}
	else
	{
		/* SnapshotAny or other non-MVCC snapshots */
		return InvalidHLCTimestamp;
	}
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
	RecnoSLogEntry slog_entries[RECNO_SLOG_MAX_OPS];
	int			slog_nfound = -1;

#define SLOG_ENSURE_FETCHED_HLC() \
	do { \
		if (slog_nfound < 0) \
			slog_nfound = RecnoSLogLookupAll(relid, &tuple->t_ctid, \
											 slog_entries, RECNO_SLOG_MAX_OPS); \
	} while (0)

	if (tuple == NULL)
		return false;

	myxid = GetTopTransactionIdIfAny();

	/*
	 * ----- UNCOMMITTED check (insert visibility) -----
	 *
	 * When RECNO_TUPLE_UNCOMMITTED is set, the inserting transaction may
	 * not have committed yet.  Use t_xid_hint (the inserter's XID, stored
	 * in the tuple header at insert time) for a fast check via CLOG /
	 * ProcArray -- no sLog partition lock needed.
	 */
	if (tuple->t_flags & RECNO_TUPLE_UNCOMMITTED)
	{
		TransactionId hint_xid = tuple->t_xid_hint;

		/*
		 * Fast path: t_xid_hint identifies the inserting transaction.
		 * This avoids the sLog lookup for the common INSERT case.
		 */
		if (TransactionIdIsValid(hint_xid))
		{
			if (TransactionIdIsCurrentTransactionId(hint_xid))
			{
				/*
				 * Our own insert.  Check for our own delete/update via sLog
				 * (DELETE/UPDATE operations still use sLog entries).
				 */
				SLOG_ENSURE_FETCHED_HLC();
				{
					int		i;

					for (i = 0; i < slog_nfound; i++)
					{
						if (!TransactionIdEquals(slog_entries[i].xid, myxid))
							continue;
						if (slog_entries[i].op_type == RECNO_SLOG_DELETE)
							return false;
						if (slog_entries[i].op_type == RECNO_SLOG_ABORTED)
							goto check_slog_fallback;
					}
				}

				/* Our insert, not deleted by us */
				if (tuple->t_flags & (RECNO_TUPLE_DELETED | RECNO_TUPLE_UPDATED))
					return false;
				if (curcid != InvalidCommandId && tuple->t_cid >= curcid)
					return false;	/* created after scan started */
				return true;
			}
			else if (TransactionIdIsInProgress(hint_xid))
			{
				/* Another transaction's uncommitted insert -- not visible */
				return false;
			}
			else if (TransactionIdDidAbort(hint_xid))
			{
				/* Inserter aborted -- tuple is invisible (UNDO pending) */
				return false;
			}
			else
			{
				/*
				 * Inserter committed.  Clear the stale UNCOMMITTED flag
				 * using BufferSetHintBits16 (handles lock upgrade from
				 * SHARE to SHARE_EXCLUSIVE internally).
				 */
				if (BufferIsValid(buffer))
					BufferSetHintBits16(&tuple->t_flags,
										tuple->t_flags & ~RECNO_TUPLE_UNCOMMITTED,
										buffer);
				else
					tuple->t_flags &= ~RECNO_TUPLE_UNCOMMITTED;
			}
		}
		else
		{
			/*
			 * No valid t_xid_hint -- fall back to full sLog lookup.
			 * This handles pre-upgrade tuples and speculative inserts.
			 */
			goto check_slog_fallback;
		}

		/* Skip the sLog fallback; UNCOMMITTED resolved via hint */
		goto uncommitted_resolved;

check_slog_fallback:
		SLOG_ENSURE_FETCHED_HLC();

		/* Check for our own insert via sLog */
		if (TransactionIdIsValid(myxid))
		{
			int		i;

			for (i = 0; i < slog_nfound; i++)
			{
				if (!TransactionIdEquals(slog_entries[i].xid, myxid))
					continue;
				if (slog_entries[i].op_type == RECNO_SLOG_DELETE)
					return false;
				if (slog_entries[i].op_type == RECNO_SLOG_ABORTED)
					break;
				if (tuple->t_flags & (RECNO_TUPLE_DELETED | RECNO_TUPLE_UPDATED))
					return false;
				if (curcid != InvalidCommandId && tuple->t_cid >= curcid)
					return false;
				return true;
			}
		}

		/* Check for another txn's in-progress operation via sLog */
		{
			int		i;

			for (i = 0; i < slog_nfound; i++)
			{
				if (TransactionIdIsCurrentTransactionId(slog_entries[i].xid))
					continue;
				if (!TransactionIdIsInProgress(slog_entries[i].xid))
					continue;
				return false;
			}
		}

		/* Check for aborted entries via sLog */
		{
			int		i;

			for (i = 0; i < slog_nfound; i++)
			{
				if (slog_entries[i].op_type == RECNO_SLOG_ABORTED)
					return false;
				if (TransactionIdIsCurrentTransactionId(slog_entries[i].xid))
					continue;
				if (!TransactionIdIsInProgress(slog_entries[i].xid) &&
					TransactionIdDidAbort(slog_entries[i].xid))
					return false;
			}
		}

		/* Stale flag: inserter committed, clear and persist */
		if (BufferIsValid(buffer))
			BufferSetHintBits16(&tuple->t_flags,
								tuple->t_flags & ~RECNO_TUPLE_UNCOMMITTED,
								buffer);
		else
			tuple->t_flags &= ~RECNO_TUPLE_UNCOMMITTED;
	}

uncommitted_resolved:

	/* SnapshotAny: see everything */
	if (snapshot_hlc == InvalidHLCTimestamp)
		return true;

	/*
	 * LOCKED flag means FOR SHARE/FOR KEY SHARE/FOR UPDATE holds a lock.
	 * The tuple itself is still live and visible -- the lock only affects
	 * concurrency semantics, not visibility.  If the tuple is only
	 * LOCKED (no DELETED or UPDATED flag), skip the deletion checks
	 * and fall through to the normal timestamp comparison.
	 */

	tuple_hlc = RecnoTupleGetHLC(tuple);

	is_deleted = (tuple->t_flags & RECNO_TUPLE_DELETED) != 0;
	if (tuple->t_flags & RECNO_TUPLE_UPDATED)
		is_deleted = true;

	if (is_deleted)
	{
		SLOG_ENSURE_FETCHED_HLC();

		/* Check for in-progress delete by any transaction */
		{
			int		i;

			for (i = 0; i < slog_nfound; i++)
			{
				if (TransactionIdIsCurrentTransactionId(slog_entries[i].xid))
				{
					/* Our uncommitted delete */
					if (slog_entries[i].op_type == RECNO_SLOG_DELETE ||
						slog_entries[i].op_type == RECNO_SLOG_UPDATE)
						return false;
					continue;
				}
				if (!TransactionIdIsInProgress(slog_entries[i].xid))
					continue;
				if (slog_entries[i].op_type != RECNO_SLOG_INSERT)
				{
					is_deleted = false;
					break;
				}
			}
		}

		/* Check for aborted delete/update */
		if (is_deleted)
		{
			int		i;

			for (i = 0; i < slog_nfound; i++)
			{
				if (slog_entries[i].op_type == RECNO_SLOG_ABORTED)
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

	if (is_deleted)
		return HLCBefore(snapshot_hlc, tuple_hlc);

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
								 RecnoTransactionState * txn_state,
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
		int		result = RecnoCheckUncommittedInsert(tuple, relid);

		if (result == 1)
			return true;		/* Our insert, not deleted by us */
		if (result == -1)
			return false;		/* Our insert, but also our delete */

		/*
		 * No sLog entry for our xid.  Check if another transaction still
		 * has an in-progress operation.  Then check for ABORTED.
		 */
		{
			TransactionId dirty_xid;

			dirty_xid = RecnoSLogGetDirtyXid(relid, &tuple->t_ctid, NULL);
			if (TransactionIdIsValid(dirty_xid))
				return false;	/* Another txn's in-progress operation */
		}

		if (RecnoSLogHasAbortedEntry(relid, &tuple->t_ctid))
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
	 */
	is_deleted = (tuple->t_flags & RECNO_TUPLE_DELETED) != 0;

	if (tuple->t_flags & RECNO_TUPLE_UPDATED)
		is_deleted = true;

	if (is_deleted)
	{
		TransactionId dirty_xid;
		bool		is_insert;

		dirty_xid = RecnoSLogGetDirtyXid(relid, &tuple->t_ctid, &is_insert);

		if (TransactionIdIsValid(dirty_xid) && !is_insert)
		{
			if (RecnoSLogIsDeletedByMe(relid, &tuple->t_ctid))
				return false;	/* Our uncommitted delete */
			else
				is_deleted = false; /* Another txn's uncommitted delete */
		}

		/* Check for aborted delete/update (UNDO pending) */
		if (is_deleted && RecnoSLogHasAbortedEntry(relid, &tuple->t_ctid))
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
						(errmsg("RECNO: transaction restart due to uncertainty, "
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
 * DVV dominance checks have been removed; HLC is the sole clock.
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
