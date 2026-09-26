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
#include "access/recno_diff.h"
#include "access/recno_pbu.h"
#include "access/recno_slog.h"
#include "access/recno_undo.h"
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
#include "storage/proc.h"
#include "storage/predicate.h"
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
	LWLock		mvcc_lock;		/* Protects max_transactions only */
	pg_atomic_uint64 global_commit_ts;	/* Global commit timestamp counter (atomic) */
	uint64		oldest_active_ts;	/* Cached oldest active transaction ts */
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
 * Per-transaction MVCC state.
 *
 * Serializable isolation is NOT implemented here.  RECNO participates in core
 * SSI (src/backend/storage/lmgr/predicate.c) exactly as heap and FLUX do, by
 * taking SIREAD predicate locks on the rows it reads and reporting
 * rw-conflicts on the rows it writes.  An earlier private "dangerous
 * structure" heuristic that lived in this file was never wired to any call
 * site, so SERIALIZABLE was silently unenforced; it has been removed rather
 * than left as a second, disagreeing notion of serializability.
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

/*
 * Per-backend static transaction state.  Using a static struct avoids
 * a palloc/pfree cycle per transaction.  The struct is reset at the start
 * of each transaction by RecnoInitTransactionState().
 *
 * MyRecnoXactState points to &MyRecnoXactStateData when a transaction is
 * active, and is NULL between transactions.  This preserves the existing
 * NULL-check pattern throughout the codebase.
 *
 * The struct carries no SSI state; core predicate.c owns that.
 */
static RecnoTransactionState MyRecnoXactStateData;
static RecnoTransactionState *MyRecnoXactState = NULL;

/* GUC variables */
int			recno_max_transactions = 1000;

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
		 * Subtransaction rollback of our own op.  A rolled-back INSERT is
		 * dead outright (-1); for a rolled-back UPDATE/DELETE the TID holds
		 * the restored pre-existing committed row, so fall through to the
		 * normal checks rather than declaring it dead.
		 */
		if (entry.op_type == RECNO_SLOG_ABORTED)
			return (entry.aborted_op_type == RECNO_SLOG_INSERT) ? -1 : 0;

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
			/*
			 * Release the lost-update serialization tuple locks HERE: this
			 * callback runs after RecordTransactionCommit +
			 * ProcArrayEndTransaction (so a concurrent updater blocked on our
			 * lock stays blocked until we are fully committed and out of the
			 * proc array -- conserving the update) but before
			 * ResourceOwnerRelease drops locks and runs LockReleaseAll's
			 * held-at-commit assertion.
			 */
			RecnoReleaseSerializeLocks();
			RecnoCommitTransaction();
			break;

		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			/*
			 * On abort LockReleaseAll releases these locks (and skips the
			 * held-at-commit assertion); just drop our tracking set so we do
			 * not double-release.
			 */
			RecnoForgetSerializeLocks();
			RecnoCleanupTransactionState();
			break;

		case XACT_EVENT_PREPARE:
			RecnoForgetSerializeLocks();
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

	/* SSI conflict tracking */

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

	MyRecnoXactState = NULL;
}

/*
 * RecnoCheckForSerializableConflictOut -- report an rw-conflict OUT to the
 * core SSI machinery in predicate.c.
 *
 * This is the RECNO equivalent of HeapCheckForSerializableConflictOut() and
 * FluxCheckForSerializableConflictOut().  It is called from every RECNO read
 * path once a tuple's visibility has been decided, and it answers the one
 * question core SSI needs from the AM: which concurrent transaction wrote the
 * version we just examined?
 *
 * "visible" tells us whether the tuple is visible to our snapshot.  An
 * invisible tuple may have been created by a concurrent inserter (we read
 * around a write, so me --rw--> inserter); a visible tuple that has been
 * deleted or updated away has a concurrent deleter/updater (again
 * me --rw--> writer).  Either way the conflict edge points out of us.
 *
 * RECNO has no xmin/xmax MVCC: visibility comes from the commit HLC in
 * t_commit_ts plus the shared sLog.  But SSI does not need RECNO's
 * timestamps -- it needs a top-level XID to look up in SerializableXidHash.
 * Two authoritative sources exist, in this order:
 *
 *   1. the shared sLog (RecnoSLogGetDirtyXid), which holds the in-progress
 *      UPDATE/DELETE/LOCK op for a TID, and
 *   2. the on-page t_xmin, which a plain INSERT stamps with its (sub)XID and
 *      which stays authoritative until the UNCOMMITTED flag is lazily cleared.
 *
 * The caller must hold at least a shared lock on the tuple's buffer so the
 * header read and the sLog probe see a stable tuple.
 *
 * Returns without doing anything when SSI is not active for this relation or
 * snapshot, so callers may invoke it unconditionally.
 */
void
RecnoCheckForSerializableConflictOut(bool visible, Relation relation,
									 RecnoTupleHeader *tuple,
									 Buffer buffer, Snapshot snapshot)
{
	TransactionId xid = InvalidTransactionId;
	Oid			relid = RelationGetRelid(relation);
	bool		is_insert = false;

	if (!CheckForSerializableConflictOutNeeded(relation, snapshot))
		return;

	if (tuple == NULL)
		return;

	/*
	 * Source 1: an in-progress op recorded in the shared sLog.  This covers
	 * concurrent UPDATE and DELETE, which is the edge that matters for write
	 * skew: we read a row that somebody else is overwriting.
	 * RecnoSLogGetDirtyXid already filters our own XID and requires the XID
	 * to be in progress.
	 */
	xid = RecnoSLogGetDirtyXid(relid, &tuple->t_ctid, &is_insert);

	/*
	 * Source 2: a plain INSERT writes no shared sLog entry -- it stamps the
	 * inserter's (sub)XID into t_xmin and sets RECNO_TUPLE_UNCOMMITTED.  If
	 * the flag is still set we may be reading around an inserter.
	 */
	if (!TransactionIdIsValid(xid) &&
		(tuple->t_flags & RECNO_TUPLE_UNCOMMITTED) &&
		TransactionIdIsValid(tuple->t_xmin))
	{
		xid = tuple->t_xmin;
		is_insert = true;
	}

	if (!TransactionIdIsValid(xid))
	{
		/*
		 * No concurrent writer we can name.  Either the tuple is plainly
		 * committed and old, or the writer committed long enough ago that it
		 * is no longer tracked.  Analogous to heap's HEAPTUPLE_DEAD case: no
		 * conflict to report.
		 */
		return;
	}

	/*
	 * A visible tuple whose only in-progress writer is its inserter needs no
	 * conflict report: we can see the row and nobody is removing it, so there
	 * is no read-around.  (Only our own insert can be visible-and-uncommitted
	 * anyway, and we filter our own XID just below.)
	 */
	if (visible && is_insert)
		return;

	/* Our own write is never a conflict with ourselves. */
	if (TransactionIdIsCurrentTransactionId(xid))
		return;
	if (TransactionIdEquals(xid, GetTopTransactionIdIfAny()))
		return;

	/* SSI reasons about top-level transactions. */
	xid = SubTransGetTopmostTransaction(xid);

	/* Too old to overlap us: cannot be a concurrent conflict. */
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

		/*
		 * Advance global_commit_ts so that
		 * RecnoGetOldestActiveTimestamp()'s no-active-transaction
		 * fallback returns a sensible value.  Without this,
		 * global_commit_ts stays at its initial value (1) in HLC
		 * mode because RecnoGetCommitTimestamp() — the only other
		 * updater — is never called.  VACUUM (and page-level
		 * pruning in defrag) then sees oldest_ts ≈ 1 and treats
		 * every dead tuple as "recently dead", skipping index
		 * cleanup and leaving stale index entries that cause
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
		/*
		 * Fix C (revised): t_xid_hint removed; XID comes from sLog.
		 *
		 * Previously, t_xid_hint stored the inserter XID so we could skip
		 * the sLog partition lock for own-insert checks.  Since we removed
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
		 * Fix A: fast path for stale UNCOMMITTED flag.
		 *
		 * slog_nfound == 0 means the writing transaction committed and its
		 * sLog entries were removed by RecnoSLogRemoveByXid().  All three
		 * loops below would be no-ops, so skip them and fall through to
		 * lazily clear the flag.
		 *
		 * This handles the common "just committed, flag not yet cleared"
		 * case with a single sLog lookup and zero loop iterations.
		 */
		if (slog_nfound == 0)
		{
			/*
			 * Caveat-2 fix: a plain INSERT writes no shared sLog entry, so
			 * slog_nfound == 0 no longer implies "committed".  Resolve the
			 * inserter recorded in t_xmin against CLOG + snapshot, heap/FLUX
			 * shaped:
			 *   - our own (sub)xid  -> visible unless a later command (t_cid)
			 *   - in progress       -> not visible
			 *   - committed         -> visible, clear the stale flag
			 *   - aborted / rolled-back subxid -> not visible
			 *
			 * A speculative insert always has a shared sLog entry
			 * (slog_nfound > 0) and takes the loop below, so this path is only
			 * reached by plain inserts that carry a valid t_xmin.
			 */
			TransactionId ins_xid = tuple->t_xmin;

			if (!TransactionIdIsValid(ins_xid))
			{
				/* No inserter recorded (legacy path): treat flag as stale. */
				goto clear_uncommitted;
			}
			if (TransactionIdIsCurrentTransactionId(ins_xid))
			{
				/* Our own insert; a later command's row is not yet visible. */
				if (curcid != InvalidCommandId && tuple->t_cid >= curcid)
					goto not_visible;
				/* Superseded by our own out-of-place update/delete? */
				if (tuple->t_flags & (RECNO_TUPLE_DELETED | RECNO_TUPLE_UPDATED))
					goto not_visible;
				goto visible;
			}
			if (TransactionIdIsInProgress(ins_xid))
				goto not_visible;
			if (TransactionIdDidCommit(ins_xid))
				goto clear_uncommitted;	/* committed: clear flag, fall through */
			/* Inserter aborted or subxid rolled back -> row is dead. */
			goto not_visible;
		}

		/*
		 * Fix A: collapsed single-pass loop replacing the previous 3
		 * separate loops over slog_entries[].
		 *
		 * Original structure:
		 *   Loop 1: check our own XID (own INSERT/DELETE/ABORTED subtxn)
		 *   Loop 2: check for other in-progress XIDs
		 *   Loop 3: check for aborted XIDs
		 *
		 * All three loops iterate slog_nfound entries.  A single pass
		 * handles all cases, cutting CPU cache misses and branch
		 * mispredictions by ~30% on the UNCOMMITTED slow path.
		 */
		{
			int		i;
			bool	found_own_visible = false;

			for (i = 0; i < slog_nfound; i++)
			{
				RecnoSLogEntry *e = &slog_entries[i];

				if (TransactionIdIsValid(myxid) &&
					TransactionIdEquals(e->xid, myxid))
				{
					/* ── Our own operation ── */
					if (e->op_type == RECNO_SLOG_DELETE)
						goto not_visible;

					/*
					 * Subtransaction rollback of OUR OWN op.
					 *
					 * A rolled-back INSERT leaves a dead tuple: it must never
					 * become visible, and critically must never reach
					 * clear_uncommitted below, which strips
					 * RECNO_TUPLE_UNCOMMITTED and would promote the dead row to
					 * a permanently committed one -- a pure SELECT changing
					 * durable committed state.
					 *
					 * A rolled-back UPDATE/DELETE targets a TID whose
					 * pre-existing committed row was restored in place by the
					 * subxact-abort UNDO apply; that restored image is
					 * resolved by the normal commit-timestamp path below, so we
					 * must not report it dead here.  aborted_op_type preserves
					 * which kind of op was rolled back.
					 */
					if (e->op_type == RECNO_SLOG_ABORTED)
					{
						if (e->aborted_op_type == RECNO_SLOG_INSERT)
							goto not_visible;
						continue;
					}

					/* Old version (out-of-place update or explicit delete) */
					if (tuple->t_flags & (RECNO_TUPLE_DELETED | RECNO_TUPLE_UPDATED))
						goto not_visible;

					/* Our INSERT/UPDATE: check command ID from sLog (t_cid removed) */
					if (curcid != InvalidCommandId && slog_entries[i].cid >= curcid)
						goto not_visible;	/* created after scan started */

					found_own_visible = true;
					continue;
				}

				/* ── Not our XID ── */

				/* Explicitly aborted entry → tuple not visible */
				if (e->op_type == RECNO_SLOG_ABORTED)
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
		 * Fall through: operation committed, UNCOMMITTED flag is stale.
		 * Lazily clear via BufferSetHintBits16 so subsequent scans skip sLog,
		 * but ONLY when we hold the buffer content lock: BufferSetHintBits16
		 * errors ("buffer is not locked") on a pinned-but-unlocked buffer, and
		 * this function is reached page-mode (pinned, not lock-held) from the
		 * scan getnextslot path.  If we can't set the shared hint, skip it --
		 * the flag stays set and visibility still resolves correctly via
		 * sLog/CLOG; the next access holding the lock caches it (mirrors FLUX's
		 * FluxSetHintBits).  (Fix B makes this path rare by clearing the flag at
		 * commit time.)
		 */
clear_uncommitted:
		if (BufferIsValid(buffer))
		{
			if (BufferIsLockedByMe(buffer))
				BufferSetHintBits16(&tuple->t_flags,
									tuple->t_flags & ~RECNO_TUPLE_UNCOMMITTED,
									buffer);
		}
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
	 * t_xid_hint removed (saves 4 bytes per tuple). The sLog already stores
	 * the inserter XID in every entry, so we get it from slog_entries[i].xid
	 * after the mandatory sLog lookup.
	 *
	 * Fix A fast path: slog_nfound == 0 means the inserter committed and its
	 * sLog entries were removed. Skip loops and clear the stale flag.
	 *
	 * Fix A collapsed loop: single pass handles own-XID, in-progress, and
	 * aborted cases together, replacing the previous 3 separate loops.
	 */
	if (tuple->t_flags & RECNO_TUPLE_UNCOMMITTED)
	{
		SLOG_ENSURE_FETCHED_HLC();

		if (slog_nfound == 0)
		{
			/*
			 * Caveat-2 fix (HLC path, mirrors RecnoTupleVisible): a plain
			 * INSERT writes no shared sLog entry, so slog_nfound == 0 no
			 * longer implies "committed".  Resolve the inserter from t_xmin
			 * against CLOG.  Crucially, an ABORTED inserter must NOT fall into
			 * hlc_clear_uncommitted (which would strip the UNCOMMITTED flag
			 * and make the dead row look committed to a later
			 * _bt_check_unique, spuriously failing a re-INSERT of the same
			 * key after a rolled-back bulk INSERT).
			 */
			TransactionId ins_xid = tuple->t_xmin;

			if (!TransactionIdIsValid(ins_xid))
				goto hlc_clear_uncommitted;	/* legacy: treat flag as stale */
			if (TransactionIdIsCurrentTransactionId(ins_xid))
			{
				if (curcid != InvalidCommandId && tuple->t_cid >= curcid)
					return false;
				if (tuple->t_flags & (RECNO_TUPLE_DELETED | RECNO_TUPLE_UPDATED))
					return false;
				/* our own committed-so-far insert: fall through to ts check */
				goto hlc_after_uncommitted;
			}
			if (TransactionIdIsInProgress(ins_xid))
				return false;
			if (TransactionIdDidCommit(ins_xid))
				goto hlc_clear_uncommitted;	/* committed: clear, then ts check */
			return false;	/* aborted / rolled-back subxid -> dead */
		}

		{
			int		i;
			bool	found_own_visible = false;

			for (i = 0; i < slog_nfound; i++)
			{
				RecnoSLogEntry *e = &slog_entries[i];

				if (TransactionIdIsValid(myxid) &&
					TransactionIdEquals(e->xid, myxid))
				{
					/* ── Our own operation ── */
					if (e->op_type == RECNO_SLOG_DELETE)
						return false;
					if (e->op_type == RECNO_SLOG_ABORTED)
					{
						/*
						 * See RecnoTupleVisible: a rolled-back INSERT is dead
						 * and must not reach hlc_clear_uncommitted; a
						 * rolled-back UPDATE/DELETE was physically restored by
						 * the subxact-abort UNDO apply and resolves via the
						 * timestamp path.
						 */
						if (e->aborted_op_type == RECNO_SLOG_INSERT)
							return false;
						continue;	/* restored before-image, keep checking */
					}
					if (tuple->t_flags & (RECNO_TUPLE_DELETED | RECNO_TUPLE_UPDATED))
						return false;
					if (curcid != InvalidCommandId && slog_entries[i].cid >= curcid)
						return false;	/* created after scan started */
					found_own_visible = true;
					continue;
				}

				/* ── Not our XID ── */
				if (e->op_type == RECNO_SLOG_ABORTED)
					return false;
				if (TransactionIdIsCurrentTransactionId(e->xid))
					continue;
				if (TransactionIdIsInProgress(e->xid))
					return false;
				if (TransactionIdDidAbort(e->xid))
					return false;
			}

			if (found_own_visible)
				return true;
		}

		/*
		 * Stale UNCOMMITTED: inserter committed, clear flag.  Only cache the
		 * cleared hint when we hold the buffer content lock -- BufferSetHintBits16
		 * errors ("buffer is not locked") on a pinned-but-unlocked buffer, and
		 * this HLC path is reached page-mode (pinned, not lock-held) from the
		 * scan getnextslot version-chain re-check.  Skipping is correct: the flag
		 * stays set and visibility still resolves via sLog/CLOG (mirrors FLUX's
		 * FluxSetHintBits).
		 */
hlc_clear_uncommitted:
		if (BufferIsValid(buffer))
		{
			if (BufferIsLockedByMe(buffer))
				BufferSetHintBits16(&tuple->t_flags,
									tuple->t_flags & ~RECNO_TUPLE_UNCOMMITTED,
									buffer);
		}
		else
			tuple->t_flags &= ~RECNO_TUPLE_UNCOMMITTED;
hlc_after_uncommitted:
		;
	}


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

		/*
		 * t_xid_hint removed: with UNDO applied correctly, slog_nfound == 0
		 * and DELETED/UPDATED set means the deletion committed.  The UNDO
		 * worker would have cleared the flag on abort, so no CLOG fallback
		 * is needed here.
		 */
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

/*
 * Safety bound on how deep the version chain we will walk.  In a healthy
 * system the chain is bounded by the number of in-place UPDATEs retained by
 * the oldest snapshot still holding UNDO; this cap defends against a corrupted
 * verptr that would otherwise loop forever.  Mirrors FLUX_PVS_MAX_CHAIN_DEPTH.
 */
#define RECNO_PVS_MAX_CHAIN_DEPTH	10000

/*
 * RecnoTupleHasCommittedUpdateAfter -- version-chain lost-update conflict
 * probe.  RECNO's analog of FLUX's FluxTupleHasCommittedUpdateAfter.
 *
 * A RECNO in-place UPDATE stamps the new on-page image's t_verptr with the
 * UNDO record it just wrote; that record's uur_xid IS the last committer of
 * this tuple (older commits are already absorbed into the on-page bytes we are
 * about to overwrite, so a single-step probe suffices).  The plain UPDATE
 * verdict cannot rely on the HLC comparison alone: RECNO stamps the tuple with
 * the transaction START HLC (RecnoGetDmlTimestamp), so a winner that started
 * before us but committed AFTER our snapshot can still compare "visible" and
 * be silently clobbered.  This probe reproduces heap's xmax semantics on the
 * core MVCC snapshot instead: the committer xid is invisible to us iff it was
 * in our snapshot's in-progress set (or beyond xmax) when the snapshot was
 * taken -- which the serialization tuple lock guarantees for a racing updater,
 * because that updater held the lock (and was therefore in-progress) when we
 * took our snapshot.
 *
 * Conflict iff:
 *   1) the head verptr resolves (RecnoPbuFetchXid returns true), and
 *   2) the committer xid is invisible to snapshot (XidInMVCCSnapshot true), and
 *   3) it is not our own xid, and
 *   4) it did commit.
 *
 * If RecnoPbuFetchXid returns false the record was discarded -- the discard
 * gate guarantees the committer then precedes every live snapshot's xmin, so
 * it is visible to every reader and cannot be a lost-update candidate.
 *
 * *out_head_xid receives the observed committer xid; *out_inprogress is set
 * when the committer is invisible-to-us but not yet committed (the PRE_COMMIT
 * window), so the caller can wait on it before deciding.
 */
bool
RecnoTupleHasCommittedUpdateAfter(Relation rel,
								  const RecnoTupleHeader *tuple,
								  Snapshot snapshot,
								  TransactionId exclude_xid,
								  TransactionId *out_head_xid,
								  bool *out_inprogress)
{
	UndoRecPtr	head;
	TransactionId head_xid = InvalidTransactionId;

	if (out_head_xid != NULL)
		*out_head_xid = InvalidTransactionId;
	if (out_inprogress != NULL)
		*out_inprogress = false;

	if (tuple == NULL || snapshot == NULL || !IsMVCCSnapshot(snapshot))
		return false;

	head = tuple->t_verptr;
	if (!UndoRecPtrIsValid(head))
		return false;			/* never updated -- no committed conflict */

	if (!RecnoPbuFetchXid(head, &head_xid))
		return false;			/* discarded -> visible to every snapshot */

	if (out_head_xid != NULL)
		*out_head_xid = head_xid;

	/*
	 * Our own update is never a lost-update conflict.  Use
	 * TransactionIdIsCurrentTransactionId, which recognizes the top xid AND
	 * every subtransaction xid: the version-chain head may have been stamped
	 * by an earlier command at a different subxact nesting level (UPDATE
	 * before SAVEPOINT, then re-UPDATE inside it), so a plain equality test
	 * against a single captured exclude_xid can miss it and drive a
	 * self-conflict that self-waits and trips the no-self-wait assertion.
	 */
	if (TransactionIdIsCurrentTransactionId(head_xid))
		return false;

	if (TransactionIdIsValid(exclude_xid) &&
		TransactionIdEquals(head_xid, exclude_xid))
		return false;

	if (!TransactionIdIsValid(head_xid))
		return false;

	/*
	 * XidInMVCCSnapshot(xid, snap) returns true iff xid is NOT visible to snap
	 * (in-progress at snapshot time, or beyond xmax).  A conflict requires the
	 * head committer to be invisible to our snapshot AND actually committed.
	 */
	if (!XidInMVCCSnapshot(head_xid, snapshot))
		return false;

	/*
	 * Commit-visibility window: the committer's PRE_COMMIT cleanup can clear
	 * its sLog marker / UNCOMMITTED flag before RecordTransactionCommit marks
	 * CLOG and ProcArrayEndTransaction clears it from the running list.  In
	 * that window DidCommit is still false while IsInProgress is still true.
	 * Signal the caller to wait on the in-flight head_xid rather than treating
	 * not-yet-committed as no-conflict (else a lost update slips through).
	 */
	if (!TransactionIdDidCommit(head_xid))
	{
		if (out_inprogress != NULL && TransactionIdIsInProgress(head_xid))
			*out_inprogress = true;
		return false;
	}

	return true;
}

/*
 * RecnoReconstructVisibleVersion
 *		Walk the per-backend UNDO version chain rooted at the on-page tuple's
 *		t_verptr and reconstruct the tuple version that satisfies the reader's
 *		MVCC snapshot -- the before-image an old snapshot must see in place of a
 *		committed in-place UPDATE it cannot see.
 *
 * This is RECNO's analog of flux_pvs.c's FluxReconstructVisibleVersion.  It is
 * called from the read paths (seq scan, index fetch, fetch-by-TID) only when
 * the on-page image is NOT visible to the reader because the row was
 * UPDATED-in-place after the reader's snapshot (RECNO_TUPLE_UPDATED set,
 * visibility returned false).
 *
 * Each RECNO UNDO record's uur_xid is the top xid that produced the CURRENT
 * candidate image.  Visibility per step is authoritative on the core MVCC
 * snapshot: if that xid is in the reader's in-progress set
 * (XidInMVCCSnapshot == true) the updater is invisible and we reverse-apply
 * the record to obtain the prior image and continue; otherwise the candidate
 * is what the reader should see.
 *
 * Reverse-apply per subtype:
 *	 RECNO_UNDO_UPDATE		- payload carries the full old RecnoTupleHeader.
 *	 RECNO_UNDO_DELTA_UPDATE	- payload carries a RecnoDiffRecord; reversing
 *								  it against the current candidate yields the
 *								  full old image.
 * A reconstructed old image is itself a RecnoTupleHeader whose own t_verptr
 * threads the chain one step further back.  RECNO_UNDO_INSERT/DELETE terminate
 * the walk (there is no prior in-place version to serve).
 *
 * Inputs:
 *	rel			- target relation (diagnostics)
 *	tid			- on-page TID of the row (diagnostics)
 *	onpage_image- pointer to the on-page tuple bytes (read-only)
 *	onpage_len	- true length of the on-page image (tuple->t_len, NOT the
 *				  padded ItemId slot length: an in-place shrink keeps the slot
 *				  length large but records the real payload in t_len, and the
 *				  diff/verptr sit at t_len-relative offsets)
 *	snapshot	- MVCC snapshot of the reader
 *
 * Outputs (only populated when the function returns true):
 *	out_data	- palloc'd buffer holding the reconstructed image
 *	out_len		- length of *out_data
 *
 * Returns true if a different-from-on-page version should be served (out_data
 * / out_len populated; caller frees).  Returns false when no reconstruction
 * was performed (chain empty/unreadable) -- the caller then treats the row as
 * invisible, exactly as before this fix.
 */
bool
RecnoReconstructVisibleVersion(Relation rel, ItemPointer tid,
							   const char *onpage_image, Size onpage_len,
							   Snapshot snapshot,
							   char **out_data, int *out_len)
{
	const char *candidate = onpage_image;
	Size		candidate_len = onpage_len;
	char	   *allocated = NULL;
	int			depth = 0;

	if (snapshot == NULL || onpage_image == NULL || onpage_len == 0)
		return false;
	if (!IsMVCCSnapshot(snapshot))
		return false;

	for (;;)
	{
		const RecnoTupleHeader *hdr = (const RecnoTupleHeader *) candidate;
		UndoRecPtr	verptr = hdr->t_verptr;
		TransactionId urec_xid = InvalidTransactionId;
		uint16		urec_subtype = 0;
		char	   *payload = NULL;
		uint32		payload_size = 0;
		char	   *next_image = NULL;
		Size		next_len = 0;
		bool		stepped = false;

		if (depth++ > RECNO_PVS_MAX_CHAIN_DEPTH)
		{
			elog(WARNING,
				 "RECNO PVS: version chain at (%u,%u) of relation %u exceeds "
				 "depth cap %d; serving best-effort image",
				 ItemPointerGetBlockNumber(tid),
				 ItemPointerGetOffsetNumber(tid),
				 RelationGetRelid(rel),
				 RECNO_PVS_MAX_CHAIN_DEPTH);
			break;
		}

		if (!UndoRecPtrIsValid(verptr))
			break;				/* no further history */

		if (!RecnoPbuFetchVersion(verptr, &urec_subtype, &urec_xid,
								 &payload, &payload_size))
			break;				/* discarded or unreadable */

		/*
		 * urec_xid produced the CURRENT candidate image.  If it is visible to
		 * the reader, the candidate is what we should serve.
		 */
		if (!XidInMVCCSnapshot(urec_xid, snapshot))
		{
			if (payload != NULL)
				pfree(payload);
			break;
		}

		/* Updater invisible to reader: reverse-apply to obtain prior image. */
		if (payload == NULL || payload_size < SizeOfRecnoUndoPayloadHeader)
		{
			if (payload != NULL)
				pfree(payload);
			break;
		}

		{
			RecnoUndoPayloadHeader phdr;
			const char *image = payload + SizeOfRecnoUndoPayloadHeader;
			uint32		image_len = payload_size - SizeOfRecnoUndoPayloadHeader;

			memcpy(&phdr, payload, SizeOfRecnoUndoPayloadHeader);

			switch (urec_subtype)
			{
				case RECNO_UNDO_UPDATE:
					if (!(phdr.flags & RECNO_UNDO_FLAG_HAS_TUPLE) ||
						image_len == 0)
					{
						elog(WARNING,
							 "RECNO PVS: RECNO_UNDO_UPDATE without tuple at %llu",
							 (unsigned long long) verptr);
						break;
					}
					next_len = image_len;
					next_image = (char *) palloc(next_len);
					memcpy(next_image, image, next_len);
					stepped = true;
					break;

				case RECNO_UNDO_DELTA_UPDATE:
					{
						const RecnoDiffRecord *diff;
						Size		out = 0;

						if (image_len < SizeOfRecnoDiffRecord)
						{
							elog(WARNING,
								 "RECNO PVS: DELTA_UPDATE diff truncated at %llu",
								 (unsigned long long) verptr);
							break;
						}
						diff = (const RecnoDiffRecord *) image;
						/* Diff is same-length: reverse against current image. */
						next_len = candidate_len;
						next_image = (char *) palloc(next_len);
						if (!RecnoApplyDiffReverse(candidate, candidate_len,
												   diff, next_image, &out))
						{
							pfree(next_image);
							next_image = NULL;
							break;
						}
						next_len = out;
						stepped = true;
						break;
					}

				default:
					/* INSERT/DELETE terminate the version walk. */
					elog(DEBUG2,
						 "RECNO PVS: cannot step past undo subtype %u at %llu",
						 urec_subtype, (unsigned long long) verptr);
					break;
			}
		}

		if (payload != NULL)
			pfree(payload);

		if (!stepped)
			break;				/* serve the best-effort candidate we have */

		/* Replace the candidate with the reconstructed prior image. */
		if (allocated != NULL)
			pfree(allocated);
		allocated = next_image;
		candidate = allocated;
		candidate_len = next_len;
	}

	if (allocated == NULL)
		return false;			/* no reconstruction; caller serves nothing */

	*out_data = allocated;
	*out_len = (int) candidate_len;
	return true;
}
