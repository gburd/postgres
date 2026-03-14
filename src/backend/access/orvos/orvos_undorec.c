/*
 * orvos_undorec.c
 *		Functions for working on UNDO records.
 *
 * This file contains higher-level functions for constructing UNDO records
 * for different kinds of WAL records.
 *
 * If you perform multiple operations in the same transaction and command, we
 * reuse the same UNDO record for it. There's a one-element cache of each
 * operation type, so this only takes effect in simple cases.
 *
 * TODO: make the caching work in more cases. A hash table or something..
 * Currently, we do this for DELETEs and INSERTs. We could perhaps do this
 * for UPDATEs as well, although they're more a bit more tricky, as we need
 * to also store the 'ctid' pointer to the new tuple in an UPDATE.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/orvos/orvos_undorec.c
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/multixact.h"
#include "access/xlogreader.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "access/orvos_internal.h"
#include "access/orvos_undolog.h"
#include "access/orvos_undorec.h"
#include "access/orvos_wal.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "lib/integerset.h"
#include "miscadmin.h"
#include "postmaster/autovacuum.h"
#include "pgstat.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "utils/memutils.h"
#include "utils/pg_rusage.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"
#include "utils/snapmgr.h"

/*
 * Working area for VACUUM.
 */
typedef struct OVVacRelStats
{
	Relation	rel;			/* heap relation being vacuumed */
	int			elevel;
	BufferAccessStrategy vac_strategy;

	/* hasindex = true means two-pass strategy; false means one-pass */
	bool		hasindex;
	/* Overall statistics about rel */
	BlockNumber rel_pages;		/* total number of pages */
	BlockNumber tupcount_pages; /* pages whose tuples we counted */
	double		old_live_tuples;	/* previous value of pg_class.reltuples */
	double		new_rel_tuples; /* new estimated total # of tuples */
	double		new_live_tuples;	/* new estimated total # of live tuples */
	double		new_dead_tuples;	/* new estimated total # of dead tuples */
	BlockNumber pages_removed;
	double		tuples_deleted;

	IntegerSet *dead_tids;
}			OVVacRelStats;

static bool ov_lazy_tid_reaped(ItemPointer itemptr, void *state);
static void lazy_vacuum_index(Relation indrel,
							  IndexBulkDeleteResult **stats,
							  OVVacRelStats * vacrelstats,
							  Relation heaprel);
static void lazy_cleanup_index(Relation indrel,
							   IndexBulkDeleteResult *stats,
							   OVVacRelStats * vacrelstats);


/*
 * Fetch the UNDO record with the given undo-pointer.
 *
 * The returned record is a palloc'd copy.
 *
 * If the record could not be found, returns NULL. That can happen if you try
 * to fetch an UNDO record that has already been discarded. I.e. if undoptr
 * is smaller than the oldest UNDO pointer stored in the metapage.
 */
OVUndoRec *
ovundo_fetch_record(Relation rel, OVUndoRecPtr undoptr)
{
	OVUndoRec  *undorec_copy;
	OVUndoRec  *undorec;
	Buffer		buf;

	undorec = (OVUndoRec *) ovundo_fetch(rel, undoptr, &buf, BUFFER_LOCK_SHARE, true);

	if (undorec)
	{
		undorec_copy = palloc(undorec->size);
		memcpy(undorec_copy, undorec, undorec->size);
	}
	else
		undorec_copy = NULL;

	if (BufferIsValid(buf))
		UnlockReleaseBuffer(buf);

	return undorec_copy;
}


ov_pending_undo_op *
ovundo_create_for_delete(Relation rel, TransactionId xid, CommandId cid, ovtid tid,
						 bool changedPart, OVUndoRecPtr prev_undo_ptr)
{
	OVUndoRec_Delete *undorec;
	ov_pending_undo_op *pending_op;

	static RelFileLocator cached_relfilenode;
	static TransactionId cached_xid;
	static CommandId cached_cid;
	static bool cached_changedPart;
	static OVUndoRecPtr cached_prev_undo_ptr;
	static OVUndoRecPtr cached_undo_ptr;

	if (RelFileLocatorEquals(rel->rd_locator, cached_relfilenode) &&
		xid == cached_xid &&
		cid == cached_cid &&
		changedPart == cached_changedPart &&
		prev_undo_ptr.counter == cached_prev_undo_ptr.counter)
	{
		Buffer		buf;
		OVUndoRec_Delete *orig_undorec;

		orig_undorec = (OVUndoRec_Delete *) ovundo_fetch(rel, cached_undo_ptr,
														 &buf, BUFFER_LOCK_EXCLUSIVE, false);

		if (orig_undorec->rec.type != OVUNDO_TYPE_DELETE)
			elog(ERROR, "unexpected undo record type %d, expected DELETE", orig_undorec->rec.type);

		/* Is there space for a new TID in the record? */
		if (orig_undorec->num_tids < OVUNDO_NUM_TIDS_PER_DELETE)
		{
			pending_op = palloc(offsetof(ov_pending_undo_op, payload) + sizeof(OVUndoRec_Delete));
			undorec = (OVUndoRec_Delete *) pending_op->payload;

			pending_op->reservation.undobuf = buf;
			pending_op->reservation.undorecptr = cached_undo_ptr;
			pending_op->reservation.length = sizeof(OVUndoRec_Delete);
			pending_op->reservation.ptr = (char *) orig_undorec;
			pending_op->is_update = true;

			memcpy(undorec, orig_undorec, sizeof(OVUndoRec_Delete));
			undorec->tids[undorec->num_tids] = tid;
			undorec->num_tids++;

			return pending_op;
		}
		UnlockReleaseBuffer(buf);
	}

	/*
	 * Cache miss. Create a new UNDO record.
	 */
	pending_op = palloc(offsetof(ov_pending_undo_op, payload) + sizeof(OVUndoRec_Delete));
	pending_op->is_update = false;

	ovundo_insert_reserve(rel, sizeof(OVUndoRec_Delete), &pending_op->reservation);

	undorec = (OVUndoRec_Delete *) pending_op->payload;
	undorec->rec.size = sizeof(OVUndoRec_Delete);
	undorec->rec.type = OVUNDO_TYPE_DELETE;
	undorec->rec.undorecptr = pending_op->reservation.undorecptr;
	undorec->rec.xid = xid;
	undorec->rec.cid = cid;
	undorec->changedPart = changedPart;
	undorec->rec.prevundorec = prev_undo_ptr;
	undorec->tids[0] = tid;
	undorec->num_tids = 1;

	/*
	 * XXX: this caching mechanism assumes that once we've reserved the undo
	 * record, we never change our minds and don't write the undo record,
	 * after all.
	 */
	cached_relfilenode = rel->rd_locator;
	cached_xid = xid;
	cached_cid = cid;
	cached_changedPart = changedPart;
	cached_prev_undo_ptr = prev_undo_ptr;
	cached_undo_ptr = pending_op->reservation.undorecptr;

	return pending_op;
}

/*
 * Create an UNDO record for insertion.
 *
 * The undo record stores the 'tid' of the row, as well as visibility information.
 *
 * There's a primitive caching mechanism here: If you perform multiple insertions
 * with same visibility information, and consecutive TIDs, we will keep modifying
 * the range of TIDs in the same UNDO record, instead of creating new records.
 * That greatly reduces the space required for UNDO log of bulk inserts.
 */
ov_pending_undo_op *
ovundo_create_for_insert(Relation rel, TransactionId xid, CommandId cid, ovtid tid,
						 int nitems, uint32 speculative_token, OVUndoRecPtr prev_undo_ptr)
{
	OVUndoRec_Insert *undorec;
	ov_pending_undo_op *pending_op;

	static RelFileLocator cached_relfilenode;
	static TransactionId cached_xid;
	static CommandId cached_cid;
	static ovtid cached_endtid;
	static OVUndoRecPtr cached_prev_undo_ptr;
	static OVUndoRecPtr cached_undo_ptr;

	if (speculative_token == INVALID_SPECULATIVE_TOKEN &&
		RelFileLocatorEquals(rel->rd_locator, cached_relfilenode) &&
		xid == cached_xid &&
		cid == cached_cid &&
		tid == cached_endtid &&
		prev_undo_ptr.counter == cached_prev_undo_ptr.counter)
	{
		Buffer		buf;
		OVUndoRec_Insert *orig_undorec;

		orig_undorec = (OVUndoRec_Insert *) ovundo_fetch(rel, cached_undo_ptr,
														 &buf, BUFFER_LOCK_EXCLUSIVE, false);

		if (orig_undorec->rec.type != OVUNDO_TYPE_INSERT)
			elog(ERROR, "unexpected undo record type %d, expected INSERT", orig_undorec->rec.type);

		/* Extend the range of the old record to cover the new TID */
		Assert(orig_undorec->endtid == tid);
		Assert(orig_undorec->speculative_token == INVALID_SPECULATIVE_TOKEN);

		pending_op = palloc(offsetof(ov_pending_undo_op, payload) + sizeof(OVUndoRec_Insert));
		undorec = (OVUndoRec_Insert *) pending_op->payload;

		pending_op->reservation.undobuf = buf;
		pending_op->reservation.undorecptr = cached_undo_ptr;
		pending_op->reservation.length = sizeof(OVUndoRec_Insert);
		pending_op->reservation.ptr = (char *) orig_undorec;
		pending_op->is_update = true;

		memcpy(undorec, orig_undorec, sizeof(OVUndoRec_Insert));
		undorec->endtid = tid + nitems;

		cached_endtid = tid + nitems;

		return pending_op;
	}

	/*
	 * Cache miss. Create a new UNDO record.
	 */
	pending_op = palloc(offsetof(ov_pending_undo_op, payload) + sizeof(OVUndoRec_Insert));
	pending_op->is_update = false;
	ovundo_insert_reserve(rel, sizeof(OVUndoRec_Insert), &pending_op->reservation);
	undorec = (OVUndoRec_Insert *) pending_op->payload;

	undorec->rec.size = sizeof(OVUndoRec_Insert);
	undorec->rec.type = OVUNDO_TYPE_INSERT;
	undorec->rec.undorecptr = pending_op->reservation.undorecptr;
	undorec->rec.xid = xid;
	undorec->rec.cid = cid;
	undorec->rec.prevundorec = prev_undo_ptr;
	undorec->firsttid = tid;
	undorec->endtid = tid + nitems;
	undorec->speculative_token = speculative_token;

	if (speculative_token == INVALID_SPECULATIVE_TOKEN)
	{
		cached_relfilenode = rel->rd_locator;
		cached_xid = xid;
		cached_cid = cid;
		cached_endtid = tid + nitems;
		cached_prev_undo_ptr = prev_undo_ptr;
		cached_undo_ptr = pending_op->reservation.undorecptr;
	}

	return pending_op;
}

ov_pending_undo_op *
ovundo_create_for_update(Relation rel, TransactionId xid, CommandId cid,
						 ovtid oldtid, ovtid newtid, OVUndoRecPtr prev_undo_ptr,
						 bool key_update)
{
	OVUndoRec_Update *undorec;
	ov_pending_undo_op *pending_op;

	/*
	 * Create a new UNDO record.
	 */
	pending_op = palloc(offsetof(ov_pending_undo_op, payload) + sizeof(OVUndoRec_Update));
	pending_op->is_update = false;
	ovundo_insert_reserve(rel, sizeof(OVUndoRec_Update), &pending_op->reservation);

	undorec = (OVUndoRec_Update *) pending_op->payload;
	undorec->rec.size = sizeof(OVUndoRec_Update);
	undorec->rec.type = OVUNDO_TYPE_UPDATE;
	undorec->rec.undorecptr = pending_op->reservation.undorecptr;
	undorec->rec.xid = xid;
	undorec->rec.cid = cid;
	undorec->rec.prevundorec = prev_undo_ptr;
	undorec->oldtid = oldtid;
	undorec->newtid = newtid;
	undorec->key_update = key_update;

	return pending_op;
}

/*
 * Create a DELTA_INSERT UNDO record for column-delta UPDATEs.
 *
 * Like ovundo_create_for_insert, but stores the predecessor TID and
 * a bitmap of which columns were actually changed. Unchanged columns
 * are not inserted into their B-trees; the fetch path will look them
 * up from the predecessor TID instead.
 */
ov_pending_undo_op *
ovundo_create_for_delta_insert(Relation rel,
							   TransactionId xid, CommandId cid,
							   ovtid tid, int nitems,
							   ovtid predecessor_tid,
							   int natts, const bool *changed_cols,
							   OVUndoRecPtr prev_undo_ptr)
{
	OVUndoRec_DeltaInsert *undorec;
	ov_pending_undo_op *pending_op;
	Size		rec_size;
	int			nwords;
	int			nchanged;

	Assert(natts > 0 && natts <= OVUNDO_DELTA_MAX_COLS);

	nwords = OVUNDO_DELTA_BITMAP_WORDS(natts);
	rec_size = SizeOfOVUndoRecDeltaInsert(natts);

	pending_op = palloc(offsetof(ov_pending_undo_op, payload) +
						rec_size);
	pending_op->is_update = false;
	ovundo_insert_reserve(rel, rec_size, &pending_op->reservation);

	undorec = (OVUndoRec_DeltaInsert *) pending_op->payload;
	undorec->rec.size = rec_size;
	undorec->rec.type = OVUNDO_TYPE_DELTA_INSERT;
	undorec->rec.undorecptr = pending_op->reservation.undorecptr;
	undorec->rec.xid = xid;
	undorec->rec.cid = cid;
	undorec->rec.prevundorec = prev_undo_ptr;
	undorec->firsttid = tid;
	undorec->endtid = tid + nitems;
	undorec->speculative_token = INVALID_SPECULATIVE_TOKEN;
	undorec->predecessor_tid = predecessor_tid;
	undorec->natts = natts;

	/* Build the changed columns bitmap */
	memset(undorec->changed_cols, 0, nwords * sizeof(uint32));
	nchanged = 0;
	for (int attno = 1; attno <= natts; attno++)
	{
		if (changed_cols[attno - 1])
		{
			ov_delta_col_set_changed(undorec, attno);
			nchanged++;
		}
	}
	undorec->nchanged = nchanged;

	return pending_op;
}

ov_pending_undo_op *
ovundo_create_for_tuple_lock(Relation rel, TransactionId xid, CommandId cid,
							 ovtid tid, LockTupleMode lockmode,
							 OVUndoRecPtr prev_undo_ptr)
{
	OVUndoRec_TupleLock *undorec;
	ov_pending_undo_op *pending_op;

	(void) tid;

	/*
	 * Create a new UNDO record.
	 */
	pending_op = palloc(offsetof(ov_pending_undo_op, payload) + sizeof(OVUndoRec_TupleLock));
	pending_op->is_update = false;
	ovundo_insert_reserve(rel, sizeof(OVUndoRec_TupleLock), &pending_op->reservation);

	undorec = (OVUndoRec_TupleLock *) pending_op->payload;
	undorec->rec.size = sizeof(OVUndoRec_TupleLock);
	undorec->rec.type = OVUNDO_TYPE_TUPLE_LOCK;
	undorec->rec.undorecptr = pending_op->reservation.undorecptr;
	undorec->rec.xid = xid;
	undorec->rec.cid = cid;
	undorec->rec.prevundorec = prev_undo_ptr;
	undorec->lockmode = lockmode;

	return pending_op;
}


/*
 * Threshold for the heuristic that decides whether UNDO trimming is
 * worthwhile.  We skip trimming unless at least this many new UNDO records
 * have been created since the last trim, OR RecentXmin has advanced past the
 * cached oldest transaction.  Keeping this small enough avoids unbounded UNDO
 * growth while still dramatically reducing metapage lock contention.
 */
#define UNDO_TRIM_COUNTER_THRESHOLD		64

/*
 * Read the cached oldest_undo_ptr from the metapage using only a shared
 * buffer lock.  This is the fast path: no heavyweight page lock, no UNDO log
 * scan.
 *
 * Returns InvalidUndoPtr if the metapage has no UNDO log yet.
 */
static OVUndoRecPtr
ovundo_read_cached_oldest(Relation rel)
{
	Buffer		metabuf;
	Page		metapage;
	OVMetaPageOpaque *metaopaque;
	OVUndoRecPtr cached;

	metabuf = ReadBuffer(rel, OV_META_BLK);
	LockBuffer(metabuf, BUFFER_LOCK_SHARE);
	metapage = BufferGetPage(metabuf);
	metaopaque = (OVMetaPageOpaque *)
		PageGetSpecialPointer(metapage);
	cached = metaopaque->ov_undo_oldestptr;
	UnlockReleaseBuffer(metabuf);

	return cached;
}

/*
 * Heuristic: decide whether we should attempt the expensive UNDO trim.
 *
 * We trim when any of these conditions hold:
 *   1. The UNDO log has grown by at least UNDO_TRIM_COUNTER_THRESHOLD
 *      records since the cached oldest pointer.
 *   2. The cached oldest transaction precedes the current RecentXmin,
 *      meaning new transactions have become removable.
 *
 * The caller must supply the cached oldest pointer (obtained via
 * ovundo_read_cached_oldest) and the current tail counter from the metapage.
 *
 * For VACUUM, the caller bypasses this heuristic entirely.
 */
static bool
should_trim_undo(Relation rel, OVUndoRecPtr cached_oldest,
				 GlobalVisState *vistest)
{
	Buffer		metabuf;
	Page		metapage;
	OVMetaPageOpaque *metaopaque;
	uint64		tail_counter;

	/*
	 * If the cached pointer is invalid, there is nothing to trim (or
	 * the UNDO log is empty).
	 */
	if (!IsOVUndoRecPtrValid(&cached_oldest))
		return false;

	/*
	 * Check whether new records have accumulated beyond the threshold.
	 * Read the tail counter from the metapage with a shared lock.
	 */
	metabuf = ReadBuffer(rel, OV_META_BLK);
	LockBuffer(metabuf, BUFFER_LOCK_SHARE);
	metapage = BufferGetPage(metabuf);
	metaopaque = (OVMetaPageOpaque *)
		PageGetSpecialPointer(metapage);
	tail_counter = metaopaque->ov_undo_tail_first_counter;
	UnlockReleaseBuffer(metabuf);

	if (tail_counter - cached_oldest.counter >=
		UNDO_TRIM_COUNTER_THRESHOLD)
		return true;

	/*
	 * Even if fewer records accumulated, trim if the oldest record's
	 * transaction is removable -- that means at least one record has
	 * become removable.
	 */
	{
		OVUndoRec  *oldest_rec;
		Buffer		undobuf;
		bool		removable = false;

		oldest_rec = (OVUndoRec *)
			ovundo_fetch(rel, cached_oldest, &undobuf,
						 BUFFER_LOCK_SHARE, true);
		if (oldest_rec != NULL)
		{
			removable = GlobalVisTestIsRemovableXid(vistest,
													oldest_rec->xid);
		}
		if (BufferIsValid(undobuf))
			UnlockReleaseBuffer(undobuf);

		if (removable)
			return true;
	}

	return false;
}

/*
 * Internal implementation of UNDO log trimming.
 *
 * The caller must already hold the ExclusiveLock on the metapage
 * (OV_META_BLK).  This function scans the UNDO log from oldest to newest,
 * applies undo actions for aborted transactions, and advances the oldest
 * pointer.
 *
 * Returns the oldest valid UNDO pointer after trimming.
 */
static OVUndoRecPtr
ovundo_trim_locked(Relation rel, GlobalVisState *vistest)
{
	Buffer		metabuf;
	Page		metapage;
	OVMetaPageOpaque *metaopaque;
	BlockNumber firstblk;
	BlockNumber lastblk;
	OVUndoRecPtr oldest_undorecptr;
	bool		can_advance_oldestundorecptr;
	char	   *ptr;
	char	   *endptr;
	BlockNumber deleted_undo_pages = 0;

	oldest_undorecptr = InvalidUndoPtr;

	/*
	 * Get the current oldest undo page from the metapage.
	 */
	metabuf = ReadBuffer(rel, OV_META_BLK);
	metapage = BufferGetPage(metabuf);
	LockBuffer(metabuf, BUFFER_LOCK_SHARE);
	metaopaque = (OVMetaPageOpaque *)
		PageGetSpecialPointer(metapage);

	firstblk = metaopaque->ov_undo_head;
	oldest_undorecptr = metaopaque->ov_undo_oldestptr;

	/*
	 * Since only one process can hold the ExclusiveLock at a time, we
	 * don't need to keep the buffer locked while scanning.
	 */
	UnlockReleaseBuffer(metabuf);

	/*
	 * Loop through UNDO records, starting from the oldest page,
	 * until we hit a record that we cannot remove.
	 */
	lastblk = firstblk;
	can_advance_oldestundorecptr = false;
	while (lastblk != InvalidBlockNumber)
	{
		Buffer		buf;
		Page		page;
		OVUndoPageOpaque *opaque;

		CHECK_FOR_INTERRUPTS();

		buf = ReadBuffer(rel, lastblk);
		page = BufferGetPage(buf);
		LockBuffer(buf, BUFFER_LOCK_SHARE);

		opaque = (OVUndoPageOpaque *)
			PageGetSpecialPointer(page);

		if (opaque->ov_page_id != OV_UNDO_PAGE_ID)
			elog(ERROR, "unexpected page id on UNDO page");

		endptr = (char *) page + ((PageHeader) page)->pd_lower;
		ptr = (char *) page + SizeOfPageHeaderData;
		while (ptr < endptr)
		{
			OVUndoRec  *undorec = (OVUndoRec *) ptr;
			bool		did_commit;

			Assert(undorec->undorecptr.blkno == lastblk);

			if (undorec->undorecptr.counter <
				oldest_undorecptr.counter)
			{
				ptr += undorec->size;
				continue;
			}
			oldest_undorecptr = undorec->undorecptr;

			if (!GlobalVisTestIsRemovableXid(vistest, undorec->xid))
			{
				/* Still needed. Bail out. */
				break;
			}

			/*
			 * No one thinks this transaction is in-progress anymore.
			 * If it committed, discard its UNDO record.  If it
			 * aborted, apply the UNDO first.  (For deletions the
			 * logic is inverted.)
			 *
			 * TODO: batch these TID operations for efficiency.
			 */
			did_commit = TransactionIdDidCommit(undorec->xid);

			switch (undorec->type)
			{
				case OVUNDO_TYPE_INSERT:
					if (!did_commit)
					{
						OVUndoRec_Insert *insertrec =
							(OVUndoRec_Insert *) undorec;

						for (ovtid tid = insertrec->firsttid;
							 tid < insertrec->endtid; tid++)
							ovbt_tid_mark_dead(rel, tid,
											   oldest_undorecptr);
					}
					break;
				case OVUNDO_TYPE_DELTA_INSERT:
					{
						OVUndoRec_DeltaInsert *deltarec =
							(OVUndoRec_DeltaInsert *) undorec;

						if (!did_commit)
						{
							for (ovtid tid = deltarec->firsttid;
								 tid < deltarec->endtid; tid++)
								ovbt_tid_mark_dead(rel, tid,
												   oldest_undorecptr);
						}
						else
						{
							/*
							 * TODO: Materialize carried-forward columns.
							 * The predecessor's column values need
							 * to be copied into the new TID's
							 * column B-trees before the predecessor
							 * can be vacuumed away.
							 *
							 * Materialize carried-forward columns from the
							 * predecessor into the new TID's column B-trees.
							 */
							ov_materialize_delta_columns(
								rel,
								deltarec->firsttid,
								deltarec->predecessor_tid,
								deltarec->natts,
								deltarec->changed_cols);
						}
					}
					break;
				case OVUNDO_TYPE_DELETE:
					{
						OVUndoRec_Delete *deleterec =
							(OVUndoRec_Delete *) undorec;

						if (did_commit)
						{
							for (int i = 0;
								 i < deleterec->num_tids; i++)
								ovbt_tid_mark_dead(
									rel,
									deleterec->tids[i],
									oldest_undorecptr);
						}
						else
						{
							for (int i = 0;
								 i < deleterec->num_tids; i++)
								ovbt_tid_undo_deletion(
									rel,
									deleterec->tids[i],
									undorec->undorecptr,
									oldest_undorecptr);
						}
					}
					break;
				case OVUNDO_TYPE_UPDATE:
					if (did_commit)
					{
						OVUndoRec_Update *updaterec =
							(OVUndoRec_Update *) undorec;

						ovbt_tid_mark_dead(rel,
										   updaterec->oldtid,
										   oldest_undorecptr);
					}
					break;
			}

			ptr += undorec->size;
			can_advance_oldestundorecptr = true;
		}

		if (ptr < endptr)
		{
			UnlockReleaseBuffer(buf);
			break;
		}
		else
		{
			Assert(ptr == endptr);
			lastblk = opaque->next;
			UnlockReleaseBuffer(buf);
			if (lastblk != InvalidBlockNumber)
				deleted_undo_pages++;
		}
	}

	if (can_advance_oldestundorecptr)
	{
		if (lastblk == InvalidBlockNumber)
		{
			oldest_undorecptr.counter++;
			oldest_undorecptr.blkno = InvalidBlockNumber;
			oldest_undorecptr.offset = 0;
		}

		ovundo_discard(rel, oldest_undorecptr);
	}

	return oldest_undorecptr;
}

/*
 * Scan the UNDO log, starting from oldest entry. Undo the effects of any
 * aborted transactions. Records for committed transactions can be discarded
 * away immediately.
 *
 * This is the blocking variant used by VACUUM, which must guarantee that
 * the UNDO log is fully trimmed.
 *
 * Returns the oldest valid UNDO ptr, after discarding.
 */
static OVUndoRecPtr
ovundo_trim(Relation rel, GlobalVisState *vistest)
{
	OVUndoRecPtr result;

	/*
	 * Acquire the exclusive page lock to serialize trimmers.  VACUUM
	 * always blocks here because it must guarantee forward progress.
	 */
	LockPage(rel, OV_META_BLK, ExclusiveLock);

	result = ovundo_trim_locked(rel, vistest);

	UnlockPage(rel, OV_META_BLK, ExclusiveLock);

	return result;
}

void
ovundo_finish_pending_op(ov_pending_undo_op * pendingop, char *payload)
{
	/*
	 * This should be used as part of a bigger critical section that writes a
	 * WAL record of the change.
	 */
	Assert(CritSectionCount > 0);

	memcpy(pendingop->reservation.ptr, payload, pendingop->reservation.length);

	if (!pendingop->is_update)
		ovundo_insert_finish(&pendingop->reservation);
	else
		MarkBufferDirty(pendingop->reservation.undobuf);
}


void
ovundo_clear_speculative_token(Relation rel, OVUndoRecPtr undoptr)
{
	OVUndoRec_Insert *undorec;
	Buffer		buf;

	undorec = (OVUndoRec_Insert *) ovundo_fetch(rel, undoptr, &buf, BUFFER_LOCK_EXCLUSIVE, false);

	if (undorec->rec.type != OVUNDO_TYPE_INSERT)
		elog(ERROR, "unexpected undo record type %d on speculatively inserted row",
			 undorec->rec.type);

	START_CRIT_SECTION();

	MarkBufferDirty(buf);

	undorec->speculative_token = INVALID_SPECULATIVE_TOKEN;

	/*
	 * The speculative insertion token becomes irrelevant, if we crash, so no
	 * need to WAL-log it. However, if checksums are enabled, we may need to
	 * take a full-page image of the page, if a checkpoint happened between
	 * the speculative insertion and this call.
	 */
	if (RelationNeedsWAL(rel))
	{
		if (XLogHintBitIsNeeded())
		{
			XLogRecPtr	lsn;

			lsn = XLogSaveBufferForHint(buf, true);
			PageSetLSN(BufferGetPage(buf), lsn);
		}
	}

	END_CRIT_SECTION();

	UnlockReleaseBuffer(buf);
}

/*
 * Support functions for WAL-logging the insertion/modification of an
 * UNDO record, as part of another WAL-logged change.
 */
void
XLogRegisterUndoOp(uint8 block_id, ov_pending_undo_op * undo_op)
{
	ov_wal_undo_op xlrec;

	xlrec.undoptr = undo_op->reservation.undorecptr;
	xlrec.length = undo_op->reservation.length;
	xlrec.is_update = undo_op->is_update;

	XLogRegisterBuffer(block_id, undo_op->reservation.undobuf,
					   REGBUF_STANDARD);
	XLogRegisterBufData(block_id, (char *) &xlrec, SizeOfOVWalUndoOp);
	XLogRegisterBufData(block_id, (char *) undo_op->payload, undo_op->reservation.length);
}

/* redo support for the above */
Buffer
XLogRedoUndoOp(XLogReaderState *record, uint8 block_id)
{
	Buffer		buffer;
	ov_pending_undo_op op;

	if (XLogReadBufferForRedo(record, block_id, &buffer) == BLK_NEEDS_REDO)
	{
		ov_wal_undo_op xlrec;
		Size		len;
		char	   *p = XLogRecGetBlockData(record, block_id, &len);

		Assert(len >= SizeOfOVWalUndoOp);

		memcpy(&xlrec, p, SizeOfOVWalUndoOp);
		p += SizeOfOVWalUndoOp;
		len -= SizeOfOVWalUndoOp;
		Assert(xlrec.length == len);

		op.reservation.undobuf = buffer;
		op.reservation.undorecptr = xlrec.undoptr;
		op.reservation.length = xlrec.length;
		op.reservation.ptr = ((char *) BufferGetPage(buffer)) + xlrec.undoptr.offset;

		START_CRIT_SECTION();
		ovundo_finish_pending_op(&op, p);
		END_CRIT_SECTION();
	}
	return buffer;
}



static bool
ov_lazy_tid_reaped(ItemPointer itemptr, void *state)
{
	OVVacRelStats *vacrelstats = (OVVacRelStats *) state;
	ovtid		tid = OVTidFromItemPointer(*itemptr);

	return intset_is_member(vacrelstats->dead_tids, tid);
}

/*
 * Entry point of VACUUM for orvos tables.
 *
 * Vacuum on a orvos table works quite differently from the heap. We don't
 * scan the table. Instead, we scan just the active UNDO log, and remove any
 * garbage left behind by aborts or deletions based on the UNDO log.
 */
void
ovundo_vacuum(Relation rel, VacuumParams *params, BufferAccessStrategy bstrategy)
{
	OVVacRelStats *vacrelstats;
	Relation   *Irel;
	int			nindexes;
	IndexBulkDeleteResult **indstats;
	ovtid		starttid;
	ovtid		endtid;
	uint64		num_live_tuples;
	uint64		num_dead_tuples;
	TimestampTz starttime;
	GlobalVisState *vistest;
	TransactionId OldestXmin;

	/* do nothing if the table is completely empty. */
	if (RelationGetTargetBlock(rel) == 0 ||
		RelationGetTargetBlock(rel) == InvalidBlockNumber)
	{
		/* don't believe the cached value without checking */
		BlockNumber nblocks = RelationGetNumberOfBlocks(rel);

		RelationSetTargetBlock(rel, nblocks);
		if (nblocks == 0)
			return;
	}

	starttime = GetCurrentTimestamp();

	/*
	 * Use GlobalVisState for accurate visibility testing during VACUUM.
	 * This replaces the old InvalidTransactionId/RecentXmin approach with
	 * the proper per-relation visibility horizon.
	 */
	vistest = GlobalVisTestFor(rel);

	/*
	 * Scan the UNDO log, and discard what we can.
	 */
	(void) ovundo_trim(rel, vistest);

	vacrelstats = (OVVacRelStats *) palloc0(sizeof(OVVacRelStats));
	vacrelstats->rel = rel;

	if (params->options & VACOPT_VERBOSE)
		vacrelstats->elevel = INFO;
	else
		vacrelstats->elevel = DEBUG2;
	vacrelstats->vac_strategy = bstrategy;

	/* Open all indexes of the relation */
	vac_open_indexes(rel, RowExclusiveLock, &nindexes, &Irel);
	vacrelstats->hasindex = (nindexes > 0);
	indstats = (IndexBulkDeleteResult **)
		palloc0(nindexes * sizeof(IndexBulkDeleteResult *));

	ereport(vacrelstats->elevel,
			(errmsg("vacuuming \"%s.%s\"",
					get_namespace_name(RelationGetNamespace(rel)),
					RelationGetRelationName(rel))));

	starttid = MinOVTid;
	num_live_tuples = 0;
	num_dead_tuples = 0;
	do
	{
		IntegerSet *dead_tids;

		/* Scan the TID tree, to collect TIDs that have been marked dead. */
		dead_tids = ovbt_collect_dead_tids(rel, starttid, &endtid, &num_live_tuples);
		num_dead_tuples += intset_num_entries(dead_tids);
		vacrelstats->dead_tids = dead_tids;

		if (intset_num_entries(dead_tids) > 0)
		{
			/* Remove index entries */
			for (int i = 0; i < nindexes; i++)
				lazy_vacuum_index(Irel[i],
								  &indstats[i],
								  vacrelstats,
								  rel);

			/*
			 * Remove the attribute data for the dead rows, and finally their
			 * TID tree entries.
			 */
			for (int attno = 1; attno <= RelationGetNumberOfAttributes(rel); attno++)
				ovbt_attr_remove(rel, attno, dead_tids);
			ovbt_tid_remove(rel, dead_tids);
		}

		ereport(vacrelstats->elevel,
				(errmsg("\"%s\": removed " UINT64_FORMAT " row versions",
						RelationGetRelationName(rel),
						intset_num_entries(dead_tids))));

		starttid = endtid;
	} while (starttid < MaxPlusOneOVTid);

	/* Do post-vacuum cleanup and statistics update for each index */
	for (int i = 0; i < nindexes; i++)
		lazy_cleanup_index(Irel[i], indstats[i], vacrelstats);

	/* Done with indexes */
	vac_close_indexes(nindexes, Irel, NoLock);

	/*
	 * Note: ovbt_collect_dead_tids counts all tuples (live + dead) in
	 * num_live_tuples. Subtract dead tuples that were just removed to get the
	 * actual live tuple count.
	 */
	if (num_live_tuples > num_dead_tuples)
		num_live_tuples -= num_dead_tuples;
	else
		num_live_tuples = 0;

	/*
	 * Update pg_class to reflect new info we know. Using OldestXmin as new
	 * frozenxid. Since we don't know the new multixid, pass it as invalid to
	 * avoid update. We don't track all-visible or all-frozen pages in the
	 * columnar store, so pass 0 for those.
	 */
	OldestXmin = GetOldestNonRemovableTransactionId(rel);
	vac_update_relstats(rel,
						RelationGetNumberOfBlocks(rel),
						num_live_tuples,
						0,		/* num_all_visible_pages */
						0,		/* num_all_frozen_pages */
						nindexes > 0,
						OldestXmin,
						InvalidMultiXactId,
						NULL,	/* frozenxid_updated */
						NULL,	/* minmulti_updated */
						false); /* in_outer_xact */

	/* report results to the cumulative stats system */
	pgstat_report_vacuum(rel,
						 num_live_tuples,
						 num_dead_tuples,
						 starttime);
}

/*
 *	lazy_vacuum_index() -- vacuum one index relation.
 *
 *		Delete all the index entries pointing to tuples listed in
 *		vacrelstats->dead_tuples, and update running statistics.
 */
static void
lazy_vacuum_index(Relation indrel,
				  IndexBulkDeleteResult **stats,
				  OVVacRelStats * vacrelstats,
				  Relation heaprel)
{
	IndexVacuumInfo ivinfo;
	PGRUsage	ru0;

	pg_rusage_init(&ru0);

	ivinfo.index = indrel;
	ivinfo.heaprel = heaprel;		/* CRITICAL: btree vacuum needs this */
	ivinfo.analyze_only = false;
	ivinfo.estimated_count = true;
	ivinfo.message_level = vacrelstats->elevel;
	/* We can only provide an approximate value of num_heap_tuples here */
	ivinfo.num_heap_tuples = vacrelstats->old_live_tuples;
	ivinfo.strategy = vacrelstats->vac_strategy;

	/* Do bulk deletion */
	*stats = index_bulk_delete(&ivinfo, *stats,
							   ov_lazy_tid_reaped, (void *) vacrelstats);

	ereport(vacrelstats->elevel,
			(errmsg("scanned index \"%s\" to remove " UINT64_FORMAT " row versions",
					RelationGetRelationName(indrel),
					intset_num_entries(vacrelstats->dead_tids)),
			 errdetail_internal("%s", pg_rusage_show(&ru0))));
}

/*
 *	lazy_cleanup_index() -- do post-vacuum cleanup for one index relation.
 */
static void
lazy_cleanup_index(Relation indrel,
				   IndexBulkDeleteResult *stats,
				   OVVacRelStats * vacrelstats)
{
	IndexVacuumInfo ivinfo;
	PGRUsage	ru0;

	pg_rusage_init(&ru0);

	ivinfo.index = indrel;
	ivinfo.heaprel = vacrelstats->rel;
	ivinfo.analyze_only = false;
	ivinfo.estimated_count = (vacrelstats->tupcount_pages < vacrelstats->rel_pages);
	ivinfo.message_level = vacrelstats->elevel;

	/*
	 * Now we can provide a better estimate of total number of surviving
	 * tuples (we assume indexes are more interested in that than in the
	 * number of nominally live tuples).
	 */
	ivinfo.num_heap_tuples = vacrelstats->new_rel_tuples;
	ivinfo.strategy = vacrelstats->vac_strategy;

	stats = index_vacuum_cleanup(&ivinfo, stats);

	if (!stats)
		return;

	/*
	 * Now update statistics in pg_class, but only if the index says the count
	 * is accurate.
	 */
	if (!stats->estimated_count)
		vac_update_relstats(indrel,
							stats->num_pages,
							stats->num_index_tuples,
							0,	/* num_all_visible_pages */
							0,	/* num_all_frozen_pages */
							false,	/* hasindex */
							InvalidTransactionId,
							InvalidMultiXactId,
							NULL,	/* frozenxid_updated */
							NULL,	/* minmulti_updated */
							false); /* in_outer_xact */

	ereport(vacrelstats->elevel,
			(errmsg("index \"%s\" now contains %.0f row versions in %u pages",
					RelationGetRelationName(indrel),
					stats->num_index_tuples,
					stats->num_pages),
			 errdetail("%.0f index row versions were removed.\n"
					   "%u index pages have been deleted, %u are currently reusable.\n"
					   "%s.",
					   stats->tuples_removed,
					   stats->pages_deleted, stats->pages_free,
					   pg_rusage_show(&ru0))));

	pfree(stats);
}


/*
 * Return the current "Oldest undo pointer". The effects of any actions with
 * undo pointer older than this is known to be visible to everyone. (i.e.
 * an inserted tuple is known to be visible, and a deleted tuple is known to
 * be invisible.)
 *
 * This uses a two-level approach to minimize metapage lock contention:
 *
 *   1. Fast path: read the cached oldest_undo_ptr from the metapage
 *      with a shared buffer lock only (no heavyweight page lock).
 *
 *   2. Heuristic check: only attempt the expensive UNDO trim if the
 *      heuristic indicates meaningful work is available.
 *
 *   3. Non-blocking lock: use ConditionalLockPage to avoid blocking
 *      if another backend is already trimming.  If the lock cannot
 *      be acquired, return the cached value -- it is always safe to
 *      use a slightly stale oldest pointer (it only means some UNDO
 *      records survive a little longer than strictly necessary).
 */
OVUndoRecPtr
ovundo_get_oldest_undo_ptr(Relation rel)
{
	OVUndoRecPtr cached_oldest;
	OVUndoRecPtr result;
	GlobalVisState *vistest;

	/* do nothing if the table is completely empty. */
	if (RelationGetTargetBlock(rel) == 0 ||
		RelationGetTargetBlock(rel) == InvalidBlockNumber)
	{
		BlockNumber nblocks;

		nblocks = RelationGetNumberOfBlocks(rel);
		RelationSetTargetBlock(rel, nblocks);
		if (nblocks == 0)
			return InvalidUndoPtr;
	}

	vistest = GlobalVisTestFor(rel);

	/*
	 * Fast path: read the cached value with shared lock only.
	 */
	cached_oldest = ovundo_read_cached_oldest(rel);

	/*
	 * Check the heuristic.  If there is not enough accumulated UNDO
	 * work, skip the expensive trim entirely.
	 */
	if (!should_trim_undo(rel, cached_oldest, vistest))
		return cached_oldest;

	/*
	 * Try to acquire the exclusive page lock for trimming.  If another
	 * backend is already trimming, just return the cached value rather
	 * than blocking.
	 */
	if (!ConditionalLockPage(rel, OV_META_BLK, ExclusiveLock))
		return cached_oldest;

	result = ovundo_trim_locked(rel, vistest);

	UnlockPage(rel, OV_META_BLK, ExclusiveLock);

	return result;
}
