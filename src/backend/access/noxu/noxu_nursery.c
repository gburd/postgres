/**
 * @file noxu_nursery.c
 * @brief In-memory nursery buffer for batching Noxu attribute insertions.
 *
 * When PostgreSQL's executor calls table_tuple_insert() per row (e.g.,
 * during INSERT...SELECT), each call would create 1-element attribute
 * items that cannot trigger type-specific compression codecs.  The
 * nursery buffers per-row attribute data and flushes it in bulk to the
 * attribute B-trees, allowing nxbt_attr_create_items() to evaluate the
 * full compression cascade (FOR, DOD, Chimp, Dict, UUID v7, etc.) on
 * large batches.
 *
 * TID tree entries are created immediately on each insert (one per row)
 * to provide valid TIDs for index insertions.  Only attribute data is
 * deferred.
 *
 * The nursery is per-backend, per-relation, and per-transaction.  It is
 * stored in a MemoryContext under TopTransactionContext and automatically
 * cleaned up on transaction abort.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/backend/access/noxu/noxu_nursery.c
 */
#include "postgres.h"

#include "access/detoast.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/noxu_internal.h"
#include "access/noxu_lsm.h"
#include "access/noxu_nursery.h"
#include "access/table.h"
#include "access/xact.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

/* GUC variables */
bool		noxu_nursery_enabled = true;
int			noxu_nursery_size = 2048;
int			noxu_nursery_mem_limit_kb = 8192;
bool		noxu_nursery_flush_on_scan = true;

/*
 * Hash table mapping RelFileLocator -> NXNurseryBuffer, keyed by
 * (spcOid, dbOid, relNumber).  Lives in TopTransactionContext.
 */
static HTAB *nursery_htab = NULL;

/* Have we registered the transaction callback? */
static bool xact_callback_registered = false;

/* Hash table entry */
typedef struct NXNurseryHashEntry
{
	RelFileLocator key;
	NXNurseryBuffer *nursery;
} NXNurseryHashEntry;

/* Forward declarations */
static void nx_nursery_xact_callback(XactEvent event, void *arg);
static void nx_nursery_subxact_callback(SubXactEvent event,
										SubTransactionId mySubid,
										SubTransactionId parentSubid,
										void *arg);
static void nx_nursery_destroy_all(void);
static void nx_nursery_reset(NXNurseryBuffer *nursery);

/*
 * nx_nursery_ensure_htab - Create the hash table if it doesn't exist.
 */
static void
nx_nursery_ensure_htab(void)
{
	HASHCTL		hashctl;

	if (nursery_htab != NULL)
		return;

	/* Register transaction callbacks on first use */
	if (!xact_callback_registered)
	{
		RegisterXactCallback(nx_nursery_xact_callback, NULL);
		RegisterSubXactCallback(nx_nursery_subxact_callback, NULL);
		xact_callback_registered = true;
	}

	memset(&hashctl, 0, sizeof(hashctl));
	hashctl.keysize = sizeof(RelFileLocator);
	hashctl.entrysize = sizeof(NXNurseryHashEntry);
	hashctl.hcxt = TopTransactionContext;

	nursery_htab = hash_create("NoxuNurseryHash",
							   16,		/* initial size */
							   &hashctl,
							   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/*
 * nx_nursery_get_or_create - Get or create a nursery buffer for a relation.
 *
 * The NXNurseryBuffer struct is allocated in TopTransactionContext (so it
 * survives nursery resets).  Per-row datum data is allocated in the
 * nursery's private MemoryContext (nursery->mcxt) which can be reset.
 */
NXNurseryBuffer *
nx_nursery_get_or_create(Relation rel)
{
	NXNurseryHashEntry *entry;
	NXNurseryBuffer *nursery;
	bool		found;
	MemoryContext oldcontext;

	nx_nursery_ensure_htab();

	entry = (NXNurseryHashEntry *)
		hash_search(nursery_htab, &rel->rd_locator, HASH_ENTER, &found);

	if (found)
		return entry->nursery;

	/*
	 * Allocate the NXNurseryBuffer struct in TopTransactionContext so
	 * it persists across nursery resets (MemoryContextReset on mcxt).
	 * Only per-row datum copies go into nursery->mcxt.
	 */
	oldcontext = MemoryContextSwitchTo(TopTransactionContext);

	nursery = (NXNurseryBuffer *) palloc0(sizeof(NXNurseryBuffer));
	nursery->rlocator = rel->rd_locator;
	nursery->relid = RelationGetRelid(rel);
	nursery->nattrs = rel->rd_att->natts;
	nursery->capacity = noxu_nursery_size;
	nursery->nrows = 0;
	nursery->mem_bytes = 0;
	nursery->xid = GetCurrentTransactionId();

	nursery->mcxt = AllocSetContextCreate(TopTransactionContext,
										  "NoxuNurseryData",
										  ALLOCSET_DEFAULT_SIZES);

	/* Rows array also in TopTransactionContext (survives resets) */
	nursery->rows = (NXNurseryRow *)
		palloc0(nursery->capacity * sizeof(NXNurseryRow));

	entry->nursery = nursery;

	MemoryContextSwitchTo(oldcontext);
	return nursery;
}

/*
 * nx_nursery_get - Get the nursery for a relation, or NULL if none exists.
 */
NXNurseryBuffer *
nx_nursery_get(Relation rel)
{
	NXNurseryHashEntry *entry;

	if (nursery_htab == NULL)
		return NULL;

	entry = (NXNurseryHashEntry *)
		hash_search(nursery_htab, &rel->rd_locator, HASH_FIND, NULL);

	if (entry == NULL)
		return NULL;

	return entry->nursery;
}

/*
 * nx_nursery_buffer_row - Buffer a row's attribute data in the nursery.
 *
 * The TID has already been assigned via nxbt_tid_multi_insert().
 * We copy the datum values into the nursery's data memory context.
 */
void
nx_nursery_buffer_row(NXNurseryBuffer *nursery,
					  TupleTableSlot *slot,
					  nxtid tid,
					  CommandId cid)
{
	NXNurseryRow *row;
	MemoryContext oldcontext;
	int			nattrs = nursery->nattrs;

	Assert(nursery->nrows < nursery->capacity);

	/* Per-row datum copies go into the nursery data context */
	oldcontext = MemoryContextSwitchTo(nursery->mcxt);

	row = &nursery->rows[nursery->nrows];
	row->tid = tid;
	row->cid = cid;
	row->subxid = GetCurrentSubTransactionId();
	row->datums = (Datum *) palloc(nattrs * sizeof(Datum));
	row->isnulls = (bool *) palloc(nattrs * sizeof(bool));

	/*
	 * Copy datum values.  We must copy pass-by-reference datums because the
	 * slot's memory context may be short-lived (e.g., per-tuple context in
	 * the executor).
	 */
	for (int i = 0; i < nattrs; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(slot->tts_tupleDescriptor, i);
		Datum		datum = slot->tts_values[i];
		bool		isnull = slot->tts_isnull[i];

		if (isnull)
		{
			row->datums[i] = (Datum) 0;
			row->isnulls[i] = true;
		}
		else if (attr->attbyval)
		{
			row->datums[i] = datum;
			row->isnulls[i] = false;
		}
		else
		{
			row->datums[i] = datumCopy(datum, attr->attbyval, attr->attlen);
			row->isnulls[i] = false;

			/* Track memory usage approximately */
			nursery->mem_bytes += datumGetSize(datum, attr->attbyval,
											   attr->attlen);
		}
	}

	nursery->nrows++;

	MemoryContextSwitchTo(oldcontext);
}

/*
 * nx_nursery_flush_to_btree - Direct flush of attribute data to B-trees.
 *
 * The Phase 1 flush path: decomposes buffered rows into per-column arrays
 * and calls nxbt_attr_multi_insert() for each column with the full batch.
 */
static void
nx_nursery_flush_to_btree(Relation rel, NXNurseryBuffer *nursery)
{
	int			nrows = nursery->nrows;
	int			nattrs = nursery->nattrs;
	nxtid	   *tids;
	Datum	   *col_datums;
	bool	   *col_isnulls;
	MemoryContext flush_mcxt;
	MemoryContext oldcontext;

	flush_mcxt = AllocSetContextCreate(CurrentMemoryContext,
									   "NoxuNurseryFlush",
									   ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(flush_mcxt);

	/* Build TID array */
	tids = (nxtid *) palloc(nrows * sizeof(nxtid));
	for (int i = 0; i < nrows; i++)
		tids[i] = nursery->rows[i].tid;

	/* Allocate per-column temporary arrays */
	col_datums = (Datum *) palloc(nrows * sizeof(Datum));
	col_isnulls = (bool *) palloc(nrows * sizeof(bool));

	/*
	 * Flush each column.  This is where the magic happens: instead of
	 * inserting 1-element items, we insert nrows-element items that
	 * trigger type-specific compression codecs.
	 */
	for (AttrNumber attno = 1; attno <= nattrs; attno++)
	{
		Form_pg_attribute attr = TupleDescAttr(rel->rd_att, attno - 1);

		/* Gather this column's values from all buffered rows */
		for (int i = 0; i < nrows; i++)
		{
			Datum		datum = nursery->rows[i].datums[attno - 1];
			bool		isnull = nursery->rows[i].isnulls[attno - 1];

			if (!isnull && attr->attlen < 0 &&
				VARATT_IS_EXTERNAL((struct varlena *) DatumGetPointer(datum)))
			{
				datum = PointerGetDatum(
					detoast_external_attr(
						(struct varlena *) DatumGetPointer(datum)));
			}

			if (!isnull && attr->attlen < 0 &&
				VARSIZE_ANY_EXHDR((struct varlena *) DatumGetPointer(datum)) > MaxNoxuDatumSize)
			{
				datum = noxu_overflow_datum(rel, attno, datum,
											nursery->rows[i].tid);
			}

			col_datums[i] = datum;
			col_isnulls[i] = isnull;
		}

		nxbt_attr_multi_insert(rel, attno, col_datums, col_isnulls,
							   tids, nrows);
	}

	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(flush_mcxt);
}

/*
 * nx_nursery_flush_to_lsm - Flush rows to LSM Level 1 row-oriented pages.
 *
 * Builds MinimalTuples from the buffered rows and writes them to Level 1
 * row pages.  The segment is then assigned to the A or B slot at Level 1.
 * If both slots are occupied (merge in progress), falls back to the
 * direct B-tree path.
 */
static void pg_attribute_unused()
nx_nursery_flush_to_lsm(Relation rel, NXNurseryBuffer *nursery)
{
	int			nrows = nursery->nrows;
	MinimalTuple *tuples;
	nxtid	   *tids;
	NXLSMSegmentDesc new_seg;
	BlockNumber first_blk,
				last_blk;
	int			npages;
	MemoryContext flush_mcxt;
	MemoryContext oldcontext;

	flush_mcxt = AllocSetContextCreate(CurrentMemoryContext,
									   "NoxuNurseryLSMFlush",
									   ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(flush_mcxt);

	/* Build TID array and MinimalTuple array */
	tids = (nxtid *) palloc(nrows * sizeof(nxtid));
	tuples = (MinimalTuple *) palloc(nrows * sizeof(MinimalTuple));

	for (int i = 0; i < nrows; i++)
	{
		HeapTuple	htup;
		NXNurseryRow *row = &nursery->rows[i];

		tids[i] = row->tid;

		/* Build a HeapTuple from the buffered datum/isnull arrays */
		htup = heap_form_tuple(rel->rd_att, row->datums, row->isnulls);
		tuples[i] = minimal_tuple_from_heap_tuple(htup, 0);
		heap_freetuple(htup);
	}

	/* Write to Level 1 row pages */
	nx_lsm_write_row_pages(rel, tuples, tids, nrows, 1, NX_LSM_SEG_NONE,
						   &first_blk, &last_blk, &npages);

	/* Build segment descriptor */
	memset(&new_seg, 0, sizeof(NXLSMSegmentDesc));
	new_seg.first_block = first_blk;
	new_seg.last_block = last_blk;
	new_seg.first_tid = tids[0];
	new_seg.last_tid = tids[nrows - 1];
	new_seg.nrows = nrows;
	new_seg.npages = npages;
	new_seg.is_columnar = false;

	/* Try to assign to Level 1 */
	if (!nx_lsm_assign_to_level(rel, 1, &new_seg))
	{
		/*
		 * Both A and B slots occupied, merge in progress.
		 * Free the row pages we just wrote and fall back to direct B-tree.
		 */
		nx_lsm_free_segment_pages(rel, &new_seg);

		MemoryContextSwitchTo(oldcontext);
		MemoryContextDelete(flush_mcxt);

		nx_nursery_flush_to_btree(rel, nursery);
		return;
	}

	/* Free the MinimalTuples */
	for (int i = 0; i < nrows; i++)
		pfree(tuples[i]);

	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(flush_mcxt);
}

/*
 * nx_nursery_flush - Flush all buffered attribute data.
 *
 * Always flushes to the B-tree to ensure data is visible to scans.
 * The LSM Level 1 path (nx_nursery_flush_to_lsm) is available as
 * infrastructure for Phase 3's background merge worker, which will
 * use it for capacity-triggered flushes while the merge worker moves
 * data from Level 1 into the B-tree asynchronously.
 */
void
nx_nursery_flush(Relation rel, NXNurseryBuffer *nursery)
{
	if (nursery->nrows == 0)
		return;

	nx_nursery_flush_to_btree(rel, nursery);

	/* Reset the nursery for reuse */
	nx_nursery_reset(nursery);
}

/*
 * nx_nursery_reset - Reset a nursery buffer for reuse after flush.
 *
 * Resets the data memory context (freeing all per-row datum copies)
 * and clears the row counters.  The rows array and NXNurseryBuffer
 * struct survive because they live in TopTransactionContext.
 */
static void
nx_nursery_reset(NXNurseryBuffer *nursery)
{
	/* Free all per-row datum copies in one operation */
	MemoryContextReset(nursery->mcxt);

	/* Clear the row entries (datums/isnulls pointers are now invalid) */
	memset(nursery->rows, 0, nursery->capacity * sizeof(NXNurseryRow));

	nursery->nrows = 0;
	nursery->mem_bytes = 0;
}

/*
 * nx_nursery_scan_next - Return the next nursery row in TID order within range.
 *
 * Returns true if a row was found, false if the nursery is exhausted.
 * *scan_idx tracks the current position in the nursery.
 *
 * The caller should not hold any buffer locks when calling this, as we
 * may allocate memory for datum copies.
 */
bool
nx_nursery_scan_next(NXNurseryBuffer *nursery,
					 int *scan_idx,
					 nxtid range_start,
					 nxtid range_end,
					 TupleTableSlot *slot,
					 Relation rel pg_attribute_unused())
{
	int			nattrs;

	if (nursery == NULL || nursery->nrows == 0)
		return false;

	nattrs = nursery->nattrs;

	while (*scan_idx < nursery->nrows)
	{
		NXNurseryRow *row = &nursery->rows[*scan_idx];

		(*scan_idx)++;

		/* Check TID range */
		if (row->tid < range_start || row->tid >= range_end)
			continue;

		/* Populate the slot */
		ExecClearTuple(slot);
		for (int i = 0; i < nattrs && i < slot->tts_tupleDescriptor->natts; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(slot->tts_tupleDescriptor, i);

			if (row->isnulls[i])
			{
				slot->tts_values[i] = (Datum) 0;
				slot->tts_isnull[i] = true;
			}
			else
			{
				/* Copy non-byval datums to the slot's context */
				if (!attr->attbyval)
					slot->tts_values[i] = datumCopy(row->datums[i],
													attr->attbyval,
													attr->attlen);
				else
					slot->tts_values[i] = row->datums[i];
				slot->tts_isnull[i] = false;
			}
		}

		slot->tts_tableOid = nursery->relid;
		slot->tts_tid = ItemPointerFromNXTid(row->tid);
		ExecStoreVirtualTuple(slot);

		return true;
	}

	return false;
}

/*
 * nx_nursery_lookup_tid - Look up a specific TID in the nursery.
 *
 * Used by index fetch to check the nursery before descending the B-tree.
 * Returns true if found.
 */
bool
nx_nursery_lookup_tid(NXNurseryBuffer *nursery,
					  nxtid tid,
					  TupleTableSlot *slot,
					  Relation rel pg_attribute_unused())
{
	int			nattrs;

	if (nursery == NULL || nursery->nrows == 0)
		return false;

	nattrs = nursery->nattrs;

	/*
	 * Linear scan.  The nursery is small (default 2048 rows) and this is
	 * only called for index lookups, which are infrequent during the
	 * buffering window.
	 */
	for (int i = 0; i < nursery->nrows; i++)
	{
		NXNurseryRow *row = &nursery->rows[i];

		if (row->tid != tid)
			continue;

		/* Found it.  Populate the slot. */
		ExecClearTuple(slot);
		for (int j = 0; j < nattrs && j < slot->tts_tupleDescriptor->natts; j++)
		{
			Form_pg_attribute attr = TupleDescAttr(slot->tts_tupleDescriptor, j);

			if (row->isnulls[j])
			{
				slot->tts_values[j] = (Datum) 0;
				slot->tts_isnull[j] = true;
			}
			else
			{
				if (!attr->attbyval)
					slot->tts_values[j] = datumCopy(row->datums[j],
													attr->attbyval,
													attr->attlen);
				else
					slot->tts_values[j] = row->datums[j];
				slot->tts_isnull[j] = false;
			}
		}

		slot->tts_tableOid = nursery->relid;
		slot->tts_tid = ItemPointerFromNXTid(row->tid);
		ExecStoreVirtualTuple(slot);

		return true;
	}

	return false;
}

/*
 * nx_nursery_flush_all - Flush all active nurseries.
 *
 * Called at transaction commit to ensure all buffered attribute data
 * is written to the B-trees before the transaction becomes visible.
 */
void
nx_nursery_flush_all(void)
{
	HASH_SEQ_STATUS status;
	NXNurseryHashEntry *entry;

	if (nursery_htab == NULL)
		return;

	hash_seq_init(&status, nursery_htab);
	while ((entry = (NXNurseryHashEntry *) hash_seq_search(&status)) != NULL)
	{
		NXNurseryBuffer *nursery = entry->nursery;

		if (nursery->nrows > 0)
		{
			Relation	rel;

			rel = table_open(nursery->relid, RowExclusiveLock);
			nx_nursery_flush(rel, nursery);
			table_close(rel, RowExclusiveLock);
		}
	}
}

/*
 * nx_nursery_destroy_all - Destroy all nurseries (on abort or post-commit).
 *
 * The nursery MemoryContexts and the hash table are children of
 * TopTransactionContext, so they will be cleaned up automatically.
 * We just need to clear our static pointer.
 */
static void
nx_nursery_destroy_all(void)
{
	nursery_htab = NULL;
}

/*
 * nx_nursery_xact_callback - Transaction callback for nursery management.
 */
static void
nx_nursery_xact_callback(XactEvent event, void *arg pg_attribute_unused())
{
	switch (event)
	{
		case XACT_EVENT_PRE_COMMIT:
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
		case XACT_EVENT_PRE_PREPARE:
			/*
			 * Flush all nurseries before commit.  After this, all attribute
			 * data is in the B-trees and WAL-logged.
			 */
			nx_nursery_flush_all();
			break;

		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			/*
			 * On abort, discard all nurseries.  TID tree entries created
			 * for buffered rows will be rolled back by the UNDO subsystem.
			 * The nursery MemoryContexts (children of TopTransactionContext)
			 * are freed automatically by the transaction cleanup.
			 */
			nx_nursery_destroy_all();
			break;

		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_PREPARE:
			/*
			 * Post-commit cleanup.  Nurseries were already flushed in
			 * PRE_COMMIT; now just drop the hash table reference.
			 */
			nx_nursery_destroy_all();
			break;

		default:
			break;
	}
}

/*
 * nx_nursery_subxact_callback - Subtransaction callback.
 *
 * On subtransaction abort, remove nursery entries belonging to the
 * aborted subtransaction.  The TID tree entries will be undone by
 * the UNDO subsystem; we just need to remove the buffered attribute data.
 *
 * Note: we cannot use MemoryContextReset here because non-aborted rows
 * still have datum pointers into nursery->mcxt.  We compact the array
 * in place instead.
 */
static void
nx_nursery_subxact_callback(SubXactEvent event,
							SubTransactionId mySubid,
							SubTransactionId parentSubid pg_attribute_unused(),
							void *arg pg_attribute_unused())
{
	HASH_SEQ_STATUS status;
	NXNurseryHashEntry *entry;

	if (event != SUBXACT_EVENT_ABORT_SUB)
		return;

	if (nursery_htab == NULL)
		return;

	hash_seq_init(&status, nursery_htab);
	while ((entry = (NXNurseryHashEntry *) hash_seq_search(&status)) != NULL)
	{
		NXNurseryBuffer *nursery = entry->nursery;
		int			dst = 0;

		for (int i = 0; i < nursery->nrows; i++)
		{
			if (nursery->rows[i].subxid == mySubid)
			{
				/*
				 * Skip this entry.  We don't pfree the datum arrays because
				 * they live in nursery->mcxt which we cannot selectively
				 * free from.  The memory will be reclaimed when the nursery
				 * is reset or destroyed.
				 */
				continue;
			}

			/* Keep this entry, compacting if needed */
			if (dst != i)
				nursery->rows[dst] = nursery->rows[i];
			dst++;
		}

		nursery->nrows = dst;

		/* Approximate: we don't track per-row memory precisely */
		nursery->mem_bytes = 0;
	}
}

/*
 * nx_nursery_init_gucs - Register nursery-related GUC parameters.
 */
void
nx_nursery_init_gucs(void)
{
	DefineCustomBoolVariable("noxu.nursery_enabled",
							 "Enable the nursery buffer for batching "
							 "attribute insertions.",
							 "When enabled, per-row attribute data is buffered "
							 "in memory and flushed in bulk to the attribute "
							 "B-trees, enabling compression codecs.",
							 &noxu_nursery_enabled,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("noxu.nursery_size",
							"Maximum number of rows to buffer in the nursery "
							"before flushing.",
							NULL,
							&noxu_nursery_size,
							2048,
							1, 65536,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("noxu.nursery_mem_limit_kb",
							"Maximum memory (in KB) for nursery attribute data "
							"before flushing.",
							NULL,
							&noxu_nursery_mem_limit_kb,
							8192,
							64, 1048576,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	DefineCustomBoolVariable("noxu.nursery_flush_on_scan",
							 "Flush the nursery before sequential scans.",
							 "When enabled, a sequential scan will flush any "
							 "pending nursery data to the B-trees first, "
							 "ensuring the scan sees all committed data via "
							 "the normal B-tree path.",
							 &noxu_nursery_flush_on_scan,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);
}
