/*-------------------------------------------------------------------------
 *
 * test_undo_tam.c
 *	  Minimal test table access method using per-relation UNDO for MVCC
 *
 * This module implements a minimal table access method that uses the
 * per-relation UNDO subsystem (RelUndo*) for INSERT operations. It stores
 * tuples in simple heap-like pages and creates UNDO records for each
 * insertion using the two-phase Reserve/Finish protocol.
 *
 * The primary purpose is to validate that the per-relation UNDO infrastructure
 * works correctly end-to-end: UNDO records can be created, read back, and
 * the chain can be walked via the introspection function.
 *
 * Only INSERT and sequential scan are fully implemented. Other operations
 * (DELETE, UPDATE, etc.) raise errors since this is a test-only AM.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/test/modules/test_undo_tam/test_undo_tam.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/amapi.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/relundo.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "access/xactundo.h"
#include "catalog/index.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "commands/vacuum.h"
#include "executor/tuptable.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

/* ----------------------------------------------------------------
 * Private data structures
 * ----------------------------------------------------------------
 */

/*
 * Simple tuple header for our test AM.
 *
 * Each tuple stored on a data page is prefixed with this header.
 * We store tuples as MinimalTuples for simplicity.
 */
typedef struct TestRelundoTupleHeader
{
	uint32		t_len;			/* Total length including this header */
	TransactionId t_xmin;		/* Inserting transaction */
	ItemPointerData t_self;		/* Tuple's own TID */
}			TestRelundoTupleHeader;

#define TESTRELUNDO_TUPLE_HEADER_SIZE	MAXALIGN(sizeof(TestRelundoTupleHeader))

/*
 * Scan descriptor for sequential scans.
 */
typedef struct TestRelundoScanDescData
{
	TableScanDescData rs_base;	/* Must be first */
	BlockNumber rs_nblocks;		/* Total blocks in relation */
	BlockNumber rs_curblock;	/* Current block being scanned */
	OffsetNumber rs_curoffset;	/* Current offset within page (byte offset) */
	Buffer		rs_cbuf;		/* Current buffer */
	bool		rs_inited;		/* Scan initialized? */
}			TestRelundoScanDescData;

typedef TestRelundoScanDescData * TestRelundoScanDesc;


/* ----------------------------------------------------------------
 * Forward declarations
 * ----------------------------------------------------------------
 */
PG_FUNCTION_INFO_V1(test_undo_tam_handler);
PG_FUNCTION_INFO_V1(test_undo_tam_dump_chain);


/* ----------------------------------------------------------------
 * Helper: insert a tuple onto a page
 *
 * Finds a page with space (or extends the relation) and writes the
 * tuple data. Returns the TID of the inserted tuple.
 * ----------------------------------------------------------------
 */
static void
testrelundo_insert_tuple(Relation rel, TupleTableSlot *slot,
						 ItemPointer tid)
{
	MinimalTuple mintuple;
	bool		shouldFree;
	Size		tuple_size;
	Size		needed;
	BlockNumber nblocks;
	BlockNumber blkno;
	Buffer		buf = InvalidBuffer;
	Page		page;
	bool		found_space = false;

	/* Materialize and get the minimal tuple */
	mintuple = ExecFetchSlotMinimalTuple(slot, &shouldFree);
	tuple_size = mintuple->t_len;
	needed = TESTRELUNDO_TUPLE_HEADER_SIZE + MAXALIGN(tuple_size);

	/* Ensure the tuple fits on an empty page */
	if (needed > BLCKSZ - SizeOfPageHeaderData)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("tuple too large for test_undo_tam: %zu bytes", needed)));

	nblocks = RelationGetNumberOfBlocks(rel);

	/* Try to find an existing page with enough space */
	for (blkno = 0; blkno < nblocks; blkno++)
	{
		Size		freespace;

		buf = ReadBuffer(rel, blkno);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

		page = BufferGetPage(buf);
		freespace = PageGetFreeSpace(page);

		if (freespace >= needed)
		{
			found_space = true;
			break;
		}

		UnlockReleaseBuffer(buf);
	}

	/* If no existing page has space, extend the relation */
	if (!found_space)
	{
		buf = ExtendBufferedRel(BMR_REL(rel), MAIN_FORKNUM, NULL,
								EB_LOCK_FIRST);
		page = BufferGetPage(buf);
		PageInit(page, BLCKSZ, 0);
		blkno = BufferGetBlockNumber(buf);
	}

	/* Write the tuple onto the page using PageAddItem-compatible layout */
	{
		TestRelundoTupleHeader thdr;
		OffsetNumber offnum;
		char	   *tup_data;
		Size		data_len;

		/* Build our header + mintuple as a single datum */
		data_len = TESTRELUNDO_TUPLE_HEADER_SIZE + tuple_size;
		tup_data = palloc(data_len);

		thdr.t_len = data_len;
		thdr.t_xmin = GetCurrentTransactionId();
		/* t_self will be set after we know the offset */
		ItemPointerSetInvalid(&thdr.t_self);

		memcpy(tup_data, &thdr, sizeof(TestRelundoTupleHeader));
		memcpy(tup_data + TESTRELUNDO_TUPLE_HEADER_SIZE, mintuple, tuple_size);

		offnum = PageAddItem(page, tup_data, data_len,
							 InvalidOffsetNumber, false, false);

		if (offnum == InvalidOffsetNumber)
			elog(ERROR, "failed to add tuple to page");

		/* Now set the TID */
		ItemPointerSet(tid, blkno, offnum);

		/* Update the stored header with the correct TID */
		{
			ItemId		itemid = PageGetItemId(page, offnum);
			TestRelundoTupleHeader *stored_hdr;

			stored_hdr = (TestRelundoTupleHeader *) PageGetItem(page, itemid);
			ItemPointerCopy(tid, &stored_hdr->t_self);
		}

		pfree(tup_data);
	}

	MarkBufferDirty(buf);
	UnlockReleaseBuffer(buf);

	if (shouldFree)
		pfree(mintuple);
}


/* ----------------------------------------------------------------
 * Slot callbacks
 * ----------------------------------------------------------------
 */
static const TupleTableSlotOps *
testrelundo_slot_callbacks(Relation relation)
{
	return &TTSOpsVirtual;
}


/* ----------------------------------------------------------------
 * Scan callbacks
 * ----------------------------------------------------------------
 */
static TableScanDesc
testrelundo_scan_begin(Relation rel, Snapshot snapshot,
					   int nkeys, ScanKeyData *key,
					   ParallelTableScanDesc pscan,
					   uint32 flags)
{
	TestRelundoScanDesc scan;

	scan = (TestRelundoScanDesc) palloc0(sizeof(TestRelundoScanDescData));
	scan->rs_base.rs_rd = rel;
	scan->rs_base.rs_snapshot = snapshot;
	scan->rs_base.rs_nkeys = nkeys;
	scan->rs_base.rs_flags = flags;
	scan->rs_base.rs_parallel = pscan;

	scan->rs_nblocks = RelationGetNumberOfBlocks(rel);
	scan->rs_curblock = 0;
	scan->rs_curoffset = FirstOffsetNumber;
	scan->rs_cbuf = InvalidBuffer;
	scan->rs_inited = false;

	return (TableScanDesc) scan;
}

static void
testrelundo_scan_end(TableScanDesc sscan)
{
	TestRelundoScanDesc scan = (TestRelundoScanDesc) sscan;

	if (BufferIsValid(scan->rs_cbuf))
		ReleaseBuffer(scan->rs_cbuf);

	pfree(scan);
}

static void
testrelundo_scan_rescan(TableScanDesc sscan, ScanKeyData *key,
						bool set_params, bool allow_strat,
						bool allow_sync, bool allow_pagemode)
{
	TestRelundoScanDesc scan = (TestRelundoScanDesc) sscan;

	if (BufferIsValid(scan->rs_cbuf))
	{
		ReleaseBuffer(scan->rs_cbuf);
		scan->rs_cbuf = InvalidBuffer;
	}

	scan->rs_nblocks = RelationGetNumberOfBlocks(scan->rs_base.rs_rd);
	scan->rs_curblock = 0;
	scan->rs_curoffset = FirstOffsetNumber;
	scan->rs_inited = false;
}

static bool
testrelundo_scan_getnextslot(TableScanDesc sscan,
							 ScanDirection direction,
							 TupleTableSlot *slot)
{
	TestRelundoScanDesc scan = (TestRelundoScanDesc) sscan;
	Relation	rel = scan->rs_base.rs_rd;

	ExecClearTuple(slot);

	for (;;)
	{
		Page		page;
		OffsetNumber maxoff;

		/* Move to next block if needed */
		if (!scan->rs_inited || !BufferIsValid(scan->rs_cbuf) ||
			scan->rs_curoffset > PageGetMaxOffsetNumber(BufferGetPage(scan->rs_cbuf)))
		{
			if (scan->rs_inited)
			{
				if (BufferIsValid(scan->rs_cbuf))
				{
					ReleaseBuffer(scan->rs_cbuf);
					scan->rs_cbuf = InvalidBuffer;
				}
				scan->rs_curblock++;
			}

			/* Find the next non-empty block */
			while (scan->rs_curblock < scan->rs_nblocks)
			{
				scan->rs_cbuf = ReadBuffer(rel, scan->rs_curblock);
				LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);

				page = BufferGetPage(scan->rs_cbuf);
				maxoff = PageGetMaxOffsetNumber(page);

				if (maxoff >= FirstOffsetNumber)
				{
					scan->rs_curoffset = FirstOffsetNumber;
					scan->rs_inited = true;
					LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);
					break;
				}

				UnlockReleaseBuffer(scan->rs_cbuf);
				scan->rs_cbuf = InvalidBuffer;
				scan->rs_curblock++;
			}

			if (scan->rs_curblock >= scan->rs_nblocks)
				return false;	/* End of scan */
		}

		/* Read tuples from the current block */
		LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(scan->rs_cbuf);
		maxoff = PageGetMaxOffsetNumber(page);

		while (scan->rs_curoffset <= maxoff)
		{
			ItemId		itemid;
			TestRelundoTupleHeader *thdr;
			MinimalTuple mintuple;
			OffsetNumber curoff = scan->rs_curoffset;

			scan->rs_curoffset++;

			itemid = PageGetItemId(page, curoff);
			if (!ItemIdIsNormal(itemid))
				continue;

			thdr = (TestRelundoTupleHeader *) PageGetItem(page, itemid);
			mintuple = (MinimalTuple) ((char *) thdr + TESTRELUNDO_TUPLE_HEADER_SIZE);

			/*
			 * Simple visibility: all committed tuples are visible. For a real
			 * AM, we would walk the UNDO chain here. For this test AM, we
			 * consider all tuples visible (the purpose is to test UNDO record
			 * creation, not visibility logic).
			 *
			 * Copy the minimal tuple while we hold the buffer lock, then
			 * force-store it into the slot (which handles Virtual slots).
			 */
			{
				MinimalTuple mt_copy;

				mt_copy = heap_copy_minimal_tuple(mintuple, 0);
				ExecForceStoreMinimalTuple(mt_copy, slot, true);
			}
			slot->tts_tableOid = RelationGetRelid(rel);
			ItemPointerSet(&slot->tts_tid, scan->rs_curblock, curoff);

			LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);
			return true;
		}

		LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);

		/* Exhausted current block, move to next */
		ReleaseBuffer(scan->rs_cbuf);
		scan->rs_cbuf = InvalidBuffer;
		scan->rs_curblock++;
		scan->rs_inited = true;
	}
}


/* ----------------------------------------------------------------
 * Parallel scan stubs (not supported for test AM)
 * ----------------------------------------------------------------
 */
static Size
testrelundo_parallelscan_estimate(Relation rel)
{
	return 0;
}

static Size
testrelundo_parallelscan_initialize(Relation rel,
									ParallelTableScanDesc pscan)
{
	return 0;
}

static void
testrelundo_parallelscan_reinitialize(Relation rel,
									  ParallelTableScanDesc pscan)
{
}


/* ----------------------------------------------------------------
 * Index fetch stubs (not supported for test AM)
 * ----------------------------------------------------------------
 */
static IndexFetchTableData *
testrelundo_index_fetch_begin(Relation rel, uint32 flags)
{
	IndexFetchTableData *scan = palloc0(sizeof(IndexFetchTableData));

	scan->rel = rel;
	return scan;
}

static void
testrelundo_index_fetch_reset(IndexFetchTableData *scan)
{
}

static void
testrelundo_index_fetch_end(IndexFetchTableData *scan)
{
	pfree(scan);
}

static bool
testrelundo_index_fetch_tuple(IndexFetchTableData *scan,
							  ItemPointer tid,
							  Snapshot snapshot,
							  TupleTableSlot *slot,
							  bool *call_again, bool *all_dead)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("index scans not supported by test_undo_tam")));
	return false;
}


/* ----------------------------------------------------------------
 * Non-modifying tuple callbacks
 * ----------------------------------------------------------------
 */
static bool
testrelundo_tuple_fetch_row_version(Relation rel, ItemPointer tid,
									Snapshot snapshot, TupleTableSlot *slot)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("tuple_fetch_row_version not supported by test_undo_tam")));
	return false;
}

static bool
testrelundo_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
	return ItemPointerIsValid(tid);
}

static void
testrelundo_tuple_get_latest_tid(TableScanDesc scan, ItemPointer tid)
{
	/* No-op: we don't support HOT chains */
}

static bool
testrelundo_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
									 Snapshot snapshot)
{
	/* For test purposes, all tuples satisfy all snapshots */
	return true;
}

static TransactionId
testrelundo_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("index_delete_tuples not supported by test_undo_tam")));
	return InvalidTransactionId;
}


/* ----------------------------------------------------------------
 * Tuple modification callbacks
 * ----------------------------------------------------------------
 */
static void
testrelundo_tuple_insert(Relation rel, TupleTableSlot *slot,
						 CommandId cid, uint32 options,
						 BulkInsertStateData *bistate)
{
	ItemPointerData tid;
	RelUndoRecPtr undo_ptr;
	Buffer		undo_buffer;
	RelUndoRecordHeader hdr;
	RelUndoInsertPayload payload;
	Size		record_size;

	/* Set the table OID on the slot */
	slot->tts_tableOid = RelationGetRelid(rel);

	/* Step 1: Insert the tuple into the data page */
	testrelundo_insert_tuple(rel, slot, &tid);
	ItemPointerCopy(&tid, &slot->tts_tid);

	/*
	 * Step 2: Create an UNDO record for this INSERT using the per-relation
	 * UNDO two-phase protocol: Reserve, then Finish.
	 */
	record_size = SizeOfRelUndoRecordHeader + sizeof(RelUndoInsertPayload);

	/* Phase 1: Reserve space in the UNDO log */
	undo_ptr = RelUndoReserve(rel, record_size, &undo_buffer);

	/* Build the UNDO record header */
	hdr.urec_type = RELUNDO_INSERT;
	hdr.urec_len = record_size;
	hdr.urec_xid = GetCurrentTransactionId();
	hdr.urec_prevundorec = GetPerRelUndoPtr(RelationGetRelid(rel));

	/* Build the INSERT payload */
	ItemPointerCopy(&tid, &payload.firsttid);
	ItemPointerCopy(&tid, &payload.endtid); /* Single tuple insert */

	/* Phase 2: Complete the UNDO record */
	RelUndoFinish(rel, undo_buffer, undo_ptr, &hdr,
				 &payload, sizeof(RelUndoInsertPayload));

	/*
	 * Step 3: Register this relation's UNDO chain with the transaction system
	 * so that rollback can find and apply the UNDO records. This function
	 * checks internally if the relation is already registered for this
	 * transaction, so it's safe to call on every insert.
	 */
	RegisterPerRelUndo(RelationGetRelid(rel), undo_ptr);
}

static void
testrelundo_tuple_insert_speculative(Relation rel, TupleTableSlot *slot,
									 CommandId cid, uint32 options,
									 BulkInsertStateData *bistate,
									 uint32 specToken)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("speculative insertion not supported by test_undo_tam")));
}

static void
testrelundo_tuple_complete_speculative(Relation rel, TupleTableSlot *slot,
									   uint32 specToken, bool succeeded)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("speculative insertion not supported by test_undo_tam")));
}

static void
testrelundo_multi_insert(Relation rel, TupleTableSlot **slots,
						 int nslots, CommandId cid, uint32 options,
						 BulkInsertStateData *bistate)
{
	/* Simple implementation: insert each slot individually */
	for (int i = 0; i < nslots; i++)
		testrelundo_tuple_insert(rel, slots[i], cid, options, bistate);
}

static TM_Result
testrelundo_tuple_delete(Relation rel, ItemPointer tid, CommandId cid,
						 uint32 options,
						 Snapshot snapshot, Snapshot crosscheck,
						 bool wait, TM_FailureData *tmfd)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("DELETE not supported by test_undo_tam")));
	return TM_Ok;
}

static TM_Result
testrelundo_tuple_update(Relation rel, ItemPointer otid,
						 TupleTableSlot *slot, CommandId cid,
						 uint32 options,
						 Snapshot snapshot, Snapshot crosscheck,
						 bool wait, TM_FailureData *tmfd,
						 LockTupleMode *lockmode,
						 TU_UpdateIndexes *update_indexes)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("UPDATE not supported by test_undo_tam")));
	return TM_Ok;
}

static TM_Result
testrelundo_tuple_lock(Relation rel, ItemPointer tid, Snapshot snapshot,
					   TupleTableSlot *slot, CommandId cid,
					   LockTupleMode mode, LockWaitPolicy wait_policy,
					   uint8 flags, TM_FailureData *tmfd)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("tuple locking not supported by test_undo_tam")));
	return TM_Ok;
}


/* ----------------------------------------------------------------
 * DDL callbacks
 * ----------------------------------------------------------------
 */
static void
testrelundo_relation_set_new_filelocator(Relation rel,
										 const RelFileLocator *newrlocator,
										 char persistence,
										 TransactionId *freezeXid,
										 MultiXactId *minmulti)
{
	SMgrRelation srel;

	*freezeXid = RecentXmin;
	*minmulti = GetOldestMultiXactId();

	srel = RelationCreateStorage(*newrlocator, persistence, true);

	/*
	 * For unlogged tables, create the init fork.
	 */
	if (persistence == RELPERSISTENCE_UNLOGGED)
	{
		smgrcreate(srel, INIT_FORKNUM, false);
		log_smgrcreate(newrlocator, INIT_FORKNUM);
	}

	smgrclose(srel);

	/*
	 * Initialize the per-relation UNDO fork.  This creates the UNDO fork file
	 * and writes the initial metapage so that subsequent INSERT operations
	 * can reserve UNDO space via RelUndoReserve().
	 */
	RelUndoInitRelation(rel);
}

static void
testrelundo_relation_nontransactional_truncate(Relation rel)
{
	RelationTruncate(rel, 0);
}

static void
testrelundo_relation_copy_data(Relation rel,
							   const RelFileLocator *newrlocator)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("relation_copy_data not supported by test_undo_tam")));
}

static void
testrelundo_relation_copy_for_cluster(Relation OldTable, Relation NewTable,
									  Relation OldIndex, bool use_sort,
									  TransactionId OldestXmin,
									  Snapshot snapshot,
									  TransactionId *xid_cutoff,
									  MultiXactId *multi_cutoff,
									  double *num_tuples,
									  double *tups_vacuumed,
									  double *tups_recently_dead)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("CLUSTER not supported by test_undo_tam")));
}

static void
testrelundo_relation_vacuum(Relation rel, const VacuumParams *params,
							BufferAccessStrategy bstrategy)
{
	/* No-op vacuum for test AM */
}


/* ----------------------------------------------------------------
 * Analyze callbacks (minimal stubs)
 * ----------------------------------------------------------------
 */
static bool
testrelundo_scan_analyze_next_block(TableScanDesc scan, ReadStream *stream)
{
	return false;
}

static bool
testrelundo_scan_analyze_next_tuple(TableScanDesc scan,
									double *liverows,
									double *deadrows,
									TupleTableSlot *slot)
{
	return false;
}


/* ----------------------------------------------------------------
 * Index build callbacks (minimal stubs)
 * ----------------------------------------------------------------
 */
static double
testrelundo_index_build_range_scan(Relation table_rel,
								   Relation index_rel,
								   IndexInfo *index_info,
								   bool allow_sync,
								   bool anyvisible,
								   bool progress,
								   BlockNumber start_blockno,
								   BlockNumber numblocks,
								   IndexBuildCallback callback,
								   void *callback_state,
								   TableScanDesc scan)
{
	return 0;
}

static void
testrelundo_index_validate_scan(Relation table_rel,
								Relation index_rel,
								IndexInfo *index_info,
								Snapshot snapshot,
								ValidateIndexState *state)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("index validation not supported by test_undo_tam")));
}


/* ----------------------------------------------------------------
 * Miscellaneous callbacks
 * ----------------------------------------------------------------
 */
static uint64
testrelundo_relation_size(Relation rel, ForkNumber forkNumber)
{
	return table_block_relation_size(rel, forkNumber);
}

static bool
testrelundo_relation_needs_toast_table(Relation rel)
{
	return false;
}

static void
testrelundo_relation_estimate_size(Relation rel, int32 *attr_widths,
								   BlockNumber *pages, double *tuples,
								   double *allvisfrac)
{
	*pages = RelationGetNumberOfBlocks(rel);
	*tuples = 0;
	*allvisfrac = 0;
}


/* ----------------------------------------------------------------
 * Bitmap/sample scan stubs
 * ----------------------------------------------------------------
 */
static bool
testrelundo_scan_sample_next_block(TableScanDesc scan,
								   SampleScanState *scanstate)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("TABLESAMPLE not supported by test_undo_tam")));
	return false;
}

static bool
testrelundo_scan_sample_next_tuple(TableScanDesc scan,
								   SampleScanState *scanstate,
								   TupleTableSlot *slot)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("TABLESAMPLE not supported by test_undo_tam")));
	return false;
}


/* ----------------------------------------------------------------
 * Per-relation UNDO callbacks
 * ----------------------------------------------------------------
 */
static void
testrelundo_relation_init_undo(Relation rel)
{
	RelUndoInitRelation(rel);
}

static bool
testrelundo_tuple_satisfies_snapshot_undo(Relation rel, ItemPointer tid,
										  Snapshot snapshot, uint64 undo_ptr)
{
	/*
	 * For the test AM, all tuples are visible. A production AM would walk the
	 * UNDO chain here to determine visibility.
	 */
	return true;
}

static void
testrelundo_relation_vacuum_undo(Relation rel, TransactionId oldest_xid)
{
	RelUndoVacuum(rel, oldest_xid);
}


/* test_undo_tam supports cluster-wide UNDO; see am_supports_undo */

/* ----------------------------------------------------------------
 * The TableAmRoutine
 * ----------------------------------------------------------------
 */
static const TableAmRoutine testrelundo_methods = {
	.type = T_TableAmRoutine,
	.am_supports_undo = true,

	.slot_callbacks = testrelundo_slot_callbacks,

	.scan_begin = testrelundo_scan_begin,
	.scan_end = testrelundo_scan_end,
	.scan_rescan = testrelundo_scan_rescan,
	.scan_getnextslot = testrelundo_scan_getnextslot,

	.parallelscan_estimate = testrelundo_parallelscan_estimate,
	.parallelscan_initialize = testrelundo_parallelscan_initialize,
	.parallelscan_reinitialize = testrelundo_parallelscan_reinitialize,

	.index_fetch_begin = testrelundo_index_fetch_begin,
	.index_fetch_reset = testrelundo_index_fetch_reset,
	.index_fetch_end = testrelundo_index_fetch_end,
	.index_fetch_tuple = testrelundo_index_fetch_tuple,

	.tuple_fetch_row_version = testrelundo_tuple_fetch_row_version,
	.tuple_tid_valid = testrelundo_tuple_tid_valid,
	.tuple_get_latest_tid = testrelundo_tuple_get_latest_tid,
	.tuple_satisfies_snapshot = testrelundo_tuple_satisfies_snapshot,
	.index_delete_tuples = testrelundo_index_delete_tuples,

	.tuple_insert = testrelundo_tuple_insert,
	.tuple_insert_speculative = testrelundo_tuple_insert_speculative,
	.tuple_complete_speculative = testrelundo_tuple_complete_speculative,
	.multi_insert = testrelundo_multi_insert,
	.tuple_delete = testrelundo_tuple_delete,
	.tuple_update = testrelundo_tuple_update,
	.tuple_lock = testrelundo_tuple_lock,

	.relation_set_new_filelocator = testrelundo_relation_set_new_filelocator,
	.relation_nontransactional_truncate = testrelundo_relation_nontransactional_truncate,
	.relation_copy_data = testrelundo_relation_copy_data,
	.relation_copy_for_cluster = testrelundo_relation_copy_for_cluster,
	.relation_vacuum = testrelundo_relation_vacuum,

	.scan_analyze_next_block = testrelundo_scan_analyze_next_block,
	.scan_analyze_next_tuple = testrelundo_scan_analyze_next_tuple,
	.index_build_range_scan = testrelundo_index_build_range_scan,
	.index_validate_scan = testrelundo_index_validate_scan,

	.relation_size = testrelundo_relation_size,
	.relation_needs_toast_table = testrelundo_relation_needs_toast_table,

	.relation_estimate_size = testrelundo_relation_estimate_size,

	.scan_sample_next_block = testrelundo_scan_sample_next_block,
	.scan_sample_next_tuple = testrelundo_scan_sample_next_tuple,

	/*
	 * Per-relation UNDO callbacks were previously registered here as
	 * TableAmRoutine fields (.relation_init_undo, .tuple_satisfies_snapshot_undo,
	 * .relation_vacuum_undo).  Those fields have been removed from the struct;
	 * the underlying functions (RelUndoVacuum etc.) are now invoked directly
	 * by the UNDO subsystem rather than through AM dispatch.
	 */
};

Datum
test_undo_tam_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&testrelundo_methods);
}


/* ----------------------------------------------------------------
 * Introspection: test_undo_tam_dump_chain(regclass)
 *
 * Walk the UNDO chain for a relation and return all records as
 * a set-returning function.
 * ----------------------------------------------------------------
 */

/*
 * Return a text name for an UNDO record type.
 */
static const char *
undo_rectype_name(uint16 rectype)
{
	switch (rectype)
	{
		case RELUNDO_INSERT:
			return "INSERT";
		case RELUNDO_DELETE:
			return "DELETE";
		case RELUNDO_UPDATE:
			return "UPDATE";
		case RELUNDO_TUPLE_LOCK:
			return "TUPLE_LOCK";
		case RELUNDO_DELTA_INSERT:
			return "DELTA_INSERT";
		default:
			return "UNKNOWN";
	}
}

/*
 * Per-call state for the SRF.
 */
typedef struct DumpChainState
{
	Relation	rel;
	BlockNumber curblock;		/* Current block in UNDO fork */
	BlockNumber nblocks;		/* Total blocks in UNDO fork */
	uint16		curoffset;		/* Current offset within page */
}			DumpChainState;

Datum
test_undo_tam_dump_chain(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	DumpChainState *state;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc	tupdesc;
		Oid			reloid = PG_GETARG_OID(0);

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Build the output tuple descriptor */
		tupdesc = CreateTemplateTupleDesc(7);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "undo_ptr",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "rec_type",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "xid",
						   XIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "prev_undo_ptr",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "payload_size",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "first_tid",
						   TIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "end_tid",
						   TIDOID, -1, 0);

		TupleDescFinalize(tupdesc);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		/* Open the relation and check for UNDO fork */
		state = (DumpChainState *) palloc0(sizeof(DumpChainState));
		state->rel = table_open(reloid, AccessShareLock);

		if (!smgrexists(RelationGetSmgr(state->rel), RELUNDO_FORKNUM))
		{
			state->nblocks = 0;
			state->curblock = 0;
		}
		else
		{
			state->nblocks = RelationGetNumberOfBlocksInFork(state->rel,
															 RELUNDO_FORKNUM);
			state->curblock = 1;	/* Skip metapage (block 0) */
		}
		state->curoffset = SizeOfRelUndoPageHeaderData;

		funcctx->user_fctx = state;
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	state = (DumpChainState *) funcctx->user_fctx;

	/* Walk through UNDO data pages */
	while (state->curblock < state->nblocks)
	{
		Buffer		buf;
		Page		page;
		char	   *contents;
		RelUndoPageHeader phdr;
		RelUndoRecordHeader rechdr;

		buf = ReadBufferExtended(state->rel, RELUNDO_FORKNUM,
								 state->curblock, RBM_NORMAL, NULL);
		LockBuffer(buf, BUFFER_LOCK_SHARE);

		page = BufferGetPage(buf);
		contents = PageGetContents(page);
		phdr = (RelUndoPageHeader) contents;

		/* Scan records on this page */
		while (state->curoffset < phdr->pd_lower)
		{
			Datum		values[7];
			bool		nulls[7];
			HeapTuple	result_tuple;
			RelUndoRecPtr recptr;
			uint16		offset = state->curoffset;

			memcpy(&rechdr, contents + offset, SizeOfRelUndoRecordHeader);

			/* Skip holes (cancelled reservations) */
			if (rechdr.urec_type == 0)
			{
				state->curoffset += SizeOfRelUndoRecordHeader;
				continue;
			}

			/* Build the RelUndoRecPtr for this record */
			recptr = MakeRelUndoRecPtr(phdr->counter,
									  state->curblock,
									  offset);

			memset(nulls, false, sizeof(nulls));

			values[0] = Int64GetDatum((int64) recptr);
			values[1] = CStringGetTextDatum(undo_rectype_name(rechdr.urec_type));
			values[2] = TransactionIdGetDatum(rechdr.urec_xid);
			values[3] = Int64GetDatum((int64) rechdr.urec_prevundorec);
			values[4] = Int32GetDatum((int32) (rechdr.urec_len - SizeOfRelUndoRecordHeader));

			/* Decode INSERT payload if present */
			if (rechdr.urec_type == RELUNDO_INSERT &&
				rechdr.urec_len >= SizeOfRelUndoRecordHeader + sizeof(RelUndoInsertPayload))
			{
				RelUndoInsertPayload insert_payload;
				ItemPointerData *first_tid_copy;
				ItemPointerData *end_tid_copy;

				memcpy(&insert_payload,
					   contents + offset + SizeOfRelUndoRecordHeader,
					   sizeof(RelUndoInsertPayload));

				first_tid_copy = palloc(sizeof(ItemPointerData));
				end_tid_copy = palloc(sizeof(ItemPointerData));
				ItemPointerCopy(&insert_payload.firsttid, first_tid_copy);
				ItemPointerCopy(&insert_payload.endtid, end_tid_copy);

				values[5] = ItemPointerGetDatum(first_tid_copy);
				values[6] = ItemPointerGetDatum(end_tid_copy);
			}
			else
			{
				nulls[5] = true;
				nulls[6] = true;
			}

			/* Advance offset past this record */
			state->curoffset += rechdr.urec_len;

			UnlockReleaseBuffer(buf);

			result_tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
			SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(result_tuple));
		}

		UnlockReleaseBuffer(buf);

		/* Move to next UNDO page */
		state->curblock++;
		state->curoffset = SizeOfRelUndoPageHeaderData;
	}

	/* Done - close the relation */
	table_close(state->rel, AccessShareLock);
	SRF_RETURN_DONE(funcctx);
}
