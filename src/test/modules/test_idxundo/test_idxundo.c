/*-------------------------------------------------------------------------
 *
 * test_idxundo.c
 *	  SQL-callable probes for the index UNDO tests.
 *
 * The tests drive index UNDO end to end through the real abort path: a table
 * created WITH (index_undo = on) makes its indexes write UNDO, and ROLLBACK
 * reverses the provisional entries.  These functions only observe the result:
 * how many index entries are LP_DEAD, and whether the current transaction has
 * published an UNDO chain for AtAbort_XactUndo() to walk.
 *
 * This is a test-only module.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/hash.h"
#include "access/nbtree.h"
#include "access/relation.h"
#include "access/xact.h"
#include "access/xactundo.h"
#include "catalog/pg_am_d.h"
#include "fmgr.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

/*
 * idxundo_count_dead_all(index regclass) -> total LP_DEAD items in the index
 *
 * The end-to-end tests do not know which block the aborted entries landed on,
 * so they count over every entry-bearing page of the index: btree leaves, hash
 * bucket and overflow pages.
 */
PG_FUNCTION_INFO_V1(idxundo_count_dead_all);
Datum
idxundo_count_dead_all(PG_FUNCTION_ARGS)
{
	Oid			indexoid = PG_GETARG_OID(0);
	Relation	indexrel;
	BlockNumber blk,
				nblocks;
	Oid			relam;
	int			ndead = 0;

	indexrel = relation_open(indexoid, AccessShareLock);
	nblocks = RelationGetNumberOfBlocks(indexrel);
	relam = indexrel->rd_rel->relam;

	if (relam != BTREE_AM_OID && relam != HASH_AM_OID)
		ereport(ERROR,
				(errmsg("index \"%s\" is neither a btree nor a hash index",
						RelationGetRelationName(indexrel))));

	for (blk = 0; blk < nblocks; blk++)
	{
		Buffer		buffer = ReadBuffer(indexrel, blk);
		Page		page;
		OffsetNumber off,
					maxoff;

		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buffer);

		/*
		 * Only count line pointers on pages that actually hold index entries.
		 * Every other page kind must be skipped, not merely tested for
		 * emptiness: a hash bitmap page is a dense run of set bits, which
		 * read as line pointers whose lp_flags happen to be LP_DEAD.
		 * Counting those reports over a thousand phantom dead entries in a
		 * freshly built hash index and makes the before/after comparison
		 * meaningless.
		 */
		if (!PageIsNew(page) && !PageIsEmpty(page) &&
			PageGetSpecialSize(page) > 0 &&
			(relam == BTREE_AM_OID
			 ? P_ISLEAF(BTPageGetOpaque(page))
			 : ((HashPageGetOpaque(page)->hasho_flag & LH_PAGE_TYPE) == LH_BUCKET_PAGE ||
				(HashPageGetOpaque(page)->hasho_flag & LH_PAGE_TYPE) == LH_OVERFLOW_PAGE)))
		{
			maxoff = PageGetMaxOffsetNumber(page);
			for (off = FirstOffsetNumber; off <= maxoff; off++)
			{
				if (ItemIdIsDead(PageGetItemId(page, off)))
					ndead++;
			}
		}
		UnlockReleaseBuffer(buffer);
	}
	relation_close(indexrel, AccessShareLock);

	PG_RETURN_INT32(ndead);
}

/*
 * idxundo_undo_chain_published() -> bool
 *
 * True when the current transaction has UNDO that AtAbort_XactUndo() will act
 * on: either a permanent-persistence batch LSN already recorded in
 * XactUndo.last_batch_lsn[], or records still sitting in the deferred batch that
 * the abort path flushes before it reads that LSN.
 *
 * An insert that produced a valid XLOG_UNDO_BATCH but published no chain head
 * would make ROLLBACK a silent no-op; this returns false in that case.
 *
 * Both states count as published, because index UNDO DEFERS its records so a
 * statement's entries share one batch (DeferXactUndoData).  Mid-transaction
 * the records are normally still pending, and that is correct -- what would be a
 * bug is neither: no pending records AND no chain head means the insert produced
 * no UNDO at all.
 */
PG_FUNCTION_INFO_V1(idxundo_undo_chain_published);
Datum
idxundo_undo_chain_published(PG_FUNCTION_ARGS)
{
	XLogRecPtr	lsn = GetCurrentXactLastBatchLSN(UNDOPERSISTENCE_PERMANENT);

	PG_RETURN_BOOL(!XLogRecPtrIsInvalid(lsn) || XactUndoHasPendingData());
}
