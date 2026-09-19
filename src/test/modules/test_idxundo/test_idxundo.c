/*-------------------------------------------------------------------------
 *
 * test_idxundo.c
 *	  Harness to exercise the nbtree/hash UNDO apply callbacks directly.
 *
 * Index UNDO is reachable in a normal build: a table created WITH
 * (index_undo = on) makes its indexes write UNDO, and ROLLBACK reverses the
 * provisional entries.  The SQL tests alongside this module cover that path end
 * to end through the real abort path, and that is the primary coverage.
 *
 * These primitives exist for the cases the end-to-end path cannot reach
 * directly: driving a single apply callback against a chosen slot, and -- via
 * idxundo_capture() + *_apply_*_with() -- reproducing "provisional insert, then
 * the entry is RELOCATED by a split or dedup, then rollback".  That last case
 * is the one that silently destroyed committed data before the apply path
 * verified tuple identity, and it cannot be staged from SQL alone because it
 * needs the pre-relocation tuple bytes.  Payloads built here are
 * byte-identical to what NbtreeUndoLogInsert()/HashUndoLogInsert() write, and
 * dispatch goes through the real GetUndoRmgr(rmid)->rm_undo.
 *
 * This is a test-only module.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/hash.h"
#include "access/nbtree.h"
#include "access/relation.h"
#include "access/undormgr.h"
#include "access/xact.h"
#include "access/xactundo.h"
#include "catalog/pg_am_d.h"
#include "common/hashfn.h"
#include "fmgr.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/itemid.h"
#include "utils/builtins.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

/*
 * Mirrors of the private payload structs in the two UNDO modules.
 *
 * These must stay byte-identical to the originals, because the whole point of
 * the module is to feed the real apply callbacks a payload indistinguishable
 * from what the real writers produce.  Both records store a DISCRIMINATOR (heap
 * TID + key digest + length), not the index tuple -- so these mirrors compute
 * the digest the same way the writers do.
 */
typedef struct NbtreeUndoInsertLeafMirror
{
	Oid			index_oid;
	BlockNumber blkno;
	OffsetNumber offset;
	uint16		itup_sz;
	ItemPointerData heap_tid;
	uint32		key_digest;
} NbtreeUndoInsertLeafMirror;

#define SizeOfNbtreeUndoInsertLeafMirror \
	(offsetof(NbtreeUndoInsertLeafMirror, key_digest) + sizeof(uint32))

typedef struct HashUndoInsertMirror
{
	Oid			index_oid;
	BlockNumber blkno;
	OffsetNumber offset;
	uint16		itup_sz;
	ItemPointerData heap_tid;
	uint32		key_digest;
} HashUndoInsertMirror;

#define SizeOfHashUndoInsertMirror \
	(offsetof(HashUndoInsertMirror, key_digest) + sizeof(uint32))

/*
 * idxundo_key_digest - the digest both writers compute (see their
 * *_undo_key_digest()).  Covers the key bytes after the IndexTuple header, not
 * t_tid, which is compared exactly.
 */
static inline uint32
idxundo_key_digest(IndexTuple itup, Size itup_sz)
{
	if (itup_sz <= sizeof(IndexTupleData))
		return 0;

	return hash_bytes((const unsigned char *) itup + sizeof(IndexTupleData),
					  (int) (itup_sz - sizeof(IndexTupleData)));
}

#define NBTREE_UNDO_INSERT_LEAF_MIRROR	0x0001
#define HASH_UNDO_INSERT_MIRROR			0x0001

/*
 * idxundo_apply_nbtree_leaf(index regclass, blkno int, offset int)
 *
 * Build the exact INSERT_LEAF payload for the live entry at (blkno, offset)
 * -- including the IndexTuple bytes, which the real writer also stores -- and
 * feed it to the registered nbtree UNDO apply callback.
 */
PG_FUNCTION_INFO_V1(idxundo_apply_nbtree_leaf);
Datum
idxundo_apply_nbtree_leaf(PG_FUNCTION_ARGS)
{
	Oid			indexoid = PG_GETARG_OID(0);
	BlockNumber blkno = (BlockNumber) PG_GETARG_INT32(1);
	OffsetNumber offset = (OffsetNumber) PG_GETARG_INT32(2);
	Relation	indexrel;
	Buffer		buffer;
	Page		page;
	NbtreeUndoInsertLeafMirror hdr;
	char	   *payload;
	Size		payload_len;
	IndexTuple	itup = NULL;
	Size		itup_sz = 0;
	const UndoRmgrData *rmgr;
	UndoApplyResult result;

	indexrel = relation_open(indexoid, AccessShareLock);

	/* Capture the tuple currently at that slot, as the writer would have. */
	buffer = ReadBuffer(indexrel, blkno);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buffer);
	if (offset <= PageGetMaxOffsetNumber(page))
	{
		ItemId		lp = PageGetItemId(page, offset);

		if (ItemIdIsNormal(lp))
		{
			itup_sz = ItemIdGetLength(lp);
			itup = (IndexTuple) palloc(itup_sz);
			memcpy(itup, PageGetItem(page, lp), itup_sz);
		}
	}
	UnlockReleaseBuffer(buffer);
	relation_close(indexrel, AccessShareLock);

	if (itup == NULL)
		ereport(ERROR,
				(errmsg("no normal tuple at block %u offset %u", blkno, offset)));

	hdr.index_oid = indexoid;
	hdr.blkno = blkno;
	hdr.offset = offset;
	hdr.itup_sz = (uint16) itup_sz;
	hdr.heap_tid = itup->t_tid;
	hdr.key_digest = idxundo_key_digest(itup, itup_sz);

	payload_len = SizeOfNbtreeUndoInsertLeafMirror;
	payload = (char *) palloc(payload_len);
	memcpy(payload, &hdr, SizeOfNbtreeUndoInsertLeafMirror);

	rmgr = GetUndoRmgr(UNDO_RMID_NBTREE);
	if (rmgr == NULL || rmgr->rm_undo == NULL)
		ereport(ERROR, (errmsg("nbtree UNDO rmgr not registered")));

	result = rmgr->rm_undo(UNDO_RMID_NBTREE,
						   NBTREE_UNDO_INSERT_LEAF_MIRROR,
						   GetCurrentTransactionId(),
						   InvalidOid,
						   payload, payload_len,
						   (UndoRecPtr) 1);

	PG_RETURN_TEXT_P(cstring_to_text(result == UNDO_APPLY_SUCCESS ? "SUCCESS" :
									 result == UNDO_APPLY_SKIPPED ? "SKIPPED" :
									 "ERROR"));
}

/*
 * idxundo_apply_hash(index regclass, blkno int, offset int)
 */
PG_FUNCTION_INFO_V1(idxundo_apply_hash);
Datum
idxundo_apply_hash(PG_FUNCTION_ARGS)
{
	Oid			indexoid = PG_GETARG_OID(0);
	BlockNumber blkno = (BlockNumber) PG_GETARG_INT32(1);
	OffsetNumber offset = (OffsetNumber) PG_GETARG_INT32(2);
	HashUndoInsertMirror hdr;
	const UndoRmgrData *rmgr;
	UndoApplyResult result;

	hdr.index_oid = indexoid;
	hdr.blkno = blkno;
	hdr.offset = offset;

	rmgr = GetUndoRmgr(UNDO_RMID_HASH);
	if (rmgr == NULL || rmgr->rm_undo == NULL)
		ereport(ERROR, (errmsg("hash UNDO rmgr not registered")));

	result = rmgr->rm_undo(UNDO_RMID_HASH,
						   HASH_UNDO_INSERT_MIRROR,
						   GetCurrentTransactionId(),
						   InvalidOid,
						   (const char *) &hdr, SizeOfHashUndoInsertMirror,
						   (UndoRecPtr) 1);

	PG_RETURN_TEXT_P(cstring_to_text(result == UNDO_APPLY_SUCCESS ? "SUCCESS" :
									 result == UNDO_APPLY_SKIPPED ? "SKIPPED" :
									 "ERROR"));
}

/*
 * idxundo_count_dead(index regclass, blkno int) -> number of LP_DEAD items
 */
PG_FUNCTION_INFO_V1(idxundo_count_dead);
Datum
idxundo_count_dead(PG_FUNCTION_ARGS)
{
	Oid			indexoid = PG_GETARG_OID(0);
	BlockNumber blkno = (BlockNumber) PG_GETARG_INT32(1);
	Relation	indexrel;
	Buffer		buffer;
	Page		page;
	OffsetNumber off,
				maxoff;
	int			ndead = 0;

	indexrel = relation_open(indexoid, AccessShareLock);
	buffer = ReadBuffer(indexrel, blkno);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buffer);
	maxoff = PageGetMaxOffsetNumber(page);
	for (off = FirstOffsetNumber; off <= maxoff; off++)
	{
		if (ItemIdIsDead(PageGetItemId(page, off)))
			ndead++;
	}
	UnlockReleaseBuffer(buffer);
	relation_close(indexrel, AccessShareLock);

	PG_RETURN_INT32(ndead);
}

/*
 * idxundo_capture(index regclass, blkno int, offset int) -> bytea
 *
 * Snapshot the IndexTuple bytes at (blkno, offset) NOW.  This is what the real
 * writer stores in the UNDO record at provisional-insert time.  Capturing must
 * happen BEFORE any relocation, otherwise the test would be verifying the
 * record against whatever later moved into the slot.
 */
PG_FUNCTION_INFO_V1(idxundo_capture);
Datum
idxundo_capture(PG_FUNCTION_ARGS)
{
	Oid			indexoid = PG_GETARG_OID(0);
	BlockNumber blkno = (BlockNumber) PG_GETARG_INT32(1);
	OffsetNumber offset = (OffsetNumber) PG_GETARG_INT32(2);
	Relation	indexrel;
	Buffer		buffer;
	Page		page;
	bytea	   *out = NULL;

	indexrel = relation_open(indexoid, AccessShareLock);
	buffer = ReadBuffer(indexrel, blkno);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buffer);
	if (offset <= PageGetMaxOffsetNumber(page))
	{
		ItemId		lp = PageGetItemId(page, offset);

		if (ItemIdIsNormal(lp))
		{
			Size		sz = ItemIdGetLength(lp);

			out = (bytea *) palloc(VARHDRSZ + sz);
			SET_VARSIZE(out, VARHDRSZ + sz);
			memcpy(VARDATA(out), PageGetItem(page, lp), sz);
		}
	}
	UnlockReleaseBuffer(buffer);
	relation_close(indexrel, AccessShareLock);

	if (out == NULL)
		ereport(ERROR,
				(errmsg("no normal tuple at block %u offset %u", blkno, offset)));
	PG_RETURN_BYTEA_P(out);
}

/*
 * idxundo_apply_nbtree_leaf_with(index, blkno, offset, itup bytea)
 *
 * Apply an nbtree INSERT_LEAF undo record built from a PREVIOUSLY captured
 * tuple -- the faithful reproduction of provisional-insert, then relocation,
 * then rollback.
 */
PG_FUNCTION_INFO_V1(idxundo_apply_nbtree_leaf_with);
Datum
idxundo_apply_nbtree_leaf_with(PG_FUNCTION_ARGS)
{
	Oid			indexoid = PG_GETARG_OID(0);
	BlockNumber blkno = (BlockNumber) PG_GETARG_INT32(1);
	OffsetNumber offset = (OffsetNumber) PG_GETARG_INT32(2);
	bytea	   *tup = PG_GETARG_BYTEA_PP(3);
	Size		itup_sz = VARSIZE_ANY_EXHDR(tup);
	NbtreeUndoInsertLeafMirror hdr;
	char	   *payload;
	Size		payload_len;
	const UndoRmgrData *rmgr;
	UndoApplyResult result;

	hdr.index_oid = indexoid;
	hdr.blkno = blkno;
	hdr.offset = offset;
	hdr.itup_sz = (uint16) itup_sz;
	hdr.heap_tid = ((IndexTuple) VARDATA_ANY(tup))->t_tid;
	hdr.key_digest = idxundo_key_digest((IndexTuple) VARDATA_ANY(tup), itup_sz);

	payload_len = SizeOfNbtreeUndoInsertLeafMirror;
	payload = (char *) palloc(payload_len);
	memcpy(payload, &hdr, SizeOfNbtreeUndoInsertLeafMirror);

	rmgr = GetUndoRmgr(UNDO_RMID_NBTREE);
	if (rmgr == NULL || rmgr->rm_undo == NULL)
		ereport(ERROR, (errmsg("nbtree UNDO rmgr not registered")));

	result = rmgr->rm_undo(UNDO_RMID_NBTREE, NBTREE_UNDO_INSERT_LEAF_MIRROR,
						   GetCurrentTransactionId(), InvalidOid,
						   payload, payload_len, (UndoRecPtr) 1);

	PG_RETURN_TEXT_P(cstring_to_text(result == UNDO_APPLY_SUCCESS ? "SUCCESS" :
									 result == UNDO_APPLY_SKIPPED ? "SKIPPED" :
									 "ERROR"));
}

/*
 * idxundo_apply_hash_with(index, blkno, offset, itup bytea)
 *
 * Apply a hash INSERT undo record built from a PREVIOUSLY captured tuple --
 * provisional insert, then bucket split / relocation, then rollback.
 */
PG_FUNCTION_INFO_V1(idxundo_apply_hash_with);
Datum
idxundo_apply_hash_with(PG_FUNCTION_ARGS)
{
	Oid			indexoid = PG_GETARG_OID(0);
	BlockNumber blkno = (BlockNumber) PG_GETARG_INT32(1);
	OffsetNumber offset = (OffsetNumber) PG_GETARG_INT32(2);
	bytea	   *tup = PG_GETARG_BYTEA_PP(3);
	Size		itup_sz = VARSIZE_ANY_EXHDR(tup);
	HashUndoInsertMirror hdr;
	char	   *payload;
	Size		payload_len;
	const UndoRmgrData *rmgr;
	UndoApplyResult result;

	hdr.index_oid = indexoid;
	hdr.blkno = blkno;
	hdr.offset = offset;
	hdr.itup_sz = (uint16) itup_sz;
	hdr.heap_tid = ((IndexTuple) VARDATA_ANY(tup))->t_tid;
	hdr.key_digest = idxundo_key_digest((IndexTuple) VARDATA_ANY(tup), itup_sz);

	payload_len = SizeOfHashUndoInsertMirror;
	payload = (char *) palloc(payload_len);
	memcpy(payload, &hdr, SizeOfHashUndoInsertMirror);

	rmgr = GetUndoRmgr(UNDO_RMID_HASH);
	if (rmgr == NULL || rmgr->rm_undo == NULL)
		ereport(ERROR, (errmsg("hash UNDO rmgr not registered")));

	result = rmgr->rm_undo(UNDO_RMID_HASH, HASH_UNDO_INSERT_MIRROR,
						   GetCurrentTransactionId(), InvalidOid,
						   payload, payload_len, (UndoRecPtr) 1);

	PG_RETURN_TEXT_P(cstring_to_text(result == UNDO_APPLY_SUCCESS ? "SUCCESS" :
									 result == UNDO_APPLY_SKIPPED ? "SKIPPED" :
									 "ERROR"));
}

/*
 * idxundo_count_dead_all(index regclass) -> total LP_DEAD items in the index
 *
 * The end-to-end tests do not know which block the aborted entries landed on,
 * so they count over the whole index.  Block 0 of an nbtree is the metapage and
 * has no line pointers, so including it is harmless.
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
		 * emptiness: a hash bitmap page is a dense run of set bits, which read
		 * as line pointers whose lp_flags happen to be LP_DEAD.  Counting
		 * those reports over a thousand phantom dead entries in a freshly
		 * built hash index and makes the before/after comparison meaningless.
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
 * This is the regression guard for the defect that made index UNDO a silent
 * no-op: the old write path wrote a valid XLOG_UNDO_BATCH but published no chain
 * head, so this would have returned false after an insert.
 *
 * Both states must count as published, because index UNDO now DEFERS its records
 * so a statement's entries share one batch (DeferXactUndoData).  Mid-transaction
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
