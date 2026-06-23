/*-------------------------------------------------------------------------
 *
 * hash_undo.c
 *	  Hash index UNDO resource manager
 *
 * This module implements UNDO apply callbacks for the hash index AM.
 * When a transaction aborts, provisionally inserted index entries are
 * marked LP_DEAD so that VACUUM is not required to clean up after
 * aborted transactions.
 *
 * Combined with heap UNDO and nbtree UNDO, hash UNDO provides a
 * "zero-VACUUM" experience for aborted transactions: heap tuples and
 * their index entries are cleaned up immediately during rollback.
 *
 * UNDO Subtypes:
 *   INSERT:  Undo a hash index tuple insertion (mark entry LP_DEAD)
 *
 * All hooks are gated by RelationAmSupportsUndo(heapRel) -- hash UNDO
 * is controlled by the parent table AM's am_supports_undo declaration.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/hash/hash_undo.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/hash.h"
#include "access/relation.h"
#include "access/undobuffer.h"
#include "access/undo_xlog.h"
#include "access/undorecord.h"
#include "access/undormgr.h"
#include "access/xact.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/itemid.h"
#include "utils/rel.h"
#include "utils/relcache.h"

/*
 * Hash UNDO subtypes (stored in urec_info)
 */
#define HASH_UNDO_INSERT	0x0001	/* bucket/overflow page tuple insertion */

/*
 * HashUndoInsert - Payload for hash insert undo
 */
typedef struct HashUndoInsert
{
	Oid			index_oid;		/* OID of the hash index relation */
	BlockNumber blkno;			/* Page where tuple was inserted */
	OffsetNumber offset;		/* Offset of the inserted tuple */
}			HashUndoInsert;

#define SizeOfHashUndoInsert \
	(offsetof(HashUndoInsert, offset) + sizeof(OffsetNumber))

/* Forward declarations */
static UndoApplyResult hash_undo_apply(uint8 rmid, uint16 info,
									   TransactionId xid, Oid reloid,
									   const char *payload, Size payload_len,
									   UndoRecPtr urec_ptr);
static void hash_undo_desc(StringInfo buf, uint8 rmid, uint16 info,
						   const char *payload, Size payload_len);

/* The hash UNDO RM registration entry */
static const UndoRmgrData hash_undo_rmgr = {
	.rm_name = "hash",
	.rm_undo = hash_undo_apply,
	.rm_desc = hash_undo_desc,
};

/*
 * HashUndoRmgrInit - Register the hash UNDO resource manager
 */
void
HashUndoRmgrInit(void)
{
	RegisterUndoRmgr(UNDO_RMID_HASH, &hash_undo_rmgr);
}

/*
 * HashUndoLogInsert - Write UNDO record for a hash index tuple insertion
 *
 * Called from _hash_doinsert() after the insertion has been WAL-logged.
 * This records enough information to mark the inserted entry LP_DEAD on abort.
 */
void
HashUndoLogInsert(Relation rel, Relation heapRel, Buffer buf,
				  OffsetNumber offset)
{
	TransactionId xid = GetCurrentTransactionId();
	HashUndoInsert hdr;

	hdr.index_oid = RelationGetRelid(rel);
	hdr.blkno = BufferGetBlockNumber(buf);
	hdr.offset = offset;

	/*
	 * When the heap has an active UNDO write buffer, piggyback on it to avoid
	 * a separate UndoLogAllocate + WAL insert + pwrite per index entry.
	 */
	if (UndoBufferIsActive(heapRel))
	{
		UndoBufferAddRecordParts(heapRel,
								 UNDO_RMID_HASH,
								 HASH_UNDO_INSERT,
								 (const char *) &hdr,
								 SizeOfHashUndoInsert,
								 NULL, 0);
	}
	else
	{
		UndoRecordSet *uset;

		uset = UndoRecordSetCreate(xid, GetCurrentTransactionUndoRecPtr());
		UndoRecordAddPayloadParts(uset,
								  UNDO_RMID_HASH,
								  HASH_UNDO_INSERT,
								  RelationGetRelid(heapRel),
								  (const char *) &hdr,
								  SizeOfHashUndoInsert,
								  NULL, 0);
		UndoRecordSetInsert(uset);
		UndoRecordSetFree(uset);
	}
}

/*
 * hash_undo_apply - Apply a single hash UNDO record
 *
 * This is the rm_undo callback for the hash RM.  On abort, marks the
 * inserted index entry as LP_DEAD.
 */
static UndoApplyResult
hash_undo_apply(uint8 rmid, uint16 info, TransactionId xid, Oid reloid,
				const char *payload, Size payload_len, UndoRecPtr urec_ptr)
{
	Assert(rmid == UNDO_RMID_HASH);

	/*
	 * During crash recovery, syscache may not be initialized when
	 * PerformUndoRecovery() runs.  Defer UNDO application until after the
	 * system is fully initialized (background worker will handle it).
	 */
	if (InRecovery)
	{
		ereport(DEBUG2,
				(errmsg("hash UNDO: deferring transaction %u to logical revert worker "
						"(in crash recovery, syscache not available)",
						xid)));
		return UNDO_APPLY_SKIPPED;
	}

	switch (info)
	{
		case HASH_UNDO_INSERT:
			{
				HashUndoInsert hdr;
				Relation	indexrel;
				Buffer		buffer;
				Page		page;

				if (payload_len < SizeOfHashUndoInsert)
					return UNDO_APPLY_ERROR;

				memcpy(&hdr, payload, SizeOfHashUndoInsert);

				/*
				 * Open the index directly using the OID stored in the UNDO
				 * payload.
				 */
				indexrel = try_relation_open(hdr.index_oid, RowExclusiveLock);
				if (indexrel == NULL)
				{
					ereport(DEBUG2,
							(errmsg("hash UNDO INSERT: index %u no longer exists",
									hdr.index_oid)));
					return UNDO_APPLY_SKIPPED;
				}

				if (RelationGetNumberOfBlocks(indexrel) <= hdr.blkno)
				{
					ereport(DEBUG2,
							(errmsg("hash UNDO INSERT: block %u beyond end of index %u",
									hdr.blkno, hdr.index_oid)));
					relation_close(indexrel, RowExclusiveLock);
					return UNDO_APPLY_SKIPPED;
				}

				buffer = ReadBuffer(indexrel, hdr.blkno);
				LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
				page = BufferGetPage(buffer);

				if (hdr.offset <= PageGetMaxOffsetNumber(page))
				{
					ItemId		lp = PageGetItemId(page, hdr.offset);

					START_CRIT_SECTION();

					if (ItemIdIsNormal(lp))
						ItemIdMarkDead(lp);

					MarkBufferDirty(buffer);

					/* Generate physiological CLR for crash recovery */
					if (RelationNeedsWAL(indexrel))
					{
						XLogRecPtr	clr_lsn;
						xl_undo_apply xlrec;

						xlrec.urec_ptr = urec_ptr;
						xlrec.xid = xid;
						xlrec.target_locator = indexrel->rd_locator;
						xlrec.target_block = hdr.blkno;
						xlrec.target_offset = hdr.offset;
						xlrec.operation_type = info;
						xlrec.clr_flags = UNDO_CLR_LP_DEAD;
						xlrec.tuple_len = 0;

						XLogBeginInsert();
						XLogRegisterData((char *) &xlrec,
										 SizeOfUndoApply);
						XLogRegisterBuffer(0, buffer,
										   REGBUF_STANDARD);
						clr_lsn = XLogInsert(RM_UNDO_ID,
											 XLOG_UNDO_APPLY_RECORD);
						PageSetLSN(page, clr_lsn);
					}

					END_CRIT_SECTION();
				}

				UnlockReleaseBuffer(buffer);
				relation_close(indexrel, RowExclusiveLock);
				return UNDO_APPLY_SUCCESS;
			}

		default:
			return UNDO_APPLY_SKIPPED;
	}
}

/*
 * hash_undo_desc - Describe a hash UNDO record for debugging
 */
static void
hash_undo_desc(StringInfo buf, uint8 rmid, uint16 info,
			   const char *payload, Size payload_len)
{
	const char *opname;

	switch (info)
	{
		case HASH_UNDO_INSERT:
			opname = "INSERT";
			break;
		default:
			opname = "UNKNOWN";
			break;
	}

	appendStringInfo(buf, "hash %s", opname);

	if (payload_len >= sizeof(Oid) && info == HASH_UNDO_INSERT)
	{
		Oid			index_oid;

		memcpy(&index_oid, payload, sizeof(Oid));
		appendStringInfo(buf, " index %u", index_oid);
	}
}
