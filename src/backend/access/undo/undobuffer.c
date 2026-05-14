/*-------------------------------------------------------------------------
 *
 * undobuffer.c
 *	  AM-agnostic Tier 2 UNDO write buffer
 *
 * This module implements a per-backend byte buffer that accumulates
 * serialized UNDO records for the current DML operation.  At WAL-write time,
 * the buffer contents are embedded directly inside the AM's WAL record,
 * eliminating a separate XLOG_UNDO_BATCH record for single-tuple operations.
 *
 * The buffer logic is entirely AM-agnostic: it serializes UndoRecordHeaders
 * with opaque payloads, identified by urec_rmid for dispatch during rollback.
 * Any access method (heap, nbtree, custom AMs) can use this buffer.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/undo/undobuffer.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/undobuffer.h"
#include "access/undolog.h"
#include "access/undorecord.h"
#include "access/undo_xlog.h"
#include "access/xactundo.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/*
 * Flush thresholds.  Tunable via the undo_batch_size_kb and
 * undo_batch_record_limit GUCs (src/backend/access/undo/undolog.c).
 */
#define UNDO_BUFFER_FLUSH_THRESHOLD		(undo_batch_size_kb * 1024)
#define UNDO_BUFFER_FLUSH_RECORDS		undo_batch_record_limit

/*
 * Per-backend Tier 2 UNDO buffer.  Only one relation can be active at a time.
 */
typedef struct UndoTier2Buffer
{
	char	   *data;			/* palloc'd: serialized
								 * UndoRecordHeader+payload */
	Size		len;			/* bytes currently used */
	Size		capacity;		/* allocated capacity */
	int			nrecords;		/* records in buffer */
	TransactionId xid;			/* owning transaction */
	XLogRecPtr	chain_prev;		/* LSN of previous UNDO batch for chain
								 * linkage */
	Oid			relid;			/* OID of the relation with active buffer */
	bool		active;
}			UndoTier2Buffer;

static UndoTier2Buffer undo_t2buf =
{
	.data = NULL,
		.len = 0,
		.capacity = 0,
		.nrecords = 0,
		.xid = InvalidTransactionId,
		.chain_prev = InvalidXLogRecPtr,
		.relid = InvalidOid,
		.active = false,
};

/*
 * UndoTier2EnsureCapacity - grow undo_t2buf to hold additional bytes
 */
static void
UndoTier2EnsureCapacity(Size additional)
{
	if (undo_t2buf.len + additional <= undo_t2buf.capacity)
		return;					/* already enough room */

	if (undo_t2buf.capacity == 0)
	{
		undo_t2buf.capacity = Max(512, additional);
		undo_t2buf.data = MemoryContextAlloc(TopMemoryContext,
											 undo_t2buf.capacity);
	}
	else
	{
		Size		new_cap = undo_t2buf.capacity;

		while (new_cap < undo_t2buf.len + additional)
			new_cap *= 2;
		undo_t2buf.data = repalloc(undo_t2buf.data, new_cap);
		undo_t2buf.capacity = new_cap;
	}
}

/*
 * UndoTier2AddRecord - serialize one UNDO record into undo_t2buf
 */
static void
UndoTier2AddRecord(uint8 rmid, uint16 info, Oid reloid,
				   const char *payload, Size payload_len)
{
	Size		record_size = SizeOfUndoRecordHeader + payload_len;
	UndoRecordHeader *header;
	char	   *dest;

	UndoTier2EnsureCapacity(record_size);

	dest = undo_t2buf.data + undo_t2buf.len;
	header = (UndoRecordHeader *) dest;
	memset(header, 0, SizeOfUndoRecordHeader);
	header->urec_rmid = rmid;
	header->urec_flags = UNDO_INFO_XID_VALID;
	if (payload_len > 0)
		header->urec_flags |= UNDO_INFO_HAS_PAYLOAD;
	header->urec_info = info;
	header->urec_len = (uint32) record_size;
	header->urec_xid = undo_t2buf.xid;
	header->urec_prev = (UndoRecPtr) undo_t2buf.chain_prev;
	header->urec_reloid = reloid;
	header->urec_payload_len = (uint32) payload_len;
	header->urec_clr_ptr = InvalidXLogRecPtr;

	if (payload_len > 0 && payload != NULL)
		memcpy(dest + SizeOfUndoRecordHeader, payload, payload_len);

	undo_t2buf.len += record_size;
	undo_t2buf.nrecords++;
}

/*
 * UndoTier2AddRecordParts - like UndoTier2AddRecord but scatter-gather
 */
static void
UndoTier2AddRecordParts(uint8 rmid, uint16 info, Oid reloid,
						const char *part1, Size part1_len,
						const char *part2, Size part2_len)
{
	Size		payload_len = part1_len + part2_len;
	Size		record_size = SizeOfUndoRecordHeader + payload_len;
	UndoRecordHeader *header;
	char	   *dest;

	UndoTier2EnsureCapacity(record_size);

	dest = undo_t2buf.data + undo_t2buf.len;
	header = (UndoRecordHeader *) dest;
	memset(header, 0, SizeOfUndoRecordHeader);
	header->urec_rmid = rmid;
	header->urec_flags = UNDO_INFO_XID_VALID;
	if (payload_len > 0)
		header->urec_flags |= UNDO_INFO_HAS_PAYLOAD;
	header->urec_info = info;
	header->urec_len = (uint32) record_size;
	header->urec_xid = undo_t2buf.xid;
	header->urec_prev = (UndoRecPtr) undo_t2buf.chain_prev;
	header->urec_reloid = reloid;
	header->urec_payload_len = (uint32) payload_len;
	header->urec_clr_ptr = InvalidXLogRecPtr;

	dest += SizeOfUndoRecordHeader;
	if (part1_len > 0 && part1 != NULL)
		memcpy(dest, part1, part1_len);
	if (part2_len > 0 && part2 != NULL)
		memcpy(dest + part1_len, part2, part2_len);

	undo_t2buf.len += record_size;
	undo_t2buf.nrecords++;
}


/* -----------------------------------------------------------------------
 * Public API
 * -----------------------------------------------------------------------
 */

void
UndoBufferBegin(Relation rel, int64 nrows)
{
	/* Only one relation at a time can have an active buffer */
	if (undo_t2buf.active)
	{
		if (undo_t2buf.relid == RelationGetRelid(rel))
			return;				/* already active for this relation */

		/* Different relation -- flush and end the previous one */
		UndoBufferEnd(rel);
	}

	undo_t2buf.xid = GetCurrentTransactionId();
	undo_t2buf.relid = RelationGetRelid(rel);
	undo_t2buf.chain_prev = (XLogRecPtr) GetCurrentTransactionUndoRecPtr();
	undo_t2buf.len = 0;
	undo_t2buf.nrecords = 0;
	undo_t2buf.active = true;
	/* undo_t2buf.data and capacity are preserved across activations */

	ereport(DEBUG2,
			(errmsg("UNDO tier2 buffer activated for relation %u, estimated %lld rows",
					RelationGetRelid(rel), (long long) nrows)));
}

void
UndoBufferEnd(Relation rel)
{
	if (!undo_t2buf.active)
		return;

	/* Flush any remaining records via the overflow path */
	if (undo_t2buf.nrecords > 0)
		UndoBufferFlush();

	ereport(DEBUG2,
			(errmsg("UNDO tier2 buffer deactivated for relation %u",
					undo_t2buf.relid)));

	undo_t2buf.relid = InvalidOid;
	undo_t2buf.len = 0;
	undo_t2buf.nrecords = 0;
	undo_t2buf.xid = InvalidTransactionId;
	undo_t2buf.chain_prev = InvalidXLogRecPtr;
	undo_t2buf.active = false;
}

bool
UndoBufferIsActive(Relation rel)
{
	return undo_t2buf.active &&
		undo_t2buf.relid == RelationGetRelid(rel);
}

void
UndoBufferAddRecord(Relation rel, uint8 rmid, uint16 info,
					const char *payload, Size payload_len)
{
	Assert(undo_t2buf.active);

	UndoTier2AddRecord(rmid, info, RelationGetRelid(rel),
					   payload, payload_len);

	/* Overflow flush when thresholds are exceeded */
	if (undo_t2buf.len >= UNDO_BUFFER_FLUSH_THRESHOLD ||
		undo_t2buf.nrecords >= UNDO_BUFFER_FLUSH_RECORDS)
		UndoBufferFlush();
}

void
UndoBufferAddRecordParts(Relation rel, uint8 rmid, uint16 info,
						 const char *part1, Size part1_len,
						 const char *part2, Size part2_len)
{
	Assert(undo_t2buf.active);

	UndoTier2AddRecordParts(rmid, info, RelationGetRelid(rel),
							part1, part1_len, part2, part2_len);

	/* Overflow flush when thresholds are exceeded */
	if (undo_t2buf.len >= UNDO_BUFFER_FLUSH_THRESHOLD ||
		undo_t2buf.nrecords >= UNDO_BUFFER_FLUSH_RECORDS)
		UndoBufferFlush();
}

bool
UndoBufferHasPendingData(void)
{
	return undo_t2buf.active && undo_t2buf.nrecords > 0;
}

void
UndoBufferTakePayload(char **data_out, Size *len_out, int *nrecords_out,
					  XLogRecPtr *chain_prev_out)
{
	Assert(undo_t2buf.active);
	Assert(undo_t2buf.nrecords > 0);

	*data_out = undo_t2buf.data;
	*len_out = undo_t2buf.len;
	*nrecords_out = undo_t2buf.nrecords;
	*chain_prev_out = undo_t2buf.chain_prev;
}

void
UndoBufferReset(XLogRecPtr embedded_lsn)
{
	/* Update chain head so the next batch links to this one */
	undo_t2buf.chain_prev = embedded_lsn;
	undo_t2buf.len = 0;
	undo_t2buf.nrecords = 0;

	/*
	 * Update the per-transaction undo pointer so that subsequent
	 * UndoBufferBegin calls (for different relations in the same transaction)
	 * pick up the correct chain_prev.  Without this, multi-table transactions
	 * would break the UNDO chain.
	 */
	SetCurrentTransactionUndoRecPtr((UndoRecPtr) embedded_lsn);
}

void
UndoBufferFlush(void)
{
	xl_undo_batch xlrec;
	XLogRecPtr	batch_lsn;
	Oid			primary_reloid = InvalidOid;

	if (!undo_t2buf.active || undo_t2buf.nrecords == 0)
		return;

	/* Extract primary reloid from first record as an optimization hint */
	if (undo_t2buf.len >= SizeOfUndoRecordHeader)
	{
		UndoRecordHeader *first_hdr = (UndoRecordHeader *) undo_t2buf.data;

		primary_reloid = first_hdr->urec_reloid;
	}

	/* Build the batch header */
	xlrec.xid = undo_t2buf.xid;
	xlrec.chain_prev = undo_t2buf.chain_prev;
	xlrec.nrecords = (uint32) undo_t2buf.nrecords;
	xlrec.total_len = (uint32) undo_t2buf.len;
	xlrec.primary_reloid = primary_reloid;
	xlrec.persistence = UNDOPERSISTENCE_PERMANENT;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfUndoBatch);
	XLogRegisterData(undo_t2buf.data, undo_t2buf.len);
	(void) XLogInsert(RM_UNDO_ID, XLOG_UNDO_BATCH);

	/*
	 * XLogInsert() returns the end+1 position; the rollback path re-reads the
	 * batch by its START LSN.  ProcLastRecPtr is the start of the record we
	 * just inserted.  See the matching comment in UndoRecordSetInsert().
	 */
	batch_lsn = ProcLastRecPtr;

	/* Update chain tracking */
	undo_t2buf.chain_prev = batch_lsn;
	UndoRegisterBatchLSN(batch_lsn);
	XActUndoUpdateLastBatchLSN(batch_lsn, UNDOPERSISTENCE_PERMANENT);
	SetCurrentTransactionUndoRecPtr((UndoRecPtr) batch_lsn);

	ereport(DEBUG2,
			(errmsg("UNDO tier2 overflow flush: %d records, %zu bytes, lsn %X/%X",
					undo_t2buf.nrecords, undo_t2buf.len,
					LSN_FORMAT_ARGS(batch_lsn))));

	/* Reset buffer for next batch */
	undo_t2buf.len = 0;
	undo_t2buf.nrecords = 0;
}
