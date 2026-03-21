/*-------------------------------------------------------------------------
 *
 * undorecord.c
 *	  UNDO record assembly and serialization
 *
 * This file implements the UNDO record format and provides functions
 * for creating, serializing, and deserializing UNDO records.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/undo/undorecord.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/undo.h"
#include "access/undorecord.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/*
 * UndoRecordGetSize - Calculate size needed for an UNDO record
 *
 * This includes the header plus any payload data (e.g., tuple data).
 */
Size
UndoRecordGetSize(uint16 record_type, HeapTuple tuple)
{
	Size		size = SizeOfUndoRecordHeader;

	switch (record_type)
	{
		case UNDO_INSERT:
			/* INSERT records don't need tuple data, just mark the operation */
			break;

		case UNDO_DELETE:
		case UNDO_UPDATE:
		case UNDO_PRUNE:
		case UNDO_INPLACE:
			/* These record types need full tuple data */
			if (tuple != NULL)
				size += tuple->t_len;
			break;

		default:
			elog(ERROR, "unknown UNDO record type: %u", record_type);
	}

	return size;
}

/*
 * UndoRecordSerialize - Serialize an UNDO record into a buffer
 *
 * The destination buffer must be large enough to hold the entire record.
 * Use UndoRecordGetSize() to determine the required size.
 */
void
UndoRecordSerialize(char *dest, UndoRecordHeader * header,
					const char *payload, Size payload_len)
{
	/* Copy header */
	memcpy(dest, header, SizeOfUndoRecordHeader);

	/* Copy payload if present */
	if (payload_len > 0 && payload != NULL)
	{
		memcpy(dest + SizeOfUndoRecordHeader, payload, payload_len);
	}
}

/*
 * UndoRecordDeserialize - Deserialize an UNDO record from a buffer
 *
 * Reads the header and allocates space for payload if needed.
 * Returns true on success, false on failure.
 *
 * The payload pointer is set to point into the source buffer (no copy).
 */
bool
UndoRecordDeserialize(const char *src, UndoRecordHeader * header,
					  char **payload)
{
	if (src == NULL || header == NULL)
		return false;

	/* Copy header */
	memcpy(header, src, SizeOfUndoRecordHeader);

	/* Set payload pointer if there is payload data */
	if (header->urec_payload_len > 0)
	{
		if (payload != NULL)
			*payload = (char *) (src + SizeOfUndoRecordHeader);
	}
	else
	{
		if (payload != NULL)
			*payload = NULL;
	}

	return true;
}

/*
 * UndoRecordSetCreate - Create a new UNDO record set
 *
 * A record set accumulates multiple UNDO records before writing them
 * to the UNDO log in a batch. This improves performance by reducing
 * I/O operations.
 */
UndoRecordSet *
UndoRecordSetCreate(TransactionId xid, UndoRecPtr prev_undo_ptr)
{
	UndoRecordSet *uset;
	MemoryContext oldcontext;
	MemoryContext mctx;
	MemoryContext parent;

	/*
	 * Use the UndoContext if available (normal backend operation), otherwise
	 * fall back to CurrentMemoryContext (e.g., during early startup).
	 */
	parent = UndoContext ? UndoContext : CurrentMemoryContext;

	/* Create memory context for this record set */
	mctx = AllocSetContextCreate(parent,
								 "UNDO record set",
								 ALLOCSET_DEFAULT_SIZES);

	oldcontext = MemoryContextSwitchTo(mctx);

	uset = (UndoRecordSet *) palloc0(sizeof(UndoRecordSet));
	uset->xid = xid;
	uset->prev_undo_ptr = prev_undo_ptr;
	uset->persistence = UNDOPERSISTENCE_PERMANENT;
	uset->type = URST_TRANSACTION;
	uset->nrecords = 0;

	/* Allocate initial buffer (will grow dynamically as needed) */
	uset->buffer_capacity = 8192;	/* 8KB initial */
	uset->buffer = (char *) palloc(uset->buffer_capacity);
	uset->buffer_size = 0;

	uset->mctx = mctx;

	MemoryContextSwitchTo(oldcontext);

	return uset;
}

/*
 * UndoRecordSetFree - Free an UNDO record set
 *
 * Destroys the memory context and all associated data.
 */
void
UndoRecordSetFree(UndoRecordSet * uset)
{
	if (uset != NULL && uset->mctx != NULL)
		MemoryContextDelete(uset->mctx);
}

/*
 * UndoRecordAddTuple - Add a tuple-based UNDO record to the set
 *
 * This is the main API for adding UNDO records. The tuple data is
 * serialized and added to the record set's buffer.
 */
void
UndoRecordAddTuple(UndoRecordSet * uset,
				   uint16 record_type,
				   Relation rel,
				   BlockNumber blkno,
				   OffsetNumber offset,
				   HeapTuple oldtuple)
{
	UndoRecordHeader header;
	Size		record_size;
	Size		payload_len;
	MemoryContext oldcontext;

	if (uset == NULL)
		elog(ERROR, "cannot add UNDO record to NULL set");

	oldcontext = MemoryContextSwitchTo(uset->mctx);

	/* Calculate record size */
	record_size = UndoRecordGetSize(record_type, oldtuple);
	payload_len = (oldtuple != NULL) ? oldtuple->t_len : 0;

	/* Expand buffer if needed */
	if (uset->buffer_size + record_size > uset->buffer_capacity)
	{
		Size		new_capacity = uset->buffer_capacity * 2;

		while (new_capacity < uset->buffer_size + record_size)
			new_capacity *= 2;

		uset->buffer = (char *) repalloc(uset->buffer, new_capacity);
		uset->buffer_capacity = new_capacity;
	}

	/* Build record header */
	header.urec_type = record_type;
	header.urec_info = UNDO_INFO_XID_VALID;
	if (oldtuple != NULL)
		header.urec_info |= UNDO_INFO_HAS_TUPLE;

	header.urec_len = record_size;
	header.urec_xid = uset->xid;
	header.urec_prev = uset->prev_undo_ptr;
	header.urec_reloid = RelationGetRelid(rel);
	header.urec_blkno = blkno;
	header.urec_offset = offset;
	header.urec_payload_len = payload_len;
	header.urec_tuple_len = payload_len;
	header.urec_clr_ptr = InvalidXLogRecPtr;

	/* Serialize record into buffer */
	UndoRecordSerialize(uset->buffer + uset->buffer_size,
						&header,
						oldtuple ? (char *) oldtuple->t_data : NULL,
						payload_len);

	uset->buffer_size += record_size;
	uset->nrecords++;

	MemoryContextSwitchTo(oldcontext);
}

/*
 * UndoRecordSetGetSize - Get total size of all records in set
 */
Size
UndoRecordSetGetSize(UndoRecordSet * uset)
{
	if (uset == NULL)
		return 0;

	return uset->buffer_size;
}
