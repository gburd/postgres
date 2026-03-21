/*-------------------------------------------------------------------------
 *
 * undorecord.c
 *	  UNDO record assembly and serialization
 *
 * This file implements the AM-agnostic UNDO record format and provides
 * functions for creating, serializing, and deserializing UNDO records.
 * All AM-specific knowledge is kept out of this module; records carry
 * opaque payloads whose interpretation is delegated to the owning RM.
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

#include "access/undorecord.h"
#include "utils/memutils.h"

/*
 * UndoRecordGetPayloadSize - Calculate size needed for an UNDO record
 *
 * This includes the fixed header plus the RM-specific payload.
 */
Size
UndoRecordGetPayloadSize(Size payload_len)
{
	return SizeOfUndoRecordHeader + payload_len;
}

/*
 * UndoRecordSerialize - Serialize an UNDO record into a buffer
 *
 * The destination buffer must be large enough to hold the entire record.
 * Use UndoRecordGetPayloadSize() to determine the required size.
 */
void
UndoRecordSerialize(char *dest, UndoRecordHeader *header,
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
 * Reads the header and sets the payload pointer into the source buffer
 * (zero-copy).  Returns true on success, false on failure.
 */
bool
UndoRecordDeserialize(const char *src, UndoRecordHeader *header,
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
	 * Use CurrentMemoryContext as the parent so that the record set is
	 * automatically freed if the caller's memory context is reset (e.g.,
	 * on transaction abort).  This avoids leaks when UndoRecordSetInsert()
	 * throws an error before UndoRecordSetFree() can run.
	 */
	parent = CurrentMemoryContext;

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
UndoRecordSetFree(UndoRecordSet *uset)
{
	if (uset != NULL && uset->mctx != NULL)
		MemoryContextDelete(uset->mctx);
}

/*
 * UndoRecordAddPayload - Add an UNDO record with opaque payload to the set
 *
 * This is the main API for adding UNDO records.  The caller provides an
 * RM ID, RM-specific info flags, a relation OID, and an opaque payload.
 * The payload's interpretation is entirely RM-specific.
 */
void
UndoRecordAddPayload(UndoRecordSet *uset,
					 uint8 rmid,
					 uint16 info,
					 Oid reloid,
					 const char *payload,
					 Size payload_len)
{
	UndoRecordHeader header;
	Size		record_size;
	MemoryContext oldcontext;

	if (uset == NULL)
		elog(ERROR, "cannot add UNDO record to NULL set");

	/* Zero the header to avoid uninitialized padding bytes */
	memset(&header, 0, sizeof(UndoRecordHeader));

	oldcontext = MemoryContextSwitchTo(uset->mctx);

	/* Calculate record size */
	record_size = UndoRecordGetPayloadSize(payload_len);

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
	header.urec_rmid = rmid;
	header.urec_flags = UNDO_INFO_XID_VALID;
	if (payload_len > 0)
		header.urec_flags |= UNDO_INFO_HAS_PAYLOAD;
	header.urec_info = info;
	header.urec_len = (uint32) record_size;
	header.urec_xid = uset->xid;
	header.urec_prev = uset->prev_undo_ptr;
	header.urec_reloid = reloid;
	header.urec_payload_len = (uint32) payload_len;
	header.urec_clr_ptr = InvalidXLogRecPtr;

	/* Serialize record into buffer */
	UndoRecordSerialize(uset->buffer + uset->buffer_size,
						&header,
						payload,
						payload_len);

	uset->buffer_size += record_size;
	uset->nrecords++;

	MemoryContextSwitchTo(oldcontext);
}

/*
 * UndoRecordSetGetSize - Get total size of all records in set
 */
Size
UndoRecordSetGetSize(UndoRecordSet *uset)
{
	if (uset == NULL)
		return 0;

	return uset->buffer_size;
}
