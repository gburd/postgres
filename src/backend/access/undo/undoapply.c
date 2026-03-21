/*-------------------------------------------------------------------------
 *
 * undoapply.c
 *	  Generic UNDO record application during transaction rollback
 *
 * When a transaction aborts, this module walks the UNDO chain backward
 * from the most recent record to the first.  For each record, it
 * dispatches to the appropriate resource manager's rm_undo callback
 * based on the urec_rmid field in the record header.
 *
 * This module is AM-agnostic: it contains no heap, nbtree, or FILEOPS
 * specific code.  All AM-specific UNDO application logic lives in the
 * respective RM modules (heapam_undo.c, nbtree_undo.c, fileops_undo.c).
 *
 * The dispatch pattern is analogous to WAL resource managers: each RM
 * registers its callbacks via RegisterUndoRmgr(), and this module
 * routes UNDO records to the correct handler.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/undo/undoapply.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/undolog.h"
#include "access/undorecord.h"
#include "access/undormgr.h"
#include "miscadmin.h"

/*
 * ApplyOneUndoRecord - Apply a single UNDO record via RM dispatch
 *
 * Checks the CLR pointer to avoid double-application, then dispatches
 * to the appropriate resource manager's rm_undo callback.
 *
 * Returns true if successfully applied, false if skipped.
 */
static bool
ApplyOneUndoRecord(UndoRecordHeader *header, char *payload,
				   UndoRecPtr urec_ptr)
{
	const UndoRmgrData *rmgr;
	UndoApplyResult result;

	/*
	 * If this UNDO record already has a CLR pointer, it was already applied
	 * during a previous rollback attempt (e.g., crash during rollback
	 * followed by recovery re-applying the UNDO chain).  Skip it to avoid
	 * double-application.
	 */
	if (XLogRecPtrIsValid(header->urec_clr_ptr))
	{
		ereport(DEBUG2,
				(errmsg("UNDO rollback: record at %llu already applied (CLR at %X/%X), skipping",
						(unsigned long long) urec_ptr,
						LSN_FORMAT_ARGS(header->urec_clr_ptr))));
		return false;
	}

	/*
	 * Look up the resource manager for this record.
	 */
	rmgr = GetUndoRmgr(header->urec_rmid);
	if (rmgr == NULL)
	{
		ereport(WARNING,
				(errmsg("UNDO rollback: unknown RM ID %u for record at %llu, skipping",
						header->urec_rmid,
						(unsigned long long) urec_ptr)));
		return false;
	}

	/*
	 * Dispatch to the RM's undo-apply callback.  The callback is responsible
	 * for all AM-specific work: opening relations, locking buffers, modifying
	 * pages, generating CLRs, and releasing resources.
	 */
	result = rmgr->rm_undo(header->urec_rmid,
						   header->urec_info,
						   header->urec_xid,
						   header->urec_reloid,
						   payload,
						   header->urec_payload_len,
						   urec_ptr);

	if (result == UNDO_APPLY_SUCCESS)
	{
		ereport(DEBUG2,
				(errmsg("UNDO rollback: applied %s record at %llu",
						rmgr->rm_name,
						(unsigned long long) urec_ptr)));
		return true;
	}
	else if (result == UNDO_APPLY_SKIPPED)
	{
		ereport(DEBUG2,
				(errmsg("UNDO rollback: skipped %s record at %llu",
						rmgr->rm_name,
						(unsigned long long) urec_ptr)));
		return false;
	}
	else
	{
		ereport(WARNING,
				(errmsg("UNDO rollback: error applying %s record at %llu",
						rmgr->rm_name,
						(unsigned long long) urec_ptr)));
		return false;
	}
}

/*
 * ApplyUndoChain - Walk and apply an UNDO chain during transaction abort
 *
 * This function reads the UNDO chain starting from 'start_ptr' and applies
 * each record in order. Records are processed from the most recent to the
 * oldest (reverse chronological order), which is the natural order for
 * rollback.
 *
 * Each record is dispatched to its owning resource manager's rm_undo
 * callback via the UNDO RM dispatch table.
 *
 * On error, we emit a WARNING and continue processing remaining records.
 * This is a best-effort approach -- we do not want UNDO failures to prevent
 * transaction abort from completing.
 */
void
ApplyUndoChain(UndoRecPtr start_ptr)
{
	UndoRecPtr	current_ptr;
	char	   *read_buffer = NULL;
	Size		buffer_size = 0;
	int			records_applied = 0;
	int			records_skipped = 0;

	if (!UndoRecPtrIsValid(start_ptr))
		return;

	ereport(DEBUG1,
			(errmsg("applying UNDO chain starting at %llu",
					(unsigned long long) start_ptr)));

	current_ptr = start_ptr;

	/* Process each UNDO record in the chain */
	while (UndoRecPtrIsValid(current_ptr))
	{
		UndoRecordHeader header;
		char	   *payload = NULL;
		Size		record_size;

		/*
		 * Read the fixed header first to determine the full record size.
		 */
		if (buffer_size < SizeOfUndoRecordHeader)
		{
			buffer_size = Max(SizeOfUndoRecordHeader + 8192, buffer_size * 2);
			if (read_buffer)
				pfree(read_buffer);
			read_buffer = (char *) palloc(buffer_size);
		}

		UndoLogRead(current_ptr, read_buffer, SizeOfUndoRecordHeader);
		memcpy(&header, read_buffer, SizeOfUndoRecordHeader);

		record_size = header.urec_len;

		/*
		 * Sanity check: record size should be at least the header size and
		 * not absurdly large.
		 */
		if (record_size < SizeOfUndoRecordHeader ||
			record_size > 1024 * 1024 * 1024)
		{
			ereport(WARNING,
					(errmsg("UNDO rollback: invalid record size %zu at %llu, stopping chain walk",
							record_size, (unsigned long long) current_ptr)));
			break;
		}

		/* Read the full record if it contains payload data */
		if (record_size > SizeOfUndoRecordHeader)
		{
			if (buffer_size < record_size)
			{
				buffer_size = record_size;
				pfree(read_buffer);
				read_buffer = (char *) palloc(buffer_size);
			}

			UndoLogRead(current_ptr, read_buffer, record_size);

			/* Re-read header from full buffer */
			memcpy(&header, read_buffer, SizeOfUndoRecordHeader);

			/*
			 * Payload data follows immediately after the fixed header in the
			 * serialized record.
			 */
			if (header.urec_payload_len > 0)
				payload = read_buffer + SizeOfUndoRecordHeader;
		}

		/* Apply this record via RM dispatch */
		if (ApplyOneUndoRecord(&header, payload, current_ptr))
			records_applied++;
		else
			records_skipped++;

		/*
		 * Follow the chain to the previous record.
		 */
		current_ptr = header.urec_prev;
	}

	if (read_buffer)
		pfree(read_buffer);

	/* Report results */
	if (records_skipped > 0)
	{
		ereport(WARNING,
				(errmsg("UNDO rollback: %d records applied, %d skipped",
						records_applied, records_skipped)));
	}
	else
	{
		ereport(DEBUG1,
				(errmsg("UNDO rollback complete: %d records applied",
						records_applied)));
	}
}
