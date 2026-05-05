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
#include "access/undo_xlog.h"
#include "miscadmin.h"
#include "utils/memutils.h"

/*
 * ApplyOneUndoRecord - Apply a single UNDO record via RM dispatch
 *
 * Checks the CLR pointer to avoid double-application, then dispatches
 * to the appropriate resource manager's rm_undo callback.
 *
 * Returns true if successfully applied, false if skipped.
 */
static bool
ApplyOneUndoRecord(UndoRecordHeader * header, char *payload,
				   UndoRecPtr urec_ptr)
{
	const		UndoRmgrData *rmgr;
	UndoApplyResult result;

	/*
	 * Idempotency design note:
	 *
	 * UNDO records are immutable once written to WAL; urec_clr_ptr in the
	 * header is always InvalidXLogRecPtr and cannot be updated after the
	 * fact. Double-application is prevented by page LSN instead: each CLR
	 * (XLOG_UNDO_APPLY_RECORD) written by rm_undo bumps the heap page LSN to
	 * the CLR's EndRecPtr.  During crash recovery Phase 2, rm_undo reads the
	 * buffer via XLogReadBufferForRedo; if the page LSN >= CLR LSN (meaning
	 * the CLR was already replayed in Phase 1), the buffer read returns
	 * BLK_DONE or BLK_RESTORED and no modification is made.
	 *
	 * This function is therefore unconditionally correct to call for every
	 * UNDO record encountered during chain walking.
	 */

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
	UndoRecPtr	current_ptr pg_attribute_unused();
	char	   *read_buffer pg_attribute_unused() = NULL;
	Size		buffer_size pg_attribute_unused() = 0;
	int			records_applied pg_attribute_unused() = 0;
	int			records_skipped pg_attribute_unused() = 0;

	if (!UndoRecPtrIsValid(start_ptr))
		return;

	/*
	 * With UNDO-in-WAL, UNDO records are no longer in segment files. Use
	 * ApplyUndoChainFromWAL() instead, which reads UNDO batches from the WAL
	 * stream.
	 */
	ereport(ERROR,
			(errmsg("ApplyUndoChain is not supported with UNDO-in-WAL"),
			 errhint("Use ApplyUndoChainFromWAL() instead.")));

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

/*
 * ApplyUndoChainFromWAL - Walk and apply an UNDO chain from WAL
 *
 * This is the WAL-based equivalent of ApplyUndoChain().  Instead of
 * reading UNDO records from segment files via UndoLogRead(), it reads
 * XLOG_UNDO_BATCH WAL records via UndoReadBatchFromWAL() and iterates
 * through the serialized records within each batch.
 *
 * The chain is walked backward via the xl_undo_batch.chain_prev LSN
 * from the most recent batch to the first.  Within each batch, records
 * are applied in reverse order (newest to oldest) as required by
 * ARIES-style rollback semantics.  This is achieved by first scanning
 * forward through the serialized records to collect their start offsets,
 * then iterating the collected offsets in reverse to apply each record.
 */
void
ApplyUndoChainFromWAL(XLogRecPtr last_batch_lsn)
{
	XLogRecPtr	batch_lsn;
	int			records_applied = 0;
	int			records_skipped = 0;
	int			batches_processed = 0;

	if (!XLogRecPtrIsValid(last_batch_lsn))
		return;

	ereport(DEBUG1,
			(errmsg("applying UNDO chain from WAL starting at %X/%X",
					LSN_FORMAT_ARGS(last_batch_lsn))));

	batch_lsn = last_batch_lsn;

	while (XLogRecPtrIsValid(batch_lsn))
	{
		UndoBatchData *batch;
		char	   *pos;
		char	   *end;

		batch = UndoReadBatchFromWAL(batch_lsn);
		if (batch == NULL)
		{
			ereport(WARNING,
					(errmsg("UNDO rollback: could not read batch at %X/%X, "
							"stopping chain walk",
							LSN_FORMAT_ARGS(batch_lsn))));
			break;
		}

		batches_processed++;

		/*
		 * Walk through records within this batch in reverse order.
		 *
		 * ARIES requires that UNDO records within a batch be applied
		 * newest-first (reverse of serialization order).  We first scan
		 * forward to collect pointers to each record start, then iterate the
		 * collected pointers in reverse to apply them.
		 */
		pos = batch->payload;
		end = pos + batch->payload_len;

		{
			char	  **record_starts;
			int			nrecords_in_batch = 0;
			int			max_records = 1024; /* Large enough to avoid
											 * reallocation */
			int			i;

			/*
			 * Allocate record_starts array. We use palloc
			 * (CurrentMemoryContext) rather than TopMemoryContext because
			 * this is a short-lived allocation that's only needed for the
			 * duration of this loop iteration.
			 *
			 * We intentionally do NOT pfree this allocation when done.
			 * Calling pfree on memory allocated from a BumpContext (which
			 * executor nodes may use) would ERROR. Since this allocation is
			 * small and short-lived, it's fine to let the memory context
			 * reset reclaim it.
			 *
			 * Use a large initial size (1024) to avoid needing repalloc(),
			 * which also doesn't work with BumpContext.
			 */
			record_starts = (char **) palloc(max_records * sizeof(char *));

			/* First pass: collect record start pointers by scanning forward */
			while (pos < end)
			{
				UndoRecordHeader hdr;

				if ((Size) (end - pos) < SizeOfUndoRecordHeader)
				{
					ereport(WARNING,
							(errmsg("UNDO rollback: truncated record in batch at %X/%X",
									LSN_FORMAT_ARGS(batch_lsn))));
					break;
				}

				memcpy(&hdr, pos, SizeOfUndoRecordHeader);

				if (hdr.urec_len < SizeOfUndoRecordHeader ||
					(Size) (end - pos) < hdr.urec_len)
				{
					ereport(WARNING,
							(errmsg("UNDO rollback: invalid record size %u in batch at %X/%X",
									hdr.urec_len, LSN_FORMAT_ARGS(batch_lsn))));
					break;
				}

				/* Check if we have exceeded the fixed-size array */
				if (nrecords_in_batch >= max_records)
				{
					ereport(WARNING,
							(errmsg("UNDO rollback: batch at %X/%X contains more than %d records, "
									"cannot process all records",
									LSN_FORMAT_ARGS(batch_lsn), max_records)));
					break;
				}

				record_starts[nrecords_in_batch++] = pos;
				pos += hdr.urec_len;
			}

			/*
			 * Second pass: apply records in reverse order (newest first).
			 * Even if there was a scan error, apply whatever records we
			 * successfully collected.
			 */
			for (i = nrecords_in_batch - 1; i >= 0; i--)
			{
				UndoRecordHeader header;
				char	   *payload = NULL;

				memcpy(&header, record_starts[i], SizeOfUndoRecordHeader);

				if (header.urec_payload_len > 0)
					payload = record_starts[i] + SizeOfUndoRecordHeader;

				if (ApplyOneUndoRecord(&header, payload, InvalidUndoRecPtr))
					records_applied++;
				else
					records_skipped++;
			}

			/*
			 * pfree(record_starts); -- Commented out: BumpContext
			 * incompatibility during abort
			 */
		}

		/* Follow chain to previous batch */
		batch_lsn = batch->header.chain_prev;
		UndoFreeBatchData(batch);
	}

	/* Report results */
	if (records_skipped > 0)
	{
		ereport(WARNING,
				(errmsg("UNDO rollback from WAL: %d batches, %d records applied, "
						"%d skipped",
						batches_processed, records_applied, records_skipped)));
	}
	else
	{
		ereport(DEBUG1,
				(errmsg("UNDO rollback from WAL complete: %d batches, "
						"%d records applied",
						batches_processed, records_applied)));
	}
}
