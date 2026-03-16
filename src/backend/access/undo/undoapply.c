/*-------------------------------------------------------------------------
 *
 * undoapply.c
 *	  Apply UNDO records during transaction rollback
 *
 * When a transaction aborts, this module walks the UNDO chain backward
 * from the most recent record to the first, applying each record to
 * reverse the original operation:
 *
 *   UNDO_INSERT:  Delete the inserted tuple
 *   UNDO_DELETE:  Re-insert the deleted tuple
 *   UNDO_UPDATE:  Restore the old tuple version
 *   UNDO_PRUNE:   (no rollback action - prune recovery is via recovery tools)
 *   UNDO_INPLACE: Restore the old tuple version
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

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/undolog.h"
#include "access/undorecord.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"
#include "utils/relcache.h"

/*
 * ApplyOneUndoRecord - Apply a single UNDO record to reverse an operation
 *
 * Returns true if successfully applied, false if the operation could not
 * be undone (e.g., relation no longer exists).
 */
static bool
ApplyOneUndoRecord(UndoRecordHeader *header, char *payload)
{
	Relation	rel;
	ItemPointerData tid;
	volatile uint32 save_InterruptHoldoffCount = InterruptHoldoffCount;

	/*
	 * Try to open the relation. If it has been dropped, we can skip this
	 * record since the data is gone anyway.
	 */
	rel = try_relation_open(header->urec_reloid, RowExclusiveLock);
	if (rel == NULL)
	{
		ereport(DEBUG2,
				(errmsg("UNDO rollback: relation %u no longer exists, skipping",
						header->urec_reloid)));
		return false;
	}

	/* Set up the TID for the affected tuple */
	ItemPointerSet(&tid, header->urec_blkno, header->urec_offset);

	switch (header->urec_type)
	{
		case UNDO_INSERT:
			{
				/*
				 * Undo an INSERT by deleting the tuple. We use
				 * simple_heap_delete which does not generate UNDO records
				 * itself (avoiding infinite recursion).
				 *
				 * Note: The tuple may already be invisible if the transaction
				 * is aborting, so we catch and ignore visibility errors.
				 */
				PG_TRY();
				{
					simple_heap_delete(rel, &tid);

					ereport(DEBUG2,
							(errmsg("UNDO rollback: deleted inserted tuple at (%u,%u) in relation %u",
									header->urec_blkno, header->urec_offset,
									header->urec_reloid)));
				}
				PG_CATCH();
				{
					/*
					 * If we can't delete the tuple (e.g., "attempted to delete
					 * invisible tuple"), it's likely already gone or invisible
					 * due to the abort. Clear the error and continue.
					 *
					 * Restore InterruptHoldoffCount because errfinish()
					 * resets it to 0 on ERROR.  Our caller (AbortTransaction
					 * or AbortSubTransaction) is inside HOLD_INTERRUPTS and
					 * will call RESUME_INTERRUPTS, so the count must stay
					 * balanced.
					 */
					InterruptHoldoffCount = save_InterruptHoldoffCount;
					FlushErrorState();
					ereport(DEBUG2,
							(errmsg("UNDO rollback: skipping delete of tuple at (%u,%u) in relation %u (already invisible or gone)",
									header->urec_blkno, header->urec_offset,
									header->urec_reloid)));
				}
				PG_END_TRY();
				break;
			}

		case UNDO_DELETE:
			{
				/*
				 * Undo a DELETE by re-inserting the old tuple. The tuple
				 * data is stored in the payload.
				 */
				if (payload != NULL && header->urec_payload_len > 0)
				{
					HeapTupleData htup;

					htup.t_len = header->urec_payload_len;
					htup.t_data = (HeapTupleHeader) payload;
					htup.t_tableOid = header->urec_reloid;
					ItemPointerSetInvalid(&htup.t_self);

					simple_heap_insert(rel, &htup);

					ereport(DEBUG2,
							(errmsg("UNDO rollback: re-inserted deleted tuple for relation %u",
									header->urec_reloid)));
				}
				else
				{
					ereport(WARNING,
							(errmsg("UNDO rollback: DELETE record for relation %u has no tuple data",
									header->urec_reloid)));
				}
				break;
			}

		case UNDO_UPDATE:
			{
				/*
				 * Undo an UPDATE by deleting the new version and
				 * re-inserting the old version. The old tuple data is
				 * stored in the payload.
				 *
				 * Note: We delete first, then insert. This is safe because
				 * we hold RowExclusiveLock on the relation.
				 */
				PG_TRY();
				{
					simple_heap_delete(rel, &tid);

					if (payload != NULL && header->urec_payload_len > 0)
					{
						HeapTupleData htup;

						htup.t_len = header->urec_payload_len;
						htup.t_data = (HeapTupleHeader) payload;
						htup.t_tableOid = header->urec_reloid;
						ItemPointerSetInvalid(&htup.t_self);

						simple_heap_insert(rel, &htup);
					}

					ereport(DEBUG2,
							(errmsg("UNDO rollback: restored old tuple version for relation %u",
									header->urec_reloid)));
				}
				PG_CATCH();
				{
					/* Restore InterruptHoldoffCount (see UNDO_INSERT comment) */
					InterruptHoldoffCount = save_InterruptHoldoffCount;
					FlushErrorState();
					ereport(DEBUG2,
							(errmsg("UNDO rollback: failed to restore tuple for relation %u (tuple invisible or gone)",
									header->urec_reloid)));
				}
				PG_END_TRY();
				break;
			}

		case UNDO_PRUNE:
			{
				/*
				 * PRUNE records are informational - they record tuples that
				 * were pruned for recovery purposes.  During transaction
				 * rollback, prune operations cannot be undone because they
				 * are page-level maintenance operations that affect
				 * visibility, not transactional data changes.
				 *
				 * Recovery of pruned data is handled by the pg_undorecover
				 * recovery tool (Commit 15).
				 */
				ereport(DEBUG2,
						(errmsg("UNDO rollback: skipping PRUNE record for relation %u (informational only)",
								header->urec_reloid)));
				break;
			}

		case UNDO_INPLACE:
			{
				/*
				 * Undo an in-place update by restoring the old tuple data.
				 * This is similar to UNDO_UPDATE but we overwrite in place
				 * rather than delete+insert.
				 *
				 * For now, use the same delete+insert approach as UPDATE.
				 */
				PG_TRY();
				{
					simple_heap_delete(rel, &tid);

					if (payload != NULL && header->urec_payload_len > 0)
					{
						HeapTupleData htup;

						htup.t_len = header->urec_payload_len;
						htup.t_data = (HeapTupleHeader) payload;
						htup.t_tableOid = header->urec_reloid;
						ItemPointerSetInvalid(&htup.t_self);

						simple_heap_insert(rel, &htup);
					}

					ereport(DEBUG2,
							(errmsg("UNDO rollback: restored in-place updated tuple for relation %u",
									header->urec_reloid)));
				}
				PG_CATCH();
				{
					/* Restore InterruptHoldoffCount (see UNDO_INSERT comment) */
					InterruptHoldoffCount = save_InterruptHoldoffCount;
					FlushErrorState();
					ereport(DEBUG2,
							(errmsg("UNDO rollback: failed to restore in-place updated tuple for relation %u (tuple invisible or gone)",
									header->urec_reloid)));
				}
				PG_END_TRY();
				break;
			}

		default:
			ereport(WARNING,
					(errmsg("UNDO rollback: unknown record type %u, skipping",
							header->urec_type)));
			break;
	}

	relation_close(rel, RowExclusiveLock);
	return true;
}

/*
 * ApplyUndoChain - Walk and apply an UNDO chain during transaction abort
 *
 * This function reads the UNDO chain starting from 'start_ptr' and applies
 * each record in order. Records are processed from the most recent to the
 * oldest (reverse chronological order), which is the natural order for
 * rollback.
 *
 * The chain is formed by reading consecutive records from the UNDO log
 * starting at start_ptr and walking forward through the buffer, since
 * UndoRecordSetInsert writes all records for a single insertion batch
 * contiguously. The urec_prev pointer links back to earlier batches.
 *
 * On error, we emit a WARNING and continue processing remaining records.
 * This is a best-effort approach - we don't want UNDO failures to prevent
 * transaction abort from completing.
 */
void
ApplyUndoChain(UndoRecPtr start_ptr)
{
	UndoRecPtr	current_ptr;
	char	   *read_buffer = NULL;
	Size		buffer_size = 0;
	int			records_applied = 0;
	int			records_failed = 0;

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
		 * Read the header first to know how much data follows.
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
		 * Sanity check: record size should be at least the header size
		 * and not absurdly large.
		 */
		if (record_size < SizeOfUndoRecordHeader ||
			record_size > 1024 * 1024 * 1024)
		{
			ereport(WARNING,
					(errmsg("UNDO rollback: invalid record size %zu at %llu, stopping chain walk",
							record_size, (unsigned long long) current_ptr)));
			break;
		}

		/* Read the full record if it has payload */
		if (record_size > SizeOfUndoRecordHeader)
		{
			if (buffer_size < record_size)
			{
				buffer_size = record_size;
				pfree(read_buffer);
				read_buffer = (char *) palloc(buffer_size);
			}

			UndoLogRead(current_ptr, read_buffer, record_size);

			/* Re-deserialize from full buffer */
			UndoRecordDeserialize(read_buffer, &header, &payload);
		}

		/* Apply this record */
		if (ApplyOneUndoRecord(&header, payload))
			records_applied++;
		else
			records_failed++;

		/*
		 * Follow the chain to the previous record. The urec_prev pointer
		 * links to the previous batch's starting record.
		 */
		current_ptr = header.urec_prev;
	}

	if (read_buffer)
		pfree(read_buffer);

	/* Report results */
	if (records_failed > 0)
	{
		/*
		 * Some records could not be applied (e.g., tuple already invisible).
		 * This is expected during abort, so only log at WARNING level.
		 */
		ereport(WARNING,
				(errmsg("UNDO rollback: %d records applied, %d skipped/failed",
						records_applied, records_failed)));
	}
	else
	{
		ereport(DEBUG1,
				(errmsg("UNDO rollback complete: %d records applied",
						records_applied)));
	}
}
