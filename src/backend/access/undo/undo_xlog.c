/*-------------------------------------------------------------------------
 *
 * undo_xlog.c
 *	  UNDO resource manager WAL redo routines
 *
 * This module implements the WAL redo callback for the RM_UNDO_ID resource
 * manager.  It handles replay of:
 *
 *   XLOG_UNDO_ROLLBACK       - Reapply a crash loser's rollback
 *   XLOG_UNDO_BATCH          - Track a transaction's UNDO for recovery
 *   XLOG_UNDO_INFLIGHT       - Track UNDO chains in flight at a checkpoint
 *
 * It also holds the recovery undo phase (UndoRecoveryRollBack), which rolls
 * back the transactions a crash left in progress, and the code that reads
 * UNDO batches back from WAL.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/undo/undo_xlog.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/transam.h"
#include "access/twophase.h"
#include "access/undo_xlog.h"
#include "access/undolog.h"
#include "access/undorecord.h"
#include "access/undormgr.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "access/xlogreader.h"
#include "access/xlogrecovery.h"
#include "access/xlogutils.h"
#include "miscadmin.h"
#include "utils/memutils.h"

static int	undo_apply_records(TransactionId xid, const char *pos, Size len,
							   int *failed, StringInfo done);

/*
 * undo_redo - Replay an UNDO WAL record during crash recovery
 */
void
undo_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
		case XLOG_UNDO_ROLLBACK:
			{
				xl_undo_rollback *xlrec = (xl_undo_rollback *) XLogRecGetData(record);

				(void) undo_apply_records(xlrec->xid,
										  XLogRecGetData(record) + SizeOfUndoRollback,
										  XLogRecGetDataLen(record) - SizeOfUndoRollback,
										  NULL, NULL);
				UndoRecoveryRemoveXid(xlrec->xid);
			}
			break;

		case XLOG_UNDO_INFLIGHT:
			{
				xl_undo_inflight *xlrec = (xl_undo_inflight *) XLogRecGetData(record);

				for (uint32 i = 0; i < xlrec->nxacts; i++)
					UndoRecoveryTrackInflight(&xlrec->xacts[i]);
			}
			break;

		case XLOG_UNDO_BATCH:
			{
				xl_undo_batch *xlrec = (xl_undo_batch *) XLogRecGetData(record);

				/*
				 * During recovery, track this batch for incomplete
				 * transaction detection.  After redo completes, any
				 * transaction that wrote UNDO batches but did not commit will
				 * need its UNDO chain walked for rollback.
				 *
				 * The batch payload (serialized UNDO records) is part of the
				 * WAL record and can be re-read later via XLogReadRecord()
				 * during the undo phase.
				 */
				UndoRecoveryTrackBatch(xlrec->xid, record->ReadRecPtr,
									   xlrec->chain_prev,
									   xlrec->persistence);

				ereport(DEBUG2,
						(errmsg("undo_redo: BATCH xid %u, nrecords %u, "
								"total_len %u, chain_prev %X/%08X",
								xlrec->xid, xlrec->nrecords,
								xlrec->total_len,
								LSN_FORMAT_ARGS(xlrec->chain_prev))));
			}
			break;

		default:
			elog(PANIC, "undo_redo: unknown op code %u", info);
	}
}

/* ----------------------------------------------------------------
 *	UNDO recovery tracking
 *
 *	During WAL redo, we track which transactions wrote UNDO batches.
 *	When a commit/abort record is redone, the XID is removed.
 *	After redo completes, remaining entries represent incomplete
 *	transactions that need their UNDO chains walked.
 * ----------------------------------------------------------------
 */

/* Hash table entry for tracking incomplete UNDO transactions */
typedef struct UndoRecoveryEntry
{
	TransactionId xid;			/* hash key */
	XLogRecPtr	last_batch_lsn[NUndoPersistenceLevels]; /* chain heads per
														 * persistence level */
	char		status;			/* in use */
} UndoRecoveryEntry;

/* Simple dynamic array for recovery tracking (used during startup only) */
static UndoRecoveryEntry *undo_recovery_entries = NULL;
static int	undo_recovery_nentries = 0;
static int	undo_recovery_capacity = 0;

/*
 * Safety cap to prevent OOM during recovery.  If more than this many
 * distinct in-flight XIDs are found in WAL at crash time, we stop
 * tracking new ones and log a warning.  The untracked transactions will
 * need manual resolution (e.g. via pg_resetwal or targeted UNDO apply).
 *
 * 1 million entries x ~40 bytes each ~ 40 MB, which is reasonable for
 * a recovery-only allocation.
 */
#define UNDO_RECOVERY_MAX_ENTRIES	1048576

/*
 * undo_recovery_entry - Find or create the recovery entry for xid
 *
 * Returns NULL if the entry would exceed UNDO_RECOVERY_MAX_ENTRIES.
 */
static UndoRecoveryEntry *
undo_recovery_entry(TransactionId xid)
{
	int			i;
	UndoRecoveryEntry *entry = NULL;

	/* Find existing entry for this XID */
	for (i = 0; i < undo_recovery_nentries; i++)
	{
		if (undo_recovery_entries[i].xid == xid)
			return &undo_recovery_entries[i];
	}

	/* Safety cap: refuse to track more XIDs to prevent OOM */
	if (undo_recovery_nentries >= UNDO_RECOVERY_MAX_ENTRIES)
	{
		static bool warned = false;

		if (!warned)
		{
			ereport(WARNING,
					(errmsg("UNDO recovery: reached maximum tracked transaction limit (%d)",
							UNDO_RECOVERY_MAX_ENTRIES),
					 errhint("Transactions beyond this limit will not be automatically rolled back. "
							 "Manual intervention may be required after recovery completes.")));
			warned = true;
		}
		return NULL;
	}

	if (undo_recovery_nentries >= undo_recovery_capacity)
	{
		int			new_capacity = (undo_recovery_capacity == 0) ? 64 :
			undo_recovery_capacity * 2;
		UndoRecoveryEntry *new_entries;

		/* Clamp doubling to not exceed the safety cap */
		if (new_capacity > UNDO_RECOVERY_MAX_ENTRIES)
			new_capacity = UNDO_RECOVERY_MAX_ENTRIES;

		if (undo_recovery_entries == NULL)
		{
			new_entries = (UndoRecoveryEntry *)
				palloc0(sizeof(UndoRecoveryEntry) * new_capacity);
		}
		else
		{
			new_entries = (UndoRecoveryEntry *)
				repalloc(undo_recovery_entries,
						 sizeof(UndoRecoveryEntry) * new_capacity);
			memset(&new_entries[undo_recovery_capacity], 0,
				   sizeof(UndoRecoveryEntry) * (new_capacity - undo_recovery_capacity));
		}
		undo_recovery_entries = new_entries;
		undo_recovery_capacity = new_capacity;
	}

	entry = &undo_recovery_entries[undo_recovery_nentries++];
	entry->xid = xid;
	for (i = 0; i < NUndoPersistenceLevels; i++)
		entry->last_batch_lsn[i] = InvalidXLogRecPtr;

	return entry;
}

/*
 * UndoRecoveryTrackBatch - Record an UNDO batch during WAL redo
 *
 * Called from the XLOG_UNDO_BATCH redo handler to track which
 * transactions have UNDO data that may need rollback.
 */
void
UndoRecoveryTrackBatch(TransactionId xid, XLogRecPtr batch_lsn,
					   XLogRecPtr chain_prev,
					   UndoPersistenceLevel persistence)
{
	UndoRecoveryEntry *entry;

	if (!TransactionIdIsValid(xid))
		return;
	entry = undo_recovery_entry(xid);
	if (entry == NULL)
		return;

	/* Update the chain head for this persistence level */
	if (persistence < NUndoPersistenceLevels)
		entry->last_batch_lsn[persistence] = batch_lsn;
}

/*
 * UndoRecoveryTrackInflight - Record a chain listed by XLOG_UNDO_INFLIGHT
 *
 * The checkpoint that wrote the record saw the transaction in flight.  Redo
 * started at its redo pointer, so batches the transaction wrote before it
 * were not replayed and the entry may not exist yet.  A batch already seen
 * by redo is at least as new as the one listed, so keep the later head.  A
 * transaction whose commit or abort precedes the record was listed after it
 * ended; its outcome is already in pg_xact, so it is ignored.
 */
void
UndoRecoveryTrackInflight(const xl_undo_inflight_xact *x)
{
	UndoRecoveryEntry *entry;

	if (!TransactionIdIsValid(x->xid) ||
		TransactionIdDidCommit(x->xid) || TransactionIdDidAbort(x->xid))
		return;
	entry = undo_recovery_entry(x->xid);
	if (entry == NULL)
		return;
	for (int i = 0; i < NUndoPersistenceLevels; i++)
	{
		if (XLogRecPtrIsValid(x->last_batch_lsn[i]) &&
			(!XLogRecPtrIsValid(entry->last_batch_lsn[i]) ||
			 x->last_batch_lsn[i] > entry->last_batch_lsn[i]))
			entry->last_batch_lsn[i] = x->last_batch_lsn[i];
	}
}

/*
 * UndoRecoveryRemoveXid - Remove an XID from recovery tracking
 *
 * Called when a commit or abort record is redone during recovery.
 * Committed transactions don't need UNDO rollback.  Aborted transactions
 * that were already fully rolled back (abort record present) also don't
 * need further work.
 */
void
UndoRecoveryRemoveXid(TransactionId xid)
{
	int			i;

	if (!TransactionIdIsValid(xid))
		return;

	for (i = 0; i < undo_recovery_nentries; i++)
	{
		if (undo_recovery_entries[i].xid == xid)
		{
			/* Mark as removed by zeroing XID */
			undo_recovery_entries[i].xid = InvalidTransactionId;
			break;
		}
	}
}

/*
 * UndoRecoveryNeeded - Check if there are incomplete transactions needing UNDO
 *
 * Returns true if any tracked transactions remain after redo is complete.
 */
static bool
UndoRecoveryNeeded(void)
{
	int			i;

	for (i = 0; i < undo_recovery_nentries; i++)
	{
		if (TransactionIdIsValid(undo_recovery_entries[i].xid))
			return true;
	}

	return false;
}

/*
 * UndoRecoveryRollingBack is true while UndoRecoveryRollBack() applies UNDO
 * records.  That runs after InRecovery has been cleared, but an object an
 * UNDO record describes may have been recreated by redo, as during redo
 * itself, so a resource manager that tests InRecovery should test this too.
 */
bool		UndoRecoveryRollingBack = false;

/*
 * undo_apply_records - Apply a packed stream of UNDO records, in order
 *
 * Shared by the undo phase and by redo of XLOG_UNDO_ROLLBACK, so the two
 * apply the same records the same way.  The stream is the payload format of
 * XLOG_UNDO_BATCH: each record is an UndoRecordHeader and its payload,
 * urec_len bytes in all.  Returns the number of records applied.  If failed
 * is not NULL, the number of records whose apply function reported an error
 * is added to it, and if done is not NULL, each record that applied is
 * appended to it.
 */
static int
undo_apply_records(TransactionId xid, const char *pos, Size len,
				   int *failed, StringInfo done)
{
	const char *end = pos + len;
	int			applied = 0;

	while ((Size) (end - pos) >= SizeOfUndoRecordHeader)
	{
		UndoRecordHeader header;
		const UndoRmgrData *rmgr;
		UndoApplyResult result;

		memcpy(&header, pos, SizeOfUndoRecordHeader);
		if (header.urec_len < SizeOfUndoRecordHeader ||
			(Size) (end - pos) < header.urec_len ||
			header.urec_payload_len > header.urec_len - SizeOfUndoRecordHeader)
			break;

		rmgr = GetUndoRmgr(header.urec_rmid);
		if (rmgr != NULL)
		{
			result = rmgr->rm_undo(header.urec_rmid, header.urec_info, xid,
								   header.urec_reloid,
								   header.urec_payload_len > 0 ?
								   pos + SizeOfUndoRecordHeader : NULL,
								   header.urec_payload_len,
								   InvalidUndoRecPtr);
			if (result == UNDO_APPLY_ERROR)
			{
				if (failed != NULL)
					(*failed)++;
			}
			else
			{
				applied++;
				if (done != NULL)
					appendBinaryStringInfo(done, pos, header.urec_len);
			}
		}
		pos += header.urec_len;
	}

	return applied;
}

/*
 * undo_collect_chain - Gather one persistence level of a loser's UNDO chain
 *
 * Walks the chain from its newest batch back to its first and appends each
 * batch's records to buf, newest batch first, so that applying buf in order
 * applies every record newest first, as an online abort does.
 *
 * Returns false, having warned, if a batch could not be read: buf then holds
 * the records of the newer batches only.
 */
static bool
undo_collect_chain(TransactionId xid, XLogRecPtr batch_lsn, StringInfo buf)
{
	int			maxstarts = 64;
	const char **starts = palloc_array(const char *, maxstarts);

	while (XLogRecPtrIsValid(batch_lsn))
	{
		int			nstarts;
		UndoBatchData *batch;
		XLogRecPtr	next_lsn;
		const char *pos;
		const char *end;

		batch = UndoReadBatchFromWAL(batch_lsn);
		if (batch == NULL)
		{
			/*
			 * The batch's WAL is gone or unreadable, for example because a
			 * restartpoint on a standby removed the segment it was in.  Skip
			 * the rest of the transaction's rollback rather than PANIC, which
			 * would leave the cluster unable to start.
			 */
			ereport(WARNING,
					(errmsg("UNDO recovery: could not read batch at %X/%08X for transaction %u; skipping rollback",
							LSN_FORMAT_ARGS(batch_lsn), xid)));
			pfree(starts);
			return false;
		}

		/*
		 * Find the whole records, then copy them newest first.  A batch holds
		 * them in the order they were written.
		 */
		pos = batch->payload;
		end = pos + batch->payload_len;
		nstarts = 0;
		while ((Size) (end - pos) >= SizeOfUndoRecordHeader)
		{
			UndoRecordHeader header;

			memcpy(&header, pos, SizeOfUndoRecordHeader);
			if (header.urec_len < SizeOfUndoRecordHeader ||
				(Size) (end - pos) < header.urec_len)
				break;
			if (nstarts == maxstarts)
			{
				maxstarts *= 2;
				starts = repalloc_array(starts, const char *, maxstarts);
			}
			starts[nstarts++] = pos;
			pos += header.urec_len;
		}
		for (int i = nstarts - 1; i >= 0; i--)
		{
			UndoRecordHeader header;

			memcpy(&header, starts[i], SizeOfUndoRecordHeader);
			appendBinaryStringInfo(buf, starts[i], header.urec_len);
		}

		/*
		 * chain_prev must point strictly backward, or the walk would never
		 * end.
		 */
		next_lsn = batch->header.chain_prev;
		if (XLogRecPtrIsValid(next_lsn) && next_lsn >= batch_lsn)
			ereport(PANIC,
					(errmsg("UNDO recovery: chain_prev %X/%08X >= batch_lsn %X/%08X for transaction %u; corrupt UNDO chain",
							LSN_FORMAT_ARGS(next_lsn),
							LSN_FORMAT_ARGS(batch_lsn), xid)));
		UndoFreeBatchData(batch);
		batch_lsn = next_lsn;
	}

	pfree(starts);
	return true;
}

/*
 * UndoRecoveryRollBack - Roll back the transactions a crash left in progress
 *
 * This is the undo phase.  StartupXLOG() calls it once WAL can be written
 * again, where resource managers may write cleanup records, just before the
 * end-of-recovery checkpoint or record.  For each transaction that redo found
 * writing UNDO records but neither committing nor aborting, other than a
 * prepared one, it applies the records, newest first, then writes an
 * XLOG_UNDO_ROLLBACK record carrying the ones that applied, and forgets the
 * transaction.  A standby and any later recovery replay that record instead
 * of reading the chain from WAL; see xl_undo_rollback.
 *
 * TEMP and UNLOGGED records are not applied: temporary relations do not
 * survive a restart, and unlogged ones are reset to their init forks.
 *
 * A record whose apply function fails is left out of the rollback record,
 * so the change it describes stays in place everywhere and no later replay
 * retries it.  The same goes for the records of a batch that could not be
 * read, and of the batches older than it.  The summary at the end says how
 * many records could not be applied.
 */
void
UndoRecoveryRollBack(void)
{
	int			total_xacts = 0;
	int			total_records = 0;
	int			failed_records = 0;
	int			incomplete_xacts = 0;
	int			pending_xacts = 0;

	if (!UndoRecoveryNeeded())
		return;
	for (int i = 0; i < undo_recovery_nentries; i++)
	{
		if (TransactionIdIsValid(undo_recovery_entries[i].xid))
			pending_xacts++;
	}

	ereport(LOG,
			(errmsg("UNDO recovery: %d incomplete transaction(s) to roll back",
					pending_xacts)));

	for (int i = 0; i < undo_recovery_nentries; i++)
	{
		UndoRecoveryEntry *entry = &undo_recovery_entries[i];
		xl_undo_rollback xlrec;
		StringInfoData buf;
		StringInfoData done;
		int			nrecords;

		if (!TransactionIdIsValid(entry->xid))
			continue;

		/*
		 * A prepared transaction stays prepared; COMMIT PREPARED or ROLLBACK
		 * PREPARED settles it later.
		 */
		if (RecoveryTransactionIdIsPrepared(entry->xid))
		{
			ereport(LOG,
					(errmsg("UNDO recovery: skipping prepared transaction %u "
							"(will remain in prepared state)",
							entry->xid)));
			continue;
		}

		total_xacts++;
		ereport(LOG,
				(errmsg("UNDO recovery: rolling back transaction %u",
						entry->xid)));

		/*
		 * FILEOPS caps the UNDO a transaction can write (see
		 * FILEOPS_MAX_ABORT_BYTES), which keeps this record far below the WAL
		 * record size limit.
		 */
		initStringInfo(&buf);
		initStringInfo(&done);
		if (!undo_collect_chain(entry->xid,
								entry->last_batch_lsn[UNDOPERSISTENCE_PERMANENT],
								&buf))
			incomplete_xacts++;

		UndoRecoveryRollingBack = true;
		nrecords = undo_apply_records(entry->xid, buf.data, buf.len,
									  &failed_records, &done);
		UndoRecoveryRollingBack = false;
		total_records += nrecords;

		/*
		 * Log the rollback, and only then forget the transaction.  The apply
		 * functions made their changes durable, and a crash before this
		 * record reaches disk leaves the loser to be rolled back again.  The
		 * end-of-recovery checkpoint or record flushes it.  Write it even if
		 * nothing applied, so that a standby forgets the loser too.
		 */
		xlrec.xid = entry->xid;
		xlrec.nrecords = nrecords;
		XLogBeginInsert();
		XLogRegisterData(&xlrec, SizeOfUndoRollback);
		XLogRegisterData(done.data, done.len);
		(void) XLogInsert(RM_UNDO_ID, XLOG_UNDO_ROLLBACK);

		pfree(buf.data);
		pfree(done.data);
		entry->xid = InvalidTransactionId;
	}

	if (failed_records > 0 || incomplete_xacts > 0)
		ereport(WARNING,
				(errmsg("UNDO recovery complete: %d transactions processed, "
						"%d records applied, %d records could not be applied",
						total_xacts, total_records, failed_records),
				 errdetail("The changes those records describe were not "
						   "rolled back.")));
	else if (total_xacts > 0)
		ereport(LOG,
				(errmsg("UNDO recovery complete: %d transactions rolled back, "
						"%d records applied",
						total_xacts, total_records)));

	/* Only prepared transactions are left, and nothing tracks them now. */
	pfree(undo_recovery_entries);
	undo_recovery_entries = NULL;
	undo_recovery_nentries = 0;
	undo_recovery_capacity = 0;
}

/* ----------------------------------------------------------------
 *	UNDO batch reading from WAL
 * ----------------------------------------------------------------
 */

/*
 * UndoReadBatchFromWAL - Read a single XLOG_UNDO_BATCH record from WAL
 *
 * Uses XLogReader to read the WAL record at the given LSN.
 * Returns a palloc'd UndoBatchData containing the header and a copy
 * of the payload.  The caller must pfree via UndoFreeBatchData().
 *
 * Returns NULL if the record cannot be read or is not an UNDO batch.
 */
/*
 * Module-level cached XLogReader for UndoReadBatchFromWAL.
 * Allocated once and reused across calls to avoid per-batch
 * open/close overhead on WAL segment files during rollback.
 */
static XLogReaderState *undo_batch_reader = NULL;
static XLogReaderRoutine undo_batch_reader_routine = {
	.page_read = read_local_xlog_page,
	.segment_open = wal_segment_open,
	.segment_close = wal_segment_close,
};

/*
 * UndoValidateBatchLSN
 *		Quick check that the WAL record at batch_lsn is a valid UNDO source.
 *
 * Returns true if the record is a standalone RM_UNDO_ID batch.  Returns
 * false for any other unrecognized record type.  Used by the inline UNDO
 * path to avoid calling ApplyUndoChainFromWAL on a batch_lsn that points to
 * the wrong record type.
 */
bool
UndoValidateBatchLSN(XLogRecPtr batch_lsn)
{
	XLogRecord *record_hdr;
	char	   *errormsg = NULL;
	uint8		rmid;
	MemoryContext old_ctx;
	bool		result = false;

	if (!XLogRecPtrIsValid(batch_lsn))
		return false;

	/*
	 * Perform the entire read under TopMemoryContext.  The cached reader and,
	 * critically, its lazily-allocated decode_buffer (xlogreader.c) are
	 * palloc'd in CurrentMemoryContext.  Callers such as the inline-abort
	 * path run with a transient context that is deleted right after;
	 * allocating the reader state there would leave the static
	 * undo_batch_reader (and its decode buffer) dangling, crashing the next
	 * abort.  TopMemoryContext makes the cache truly persistent.
	 */
	old_ctx = MemoryContextSwitchTo(TopMemoryContext);

	if (undo_batch_reader == NULL)
	{
		undo_batch_reader = XLogReaderAllocate(wal_segment_size, NULL,
											   &undo_batch_reader_routine, NULL);
		if (undo_batch_reader == NULL)
		{
			MemoryContextSwitchTo(old_ctx);
			return false;
		}
	}

	XLogBeginRead(undo_batch_reader, batch_lsn);
	record_hdr = XLogReadRecord(undo_batch_reader, &errormsg);
	if (record_hdr == NULL)
	{
		MemoryContextSwitchTo(old_ctx);
		return false;
	}

	rmid = XLogRecGetRmid(undo_batch_reader);

	/* Only a standalone UNDO batch is a valid inline UNDO source */
	if (rmid == RM_UNDO_ID)
		result = true;

	MemoryContextSwitchTo(old_ctx);
	return result;
}

UndoBatchData *
UndoReadBatchFromWAL(XLogRecPtr batch_lsn)
{
	XLogRecord *record_hdr;
	char	   *errormsg = NULL;
	UndoBatchData *result;
	xl_undo_batch *xlrec;
	char	   *record_data;
	Size		record_len;
	Size		payload_offset;

	if (!XLogRecPtrIsValid(batch_lsn))
		return NULL;

	/*
	 * Safety check: verify the WAL segment containing this LSN has not been
	 * recycled by a checkpoint.  If the LSN is behind the current redo
	 * pointer and the segment file doesn't exist, reading would cause SIGBUS
	 * (signal 10: Bus error) or SIGSEGV.  Return NULL gracefully instead.
	 *
	 * Compare against GetRedoRecPtr(); if our target is well behind the redo
	 * pointer AND behind the last checkpoint's redo location, the segment may
	 * have been recycled.
	 */
	{
		XLogRecPtr	redo_ptr = GetRedoRecPtr();
		XLogSegNo	target_segno;
		char		path[MAXPGPATH];
		TimeLineID	tli;

		if (batch_lsn < redo_ptr)
		{
			/*
			 * Determine the timeline for the segment path.
			 * GetWALInsertionTimeLine() is only valid once recovery is DONE
			 * (it asserts RECOVERY_STATE_DONE); during the crash-recovery
			 * undo phase we must instead use the replay timeline.
			 * RecoveryInProgress() distinguishes the two callers
			 * (crash-recovery undo phase vs. online abort).
			 */
			if (RecoveryInProgress())
			{
				(void) GetXLogReplayRecPtr(&tli);
				if (tli == 0)
					tli = 1;	/* no replay position yet: default to TLI 1 */
			}
			else
				tli = GetWALInsertionTimeLine();

			XLByteToSeg(batch_lsn, target_segno, wal_segment_size);
			XLogFilePath(path, tli, target_segno,
						 wal_segment_size);

			if (access(path, F_OK) != 0)
			{
				ereport(WARNING,
						(errmsg("UNDO batch at %X/%08X: WAL segment \"%s\" no longer "
								"exists (recycled by checkpoint); skipping rollback",
								LSN_FORMAT_ARGS(batch_lsn), path)));
				return NULL;
			}
		}
	}

	/*
	 * Allocate the reader and perform the read under TopMemoryContext.  Both
	 * the cached reader and its lazily-allocated decode_buffer (xlogreader.c)
	 * are palloc'd in CurrentMemoryContext; if that is a transient caller
	 * context (e.g. the inline-abort context) it gets deleted, leaving the
	 * static undo_batch_reader and its decode buffer dangling and crashing
	 * the next abort.  We switch back to the caller's context before
	 * allocating the returned UndoBatchData, which the caller owns.  See the
	 * matching comment in UndoValidateBatchLSN.
	 */
	{
		MemoryContext read_ctx = MemoryContextSwitchTo(TopMemoryContext);

		if (undo_batch_reader == NULL)
		{
			undo_batch_reader = XLogReaderAllocate(wal_segment_size, NULL,
												   &undo_batch_reader_routine, NULL);
			if (undo_batch_reader == NULL)
			{
				MemoryContextSwitchTo(read_ctx);
				ereport(WARNING,
						(errmsg("could not allocate XLogReader for UNDO batch read")));
				return NULL;
			}
		}

		/* Position the reader at the target LSN, then read */
		XLogBeginRead(undo_batch_reader, batch_lsn);
		record_hdr = XLogReadRecord(undo_batch_reader, &errormsg);
		MemoryContextSwitchTo(read_ctx);
	}
	if (record_hdr == NULL)
	{
		if (errormsg)
			ereport(WARNING,
					(errmsg("could not read WAL record at %X/%08X: %s",
							LSN_FORMAT_ARGS(batch_lsn), errormsg)));
		return NULL;
	}

	/*
	 * Determine record format: a standalone XLOG_UNDO_BATCH record carrying
	 * the UNDO payload.
	 */
	record_data = XLogRecGetData(undo_batch_reader);
	record_len = XLogRecGetDataLen(undo_batch_reader);

	if (XLogRecGetRmid(undo_batch_reader) == RM_UNDO_ID &&
		(XLogRecGetInfo(undo_batch_reader) & ~XLR_INFO_MASK) == XLOG_UNDO_BATCH)
	{
		/* Standalone XLOG_UNDO_BATCH record */
		if (record_len < SizeOfUndoBatch)
		{
			ereport(WARNING,
					(errmsg("UNDO batch record at %X/%08X too short: %zu bytes",
							LSN_FORMAT_ARGS(batch_lsn), record_len)));
			return NULL;
		}

		xlrec = (xl_undo_batch *) record_data;
		payload_offset = SizeOfUndoBatch;
	}
	else
	{
		ereport(WARNING,
				(errmsg("WAL record at %X/%08X is not an UNDO batch (rmid=%u, info=0x%02x)",
						LSN_FORMAT_ARGS(batch_lsn),
						XLogRecGetRmid(undo_batch_reader),
						XLogRecGetInfo(undo_batch_reader) & ~XLR_INFO_MASK)));
		return NULL;
	}

	/*
	 * Validate that the claimed payload length fits within the WAL record. A
	 * mismatch here means the WAL segment was recycled and overwritten (the
	 * LSN now points to a different record), or the record is corrupt.
	 * Without this check, the memcpy below would read past the XLogReader's
	 * internal buffer, potentially accessing unmapped memory
	 * (SIGBUS/SIGSEGV).
	 */
	if (payload_offset + (Size) xlrec->total_len > record_len)
	{
		ereport(WARNING,
				(errmsg("UNDO batch at %X/%08X: payload length %u exceeds "
						"record data (offset %zu, record_len %zu)",
						LSN_FORMAT_ARGS(batch_lsn),
						xlrec->total_len, payload_offset, record_len)));
		return NULL;
	}

	/*
	 * Allocate UndoBatchData. We use palloc (CurrentMemoryContext) because
	 * this structure is only needed until ApplyUndoChainFromWAL processes the
	 * batch. We intentionally do NOT pfree in UndoFreeBatchData() because
	 * calling pfree on BumpContext memory would ERROR. The memory will be
	 * reclaimed when the current memory context is reset.
	 */
	result = (UndoBatchData *) palloc(sizeof(UndoBatchData));
	memcpy(&result->header, xlrec, SizeOfUndoBatch);
	result->payload_len = (Size) xlrec->total_len;
	if (result->payload_len > 0)
	{
		result->payload = (char *) palloc(result->payload_len);
		memcpy(result->payload, record_data + payload_offset,
			   result->payload_len);
	}
	else
	{
		result->payload = NULL;
	}

	/* Do not free reader -- it is cached for reuse. */
	return result;
}

/*
 * UndoFreeBatchData - Release a UndoBatchData structure
 *
 * This is a no-op function. We don't actually pfree the batch or payload
 * because they were allocated with palloc() from CurrentMemoryContext, which
 * may be a BumpContext. Calling pfree on BumpContext memory would ERROR.
 * The memory will be automatically reclaimed when the current memory context
 * is reset (e.g., at end of query, transaction, or subtransaction).
 *
 * This function exists to maintain API compatibility and to serve as a
 * clear marker in the code where batch data is no longer needed.
 */
void
UndoFreeBatchData(UndoBatchData *batch)
{
	/* Intentionally empty - memory reclaimed by context reset */
	(void) batch;
}
