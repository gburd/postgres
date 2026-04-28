/*-------------------------------------------------------------------------
 *
 * undo_xlog.c
 *	  UNDO resource manager WAL redo routines
 *
 * This module implements the WAL redo callback for the RM_UNDO_ID resource
 * manager.  It handles replay of:
 *
 *   XLOG_UNDO_ALLOCATE       - Replay UNDO log space allocation
 *   XLOG_UNDO_DISCARD        - Replay UNDO record discard
 *   XLOG_UNDO_EXTEND         - Replay UNDO log file extension
 *   XLOG_UNDO_APPLY_RECORD   - Replay CLR (Compensation Log Record)
 *
 * CLR Redo Strategy
 * -----------------
 * CLRs for UNDO application use REGBUF_FORCE_IMAGE to store a full page
 * image.  During redo, XLogReadBufferForRedo() will restore the full page
 * image automatically (returning BLK_RESTORED).  No additional replay
 * logic is needed because the page image already contains the result of
 * the UNDO application.
 *
 * This is the same strategy used by ZHeap (log_zheap_undo_actions with
 * REGBUF_FORCE_IMAGE) and is the simplest correct approach for crash
 * recovery of UNDO operations.
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

#include "access/undo_xlog.h"
#include "access/undolog.h"
#include "access/xlogutils.h"
#include "storage/bufmgr.h"

/*
 * undo_redo - Replay an UNDO WAL record during crash recovery
 *
 * This function handles all UNDO resource manager WAL record types.
 * For CLRs (XLOG_UNDO_APPLY_RECORD), the full page image is restored
 * automatically by XLogReadBufferForRedo(), so no additional replay
 * logic is needed.
 */
void
undo_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
		case XLOG_UNDO_ALLOCATE:
			{
				xl_undo_allocate *xlrec = (xl_undo_allocate *) XLogRecGetData(record);

				/*
				 * During recovery, update the UNDO log's insert pointer to
				 * reflect this allocation.  This ensures that after crash
				 * recovery the UNDO log metadata is consistent.
				 *
				 * Note: UndoLogShared may not be initialized yet during early
				 * recovery.  We guard against that.
				 */
				if (UndoLogShared != NULL)
				{
					UndoLogControl *log = NULL;
					int			i;

					/* Find the log control structure */
					for (i = 0; i < MAX_UNDO_LOGS; i++)
					{
						if (UndoLogShared->logs[i].in_use &&
							UndoLogShared->logs[i].log_number == xlrec->log_number)
						{
							log = &UndoLogShared->logs[i];
							break;
						}
					}

					if (log == NULL)
					{
						/* Log doesn't exist yet, create it */
						for (i = 0; i < MAX_UNDO_LOGS; i++)
						{
							if (!UndoLogShared->logs[i].in_use)
							{
								log = &UndoLogShared->logs[i];
								log->log_number = xlrec->log_number;
								pg_atomic_write_u64(&log->insert_ptr, xlrec->start_ptr);
								log->discard_ptr = MakeUndoRecPtr(xlrec->log_number, 0);
								log->oldest_xid = InvalidTransactionId;
								log->in_use = true;
								break;
							}
						}
					}

					if (log != NULL)
					{
						/* Advance insert pointer past this allocation */
						pg_atomic_write_u64(&log->insert_ptr,
											xlrec->start_ptr + xlrec->length);
					}
				}
			}
			break;

		case XLOG_UNDO_DISCARD:
			{
				xl_undo_discard *xlrec = (xl_undo_discard *) XLogRecGetData(record);

				if (UndoLogShared != NULL)
				{
					int			i;

					for (i = 0; i < MAX_UNDO_LOGS; i++)
					{
						if (UndoLogShared->logs[i].in_use &&
							UndoLogShared->logs[i].log_number == xlrec->log_number)
						{
							UndoLogShared->logs[i].discard_ptr = xlrec->discard_ptr;
							UndoLogShared->logs[i].oldest_xid = xlrec->oldest_xid;
							break;
						}
					}
				}
			}
			break;

		case XLOG_UNDO_EXTEND:
			{
				xl_undo_extend *xlrec = (xl_undo_extend *) XLogRecGetData(record);

				/*
				 * Extend the UNDO log file to the specified size.  The file
				 * will be created if it doesn't exist.
				 */
				ExtendUndoLogFile(xlrec->log_number, xlrec->new_size);
			}
			break;

		case XLOG_UNDO_APPLY_RECORD:
			{
				/*
				 * CLR redo: restore the page to its post-UNDO-application
				 * state.
				 *
				 * Since we use REGBUF_FORCE_IMAGE when logging the CLR, the
				 * full page image is always present.  XLogReadBufferForRedo
				 * will restore it and return BLK_RESTORED, in which case we
				 * just need to release the buffer.
				 *
				 * If for some reason BLK_NEEDS_REDO is returned (which should
				 * not happen with REGBUF_FORCE_IMAGE unless the page was
				 * already up-to-date), we would need to re-apply the UNDO
				 * operation.  For safety we treat this as an error since it
				 * indicates a WAL consistency problem.
				 */
				Buffer		buffer;
				XLogRedoAction action;

				action = XLogReadBufferForRedo(record, 0, &buffer);

				switch (action)
				{
					case BLK_RESTORED:

						/*
						 * Full page image was applied.  Nothing more to do.
						 * The page is already in its correct post-undo state.
						 */
						break;

					case BLK_DONE:

						/*
						 * Page is already up-to-date (LSN check passed). This
						 * is fine -- the UNDO was already applied.
						 */
						break;

					case BLK_NEEDS_REDO:

						/*
						 * This should not happen with REGBUF_FORCE_IMAGE. If
						 * it does, it indicates the full page image was not
						 * stored (e.g., due to a bug in the write path). We
						 * cannot safely re-apply the UNDO operation here
						 * because we don't have the tuple data.  Log an
						 * error.
						 */
						elog(WARNING, "UNDO CLR redo: BLK_NEEDS_REDO unexpected for "
							 "full-page-image CLR record");
						break;

					case BLK_NOTFOUND:

						/*
						 * Block doesn't exist (relation truncated?).  This is
						 * acceptable -- the data is gone and the UNDO
						 * application is moot.
						 */
						break;
				}

				if (BufferIsValid(buffer))
					UnlockReleaseBuffer(buffer);
			}
			break;

		case XLOG_UNDO_ROTATE:
			{
				xl_undo_rotate *xlrec = (xl_undo_rotate *) XLogRecGetData(record);

				/*
				 * Replay segment rotation: mark the old log SEALED and the
				 * new log ACTIVE.  This reconstructs the lifecycle state so
				 * that after recovery the discard worker can clean up sealed
				 * logs properly.
				 */
				if (UndoLogShared != NULL)
				{
					int			j;

					/* Seal the old log */
					if (xlrec->old_log_number != 0)
					{
						for (j = 0; j < MAX_UNDO_LOGS; j++)
						{
							UndoLogControl *old_log = &UndoLogShared->logs[j];

							if (old_log->in_use &&
								old_log->log_number == xlrec->old_log_number)
							{
								old_log->state = UNDO_LOG_SEALED;
								pg_atomic_write_u64(&old_log->seal_ptr,
													xlrec->old_seal_ptr);
								break;
							}
						}
					}

					/* Activate the new log (find or create slot) */
					{
						UndoLogControl *new_log = NULL;
						int			new_slot = -1;

						/* Check if it already exists (idempotent replay) */
						for (j = 0; j < MAX_UNDO_LOGS; j++)
						{
							if (UndoLogShared->logs[j].in_use &&
								UndoLogShared->logs[j].log_number == xlrec->new_log_number)
							{
								new_log = &UndoLogShared->logs[j];
								new_slot = j;
								break;
							}
						}

						/* If not found, allocate a free slot */
						if (new_log == NULL)
						{
							for (j = 0; j < MAX_UNDO_LOGS; j++)
							{
								if (!UndoLogShared->logs[j].in_use)
								{
									new_log = &UndoLogShared->logs[j];
									new_slot = j;
									new_log->log_number = xlrec->new_log_number;
									pg_atomic_write_u64(&new_log->insert_ptr,
														MakeUndoRecPtr(xlrec->new_log_number, 0));
									new_log->discard_ptr = MakeUndoRecPtr(xlrec->new_log_number, 0);
									new_log->oldest_xid = InvalidTransactionId;
									new_log->in_use = true;
									break;
								}
							}
						}

						if (new_log != NULL)
						{
							new_log->state = UNDO_LOG_ACTIVE;
							pg_atomic_write_u64(&new_log->seal_ptr, InvalidUndoRecPtr);
							pg_atomic_write_u32(&UndoLogShared->active_log_idx,
												(uint32) new_slot);
						}
					}
				}
			}
			break;

		default:
			elog(PANIC, "undo_redo: unknown op code %u", info);
	}
}
