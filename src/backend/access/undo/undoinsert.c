/*-------------------------------------------------------------------------
 *
 * undoinsert.c
 *	  UNDO record batch insertion operations
 *
 * This file implements batch insertion of UNDO records into the UNDO log.
 * Records are accumulated in an UndoRecordSet and then written to the
 * UNDO log in a single operation, with appropriate WAL logging.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/undo/undoinsert.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/undolog.h"
#include "access/undorecord.h"
#include "access/undo_xlog.h"
#include "access/xloginsert.h"

/*
 * UndoRecordSetInsert - Insert accumulated UNDO records into log
 *
 * This function writes all UNDO records in the set to the UNDO log
 * in a single batch operation. It performs the following steps:
 *
 * 1. Allocate space in the UNDO log
 * 2. Log a WAL record for the allocation
 * 3. Write the serialized records to the UNDO log
 * 4. Return the starting UndoRecPtr (first record in chain)
 *
 * The records form a backward chain via urec_prev pointers.
 * Returns InvalidUndoRecPtr if the set is empty.
 */
UndoRecPtr
UndoRecordSetInsert(UndoRecordSet * uset)
{
	UndoRecPtr	start_ptr;
	UndoRecPtr	current_ptr;
	xl_undo_allocate xlrec;

	if (uset == NULL || uset->nrecords == 0)
		return InvalidUndoRecPtr;

	/* Allocate space in UNDO log */
	start_ptr = UndoLogAllocate(uset->buffer_size);
	if (!UndoRecPtrIsValid(start_ptr))
		elog(ERROR, "failed to allocate UNDO log space");

	/*
	 * Log the allocation in WAL for crash recovery. This ensures the UNDO log
	 * state can be reconstructed.
	 */
	XLogBeginInsert();

	xlrec.start_ptr = start_ptr;
	xlrec.length = uset->buffer_size;
	xlrec.xid = uset->xid;
	xlrec.log_number = UndoRecPtrGetLogNo(start_ptr);

	XLogRegisterData((char *) &xlrec, SizeOfUndoAllocate);

	(void) XLogInsert(RM_UNDO_ID, XLOG_UNDO_ALLOCATE);

	/* Write the records to the UNDO log */
	UndoLogWrite(start_ptr, uset->buffer, uset->buffer_size);

	/*
	 * Update the record set's previous pointer chain. Each subsequent
	 * insertion will chain backward through this pointer.
	 */
	current_ptr = start_ptr;
	if (uset->nrecords > 1)
	{
		/*
		 * The last record in the set becomes the previous pointer for the
		 * next insertion.
		 */
		current_ptr = start_ptr + (uset->buffer_size - 1);
	}

	uset->prev_undo_ptr = current_ptr;

	return start_ptr;
}
