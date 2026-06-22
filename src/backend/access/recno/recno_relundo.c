/*-------------------------------------------------------------------------
 *
 * recno_relundo.c
 *	  RECNO adapters for the AM-neutral per-relation UNDO hooks
 *
 * The per-relation UNDO core (src/backend/access/undo/relundo*.c) applies
 * UNDO chains and cleans up retained before-images without any compile-time
 * knowledge of the RECNO access method.  Where it needs RECNO-specific
 * behavior -- clearing RECNO transient tuple flags, reversing a RECNO
 * byte-diff, or cleaning up
 * RECNO's sLog bookkeeping after an abort/discard -- it calls through
 * function pointers declared in access/relundo.h.
 *
 * This file provides the RECNO implementations of those hooks and installs
 * them.  RecnoRelUndoInstallHooks() is invoked from RecnoUndoRmgrInit() so
 * the pointers are live before crash recovery replays any RELUNDO CLR.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/recno/recno_relundo.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/recno.h"
#include "access/recno_diff.h"
#include "access/recno_undo.h"
#include "access/relundo.h"
#include "access/slog.h"
#include "access/xact.h"

/*
 * RecnoRelUndoClearTransientFlags
 *		Clear the transient flags on a restored RECNO tuple.
 *
 * The before-image restored during UNDO is the committed version, so the
 * UNCOMMITTED/DELETED/UPDATED markers left by the rolled-back operation must
 * be cleared.
 */
static void
RecnoRelUndoClearTransientFlags(char *tuple_data)
{
	RecnoTupleHeader *hdr = (RecnoTupleHeader *) tuple_data;

	hdr->t_flags &= ~(RECNO_TUPLE_UNCOMMITTED |
					  RECNO_TUPLE_DELETED |
					  RECNO_TUPLE_UPDATED);
}

/*
 * RecnoRelUndoReverseDelta
 *		Reconstruct the old RECNO tuple from the new tuple plus a stored diff.
 *
 * The opaque diff pointer is a RecnoDiffRecord produced by the RECNO write
 * path.  Returns true on success.
 */
static bool
RecnoRelUndoReverseDelta(const char *new_data, Size new_len,
						 const void *diff,
						 char *old_out, Size *old_len_out)
{
	return RecnoApplyDiffReverse(new_data, new_len,
								 (const RecnoDiffRecord *) diff,
								 old_out, old_len_out);
}

/*
 * RecnoRelUndoReverseDeltaOldLen
 *		Report the length of the tuple a diff reconstructs to.
 *
 * Lets the AM-neutral DELTA apply path size its reconstruction buffer before
 * calling RecnoRelUndoReverseDelta, since a length-changing diff yields a tuple
 * whose size differs from the current on-page tuple.
 */
static Size
RecnoRelUndoReverseDeltaOldLen(const void *diff)
{
	return ((const RecnoDiffRecord *) diff)->old_total_len;
}

/*
 * RecnoRelUndoAbortCleanup
 *		Drop sLog dirty-xid markers for a transaction whose before-images
 *		have just been physically restored by rollback.
 *
 * At abort time, sLog entries for this xid were marked ABORTED (not
 * removed) so visibility checks could keep treating the tuples as live
 * until the physical restore completed.  Now that the restore is done, the
 * markers can be dropped.  Called by the AM-neutral UNDO core (inline from
 * xactundo.c, or from the background worker in relundo_worker.c); RECNO is
 * the only in-place AM that keeps this kind of transient bookkeeping, so the
 * core reaches it through this hook rather than a compile-time dependency.
 */
static void
RecnoRelUndoAbortCleanup(TransactionId xid)
{
	SLogTupleRemoveByXidGlobal(xid);
}

/*
 * RecnoRelUndoDiscardRetained
 *		Reclaim sLog before-image entries no longer needed by any active
 *		snapshot.
 *
 * Called periodically by the UNDO discard worker (undoworker.c).  The
 * reclamation horizon is the xid horizon computed inside
 * SLogTupleCleanupRetained().
 */
static void
RecnoRelUndoDiscardRetained(void)
{
	SLogTupleCleanupRetained();
}

/*
 * RecnoRelUndoInstallHooks
 *		Wire the RECNO implementations into the AM-neutral UNDO core.
 *
 * Called from RecnoUndoRmgrInit() at postmaster startup, before crash
 * recovery can replay any RELUNDO CLR that would invoke these hooks.
 */
void
RecnoRelUndoInstallHooks(void)
{
	RelUndoClearTransientFlags_hook = RecnoRelUndoClearTransientFlags;
	RelUndoReverseDelta_hook = RecnoRelUndoReverseDelta;
	RelUndoReverseDeltaOldLen_hook = RecnoRelUndoReverseDeltaOldLen;
	RelUndoAbortCleanup_hook = RecnoRelUndoAbortCleanup;
	RelUndoDiscardRetained_hook = RecnoRelUndoDiscardRetained;
	TableAMPrepare_hook = AtPrepare_Recno;
}
