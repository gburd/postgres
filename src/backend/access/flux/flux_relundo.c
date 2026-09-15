/*-------------------------------------------------------------------------
 *
 * flux_relundo.c
 *	  FLUX adapters for the AM-neutral per-relation UNDO hooks
 *
 * The per-relation UNDO core (src/backend/access/undo/relundo*.c) applies
 * UNDO chains and cleans up retained before-images without any compile-time
 * knowledge of the FLUX access method.  Where it needs FLUX-specific
 * behavior -- clearing FLUX transient tuple flags, reversing a FLUX
 * byte-diff, or cleaning up
 * FLUX's sLog bookkeeping after an abort/discard -- it calls through
 * function pointers declared in access/relundo.h.
 *
 * This file provides the FLUX implementations of those hooks and installs
 * them.  FluxRelUndoInstallHooks() is invoked from FluxUndoRmgrInit() so
 * the pointers are live before crash recovery replays any RELUNDO CLR.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/flux/flux_relundo.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/flux.h"
#include "access/flux_undo.h"
#include "access/relundo.h"
#include "access/slog.h"
#include "access/xact.h"

/*
 * FluxRelUndoClearTransientFlags
 *		Clear the transient flags on a restored FLUX tuple.
 *
 * The before-image restored during UNDO is the committed version, so the
 * UNCOMMITTED/DELETED/UPDATED markers left by the rolled-back operation must
 * be cleared.
 */
static void
FluxRelUndoClearTransientFlags(char *tuple_data)
{
	FluxTupleHeader *hdr = (FluxTupleHeader *) tuple_data;

	hdr->t_flags &= ~(FLUX_TUPLE_UNCOMMITTED |
					  FLUX_TUPLE_DELETED |
					  FLUX_TUPLE_UPDATED);
}

/*
 * FluxRelUndoAbortCleanup
 *		Drop sLog dirty-xid markers for a transaction whose before-images
 *		have just been physically restored by rollback.
 *
 * At abort time, sLog entries for this xid were marked ABORTED (not
 * removed) so visibility checks could keep treating the tuples as live
 * until the physical restore completed.  Now that the restore is done, the
 * markers can be dropped.  Called by the AM-neutral UNDO core (inline from
 * xactundo.c, or from the background worker in relundo_worker.c); FLUX is
 * the only in-place AM that keeps this kind of transient bookkeeping, so the
 * core reaches it through this hook rather than a compile-time dependency.
 */
static void
FluxRelUndoAbortCleanup(TransactionId xid)
{
	SLogTupleRemoveByXidGlobal(xid);
}

/*
 * FluxRelUndoDiscardRetained
 *		Reclaim sLog before-image entries no longer needed by any active
 *		snapshot.
 *
 * Called periodically by the UNDO discard worker (undoworker.c).  The
 * reclamation horizon is the xid horizon computed inside
 * SLogTupleCleanupRetained().
 */
static void
FluxRelUndoDiscardRetained(void)
{
	SLogTupleCleanupRetained();
}

/*
 * FluxRelUndoInstallHooks
 *		Wire the FLUX implementations into the AM-neutral UNDO core.
 *
 * Called from FluxUndoRmgrInit() at postmaster startup, before crash
 * recovery can replay any RELUNDO CLR that would invoke these hooks.
 */
void
FluxRelUndoInstallHooks(void)
{
	RelUndoClearTransientFlags_hook = FluxRelUndoClearTransientFlags;
	RelUndoAbortCleanup_hook = FluxRelUndoAbortCleanup;
	RelUndoDiscardRetained_hook = FluxRelUndoDiscardRetained;
	TableAMPrepare_hook = AtPrepare_Flux;
}
