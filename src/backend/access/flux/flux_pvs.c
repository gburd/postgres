/*-------------------------------------------------------------------------
 *
 * flux_pvs.c
 *	  FLUX per-relation versioned storage (PVS) read path
 *
 * In-place UPDATEs on a FLUX tuple stamp a trailing UndoRecPtr (verptr)
 * into the new on-page image (WS-PVS1).  Each verptr points to the UNDO
 * record that describes the update which produced its host image; reversing
 * the diff (or full-tuple) in that record yields the immediately prior
 * committed image, whose own trailing verptr -- preserved verbatim through
 * the reverse-apply -- continues the chain one further step back.
 *
 * FluxReconstructVisibleVersion walks that chain on behalf of an MVCC
 * reader, stepping back one version at a time until it finds the image
 * created by an xid that is VISIBLE to the reader's snapshot (i.e. one
 * whose urec_xid is NOT in the in-progress set).  The result is the
 * before-image the reader should see in place of the on-page (newer) data.
 *
 * urec_prevundorec is NOT used here: that field threads a per-relation,
 * per-transaction rollback LIFO, not a per-tuple version chain.  The
 * authoritative per-tuple chain is the verptr threaded through reconstructed
 * images.
 *
 * Visibility per step is authoritative on the core MVCC snapshot:
 *	XidInMVCCSnapshot(urec_xid, snapshot) == true  -> updater invisible,
 *	                                                  step back
 *	XidInMVCCSnapshot(urec_xid, snapshot) == false -> updater visible,
 *	                                                  serve current candidate
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/flux/flux_pvs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/flux.h"
#include "access/flux_pbu.h"
#include "access/flux_undo.h"
#include "access/transam.h"
#include "access/xact.h"
#include "utils/snapmgr.h"

/*
 * Safety bound on how deep the version chain we will walk.  In a healthy
 * system the chain is bounded by the number of UPDATEs visible to the oldest
 * snapshot retaining UNDO records; this cap defends against a corrupted
 * record that would otherwise loop forever.
 */
#define FLUX_PVS_MAX_CHAIN_DEPTH	10000

/*
 * FluxReconstructVisibleVersion
 *		Walk the UNDO version chain and reconstruct the tuple version
 *		that satisfies the reader's MVCC snapshot.
 *
 * Inputs:
 *	rel				- target relation (used to read its UNDO log)
 *	tid				- on-page TID of the row (currently unused but reserved
 *					  for future diagnostics)
 *	onpage_image	- pointer to the on-page tuple bytes (read-only)
 *	onpage_len		- length of onpage_image (ItemIdGetLength of the slot)
 *	snapshot		- MVCC snapshot of the reader
 *
 * Outputs (only populated when the function returns true):
 *	out_data		- palloc'd buffer holding the reconstructed image
 *	out_len			- length of *out_data
 *
 * Returns true if a different-from-on-page version should be served and
 * *out_data / *out_len have been populated.  Returns false when the on-page
 * value is what the reader should see (caller must NOT free *out_data in
 * that case; it is left untouched).
 *
 * If the chain is incomplete (record discarded, malformed, or terminates
 * before a visible version is found), the function returns the deepest
 * reconstructed image it could build, mirroring the "best-effort" semantics
 * of the legacy sLog before-image path.  If no reconstruction was performed
 * (i.e. the on-page image's own verptr is invalid or the very first record
 * read failed), the function returns false and the caller serves on-page
 * data unchanged.
 */
bool
FluxReconstructVisibleVersion(Relation rel, ItemPointer tid,
							  const char *onpage_image, Size onpage_len,
							  Snapshot snapshot,
							  char **out_data, int *out_len)
{
	const char *candidate = onpage_image;
	Size		candidate_len = onpage_len;
	char	   *allocated = NULL;
	int			depth = 0;

	(void) tid;					/* reserved for future diagnostics */

	if (snapshot == NULL || onpage_image == NULL || onpage_len == 0)
		return false;

	for (;;)
	{
		const FluxTupleHeader *hdr = (const FluxTupleHeader *) candidate;
		UndoRecPtr	verptr;
		TransactionId urec_xid;
		uint16		urec_subtype;
		char	   *payload = NULL;
		Size		payload_size = 0;
		char	   *next_image = NULL;
		Size		next_len = 0;
		bool		stepped = false;

		if (depth++ > FLUX_PVS_MAX_CHAIN_DEPTH)
		{
			elog(WARNING,
				 "FLUX PVS: version chain at (%u,%u) of relation %u exceeds "
				 "depth cap %d; serving best-effort image",
				 ItemPointerGetBlockNumber(tid),
				 ItemPointerGetOffsetNumber(tid),
				 RelationGetRelid(rel),
				 FLUX_PVS_MAX_CHAIN_DEPTH);
			break;
		}

		verptr = FluxTupleGetVersionPtr(hdr, candidate_len);
		if (!UndoRecPtrIsValid(verptr))
		{
			/*
			 * End of the version chain: this candidate is the original
			 * inserted image (an INSERT writes no undo record and leaves an
			 * invalid verptr).  There is no older version to fall back to, so
			 * the row is visible only if the inserter itself is visible to the
			 * reader.  If the inserter is invisible (aborted, crashed, or
			 * in-progress and not our own), the row was never really there for
			 * this snapshot: report no visible version rather than serving a
			 * phantom before-image.
			 */
			TransactionId ins_xid = FluxTupleGetXmin(hdr);
			bool		ins_visible;

			if (!TransactionIdIsValid(ins_xid) ||
				TransactionIdIsCurrentTransactionId(ins_xid))
			{
				/* pre-upgrade tuple (no xmin) or our own insert: visible */
				ins_visible = true;
			}
			else
			{
				/*
				 * Heap-style: the inserted row is visible only if its inserter
				 * committed AND committed before this snapshot (not in the
				 * snapshot's in-progress set).  An aborted or crashed inserter
				 * reports !DidCommit, so its image is never served -- this is
				 * the rollback signal now that INSERT writes no undo record.
				 */
				ins_visible = TransactionIdDidCommit(ins_xid) &&
					!XidInMVCCSnapshot(ins_xid, snapshot);
			}

			if (!ins_visible)
			{
				if (allocated != NULL)
					pfree(allocated);
				*out_data = NULL;
				*out_len = 0;
				return false;
			}
			break;			/* no further history; serve this candidate */
		}

		if (!FluxPbuFetchVersion(verptr, &urec_xid, &urec_subtype,
								 &payload, &payload_size))
			break;				/* discarded or unreadable */

		/*
		 * urec_xid is the xid that produced the CURRENT candidate image.  If
		 * it is visible to the reader, the candidate is what we should serve.
		 */
		if (!XidInMVCCSnapshot(urec_xid, snapshot))
		{
			if (payload != NULL)
				pfree(payload);
			break;
		}

		/*
		 * Updater invisible to reader: reverse-apply the record to obtain the
		 * prior committed image and continue.  The per-backend UNDO record's
		 * uur_payload is [FluxUndoPayloadHeader][old tuple bytes] (built by
		 * FluxPbuPrepareInsert), so the before-image is the tuple_len bytes
		 * immediately after the fixed header.
		 */
		switch (urec_subtype)
		{
			case FLUX_UNDO_UPDATE:
				{
					FluxUndoPayloadHeader phdr;

					if (payload == NULL ||
						payload_size < SizeOfFluxUndoPayloadHeader)
					{
						elog(WARNING,
							 "FLUX PVS: FLUX_UNDO_UPDATE payload too short at %llu",
							 (unsigned long long) verptr);
						break;
					}
					memcpy(&phdr, payload, SizeOfFluxUndoPayloadHeader);
					if (!(phdr.flags & FLUX_UNDO_FLAG_HAS_TUPLE) ||
						phdr.tuple_len == 0)
					{
						elog(WARNING,
							 "FLUX PVS: FLUX_UNDO_UPDATE without tuple at %llu",
							 (unsigned long long) verptr);
						break;
					}
					if (payload_size < SizeOfFluxUndoPayloadHeader + phdr.tuple_len)
					{
						elog(WARNING,
							 "FLUX PVS: FLUX_UNDO_UPDATE payload (%zu) smaller "
							 "than header+tuple_len (%zu+%u) at %llu",
							 payload_size, SizeOfFluxUndoPayloadHeader,
							 phdr.tuple_len, (unsigned long long) verptr);
						break;
					}

					next_len = phdr.tuple_len;
					next_image = (char *) palloc(next_len);
					memcpy(next_image,
						   payload + SizeOfFluxUndoPayloadHeader,
						   next_len);
					stepped = true;
					break;
				}

			case FLUX_UNDO_UPDATE_DELTA:
				{
					FluxUndoPayloadHeader phdr;
					const FluxUndoDelta *delta;
					Size		delta_len;

					if (payload == NULL ||
						payload_size < SizeOfFluxUndoPayloadHeader +
						SizeOfFluxUndoDelta)
					{
						elog(WARNING,
							 "FLUX PVS: FLUX_UNDO_UPDATE_DELTA payload too short at %llu",
							 (unsigned long long) verptr);
						break;
					}
					memcpy(&phdr, payload, SizeOfFluxUndoPayloadHeader);
					if (!(phdr.flags & FLUX_UNDO_FLAG_HAS_TUPLE))
					{
						elog(WARNING,
							 "FLUX PVS: FLUX_UNDO_UPDATE_DELTA without tuple at %llu",
							 (unsigned long long) verptr);
						break;
					}

					delta = (const FluxUndoDelta *)
						(payload + SizeOfFluxUndoPayloadHeader);
					delta_len = payload_size - SizeOfFluxUndoPayloadHeader;

					/*
					 * Reconstruct the prior image by reverse-applying the delta
					 * onto the CURRENT candidate image (not onto the page).
					 * The delta always covers the t_verptr at offset 8, so the
					 * reconstructed image carries the prior chain link and the
					 * walk continues one step further back.
					 */
					next_len = delta->old_len;
					next_image = (char *) palloc(next_len);
					if (!FluxUndoDeltaReconstruct(delta, delta_len,
												  candidate, candidate_len,
												  next_image))
					{
						elog(WARNING,
							 "FLUX PVS: FLUX_UNDO_UPDATE_DELTA reverse-diff failed at %llu",
							 (unsigned long long) verptr);
						pfree(next_image);
						next_image = NULL;
						break;
					}
					stepped = true;
					break;
				}

			default:
				elog(DEBUG2,
					 "FLUX PVS: cannot step past undo subtype %u at %llu",
					 urec_subtype, (unsigned long long) verptr);
				break;
		}

		if (payload != NULL)
			pfree(payload);

		if (!stepped)
			break;				/* serve the best-effort candidate we have */

		/* Replace the candidate with the reconstructed prior image. */
		if (allocated != NULL)
			pfree(allocated);
		allocated = next_image;
		candidate = allocated;
		candidate_len = next_len;
	}

	if (allocated == NULL)
		return false;			/* no reconstruction; caller serves on-page */

	*out_data = allocated;
	*out_len = (int) candidate_len;
	return true;
}
