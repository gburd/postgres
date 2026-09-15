/*-------------------------------------------------------------------------
 *
 * flux_pvs.c
 *	  FLUX per-relation versioned storage (PVS) read path
 *
 * In-place UPDATEs on a FLUX tuple stamp a trailing RelUndoRecPtr (verptr)
 * into the new on-page image (WS-PVS1).  Each verptr points to the UNDO-fork
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
#include "access/relundo.h"
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
 *		Walk the UNDO-fork version chain and reconstruct the tuple version
 *		that satisfies the reader's MVCC snapshot.
 *
 * Inputs:
 *	rel				- target relation (used to read its UNDO fork)
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
		RelUndoRecPtr verptr;
		RelUndoRecordHeader urec_hdr;
		void	   *payload = NULL;
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
		if (!RelUndoRecPtrIsValid(verptr))
			break;				/* no further history */

		if (!RelUndoReadRecord(rel, verptr, &urec_hdr, &payload, &payload_size))
			break;				/* discarded or unreadable */

		/*
		 * urec_xid is the xid that produced the CURRENT candidate image.  If
		 * it is visible to the reader, the candidate is what we should serve.
		 */
		if (!XidInMVCCSnapshot(urec_hdr.urec_xid, snapshot))
		{
			if (payload != NULL)
				pfree(payload);
			break;
		}

		/*
		 * Updater invisible to reader: reverse-apply the record to obtain the
		 * prior committed image and continue.
		 */
		switch (urec_hdr.urec_type)
		{
			case RELUNDO_UPDATE:
				{
					if (!(urec_hdr.info_flags & RELUNDO_INFO_HAS_TUPLE) ||
						urec_hdr.tuple_len == 0)
					{
						elog(WARNING,
							 "FLUX PVS: RELUNDO_UPDATE without tuple at %llu",
							 (unsigned long long) verptr);
						break;
					}
					if (payload_size < urec_hdr.tuple_len)
					{
						elog(WARNING,
							 "FLUX PVS: RELUNDO_UPDATE payload (%zu) smaller "
							 "than tuple_len (%u) at %llu",
							 payload_size, urec_hdr.tuple_len,
							 (unsigned long long) verptr);
						break;
					}

					/*
					 * Layout written by RelUndoFinish for a full UPDATE
					 * record is [RelUndoUpdatePayload][old tuple bytes]; the
					 * old tuple occupies the trailing tuple_len bytes of the
					 * payload region returned by RelUndoReadRecord.
					 */
					next_len = urec_hdr.tuple_len;
					next_image = (char *) palloc(next_len);
					memcpy(next_image,
						   (const char *) payload +
						   (payload_size - next_len),
						   next_len);
					stepped = true;
					break;
				}

			default:
				elog(DEBUG2,
					 "FLUX PVS: cannot step past urec_type %u at %llu",
					 urec_hdr.urec_type, (unsigned long long) verptr);
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
