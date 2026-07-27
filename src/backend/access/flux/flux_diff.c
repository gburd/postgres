/*-------------------------------------------------------------------------
 *
 * flux_diff.c
 *	  Byte-diff (delta) codec for FLUX UNDO before-images
 *
 * FLUX stores the before-image of an in-place non-key UPDATE in its
 * per-relation UNDO fork so rollback and old-snapshot readers can recover the
 * prior committed version.  The full before-image is the bulk of a FLUX
 * UPDATE's WAL volume.  This module computes a compact byte-diff instead: for
 * a small change to a wide tuple it carries only the changed region of the OLD
 * bytes, cutting per-UPDATE WAL volume against the dominant write-path
 * bottleneck.
 *
 * The diff is a single splice in the shared RelUndoDiffRecord wire layout
 * (access/relundo.h): strip the longest common prefix and suffix, carry the
 * differing middle of the old tuple, and record old_total_len so a
 * length-changing update round-trips.  The reverse-apply is a self-describing
 * byte splice, so the generic UNDO engine reverses it for rollback
 * (RelUndoApplyDiffReverse) with no table-AM knowledge; FluxApplyDiffReverse
 * here mirrors that algorithm for FLUX's PVS old-version reconstruction so one
 * delta record serves both paths.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/flux/flux_diff.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/flux_diff.h"
#include "utils/palloc.h"

/*
 * FluxComputeTupleDiff - compute the byte-level diff between old and new tuple.
 *
 * Anchors a single splice by the longest common prefix and suffix, so an
 * equal-length change (contiguous or not, the splice spans the whole differing
 * run) and a length-changing change (one varlen column grows/shrinks) both
 * reduce to one splice covering the differing middle.  Returns a palloc'd blob
 * in RelUndoDiffRecord layout with *out_size set, or NULL (caller stores the
 * full old tuple) when either tuple overflows the uint16 offset domain or the
 * two tuples are byte-identical.  The threshold check is left to the caller
 * (FluxDiffIsCompact) so the caller can log an over-threshold diff at DEBUG.
 */
RelUndoDiffRecord *
FluxComputeTupleDiff(const char *old_data, Size old_len,
					 const char *new_data, Size new_len,
					 Size *out_size)
{
	Size		prefix = 0;
	Size		suffix = 0;
	Size		min_len = Min(old_len, new_len);
	Size		ins_len;
	Size		blob_size;
	RelUndoDiffRecord *diff;

	/*
	 * Offsets/lengths are uint16.  FLUX tuples live within one page (< BLCKSZ)
	 * so this is a defensive guard; fall back to the full before-image if ever
	 * violated.
	 */
	if (old_len > PG_UINT16_MAX || new_len > PG_UINT16_MAX)
		return NULL;

	/* Longest common prefix. */
	while (prefix < min_len && old_data[prefix] == new_data[prefix])
		prefix++;

	/* Longest common suffix, not overrunning the prefix on either tuple. */
	while (suffix < (min_len - prefix) &&
		   old_data[old_len - 1 - suffix] == new_data[new_len - 1 - suffix])
		suffix++;

	/* No difference: caller should not have reached this for a real UPDATE. */
	if (prefix == old_len && old_len == new_len)
		return NULL;

	ins_len = old_len - prefix - suffix;		/* old bytes to carry */

	/*
	 * ponytail: both call sites are same-size updates (del_len == ins_len), so
	 * the length-changing prefix/suffix splice is not exercised today; it costs
	 * nothing extra (same two scans) and keeps the codec symmetric with the
	 * engine's general RelUndoApplyDiffReverse and the RelUndoDiffRecord wire
	 * format.  If a length-changing in-place update ever emits a delta, this
	 * already round-trips.
	 */
	blob_size = SizeOfRelUndoDiffRecord + ins_len;
	diff = (RelUndoDiffRecord *) palloc(blob_size);
	diff->old_total_len = (uint16) old_len;
	diff->offset = (uint16) prefix;
	diff->del_len = (uint16) (new_len - prefix - suffix);	/* new bytes replaced */
	diff->ins_len = (uint16) ins_len;
	memcpy((char *) diff + SizeOfRelUndoDiffRecord, old_data + prefix, ins_len);

	*out_size = blob_size;
	return diff;
}

/*
 * FluxApplyDiffReverse - reconstruct the old tuple from the new tuple + diff.
 *
 * Mirror of the engine's RelUndoApplyDiffReverse for FLUX's PVS read path.
 * out_old_data must hold diff->old_total_len bytes.  Returns true on success,
 * false on a malformed / out-of-bounds diff.
 */
bool
FluxApplyDiffReverse(const char *new_data, Size new_len,
					 const RelUndoDiffRecord *diff,
					 char *out_old_data, Size *out_old_len)
{
	Size		old_total_len;
	Size		tail;

	if (diff == NULL || new_data == NULL || out_old_data == NULL)
		return false;

	old_total_len = diff->old_total_len;

	if ((Size) diff->offset > new_len ||
		(Size) diff->offset + diff->del_len > new_len)
		return false;

	tail = new_len - diff->offset - diff->del_len;

	if ((Size) diff->offset + diff->ins_len + tail != old_total_len)
		return false;

	memcpy(out_old_data, new_data, diff->offset);
	memcpy(out_old_data + diff->offset,
		   (const char *) diff + SizeOfRelUndoDiffRecord,
		   diff->ins_len);
	memcpy(out_old_data + diff->offset + diff->ins_len,
		   new_data + diff->offset + diff->del_len,
		   tail);

	*out_old_len = old_total_len;
	return true;
}

/*
 * FluxDiffIsCompact - is a diff blob small enough to be worth storing?
 */
bool
FluxDiffIsCompact(Size diff_size, Size tuple_len)
{
	if (tuple_len == 0)
		return false;

	return diff_size < (tuple_len * FLUX_DIFF_THRESHOLD_PCT / 100);
}
