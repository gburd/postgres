/*-------------------------------------------------------------------------
 *
 * flux_diff.h
 *	  Byte-diff (delta) codec for FLUX UNDO before-images
 *
 * Instead of storing full old tuples in the per-relation UNDO fork, FLUX can
 * store a compact byte-diff: for an UPDATE that changes a few bytes in a wide
 * tuple, store only the changed region of the OLD bytes plus a small header,
 * not the whole old tuple.  This directly cuts per-UPDATE WAL volume (the UNDO
 * before-image is the bulk of a FLUX UPDATE's WAL).
 *
 * The diff is a single splice describing the differing region: the longest
 * common prefix and suffix are stripped, and only the middle old bytes are
 * carried.  Reconstruction (FluxApplyDiffReverse) starts from the current
 * (new) on-page bytes and substitutes the carried old bytes over the changed
 * region, yielding the old tuple.  The format is self-describing (offset +
 * lengths + old bytes), so the reverse-apply needs no catalog or tuple-layout
 * knowledge -- the generic UNDO engine reverse-applies it for rollback and
 * FLUX's PVS read path reverse-applies it for old-version reconstruction using
 * the identical wire layout (see RELUNDO_DIFF_* in access/relundo.h).
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/flux_diff.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef FLUX_DIFF_H
#define FLUX_DIFF_H

#include "access/relundo.h"

/*
 * Threshold: only emit a delta when the diff record is smaller than this
 * fraction of the old tuple size; otherwise the full before-image is cheaper
 * to store and simpler to restore.  Value is a percentage (0-100).
 */
#define FLUX_DIFF_THRESHOLD_PCT		50

/*
 * FluxComputeTupleDiff - Compute the byte-level diff between old and new tuple.
 *
 * Returns a palloc'd diff blob in the on-disk RelUndoDiffRecord layout
 * (access/relundo.h), or NULL if the change spans the whole tuple / exceeds
 * the threshold (caller stores the full old tuple instead).  Handles both
 * equal-length and length-changing updates via a single prefix/suffix-anchored
 * splice; old_total_len records the reconstructed length so a length-changing
 * diff round-trips.
 *
 * *out_size receives the blob size on success.
 */
extern RelUndoDiffRecord *FluxComputeTupleDiff(const char *old_data, Size old_len,
											   const char *new_data, Size new_len,
											   Size *out_size);

/*
 * FluxApplyDiffReverse - Reconstruct the old tuple from the new tuple + diff.
 *
 * Substitutes the diff's carried old bytes over the changed region of the new
 * (current on-page) tuple.  out_old_data must have room for
 * diff->old_total_len bytes.  Returns true on success, false on a malformed or
 * out-of-bounds diff.  This is the same reverse-apply the generic UNDO engine
 * performs for rollback; keeping a FLUX copy lets the PVS read path serve prior
 * versions without an engine round-trip.
 */
extern bool FluxApplyDiffReverse(const char *new_data, Size new_len,
								 const RelUndoDiffRecord *diff,
								 char *out_old_data, Size *out_old_len);

/*
 * FluxDiffIsCompact - Is a diff blob small enough to be worth storing?
 *
 * Returns true if diff_size is below FLUX_DIFF_THRESHOLD_PCT of tuple_len.
 */
extern bool FluxDiffIsCompact(Size diff_size, Size tuple_len);

#endif							/* FLUX_DIFF_H */
