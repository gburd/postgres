/*-------------------------------------------------------------------------
 *
 * recno_escrow.c
 *	  RECNO escrow / delta-accumulation for commutative columns (prototype)
 *
 * An escrow column is a per-attribute reloption (attoptions "escrow=true",
 * AttributeOpts.escrow).  When an UPDATE touches only escrow column(s), the
 * RECNO CAS write path applies the change as a running sum -- onpage +=
 * (new - old) -- in place under the content lock, and records the reverse as
 * a per-relation UNDO record flagged RELUNDO_INFO_ESCROW carrying the NEGATED
 * delta.  Because the reverse is absolute-per-record (add the record's negated
 * delta), a concurrent uncommitted writer's delta on the same running sum is
 * never clobbered on rollback -- the lost-update linchpin (H1).  The redo path
 * carries the RESULT byte-image (H2 idempotency), unchanged from the ordinary
 * CAS UPDATE.
 *
 * Type support: int8 and numeric ONLY.  Float (and any other type) is rejected
 * at flag detection time -- floating-point addition is not associative, so a
 * running sum built from concurrent deltas would be order-dependent and could
 * not be reconstructed for an older snapshot.
 *
 * Prototype scope: a single escrow column per row.  A width-changing numeric
 * accumulate (the running sum needs more digits than the slot holds) is not
 * handled here; the caller must fall back to the ordinary grow path.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/recno/recno_escrow.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/recno.h"
#include "access/recno_undo.h"
#include "access/relundo.h"
#include "access/transam.h"
#include "access/tupmacs.h"
#include "catalog/pg_type.h"
#include "common/int.h"
#include "executor/tuptable.h"
#include "utils/attoptcache.h"
#include "utils/datum.h"
#include "utils/fmgrprotos.h"
#include "utils/numeric.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"


/*
 * RecnoEscrowAttnum
 *		Return the 1-based attnum of the relation's escrow column, or 0 if none.
 *
 * Reads the per-attribute "escrow" reloption from attoptions (via the cached
 * get_attribute_options).  The prototype supports a single escrow column per
 * row; if more than one attribute is flagged, the FIRST is used and the caller
 * is responsible for the single-column assumption (documented, not enforced
 * here to keep the hot path branch-light).
 *
 * The escrow column type MUST be int8 or numeric.  Any other type -- notably
 * float4/float8, whose addition is non-associative -- is rejected with an
 * ERROR, so a mis-flagged column fails loudly at the first escrow UPDATE
 * rather than silently corrupting a money value.
 */
AttrNumber
RecnoEscrowAttnum(Relation rel)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	int			i;

	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);
		AttributeOpts *aopt;

		if (att->attisdropped)
			continue;

		aopt = get_attribute_options(RelationGetRelid(rel), att->attnum);
		if (aopt == NULL || !aopt->escrow)
			continue;

		if (att->atttypid != INT8OID && att->atttypid != NUMERICOID)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("escrow column \"%s\" of relation \"%s\" has unsupported type",
							NameStr(att->attname), RelationGetRelationName(rel)),
					 errdetail("Escrow (commutative delta-accumulation) columns must be of type bigint or numeric; floating-point addition is not associative and cannot be accumulated safely.")));

		return att->attnum;
	}

	return 0;
}


/*
 * escrow_add
 *		Return a + b for an escrow attribute of the given type OID.
 *
 * int8:   checked 64-bit add; overflow raises ERROR (H5).
 * numeric: numeric_add (arbitrary precision; overflow only on absurd scale).
 *
 * Both inputs are pass-by-value/by-ref Datums already matching typid.
 */
static Datum
escrow_add(Oid typid, Datum a, Datum b)
{
	if (typid == INT8OID)
	{
		int64		r;

		if (pg_add_s64_overflow(DatumGetInt64(a), DatumGetInt64(b), &r))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("bigint escrow accumulation out of range")));
		return Int64GetDatum(r);
	}

	Assert(typid == NUMERICOID);
	return DirectFunctionCall2(numeric_add, a, b);
}

/*
 * escrow_sub
 *		Return a - b for an escrow attribute of the given type OID.
 */
static Datum
escrow_sub(Oid typid, Datum a, Datum b)
{
	if (typid == INT8OID)
	{
		int64		r;

		if (pg_sub_s64_overflow(DatumGetInt64(a), DatumGetInt64(b), &r))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("bigint escrow delta out of range")));
		return Int64GetDatum(r);
	}

	Assert(typid == NUMERICOID);
	return DirectFunctionCall2(numeric_sub, a, b);
}

/*
 * escrow_datum_image_len
 *		On-disk image length of an escrow Datum (for the UNDO neg_delta blob).
 *		int8 is 8 raw bytes; numeric is the FIXED-LAYOUT image size for the
 *		column's typmod (constant, independent of the value), so an in-place
 *		numeric accumulate is width-stable and the fast path can engage.
 */
static uint16
escrow_datum_image_len(Oid typid, int32 typmod, Datum value)
{
	Size		img_len;

	if (typid == INT8OID)
		return (uint16) sizeof(int64);

	Assert(typid == NUMERICOID);
	(void) value;
	if (!numeric_fixed_layout_params(typmod, NULL, NULL, NULL, &img_len))
		elog(ERROR, "escrow numeric column has no fixed typmod");
	return (uint16) img_len;
}

/*
 * escrow_datum_to_image / escrow_image_to_datum
 *		Serialize/deserialize an escrow Datum to/from the raw bytes carried in
 *		the UNDO record.  int8 is stored as 8 raw bytes; numeric as its
 *		FIXED-LAYOUT varlena (constant size for the column typmod).
 */
static void
escrow_datum_to_image(Oid typid, int32 typmod, Datum value, char *dst, uint16 len)
{
	if (typid == INT8OID)
	{
		int64		v = DatumGetInt64(value);

		memcpy(dst, &v, sizeof(int64));
		return;
	}

	Assert(typid == NUMERICOID);
	numeric_to_fixed_layout((Numeric) DatumGetPointer(value), typmod, dst, len);
}

static Datum
escrow_image_to_datum(Oid typid, const char *src, uint16 len)
{
	if (typid == INT8OID)
	{
		int64		v;

		Assert(len == sizeof(int64));
		memcpy(&v, src, sizeof(int64));
		return Int64GetDatum(v);
	}

	Assert(typid == NUMERICOID);
	{
		char	   *p = palloc(len);

		memcpy(p, src, len);
		return PointerGetDatum(p);
	}
}


/*
 * RecnoEscrowComputeDelta
 *		Compute delta = new_value - old_value for the escrow attribute, and its
 *		serialized negated image (neg_delta = -delta) for the UNDO record.
 *
 * old_values/new_values are the deformed attribute arrays of the OLD on-page
 * tuple and the NEW (executor-formed) tuple.  attnum is 1-based.
 *
 * Outputs:
 *	*delta_out       - the forward delta Datum (new - old)
 *	delta_image      - caller buffer receiving +delta bytes; *delta_len set
 *	neg_delta_image  - caller buffer receiving -delta bytes; *neg_delta_len set
 *
 * Both image buffers must be at least RECNO_ESCROW_MAX_DELTA_IMAGE bytes.
 */
void
RecnoEscrowComputeDelta(Relation rel, AttrNumber attnum,
						const Datum *old_values, const bool *old_isnull,
						const Datum *new_values, const bool *new_isnull,
						Datum *delta_out,
						char *delta_image, uint16 *delta_len,
						char *neg_delta_image, uint16 *neg_delta_len)
{
	Form_pg_attribute att = TupleDescAttr(RelationGetDescr(rel), attnum - 1);
	Oid			typid = att->atttypid;
	int32		typmod = att->atttypmod;
	Datum		delta;
	Datum		neg_delta;
	uint16		dlen;
	uint16		nlen;

	/* NULL escrow values are not supported in the prototype. */
	if (old_isnull[attnum - 1] || new_isnull[attnum - 1])
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("escrow column may not be NULL")));

	delta = escrow_sub(typid, new_values[attnum - 1], old_values[attnum - 1]);
	neg_delta = escrow_sub(typid, old_values[attnum - 1], new_values[attnum - 1]);

	dlen = escrow_datum_image_len(typid, typmod, delta);
	nlen = escrow_datum_image_len(typid, typmod, neg_delta);
	if (dlen > RECNO_ESCROW_MAX_DELTA_IMAGE || nlen > RECNO_ESCROW_MAX_DELTA_IMAGE)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("escrow delta image too large")));

	escrow_datum_to_image(typid, typmod, delta, delta_image, dlen);
	escrow_datum_to_image(typid, typmod, neg_delta, neg_delta_image, nlen);

	*delta_out = delta;
	*delta_len = dlen;
	*neg_delta_len = nlen;
}


/*
 * RecnoEscrowUpdateIsEligible
 *		True iff this UPDATE changes ONLY the escrow column.
 *
 * Deforms the old on-page image and compares every attribute against the new
 * slot values.  If any non-escrow attribute differs (or its null-ness differs),
 * the update is a mixed escrow+plain change and is NOT eligible -- the caller
 * falls through to the ordinary full-before-image path so non-escrow columns
 * keep INV-4 write-write blocking unchanged.  Also returns false if the escrow
 * column itself did not change (nothing to accumulate).
 *
 * ponytail: O(natts) datum compare per escrow update; the escrow column is
 * typically the only wide comparison and rows are narrow, so this is cheap.
 */
bool
RecnoEscrowUpdateIsEligible(Relation rel, AttrNumber attnum,
							const char *old_image, uint32 old_len,
							TupleTableSlot *slot)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	RecnoTupleData wrap;
	Datum	   *old_values;
	bool	   *old_isnull;
	bool		escrow_changed = false;
	bool		other_changed = false;
	int			i;

	/*
	 * Fast-path types: int8 (fixed width) and numeric with an explicit
	 * precision/scale (stored on-page in a fixed-layout NumericLong of
	 * constant size -- see numeric_to_fixed_layout and the form path).  Both
	 * give a width-stable in-place running sum so the INV-4 concurrency
	 * relaxation applies.  An unconstrained numeric has no fixed layout and
	 * takes the ordinary CAS/grow path (correct, no concurrency win).
	 */
	{
		Form_pg_attribute eatt = TupleDescAttr(tupdesc, attnum - 1);

		if (eatt->atttypid == INT8OID)
			 /* ok */ ;
		else if (eatt->atttypid == NUMERICOID &&
				 numeric_fixed_layout_params(eatt->atttypmod, NULL, NULL,
											 NULL, NULL))
			 /* ok */ ;
		else
			return false;
	}

	old_values = (Datum *) palloc(tupdesc->natts * sizeof(Datum));
	old_isnull = (bool *) palloc(tupdesc->natts * sizeof(bool));

	wrap.t_len = old_len;
	wrap.t_data = (RecnoTupleHeader *) unconstify(char *, old_image);
	RecnoDeformTuple(rel, &wrap, tupdesc, old_values, old_isnull);

	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);
		bool		diff;

		if (att->attisdropped)
			continue;

		if (old_isnull[i] != slot->tts_isnull[i])
			diff = true;
		else if (old_isnull[i])
			diff = false;		/* both NULL */
		else
			diff = !datumIsEqual(old_values[i], slot->tts_values[i],
								 att->attbyval, att->attlen);

		if (!diff)
			continue;

		if (att->attnum == attnum)
			escrow_changed = true;
		else
			other_changed = true;
	}

	/*
	 * Width-stability gate.  The escrow fast-path overwrites the escrow
	 * attribute in place under a same-size slot.  int8 is fixed-width so this
	 * always holds; the check is kept as a defensive assert of that invariant.
	 */
	if (escrow_changed && !other_changed)
	{
		RecnoTuple	newt = RecnoFormTuple(tupdesc, slot->tts_values,
										  slot->tts_isnull, rel, NULL);
		bool		same_width = (newt->t_len == old_len);

		RecnoFreeTuple(newt);
		if (!same_width)
		{
			pfree(old_values);
			pfree(old_isnull);
			return false;
		}
	}

	pfree(old_values);
	pfree(old_isnull);

	return escrow_changed && !other_changed;
}

/*
 * RecnoEscrowComputeDeltaFromSlot
 *		Compute the escrow forward/negated deltas for a CAS update.
 *
 * delta = new_slot[escrow] - old_VISIBLE[escrow], where old_VISIBLE is the
 * escrow value the WRITER's snapshot saw -- NOT the current on-page running
 * sum.  Under concurrency the on-page value may already include a sibling
 * writer's uncommitted delta invisible to this writer; the executor computed
 * new = old_visible + k against its snapshot, so the true increment k is
 * new - old_visible.  Subtracting the (possibly-advanced) on-page value would
 * mis-attribute part of the sibling's delta to this writer and lose an update.
 *
 * If the on-page image is visible to the writer's snapshot (the common
 * single-writer case), old_VISIBLE == on-page and this reduces to
 * new - on-page.  Otherwise the writer's visible version is reconstructed from
 * the UNDO-fork version chain (the same path a reader would take).
 */
void
RecnoEscrowComputeDeltaFromSlot(Relation rel, AttrNumber attnum,
								ItemPointer tid, Snapshot snapshot,
								const char *old_image, uint32 old_len,
								TupleTableSlot *slot,
								char *delta_image, uint16 *delta_len,
								char *neg_delta_image, uint16 *neg_delta_len)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	RecnoTupleData wrap;
	Datum	   *old_values;
	bool	   *old_isnull;
	Datum		delta;
	char	   *vis_data = NULL;
	int			vis_len = 0;
	const char *vis_image = old_image;
	uint32		vis_image_len = old_len;

	old_values = (Datum *) palloc(tupdesc->natts * sizeof(Datum));
	old_isnull = (bool *) palloc(tupdesc->natts * sizeof(bool));

	/*
	 * Reconstruct the version this writer's snapshot sees.  If the on-page
	 * image is not visible (a sibling's uncommitted delta is on top), the walk
	 * peels back to the writer's visible sum; if it is visible, the walk
	 * returns false and we use the on-page image directly.
	 */
	if (snapshot != NULL && IsMVCCSnapshot(snapshot) &&
		RecnoReconstructVisibleVersion(rel, tid, old_image, old_len, snapshot,
									   &vis_data, &vis_len))
	{
		vis_image = vis_data;
		vis_image_len = (uint32) vis_len;
	}

	wrap.t_len = vis_image_len;
	wrap.t_data = (RecnoTupleHeader *) unconstify(char *, vis_image);
	RecnoDeformTuple(rel, &wrap, tupdesc, old_values, old_isnull);

	RecnoEscrowComputeDelta(rel, attnum,
							old_values, old_isnull,
							slot->tts_values, slot->tts_isnull,
							&delta, delta_image, delta_len,
							neg_delta_image, neg_delta_len);

	if (vis_data != NULL)
		pfree(vis_data);
	pfree(old_values);
	pfree(old_isnull);
}

/*
 * RecnoEscrowSetOnpageSum
 *		Set image[escrow] = onpage_image[escrow] + delta, in place.
 *
 * The result image already carries the correct new-version header and every
 * non-escrow column (the escrow update changes only the escrow column).  This
 * overwrites just the escrow attribute with the running sum computed from the
 * CURRENT on-page value plus this writer's forward delta -- so a sibling
 * writer's uncommitted delta already folded into onpage_image is preserved
 * (the INV-4 relaxation / stacking).  In the committed single-writer case
 * onpage_image == the value the executor read, so the sum equals the value
 * already in image and this is a no-op.
 *
 * Same-width required (int8 always; numeric when digits are unchanged); a
 * width change raises ERROR because the slot cannot grow here -- the caller
 * must fall back to the grow path for a widening numeric accumulate.
 */
void
RecnoEscrowSetOnpageSum(Relation rel, char *image, uint32 image_len,
						AttrNumber attnum,
						const char *onpage_image, uint32 onpage_len,
						const char *delta_image, uint16 delta_len)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	Form_pg_attribute att = TupleDescAttr(tupdesc, attnum - 1);
	Oid			typid = att->atttypid;
	RecnoTupleData wrap;
	Datum	   *values;
	bool	   *isnull;
	Datum		delta;
	RecnoTuple	reformed;

	values = (Datum *) palloc(tupdesc->natts * sizeof(Datum));
	isnull = (bool *) palloc(tupdesc->natts * sizeof(bool));

	wrap.t_len = onpage_len;
	wrap.t_data = (RecnoTupleHeader *) unconstify(char *, onpage_image);
	RecnoDeformTuple(rel, &wrap, tupdesc, values, isnull);

	if (isnull[attnum - 1])
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("escrow column may not be NULL")));

	delta = escrow_image_to_datum(typid, delta_image, delta_len);
	values[attnum - 1] = escrow_add(typid, values[attnum - 1], delta);

	reformed = RecnoFormTuple(tupdesc, values, isnull, rel, NULL);

	if (reformed->t_len != image_len)
	{
		RecnoFreeTuple(reformed);
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("escrow accumulate changed tuple width (%u -> %u); grow-path fallback needed",
						image_len, reformed->t_len)));
	}

	{
		RecnoTupleHeader *hdr = (RecnoTupleHeader *) image;
		Size		bitmap_len = MAXALIGN(BITMAPLEN(hdr->t_natts));
		Size		data_off = RECNO_TUPLE_OVERHEAD + bitmap_len;

		if (data_off < image_len)
			memcpy(image + data_off,
				   (char *) reformed->t_data + data_off,
				   image_len - data_off);
	}

	RecnoFreeTuple(reformed);
	pfree(values);
	pfree(isnull);
}


/*
 * RecnoEscrowAttrOffset
 *		Byte offset of the escrow attribute's value within a formed tuple
 *		image.  Computed at write time (TupleDesc available) and stored in the
 *		UNDO record so reverse-apply / crash recovery can locate the int8
 *		value with no catalog access.
 *
 * Walks the on-disk attribute layout exactly as RecnoDeformTuple does
 * (null-bitmap after the header, then aligned column data).  Escrow columns
 * are NOT NULL and int8 (fixed width), and the escrow fast-path forbids other
 * NULLs from shifting layout under it in the prototype, so the offset is
 * stable across the running sum.
 */
uint16
RecnoEscrowAttrOffset(Relation rel, const char *image, uint32 image_len,
					  AttrNumber attnum)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	const RecnoTupleHeader *hdr = (const RecnoTupleHeader *) image;
	Size		bitmap_len = MAXALIGN(BITMAPLEN(hdr->t_natts));
	char	   *base = (char *) image;
	char	   *data_ptr = base + RECNO_TUPLE_OVERHEAD + bitmap_len;
	int			i;

	for (i = 0; i < attnum - 1 && i < tupdesc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);

		if (att->attisdropped)
			continue;
		data_ptr = (char *) att_align_nominal(data_ptr, att->attalign);
		if (att->attlen > 0)
			data_ptr += att->attlen;
		else
			data_ptr += VARSIZE_ANY(data_ptr);
	}

	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, attnum - 1);
		Size		val_len;

		data_ptr = (char *) att_align_nominal(data_ptr, att->attalign);

		if (att->atttypid == NUMERICOID)
		{
			if (!numeric_fixed_layout_params(att->atttypmod, NULL, NULL,
											 NULL, &val_len))
				elog(ERROR, "RecnoEscrowAttrOffset: numeric escrow column lacks fixed typmod");
		}
		else
			val_len = sizeof(int64);

		if ((uint32) (data_ptr - base) + val_len > image_len)
			elog(ERROR, "RecnoEscrowAttrOffset: computed offset past image end");
	}

	return (uint16) (data_ptr - base);
}

/*
 * RecnoEscrowRollback
 *		Reverse-apply an escrow record for transaction rollback / crash recovery.
 *
 * BYTE-LEVEL, catalog-free.  This routine must run during crash recovery on a
 * fake relcache entry with NO TupleDesc, so it must NOT deform/reform.  Both
 * value types operate on raw bytes at a known offset (esc_off, recorded at
 * write time): int8 as a fixed 8-byte integer; numeric as a FIXED-LAYOUT
 * NumericLong of neg_delta_len bytes (numeric_add and numeric_to_fixed_layout
 * are pure varlena arithmetic with no syscache access, so they are safe here;
 * the column typmod is carried in the UNDO record so the fixed re-layout needs
 * no catalog).
 *
 * Steps:
 *  1. value = current_onpage + neg_delta  (absolute-per-record subtract: a
 *     concurrent writer's committed delta in the running sum survives, since
 *     we add back only THIS writer's negated delta -- the out-of-order-abort
 *     lost-update guard, H3).
 *  2. Restore the saved old before-image header (visibility fields); if the
 *     old header's xmin is not committed (it captured a still-in-flight
 *     sibling), stamp FrozenTransactionId so the row does not vanish.
 *  3. Clear transient flags (UNCOMMITTED/DELETED/UPDATED).
 *  4. Write the recomputed value back at esc_off.
 *
 * ponytail: clearing UNCOMMITTED on an out-of-order abort of a non-latest
 * writer can briefly expose a still-uncommitted sibling's delta to a
 * concurrent reader (narrow dirty-read window).  The committed VALUE is always
 * correct; only that isolation edge is approximate.
 */
void
RecnoEscrowRollback(char *image, uint32 image_len,
					uint16 esc_off,
					const char *neg_delta, uint16 neg_delta_len,
					int32 typmod,
					const char *old_image, uint32 old_len)
{
	bool		is_int8 = (neg_delta_len == sizeof(int64));

	if (old_image == NULL || old_len == 0)
		elog(ERROR, "RecnoEscrowRollback: missing old before-image");
	if (old_len != image_len)
		elog(ERROR, "RecnoEscrowRollback: old image length %u != on-page %u",
			 old_len, image_len);
	if ((uint32) esc_off + neg_delta_len > image_len)
		elog(ERROR, "RecnoEscrowRollback: escrow offset %u past image end %u",
			 esc_off, image_len);

	/*
	 * Step 1: recompute value = current running sum + this writer's negated
	 * delta, in place at esc_off.  int8 is raw arithmetic; numeric is
	 * fixed-layout NumericLong arithmetic via the public (syscache-free)
	 * numeric functions.  Do this BEFORE overwriting the header (step 2)
	 * because the header restore does not touch the data region.
	 */
	if (is_int8)
	{
		int64		cur_val;
		int64		neg_val;
		int64		result;

		memcpy(&cur_val, image + esc_off, sizeof(int64));
		memcpy(&neg_val, neg_delta, sizeof(int64));
		result = cur_val + neg_val;
		memcpy(image + esc_off, &result, sizeof(int64));
	}
	else
	{
		/* numeric fixed-layout: cur (on-page) + neg_delta, re-lay-out result */
		Datum		cur = PointerGetDatum(image + esc_off);
		Datum		neg = PointerGetDatum(unconstify(char *, neg_delta));
		Datum		sum;

		if (VARSIZE_ANY(image + esc_off) != neg_delta_len)
			elog(ERROR, "RecnoEscrowRollback: on-page numeric size %u != record %u",
				 (unsigned) VARSIZE_ANY(image + esc_off), neg_delta_len);
		sum = DirectFunctionCall2(numeric_add, cur, neg);
		numeric_to_fixed_layout((Numeric) DatumGetPointer(sum), typmod,
								image + esc_off, neg_delta_len);
		pfree(DatumGetPointer(sum));
	}

	/* Step 2: restore the old header (visibility) */
	memcpy(image, old_image, RECNO_TUPLE_OVERHEAD);
	{
		RecnoTupleHeader *ihdr = (RecnoTupleHeader *) image;

		if (TransactionIdIsValid(ihdr->t_xmin) &&
			!TransactionIdDidCommit(ihdr->t_xmin))
		{
			ihdr->t_xmin = FrozenTransactionId;
			ihdr->t_flags |= RECNO_TUPLE_XMIN_COMMITTED;
		}
	}
	/* Step 3: clear transient flags */
	if (RelUndoClearTransientFlags_hook)
		RelUndoClearTransientFlags_hook(image);
}