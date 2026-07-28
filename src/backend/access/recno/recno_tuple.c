/*-------------------------------------------------------------------------
 *
 * recno_tuple.c
 *	  RECNO tuple handling routines
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/recno/recno_tuple.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/recno.h"
#include "access/recno_xlog.h"
#include "access/tupdesc.h"
#include "access/tupmacs.h"
#include "catalog/pg_type.h"
#include "utils/numeric.h"
#include "common/hashfn.h"
#include "executor/tuptable.h"
#include "storage/bufpage.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

/*
 * RecnoComputeDataSize
 *
 * Calculate the total on-disk size needed to store a tuple with the given
 * attributes.  This includes the fixed-size RecnoTupleHeader, the null
 * bitmap, alignment padding, and all attribute data.
 *
 * Parameters:
 *   tupdesc - tuple descriptor defining the attributes
 *   values  - array of Datum values for each attribute
 *   isnull  - array of boolean null indicators
 *
 * Returns the total size in bytes, including header and alignment.
 */
Size
RecnoComputeDataSize(TupleDesc tupdesc, Datum *values, bool *isnull)
{
	Size		data_length = 0;
	Size		bitmap_len;
	int			i;

	Assert(tupdesc != NULL);
	Assert(values != NULL);
	Assert(isnull != NULL);

	/* Calculate null bitmap length */
	bitmap_len = BITMAPLEN(tupdesc->natts);

	/* Start with tuple header size */
	data_length = RECNO_TUPLE_OVERHEAD + MAXALIGN(bitmap_len);

	/* Add space for each attribute */
	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);

		if (att->attisdropped)
			continue;

		if (!isnull[i])
		{
			Size		attr_len;

			/* Align attribute */
			data_length = att_align_nominal(data_length, att->attalign);

			if (att->attlen > 0)
			{
				/* Fixed-length attribute */
				attr_len = att->attlen;
			}
			else if (att->attlen == -1)
			{
				/* Variable-length attribute */
				attr_len = VARSIZE_ANY(DatumGetPointer(values[i]));
			}
			else if (att->attlen == -2)
			{
				/* C string */
				attr_len = strlen(DatumGetCString(values[i])) + 1;
			}
			else
			{
				elog(ERROR, "unsupported attribute length: %d", att->attlen);
			}

			data_length += attr_len;
		}
	}

	return data_length;
}

/*
 * RecnoFormTuple
 *
 * Create a new RECNO tuple from the given attribute values and null indicators.
 * Allocates memory for the RecnoTupleData wrapper and the on-disk
 * RecnoTupleHeader + attribute data.
 *
 * When compression is enabled (recno_enable_compression GUC), variable-length
 * attributes exceeding RECNO_MIN_COMPRESS_SIZE (32 bytes) are automatically
 * compressed using the algorithm selected by RecnoChooseCompressionType().
 * Compressed attributes are stored with a RecnoCompressionHeader prefix and
 * the tuple's RECNO_INFOMASK_COMPRESSED bit is set.
 *
 * When a relation is provided, large attributes exceeding RECNO_OVERFLOW_THRESHOLD
 * are automatically stored in overflow pages. Overflow pointers are collected in
 * overflow_buffers for atomic WAL logging by the caller.
 *
 * Parameters:
 *   tupdesc - tuple descriptor defining the schema
 *   values  - array of Datum values for each attribute
 *   isnull  - array of boolean null indicators
 *   rel     - relation for overflow storage (NULL to disable overflow handling)
 *   overflow_buffers - output for overflow buffers (NULL if rel is NULL)
 *
 * Returns a palloc'd RecnoTuple.  The caller is responsible for freeing it
 * with RecnoFreeTuple() when done.
 */
static RecnoTuple recno_form_tuple_internal(TupleDesc tupdesc, Datum *values,
											bool *isnull, Relation rel,
											RecnoOverflowBuffers *overflow_buffers,
											bool force_shrink,
											const RecnoOverflowPtr *old_ovptrs,
											const bool *old_ovpresent);

RecnoTuple
RecnoFormTuple(TupleDesc tupdesc, Datum *values, bool *isnull,
			   Relation rel, RecnoOverflowBuffers *overflow_buffers)
{
	return recno_form_tuple_internal(tupdesc, values, isnull, rel,
									 overflow_buffers, false, NULL, NULL);
}

/*
 * RecnoFormTupleUpdate
 *
 * Like RecnoFormTuple, but for the in-place UPDATE path.  old_ovptrs and
 * old_ovpresent (indexed by attnum, natts entries) describe the OLD tuple's
 * on-page overflow pointers, collected while its buffer was still locked.  Any
 * over-threshold varlena whose content hash matches the old pointer's stored
 * hash (and byte-verifies equal) is COW-referenced against the existing
 * overflow chain instead of being re-stored, avoiding needless disk growth and
 * WAL.  Pass NULL arrays to disable this (identical to RecnoFormTuple).
 */
RecnoTuple
RecnoFormTupleUpdate(TupleDesc tupdesc, Datum *values, bool *isnull,
					 Relation rel, RecnoOverflowBuffers *overflow_buffers,
					 const RecnoOverflowPtr *old_ovptrs,
					 const bool *old_ovpresent)
{
	return recno_form_tuple_internal(tupdesc, values, isnull, rel,
									 overflow_buffers, false,
									 old_ovptrs, old_ovpresent);
}

/*
 * RecnoFormTupleForceShrink
 *
 * Like RecnoFormTuple, but forces every inline varlena attribute larger than
 * an overflow pointer off-page with a zero inline prefix, regardless of the
 * normal RECNO_OVERFLOW_THRESHOLD.  This shrinks the main tuple to its minimum
 * footprint (header + fixed columns + one overflow pointer per large varlena).
 *
 * Used as a last resort by the in-place UPDATE path: when an updated tuple has
 * grown beyond the space available on its page and TID stability forbids
 * relocating it, pushing its variable-length data off-page lets the main tuple
 * fit back into (or near) its original slot.  A relation and overflow_buffers
 * are mandatory because every forced column is written to overflow pages.
 */
RecnoTuple
RecnoFormTupleForceShrink(TupleDesc tupdesc, Datum *values, bool *isnull,
						  Relation rel, RecnoOverflowBuffers *overflow_buffers)
{
	Assert(rel != NULL);
	Assert(overflow_buffers != NULL);
	return recno_form_tuple_internal(tupdesc, values, isnull, rel,
									 overflow_buffers, true, NULL, NULL);
}

/*
 * recno_try_cow_overflow
 *
 * Attempt to COW-reference an unchanged overflow column against the old
 * tuple's existing chain instead of re-storing it.  Returns a freshly palloc'd
 * overflow-pointer varlena (with zero inline prefix, no new chain, nothing to
 * WAL-log) on success, or (Datum) 0 when the value differs and must be stored
 * fresh by the caller.
 *
 * The gate is a whole-value content hash carried in the old on-page pointer
 * (zero I/O -- the old page is already pinned).  Only when hashes match do we
 * fetch the old chain once and byte-compare to defeat collisions.
 *
 * SAFETY INVARIANT: RECNO UPDATE is strictly in-place -- the new version
 * overwrites the old one in the same slot/TID and no second on-page version
 * survives.  That is what lets the new tuple share the old chain without a
 * refcount: VACUUM Pass 1 unions chain locators from every live HAS_OVERFLOW
 * tuple, so a shared chain stays live while any referencer is live, and the
 * only chain-delete site (VACUUM Pass 1, committed RECNO_TUPLE_DELETED) fires
 * only when no live tuple references it.  If a future out-of-place or
 * cross-page UPDATE variant leaves the old version on-page, this sharing and
 * the VACUUM delete must be re-audited.
 */
static Datum
recno_try_cow_overflow(Relation rel, Datum newval,
					   const RecnoOverflowPtr *old_ovp)
{
	char	   *new_data = VARDATA_ANY(DatumGetPointer(newval));
	Size		new_len = VARSIZE_ANY_EXHDR(DatumGetPointer(newval));
	uint32		newhash;
	Datum		olddat;
	char	   *old_data;
	Size		old_len;
	char	   *result;
	Size		result_size;
	RecnoOverflowPtr *ovp;
	char		ovbuf[VARHDRSZ + sizeof(RecnoOverflowPtr)];

	/* Lengths and hash must agree before we pay for a fetch */
	if (new_len != old_ovp->ov_total_length)
		return (Datum) 0;

	newhash = hash_bytes((const unsigned char *) new_data, (int) new_len);
	if (newhash != old_ovp->ov_content_hash)
		return (Datum) 0;

	/*
	 * Byte-verify against the old chain to defeat hash collisions.  Wrap the
	 * old pointer in a bare varlena (no inline prefix) so RecnoFetchOverflowColumn
	 * can walk the chain from it.
	 */
	SET_VARSIZE(ovbuf, sizeof(ovbuf));
	memcpy(VARDATA(ovbuf), old_ovp, sizeof(RecnoOverflowPtr));
	((RecnoOverflowPtr *) VARDATA(ovbuf))->ov_inline_prefix = 0;
	olddat = RecnoFetchOverflowColumn(rel, ovbuf);
	old_data = VARDATA_ANY(DatumGetPointer(olddat));
	old_len = VARSIZE_ANY_EXHDR(DatumGetPointer(olddat));
	if (old_len != new_len || memcmp(old_data, new_data, new_len) != 0)
	{
		pfree(DatumGetPointer(olddat));
		return (Datum) 0;
	}
	pfree(DatumGetPointer(olddat));

	/*
	 * Unchanged: build a pointer varlena that references the old chain
	 * verbatim, with no inline prefix and no newly-allocated overflow records.
	 */
	result_size = VARHDRSZ + sizeof(RecnoOverflowPtr);
	result = (char *) palloc0(result_size);
	SET_VARSIZE(result, result_size);

	ovp = (RecnoOverflowPtr *) VARDATA(result);
	ovp->ov_magic = RECNO_OVERFLOW_PTR_MAGIC;
	ovp->ov_first_block = old_ovp->ov_first_block;
	ovp->ov_first_offset = old_ovp->ov_first_offset;
	ovp->ov_total_length = old_ovp->ov_total_length;
	ovp->ov_inline_prefix = 0;
	ovp->ov_content_hash = old_ovp->ov_content_hash;

	return PointerGetDatum(result);
}

static RecnoTuple
recno_form_tuple_internal(TupleDesc tupdesc, Datum *values, bool *isnull,
						  Relation rel, RecnoOverflowBuffers *overflow_buffers,
						  bool force_shrink,
						  const RecnoOverflowPtr *old_ovptrs,
						  const bool *old_ovpresent)
{
	RecnoTuple	tuple;
	RecnoTupleHeader *header;
	Size		data_length;
	Size		tuple_length;
	Size		bitmap_len;
	char	   *data_ptr;
	uint8	   *nulls_bitmap;
	int			i;
	bool		has_nulls = false;
	bool		has_varwidth = false;
	bool		has_external = false;
	bool		has_compressed = false;
	bool		has_overflow = false;

	/*
	 * Working arrays for compressed/overflowed values. We attempt compression
	 * and overflow first, then compute the final tuple size using the
	 * (possibly compressed/overflowed) attribute values.
	 *
	 * Use stack arrays for small tuples (common OLTP case) to avoid palloc.
	 */
#define RECNO_FORM_STACK_ATTRS	16
	Datum	   *work_values;
	bool	   *is_compressed;	/* Track which attrs were compressed */
	bool	   *is_overflowed;	/* Track which attrs were overflowed */
	Datum		work_values_stack[RECNO_FORM_STACK_ATTRS];
	bool		is_compressed_stack[RECNO_FORM_STACK_ATTRS];
	bool		is_overflowed_stack[RECNO_FORM_STACK_ATTRS];

	Assert(tupdesc != NULL);
	Assert(values != NULL);
	Assert(isnull != NULL);

	if (tupdesc->natts <= RECNO_FORM_STACK_ATTRS)
	{
		work_values = work_values_stack;
		is_compressed = is_compressed_stack;
		is_overflowed = is_overflowed_stack;
		memset(is_compressed, 0, tupdesc->natts * sizeof(bool));
		memset(is_overflowed, 0, tupdesc->natts * sizeof(bool));
	}
	else
	{
		work_values = (Datum *) palloc(tupdesc->natts * sizeof(Datum));
		is_compressed = (bool *) palloc0(tupdesc->natts * sizeof(bool));
		is_overflowed = (bool *) palloc0(tupdesc->natts * sizeof(bool));
	}
	memcpy(work_values, values, tupdesc->natts * sizeof(Datum));

	/*
	 * Escrow numeric normalization.  If this relation has a numeric escrow
	 * column with a fixed-layout typmod, store that column in its fixed-width
	 * NumericLong image so every formed tuple (INSERT and the normal
	 * CAS/grow path) has a width-stable escrow value.  That is what lets the
	 * escrow fast path overwrite the running sum in place.  Only attempted
	 * when rel is available (the WAL/recovery form paths pass rel == NULL and
	 * operate on already-stored bytes).  Reads need no special handling: the
	 * fixed-layout image is a valid numeric.
	 */
	if (rel != NULL)
	{
		AttrNumber	esc_attn = RecnoEscrowAttnum(rel);

		if (esc_attn > 0)
		{
			Form_pg_attribute eatt = TupleDescAttr(tupdesc, esc_attn - 1);

			if (eatt->atttypid == NUMERICOID && !isnull[esc_attn - 1])
			{
				Size		img_len;

				if (numeric_fixed_layout_params(eatt->atttypmod, NULL, NULL,
												NULL, &img_len))
				{
					char	   *fx = (char *) palloc(img_len);

					numeric_to_fixed_layout((Numeric) DatumGetPointer(work_values[esc_attn - 1]),
											eatt->atttypmod, fx, img_len);
					work_values[esc_attn - 1] = PointerGetDatum(fx);
				}
			}
		}
	}

	/* Initialize overflow buffers if provided */
	if (overflow_buffers != NULL)
		overflow_buffers->count = 0;

	/*
	 * Phase 1: Attempt compression on eligible variable-length attributes.
	 * RecnoCompressAttribute returns the original value unchanged if
	 * compression is disabled, the value is too small, or compression did not
	 * achieve a worthwhile ratio.
	 */
	if (recno_enable_compression)
	{
		for (i = 0; i < tupdesc->natts; i++)
		{
			Form_pg_attribute att = TupleDescAttr(tupdesc, i);

			if (att->attisdropped || isnull[i])
				continue;

			/* Only compress variable-length, non-external attributes */
			if (att->attlen == -1 &&
				!VARATT_IS_EXTERNAL(DatumGetPointer(values[i])))
			{
				Datum		compressed;

				compressed = RecnoCompressAttribute(rel, values[i],
													att->atttypid,
													RECNO_COMP_NONE);

				if (compressed != values[i])
				{
					work_values[i] = compressed;
					is_compressed[i] = true;
					has_compressed = true;
				}
			}
		}
	}

	/*
	 * Phase 1b: Handle overflow for large attributes (if relation provided).
	 * Check each varlena attribute: if it exceeds the overflow threshold,
	 * store it in overflow records and replace the value with an overflow
	 * pointer.
	 */
	if (rel != NULL)
	{
		for (i = 0; i < tupdesc->natts; i++)
		{
			Form_pg_attribute att = TupleDescAttr(tupdesc, i);
			Size		attr_size;

			if (att->attisdropped || isnull[i])
				continue;

			if (att->attlen != -1)
				continue;		/* Only varlena attributes can overflow */

			if (VARATT_IS_EXTERNAL(DatumGetPointer(work_values[i])))
				continue;		/* Already external */

			attr_size = VARSIZE_ANY(DatumGetPointer(work_values[i]));

			/*
			 * In normal mode, only attributes exceeding the overflow
			 * threshold are pushed off-page.  In force_shrink mode (used to
			 * recover from a page-full in-place UPDATE), every eligible
			 * varlena is pushed off-page with a zero inline prefix, so the
			 * main tuple shrinks to its minimum footprint and can fit back
			 * into the old slot.
			 */
			if (!force_shrink && attr_size <= RECNO_OVERFLOW_THRESHOLD)
				continue;		/* Fits inline */

			/*
			 * Pushing an attribute that is no larger than its own overflow
			 * pointer would not save space, so skip it even under
			 * force_shrink.
			 */
			if (force_shrink && attr_size <= RECNO_OVERFLOW_PTR_SIZE)
				continue;

			/*
			 * Attribute exceeds threshold: store in overflow records.
			 * RecnoStoreOverflowColumn returns a varlena containing
			 * [RecnoOverflowPtr][inline_prefix], and collects buffers in
			 * overflow_buffers for atomic WAL logging by caller.
			 *
			 * UPDATE fast path: if this attribute had an on-page overflow
			 * pointer in the OLD tuple and its content is unchanged, COW-
			 * reference the existing chain instead of re-storing it.  This
			 * costs one chain fetch only on a hash match and writes no new
			 * overflow records or WAL.
			 */
			if (old_ovpresent != NULL && old_ovpresent[i])
			{
				Datum		cow;

				cow = recno_try_cow_overflow(rel, work_values[i],
											 &old_ovptrs[i]);
				if (cow != (Datum) 0)
				{
					work_values[i] = cow;
					is_overflowed[i] = true;
					has_overflow = true;
					continue;
				}
			}

			work_values[i] = RecnoStoreOverflowColumn(rel, work_values[i], i,
													  force_shrink ? 0 : recno_overflow_inline_prefix,
													  overflow_buffers);
			is_overflowed[i] = true;
			has_overflow = true;
		}
	}

	/*
	 * Phase 2: Calculate total space needed using (possibly
	 * compressed/overflowed) values
	 */
	data_length = RecnoComputeDataSize(tupdesc, work_values, isnull);
	tuple_length = data_length;

	/* Allocate tuple */
	tuple = (RecnoTuple) palloc0(sizeof(RecnoTupleData));
	tuple->t_len = tuple_length;
	tuple->t_data = (RecnoTupleHeader *) palloc0(tuple_length);

	/* Set up header */
	header = tuple->t_data;
	header->t_natts = tupdesc->natts;
	header->t_flags = 0;
	header->t_commit_ts = 0;	/* Will be set during insert */
	ItemPointerSetInvalid(&header->t_ctid);
	header->t_infomask = 0;

	if (has_compressed)
	{
		header->t_flags |= RECNO_TUPLE_COMPRESSED;
		header->t_infomask |= RECNO_INFOMASK_COMPRESSED;
	}

	/* Set up null bitmap */
	bitmap_len = BITMAPLEN(tupdesc->natts);
	nulls_bitmap = (uint8 *) header->t_attrs_bitmap;
	data_ptr = (char *) header + RECNO_TUPLE_OVERHEAD + MAXALIGN(bitmap_len);

	/*
	 * Initialize null bitmap - PostgreSQL expects all bits set to 1 initially
	 * (all NOT NULL)
	 */
	memset(nulls_bitmap, 0xFF, bitmap_len);

	/* Set infomask bits */
	for (i = 0; i < tupdesc->natts; i++)
	{
		if (isnull[i])
		{
			has_nulls = true;
			/* Clear the bit for NULL attributes (bit=0 means NULL) */
			nulls_bitmap[i >> 3] &= ~(1 << (i & 0x07));
		}
		else
		{
			Form_pg_attribute att = TupleDescAttr(tupdesc, i);

			if (att->attlen == -1 || att->attlen == -2)
				has_varwidth = true;

			/* Check for external storage */
			if (att->attlen == -1 && VARATT_IS_EXTERNAL(DatumGetPointer(work_values[i])))
				has_external = true;
		}
	}

	if (has_nulls)
		header->t_infomask |= RECNO_INFOMASK_HASNULL;
	if (has_varwidth)
		header->t_infomask |= RECNO_INFOMASK_HASVARWIDTH;
	if (has_external)
		header->t_infomask |= RECNO_INFOMASK_HASEXTERNAL;
	if (has_overflow)
	{
		header->t_flags |= RECNO_TUPLE_HAS_OVERFLOW;
		header->t_infomask |= RECNO_INFOMASK_HASOVERFLOW;
	}

	/*
	 * Phase 3: Store attribute values (using compressed data where
	 * applicable)
	 */
	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);

		if (att->attisdropped || isnull[i])
			continue;

		/* Align attribute */
		data_ptr = (char *) att_align_nominal(data_ptr, att->attalign);

		if (att->attlen > 0)
		{
			/*
			 * Fixed-length attribute - never compressed. Must distinguish
			 * byval from by-reference fixed-length types (e.g., timetz is 12
			 * bytes but passed by reference).
			 */
			if (att->attbyval)
				store_att_byval(data_ptr, work_values[i], att->attlen);
			else
				memcpy(data_ptr, DatumGetPointer(work_values[i]), att->attlen);
			data_ptr += att->attlen;
		}
		else if (att->attlen == -1)
		{
			/* Variable-length attribute (possibly compressed) */
			Size		attr_len = VARSIZE_ANY(DatumGetPointer(work_values[i]));

			memcpy(data_ptr, DatumGetPointer(work_values[i]), attr_len);
			data_ptr += attr_len;
		}
		else if (att->attlen == -2)
		{
			/* C string */
			Size		attr_len = strlen(DatumGetCString(work_values[i])) + 1;

			memcpy(data_ptr, DatumGetCString(work_values[i]), attr_len);
			data_ptr += attr_len;
		}
	}

	/*
	 * Free compressed and overflow datums that were allocated by
	 * RecnoCompressAttribute and RecnoStoreOverflowColumn
	 */
	for (i = 0; i < tupdesc->natts; i++)
	{
		if (is_compressed[i] || is_overflowed[i])
			pfree(DatumGetPointer(work_values[i]));
	}
	if (tupdesc->natts > RECNO_FORM_STACK_ATTRS)
	{
		pfree(work_values);
		pfree(is_compressed);
		pfree(is_overflowed);
	}

	return tuple;
}

/*
 * RecnoDeformTuple
 *
 * Extract attribute values and null indicators from a RECNO tuple into the
 * provided arrays.  This is the inverse of RecnoFormTuple().
 *
 * When the tuple has the RECNO_INFOMASK_COMPRESSED flag set, variable-length
 * attributes may contain a RecnoCompressionHeader prefix followed by
 * compressed data.  This function transparently decompresses such attributes
 * so that callers always see the original uncompressed Datum values.
 *
 * Parameters:
 *   tuple   - the RECNO tuple to deform
 *   tupdesc - tuple descriptor defining the schema
 *   values  - output array of Datum values (must be pre-allocated)
 *   isnull  - output array of boolean null indicators (must be pre-allocated)
 */
void
RecnoDeformTuple(Relation rel, RecnoTuple tuple, TupleDesc tupdesc, Datum *values, bool *isnull)
{
	RecnoTupleHeader *header;
	uint8	   *nulls_bitmap;
	char	   *data_ptr;
	Size		bitmap_len;
	int			i;
	bool		tuple_has_compressed;

	Assert(tuple != NULL);
	Assert(tupdesc != NULL);
	Assert(values != NULL);
	Assert(isnull != NULL);

	header = tuple->t_data;

	/*
	 * Use the tuple's actual natts for bitmap_len and data_ptr calculation.
	 * After ALTER TABLE ADD COLUMN, old tuples may have fewer attributes.
	 */
	{
		int			tuple_natts = header->t_natts;
		int			loop_natts = Min(tupdesc->natts, tuple_natts);

		bitmap_len = BITMAPLEN(tuple_natts);
		nulls_bitmap = (uint8 *) header->t_attrs_bitmap;
		data_ptr = (char *) header + RECNO_TUPLE_OVERHEAD + MAXALIGN(bitmap_len);

		tuple_has_compressed = (header->t_infomask & RECNO_INFOMASK_COMPRESSED) != 0;

		/* Extract each attribute present in the tuple */
		for (i = 0; i < loop_natts; i++)
		{
			Form_pg_attribute att = TupleDescAttr(tupdesc, i);

			if (att->attisdropped)
			{
				values[i] = (Datum) 0;
				isnull[i] = true;
				continue;
			}

			/*
			 * Check null bitmap: bit=0 means NULL (bit cleared in
			 * RecnoFormTuple)
			 */
			if (header->t_infomask & RECNO_INFOMASK_HASNULL &&
				att_isnull(i, nulls_bitmap))
			{
				values[i] = (Datum) 0;
				isnull[i] = true;
				continue;
			}

			isnull[i] = false;

			/* Align attribute */
			data_ptr = (char *) att_align_nominal(data_ptr, att->attalign);

			if (att->attlen > 0)
			{
				/*
				 * Fixed-length attribute - never compressed. Use actual
				 * attbyval flag (e.g., timetz is 12 bytes but by-ref).
				 */
				values[i] = fetch_att(data_ptr, att->attbyval, att->attlen);
				data_ptr += att->attlen;
			}
			else if (att->attlen == -1)
			{
				/* Variable-length attribute - may be compressed */
				Size		attr_len = VARSIZE_ANY(data_ptr);

				if (tuple_has_compressed)
				{
					/*
					 * Check if this varlena contains a compression header. A
					 * compressed attribute has VARHDRSZ +
					 * RecnoCompressionHeader + compressed payload. We
					 * identify it by checking the comp_type field in the
					 * header position.
					 */
					Size		data_size = VARSIZE_ANY_EXHDR(data_ptr);

					if (data_size >= sizeof(RecnoCompressionHeader))
					{
						RecnoCompressionHeader *comp_hdr =
							(RecnoCompressionHeader *) VARDATA_ANY(data_ptr);

						if (comp_hdr->comp_type > RECNO_COMP_NONE &&
							comp_hdr->comp_type <= RECNO_COMP_DICTIONARY &&
							comp_hdr->comp_size > 0 &&
							comp_hdr->orig_size > 0 &&
							comp_hdr->comp_size + sizeof(RecnoCompressionHeader) <= data_size)
						{
							/* This attribute is compressed - decompress it */
							values[i] = RecnoDecompressAttribute(
																 rel ? RelationGetRelid(rel) : InvalidOid,
																 PointerGetDatum(data_ptr),
																 att->atttypid,
																 comp_hdr);
							data_ptr += attr_len;
							continue;
						}
					}
				}

				/* Not compressed (or compression not detected) - return as-is */
				values[i] = PointerGetDatum(data_ptr);
				data_ptr += attr_len;
			}
			else if (att->attlen == -2)
			{
				/* C string - never compressed */
				values[i] = CStringGetDatum(data_ptr);
				data_ptr += strlen(data_ptr) + 1;
			}
			else
			{
				elog(ERROR, "unsupported attribute length: %d", att->attlen);
			}
		}

		/*
		 * Fill missing attributes with defaults for columns added by ALTER
		 * TABLE ADD COLUMN after this tuple was stored.
		 */
		for (i = loop_natts; i < tupdesc->natts; i++)
		{
			values[i] = (Datum) 0;
			isnull[i] = true;
		}
	}							/* end of tuple_natts scope block */
}

/*
 * RecnoFreeTuple
 *
 * Free a RECNO tuple and its associated data.  Safe to call with NULL.
 *
 * Parameters:
 *   tuple - the RecnoTuple to free (may be NULL)
 */
void
RecnoFreeTuple(RecnoTuple tuple)
{
	if (tuple)
	{
		if (tuple->t_data)
			pfree(tuple->t_data);
		pfree(tuple);
	}
}

/*
 * RecnoInitPage
 *
 * Initialize a new RECNO page.  Calls PostgreSQL's PageInit() with space
 * reserved for RecnoPageOpaqueData in the special area, then initializes
 * the opaque data fields to their default values.
 *
 * Parameters:
 *   page     - pointer to the page buffer
 *   pageSize - size of the page (typically BLCKSZ = 8192)
 */
void
RecnoInitPage(Page page, Size pageSize)
{
	RecnoPageOpaque phdr;

	PageInit(page, pageSize, sizeof(RecnoPageOpaqueData));

	phdr = RecnoPageGetOpaque(page);
	phdr->pd_commit_ts_and_flags = 0;
}

/*
 * RecnoPageAddTuple
 *
 * Add a RECNO tuple to a page using PageAddItem().  Updates the page's
 * opaque data (commit timestamp, free space) after successful insertion.
 *
 * Parameters:
 *   page       - the page to add the tuple to (must be exclusively locked)
 *   tuple      - the RECNO tuple to add
 *   tuple_size - size of the tuple data in bytes
 *
 * Returns the OffsetNumber where the tuple was placed, or
 * InvalidOffsetNumber if the page does not have enough space.
 */
OffsetNumber
RecnoPageAddTuple(Page page, RecnoTuple tuple, Size tuple_size)
{
	RecnoPageOpaque phdr;
	OffsetNumber offnum;

	/* Try to add the tuple */
	offnum = PageAddItem(page, tuple->t_data, tuple_size,
						 InvalidOffsetNumber, false, false);

	if (offnum == InvalidOffsetNumber)
		return InvalidOffsetNumber;

	/* Update page header */
	phdr = RecnoPageGetOpaque(page);

	/* Mark page for defragmentation if fragmented */
	if (PageGetFreeSpace(page) >= tuple_size * 2 &&
		PageGetMaxOffsetNumber(page) > FirstOffsetNumber + 5)
	{
		RecnoPageSetFlag(phdr, RECNO_PAGE_DEFRAG_NEEDED);
	}

	return offnum;
}

/*
 * RecnoPageUpdateTuple
 *
 * Attempt to update a tuple in place on a RECNO page.  If the new tuple
 * fits within the existing allocation (same size or smaller), the data is
 * overwritten directly (in-place update).  If the new tuple is larger but
 * the page has enough total free space, the old tuple is removed and the
 * new tuple is added at the same or a new offset.
 *
 * Parameters:
 *   page           - the page containing the tuple (must be exclusively locked)
 *   offnum         - offset number of the tuple to update
 *   new_tuple      - the new tuple data
 *   old_commit_ts  - commit timestamp of the old version (for WAL logging)
 *   new_commit_ts  - commit timestamp for the new version
 *
 * Returns true if the update was performed on this page, false if the new
 * tuple does not fit (caller must handle cross-page update).
 */
bool
RecnoPageUpdateTuple(Page page, OffsetNumber offnum, RecnoTuple new_tuple,
					 uint64 old_commit_ts, uint64 new_commit_ts)
{
	ItemId		itemid;
	RecnoTupleHeader *old_tuple;
	Size		old_size,
				new_size;
	RecnoPageOpaque phdr;
	Size		available_space;
	OffsetNumber new_offnum;

	itemid = PageGetItemId(page, offnum);
	if (!ItemIdIsNormal(itemid))
		return false;

	old_tuple = (RecnoTupleHeader *) PageGetItem(page, itemid);
	old_size = ItemIdGetLength(itemid);
	new_size = new_tuple->t_len;

	/* Check if new tuple fits in same space */
	if (new_size <= old_size)
	{
		/* In-place update */
		memcpy(old_tuple, new_tuple->t_data, new_size);
		if (new_size < old_size)
		{
			/* Update item length */
			ItemIdSetNormal(itemid, ItemIdGetOffset(itemid), new_size);
		}

		/* Update page header */
		phdr = RecnoPageGetOpaque(page);
		RecnoPageSetCommitTs(phdr, Max(RecnoPageGetCommitTs(phdr), new_commit_ts));

		return true;
	}

	/* Need more space - check if available */
	available_space = PageGetFreeSpace(page) + old_size;
	if (new_size <= available_space)
	{
		/*
		 * Remove old tuple and re-add the new (larger) one.
		 *
		 * We use RecnoPageIndexTupleDelete instead of PageIndexTupleDelete
		 * because the page may contain LP_UNUSED items from defragmentation.
		 * PageIndexTupleDelete asserts all items are LP_NORMAL;
		 * RecnoPageIndexTupleDelete skips LP_UNUSED items safely.
		 */
		RecnoPageIndexTupleDelete(page, offnum);

		new_offnum = PageAddItem(page, new_tuple->t_data,
								 new_size, offnum,
								 false, false);

		if (new_offnum != InvalidOffsetNumber)
		{
			/* Update page header */
			phdr = RecnoPageGetOpaque(page);
			RecnoPageSetCommitTs(phdr, Max(RecnoPageGetCommitTs(phdr), new_commit_ts));
			return true;
		}
	}

	return false;				/* Update failed - need new page */
}

/*
 * Get number of live tuples on a RECNO page
 */
int
RecnoPageGetLiveTuples(Page page, uint64 snapshot_ts)
{
	OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
	int			live_tuples = 0;
	OffsetNumber offnum;

	for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum++)
	{
		ItemId		itemid = PageGetItemId(page, offnum);

		if (ItemIdIsNormal(itemid))
		{
			RecnoTupleHeader *tuple = (RecnoTupleHeader *) PageGetItem(page, itemid);

			/* Skip overflow records */
			if (RecnoIsOverflowRecord(tuple, ItemIdGetLength(itemid)))
				continue;

			/*
			 * Heap-shaped: "live" here means not carrying a committed delete
			 * marker.  This helper is only an estimate (no current callers), so
			 * it counts non-DELETED tuples rather than doing a full snapshot
			 * visibility check.  snapshot_ts is unused.
			 */
			(void) snapshot_ts;
			if (!(tuple->t_flags & RECNO_TUPLE_DELETED))
				live_tuples++;
		}
	}

	return live_tuples;
}

/*
 * RecnoPageDefragment
 *
 * Compact a RECNO page by calling PageRepairFragmentation() to consolidate
 * free space.  Updates the page opaque data with the new free space amount,
 * increments the defrag counter, and clears the RECNO_PAGE_DEFRAG_NEEDED flag.
 *
 * Parameters:
 *   page - the page to defragment (must be exclusively locked)
 */
void
RecnoPageDefragment(Page page)
{
	RecnoPageOpaque phdr = RecnoPageGetOpaque(page);

	/* Use standard PageRepairFragmentation */
	PageRepairFragmentation(page);

	/* Update page header */
	RecnoPageClearFlag(phdr, RECNO_PAGE_DEFRAG_NEEDED);
}

/*
 * RecnoPageIndexTupleDelete
 *
 * Like PageIndexTupleDelete, but tolerates LP_UNUSED items on the page.
 *
 * Standard PageIndexTupleDelete asserts that ALL line pointers have storage
 * (ItemIdHasStorage).  RECNO pages may contain LP_UNUSED items left behind
 * by opportunistic defragmentation.  This function skips LP_UNUSED items
 * when adjusting offsets, preventing both assertion failures and data
 * corruption (LP_UNUSED items have lp_off=0 and must not be adjusted).
 */
void
RecnoPageIndexTupleDelete(Page page, OffsetNumber offnum)
{
	PageHeader	phdr = (PageHeader) page;
	char	   *addr;
	ItemId		tup;
	Size		size;
	unsigned	offset;
	int			nbytes;
	int			offidx;
	int			nline;

	if (phdr->pd_lower < SizeOfPageHeaderData ||
		phdr->pd_lower > phdr->pd_upper ||
		phdr->pd_upper > phdr->pd_special ||
		phdr->pd_special > BLCKSZ ||
		phdr->pd_special != MAXALIGN(phdr->pd_special))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("corrupted page pointers: lower = %u, upper = %u, special = %u",
						phdr->pd_lower, phdr->pd_upper, phdr->pd_special)));

	nline = PageGetMaxOffsetNumber(page);
	if ((int) offnum <= 0 || (int) offnum > nline)
		elog(ERROR, "invalid index offnum: %u", offnum);

	offidx = offnum - 1;

	tup = PageGetItemId(page, offnum);
	Assert(ItemIdHasStorage(tup));
	size = ItemIdGetLength(tup);
	offset = ItemIdGetOffset(tup);

	if (offset < phdr->pd_upper || (offset + size) > phdr->pd_special ||
		offset != MAXALIGN(offset))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("corrupted line pointer: offset = %u, size = %zu",
						offset, size)));

	size = MAXALIGN(size);

	/* Remove the line pointer entry by shifting subsequent entries down */
	nbytes = phdr->pd_lower -
		((char *) &phdr->pd_linp[offidx + 1] - (char *) phdr);

	if (nbytes > 0)
		memmove(&(phdr->pd_linp[offidx]),
				&(phdr->pd_linp[offidx + 1]),
				nbytes);

	/* Shift tuple data forward to fill the gap */
	addr = (char *) page + phdr->pd_upper;

	if (offset > phdr->pd_upper)
		memmove(addr + size, addr, offset - phdr->pd_upper);

	phdr->pd_upper += size;
	phdr->pd_lower -= sizeof(ItemIdData);

	/* Adjust remaining line pointer offsets, skipping LP_UNUSED items */
	if (!PageIsEmpty(page))
	{
		int			i;

		nline--;
		for (i = 1; i <= nline; i++)
		{
			ItemId		ii = PageGetItemId(page, i);

			if (!ItemIdHasStorage(ii))
				continue;
			if (ItemIdGetOffset(ii) <= offset)
				ii->lp_off += size;
		}
	}
}

/*
 * Convert a RECNO tuple to a TupleTableSlot
 *
 * This is the primary retrieval path used during sequential scans.
 * When the tuple has compressed attributes (RECNO_INFOMASK_COMPRESSED),
 * they are transparently decompressed so the slot always contains
 * uncompressed data visible to the executor.
 *
 * Overflow attributes (RECNO_INFOMASK_HASOVERFLOW) are returned as-is
 * by this function since it has no Relation handle.  Use
 * RecnoTupleToSlotWithOverflow() for transparent overflow fetching.
 */
bool
RecnoTupleToSlot(RecnoTupleHeader *tuple_header, TupleTableSlot *slot)
{
	return RecnoTupleToSlotWithOverflow(tuple_header, slot, NULL);
}

/*
 * Convert a RECNO tuple to a TupleTableSlot with overflow support.
 *
 * When rel is non-NULL and the tuple has overflow attributes, they are
 * transparently fetched from overflow records and the slot receives the
 * complete original values.  When rel is NULL, overflow pointers are
 * returned as-is (same as RecnoTupleToSlot).
 */
bool
RecnoTupleToSlotWithOverflow(RecnoTupleHeader *tuple_header,
							 TupleTableSlot *slot, Relation rel)
{
	TupleDesc	tupdesc = slot->tts_tupleDescriptor;
	char	   *data_ptr;
	uint8	   *nulls_bitmap;
	int			i;
	Size		bitmap_len;
	bool		tuple_has_compressed;
	bool		tuple_has_overflow;

	if (!tuple_header)
		return false;

	/* Check if tuple is deleted */
	if (tuple_header->t_flags & RECNO_TUPLE_DELETED)
		return false;

	/* Clear the slot first */
	ExecClearTuple(slot);

	/*
	 * Use the tuple's actual natts for bitmap_len and data_ptr calculation.
	 * After ALTER TABLE ADD COLUMN, old tuples may have fewer attributes than
	 * the current schema expects.
	 */
	{
		int			tuple_natts = tuple_header->t_natts;
		int			loop_natts = Min(tupdesc->natts, tuple_natts);

		bitmap_len = BITMAPLEN(tuple_natts);

		/* Set up pointers to data */
		nulls_bitmap = (uint8 *) tuple_header->t_attrs_bitmap;
		data_ptr = (char *) tuple_header + RECNO_TUPLE_OVERHEAD + MAXALIGN(bitmap_len);

		tuple_has_compressed = (tuple_header->t_infomask & RECNO_INFOMASK_COMPRESSED) != 0;
		tuple_has_overflow = (tuple_header->t_flags & RECNO_TUPLE_HAS_OVERFLOW) != 0;

		/* Decode each attribute present in the tuple */
		for (i = 0; i < loop_natts; i++)
		{
			Form_pg_attribute att = TupleDescAttr(tupdesc, i);
			bool		is_null;

			if (att->attisdropped)
			{
				slot->tts_values[i] = (Datum) 0;
				slot->tts_isnull[i] = true;
				continue;
			}

			/* Check if attribute is null */
			is_null = att_isnull(i, nulls_bitmap);

			if (is_null)
			{
				slot->tts_values[i] = (Datum) 0;
				slot->tts_isnull[i] = true;
			}
			else
			{
				/* Extract the actual data */
				if (att->attlen == -1)
				{
					Size		attr_len;

					/*
					 * Align to the start of this varlena, matching the form
					 * path (RecnoFormTuple aligns data_ptr at the start of each
					 * attribute before writing).  Without this the previous
					 * attribute's raw += attr_len advance can leave data_ptr
					 * unaligned, so VARSIZE_ANY reads the length from padding
					 * bytes -> garbage length -> pglz-corrupt / overrun.
					 */
					data_ptr = (char *) att_align_nominal(data_ptr, att->attalign);
					attr_len = VARSIZE_ANY(data_ptr);

					/*
					 * Check for overflow pointer first (takes priority over
					 * compression since the on-disk data is an overflow
					 * pointer, not the compressed payload).
					 */
					if (tuple_has_overflow && RecnoIsOverflowPtr(data_ptr))
					{
						if (rel != NULL)
						{
							/* Fetch the full column value from overflow chain */
							Datum		fetched = RecnoFetchOverflowColumn(rel, data_ptr);

							/*
							 * The fetched data may be a compressed varlena,
							 * since RecnoFormTuple compresses before
							 * overflowing.  Check the fetched value (not
							 * data_ptr) for a compression header and
							 * decompress if needed.
							 */
							if (tuple_has_compressed)
							{
								char	   *fetched_ptr = DatumGetPointer(fetched);
								Size		fdata_size = VARSIZE_ANY_EXHDR(fetched_ptr);

								if (fdata_size >= sizeof(RecnoCompressionHeader))
								{
									RecnoCompressionHeader *comp_hdr =
										(RecnoCompressionHeader *) VARDATA_ANY(fetched_ptr);

									if (comp_hdr->comp_type > RECNO_COMP_NONE &&
										comp_hdr->comp_type <= RECNO_COMP_DICTIONARY &&
										comp_hdr->comp_size > 0 &&
										comp_hdr->orig_size > 0 &&
										comp_hdr->comp_size + sizeof(RecnoCompressionHeader) <= fdata_size)
									{
										slot->tts_values[i] = RecnoDecompressAttribute(
																					   slot->tts_tableOid,
																					   fetched,
																					   att->atttypid,
																					   comp_hdr);
										slot->tts_isnull[i] = false;
										data_ptr += attr_len;
										continue;
									}
								}
							}

							/* Not compressed - use fetched data as-is */
							slot->tts_values[i] = fetched;
						}
						else
						{
							/* No relation - return overflow pointer as-is */
							slot->tts_values[i] = PointerGetDatum(data_ptr);
						}
						slot->tts_isnull[i] = false;
						data_ptr += attr_len;
						continue;
					}

					if (tuple_has_compressed)
					{
						/*
						 * Check for compression header in this varlena
						 * attribute.
						 */
						Size		data_size = VARSIZE_ANY_EXHDR(data_ptr);

						if (data_size >= sizeof(RecnoCompressionHeader))
						{
							RecnoCompressionHeader *comp_hdr =
								(RecnoCompressionHeader *) VARDATA_ANY(data_ptr);

							if (comp_hdr->comp_type > RECNO_COMP_NONE &&
								comp_hdr->comp_type <= RECNO_COMP_DICTIONARY &&
								comp_hdr->comp_size > 0 &&
								comp_hdr->orig_size > 0 &&
								comp_hdr->comp_size + sizeof(RecnoCompressionHeader) <= data_size)
							{
								/* Decompress the attribute */
								slot->tts_values[i] = RecnoDecompressAttribute(
																			   slot->tts_tableOid,
																			   PointerGetDatum(data_ptr),
																			   att->atttypid,
																			   comp_hdr);
								slot->tts_isnull[i] = false;
								data_ptr = (char *) att_align_nominal(
																	  data_ptr + attr_len, att->attalign);
								continue;
							}
						}
					}

					/* Not compressed or overflow - return as-is */
					slot->tts_values[i] = PointerGetDatum(data_ptr);
					data_ptr = (char *) att_align_nominal(data_ptr + attr_len, att->attalign);
				}
				else if (att->attlen > 0)
				{
					/* Fixed-length attribute - never compressed or overflow */
					data_ptr = (char *) att_align_nominal(data_ptr, att->attalign);
					slot->tts_values[i] = fetchatt(att, data_ptr);
					data_ptr += att->attlen;
				}
				else
				{
					/* This shouldn't happen */
					elog(ERROR, "unsupported attribute length: %d", att->attlen);
				}

				slot->tts_isnull[i] = false;
			}
		}

		/*
		 * Fill missing attributes with defaults for columns added by ALTER
		 * TABLE ADD COLUMN after this tuple was stored.
		 */
		if (loop_natts < tupdesc->natts)
		{
			for (i = loop_natts; i < tupdesc->natts; i++)
			{
				slot->tts_values[i] = (Datum) 0;
				slot->tts_isnull[i] = true;
			}
			slot->tts_nvalid = loop_natts;
			slot_getmissingattrs(slot, loop_natts, tupdesc->natts);
		}
	}							/* end of tuple_natts scope block */

	/* Mark slot as valid */
	slot->tts_flags &= ~TTS_FLAG_EMPTY;
	slot->tts_nvalid = tupdesc->natts;

	return true;
}
