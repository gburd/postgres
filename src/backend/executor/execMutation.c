/*-------------------------------------------------------------------------
 *
 * execMutation.c
 *    Sub-attribute mutation tracking for UPDATE HOT optimization.
 *
 * src/backend/executor/execMutation.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "executor/execMutation.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/tupdesc.h"
#include "fmgr.h"
#include "nodes/bitmapset.h"
#include "utils/idxsubattr.h"
#include "utils/memutils.h"
#include "varatt.h"

void
add_modified_idx_attr(Bitmapset **mix_attrs, MemoryContext mix_mcxt,
					  AttrNumber attnum)
{
	MemoryContext oldcxt;
	int			attidx;

	Assert(mix_attrs != NULL);
	Assert(AttributeNumberIsValid(attnum));

	attidx = attnum - FirstLowInvalidHeapAttributeNumber;

	/*
	 * Switch to the per-query memory context (mix_mcxt) before allocating the
	 * Bitmapset.  This ensures the accumulator survives per-tuple expression
	 * context resets between ExecProcNode and
	 * ExecCheckIndexedAttrsForChanges.
	 */
	oldcxt = MemoryContextSwitchTo(mix_mcxt);
	*mix_attrs = bms_add_member(*mix_attrs, attidx);
	MemoryContextSwitchTo(oldcxt);
}

/*----------
 * HeapCheckSubattrChanges - refine modified index attributes via sub-attribute comparison
 *
 * For each attribute number in 'check_attrs' (encoded with
 * FirstLowInvalidHeapAttributeNumber offset as used by the bitmapset
 * conventions in heapam.c), check whether the indexed sub-attributes
 * actually changed between oldtup and newtup.
 *
 * Returns a Bitmapset of attribute numbers (same encoding) where
 * the indexed sub-attributes did NOT change -- these can be removed from
 * the modified index attributes set.
 *
 * Dual-path architecture
 * ----------------------
 * Sub-attribute modification tracking uses two complementary strategies:
 *
 *   1. Instrumented path (executor only): Mutation functions
 *      (jsonb_set, jsonb_delete, jsonb_insert, etc.) receive a
 *      SubattrTrackingContext via fcinfo->context.  The context carries
 *      SubattrInfo descriptors that let the function check whether its
 *      modification path intersects any indexed sub-attribute path.
 *      Crucially, the context does NOT contain a Relation pointer --
 *      functions only see the sub-attribute descriptors, not the
 *      underlying relation.  When a function determines that its
 *      modification intersects an indexed path, it sets
 *      fcinfo->modified_idx_subattr = true.  After the function returns,
 *      the executor's ACCUMULATE_SUBATTR_MODIFICATIONS macro checks the
 *      flag and, if set, records the column in ri_ModifiedIdxAttrs via
 *      the context's target_attnum.  This is the fast path -- it avoids
 *      re-reading and re-comparing old/new values entirely.
 *
 *   2. Fallback path (this function): For non-executor callers
 *      (simple_heap_update, catalog operations) where instrumentation
 *      is unavailable, and for executor updates with uninstrumented
 *      mutation functions (direct assignment, opaque functions, etc.).
 *      Extracts old and new column values, then calls the type-specific
 *      comparator (e.g. jsonb_idx_compare, xml_idx_compare) to check
 *      each indexed sub-attribute individually.
 *
 * For typical JSONB workloads with expression indexes, the instrumented
 * path avoids the full-value comparison, yielding significant speedups.
 *
 *
 * TOAST safety
 * ------------
 * This function handles TOAST values correctly:
 *   - Inline-compressed values: decompressed in-memory (safe).
 *   - Externally-TOASTed values: skipped conservatively.  Detoasting
 *     external values would read TOAST relation pages, risking
 *     lock-ordering issues when the caller holds a buffer lock.
 *     Skipping means we treat the column as changed, which is safe
 *     (correctly identifies the attribute as modified but may be conservative).
 *----------
 */
Bitmapset *
HeapCheckSubattrChanges(Relation relation,
						HeapTuple oldtup,
						HeapTuple newtup,
						Bitmapset *check_attrs)
{
	RelSubattrInfo *subattr_info;
	TupleDesc	tupdesc;
	Bitmapset  *safe_attrs = NULL;
	int			bms_idx;

	subattr_info = RelationGetIdxSubattrs(relation);
	if (subattr_info == NULL)
		return NULL;

	tupdesc = RelationGetDescr(relation);

	bms_idx = -1;
	while ((bms_idx = bms_next_member(check_attrs, bms_idx)) >= 0)
	{
		AttrNumber	realattnum;
		SubattrInfo *attr_info;
		bool		old_isnull;
		bool		new_isnull;
		Datum		old_val;
		Datum		new_val;
		bool		subattr_changed;

		realattnum = bms_idx + FirstLowInvalidHeapAttributeNumber;

		/* Only user-defined attributes can have subattr info */
		if (realattnum < 1 || realattnum > tupdesc->natts)
			continue;

		/*
		 * Skip attributes that are also referenced by a simple (whole-column)
		 * index.  For those, any byte change requires an index update
		 * regardless of subattr analysis.
		 */
		if (bms_is_member(bms_idx, subattr_info->simple_indexed_attrs))
			continue;

		/* Quick membership test before linear scan */
		if (!bms_is_member(bms_idx, subattr_info->subattr_attrs))
			continue;

		/* Look up subattr info for this attribute */
		attr_info = NULL;
		for (int i = 0; i < subattr_info->nattrs; i++)
		{
			if (subattr_info->attrs[i].attnum == realattnum)
			{
				attr_info = &subattr_info->attrs[i];
				break;
			}
		}

		if (attr_info == NULL || !attr_info->has_comparefn)
			continue;

		/* Extract old and new values */
		old_val = heap_getattr(oldtup, realattnum, tupdesc, &old_isnull);
		new_val = heap_getattr(newtup, realattnum, tupdesc, &new_isnull);

		/* NULL transitions always count as changed */
		if (old_isnull != new_isnull)
			continue;

		/* Both NULL: effectively unchanged for index purposes */
		if (old_isnull)
		{
			safe_attrs = bms_add_member(safe_attrs, bms_idx);
			continue;
		}

		/*
		 * For varlena types, skip externally-TOASTed values.  We cannot
		 * safely detoast while the caller holds a buffer lock because
		 * detoasting reads from the TOAST relation (acquires buffer pins on
		 * different pages, risking lock-ordering issues).
		 *
		 * Inline-compressed values are fine -- decompression is purely
		 * in-memory.
		 */
		if (TupleDescAttr(tupdesc, realattnum - 1)->attlen == -1)
		{
			struct varlena *old_ptr = (struct varlena *) DatumGetPointer(old_val);
			struct varlena *new_ptr = (struct varlena *) DatumGetPointer(new_val);

			if (VARATT_IS_EXTERNAL(old_ptr) || VARATT_IS_EXTERNAL(new_ptr))
				continue;		/* conservative: treat as changed */
		}

		/*
		 * Call the type-specific subattr comparator.  The function receives
		 * the old value, new value, descriptor array, and descriptor count.
		 * Returns true if any indexed subattr value differs between old and
		 * new.
		 */
		subattr_changed = DatumGetBool(
									   FunctionCall4(&attr_info->comparefn,
													 old_val,
													 new_val,
													 PointerGetDatum(attr_info->descriptors),
													 Int32GetDatum(attr_info->ndescriptors)));

		if (!subattr_changed)
		{
			safe_attrs = bms_add_member(safe_attrs, bms_idx);
		}
	}

	return safe_attrs;
}
