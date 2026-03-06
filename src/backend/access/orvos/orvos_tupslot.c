/*
 * orvos_tupslot.c
 *		Implementation of a TupleTableSlot for orvos.
 *
 * This implementation is identical to a Virtual tuple slot
 * (TTSOpsVirtual), but it has a slot_getsysattr() implementation
 * that can fetch and compute the 'xmin' for the tuple.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/orvos/orvos_tupslot.c
 */
#include "postgres.h"

#include "access/table.h"
#include "access/orvos_internal.h"
#include "executor/tuptable.h"
#include "utils/expandeddatum.h"

const TupleTableSlotOps TTSOpsOrvos;

static void
tts_orvos_init(TupleTableSlot *slot)
{
	OrvosTupleTableSlot *ovslot = (OrvosTupleTableSlot *) slot;

	ovslot->visi_info = NULL;
}

static void
tts_orvos_release(TupleTableSlot *slot)
{
}

static void
tts_orvos_clear(TupleTableSlot *slot)
{
	OrvosTupleTableSlot *ovslot = (OrvosTupleTableSlot *) slot;

	if (unlikely(TTS_SHOULDFREE(slot)))
	{
		pfree(ovslot->data);
		ovslot->data = NULL;

		slot->tts_flags &= ~TTS_FLAG_SHOULDFREE;
	}

	slot->tts_nvalid = 0;
	slot->tts_flags |= TTS_FLAG_EMPTY;
	ItemPointerSetInvalid(&slot->tts_tid);

	ovslot->visi_info = NULL;
}

/*
 * Attribute values are readily available in tts_values and tts_isnull array
 * in a OrvosTupleTableSlot. So there should be no need to call either of the
 * following two functions.
 */
static void
tts_orvos_getsomeattrs(TupleTableSlot *slot, int natts)
{
	elog(ERROR, "getsomeattrs is not required to be called on a orvos tuple table slot");
}

/*
 * We only support fetching 'xmin', currently. It's needed for referential
 * integrity triggers (i.e. foreign keys).
 */
static Datum
tts_orvos_getsysattr(TupleTableSlot *slot, int attnum, bool *isnull)
{
	OrvosTupleTableSlot *ovslot = (OrvosTupleTableSlot *) slot;

	if (attnum == MinTransactionIdAttributeNumber ||
		attnum == MinCommandIdAttributeNumber)
	{
		*isnull = false;
		if (attnum == MinTransactionIdAttributeNumber)
			return ovslot->visi_info ? TransactionIdGetDatum(ovslot->visi_info->xmin) : InvalidTransactionId;
		else
		{
			Assert(attnum == MinCommandIdAttributeNumber);
			return ovslot->visi_info ? CommandIdGetDatum(ovslot->visi_info->cmin) : InvalidCommandId;
		}
	}
	elog(ERROR, "orvos tuple table slot does not have system attributes (except xmin and cmin)");

	return 0;					/* silence compiler warnings */
}

/*
 * To materialize a orvos slot all the datums that aren't passed by value
 * have to be copied into the slot's memory context.  To do so, compute the
 * required size, and allocate enough memory to store all attributes.  That's
 * good for cache hit ratio, but more importantly requires only memory
 * allocation/deallocation.
 */
static void
tts_orvos_materialize(TupleTableSlot *slot)
{
	OrvosTupleTableSlot *vslot = (OrvosTupleTableSlot *) slot;
	TupleDesc	desc = slot->tts_tupleDescriptor;
	Size		sz = 0;
	char	   *data;

	/* already materialized */
	if (TTS_SHOULDFREE(slot))
		return;

	/* copy visibility information to go with the slot */
	if (vslot->visi_info)
	{
		vslot->visi_info_buf = *vslot->visi_info;
		vslot->visi_info = &vslot->visi_info_buf;
	}

	/* compute size of memory required */
	for (int natt = 0; natt < desc->natts; natt++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, natt);
		Datum		val;

		if (att->attbyval || slot->tts_isnull[natt])
			continue;

		val = slot->tts_values[natt];

		if (att->attlen == -1 &&
			VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(val)))
		{
			/*
			 * We want to flatten the expanded value so that the materialized
			 * slot doesn't depend on it.
			 */
			sz = att_align_nominal(sz, att->attalign);
			sz += EOH_get_flat_size(DatumGetEOHP(val));
		}
		else
		{
			sz = att_align_nominal(sz, att->attalign);
			sz = att_addlength_datum(sz, att->attlen, val);
		}
	}

	/* all data is byval */
	if (sz == 0)
		return;

	/* allocate memory */
	vslot->data = data = MemoryContextAlloc(slot->tts_mcxt, sz);
	slot->tts_flags |= TTS_FLAG_SHOULDFREE;

	/* and copy all attributes into the pre-allocated space */
	for (int natt = 0; natt < desc->natts; natt++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, natt);
		Datum		val;

		if (att->attbyval || slot->tts_isnull[natt])
			continue;

		val = slot->tts_values[natt];

		if (att->attlen == -1 &&
			VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(val)))
		{
			Size		data_length;

			/*
			 * We want to flatten the expanded value so that the materialized
			 * slot doesn't depend on it.
			 */
			ExpandedObjectHeader *eoh = DatumGetEOHP(val);

			data = (char *) att_align_nominal(data,
											  att->attalign);
			data_length = EOH_get_flat_size(eoh);
			EOH_flatten_into(eoh, data, data_length);

			slot->tts_values[natt] = PointerGetDatum(data);
			data += data_length;
		}
		else
		{
			Size		data_length = 0;

			data = (char *) att_align_nominal(data, att->attalign);
			data_length = att_addlength_datum(data_length, att->attlen, val);

			memcpy(data, DatumGetPointer(val), data_length);

			slot->tts_values[natt] = PointerGetDatum(data);
			data += data_length;
		}
	}
}

static void
tts_orvos_copyslot(TupleTableSlot *dstslot, TupleTableSlot *srcslot)
{
	OrvosTupleTableSlot *ovdstslot = (OrvosTupleTableSlot *) dstslot;

	TupleDesc	srcdesc = dstslot->tts_tupleDescriptor;

	Assert(srcdesc->natts <= dstslot->tts_tupleDescriptor->natts);

	tts_orvos_clear(dstslot);

	slot_getallattrs(srcslot);

	for (int natt = 0; natt < srcdesc->natts; natt++)
	{
		dstslot->tts_values[natt] = srcslot->tts_values[natt];
		dstslot->tts_isnull[natt] = srcslot->tts_isnull[natt];
	}

	if (srcslot->tts_ops == &TTSOpsOrvos)
		ovdstslot->visi_info = ((OrvosTupleTableSlot *) srcslot)->visi_info;
	else
		ovdstslot->visi_info = NULL;

	dstslot->tts_nvalid = srcdesc->natts;
	dstslot->tts_flags &= ~TTS_FLAG_EMPTY;

	/* make sure storage doesn't depend on external memory */
	tts_orvos_materialize(dstslot);
}

static HeapTuple
tts_orvos_copy_heap_tuple(TupleTableSlot *slot)
{
	Assert(!TTS_EMPTY(slot));

	return heap_form_tuple(slot->tts_tupleDescriptor,
						   slot->tts_values,
						   slot->tts_isnull);
}

static MinimalTuple
tts_orvos_copy_minimal_tuple(TupleTableSlot *slot, Size extra)
{
	Assert(!TTS_EMPTY(slot));

	return heap_form_minimal_tuple(slot->tts_tupleDescriptor,
								   slot->tts_values,
								   slot->tts_isnull,
								   extra);
}


const TupleTableSlotOps TTSOpsOrvos = {
	.base_slot_size = sizeof(OrvosTupleTableSlot),
	.init = tts_orvos_init,
	.release = tts_orvos_release,
	.clear = tts_orvos_clear,
	.getsomeattrs = tts_orvos_getsomeattrs,
	.getsysattr = tts_orvos_getsysattr,
	.materialize = tts_orvos_materialize,
	.copyslot = tts_orvos_copyslot,

	/*
	 * A orvos tuple table slot can not "own" a heap tuple or a minimal tuple.
	 */
	.get_heap_tuple = NULL,
	.get_minimal_tuple = NULL,
	.copy_heap_tuple = tts_orvos_copy_heap_tuple,
	.copy_minimal_tuple = tts_orvos_copy_minimal_tuple
};
