/*-------------------------------------------------------------------------
 *
 * flux_slot.c
 *	  FLUX-specific TupleTableSlot implementation
 *
 * This implements custom TupleTableSlotOps for FLUX table access method.
 * FLUX tuples use timestamps for MVCC instead of transaction IDs, and
 * have a different on-disk format than heap tuples. This slot type handles
 * the FLUX tuple format natively, avoiding unnecessary conversions
 * through the heap tuple format.
 *
 * The slot can hold either:
 *   - A reference to a FLUX tuple in a buffer page (pinned buffer)
 *   - A materialized (palloc'd) copy of a FLUX tuple
 *   - Virtual data in tts_values/tts_isnull (after deforming or direct store)
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/flux/flux_slot.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/flux.h"
#include "access/slog.h"
#include "access/htup_details.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "executor/tuptable.h"
#include "storage/bufmgr.h"
#include "utils/expandeddatum.h"
#include "utils/memutils.h"

/*
 * FluxTupleTableSlot - slot type for FLUX tuples
 *
 * This extends the base TupleTableSlot with FLUX-specific fields to
 * hold a reference to a FLUX tuple either in a buffer or materialized
 * in memory.
 */
typedef struct FluxTupleTableSlot
{
	TupleTableSlot base;

	/* Pointer to the FLUX tuple header (in buffer or materialized) */
	FluxTupleHeader *tuple;

	/* Length of the tuple data pointed to by 'tuple' */
	uint32		tuple_len;

	/*
	 * Values-only ("virtual") payload block.  When copyslot deep-copies a
	 * source slot's datums instead of forming a physical tuple, the
	 * pass-by-reference values are packed into this single palloc'd block so
	 * that clear() can free them with one pfree (mirrors VirtualTupleTableSlot's
	 * ->data).  NULL when the slot holds a physical tuple or has no pass-by-ref
	 * values.
	 */
	void	   *values_block;

	/* Deform state: offset into tuple data for lazy attribute extraction */
	uint32		off;

	/*
	 * If buffer is not InvalidBuffer, the slot holds a pin on this buffer and
	 * 'tuple' points into the buffer page. When the slot is cleared or
	 * materialized, the pin is released.
	 */
	Buffer		buffer;
}			FluxTupleTableSlot;

/* Forward declarations */
const TupleTableSlotOps TTSOpsFluxTuple;
static void tts_flux_deform(TupleTableSlot *slot, int natts);


/*
 * Initialization - nothing special needed.
 */
static void
tts_flux_init(TupleTableSlot *slot)
{
	FluxTupleTableSlot *rslot = (FluxTupleTableSlot *) slot;

	rslot->tuple = NULL;
	rslot->tuple_len = 0;
	rslot->off = 0;
	rslot->buffer = InvalidBuffer;
	rslot->values_block = NULL;
}

/*
 * Destruction - release any resources.
 */
static void
tts_flux_release(TupleTableSlot *slot)
{
	FluxTupleTableSlot *rslot = (FluxTupleTableSlot *) slot;

	/* If we own a materialized tuple, free it */
	if (TTS_SHOULDFREE(slot) && rslot->tuple)
	{
		pfree(rslot->tuple);
		rslot->tuple = NULL;
	}
	if (TTS_SHOULDFREE(slot) && rslot->values_block)
	{
		pfree(rslot->values_block);
		rslot->values_block = NULL;
	}

	/* Release buffer pin if held */
	if (BufferIsValid(rslot->buffer))
	{
		ReleaseBuffer(rslot->buffer);
		rslot->buffer = InvalidBuffer;
	}
}

/*
 * Clear the slot contents.
 *
 * Free materialized tuple if owned, release buffer pin, and reset
 * the slot to empty state.
 */
static void
tts_flux_clear(TupleTableSlot *slot)
{
	FluxTupleTableSlot *rslot = (FluxTupleTableSlot *) slot;

	/*
	 * Free materialized tuple data if we own it. A tuple residing in a buffer
	 * cannot be freed directly; only materialized copies can.
	 */
	if (TTS_SHOULDFREE(slot))
	{
		Assert(!BufferIsValid(rslot->buffer));

		if (rslot->tuple)
			pfree(rslot->tuple);
		if (rslot->values_block)
			pfree(rslot->values_block);

		slot->tts_flags &= ~TTS_FLAG_SHOULDFREE;
	}

	/* Release buffer pin if held */
	if (BufferIsValid(rslot->buffer))
	{
		ReleaseBuffer(rslot->buffer);
		rslot->buffer = InvalidBuffer;
	}

	slot->tts_nvalid = 0;
	slot->tts_flags |= TTS_FLAG_EMPTY;
	ItemPointerSetInvalid(&slot->tts_tid);
	rslot->tuple = NULL;
	rslot->tuple_len = 0;
	rslot->off = 0;
	rslot->values_block = NULL;
}

/*
 * Deform FLUX tuple to extract attributes into tts_values/tts_isnull.
 *
 * This is the FLUX-native equivalent of slot_deform_heap_tuple. It reads
 * the FLUX tuple format directly (bitmap + inline attribute data) rather
 * than going through the heap tuple deforming path.
 */
static void
tts_flux_deform(TupleTableSlot *slot, int natts)
{
	FluxTupleTableSlot *rslot = (FluxTupleTableSlot *) slot;
	TupleDesc	tupdesc = slot->tts_tupleDescriptor;
	FluxTupleHeader *header = rslot->tuple;
	int			attnum;
	char	   *data_ptr;
	uint8	   *nulls_bitmap;
	Size		bitmap_len;
	bool		has_nulls;
	bool		tuple_has_compressed;

	Assert(header != NULL);
	Assert(natts <= tupdesc->natts);

	/* Start from where we left off last time */
	attnum = slot->tts_nvalid;
	if (attnum >= natts)
		return;

	/*
	 * Use the tuple's actual natts for bitmap_len and data_ptr calculation,
	 * not the tupdesc's natts.  After ALTER TABLE ADD COLUMN, old tuples may
	 * have fewer attributes than the current schema expects.
	 */
	{
		int			tuple_natts = header->t_natts;

		bitmap_len = BITMAPLEN(tuple_natts);
		nulls_bitmap = (uint8 *) header->t_attrs_bitmap;
		has_nulls = (header->t_infomask & FLUX_INFOMASK_HASNULL) != 0;
		tuple_has_compressed = false;	/* FLUX does not compress attributes */
		(void) tuple_has_compressed;

		/*
		 * If this is the first time deforming (attnum == 0), start from the
		 * beginning of the data area. Otherwise, resume from saved offset.
		 */
		if (attnum == 0)
			data_ptr = (char *) header + FLUX_TUPLE_OVERHEAD + MAXALIGN(bitmap_len);
		else
			data_ptr = (char *) header + rslot->off;

		/*
		 * Limit deformation to the attributes physically present in the
		 * tuple.  Attributes beyond tuple_natts were added by ALTER TABLE ADD
		 * COLUMN and will be filled with their defaults below.
		 */
		natts = Min(natts, tuple_natts);
	}

	for (; attnum < natts; attnum++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, attnum);

		if (att->attisdropped)
		{
			slot->tts_values[attnum] = (Datum) 0;
			slot->tts_isnull[attnum] = true;
			continue;
		}

		/* Check null bitmap */
		if (has_nulls && att_isnull(attnum, nulls_bitmap))
		{
			slot->tts_values[attnum] = (Datum) 0;
			slot->tts_isnull[attnum] = true;
			continue;
		}

		slot->tts_isnull[attnum] = false;

		if (att->attlen > 0)
		{
			/* Fixed-length attribute - align first */
			data_ptr = (char *) att_align_nominal(data_ptr, att->attalign);
			slot->tts_values[attnum] = fetchatt(att, data_ptr);
			data_ptr += att->attlen;
		}
		else if (att->attlen == -1)
		{
			Size		attr_len;

			/* Variable-length attribute - align first */
			data_ptr = (char *) att_align_nominal(data_ptr, att->attalign);
			attr_len = VARSIZE_ANY(data_ptr);

			/*
			 * FLUX stores varlena values verbatim (wide values are TOASTed
			 * through the standard heap TOAST path; FLUX has no on-page
			 * overflow and does not compress attributes).
			 */
			slot->tts_values[attnum] = PointerGetDatum(data_ptr);
			data_ptr += attr_len;
		}
		else if (att->attlen == -2)
		{
			/* C string */
			data_ptr = (char *) att_align_nominal(data_ptr, att->attalign);
			slot->tts_values[attnum] = CStringGetDatum(data_ptr);
			data_ptr += strlen(data_ptr) + 1;
		}
		else
		{
			elog(ERROR, "unsupported attribute length: %d", att->attlen);
		}
	}

	/* Save deform state for incremental deforming */
	rslot->off = (uint32) (data_ptr - (char *) header);
	slot->tts_nvalid = natts;
}

/*
 * Fill up first natts entries of tts_values and tts_isnull.
 *
 * If the slot has a FLUX tuple, deform it natively. If values were already
 * stored directly (virtual-style), they are already present.
 */
static void
tts_flux_getsomeattrs(TupleTableSlot *slot, int natts)
{
	FluxTupleTableSlot *rslot = (FluxTupleTableSlot *) slot;

	Assert(!TTS_EMPTY(slot));

	if (rslot->tuple != NULL)
	{
		/* Deform from the FLUX tuple */
		tts_flux_deform(slot, natts);

		/*
		 * If the tuple had fewer attributes than requested (e.g., after ALTER
		 * TABLE ADD COLUMN), fill in defaults for the missing ones.
		 */
		if (slot->tts_nvalid < natts)
		{
			slot_getmissingattrs(slot, slot->tts_nvalid, natts);
			slot->tts_nvalid = natts;
		}
	}
	else
	{
		/*
		 * No physical tuple - values were stored directly into tts_values
		 * (virtual-style). Fill missing attributes.
		 */
		slot_getmissingattrs(slot, slot->tts_nvalid, natts);
		slot->tts_nvalid = natts;
	}
}

/*
 * Return system attribute value for FLUX tuples.
 *
 * FLUX tuples have timestamps instead of XIDs, so most heap system columns
 * are not directly applicable. We handle the subset that makes sense.
 */
static Datum
tts_flux_getsysattr(TupleTableSlot *slot, int attnum, bool *isnull)
{
	FluxTupleTableSlot *rslot = (FluxTupleTableSlot *) slot;

	Assert(!TTS_EMPTY(slot));

	/* If no physical tuple, we cannot provide system attributes */
	if (!rslot->tuple)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot retrieve a system column in this context")));

	/*
	 * Return the tuple's real MVCC system columns.  FLUX uses heap-shaped
	 * xmin/xmax semantics: t_xmin is the inserter XID (always valid), and the
	 * xmax (deleter/updater XID) lives in the low 32 bits of t_commit_ts,
	 * accessed via FluxTupleGetXmax (0 == not deleted/updated).
	 */
	*isnull = false;

	switch (attnum)
	{
		case MinTransactionIdAttributeNumber:	/* xmin */
			return TransactionIdGetDatum(rslot->tuple->t_xmin);
		case MaxTransactionIdAttributeNumber:	/* xmax */
			return TransactionIdGetDatum(FluxTupleGetXmax(rslot->tuple));
		case MinCommandIdAttributeNumber:	/* cmin */
		case MaxCommandIdAttributeNumber:	/* cmax */
			{
				/*
				 * t_cid removed from FluxTupleHeader (saves 4 bytes). Look
				 * up the command ID from the sLog for in-progress operations;
				 * return InvalidCommandId if no sLog entry exists (committed
				 * tuple).
				 */
				SLogTupleOp slog_entry;
				int			nfound = SLogTupleLookupFiltered(slot->tts_tableOid,
															 &slot->tts_tid,
															 GetTopTransactionIdIfAny(),
															 &slog_entry, 1);

				return CommandIdGetDatum(nfound > 0 ? slog_entry.cid : InvalidCommandId);
			}
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("FLUX does not support system attribute %d",
							attnum)));
			return 0;			/* silence compiler */
	}
}

/*
 * Check if the tuple was created by the current transaction.
 *
 * For FLUX, we consult the sLog to determine whether the current
 * transaction inserted this tuple.  This replaces the old t_xact_ts
 * comparison that was removed in the sLog migration.
 */
static bool
tts_flux_is_current_xact_tuple(TupleTableSlot *slot)
{
	Assert(!TTS_EMPTY(slot));

	if (!ItemPointerIsValid(&slot->tts_tid) ||
		slot->tts_tableOid == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("don't have a storage tuple in this context")));

	/*
	 * Ask the sLog whether the current transaction inserted this tuple. This
	 * is the FLUX equivalent of checking xmin == current xid.
	 */
	return SLogTupleIsInsertedByMe(slot->tts_tableOid, &slot->tts_tid);
}

/*
 * Pack a slot's pass-by-reference Datums into a single palloc'd block owned by
 * the slot, leaving the slot in a values-only ("virtual") state (tuple == NULL).
 *
 * This mirrors tts_virtual_materialize: a two-pass scan that first sums the
 * aligned size of every non-null pass-by-reference attribute, makes one
 * allocation in the slot's memory context, then copies each Datum in and
 * repoints tts_values at the copy.  byval and null Datums are left untouched.
 *
 * We deliberately do NOT form a physical FLUX tuple here.  Forming one would
 * compress every varlena, and the immediate next consumer (flux_multi_insert /
 * flux_tuple_insert via slot_getallattrs) would decompress it again before
 * re-forming the on-page tuple.  Deferring tuple formation to the insert path
 * makes bulk COPY compress each value exactly once instead of three times.
 *
 * The caller must have already populated tts_values/tts_isnull and set
 * tts_nvalid to the attribute count; those stay valid because getsomeattrs and
 * getsysattr handle tuple == NULL by working from tts_values.
 */
static void
tts_flux_materialize_values(TupleTableSlot *slot)
{
	FluxTupleTableSlot *rslot = (FluxTupleTableSlot *) slot;
	TupleDesc	desc = slot->tts_tupleDescriptor;
	Size		sz = 0;
	char	   *data;

	/* First pass: compute the aligned size of the owned block. */
	for (int natt = 0; natt < desc->natts; natt++)
	{
		CompactAttribute *att = TupleDescCompactAttr(desc, natt);
		Datum		val;

		if (att->attbyval || slot->tts_isnull[natt])
			continue;

		val = slot->tts_values[natt];

		if (att->attlen == -1 &&
			VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(val)))
		{
			sz = att_nominal_alignby(sz, att->attalignby);
			sz += EOH_get_flat_size(DatumGetEOHP(val));
		}
		else
		{
			sz = att_nominal_alignby(sz, att->attalignby);
			sz = att_addlength_datum(sz, att->attlen, val);
		}
	}

	rslot->tuple = NULL;
	rslot->tuple_len = 0;
	rslot->off = 0;

	/* all data is byval / null: nothing to own */
	if (sz == 0)
		return;

	rslot->values_block = data = MemoryContextAlloc(slot->tts_mcxt, sz);
	slot->tts_flags |= TTS_FLAG_SHOULDFREE;

	/* Second pass: copy each pass-by-reference Datum and repoint tts_values. */
	for (int natt = 0; natt < desc->natts; natt++)
	{
		CompactAttribute *att = TupleDescCompactAttr(desc, natt);
		Datum		val;

		if (att->attbyval || slot->tts_isnull[natt])
			continue;

		val = slot->tts_values[natt];

		if (att->attlen == -1 &&
			VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(val)))
		{
			ExpandedObjectHeader *eoh = DatumGetEOHP(val);
			Size		data_length = EOH_get_flat_size(eoh);

			data = (char *) att_nominal_alignby(data, att->attalignby);
			EOH_flatten_into(eoh, data, data_length);

			slot->tts_values[natt] = PointerGetDatum(data);
			data += data_length;
		}
		else
		{
			Size		data_length = 0;

			data = (char *) att_nominal_alignby(data, att->attalignby);
			data_length = att_addlength_datum(data_length, att->attlen, val);

			memcpy(data, DatumGetPointer(val), data_length);

			slot->tts_values[natt] = PointerGetDatum(data);
			data += data_length;
		}
	}
}

/*
 * Materialize the slot contents.
 *
 * After materialization, the slot's data is independent of any external
 * storage (buffers, other memory contexts). If the slot references a
 * tuple in a buffer, the tuple data is copied and the buffer pin released.
 */
static void
tts_flux_materialize(TupleTableSlot *slot)
{
	FluxTupleTableSlot *rslot = (FluxTupleTableSlot *) slot;
	MemoryContext oldContext;

	Assert(!TTS_EMPTY(slot));

	/* Already materialized */
	if (TTS_SHOULDFREE(slot))
		return;

	oldContext = MemoryContextSwitchTo(slot->tts_mcxt);

	if (rslot->tuple != NULL)
	{
		/*
		 * We have a physical FLUX tuple (in a buffer or external memory).
		 * Copy it into the slot's own memory context.
		 */
		FluxTupleHeader *newtuple;

		newtuple = (FluxTupleHeader *) palloc(rslot->tuple_len);
		memcpy(newtuple, rslot->tuple, rslot->tuple_len);
		rslot->tuple = newtuple;

		/*
		 * Reset deform state since tts_values entries may point into the old
		 * (buffer) tuple data that we're about to release.
		 */
		rslot->off = 0;
		slot->tts_nvalid = 0;
	}
	else
	{
		/*
		 * Virtual tuple (values stored directly).  Materialize by copying all
		 * pass-by-reference Datums into a single block in the slot's memory
		 * context and leaving the slot in a values-only state (tuple == NULL).
		 * We deliberately do NOT form a physical FLUX tuple: that would
		 * compress every varlena, and the insert path (flux_multi_insert via
		 * slot_getallattrs) would decompress and recompress it.  Deferring
		 * tuple formation to insert compresses each value exactly once.
		 */
		tts_flux_materialize_values(slot);

		/*
		 * tts_flux_materialize_values sets SHOULDFREE itself when it owns a
		 * block; return early so we don't set SHOULDFREE unconditionally for a
		 * slot that owns nothing (all-byval/null).  We still must release any
		 * buffer pin first.
		 */
		if (BufferIsValid(rslot->buffer))
		{
			ReleaseBuffer(rslot->buffer);
			rslot->buffer = InvalidBuffer;
		}
		MemoryContextSwitchTo(oldContext);
		return;
	}

	/*
	 * Release buffer pin if held. Do this after copying but before setting
	 * TTS_FLAG_SHOULDFREE to avoid a transient state where the slot owns a
	 * buffer and has SHOULDFREE set.
	 */
	if (BufferIsValid(rslot->buffer))
	{
		ReleaseBuffer(rslot->buffer);
		rslot->buffer = InvalidBuffer;
	}

	slot->tts_flags |= TTS_FLAG_SHOULDFREE;

	MemoryContextSwitchTo(oldContext);
}

/*
 * Copy the contents of srcslot into dstslot.
 *
 * The destination must not depend on the source slot's memory after this
 * returns.  We satisfy that by deep-copying the source's attribute values
 * into the destination's own memory context and leaving the destination in
 * a values-only ("virtual") state -- rdst->tuple stays NULL.  We deliberately
 * do NOT form a physical FLUX tuple here: forming one would compress every
 * varlena, and the immediate next consumer (flux_multi_insert /
 * flux_tuple_insert via slot_getallattrs) would have to decompress it again
 * before re-forming the on-page tuple.  Deferring tuple formation to the
 * insert path makes bulk COPY compress each value exactly once instead of
 * three times (compress here, decompress there, recompress there).
 *
 * The values-only state is fully supported by the other slot ops:
 * getsomeattrs and materialize both handle rslot->tuple == NULL by working
 * from tts_values, and copy_heap_tuple/copy_minimal_tuple go through
 * slot_getallattrs.
 */
static void
tts_flux_copyslot(TupleTableSlot *dstslot, TupleTableSlot *srcslot)
{
	TupleDesc	desc = dstslot->tts_tupleDescriptor;

	tts_flux_clear(dstslot);

	slot_getallattrs(srcslot);

	/* Copy the datum pointers first; they still reference source memory. */
	for (int natt = 0; natt < desc->natts; natt++)
	{
		dstslot->tts_values[natt] = srcslot->tts_values[natt];
		dstslot->tts_isnull[natt] = srcslot->tts_isnull[natt];
	}
	dstslot->tts_nvalid = desc->natts;
	dstslot->tts_flags &= ~TTS_FLAG_EMPTY;
	dstslot->tts_tid = srcslot->tts_tid;

	/*
	 * Deep-copy the pass-by-reference datums into a single owned block so the
	 * destination no longer depends on the source's memory (in COPY, the
	 * source is a per-row scratch slot that gets reset).  This leaves the
	 * destination in a values-only state (tuple == NULL), deferring compression
	 * to the insert path.
	 */
	tts_flux_materialize_values(dstslot);
}

/*
 * Return a HeapTuple "owned" by the slot.
 *
 * Since FLUX tuples are not heap tuples, we must form one from the
 * deformed values. The result is a palloc'd HeapTuple that the slot owns.
 *
 * This is needed by parts of the executor that require heap tuples
 * (e.g., for index tuple formation, triggers, etc.).
 */
static HeapTuple
tts_flux_copy_heap_tuple(TupleTableSlot *slot)
{
	HeapTuple	htup;

	Assert(!TTS_EMPTY(slot));

	/* Ensure all attributes are deformed */
	slot_getallattrs(slot);

	htup = heap_form_tuple(slot->tts_tupleDescriptor,
						   slot->tts_values,
						   slot->tts_isnull);

	/*
	 * Propagate TID and table OID from the slot to the HeapTuple. ANALYZE's
	 * compare_rows() sorts sample tuples by t_self (TID), which
	 * heap_form_tuple leaves zeroed.  Without this, the ItemPointerIsValid
	 * assertion in ItemPointerGetBlockNumber fires.
	 */
	htup->t_self = slot->tts_tid;
	htup->t_tableOid = slot->tts_tableOid;

	return htup;
}

/*
 * Return a MinimalTuple copy allocated in the caller's memory context.
 */
static MinimalTuple
tts_flux_copy_minimal_tuple(TupleTableSlot *slot, Size extra)
{
	Assert(!TTS_EMPTY(slot));

	/* Ensure all attributes are deformed */
	slot_getallattrs(slot);

	return heap_form_minimal_tuple(slot->tts_tupleDescriptor,
								   slot->tts_values,
								   slot->tts_isnull,
								   extra);
}

/*
 * The FLUX TupleTableSlotOps structure.
 *
 * FLUX slots do not "own" heap tuples or minimal tuples natively, so
 * get_heap_tuple and get_minimal_tuple are NULL. The copy_ variants are
 * provided to satisfy the executor's needs.
 */
const TupleTableSlotOps TTSOpsFluxTuple = {
	.base_slot_size = sizeof(FluxTupleTableSlot),
	.init = tts_flux_init,
	.release = tts_flux_release,
	.clear = tts_flux_clear,
	.getsomeattrs = tts_flux_getsomeattrs,
	.getsysattr = tts_flux_getsysattr,
	.is_current_xact_tuple = tts_flux_is_current_xact_tuple,
	.materialize = tts_flux_materialize,
	.copyslot = tts_flux_copyslot,

	/* FLUX slots do not natively own heap or minimal tuples */
	.get_heap_tuple = NULL,
	.get_minimal_tuple = NULL,
	.copy_heap_tuple = tts_flux_copy_heap_tuple,
	.copy_minimal_tuple = tts_flux_copy_minimal_tuple,
};


/*
 * Store a FLUX tuple from a buffer page into the slot.
 *
 * The tuple data remains in the buffer; a pin is acquired to keep the
 * buffer valid for the lifetime of the slot reference.
 *
 * This is the primary way scan routines populate FLUX slots.
 */
void
FluxSlotStoreTuple(TupleTableSlot *slot, FluxTupleHeader *tuple,
					uint32 tuple_len, Buffer buffer)
{
	FluxTupleTableSlot *rslot = (FluxTupleTableSlot *) slot;

	Assert(slot->tts_ops == &TTSOpsFluxTuple);

	/*
	 * Optimize for the common case during sequential scans: if the new tuple
	 * is on the same buffer as the previous one, skip the expensive
	 * ReleaseBuffer + IncrBufferRefCount cycle.  This mirrors the
	 * optimization in heap's tts_buffer_heap_store_tuple().
	 */
	if (rslot->buffer == buffer)
	{
		/* Same buffer — just free any materialized data */
		if (unlikely(TTS_SHOULDFREE(slot)))
		{
			if (rslot->tuple)
				pfree(rslot->tuple);
			slot->tts_flags &= ~TTS_FLAG_SHOULDFREE;
		}
	}
	else
	{
		/* Different buffer — full clear (releases old pin) and acquire new */
		tts_flux_clear(slot);
		rslot->buffer = buffer;

		if (BufferIsValid(buffer))
			IncrBufferRefCount(buffer);
	}

	rslot->tuple = tuple;
	rslot->tuple_len = tuple_len;
	rslot->off = 0;

	slot->tts_flags &= ~TTS_FLAG_EMPTY;
	slot->tts_nvalid = 0;
}

/*
 * Store a materialized (palloc'd) FLUX tuple into the slot.
 *
 * The slot takes ownership of the tuple data and will pfree it when
 * cleared or released.
 */
void
FluxSlotStoreMaterializedTuple(TupleTableSlot *slot,
								FluxTupleHeader *tuple,
								uint32 tuple_len)
{
	FluxTupleTableSlot *rslot = (FluxTupleTableSlot *) slot;

	Assert(slot->tts_ops == &TTSOpsFluxTuple);

	tts_flux_clear(slot);

	rslot->tuple = tuple;
	rslot->tuple_len = tuple_len;
	rslot->off = 0;
	rslot->buffer = InvalidBuffer;

	slot->tts_flags &= ~TTS_FLAG_EMPTY;
	slot->tts_flags |= TTS_FLAG_SHOULDFREE;
	slot->tts_nvalid = 0;
}
