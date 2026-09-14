/*-------------------------------------------------------------------------
 *
 * recno_slot.c
 *	  RECNO-specific TupleTableSlot implementation
 *
 * This implements custom TupleTableSlotOps for RECNO table access method.
 * RECNO tuples use timestamps for MVCC instead of transaction IDs, and
 * have a different on-disk format than heap tuples. This slot type handles
 * the RECNO tuple format natively, avoiding unnecessary conversions
 * through the heap tuple format.
 *
 * The slot can hold either:
 *   - A reference to a RECNO tuple in a buffer page (pinned buffer)
 *   - A materialized (palloc'd) copy of a RECNO tuple
 *   - Virtual data in tts_values/tts_isnull (after deforming or direct store)
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/recno/recno_slot.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/recno.h"
#include "access/recno_slog.h"
#include "access/htup_details.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "executor/tuptable.h"
#include "storage/bufmgr.h"
#include "utils/expandeddatum.h"
#include "utils/memutils.h"

/*
 * RecnoTupleTableSlot - slot type for RECNO tuples
 *
 * This extends the base TupleTableSlot with RECNO-specific fields to
 * hold a reference to a RECNO tuple either in a buffer or materialized
 * in memory.
 */
typedef struct RecnoTupleTableSlot
{
	TupleTableSlot base;

	/* Pointer to the RECNO tuple header (in buffer or materialized) */
	RecnoTupleHeader *tuple;

	/* Length of the tuple data pointed to by 'tuple' */
	uint32		tuple_len;

	/* Deform state: offset into tuple data for lazy attribute extraction */
	uint32		off;

	/*
	 * If buffer is not InvalidBuffer, the slot holds a pin on this buffer and
	 * 'tuple' points into the buffer page. When the slot is cleared or
	 * materialized, the pin is released.
	 */
	Buffer		buffer;
}			RecnoTupleTableSlot;

/* Forward declarations */
const TupleTableSlotOps TTSOpsRecnoTuple;
static void tts_recno_deform(TupleTableSlot *slot, int natts);


/*
 * Initialization - nothing special needed.
 */
static void
tts_recno_init(TupleTableSlot *slot)
{
	RecnoTupleTableSlot *rslot = (RecnoTupleTableSlot *) slot;

	rslot->tuple = NULL;
	rslot->tuple_len = 0;
	rslot->off = 0;
	rslot->buffer = InvalidBuffer;
}

/*
 * Destruction - release any resources.
 */
static void
tts_recno_release(TupleTableSlot *slot)
{
	RecnoTupleTableSlot *rslot = (RecnoTupleTableSlot *) slot;

	/* If we own a materialized tuple, free it */
	if (TTS_SHOULDFREE(slot) && rslot->tuple)
	{
		pfree(rslot->tuple);
		rslot->tuple = NULL;
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
tts_recno_clear(TupleTableSlot *slot)
{
	RecnoTupleTableSlot *rslot = (RecnoTupleTableSlot *) slot;

	/*
	 * Free materialized tuple data if we own it. A tuple residing in a buffer
	 * cannot be freed directly; only materialized copies can.
	 */
	if (TTS_SHOULDFREE(slot))
	{
		Assert(!BufferIsValid(rslot->buffer));

		if (rslot->tuple)
			pfree(rslot->tuple);

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
}

/*
 * Deform RECNO tuple to extract attributes into tts_values/tts_isnull.
 *
 * This is the RECNO-native equivalent of slot_deform_heap_tuple. It reads
 * the RECNO tuple format directly (bitmap + inline attribute data) rather
 * than going through the heap tuple deforming path.
 */
static void
tts_recno_deform(TupleTableSlot *slot, int natts)
{
	RecnoTupleTableSlot *rslot = (RecnoTupleTableSlot *) slot;
	TupleDesc	tupdesc = slot->tts_tupleDescriptor;
	RecnoTupleHeader *header = rslot->tuple;
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
		int		tuple_natts = header->t_natts;

		bitmap_len = BITMAPLEN(tuple_natts);
		nulls_bitmap = (uint8 *) header->t_attrs_bitmap;
		has_nulls = (header->t_infomask & RECNO_INFOMASK_HASNULL) != 0;
		tuple_has_compressed = (header->t_infomask & RECNO_INFOMASK_COMPRESSED) != 0;

		/*
		 * If this is the first time deforming (attnum == 0), start from the
		 * beginning of the data area. Otherwise, resume from saved offset.
		 */
		if (attnum == 0)
			data_ptr = (char *) header + RECNO_TUPLE_OVERHEAD + MAXALIGN(bitmap_len);
		else
			data_ptr = (char *) header + rslot->off;

		/*
		 * Limit deformation to the attributes physically present in the
		 * tuple.  Attributes beyond tuple_natts were added by ALTER TABLE
		 * ADD COLUMN and will be filled with their defaults below.
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
			 * Check for overflow pointer FIRST.  An attribute that was
			 * compressed and then overflowed has an overflow pointer on
			 * the page, not the compressed data.  We must fetch from
			 * overflow before attempting decompression.
			 */
			if ((header->t_flags & RECNO_TUPLE_HAS_OVERFLOW) &&
				RecnoIsOverflowPtr(data_ptr))
			{
				Datum		fetched = (Datum) 0;
				bool		fetched_from_overflow = false;

				/*
				 * Get the relation to fetch overflow data.  The slot must
				 * have a relation set if we're deforming overflow attributes.
				 */
				if (slot->tts_tableOid != InvalidOid)
				{
					Relation	rel;

					rel = relation_open(slot->tts_tableOid, NoLock);
					fetched = RecnoFetchOverflowColumn(rel, data_ptr);
					relation_close(rel, NoLock);
					fetched_from_overflow = true;
				}
				else
				{
					/*
					 * No relation OID - return overflow pointer as-is. This
					 * can happen for transient slots that don't have a table
					 * relation associated.
					 */
					slot->tts_values[attnum] = PointerGetDatum(data_ptr);
					data_ptr += attr_len;
					continue;
				}

				/*
				 * The fetched data may be a compressed varlena, since
				 * RecnoFormTuple compresses before overflowing.  Check
				 * the fetched value for a compression header and decompress.
				 */
				if (fetched_from_overflow && tuple_has_compressed)
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
							slot->tts_values[attnum] = RecnoDecompressAttribute(
																				fetched,
																				att->atttypid,
																				comp_hdr);
							data_ptr += attr_len;
							continue;
						}
					}
				}

				/* Not compressed - use fetched data as-is */
				slot->tts_values[attnum] = fetched;
				data_ptr += attr_len;
				continue;
			}

			/*
			 * Check for compressed attribute (inline, not overflowed).
			 * When the tuple has the COMPRESSED infomask bit, varlena
			 * attributes may contain a RecnoCompressionHeader prefix
			 * followed by compressed data.  Decompress transparently so
			 * callers see the original value.
			 */
			if (tuple_has_compressed)
			{
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
						slot->tts_values[attnum] = RecnoDecompressAttribute(
																			PointerGetDatum(data_ptr),
																			att->atttypid,
																			comp_hdr);
						data_ptr += attr_len;
						continue;
					}
				}
			}

			/* Not compressed, not overflow - return pointer to in-place data */
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
 * If the slot has a RECNO tuple, deform it natively. If values were already
 * stored directly (virtual-style), they are already present.
 */
static void
tts_recno_getsomeattrs(TupleTableSlot *slot, int natts)
{
	RecnoTupleTableSlot *rslot = (RecnoTupleTableSlot *) slot;

	Assert(!TTS_EMPTY(slot));

	if (rslot->tuple != NULL)
	{
		/* Deform from the RECNO tuple */
		tts_recno_deform(slot, natts);

		/*
		 * If the tuple had fewer attributes than requested (e.g., after
		 * ALTER TABLE ADD COLUMN), fill in defaults for the missing ones.
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
 * Return system attribute value for RECNO tuples.
 *
 * RECNO tuples have timestamps instead of XIDs, so most heap system columns
 * are not directly applicable. We handle the subset that makes sense.
 */
static Datum
tts_recno_getsysattr(TupleTableSlot *slot, int attnum, bool *isnull)
{
	RecnoTupleTableSlot *rslot = (RecnoTupleTableSlot *) slot;

	Assert(!TTS_EMPTY(slot));

	/* If no physical tuple, we cannot provide system attributes */
	if (!rslot->tuple)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot retrieve a system column in this context")));

	/*
	 * RECNO doesn't use traditional XIDs. For compatibility with code that
	 * requests xmin/xmax, return the current transaction ID. The real MVCC
	 * information is in commit_ts/xact_ts fields.
	 */
	*isnull = false;

	switch (attnum)
	{
		case MinTransactionIdAttributeNumber:	/* xmin */
			return TransactionIdGetDatum(GetCurrentTransactionId());
		case MaxTransactionIdAttributeNumber:	/* xmax */
			if (rslot->tuple->t_flags & RECNO_TUPLE_DELETED)
				return TransactionIdGetDatum(GetCurrentTransactionId());
			return TransactionIdGetDatum(InvalidTransactionId);
		case MinCommandIdAttributeNumber:	/* cmin */
		case MaxCommandIdAttributeNumber:	/* cmax */
		{
			/*
			 * t_cid removed from RecnoTupleHeader (saves 4 bytes).
			 * Look up the command ID from the sLog for in-progress operations;
			 * return InvalidCommandId if no sLog entry exists (committed tuple).
			 */
			RecnoSLogEntry slog_entry;
			int nfound = RecnoSLogLookup(slot->tts_tableOid,
										 &slot->tts_tid,
										 GetTopTransactionIdIfAny(),
										 &slog_entry, 1);
			return CommandIdGetDatum(nfound > 0 ? slog_entry.cid : InvalidCommandId);
		}
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("RECNO does not support system attribute %d",
							attnum)));
			return 0;			/* silence compiler */
	}
}

/*
 * Check if the tuple was created by the current transaction.
 *
 * For RECNO, we consult the sLog to determine whether the current
 * transaction inserted this tuple.  This replaces the old t_xact_ts
 * comparison that was removed in the sLog migration.
 */
static bool
tts_recno_is_current_xact_tuple(TupleTableSlot *slot)
{
	Assert(!TTS_EMPTY(slot));

	if (!ItemPointerIsValid(&slot->tts_tid) ||
		slot->tts_tableOid == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("don't have a storage tuple in this context")));

	/*
	 * Ask the sLog whether the current transaction inserted this tuple.
	 * This is the RECNO equivalent of checking xmin == current xid.
	 */
	return RecnoSLogIsInsertedByMe(slot->tts_tableOid, &slot->tts_tid);
}

/*
 * Materialize the slot contents.
 *
 * After materialization, the slot's data is independent of any external
 * storage (buffers, other memory contexts). If the slot references a
 * tuple in a buffer, the tuple data is copied and the buffer pin released.
 */
static void
tts_recno_materialize(TupleTableSlot *slot)
{
	RecnoTupleTableSlot *rslot = (RecnoTupleTableSlot *) slot;
	MemoryContext oldContext;

	Assert(!TTS_EMPTY(slot));

	/* Already materialized */
	if (TTS_SHOULDFREE(slot))
		return;

	oldContext = MemoryContextSwitchTo(slot->tts_mcxt);

	if (rslot->tuple != NULL)
	{
		/*
		 * We have a physical RECNO tuple (in a buffer or external memory).
		 * Copy it into the slot's own memory context.
		 */
		RecnoTupleHeader *newtuple;

		newtuple = (RecnoTupleHeader *) palloc(rslot->tuple_len);
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
		 * Virtual tuple (values stored directly). Materialize by copying all
		 * pass-by-reference Datums into the slot's memory context. We build a
		 * RECNO tuple from the current values.
		 */
		RecnoTuple	rtuple;
		RecnoTupleHeader *newtuple;

		rtuple = RecnoFormTuple(slot->tts_tupleDescriptor,
								slot->tts_values,
								slot->tts_isnull,
								NULL, NULL);
		newtuple = (RecnoTupleHeader *) palloc(rtuple->t_len);
		memcpy(newtuple, rtuple->t_data, rtuple->t_len);
		rslot->tuple = newtuple;
		rslot->tuple_len = rtuple->t_len;
		rslot->off = 0;
		slot->tts_nvalid = 0;

		RecnoFreeTuple(rtuple);
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
 * If both slots are RECNO slots and the source has an in-buffer tuple,
 * we can reference it directly (with a new buffer pin). Otherwise, we
 * materialize the source data into the destination.
 */
static void
tts_recno_copyslot(TupleTableSlot *dstslot, TupleTableSlot *srcslot)
{
	RecnoTupleTableSlot *rdst = (RecnoTupleTableSlot *) dstslot;
	MemoryContext oldContext;

	tts_recno_clear(dstslot);

	/*
	 * Always copy by extracting all attributes from the source slot and
	 * forming a new RECNO tuple. This handles cross-slot-type copies
	 * correctly.
	 */
	slot_getallattrs(srcslot);

	for (int i = 0; i < srcslot->tts_tupleDescriptor->natts; i++)
	{
		dstslot->tts_values[i] = srcslot->tts_values[i];
		dstslot->tts_isnull[i] = srcslot->tts_isnull[i];
	}

	dstslot->tts_nvalid = srcslot->tts_tupleDescriptor->natts;
	dstslot->tts_flags &= ~TTS_FLAG_EMPTY;
	dstslot->tts_tid = srcslot->tts_tid;

	/*
	 * Materialize to ensure the destination does not depend on the source
	 * slot's memory.
	 */
	oldContext = MemoryContextSwitchTo(dstslot->tts_mcxt);

	{
		RecnoTuple	rtuple;
		RecnoTupleHeader *newtuple;

		rtuple = RecnoFormTuple(dstslot->tts_tupleDescriptor,
								dstslot->tts_values,
								dstslot->tts_isnull,
								NULL, NULL);
		newtuple = (RecnoTupleHeader *) palloc(rtuple->t_len);
		memcpy(newtuple, rtuple->t_data, rtuple->t_len);
		rdst->tuple = newtuple;
		rdst->tuple_len = rtuple->t_len;
		rdst->off = 0;
		dstslot->tts_nvalid = 0;
		dstslot->tts_flags |= TTS_FLAG_SHOULDFREE;

		RecnoFreeTuple(rtuple);
	}

	MemoryContextSwitchTo(oldContext);
}

/*
 * Return a HeapTuple "owned" by the slot.
 *
 * Since RECNO tuples are not heap tuples, we must form one from the
 * deformed values. The result is a palloc'd HeapTuple that the slot owns.
 *
 * This is needed by parts of the executor that require heap tuples
 * (e.g., for index tuple formation, triggers, etc.).
 */
static HeapTuple
tts_recno_copy_heap_tuple(TupleTableSlot *slot)
{
	HeapTuple	htup;

	Assert(!TTS_EMPTY(slot));

	/* Ensure all attributes are deformed */
	slot_getallattrs(slot);

	htup = heap_form_tuple(slot->tts_tupleDescriptor,
						   slot->tts_values,
						   slot->tts_isnull);

	/*
	 * Propagate TID and table OID from the slot to the HeapTuple.
	 * ANALYZE's compare_rows() sorts sample tuples by t_self (TID),
	 * which heap_form_tuple leaves zeroed.  Without this, the
	 * ItemPointerIsValid assertion in ItemPointerGetBlockNumber fires.
	 */
	htup->t_self = slot->tts_tid;
	htup->t_tableOid = slot->tts_tableOid;

	return htup;
}

/*
 * Return a MinimalTuple copy allocated in the caller's memory context.
 */
static MinimalTuple
tts_recno_copy_minimal_tuple(TupleTableSlot *slot, Size extra)
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
 * The RECNO TupleTableSlotOps structure.
 *
 * RECNO slots do not "own" heap tuples or minimal tuples natively, so
 * get_heap_tuple and get_minimal_tuple are NULL. The copy_ variants are
 * provided to satisfy the executor's needs.
 */
const TupleTableSlotOps TTSOpsRecnoTuple = {
	.base_slot_size = sizeof(RecnoTupleTableSlot),
	.init = tts_recno_init,
	.release = tts_recno_release,
	.clear = tts_recno_clear,
	.getsomeattrs = tts_recno_getsomeattrs,
	.getsysattr = tts_recno_getsysattr,
	.is_current_xact_tuple = tts_recno_is_current_xact_tuple,
	.materialize = tts_recno_materialize,
	.copyslot = tts_recno_copyslot,

	/* RECNO slots do not natively own heap or minimal tuples */
	.get_heap_tuple = NULL,
	.get_minimal_tuple = NULL,
	.copy_heap_tuple = tts_recno_copy_heap_tuple,
	.copy_minimal_tuple = tts_recno_copy_minimal_tuple,
};


/*
 * Store a RECNO tuple from a buffer page into the slot.
 *
 * The tuple data remains in the buffer; a pin is acquired to keep the
 * buffer valid for the lifetime of the slot reference.
 *
 * This is the primary way scan routines populate RECNO slots.
 */
void
RecnoSlotStoreTuple(TupleTableSlot *slot, RecnoTupleHeader *tuple,
					uint32 tuple_len, Buffer buffer)
{
	RecnoTupleTableSlot *rslot = (RecnoTupleTableSlot *) slot;

	Assert(slot->tts_ops == &TTSOpsRecnoTuple);

	tts_recno_clear(slot);

	rslot->tuple = tuple;
	rslot->tuple_len = tuple_len;
	rslot->off = 0;
	rslot->buffer = buffer;

	/* Acquire a pin on the buffer */
	if (BufferIsValid(buffer))
		IncrBufferRefCount(buffer);

	slot->tts_flags &= ~TTS_FLAG_EMPTY;
	slot->tts_nvalid = 0;
}

/*
 * Store a materialized (palloc'd) RECNO tuple into the slot.
 *
 * The slot takes ownership of the tuple data and will pfree it when
 * cleared or released.
 */
void
RecnoSlotStoreMaterializedTuple(TupleTableSlot *slot,
								RecnoTupleHeader *tuple,
								uint32 tuple_len)
{
	RecnoTupleTableSlot *rslot = (RecnoTupleTableSlot *) slot;

	Assert(slot->tts_ops == &TTSOpsRecnoTuple);

	tts_recno_clear(slot);

	rslot->tuple = tuple;
	rslot->tuple_len = tuple_len;
	rslot->off = 0;
	rslot->buffer = InvalidBuffer;

	slot->tts_flags &= ~TTS_FLAG_EMPTY;
	slot->tts_flags |= TTS_FLAG_SHOULDFREE;
	slot->tts_nvalid = 0;
}
