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
#include "access/slog.h"
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
	rslot->values_block = NULL;
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
		int			tuple_natts = header->t_natts;

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
#ifdef USE_ASSERT_CHECKING
			/*
			 * Deform-bounds oracle (fix-6): a torn in-place-UPDATE copy (a
			 * plain-SHARE reader is NOT excluded by the CAS writer's
			 * SHARE_EXCLUSIVE lock) can leave a garbage varlena length here,
			 * walking data_ptr off the tuple -- and off the page -- so the
			 * next VARSIZE_ANY faults.  rslot->tuple is a slot-private copy of
			 * rslot->tuple_len bytes; every advance must stay inside it.  Trip
			 * here on the first bad advance instead of SIGSEGV'ing later.
			 */
			{
				char	   *lo = (char *) header;
				char	   *hi = (char *) header + rslot->tuple_len;

				Assert(data_ptr >= lo && data_ptr < hi);
				attr_len = VARSIZE_ANY(data_ptr);
				Assert(data_ptr + attr_len > data_ptr &&
					   data_ptr + attr_len <= hi);
			}
#else
			attr_len = VARSIZE_ANY(data_ptr);
#endif

			/*
			 * Check for overflow pointer FIRST.  An attribute that was
			 * compressed and then overflowed has an overflow pointer on the
			 * page, not the compressed data.  We must fetch from overflow
			 * before attempting decompression.
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
				 * RecnoFormTuple compresses before overflowing.  Check the
				 * fetched value for a compression header and decompress.
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
																				slot->tts_tableOid,
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
			 * Check for compressed attribute (inline, not overflowed). When
			 * the tuple has the COMPRESSED infomask bit, varlena attributes
			 * may contain a RecnoCompressionHeader prefix followed by
			 * compressed data.  Decompress transparently so callers see the
			 * original value.
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
																			slot->tts_tableOid,
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
				 * t_cid removed from RecnoTupleHeader (saves 4 bytes). Look
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
	 * Ask the sLog whether the current transaction inserted this tuple. This
	 * is the RECNO equivalent of checking xmin == current xid.
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
 * We deliberately do NOT form a physical RECNO tuple here.  Forming one would
 * compress every varlena, and the immediate next consumer (recno_multi_insert /
 * recno_tuple_insert via slot_getallattrs) would decompress it again before
 * re-forming the on-page tuple.  Deferring tuple formation to the insert path
 * makes bulk COPY compress each value exactly once instead of three times.
 *
 * The caller must have already populated tts_values/tts_isnull and set
 * tts_nvalid to the attribute count; those stay valid because getsomeattrs and
 * getsysattr handle tuple == NULL by working from tts_values.
 */
static void
tts_recno_materialize_values(TupleTableSlot *slot)
{
	RecnoTupleTableSlot *rslot = (RecnoTupleTableSlot *) slot;
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
		 * Virtual tuple (values stored directly).  Materialize by copying all
		 * pass-by-reference Datums into a single block in the slot's memory
		 * context and leaving the slot in a values-only state (tuple == NULL).
		 * We deliberately do NOT form a physical RECNO tuple: that would
		 * compress every varlena, and the insert path (recno_multi_insert via
		 * slot_getallattrs) would decompress and recompress it.  Deferring
		 * tuple formation to insert compresses each value exactly once.
		 */
		tts_recno_materialize_values(slot);

		/*
		 * tts_recno_materialize_values sets SHOULDFREE itself when it owns a
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
 * do NOT form a physical RECNO tuple here: forming one would compress every
 * varlena, and the immediate next consumer (recno_multi_insert /
 * recno_tuple_insert via slot_getallattrs) would have to decompress it again
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
tts_recno_copyslot(TupleTableSlot *dstslot, TupleTableSlot *srcslot)
{
	TupleDesc	desc = dstslot->tts_tupleDescriptor;

	tts_recno_clear(dstslot);

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
	tts_recno_materialize_values(dstslot);
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
 * RECNO performs in-place MVCC UPDATEs: a committed updater overwrites the
 * on-page tuple bytes of a live tuple that concurrent readers may be scanning.
 * The scan/index-fetch paths formerly parked a pointer into the buffer in the
 * slot (pinned but not content-locked, mirroring heap's page-mode pattern) --
 * but that pattern is only safe for heap because heap tuples are immutable once
 * written.  For RECNO it is a torn-read hazard: an in-place UPDATE (same-size
 * CAS overwrite or the delete+re-add growth path) can mutate the exact bytes a
 * reader is deforming/decompressing, producing corrupt varlena lengths, garbage
 * compressed frames (zstd "unknown frame descriptor"), bad overflow-pointer
 * lengths ("invalid memory alloc request size"), and heap_form_tuple
 * size-mismatch asserts / SIGSEGV.
 *
 * We therefore copy the tuple into slot-private memory here, so the slot never
 * aliases a live buffer byte and subsequent deform/detoast cannot observe a
 * concurrent overwrite.  The COPY MUST be taken while the buffer's content is
 * stable: the caller must hold at least BUFFER_LOCK_SHARE on `buffer`.  The
 * same-size CAS in-place updater takes BUFFER_LOCK_EXCLUSIVE across its
 * non-atomic overwrite (see recno_tuple_update), which conflicts with SHARE,
 * so this memcpy never races a writer and cannot capture a torn image.  A
 * caller with the tuple already in private memory (transient / test paths)
 * passes InvalidBuffer and no copy is made.
 *
 * This costs one tuple-sized memcpy per scanned row versus heap's zero-copy
 * page-mode; it is mandatory for correctness under in-place MVCC and is cheap
 * relative to the deform/decompress/overflow work RECNO already does.
 *
 * This is the primary way scan routines populate RECNO slots.
 */
void
RecnoSlotStoreTuple(TupleTableSlot *slot, RecnoTupleHeader *tuple,
					uint32 tuple_len, Buffer buffer)
{
	RecnoTupleTableSlot *rslot = (RecnoTupleTableSlot *) slot;
	MemoryContext oldcxt;

	Assert(slot->tts_ops == &TTSOpsRecnoTuple);

	tts_recno_clear(slot);

	if (BufferIsValid(buffer))
	{
		/*
		 * On-page tuple: caller holds the buffer content lock (>= SHARE),
		 * which excludes the EXCLUSIVE-holding in-place updater.  Copy the
		 * bytes into slot-owned memory so the slot is independent of the
		 * (mutable, in-place-updated) buffer after this returns.
		 */
		RecnoTupleHeader *tuple_copy;

		oldcxt = MemoryContextSwitchTo(slot->tts_mcxt);
		tuple_copy = (RecnoTupleHeader *) palloc(tuple_len);
		MemoryContextSwitchTo(oldcxt);

		memcpy(tuple_copy, tuple, tuple_len);
		rslot->tuple = tuple_copy;
		slot->tts_flags |= TTS_FLAG_SHOULDFREE;
	}
	else
	{
		/* Transient path: tuple pointer is already in private memory. */
		rslot->tuple = tuple;
	}

	rslot->tuple_len = tuple_len;
	rslot->off = 0;
	rslot->buffer = InvalidBuffer;

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
