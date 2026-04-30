/*-------------------------------------------------------------------------
 *
 * heapam_undo.c
 *	  Heap AM UNDO resource manager
 *
 * This module implements the UNDO apply callbacks for the heap access
 * method.  It handles physical reversal of INSERT, DELETE, UPDATE, and
 * INPLACE operations by directly manipulating page contents via memcpy.
 *
 * The approach follows ZHeap's physical undo model:
 *   - Read the target page into a shared buffer
 *   - Acquire an exclusive lock
 *   - Apply the reversal (mark dead, restore tuple, etc.)
 *   - Generate a CLR (Compensation Log Record) for crash safety
 *   - Release the buffer
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/heapam_undo.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/detoast.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/toast_internals.h"
#include "access/undo_xlog.h"
#include "access/undolog.h"
#include "access/undorecord.h"
#include "access/undormgr.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/itemid.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "varatt.h"

/*
 * Heap UNDO subtypes (stored in urec_info)
 */
#define HEAP_UNDO_INSERT		0x0001
#define HEAP_UNDO_DELETE		0x0002
#define HEAP_UNDO_UPDATE		0x0003
#define HEAP_UNDO_PRUNE			0x0004
#define HEAP_UNDO_INPLACE		0x0005
#define HEAP_UNDO_HOT_UPDATE	0x0006

/*
 * Heap UNDO payload flags
 */
#define HEAP_UNDO_HAS_INDEX		0x0001	/* Relation has indexes */
#define HEAP_UNDO_HAS_TUPLE		0x0002	/* Payload contains tuple data */

/*
 * HeapUndoPayload - RM-specific payload for heap UNDO records
 *
 * This structure is serialized into the UNDO record's opaque payload.
 * It contains the target page location and optionally the full tuple
 * data needed for physical restoration.
 */
typedef struct HeapUndoPayload
{
	BlockNumber blkno;			/* Target block */
	OffsetNumber offset;		/* Item offset within page */
	uint16		flags;			/* HEAP_UNDO_HAS_INDEX etc. */
	uint32		tuple_len;		/* Length of tuple data (0 for INSERT) */
	/* Followed by tuple_len bytes of HeapTupleHeaderData + user data */
}			HeapUndoPayload;

#define SizeOfHeapUndoPayload	offsetof(HeapUndoPayload, tuple_len) + sizeof(uint32)

/* Forward declarations */
static UndoApplyResult heap_undo_apply(uint8 rmid, uint16 info,
									   TransactionId xid, Oid reloid,
									   const char *payload, Size payload_len,
									   UndoRecPtr urec_ptr);
static void heap_undo_desc(StringInfo buf, uint8 rmid, uint16 info,
						   const char *payload, Size payload_len);

/* The heap UNDO RM registration entry */
static const UndoRmgrData heap_undo_rmgr = {
	.rm_name = "heap",
	.rm_undo = heap_undo_apply,
	.rm_desc = heap_undo_desc,
};

/*
 * HeapUndoRmgrInit - Register the heap UNDO resource manager
 *
 * Called during postmaster startup to register the heap RM's callbacks.
 */
void
HeapUndoRmgrInit(void)
{
	RegisterUndoRmgr(UNDO_RMID_HEAP, &heap_undo_rmgr);
}

/*
 * HeapUndoBuildPayload - Build a heap UNDO payload for a given operation
 *
 * This is the helper used by heapam.c to construct the payload that gets
 * stored in the UNDO record.  Returns the total payload size.
 */
Size
HeapUndoBuildPayload(char *dest, Size dest_size,
					 BlockNumber blkno, OffsetNumber offset,
					 bool relhasindex, const char *tuple_data,
					 uint32 tuple_len)
{
	HeapUndoPayload hdr;
	Size		total_size;

	total_size = SizeOfHeapUndoPayload + tuple_len;
	Assert(dest_size >= total_size);

	hdr.blkno = blkno;
	hdr.offset = offset;
	hdr.flags = 0;
	if (relhasindex)
		hdr.flags |= HEAP_UNDO_HAS_INDEX;
	if (tuple_len > 0)
		hdr.flags |= HEAP_UNDO_HAS_TUPLE;
	hdr.tuple_len = tuple_len;

	memcpy(dest, &hdr, SizeOfHeapUndoPayload);
	if (tuple_len > 0 && tuple_data != NULL)
		memcpy(dest + SizeOfHeapUndoPayload, tuple_data, tuple_len);

	return total_size;
}

/*
 * HeapUndoPayloadSize - Calculate payload size for a heap UNDO record
 */
Size
HeapUndoPayloadSize(uint32 tuple_len)
{
	return SizeOfHeapUndoPayload + tuple_len;
}

/*
 * HeapUndoBuildHeader - Fill in a HeapUndoPayloadHeader struct
 *
 * This builds just the fixed header portion of the UNDO payload.
 * Callers can pass this header and the raw tuple data separately to
 * scatter-gather UNDO APIs, avoiding an intermediate palloc + memcpy.
 */
void
HeapUndoBuildHeader(HeapUndoPayloadHeader * hdr,
					BlockNumber blkno, OffsetNumber offset,
					bool relhasindex, uint32 tuple_len)
{
	hdr->blkno = blkno;
	hdr->offset = offset;
	hdr->flags = 0;
	if (relhasindex)
		hdr->flags |= HEAP_UNDO_HAS_INDEX;
	if (tuple_len > 0)
		hdr->flags |= HEAP_UNDO_HAS_TUPLE;
	hdr->tuple_len = tuple_len;
}

/*
 * heap_undo_check_toast - Validate TOAST pointers in UNDO tuple data
 *
 * When enable_undo is enabled on a table, its pg_toast_NNN relation is
 * automatically enrolled in the same UNDO log.  The cross-relation LSN
 * ordering invariant (documented in xactundo.c) guarantees that old TOAST
 * chunks are present when the heap tuple referencing them is restored.
 *
 * If this check nevertheless finds a missing TOAST chunk, it almost
 * certainly means enable_undo is not consistently set on the TOAST table
 * (e.g., the TOAST table was created before TOAST propagation was active,
 * or enable_undo was retroactively disabled on the TOAST table directly,
 * or the cluster was upgraded from a version that predates TOAST propagation).
 *
 * During normal operation, a missing TOAST chunk is an unexpected error and
 * we raise ERROR so the rollback fails visibly rather than silently leaving
 * the row in an inconsistent post-DML state.
 *
 * During crash recovery (InRecovery), an ERROR would be catastrophic -- the
 * recovery process cannot handle per-record errors in UNDO apply.  We fall
 * back to WARNING+skip, accepting potential data loss in exchange for
 * completing recovery.  Operators must investigate log entries at this level.
 *
 * Note: we only check VARATT_IS_EXTERNAL_ONDISK attributes.  Inline
 * compressed values and VARATT_IS_EXTERNAL_EXPANDED datums cannot appear
 * in UNDO records because UNDO stores raw on-disk tuple data, not in-memory
 * expanded representations.
 *
 * Lock safety: this function acquires AccessShareLock on the TOAST relation.
 * AccessShareLock is compatible with all other AccessShareLocks, so there is
 * no deadlock risk here regardless of what relation-level locks the caller
 * holds.  No page locks are held at this call site; the check runs before
 * the critical section is entered.
 *
 * Validates external TOAST pointers in the UNDO record's tuple data.  For
 * each VARATT_IS_EXTERNAL_ONDISK attribute, opens the TOAST relation and
 * performs a SnapshotAny index scan to verify at least one chunk exists.
 *
 * Returns true if the tuple is safe to restore.  During crash recovery,
 * returns false (with WARNING) if any TOAST pointer is invalid; during
 * normal operation, raises ERROR instead of returning false.
 */
static bool
heap_undo_check_toast(Relation rel, const char *tuple_data, uint32 tuple_len)
{
	HeapTupleHeader htup = (HeapTupleHeader) tuple_data;
	HeapTupleData tup;
	TupleDesc	tupdesc;
	int			natts;
	int			i;
	Oid			toastrelid;

	/* Quick exit if no external storage indicated */
	if (!HeapTupleHeaderHasExternal(htup))
		return true;

	/* Quick exit if the relation has no TOAST table */
	toastrelid = rel->rd_rel->reltoastrelid;
	if (!OidIsValid(toastrelid))
		return true;

	tupdesc = RelationGetDescr(rel);
	natts = Min(HeapTupleHeaderGetNatts(htup), tupdesc->natts);

	/*
	 * Build a temporary HeapTupleData for heap_getattr.  We point t_data
	 * at the UNDO record's copy of the tuple, which is read-only.
	 */
	memset(&tup, 0, sizeof(tup));
	tup.t_data = htup;
	tup.t_len = tuple_len;
	ItemPointerSetInvalid(&tup.t_self);
	tup.t_tableOid = RelationGetRelid(rel);

	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);
		Datum		val;
		bool		isnull;

		/* Skip dropped columns and fixed-length types (never TOASTed) */
		if (att->attisdropped || att->attlen > 0)
			continue;

		val = heap_getattr(&tup, i + 1, tupdesc, &isnull);
		if (isnull)
			continue;

		/* Check only on-disk external TOAST pointers */
		if (VARATT_IS_EXTERNAL_ONDISK(DatumGetPointer(val)))
		{
			varatt_external toast_pointer;
			volatile Relation vtoastrel = NULL;
			Relation	toastrel;
			Relation   *toastidxs = NULL;
			int			num_indexes = 0;
			int			validIndex = -1;
			ScanKeyData toastkey;
			SysScanDesc toastscan;
			HeapTuple	ttup;
			bool		found;

			VARATT_EXTERNAL_GET_POINTER(toast_pointer, DatumGetPointer(val));

			/*
			 * Open the TOAST relation and its indexes.  During crash recovery,
			 * table_open() or toast_open_indexes() may raise ERROR on a
			 * corrupted TOAST relation.  Wrap in PG_TRY when InRecovery so
			 * that we can demote the error to a WARNING and skip this
			 * attribute rather than aborting the entire recovery process.
			 * The heap tuple will retain its post-DML state; the operator
			 * must investigate.
			 *
			 * In normal operation, ERROR is propagated as-is.
			 */
			if (InRecovery)
			{
				bool		open_failed = false;

				PG_TRY();
				{
					vtoastrel = table_open(toastrelid, AccessShareLock);
					validIndex = toast_open_indexes((Relation) vtoastrel,
												   AccessShareLock,
												   &toastidxs,
												   &num_indexes);
				}
				PG_CATCH();
				{
					FlushErrorState();
					if (vtoastrel != NULL)
						table_close((Relation) vtoastrel, AccessShareLock);
					ereport(WARNING,
							(errmsg("heap UNDO: could not open TOAST relation %u "
									"for attribute %d chunk validation during "
									"crash recovery; skipping TOAST check",
									toastrelid, i + 1)));
					open_failed = true;
				}
				PG_END_TRY();

				if (open_failed)
					return false;
			}
			else
			{
				vtoastrel = table_open(toastrelid, AccessShareLock);
				validIndex = toast_open_indexes((Relation) vtoastrel,
											   AccessShareLock,
											   &toastidxs,
											   &num_indexes);
			}

			toastrel = (Relation) vtoastrel;

			ScanKeyInit(&toastkey,
						(AttrNumber) 1,
						BTEqualStrategyNumber, F_OIDEQ,
						ObjectIdGetDatum(toast_pointer.va_valueid));

			toastscan = systable_beginscan_ordered(toastrel,
												   toastidxs[validIndex],
												   SnapshotAny,
												   1, &toastkey);

			ttup = systable_getnext_ordered(toastscan, ForwardScanDirection);
			found = HeapTupleIsValid(ttup);

			systable_endscan_ordered(toastscan);
			toast_close_indexes(toastidxs, num_indexes, AccessShareLock);
			table_close(toastrel, AccessShareLock);

			if (!found)
			{
				if (InRecovery)
				{
					/*
					 * During crash recovery we cannot raise ERROR -- fall back
					 * to WARNING and skip the restore.  The row will retain its
					 * post-DML (incorrect) state; operators must investigate.
					 */
					ereport(WARNING,
							(errmsg("heap UNDO: attribute %d of restored tuple has a dangling TOAST pointer "
									"(valueid %u); skipping tuple restore during crash recovery.  "
									"enable_undo was likely not set consistently on the TOAST table.",
									i + 1, toast_pointer.va_valueid)));
					return false;
				}
				else
				{
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("heap UNDO: attribute %d of restored tuple has a dangling TOAST pointer "
									"(valueid %u)",
									i + 1, toast_pointer.va_valueid),
							 errdetail("The TOAST table does not have enable_undo set, or it was "
									   "disabled after UNDO records were written.  Rollback cannot "
									   "safely restore the pre-DML tuple state.")));
				}
			}
		}
	}

	return true;
}

/*
 * heap_undo_apply - Apply a single heap UNDO record
 *
 * This is the rm_undo callback for the heap RM.  It dispatches by
 * subtype (INSERT/DELETE/UPDATE/INPLACE) and applies the physical
 * page modification.
 */
static UndoApplyResult
heap_undo_apply(uint8 rmid, uint16 info, TransactionId xid, Oid reloid,
				const char *payload, Size payload_len, UndoRecPtr urec_ptr)
{
	HeapUndoPayload hdr;
	Relation	rel;
	Buffer		buffer;
	Page		page;
	const char *tuple_data;
	HeapUndoDeltaHeader delta;
	char	   *delta_restored = NULL;
	OffsetNumber hot_new_off = InvalidOffsetNumber;
	bool		skip_tuple_restore = false;

	Assert(rmid == UNDO_RMID_HEAP);

	/* Deserialize the heap-specific payload header */
	if (payload_len < SizeOfHeapUndoPayload)
	{
		ereport(WARNING,
				(errmsg("heap UNDO: payload too short (%zu bytes) for record at %llu",
						payload_len, (unsigned long long) urec_ptr)));
		return UNDO_APPLY_ERROR;
	}

	memcpy(&hdr, payload, SizeOfHeapUndoPayload);
	tuple_data = (payload_len > SizeOfHeapUndoPayload) ?
		payload + SizeOfHeapUndoPayload : NULL;

	/*
	 * Try to open the relation. If it has been dropped, skip this record.
	 */
	rel = try_relation_open(reloid, RowExclusiveLock);
	if (rel == NULL)
	{
		ereport(DEBUG2,
				(errmsg("heap UNDO: relation %u no longer exists, skipping",
						reloid)));
		return UNDO_APPLY_SKIPPED;
	}

	/*
	 * Check if the block still exists (relation may have been truncated).
	 */
	if (RelationGetNumberOfBlocks(rel) <= hdr.blkno)
	{
		ereport(DEBUG2,
				(errmsg("heap UNDO: block %u beyond end of relation %u, skipping",
						hdr.blkno, reloid)));
		relation_close(rel, RowExclusiveLock);
		return UNDO_APPLY_SKIPPED;
	}

	/*
	 * For delta-encoded UPDATEs, pre-parse the delta header and pre-allocate
	 * the restoration buffer BEFORE the critical section.  palloc() can throw
	 * an error, and throwing inside a critical section causes a PANIC.
	 */
	memset(&delta, 0, sizeof(delta));
	if (info == HEAP_UNDO_UPDATE && (hdr.flags & HEAP_UNDO_HAS_DELTA))
	{
		if (payload_len < SizeOfHeapUndoDeltaHeader)
		{
			ereport(WARNING,
					(errmsg("heap UNDO UPDATE delta: payload too short (%zu bytes)",
							payload_len)));
			relation_close(rel, RowExclusiveLock);
			return UNDO_APPLY_ERROR;
		}

		memcpy(&delta, payload, SizeOfHeapUndoDeltaHeader);
		delta_restored = palloc(delta.old_tuple_len);
	}

	/*
	 * Validate TOAST pointers in the old tuple data before restoring it.
	 *
	 * If the rolled-back transaction modified a TOASTed column, the old
	 * TOAST chunks referenced by the UNDO record may no longer exist.
	 * Restoring such a tuple would leave dangling TOAST pointers that crash
	 * on subsequent access.  Check now (before the critical section, where
	 * errors would cause a PANIC) and skip the tuple restore if invalid.
	 */
	if (tuple_data != NULL && hdr.tuple_len > 0 &&
		(info == HEAP_UNDO_DELETE || info == HEAP_UNDO_UPDATE ||
		 info == HEAP_UNDO_INPLACE))
	{
		if (!heap_undo_check_toast(rel, tuple_data, hdr.tuple_len))
			skip_tuple_restore = true;
	}

	/*
	 * Read the target page and acquire exclusive lock.
	 */
	buffer = ReadBuffer(rel, hdr.blkno);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	page = BufferGetPage(buffer);

	/*
	 * Apply the UNDO operation within a critical section.
	 */
	START_CRIT_SECTION();

	switch (info)
	{
		case HEAP_UNDO_INSERT:
			{
				ItemId		lp = PageGetItemId(page, hdr.offset);

				if (!ItemIdIsNormal(lp))
				{
					ereport(DEBUG2,
							(errmsg("heap UNDO INSERT: item (%u) already dead/unused",
									hdr.offset)));
				}
				else if (hdr.flags & HEAP_UNDO_HAS_INDEX)
				{
					ItemIdSetDead(lp);
				}
				else
				{
					ItemIdSetUnused(lp);
					PageSetHasFreeLinePointers(page);
				}
			}
			break;

		case HEAP_UNDO_DELETE:
			if (tuple_data != NULL && hdr.tuple_len > 0)
			{
				if (hdr.flags & HEAP_UNDO_DELETE_VISIBILITY_ONLY)
				{
					/*
					 * Visibility-delta path: only restore the three
					 * tuple-header fields changed by heap_delete().
					 * Column data is unchanged and stays on the page.
					 */
					if (hdr.tuple_len != SizeOfHeapUndoDeleteDelta)
						elog(ERROR,
							 "heap UNDO DELETE visibility-delta: expected %zu bytes, got %u",
							 SizeOfHeapUndoDeleteDelta, hdr.tuple_len);
					else
					{
						HeapUndoDeleteDelta vis_delta;
						ItemId		lp = PageGetItemId(page, hdr.offset);

						memcpy(&vis_delta, tuple_data, SizeOfHeapUndoDeleteDelta);

						if (!ItemIdIsUsed(lp) || !ItemIdHasStorage(lp))
						{
							ereport(WARNING,
									(errmsg("heap UNDO DELETE (visibility-delta): item (%u) has no storage",
											hdr.offset)));
						}
						else
						{
							HeapTupleHeader page_htup =
								(HeapTupleHeader) PageGetItem(page, lp);

							/*
							 * Restore the three visibility fields that
							 * heap_delete() modified.  ItemIdSetNormal is
							 * intentionally omitted: the item ID length
							 * reflects the actual on-page tuple, which is
							 * unchanged.
							 */
							HeapTupleHeaderSetXmax(page_htup,
												   vis_delta.old_xmax);
							page_htup->t_infomask = vis_delta.old_infomask;
							page_htup->t_infomask2 = vis_delta.old_infomask2;
						}
					}
				}
				else if (skip_tuple_restore)
				{
					ereport(WARNING,
							(errmsg("heap UNDO DELETE: skipping tuple restore for blk %u off %u due to dangling TOAST pointer",
									hdr.blkno, hdr.offset)));
				}
				else
				{
					/* Full-tuple restore path (same-xact insert-then-delete). */
					ItemId		lp = PageGetItemId(page, hdr.offset);

					if (!ItemIdIsUsed(lp) || !ItemIdHasStorage(lp))
					{
						ereport(WARNING,
								(errmsg("heap UNDO DELETE: item (%u) has no storage",
										hdr.offset)));
					}
					else
					{
						HeapTupleHeader page_htup =
							(HeapTupleHeader) PageGetItem(page, lp);

						ItemIdSetNormal(lp, ItemIdGetOffset(lp), hdr.tuple_len);
						memcpy(page_htup, tuple_data, hdr.tuple_len);
					}
				}
			}
			else
			{
				/*
				 * Min-mode UNDO records are no longer supported.
				 * enable_undo='min' has been removed.
				 */
				elog(ERROR,
					 "heap UNDO DELETE: unexpected min-mode record without tuple data for blk %u off %u",
					 hdr.blkno, hdr.offset);
			}
			break;

		case HEAP_UNDO_UPDATE:
			if (hdr.flags & HEAP_UNDO_HAS_DELTA)
			{
				/*
				 * Delta-encoded UPDATE: reconstruct the old tuple from the
				 * current (new) tuple on the page plus the delta in the UNDO
				 * record.
				 *
				 * The delta header was pre-parsed and the restoration buffer
				 * was pre-allocated before the critical section (palloc can
				 * throw, which would PANIC in a critical section).
				 */
				ItemId		lp;
				HeapTupleHeader page_htup;
				const char *cur_data;
				Size		cur_len;
				const char *changed_data;

				Assert(delta_restored != NULL);

				changed_data = payload + SizeOfHeapUndoDeltaHeader;

				lp = PageGetItemId(page, hdr.offset);

				if (!ItemIdIsUsed(lp) || !ItemIdHasStorage(lp))
				{
					ereport(WARNING,
							(errmsg("heap UNDO UPDATE delta: item (%u) has no storage",
									hdr.offset)));
					break;
				}

				page_htup = (HeapTupleHeader) PageGetItem(page, lp);
				cur_data = (const char *) page_htup;
				cur_len = ItemIdGetLength(lp);

				/*
				 * Reconstruct old tuple from prefix (from new tuple) +
				 * changed bytes (from UNDO) + suffix (from new tuple).
				 */

				/* Copy unchanged prefix from current (new) tuple */
				if (delta.prefix_len > 0)
					memcpy(delta_restored, cur_data, delta.prefix_len);

				/* Copy changed middle from UNDO record */
				if (delta.changed_len > 0)
					memcpy(delta_restored + delta.prefix_len,
						   changed_data, delta.changed_len);

				/* Copy unchanged suffix from current (new) tuple */
				if (delta.suffix_len > 0)
					memcpy(delta_restored + delta.prefix_len + delta.changed_len,
						   cur_data + cur_len - delta.suffix_len,
						   delta.suffix_len);

				/* Write restored old tuple to the page */
				ItemIdSetNormal(lp, ItemIdGetOffset(lp), delta.old_tuple_len);
				memcpy(page_htup, delta_restored, delta.old_tuple_len);
			}
			else if (tuple_data != NULL && hdr.tuple_len > 0)
			{
				/* Full-tuple UPDATE: direct memcpy restoration */
				if (skip_tuple_restore)
				{
					ereport(WARNING,
							(errmsg("heap UNDO UPDATE: skipping tuple restore for blk %u off %u due to dangling TOAST pointer",
									hdr.blkno, hdr.offset)));
				}
				else
				{
					ItemId		lp = PageGetItemId(page, hdr.offset);

					if (!ItemIdIsUsed(lp) || !ItemIdHasStorage(lp))
					{
						ereport(WARNING,
								(errmsg("heap UNDO UPDATE: item (%u) has no storage",
										hdr.offset)));
					}
					else
					{
						HeapTupleHeader page_htup =
							(HeapTupleHeader) PageGetItem(page, lp);

						ItemIdSetNormal(lp, ItemIdGetOffset(lp), hdr.tuple_len);
						memcpy(page_htup, tuple_data, hdr.tuple_len);
					}
				}
			}
			else
			{
				/*
				 * Min-mode UNDO records are no longer supported.
				 * enable_undo='min' has been removed.
				 */
				elog(ERROR,
					 "heap UNDO UPDATE: unexpected min-mode record without tuple data for blk %u off %u",
					 hdr.blkno, hdr.offset);
			}
			break;

		case HEAP_UNDO_PRUNE:
			/* PRUNE records are informational only -- no rollback action */
			break;

		case HEAP_UNDO_HOT_UPDATE:
			{
				/*
				 * HOT update rollback: restore the old tuple's visibility
				 * info and kill the new (heap-only) tuple version.
				 *
				 * The old tuple's t_ctid was set during the update to point
				 * to the new version, so we follow it to find and kill the
				 * new tuple.
				 */
				HeapUndoHotPayload hot_hdr;
				ItemId		old_lp;
				HeapTupleHeader old_htup;
				OffsetNumber new_off;
				ItemId		new_lp;

				if (payload_len < SizeOfHeapUndoHotPayload)
				{
					ereport(WARNING,
							(errmsg("heap UNDO HOT_UPDATE: payload too short")));
					break;
				}

				memcpy(&hot_hdr, payload, SizeOfHeapUndoHotPayload);

				old_lp = PageGetItemId(page, hot_hdr.old_offset);
				if (!ItemIdIsNormal(old_lp))
				{
					ereport(WARNING,
							(errmsg("heap UNDO HOT_UPDATE: old item (%u) not normal",
									hot_hdr.old_offset)));
					break;
				}

				old_htup = (HeapTupleHeader) PageGetItem(page, old_lp);

				/*
				 * Follow old tuple's t_ctid to find the new tuple.  For HOT
				 * chains, the new tuple is on the same page.
				 */
				new_off = ItemPointerGetOffsetNumber(&old_htup->t_ctid);

				/* Save for CLR generation later */
				hot_new_off = new_off;

				/* Restore old tuple: clear XMAX/HOT flags, restore infomask */
				old_htup->t_infomask = hot_hdr.old_infomask;
				old_htup->t_infomask2 = hot_hdr.old_infomask2;
				/* Point t_ctid back to self (no longer updated) */
				ItemPointerSet(&old_htup->t_ctid,
							   hot_hdr.blkno, hot_hdr.old_offset);

				/*
				 * Kill the new tuple version.  If the relation has indexes,
				 * mark it LP_DEAD so index entries can be cleaned up later.
				 * Otherwise mark it LP_UNUSED for immediate reuse.
				 */
				new_lp = PageGetItemId(page, new_off);
				if (ItemIdIsNormal(new_lp))
				{
					if (hot_hdr.flags & HEAP_UNDO_HAS_INDEX)
						ItemIdSetDead(new_lp);
					else
					{
						ItemIdSetUnused(new_lp);
						PageSetHasFreeLinePointers(page);
					}
				}
			}
			break;

		case HEAP_UNDO_INPLACE:
			if (tuple_data != NULL && hdr.tuple_len > 0)
			{
				if (skip_tuple_restore)
				{
					ereport(WARNING,
							(errmsg("heap UNDO INPLACE: skipping tuple restore for blk %u off %u due to dangling TOAST pointer",
									hdr.blkno, hdr.offset)));
				}
				else
				{
					ItemId		lp = PageGetItemId(page, hdr.offset);

					if (!ItemIdIsNormal(lp))
					{
						ereport(WARNING,
								(errmsg("heap UNDO INPLACE: item (%u) not normal",
										hdr.offset)));
					}
					else
					{
						HeapTupleHeader page_htup =
							(HeapTupleHeader) PageGetItem(page, lp);

						lp->lp_len = hdr.tuple_len;
						memcpy(page_htup, tuple_data, hdr.tuple_len);
					}
				}
			}
			break;

		default:
			ereport(WARNING,
					(errmsg("heap UNDO: unknown subtype %u", info)));
			break;
	}

	MarkBufferDirty(buffer);

	/*
	 * Generate physiological CLR (Compensation Log Record) for crash safety.
	 *
	 * Instead of storing a full 8KB page image, we log just the operation
	 * metadata and any necessary tuple data.  The redo handler re-applies the
	 * exact same page modification during crash recovery.
	 */
	if (RelationNeedsWAL(rel))
	{
		XLogRecPtr	lsn;
		xl_undo_apply xlrec;

		xlrec.urec_ptr = urec_ptr;
		xlrec.xid = xid;
		xlrec.target_locator = rel->rd_locator;
		xlrec.target_block = hdr.blkno;
		xlrec.target_offset = hdr.offset;
		xlrec.operation_type = info;
		xlrec.clr_flags = 0;
		xlrec.tuple_len = 0;

		XLogBeginInsert();

		switch (info)
		{
			case HEAP_UNDO_INSERT:
				/* LP state change only -- no tuple data needed */
				if (hdr.flags & HEAP_UNDO_HAS_INDEX)
					xlrec.clr_flags = UNDO_CLR_LP_DEAD;
				else
					xlrec.clr_flags = UNDO_CLR_LP_UNUSED;
				break;

			case HEAP_UNDO_DELETE:
				if (tuple_data != NULL && hdr.tuple_len > 0)
				{
					if (hdr.flags & HEAP_UNDO_DELETE_VISIBILITY_ONLY)
					{
						/* Compact CLR: store only the 8-byte visibility delta. */
						xlrec.clr_flags = UNDO_CLR_HAS_VISIBILITY;
						xlrec.tuple_len = SizeOfHeapUndoDeleteDelta;
					}
					else
					{
						xlrec.clr_flags = UNDO_CLR_HAS_TUPLE;
						xlrec.tuple_len = hdr.tuple_len;
					}
				}
				break;

			case HEAP_UNDO_INPLACE:
				if (tuple_data != NULL && hdr.tuple_len > 0)
				{
					xlrec.clr_flags = UNDO_CLR_HAS_TUPLE;
					xlrec.tuple_len = hdr.tuple_len;
				}
				break;

			case HEAP_UNDO_UPDATE:
				if (hdr.flags & HEAP_UNDO_HAS_DELTA)
				{
					/*
					 * Delta-encoded: pack the delta parameters and changed
					 * bytes into CLR buffer data for redo.
					 */
					xlrec.clr_flags = UNDO_CLR_HAS_DELTA;
					xlrec.tuple_len = delta.old_tuple_len;
				}
				else if (tuple_data != NULL && hdr.tuple_len > 0)
				{
					xlrec.clr_flags = UNDO_CLR_HAS_TUPLE;
					xlrec.tuple_len = hdr.tuple_len;
				}
				break;

			case HEAP_UNDO_HOT_UPDATE:
				xlrec.clr_flags = UNDO_CLR_HOT_RESTORE;
				break;

			case HEAP_UNDO_PRUNE:
				/* Informational only -- no redo needed */
				break;

			default:
				break;
		}

		XLogRegisterData((char *) &xlrec, SizeOfUndoApply);
		XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

		/*
		 * Register operation-specific buffer data for redo.
		 */
		if (xlrec.clr_flags & UNDO_CLR_HAS_TUPLE)
		{
			/* Full tuple data for DELETE/UPDATE/INPLACE redo */
			if (info == HEAP_UNDO_UPDATE && tuple_data != NULL)
				XLogRegisterBufData(0, tuple_data, hdr.tuple_len);
			else if (tuple_data != NULL)
				XLogRegisterBufData(0, tuple_data, hdr.tuple_len);
		}
		else if (xlrec.clr_flags & UNDO_CLR_HAS_DELTA)
		{
			/*
			 * Delta redo data: old_tuple_len + prefix_len + suffix_len +
			 * changed_len + changed_bytes.  We pack these into a contiguous
			 * buffer for redo.
			 */
			uint32		old_tl = delta.old_tuple_len;
			uint16		plen = delta.prefix_len;
			uint16		slen = delta.suffix_len;
			uint32		clen = delta.changed_len;
			const char *changed_bytes = payload + SizeOfHeapUndoDeltaHeader;

			XLogRegisterBufData(0, (const char *) &old_tl, sizeof(uint32));
			XLogRegisterBufData(0, (const char *) &plen, sizeof(uint16));
			XLogRegisterBufData(0, (const char *) &slen, sizeof(uint16));
			XLogRegisterBufData(0, (const char *) &clen, sizeof(uint32));
			if (clen > 0)
				XLogRegisterBufData(0, changed_bytes, clen);
		}
		else if (xlrec.clr_flags & UNDO_CLR_HAS_VISIBILITY)
		{
			/*
			 * Visibility-delta CLR: store only the three tuple-header
			 * fields that heap_delete() changed (8 bytes total).
			 */
			HeapUndoDeleteDelta vis_delta;

			memcpy(&vis_delta, tuple_data, SizeOfHeapUndoDeleteDelta);
			XLogRegisterBufData(0, (const char *) &vis_delta,
								SizeOfHeapUndoDeleteDelta);
		}
		else if (xlrec.clr_flags & UNDO_CLR_HOT_RESTORE)
		{
			/*
			 * HOT restore: pack the new tuple offset and old infomask values
			 * for redo.  hot_new_off was captured from the old tuple's t_ctid
			 * before the apply modified it.
			 */
			xl_undo_apply_hot hot_clr;
			HeapUndoHotPayload hot_hdr_clr;

			memcpy(&hot_hdr_clr, payload, SizeOfHeapUndoHotPayload);

			hot_clr.new_offset = hot_new_off;
			hot_clr.old_infomask = hot_hdr_clr.old_infomask;
			hot_clr.old_infomask2 = hot_hdr_clr.old_infomask2;

			XLogRegisterBufData(0, (const char *) &hot_clr,
								SizeOfUndoApplyHot);
		}

		lsn = XLogInsert(RM_UNDO_ID, XLOG_UNDO_APPLY_RECORD);
		PageSetLSN(page, lsn);

		/*
		 * With UNDO-in-WAL, UNDO records live in the WAL stream and are
		 * immutable.  The CLR WAL record itself (XLOG_UNDO_APPLY_RECORD)
		 * serves as proof that this UNDO record has been applied.  During
		 * crash recovery, the CLR's existence prevents double-application.
		 * No need to write back into the UNDO record.
		 */
	}

	END_CRIT_SECTION();

	UnlockReleaseBuffer(buffer);

	if (delta_restored != NULL)
		pfree(delta_restored);

	relation_close(rel, RowExclusiveLock);

	return UNDO_APPLY_SUCCESS;
}

/*
 * heap_undo_desc - Describe a heap UNDO record for debugging
 */
static void
heap_undo_desc(StringInfo buf, uint8 rmid, uint16 info,
			   const char *payload, Size payload_len)
{
	HeapUndoPayload hdr;
	const char *opname;

	if (payload_len < SizeOfHeapUndoPayload)
	{
		appendStringInfo(buf, "heap UNDO: invalid payload");
		return;
	}

	memcpy(&hdr, payload, SizeOfHeapUndoPayload);

	switch (info)
	{
		case HEAP_UNDO_INSERT:
			opname = "INSERT";
			break;
		case HEAP_UNDO_DELETE:
			opname = "DELETE";
			break;
		case HEAP_UNDO_UPDATE:
			opname = "UPDATE";
			break;
		case HEAP_UNDO_PRUNE:
			opname = "PRUNE";
			break;
		case HEAP_UNDO_INPLACE:
			opname = "INPLACE";
			break;
		case HEAP_UNDO_HOT_UPDATE:
			opname = "HOT_UPDATE";
			break;
		default:
			opname = "UNKNOWN";
			break;
	}

	if (info == HEAP_UNDO_HOT_UPDATE &&
		payload_len >= SizeOfHeapUndoHotPayload)
	{
		HeapUndoHotPayload hhdr;

		memcpy(&hhdr, payload, SizeOfHeapUndoHotPayload);
		appendStringInfo(buf, "heap HOT_UPDATE: blk %u old_off %u "
						 "infomask 0x%04x infomask2 0x%04x",
						 hhdr.blkno, hhdr.old_offset,
						 hhdr.old_infomask, hhdr.old_infomask2);
	}
	else if (info == HEAP_UNDO_UPDATE && (hdr.flags & HEAP_UNDO_HAS_DELTA) &&
			 payload_len >= SizeOfHeapUndoDeltaHeader)
	{
		HeapUndoDeltaHeader dhdr;

		memcpy(&dhdr, payload, SizeOfHeapUndoDeltaHeader);
		appendStringInfo(buf, "heap UPDATE (delta): blk %u off %u "
						 "old_len %u prefix %u suffix %u changed %u",
						 dhdr.blkno, dhdr.offset, dhdr.old_tuple_len,
						 dhdr.prefix_len, dhdr.suffix_len, dhdr.changed_len);
	}
	else
	{
		appendStringInfo(buf, "heap %s: blk %u off %u tuple_len %u",
						 opname, hdr.blkno, hdr.offset, hdr.tuple_len);
	}
}
