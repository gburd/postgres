/*
 * orvos_toast.c
 *		Routines for Toasting oversized tuples in Orvos
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/orvos/orvos_toast.c
 */
#include "postgres.h"

#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "access/orvos_internal.h"
#include "access/orvos_wal.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/datum.h"
#include "utils/rel.h"

static void ovtoast_wal_log_newpage(Buffer prevbuf, Buffer buf, ovtid tid, AttrNumber attno,
									int offset, int32 total_size);

/*
 * Toast a datum, inside the Orvos file.
 *
 * This is similar to regular toasting, but instead of using a separate index and
 * heap, the datum is stored within the same Orvos file as all the btrees and
 * stuff. A chain of "toast-pages" is allocated for the datum, and each page is filled
 * with as much of the datum as possible.
 */
Datum
orvos_toast_datum(Relation rel, AttrNumber attno, Datum value, ovtid tid)
{
	varatt_ov_toastptr *toastptr;
	BlockNumber firstblk = InvalidBlockNumber;
	Buffer		buf = InvalidBuffer;
	Page		page;
	OVToastPageOpaque *opaque;
	Buffer		prevbuf = InvalidBuffer;
	OVToastPageOpaque *prevopaque = NULL;
	char	   *ptr;
	int32		total_size;
	int32		offset;
	bool		is_first;
	struct varlena *vl;

	Assert(tid != InvalidOVTid);

	/*
	 * TID btree will always be inserted first, so there must be > 0 blocks
	 */
	Assert(RelationGetNumberOfBlocks(rel) != 0);

	/*
	 * TODO: try to compress it in place first. Maybe just call
	 * toast_compress_datum?
	 */

	/*
	 * If that doesn't reduce it enough, allocate a toast page for it.
	 */
	vl = (struct varlena *) DatumGetPointer(value);

	ptr = VARDATA_ANY(vl);
	total_size = VARSIZE_ANY_EXHDR(vl);
	offset = 0;
	is_first = true;
	while (total_size - offset > 0)
	{
		Size		thisbytes;

		buf = ovpage_getnewbuf(rel, InvalidBuffer);
		if (prevbuf == InvalidBuffer)
			firstblk = BufferGetBlockNumber(buf);

		START_CRIT_SECTION();

		page = BufferGetPage(buf);
		PageInit(page, BLCKSZ, sizeof(OVToastPageOpaque));

		thisbytes = Min(total_size - offset, PageGetExactFreeSpace(page));

		opaque = (OVToastPageOpaque *) PageGetSpecialPointer(page);
		opaque->ov_tid = tid;
		opaque->ov_attno = attno;
		opaque->ov_total_size = total_size;
		opaque->ov_slice_offset = offset;
		opaque->ov_prev = is_first ? InvalidBlockNumber : BufferGetBlockNumber(prevbuf);
		opaque->ov_next = InvalidBlockNumber;
		opaque->ov_flags = 0;
		opaque->ov_page_id = OV_TOAST_PAGE_ID;

		memcpy((char *) page + SizeOfPageHeaderData, ptr, thisbytes);
		((PageHeader) page)->pd_lower += thisbytes;

		if (!is_first)
		{
			prevopaque->ov_next = BufferGetBlockNumber(buf);
			MarkBufferDirty(prevbuf);
		}

		MarkBufferDirty(buf);

		if (RelationNeedsWAL(rel))
			ovtoast_wal_log_newpage(prevbuf, buf, tid, attno, offset, total_size);

		END_CRIT_SECTION();

		if (prevbuf != InvalidBuffer)
			UnlockReleaseBuffer(prevbuf);
		ptr += thisbytes;
		offset += thisbytes;
		prevbuf = buf;
		prevopaque = opaque;
		is_first = false;
	}

	UnlockReleaseBuffer(buf);

	toastptr = palloc0(sizeof(varatt_ov_toastptr));
	SET_VARTAG_1B_E(toastptr, VARTAG_ORVOS);
	toastptr->ovt_block = firstblk;

	return PointerGetDatum(toastptr);
}

Datum
orvos_toast_flatten(Relation rel, AttrNumber attno, ovtid tid, Datum toasted)
{
	varatt_ov_toastptr *toastptr = (varatt_ov_toastptr *) DatumGetPointer(toasted);
	BlockNumber nextblk;
	BlockNumber prevblk;
	char	   *result = NULL;
	char	   *ptr = NULL;
	int32		total_size = 0;

	Assert(toastptr->va_tag == VARTAG_ORVOS);

	prevblk = InvalidBlockNumber;
	nextblk = toastptr->ovt_block;

	while (nextblk != InvalidBlockNumber)
	{
		Buffer		buf;
		Page		page;
		OVToastPageOpaque *opaque;
		uint32		size;

		buf = ReadBuffer(rel, nextblk);
		page = BufferGetPage(buf);
		LockBuffer(buf, BUFFER_LOCK_SHARE);

		opaque = (OVToastPageOpaque *) PageGetSpecialPointer(page);

		Assert(opaque->ov_attno == attno);
		Assert(opaque->ov_prev == prevblk);

		if (prevblk == InvalidBlockNumber)
		{
			Assert(opaque->ov_tid == tid);

			total_size = opaque->ov_total_size;

			result = palloc(total_size + VARHDRSZ);
			SET_VARSIZE(result, total_size + VARHDRSZ);
			ptr = result + VARHDRSZ;
		}

		size = ((PageHeader) page)->pd_lower - SizeOfPageHeaderData;
		memcpy(ptr, (char *) page + SizeOfPageHeaderData, size);
		ptr += size;

		prevblk = nextblk;
		nextblk = opaque->ov_next;
		UnlockReleaseBuffer(buf);
	}
	Assert(total_size > 0);
	Assert(ptr == result + total_size + VARHDRSZ);

	return PointerGetDatum(result);
}

static void
ovtoast_wal_log_newpage(Buffer prevbuf, Buffer buf, ovtid tid, AttrNumber attno,
						int offset, int32 total_size)
{
	wal_orvos_toast_newpage xlrec;
	XLogRecPtr	recptr;

	Assert(offset <= total_size);

	xlrec.tid = tid;
	xlrec.attno = attno;
	xlrec.offset = offset;
	xlrec.total_size = total_size;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfZSWalToastNewPage);

	/*
	 * It is easier to just force a full-page image, than WAL-log data. That
	 * means that the information in the wal_orvos_toast_newpage struct isn't
	 * really necessary, but keep it for now, for the benefit of debugging
	 * with pg_waldump.
	 */
	XLogRegisterBuffer(0, buf, REGBUF_FORCE_IMAGE | REGBUF_STANDARD);

	if (BufferIsValid(prevbuf))
		XLogRegisterBuffer(1, prevbuf, REGBUF_STANDARD);

	recptr = XLogInsert(RM_ORVOS_ID, WAL_ORVOS_TOAST_NEWPAGE);

	PageSetLSN(BufferGetPage(buf), recptr);
	if (BufferIsValid(prevbuf))
		PageSetLSN(BufferGetPage(prevbuf), recptr);
}

void
ovtoast_newpage_redo(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
#if UNUSED
	wal_orvos_toast_newpage *xlrec = (wal_orvos_toast_newpage *) XLogRecGetData(record);
#endif
	BlockNumber blkno;
	Buffer		buf;
	Buffer		prevbuf = InvalidBuffer;

	XLogRecGetBlockTag(record, 0, NULL, NULL, &blkno);

	if (XLogReadBufferForRedo(record, 0, &buf) != BLK_RESTORED)
		elog(ERROR, "orvos toast newpage WAL record did not contain a full-page image");

	if (XLogRecHasBlockRef(record, 1))
	{
		if (XLogReadBufferForRedo(record, 1, &prevbuf) == BLK_NEEDS_REDO)
		{
			Page		prevpage = BufferGetPage(prevbuf);
			OVToastPageOpaque *prevopaque;

			prevopaque = (OVToastPageOpaque *) PageGetSpecialPointer(prevpage);
			prevopaque->ov_next = BufferGetBlockNumber(buf);

			PageSetLSN(prevpage, lsn);
			MarkBufferDirty(prevbuf);
		}
	}
	else
		prevbuf = InvalidBuffer;

	if (BufferIsValid(prevbuf))
		UnlockReleaseBuffer(prevbuf);
	UnlockReleaseBuffer(buf);
}
