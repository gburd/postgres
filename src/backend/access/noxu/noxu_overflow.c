/*
 * noxu_overflow.c
 *		Routines for storing oversized tuples in Noxu
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/noxu/noxu_overflow.c
 */
#include "postgres.h"

#include "access/toast_internals.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "access/noxu_internal.h"
#include "access/noxu_wal.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/datum.h"
#include "utils/rel.h"

static void nxoverflow_wal_log_newpage(Buffer prevbuf, Buffer buf, nxtid tid, AttrNumber attno,
									int offset, int32 total_size);

/*
 * Overflow a datum, inside the Noxu file.
 *
 * This is similar to regular overflowing, but instead of using a separate index and
 * heap, the datum is stored within the same Noxu file as all the btrees and
 * stuff. A chain of "overflow-pages" is allocated for the datum, and each page is filled
 * with as much of the datum as possible.
 */
Datum
noxu_overflow_datum(Relation rel, AttrNumber attno, Datum value, nxtid tid)
{
	varatt_nx_overflowptr *overflowptr;
	BlockNumber firstblk = InvalidBlockNumber;
	Buffer		buf = InvalidBuffer;
	Page		page;
	NXOverflowPageOpaque *opaque;
	Buffer		prevbuf = InvalidBuffer;
	NXOverflowPageOpaque *prevopaque = NULL;
	char	   *ptr;
	int32		total_size;
	int32		offset;
	bool		is_first;
	struct varlena *vl;

	Assert(tid != InvalidNXTid);

	/*
	 * TID btree will always be inserted first, so there must be > 0 blocks
	 */
	Assert(RelationGetNumberOfBlocks(rel) != 0);

	/*
	 * Try to compress the datum in place first.  If the compressed result
	 * fits within MaxNoxuDatumSize we can return it directly, avoiding the
	 * I/O cost of allocating and writing overflow pages.
	 *
	 * toast_compress_datum() requires a non-external, non-compressed input,
	 * so skip this when the datum is already in one of those formats.
	 */
	vl = (struct varlena *) DatumGetPointer(value);
	if (!VARATT_IS_EXTERNAL(vl) && !VARATT_IS_COMPRESSED(vl))
	{
		Form_pg_attribute attr = TupleDescAttr(rel->rd_att, attno - 1);
		Datum		compressed;

		compressed = toast_compress_datum(value, attr->attcompression);
		if (DatumGetPointer(compressed) != NULL)
		{
			struct varlena *cvl = (struct varlena *) DatumGetPointer(compressed);

			if (VARSIZE_ANY_EXHDR(cvl) <= MaxNoxuDatumSize)
				return compressed;

			/* Compressed but still too large; fall through to overflow */
			pfree(cvl);
		}
	}

	/*
	 * Compression didn't shrink it enough (or wasn't applicable); allocate
	 * overflow pages.
	 */

	ptr = VARDATA_ANY(vl);
	total_size = VARSIZE_ANY_EXHDR(vl);
	offset = 0;
	is_first = true;
	while (total_size - offset > 0)
	{
		Size		thisbytes;

		buf = nxpage_getnewbuf(rel, InvalidBuffer);
		if (prevbuf == InvalidBuffer)
			firstblk = BufferGetBlockNumber(buf);

		START_CRIT_SECTION();

		page = BufferGetPage(buf);
		PageInit(page, BLCKSZ, sizeof(NXOverflowPageOpaque));

		thisbytes = Min(total_size - offset, PageGetExactFreeSpace(page));

		opaque = (NXOverflowPageOpaque *) PageGetSpecialPointer(page);
		opaque->nx_tid = tid;
		opaque->nx_attno = attno;
		opaque->nx_total_size = total_size;
		opaque->nx_slice_offset = offset;
		opaque->nx_prev = is_first ? InvalidBlockNumber : BufferGetBlockNumber(prevbuf);
		opaque->nx_next = InvalidBlockNumber;
		opaque->nx_flags = 0;
		opaque->nx_page_id = NX_OVERFLOW_PAGE_ID;

		memcpy((char *) page + SizeOfPageHeaderData, ptr, thisbytes);
		((PageHeader) page)->pd_lower += thisbytes;

		if (!is_first)
		{
			prevopaque->nx_next = BufferGetBlockNumber(buf);
			MarkBufferDirty(prevbuf);
		}

		MarkBufferDirty(buf);

		if (RelationNeedsWAL(rel))
			nxoverflow_wal_log_newpage(prevbuf, buf, tid, attno, offset, total_size);

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

	overflowptr = palloc0(sizeof(varatt_nx_overflowptr));
	SET_VARTAG_1B_E(overflowptr, VARTAG_NOXU);
	overflowptr->nxt_block = firstblk;

	return PointerGetDatum(overflowptr);
}

Datum
noxu_overflow_flatten(Relation rel, AttrNumber attno, nxtid tid, Datum overflowed)
{
	varatt_nx_overflowptr *overflowptr = (varatt_nx_overflowptr *) DatumGetPointer(overflowed);
	BlockNumber nextblk;
	BlockNumber prevblk;
	char	   *result = NULL;
	char	   *ptr = NULL;
	int32		total_size = 0;

	Assert(overflowptr->va_tag == VARTAG_NOXU);

	prevblk = InvalidBlockNumber;
	nextblk = overflowptr->nxt_block;

	while (nextblk != InvalidBlockNumber)
	{
		Buffer		buf;
		Page		page;
		NXOverflowPageOpaque *opaque;
		uint32		size;

		buf = ReadBuffer(rel, nextblk);
		page = BufferGetPage(buf);
		LockBuffer(buf, BUFFER_LOCK_SHARE);

		opaque = (NXOverflowPageOpaque *) PageGetSpecialPointer(page);

		Assert(opaque->nx_attno == attno);
		Assert(opaque->nx_prev == prevblk);

		if (prevblk == InvalidBlockNumber)
		{
			Assert(opaque->nx_tid == tid);

			total_size = opaque->nx_total_size;

			result = palloc(total_size + VARHDRSZ);
			SET_VARSIZE(result, total_size + VARHDRSZ);
			ptr = result + VARHDRSZ;
		}

		size = ((PageHeader) page)->pd_lower - SizeOfPageHeaderData;
		memcpy(ptr, (char *) page + SizeOfPageHeaderData, size);
		ptr += size;

		prevblk = nextblk;
		nextblk = opaque->nx_next;
		UnlockReleaseBuffer(buf);
	}
	Assert(total_size > 0);
	Assert(ptr == result + total_size + VARHDRSZ);

	return PointerGetDatum(result);
}

static void
nxoverflow_wal_log_newpage(Buffer prevbuf, Buffer buf, nxtid tid, AttrNumber attno,
						int offset, int32 total_size)
{
	wal_noxu_overflow_newpage xlrec;
	XLogRecPtr	recptr;

	Assert(offset <= total_size);

	xlrec.tid = tid;
	xlrec.attno = attno;
	xlrec.offset = offset;
	xlrec.total_size = total_size;

	XLogBeginInsert();

	/* Register ALL buffers first, before any data */
	/*
	 * It is easier to just force a full-page image, than WAL-log data. That
	 * means that the information in the wal_noxu_overflow_newpage struct isn't
	 * really necessary, but keep it for now, for the benefit of debugging
	 * with pg_waldump.
	 */
	XLogRegisterBuffer(0, buf, REGBUF_FORCE_IMAGE | REGBUF_STANDARD);

	if (BufferIsValid(prevbuf))
		XLogRegisterBuffer(1, prevbuf, REGBUF_STANDARD);

	/* Now register data after buffers are registered */
	XLogRegisterData((char *) &xlrec, SizeOfNXWalOverflowNewPage);

	recptr = XLogInsert(RM_NOXU_ID, WAL_NOXU_OVERFLOW_NEWPAGE);

	PageSetLSN(BufferGetPage(buf), recptr);
	if (BufferIsValid(prevbuf))
		PageSetLSN(BufferGetPage(prevbuf), recptr);
}

void
nxoverflow_newpage_redo(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
#if UNUSED
	wal_noxu_overflow_newpage *xlrec = (wal_noxu_overflow_newpage *) XLogRecGetData(record);
#endif
	BlockNumber blkno;
	Buffer		buf;
	Buffer		prevbuf = InvalidBuffer;

	XLogRecGetBlockTag(record, 0, NULL, NULL, &blkno);

	if (XLogReadBufferForRedo(record, 0, &buf) != BLK_RESTORED)
		elog(ERROR, "noxu overflow newpage WAL record did not contain a full-page image");

	if (XLogRecHasBlockRef(record, 1))
	{
		if (XLogReadBufferForRedo(record, 1, &prevbuf) == BLK_NEEDS_REDO)
		{
			Page		prevpage = BufferGetPage(prevbuf);
			NXOverflowPageOpaque *prevopaque;

			prevopaque = (NXOverflowPageOpaque *) PageGetSpecialPointer(prevpage);
			prevopaque->nx_next = BufferGetBlockNumber(buf);

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
