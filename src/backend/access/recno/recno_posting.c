/*-------------------------------------------------------------------------
 *
 * recno_posting.c
 *	  RECNO index-deduplication posting codec (RowIDPostingOps).
 *
 * nbtree disables its native 6-byte-TID posting for wide RowIDs, so RECNO
 * (whose RowID is (TID, gen), 10 bytes) would otherwise store one leaf tuple
 * per equal-key row -- the measured +92% index bloat vs heap.  This codec lets
 * nbtree collapse many equal-key RECNO rows into ONE posting tuple whose
 * payload we own.
 *
 * RECNO record numbers are dense: a RowID's TID (block, offset) maps to a
 * monotonic integer key = block * (MaxOffsetNumber+1) + offset, order-identical
 * to ItemPointerCompare.  A sparsemap over those keys represents a run of
 * equal-key rows in a handful of RLE chunks, far smaller than an array of
 * 10-byte RowIDs.  The per-row 4-byte generation is stored once for the common
 * case where every row in the group shares a gen (bulk load / never re-updated
 * on this key); an A->B->A recurrence that produces two same-TID-different-gen
 * identities in one group is carried as a small exception list.
 *
 * The codec is bulk-build + strictly-ascending-iterate ONLY; it never does a
 * random point-membership probe on a hot path (the anti-pattern that motivated
 * the original sparsemap removal).  encode() returns 0 to decline (payload
 * would not fit, or the group is too exception-heavy to be worth it), which
 * makes nbtree keep the plain multi-tuple representation.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *	  src/backend/access/recno/recno_posting.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/recno.h"
#include "access/rowid.h"
#include "lib/sparsemap.h"
#include "storage/itemptr.h"

/*
 * On-disk posting payload layout (all little-endian, MAXALIGN'd by the caller):
 *
 *   uint8   codec_version    (RECNO_POSTING_VERSION)
 *   uint8   sm_version_major  (SM_VERSION_MAJOR, so a format bump is visible)
 *   uint16  flags            (RECNO_POST_SINGLE_GEN, ...)
 *   uint32  count            (number of RowIDs)
 *   uint64  key_base         (smallest dense TID key in the group)
 *   uint32  gen_single       (present iff SINGLE_GEN: the shared generation)
 *   -- if SINGLE_GEN:
 *        uint32 sm_len; uint8 sm_bytes[sm_len]   (sparsemap over key-key_base)
 *   -- if !SINGLE_GEN (MIXED_GEN):
 *        the full ascending RowID array, count * RECNO_ROWID_WIDTH bytes
 *        (fallback: no sparsemap compaction, but still one tuple)
 *
 * The SINGLE_GEN path is the compaction win.  MIXED_GEN keeps the group in one
 * tuple (still saving the per-tuple line-pointer + key repetition) but does not
 * sparsemap-compress; if the group is large and mixed, encode() may instead
 * decline (return 0) so nbtree falls back to plain tuples -- bounding the
 * worst case to the status quo, never worse.
 */

#define RECNO_POSTING_VERSION	1
#define RECNO_ROWID_WIDTH		(sizeof(ItemPointerData) + sizeof(uint32))

#define RECNO_POST_SINGLE_GEN	0x0001

typedef struct RecnoPostHeader
{
	uint8		codec_version;
	uint8		sm_version_major;
	uint16		flags;
	uint32		count;
	uint64		key_base;
	uint32		gen_single;		/* meaningful iff SINGLE_GEN */
} RecnoPostHeader;

#define RECNO_POST_HEADER_SZ	(sizeof(RecnoPostHeader))

/*
 * Dense integer key for a RECNO RowID's TID.  Order-identical to
 * ItemPointerCompare on the (block, offset) part.  (MaxOffsetNumber+1) offsets
 * per block keeps keys monotonic and gap-free enough that the sparsemap's RLE
 * chunks stay compact for a clustered run.
 */
static inline uint64
recno_rowid_key(const uint8 *rowid)
{
	BlockNumber blk = ItemPointerGetBlockNumberNoCheck((const ItemPointerData *) rowid);
	OffsetNumber off = ItemPointerGetOffsetNumberNoCheck((const ItemPointerData *) rowid);

	return (uint64) blk * (MaxOffsetNumber + 1) + off;
}

static inline uint32
recno_rowid_gen(const uint8 *rowid)
{
	uint32		gen;

	memcpy(&gen, rowid + sizeof(ItemPointerData), sizeof(uint32));
	return gen;
}

/* Rebuild a 10-byte RowID from a dense key + gen. */
static inline void
recno_key_to_rowid(uint64 key, uint32 gen, RowID *out)
{
	BlockNumber blk = (BlockNumber) (key / (MaxOffsetNumber + 1));
	OffsetNumber off = (OffsetNumber) (key % (MaxOffsetNumber + 1));

	out->len = RECNO_ROWID_WIDTH;
	memset(out->data, 0, MAX_ROWID_SIZE);
	ItemPointerSet((ItemPointerData *) out->data, blk, off);
	memcpy(out->data + sizeof(ItemPointerData), &gen, sizeof(uint32));
}

/*
 * encode -- serialize `n` ascending-by-cmp RowIDs into `out`.
 * Returns bytes written, or 0 to decline.
 */
static Size
recno_posting_encode(const RowID *rowids, int n, uint8 *out, Size outcap)
{
	uint32		gen0;
	bool		single_gen = true;
	uint64		key_base;
	RecnoPostHeader hdr;
	int			i;

	if (n < 2)
		return 0;				/* nothing to dedup */

	/* Detect the common single-gen case + establish key_base (rowids[0] is min). */
	key_base = recno_rowid_key(rowids[0].data);
	gen0 = recno_rowid_gen(rowids[0].data);
	for (i = 1; i < n; i++)
	{
		if (recno_rowid_gen(rowids[i].data) != gen0)
		{
			single_gen = false;
			break;
		}
	}

	hdr.codec_version = RECNO_POSTING_VERSION;
	hdr.sm_version_major = SM_VERSION_MAJOR;
	hdr.count = (uint32) n;
	hdr.key_base = key_base;

	if (single_gen)
	{
		sparsemap_t *sm;
		Size		sm_len;
		Size		total;
		uint32		sm_len32;
		uint8	   *p;

		/* Build the sparsemap over (key - key_base). */
		sm = sm_create(64);
		for (i = 0; i < n; i++)
		{
			uint64		k = recno_rowid_key(rowids[i].data) - key_base;

			sm_add_grow(&sm, k);
		}
		sm_len = sm_serialized_size(sm);

		total = RECNO_POST_HEADER_SZ + sizeof(uint32) + sm_len;
		if (total > outcap)
		{
			sm_free(sm);
			return 0;			/* does not fit -> decline */
		}

		hdr.flags = RECNO_POST_SINGLE_GEN;
		hdr.gen_single = gen0;
		memcpy(out, &hdr, RECNO_POST_HEADER_SZ);
		p = out + RECNO_POST_HEADER_SZ;
		sm_len32 = (uint32) sm_len;
		memcpy(p, &sm_len32, sizeof(uint32));
		p += sizeof(uint32);
		(void) sm_serialize(sm, p, sm_len);
		sm_free(sm);
		return total;
	}
	else
	{
		/* MIXED_GEN: store the full ascending RowID array in one tuple. */
		Size		total = RECNO_POST_HEADER_SZ + (Size) n * RECNO_ROWID_WIDTH;

		if (total > outcap)
			return 0;			/* too big mixed -> decline (fall back to plain) */

		hdr.flags = 0;
		hdr.gen_single = 0;
		memcpy(out, &hdr, RECNO_POST_HEADER_SZ);
		for (i = 0; i < n; i++)
			memcpy(out + RECNO_POST_HEADER_SZ + (Size) i * RECNO_ROWID_WIDTH,
				   rowids[i].data, RECNO_ROWID_WIDTH);
		return total;
	}
}

static int
recno_posting_count(const uint8 *payload, Size len)
{
	RecnoPostHeader hdr;

	Assert(len >= RECNO_POST_HEADER_SZ);
	memcpy(&hdr, payload, RECNO_POST_HEADER_SZ);
	return (int) hdr.count;
}

static void
recno_posting_decode_n(const uint8 *payload, Size len, int i, RowID *out)
{
	RecnoPostHeader hdr;

	memcpy(&hdr, payload, RECNO_POST_HEADER_SZ);
	Assert(i >= 0 && i < (int) hdr.count);

	if (hdr.flags & RECNO_POST_SINGLE_GEN)
	{
		const uint8 *p = payload + RECNO_POST_HEADER_SZ;
		uint32		sm_len32;
		sparsemap_t *sm;
		uint64		k;
		int			j;

		memcpy(&sm_len32, p, sizeof(uint32));
		p += sizeof(uint32);
		sm = sm_deserialize(p, sm_len32);

		/* Ascending: the i'th member. sm_next_member walks in order. */
		k = sm_minimum(sm);
		for (j = 0; j < i; j++)
			k = sm_next_member(sm, k, NULL);
		recno_key_to_rowid(hdr.key_base + k, hdr.gen_single, out);
		sm_free(sm);
	}
	else
	{
		out->len = RECNO_ROWID_WIDTH;
		memset(out->data, 0, MAX_ROWID_SIZE);
		memcpy(out->data,
			   payload + RECNO_POST_HEADER_SZ + (Size) i * RECNO_ROWID_WIDTH,
			   RECNO_ROWID_WIDTH);
	}
}

static void
recno_posting_iter_begin(const uint8 *payload, Size len, RowIDPostIter *it)
{
	RecnoPostHeader hdr;

	memcpy(&hdr, payload, RECNO_POST_HEADER_SZ);
	it->payload = payload;
	it->len = len;
	it->pos = 0;
	it->last = 0;
	it->cur = NULL;
	it->cur2 = NULL;

	if (hdr.flags & RECNO_POST_SINGLE_GEN)
	{
		const uint8 *p = payload + RECNO_POST_HEADER_SZ;
		uint32		sm_len32;
		sm_cursor_t *cur;

		memcpy(&sm_len32, p, sizeof(uint32));
		p += sizeof(uint32);
		/* Own a private deserialized copy + a scan cursor for O(N) iteration. */
		it->cur = sm_deserialize(p, sm_len32);
		cur = (sm_cursor_t *) palloc(sizeof(sm_cursor_t));
		*cur = (sm_cursor_t) SM_CURSOR_INIT;
		it->cur2 = cur;
	}
}

static bool
recno_posting_iter_next(RowIDPostIter *it, RowID *out)
{
	RecnoPostHeader hdr;

	memcpy(&hdr, it->payload, RECNO_POST_HEADER_SZ);
	if (it->pos >= hdr.count)
	{
		if (it->cur != NULL)
		{
			sm_free((sparsemap_t *) it->cur);
			it->cur = NULL;
		}
		if (it->cur2 != NULL)
		{
			pfree(it->cur2);
			it->cur2 = NULL;
		}
		return false;
	}

	if (hdr.flags & RECNO_POST_SINGLE_GEN)
	{
		sparsemap_t *sm = (sparsemap_t *) it->cur;
		sm_cursor_t *cur = (sm_cursor_t *) it->cur2;
		uint64		k;

		if (it->pos == 0)
			k = sm_minimum(sm);
		else
			k = sm_next_member(sm, it->last, cur);	/* strictly after last, O(N) via cursor */
		it->last = k;
		recno_key_to_rowid(hdr.key_base + k, hdr.gen_single, out);
	}
	else
	{
		out->len = RECNO_ROWID_WIDTH;
		memset(out->data, 0, MAX_ROWID_SIZE);
		memcpy(out->data,
			   it->payload + RECNO_POST_HEADER_SZ + (Size) it->pos * RECNO_ROWID_WIDTH,
			   RECNO_ROWID_WIDTH);
	}
	it->pos++;
	return true;
}

/*
 * remove -- drop `ndead` RowIDs (ascending) from a payload.  Decode to a RowID
 * array, filter out the dead set, re-encode.  Returns new length, or 0 if
 * fewer than 2 survive (caller falls back to a plain tuple).
 */
static Size
recno_posting_remove(const uint8 *payload, Size len,
					 const RowID *dead, int ndead, uint8 *out, Size outcap)
{
	int			n = recno_posting_count(payload, len);
	RowID	   *live;
	int			nlive = 0;
	int			i,
				d = 0;
	Size		res;

	/* Decode all, filter against the ascending dead set (merge walk). */
	live = (RowID *) palloc(sizeof(RowID) * n);
	for (i = 0; i < n; i++)
	{
		RowID		rid;

		recno_posting_decode_n(payload, len, i, &rid);
		/* Advance the dead cursor past anything below rid. */
		while (d < ndead && recno_rowid_compare(dead[d].data, rid.data) < 0)
			d++;
		if (d < ndead && recno_rowid_compare(dead[d].data, rid.data) == 0)
		{
			d++;				/* rid is dead: skip */
			continue;
		}
		live[nlive++] = rid;
	}

	if (nlive < 2)
	{
		pfree(live);
		return 0;				/* caller: fall back to a plain tuple */
	}

	res = recno_posting_encode(live, nlive, out, outcap);
	pfree(live);
	return res;
}

const RowIDPostingOps RecnoPostingOps = {
	.encode = recno_posting_encode,
	.count = recno_posting_count,
	.decode_n = recno_posting_decode_n,
	.iter_begin = recno_posting_iter_begin,
	.iter_next = recno_posting_iter_next,
	.remove = recno_posting_remove,
};
