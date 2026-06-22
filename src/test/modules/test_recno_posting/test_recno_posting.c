/*
 * test_recno_posting.c
 *		Unit + fuzz test for the RECNO index-deduplication posting codec
 *		(RecnoPostingOps in src/backend/access/recno/recno_posting.c).
 *
 * SQL: SELECT test_recno_posting();  -- returns void, ereports on failure.
 */
#include "postgres.h"

#include "access/recno.h"
#include "access/rowid.h"
#include "fmgr.h"
#include "storage/itemptr.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

#define RID_WIDTH	(sizeof(ItemPointerData) + sizeof(uint32))

static void
mk_rowid(RowID *r, BlockNumber blk, OffsetNumber off, uint32 gen)
{
	r->len = RID_WIDTH;
	memset(r->data, 0, MAX_ROWID_SIZE);
	ItemPointerSet((ItemPointerData *) r->data, blk, off);
	memcpy(r->data + sizeof(ItemPointerData), &gen, sizeof(uint32));
}

/* round-trip: encode n rowids, iterate them back, assert identical order+bytes */
static void
check_roundtrip(const RowID *in, int n, const char *label)
{
	uint8		buf[BLCKSZ];
	Size		len;
	RowIDPostIter it;
	RowID		got;
	int			i;

	len = RecnoPostingOps.encode(in, n, buf, sizeof(buf));
	if (len == 0)
		return;					/* codec declined (fallback) -- valid, not a bug */

	if (RecnoPostingOps.count(buf, len) != n)
		elog(ERROR, "recno_posting %s: count=%d expected %d",
			 label, RecnoPostingOps.count(buf, len), n);

	/* iterate */
	RecnoPostingOps.iter_begin(buf, len, &it);
	for (i = 0; i < n; i++)
	{
		if (!RecnoPostingOps.iter_next(&it, &got))
			elog(ERROR, "recno_posting %s: iter ended early at %d", label, i);
		if (recno_rowid_compare(got.data, in[i].data) != 0)
			elog(ERROR, "recno_posting %s: iter[%d] mismatch", label, i);
	}
	if (RecnoPostingOps.iter_next(&it, &got))
		elog(ERROR, "recno_posting %s: iter yielded extra", label);

	/* decode_n random access */
	for (i = 0; i < n; i++)
	{
		RecnoPostingOps.decode_n(buf, len, i, &got);
		if (recno_rowid_compare(got.data, in[i].data) != 0)
			elog(ERROR, "recno_posting %s: decode_n[%d] mismatch", label, i);
	}
}

PG_FUNCTION_INFO_V1(test_recno_posting);
Datum
test_recno_posting(PG_FUNCTION_ARGS)
{
	RowID		rids[512];
	int			i;

	/* 1. SINGLE_GEN dense run */
	for (i = 0; i < 100; i++)
		mk_rowid(&rids[i], 1, (OffsetNumber) (i + 1), 7);
	check_roundtrip(rids, 100, "single-gen-dense");

	/* 2. SINGLE_GEN sparse (multi-block) run */
	for (i = 0; i < 100; i++)
		mk_rowid(&rids[i], (BlockNumber) (i * 13), (OffsetNumber) ((i % 200) + 1), 42);
	check_roundtrip(rids, 100, "single-gen-sparse");

	/* 3. MIXED_GEN: same TIDs, alternating gens (A->B->A recurrence shape) */
	{
		int			k = 0;

		for (i = 0; i < 40; i++)
		{
			mk_rowid(&rids[k++], 5, (OffsetNumber) (i + 1), 1);
			mk_rowid(&rids[k++], 5, (OffsetNumber) (i + 1), 2);
		}
		/* rids must be ascending by (TID,gen): same TID, gen1 then gen2 -- OK */
		check_roundtrip(rids, k, "mixed-gen");
	}

	/* 4. remove: drop half of a single-gen run, re-iterate survivors */
	{
		uint8		buf[BLCKSZ];
		uint8		buf2[BLCKSZ];
		Size		len,
					len2;
		RowID		dead[50];
		int			nd = 0;
		RowIDPostIter it;
		RowID		got;
		int			expect;

		for (i = 0; i < 100; i++)
			mk_rowid(&rids[i], 2, (OffsetNumber) (i + 1), 9);
		len = RecnoPostingOps.encode(rids, 100, buf, sizeof(buf));
		if (len > 0)
		{
			/* dead = every even index (ascending) */
			for (i = 0; i < 100; i += 2)
				dead[nd++] = rids[i];
			len2 = RecnoPostingOps.remove(buf, len, dead, nd, buf2, sizeof(buf2));
			if (len2 == 0)
				elog(ERROR, "recno_posting remove: unexpected fallback");
			expect = 100 - nd;
			if (RecnoPostingOps.count(buf2, len2) != expect)
				elog(ERROR, "recno_posting remove: survivors=%d expected %d",
					 RecnoPostingOps.count(buf2, len2), expect);
			/* survivors must be exactly the odd-index rowids, ascending */
			RecnoPostingOps.iter_begin(buf2, len2, &it);
			for (i = 1; i < 100; i += 2)
			{
				if (!RecnoPostingOps.iter_next(&it, &got))
					elog(ERROR, "recno_posting remove: iter short");
				if (recno_rowid_compare(got.data, rids[i].data) != 0)
					elog(ERROR, "recno_posting remove: survivor[%d] mismatch", i);
			}
		}
	}

	/* 5. fuzz: random ascending single-gen runs of varying size */
	for (int t = 0; t < 200; t++)
	{
		int			n = 2 + (int) (random() % 300);
		BlockNumber blk = 0;
		OffsetNumber off = 0;
		uint32		gen = (uint32) (random() % 5);

		for (i = 0; i < n; i++)
		{
			/* strictly ascending TID */
			off++;
			if (off > 200)
			{
				off = 1;
				blk++;
			}
			mk_rowid(&rids[i], blk, off, gen);
		}
		check_roundtrip(rids, n, "fuzz");
	}

	PG_RETURN_VOID();
}
