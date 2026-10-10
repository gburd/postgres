/*-------------------------------------------------------------------------
 *
 * barkdelete.c
 *	  Bottom-up deletion of BARK leaf entries, the re-forming of LIST and
 *	  POSTING entries that it shares with VACUUM, and the merging of a
 *	  leaf's equal-key entries before a split.
 *
 * Bottom-up deletion is nbtree's (_bt_bottomupdel_pass, nbtdedup.c): when an
 * UPDATE that did not change an index's key would split a leaf, ask the table
 * AM which of the leaf's heap TIDs point to versions dead to every snapshot,
 * and delete their entries, so that version churn does not grow the index.
 * See "Bottom-up deletion" in src/backend/access/bark/README.  The merge is
 * nbtree's deduplication pass (_bt_dedup_pass, nbtdedup.c); see "Merging a
 * page's entries before a split" there.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/bark/barkdelete.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bark.h"
#include "access/barkxlog.h"
#include "access/tableam.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"

/*
 * Re-form the LIST or POSTING entry `itup` at `off` on the leaf `buf` (as
 * read through BarkPageGetItem) with `tids`, the `nlive` heap TIDs of it that
 * survive, ascending, 1 <= nlive < its member count.  The shape is chosen
 * again: a POSTING if its sbm still wins, else a LIST, else a plain SINGLE
 * when exactly one survives.  Returns the new entry coded for the page,
 * palloc'd, for the caller to write with PageIndexTupleOverwrite.
 *
 * The new entry is never larger than the old one, so it always fits in the
 * old one's place.  A POSTING entry is sized for the removal bound of its set
 * (see bark_form_posting), which no subset exceeds; a LIST of fewer locators
 * is shorter; a LIST is chosen over the POSTING only when it is the smaller;
 * and a SINGLE is smaller than either.  On a BARK_PREFIX page the key codes
 * as the old entry's did, so the coded entry shrinks with the plain one.
 * VACUUM and bottom-up deletion both rely on this; the check below turns a
 * violation into an error before the caller enters its critical section.
 */
IndexTuple
bark_reform_entry(Relation index, Buffer buf, OffsetNumber off,
				  IndexTuple itup, ItemPointer tids, int nlive)
{
	Page		page = BufferGetPage(buf);
	ItemId		iid = PageGetItemId(page, off);
	IndexTuple	key = bark_single_from_list(index, itup, NULL);
	IndexTuple	newentry;
	IndexTuple	posting;
	IndexTuple	coded;

	Assert(nlive > 0);

	if (nlive == 1)
	{
		newentry = key;
		newentry->t_tid = tids[0];
	}
	else if ((posting = bark_form_posting(RelationGetDescr(index),
										  key, tids, nlive)) != NULL)
	{
		newentry = posting;
		pfree(key);
	}
	else
	{
		newentry = bark_form_list(RelationGetDescr(index), key, tids, nlive);
		pfree(key);
	}

	coded = bark_prefix_encode(page, newentry);
	if (coded != newentry)
		pfree(newentry);
	if (MAXALIGN(IndexTupleSize(coded)) > MAXALIGN(ItemIdGetLength(iid)))
		elog(ERROR, "failed to shrink BARK leaf entry: entry at offset %u of block %u grew from %u to %zu bytes",
			 off, BufferGetBlockNumber(buf), ItemIdGetLength(iid),
			 IndexTupleSize(coded));
	return coded;
}

/*
 * Delete the entries at deletable[] from the exclusive-locked leaf `buf`, and
 * overwrite those at updatedoffsets[] with updated[] (coded for the page), as
 * nbtree's _bt_delitems_delete: the changes and their WAL record are made in
 * one critical section.  Unlike bark_delitems_vacuum, this leaves the page's
 * vacuum cycle ID alone: only VACUUM manages it.
 *
 * For bottom-up deletion the entries are re-formed by bark_reform_entry and
 * never grow, so they are rewritten first, while updatedoffsets[] still name
 * them, and the record is an XLOG_BARK_DELETE with the conflict horizon.  For
 * a merge (`merge`; bark_merge_page) each rewritten entry absorbs the ones
 * deleted after it and grows, which on a full page fits only once their
 * space is free; so the deletions come first, updatedoffsets[] are numbered
 * as after them, and the record is an XLOG_BARK_MERGE.
 */
static void
bark_delitems_delete(Relation index, Buffer buf, bool merge,
					 TransactionId snapshotConflictHorizon, bool isCatalogRel,
					 OffsetNumber *deletable, int ndeletable,
					 OffsetNumber *updatedoffsets, IndexTuple *updated,
					 int nupdated)
{
	Page		page = BufferGetPage(buf);
	bool		needswal = RelationNeedsWAL(index);
	char	   *updatedbuf = NULL;
	Size		updatedbuflen = 0;
	XLogRecPtr	recptr;

	Assert(ndeletable > 0 || nupdated > 0);

	/* The new entries, each padded to MAXALIGN, for the WAL record */
	if (needswal && nupdated > 0)
	{
		for (int i = 0; i < nupdated; i++)
			updatedbuflen += MAXALIGN(IndexTupleSize(updated[i]));
		updatedbuf = palloc0(updatedbuflen);
		updatedbuflen = 0;
		for (int i = 0; i < nupdated; i++)
		{
			memcpy(updatedbuf + updatedbuflen, updated[i],
				   IndexTupleSize(updated[i]));
			updatedbuflen += MAXALIGN(IndexTupleSize(updated[i]));
		}
	}

	/* No ereport(ERROR) until changes are logged */
	START_CRIT_SECTION();

	if (merge && ndeletable > 0)
		PageIndexMultiDelete(page, deletable, ndeletable);
	for (int i = 0; i < nupdated; i++)
	{
		if (!PageIndexTupleOverwrite(page, updatedoffsets[i], updated[i],
									 IndexTupleSize(updated[i])))
			elog(PANIC, "failed to rewrite BARK leaf entry at offset %u of block %u of index \"%s\"",
				 updatedoffsets[i], BufferGetBlockNumber(buf),
				 RelationGetRelationName(index));
	}
	if (!merge && ndeletable > 0)
		PageIndexMultiDelete(page, deletable, ndeletable);

	MarkBufferDirty(buf);

	if (needswal)
	{
		xl_bark_delete xlrec;
		xl_bark_merge xlmerge;

		XLogBeginInsert();
		XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
		if (merge)
		{
			xlmerge.ndeleted = ndeletable;
			xlmerge.nupdated = nupdated;
			XLogRegisterData(&xlmerge, SizeOfBarkMerge);
		}
		else
		{
			xlrec.snapshotConflictHorizon = snapshotConflictHorizon;
			xlrec.ndeleted = ndeletable;
			xlrec.nupdated = nupdated;
			xlrec.isCatalogRel = isCatalogRel;
			XLogRegisterData(&xlrec, SizeOfBarkDelete);
		}
		if (ndeletable > 0)
			XLogRegisterBufData(0, deletable,
								ndeletable * sizeof(OffsetNumber));
		if (nupdated > 0)
		{
			XLogRegisterBufData(0, updatedoffsets,
								nupdated * sizeof(OffsetNumber));
			XLogRegisterBufData(0, updatedbuf, updatedbuflen);
		}

		recptr = XLogInsert(RM_BARK_ID,
							merge ? XLOG_BARK_MERGE : XLOG_BARK_DELETE);
	}
	else
		recptr = XLogGetFakeLSN(index);

	PageSetLSN(page, recptr);

	END_CRIT_SECTION();

	if (updatedbuf != NULL)
		pfree(updatedbuf);
}

/* qsort comparator: TM_IndexDelete by TID, then by id (stable order). */
static int
bark_deltid_cmp(const void *a, const void *b)
{
	const TM_IndexDelete *da = (const TM_IndexDelete *) a;
	const TM_IndexDelete *db = (const TM_IndexDelete *) b;
	int			c = ItemPointerCompare(&da->tid, &db->tid);

	if (c != 0)
		return c;
	return (da->id > db->id) - (da->id < db->id);
}

/*
 * Bottom-up deletion pass over the exclusive-locked leaf `buf`, as nbtree's
 * _bt_bottomupdel_pass and _bt_delitems_delete_check.  bark_insert calls this
 * when `newitem`, an entry the executor says is a new version of a row whose
 * key in this index did not change (indexUnchanged), would split the leaf.
 * Every heap TID on the page is offered to the table AM, which visits a few
 * heap blocks, reports the TIDs whose whole HOT chains are dead to every
 * snapshot, and returns their conflict horizon; their entries are deleted,
 * or, for a LIST or POSTING that keeps some members, re-formed.  Returns true
 * if the page now has room for an entry of `newitemsz` bytes (its coded size)
 * and its line pointer.
 *
 * The TIDs the table AM is told are promising, the ones to look at first, are
 * those of entries whose key equals newitem's, and every LIST and POSTING
 * member: these are duplicates, and so most likely old versions.  Each TID's
 * freespace, the room its deletion gives back, is a SINGLE entry's whole
 * size, a LIST member's locator, or a POSTING member's share of the entry;
 * the table AM uses it only to judge when it has found enough.
 *
 * OVERSIZED entries are not offered: deleting one must also free its overflow
 * chain, which VACUUM does once the leaf no longer references it
 * (bark_free_oversized), and this runs inside an insert that holds the leaf
 * locked.  At most PG_INT16_MAX TIDs are offered (TM_IndexDelete.id is an
 * int16; one POSTING entry can hold tens of thousands); entries past that
 * are left for VACUUM.
 *
 * No cleanup lock is needed, as in nbtree's simple and bottom-up deletion:
 * the heap tuples are dead to every snapshot, and their line pointers stay
 * allocated until VACUUM, which takes a cleanup lock on every leaf before the
 * heap may reuse them, so a scan that copied one of these TIDs before the
 * deletion only finds a dead tuple at it.
 */
bool
bark_bottomup_delete(Relation index, Relation heapRel, BarkKeyInfo *keyinfo,
					 Buffer buf, IndexTuple newitem, Size newitemsz)
{
	Page		page = BufferGetPage(buf);
	BarkPageOpaque opaque = BarkPageGetOpaque(page);
	OffsetNumber minoff = BarkPageFirstDataKey(opaque);
	OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
	TM_IndexDeleteOp delstate;
	ItemPointer tids;			/* every candidate TID, indexed by id */
	int		   *dupof;			/* per id: the id offered for its TID, or -1 */
	int			ntids = 0;
	int			tidsalloc = MaxIndexTuplesPerPage;
	int		   *firstid;		/* per offset: id of its first TID */
	int		   *nids;			/* per offset: number of its TIDs offered */
	bool	   *dead;			/* per id: reported deletable */
	TransactionId snapshotConflictHorizon;
	bool		isCatalogRel;
	OffsetNumber deletable[MaxIndexTuplesPerPage];
	int			ndeletable = 0;
	OffsetNumber updatedoffsets[MaxIndexTuplesPerPage];
	IndexTuple	updated[MaxIndexTuplesPerPage];
	int			nupdated = 0;

	Assert(BarkPageIsLeaf(opaque));

	tids = palloc_array(ItemPointerData, tidsalloc);
	firstid = palloc0_array(int, maxoff + 1);
	nids = palloc0_array(int, maxoff + 1);

	delstate.irel = index;
	delstate.iblknum = BufferGetBlockNumber(buf);
	delstate.bottomup = true;
	delstate.bottomupfreespace = Max(BLCKSZ / 16, newitemsz);

	/* Collect the TIDs, entry by entry */
	for (OffsetNumber off = minoff; off <= maxoff; off = OffsetNumberNext(off))
	{
		BarkItemBuf ibuf;
		IndexTuple	itup = BarkPageGetItem(page, off, &ibuf);
		int			n;

		/* A marker's TID names no row to offer the table AM. */
		if (BarkEntryGetShape(itup) == BARK_SHAPE_OVERSIZED ||
			BarkEntryIsMarker(itup))
			continue;
		n = bark_entry_count_tids(itup);
		if (ntids + n > PG_INT16_MAX)
			continue;
		while (ntids + n > tidsalloc)
		{
			tidsalloc *= 2;
			tids = repalloc_array(tids, ItemPointerData, tidsalloc);
		}
		firstid[off] = ntids;
		nids[off] = bark_entry_get_tids(itup, tids + ntids, n);
		ntids += nids[off];
	}

	if (ntids == 0)
	{
		pfree(tids);
		pfree(firstid);
		pfree(nids);
		return false;
	}

	delstate.deltids = palloc_array(TM_IndexDelete, ntids);
	delstate.status = palloc_array(TM_IndexStatus, ntids);
	for (OffsetNumber off = minoff; off <= maxoff; off = OffsetNumberNext(off))
	{
		BarkItemBuf ibuf;
		IndexTuple	itup;
		BarkEntryShape shape;
		bool		promising;
		int16		freespace;

		if (nids[off] == 0)
			continue;
		itup = BarkPageGetItem(page, off, &ibuf);
		shape = BarkEntryGetShape(itup);
		if (shape == BARK_SHAPE_SINGLE)
		{
			promising = bark_compare_itups(keyinfo, index, newitem, itup) == 0;
			freespace = MAXALIGN(ItemIdGetLength(PageGetItemId(page, off))) +
				sizeof(ItemIdData);
		}
		else if (shape == BARK_SHAPE_LIST)
		{
			promising = true;
			freespace = sizeof(ItemPointerData);
		}
		else
		{
			promising = true;
			freespace = Max(1, ItemIdGetLength(PageGetItemId(page, off)) /
							nids[off]);
		}

		for (int i = firstid[off]; i < firstid[off] + nids[off]; i++)
		{
			delstate.deltids[i].tid = tids[i];
			delstate.deltids[i].id = i;
			delstate.status[i].idxoffnum = off;
			delstate.status[i].knowndeletable = false;
			delstate.status[i].promising = promising;
			delstate.status[i].freespace = freespace;
		}
	}
	delstate.ndeltids = ntids;

	/*
	 * The table AM requires each TID at most once (heapam's sort asserts
	 * that no two are equal), but a row of an index with an extracted column
	 * has an entry per key, and several of them may be on this leaf.  Offer
	 * each TID once: sort by TID, keep the first of each run, and remember
	 * the others in dupof[] so that the answer for the one offered applies to
	 * all.  A TID offered more than once is promising if any of its entries
	 * is, and frees the space of all of them.
	 */
	dupof = palloc_array(int, ntids);
	for (int i = 0; i < ntids; i++)
		dupof[i] = -1;
	if (bark_index_extracted_column(index) > 0 && ntids > 1)
	{
		int			nkept = 0;

		qsort(delstate.deltids, ntids, sizeof(TM_IndexDelete),
			  bark_deltid_cmp);
		for (int i = 0; i < ntids; i++)
		{
			TM_IndexDelete *d = &delstate.deltids[i];

			if (nkept > 0 &&
				ItemPointerEquals(&delstate.deltids[nkept - 1].tid, &d->tid))
			{
				TM_IndexStatus *keep = &delstate.status[delstate.deltids[nkept - 1].id];
				TM_IndexStatus *dup = &delstate.status[d->id];

				dupof[d->id] = delstate.deltids[nkept - 1].id;
				keep->promising = keep->promising || dup->promising;
				keep->freespace = Min(PG_INT16_MAX,
									  (int) keep->freespace + dup->freespace);
				continue;
			}
			delstate.deltids[nkept++] = *d;
		}
		delstate.ndeltids = nkept;
	}

	/*
	 * Ask the table AM.  It sorts and shrinks deltids, so read its answer
	 * back through the ids, which are the indexes into tids[].
	 */
	snapshotConflictHorizon = table_index_delete_tuples(heapRel, &delstate);
	isCatalogRel = RelationIsAccessibleInLogicalDecoding(heapRel);
	if (!XLogStandbyInfoActive())
		snapshotConflictHorizon = InvalidTransactionId;

	dead = palloc0_array(bool, ntids);
	for (int i = 0; i < delstate.ndeltids; i++)
	{
		if (delstate.status[delstate.deltids[i].id].knowndeletable)
			dead[delstate.deltids[i].id] = true;
	}
	for (int i = 0; i < ntids; i++)
	{
		if (dupof[i] >= 0)
			dead[i] = dead[dupof[i]];
	}

	/* Decide each entry's fate, then make the changes in one record */
	for (OffsetNumber off = minoff; off <= maxoff; off = OffsetNumberNext(off))
	{
		int			nlive = 0;

		for (int i = firstid[off]; i < firstid[off] + nids[off]; i++)
		{
			if (!dead[i])
				tids[firstid[off] + nlive++] = tids[i];
		}
		if (nlive == nids[off])
			continue;			/* nothing deletable (or not offered) */
		if (nlive == 0)
			deletable[ndeletable++] = off;
		else
		{
			BarkItemBuf ibuf;
			IndexTuple	itup = BarkPageGetItem(page, off, &ibuf);

			updatedoffsets[nupdated] = off;
			updated[nupdated++] = bark_reform_entry(index, buf, off, itup,
													tids + firstid[off], nlive);
		}
	}

	if (ndeletable > 0 || nupdated > 0)
		bark_delitems_delete(index, buf, false, snapshotConflictHorizon,
							 isCatalogRel, deletable, ndeletable,
							 updatedoffsets, updated, nupdated);

	for (int i = 0; i < nupdated; i++)
		pfree(updated[i]);
	pfree(dead);
	pfree(dupof);
	pfree(delstate.deltids);
	pfree(delstate.status);
	pfree(tids);
	pfree(firstid);
	pfree(nids);

	return bark_leaf_free_space(page) >= newitemsz;
}

/*
 * The entry of `key` holding the ascending heap TIDs tids[0..n), coded for
 * `page`, or NULL when it would exceed the item ceiling (plain or coded).
 */
static IndexTuple
bark_merge_form(Page page, IndexTuple key, ItemPointer tids, int n)
{
	IndexTuple	entry = bark_form_entry(key, tids, n);
	IndexTuple	coded;

	if (entry == NULL)
		return NULL;
	coded = bark_prefix_encode(page, entry);
	if (coded != entry)
		pfree(entry);
	if (MAXALIGN(IndexTupleSize(coded)) > BarkMaxItemSize)
	{
		pfree(coded);
		return NULL;
	}
	return coded;
}

/*
 * Merge runs of adjacent equal-key entries on the exclusive-locked leaf `buf`
 * into fewer, larger LIST or POSTING entries, as nbtree's _bt_dedup_pass
 * does in the same place: bark_insert calls this when the leaf would
 * otherwise split, because a new entry of `newitemsz` bytes (its coded size)
 * to go at offset `newitemoff` does not fit, or because a new heap TID falls
 * inside an entry that cannot take it and dividing that entry needs
 * `newitemsz` more bytes (`newitemoff` is then InvalidOffsetNumber: the TID
 * goes into an existing entry, which may itself be merged).  Returns true if
 * the page now has room for `newitemsz` bytes and a line pointer
 * (bark_leaf_free_space).  Only for an index whose entries coalesce (see
 * bark_allequalimage).
 *
 * Entries are taken in page order.  A group starts at an entry and absorbs
 * the next while that one has the same key and the merged entry, in the
 * shape bark_form_entry chooses by size, stays within BarkMaxItemSize; the
 * entry that does not fit starts the next group.  Equal-key entries are in
 * heap TID order with disjoint ranges, so the members of a group, read in
 * order, are already ascending.  SINGLE entries fold in like the rest;
 * OVERSIZED ones and markers are left alone, and end a group.  A group never spans
 * newitemoff: the merged entry's TID range would then hold the new TID, and
 * the new entry could not go next to it.  A group that would save nothing (a
 * single entry, or alignment eating the gain) is left as it is.  The pass
 * stops after the group that frees enough for the new entry, so a page is
 * not rewritten further than the insert needs.
 *
 * The first entry of each group is overwritten with the merged entry and the
 * rest are deleted, in one XLOG_BARK_MERGE record (bark_delitems_delete).  No
 * heap TID leaves the page and no entry moves to another page, so nothing a
 * scan or VACUUM relies on changes: a scan copies the items it returns while
 * it holds the page's share lock (as nbtree's _bt_readpage does, which its
 * deduplication pass relies on too), and positions itself afterwards by key
 * and heap TID, never by offset; VACUUM's cycle IDs only track entries that
 * move right.  So, as for nbtree, an exclusive lock suffices here and in
 * redo, and there is no recovery conflict.
 */
bool
bark_merge_page(Relation index, BarkKeyInfo *keyinfo, Buffer buf,
				OffsetNumber newitemoff, Size newitemsz)
{
	Page		page = BufferGetPage(buf);
	BarkPageOpaque opaque = BarkPageGetOpaque(page);
	OffsetNumber minoff = BarkPageFirstDataKey(opaque);
	OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
	int			need;
	int			saved = 0;
	ItemPointer tids;
	int			tidsalloc = MaxIndexTuplesPerPage;
	OffsetNumber deletable[MaxIndexTuplesPerPage];
	int			ndeletable = 0;
	OffsetNumber updatedoffsets[MaxIndexTuplesPerPage];
	IndexTuple	updated[MaxIndexTuplesPerPage];
	int			nupdated = 0;
	OffsetNumber off = minoff;

	Assert(BarkPageIsLeaf(opaque));

	/* The bytes bark_leaf_free_space must gain to hold the new entry */
	need = (int) newitemsz + (int) MAXALIGN(sizeof(ItemPointerData)) -
		(int) PageGetFreeSpace(page);
	tids = palloc_array(ItemPointerData, tidsalloc);

	while (off <= maxoff && saved < need)
	{
		BarkItemBuf ibuf;
		BarkItemBuf nbuf;
		IndexTuple	itup = BarkPageGetItem(page, off, &ibuf);
		IndexTuple	next;
		OffsetNumber first = off;
		IndexTuple	key;
		IndexTuple	merged = NULL;
		int			ntids;
		int			groupsz;

		off = OffsetNumberNext(off);
		if (BarkEntryGetShape(itup) == BARK_SHAPE_OVERSIZED ||
			BarkEntryIsMarker(itup))
			continue;

		/*
		 * Most entries start no group: test the next one before forming a
		 * key and reading the members.
		 */
		if (off > maxoff || off == newitemoff)
			continue;
		next = BarkPageGetItem(page, off, &nbuf);
		if (BarkEntryGetShape(next) == BARK_SHAPE_OVERSIZED ||
			BarkEntryIsMarker(next) ||
			bark_compare_itups(keyinfo, index, itup, next) != 0)
			continue;

		key = bark_single_from_list(index, itup, NULL);
		ntids = bark_entry_count_tids(itup);
		if (ntids > tidsalloc)
		{
			while (ntids > tidsalloc)
				tidsalloc *= 2;
			tids = repalloc_array(tids, ItemPointerData, tidsalloc);
		}
		ntids = bark_entry_get_tids(itup, tids, ntids);
		groupsz = MAXALIGN(ItemIdGetLength(PageGetItemId(page, first)));

		/* Absorb the following entries of the key while the result fits */
		for (; off <= maxoff && off != newitemoff; off = OffsetNumberNext(off))
		{
			IndexTuple	cur = BarkPageGetItem(page, off, &ibuf);
			int			n;
			IndexTuple	candidate;

			if (BarkEntryGetShape(cur) == BARK_SHAPE_OVERSIZED ||
				BarkEntryIsMarker(cur) ||
				bark_compare_itups(keyinfo, index, key, cur) != 0)
				break;
			n = bark_entry_count_tids(cur);
			if (ntids + n > tidsalloc)
			{
				while (ntids + n > tidsalloc)
					tidsalloc *= 2;
				tids = repalloc_array(tids, ItemPointerData, tidsalloc);
			}
			n = bark_entry_get_tids(cur, tids + ntids, n);
			Assert(ItemPointerCompare(&tids[ntids - 1], &tids[ntids]) < 0);

			candidate = bark_merge_form(page, key, tids, ntids + n);
			if (candidate == NULL)
				break;
			if (merged != NULL)
				pfree(merged);
			merged = candidate;
			ntids += n;
			groupsz += MAXALIGN(ItemIdGetLength(PageGetItemId(page, off))) +
				sizeof(ItemIdData);
		}
		pfree(key);

		if (merged == NULL)
			continue;
		if (groupsz <= (int) MAXALIGN(IndexTupleSize(merged)))
		{
			pfree(merged);
			continue;
		}

		/* first is renumbered by the deletions before it */
		updatedoffsets[nupdated] = first - ndeletable;
		updated[nupdated++] = merged;
		for (OffsetNumber d = OffsetNumberNext(first); d < off;
			 d = OffsetNumberNext(d))
			deletable[ndeletable++] = d;
		saved += groupsz - (int) MAXALIGN(IndexTupleSize(merged));
	}

	if (nupdated > 0)
		bark_delitems_delete(index, buf, true, InvalidTransactionId, false,
							 deletable, ndeletable, updatedoffsets, updated,
							 nupdated);

	for (int i = 0; i < nupdated; i++)
		pfree(updated[i]);
	pfree(tids);

	return bark_leaf_free_space(page) >= newitemsz;
}
