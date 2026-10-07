/*-------------------------------------------------------------------------
 *
 * barkdelete.c
 *	  Bottom-up deletion of BARK leaf entries, and the re-forming of LIST
 *	  and POSTING entries that it shares with VACUUM.
 *
 * Bottom-up deletion is nbtree's (_bt_bottomupdel_pass, nbtdedup.c): when an
 * UPDATE that did not change an index's key would split a leaf, ask the table
 * AM which of the leaf's heap TIDs point to versions dead to every snapshot,
 * and delete their entries, so that version churn does not grow the index.
 * See "Bottom-up deletion" in src/backend/access/bark/README.
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
 * overwrite those at updatedoffsets[] with updated[] (re-formed by
 * bark_reform_entry, coded for the page), as nbtree's _bt_delitems_delete:
 * the changes and their XLOG_BARK_DELETE record are made in one critical
 * section, rewrites first, since PageIndexTupleOverwrite keeps offsets
 * stable and the deletion renumbers them.  Unlike bark_delitems_vacuum, this
 * leaves the page's vacuum cycle ID alone: only VACUUM manages it.
 */
static void
bark_delitems_delete(Relation index, Buffer buf,
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

	for (int i = 0; i < nupdated; i++)
	{
		if (!PageIndexTupleOverwrite(page, updatedoffsets[i], updated[i],
									 IndexTupleSize(updated[i])))
			elog(PANIC, "failed to rewrite BARK leaf entry at offset %u of block %u of index \"%s\"",
				 updatedoffsets[i], BufferGetBlockNumber(buf),
				 RelationGetRelationName(index));
	}
	if (ndeletable > 0)
		PageIndexMultiDelete(page, deletable, ndeletable);

	MarkBufferDirty(buf);

	if (needswal)
	{
		xl_bark_delete xlrec;

		xlrec.snapshotConflictHorizon = snapshotConflictHorizon;
		xlrec.ndeleted = ndeletable;
		xlrec.nupdated = nupdated;
		xlrec.isCatalogRel = isCatalogRel;

		XLogBeginInsert();
		XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
		XLogRegisterData(&xlrec, SizeOfBarkDelete);
		if (ndeletable > 0)
			XLogRegisterBufData(0, deletable,
								ndeletable * sizeof(OffsetNumber));
		if (nupdated > 0)
		{
			XLogRegisterBufData(0, updatedoffsets,
								nupdated * sizeof(OffsetNumber));
			XLogRegisterBufData(0, updatedbuf, updatedbuflen);
		}

		recptr = XLogInsert(RM_BARK_ID, XLOG_BARK_DELETE);
	}
	else
		recptr = XLogGetFakeLSN(index);

	PageSetLSN(page, recptr);

	END_CRIT_SECTION();

	if (updatedbuf != NULL)
		pfree(updatedbuf);
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

		if (BarkEntryGetShape(itup) == BARK_SHAPE_OVERSIZED)
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
		bark_delitems_delete(index, buf, snapshotConflictHorizon, isCatalogRel,
							 deletable, ndeletable, updatedoffsets, updated,
							 nupdated);

	for (int i = 0; i < nupdated; i++)
		pfree(updated[i]);
	pfree(dead);
	pfree(delstate.deltids);
	pfree(delstate.status);
	pfree(tids);
	pfree(firstid);
	pfree(nids);

	return bark_leaf_free_space(page) >= newitemsz;
}
