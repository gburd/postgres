/*
 * noxu_attpage.c
 *		Routines for handling attribute leaf pages.
 *
 * A Noxu table consists of multiple B-trees, one for each attribute. The
 * functions in this file deal with a scan of one attribute tree.
 *
 * Operations:
 *
 * - Sequential scan in TID order
 *  - must be efficient with scanning multiple trees in sync
 *
 * - random lookups, by TID (for index scan)
 *
 * - range scans by TID (for bitmap index scan)
 *
 * NOTES:
 * - Locking order: child before parent, left before right
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/noxu/noxu_attpage.c
 */
#include "postgres.h"

#include "access/noxu_compression.h"
#include "access/noxu_internal.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/datum.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/* prototypes for local functions */
static void nxbt_attr_repack_replace(Relation rel, AttrNumber attno,
									 Buffer oldbuf, List *items);
static void nxbt_attr_add_items(Relation rel, AttrNumber attno, Buffer buf,
								List *newitems);

/* ----------------------------------------------------------------
 *						 Public interface
 * ----------------------------------------------------------------
 */

/*
 * Begin a scan of an attribute btree.
 *
 * Fills in the scan struct in *scan.
 */
void
nxbt_attr_begin_scan(Relation rel, TupleDesc tdesc, AttrNumber attno,
					 NXAttrTreeScan * scan)
{
	scan->rel = rel;
	scan->attno = attno;
	scan->attdesc = TupleDescAttr(tdesc, attno - 1);

	scan->context = CurrentMemoryContext;
	scan->array_datums = MemoryContextAlloc(scan->context, sizeof(Datum));
	scan->array_isnulls = MemoryContextAlloc(scan->context, sizeof(bool) + 7);
	scan->array_tids = MemoryContextAlloc(scan->context, sizeof(nxtid));
	scan->array_datums_allocated_size = 1;
	scan->array_num_elements = 0;
	scan->array_curr_idx = -1;
	scan->extract_hint_tid = InvalidNXTid;

	scan->decompress_buf = NULL;
	scan->decompress_buf_size = 0;
	scan->attr_buf = NULL;
	scan->attr_buf_size = 0;

	scan->active = true;
	scan->lastbuf = InvalidBuffer;
	scan->lastoff = InvalidOffsetNumber;
}

void
nxbt_attr_end_scan(NXAttrTreeScan * scan)
{
	if (!scan->active)
		return;

	if (scan->lastbuf != InvalidBuffer)
		ReleaseBuffer(scan->lastbuf);

	scan->active = false;
	scan->array_num_elements = 0;
	scan->array_curr_idx = -1;

	if (scan->array_datums)
		pfree(scan->array_datums);
	if (scan->array_isnulls)
		pfree(scan->array_isnulls);
	if (scan->array_tids)
		pfree(scan->array_tids);
	if (scan->decompress_buf)
		pfree(scan->decompress_buf);
	if (scan->attr_buf)
		pfree(scan->attr_buf);
}

/*
 * Fetch the array item whose firsttid-endtid range contains 'nexttid',
 * if any.
 *
 * Return true if an item was found. The Datum/isnull data of are
 * placed into scan->array_* fields. The data is valid until the next
 * call of this function. Note that the item's range contains 'nexttid',
 * but its TID list might not include the exact TID itself. The caller
 * must scan the array to check for that.
 *
 * This is normally not used directly. Use the nxbt_attr_fetch() wrapper,
 * instead.
 */
bool
nxbt_attr_scan_fetch_array(NXAttrTreeScan * scan, nxtid nexttid)
{
	if (!scan->active)
		return InvalidNXTid;

	/*
	 * Find the item containing nexttid.
	 */
	for (;;)
	{
		Buffer		buf;
		Page		page;
		OffsetNumber off;
		OffsetNumber maxoff;

		/*
		 * Find and lock the leaf page containing scan->nexttid.
		 */
		buf = nxbt_find_and_lock_leaf_containing_tid(scan->rel, scan->attno,
													 scan->lastbuf, nexttid,
													 BUFFER_LOCK_SHARE);
		scan->lastbuf = buf;
		if (!BufferIsValid(buf))
		{
			/*
			 * Completely empty tree. This should only happen at the beginning
			 * of a scan - a tree cannot go missing after it's been created -
			 * but we don't currently check for that.
			 */
			break;
		}
		page = BufferGetPage(buf);

		/*
		 * Scan the items on the page, to find the next one that covers
		 * nexttid.
		 *
		 * As an optimization, check the last offset first. During sequential
		 * scans, the next item is usually at the same offset or just after
		 * the one we found last time, so we can avoid scanning from the
		 * beginning of the page.
		 */
		maxoff = PageGetMaxOffsetNumber(page);

		off = FirstOffsetNumber;
		/* Set hint for binary search optimization in extract */
		scan->extract_hint_tid = nexttid;

		if (scan->lastoff >= FirstOffsetNumber && scan->lastoff <= maxoff)
		{
			ItemId		iid = PageGetItemId(page, scan->lastoff);
			NXAttributeArrayItem *item = (NXAttributeArrayItem *) PageGetItem(page, iid);

			if (item->t_firsttid <= nexttid && item->t_endtid > nexttid)
			{
				nxbt_attr_item_extract(scan, item);
				scan->array_curr_idx = -1;

				if (scan->array_num_elements > 0)
				{
					LockBuffer(buf, BUFFER_LOCK_UNLOCK);
					return true;
				}
			}

			/*
			 * The item at lastoff didn't match. Start scanning from
			 * lastoff rather than the beginning, since items before it
			 * are unlikely to match in a forward scan.
			 */
			if (item->t_endtid <= nexttid)
				off = scan->lastoff + 1;
		}

		for (; off <= maxoff; off++)
		{
			ItemId		iid = PageGetItemId(page, off);
			NXAttributeArrayItem *item = (NXAttributeArrayItem *) PageGetItem(page, iid);

			if (item->t_endtid <= nexttid)
				continue;

			if (item->t_firsttid > nexttid)
				break;

			/*
			 * Extract the data into scan->array_* fields.
			 *
			 * NOTE: nxbt_attr_item_extract() always makes a copy of the data,
			 * so we can release the lock on the page after doing this.
			 */
			nxbt_attr_item_extract(scan, item);
			scan->array_curr_idx = -1;
			scan->lastoff = off;

			if (scan->array_num_elements > 0)
			{
				/* Found it! */
				LockBuffer(buf, BUFFER_LOCK_UNLOCK);
				return true;
			}
		}

		/*
		 * No matching items.  Cache the next block pointer so that
		 * sequential scans can avoid a full B-tree descent on the next
		 * call -- we almost certainly need that next page.
		 */
		{
			NXBtreePageOpaque *opaque = NXBtreePageGetOpaque(page);
			BlockNumber nextblk = opaque->nx_next;

			LockBuffer(buf, BUFFER_LOCK_UNLOCK);

			if (nextblk != InvalidBlockNumber)
				scan->lastbuf = ReleaseAndReadBuffer(scan->lastbuf, scan->rel, nextblk);
		}
		return false;
	}

	/* Reached end of scan. */
	scan->array_num_elements = 0;
	scan->array_curr_idx = -1;
	if (BufferIsValid(scan->lastbuf))
		ReleaseBuffer(scan->lastbuf);
	scan->lastbuf = InvalidBuffer;
	return false;
}

/*
 * Insert a multiple items to the given attribute's btree.
 */
void
nxbt_attr_multi_insert(Relation rel, AttrNumber attno,
					   Datum *datums, bool *isnulls, nxtid *tids, int nitems)
{
	Form_pg_attribute attr;
	Buffer		buf;
	nxtid		insert_target_key;
	List	   *newitems;

	Assert(attno >= 1);
	attr = TupleDescAttr(rel->rd_att, attno - 1);

	/*
	 * Find the right place for the given TID.
	 */
	insert_target_key = tids[0];

	/* Create items to insert. */
	newitems = nxbt_attr_create_items(attr, datums, isnulls, tids, nitems);

	/*
	 * Insert the items, handling the case where the target page has been
	 * split by a concurrent backend.
	 *
	 * nxbt_descend() returns the leaf page covering insert_target_key with
	 * an exclusive lock.  However, the items in 'newitems' may span a TID
	 * range wider than what this single page covers (its nx_hikey may be
	 * less than the last item's TID).  This can happen when a concurrent
	 * backend split the page between our TID allocation and now, or simply
	 * because we're inserting a batch that spans multiple pages.
	 *
	 * To handle this correctly, we loop: insert items that fit within the
	 * current page's key range, then descend again for any remaining items
	 * whose TIDs fall beyond the page's nx_hikey.
	 */
	{
		int insert_loop_count = 0;
		BlockNumber last_buf_blkno = InvalidBlockNumber;

		while (newitems != NIL)
		{
			Page		page;
			NXBtreePageOpaque *opaque;
			nxtid		page_hikey;
			NXAttributeArrayItem *lastitem;
			nxtid		last_item_firsttid;

			buf = nxbt_descend(rel, attno, insert_target_key, 0, false, true,
							   InvalidBuffer, InvalidBuffer);
			page = BufferGetPage(buf);
			opaque = NXBtreePageGetOpaque(page);
			page_hikey = opaque->nx_hikey;

			if (BufferGetBlockNumber(buf) == last_buf_blkno)
				insert_loop_count++;
			else
			{
				insert_loop_count = 1;
				last_buf_blkno = BufferGetBlockNumber(buf);
			}

			if (insert_loop_count > 100)
				elog(ERROR, "nxbt_attr_multi_insert: infinite loop detected! Same page (blk=%u) returned %d times. insert_target_key=%lu, page_hikey=%lu",
					 BufferGetBlockNumber(buf), insert_loop_count,
					 (unsigned long) insert_target_key,
					 (unsigned long) page_hikey);

		/*
		 * Check whether all remaining items fit within this page's key
		 * range.  We look at the firsttid of the last item -- if it is
		 * below page_hikey, all items belong here.
		 */
		lastitem = (NXAttributeArrayItem *) llast(newitems);
		last_item_firsttid = lastitem->t_firsttid;

		if (last_item_firsttid < page_hikey)
		{
			/* All items fit on this page.  Insert and we're done. */
			nxbt_attr_add_items(rel, attno, buf, newitems);
			/* nxbt_attr_add_items unlocked 'buf' */
			ReleaseBuffer(buf);
			newitems = NIL;
		}
		else
		{
			/*
			 * Some items have TIDs beyond this page's key range.  Split the
			 * list: items with firsttid < page_hikey go to this page, the
			 * rest will be inserted in a subsequent iteration.
			 */
			List	   *items_this_page = NIL;
			List	   *items_remaining = NIL;
			ListCell   *lc;

			foreach(lc, newitems)
			{
				NXAttributeArrayItem *item = (NXAttributeArrayItem *) lfirst(lc);

				if (item->t_firsttid < page_hikey)
					items_this_page = lappend(items_this_page, item);
				else
					items_remaining = lappend(items_remaining, item);
			}

			if (items_this_page != NIL)
			{
				nxbt_attr_add_items(rel, attno, buf, items_this_page);
				/* nxbt_attr_add_items unlocked 'buf' */
				ReleaseBuffer(buf);
				list_free(items_this_page);
			}
			else
			{
				/*
				 * No items belong on this page at all.  This shouldn't
				 * normally happen since nxbt_descend() found this page for
				 * insert_target_key, but can occur if the page was split
				 * between our descend and the lock acquisition (the descend
				 * retries should handle this, but be defensive).
				 */
				LockBuffer(buf, BUFFER_LOCK_UNLOCK);
				ReleaseBuffer(buf);
			}

			list_free(newitems);
			newitems = items_remaining;

			/*
			 * Update the target key for the next descent to the first TID
			 * of the remaining items.
			 */
			if (newitems != NIL)
			{
				NXAttributeArrayItem *nextitem;

				nextitem = (NXAttributeArrayItem *) linitial(newitems);
				insert_target_key = nextitem->t_firsttid;
			}
		}
		}
	}
}

/*
 * Remove datums for the given TIDs from the attribute tree.
 */
void
nxbt_attr_remove(Relation rel, AttrNumber attno, IntegerSet *tids)
{
	Form_pg_attribute attr;
	Buffer		buf;
	Page		page;
	NXBtreePageOpaque *opaque;
	OffsetNumber maxoff;
	OffsetNumber off;
	List	   *newitems = NIL;
	NXAttributeArrayItem *item;
	NXExplodedItem *newitem;
	nxtid		nexttid;
	MemoryContext oldcontext;
	MemoryContext tmpcontext;

	tmpcontext = AllocSetContextCreate(CurrentMemoryContext,
									   "NoxuAMVacuumContext",
									   ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(tmpcontext);

	attr = TupleDescAttr(rel->rd_att, attno - 1);

	intset_begin_iterate(tids);
	if (!intset_iterate_next(tids, &nexttid))
		nexttid = InvalidNXTid;

	while (nexttid < MaxPlusOneNXTid)
	{
		buf = nxbt_descend(rel, attno, nexttid, 0, false, true, InvalidBuffer, InvalidBuffer);
		page = BufferGetPage(buf);
		opaque = NXBtreePageGetOpaque(page);

		newitems = NIL;

		/*
		 * Find the item containing the first tid to remove.
		 */
		maxoff = PageGetMaxOffsetNumber(page);
		off = FirstOffsetNumber;
		for (;;)
		{
			nxtid		endtid;
			ItemId		iid;
			int			num_to_remove;
			nxtid	   *tids_arr;

			if (off > maxoff)
				break;

			iid = PageGetItemId(page, off);
			item = (NXAttributeArrayItem *) PageGetItem(page, iid);
			off++;

			/*
			 * If we don't find an item containing the given TID, just skip
			 * over it.
			 *
			 * This can legitimately happen, if e.g. VACUUM is interrupted,
			 * after it has already removed the attribute data for the dead
			 * tuples.
			 */
			while (nexttid < item->t_firsttid)
			{
				if (!intset_iterate_next(tids, &nexttid))
					nexttid = MaxPlusOneNXTid;
			}

			/*
			 * If this item doesn't contain any of the items we're removing,
			 * keep it as it is.
			 */
			endtid = item->t_endtid;
			if (endtid < nexttid)
			{
				newitems = lappend(newitems, item);
				continue;
			}

			/*
			 * We now have an array item at hand, that contains at least one
			 * of the TIDs we want to remove. Split the array, removing all
			 * the target tids.
			 */
			tids_arr = palloc((item->t_num_elements + 1) * sizeof(nxtid));
			num_to_remove = 0;
			while (nexttid < endtid)
			{
				tids_arr[num_to_remove++] = nexttid;
				if (!intset_iterate_next(tids, &nexttid))
					nexttid = MaxPlusOneNXTid;
			}
			tids_arr[num_to_remove++] = MaxPlusOneNXTid;
			newitem = nxbt_attr_remove_from_item(attr, item, tids_arr, rel, attno);
			pfree(tids_arr);
			if (newitem)
				newitems = lappend(newitems, newitem);
		}

		/*
		 * Skip over any remaining TIDs in the dead TID list that would be on
		 * this page, but are missing.
		 */
		while (nexttid < opaque->nx_hikey)
		{
			if (!intset_iterate_next(tids, &nexttid))
				nexttid = MaxPlusOneNXTid;
		}

		/* Now pass the list to the recompressor. */
		IncrBufferRefCount(buf);
		if (newitems)
		{
			nxbt_attr_repack_replace(rel, attno, buf, newitems);
		}
		else
		{
			nx_split_stack *stack;

			stack = nxbt_unlink_page(rel, attno, buf, 0);

			if (!stack)
			{
				/* failed. */
				Page		newpage = PageGetTempPageCopySpecial(BufferGetPage(buf));

				stack = nx_new_split_stack_entry(buf, newpage);
			}

			/* apply the changes */
			nx_apply_split_changes(rel, stack, NULL);
		}
		ReleaseBuffer(buf);		/* nxbt_apply_split_changes unlocked 'buf' */

		/*
		 * We can now free the decompression contexts. The pointers in the
		 * 'items' list point to decompression buffers, so we cannot free them
		 * until after writing out the pages.
		 */
		MemoryContextReset(tmpcontext);
	}
	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(tmpcontext);
}

/* ----------------------------------------------------------------
 *						 Internal routines
 * ----------------------------------------------------------------
 */

/*
 * This helper function is used to implement INSERT, UPDATE and DELETE.
 *
 * The items in the 'newitems' list are added to the page, to the correct position.
 *
 * This function handles decompressing and recompressing items, and splitting
 * existing items, or the page, as needed.
 */
static void
nxbt_attr_add_items(Relation rel, AttrNumber attno, Buffer buf, List *newitems)
{
	Form_pg_attribute attr;
	Page		page = BufferGetPage(buf);
	OffsetNumber off;
	OffsetNumber maxoff;
	List	   *items = NIL;
	Size		growth;
	ListCell   *lc;
	ListCell   *nextnewlc;
	nxtid		last_existing_tid;
	NXAttributeArrayItem *olditem;
	NXAttributeArrayItem *newitem;

	attr = TupleDescAttr(rel->rd_att, attno - 1);

	nextnewlc = list_head(newitems);

	Assert(newitems != NIL);

	maxoff = PageGetMaxOffsetNumber(page);

	/*
	 * Quick check if the new items go to the end of the page. This is the
	 * common case, when inserting new rows, since we allocate TIDs in order.
	 */
	if (maxoff == 0)
		last_existing_tid = 0;
	else
	{
		ItemId		iid;
		NXAttributeArrayItem *lastitem;

		iid = PageGetItemId(page, maxoff);
		lastitem = (NXAttributeArrayItem *) PageGetItem(page, iid);

		last_existing_tid = lastitem->t_endtid;
	}

	/*
	 * If the new items go to the end of the page, and they fit without
	 * splitting the page, just add them to the end.
	 */
	if (((NXAttributeArrayItem *) lfirst(nextnewlc))->t_firsttid >= last_existing_tid)
	{
		growth = 0;
		foreach(lc, newitems)
		{
			NXAttributeArrayItem *item = (NXAttributeArrayItem *) lfirst(lc);

			growth += MAXALIGN(item->t_size) + sizeof(ItemId);
		}

		if (growth <= PageGetExactFreeSpace(page))
		{
			/* The new items fit on the page. Add them. */
			OffsetNumber startoff;

			START_CRIT_SECTION();

			startoff = PageGetMaxOffsetNumber(page) + 1;
			off = startoff;
			foreach(lc, newitems)
			{
				NXAttributeArrayItem *item = (NXAttributeArrayItem *) lfirst(lc);

				Assert(item->t_size > 0);

				if (PageAddItemExtended(page,
										item, item->t_size, off,
										PAI_OVERWRITE) == InvalidOffsetNumber)
					elog(ERROR, "could not add item to attribute page");
				off++;
			}

			MarkBufferDirty(buf);

			if (RelationNeedsWAL(rel))
				nxbt_wal_log_leaf_items(rel, attno, buf, startoff, false, newitems, NULL);
			else
			{
				/*
				 * For unlogged relations, we still need to update the page LSN
				 * to ensure proper page consistency checks.
				 */
				PageSetLSN(BufferGetPage(buf), GetXLogInsertRecPtr());
			}

			END_CRIT_SECTION();

			LockBuffer(buf, BUFFER_LOCK_UNLOCK);

			list_free(newitems);

			return;
		}
	}

	/*
	 * Need to recompress and/or split the hard way.
	 *
	 * First, loop through the old and new items in lockstep, to figure out
	 * where the new items go to. If some of the old and new items have
	 * overlapping TID ranges, we will need to split some items to make them
	 * not overlap.
	 */
	off = 1;
	if (off <= maxoff)
	{
		ItemId		iid = PageGetItemId(page, off);

		olditem = (NXAttributeArrayItem *) PageGetItem(page, iid);
		off++;
	}
	else
		olditem = NULL;

	if (nextnewlc)
	{
		newitem = lfirst(nextnewlc);
		nextnewlc = lnext(newitems, nextnewlc);
	}

	for (;;)
	{
		if (!newitem && !olditem)
			break;

		if (newitem && olditem && newitem->t_firsttid == olditem->t_firsttid)
			elog(ERROR, "duplicate TID on attribute page");

		/*
		 * NNNNNNNN OOOOOOOOO
		 */
		if (newitem && (!olditem || newitem->t_endtid <= olditem->t_firsttid))
		{
			items = lappend(items, newitem);
			if (nextnewlc)
			{
				newitem = lfirst(nextnewlc);
				nextnewlc = lnext(newitems, nextnewlc);
			}
			else
				newitem = NULL;
			continue;
		}

		/*
		 * NNNNNNNN OOOOOOOOO
		 */
		if (olditem && (!newitem || olditem->t_endtid <= newitem->t_firsttid))
		{
			items = lappend(items, olditem);
			if (off <= maxoff)
			{
				ItemId		iid = PageGetItemId(page, off);

				olditem = (NXAttributeArrayItem *) PageGetItem(page, iid);
				off++;
			}
			else
				olditem = NULL;
			continue;
		}

		/*
		 * NNNNNNNN OOOOOOOOO
		 */
		if (olditem->t_firsttid > newitem->t_firsttid)
		{
			NXExplodedItem *left_newitem;
			NXExplodedItem *right_newitem;

			/*
			 * split newitem:
			 *
			 * NNNNNnnnn OOOOOOOOO
			 */
			nxbt_split_item(attr, (NXExplodedItem *) newitem, olditem->t_firsttid,
							&left_newitem, &right_newitem, rel, attno);
			items = lappend(items, left_newitem);
			newitem = (NXAttributeArrayItem *) right_newitem;
			continue;
		}

		/*
		 * NNNNNNNN OOOOOOOOO
		 */
		if (olditem->t_firsttid < newitem->t_firsttid)
		{
			NXExplodedItem *left_olditem;
			NXExplodedItem *right_olditem;

			/*
			 * split olditem:
			 *
			 * OOOOOoooo NNNNNNNNN
			 */
			nxbt_split_item(attr, (NXExplodedItem *) olditem, newitem->t_firsttid,
							&left_olditem, &right_olditem, rel, attno);
			items = lappend(items, left_olditem);
			olditem = (NXAttributeArrayItem *) right_olditem;
			continue;
		}

		elog(ERROR, "shouldn't reach here");
	}

	/* Now pass the list to the repacker, to distribute the items to pages. */
	IncrBufferRefCount(buf);

	/*
	 * Now we have a list of non-overlapping items, containing all the old and
	 * new data. nxbt_attr_repack_replace() takes care of storing them on the
	 * page, splitting the page if needed.
	 */
	nxbt_attr_repack_replace(rel, attno, buf, items);

	list_free(items);
}


/*
 * Repacker routines
 */
typedef struct
{
	Page		currpage;
	int			compressed_items;

	/*
	 * first page writes over the old buffer, subsequent pages get
	 * newly-allocated buffers
	 */
	nx_split_stack *stack_head;
	nx_split_stack *stack_tail;

	int			total_items;
	int			total_packed_items;

	AttrNumber	attno;
	nxtid		hikey;
}			nxbt_attr_repack_context;

static void
nxbt_attr_repack_newpage(nxbt_attr_repack_context * cxt, nxtid nexttid, int flags)
{
	Page		newpage;
	NXBtreePageOpaque *newopaque;
	nx_split_stack *stack;

	if (cxt->currpage)
	{
		/* set the last tid on previous page */
		NXBtreePageOpaque *oldopaque = NXBtreePageGetOpaque(cxt->currpage);

		oldopaque->nx_hikey = nexttid;
	}

	newpage = (Page) palloc(BLCKSZ);
	PageInit(newpage, BLCKSZ, sizeof(NXBtreePageOpaque));

	stack = nx_new_split_stack_entry(InvalidBuffer, /* will be assigned later */
									 newpage);
	if (cxt->stack_tail)
		cxt->stack_tail->next = stack;
	else
		cxt->stack_head = stack;
	cxt->stack_tail = stack;

	cxt->currpage = newpage;

	newopaque = NXBtreePageGetOpaque(newpage);
	newopaque->nx_attno = cxt->attno;
	newopaque->nx_next = InvalidBlockNumber;	/* filled in later */
	newopaque->nx_lokey = nexttid;
	newopaque->nx_hikey = cxt->hikey;	/* overwritten later, if this is not
										 * last page */
	newopaque->nx_level = 0;
	newopaque->nx_flags = flags;
	newopaque->nx_page_id = NX_BTREE_PAGE_ID;
}

/*
 * Rewrite a leaf page, with given 'items' as the new content.
 *
 * First, calls nxbt_attr_recompress_items(), which will try to combine
 * short items, and compress uncompressed items. After that, will try to
 * store all the items on the page, replacing old content on the page.
 *
 * The items may contain "exploded" items, as NXExplodedItem. They will
 * be converted to normal array items suitable for storing on-disk.
 *
 * If the items don't fit on the page, then the page is split. It is
 * entirely possible that they don't fit even on two pages; we split the page
 * into as many pages as needed. Hopefully not more than a few pages, though,
 * because otherwise you might hit limits on the number of buffer pins (with
 * tiny shared_buffers).
 *
 * On entry, 'oldbuf' must be pinned and exclusive-locked. On exit, the lock
 * is released, but it's still pinned.
 */
static void
nxbt_attr_repack_replace(Relation rel, AttrNumber attno, Buffer oldbuf, List *items)
{
	Form_pg_attribute attr = TupleDescAttr(rel->rd_att, attno - 1);
	ListCell   *lc;
	nxbt_attr_repack_context cxt;
	NXBtreePageOpaque *oldopaque = NXBtreePageGetOpaque(BufferGetPage(oldbuf));
	BlockNumber orignextblk;
	nx_split_stack *stack;
	List	   *downlinks = NIL;
	List	   *recompressed_items;

	/*
	 * Check that the items in the input are in correct order and don't
	 * overlap.
	 */
#ifdef USE_ASSERT_CHECKING
	{
		nxtid		prev_endtid = 0;

		foreach(lc, items)
		{
			NXAttributeArrayItem *item = (NXAttributeArrayItem *) lfirst(lc);
			nxtid		item_firsttid;
			nxtid		item_endtid;

			if (item->t_size == 0)
			{
				NXExplodedItem *eitem = (NXExplodedItem *) item;

				item_firsttid = eitem->tids[0];
				item_endtid = eitem->tids[eitem->t_num_elements - 1] + 1;
			}
			else
			{
				item_firsttid = item->t_firsttid;
				item_endtid = item->t_endtid;;
			}

			Assert(item_firsttid >= prev_endtid);
			Assert(item_endtid > item_firsttid);
			prev_endtid = item_endtid;
		}
	}
#endif

	/*
	 * First, split, merge and compress the items as needed, into suitable
	 * chunks.
	 */
	recompressed_items = nxbt_attr_recompress_items(attr, items, rel, attno);

	/*
	 * Then, store them on the page, creating new pages as needed.
	 */
	orignextblk = oldopaque->nx_next;
	Assert(orignextblk != BufferGetBlockNumber(oldbuf));

	cxt.currpage = NULL;
	cxt.stack_head = cxt.stack_tail = NULL;
	cxt.attno = attno;
	cxt.hikey = oldopaque->nx_hikey;

	cxt.total_items = 0;

	nxbt_attr_repack_newpage(&cxt, oldopaque->nx_lokey, (oldopaque->nx_flags & NXBT_ROOT));

	foreach(lc, recompressed_items)
	{
		NXAttributeArrayItem *item = lfirst(lc);

		if (PageGetFreeSpace(cxt.currpage) < MAXALIGN(item->t_size))
			nxbt_attr_repack_newpage(&cxt, item->t_firsttid, 0);

		if (PageAddItemExtended(cxt.currpage,
								item, item->t_size,
								PageGetMaxOffsetNumber(cxt.currpage) + 1,
								PAI_OVERWRITE) == InvalidOffsetNumber)
			elog(ERROR, "could not add item to page while recompressing");

		cxt.total_items++;
	}

	/*
	 * Ok, we now have a list of pages, to replace the original page, as
	 * private in-memory copies. Allocate buffers for them, and write them
	 * out.
	 *
	 * allocate all the pages before entering critical section, so that
	 * out-of-disk-space doesn't lead to PANIC
	 */
	stack = cxt.stack_head;
	Assert(stack->buf == InvalidBuffer);
	stack->buf = oldbuf;
	while (stack->next)
	{
		Page		thispage = stack->page;
		NXBtreePageOpaque *thisopaque = NXBtreePageGetOpaque(thispage);
		NXBtreeInternalPageItem *downlink;
		Buffer		nextbuf;

		Assert(stack->next->buf == InvalidBuffer);

		nextbuf = nxpage_getnewbuf(rel, InvalidBuffer);
		stack->next->buf = nextbuf;
		Assert(BufferGetBlockNumber(nextbuf) != orignextblk);

		thisopaque->nx_next = BufferGetBlockNumber(nextbuf);

		downlink = palloc(sizeof(NXBtreeInternalPageItem));
		downlink->tid = thisopaque->nx_hikey;
		downlink->childblk = BufferGetBlockNumber(nextbuf);
		downlinks = lappend(downlinks, downlink);

		stack = stack->next;
	}
	/* last one in the chain */
	NXBtreePageGetOpaque(stack->page)->nx_next = orignextblk;

	/* If we had to split, insert downlinks for the new pages. */
	if (cxt.stack_head->next)
	{
		oldopaque = NXBtreePageGetOpaque(cxt.stack_head->page);

		if ((oldopaque->nx_flags & NXBT_ROOT) != 0)
		{
			NXBtreeInternalPageItem *downlink;

			downlink = palloc(sizeof(NXBtreeInternalPageItem));
			downlink->tid = MinNXTid;
			downlink->childblk = BufferGetBlockNumber(cxt.stack_head->buf);
			downlinks = lcons(downlink, downlinks);

			cxt.stack_tail->next = nxbt_newroot(rel, attno, oldopaque->nx_level + 1, downlinks);

			/* clear the NXBT_ROOT flag on the old root page */
			oldopaque->nx_flags &= ~NXBT_ROOT;
		}
		else
		{
			cxt.stack_tail->next = nxbt_insert_downlinks(rel, attno,
														 oldopaque->nx_lokey, BufferGetBlockNumber(oldbuf), oldopaque->nx_level + 1,
														 downlinks, oldbuf,
														 NULL);
		}
		/* note: stack_tail is not the real tail anymore */
	}

	/* Free the downlinks list and items allocated during split */
	list_free_deep(downlinks);

	/* Finally, overwrite all the pages we had to modify */
	nx_apply_split_changes(rel, cxt.stack_head, NULL);
}
