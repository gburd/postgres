/*-------------------------------------------------------------------------
 *
 * rowid.c
 *	  Table-AM-governed row identifier ("RowID") support.
 *
 * See access/rowid.h for the contract.  This file provides the in-core heap
 * descriptor: a width-6 RowID whose bytes are a bare heap ItemPointerData,
 * ordered by ItemPointerCompare -- i.e. the historical index behavior, now
 * expressed through the descriptor with no privileged path in the index AM.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/common/rowid.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/rowid.h"

/*
 * rowid_tid_compare -- RowIDCmpFn for a 6-byte heap-TID identity.
 *
 * The stored bytes are an ItemPointerData; order is ItemPointerCompare.
 */
int32
rowid_tid_compare(const uint8 *a, const uint8 *b)
{
	return ItemPointerCompare((ItemPointer) a, (ItemPointer) b);
}

/*
 * The in-core heap descriptor.  width == sizeof(ItemPointerData) (6) so heap
 * index tuples store exactly the TID they always did; cmp is the historical
 * order.  Heap has no privileged branch in the index AM: its behavior comes
 * entirely from this descriptor's shape.  Fetching a row from a heap RowID is
 * done by the heap table AM's index_fetch_tuple, which reads the 6 bytes as a
 * TID -- the index AM never does that itself.
 */
const RowIDType HeapRowIDType = {
	.width = sizeof(ItemPointerData),
	.cmp = rowid_tid_compare,
	.posting = NULL,			/* heap keeps nbtree's native TID-array posting */
};
