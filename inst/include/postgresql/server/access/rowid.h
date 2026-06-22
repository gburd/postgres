/*-------------------------------------------------------------------------
 *
 * rowid.h
 *	  Table-AM-governed row identifier ("RowID") contract for index access.
 *
 * An index entry must be able to (Role 1) locate the table row it points at
 * and (Role 2) provide a total order that makes the (index-key, row-id) pair
 * unique so the index AM can dedup, order, and tiebreak entries with equal
 * keys.  Historically both roles were hardcoded as a 6-byte heap ItemPointer
 * ("TID") throughout the index and executor layers.  That bakes heap's model
 * into the index AM: a table AM whose row identity is not a heap TID (an
 * in-place-MVCC AM that reuses a TID across versions, or an index-organized
 * table whose identity is its primary key) cannot express its ordering.
 *
 * The RowIDType descriptor decouples Role 2 from the index AM.  The table AM
 * registers a descriptor; the index AM orders and tiebreaks index entries by
 * calling the descriptor's comparator, never by interpreting the identity
 * bytes and never by testing "is this a heap TID".  Heap registers a
 * descriptor whose comparator is exactly ItemPointerCompare, so heap indexes
 * are byte-for-byte and behavior identical -- heap is simply one descriptor
 * instance with no privileged path.
 *
 * Role 1 (the physical locator used by index_fetch_tuple) remains a TID for
 * the in-core heap and for any AM that addresses rows by TID; only Role 2 is
 * generalized here.  A fully opaque, variable-width Role-1 identity (needed by
 * an index-organized table) is a larger, separate change; see the design note
 * in the RECNO tree.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/rowid.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ROWID_H
#define ROWID_H

#include "storage/itemptr.h"

/*
 * RowIDCmpFn -- total order over row identities, used by the index AM as the
 * tiebreaker suffix when two entries share the same index key.  Returns <0,
 * 0, or >0 like ItemPointerCompare.
 */
typedef int32 (*RowIDCmpFn) (const ItemPointerData *a,
							 const ItemPointerData *b);

/*
 * RowIDType -- a table AM's description of how its index entries are ordered
 * and made unique (Role 2).  Treated as opaque, immutable data by the index
 * AM: the index AM only ever calls the function pointers below.
 */
typedef struct RowIDType
{
	/*
	 * cmp -- total order over row identities, used by the index AM as the
	 * tiebreaker suffix when two entries share the same index key.  It is
	 * passed the two ItemPointers the index AM stores per entry (t_tid); an
	 * AM whose Role-2 identity is wider than a TID encodes the extra ordering
	 * information into the values it hands the index AM to store, so that a
	 * TID-shaped comparison over those stored values yields the intended
	 * order.  Must return <0, 0, or >0 like ItemPointerCompare.
	 */
	RowIDCmpFn	cmp;
} RowIDType;

/*
 * The in-core heap descriptor: Role-2 order is ItemPointerCompare, i.e. the
 * historical behavior.  Defined in access/common/rowid.c and returned by the
 * heap AM's index_row_key_type callback.
 */
extern const RowIDType HeapRowIDType;

/*
 * Convenience comparator matching RowIDType.cmp for the heap descriptor.
 * Thin wrapper over ItemPointerCompare so the descriptor can hold a plain
 * function pointer (ItemPointerCompare's signature already matches).
 */
extern int32 rowid_tid_compare(const ItemPointerData *a,
							   const ItemPointerData *b);

#endif							/* ROWID_H */
