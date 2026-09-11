/*-------------------------------------------------------------------------
 *
 * amlocator.c
 *	  Locator capabilities shared between table and index access methods.
 *
 * See amlocator.h for what a locator capability is and why it is declared.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/common/amlocator.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/amlocator.h"
#include "storage/itemptr.h"

/*
 * Properties of every locator capability the core code knows about.
 *
 * LOCATOR_CAP_TID describes the ItemPointerData that every in-core table AM
 * hands to its indexes.  Its answers here are simply a written-down form of
 * what the executor and the index AMs already assume:
 *
 *	decomposable   -- tidbitmap.c splits it into a block and an offset, which is
 *					  what makes bitmap scans and BitmapAnd/BitmapOr possible.
 *	prefetchable   -- index-scan prefetching passes the block half to the read
 *					  stream as a block of the table relation.
 *	stable		   -- false: a heap update that does not fit on the row's
 *					  current page stores the new version elsewhere, leaving the
 *					  old index entry behind.
 *	may_be_inexact -- false: every stored TID identifies exactly the row version
 *					  a fetch through it produces.  A table AM that wants to
 *					  store entries which only approximately locate a row sets
 *					  this and marks those entries.
 */
static const LocatorCapabilityInfo locator_capabilities[LOCATOR_CAP_COUNT] = {
	[LOCATOR_CAP_TID] = {
		.cap = LOCATOR_CAP_TID,
		.name = "tid",
		.size = sizeof(ItemPointerData),
		.decomposable = true,
		.prefetchable = true,
		.stable = false,
		.may_be_inexact = false,
	},
};

/*
 * GetLocatorCapability
 *		Return the properties of a locator capability.
 */
const LocatorCapabilityInfo *
GetLocatorCapability(LocatorCapability cap)
{
	if (cap < 0 || cap >= LOCATOR_CAP_COUNT)
		elog(ERROR, "invalid locator capability: %d", (int) cap);

	return &locator_capabilities[cap];
}
