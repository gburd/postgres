/*-------------------------------------------------------------------------
 *
 * amlocator.h
 *	  The locator contract between table and index access methods.
 *
 * An index stores, for every entry, a locator: the value it hands back to
 * the table access method to identify the row the entry describes.  Heap's
 * locator is an ItemPointerData, and several parts of the system rely on
 * properties of that particular locator.  Bitmap scans split it into a block
 * and an offset.  TID range scans expose its ordering to SQL.  A table AM
 * whose rows are located differently would break those assumptions without
 * any way to say so.
 *
 * The locator descriptor lets a table AM state these properties, and lets the
 * code that depends on one test it.  A table AM returns a descriptor for each
 * relation from its relation_locator callback, reached through
 * RelationGetLocatorDesc().
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/amlocator.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef AMLOCATOR_H
#define AMLOCATOR_H

#include "storage/buf.h"

/* avoid including rel.h here; rel.h and tableam.h include this header */
struct RelationData;

/*
 * How a locator can take part in a bitmap scan.
 */
typedef enum LocatorBitmapMode
{
	LOCATOR_BITMAP_NONE = 0,	/* no bitmap scans on this table */
	LOCATOR_BITMAP_DIRECT,		/* the locator splits directly into a bucket
								 * and a slot, as a TID splits into a block
								 * and an offset */
} LocatorBitmapMode;

/*
 * A description of the locator a relation's table AM hands to its indexes.
 *
 * NB: the table AM returns this from its relation_locator callback, and the
 * relcache keeps the pointer for the life of the relcache entry, so it must
 * remain valid that long.  A static descriptor, as heap uses, satisfies this.
 */
typedef struct LocatorDesc
{
	bool		fixed_width;	/* is every locator the same size? */
	int16		width;			/* size in bytes, when fixed_width */
	const char *name;			/* for error messages */

	/* How the locator can serve a bitmap scan. */
	LocatorBitmapMode bitmap_mode;

	/*
	 * Does a row keep its locator across an UPDATE?  If not, as for heap, an
	 * update may store the new version at a different locator, and a caller
	 * that locks the latest version of a row may be handed a different one;
	 * see TM_FailureData.retargeted.
	 */
	bool		stable;

	/*
	 * Can two index entries for one row version name different slots within
	 * the same bucket?  If this is NULL they cannot, and BitmapAnd intersects
	 * each bucket's slots exactly.  Otherwise BitmapAnd calls it once per
	 * bucket, without reading the table, and unions the two sides' slots and
	 * rechecks the bucket when it returns true.  "cache" lets the function
	 * keep a pinned buffer between calls; the caller releases it.
	 */
	bool		(*bucket_may_disagree) (struct RelationData *rel, uint64 bucket,
										Buffer *cache);
} LocatorDesc;

#endif							/* AMLOCATOR_H */
