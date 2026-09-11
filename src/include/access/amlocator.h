/*-------------------------------------------------------------------------
 *
 * amlocator.h
 *	  Locator capabilities shared between table and index access methods.
 *
 * An index stores, for every entry, a *locator*: the value it hands back to
 * the table access method to identify the row that entry describes.  Today
 * every in-core table AM hands out an ItemPointerData (a block number plus an
 * offset within that block) and every in-core index AM stores one, so the
 * contract is satisfied by convention and never checked.  A number of
 * behaviours in the executor and in the index AMs are only correct because of
 * specific properties of that particular locator, for example:
 *
 *	- Bitmap scans (and therefore BitmapAnd/BitmapOr) decompose a locator into
 *	  a page number and a small in-page slot number so that a set of locators
 *	  can be represented as a per-page bitmap, and so that a page may be
 *	  reported lossily.
 *
 *	- Index-scan prefetching feeds the page component of a locator to the read
 *	  stream as a block number of the table relation.
 *
 *	- TID range scans expose the locator's ordering directly to SQL.
 *
 * A table AM that identifies rows some other way (for example, an
 * index-organized AM that identifies a row by its primary key) cannot satisfy
 * those assumptions, and today has no way to say so.  This header names the
 * contract so both sides can declare what they implement, and so the code that
 * relies on a particular property can test for it instead of assuming it.
 *
 * A table AM provides exactly one locator capability.  An index AM supports one
 * or more.  CREATE INDEX requires that the index AM support the locator the
 * table AM provides.
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

#include "c.h"

/*
 * Identifiers for the locator capabilities known to the core code.
 *
 * This is a closed enum rather than an extensible registry because there is
 * currently exactly one in-core locator.  An out-of-tree table AM that wants a
 * locator of its own needs a registration mechanism; adding one is left for
 * when there is a caller that needs it.
 */
typedef enum LocatorCapability
{
	/*
	 * LOCATOR_CAP_TID: an ItemPointerData, that is a block number in the table
	 * relation plus a one-based offset of a line pointer within that block.
	 * This is what every in-core table AM provides.
	 */
	LOCATOR_CAP_TID = 0,

	LOCATOR_CAP_COUNT			/* must be last */
} LocatorCapability;

/* Bitmask form, for index AMs that support more than one locator. */
#define LOCATOR_CAP_MASK(cap)	(1U << (cap))

/*
 * Properties of a locator capability.
 *
 * Every field here records something that core code assumes today without
 * asking.  A new locator gets to answer these questions differently, and the
 * code that cares is expected to consult the answer.
 */
typedef struct LocatorCapabilityInfo
{
	LocatorCapability cap;
	const char *name;			/* for error messages, e.g. "tid" */

	/*
	 * Size of the locator in bytes, or 0 if it is variable length.  An index
	 * AM uses this to lay out its entries.
	 */
	Size		size;

	/*
	 * Can the locator be decomposed into a page number and a small in-page
	 * slot number?
	 *
	 * True permits bitmap scans: a set of locators can be held as a per-page
	 * bitmap, an individual page may be degraded to a lossy representation,
	 * and BitmapAnd/BitmapOr can combine two such sets before the table is
	 * consulted.  False means bitmap paths must not be generated for the
	 * table at all.
	 */
	bool		decomposable;

	/*
	 * Is the page component of the locator a real, readable block number of
	 * the table relation?
	 *
	 * True permits index-scan prefetching to hand that block number to the
	 * read stream.  False means the value is opaque and prefetching from it
	 * would read an unrelated block, so it must be disabled.
	 */
	bool		prefetchable;

	/*
	 * Does a locator, once stored in an index, continue to identify the same
	 * logical row for the remainder of that row's lifetime?
	 *
	 * Heap answers false: an update that cannot be applied on the same page
	 * stores the new row version at a different location, and the index entry
	 * that pointed at the old one is left behind for VACUUM.  An AM that
	 * updates rows without moving them answers true, and indexes over it need
	 * no maintenance for an update that leaves their keys unchanged.
	 */
	bool		stable;

	/*
	 * May the table AM hand the index a locator that does not exactly identify
	 * the row version a later fetch through it will produce?
	 *
	 * True means the table AM will mark such entries when it stores them, and
	 * that consumers which combine locators positionally -- bitmap scans in
	 * particular -- must treat a marked entry's page lossily rather than
	 * trusting its slot number.  False, the traditional behaviour, means every
	 * stored locator identifies exactly the row version a fetch will produce.
	 */
	bool		may_be_inexact;
} LocatorCapabilityInfo;

extern const LocatorCapabilityInfo *GetLocatorCapability(LocatorCapability cap);

#endif							/* AMLOCATOR_H */
