/*-------------------------------------------------------------------------
 *
 * amlocator.h
 *	  The locator contract between table and index access methods.
 *
 * An index stores, for every entry, a locator: the value it hands back to
 * the table access method to identify the row the entry describes.  Heap's
 * locator is an ItemPointerData, and several parts of the system rely on
 * properties of that particular locator.  Bitmap scans split it into a block
 * and an offset.  TID range scans expose its ordering to SQL.  The
 * after-trigger queue records a row's locator during an UPDATE and fetches the
 * pre-update row through it later.  A table AM whose rows are located
 * differently, or that overwrites rows in place, would break those assumptions
 * without any way to say so.
 *
 * The locator descriptor lets a table AM state the properties in which its
 * locator may differ from heap's, and lets the code that depends on one test
 * it.  A table AM returns a descriptor for each relation from its
 * relation_locator callback, reached through RelationGetLocatorDesc().
 *
 * Core code also relies on these properties of every locator, which no
 * descriptor field states:
 *
 * - Index AMs and the executor keep a locator in an ItemPointerData: a block
 *   number and an offset.  validate_index() packs it into an int8 with
 *   itemptr_encode() for CREATE INDEX CONCURRENTLY.  An index can therefore
 *   hold only a locator that fits in sizeof(ItemPointerData) bytes.
 *
 * - The locator is the row's ctid, a value of type tid.  The executor carries
 *   it as a junk column through UPDATE, DELETE, MERGE and row locking
 *   (preptlist.c, appendinfo.c), WHERE CURRENT OF finds a row by it
 *   (execCurrent.c), and TID scans fetch rows by it.  This holds whether or
 *   not the table has indexes.
 *
 * - Some values mean something else to core code, so no locator uses them:
 *   offset InvalidOffsetNumber, offsets above MaxOffsetNumber (such as
 *   SpecTokenOffsetNumber and MovedPartitionsOffsetNumber in itemptr.h, and
 *   GIN's lossy-page marker), and block InvalidBlockNumber.
 *
 * - Locators sort as unsigned (block, offset) pairs, and rows near each other
 *   in that order should be near each other in storage.  nbtree orders equal
 *   keys by locator and relies on that order for deduplication and suffix
 *   truncation; ANALYZE sorts sampled rows by it (analyze.c); TID range scans
 *   expose it.  BRIN summarizes rows by ranges of block numbers, read through
 *   table_index_build_range_scan().
 *
 * Versioned locators
 * ------------------
 *
 * Heap gives a row a new locator whenever an UPDATE changes an indexed
 * column, so an index entry (key, locator) names one version of the row and
 * its key always describes that version.  A table AM that updates rows in
 * place keeps the locator, so an entry made for an older version would still
 * reach the row after its key had changed.  Such an AM can say its locator
 * is versioned: every index entry then carries a version tag, and an entry
 * names a row version as (key, locator, tag).
 *
 * - A tag is an IndexVersionTag.  InvalidIndexVersionTag (0) means untagged:
 *   the entry was made by a table AM that does not tag, or for a version
 *   older than whatever the table AM keeps history for (its "base").  An
 *   untagged entry does not mean "the current version".
 *
 * - The table AM hands out tags (with a new row version from
 *   table_tuple_update, and before each callback of its index build and
 *   validation scans) and alone defines what they mean.
 *
 * - An index AM that can store tags (amcanversion) stores each entry's tag,
 *   orders equal (key, locator) entries by it as an unsigned integer, and
 *   returns it with the locator, but never interprets it.  Other index AMs
 *   store no tag; their entries are untagged.
 *
 * - Deciding whether an entry is valid for the version a snapshot sees, and
 *   when tags may be discarded, is the table AM's obligation.  Core only
 *   carries tags between the index AM and the table AM.
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
#include "storage/off.h"

/* avoid including rel.h here; rel.h and tableam.h include this header */
struct RelationData;

/*
 * The version tag an index entry carries when the table's locator is
 * versioned; see above.  Code outside the table AM treats it as opaque.
 */
typedef uint32 IndexVersionTag;

#define InvalidIndexVersionTag	((IndexVersionTag) 0)

/*
 * A description of the locator a relation's table AM hands to its indexes.
 *
 * NB: the table AM returns this from its relation_locator callback, and the
 * relcache keeps the pointer for the life of the relcache entry, so it must
 * remain valid that long.  A static descriptor, as heap uses, satisfies this.
 */
typedef struct LocatorDesc
{
	/*
	 * The locator's size in bytes.  A positive value is an exact size, the
	 * same for every row; a negative value is the largest a variable-width
	 * locator can be, so its magnitude bounds the width either way.
	 */
	int16		width;

	/*
	 * The largest offset any locator of the relation carries, at most
	 * MaxOffsetNumber; for heap, MaxHeapTuplesPerPage.  A TID bitmap holds
	 * offsets only up to TBM_MAX_TUPLES_PER_PAGE, so the planner considers no
	 * bitmap scan of a relation whose max_offset is larger, and GIN, whose
	 * posting lists and partial-match scans assume the same bound, refuses to
	 * index it.
	 */
	OffsetNumber max_offset;

	const char *name;			/* for error messages */

	/*
	 * Does a row keep its locator for its whole life?  Heap moves a row to a
	 * new TID on every UPDATE, so it says false: a lock that follows the
	 * update chain may change *tid.  An AM that updates rows in place, or
	 * names rows by a key that UPDATE cannot change, says true, and
	 * table_tuple_lock, table_tuple_update and table_tuple_delete never
	 * change the caller's locator.
	 *
	 * This says nothing about whether the row's contents changed; a caller
	 * that must re-evaluate a concurrently updated row learns that from
	 * TM_FailureData.retargeted, whatever this says.
	 */
	bool		stable;

	/*
	 * Is the pre-update version of a row still fetchable through its locator
	 * after the UPDATE that replaced it?  Heap keeps the old version on its
	 * page until VACUUM, so it is.  An AM that overwrites rows in place says
	 * false; see RelationUpdatesInPlace().
	 *
	 * For such an AM the executor copies the old row before an UPDATE writes
	 * it, for RETURNING OLD and for the AFTER ROW triggers and transition
	 * tables, and an after-trigger event queued for the UPDATE carries that
	 * copy.  An INSERT or UPDATE event for a row trigger other than a foreign
	 * key check or a unique recheck also carries a copy of the new row, so
	 * that trigger sees OLD and NEW as of the event, as on heap.  Nothing
	 * else is copied, and the copies lack system columns.  The AM must still
	 * guarantee that:
	 *
	 * - a deleted row stays fetchable by its locator with SnapshotAny, with
	 * the contents it had when deleted, until the deleting transaction ends.
	 * The row has to survive an abort anyway; DELETE RETURNING, AFTER ROW
	 * DELETE triggers, and deferred ones at COMMIT fetch it this way.
	 *
	 * - fetching a locator with SnapshotAny yields the row's current version.
	 * Foreign key checks and unique rechecks fetch the new row by locator
	 * when they fire, and their liveness tests with SnapshotSelf must see the
	 * version current at that time, not the one the event was queued for.
	 */
	bool		old_version_retained;

	/*
	 * Does an index entry name a row version as (locator, tag)?  See
	 * "Versioned locators" above.  Heap says false: its locator alone names a
	 * version, and every entry is untagged.  An AM that says true hands out
	 * tags with new row versions, and must resolve every (locator, tag) an
	 * index hands back to it, wherever core code does that; tableam.h says
	 * where.
	 */
	bool		versioned;

	/*
	 * May a set intersection (BitmapAnd) lose a row in this group?  A bitmap
	 * scan splits each locator into a group and a slot within it, the block
	 * and line pointer offset for heap, and an intersection keeps a slot only
	 * when every input has it.  If two index entries for one row version can
	 * name different slots in a group, the intersection would drop the row;
	 * when this returns true, BitmapAnd unions the inputs' slots for the
	 * group instead and marks it for recheck.
	 *
	 * BitmapAnd consults only this callback, never the table.  The callback
	 * may keep a buffer pinned in *cache across calls on one scan; the caller
	 * releases it.  NULL means every group intersects exactly.
	 */
	bool		(*bitmap_and_inexact) (struct RelationData *rel, uint64 group,
									   Buffer *cache);

	/*
	 * May a set union (BitmapOr) mishandle this group?  Union keeps a slot
	 * when any input has it, so for heap it cannot lose a row.  This is for
	 * an AM whose groups do not correspond one to one with its storage, so
	 * that a union of two exact inputs may still need the group rechecked;
	 * when it returns true, BitmapOr marks the group for recheck.  Same
	 * calling convention as bitmap_and_inexact; NULL means every group unions
	 * exactly.
	 */
	bool		(*bitmap_or_inexact) (struct RelationData *rel, uint64 group,
									  Buffer *cache);
} LocatorDesc;

#endif							/* AMLOCATOR_H */
