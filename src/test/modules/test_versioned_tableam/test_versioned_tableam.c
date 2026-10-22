/*-------------------------------------------------------------------------
 *
 * test_versioned_tableam.c
 *		Test access methods for the version-tag API.
 *
 * heap_versioned is heap, except that its locator says it is versioned
 * (see "Versioned locators" in amlocator.h).  Its tag for a row version is
 * the version's raw xmin, which freezing keeps.  Rows still move on every
 * update that changes an indexed column, as heap's do, so every index entry
 * keeps describing the version its TID names.  That makes heap's own
 * visibility answers right for it, and lets each place that hands a
 * (TID, tag) pair to this AM check the pair against the tuple at that TID:
 * the tag must be 0 or that tuple's raw xmin.  This tests that core carries
 * tags to the table AM, not the semantics of in-place updates, which belong
 * to an AM that updates in place.  heap_versioned never keeps a locator, so
 * no row has history (see "Versioned locators" in amlocator.h), and its
 * build and validation scans may tag or not.  They tag every row, as an AM
 * must tag a row that has history, and tvh_build_callback asserts that the
 * tag is valid, so the build side of that rule is exercised even though
 * nothing here creates history.
 *
 * Because every update moves the row, heap_versioned never really keeps a
 * row's locator across a change to an indexed attribute.  To test the
 * executor's TU_Affected path anyway, test_versioned_tableam.keep_locator =
 * pretend makes heap_versioned report TU_Affected whenever the executor
 * allowed it to keep the locator (TABLE_UPDATE_MAY_KEEP_LOCATOR) and heap
 * moved the row.  The executor then makes entries only in the indexes the
 * update affected, so the others lack an entry for the moved row: the table
 * is corrupt afterwards, and the tests use it only on scratch tables.  With
 * 'untagged', it also reports tag 0, which the executor must refuse.
 *
 * nbtree's unique check calls table_fetch_tid(), since nbtree stores no
 * tags, so fetch_tid_version is reached from SQL (test_versioned_fetch),
 * which passes it an entry's recorded tag, as an index AM that stores tags
 * would from its unique check.
 *
 * test_versioned_btree is nbtree with amcanversion, amindexcanversion and
 * amimplements =
 * btree, so that heap_versioned can ask for it for its btree indexes.  It
 * stores no tag on disk: aminsert and its build callback record (index,
 * TID) -> tag in a shared hash, and amgettuple reports the recorded tag in
 * xs_heaptag.  heap's index scan code replaces xs_heaptid with the HOT
 * chain member it returns, so amgettuple also remembers the TID it returned,
 * for heap_versioned's check.  The hash lives in dynamic shared memory, so it survives
 * across backends but not restarts, and an index rebuilt in place
 * (REINDEX, REPACK) gets new entries over the old ones.  One tag per
 * (index, TID) is enough because rows move: a TID names one version.
 *
 * heap_versioned's VACUUM runs heap's, then vacuums each
 * test_versioned_btree index again with a version callback that answers
 * DEAD for TIDs whose tuple is gone and LIVE_UNTAG for live tagged ones;
 * the index then clears the recorded tag.
 *
 * Counters (test_versioned_counts()) record how many tags each check saw,
 * so that the tests can tell that tags reached the table AM.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/test/modules/test_versioned_tableam/test_versioned_tableam.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/amapi.h"
#include "access/amlocator.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_operator_d.h"
#include "catalog/pg_type_d.h"
#include "commands/defrem.h"
#include "commands/vacuum.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/dshash.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/dsm_registry.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/tuplesort.h"
#include "utils/tuplestore.h"

PG_MODULE_MAGIC;

/* ------------------------------------------------------------------------
 * Shared state: recorded tags and counters
 * ------------------------------------------------------------------------
 */

typedef struct TagKey
{
	Oid			indexoid;
	ItemPointerData tid;
	uint16		pad;			/* keep the key free of uninitialized bytes */
} TagKey;

typedef struct TagEntry
{
	TagKey		key;
	IndexVersionTag tag;
} TagEntry;

/* what each check saw: tagged and untagged (TID, tag) pairs */
typedef enum TvCounter
{
	TVC_INSERT_TAGGED,
	TVC_BUILD_TAGGED,
	TVC_SCAN_TAGGED,
	TVC_SCAN_UNTAGGED,
	TVC_FETCH_TAGGED,
	TVC_FETCH_UNTAGGED,
	TVC_DELETE_STATUS,
	TVC_VACUUM_DEAD,
	TVC_VACUUM_UNTAG,
	TVC_VALIDATE_TAGGED,
	TVC_VALIDATE_UNTAGGED,
	TVC_BITMAP_ROWS,
	TVC_UPDATE_KEPT,
	NUM_TVC
} TvCounter;

static const char *const tvc_names[NUM_TVC] = {
	"insert_tagged", "build_tagged", "scan_tagged", "scan_untagged",
	"fetch_tagged", "fetch_untagged", "delete_status", "vacuum_dead",
	"vacuum_untag", "validate_tagged", "validate_untagged", "bitmap_rows",
	"update_kept",
};

typedef struct TvShared
{
	pg_atomic_uint64 counters[NUM_TVC];
} TvShared;

static TvShared *tv_shared = NULL;
static dshash_table *tv_tags = NULL;

static const dshash_parameters tv_tags_params = {
	sizeof(TagKey),
	sizeof(TagEntry),
	dshash_memcmp,
	dshash_memhash,
	dshash_memcpy,
};

static void
tv_init_shared(void *ptr, void *arg)
{
	TvShared   *shared = (TvShared *) ptr;

	for (int i = 0; i < NUM_TVC; i++)
		pg_atomic_init_u64(&shared->counters[i], 0);
}

static void
tv_attach(void)
{
	bool		found;

	if (tv_shared == NULL)
		tv_shared = GetNamedDSMSegment("test_versioned_tableam",
									   sizeof(TvShared), tv_init_shared,
									   &found, NULL);
	if (tv_tags == NULL)
		tv_tags = GetNamedDSHash("test_versioned_tableam_tags",
								 &tv_tags_params, &found);
}

static void
tv_count(TvCounter c)
{
	tv_attach();
	pg_atomic_fetch_add_u64(&tv_shared->counters[c], 1);
}

static TagKey
tv_key(Oid indexoid, ItemPointer tid)
{
	TagKey		key;

	memset(&key, 0, sizeof(key));
	key.indexoid = indexoid;
	key.tid = *tid;
	return key;
}

static void
tv_record(Oid indexoid, ItemPointer tid, IndexVersionTag tag)
{
	TagKey		key = tv_key(indexoid, tid);
	TagEntry   *entry;
	bool		found;

	tv_attach();
	entry = dshash_find_or_insert(tv_tags, &key, &found);
	entry->tag = tag;
	dshash_release_lock(tv_tags, entry);
}

static IndexVersionTag
tv_lookup(Oid indexoid, ItemPointer tid)
{
	TagKey		key = tv_key(indexoid, tid);
	TagEntry   *entry;
	IndexVersionTag tag = InvalidIndexVersionTag;

	tv_attach();
	entry = dshash_find(tv_tags, &key, false);
	if (entry)
	{
		tag = entry->tag;
		dshash_release_lock(tv_tags, entry);
	}
	return tag;
}

static void
tv_forget(Oid indexoid, ItemPointer tid)
{
	TagKey		key = tv_key(indexoid, tid);

	tv_attach();
	(void) dshash_delete_key(tv_tags, &key);
}

/* ------------------------------------------------------------------------
 * The plumbing check
 * ------------------------------------------------------------------------
 */

/*
 * Check that (tid, tag), handed to the table AM by `what`, names the tuple
 * at tid: the tag is 0 or that tuple's raw xmin.  Returns whether a live
 * row may be reached through tid: a tuple is there, or a redirect to the
 * rest of a pruned HOT chain (whose root, the tuple the tag named, is gone,
 * so the tag cannot be checked).
 */
static bool
tv_check_tag(Relation rel, ItemPointer tid, IndexVersionTag tag,
			 const char *what)
{
	Buffer		buf;
	Page		page;
	OffsetNumber off = ItemPointerGetOffsetNumber(tid);
	ItemId		lp;
	bool		present = false;

	if (ItemPointerGetBlockNumber(tid) >= RelationGetNumberOfBlocks(rel))
		return false;

	buf = ReadBuffer(rel, ItemPointerGetBlockNumber(tid));
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	if (off >= FirstOffsetNumber && off <= PageGetMaxOffsetNumber(page))
	{
		lp = PageGetItemId(page, off);
		if (ItemIdIsRedirected(lp))
			present = true;
		else if (ItemIdIsNormal(lp))
		{
			HeapTupleHeader htup = (HeapTupleHeader) PageGetItem(page, lp);
			TransactionId xmin = HeapTupleHeaderGetRawXmin(htup);

			present = true;
			if (tag != InvalidIndexVersionTag && tag != (IndexVersionTag) xmin)
			{
				UnlockReleaseBuffer(buf);
				elog(ERROR, "%s: tag %u does not name the tuple at (%u,%u), whose xmin is %u",
					 what, tag, ItemPointerGetBlockNumber(tid), off, xmin);
			}
		}
	}
	UnlockReleaseBuffer(buf);
	return present;
}

/* The tag heap_versioned gives a tuple: its raw xmin. */
static IndexVersionTag
tv_tag_of(HeapTuple tuple)
{
	return (IndexVersionTag) HeapTupleHeaderGetRawXmin(tuple->t_data);
}

/* ------------------------------------------------------------------------
 * test_versioned_btree
 * ------------------------------------------------------------------------
 */

static IndexAmRoutine tvb_routine;

static bool
tvb_insert(Relation rel, Datum *values, bool *isnull,
		   ItemPointer ht_ctid, Relation heapRel,
		   IndexUniqueCheck checkUnique, bool indexUnchanged,
		   IndexInfo *indexInfo)
{
	bool		result;

	result = btinsert(rel, values, isnull, ht_ctid, heapRel, checkUnique,
					  indexUnchanged, indexInfo);

	/* a recheck makes no entry; see indexam.sgml */
	if (checkUnique != UNIQUE_CHECK_EXISTING)
	{
		tv_record(RelationGetRelid(rel), ht_ctid, indexInfo->ii_VersionTag);
		if (indexInfo->ii_VersionTag != InvalidIndexVersionTag)
			tv_count(TVC_INSERT_TAGGED);
	}
	return result;
}

/*
 * The build callback's side of recording a tag.  nbtree's btbuild passes
 * its own callback to the table AM's build scan, which test_versioned_btree
 * cannot wrap without copying btbuild, so heap_versioned's build scan calls
 * this after nbtree's callback, while ii_VersionTag still holds the row's
 * tag.  (A real index AM reads ii_VersionTag in its own callback.)
 */
static void
tvb_build_record(Relation index, ItemPointer tid, IndexInfo *indexInfo)
{
	tv_record(RelationGetRelid(index), tid, indexInfo->ii_VersionTag);
	if (indexInfo->ii_VersionTag != InvalidIndexVersionTag)
		tv_count(TVC_BUILD_TAGGED);
}

/* the entry the last tvb_gettuple call returned; see the file header */
static IndexScanDesc tvb_last_scan = NULL;
static ItemPointerData tvb_last_tid;

static bool
tvb_gettuple(IndexScanDesc scan, ScanDirection dir)
{
	if (!btgettuple(scan, dir))
		return false;
	scan->xs_heaptag = tv_lookup(RelationGetRelid(scan->indexRelation),
								 &scan->xs_heaptid);
	tvb_last_scan = scan;
	tvb_last_tid = scan->xs_heaptid;
	return true;
}

typedef struct TvbVacuumState
{
	IndexBulkDeleteCallback callback;
	void	   *callback_state;
	IndexBulkDeleteVersionCallback version_callback;
	void	   *version_state;
	Oid			indexoid;
} TvbVacuumState;

/* hand nbtree's (TID) question to the version callback with its tag */
static bool
tvb_vacuum_callback(ItemPointer tid, void *state)
{
	TvbVacuumState *vs = (TvbVacuumState *) state;
	IndexVersionTag tag = tv_lookup(vs->indexoid, tid);

	switch (vs->version_callback(tid, tag, vs->version_state))
	{
		case INDEX_ENTRY_DEAD:
			tv_forget(vs->indexoid, tid);
			return true;
		case INDEX_ENTRY_LIVE_UNTAG:
			tv_record(vs->indexoid, tid, InvalidIndexVersionTag);
			return false;
		case INDEX_ENTRY_LIVE:
			return false;
	}
	return false;
}

static IndexBulkDeleteResult *
tvb_bulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
			   IndexBulkDeleteCallback callback, void *callback_state)
{
	TvbVacuumState vs;

	if (info->version_callback == NULL)
		return btbulkdelete(info, stats, callback, callback_state);

	vs.callback = callback;
	vs.callback_state = callback_state;
	vs.version_callback = info->version_callback;
	vs.version_state = info->version_state;
	vs.indexoid = RelationGetRelid(info->index);
	return btbulkdelete(info, stats, tvb_vacuum_callback, &vs);
}

/*
 * amindexcanversion: an index whose name ends in "_notag" stores no tags, so
 * that a test can show the executor treating one index of an AM with
 * amcanversion as an index without it.
 */
static bool
tvb_indexcanversion(Relation index)
{
	const char *name = RelationGetRelationName(index);
	size_t		len = strlen(name);

	return !(len >= 6 && strcmp(name + len - 6, "_notag") == 0);
}

PG_FUNCTION_INFO_V1(test_versioned_btree_handler);
Datum
test_versioned_btree_handler(PG_FUNCTION_ARGS)
{
	if (tvb_routine.type == 0)
	{
		IndexAmRoutine routine = *GetIndexAmRoutine(F_BTHANDLER);

		routine.amcanversion = true;
		routine.amindexcanversion = tvb_indexcanversion;
		routine.amimplements = BTREE_AM_OID;
		routine.amcanbuildparallel = false; /* see tvb_build_record */
		routine.aminsert = tvb_insert;
		routine.amgettuple = tvb_gettuple;
		routine.ambulkdelete = tvb_bulkdelete;
		tvb_routine = routine;
	}
	PG_RETURN_POINTER(&tvb_routine);
}

/* ------------------------------------------------------------------------
 * heap_versioned
 * ------------------------------------------------------------------------
 */

static TableAmRoutine tvh_routine;

typedef enum
{
	KEEP_LOCATOR_OFF,
	KEEP_LOCATOR_PRETEND,
	KEEP_LOCATOR_UNTAGGED,
} KeepLocatorMode;

static int	keep_locator = KEEP_LOCATOR_OFF;

/* percentage of rows its build scan tags; see tvh_build_callback */
static int	build_tag_percent = 100;

static const struct config_enum_entry keep_locator_options[] = {
	{"off", KEEP_LOCATOR_OFF, false},
	{"pretend", KEEP_LOCATOR_PRETEND, false},
	{"untagged", KEEP_LOCATOR_UNTAGGED, false},
	{NULL, 0, false}
};

static bool tvh_fetch_tid_version(Relation rel, ItemPointer tid,
								  IndexVersionTag tag, Relation index,
								  Snapshot snapshot, bool *all_dead);

static const LocatorDesc tvh_locator = {
	.width = sizeof(ItemPointerData),
	.max_offset = MaxHeapTuplesPerPage,
	.name = "tid",
	.stable = false,
	.old_version_retained = true,
	.versioned = true,
};

static const LocatorDesc *
tvh_relation_locator(Relation rel)
{
	return &tvh_locator;
}

/*
 * btree indexes are test_versioned_btree, unless default_btree_implementation
 * names another implementation (bark), which then implements them.
 */
static Oid
tvh_index_implementation(Relation rel, Oid amoid)
{
	if (amoid == BTREE_AM_OID && default_btree_implementation[0] == '\0')
		return get_index_am_oid("test_versioned_btree", false);
	return InvalidOid;
}

/*
 * The root of the HOT chain that tid is a member of: the TID an index entry
 * for that row names.  heap's index scan replaces xs_heaptid with the chain
 * member it returns.
 */
static ItemPointerData
tv_root_tid(Relation rel, ItemPointer tid)
{
	OffsetNumber roots[MaxHeapTuplesPerPage];
	Buffer		buf = ReadBuffer(rel, ItemPointerGetBlockNumber(tid));
	ItemPointerData root = *tid;

	LockBuffer(buf, BUFFER_LOCK_SHARE);
	heap_get_root_tuples(BufferGetPage(buf), roots);
	UnlockReleaseBuffer(buf);
	if (OffsetNumberIsValid(roots[ItemPointerGetOffsetNumber(tid) - 1]))
		ItemPointerSetOffsetNumber(&root,
								   roots[ItemPointerGetOffsetNumber(tid) - 1]);
	return root;
}

/* heap's TOAST tables are plain heap */
static Oid
tvh_relation_toast_am(Relation rel)
{
	return HEAP_TABLE_AM_OID;
}

/*
 * heap_getnext() refuses any relation whose routine is not heap's, and
 * heap's build, validation and CLUSTER code uses it, so present heap's
 * routine while running them.  The locator descriptor is cached first, so
 * that it stays ours.
 */
#define AS_HEAP(rel, stmt) \
	do { \
		const TableAmRoutine *saved_ = (rel)->rd_tableam; \
		(void) RelationGetLocatorDesc(rel); \
		(rel)->rd_tableam = GetHeapamTableAmRoutine(); \
		PG_TRY(); \
		{ \
			stmt; \
		} \
		PG_FINALLY(); \
		{ \
			(rel)->rd_tableam = saved_; \
		} \
		PG_END_TRY(); \
	} while (0)

/* --- index scans --- */

typedef struct TvhScanState
{
	void	   *heap_opaque;	/* heap's own xs_table_opaque */
	bool		(*heap_getnext) (IndexScanDesc scan, ScanDirection direction,
								 TupleTableSlot *slot);
} TvhScanState;

static bool
tvh_index_getnext_slot(IndexScanDesc scan, ScanDirection direction,
					   TupleTableSlot *slot)
{
	TvhScanState *ts = (TvhScanState *) scan->xs_table_opaque;
	bool		result;

	scan->xs_table_opaque = ts->heap_opaque;
	PG_TRY();
	{
		result = ts->heap_getnext(scan, direction, slot);
	}
	PG_FINALLY();
	{
		scan->xs_table_opaque = ts;
	}
	PG_END_TRY();

	if (result && index_can_version(scan->indexRelation))
	{
		/* test_versioned_btree remembers its entry's TID; others: the root */
		ItemPointerData tid = tvb_last_scan == scan ? tvb_last_tid :
			tv_root_tid(scan->heapRelation, &scan->xs_heaptid);

		tv_check_tag(scan->heapRelation, &tid, scan->xs_heaptag,
					 "index scan");
		tv_count(scan->xs_heaptag != InvalidIndexVersionTag ?
				 TVC_SCAN_TAGGED : TVC_SCAN_UNTAGGED);
	}
	else if (result && scan->xs_heaptag != InvalidIndexVersionTag)
		elog(ERROR, "index scan: index \"%s\" stores no tags, but returned tag %u",
			 RelationGetRelationName(scan->indexRelation), scan->xs_heaptag);
	return result;
}

static void
tvh_index_scan_begin(IndexScanDesc scan, uint32 flags)
{
	TvhScanState *ts = palloc_object(TvhScanState);

	heapam_index_scan_begin(scan, flags);
	ts->heap_opaque = scan->xs_table_opaque;
	ts->heap_getnext = scan->xs_getnext_slot;
	scan->xs_table_opaque = ts;
	scan->xs_getnext_slot = tvh_index_getnext_slot;
}

static void
tvh_index_scan_reset(IndexScanDesc scan)
{
	TvhScanState *ts = (TvhScanState *) scan->xs_table_opaque;

	scan->xs_table_opaque = ts->heap_opaque;
	heapam_index_scan_reset(scan);
	scan->xs_table_opaque = ts;
}

static void
tvh_index_scan_end(IndexScanDesc scan)
{
	TvhScanState *ts = (TvhScanState *) scan->xs_table_opaque;

	scan->xs_table_opaque = ts->heap_opaque;
	heapam_index_scan_end(scan);
	pfree(ts);
}

/* --- constraint fetch --- */

static bool
tvh_fetch_tid_version(Relation rel, ItemPointer tid, IndexVersionTag tag,
					  Relation index, Snapshot snapshot, bool *all_dead)
{
	if (index_can_version(index))
	{
		tv_check_tag(rel, tid, tag, "fetch_tid_version");
		tv_count(tag != InvalidIndexVersionTag ?
				 TVC_FETCH_TAGGED : TVC_FETCH_UNTAGGED);
	}
	else if (tag != InvalidIndexVersionTag)
		elog(ERROR, "fetch_tid_version: index \"%s\" stores no tags, but got tag %u",
			 RelationGetRelationName(index), tag);

	/* every version a TID reaches has the entry's key; see file header */
	return heapam_fetch_tid(rel, tid, snapshot, all_dead);
}

/* --- index deletion --- */

static TransactionId
tvh_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
{
	for (int i = 0; i < delstate->ndeltids; i++)
	{
		TM_IndexStatus *status = &delstate->status[delstate->deltids[i].id];

		tv_check_tag(rel, &delstate->deltids[i].tid, status->tag,
					 "index_delete_tuples");
		tv_count(TVC_DELETE_STATUS);
	}
	return heap_index_delete_tuples(rel, delstate);
}

/* --- update --- */

static TM_Result
tvh_tuple_update(Relation rel, ItemPointer otid, TupleTableSlot *slot,
				 CommandId cid, uint32 options, Snapshot snapshot,
				 Snapshot crosscheck, bool wait, TM_FailureData *tmfd,
				 LockTupleMode *lockmode,
				 const Bitmapset *modified_idx_attrs,
				 TU_UpdateIndexes *update_indexes,
				 IndexVersionTag *version_tag)
{
	TM_Result	result;

	result = GetHeapamTableAmRoutine()->tuple_update(rel, otid, slot, cid,
													 options, snapshot,
													 crosscheck, wait, tmfd,
													 lockmode,
													 modified_idx_attrs,
													 update_indexes,
													 version_tag);
	if (result != TM_Ok)
		return result;

	/* tag every new version with its xmin, the current transaction */
	*version_tag = (IndexVersionTag) GetCurrentTransactionId();

	/* see the file header */
	if (keep_locator != KEEP_LOCATOR_OFF &&
		(options & TABLE_UPDATE_MAY_KEEP_LOCATOR) &&
		*update_indexes == TU_All)
	{
		*update_indexes = TU_Affected;
		if (keep_locator == KEEP_LOCATOR_UNTAGGED)
			*version_tag = InvalidIndexVersionTag;
		tv_count(TVC_UPDATE_KEPT);
	}
	return result;
}

/* --- build, validation --- */

typedef struct TvhBuildState
{
	IndexBuildCallback callback;
	void	   *callback_state;
	IndexInfo  *indexInfo;
	Relation	heap;
} TvhBuildState;

/*
 * Tag each row handed to a test_versioned_btree index with the xmin of the
 * tuple at its TID.  heap hands a heap-only tuple under its chain root's
 * TID, and the entry names the root, so that is the tuple whose xmin the
 * other checks compare against.  (No row has history, so leaving rows
 * untagged would be as valid; tagging them makes build tags reach scans.)
 */
static void
tvh_build_callback(Relation index, ItemPointer tid, Datum *values,
				   bool *isnull, bool tupleIsAlive, void *state)
{
	TvhBuildState *bs = (TvhBuildState *) state;
	Buffer		buf;
	HeapTupleData tuple;

	bs->indexInfo->ii_VersionTag = InvalidIndexVersionTag;
	tuple.t_self = *tid;
	if (heap_fetch(bs->heap, SnapshotAny, &tuple, &buf, true))
	{
		/*
		 * Tag every row, as rows with history must be (see file header), or
		 * with build_tag_percent < 100 only that share of them, chosen by
		 * TID, which no row here needs since none has history.
		 */
		if ((ItemPointerGetBlockNumber(tid) * 7 +
			 ItemPointerGetOffsetNumber(tid)) % 100 < build_tag_percent)
		{
			bs->indexInfo->ii_VersionTag = tv_tag_of(&tuple);
			Assert(bs->indexInfo->ii_VersionTag != InvalidIndexVersionTag);
		}
		ReleaseBuffer(buf);
	}
	else if (BufferIsValid(buf))
		ReleaseBuffer(buf);
	bs->callback(index, tid, values, isnull, tupleIsAlive,
				 bs->callback_state);
	/* an index AM that stores tags itself (bark) needs no record */
	if (index->rd_indam->aminsert == tvb_insert)
		tvb_build_record(index, tid, bs->indexInfo);
}

static double
tvh_index_build_range_scan(Relation table_rel, Relation index_rel,
						   IndexInfo *index_info, bool allow_sync,
						   bool anyvisible, bool progress,
						   BlockNumber start_blockno, BlockNumber numblocks,
						   IndexBuildCallback callback, void *callback_state,
						   TableScanDesc scan)
{
	volatile double result = 0;
	TvhBuildState bs;

	bs.callback = callback;
	bs.callback_state = callback_state;
	bs.indexInfo = index_info;
	bs.heap = table_rel;

	/* other indexes' entries are untagged */
	if (!index_can_version(index_rel))
		bs.callback = NULL;

	AS_HEAP(table_rel,
			result = table_rel->rd_tableam->index_build_range_scan(table_rel, index_rel,
																   index_info, allow_sync,
																   anyvisible, progress,
																   start_blockno, numblocks,
																   bs.callback ? tvh_build_callback : callback,
																   bs.callback ? (void *) &bs : callback_state,
																   scan));
	index_info->ii_VersionTag = InvalidIndexVersionTag;
	return result;
}

/*
 * Validation with (TID, tag) pairs: check every collected pair, then hand
 * heap's validate scan the TIDs.  Since rows move, a TID alone tells whether
 * the version it names is indexed, which is what heap's scan relies on.
 * Rows it inserts are tagged by tvh_validate_insert.
 */
static void
tvh_index_validate_scan(Relation table_rel, Relation index_rel,
						IndexInfo *index_info, Snapshot snapshot,
						ValidateIndexState *state)
{
	ValidateIndexState heapstate;
	TupleTableSlot *slot;

	if (!state->versioned)
	{
		AS_HEAP(table_rel,
				table_rel->rd_tableam->index_validate_scan(table_rel, index_rel,
														   index_info, snapshot,
														   state));
		return;
	}

	heapstate = *state;
	heapstate.versioned = false;
	heapstate.tupdesc = NULL;
	heapstate.tuplesort = tuplesort_begin_datum(INT8OID, Int8LessOperator,
												InvalidOid, false,
												maintenance_work_mem,
												NULL, TUPLESORT_NONE);
	slot = MakeSingleTupleTableSlot(state->tupdesc, &TTSOpsMinimalTuple);
	while (tuplesort_gettupleslot(state->tuplesort, true, false, slot, NULL))
	{
		bool		isnull;
		int64		encoded = DatumGetInt64(slot_getattr(slot, 1, &isnull));
		IndexVersionTag tag = (IndexVersionTag) DatumGetInt64(slot_getattr(slot, 2, &isnull));
		ItemPointerData tid;

		itemptr_decode(&tid, encoded);
		tv_check_tag(table_rel, &tid, tag, "index_validate_scan");
		tv_count(tag != InvalidIndexVersionTag ?
				 TVC_VALIDATE_TAGGED : TVC_VALIDATE_UNTAGGED);
		tuplesort_putdatum(heapstate.tuplesort, Int64GetDatum(encoded), false);
	}
	ExecDropSingleTupleTableSlot(slot);
	tuplesort_performsort(heapstate.tuplesort);

	/* heap's scan inserts with index_insert; tvb_insert reads the tag */
	index_info->ii_VersionTag = InvalidIndexVersionTag;
	AS_HEAP(table_rel,
			table_rel->rd_tableam->index_validate_scan(table_rel, index_rel,
													   index_info, snapshot,
													   &heapstate));
	tuplesort_end(heapstate.tuplesort);
	state->htups = heapstate.htups;
	state->tups_inserted = heapstate.tups_inserted;
}

/* --- bitmap scans --- */

static bool
tvh_scan_bitmap_next_tuple(TableScanDesc scan, TupleTableSlot *slot,
						   bool *recheck, uint64 *lossy_pages,
						   uint64 *exact_pages)
{
	if (!GetHeapamTableAmRoutine()->scan_bitmap_next_tuple(scan, slot, recheck,
														   lossy_pages,
														   exact_pages))
		return false;

	/*
	 * A bitmap holds TIDs only.  Every entry still names the version at its
	 * TID (rows move), so there is nothing to recheck; just count the rows.
	 */
	tv_count(TVC_BITMAP_ROWS);
	return true;
}

/* --- VACUUM --- */

typedef struct TvhVacuumState
{
	Relation	heap;
	Relation	index;
} TvhVacuumState;

static IndexEntryVerdict
tvh_version_callback(ItemPointer tid, IndexVersionTag tag, void *state)
{
	TvhVacuumState *vs = (TvhVacuumState *) state;

	if (!tv_check_tag(vs->heap, tid, tag, "vacuum version callback"))
	{
		tv_count(TVC_VACUUM_DEAD);
		return INDEX_ENTRY_DEAD;
	}
	if (tag != InvalidIndexVersionTag)
	{
		tv_count(TVC_VACUUM_UNTAG);
		return INDEX_ENTRY_LIVE_UNTAG;
	}
	return INDEX_ENTRY_LIVE;
}

/*
 * Heap's VACUUM, then an extra pass over each test_versioned_btree index.
 * Heap's pass has already removed the entries of dead tuples and made their
 * line pointers unused, so the extra pass finds them gone; the DEAD verdict
 * is for line pointers whose tuple has no storage, which a heap pass that
 * skipped index vacuuming (INDEX_CLEANUP off) leaves behind.
 */
static void
tvh_relation_vacuum(Relation rel, const VacuumParams *params,
					BufferAccessStrategy bstrategy)
{
	Relation   *indrels;
	int			nindexes;

	heap_vacuum_rel(rel, params, bstrategy);

	vac_open_indexes(rel, RowExclusiveLock, &nindexes, &indrels);
	for (int i = 0; i < nindexes; i++)
	{
		IndexVacuumInfo ivinfo;
		TvhVacuumState vs;
		IndexBulkDeleteResult *stats;

		if (!index_can_version(indrels[i]))
			continue;

		memset(&ivinfo, 0, sizeof(ivinfo));
		ivinfo.index = indrels[i];
		ivinfo.heaprel = rel;
		ivinfo.message_level = DEBUG2;
		ivinfo.num_heap_tuples = rel->rd_rel->reltuples;
		ivinfo.estimated_count = true;
		ivinfo.strategy = bstrategy;
		vs.heap = rel;
		vs.index = indrels[i];
		ivinfo.version_callback = tvh_version_callback;
		ivinfo.version_state = &vs;

		stats = index_bulk_delete(&ivinfo, NULL, NULL, NULL);
		stats = index_vacuum_cleanup(&ivinfo, stats);
		if (stats)
			pfree(stats);
	}
	vac_close_indexes(nindexes, indrels, NoLock);
}

PG_FUNCTION_INFO_V1(heap_versioned_handler);
Datum
heap_versioned_handler(PG_FUNCTION_ARGS)
{
	if (tvh_routine.type == 0)
	{
		TableAmRoutine routine = *GetHeapamTableAmRoutine();

		routine.relation_locator = tvh_relation_locator;
		routine.index_implementation = tvh_index_implementation;
		routine.relation_toast_am = tvh_relation_toast_am;
		routine.index_scan_begin = tvh_index_scan_begin;
		routine.index_scan_reset = tvh_index_scan_reset;
		routine.index_scan_end = tvh_index_scan_end;
		routine.fetch_tid_version = tvh_fetch_tid_version;
		routine.index_delete_tuples = tvh_index_delete_tuples;
		routine.tuple_update = tvh_tuple_update;
		routine.index_build_range_scan = tvh_index_build_range_scan;
		routine.index_validate_scan = tvh_index_validate_scan;
		routine.scan_bitmap_next_tuple = tvh_scan_bitmap_next_tuple;
		routine.relation_vacuum = tvh_relation_vacuum;
		tvh_routine = routine;
	}
	PG_RETURN_POINTER(&tvh_routine);
}

/* ------------------------------------------------------------------------
 * SQL-callable helpers
 * ------------------------------------------------------------------------
 */

/* the recorded tag of (index, tid), or NULL */
PG_FUNCTION_INFO_V1(test_versioned_tag);
Datum
test_versioned_tag(PG_FUNCTION_ARGS)
{
	Oid			indexoid = PG_GETARG_OID(0);
	ItemPointer tid = PG_GETARG_ITEMPOINTER(1);
	TagKey		key = tv_key(indexoid, tid);
	TagEntry   *entry;
	int64		tag;

	tv_attach();
	entry = dshash_find(tv_tags, &key, false);
	if (entry == NULL)
		PG_RETURN_NULL();
	tag = entry->tag;
	dshash_release_lock(tv_tags, entry);
	PG_RETURN_INT64(tag);
}

/* table_fetch_tid_version() for (index, tid) with its recorded tag */
PG_FUNCTION_INFO_V1(test_versioned_fetch);
Datum
test_versioned_fetch(PG_FUNCTION_ARGS)
{
	Oid			indexoid = PG_GETARG_OID(0);
	ItemPointerData tid = *PG_GETARG_ITEMPOINTER(1);
	Relation	index = index_open(indexoid, AccessShareLock);
	Relation	heap = table_open(index->rd_index->indrelid, AccessShareLock);
	bool		all_dead;
	bool		found;

	found = table_fetch_tid_version(heap, &tid, tv_lookup(indexoid, &tid),
									index, GetActiveSnapshot(), &all_dead);
	table_close(heap, AccessShareLock);
	index_close(index, AccessShareLock);
	PG_RETURN_BOOL(found);
}

/*
 * Insert one entry into `index` for the row version at `tid`, with its
 * current key and tag `tag`, as a table AM that updates in place would
 * insert the entry of a new version of the row: for tests that need an
 * entry at a TID the index already has entries for.  The row's key is read
 * from the tuple at `tid` (SnapshotAny).  heap_versioned's checks then hold
 * the tag to that tuple's xmin.
 */
PG_FUNCTION_INFO_V1(test_versioned_insert_entry);
Datum
test_versioned_insert_entry(PG_FUNCTION_ARGS)
{
	Oid			indexoid = PG_GETARG_OID(0);
	ItemPointerData tid = *PG_GETARG_ITEMPOINTER(1);
	int64		tag = PG_GETARG_INT64(2);
	Relation	index = index_open(indexoid, RowExclusiveLock);
	Relation	heap = table_open(index->rd_index->indrelid, RowExclusiveLock);
	IndexInfo  *indexInfo = BuildIndexInfo(index);
	EState	   *estate = CreateExecutorState();
	ExprContext *econtext = GetPerTupleExprContext(estate);
	TupleTableSlot *slot = table_slot_create(heap, NULL);
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];

	if (tag < 0 || tag > PG_UINT32_MAX)
		elog(ERROR, "tag %lld is out of range", (long long) tag);
	/* as an UPDATE would: its commit then flushes the insert's WAL */
	(void) GetCurrentTransactionId();
	if (!table_tuple_fetch_row_version(heap, &tid, SnapshotAny, slot))
		elog(ERROR, "no tuple at (%u,%u)", ItemPointerGetBlockNumber(&tid),
			 ItemPointerGetOffsetNumber(&tid));
	econtext->ecxt_scantuple = slot;
	FormIndexDatum(indexInfo, slot, estate, values, isnull);
	indexInfo->ii_VersionTag = (IndexVersionTag) tag;
	index_insert(index, values, isnull, &tid, heap, UNIQUE_CHECK_NO, false,
				 indexInfo);
	index_insert_cleanup(index, indexInfo);
	ExecDropSingleTupleTableSlot(slot);
	FreeExecutorState(estate);
	table_close(heap, RowExclusiveLock);
	index_close(index, RowExclusiveLock);
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(test_versioned_counts);
Datum
test_versioned_counts(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);
	tv_attach();
	for (int i = 0; i < NUM_TVC; i++)
	{
		Datum		values[2];
		bool		nulls[2] = {false, false};

		values[0] = CStringGetTextDatum(tvc_names[i]);
		values[1] = Int64GetDatum((int64) pg_atomic_read_u64(&tv_shared->counters[i]));
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}
	return (Datum) 0;
}

PG_FUNCTION_INFO_V1(test_versioned_reset);
Datum
test_versioned_reset(PG_FUNCTION_ARGS)
{
	tv_attach();
	for (int i = 0; i < NUM_TVC; i++)
		pg_atomic_write_u64(&tv_shared->counters[i], 0);
	PG_RETURN_VOID();
}

void
_PG_init(void)
{
	DefineCustomEnumVariable("test_versioned_tableam.keep_locator",
							 "Makes heap_versioned pretend it kept a moved row's locator.",
							 NULL,
							 &keep_locator,
							 KEEP_LOCATOR_OFF,
							 keep_locator_options,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);
	DefineCustomIntVariable("test_versioned_tableam.build_tag_percent",
							"Percentage of rows heap_versioned's index build scan tags.",
							NULL,
							&build_tag_percent,
							100, 0, 100,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);
	MarkGUCPrefixReserved("test_versioned_tableam");
}
