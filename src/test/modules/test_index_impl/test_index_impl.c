/*-------------------------------------------------------------------------
 *
 * test_index_impl.c
 *		Index and table access methods for testing index implementations.
 *
 * test_btree_impl implements btree indexes (amimplements = btree) with
 * nbtree's own code, but counts the aminsert and ambeginscan calls it gets,
 * so a test can tell that an index created USING btree is served by it.
 * test_btree_impl_nounique is the same without unique index support, and
 * test_hash_impl claims to implement hash indexes, which core refuses.
 *
 * test_impl_heap and test_impl_heap_nopref are heap with an
 * index_implementation callback.  The first asks for test_btree_impl for
 * btree indexes and test_hash_impl for hash indexes; the second has no
 * preference.
 *
 * heap_getnext() refuses any relation whose AM routine is not heap's, and
 * heap's index build and validation scans use it, so the table AMs present
 * heap's routine for the duration of those scans.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/test/modules/test_index_impl/test_index_impl.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/amapi.h"
#include "access/heapam.h"
#include "access/nbtree.h"
#include "access/tableam.h"
#include "catalog/pg_am_d.h"
#include "commands/defrem.h"
#include "fmgr.h"
#include "utils/fmgroids.h"

PG_MODULE_MAGIC;

static IndexAmRoutine btree_impl;
static IndexAmRoutine btree_impl_nounique;
static IndexAmRoutine hash_impl;
static TableAmRoutine impl_heap;
static TableAmRoutine impl_heap_nopref;

/* calls into test_btree_impl indexes by this backend */
static int64 insert_calls = 0;
static int64 scan_calls = 0;

static bool
counting_btinsert(Relation rel, Datum *values, bool *isnull,
				  ItemPointer ht_ctid, Relation heapRel,
				  IndexUniqueCheck checkUnique, bool indexUnchanged,
				  IndexInfo *indexInfo)
{
	insert_calls++;
	return btinsert(rel, values, isnull, ht_ctid, heapRel, checkUnique,
					indexUnchanged, indexInfo);
}

static IndexScanDesc
counting_btbeginscan(Relation rel, int nkeys, int norderbys)
{
	scan_calls++;
	return btbeginscan(rel, nkeys, norderbys);
}

static void
init_btree_impl(IndexAmRoutine *routine)
{
	*routine = *GetIndexAmRoutine(F_BTHANDLER);
	routine->amimplements = BTREE_AM_OID;
	routine->aminsert = counting_btinsert;
	routine->ambeginscan = counting_btbeginscan;
}

PG_FUNCTION_INFO_V1(test_btree_impl_handler);
Datum
test_btree_impl_handler(PG_FUNCTION_ARGS)
{
	if (btree_impl.type == 0)
		init_btree_impl(&btree_impl);
	PG_RETURN_POINTER(&btree_impl);
}

PG_FUNCTION_INFO_V1(test_btree_impl_nounique_handler);
Datum
test_btree_impl_nounique_handler(PG_FUNCTION_ARGS)
{
	if (btree_impl_nounique.type == 0)
	{
		init_btree_impl(&btree_impl_nounique);
		btree_impl_nounique.amcanunique = false;
	}
	PG_RETURN_POINTER(&btree_impl_nounique);
}

PG_FUNCTION_INFO_V1(test_hash_impl_handler);
Datum
test_hash_impl_handler(PG_FUNCTION_ARGS)
{
	if (hash_impl.type == 0)
	{
		hash_impl = *GetIndexAmRoutine(F_HASHHANDLER);
		hash_impl.amimplements = HASH_AM_OID;
	}
	PG_RETURN_POINTER(&hash_impl);
}

PG_FUNCTION_INFO_V1(test_index_impl_inserts);
Datum
test_index_impl_inserts(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(insert_calls);
}

PG_FUNCTION_INFO_V1(test_index_impl_scans);
Datum
test_index_impl_scans(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(scan_calls);
}

PG_FUNCTION_INFO_V1(test_index_impl_reset);
Datum
test_index_impl_reset(PG_FUNCTION_ARGS)
{
	insert_calls = scan_calls = 0;
	PG_RETURN_VOID();
}

static Oid
impl_heap_index_implementation(Relation rel, Oid amoid)
{
	if (amoid == BTREE_AM_OID)
		return get_index_am_oid("test_btree_impl", false);
	if (amoid == HASH_AM_OID)
		return get_index_am_oid("test_hash_impl", false);
	return InvalidOid;
}

static Oid
nopref_index_implementation(Relation rel, Oid amoid)
{
	return InvalidOid;
}

static double
as_heap_index_build_range_scan(Relation table_rel, Relation index_rel,
							   IndexInfo *index_info, bool allow_sync,
							   bool anyvisible, bool progress,
							   BlockNumber start_blockno,
							   BlockNumber numblocks,
							   IndexBuildCallback callback,
							   void *callback_state, TableScanDesc scan)
{
	const TableAmRoutine *saved = table_rel->rd_tableam;
	volatile double result = 0;

	/* Cache our own locator before anything can ask heap for one. */
	(void) RelationGetLocatorDesc(table_rel);

	table_rel->rd_tableam = GetHeapamTableAmRoutine();
	PG_TRY();
	{
		result = table_rel->rd_tableam->index_build_range_scan(table_rel,
															   index_rel,
															   index_info,
															   allow_sync,
															   anyvisible,
															   progress,
															   start_blockno,
															   numblocks,
															   callback,
															   callback_state,
															   scan);
	}
	PG_FINALLY();
	{
		table_rel->rd_tableam = saved;
	}
	PG_END_TRY();

	return result;
}

static void
as_heap_index_validate_scan(Relation table_rel, Relation index_rel,
							IndexInfo *index_info, Snapshot snapshot,
							ValidateIndexState *state)
{
	const TableAmRoutine *saved = table_rel->rd_tableam;

	(void) RelationGetLocatorDesc(table_rel);

	table_rel->rd_tableam = GetHeapamTableAmRoutine();
	PG_TRY();
	{
		table_rel->rd_tableam->index_validate_scan(table_rel, index_rel,
												   index_info, snapshot,
												   state);
	}
	PG_FINALLY();
	{
		table_rel->rd_tableam = saved;
	}
	PG_END_TRY();
}

static void
init_impl_heap(TableAmRoutine *routine,
			   Oid (*index_implementation) (Relation rel, Oid amoid))
{
	*routine = *GetHeapamTableAmRoutine();
	routine->index_implementation = index_implementation;
	routine->index_build_range_scan = as_heap_index_build_range_scan;
	routine->index_validate_scan = as_heap_index_validate_scan;
}

PG_FUNCTION_INFO_V1(test_impl_heap_handler);
Datum
test_impl_heap_handler(PG_FUNCTION_ARGS)
{
	if (impl_heap.type == 0)
		init_impl_heap(&impl_heap, impl_heap_index_implementation);
	PG_RETURN_POINTER(&impl_heap);
}

PG_FUNCTION_INFO_V1(test_impl_heap_nopref_handler);
Datum
test_impl_heap_nopref_handler(PG_FUNCTION_ARGS)
{
	if (impl_heap_nopref.type == 0)
		init_impl_heap(&impl_heap_nopref, nopref_index_implementation);
	PG_RETURN_POINTER(&impl_heap_nopref);
}
