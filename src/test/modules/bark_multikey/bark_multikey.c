/*--------------------------------------------------------------------------
 *
 * bark_multikey.c
 *		A multikey operator class for BARK over int4[], for testing.
 *
 * The class's keys are an array's elements.  Its extract-value procedure (7)
 * returns them, its extract-query procedure (8) turns &&, @>, <@ and = into
 * boundaries over them, and its index-recheck procedure (9) answers for one
 * entry.  |<| is an ordering operator: it orders arrays by their least
 * element, MongoDB's ascending sort rule for arrays.  A second procedure 7,
 * for a class with markers, also returns a marker for an array of more than
 * one element, as a wildcard class marks a path that has held an array.
 * The SQL-callable helpers at the end run procedures 7 and 8 as BARK would
 * and print what they return, so the class can be checked before BARK calls
 * it, and show what BARK stored: the meta page's flag and count, an index's
 * entries in index order, and its markers.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/bark_multikey/bark_multikey.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/bark.h"
#include "access/cmptype.h"
#include "access/genam.h"
#include "access/gin.h"
#include "access/htup_details.h"
#include "access/relation.h"
#include "access/relscan.h"
#include "access/stratnum.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/tableam_indexscan.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "common/int.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/qunique.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/tuplestore.h"
#include "utils/varlena.h"

PG_MODULE_MAGIC;

/* The class's strategy numbers, GIN's array_ops numbers plus |<|. */
#define BARK_MK_OVERLAP		1	/* && */
#define BARK_MK_CONTAINS	2	/* @> */
#define BARK_MK_CONTAINED	3	/* <@ */
#define BARK_MK_EQUAL		4	/* = */
#define BARK_MK_ORDER		5	/* |<|, ordering */
#define BARK_MK_ORDER_DESC	15	/* |>>|, ordering by the greatest element */

/*
 * The range class's strategies, for the scan's boundary walk: an element
 * above a value, one at or below it, one in [q[1], q[2]), an even element
 * of the query, an element equal to a value, and a strategy whose
 * procedures misbehave on purpose.
 */
#define BARK_MK_GT			6	/* |>| */
#define BARK_MK_LE			7	/* |<=| */
#define BARK_MK_WITHIN		8	/* |><| */
#define BARK_MK_EVEN_IN		9	/* |%| */
#define BARK_MK_HAS			10	/* |=| */
#define BARK_MK_BROKEN		11	/* |!| */
#define BARK_MK_NONE_GT		12	/* |?| */
#define BARK_MK_NE			13	/* |<>| */
#define BARK_MK_NEAR		14	/* |~| */

PG_FUNCTION_INFO_V1(bark_multikey_extract_value);
PG_FUNCTION_INFO_V1(bark_multikey_extract_marked);
PG_FUNCTION_INFO_V1(bark_multikey_meta);
PG_FUNCTION_INFO_V1(bark_multikey_entries);
PG_FUNCTION_INFO_V1(bark_multikey_has_marker);
PG_FUNCTION_INFO_V1(bark_multikey_mark_restore);
PG_FUNCTION_INFO_V1(bark_multikey_reverse);
PG_FUNCTION_INFO_V1(bark_multikey_walk);
PG_FUNCTION_INFO_V1(bark_multikey_rescan_fewer);
PG_FUNCTION_INFO_V1(bark_multikey_bare_scan);
PG_FUNCTION_INFO_V1(bark_multikey_parallel);
PG_FUNCTION_INFO_V1(bark_multikey_knn);
PG_FUNCTION_INFO_V1(bark_multikey_extract_query);
PG_FUNCTION_INFO_V1(bark_multikey_recheck);
PG_FUNCTION_INFO_V1(bark_multikey_translate_cmptype);
PG_FUNCTION_INFO_V1(bark_multikey_precheck);
PG_FUNCTION_INFO_V1(bark_multikey_least);
PG_FUNCTION_INFO_V1(bark_multikey_greatest);
PG_FUNCTION_INFO_V1(bark_multikey_keys);
PG_FUNCTION_INFO_V1(bark_multikey_boundaries);

static int
cmp_int4(const void *a, const void *b)
{
	return pg_cmp_s32(*(const int32 *) a, *(const int32 *) b);
}

/*
 * The array's non-NULL elements, sorted and distinct, in a palloc'd array;
 * *nkeys is their count.
 */
static int32 *
sorted_elements(ArrayType *array, int *nkeys)
{
	Datum	   *elems;
	bool	   *nulls;
	int			nelems;
	int32	   *keys;
	int			n = 0;

	deconstruct_array_builtin(array, INT4OID, &elems, &nulls, &nelems);
	keys = palloc_array(int32, Max(nelems, 1));
	for (int i = 0; i < nelems; i++)
	{
		if (!nulls[i])
			keys[n++] = DatumGetInt32(elems[i]);
	}
	qsort(keys, n, sizeof(int32), cmp_int4);
	*nkeys = qunique(keys, n, sizeof(int32), cmp_int4);
	return keys;
}

/*
 * Procedure 7: int4[] -> the array's elements, NULL elements flagged.  The
 * elements may repeat and come in any order; BARK sorts them and removes
 * duplicates.
 */
Datum
bark_multikey_extract_value(PG_FUNCTION_ARGS)
{
	ArrayType  *array = PG_GETARG_ARRAYTYPE_P(0);
	int32	   *nkeys = (int32 *) PG_GETARG_POINTER(1);
	bool	  **nullFlags = (bool **) PG_GETARG_POINTER(2);
	Datum	   *elems;
	bool	   *nulls;
	int			nelems;

	deconstruct_array_builtin(array, INT4OID, &elems, &nulls, &nelems);
	*nkeys = nelems;
	*nullFlags = nulls;
	PG_RETURN_POINTER(elems);
}

/*
 * The marker of the marked class: a key no element can equal, INT4_MIN
 * being reserved for it.  An element equal to it is refused.
 */
#define BARK_MK_ARRAY_MARKER	PG_INT32_MIN

/*
 * Procedure 7 of the marked class, with all five arguments: the elements,
 * as above, and the marker "the column has held an array of two or more
 * elements" for such an array, as a wildcard class marks a path that has
 * held an array.
 */
Datum
bark_multikey_extract_marked(PG_FUNCTION_ARGS)
{
	ArrayType  *array = PG_GETARG_ARRAYTYPE_P(0);
	int32	   *nkeys = (int32 *) PG_GETARG_POINTER(1);
	bool	  **nullFlags = (bool **) PG_GETARG_POINTER(2);
	Datum	  **markers = (Datum **) PG_GETARG_POINTER(3);
	int32	   *nmarkers = (int32 *) PG_GETARG_POINTER(4);
	Datum	   *elems;
	bool	   *nulls;
	int			nelems;

	deconstruct_array_builtin(array, INT4OID, &elems, &nulls, &nelems);
	for (int i = 0; i < nelems; i++)
	{
		if (!nulls[i] && DatumGetInt32(elems[i]) == BARK_MK_ARRAY_MARKER)
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("%d is reserved for the array marker",
							BARK_MK_ARRAY_MARKER)));
	}
	*nkeys = nelems;
	*nullFlags = nulls;
	if (nelems > 1)
	{
		*markers = palloc_object(Datum);
		(*markers)[0] = Int32GetDatum(BARK_MK_ARRAY_MARKER);
		*nmarkers = 1;
	}
	PG_RETURN_POINTER(elems);
}

/*
 * Procedure 8: a query and its strategy -> boundaries over the keys.
 *
 * && is exact: a row overlaps the query exactly when one of its keys is an
 * element of the query, so each element is a point boundary.  @>, <@ and =
 * need the whole array, so their points are candidates, rechecked: a row
 * that contains the query has each query element as a key, and a row that
 * is contained in or equal to it has only query elements as keys.  A NULL
 * query element matches no key.
 *
 * Rows whose only entry is the NULL one (a NULL or empty array, or one of
 * NULL elements only) are what searchnulls is for.  <@ needs them, since
 * the empty array is contained in every array, and so do @> '{}', which
 * every array satisfies (one unbounded boundary), and an = whose query has
 * no element to point at.  The ordering operators read every row: the whole
 * key range, ascending for |<| and descending for |>>|, and the NULL
 * entries, whose rows sort last as the operator's NULL does.
 */
Datum
bark_multikey_extract_query(PG_FUNCTION_ARGS)
{
	StrategyNumber strategy = PG_GETARG_UINT16(1);
	BarkBoundary **boundaries = (BarkBoundary **) PG_GETARG_POINTER(2);
	int32	   *nboundaries = (int32 *) PG_GETARG_POINTER(3);
	bool	   *recheck = (bool *) PG_GETARG_POINTER(5);
	BarkQueryFlags noflags;
	BarkQueryFlags *flags = &noflags;
	ArrayType  *query;
	int32	   *keys;
	int			nkeys;
	BarkBoundary *result;
	BarkSearchElement *points;
	Datum	   *qelems;
	bool	   *qnulls;

	/* Declared with six arguments, as for pg_extended_btree, it has none. */
	if (PG_NARGS() >= 7)
		flags = (BarkQueryFlags *) PG_GETARG_POINTER(6);
	*nboundaries = 0;
	*boundaries = NULL;
	*recheck = false;

	/* The ordering key's argument is |<|'s right operand, not an array. */
	if (strategy == BARK_MK_ORDER || strategy == BARK_MK_ORDER_DESC)
	{
		*boundaries = palloc0_object(BarkBoundary);
		*nboundaries = 1;
		flags->backward = (strategy == BARK_MK_ORDER_DESC);
		flags->searchnulls = true;
		PG_RETURN_VOID();
	}

	/*
	 * The range class.  |>| and |<=| are one half-bounded boundary, |=| a
	 * point, |><| a boundary closed below and open above, and |%| a point per
	 * query element whose entries procedure 9 decides.  |?| (no element above
	 * x) is every key up to x and the NULL entries, rechecked, as <@ is.
	 * |<>| (an element other than x) is the keys below x and those above it,
	 * in that order reversed.  |~| reads x as 10 * c + f: the keys from c - 2
	 * to c + 2, the lower end excluded when f has bit 1 and the upper end
	 * when f has bit 2, and no upper end when f has bit 4.  |!| misbehaves:
	 * for a positive argument its two boundaries intersect, for one below
	 * -100 its upper search element has a lower strategy, for another
	 * negative one its lower element has an upper strategy, and for 0
	 * procedure 9 gives an answer that is not one.
	 */
	if (strategy == BARK_MK_GT || strategy == BARK_MK_LE ||
		strategy == BARK_MK_HAS || strategy == BARK_MK_BROKEN ||
		strategy == BARK_MK_NONE_GT || strategy == BARK_MK_NE ||
		strategy == BARK_MK_NEAR)
	{
		int32		x = PG_GETARG_INT32(0);

		result = palloc0_array(BarkBoundary, 2);
		points = palloc_array(BarkSearchElement, 2);
		points[0].argument = Int32GetDatum(x);
		points[1].argument = Int32GetDatum(x + 1);
		*boundaries = result;
		*nboundaries = 1;
		if (strategy == BARK_MK_GT)
		{
			points[0].strategy = BTGreaterStrategyNumber;
			result[0].lower = &points[0];
		}
		else if (strategy == BARK_MK_LE)
		{
			points[0].strategy = BTLessEqualStrategyNumber;
			result[0].upper = &points[0];
		}
		else if (strategy == BARK_MK_HAS)
		{
			points[0].strategy = BTEqualStrategyNumber;
			result[0].lower = result[0].upper = &points[0];
		}
		else if (strategy == BARK_MK_NONE_GT)
		{
			points[0].strategy = BTLessEqualStrategyNumber;
			result[0].upper = &points[0];
			*recheck = true;
			flags->searchnulls = true;
		}
		else if (strategy == BARK_MK_NE)
		{
			points[0].strategy = BTGreaterStrategyNumber;
			points[1].argument = Int32GetDatum(x);
			points[1].strategy = BTLessStrategyNumber;
			result[0].lower = &points[0];
			result[1].upper = &points[1];
			*nboundaries = 2;
		}
		else if (strategy == BARK_MK_NEAR)
		{
			int32		c = x / 10;

			points[0].argument = Int32GetDatum(c - 2);
			points[0].strategy = (x % 10) & 1 ? BTGreaterStrategyNumber :
				BTGreaterEqualStrategyNumber;
			points[1].argument = Int32GetDatum(c + 2);
			points[1].strategy = (x % 10) & 2 ? BTLessStrategyNumber :
				BTLessEqualStrategyNumber;
			result[0].lower = &points[0];
			result[0].upper = (x % 10) & 4 ? NULL : &points[1];
		}
		else if (x > 0)
		{
			/* (-inf, x] and [x, +inf) share x */
			points[0].strategy = BTLessEqualStrategyNumber;
			points[1].argument = Int32GetDatum(x);
			points[1].strategy = BTGreaterEqualStrategyNumber;
			result[0].upper = &points[0];
			result[1].lower = &points[1];
			*nboundaries = 2;
		}
		else if (x < -100)
		{
			points[0].strategy = BTGreaterStrategyNumber;
			result[0].upper = &points[0];
		}
		else if (x < 0)
		{
			points[0].strategy = BTLessStrategyNumber;
			result[0].lower = &points[0];
		}
		else
		{
			points[0].strategy = BTEqualStrategyNumber;
			result[0].lower = result[0].upper = &points[0];
			*recheck = true;
		}
		PG_RETURN_VOID();
	}

	query = PG_GETARG_ARRAYTYPE_P(0);
	keys = sorted_elements(query, &nkeys);
	if (strategy == BARK_MK_WITHIN)
	{
		deconstruct_array_builtin(query, INT4OID, &qelems, &qnulls, &nkeys);
		if (nkeys < 2 || qnulls[0] || qnulls[1])
			PG_RETURN_VOID();
		result = palloc0_object(BarkBoundary);
		points = palloc_array(BarkSearchElement, 2);
		points[0].argument = qelems[0];
		points[0].strategy = BTGreaterEqualStrategyNumber;
		points[1].argument = qelems[1];
		points[1].strategy = BTLessStrategyNumber;
		result->lower = &points[0];
		result->upper = &points[1];
		*boundaries = result;
		*nboundaries = 1;
		PG_RETURN_VOID();
	}
	switch (strategy)
	{
		case BARK_MK_OVERLAP:
			break;
		case BARK_MK_CONTAINS:
			*recheck = true;
			if (ARR_HASNULL(query))
				PG_RETURN_VOID();	/* no array contains a NULL element */
			if (nkeys == 0)
			{
				*boundaries = palloc0_object(BarkBoundary);
				*nboundaries = 1;
				flags->searchnulls = true;
				PG_RETURN_VOID();
			}
			break;
		case BARK_MK_CONTAINED:
			*recheck = true;
			flags->searchnulls = true;
			break;
		case BARK_MK_EQUAL:
			*recheck = true;
			if (nkeys == 0)
				flags->searchnulls = true;
			break;
		case BARK_MK_EVEN_IN:
			*recheck = true;
			break;
		default:
			elog(ERROR, "bark_multikey_extract_query: unknown strategy number: %d",
				 strategy);
	}

	result = palloc_array(BarkBoundary, Max(nkeys, 1));
	points = palloc_array(BarkSearchElement, Max(nkeys, 1));
	for (int i = 0; i < nkeys; i++)
	{
		points[i].argument = Int32GetDatum(keys[i]);
		points[i].strategy = BTEqualStrategyNumber;
		result[i].lower = &points[i];
		result[i].upper = &points[i];
	}
	*boundaries = result;
	*nboundaries = nkeys;
	PG_RETURN_VOID();
}

/*
 * Procedure 9: one entry's key, for a strategy whose boundaries asked for a
 * recheck.  A single element cannot decide @>, <@ or = for the row, so the
 * answer is maybe, which leaves the row to the executor's recheck.  An
 * element of |%|'s query is a match exactly when it is even.
 */
Datum
bark_multikey_recheck(PG_FUNCTION_ARGS)
{
	int32		key = PG_GETARG_INT32(0);
	StrategyNumber strategy = PG_GETARG_UINT16(1);

	if (strategy == BARK_MK_EVEN_IN)
		PG_RETURN_INT16(key % 2 == 0 ? BARK_RECHECK_TRUE : BARK_RECHECK_FALSE);
	if (strategy == BARK_MK_NONE_GT)
		PG_RETURN_INT16(BARK_RECHECK_MAYBE);
	if (strategy == BARK_MK_BROKEN)
		PG_RETURN_INT16(7);
	if (strategy == BARK_MK_OVERLAP || strategy == BARK_MK_ORDER)
		PG_RETURN_INT16(BARK_RECHECK_TRUE);
	PG_RETURN_INT16(BARK_RECHECK_MAYBE);
}

/*
 * Procedure 15: the overlap operator, |&&|, is strategy 1, which procedure 8
 * reads as && (both are overlap over int4[]).  No other compare type has a
 * strategy.
 */
Datum
bark_multikey_translate_cmptype(PG_FUNCTION_ARGS)
{
	CompareType cmptype = PG_GETARG_INT32(0);

	PG_RETURN_UINT16(cmptype == COMPARE_OVERLAP ? BARK_MK_OVERLAP :
					 InvalidStrategy);
}

/*
 * Run the executor's conflict pre-check (check_exclusion_constraint, with
 * BuildSpeculativeIndexInfo's operators) on index for the row at tid,
 * passing the row's own TID as logical replication's FindConflictTuple
 * does.  The scan must return that row once, or the executor raises "found
 * self tuple multiple times"; another row that conflicts raises the
 * violation.  Returns "ok".
 */
Datum
bark_multikey_precheck(PG_FUNCTION_ARGS)
{
	Relation	index = index_open(PG_GETARG_OID(0), AccessShareLock);
	ItemPointer tid = PG_GETARG_ITEMPOINTER(1);
	Relation	heap = table_open(index->rd_index->indrelid, AccessShareLock);
	IndexInfo  *ii = BuildIndexInfo(index);
	EState	   *estate = CreateExecutorState();
	TupleTableSlot *slot = table_slot_create(heap, NULL);
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];

	if (!table_tuple_fetch_row_version(heap, tid, GetActiveSnapshot(), slot))
		elog(ERROR, "no visible row at (%u,%u)",
			 ItemPointerGetBlockNumber(tid), ItemPointerGetOffsetNumber(tid));
	BuildSpeculativeIndexInfo(index, ii);
	GetPerTupleExprContext(estate)->ecxt_scantuple = slot;
	FormIndexDatum(ii, slot, estate, values, isnull);
	check_exclusion_constraint(heap, index, ii, tid, values, isnull, estate,
							   false);

	ExecDropSingleTupleTableSlot(slot);
	FreeExecutorState(estate);
	table_close(heap, AccessShareLock);
	index_close(index, AccessShareLock);
	PG_RETURN_TEXT_P(cstring_to_text("ok"));
}

/*
 * int4[] |<| int4: the array's least non-NULL element, or NULL when it has
 * none.  The right operand is there only because an index operator must be
 * binary; its value is not used.
 */
Datum
bark_multikey_least(PG_FUNCTION_ARGS)
{
	int32	   *keys;
	int			nkeys;

	keys = sorted_elements(PG_GETARG_ARRAYTYPE_P(0), &nkeys);
	if (nkeys == 0)
		PG_RETURN_NULL();
	PG_RETURN_INT32(keys[0]);
}

/*
 * int4[] |>>| int4: the array's greatest non-NULL element, or NULL when it
 * has none.  Its sort family orders int4 descending.
 */
Datum
bark_multikey_greatest(PG_FUNCTION_ARGS)
{
	int32	   *keys;
	int			nkeys;

	keys = sorted_elements(PG_GETARG_ARRAYTYPE_P(0), &nkeys);
	if (nkeys == 0)
		PG_RETURN_NULL();
	PG_RETURN_INT32(keys[nkeys - 1]);
}

/*
 * The support procedure procnum of operator class opclass, registered under
 * its input type, as BARK looks it up.
 */
static Oid
class_proc(Oid opclass, int procnum)
{
	Oid			opfamily;
	Oid			opcintype;
	Oid			proc;

	if (!get_opclass_opfamily_and_input_type(opclass, &opfamily, &opcintype))
		elog(ERROR, "cache lookup failed for operator class %u", opclass);
	proc = get_opfamily_proc(opfamily, opcintype, opcintype, procnum);
	if (!OidIsValid(proc))
		elog(ERROR, "operator class %u has no support function %d",
			 opclass, procnum);
	return proc;
}

/*
 * bark_multikey_keys(opclass, value) -> text: the keys procedure 7 of the
 * class returns for value, in the order it returns them.
 */
Datum
bark_multikey_keys(PG_FUNCTION_ARGS)
{
	Oid			proc = class_proc(PG_GETARG_OID(0), BARK_EXTRACTVALUE_PROC);
	int32		nkeys = 0;
	bool	   *nulls = NULL;
	Datum	   *keys;
	StringInfoData buf;

	keys = (Datum *) DatumGetPointer(OidFunctionCall3(proc, PG_GETARG_DATUM(1),
													  PointerGetDatum(&nkeys),
													  PointerGetDatum(&nulls)));
	initStringInfo(&buf);
	appendStringInfoChar(&buf, '{');
	for (int i = 0; i < nkeys; i++)
	{
		if (i > 0)
			appendStringInfoChar(&buf, ',');
		if (nulls && nulls[i])
			appendStringInfoString(&buf, "NULL");
		else
			appendStringInfo(&buf, "%d", DatumGetInt32(keys[i]));
	}
	appendStringInfoChar(&buf, '}');
	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

static void
append_element(StringInfo buf, BarkSearchElement *elem, bool lower)
{
	const char *op;

	if (elem == NULL)
	{
		appendStringInfoString(buf, lower ? "-inf" : "+inf");
		return;
	}
	switch (elem->strategy)
	{
		case BTLessStrategyNumber:
			op = "<";
			break;
		case BTLessEqualStrategyNumber:
			op = "<=";
			break;
		case BTEqualStrategyNumber:
			op = "=";
			break;
		case BTGreaterEqualStrategyNumber:
			op = ">=";
			break;
		case BTGreaterStrategyNumber:
			op = ">";
			break;
		default:
			op = "?";
			break;
	}
	appendStringInfo(buf, "%s%d", op, DatumGetInt32(elem->argument));
}

/*
 * bark_multikey_boundaries(opclass, query, strategy) -> text: what
 * procedure 8 of the class returns for the query, with BarkQueryFlags
 * zeroed first as BARK zeroes them, and, when it asks for a recheck,
 * procedure 9's answer for the first boundary's key.
 */
Datum
bark_multikey_boundaries(PG_FUNCTION_ARGS)
{
	Oid			opclass = PG_GETARG_OID(0);
	int16		strategy = PG_GETARG_INT16(2);
	BarkBoundary *boundaries = NULL;
	int32		nboundaries = 0;
	void	   *extra = NULL;
	bool		recheck = false;
	BarkQueryFlags flags;
	StringInfoData buf;

	memset(&flags, 0, sizeof(flags));
	OidFunctionCall7Coll(class_proc(opclass, BARK_EXTRACTQUERY_PROC),
						 InvalidOid, PG_GETARG_DATUM(1),
						 Int16GetDatum(strategy),
						 PointerGetDatum(&boundaries),
						 PointerGetDatum(&nboundaries),
						 PointerGetDatum(&extra),
						 PointerGetDatum(&recheck),
						 PointerGetDatum(&flags));

	initStringInfo(&buf);
	for (int i = 0; i < nboundaries; i++)
	{
		appendStringInfoChar(&buf, '[');
		append_element(&buf, boundaries[i].lower, true);
		appendStringInfoChar(&buf, ',');
		append_element(&buf, boundaries[i].upper, false);
		appendStringInfoString(&buf, "] ");
	}
	appendStringInfo(&buf, "recheck=%s searchnulls=%s backward=%s",
					 recheck ? "t" : "f",
					 flags.searchnulls ? "t" : "f",
					 flags.backward ? "t" : "f");
	if (recheck && nboundaries > 0 && boundaries[0].lower != NULL)
	{
		int16		answer;

		answer = DatumGetInt16(OidFunctionCall3(class_proc(opclass,
														   BARK_INDEXRECHECK_PROC),
												boundaries[0].lower->argument,
												Int16GetDatum(strategy),
												PointerGetDatum(extra)));
		appendStringInfo(&buf, " proc9=%s",
						 answer == BARK_RECHECK_TRUE ? "true" :
						 answer == BARK_RECHECK_MAYBE ? "maybe" : "false");
	}
	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

/* Open a BARK index by name, for the functions below. */
static Relation
open_bark_index(text *relname)
{
	RangeVar   *rv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
	Relation	rel = relation_openrv(rv, AccessShareLock);

	if (!bark_relation_is_bark(rel))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a BARK index",
						RelationGetRelationName(rel))));
	return rel;
}

/*
 * bark_multikey_meta(index) -> (multikey bool, nkeys int8): the meta page's
 * BARK_META_MULTIKEY flag and bark_nkeys.
 */
Datum
bark_multikey_meta(PG_FUNCTION_ARGS)
{
	Relation	rel = open_bark_index(PG_GETARG_TEXT_PP(0));
	Buffer		buf = ReadBuffer(rel, BARK_METAPAGE);
	BarkMetaPageData *meta;
	TupleDesc	tupdesc;
	Datum		values[2];
	bool		nulls[2] = {0};

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	meta = BarkPageGetMeta(BufferGetPage(buf));
	values[0] = BoolGetDatum((meta->bark_flags & BARK_META_MULTIKEY) != 0);
	values[1] = Int64GetDatum((int64) meta->bark_nkeys);
	UnlockReleaseBuffer(buf);
	relation_close(rel, AccessShareLock);
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values,
													  nulls)));
}

/*
 * bark_multikey_entries(index) -> setof (key int4, tid tid, marker bool):
 * every (key, heap TID) member of the int4 key column 1 of a BARK index, in
 * index order, walking the leaves left to right.  A NULL key is returned as
 * NULL.  Scans of a multikey index are refused until they are implemented,
 * so this is how the tests compare what is stored with what should be.
 */
Datum
bark_multikey_entries(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Relation	rel = open_bark_index(PG_GETARG_TEXT_PP(0));
	TupleDesc	itupdesc = RelationGetDescr(rel);
	BlockNumber blkno;
	Buffer		buf;

	InitMaterializedSRF(fcinfo, 0);
	if (TupleDescAttr(itupdesc, 0)->atttypid != INT4OID)
		elog(ERROR, "bark_multikey_entries needs an int4 first key column");

	/* The leftmost leaf: follow first downlinks from the root. */
	blkno = bark_get_root(rel, NULL);
	if (blkno == BARK_P_NONE)
	{
		relation_close(rel, AccessShareLock);
		return (Datum) 0;
	}
	for (;;)
	{
		Page		page;
		BarkPageOpaque opaque;
		BarkItemBuf ibuf;

		buf = ReadBuffer(rel, blkno);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		opaque = BarkPageGetOpaque(page);
		if (BarkPageIsLeaf(opaque))
			break;
		blkno = BarkEntryGetDownLink(BarkPageGetItem(page,
													 BarkPageFirstDataKey(opaque),
													 &ibuf));
		UnlockReleaseBuffer(buf);
	}

	for (;;)
	{
		Page		page = BufferGetPage(buf);
		BarkPageOpaque opaque = BarkPageGetOpaque(page);

		if (!BarkPageIgnore(opaque))
		{
			for (OffsetNumber off = BarkPageFirstDataKey(opaque);
				 off <= PageGetMaxOffsetNumber(page); off = OffsetNumberNext(off))
			{
				BarkItemBuf ibuf;
				IndexTuple	itup = BarkPageGetItem(page, off, &ibuf);
				IndexTuple	full = itup;
				int			ntids = bark_entry_count_tids(itup);
				ItemPointer tids = palloc_array(ItemPointerData, ntids);
				Datum		key;
				bool		keynull;

				if (BarkEntryGetShape(itup) == BARK_SHAPE_OVERSIZED)
					full = bark_fetch_oversized(rel, itup);
				key = index_getattr(full, 1, itupdesc, &keynull);
				ntids = bark_entry_get_tids(itup, tids, ntids,
											bark_index_tid_stride(rel));
				for (int i = 0; i < ntids; i++)
				{
					Datum		values[3];
					bool		nulls[3] = {0};

					values[0] = key;
					nulls[0] = keynull;
					values[1] = ItemPointerGetDatum(&tids[i]);
					values[2] = BoolGetDatum(BarkTidIsMarker(&tids[i]));
					tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
										 values, nulls);
				}
				pfree(tids);
				if (full != itup)
					pfree(full);
			}
		}
		if (BarkPageRightmost(opaque))
			break;
		blkno = opaque->bark_next;
		UnlockReleaseBuffer(buf);
		buf = ReadBuffer(rel, blkno);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
	}
	UnlockReleaseBuffer(buf);
	relation_close(rel, AccessShareLock);
	return (Datum) 0;
}

/*
 * bark_multikey_has_marker(index, key int4) -> bool: is the marker key in
 * the index (bark_index_has_marker)?
 */
Datum
bark_multikey_has_marker(PG_FUNCTION_ARGS)
{
	Relation	rel = open_bark_index(PG_GETARG_TEXT_PP(0));
	bool		found = bark_index_has_marker(rel, PG_GETARG_DATUM(1));

	relation_close(rel, AccessShareLock);
	PG_RETURN_BOOL(found);
}

/*
 * Read up to max heap TIDs from an index scan in TID order of return.
 */
static int
read_tids(IndexScanDesc scan, ItemPointer tids, int max)
{
	int			n = 0;

	while (n < max && tableam_index_getnext_tid(scan, ForwardScanDirection))
		tids[n++] = scan->xs_heaptid;
	return n;
}

/*
 * bark_multikey_walk(index, op, query, steps) -> text: scan index with one
 * key, column 1 op query, taking one step per character of steps, 'f'
 * forward and 'b' backward, without asking whether the index scans
 * backward.  The answer has a word per step: the TID read, or "-".  A
 * leading 'a' scans with SnapshotAny instead of the active snapshot, as a
 * caller of the AM with a non-MVCC snapshot would; 'o' adds an ordering
 * key, column 1 op query, that the AM did not ask for.
 */
Datum
bark_multikey_walk(PG_FUNCTION_ARGS)
{
	Relation	index = open_bark_index(PG_GETARG_TEXT_PP(0));
	Oid			opno = PG_GETARG_OID(1);
	Datum		query = PG_GETARG_DATUM(2);
	char	   *steps = text_to_cstring(PG_GETARG_TEXT_PP(3));
	Relation	heap = table_open(index->rd_index->indrelid, AccessShareLock);
	StringInfoData buf;
	int			strategy;
	Oid			lefttype;
	Oid			righttype;
	ScanKeyData key;
	ScanKeyData ob;
	IndexScanDesc scan;
	Snapshot	snapshot = GetActiveSnapshot();
	int			norderbys = 0;

	if (*steps == 'a')
	{
		snapshot = SnapshotAny;
		steps++;
	}
	if (*steps == 'o')
	{
		norderbys = 1;
		steps++;
	}
	get_op_opfamily_properties(opno, index->rd_opfamily[0], false, &strategy,
							   &lefttype, &righttype);
	ScanKeyEntryInitialize(&key, 0, 1, strategy, righttype,
						   index->rd_indcollation[0], get_opcode(opno), query);
	ScanKeyEntryInitialize(&ob, SK_ORDER_BY, 1, strategy, righttype,
						   index->rd_indcollation[0], get_opcode(opno), query);
	scan = index_beginscan(heap, index, false, snapshot, NULL, 1, norderbys,
						   0);
	index_rescan(scan, &key, 1, norderbys ? &ob : NULL, norderbys);
	initStringInfo(&buf);
	for (char *c = steps; *c; c++)
	{
		ScanDirection dir = *c == 'b' ? BackwardScanDirection :
			ForwardScanDirection;

		if (buf.len > 0)
			appendStringInfoChar(&buf, ' ');
		if (tableam_index_getnext_tid(scan, dir))
			appendStringInfo(&buf, "(%u,%u)",
							 ItemPointerGetBlockNumber(&scan->xs_heaptid),
							 ItemPointerGetOffsetNumber(&scan->xs_heaptid));
		else
			appendStringInfoChar(&buf, '-');
	}
	index_endscan(scan);

	table_close(heap, AccessShareLock);
	relation_close(index, AccessShareLock);
	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

/*
 * bark_multikey_rescan_fewer(index, op, query) -> text: ask the AM for a
 * scan with two key slots, then call its amrescan DIRECTLY with ONE key and
 * nscankeys = 1, as DocumentDB's composite-scan fold does (pg_documentdb's
 * rum.c folds several planner keys into one BSON_INDEX_STRATEGY_COMPOSITE_QUERY
 * key and calls coreRoutine->amrescan(scan, &key, 1, ...), bypassing
 * index_rescan, while scan->numberOfKeys still holds the beginscan count).
 * BARK must believe the argument, not scan->numberOfKeys, or it would read a
 * second, uninitialized key slot and return wrong rows or error.  The answer
 * is true when the folded scan reads exactly the same TID multiset, in the
 * same order, as a plain one-key index_rescan of the qual.  index_rescan
 * cannot stand in for the fold here: it asserts nscankeys == numberOfKeys.
 */
Datum
bark_multikey_rescan_fewer(PG_FUNCTION_ARGS)
{
	Relation	index = open_bark_index(PG_GETARG_TEXT_PP(0));
	Oid			opno = PG_GETARG_OID(1);
	Datum		query = PG_GETARG_DATUM(2);
	Relation	heap = table_open(index->rd_index->indrelid, AccessShareLock);
	int			strategy;
	Oid			lefttype;
	Oid			righttype;
	ScanKeyData key;
	IndexScanDesc scan;
	ItemPointer tids;
	ItemPointer tids2;
	int			cap = 1024;
	int			n;
	int			n2;
	bool		same;

	if (heap->rd_rel->reltuples > cap)
		cap = (int) heap->rd_rel->reltuples + 1;
	tids = palloc(cap * sizeof(ItemPointerData));
	tids2 = palloc(cap * sizeof(ItemPointerData));
	get_op_opfamily_properties(opno, index->rd_opfamily[0], false, &strategy,
							   &lefttype, &righttype);
	ScanKeyEntryInitialize(&key, 0, 1, strategy, righttype,
						   index->rd_indcollation[0], get_opcode(opno), query);

	/*
	 * The fold: ask beginscan for two key slots (scan->numberOfKeys becomes
	 * 2), then call the AM's amrescan with a single key.  The second slot is
	 * left as index_beginscan palloc0'd it; if BARK trusted numberOfKeys it
	 * would process that empty key and read the wrong rows.
	 */
	scan = index_beginscan(heap, index, false, GetActiveSnapshot(), NULL,
						   2, 0, 0);
	index->rd_indam->amrescan(scan, &key, 1, NULL, 0);
	n = read_tids(scan, tids, cap);
	index_endscan(scan);

	/* The plain one-key scan of the same qual. */
	scan = index_beginscan(heap, index, false, GetActiveSnapshot(), NULL,
						   1, 0, 0);
	index_rescan(scan, &key, 1, NULL, 0);
	n2 = read_tids(scan, tids2, cap);
	index_endscan(scan);

	same = (n == n2);
	for (int i = 0; same && i < n; i++)
		same = ItemPointerEquals(&tids[i], &tids2[i]);

	table_close(heap, AccessShareLock);
	relation_close(index, AccessShareLock);
	PG_RETURN_BOOL(same);
}

/*
 * bark_multikey_bare_scan(index, op, query) -> int: count the TIDs a scan of
 * index with one key, column 1 op query, returns when it is driven through
 * the AM's routine alone -- ambeginscan, amrescan, amgettuple, amendscan --
 * with no heap relation and no snapshot, as DocumentDB asks a RUM index
 * whether a metadata key is present (rum.c's CheckIndexHasArrays).
 */
Datum
bark_multikey_bare_scan(PG_FUNCTION_ARGS)
{
	Relation	index = open_bark_index(PG_GETARG_TEXT_PP(0));
	Oid			opno = PG_GETARG_OID(1);
	Datum		query = PG_GETARG_DATUM(2);
	const IndexAmRoutine *am = index->rd_indam;
	int			strategy;
	Oid			lefttype;
	Oid			righttype;
	ScanKeyData key;
	IndexScanDesc scan;
	int			n = 0;

	get_op_opfamily_properties(opno, index->rd_opfamily[0], false, &strategy,
							   &lefttype, &righttype);
	ScanKeyEntryInitialize(&key, 0, 1, strategy, righttype,
						   index->rd_indcollation[0], get_opcode(opno), query);
	scan = am->ambeginscan(index, 1, 0);
	scan->parallel_scan = NULL;
	am->amrescan(scan, &key, 1, NULL, 0);
	while (am->amgettuple(scan, ForwardScanDirection))
		n++;
	am->amendscan(scan);
	relation_close(index, AccessShareLock);
	PG_RETURN_INT32(n);
}

/*
 * bark_multikey_mark_restore(index, op, query, mark_at, read_after) -> text:
 * scan index with one key, column 1 op query, and read every TID; then scan
 * again, mark after mark_at TIDs (0: before the first), read read_after
 * more, restore, and read the rest, as a merge join does.  The answer is
 * "total:<n> resumed:<m> same:<bool>": n TIDs in the plain scan, m read
 * after the restore, and whether those are the plain scan's from mark_at
 * on.
 */
Datum
bark_multikey_mark_restore(PG_FUNCTION_ARGS)
{
	Relation	index = open_bark_index(PG_GETARG_TEXT_PP(0));
	Oid			opno = PG_GETARG_OID(1);
	Datum		query = PG_GETARG_DATUM(2);
	int			mark_at = PG_GETARG_INT32(3);
	int			read_after = PG_GETARG_INT32(4);
	Relation	heap = table_open(index->rd_index->indrelid, AccessShareLock);
	int			max = 1000000;
	ItemPointer all = palloc_array(ItemPointerData, max);
	ItemPointer resumed = palloc_array(ItemPointerData, max);
	int			nall;
	int			nresumed;
	int			strategy;
	Oid			lefttype;
	Oid			righttype;
	ScanKeyData key;
	IndexScanDesc scan;
	bool		same;

	get_op_opfamily_properties(opno, index->rd_opfamily[0], false, &strategy,
							   &lefttype, &righttype);
	ScanKeyEntryInitialize(&key, 0, 1, strategy, righttype,
						   index->rd_indcollation[0], get_opcode(opno), query);

	scan = index_beginscan(heap, index, false, GetActiveSnapshot(), NULL, 1, 0,
						   0);
	index_rescan(scan, &key, 1, NULL, 0);
	nall = read_tids(scan, all, max);
	index_endscan(scan);

	scan = index_beginscan(heap, index, false, GetActiveSnapshot(), NULL, 1, 0,
						   0);
	index_rescan(scan, &key, 1, NULL, 0);
	(void) read_tids(scan, resumed, mark_at);
	index_markpos(scan);
	(void) read_tids(scan, resumed, read_after);
	index_restrpos(scan);
	nresumed = read_tids(scan, resumed, max);
	index_endscan(scan);

	same = nresumed == nall - mark_at;
	for (int i = 0; same && i < nresumed; i++)
		same = ItemPointerEquals(&resumed[i], &all[mark_at + i]);

	table_close(heap, AccessShareLock);
	relation_close(index, AccessShareLock);
	PG_RETURN_TEXT_P(cstring_to_text(psprintf("total:%d resumed:%d same:%s",
											  nall, nresumed,
											  same ? "true" : "false")));
}

/*
 * bark_multikey_reverse(index, op, query, n) -> text: scan index with one
 * key, column 1 op query, read n TIDs forward and then one backward, as a
 * caller would that does not ask IndexSupportsBackwardScan.  The answer is
 * "forward:<n> back:<bool>": the TIDs read forward, and whether the
 * backward step returned the one before the last of them, as a scroll
 * cursor expects.  A scan that keeps a seen set raises an error instead.
 */
Datum
bark_multikey_reverse(PG_FUNCTION_ARGS)
{
	Relation	index = open_bark_index(PG_GETARG_TEXT_PP(0));
	Oid			opno = PG_GETARG_OID(1);
	Datum		query = PG_GETARG_DATUM(2);
	int			n = Max(PG_GETARG_INT32(3), 2);
	Relation	heap = table_open(index->rd_index->indrelid, AccessShareLock);
	ItemPointer tids = palloc_array(ItemPointerData, n);
	int			nread;
	bool		back;
	int			strategy;
	Oid			lefttype;
	Oid			righttype;
	ScanKeyData key;
	IndexScanDesc scan;

	get_op_opfamily_properties(opno, index->rd_opfamily[0], false, &strategy,
							   &lefttype, &righttype);
	ScanKeyEntryInitialize(&key, 0, 1, strategy, righttype,
						   index->rd_indcollation[0], get_opcode(opno), query);
	scan = index_beginscan(heap, index, false, GetActiveSnapshot(), NULL, 1, 0,
						   0);
	index_rescan(scan, &key, 1, NULL, 0);
	nread = read_tids(scan, tids, n);
	back = nread >= 2 &&
		tableam_index_getnext_tid(scan, BackwardScanDirection) &&
		ItemPointerEquals(&scan->xs_heaptid, &tids[nread - 2]);
	index_endscan(scan);

	table_close(heap, AccessShareLock);
	relation_close(index, AccessShareLock);
	PG_RETURN_TEXT_P(cstring_to_text(psprintf("forward:%d back:%s", nread,
											  back ? "true" : "false")));
}

/*
 * bark_multikey_parallel(index, op, query) -> int: scan index with one key,
 * column 1 op query, as the only participant of a parallel scan, and count
 * the TIDs, as a caller would that does not ask amcanparallel.  A scan that
 * keeps a seen set raises an error instead.
 */
Datum
bark_multikey_parallel(PG_FUNCTION_ARGS)
{
	Relation	index = open_bark_index(PG_GETARG_TEXT_PP(0));
	Oid			opno = PG_GETARG_OID(1);
	Datum		query = PG_GETARG_DATUM(2);
	Relation	heap = table_open(index->rd_index->indrelid, AccessShareLock);
	Snapshot	snapshot = GetActiveSnapshot();
	ParallelIndexScanDesc pscan;
	int			strategy;
	Oid			lefttype;
	Oid			righttype;
	ScanKeyData key;
	IndexScanDesc scan;
	int64		n = 0;

	get_op_opfamily_properties(opno, index->rd_opfamily[0], false, &strategy,
							   &lefttype, &righttype);
	ScanKeyEntryInitialize(&key, 0, 1, strategy, righttype,
						   index->rd_indcollation[0], get_opcode(opno), query);
	pscan = palloc0(index_parallelscan_estimate(index, 1, 0, snapshot));
	index_parallelscan_initialize(heap, index, snapshot, pscan);
	scan = index_beginscan_parallel(heap, index, false, NULL, 1, 0, pscan, 0);
	index_rescan(scan, &key, 1, NULL, 0);
	while (tableam_index_getnext_tid(scan, ForwardScanDirection))
		n++;
	index_endscan(scan);

	table_close(heap, AccessShareLock);
	relation_close(index, AccessShareLock);
	PG_RETURN_INT64(n);
}

/*
 * bark_multikey_knn(index, cols, ops, args, ordcol, ordop, center) -> setof
 * (tid, dist, recheck): an ordered scan of index with the scan keys and the
 * one ordering key the caller gives, passed to the index AM as given, so
 * that keys the planner never pairs with an ordering key can be tried.  Key
 * i is column cols[i] ops[i] args[i], args[i] read as the operator's right
 * type (the column's type for a polymorphic one), an array of it (a
 * ScalarArrayOp key) when it is not an array type and args[i] starts with
 * '{', and NULL for a NULL argument; a NULL ops[i] is a NULL test, IS NULL
 * when args[i] is 'null' and IS NOT NULL otherwise.  The ordering key is
 * column ordcol ordop center.  The scan is rescanned once before it is
 * read, as a nested loop rescans its inner side.  Every TID comes back in
 * the order returned, with its distance and whether the executor would
 * recheck it.
 */
Datum
bark_multikey_knn(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Relation	index = open_bark_index(PG_GETARG_TEXT_PP(0));
	AttrNumber	ordcol = PG_GETARG_INT32(4);
	Oid			ordop = PG_GETARG_OID(5);
	Relation	heap = table_open(index->rd_index->indrelid, AccessShareLock);
	Datum	   *cols;
	Datum	   *ops;
	Datum	   *args;
	bool	   *colnulls;
	bool	   *opnulls;
	bool	   *argnulls;
	int			nkeys;
	int			nops;
	int			nargs;
	ScanKey		keys;
	ScanKeyData ob;
	int			strategy;
	Oid			lefttype;
	Oid			righttype;
	IndexScanDesc scan;

	InitMaterializedSRF(fcinfo, 0);
	deconstruct_array_builtin(PG_GETARG_ARRAYTYPE_P(1), INT2OID, &cols,
							  &colnulls, &nkeys);
	deconstruct_array(PG_GETARG_ARRAYTYPE_P(2), REGOPERATOROID,
					  sizeof(Oid), true, TYPALIGN_INT, &ops, &opnulls, &nops);
	deconstruct_array_builtin(PG_GETARG_ARRAYTYPE_P(3), TEXTOID, &args,
							  &argnulls, &nargs);
	if (nops != nkeys || nargs != nkeys)
		elog(ERROR, "cols, ops and args must have the same length");
	keys = palloc0_array(ScanKeyData, Max(nkeys, 1));
	for (int i = 0; i < nkeys; i++)
	{
		AttrNumber	col = DatumGetInt16(cols[i]);
		Oid			opno;
		Oid			typinput;
		Oid			typioparam;
		char	   *arg;
		bool		saop;

		if (opnulls[i])
		{
			bool		isnull = !argnulls[i] &&
				strcmp(TextDatumGetCString(args[i]), "null") == 0;

			ScanKeyEntryInitialize(&keys[i], SK_ISNULL |
								   (isnull ? SK_SEARCHNULL : SK_SEARCHNOTNULL),
								   col, InvalidStrategy, InvalidOid,
								   InvalidOid, InvalidOid, (Datum) 0);
			continue;
		}
		opno = DatumGetObjectId(ops[i]);
		get_op_opfamily_properties(opno, index->rd_opfamily[col - 1], false,
								   &strategy, &lefttype, &righttype);
		if (IsPolymorphicType(righttype))
			righttype = index->rd_opcintype[col - 1];
		arg = argnulls[i] ? NULL : TextDatumGetCString(args[i]);
		saop = arg != NULL && arg[0] == '{' && !type_is_array(righttype);
		ScanKeyEntryInitialize(&keys[i], (arg == NULL ? SK_ISNULL : 0) |
							   (saop ? SK_SEARCHARRAY : 0), col,
							   strategy, righttype,
							   index->rd_indcollation[col - 1],
							   get_opcode(opno), (Datum) 0);
		if (arg != NULL)
		{
			getTypeInputInfo(saop ? get_array_type(righttype) : righttype,
							 &typinput, &typioparam);
			keys[i].sk_argument = OidInputFunctionCall(typinput, arg,
													   typioparam, -1);
		}
	}
	get_op_opfamily_properties(ordop, index->rd_opfamily[ordcol - 1], true,
							   &strategy, &lefttype, &righttype);
	ScanKeyEntryInitialize(&ob, SK_ORDER_BY, ordcol, strategy, righttype,
						   index->rd_indcollation[ordcol - 1],
						   get_opcode(ordop), PG_GETARG_DATUM(6));

	scan = index_beginscan(heap, index, false, GetActiveSnapshot(), NULL,
						   nkeys, 1, 0);
	index_rescan(scan, keys, nkeys, &ob, 1);
	index_rescan(scan, keys, nkeys, &ob, 1);
	while (tableam_index_getnext_tid(scan, ForwardScanDirection))
	{
		Datum		values[3];
		bool		nulls[3] = {0};

		values[0] = ItemPointerGetDatum(&scan->xs_heaptid);
		values[1] = scan->xs_orderbyvals[0];
		nulls[1] = scan->xs_orderbynulls[0];
		values[2] = BoolGetDatum(scan->xs_recheck);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values,
							 nulls);
	}
	index_endscan(scan);

	table_close(heap, AccessShareLock);
	relation_close(index, AccessShareLock);
	return (Datum) 0;
}

/*
 * GIN-style procedures for a prefix class over text[] (bark_text_prefix_ops):
 * extractQuery returns the query text as one partial-match key, consistent
 * accepts a row that has it.  comparePartial is core's gin_cmp_prefix.
 */
PG_FUNCTION_INFO_V1(bark_multikey_prefix_query);
PG_FUNCTION_INFO_V1(bark_multikey_prefix_consistent);

Datum
bark_multikey_prefix_query(PG_FUNCTION_ARGS)
{
	int32	   *nkeys = (int32 *) PG_GETARG_POINTER(1);
	bool	  **pmatch = (bool **) PG_GETARG_POINTER(3);
	Datum	   *keys = palloc_object(Datum);

	keys[0] = PointerGetDatum(PG_GETARG_TEXT_P_COPY(0));
	*nkeys = 1;
	*pmatch = palloc_object(bool);
	(*pmatch)[0] = true;
	PG_RETURN_POINTER(keys);
}

Datum
bark_multikey_prefix_consistent(PG_FUNCTION_ARGS)
{
	bool	   *check = (bool *) PG_GETARG_POINTER(0);
	bool	   *recheck = (bool *) PG_GETARG_POINTER(5);

	*recheck = false;
	PG_RETURN_BOOL(check[0]);
}

/*
 * An array similarity class over int4[], after RUM's rum_anyarray_ops:
 * a % b when the similarity of their sets of non-NULL elements,
 * |a & b| / sqrt(|a| |b|), is at least 0.5, and a <=> b is 1 minus it.  An
 * empty set is similar to nothing.  The class is GIN-interface: a row
 * matching m of the query's q keys has at least m elements, so its
 * similarity is at most sqrt(m / q), and % needs 4 m >= q; the heap
 * recheck applies the exact test.
 */
PG_FUNCTION_INFO_V1(bark_multikey_sim_match);
PG_FUNCTION_INFO_V1(bark_multikey_sim_distance);
PG_FUNCTION_INFO_V1(bark_multikey_sim_query);
PG_FUNCTION_INFO_V1(bark_multikey_sim_consistent);
PG_FUNCTION_INFO_V1(bark_multikey_sim_triconsistent);

/* |a & b|, |a| and |b| over the arrays' distinct non-NULL elements. */
static void
sim_counts(ArrayType *a, ArrayType *b, int *common, int *na, int *nb)
{
	int32	   *ka = sorted_elements(a, na);
	int32	   *kb = sorted_elements(b, nb);
	int			i = 0,
				j = 0;

	*common = 0;
	while (i < *na && j < *nb)
	{
		if (ka[i] < kb[j])
			i++;
		else if (ka[i] > kb[j])
			j++;
		else
		{
			(*common)++;
			i++;
			j++;
		}
	}
}

Datum
bark_multikey_sim_match(PG_FUNCTION_ARGS)
{
	int			m,
				na,
				nb;

	sim_counts(PG_GETARG_ARRAYTYPE_P(0), PG_GETARG_ARRAYTYPE_P(1),
			   &m, &na, &nb);
	/* m / sqrt(na nb) >= 1/2, in integers so that 0.5 itself is exact */
	PG_RETURN_BOOL(m > 0 && 4 * (int64) m * m >= (int64) na * nb);
}

Datum
bark_multikey_sim_distance(PG_FUNCTION_ARGS)
{
	int			m,
				na,
				nb;

	sim_counts(PG_GETARG_ARRAYTYPE_P(0), PG_GETARG_ARRAYTYPE_P(1),
			   &m, &na, &nb);
	PG_RETURN_FLOAT8(m == 0 ? 1.0 : 1.0 - m / sqrt((double) na * nb));
}

/* extractQuery: the query's distinct non-NULL elements; none matches nothing. */
Datum
bark_multikey_sim_query(PG_FUNCTION_ARGS)
{
	int32	   *nkeys = (int32 *) PG_GETARG_POINTER(1);
	int			n;
	int32	   *elems = sorted_elements(PG_GETARG_ARRAYTYPE_P(0), &n);
	Datum	   *keys = palloc_array(Datum, Max(n, 1));

	for (int i = 0; i < n; i++)
		keys[i] = Int32GetDatum(elems[i]);
	*nkeys = n;
	PG_RETURN_POINTER(keys);
}

Datum
bark_multikey_sim_consistent(PG_FUNCTION_ARGS)
{
	bool	   *check = (bool *) PG_GETARG_POINTER(0);
	int32		nkeys = PG_GETARG_INT32(3);
	bool	   *recheck = (bool *) PG_GETARG_POINTER(5);
	int			m = 0;

	for (int i = 0; i < nkeys; i++)
		m += check[i];
	*recheck = true;
	PG_RETURN_BOOL(4 * m >= nkeys);
}

Datum
bark_multikey_sim_triconsistent(PG_FUNCTION_ARGS)
{
	GinTernaryValue *check = (GinTernaryValue *) PG_GETARG_POINTER(0);
	int32		nkeys = PG_GETARG_INT32(3);
	int			m = 0;

	for (int i = 0; i < nkeys; i++)
		m += check[i] != GIN_FALSE;
	PG_RETURN_GIN_TERNARY_VALUE(4 * m >= nkeys ? GIN_MAYBE : GIN_FALSE);
}

/*
 * bark_test_bigoffset: heap, declaring locator offsets up to MaxOffsetNumber,
 * so that a BARK index of it is built with that TID stride while its real
 * offsets stay heap's (bark_locator.sql).  It must have no column that needs
 * a TOAST table.
 */
static const LocatorDesc bigoffset_locator = {
	.width = sizeof(ItemPointerData),
	.max_offset = MaxOffsetNumber,
	.name = "bigoffset",
	.stable = false,
	.old_version_retained = true,
};

static const LocatorDesc *
bigoffset_relation_locator(Relation rel)
{
	return &bigoffset_locator;
}

PG_FUNCTION_INFO_V1(bark_test_bigoffset_handler);
PG_FUNCTION_INFO_V1(bark_test_tid_stride);
PG_FUNCTION_INFO_V1(bark_test_meta_stride);
PG_FUNCTION_INFO_V1(bark_test_tid_mappings);
PG_FUNCTION_INFO_V1(bark_test_insert_tid);

/* bark_test_insert_tid(index, key, tid): index_insert of (key) at tid. */
Datum
bark_test_insert_tid(PG_FUNCTION_ARGS)
{
	Relation	index = index_open(PG_GETARG_OID(0), RowExclusiveLock);
	Datum		value = PG_GETARG_DATUM(1);
	bool		isnull = false;
	Relation	heap = table_open(index->rd_index->indrelid, RowExclusiveLock);
	IndexInfo  *ii = BuildIndexInfo(index);

	if (!bark_relation_is_bark(index) || ii->ii_NumIndexAttrs != 1 ||
		TupleDescAttr(RelationGetDescr(index), 0)->atttypid != INT4OID)
		elog(ERROR, "bark_test_insert_tid needs a BARK index on one int4 column");
	index_insert(index, &value, &isnull, PG_GETARG_ITEMPOINTER(2), heap,
				 UNIQUE_CHECK_NO, false, ii);
	table_close(heap, RowExclusiveLock);
	index_close(index, RowExclusiveLock);
	PG_RETURN_VOID();
}

/* The scan seen set's radix tree, as barkscan.c declares it. */
#define RT_PREFIX test_seen
#define RT_SCOPE static inline
#define RT_DECLARE
#define RT_DEFINE
#define RT_VALUE_TYPE uint64
#include "lib/radixtree.h"

/* A key tuple of one int4 column, 42, for bark_test_tid_mappings' entries. */
static IndexTuple
mapping_key(TupleDesc *desc)
{
	Datum		v = Int32GetDatum(42);
	bool		n = false;

	*desc = CreateTemplateTupleDesc(1);
	TupleDescInitBuiltinEntry(*desc, 1, "k", INT4OID, -1, 0);
	TupleDescFinalize(*desc);
	return index_form_tuple(*desc, &v, &n);
}

/* Do entry's TIDs, read back at stride, equal tids[0..n)? */
static bool
mapping_entry_holds(IndexTuple entry, ItemPointer tids, int n, uint16 stride)
{
	ItemPointer out = palloc_array(ItemPointerData, n + 1);
	ItemPointerData lo;
	ItemPointerData hi;
	bool		ok;

	ok = bark_entry_count_tids(entry) == n &&
		bark_entry_get_tids(entry, out, n + 1, stride) == n &&
		memcmp(out, tids, n * sizeof(ItemPointerData)) == 0;
	bark_entry_tid_range(entry, &lo, &hi, stride);
	ok = ok && ItemPointerEquals(&lo, &tids[0]) &&
		ItemPointerEquals(&hi, &tids[n - 1]);
	for (int i = 0; ok && i < n; i++)
		ok = bark_entry_has_tid(entry, &tids[i], stride);
	pfree(out);
	return ok;
}

static void
mapping_result(ReturnSetInfo *rsinfo, const char *name, bool ok, int64 n)
{
	Datum		values[3];
	bool		nulls[3] = {0};

	values[0] = CStringGetTextDatum(name);
	values[1] = BoolGetDatum(ok);
	values[2] = Int64GetDatum(n);
	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
}

/*
 * bark_test_tid_mappings(stride): check BARK's TID mappings at TID stride
 * `stride` (0 means MaxOffsetNumber) on synthetic TIDs, every offset 1 ..
 * stride on blocks 0, 1, 2^20 and MaxBlockNumber, without an index:
 *
 * - posting_key: bark_tid_to_key / bark_key_to_tid round trip, and the keys
 *   ascend with the TIDs, so distinct TIDs never share a key;
 * - coded_list: a coded LIST (forced through bark_form_list_best on a
 *   clustered set) of runs that cross each boundary reads back exactly;
 * - posting_entry: likewise for a POSTING entry (bark_form_posting);
 * - add_tid: a LIST and a POSTING each take a TID at the boundaries by
 *   bark_entry_add_tid, as WAL replay of ADD_TID does;
 * - seen_set: the scan seen set's window (key >> 6, bit key & 63) of every
 *   TID is distinct.
 * - heap_coding: at stride MaxHeapTuplesPerPage, a coded LIST's bytes are
 *   GIN's (offset in the low 11 bits): the first delta of (0,1), (0,2) is 1,
 *   and of (0,1), (1,1) is 2048.
 */
Datum
bark_test_tid_mappings(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int32		arg = PG_GETARG_INT32(0);
	uint16		stride = arg == 0 ? MaxOffsetNumber : (uint16) arg;
	BlockNumber blocks[] = {0, 1, 1 << 20, MaxBlockNumber - 1, MaxBlockNumber};
	OffsetNumber bounds[] = {1, 2, MaxHeapTuplesPerPage - 1,
		MaxHeapTuplesPerPage, MaxHeapTuplesPerPage + 1, 2046, 2047, 2048};
	TupleDesc	desc;
	IndexTuple	key;
	bool		ok;
	int64		n;
	uint64		prevkey = 0;

	if (arg < 0 || arg > MaxOffsetNumber || (arg > 0 && arg < MaxHeapTuplesPerPage))
		elog(ERROR, "stride must be 0 or between %d and %d",
			 (int) MaxHeapTuplesPerPage, (int) MaxOffsetNumber);
	InitMaterializedSRF(fcinfo, 0);
	key = mapping_key(&desc);

	/* posting_key and seen_set over every offset of each block */
	ok = true;
	n = 0;
	for (int b = 0; b < lengthof(blocks); b++)
	{
		for (int off = 1; off <= stride; off++)
		{
			ItemPointerData tid;
			ItemPointerData back;
			uint64		k;

			ItemPointerSet(&tid, blocks[b], off);
			k = bark_tid_to_key(&tid, stride);
			bark_key_to_tid(k, &back, stride);
			if (!ItemPointerEquals(&tid, &back) || (n > 0 && k <= prevkey))
				ok = false;
			prevkey = k;
			n++;
		}
	}
	mapping_result(rsinfo, "posting_key", ok, n);

	/*
	 * seen_set: the scan's seen set as bark_mk_seen builds it, a radix tree
	 * from bark_tid_to_key(tid) >> 6 to a word with bit key & 63, takes
	 * every TID of blocks b and b + 1 as new, once.
	 */
	{
		MemoryContext cxt = AllocSetContextCreate(CurrentMemoryContext,
												  "seen", ALLOCSET_DEFAULT_SIZES);
		test_seen_radix_tree *seen = test_seen_create(cxt);

		ok = true;
		n = 0;
		for (int b = 0; b < lengthof(blocks); b++)
		{
			for (int off = 1; off <= stride; off++)
			{
				ItemPointerData tid;
				uint64		k;
				uint64		bit;
				uint64	   *word;

				ItemPointerSet(&tid, blocks[b], off);
				k = bark_tid_to_key(&tid, stride);
				bit = UINT64CONST(1) << (k & 63);
				word = test_seen_find(seen, k >> 6);
				if (word != NULL && (*word & bit) != 0)
					ok = false; /* a TID taken for one already seen */
				else if (word != NULL)
					*word |= bit;
				else
					test_seen_set(seen, k >> 6, &bit);
				n++;
			}
		}
		mapping_result(rsinfo, "seen_set", ok, n);
		MemoryContextDelete(cxt);
	}

	/*
	 * coded_list and posting_entry: for each block and each boundary offset
	 * not past the stride, the run of offsets around it, and a run that
	 * spans into the next block.
	 */
	{
		bool		okl = true;
		bool		okp = true;
		int64		nl = 0;
		int64		np = 0;

		for (int b = 0; b < lengthof(blocks); b++)
		{
			for (int j = 0; j < lengthof(bounds); j++)
			{
				ItemPointerData tids[16];
				int			ntids = 0;
				IndexTuple	entry;

				if (bounds[j] > stride)
					continue;
				for (int d = -4; d <= 4; d++)
				{
					int			off = bounds[j] + d;

					if (off >= 1 && off <= stride)
						ItemPointerSet(&tids[ntids++], blocks[b], off);
				}
				if (blocks[b] < MaxBlockNumber)
				{
					ItemPointerSet(&tids[ntids++], blocks[b] + 1, 1);
					ItemPointerSet(&tids[ntids++], blocks[b] + 1, stride);
				}
				entry = bark_form_list_best(desc, key, tids, ntids, stride);
				okl &= BarkListIsCoded(entry) &&
					mapping_entry_holds(entry, tids, ntids, stride);
				pfree(entry);
				nl += ntids;

				/*
				 * A POSTING beats a LIST only for a dense set: every offset
				 * within 200 of the boundary, and the next block's first 50.
				 */
				{
					ItemPointerData many[460];
					int			nmany = 0;

					for (int off = Max(1, bounds[j] - 200);
						 off <= Min(stride, bounds[j] + 200); off++)
						ItemPointerSet(&many[nmany++], blocks[b], off);
					for (int off = 1; blocks[b] < MaxBlockNumber && off <= 50; off++)
						ItemPointerSet(&many[nmany++], blocks[b] + 1, off);
					entry = bark_form_posting(desc, key, many, nmany, stride);
					okp &= entry != NULL &&
						mapping_entry_holds(entry, many, nmany, stride);
					np += nmany;
					if (entry)
						pfree(entry);
				}
			}
		}
		mapping_result(rsinfo, "coded_list", okl, nl);
		mapping_result(rsinfo, "posting_entry", okp, np);
	}

	/* add_tid: grow a coded LIST and a POSTING by the boundary TIDs */
	{
		ItemPointerData tids[2];
		ItemPointerData all[600];
		int			nall = 0;
		IndexTuple	list;
		IndexTuple	posting;

		ok = true;
		n = 0;
		ItemPointerSet(&tids[0], 7, 1);
		ItemPointerSet(&tids[1], 7, 2);
		list = bark_form_list_best(desc, key, tids, 2, stride);
		for (int off = 1; off <= stride && nall < 590; off++)
			ItemPointerSet(&all[nall++], 9, off);
		posting = bark_form_posting(desc, key, all, nall, stride);
		ok = posting != NULL;
		for (int j = 0; ok && j < lengthof(bounds); j++)
		{
			ItemPointerData t;
			IndexTuple	ext;

			if (bounds[j] > stride)
				continue;
			ItemPointerSet(&t, 8, bounds[j]);
			ext = bark_entry_add_tid(list, &t, BLCKSZ, stride);
			ok = ext != NULL && bark_entry_has_tid(ext, &t, stride) &&
				bark_entry_count_tids(ext) == bark_entry_count_tids(list) + 1;
			if (ext != NULL)
			{
				pfree(list);
				list = ext;
			}
			ItemPointerSet(&t, 10, bounds[j]);
			ext = bark_entry_add_tid(posting, &t, BLCKSZ, stride);
			ok = ok && ext != NULL && bark_entry_has_tid(ext, &t, stride) &&
				bark_entry_count_tids(ext) == bark_entry_count_tids(posting) + 1;
			if (ext != NULL)
			{
				pfree(posting);
				posting = ext;
			}
			n += 2;
		}
		mapping_result(rsinfo, "add_tid", ok, n);
	}

	/*
	 * heap_coding: heap's stride codes the offset in GIN's 11 bits.  (0,1),
	 * (0,2) .. (0,8) code their first delta as 1; (0,1), (1,1) .. (1,7) as
	 * 1 << 11 = 2048, varbyte 0x80 0x10.  (Eight TIDs, so the coded LIST is
	 * the smaller.)
	 */
	if (stride == MaxHeapTuplesPerPage)
	{
		ItemPointerData tids[8];
		IndexTuple	entry;
		unsigned char *d;

		for (int i = 0; i < 8; i++)
			ItemPointerSet(&tids[i], 0, i + 1);
		entry = bark_form_list_best(desc, key, tids, 8, stride);
		d = BarkListIsCoded(entry) ? BarkCodedListGetDeltas(entry) : NULL;
		ok = d != NULL && d[0] == 1;
		for (int i = 1; i < 8; i++)
			ItemPointerSet(&tids[i], 1, i);
		entry = bark_form_list_best(desc, key, tids, 8, stride);
		d = BarkListIsCoded(entry) ? BarkCodedListGetDeltas(entry) : NULL;
		ok = ok && d != NULL && d[0] == 0x80 && d[1] == 0x10;
		mapping_result(rsinfo, "heap_coding", ok, 16);
	}

	return (Datum) 0;
}

/* bark_test_tid_stride(index): bark_index_tid_stride. */
Datum
bark_test_tid_stride(PG_FUNCTION_ARGS)
{
	Relation	rel = open_bark_index(PG_GETARG_TEXT_PP(0));
	int32		stride = bark_index_tid_stride(rel);

	relation_close(rel, AccessShareLock);
	PG_RETURN_INT32(stride);
}

/* bark_test_meta_stride(index): the meta page's bark_tid_stride as stored. */
Datum
bark_test_meta_stride(PG_FUNCTION_ARGS)
{
	Relation	rel = open_bark_index(PG_GETARG_TEXT_PP(0));
	Buffer		buf = ReadBuffer(rel, BARK_METAPAGE);
	int32		stride;

	LockBuffer(buf, BUFFER_LOCK_SHARE);
	stride = BarkPageGetMeta(BufferGetPage(buf))->bark_tid_stride;
	UnlockReleaseBuffer(buf);
	relation_close(rel, AccessShareLock);
	PG_RETURN_INT32(stride);
}

/*
 * heap_getnext, which heap's index build and validate scans read the table
 * with, refuses a relation whose routine is not heap's, so these run them
 * with heap's routine in the relation; the TIDs are heap's, as they are.
 */
static double
bigoffset_index_build_range_scan(Relation table_rel, Relation index_rel,
								 IndexInfo *index_info, bool allow_sync,
								 bool anyvisible, bool progress,
								 BlockNumber start_blockno,
								 BlockNumber numblocks,
								 IndexBuildCallback callback,
								 void *callback_state, TableScanDesc scan)
{
	const TableAmRoutine *saved = table_rel->rd_tableam;
	double		result;

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
bigoffset_index_validate_scan(Relation table_rel, Relation index_rel,
							  IndexInfo *index_info, Snapshot snapshot,
							  ValidateIndexState *state)
{
	const TableAmRoutine *saved = table_rel->rd_tableam;

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

Datum
bark_test_bigoffset_handler(PG_FUNCTION_ARGS)
{
	static TableAmRoutine methods;

	if (methods.type == 0)
	{
		methods = *GetHeapamTableAmRoutine();
		methods.relation_locator = bigoffset_relation_locator;
		methods.index_build_range_scan = bigoffset_index_build_range_scan;
		methods.index_validate_scan = bigoffset_index_validate_scan;
	}
	PG_RETURN_POINTER(&methods);
}
