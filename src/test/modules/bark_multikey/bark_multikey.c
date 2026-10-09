/*--------------------------------------------------------------------------
 *
 * bark_multikey.c
 *		A multikey operator class for BARK over int4[], for testing.
 *
 * The class's keys are an array's elements.  Its extract-value procedure (7)
 * returns them, its extract-query procedure (8) turns &&, @>, <@ and = into
 * boundaries over them, and its index-recheck procedure (9) answers for one
 * entry.  |<| is an ordering operator: it orders arrays by their least
 * element, MongoDB's ascending sort rule for arrays.  The SQL-callable
 * helpers at the end run procedures 7 and 8 as BARK would and print what
 * they return, so the class can be checked before BARK calls it.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/bark_multikey/bark_multikey.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bark.h"
#include "access/stratnum.h"
#include "catalog/pg_type.h"
#include "common/int.h"
#include "fmgr.h"
#include "lib/qunique.h"
#include "lib/stringinfo.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

PG_MODULE_MAGIC;

/* The class's strategy numbers, GIN's array_ops numbers plus |<|. */
#define BARK_MK_OVERLAP		1	/* && */
#define BARK_MK_CONTAINS	2	/* @> */
#define BARK_MK_CONTAINED	3	/* <@ */
#define BARK_MK_EQUAL		4	/* = */
#define BARK_MK_ORDER		5	/* |<|, ordering */

PG_FUNCTION_INFO_V1(bark_multikey_extract_value);
PG_FUNCTION_INFO_V1(bark_multikey_extract_query);
PG_FUNCTION_INFO_V1(bark_multikey_recheck);
PG_FUNCTION_INFO_V1(bark_multikey_least);
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
 * no element to point at.  The ordering operator reads every row: the whole
 * key range, ascending, and the NULL entries, whose rows sort last as the
 * operator's NULL does.
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

	/* Declared with six arguments, as for pg_extended_btree, it has none. */
	if (PG_NARGS() >= 7)
		flags = (BarkQueryFlags *) PG_GETARG_POINTER(6);
	*nboundaries = 0;
	*boundaries = NULL;
	*recheck = false;

	/* The ordering key's argument is |<|'s right operand, not an array. */
	if (strategy == BARK_MK_ORDER)
	{
		*boundaries = palloc0_object(BarkBoundary);
		*nboundaries = 1;
		flags->backward = false;
		flags->searchnulls = true;
		PG_RETURN_VOID();
	}

	query = PG_GETARG_ARRAYTYPE_P(0);
	keys = sorted_elements(query, &nkeys);
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
 * answer is maybe, which leaves the row to the executor's recheck.
 */
Datum
bark_multikey_recheck(PG_FUNCTION_ARGS)
{
	StrategyNumber strategy = PG_GETARG_UINT16(1);

	if (strategy == BARK_MK_OVERLAP || strategy == BARK_MK_ORDER)
		PG_RETURN_INT16(BARK_RECHECK_TRUE);
	PG_RETURN_INT16(BARK_RECHECK_MAYBE);
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
