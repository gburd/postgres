/*--------------------------------------------------------------------------
 *
 * bark_wildcard.c
 *		A wildcard operator class for BARK over jsonb, for testing.
 *
 * bark_jsonb_wildcard_ops indexes every scalar of a jsonb document under
 * its path, as MongoDB's wildcard indexes do: one key per (path, value),
 * and one per element of an array.  A key is a two-element jsonb array
 * [path, value]: the path is a jsonb string of the dotted member names
 * ("a.b"), the value the jsonb scalar, or {} / [] for an empty object or
 * array.  jsonb_cmp orders arrays of equal length element by element, so the
 * keys of one path are contiguous and ordered by jsonb's value order.
 * Array indexes are not part of a path: a.b names every element of an array
 * at a.b, as in MongoDB.  An array of arrays is indexed one level deep, as
 * MongoDB does: the inner arrays are values.
 *
 * Markers: for every path that holds an array, a one-element jsonb array
 * [path].  It sorts before every two-element data key (jsonb orders arrays
 * by length first), so no marker equals a data key.  A path below no marked
 * path has at most one value per document, so procedure 8 tells the scan
 * that a query on it needs no seen set.  For that, an include projection
 * keeps the markers of the paths above its paths too, and a member named ""
 * of an object at path "" marks "": its members' paths are those of the
 * object's own members ({"": {"a": 1}, "a": 2} has two values at a).
 *
 * The column's options (procedure 5) are include and exclude, each a
 * comma-separated list of dotted paths, mutually exclusive, as MongoDB's
 * wildcardProjection: with include, only those paths and the paths below
 * them are indexed; with exclude, all others.
 *
 * Operators (doc jsonb, query jsonb), the query a key-shaped [path, value]:
 * #< #<= #= #>= #> are true when some value the class indexes at the path
 * (a scalar, an element of an array, an array nested in one, or an empty
 * object or array) compares so with the query's value, MongoDB's rule for
 * arrays.  Values compare only within their type, as MongoDB brackets types:
 * a number never compares with a string.  doc #? path is true when the
 * document has such a value at the path.  These are the values a wildcard
 * index can answer for, so two of MongoDB's rules are not followed: a
 * missing path does not equal null, and a value that is a non-empty object,
 * or a whole non-empty array, is not compared (MongoDB's wildcard indexes
 * do not answer those queries either).  A path that holds only non-empty
 * objects has no value, so #? is false there.
 *
 * Sorting by a path: doc |<| path is the smallest key [path, value] the
 * class indexes at the path, in jsonb_cmp order, or NULL when there is
 * none; an ordering operator, sorted by jsonb's btree family.  So ORDER BY
 * doc |<| 'a' orders documents by their smallest value at a (MongoDB's sort
 * rule for arrays), across types in jsonb_cmp's order, documents without a
 * value at a last.  The index serves it without a Sort: procedure 8 returns
 * the path's range and sets BarkQueryFlags.orderrest, so the scan reports
 * each row's first key in the range as its ORDER BY value and then reads
 * the rest of the column for the documents without the path.  It returns
 * the key, not the value, because the scan reports an index key, exactly.
 * A path the projection drops has no keys to sort by, so procedure 8 then
 * raises an error rather than order every row as NULL.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/bark_wildcard/bark_wildcard.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bark.h"
#include "access/relation.h"
#include "access/reloptions.h"
#include "access/stratnum.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_class.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "funcapi.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/tuplestore.h"
#include "utils/jsonb.h"
#include "utils/numeric.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(bark_wildcard_options);
PG_FUNCTION_INFO_V1(bark_wildcard_extract_value);
PG_FUNCTION_INFO_V1(bark_wildcard_keys);
PG_FUNCTION_INFO_V1(bark_wildcard_lt);
PG_FUNCTION_INFO_V1(bark_wildcard_le);
PG_FUNCTION_INFO_V1(bark_wildcard_eq);
PG_FUNCTION_INFO_V1(bark_wildcard_ge);
PG_FUNCTION_INFO_V1(bark_wildcard_gt);
PG_FUNCTION_INFO_V1(bark_wildcard_exists);
PG_FUNCTION_INFO_V1(bark_wildcard_least);
PG_FUNCTION_INFO_V1(bark_wildcard_extract_query);
PG_FUNCTION_INFO_V1(bark_wildcard_recheck);
PG_FUNCTION_INFO_V1(bark_wildcard_boundaries);
PG_FUNCTION_INFO_V1(bark_wildcard_in_boundaries);
PG_FUNCTION_INFO_V1(bark_wildcard_entries);

/* The operators' strategy numbers: btree's for the comparisons. */
#define BARK_WC_LT		BTLessStrategyNumber	/* #< */
#define BARK_WC_LE		BTLessEqualStrategyNumber	/* #<= */
#define BARK_WC_EQ		BTEqualStrategyNumber	/* #= */
#define BARK_WC_GE		BTGreaterEqualStrategyNumber	/* #>= */
#define BARK_WC_GT		BTGreaterStrategyNumber /* #> */
#define BARK_WC_EXISTS	6				/* #? */
#define BARK_WC_ORDER	7				/* |<|, ordering */

/* The column's options, procedure 5. */
typedef struct BarkWildcardOptions
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int			include;		/* offset of the include list, or 0 */
	int			exclude;		/* offset of the exclude list, or 0 */
} BarkWildcardOptions;

/* The projection of one column, parsed from its options. */
typedef struct BarkWildcardProjection
{
	bool		include;		/* the paths are an include list */
	int			npaths;			/* 0: index every path */
	char	  **paths;
} BarkWildcardProjection;

/* The keys and markers of one document, as procedure 7 builds them. */
typedef struct BarkWildcardKeys
{
	const BarkWildcardProjection *proj;
	const char *only;			/* if set, only the keys of this path */
	Datum	   *keys;
	int			nkeys;
	int			maxkeys;
	Datum	   *markers;
	int			nmarkers;
	int			maxmarkers;
} BarkWildcardKeys;

static void
bark_wildcard_validate_paths(const char *value)
{
	if (value == NULL)
		return;
	for (const char *p = value; *p; p++)
	{
		if ((*p == ',' && (p == value || p[1] == '\0' || p[1] == ',')) ||
			(*p == '.' && (p == value || p[1] == '\0' || p[1] == '.' ||
						   p[1] == ',' || p[-1] == ',')))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid path list \"%s\"", value),
					 errdetail("A path list is dotted member names separated by commas.")));
	}
}

/*
 * Procedure 5: the column's options, include and exclude.
 */
Datum
bark_wildcard_options(PG_FUNCTION_ARGS)
{
	local_relopts *relopts = (local_relopts *) PG_GETARG_POINTER(0);

	init_local_reloptions(relopts, sizeof(BarkWildcardOptions));
	/* validated here, at CREATE INDEX, so that procedures 7 and 8 never fail */
	add_local_string_reloption(relopts, "include",
							   "paths to index (comma-separated, dotted)",
							   NULL, bark_wildcard_validate_paths, NULL,
							   offsetof(BarkWildcardOptions, include));
	add_local_string_reloption(relopts, "exclude",
							   "paths not to index (comma-separated, dotted)",
							   NULL, bark_wildcard_validate_paths, NULL,
							   offsetof(BarkWildcardOptions, exclude));
	PG_RETURN_VOID();
}

/* Split a path list, which is scribbled on and kept, into proj's paths. */
static void
bark_wildcard_parse_list(BarkWildcardProjection *proj, char *list)
{
	char	   *save;

	proj->paths = palloc_array(char *, strlen(list) / 2 + 1);
	for (char *tok = strtok_r(list, ",", &save); tok != NULL;
		 tok = strtok_r(NULL, ",", &save))
		proj->paths[proj->npaths++] = tok;
}

/* The column's projection, from its options. */
static BarkWildcardProjection *
bark_wildcard_projection(FunctionCallInfo fcinfo)
{
	BarkWildcardProjection *proj = palloc0_object(BarkWildcardProjection);
	BarkWildcardOptions *opts;
	const char *list = NULL;

	if (!PG_HAS_OPCLASS_OPTIONS())
		return proj;
	opts = (BarkWildcardOptions *) PG_GET_OPCLASS_OPTIONS();
	if (opts->include != 0 && opts->exclude != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("include and exclude cannot both be set")));
	if (opts->include != 0)
	{
		proj->include = true;
		list = GET_STRING_RELOPTION(opts, include);
	}
	else if (opts->exclude != 0)
		list = GET_STRING_RELOPTION(opts, exclude);
	if (list != NULL)
		bark_wildcard_parse_list(proj, pstrdup(list));
	return proj;
}

/*
 * Is `path` equal to `prefix`, or below it (prefix + "." + more)?  Every
 * path is below "": a member named "" at the root adds no segment
 * (bark_wildcard_child), so {"": {"s": 5}} has 5 at path s, as procedure 7
 * indexes it, and the operators' walk must enter that member.
 */
static bool
bark_wildcard_path_under(const char *path, const char *prefix)
{
	size_t		n = strlen(prefix);

	return n == 0 ||
		(strncmp(path, prefix, n) == 0 &&
		 (path[n] == '\0' || path[n] == '.'));
}

/* Does the projection keep the scalar at `path`? */
static bool
bark_wildcard_keeps(const BarkWildcardProjection *proj, const char *path)
{
	bool		listed = false;

	if (proj->npaths == 0)
		return true;
	for (int i = 0; i < proj->npaths && !listed; i++)
		listed = bark_wildcard_path_under(path, proj->paths[i]);
	return proj->include ? listed : !listed;
}

/* A jsonb array of the given values, as a Datum. */
static Datum
bark_wildcard_array(JsonbValue *elems, int nelems)
{
	JsonbInState state = {0};

	pushJsonbValue(&state, WJB_BEGIN_ARRAY, NULL);
	for (int i = 0; i < nelems; i++)
		pushJsonbValue(&state, WJB_ELEM, &elems[i]);
	pushJsonbValue(&state, WJB_END_ARRAY, NULL);
	return JsonbPGetDatum(JsonbValueToJsonb(state.result));
}

/* The key [path, value]. */
static Datum
bark_wildcard_key(const char *path, JsonbValue *value)
{
	JsonbValue	pair[2];

	pair[0].type = jbvString;
	pair[0].val.string.val = (char *) path;
	pair[0].val.string.len = strlen(path);
	pair[1] = *value;
	return bark_wildcard_array(pair, 2);
}

static void
bark_wildcard_add_key(BarkWildcardKeys *k, const char *path, JsonbValue *value)
{
	if (!bark_wildcard_keeps(k->proj, path) ||
		(k->only != NULL && strcmp(path, k->only) != 0))
		return;
	if (k->nkeys == k->maxkeys)
	{
		k->maxkeys *= 2;
		k->keys = repalloc_array(k->keys, Datum, k->maxkeys);
	}
	k->keys[k->nkeys++] = bark_wildcard_key(path, value);
}

/*
 * Does the projection keep the marker of `path`: the path, or a path below
 * it, is kept?  Under exclude, a path above a kept one is kept itself.
 */
static bool
bark_wildcard_keeps_marker(const BarkWildcardProjection *proj, const char *path)
{
	if (bark_wildcard_keeps(proj, path))
		return true;
	for (int i = 0; i < proj->npaths && proj->include; i++)
		if (path[0] == '\0' || bark_wildcard_path_under(proj->paths[i], path))
			return true;
	return false;
}

/* The marker [path]. */
static Datum
bark_wildcard_marker(const char *path, int len)
{
	JsonbValue	p;

	p.type = jbvString;
	p.val.string.val = (char *) path;
	p.val.string.len = len;
	return bark_wildcard_array(&p, 1);
}

static void
bark_wildcard_add_marker(BarkWildcardKeys *k, const char *path)
{
	if (!bark_wildcard_keeps_marker(k->proj, path) ||
		(k->only != NULL && strcmp(path, k->only) != 0))
		return;
	if (k->nmarkers == k->maxmarkers)
	{
		k->maxmarkers *= 2;
		k->markers = repalloc_array(k->markers, Datum, k->maxmarkers);
	}
	k->markers[k->nmarkers++] = bark_wildcard_marker(path, strlen(path));
}

/* A jsonb {} or [] as a scalar value, for an empty container. */
static void
bark_wildcard_empty(JsonbValue *v, bool isobject)
{
	JsonbInState state = {0};
	Jsonb	   *empty;

	pushJsonbValue(&state, isobject ? WJB_BEGIN_OBJECT : WJB_BEGIN_ARRAY, NULL);
	pushJsonbValue(&state, isobject ? WJB_END_OBJECT : WJB_END_ARRAY, NULL);
	empty = JsonbValueToJsonb(state.result);
	v->type = jbvBinary;
	v->val.binary.data = &empty->root;
	v->val.binary.len = VARSIZE(empty) - VARHDRSZ;
}

/* `path` + "." + `name`, escaping dots and backslashes in the name. */
static char *
bark_wildcard_child(const char *path, const char *name, int len)
{
	StringInfoData buf;

	initStringInfo(&buf);
	if (path[0] != '\0')
	{
		appendStringInfoString(&buf, path);
		appendStringInfoChar(&buf, '.');
	}
	for (int i = 0; i < len; i++)
	{
		if (name[i] == '.' || name[i] == '\\')
			appendStringInfoChar(&buf, '\\');
		appendStringInfoChar(&buf, name[i]);
	}
	return buf.data;
}

static void bark_wildcard_walk(BarkWildcardKeys *k, const char *path,
							   JsonbContainer *container, bool inarray);

/*
 * Index one value found at `path`: a scalar becomes a key; an object is
 * walked; an array is walked one level (its elements are values at the same
 * path; an inner array is a value, as in MongoDB), and marks the path.
 * `inarray` says the value is an element of an array already.
 */
static void
bark_wildcard_value(BarkWildcardKeys *k, const char *path, JsonbValue *v,
					bool inarray)
{
	if (v->type != jbvBinary)
	{
		bark_wildcard_add_key(k, path, v);
		return;
	}
	if (JsonContainerIsObject(v->val.binary.data))
	{
		if (JsonContainerSize(v->val.binary.data) == 0)
		{
			JsonbValue	empty;

			bark_wildcard_empty(&empty, true);
			bark_wildcard_add_key(k, path, &empty);
		}
		else
			bark_wildcard_walk(k, path, v->val.binary.data, false);
		return;
	}
	/* an array */
	if (inarray)
	{
		bark_wildcard_add_key(k, path, v);	/* a nested array is a value */
		return;
	}
	bark_wildcard_add_marker(k, path);
	if (JsonContainerSize(v->val.binary.data) == 0)
	{
		JsonbValue	empty;

		bark_wildcard_empty(&empty, false);
		bark_wildcard_add_key(k, path, &empty);
	}
	else
		bark_wildcard_walk(k, path, v->val.binary.data, true);
}

static void
bark_wildcard_walk(BarkWildcardKeys *k, const char *path,
				   JsonbContainer *container, bool inarray)
{
	JsonbIterator *it = JsonbIteratorInit(container);
	JsonbValue	v;
	JsonbIteratorToken tok;
	char	   *member = NULL;

	check_stack_depth();
	while ((tok = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
	{
		switch (tok)
		{
			case WJB_KEY:
				member = bark_wildcard_child(path, v.val.string.val,
											 v.val.string.len);
				if (member[0] == '\0')
					bark_wildcard_add_marker(k, member);
				break;
			case WJB_VALUE:
				/* a walk for one path enters only the members on its way */
				if (k->only == NULL || bark_wildcard_path_under(k->only, member))
					bark_wildcard_value(k, member, &v, false);
				break;
			case WJB_ELEM:
				bark_wildcard_value(k, path, &v, inarray);
				break;
			default:
				break;
		}
	}
}

/*
 * The keys and markers of doc, into k, whose proj (and only) are set.  A
 * scalar document (a top-level number, say) has the empty path "".
 */
static void
bark_wildcard_doc(BarkWildcardKeys *k, Jsonb *doc)
{
	JsonbValue	v;

	k->maxkeys = 16;
	k->keys = palloc_array(Datum, k->maxkeys);
	k->maxmarkers = 4;
	k->markers = palloc_array(Datum, k->maxmarkers);
	if (JB_ROOT_IS_SCALAR(doc))
	{
		if (!JsonbExtractScalar(&doc->root, &v))
			elog(ERROR, "could not extract a scalar from a scalar jsonb");
		bark_wildcard_add_key(k, "", &v);
	}
	else
	{
		v.type = jbvBinary;
		v.val.binary.data = &doc->root;
		v.val.binary.len = VARSIZE(doc) - VARHDRSZ;
		bark_wildcard_value(k, "", &v, false);
	}
}

/*
 * Procedure 7, all five arguments: the document's keys and markers.
 */
Datum
bark_wildcard_extract_value(PG_FUNCTION_ARGS)
{
	Jsonb	   *doc = PG_GETARG_JSONB_P(0);
	int32	   *nkeys = (int32 *) PG_GETARG_POINTER(1);
	bool	  **nullFlags = (bool **) PG_GETARG_POINTER(2);
	Datum	  **markers = (Datum **) PG_GETARG_POINTER(3);
	int32	   *nmarkers = (int32 *) PG_GETARG_POINTER(4);
	BarkWildcardKeys k = {0};

	k.proj = bark_wildcard_projection(fcinfo);
	bark_wildcard_doc(&k, doc);

	*nkeys = k.nkeys;
	*nullFlags = NULL;
	*markers = k.markers;
	*nmarkers = k.nmarkers;
	PG_RETURN_POINTER(k.keys);
}

/*
 * For the test helpers: the projection given by the function's arguments
 * argno (include) and argno + 1 (exclude), as the column's options give it.
 */
static BarkWildcardProjection *
bark_wildcard_projection_args(FunctionCallInfo fcinfo, int argno)
{
	BarkWildcardProjection *proj = palloc0_object(BarkWildcardProjection);
	char	   *list = NULL;

	if (!PG_ARGISNULL(argno) && !PG_ARGISNULL(argno + 1))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("include and exclude cannot both be set")));
	if (!PG_ARGISNULL(argno))
	{
		proj->include = true;
		list = text_to_cstring(PG_GETARG_TEXT_PP(argno));
	}
	else if (!PG_ARGISNULL(argno + 1))
		list = text_to_cstring(PG_GETARG_TEXT_PP(argno + 1));
	if (list != NULL)
	{
		bark_wildcard_validate_paths(list);
		bark_wildcard_parse_list(proj, list);
	}
	return proj;
}

/*
 * bark_wildcard_keys(doc, include, exclude): procedure 7's keys of doc, then
 * its markers, for tests, with the projection given directly.  Not STRICT,
 * since include and exclude may be NULL; a NULL document has no keys.
 */
Datum
bark_wildcard_keys(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	BarkWildcardKeys k = {0};

	if (PG_ARGISNULL(0))
	{
		InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC | MAT_SRF_BLESS);
		return (Datum) 0;
	}
	k.proj = bark_wildcard_projection_args(fcinfo, 1);
	bark_wildcard_doc(&k, PG_GETARG_JSONB_P(0));

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC | MAT_SRF_BLESS);
	for (int i = 0; i < k.nkeys + k.nmarkers; i++)
	{
		Datum		value = i < k.nkeys ? k.keys[i] : k.markers[i - k.nkeys];
		bool		isnull = false;

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, &value,
							 &isnull);
	}
	return (Datum) 0;
}

/*
 * The type bracket of a value: jsonb's type code, which jsonb_cmp orders
 * null < string < number < boolean < array < object, with a container
 * (jbvBinary) as the array or object it is.  Values compare only within a
 * bracket.
 */
static int
bark_wildcard_bracket(const JsonbValue *v)
{
	if (v->type == jbvBinary)
		return JsonContainerIsArray(v->val.binary.data) ? jbvArray : jbvObject;
	return v->type;
}

/*
 * A query of the comparison operators: a two-element jsonb array [path,
 * value], the path a string written as the class writes keys' paths.  Sets
 * *path (a palloc'd C string) and *value.
 */
static void
bark_wildcard_query(Jsonb *query, char **path, JsonbValue *value)
{
	JsonbValue *p;
	JsonbValue *v;

	if (!JB_ROOT_IS_ARRAY(query) || JB_ROOT_IS_SCALAR(query) ||
		JB_ROOT_COUNT(query) != 2 ||
		(p = getIthJsonbValueFromContainer(&query->root, 0))->type != jbvString)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("a wildcard query must be a jsonb array [path, value] whose path is a string")));
	v = getIthJsonbValueFromContainer(&query->root, 1);
	*path = pnstrdup(p->val.string.val, p->val.string.len);
	*value = *v;
}

/*
 * The comparison operators, which are the heap recheck: is some value the
 * class indexes at the query's path in the query's type bracket, and does
 * its key [path, value] compare with the query, in jsonb_cmp's order, as
 * the strategy asks?  The keys are compared rather than the values so that
 * the operators and the index agree on the order by construction.
 */
static bool
bark_wildcard_compare(Jsonb *doc, Jsonb *query, StrategyNumber strategy)
{
	static const BarkWildcardProjection all = {0};
	BarkWildcardKeys k = {0};
	JsonbValue	qvalue;
	char	   *path;
	int			bracket;

	bark_wildcard_query(query, &path, &qvalue);
	bracket = bark_wildcard_bracket(&qvalue);
	k.proj = &all;
	k.only = path;
	bark_wildcard_doc(&k, doc);
	for (int i = 0; i < k.nkeys; i++)
	{
		Jsonb	   *key = DatumGetJsonbP(k.keys[i]);
		int			c;

		if (bark_wildcard_bracket(getIthJsonbValueFromContainer(&key->root, 1)) != bracket)
			continue;
		c = compareJsonbContainers(&key->root, &query->root);
		switch (strategy)
		{
			case BARK_WC_LT:
				if (c < 0)
					return true;
				break;
			case BARK_WC_LE:
				if (c <= 0)
					return true;
				break;
			case BARK_WC_EQ:
				if (c == 0)
					return true;
				break;
			case BARK_WC_GE:
				if (c >= 0)
					return true;
				break;
			case BARK_WC_GT:
				if (c > 0)
					return true;
				break;
		}
	}
	return false;
}

Datum
bark_wildcard_lt(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(bark_wildcard_compare(PG_GETARG_JSONB_P(0),
										 PG_GETARG_JSONB_P(1), BARK_WC_LT));
}

Datum
bark_wildcard_le(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(bark_wildcard_compare(PG_GETARG_JSONB_P(0),
										 PG_GETARG_JSONB_P(1), BARK_WC_LE));
}

Datum
bark_wildcard_eq(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(bark_wildcard_compare(PG_GETARG_JSONB_P(0),
										 PG_GETARG_JSONB_P(1), BARK_WC_EQ));
}

Datum
bark_wildcard_ge(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(bark_wildcard_compare(PG_GETARG_JSONB_P(0),
										 PG_GETARG_JSONB_P(1), BARK_WC_GE));
}

Datum
bark_wildcard_gt(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(bark_wildcard_compare(PG_GETARG_JSONB_P(0),
										 PG_GETARG_JSONB_P(1), BARK_WC_GT));
}

/*
 * doc #? path: does the document have a value the class indexes at the
 * path (a scalar, an array's element, or an empty container)?
 */
Datum
bark_wildcard_exists(PG_FUNCTION_ARGS)
{
	static const BarkWildcardProjection all = {0};
	BarkWildcardKeys k = {0};

	k.proj = &all;
	k.only = text_to_cstring(PG_GETARG_TEXT_PP(1));
	bark_wildcard_doc(&k, PG_GETARG_JSONB_P(0));
	PG_RETURN_BOOL(k.nkeys > 0);
}

/*
 * doc |<| path: the smallest key [path, value] the class indexes at the
 * path, or NULL.
 */
Datum
bark_wildcard_least(PG_FUNCTION_ARGS)
{
	static const BarkWildcardProjection all = {0};
	BarkWildcardKeys k = {0};
	Jsonb	   *least = NULL;

	k.proj = &all;
	k.only = text_to_cstring(PG_GETARG_TEXT_PP(1));
	bark_wildcard_doc(&k, PG_GETARG_JSONB_P(0));
	for (int i = 0; i < k.nkeys; i++)
	{
		Jsonb	   *key = DatumGetJsonbP(k.keys[i]);

		if (least == NULL || compareJsonbContainers(&key->root, &least->root) < 0)
			least = key;
	}
	if (least == NULL)
		PG_RETURN_NULL();
	PG_RETURN_JSONB_P(least);
}

/*
 * The key [path, value] of one end of a boundary, value a scalar of the
 * given type, false for jbvBool, or {} for jbvObject.
 */
static Datum
bark_wildcard_end(const char *path, enum jbvType type)
{
	JsonbValue	v;

	v.type = type;
	if (type == jbvBool)
		v.val.boolean = false;
	else if (type == jbvNumeric)
	{
		/*
		 * Every number a key can hold sorts after -Infinity, and every string
		 * before it: jsonb cannot store an infinite number (to_jsonb writes
		 * one as a string), so the key is only ever a search argument, and
		 * jsonb_cmp compares it with numeric_cmp, which orders infinities.
		 */
		v.val.numeric = DatumGetNumeric(DirectFunctionCall3(numeric_in,
															CStringGetDatum("-Infinity"),
															ObjectIdGetDatum(InvalidOid),
															Int32GetDatum(-1)));
	}
	else if (type == jbvObject)
		bark_wildcard_empty(&v, true);
	return bark_wildcard_key(path, &v);
}

/*
 * The ends of a type bracket at a path, in jsonb_cmp's order of two-element
 * arrays: the paths compare first, then the values, by type (null < string
 * < number < boolean < array < object, jsonb's type codes) and then within
 * the type.  Each end is exact:
 *
 *	null	[>= [p, null], <= [p, null]]
 *	string	(>	[p, null], <  [p, -Infinity])
 *	number	[>= [p, -Infinity], < [p, false])
 *	boolean [>= [p, false], <= [p, true]]
 *	array	(>	[p, true], <  [p, {}])
 *	object	[>= [p, {}], <= [p, {}]]
 *
 * The object bracket is the one value {}: the class indexes no other object
 * as a value (procedure 7 walks a non-empty object, and an array nested in
 * an array sorts as an array whatever it holds).  So [p, {}] is the last
 * key of path p.  Sets elems[0] (lower) and elems[1] (upper); each strategy
 * is BTEqualStrategyNumber's neighbors, never equality.
 */
static void
bark_wildcard_bracket_ends(const char *path, int bracket,
						   BarkSearchElement *lower, BarkSearchElement *upper)
{
	JsonbValue	t;

	switch (bracket)
	{
		case jbvNull:
			lower->argument = upper->argument = bark_wildcard_end(path, jbvNull);
			lower->strategy = BTGreaterEqualStrategyNumber;
			upper->strategy = BTLessEqualStrategyNumber;
			break;
		case jbvString:
			lower->argument = bark_wildcard_end(path, jbvNull);
			lower->strategy = BTGreaterStrategyNumber;
			upper->argument = bark_wildcard_end(path, jbvNumeric);
			upper->strategy = BTLessStrategyNumber;
			break;
		case jbvNumeric:
			lower->argument = bark_wildcard_end(path, jbvNumeric);
			lower->strategy = BTGreaterEqualStrategyNumber;
			upper->argument = bark_wildcard_end(path, jbvBool);
			upper->strategy = BTLessStrategyNumber;
			break;
		case jbvBool:
			lower->argument = bark_wildcard_end(path, jbvBool);
			lower->strategy = BTGreaterEqualStrategyNumber;
			t.type = jbvBool;
			t.val.boolean = true;
			upper->argument = bark_wildcard_key(path, &t);
			upper->strategy = BTLessEqualStrategyNumber;
			break;
		case jbvArray:
			t.type = jbvBool;
			t.val.boolean = true;
			lower->argument = bark_wildcard_key(path, &t);
			lower->strategy = BTGreaterStrategyNumber;
			upper->argument = bark_wildcard_end(path, jbvObject);
			upper->strategy = BTLessStrategyNumber;
			break;
		default:
			lower->argument = upper->argument = bark_wildcard_end(path, jbvObject);
			lower->strategy = BTGreaterEqualStrategyNumber;
			upper->strategy = BTLessEqualStrategyNumber;
			break;
	}
}

/*
 * Does the index hold the marker of `path` or of a path above it, "" (the
 * document) included?  If not, no document has an array there or a second
 * value at `path`.  Dots escaped with a backslash do not end a path.
 */
static bool
bark_wildcard_marked(Relation index, const char *path)
{
	int			len = 0;

	for (;;)
	{
		if (bark_index_has_marker(index, bark_wildcard_marker(path, len)))
			return true;
		if (path[len] == '\0')
			return false;
		if (len > 0)
			len++;				/* past the dot */
		while (path[len] != '\0' && path[len] != '.')
			len += (path[len] == '\\' && path[len + 1] != '\0') ? 2 : 1;
	}
}

/*
 * Procedure 8's work, with the column's projection given: the query's one
 * boundary, inside its path's range and its value's type bracket.  #= is
 * the point [path, value]; #< and #<= run from the bracket's start to the
 * query, #> and #>= from the query to the bracket's end; #? is the path's
 * whole range, from its first bracket's start to its last's end, and so is
 * |<|, which also sets orderrest: the documents without a value at the
 * path have no key in the range, and the scan reads them after it.  These
 * are exact, so procedure 9 is not asked.  The boundary holds keys of one
 * path only, so when BARK names the index and the index has no marker on
 * the way to that path, a row matches through at most one entry and the
 * scan needs no seen set (BarkQueryFlags.oneentry).
 *
 * A path the projection does not keep has no keys, though its documents
 * may match, so the scan then reads every key and the NULL entries (where
 * a document with no kept key is) and returns each row with a recheck,
 * procedure 9 answering maybe.
 */
static void
bark_wildcard_bounds(const BarkWildcardProjection *proj, Datum querydatum,
					 StrategyNumber strategy, BarkBoundary **boundaries,
					 int32 *nboundaries, bool *recheck, BarkQueryFlags *flags)
{
	BarkBoundary *b = palloc0_object(BarkBoundary);
	BarkSearchElement *e = palloc0_array(BarkSearchElement, 4);
	Jsonb	   *query = NULL;
	JsonbValue	value = {0};
	char	   *path = NULL;

	if (strategy == BARK_WC_EXISTS || strategy == BARK_WC_ORDER)
		path = text_to_cstring(DatumGetTextPP(querydatum));
	else if (strategy >= BARK_WC_LT && strategy <= BARK_WC_GT)
	{
		query = DatumGetJsonbP(querydatum);
		bark_wildcard_query(query, &path, &value);
	}
	else
		elog(ERROR, "bark_wildcard_extract_query: unknown strategy number: %d",
			 strategy);

	*boundaries = b;
	*nboundaries = 1;
	*recheck = false;
	if (!bark_wildcard_keeps(proj, path))
	{
		if (strategy == BARK_WC_ORDER)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot sort by path \"%s\", which the wildcard index's projection does not keep",
							path)));
		*recheck = true;
		flags->searchnulls = true;
		return;
	}
	if (flags->index != NULL)
		flags->oneentry = !bark_wildcard_marked(flags->index, path);

	if (strategy == BARK_WC_EXISTS || strategy == BARK_WC_ORDER)
	{
		flags->orderrest = (strategy == BARK_WC_ORDER);
		bark_wildcard_bracket_ends(path, jbvNull, &e[0], &e[1]);
		bark_wildcard_bracket_ends(path, jbvObject, &e[2], &e[3]);
		b->lower = &e[0];
		b->upper = &e[3];
		return;
	}

	bark_wildcard_bracket_ends(path, bark_wildcard_bracket(&value), &e[0],
							   &e[1]);
	e[2].argument = JsonbPGetDatum(query);
	e[2].strategy = strategy;
	switch (strategy)
	{
		case BARK_WC_LT:
		case BARK_WC_LE:
			b->lower = &e[0];
			b->upper = &e[2];
			break;
		case BARK_WC_EQ:
			b->lower = b->upper = &e[2];
			break;
		default:
			b->lower = &e[2];
			b->upper = &e[1];
			break;
	}
}

/*
 * Procedure 8, with BarkQueryFlags: see bark_wildcard_bounds.  The query
 * is the operator's right operand, jsonb [path, value] or, for #?, a text
 * path.
 */
Datum
bark_wildcard_extract_query(PG_FUNCTION_ARGS)
{
	BarkQueryFlags noflags = {0};

	bark_wildcard_bounds(bark_wildcard_projection(fcinfo), PG_GETARG_DATUM(0),
						 PG_GETARG_UINT16(1),
						 (BarkBoundary **) PG_GETARG_POINTER(2),
						 (int32 *) PG_GETARG_POINTER(3),
						 (bool *) PG_GETARG_POINTER(5),
						 PG_NARGS() >= 7 ?
						 (BarkQueryFlags *) PG_GETARG_POINTER(6) : &noflags);
	PG_RETURN_VOID();
}

/*
 * Procedure 9: asked only for a query on a path the projection drops, whose
 * scan reads every key; one key of another path cannot decide the row.
 */
Datum
bark_wildcard_recheck(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT16(BARK_RECHECK_MAYBE);
}

static void
bark_wildcard_append_element(StringInfo buf, const BarkSearchElement *e)
{
	static const char *const ops[] = {"?", "<", "<=", "=", ">=", ">"};

	appendStringInfo(buf, "%s %s",
					 e->strategy <= BTGreaterStrategyNumber ? ops[e->strategy] : "?",
					 JsonbToCString(NULL, &DatumGetJsonbP(e->argument)->root, 0));
}

/*
 * bark_wildcard_boundaries(query, strategy, include, exclude) -> text: what
 * procedure 8 returns for a query (jsonb, or text for #?) under the given
 * projection (text for #? and |<|), with BarkQueryFlags zeroed first as
 * BARK zeroes them.
 */
Datum
bark_wildcard_boundaries(PG_FUNCTION_ARGS)
{
	BarkBoundary *boundaries = NULL;
	int32		nboundaries = 0;
	bool		recheck = false;
	BarkQueryFlags flags = {0};
	StringInfoData buf;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();
	bark_wildcard_bounds(bark_wildcard_projection_args(fcinfo, 2),
						 PG_GETARG_DATUM(0), PG_GETARG_INT16(1), &boundaries,
						 &nboundaries, &recheck, &flags);
	initStringInfo(&buf);
	for (int i = 0; i < nboundaries; i++)
	{
		appendStringInfoChar(&buf, '(');
		if (boundaries[i].lower != NULL)
			bark_wildcard_append_element(&buf, boundaries[i].lower);
		else
			appendStringInfoString(&buf, "-inf");
		appendStringInfoString(&buf, ", ");
		if (boundaries[i].upper != NULL)
			bark_wildcard_append_element(&buf, boundaries[i].upper);
		else
			appendStringInfoString(&buf, "+inf");
		appendStringInfoString(&buf, ") ");
	}
	appendStringInfo(&buf, "recheck=%s searchnulls=%s", recheck ? "t" : "f",
					 flags.searchnulls ? "t" : "f");
	if (flags.orderrest)
		appendStringInfoString(&buf, " orderrest=t");
	if (recheck)
		appendStringInfo(&buf, " proc9=%d",
						 DatumGetInt16(DirectFunctionCall3(bark_wildcard_recheck,
														   (Datum) 0,
														   Int16GetDatum(PG_GETARG_INT16(1)),
														   (Datum) 0)));
	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

/* Does key satisfy one end of a boundary (none: unbounded), by jsonb_cmp? */
static bool
bark_wildcard_satisfies(Jsonb *key, const BarkSearchElement *e)
{
	int			c;

	if (e == NULL)
		return true;
	c = compareJsonbContainers(&key->root, &DatumGetJsonbP(e->argument)->root);
	switch (e->strategy)
	{
		case BTLessStrategyNumber:
			return c < 0;
		case BTLessEqualStrategyNumber:
			return c <= 0;
		case BTEqualStrategyNumber:
			return c == 0;
		case BTGreaterEqualStrategyNumber:
			return c >= 0;
		case BTGreaterStrategyNumber:
			return c > 0;
	}
	elog(ERROR, "unexpected boundary strategy %d", e->strategy);
	return false;				/* keep compiler quiet */
}

/*
 * bark_wildcard_in_boundaries(key, query, strategy) -> bool: does the key
 * lie inside a boundary that procedure 8 returns for the query, with no
 * projection?  So the tests can check the boundaries against jsonb_cmp's
 * order of real keys, including the ends no SQL value can spell
 * ([path, -Infinity]).
 */
Datum
bark_wildcard_in_boundaries(PG_FUNCTION_ARGS)
{
	static const BarkWildcardProjection all = {0};
	Jsonb	   *key = PG_GETARG_JSONB_P(0);
	BarkBoundary *boundaries = NULL;
	int32		nboundaries = 0;
	bool		recheck = false;
	BarkQueryFlags flags = {0};

	bark_wildcard_bounds(&all, PG_GETARG_DATUM(1), PG_GETARG_INT16(2),
						 &boundaries, &nboundaries, &recheck, &flags);
	for (int i = 0; i < nboundaries; i++)
	{
		if (bark_wildcard_satisfies(key, boundaries[i].lower) &&
			bark_wildcard_satisfies(key, boundaries[i].upper))
			PG_RETURN_BOOL(true);
	}
	PG_RETURN_BOOL(false);
}

/*
 * bark_wildcard_entries(index) -> setof (key jsonb, tid tid, marker bool):
 * every (key, heap TID) member of a BARK index's wildcard column, walking
 * the leaves left to right, a NULL entry's key as NULL.  amcheck's
 * heapallindexed refuses an index with an extracted column, so the tests
 * compare these with bark_wildcard_keys over the heap instead.
 */
Datum
bark_wildcard_entries(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Relation	rel = relation_open(PG_GETARG_OID(0), AccessShareLock);
	TupleDesc	itupdesc = RelationGetDescr(rel);
	int			attno;
	BlockNumber blkno;
	Buffer		buf;

	if (rel->rd_rel->relkind != RELKIND_INDEX ||
		rel->rd_rel->relam != BARK_AM_OID ||
		(attno = bark_index_extracted_column(rel)) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a BARK index with a wildcard column",
						RelationGetRelationName(rel))));
	InitMaterializedSRF(fcinfo, 0);

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
				key = index_getattr(full, attno, itupdesc, &keynull);
				ntids = bark_entry_get_tids(itup, tids, ntids);
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
