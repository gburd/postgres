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
 * Every number in a key, an inner array's included, is written in one form,
 * its scale trimmed as trim_scale does (1.0 and 1.00 are written 1).
 * jsonb_cmp compares numbers with numeric_cmp, which ignores the scale, so
 * without this keys it finds equal could differ in their bytes; with it
 * they are identical, which is what lets the class declare equalimage and
 * BARK coalesce equal keys into LIST and POSTING entries.
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
#include "access/htup_details.h"
#include "access/relation.h"
#include "access/reloptions.h"
#include "access/stratnum.h"
#include "catalog/pg_am.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_statistic.h"
#include "commands/defrem.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "funcapi.h"
#include "nodes/primnodes.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/lsyscache.h"
#include "utils/selfuncs.h"
#include "utils/syscache.h"
#include "utils/tuplestore.h"
#include "utils/jsonb.h"
#include "utils/numeric.h"
#include "utils/rel.h"
#include "utils/varlena.h"

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
PG_FUNCTION_INFO_V1(bark_wildcard_sel);

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

/* A jsonb scalar with its number, if it is one, in canonical form. */
static void
bark_wildcard_canonical(JsonbValue *v)
{
	if (v->type == jbvNumeric)
		v->val.numeric = DatumGetNumeric(DirectFunctionCall1(numeric_trim_scale,
																	 NumericGetDatum(v->val.numeric)));
}

/*
 * The key [path, value], every number in value in canonical form (see the
 * file header).  A container (an empty one, or an array nested in an array)
 * is pushed token by token, to reach the numbers inside it.
 */
static Datum
bark_wildcard_key(const char *path, JsonbValue *value)
{
	JsonbInState state = {0};
	JsonbValue	v;

	v.type = jbvString;
	v.val.string.val = (char *) path;
	v.val.string.len = strlen(path);
	pushJsonbValue(&state, WJB_BEGIN_ARRAY, NULL);
	pushJsonbValue(&state, WJB_ELEM, &v);
	if (value->type != jbvBinary)
	{
		v = *value;
		bark_wildcard_canonical(&v);
		pushJsonbValue(&state, WJB_ELEM, &v);
	}
	else
	{
		JsonbIterator *it = JsonbIteratorInit(value->val.binary.data);
		JsonbIteratorToken tok;

		while ((tok = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
		{
			bark_wildcard_canonical(&v);
			pushJsonbValue(&state, tok, tok < WJB_BEGIN_ARRAY ? &v : NULL);
		}
	}
	pushJsonbValue(&state, WJB_END_ARRAY, NULL);
	return JsonbPGetDatum(JsonbValueToJsonb(state.result));
}

static void
bark_wildcard_add_key(BarkWildcardKeys *k, const char *path, JsonbValue *value)
{
	if (!bark_wildcard_keeps(k->proj, path))
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
	if (!bark_wildcard_keeps_marker(k->proj, path))
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
 * The keys and markers of doc, into k, whose proj is set.  A
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
 * The values the class indexes at one path of a document, as procedure 7
 * would find them, but found by descending to the path alone: each object
 * on the way is searched for the path's next member name only, so the cost
 * is that of the path, not of the document.
 */
typedef struct BarkWildcardFound
{
	char	  **segs;			/* the path's member names, unescaped */
	int		   *seglens;
	int			nsegs;
	JsonbValue *values;
	int			nvalues;
	int			maxvalues;
} BarkWildcardFound;

static void
bark_wildcard_found(BarkWildcardFound *f, JsonbValue *v)
{
	if (f->nvalues == f->maxvalues)
	{
		f->maxvalues = Max(8, f->maxvalues * 2);
		f->values = f->values == NULL ?
			palloc_array(JsonbValue, f->maxvalues) :
			repalloc_array(f->values, JsonbValue, f->maxvalues);
	}
	f->values[f->nvalues++] = *v;
}

/*
 * Split a path, written as bark_wildcard_child writes one, into its member
 * names.  "" has none.  A backslash that escapes neither a dot nor a
 * backslash never occurs in a path the class writes, so such a path has no
 * values; return false for it.
 */
static bool
bark_wildcard_split(BarkWildcardFound *f, const char *path)
{
	int			len = strlen(path);
	char	   *seg = palloc(len + 1);

	f->segs = palloc_array(char *, len + 1);
	f->seglens = palloc_array(int, len + 1);
	f->nsegs = 0;
	if (len == 0)
		return true;
	f->segs[0] = seg;
	f->seglens[0] = 0;
	for (int i = 0; i < len; i++)
	{
		if (path[i] == '\\')
		{
			if (i + 1 == len || (path[i + 1] != '.' && path[i + 1] != '\\'))
				return false;
			seg[f->seglens[f->nsegs]++] = path[++i];
		}
		else if (path[i] == '.')
		{
			seg += f->seglens[f->nsegs++];
			f->segs[f->nsegs] = seg;
			f->seglens[f->nsegs] = 0;
		}
		else
			seg[f->seglens[f->nsegs]++] = path[i];
	}
	f->nsegs++;
	return true;
}

/*
 * bark_wildcard_value's walk restricted to one path: v is a value at the
 * path's first `depth` member names; `inarray` as there.  A member named ""
 * of an object at path "" stays at "" (bark_wildcard_child), so at depth 0
 * it is entered too, and never consumes a name.
 */
static void
bark_wildcard_descend(BarkWildcardFound *f, int depth, JsonbValue *v,
					  bool inarray)
{
	JsonbContainer *c;

	check_stack_depth();
	if (v->type != jbvBinary)
	{
		if (depth == f->nsegs)
			bark_wildcard_found(f, v);
		return;
	}
	c = v->val.binary.data;
	if (JsonContainerIsObject(c))
	{
		JsonbValue	member;

		if (JsonContainerSize(c) == 0)
		{
			if (depth == f->nsegs)
				bark_wildcard_found(f, v);
			return;
		}
		if (depth == 0 && getKeyJsonValueFromContainer(c, "", 0, &member))
			bark_wildcard_descend(f, 0, &member, false);
		if (depth < f->nsegs && (depth > 0 || f->seglens[0] > 0) &&
			getKeyJsonValueFromContainer(c, f->segs[depth], f->seglens[depth],
										 &member))
			bark_wildcard_descend(f, depth + 1, &member, false);
		return;
	}
	/* an array: a nested one, or an empty one, is a value */
	if (inarray || JsonContainerSize(c) == 0)
	{
		if (depth == f->nsegs)
			bark_wildcard_found(f, v);
		return;
	}
	{
		JsonbIterator *it = JsonbIteratorInit(c);
		JsonbValue	elem;
		JsonbIteratorToken tok;

		while ((tok = JsonbIteratorNext(&it, &elem, true)) != WJB_DONE)
			if (tok == WJB_ELEM)
				bark_wildcard_descend(f, depth, &elem, true);
	}
}

/* The values the class indexes at `path` of doc, into f. */
static void
bark_wildcard_find(BarkWildcardFound *f, Jsonb *doc, const char *path)
{
	JsonbValue	v;

	memset(f, 0, sizeof(*f));
	if (!bark_wildcard_split(f, path))
		return;
	if (JB_ROOT_IS_SCALAR(doc))
	{
		if (!JsonbExtractScalar(&doc->root, &v))
			elog(ERROR, "could not extract a scalar from a scalar jsonb");
	}
	else
	{
		v.type = jbvBinary;
		v.val.binary.data = &doc->root;
		v.val.binary.len = VARSIZE(doc) - VARHDRSZ;
	}
	bark_wildcard_descend(f, 0, &v, false);
}

/*
 * Compare two values found at one path as jsonb_cmp compares their keys
 * [path, value]: by type bracket, then within it as jsonb compares scalars
 * (strings in the default collation, as jsonb_cmp does) or containers.
 */
static int
bark_wildcard_value_cmp(JsonbValue *a, JsonbValue *b)
{
	int			ba = bark_wildcard_bracket(a);
	int			bb = bark_wildcard_bracket(b);

	if (ba != bb)
		return ba > bb ? 1 : -1;
	switch (a->type)
	{
		case jbvNull:
			return 0;
		case jbvString:
			return varstr_cmp(a->val.string.val, a->val.string.len,
							  b->val.string.val, b->val.string.len,
							  DEFAULT_COLLATION_OID);
		case jbvNumeric:
			return DatumGetInt32(DirectFunctionCall2(numeric_cmp,
													 NumericGetDatum(a->val.numeric),
													 NumericGetDatum(b->val.numeric)));
		case jbvBool:
			return (int) a->val.boolean - (int) b->val.boolean;
		case jbvBinary:
			return compareJsonbContainers(a->val.binary.data, b->val.binary.data);
		default:
			elog(ERROR, "unexpected jsonb value type %d", a->type);
	}
	return 0;					/* keep compiler quiet */
}

/*
 * The comparison operators, which are the heap recheck: is some value the
 * class indexes at the query's path in the query's type bracket, and does
 * it compare with the query's value as the strategy asks?  Within a path,
 * keys [path, value] order as their values (bark_wildcard_value_cmp), so
 * the operators and the index agree on the order.
 */
static bool
bark_wildcard_compare(Jsonb *doc, Jsonb *query, StrategyNumber strategy)
{
	BarkWildcardFound f;
	JsonbValue	qvalue;
	char	   *path;
	int			bracket;

	bark_wildcard_query(query, &path, &qvalue);
	bracket = bark_wildcard_bracket(&qvalue);
	bark_wildcard_find(&f, doc, path);
	for (int i = 0; i < f.nvalues; i++)
	{
		int			c;

		if (bark_wildcard_bracket(&f.values[i]) != bracket)
			continue;
		c = bark_wildcard_value_cmp(&f.values[i], &qvalue);
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
	BarkWildcardFound f;

	bark_wildcard_find(&f, PG_GETARG_JSONB_P(0),
					   text_to_cstring(PG_GETARG_TEXT_PP(1)));
	PG_RETURN_BOOL(f.nvalues > 0);
}

/*
 * doc |<| path: the smallest key [path, value] the class indexes at the
 * path, or NULL.
 */
Datum
bark_wildcard_least(PG_FUNCTION_ARGS)
{
	char	   *path = text_to_cstring(PG_GETARG_TEXT_PP(1));
	BarkWildcardFound f;
	JsonbValue *least = NULL;

	bark_wildcard_find(&f, PG_GETARG_JSONB_P(0), path);
	for (int i = 0; i < f.nvalues; i++)
		if (least == NULL || bark_wildcard_value_cmp(&f.values[i], least) < 0)
			least = &f.values[i];
	if (least == NULL)
		PG_RETURN_NULL();
	PG_RETURN_DATUM(bark_wildcard_key(path, least));
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

	if (!bark_relation_is_bark(rel) ||
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

/*
 * The strategy number an operator holds in the wildcard operator family
 * (BARK_WC_LT .. BARK_WC_EXISTS), or 0 if it holds none -- which only
 * happens if the estimator is attached to the wrong operator.
 */
static StrategyNumber
bark_wildcard_strategy(Oid operator)
{
	Oid			barkam = get_index_am_oid("bark", false);
	CatCList   *catlist;
	StrategyNumber strategy = 0;

	catlist = SearchSysCacheList1(AMOPOPID, ObjectIdGetDatum(operator));
	for (int i = 0; i < catlist->n_members; i++)
	{
		Form_pg_amop aform = (Form_pg_amop) GETSTRUCT(&catlist->members[i]->tuple);

		if (aform->amopmethod == barkam)
		{
			strategy = aform->amopstrategy;
			break;
		}
	}
	ReleaseSysCacheList(catlist);
	return strategy;
}

/*
 * bark_wildcard_sel(root, operator, args, varRelid) -> float8: the
 * restriction selectivity of a wildcard operator, registered as each
 * operator's RESTRICT in place of matchingsel.
 *
 * matchingsel estimates by applying the operator to every row of the
 * column's statistics sample (generic_restriction_selectivity).  That is
 * sound for an operator the planner has standard statistics for, but a
 * jsonb column's MCV and histogram describe whole documents, not the
 * per-path values these operators test, so the sample buys no accuracy
 * here; it only runs the operator -- about ten times the cost of jsonb's
 * own containment per document -- once per sample row, and that cost grows
 * with default_statistics_target.  BENCHMARK-M5b.md measured a #= planning
 * at 0.25 to 0.68 ms against GIN's 0.04 to 0.08, almost all of it this
 * per-row cost.
 *
 * So estimate without touching the sample: a fixed selectivity per
 * strategy, equality the most selective, a range the least, path existence
 * in between, each the default the planner uses for its kind, scaled down
 * by the column's NULL fraction.  The estimate is coarse, but these
 * queries drive the index-vs-seqscan choice the same way the sample-based
 * estimate did (equality and existence stay selective enough for an index
 * or bitmap scan, a range stays below a seqscan's cost), and planning no
 * longer scans the sample.
 */
Datum
bark_wildcard_sel(PG_FUNCTION_ARGS)
{
	PlannerInfo *root = (PlannerInfo *) PG_GETARG_POINTER(0);
	Oid			operator = PG_GETARG_OID(1);
	List	   *args = (List *) PG_GETARG_POINTER(2);
	int			varRelid = PG_GETARG_INT32(3);
	VariableStatData vardata;
	Node	   *other;
	bool		varonleft;
	double		selec;
	double		nullfrac = 0.0;
	StrategyNumber strategy;

	/*
	 * Not "variable OP constant" (or the mirror): fall back to the generic
	 * default, as matchingsel does for such an expression.
	 */
	if (!get_restriction_variable(root, args, varRelid,
								  &vardata, &other, &varonleft))
		PG_RETURN_FLOAT8(DEFAULT_MATCHING_SEL);

	/* A NULL constant: the operators are strict, so nothing matches. */
	if (IsA(other, Const) && ((Const *) other)->constisnull)
	{
		ReleaseVariableStats(vardata);
		PG_RETURN_FLOAT8(0.0);
	}

	/* A non-constant right operand: nothing to key an estimate on. */
	if (!IsA(other, Const))
	{
		ReleaseVariableStats(vardata);
		PG_RETURN_FLOAT8(DEFAULT_MATCHING_SEL);
	}

	/* The documents that are NULL match nothing, so discount them. */
	if (HeapTupleIsValid(vardata.statsTuple))
		nullfrac = ((Form_pg_statistic) GETSTRUCT(vardata.statsTuple))->stanullfrac;

	strategy = bark_wildcard_strategy(operator);
	switch (strategy)
	{
		case BARK_WC_EQ:
			/* Equality on a path's value: as selective as a scalar =. */
			selec = DEFAULT_EQ_SEL;
			break;
		case BARK_WC_LT:
		case BARK_WC_LE:
		case BARK_WC_GE:
		case BARK_WC_GT:
			/* A range on a path's value: as a scalar inequality. */
			selec = DEFAULT_INEQ_SEL;
			break;
		case BARK_WC_EXISTS:
			/* Path existence: a middling fraction of documents. */
			selec = DEFAULT_MATCHING_SEL;
			break;
		default:
			selec = DEFAULT_MATCHING_SEL;
			break;
	}

	selec *= 1.0 - nullfrac;
	ReleaseVariableStats(vardata);

	CLAMP_PROBABILITY(selec);
	PG_RETURN_FLOAT8((float8) selec);
}
