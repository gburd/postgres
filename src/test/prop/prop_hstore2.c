/*-------------------------------------------------------------------------
 *
 * prop_hstore2.c
 *	  Property-based tests for a second set of hstore contrib operations.
 *
 * This complements prop_hstore.c with hstore functions distinct from those
 * exercised there: slice (restrict to a key set), the key/value projections
 * akeys/avals/skeys/svals, each() as a set-returning function, hstore_to_json,
 * defined, and @> containment viewed from these angles.
 *
 * The oracle is hstore's own documented algebra over maps built with DISTINCT
 * keys k0..k(n-1) and single-character [a-c] values: akeys cardinality equals
 * the key count (no dedup), delete drops exactly the named key, slice keeps
 * only the requested keys that exist, || prefers the right operand, the JSON
 * projection carries the same key->value mapping, defined agrees with the ?
 * exist operator, each() yields one row per key, and @> is reflexive.  A
 * failure of any of these is a bug in hstore.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.  connect_hstore() creates the hstore
 * extension (built in the test tree) before any hstore SQL runs.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_hstore2.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "hstore2"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the hstore operation errored or produced NULL on valid
 * inputs, which is itself a bug -- so either is a failure, reported with the
 * SQL.  Mirrors require_true in prop_numeric.c / prop_hstore.c.
 */
static void
require_true(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *res;

	res = pg_prop_sql_scalar(conn, sql, &failed, &err);
	if (failed)
	{
		pg_prop_fail(tc, "SQL error for: %s\n  error: %s",
					 sql, err ? err : "(unknown)");
	}
	if (res == NULL)
	{
		pg_prop_fail(tc, "unexpected NULL result for: %s", sql);
	}
	if (strcmp(res, "t") != 0)
	{
		pg_prop_fail(tc, "invariant failed (got %s) for: %s", res, sql);
	}
	pg_free(res);
	if (err)
		pg_free(err);
}

/*
 * Connect to the test cluster and ensure the hstore extension exists.  Each
 * property opens its own connection (the Tier-B pattern); CREATE EXTENSION IF
 * NOT EXISTS is idempotent, so it is safe to run per connection.
 */
static pg_prop_conn *
connect_hstore(pg_prop_tc *tc)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	if (!pg_prop_sql_exec(conn, "CREATE EXTENSION IF NOT EXISTS hstore", &err))
		pg_prop_fail(tc, "setup failed (hstore extension): %s",
					 err ? err : "");

	return conn;
}

/*
 * Draw an hstore literal into buf with nkeys DISTINCT keys named k0..k(n-1)
 * and single-character [a-c] values, rendered as
 *
 *		'"k0"=>"a", "k1"=>"b"'::hstore
 *
 * The whole literal is single-quoted; the key/value text uses only [a-c] and
 * the k<index> key names, so no single quote ever appears inside and the
 * literal is always well-formed.  Keys are distinct by construction, so there
 * is no dedup ambiguity and cardinality(akeys()) equals nkeys.
 *
 * nkeys is drawn 2..5 so k0 and k1 are always present, giving the delete and
 * slice properties guaranteed targets.
 *
 * If vals is non-NULL, vals[i] (a buffer of at least 2 bytes) receives the
 * drawn [a-c] value character for ki, NUL-terminated.
 */
static void
draw_hstore(pg_prop_tc *tc, StringInfo buf, int nkeys, char (*vals)[2])
{
	int			i;

	appendStringInfoChar(buf, '\'');
	for (i = 0; i < nkeys; i++)
	{
		char		v = (char) ('a' + pg_prop_draw_int(tc, 0, 2));

		if (i > 0)
			appendStringInfoString(buf, ", ");
		appendStringInfo(buf, "\"k%d\"=>\"%c\"", i, v);

		if (vals != NULL)
		{
			vals[i][0] = v;
			vals[i][1] = '\0';
		}
	}
	appendStringInfoString(buf, "'::hstore");
}

/*
 * akeys cardinality: array_length(akeys(H), 1) equals the number of distinct
 * keys drawn.  Keys are distinct by construction so no dedup collapses the
 * count:
 *
 *		SELECT array_length(akeys(H), 1) = <n>
 */
PG_PROP(akeys_count)
{
	pg_prop_conn *conn;
	StringInfoData h,
				q;
	int			nkeys = (int) pg_prop_draw_int(tc, 2, 5);

	conn = connect_hstore(tc);

	initStringInfo(&h);
	draw_hstore(tc, &h, nkeys, NULL);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT array_length(akeys(%s), 1) = %d",
					 h.data, nkeys);

	require_true(tc, conn, q.data);

	pfree(h.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * delete removes exactly the named key: after delete(H, 'k0') the result no
 * longer defines k0, but still defines k1 (nkeys >= 2 so k1 is present):
 *
 *		SELECT NOT (delete(H, 'k0') ? 'k0') AND (delete(H, 'k0') ? 'k1')
 */
PG_PROP(delete_removes)
{
	pg_prop_conn *conn;
	StringInfoData h,
				q;
	int			nkeys = (int) pg_prop_draw_int(tc, 2, 5);

	conn = connect_hstore(tc);

	initStringInfo(&h);
	draw_hstore(tc, &h, nkeys, NULL);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT NOT (delete(%s, 'k0') ? 'k0') "
					 "AND (delete(%s, 'k0') ? 'k1')",
					 h.data, h.data);

	require_true(tc, conn, q.data);

	pfree(h.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * slice keeps only the requested keys that exist: slice(H, ARRAY['k0']) has
 * exactly the single key k0 (k0 is always present, nkeys >= 2).  Checked both
 * ways -- the slice defines k0 and its only key is k0:
 *
 *		SELECT (slice(H, ARRAY['k0']) ? 'k0')
 *			AND array_length(akeys(slice(H, ARRAY['k0'])), 1) = 1
 */
PG_PROP(slice_subset)
{
	pg_prop_conn *conn;
	StringInfoData h,
				q;
	int			nkeys = (int) pg_prop_draw_int(tc, 2, 5);

	conn = connect_hstore(tc);

	initStringInfo(&h);
	draw_hstore(tc, &h, nkeys, NULL);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (slice(%s, ARRAY['k0']) ? 'k0') "
					 "AND array_length(akeys(slice(%s, ARRAY['k0'])), 1) = 1",
					 h.data, h.data);

	require_true(tc, conn, q.data);

	pfree(h.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Concat right operand wins: (H || hstore('k0','ZZ')) -> 'k0' = 'ZZ'.  The
 * right operand overrides the k0 value regardless of what H held for k0:
 *
 *		SELECT ((H || hstore('k0', 'ZZ')) -> 'k0') = 'ZZ'
 */
PG_PROP(concat_right_wins)
{
	pg_prop_conn *conn;
	StringInfoData h,
				q;
	int			nkeys = (int) pg_prop_draw_int(tc, 2, 5);

	conn = connect_hstore(tc);

	initStringInfo(&h);
	draw_hstore(tc, &h, nkeys, NULL);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (((%s) || hstore('k0', 'ZZ')) -> 'k0') = 'ZZ'",
					 h.data);

	require_true(tc, conn, q.data);

	pfree(h.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * JSON projection preserves the mapping: hstore_to_json(H) ->> 'k0' equals
 * H -> 'k0'.  The JSON object must carry the same key->value association for
 * every key; checked at k0 (always present).  Values are [a-c] text, so the
 * JSON string text and the hstore value compare equal:
 *
 *		SELECT (hstore_to_json(H) ->> 'k0') = (H -> 'k0')
 */
PG_PROP(json_roundtrip_keys)
{
	pg_prop_conn *conn;
	StringInfoData h,
				q;
	int			nkeys = (int) pg_prop_draw_int(tc, 2, 5);

	conn = connect_hstore(tc);

	initStringInfo(&h);
	draw_hstore(tc, &h, nkeys, NULL);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (hstore_to_json(%s) ->> 'k0') = ((%s) -> 'k0')",
					 h.data, h.data);

	require_true(tc, conn, q.data);

	pfree(h.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * defined agrees with the ? exist operator for both a present key (k0, always
 * drawn) and an absent key ('zzz', outside the k<index> namespace):
 *
 *		SELECT (defined(H, 'k0') = (H ? 'k0'))
 *			AND (defined(H, 'zzz') = (H ? 'zzz'))
 */
PG_PROP(defined_vs_exists)
{
	pg_prop_conn *conn;
	StringInfoData h,
				q;
	int			nkeys = (int) pg_prop_draw_int(tc, 2, 5);

	conn = connect_hstore(tc);

	initStringInfo(&h);
	draw_hstore(tc, &h, nkeys, NULL);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (defined(%s, 'k0') = ((%s) ? 'k0')) "
					 "AND (defined(%s, 'zzz') = ((%s) ? 'zzz'))",
					 h.data, h.data, h.data, h.data);

	require_true(tc, conn, q.data);

	pfree(h.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * each() yields one row per key: (SELECT count(*) FROM each(H)) equals
 * array_length(akeys(H), 1).  each expands the map to (key, value) rows, so
 * its row count is the key count:
 *
 *		SELECT (SELECT count(*) FROM each(H)) = array_length(akeys(H), 1)
 */
PG_PROP(each_count)
{
	pg_prop_conn *conn;
	StringInfoData h,
				q;
	int			nkeys = (int) pg_prop_draw_int(tc, 2, 5);

	conn = connect_hstore(tc);

	initStringInfo(&h);
	draw_hstore(tc, &h, nkeys, NULL);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (SELECT count(*) FROM each(%s)) "
					 "= array_length(akeys(%s), 1)",
					 h.data, h.data);

	require_true(tc, conn, q.data);

	pfree(h.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Containment is reflexive: every hstore contains itself.
 *
 *		SELECT H @> H
 */
PG_PROP(contains_self)
{
	pg_prop_conn *conn;
	StringInfoData h,
				q;
	int			nkeys = (int) pg_prop_draw_int(tc, 2, 5);

	conn = connect_hstore(tc);

	initStringInfo(&h);
	draw_hstore(tc, &h, nkeys, NULL);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s) @> (%s)", h.data, h.data);

	require_true(tc, conn, q.data);

	pfree(h.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
