/*-------------------------------------------------------------------------
 *
 * prop_hstore.c
 *	  Property-based tests for the hstore contrib key/value map type.
 *
 * hstore is a key/value map stored as a single value; this exercises its
 * text round-trip, the || merge (right operand wins on key conflict),
 * key/value extraction (akeys, exist, ->), key deletion (delete), and
 * containment (@>), over generated maps with distinct keys.
 *
 * The oracle is hstore's own documented algebra: a map parsed back from its
 * text output must equal the original, a merge must prefer the right
 * operand's value on a shared key, akeys must report every distinct key,
 * exist must agree with whether a key is defined, delete must drop exactly
 * one key, and @> must hold reflexively.  Keys are generated distinct (k0,
 * k1, ...) so there is no dedup ambiguity and cardinality(akeys()) equals the
 * number of keys drawn.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.  connect_hstore() creates the hstore
 * extension (built in the test tree) before any hstore SQL runs.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_hstore.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "hstore"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the hstore operation errored or produced NULL on valid
 * inputs, which is itself a bug -- so either is a failure, reported with the
 * SQL.  Mirrors require_true in prop_numeric.c.
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
 * NOT EXISTS is idempotent, so it is safe to run per connection.  The hstore
 * contrib module is built in the test tree, so this must succeed; on any
 * failure the property is failed with the message.
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
 * and single-character [a-z] values, rendered as
 *
 *		'"k0"=>"va", "k1"=>"vb"'::hstore
 *
 * The whole literal is single-quoted; the key/value text uses only [a-z] and
 * the k<index> key names, so no single quote ever appears inside and the
 * literal is always well-formed.  Keys are distinct by construction, so there
 * is no dedup ambiguity and cardinality(akeys()) equals nkeys.
 *
 * If vals is non-NULL, vals[i] (a caller-provided buffer of at least 2 bytes)
 * receives the drawn [a-z] value character for ki, NUL-terminated, so a
 * property can cross-check H -> 'ki' against the exact value stored.
 */
static void
draw_hstore(pg_prop_tc *tc, StringInfo buf, int nkeys, char (*vals)[2])
{
	int			i;

	appendStringInfoChar(buf, '\'');
	for (i = 0; i < nkeys; i++)
	{
		char		v = (char) ('a' + pg_prop_draw_int(tc, 0, 25));

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
 * Text round-trip: an hstore cast to text and parsed back is the same map.
 * hstore_out must render the map so hstore_in parses it to an equal value; a
 * quoting or escaping bug breaks this.
 *
 *		SELECT (H)::text::hstore = H
 */
PG_PROP(text_roundtrip)
{
	pg_prop_conn *conn;
	StringInfoData h,
				q;
	int			nkeys = (int) pg_prop_draw_int(tc, 0, 6);

	conn = connect_hstore(tc);

	initStringInfo(&h);
	draw_hstore(tc, &h, nkeys, NULL);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s)::text::hstore = (%s)", h.data, h.data);

	require_true(tc, conn, q.data);

	pfree(h.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Concat right operand wins: in A || B, every key present in B maps to B's
 * value.  B is built to contain at least k0, so k0 is a shared/overriding key
 * and (A || B) -> 'k0' must equal B -> 'k0':
 *
 *		SELECT (A || B) -> 'k0' IS NOT DISTINCT FROM (B -> 'k0')
 *
 * IS NOT DISTINCT FROM so a NULL value compares equal (values are [a-z] here,
 * but this keeps the comparison two-valued regardless).
 */
PG_PROP(concat_right_wins)
{
	pg_prop_conn *conn;
	StringInfoData a,
				b,
				q;
	int			na = (int) pg_prop_draw_int(tc, 1, 6);
	int			nb = (int) pg_prop_draw_int(tc, 1, 6);

	conn = connect_hstore(tc);

	initStringInfo(&a);
	initStringInfo(&b);
	draw_hstore(tc, &a, na, NULL);
	draw_hstore(tc, &b, nb, NULL);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%s) || (%s)) -> 'k0' "
					 "IS NOT DISTINCT FROM ((%s) -> 'k0')",
					 a.data, b.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * akeys cardinality: akeys(H) returns the key array, whose cardinality equals
 * the number of distinct keys drawn.  Keys are distinct by construction, so
 * no dedup collapses the count:
 *
 *		SELECT cardinality(akeys(H)) = <n>
 */
PG_PROP(akeys_count_matches)
{
	pg_prop_conn *conn;
	StringInfoData h,
				q;
	int			nkeys = (int) pg_prop_draw_int(tc, 0, 6);

	conn = connect_hstore(tc);

	initStringInfo(&h);
	draw_hstore(tc, &h, nkeys, NULL);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT cardinality(akeys(%s)) = %d", h.data, nkeys);

	require_true(tc, conn, q.data);

	pfree(h.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * exist agrees with definedness: for a key k0 present in H, exist(H, 'k0') is
 * true, and for 'zzz' (never generated -- keys are k<index>) it is false:
 *
 *		SELECT exist(H, 'k0') AND NOT exist(H, 'zzz')
 *
 * nkeys >= 1 so k0 is always present; 'zzz' lies outside the k<index> key
 * namespace, so it is guaranteed absent.
 */
PG_PROP(exist_matches_defined)
{
	pg_prop_conn *conn;
	StringInfoData h,
				q;
	int			nkeys = (int) pg_prop_draw_int(tc, 1, 6);

	conn = connect_hstore(tc);

	initStringInfo(&h);
	draw_hstore(tc, &h, nkeys, NULL);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT exist(%s, 'k0') AND NOT exist(%s, 'zzz')",
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
	int			nkeys = (int) pg_prop_draw_int(tc, 0, 6);

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

/*
 * delete removes exactly one key: delete(H, 'k0') no longer has k0 and has
 * one fewer key.  nkeys >= 1 so k0 is present and the expected count is n-1:
 *
 *		SELECT NOT exist(delete(H, 'k0'), 'k0')
 *			AND cardinality(akeys(delete(H, 'k0'))) = <n> - 1
 */
PG_PROP(delete_removes_key)
{
	pg_prop_conn *conn;
	StringInfoData h,
				q;
	int			nkeys = (int) pg_prop_draw_int(tc, 1, 6);

	conn = connect_hstore(tc);

	initStringInfo(&h);
	draw_hstore(tc, &h, nkeys, NULL);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT NOT exist(delete(%s, 'k0'), 'k0') "
					 "AND cardinality(akeys(delete(%s, 'k0'))) = %d",
					 h.data, h.data, nkeys - 1);

	require_true(tc, conn, q.data);

	pfree(h.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Arrow get matches the stored value: H -> 'k0' equals the [a-z] value drawn
 * for k0.  The value is captured by draw_hstore into vals[0] and substituted
 * into the query, so this cross-checks hstore's -> against the exact literal
 * that was stored rather than comparing H to itself:
 *
 *		SELECT (H -> 'k0') = '<V>'
 */
PG_PROP(get_matches_arrow)
{
	pg_prop_conn *conn;
	StringInfoData h,
				q;
	int			nkeys = (int) pg_prop_draw_int(tc, 1, 6);
	char		vals[6][2];

	conn = connect_hstore(tc);

	initStringInfo(&h);
	draw_hstore(tc, &h, nkeys, vals);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ((%s) -> 'k0') = '%s'", h.data, vals[0]);

	require_true(tc, conn, q.data);

	pfree(h.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
