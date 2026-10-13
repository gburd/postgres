/*-------------------------------------------------------------------------
 *
 * prop_ltree2.c
 *	  Property-based tests for ltree query operators and sub-extractors.
 *
 * ltree is a dotted label path (contrib/ltree).  This file is distinct from
 * prop_ltree.c: it exercises the query side -- lquery matching (~), the
 * subltree/subpath extractors, index() position lookup, and the ancestor
 * operators (@>, <@) -- rather than concatenation/round-trip algebra.
 *
 * The oracle is the type's own structure: a prefix is an ancestor, the
 * whole path is subpath from offset 0, nlevel counts labels, index finds a
 * path within itself at 0, and the match-anything lquery '*' matches every
 * ltree.  These identities hold with no slack; any "f" is a bug in ltree.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_ltree2.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "ltree2"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means an ltree function errored or produced NULL on valid
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
 * Connect to the test cluster and make sure the ltree extension exists.
 * CREATE EXTENSION IF NOT EXISTS makes the DDL idempotent across the per-case
 * connections.  On any failure the property is failed with the message.
 */
static pg_prop_conn *
connect_ltree(pg_prop_tc *tc)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	if (!pg_prop_sql_exec(conn,
						  "CREATE EXTENSION IF NOT EXISTS ltree",
						  &err))
		pg_prop_fail(tc, "setup failed (ltree): %s", err ? err : "");

	return conn;
}

/*
 * Draw an ltree path literal into buf as 'l1.l2...'::ltree, with 1..5 labels
 * each 1..3 characters from [a-c], joined by '.'.  [a-c] always begins with a
 * letter and needs no escaping, so the literal is always well-formed.
 * *nlabels receives the label count.
 */
static void
draw_ltree(pg_prop_tc *tc, StringInfo buf, int *nlabels)
{
	int			n = (int) pg_prop_draw_int(tc, 1, 5);
	int			i;

	appendStringInfoChar(buf, '\'');
	for (i = 0; i < n; i++)
	{
		int			nchars = (int) pg_prop_draw_int(tc, 1, 3);
		int			j;

		if (i > 0)
			appendStringInfoChar(buf, '.');
		for (j = 0; j < nchars; j++)
			appendStringInfoChar(buf,
								 (char) ('a' + pg_prop_draw_int(tc, 0, 2)));
	}
	appendStringInfoString(buf, "'::ltree");

	*nlabels = n;
}

/*
 * subltree(L, 0, k) is the k-label prefix of L, and a prefix is an ancestor:
 * subltree(L, 0, k) @> L for k drawn in 1..nlevel(L).  Offsets are 0-based.
 */
PG_PROP(subltree_prefix)
{
	pg_prop_conn *conn;
	StringInfoData l,
				q;
	int			n;
	int			k;

	conn = connect_ltree(tc);

	initStringInfo(&l);
	draw_ltree(tc, &l, &n);
	k = (int) pg_prop_draw_int(tc, 1, n);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT subltree(%s, 0, %d) @> %s",
					 l.data, k, l.data);

	require_true(tc, conn, q.data);

	pfree(l.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * subpath from offset 0 with no length returns the whole path:
 * subpath(L, 0) = L.
 */
PG_PROP(subpath_roundtrip)
{
	pg_prop_conn *conn;
	StringInfoData l,
				q;
	int			n;

	conn = connect_ltree(tc);

	initStringInfo(&l);
	draw_ltree(tc, &l, &n);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT subpath(%s, 0) = %s", l.data, l.data);

	require_true(tc, conn, q.data);

	pfree(l.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The k-label prefix has exactly k levels: nlevel(subpath(L, 0, k)) = k for k
 * drawn in 0..nlevel(L).  k = 0 yields the empty path with nlevel 0.
 */
PG_PROP(nlevel_subpath)
{
	pg_prop_conn *conn;
	StringInfoData l,
				q;
	int			n;
	int			k;

	conn = connect_ltree(tc);

	initStringInfo(&l);
	draw_ltree(tc, &l, &n);
	k = (int) pg_prop_draw_int(tc, 0, n);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT nlevel(subpath(%s, 0, %d)) = %d",
					 l.data, k, k);

	require_true(tc, conn, q.data);

	pfree(l.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A path contains itself at position 0: index(L, L) = 0.  index returns the
 * 0-based position of the first match of the second path within the first.
 */
PG_PROP(index_of_self)
{
	pg_prop_conn *conn;
	StringInfoData l,
				q;
	int			n;

	conn = connect_ltree(tc);

	initStringInfo(&l);
	draw_ltree(tc, &l, &n);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT index(%s, %s) = 0", l.data, l.data);

	require_true(tc, conn, q.data);

	pfree(l.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * ltree concatenation adds levels: nlevel(A || B) = nlevel(A) + nlevel(B).
 */
PG_PROP(concat_nlevel)
{
	pg_prop_conn *conn;
	StringInfoData a,
				b,
				q;
	int			na,
				nb;

	conn = connect_ltree(tc);

	initStringInfo(&a);
	initStringInfo(&b);
	draw_ltree(tc, &a, &na);
	draw_ltree(tc, &b, &nb);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT nlevel(%s || %s) = nlevel(%s) + nlevel(%s)",
					 a.data, b.data, a.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The match-anything lquery '*' matches every ltree: L ~ '*'::lquery.
 */
PG_PROP(lquery_star_matches)
{
	pg_prop_conn *conn;
	StringInfoData l,
				q;
	int			n;

	conn = connect_ltree(tc);

	initStringInfo(&l);
	draw_ltree(tc, &l, &n);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT %s ~ '*'::lquery", l.data);

	require_true(tc, conn, q.data);

	pfree(l.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The first label followed by anything matches the path:
 * L ~ (subltree(L, 0, 1)::text || '.*')::lquery, for nlevel(L) >= 1 (always
 * true here, paths have 1..5 labels).  subltree(L, 0, 1) is the literal first
 * label, which [a-c] guarantees is a valid lquery atom needing no escaping.
 */
PG_PROP(lquery_prefix_matches)
{
	pg_prop_conn *conn;
	StringInfoData l,
				q;
	int			n;

	conn = connect_ltree(tc);

	initStringInfo(&l);
	draw_ltree(tc, &l, &n);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT %s ~ (subltree(%s, 0, 1)::text || '.*')::lquery",
					 l.data, l.data);

	require_true(tc, conn, q.data);

	pfree(l.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Ancestor is antisymmetric: if A @> B and B @> A then A = B.  Tested on L
 * and itself, where both directions hold: L @> L AND L <@ L.  <@ is the
 * descendant operator (the converse of @>).
 */
PG_PROP(ancestor_antisymmetric)
{
	pg_prop_conn *conn;
	StringInfoData l,
				q;
	int			n;

	conn = connect_ltree(tc);

	initStringInfo(&l);
	draw_ltree(tc, &l, &n);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s @> %s) AND (%s <@ %s)",
					 l.data, l.data, l.data, l.data);

	require_true(tc, conn, q.data);

	pfree(l.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
