/*-------------------------------------------------------------------------
 *
 * prop_ltree.c
 *	  Property-based tests for the ltree contrib extension (label paths).
 *
 * ltree is a dotted label path (contrib/ltree); this exercises concatenation
 * length, ancestor (@>) semantics, subpath, nlevel, and text round-trip.
 *
 * The oracle is the type's own structure: a path is a sequence of labels, so
 * nlevel counts them, concatenation adds them, a prefix is an ancestor, and
 * the two sub-extractors (subpath / subltree) must agree on a single label.
 * These identities must hold with no slack; any "f" is a bug in ltree.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_ltree.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "ltree"
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
 * ltree is a contrib module built in the test tree; CREATE EXTENSION IF NOT
 * EXISTS makes the DDL idempotent across the per-case connections.  On any
 * failure the property is failed with the message.  Mirrors the connect
 * helper pattern in prop_record.c.
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
 * each 1..4 characters from [a-z], joined by '.'.  ltree labels are
 * alphanumeric/underscore; [a-z] is a safe subset that needs no escaping, so
 * the literal is always well-formed.  *nlabels receives the label count.
 */
static void
draw_ltree(pg_prop_tc *tc, StringInfo buf, int *nlabels)
{
	int			n = (int) pg_prop_draw_int(tc, 1, 5);
	int			i;

	appendStringInfoChar(buf, '\'');
	for (i = 0; i < n; i++)
	{
		int			nchars = (int) pg_prop_draw_int(tc, 1, 4);
		int			j;

		if (i > 0)
			appendStringInfoChar(buf, '.');
		for (j = 0; j < nchars; j++)
			appendStringInfoChar(buf,
								 (char) ('a' + pg_prop_draw_int(tc, 0, 25)));
	}
	appendStringInfoString(buf, "'::ltree");

	*nlabels = n;
}

/*
 * Text round-trip: ltree output parses back equal, (L)::text::ltree = L.
 */
PG_PROP(text_roundtrip)
{
	pg_prop_conn *conn;
	StringInfoData l,
				q;
	int			n;

	conn = connect_ltree(tc);

	initStringInfo(&l);
	draw_ltree(tc, &l, &n);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s)::text::ltree = (%s)",
					 l.data, l.data);

	require_true(tc, conn, q.data);

	pfree(l.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * nlevel(L) equals the number of labels n drawn into the path.
 */
PG_PROP(nlevel_matches_label_count)
{
	pg_prop_conn *conn;
	StringInfoData l,
				q;
	int			n;

	conn = connect_ltree(tc);

	initStringInfo(&l);
	draw_ltree(tc, &l, &n);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT nlevel(%s) = %d", l.data, n);

	require_true(tc, conn, q.data);

	pfree(l.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * ltree concatenation adds levels: nlevel(A || B) = nlevel(A) + nlevel(B).
 */
PG_PROP(concat_nlevel_additive)
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
 * A path is an ancestor (@>) of itself concatenated with anything:
 * A @> (A || B).  @> means "is ancestor of or equal", so a prefix of a path
 * is always an ancestor of it.
 */
PG_PROP(prefix_is_ancestor)
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
	appendStringInfo(&q, "SELECT %s @> (%s || %s)",
					 a.data, a.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The first k labels of a path form an ancestor of it: subpath(L, 0, k) @> L
 * for k drawn in 1..nlevel(L).  Offsets are 0-based, so subpath(L, 0, k) is
 * the k-label prefix.
 */
PG_PROP(subpath_prefix_is_ancestor)
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
	appendStringInfo(&q, "SELECT subpath(%s, 0, %d) @> %s",
					 l.data, k, l.data);

	require_true(tc, conn, q.data);

	pfree(l.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Ancestor is reflexive, and ancestry implies no-greater level: L @> L, and
 * if A @> B then nlevel(A) <= nlevel(B).  Boolean implication "a implies b"
 * is written (a <= b) for booleans (false <= anything, true <= true), so the
 * second clause is ((A @> B) <= (nlevel(A) <= nlevel(B))).  A and B are two
 * independent paths.
 */
PG_PROP(ancestor_reflexive_and_level)
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
					 "SELECT (%s @> %s) "
					 "AND ((%s @> %s) <= (nlevel(%s) <= nlevel(%s)))",
					 a.data, a.data,
					 a.data, b.data, a.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The two sub-extractors agree on a single label: subpath(L, k, 1) extracts
 * the label at position k and must equal subltree(L, k, k+1), for k in
 * 0..n-1.  Offsets are 0-based; subpath takes (offset, len) while subltree
 * takes (start, end), so a one-off bug in either breaks this cross-check.
 */
PG_PROP(index_access_matches_subpath)
{
	pg_prop_conn *conn;
	StringInfoData l,
				q;
	int			n;
	int			k;

	conn = connect_ltree(tc);

	initStringInfo(&l);
	draw_ltree(tc, &l, &n);
	k = (int) pg_prop_draw_int(tc, 0, n - 1);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT subpath(%s, %d, 1) = subltree(%s, %d, %d)",
					 l.data, k, l.data, k, k + 1);

	require_true(tc, conn, q.data);

	pfree(l.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
