/*-------------------------------------------------------------------------
 *
 * prop_citext2.c
 *	  Property-based tests for citext operators and functions beyond the
 *	  basic equality/ordering contract (LIKE, DISTINCT/GROUP BY folding,
 *	  concat, aggregates).
 *
 * citext compares and hashes case-insensitively while storing the original
 * bytes.  The oracle throughout is lower()-folded plain text: LIKE on citext
 * must match lower() LIKE lower(); counting distinct citext values must equal
 * counting distinct lower() values; min/max fold the same way.  Any
 * disagreement is a bug in citext's operators or in the aggregate/sort paths
 * that use its comparison function.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_citext2.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "citext2"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Connect to the test cluster and ensure the citext extension exists.  Each
 * property opens its own connection (the Tier-B pattern); CREATE EXTENSION IF
 * NOT EXISTS makes the DDL idempotent across the thousands of per-case
 * connections.  On any failure the property is failed with the message.
 */
static pg_prop_conn *
connect_citext(pg_prop_tc *tc)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	if (!pg_prop_sql_exec(conn,
						  "CREATE EXTENSION IF NOT EXISTS citext",
						  &err))
		pg_prop_fail(tc, "setup failed (citext): %s", err ? err : "");

	return conn;
}

/*
 * Draw 0..10 characters of mixed-case ASCII from "abcABC" into buf as a bare
 * string (no quoting).  The alphabet mixes upper and lower case so case
 * folding is exercised; every character is single-byte so lower()/upper() are
 * exact oracles and the strings contain no LIKE metacharacters (so LIKE
 * reduces to case-insensitive equality).
 */
static void
draw_mixed(pg_prop_tc *tc, StringInfo buf)
{
	static const char letters[] = "abcABC";
	int			n = (int) pg_prop_draw_int(tc, 0, 10);

	for (int i = 0; i < n; i++)
		appendStringInfoChar(buf, letters[pg_prop_draw_int(tc, 0, 5)]);
}

/*
 * Append a bare string as a single-quoted SQL literal, doubling any embedded
 * single quote.  Our alphabet never contains a quote, but do it defensively
 * so the generated SQL is always well-formed.
 */
static void
append_sql_literal(StringInfo buf, const char *s)
{
	appendStringInfoChar(buf, '\'');
	for (const char *p = s; *p != '\0'; p++)
	{
		if (*p == '\'')
			appendStringInfoChar(buf, '\'');
		appendStringInfoChar(buf, *p);
	}
	appendStringInfoChar(buf, '\'');
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means a citext operator/function errored or produced NULL on
 * valid single-byte strings, which is itself a bug -- so either is a failure,
 * reported with the SQL.  Mirrors require_true in prop_numeric.c.
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
 * Build a one-column table ci_t(x text) with 0..12 mixed-case rows drawn from
 * "abcABC".  Rebuilt fresh in each property that needs it.  The rows are
 * plain text; properties cast x to citext in their queries so the folding
 * behaviour under aggregation/DISTINCT is exercised.  Failure is a harness
 * problem, not a property counterexample, so it fails the case directly.
 */
static void
build_ci_t(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows = (int) pg_prop_draw_int(tc, 0, 12);

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS ci_t; "
						   "CREATE TABLE ci_t (x text); ");
	for (int r = 0; r < nrows; r++)
	{
		StringInfoData v;

		initStringInfo(&v);
		draw_mixed(tc, &v);
		appendStringInfoString(&ddl, "INSERT INTO ci_t VALUES (");
		append_sql_literal(&ddl, v.data);
		appendStringInfoString(&ddl, "); ");
		pfree(v.data);
	}
	appendStringInfoString(&ddl, "ANALYZE ci_t;");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "build ci_t failed: %s", err ? err : "");

	pfree(ddl.data);
}

/*
 * citext equality folds case: a string equals both its uppercased and its
 * lowercased forms when compared as citext.
 *
 *		S::citext = upper(S)::citext AND S::citext = lower(S)::citext
 */
PG_PROP(citext_eq_case_insensitive)
{
	pg_prop_conn *conn;
	StringInfoData s,
				q;

	conn = connect_citext(tc);

	initStringInfo(&s);
	draw_mixed(tc, &s);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, "::citext = upper(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ")::citext) AND (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, "::citext = lower(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ")::citext)");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * LIKE on citext is case-insensitive.  With no wildcards in the pattern P,
 * citext LIKE reduces to case-insensitive equality, so it must agree with
 * plain-text LIKE on the lower()-folded strings:
 *
 *		(S::citext LIKE P::citext) = (lower(S) LIKE lower(P))
 *
 * Our alphabet contains no %, _ or backslash, so P is a literal pattern and
 * the identity is sound.  A folding bug in citext's LIKE operator fails here.
 */
PG_PROP(citext_like_insensitive)
{
	pg_prop_conn *conn;
	StringInfoData s,
				p,
				q;

	conn = connect_citext(tc);

	initStringInfo(&s);
	initStringInfo(&p);
	draw_mixed(tc, &s);
	draw_mixed(tc, &p);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, "::citext LIKE ");
	append_sql_literal(&q, p.data);
	appendStringInfoString(&q, "::citext) = (lower(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") LIKE lower(");
	append_sql_literal(&q, p.data);
	appendStringInfoString(&q, "))");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(p.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * DISTINCT over citext folds case exactly like DISTINCT over lower()-folded
 * text.  Counting distinct x::citext must equal counting distinct lower(x)
 * over the same rows:
 *
 *		(SELECT count(DISTINCT x::citext) FROM ci_t)
 *	  = (SELECT count(DISTINCT lower(x))  FROM ci_t)
 *
 * Both aggregates return exactly one row even when ci_t is empty (count is 0
 * on both sides), so the scalar oracle is sound at the empty boundary.  A bug
 * in citext's hash/equality used by the DISTINCT path fails here.
 */
PG_PROP(citext_distinct_folds)
{
	pg_prop_conn *conn;

	conn = connect_citext(tc);
	build_ci_t(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(DISTINCT x::citext) FROM ci_t) "
				 "= (SELECT count(DISTINCT lower(x)) FROM ci_t)");

	pg_prop_disconnect(conn);
}

/*
 * citext ordering is case-insensitive, matching lower()-folded comparison:
 *
 *		(A::citext < B::citext) = (lower(A) < lower(B))
 *
 * A sign or short-circuit bug in citext's comparison routine (the one the
 * sort/aggregate paths use) fails here.
 */
PG_PROP(citext_order_matches_lower)
{
	pg_prop_conn *conn;
	StringInfoData a,
				b,
				q;

	conn = connect_citext(tc);

	initStringInfo(&a);
	initStringInfo(&b);
	draw_mixed(tc, &a);
	draw_mixed(tc, &b);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_sql_literal(&q, a.data);
	appendStringInfoString(&q, "::citext < ");
	append_sql_literal(&q, b.data);
	appendStringInfoString(&q, "::citext) = (lower(");
	append_sql_literal(&q, a.data);
	appendStringInfoString(&q, ") < lower(");
	append_sql_literal(&q, b.data);
	appendStringInfoString(&q, "))");

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Concatenation is plain text, not folded: concatenating two citext values
 * and casting the result back to text yields the raw concatenation of the
 * stored bytes (folding happens only for comparison, never for storage or
 * output):
 *
 *		(S::citext || T::citext)::text = (S || T)
 */
PG_PROP(citext_concat_preserves)
{
	pg_prop_conn *conn;
	StringInfoData s,
				t,
				q;

	conn = connect_citext(tc);

	initStringInfo(&s);
	initStringInfo(&t);
	draw_mixed(tc, &s);
	draw_mixed(tc, &t);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT ((");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, "::citext || ");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, "::citext)::text) = (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " || ");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, ")");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * min/max over citext use its comparison function; over any set of rows the
 * minimum must be <= the maximum.  Over an empty ci_t both aggregates are
 * NULL, so coalesce the comparison to true to keep the empty case sound:
 *
 *		coalesce(min(x::citext) <= max(x::citext), true)
 *
 * A comparison-direction bug in citext's min/max support would make min > max
 * and fail here.
 */
PG_PROP(citext_min_max)
{
	pg_prop_conn *conn;

	conn = connect_citext(tc);
	build_ci_t(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(min(x::citext) <= max(x::citext), true) "
				 "FROM ci_t");

	pg_prop_disconnect(conn);
}
