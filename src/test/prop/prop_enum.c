/*-------------------------------------------------------------------------
 *
 * prop_enum.c
 *	  Property-based tests for enum types (src/backend/utils/adt/enum.c).
 *
 * Enum ordering follows declaration order, and enum_range/enum_first/enum_last
 * reflect it.  Each property builds a random enum type per case and checks the
 * ordering and range invariants against that known declaration order.
 *
 * The oracle is the declaration order itself: labels are named "l0".."l(n-1)"
 * so a label's index is exactly its sort position.  Enum comparison, enum_first
 * and enum_last, and the two enum_range forms must agree with those indices --
 * '<' tracks index order, enum_first/last are the first/last labels, and
 * enum_range returns the inclusive run of labels.  Any disagreement is a bug in
 * enum.c, not a modelling slack, so every property requires an exact "t".
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_enum.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "enum"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means an enum operation errored or produced NULL on valid
 * inputs, which is itself a bug -- so either is a failure, reported with the
 * SQL.  Mirrors require_true in prop_numeric.c / prop_record.c.
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
 * Connect to the test cluster and build a FRESH enum type named prop_enum.
 *
 * A label count n is drawn in 2..6 (at least 2 so a strict i<j can be drawn),
 * and the type is declared with labels "l0","l1",...,"l(n-1)" in that order.
 * Because the labels are index-named and declaration order is sort order, a
 * label's numeric suffix is exactly its position in the enum -- which is what
 * makes the ordering/range oracles below exact.
 *
 * Each property opens its own connection (the Tier-B pattern), so the type is
 * created per connection; DROP TYPE IF EXISTS ... CASCADE makes the DDL
 * idempotent against a connection that reused a backend.  The harness sets
 * client_min_messages=warning, so the DROP's "does not exist" notice is quiet.
 * Returns the connection and stores the drawn label count in *n.
 */
static pg_prop_conn *
connect_and_setup(pg_prop_tc *tc, int *n)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData ddl;
	int			nlabels;
	int			i;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	nlabels = (int) pg_prop_draw_int(tc, 2, 6);

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TYPE IF EXISTS prop_enum CASCADE; "
						   "CREATE TYPE prop_enum AS ENUM (");
	for (i = 0; i < nlabels; i++)
	{
		if (i > 0)
			appendStringInfoString(&ddl, ", ");
		appendStringInfo(&ddl, "'l%d'", i);
	}
	appendStringInfoChar(&ddl, ')');

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "setup failed (prop_enum): %s", err ? err : "");

	pfree(ddl.data);

	*n = nlabels;
	return conn;
}

/*
 * Enum comparison follows declaration order: for two distinct indices i<j,
 * 'l<i>'::prop_enum < 'l<j>'::prop_enum.  i<j is enforced in C by swapping, so
 * a strict '<' is always expected to hold.  A sign or ordering bug in the enum
 * comparison function breaks this.
 */
PG_PROP(order_follows_declaration)
{
	pg_prop_conn *conn;
	StringInfoData q;
	int			n;
	int			i,
				j;

	conn = connect_and_setup(tc, &n);

	i = (int) pg_prop_draw_int(tc, 0, n - 1);
	j = (int) pg_prop_draw_int(tc, 0, n - 1);
	if (i == j)
		j = (j + 1) % n;		/* make them distinct */
	if (i > j)
	{
		int			t = i;

		i = j;
		j = t;
	}

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT 'l%d'::prop_enum < 'l%d'::prop_enum", i, j);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Enum comparison is a total order: for any two labels a, b exactly one of
 * <, =, > holds, so the three boolean-to-int results sum to 1.  A missing or
 * overlapping case in the enum comparison breaks this.
 */
PG_PROP(comparison_total_order)
{
	pg_prop_conn *conn;
	StringInfoData q;
	int			n;
	int			a,
				b;

	conn = connect_and_setup(tc, &n);

	a = (int) pg_prop_draw_int(tc, 0, n - 1);
	b = (int) pg_prop_draw_int(tc, 0, n - 1);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (('l%d'::prop_enum < 'l%d'::prop_enum)::int "
					 "+ ('l%d'::prop_enum = 'l%d'::prop_enum)::int "
					 "+ ('l%d'::prop_enum > 'l%d'::prop_enum)::int) = 1",
					 a, b, a, b, a, b);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * enum_first and enum_last return the first and last declared labels: 'l0' and
 * 'l(n-1)'.  NULL::prop_enum selects the type for the type-only functions.
 */
PG_PROP(enum_first_last)
{
	pg_prop_conn *conn;
	StringInfoData q;
	int			n;

	conn = connect_and_setup(tc, &n);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT enum_first(NULL::prop_enum) = 'l0'::prop_enum "
					 "AND enum_last(NULL::prop_enum) = 'l%d'::prop_enum",
					 n - 1);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * enum_range(NULL::prop_enum) returns all n labels in declaration order: the
 * array has cardinality n, its first element is 'l0' and its last is 'l(n-1)'.
 * A missing label or wrong order in the range function breaks this.
 */
PG_PROP(enum_range_cardinality)
{
	pg_prop_conn *conn;
	StringInfoData q;
	int			n;

	conn = connect_and_setup(tc, &n);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT cardinality(enum_range(NULL::prop_enum)) = %d "
					 "AND (enum_range(NULL::prop_enum))[1] = 'l0'::prop_enum "
					 "AND (enum_range(NULL::prop_enum))[%d] = 'l%d'::prop_enum",
					 n, n, n - 1);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * enum_range(a, b) for labels at indices i<=j returns exactly the inclusive
 * run from i to j, which has j-i+1 labels.  i<=j is enforced in C by swapping.
 * An off-by-one or wrong-bound in the two-argument range function breaks this.
 */
PG_PROP(enum_range_bounded)
{
	pg_prop_conn *conn;
	StringInfoData q;
	int			n;
	int			i,
				j;

	conn = connect_and_setup(tc, &n);

	i = (int) pg_prop_draw_int(tc, 0, n - 1);
	j = (int) pg_prop_draw_int(tc, 0, n - 1);
	if (i > j)
	{
		int			t = i;

		i = j;
		j = t;
	}

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT cardinality(enum_range('l%d'::prop_enum, "
					 "'l%d'::prop_enum)) = (%d - %d + 1)",
					 i, j, j, i);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * An enum label cast to text and back to the enum equals the original label:
 * ('l<i>'::prop_enum)::text::prop_enum = 'l<i>'::prop_enum.  A bug in enum
 * output (enum_out) or input (enum_in) breaks this round-trip.
 */
PG_PROP(text_roundtrip)
{
	pg_prop_conn *conn;
	StringInfoData q;
	int			n;
	int			i;

	conn = connect_and_setup(tc, &n);

	i = (int) pg_prop_draw_int(tc, 0, n - 1);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ('l%d'::prop_enum)::text::prop_enum "
					 "= 'l%d'::prop_enum",
					 i, i);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
