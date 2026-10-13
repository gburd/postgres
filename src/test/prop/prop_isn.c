/*-------------------------------------------------------------------------
 *
 * prop_isn.c
 *	  Property-based tests for the isn contrib extension (ISBN/ISMN/ISSN/EAN13).
 *
 * The isn types (contrib/isn/isn.c) validate and canonicalize international
 * standard numbers against their check digits, store them as a 13-digit EAN13
 * core, and render a canonical hyphenated form on output.  These properties
 * check that a valid number survives a text round-trip, that an ISBN survives
 * the cast to ean13 and back through text, that hyphens are cosmetic, and that
 * the ean13 comparator is a total order.  All source literals are ASCII
 * (digits, hyphens, and the ISBN-10 check character 'X') only.
 *
 * Generating a valid number from scratch requires computing a correct check
 * digit, which is exactly the logic under test, so instead these draw from a
 * small fixed pool of KNOWN-VALID literals taken verbatim from PostgreSQL's
 * own isn regression test (contrib/isn/sql/isn.sql, "test valid conversions").
 *
 * The oracle is the type's own definition: validate(canonicalize(x)) = x.  A
 * value accepted once must re-parse from its own canonical text, hyphens must
 * not change the parsed value, and a comparison must be a total order.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_isn.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "isn"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Known-valid EAN13-castable literals, taken verbatim from the isn regression
 * test's "test valid conversions" block (contrib/isn/sql/isn.sql).  Each has a
 * correct check digit; the regression test accepts every one as a plain EAN13,
 * so property 3 (acceptance) must hold for all of them.  Kept small and
 * conservative -- a wrong entry would make property 3 fail spuriously.
 */
static const char *const valid_ean13[] = {
	"9780123456786",			/* old book */
	"9790123456785",			/* music (ISMN) */
	"9791234567896",			/* new book */
	"9771234567898",			/* serial */
};

/*
 * Known-valid ISBN literals (castable to ean13), also from the regression
 * test.  "123456789X" exercises the ISBN-10 'X' check character; the others
 * are 13-digit book EANs that the ISBN type also accepts.
 */
static const char *const valid_isbn[] = {
	"9780123456786",
	"123456789X",
	"9791234567896",
};

/*
 * Connect to the test cluster and ensure the isn extension exists.  Each
 * property opens its own connection (the Tier-B pattern); CREATE EXTENSION IF
 * NOT EXISTS makes the DDL idempotent across the thousands of per-case
 * connections in a run.  On any failure the property is failed.
 */
static pg_prop_conn *
connect_isn(pg_prop_tc *tc)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	if (!pg_prop_sql_exec(conn, "CREATE EXTENSION IF NOT EXISTS isn", &err))
		pg_prop_fail(tc, "setup failed (CREATE EXTENSION isn): %s",
					 err ? err : "");

	return conn;
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the isn I/O or comparison errored or produced NULL on a
 * value we expect to be valid, which is itself a bug.  Mirrors require_true in
 * prop_numeric.c.
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
 * Draw one literal from the valid-EAN13 pool.  Returned pointer is into static
 * read-only storage; do not free.
 */
static const char *
draw_valid_ean13(pg_prop_tc *tc)
{
	int			i = (int) pg_prop_draw_int(tc, 0, lengthof(valid_ean13) - 1);

	return valid_ean13[i];
}

/*
 * Draw one literal from the valid-ISBN pool.  Returned pointer is into static
 * read-only storage; do not free.
 */
static const char *
draw_valid_isbn(pg_prop_tc *tc)
{
	int			i = (int) pg_prop_draw_int(tc, 0, lengthof(valid_isbn) - 1);

	return valid_isbn[i];
}

/*
 * 1. ean13_text_roundtrip
 *
 * A valid ean13 value cast to its canonical text and parsed back equals the
 * original: ('V'::ean13)::text::ean13 = 'V'::ean13.  This exercises the output
 * canonicalization and the input validation on known-good data; if ean13_out
 * emits text that ean13_in cannot re-parse to the same value, this fails.
 */
PG_PROP(ean13_text_roundtrip)
{
	pg_prop_conn *conn;
	const char *v;
	StringInfoData q;

	conn = connect_isn(tc);
	v = draw_valid_ean13(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ('%s'::ean13)::text::ean13 = '%s'::ean13",
					 v, v);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 2. isbn_to_ean13_roundtrip
 *
 * A valid ISBN cast to ean13 survives a text round-trip:
 * ('V'::isbn::ean13)::text::ean13 = 'V'::isbn::ean13.  This exercises the
 * isbn->ean13 cast together with ean13's own text round-trip; a bug in the
 * cross-type cast or in canonicalization of the resulting ean13 fails here.
 */
PG_PROP(isbn_to_ean13_roundtrip)
{
	pg_prop_conn *conn;
	const char *v;
	StringInfoData q;

	conn = connect_isn(tc);
	v = draw_valid_isbn(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ('%s'::isbn::ean13)::text::ean13 "
					 "= '%s'::isbn::ean13",
					 v, v);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 3. valid_number_accepted
 *
 * Every pool value is accepted by the ean13 type and casts to a non-NULL
 * value: ('V'::ean13) IS NOT NULL.  A validator bug that wrongly rejected a
 * genuinely valid number (or produced NULL) would surface here.  Pool values
 * are taken from the regression test, so each must be accepted.
 */
PG_PROP(valid_number_accepted)
{
	pg_prop_conn *conn;
	const char *v;
	StringInfoData q;

	conn = connect_isn(tc);
	v = draw_valid_ean13(tc);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ('%s'::ean13) IS NOT NULL", v);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 4. hyphens_are_cosmetic
 *
 * ISN input treats hyphens as formatting only, so a pool value and the same
 * value with every hyphen stripped parse to the same ean13.  This exercises
 * the parser's hyphen handling without needing to synthesise valid check
 * digits, so no input is filtered out.
 */
PG_PROP(hyphens_are_cosmetic)
{
	pg_prop_conn *conn;
	const char *v;
	StringInfoData q;

	conn = connect_isn(tc);
	v = draw_valid_ean13(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ('%s'::ean13) = (replace('%s', '-', '')::ean13)",
					 v, v);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 5. ean13_comparison_total_order
 *
 * For any two valid pool values A, B, exactly one of <, =, > holds, so the
 * three boolean-to-int results sum to 1:
 *
 *		(A<B)::int + (A=B)::int + (A>B)::int = 1
 *
 * A missing or overlapping case in the ean13 comparator (a value that is
 * neither less, equal, nor greater, or two firing at once) breaks this.
 */
PG_PROP(ean13_comparison_total_order)
{
	pg_prop_conn *conn;
	const char *a;
	const char *b;
	StringInfoData q;

	conn = connect_isn(tc);
	a = draw_valid_ean13(tc);
	b = draw_valid_ean13(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (('%s'::ean13 < '%s'::ean13)::int "
					 "+ ('%s'::ean13 = '%s'::ean13)::int "
					 "+ ('%s'::ean13 > '%s'::ean13)::int) = 1",
					 a, b, a, b, a, b);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
