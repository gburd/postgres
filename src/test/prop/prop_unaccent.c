/*-------------------------------------------------------------------------
 *
 * prop_unaccent.c
 *	  Property-based tests for the unaccent contrib extension.
 *
 * unaccent() strips diacritics from text using the default unaccent.rules;
 * these check idempotence, the ASCII no-op property, and length relationships
 * that hold because stripping a simple Latin diacritic is a 1:1 map to a
 * single-byte ASCII letter.
 *
 * The oracle is unaccent's own stability plus independent builtins
 * (length, octet_length, char_length, starts_with, left, ||): a pure-ASCII
 * input is left untouched, the Latin accented pool folds to pure ASCII (so
 * char length equals octet length and, for 1:1 diacritics, equals the input
 * char length), and unaccent distributes over concatenation because it is a
 * per-character map.
 *
 * NOTE: this source file is strictly ASCII.  The accented inputs are built in
 * SQL using PostgreSQL's U&'...' escape-string syntax with \XXXX code-point
 * escapes (e.g. U&'caf\00E9' is cafe-with-acute-e), so no non-ASCII byte ever
 * appears in the C source.  Each U&'...' token is already a quoted SQL string
 * literal and is substituted directly into the generated SQL.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_unaccent.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "unaccent"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Curated pool of accented strings whose diacritics are simple 1:1 Latin
 * marks (acute, grave, umlaut, circumflex, tilde, cedilla).  Each accented
 * letter folds to exactly one single-byte ASCII letter, so unaccent preserves
 * character length and yields pure ASCII.  Ligatures (Oe U+0152, ss U+00DF,
 * Ae U+00C6) are deliberately excluded because they may expand to two
 * characters and would break the length-preservation properties.
 *
 * Every entry is written with ASCII-only U&'...' Unicode escapes so the C
 * source contains no byte >= 0x80.  The doubled backslashes in the C literal
 * yield single backslashes in the SQL text, so each entry is a valid U&'...'
 * string constant.  Decoded (for the reader only, not present in the file):
 *   U&'caf\00E9'        -> cafe + acute e        (U+00E9)
 *   U&'na\00EFve'       -> na + diaeresis i + ve (U+00EF)
 *   U&'Z\00FCrich'      -> Z + umlaut u + rich    (U+00FC)
 *   U&'\00E0\00E9\00EE\00F5\00FC' -> a-grave e-acute i-circ o-tilde u-umlaut
 *   U&'\00D1and\00FA'   -> N-tilde + and + u-acute (U+00D1, U+00FA)
 *   U&'r\00E9sum\00E9'  -> r + acute e + sum + acute e
 *   U&'jalape\00F1o'    -> jalape + n-tilde + o   (U+00F1)
 *   U&'M\00FCller'      -> M + umlaut u + ller     (U+00FC)
 */
static const char *const accented_pool[] = {
	"U&'caf\\00E9'",
	"U&'na\\00EFve'",
	"U&'Z\\00FCrich'",
	"U&'\\00E0\\00E9\\00EE\\00F5\\00FC'",
	"U&'\\00D1and\\00FA'",
	"U&'r\\00E9sum\\00E9'",
	"U&'jalape\\00F1o'",
	"U&'M\\00FCller'"
};

#define ACCENTED_POOL_LEN (sizeof(accented_pool) / sizeof(accented_pool[0]))

/*
 * Append a bare string as a single-quoted SQL literal, doubling any embedded
 * single quote.  Our inputs never contain a quote, but double defensively so
 * the generated SQL is always well-formed.  Mirrors prop_like.c.
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
 * Draw 0..12 characters of pure ASCII text from "[a-zA-Z0-9 ]" into buf as a
 * bare string (no quoting, no diacritics).  Used for the no-op properties
 * where unaccent must be the identity.
 */
static void
draw_ascii(pg_prop_tc *tc, StringInfo buf)
{
	static const char alphabet[] =
		"abcdefghijklmnopqrstuvwxyz"
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
		"0123456789 ";
	int			n = (int) pg_prop_draw_int(tc, 0, 12);

	for (int i = 0; i < n; i++)
		appendStringInfoChar(buf,
							 alphabet[pg_prop_draw_int(tc, 0,
													   sizeof(alphabet) - 2)]);
}

/*
 * Return one entry of the curated accented pool.  The engine draws the index,
 * so it explores every pool entry (and favours the boundaries).
 */
static const char *
draw_accented(pg_prop_tc *tc)
{
	int			i = (int) pg_prop_draw_int(tc, 0, (int) ACCENTED_POOL_LEN - 1);

	return accented_pool[i];
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means unaccent errored or produced NULL on valid text, which is
 * itself a bug -- so either is a failure, reported with the SQL.  Mirrors
 * require_true in prop_numeric.c.
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
 * Connect to the test cluster and ensure the unaccent extension exists.  Each
 * property opens its own connection (the Tier-B pattern); CREATE EXTENSION IF
 * NOT EXISTS makes it idempotent against a reused backend.  Mirrors the
 * connect-and-setup helper in prop_record.c.
 */
static pg_prop_conn *
connect_unaccent(pg_prop_tc *tc)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	if (!pg_prop_sql_exec(conn,
						  "CREATE EXTENSION IF NOT EXISTS unaccent",
						  &err))
		pg_prop_fail(tc, "setup failed (CREATE EXTENSION unaccent): %s",
					 err ? err : "");

	return conn;
}

/*
 * unaccent is idempotent: unaccent(unaccent(S)) = unaccent(S).  Once the
 * diacritics are stripped, re-stripping changes nothing.  Uses accented input
 * so the first pass actually removes something.
 */
PG_PROP(idempotent)
{
	pg_prop_conn *conn;
	const char *a;
	StringInfoData q;

	conn = connect_unaccent(tc);
	a = draw_accented(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT unaccent(unaccent(%s)) = unaccent(%s)",
					 a, a);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * For a pure-ASCII string S (no diacritics), unaccent is the identity:
 * unaccent(S) = S.  S is drawn from "[a-zA-Z0-9 ]", which has nothing for
 * unaccent to strip, so the result must be byte-for-byte S.
 */
PG_PROP(ascii_is_noop)
{
	pg_prop_conn *conn;
	StringInfoData s,
				q;

	conn = connect_unaccent(tc);

	initStringInfo(&s);
	draw_ascii(tc, &s);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT unaccent(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") = ");
	append_sql_literal(&q, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * For the Latin accented pool, unaccent produces pure ASCII, so the result's
 * character length equals its octet length (one byte per char):
 *
 *		length(unaccent(A)) = octet_length(unaccent(A))
 *
 * This guards that the accents were actually removed down to single-byte
 * ASCII characters -- if any multibyte char survived, octet_length would
 * exceed length.
 */
PG_PROP(result_is_ascii_for_latin_accents)
{
	pg_prop_conn *conn;
	const char *a;
	StringInfoData q;

	conn = connect_unaccent(tc);
	a = draw_accented(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT length(unaccent(%s)) = octet_length(unaccent(%s))",
					 a, a);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * For the curated 1:1 Latin pool, unaccent maps each accented letter to
 * exactly one ASCII letter, so the character length is unchanged:
 *
 *		char_length(unaccent(A)) = char_length(A)
 *
 * The pool deliberately excludes ligatures (Oe, ss, Ae) that may expand to
 * two characters, which would break this equality.
 */
PG_PROP(length_preserved_for_simple_accents)
{
	pg_prop_conn *conn;
	const char *a;
	StringInfoData q;

	conn = connect_unaccent(tc);
	a = draw_accented(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT char_length(unaccent(%s)) = char_length(%s)",
					 a, a);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * unaccent distributes over concatenation of two accented strings:
 *
 *		unaccent(A || B) = unaccent(A) || unaccent(B)
 *
 * unaccent is a per-character map, so stripping the concatenation equals
 * concatenating the stripped halves.
 */
PG_PROP(concat_distributes)
{
	pg_prop_conn *conn;
	const char *a;
	const char *b;
	StringInfoData q;

	conn = connect_unaccent(tc);
	a = draw_accented(tc);
	b = draw_accented(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT unaccent(%s || %s) = (unaccent(%s) || unaccent(%s))",
					 a, b, a, b);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * For a pure-ASCII S, unaccent is the identity, so a prefix of unaccent(S)
 * still prefixes S:
 *
 *		starts_with(unaccent(S), left(S, least(3, length(S))))
 *
 * This holds trivially because unaccent(S) = S for ASCII input; it is a light
 * cross-check that the no-op path leaves the leading characters in place.
 */
PG_PROP(ascii_prefix_preserved)
{
	pg_prop_conn *conn;
	StringInfoData s,
				q;

	conn = connect_unaccent(tc);

	initStringInfo(&s);
	draw_ascii(tc, &s);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT starts_with(unaccent(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, "), left(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", least(3, length(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, "))))");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
