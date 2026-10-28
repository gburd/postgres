/*-------------------------------------------------------------------------
 *
 * prop_collation.c
 *	  Case-insensitive and collation-dependent operators across encodings,
 *	  locales and collations.
 *
 * The rest of the suite runs every query in the default database (UTF-8,
 * whatever locale initdb picked) over ASCII text.  Collation code, though,
 * has one implementation per (encoding, ctype, provider) combination, with
 * hand-written fast paths for some of them -- ILIKE in a C-ctype single-byte
 * database never calls lower() at all, but folds case byte by byte inside
 * like_match.c.  A bug in one path is invisible from every other.
 *
 * So each case here builds its own databases, one per drawn (encoding,
 * locale) pair, and draws text from the bytes that differ between paths:
 * ASCII letters, high-bit bytes (which are letters in LATIN1/WIN1252 and
 * case-pair with each other: 0xC6/0xE6 is AE/ae), and the LIKE metacharacters.
 * Text is built server-side from byte values (chr() in single-byte encodings,
 * so no client encoding conversion can intervene), and every check is a
 * definitional identity that must hold for any encoding and collation:
 *
 *	 s ILIKE p        =  lower(s) LIKE lower(p)        (ILIKE's definition)
 *	 s ~* r           =  lower(s) ~ lower(r)           for a literal r
 *	 s ILIKE p        =  s ILIKE p  with p's literal characters case-swapped
 *	 s LIKE p         is decided by the same rows under COLLATE "C" and the
 *	                     database default when the collation is deterministic
 *	 position(p in s) > 0  =  s LIKE '%' || escaped(p) || '%'
 *	 upper(lower(s))  =  upper(s)  and  lower(upper(s)) = lower(s)
 *	                     (holds for C ctype; checked only there)
 *
 * The first identity is exactly what the documentation says ILIKE is.  It
 * finds the single-byte C-locale ILIKE bug ('xÆy' ILIKE '%Æ%' is false in a
 * LATIN1 database with LC_CTYPE=C on a signed-char platform): like_match.c
 * stores the folded first pattern byte in a plain char, so a high-bit byte
 * sign-extends and never equals the unsigned folded text byte.
 *
 * Portions Copyright (c) 2024, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "collation"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Database flavours.  Each is (name suffix, encoding, locale clause).  A
 * flavour whose locale is not installed on the host is skipped at run time.
 */
typedef struct CollDb
{
	const char *suffix;
	const char *encoding;
	const char *locale_sql;		/* appended to CREATE DATABASE */
	bool		single_byte;
	bool		c_ctype;
} CollDb;

static const CollDb coll_dbs[] = {
	{"latin1_c", "LATIN1", "LOCALE 'C'", true, true},
	{"latin9_c", "LATIN9", "LOCALE 'C'", true, true},
	{"win1252_c", "WIN1252", "LOCALE 'C'", true, true},
	{"sqlascii_c", "SQL_ASCII", "LOCALE 'C'", true, true},
	{"utf8_c", "UTF8", "LOCALE 'C'", false, true},
	{"utf8_builtin", "UTF8", "LOCALE_PROVIDER builtin BUILTIN_LOCALE 'C.UTF-8'", false, false},
	{"utf8_icu", "UTF8", "LOCALE_PROVIDER icu ICU_LOCALE 'en' LOCALE 'C'", false, false},
	{"latin1_libc", "LATIN1", "LOCALE 'en_US.ISO8859-1'", true, false},
};
#define N_COLL_DBS ((int) lengthof(coll_dbs))

/*
 * Bytes text is drawn from.  For single-byte databases the high-bit bytes are
 * real characters; for UTF-8 databases each entry is rendered as the code
 * point of the same value (chr(198) is U+00C6 there too), so the same drawn
 * string means the same characters in every flavour.
 */
static const unsigned char coll_alphabet[] = {
	'a', 'b', 'x', 'A', 'B', 'X', 'z', 'Z', '0', ' ',
	0xC6, 0xE6,					/* AE ae */
	0xC9, 0xE9,					/* E-acute e-acute */
	0xD0, 0xF0,					/* ETH eth */
	0xDF,						/* sharp s (no single-char upper) */
	0xFF,						/* y-diaeresis (upper is outside LATIN1) */
	0xB5,						/* micro sign */
	0xAA, 0xBA,					/* ordinal indicators (letters, no case) */
	0xC0, 0xE0, 0xD8, 0xF8, 0xDE, 0xFE,
};
#define N_ALPHA ((int) lengthof(coll_alphabet))

/* Open (creating if needed) the database for a flavour; NULL if unavailable. */
static pg_prop_conn *
coll_connect(pg_prop_tc *tc, const CollDb *d)
{
	char	   *err = NULL;
	pg_prop_conn *admin;
	pg_prop_conn *conn;
	char		dbname[64];
	const char *saved;
	StringInfoData q;
	bool		failed = false;
	char	   *exists;

	snprintf(dbname, sizeof(dbname), "pgprop_coll_%s", d->suffix);

	admin = pg_prop_connect(&err);
	if (admin == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	initStringInfo(&q);
	appendStringInfo(&q, "SELECT count(*) FROM pg_database WHERE datname = '%s'", dbname);
	exists = pg_prop_sql_scalar(admin, q.data, &failed, NULL);
	if (!failed && exists && strcmp(exists, "0") == 0)
	{
		resetStringInfo(&q);
		appendStringInfo(&q, "CREATE DATABASE %s ENCODING '%s' %s TEMPLATE template0",
						 dbname, d->encoding, d->locale_sql);
		if (!pg_prop_sql_exec(admin, q.data, &err))
		{
			/* locale not installed on this host, or provider unavailable */
			pg_free(err);
			pg_free(exists);
			pfree(q.data);
			pg_prop_disconnect(admin);
			return NULL;
		}
	}
	if (exists)
		pg_free(exists);
	pfree(q.data);
	pg_prop_disconnect(admin);

	saved = getenv("PGDATABASE");
	setenv("PGDATABASE", dbname, 1);
	conn = pg_prop_connect(&err);
	if (saved)
		setenv("PGDATABASE", saved, 1);
	else
		unsetenv("PGDATABASE");
	if (conn == NULL)
		pg_prop_fail(tc, "connect to %s failed: %s", dbname, err ? err : "");
	return conn;
}

/* append chr(b) || ... for a drawn byte string; '' when empty */
static void
coll_text_sql(StringInfo b, const unsigned char *s, int n)
{
	if (n == 0)
	{
		appendStringInfoString(b, "''");
		return;
	}
	appendStringInfoChar(b, '(');
	for (int i = 0; i < n; i++)
		appendStringInfo(b, "%schr(%d)", i ? "||" : "", s[i]);
	appendStringInfoChar(b, ')');
}

/*
 * Draw n bytes from the alphabet; with wildcards, also '%' and '_'.  About
 * half the bytes are high-bit (the half of the alphabet where the encodings
 * and ctypes disagree), and a pattern starts with '%' a third of the time,
 * because a leading '%' is what sends like_match.c to its "scan for the first
 * literal" loop -- the code that differs most between the matcher variants.
 */
static int
coll_draw(pg_prop_tc *tc, unsigned char *out, int maxn, bool wildcards)
{
	int			n = (int) pg_prop_draw_int(tc, 0, maxn);
	int			i = 0;

	if (wildcards && n > 0 && pg_prop_draw_int(tc, 0, 2) == 0)
		out[i++] = '%';
	for (; i < n; i++)
	{
		if (wildcards && pg_prop_draw_int(tc, 0, 4) == 0)
			out[i] = pg_prop_draw_bool(tc) ? '%' : '_';
		else if (pg_prop_draw_bool(tc))
			out[i] = coll_alphabet[pg_prop_draw_int(tc, 10, N_ALPHA - 1)];	/* high-bit */
		else
			out[i] = coll_alphabet[pg_prop_draw_int(tc, 0, 9)];	/* ASCII */
	}
	return n;
}

/*
 * Draw a LIKE pattern by shape rather than byte by byte.  An unstructured
 * draw almost never produces the combinations the matchers special-case, so
 * the shape is chosen first:
 *
 *	 0  '%' || lit || '%'        lit is a substring of s, maybe case-swapped
 *	 1  '%' || lit               the "scan for the first literal" loop
 *	 2  lit || '%'
 *	 3  s with one char replaced by '_' and its case swapped
 *	 4  s case-swapped (whole-string match)
 *	 5  free-form (the old byte-by-byte draw)
 *
 * "Case-swapped" maps each byte to its LATIN1 case partner (A<->a, and
 * 0xC0-0xDE <-> 0xE0-0xFE except the multiplication/division signs), which
 * is the pair ILIKE must treat as equal in a single-byte database.
 */
static unsigned char
coll_swapcase(unsigned char c)
{
	if (c >= 'A' && c <= 'Z')
		return c + 32;
	if (c >= 'a' && c <= 'z')
		return c - 32;
	if (c >= 0xC0 && c <= 0xDE && c != 0xD7)
		return c + 32;
	if (c >= 0xE0 && c <= 0xFE && c != 0xF7)
		return c - 32;
	return c;
}

static int
coll_draw_pattern(pg_prop_tc *tc, unsigned char *p, int maxn,
				  const unsigned char *s, int sn)
{
	int			shape = (int) pg_prop_draw_int(tc, 0, 5);
	int			n = 0;
	int			from;
	int			len;
	bool		swap = pg_prop_draw_bool(tc);

	if (sn == 0 && shape != 5)
		shape = 5;
	switch (shape)
	{
		case 0:
		case 1:
		case 2:
			from = (int) pg_prop_draw_int(tc, 0, sn - 1);
			len = (int) pg_prop_draw_int(tc, 1, Min(sn - from, maxn - 2));
			if (shape != 2)
				p[n++] = '%';
			for (int i = 0; i < len; i++)
				p[n++] = swap ? coll_swapcase(s[from + i]) : s[from + i];
			if (shape != 1)
				p[n++] = '%';
			return n;
		case 3:
		case 4:
			len = Min(sn, maxn);
			for (int i = 0; i < len; i++)
				p[n++] = coll_swapcase(s[i]);
			if (shape == 3 && len > 0)
				p[pg_prop_draw_int(tc, 0, len - 1)] = '_';
			return n;
		default:
			return coll_draw(tc, p, maxn, true);
	}
}

/*
 * Single-byte C-ctype databases get half the draws: they are the only ones
 * with a hand-written case-folding matcher (C_IMatchText), and four of the
 * eight flavours.
 */
static const CollDb *
coll_pick(pg_prop_tc *tc)
{
	if (pg_prop_draw_bool(tc))
		return &coll_dbs[pg_prop_draw_int(tc, 0, 3)];
	return &coll_dbs[pg_prop_draw_int(tc, 0, N_COLL_DBS - 1)];
}

static bool
coll_bool(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql, const char *ctx)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *v = pg_prop_sql_scalar(conn, sql, &failed, &err);
	bool		r;

	if (failed || v == NULL)
		pg_prop_fail(tc, "%s: query failed: %s\n  SQL: %s", ctx, err ? err : "", sql);
	r = (strcmp(v, "t") == 0);
	pg_free(v);
	return r;
}

/* describe the drawn bytes for a failure message */
static char *
coll_hex(const unsigned char *s, int n)
{
	StringInfoData b;

	initStringInfo(&b);
	for (int i = 0; i < n; i++)
		appendStringInfo(&b, "%s%02X", i ? " " : "", s[i]);
	return b.data;
}

/*
 * s ILIKE p  ==  lower(s) LIKE lower(p)
 *
 * This is ILIKE's documented definition.  Both sides are computed by the
 * server under the same database collation; any disagreement means the ILIKE
 * implementation for this (encoding, ctype) path is wrong.
 */
PG_PROP(ilike_equals_lower_like)
{
	const CollDb *d = coll_pick(tc);
	pg_prop_conn *conn = coll_connect(tc, d);
	unsigned char s[12];
	unsigned char p[8];
	int			sn;
	int			pn;
	StringInfoData q;
	bool		a;
	bool		b;

	if (conn == NULL)
		pg_prop_skip(tc, "database flavour %s not available here", d->suffix);

	sn = coll_draw(tc, s, sizeof(s), false);
	pn = coll_draw_pattern(tc, p, sizeof(p), s, sn);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT ");
	coll_text_sql(&q, s, sn);
	appendStringInfoString(&q, " ILIKE ");
	coll_text_sql(&q, p, pn);
	a = coll_bool(tc, conn, q.data, "ILIKE");

	resetStringInfo(&q);
	appendStringInfoString(&q, "SELECT lower(");
	coll_text_sql(&q, s, sn);
	appendStringInfoString(&q, ") LIKE lower(");
	coll_text_sql(&q, p, pn);
	appendStringInfoChar(&q, ')');
	b = coll_bool(tc, conn, q.data, "lower LIKE lower");

	if (a != b)
		pg_prop_fail(tc,
					 "s ILIKE p = %s but lower(s) LIKE lower(p) = %s\n"
					 "  database: %s (encoding %s, %s)\n"
					 "  s bytes: [%s]\n  p bytes: [%s]",
					 a ? "true" : "false", b ? "true" : "false",
					 d->suffix, d->encoding, d->locale_sql,
					 coll_hex(s, sn), coll_hex(p, pn));

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * s ~* r  ==  lower(s) ~ lower(r), for r a literal (no regex metacharacters:
 * the alphabet has none).  Same definition, the regex engine's own
 * case-insensitive path.
 */
PG_PROP(regex_icase_equals_lower_regex)
{
	const CollDb *d = coll_pick(tc);
	pg_prop_conn *conn = coll_connect(tc, d);
	unsigned char s[12];
	unsigned char r[6];
	int			sn;
	int			rn;
	StringInfoData q;
	bool		a;
	bool		b;

	if (conn == NULL)
		pg_prop_skip(tc, "database flavour %s not available here", d->suffix);

	sn = coll_draw(tc, s, sizeof(s), false);
	rn = coll_draw(tc, r, sizeof(r), false);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT ");
	coll_text_sql(&q, s, sn);
	appendStringInfoString(&q, " ~* ");
	coll_text_sql(&q, r, rn);
	a = coll_bool(tc, conn, q.data, "~*");

	resetStringInfo(&q);
	appendStringInfoString(&q, "SELECT lower(");
	coll_text_sql(&q, s, sn);
	appendStringInfoString(&q, ") ~ lower(");
	coll_text_sql(&q, r, rn);
	appendStringInfoChar(&q, ')');
	b = coll_bool(tc, conn, q.data, "lower ~ lower");

	if (a != b)
		pg_prop_fail(tc,
					 "s ~* r = %s but lower(s) ~ lower(r) = %s\n"
					 "  database: %s (encoding %s, %s)\n"
					 "  s bytes: [%s]\n  r bytes: [%s]",
					 a ? "true" : "false", b ? "true" : "false",
					 d->suffix, d->encoding, d->locale_sql,
					 coll_hex(s, sn), coll_hex(r, rn));

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * ILIKE is invariant under changing the case of the pattern: s ILIKE p ==
 * s ILIKE upper(p) == s ILIKE lower(p).  (Fold-equivalent patterns must
 * match the same strings.)  Independent of lower()'s own correctness.
 */
PG_PROP(ilike_invariant_under_pattern_case)
{
	const CollDb *d = coll_pick(tc);
	pg_prop_conn *conn = coll_connect(tc, d);
	unsigned char s[12];
	unsigned char p[8];
	int			sn;
	int			pn;
	StringInfoData q;
	bool		a;
	bool		up;
	bool		lo;

	if (conn == NULL)
		pg_prop_skip(tc, "database flavour %s not available here", d->suffix);

	sn = coll_draw(tc, s, sizeof(s), false);
	pn = coll_draw_pattern(tc, p, sizeof(p), s, sn);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT ");
	coll_text_sql(&q, s, sn);
	appendStringInfoString(&q, " ILIKE ");
	coll_text_sql(&q, p, pn);
	a = coll_bool(tc, conn, q.data, "ILIKE p");

	resetStringInfo(&q);
	appendStringInfoString(&q, "SELECT ");
	coll_text_sql(&q, s, sn);
	appendStringInfoString(&q, " ILIKE upper(");
	coll_text_sql(&q, p, pn);
	appendStringInfoChar(&q, ')');
	up = coll_bool(tc, conn, q.data, "ILIKE upper(p)");

	resetStringInfo(&q);
	appendStringInfoString(&q, "SELECT ");
	coll_text_sql(&q, s, sn);
	appendStringInfoString(&q, " ILIKE lower(");
	coll_text_sql(&q, p, pn);
	appendStringInfoChar(&q, ')');
	lo = coll_bool(tc, conn, q.data, "ILIKE lower(p)");

	/*
	 * Only fold-preserving pairs are required to agree: if upper() maps a
	 * character to something lower() does not map back (sharp s, y-diaeresis
	 * in LATIN1), the patterns are not fold-equivalent.  Restrict to the
	 * case where lower(upper(p)) = lower(p).
	 */
	resetStringInfo(&q);
	appendStringInfoString(&q, "SELECT lower(upper(");
	coll_text_sql(&q, p, pn);
	appendStringInfoString(&q, ")) = lower(");
	coll_text_sql(&q, p, pn);
	appendStringInfoChar(&q, ')');
	if (coll_bool(tc, conn, q.data, "fold check") && (a != up || a != lo))
		pg_prop_fail(tc,
					 "ILIKE depends on the pattern's case: p=%s upper(p)=%s lower(p)=%s\n"
					 "  database: %s (encoding %s, %s)\n"
					 "  s bytes: [%s]\n  p bytes: [%s]",
					 a ? "t" : "f", up ? "t" : "f", lo ? "t" : "f",
					 d->suffix, d->encoding, d->locale_sql,
					 coll_hex(s, sn), coll_hex(p, pn));

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Containment, three ways: position(p in s) > 0, strpos(s, p) > 0, and
 * s LIKE '%' || p || '%' (p drawn without wildcards) must agree under a
 * deterministic collation.
 */
PG_PROP(like_contains_equals_position)
{
	const CollDb *d = coll_pick(tc);
	pg_prop_conn *conn = coll_connect(tc, d);
	unsigned char s[12];
	unsigned char p[5];
	int			sn;
	int			pn;
	StringInfoData q;
	bool		pos;
	bool		lk;

	if (conn == NULL)
		pg_prop_skip(tc, "database flavour %s not available here", d->suffix);

	sn = coll_draw(tc, s, sizeof(s), false);
	pn = coll_draw(tc, p, sizeof(p), false);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT position(");
	coll_text_sql(&q, p, pn);
	appendStringInfoString(&q, " in ");
	coll_text_sql(&q, s, sn);
	appendStringInfoString(&q, ") > 0 OR length(");
	coll_text_sql(&q, p, pn);
	appendStringInfoString(&q, ") = 0");
	pos = coll_bool(tc, conn, q.data, "position");

	resetStringInfo(&q);
	appendStringInfoString(&q, "SELECT ");
	coll_text_sql(&q, s, sn);
	appendStringInfoString(&q, " LIKE ('%' || ");
	coll_text_sql(&q, p, pn);
	appendStringInfoString(&q, " || '%')");
	lk = coll_bool(tc, conn, q.data, "LIKE %p%");

	if (pos != lk)
		pg_prop_fail(tc,
					 "position(p in s) > 0 = %s but s LIKE '%%p%%' = %s\n"
					 "  database: %s (encoding %s, %s)\n"
					 "  s bytes: [%s]\n  p bytes: [%s]",
					 pos ? "true" : "false", lk ? "true" : "false",
					 d->suffix, d->encoding, d->locale_sql,
					 coll_hex(s, sn), coll_hex(p, pn));

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The case-insensitive operators must not depend on whether the comparison
 * runs in a C-ctype database or in a non-C database under COLLATE "C": both
 * are ctype C.  (Two different code paths in like.c / regc_pg_locale.c.)
 * Bytes are restricted to what both databases can represent: the
 * single-byte C database and a single-byte libc database of the same
 * encoding.
 */
PG_PROP(collate_c_matches_c_database)
{
	pg_prop_conn *cdb;
	pg_prop_conn *ldb;
	unsigned char s[12];
	unsigned char p[8];
	int			sn;
	int			pn;
	StringInfoData q;
	bool		a;
	bool		b;
	bool		like = pg_prop_draw_bool(tc);

	cdb = coll_connect(tc, &coll_dbs[0]);	/* latin1_c */
	ldb = coll_connect(tc, &coll_dbs[N_COLL_DBS - 1]);	/* latin1_libc */
	if (cdb == NULL || ldb == NULL)
	{
		if (cdb)
			pg_prop_disconnect(cdb);
		if (ldb)
			pg_prop_disconnect(ldb);
		pg_prop_skip(tc, "a LATIN1 libc locale is not available here");
	}

	sn = coll_draw(tc, s, sizeof(s), false);
	pn = coll_draw(tc, p, sizeof(p), like);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT ");
	coll_text_sql(&q, s, sn);
	appendStringInfoString(&q, like ? " ILIKE " : " ~* ");
	coll_text_sql(&q, p, pn);
	a = coll_bool(tc, cdb, q.data, "C database");
	appendStringInfoString(&q, " COLLATE \"C\"");
	b = coll_bool(tc, ldb, q.data, "COLLATE C");

	if (a != b)
		pg_prop_fail(tc,
					 "%s in a C-ctype LATIN1 database = %s, but under COLLATE \"C\" "
					 "in a LATIN1 libc database = %s\n  s bytes: [%s]\n  p bytes: [%s]",
					 like ? "ILIKE" : "~*", a ? "true" : "false", b ? "true" : "false",
					 coll_hex(s, sn), coll_hex(p, pn));

	pfree(q.data);
	pg_prop_disconnect(cdb);
	pg_prop_disconnect(ldb);
}
