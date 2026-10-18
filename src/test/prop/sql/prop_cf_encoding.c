/*-------------------------------------------------------------------------
 *
 * prop_cf_encoding.c
 *	  Encoding verifiers and text producers from the open commitfest.
 *
 * The invariant shared by every property here: text the server accepts is
 * valid in its encoding, and a byte string the verifier accepts converts.
 * A verifier that is too lax lets bytes through that fail later ("no
 * equivalent"), one that is too strict (or a mblen that disagrees with it)
 * miscounts characters; a function that builds text from bytes must not
 * cut a multibyte character in half.
 *
 *	 CF 6913  pg_uhc_verifychar accepts lead/trail bytes outside CP949
 *	 CF 6761  EUC_KR mblen treats SS2/SS3 as lead bytes (maxlen 3)
 *	 CF 7417  pgcrypto crypt() copies a salt prefix that splits a character
 *
 * Portions Copyright (c) 2024, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "cf_encoding"
#include "pg_prop.h"

#include "lib/stringinfo.h"

static pg_prop_conn *
en_connect(pg_prop_tc *tc)
{
	char	   *err = NULL;
	pg_prop_conn *conn = pg_prop_connect(&err);

	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	return conn;
}

static void
en_hex(StringInfo b, const unsigned char *p, int n)
{
	for (int i = 0; i < n; i++)
		appendStringInfo(b, "%02x", p[i]);
}

/*
 * The SQLSTATE class of an error, or NULL on success.  Uses the async
 * primitives because they return the SQLSTATE.
 */
static bool
en_run(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql, char **st, char **err)
{
	*st = NULL;
	*err = NULL;
	if (!pg_prop_sql_send(conn, sql))
		pg_prop_fail(tc, "could not dispatch");
	return pg_prop_sql_await(conn, st, err);
}

/*
 * CF 6913: pg_uhc_verifychar must enforce CP949's byte ranges -- lead
 * 0x81..0xFE, trail 0x41..0x5A, 0x61..0x7A or 0x81..0xFE.  A pair outside
 * them is an invalid byte sequence (22021) and must be rejected by the
 * verifier, not passed on to fail in the conversion table ("has no
 * equivalent", 22P05).  (A pair inside the ranges may still be an
 * unassigned code point; verifiers are structural, so "no equivalent" is
 * then the right answer.)  Drawn: lead and trail bytes, biased to the
 * range boundaries.
 */
static bool
en_cp949_pair(unsigned char c1, unsigned char c2)
{
	return c1 >= 0x81 && c1 <= 0xfe &&
		((c2 >= 0x41 && c2 <= 0x5a) || (c2 >= 0x61 && c2 <= 0x7a) || (c2 >= 0x81 && c2 <= 0xfe));
}

PG_PROP(uhc_verifier_matches_conversion)
{
	static const int edges[] = {0x40, 0x41, 0x5a, 0x5b, 0x60, 0x61, 0x7a, 0x7b, 0x80, 0x81, 0xa0,
	0xa1, 0xc6, 0xc7, 0xfd, 0xfe, 0xff};
	pg_prop_conn *conn = en_connect(tc);
	unsigned char b[2];
	StringInfoData s;
	char	   *st;
	char	   *err;
	bool		ok;

	/* the lead byte has its high bit set: otherwise the pair is two ASCII characters */
	b[0] = (unsigned char) (pg_prop_draw_bool(tc) ? edges[pg_prop_draw_int(tc, 8, lengthof(edges) - 1)]
							: pg_prop_draw_int(tc, 0x80, 0xff));
	b[1] = (unsigned char) (pg_prop_draw_bool(tc) ? edges[pg_prop_draw_int(tc, 0, lengthof(edges) - 1)]
							: pg_prop_draw_int(tc, 0x30, 0xff));
	initStringInfo(&s);
	appendStringInfoString(&s, "SELECT convert_from('\\x");
	en_hex(&s, b, 2);
	appendStringInfoString(&s, "'::bytea, 'UHC')");
	ok = en_run(tc, conn, s.data, &st, &err);
	if (!en_cp949_pair(b[0], b[1]) && (ok || st == NULL || strcmp(st, "22021") != 0))
		pg_prop_fail(tc, "UHC byte pair %02x %02x is outside CP949's lead/trail ranges but the "
					 "verifier accepted it: %s", b[0], b[1], ok ? "converted" : (err ? err : ""));
	pfree(s.data);
	pg_prop_disconnect(conn);
}

/*
 * CF 6761: every EUC_KR character is one byte (ASCII) or two (KS X 1001);
 * code sets 2 and 3 do not exist, so the encoding's maximum length is 2 and
 * SS2/SS3 (0x8e / 0x8f) are not lead bytes.  Drawn strings of valid EUC_KR
 * characters must have char_length = the number of characters drawn, and
 * pg_encoding_max_length must be 2.  A drawn string with an SS2/SS3 byte
 * must be rejected.
 */
PG_PROP(euc_kr_lengths_match_verifier)
{
	pg_prop_conn *conn = en_connect(tc);
	int			n = (int) pg_prop_draw_int(tc, 1, 12);
	bool		ss = pg_prop_draw_int(tc, 0, 3) == 0;
	StringInfoData s;
	StringInfoData bytes;
	bool		failed = false;
	char	   *v;
	char	   *st;
	char	   *err;

	initStringInfo(&bytes);
	for (int i = 0; i < n; i++)
	{
		unsigned char c[2];

		if (pg_prop_draw_bool(tc))
		{
			c[0] = (unsigned char) pg_prop_draw_int(tc, 0x20, 0x7e);
			en_hex(&bytes, c, 1);
		}
		else
		{
			/* KS X 1001 Hangul block, rows 0xb0..0xc8, cols 0xa1..0xfe */
			c[0] = (unsigned char) pg_prop_draw_int(tc, 0xb0, 0xc8);
			c[1] = (unsigned char) pg_prop_draw_int(tc, 0xa1, 0xfe);
			en_hex(&bytes, c, 2);
		}
	}
	initStringInfo(&s);
	if (ss)
	{
		unsigned char c[3] = {pg_prop_draw_bool(tc) ? 0x8e : 0x8f, 0xa1, 0xa1};

		en_hex(&bytes, c, (int) pg_prop_draw_int(tc, 2, 3));
		appendStringInfo(&s, "SELECT char_length(convert_from('\\x%s'::bytea, 'EUC_KR'))", bytes.data);
		if (en_run(tc, conn, s.data, &st, &err))
			pg_prop_fail(tc, "EUC_KR string with an SS2/SS3 byte was accepted: %s", bytes.data);
	}
	else
	{
		appendStringInfo(&s, "SELECT char_length(convert_from('\\x%s'::bytea, 'EUC_KR'))", bytes.data);
		v = pg_prop_sql_scalar(conn, s.data, &failed, &err);
		if (failed || v == NULL || atoi(v) != n)
			pg_prop_fail(tc, "char_length of %d EUC_KR characters (%s) = %s", n, bytes.data,
						 failed ? (err ? err : "error") : v);
		pg_free(v);
	}
	v = pg_prop_sql_scalar(conn, "SELECT pg_encoding_max_length(pg_char_to_encoding('EUC_KR'))", &failed, NULL);
	if (v && strcmp(v, "2") != 0)
		pg_prop_fail(tc, "pg_encoding_max_length('EUC_KR') = %s, but EUC_KR has no 3-byte characters", v);
	if (v)
		pg_free(v);
	pfree(s.data);
	pfree(bytes.data);
	pg_prop_disconnect(conn);
}

/*
 * CF 7417: crypt(password, salt) returns text; with a salt containing
 * multibyte characters the result must still be valid in the database
 * encoding (the DES and MD5 / Blowfish salt parsers copy a fixed number of
 * bytes).  Invariant: crypt() either raises an error itself or returns a
 * value that later operations (upper()) accept as valid text.
 * Needs a UTF8 database and pgcrypto.
 */
PG_PROP(crypt_result_is_valid_text)
{
	static const char *const mb[] = {"\\u20ac", "\\u00e9", "\\u4e2d", "\\U0001F600", "\\u0416"};
	static const char *const pre[] = {"", "$1$", "$2a$06$", "_J9.."};
	pg_prop_conn *conn = en_connect(tc);
	bool		failed = false;
	char	   *enc = pg_prop_sql_scalar(conn, "SELECT getdatabaseencoding()", &failed, NULL);
	const char *prefix = pre[pg_prop_draw_int(tc, 0, lengthof(pre) - 1)];
	int			len = (int) pg_prop_draw_int(tc, 1, 4);
	StringInfoData salt;
	StringInfoData s;
	char	   *st;
	char	   *err;

	if (enc == NULL || strcmp(enc, "UTF8") != 0)
		pg_prop_skip(tc, "needs a UTF8 database");
	pg_free(enc);
	if (!pg_prop_require_extension(conn, "pgcrypto"))
		pg_prop_skip(tc, "pgcrypto not installed");
	initStringInfo(&salt);
	for (int i = 0; i < len; i++)
	{
		if (pg_prop_draw_bool(tc))
			appendStringInfoString(&salt, mb[pg_prop_draw_int(tc, 0, lengthof(mb) - 1)]);
		else
			appendStringInfoChar(&salt, "./ABCxyz019"[pg_prop_draw_int(tc, 0, 10)]);
	}
	/*
	 * Two steps, so a rejection by crypt() itself (the fix) is told apart
	 * from a value that only fails when used: store the result first (a
	 * bare SELECT of an invalid value already fails in the output function),
	 * then use it.
	 */
	pg_prop_sql_exec(conn, "DROP TABLE IF EXISTS cfe_crypt", NULL);
	initStringInfo(&s);
	appendStringInfo(&s, "CREATE TEMP TABLE cfe_crypt AS SELECT crypt('password', E'%s%s') AS h",
					 prefix, salt.data);
	if (en_run(tc, conn, s.data, &st, &err) &&
		!en_run(tc, conn, "SELECT length(upper(h)) FROM cfe_crypt", &st, &err) &&
		st && strcmp(st, "22021") == 0)
		pg_prop_fail(tc, "crypt() returned text that is not valid UTF8: %s\n  salt: E'%s%s'",
					 err ? err : "", prefix, salt.data);
	pg_prop_sql_exec(conn, "DROP TABLE IF EXISTS cfe_crypt", NULL);
	pfree(s.data);
	pfree(salt.data);
	pg_prop_disconnect(conn);
}
