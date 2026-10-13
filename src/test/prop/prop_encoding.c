/*-------------------------------------------------------------------------
 *
 * prop_encoding.c
 *	  Property-based tests for src/common/wchar.c multibyte verification.
 *
 * The multibyte verifiers are the trust boundary for every byte that enters
 * the server claiming to be text in some encoding.  pg_encoding_verifymbstr()
 * decides how much of an input buffer is a valid string; pg_encoding_mblen()
 * and its bounded cousin decide how far to advance.  A short input or a
 * malformed lead byte that makes one of these over-read is a classic class of
 * bug (see commit 74ee636cc93, which fixed exactly such an over-read), so we
 * exercise them with arbitrary byte buffers across a spread of encodings with
 * interesting multibyte rules.
 *
 * Properties:
 *	- verifymbstr returns a count in [0, len] and, when short, stops exactly on
 *	  a byte that does not begin a complete valid character.
 *	- verifymbstr agrees with an independent char-by-char re-walk.
 *	- mblen_bounded always returns a length in [1, max_length], even when the
 *	  input is shorter than max_length (the over-read class).
 *	- verifymbstr is idempotent on its own accepted UTF-8 prefix.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_encoding.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "encoding"
#include "pg_prop.h"
#include "mb/pg_wchar.h"

/*
 * Draw one encoding from a small fixed set that is server-safe and has
 * interesting multibyte rules (variable-length lead bytes, 4-byte GB18030,
 * EUC multibyte, and plain single-byte baselines).
 */
static int
draw_encoding(pg_prop_tc *tc)
{
	static const int encs[] = {
		PG_SQL_ASCII, PG_UTF8, PG_LATIN1, PG_EUC_JP,
		PG_EUC_KR, PG_GB18030, PG_BIG5
	};

	return encs[pg_prop_draw_int(tc, 0, (int) (lengthof(encs) - 1))];
}

/*
 * Append the hex of a byte buffer to a note so a shrunk counterexample shows
 * the exact bytes.  The buffer is small (<= 64 bytes) so a stack buffer is
 * plenty.
 */
static void
note_hex(pg_prop_tc *tc, const unsigned char *buf, size_t len)
{
	char		hex[64 * 3 + 1];
	size_t		i;
	size_t		off = 0;

	for (i = 0; i < len && off + 3 < sizeof(hex); i++)
		off += sprintf(hex + off, "%02x ", buf[i]);
	hex[off] = '\0';
	pg_prop_note(tc, "bytes: %s", hex);
}

PG_PROP(verifymbstr_prefix_is_whole)
{
	int			enc = draw_encoding(tc);
	size_t		len;
	unsigned char *buf = pg_prop_draw_bytes(tc, 0, 64, &len);
	int			n;

	n = pg_encoding_verifymbstr(enc, (char *) buf, (int) len);

	/* n is the number of leading bytes that form a valid string */
	if (!(n >= 0 && n <= (int) len))
	{
		note_hex(tc, buf, len);
		pg_prop_fail(tc, "verifymbstr out of range: enc=%d len=%zu n=%d",
					 enc, len, n);
	}

	/*
	 * If the verifier stopped short, the byte at offset n must be a legitimate
	 * stop: either it does not begin a complete valid character (verifymbchar
	 * returns <= 0, i.e. invalid or incomplete), or it is a NUL byte.  The
	 * string verifiers deliberately stop at an embedded NUL even though the
	 * per-character verifier (e.g. SQL_ASCII's, which accepts any byte) treats
	 * NUL as a valid 1-byte character -- a text value must not contain NUL.
	 * Anything else means verifymbstr stopped on a boundary it should have
	 * consumed.
	 */
	if (n < (int) len)
	{
		int			c = pg_encoding_verifymbchar(enc, (char *) buf + n,
												 (int) len - n);

		if (c > 0 && buf[n] != '\0')
		{
			note_hex(tc, buf, len);
			pg_prop_fail(tc, "verifymbstr stopped at a valid char boundary: "
						 "enc=%d len=%zu n=%d verifymbchar=%d",
						 enc, len, n, c);
		}
	}

	pg_free(buf);
}

PG_PROP(verifymbstr_consistent_with_charwalk)
{
	int			enc = draw_encoding(tc);
	size_t		len;
	unsigned char *buf = pg_prop_draw_bytes(tc, 0, 64, &len);
	int			n;
	int			off = 0;

	/*
	 * Independently re-walk the buffer one character at a time; the sum of the
	 * valid char lengths must equal what the whole-string verifier reports.
	 * The string verifier also stops at an embedded NUL (a text value cannot
	 * contain one), which the per-character verifier does not model, so stop
	 * the walk at a NUL as well to compare like with like.
	 */
	for (;;)
	{
		int			c;

		if (off >= (int) len || buf[off] == '\0')
			break;
		c = pg_encoding_verifymbchar(enc, (char *) buf + off,
									 (int) len - off);
		if (c <= 0)
			break;
		off += c;
	}

	n = pg_encoding_verifymbstr(enc, (char *) buf, (int) len);

	if (off != n)
	{
		note_hex(tc, buf, len);
		pg_prop_fail(tc, "verifymbstr disagrees with char-walk: "
					 "enc=%d len=%zu verifymbstr=%d charwalk=%d",
					 enc, len, n, off);
	}

	pg_free(buf);
}

PG_PROP(mblen_bounded_within_limit)
{
	int			enc = draw_encoding(tc);
	size_t		len;
	unsigned char *buf = pg_prop_draw_bytes(tc, 1, 64, &len);
	int			maxlen = pg_encoding_max_length(enc);
	unsigned char *padded;
	int			m;

	/*
	 * pg_encoding_mblen_bounded() takes only (enc, mbstr) and may read up to
	 * pg_encoding_mblen() bytes before its strnlen() stops.  For GB18030 that
	 * is up to two bytes examined to decide the length, so a short drawn
	 * buffer could over-read.  Pad with max_length trailing zero bytes: any
	 * over-read then reads our own zeros, and strnlen() bounds the result at
	 * the first zero -- exactly the "short input over-read" guard this
	 * property exists to defend (commit 74ee636cc93).
	 */
	padded = pg_malloc(len + maxlen + 1);
	memcpy(padded, buf, len);
	memset(padded + len, 0, maxlen + 1);

	m = pg_encoding_mblen_bounded(enc, (char *) padded);

	/*
	 * mblen_bounded is strnlen(mbstr, mblen), so it returns 0 for a buffer
	 * that begins with a NUL (it must never report length past a NUL), and
	 * otherwise a positive length no larger than the encoding's maximum.
	 */
	if (padded[0] == '\0')
	{
		if (m != 0)
		{
			note_hex(tc, buf, len);
			pg_prop_fail(tc, "mblen_bounded of leading NUL not 0: enc=%d m=%d",
						 enc, m);
		}
	}
	else if (!(m >= 1 && m <= maxlen))
	{
		note_hex(tc, buf, len);
		pg_prop_fail(tc, "mblen_bounded out of range: enc=%d m=%d max_length=%d len=%zu",
					 enc, m, maxlen, len);
	}

	pg_free(padded);
	pg_free(buf);
}

PG_PROP(utf8_valid_string_fully_accepted)
{
	int			enc = PG_UTF8;
	size_t		len;
	unsigned char *buf = pg_prop_draw_bytes(tc, 0, 64, &len);
	int			n;
	int			n2;

	/*
	 * Whatever prefix the UTF-8 verifier accepts, re-verifying just that
	 * prefix must accept all of it -- verifymbstr is idempotent on its own
	 * accepted output.
	 */
	n = pg_encoding_verifymbstr(enc, (char *) buf, (int) len);
	n2 = pg_encoding_verifymbstr(enc, (char *) buf, n);

	if (n2 != n)
	{
		note_hex(tc, buf, len);
		pg_prop_fail(tc, "verifymbstr not idempotent on accepted prefix: "
					 "len=%zu n=%d reverify=%d", len, n, n2);
	}

	pg_free(buf);
}
