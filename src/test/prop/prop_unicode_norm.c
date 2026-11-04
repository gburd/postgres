/*-------------------------------------------------------------------------
 *
 * prop_unicode_norm.c
 *	  Property-based tests for src/common/unicode_norm.c normalization.
 *
 * unicode_normalize() implements the UAX #15 normalization forms (NFC, NFD,
 * NFKC, NFKD) over NUL-terminated char32_t code point arrays.  It is pure,
 * shared by frontend and backend, and has a history of subtle composition
 * bugs: f2ff15e4c37 fixed an off-by-one in NFC recomposition for Hangul, and
 * 066b7b144f3 fixed a buffer overrun -- both in exactly the recompose path
 * these properties drive.
 *
 * This binary is a frontend program, so unicode_normalize() allocates its
 * results with malloc (freed here with pg_free) and may return NULL on OOM.
 * We deliberately do NOT call unicode_is_normalized_quickcheck(): it is
 * compiled only in the backend (#ifndef FRONTEND).
 *
 * Properties:
 *	- NFC is idempotent: NFC(NFC(x)) == NFC(x).
 *	- NFD is idempotent: NFD(NFD(x)) == NFD(x).
 *	- Canonical equivalence: NFC(x) == NFC(NFD(x)) for all x (the strong
 *	  property that catches the Hangul recomposition bugs above).
 *	- NFKD is idempotent: NFKD(NFKD(x)) == NFKD(x) (exercises the compat path).
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_unicode_norm.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "unicode_norm"
#include "pg_prop.h"
#include "common/unicode_norm.h"
#include "lib/stringinfo.h"

/*
 * Draw a NUL-terminated char32_t code point array.  Returns a pg_malloc'd
 * array with cps[n] == 0; *out_n receives the code point count (which may be
 * 0 -- the empty string is a valid and interesting input).
 */
static char32_t *
draw_codepoints(pg_prop_tc *tc, int *out_n)
{
	int			n = (int) pg_prop_draw_int(tc, 0, 24);	/* small strings */
	char32_t   *cps = pg_malloc(sizeof(char32_t) * (n + 1));
	int			i;

	for (i = 0; i < n; i++)
	{
		/*
		 * Draw an assigned, non-surrogate scalar value.  Bias toward the
		 * ranges where normalization actually does something: Latin +
		 * combining marks (U+0041..U+024F, U+0300..U+036F), Hangul
		 * (U+AC00..U+D7A3 precomposed and U+1100..U+11FF jamo), and a few
		 * compatibility chars.  Draw a bucket, then a codepoint in it.
		 */
		int			bucket = (int) pg_prop_draw_int(tc, 0, 4);
		char32_t	c;

		switch (bucket)
		{
			case 0:
				c = (char32_t) pg_prop_draw_int(tc, 0x0041, 0x024F);	/* Latin + ext */
				break;
			case 1:
				c = (char32_t) pg_prop_draw_int(tc, 0x0300, 0x036F);	/* combining marks */
				break;
			case 2:
				c = (char32_t) pg_prop_draw_int(tc, 0xAC00, 0xD7A3);	/* Hangul syllables */
				break;
			case 3:
				c = (char32_t) pg_prop_draw_int(tc, 0x1100, 0x11FF);	/* Hangul jamo */
				break;
			default:
				c = (char32_t) pg_prop_draw_int(tc, 0x00A0, 0x2FFF);	/* mixed BMP */
				break;
		}
		cps[i] = c;
	}
	cps[n] = 0;
	*out_n = n;
	return cps;
}

/* Equality of two NUL-terminated char32_t arrays (same length, same values). */
static bool
cp_equal(const char32_t *a, const char32_t *b)
{
	int			i;

	for (i = 0; a[i] != 0 || b[i] != 0; i++)
	{
		if (a[i] != b[i])
			return false;
	}
	return true;
}

/* Format a code point array as "U+0041 U+0301 ..." into a palloc'd buffer. */
static char *
cp_show(const char32_t *s)
{
	StringInfoData buf;
	int			i;

	initStringInfo(&buf);
	if (s[0] == 0)
		appendStringInfoString(&buf, "(empty)");
	for (i = 0; s[i] != 0; i++)
		appendStringInfo(&buf, "%sU+%04X", i == 0 ? "" : " ", (unsigned) s[i]);
	return buf.data;
}

PG_PROP(nfc_idempotent)
{
	int			n;
	char32_t   *cps = draw_codepoints(tc, &n);
	char32_t   *a = unicode_normalize(UNICODE_NFC, cps);
	char32_t   *b = unicode_normalize(UNICODE_NFC, a);

	if (!cp_equal(a, b))
	{
		char	   *in = cp_show(cps);

		pg_prop_fail(tc, "NFC not idempotent for input [%s]: NFC(x)=[%s] NFC(NFC(x))=[%s]",
					 in, cp_show(a), cp_show(b));
	}
	pg_free(a);
	pg_free(b);
	pg_free(cps);
}

PG_PROP(nfd_idempotent)
{
	int			n;
	char32_t   *cps = draw_codepoints(tc, &n);
	char32_t   *a = unicode_normalize(UNICODE_NFD, cps);
	char32_t   *b = unicode_normalize(UNICODE_NFD, a);

	if (!cp_equal(a, b))
	{
		char	   *in = cp_show(cps);

		pg_prop_fail(tc, "NFD not idempotent for input [%s]: NFD(x)=[%s] NFD(NFD(x))=[%s]",
					 in, cp_show(a), cp_show(b));
	}
	pg_free(a);
	pg_free(b);
	pg_free(cps);
}

PG_PROP(nfc_of_nfd_equals_nfc)
{
	int			n;
	char32_t   *cps = draw_codepoints(tc, &n);
	char32_t   *direct = unicode_normalize(UNICODE_NFC, cps);
	char32_t   *nfd = unicode_normalize(UNICODE_NFD, cps);
	char32_t   *viaD = unicode_normalize(UNICODE_NFC, nfd);

	/*
	 * Canonical equivalence: decomposing fully and recomposing must land on
	 * the same NFC form as composing directly.  This is where the Hangul
	 * recomposition off-by-one (f2ff15e4c37) and buffer overrun (066b7b144f3)
	 * showed up.
	 */
	if (!cp_equal(direct, viaD))
	{
		char	   *in = cp_show(cps);

		pg_prop_fail(tc, "NFC(x) != NFC(NFD(x)) for input [%s]: NFC(x)=[%s] NFC(NFD(x))=[%s] (NFD(x)=[%s])",
					 in, cp_show(direct), cp_show(viaD), cp_show(nfd));
	}
	pg_free(direct);
	pg_free(nfd);
	pg_free(viaD);
	pg_free(cps);
}

PG_PROP(nfkd_idempotent)
{
	int			n;
	char32_t   *cps = draw_codepoints(tc, &n);
	char32_t   *a = unicode_normalize(UNICODE_NFKD, cps);
	char32_t   *b = unicode_normalize(UNICODE_NFKD, a);

	if (!cp_equal(a, b))
	{
		char	   *in = cp_show(cps);

		pg_prop_fail(tc, "NFKD not idempotent for input [%s]: NFKD(x)=[%s] NFKD(NFKD(x))=[%s]",
					 in, cp_show(a), cp_show(b));
	}
	pg_free(a);
	pg_free(b);
	pg_free(cps);
}
