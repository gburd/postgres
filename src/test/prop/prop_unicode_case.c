/*-------------------------------------------------------------------------
 *
 * prop_unicode_case.c
 *	  Property-based tests for src/common/unicode_case.c case mapping.
 *
 * Unicode case conversion (unicode_strlower/strupper/strfold) is pure,
 * length-based, and a classic bug site: special mappings (ß, final sigma),
 * measure-mode buffer sizing, and truncated-UTF-8 inputs.  Recent fixes
 * defending against truncated UTF-8 (21ffc271d47, 07211f64ace) live here.
 *
 * Properties:
 *	- lowercase is idempotent: lower(lower(x)) == lower(x) (full mappings).
 *	- casefold is idempotent: fold(fold(x)) == fold(x) -- a documented
 *	  contract and the classic fold bug site.
 *	- measure mode (dstsize 0, dst NULL) returns the same length as the real
 *	  conversion, and consumes all of valid src both times.
 *	- truncated UTF-8 input never consumes past the given srclen (no overrun).
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_unicode_case.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "unicode_case"
#include "pg_prop.h"
#include "common/unicode_case.h"
#include "mb/pg_wchar.h"

/* signature shared by unicode_strlower/strupper/strfold */
typedef size_t (*case_fn) (char *dst, size_t dstsize, const char *src,
						   size_t srclen, size_t *pconsumed, bool full);

/*
 * Convert src (length srclen) with the given case function into a freshly
 * malloc'd, NUL-terminated buffer.  Returns the buffer and stores the result
 * byte length in *outlen.  src must be valid UTF-8; if a decoding error is
 * detected (consumed < srclen) the current test case is discarded.
 */
static char *
convert_dup(pg_prop_tc *tc, case_fn fn, const char *src, size_t srclen,
			bool full, size_t *outlen)
{
	size_t		need;
	size_t		consumed;
	size_t		got;
	char	   *buf;

	/* measure mode: dst may be NULL when dstsize is 0 */
	need = fn(NULL, 0, src, srclen, &consumed, full);

	/* harness draws valid UTF-8, so the whole input must be consumed */
	Assert(consumed == srclen);
	pg_prop_assume(tc, consumed == srclen);

	buf = pg_malloc(need + 1);
	got = fn(buf, need + 1, src, srclen, &consumed, full);

	/* dstsize > result length guarantees NUL termination */
	Assert(got == need);
	Assert(consumed == srclen);
	buf[need] = '\0';
	*outlen = got;
	return buf;
}

/* Append src bytes to a note-friendly hex string; caller frees. */
static char *
to_hex(const char *src, size_t len)
{
	char	   *out = pg_malloc(len * 2 + 1);
	size_t		i;

	for (i = 0; i < len; i++)
		snprintf(out + i * 2, 3, "%02x", (unsigned char) src[i]);
	out[len * 2] = '\0';
	return out;
}

PG_PROP(lower_idempotent)
{
	size_t		len;
	char	   *s = pg_prop_draw_utf8(tc, 0, 60, &len);
	size_t		len1,
				len2;
	char	   *once = convert_dup(tc, unicode_strlower, s, len, true, &len1);
	char	   *twice = convert_dup(tc, unicode_strlower, once, len1, true, &len2);

	if (len1 != len2 || memcmp(once, twice, len1) != 0)
	{
		char	   *hex = to_hex(s, len);

		pg_prop_fail(tc, "lower not idempotent for input (hex): %s", hex);
	}

	pg_free(twice);
	pg_free(once);
	pg_free(s);
}

PG_PROP(fold_idempotent)
{
	size_t		len;
	char	   *s = pg_prop_draw_utf8(tc, 0, 60, &len);
	size_t		len1,
				len2;
	char	   *once = convert_dup(tc, unicode_strfold, s, len, true, &len1);
	char	   *twice = convert_dup(tc, unicode_strfold, once, len1, true, &len2);

	if (len1 != len2 || memcmp(once, twice, len1) != 0)
	{
		char	   *hex = to_hex(s, len);

		pg_prop_fail(tc, "fold not idempotent for input (hex): %s", hex);
	}

	pg_free(twice);
	pg_free(once);
	pg_free(s);
}

PG_PROP(dstsize_zero_computes_true_length)
{
	size_t		len;
	char	   *s = pg_prop_draw_utf8(tc, 0, 60, &len);
	bool		full = pg_prop_draw_bool(tc);
	size_t		need;
	size_t		consumed;
	size_t		got;
	size_t		consumed2;
	char	   *buf;

	/* measure mode: dst NULL, dstsize 0 */
	need = unicode_strupper(NULL, 0, s, len, &consumed, full);
	pg_prop_assume(tc, consumed == len);

	buf = pg_malloc(need + 1);
	got = unicode_strupper(buf, need + 1, s, len, &consumed2, full);

	if (got != need)
		pg_prop_fail(tc,
					 "measure length %zu != real length %zu (full=%d)",
					 need, got, (int) full);
	if (consumed != len || consumed2 != len)
		pg_prop_fail(tc,
					 "consumed mismatch: measure=%zu real=%zu srclen=%zu",
					 consumed, consumed2, len);

	pg_free(buf);
	pg_free(s);
}

PG_PROP(truncated_utf8_no_overrun)
{
	size_t		len;
	char	   *s = pg_prop_draw_utf8(tc, 1, 60, &len);
	size_t		tlen;
	size_t		consumed;
	bool		full = pg_prop_draw_bool(tc);

	/*
	 * Truncate to a shorter byte length, possibly cutting a multibyte
	 * character in half.  len >= 1 so [0, len-1] is a valid range.
	 */
	tlen = (size_t) pg_prop_draw_int(tc, 0, (int64) len - 1);

	/*
	 * Measure mode on the truncated input.  The function must return without
	 * reading past src + tlen (verified under ASan) and must never report
	 * consuming more than it was given.  A trailing truncated char should
	 * leave consumed < tlen (decoding error); a clean cut consumes all tlen.
	 */
	(void) unicode_strlower(NULL, 0, s, tlen, &consumed, full);

	if (consumed > tlen)
		pg_prop_fail(tc,
					 "consumed %zu > srclen %zu on truncated input",
					 consumed, tlen);

	pg_free(s);
}
