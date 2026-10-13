/*-------------------------------------------------------------------------
 *
 * prop_pglz.c
 *	  Property-based tests for src/common/pg_lzcompress.c, the builtin pglz
 *	  compressor used by TOAST.
 *
 * pglz_compress() returns the compressed length, or -1 when the input is
 * refused (too small for the strategy, or the result would not be smaller
 * than the input).  A -1 is an ordinary outcome, not a bug, so the roundtrip
 * properties treat it as "nothing to check" rather than a failure, and lean
 * on repetitive inputs drawn from a tiny alphabet to keep compression (and
 * thus the interesting path) exercised most of the time.
 *
 * Properties:
 *	- roundtrip: decompress(compress(x)) == x for every x that compresses.
 *	- prefix_decompress_matches: decompressing only the first
 *	  pglz_maximum_compressed_size(prefix_len, clen) compressed bytes with
 *	  check_complete=false yields exactly the first prefix_len raw bytes.
 *	  This is the TOAST slice-decompress path (detoast.c); see the heap-
 *	  overflow fix 2b5ba2a0a14 and the "+2" match-tag corner case.
 *	- decompress_corrupt_no_crash: pglz_decompress() on arbitrary bytes never
 *	  returns more than rawsize (which would mean it wrote past the output
 *	  buffer) and never reads/writes out of bounds.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_pglz.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "pglz"
#include "pg_prop.h"
#include "common/pg_lzcompress.h"

/*
 * Keep every length well under 1<<16 so all casts into the int32-typed pglz
 * API are safe and PGLZ_MAX_OUTPUT (+4) cannot overflow.
 */
#define PGLZ_PROP_MAX_LEN	65536

/*
 * Format up to the first "n" bytes of "buf" as hex into "out", for failure
 * messages.  "out" must hold at least prefix*2 + 1 bytes.
 */
static void
hex_prefix(const unsigned char *buf, size_t len, size_t prefix, char *out)
{
	static const char hexdig[] = "0123456789abcdef";
	size_t		i;
	size_t		n = Min(len, prefix);

	for (i = 0; i < n; i++)
	{
		out[i * 2] = hexdig[(buf[i] >> 4) & 0x0f];
		out[i * 2 + 1] = hexdig[buf[i] & 0x0f];
	}
	out[n * 2] = '\0';
}

/*
 * Draw a compressible source: a repetitive string over a tiny alphabet, long
 * enough that pglz_compress almost always finds a match and succeeds.
 */
static unsigned char *
draw_compressible(pg_prop_tc *tc, size_t min_len, size_t *len)
{
	return (unsigned char *) pg_prop_draw_from(tc, "ab", min_len,
											   PGLZ_PROP_MAX_LEN, len);
}

PG_PROP(roundtrip)
{
	size_t		slen;
	unsigned char *src = draw_compressible(tc, 16, &slen);
	char	   *cbuf = pg_malloc(PGLZ_MAX_OUTPUT(slen));
	char	   *dbuf;
	int32		clen;
	int32		dlen;
	char		hex[65];

	/*
	 * Use PGLZ_strategy_always so compression is attempted on inputs the
	 * default strategy's min-length gate would reject outright.
	 */
	clen = pglz_compress((const char *) src, (int32) slen, cbuf,
						 PGLZ_strategy_always);

	/* -1 means the input was deemed incompressible: nothing to roundtrip. */
	if (clen < 0)
	{
		pg_free(src);
		pg_free(cbuf);
		return;
	}

	dbuf = pg_malloc(slen);
	dlen = pglz_decompress(cbuf, clen, dbuf, (int32) slen, true);

	if (dlen != (int32) slen || memcmp(dbuf, src, slen) != 0)
	{
		hex_prefix(src, slen, 32, hex);
		pg_prop_fail(tc,
					 "roundtrip failed: slen=%zu clen=%d dlen=%d src[0..]=%s",
					 slen, clen, dlen, hex);
	}

	pg_free(src);
	pg_free(cbuf);
	pg_free(dbuf);
}

PG_PROP(prefix_decompress_matches)
{
	size_t		slen;
	unsigned char *src = draw_compressible(tc, 32, &slen);
	char	   *cbuf = pg_malloc(PGLZ_MAX_OUTPUT(slen));
	char	   *dbuf;
	int32		clen;
	int32		plen;
	int32		need;
	int32		got;
	char		hex[65];

	clen = pglz_compress((const char *) src, (int32) slen, cbuf,
						 PGLZ_strategy_always);

	if (clen < 0)
	{
		pg_free(src);
		pg_free(cbuf);
		return;
	}

	/* Choose a raw prefix length to reproduce from the compressed data. */
	plen = (int32) pg_prop_draw_int(tc, 1, (int64) slen);

	/*
	 * Maximum compressed bytes needed to produce "plen" raw bytes.  The
	 * function is documented to cap this at total_compressed_size (clen); the
	 * clamp below enforces that defensively in case that contract changes.
	 */
	need = pglz_maximum_compressed_size(plen, clen);
	if (need > clen)
		need = clen;

	dbuf = pg_malloc(plen);
	got = pglz_decompress(cbuf, need, dbuf, plen, false);

	if (got != plen || memcmp(dbuf, src, plen) != 0)
	{
		hex_prefix(src, slen, 32, hex);
		pg_prop_fail(tc,
					 "prefix mismatch: slen=%zu clen=%d plen=%d need=%d got=%d src[0..]=%s",
					 slen, clen, plen, need, got, hex);
	}

	pg_free(src);
	pg_free(cbuf);
	pg_free(dbuf);
}

PG_PROP(decompress_corrupt_no_crash)
{
	size_t		slen;
	unsigned char *src = pg_prop_draw_bytes(tc, 0, 256, &slen);
	int32		rawsize = (int32) pg_prop_draw_int(tc, 0, 4096);
	char	   *dbuf = pg_malloc(rawsize > 0 ? (size_t) rawsize : 1);
	int32		got;
	char		hex[65];

	/*
	 * Treat arbitrary bytes as compressed input.  pglz_decompress must return
	 * at most rawsize (a larger return would mean it wrote past dbuf) or -1,
	 * and must never read or write out of bounds (under ASan the latter would
	 * fault; here we assert the return bound directly).
	 */
	got = pglz_decompress((const char *) src, (int32) slen, dbuf, rawsize,
						 false);

	if (got > rawsize)
	{
		hex_prefix(src, slen, 32, hex);
		pg_prop_fail(tc,
					 "decompress overran output: slen=%zu rawsize=%d got=%d src[0..]=%s",
					 slen, rawsize, got, hex);
	}

	pg_free(src);
	pg_free(dbuf);
}
