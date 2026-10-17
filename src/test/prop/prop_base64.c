/*-------------------------------------------------------------------------
 *
 * prop_base64.c
 *	  Property-based tests for src/common/base64.c.
 *
 * base64 encode/decode is a pure codec with a clean round-trip property and
 * explicit length contracts (pg_b64_enc_len / pg_b64_dec_len), used for SCRAM,
 * the COPY and replication protocols, and more.  A codec is the textbook case
 * for property testing: encode-then-decode must reproduce the input for every
 * byte string, and the length helpers must bound the real output.
 *
 * Tier A: in-process, no server needed.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_base64.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "base64"
#include "pg_prop.h"

#include "common/base64.h"

PG_PROP(roundtrip)
{
	size_t		len;
	unsigned char *src = pg_prop_draw_bytes(tc, 0, 256, &len);
	int			enclen;
	int			declen;
	char	   *enc;
	unsigned char *dec;

	/* encode into a buffer sized by the library's own length helper */
	enc = pg_malloc(pg_b64_enc_len((int) len) + 1);
	enclen = pg_b64_encode(src, (int) len, enc, pg_b64_enc_len((int) len));
	if (enclen < 0)
		pg_prop_fail(tc, "encode failed for len=%zu", len);

	/* decode back; the result must be byte-identical to the input */
	dec = pg_malloc(pg_b64_dec_len(enclen) + 1);
	declen = pg_b64_decode(enc, enclen, dec, pg_b64_dec_len(enclen));
	if (declen < 0)
		pg_prop_fail(tc, "decode failed for len=%zu enclen=%d", len, enclen);

	if (declen != (int) len || memcmp(dec, src, len) != 0)
		pg_prop_fail(tc, "roundtrip mismatch: len=%zu declen=%d", len, declen);

	pg_free(dec);
	pg_free(enc);
	pg_free(src);
}

PG_PROP(enc_len_bounds_output)
{
	size_t		len;
	unsigned char *src = pg_prop_draw_bytes(tc, 0, 256, &len);
	int			bound = pg_b64_enc_len((int) len);
	char	   *enc = pg_malloc(bound + 1);
	int			enclen = pg_b64_encode(src, (int) len, enc, bound);

	/*
	 * pg_b64_enc_len() is what every caller uses to size the destination, so
	 * the actual encoded length must never exceed it.
	 */
	if (enclen < 0 || enclen > bound)
		pg_prop_fail(tc, "encoded length %d exceeds pg_b64_enc_len bound %d (input len=%zu)",
					 enclen, bound, len);

	pg_free(enc);
	pg_free(src);
}

PG_PROP(decode_garbage_no_overrun)
{
	size_t		len;
	char	   *src = pg_prop_draw_from(tc,
									   "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
									   "abcdefghijklmnopqrstuvwxyz0123456789+/= \n",
									   0, 256, &len);
	int			bound = pg_b64_dec_len((int) len);
	unsigned char *dst = pg_malloc(bound > 0 ? bound + 1 : 1);
	int			declen = pg_b64_decode(src, (int) len, dst, bound);

	/*
	 * Arbitrary base64-alphabet-ish input either decodes to at most the
	 * length pg_b64_dec_len() promised, or is rejected (< 0); it must never
	 * report writing more than the buffer bound.
	 */
	if (declen > bound)
		pg_prop_fail(tc, "decoded length %d exceeds pg_b64_dec_len bound %d (input len=%zu)",
					 declen, bound, len);

	pg_free(dst);
	pg_free(src);
}
