/*-------------------------------------------------------------------------
 *
 * prop_crypto.c
 *	  Property-based tests for src/common cryptohash and HMAC.
 *
 * The hash and HMAC wrappers (pg_cryptohash_*, pg_hmac_*) back SCRAM, MD5
 * auth, checksums, and the backup manifest.  They are deterministic functions
 * with fixed-size output: the same input must always produce the same digest,
 * a one-byte change must change it, and the digest is exactly the type's
 * length.  These are the properties every caller silently relies on.
 *
 * Tier A: in-process, no server needed.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_crypto.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "crypto"
#include "pg_prop.h"

#include "common/cryptohash.h"
#include "common/hmac.h"
#include "common/sha2.h"

/*
 * Compute a SHA-256 digest of src[0..len) into dest (PG_SHA256_DIGEST_LENGTH
 * bytes).  Fails the test case on any wrapper error (these should never fail
 * on valid input).
 */
static void
sha256(pg_prop_tc *tc, const uint8 *src, size_t len, uint8 *dest)
{
	pg_cryptohash_ctx *ctx = pg_cryptohash_create(PG_SHA256);

	if (ctx == NULL)
		pg_prop_fail(tc, "pg_cryptohash_create failed");
	if (pg_cryptohash_init(ctx) < 0 ||
		pg_cryptohash_update(ctx, src, len) < 0 ||
		pg_cryptohash_final(ctx, dest, PG_SHA256_DIGEST_LENGTH) < 0)
	{
		pg_cryptohash_free(ctx);
		pg_prop_fail(tc, "sha256 computation failed for len=%zu", len);
	}
	pg_cryptohash_free(ctx);
}

PG_PROP(sha256_deterministic)
{
	size_t		len;
	unsigned char *src = pg_prop_draw_bytes(tc, 0, 256, &len);
	uint8		a[PG_SHA256_DIGEST_LENGTH];
	uint8		b[PG_SHA256_DIGEST_LENGTH];

	/* hashing the same bytes twice must give the same digest */
	sha256(tc, src, len, a);
	sha256(tc, src, len, b);

	if (memcmp(a, b, PG_SHA256_DIGEST_LENGTH) != 0)
		pg_prop_fail(tc, "sha256 not deterministic for len=%zu", len);

	pg_free(src);
}

PG_PROP(sha256_sensitive_to_single_bit)
{
	size_t		len;
	unsigned char *src;
	uint8		a[PG_SHA256_DIGEST_LENGTH];
	uint8		b[PG_SHA256_DIGEST_LENGTH];
	size_t		pos;
	int			bit;

	/* need at least one byte to flip */
	src = pg_prop_draw_bytes(tc, 1, 256, &len);
	pos = (size_t) pg_prop_draw_int(tc, 0, (int) len - 1);
	bit = (int) pg_prop_draw_int(tc, 0, 7);

	sha256(tc, src, len, a);
	src[pos] ^= (unsigned char) (1 << bit);
	sha256(tc, src, len, b);

	/*
	 * Flipping any single bit of the input must change the digest.  A
	 * collision here would be a catastrophic hash failure.
	 */
	if (memcmp(a, b, PG_SHA256_DIGEST_LENGTH) == 0)
		pg_prop_fail(tc, "sha256 unchanged after flipping bit %d of byte %zu (len=%zu)",
					 bit, pos, len);

	pg_free(src);
}

PG_PROP(hmac_sha256_deterministic_and_sized)
{
	size_t		klen;
	size_t		dlen;
	unsigned char *key = pg_prop_draw_bytes(tc, 0, 64, &klen);
	unsigned char *data = pg_prop_draw_bytes(tc, 0, 256, &dlen);
	uint8		a[PG_SHA256_DIGEST_LENGTH];
	uint8		b[PG_SHA256_DIGEST_LENGTH];
	pg_hmac_ctx *ctx;

	/* compute the HMAC twice over the same key and data */
	for (int pass = 0; pass < 2; pass++)
	{
		uint8	   *dest = (pass == 0) ? a : b;

		ctx = pg_hmac_create(PG_SHA256);
		if (ctx == NULL)
			pg_prop_fail(tc, "pg_hmac_create failed");
		if (pg_hmac_init(ctx, key, klen) < 0 ||
			pg_hmac_update(ctx, data, dlen) < 0 ||
			pg_hmac_final(ctx, dest, PG_SHA256_DIGEST_LENGTH) < 0)
		{
			pg_hmac_free(ctx);
			pg_prop_fail(tc, "hmac computation failed (klen=%zu dlen=%zu)",
						 klen, dlen);
		}
		pg_hmac_free(ctx);
	}

	if (memcmp(a, b, PG_SHA256_DIGEST_LENGTH) != 0)
		pg_prop_fail(tc, "hmac-sha256 not deterministic (klen=%zu dlen=%zu)",
					 klen, dlen);

	pg_free(data);
	pg_free(key);
}
