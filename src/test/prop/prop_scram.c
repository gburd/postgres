/*-------------------------------------------------------------------------
 *
 * prop_scram.c
 *	  Property-based tests for src/common/scram-common.c.
 *
 * SCRAM secret derivation (scram_SaltedPassword, scram_ClientKey,
 * scram_ServerKey, scram_build_secret) is the heart of SCRAM-SHA-256
 * authentication.  It is deterministic in all of its inputs -- password,
 * salt, iteration count -- so the same inputs must yield the same keys, and a
 * change in any input must change them.  Getting this wrong is an
 * authentication correctness and security problem, which makes it worth
 * pinning down with properties.
 *
 * Tier A: in-process, no server needed.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_scram.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "scram"
#include "pg_prop.h"

#include "common/cryptohash.h"
#include "common/scram-common.h"

#define KEYLEN SCRAM_SHA_256_KEY_LEN

/*
 * Draw an ASCII password (SCRAM input is a UTF-8 string; keep it to printable
 * ASCII so the SQL-free literal is unambiguous and SASLprep is a no-op).
 */
static char *
draw_password(pg_prop_tc *tc, size_t *len)
{
	return pg_prop_draw_from(tc,
							 "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ",
							 0, 40, len);
}

/* Compute the salted password for the given inputs; fail on any error. */
static void
salted(pg_prop_tc *tc, const char *pw, const uint8 *salt, int saltlen,
	   int iters, uint8 *out)
{
	const char *errstr = NULL;

	if (scram_SaltedPassword(pw, PG_SHA256, KEYLEN, salt, saltlen, iters,
							 out, &errstr) < 0)
		pg_prop_fail(tc, "scram_SaltedPassword failed: %s",
					 errstr ? errstr : "(no message)");
}

PG_PROP(salted_password_deterministic)
{
	size_t		pwlen;
	size_t		saltlen;
	char	   *pw = draw_password(tc, &pwlen);
	unsigned char *salt = pg_prop_draw_bytes(tc, 1, 32, &saltlen);
	int			iters = (int) pg_prop_draw_int(tc, 1, 50);
	uint8		a[KEYLEN];
	uint8		b[KEYLEN];

	/* same password, salt, and iteration count -> same salted password */
	salted(tc, pw, salt, (int) saltlen, iters, a);
	salted(tc, pw, salt, (int) saltlen, iters, b);

	if (memcmp(a, b, KEYLEN) != 0)
		pg_prop_fail(tc, "salted password not deterministic (pwlen=%zu iters=%d)",
					 pwlen, iters);

	pg_free(salt);
	pg_free(pw);
}

PG_PROP(client_server_keys_deterministic_and_differ)
{
	size_t		pwlen;
	size_t		saltlen;
	char	   *pw = draw_password(tc, &pwlen);
	unsigned char *salt = pg_prop_draw_bytes(tc, 1, 32, &saltlen);
	int			iters = (int) pg_prop_draw_int(tc, 1, 50);
	uint8		sp[KEYLEN];
	uint8		ck[KEYLEN];
	uint8		sk[KEYLEN];
	uint8		ck2[KEYLEN];
	const char *errstr = NULL;

	salted(tc, pw, salt, (int) saltlen, iters, sp);

	if (scram_ClientKey(sp, PG_SHA256, KEYLEN, ck, &errstr) < 0 ||
		scram_ServerKey(sp, PG_SHA256, KEYLEN, sk, &errstr) < 0 ||
		scram_ClientKey(sp, PG_SHA256, KEYLEN, ck2, &errstr) < 0)
		pg_prop_fail(tc, "scram key derivation failed: %s",
					 errstr ? errstr : "(no message)");

	/* ClientKey is deterministic */
	if (memcmp(ck, ck2, KEYLEN) != 0)
		pg_prop_fail(tc, "ClientKey not deterministic");

	/*
	 * ClientKey and ServerKey are derived from the same salted password by
	 * different HMAC messages ("Client Key" vs "Server Key"), so they must
	 * differ -- if they were equal the protocol's separation would be broken.
	 */
	if (memcmp(ck, sk, KEYLEN) == 0)
		pg_prop_fail(tc, "ClientKey equals ServerKey");

	pg_free(salt);
	pg_free(pw);
}

PG_PROP(salt_change_changes_salted_password)
{
	size_t		pwlen;
	size_t		saltlen;
	char	   *pw = draw_password(tc, &pwlen);
	unsigned char *salt = pg_prop_draw_bytes(tc, 1, 32, &saltlen);
	int			iters = (int) pg_prop_draw_int(tc, 1, 50);
	size_t		pos = 0;
	uint8		a[KEYLEN];
	uint8		b[KEYLEN];

	salted(tc, pw, salt, (int) saltlen, iters, a);

	/* flip one bit of the salt; the salted password must change */
	pos = (size_t) pg_prop_draw_int(tc, 0, (int) saltlen - 1);
	salt[pos] ^= 0x01;
	salted(tc, pw, salt, (int) saltlen, iters, b);

	if (memcmp(a, b, KEYLEN) == 0)
		pg_prop_fail(tc, "salted password unchanged after altering salt byte %zu",
					 pos);

	pg_free(salt);
	pg_free(pw);
}

PG_PROP(build_secret_nonempty_and_stable_shape)
{
	size_t		pwlen;
	size_t		saltlen;
	char	   *pw = draw_password(tc, &pwlen);
	unsigned char *salt = pg_prop_draw_bytes(tc, 1, 32, &saltlen);
	int			iters = (int) pg_prop_draw_int(tc, 1, 50);
	const char *errstr = NULL;
	char	   *s1;
	char	   *s2;

	/*
	 * scram_build_secret is deterministic in (hash, key_length, salt,
	 * iterations, password): two calls with identical inputs yield identical
	 * SCRAM verifier strings.
	 */
	s1 = scram_build_secret(PG_SHA256, KEYLEN, salt, (int) saltlen, iters, pw, &errstr);
	s2 = scram_build_secret(PG_SHA256, KEYLEN, salt, (int) saltlen, iters, pw, &errstr);
	if (s1 == NULL || s2 == NULL)
		pg_prop_fail(tc, "scram_build_secret failed: %s",
					 errstr ? errstr : "(no message)");

	if (strcmp(s1, s2) != 0)
		pg_prop_fail(tc, "scram_build_secret not deterministic:\n  %s\n  %s", s1, s2);

	free(s1);
	free(s2);
	pg_free(salt);
	pg_free(pw);
}
