/* contrib/ltree/crc32.c */

/*
 * Implements CRC-32, as used in ltree.
 *
 * Note that the CRC is used in the on-disk format of GiST indexes, so we
 * must stay backwards-compatible!
 */

#include "postgres.h"
#include "ltree.h"

#include "crc32.h"
#include "utils/backend_runtime.h"
#include "utils/pg_crc.h"
#ifdef LOWER_NODE
#include "utils/pg_locale.h"
#endif

#ifdef LOWER_NODE

#define LTREE_CRC32_SESSION_STATE_KEY "ltree.crc32.session"

typedef struct LtreeCrc32SessionState
{
	pg_locale_t locale;
} LtreeCrc32SessionState;

static pg_locale_t
ltree_crc32_locale(void)
{
	LtreeCrc32SessionState *state;

	state = (LtreeCrc32SessionState *)
		PgSessionEnsureExtensionPrivateState(LTREE_CRC32_SESSION_STATE_KEY,
											 sizeof(LtreeCrc32SessionState),
											 NULL);
	if (state->locale == NULL)
		state->locale = pg_database_locale();

	return state->locale;
}

unsigned int
ltree_crc32_sz(const char *buf, int size)
{
	pg_crc32	crc;
	const char *p = buf;
	const char *end = buf + size;
	pg_locale_t locale = ltree_crc32_locale();

	INIT_TRADITIONAL_CRC32(crc);
	while (size > 0)
	{
		char		foldstr[UNICODE_CASEMAP_BUFSZ];
		int			srclen = pg_mblen_range(p, end);
		size_t		foldlen;

		/* fold one codepoint at a time */
		foldlen = pg_strfold(foldstr, UNICODE_CASEMAP_BUFSZ, p, srclen,
							 locale);

		COMP_TRADITIONAL_CRC32(crc, foldstr, foldlen);

		size -= srclen;
		p += srclen;
	}
	FIN_TRADITIONAL_CRC32(crc);
	return (unsigned int) crc;
}

#else

unsigned int
ltree_crc32_sz(const char *buf, int size)
{
	pg_crc32	crc;
	const char *p = buf;

	INIT_TRADITIONAL_CRC32(crc);
	while (size > 0)
	{
		COMP_TRADITIONAL_CRC32(crc, p, 1);
		size--;
		p++;
	}
	FIN_TRADITIONAL_CRC32(crc);
	return (unsigned int) crc;
}

#endif							/* !LOWER_NODE */
