/*-------------------------------------------------------------------------
 *
 * prop_path.c
 *	  Property-based tests for src/port/path.c path manipulation.
 *
 * Target #10 in the design.  canonicalize_path() and friends are the trust
 * boundary for archive extraction (pg_basebackup, pg_rewind): a recent fix
 * (a1063eecedf, "Prevent path traversal in pg_basebackup and pg_rewind") and
 * the SJIS de-backslashing fix (6cddecdfb00) both live here.  Security-
 * relevant, pure, and cheap to exercise.
 *
 * Properties:
 *	- canonicalize_path is idempotent: canon(canon(p)) == canon(p).
 *	- canonicalize never lengthens a path (the code relies on this to rewrite
 *	  in place; prove it externally).
 *	- path_is_prefix_of_path(a,a) holds for any path (reflexive).
 *	- a path judged safe for extraction, once canonicalized, truly contains no
 *	  ".." component that could escape the extraction root.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_path.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "path"
#include "pg_prop.h"
#include "port.h"

/*
 * Draw a path-shaped string: slashes, dots, and a few letters, so most draws
 * are actual paths with components, "." and ".." in them rather than random
 * noise.  Keep it bounded well under MAXPGPATH.
 */
static char *
draw_path(pg_prop_tc *tc, size_t *len)
{
	return pg_prop_draw_from(tc, "/.ab", 0, 200, len);
}

PG_PROP(canonicalize_idempotent)
{
	size_t		len;
	char	   *p = draw_path(tc, &len);
	char		a[MAXPGPATH];
	char		b[MAXPGPATH];

	/* canonicalize_path works on NUL-terminated input within MAXPGPATH */
	strlcpy(a, p, sizeof(a));
	canonicalize_path(a);

	strlcpy(b, a, sizeof(b));
	canonicalize_path(b);

	if (strcmp(a, b) != 0)
		pg_prop_fail(tc, "not idempotent: canon(%s)=\"%s\" but canon again=\"%s\"",
					 p, a, b);
	pg_free(p);
}

PG_PROP(canonicalize_never_grows)
{
	size_t		len;
	char	   *p = draw_path(tc, &len);
	char		a[MAXPGPATH];
	size_t		before,
				after;

	strlcpy(a, p, sizeof(a));
	before = strlen(a);
	canonicalize_path(a);
	after = strlen(a);

	/*
	 * canonicalize_path rewrites in place and documents that it never makes
	 * the path longer.  If that is ever violated it is a buffer-overflow
	 * waiting to happen.
	 */
	if (after > before)
		pg_prop_fail(tc, "canon grew \"%s\" (%zu) to \"%s\" (%zu)",
					 p, before, a, after);
	pg_free(p);
}

PG_PROP(prefix_reflexive)
{
	size_t		len;
	char	   *p = draw_path(tc, &len);

	/* every path is a prefix of itself */
	if (!path_is_prefix_of_path(p, p))
		pg_prop_fail(tc, "path is not a prefix of itself: \"%s\"", p);
	pg_free(p);
}

PG_PROP(safe_extraction_has_no_parent_ref)
{
	size_t		len;
	char	   *p = draw_path(tc, &len);
	char		buf[MAXPGPATH];

	if (path_is_safe_for_extraction(p))
	{
		/*
		 * If the path is declared safe to extract, then after canonicalizing
		 * it there must be no leading ".." component -- otherwise extraction
		 * could write outside the target directory.  This re-derives the
		 * check independently of path_is_relative_and_below_cwd().
		 */
		strlcpy(buf, p, sizeof(buf));
		canonicalize_path(buf);

		if (is_absolute_path(buf))
			pg_prop_fail(tc, "safe-for-extraction but absolute: \"%s\" -> \"%s\"",
						 p, buf);
		if (buf[0] == '.' && buf[1] == '.' &&
			(buf[2] == '\0' || buf[2] == '/'))
			pg_prop_fail(tc, "safe-for-extraction but escapes: \"%s\" -> \"%s\"",
						 p, buf);
	}
	pg_free(p);
}
