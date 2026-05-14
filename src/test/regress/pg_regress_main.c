/*-------------------------------------------------------------------------
 *
 * pg_regress_main --- regression test for the main backend
 *
 * This is a C implementation of the previous shell script for running
 * the regression tests, and should be mostly compatible with it.
 * Initial author of C translation: Magnus Hagander
 *
 * This code is released under the terms of the PostgreSQL License.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/test/regress/pg_regress_main.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <ctype.h>

#include "common/string.h"
#include "lib/stringinfo.h"
#include "pg_regress.h"

/*
 * start a psql test process for specified file (including redirection),
 * and return process ID
 */
static PID_TYPE
psql_start_test(const char *testname,
				_stringlist **resultfiles,
				_stringlist **expectfiles,
				_stringlist **tags)
{
	PID_TYPE	pid;
	char		infile[MAXPGPATH];
	char		outfile[MAXPGPATH];
	char		expectfile[MAXPGPATH];
	StringInfoData psql_cmd;
	char	   *appnameenv;

	/*
	 * Look for files in the output dir first, consistent with a vpath search.
	 * This is mainly to create more reasonable error messages if the file is
	 * not found.  It also allows local test overrides when running pg_regress
	 * outside of the source tree.
	 */
	snprintf(infile, sizeof(infile), "%s/sql/%s.sql",
			 outputdir, testname);
	if (!file_exists(infile))
		snprintf(infile, sizeof(infile), "%s/sql/%s.sql",
				 inputdir, testname);

	snprintf(outfile, sizeof(outfile), "%s/results/%s.out",
			 outputdir, testname);

	snprintf(expectfile, sizeof(expectfile), "%s/expected/%s.out",
			 expecteddir, testname);
	if (!file_exists(expectfile))
		snprintf(expectfile, sizeof(expectfile), "%s/expected/%s.out",
				 inputdir, testname);

	add_stringlist_item(resultfiles, outfile);
	add_stringlist_item(expectfiles, expectfile);

	initStringInfo(&psql_cmd);

	if (launcher)
		appendStringInfo(&psql_cmd, "%s ", launcher);

	/*
	 * Use HIDE_TABLEAM to hide different AMs to allow to use regression tests
	 * against different AMs without unnecessary differences.
	 */
	appendStringInfo(&psql_cmd,
					 "\"%s%spsql\" -X -a -q -d \"%s\" %s < \"%s\" > \"%s\" 2>&1",
					 bindir ? bindir : "",
					 bindir ? "/" : "",
					 dblist->str,
					 "-v HIDE_TABLEAM=on -v HIDE_TOAST_COMPRESSION=on",
					 infile,
					 outfile);

	appnameenv = psprintf("pg_regress/%s", testname);
	setenv("PGAPPNAME", appnameenv, 1);
	free(appnameenv);

	pid = spawn_process(psql_cmd.data);

	if (pid == INVALID_PID)
	{
		fprintf(stderr, _("could not start process for test %s\n"),
				testname);
		exit(2);
	}

	unsetenv("PGAPPNAME");

	pfree(psql_cmd.data);

	return pid;
}

static void
psql_init(int argc, char **argv)
{
	/* set default regression database name */
	add_stringlist_item(&dblist, "regression");
}

/*
 * Replace a run of digits starting at *p with a single '#' character.
 * Returns pointer to the replacement character (the '#').
 */
static char *
replace_digits(char *p)
{
	char	   *end = p;

	while (isdigit((unsigned char) *end))
		end++;

	/* Replace the span with '#' and shift the rest of the string */
	*p = '#';
	if (end > p + 1)
		memmove(p + 1, end, strlen(end) + 1);

	return p;
}

/*
 * Normalize non-deterministic output in regression test result files.
 *
 * This filters result files in-place to replace run-specific values
 * (buffer IDs, relation OIDs, timing values) with stable placeholders
 * so that diff-based comparison with expected output succeeds across
 * different test runs.
 *
 * Patterns normalized:
 *   WARNING:  resource was not closed: [NNN] (rel=base/NNN/NNN, ...)
 *     -> WARNING:  resource was not closed: [#] (rel=base/#/#, ...)
 *   Planning Time: N.NNN ms  ->  Planning Time: #.# ms
 *   Execution Time: N.NNN ms ->  Execution Time: #.# ms
 */
static void
psql_postprocess_result(const char *filename)
{
	FILE	   *s,
			   *t;
	StringInfoData linebuf;
	char		tmpfile[MAXPGPATH];

	snprintf(tmpfile, sizeof(tmpfile), "%s.tmp", filename);

	s = fopen(filename, "r");
	if (!s)
		return;
	t = fopen(tmpfile, "w");
	if (!t)
	{
		fclose(s);
		return;
	}

	initStringInfo(&linebuf);

	while (pg_get_line_buf(s, &linebuf))
	{
		char	   *p;

		/*
		 * Normalize "resource was not closed: [NNN] (rel=base/NNN/NNN, ...)"
		 *
		 * The bracket number, database OID, and relation file number are all
		 * non-deterministic.
		 */
		p = strstr(linebuf.data, "resource was not closed: [");
		if (p)
		{
			char	   *q;

			/* Replace the number inside brackets: [NNN] -> [#] */
			q = p + strlen("resource was not closed: [");
			if (isdigit((unsigned char) *q))
				replace_digits(q);

			/* Replace numbers after "rel=base/" */
			q = strstr(p, "rel=base/");
			if (q)
			{
				q += strlen("rel=base/");
				if (isdigit((unsigned char) *q))
				{
					q = replace_digits(q);
					/* Skip the '/' separator */
					if (*(q + 1) == '/')
					{
						q += 2;
						if (isdigit((unsigned char) *q))
							replace_digits(q);
					}
				}
			}
		}

		/*
		 * Normalize "Planning Time: N.NNN ms" and "Execution Time: N.NNN ms"
		 *
		 * These timing values vary between runs.
		 */
		p = strstr(linebuf.data, "Planning Time: ");
		if (!p)
			p = strstr(linebuf.data, "Execution Time: ");
		if (p)
		{
			/* Find the start of the number after ": " */
			char	   *q = strchr(p, ':');

			if (q)
			{
				q++;
				while (*q == ' ')
					q++;
				if (isdigit((unsigned char) *q))
				{
					replace_digits(q);
					/* Skip past '#' and the decimal point */
					q++;
					if (*q == '.')
					{
						q++;
						if (isdigit((unsigned char) *q))
							replace_digits(q);
					}
				}
			}
		}

		fputs(linebuf.data, t);
	}

	pfree(linebuf.data);
	fclose(s);
	fclose(t);
	if (rename(tmpfile, filename) != 0)
	{
		fprintf(stderr, "Could not overwrite file %s with %s\n",
				filename, tmpfile);
	}
}

int
main(int argc, char *argv[])
{
	return regression_main(argc, argv,
						   psql_init,
						   psql_start_test,
						   psql_postprocess_result);
}
