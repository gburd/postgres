/*-------------------------------------------------------------------------
 *
 * ext_mysql_compat.c
 *	  Simulates a MySQL SQL compatibility extension.
 *
 * Adds DESCRIBE (alias for EXPLAIN) and USE (database switch).
 * Also adds UTC_TIMESTAMP as a MySQL-flavored function-style token
 * to test that lowercase keywords and underscore-bearing tokens
 * work cleanly through the extension API.
 *
 * Portions Copyright (c) 2026, PostgreSQL Global Development Group
 *-------------------------------------------------------------------------
 */
#include "overlap_helpers.h"

#include "fmgr.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

static const OvlToken tokens[] = {
	{"K_MYSQL_DESCRIBE", "describe_mysql"},
	{"K_MYSQL_USE", "use_mysql"},
	{"K_MYSQL_UTC_TIMESTAMP", "utc_timestamp"},
};

static const OvlRule rules[] = {
	{"stmt", {"K_MYSQL_DESCRIBE", NULL}, "mysql:describe"},
	{"stmt", {"K_MYSQL_USE", NULL}, "mysql:use"},
	{"stmt", {"K_MYSQL_UTC_TIMESTAMP", NULL}, "mysql:utc_timestamp"},
};

void
_PG_init(void)
{
	const OvlSpec spec = {
		.name = "ext_mysql_compat",
		.tokens = tokens, .ntokens = lengthof(tokens),
		.rules = rules, .nrules = lengthof(rules),
	};

	register_overlap_extension(&spec);
}
