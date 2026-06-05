/*-------------------------------------------------------------------------
 *
 * ext_duckdb_compat.c
 *	  Simulates a DuckDB SQL compatibility extension.
 *
 * Adds PIVOT, UNPIVOT, QUALIFY -- DuckDB-specific clauses that PG
 * doesn't natively support.  For the test, each surfaces as a
 * top-level statement that NOTICEs which production fired.
 *
 * Portions Copyright (c) 2026, PostgreSQL Global Development Group
 *-------------------------------------------------------------------------
 */
#include "overlap_helpers.h"

#include "fmgr.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

static const OvlToken tokens[] = {
	{"K_DUCKDB_PIVOT", "pivot"},
	{"K_DUCKDB_UNPIVOT", "unpivot"},
	{"K_DUCKDB_QUALIFY", "qualify"},
};

static const OvlRule rules[] = {
	{"stmt", {"K_DUCKDB_PIVOT", NULL}, "duckdb:pivot"},
	{"stmt", {"K_DUCKDB_UNPIVOT", NULL}, "duckdb:unpivot"},
	{"stmt", {"K_DUCKDB_QUALIFY", NULL}, "duckdb:qualify"},
};

void
_PG_init(void)
{
	const OvlSpec spec = {
		.name = "ext_duckdb_compat",
		.tokens = tokens, .ntokens = lengthof(tokens),
		.rules = rules, .nrules = lengthof(rules),
	};

	register_overlap_extension(&spec);
}
