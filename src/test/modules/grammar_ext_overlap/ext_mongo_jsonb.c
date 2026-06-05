/*-------------------------------------------------------------------------
 *
 * ext_mongo_jsonb.c
 *	  Simulates a MongoDB-style JSONB query extension.
 *
 * Adds FIND and AGGPIPE -- mongo-flavored entry points for JSONB
 * queries.  Each takes a JSONB document as RHS but for this test
 * the rules are bare keywords (the value-shaping code lives in the
 * reduce callback, not in the grammar).
 *
 * Portions Copyright (c) 2026, PostgreSQL Global Development Group
 *-------------------------------------------------------------------------
 */
#include "overlap_helpers.h"

#include "fmgr.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

static const OvlToken tokens[] = {
	{"K_MONGO_FIND", "mongo_find"},
	{"K_MONGO_AGGPIPE", "mongo_aggpipe"},
};

static const OvlRule rules[] = {
	{"stmt", {"K_MONGO_FIND", NULL}, "mongo:find"},
	{"stmt", {"K_MONGO_AGGPIPE", NULL}, "mongo:aggpipe"},
};

void
_PG_init(void)
{
	const OvlSpec spec = {
		.name = "ext_mongo_jsonb",
		.tokens = tokens, .ntokens = lengthof(tokens),
		.rules = rules, .nrules = lengthof(rules),
	};

	register_overlap_extension(&spec);
}
