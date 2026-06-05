/*-------------------------------------------------------------------------
 *
 * ext_pg_infer.c
 *	  Simulates the user's pg_infer extension -- inference DSL.
 *
 * Adds INFER, PREDICT, TRAIN -- ML-flavored entry points.  Each
 * surfaces as a top-level statement form.  Real pg_infer would
 * extend with type signatures; for the overlap test the keywords
 * alone exercise the compose path.
 *
 * Portions Copyright (c) 2026, PostgreSQL Global Development Group
 *-------------------------------------------------------------------------
 */
#include "overlap_helpers.h"

#include "fmgr.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

static const OvlToken tokens[] = {
	{"K_INFER_INFER", "infer"},
	{"K_INFER_PREDICT", "predict"},
	{"K_INFER_TRAIN", "train"},
};

static const OvlRule rules[] = {
	{"stmt", {"K_INFER_INFER", NULL}, "infer:infer"},
	{"stmt", {"K_INFER_PREDICT", NULL}, "infer:predict"},
	{"stmt", {"K_INFER_TRAIN", NULL}, "infer:train"},
};

void
_PG_init(void)
{
	const OvlSpec spec = {
		.name = "ext_pg_infer",
		.tokens = tokens, .ntokens = lengthof(tokens),
		.rules = rules, .nrules = lengthof(rules),
	};

	register_overlap_extension(&spec);
}
