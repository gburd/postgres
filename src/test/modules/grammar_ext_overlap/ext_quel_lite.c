/*-------------------------------------------------------------------------
 *
 * ext_quel_lite.c
 *	  Minimal QUEL extension for the overlap test.
 *
 * Trimmed-down version of contrib/quel: just RETRIEVE and APPEND
 * keywords, two rules.  Full contrib/quel is loaded separately;
 * this lite version exists so the overlap test doesn't pull in
 * contrib/quel's full grammar (which would be tested independently).
 *
 * Portions Copyright (c) 2026, PostgreSQL Global Development Group
 *-------------------------------------------------------------------------
 */
#include "overlap_helpers.h"

#include "fmgr.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

static const OvlToken tokens[] = {
	{"K_QUEL_LITE_RETRIEVE", "retrieve_lite"},
	{"K_QUEL_LITE_APPEND", "append_lite"},
};

static const OvlRule rules[] = {
	{"stmt", {"K_QUEL_LITE_RETRIEVE", NULL}, "quel_lite:retrieve"},
	{"stmt", {"K_QUEL_LITE_APPEND", NULL}, "quel_lite:append"},
};

void
_PG_init(void)
{
	const OvlSpec spec = {
		.name = "ext_quel_lite",
		.tokens = tokens, .ntokens = lengthof(tokens),
		.rules = rules, .nrules = lengthof(rules),
	};

	register_overlap_extension(&spec);
}
