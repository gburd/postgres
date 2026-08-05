/*-------------------------------------------------------------------------
 *
 * parser_extension.c
 *	  Runtime grammar extension API -- in-process compose implementation
 *	  (Phase 4 Track B).
 *
 * The C contract in include/parser/parser_extension.h is the final
 * shape; extensions compile against it.  This translation unit:
 *
 *   1. Accepts and validates all add_token / add_type / add_rule /
 *      set_precedence calls.  Stores them in a per-extension memory
 *      context owned by the handle.
 *   2. On register(), serializes the extension to a .lime-syntax
 *      fragment (empty action bodies -- reduces are dispatched at parse
 *      time via host-reduce callbacks) and queues it on a static
 *      "pending" list, recording each rule's composed rule-id.  Multiple
 *      extensions registered from different shared_preload_libraries are
 *      batched.
 *   3. pg_grammar_ext_prewarm(), called once at postmaster start after
 *      all _PG_init()s (see miscinit.c), composes the base grammar source
 *      with the queued fragments and compiles the result to runtime
 *      ParserSnapshots via lime_compile_grammar_in_process()
 *      (parser_pushparse.c).  It builds the default "all" snapshot (base
 *      SQL + every extension) and one isolated snapshot per distinct
 *      extension/dialect name (base SQL + that dialect only).  No
 *      subprocess, no C compiler, no .so cache, no dlopen.  The cost is
 *      absorbed pre-fork so backends see warm parsers with no cold-start.
 *   4. At parse time a session selects one dialect snapshot via the
 *      grammar_dialect GUC; the push driver routes base rules to the
 *      generated base actions (base_yyHostReduce) and extension rules to
 *      their registered PgGrammarReduceFn, keyed by each snapshot's own
 *      ruleno -> rule_id map resolved by stable rule identity (Lime
 *      v1.10.0 lime_snapshot_rule_by_id).
 *
 * The historical Track A path (serialize -> fork lime + cc -> dlopen a
 * cached .so) has been removed entirely; this file is the in-process
 * implementation.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/parser/parser_extension.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "parser/parser_extension.h"
#include "parser/scanner.h"
#include "utils/elog.h"
#include "utils/memutils.h"

/*
 * Track B compose entry point (parser_pushparse.c).  Merges the base
 * grammar source with the registered extension fragments, compiles the
 * result to a runtime ParserSnapshot in-process (no subprocess, no cc),
 * and installs it as the active parser snapshot.  base_nrule_out receives
 * the base grammar's rule count so the host-reduce dispatcher can split
 * base vs extension rules.
 */
extern bool pg_grammar_compose_install(unsigned int *base_nrule_out,
									   char **errmsg_out);

/*
 * One per add_rule call.  Linked list rooted at PgGrammarExtension.rules.
 *
 * `rule_id` is assigned at register() time from a process-global atomic
 * counter; the rebuilt parser .so calls back via
 * pg_grammar_ext_dispatch_reduce(rule_id, ...) which uses this id to
 * find the user's reduce callback in `g_rule_table`.
 */
typedef struct ExtRule
{
	struct ExtRule *next;
	const char *lhs;
	const char **rhs;
	int			nrhs;
	PgGrammarReduceFn reduce;
	void	   *reduce_user;
	unsigned int rule_id;		/* 0 until register() runs */
} ExtRule;

typedef struct ExtToken
{
	struct ExtToken *next;
	const char *name;
	const char *lexeme;
	PgGrammarExtKeywordCategory category;
} ExtToken;

typedef struct ExtType
{
	struct ExtType *next;
	const char *name;
	const char *datatype;
} ExtType;

typedef struct ExtPrec
{
	struct ExtPrec *next;
	const char *symbol;
	int			level;
	PgGrammarExtAssoc assoc;
} ExtPrec;

struct PgGrammarExtension
{
	const char *name;
	const char *version;

	/*
	 * Memory context owning all strings/lists below.  Allocated as a child of
	 * TopMemoryContext so the extension survives backend life and forks.
	 */
	MemoryContext context;

	ExtToken   *tokens;
	ExtType    *types;
	ExtRule    *rules;
	ExtPrec    *precs;

	bool		registered;

	/*
	 * Serialized .lime-fragment produced by pg_grammar_ext_register(). Owned
	 * by `context`.  NULL until register() runs.
	 */
	char	   *serialized_lime;
};

/*
 * Module-level guards.
 *
 * parser_locked: once raw_parser() runs the lock is taken; subsequent
 *   register() calls fail (extensions must register from _PG_init or
 *   shared_preload_libraries-loaded code).  Taking the lock is also when
 *   the registered extensions are composed into the active snapshot.
 *
 * pending_*: the registered extensions queued for compose.  Stored as
 *   a parallel array of fragment text + provenance for diagnostics.
 *   Owned by parser_extension_context (a child of TopMemoryContext)
 *   so we survive memory-context resets between transactions.
 */
static bool parser_locked = false;
static MemoryContext parser_extension_context = NULL;

typedef struct PendingExt
{
	const char *name;
	const char *fragment;
	PgGrammarExtension *ext;	/* live handle (token list etc.) */
} PendingExt;

static PendingExt *pending = NULL;
static int	npending = 0;
static int	pending_capacity = 0;

/*
 * Hook published to scan.c so an extension keyword lexeme resolves to
 * its token code.  Wired by the keyword-override work (Track B P4);
 * NULL until then, which leaves the base scanner behaviour unchanged.
 */
PgGrammarExtKeywordHook pg_grammar_ext_keyword_hook = NULL;

/*
 * g_rule_table -- dispatch table for pg_grammar_ext_dispatch_reduce.
 *
 * Indexed by rule_id (assigned at register() time from a 1-based
 * counter; index 0 is unused so a dispatch with rule_id=0 is a clear
 * bug).  Each slot points to the ExtRule whose reduce/reduce_user the
 * trampoline forwards to.  The composed snapshot is recomposed every
 * time the set of registered extensions changes (the composed grammar
 * text includes the fragments, which include the rule_ids), so the
 * active snapshot is always paired with the rules that were live when
 * it was composed.
 *
 * The table is sized once at register() time and never resized, so
 * pointer reads from any backend are stable for the life of the
 * postmaster.  Rules are registered from _PG_init / shared_preload_libraries
 * paths only; pg_grammar_ext_lock_parser() refuses post-init registration.
 */
static ExtRule **g_rule_table = NULL;
static unsigned int g_rule_table_size = 0;
static unsigned int g_next_rule_id = 1;

/* Static prototypes (Phase 4 internals). */
static MemoryContext ext_context(void);
static char *serialize_extension(PgGrammarExtension *ext);
static void enqueue_pending(const char *name, const char *fragment,
							PgGrammarExtension *ext);


PgGrammarExtension *
pg_grammar_ext_create(const char *name, const char *version)
{
	MemoryContext ctx;
	PgGrammarExtension *ext;
	MemoryContext old;

	Assert(name != NULL);

	ctx = AllocSetContextCreate(TopMemoryContext,
								"PgGrammarExtension",
								ALLOCSET_SMALL_SIZES);
	old = MemoryContextSwitchTo(ctx);
	ext = palloc0(sizeof(*ext));
	ext->context = ctx;
	ext->name = pstrdup(name);
	ext->version = version ? pstrdup(version) : NULL;
	MemoryContextSwitchTo(old);
	return ext;
}

void
pg_grammar_ext_add_token(PgGrammarExtension *ext,
						 const char *name,
						 const char *lexeme,
						 PgGrammarExtKeywordCategory category)
{
	MemoryContext old;
	ExtToken   *tok;

	Assert(ext != NULL);
	Assert(name != NULL);
	if (ext->registered)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("grammar extension \"%s\" is already registered",
						ext->name)));

	old = MemoryContextSwitchTo(ext->context);
	tok = palloc(sizeof(*tok));
	tok->name = pstrdup(name);
	tok->lexeme = lexeme ? pstrdup(lexeme) : NULL;
	tok->category = category;
	tok->next = ext->tokens;
	ext->tokens = tok;
	MemoryContextSwitchTo(old);
}

void
pg_grammar_ext_add_type(PgGrammarExtension *ext,
						const char *name,
						const char *datatype)
{
	MemoryContext old;
	ExtType    *typ;

	Assert(ext != NULL);
	Assert(name != NULL);
	Assert(datatype != NULL);
	if (ext->registered)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("grammar extension \"%s\" is already registered",
						ext->name)));

	old = MemoryContextSwitchTo(ext->context);
	typ = palloc(sizeof(*typ));
	typ->name = pstrdup(name);
	typ->datatype = pstrdup(datatype);
	typ->next = ext->types;
	ext->types = typ;
	MemoryContextSwitchTo(old);
}

void
pg_grammar_ext_add_rule(PgGrammarExtension *ext,
						const char *lhs,
						const char **rhs,
						PgGrammarReduceFn reduce,
						void *reduce_user)
{
	MemoryContext old;
	ExtRule    *rule;
	int			nrhs = 0;

	Assert(ext != NULL);
	Assert(lhs != NULL);
	Assert(rhs != NULL);
	if (ext->registered)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("grammar extension \"%s\" is already registered",
						ext->name)));

	while (rhs[nrhs] != NULL)
		nrhs++;

	old = MemoryContextSwitchTo(ext->context);
	rule = palloc(sizeof(*rule));
	rule->lhs = pstrdup(lhs);
	rule->nrhs = nrhs;
	rule->rhs = palloc_array(const char *, nrhs);
	for (int i = 0; i < nrhs; i++)
		rule->rhs[i] = pstrdup(rhs[i]);
	rule->reduce = reduce;
	rule->reduce_user = reduce_user;
	rule->next = ext->rules;
	ext->rules = rule;
	MemoryContextSwitchTo(old);
}

void
pg_grammar_ext_set_precedence(PgGrammarExtension *ext,
							  const char *symbol,
							  int level,
							  PgGrammarExtAssoc assoc)
{
	MemoryContext old;
	ExtPrec    *prec;

	Assert(ext != NULL);
	Assert(symbol !=NULL);
	Assert(level >= 0);
	if (ext->registered)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("grammar extension \"%s\" is already registered",
						ext->name)));

	old = MemoryContextSwitchTo(ext->context);
	prec = palloc(sizeof(*prec));
	prec->symbol = pstrdup(symbol);
	prec->level = level;
	prec->assoc = assoc;
	prec->next = ext->precs;
	ext->precs = prec;
	MemoryContextSwitchTo(old);
}

bool
pg_grammar_ext_register(PgGrammarExtension *ext, char **err)
{
	MemoryContext old;
	char	   *fragment;

	Assert(ext != NULL);

	if (ext->registered)
	{
		if (err)
			*err = pstrdup("extension already registered");
		return false;
	}
	if (parser_locked)
	{
		if (err)
			*err = pstrdup("parser already initialised; "
						   "register grammar extensions from _PG_init() "
						   "or shared_preload_libraries-loaded code only");
		return false;
	}

	/*
	 * Assign rule_ids and populate the dispatch table.  Rule ids are 1-based
	 * so a stray dispatch with rule_id=0 is an obvious bug. The dispatch
	 * table grows monotonically across registrations; we never reclaim slots
	 * because the rebuilt .so is keyed on the exact ExtRule layout that was
	 * live at register time.
	 */
	for (ExtRule *r = ext->rules; r != NULL; r = r->next)
	{
		if (r->reduce == NULL)
			continue;			/* silent rule, no dispatch needed */
		r->rule_id = g_next_rule_id++;
	}
	if (g_next_rule_id > g_rule_table_size)
	{
		unsigned int newsize = g_rule_table_size ? g_rule_table_size * 2 : 16;

		while (newsize < g_next_rule_id)
			newsize *= 2;

		old = MemoryContextSwitchTo(ext_context());
		if (g_rule_table == NULL)
			g_rule_table = palloc0(sizeof(ExtRule *) * newsize);
		else
		{
			ExtRule   **newtab = palloc0(sizeof(ExtRule *) * newsize);

			memcpy(newtab, g_rule_table,
				   sizeof(ExtRule *) * g_rule_table_size);
			pfree(g_rule_table);
			g_rule_table = newtab;
		}
		g_rule_table_size = newsize;
		MemoryContextSwitchTo(old);
	}
	for (ExtRule *r = ext->rules; r != NULL; r = r->next)
	{
		if (r->reduce == NULL)
			continue;
		g_rule_table[r->rule_id] = r;
	}

	/*
	 * Serialize.  The text becomes part of the cache-key digest, so it must
	 * be deterministic across runs of the same extension set.
	 */
	old = MemoryContextSwitchTo(ext->context);
	fragment = serialize_extension(ext);
	ext->serialized_lime = fragment;
	MemoryContextSwitchTo(old);

	enqueue_pending(ext->name, fragment, ext);
	ext->registered = true;
	if (err)
		*err = NULL;
	return true;
}

/*
 * pg_grammar_ext_get_serialized_lime
 *	  Return the .lime-fragment text the registration step produced,
 *	  or NULL if register() hasn't been called.  The returned pointer
 *	  is owned by the extension handle; callers must NOT free it.
 */
const char *
pg_grammar_ext_get_serialized_lime(const PgGrammarExtension *ext)
{
	return ext ? ext->serialized_lime : NULL;
}

void
pg_grammar_ext_unregister(PgGrammarExtension *ext)
{
	if (ext == NULL)
		return;

	/*
	 * The pending list keeps only borrowed pointers into the extension's
	 * context; if the caller unregisters BEFORE lock_parser() composes the
	 * snapshot, those pointers become dangling.  We don't currently support
	 * that flow -- removing an extension after register() but before
	 * parse-time would require recomposing the snapshot.  SIGHUP-driven
	 * teardown/recompose is a later step; for now document the limitation.
	 */
	if (ext->registered && !parser_locked)
		ereport(WARNING,
				(errmsg("grammar extension \"%s\" was unregistered "
						"before the parser snapshot was composed; this leaves "
						"a dangling fragment in the compose queue",
						ext->name)));

	if (ext->context != NULL)
		MemoryContextDelete(ext->context);
}

/*
 * pg_grammar_ext_dispatch_reduce
 *	  Called from the rebuilt parser .so when a rule with a registered
 *	  PgGrammarReduceFn fires.  Forwards to the user callback after
 *	  validating the rule_id.
 *
 *	  Errors here are programmer errors (the rebuild produced bogus
 *	  serialized C, or the host/.so are out of sync) -- ereport(ERROR)
 *	  rather than fall back silently, so the user sees the diagnostic
 *	  instead of a parse that runs to a wrong tree.
 */
void
pg_grammar_ext_dispatch_reduce(unsigned int rule_id,
							   void *extra_arg,
							   int nrhs,
							   const void *const *rhs_values,
							   const int *rhs_locs,
							   void *lhs_out)
{
	ExtRule    *rule;

	if (rule_id == 0 || rule_id >= g_rule_table_size
		|| g_rule_table == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("grammar extension reduce dispatch: invalid "
						"rule_id %u (table size %u)",
						rule_id, g_rule_table_size)));

	rule = g_rule_table[rule_id];
	if (rule == NULL || rule->reduce == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("grammar extension reduce dispatch: rule_id %u "
						"has no registered callback", rule_id)));

	rule->reduce(rule->reduce_user, extra_arg, nrhs,
				 rhs_values, rhs_locs, lhs_out);
}

/*
 * pg_grammar_ext_lock_parser
 *	  Called by raw_parser() at the top of every parse.  The first call
 *	  composes the registered extension grammars into the active parser
 *	  snapshot, in-process (no subprocess, no C compiler); subsequent
 *	  calls are O(1).
 *
 *	  On failure we ereport(ERROR) so the user sees the compose error
 *	  rather than silently falling back to the pre-extension parser,
 *	  which would mask broken extensions.
 */
void
pg_grammar_ext_lock_parser(void)
{
	char	   *err_detail = NULL;
	unsigned int base_nrule = 0;

	if (parser_locked)
		return;
	parser_locked = true;

	if (npending == 0)
		return;

	if (!pg_grammar_compose_install(&base_nrule, &err_detail))
		ereport(ERROR,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("grammar extension compose failed: %s",
						err_detail ? err_detail : "(no detail)")));
}

/*
 * pg_grammar_ext_prewarm
 *	  Compose the registered extension grammars into the active parser
 *	  snapshot at postmaster startup, after all shared_preload_libraries
 *	  have run their _PG_init() (and so registered their extensions) but
 *	  before any backend forks.  Every backend then inherits the composed
 *	  snapshot across fork, so the first query in every session parses at
 *	  warm-cache speed -- no per-backend, no first-query compose latency.
 *
 *	  A compose failure here is FATAL: a broken grammar extension in
 *	  shared_preload_libraries should stop the postmaster at startup
 *	  rather than fail every backend's first parse.
 *
 *	  No-op when no grammar extension registered, or after the parser is
 *	  already locked (idempotent).
 */
void
pg_grammar_ext_prewarm(void)
{
	char	   *err_detail = NULL;
	unsigned int base_nrule = 0;

	if (parser_locked || npending == 0)
		return;
	parser_locked = true;

	if (!pg_grammar_compose_install(&base_nrule, &err_detail))
		ereport(FATAL,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("grammar extension compose failed at startup: %s",
						err_detail ? err_detail : "(no detail)")));
}


/*
 * ----------------------------------------------------------------------
 * Internals.
 * ----------------------------------------------------------------------
 */

static MemoryContext
ext_context(void)
{
	if (parser_extension_context == NULL)
		parser_extension_context =
			AllocSetContextCreate(TopMemoryContext,
								  "PgGrammarExtension/internal",
								  ALLOCSET_DEFAULT_SIZES);
	return parser_extension_context;
}

/*
 * serialize_extension
 *	  Render an extension into a .lime-syntax fragment.  Lives in a
 *	  child of the extension's MemoryContext, so it is owned by the
 *	  caller's context-switch frame.  Output mirrors the spelling
 *	  Lime upstream's lime_modifications_to_grammar_text() emits;
 *	  the two implementations must stay in sync (the dummy_grammar_ext
 *	  smoke test pins the exact fragment text via diff).
 */
static char *
serialize_extension(PgGrammarExtension *ext)
{
	StringInfoData buf;
	ExtToken   *tok;
	ExtType    *typ;
	ExtRule    *rule;
	ExtPrec    *prec;

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "/* Grammar extension: %s%s%s\n"
					 " * Generated by pg_grammar_ext_register() at backend\n"
					 " * startup.  Concatenate after the base gram.lime, then\n"
					 " * feed to `lime` to produce the extended parser. */\n\n",
					 ext->name,
					 ext->version ? " v" : "",
					 ext->version ? ext->version : "");

	/*
	 * Tokens.  The keyword category becomes a comment here; the %token
	 * directive itself doesn't carry category info.  Category-aware keyword
	 * behaviour at parse time is handled by the push driver's keyword map and
	 * admissibility oracle (parser_pushparse.c): each registered token's
	 * lexeme is published to scan.c via pg_grammar_ext_keyword_hook, and the
	 * oracle decides keyword-vs-identifier and keyword-vs-keyword per parse
	 * state.
	 */
	for (tok = ext->tokens; tok != NULL; tok = tok->next)
	{
		appendStringInfo(&buf,
						 "%%token %s.\t/* lexeme=\"%s\" category=%d */\n",
						 tok->name,
						 tok->lexeme ? tok->lexeme : "(none)",
						 tok->category);
	}
	if (ext->tokens != NULL)
		appendStringInfoChar(&buf, '\n');

	/* Non-terminal types. */
	for (typ = ext->types; typ != NULL; typ = typ->next)
		appendStringInfo(&buf, "%%type %s {%s}\n", typ->name, typ->datatype);
	if (ext->types != NULL)
		appendStringInfoChar(&buf, '\n');

	/* Precedence overrides. */
	for (prec = ext->precs; prec != NULL; prec = prec->next)
	{
		const char *kw;

		switch (prec->assoc)
		{
			case PG_GRAMMAR_ASSOC_LEFT:
				kw = "%left";
				break;
			case PG_GRAMMAR_ASSOC_RIGHT:
				kw = "%right";
				break;
			case PG_GRAMMAR_ASSOC_NONASSOC:
				kw = "%nonassoc";
				break;
			default:
				kw = "/* PG_GRAMMAR_ASSOC_NONE */";
				break;
		}
		appendStringInfo(&buf, "%s %s.\t/* level=%d */\n",
						 kw, prec->symbol, prec->level);
	}
	if (ext->precs != NULL)
		appendStringInfoChar(&buf, '\n');

	/*
	 * Rules.  Reduce-callback dispatch goes through
	 * pg_grammar_ext_dispatch_reduce(), which the rebuilt .so resolves
	 * against the host postgres binary at dlopen time (postgres is linked
	 * with --export-dynamic, making its globals visible to dlopen'd modules).
	 *
	 * For each rule we emit Lime's parenthesized-letter labels: the LHS
	 * becomes label `A`; the RHS symbols become `B`, `C`, ... inside the
	 * action body.  Lime expands these to lvalue expressions over its
	 * parse-stack (yymsp[N].minor.yyXXX), so `&B` is a valid pointer to the
	 * per-symbol-typed slot.  The trampoline assembles those pointers into an
	 * array and forwards to the user's PgGrammarReduceFn via the dispatch
	 * table.
	 *
	 * Hard limit: at most 25 RHS symbols (B-Z).  PG's grammar uses up to ~13;
	 * extension rules with more than 25 RHS symbols hit EREPORT below at
	 * register() time.
	 */
	for (rule = ext->rules; rule != NULL; rule = rule->next)
	{
		if (rule->nrhs > 25)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("grammar extension \"%s\" rule with LHS \"%s\" "
							"has %d RHS symbols (limit 25)",
							ext->name, rule->lhs, rule->nrhs)));

		appendStringInfo(&buf, "%s(A) ::= ", rule->lhs);
		for (int i = 0; i < rule->nrhs; i++)
		{
			if (i > 0)
				appendStringInfoChar(&buf, ' ');
			appendStringInfo(&buf, "%s(%c)",
							 rule->rhs[i],
							 'B' + i);
		}

		if (rule->reduce != NULL)
		{
			/*
			 * Track B: the composed grammar is compiled to a runtime
			 * ParserSnapshot (tables only -- no action code), so the rule's
			 * action body is empty here.  At parse time the push parser
			 * resolves this rule's composed ruleno by identity (Lime v1.10.0
			 * lime_snapshot_rule_by_id) and routes its reduce to the
			 * registered PgGrammarReduceFn via
			 * pg_grammar_ext_reduce_by_ruleno.
			 */
			appendStringInfoString(&buf, ".\n");
		}
		else
		{
			/* Silent rule: no reduce callback; nothing to dispatch. */
			appendStringInfoString(&buf, ".\n");
		}
	}

	return buf.data;
}

static void
enqueue_pending(const char *name, const char *fragment,
				PgGrammarExtension *ext)
{
	MemoryContext old = MemoryContextSwitchTo(ext_context());

	if (npending == pending_capacity)
	{
		int			newcap = pending_capacity ? pending_capacity * 2 : 4;

		pending = repalloc(pending ? pending : palloc0(0),
						   sizeof(PendingExt) * newcap);
		pending_capacity = newcap;
	}
	pending[npending].name = pstrdup(name);
	pending[npending].fragment = pstrdup(fragment);
	pending[npending].ext = ext;
	npending++;
	MemoryContextSwitchTo(old);
}

/*
 * ruleno_to_ruleid -- the ACTIVE composed snapshot's ruleno -> extension
 * rule_id map, built by identity lookup after compose (Lime v1.10.0
 * lime_snapshot_rule_by_id).  ruleno_to_ruleid[n] is the 1-based extension
 * rule_id for composed rule n, or 0 if composed rule n is a base-grammar
 * rule (or a silent extension rule).  Stable across the compose
 * renumbering that the old positional composed_ruleno - base_nrule
 * arithmetic could not survive.
 *
 * With per-session dialect selection each composed snapshot (the default
 * "all" grammar and one per named dialect) has its OWN ruleno map, owned
 * by the push-parse driver's dialect bundle.  parser_extension.c holds a
 * pointer to whichever map belongs to the snapshot the current backend is
 * parsing with; the driver swaps it via pg_grammar_ext_use_ruleno_map at
 * compose time (to populate a bundle's map) and at parse time (to select
 * the active bundle's map).  The array memory is allocated here (in
 * ext_context, which survives forks) so the driver need not; the pointer
 * is process-global but only ever read/written single-threaded per backend
 * (one parse at a time), and the arrays are immutable after their compose.
 */
static unsigned int *ruleno_to_ruleid = NULL;
static int	ruleno_to_ruleid_count = 0;

/*
 * pg_grammar_ext_build_ruleno_map
 *	  Build the composed snapshot's ruleno -> extension rule_id map by
 *	  IDENTITY, using Lime v1.10.0's lime_snapshot_rule_by_id.  Each
 *	  registered extension rule's canonical identity string
 *	  ("lhs ::= rhs1 rhs2 ...") is stable across the two-pass action-first
 *	  rule numbering and the base/composed renumbering, unlike the raw
 *	  ruleno.  We resolve each extension rule's composed ruleno ONCE here,
 *	  at compose time, so the host-reduce router can dispatch by ruleno
 *	  directly without the fragile positional composed_ruleno - base_nrule
 *	  arithmetic (which broke whenever composition renumbered rules).
 *
 *	  Returns true on success; false (with *errmsg_out set) if a registered
 *	  extension rule cannot be found in the composed snapshot by identity,
 *	  which would indicate the fragment did not compose as expected.
 */
static char *
ext_rule_identity(const ExtRule *r)
{
	StringInfoData id;

	initStringInfo(&id);
	appendStringInfoString(&id, r->lhs);
	appendStringInfoString(&id, " ::=");
	for (int i = 0; i < r->nrhs; i++)
	{
		appendStringInfoChar(&id, ' ');
		appendStringInfoString(&id, r->rhs[i]);
	}
	return id.data;
}

/*
 * pg_grammar_ext_reset_ruleno_map
 *	  Allocate a fresh composed-ruleno -> extension rule_id map for a
 *	  composed snapshot with `nrule` rules, make it the active map, and
 *	  return it so the caller (a dialect bundle) can retain ownership and
 *	  re-select it later via pg_grammar_ext_use_ruleno_map.  The push
 *	  driver populates it by identity immediately after (see below).
 */
unsigned int *
pg_grammar_ext_reset_ruleno_map(unsigned int nrule)
{
	MemoryContext old = MemoryContextSwitchTo(ext_context());

	ruleno_to_ruleid = palloc0(sizeof(unsigned int) * (nrule ? nrule : 1));
	ruleno_to_ruleid_count = (int) nrule;
	MemoryContextSwitchTo(old);
	return ruleno_to_ruleid;
}

/*
 * pg_grammar_ext_use_ruleno_map
 *	  Select which composed snapshot's ruleno -> rule_id map is active.
 *	  Called by the push-parse driver: once per bundle at compose time so
 *	  the map it just allocated (pg_grammar_ext_reset_ruleno_map) receives
 *	  the set_ruleno writes, and once per parse to point dispatch at the
 *	  dialect this backend selected.  `map` may be NULL (base-only, no
 *	  extension rules), in which case reduce dispatch always falls through
 *	  to the base grammar.
 */
void
pg_grammar_ext_use_ruleno_map(unsigned int *map, int count)
{
	ruleno_to_ruleid = map;
	ruleno_to_ruleid_count = map ? count : 0;
}

/*
 * pg_grammar_ext_foreach_reducible
 *	  Invoke cb(rule_id, identity, arg) for every registered extension rule
 *	  that carries a reduce callback, where `identity` is the rule's
 *	  canonical "lhs ::= rhs..." string (matching Lime's ruleIdentityString,
 *	  the key for lime_snapshot_rule_by_id).  Used by the push driver to
 *	  resolve each rule's composed ruleno by identity and record it via
 *	  pg_grammar_ext_set_ruleno.  `identity` is palloc'd in the caller's
 *	  context; the callback must copy anything it retains (the driver only
 *	  uses it transiently for the lookup).
 */
void
pg_grammar_ext_foreach_reducible(PgGrammarExtRuleCB cb, void *arg)
{
	for (unsigned int rid = 1; rid < g_next_rule_id; rid++)
	{
		ExtRule    *r = (rid < g_rule_table_size) ? g_rule_table[rid] : NULL;
		char	   *identity;

		if (r == NULL || r->reduce == NULL)
			continue;
		identity = ext_rule_identity(r);
		cb(rid, identity, arg);
		pfree(identity);
	}
}

/*
 * pg_grammar_ext_set_ruleno
 *	  Record that composed rule `ruleno` is extension rule `rule_id`.
 *	  Called by the push driver once per extension rule after resolving
 *	  its composed ruleno by identity.
 */
void
pg_grammar_ext_set_ruleno(int ruleno, unsigned int rule_id)
{
	if (ruleno >= 0 && ruleno < ruleno_to_ruleid_count)
		ruleno_to_ruleid[ruleno] = rule_id;
}

/*
 * pg_grammar_ext_reduce_by_ruleno
 *	  The host-reduce dispatcher's extension path, keyed on the COMPOSED
 *	  ruleno (resolved by identity above -- stable across compose
 *	  renumbering, unlike the old composed_ruleno - base_nrule arithmetic).
 *	  Returns true and dispatches to the rule's PgGrammarReduceFn if
 *	  `ruleno` is an extension rule; returns false if it is a base-grammar
 *	  rule (the caller then runs the base action).
 *
 *	  `extra_arg` is the core scanner (threaded as the host_reduce user
 *	  pointer), matching the base path's yyscanner.
 */
bool
pg_grammar_ext_reduce_by_ruleno(int ruleno,
								void *extra_arg,
								int nrhs,
								const void *const *rhs_values,
								const int *rhs_locs,
								void *lhs_out)
{
	unsigned int rule_id;

	if (ruleno < 0 || ruleno >= ruleno_to_ruleid_count)
		return false;			/* out of range: treat as base */

	rule_id = ruleno_to_ruleid[ruleno];
	if (rule_id == 0)
		return false;			/* base-grammar rule */

	pg_grammar_ext_dispatch_reduce(rule_id, extra_arg, nrhs,
								   rhs_values, rhs_locs, lhs_out);
	return true;
}

/*
 * pg_grammar_ext_pending_fragments
 *	  Expose the registered extension fragments to the push-parse driver
 *	  so it can build the merged grammar text.  Returns the fragment
 *	  count; *frags_out points at an array of NUL-terminated fragment
 *	  strings (owned by parser_extension.c; the caller must not free).
 */
int
pg_grammar_ext_pending_fragments(const char ***frags_out)
{
	static const char **frag_array = NULL;

	if (npending == 0)
	{
		*frags_out = NULL;
		return 0;
	}

	if (frag_array == NULL)
	{
		MemoryContext old = MemoryContextSwitchTo(ext_context());

		frag_array = palloc(sizeof(char *) * npending);
		MemoryContextSwitchTo(old);
	}
	for (int i = 0; i < npending; i++)
		frag_array[i] = pending[i].fragment;

	*frags_out = frag_array;
	return npending;
}

/*
 * pg_grammar_ext_dialect_count / pg_grammar_ext_dialect_name
 *	  Enumerate the DISTINCT dialect names among the registered
 *	  extensions, in first-registration order.  A dialect name is just the
 *	  registered extension's name (pg_grammar_ext_create(name, ...)); a
 *	  session picks one via the grammar_dialect GUC.  Two extensions that
 *	  share a name compose into the same dialect snapshot.
 */
int
pg_grammar_ext_dialect_count(void)
{
	int			n = 0;

	for (int i = 0; i < npending; i++)
	{
		bool		seen = false;

		for (int j = 0; j < i; j++)
			if (strcmp(pending[i].name, pending[j].name) == 0)
			{
				seen = true;
				break;
			}
		if (!seen)
			n++;
	}
	return n;
}

const char *
pg_grammar_ext_dialect_name(int idx)
{
	int			n = 0;

	for (int i = 0; i < npending; i++)
	{
		bool		seen = false;

		for (int j = 0; j < i; j++)
			if (strcmp(pending[i].name, pending[j].name) == 0)
			{
				seen = true;
				break;
			}
		if (seen)
			continue;
		if (n == idx)
			return pending[i].name;
		n++;
	}
	return NULL;
}

/*
 * pg_grammar_ext_dialect_registered
 *	  True if `name` matches a registered extension (dialect) name.
 */
bool
pg_grammar_ext_dialect_registered(const char *name)
{
	if (name == NULL)
		return false;
	for (int i = 0; i < npending; i++)
		if (strcmp(pending[i].name, name) == 0)
			return true;
	return false;
}

/*
 * pg_grammar_ext_dialect_fragments
 *	  Like pg_grammar_ext_pending_fragments, but restricted to the
 *	  fragments registered under dialect `name`.  Used by the push driver
 *	  to compose one isolated snapshot per named dialect (base SQL + only
 *	  that dialect's rules).  A NULL name selects EVERY fragment (the
 *	  default "all" grammar -- base SQL + every loaded extension), which is
 *	  identical to pg_grammar_ext_pending_fragments.  Returns the count;
 *	  *frags_out points at a freshly palloc'd array the caller may free
 *	  (its element strings are owned by parser_extension.c).
 */
int
pg_grammar_ext_dialect_fragments(const char *name, const char ***frags_out)
{
	const char **arr;
	int			n = 0;

	if (npending == 0)
	{
		*frags_out = NULL;
		return 0;
	}

	arr = palloc(sizeof(char *) * npending);
	for (int i = 0; i < npending; i++)
	{
		if (name != NULL && strcmp(pending[i].name, name) != 0)
			continue;
		arr[n++] = pending[i].fragment;
	}
	*frags_out = arr;
	return n;
}

/*
 * pg_grammar_ext_foreach_token
 *	  Invoke `cb(name, lexeme, category, cb_arg)` once for every token
 *	  registered by every loaded grammar extension.  Used by the push-parse
 *	  driver to build the scanner keyword map (resolving each extension
 *	  token's NAME to its external code in the composed snapshot via
 *	  lime_snapshot_token_code).  Tokens with no lexeme (purely internal
 *	  symbolic tokens) are skipped -- only keyword-shaped tokens with a
 *	  source lexeme are scanner-relevant.
 */
void
pg_grammar_ext_foreach_token(PgGrammarExtTokenCB cb, void *cb_arg)
{
	for (int i = 0; i < npending; i++)
	{
		PgGrammarExtension *ext = pending[i].ext;

		for (ExtToken *tok = ext->tokens; tok != NULL; tok = tok->next)
		{
			if (tok->lexeme == NULL || tok->lexeme[0] == '\0')
				continue;
			cb(tok->name, tok->lexeme, tok->category, cb_arg);
		}
	}
}
