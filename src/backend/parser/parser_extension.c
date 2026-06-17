/*-------------------------------------------------------------------------
 *
 * parser_extension.c
 *	  Runtime grammar extension API -- subprocess pipeline implementation
 *	  (Phase 4 Track A).
 *
 * The C contract in include/parser/parser_extension.h is the final
 * shape; extensions can compile against it today.  This translation
 * unit:
 *
 *   1. Accepts and validates all add_token / add_type / add_rule /
 *      set_precedence calls.  Stores them in a per-extension memory
 *      context owned by the handle.
 *   2. On register(), serializes the extension to a .lime-syntax
 *      fragment and queues it on a static "pending" list.  No
 *      subprocess work happens yet -- multiple extensions registered
 *      from different shared_preload_libraries are batched.
 *   3. The first call to pg_grammar_ext_lock_parser() (issued by
 *      raw_parser() at the top of the first parse) drains the pending
 *      list.  If non-empty, the build pipeline runs:
 *
 *        a. SHA-256(base gram.lime || all fragments) gives a hex
 *           digest used as both filename root and cache key.
 *        b. <DataDir>/pg_parser_cache/<hex>.so is the cached parser.
 *           If it exists and dlopens, we skip the rebuild.
 *        c. Otherwise: write the concatenated .lime to <hex>.lime,
 *           fork+exec the pinned `lime` to produce <hex>.c, fork+exec
 *           the host C compiler to produce <hex>.so, then dlopen.
 *        d. dlsym("base_yyparse") produces the new parser entry
 *           point; we install it into the function pointer parser.c
 *           dispatches through.  The static fallback (the in-binary
 *           base_yyparse) remains valid if no extensions ever
 *           register, with zero overhead beyond an indirect call.
 *
 *   Track B (in-process snapshot patching) shares the same C contract
 *   and replaces only this file's implementation.  Blocks on Lime
 *   upstream commits P0-1-wall-{1,2,3}; not addressed here.
 *
 * Cache-key invariants:
 *   - The base gram.lime bytes are hashed in.  A new server build
 *     that ships a different gram.lime invalidates every cached
 *     .so via the hash, no special-case logic.
 *   - Fragments are concatenated in *registration order*.  Two
 *     servers loading the same set of extensions in different orders
 *     produce different hashes.  That's deliberate: rule order can
 *     affect Lime's conflict resolution, so we don't pretend the
 *     order is irrelevant.
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
 * trampoline forwards to.  The .so produced by the subprocess pipeline
 * is rebuilt every time the set of registered extensions changes (the
 * cache key includes the fragments, which include the rule_ids), so a
 * .so loaded against this table is always paired with the rules that
 * were live when it was built.
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
static void record_composed_rule(unsigned int rule_id);


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
	 * Tokens.  ScanKeywordCategory becomes a comment for now; the %token
	 * directive itself doesn't carry category info -- that lives in the
	 * keyword lookup table in scan.c, which Track A does NOT modify (the
	 * rebuilt parser is invoked via dlopen but the scanner stays
	 * compiled-in).  Track B will need to update the keyword table at
	 * register-time.
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
			 * action body is empty here.  At parse time the push parser's
			 * host-reduce dispatcher routes this rule's reduce -- identified
			 * by its composed rule number -- to pg_grammar_ext_dispatch_reduce
			 * and on to the user's PgGrammarReduceFn.  Record the rule_id in
			 * append order so the dispatcher can map composed-ruleno ->
			 * rule_id (extension rules are appended after the base grammar's
			 * rules, in fragment/text order).
			 */
			record_composed_rule(rule->rule_id);
			appendStringInfoString(&buf, ".\n");
		}
		else
		{
			/* Silent rule: still occupies a composed rule slot. */
			record_composed_rule(0);
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
 * record_composed_rule
 *	  Append `rule_id` to the composed-rule map.  Called once per rule
 *	  emitted by serialize_extension(), in the order extension rules are
 *	  appended to the merged grammar text.  The Nth recorded entry is the
 *	  rule_id of composed rule (base_nrule + N); 0 marks a silent rule
 *	  (no reduce callback).  The push parser's host-reduce dispatcher uses
 *	  this to route an extension-rule reduce to its PgGrammarReduceFn.
 */
static unsigned int *composed_ruleid = NULL;
static int	composed_nrule = 0;
static int	composed_capacity = 0;

static void
record_composed_rule(unsigned int rule_id)
{
	MemoryContext old = MemoryContextSwitchTo(ext_context());

	if (composed_nrule == composed_capacity)
	{
		int			newcap = composed_capacity ? composed_capacity * 2 : 16;

		if (composed_ruleid == NULL)
			composed_ruleid = palloc0(sizeof(unsigned int) * newcap);
		else
			composed_ruleid = repalloc(composed_ruleid,
									   sizeof(unsigned int) * newcap);
		composed_capacity = newcap;
	}
	composed_ruleid[composed_nrule++] = rule_id;
	MemoryContextSwitchTo(old);
}

/*
 * pg_grammar_ext_resolve_reduce
 *	  Map an EXTENSION rule's composed-relative index (composed_ruleno -
 *	  base_nrule, i.e. the Nth appended rule) to its registered
 *	  PgGrammarReduceFn and invoke it.  Called from the push parser's
 *	  host-reduce dispatcher (parser_pushparse.c) for rulenos at or above
 *	  the base grammar's rule count.  Returns 0 on success.
 *
 *	  `extra_arg` is the core scanner (threaded as the host_reduce user
 *	  pointer), matching the base path's yyscanner.
 */
int
pg_grammar_ext_resolve_reduce(int ext_rule_index,
							  void *extra_arg,
							  int nrhs,
							  const void *const *rhs_values,
							  const int *rhs_locs,
							  void *lhs_out)
{
	unsigned int rule_id;

	if (ext_rule_index < 0 || ext_rule_index >= composed_nrule)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("grammar extension reduce: composed rule index %d "
						"out of range (have %d extension rules)",
						ext_rule_index, composed_nrule)));

	rule_id = composed_ruleid[ext_rule_index];
	if (rule_id == 0)
		return 0;				/* silent rule: nothing to dispatch */

	pg_grammar_ext_dispatch_reduce(rule_id, extra_arg, nrhs,
								   rhs_values, rhs_locs, lhs_out);
	return 0;
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
