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

#include <dlfcn.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "common/cryptohash.h"
#include "common/sha2.h"
#include "common/string.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "parser/parser_extension.h"
#include "parser/scanner.h"
#include "port.h"
#include "storage/fd.h"
#include "utils/elog.h"
#include "utils/memutils.h"

/*
 * Function-pointer slot raw_parser() dispatches through.  Defined in
 * parser.c; this TU writes to it after a successful subprocess
 * rebuild.  Declared with the same prototype as base_yyparse() so
 * the compiler catches a signature drift early.
 */
extern int	(*base_yyparse_fn) (core_yyscan_t yyscanner);

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
 *   shared_preload_libraries-loaded code).
 *
 * pipeline_run: once we've drained the pending list (success or
 *   failure) we don't try again; otherwise a per-parse retry loop
 *   could thrash on a broken cc invocation.
 *
 * pending_*: the registered extensions queued for build.  Stored as
 *   a parallel array of fragment text + provenance for diagnostics.
 *   Owned by parser_extension_context (a child of TopMemoryContext)
 *   so we survive memory-context resets between transactions.
 */
static bool parser_locked = false;
static bool pipeline_run = false;
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

 /* dlopen handle of the rebuilt parser, or NULL if no rebuild ran. */ static void *loaded_dl_handle = NULL;

/*
 * Phase 4 Track B: extension keyword map.
 *
 * Built at parser-rebuild time from the union of every PendingExt's
 * tokens.  Each entry pairs the lexeme (the lowercase source text the
 * scanner sees, e.g. "retrieve") with the token code assigned by the
 * rebuilt parser to the matching token name (e.g. K_QUEL_RETRIEVE).
 *
 * Lookup via pg_grammar_ext_keyword_hook() does a linear scan; this is
 * O(n) in the number of registered extension tokens.  Acceptable for
 * any realistic extension set; a future optimization can swap in a
 * perfect hash if extension counts climb into the hundreds.
 */
typedef struct ExtKeywordEntry
{
	const char *lexeme;			/* lowercase, NUL-terminated */
	int			token_code;		/* token code in the rebuilt parser */
} ExtKeywordEntry;

static ExtKeywordEntry *ext_keyword_map = NULL;
static int	ext_keyword_count = 0;

static int	pg_grammar_ext_keyword_lookup(const char *lower_lexeme);
static bool build_extension_keyword_map(void *handle,
										const char *so_path,
										char **errmsg_out);

/* Hook published to scan.c after the keyword map is built. */
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
static bool run_subprocess_pipeline(char **errmsg_out);
static bool sha256_concat_hex(const char *base, size_t blen,
							  PendingExt *frags, int nfrags,
							  char hex[PG_SHA256_DIGEST_STRING_LENGTH]);
static char *locate_base_gram_lime(char **errmsg_out);
static char *read_file_contents(const char *path, size_t *out_len,
								char **errmsg_out);
static bool write_concatenated_lime(const char *path,
									const char *base, size_t blen,
									PendingExt *frags, int nfrags,
									char **errmsg_out);
static bool ensure_cache_dir(const char *path, char **errmsg_out);
static bool run_program(const char *progname, const char *const *argv,
						char **errmsg_out);
static const char *resolve_cc(void);
static const char *resolve_lime_version(void);
static void resolve_server_includedir(char *out, size_t outlen);


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
	 * context; if the caller unregisters BEFORE lock_parser() drains the
	 * queue, those pointers become dangling.  We don't currently support that
	 * flow -- removing an extension after register() but before parse-time
	 * would mean tearing down the dlopen'd parser, which Track A doesn't do.
	 * Document the limitation rather than pretend to handle it.
	 */
	if (ext->registered && !pipeline_run)
		ereport(WARNING,
				(errmsg("grammar extension \"%s\" was unregistered "
						"before the parser rebuild ran; this leaves "
						"a dangling fragment in the rebuild queue",
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
 *	  Called by raw_parser() at the top of every parse.  The first
 *	  call drains the pending registration queue and runs the
 *	  subprocess rebuild; subsequent calls are O(1).
 *
 *	  On success, base_yyparse_fn is updated to point at the freshly
 *	  loaded parser.  On failure we ereport(ERROR) so the user sees
 *	  the build error rather than silently falling back to the
 *	  pre-extension parser, which would mask broken extensions.
 */
void
pg_grammar_ext_lock_parser(void)
{
	char	   *err_detail = NULL;

	if (parser_locked)
		return;
	parser_locked = true;

	if (npending == 0)
	{
		pipeline_run = true;
		return;
	}

	if (!run_subprocess_pipeline(&err_detail))
	{
		pipeline_run = true;
		ereport(ERROR,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("grammar extension build failed: %s",
						err_detail ? err_detail : "(no detail)")));
	}
	pipeline_run = true;
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
			appendStringInfoString(&buf, ". {\n");
			if (rule->nrhs > 0)
			{
				appendStringInfo(&buf,
								 "\tconst void *_pg_rhs[%d] = {",
								 rule->nrhs);
				for (int i = 0; i < rule->nrhs; i++)
					appendStringInfo(&buf, "%s(const void *) &%c",
									 i == 0 ? " " : ", ",
									 'B' + i);
				appendStringInfoString(&buf, " };\n");
				appendStringInfo(&buf, "\tint _pg_locs[%d] = {", rule->nrhs);
				for (int i = 0; i < rule->nrhs; i++)
					appendStringInfo(&buf, "%s@%c",
									 i == 0 ? " " : ", ",
									 'B' + i);
				appendStringInfoString(&buf, " };\n");
			}
			else
			{
				/*
				 * Empty RHS: pass NULL arrays.  C99 zero-length arrays are
				 * non-portable; using NULL avoids the warning and the
				 * trampoline checks nrhs==0 before deref.
				 */
				appendStringInfoString(&buf,
									   "\tconst void *const *_pg_rhs = NULL;\n"
									   "\tconst int *_pg_locs = NULL;\n");
			}
			appendStringInfo(&buf,
							 "\tpg_grammar_ext_dispatch_reduce(%uu, "
							 "(void *) yyscanner, %d, _pg_rhs, _pg_locs, "
							 "(void *) &A);\n"
							 "}\n",
							 rule->rule_id,
							 rule->nrhs);
		}
		else
			appendStringInfoString(&buf, ".\n");
	}

	/*
	 * External symbols the trampoline expects from the host binary.
	 * Forward-declared inside the fragment so the compiled .so resolves the
	 * call at dlopen time without needing PG headers (the .so is compiled
	 * with -I$PGINCLUDEDIR/server, but a forward decl makes the dependency
	 * explicit).
	 */
	appendStringInfoString(&buf,
						   "\n%include {\n"
						   "extern void pg_grammar_ext_dispatch_reduce("
						   "unsigned int rule_id, void *extra_arg, "
						   "int nrhs, const void *const *rhs_values, "
						   "const int *rhs_locs, void *lhs_out);\n"
						   "}\n");

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
 * sha256_concat_hex
 *	  Compute SHA-256 over (base bytes || every fragment's bytes) and
 *	  return the 64-character lowercase hex digest in `hex`.
 */
/*
 * sha256_concat_hex
 *	  Compute SHA-256 over (lime version || base bytes || every fragment's
 *	  bytes) and return the 64-character lowercase hex digest in `hex`.
 *
 *	  The lime version is mixed in so that a Lime upgrade automatically
 *	  invalidates the cache.  Without this, a server compiled against
 *	  Lime v0.6.4 could load a .so that v0.7.0 lime regenerated with
 *	  different ABI shape (magic, snapshot layout, etc.) and silently
 *	  miscompile.  We capture the version string at first call via
 *	  `lime -v` and feed its bytes into the hash.
 */
static bool
sha256_concat_hex(const char *base, size_t blen,
				  PendingExt *frags, int nfrags,
				  char hex[PG_SHA256_DIGEST_STRING_LENGTH])
{
	pg_cryptohash_ctx *ctx;
	uint8		digest[PG_SHA256_DIGEST_LENGTH];
	static const char hexchars[] = "0123456789abcdef";
	const char *lime_version_tag;

	lime_version_tag = resolve_lime_version();

	ctx = pg_cryptohash_create(PG_SHA256);
	if (ctx == NULL)
		return false;
	if (pg_cryptohash_init(ctx) < 0)
		goto fail;
	if (lime_version_tag != NULL
		&& pg_cryptohash_update(ctx,
								(const uint8 *) lime_version_tag,
								strlen(lime_version_tag)) < 0)
		goto fail;
	if (pg_cryptohash_update(ctx, (const uint8 *) base, blen) < 0)
		goto fail;
	for (int i = 0; i < nfrags; i++)
	{
		size_t		flen = strlen(frags[i].fragment);

		if (pg_cryptohash_update(ctx,
								 (const uint8 *) frags[i].fragment,
								 flen) < 0)
			goto fail;
	}
	if (pg_cryptohash_final(ctx, digest, sizeof(digest)) < 0)
		goto fail;
	pg_cryptohash_free(ctx);

	for (int i = 0; i < PG_SHA256_DIGEST_LENGTH; i++)
	{
		hex[2 * i] = hexchars[(digest[i] >> 4) & 0xF];
		hex[2 * i + 1] = hexchars[digest[i] & 0xF];
	}
	hex[PG_SHA256_DIGEST_STRING_LENGTH - 1] = '\0';
	return true;

fail:
	pg_cryptohash_free(ctx);
	return false;
}

/*
 * locate_base_gram_lime
 *	  Resolve the path to the install copy of gram.lime.  Falls back
 *	  to the in-tree source path if the install copy is missing
 *	  (development builds running uninstalled binaries).
 *
 *	  Returns a palloc'd path or NULL on failure (with *errmsg set).
 */
static char *
locate_base_gram_lime(char **errmsg_out)
{
	char		share_path[MAXPGPATH];
	char	   *candidate;
	struct stat st;

	if (my_exec_path[0] == '\0')
	{
		*errmsg_out = pstrdup("my_exec_path not initialised");
		return NULL;
	}

	get_share_path(my_exec_path, share_path);
	candidate = psprintf("%s/parser/gram.lime", share_path);
	if (stat(candidate, &st) == 0 && S_ISREG(st.st_mode))
		return candidate;

	*errmsg_out = psprintf("base gram.lime not found at \"%s\" "
						   "(install src/backend/parser/gram.lime to "
						   "$PGSHAREDIR/parser/gram.lime; the meson and "
						   "make install rules cover this)",
						   candidate);
	pfree(candidate);
	return NULL;
}

static char *
read_file_contents(const char *path, size_t *out_len, char **errmsg_out)
{
	FILE	   *f;
	long		size;
	char	   *buf;
	size_t		nread;

	f = AllocateFile(path, "rb");
	if (f == NULL)
	{
		*errmsg_out = psprintf("could not open \"%s\": %m", path);
		return NULL;
	}
	if (fseek(f, 0, SEEK_END) != 0)
	{
		FreeFile(f);
		*errmsg_out = psprintf("seek on \"%s\" failed: %m", path);
		return NULL;
	}
	size = ftell(f);
	if (size < 0)
	{
		FreeFile(f);
		*errmsg_out = psprintf("ftell on \"%s\" failed: %m", path);
		return NULL;
	}
	rewind(f);
	buf = palloc((size_t) size + 1);
	nread = fread(buf, 1, (size_t) size, f);
	FreeFile(f);
	if (nread != (size_t) size)
	{
		*errmsg_out = psprintf("short read on \"%s\" (%zu of %ld bytes)",
							   path, nread, size);
		pfree(buf);
		return NULL;
	}
	buf[size] = '\0';
	*out_len = (size_t) size;
	return buf;
}

static bool
write_concatenated_lime(const char *path,
						const char *base, size_t blen,
						PendingExt *frags, int nfrags,
						char **errmsg_out)
{
	FILE	   *f = AllocateFile(path, "wb");

	if (f == NULL)
	{
		*errmsg_out = psprintf("could not open \"%s\" for writing: %m", path);
		return false;
	}
	if (fwrite(base, 1, blen, f) != blen)
	{
		*errmsg_out = psprintf("short write on \"%s\": %m", path);
		FreeFile(f);
		return false;
	}
	if (fwrite("\n\n", 1, 2, f) != 2)
	{
		*errmsg_out = psprintf("short write on \"%s\": %m", path);
		FreeFile(f);
		return false;
	}
	for (int i = 0; i < nfrags; i++)
	{
		size_t		flen = strlen(frags[i].fragment);

		if (fwrite(frags[i].fragment, 1, flen, f) != flen)
		{
			*errmsg_out = psprintf("short write on \"%s\": %m", path);
			FreeFile(f);
			return false;
		}
	}
	if (FreeFile(f) != 0)
	{
		*errmsg_out = psprintf("close failed on \"%s\": %m", path);
		return false;
	}
	return true;
}

static bool
ensure_cache_dir(const char *path, char **errmsg_out)
{
	struct stat st;

	if (stat(path, &st) == 0)
	{
		if (!S_ISDIR(st.st_mode))
		{
			*errmsg_out = psprintf("\"%s\" exists and is not a directory", path);
			return false;
		}
		return true;
	}
	if (MakePGDirectory(path) != 0)
	{
		*errmsg_out = psprintf("could not create directory \"%s\": %m", path);
		return false;
	}
	/* Tighten perms: this dir holds compiled .so binaries. */
	if (chmod(path, S_IRWXU) != 0)
	{
		*errmsg_out = psprintf("could not chmod \"%s\": %m", path);
		return false;
	}
	return true;
}

/*
 * run_program
 *	  fork+execvp() a helper, wait for completion, return success.
 *	  Stderr is captured into a pipe and folded into *errmsg on
 *	  failure so the user sees what the subprocess actually said.
 *	  argv[] must be NULL-terminated.
 */
static bool
run_program(const char *progname, const char *const *argv, char **errmsg_out)
{
	int			pipefd[2];
	pid_t		pid;
	int			status;
	StringInfoData errbuf;

	if (pipe(pipefd) != 0)
	{
		*errmsg_out = psprintf("pipe() for %s failed: %m", progname);
		return false;
	}

	pid = fork();
	if (pid < 0)
	{
		close(pipefd[0]);
		close(pipefd[1]);
		*errmsg_out = psprintf("fork() for %s failed: %m", progname);
		return false;
	}
	if (pid == 0)
	{
		/* Child.  Redirect stderr (and stdout) to the pipe. */
		close(pipefd[0]);
		if (dup2(pipefd[1], STDOUT_FILENO) < 0 ||
			dup2(pipefd[1], STDERR_FILENO) < 0)
			_exit(127);
		close(pipefd[1]);
		execvp(progname, (char *const *) argv);
		_exit(127);
	}

	close(pipefd[1]);
	initStringInfo(&errbuf);
	for (;;)
	{
		char		chunk[1024];
		ssize_t		n = read(pipefd[0], chunk, sizeof(chunk));

		if (n < 0)
		{
			if (errno == EINTR)
				continue;
			break;
		}
		if (n == 0)
			break;
		appendBinaryStringInfo(&errbuf, chunk, (int) n);
		if (errbuf.len > 64 * 1024)
			break;
	}
	close(pipefd[0]);

	while (waitpid(pid, &status, 0) < 0)
	{
		if (errno != EINTR)
		{
			pfree(errbuf.data);
			*errmsg_out = psprintf("waitpid() for %s failed: %m", progname);
			return false;
		}
	}

	if (WIFEXITED(status) && WEXITSTATUS(status) == 0)
	{
		pfree(errbuf.data);
		return true;
	}

	if (WIFEXITED(status))
		*errmsg_out = psprintf("%s exited with status %d: %s",
							   progname, WEXITSTATUS(status),
							   errbuf.data);
	else if (WIFSIGNALED(status))
		*errmsg_out = psprintf("%s killed by signal %d: %s",
							   progname, WTERMSIG(status), errbuf.data);
	else
		*errmsg_out = psprintf("%s exited abnormally (status=0x%x): %s",
							   progname, status, errbuf.data);
	pfree(errbuf.data);
	return false;
}

static const char *
resolve_cc(void)
{
	const char *cc = getenv("CC");

	if (cc != NULL && cc[0] != '\0')
		return cc;
	return "cc";
}

/*
 * resolve_lime_version
 *	  Return a stable string identifying the Lime binary in use.
 *	  Used as a salt for the parser-cache SHA256 so that upgrading
 *	  Lime (which can change the ABI of generated parsers, e.g. the
 *	  v0.6.4 -> v0.7.0 ABI cleanup that added LIME_TABLES_MAGIC)
 *	  automatically invalidates every cached .so.
 *
 *	  Captured once per backend, lazily, via `lime -v`.  Returns
 *	  the empty string if the lime binary is missing or doesn't
 *	  produce a recognisable version line; in that case the cache
 *	  is still keyed on base+fragment bytes (the pre-this-feature
 *	  behaviour) but loses the upgrade-invalidates-cache property.
 */
static const char *
resolve_lime_version(void)
{
	static char version_buf[64];
	static bool version_resolved = false;
	FILE	   *pipe;

	if (version_resolved)
		return version_buf;
	version_resolved = true;

	/*
	 * Capture lime's version output.  popen avoids dragging in the project's
	 * run_program helper for what's a one-shot read at first-use;
	 * OpenPipeStream isn't suitable because we want a tight read with a fixed
	 * buffer.
	 */
	pipe = popen("lime -v 2>/dev/null", "r");
	if (pipe == NULL)
	{
		version_buf[0] = '\0';
		return version_buf;
	}

	if (fgets(version_buf, sizeof(version_buf), pipe) == NULL)
		version_buf[0] = '\0';
	else
	{
		/* Strip trailing newline. */
		size_t		n = strlen(version_buf);

		while (n > 0 && (version_buf[n - 1] == '\n'
						 || version_buf[n - 1] == '\r'))
			version_buf[--n] = '\0';
	}
	pclose(pipe);
	return version_buf;
}

static void
resolve_server_includedir(char *out, size_t outlen)
{
	get_includeserver_path(my_exec_path, out);
	if (out[0] == '\0')
		strlcpy(out, "/usr/local/pgsql/include/server", outlen);
}

/*
 * run_subprocess_pipeline
 *	  Drain the pending list, build (or reuse cached) extended parser
 *	  .so, dlopen, and install base_yyparse into the function-pointer
 *	  slot parser.c dispatches through.
 */
static bool
run_subprocess_pipeline(char **errmsg_out)
{
	char	   *base_path;
	char	   *base_text;
	size_t		base_len;
	char		hex[PG_SHA256_DIGEST_STRING_LENGTH];
	char		cache_dir[MAXPGPATH];
	char		so_path[MAXPGPATH];
	char		c_path[MAXPGPATH];
	char		lime_path[MAXPGPATH];
	char		include_dir[MAXPGPATH];
	void	   *handle;
	int			(*new_yyparse) (core_yyscan_t);
	struct stat st;
	MemoryContext old;

	if (DataDir == NULL || DataDir[0] == '\0')
	{
		*errmsg_out = pstrdup("DataDir not set; "
							  "grammar extensions require an initialised cluster");
		return false;
	}

	base_path = locate_base_gram_lime(errmsg_out);
	if (base_path == NULL)
		return false;

	old = MemoryContextSwitchTo(ext_context());
	base_text = read_file_contents(base_path, &base_len, errmsg_out);
	if (base_text == NULL)
	{
		MemoryContextSwitchTo(old);
		return false;
	}

	if (!sha256_concat_hex(base_text, base_len, pending, npending, hex))
	{
		*errmsg_out = pstrdup("SHA-256 hashing failed");
		MemoryContextSwitchTo(old);
		return false;
	}

	snprintf(cache_dir, sizeof(cache_dir), "%s/pg_parser_cache", DataDir);
	if (!ensure_cache_dir(cache_dir, errmsg_out))
	{
		MemoryContextSwitchTo(old);
		return false;
	}
	snprintf(so_path, sizeof(so_path), "%s/%s.so", cache_dir, hex);
	snprintf(c_path, sizeof(c_path), "%s/%s.c", cache_dir, hex);
	snprintf(lime_path, sizeof(lime_path), "%s/%s.lime", cache_dir, hex);

	/*
	 * Cache hit?  Skip the rebuild, reuse the existing .so.
	 */
	if (stat(so_path, &st) == 0 && S_ISREG(st.st_mode))
	{
		ereport(LOG,
				(errmsg("grammar extension cache hit: %s", so_path)));
		goto dlopen_step;
	}

	/* Cache miss: emit fresh .lime, compile, link. */
	if (!write_concatenated_lime(lime_path, base_text, base_len,
								 pending, npending, errmsg_out))
	{
		MemoryContextSwitchTo(old);
		return false;
	}

	{
		char		dflag[MAXPGPATH + 4];
		const char *lime_argv[] = {
			"lime",
			"-q",
			dflag,
			lime_path,
			NULL
		};

		snprintf(dflag, sizeof(dflag), "-d%s", cache_dir);
		ereport(LOG,
				(errmsg("running lime to rebuild parser for %d extension(s)",
						npending)));
		if (!run_program("lime", lime_argv, errmsg_out))
		{
			MemoryContextSwitchTo(old);
			return false;
		}
	}

	/*
	 * Lime emits <basename>.c and <basename>.h into -d <cache_dir>. For our
	 * hex-named input the outputs are <hex>.c / <hex>.h.
	 */
	if (stat(c_path, &st) != 0)
	{
		*errmsg_out = psprintf("lime did not produce \"%s\"", c_path);
		MemoryContextSwitchTo(old);
		return false;
	}

	/*
	 * gramparse.h's prologue does `#include "gram.h"` verbatim.  In the
	 * runtime-rebuild context the regenerated header is named <hex>.h, so we
	 * drop a one-line `gram.h` shim into the cache dir that redirects.  The
	 * cc invocation lists -I<cache_dir> first so this shim wins over any
	 * other gram.h on the include path.
	 */
	{
		char		shim_path[MAXPGPATH];
		FILE	   *shim;

		snprintf(shim_path, sizeof(shim_path), "%s/gram.h", cache_dir);
		shim = AllocateFile(shim_path, "wb");
		if (shim == NULL)
		{
			*errmsg_out = psprintf("could not write \"%s\": %m", shim_path);
			MemoryContextSwitchTo(old);
			return false;
		}
		fprintf(shim,
				"/* Auto-generated by parser_extension.c at runtime. */\n"
				"#include \"%s.h\"\n",
				hex);
		if (FreeFile(shim) != 0)
		{
			*errmsg_out = psprintf("close failed on \"%s\": %m", shim_path);
			MemoryContextSwitchTo(old);
			return false;
		}
	}

	resolve_server_includedir(include_dir, sizeof(include_dir));

	{
		const char *cc = resolve_cc();
		char		inc_parser[MAXPGPATH];
		const char *cc_argv[] = {
			cc,
			"-shared",
			"-fPIC",
			"-O2",
			"-Wno-unused-variable",
			"-Wno-missing-prototypes",

			/*
			 * -Wl,-Bsymbolic forces the .so to bind its own `base_yyparse`,
			 * `base_yy`, `base_yyLoc`, etc. to its INTERNAL copies rather
			 * than the global symbol table. The host postgres binary exports
			 * identically-named functions (the static gram.c-derived parser)
			 * and the default ELF binding rules would resolve calls inside
			 * the .so to the host's symbols, which are the UNEXTENDED grammar
			 * that doesn't know about extension-added tokens / rules. Without
			 * -Bsymbolic the rebuilt parser silently runs the static grammar
			 * and rejects extension keywords with a syntax error even though
			 * the .so's tables accept them.
			 *
			 * Lime upstream applied the same fix to its snapshot_create.c in
			 * v0.3.3 (commit 491e02a) for the same bug class.
			 */
			"-Wl,-Bsymbolic",

			/*
			 * Feature-test macros mirroring meson.build's cppflags so the
			 * runtime-rebuilt parser .so compiles cleanly under -std=c11 on
			 * glibc / FreeBSD / illumos / Darwin. Without these, headers like
			 * <pthread.h> hide POSIX 2008 symbols (clock_gettime, strdup,
			 * pthread_*, etc.) the generated parser may reach via PG's
			 * includes. Nix's gcc wrapper auto-defines them; system gcc does
			 * not.
			 */
			"-D_GNU_SOURCE",
			"-D_POSIX_C_SOURCE=200809L",
			"-D__EXTENSIONS__",
			"-D_DARWIN_C_SOURCE",
			"-I", cache_dir,
			"-I", include_dir,
			"-I", inc_parser,
			c_path,
			"-o", so_path,
			NULL
		};

		snprintf(inc_parser, sizeof(inc_parser), "%s/parser", include_dir);
		ereport(LOG,
				(errmsg("compiling extended parser: %s -> %s",
						c_path, so_path)));
		if (!run_program(cc, cc_argv, errmsg_out))
		{
			MemoryContextSwitchTo(old);
			return false;
		}
	}

dlopen_step:
	MemoryContextSwitchTo(old);

	/*
	 * dlopen with RTLD_NOW so undefined symbols (palloc, makeNode,
	 * base_yylex, ...) are resolved against the main executable's exported
	 * symbol table immediately; we want compile-time-style link errors at
	 * load time, not at first parse.  RTLD_LOCAL keeps the .so's
	 * parser_init/base_yyparse from leaking into the global namespace and
	 * clobbering the in-binary versions.
	 */
	handle = dlopen(so_path, RTLD_NOW | RTLD_LOCAL);
	if (handle == NULL)
	{
		const char *e = dlerror();

		*errmsg_out = psprintf("dlopen(\"%s\") failed: %s",
							   so_path, e ? e : "(unknown)");
		return false;
	}

	new_yyparse = (int (*) (core_yyscan_t)) dlsym(handle, "base_yyparse");
	if (new_yyparse == NULL)
	{
		const char *e = dlerror();

		*errmsg_out = psprintf("dlsym(\"base_yyparse\") in \"%s\" failed: %s",
							   so_path, e ? e : "(symbol missing)");
		dlclose(handle);
		return false;
	}

	/*
	 * Phase 4 Track B: resolve registered tokens to their numeric codes in
	 * the rebuilt parser using the .so's exported base_yyTokenName(int code)
	 * symbol.  Each PendingExt's tokens are scanned and the per-extension
	 * lexeme -> code map is appended to the global ext_keyword_map.  When
	 * done, we publish pg_grammar_ext_keyword_hook so scan.c starts
	 * consulting the map for IDENT-shaped lookups that miss the base
	 * ScanKeywords table.
	 */
	if (!build_extension_keyword_map(handle, so_path, errmsg_out))
	{
		dlclose(handle);
		return false;
	}

	/*
	 * Install the new parser.  From this point forward raw_parser()
	 * dispatches through it.  We keep the dlopen handle alive until
	 * postmaster shutdown -- there's no clean way to swap it out mid-flight
	 * without a coordinated quiesce.
	 */
	loaded_dl_handle = handle;
	base_yyparse_fn = new_yyparse;

	ereport(LOG,
			(errmsg("loaded extended parser from %s", so_path)));

	return true;
}

/*
 * pg_grammar_ext_keyword_lookup
 *	  Linear-scan lookup over the extension keyword map.  Called by
 *	  scan.c via pg_grammar_ext_keyword_hook for IDENT-shaped tokens
 *	  that miss the base ScanKeywordLookup perfect hash.
 *
 *	  Returns the rebuilt-parser token code on a match, -1 on miss.
 *
 *	  The lexeme passed in is already lowercase (downcase_truncate_-
 *	  identifier output); we compare via plain strcmp.
 */
static int
pg_grammar_ext_keyword_lookup(const char *lower_lexeme)
{
	if (ext_keyword_map == NULL || ext_keyword_count == 0)
		return -1;

	for (int i = 0; i < ext_keyword_count; i++)
	{
		if (strcmp(ext_keyword_map[i].lexeme, lower_lexeme) == 0)
			return ext_keyword_map[i].token_code;
	}
	return -1;
}

/*
 * build_extension_keyword_map
 *	  Resolve each PendingExt's tokens to numeric codes in the rebuilt
 *	  .so, build the static ext_keyword_map, and publish the keyword
 *	  hook to scan.c.
 *
 *	  The rebuilt .so exports `base_yyTokenName(int code)` -- Lime's
 *	  built-in token-name accessor.  We iterate code 1..N looking for
 *	  each registered token name and record the (lexeme, code) pair.
 *
 *	  N is bounded above by the static parser's token count plus the
 *	  sum of all extensions' token counts; a few hundred at most for
 *	  typical extension loads.  The walk is O(M*N) where M is the
 *	  number of registered extension tokens; acceptable at startup.
 *
 *	  Returns true on success.  On failure, sets *errmsg_out (palloc'd
 *	  in the caller's context) and returns false; pipeline tears down.
 */
static bool
build_extension_keyword_map(void *handle, const char *so_path,
							char **errmsg_out)
{
	FILE	   *h_file;
	char		header_path[MAXPGPATH];
	size_t		len;
	char	   *line = NULL;
	size_t		line_cap = 0;
	ssize_t		nread;
	MemoryContext old;
	int			total_tokens = 0;
	int			next_idx = 0;

	/*
	 * Derive the header path from so_path: <cache>/<hex>.so ->
	 * <cache>/<hex>.h.  The header was emitted by lime alongside the .c
	 * source and contains the externally-visible #define for every token used
	 * by the rebuilt parser.
	 */
	len = strlen(so_path);
	if (len < 3 || strcmp(so_path + len - 3, ".so") != 0)
	{
		*errmsg_out = psprintf("unexpected so_path shape: %s", so_path);
		return false;
	}
	snprintf(header_path, sizeof(header_path), "%.*s.h",
			 (int) (len - 3), so_path);

	h_file = AllocateFile(header_path, "r");
	if (h_file == NULL)
	{
		*errmsg_out = psprintf("could not open rebuilt parser header \"%s\": %m",
							   header_path);
		return false;
	}

	/* First pass: count extension tokens across all pending exts. */
	for (int i = 0; i < npending; i++)
	{
		PgGrammarExtension *ext = pending[i].ext;

		for (ExtToken *t = ext->tokens; t != NULL; t = t->next)
			total_tokens++;
	}

	if (total_tokens == 0)
	{
		/* No extension tokens; clear any stale map but leave hook NULL. */
		ext_keyword_map = NULL;
		ext_keyword_count = 0;
		FreeFile(h_file);
		return true;
	}

	old = MemoryContextSwitchTo(ext_context());
	ext_keyword_map = palloc0(sizeof(ExtKeywordEntry) * total_tokens);
	MemoryContextSwitchTo(old);

	/*
	 * Second pass: scan the header line by line.  Each `#define <name>
	 * <code>` declaration carries the externally-visible token code.  For
	 * every line, if the name matches a registered extension token, record
	 * (lexeme, code) in the map.  This is O((H + M*K)) where H is the header
	 * line count, M is the extension count, K is the per-extension token
	 * count -- well within budget at startup.
	 */
	while ((nread = getline(&line, &line_cap, h_file)) != -1)
	{
		char	   *p,
				   *name_start,
				   *name_end;
		int			code;

		if (nread < 9)
			continue;
		if (strncmp(line, "#define ", 8) != 0)
			continue;

		p = line + 8;
		while (*p == ' ' || *p == '\t')
			p++;
		name_start = p;
		while (*p && *p != ' ' && *p != '\t')
			p++;
		name_end = p;
		if (name_start == name_end || *p == '\0')
			continue;

		while (*p == ' ' || *p == '\t')
			p++;
		if (*p < '0' || *p > '9')
			continue;
		code = atoi(p);
		if (code <= 0)
			continue;

		/*
		 * Match against every registered extension token.  Linear across
		 * pending; bounded by total_tokens which is small.
		 */
		for (int i = 0; i < npending; i++)
		{
			PgGrammarExtension *ext = pending[i].ext;

			for (ExtToken *t = ext->tokens; t != NULL; t = t->next)
			{
				size_t		tn_len = strlen(t->name);

				if ((size_t) (name_end - name_start) != tn_len)
					continue;
				if (memcmp(name_start, t->name, tn_len) != 0)
					continue;

				old = MemoryContextSwitchTo(ext_context());
				ext_keyword_map[next_idx].lexeme =
					pstrdup(t->lexeme ? t->lexeme : "");
				ext_keyword_map[next_idx].token_code = code;
				MemoryContextSwitchTo(old);
				next_idx++;

				ereport(DEBUG1,
						(errmsg("grammar extension keyword: %s -> code %d (token %s)",
								t->lexeme ? t->lexeme : "(none)", code,
								t->name)));
				goto next_line;
			}
		}
next_line:
		;
	}

	if (line)
		free(line);
	FreeFile(h_file);

	ext_keyword_count = next_idx;

	/* Publish the hook so scan.c starts using the map. */
	pg_grammar_ext_keyword_hook = pg_grammar_ext_keyword_lookup;

	/*
	 * Suppress unused-warning on the dlopen handle parameter.  We may use it
	 * in the future to call back into the .so for fields we can't read from
	 * the header alone; for now the header is sufficient.
	 */
	(void) handle;

	ereport(LOG,
			(errmsg("grammar extension keyword map: %d entries published",
					ext_keyword_count)));

	return true;
}
