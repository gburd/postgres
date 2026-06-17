/*-------------------------------------------------------------------------
 *
 * parser_extension.h
 *	  Runtime grammar extension API.
 *
 * Allows PostgreSQL extensions to register additional tokens and
 * grammar rules at backend startup, before any backend has called
 * raw_parser().  Once registered, the extra grammar is applied to a
 * Lime-generated parser snapshot, either by spawning a subprocess
 * `lime` invocation (Track A subprocess fallback) or by patching
 * Lime's snapshot in place (Track B in-process dispatch).  Both
 * tracks share this exact same C contract; switching tracks changes
 * only the implementation of pg_grammar_ext_register().
 *
 * Calls are valid from `_PG_init()` or other shared_preload_libraries-
 * loaded code only, and must complete before the FIRST backend has
 * begun parsing user queries.  Calling pg_grammar_ext_register() after
 * the parser has been initialised returns false with err set.
 *
 * Phase 4 status:
 *
 *   - Track A subprocess pipeline:  LIVE.  pg_grammar_ext_register()
 *     queues the extension; on first parse, the rebuild pipeline
 *     forks lime + cc, dlopens the result, and installs it into the
 *     base_yyparse_fn function-pointer slot in parser.c.  Tested by
 *     dummy_grammar_ext smoke + grammar_ext_compose torture suite
 *     (22 subtests) + grammar_ext_overlap multi-extension test
 *     (42 subtests covering 5 simultaneously-loaded extensions).
 *
 *   - Track B Phase 1 scanner-table updates:  LIVE.  After the
 *     rebuild pipeline produces the .so, build_extension_keyword_map()
 *     reads the rebuilt .h to resolve each registered token name to
 *     its external token code, and publishes pg_grammar_ext_keyword_-
 *     hook to scan.c.  User input matching an extension's keyword
 *     lexeme is now emitted as the rebuilt-parser's token code
 *     (rather than IDENT), so registered rules are reachable from
 *     real psql input.  Verified by dummy_grammar_ext: input
 *     "dummy;" reduces via the user-supplied callback.
 *
 *   - Track B Phase 2 (in-process compose):  NOT YET STARTED.  The
 *     subprocess pipeline is the only path today; cold rebuild ~9s,
 *     warm cache ~11ms.  Lime's in-process API
 *     (lime_compile_grammar_in_process, v0.5.4+) would cut cold
 *     rebuild to ~5ms.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parser_extension.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PARSER_EXTENSION_H
#define PARSER_EXTENSION_H

#include "common/keywords.h"

/*
 * Keyword category for pg_grammar_ext_add_token().  Uses the same
 * UNRESERVED_KEYWORD / COL_NAME_KEYWORD / TYPE_FUNC_NAME_KEYWORD /
 * RESERVED_KEYWORD #define values from common/keywords.h.
 */
typedef int PgGrammarExtKeywordCategory;

/*
 * Associativity hints for pg_grammar_ext_set_precedence().  Mirrors
 * Bison/Lime's three associativity modes plus a NONE marker.
 */
typedef enum PgGrammarExtAssoc
{
	PG_GRAMMAR_ASSOC_NONE = 0,
	PG_GRAMMAR_ASSOC_LEFT,
	PG_GRAMMAR_ASSOC_RIGHT,
	PG_GRAMMAR_ASSOC_NONASSOC,
} PgGrammarExtAssoc;

/*
 * Reduce-action callback.  Mirrors Lime's LimeReduceFn typedef from
 * upstream src/extension.h (commit e2f84c6, hardened in 64b6854).
 *
 *   user_data   The reduce_user pointer set on pg_grammar_ext_add_rule().
 *   extra_arg   Lime's %extra_argument value.  For PG's backend grammar
 *               this is the core_yyscan_t.
 *   nrhs        Number of RHS symbols at this reduction.
 *   rhs_values  Array of nrhs pointers, one per RHS symbol.  Element
 *               i points at the value of the i-th RHS symbol, with the
 *               C type the extension declared for that symbol via
 *               pg_grammar_ext_add_type() (or, for terminals, the
 *               %token_type of the base grammar -- YYSTYPE for the
 *               backend SQL grammar).  Read with
 *               `*(const Type *) rhs_values[i]`.  Pointer storage is
 *               valid for the duration of the callback only; copy out
 *               anything the extension wants to retain.
 *   rhs_locs    Parallel array of source locations (one int per RHS
 *               symbol, byte offset into the original input).
 *   lhs_out     Destination for the reduced value.  The extension
 *               casts to the target's C type (matching the %type
 *               declared in pg_grammar_ext_add_type) and writes the
 *               result.  Memory MUST be palloc'd in
 *               CurrentMemoryContext if pointer-typed.
 */
typedef void (*PgGrammarReduceFn) (void *user_data,
								   void *extra_arg,
								   int nrhs,
								   const void *const *rhs_values,
								   const int *rhs_locs,
								   void *lhs_out);

/* Opaque handle.  Concrete type defined in parser_extension.c. */
typedef struct PgGrammarExtension PgGrammarExtension;

/*
 * pg_grammar_ext_create
 *	  Allocate a new (empty) grammar extension descriptor.
 *
 * `name` and `version` are copied; the caller can free them after
 * return.  The returned handle lives until pg_grammar_ext_unregister()
 * (or never -- shared_preload_libraries extensions typically don't
 * unregister).
 */
extern PgGrammarExtension *pg_grammar_ext_create(const char *name,
												 const char *version);

/*
 * pg_grammar_ext_add_token
 *	  Add a new terminal token.
 *
 * `name` is the symbolic identifier the extension's rules will use
 * (e.g. "JSONB_QUERY").  `lexeme` is the matching source text.  If a
 * token with the same name already exists and `lexeme` and `category`
 * match, the call is a no-op (extensions can re-declare canonical
 * tokens without conflict).  Mismatch is an error.
 *
 * `category` is one of UNRESERVED_KEYWORD, COL_NAME_KEYWORD,
 * TYPE_FUNC_NAME_KEYWORD, RESERVED_KEYWORD (from common/keywords.h).
 */
extern void pg_grammar_ext_add_token(PgGrammarExtension *ext,
									 const char *name,
									 const char *lexeme,
									 PgGrammarExtKeywordCategory category);

/*
 * pg_grammar_ext_add_type
 *	  Declare a new non-terminal symbol with its C value type.
 *
 * Required for any non-terminal an extension's rules emit.  `datatype`
 * is the C type that this non-terminal's value carries -- e.g.
 * "Node *" for a parse-tree node, or "int" for an integer-valued
 * placeholder.
 */
extern void pg_grammar_ext_add_type(PgGrammarExtension *ext,
									const char *name,
									const char *datatype);

/*
 * pg_grammar_ext_add_rule
 *	  Add a new production rule.
 *
 * `lhs` is the non-terminal the rule reduces to (must have been
 * declared via pg_grammar_ext_add_type or already exist in the base
 * grammar).
 *
 * `rhs` is a NULL-terminated array of symbol names.  Each name must be
 * either an existing terminal/non-terminal, or one declared earlier on
 * this same extension via _add_token / _add_type.
 *
 * `reduce` is the callback invoked when the rule reduces.  May be
 * NULL for rules that reduce silently (no value produced).
 * `reduce_user` is an opaque pointer threaded into the callback.
 */
extern void pg_grammar_ext_add_rule(PgGrammarExtension *ext,
									const char *lhs,
									const char **rhs,
									PgGrammarReduceFn reduce,
									void *reduce_user);

/*
 * pg_grammar_ext_dispatch_reduce
 *	  Trampoline called from the rebuilt parser .so when a rule with a
 *	  registered PgGrammarReduceFn fires.  Looks up the rule by its
 *	  numeric id (assigned at register time) and forwards to the
 *	  user-supplied callback.
 *
 *	  This function is part of the implicit ABI between the host
 *	  postgres binary and the .so produced by the Phase 4 subprocess
 *	  pipeline.  It must be exported (postgres is linked with
 *	  --export-dynamic) so the dlopen'd .so can resolve it.
 *
 *	  Callers other than the rebuilt parser have no business invoking
 *	  this directly -- it's declared here only so the .so source
 *	  emitted by parser_extension.c gets a matching prototype when
 *	  compiled.
 */
extern void pg_grammar_ext_dispatch_reduce(unsigned int rule_id,
										   void *extra_arg,
										   int nrhs,
										   const void *const *rhs_values,
										   const int *rhs_locs,
										   void *lhs_out);

/*
 * pg_grammar_ext_resolve_reduce
 *	  Track B host-reduce routing for EXTENSION rules.  The push parser's
 *	  composed host-reduce dispatcher calls this with the extension rule's
 *	  composed-relative index (composed_ruleno - base_nrule); it maps that
 *	  to the registered PgGrammarReduceFn and invokes it.  Returns 0 on
 *	  success.  `extra_arg` is the core scanner.
 */
extern int pg_grammar_ext_resolve_reduce(int ext_rule_index,
										 void *extra_arg,
										 int nrhs,
										 const void *const *rhs_values,
										 const int *rhs_locs,
										 void *lhs_out);

/*
 * pg_grammar_ext_pending_fragments
 *	  Return the registered extension fragments (NUL-terminated .lime
 *	  text) for the push-parse driver to merge with the base grammar
 *	  source.  Returns the count; *frags_out points at an array the
 *	  caller must not free.
 */
extern int pg_grammar_ext_pending_fragments(const char ***frags_out);

/*
 * pg_grammar_ext_set_precedence
 *	  Set or override the precedence of a symbol.
 *
 * `level` is a non-negative integer; higher binds tighter.  `assoc`
 * controls associativity at that level.
 */
extern void pg_grammar_ext_set_precedence(PgGrammarExtension *ext,
										  const char *symbol,
										  int level,
										  PgGrammarExtAssoc assoc);

/*
 * pg_grammar_ext_register
 *	  Commit the accumulated extension to the parser.
 *
 * Triggers a parser rebuild (expensive; extensions should batch their
 * registrations into a single _PG_init call).  Returns true on success;
 * on failure, sets *err to a palloc'd string describing the problem
 * and returns false.  *err may be NULL on success.
 *
 * Once register() succeeds, the extension's tokens and rules are
 * available to subsequent raw_parser() calls in the same backend AND
 * all subsequent forks of this postmaster.
 */
extern bool pg_grammar_ext_register(PgGrammarExtension *ext, char **err);

/*
 * pg_grammar_ext_unregister
 *	  Roll back a registered extension.
 *
 * Triggers another parser rebuild (also expensive).  After return, the
 * handle is invalidated; callers must discard it.  Calling _unregister
 * on an unregistered or already-unregistered handle is a no-op.
 */
extern void pg_grammar_ext_unregister(PgGrammarExtension *ext);

/*
 * pg_grammar_ext_get_serialized_lime
 *	  Return the .lime-fragment text the last successful registration
 *	  step produced.  Returns NULL if register() hasn't been called or
 *	  failed before serializing.  The pointer is owned by the
 *	  extension handle and stays valid until pg_grammar_ext_unregister.
 *
 *	  Useful for tests and for the upcoming subprocess pipeline that
 *	  will concatenate this fragment with the base gram.lime, fork
 *	  `lime` to compile, and dlopen the result.  Today it lets the
 *	  dummy_grammar_ext smoke test verify the converter produces
 *	  parseable .lime syntax.
 */
extern const char *pg_grammar_ext_get_serialized_lime(const PgGrammarExtension *ext);

/*
 * pg_grammar_ext_lock_parser
 *	  Internal hook called the first time raw_parser() runs.  After
 *	  this point pg_grammar_ext_register() returns false.  Not for
 *	  extension consumption; declared here so the public translation
 *	  unit doesn't need a private header.
 */
extern void pg_grammar_ext_lock_parser(void);

/*
 * pg_grammar_ext_prewarm
 *	  Internal hook called by the postmaster after all
 *	  shared_preload_libraries have run _PG_init().  Composes the
 *	  registered grammar extensions into the active parser snapshot at
 *	  startup (before backends fork), so no session pays a first-query
 *	  compose cost.  No-op when no extension registered.
 */
extern void pg_grammar_ext_prewarm(void);

/*
 * Phase 4 Track B: scanner-keyword hook.
 *
 * If non-NULL, scan.c calls this function for every identifier-shaped
 * token AFTER a base ScanKeywordLookup miss, BEFORE classifying as
 * IDENT.  The argument is a palloc'd lowercased copy of the input
 * lexeme (NUL-terminated).  Returns the rebuilt-parser token code
 * (>= 0) for a registered extension keyword, or -1 for a miss.
 *
 * Set during the parser-rebuild pipeline once the rebuilt .so is
 * dlopen'd and registered tokens have had their codes resolved
 * (via base_yyTokenName).
 *
 * Lifecycle (intentional non-feature):
 *   The hook is published once at the first raw_parser() call after
 *   shared_preload_libraries-loaded extensions register their
 *   contributions, and stays installed until postmaster exit.
 *   There is NO SIGHUP-driven teardown / re-publish.  Reasons:
 *
 *     1. dlclose'ing the rebuilt .so while a backend has a
 *        ParseTree referencing strings allocated in that .so's
 *        text segment is unsafe.  The parse tree outlives the
 *        parse call (parse_analyze, planner, executor all see it).
 *
 *     2. The cache key is keyed on the SET of registered extensions,
 *        not their evolving state.  A SIGHUP that adds an extension
 *        would invalidate the key for every existing backend, but
 *        existing backends still hold the OLD .so.  Live-swapping
 *        without a coordinated quiesce would race.
 *
 *     3. shared_preload_libraries is the established PG idiom for
 *        load-at-startup-only extension code.  Grammar extensions
 *        slot in there cleanly.
 *
 *   To add or remove an extension: edit shared_preload_libraries,
 *   restart the postmaster.  This is documented behaviour, not a
 *   limitation.
 *
 * Hook ordering matters: the base perfect-hash ScanKeywordLookup
 * runs first.  Extensions cannot OVERRIDE existing SQL keywords --
 * a registered extension keyword whose lexeme matches a base SQL
 * keyword is silently shadowed.  Concrete impact: contrib/quel
 * cannot use lexemes like "range", "of", "is", "to", "by",
 * "replace" because those are base SQL keywords.  QUEL keywords
 * use prefixed lexemes ("into_quel", "delete_quel") to avoid the
 * collision.  A future API extension could add a per-extension
 * "shadow base SQL keyword" capability, but that's a Phase 2+
 * feature with significant grammar-conflict implications.
 */
typedef int (*PgGrammarExtKeywordHook) (const char *lower_lexeme);
extern PGDLLIMPORT PgGrammarExtKeywordHook pg_grammar_ext_keyword_hook;

#endif							/* PARSER_EXTENSION_H */
