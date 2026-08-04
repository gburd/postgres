/*-------------------------------------------------------------------------
 *
 * parser_extension.h
 *	  Runtime grammar extension API.
 *
 * Allows PostgreSQL extensions to register additional tokens and
 * grammar rules at backend startup, before any backend has called
 * raw_parser().  Once registered, the extra grammar is applied to a
 * Lime-generated parser snapshot, either by spawning a subprocess
 * `lime` invocation (historical Track A subprocess path, removed) or
 * by composing Lime's grammar snapshot in process (Track B in-process
 * dispatch -- the live implementation).  The C contract below is
 * independent of the implementation.
 *
 * Calls are valid from `_PG_init()` or other shared_preload_libraries-
 * loaded code only, and must complete before the FIRST backend has
 * begun parsing user queries.  Calling pg_grammar_ext_register() after
 * the parser has been initialised returns false with err set.
 *
 * Implementation status (Track B, in-process compose):
 *
 *   - Registration queues the extension's tokens, types, precedences,
 *     and rules.  At postmaster start, after all _PG_init()s run,
 *     pg_grammar_ext_prewarm() composes the base grammar source with
 *     every queued fragment and compiles the result to a runtime
 *     ParserSnapshot via lime_compile_grammar_in_process() -- no
 *     subprocess, no C compiler, no .so cache.  The composed snapshot
 *     is installed as the active push-parse grammar.
 *
 *   - Reduce dispatch: base rules run the generated base actions
 *     through the host-reduce wrapper (base_yyHostReduce); extension
 *     rules route to their registered PgGrammarReduceFn keyed by
 *     composed-ruleno - base_nrule.  rhs_values[i] is the i-th RHS
 *     symbol's value by value (see PgGrammarReduceFn below).
 *
 *   - Keyword override: each registered token's lexeme is published to
 *     scan.c via pg_grammar_ext_keyword_hook.  Non-colliding keywords
 *     fall back to IDENT where the parse state expects a name; keywords
 *     that collide with base SQL are resolved at parse time by the LR
 *     admissibility oracle (parse_context_token_admissible) plus, for
 *     genuinely ambiguous verbs, one token of lookahead.
 *
 *   - Tested by dummy_grammar_ext smoke, grammar_ext_compose (22
 *     subtests), grammar_ext_overlap (44 subtests, 5 simultaneously-
 *     loaded dialects), the quel demonstrator, and the upsert
 *     demonstrator.
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
 *   rhs_values  Array of nrhs symbol values in rule order (index 0 =
 *               leftmost).  Per the Lime host-reduce ABI (confirmed and
 *               regression-locked in Lime v1.7.1), each element IS the
 *               symbol's value BY VALUE -- a pointer-width payload --
 *               NOT a pointer to a slot holding the value.  Read it
 *               directly: for a %type {Type *} symbol the element is the
 *               Type * itself, so use `(Type *) rhs_values[i]`, never
 *               `*(const Type *) rhs_values[i]`.  For a terminal whose
 *               base %token_type is a union (the backend grammar's
 *               core_YYSTYPE { int ival; char *str; const char *keyword;
 *               ... }), the element is the union's pointer-width content
 *               (the active member's bits): a string terminal arrives as
 *               the char* directly -- `(const char *) rhs_values[i]` --
 *               do NOT reconstruct a `core_YYSTYPE *` and dereference.
 *               The values are valid for the duration of the callback
 *               only; copy out anything the extension wants to retain.
 *   rhs_locs    Parallel array of source locations (one int per RHS
 *               symbol, byte offset into the original input).
 *   lhs_out     Destination for the reduced value.  Write the value by
 *               value, symmetric with reading rhs_values:
 *               `*(Type *) lhs_out = v` (e.g. `*(Node **) lhs_out =
 *               node;`).  The extension casts to the target's C type
 *               (matching the %type declared in pg_grammar_ext_add_type).
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
 * pg_grammar_ext reduce dispatch (identity-based, Lime v1.10.0):
 *
 * The composed snapshot renumbers rules across the base/composed/extension
 * spaces, so an extension rule's composed ruleno is not a fixed offset from
 * the base rule count.  We therefore resolve each extension rule's composed
 * ruleno ONCE, by its stable canonical identity ("lhs ::= rhs...", the key
 * for lime_snapshot_rule_by_id), and dispatch by ruleno at parse time.
 *
 * Wiring (driven by the push parser after each compose):
 *   1. pg_grammar_ext_reset_ruleno_map(nrule)      -- size the map.
 *   2. pg_grammar_ext_foreach_reducible(cb, arg)   -- for each extension
 *      rule the driver resolves its ruleno via lime_snapshot_rule_by_id
 *      and calls pg_grammar_ext_set_ruleno(ruleno, rule_id).
 *   3. pg_grammar_ext_reduce_by_ruleno(...)         -- host-reduce path:
 *      returns true (and dispatches) for an extension ruleno, false for a
 *      base-grammar ruleno.
 */
typedef void (*PgGrammarExtRuleCB) (unsigned int rule_id,
									const char *identity, void *arg);

extern void pg_grammar_ext_reset_ruleno_map(unsigned int nrule);
extern void pg_grammar_ext_foreach_reducible(PgGrammarExtRuleCB cb, void *arg);
extern void pg_grammar_ext_set_ruleno(int ruleno, unsigned int rule_id);
extern bool pg_grammar_ext_reduce_by_ruleno(int ruleno,
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
extern int	pg_grammar_ext_pending_fragments(const char ***frags_out);

/*
 * Callback for pg_grammar_ext_foreach_token: receives one registered
 * extension token's symbolic name, source lexeme, and keyword category.
 */
typedef void (*PgGrammarExtTokenCB) (const char *name,
									 const char *lexeme,
									 PgGrammarExtKeywordCategory category,
									 void *cb_arg);

/*
 * pg_grammar_ext_foreach_token
 *	  Enumerate every registered extension token (name, lexeme,
 *	  category) so the scanner-keyword map can resolve each name to its
 *	  external code in the composed snapshot.  Tokens with no lexeme are
 *	  skipped.
 */
extern void pg_grammar_ext_foreach_token(PgGrammarExtTokenCB cb, void *cb_arg);

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
 * lexeme (NUL-terminated).  Returns the extension token code (>= 0) in
 * the composed snapshot for a registered extension keyword, or -1 for a
 * miss.
 *
 * Published after the in-process grammar compose (pg_grammar_ext_prewarm
 * at postmaster start), once each registered extension token's code has
 * been resolved against the composed snapshot via
 * lime_snapshot_token_code().
 *
 * Lifecycle and config reload (Option A -- load-at-start):
 *   Grammar extensions register from shared_preload_libraries _PG_init,
 *   which runs only at postmaster start.  The composed snapshot is built
 *   once there (pg_grammar_ext_prewarm) and inherited by every backend
 *   across fork.  A config reload (SIGHUP -- pg_ctl reload /
 *   pg_reload_conf() / kill -HUP) re-reads GUCs as usual; it does NOT
 *   recompose the grammar, and need not: the registered extension set is
 *   fixed for the postmaster's lifetime.
 *
 *   This is deliberate and matches every other shared_preload_libraries
 *   extension: the LIBRARIES cannot be hot-loaded into a running
 *   cluster, so the grammar set cannot change without a restart.  Not
 *   recomposing also keeps in-flight parse trees safe -- a RawStmt and
 *   its token strings outlive the parse call (parse_analyze, planner,
 *   executor all see them), so the snapshot they were parsed against
 *   must stay valid.
 *
 *   To add or remove a grammar extension: edit
 *   shared_preload_libraries, restart the postmaster.  (A future
 *   enhancement could let a GUC activate/deactivate an already-loaded
 *   dialect across a standard config reload, with a refcounted snapshot
 *   swap at the raw_parser boundary; not implemented.)
 *
 * Keyword override:
 *   An extension keyword resolves to its own token even when its lexeme
 *   collides with a base SQL keyword.  Non-colliding lexemes resolve
 *   straight through this hook; colliding lexemes (a word that is both a
 *   base and an extension keyword) are resolved at parse time by the
 *   admissibility oracle, which emits the extension token only in parse
 *   states where the base keyword is inadmissible (e.g. a QUEL verb at
 *   statement start).  contrib/quel therefore uses its real lexemes
 *   (range/of/is/to/into/by/replace); a verb that legitimately begins a
 *   statement in both grammars (DELETE) is left to multi-token
 *   fork-resolve.
 */
typedef int (*PgGrammarExtKeywordHook) (const char *lower_lexeme);
extern PGDLLIMPORT PgGrammarExtKeywordHook pg_grammar_ext_keyword_hook;

#endif							/* PARSER_EXTENSION_H */
