/*-------------------------------------------------------------------------
 *
 * quel.c
 *	  QUEL query language as a parser extension.
 *
 * QUEL was the query language of UC Berkeley's Ingres relational DBMS,
 * developed by Stonebraker et al. starting in 1973.  POSTGRES (the
 * project that became PostgreSQL) inherited a derivative called
 * Postquel, which PostgreSQL replaced with SQL in 1995 (PostgreSQL
 * 6.0).  This extension reintroduces a small but representative
 * subset of QUEL via the parser_extension.h API, demonstrating that
 * Lime's runtime grammar composition can host an entire alternative
 * query language alongside SQL in the same backend.
 *
 *
 * QUEL syntax (subset implemented here):
 *
 *	   range of e is emp
 *	   retrieve (e.name, e.salary) where e.dept = "shoe"
 *	   retrieve into expensive (e.name, e.salary) where e.salary > 50000
 *	   append to emp (name = "alice", salary = 1000, dept = "toy")
 *	   replace e (salary = e.salary * 1.1) where e.dept = "shoe"
 *	   delete e where e.salary < 1000
 *
 * This is enough to make a roundtrip test interesting: every QUEL
 * shape lifts to a PostgreSQL query plan via the reduce callbacks.
 *
 *
 * Track A scope (this file):
 *
 *	   - Register the QUEL keyword vocabulary with the parser via
 *	     pg_grammar_ext_add_token().
 *	   - Register six new statement-level rules off `stmt` with
 *	     reduce callbacks.  The rebuilt parser .so dispatches to the
 *	     callbacks via pg_grammar_ext_dispatch_reduce().
 *	   - Verify the rebuild pipeline runs end-to-end at postmaster
 *	     start and the cache key is stable across boots.
 *
 *
 * Track B Phase 1 status (LIVE):
 *
 *	   The scanner-keyword hook in scan.c (pg_grammar_ext_keyword_-
 *	   hook) recognises QUEL keyword lexemes and emits them as the
 *	   rebuilt parser's token codes.  User input "retrieve" reaches
 *	   the parser as K_QUEL_RETRIEVE (not IDENT).
 *
 *	   The QUEL grammar itself is incomplete: this file registers
 *	   bare-keyword rules (quel_retrieve_stmt ::= K_QUEL_RETRIEVE.)
 *	   only; the full RHS shapes -- parens, target lists, WHERE,
 *	   BY sort lists -- are deferred to QUEL Phase A grammar
 *	   expansion (.agent/notes/quel-full-implementation-plan.md).
 *	   So real QUEL queries parse to K_QUEL_RETRIEVE then fail at
 *	   the next token until the grammar is extended.
 *
 *
 * What this extension demonstrates regardless of Track A vs B:
 *
 *	   1. The full keyword vocabulary of an alternative query
 *	      language can be registered without conflicting with SQL.
 *	      QUEL's RETRIEVE / APPEND / REPLACE / DELETE / RANGE / OF
 *	      / IS / TO / WHERE / INTO / BY tokens have prefixes that
 *	      do not collide with SQL's reserved-word table.
 *
 *	   2. Cross-statement non-terminals (quel_stmt, quel_target_-
 *	      list, quel_expr, ...) compose cleanly with the base
 *	      grammar's `stmt` LHS without forcing the QUEL extension
 *	      to know about every existing PG statement type.
 *
 *	   3. Precedence directives for QUEL operators (=, >, <, AND,
 *	      OR, NOT) can be set without disturbing SQL's
 *	      precedence ladder, demonstrating the precedence-merge
 *	      behaviour of the underlying Lime composition.
 *
 *	   4. The cache-key story: the same QUEL extension on a
 *	      different host will produce the same SHA256-keyed .so;
 *	      reloading without grammar changes hits the cache.
 *
 *
 * Portions Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * contrib/quel/quel.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "common/keywords.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "parser/parser_extension.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/memutils.h"

#include "quel_grammar.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

PG_FUNCTION_INFO_V1(quel_extension_status);
PG_FUNCTION_INFO_V1(quel_serialized_lime);

/*
 * State captured at register() time so the introspection functions
 * can report the cache key and the serialized .lime fragment.
 */
static bool quel_registered = false;
static char *quel_status_msg = NULL;
static const char *quel_lime_text = NULL;

/*
 * QUEL keyword vocabulary.  Every keyword maps to a token name
 * prefixed K_QUEL_ to avoid collisions with PG's existing token
 * codes (which are all-caps but unprefixed: SELECT, INSERT, etc.).
 *
 * Lexemes deliberately AVOID base SQL keywords by using prefixed
 * forms ("q_range" not "range").  The Phase 1 scanner-keyword
 * hook in scan.c runs only on a base ScanKeywordLookup MISS, so
 * lexemes that match base SQL keywords (RANGE, OF, IS, TO, BY,
 * REPLACE) are silently shadowed and never reach our rules.  This
 * is documented in src/include/parser/parser_extension.h's
 * pg_grammar_ext_keyword_hook block.
 *
 * The full Berkeley QUEL vocabulary (RANGE OF e IS emp; APPEND TO r;
 * REPLACE r SET ...) requires either lexeme renaming (this file's
 * choice today) or a future API extension that lets an extension
 * shadow base SQL keywords.  The shadow path would be Phase 2+
 * work; today's QUEL uses q_range / q_of / q_is / q_to /
 * q_by / q_replace etc.
 */
typedef struct QuelToken
{
	const char *name;
	const char *lexeme;
} QuelToken;

static const QuelToken quel_tokens[] = {
	{"K_QUEL_RETRIEVE", "retrieve"},
	{"K_QUEL_REPLACE", "q_replace"},	/* base SQL: REPLACE */
	{"K_QUEL_APPEND", "append"},
	{"K_QUEL_DELETE_QUEL", "q_delete"}, /* base SQL: DELETE */
	{"K_QUEL_RANGE", "q_range"},	/* base SQL: RANGE */
	{"K_QUEL_OF", "q_of"},		/* base SQL: OF */
	{"K_QUEL_IS", "q_is"},		/* base SQL: IS */
	{"K_QUEL_TO", "q_to"},		/* base SQL: TO */
	{"K_QUEL_INTO_QUEL", "q_into"}, /* base SQL: INTO */
	{"K_QUEL_BY", "q_by"},		/* base SQL: BY */
};

/*
 * QUEL non-terminals.  `quel_stmt` is the entry point that
 * `stmt ::= quel_stmt` forwards through.  Sub-non-terminals match
 * QUEL's grammar shapes from the original Ingres documentation.
 *
 * For Track A's purposes the C type is `Node *`; the reduce
 * callbacks construct stub Node-wrapped result values that
 * downstream parse-analysis would convert into PG plan trees.
 */
typedef struct QuelType
{
	const char *name;
	const char *datatype;
} QuelType;

static const QuelType quel_types[] = {
	{"quel_stmt", "Node *"},
	{"quel_retrieve_stmt", "Node *"},
	{"quel_replace_stmt", "Node *"},
	{"quel_append_stmt", "Node *"},
	{"quel_delete_stmt", "Node *"},
	{"quel_range_stmt", "Node *"},

	/*
	 * QUEL Phase A grammar expansion: paren-wrapped attribute lists for
	 * RETRIEVE.  quel_attr_list is List *, quel_attr is Node * (a ColumnRef
	 * once builders extract it).
	 */
	{"quel_attr_list", "List *"},
	{"quel_attr", "Node *"},
};

/*
 * Reduce callback shared by all QUEL rules.  Logs which production
 * fired with NOTICE so the regression-test expected output captures
 * the dispatch sequence.  Returns NULL into the LHS slot; downstream
 * parse-analysis is a Track B follow-up.
 */
/*
 * Reduce callback shared by all QUEL rules.  Logs which production
 * fired with NOTICE so the regression-test expected output captures
 * the dispatch sequence.  For specific rules with side effects (e.g.
 * RANGE OF e IS r updates the session-scoped tuple-variable table),
 * routes to the matching builder in quel_grammar.c.
 *
 * Phase B (this commit's expansion): for the attribute-list and
 * statement-shape rules, each label dispatches to a builder that
 * constructs a real PostgreSQL parse-tree node and writes it into
 * lhs_out.  The LHS type per rule:
 *
 *   quel_attr        -> Node *   (ColumnRef)
 *   quel_attr_list   -> List *   (list of ColumnRef)
 *   quel_retrieve_stmt -> Node * (SelectStmt)
 *   quel_replace_stmt  -> Node * (UpdateStmt)
 *   quel_append_stmt   -> Node * (InsertStmt)
 *   quel_delete_stmt   -> Node * (DeleteStmt)
 *   quel_stmt          -> Node * (forwarded)
 *   stmt               -> Node * (forwarded; matches base grammar)
 *
 * The parse tree returned to raw_parser() is whatever the topmost
 * reduce produces.  Downstream parse_analyze / planner / executor /
 * EXPLAIN handle a QUEL-derived SelectStmt identically to a SQL-
 * derived SelectStmt -- this is the migration's "QUEL queries
 * produce identical results to equivalent SQL" milestone.
 */
static void
quel_reduce(void *user_data, void *extra_arg, int nrhs,
			const void *const *rhs_values, const int *rhs_locs,
			void *lhs_out)
{
	const char *label = (const char *) user_data;

	ereport(NOTICE,
			(errmsg("quel: %s reduced (nrhs=%d)",
					label ? label : "(?)", nrhs)));

	(void) extra_arg;

	/* Default: NULL.  Specific labels override below. */
	*(void **) lhs_out = NULL;

	if (label == NULL)
		return;

	/* RANGE has no parse tree -- updates state and returns NULL. */
	if (strcmp(label, "range of IDENT is IDENT") == 0)
	{
		quel_apply_range(rhs_values, rhs_locs, nrhs);
		return;
	}

	/* Attribute list builders: each produces a List * of ColumnRefs. */
	if (strcmp(label, "attr (single IDENT)") == 0
		|| strcmp(label, "attr (bare_label_keyword)") == 0)
	{
		*(Node **) lhs_out =
			quel_build_attr_simple(rhs_values, rhs_locs, nrhs);
		return;
	}
	if (strcmp(label, "attr (tuple_var.column)") == 0
		|| strcmp(label, "attr (tuple_var.bare_keyword)") == 0)
	{
		*(Node **) lhs_out =
			quel_build_attr_qualified(rhs_values, rhs_locs, nrhs);
		return;
	}
	if (strcmp(label, "attr_list (single)") == 0)
	{
		*(List **) lhs_out =
			quel_build_attr_list_single(rhs_values, rhs_locs, nrhs);
		return;
	}
	if (strcmp(label, "attr_list (cons)") == 0)
	{
		*(List **) lhs_out =
			quel_build_attr_list_cons(rhs_values, rhs_locs, nrhs);
		return;
	}

	/* RETRIEVE builders: each produces a SelectStmt *. */
	if (strcmp(label, "retrieve (attr_list)") == 0)
	{
		*(Node **) lhs_out =
			quel_build_retrieve_simple(rhs_values, rhs_locs, nrhs);
		return;
	}
	if (strcmp(label, "retrieve (attr_list) WHERE a_expr") == 0)
	{
		*(Node **) lhs_out =
			quel_build_retrieve_where(rhs_values, rhs_locs, nrhs);
		return;
	}
	if (strcmp(label, "retrieve (attr_list) BY sortby_list") == 0)
	{
		*(Node **) lhs_out =
			quel_build_retrieve_by(rhs_values, rhs_locs, nrhs);
		return;
	}
	if (strcmp(label,
			   "retrieve (attr_list) WHERE a_expr BY sortby_list") == 0)
	{
		*(Node **) lhs_out =
			quel_build_retrieve_where_by(rhs_values, rhs_locs, nrhs);
		return;
	}

	/* REPLACE builders: each produces an UpdateStmt *. */
	if (strcmp(label, "replace IDENT (set_clause_list)") == 0)
	{
		*(Node **) lhs_out =
			quel_build_replace_simple(rhs_values, rhs_locs, nrhs);
		return;
	}
	if (strcmp(label,
			   "replace IDENT (set_clause_list) WHERE a_expr") == 0)
	{
		*(Node **) lhs_out =
			quel_build_replace_where(rhs_values, rhs_locs, nrhs);
		return;
	}

	/* APPEND builder: produces an InsertStmt *. */
	if (strcmp(label, "append to IDENT (set_clause_list)") == 0)
	{
		*(Node **) lhs_out =
			quel_build_append_full(rhs_values, rhs_locs, nrhs);
		return;
	}

	/* DELETE builders: each produces a DeleteStmt *. */
	if (strcmp(label, "delete_quel IDENT") == 0)
	{
		*(Node **) lhs_out =
			quel_build_delete_simple(rhs_values, rhs_locs, nrhs);
		return;
	}
	if (strcmp(label, "delete_quel IDENT WHERE a_expr") == 0)
	{
		*(Node **) lhs_out =
			quel_build_delete_where(rhs_values, rhs_locs, nrhs);
		return;
	}

	/*
	 * Forwarder rules: stmt ::= quel_stmt and quel_stmt ::= ... pass through
	 * the child node unchanged.  These reduce ONE rhs symbol whose value
	 * already lives at rhs_values[0].
	 */
	if (nrhs == 1
		&& (strcmp(label, "stmt->quel_stmt") == 0
			|| strcmp(label, "quel_stmt->retrieve") == 0
			|| strcmp(label, "quel_stmt->replace") == 0
			|| strcmp(label, "quel_stmt->append") == 0
			|| strcmp(label, "quel_stmt->delete") == 0
			|| strcmp(label, "quel_stmt->range") == 0
			|| strcmp(label, "explainableStmt -> retrieve") == 0
			|| strcmp(label, "explainableStmt -> replace") == 0
			|| strcmp(label, "explainableStmt -> append") == 0
			|| strcmp(label, "explainableStmt -> delete") == 0))
	{
		*(Node **) lhs_out = *(Node **) rhs_values[0];
		return;
	}
}

/*
 * QUEL rule set.  Each rule reduces a QUEL statement form into a
 * `quel_stmt` -- the gateway non-terminal that `stmt ::= quel_stmt`
 * binds to the base SQL grammar's start symbol.
 *
 * RHS sketches (English):
 *
 *	 quel_retrieve_stmt   retrieve [ into_quel IDENT ] (...)
 *	                       [ where ... ]
 *	 quel_replace_stmt    replace IDENT (...) [ where ... ]
 *	 quel_append_stmt     append to IDENT (...)
 *	 quel_delete_stmt     delete_quel IDENT [ where ... ]
 *	 quel_range_stmt      range of IDENT is IDENT
 *
 * Detailed RHS would require reusing the base grammar's `expr`,
 * `qualified_name`, and `target_list` non-terminals; this initial
 * version keeps RHS sketches minimal so Lime can build the LALR
 * machine on the rebuilt grammar.  Refining QUEL's expression
 * grammar to fully share PG's `a_expr` is Track B work.
 *
 * rhs[] is sized 12 to fit the longest current rule (8-symbol
 * retrieve + NUL plus headroom).  Extending past 12 means widening
 * here AND in the trampoline-emit path in parser_extension.c which
 * encodes RHS values as letter labels A-Z (25-symbol hard limit).
 */
typedef struct QuelRule
{
	const char *lhs;
	const char *rhs[12];
	const char *label;
} QuelRule;

static const QuelRule quel_rules[] = {
	/*
	 * Forward stmt -> quel_stmt so the rebuilt parser's start symbol can
	 * reach QUEL productions.  Once Track B emits QUEL keywords from the
	 * scanner, real input tokens will land here.
	 */
	{"stmt", {"quel_stmt", NULL}, "stmt->quel_stmt"},

	/* QUEL statements bubble up to quel_stmt. */
	{"quel_stmt", {"quel_retrieve_stmt", NULL}, "quel_stmt->retrieve"},
	{"quel_stmt", {"quel_replace_stmt", NULL}, "quel_stmt->replace"},
	{"quel_stmt", {"quel_append_stmt", NULL}, "quel_stmt->append"},
	{"quel_stmt", {"quel_delete_stmt", NULL}, "quel_stmt->delete"},
	{"quel_stmt", {"quel_range_stmt", NULL}, "quel_stmt->range"},

	/*
	 * Make QUEL statements EXPLAIN-able by adding alternatives to the base
	 * grammar's explainableStmt non-terminal.  Berkeley QUEL has no EXPLAIN
	 * equivalent; this is a PG extension -- `EXPLAIN retrieve (...)` returns
	 * the same plan tree as the equivalent `EXPLAIN SELECT ...`.  RANGE has
	 * no plan (state- only) so we forward only the four DML forms.
	 */
	{"explainableStmt", {"quel_retrieve_stmt", NULL},
	"explainableStmt -> retrieve"},
	{"explainableStmt", {"quel_replace_stmt", NULL},
	"explainableStmt -> replace"},
	{"explainableStmt", {"quel_append_stmt", NULL},
	"explainableStmt -> append"},
	{"explainableStmt", {"quel_delete_stmt", NULL},
	"explainableStmt -> delete"},

	/*
	 * RETRIEVE forms.  Bare keyword (no target list) is the minimum-viable
	 * retrieve.  retrieve INTO names a destination relation.  retrieve (...)
	 * attaches a paren-wrapped attribute list -- this is QUEL Phase A grammar
	 * expansion.  retrieve (...) WHERE <a_expr> adds a SQL-style where clause
	 * that reuses the base grammar's a_expr non-terminal (handles
	 * comparisons, boolean logic, function calls, subqueries -- the full SQL
	 * expression surface).
	 */
	{"quel_retrieve_stmt", {"K_QUEL_RETRIEVE", NULL},
	"retrieve (bare)"},
	{"quel_retrieve_stmt",
		{"K_QUEL_RETRIEVE", "K_QUEL_INTO_QUEL", "IDENT", NULL},
	"retrieve into IDENT"},
	{"quel_retrieve_stmt",
		{"K_QUEL_RETRIEVE", "LPAREN", "quel_attr_list", "RPAREN", NULL},
	"retrieve (attr_list)"},
	{"quel_retrieve_stmt",
		{"K_QUEL_RETRIEVE", "LPAREN", "quel_attr_list", "RPAREN",
		"WHERE", "a_expr", NULL},
	"retrieve (attr_list) WHERE a_expr"},

	/*
	 * Berkeley QUEL: `retrieve (...) BY <sort_list>`.  Maps to SQL ORDER BY.
	 * Reuses the base grammar's sortby_list which handles `expr ASC/DESC
	 * NULLS FIRST/LAST` shapes.  The lexeme conflict with base SQL's BY meant
	 * we registered K_QUEL_BY with q_by lexeme; user types `retrieve (...)
	 * q_by e.salary`.
	 */
	{"quel_retrieve_stmt",
		{"K_QUEL_RETRIEVE", "LPAREN", "quel_attr_list", "RPAREN",
		"K_QUEL_BY", "sortby_list", NULL},
	"retrieve (attr_list) BY sortby_list"},
	{"quel_retrieve_stmt",
		{"K_QUEL_RETRIEVE", "LPAREN", "quel_attr_list", "RPAREN",
		"WHERE", "a_expr", "K_QUEL_BY", "sortby_list", NULL},
	"retrieve (attr_list) WHERE a_expr BY sortby_list"},

	/*
	 * Attribute list: tuple_var.attribute references, comma- separated.
	 * Lime's LALR(1) handles the left-recursion fine. For now each attr is a
	 * single IDENT (column name); a fuller Phase A would add
	 * tuple-var-qualified shapes (e.IDENT.IDENT).
	 *
	 * To accept SQL-keyword names in attr position (e.g. `retrieve (name,
	 * salary, dept)` where `name` is NAME_P), we add an alternative that uses
	 * the base grammar's bare_label_keyword non-terminal.  bare_label_keyword
	 * expands to any of the ~600 base SQL keywords whose ScanKeywordCategory
	 * permits use as a bare column label.  This is the same trick PG's gram.y
	 * uses for column references in target lists.
	 */
	{"quel_attr_list", {"quel_attr", NULL}, "attr_list (single)"},
	{"quel_attr_list",
		{"quel_attr_list", "COMMA", "quel_attr", NULL},
	"attr_list (cons)"},
	{"quel_attr", {"IDENT", NULL}, "attr (single IDENT)"},
	{"quel_attr", {"IDENT", "DOT", "IDENT", NULL},
	"attr (tuple_var.column)"},
	{"quel_attr", {"bare_label_keyword", NULL},
	"attr (bare_label_keyword)"},
	{"quel_attr", {"IDENT", "DOT", "bare_label_keyword", NULL},
	"attr (tuple_var.bare_keyword)"},

	/*
	 * REPLACE tuple_var (set_clause_list) [WHERE a_expr] -- Berkeley QUEL's
	 * UPDATE form.  Reuses base set_clause_list which handles `column = expr`
	 * pairs.
	 */
	{"quel_replace_stmt", {"K_QUEL_REPLACE", "IDENT", NULL},
	"replace IDENT"},
	{"quel_replace_stmt",
		{"K_QUEL_REPLACE", "IDENT", "LPAREN", "set_clause_list",
		"RPAREN", NULL},
	"replace IDENT (set_clause_list)"},
	{"quel_replace_stmt",
		{"K_QUEL_REPLACE", "IDENT", "LPAREN", "set_clause_list",
		"RPAREN", "WHERE", "a_expr", NULL},
	"replace IDENT (set_clause_list) WHERE a_expr"},

	/*
	 * APPEND TO IDENT (set_clause_list) -- Berkeley QUEL's INSERT form.  Uses
	 * set_clause_list because Berkeley QUEL's append syntax is `append to r
	 * (name = "alice", salary = 5000)`.
	 */
	{"quel_append_stmt",
		{"K_QUEL_APPEND", "K_QUEL_TO", "IDENT", NULL},
	"append to IDENT"},
	{"quel_append_stmt",
		{"K_QUEL_APPEND", "K_QUEL_TO", "IDENT", "LPAREN",
		"set_clause_list", "RPAREN", NULL},
	"append to IDENT (set_clause_list)"},

	/*
	 * DELETE_QUEL tuple_var [WHERE a_expr] -- Berkeley QUEL's DELETE form. We
	 * use K_QUEL_DELETE_QUEL (lexeme q_delete) to avoid scanner shadowing of
	 * base SQL DELETE.
	 */
	{"quel_delete_stmt", {"K_QUEL_DELETE_QUEL", "IDENT", NULL},
	"delete_quel IDENT"},
	{"quel_delete_stmt",
		{"K_QUEL_DELETE_QUEL", "IDENT", "WHERE", "a_expr", NULL},
	"delete_quel IDENT WHERE a_expr"},

	/*
	 * RANGE OF e IS r -- the QUEL tuple-variable binding.  No SQL equivalent;
	 * the RHS is fully QUEL-specific.
	 */
	{"quel_range_stmt",
		{"K_QUEL_RANGE", "K_QUEL_OF", "IDENT", "K_QUEL_IS", "IDENT", NULL},
	"range of IDENT is IDENT"},
};

/*
 * QUEL operator precedence.  These levels mirror the SQL ladder
 * but on QUEL-private operator tokens, so they don't perturb SQL's
 * own precedence resolution.  Set at register() time.  Track A's
 * subprocess pipeline serializes them as %left/%right/%nonassoc
 * directives that Lime applies during the rebuild.
 */
typedef struct QuelPrec
{
	const char *symbol;
	int			level;
	PgGrammarExtAssoc assoc;
} QuelPrec;

static const QuelPrec quel_precs[] = {
	/*
	 * Reserved levels start at 1000 to leave the 0-99 range for extensions
	 * that genuinely want to outrank SQL operators. These QUEL operators are
	 * all on tokens we declared above so Lime resolves them to the
	 * extension's symbol table.
	 */
	{"K_QUEL_RETRIEVE", 1000, PG_GRAMMAR_ASSOC_NONASSOC},
	{"K_QUEL_REPLACE", 1000, PG_GRAMMAR_ASSOC_NONASSOC},
	{"K_QUEL_APPEND", 1000, PG_GRAMMAR_ASSOC_NONASSOC},
	{"K_QUEL_DELETE_QUEL", 1000, PG_GRAMMAR_ASSOC_NONASSOC},
};

void
_PG_init(void)
{
	PgGrammarExtension *ext;
	char	   *err = NULL;
	bool		ok;

	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("quel must be loaded via shared_preload_libraries"),
				 errhint("Add quel to shared_preload_libraries in "
						 "postgresql.conf and restart the postmaster.")));

	ext = pg_grammar_ext_create("quel", "1.0");

	for (size_t i = 0; i < lengthof(quel_tokens); i++)
	{
		const QuelToken *t = &quel_tokens[i];

		pg_grammar_ext_add_token(ext, t->name, t->lexeme,
								 UNRESERVED_KEYWORD);
	}

	for (size_t i = 0; i < lengthof(quel_types); i++)
	{
		const QuelType *t = &quel_types[i];

		pg_grammar_ext_add_type(ext, t->name, t->datatype);
	}

	for (size_t i = 0; i < lengthof(quel_precs); i++)
	{
		const QuelPrec *p = &quel_precs[i];

		pg_grammar_ext_set_precedence(ext, p->symbol, p->level, p->assoc);
	}

	for (size_t i = 0; i < lengthof(quel_rules); i++)
	{
		const QuelRule *r = &quel_rules[i];

		pg_grammar_ext_add_rule(ext, r->lhs, (const char **) r->rhs,
								quel_reduce, (void *) r->label);
	}

	ok = pg_grammar_ext_register(ext, &err);
	if (ok)
	{
		MemoryContext oldctx;

		quel_registered = true;
		quel_lime_text = pg_grammar_ext_get_serialized_lime(ext);

		oldctx = MemoryContextSwitchTo(TopMemoryContext);
		quel_status_msg = psprintf(
								   "quel registered: %zu tokens, %zu types, %zu rules, %zu prec; "
								   "reduce callbacks wired via pg_grammar_ext_dispatch_reduce; "
								   "scanner-keyword hook (Track B Phase 1) live; Phase B "
								   "complete: RETRIEVE / REPLACE / APPEND / DELETE build real "
								   "PG parse trees that flow through parse_analyze + planner + "
								   "executor and return identical results to equivalent SQL; "
								   "multi-tuple-variable joins via FROM synthesis from rangetab; "
								   "EXPLAIN supported via explainableStmt forwarders; FROM "
								   "clause pruned to only tuple-vars referenced by the query",
								   lengthof(quel_tokens), lengthof(quel_types),
								   lengthof(quel_rules), lengthof(quel_precs));
		MemoryContextSwitchTo(oldctx);

		ereport(NOTICE,
				(errmsg("quel: registered (rebuild will run on first parse)")));
	}
	else
	{
		MemoryContext oldctx;

		oldctx = MemoryContextSwitchTo(TopMemoryContext);
		quel_status_msg = psprintf("quel registration FAILED: %s",
								   err ? err : "(no error message)");
		MemoryContextSwitchTo(oldctx);

		ereport(WARNING,
				(errmsg("quel: register() failed: %s",
						err ? err : "(no error)")));

		pg_grammar_ext_unregister(ext);
	}
}

Datum
quel_extension_status(PG_FUNCTION_ARGS)
{
	const char *msg = quel_status_msg
		? quel_status_msg
		: "quel: _PG_init() did not run (extension not in "
		"shared_preload_libraries?)";

	PG_RETURN_TEXT_P(cstring_to_text(msg));
}

Datum
quel_serialized_lime(PG_FUNCTION_ARGS)
{
	const char *txt = quel_lime_text
		? quel_lime_text
		: "quel: register() did not run or failed; no fragment available";

	PG_RETURN_TEXT_P(cstring_to_text(txt));
}
