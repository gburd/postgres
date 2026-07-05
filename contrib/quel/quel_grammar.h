/*-------------------------------------------------------------------------
 *
 * quel_grammar.h
 *	  Internal declarations shared between contrib/quel's translation
 *	  units.
 *
 * The QUEL extension splits across:
 *	  quel.c            -- _PG_init, token + rule registration
 *	  quel_grammar.c    -- reduce-callback implementations
 *	  quel_rangetab.c   -- session-scoped tuple variable table
 *
 * All three include this header for shared types and prototypes.
 * Public-facing C functions exposed via quel--1.0.sql live in
 * quel.c; everything else is module-internal.
 *
 * Portions Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * contrib/quel/quel_grammar.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef QUEL_GRAMMAR_H
#define QUEL_GRAMMAR_H

#include "postgres.h"

#include "nodes/parsenodes.h"
#include "parser/parser_extension.h"

/* ------------------------------------------------------------------------- */
/* Tuple-variable table                                                      */
/* ------------------------------------------------------------------------- */

/*
 * Berkeley QUEL declares tuple variables via `RANGE OF e IS emp`.
 * Tuple variables persist across statements within a session, so a
 * subsequent `RETRIEVE (e.name)` resolves `e` to its bound relation.
 *
 * We maintain a per-backend hash table keyed on the tuple variable
 * name.  The table is reset at backend start and updated by
 * QuelRangeStmt reductions.  Subsequent QUEL statements that
 * reference `e.column` consult this table to resolve the relation.
 */
typedef struct QuelRangeEntry
{
	const char *name;			/* tuple variable name (lowercase) */
	const char *relation;		/* backing relation name */
	int			lineno;			/* line where RANGE was registered */
} QuelRangeEntry;

extern void quel_rangetab_init(void);
extern void quel_rangetab_reset(void);
extern void quel_rangetab_set(const char *name, const char *relation);
extern const char *quel_rangetab_lookup(const char *name);
extern bool quel_rangetab_iterate(int *cursor, QuelRangeEntry *out);
extern int	quel_rangetab_count(void);

/* ------------------------------------------------------------------------- */
/* Reduce-callback dispatch                                                  */
/* ------------------------------------------------------------------------- */

/*
 * Each QUEL rule's reduce callback receives:
 *   nrhs:        number of RHS symbols
 *   rhs_values:  pointers to per-symbol values (typed per the rule
 *                declaration in quel.c)
 *   rhs_locs:    parallel array of source byte offsets
 *   lhs_out:     destination -- the callback writes a Node * (or
 *                whatever the rule's LHS type is) here
 *
 * The dispatch trampoline in quel.c (quel_dispatch) takes the rule
 * id and forwards to the matching builder below.
 */

/* Phase B builders -- produce PG parse-tree nodes from rhs_values. */
extern Node *quel_build_attr_simple(const void *const *rhs_values,
									const int *rhs_locs, int nrhs);
extern Node *quel_build_attr_qualified(const void *const *rhs_values,
									   const int *rhs_locs, int nrhs);
extern Node *quel_build_attr_qualified_kw(const void *const *rhs_values,
										  const int *rhs_locs, int nrhs);
extern List *quel_build_attr_list_single(const void *const *rhs_values,
										 const int *rhs_locs, int nrhs);
extern List *quel_build_attr_list_cons(const void *const *rhs_values,
									   const int *rhs_locs, int nrhs);
extern Node *quel_build_retrieve_simple(const void *const *rhs_values,
										const int *rhs_locs, int nrhs);
extern Node *quel_build_retrieve_where(const void *const *rhs_values,
									   const int *rhs_locs, int nrhs);
extern Node *quel_build_retrieve_by(const void *const *rhs_values,
									const int *rhs_locs, int nrhs);
extern Node *quel_build_retrieve_where_by(const void *const *rhs_values,
										  const int *rhs_locs, int nrhs);

/* Phase B builders for the DML statement forms. */
extern Node *quel_build_replace_simple(const void *const *rhs_values,
									   const int *rhs_locs, int nrhs);
extern Node *quel_build_replace_where(const void *const *rhs_values,
									  const int *rhs_locs, int nrhs);
extern Node *quel_build_append_full(const void *const *rhs_values,
									const int *rhs_locs, int nrhs);
extern Node *quel_build_delete_simple(const void *const *rhs_values,
									  const int *rhs_locs, int nrhs);
extern Node *quel_build_delete_where(const void *const *rhs_values,
									 const int *rhs_locs, int nrhs);

extern Node *quel_build_retrieve(const void *const *rhs_values,
								 const int *rhs_locs, int nrhs);
extern Node *quel_build_replace(const void *const *rhs_values,
								const int *rhs_locs, int nrhs);
extern Node *quel_build_append(const void *const *rhs_values,
							   const int *rhs_locs, int nrhs);
extern Node *quel_build_delete(const void *const *rhs_values,
							   const int *rhs_locs, int nrhs);
extern Node *quel_build_create(const void *const *rhs_values,
							   const int *rhs_locs, int nrhs);
extern Node *quel_build_destroy(const void *const *rhs_values,
								const int *rhs_locs, int nrhs);
extern Node *quel_build_copy(const void *const *rhs_values,
							 const int *rhs_locs, int nrhs);
extern Node *quel_build_define_view(const void *const *rhs_values,
									const int *rhs_locs, int nrhs);
extern Node *quel_build_remove_view(const void *const *rhs_values,
									const int *rhs_locs, int nrhs);
extern Node *quel_build_index(const void *const *rhs_values,
							  const int *rhs_locs, int nrhs);
extern Node *quel_build_help(const void *const *rhs_values,
							 const int *rhs_locs, int nrhs);

/* RANGE has no parse tree -- updates state and returns NULL. */
extern void quel_apply_range(const void *const *rhs_values,
							 const int *rhs_locs, int nrhs);

/* ------------------------------------------------------------------------- */
/* Helpers used by reduce callbacks                                          */
/* ------------------------------------------------------------------------- */

/*
 * Build a RangeVar from a tuple variable name (e.g. "e") by
 * resolving against the rangetab.  Returns NULL with an ereport
 * if the tuple variable is unbound.
 */
extern RangeVar *quel_resolve_tuple_var(const char *tvname, int location);

/*
 * Build a column reference of the form "e.name" where e is a
 * tuple variable name and "name" is the column.  Returns a
 * ColumnRef node.
 */
extern Node *quel_make_column_ref(const char *tvname, const char *colname,
								  int location);

/*
 * Build the FROM clause for a RETRIEVE based on the tuple variables
 * referenced in target_list and where_clause.  Returns a List of
 * RangeVar nodes.  Used when QUEL doesn't have an explicit FROM
 * clause (Berkeley dialect).
 */
extern List *quel_implied_from_clause(List *target_list, Node *where_clause);

#endif							/* QUEL_GRAMMAR_H */
