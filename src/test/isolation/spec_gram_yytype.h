/*-------------------------------------------------------------------------
 *
 * spec_gram_yytype.h
 *	  YYSTYPE union for the isolation-test spec file parser.
 *
 * This header is private to src/test/isolation/.  Both the Lime grammar
 * (specparse.lime, via its %include block) and the hand-rolled scanner /
 * driver (specscanner.c) pull this in so the token semantic-value union
 * has exactly one definition.
 *
 * The union shape matches the Bison %union in the retired grammar
 * (pre-Phase 2f specparse.y); it is kept identical so that the
 * grammar actions and the rest of isolationtester keep building the
 * same TestSpec representation.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/test/isolation/spec_gram_yytype.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SPEC_GRAM_YYTYPE_H
#define SPEC_GRAM_YYTYPE_H

#include "isolationtester.h"

/*
 * Generic "pointer list" built up by the list-producing non-terminals
 * (setup_list, session_list, step_list, permutation_list,
 * permutation_step_list, blocker_list).  The shape matches the anonymous
 * struct member named "ptr_list" in the retired %union.
 */
typedef struct SpecPtrList
{
	void	  **elements;
	int			nelements;
} SpecPtrList;

/*
 * Semantic value carried by every terminal and non-terminal in the Lime
 * parser.  Non-terminals with a declared %type access a specific member
 * directly; terminals access the member appropriate to the token.
 */
typedef union SpecYYSTYPE
{
	char	   *str;
	int			integer;
	Session    *session;
	Step	   *step;
	Permutation *permutation;
	PermutationStep *permutationstep;
	PermutationStepBlocker *blocker;
	SpecPtrList ptr_list;
} SpecYYSTYPE;

/*
 * Current input line number, maintained by the Lime-generated scanner's
 * newline rule and read by the parser's error reporter.  Defined in
 * specscanner.c; the generated lexer references it through the
 * specscanner.lex %include block.
 */
extern int	spec_yyline;

#endif							/* SPEC_GRAM_YYTYPE_H */
