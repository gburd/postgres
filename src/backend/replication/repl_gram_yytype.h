/*-------------------------------------------------------------------------
 *
 * repl_gram_yytype.h
 *	  YYSTYPE union and parser extra-state for the replication command
 *	  parser.
 *
 * This header is private to src/backend/replication/.  Both the Lime
 * grammar (repl_gram.lime, via its %include block) and the hand-rolled
 * scanner/driver (repl_scanner.c) pull this in so the token semantic-value
 * union and the %extra_argument struct have exactly one definition.
 *
 * The union shape matches the Bison %union in the retired grammar
 * (pre-Phase 2b repl_gram source); it
 * is kept identical so that replication_yylex() retains the signature
 * `int replication_yylex(union YYSTYPE *, yyscan_t)` declared in
 * walsender_private.h.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/replication/repl_gram_yytype.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef REPL_GRAM_YYTYPE_H
#define REPL_GRAM_YYTYPE_H

#include "access/xlogdefs.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"

/*
 * Semantic value union carried by every token and every grammar symbol
 * whose %type resolves through this struct.  Non-terminals with specific
 * Lime %type declarations read the relevant member directly in their
 * action bodies.
 */
typedef union YYSTYPE
{
	char	   *str;
	bool		boolval;
	uint32		uintval;
	XLogRecPtr	recptr;
	Node	   *node;
	List	   *list;
	DefElem    *defelt;
} YYSTYPE;

/*
 * Extra state threaded through the Lime parser as %extra_argument.  Mirrors
 * the Bison %parse-param list: the caller-provided result slot and the
 * scanner handle (forwarded to replication_yyerror() on failure).
 */
typedef struct replication_yy_extra
{
	Node	  **replication_parse_result_p;
	void	   *yyscanner;
} replication_yy_extra;

#endif							/* REPL_GRAM_YYTYPE_H */
