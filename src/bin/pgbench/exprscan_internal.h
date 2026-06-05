/*-------------------------------------------------------------------------
 *
 * exprscan_internal.h
 *	  YYSTYPE union and parser-extra struct for the pgbench expression parser.
 *
 * Private to src/bin/pgbench/.  Both the Lime grammar (exprparse.lime,
 * via its %include block) and the hand-rolled scanner (exprscan.c) pull
 * this in so they agree on token-value layout.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/bin/pgbench/exprscan_internal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef EXPRSCAN_INTERNAL_H
#define EXPRSCAN_INTERNAL_H

#include "fe_utils/psqlscan.h"
#include "pgbench.h"

/*
 * YYSTYPE union: same shape as the retired Bison %union in exprparse.y.
 */
typedef union YYSTYPE
{
	int64		ival;
	double		dval;
	bool		bval;
	char	   *str;
	PgBenchExpr *expr;
	PgBenchExprList *elist;
} YYSTYPE;

/*
 * Lime's single %extra_argument; collapses Bison's two %parse-param slots
 * (result, yyscanner) plus an aborted flag.
 */
typedef struct expr_yy_extra
{
	PgBenchExpr **result;
	yyscan_t	yyscanner;
	bool		aborted;
} expr_yy_extra;

/*
 * Lime push-parser entry points (generated from exprparse.lime;
 * %name expr_yy).
 */
extern void *expr_yyAlloc(void *(*mallocProc) (size_t));
extern void expr_yyFree(void *p, void (*freeProc) (void *));
extern void expr_yy(void *yyp, int yymajor, YYSTYPE yyminor,
					expr_yy_extra *extra);

#endif							/* EXPRSCAN_INTERNAL_H */
