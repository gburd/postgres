/*-------------------------------------------------------------------------
 *
 * seg_gram_yytype.h
 *	  YYSTYPE union for the seg input parser.
 *
 * This header is private to contrib/seg/.  Both the Lime grammar
 * (segparse.lime, via its %include block) and the lexer driver
 * (segparse_driver.c) include this so the token semantic-value
 * union has exactly one definition.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * contrib/seg/seg_gram_yytype.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SEG_GRAM_YYTYPE_H
#define SEG_GRAM_YYTYPE_H

#include "segdata.h"

/* Boundary descriptor used by both the boundary and deviation rules. */
struct BND
{
	float		val;
	char		ext;
	char		sigd;
};

/*
 * YYSTYPE union.  text-bearing tokens (SEGFLOAT, RANGE, PLUMIN,
 * EXTENSION) read .text; the boundary and deviation non-terminals
 * populate .bnd.
 */
union YYSTYPE
{
	struct BND	bnd;
	char	   *text;
};

typedef union YYSTYPE YYSTYPE;

#endif							/* SEG_GRAM_YYTYPE_H */
