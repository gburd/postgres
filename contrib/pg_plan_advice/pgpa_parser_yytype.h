/*-------------------------------------------------------------------------
 *
 * pgpa_parser_yytype.h
 *	  YYSTYPE union for the pg_plan_advice parser.
 *
 * Private to contrib/pg_plan_advice/.  Both the Lime grammar
 * (pgpa_parser.lime, via its %include block) and the lexer driver
 * (pgpa_scan.c) include this so the token semantic-value union has
 * exactly one definition.
 *
 * Portions Copyright (c) 2000-2026, PostgreSQL Global Development Group
 *
 * contrib/pg_plan_advice/pgpa_parser_yytype.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGPA_PARSER_YYTYPE_H
#define PGPA_PARSER_YYTYPE_H

#include "nodes/pg_list.h"

/* Forward decls so the union compiles without pulling in pgpa_ast.h. */
typedef struct pgpa_advice_item pgpa_advice_item;
typedef struct pgpa_advice_target pgpa_advice_target;
typedef struct pgpa_index_target pgpa_index_target;

/*
 * Semantic value union carried by every token and every grammar
 * symbol whose %type resolves through this struct.  Layout matches
 * the retired Bison %union in pgpa_parser.y.
 */
union YYSTYPE
{
	char	   *str;
	int			integer;
	List	   *list;
	pgpa_advice_item *item;
	pgpa_advice_target *target;
	pgpa_index_target *itarget;
};

typedef union YYSTYPE YYSTYPE;

#endif							/* PGPA_PARSER_YYTYPE_H */
