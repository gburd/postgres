/*-------------------------------------------------------------------------
 *
 * preproc_yytype.h
 *	  YYSTYPE union for ecpg's grammar.
 *
 * Lime emits the YYSTYPE union body inside the generated preproc.c
 * (not in preproc.h), so consumers of preproc.h that need YYSTYPE
 * must declare it separately.  This header provides the typedef
 * shared by parser.c (which uses base_yylval/base_yylloc) and pgc.c.
 *
 * The struct definitions match the %union in preproc.y (which is
 * kept in sync via parse.pl-driven generation from gram.lime).
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/interfaces/ecpg/preproc/preproc_yytype.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PREPROC_YYTYPE_H
#define PREPROC_YYTYPE_H

#include "preproc_extern.h"		/* base ecpg types */
#include "type.h"				/* struct when, struct prep, etc. */

#ifndef YYSTYPE_IS_DECLARED
#define YYSTYPE_IS_DECLARED 1

typedef union YYSTYPE
{
	double		dval;
	char	   *str;
	int			ival;
	struct when action;
	struct index index;
	int			tagname;
	struct this_type type;
	enum ECPGttype type_enum;
	enum ECPGdtype dtype_enum;
	struct fetch_desc descriptor;
	struct su_symbol struct_union;
	struct prep prep;
	struct exec exec;
	struct describe describe;
} YYSTYPE;

#endif							/* YYSTYPE_IS_DECLARED */

#endif							/* PREPROC_YYTYPE_H */
