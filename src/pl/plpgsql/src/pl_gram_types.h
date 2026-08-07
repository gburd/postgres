/*-------------------------------------------------------------------------
 *
 * pl_gram_types.h
 *	  Declares YYSTYPE for plpgsql's parser.
 *
 * Lime emits the YYSTYPE union body inside the generated pl_gram.c
 * (matching the backend gram.lime convention), but consumers of
 * pl_gram.h (pl_scanner.c, pl_comp.c, pl_exec.c, etc.) need the body
 * visible.  This header carries it.  The member set must stay in sync
 * with the original `%union` from pl_gram.y -- when the union changes,
 * regenerate pl_gram.lime via lime_convert_gram.py and update both
 * places together.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/pl/plpgsql/src/pl_gram_types.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PL_GRAM_TYPES_H
#define PL_GRAM_TYPES_H

/*
 * Self-contained: include the prerequisites we need so this header can
 * be pulled into Lime's generated pl_gram.c BEFORE the user prologue's
 * function-declaration block (which references YYSTYPE unqualified).
 */
#include "postgres.h"
#include "common/keywords.h"
#include "parser/scanner.h"
#include "plpgsql.h"

/*
 * When this header is included alongside a bison-generated pl_gram.h
 * (the legacy build path before Phase 2j flips), pl_gram.h emits its
 * own YYSTYPE typedef.  Skip ours to avoid a redefinition conflict.
 * Once Phase 2j lands, pl_gram.h is Lime-emitted and contains only
 * #define lines for tokens, so this guard is a no-op.
 */
#ifndef YYSTYPE_IS_DECLARED

typedef union YYSTYPE
{
	core_YYSTYPE core_yystype;
	/* these fields must match core_YYSTYPE: */
	int			ival;
	char	   *str;
	const char *keyword;

	PLword		word;
	PLcword		cword;
	PLwdatum	wdatum;
	bool		boolean;
	Oid			oid;
	struct
	{
		char	   *name;
		int			lineno;
	}			varname;
	struct
	{
		char	   *name;
		int			lineno;
		int			cursor_options;
	}			cursor_intro;
	struct
	{
		char	   *name;
		int			lineno;
		PLpgSQL_datum *scalar;
		PLpgSQL_datum *row;
	}			forvariable;
	struct
	{
		char	   *label;
		int			n_initvars;
		int		   *initvarnos;
	}			declhdr;
	struct
	{
		List	   *stmts;
		char	   *end_label;
		int			end_label_location;
	}			loop_body;
	List	   *list;
	PLpgSQL_type *dtype;
	PLpgSQL_datum *datum;
	PLpgSQL_var *var;
	PLpgSQL_expr *expr;
	PLpgSQL_stmt *stmt;
	PLpgSQL_condition *condition;
	PLpgSQL_exception *exception;
	PLpgSQL_exception_block *exception_block;
	PLpgSQL_nsitem *nsitem;
	PLpgSQL_diag_item *diagitem;
	PLpgSQL_stmt_fetch *fetch;
	PLpgSQL_case_when *casewhen;
} YYSTYPE;

/*
 * Tell bison-generated pl_gram.h (legacy build path) that YYSTYPE is
 * already declared so it doesn't redefine.  Once Phase 2j flips to
 * Lime, pl_gram.h has no body of its own and this is a no-op.
 */
#define YYSTYPE_IS_DECLARED 1

#endif							/* YYSTYPE_IS_DECLARED */

#endif							/* PL_GRAM_TYPES_H */
