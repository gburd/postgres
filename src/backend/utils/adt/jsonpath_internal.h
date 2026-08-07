/*-------------------------------------------------------------------------
 *
 * jsonpath_internal.h
 *     Private definitions for jsonpath scanner & parser
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/utils/adt/jsonpath_internal.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef JSONPATH_INTERNAL_H
#define JSONPATH_INTERNAL_H

#include "nodes/pg_list.h"

/* struct JsonPathString is shared between scan and gram */
typedef struct JsonPathString
{
	char	   *val;
	int			len;
	int			total;
} JsonPathString;

typedef void *yyscan_t;

#include "utils/jsonpath.h"

/* ------------------------------------------------------------------------- */
/* YYSTYPE union and parse-context types.                                     */
/*                                                                            */
/* Lime requires the token-value union to be visible BEFORE the generated     */
/* jsonpath_gram.h is included (because the generated header references       */
/* YYSTYPE for action stack typing).  Both definitions live here.             */
/* ------------------------------------------------------------------------- */

typedef union YYSTYPE
{
	JsonPathString str;
	List	   *elems;			/* list of JsonPathParseItem */
	List	   *indexs;			/* list of integers */
	JsonPathParseItem *value;
	JsonPathParseResult *result;
	JsonPathItemType optype;
	bool		boolean;
	int			integer;
} YYSTYPE;

typedef struct jsonpath_yy_extra
{
	JsonPathParseResult **result;
	struct Node *escontext;
	yyscan_t	scanner;		/* points at JsonPathYyScanner */
	bool		aborted;
} jsonpath_yy_extra;

typedef struct JsonPathYyScanner
{
	const char *input;			/* caller-owned input buffer */
	int			pos;			/* cursor into input[] */
	int			len;			/* total length of input */
	JsonPathString scanstring;	/* staging buffer for the current token */
	int			hi_surrogate;	/* pending high surrogate, or -1 */

	/*
	 * Most recently matched token's literal text, NUL-terminated.  Updated at
	 * every successful return from jsonpath_yylex_internal.  Read by
	 * jsonpath_yyerror{,_token} for the "at or near \"X\"" branch.  Mirrors
	 * flex's yytext semantics; cleared (set to empty) on EOF so the "at end
	 * of input" branch fires correctly.
	 */
	char		yytext[64];
	int			yytext_len;
} JsonPathYyScanner;

#include "jsonpath_gram.h"		/* token code macros */

/* ------------------------------------------------------------------------- */
/* Helpers shared between scanner and grammar actions.                       */
/* ------------------------------------------------------------------------- */

extern JsonPathParseItem *jpMakeItemType(JsonPathItemType type);
extern JsonPathParseItem *jpMakeItemString(JsonPathString *s);
extern JsonPathParseItem *jpMakeItemVariable(JsonPathString *s);
extern JsonPathParseItem *jpMakeItemKey(JsonPathString *s);
extern JsonPathParseItem *jpMakeItemNumeric(JsonPathString *s);
extern JsonPathParseItem *jpMakeItemBool(bool val);
extern JsonPathParseItem *jpMakeItemBinary(JsonPathItemType type,
										   JsonPathParseItem *la,
										   JsonPathParseItem *ra);
extern JsonPathParseItem *jpMakeItemUnary(JsonPathItemType type,
										  JsonPathParseItem *a);
extern JsonPathParseItem *jpMakeItemList(List *list);
extern JsonPathParseItem *jpMakeIndexArray(List *list);
extern JsonPathParseItem *jpMakeAny(int first, int last);
extern bool jpMakeItemLikeRegex(JsonPathParseItem *expr,
								JsonPathString *pattern,
								JsonPathString *flags,
								JsonPathParseItem **result,
								struct Node *escontext);

/* Token-aware error helper called by the grammar's %syntax_error block. */
extern void jsonpath_yyerror_token(jsonpath_yy_extra *extra,
								   int yymajor, const char *message);

/* Lime push-parser entry points (generated; %name jsonpath_yy). */
extern void *jsonpath_yyAlloc(void *(*mallocProc) (size_t));
extern void jsonpath_yyFree(void *p, void (*freeProc) (void *));
extern void jsonpath_yy(void *yyp, int yymajor, YYSTYPE yyminor,
						jsonpath_yy_extra *extra);

/* Public entry points. */
extern int	jsonpath_yyparse(JsonPathParseResult **result,
							 struct Node *escontext,
							 yyscan_t yyscanner);
extern void jsonpath_yyerror(JsonPathParseResult **result,
							 struct Node *escontext,
							 yyscan_t yyscanner,
							 const char *message);

#endif							/* JSONPATH_INTERNAL_H */
