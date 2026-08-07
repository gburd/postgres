/*-------------------------------------------------------------------------
 *
 * boot_gram_yytype.h
 *	  YYSTYPE union for the bootstrap (BKI) parser.
 *
 * This header is private to src/backend/bootstrap/.  Both the Lime grammar
 * (bootparse.lime, via its %include block) and the hand-rolled scanner/
 * driver (bootscanner.c) pull this in so the token semantic-value union
 * has exactly one definition.
 *
 * The union shape matches the Bison %union in the retired grammar
 * (pre-Phase 2c bootparse.y).  It is kept identical so that boot_yylex()
 * retains the signature `int boot_yylex(union YYSTYPE *, yyscan_t)`
 * declared in include/bootstrap/bootstrap.h.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/bootstrap/boot_gram_yytype.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BOOT_GRAM_YYTYPE_H
#define BOOT_GRAM_YYTYPE_H

#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "utils/memutils.h"

/*
 * Semantic value union carried by every token and every grammar symbol
 * whose %type resolves through this struct.  bootstrap.h forward-declares
 * `union YYSTYPE` and boot_yylex() takes `union YYSTYPE *`, so we must
 * keep the tag and name intact.
 */
typedef union YYSTYPE
{
	List	   *list;
	IndexElem  *ielem;
	char	   *str;
	const char *kw;
	int			ival;
	Oid			oidval;
} YYSTYPE;

/*
 * File-scope globals shared by bootscanner.c (definitions) and the
 * Lime-generated bootparse.c (references from action blocks).  The
 * retired Bison grammar declared these as statics inside the .y file;
 * now that the actions live on the grammar side and the helpers live
 * in the scanner file, the linkage has to be explicit.
 */
extern MemoryContext boot_per_line_ctx;
extern int	boot_num_columns_read;

extern void boot_do_start(void);
extern void boot_do_end(void);

#endif							/* BOOT_GRAM_YYTYPE_H */
