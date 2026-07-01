/*-------------------------------------------------------------------------
 *
 * pg_fts.h
 *		Next-generation full-text search for PostgreSQL.
 *
 * Stage 1: on-disk representations and match evaluation for the analyzed
 * document type (ftsdoc) and the parsed query type (ftsquery).  No index
 * access method yet -- matching is evaluated by sequential scan via the
 * @@@ operator, exactly as tsvector/tsquery were first introduced.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/pg_fts/pg_fts.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_FTS_H
#define PG_FTS_H

#include "postgres.h"

#include "fmgr.h"
#include "varatt.h"

/*
 * ftsdoc -- an analyzed document.
 *
 * A varlena holding a sorted, de-duplicated array of terms.  Each term entry
 * records its term frequency (tf) and, immediately after the entry array, the
 * term text.  Positions are deliberately NOT stored in stage 1: they are only
 * needed for phrase/NEAR (a later stage) and storing them now would bake an
 * on-disk format we have not yet exercised.  The format is versioned so it can
 * grow.
 *
 * Layout:
 *	  FtsDocData header
 *	  FtsTermEntry entries[nterms]		(sorted by term text)
 *	  char lexemes[]					(term texts, in entry order)
 */
typedef struct FtsTermEntry
{
	uint32		off;			/* byte offset of term text within lexemes[] */
	uint32		len;			/* length of term text in bytes */
	uint32		tf;				/* term frequency within this document */
} FtsTermEntry;

typedef struct FtsDocData
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	uint16		version;		/* format version, currently 1 */
	uint16		flags;			/* reserved, must be 0 in v1 */
	uint32		nterms;			/* number of distinct terms */
	uint32		doclen;			/* total token count (sum of tf); needed by BM25 */
	FtsTermEntry entries[FLEXIBLE_ARRAY_MEMBER];
} FtsDocData;

typedef FtsDocData *FtsDoc;

#define FTS_DOC_VERSION			1
#define FTS_DOC_HDRSIZE			offsetof(FtsDocData, entries)
#define FTS_DOC_ENTRIES(d)		((d)->entries)
#define FTS_DOC_LEXEMES(d) \
	((char *) &(d)->entries[(d)->nterms])
#define FTS_DOC_TERMTEXT(d, e)	(FTS_DOC_LEXEMES(d) + (e)->off)

#define DatumGetFtsDoc(X)		((FtsDoc) PG_DETOAST_DATUM(X))
#define PG_GETARG_FTSDOC(n)		DatumGetFtsDoc(PG_GETARG_DATUM(n))
#define PG_RETURN_FTSDOC(x)		PG_RETURN_POINTER(x)

/*
 * ftsquery -- a parsed boolean query.
 *
 * Stored as a varlena flattened postfix (RPN) list of items.  This mirrors the
 * proven tsquery representation: operands and operators in one array, term
 * text appended after.  Stage 1 supports AND, OR, NOT and parenthesised
 * grouping.  Phrase/NEAR/prefix/field-scope are later stages and get their own
 * item kinds; the version field lets us add them without breaking v1 data.
 */
typedef enum FtsQueryItemType
{
	FTS_QI_VAL = 1,				/* a term operand */
	FTS_QI_OPR					/* a boolean operator */
} FtsQueryItemType;

typedef enum FtsQueryOp
{
	FTS_OP_NOT = 1,
	FTS_OP_AND,
	FTS_OP_OR
} FtsQueryOp;

typedef struct FtsQueryItem
{
	uint8		type;			/* FtsQueryItemType */
	uint8		op;				/* FtsQueryOp, valid when type == FTS_QI_OPR */
	uint16		pad;
	/* for FTS_QI_VAL: */
	uint32		termoff;		/* offset of term text within the text region */
	uint32		termlen;		/* length of term text */
} FtsQueryItem;

typedef struct FtsQueryData
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	uint16		version;		/* format version, currently 1 */
	uint16		flags;			/* reserved */
	uint32		nitems;			/* number of items in RPN list */
	FtsQueryItem items[FLEXIBLE_ARRAY_MEMBER];
	/* term texts follow items[] */
} FtsQueryData;

typedef FtsQueryData *FtsQuery;

#define FTS_QUERY_VERSION		1
#define FTS_QUERY_HDRSIZE		offsetof(FtsQueryData, items)
#define FTS_QUERY_TEXTBASE(q)	((char *) &(q)->items[(q)->nitems])
#define FTS_QUERY_ITEMTEXT(q, it) (FTS_QUERY_TEXTBASE(q) + (it)->termoff)

#define DatumGetFtsQuery(X)		((FtsQuery) PG_DETOAST_DATUM(X))
#define PG_GETARG_FTSQUERY(n)	DatumGetFtsQuery(PG_GETARG_DATUM(n))
#define PG_RETURN_FTSQUERY(x)	PG_RETURN_POINTER(x)

/* pg_fts_analyze.c -- the built-in stage-1 tokenizer */
extern FtsDoc fts_analyze_text(const char *str, int len);

/* pg_fts_tsanalyze.c -- analyzer reusing an installed TS configuration */
extern FtsDoc fts_analyze_with_config(Oid cfgId, const char *str, int len);

/* pg_fts_query.c -- parse query text into an ftsquery */
extern FtsQuery fts_parse_query(const char *str, int len);

/* pg_fts_match.c -- evaluate a parsed query against an analyzed doc */
extern bool fts_doc_matches(FtsDoc doc, FtsQuery query);

/* shared: binary-search a term in a doc; returns entry or NULL */
extern FtsTermEntry *fts_doc_lookup(FtsDoc doc, const char *term, int termlen);

#endif							/* PG_FTS_H */
