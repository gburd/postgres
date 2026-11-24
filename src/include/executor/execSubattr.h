/*-------------------------------------------------------------------------
 *
 * execSubattr.h
 *	  Prove that an UPDATE cannot change what an index reads of a column.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/include/executor/execSubattr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef EXECSUBATTR_H
#define EXECSUBATTR_H

#include "nodes/execnodes.h"
#include "nodes/supportnodes.h"

/*
 * What an index makes of one column of its table depends only on these
 * parts of the column's value, as compare reads them.  See
 * RelationGetIndexSubattrReads().
 */
typedef struct IndexSubattrReads
{
	AttrNumber	attnum;			/* the table's column */
	List	   *paths;			/* List of List of SubattrPathElem */
	SubattrCompareFunc compare;
	bool		stored;			/* described by amsubattrreads */
} IndexSubattrReads;

extern Bitmapset *ExecSubattrProveUntouched(ResultRelInfo *resultRelInfo,
											Node *setexpr, int varno,
											AttrNumber attnum,
											ParamListInfo params);

extern List *RelationGetIndexSubattrReads(Relation indexRel);

#endif							/* EXECSUBATTR_H */
