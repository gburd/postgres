/*-------------------------------------------------------------------------
 *
 * matview_ivm.h
 *	  incremental maintenance of materialized views
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/matview_ivm.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MATVIEW_IVM_H
#define MATVIEW_IVM_H

#include "nodes/parsenodes.h"

extern void CompileIncrementalMatView(Oid matviewOid, Query *viewQuery);
extern void RemoveMatViewIvmEntry(Oid matviewOid);

#endif							/* MATVIEW_IVM_H */
