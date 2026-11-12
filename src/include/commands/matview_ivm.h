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
#include "utils/relcache.h"

extern void CompileIncrementalMatView(Oid matviewOid, Query *viewQuery);
extern bool IsIncrementalMatView(Oid matviewOid);
extern void CheckNoIncrementalMatViews(Relation rel, const char *detail);
extern void RemoveMatViewIvmEntry(Oid matviewOid);

/* values of ResultRelInfo.ri_IvmCapture */
#define IVM_CAPTURE_UNKNOWN		0
#define IVM_CAPTURE_YES			1
#define IVM_CAPTURE_NO			2

/* capture of base-table changes, matview_ivm_capture.c */
struct ResultRelInfo;
struct TupleTableSlot;
extern void IvmCaptureRow(struct ResultRelInfo *relinfo,
						  struct TupleTableSlot *oldslot,
						  struct TupleTableSlot *newslot);
extern void IvmCaptureRecompute(Relation rel);
extern void AtEOSubXact_IvmCapture(bool isCommit, SubTransactionId mySubid,
								   SubTransactionId parentSubid);
extern void AtEOXact_IvmCapture(bool isCommit);
extern bool IvmCapturedCounts(Oid relid, int64 *nold, int64 *nnew,
							  bool *recompute);

/* the changes to one table handed to the commit pass */
typedef struct IvmCapturedTable
{
	Oid			relid;
	struct Tuplestorestate *olds;	/* rows removed, or NULL */
	struct Tuplestorestate *news;	/* rows added, or NULL */
	bool		recompute;		/* changed outside the executor */
} IvmCapturedTable;

extern List *IvmTakeCapturedChanges(void);

/* the commit pass, matview_ivm_maintain.c */
extern bool PreCommit_MaintainMatViews(void);

#endif							/* MATVIEW_IVM_H */
