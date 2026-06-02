/*-------------------------------------------------------------------------
 *
 * nodeIndexscan.h
 *
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeIndexscan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEINDEXSCAN_H
#define NODEINDEXSCAN_H

#include "access/genam.h"
#include "access/itup.h"
#include "access/parallel.h"
#include "nodes/execnodes.h"

extern IndexScanState *ExecInitIndexScan(IndexScan *node, EState *estate, int eflags);
extern void ExecEndIndexScan(IndexScanState *node);
extern void ExecIndexMarkPos(IndexScanState *node);
extern void ExecIndexRestrPos(IndexScanState *node);
extern void ExecReScanIndexScan(IndexScanState *node);
extern void ExecIndexScanEstimate(IndexScanState *node, ParallelContext *pcxt);
extern void ExecIndexScanInitializeDSM(IndexScanState *node, ParallelContext *pcxt);
extern void ExecIndexScanReInitializeDSM(IndexScanState *node, ParallelContext *pcxt);
extern void ExecIndexScanInitializeWorker(IndexScanState *node,
										  ParallelWorkerContext *pwcxt);
extern void ExecIndexScanInstrumentEstimate(IndexScanState *node,
											ParallelContext *pcxt);
extern void ExecIndexScanInstrumentInitDSM(IndexScanState *node,
										   ParallelContext *pcxt);
extern void ExecIndexScanInstrumentInitWorker(IndexScanState *node,
											  ParallelWorkerContext *pwcxt);
extern void ExecIndexScanRetrieveInstrumentation(IndexScanState *node);

/*
 * These routines are exported to share code with nodeIndexonlyscan.c and
 * nodeBitmapIndexscan.c
 */
extern void ExecIndexBuildScanKeys(PlanState *planstate, Relation index,
								   List *quals, bool isorderby,
								   ScanKey *scanKeys, int *numScanKeys,
								   IndexRuntimeKeyInfo **runtimeKeys, int *numRuntimeKeys,
								   IndexArrayKeyInfo **arrayKeys, int *numArrayKeys);
extern void ExecIndexEvalRuntimeKeys(ExprContext *econtext,
									 IndexRuntimeKeyInfo *runtimeKeys, int numRuntimeKeys);
extern bool ExecIndexEvalArrayKeys(ExprContext *econtext,
								   IndexArrayKeyInfo *arrayKeys, int numArrayKeys);
extern bool ExecIndexAdvanceArrayKeys(IndexArrayKeyInfo *arrayKeys, int numArrayKeys);

/*
 * Stale-entry recheck for table AMs that update in place keeping the same
 * TID.  Returns true if the index entry that produced scandesc->xs_itup has a
 * stored key that no longer matches the live tuple in liveslot, and so should
 * be skipped.  When the entry is current and live_itup_out is non-NULL, it is
 * set to the reformed live index tuple (caller must pfree); index-only scans
 * use this to emit live INCLUDE columns.  Shared with nodeIndexonlyscan.c.
 */
extern bool ExecIndexInplaceEntryIsStale(EState *estate, Relation idxrel,
										 IndexInfo *indexInfo,
										 IndexScanDesc scandesc,
										 TupleTableSlot *liveslot,
										 IndexTuple *live_itup_out);

#endif							/* NODEINDEXSCAN_H */
