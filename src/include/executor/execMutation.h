/*-------------------------------------------------------------------------
 *
 * execMutation.h
 *    Declarations for sub-attribute mutation tracking during UPDATE.
 *
 * src/include/executor/execMutation.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef EXEC_MUTATION_H
#define EXEC_MUTATION_H

#include "nodes/nodes.h"
#include "nodes/bitmapset.h"
#include "access/htup.h"
#include "nodes/memnodes.h"
#include "utils/rel.h"

/*
 * add_modified_idx_attr
 *
 * Record that a mutation to the given base-table attribute affected an
 * indexed subattr.  Called by sub-attribute-aware mutation functions
 * (jsonb_set, etc.) during UPDATE SET expression evaluation.
 *
 * mix_attrs is a pointer to a Bitmapset * accumulator (typically
 * &ResultRelInfo.ri_ModifiedIdxAttrs).  mix_mcxt is the memory context
 * in which the Bitmapset should be allocated (typically the per-query
 * context, so it survives per-tuple expression context resets).
 *
 * The Bitmapset is additive: successive calls from different mutation
 * functions (or nested calls on the same column) union their results.
 */
extern void add_modified_idx_attr(Bitmapset **mix_attrs, MemoryContext mix_mcxt,
								  AttrNumber attnum);

/*
 * HeapCheckSubattrChanges
 *
 * Fallback subattr comparison for non-executor code paths (e.g.,
 * simple_heap_update used by catalog operations) and for executor
 * updates with uninstrumented mutation functions.  For each attribute
 * in check_attrs that has subattr descriptors, compares old and new
 * values using the type's typidxcompare function.  Returns the subset
 * of check_attrs where no indexed subattr actually changed (safe to
 * remove from the HOT-blocking set).
 *
 * See the detailed "Dual-path architecture" comment in execMutation.c
 * for the relationship between this fallback path and the instrumented
 * path (SubattrTrackingContext / add_modified_idx_attr).
 */
extern Bitmapset *HeapCheckSubattrChanges(Relation relation,
										  HeapTuple oldtup,
										  HeapTuple newtup,
										  Bitmapset *check_attrs);

#endif							/* EXEC_MUTATION_H */
