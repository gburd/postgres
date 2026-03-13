/*-------------------------------------------------------------------------
 *
 * miscnodes.h
 *	  Definitions for hard-to-classify node types.
 *
 * Node types declared here are not part of parse trees, plan trees,
 * or execution state trees.  We only assign them NodeTag values because
 * IsA() tests provide a convenient way to disambiguate what kind of
 * structure is being passed through assorted APIs, such as function
 * "context" pointers.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/nodes/miscnodes.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MISCNODES_H
#define MISCNODES_H

#include "nodes/nodes.h"
#include "nodes/bitmapset.h"
#include "nodes/pg_list.h"
#include "access/attnum.h"

/* Forward declarations */
typedef struct RelationData *Relation;
typedef struct MemoryContextData *MemoryContext;
struct SubattrInfo;

/*
 * ErrorSaveContext -
 *		function call context node for handling of "soft" errors
 *
 * A caller wishing to trap soft errors must initialize a struct like this
 * with all fields zero/NULL except for the NodeTag.  Optionally, set
 * details_wanted = true if more than the bare knowledge that a soft error
 * occurred is required.  The struct is then passed to a SQL-callable function
 * via the FunctionCallInfo.context field; or below the level of SQL calls,
 * it could be passed to a subroutine directly.
 *
 * After calling code that might report an error this way, check
 * error_occurred to see if an error happened.  If so, and if details_wanted
 * is true, error_data has been filled with error details (stored in the
 * callee's memory context!).  The ErrorData can be modified (e.g. downgraded
 * to a WARNING) and reported with ThrowErrorData().  FreeErrorData() can be
 * called to release error_data, although that step is typically not necessary
 * if the called code was run in a short-lived context.
 */
typedef struct ErrorSaveContext
{
	NodeTag		type;
	bool		error_occurred; /* set to true if we detect a soft error */
	bool		details_wanted; /* does caller want more info than that? */
	ErrorData  *error_data;		/* details of error, if so */
} ErrorSaveContext;

/* Often-useful macro for checking if a soft error was reported */
#define SOFT_ERROR_OCCURRED(escontext) \
	((escontext) != NULL && IsA(escontext, ErrorSaveContext) && \
	 ((ErrorSaveContext *) (escontext))->error_occurred)

/*
 * SubattrTrackingContext -
 *		context node for sub-attribute modification tracking during UPDATE
 *
 * This context enables the instrumented path of sub-attribute modification
 * tracking.  It is passed to mutation functions (jsonb_set, jsonb_delete,
 * jsonb_insert, etc.) via fcinfo->context during UPDATE expression evaluation.
 *
 * How functions use it:
 *   1. The function checks IsA(fcinfo->context, SubattrTrackingContext) to
 *      see if tracking is active.
 *   2. If so, the function reads the subattr_info field to obtain indexed
 *      sub-attribute descriptors (path arrays for JSONB, XPath expressions
 *      for XML, etc.).
 *   3. The function checks whether its modification path intersects any
 *      indexed descriptor.  For example, jsonb_set modifying path {a,b}
 *      intersects an index on (col->'a'->'b'->'c') because {a,b} is a
 *      prefix of the index path {a,b,c}.
 *   4. If the modification intersects an indexed path, the function sets
 *      fcinfo->modified_idx_subattr = true.  If there is no intersection,
 *      the flag remains false, signaling that the modification is irrelevant
 *      to all indexes.
 *
 * After the function returns, the executor's ACCUMULATE_SUBATTR_MODIFICATIONS
 * macro in execExprInterp.c checks the flag and, if set, records
 * target_attnum in ri_ModifiedIdxAttrs.
 *
 * Security design: This context does NOT contain a Relation pointer.
 * Mutation functions only see the subattr_info field (descriptors and
 * compare function OIDs).  The executor-internal fields (modified_idx_attrs,
 * modified_idx_mcxt, resno_to_attnum) are for the executor's accumulation
 * and column-mapping logic; functions must not touch them.
 *
 * Multi-column support: For UPDATE statements that modify multiple columns
 * (e.g., UPDATE t SET j1 = jsonb_set(j1, ...), j2 = jsonb_set(j2, ...)),
 * ExecInjectSubattrContext creates a separate context per column, each with
 * the correct target_attnum and column-specific subattr_info.  Column
 * boundaries are identified by EEOP_ASSIGN_TMP / EEOP_ASSIGN_TMP_MAKE_RO
 * steps in the expression step array.
 */
typedef struct SubattrTrackingContext
{
	pg_node_attr(no_copy_equal, no_read, no_query_jumble)

	NodeTag		type;			/* T_SubattrTrackingContext */

	/*--- Executor-internal fields: NOT for use by mutation functions ---*/

	AttrNumber	target_attnum;	/* Column being modified by this expression */
	Bitmapset **modified_idx_attrs pg_node_attr(read_write_ignore); /* ->
																	 * ri_ModifiedIdxAttrs */
	MemoryContext modified_idx_mcxt pg_node_attr(read_write_ignore);	/* Per-query context for
																		 * bms */

	/*
	 * Mapping from subplan result tuple position (resno) to table column
	 * number (attnum).  Array indexed by (resno - 1).  Used during expression
	 * compilation (ExecBuildProjectionInfo) to set the correct target_attnum
	 * for each column's context.
	 */
	AttrNumber *resno_to_attnum pg_node_attr(read_write_ignore);
	int			max_resno;		/* Size of resno_to_attnum array */

	/*
	 * List of table column numbers being modified (updateColnos from
	 * ModifyTable).  Used in ExecBuildProjectionInfo to populate the
	 * resno_to_attnum mapping.
	 */
	List	   *updateColnos pg_node_attr(read_write_ignore);

	/*--- Function-visible field ---*/

	/*
	 * Indexed sub-attribute descriptors for the target column.  This is the
	 * only field that mutation functions should read.  It provides path
	 * descriptors and compare function OIDs needed for intersection checking,
	 * without exposing the full Relation structure.
	 */
	struct SubattrInfo *subattr_info pg_node_attr(read_write_ignore);
} SubattrTrackingContext;

#endif							/* MISCNODES_H */
