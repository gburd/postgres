/*-------------------------------------------------------------------------
 *
 * cost.h
 *	  prototypes for costsize.c and clausesel.c.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/cost.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COST_H
#define COST_H

#include "nodes/pathnodes.h"
#include "utils/backend_runtime_current.h"
#include "utils/global_lifetime.h"
#include "nodes/plannodes.h"


/* defaults for costsize.c's Cost parameters */
/* NB: cost-estimation code should use the variables, not these constants! */
/* If you change these, update backend/utils/misc/postgresql.conf.sample */
#define DEFAULT_SEQ_PAGE_COST  1.0
#define DEFAULT_RANDOM_PAGE_COST  4.0
#define DEFAULT_CPU_TUPLE_COST	0.01
#define DEFAULT_CPU_INDEX_TUPLE_COST 0.005
#define DEFAULT_CPU_OPERATOR_COST  0.0025
#define DEFAULT_PARALLEL_TUPLE_COST 0.1
#define DEFAULT_PARALLEL_SETUP_COST  1000.0

/* defaults for non-Cost parameters */
#define DEFAULT_RECURSIVE_WORKTABLE_FACTOR  10.0
#define DEFAULT_EFFECTIVE_CACHE_SIZE  524288	/* measured in pages */

typedef enum
{
	CONSTRAINT_EXCLUSION_OFF,	/* do not use c_e */
	CONSTRAINT_EXCLUSION_ON,	/* apply c_e to all rels */
	CONSTRAINT_EXCLUSION_PARTITION, /* apply c_e to otherrels only */
}			ConstraintExclusionType;


/*
 * prototypes for costsize.c
 *	  routines to compute costs and sizes
 */

/* parameter variables and flags (see also optimizer.h) */
extern Cost *PgCurrentDisableCostRef(void);
#ifndef PgCurrentMaxParallelWorkersPerGatherRef
extern int *PgCurrentMaxParallelWorkersPerGatherRef(void);
#endif
#ifndef PgCurrentEnableSeqscanRef
extern bool *PgCurrentEnableSeqscanRef(void);
#endif
#ifndef PgCurrentEnableIndexscanRef
extern bool *PgCurrentEnableIndexscanRef(void);
#endif
#ifndef PgCurrentEnableIndexonlyscanRef
extern bool *PgCurrentEnableIndexonlyscanRef(void);
#endif
#ifndef PgCurrentEnableBitmapscanRef
extern bool *PgCurrentEnableBitmapscanRef(void);
#endif
#ifndef PgCurrentEnableTidscanRef
extern bool *PgCurrentEnableTidscanRef(void);
#endif
#ifndef PgCurrentEnableSortRef
extern bool *PgCurrentEnableSortRef(void);
#endif
#ifndef PgCurrentEnableIncrementalSortRef
extern bool *PgCurrentEnableIncrementalSortRef(void);
#endif
#ifndef PgCurrentEnableHashaggRef
extern bool *PgCurrentEnableHashaggRef(void);
#endif
#ifndef PgCurrentEnableNestloopRef
extern bool *PgCurrentEnableNestloopRef(void);
#endif
#ifndef PgCurrentEnableMaterialRef
extern bool *PgCurrentEnableMaterialRef(void);
#endif
#ifndef PgCurrentEnableMemoizeRef
extern bool *PgCurrentEnableMemoizeRef(void);
#endif
#ifndef PgCurrentEnableMergejoinRef
extern bool *PgCurrentEnableMergejoinRef(void);
#endif
#ifndef PgCurrentEnableHashjoinRef
extern bool *PgCurrentEnableHashjoinRef(void);
#endif
#ifndef PgCurrentEnableGathermergeRef
extern bool *PgCurrentEnableGathermergeRef(void);
#endif
#ifndef PgCurrentEnablePartitionwiseJoinRef
extern bool *PgCurrentEnablePartitionwiseJoinRef(void);
#endif
#ifndef PgCurrentEnablePartitionwiseAggregateRef
extern bool *PgCurrentEnablePartitionwiseAggregateRef(void);
#endif
#ifndef PgCurrentEnableParallelAppendRef
extern bool *PgCurrentEnableParallelAppendRef(void);
#endif
#ifndef PgCurrentEnableParallelHashRef
extern bool *PgCurrentEnableParallelHashRef(void);
#endif
#ifndef PgCurrentEnablePartitionPruningRef
extern bool *PgCurrentEnablePartitionPruningRef(void);
#endif
#ifndef PgCurrentEnablePresortedAggregateRef
extern bool *PgCurrentEnablePresortedAggregateRef(void);
#endif
#ifndef PgCurrentEnableAsyncAppendRef
extern bool *PgCurrentEnableAsyncAppendRef(void);
#endif
#ifndef PgCurrentConstraintExclusionRef
extern int *PgCurrentConstraintExclusionRef(void);
#endif

#define disable_cost \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentDisableCostHotRef, \
									   CurrentPgSession, \
									   PgCurrentDisableCostRef))
#define max_parallel_workers_per_gather \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentMaxParallelWorkersPerGatherHotRef, \
									   CurrentPgSession, \
									   PgCurrentMaxParallelWorkersPerGatherRef))
#define enable_seqscan \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentEnableSeqscanHotRef, \
									   CurrentPgSession, \
									   PgCurrentEnableSeqscanRef))
#define enable_indexscan \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentEnableIndexscanHotRef, \
									   CurrentPgSession, \
									   PgCurrentEnableIndexscanRef))
#define enable_indexonlyscan \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentEnableIndexonlyscanHotRef, \
									   CurrentPgSession, \
									   PgCurrentEnableIndexonlyscanRef))
#define enable_bitmapscan \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentEnableBitmapscanHotRef, \
									   CurrentPgSession, \
									   PgCurrentEnableBitmapscanRef))
#define enable_tidscan \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentEnableTidscanHotRef, \
									   CurrentPgSession, \
									   PgCurrentEnableTidscanRef))
#define enable_sort \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentEnableSortHotRef, \
									   CurrentPgSession, \
									   PgCurrentEnableSortRef))
#define enable_incremental_sort \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentEnableIncrementalSortHotRef, \
									   CurrentPgSession, \
									   PgCurrentEnableIncrementalSortRef))
#define enable_hashagg \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentEnableHashaggHotRef, \
									   CurrentPgSession, \
									   PgCurrentEnableHashaggRef))
#define enable_nestloop \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentEnableNestloopHotRef, \
									   CurrentPgSession, \
									   PgCurrentEnableNestloopRef))
#define enable_material \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentEnableMaterialHotRef, \
									   CurrentPgSession, \
									   PgCurrentEnableMaterialRef))
#define enable_memoize \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentEnableMemoizeHotRef, \
									   CurrentPgSession, \
									   PgCurrentEnableMemoizeRef))
#define enable_mergejoin \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentEnableMergejoinHotRef, \
									   CurrentPgSession, \
									   PgCurrentEnableMergejoinRef))
#define enable_hashjoin \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentEnableHashjoinHotRef, \
									   CurrentPgSession, \
									   PgCurrentEnableHashjoinRef))
#define enable_gathermerge \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentEnableGathermergeHotRef, \
									   CurrentPgSession, \
									   PgCurrentEnableGathermergeRef))
#define enable_partitionwise_join \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentEnablePartitionwiseJoinHotRef, \
									   CurrentPgSession, \
									   PgCurrentEnablePartitionwiseJoinRef))
#define enable_partitionwise_aggregate \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentEnablePartitionwiseAggregateHotRef, \
									   CurrentPgSession, \
									   PgCurrentEnablePartitionwiseAggregateRef))
#define enable_parallel_append \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentEnableParallelAppendHotRef, \
									   CurrentPgSession, \
									   PgCurrentEnableParallelAppendRef))
#define enable_parallel_hash \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentEnableParallelHashHotRef, \
									   CurrentPgSession, \
									   PgCurrentEnableParallelHashRef))
#define enable_partition_pruning \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentEnablePartitionPruningHotRef, \
									   CurrentPgSession, \
									   PgCurrentEnablePartitionPruningRef))
#define enable_presorted_aggregate \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentEnablePresortedAggregateHotRef, \
									   CurrentPgSession, \
									   PgCurrentEnablePresortedAggregateRef))
#define enable_async_append \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentEnableAsyncAppendHotRef, \
									   CurrentPgSession, \
									   PgCurrentEnableAsyncAppendRef))
#define constraint_exclusion \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentConstraintExclusionHotRef, \
									   CurrentPgSession, \
									   PgCurrentConstraintExclusionRef))

/*
 * enable_groupagg is a new upstream GUC (merged from heikki/master, commit
 * e01b23b84e4).  It is not yet relocated to a per-session PgCurrent...Ref
 * accessor like the enable_* GUCs above; left as a plain global for now.
 * ponytail: threaded-relocate it with the rest of the planner enable_* GUCs in
 * a Phase 16 GUC pass if it proves to matter under concurrent sessions.
 */
extern PGDLLIMPORT bool enable_groupagg;

extern double index_pages_fetched(double tuples_fetched, BlockNumber pages,
								  double index_pages, PlannerInfo *root);
extern void cost_seqscan(Path *path, PlannerInfo *root, RelOptInfo *baserel,
						 ParamPathInfo *param_info);
extern void cost_samplescan(Path *path, PlannerInfo *root, RelOptInfo *baserel,
							ParamPathInfo *param_info);
extern void cost_index(IndexPath *path, PlannerInfo *root,
					   double loop_count, bool partial_path);
extern void cost_bitmap_heap_scan(Path *path, PlannerInfo *root, RelOptInfo *baserel,
								  ParamPathInfo *param_info,
								  Path *bitmapqual, double loop_count);
extern void cost_bitmap_and_node(BitmapAndPath *path, PlannerInfo *root);
extern void cost_bitmap_or_node(BitmapOrPath *path, PlannerInfo *root);
extern void cost_bitmap_tree_node(Path *path, Cost *cost, Selectivity *selec);
extern void cost_tidscan(Path *path, PlannerInfo *root,
						 RelOptInfo *baserel, List *tidquals, ParamPathInfo *param_info);
extern void cost_tidrangescan(Path *path, PlannerInfo *root,
							  RelOptInfo *baserel, List *tidrangequals,
							  ParamPathInfo *param_info);
extern void cost_subqueryscan(SubqueryScanPath *path, PlannerInfo *root,
							  RelOptInfo *baserel, ParamPathInfo *param_info,
							  bool trivial_pathtarget);
extern void cost_functionscan(Path *path, PlannerInfo *root,
							  RelOptInfo *baserel, ParamPathInfo *param_info);
extern void cost_valuesscan(Path *path, PlannerInfo *root,
							RelOptInfo *baserel, ParamPathInfo *param_info);
extern void cost_tablefuncscan(Path *path, PlannerInfo *root,
							   RelOptInfo *baserel, ParamPathInfo *param_info);
extern void cost_ctescan(Path *path, PlannerInfo *root,
						 RelOptInfo *baserel, ParamPathInfo *param_info);
extern void cost_namedtuplestorescan(Path *path, PlannerInfo *root,
									 RelOptInfo *baserel, ParamPathInfo *param_info);
extern void cost_resultscan(Path *path, PlannerInfo *root,
							RelOptInfo *baserel, ParamPathInfo *param_info);
extern void cost_recursive_union(Path *runion, Path *nrterm, Path *rterm);
extern void cost_sort(Path *path, PlannerInfo *root,
					  List *pathkeys, int input_disabled_nodes,
					  Cost input_cost, double tuples, int width,
					  Cost comparison_cost, int sort_mem,
					  double limit_tuples);
extern void cost_incremental_sort(Path *path,
								  PlannerInfo *root, List *pathkeys, int presorted_keys,
								  int input_disabled_nodes,
								  Cost input_startup_cost, Cost input_total_cost,
								  double input_tuples, int width, Cost comparison_cost, int sort_mem,
								  double limit_tuples);
extern void cost_append(AppendPath *apath, PlannerInfo *root);
extern void cost_merge_append(Path *path, PlannerInfo *root,
							  List *pathkeys, int n_streams,
							  int input_disabled_nodes,
							  Cost input_startup_cost, Cost input_total_cost,
							  double tuples);
extern void cost_material(Path *path,
						  bool enabled, int input_disabled_nodes,
						  Cost input_startup_cost, Cost input_total_cost,
						  double tuples, int width);
extern void cost_agg(Path *path, PlannerInfo *root,
					 AggStrategy aggstrategy, const AggClauseCosts *aggcosts,
					 int numGroupCols, double numGroups,
					 List *quals,
					 int disabled_nodes,
					 Cost input_startup_cost, Cost input_total_cost,
					 double input_tuples, double input_width);
extern void cost_windowagg(Path *path, PlannerInfo *root,
						   List *windowFuncs, WindowClause *winclause,
						   int input_disabled_nodes,
						   Cost input_startup_cost, Cost input_total_cost,
						   double input_tuples);
extern void cost_group(Path *path, PlannerInfo *root,
					   int numGroupCols, double numGroups,
					   List *quals,
					   int input_disabled_nodes,
					   Cost input_startup_cost, Cost input_total_cost,
					   double input_tuples);
extern void initial_cost_nestloop(PlannerInfo *root,
								  JoinCostWorkspace *workspace,
								  JoinType jointype, uint64 enable_mask,
								  Path *outer_path, Path *inner_path,
								  JoinPathExtraData *extra);
extern void final_cost_nestloop(PlannerInfo *root, NestPath *path,
								JoinCostWorkspace *workspace,
								JoinPathExtraData *extra);
extern void initial_cost_mergejoin(PlannerInfo *root,
								   JoinCostWorkspace *workspace,
								   JoinType jointype,
								   List *mergeclauses,
								   Path *outer_path, Path *inner_path,
								   List *outersortkeys, List *innersortkeys,
								   int outer_presorted_keys,
								   JoinPathExtraData *extra);
extern void final_cost_mergejoin(PlannerInfo *root, MergePath *path,
								 JoinCostWorkspace *workspace,
								 JoinPathExtraData *extra);
extern void initial_cost_hashjoin(PlannerInfo *root,
								  JoinCostWorkspace *workspace,
								  JoinType jointype,
								  List *hashclauses,
								  Path *outer_path, Path *inner_path,
								  JoinPathExtraData *extra,
								  bool parallel_hash);
extern void final_cost_hashjoin(PlannerInfo *root, HashPath *path,
								JoinCostWorkspace *workspace,
								JoinPathExtraData *extra);
extern void cost_gather(GatherPath *path, PlannerInfo *root,
						RelOptInfo *rel, ParamPathInfo *param_info, double *rows);
extern void cost_gather_merge(GatherMergePath *path, PlannerInfo *root,
							  RelOptInfo *rel, ParamPathInfo *param_info,
							  int input_disabled_nodes,
							  Cost input_startup_cost, Cost input_total_cost,
							  double *rows);
extern void cost_subplan(PlannerInfo *root, SubPlan *subplan, Plan *plan);
extern void cost_qual_eval(QualCost *cost, List *quals, PlannerInfo *root);
extern void cost_qual_eval_node(QualCost *cost, Node *qual, PlannerInfo *root);
extern void compute_semi_anti_join_factors(PlannerInfo *root,
										   RelOptInfo *joinrel,
										   RelOptInfo *outerrel,
										   RelOptInfo *innerrel,
										   JoinType jointype,
										   SpecialJoinInfo *sjinfo,
										   List *restrictlist,
										   SemiAntiJoinFactors *semifactors);
extern void set_baserel_size_estimates(PlannerInfo *root, RelOptInfo *rel);
extern double get_parameterized_baserel_size(PlannerInfo *root,
											 RelOptInfo *rel,
											 List *param_clauses);
extern double get_parameterized_joinrel_size(PlannerInfo *root,
											 RelOptInfo *rel,
											 Path *outer_path,
											 Path *inner_path,
											 SpecialJoinInfo *sjinfo,
											 List *restrict_clauses);
extern void set_joinrel_size_estimates(PlannerInfo *root, RelOptInfo *rel,
									   RelOptInfo *outer_rel,
									   RelOptInfo *inner_rel,
									   SpecialJoinInfo *sjinfo,
									   List *restrictlist);
extern void set_subquery_size_estimates(PlannerInfo *root, RelOptInfo *rel);
extern void set_function_size_estimates(PlannerInfo *root, RelOptInfo *rel);
extern void set_values_size_estimates(PlannerInfo *root, RelOptInfo *rel);
extern void set_cte_size_estimates(PlannerInfo *root, RelOptInfo *rel,
								   double cte_rows);
extern void set_tablefunc_size_estimates(PlannerInfo *root, RelOptInfo *rel);
extern void set_namedtuplestore_size_estimates(PlannerInfo *root, RelOptInfo *rel);
extern void set_result_size_estimates(PlannerInfo *root, RelOptInfo *rel);
extern void set_foreign_size_estimates(PlannerInfo *root, RelOptInfo *rel);
extern PathTarget *set_pathtarget_cost_width(PlannerInfo *root, PathTarget *target);
extern double compute_bitmap_pages(PlannerInfo *root, RelOptInfo *baserel,
								   Path *bitmapqual, double loop_count,
								   Cost *cost_p, double *tuples_p);
extern double compute_gather_rows(Path *path);

#endif							/* COST_H */
