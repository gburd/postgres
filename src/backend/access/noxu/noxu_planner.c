/*
 * noxu_planner.c
 *		Query planner integration for Noxu columnar storage
 *
 * This module implements planner hooks that inform PostgreSQL's optimizer
 * about the characteristics of Noxu's columnar storage, enabling better
 * query plans for workloads that benefit from column projection.
 *
 * Key optimizations:
 * - Reduce I/O cost for sequential scans that access few columns
 * - Add CPU cost for decompression of compressed column data
 * - Prefer index-only scans when column projection is beneficial
 * - Annotate relations with columnar access statistics
 *
 * Copyright (c) 2019-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/backend/access/noxu/noxu_planner.c
 */
#include "postgres.h"

#include "access/noxu_internal.h"
#include "access/noxu_planner.h"
#include "access/noxu_stats.h"
#include "access/table.h"
#include "catalog/indexing.h"
#include "catalog/pg_am.h"
#include "catalog/pg_statistic.h"
#include "nodes/pathnodes.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/plancat.h"
#include "optimizer/planmain.h"
#include "utils/array.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/selfuncs.h"
#include "utils/syscache.h"

/* Reference to noxuam_methods from noxu_handler.c */
extern const TableAmRoutine noxuam_methods;

/* Saved hook pointer */
static build_simple_rel_hook_type prev_build_simple_rel_hook = NULL;

/* Forward declarations */
static void noxu_build_simple_rel(PlannerInfo *root, RelOptInfo *rel,
								   RangeTblEntry *rte);

static bool is_noxu_relation(Relation relation);
static NoxuRelStats *create_noxu_rel_stats(PlannerInfo *root, RelOptInfo *rel,
											  Relation relation);
static double calculate_column_selectivity(Bitmapset *accessed_columns, int natts);

/*
 * Initialize Noxu planner hooks.
 * Called when the noxu table AM module is loaded.
 */
void
noxu_planner_init(void)
{
	/* Save previous hook (for chaining) */
	prev_build_simple_rel_hook = build_simple_rel_hook;

	/* Install our hooks */
	build_simple_rel_hook = noxu_build_simple_rel;
	analyze_store_custom_stats_hook = noxu_analyze_store_compression_stats;

	elog(DEBUG1, "Noxu planner hooks initialized");
}

/*
 * Cleanup Noxu planner hooks.
 * Called when the noxu table AM module is unloaded.
 */
void
noxu_planner_fini(void)
{
	/* Restore previous hooks */
	build_simple_rel_hook = prev_build_simple_rel_hook;
	analyze_store_custom_stats_hook = NULL;

	elog(DEBUG1, "Noxu planner hooks removed");
}

/*
 * build_simple_rel hook - annotate Noxu relations with columnar metadata.
 *
 * This hook is called during query planning when the planner builds
 * information about base relations. For Noxu tables, we:
 * 1. Identify which columns are accessed in the query
 * 2. Calculate column selectivity (fraction of columns accessed)
 * 3. Store columnar statistics in rel->fdw_private for later use
 */
static void
noxu_build_simple_rel(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	Relation	relation;

	/* Chain to previous hook if exists */
	if (prev_build_simple_rel_hook)
		prev_build_simple_rel_hook(root, rel, rte);

	/* Only process base relations (not joins, subqueries, etc.) */
	if (rel->reloptkind != RELOPT_BASEREL)
		return;

	/* Skip non-relation RTEs (VALUES lists, subqueries, functions, CTEs) */
	if (rte->rtekind != RTE_RELATION)
		return;

	/* Open the relation to check if it's an Noxu table */
	relation = table_open(rte->relid, NoLock);

	if (is_noxu_relation(relation))
	{
		NoxuRelStats *stats;

		/* Create and populate columnar statistics */
		stats = create_noxu_rel_stats(root, rel, relation);

		/* Store in rel->fdw_private for use by other hooks */
		rel->fdw_private = stats;

		elog(DEBUG2, "Noxu relation %s: %d/%d columns accessed (%.1f%% selectivity)",
			 RelationGetRelationName(relation),
			 bms_num_members(stats->accessed_columns),
			 stats->natts,
			 stats->column_selectivity * 100.0);
	}

	table_close(relation, NoLock);
}

/*
 * Retrieve columnar statistics for a relation from the current planner context.
 *
 * This function is called by noxuam_relation_estimate_size() to get column
 * access patterns detected during query planning. Returns NULL if not called
 * within a planner context or if no stats available.
 *
 * Note: This relies on the statistics being stored in rel->fdw_private by
 * noxu_get_relation_info() earlier in planning.
 */
NoxuRelStats *
noxu_get_relation_stats(Oid relid)
{
	NoxuRelStats *stats;
	double		live_tuples;
	double		dead_tuples;
	double		comp_ratio;

	if (!nxstats_is_fresh(relid, noxu_stats_freshness_threshold))
		return NULL;

	stats = (NoxuRelStats *) palloc0(sizeof(NoxuRelStats));

	if (nxstats_get_tuple_counts(relid, &live_tuples, &dead_tuples))
	{
		stats->has_columnar_stats = true;
	}

	if (nxstats_get_compression_ratio(relid, &comp_ratio))
	{
		stats->avg_compression_ratio = comp_ratio;
		stats->has_columnar_stats = true;
	}
	else
	{
		stats->avg_compression_ratio = NOXU_DEFAULT_COMPRESSION_RATIO;
	}

	if (!stats->has_columnar_stats)
	{
		pfree(stats);
		return NULL;
	}

	return stats;
}

/*
 * Calculate cost adjustment factors for columnar access.
 *
 * Given column selectivity and compression ratio, compute:
 * - I/O reduction factor (how much less data to read)
 * - CPU cost multiplier (decompression overhead)
 *
 * These can be applied in noxuam_relation_estimate_size().
 */
void
noxu_calculate_cost_factors(double column_selectivity,
							  double compression_ratio,
							  double *io_factor_out,
							  double *cpu_factor_out)
{
	double		io_reduction_factor;

	(void) compression_ratio;

	/*
	 * I/O reduction: accessing fewer columns means less data to read.
	 * However, TID tree and metadata add fixed overhead (~20%).
	 *
	 * Formula: io_factor = 0.2 + 0.8 * selectivity
	 * Example: 50% of columns → 60% of I/O, not 50%
	 */
	io_reduction_factor = 0.2 + (0.8 * column_selectivity);

	/*
	 * If accessing most columns (>= 80%), don't apply reduction.
	 * Columnar overhead may negate benefits.
	 */
	if (column_selectivity >= NOXU_MIN_COLUMN_SELECTIVITY)
		io_reduction_factor = 1.0;

	*io_factor_out = io_reduction_factor;

	/*
	 * CPU cost: decompression adds overhead.
	 * Higher compression → more CPU, but also less I/O (already factored).
	 */
	*cpu_factor_out = 1.0 + NOXU_DECOMPRESSION_CPU_FACTOR;
}

/*
 * Check if a relation uses the Noxu table access method.
 */
static bool
is_noxu_relation(Relation relation)
{
	/*
	 * Simple check: compare the table AM OID against known Noxu AM OID.
	 * This is more efficient than string comparison.
	 *
	 * If Noxu OID is not known at compile time, we'd need to look it up,
	 * but since we're part of the noxu module, we know our own OID.
	 */
	return relation->rd_tableam == &noxuam_methods;
}

/*
 * Create columnar statistics for an Noxu relation.
 *
 * This analyzes the query to determine which columns are accessed,
 * calculates column selectivity, and retrieves any stored statistics
 * from prior ANALYZE runs.
 */
static NoxuRelStats *
create_noxu_rel_stats(PlannerInfo *root, RelOptInfo *rel, Relation relation)
{
	NoxuRelStats *stats;
	int			natts;

	(void) root;

	stats = (NoxuRelStats *) palloc0(sizeof(NoxuRelStats));

	/* Get number of columns */
	natts = RelationGetNumberOfAttributes(relation);
	stats->natts = natts;

	/* Initialize with empty column set */
	stats->accessed_columns = NULL;

	/*
	 * Extract columns accessed in target list and quals.
	 * Note: This gives us an upper bound; actual access may be less
	 * if the executor can push down projections.
	 */
	if (rel->reltarget)
	{
		/* Pull columns from target list */
		pull_varattnos((Node *) rel->reltarget->exprs,
					   rel->relid,
					   &stats->accessed_columns);
	}

	/* Pull columns from base restriction quals */
	if (rel->baserestrictinfo)
	{
		ListCell   *lc;

		foreach(lc, rel->baserestrictinfo)
		{
			RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

			pull_varattnos((Node *) rinfo->clause,
						   rel->relid,
						   &stats->accessed_columns);
		}
	}

	/*
	 * If no columns identified (shouldn't happen in practice),
	 * assume all columns accessed.
	 */
	if (bms_is_empty(stats->accessed_columns))
	{
		int			i;

		for (i = 1; i <= natts; i++)
			stats->accessed_columns = bms_add_member(stats->accessed_columns, i);
	}

	/* Calculate column selectivity */
	stats->column_selectivity = calculate_column_selectivity(
		stats->accessed_columns, natts);

	/*
	 * Retrieve per-column compression ratios from pg_statistic.
	 * Compute a weighted average based on accessed columns.
	 */
	{
		Oid			relid = RelationGetRelid(relation);
		double		weighted_ratio;

		weighted_ratio = noxu_get_weighted_compression_ratio(
			relid, stats->accessed_columns, natts);

		if (weighted_ratio > 0.0)
		{
			stats->avg_compression_ratio = weighted_ratio;
			stats->has_columnar_stats = true;
		}
		else
		{
			stats->avg_compression_ratio = NOXU_DEFAULT_COMPRESSION_RATIO;
			stats->has_columnar_stats = false;
		}
	}

	return stats;
}

/*
 * Calculate column selectivity (fraction of columns accessed).
 *
 * This is the ratio of accessed columns to total columns,
 * accounting for system columns.
 */
static double
calculate_column_selectivity(Bitmapset *accessed_columns, int natts)
{
	int			num_accessed;

	if (natts <= 0)
		return 1.0;

	num_accessed = bms_num_members(accessed_columns);

	/* Selectivity is clamped to [0, 1] */
	return Min(1.0, (double) num_accessed / (double) natts);
}

/*
 * Compute and store Noxu compression statistics after ANALYZE.
 *
 * Called from do_analyze_rel() after standard statistics have been stored.
 * Iterates through all analyzed columns, computes compression statistics
 * from the sampled data, and stores them via noxu_store_column_stats().
 */
void
noxu_analyze_store_compression_stats(Relation onerel, int attr_cnt,
									   VacAttrStats **vacattrstats)
{
	Oid			relid = RelationGetRelid(onerel);
	TupleDesc	tupdesc = RelationGetDescr(onerel);
	int			i;

	/* Only process Noxu tables */
	if (!is_noxu_relation(onerel))
		return;

	for (i = 0; i < attr_cnt; i++)
	{
		VacAttrStats *stats = vacattrstats[i];
		AttrNumber	attnum = stats->tupattnum;
		Form_pg_attribute attr;
		float4		compression_ratio;
		float4		null_frac;
		float4		avg_width_compressed;
		float4		avg_width_uncompressed;

		/* Skip if we don't have valid statistics */
		if (!stats->stats_valid)
			continue;

		/* Get attribute metadata */
		if (attnum <= 0 || attnum > tupdesc->natts)
			continue;

		attr = TupleDescAttr(tupdesc, attnum - 1);

		/*
		 * Use the already-computed statistics from ANALYZE.
		 * stats->stawidth is the average width of non-null values.
		 * stats->stanullfrac is the fraction of NULL values.
		 */
		null_frac = stats->stanullfrac;
		avg_width_uncompressed = stats->stawidth;

		/* Skip if width is invalid or zero */
		if (avg_width_uncompressed <= 0)
		{
			if (attr->attlen > 0)
				avg_width_uncompressed = attr->attlen;
			else
				avg_width_uncompressed = 32;	/* default estimate */
		}

		/*
		 * Estimate compression ratio based on data type.
		 * For Noxu columnar storage with LZ4 compression:
		 * - Fixed-width types (int, float): ~50% compression
		 * - Variable-length types (text, bytea): ~40% compression
		 * These are conservative estimates; actual compression varies.
		 */
		if (attr->attlen > 0)
		{
			/* Fixed-width types */
			avg_width_compressed = avg_width_uncompressed * 0.5;
		}
		else
		{
			/* Variable-length types */
			avg_width_compressed = avg_width_uncompressed * 0.4;
		}

		/*
		 * Ensure we don't claim compression for very small values
		 * where overhead might dominate.
		 */
		if (avg_width_compressed < 1.0)
			avg_width_compressed = 1.0;

		compression_ratio = avg_width_uncompressed / avg_width_compressed;

		/* Store the compression statistics */
		noxu_store_column_stats(relid, attnum,
								 compression_ratio, null_frac,
								 avg_width_compressed, avg_width_uncompressed);
	}
}

/*
 * Store per-column compression statistics into pg_statistic.
 *
 * Called during ANALYZE for each column of an Noxu table.
 * We find an unused stakind slot in the existing pg_statistic row
 * and write our custom STATISTIC_KIND_NOXU_COMPRESSION data there.
 *
 * stanumbers[] layout:
 *   [0] = compression_ratio
 *   [1] = null_frac
 *   [2] = avg_width_compressed
 *   [3] = avg_width_uncompressed
 */
void
noxu_store_column_stats(Oid relid, AttrNumber attnum,
						 float4 compression_ratio, float4 null_frac,
						 float4 avg_width_compressed,
						 float4 avg_width_uncompressed)
{
	HeapTuple	oldtup;
	HeapTuple	newtup;
	Relation	sd;
	Datum		values[Natts_pg_statistic];
	bool		nulls[Natts_pg_statistic];
	bool		replaces[Natts_pg_statistic];
	float4		stanumbers[4];
	int			slot_idx;
	Datum		arry;

	oldtup = SearchSysCache3(STATRELATTINH,
							 ObjectIdGetDatum(relid),
							 Int16GetDatum(attnum),
							 BoolGetDatum(false));

	if (!HeapTupleIsValid(oldtup))
	{
		elog(DEBUG2, "Noxu: no pg_statistic row for rel %u att %d, "
			 "skipping compression stats", relid, attnum);
		return;
	}

	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));
	memset(replaces, false, sizeof(replaces));

	/*
	 * Find a free stakind slot, or one already holding our kind.
	 * Slots are stakind1..stakind5 (attribute indices 6..10 in the
	 * catalog, but we access them via the Form_pg_statistic struct).
	 */
	{
		Form_pg_statistic form = (Form_pg_statistic) GETSTRUCT(oldtup);
		int16		kinds[STATISTIC_NUM_SLOTS];

		kinds[0] = form->stakind1;
		kinds[1] = form->stakind2;
		kinds[2] = form->stakind3;
		kinds[3] = form->stakind4;
		kinds[4] = form->stakind5;

		slot_idx = -1;
		for (int i = 0; i < STATISTIC_NUM_SLOTS; i++)
		{
			if (kinds[i] == STATISTIC_KIND_NOXU_COMPRESSION)
			{
				slot_idx = i;
				break;
			}
		}

		if (slot_idx < 0)
		{
			for (int i = 0; i < STATISTIC_NUM_SLOTS; i++)
			{
				if (kinds[i] == 0)
				{
					slot_idx = i;
					break;
				}
			}
		}
	}

	if (slot_idx < 0)
	{
		elog(DEBUG2, "Noxu: no free stakind slot for rel %u att %d",
			 relid, attnum);
		ReleaseSysCache(oldtup);
		return;
	}

	stanumbers[0] = compression_ratio;
	stanumbers[1] = null_frac;
	stanumbers[2] = avg_width_compressed;
	stanumbers[3] = avg_width_uncompressed;

	arry = PointerGetDatum(construct_array((Datum *) stanumbers, 4,
										   FLOAT4OID,
										   sizeof(float4), true, TYPALIGN_INT));

	/*
	 * Set the stakindN, staopN, stacollN, stanumbersN for the chosen slot.
	 * Attribute numbers in pg_statistic catalog:
	 *   stakind1 = Anum_pg_statistic_stakind1 (slot_idx 0)
	 *   stanumbers1 = Anum_pg_statistic_stanumbers1 (slot_idx 0)
	 * Each subsequent slot is offset by 1.
	 */
	replaces[Anum_pg_statistic_stakind1 - 1 + slot_idx] = true;
	values[Anum_pg_statistic_stakind1 - 1 + slot_idx] =
		Int16GetDatum(STATISTIC_KIND_NOXU_COMPRESSION);

	replaces[Anum_pg_statistic_staop1 - 1 + slot_idx] = true;
	values[Anum_pg_statistic_staop1 - 1 + slot_idx] =
		ObjectIdGetDatum(InvalidOid);

	replaces[Anum_pg_statistic_stacoll1 - 1 + slot_idx] = true;
	values[Anum_pg_statistic_stacoll1 - 1 + slot_idx] =
		ObjectIdGetDatum(InvalidOid);

	replaces[Anum_pg_statistic_stanumbers1 - 1 + slot_idx] = true;
	values[Anum_pg_statistic_stanumbers1 - 1 + slot_idx] = arry;

	sd = table_open(StatisticRelationId, RowExclusiveLock);

	newtup = heap_modify_tuple(oldtup, RelationGetDescr(sd),
							   values, nulls, replaces);
	CatalogTupleUpdate(sd, &newtup->t_self, newtup);

	heap_freetuple(newtup);
	ReleaseSysCache(oldtup);
	table_close(sd, RowExclusiveLock);

	elog(DEBUG2, "Noxu: stored compression stats for rel %u att %d: "
		 "ratio=%.2f null_frac=%.2f avg_compressed=%.0f avg_uncompressed=%.0f",
		 relid, attnum, compression_ratio, null_frac,
		 avg_width_compressed, avg_width_uncompressed);
}

/*
 * Retrieve per-column compression statistics from pg_statistic.
 * Returns true if stats were found, false otherwise.
 */
bool
noxu_get_column_stats(Oid relid, AttrNumber attnum,
					   NoxuColumnStats *stats)
{
	HeapTuple	tuple;
	AttStatsSlot sslot;
	bool		found = false;

	memset(stats, 0, sizeof(NoxuColumnStats));
	stats->attnum = attnum;
	stats->has_stats = false;

	tuple = SearchSysCache3(STATRELATTINH,
							ObjectIdGetDatum(relid),
							Int16GetDatum(attnum),
							BoolGetDatum(false));

	if (!HeapTupleIsValid(tuple))
		return false;

	if (get_attstatsslot(&sslot, tuple,
						 STATISTIC_KIND_NOXU_COMPRESSION,
						 InvalidOid,
						 ATTSTATSSLOT_NUMBERS))
	{
		if (sslot.nnumbers >= 4)
		{
			stats->compression_ratio = sslot.numbers[0];
			stats->null_frac = sslot.numbers[1];
			stats->avg_width_compressed = sslot.numbers[2];
			stats->avg_width_uncompressed = sslot.numbers[3];
			stats->has_stats = true;
			found = true;
		}
		free_attstatsslot(&sslot);
	}

	ReleaseSysCache(tuple);
	return found;
}

/*
 * Compute a weighted average compression ratio for accessed columns.
 *
 * For each accessed column with stored Noxu stats, weight the
 * compression ratio by the column's uncompressed width. Columns
 * without stats are excluded. Returns 0.0 if no stats found.
 */
double
noxu_get_weighted_compression_ratio(Oid relid,
									 Bitmapset *accessed_columns,
									 int natts)
{
	double		total_weight = 0.0;
	double		weighted_sum = 0.0;
	int			attnum;

	attnum = -1;
	while ((attnum = bms_next_member(accessed_columns, attnum)) >= 0)
	{
		NoxuColumnStats col_stats;

		/* bitmapset from pull_varattnos is 1-based */
		if (attnum < 1 || attnum > natts)
			continue;

		if (noxu_get_column_stats(relid, (AttrNumber) attnum,
								   &col_stats))
		{
			double		weight = col_stats.avg_width_uncompressed;

			if (weight <= 0.0)
				weight = 1.0;

			weighted_sum += col_stats.compression_ratio * weight;
			total_weight += weight;
		}
	}

	if (total_weight <= 0.0)
		return 0.0;

	return weighted_sum / total_weight;
}
