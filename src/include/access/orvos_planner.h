/*
 * orvos_planner.h
 *		Planner integration for Orvos columnar table access method
 *
 * This module provides planner hooks to inform PostgreSQL's query planner
 * about Orvos's columnar storage characteristics, enabling better cost
 * estimation for queries that benefit from column projection.
 *
 * Copyright (c) 2019-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/orvos_planner.h
 */
#ifndef ORVOS_PLANNER_H
#define ORVOS_PLANNER_H

#include "commands/vacuum.h"
#include "nodes/pathnodes.h"
#include "optimizer/planmain.h"
#include "utils/relcache.h"

/*
 * Custom stakind for Orvos columnar compression statistics.
 * Stored in pg_statistic slots during ANALYZE.
 * Per pg_statistic.h, private-use kind codes should be in 10000-30000.
 *
 * stanumbers[] layout for this slot:
 *   [0] = compression_ratio (uncompressed_size / compressed_size)
 *   [1] = null_fraction (fraction of NULL values in this column)
 *   [2] = avg_width_compressed (average byte width after compression)
 *   [3] = avg_width_uncompressed (average byte width before compression)
 */
#define STATISTIC_KIND_ORVOS_COMPRESSION 10001

/*
 * Estimated compression ratio for Orvos columnar data.
 * This is a conservative estimate; actual ratios vary by column type:
 * - Text/varchar: 3-5x with zstd
 * - Numeric: 2-4x
 * - Timestamps: 2-3x
 * - Already compressed data: ~1x
 *
 * We use 2.5x as a reasonable average for mixed workloads.
 */
#define ORVOS_DEFAULT_COMPRESSION_RATIO 2.5

/*
 * CPU cost multiplier for decompression.
 * Reading compressed data incurs CPU overhead for decompression.
 * This factor is multiplied by cpu_tuple_cost to estimate decompression cost.
 *
 * Benchmarking suggests zstd decompression adds ~0.2-0.5x tuple processing cost.
 */
#define ORVOS_DECOMPRESSION_CPU_FACTOR 0.3

/*
 * Minimum column selectivity for cost reduction.
 * If a query accesses fewer than this fraction of columns, apply
 * columnar optimization. Below this threshold, overhead may dominate.
 */
#define ORVOS_MIN_COLUMN_SELECTIVITY 0.8

/*
 * Per-column compression statistics collected during ANALYZE.
 */
typedef struct OrvosColumnStats
{
	AttrNumber	attnum;
	float4		compression_ratio;
	float4		avg_width_compressed;
	float4		avg_width_uncompressed;
	float4		null_frac;
	bool		has_stats;
} OrvosColumnStats;

/*
 * Per-relation columnar statistics stored during ANALYZE.
 * These are cached in RelOptInfo->fdw_private for Orvos tables.
 */
typedef struct OrvosRelStats
{
	/* Number of columns in the table */
	int			natts;

	/* Bitmap of columns accessed in this query (pulled from target/qual) */
	Bitmapset  *accessed_columns;

	/* Fraction of columns accessed (accessed / total) */
	double		column_selectivity;

	/* Average compression ratio across all columns (default if unknown) */
	double		avg_compression_ratio;

	/* Whether this table has been ANALYZEd with columnar stats */
	bool		has_columnar_stats;

	/* Per-column stats retrieved from pg_statistic (NULL if unavailable) */
	OrvosColumnStats *col_stats;
	int			num_col_stats;
} OrvosRelStats;

/* Initialize planner hooks for Orvos (called at module load) */
extern void orvos_planner_init(void);

/* Cleanup planner hooks for Orvos (called at module unload) */
extern void orvos_planner_fini(void);

/* Retrieve columnar statistics for a relation (called during cost estimation) */
extern OrvosRelStats *orvos_get_relation_stats(Oid relid);

/* Calculate cost adjustment factors for columnar access */
extern void orvos_calculate_cost_factors(double column_selectivity,
										 double compression_ratio,
										 double *io_factor_out,
										 double *cpu_factor_out);

/* Compute and store Orvos compression statistics after ANALYZE */
extern void orvos_analyze_store_compression_stats(Relation onerel, int attr_cnt,
												   VacAttrStats **vacattrstats);

/* Store per-column compression stats into pg_statistic during ANALYZE */
extern void orvos_store_column_stats(Oid relid, AttrNumber attnum,
									 float4 compression_ratio,
									 float4 null_frac,
									 float4 avg_width_compressed,
									 float4 avg_width_uncompressed);

/* Retrieve per-column compression stats from pg_statistic */
extern bool orvos_get_column_stats(Oid relid, AttrNumber attnum,
								   OrvosColumnStats *stats);

/*
 * Compute weighted compression ratio for accessed columns.
 * Looks up per-column stats from pg_statistic and averages them
 * weighted by uncompressed width for the accessed column set.
 */
extern double orvos_get_weighted_compression_ratio(Oid relid,
												   Bitmapset *accessed_columns,
												   int natts);

#endif							/* ORVOS_PLANNER_H */
