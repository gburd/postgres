/**
 * @file noxu_planner.h
 * @brief Planner integration for Noxu columnar table access method.
 *
 * This module provides planner hooks to inform PostgreSQL's query planner
 * about Noxu's columnar storage characteristics, enabling better cost
 * estimation for queries that benefit from column projection.
 *
 * @par Cost Model Adjustments
 * The hooks adjust I/O costs based on:
 * - Column selectivity (fraction of columns accessed).
 * - Compression ratio (from pg_statistic or default estimate).
 * - Decompression CPU overhead factor.
 *
 * @par Statistics Storage
 * Per-column compression statistics are stored in pg_statistic using
 * custom stakind STATISTIC_KIND_NOXU_COMPRESSION (10001).
 *
 * Copyright (c) 2019-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/noxu_planner.h
 */
#ifndef NOXU_PLANNER_H
#define NOXU_PLANNER_H

#include "c.h"					/* for int, bool, float4, etc. */
#include "commands/vacuum.h"
#include "nodes/pathnodes.h"
#include "optimizer/planmain.h"
#include "utils/relcache.h"

/**
 * @brief Custom stakind for Noxu columnar compression statistics.
 *
 * Stored in pg_statistic slots during ANALYZE.
 * Per pg_statistic.h, private-use kind codes should be in 10000-30000.
 *
 * @par stanumbers[] layout:
 * - [0] = compression_ratio (uncompressed_size / compressed_size)
 * - [1] = null_fraction (fraction of NULL values in this column)
 * - [2] = avg_width_compressed (average byte width after compression)
 * - [3] = avg_width_uncompressed (average byte width before compression)
 */
#define STATISTIC_KIND_NOXU_COMPRESSION 10001

/**
 * @brief Default estimated compression ratio for Noxu columnar data.
 *
 * Conservative estimate; actual ratios vary by column type:
 * - Text/varchar: 3-5x with zstd
 * - Numeric: 2-4x
 * - Timestamps: 2-3x
 * - Already compressed data: ~1x
 *
 * Used as the fallback when per-column statistics are not available.
 */
#define NOXU_DEFAULT_COMPRESSION_RATIO 2.5

/**
 * @brief CPU cost multiplier for decompression overhead.
 *
 * Multiplied by cpu_tuple_cost to estimate the additional CPU cost of
 * decompressing columnar data.  Benchmarking suggests zstd decompression
 * adds ~0.2-0.5x tuple processing cost.
 */
#define NOXU_DECOMPRESSION_CPU_FACTOR 0.3

/**
 * @brief Minimum column selectivity threshold for columnar cost reduction.
 *
 * If a query accesses fewer than this fraction of columns, the planner
 * applies columnar I/O optimization.  Above this threshold, the
 * per-column B-tree overhead may dominate.
 */
#define NOXU_MIN_COLUMN_SELECTIVITY 0.8

/**
 * @brief Per-column compression statistics from pg_statistic.
 *
 * Populated during ANALYZE and retrieved by the planner for cost
 * estimation.
 *
 * @param attnum                   Attribute number (1-based).
 * @param compression_ratio        Uncompressed / compressed size ratio.
 * @param avg_width_compressed     Average datum width after compression.
 * @param avg_width_uncompressed   Average datum width before compression.
 * @param null_frac                Fraction of NULL values.
 * @param has_stats                True if statistics are available.
 */
typedef struct NoxuColumnStats
{
	AttrNumber	attnum;
	float4		compression_ratio;
	float4		avg_width_compressed;
	float4		avg_width_uncompressed;
	float4		null_frac;
	bool		has_stats;
} NoxuColumnStats;

/**
 * @brief Per-relation columnar statistics for planner cost estimation.
 *
 * Aggregates per-column statistics and query-specific column access
 * information.  Cached in RelOptInfo->fdw_private for Noxu tables.
 *
 * @param natts                  Number of columns in the table.
 * @param accessed_columns       Bitmap of columns needed by the query.
 * @param column_selectivity     Fraction of columns accessed (0.0-1.0).
 * @param avg_compression_ratio  Average compression ratio across columns.
 * @param has_columnar_stats     True if ANALYZE has collected Noxu stats.
 * @param col_stats              Per-column statistics array (may be NULL).
 * @param num_col_stats          Number of entries in col_stats.
 */
typedef struct NoxuRelStats
{
	int			natts;
	Bitmapset  *accessed_columns;
	double		column_selectivity;
	double		avg_compression_ratio;
	bool		has_columnar_stats;
	NoxuColumnStats *col_stats;
	int			num_col_stats;
} NoxuRelStats;

/** @brief Initialize planner hooks for Noxu (called from _PG_init). */
extern void noxu_planner_init(void);

/** @brief Remove planner hooks for Noxu (called at module unload). */
extern void noxu_planner_fini(void);

/**
 * @brief Retrieve columnar statistics for a relation.
 *
 * Looks up per-column compression statistics from pg_statistic and
 * constructs an NoxuRelStats suitable for planner cost estimation.
 *
 * @param relid  OID of the relation.
 * @return Pointer to a palloc'd NoxuRelStats, or NULL if unavailable.
 */
extern NoxuRelStats *noxu_get_relation_stats(Oid relid);

/**
 * @brief Calculate I/O and CPU cost adjustment factors for columnar access.
 *
 * @param column_selectivity  Fraction of columns accessed (0.0-1.0).
 * @param compression_ratio   Estimated compression ratio.
 * @param io_factor_out       Output: I/O cost multiplier.
 * @param cpu_factor_out      Output: CPU cost multiplier (includes decompression).
 */
extern void noxu_calculate_cost_factors(double column_selectivity,
										 double compression_ratio,
										 double *io_factor_out,
										 double *cpu_factor_out);

/**
 * @brief Compute and store Noxu compression statistics after ANALYZE.
 *
 * Called at the end of ANALYZE to measure per-column compression ratios
 * and store them in pg_statistic.
 *
 * @param onerel         The analyzed relation.
 * @param attr_cnt       Number of analyzed attributes.
 * @param vacattrstats   Per-attribute ANALYZE statistics.
 */
extern void noxu_analyze_store_compression_stats(Relation onerel, int attr_cnt,
												   VacAttrStats **vacattrstats);

/**
 * @brief Store per-column compression stats into pg_statistic.
 *
 * @param relid                    Relation OID.
 * @param attnum                   Attribute number (1-based).
 * @param compression_ratio        Uncompressed / compressed size ratio.
 * @param null_frac                Fraction of NULL values.
 * @param avg_width_compressed     Average compressed datum width.
 * @param avg_width_uncompressed   Average uncompressed datum width.
 */
extern void noxu_store_column_stats(Oid relid, AttrNumber attnum,
									 float4 compression_ratio,
									 float4 null_frac,
									 float4 avg_width_compressed,
									 float4 avg_width_uncompressed);

/**
 * @brief Retrieve per-column compression stats from pg_statistic.
 *
 * @param relid   Relation OID.
 * @param attnum  Attribute number (1-based).
 * @param stats   Output: populated with the column's statistics.
 * @return true if statistics were found, false otherwise.
 */
extern bool noxu_get_column_stats(Oid relid, AttrNumber attnum,
								   NoxuColumnStats *stats);

/**
 * @brief Compute weighted compression ratio for a set of accessed columns.
 *
 * Looks up per-column stats from pg_statistic and computes a weighted
 * average compression ratio, where each column's weight is its
 * uncompressed width.
 *
 * @param relid              Relation OID.
 * @param accessed_columns   Bitmap of accessed column attribute numbers.
 * @param natts              Total number of attributes.
 * @return Weighted average compression ratio, or
 *         NOXU_DEFAULT_COMPRESSION_RATIO if no stats are available.
 */
extern double noxu_get_weighted_compression_ratio(Oid relid,
												   Bitmapset *accessed_columns,
												   int natts);

#endif							/* NOXU_PLANNER_H */
