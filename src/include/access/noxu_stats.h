/**
 * @file noxu_stats.h
 * @brief Opportunistic statistics collection for Noxu columnar storage.
 *
 * Tracks tuple counts, dead tuples, null fractions, and compression
 * ratios during normal DML and scan operations, so the planner has
 * fresh estimates even between ANALYZE runs.
 *
 * @par Design
 * Statistics are stored per-relation in a backend-local hash table
 * (keyed by OID).  INSERT/DELETE callbacks bump tuple counters cheaply.
 * Sequential scans sample every Nth tuple (controlled by the
 * noxu.stats_sample_rate GUC) to update live/dead counts and
 * per-column null fractions.  The planner reads these counters via
 * nxstats_get_*() and, when fresh enough, uses them in preference to
 * stale pg_class.reltuples.
 *
 * @par Thread Safety
 * The hash table is backend-local; no locking is needed.  Each backend
 * maintains its own view; stats converge after a few scans.
 *
 * @par GUC Parameters
 * - noxu.enable_opportunistic_stats (bool, default on)
 * - noxu.stats_sample_rate (int, default 100, range 1-10000)
 * - noxu.stats_freshness_threshold (int, default 3600, range 1-86400)
 *
 * Copyright (c) 2019-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/noxu_stats.h
 */
#ifndef NOXU_STATS_H
#define NOXU_STATS_H

#include "c.h"					/* for int64, bool, uint32, etc. */
#include "utils/relcache.h"
#include "utils/timestamp.h"

/**
 * @brief Maximum number of columns tracked for per-column null fractions.
 *
 * Tables wider than this only track the first NXSTATS_MAX_TRACKED_COLS
 * columns.  This bounds memory usage per hash table entry.
 */
#define NXSTATS_MAX_TRACKED_COLS	64

/**
 * @brief Per-relation opportunistic statistics.
 *
 * Stored in a backend-local hash table keyed by relation OID.  Tuple
 * counts from DML operations are maintained as deltas; scan-based
 * counts provide an independent cross-check.
 *
 * @param relid               Hash key: relation OID.
 * @param tuples_inserted     Cumulative inserts since last ANALYZE.
 * @param tuples_deleted      Cumulative deletes since last ANALYZE.
 * @param scan_live_tuples    Live tuples observed during the most recent scan.
 * @param scan_dead_tuples    Dead tuples observed during the most recent scan.
 * @param scan_count_valid    True if scan-based counts are populated.
 * @param natts_tracked       Number of columns with null-fraction tracking.
 * @param col_null_count      Per-column count of NULLs observed during sampling.
 * @param col_total_count     Per-column count of tuples sampled.
 * @param compressed_bytes    Accumulated compressed page bytes (sampling).
 * @param uncompressed_bytes  Accumulated uncompressed page bytes (sampling).
 * @param compression_valid   True if compression ratio estimate is populated.
 * @param last_dml_update     Timestamp of last DML-based update.
 * @param last_scan_update    Timestamp of last scan-based update.
 */
typedef struct NoxuOpStats
{
	Oid			relid;			/* hash key */

	/* Tuple counts from DML tracking */
	int64		tuples_inserted;
	int64		tuples_deleted;

	/* Tuple count observed during most recent scan */
	int64		scan_live_tuples;
	int64		scan_dead_tuples;
	bool		scan_count_valid;

	/* Per-column null counts (from scan sampling) */
	int			natts_tracked;
	int64		col_null_count[NXSTATS_MAX_TRACKED_COLS];
	int64		col_total_count[NXSTATS_MAX_TRACKED_COLS];

	/* Compression ratio estimate (from scan sampling) */
	double		compressed_bytes;
	double		uncompressed_bytes;
	bool		compression_valid;

	/* When these stats were last updated */
	TimestampTz last_dml_update;
	TimestampTz last_scan_update;
} NoxuOpStats;

/**
 * @name GUC Variables
 * @{
 */
/** @brief Enable/disable opportunistic statistics collection (default: on). */
extern bool noxu_enable_opportunistic_stats;
/** @brief Scan sampling rate: every Nth tuple is sampled (default: 100). */
extern int noxu_stats_sample_rate;
/** @brief Seconds before opportunistic stats are considered stale (default: 3600). */
extern int noxu_stats_freshness_threshold;
/** @} */

/** @brief Initialize GUC variables and hash table (called from _PG_init). */
extern void noxu_stats_init(void);

/**
 * @name DML Tracking
 * @brief Called from noxu_handler.c DML callbacks.
 * @{
 */
/** @brief Record that @a ntuples rows were inserted into @a relid. */
extern void nxstats_count_insert(Oid relid, int ntuples);
/** @brief Record that a row was deleted from @a relid. */
extern void nxstats_count_delete(Oid relid);
/** @} */

/**
 * @name Scan Tracking
 * @brief Called from noxu_handler.c sequential scan callbacks.
 * @{
 */
/** @brief Begin tracking statistics for a sequential scan of @a relid. */
extern void nxstats_scan_begin(Oid relid);
/** @brief Observe a single tuple during scan sampling. */
extern void nxstats_scan_observe_tuple(Oid relid, bool is_live,
									   bool *isnulls, int natts);
/** @brief Finalize scan-based statistics for @a relid. */
extern void nxstats_scan_end(Oid relid);
/** @} */

/**
 * @name Planner Access
 * @brief Called from noxu_planner.c during cost estimation.
 * @{
 */

/**
 * @brief Retrieve estimated live and dead tuple counts.
 * @param relid        Relation OID.
 * @param live_tuples  Output: estimated live tuple count.
 * @param dead_tuples  Output: estimated dead tuple count.
 * @return true if counts are available and fresh.
 */
extern bool nxstats_get_tuple_counts(Oid relid,
									 double *live_tuples,
									 double *dead_tuples);

/**
 * @brief Retrieve estimated null fraction for a column.
 * @param relid     Relation OID.
 * @param attnum    Attribute number (1-based).
 * @param null_frac Output: estimated null fraction (0.0-1.0).
 * @return true if the estimate is available and fresh.
 */
extern bool nxstats_get_null_frac(Oid relid, AttrNumber attnum,
								  float4 *null_frac);

/**
 * @brief Retrieve estimated compression ratio.
 * @param relid  Relation OID.
 * @param ratio  Output: estimated compression ratio.
 * @return true if the estimate is available and fresh.
 */
extern bool nxstats_get_compression_ratio(Oid relid,
										  double *ratio);

/**
 * @brief Check whether opportunistic stats are fresh enough to use.
 * @param relid           Relation OID.
 * @param threshold_secs  Maximum age in seconds.
 * @return true if stats were updated within @a threshold_secs.
 */
extern bool nxstats_is_fresh(Oid relid, int threshold_secs);
/** @} */

#endif							/* NOXU_STATS_H */
