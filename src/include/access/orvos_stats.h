/*
 * orvos_stats.h
 *		Opportunistic statistics collection for Orvos columnar storage
 *
 * Tracks tuple counts, dead tuples, null fractions, and compression
 * ratios during normal DML and scan operations, so the planner has
 * fresh estimates even between ANALYZE runs.
 *
 * Statistics are stored per-relation in a backend-local hash table and
 * reported to pgstat via the standard pgstat_report_* infrastructure.
 *
 * Copyright (c) 2019-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/orvos_stats.h
 */
#ifndef ORVOS_STATS_H
#define ORVOS_STATS_H

#include "postgres.h"
#include "utils/relcache.h"
#include "utils/timestamp.h"

/*
 * Maximum number of columns we track per-column null fractions for.
 * Tables wider than this only track the first OVSTATS_MAX_TRACKED_COLS
 * columns. This bounds memory usage per entry.
 */
#define OVSTATS_MAX_TRACKED_COLS	64

/*
 * Per-relation opportunistic statistics, stored in a backend-local
 * hash table keyed by relation OID.
 *
 * Tuple counts are maintained as deltas from the last known pg_class
 * values, accumulated during DML. Scan-based counts provide an
 * independent cross-check.
 */
typedef struct OrvosOpStats
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
	int64		col_null_count[OVSTATS_MAX_TRACKED_COLS];
	int64		col_total_count[OVSTATS_MAX_TRACKED_COLS];

	/* Compression ratio estimate (from scan sampling) */
	double		compressed_bytes;
	double		uncompressed_bytes;
	bool		compression_valid;

	/* When these stats were last updated */
	TimestampTz last_dml_update;
	TimestampTz last_scan_update;
} OrvosOpStats;

/* GUC variables */
extern bool orvos_enable_opportunistic_stats;
extern int orvos_stats_sample_rate;
extern int orvos_stats_freshness_threshold;

/* Initialization (called from _PG_init) */
extern void orvos_stats_init(void);

/* DML tracking - called from orvos_handler.c DML callbacks */
extern void ovstats_count_insert(Oid relid, int ntuples);
extern void ovstats_count_delete(Oid relid);

/* Scan tracking - called from orvos_handler.c scan callbacks */
extern void ovstats_scan_begin(Oid relid);
extern void ovstats_scan_observe_tuple(Oid relid, bool is_live,
									   bool *isnulls, int natts);
extern void ovstats_scan_end(Oid relid);

/* Planner access - called from orvos_planner.c */
extern bool ovstats_get_tuple_counts(Oid relid,
									 double *live_tuples,
									 double *dead_tuples);
extern bool ovstats_get_null_frac(Oid relid, AttrNumber attnum,
								  float4 *null_frac);
extern bool ovstats_get_compression_ratio(Oid relid,
										  double *ratio);
extern bool ovstats_is_fresh(Oid relid, int threshold_secs);

#endif							/* ORVOS_STATS_H */
