/*
 * orvos_stats.c
 *		Opportunistic statistics collection for Orvos columnar storage
 *
 * This module collects fresh tuple counts, null fractions, and
 * compression ratios during normal DML and sequential scan operations.
 * The planner consults these statistics (via ovstats_get_*) to produce
 * better cost estimates between ANALYZE runs.
 *
 * Design:
 *   - A backend-local hash table (keyed by Oid) stores per-relation
 *     OrvosOpStats structs.
 *   - INSERT/DELETE callbacks bump tuple counters cheaply.
 *   - Sequential scans sample every Nth tuple (controlled by the
 *     orvos.stats_sample_rate GUC) to update live/dead counts and
 *     per-column null fractions.
 *   - The planner reads these counters and, when fresh enough (per
 *     orvos.stats_freshness_threshold), uses them in preference to
 *     stale pg_class.reltuples.
 *
 * Thread safety:
 *   The hash table is backend-local, so no locking is needed.  Each
 *   backend maintains its own view; stats converge after a few scans.
 *
 * Copyright (c) 2019-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/backend/access/orvos/orvos_stats.c
 */
#include "postgres.h"

#include "access/orvos_stats.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

/* GUC variables */
bool		orvos_enable_opportunistic_stats = true;
int			orvos_stats_sample_rate = 100;
int			orvos_stats_freshness_threshold = 3600;

/* Backend-local hash table */
static HTAB *orvos_stats_hash = NULL;
static MemoryContext orvos_stats_mcxt = NULL;

/* Per-scan accumulator stored in scan_accum_hash, keyed by Oid */
typedef struct OvstatsScanAccum
{
	Oid			relid;
	int64		live_count;
	int64		dead_count;
	int			natts;
	int64		col_null_count[OVSTATS_MAX_TRACKED_COLS];
	int64		col_total_count[OVSTATS_MAX_TRACKED_COLS];
	int64		tuple_counter;	/* for sampling */
} OvstatsScanAccum;

static HTAB *scan_accum_hash = NULL;

/*
 * Ensure the stats hash table exists.
 */
static void
ovstats_ensure_hash(void)
{
	HASHCTL		ctl;

	if (orvos_stats_hash != NULL)
		return;

	orvos_stats_mcxt = AllocSetContextCreate(TopMemoryContext,
											 "OrvosOpStats",
											 ALLOCSET_DEFAULT_SIZES);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(OrvosOpStats);
	ctl.hcxt = orvos_stats_mcxt;

	orvos_stats_hash = hash_create("OrvosOpStats hash",
								   64,
								   &ctl,
								   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(OvstatsScanAccum);
	ctl.hcxt = orvos_stats_mcxt;

	scan_accum_hash = hash_create("OrvosOpStats scan accum",
								  16,
								  &ctl,
								  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/*
 * Find or create an OrvosOpStats entry for a relation.
 */
static OrvosOpStats *
ovstats_get_or_create(Oid relid)
{
	OrvosOpStats *entry;
	bool		found;

	ovstats_ensure_hash();

	entry = (OrvosOpStats *) hash_search(orvos_stats_hash,
										 &relid,
										 HASH_ENTER,
										 &found);
	if (!found)
	{
		/* Zero-initialize everything except the key */
		memset((char *) entry + sizeof(Oid), 0,
			   sizeof(OrvosOpStats) - sizeof(Oid));
	}

	return entry;
}

/*
 * Register GUCs for opportunistic statistics.
 * Called from _PG_init().
 */
void
orvos_stats_init(void)
{
	DefineCustomBoolVariable("orvos.enable_opportunistic_stats",
							 "Enable opportunistic statistics collection "
							 "during DML and scans.",
							 NULL,
							 &orvos_enable_opportunistic_stats,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("orvos.stats_sample_rate",
							"Sample every Nth tuple during sequential scans "
							"for null fraction and compression statistics.",
							NULL,
							&orvos_stats_sample_rate,
							100,
							1, 10000,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("orvos.stats_freshness_threshold",
							"Seconds after which opportunistic statistics "
							"are considered stale.",
							NULL,
							&orvos_stats_freshness_threshold,
							3600,
							1, 86400,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	MarkGUCPrefixReserved("orvos");
}

/* ----------------------------------------------------------------
 * DML tracking
 * ----------------------------------------------------------------
 */

void
ovstats_count_insert(Oid relid, int ntuples)
{
	OrvosOpStats *entry;

	if (!orvos_enable_opportunistic_stats)
		return;

	entry = ovstats_get_or_create(relid);
	entry->tuples_inserted += ntuples;
	entry->last_dml_update = GetCurrentTimestamp();
}

void
ovstats_count_delete(Oid relid)
{
	OrvosOpStats *entry;

	if (!orvos_enable_opportunistic_stats)
		return;

	entry = ovstats_get_or_create(relid);
	entry->tuples_deleted++;
	entry->last_dml_update = GetCurrentTimestamp();
}

/* ----------------------------------------------------------------
 * Scan tracking
 * ----------------------------------------------------------------
 */

void
ovstats_scan_begin(Oid relid)
{
	OvstatsScanAccum *accum;
	bool		found;

	if (!orvos_enable_opportunistic_stats)
		return;

	ovstats_ensure_hash();

	accum = (OvstatsScanAccum *) hash_search(scan_accum_hash,
											 &relid,
											 HASH_ENTER,
											 &found);
	/* Always reset the accumulator at scan start */
	memset((char *) accum + sizeof(Oid), 0,
		   sizeof(OvstatsScanAccum) - sizeof(Oid));
}

void
ovstats_scan_observe_tuple(Oid relid, bool is_live,
						   bool *isnulls, int natts)
{
	OvstatsScanAccum *accum;
	int			tracked;

	if (!orvos_enable_opportunistic_stats)
		return;

	ovstats_ensure_hash();

	accum = (OvstatsScanAccum *) hash_search(scan_accum_hash,
											 &relid,
											 HASH_FIND,
											 NULL);
	if (accum == NULL)
		return;

	if (is_live)
		accum->live_count++;
	else
		accum->dead_count++;

	/* Sample null fractions every N tuples */
	accum->tuple_counter++;
	if (isnulls != NULL &&
		(accum->tuple_counter % orvos_stats_sample_rate) == 0)
	{
		tracked = Min(natts, OVSTATS_MAX_TRACKED_COLS);
		accum->natts = Max(accum->natts, tracked);

		for (int i = 0; i < tracked; i++)
		{
			accum->col_total_count[i]++;
			if (isnulls[i])
				accum->col_null_count[i]++;
		}
	}
}

void
ovstats_scan_end(Oid relid)
{
	OvstatsScanAccum *accum;
	OrvosOpStats *entry;

	if (!orvos_enable_opportunistic_stats)
		return;

	ovstats_ensure_hash();

	accum = (OvstatsScanAccum *) hash_search(scan_accum_hash,
											 &relid,
											 HASH_FIND,
											 NULL);
	if (accum == NULL)
		return;

	/* Only commit if we actually scanned something */
	if (accum->live_count == 0 && accum->dead_count == 0)
	{
		hash_search(scan_accum_hash, &relid, HASH_REMOVE, NULL);
		return;
	}

	entry = ovstats_get_or_create(relid);

	entry->scan_live_tuples = accum->live_count;
	entry->scan_dead_tuples = accum->dead_count;
	entry->scan_count_valid = true;

	/* Merge per-column null fractions */
	if (accum->natts > 0)
	{
		int			tracked = Min(accum->natts, OVSTATS_MAX_TRACKED_COLS);

		entry->natts_tracked = tracked;
		for (int i = 0; i < tracked; i++)
		{
			entry->col_null_count[i] = accum->col_null_count[i];
			entry->col_total_count[i] = accum->col_total_count[i];
		}
	}

	entry->last_scan_update = GetCurrentTimestamp();

	hash_search(scan_accum_hash, &relid, HASH_REMOVE, NULL);
}

/* ----------------------------------------------------------------
 * Planner access
 * ----------------------------------------------------------------
 */

bool
ovstats_get_tuple_counts(Oid relid, double *live_tuples,
						 double *dead_tuples)
{
	OrvosOpStats *entry;

	if (!orvos_enable_opportunistic_stats || orvos_stats_hash == NULL)
		return false;

	entry = (OrvosOpStats *) hash_search(orvos_stats_hash,
										 &relid,
										 HASH_FIND,
										 NULL);
	if (entry == NULL)
		return false;

	/*
	 * Prefer scan-based counts when available.  They give an absolute count
	 * from the most recent sequential scan, which is more accurate than DML
	 * deltas.  Supplement with DML deltas that occurred after the scan.
	 */
	if (entry->scan_count_valid)
	{
		*live_tuples = (double) entry->scan_live_tuples
			+ (double) entry->tuples_inserted;
		*dead_tuples = (double) entry->scan_dead_tuples;

		if (*live_tuples < 0)
			*live_tuples = 0;

		return true;
	}

	/*
	 * No scan data yet - we only have DML deltas.  The caller must combine
	 * these with pg_class.reltuples as the baseline.  Indicate availability
	 * by returning the deltas as-is; the caller checks for this case.
	 */
	if (entry->tuples_inserted > 0 || entry->tuples_deleted > 0)
	{
		*live_tuples = (double) entry->tuples_inserted;
		*dead_tuples = (double) entry->tuples_deleted;
		return true;
	}

	return false;
}

bool
ovstats_get_null_frac(Oid relid, AttrNumber attnum, float4 *null_frac)
{
	OrvosOpStats *entry;
	int			idx;

	if (!orvos_enable_opportunistic_stats || orvos_stats_hash == NULL)
		return false;

	entry = (OrvosOpStats *) hash_search(orvos_stats_hash,
										 &relid,
										 HASH_FIND,
										 NULL);
	if (entry == NULL)
		return false;

	idx = attnum - 1;
	if (idx < 0 || idx >= entry->natts_tracked)
		return false;

	if (entry->col_total_count[idx] == 0)
		return false;

	*null_frac = (float4) entry->col_null_count[idx] /
		(float4) entry->col_total_count[idx];
	return true;
}

bool
ovstats_get_compression_ratio(Oid relid, double *ratio)
{
	OrvosOpStats *entry;

	if (!orvos_enable_opportunistic_stats || orvos_stats_hash == NULL)
		return false;

	entry = (OrvosOpStats *) hash_search(orvos_stats_hash,
										 &relid,
										 HASH_FIND,
										 NULL);
	if (entry == NULL || !entry->compression_valid)
		return false;

	if (entry->compressed_bytes <= 0)
		return false;

	*ratio = entry->uncompressed_bytes / entry->compressed_bytes;
	return true;
}

bool
ovstats_is_fresh(Oid relid, int threshold_secs)
{
	OrvosOpStats *entry;
	TimestampTz latest;
	TimestampTz cutoff;

	if (!orvos_enable_opportunistic_stats || orvos_stats_hash == NULL)
		return false;

	entry = (OrvosOpStats *) hash_search(orvos_stats_hash,
										 &relid,
										 HASH_FIND,
										 NULL);
	if (entry == NULL)
		return false;

	latest = Max(entry->last_dml_update, entry->last_scan_update);
	if (latest == 0)
		return false;

	cutoff = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
										 -((int64) threshold_secs * 1000));
	return (latest >= cutoff);
}
