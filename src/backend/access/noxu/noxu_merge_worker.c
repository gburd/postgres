/**
 * @file noxu_merge_worker.c
 * @brief Background merge worker for Noxu LSM-tree levels.
 *
 * Merges Level 1 A+B row-oriented segments into columnar B-tree format.
 * The merge is the critical format transition: row data is decomposed
 * into per-column arrays and inserted via nxbt_attr_multi_insert(),
 * which evaluates the full compression cascade (FOR, DOD, Chimp, Dict,
 * UUID v7, etc.) because the batch is large.
 *
 * The merge worker can run as:
 * 1. A background worker (periodic scan for pending merges)
 * 2. Synchronously during VACUUM (forced merge)
 * 3. Explicitly via nx_lsm_merge_level() SQL function
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/backend/access/noxu/noxu_merge_worker.c
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/noxu_internal.h"
#include "access/noxu_lsm.h"
#include "access/noxu_wal.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/pg_class.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

/* GUC: merge worker interval in milliseconds */
int			noxu_lsm_merge_interval_ms = 5000;

/* Forward declarations */
static void nx_lsm_merge_segments(Relation rel, int level_num,
								  NXLSMSegmentDesc *seg_a,
								  NXLSMSegmentDesc *seg_b,
								  NXLSMMetaPageData *meta);
static void nx_lsm_promote_or_keep(Relation rel, int level_num,
									NXLSMSegmentDesc *merged,
									NXLSMMetaPageData *meta);

/*
 * nx_lsm_merge_level - Merge A+B segments at a given level.
 *
 * This is the core merge algorithm.  It reads row-oriented data from
 * segments A and B, decomposes them into per-column arrays, and inserts
 * into the B-tree via nxbt_attr_multi_insert() (columnar format).
 *
 * After merge, the result either stays at this level (as new A) or
 * promotes to the next level if it exceeds capacity.
 *
 * Can be called synchronously (during VACUUM) or from the background
 * merge worker.
 */
void
nx_lsm_merge_level(Relation rel, int level_num)
{
	NXLSMMetaPageData *meta;
	NXLSMLevelDesc *level;

	meta = nx_lsm_get_meta(rel);

	if (level_num < 1 || level_num > meta->nlevels)
		return;

	level = &meta->levels[level_num - 1];

	/* Both A and B must be present for a merge */
	if (NXLSMSegmentIsEmpty(&level->seg_a) ||
		NXLSMSegmentIsEmpty(&level->seg_b))
		return;

	/* Don't start a merge if one is already in progress */
	if (level->merge_active)
		return;

	/* Mark merge as active */
	level->merge_active = true;
	nx_lsm_meta_update(rel, meta);

	/* Perform the actual merge */
	PG_TRY();
	{
		nx_lsm_merge_segments(rel, level_num,
							  &level->seg_a, &level->seg_b, meta);
	}
	PG_CATCH();
	{
		/* On error, clear merge-active flag */
		meta = nx_lsm_get_meta(rel);
		level = &meta->levels[level_num - 1];
		level->merge_active = false;
		nx_lsm_meta_update(rel, meta);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * nx_lsm_merge_segments - Merge two segments into columnar B-tree data.
 *
 * Reads all rows from segments A and B, decomposes into per-column
 * Datum/isnull arrays, and inserts into the B-tree attribute pages.
 * After successful merge, frees the old segments and updates metadata.
 */
static void
nx_lsm_merge_segments(Relation rel, int level_num,
					   NXLSMSegmentDesc *seg_a,
					   NXLSMSegmentDesc *seg_b,
					   NXLSMMetaPageData *meta)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	int			nattrs = tupdesc->natts;
	int			total_rows;
	int			n;
	nxtid	   *tids;
	Datum	  **col_datums;
	bool	  **col_isnulls;
	NXLSMLevelDesc *level;
	NXLSMSegmentDesc merged_desc;
	MemoryContext merge_mcxt;
	MemoryContext oldcontext;

	total_rows = seg_a->nrows + seg_b->nrows;
	if (total_rows == 0)
		return;

	merge_mcxt = AllocSetContextCreate(CurrentMemoryContext,
									   "NoxuLSMMerge",
									   ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(merge_mcxt);

	/* Allocate arrays for all rows */
	tids = (nxtid *) palloc(total_rows * sizeof(nxtid));
	col_datums = (Datum **) palloc(nattrs * sizeof(Datum *));
	col_isnulls = (bool **) palloc(nattrs * sizeof(bool *));
	for (int i = 0; i < nattrs; i++)
	{
		col_datums[i] = (Datum *) palloc(total_rows * sizeof(Datum));
		col_isnulls[i] = (bool *) palloc(total_rows * sizeof(bool));
	}

	/* Read rows from segment A */
	n = nx_lsm_read_row_segment(rel, seg_a, tids, col_datums,
								col_isnulls, 0, tupdesc);

	/* Read rows from segment B */
	n += nx_lsm_read_row_segment(rel, seg_b, tids, col_datums,
								 col_isnulls, n, tupdesc);

	/*
	 * Sort by TID if needed.  Segments A and B may have interleaved
	 * TID ranges if nursery flushes were not sequential.
	 */
	if (n > 1)
	{
		bool		needs_sort = false;

		for (int i = 1; i < n; i++)
		{
			if (tids[i] < tids[i - 1])
			{
				needs_sort = true;
				break;
			}
		}

		if (needs_sort)
		{
			/*
			 * Simple insertion sort (acceptable for reasonable segment
			 * sizes).  For very large segments, a more efficient sort
			 * would be needed, but merge segments are bounded by level
			 * capacity (a few thousand rows at Level 1).
			 */
			for (int i = 1; i < n; i++)
			{
				nxtid		key_tid = tids[i];
				Datum	   *key_datums = (Datum *) palloc(nattrs * sizeof(Datum));
				bool	   *key_isnulls = (bool *) palloc(nattrs * sizeof(bool));
				int			j;

				for (int a = 0; a < nattrs; a++)
				{
					key_datums[a] = col_datums[a][i];
					key_isnulls[a] = col_isnulls[a][i];
				}

				j = i - 1;
				while (j >= 0 && tids[j] > key_tid)
				{
					tids[j + 1] = tids[j];
					for (int a = 0; a < nattrs; a++)
					{
						col_datums[a][j + 1] = col_datums[a][j];
						col_isnulls[a][j + 1] = col_isnulls[a][j];
					}
					j--;
				}

				tids[j + 1] = key_tid;
				for (int a = 0; a < nattrs; a++)
				{
					col_datums[a][j + 1] = key_datums[a];
					col_isnulls[a][j + 1] = key_isnulls[a];
				}

				pfree(key_datums);
				pfree(key_isnulls);
			}
		}
	}

	/*
	 * Insert into B-tree as compressed columnar items.
	 *
	 * The TID tree entries already exist (created during the original
	 * INSERT).  We only need to insert the attribute data.
	 *
	 * nxbt_attr_multi_insert → nxbt_attr_create_items evaluates the
	 * full codec cascade (FOR → DOD → Chimp → Dict → Array → UUID)
	 * because nitems = n (the full merged batch).
	 *
	 * We insert in sub-batches to stay within the well-tested range.
	 */
#define NX_MERGE_BATCH_SIZE		1000

	for (int batch_start = 0; batch_start < n; batch_start += NX_MERGE_BATCH_SIZE)
	{
		int			batch_end = Min(batch_start + NX_MERGE_BATCH_SIZE, n);
		int			nbatch = batch_end - batch_start;

		for (AttrNumber attno = 1; attno <= nattrs; attno++)
		{
			nxbt_attr_multi_insert(rel, attno,
								   col_datums[attno - 1] + batch_start,
								   col_isnulls[attno - 1] + batch_start,
								   tids + batch_start,
								   nbatch);
		}
	}

	/* Free old A and B segments → add pages to free page map */
	nx_lsm_free_segment_pages(rel, seg_a);
	nx_lsm_free_segment_pages(rel, seg_b);

	/* Build descriptor for the merged result */
	memset(&merged_desc, 0, sizeof(NXLSMSegmentDesc));
	merged_desc.nrows = n;
	merged_desc.is_columnar = true;		/* now in B-tree format */
	if (n > 0)
	{
		merged_desc.first_tid = tids[0];
		merged_desc.last_tid = tids[n - 1];
	}

	/*
	 * Update metadata: clear A and B, set merge_active = false.
	 * Then decide whether to keep the result here or promote.
	 */
	meta = nx_lsm_get_meta(rel);
	level = &meta->levels[level_num - 1];

	memset(&level->seg_a, 0, sizeof(NXLSMSegmentDesc));
	level->seg_a.segment_id = NX_LSM_SEG_NONE;
	memset(&level->seg_b, 0, sizeof(NXLSMSegmentDesc));
	level->seg_b.segment_id = NX_LSM_SEG_NONE;
	memset(&level->seg_x, 0, sizeof(NXLSMSegmentDesc));
	level->seg_x.segment_id = NX_LSM_SEG_NONE;
	level->merge_active = false;

	nx_lsm_meta_update(rel, meta);

	/*
	 * Promote or keep the merged result.  Since the data is now in the
	 * B-tree (columnar format), we don't need to track it as a segment
	 * at any level — it's directly accessible via the normal B-tree scan.
	 * The segment descriptor is informational only for statistics.
	 */
	nx_lsm_promote_or_keep(rel, level_num, &merged_desc, meta);

	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(merge_mcxt);
}

/*
 * nx_lsm_promote_or_keep - Decide whether merged data stays or promotes.
 *
 * Since merged data is already in the B-tree (columnar format), there's
 * no physical promotion needed.  This function updates level statistics
 * and triggers cascading merges if the next level has pending segments.
 */
static void
nx_lsm_promote_or_keep(Relation rel, int level_num,
						NXLSMSegmentDesc *merged,
						NXLSMMetaPageData *meta)
{
	NXLSMLevelDesc *level = &meta->levels[level_num - 1];

	/*
	 * If the merged row count exceeds this level's capacity and there
	 * is a next level, check if the next level also needs merging.
	 */
	if (merged->nrows > level->capacity &&
		level_num < noxu_lsm_max_levels)
	{
		int			next_level = level_num + 1;

		/*
		 * Check if the next level has both A and B segments pending.
		 * If so, trigger a cascading merge.
		 */
		if (next_level <= meta->nlevels)
		{
			NXLSMLevelDesc *next = &meta->levels[next_level - 1];

			if (!NXLSMSegmentIsEmpty(&next->seg_a) &&
				!NXLSMSegmentIsEmpty(&next->seg_b))
			{
				nx_lsm_merge_level(rel, next_level);
			}
		}
	}
}

/*
 * nx_lsm_merge_all_pending - Merge all pending levels for a relation.
 *
 * Called from VACUUM to force all pending merges synchronously.
 */
void
nx_lsm_merge_all_pending(Relation rel)
{
	NXLSMMetaPageData *meta;

	if (!noxu_lsm_enabled)
		return;

	meta = nx_lsm_get_meta(rel);

	for (int i = 0; i < meta->nlevels; i++)
	{
		NXLSMLevelDesc *level = &meta->levels[i];

		if (!NXLSMSegmentIsEmpty(&level->seg_a) &&
			!NXLSMSegmentIsEmpty(&level->seg_b) &&
			!level->merge_active)
		{
			nx_lsm_merge_level(rel, i + 1);

			/* Re-read metadata after merge (it was updated) */
			meta = nx_lsm_get_meta(rel);
		}
	}
}

/* ----------------------------------------------------------------
 * Background Merge Worker
 * ----------------------------------------------------------------
 */

/* Signal handlers */
static volatile sig_atomic_t merge_worker_got_sighup = false;
static volatile sig_atomic_t merge_worker_got_sigterm = false;

static void
nx_merge_worker_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	merge_worker_got_sighup = true;
	SetLatch(MyLatch);
	errno = save_errno;
}

static void
nx_merge_worker_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	merge_worker_got_sigterm = true;
	SetLatch(MyLatch);
	errno = save_errno;
}

/*
 * nx_lsm_find_pending_merges - Scan pg_class for Noxu relations with
 * pending merges.
 *
 * Returns a list of relation OIDs that need merging.  This is a simple
 * scan of pg_class filtering for noxu AM relations.  The actual merge
 * decision (whether A+B both exist) is made when opening each relation.
 */
static List *
nx_lsm_find_pending_merges(void)
{
	/* TODO: Scan pg_class for noxu relations with LSM metadata.
	 * For now, this is a placeholder that returns NIL.
	 * A real implementation would:
	 * 1. Open pg_class
	 * 2. Scan for relam = noxuam OID
	 * 3. For each, check LSM metadata for pending merges
	 * 4. Return OID list
	 */
	return NIL;
}

/*
 * NoxuMergeWorkerMain - Background merge worker entry point.
 *
 * Periodically scans for Noxu relations with pending LSM merges
 * and processes them.
 */
void
NoxuMergeWorkerMain(Datum main_arg)
{
	(void) main_arg;

	/* Set up signal handlers */
	pqsignal(SIGHUP, nx_merge_worker_sighup);
	pqsignal(SIGTERM, nx_merge_worker_sigterm);
	BackgroundWorkerUnblockSignals();

	/* Connect to default database */
	BackgroundWorkerInitializeConnection(NULL, NULL, 0);

	ereport(LOG, (errmsg("Noxu LSM merge worker started")));

	while (!merge_worker_got_sigterm)
	{
		int			rc;
		List	   *pending;

		/* Handle config reload */
		if (merge_worker_got_sighup)
		{
			merge_worker_got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		CHECK_FOR_INTERRUPTS();

		/* Find relations with pending merges */
		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());

		pending = nx_lsm_find_pending_merges();

		if (pending != NIL)
		{
			ListCell   *lc;

			foreach(lc, pending)
			{
				Oid			relid = lfirst_oid(lc);

				PG_TRY();
				{
					Relation	rel = table_open(relid, RowExclusiveLock);

					nx_lsm_merge_all_pending(rel);
					table_close(rel, RowExclusiveLock);
				}
				PG_CATCH();
				{
					/* Skip failed relations gracefully */
					EmitErrorReport();
					FlushErrorState();
				}
				PG_END_TRY();

				CHECK_FOR_INTERRUPTS();
			}

			list_free(pending);
		}

		PopActiveSnapshot();
		CommitTransactionCommand();

		/* Sleep until next interval or signal */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   noxu_lsm_merge_interval_ms,
					   PG_WAIT_EXTENSION);

		ResetLatch(MyLatch);

		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}

	ereport(LOG, (errmsg("Noxu LSM merge worker shutting down")));
	proc_exit(0);
}

/*
 * NoxuMergeWorkerRegister - Register the background merge worker.
 *
 * Called during shared_preload_libraries initialization.
 * The worker starts after recovery is complete.
 */
void
NoxuMergeWorkerRegister(void)
{
	BackgroundWorker worker;

	memset(&worker, 0, sizeof(BackgroundWorker));

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 30;	/* restart after 30s if crashed */

	snprintf(worker.bgw_library_name, BGW_MAXLEN, "postgres");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "NoxuMergeWorkerMain");
	snprintf(worker.bgw_name, BGW_MAXLEN, "noxu lsm merge worker");
	snprintf(worker.bgw_type, BGW_MAXLEN, "noxu lsm merge worker");

	worker.bgw_main_arg = (Datum) 0;
	worker.bgw_notify_pid = 0;

	RegisterBackgroundWorker(&worker);
}

/*
 * nx_lsm_merge_init_gucs - Register merge worker GUC parameters.
 */
void
nx_lsm_merge_init_gucs(void)
{
	DefineCustomIntVariable("noxu.lsm_merge_interval_ms",
							"Interval between merge worker scans (ms).",
							NULL,
							&noxu_lsm_merge_interval_ms,
							5000,
							100, 600000,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);
}
