/**
 * @file noxu_nursery.h
 * @brief In-memory nursery buffer for batching Noxu attribute insertions.
 *
 * The nursery accumulates per-row attribute data in memory and flushes it
 * in bulk to the attribute B-trees.  TID tree entries are created
 * immediately (for index correctness), but attribute items are deferred
 * until the nursery flushes.
 *
 * This avoids creating single-element NXAttributeArrayItems (which cannot
 * trigger type-specific compression codecs like Chimp, DOD, or UUID v7
 * delta) and eliminates the O(N^2) recompression overhead that occurs when
 * thousands of tiny items are combined during page splits.
 *
 * The nursery is transaction-local and visible only to the owning backend.
 * Data is flushed to the attribute B-trees on capacity/memory limits,
 * transaction commit, or (optionally) before sequential scans.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/noxu_nursery.h
 */
#ifndef NOXU_NURSERY_H
#define NOXU_NURSERY_H

#include "access/noxu_internal.h"
#include "access/tableam.h"
#include "utils/relcache.h"

/*
 * NXNurseryRow - A single buffered row in the nursery.
 *
 * TID is already assigned and present in the TID tree.  The nursery
 * holds the per-column datum/isnull arrays until flush time.
 */
typedef struct NXNurseryRow
{
	nxtid		tid;			/* Already assigned via nxbt_tid_multi_insert */
	CommandId	cid;
	SubTransactionId subxid;
	Datum	   *datums;			/* [nattrs] column values */
	bool	   *isnulls;		/* [nattrs] null flags */
} NXNurseryRow;

/*
 * NXNurseryBuffer - Per-relation in-memory attribute data buffer.
 *
 * Holds buffered per-row attribute data until flushed in bulk to the
 * attribute B-trees.  The nursery is allocated in a dedicated MemoryContext
 * under TopTransactionContext so it is automatically cleaned up on abort.
 */
typedef struct NXNurseryBuffer
{
	RelFileLocator rlocator;	/* Identifies the relation */
	Oid			relid;			/* Relation OID */
	int			nattrs;			/* Number of user columns */

	/* Buffered rows */
	NXNurseryRow *rows;			/* Array of buffered rows */
	int			nrows;			/* Number of rows buffered */
	int			capacity;		/* Allocated size of rows array */

	/* Memory tracking */
	MemoryContext mcxt;			/* Nursery's private memory context */
	Size		mem_bytes;		/* Approximate memory used by datum copies */

	/* Transaction context */
	TransactionId xid;			/* Owning transaction */
} NXNurseryBuffer;

/* GUC variables */
extern PGDLLIMPORT bool noxu_nursery_enabled;
extern PGDLLIMPORT int noxu_nursery_size;
extern PGDLLIMPORT int noxu_nursery_mem_limit_kb;
extern PGDLLIMPORT bool noxu_nursery_flush_on_scan;

/* Nursery buffer management */
extern NXNurseryBuffer *nx_nursery_get_or_create(Relation rel);
extern NXNurseryBuffer *nx_nursery_get(Relation rel);
extern void nx_nursery_flush(Relation rel, NXNurseryBuffer *nursery);

/* Buffer a row's attribute data (TID already assigned) */
extern void nx_nursery_buffer_row(NXNurseryBuffer *nursery,
								  TupleTableSlot *slot,
								  nxtid tid,
								  CommandId cid);

/* Nursery scan for read-your-writes */
extern bool nx_nursery_scan_next(NXNurseryBuffer *nursery,
								 int *scan_idx,
								 nxtid range_start,
								 nxtid range_end,
								 TupleTableSlot *slot,
								 Relation rel);
extern bool nx_nursery_lookup_tid(NXNurseryBuffer *nursery,
								  nxtid tid,
								  TupleTableSlot *slot,
								  Relation rel);

/* Flush all nurseries for all relations (transaction boundary) */
extern void nx_nursery_flush_all(void);

/* Initialization */
extern void nx_nursery_init_gucs(void);

#endif							/* NOXU_NURSERY_H */
