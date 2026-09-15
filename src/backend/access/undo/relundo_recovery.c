/*-------------------------------------------------------------------------
 *
 * relundo_recovery.c
 *	  Crash-recovery driver for per-relation UNDO (loser-transaction rollback)
 *
 * An in-place MVCC table access method overwrites the
 * committed tuple bytes on the data page on UPDATE, and the only durable copy
 * of the prior committed version is the before-image stored in the relation's
 * UNDO fork.  WAL redo faithfully re-establishes the page state as it was at
 * crash time, including modifications made by transactions that never
 * committed.
 * Nothing in the redo pass reverses those uncommitted in-place changes, so an
 * uncommitted new value would remain visible after restart -- a wrong-results
 * bug.
 *
 * This module closes that gap with an end-of-recovery scan of the UNDO forks
 * on disk.  A track-during-redo approach cannot work: a CHECKPOINT taken after
 * an uncommitted in-place UPDATE advances the redo start LSN past that UPDATE's
 * WAL, so redo never replays it and a tracker would stay empty even though the
 * uncommitted value is durable on the flushed data page.  The before-image,
 * however, is always durable in the UNDO fork.  After redo finishes, CLOG has
 * been fully reconstructed, so TransactionIdDidCommit() authoritatively
 * separates committed (winner) from incomplete (loser) transactions.
 *
 * PerformRelUndoRecovery() enumerates every relundo fork on disk without
 * catalog access (the ResetUnloggedRelations filesystem-walk pattern), opens
 * each relation with a fake relcache entry, walks its UNDO page chain
 * newest-first (metapage head_blkno then prev_blkno toward the tail; within a
 * page, records are replayed high offset to low), and for every record whose
 * creating transaction did not commit and is not a prepared transaction, calls
 * RelUndoApplyRecordForRecovery() to restore the before-image in place.
 *
 * Newest-first order matters: a later in-place update's before-image is the
 * earlier update's after-image, so the most recent record for a tuple must be
 * reverse-applied first.  Walking head->tail across pages and high->low within
 * a page yields newest-first within every transaction; cross-transaction order
 * is immaterial because each record restores an independent before-image.
 *
 * No compensation log record (CLR) is written here.  PerformRelUndoRecovery()
 * runs from PerformWalRecovery(), before StartupXLOG() enables WAL insertion
 * for this backend, so WAL cannot be emitted.  This mirrors the cluster-wide
 * PerformUndoRecovery() path: durability is provided by the end-of-recovery
 * checkpoint, and re-application after a mid-recovery crash is harmless because
 * redo first re-establishes the post-modification page before the before-image
 * is restored again.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/undo/relundo_recovery.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relundo.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "access/xlogutils.h"
#include "catalog/pg_tablespace_d.h"
#include "common/relpath.h"
#include "miscadmin.h"
#include "postgres_ext.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/fd.h"
#include "storage/reinit.h"
#include "storage/smgr.h"
#include "utils/memutils.h"

static void RelUndoRecoveryScanTablespaceDir(const char *tsdirname, Oid spcoid);
static void RelUndoRecoveryScanDbspaceDir(const char *dbspacedirname,
										  Oid spcoid, Oid dboid);
static void RelUndoRecoveryScanOneFork(RelFileLocator rloc);
static int	RelUndoRecoveryApplyPage(Relation rel, BlockNumber blkno);

/* Running count of before-images restored across all forks. */
static int	relundo_recovery_applied = 0;

/*
 * PerformRelUndoRecovery - Reverse-apply loser transactions' before-images.
 *
 * Entry point called once at the end of WAL redo (crash recovery / PITR).
 * Walks the data directory for per-relation UNDO forks and rolls back every
 * incomplete transaction's in-place modifications.
 */
void
PerformRelUndoRecovery(void)
{
	char		tblspc_path[MAXPGPATH + sizeof(PG_TBLSPC_DIR) + sizeof(TABLESPACE_VERSION_DIRECTORY)];
	DIR		   *spc_dir;
	struct dirent *spc_de;
	MemoryContext tmpctx,
				oldctx;

	relundo_recovery_applied = 0;

	/*
	 * Use a private memory context so the directory-walk allocations are
	 * reclaimed in one shot regardless of how the scan exits.
	 */
	tmpctx = AllocSetContextCreate(CurrentMemoryContext,
								   "PerformRelUndoRecovery",
								   ALLOCSET_DEFAULT_SIZES);
	oldctx = MemoryContextSwitchTo(tmpctx);

	/* Default tablespace lives under $PGDATA/base. */
	RelUndoRecoveryScanTablespaceDir("base", DEFAULTTABLESPACE_OID);

	/* Non-default tablespaces are symlinked under pg_tblspc. */
	spc_dir = AllocateDir(PG_TBLSPC_DIR);
	while ((spc_de = ReadDir(spc_dir, PG_TBLSPC_DIR)) != NULL)
	{
		Oid			spcoid;

		if (strcmp(spc_de->d_name, ".") == 0 ||
			strcmp(spc_de->d_name, "..") == 0)
			continue;

		spcoid = atooid(spc_de->d_name);
		if (!OidIsValid(spcoid))
			continue;

		snprintf(tblspc_path, sizeof(tblspc_path), "%s/%s/%s",
				 PG_TBLSPC_DIR, spc_de->d_name, TABLESPACE_VERSION_DIRECTORY);
		RelUndoRecoveryScanTablespaceDir(tblspc_path, spcoid);
	}
	FreeDir(spc_dir);

	MemoryContextSwitchTo(oldctx);
	MemoryContextDelete(tmpctx);

	if (relundo_recovery_applied > 0)
		ereport(LOG,
				(errmsg("per-relation UNDO recovery complete: %d before-image(s) restored",
						relundo_recovery_applied)));
}

/*
 * RelUndoRecoveryScanTablespaceDir - Scan one tablespace's per-database dirs.
 */
static void
RelUndoRecoveryScanTablespaceDir(const char *tsdirname, Oid spcoid)
{
	DIR		   *ts_dir;
	struct dirent *de;
	char		dbspace_path[MAXPGPATH * 2];

	ts_dir = AllocateDir(tsdirname);

	/*
	 * A missing tablespace directory is not fatal: a crashed DROP TABLESPACE
	 * can leave a dangling pg_tblspc symlink.  Mirror ResetUnloggedRelations
	 * and let it pass.
	 */
	if (ts_dir == NULL && errno == ENOENT)
		return;

	while ((de = ReadDir(ts_dir, tsdirname)) != NULL)
	{
		Oid			dboid;

		/* Per-database directories have purely numeric names. */
		if (strspn(de->d_name, "0123456789") != strlen(de->d_name))
			continue;

		dboid = atooid(de->d_name);

		snprintf(dbspace_path, sizeof(dbspace_path), "%s/%s",
				 tsdirname, de->d_name);
		RelUndoRecoveryScanDbspaceDir(dbspace_path, spcoid, dboid);
	}

	FreeDir(ts_dir);
}

/*
 * RelUndoRecoveryScanDbspaceDir - Scan one database dir for relundo forks.
 *
 * Only the first segment (segno 0) of each relundo fork is processed; the
 * buffer manager transparently spans higher segments when reading the fork.
 */
static void
RelUndoRecoveryScanDbspaceDir(const char *dbspacedirname, Oid spcoid, Oid dboid)
{
	DIR		   *dbspace_dir;
	struct dirent *de;

	dbspace_dir = AllocateDir(dbspacedirname);
	if (dbspace_dir == NULL && errno == ENOENT)
		return;

	while ((de = ReadDir(dbspace_dir, dbspacedirname)) != NULL)
	{
		RelFileNumber relnumber;
		ForkNumber	forknum;
		unsigned	segno;
		RelFileLocator rloc;

		if (!parse_filename_for_nontemp_relation(de->d_name, &relnumber,
												 &forknum, &segno))
			continue;

		if (forknum != RELUNDO_FORKNUM || segno != 0)
			continue;

		rloc.spcOid = spcoid;
		rloc.dbOid = dboid;
		rloc.relNumber = relnumber;

		RelUndoRecoveryScanOneFork(rloc);
	}

	FreeDir(dbspace_dir);
}

/*
 * RelUndoRecoveryScanOneFork - Roll back losers recorded in one UNDO fork.
 *
 * Opens the relation with a fake relcache entry (no catalog access), reads the
 * metapage to find the head of the page chain, and walks the chain newest-page
 * first applying loser before-images.
 */
static void
RelUndoRecoveryScanOneFork(RelFileLocator rloc)
{
	Relation	rel;
	Buffer		metabuf;
	Page		metapage;
	RelUndoMetaPage meta;
	BlockNumber head_blkno[RELUNDO_NUM_HEADS];
	BlockNumber nblocks;

	rel = CreateFakeRelcacheEntry(rloc);

	/*
	 * An empty or absent fork has nothing to roll back.  The relation is a
	 * fake relcache entry whose rd_rel->relkind is zero, so the relkind
	 * dispatch in RelationGetNumberOfBlocksInFork() would trip an assertion;
	 * probe the underlying smgr directly instead, as XLOG replay does.
	 */
	if (!smgrexists(RelationGetSmgr(rel), RELUNDO_FORKNUM))
	{
		FreeFakeRelcacheEntry(rel);
		return;
	}

	nblocks = smgrnblocks(RelationGetSmgr(rel), RELUNDO_FORKNUM);
	if (nblocks == 0)
	{
		FreeFakeRelcacheEntry(rel);
		return;
	}

	/* Read the metapage (block 0) directly; do not auto-initialize it. */
	metabuf = ReadBufferExtended(rel, RELUNDO_FORKNUM, 0, RBM_NORMAL, NULL);
	LockBuffer(metabuf, BUFFER_LOCK_SHARE);
	metapage = BufferGetPage(metabuf);
	meta = (RelUndoMetaPage) PageGetContents(metapage);

	if (meta->magic != RELUNDO_METAPAGE_MAGIC)
	{
		UnlockReleaseBuffer(metabuf);
		FreeFakeRelcacheEntry(rel);
		return;
	}

	for (int slot = 0; slot < RELUNDO_NUM_HEADS; slot++)
		head_blkno[slot] = meta->head_blkno[slot];
	UnlockReleaseBuffer(metabuf);

	/*
	 * Walk each of the RELUNDO_NUM_HEADS independent append chains from its
	 * head (newest) toward its tail (oldest).  Only real data blocks (block
	 * >= 1, below the fork's block count) are valid chain links.  Block 0 is
	 * the metapage: a zeroed data page carries a prev_blkno of 0 -- not
	 * InvalidBlockNumber -- and BlockNumberIsValid(0) is true, so an
	 * unguarded walk would read the metapage as a data page, interpret its
	 * magic as the next block number, and fault.  Treat block 0 and any
	 * out-of-range block as the chain terminus.
	 *
	 * A corrupt prev_blkno could form a cycle.  Each chain has at most
	 * nblocks links, so cap the walk at nblocks iterations and fail loudly on
	 * overrun rather than spinning forever during recovery.
	 */
	for (int slot = 0; slot < RELUNDO_NUM_HEADS; slot++)
	{
		BlockNumber blkno = head_blkno[slot];

		for (BlockNumber steps = 0;
			 blkno != InvalidBlockNumber && blkno >= 1 && blkno < nblocks;
			 steps++)
		{
			if (steps >= nblocks)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("per-relation UNDO page chain for relation %u/%u/%u exceeds %u pages; possible cycle from corrupt prev_blkno",
								rloc.spcOid, rloc.dbOid, rloc.relNumber, nblocks)));

			blkno = RelUndoRecoveryApplyPage(rel, blkno);
		}
	}

	FreeFakeRelcacheEntry(rel);
}

/*
 * RelUndoRecoveryApplyPage - Reverse-apply loser records on one UNDO page.
 *
 * Reads the page's records (oldest-to-newest by ascending offset) under a
 * share lock, then releases the lock and reverse-applies the loser records
 * newest-first.  The undo-page lock must be dropped before
 * RelUndoApplyRecordForRecovery() runs, because that routine re-reads the same
 * undo page and would otherwise self-deadlock on the buffer lock.
 *
 * Returns the previous page in the chain (toward the tail), or
 * InvalidBlockNumber when this is the oldest page.
 */
static int
RelUndoRecoveryApplyPage(Relation rel, BlockNumber blkno)
{
	Buffer		buf;
	Page		page;
	char	   *contents;
	RelUndoPageHeader hdr;
	BlockNumber prev;
	uint16		page_counter;
	uint16		pd_lower;
	uint16		offset;
	uint16	   *offsets;
	TransactionId *xids;
	int			nrecs = 0;
	int			maxrecs;
	int			i;

	buf = ReadBufferExtended(rel, RELUNDO_FORKNUM, blkno, RBM_NORMAL, NULL);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	contents = PageGetContents(page);
	hdr = (RelUndoPageHeader) contents;

	prev = hdr->prev_blkno;
	page_counter = hdr->counter;
	pd_lower = hdr->pd_lower;

	/* Upper bound on records: each record is at least a header in size. */
	maxrecs = (pd_lower > SizeOfRelUndoPageHeaderData)
		? (pd_lower - SizeOfRelUndoPageHeaderData) / SizeOfRelUndoRecordHeader + 1
		: 0;

	if (maxrecs == 0)
	{
		UnlockReleaseBuffer(buf);
		return prev;
	}

	offsets = (uint16 *) palloc(sizeof(uint16) * maxrecs);
	xids = (TransactionId *) palloc(sizeof(TransactionId) * maxrecs);

	/* Collect record offsets and xids in insertion order. */
	offset = SizeOfRelUndoPageHeaderData;
	while (offset < pd_lower && nrecs < maxrecs)
	{
		RelUndoRecordHeader rhdr;

		memcpy(&rhdr, contents + offset, SizeOfRelUndoRecordHeader);

		/* A zero-length or malformed record terminates the scan defensively. */
		if (rhdr.urec_len < SizeOfRelUndoRecordHeader)
			break;

		/* urec_type 0 marks a cancelled hole; skip but keep striding. */
		if (rhdr.urec_type != 0)
		{
			offsets[nrecs] = offset;
			xids[nrecs] = rhdr.urec_xid;
			nrecs++;
		}

		offset += rhdr.urec_len;
	}

	UnlockReleaseBuffer(buf);

	/* Reverse-apply newest-first; skip winners and prepared transactions. */
	for (i = nrecs - 1; i >= 0; i--)
	{
		TransactionId xid = xids[i];

		if (!TransactionIdIsNormal(xid))
			continue;
		if (TransactionIdDidCommit(xid))
			continue;
		if (RecoveryTransactionIdIsPrepared(xid))
			continue;

		RelUndoApplyRecordForRecovery(rel,
									  MakeRelUndoRecPtr(page_counter, blkno,
														offsets[i]));
		relundo_recovery_applied++;
	}

	pfree(offsets);
	pfree(xids);

	return prev;
}
