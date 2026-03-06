/*
 * orvos_undolog.h
 *		internal declarations for Orvos undo logging
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/orvos_undolog.h
 */
#ifndef ORVOS_UNDOLOG_H
#define ORVOS_UNDOLOG_H

#include "postgres.h"

#include "access/xlogreader.h"
#include "storage/block.h"
#include "storage/buf.h"
#include "utils/relcache.h"

/*
 * Maximum size of an UNDO record.
 *
 * We set this to half a block size to ensure that at least two UNDO records
 * can fit on a page, simplifying page management and reducing fragmentation.
 * This is a conservative estimate; most UNDO records are much smaller.
 */
#define MaxUndoRecordSize		(BLCKSZ / 2)

/*
 * An UNDO-pointer.
 *
 * In the "real" UNDO-logging work from EDB, an UndoRecPtr is only 64 bits.
 * But we make life easier for us, by encoding more information in it.
 *
 * 'counter' is a number that's incremented every time a new undo record is
 * created. It can be used to determine if an undo pointer is too old to be
 * of interest to anyone.
 *
 * 'blkno' and 'offset' are the physical location of the UNDO record. They
 * can be used to easily fetch a given record.
 */
typedef struct
{
	uint64		counter;
	BlockNumber blkno;
	int32		offset;			/* int16 would suffice, but avoid padding */
} OVUndoRecPtr;

/* TODO: assert that blkno and offset match, too, if counter matches */
#define OVUndoRecPtrEquals(a, b) ((a).counter == (b).counter)

typedef struct
{
	BlockNumber next;
	OVUndoRecPtr first_undorecptr;	/* note: this is set even if the page is
									 * empty! */
	OVUndoRecPtr last_undorecptr;
	uint16		padding0;		/* padding, to put ov_page_id last */
	uint16		padding1;		/* padding, to put ov_page_id last */
	uint16		padding2;		/* padding, to put ov_page_id last */
	uint16		ov_page_id;		/* OV_UNDO_PAGE_ID */
} OVUndoPageOpaque;

/*
 * "invalid" undo pointer. The value is chosen so that an invalid pointer
 * is less than any real UNDO pointer value. Therefore, a tuple with an
 * invalid UNDO pointer is considered visible to everyone.
 */
#define InvalidUndoPtr ((OVUndoRecPtr){.counter = 0, .blkno = InvalidBlockNumber, .offset = 0})

/*
 * A special value used on TID items, to mean that a tuple is not visible to
 * anyone
 */
#define DeadUndoPtr ((OVUndoRecPtr){.counter = 1, .blkno = InvalidBlockNumber, .offset = 0})

static inline bool
IsOVUndoRecPtrValid(OVUndoRecPtr *uptr)
{
	return uptr->counter != 0;
}

/*
 * ov_undo_reservation represents a piece of UNDO log that has been reserved for
 * inserting a new UNDO record, but the UNDO record hasn't been written yet.
 */
typedef struct
{
	Buffer		undobuf;
	OVUndoRecPtr undorecptr;
	size_t		length;

	char	   *ptr;
}			ov_undo_reservation;

/* prototypes for functions in orvos_undolog.c */
extern void ovundo_insert_reserve(Relation rel, size_t size, ov_undo_reservation * reservation_p);
extern void ovundo_insert_finish(ov_undo_reservation * reservation);

extern char *ovundo_fetch(Relation rel, OVUndoRecPtr undoptr, Buffer *buf_p, int lockmode, bool missing_ok);

extern void ovundo_discard(Relation rel, OVUndoRecPtr oldest_undorecptr);

extern void ovundo_newpage_redo(XLogReaderState *record);
extern void ovundo_discard_redo(XLogReaderState *record);

#endif							/* ORVOS_UNDOLOG_H */
