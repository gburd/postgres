/**
 * @file orvos_undorec.h
 * @brief UNDO record types and creation functions for Orvos.
 *
 * Defines the on-disk format of each UNDO record type (INSERT, DELETE,
 * UPDATE, TUPLE_LOCK, DELTA_INSERT) and the "pending undo op" mechanism
 * used to prepare UNDO records before entering a critical section.
 *
 * @par UNDO Record Types
 * | Type | Constant                   | Purpose                                |
 * |------|----------------------------|----------------------------------------|
 * | 1    | OVUNDO_TYPE_INSERT         | Row insertion                          |
 * | 2    | OVUNDO_TYPE_DELETE         | Row deletion (batched, up to 50 TIDs)  |
 * | 3    | OVUNDO_TYPE_UPDATE         | Update (marks old tuple as updated)    |
 * | 4    | OVUNDO_TYPE_TUPLE_LOCK     | SELECT FOR UPDATE/SHARE locking        |
 * | 5    | OVUNDO_TYPE_DELTA_INSERT   | Partial-column INSERT (column-delta)   |
 *
 * @par Pending UNDO Operations
 * UNDO records are not written directly during DML.  Instead,
 * ovundo_create_for_*() prepares a ov_pending_undo_op, which is
 * finalized by ovundo_finish_pending_op() after the B-tree modifications
 * succeed.  This allows clean abort if the B-tree operation fails.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/orvos_undorec.h
 */
#ifndef ORVOS_UNDOREC_H
#define ORVOS_UNDOREC_H

#include "c.h"
#include "access/transam.h"
#include "access/orvos_tid.h"
#include "nodes/lockoptions.h"
#include "storage/buf.h"
#include "storage/off.h"
#include "utils/relcache.h"

#define OVUNDO_TYPE_INSERT			1
#define OVUNDO_TYPE_DELETE			2
#define OVUNDO_TYPE_UPDATE			3
#define OVUNDO_TYPE_TUPLE_LOCK		4
#define OVUNDO_TYPE_DELTA_INSERT	5	/* INSERT with column-delta info */

/*
 * DELTA_INSERT is treated as INSERT for visibility purposes.
 * Use this macro wherever INSERT-type checks are needed.
 */
#define OVUNDO_TYPE_IS_INSERT(type) \
	((type) == OVUNDO_TYPE_INSERT || (type) == OVUNDO_TYPE_DELTA_INSERT)

struct OVUndoRec
{
	int16		size;			/* size of this record, including header */
	uint8		type;			/* OVUNDO_TYPE_* */
	OVUndoRecPtr undorecptr;
	TransactionId xid;
	CommandId	cid;

	/*
	 * UNDO-record of the inserter. This is needed if a row is inserted, and
	 * deleted, and there are some snapshots active don't don't consider even
	 * the insertion as visible.
	 *
	 * This is also used in Insert records, if the record represents the new
	 * tuple version of an UPDATE, rather than an INSERT. It's needed to dig
	 * into possible KEY SHARE locks held on the row, which didn't prevent the
	 * tuple from being updated.
	 */
	OVUndoRecPtr prevundorec;
};
typedef struct OVUndoRec OVUndoRec;

/*
 * Type-specific record formats.
 *
 * We store similar info as zheap for INSERT/UPDATE/DELETE. See zheap README.
 */
typedef struct
{
	OVUndoRec	rec;
	ovtid		firsttid;
	ovtid		endtid;			/* exclusive */
	uint32		speculative_token;	/* Only used for INSERT records */

} OVUndoRec_Insert;

/* Increased batch size for better VACUUM performance (2-3x faster) */
#define OVUNDO_NUM_TIDS_PER_DELETE	50

typedef struct
{
	OVUndoRec	rec;

	bool		changedPart;	/* tuple was moved to a different partition by
								 * UPDATE */

	/*
	 * One deletion record can represent deleting up to
	 * OVUNDO_NUM_TIDS_PER_DELETE tuples. The 'rec.tid' field is unused.
	 */
	uint16		num_tids;
	ovtid		tids[OVUNDO_NUM_TIDS_PER_DELETE];

	/*
	 * TODO: It might be good to move the deleted tuple to the undo-log, so
	 * that the space can immediately be reused. But currently, we don't do
	 * that. Or even better, move the old tuple to the undo-log lazily, if the
	 * space is needed for a new insertion, before the old tuple becomes
	 * recyclable.
	 */
} OVUndoRec_Delete;

/*
 * This is used for an UPDATE, to mark the old tuple version as updated.
 * It's the same as a deletion, except this stores the TID of the new tuple
 * version, so it can be followed in READ COMMITTED mode.
 *
 * The OVUndoRec_Insert record is used for the insertion of the new tuple
 * version.
 */
typedef struct
{
	OVUndoRec	rec;

	ovtid		oldtid;
	ovtid		newtid;

	bool		key_update;		/* were key columns updated? (for conflicting
								 * with FOR KEY SHARE) */

} OVUndoRec_Update;

/*
 * Column-delta INSERT record for UPDATE operations.
 *
 * When an UPDATE only changes a subset of columns, the new tuple version
 * uses this record type instead of OVUNDO_TYPE_INSERT. It stores which
 * columns were actually changed, and the predecessor TID from which
 * unchanged column values should be fetched.
 *
 * This avoids inserting unchanged columns into their B-trees, reducing
 * WAL volume by up to 80% for partial UPDATEs on wide tables.
 *
 * The changed_cols bitmap uses one bit per attribute (1-indexed).
 * Bit N set means attribute N was changed and has a B-tree entry
 * under this TID. Bit N clear means attribute N is unchanged and
 * should be fetched from predecessor_tid instead.
 *
 * Maximum supported columns: OVUNDO_DELTA_MAX_COLS (currently 1664,
 * matching MaxHeapAttributeNumber). The bitmap is stored inline
 * as a fixed-size array sized to the actual natts of the relation.
 */
#define OVUNDO_DELTA_MAX_COLS	1664
#define OVUNDO_DELTA_BITMAP_WORDS(natts) \
	(((natts) + 31) / 32)

typedef struct
{
	OVUndoRec	rec;
	ovtid		firsttid;
	ovtid		endtid;			/* exclusive */
	uint32		speculative_token;
	ovtid		predecessor_tid;	/* TID of previous tuple version */
	int16		natts;				/* number of attributes in relation */
	int16		nchanged;			/* number of changed columns */
	/*
	 * Variable-length bitmap follows: uint32 words, one bit per column.
	 * Bit (attno-1) set means column attno was changed.
	 */
	uint32		changed_cols[FLEXIBLE_ARRAY_MEMBER];
} OVUndoRec_DeltaInsert;

#define SizeOfOVUndoRecDeltaInsert(natts) \
	(offsetof(OVUndoRec_DeltaInsert, changed_cols) + \
	 OVUNDO_DELTA_BITMAP_WORDS(natts) * sizeof(uint32))

static inline bool
ov_delta_col_is_changed(const OVUndoRec_DeltaInsert *delta, int attno)
{
	int idx = (attno - 1) / 32;
	int bit = (attno - 1) % 32;
	return (delta->changed_cols[idx] & (1U << bit)) != 0;
}

static inline void
ov_delta_col_set_changed(OVUndoRec_DeltaInsert *delta, int attno)
{
	int idx = (attno - 1) / 32;
	int bit = (attno - 1) % 32;
	delta->changed_cols[idx] |= (1U << bit);
}

/*
 * This is used when a tuple is locked e.g. with SELECT FOR UPDATE.
 * The tuple isn't really changed in any way, but the undo record gives
 * a place to store the XID of the locking transaction.
 *
 * In case of a FOR SHARE lock, there can be multiple lockers. Each locker
 * will create a new undo record with its own XID that points to the previous
 * record. So the records will form a chain, leading finally to the insertion
 * record (or beyond the UNDO horizon, meaning the tuple's insertion is visible
 * to everyone)
 */
typedef struct
{
	OVUndoRec	rec;
	ovtid		tid;

	/*
	 * XXX: Is it OK to store this on disk? The enum values could change. Then
	 * again, no one should care about old locks that were acquired before
	 * last restart. Except with two-phase commit prepared transactions.
	 */
	LockTupleMode lockmode;
} OVUndoRec_TupleLock;

/*
 * ov_pending_undo_op encapsulates the insertion or modification of an UNDO
 * record. The ovundo_create_* functions don't insert UNDO records directly,
 * because the callers are not in a critical section yet, and may still need
 * to abort. For example, to inserting a new TID to the TID tree, we first
 * construct the UNDO record for the insertion, and then lock the correct
 * TID tree page to insert to. But if e.g. we need to split the TID page,
 * we might still have to error out.
 */
struct ov_pending_undo_op
{
	ov_undo_reservation reservation;

	bool		is_update;
	/* more data follows (defined as uint64, to force alignment) */
	uint64		payload[FLEXIBLE_ARRAY_MEMBER];
};
typedef struct ov_pending_undo_op ov_pending_undo_op;

/*
 * These are used in WAL records, to represent insertion or modification
 * of an UNDO record.
 *
 * We use this same record for all UNDO operations. It's a bit wasteful;
 * if an existing UNDO record is modified, we wouldn't need to overwrite
 * the whole record. Also, no need to WAL-log the command ids, because
 * they don't matter after crash/replay.
 */
typedef struct
{
	OVUndoRecPtr undoptr;
	uint16		length;
	bool		is_update;

	char		payload[FLEXIBLE_ARRAY_MEMBER];
}			ov_wal_undo_op;

#define SizeOfOVWalUndoOp	offsetof(ov_wal_undo_op, payload)

/* prototypes for functions in orvos_undorec.c */
extern struct OVUndoRec *ovundo_fetch_record(Relation rel, OVUndoRecPtr undorecptr);

extern ov_pending_undo_op * ovundo_create_for_delete(Relation rel, TransactionId xid, CommandId cid, ovtid tid,
													 bool changedPart, OVUndoRecPtr prev_undo_ptr);
extern ov_pending_undo_op * ovundo_create_for_insert(Relation rel, TransactionId xid, CommandId cid,
													 ovtid tid, int nitems,
													 uint32 speculative_token, OVUndoRecPtr prev_undo_ptr);
extern ov_pending_undo_op * ovundo_create_for_update(Relation rel, TransactionId xid, CommandId cid,
													 ovtid oldtid, ovtid newtid, OVUndoRecPtr prev_undo_ptr,
													 bool key_update);
extern ov_pending_undo_op * ovundo_create_for_delta_insert(Relation rel,
														   TransactionId xid, CommandId cid,
														   ovtid tid, int nitems,
														   ovtid predecessor_tid,
														   int natts, const bool *changed_cols,
														   OVUndoRecPtr prev_undo_ptr);
extern ov_pending_undo_op * ovundo_create_for_tuple_lock(Relation rel, TransactionId xid, CommandId cid,
														 ovtid tid, LockTupleMode lockmode,
														 OVUndoRecPtr prev_undo_ptr);
extern void ovundo_finish_pending_op(ov_pending_undo_op * pendingop, char *payload);
extern void ovundo_clear_speculative_token(Relation rel, OVUndoRecPtr undoptr);

extern void XLogRegisterUndoOp(uint8 block_id, ov_pending_undo_op * undo_op);
extern Buffer XLogRedoUndoOp(XLogReaderState *record, uint8 block_id);

struct VacuumParams;
extern void ovundo_vacuum(Relation rel, struct VacuumParams *params, BufferAccessStrategy bstrategy);
extern OVUndoRecPtr ovundo_get_oldest_undo_ptr(Relation rel);

#endif							/* ORVOS_UNDOREC_H */
