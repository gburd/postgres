/*-------------------------------------------------------------------------
 *
 * recno_slog.h
 *	  RECNO status log (sLog): shared-memory (tid,relid) -> pending-op map
 *	  used by the MVCC visibility path for uncommitted rows.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * Author: Greg Burd <greg@burd.me>
 *
 * src/include/access/recno_slog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RECNO_SLOG_H
#define RECNO_SLOG_H

#include "postgres.h"

#include "access/xact.h"
#include "storage/itemptr.h"

/* Op kinds recorded in the sLog. */
typedef enum RecnoSLogOpType
{
	RECNO_SLOG_INSERT = 0,
	RECNO_SLOG_DELETE,
	RECNO_SLOG_UPDATE,
	RECNO_SLOG_LOCK_SHARE,
	RECNO_SLOG_LOCK_EXCL,
	RECNO_SLOG_ABORTED
} RecnoSLogOpType;

/* Hash key: (relid, tid). */
typedef struct RecnoSLogKey
{
	Oid			relid;
	ItemPointerData tid;
} RecnoSLogKey;

/* Read-out copy of one pending op (returned by lookups). */
typedef struct RecnoSLogEntry
{
	ItemPointerData tid;
	Oid			relid;
	uint64		commit_ts;
	TransactionId xid;
	uint32		spec_token;
	CommandId	cid;
	SubTransactionId subxid;
	uint8		op_type;		/* RecnoSLogOpType */
	bool		in_use;
} RecnoSLogEntry;

#define RECNO_SLOG_MAX_OPS			8
#define RECNO_SLOG_PARTITIONS		128
#define RECNO_SLOG_PARTITION_MASK	(RECNO_SLOG_PARTITIONS - 1)

/* Shared memory sizing/init. */
extern Size RecnoSLogShmemSize(void);
extern void RecnoSLogShmemInit(void);

/* Mutators. */
extern void RecnoSLogInsert(Oid relid, ItemPointer tid, TransactionId xid,
							uint64 commit_ts, CommandId cid,
							RecnoSLogOpType op_type, SubTransactionId subxid,
							uint32 spec_token);
extern bool RecnoSLogRemove(Oid relid, ItemPointer tid, TransactionId xid);
extern void RecnoSLogRemoveByXid(TransactionId xid);
extern void RecnoSLogRemoveByXidGlobal(TransactionId xid);
extern void RecnoSLogRemoveBySubXid(TransactionId xid, SubTransactionId subxid);
extern void RecnoSLogUpdateSubXid(TransactionId xid, SubTransactionId old_subxid,
								  SubTransactionId new_subxid);
extern void RecnoSLogMarkAborted(TransactionId xid);
extern void RecnoSLogTrackSubXact(Oid relid, ItemPointer tid,
								  TransactionId xid, SubTransactionId subxid);

/* Lookups. */
extern int	RecnoSLogLookup(Oid relid, ItemPointer tid,
							TransactionId xid_filter,
							RecnoSLogEntry *entries, int max_entries);
extern int	RecnoSLogLookupAll(Oid relid, ItemPointer tid,
							   RecnoSLogEntry *entries, int max_entries);
extern bool RecnoSLogHasEntry(Oid relid, ItemPointer tid);
extern bool RecnoSLogHasAbortedEntry(Oid relid, ItemPointer tid);
extern bool RecnoSLogIsInsertedByMe(Oid relid, ItemPointer tid);
extern bool RecnoSLogIsDeletedByMe(Oid relid, ItemPointer tid);
extern TransactionId RecnoSLogGetDirtyXid(Oid relid, ItemPointer tid,
										  bool *is_insert);
extern bool RecnoSLogHasLockConflict(Oid relid, ItemPointer tid,
									 TransactionId my_xid,
									 RecnoSLogOpType requested_lock);

/* Xact callbacks (registered lazily per backend). */
extern void RecnoSLogXactCallback(XactEvent event, void *arg);
extern void RecnoSLogSubXactCallback(SubXactEvent event,
									 SubTransactionId mySubid,
									 SubTransactionId parentSubid, void *arg);

#endif							/* RECNO_SLOG_H */
