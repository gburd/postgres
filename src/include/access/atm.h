/*-------------------------------------------------------------------------
 *
 * atm.h
 *	  Aborted Transaction Map for CTR (Constant-Time Recovery)
 *
 * The ATM is a shared-memory structure that tracks aborted transactions
 * whose per-relation UNDO chains have not yet been applied (Logical
 * Revert). It enables O(1) visibility checks for aborted transactions
 * and drives the background Logical Revert worker.
 *
 * The ATM is now backed by the sLog (Secondary Log) shared-memory hash
 * tables defined in access/slog.h.  All ATM functions are thin wrappers
 * around sLog operations, preserving the existing API and WAL format.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/atm.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ATM_H
#define ATM_H

#include "access/atm_xlog.h"
#include "access/transam.h"
#include "datatype/timestamp.h"
#include "storage/lwlock.h"

/* Shared memory sizing and initialization */
extern Size ATMShmemSize(void);
extern void ATMShmemInit(void);

/* Core API */
extern bool ATMIsAborted(TransactionId xid);
extern bool ATMGetUndoChain(TransactionId xid, RelUndoRecPtr *chain_out);
extern bool ATMAddAborted(TransactionId xid, Oid dboid, Oid reloid,
						  RelUndoRecPtr chain_ptr);
extern void ATMForget(TransactionId xid);
extern void ATMMarkReverted(TransactionId xid);

/* Iteration for Logical Revert worker */
extern bool ATMGetNextUnreverted(TransactionId *xid_out, Oid *dboid_out,
								 Oid *reloid_out, RelUndoRecPtr *chain_out);

/* Recovery support */
extern void ATMRecoveryFinalize(void);

#endif							/* ATM_H */
