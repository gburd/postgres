/*-------------------------------------------------------------------------
 *
 * slotsync.h
 *	  Exports for slot synchronization.
 *
 * Portions Copyright (c) 2016-2026, PostgreSQL Global Development Group
 *
 * src/include/replication/slotsync.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SLOTSYNC_H
#define SLOTSYNC_H

#include <signal.h>

#include "replication/walreceiver.h"
#include "utils/backend_runtime.h"
#include "utils/global_lifetime.h"

extern PGDLLIMPORT PG_GLOBAL_RUNTIME bool sync_replication_slots;

/* Interrupt flag set by HandleSlotSyncMessageInterrupt() */
#define SlotSyncShutdownPending \
	(PgCurrentLogicalReplicationState()->slotsync_shutdown_pending)

/*
 * GUCs needed by slot sync worker to connect to the primary
 * server and carry on with slots synchronization.
 */
extern PGDLLIMPORT PG_GLOBAL_RUNTIME char *PrimaryConnInfo;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME char *PrimarySlotName;

extern char *CheckAndGetDbnameFromConninfo(void);
extern bool ValidateSlotSyncParams(int elevel);

pg_noreturn extern void ReplSlotSyncWorkerMain(const void *startup_data, size_t startup_data_len);

extern void ShutDownSlotSync(void);
extern bool SlotSyncWorkerCanRestart(void);
extern bool IsSyncingReplicationSlots(void);
extern void SyncReplicationSlots(WalReceiverConn *wrconn);
extern void HandleSlotSyncMessageInterrupt(void);
extern void ProcessSlotSyncMessage(void);

#endif							/* SLOTSYNC_H */
