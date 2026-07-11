/*-------------------------------------------------------------------------
 *
 * walsender.h
 *	  Exports from replication/walsender.c.
 *
 * Portions Copyright (c) 2010-2026, PostgreSQL Global Development Group
 *
 * src/include/replication/walsender.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _WALSENDER_H
#define _WALSENDER_H

#include "access/xlogdefs.h"
#include "utils/backend_runtime.h"
#include "utils/global_lifetime.h"

/*
 * What to do with a snapshot in create replication slot command.
 */
typedef enum
{
	CRS_EXPORT_SNAPSHOT,
	CRS_NOEXPORT_SNAPSHOT,
	CRS_USE_SNAPSHOT,
} CRSSnapshotAction;

/* global state */
#define am_walsender (PgCurrentWalSenderState()->is_walsender)
#define am_cascading_walsender \
	(PgCurrentWalSenderState()->is_cascading_walsender)
#define am_db_walsender (PgCurrentWalSenderState()->is_db_walsender)
#define wake_wal_senders (PgCurrentWalSenderState()->wake_requested)

/* user-settable parameters */
extern PGDLLIMPORT PG_GLOBAL_RUNTIME int max_wal_senders;
#ifndef PgCurrentWalSenderTimeoutRef
extern int *PgCurrentWalSenderTimeoutRef(void);
#endif
#ifndef PgCurrentWalSenderShutdownTimeoutRef
extern int *PgCurrentWalSenderShutdownTimeoutRef(void);
#endif
#ifndef PgCurrentLogReplicationCommandsRef
extern bool *PgCurrentLogReplicationCommandsRef(void);
#endif

#define wal_sender_timeout (*PgCurrentWalSenderTimeoutRef())
#define wal_sender_shutdown_timeout (*PgCurrentWalSenderShutdownTimeoutRef())
#define log_replication_commands (*PgCurrentLogReplicationCommandsRef())

extern void InitWalSender(void);
extern bool exec_replication_command(const char *cmd_string);
extern void WalSndErrorCleanup(void);
extern void PhysicalWakeupLogicalWalSnd(void);
extern XLogRecPtr GetStandbyFlushRecPtr(TimeLineID *tli);
extern void WalSndSignals(void);
extern void WalSndWakeup(bool physical, bool logical);
extern void WalSndInitStopping(void);
extern void WalSndWaitStopping(void);
extern void HandleWalSndInitStopping(void);
extern void WalSndRqstFileReload(void);

/*
 * Remember that we want to wakeup walsenders later
 *
 * This is separated from doing the actual wakeup because the writeout is done
 * while holding contended locks.
 */
#define WalSndWakeupRequest() \
	do { wake_wal_senders = true; } while (0)

/*
 * wakeup walsenders if there is work to be done
 */
static inline void
WalSndWakeupProcessRequests(bool physical, bool logical)
{
	if (wake_wal_senders)
	{
		wake_wal_senders = false;
		if (max_wal_senders > 0)
			WalSndWakeup(physical, logical);
	}
}

#endif							/* _WALSENDER_H */
