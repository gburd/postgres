/*-------------------------------------------------------------------------
 *
 * miscadmin.h
 *	  This file contains general postgres administration and initialization
 *	  stuff that used to be spread out between the following files:
 *		globals.h						global variables
 *		pdir.h							directory path crud
 *		pinit.h							postgres initialization
 *		pmod.h							processing modes
 *	  Over time, this has also become the preferred place for widely known
 *	  resource-limitation stuff, such as work_mem and check_stack_depth().
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/miscadmin.h
 *
 * NOTES
 *	  some of the information in this file should be moved to other files.
 *
 *-------------------------------------------------------------------------
 */
#ifndef MISCADMIN_H
#define MISCADMIN_H

#include <signal.h>

#include "datatype/timestamp.h" /* for TimestampTz */
#include "pgtime.h"				/* for pg_time_t */
#ifndef FRONTEND
#include "port/atomics.h"
#endif
#include "utils/backend_runtime_current.h"
#include "utils/global_lifetime.h"

struct PgConnection;


#define InvalidPid				(-1)


/*****************************************************************************
 *	  System interrupt and critical section handling
 *
 * There are two types of interrupts that a running backend needs to accept
 * without messing up its state: QueryCancel (SIGINT) and ProcDie (SIGTERM).
 * In both cases, we need to be able to clean up the current transaction
 * gracefully, so we can't respond to the interrupt instantaneously ---
 * there's no guarantee that internal data structures would be self-consistent
 * if the code is interrupted at an arbitrary instant.  Instead, the signal
 * handlers set flags that are checked periodically during execution.
 *
 * The CHECK_FOR_INTERRUPTS() macro is called at strategically located spots
 * where it is normally safe to accept a cancel or die interrupt.  In some
 * cases, we invoke CHECK_FOR_INTERRUPTS() inside low-level subroutines that
 * might sometimes be called in contexts that do *not* want to allow a cancel
 * or die interrupt.  The HOLD_INTERRUPTS() and RESUME_INTERRUPTS() macros
 * allow code to ensure that no cancel or die interrupt will be accepted,
 * even if CHECK_FOR_INTERRUPTS() gets called in a subroutine.  The interrupt
 * will be held off until CHECK_FOR_INTERRUPTS() is done outside any
 * HOLD_INTERRUPTS() ... RESUME_INTERRUPTS() section.
 *
 * There is also a mechanism to prevent query cancel interrupts, while still
 * allowing die interrupts: HOLD_CANCEL_INTERRUPTS() and
 * RESUME_CANCEL_INTERRUPTS().
 *
 * Note that ProcessInterrupts() has also acquired a number of tasks that
 * do not necessarily cause a query-cancel-or-die response.  Hence, it's
 * possible that it will just clear InterruptPending and return.
 *
 * INTERRUPTS_PENDING_CONDITION() can be checked to see whether an
 * interrupt needs to be serviced, without trying to do so immediately.
 * Some callers are also interested in INTERRUPTS_CAN_BE_PROCESSED(),
 * which tells whether ProcessInterrupts is sure to clear the interrupt.
 *
 * Special mechanisms are used to let an interrupt be accepted when we are
 * waiting for a lock or when we are waiting for command input (but, of
 * course, only if the interrupt holdoff counter is zero).  See the
 * related code for details.
 *
 * A lost connection is handled similarly, although the loss of connection
 * does not raise a signal, but is detected when we fail to write to the
 * socket. If there was a signal for a broken connection, we could make use of
 * it by setting ClientConnectionLost in the signal handler.
 *
 * A related, but conceptually distinct, mechanism is the "critical section"
 * mechanism.  A critical section not only holds off cancel/die interrupts,
 * but causes any ereport(ERROR) or ereport(FATAL) to become ereport(PANIC)
 * --- that is, a system-wide reset is forced.  Needless to say, only really
 * *critical* code should be marked as a critical section!	Currently, this
 * mechanism is only used for XLOG-related code.
 *
 *****************************************************************************/

/* these are marked volatile because they are set by signal handlers: */
typedef struct PgBackendPendingInterruptState
{
	volatile sig_atomic_t interrupt_pending;
	volatile sig_atomic_t query_cancel_pending;
	volatile sig_atomic_t proc_die_pending;
	volatile int proc_die_sender_pid;
	volatile int proc_die_sender_uid;
	volatile sig_atomic_t idle_in_transaction_session_timeout_pending;
	volatile sig_atomic_t transaction_timeout_pending;
	volatile sig_atomic_t idle_session_timeout_pending;
	volatile sig_atomic_t proc_signal_barrier_pending;
	volatile sig_atomic_t log_memory_context_pending;
	volatile sig_atomic_t idle_stats_update_timeout_pending;
	volatile sig_atomic_t config_reload_pending;
	volatile sig_atomic_t shutdown_request_pending;
	volatile sig_atomic_t wakeup_stop_pending;
	volatile sig_atomic_t autovac_launcher_pending;
	volatile sig_atomic_t checkpointer_shutdown_xlog_pending;
} PgBackendPendingInterruptState;

extern PgBackendPendingInterruptState *PgCurrentPendingInterruptStateRef(void);

static inline PgBackendPendingInterruptState *
PgCurrentPendingInterruptStateRefFast(void)
{
	return PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentPendingInterruptStateHotRef,
											CurrentPgBackend,
											PgCurrentPendingInterruptStateRef);
}

/*
 * Compatibility lvalues for the historic pending interrupt globals. The
 * storage now belongs to the current PgBackend object, while early startup
 * paths before runtime installation use backend_runtime.c fallback storage.
 */
#define InterruptPending \
	(PgCurrentPendingInterruptStateRefFast()->interrupt_pending)
#define QueryCancelPending \
	(PgCurrentPendingInterruptStateRefFast()->query_cancel_pending)
#define ProcDiePending \
	(PgCurrentPendingInterruptStateRefFast()->proc_die_pending)
#define ProcDieSenderPid \
	(PgCurrentPendingInterruptStateRefFast()->proc_die_sender_pid)
#define ProcDieSenderUid \
	(PgCurrentPendingInterruptStateRefFast()->proc_die_sender_uid)
#define IdleInTransactionSessionTimeoutPending \
	(PgCurrentPendingInterruptStateRefFast()->idle_in_transaction_session_timeout_pending)
#define TransactionTimeoutPending \
	(PgCurrentPendingInterruptStateRefFast()->transaction_timeout_pending)
#define IdleSessionTimeoutPending \
	(PgCurrentPendingInterruptStateRefFast()->idle_session_timeout_pending)
#define ProcSignalBarrierPending \
	(PgCurrentPendingInterruptStateRefFast()->proc_signal_barrier_pending)
#define LogMemoryContextPending \
	(PgCurrentPendingInterruptStateRefFast()->log_memory_context_pending)
#define IdleStatsUpdateTimeoutPending \
	(PgCurrentPendingInterruptStateRefFast()->idle_stats_update_timeout_pending)
#define ConfigReloadPending \
	(PgCurrentPendingInterruptStateRefFast()->config_reload_pending)
#define ShutdownRequestPending \
	(PgCurrentPendingInterruptStateRefFast()->shutdown_request_pending)
#define WakeupStopPending \
	(PgCurrentPendingInterruptStateRefFast()->wakeup_stop_pending)
#define AutoVacLauncherPending \
	(PgCurrentPendingInterruptStateRefFast()->autovac_launcher_pending)
#define CheckpointerShutdownXLOGPending \
	(PgCurrentPendingInterruptStateRefFast()->checkpointer_shutdown_xlog_pending)

#ifndef PgCurrentCheckClientConnectionPendingRef
extern volatile sig_atomic_t *PgCurrentCheckClientConnectionPendingRef(void);
#endif
#ifndef PgCurrentClientConnectionLostRef
extern volatile sig_atomic_t *PgCurrentClientConnectionLostRef(void);
#endif

#define CheckClientConnectionPending (*PgCurrentCheckClientConnectionPendingRef())
#define ClientConnectionLost (*PgCurrentClientConnectionLostRef())

/* these are marked volatile because they are examined by signal handlers: */
typedef struct PgBackendInterruptHoldoffState
{
	volatile uint32 interrupt_holdoff_count;
	volatile uint32 query_cancel_holdoff_count;
	volatile uint32 crit_section_count;
} PgBackendInterruptHoldoffState;

#ifndef PgCurrentInterruptHoldoffCountRef
extern volatile uint32 *PgCurrentInterruptHoldoffCountRef(void);
#endif
#ifndef PgCurrentQueryCancelHoldoffCountRef
extern volatile uint32 *PgCurrentQueryCancelHoldoffCountRef(void);
#endif
#ifndef PgCurrentCritSectionCountRef
extern volatile uint32 *PgCurrentCritSectionCountRef(void);
#endif

/*
 * Compatibility lvalues for the historic interrupt holdoff globals.  The
 * storage now belongs to the current PgBackend object, while early startup
 * paths before runtime installation use backend_runtime.c fallback storage.
 */
#define InterruptHoldoffCount \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentInterruptHoldoffCountHotRef, \
									   CurrentPgBackend, \
									   PgCurrentInterruptHoldoffCountRef))
#define QueryCancelHoldoffCount \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentQueryCancelHoldoffCountHotRef, \
									   CurrentPgBackend, \
									   PgCurrentQueryCancelHoldoffCountRef))
#define CritSectionCount \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentCritSectionCountHotRef, \
									   CurrentPgBackend, \
									   PgCurrentCritSectionCountRef))

/* in tcop/postgres.c */
extern void ProcessInterrupts(void);
extern bool PgCurrentBackendHasPendingInterrupts(void);
extern bool ProcSignalBackendInterruptsPending(void);
extern void *PgCurrentBackendInterruptMaskRef(void);

static inline bool
PgThreadedInterruptsPendingFast(void)
{
#ifdef FRONTEND
	return false;
#else
	void	   *backend_mask;

	backend_mask =
		PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentBackendInterruptMaskHotRef,
										 CurrentPgBackend,
										 PgCurrentBackendInterruptMaskRef);

	if (unlikely(backend_mask == NULL))
	{
		PG_RUNTIME_BRIDGE_COUNT_FALLBACK(interrupts);
		return PgCurrentBackendHasPendingInterrupts();
	}

	return pg_atomic_read_u32((pg_atomic_uint32 *) backend_mask) != 0;
#endif
}

/* Test whether an interrupt is pending */
#ifndef WIN32
#define INTERRUPTS_PENDING_CONDITION() \
	(unlikely(InterruptPending || \
			  PgThreadedInterruptsPendingFast()))
#else
#define INTERRUPTS_PENDING_CONDITION() \
	(unlikely(UNBLOCKED_SIGNAL_QUEUE()) ? \
	 pgwin32_dispatch_queued_signals() : (void) 0, \
	 unlikely(InterruptPending || \
			  PgThreadedInterruptsPendingFast()))
#endif

/* Service interrupt, if one is pending and it's safe to service it now */
#define CHECK_FOR_INTERRUPTS() \
do { \
	if (INTERRUPTS_PENDING_CONDITION()) \
		ProcessInterrupts(); \
} while(0)

/* Is ProcessInterrupts() guaranteed to clear InterruptPending? */
#define INTERRUPTS_CAN_BE_PROCESSED() \
	(InterruptHoldoffCount == 0 && CritSectionCount == 0 && \
	 QueryCancelHoldoffCount == 0)

#define HOLD_INTERRUPTS()  (InterruptHoldoffCount++)

#define RESUME_INTERRUPTS() \
do { \
	Assert(InterruptHoldoffCount > 0); \
	InterruptHoldoffCount--; \
} while(0)

#define HOLD_CANCEL_INTERRUPTS()  (QueryCancelHoldoffCount++)

#define RESUME_CANCEL_INTERRUPTS() \
do { \
	Assert(QueryCancelHoldoffCount > 0); \
	QueryCancelHoldoffCount--; \
} while(0)

#define START_CRIT_SECTION()  (CritSectionCount++)

#define END_CRIT_SECTION() \
do { \
	Assert(CritSectionCount > 0); \
	CritSectionCount--; \
} while(0)


/*****************************************************************************
 *	  globals.h --															 *
 *****************************************************************************/

/*
 * from utils/init/globals.c
 */
extern PGDLLIMPORT PG_GLOBAL_RUNTIME pid_t PostmasterPid;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME bool IsPostmasterEnvironment;
extern bool *(PgCurrentIsUnderPostmasterRef) (void);
#define IsUnderPostmaster \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentIsUnderPostmasterHotRef, \
									   CurrentPgCarrier, \
									   PgCurrentIsUnderPostmasterRef))
extern PGDLLIMPORT PG_GLOBAL_RUNTIME bool IsBinaryUpgrade;

#ifndef PgCurrentExitOnAnyErrorRef
extern bool *PgCurrentExitOnAnyErrorRef(void);
#endif
#define ExitOnAnyError (*PgCurrentExitOnAnyErrorRef())

extern PGDLLIMPORT PG_GLOBAL_RUNTIME char *DataDir;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME int data_directory_mode;

extern PGDLLIMPORT PG_GLOBAL_RUNTIME int NBuffers;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME int MaxBackends;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME int MaxConnections;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME int max_worker_processes;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME int max_parallel_workers;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME int autovacuum_max_parallel_workers;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME bool multithreaded;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME int pooled_protocol_carriers;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME int pooled_protocol_sticky_idle_ms;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME int pooled_protocol_hibernate_after_ms;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME int pooled_protocol_idle_memory_compaction;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME bool threaded_lazy_relcache_init_file;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME bool log_protocol_park_memory;

#define POOLED_PROTOCOL_IDLE_MEMORY_COMPACTION_OFF	0
#define POOLED_PROTOCOL_IDLE_MEMORY_COMPACTION_TRIM	1
#define POOLED_PROTOCOL_IDLE_MEMORY_COMPACTION_CACHE	2

extern PGDLLIMPORT PG_GLOBAL_RUNTIME int commit_timestamp_buffers;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME int multixact_member_buffers;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME int multixact_offset_buffers;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME int notify_buffers;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME int serializable_buffers;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME int subtransaction_buffers;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME int transaction_buffers;

#ifndef PgCurrentMyProcPidRef
extern int *PgCurrentMyProcPidRef(void);
#endif
#ifndef PgCurrentMyStartTimeRef
extern pg_time_t *PgCurrentMyStartTimeRef(void);
#endif
#ifndef PgCurrentMyStartTimestampRef
extern TimestampTz *PgCurrentMyStartTimestampRef(void);
#endif
#ifndef PgCurrentProcPortRef
extern struct Port **PgCurrentProcPortRef(void);
#endif
#ifndef PgCurrentMyLatchRef
extern struct Latch **PgCurrentMyLatchRef(void);
#endif
#ifndef PgCurrentCancelKey
extern uint8 *PgCurrentCancelKey(void);
#endif
#ifndef PgCurrentCancelKeyLengthRef
extern int *PgCurrentCancelKeyLengthRef(void);
#endif
#ifndef PgCurrentMyPMChildSlotRef
extern int *PgCurrentMyPMChildSlotRef(void);
#endif

#define MyProcPid (*PgCurrentMyProcPidRef())
#define MyStartTime (*PgCurrentMyStartTimeRef())
#define MyStartTimestamp (*PgCurrentMyStartTimestampRef())
#define MyProcPort \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentProcPortHotRef, \
									   CurrentPgConnection, \
									   PgCurrentProcPortRef))
#define MyLatch (*PgCurrentMyLatchRef())
#define MyCancelKey (PgCurrentCancelKey())
#define MyCancelKeyLength (*PgCurrentCancelKeyLengthRef())
#define MyPMChildSlot (*PgCurrentMyPMChildSlotRef())

#ifndef PgCurrentOutputFileNameRef
extern char *PgCurrentOutputFileNameRef(void);
#endif
#define OutputFileName (PgCurrentOutputFileNameRef())
extern PGDLLIMPORT PG_GLOBAL_RUNTIME char my_exec_path[];
extern PGDLLIMPORT PG_GLOBAL_RUNTIME char pkglib_path[];

#ifdef FORKEXEC_BACKEND
extern PGDLLIMPORT PG_GLOBAL_RUNTIME char postgres_exec_path[];
#endif

#ifndef PgCurrentMyDatabaseIdRef
extern Oid *PgCurrentMyDatabaseIdRef(void);
#endif
#ifndef PgCurrentMyDatabaseTableSpaceRef
extern Oid *PgCurrentMyDatabaseTableSpaceRef(void);
#endif
#ifndef PgCurrentMyDatabaseHasLoginEventTriggersRef
extern bool *PgCurrentMyDatabaseHasLoginEventTriggersRef(void);
#endif

#define MyDatabaseId \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentMyDatabaseIdHotRef, \
									   CurrentPgSession, \
									   PgCurrentMyDatabaseIdRef))
#define MyDatabaseTableSpace \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentMyDatabaseTableSpaceHotRef, \
									   CurrentPgSession, \
									   PgCurrentMyDatabaseTableSpaceRef))
#define MyDatabaseHasLoginEventTriggers \
	(*PgCurrentMyDatabaseHasLoginEventTriggersRef())

/*
 * Date/Time Configuration
 *
 * DateStyle defines the output formatting choice for date/time types:
 *	USE_POSTGRES_DATES specifies traditional Postgres format
 *	USE_ISO_DATES specifies ISO-compliant format
 *	USE_SQL_DATES specifies Oracle/Ingres-compliant format
 *	USE_GERMAN_DATES specifies German-style dd.mm/yyyy
 *
 * DateOrder defines the field order to be assumed when reading an
 * ambiguous date (anything not in YYYY-MM-DD format, with a four-digit
 * year field first, is taken to be ambiguous):
 *	DATEORDER_YMD specifies field order yy-mm-dd
 *	DATEORDER_DMY specifies field order dd-mm-yy ("European" convention)
 *	DATEORDER_MDY specifies field order mm-dd-yy ("US" convention)
 *
 * In the Postgres and SQL DateStyles, DateOrder also selects output field
 * order: day comes before month in DMY style, else month comes before day.
 *
 * The user-visible "DateStyle" run-time parameter subsumes both of these.
 */

/* valid DateStyle values */
#define USE_POSTGRES_DATES		0
#define USE_ISO_DATES			1
#define USE_SQL_DATES			2
#define USE_GERMAN_DATES		3
#define USE_XSD_DATES			4

/* valid DateOrder values */
#define DATEORDER_YMD			0
#define DATEORDER_DMY			1
#define DATEORDER_MDY			2

#ifndef PgCurrentDateStyleRef
extern int *PgCurrentDateStyleRef(void);
#endif
#ifndef PgCurrentDateOrderRef
extern int *PgCurrentDateOrderRef(void);
#endif
#ifndef PgCurrentIntervalStyleRef
extern int *PgCurrentIntervalStyleRef(void);
#endif

#define DateStyle (*PgCurrentDateStyleRef())
#define DateOrder (*PgCurrentDateOrderRef())

/*
 * IntervalStyles
 *	 INTSTYLE_POSTGRES			   Like Postgres < 8.4 when DateStyle = 'iso'
 *	 INTSTYLE_POSTGRES_VERBOSE	   Like Postgres < 8.4 when DateStyle != 'iso'
 *	 INTSTYLE_SQL_STANDARD		   SQL standard interval literals
 *	 INTSTYLE_ISO_8601			   ISO-8601-basic formatted intervals
 */
#define INTSTYLE_POSTGRES			0
#define INTSTYLE_POSTGRES_VERBOSE	1
#define INTSTYLE_SQL_STANDARD		2
#define INTSTYLE_ISO_8601			3

#define IntervalStyle (*PgCurrentIntervalStyleRef())

#define MAXTZLEN		10		/* max TZ name len, not counting tr. null */

extern PGDLLIMPORT PG_GLOBAL_RUNTIME bool enableFsync;
#ifndef PgCurrentAllowSystemTableModsRef
extern bool *PgCurrentAllowSystemTableModsRef(void);
#endif
#define allowSystemTableMods (*PgCurrentAllowSystemTableModsRef())

#ifndef PgCurrentWorkMemRef
extern int *PgCurrentWorkMemRef(void);
#endif
#ifndef PgCurrentHashMemMultiplierRef
extern double *PgCurrentHashMemMultiplierRef(void);
#endif
#ifndef PgCurrentMaintenanceWorkMemRef
extern int *PgCurrentMaintenanceWorkMemRef(void);
#endif
#ifndef PgCurrentMaxParallelMaintenanceWorkersRef
extern int *PgCurrentMaxParallelMaintenanceWorkersRef(void);
#endif

#define work_mem \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentWorkMemHotRef, \
									   CurrentPgSession, \
									   PgCurrentWorkMemRef))
#define hash_mem_multiplier \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentHashMemMultiplierHotRef, \
									   CurrentPgSession, \
									   PgCurrentHashMemMultiplierRef))
#define maintenance_work_mem \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentMaintenanceWorkMemHotRef, \
									   CurrentPgSession, \
									   PgCurrentMaintenanceWorkMemRef))
#define max_parallel_maintenance_workers \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentMaxParallelMaintenanceWorkersHotRef, \
									   CurrentPgSession, \
									   PgCurrentMaxParallelMaintenanceWorkersRef))

/*
 * Upper and lower hard limits for the buffer access strategy ring size
 * specified by the VacuumBufferUsageLimit GUC and BUFFER_USAGE_LIMIT option
 * to VACUUM and ANALYZE.
 */
#define MIN_BAS_VAC_RING_SIZE_KB 128
#define MAX_BAS_VAC_RING_SIZE_KB (16 * 1024 * 1024)

#ifndef PgCurrentVacuumBufferUsageLimitRef
extern int *PgCurrentVacuumBufferUsageLimitRef(void);
#endif
#ifndef PgCurrentVacuumCostPageHitRef
extern int *PgCurrentVacuumCostPageHitRef(void);
#endif
#ifndef PgCurrentVacuumCostPageMissRef
extern int *PgCurrentVacuumCostPageMissRef(void);
#endif
#ifndef PgCurrentVacuumCostPageDirtyRef
extern int *PgCurrentVacuumCostPageDirtyRef(void);
#endif
#ifndef PgCurrentVacuumCostLimitRef
extern int *PgCurrentVacuumCostLimitRef(void);
#endif
#ifndef PgCurrentVacuumCostDelayRef
extern double *PgCurrentVacuumCostDelayRef(void);
#endif

#define VacuumBufferUsageLimit (*PgCurrentVacuumBufferUsageLimitRef())
#define VacuumCostPageHit (*PgCurrentVacuumCostPageHitRef())
#define VacuumCostPageMiss (*PgCurrentVacuumCostPageMissRef())
#define VacuumCostPageDirty (*PgCurrentVacuumCostPageDirtyRef())
#define VacuumCostLimit (*PgCurrentVacuumCostLimitRef())
#define VacuumCostDelay (*PgCurrentVacuumCostDelayRef())

#ifndef PgCurrentVacuumCostBalanceRef
extern int *PgCurrentVacuumCostBalanceRef(void);
#endif
#ifndef PgCurrentVacuumCostActiveRef
extern bool *PgCurrentVacuumCostActiveRef(void);
#endif

#define VacuumCostBalance (*PgCurrentVacuumCostBalanceRef())
#define VacuumCostActive (*PgCurrentVacuumCostActiveRef())


/* in utils/misc/stack_depth.c */

#ifndef PgCurrentMaxStackDepthRef
extern int *PgCurrentMaxStackDepthRef(void);
#endif
#ifndef PgCurrentMaxStackDepthBytesRef
extern ssize_t *PgCurrentMaxStackDepthBytesRef(void);
#endif
#define max_stack_depth \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentMaxStackDepthHotRef, \
									   CurrentPgSession, \
									   PgCurrentMaxStackDepthRef))

/* Required daylight between max_stack_depth and the kernel limit, in bytes */
#define STACK_DEPTH_SLOP (512 * 1024)

typedef char *pg_stack_base_t;

extern pg_stack_base_t set_stack_base(void);
extern void restore_stack_base(pg_stack_base_t base);
extern void check_stack_depth(void);
extern bool stack_is_too_deep(void);
extern ssize_t get_stack_depth_rlimit(void);

/* in tcop/utility.c */
extern void PreventCommandIfReadOnly(const char *cmdname);
extern void PreventCommandIfParallelMode(const char *cmdname);
extern void PreventCommandDuringRecovery(const char *cmdname);

/*****************************************************************************
 *	  pdir.h --																 *
 *			POSTGRES directory path definitions.                             *
 *****************************************************************************/

/* flags to be OR'd to form sec_context */
#define SECURITY_LOCAL_USERID_CHANGE	0x0001
#define SECURITY_RESTRICTED_OPERATION	0x0002
#define SECURITY_NOFORCE_RLS			0x0004

#ifndef PgCurrentDatabasePathRef
extern char **PgCurrentDatabasePathRef(void);
#endif
#ifndef PgCurrentDatabasePathOwnedRef
extern bool *PgCurrentDatabasePathOwnedRef(void);
#endif
#define DatabasePath (*PgCurrentDatabasePathRef())

/* now in utils/init/miscinit.c */
extern void InitPostmasterChild(void);
extern void InitStandaloneProcess(const char *argv0);
extern void InitProcessLocalLatch(void);
extern void SwitchToSharedLatch(void);
extern void SwitchBackToLocalLatch(void);

/*
 * MyBackendType indicates what kind of a backend this is.
 *
 * If you add entries, please also update the child_process_kinds array in
 * launch_backend.c.
 */
typedef enum BackendType
{
	B_INVALID = 0,

	/* Backends and other backend-like processes */
	B_BACKEND,
	B_DEAD_END_BACKEND,
	B_AUTOVAC_LAUNCHER,
	B_AUTOVAC_WORKER,
	B_BG_WORKER,
	B_WAL_SENDER,
	B_SLOTSYNC_WORKER,

	B_STANDALONE_BACKEND,

	/*
	 * Auxiliary processes. These have PGPROC entries, but they are not
	 * attached to any particular database, and cannot run transactions or
	 * even take heavyweight locks. There can be only one of each of these
	 * running at a time, except for IO workers.
	 *
	 * If you modify these, make sure to update NUM_AUXILIARY_PROCS and the
	 * glossary in the docs.
	 */
	B_ARCHIVER,
	B_BG_WRITER,
	B_CHECKPOINTER,
	B_IO_WORKER,
	B_STARTUP,
	B_WAL_RECEIVER,
	B_WAL_SUMMARIZER,
	B_WAL_WRITER,

	/*
	 * Data checksums processes are dynamic background workers, but they use
	 * dedicated backend types for pgstat I/O accounting.
	 */
	B_DATACHECKSUMSWORKER_LAUNCHER,
	B_DATACHECKSUMSWORKER_WORKER,

	/*
	 * Logger is not connected to shared memory and does not have a PGPROC
	 * entry.
	 */
	B_LOGGER,
} BackendType;

#define BACKEND_NUM_TYPES (B_LOGGER + 1)

extern BackendType *(PgCurrentMyBackendTypeRef) (void);
#define MyBackendType (*PgCurrentMyBackendTypeRef())

#define AmRegularBackendProcess()	(MyBackendType == B_BACKEND)
#define AmAutoVacuumLauncherProcess() (MyBackendType == B_AUTOVAC_LAUNCHER)
#define AmAutoVacuumWorkerProcess()	(MyBackendType == B_AUTOVAC_WORKER)
#define AmBackgroundWorkerProcess() (MyBackendType == B_BG_WORKER)
#define AmWalSenderProcess()        (MyBackendType == B_WAL_SENDER)
#define AmLogicalSlotSyncWorkerProcess() (MyBackendType == B_SLOTSYNC_WORKER)
#define AmArchiverProcess()			(MyBackendType == B_ARCHIVER)
#define AmBackgroundWriterProcess() (MyBackendType == B_BG_WRITER)
#define AmCheckpointerProcess()		(MyBackendType == B_CHECKPOINTER)
#define AmStartupProcess()			(MyBackendType == B_STARTUP)
#define AmWalReceiverProcess()		(MyBackendType == B_WAL_RECEIVER)
#define AmWalSummarizerProcess()	(MyBackendType == B_WAL_SUMMARIZER)
#define AmWalWriterProcess()		(MyBackendType == B_WAL_WRITER)
#define AmIoWorkerProcess()			(MyBackendType == B_IO_WORKER)
#define AmDataChecksumsWorkerProcess() \
	(MyBackendType == B_DATACHECKSUMSWORKER_LAUNCHER || \
	 MyBackendType == B_DATACHECKSUMSWORKER_WORKER)

#define AmSpecialWorkerProcess() \
	(AmAutoVacuumLauncherProcess() || \
	 AmLogicalSlotSyncWorkerProcess())

/*
 * Backend types that are spawned by the postmaster to serve a client or
 * replication connection. These backend types have in common that they are
 * externally initiated.
 */
#define IsExternalConnectionBackend(backend_type) \
	(backend_type == B_BACKEND || backend_type == B_WAL_SENDER)

extern const char *GetBackendTypeDesc(BackendType backendType);

extern void SetDatabasePath(const char *path);
extern void checkDataDir(void);
extern void SetDataDir(const char *dir);
extern void ChangeToDataDir(void);

extern char *GetUserNameFromId(Oid roleid, bool noerr);
extern Oid	GetUserId(void);
extern Oid	GetOuterUserId(void);
extern Oid	GetSessionUserId(void);
extern bool GetSessionUserIsSuperuser(void);
extern Oid	GetAuthenticatedUserId(void);
extern void SetAuthenticatedUserId(Oid userid);
extern void GetUserIdAndSecContext(Oid *userid, int *sec_context);
extern void SetUserIdAndSecContext(Oid userid, int sec_context);
extern bool InLocalUserIdChange(void);
extern bool InSecurityRestrictedOperation(void);
extern bool InNoForceRLSOperation(void);
extern void GetUserIdAndContext(Oid *userid, bool *sec_def_context);
extern void SetUserIdAndContext(Oid userid, bool sec_def_context);
extern void InitializeSessionUserId(const char *rolename, Oid roleid,
									bool bypass_login_check);
extern void InitializeSessionUserIdStandalone(void);
extern void SetSessionAuthorization(Oid userid, bool is_superuser);
extern Oid	GetCurrentRoleId(void);
extern void SetCurrentRoleId(Oid roleid, bool is_superuser);
extern void InitializeSystemUser(const char *authn_id,
								 const char *auth_method);
extern const char *GetSystemUser(void);

/* in utils/misc/superuser.c */
extern bool superuser(void);	/* current user is superuser */
extern bool superuser_arg(Oid roleid);	/* given user is superuser */


/*****************************************************************************
 *	  pmod.h --																 *
 *			POSTGRES processing mode definitions.                            *
 *****************************************************************************/

/*
 * Description:
 *		There are three processing modes in POSTGRES.  They are
 * BootstrapProcessing or "bootstrap," InitProcessing or
 * "initialization," and NormalProcessing or "normal."
 *
 * The first two processing modes are used during special times. When the
 * system state indicates bootstrap processing, transactions are all given
 * transaction id "one" and are consequently guaranteed to commit. This mode
 * is used during the initial generation of template databases.
 *
 * Initialization mode: used while starting a backend, until all normal
 * initialization is complete.  Some code behaves differently when executed
 * in this mode to enable system bootstrapping.
 *
 * If a POSTGRES backend process is in normal mode, then all code may be
 * executed normally.
 */

typedef enum ProcessingMode
{
	BootstrapProcessing,		/* bootstrap creation of template database */
	InitProcessing,				/* initializing system */
	NormalProcessing,			/* normal processing */
} ProcessingMode;

#ifndef PgCurrentProcessingModeRef
extern ProcessingMode *PgCurrentProcessingModeRef(void);
#endif
#ifndef FRONTEND
extern void PgRuntimeAfterProcessingModeChange(ProcessingMode mode);
#else
#define PgRuntimeAfterProcessingModeChange(mode) ((void) 0)
#endif
#define Mode (*PgCurrentProcessingModeRef())

#define IsBootstrapProcessingMode() (Mode == BootstrapProcessing)
#define IsInitProcessingMode()		(Mode == InitProcessing)
#define IsNormalProcessingMode()	(Mode == NormalProcessing)

#define GetProcessingMode() Mode

#define SetProcessingMode(mode) \
	do { \
		Assert((mode) == BootstrapProcessing || \
				  (mode) == InitProcessing || \
				  (mode) == NormalProcessing); \
		Mode = (mode); \
		PgRuntimeAfterProcessingModeChange(mode); \
	} while(0)


/*****************************************************************************
 *	  pinit.h --															 *
 *			POSTGRES initialization and cleanup definitions.                 *
 *****************************************************************************/

/* in utils/init/postinit.c */
/* flags for InitPostgres() */
#define INIT_PG_LOAD_SESSION_LIBS		0x0001
#define INIT_PG_OVERRIDE_ALLOW_CONNS	0x0002
#define INIT_PG_OVERRIDE_ROLE_LOGIN		0x0004
extern void pg_split_opts(char **argv, int *argcp, const char *optstr);
extern void InitializeMaxBackends(void);
extern void InitializeFastPathLocks(void);
extern void InitPostgres(const char *in_dbname, Oid dboid,
						 const char *username, Oid useroid,
						 uint32 flags,
						 char *out_dbname);
extern void BaseInit(void);
typedef bool (*ConnectionWarningFilter) (void);
extern void StoreConnectionWarningForConnection(struct PgConnection *connection,
												const char *msg,
												const char *detail,
												ConnectionWarningFilter filter);
extern void StoreConnectionWarning(const char *msg, const char *detail,
								   ConnectionWarningFilter filter);

/* in utils/init/miscinit.c */
#ifndef PgCurrentIgnoreSystemIndexesRef
extern bool *PgCurrentIgnoreSystemIndexesRef(void);
#endif
#define IgnoreSystemIndexes (*PgCurrentIgnoreSystemIndexesRef())
extern PGDLLIMPORT PG_GLOBAL_RUNTIME bool process_shared_preload_libraries_in_progress;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME bool process_shared_preload_libraries_done;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME bool process_shmem_requests_in_progress;
#ifndef PgCurrentSessionPreloadLibrariesRef
extern char **PgCurrentSessionPreloadLibrariesRef(void);
#endif
extern PGDLLIMPORT PG_GLOBAL_RUNTIME char *shared_preload_libraries_string;
#ifndef PgCurrentLocalPreloadLibrariesRef
extern char **PgCurrentLocalPreloadLibrariesRef(void);
#endif

#define session_preload_libraries_string \
	(*PgCurrentSessionPreloadLibrariesRef())
#define local_preload_libraries_string \
	(*PgCurrentLocalPreloadLibrariesRef())

extern void CreateDataDirLockFile(bool amPostmaster);
extern void CreateSocketLockFile(const char *socketfile, bool amPostmaster,
								 const char *socketDir);
extern void TouchSocketLockFiles(void);
extern void AddToDataDirLockFile(int target_line, const char *str);
extern bool RecheckDataDirLockFile(void);
extern void ValidatePgVersion(const char *path);
extern void process_shared_preload_libraries(void);
extern void process_session_preload_libraries(void);
extern void process_shmem_requests(void);
extern void pg_bindtextdomain(const char *domain);
extern bool has_rolreplication(Oid roleid);

typedef void (*shmem_request_hook_type) (void);
extern PGDLLIMPORT PG_GLOBAL_RUNTIME shmem_request_hook_type shmem_request_hook;

extern Size EstimateClientConnectionInfoSpace(void);
extern void SerializeClientConnectionInfo(Size maxsize, char *start_address);
extern void RestoreClientConnectionInfo(char *conninfo);

/* in executor/nodeHash.c */
extern size_t get_hash_memory_limit(void);

#endif							/* MISCADMIN_H */
