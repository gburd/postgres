/*-------------------------------------------------------------------------
 *
 * backend_runtime.h
 *	  Runtime/backend/session scaffolding for backend execution.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/include/utils/backend_runtime.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BACKEND_RUNTIME_H
#define BACKEND_RUNTIME_H

#include "access/session.h"
#include "fmgr.h"
#include "lib/ilist.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/ipc.h"
#include "utils/backend_id.h"
#include "utils/global_lifetime.h"

typedef struct PgRuntime PgRuntime;
typedef struct PgCarrier PgCarrier;
typedef struct PgBackend PgBackend;
typedef struct PgSession PgSession;
typedef struct PgConnection PgConnection;
typedef struct PgExecution PgExecution;
typedef void (*PgBackendExitContinuation) (int code);
typedef int (*PgSuspendCallback) (void *callback_arg);

typedef enum PgRuntimeKind
{
	PG_RUNTIME_PROCESS,
	PG_RUNTIME_THREAD_PER_SESSION
} PgRuntimeKind;

typedef enum PgCarrierKind
{
	PG_CARRIER_PROCESS,
	PG_CARRIER_THREAD
} PgCarrierKind;

typedef enum PgBackendLaunchModel
{
	PG_BACKEND_LAUNCH_PROCESS,
	PG_BACKEND_LAUNCH_THREAD
} PgBackendLaunchModel;

/*
 * Budget for one invocation of PgSessionStep().  The process-mode runner uses
 * a single-message budget today; later schedulers can extend this contract
 * without changing the caller shape.
 */
typedef struct PgStepBudget
{
	int			max_messages;
} PgStepBudget;

typedef enum PgStepResult
{
	PG_STEP_CONTINUE,
	PG_STEP_ERROR_RECOVERED
} PgStepResult;

/*
 * Logical interrupts target a backend object first.  In process mode these are
 * bridged back to the historical volatile globals serviced by
 * CHECK_FOR_INTERRUPTS(); later backend models can route these bits without
 * depending on Unix signals as the in-process representation.
 */
typedef enum PgBackendInterruptType
{
	PG_BACKEND_INTERRUPT_QUERY_CANCEL,
	PG_BACKEND_INTERRUPT_PROC_DIE,
	PG_BACKEND_INTERRUPT_CLIENT_CONNECTION_CHECK,
	PG_BACKEND_INTERRUPT_IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
	PG_BACKEND_INTERRUPT_TRANSACTION_TIMEOUT,
	PG_BACKEND_INTERRUPT_IDLE_SESSION_TIMEOUT,
	PG_BACKEND_INTERRUPT_IDLE_STATS_UPDATE_TIMEOUT,
	PG_BACKEND_INTERRUPT_PROC_SIGNAL_BARRIER,
	PG_BACKEND_INTERRUPT_LOG_MEMORY_CONTEXT,
	PG_BACKEND_INTERRUPT_RECOVERY_CONFLICT,
	PG_BACKEND_INTERRUPT_CONFIG_RELOAD,
	PG_BACKEND_INTERRUPT_SHUTDOWN_REQUEST,
	PG_BACKEND_INTERRUPT_CATCHUP,
	PG_BACKEND_INTERRUPT_NOTIFY,
	PG_BACKEND_INTERRUPT_PARALLEL_MESSAGE,
	PG_BACKEND_INTERRUPT_PARALLEL_APPLY_MESSAGE,
	PG_BACKEND_INTERRUPT_SLOT_SYNC_MESSAGE,
	PG_BACKEND_INTERRUPT_REPACK_MESSAGE,
	PG_BACKEND_INTERRUPT_WAKEUP_STOP,
	PG_BACKEND_INTERRUPT_AUTOVAC_LAUNCHER,
	PG_BACKEND_INTERRUPT_CHECKPOINTER_SHUTDOWN_XLOG,
	PG_BACKEND_INTERRUPT_LOG_ROTATE,
	PG_BACKEND_INTERRUPT_COUNT
} PgBackendInterruptType;

typedef uint32 PgBackendInterruptMask;

#define PG_BACKEND_INTERRUPT_MASK(interrupt_type) \
	(((PgBackendInterruptMask) 1) << (interrupt_type))

typedef struct PgBackendInterruptMailbox
{
	pg_atomic_uint32 pending_mask;
	volatile int proc_die_sender_pid;
	volatile int proc_die_sender_uid;
} PgBackendInterruptMailbox;

typedef enum PgWaitKind
{
	PG_WAIT_KIND_NONE,
	PG_WAIT_KIND_EVENT_SET
} PgWaitKind;

typedef struct PgWaitSpec
{
	PgWaitKind	kind;
	uint32		wait_event_info;
	uint32		wake_events;
	long		timeout;
} PgWaitSpec;

typedef struct PgBackendWaitState
{
	PgWaitSpec	spec;
	pg_atomic_uint32 waiting;
} PgBackendWaitState;

/*
 * Main-loop state owned by PgSession. Some of this state used to be volatile
 * locals in PostgresMain(); keep the loop flags volatile because they must
 * survive the top-level longjmp used for backend error recovery.
 */
typedef struct PgSessionLoopState
{
	volatile bool send_ready_for_query;
	volatile bool idle_in_transaction_timeout_enabled;
	volatile bool idle_session_timeout_enabled;
	volatile bool doing_extended_query_message;
	volatile bool ignore_till_sync;
	volatile bool step_error_boundary_active;
} PgSessionLoopState;

struct PgRuntime
{
	PgRuntimeKind kind;
	PgCarrier  *current_carrier;
	PgBackendModel extension_backend_model;

	/*
	 * Optional continuation used after PgBackendExitCleanup().  Process mode
	 * leaves this NULL and falls through to exit().  A threaded runtime must
	 * install a handler that removes the logical backend from its scheduler
	 * without returning to the cleaned-up backend stack.
	 */
	PgBackendExitContinuation exit_backend;
};

struct PgCarrier
{
	PgCarrierKind kind;
	PgRuntime  *runtime;
	PgBackend  *current_backend;
	PgSession  *current_session;
	PgExecution *current_execution;
};

struct PgBackend
{
	PgBackendId id;
	PgRuntime  *runtime;
	PgCarrier  *carrier;
	PgSession  *session;
	PgConnection *connection;
	PgExecution *execution;
	PgBackendInterruptMailbox interrupts;
	struct Latch *interrupt_latch;
	PgBackendExitState exit_state;
	PgBackendWaitState wait_state;

	/* Backend-local dynamic shared memory mappings and detach callbacks. */
	dlist_head	dsm_segment_list;

	BackendType backend_type;
};

struct PgSession
{
	PgBackend  *backend;
	PgConnection *connection;
	PgExecution *execution;
	Session    *legacy_session;
	PgSessionLoopState loop_state;
};

struct PgConnection
{
	PgBackend  *backend;
	PgSession  *session;
	struct Port *port;
};

struct PgExecution
{
	PgBackend  *backend;
	PgSession  *session;
	PgCarrier  *carrier;
};

typedef struct PgThreadBackendRuntimeState
{
	PgCarrier	carrier;
	PgBackend	backend;
	PgSession	session;
	PgConnection connection;
	PgExecution execution;
} PgThreadBackendRuntimeState;

extern PGDLLIMPORT PG_GLOBAL_RUNTIME PgRuntime *CurrentPgRuntime;
extern PGDLLIMPORT PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgCarrier *CurrentPgCarrier;
extern PGDLLIMPORT PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgBackend *CurrentPgBackend;
extern PGDLLIMPORT PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgSession *CurrentPgSession;
extern PGDLLIMPORT PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgConnection *CurrentPgConnection;
extern PGDLLIMPORT PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgExecution *CurrentPgExecution;

extern void InitializePgProcessRuntime(void);
extern void InitializePgThreadRuntime(PgBackendExitContinuation exit_backend);
extern void InitializePgThreadBackendRuntimeState(PgThreadBackendRuntimeState *state,
												 BackendType backend_type,
												 struct Port *port,
												 struct Latch *interrupt_latch);
extern void InstallPgThreadBackendRuntimeState(PgThreadBackendRuntimeState *state);
extern void InitializePgThreadBackendRuntime(PgThreadBackendRuntimeState *state,
											 BackendType backend_type,
											 struct Port *port,
											 struct Latch *interrupt_latch);
extern void PgProcessRuntimeAttachSession(Session *session);
extern PgBackendLaunchModel PgRuntimeGetBackendLaunchModel(BackendType backend_type);
extern bool PgRuntimeShouldThreadBackend(BackendType backend_type);
extern PgBackendModel PgRuntimeGetExtensionBackendModel(void);
extern void PgRuntimeSetExtensionBackendModel(PgBackendModel backend_model);
extern void PgBackendInitializeInterrupts(PgBackend *backend);
extern void PgBackendSetInterruptLatch(PgBackend *backend,
										struct Latch *interrupt_latch);
extern PgBackendId PgBackendGetId(PgBackend *backend);
extern PgBackendId PgCurrentBackendId(void);
extern int	PgBackendGetSignalPid(PgBackend *backend);
extern int	PgCurrentBackendSignalPid(void);
extern bool PgBackendUsesProcessSignals(PgBackend *backend);
extern void PgBackendWakeup(PgBackend *backend);
extern void PgBackendRaiseInterrupt(PgBackend *backend,
									 PgBackendInterruptType interrupt_type);
extern void PgBackendRaiseProcDieInterrupt(PgBackend *backend, int sender_pid,
										   int sender_uid);
extern void PgCurrentBackendRaiseInterrupt(PgBackendInterruptType interrupt_type);
extern void PgCurrentBackendRaiseProcDieInterrupt(int sender_pid,
												 int sender_uid);
extern PgBackendInterruptMask PgBackendConsumeInterrupts(PgBackend *backend);
extern void PgBackendConsumeProcDieSender(PgBackend *backend, int *sender_pid,
										  int *sender_uid);
extern bool PgCurrentBackendHasPendingInterrupts(void);
extern void PgCurrentBackendApplyInterrupts(void);
extern int	PgSuspend(const PgWaitSpec *wait_spec,
					  PgSuspendCallback callback, void *callback_arg);
extern PgStepResult PgSessionStep(PgSession *session, PgStepBudget budget);
pg_noreturn extern void PgSessionRun(PgSession *session);

#endif							/* BACKEND_RUNTIME_H */
