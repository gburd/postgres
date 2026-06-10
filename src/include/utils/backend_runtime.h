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
#include "miscadmin.h"
#include "utils/global_lifetime.h"

typedef struct PgRuntime PgRuntime;
typedef struct PgCarrier PgCarrier;
typedef struct PgBackend PgBackend;
typedef struct PgSession PgSession;
typedef struct PgConnection PgConnection;
typedef struct PgExecution PgExecution;

typedef enum PgRuntimeKind
{
	PG_RUNTIME_PROCESS
} PgRuntimeKind;

typedef enum PgCarrierKind
{
	PG_CARRIER_PROCESS
} PgCarrierKind;

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
	PgRuntime  *runtime;
	PgCarrier  *carrier;
	PgSession  *session;
	PgConnection *connection;
	PgExecution *execution;
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

extern PGDLLIMPORT PG_GLOBAL_RUNTIME PgRuntime *CurrentPgRuntime;
extern PGDLLIMPORT PG_GLOBAL_CARRIER PgCarrier *CurrentPgCarrier;
extern PGDLLIMPORT PG_GLOBAL_CARRIER PgBackend *CurrentPgBackend;
extern PGDLLIMPORT PG_GLOBAL_CARRIER PgSession *CurrentPgSession;
extern PGDLLIMPORT PG_GLOBAL_CARRIER PgConnection *CurrentPgConnection;
extern PGDLLIMPORT PG_GLOBAL_CARRIER PgExecution *CurrentPgExecution;

extern void InitializePgProcessRuntime(void);
extern void PgProcessRuntimeAttachSession(Session *session);
extern PgStepResult PgSessionStep(PgSession *session, PgStepBudget budget);
pg_noreturn extern void PgSessionRun(PgSession *session);

#endif							/* BACKEND_RUNTIME_H */
