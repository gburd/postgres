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
 * Main-loop state owned by PgSession. These fields used to be volatile locals
 * in PostgresMain(); keep them volatile because they must survive the
 * top-level longjmp used for backend error recovery.
 */
typedef struct PgSessionLoopState
{
	volatile bool send_ready_for_query;
	volatile bool idle_in_transaction_timeout_enabled;
	volatile bool idle_session_timeout_enabled;
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

extern PGDLLIMPORT PgRuntime *CurrentPgRuntime;
extern PGDLLIMPORT PgCarrier *CurrentPgCarrier;
extern PGDLLIMPORT PgBackend *CurrentPgBackend;
extern PGDLLIMPORT PgSession *CurrentPgSession;
extern PGDLLIMPORT PgConnection *CurrentPgConnection;
extern PGDLLIMPORT PgExecution *CurrentPgExecution;

extern void InitializePgProcessRuntime(void);
extern void PgProcessRuntimeAttachSession(Session *session);

#endif							/* BACKEND_RUNTIME_H */
