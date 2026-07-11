/*-------------------------------------------------------------------------
 *
 * backend_startup.h
 *	  prototypes for backend_startup.c.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/tcop/backend_startup.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BACKEND_STARTUP_H
#define BACKEND_STARTUP_H

#include "utils/backend_runtime.h"
#include "utils/global_lifetime.h"
#include "utils/timestamp.h"

/* GUCs */
extern PGDLLIMPORT PG_GLOBAL_RUNTIME bool Trace_connection_negotiation;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME uint32 log_connections;
extern PGDLLIMPORT PG_GLOBAL_RUNTIME char *log_connections_string;

/*
 * Connection timing state remains source-compatible, but storage belongs to
 * PgConnection so a logical connection can move between carriers.
 */
#define conn_timing (*PgCurrentConnectionTimingRef())

/*
 * CAC_state is passed from postmaster to the backend process, to indicate
 * whether the connection should be accepted, or if the process should just
 * send an error to the client and close the connection.  Note that the
 * connection can fail for various reasons even if postmaster passed CAC_OK.
 */
typedef enum CAC_state
{
	CAC_OK,
	CAC_STARTUP,
	CAC_SHUTDOWN,
	CAC_RECOVERY,
	CAC_NOTHOTSTANDBY,
	CAC_TOOMANY,
} CAC_state;

/*
 * Physical startup environment for a client backend.
 *
 * Process mode keeps the historical SIGTERM/startup-timeout handling that can
 * safely call _exit() before shared memory has been touched. Threaded startup
 * must route those events through logical backend exit instead, and is wired
 * in a later Phase 10 slice.
 */
typedef enum BackendStartupMode
{
	BACKEND_STARTUP_PROCESS,
	BACKEND_STARTUP_THREAD
} BackendStartupMode;

/* Information passed from postmaster to backend process in 'startup_data' */
typedef struct BackendStartupData
{
	CAC_state	canAcceptConnections;

	/*
	 * Time at which the connection client socket is created. Only used for
	 * client and wal sender connections.
	 */
	TimestampTz socket_created;

	/*
	 * Time at which the postmaster initiates process creation -- either
	 * through fork or otherwise. Only used for client and wal sender
	 * connections.
	 */
	TimestampTz fork_started;
} BackendStartupData;

/*
 * Granular control over which messages to log for the log_connections GUC.
 *
 * RECEIPT, AUTHENTICATION, AUTHORIZATION, and SETUP_DURATIONS are different
 * aspects of connection establishment and backend setup for which we may emit
 * a log message.
 *
 * ALL is a convenience alias equivalent to all of the above aspects.
 *
 * ON is backwards compatibility alias for the connection aspects that were
 * logged in Postgres versions < 18.
 */
typedef enum LogConnectionOption
{
	LOG_CONNECTION_RECEIPT = (1 << 0),
	LOG_CONNECTION_AUTHENTICATION = (1 << 1),
	LOG_CONNECTION_AUTHORIZATION = (1 << 2),
	LOG_CONNECTION_SETUP_DURATIONS = (1 << 3),
	LOG_CONNECTION_ON =
		LOG_CONNECTION_RECEIPT |
		LOG_CONNECTION_AUTHENTICATION |
		LOG_CONNECTION_AUTHORIZATION,
	LOG_CONNECTION_ALL =
		LOG_CONNECTION_RECEIPT |
		LOG_CONNECTION_AUTHENTICATION |
		LOG_CONNECTION_AUTHORIZATION |
		LOG_CONNECTION_SETUP_DURATIONS,
}			LogConnectionOption;

pg_noreturn extern void BackendMain(const void *startup_data, size_t startup_data_len);
extern PgSession *BackendStartSessionWithStartupData(const BackendStartupData *startup_data,
													 struct ClientSocket *client_sock,
													 BackendStartupMode startup_mode);
pg_noreturn extern void BackendMainWithStartupData(const BackendStartupData *startup_data,
												  struct ClientSocket *client_sock,
												  BackendStartupMode startup_mode);

#endif							/* BACKEND_STARTUP_H */
