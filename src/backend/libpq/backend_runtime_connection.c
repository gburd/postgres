/*-------------------------------------------------------------------------
 *
 * backend_runtime_connection.c
 *	  Runtime bridge accessors for frontend/backend connection state.
 *
 * These accessors keep backend libpq and startup compatibility globals mapped
 * onto the current PgConnection while leaving runtime construction and early
 * fallback ownership in utils/init/backend_runtime.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/libpq/backend_runtime_connection.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "libpq/libpq.h"
#include "tcop/dest.h"
#include "../utils/init/backend_runtime_internal.h"

struct Port **
PgConnectionProcPortRef(PgConnection *connection)
{
	return &PgConnectionIdentityStateRef(connection)->port;
}

struct Port **
PgCurrentProcPortRef(void)
{
	return PgConnectionProcPortRef(CurrentPgConnection);
}

uint8 *
PgConnectionCancelKey(PgConnection *connection)
{
	return PgConnectionIdentityStateRef(connection)->cancel_key;
}

uint8 *
PgCurrentCancelKey(void)
{
	return PgConnectionCancelKey(CurrentPgConnection);
}

int *
PgConnectionCancelKeyLengthRef(PgConnection *connection)
{
	return &PgConnectionIdentityStateRef(connection)->cancel_key_length;
}

int *
PgCurrentCancelKeyLengthRef(void)
{
	return PgConnectionCancelKeyLengthRef(CurrentPgConnection);
}

PgConnectionSocketIOState *
PgConnectionSocketIORef(PgConnection *connection)
{
	return PgConnectionSocketIOStateRef(connection);
}

PgConnectionSocketIOState *
PgCurrentConnectionSocketIORef(void)
{
	return PgConnectionSocketIORef(CurrentPgConnection);
}

int *
PgCurrentPgwin32NoBlockRef(void)
{
	return &PgCurrentConnectionSocketIORef()->win32_noblock;
}

const PQcommMethods **
PgConnectionPqCommMethodsRef(PgConnection *connection)
{
	return &PgConnectionProtocolStateRef(connection)->comm_methods;
}

const PQcommMethods **
PgCurrentPqCommMethodsRef(void)
{
	return PgConnectionPqCommMethodsRef(CurrentPgConnection);
}

WaitEventSet **
PgConnectionFeBeWaitSetRef(PgConnection *connection)
{
	return &PgConnectionProtocolStateRef(connection)->fe_be_wait_set;
}

WaitEventSet **
PgCurrentFeBeWaitSetRef(void)
{
	return PgConnectionFeBeWaitSetRef(CurrentPgConnection);
}

uint32 *
PgConnectionFrontendProtocolRef(PgConnection *connection)
{
	return &PgConnectionProtocolStateRef(connection)->frontend_protocol;
}

uint32 *
PgCurrentFrontendProtocolRef(void)
{
	return PgConnectionFrontendProtocolRef(CurrentPgConnection);
}

static CommandDest *
PgConnectionWhereToSendOutputRef(PgConnection *connection)
{
	return &PgConnectionOutputStateRef(connection)->where_to_send_output;
}

CommandDest *
PgCurrentWhereToSendOutputRef(void)
{
	return PgConnectionWhereToSendOutputRef(CurrentPgConnection);
}

static int *
PgConnectionClientConnectionCheckIntervalRef(PgConnection *connection)
{
	return &PgConnectionOutputStateRef(connection)->client_connection_check_interval;
}

int *
PgCurrentClientConnectionCheckIntervalRef(void)
{
	return PgConnectionClientConnectionCheckIntervalRef(CurrentPgConnection);
}

volatile sig_atomic_t *
PgConnectionCheckClientConnectionPendingRef(PgConnection *connection)
{
	return &PgConnectionInterruptStateRef(connection)->check_client_connection_pending;
}

volatile sig_atomic_t *
PgCurrentCheckClientConnectionPendingRef(void)
{
	return PgConnectionCheckClientConnectionPendingRef(CurrentPgConnection);
}

volatile sig_atomic_t *
PgConnectionClientConnectionLostRef(PgConnection *connection)
{
	return &PgConnectionInterruptStateRef(connection)->client_connection_lost;
}

volatile sig_atomic_t *
PgCurrentClientConnectionLostRef(void)
{
	return PgConnectionClientConnectionLostRef(CurrentPgConnection);
}

bool *
PgConnectionClientAuthInProgressRef(PgConnection *connection)
{
	return &PgConnectionStartupStateRef(connection)->client_auth_in_progress;
}

bool *
PgCurrentClientAuthInProgressRef(void)
{
	return PgConnectionClientAuthInProgressRef(CurrentPgConnection);
}

struct ClientSocket **
PgConnectionClientSocketRef(PgConnection *connection)
{
	return &PgConnectionStartupStateRef(connection)->client_socket;
}

struct ClientSocket **
PgCurrentClientSocketRef(void)
{
	return PgConnectionClientSocketRef(CurrentPgConnection);
}

static ConnectionTiming *
PgConnectionTimingRef(PgConnection *connection)
{
	return &PgConnectionStartupStateRef(connection)->timing;
}

ConnectionTiming *
PgCurrentConnectionTimingRef(void)
{
	return PgConnectionTimingRef(CurrentPgConnection);
}

bool *
PgCurrentConnectionWarningsEmittedRef(void)
{
	return &PgConnectionStartupStateRef(CurrentPgConnection)->connection_warnings_emitted;
}

List **
PgCurrentConnectionWarningMessagesRef(void)
{
	return &PgConnectionStartupStateRef(CurrentPgConnection)->connection_warning_messages;
}

List **
PgCurrentConnectionWarningDetailsRef(void)
{
	return &PgConnectionStartupStateRef(CurrentPgConnection)->connection_warning_details;
}

void *
PgConnectionClientConnectionInfoRef(PgConnection *connection)
{
	return PgConnectionClientConnectionInfoStateRef(connection);
}

void *
PgCurrentClientConnectionInfoRef(void)
{
	return PgConnectionClientConnectionInfoRef(CurrentPgConnection);
}

PgConnectionSecurityState *
PgConnectionSecurityStateRef(PgConnection *connection)
{
	return PgConnectionRuntimeSecurityStateRef(connection);
}

PgConnectionSecurityState *
PgCurrentConnectionSecurityStateRef(void)
{
	return PgConnectionSecurityStateRef(CurrentPgConnection);
}
