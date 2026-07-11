/*-------------------------------------------------------------------------
 *
 * backend_runtime_connection.c
 *	  Runtime bridge accessors for frontend/backend connection state.
 *
 * This file owns connection fallback state, connection construction/adoption,
 * closed-state reset, and backend libpq/startup compatibility accessors.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/libpq/backend_runtime_connection.c
 *
 *-------------------------------------------------------------------------
 */
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include "libpq/libpq.h"
#include "miscadmin.h"
#include "tcop/dest.h"
#include "../utils/init/backend_runtime_internal.h"

static PG_THREAD_LOCAL PG_GLOBAL_CONNECTION PgConnection early_connection_fallback = {
	.output = {
		.where_to_send_output = DestDebug
	},
	.startup = {
		.timing.ready_for_use = TIMESTAMP_MINUS_INFINITY
	}
};
#define early_connection_identity early_connection_fallback.identity
#define early_connection_socket_io early_connection_fallback.socket_io
#define early_connection_protocol early_connection_fallback.protocol
#define early_connection_output early_connection_fallback.output
#define early_connection_interrupts early_connection_fallback.interrupts
#define early_connection_startup early_connection_fallback.startup
#define early_client_connection_info \
	early_connection_fallback.client_connection_info
#define early_client_connection_info_context \
	early_connection_fallback.client_connection_info_context
#define early_client_connection_info_authn_id_owned \
	early_connection_fallback.client_connection_info_authn_id_owned
#define early_connection_security early_connection_fallback.security

static void PgConnectionAdoptEarlyIdentity(PgConnection *connection);
static void PgConnectionAdoptEarlySocketIO(PgConnection *connection);
static void PgConnectionAdoptEarlyProtocolState(PgConnection *connection);
static void PgConnectionInitializeOutputState(PgConnectionOutputState *output);
static void PgConnectionAdoptEarlyOutputState(PgConnection *connection);
static void PgConnectionInitializeStartupState(PgConnectionStartupState *startup);
static void PgConnectionAdoptEarlyInterruptState(PgConnection *connection);
static void PgConnectionAdoptEarlyStartupState(PgConnection *connection);
static void PgConnectionAdoptEarlyClientConnectionInfo(PgConnection *connection);
static void PgConnectionAdoptEarlyClientConnectionInfoContext(PgConnection *connection);
static void PgConnectionAdoptEarlyClientConnectionInfoAuthnIdOwned(PgConnection *connection);
static void PgConnectionResetClientConnectionInfoClosedState(PgConnection *connection);
static void PgConnectionAdoptEarlySecurityState(PgConnection *connection);

PgConnectionIdentityState *
PgConnectionIdentityStateRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_identity;

	return &connection->identity;
}

PgConnectionSocketIOState *
PgConnectionSocketIOStateRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_socket_io;

	return &connection->socket_io;
}

PgConnectionProtocolState *
PgConnectionProtocolStateRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_protocol;

	return &connection->protocol;
}

PgConnectionOutputState *
PgConnectionOutputStateRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_output;

	return &connection->output;
}

PgConnectionInterruptState *
PgConnectionInterruptStateRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_interrupts;

	return &connection->interrupts;
}

PgConnectionStartupState *
PgConnectionStartupStateRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_startup;

	return &connection->startup;
}

PgConnectionClientConnectionInfoState *
PgConnectionClientConnectionInfoStateRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_client_connection_info;

	return &connection->client_connection_info;
}

MemoryContext *
PgConnectionClientConnectionInfoContextRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_client_connection_info_context;

	return &connection->client_connection_info_context;
}

bool *
PgConnectionClientConnectionInfoAuthnIdOwnedRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_client_connection_info_authn_id_owned;

	return &connection->client_connection_info_authn_id_owned;
}

PgConnectionSecurityState *
PgConnectionRuntimeSecurityStateRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_security;

	return &connection->security;
}

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

MemoryContext *
PgConnectionPortContextRef(PgConnection *connection)
{
	return &PgConnectionIdentityStateRef(connection)->port_context;
}

MemoryContext *
PgCurrentPortContextRef(void)
{
	return PgConnectionPortContextRef(CurrentPgConnection);
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
	if (likely(CurrentPgConnectionSocketIORuntimeState != NULL))
		return CurrentPgConnectionSocketIORuntimeState;

	return PgConnectionSocketIORef(CurrentPgConnection);
}

MemoryContext *
PgConnectionSocketIOContextRef(PgConnection *connection)
{
	return &PgConnectionSocketIORef(connection)->socket_io_context;
}

MemoryContext *
PgCurrentConnectionSocketIOContextRef(void)
{
	return PgConnectionSocketIOContextRef(CurrentPgConnection);
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
	if (likely(CurrentPgConnectionProtocolRuntimeState != NULL))
		return &CurrentPgConnectionProtocolRuntimeState->comm_methods;

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
	if (likely(CurrentPgConnectionProtocolRuntimeState != NULL))
		return &CurrentPgConnectionProtocolRuntimeState->fe_be_wait_set;

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
	if (likely(CurrentPgConnectionProtocolRuntimeState != NULL))
		return &CurrentPgConnectionProtocolRuntimeState->frontend_protocol;

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

List **
PgCurrentConnectionWarningFiltersRef(void)
{
	return &PgConnectionStartupStateRef(CurrentPgConnection)->connection_warning_filters;
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

MemoryContext *
PgCurrentClientConnectionInfoContextRef(void)
{
	return PgConnectionClientConnectionInfoContextRef(CurrentPgConnection);
}

bool *
PgCurrentClientConnectionInfoAuthnIdOwnedRef(void)
{
	return PgConnectionClientConnectionInfoAuthnIdOwnedRef(CurrentPgConnection);
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

PG_RUNTIME_DEFINE_ADOPT_EARLY_ZERO(PgConnectionAdoptEarlyIdentity,
								   PgConnection, connection, identity,
								   early_connection_identity)
PG_RUNTIME_DEFINE_ADOPT_EARLY_ZERO(PgConnectionAdoptEarlySocketIO,
								   PgConnection, connection, socket_io,
								   early_connection_socket_io)
PG_RUNTIME_DEFINE_ADOPT_EARLY_ZERO(PgConnectionAdoptEarlyProtocolState,
								   PgConnection, connection, protocol,
								   early_connection_protocol)

static void
PgConnectionInitializeOutputState(PgConnectionOutputState *output)
{
	Assert(output != NULL);

	MemSet(output, 0, sizeof(*output));
	output->where_to_send_output = DestDebug;
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgConnectionAdoptEarlyOutputState,
										PgConnection, connection, output,
										early_connection_output,
										PgConnectionInitializeOutputState)
PG_RUNTIME_DEFINE_ADOPT_EARLY_ZERO(PgConnectionAdoptEarlyInterruptState,
								   PgConnection, connection, interrupts,
								   early_connection_interrupts)

static void
PgConnectionInitializeStartupState(PgConnectionStartupState *startup)
{
	Assert(startup != NULL);

	MemSet(startup, 0, sizeof(*startup));
	startup->timing.ready_for_use = TIMESTAMP_MINUS_INFINITY;
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgConnectionAdoptEarlyStartupState,
										PgConnection, connection, startup,
										early_connection_startup,
										PgConnectionInitializeStartupState)
PG_RUNTIME_DEFINE_ADOPT_EARLY_ZERO(PgConnectionAdoptEarlyClientConnectionInfo,
								   PgConnection, connection,
								   client_connection_info,
								   early_client_connection_info)
PG_RUNTIME_DEFINE_ADOPT_EARLY_ZERO(PgConnectionAdoptEarlyClientConnectionInfoContext,
								   PgConnection, connection,
								   client_connection_info_context,
								   early_client_connection_info_context)

static void
PgConnectionAdoptEarlyClientConnectionInfoAuthnIdOwned(PgConnection *connection)
{
	Assert(connection != NULL);

	connection->client_connection_info_authn_id_owned =
		early_client_connection_info_authn_id_owned;
	early_client_connection_info_authn_id_owned = false;
}

static void
PgConnectionResetClientConnectionInfoClosedState(PgConnection *connection)
{
	Assert(connection != NULL);

	if (connection->client_connection_info_authn_id_owned &&
		connection->client_connection_info.authn_id != NULL &&
		connection->client_connection_info_context == NULL)
		pfree((void *) connection->client_connection_info.authn_id);

	MemSet(&connection->client_connection_info, 0,
		   sizeof(connection->client_connection_info));
	connection->client_connection_info_authn_id_owned = false;
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_ZERO(PgConnectionAdoptEarlySecurityState,
								   PgConnection, connection, security,
								   early_connection_security)

static void
PgConnectionResetIdentityClosedState(PgConnection *connection)
{
	Assert(connection != NULL);

	if (CurrentPgConnection == connection &&
		MyProcPort == connection->identity.port)
		MyProcPort = NULL;

	PG_RUNTIME_DELETE_MEMORY_CONTEXT(connection->identity.port_context);
	connection->identity.port = NULL;
	MemSet(connection->identity.cancel_key, 0,
		   sizeof(connection->identity.cancel_key));
	connection->identity.cancel_key_length = 0;
}

static void
PgConnectionResetSocketIOClosedState(PgConnection *connection)
{
	Assert(connection != NULL);

	/*
	 * socket_close() releases the palloc-backed send buffer and wait set.
	 * This reset makes the retained logical connection object stop pointing
	 * at resources that no longer exist.
	 */
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(connection->socket_io.socket_io_context);
	MemSet(&connection->socket_io, 0, sizeof(connection->socket_io));
}

static void
PgConnectionResetProtocolClosedState(PgConnection *connection)
{
	Assert(connection != NULL);

	connection->protocol.comm_methods = NULL;
	connection->protocol.fe_be_wait_set = NULL;
	connection->protocol.frontend_protocol = 0;
}

static void
PgConnectionResetStartupClosedState(PgConnection *connection)
{
	Assert(connection != NULL);

	connection->startup.client_auth_in_progress = false;
	connection->startup.client_socket = NULL;
	if (connection->startup.connection_warning_context != NULL)
	{
		if (CurrentMemoryContext == connection->startup.connection_warning_context)
			MemoryContextSwitchTo(TopMemoryContext);
		PG_RUNTIME_DELETE_MEMORY_CONTEXT(connection->startup.connection_warning_context);
	}
	else
	{
		list_free_deep(connection->startup.connection_warning_messages);
		list_free_deep(connection->startup.connection_warning_details);
		/* filters are function pointers, not owned allocations */
		list_free(connection->startup.connection_warning_filters);
	}
	connection->startup.connection_warnings_emitted = false;
	connection->startup.connection_warning_messages = NIL;
	connection->startup.connection_warning_details = NIL;
	connection->startup.connection_warning_filters = NIL;
}

static void
PgConnectionResetSecurityClosedState(PgConnection *connection)
{
	PgConnectionSecurityState *security;

	Assert(connection != NULL);

	/*
	 * GSSAPI connection buffers are malloc-backed in be-secure-gssapi.c.
	 * PAM fields are borrowed authentication-time pointers, so reset them but
	 * do not free them here.
	 */
	security = &connection->security;
	free(security->gss_send_buffer);
	free(security->gss_recv_buffer);
	free(security->gss_result_buffer);
	MemSet(security, 0, sizeof(*security));
}

void
PgConnectionInitializeRuntimeObject(PgConnection *connection,
									PgBackend *backend,
									PgSession *session,
									struct Port *port)
{
	Assert(connection != NULL);

#define PG_CONNECTION_BUCKET(field, init, adopt, reset) \
	do { init; } while (0);
#include "../utils/init/backend_runtime_connection_buckets.def"
#undef PG_CONNECTION_BUCKET
}

void
PgConnectionResetClosedState(PgConnection *connection)
{
	Assert(connection != NULL);

#define PG_CONNECTION_BUCKET(field, init, adopt, reset) \
	do { reset; } while (0);
#include "../utils/init/backend_runtime_connection_buckets.def"
#undef PG_CONNECTION_BUCKET
}

void
PgConnectionAdoptEarlyState(PgConnection *connection,
							struct Port *preserved_port)
{
	Assert(connection != NULL);

#define PG_CONNECTION_BUCKET(field, init, adopt, reset) \
	do { adopt; } while (0);
#include "../utils/init/backend_runtime_connection_buckets.def"
#undef PG_CONNECTION_BUCKET
}
