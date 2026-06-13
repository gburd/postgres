/*-------------------------------------------------------------------------
 *
 * backend_runtime.c
 *	  Runtime/backend/session scaffolding for backend execution.
 *
 * The process-mode implementation uses static per-process objects. Later
 * phases can replace or extend these objects without changing callers that
 * already refer to the current runtime/backend/session/execution.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/utils/init/backend_runtime.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/parallel.h"
#include "commands/async.h"
#include "commands/repack.h"
#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "replication/logicalworker.h"
#include "replication/slotsync.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/sinval.h"
#include "utils/backend_runtime.h"
#include "utils/guc.h"
#include "utils/resowner.h"

PG_GLOBAL_RUNTIME PgRuntime *CurrentPgRuntime = NULL;
PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgCarrier *CurrentPgCarrier = NULL;
PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgBackend *CurrentPgBackend = NULL;
PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgSession *CurrentPgSession = NULL;
PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgConnection *CurrentPgConnection = NULL;
PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgExecution *CurrentPgExecution = NULL;

static PG_GLOBAL_RUNTIME PgRuntime process_runtime;
static PG_GLOBAL_RUNTIME PgRuntime thread_runtime;
static PG_GLOBAL_RUNTIME bool thread_runtime_initialized = false;
static PG_GLOBAL_RUNTIME bool backend_id_counter_initialized = false;
static PG_GLOBAL_RUNTIME pg_atomic_uint64 next_backend_id;
static PG_GLOBAL_CARRIER PgCarrier process_carrier;
static PG_GLOBAL_BACKEND PgBackend process_backend;
static PG_GLOBAL_SESSION PgSession process_session;
static PG_GLOBAL_CONNECTION PgConnection process_connection;
static PG_GLOBAL_EXECUTION PgExecution process_execution;
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgBackendCoreState early_backend_core = {
	.mode = InitProcessing
};
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND BackendType early_backend_type = B_INVALID;
static PG_THREAD_LOCAL PG_GLOBAL_CONNECTION PgConnectionIdentityState early_connection_identity;
static PG_THREAD_LOCAL PG_GLOBAL_CONNECTION PgConnectionSocketIOState early_connection_socket_io;
static PG_THREAD_LOCAL PG_GLOBAL_CONNECTION PgConnectionProtocolState early_connection_protocol;
static PG_THREAD_LOCAL PG_GLOBAL_CONNECTION PgConnectionInterruptState early_connection_interrupts;
static PG_THREAD_LOCAL PG_GLOBAL_CONNECTION PgConnectionStartupState early_connection_startup;
static PG_THREAD_LOCAL PG_GLOBAL_CONNECTION PgConnectionClientConnectionInfoState early_client_connection_info;
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionDatabaseState early_session_database;
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionDateTimeState early_session_datetime = {
	.initialized = true,
	.date_style = USE_ISO_DATES,
	.date_order = DATEORDER_MDY,
	.interval_style = INTSTYLE_POSTGRES
};
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionQueryMemoryState early_session_query_memory = {
	.initialized = true,
	.work_mem_kb = 4096,
	.hash_mem_multiplier_value = 2.0,
	.maintenance_work_mem_kb = 65536,
	.max_parallel_maintenance_workers_value = 2
};
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgBackendPendingInterruptState early_pending_interrupts;
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgBackendInterruptHoldoffState early_interrupt_holdoffs;
static PG_THREAD_LOCAL PG_GLOBAL_EXECUTION PgExecutionDebugState early_execution_debug;
static PG_THREAD_LOCAL PG_GLOBAL_EXECUTION PgExecutionErrorState early_execution_error;
static PG_THREAD_LOCAL PG_GLOBAL_EXECUTION PgExecutionMemoryContextState early_execution_memory_contexts;
static PG_THREAD_LOCAL PG_GLOBAL_EXECUTION PgExecutionResourceOwnerState early_execution_resource_owners;

StaticAssertDecl(PG_BACKEND_INTERRUPT_COUNT <= 32,
				 "PgBackendInterruptMask must fit all backend interrupts");

static void PgBackendInitializeIdCounter(void);
static PgBackendId PgBackendAssignId(void);
static void PgBackendWakeForInterrupt(PgBackend *backend);
static void PgConnectionAdoptEarlyIdentity(PgConnection *connection);
static void PgConnectionAdoptEarlySocketIO(PgConnection *connection);
static void PgConnectionAdoptEarlyProtocolState(PgConnection *connection);
static void PgConnectionAdoptEarlyInterruptState(PgConnection *connection);
static void PgConnectionAdoptEarlyStartupState(PgConnection *connection);
static void PgConnectionAdoptEarlyClientConnectionInfo(PgConnection *connection);
static void PgSessionAdoptEarlyDatabaseState(PgSession *session);
static void PgSessionInitializeDateTimeState(PgSessionDateTimeState *datetime);
static void PgSessionAdoptEarlyDateTimeState(PgSession *session);
static void PgSessionInitializeQueryMemoryState(PgSessionQueryMemoryState *query_memory);
static void PgSessionAdoptEarlyQueryMemoryState(PgSession *session);
static void PgBackendResetCoreState(PgBackendCoreState *core);
static void PgBackendAdoptEarlyCoreState(PgBackend *backend);
static void PgBackendAdoptEarlyPendingInterrupts(PgBackend *backend);
static void PgBackendAdoptEarlyInterruptHoldoffs(PgBackend *backend);
static BackendType *PgCurrentBackendTypeRef(void);
static void PgExecutionAdoptEarlyDebugState(PgExecution *execution);
static void PgExecutionAdoptEarlyErrorState(PgExecution *execution);
static void PgExecutionAdoptEarlyMemoryContexts(PgExecution *execution);
static void PgExecutionAdoptEarlyResourceOwners(PgExecution *execution);
static PgBackendCoreState *PgCurrentCoreState(void);
static PgSessionDatabaseState *PgCurrentSessionDatabaseState(void);
static PgSessionDateTimeState *PgCurrentSessionDateTimeState(void);
static PgSessionQueryMemoryState *PgCurrentSessionQueryMemoryState(void);
static PgExecutionErrorState *PgCurrentExecutionErrorState(void);
static PgExecutionMemoryContextState *PgCurrentExecutionMemoryContexts(void);
static PgExecutionResourceOwnerState *PgCurrentExecutionResourceOwners(void);
static PgBackendPendingInterruptState *PgCurrentPendingInterrupts(void);
static PgBackendInterruptHoldoffState *PgCurrentInterruptHoldoffs(void);

static void
PgBackendInitializeIdCounter(void)
{
	if (backend_id_counter_initialized)
		return;

	pg_atomic_init_u64(&next_backend_id, 0);
	backend_id_counter_initialized = true;
}

static PgBackendId
PgBackendAssignId(void)
{
	PgBackendInitializeIdCounter();

	return pg_atomic_add_fetch_u64(&next_backend_id, 1);
}

static void
PgConnectionAdoptEarlyIdentity(PgConnection *connection)
{
	Assert(connection != NULL);

	connection->identity = early_connection_identity;
	MemSet(&early_connection_identity, 0, sizeof(early_connection_identity));
}

static void
PgConnectionAdoptEarlySocketIO(PgConnection *connection)
{
	Assert(connection != NULL);

	connection->socket_io = early_connection_socket_io;
	MemSet(&early_connection_socket_io, 0, sizeof(early_connection_socket_io));
}

static void
PgConnectionAdoptEarlyProtocolState(PgConnection *connection)
{
	Assert(connection != NULL);

	connection->protocol = early_connection_protocol;
	MemSet(&early_connection_protocol, 0, sizeof(early_connection_protocol));
}

static void
PgConnectionAdoptEarlyInterruptState(PgConnection *connection)
{
	Assert(connection != NULL);

	connection->interrupts = early_connection_interrupts;
	MemSet(&early_connection_interrupts, 0, sizeof(early_connection_interrupts));
}

static void
PgConnectionAdoptEarlyStartupState(PgConnection *connection)
{
	Assert(connection != NULL);

	connection->startup = early_connection_startup;
	MemSet(&early_connection_startup, 0, sizeof(early_connection_startup));
}

static void
PgConnectionAdoptEarlyClientConnectionInfo(PgConnection *connection)
{
	Assert(connection != NULL);

	connection->client_connection_info = early_client_connection_info;
	MemSet(&early_client_connection_info, 0, sizeof(early_client_connection_info));
}

static void
PgSessionAdoptEarlyDatabaseState(PgSession *session)
{
	Assert(session != NULL);

	session->database = early_session_database;
	MemSet(&early_session_database, 0, sizeof(early_session_database));
}

static void
PgSessionInitializeDateTimeState(PgSessionDateTimeState *datetime)
{
	Assert(datetime != NULL);

	datetime->initialized = true;
	datetime->date_style = USE_ISO_DATES;
	datetime->date_order = DATEORDER_MDY;
	datetime->interval_style = INTSTYLE_POSTGRES;
}

static void
PgSessionAdoptEarlyDateTimeState(PgSession *session)
{
	Assert(session != NULL);

	if (!early_session_datetime.initialized)
		PgSessionInitializeDateTimeState(&early_session_datetime);

	session->datetime = early_session_datetime;
	PgSessionInitializeDateTimeState(&early_session_datetime);
}

static void
PgSessionInitializeQueryMemoryState(PgSessionQueryMemoryState *query_memory)
{
	Assert(query_memory != NULL);

	query_memory->initialized = true;
	query_memory->work_mem_kb = 4096;
	query_memory->hash_mem_multiplier_value = 2.0;
	query_memory->maintenance_work_mem_kb = 65536;
	query_memory->max_parallel_maintenance_workers_value = 2;
}

static void
PgSessionAdoptEarlyQueryMemoryState(PgSession *session)
{
	Assert(session != NULL);

	if (!early_session_query_memory.initialized)
		PgSessionInitializeQueryMemoryState(&early_session_query_memory);

	session->query_memory = early_session_query_memory;
	PgSessionInitializeQueryMemoryState(&early_session_query_memory);
}

static void
PgBackendResetCoreState(PgBackendCoreState *core)
{
	MemSet(core, 0, sizeof(*core));
	core->mode = InitProcessing;
}

static void
PgBackendAdoptEarlyCoreState(PgBackend *backend)
{
	struct Latch *existing_interrupt_latch;

	Assert(backend != NULL);

	existing_interrupt_latch = backend->interrupt_latch;
	backend->core = early_backend_core;
	PgBackendResetCoreState(&early_backend_core);

	if (backend->core.latch == NULL)
		backend->core.latch = existing_interrupt_latch;
	else
		PgBackendSetInterruptLatch(backend, backend->core.latch);

	if (early_backend_type != B_INVALID)
	{
		backend->backend_type = early_backend_type;
		early_backend_type = B_INVALID;
	}
}

static void
PgBackendAdoptEarlyPendingInterrupts(PgBackend *backend)
{
	Assert(backend != NULL);

	backend->pending_interrupts = early_pending_interrupts;
	MemSet(&early_pending_interrupts, 0, sizeof(early_pending_interrupts));
}

static void
PgBackendAdoptEarlyInterruptHoldoffs(PgBackend *backend)
{
	Assert(backend != NULL);

	backend->interrupt_holdoffs.interrupt_holdoff_count =
		early_interrupt_holdoffs.interrupt_holdoff_count;
	backend->interrupt_holdoffs.query_cancel_holdoff_count =
		early_interrupt_holdoffs.query_cancel_holdoff_count;
	backend->interrupt_holdoffs.crit_section_count =
		early_interrupt_holdoffs.crit_section_count;

	early_interrupt_holdoffs.interrupt_holdoff_count = 0;
	early_interrupt_holdoffs.query_cancel_holdoff_count = 0;
	early_interrupt_holdoffs.crit_section_count = 0;
}

static void
PgExecutionAdoptEarlyDebugState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->debug.debug_query_string =
		early_execution_debug.debug_query_string;

	early_execution_debug.debug_query_string = NULL;
}

static void
PgExecutionAdoptEarlyErrorState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->error = early_execution_error;
	MemSet(&early_execution_error, 0, sizeof(early_execution_error));
}

static void
PgExecutionAdoptEarlyMemoryContexts(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->memory_contexts = early_execution_memory_contexts;
	MemSet(&early_execution_memory_contexts, 0,
		   sizeof(early_execution_memory_contexts));
}

static void
PgExecutionAdoptEarlyResourceOwners(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->resource_owners = early_execution_resource_owners;
	MemSet(&early_execution_resource_owners, 0,
		   sizeof(early_execution_resource_owners));
}

void
InitializePgProcessRuntime(void)
{
	MemSet(&process_runtime, 0, sizeof(process_runtime));
	MemSet(&process_carrier, 0, sizeof(process_carrier));
	MemSet(&process_backend, 0, sizeof(process_backend));
	MemSet(&process_session, 0, sizeof(process_session));
	MemSet(&process_connection, 0, sizeof(process_connection));
	MemSet(&process_execution, 0, sizeof(process_execution));

	process_runtime.kind = PG_RUNTIME_PROCESS;
	process_runtime.current_carrier = &process_carrier;
	process_runtime.extension_backend_model = PG_BACKEND_MODEL_PROCESS;

	process_carrier.kind = PG_CARRIER_PROCESS;
	process_carrier.runtime = &process_runtime;
	process_carrier.current_backend = &process_backend;
	process_carrier.current_session = &process_session;
	process_carrier.current_execution = &process_execution;

	process_backend.runtime = &process_runtime;
	process_backend.id = PgBackendAssignId();
	process_backend.carrier = &process_carrier;
	process_backend.session = &process_session;
	process_backend.connection = &process_connection;
	process_backend.execution = &process_execution;
	process_backend.backend_type = MyBackendType;
	PgBackendInitializeInterrupts(&process_backend);
	PgBackendAdoptEarlyCoreState(&process_backend);
	PgBackendSetInterruptLatch(&process_backend, process_backend.core.latch);
	dlist_init(&process_backend.dsm_segment_list);
	pg_atomic_init_u32(&process_backend.wait_state.waiting, 0);
	PgBackendInitializeExitState(&process_backend.exit_state);
	PgBackendAdoptEarlyPendingInterrupts(&process_backend);
	PgBackendAdoptEarlyInterruptHoldoffs(&process_backend);
	PgBackendAdoptEarlyExitState(&process_backend.exit_state);

	process_session.backend = &process_backend;
	process_session.connection = &process_connection;
	process_session.execution = &process_execution;
	PgSessionAdoptEarlyDatabaseState(&process_session);
	PgSessionAdoptEarlyDateTimeState(&process_session);
	PgSessionAdoptEarlyQueryMemoryState(&process_session);

	process_connection.backend = &process_backend;
	process_connection.session = &process_session;
	PgConnectionAdoptEarlyIdentity(&process_connection);
	PgConnectionAdoptEarlySocketIO(&process_connection);
	PgConnectionAdoptEarlyProtocolState(&process_connection);
	PgConnectionAdoptEarlyInterruptState(&process_connection);
	PgConnectionAdoptEarlyStartupState(&process_connection);
	PgConnectionAdoptEarlyClientConnectionInfo(&process_connection);

	process_execution.backend = &process_backend;
	process_execution.session = &process_session;
	process_execution.carrier = &process_carrier;
	PgExecutionAdoptEarlyDebugState(&process_execution);
	PgExecutionAdoptEarlyErrorState(&process_execution);
	PgExecutionAdoptEarlyMemoryContexts(&process_execution);
	PgExecutionAdoptEarlyResourceOwners(&process_execution);

	CurrentPgRuntime = &process_runtime;
	CurrentPgCarrier = &process_carrier;
	CurrentPgBackend = &process_backend;
	PgSetCurrentSession(&process_session);
	CurrentPgConnection = &process_connection;
	CurrentPgExecution = &process_execution;

	if (MyProc != NULL && MyProc->backendId == 0)
		MyProc->backendId = process_backend.id;
}

void
InitializePgThreadRuntime(PgBackendExitContinuation exit_backend)
{
	if (!thread_runtime_initialized)
	{
		MemSet(&thread_runtime, 0, sizeof(thread_runtime));

		thread_runtime.kind = PG_RUNTIME_THREAD_PER_SESSION;
		thread_runtime.extension_backend_model =
			PG_BACKEND_MODEL_THREAD_PER_SESSION;
		PgBackendInitializeIdCounter();
		thread_runtime_initialized = true;
	}

	thread_runtime.exit_backend = exit_backend;
}

void
InitializePgThreadBackendRuntimeState(PgThreadBackendRuntimeState *state,
									  BackendType backend_type,
									  struct Port *port,
									  struct Latch *interrupt_latch)
{
	Assert(state != NULL);
	Assert(thread_runtime_initialized);

	MemSet(state, 0, sizeof(*state));

	state->carrier.kind = PG_CARRIER_THREAD;
	state->carrier.runtime = &thread_runtime;
	state->carrier.current_backend = &state->backend;
	state->carrier.current_session = &state->session;
	state->carrier.current_execution = &state->execution;

	state->backend.runtime = &thread_runtime;
	state->backend.id = PgBackendAssignId();
	state->backend.carrier = &state->carrier;
	state->backend.session = &state->session;
	state->backend.connection = &state->connection;
	state->backend.execution = &state->execution;
	state->backend.backend_type = backend_type;
	PgBackendInitializeInterrupts(&state->backend);
	PgBackendSetInterruptLatch(&state->backend, interrupt_latch);
	dlist_init(&state->backend.dsm_segment_list);
	pg_atomic_init_u32(&state->backend.wait_state.waiting, 0);
	PgBackendInitializeExitState(&state->backend.exit_state);

	state->session.backend = &state->backend;
	state->session.connection = &state->connection;
	state->session.execution = &state->execution;
	PgSessionInitializeDateTimeState(&state->session.datetime);
	PgSessionInitializeQueryMemoryState(&state->session.query_memory);

	state->connection.backend = &state->backend;
	state->connection.session = &state->session;
	state->connection.identity.port = port;

	state->execution.backend = &state->backend;
	state->execution.session = &state->session;
	state->execution.carrier = &state->carrier;
}

void
InstallPgThreadBackendRuntimeState(PgThreadBackendRuntimeState *state)
{
	Assert(state != NULL);

	state->carrier.current_backend = &state->backend;
	state->carrier.current_session = &state->session;
	state->carrier.current_execution = &state->execution;
	PgBackendAdoptEarlyCoreState(&state->backend);
	PgSessionAdoptEarlyDatabaseState(&state->session);
	PgSessionAdoptEarlyDateTimeState(&state->session);
	PgSessionAdoptEarlyQueryMemoryState(&state->session);
	PgExecutionAdoptEarlyErrorState(&state->execution);
	PgExecutionAdoptEarlyMemoryContexts(&state->execution);
	PgExecutionAdoptEarlyResourceOwners(&state->execution);
	CurrentPgRuntime = &thread_runtime;
	CurrentPgCarrier = &state->carrier;
	CurrentPgBackend = &state->backend;
	PgSetCurrentSession(&state->session);
	CurrentPgConnection = &state->connection;
	CurrentPgExecution = &state->execution;

	proc_exit_inprogress = false;
	shmem_exit_inprogress = false;
}

void
InitializePgThreadBackendRuntime(PgThreadBackendRuntimeState *state,
								 BackendType backend_type,
								 struct Port *port,
								 struct Latch *interrupt_latch)
{
	InitializePgThreadBackendRuntimeState(state, backend_type, port,
										  interrupt_latch);
	InstallPgThreadBackendRuntimeState(state);
}

void
PgSetCurrentSession(PgSession *session)
{
	CurrentPgSession = session;
	RebindSessionGUCVariablePointers();
}

Session *
PgSessionGetLegacySession(PgSession *session)
{
	if (session == NULL)
		return NULL;

	return session->legacy_session;
}

void
PgSessionSetLegacySession(PgSession *session, Session *legacy_session)
{
	if (session == NULL)
		return;

	session->legacy_session = legacy_session;
}

Session *
PgCurrentLegacySession(void)
{
	return PgSessionGetLegacySession(CurrentPgSession);
}

static PgSessionDatabaseState *
PgCurrentSessionDatabaseState(void)
{
	if (CurrentPgSession == NULL)
		return &early_session_database;

	return &CurrentPgSession->database;
}

static PgSessionDateTimeState *
PgCurrentSessionDateTimeState(void)
{
	PgSessionDateTimeState *datetime;

	if (CurrentPgSession == NULL)
		datetime = &early_session_datetime;
	else
		datetime = &CurrentPgSession->datetime;

	if (!datetime->initialized)
		PgSessionInitializeDateTimeState(datetime);

	return datetime;
}

static PgSessionQueryMemoryState *
PgCurrentSessionQueryMemoryState(void)
{
	PgSessionQueryMemoryState *query_memory;

	if (CurrentPgSession == NULL)
		query_memory = &early_session_query_memory;
	else
		query_memory = &CurrentPgSession->query_memory;

	if (!query_memory->initialized)
		PgSessionInitializeQueryMemoryState(query_memory);

	return query_memory;
}

Oid *
PgCurrentMyDatabaseIdRef(void)
{
	return &PgCurrentSessionDatabaseState()->database_id;
}

Oid *
PgCurrentMyDatabaseTableSpaceRef(void)
{
	return &PgCurrentSessionDatabaseState()->database_tablespace;
}

bool *
PgCurrentMyDatabaseHasLoginEventTriggersRef(void)
{
	return &PgCurrentSessionDatabaseState()->database_has_login_event_triggers;
}

char **
PgCurrentDatabasePathRef(void)
{
	return &PgCurrentSessionDatabaseState()->database_path;
}

int *
PgCurrentDateStyleRef(void)
{
	return &PgCurrentSessionDateTimeState()->date_style;
}

int *
PgCurrentDateOrderRef(void)
{
	return &PgCurrentSessionDateTimeState()->date_order;
}

int *
PgCurrentIntervalStyleRef(void)
{
	return &PgCurrentSessionDateTimeState()->interval_style;
}

int *
PgCurrentWorkMemRef(void)
{
	return &PgCurrentSessionQueryMemoryState()->work_mem_kb;
}

double *
PgCurrentHashMemMultiplierRef(void)
{
	return &PgCurrentSessionQueryMemoryState()->hash_mem_multiplier_value;
}

int *
PgCurrentMaintenanceWorkMemRef(void)
{
	return &PgCurrentSessionQueryMemoryState()->maintenance_work_mem_kb;
}

int *
PgCurrentMaxParallelMaintenanceWorkersRef(void)
{
	return &PgCurrentSessionQueryMemoryState()->max_parallel_maintenance_workers_value;
}

struct Port **
PgConnectionProcPortRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_identity.port;

	return &connection->identity.port;
}

struct Port **
PgCurrentProcPortRef(void)
{
	return PgConnectionProcPortRef(CurrentPgConnection);
}

uint8 *
PgConnectionCancelKey(PgConnection *connection)
{
	if (connection == NULL)
		return early_connection_identity.cancel_key;

	return connection->identity.cancel_key;
}

uint8 *
PgCurrentCancelKey(void)
{
	return PgConnectionCancelKey(CurrentPgConnection);
}

int *
PgConnectionCancelKeyLengthRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_identity.cancel_key_length;

	return &connection->identity.cancel_key_length;
}

int *
PgCurrentCancelKeyLengthRef(void)
{
	return PgConnectionCancelKeyLengthRef(CurrentPgConnection);
}

const char **
PgExecutionDebugQueryStringRef(PgExecution *execution)
{
	if (execution == NULL)
		return &early_execution_debug.debug_query_string;

	return &execution->debug.debug_query_string;
}

const char **
PgCurrentDebugQueryStringRef(void)
{
	return PgExecutionDebugQueryStringRef(CurrentPgExecution);
}

static PgExecutionErrorState *
PgCurrentExecutionErrorState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_error;

	return &CurrentPgExecution->error;
}

ErrorContextCallback **
PgCurrentErrorContextStackRef(void)
{
	return &PgCurrentExecutionErrorState()->context_stack;
}

sigjmp_buf **
PgCurrentExceptionStackRef(void)
{
	return &PgCurrentExecutionErrorState()->exception_stack;
}

static PgExecutionMemoryContextState *
PgCurrentExecutionMemoryContexts(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_memory_contexts;

	return &CurrentPgExecution->memory_contexts;
}

MemoryContext *
PgCurrentMemoryContextRef(void)
{
	return &PgCurrentExecutionMemoryContexts()->current_context;
}

MemoryContext *
PgErrorContextRef(void)
{
	return &PgCurrentExecutionMemoryContexts()->error_context;
}

MemoryContext *
PgMessageContextRef(void)
{
	return &PgCurrentExecutionMemoryContexts()->message_context;
}

MemoryContext *
PgTopTransactionContextRef(void)
{
	return &PgCurrentExecutionMemoryContexts()->top_transaction_context;
}

MemoryContext *
PgCurTransactionContextRef(void)
{
	return &PgCurrentExecutionMemoryContexts()->cur_transaction_context;
}

MemoryContext *
PgPortalContextRef(void)
{
	return &PgCurrentExecutionMemoryContexts()->portal_context;
}

static PgExecutionResourceOwnerState *
PgCurrentExecutionResourceOwners(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_resource_owners;

	return &CurrentPgExecution->resource_owners;
}

ResourceOwner *
PgCurrentResourceOwnerRef(void)
{
	return &PgCurrentExecutionResourceOwners()->current_owner;
}

ResourceOwner *
PgCurTransactionResourceOwnerRef(void)
{
	return &PgCurrentExecutionResourceOwners()->cur_transaction_owner;
}

ResourceOwner *
PgTopTransactionResourceOwnerRef(void)
{
	return &PgCurrentExecutionResourceOwners()->top_transaction_owner;
}

PgConnectionSocketIOState *
PgConnectionSocketIORef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_socket_io;

	return &connection->socket_io;
}

PgConnectionSocketIOState *
PgCurrentConnectionSocketIORef(void)
{
	return PgConnectionSocketIORef(CurrentPgConnection);
}

const PQcommMethods **
PgConnectionPqCommMethodsRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_protocol.comm_methods;

	return &connection->protocol.comm_methods;
}

const PQcommMethods **
PgCurrentPqCommMethodsRef(void)
{
	return PgConnectionPqCommMethodsRef(CurrentPgConnection);
}

WaitEventSet **
PgConnectionFeBeWaitSetRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_protocol.fe_be_wait_set;

	return &connection->protocol.fe_be_wait_set;
}

WaitEventSet **
PgCurrentFeBeWaitSetRef(void)
{
	return PgConnectionFeBeWaitSetRef(CurrentPgConnection);
}

uint32 *
PgConnectionFrontendProtocolRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_protocol.frontend_protocol;

	return &connection->protocol.frontend_protocol;
}

uint32 *
PgCurrentFrontendProtocolRef(void)
{
	return PgConnectionFrontendProtocolRef(CurrentPgConnection);
}

volatile sig_atomic_t *
PgConnectionCheckClientConnectionPendingRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_interrupts.check_client_connection_pending;

	return &connection->interrupts.check_client_connection_pending;
}

volatile sig_atomic_t *
PgCurrentCheckClientConnectionPendingRef(void)
{
	return PgConnectionCheckClientConnectionPendingRef(CurrentPgConnection);
}

volatile sig_atomic_t *
PgConnectionClientConnectionLostRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_interrupts.client_connection_lost;

	return &connection->interrupts.client_connection_lost;
}

volatile sig_atomic_t *
PgCurrentClientConnectionLostRef(void)
{
	return PgConnectionClientConnectionLostRef(CurrentPgConnection);
}

bool *
PgConnectionClientAuthInProgressRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_startup.client_auth_in_progress;

	return &connection->startup.client_auth_in_progress;
}

bool *
PgCurrentClientAuthInProgressRef(void)
{
	return PgConnectionClientAuthInProgressRef(CurrentPgConnection);
}

struct ClientSocket **
PgConnectionClientSocketRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_startup.client_socket;

	return &connection->startup.client_socket;
}

struct ClientSocket **
PgCurrentClientSocketRef(void)
{
	return PgConnectionClientSocketRef(CurrentPgConnection);
}

void *
PgConnectionClientConnectionInfoRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_client_connection_info;

	return &connection->client_connection_info;
}

void *
PgCurrentClientConnectionInfoRef(void)
{
	return PgConnectionClientConnectionInfoRef(CurrentPgConnection);
}

static PgBackendCoreState *
PgCurrentCoreState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_core;

	return &CurrentPgBackend->core;
}

bool *
PgCurrentExitOnAnyErrorRef(void)
{
	return &PgCurrentCoreState()->exit_on_any_error;
}

int *
PgCurrentMyProcPidRef(void)
{
	return &PgCurrentCoreState()->proc_pid;
}

pg_time_t *
PgCurrentMyStartTimeRef(void)
{
	return &PgCurrentCoreState()->start_time;
}

TimestampTz *
PgCurrentMyStartTimestampRef(void)
{
	return &PgCurrentCoreState()->start_timestamp;
}

struct Latch **
PgCurrentMyLatchRef(void)
{
	return &PgCurrentCoreState()->latch;
}

int *
PgCurrentMyPMChildSlotRef(void)
{
	return &PgCurrentCoreState()->pm_child_slot;
}

char *
PgCurrentOutputFileNameRef(void)
{
	return PgCurrentCoreState()->output_file_name;
}

static BackendType *
PgCurrentBackendTypeRef(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_type;

	return &CurrentPgBackend->backend_type;
}

BackendType *
PgCurrentMyBackendTypeRef(void)
{
	return PgCurrentBackendTypeRef();
}

ProcessingMode *
PgCurrentProcessingModeRef(void)
{
	return &PgCurrentCoreState()->mode;
}

bool *
PgCurrentIgnoreSystemIndexesRef(void)
{
	return &PgCurrentCoreState()->ignore_system_indexes;
}

static PgBackendPendingInterruptState *
PgCurrentPendingInterrupts(void)
{
	if (CurrentPgBackend == NULL)
		return &early_pending_interrupts;

	return &CurrentPgBackend->pending_interrupts;
}

PgBackendPendingInterruptState *
PgCurrentPendingInterruptStateRef(void)
{
	return PgCurrentPendingInterrupts();
}

static PgBackendInterruptHoldoffState *
PgCurrentInterruptHoldoffs(void)
{
	if (CurrentPgBackend == NULL)
		return &early_interrupt_holdoffs;

	return &CurrentPgBackend->interrupt_holdoffs;
}

volatile uint32 *
PgCurrentInterruptHoldoffCountRef(void)
{
	return &PgCurrentInterruptHoldoffs()->interrupt_holdoff_count;
}

volatile uint32 *
PgCurrentQueryCancelHoldoffCountRef(void)
{
	return &PgCurrentInterruptHoldoffs()->query_cancel_holdoff_count;
}

volatile uint32 *
PgCurrentCritSectionCountRef(void)
{
	return &PgCurrentInterruptHoldoffs()->crit_section_count;
}

PgBackendLaunchModel
PgRuntimeGetBackendLaunchModel(BackendType backend_type)
{
	if (PgRuntimeShouldThreadBackend(backend_type))
		return PG_BACKEND_LAUNCH_THREAD;

	return PG_BACKEND_LAUNCH_PROCESS;
}

bool
PgRuntimeShouldThreadBackend(BackendType backend_type)
{
	if (!multithreaded)
		return false;

	/*
	 * Phase 10 is scoped to regular client backends.  Phase 11 incrementally
	 * moves in-tree server-owned worker families onto the same carrier
	 * infrastructure as they get dedicated signal and lifecycle conversion.
	 */
	return backend_type == B_BACKEND ||
		backend_type == B_ARCHIVER ||
		backend_type == B_AUTOVAC_LAUNCHER ||
		backend_type == B_AUTOVAC_WORKER ||
		backend_type == B_BG_WRITER ||
		backend_type == B_CHECKPOINTER ||
		backend_type == B_LOGGER ||
		backend_type == B_STARTUP ||
		backend_type == B_WAL_RECEIVER ||
		backend_type == B_SLOTSYNC_WORKER ||
		backend_type == B_WAL_WRITER ||
		backend_type == B_WAL_SUMMARIZER;
}

PgBackendModel
PgRuntimeGetExtensionBackendModel(void)
{
	if (CurrentPgRuntime == NULL)
		return PG_BACKEND_MODEL_PROCESS;

	return CurrentPgRuntime->extension_backend_model;
}

void
PgRuntimeSetExtensionBackendModel(PgBackendModel backend_model)
{
	if (backend_model < PG_BACKEND_MODEL_PROCESS ||
		backend_model > PG_BACKEND_MODEL_POOLED_SCHEDULER)
		elog(ERROR, "invalid backend model: %d", backend_model);

	if (CurrentPgRuntime == NULL)
		return;

	check_loaded_modules_backend_model(backend_model);
	CurrentPgRuntime->extension_backend_model = backend_model;
}

void
PgBackendInitializeInterrupts(PgBackend *backend)
{
	if (backend == NULL)
		return;

	pg_atomic_init_u32(&backend->interrupts.pending_mask, 0);
	backend->interrupts.proc_die_sender_pid = 0;
	backend->interrupts.proc_die_sender_uid = 0;
}

void
PgBackendSetInterruptLatch(PgBackend *backend, struct Latch *interrupt_latch)
{
	if (backend == NULL)
		return;

	backend->interrupt_latch = interrupt_latch;
}

PgBackendId
PgBackendGetId(PgBackend *backend)
{
	if (backend == NULL)
		return 0;

	return backend->id;
}

PgBackendId
PgCurrentBackendId(void)
{
	return PgBackendGetId(CurrentPgBackend);
}

int
PgBackendGetSignalPid(PgBackend *backend)
{
	if (backend == NULL)
		return MyProcPid;

	if (backend->runtime != NULL &&
		backend->runtime->kind == PG_RUNTIME_THREAD_PER_SESSION)
	{
		if (backend->id > PG_INT32_MAX)
			elog(ERROR, "threaded backend identifier exceeds protocol range");

		return (int) backend->id;
	}

	return MyProcPid;
}

int
PgCurrentBackendSignalPid(void)
{
	return PgBackendGetSignalPid(CurrentPgBackend);
}

bool
PgBackendUsesProcessSignals(PgBackend *backend)
{
	if (backend == NULL || backend->runtime == NULL)
		return true;

	return backend->runtime->kind == PG_RUNTIME_PROCESS;
}

void
PgBackendWakeup(PgBackend *backend)
{
	if (backend == NULL)
		return;

	PgBackendWakeForInterrupt(backend);
}

void
PgBackendRaiseInterrupt(PgBackend *backend,
						PgBackendInterruptType interrupt_type)
{
	PgBackendInterruptMask interrupt_mask;

	if (backend == NULL)
		return;
	if (interrupt_type < 0 || interrupt_type >= PG_BACKEND_INTERRUPT_COUNT)
		return;

	interrupt_mask = PG_BACKEND_INTERRUPT_MASK(interrupt_type);
	pg_atomic_fetch_or_u32(&backend->interrupts.pending_mask, interrupt_mask);
	PgBackendWakeForInterrupt(backend);
}

static void
PgBackendWakeForInterrupt(PgBackend *backend)
{
	/*
	 * Process mode has one logical backend per address space, so waking the
	 * current backend must still arm the historical fast-path flag used by
	 * signal-era code. Non-current logical backends rely on the mailbox test in
	 * CHECK_FOR_INTERRUPTS() after their carrier wakes.
	 */
	if (backend == CurrentPgBackend)
		InterruptPending = true;

	if (backend->interrupt_latch != NULL)
		SetLatch(backend->interrupt_latch);
	else if (backend == CurrentPgBackend && MyLatch != NULL)
		SetLatch(MyLatch);
}

void
PgCurrentBackendRaiseInterrupt(PgBackendInterruptType interrupt_type)
{
	PgBackendRaiseInterrupt(CurrentPgBackend, interrupt_type);
}

void
PgBackendRaiseProcDieInterrupt(PgBackend *backend, int sender_pid,
							   int sender_uid)
{
	if (backend == NULL)
		return;

	if (backend->interrupts.proc_die_sender_pid == 0)
	{
		backend->interrupts.proc_die_sender_pid = sender_pid;
		backend->interrupts.proc_die_sender_uid = sender_uid;
	}

	PgBackendRaiseInterrupt(backend, PG_BACKEND_INTERRUPT_PROC_DIE);
}

void
PgCurrentBackendRaiseProcDieInterrupt(int sender_pid, int sender_uid)
{
	PgBackendRaiseProcDieInterrupt(CurrentPgBackend, sender_pid, sender_uid);
}

PgBackendInterruptMask
PgBackendConsumeInterrupts(PgBackend *backend)
{
	if (backend == NULL)
		return 0;

	return pg_atomic_exchange_u32(&backend->interrupts.pending_mask, 0);
}

bool
PgCurrentBackendHasPendingInterrupts(void)
{
	PgBackend  *backend = CurrentPgBackend;

	if (backend == NULL)
		return ProcSignalBackendInterruptsPending();

	return pg_atomic_read_u32(&backend->interrupts.pending_mask) != 0 ||
		ProcSignalBackendInterruptsPending();
}

void
PgBackendConsumeProcDieSender(PgBackend *backend, int *sender_pid,
							  int *sender_uid)
{
	if (sender_pid != NULL)
		*sender_pid = 0;
	if (sender_uid != NULL)
		*sender_uid = 0;

	if (backend == NULL)
		return;

	if (sender_pid != NULL)
		*sender_pid = backend->interrupts.proc_die_sender_pid;
	if (sender_uid != NULL)
		*sender_uid = backend->interrupts.proc_die_sender_uid;

	backend->interrupts.proc_die_sender_pid = 0;
	backend->interrupts.proc_die_sender_uid = 0;
}

void
PgCurrentBackendApplyInterrupts(void)
{
	PgBackendInterruptMask pending;
	int			proc_signal_sender_pid = 0;
	int			proc_signal_sender_uid = 0;

	pending = PgBackendConsumeInterrupts(CurrentPgBackend);
	pending |= ConsumeBackendInterruptsFromProcSignal(&proc_signal_sender_pid,
													  &proc_signal_sender_uid);
	if (pending == 0)
		return;

	/*
	 * The logical mailbox feeds the legacy per-backend pending flags below.
	 * Arm the legacy dispatcher as well, so callers that consume the mailbox
	 * immediately before CHECK_FOR_INTERRUPTS() still run ProcessInterrupts().
	 */
	InterruptPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_QUERY_CANCEL))
		QueryCancelPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_PROC_DIE))
	{
		int			sender_pid;
		int			sender_uid;

		ProcDiePending = true;
		PgBackendConsumeProcDieSender(CurrentPgBackend, &sender_pid,
									  &sender_uid);
		if (sender_pid == 0 && proc_signal_sender_pid != 0)
		{
			sender_pid = proc_signal_sender_pid;
			sender_uid = proc_signal_sender_uid;
		}
		if (ProcDieSenderPid == 0)
		{
			ProcDieSenderPid = sender_pid;
			ProcDieSenderUid = sender_uid;
		}
	}

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_CLIENT_CONNECTION_CHECK))
		CheckClientConnectionPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_IDLE_IN_TRANSACTION_SESSION_TIMEOUT))
		IdleInTransactionSessionTimeoutPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_TRANSACTION_TIMEOUT))
		TransactionTimeoutPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_IDLE_SESSION_TIMEOUT))
		IdleSessionTimeoutPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_IDLE_STATS_UPDATE_TIMEOUT))
		IdleStatsUpdateTimeoutPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_PROC_SIGNAL_BARRIER))
		ProcSignalBarrierPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_LOG_MEMORY_CONTEXT))
		LogMemoryContextPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_CONFIG_RELOAD))
		ConfigReloadPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_SHUTDOWN_REQUEST))
		ShutdownRequestPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_CATCHUP))
		catchupInterruptPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_NOTIFY))
		notifyInterruptPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_PARALLEL_MESSAGE))
		ParallelMessagePending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_PARALLEL_APPLY_MESSAGE))
		ParallelApplyMessagePending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_SLOT_SYNC_MESSAGE))
		SlotSyncShutdownPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_REPACK_MESSAGE))
		RepackMessagePending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_WAKEUP_STOP))
		WakeupStopPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_AUTOVAC_LAUNCHER))
		AutoVacLauncherPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_CHECKPOINTER_SHUTDOWN_XLOG))
		CheckpointerShutdownXLOGPending = true;
}

int
PgSuspend(const PgWaitSpec *wait_spec, PgSuspendCallback callback,
		  void *callback_arg)
{
	PgBackend  *backend = CurrentPgBackend;
	int			result = 0;

	Assert(callback != NULL);

	if (backend != NULL && wait_spec != NULL)
	{
		backend->wait_state.spec = *wait_spec;
		pg_atomic_write_membarrier_u32(&backend->wait_state.waiting, 1);
	}

	PG_TRY();
	{
		result = callback(callback_arg);
	}
	PG_CATCH();
	{
		if (backend != NULL)
		{
			pg_atomic_write_u32(&backend->wait_state.waiting, 0);
			backend->wait_state.spec.kind = PG_WAIT_KIND_NONE;
		}
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (backend != NULL)
	{
		pg_atomic_write_u32(&backend->wait_state.waiting, 0);
		backend->wait_state.spec.kind = PG_WAIT_KIND_NONE;
	}

	return result;
}
