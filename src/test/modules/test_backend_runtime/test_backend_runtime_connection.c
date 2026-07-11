/*--------------------------------------------------------------------------
 *
 * test_backend_runtime_connection.c
 *		Connection-owned backend runtime state tests.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_backend_runtime/test_backend_runtime_connection.c
 *
 * -------------------------------------------------------------------------
 */
#include "test_backend_runtime.h"

#ifndef WIN32
#include <sys/socket.h>
#endif

/*
 * libpq.h macro-izes ssl_loaded_verify_locations into the current-connection
 * accessor under USE_SSL.  This test manipulates the raw struct field on an
 * explicit PgConnectionSecurityState *, so drop the macro here.
 */
#ifdef ssl_loaded_verify_locations
#undef ssl_loaded_verify_locations
#endif

#ifndef WIN32
static void
test_close_socket(pgsocket *sock)
{
	if (*sock != PGINVALID_SOCKET)
	{
		closesocket(*sock);
		*sock = PGINVALID_SOCKET;
	}
}

static void
test_make_socket_pair(pgsocket socks[2])
{
	socks[0] = PGINVALID_SOCKET;
	socks[1] = PGINVALID_SOCKET;

	if (socketpair(AF_UNIX, SOCK_STREAM, 0, socks) != 0)
		elog(ERROR, "could not create socket pair: %m");

	if (!pg_set_noblock(socks[0]) || !pg_set_noblock(socks[1]))
		elog(ERROR, "could not set socket pair nonblocking: %m");
}
#endif

PG_FUNCTION_INFO_V1(test_connection_socket_io_is_connection_local);
Datum
test_connection_socket_io_is_connection_local(PG_FUNCTION_ARGS)
{
	PgConnection *saved_connection;
	PgConnection fake_connection1;
	PgConnection fake_connection2;
	PgConnectionSocketIOState *socket_io;
	bool		ok = true;

	saved_connection = CurrentPgConnection;
	MemSet(&fake_connection1, 0, sizeof(fake_connection1));
	MemSet(&fake_connection2, 0, sizeof(fake_connection2));

	PG_TRY();
	{
		PgSetCurrentConnection(&fake_connection1);
		socket_io = PgCurrentConnectionSocketIORef();
		socket_io->send_buffer = (char *) "fake connection one";
		socket_io->send_buffer_size = 11;
		socket_io->send_pointer = 7;
		socket_io->send_start = 3;
		socket_io->recv_pointer = 5;
		socket_io->recv_length = 9;
		socket_io->comm_busy = true;
		socket_io->comm_reading_msg = true;
		socket_io->win32_noblock = 1;

		PgSetCurrentConnection(&fake_connection2);
		socket_io = PgCurrentConnectionSocketIORef();
		ok = ok && socket_io->send_buffer == NULL;
		ok = ok && socket_io->send_buffer_size == 0;
		ok = ok && socket_io->send_pointer == 0;
		ok = ok && socket_io->send_start == 0;
		ok = ok && socket_io->recv_pointer == 0;
		ok = ok && socket_io->recv_length == 0;
		ok = ok && !socket_io->comm_busy;
		ok = ok && !socket_io->comm_reading_msg;
		ok = ok && socket_io->win32_noblock == 0;
		socket_io->send_buffer = (char *) "fake connection two";
		socket_io->comm_busy = true;
		socket_io->win32_noblock = 2;

		PgSetCurrentConnection(&fake_connection1);
		socket_io = PgCurrentConnectionSocketIORef();
		ok = ok && strcmp(socket_io->send_buffer, "fake connection one") == 0;
		ok = ok && socket_io->send_buffer_size == 11;
		ok = ok && socket_io->send_pointer == 7;
		ok = ok && socket_io->send_start == 3;
		ok = ok && socket_io->recv_pointer == 5;
		ok = ok && socket_io->recv_length == 9;
		ok = ok && socket_io->comm_busy;
		ok = ok && socket_io->comm_reading_msg;
		ok = ok && socket_io->win32_noblock == 1;

		PgSetCurrentConnection(&fake_connection2);
		socket_io = PgCurrentConnectionSocketIORef();
		ok = ok && strcmp(socket_io->send_buffer, "fake connection two") == 0;
		ok = ok && socket_io->comm_busy;
		ok = ok && !socket_io->comm_reading_msg;
		ok = ok && socket_io->win32_noblock == 2;

		PgSetCurrentConnection(saved_connection);
	}
	PG_CATCH();
	{
		PgSetCurrentConnection(saved_connection);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "connection socket I/O state was not connection-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_connection_protocol_byte_probe);
Datum
test_connection_protocol_byte_probe(PG_FUNCTION_ARGS)
{
#ifndef WIN32
	PgConnection *saved_connection;
	volatile uint32 saved_query_cancel_holdoff_count;
	PgConnection connection;
	PgConnectionSocketIOState *socket_io;
	Port		port;
	pgsocket	socks[2] = {PGINVALID_SOCKET, PGINVALID_SOCKET};
	PgProtocolByteProbe probe;
	PgProtocolByteResult result;
	char		recv_buffer[PG_CONNECTION_RECV_BUFFER_SIZE];
	unsigned char type_byte;
	bool		ok = true;

	saved_connection = CurrentPgConnection;
	saved_query_cancel_holdoff_count = QueryCancelHoldoffCount;
	MemSet(&connection, 0, sizeof(connection));
	MemSet(&port, 0, sizeof(port));

	PG_TRY();
	{
		PgSetCurrentConnection(&connection);
		QueryCancelHoldoffCount = 0;

		socket_io = &connection.socket_io;
		socket_io->recv_buffer = recv_buffer;
		socket_io->recv_buffer[0] = PqMsg_Query;
		socket_io->recv_pointer = 0;
		socket_io->recv_length = 1;
		socket_io->transport_generation = 7;
		connection.identity.port = NULL;
		result = PgConnectionProbeMessageType(&connection, &probe);
		ok = ok && result == PG_PROTOCOL_BYTE_AVAILABLE;
		ok = ok && probe.type == PqMsg_Query;
		ok = ok && probe.transport_wait_events == 0;
		ok = ok && probe.transport_buffered_input;
		ok = ok && probe.transport_generation == 7;
		ok = ok && socket_io->recv_pointer == 1;
		ok = ok && pq_is_reading_msg();
		ok = ok && !PgConnectionCanParkBeforeMessage(&connection);
		pq_endmsgread();
		ok = ok && PgConnectionCanParkBeforeMessage(&connection);

		MemSet(&connection, 0, sizeof(connection));
		MemSet(&port, 0, sizeof(port));
		test_make_socket_pair(socks);
		port.sock = socks[0];
		connection.identity.port = &port;
		socket_io = &connection.socket_io;
		socket_io->recv_buffer = recv_buffer;
		socket_io->recv_pointer = 3;
		socket_io->recv_length = 3;
		socket_io->transport_generation = 42;
		result = PgConnectionProbeMessageType(&connection, &probe);
		ok = ok && result == PG_PROTOCOL_BYTE_NONE;
		ok = ok && (probe.transport_wait_events & WL_SOCKET_READABLE) != 0;
		ok = ok && !probe.transport_buffered_input;
		ok = ok && probe.transport_generation == 42;
		ok = ok && socket_io->recv_pointer == 3;
		ok = ok && socket_io->recv_length == 3;
		ok = ok && !pq_is_reading_msg();
		ok = ok && PgConnectionCanParkBeforeMessage(&connection);
		ok = ok && QueryCancelHoldoffCount == 0;
		test_close_socket(&socks[0]);
		test_close_socket(&socks[1]);

		MemSet(&connection, 0, sizeof(connection));
		MemSet(&port, 0, sizeof(port));
		test_make_socket_pair(socks);
		type_byte = PqMsg_Query;
		if (send(socks[1], &type_byte, 1, 0) != 1)
			elog(ERROR, "could not write protocol byte to socket pair: %m");
		port.sock = socks[0];
		connection.identity.port = &port;
		socket_io = &connection.socket_io;
		socket_io->recv_buffer = recv_buffer;
		socket_io->transport_generation = 90;
		result = PgConnectionProbeMessageType(&connection, &probe);
		ok = ok && result == PG_PROTOCOL_BYTE_AVAILABLE;
		ok = ok && probe.type == PqMsg_Query;
		ok = ok && probe.transport_wait_events == 0;
		ok = ok && !probe.transport_buffered_input;
		ok = ok && probe.transport_generation == 91;
		ok = ok && pq_is_reading_msg();
		ok = ok && !PgConnectionCanParkBeforeMessage(&connection);
		pq_endmsgread();
		ok = ok && PgConnectionCanParkBeforeMessage(&connection);
		test_close_socket(&socks[0]);
		test_close_socket(&socks[1]);

		MemSet(&connection, 0, sizeof(connection));
		MemSet(&port, 0, sizeof(port));
		test_make_socket_pair(socks);
		port.sock = socks[0];
		connection.identity.port = &port;
		test_close_socket(&socks[1]);
		result = PgConnectionProbeMessageType(&connection, &probe);
		ok = ok && result == PG_PROTOCOL_BYTE_EOF;
		ok = ok && !pq_is_reading_msg();
		ok = ok && PgConnectionCanParkBeforeMessage(&connection);
		test_close_socket(&socks[0]);

		MemSet(&connection, 0, sizeof(connection));
		MemSet(&port, 0, sizeof(port));
		test_make_socket_pair(socks);
		port.sock = socks[0];
		connection.identity.port = &port;
		connection.security.gss_send_length = 2;
		connection.security.gss_send_next = 1;
		result = PgConnectionProbeMessageType(&connection, &probe);
		ok = ok && result == PG_PROTOCOL_BYTE_NONE;
		ok = ok && probe.transport_wait_events == WL_SOCKET_WRITEABLE;
		ok = ok && !probe.transport_buffered_input;
		ok = ok && !pq_is_reading_msg();
		test_close_socket(&socks[0]);
		test_close_socket(&socks[1]);

		QueryCancelHoldoffCount = saved_query_cancel_holdoff_count;
		PgSetCurrentConnection(saved_connection);
	}
	PG_CATCH();
	{
		test_close_socket(&socks[0]);
		test_close_socket(&socks[1]);
		QueryCancelHoldoffCount = saved_query_cancel_holdoff_count;
		PgSetCurrentConnection(saved_connection);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "protocol byte probe did not preserve boundary semantics");

	PG_RETURN_BOOL(true);
#else
	PG_RETURN_BOOL(true);
#endif
}
PG_FUNCTION_INFO_V1(test_connection_protocol_state_is_connection_local);
Datum
test_connection_protocol_state_is_connection_local(PG_FUNCTION_ARGS)
{
	PgConnection *saved_connection;
	PgConnection fake_connection1;
	PgConnection fake_connection2;
	const PQcommMethods *saved_comm_methods;
	WaitEventSet *saved_wait_set;
	const PQcommMethods methods1 = {0};
	const PQcommMethods methods2 = {0};
	WaitEventSet *wait_set1;
	WaitEventSet *wait_set2;
	bool		ok = true;

	saved_connection = CurrentPgConnection;
	saved_comm_methods = PqCommMethods;
	saved_wait_set = FeBeWaitSet;
	wait_set1 = (WaitEventSet *) &fake_connection1;
	wait_set2 = (WaitEventSet *) &fake_connection2;
	MemSet(&fake_connection1, 0, sizeof(fake_connection1));
	MemSet(&fake_connection2, 0, sizeof(fake_connection2));

	PG_TRY();
	{
		PgSetCurrentConnection(&fake_connection1);
		PqCommMethods = &methods1;
		FeBeWaitSet = wait_set1;

		PgSetCurrentConnection(&fake_connection2);
		ok = ok && PqCommMethods == NULL;
		ok = ok && FeBeWaitSet == NULL;
		PqCommMethods = &methods2;
		FeBeWaitSet = wait_set2;

		PgSetCurrentConnection(&fake_connection1);
		ok = ok && PqCommMethods == &methods1;
		ok = ok && FeBeWaitSet == wait_set1;

		PgSetCurrentConnection(&fake_connection2);
		ok = ok && PqCommMethods == &methods2;
		ok = ok && FeBeWaitSet == wait_set2;

		PgSetCurrentConnection(saved_connection);
		PqCommMethods = saved_comm_methods;
		FeBeWaitSet = saved_wait_set;
	}
	PG_CATCH();
	{
		PgSetCurrentConnection(saved_connection);
		PqCommMethods = saved_comm_methods;
		FeBeWaitSet = saved_wait_set;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "connection protocol state was not connection-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_connection_reset_closed_state);
Datum
test_connection_reset_closed_state(PG_FUNCTION_ARGS)
{
	PgConnection connection;
	PgConnection *saved_connection;
	PgConnectionSocketIOState *socket_io;
	PgConnectionSecurityState *security;
	const PQcommMethods methods = {0};
	struct ClientSocket fake_client_socket;
	WaitEventSet *fake_wait_set;
	MemoryContext oldcontext;
	MemoryContext port_context;
	MemoryContext warning_context;
	bool		ok = true;

	saved_connection = CurrentPgConnection;
	MemSet(&connection, 0, sizeof(connection));
	MemSet(&fake_client_socket, 0, sizeof(fake_client_socket));
	fake_wait_set = (WaitEventSet *) &connection;

	port_context = AllocSetContextCreate(TopMemoryContext,
										 "test port state",
										 ALLOCSET_SMALL_SIZES);
	connection.identity.port_context = port_context;
	oldcontext = MemoryContextSwitchTo(port_context);
	connection.identity.port = palloc0_object(Port);
	MemoryContextSwitchTo(oldcontext);
	MemSet(connection.identity.cancel_key, 0x7a,
		   sizeof(connection.identity.cancel_key));
	connection.identity.cancel_key_length =
		sizeof(connection.identity.cancel_key);

	socket_io = &connection.socket_io;
	socket_io->socket_io_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test socket I/O state",
							  ALLOCSET_SMALL_SIZES);
	oldcontext = MemoryContextSwitchTo(socket_io->socket_io_context);
	socket_io->send_buffer = pstrdup("released by socket reset");
	socket_io->recv_buffer = palloc0(PG_CONNECTION_RECV_BUFFER_SIZE);
	MemoryContextSwitchTo(oldcontext);
	socket_io->send_buffer_size = 128;
	socket_io->send_pointer = 64;
	socket_io->send_start = 32;
	socket_io->recv_buffer[0] = 'x';
	socket_io->recv_pointer = 7;
	socket_io->recv_length = 9;
	socket_io->comm_busy = true;
	socket_io->comm_reading_msg = true;
	socket_io->win32_noblock = 1;

	connection.protocol.comm_methods = &methods;
	connection.protocol.fe_be_wait_set = fake_wait_set;
	connection.protocol.frontend_protocol = PG_PROTOCOL(3, 2);
	connection.output.where_to_send_output = DestRemote;
	connection.interrupts.check_client_connection_pending = true;
	connection.interrupts.client_connection_lost = true;
	connection.startup.client_auth_in_progress = true;
	connection.startup.client_socket = &fake_client_socket;
	connection.startup.connection_warnings_emitted = true;
	warning_context = AllocSetContextCreate(TopMemoryContext,
											"test connection warning state",
											ALLOCSET_SMALL_SIZES);
	connection.startup.connection_warning_context = warning_context;
	oldcontext = MemoryContextSwitchTo(warning_context);
	connection.startup.connection_warning_messages =
		list_make1(pstrdup("test warning"));
	connection.startup.connection_warning_details =
		list_make1(pstrdup("test detail"));
	MemoryContextSwitchTo(oldcontext);
	connection.client_connection_info_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test client connection info state",
							  ALLOCSET_SMALL_SIZES);
	oldcontext = MemoryContextSwitchTo(connection.client_connection_info_context);
	connection.client_connection_info.authn_id = pstrdup("owned-authn");
	MemoryContextSwitchTo(oldcontext);
	connection.client_connection_info.auth_method = uaSCRAM;
	connection.client_connection_info_authn_id_owned = true;

	security = &connection.security;
	security->ssl_loaded_verify_locations = true;
	security->gss_send_buffer = malloc(8);
	security->gss_send_length = 1;
	security->gss_send_next = 2;
	security->gss_send_consumed = 3;
	security->gss_recv_buffer = malloc(8);
	security->gss_recv_length = 4;
	security->gss_result_buffer = malloc(8);
	security->gss_result_length = 5;
	security->gss_result_next = 6;
	security->gss_max_packet_size = 7;
	security->pam_password = "borrowed";
	security->pam_port = (struct Port *) &connection;
	security->pam_no_password = true;

	if (security->gss_send_buffer == NULL ||
		security->gss_recv_buffer == NULL ||
		security->gss_result_buffer == NULL)
	{
		free(security->gss_send_buffer);
		free(security->gss_recv_buffer);
		free(security->gss_result_buffer);
		elog(ERROR, "out of memory");
	}

	PG_TRY();
	{
		PgSetCurrentConnection(&connection);
		client_connection_check_interval = 99;
		PgSetCurrentConnection(saved_connection);

		PgConnectionResetClosedState(&connection);

		ok = ok && connection.identity.port == NULL;
		ok = ok && connection.identity.port_context == NULL;
		ok = ok && connection.identity.cancel_key[0] == 0;
		ok = ok && connection.identity.cancel_key_length == 0;

		socket_io = &connection.socket_io;
		ok = ok && socket_io->send_buffer == NULL;
		ok = ok && socket_io->socket_io_context == NULL;
		ok = ok && socket_io->send_buffer_size == 0;
		ok = ok && socket_io->send_pointer == 0;
		ok = ok && socket_io->send_start == 0;
		ok = ok && socket_io->recv_buffer == NULL;
		ok = ok && socket_io->recv_pointer == 0;
		ok = ok && socket_io->recv_length == 0;
		ok = ok && !socket_io->comm_busy;
		ok = ok && !socket_io->comm_reading_msg;
		ok = ok && socket_io->win32_noblock == 0;

		ok = ok && connection.protocol.comm_methods == NULL;
		ok = ok && connection.protocol.fe_be_wait_set == NULL;
		ok = ok && connection.protocol.frontend_protocol == 0;
		ok = ok && connection.output.where_to_send_output == DestDebug;
		PgSetCurrentConnection(&connection);
		ok = ok && client_connection_check_interval == 0;
		PgSetCurrentConnection(saved_connection);
		ok = ok && !connection.interrupts.check_client_connection_pending;
		ok = ok && !connection.interrupts.client_connection_lost;
		ok = ok && !connection.startup.client_auth_in_progress;
		ok = ok && connection.startup.client_socket == NULL;
		ok = ok && !connection.startup.connection_warnings_emitted;
		ok = ok && connection.startup.connection_warning_context == NULL;
		ok = ok && connection.startup.connection_warning_messages == NIL;
		ok = ok && connection.startup.connection_warning_details == NIL;
		ok = ok && connection.client_connection_info.authn_id == NULL;
		ok = ok && connection.client_connection_info_context == NULL;
		ok = ok && connection.client_connection_info.auth_method == uaReject;
		ok = ok && !connection.client_connection_info_authn_id_owned;

		security = &connection.security;
		ok = ok && !security->ssl_loaded_verify_locations;
		ok = ok && security->gss_send_buffer == NULL;
		ok = ok && security->gss_send_length == 0;
		ok = ok && security->gss_send_next == 0;
		ok = ok && security->gss_send_consumed == 0;
		ok = ok && security->gss_recv_buffer == NULL;
		ok = ok && security->gss_recv_length == 0;
		ok = ok && security->gss_result_buffer == NULL;
		ok = ok && security->gss_result_length == 0;
		ok = ok && security->gss_result_next == 0;
		ok = ok && security->gss_max_packet_size == 0;
		ok = ok && security->pam_password == NULL;
		ok = ok && security->pam_port == NULL;
		ok = ok && !security->pam_no_password;

		PgConnectionResetClosedState(&connection);
		PgSetCurrentConnection(&connection);
		ok = ok && client_connection_check_interval == 0;
		PgSetCurrentConnection(saved_connection);
		ok = ok && connection.identity.port == NULL;
		ok = ok && connection.identity.port_context == NULL;
		ok = ok && connection.protocol.comm_methods == NULL;
		ok = ok && connection.startup.connection_warning_context == NULL;
		ok = ok && connection.startup.connection_warning_messages == NIL;
		ok = ok && connection.client_connection_info.authn_id == NULL;
		ok = ok && connection.client_connection_info_context == NULL;
		ok = ok && !connection.client_connection_info_authn_id_owned;
		ok = ok && connection.security.gss_send_buffer == NULL;
		ok = ok && connection.security.gss_recv_buffer == NULL;
		ok = ok && connection.security.gss_result_buffer == NULL;

		PgSetCurrentConnection(saved_connection);
	}
	PG_CATCH();
	{
		PgSetCurrentConnection(saved_connection);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "closed connection runtime state was not reset");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_connection_warning_state_is_connection_local);
Datum
test_connection_warning_state_is_connection_local(PG_FUNCTION_ARGS)
{
	PgConnection *saved_connection;
	PgThreadBackendRuntimeState state1;
	PgThreadBackendRuntimeState state2;
	PgConnection *fake_connection1;
	PgConnection *fake_connection2;
	MemoryContext warning_context1;
	MemoryContext warning_context2;
	bool		ok = true;

	saved_connection = CurrentPgConnection;
	InitializePgThreadRuntime(NULL);
	InitializePgThreadBackendRuntimeState(&state1, B_BACKEND, NULL, NULL);
	InitializePgThreadBackendRuntimeState(&state2, B_BACKEND, NULL, NULL);
	fake_connection1 = &state1.logical.connection;
	fake_connection2 = &state2.logical.connection;

	PG_TRY();
	{
		StoreConnectionWarningForConnection(fake_connection1,
											"warning one", "detail one", NULL);
		warning_context1 =
			fake_connection1->startup.connection_warning_context;
		ok = ok && warning_context1 != NULL;
		ok = ok && list_length(fake_connection1->startup.connection_warning_messages) == 1;
		ok = ok && list_length(fake_connection1->startup.connection_warning_details) == 1;
		ok = ok && strcmp((char *) linitial(fake_connection1->startup.connection_warning_messages),
						  "warning one") == 0;
		ok = ok && strcmp((char *) linitial(fake_connection1->startup.connection_warning_details),
						  "detail one") == 0;

		StoreConnectionWarningForConnection(fake_connection2,
											"warning two", "detail two", NULL);
		warning_context2 =
			fake_connection2->startup.connection_warning_context;
		ok = ok && warning_context2 != NULL;
		ok = ok && warning_context2 != warning_context1;
		ok = ok && list_length(fake_connection2->startup.connection_warning_messages) == 1;
		ok = ok && list_length(fake_connection2->startup.connection_warning_details) == 1;
		ok = ok && strcmp((char *) linitial(fake_connection2->startup.connection_warning_messages),
						  "warning two") == 0;
		ok = ok && strcmp((char *) linitial(fake_connection2->startup.connection_warning_details),
						  "detail two") == 0;

		ok = ok && fake_connection1->startup.connection_warning_context ==
			warning_context1;
		ok = ok && list_length(fake_connection1->startup.connection_warning_messages) == 1;
		ok = ok && strcmp((char *) linitial(fake_connection1->startup.connection_warning_messages),
						  "warning one") == 0;

		PgSetCurrentConnection(saved_connection);
	}
	PG_CATCH();
	{
		PgSetCurrentConnection(saved_connection);
		PgConnectionResetClosedState(fake_connection1);
		PgConnectionResetClosedState(fake_connection2);
		PG_RE_THROW();
	}
	PG_END_TRY();

	PgConnectionResetClosedState(fake_connection1);
	PgConnectionResetClosedState(fake_connection2);
	ok = ok && fake_connection1->startup.connection_warning_context == NULL;
	ok = ok && fake_connection1->startup.connection_warning_messages == NIL;
	ok = ok && fake_connection1->startup.connection_warning_details == NIL;
	ok = ok && fake_connection2->startup.connection_warning_context == NULL;
	ok = ok && fake_connection2->startup.connection_warning_messages == NIL;
	ok = ok && fake_connection2->startup.connection_warning_details == NIL;

	if (!ok)
		elog(ERROR, "connection warning state was not connection-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_connection_output_state_is_connection_local);
Datum
test_connection_output_state_is_connection_local(PG_FUNCTION_ARGS)
{
	PgConnection *saved_connection;
	PgConnection fake_connection1;
	PgConnection fake_connection2;
	CommandDest saved_where_to_send_output;
	int			saved_client_connection_check_interval;
	bool		ok = true;

	saved_connection = CurrentPgConnection;
	saved_where_to_send_output = whereToSendOutput;
	saved_client_connection_check_interval = client_connection_check_interval;
	MemSet(&fake_connection1, 0, sizeof(fake_connection1));
	MemSet(&fake_connection2, 0, sizeof(fake_connection2));
	fake_connection1.output.where_to_send_output = DestDebug;
	fake_connection2.output.where_to_send_output = DestDebug;

	PG_TRY();
	{
		PgSetCurrentConnection(&fake_connection1);
		whereToSendOutput = DestRemote;
		client_connection_check_interval = 11;

		PgSetCurrentConnection(&fake_connection2);
		ok = ok && whereToSendOutput == DestDebug;
		ok = ok && client_connection_check_interval == 0;
		whereToSendOutput = DestNone;
		client_connection_check_interval = 22;

		PgSetCurrentConnection(&fake_connection1);
		ok = ok && whereToSendOutput == DestRemote;
		ok = ok && client_connection_check_interval == 11;

		PgSetCurrentConnection(&fake_connection2);
		ok = ok && whereToSendOutput == DestNone;
		ok = ok && client_connection_check_interval == 22;

		PgSetCurrentConnection(saved_connection);
		whereToSendOutput = saved_where_to_send_output;
		client_connection_check_interval = saved_client_connection_check_interval;
	}
	PG_CATCH();
	{
		PgSetCurrentConnection(saved_connection);
		whereToSendOutput = saved_where_to_send_output;
		client_connection_check_interval = saved_client_connection_check_interval;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "connection output state was not connection-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_connection_identity_state_is_connection_local);
Datum
test_connection_identity_state_is_connection_local(PG_FUNCTION_ARGS)
{
	PgConnection *saved_connection;
	PgConnection fake_connection1;
	PgConnection fake_connection2;
	Port	   *saved_port;
	Port		fake_port1;
	Port		fake_port2;
	MemoryContext saved_port_context;
	uint8		saved_cancel_key[PG_CONNECTION_CANCEL_KEY_LENGTH];
	int			saved_cancel_key_length;
	bool		ok = true;

	saved_connection = CurrentPgConnection;
	saved_port = MyProcPort;
	saved_port_context = *PgCurrentPortContextRef();
	saved_cancel_key_length = MyCancelKeyLength;
	memcpy(saved_cancel_key, MyCancelKey, sizeof(saved_cancel_key));
	MemSet(&fake_connection1, 0, sizeof(fake_connection1));
	MemSet(&fake_connection2, 0, sizeof(fake_connection2));
	MemSet(&fake_port1, 0, sizeof(fake_port1));
	MemSet(&fake_port2, 0, sizeof(fake_port2));

	PG_TRY();
	{
		PgSetCurrentConnection(&fake_connection1);
		MyProcPort = &fake_port1;
		*PgCurrentPortContextRef() = TopMemoryContext;
		MyCancelKey[0] = 1;
		MyCancelKey[1] = 2;
		MyCancelKeyLength = 2;

		PgSetCurrentConnection(&fake_connection2);
		ok = ok && MyProcPort == NULL;
		ok = ok && *PgCurrentPortContextRef() == NULL;
		ok = ok && MyCancelKeyLength == 0;
		MyProcPort = &fake_port2;
		*PgCurrentPortContextRef() = ErrorContext;
		MyCancelKey[0] = 7;
		MyCancelKey[1] = 8;
		MyCancelKey[2] = 9;
		MyCancelKeyLength = 3;

		PgSetCurrentConnection(&fake_connection1);
		ok = ok && MyProcPort == &fake_port1;
		ok = ok && *PgCurrentPortContextRef() == TopMemoryContext;
		ok = ok && MyCancelKeyLength == 2;
		ok = ok && MyCancelKey[0] == 1;
		ok = ok && MyCancelKey[1] == 2;

		PgSetCurrentConnection(&fake_connection2);
		ok = ok && MyProcPort == &fake_port2;
		ok = ok && *PgCurrentPortContextRef() == ErrorContext;
		ok = ok && MyCancelKeyLength == 3;
		ok = ok && MyCancelKey[0] == 7;
		ok = ok && MyCancelKey[1] == 8;
		ok = ok && MyCancelKey[2] == 9;

		PgSetCurrentConnection(saved_connection);
		MyProcPort = saved_port;
		*PgCurrentPortContextRef() = saved_port_context;
		memcpy(MyCancelKey, saved_cancel_key, sizeof(saved_cancel_key));
		MyCancelKeyLength = saved_cancel_key_length;
	}
	PG_CATCH();
	{
		PgSetCurrentConnection(saved_connection);
		MyProcPort = saved_port;
		*PgCurrentPortContextRef() = saved_port_context;
		memcpy(MyCancelKey, saved_cancel_key, sizeof(saved_cancel_key));
		MyCancelKeyLength = saved_cancel_key_length;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "connection identity state was not connection-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_connection_interrupt_state_is_connection_local);
Datum
test_connection_interrupt_state_is_connection_local(PG_FUNCTION_ARGS)
{
	PgConnection *saved_connection;
	PgConnection fake_connection1;
	PgConnection fake_connection2;
	volatile sig_atomic_t saved_check_client_connection_pending;
	volatile sig_atomic_t saved_client_connection_lost;
	bool		ok = true;

	saved_connection = CurrentPgConnection;
	saved_check_client_connection_pending = CheckClientConnectionPending;
	saved_client_connection_lost = ClientConnectionLost;
	MemSet(&fake_connection1, 0, sizeof(fake_connection1));
	MemSet(&fake_connection2, 0, sizeof(fake_connection2));

	PG_TRY();
	{
		PgSetCurrentConnection(&fake_connection1);
		CheckClientConnectionPending = true;
		ClientConnectionLost = false;

		PgSetCurrentConnection(&fake_connection2);
		ok = ok && !CheckClientConnectionPending;
		ok = ok && !ClientConnectionLost;
		CheckClientConnectionPending = false;
		ClientConnectionLost = true;

		PgSetCurrentConnection(&fake_connection1);
		ok = ok && CheckClientConnectionPending;
		ok = ok && !ClientConnectionLost;

		PgSetCurrentConnection(&fake_connection2);
		ok = ok && !CheckClientConnectionPending;
		ok = ok && ClientConnectionLost;

		PgSetCurrentConnection(saved_connection);
		CheckClientConnectionPending = saved_check_client_connection_pending;
		ClientConnectionLost = saved_client_connection_lost;
	}
	PG_CATCH();
	{
		PgSetCurrentConnection(saved_connection);
		CheckClientConnectionPending = saved_check_client_connection_pending;
		ClientConnectionLost = saved_client_connection_lost;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "connection interrupt state was not connection-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_connection_frontend_protocol_is_connection_local);
Datum
test_connection_frontend_protocol_is_connection_local(PG_FUNCTION_ARGS)
{
	PgConnection *saved_connection;
	PgConnection fake_connection1;
	PgConnection fake_connection2;
	ProtocolVersion saved_frontend_protocol;
	bool		ok = true;

	saved_connection = CurrentPgConnection;
	saved_frontend_protocol = FrontendProtocol;
	MemSet(&fake_connection1, 0, sizeof(fake_connection1));
	MemSet(&fake_connection2, 0, sizeof(fake_connection2));

	PG_TRY();
	{
		PgSetCurrentConnection(&fake_connection1);
		FrontendProtocol = PG_PROTOCOL(3, 0);

		PgSetCurrentConnection(&fake_connection2);
		ok = ok && FrontendProtocol == 0;
		FrontendProtocol = PG_PROTOCOL(3, 2);

		PgSetCurrentConnection(&fake_connection1);
		ok = ok && FrontendProtocol == PG_PROTOCOL(3, 0);

		PgSetCurrentConnection(&fake_connection2);
		ok = ok && FrontendProtocol == PG_PROTOCOL(3, 2);

		PgSetCurrentConnection(saved_connection);
		FrontendProtocol = saved_frontend_protocol;
	}
	PG_CATCH();
	{
		PgSetCurrentConnection(saved_connection);
		FrontendProtocol = saved_frontend_protocol;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "frontend protocol state was not connection-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_connection_startup_state_is_connection_local);
Datum
test_connection_startup_state_is_connection_local(PG_FUNCTION_ARGS)
{
	PgConnection *saved_connection;
	PgConnection fake_connection1;
	PgConnection fake_connection2;
	struct ClientSocket *saved_client_socket;
	struct ClientSocket *fake_client_socket1;
	struct ClientSocket *fake_client_socket2;
	bool		saved_client_auth_in_progress;
	ConnectionTiming saved_timing;
	List	   *warning_messages1;
	List	   *warning_messages2;
	List	   *warning_details1;
	List	   *warning_details2;
	bool		ok = true;

	saved_connection = CurrentPgConnection;
	saved_client_auth_in_progress = ClientAuthInProgress;
	saved_client_socket = MyClientSocket;
	saved_timing = conn_timing;
	fake_client_socket1 = (struct ClientSocket *) &fake_connection1;
	fake_client_socket2 = (struct ClientSocket *) &fake_connection2;
	warning_messages1 = list_make1(&fake_connection1);
	warning_messages2 = list_make1(&fake_connection2);
	warning_details1 = list_make1(&fake_client_socket1);
	warning_details2 = list_make1(&fake_client_socket2);
	MemSet(&fake_connection1, 0, sizeof(fake_connection1));
	MemSet(&fake_connection2, 0, sizeof(fake_connection2));
	fake_connection1.startup.timing.ready_for_use = TIMESTAMP_MINUS_INFINITY;
	fake_connection2.startup.timing.ready_for_use = TIMESTAMP_MINUS_INFINITY;

	PG_TRY();
	{
		PgSetCurrentConnection(&fake_connection1);
		ClientAuthInProgress = true;
		MyClientSocket = fake_client_socket1;
		conn_timing.socket_create = 11;
		conn_timing.ready_for_use = 12;
		conn_timing.fork_start = 13;
		conn_timing.fork_end = 14;
		conn_timing.auth_start = 15;
		conn_timing.auth_end = 16;
		*PgCurrentConnectionWarningsEmittedRef() = true;
		*PgCurrentConnectionWarningMessagesRef() = warning_messages1;
		*PgCurrentConnectionWarningDetailsRef() = warning_details1;

		PgSetCurrentConnection(&fake_connection2);
		ok = ok && !ClientAuthInProgress;
		ok = ok && MyClientSocket == NULL;
		ok = ok && conn_timing.socket_create == 0;
		ok = ok && conn_timing.ready_for_use == TIMESTAMP_MINUS_INFINITY;
		ok = ok && conn_timing.fork_start == 0;
		ok = ok && conn_timing.fork_end == 0;
		ok = ok && conn_timing.auth_start == 0;
		ok = ok && conn_timing.auth_end == 0;
		ok = ok && !*PgCurrentConnectionWarningsEmittedRef();
		ok = ok && *PgCurrentConnectionWarningMessagesRef() == NIL;
		ok = ok && *PgCurrentConnectionWarningDetailsRef() == NIL;
		ClientAuthInProgress = false;
		MyClientSocket = fake_client_socket2;
		conn_timing.socket_create = 21;
		conn_timing.ready_for_use = 22;
		conn_timing.fork_start = 23;
		conn_timing.fork_end = 24;
		conn_timing.auth_start = 25;
		conn_timing.auth_end = 26;
		*PgCurrentConnectionWarningsEmittedRef() = false;
		*PgCurrentConnectionWarningMessagesRef() = warning_messages2;
		*PgCurrentConnectionWarningDetailsRef() = warning_details2;

		PgSetCurrentConnection(&fake_connection1);
		ok = ok && ClientAuthInProgress;
		ok = ok && MyClientSocket == fake_client_socket1;
		ok = ok && conn_timing.socket_create == 11;
		ok = ok && conn_timing.ready_for_use == 12;
		ok = ok && conn_timing.fork_start == 13;
		ok = ok && conn_timing.fork_end == 14;
		ok = ok && conn_timing.auth_start == 15;
		ok = ok && conn_timing.auth_end == 16;
		ok = ok && *PgCurrentConnectionWarningsEmittedRef();
		ok = ok && *PgCurrentConnectionWarningMessagesRef() ==
			warning_messages1;
		ok = ok && *PgCurrentConnectionWarningDetailsRef() ==
			warning_details1;

		PgSetCurrentConnection(&fake_connection2);
		ok = ok && !ClientAuthInProgress;
		ok = ok && MyClientSocket == fake_client_socket2;
		ok = ok && conn_timing.socket_create == 21;
		ok = ok && conn_timing.ready_for_use == 22;
		ok = ok && conn_timing.fork_start == 23;
		ok = ok && conn_timing.fork_end == 24;
		ok = ok && conn_timing.auth_start == 25;
		ok = ok && conn_timing.auth_end == 26;
		ok = ok && !*PgCurrentConnectionWarningsEmittedRef();
		ok = ok && *PgCurrentConnectionWarningMessagesRef() ==
			warning_messages2;
		ok = ok && *PgCurrentConnectionWarningDetailsRef() ==
			warning_details2;

		PgSetCurrentConnection(saved_connection);
		ClientAuthInProgress = saved_client_auth_in_progress;
		MyClientSocket = saved_client_socket;
		conn_timing = saved_timing;
	}
	PG_CATCH();
	{
		PgSetCurrentConnection(saved_connection);
		ClientAuthInProgress = saved_client_auth_in_progress;
		MyClientSocket = saved_client_socket;
		conn_timing = saved_timing;
		list_free(warning_messages1);
		list_free(warning_messages2);
		list_free(warning_details1);
		list_free(warning_details2);
		PG_RE_THROW();
	}
	PG_END_TRY();

	list_free(warning_messages1);
	list_free(warning_messages2);
	list_free(warning_details1);
	list_free(warning_details2);

	if (!ok)
		elog(ERROR, "connection startup state was not connection-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_client_connection_info_is_connection_local);
Datum
test_client_connection_info_is_connection_local(PG_FUNCTION_ARGS)
{
	PgConnection *saved_connection;
	PgConnection fake_connection1;
	PgConnection fake_connection2;
	const char *saved_authn_id;
	UserAuth	saved_auth_method;
	bool		ok = true;

	saved_connection = CurrentPgConnection;
	saved_authn_id = MyClientConnectionInfo.authn_id;
	saved_auth_method = MyClientConnectionInfo.auth_method;
	MemSet(&fake_connection1, 0, sizeof(fake_connection1));
	MemSet(&fake_connection2, 0, sizeof(fake_connection2));

	PG_TRY();
	{
		PgSetCurrentConnection(&fake_connection1);
		MyClientConnectionInfo.authn_id = "connection-one";
		MyClientConnectionInfo.auth_method = uaTrust;

		PgSetCurrentConnection(&fake_connection2);
		ok = ok && MyClientConnectionInfo.authn_id == NULL;
		MyClientConnectionInfo.authn_id = "connection-two";
		MyClientConnectionInfo.auth_method = uaSCRAM;

		PgSetCurrentConnection(&fake_connection1);
		ok = ok && strcmp(MyClientConnectionInfo.authn_id,
						  "connection-one") == 0;
		ok = ok && MyClientConnectionInfo.auth_method == uaTrust;

		PgSetCurrentConnection(&fake_connection2);
		ok = ok && strcmp(MyClientConnectionInfo.authn_id,
						  "connection-two") == 0;
		ok = ok && MyClientConnectionInfo.auth_method == uaSCRAM;

		PgSetCurrentConnection(saved_connection);
		MyClientConnectionInfo.authn_id = saved_authn_id;
		MyClientConnectionInfo.auth_method = saved_auth_method;
	}
	PG_CATCH();
	{
		PgSetCurrentConnection(saved_connection);
		MyClientConnectionInfo.authn_id = saved_authn_id;
		MyClientConnectionInfo.auth_method = saved_auth_method;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "client connection info was not connection-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_connection_security_state_is_connection_local);
Datum
test_connection_security_state_is_connection_local(PG_FUNCTION_ARGS)
{
	PgConnection *saved_connection;
	PgConnection fake_connection1;
	PgConnection fake_connection2;
	PgConnectionSecurityState *security;
	bool		ok = true;

	saved_connection = CurrentPgConnection;
	MemSet(&fake_connection1, 0, sizeof(fake_connection1));
	MemSet(&fake_connection2, 0, sizeof(fake_connection2));

	PG_TRY();
	{
		PgSetCurrentConnection(&fake_connection1);
		security = PgCurrentConnectionSecurityStateRef();
		ok = ok && !security->ssl_loaded_verify_locations;
		ok = ok && security->gss_send_buffer == NULL;
		ok = ok && security->gss_send_length == 0;
		ok = ok && security->gss_recv_buffer == NULL;
		ok = ok && security->gss_result_buffer == NULL;
		ok = ok && security->gss_max_packet_size == 0;
		ok = ok && security->pam_password == NULL;
		ok = ok && security->pam_port == NULL;
		ok = ok && !security->pam_no_password;
		security->ssl_loaded_verify_locations = true;
		security->gss_send_buffer = (char *) &fake_connection1;
		security->gss_send_length = 11;
		security->gss_send_next = 12;
		security->gss_send_consumed = 13;
		security->gss_recv_buffer = (char *) &fake_connection2;
		security->gss_recv_length = 14;
		security->gss_result_buffer = (char *) &saved_connection;
		security->gss_result_length = 15;
		security->gss_result_next = 16;
		security->gss_max_packet_size = 17;
		security->pam_password = "pam-one";
		security->pam_port = (struct Port *) &fake_connection1;
		security->pam_no_password = true;

		PgSetCurrentConnection(&fake_connection2);
		security = PgCurrentConnectionSecurityStateRef();
		ok = ok && !security->ssl_loaded_verify_locations;
		ok = ok && security->gss_send_buffer == NULL;
		ok = ok && security->gss_send_length == 0;
		ok = ok && security->gss_recv_buffer == NULL;
		ok = ok && security->gss_result_buffer == NULL;
		ok = ok && security->gss_max_packet_size == 0;
		ok = ok && security->pam_password == NULL;
		ok = ok && security->pam_port == NULL;
		ok = ok && !security->pam_no_password;
		security->ssl_loaded_verify_locations = false;
		security->gss_send_buffer = (char *) &fake_connection2;
		security->gss_send_length = 21;
		security->gss_send_next = 22;
		security->gss_send_consumed = 23;
		security->gss_recv_buffer = (char *) &fake_connection1;
		security->gss_recv_length = 24;
		security->gss_result_buffer = (char *) &fake_connection2;
		security->gss_result_length = 25;
		security->gss_result_next = 26;
		security->gss_max_packet_size = 27;
		security->pam_password = "pam-two";
		security->pam_port = (struct Port *) &fake_connection2;
		security->pam_no_password = false;

		PgSetCurrentConnection(&fake_connection1);
		security = PgCurrentConnectionSecurityStateRef();
		ok = ok && security->ssl_loaded_verify_locations;
		ok = ok && security->gss_send_buffer == (char *) &fake_connection1;
		ok = ok && security->gss_send_length == 11;
		ok = ok && security->gss_send_next == 12;
		ok = ok && security->gss_send_consumed == 13;
		ok = ok && security->gss_recv_buffer == (char *) &fake_connection2;
		ok = ok && security->gss_recv_length == 14;
		ok = ok && security->gss_result_buffer == (char *) &saved_connection;
		ok = ok && security->gss_result_length == 15;
		ok = ok && security->gss_result_next == 16;
		ok = ok && security->gss_max_packet_size == 17;
		ok = ok && strcmp(security->pam_password, "pam-one") == 0;
		ok = ok && security->pam_port == (struct Port *) &fake_connection1;
		ok = ok && security->pam_no_password;

		PgSetCurrentConnection(&fake_connection2);
		security = PgCurrentConnectionSecurityStateRef();
		ok = ok && !security->ssl_loaded_verify_locations;
		ok = ok && security->gss_send_buffer == (char *) &fake_connection2;
		ok = ok && security->gss_send_length == 21;
		ok = ok && security->gss_send_next == 22;
		ok = ok && security->gss_send_consumed == 23;
		ok = ok && security->gss_recv_buffer == (char *) &fake_connection1;
		ok = ok && security->gss_recv_length == 24;
		ok = ok && security->gss_result_buffer == (char *) &fake_connection2;
		ok = ok && security->gss_result_length == 25;
		ok = ok && security->gss_result_next == 26;
		ok = ok && security->gss_max_packet_size == 27;
		ok = ok && strcmp(security->pam_password, "pam-two") == 0;
		ok = ok && security->pam_port == (struct Port *) &fake_connection2;
		ok = ok && !security->pam_no_password;

		PgSetCurrentConnection(saved_connection);
	}
	PG_CATCH();
	{
		PgSetCurrentConnection(saved_connection);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "connection security state was not connection-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_thread_install_adopts_connection_fallback_state);
Datum
test_thread_install_adopts_connection_fallback_state(PG_FUNCTION_ARGS)
{
	PgConnection *saved_connection;
	PgConnection connection;
	Port		fallback_port;
	Port		preserved_port;
	const PQcommMethods methods = {0};
	WaitEventSet *fake_wait_set;
	PgConnectionSocketIOState *socket_io;
	PgConnectionSecurityState *security;
	bool		ok = true;

	saved_connection = CurrentPgConnection;
	MemSet(&connection, 0, sizeof(connection));
	MemSet(&fallback_port, 0, sizeof(fallback_port));
	MemSet(&preserved_port, 0, sizeof(preserved_port));
	fake_wait_set = (WaitEventSet *) &connection;

	PG_TRY();
	{
		PgSetCurrentConnection(NULL);
		MyProcPort = &fallback_port;
		MyCancelKey[0] = 11;
		MyCancelKeyLength = 1;
		socket_io = PgCurrentConnectionSocketIORef();
		socket_io->send_buffer = (char *) "connection fallback";
		socket_io->send_buffer_size = 32;
		PqCommMethods = &methods;
		FeBeWaitSet = fake_wait_set;
		FrontendProtocol = PG_PROTOCOL(3, 2);
		whereToSendOutput = DestRemote;
		client_connection_check_interval = 13;
		CheckClientConnectionPending = true;
		ClientConnectionLost = true;
		ClientAuthInProgress = true;
		MyClientSocket = (struct ClientSocket *) &fallback_port;
		conn_timing.socket_create = 21;
		conn_timing.ready_for_use = 22;
		MyClientConnectionInfo.authn_id = "fallback-authn";
		MyClientConnectionInfo.auth_method = uaSCRAM;
		security = PgCurrentConnectionSecurityStateRef();
		security->ssl_loaded_verify_locations = true;
		security->gss_send_buffer = (char *) "gss-send";
		security->gss_send_length = 31;
		security->pam_password = "pam-fallback";
		security->pam_port = &fallback_port;
		security->pam_no_password = true;

		PgConnectionAdoptEarlyState(&connection, &preserved_port);

		PgSetCurrentConnection(&connection);
		ok = ok && MyProcPort == &preserved_port;
		ok = ok && MyCancelKey[0] == 11;
		ok = ok && MyCancelKeyLength == 1;
		ok = ok && strcmp(PgCurrentConnectionSocketIORef()->send_buffer,
						  "connection fallback") == 0;
		ok = ok && PgCurrentConnectionSocketIORef()->send_buffer_size == 32;
		ok = ok && PqCommMethods == &methods;
		ok = ok && FeBeWaitSet == fake_wait_set;
		ok = ok && FrontendProtocol == PG_PROTOCOL(3, 2);
		ok = ok && whereToSendOutput == DestRemote;
		ok = ok && client_connection_check_interval == 13;
		ok = ok && CheckClientConnectionPending;
		ok = ok && ClientConnectionLost;
		ok = ok && ClientAuthInProgress;
		ok = ok && MyClientSocket == (struct ClientSocket *) &fallback_port;
		ok = ok && conn_timing.socket_create == 21;
		ok = ok && conn_timing.ready_for_use == 22;
		ok = ok && strcmp(MyClientConnectionInfo.authn_id,
						  "fallback-authn") == 0;
		ok = ok && MyClientConnectionInfo.auth_method == uaSCRAM;
		ok = ok && PgCurrentConnectionSecurityStateRef()->ssl_loaded_verify_locations;
		ok = ok && strcmp(PgCurrentConnectionSecurityStateRef()->gss_send_buffer,
						  "gss-send") == 0;
		ok = ok && PgCurrentConnectionSecurityStateRef()->gss_send_length == 31;
		ok = ok && strcmp(PgCurrentConnectionSecurityStateRef()->pam_password,
						  "pam-fallback") == 0;
		ok = ok && PgCurrentConnectionSecurityStateRef()->pam_port ==
			&fallback_port;
		ok = ok && PgCurrentConnectionSecurityStateRef()->pam_no_password;

		PgSetCurrentConnection(NULL);
		ok = ok && MyProcPort == NULL;
		ok = ok && MyCancelKeyLength == 0;
		ok = ok && PgCurrentConnectionSocketIORef()->send_buffer == NULL;
		ok = ok && PgCurrentConnectionSocketIORef()->send_buffer_size == 0;
		ok = ok && PqCommMethods == NULL;
		ok = ok && FeBeWaitSet == NULL;
		ok = ok && FrontendProtocol == 0;
		ok = ok && whereToSendOutput == DestDebug;
		ok = ok && client_connection_check_interval == 0;
		ok = ok && !CheckClientConnectionPending;
		ok = ok && !ClientConnectionLost;
		ok = ok && !ClientAuthInProgress;
		ok = ok && MyClientSocket == NULL;
		ok = ok && conn_timing.socket_create == 0;
		ok = ok && conn_timing.ready_for_use == TIMESTAMP_MINUS_INFINITY;
		ok = ok && MyClientConnectionInfo.authn_id == NULL;
		ok = ok && !PgCurrentConnectionSecurityStateRef()->ssl_loaded_verify_locations;
		ok = ok && PgCurrentConnectionSecurityStateRef()->gss_send_buffer == NULL;
		ok = ok && PgCurrentConnectionSecurityStateRef()->pam_password == NULL;

		PgSetCurrentConnection(saved_connection);
	}
	PG_CATCH();
	{
		PgSetCurrentConnection(saved_connection);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR,
			 "thread backend install did not adopt connection fallback state");

	PG_RETURN_BOOL(true);
}
