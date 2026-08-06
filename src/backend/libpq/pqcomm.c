/*-------------------------------------------------------------------------
 *
 * pqcomm.c
 *	  Communication functions between the Frontend and the Backend
 *
 * These routines handle the low-level details of communication between
 * frontend and backend.  They just shove data across the communication
 * channel, and are ignorant of the semantics of the data.
 *
 * To emit an outgoing message, use the routines in pqformat.c to construct
 * the message in a buffer and then emit it in one call to pq_putmessage.
 * There are no functions to send raw bytes or partial messages; this
 * ensures that the channel will not be clogged by an incomplete message if
 * execution is aborted by ereport(ERROR) partway through the message.
 *
 * At one time, libpq was shared between frontend and backend, but now
 * the backend's "backend/libpq" is quite separate from "interfaces/libpq".
 * All that remains is similarities of names to trap the unwary...
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *	src/backend/libpq/pqcomm.c
 *
 *-------------------------------------------------------------------------
 */

/*------------------------
 * INTERFACE ROUTINES
 *
 * setup/teardown:
 *		ListenServerPort	- Open postmaster's server port
 *		AcceptConnection	- Accept new connection with client
 *		TouchSocketFiles	- Protect socket files against /tmp cleaners
 *		pq_init				- initialize libpq at backend startup
 *		socket_comm_reset	- reset libpq during error recovery
 *		socket_close		- shutdown libpq at backend exit
 *
 * low-level I/O:
 *		pq_getbytes		- get a known number of bytes from connection
 *		pq_getmessage	- get a message with length word from connection
 *		pq_getbyte		- get next byte from connection
 *		pq_peekbyte		- peek at next byte from connection
 *		pq_flush		- flush pending output
 *		pq_flush_if_writable - flush pending output if writable without blocking
 *		pq_getbyte_if_available - get a byte if available without blocking
 *
 * message-level I/O
 *		pq_putmessage	- send a normal message (suppressed in COPY OUT mode)
 *		pq_putmessage_noblock - buffer a normal message (suppressed in COPY OUT)
 *
 *------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <fcntl.h>
#include <grp.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <utime.h>
#ifdef WIN32
#include <mstcpip.h>
#endif

#include "common/ip.h"
#include "libpq/libpq.h"
#include "miscadmin.h"
#include "port/pg_bswap.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/waiteventset.h"
#include "utils/backend_runtime.h"
#include "utils/guc_hooks.h"
#include "utils/memutils.h"

/*
 * Cope with the various platform-specific ways to spell TCP keepalive socket
 * options.  This doesn't cover Windows, which as usual does its own thing.
 */
#if defined(TCP_KEEPIDLE)
/* TCP_KEEPIDLE is the name of this option on Linux and *BSD */
#define PG_TCP_KEEPALIVE_IDLE TCP_KEEPIDLE
#define PG_TCP_KEEPALIVE_IDLE_STR "TCP_KEEPIDLE"
#elif defined(TCP_KEEPALIVE_THRESHOLD)
/* TCP_KEEPALIVE_THRESHOLD is the name of this option on Solaris >= 11 */
#define PG_TCP_KEEPALIVE_IDLE TCP_KEEPALIVE_THRESHOLD
#define PG_TCP_KEEPALIVE_IDLE_STR "TCP_KEEPALIVE_THRESHOLD"
#elif defined(TCP_KEEPALIVE) && defined(__darwin__)
/* TCP_KEEPALIVE is the name of this option on macOS */
/* Caution: Solaris has this symbol but it means something different */
#define PG_TCP_KEEPALIVE_IDLE TCP_KEEPALIVE
#define PG_TCP_KEEPALIVE_IDLE_STR "TCP_KEEPALIVE"
#endif

/*
 * Configuration options
 */
PG_GLOBAL_RUNTIME int Unix_socket_permissions;
PG_GLOBAL_RUNTIME char *Unix_socket_group;

/* Where the Unix socket files are (list of palloc'd strings) */
static PG_GLOBAL_RUNTIME List *sock_paths = NIL;

/*
 * Buffers for low-level I/O.
 *
 * The receive buffer is fixed size. Send buffer is usually 8k, but can be
 * enlarged by pq_putmessage_noblock() if the message doesn't fit otherwise.
 */

#define PQ_SEND_BUFFER_SIZE PG_CONNECTION_SEND_BUFFER_SIZE
#define PQ_RECV_BUFFER_SIZE PG_CONNECTION_RECV_BUFFER_SIZE

#define PqSendBuffer (PqSocketIO()->send_buffer)
#define PqSendBufferSize (PqSocketIO()->send_buffer_size)
#define PqSendPointer (PqSocketIO()->send_pointer)
#define PqSendStart (PqSocketIO()->send_start)
#define PqRecvBuffer (PqSocketIO()->recv_buffer)
#define PqRecvPointer (PqSocketIO()->recv_pointer)
#define PqRecvLength (PqSocketIO()->recv_length)

/*
 * Message status
 */
#define PqCommBusy (PqSocketIO()->comm_busy)
#define PqCommReadingMsg (PqSocketIO()->comm_reading_msg)


/* Internal functions */
static void socket_comm_reset(void);
static void socket_close(int code, Datum arg);
static void socket_set_nonblocking(bool nonblocking);
static int	socket_flush(void);
static int	socket_flush_if_writable(void);
static bool socket_is_send_pending(void);
static int	socket_putmessage(char msgtype, const char *s, size_t len);
static void socket_putmessage_noblock(char msgtype, const char *s, size_t len);
static inline PgConnectionSocketIOState *PqSocketIO(void);
static pg_always_inline void pq_advance_recv_pointer(PgConnectionSocketIOState *io,
															   size_t amount);
static bool pq_connection_transport_buffered_input(PgConnection *connection);
static uint32 pq_connection_transport_wait_events(PgConnection *connection);
static int	pq_probe_recvbuf(PgConnection *connection);
static PgProtocolByteResult pq_probe_message_type(PgConnection *connection,
												  PgProtocolByteProbe *probe,
												  bool probe_kernel);
static inline int pq_getbyte_from(PgConnectionSocketIOState *io);
static inline int pq_getbytes_from(PgConnectionSocketIOState *io,
								   void *b, size_t len);
static inline void pq_ensure_recv_buffer(PgConnectionSocketIOState *io);
static int	pq_discardbytes_from(PgConnectionSocketIOState *io, size_t len);
static inline int internal_putbytes(PgConnectionSocketIOState *io,
								   const void *b, size_t len);
static inline int internal_flush(PgConnectionSocketIOState *io);
static pg_noinline int internal_flush_buffer(const char *buf, size_t *start,
											 size_t *end);

static int	Lock_AF_UNIX(const char *unixSocketDir, const char *unixSocketPath);
static int	Setup_AF_UNIX(const char *sock_path);

static const PQcommMethods PqCommSocketMethods = {
	.comm_reset = socket_comm_reset,
	.flush = socket_flush,
	.flush_if_writable = socket_flush_if_writable,
	.is_send_pending = socket_is_send_pending,
	.putmessage = socket_putmessage,
	.putmessage_noblock = socket_putmessage_noblock
};

static inline PgConnectionSocketIOState *
PqSocketIO(void)
{
	PgConnectionSocketIOState *io = CurrentPgConnectionSocketIORuntimeState;

	if (likely(io != NULL))
	{
		Assert(CurrentPgConnection == NULL ||
			   io == &CurrentPgConnection->socket_io);
		return io;
	}

	return PgCurrentConnectionSocketIORef();
}

static inline void
pq_ensure_recv_buffer(PgConnectionSocketIOState *io)
{
	Assert(io != NULL);

	if (io->recv_buffer == NULL)
		io->recv_buffer = MemoryContextAlloc(PgRuntimeGetOwnedMemoryContext(
												 PgCurrentConnectionSocketIOContextRef(),
												 "socket I/O connection state"),
											 PQ_RECV_BUFFER_SIZE);
}

static pg_always_inline void
pq_advance_recv_pointer(PgConnectionSocketIOState *io, size_t amount)
{
	Assert(io != NULL);

	io->recv_pointer += amount;
}

static bool
pq_connection_transport_buffered_input(PgConnection *connection)
{
	Port	   *port;
	PgConnectionSocketIOState *io;
	PgConnectionSecurityState *security;

	Assert(connection != NULL);

	io = &connection->socket_io;
	if (io->recv_pointer < io->recv_length)
		return true;

	port = connection->identity.port;
	if (port != NULL && port->raw_buf_remaining > 0)
		return true;

	security = &connection->security;
	if (security->gss_result_next < security->gss_result_length)
		return true;

	return false;
}

static uint32
pq_connection_transport_wait_events(PgConnection *connection)
{
	PgConnectionSecurityState *security;
	Port	   *port;

	Assert(connection != NULL);

	security = &connection->security;
	if (security->gss_send_next < security->gss_send_length)
		return WL_SOCKET_WRITEABLE;

	port = connection->identity.port;
	if (port == NULL)
		return 0;

#ifdef USE_SSL
	if (port->ssl_in_use)
		return 0;
#endif
#ifdef ENABLE_GSS
	if (port->gss && port->gss->enc)
		return 0;
#endif

	return WL_SOCKET_READABLE;
}


/* --------------------------------
 *		pq_init - initialize libpq at backend startup
 * --------------------------------
 */
Port *
pq_init(ClientSocket *client_sock)
{
	MemoryContext oldcontext;
	MemoryContext port_context;
	Port	   *port;
	int			socket_pos PG_USED_FOR_ASSERTS_ONLY;
	int			latch_pos PG_USED_FOR_ASSERTS_ONLY;

	/* allocate the Port struct and copy the ClientSocket contents to it */
	port_context = PgRuntimeGetOwnedMemoryContextWithSizes(
		PgCurrentPortContextRef(), "PortContext", ALLOCSET_START_SMALL_SIZES);
	oldcontext = MemoryContextSwitchTo(port_context);
	port = palloc0_object(Port);
	MemoryContextSwitchTo(oldcontext);
	port->sock = client_sock->sock;
	port->ssl_prenegotiated = client_sock->ssl_negotiated;
	memcpy(&port->raddr.addr, &client_sock->raddr.addr, client_sock->raddr.salen);
	port->raddr.salen = client_sock->raddr.salen;

	/* fill in the server (local) address */
	port->laddr.salen = sizeof(port->laddr.addr);
	if (getsockname(port->sock,
					(struct sockaddr *) &port->laddr.addr,
					&port->laddr.salen) < 0)
	{
		ereport(FATAL,
				(errmsg("%s() failed: %m", "getsockname")));
	}

	/* select NODELAY and KEEPALIVE options if it's a TCP connection */
	if (port->laddr.addr.ss_family != AF_UNIX)
	{
		int			on;
#ifdef WIN32
		int			oldopt;
		int			optlen;
		int			newopt;
#endif

#ifdef	TCP_NODELAY
		on = 1;
		if (setsockopt(port->sock, IPPROTO_TCP, TCP_NODELAY,
					   (char *) &on, sizeof(on)) < 0)
		{
			ereport(FATAL,
					(errmsg("%s(%s) failed: %m", "setsockopt", "TCP_NODELAY")));
		}
#endif
		on = 1;
		if (setsockopt(port->sock, SOL_SOCKET, SO_KEEPALIVE,
					   (char *) &on, sizeof(on)) < 0)
		{
			ereport(FATAL,
					(errmsg("%s(%s) failed: %m", "setsockopt", "SO_KEEPALIVE")));
		}

#ifdef WIN32

		/*
		 * This is a Win32 socket optimization.  The OS send buffer should be
		 * large enough to send the whole Postgres send buffer in one go, or
		 * performance suffers.  The Postgres send buffer can be enlarged if a
		 * very large message needs to be sent, but we won't attempt to
		 * enlarge the OS buffer if that happens, so somewhat arbitrarily
		 * ensure that the OS buffer is at least PQ_SEND_BUFFER_SIZE * 4.
		 * (That's 32kB with the current default).
		 *
		 * The default OS buffer size used to be 8kB in earlier Windows
		 * versions, but was raised to 64kB in Windows 2012.  So it shouldn't
		 * be necessary to change it in later versions anymore.  Changing it
		 * unnecessarily can even reduce performance, because setting
		 * SO_SNDBUF in the application disables the "dynamic send buffering"
		 * feature that was introduced in Windows 7.  So before fiddling with
		 * SO_SNDBUF, check if the current buffer size is already large enough
		 * and only increase it if necessary.
		 *
		 * See https://support.microsoft.com/kb/823764/EN-US/ and
		 * https://msdn.microsoft.com/en-us/library/bb736549%28v=vs.85%29.aspx
		 */
		optlen = sizeof(oldopt);
		if (getsockopt(port->sock, SOL_SOCKET, SO_SNDBUF, (char *) &oldopt,
					   &optlen) < 0)
		{
			ereport(FATAL,
					(errmsg("%s(%s) failed: %m", "getsockopt", "SO_SNDBUF")));
		}
		newopt = PQ_SEND_BUFFER_SIZE * 4;
		if (oldopt < newopt)
		{
			if (setsockopt(port->sock, SOL_SOCKET, SO_SNDBUF, (char *) &newopt,
						   sizeof(newopt)) < 0)
			{
				ereport(FATAL,
						(errmsg("%s(%s) failed: %m", "setsockopt", "SO_SNDBUF")));
			}
		}
#endif

		/*
		 * Also apply the current keepalive parameters.  If we fail to set a
		 * parameter, don't error out, because these aren't universally
		 * supported.  (Note: you might think we need to reset the GUC
		 * variables to 0 in such a case, but it's not necessary because the
		 * show hooks for these variables report the truth anyway.)
		 */
		(void) pq_setkeepalivesidle(tcp_keepalives_idle, port);
		(void) pq_setkeepalivesinterval(tcp_keepalives_interval, port);
		(void) pq_setkeepalivescount(tcp_keepalives_count, port);
		(void) pq_settcpusertimeout(tcp_user_timeout, port);
	}

	/* initialize state variables */
	PqCommMethods = &PqCommSocketMethods;
	PqSendBufferSize = PQ_SEND_BUFFER_SIZE;
	{
		MemoryContext socket_io_context;

		socket_io_context =
			PgRuntimeGetOwnedMemoryContext(PgCurrentConnectionSocketIOContextRef(),
										   "socket I/O connection state");
		PqSendBuffer = MemoryContextAlloc(socket_io_context, PqSendBufferSize);
		PqRecvBuffer = MemoryContextAlloc(socket_io_context, PQ_RECV_BUFFER_SIZE);
	}
	PqSendPointer = PqSendStart = PqRecvPointer = PqRecvLength = 0;
	PqCommBusy = false;
	PqCommReadingMsg = false;

	/* set up process-exit hook to close the socket */
	on_proc_exit(socket_close, 0);

	/*
	 * The Port now owns the accepted socket and socket_close() is registered
	 * as its exit backstop.  Mark the launch-time ClientSocket as consumed so
	 * threaded backend teardown can distinguish an early-startup failure from
	 * a descriptor already owned by MyProcPort.
	 */
	client_sock->sock = PGINVALID_SOCKET;

	/*
	 * In backends (as soon as forked) we operate the underlying socket in
	 * nonblocking mode and use latches to implement blocking semantics if
	 * needed. That allows us to provide safely interruptible reads and
	 * writes.
	 */
#ifndef WIN32
	if (!pg_set_noblock(port->sock))
		ereport(FATAL,
				(errmsg("could not set socket to nonblocking mode: %m")));
#endif

#ifndef WIN32

	/* Don't give the socket to any subprograms we execute. */
	if (fcntl(port->sock, F_SETFD, FD_CLOEXEC) < 0)
		elog(FATAL, "fcntl(F_SETFD) failed on socket: %m");
#endif

	FeBeWaitSet = CreateWaitEventSet(NULL, FeBeWaitSetNEvents);
	socket_pos = AddWaitEventToSet(FeBeWaitSet, WL_SOCKET_WRITEABLE,
								   port->sock, NULL, NULL);
	latch_pos = AddWaitEventToSet(FeBeWaitSet, WL_LATCH_SET, PGINVALID_SOCKET,
								  MyLatch, NULL);
	AddWaitEventToSet(FeBeWaitSet, WL_POSTMASTER_DEATH, PGINVALID_SOCKET,
					  NULL, NULL);

	/*
	 * The event positions match the order we added them, but let's sanity
	 * check them to be sure.
	 */
	Assert(socket_pos == FeBeWaitSetSocketPos);
	Assert(latch_pos == FeBeWaitSetLatchPos);

	return port;
}

/* --------------------------------
 *		socket_comm_reset - reset libpq during error recovery
 *
 * This is called from error recovery at the outer idle loop.  It's
 * just to get us out of trouble if we somehow manage to elog() from
 * inside a pqcomm.c routine (which ideally will never happen, but...)
 * --------------------------------
 */
static void
socket_comm_reset(void)
{
	/* Do not throw away pending data, but do reset the busy flag */
	PqCommBusy = false;
}

/* --------------------------------
 *		socket_close - shutdown libpq at backend exit
 *
 * This is the one pg_on_exit_callback in place during BackendInitialize().
 * That function's unusual signal handling constrains that this callback be
 * safe to run at any instant.
 * --------------------------------
 */
static void
socket_close(int code, Datum arg)
{
	PgConnection *connection = CurrentPgConnection;
	MemoryContext port_context = NULL;
	MemoryContext *port_context_ref = PgCurrentPortContextRef();
	MemoryContext *socket_io_context = PgCurrentConnectionSocketIOContextRef();

	if (FeBeWaitSet != NULL)
	{
		FreeWaitEventSet(FeBeWaitSet);
		FeBeWaitSet = NULL;
	}

	if (*socket_io_context != NULL)
	{
		PgRuntimeDeleteOwnedMemoryContext(socket_io_context);
		PqSendBuffer = NULL;
		PqRecvBuffer = NULL;
		PqSendBufferSize = 0;
		PqSendPointer = PqSendStart = 0;
		PqRecvPointer = PqRecvLength = 0;
	}
	else if (PqSendBuffer != NULL || PqRecvBuffer != NULL)
	{
		if (PqSendBuffer != NULL)
			pfree(PqSendBuffer);
		if (PqRecvBuffer != NULL)
			pfree(PqRecvBuffer);
		PqSendBuffer = NULL;
		PqRecvBuffer = NULL;
		PqSendBufferSize = 0;
		PqSendPointer = PqSendStart = 0;
		PqRecvPointer = PqRecvLength = 0;
	}

	/* Nothing to do in a standalone backend, where MyProcPort is NULL. */
	if (MyProcPort != NULL)
	{
		port_context = *port_context_ref;
		if (port_context == NULL)
			port_context = GetMemoryChunkContext(MyProcPort);

#ifdef ENABLE_GSS
		/*
		 * Shutdown GSSAPI layer.  This section does nothing when interrupting
		 * BackendInitialize(), because pg_GSS_recvauth() makes first use of
		 * "ctx" and "cred".
		 *
		 * Note that we don't bother to free MyProcPort->gss, since we're
		 * about to exit anyway.
		 */
		if (MyProcPort->gss)
		{
			OM_uint32	min_s;

			if (MyProcPort->gss->ctx != GSS_C_NO_CONTEXT)
				gss_delete_sec_context(&min_s, &MyProcPort->gss->ctx, NULL);

			if (MyProcPort->gss->cred != GSS_C_NO_CREDENTIAL)
				gss_release_cred(&min_s, &MyProcPort->gss->cred);
		}
#endif							/* ENABLE_GSS */

		/*
		 * Cleanly shut down SSL layer.  Nowhere else does a postmaster child
		 * call this, so this is safe when interrupting BackendInitialize().
		 */
		secure_close(MyProcPort);

		/*
		 * Process backends leave the socket open until process death, which
		 * allows clients to perform a synchronous close.  Threaded backends
		 * cannot rely on process exit to release the accepted descriptor.
		 */
		if (PgRuntimeIsThreadBacked(CurrentPgRuntime) &&
			MyProcPort->sock != PGINVALID_SOCKET)
			closesocket(MyProcPort->sock);

		/* Prevent any further I/O through this Port. */
		MyProcPort->sock = PGINVALID_SOCKET;

		if (port_context != NULL && port_context != TopMemoryContext)
		{
			MyProcPort = NULL;
			if (*port_context_ref == port_context)
				PgRuntimeDeleteOwnedMemoryContext(port_context_ref);
			else
				MemoryContextDelete(port_context);
		}
	}

	if (connection != NULL)
		PgConnectionResetClosedState(connection);
}



/* --------------------------------
 * Postmaster functions to handle sockets.
 * --------------------------------
 */

/*
 * ListenServerPort -- open a "listening" port to accept connections.
 *
 * family should be AF_UNIX or AF_UNSPEC; portNumber is the port number.
 * For AF_UNIX ports, hostName should be NULL and unixSocketDir must be
 * specified.  For TCP ports, hostName is either NULL for all interfaces or
 * the interface to listen on, and unixSocketDir is ignored (can be NULL).
 *
 * Successfully opened sockets are appended to the ListenSockets[] array.  On
 * entry, *NumListenSockets holds the number of elements currently in the
 * array, and it is updated to reflect the opened sockets.  MaxListen is the
 * allocated size of the array.
 *
 * RETURNS: STATUS_OK or STATUS_ERROR
 */
int
ListenServerPort(int family, const char *hostName, unsigned short portNumber,
				 const char *unixSocketDir,
				 pgsocket ListenSockets[], int *NumListenSockets, int MaxListen)
{
	pgsocket	fd;
	int			err;
	int			maxconn;
	int			ret;
	char		portNumberStr[32];
	const char *familyDesc;
	char		familyDescBuf[64];
	const char *addrDesc;
	char		addrBuf[NI_MAXHOST];
	char	   *service;
	struct addrinfo *addrs = NULL,
			   *addr;
	struct addrinfo hint;
	int			added = 0;
	char		unixSocketPath[MAXPGPATH];
#if !defined(WIN32) || defined(IPV6_V6ONLY)
	int			one = 1;
#endif

	/* Initialize hint structure */
	MemSet(&hint, 0, sizeof(hint));
	hint.ai_family = family;
	hint.ai_flags = AI_PASSIVE;
	hint.ai_socktype = SOCK_STREAM;

	if (family == AF_UNIX)
	{
		/*
		 * Create unixSocketPath from portNumber and unixSocketDir and lock
		 * that file path
		 */
		UNIXSOCK_PATH(unixSocketPath, portNumber, unixSocketDir);
		if (strlen(unixSocketPath) >= UNIXSOCK_PATH_BUFLEN)
		{
			ereport(LOG,
					(errmsg("Unix-domain socket path \"%s\" is too long (maximum %zu bytes)",
							unixSocketPath,
							(UNIXSOCK_PATH_BUFLEN - 1))));
			return STATUS_ERROR;
		}
		if (Lock_AF_UNIX(unixSocketDir, unixSocketPath) != STATUS_OK)
			return STATUS_ERROR;
		service = unixSocketPath;
	}
	else
	{
		snprintf(portNumberStr, sizeof(portNumberStr), "%d", portNumber);
		service = portNumberStr;
	}

	ret = pg_getaddrinfo_all(hostName, service, &hint, &addrs);
	if (ret || !addrs)
	{
		if (hostName)
			ereport(LOG,
					(errmsg("could not translate host name \"%s\", service \"%s\" to address: %s",
							hostName, service, gai_strerror(ret))));
		else
			ereport(LOG,
					(errmsg("could not translate service \"%s\" to address: %s",
							service, gai_strerror(ret))));
		if (addrs)
			pg_freeaddrinfo_all(hint.ai_family, addrs);
		return STATUS_ERROR;
	}

	for (addr = addrs; addr; addr = addr->ai_next)
	{
		if (family != AF_UNIX && addr->ai_family == AF_UNIX)
		{
			/*
			 * Only set up a unix domain socket when they really asked for it.
			 * The service/port is different in that case.
			 */
			continue;
		}

		/* See if there is still room to add 1 more socket. */
		if (*NumListenSockets == MaxListen)
		{
			ereport(LOG,
					(errmsg("could not bind to all requested addresses: MAXLISTEN (%d) exceeded",
							MaxListen)));
			break;
		}

		/* set up address family name for log messages */
		switch (addr->ai_family)
		{
			case AF_INET:
				familyDesc = _("IPv4");
				break;
			case AF_INET6:
				familyDesc = _("IPv6");
				break;
			case AF_UNIX:
				familyDesc = _("Unix");
				break;
			default:
				snprintf(familyDescBuf, sizeof(familyDescBuf),
						 _("unrecognized address family %d"),
						 addr->ai_family);
				familyDesc = familyDescBuf;
				break;
		}

		/* set up text form of address for log messages */
		if (addr->ai_family == AF_UNIX)
			addrDesc = unixSocketPath;
		else
		{
			pg_getnameinfo_all((const struct sockaddr_storage *) addr->ai_addr,
							   addr->ai_addrlen,
							   addrBuf, sizeof(addrBuf),
							   NULL, 0,
							   NI_NUMERICHOST);
			addrDesc = addrBuf;
		}

		if ((fd = socket(addr->ai_family, SOCK_STREAM, 0)) == PGINVALID_SOCKET)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
			/* translator: first %s is IPv4, IPv6, or Unix */
					 errmsg("could not create %s socket for address \"%s\": %m",
							familyDesc, addrDesc)));
			continue;
		}

#ifndef WIN32
		/* Don't give the listen socket to any subprograms we execute. */
		if (fcntl(fd, F_SETFD, FD_CLOEXEC) < 0)
			elog(FATAL, "fcntl(F_SETFD) failed on socket: %m");

		/*
		 * Without the SO_REUSEADDR flag, a new postmaster can't be started
		 * right away after a stop or crash, giving "address already in use"
		 * error on TCP ports.
		 *
		 * On win32, however, this behavior only happens if the
		 * SO_EXCLUSIVEADDRUSE is set. With SO_REUSEADDR, win32 allows
		 * multiple servers to listen on the same address, resulting in
		 * unpredictable behavior. With no flags at all, win32 behaves as Unix
		 * with SO_REUSEADDR.
		 */
		if (addr->ai_family != AF_UNIX)
		{
			if ((setsockopt(fd, SOL_SOCKET, SO_REUSEADDR,
							(char *) &one, sizeof(one))) == -1)
			{
				ereport(LOG,
						(errcode_for_socket_access(),
				/* translator: third %s is IPv4 or IPv6 */
						 errmsg("%s(%s) failed for %s address \"%s\": %m",
								"setsockopt", "SO_REUSEADDR",
								familyDesc, addrDesc)));
				closesocket(fd);
				continue;
			}
		}
#endif

#ifdef IPV6_V6ONLY
		if (addr->ai_family == AF_INET6)
		{
			if (setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY,
						   (char *) &one, sizeof(one)) == -1)
			{
				ereport(LOG,
						(errcode_for_socket_access(),
				/* translator: third %s is IPv6 */
						 errmsg("%s(%s) failed for %s address \"%s\": %m",
								"setsockopt", "IPV6_V6ONLY",
								familyDesc, addrDesc)));
				closesocket(fd);
				continue;
			}
		}
#endif

		/*
		 * Note: This might fail on some OS's, like Linux older than
		 * 2.4.21-pre3, that don't have the IPV6_V6ONLY socket option, and map
		 * ipv4 addresses to ipv6.  It will show ::ffff:ipv4 for all ipv4
		 * connections.
		 */
		err = bind(fd, addr->ai_addr, addr->ai_addrlen);
		if (err < 0)
		{
			int			saved_errno = errno;

			ereport(LOG,
					(errcode_for_socket_access(),
			/* translator: first %s is IPv4, IPv6, or Unix */
					 errmsg("could not bind %s address \"%s\": %m",
							familyDesc, addrDesc),
					 saved_errno == EADDRINUSE ?
					 (addr->ai_family == AF_UNIX ?
					  errhint("Is another postmaster already running on port %d?",
							  portNumber) :
					  errhint("Is another postmaster already running on port %d?"
							  " If not, wait a few seconds and retry.",
							  portNumber)) : 0));
			closesocket(fd);
			continue;
		}

		if (addr->ai_family == AF_UNIX)
		{
			if (Setup_AF_UNIX(service) != STATUS_OK)
			{
				closesocket(fd);
				break;
			}
		}

		/*
		 * Select appropriate accept-queue length limit.  It seems reasonable
		 * to use a value similar to the maximum number of child processes
		 * that the postmaster will permit.
		 */
		maxconn = MaxConnections * 2;

		err = listen(fd, maxconn);
		if (err < 0)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
			/* translator: first %s is IPv4, IPv6, or Unix */
					 errmsg("could not listen on %s address \"%s\": %m",
							familyDesc, addrDesc)));
			closesocket(fd);
			continue;
		}

		if (addr->ai_family == AF_UNIX)
			ereport(LOG,
					(errmsg("listening on Unix socket \"%s\"",
							addrDesc)));
		else
			ereport(LOG,
			/* translator: first %s is IPv4 or IPv6 */
					(errmsg("listening on %s address \"%s\", port %d",
							familyDesc, addrDesc, portNumber)));

		ListenSockets[*NumListenSockets] = fd;
		(*NumListenSockets)++;
		added++;
	}

	pg_freeaddrinfo_all(hint.ai_family, addrs);

	if (!added)
		return STATUS_ERROR;

	return STATUS_OK;
}


/*
 * Lock_AF_UNIX -- configure unix socket file path
 */
static int
Lock_AF_UNIX(const char *unixSocketDir, const char *unixSocketPath)
{
	/* no lock file for abstract sockets */
	if (unixSocketPath[0] == '@')
		return STATUS_OK;

	/*
	 * Grab an interlock file associated with the socket file.
	 *
	 * Note: there are two reasons for using a socket lock file, rather than
	 * trying to interlock directly on the socket itself.  First, it's a lot
	 * more portable, and second, it lets us remove any pre-existing socket
	 * file without race conditions.
	 */
	CreateSocketLockFile(unixSocketPath, true, unixSocketDir);

	/*
	 * Once we have the interlock, we can safely delete any pre-existing
	 * socket file to avoid failure at bind() time.
	 */
	(void) unlink(unixSocketPath);

	/*
	 * Remember socket file pathnames for later maintenance.
	 */
	sock_paths = lappend(sock_paths, pstrdup(unixSocketPath));

	return STATUS_OK;
}


/*
 * Setup_AF_UNIX -- configure unix socket permissions
 */
static int
Setup_AF_UNIX(const char *sock_path)
{
	/* no file system permissions for abstract sockets */
	if (sock_path[0] == '@')
		return STATUS_OK;

	/*
	 * Fix socket ownership/permission if requested.  Note we must do this
	 * before we listen() to avoid a window where unwanted connections could
	 * get accepted.
	 */
	Assert(Unix_socket_group);
	if (Unix_socket_group[0] != '\0')
	{
#ifdef WIN32
		elog(WARNING, "configuration item \"unix_socket_group\" is not supported on this platform");
#else
		char	   *endptr;
		unsigned long val;
		gid_t		gid;

		val = strtoul(Unix_socket_group, &endptr, 10);
		if (*endptr == '\0')
		{						/* numeric group id */
			gid = val;
		}
		else
		{						/* convert group name to id */
			struct group *gr;

			gr = getgrnam(Unix_socket_group);
			if (!gr)
			{
				ereport(LOG,
						(errmsg("group \"%s\" does not exist",
								Unix_socket_group)));
				return STATUS_ERROR;
			}
			gid = gr->gr_gid;
		}
		if (chown(sock_path, -1, gid) == -1)
		{
			ereport(LOG,
					(errcode_for_file_access(),
					 errmsg("could not set group of file \"%s\": %m",
							sock_path)));
			return STATUS_ERROR;
		}
#endif
	}

	if (chmod(sock_path, Unix_socket_permissions) == -1)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not set permissions of file \"%s\": %m",
						sock_path)));
		return STATUS_ERROR;
	}
	return STATUS_OK;
}


/*
 * AcceptConnection -- accept a new connection with client using
 *		server port.  Fills *client_sock with the FD and endpoint info
 *		of the new connection.
 *
 * ASSUME: that this doesn't need to be non-blocking because
 *		the Postmaster waits for the socket to be ready to accept().
 *
 * RETURNS: STATUS_OK or STATUS_ERROR
 */
int
AcceptConnection(pgsocket server_fd, ClientSocket *client_sock)
{
	/* accept connection and fill in the client (remote) address */
	client_sock->ssl_negotiated = false;
	client_sock->raddr.salen = sizeof(client_sock->raddr.addr);
	if ((client_sock->sock = accept(server_fd,
									(struct sockaddr *) &client_sock->raddr.addr,
									&client_sock->raddr.salen)) == PGINVALID_SOCKET)
	{
		ereport(LOG,
				(errcode_for_socket_access(),
				 errmsg("could not accept new connection: %m")));

		/*
		 * If accept() fails then postmaster.c will still see the server
		 * socket as read-ready, and will immediately try again.  To avoid
		 * uselessly sucking lots of CPU, delay a bit before trying again.
		 * (The most likely reason for failure is being out of kernel file
		 * table slots; we can do little except hope some will get freed up.)
		 */
		pg_usleep(100000L);		/* wait 0.1 sec */
		return STATUS_ERROR;
	}

	return STATUS_OK;
}

/*
 * pg_prenegotiate_ssl_request -- answer a client's SSLRequest at accept time,
 * in the postmaster, WITHOUT ever blocking on the client.
 *
 * Threaded mode emits the SSLRequest 'N' (no-SSL) negotiation byte from the
 * backend FIBER after handoff; under a large simultaneous connect burst on a
 * many-core box, scheduling that fiber across the executor loops is slow enough
 * that some clients' connect-time read of the 1-byte reply times out and they
 * abort ("server sent an error response during SSL exchange").  Answering the
 * 'N' here, at accept time, is scheduling-independent and eliminates that race.
 *
 * SAFETY (why the postmaster can do this without risk):
 *  - MSG_PEEK|MSG_DONTWAIT: a SINGLE non-blocking peek of up to 8 bytes.  If
 *    fewer than 8 are available (slow client) it does NOTHING and returns
 *    false -- the postmaster never blocks, never loops, never waits on data.
 *  - It consumes + answers ONLY an exact 8-byte SSLRequest (len==8 &&
 *    code==NEGOTIATE_SSL_CODE); a GSSRequest, a plain startup packet, a cancel
 *    request, or anything else is left untouched for the fiber to handle.
 *  - The reply is a single non-blocking send of 1 byte on a socket whose send
 *    buffer is empty, so it cannot block either.
 *  - No new MITM surface: this is exactly the plaintext SSLRequest->'N' exchange
 *    PostgreSQL already performs; it only moves WHERE the 'N' is written.  The
 *    caller gates it on multithreaded && !LoadedSSL (SSL off) && TCP, so when
 *    SSL is enabled the fiber still runs the real negotiation/secure_open_server.
 *
 * On success (SSLRequest consumed + 'N' sent) sets sock->ssl_negotiated = true
 * and returns true; the client then sends its real startup packet, which the
 * fiber's ProcessStartupPacket reads (ssl_done initialized from the flag).
 */
bool
pg_prenegotiate_ssl_request(ClientSocket *sock)
{
#ifndef WIN32
	unsigned char	buf[8];
	ssize_t			n;
	uint32			len;
	uint32			code;
	char			reply = 'N';

	sock->ssl_negotiated = false;

	/* Non-blocking PEEK: inspect without consuming; never wait. */
	n = recv(sock->sock, buf, sizeof(buf), MSG_PEEK | MSG_DONTWAIT);
	if (n != (ssize_t) sizeof(buf))
		return false;			/* <8 bytes ready, or error -> leave to fiber */

	memcpy(&len, buf, 4);
	memcpy(&code, buf + 4, 4);
	len = pg_ntoh32(len);
	code = pg_ntoh32(code);
	if (len != 8 || code != (uint32) NEGOTIATE_SSL_CODE)
		return false;			/* not an SSLRequest -> leave to fiber */

	/* It IS an 8-byte SSLRequest.  Consume exactly those 8 bytes. */
	n = recv(sock->sock, buf, sizeof(buf), MSG_DONTWAIT);
	if (n != (ssize_t) sizeof(buf))
		return false;			/* raced away; fiber will re-read */

	/* Answer 'N' (no SSL).  Send buffer is empty; a 1-byte send cannot block. */
	do
		n = send(sock->sock, &reply, 1, MSG_DONTWAIT);
	while (n < 0 && errno == EINTR);
	if (n != 1)
		return false;			/* could not send -> fiber path (rare) */

	sock->ssl_negotiated = true;
	return true;
#else
	sock->ssl_negotiated = false;
	return false;
#endif
}

/*
 * AcceptConnectionDrain -- accept up to `max` pending connections in one call,
 * without blocking after the backlog is drained.
 *
 * The threaded postmaster processes the SSLRequest/startup handshake in the
 * backend fiber AFTER handoff, and its ServerLoop otherwise accepts only one
 * connection per iteration.  Under a large SIMULTANEOUS connection burst that
 * serialization makes many clients wait for the 'N' negotiation byte longer
 * than their connect timeout, and they abort ("error response during SSL
 * exchange").  Draining the accept backlog in a bounded batch admits a burst
 * quickly so the handshakes are not serialized behind the whole ServerLoop.
 *
 * The caller passes an already-accept-ready listen fd (WL_SOCKET_ACCEPT fired).
 * We accept it, then temporarily set the listen fd non-blocking and keep
 * accepting until EAGAIN/EWOULDBLOCK or the batch cap, then restore blocking
 * mode.  Each accepted socket is handed to `cb` immediately (mirrors the
 * one-per-iteration path so slot/fiber launch happens per connection).  Returns
 * the number of connections accepted.
 *
 * Non-blocking is applied to the LISTEN fd only for the duration of the drain.
 * Fork mode does not use this path, so its one-accept-per-iteration behavior is
 * unchanged.
 */
int
AcceptConnectionDrain(pgsocket server_fd, int max,
					  void (*cb) (ClientSocket *sock))
{
	int			accepted = 0;
	bool		set_nonblock = false;

	while (accepted < max)
	{
		ClientSocket s;

		s.ssl_negotiated = false;
		s.raddr.salen = sizeof(s.raddr.addr);
		s.sock = accept(server_fd,
						(struct sockaddr *) &s.raddr.addr,
						&s.raddr.salen);
		if (s.sock == PGINVALID_SOCKET)
		{
			if (errno == EAGAIN || errno == EWOULDBLOCK)
				break;			/* backlog drained */
			if (errno == EINTR)
				continue;
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("could not accept new connection: %m")));
			pg_usleep(100000L);	/* back off briefly on a real error */
			break;
		}

		/*
		 * After the first successful accept, switch the listen fd non-blocking
		 * so the next accept() returns EAGAIN instead of blocking once the
		 * backlog is empty.
		 */
		if (!set_nonblock && accepted == 0 && max > 1)
		{
			if (pg_set_noblock(server_fd))
				set_nonblock = true;
		}

		accepted++;
		cb(&s);
		if (s.sock != PGINVALID_SOCKET)
		{
			if (closesocket(s.sock) != 0)
				elog(LOG, "could not close client socket: %m");
		}

		if (!set_nonblock)
			break;				/* single-accept mode (max==1 or set failed) */
	}

	if (set_nonblock)
		(void) pg_set_block(server_fd);	/* restore blocking accept */

	return accepted;
}

/*
 * TouchSocketFiles -- mark socket files as recently accessed
 *
 * This routine should be called every so often to ensure that the socket
 * files have a recent mod date (ordinary operations on sockets usually won't
 * change the mod date).  That saves them from being removed by
 * overenthusiastic /tmp-directory-cleaner daemons.  (Another reason we should
 * never have put the socket file in /tmp...)
 */
void
TouchSocketFiles(void)
{
	ListCell   *l;

	/* Loop through all created sockets... */
	foreach(l, sock_paths)
	{
		char	   *sock_path = (char *) lfirst(l);

		/* Ignore errors; there's no point in complaining */
		(void) utime(sock_path, NULL);
	}
}

/*
 * RemoveSocketFiles -- unlink socket files at postmaster shutdown
 */
void
RemoveSocketFiles(void)
{
	ListCell   *l;

	/* Loop through all created sockets... */
	foreach(l, sock_paths)
	{
		char	   *sock_path = (char *) lfirst(l);

		/* Ignore any error. */
		(void) unlink(sock_path);
	}
	/* Since we're about to exit, no need to reclaim storage */
}


/* --------------------------------
 * Low-level I/O routines begin here.
 *
 * These routines communicate with a frontend client across a connection
 * already established by the preceding routines.
 * --------------------------------
 */

/* --------------------------------
 *			  socket_set_nonblocking - set socket blocking/non-blocking
 *
 * Sets the socket non-blocking if nonblocking is true, or sets it
 * blocking otherwise.
 * --------------------------------
 */
static void
socket_set_nonblocking(bool nonblocking)
{
	if (MyProcPort == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_DOES_NOT_EXIST),
				 errmsg("there is no client connection")));

	MyProcPort->noblock = nonblocking;
}

/* --------------------------------
 *		pq_recvbuf - load some bytes into the input buffer
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
static int
pq_recvbuf(PgConnectionSocketIOState *io)
{
	pq_ensure_recv_buffer(io);

	if (io->recv_pointer > 0)
	{
		if (io->recv_length > io->recv_pointer)
		{
			/* still some unread data, left-justify it in the buffer */
			memmove(io->recv_buffer, io->recv_buffer + io->recv_pointer,
					io->recv_length - io->recv_pointer);
			io->recv_length -= io->recv_pointer;
			io->recv_pointer = 0;
		}
		else
			io->recv_length = io->recv_pointer = 0;
	}

	/* Ensure that we're in blocking mode */
	socket_set_nonblocking(false);

	/* Can fill buffer from io->recv_length and upwards */
	for (;;)
	{
		ssize_t		r;

		errno = 0;

		r = secure_read(MyProcPort, io->recv_buffer + io->recv_length,
						PQ_RECV_BUFFER_SIZE - io->recv_length);

		if (r < 0)
		{
			if (errno == EINTR)
				continue;		/* Ok if interrupted */

			/*
			 * Careful: an ereport() that tries to write to the client would
			 * cause recursion to here, leading to stack overflow and core
			 * dump!  This message must go *only* to the postmaster log.
			 *
			 * If errno is zero, assume it's EOF and let the caller complain.
			 */
			if (errno != 0)
				ereport(COMMERROR,
						(errcode_for_socket_access(),
						 errmsg("could not receive data from client: %m")));
			return EOF;
		}
		if (r == 0)
		{
			/*
			 * EOF detected.  We used to write a log message here, but it's
			 * better to expect the ultimate caller to do that.
			 */
			return EOF;
		}
		/* r contains number of bytes read, so just incr length */
		io->recv_length += r;
		io->transport_generation++;
		return 0;
	}
}

/* --------------------------------
 *		pq_getbyte	- get a single byte from connection, or return EOF
 * --------------------------------
 */
int
pq_getbyte(void)
{
	PgConnectionSocketIOState *io = PqSocketIO();

	return pq_getbyte_from(io);
}

static inline int
pq_getbyte_from(PgConnectionSocketIOState *io)
{

	Assert(io->comm_reading_msg);

	while (io->recv_pointer >= io->recv_length)
	{
		if (pq_recvbuf(io))		/* If nothing in buffer, then recv some */
			return EOF;			/* Failed to recv data */
	}
	{
		unsigned char c = (unsigned char) io->recv_buffer[io->recv_pointer];

		pq_advance_recv_pointer(io, 1);
		return c;
	}
}

/* --------------------------------
 *		pq_peekbyte		- peek at next byte from connection
 *
 *	 Same as pq_getbyte() except we don't advance the pointer.
 * --------------------------------
 */
int
pq_peekbyte(void)
{
	PgConnectionSocketIOState *io = PqSocketIO();

	Assert(io->comm_reading_msg);

	while (io->recv_pointer >= io->recv_length)
	{
		if (pq_recvbuf(io))		/* If nothing in buffer, then recv some */
			return EOF;			/* Failed to recv data */
	}
	return (unsigned char) io->recv_buffer[io->recv_pointer];
}

/* --------------------------------
 *		pq_getbyte_if_available - get a single byte from connection,
 *			if available
 *
 * The received byte is stored in *c. Returns 1 if a byte was read,
 * 0 if no data was available, or EOF if trouble.
 * --------------------------------
 */
int
pq_getbyte_if_available(unsigned char *c)
{
	PgConnectionSocketIOState *io = PqSocketIO();
	ssize_t		r;

	Assert(io->comm_reading_msg);

	if (io->recv_pointer < io->recv_length)
	{
		*c = io->recv_buffer[io->recv_pointer];
		pq_advance_recv_pointer(io, 1);
		return 1;
	}

	/* Put the socket into non-blocking mode */
	socket_set_nonblocking(true);

	errno = 0;

	r = secure_read(MyProcPort, c, 1);
	if (r < 0)
	{
		/*
		 * Ok if no data available without blocking or interrupted (though
		 * EINTR really shouldn't happen with a non-blocking socket). Report
		 * other errors.
		 */
		if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
			r = 0;
		else
		{
			/*
			 * Careful: an ereport() that tries to write to the client would
			 * cause recursion to here, leading to stack overflow and core
			 * dump!  This message must go *only* to the postmaster log.
			 *
			 * If errno is zero, assume it's EOF and let the caller complain.
			 */
			if (errno != 0)
				ereport(COMMERROR,
						(errcode_for_socket_access(),
						 errmsg("could not receive data from client: %m")));
			r = EOF;
		}
	}
	else if (r == 0)
	{
		/* EOF detected */
		r = EOF;
	}
	else
		io->transport_generation++;

	return r;
}

/* --------------------------------
 *		pq_getbytes		- get a known number of bytes from connection
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int
pq_getbytes(void *b, size_t len)
{
	PgConnectionSocketIOState *io = PqSocketIO();

	return pq_getbytes_from(io, b, len);
}

static inline int
pq_getbytes_from(PgConnectionSocketIOState *io, void *b, size_t len)
{
	char	   *s = b;
	size_t		amount;

	Assert(io->comm_reading_msg);

	while (len > 0)
	{
		while (io->recv_pointer >= io->recv_length)
		{
			if (pq_recvbuf(io)) /* If nothing in buffer, then recv some */
				return EOF;		/* Failed to recv data */
		}
		amount = io->recv_length - io->recv_pointer;
		if (amount > len)
			amount = len;
		memcpy(s, io->recv_buffer + io->recv_pointer, amount);
		pq_advance_recv_pointer(io, amount);
		s += amount;
		len -= amount;
	}
	return 0;
}

static int
pq_discardbytes_from(PgConnectionSocketIOState *io, size_t len)
{
	size_t		amount;

	Assert(io->comm_reading_msg);

	while (len > 0)
	{
		while (io->recv_pointer >= io->recv_length)
		{
			if (pq_recvbuf(io)) /* If nothing in buffer, then recv some */
				return EOF;		/* Failed to recv data */
		}
		amount = io->recv_length - io->recv_pointer;
		if (amount > len)
			amount = len;
		pq_advance_recv_pointer(io, amount);
		len -= amount;
	}
	return 0;
}

/* --------------------------------
 *		pq_buffer_remaining_data	- return number of bytes in receive buffer
 *
 * This will *not* attempt to read more data. And reading up to that number of
 * bytes should not cause reading any more data either.
 * --------------------------------
 */
ssize_t
pq_buffer_remaining_data(void)
{
	PgConnectionSocketIOState *io = PqSocketIO();

	Assert(io->recv_length >= io->recv_pointer);
	return (io->recv_length - io->recv_pointer);
}

static int
pq_probe_recvbuf(PgConnection *connection)
{
	PgConnectionSocketIOState *io;
	Port	   *port;
	size_t		old_recv_pointer;
	size_t		old_recv_length;
	bool		saved_noblock;
	int			r;

	Assert(connection != NULL);
	io = &connection->socket_io;
	port = connection->identity.port;
	Assert(port != NULL);
	Assert(io->recv_pointer >= io->recv_length);

	old_recv_pointer = io->recv_pointer;
	old_recv_length = io->recv_length;

	pq_ensure_recv_buffer(io);
	io->recv_pointer = 0;
	io->recv_length = 0;

	saved_noblock = port->noblock;
	port->noblock = true;

	errno = 0;
	r = secure_read(port, io->recv_buffer, PQ_RECV_BUFFER_SIZE);
	port->noblock = saved_noblock;
	if (r < 0)
	{
		if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
		{
			io->recv_pointer = old_recv_pointer;
			io->recv_length = old_recv_length;
			return 0;
		}

		if (errno != 0)
			ereport(COMMERROR,
					(errcode_for_socket_access(),
					 errmsg("could not receive data from client: %m")));
		io->recv_pointer = old_recv_pointer;
		io->recv_length = old_recv_length;
		return EOF;
	}
	if (r == 0)
	{
		io->recv_pointer = old_recv_pointer;
		io->recv_length = old_recv_length;
		return EOF;
	}

	io->recv_length = r;
	io->transport_generation++;
	return 1;
}


/* --------------------------------
 *		pq_startmsgread - begin reading a message from the client.
 *
 *		This must be called before any of the pq_get* functions.
 * --------------------------------
 */
void
pq_startmsgread(void)
{
	PgConnectionSocketIOState *io = PqSocketIO();

	/*
	 * There shouldn't be a read active already, but let's check just to be
	 * sure.
	 */
	if (io->comm_reading_msg)
		ereport(FATAL,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("terminating connection because protocol synchronization was lost")));

	io->comm_reading_msg = true;
}

/*
 * Begin reading a frontend message and return its message type byte.
 */
int
pq_startmsgread_getbyte(void)
{
	PgConnectionSocketIOState *io = PqSocketIO();

	/*
	 * There shouldn't be a read active already, but let's check just to be
	 * sure.
	 */
	if (io->comm_reading_msg)
		ereport(FATAL,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("terminating connection because protocol synchronization was lost")));

	io->comm_reading_msg = true;
	return pq_getbyte_from(io);
}

bool
PgConnectionCanParkBeforeMessage(PgConnection *connection)
{
	if (connection == NULL)
		return false;

	return !connection->socket_io.comm_reading_msg;
}

void
PgConnectionReleaseIdleRecvBuffer(PgConnection *connection)
{
	PgConnectionSocketIOState *io;

	if (connection == NULL)
		return;

	io = &connection->socket_io;
	if (io->recv_buffer == NULL)
		return;
	if (io->comm_reading_msg)
		return;
	if (io->recv_pointer < io->recv_length)
		return;

	pfree(io->recv_buffer);
	io->recv_buffer = NULL;
	io->recv_pointer = 0;
	io->recv_length = 0;
}

static PgProtocolByteResult
pq_probe_message_type(PgConnection *connection, PgProtocolByteProbe *probe,
					  bool probe_kernel)
{
	PgConnectionSocketIOState *io;
	Port	   *port;
	unsigned char c;
	bool		buffered_input;
	uint32		wait_events;
	int			r;

	Assert(connection != NULL);

	io = &connection->socket_io;
	if (probe != NULL)
		MemSet(probe, 0, sizeof(*probe));

	if (io->comm_reading_msg)
		ereport(FATAL,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("terminating connection because protocol synchronization was lost")));

	buffered_input = pq_connection_transport_buffered_input(connection);
	wait_events = pq_connection_transport_wait_events(connection);
	if (probe != NULL)
	{
		probe->transport_wait_events = wait_events;
		probe->transport_buffered_input = buffered_input;
		probe->transport_generation = io->transport_generation;
	}

	if (io->recv_pointer < io->recv_length)
	{
		c = (unsigned char) io->recv_buffer[io->recv_pointer];
		pq_advance_recv_pointer(io, 1);
		io->comm_reading_msg = true;
		if (probe != NULL)
		{
			probe->type = c;
			probe->transport_wait_events = 0;
			probe->transport_buffered_input = true;
			probe->transport_generation = io->transport_generation;
		}
		return PG_PROTOCOL_BYTE_AVAILABLE;
	}

	port = connection->identity.port;
	if (port == NULL)
		return PG_PROTOCOL_BYTE_EOF;

	if (!probe_kernel && !buffered_input)
		return PG_PROTOCOL_BYTE_NONE;

	if (wait_events == 0 && !buffered_input)
		return PG_PROTOCOL_BYTE_NONE;

	r = pq_probe_recvbuf(connection);
	if (r == 0)
		return PG_PROTOCOL_BYTE_NONE;
	if (r == EOF)
		return PG_PROTOCOL_BYTE_EOF;

	c = (unsigned char) io->recv_buffer[io->recv_pointer];
	pq_advance_recv_pointer(io, 1);
	io->comm_reading_msg = true;
	if (probe != NULL)
	{
		probe->type = c;
		probe->transport_wait_events = 0;
		probe->transport_buffered_input = buffered_input;
		probe->transport_generation = io->transport_generation;
	}
	return PG_PROTOCOL_BYTE_AVAILABLE;
}

PgProtocolByteResult
PgConnectionProbeBufferedMessageType(PgConnection *connection,
									 PgProtocolByteProbe *probe)
{
	return pq_probe_message_type(connection, probe, false);
}

PgProtocolByteResult
PgConnectionProbeMessageType(PgConnection *connection,
							 PgProtocolByteProbe *probe)
{
	return pq_probe_message_type(connection, probe, true);
}


/* --------------------------------
 *		pq_endmsgread	- finish reading message.
 *
 *		This must be called after reading a message with pq_getbytes()
 *		and friends, to indicate that we have read the whole message.
 *		pq_getmessage() does this implicitly.
 * --------------------------------
 */
void
pq_endmsgread(void)
{
	PgConnectionSocketIOState *io = PqSocketIO();

	Assert(io->comm_reading_msg);

	io->comm_reading_msg = false;
}

/* --------------------------------
 *		pq_is_reading_msg - are we currently reading a message?
 *
 * This is used in error recovery at the outer idle loop to detect if we have
 * lost protocol sync, and need to terminate the connection. pq_startmsgread()
 * will check for that too, but it's nicer to detect it earlier.
 * --------------------------------
 */
bool
pq_is_reading_msg(void)
{
	PgConnectionSocketIOState *io = PqSocketIO();

	return io->comm_reading_msg;
}

/* --------------------------------
 *		pq_getmessage	- get a message with length word from connection
 *
 *		The return value is placed in an expansible StringInfo, which has
 *		already been initialized by the caller.
 *		Only the message body is placed in the StringInfo; the length word
 *		is removed.  Also, s->cursor is initialized to zero for convenience
 *		in scanning the message contents.
 *
 *		maxlen is the upper limit on the length of the
 *		message we are willing to accept.  We abort the connection (by
 *		returning EOF) if client tries to send more than that.
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int
pq_getmessage(StringInfo s, int maxlen)
{
	PgConnectionSocketIOState *io = PqSocketIO();
	int32		len;

	Assert(io->comm_reading_msg);

	resetStringInfo(s);

	/* Read message length word */
	if (pq_getbytes_from(io, &len, 4) == EOF)
	{
		ereport(COMMERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("unexpected EOF within message length word")));
		return EOF;
	}

	len = pg_ntoh32(len);

	if (len < 4 || len > maxlen)
	{
		ereport(COMMERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid message length")));
		return EOF;
	}

	len -= 4;					/* discount length itself */

	if (len > 0)
	{
		/*
		 * Allocate space for message.  If we run out of room (ridiculously
		 * large message), we will elog(ERROR), but we want to discard the
		 * message body so as not to lose communication sync.
		 */
		PG_TRY();
		{
			enlargeStringInfo(s, len);
		}
		PG_CATCH();
		{
			if (pq_discardbytes_from(io, len) == EOF)
				ereport(COMMERROR,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("incomplete message from client")));

			/* we discarded the rest of the message so we're back in sync. */
			io->comm_reading_msg = false;
			PG_RE_THROW();
		}
		PG_END_TRY();

		/* And grab the message */
		if (pq_getbytes_from(io, s->data, len) == EOF)
		{
			ereport(COMMERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("incomplete message from client")));
			return EOF;
		}
		s->len = len;
		/* Place a trailing null per StringInfo convention */
		s->data[len] = '\0';
	}

	/* finished reading the message. */
	io->comm_reading_msg = false;

	return 0;
}


static inline int
internal_putbytes(PgConnectionSocketIOState *io, const void *b, size_t len)
{
	const char *s = b;

	while (len > 0)
	{
		/* If buffer is full, then flush it out */
		if (io->send_pointer >= io->send_buffer_size)
		{
			socket_set_nonblocking(false);
			if (internal_flush(io))
				return EOF;
		}

		/*
		 * If the buffer is empty and data length is larger than the buffer
		 * size, send it without buffering.  Otherwise, copy as much data as
		 * possible into the buffer.
		 */
		if (len >= io->send_buffer_size &&
			io->send_start == io->send_pointer)
		{
			size_t		start = 0;

			socket_set_nonblocking(false);
			if (internal_flush_buffer(s, &start, &len))
				return EOF;
		}
		else
		{
			size_t		amount = io->send_buffer_size - io->send_pointer;

			if (amount > len)
				amount = len;
			memcpy(io->send_buffer + io->send_pointer, s, amount);
			io->send_pointer += amount;
			s += amount;
			len -= amount;
		}
	}

	return 0;
}

/* --------------------------------
 *		socket_flush		- flush pending output
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
static int
socket_flush(void)
{
	PgConnectionSocketIOState *io = PqSocketIO();
	int			res;

	/* No-op if reentrant call */
	if (io->comm_busy)
		return 0;
	io->comm_busy = true;
	socket_set_nonblocking(false);
	res = internal_flush(io);
	io->comm_busy = false;
	return res;
}

/* --------------------------------
 *		internal_flush - flush pending output
 *
 * Returns 0 if OK (meaning everything was sent, or operation would block
 * and the socket is in non-blocking mode), or EOF if trouble.
 * --------------------------------
 */
static inline int
internal_flush(PgConnectionSocketIOState *io)
{
	return internal_flush_buffer(io->send_buffer, &io->send_start,
								 &io->send_pointer);
}

/* --------------------------------
 *		internal_flush_buffer - flush the given buffer content
 *
 * Returns 0 if OK (meaning everything was sent, or operation would block
 * and the socket is in non-blocking mode), or EOF if trouble.
 * --------------------------------
 */
static pg_noinline int
internal_flush_buffer(const char *buf, size_t *start, size_t *end)
{
	static int	last_reported_send_errno = 0;

	const char *bufptr = buf + *start;
	const char *bufend = buf + *end;

	while (bufptr < bufend)
	{
		ssize_t		r;

		r = secure_write(MyProcPort, bufptr, bufend - bufptr);

		if (r <= 0)
		{
			if (errno == EINTR)
				continue;		/* Ok if we were interrupted */

			/*
			 * Ok if no data writable without blocking, and the socket is in
			 * non-blocking mode.
			 */
			if (errno == EAGAIN ||
				errno == EWOULDBLOCK)
			{
				return 0;
			}

			/*
			 * Careful: an ereport() that tries to write to the client would
			 * cause recursion to here, leading to stack overflow and core
			 * dump!  This message must go *only* to the postmaster log.
			 *
			 * If a client disconnects while we're in the midst of output, we
			 * might write quite a bit of data before we get to a safe query
			 * abort point.  So, suppress duplicate log messages.
			 */
			if (errno != last_reported_send_errno)
			{
				last_reported_send_errno = errno;
				ereport(COMMERROR,
						(errcode_for_socket_access(),
						 errmsg("could not send data to client: %m")));
			}

			/*
			 * We drop the buffered data anyway so that processing can
			 * continue, even though we'll probably quit soon. We also set a
			 * flag that'll cause the next CHECK_FOR_INTERRUPTS to terminate
			 * the connection.
			 */
			*start = *end = 0;
			ClientConnectionLost = 1;
			InterruptPending = 1;
			return EOF;
		}

		last_reported_send_errno = 0;	/* reset after any successful send */
		bufptr += r;
		*start += r;
	}

	*start = *end = 0;
	return 0;
}

/* --------------------------------
 *		pq_flush_if_writable - flush pending output if writable without blocking
 *
 * Returns 0 if OK, or EOF if trouble.
 * --------------------------------
 */
static int
socket_flush_if_writable(void)
{
	PgConnectionSocketIOState *io = PqSocketIO();
	int			res;

	/* Quick exit if nothing to do */
	if (io->send_pointer == io->send_start)
		return 0;

	/* No-op if reentrant call */
	if (io->comm_busy)
		return 0;

	/* Temporarily put the socket into non-blocking mode */
	socket_set_nonblocking(true);

	io->comm_busy = true;
	res = internal_flush(io);
	io->comm_busy = false;
	return res;
}

/* --------------------------------
 *	socket_is_send_pending	- is there any pending data in the output buffer?
 * --------------------------------
 */
static bool
socket_is_send_pending(void)
{
	PgConnectionSocketIOState *io = PqSocketIO();

	return (io->send_start < io->send_pointer);
}

/* --------------------------------
 * Message-level I/O routines begin here.
 * --------------------------------
 */


/* --------------------------------
 *		socket_putmessage - send a normal message (suppressed in COPY OUT mode)
 *
 *		msgtype is a message type code to place before the message body.
 *
 *		len is the length of the message body data at *s.  A message length
 *		word (equal to len+4 because it counts itself too) is inserted by this
 *		routine.
 *
 *		We suppress messages generated while pqcomm.c is busy.  This
 *		avoids any possibility of messages being inserted within other
 *		messages.  The only known trouble case arises if SIGQUIT occurs
 *		during a pqcomm.c routine --- quickdie() will try to send a warning
 *		message, and the most reasonable approach seems to be to drop it.
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
static int
socket_putmessage(char msgtype, const char *s, size_t len)
{
	PgConnectionSocketIOState *io = PqSocketIO();
	uint32		n32;

	Assert(msgtype != 0);

	if (io->comm_busy)
		return 0;
	io->comm_busy = true;
	if (internal_putbytes(io, &msgtype, 1))
		goto fail;

	n32 = pg_hton32((uint32) (len + 4));
	if (internal_putbytes(io, &n32, 4))
		goto fail;

	if (internal_putbytes(io, s, len))
		goto fail;
	io->comm_busy = false;
	return 0;

fail:
	io->comm_busy = false;
	return EOF;
}

/* --------------------------------
 *		pq_putmessage_noblock	- like pq_putmessage, but never blocks
 *
 *		If the output buffer is too small to hold the message, the buffer
 *		is enlarged.
 */
static void
socket_putmessage_noblock(char msgtype, const char *s, size_t len)
{
	PgConnectionSocketIOState *io = PqSocketIO();
	int			res PG_USED_FOR_ASSERTS_ONLY;
	int			required;

	/*
	 * Ensure we have enough space in the output buffer for the message header
	 * as well as the message itself.
	 */
	required = io->send_pointer + 1 + 4 + len;
	if (required > io->send_buffer_size)
	{
		io->send_buffer = repalloc(io->send_buffer, required);
		io->send_buffer_size = required;
	}
	res = socket_putmessage(msgtype, s, len);
	Assert(res == 0);			/* should not fail when the message fits in
								 * buffer */
}

/* --------------------------------
 *		pq_putmessage_v2 - send a message in protocol version 2
 *
 *		msgtype is a message type code to place before the message body.
 *
 *		We no longer support protocol version 2, but we have kept this
 *		function so that if a client tries to connect with protocol version 2,
 *		as a courtesy we can still send the "unsupported protocol version"
 *		error to the client in the old format.
 *
 *		Like in pq_putmessage(), we suppress messages generated while
 *		pqcomm.c is busy.
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int
pq_putmessage_v2(char msgtype, const char *s, size_t len)
{
	PgConnectionSocketIOState *io = PqSocketIO();

	Assert(msgtype != 0);

	if (io->comm_busy)
		return 0;
	io->comm_busy = true;
	if (internal_putbytes(io, &msgtype, 1))
		goto fail;

	if (internal_putbytes(io, s, len))
		goto fail;
	io->comm_busy = false;
	return 0;

fail:
	io->comm_busy = false;
	return EOF;
}

/*
 * Support for TCP Keepalive parameters
 */

/*
 * On Windows, we need to set both idle and interval at the same time.
 * We also cannot reset them to the default (setting to zero will
 * actually set them to zero, not default), therefore we fallback to
 * the out-of-the-box default instead.
 */
#if defined(WIN32) && defined(SIO_KEEPALIVE_VALS)
static int
pq_setkeepaliveswin32(Port *port, int idle, int interval)
{
	struct tcp_keepalive ka;
	DWORD		retsize;

	if (idle <= 0)
		idle = 2 * 60 * 60;		/* default = 2 hours */
	if (interval <= 0)
		interval = 1;			/* default = 1 second */

	ka.onoff = 1;
	ka.keepalivetime = idle * 1000;
	ka.keepaliveinterval = interval * 1000;

	if (WSAIoctl(port->sock,
				 SIO_KEEPALIVE_VALS,
				 (LPVOID) &ka,
				 sizeof(ka),
				 NULL,
				 0,
				 &retsize,
				 NULL,
				 NULL)
		!= 0)
	{
		ereport(LOG,
				(errmsg("%s(%s) failed: error code %d",
						"WSAIoctl", "SIO_KEEPALIVE_VALS", WSAGetLastError())));
		return STATUS_ERROR;
	}
	if (port->keepalives_idle != idle)
		port->keepalives_idle = idle;
	if (port->keepalives_interval != interval)
		port->keepalives_interval = interval;
	return STATUS_OK;
}
#endif

int
pq_getkeepalivesidle(Port *port)
{
#if defined(PG_TCP_KEEPALIVE_IDLE) || defined(SIO_KEEPALIVE_VALS)
	if (port == NULL || port->laddr.addr.ss_family == AF_UNIX)
		return 0;

	if (port->keepalives_idle != 0)
		return port->keepalives_idle;

	if (port->default_keepalives_idle == 0)
	{
#ifndef WIN32
		socklen_t	size = sizeof(port->default_keepalives_idle);

		if (getsockopt(port->sock, IPPROTO_TCP, PG_TCP_KEEPALIVE_IDLE,
					   (char *) &port->default_keepalives_idle,
					   &size) < 0)
		{
			ereport(LOG,
					(errmsg("%s(%s) failed: %m", "getsockopt", PG_TCP_KEEPALIVE_IDLE_STR)));
			port->default_keepalives_idle = -1; /* don't know */
		}
#else							/* WIN32 */
		/* We can't get the defaults on Windows, so return "don't know" */
		port->default_keepalives_idle = -1;
#endif							/* WIN32 */
	}

	return port->default_keepalives_idle;
#else
	return 0;
#endif
}

int
pq_setkeepalivesidle(int idle, Port *port)
{
	if (port == NULL || port->laddr.addr.ss_family == AF_UNIX)
		return STATUS_OK;

/* check SIO_KEEPALIVE_VALS here, not just WIN32, as some toolchains lack it */
#if defined(PG_TCP_KEEPALIVE_IDLE) || defined(SIO_KEEPALIVE_VALS)
	if (idle == port->keepalives_idle)
		return STATUS_OK;

#ifndef WIN32
	if (port->default_keepalives_idle <= 0)
	{
		if (pq_getkeepalivesidle(port) < 0)
		{
			if (idle == 0)
				return STATUS_OK;	/* default is set but unknown */
			else
				return STATUS_ERROR;
		}
	}

	if (idle == 0)
		idle = port->default_keepalives_idle;

	if (setsockopt(port->sock, IPPROTO_TCP, PG_TCP_KEEPALIVE_IDLE,
				   (char *) &idle, sizeof(idle)) < 0)
	{
		ereport(LOG,
				(errmsg("%s(%s) failed: %m", "setsockopt", PG_TCP_KEEPALIVE_IDLE_STR)));
		return STATUS_ERROR;
	}

	port->keepalives_idle = idle;
#else							/* WIN32 */
	return pq_setkeepaliveswin32(port, idle, port->keepalives_interval);
#endif
#else
	if (idle != 0)
	{
		ereport(LOG,
				(errmsg("setting the keepalive idle time is not supported")));
		return STATUS_ERROR;
	}
#endif

	return STATUS_OK;
}

int
pq_getkeepalivesinterval(Port *port)
{
#if defined(TCP_KEEPINTVL) || defined(SIO_KEEPALIVE_VALS)
	if (port == NULL || port->laddr.addr.ss_family == AF_UNIX)
		return 0;

	if (port->keepalives_interval != 0)
		return port->keepalives_interval;

	if (port->default_keepalives_interval == 0)
	{
#ifndef WIN32
		socklen_t	size = sizeof(port->default_keepalives_interval);

		if (getsockopt(port->sock, IPPROTO_TCP, TCP_KEEPINTVL,
					   (char *) &port->default_keepalives_interval,
					   &size) < 0)
		{
			ereport(LOG,
					(errmsg("%s(%s) failed: %m", "getsockopt", "TCP_KEEPINTVL")));
			port->default_keepalives_interval = -1; /* don't know */
		}
#else
		/* We can't get the defaults on Windows, so return "don't know" */
		port->default_keepalives_interval = -1;
#endif							/* WIN32 */
	}

	return port->default_keepalives_interval;
#else
	return 0;
#endif
}

int
pq_setkeepalivesinterval(int interval, Port *port)
{
	if (port == NULL || port->laddr.addr.ss_family == AF_UNIX)
		return STATUS_OK;

#if defined(TCP_KEEPINTVL) || defined(SIO_KEEPALIVE_VALS)
	if (interval == port->keepalives_interval)
		return STATUS_OK;

#ifndef WIN32
	if (port->default_keepalives_interval <= 0)
	{
		if (pq_getkeepalivesinterval(port) < 0)
		{
			if (interval == 0)
				return STATUS_OK;	/* default is set but unknown */
			else
				return STATUS_ERROR;
		}
	}

	if (interval == 0)
		interval = port->default_keepalives_interval;

	if (setsockopt(port->sock, IPPROTO_TCP, TCP_KEEPINTVL,
				   (char *) &interval, sizeof(interval)) < 0)
	{
		ereport(LOG,
				(errmsg("%s(%s) failed: %m", "setsockopt", "TCP_KEEPINTVL")));
		return STATUS_ERROR;
	}

	port->keepalives_interval = interval;
#else							/* WIN32 */
	return pq_setkeepaliveswin32(port, port->keepalives_idle, interval);
#endif
#else
	if (interval != 0)
	{
		ereport(LOG,
				(errmsg("%s(%s) not supported", "setsockopt", "TCP_KEEPINTVL")));
		return STATUS_ERROR;
	}
#endif

	return STATUS_OK;
}

int
pq_getkeepalivescount(Port *port)
{
#ifdef TCP_KEEPCNT
	if (port == NULL || port->laddr.addr.ss_family == AF_UNIX)
		return 0;

	if (port->keepalives_count != 0)
		return port->keepalives_count;

	if (port->default_keepalives_count == 0)
	{
		socklen_t	size = sizeof(port->default_keepalives_count);

		if (getsockopt(port->sock, IPPROTO_TCP, TCP_KEEPCNT,
					   (char *) &port->default_keepalives_count,
					   &size) < 0)
		{
			ereport(LOG,
					(errmsg("%s(%s) failed: %m", "getsockopt", "TCP_KEEPCNT")));
			port->default_keepalives_count = -1;	/* don't know */
		}
	}

	return port->default_keepalives_count;
#else
	return 0;
#endif
}

int
pq_setkeepalivescount(int count, Port *port)
{
	if (port == NULL || port->laddr.addr.ss_family == AF_UNIX)
		return STATUS_OK;

#ifdef TCP_KEEPCNT
	if (count == port->keepalives_count)
		return STATUS_OK;

	if (port->default_keepalives_count <= 0)
	{
		if (pq_getkeepalivescount(port) < 0)
		{
			if (count == 0)
				return STATUS_OK;	/* default is set but unknown */
			else
				return STATUS_ERROR;
		}
	}

	if (count == 0)
		count = port->default_keepalives_count;

	if (setsockopt(port->sock, IPPROTO_TCP, TCP_KEEPCNT,
				   (char *) &count, sizeof(count)) < 0)
	{
		ereport(LOG,
				(errmsg("%s(%s) failed: %m", "setsockopt", "TCP_KEEPCNT")));
		return STATUS_ERROR;
	}

	port->keepalives_count = count;
#else
	if (count != 0)
	{
		ereport(LOG,
				(errmsg("%s(%s) not supported", "setsockopt", "TCP_KEEPCNT")));
		return STATUS_ERROR;
	}
#endif

	return STATUS_OK;
}

int
pq_gettcpusertimeout(Port *port)
{
#ifdef TCP_USER_TIMEOUT
	if (port == NULL || port->laddr.addr.ss_family == AF_UNIX)
		return 0;

	if (port->socket_tcp_user_timeout != 0)
		return port->socket_tcp_user_timeout;

	if (port->default_tcp_user_timeout == 0)
	{
		socklen_t	size = sizeof(port->default_tcp_user_timeout);

		if (getsockopt(port->sock, IPPROTO_TCP, TCP_USER_TIMEOUT,
					   (char *) &port->default_tcp_user_timeout,
					   &size) < 0)
		{
			ereport(LOG,
					(errmsg("%s(%s) failed: %m", "getsockopt", "TCP_USER_TIMEOUT")));
			port->default_tcp_user_timeout = -1;	/* don't know */
		}
	}

	return port->default_tcp_user_timeout;
#else
	return 0;
#endif
}

int
pq_settcpusertimeout(int timeout, Port *port)
{
	if (port == NULL || port->laddr.addr.ss_family == AF_UNIX)
		return STATUS_OK;

#ifdef TCP_USER_TIMEOUT
	if (timeout == port->socket_tcp_user_timeout)
		return STATUS_OK;

	if (port->default_tcp_user_timeout <= 0)
	{
		if (pq_gettcpusertimeout(port) < 0)
		{
			if (timeout == 0)
				return STATUS_OK;	/* default is set but unknown */
			else
				return STATUS_ERROR;
		}
	}

	if (timeout == 0)
		timeout = port->default_tcp_user_timeout;

	if (setsockopt(port->sock, IPPROTO_TCP, TCP_USER_TIMEOUT,
				   (char *) &timeout, sizeof(timeout)) < 0)
	{
		ereport(LOG,
				(errmsg("%s(%s) failed: %m", "setsockopt", "TCP_USER_TIMEOUT")));
		return STATUS_ERROR;
	}

	port->socket_tcp_user_timeout = timeout;
#else
	if (timeout != 0)
	{
		ereport(LOG,
				(errmsg("%s(%s) not supported", "setsockopt", "TCP_USER_TIMEOUT")));
		return STATUS_ERROR;
	}
#endif

	return STATUS_OK;
}

/*
 * GUC assign_hook for tcp_keepalives_idle
 */
void
assign_tcp_keepalives_idle(int newval, void *extra)
{
	/*
	 * The kernel API provides no way to test a value without setting it; and
	 * once we set it we might fail to unset it.  So there seems little point
	 * in fully implementing the check-then-assign GUC API for these
	 * variables.  Instead we just do the assignment on demand.
	 * pq_setkeepalivesidle reports any problems via ereport(LOG).
	 *
	 * This approach means that the GUC value might have little to do with the
	 * actual kernel value, so we use a show_hook that retrieves the kernel
	 * value rather than trusting GUC's copy.
	 */
	(void) pq_setkeepalivesidle(newval, MyProcPort);
}

/*
 * GUC show_hook for tcp_keepalives_idle
 */
const char *
show_tcp_keepalives_idle(void)
{
	/* See comments in assign_tcp_keepalives_idle */
	static char nbuf[16];

	snprintf(nbuf, sizeof(nbuf), "%d", pq_getkeepalivesidle(MyProcPort));
	return nbuf;
}

/*
 * GUC assign_hook for tcp_keepalives_interval
 */
void
assign_tcp_keepalives_interval(int newval, void *extra)
{
	/* See comments in assign_tcp_keepalives_idle */
	(void) pq_setkeepalivesinterval(newval, MyProcPort);
}

/*
 * GUC show_hook for tcp_keepalives_interval
 */
const char *
show_tcp_keepalives_interval(void)
{
	/* See comments in assign_tcp_keepalives_idle */
	static char nbuf[16];

	snprintf(nbuf, sizeof(nbuf), "%d", pq_getkeepalivesinterval(MyProcPort));
	return nbuf;
}

/*
 * GUC assign_hook for tcp_keepalives_count
 */
void
assign_tcp_keepalives_count(int newval, void *extra)
{
	/* See comments in assign_tcp_keepalives_idle */
	(void) pq_setkeepalivescount(newval, MyProcPort);
}

/*
 * GUC show_hook for tcp_keepalives_count
 */
const char *
show_tcp_keepalives_count(void)
{
	/* See comments in assign_tcp_keepalives_idle */
	static char nbuf[16];

	snprintf(nbuf, sizeof(nbuf), "%d", pq_getkeepalivescount(MyProcPort));
	return nbuf;
}

/*
 * GUC assign_hook for tcp_user_timeout
 */
void
assign_tcp_user_timeout(int newval, void *extra)
{
	/* See comments in assign_tcp_keepalives_idle */
	(void) pq_settcpusertimeout(newval, MyProcPort);
}

/*
 * GUC show_hook for tcp_user_timeout
 */
const char *
show_tcp_user_timeout(void)
{
	/* See comments in assign_tcp_keepalives_idle */
	static char nbuf[16];

	snprintf(nbuf, sizeof(nbuf), "%d", pq_gettcpusertimeout(MyProcPort));
	return nbuf;
}

/*
 * Check if the client is still connected.
 */
bool
pq_check_connection(void)
{
	WaitEvent	events[FeBeWaitSetNEvents];
	int			rc;

	/*
	 * It's OK to modify the socket event filter without restoring, because
	 * all FeBeWaitSet socket wait sites do the same.
	 */
	ModifyWaitEvent(FeBeWaitSet, FeBeWaitSetSocketPos, WL_SOCKET_CLOSED, NULL);

retry:
	rc = WaitEventSetWait(FeBeWaitSet, 0, events, lengthof(events), 0);
	for (int i = 0; i < rc; ++i)
	{
		if (events[i].events & WL_SOCKET_CLOSED)
			return false;
		if (events[i].events & WL_LATCH_SET)
		{
			/*
			 * A latch event might be preventing other events from being
			 * reported.  Reset it and poll again.  No need to restore it
			 * because no code should expect latches to survive across
			 * CHECK_FOR_INTERRUPTS().
			 */
			ResetLatch(MyLatch);
			goto retry;
		}
	}

	return true;
}
