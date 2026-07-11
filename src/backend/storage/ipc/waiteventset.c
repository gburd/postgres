/*-------------------------------------------------------------------------
 *
 * waiteventset.c
 *	  ppoll()/pselect() like abstraction
 *
 * WaitEvents are an abstraction for waiting for one or more events at a time.
 * The waiting can be done in a race free fashion, similar ppoll() or
 * pselect() (as opposed to plain poll()/select()).
 *
 * You can wait for:
 * - a latch being set from another process or from signal handler in the same
 *   process (WL_LATCH_SET)
 * - data to become readable or writeable on a socket (WL_SOCKET_*)
 * - postmaster death (WL_POSTMASTER_DEATH or WL_EXIT_ON_PM_DEATH)
 * - timeout (WL_TIMEOUT)
 *
 * Implementation
 * --------------
 *
 * The poll() implementation uses the so-called self-pipe trick to overcome the
 * race condition involved with poll() and setting a global flag in the signal
 * handler. When a latch is set and the current process is waiting for it, the
 * signal handler wakes up the poll() in WaitLatch by writing a byte to a pipe.
 * A signal by itself doesn't interrupt poll() on all platforms, and even on
 * platforms where it does, a signal that arrives just before the poll() call
 * does not prevent poll() from entering sleep. An incoming byte on a pipe
 * however reliably interrupts the sleep, and causes poll() to return
 * immediately even if the signal arrives before poll() begins.
 *
 * The epoll() implementation overcomes the race with a different technique: it
 * keeps SIGURG blocked and consumes from a signalfd() descriptor instead.  We
 * don't need to register a signal handler or create our own self-pipe.  We
 * assume that any system that has Linux epoll() also has Linux signalfd().
 *
 * The kqueue() implementation waits for SIGURG with EVFILT_SIGNAL.
 *
 * The Windows implementation uses Windows events that are inherited by all
 * postmaster child processes. There's no need for the self-pipe trick there.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/ipc/waiteventset.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <unistd.h>
#ifdef HAVE_SYS_EPOLL_H
#include <sys/epoll.h>
#endif
#ifdef HAVE_SYS_EVENT_H
#include <sys/event.h>
#endif
#ifdef HAVE_SYS_SIGNALFD_H
#include <sys/signalfd.h>
#endif
#ifdef HAVE_POLL_H
#include <poll.h>
#endif

#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "port/atomics.h"
#include "portability/instr_time.h"
#include "postmaster/postmaster.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/pmsignal.h"
#include "storage/latch.h"
#include "storage/waiteventset.h"
#ifdef USE_XTC_CARRIER
#include "postmaster/pg_xtc_carrier.h"
#include "xtc_proc.h"					/* xtc_self / xtc_pid_is_none */
#endif
#include "utils/backend_runtime.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/timeout.h"
#include "utils/wait_event.h"

/*
 * Select the fd readiness primitive to use. Normally the "most modern"
 * primitive supported by the OS will be used, but for testing it can be
 * useful to manually specify the used primitive.  If desired, just add a
 * define somewhere before this block.
 */
#if defined(WAIT_USE_EPOLL) || defined(WAIT_USE_POLL) || \
	defined(WAIT_USE_KQUEUE) || defined(WAIT_USE_WIN32)
/* don't overwrite manual choice */
#elif defined(HAVE_SYS_EPOLL_H)
#define WAIT_USE_EPOLL
#elif defined(HAVE_KQUEUE)
#define WAIT_USE_KQUEUE
#elif defined(HAVE_POLL)
#define WAIT_USE_POLL
#elif WIN32
#define WAIT_USE_WIN32
#else
#error "no wait set implementation available"
#endif

/*
 * By default, we use a self-pipe with poll() and a signalfd with epoll(), if
 * available.  For testing the choice can also be manually specified.
 */
#if defined(WAIT_USE_POLL) || defined(WAIT_USE_EPOLL)
#if defined(WAIT_USE_SELF_PIPE) || defined(WAIT_USE_SIGNALFD)
/* don't overwrite manual choice */
#elif defined(WAIT_USE_EPOLL)
#define WAIT_USE_SELF_PIPE
#else
#define WAIT_USE_SELF_PIPE
#endif
#endif

/* typedef in waiteventset.h */
struct WaitEventSet
{
	ResourceOwner owner;

	int			nevents;		/* number of registered events */
	int			nevents_space;	/* maximum number of events in this set */

	/*
	 * Array, of nevents_space length, storing the definition of events this
	 * set is waiting for.
	 */
	WaitEvent  *events;

	/*
	 * If WL_LATCH_SET is specified in any wait event, latch is a pointer to
	 * said latch, and latch_pos the offset in the ->events array. This is
	 * useful because we check the state of the latch before performing doing
	 * syscalls related to waiting.
	 */
	Latch	   *latch;
	int			latch_pos;

	/*
	 * WL_EXIT_ON_PM_DEATH is converted to WL_POSTMASTER_DEATH, but this flag
	 * is set so that we'll exit immediately if postmaster death is detected,
	 * instead of returning.
	 */
	bool		exit_on_postmaster_death;

#if defined(WAIT_USE_EPOLL)
	int			epoll_fd;
	/* epoll_wait returns events in a user provided arrays, allocate once */
	struct epoll_event *epoll_ret_events;
#elif defined(WAIT_USE_KQUEUE)
	int			kqueue_fd;
	/* kevent returns events in a user provided arrays, allocate once */
	struct kevent *kqueue_ret_events;
	bool		report_postmaster_not_running;
#elif defined(WAIT_USE_POLL)
	/* poll expects events to be waited on every poll() call, prepare once */
	struct pollfd *pollfds;
#elif defined(WAIT_USE_WIN32)

	/*
	 * Array of windows events. The first element always contains
	 * pgwin32_signal_event, so the remaining elements are offset by one (i.e.
	 * event->pos + 1).
	 */
	HANDLE	   *handles;
#endif
};

typedef struct WaitEventSetWaitArgs
{
	WaitEventSet *set;
	long		timeout;
	WaitEvent  *occurred_events;
	int			nevents;
	uint32		wait_event_info;
} WaitEventSetWaitArgs;

#ifndef WIN32
/* Are we currently in WaitLatch? The signal handler would like to know. */
#define waiting (*PgCurrentWaitEventWaitingRef())
#endif

#ifdef WAIT_USE_SIGNALFD
/* On Linux, we'll receive SIGURG via a signalfd file descriptor. */
#define signal_fd (*PgCurrentWaitEventSignalFdRef())
#endif

#ifdef WAIT_USE_SELF_PIPE
/* Read and write ends of the self-pipe */
#define selfpipe_readfd (*PgCurrentWaitEventSelfPipeReadFdRef())
#define selfpipe_writefd (*PgCurrentWaitEventSelfPipeWriteFdRef())

/* Process owning the self-pipe --- needed for checking purposes */
#define selfpipe_owner_pid (*PgCurrentWaitEventSelfPipeOwnerPidRef())

/* Private function prototypes */
static void latch_sigurg_handler(SIGNAL_ARGS);
static void sendSelfPipeByte(void);
static void sendSelfPipeByteToFd(int fd);
#endif

#if defined(WAIT_USE_SELF_PIPE) || defined(WAIT_USE_SIGNALFD)
static void drain(void);
#endif

#if defined(WAIT_USE_EPOLL)
static void WaitEventAdjustEpoll(WaitEventSet *set, WaitEvent *event, int action);
#elif defined(WAIT_USE_KQUEUE)
static void WaitEventAdjustKqueue(WaitEventSet *set, WaitEvent *event, int old_events);
#elif defined(WAIT_USE_POLL)
static void WaitEventAdjustPoll(WaitEventSet *set, WaitEvent *event);
#elif defined(WAIT_USE_WIN32)
static void WaitEventAdjustWin32(WaitEventSet *set, WaitEvent *event);
#endif

static inline int WaitEventSetWaitBlock(WaitEventSet *set, int cur_timeout,
										WaitEvent *occurred_events, int nevents);
static int WaitEventSetWaitInternal(void *callback_arg);
static bool WaitEventLatchFdNeedsRefresh(WaitEvent *event);
static void WaitEventRefreshLatchFd(WaitEventSet *set, WaitEvent *event);

/* ResourceOwner support to hold WaitEventSets */
static void ResOwnerReleaseWaitEventSet(Datum res);

static const ResourceOwnerDesc wait_event_set_resowner_desc =
{
	.name = "WaitEventSet",
	.release_phase = RESOURCE_RELEASE_AFTER_LOCKS,
	.release_priority = RELEASE_PRIO_WAITEVENTSETS,
	.ReleaseResource = ResOwnerReleaseWaitEventSet,
	.DebugPrint = NULL
};

/* Convenience wrappers over ResourceOwnerRemember/Forget */
static inline void
ResourceOwnerRememberWaitEventSet(ResourceOwner owner, WaitEventSet *set)
{
	ResourceOwnerRemember(owner, PointerGetDatum(set), &wait_event_set_resowner_desc);
}
static inline void
ResourceOwnerForgetWaitEventSet(ResourceOwner owner, WaitEventSet *set)
{
	ResourceOwnerForget(owner, PointerGetDatum(set), &wait_event_set_resowner_desc);
}


/*
 * Initialize the process-local wait event infrastructure.
 *
 * This must be called once during startup of any process that can wait on
 * latches, before it issues any InitLatch() or OwnLatch() calls.
 */
void
InitializeWaitEventSupport(void)
{
#if defined(WAIT_USE_SELF_PIPE)
	int			pipefd[2];

	if (IsUnderPostmaster)
	{
		/*
		 * We might have inherited connections to a self-pipe created by the
		 * postmaster.  It's critical that child processes create their own
		 * self-pipes, of course, and we really want them to close the
		 * inherited FDs for safety's sake.
		 */
		if (selfpipe_owner_pid != 0)
		{
			/* Assert we go through here but once in a child process */
			Assert(selfpipe_owner_pid != MyProcPid);
			/* Release postmaster's pipe FDs; ignore any error */
			(void) close(selfpipe_readfd);
			(void) close(selfpipe_writefd);
			/* Clean up, just for safety's sake; we'll set these below */
			selfpipe_readfd = selfpipe_writefd = -1;
			selfpipe_owner_pid = 0;
			/* Keep fd.c's accounting straight */
			ReleaseExternalFD();
			ReleaseExternalFD();
		}
		else
		{
			/*
			 * Postmaster didn't create a self-pipe ... or else we're in an
			 * EXEC_BACKEND build, in which case it doesn't matter since the
			 * postmaster's pipe FDs were closed by the action of FD_CLOEXEC.
			 * fd.c won't have state to clean up, either.
			 */
			Assert(selfpipe_readfd == -1);
		}
	}
	else
	{
		/* In postmaster or standalone backend, assert we do this but once */
		Assert(selfpipe_readfd == -1);
		Assert(selfpipe_owner_pid == 0);
	}

	/*
	 * Set up the self-pipe that allows a signal handler to wake up the
	 * poll()/epoll_wait() in WaitLatch. Make the write-end non-blocking, so
	 * that SetLatch won't block if the event has already been set many times
	 * filling the kernel buffer. Make the read-end non-blocking too, so that
	 * we can easily clear the pipe by reading until EAGAIN or EWOULDBLOCK.
	 * Also, make both FDs close-on-exec, since we surely do not want any
	 * child processes messing with them.
	 */
	if (pipe(pipefd) < 0)
		elog(FATAL, "pipe() failed: %m");
	if (fcntl(pipefd[0], F_SETFL, O_NONBLOCK) == -1)
		elog(FATAL, "fcntl(F_SETFL) failed on read-end of self-pipe: %m");
	if (fcntl(pipefd[1], F_SETFL, O_NONBLOCK) == -1)
		elog(FATAL, "fcntl(F_SETFL) failed on write-end of self-pipe: %m");
	if (fcntl(pipefd[0], F_SETFD, FD_CLOEXEC) == -1)
		elog(FATAL, "fcntl(F_SETFD) failed on read-end of self-pipe: %m");
	if (fcntl(pipefd[1], F_SETFD, FD_CLOEXEC) == -1)
		elog(FATAL, "fcntl(F_SETFD) failed on write-end of self-pipe: %m");

	selfpipe_readfd = pipefd[0];
	selfpipe_writefd = pipefd[1];
	selfpipe_owner_pid = MyProcPid;

	/* Tell fd.c about these two long-lived FDs */
	ReserveExternalFD();
	ReserveExternalFD();

	pqsignal(SIGURG, latch_sigurg_handler);
#endif

#ifdef WAIT_USE_SIGNALFD
	sigset_t	signalfd_mask;

	if (IsUnderPostmaster)
	{
		/*
		 * It would probably be safe to re-use the inherited signalfd since
		 * signalfds only see the current process's pending signals, but it
		 * seems less surprising to close it and create our own.
		 */
		if (signal_fd != -1)
		{
			/* Release postmaster's signal FD; ignore any error */
			(void) close(signal_fd);
			signal_fd = -1;
			ReleaseExternalFD();
		}
	}

	/* Block SIGURG, because we'll receive it through a signalfd. */
	sigaddset(&UnBlockSig, SIGURG);

	/* Set up the signalfd to receive SIGURG notifications. */
	sigemptyset(&signalfd_mask);
	sigaddset(&signalfd_mask, SIGURG);
	signal_fd = signalfd(-1, &signalfd_mask, SFD_NONBLOCK | SFD_CLOEXEC);
	if (signal_fd < 0)
		elog(FATAL, "signalfd() failed");
	ReserveExternalFD();
#endif

#ifdef WAIT_USE_KQUEUE
	/* Ignore SIGURG, because we'll receive it via kqueue. */
	pqsignal(SIGURG, PG_SIG_IGN);
#endif
}

/*
 * Release process-level wait event support owned by the current carrier.
 *
 * Backends historically relied on process exit to close these descriptors.
 * Threaded carriers exit without process exit, so descriptor and fd.c
 * accounting must be released explicitly at carrier teardown.
 */
void
ShutdownWaitEventSupport(void)
{
#ifndef WIN32
	waiting = false;
#endif

#ifdef WAIT_USE_SIGNALFD
	if (signal_fd != -1)
	{
		(void) close(signal_fd);
		signal_fd = -1;
		ReleaseExternalFD();
	}
#endif

#ifdef WAIT_USE_SELF_PIPE
	if (selfpipe_readfd != -1)
	{
		(void) close(selfpipe_readfd);
		selfpipe_readfd = -1;
		ReleaseExternalFD();
	}
	if (selfpipe_writefd != -1)
	{
		(void) close(selfpipe_writefd);
		selfpipe_writefd = -1;
		ReleaseExternalFD();
	}
	selfpipe_owner_pid = 0;
#endif
}

/*
 * Create a WaitEventSet with space for nevents different events to wait for.
 *
 * These events can then be efficiently waited upon together, using
 * WaitEventSetWait().
 *
 * The WaitEventSet is tracked by the given 'resowner'.  Use NULL for session
 * lifetime.
 */
WaitEventSet *
CreateWaitEventSet(ResourceOwner resowner, int nevents)
{
	WaitEventSet *set;
	char	   *data;
	Size		sz = 0;

	/*
	 * Use MAXALIGN size/alignment to guarantee that later uses of memory are
	 * aligned correctly. E.g. epoll_event might need 8 byte alignment on some
	 * platforms, but earlier allocations like WaitEventSet and WaitEvent
	 * might not be sized to guarantee that when purely using sizeof().
	 */
	sz += MAXALIGN(sizeof(WaitEventSet));
	sz += MAXALIGN(sizeof(WaitEvent) * nevents);

#if defined(WAIT_USE_EPOLL)
	sz += MAXALIGN(sizeof(struct epoll_event) * nevents);
#elif defined(WAIT_USE_KQUEUE)
	sz += MAXALIGN(sizeof(struct kevent) * nevents);
#elif defined(WAIT_USE_POLL)
	sz += MAXALIGN(sizeof(struct pollfd) * nevents);
#elif defined(WAIT_USE_WIN32)
	/* need space for the pgwin32_signal_event */
	sz += MAXALIGN(sizeof(HANDLE) * (nevents + 1));
#endif

	if (resowner != NULL)
		ResourceOwnerEnlarge(resowner);

	data = (char *) MemoryContextAllocZero(TopMemoryContext, sz);

	set = (WaitEventSet *) data;
	data += MAXALIGN(sizeof(WaitEventSet));

	set->events = (WaitEvent *) data;
	data += MAXALIGN(sizeof(WaitEvent) * nevents);

#if defined(WAIT_USE_EPOLL)
	set->epoll_ret_events = (struct epoll_event *) data;
	data += MAXALIGN(sizeof(struct epoll_event) * nevents);
#elif defined(WAIT_USE_KQUEUE)
	set->kqueue_ret_events = (struct kevent *) data;
	data += MAXALIGN(sizeof(struct kevent) * nevents);
#elif defined(WAIT_USE_POLL)
	set->pollfds = (struct pollfd *) data;
	data += MAXALIGN(sizeof(struct pollfd) * nevents);
#elif defined(WAIT_USE_WIN32)
	set->handles = (HANDLE) data;
	data += MAXALIGN(sizeof(HANDLE) * nevents);
#endif

	set->latch = NULL;
	set->nevents_space = nevents;
	set->exit_on_postmaster_death = false;

	if (resowner != NULL)
	{
		ResourceOwnerRememberWaitEventSet(resowner, set);
		set->owner = resowner;
	}

#if defined(WAIT_USE_EPOLL)
	if (!AcquireExternalFD())
		elog(ERROR, "AcquireExternalFD, for epoll_create1, failed: %m");
	set->epoll_fd = epoll_create1(EPOLL_CLOEXEC);
	if (set->epoll_fd < 0)
	{
		ReleaseExternalFD();
		elog(ERROR, "epoll_create1 failed: %m");
	}
#elif defined(WAIT_USE_KQUEUE)
	if (!AcquireExternalFD())
		elog(ERROR, "AcquireExternalFD, for kqueue, failed: %m");
	set->kqueue_fd = kqueue();
	if (set->kqueue_fd < 0)
	{
		ReleaseExternalFD();
		elog(ERROR, "kqueue failed: %m");
	}
	if (fcntl(set->kqueue_fd, F_SETFD, FD_CLOEXEC) == -1)
	{
		int			save_errno = errno;

		close(set->kqueue_fd);
		ReleaseExternalFD();
		errno = save_errno;
		elog(ERROR, "fcntl(F_SETFD) failed on kqueue descriptor: %m");
	}
	set->report_postmaster_not_running = false;
#elif defined(WAIT_USE_WIN32)

	/*
	 * To handle signals while waiting, we need to add a win32 specific event.
	 * We accounted for the additional event at the top of this routine. See
	 * port/win32/signal.c for more details.
	 *
	 * Note: pgwin32_signal_event should be first to ensure that it will be
	 * reported when multiple events are set.  We want to guarantee that
	 * pending signals are serviced.
	 */
	set->handles[0] = pgwin32_signal_event;
#endif

	return set;
}

/*
 * Free a previously created WaitEventSet.
 *
 * Note: preferably, this shouldn't have to free any resources that could be
 * inherited across an exec().  If it did, we'd likely leak those resources in
 * many scenarios.  For the epoll case, we ensure that by setting EPOLL_CLOEXEC
 * when the FD is created.  For the Windows case, we assume that the handles
 * involved are non-inheritable.
 */
void
FreeWaitEventSet(WaitEventSet *set)
{
	if (set->owner)
	{
		ResourceOwnerForgetWaitEventSet(set->owner, set);
		set->owner = NULL;
	}

#if defined(WAIT_USE_EPOLL)
	close(set->epoll_fd);
	ReleaseExternalFD();
#elif defined(WAIT_USE_KQUEUE)
	close(set->kqueue_fd);
	ReleaseExternalFD();
#elif defined(WAIT_USE_WIN32)
	for (WaitEvent *cur_event = set->events;
		 cur_event < (set->events + set->nevents);
		 cur_event++)
	{
		if (cur_event->events & WL_LATCH_SET)
		{
			/* uses the latch's HANDLE */
		}
		else if (cur_event->events & WL_POSTMASTER_DEATH)
		{
			/* uses PostmasterHandle */
		}
		else
		{
			/* Clean up the event object we created for the socket */
			WSAEventSelect(cur_event->fd, NULL, 0);
			WSACloseEvent(set->handles[cur_event->pos + 1]);
		}
	}
#endif

	pfree(set);
}

/*
 * Free a previously created WaitEventSet in a child process after a fork().
 */
void
FreeWaitEventSetAfterFork(WaitEventSet *set)
{
#if defined(WAIT_USE_EPOLL)
	close(set->epoll_fd);
	ReleaseExternalFD();
#elif defined(WAIT_USE_KQUEUE)
	/* kqueues are not normally inherited by child processes */
	ReleaseExternalFD();
#endif

	pfree(set);
}

/* ---
 * Add an event to the set. Possible events are:
 * - WL_LATCH_SET: Wait for the latch to be set
 * - WL_POSTMASTER_DEATH: Wait for postmaster to die
 * - WL_SOCKET_READABLE: Wait for socket to become readable,
 *	 can be combined in one event with other WL_SOCKET_* events
 * - WL_SOCKET_WRITEABLE: Wait for socket to become writeable,
 *	 can be combined with other WL_SOCKET_* events
 * - WL_SOCKET_CONNECTED: Wait for socket connection to be established,
 *	 can be combined with other WL_SOCKET_* events (on non-Windows
 *	 platforms, this is the same as WL_SOCKET_WRITEABLE)
 * - WL_SOCKET_ACCEPT: Wait for new connection to a server socket,
 *	 can be combined with other WL_SOCKET_* events (on non-Windows
 *	 platforms, this is the same as WL_SOCKET_READABLE)
 * - WL_SOCKET_CLOSED: Wait for socket to be closed by remote peer.
 * - WL_EXIT_ON_PM_DEATH: Exit immediately if the postmaster dies
 *
 * Returns the offset in WaitEventSet->events (starting from 0), which can be
 * used to modify previously added wait events using ModifyWaitEvent().
 *
 * In the WL_LATCH_SET case the latch must be owned by the current process,
 * i.e. it must be a process-local latch initialized with InitLatch, or a
 * shared latch associated with the current process by calling OwnLatch.
 *
 * In the WL_SOCKET_READABLE/WRITEABLE/CONNECTED/ACCEPT cases, EOF and error
 * conditions cause the socket to be reported as readable/writable/connected,
 * so that the caller can deal with the condition.
 *
 * The user_data pointer specified here will be set for the events returned
 * by WaitEventSetWait(), allowing to easily associate additional data with
 * events.
 */
int
AddWaitEventToSet(WaitEventSet *set, uint32 events, pgsocket fd, Latch *latch,
				  void *user_data)
{
	WaitEvent  *event;

	/* not enough space */
	Assert(set->nevents < set->nevents_space);

	if (events == WL_EXIT_ON_PM_DEATH)
	{
		events = WL_POSTMASTER_DEATH;
		set->exit_on_postmaster_death = true;
	}

	if (latch)
	{
		if (latch->owner_pid != MyProcPid)
			elog(ERROR, "cannot wait on a latch owned by another process");
		if (set->latch)
			elog(ERROR, "cannot wait on more than one latch");
		if ((events & WL_LATCH_SET) != WL_LATCH_SET)
			elog(ERROR, "latch events only support being set");
	}
	else
	{
		if (events & WL_LATCH_SET)
			elog(ERROR, "cannot wait on latch without a specified latch");
	}

	/* waiting for socket readiness without a socket indicates a bug */
	if (fd == PGINVALID_SOCKET && (events & WL_SOCKET_MASK))
		elog(ERROR, "cannot wait on socket event without a socket");

	event = &set->events[set->nevents];
	event->pos = set->nevents++;
	event->fd = fd;
	event->events = events;
	event->user_data = user_data;
#ifdef WIN32
	event->reset = false;
#endif

	if (events == WL_LATCH_SET)
	{
		set->latch = latch;
		set->latch_pos = event->pos;
#if defined(WAIT_USE_SELF_PIPE)
		event->fd = selfpipe_readfd;
#elif defined(WAIT_USE_SIGNALFD)
		event->fd = signal_fd;
#elif defined(WAIT_USE_KQUEUE)
		latch->owner_wakeup_fd = set->kqueue_fd;
		event->fd = PGINVALID_SOCKET;
#else
		event->fd = PGINVALID_SOCKET;
#ifdef WAIT_USE_EPOLL
		return event->pos;
#endif
#endif
	}
	else if (events == WL_POSTMASTER_DEATH)
	{
#ifndef WIN32
		event->fd = postmaster_alive_fds[POSTMASTER_FD_WATCH];
#endif
	}

	/* perform wait primitive specific initialization, if needed */
#if defined(WAIT_USE_EPOLL)
	WaitEventAdjustEpoll(set, event, EPOLL_CTL_ADD);
#elif defined(WAIT_USE_KQUEUE)
	WaitEventAdjustKqueue(set, event, 0);
#elif defined(WAIT_USE_POLL)
	WaitEventAdjustPoll(set, event);
#elif defined(WAIT_USE_WIN32)
	WaitEventAdjustWin32(set, event);
#endif

	return event->pos;
}

/*
 * Change the event mask and, in the WL_LATCH_SET case, the latch associated
 * with the WaitEvent.  The latch may be changed to NULL to disable the latch
 * temporarily, and then set back to a latch later.
 *
 * 'pos' is the id returned by AddWaitEventToSet.
 */
void
ModifyWaitEvent(WaitEventSet *set, int pos, uint32 events, Latch *latch)
{
	WaitEvent  *event;
	bool		latch_fd_needs_refresh;
#if defined(WAIT_USE_KQUEUE)
	int			old_events;
#endif

	Assert(pos < set->nevents);

	event = &set->events[pos];
#if defined(WAIT_USE_KQUEUE)
	old_events = event->events;
#endif

	/*
	 * Allow switching between WL_POSTMASTER_DEATH and WL_EXIT_ON_PM_DEATH.
	 *
	 * Note that because WL_EXIT_ON_PM_DEATH is mapped to WL_POSTMASTER_DEATH
	 * in AddWaitEventToSet(), this needs to be checked before the fast-path
	 * below that checks if 'events' has changed.
	 */
	if (event->events == WL_POSTMASTER_DEATH)
	{
		if (events != WL_POSTMASTER_DEATH && events != WL_EXIT_ON_PM_DEATH)
			elog(ERROR, "cannot remove postmaster death event");
		set->exit_on_postmaster_death = ((events & WL_EXIT_ON_PM_DEATH) != 0);
		return;
	}

	latch_fd_needs_refresh = WaitEventLatchFdNeedsRefresh(event);

	/*
	 * If neither the event mask nor the associated latch changes, return
	 * early. That's an important optimization for some sockets, where
	 * ModifyWaitEvent is frequently used to switch from waiting for reads to
	 * waiting on writes.
	 */
	if (events == event->events &&
		(!(event->events & WL_LATCH_SET) || set->latch == latch) &&
		!latch_fd_needs_refresh)
		return;

	if (event->events & WL_LATCH_SET && events != event->events)
		elog(ERROR, "cannot modify latch event");

	/* FIXME: validate event mask */
	event->events = events;

	if (events == WL_LATCH_SET)
	{
		if (latch && latch->owner_pid != MyProcPid)
			elog(ERROR, "cannot wait on a latch owned by another process");
		set->latch = latch;
#if defined(WAIT_USE_KQUEUE)
		if (latch)
			latch->owner_wakeup_fd = set->kqueue_fd;
#endif

		/*
		 * On Unix, we don't need to modify the kernel object because the
		 * underlying pipe (if there is one) is the same for all latches in a
		 * carrier.  A pooled logical backend can move to another carrier,
		 * though, so refresh the registered latch fd before returning.  On
		 * Windows, we need to update our array of handles, but we leave the
		 * old one in place and tolerate spurious wakeups if the latch is
		 * disabled.
		 */
#if defined(WAIT_USE_WIN32)
		if (!latch)
			return;
#else
		if (latch_fd_needs_refresh)
			WaitEventRefreshLatchFd(set, event);
		return;
#endif
	}

#if defined(WAIT_USE_EPOLL)
	WaitEventAdjustEpoll(set, event, EPOLL_CTL_MOD);
#elif defined(WAIT_USE_KQUEUE)
	WaitEventAdjustKqueue(set, event, old_events);
#elif defined(WAIT_USE_POLL)
	WaitEventAdjustPoll(set, event);
#elif defined(WAIT_USE_WIN32)
	WaitEventAdjustWin32(set, event);
#endif
}

#if defined(WAIT_USE_EPOLL)
/*
 * action can be one of EPOLL_CTL_ADD | EPOLL_CTL_MOD | EPOLL_CTL_DEL
 */
static void
WaitEventAdjustEpoll(WaitEventSet *set, WaitEvent *event, int action)
{
	struct epoll_event epoll_ev;
	int			rc;

	/* pointer to our event, returned by epoll_wait */
	epoll_ev.data.ptr = event;
	/* always wait for errors */
	epoll_ev.events = EPOLLERR | EPOLLHUP;

	/* prepare pollfd entry once */
	if (event->events == WL_LATCH_SET)
	{
		Assert(set->latch != NULL);
		epoll_ev.events |= EPOLLIN;
	}
	else if (event->events == WL_POSTMASTER_DEATH)
	{
		epoll_ev.events |= EPOLLIN;
	}
	else
	{
		Assert(event->fd != PGINVALID_SOCKET);
		Assert(event->events & (WL_SOCKET_READABLE |
								WL_SOCKET_WRITEABLE |
								WL_SOCKET_CLOSED));

		if (event->events & WL_SOCKET_READABLE)
			epoll_ev.events |= EPOLLIN;
		if (event->events & WL_SOCKET_WRITEABLE)
			epoll_ev.events |= EPOLLOUT;
		if (event->events & WL_SOCKET_CLOSED)
			epoll_ev.events |= EPOLLRDHUP;
	}

	/*
	 * Even though unused, we also pass epoll_ev as the data argument if
	 * EPOLL_CTL_DEL is passed as action.  There used to be an epoll bug
	 * requiring that, and actually it makes the code simpler...
	 */
	rc = epoll_ctl(set->epoll_fd, action, event->fd, &epoll_ev);

	if (rc < 0)
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("%s() failed: %m",
						"epoll_ctl")));
}
#endif

#if defined(WAIT_USE_POLL)
static void
WaitEventAdjustPoll(WaitEventSet *set, WaitEvent *event)
{
	struct pollfd *pollfd = &set->pollfds[event->pos];

	pollfd->revents = 0;
	pollfd->fd = event->fd;

	/* prepare pollfd entry once */
	if (event->events == WL_LATCH_SET)
	{
		Assert(set->latch != NULL);
		pollfd->events = POLLIN;
	}
	else if (event->events == WL_POSTMASTER_DEATH)
	{
		pollfd->events = POLLIN;
	}
	else
	{
		Assert(event->events & (WL_SOCKET_READABLE |
								WL_SOCKET_WRITEABLE |
								WL_SOCKET_CLOSED));
		pollfd->events = 0;
		if (event->events & WL_SOCKET_READABLE)
			pollfd->events |= POLLIN;
		if (event->events & WL_SOCKET_WRITEABLE)
			pollfd->events |= POLLOUT;
#ifdef POLLRDHUP
		if (event->events & WL_SOCKET_CLOSED)
			pollfd->events |= POLLRDHUP;
#endif
	}

	Assert(event->fd != PGINVALID_SOCKET);
}
#endif

#if defined(WAIT_USE_KQUEUE)

/*
 * On most BSD family systems, the udata member of struct kevent is of type
 * void *, so we could directly convert to/from WaitEvent *.  Unfortunately,
 * NetBSD has it as intptr_t, so here we wallpaper over that difference with
 * an lvalue cast.
 */
#define AccessWaitEvent(k_ev) (*((WaitEvent **)(&(k_ev)->udata)))

static inline void
WaitEventAdjustKqueueAdd(struct kevent *k_ev, int filter, int action,
						 WaitEvent *event)
{
	k_ev->ident = event->fd;
	k_ev->filter = filter;
	k_ev->flags = action;
	k_ev->fflags = 0;
	k_ev->data = 0;
	AccessWaitEvent(k_ev) = event;
}

static inline void
WaitEventAdjustKqueueAddPostmaster(struct kevent *k_ev, WaitEvent *event)
{
	/* For now postmaster death can only be added, not removed. */
	k_ev->ident = PostmasterPid;
	k_ev->filter = EVFILT_PROC;
	k_ev->flags = EV_ADD;
	k_ev->fflags = NOTE_EXIT;
	k_ev->data = 0;
	AccessWaitEvent(k_ev) = event;
}

static inline void
WaitEventAdjustKqueueAddLatch(struct kevent *k_ev, WaitEvent *event)
{
	/* For now latch can only be added, not removed. */
	k_ev->ident = SIGURG;
	k_ev->filter = EVFILT_SIGNAL;
	k_ev->flags = EV_ADD;
	k_ev->fflags = 0;
	k_ev->data = 0;
	AccessWaitEvent(k_ev) = event;
}

static inline void
WaitEventAdjustKqueueAddLatchWakeup(struct kevent *k_ev, WaitEvent *event)
{
	/*
	 * Threaded backends in one process cannot reliably target another
	 * thread's kqueue through the historical process-level SIGURG wakeup.
	 * Register a user event as a direct wake channel for the wait set that
	 * owns this latch.
	 */
	k_ev->ident = WL_LATCH_SET;
	k_ev->filter = EVFILT_USER;
	k_ev->flags = EV_ADD | EV_CLEAR;
	k_ev->fflags = 0;
	k_ev->data = 0;
	AccessWaitEvent(k_ev) = event;
}

/*
 * old_events is the previous event mask, used to compute what has changed.
 */
static void
WaitEventAdjustKqueue(WaitEventSet *set, WaitEvent *event, int old_events)
{
	int			rc;
	struct kevent k_ev[2];
	int			count = 0;
	bool		new_filt_read = false;
	bool		old_filt_read = false;
	bool		new_filt_write = false;
	bool		old_filt_write = false;

	if (old_events == event->events)
		return;

	Assert(event->events != WL_LATCH_SET || set->latch != NULL);
	Assert(event->events == WL_LATCH_SET ||
		   event->events == WL_POSTMASTER_DEATH ||
		   (event->events & (WL_SOCKET_READABLE |
							 WL_SOCKET_WRITEABLE |
							 WL_SOCKET_CLOSED)));

	if (event->events == WL_POSTMASTER_DEATH)
	{
		/*
		 * Unlike all the other implementations, we detect postmaster death
		 * using process notification instead of waiting on the postmaster
		 * alive pipe.
		 */
		WaitEventAdjustKqueueAddPostmaster(&k_ev[count++], event);
	}
	else if (event->events == WL_LATCH_SET)
	{
		/* We detect latch wakeup using a signal event. */
		WaitEventAdjustKqueueAddLatch(&k_ev[count++], event);
		WaitEventAdjustKqueueAddLatchWakeup(&k_ev[count++], event);
	}
	else
	{
		/*
		 * We need to compute the adds and deletes required to get from the
		 * old event mask to the new event mask, since kevent treats readable
		 * and writable as separate events.
		 */
		if (old_events & (WL_SOCKET_READABLE | WL_SOCKET_CLOSED))
			old_filt_read = true;
		if (event->events & (WL_SOCKET_READABLE | WL_SOCKET_CLOSED))
			new_filt_read = true;
		if (old_events & WL_SOCKET_WRITEABLE)
			old_filt_write = true;
		if (event->events & WL_SOCKET_WRITEABLE)
			new_filt_write = true;
		if (old_filt_read && !new_filt_read)
			WaitEventAdjustKqueueAdd(&k_ev[count++], EVFILT_READ, EV_DELETE,
									 event);
		else if (!old_filt_read && new_filt_read)
			WaitEventAdjustKqueueAdd(&k_ev[count++], EVFILT_READ, EV_ADD,
									 event);
		if (old_filt_write && !new_filt_write)
			WaitEventAdjustKqueueAdd(&k_ev[count++], EVFILT_WRITE, EV_DELETE,
									 event);
		else if (!old_filt_write && new_filt_write)
			WaitEventAdjustKqueueAdd(&k_ev[count++], EVFILT_WRITE, EV_ADD,
									 event);
	}

	/* For WL_SOCKET_READ -> WL_SOCKET_CLOSED, no change needed. */
	if (count == 0)
		return;

	Assert(count <= 2);

	rc = kevent(set->kqueue_fd, &k_ev[0], count, NULL, 0, NULL);

	/*
	 * When adding the postmaster's pid, we have to consider that it might
	 * already have exited and perhaps even been replaced by another process
	 * with the same pid.  If so, we have to defer reporting this as an event
	 * until the next call to WaitEventSetWaitBlock().
	 */

	if (rc < 0)
	{
		if (event->events == WL_POSTMASTER_DEATH &&
			(errno == ESRCH || errno == EACCES))
			set->report_postmaster_not_running = true;
		else
			ereport(ERROR,
					(errcode_for_socket_access(),
					 errmsg("%s() failed: %m",
							"kevent")));
	}
	else if (event->events == WL_POSTMASTER_DEATH &&
			 PostmasterPid != getppid() &&
			 !PostmasterIsAlive())
	{
		/*
		 * The extra PostmasterIsAliveInternal() check prevents false alarms
		 * on systems that give a different value for getppid() while being
		 * traced by a debugger.
		 */
		set->report_postmaster_not_running = true;
	}
}

#endif

#if defined(WAIT_USE_WIN32)
StaticAssertDecl(WSA_INVALID_EVENT == NULL, "");

static void
WaitEventAdjustWin32(WaitEventSet *set, WaitEvent *event)
{
	HANDLE	   *handle = &set->handles[event->pos + 1];

	if (event->events == WL_LATCH_SET)
	{
		Assert(set->latch != NULL);
		*handle = set->latch->event;
	}
	else if (event->events == WL_POSTMASTER_DEATH)
	{
		*handle = PostmasterHandle;
	}
	else
	{
		int			flags = FD_CLOSE;	/* always check for errors/EOF */

		if (event->events & WL_SOCKET_READABLE)
			flags |= FD_READ;
		if (event->events & WL_SOCKET_WRITEABLE)
			flags |= FD_WRITE;
		if (event->events & WL_SOCKET_CONNECTED)
			flags |= FD_CONNECT;
		if (event->events & WL_SOCKET_ACCEPT)
			flags |= FD_ACCEPT;

		if (*handle == WSA_INVALID_EVENT)
		{
			*handle = WSACreateEvent();
			if (*handle == WSA_INVALID_EVENT)
				elog(ERROR, "failed to create event for socket: error code %d",
					 WSAGetLastError());
		}
		if (WSAEventSelect(event->fd, *handle, flags) != 0)
			elog(ERROR, "failed to set up event for socket: error code %d",
				 WSAGetLastError());

		Assert(event->fd != PGINVALID_SOCKET);
	}
}
#endif

/*
 * Wait for events added to the set to happen, or until the timeout is
 * reached.  At most nevents occurred events are returned.
 *
 * If timeout = -1, block until an event occurs; if 0, check sockets for
 * readiness, but don't block; if > 0, block for at most timeout milliseconds.
 *
 * Returns the number of events occurred, or 0 if the timeout was reached.
 *
 * Returned events will have the fd, pos, user_data fields set to the
 * values associated with the registered event.
 */
int
WaitEventSetWait(WaitEventSet *set, long timeout,
				 WaitEvent *occurred_events, int nevents,
				 uint32 wait_event_info)
{
	WaitEventSetWaitArgs args;

	args.set = set;
	args.timeout = timeout;
	args.occurred_events = occurred_events;
	args.nevents = nevents;
	args.wait_event_info = wait_event_info;

#ifndef PG_RUNTIME_ENABLE_WAIT_COMPLETION_PUBLICATION
	return WaitEventSetWaitInternal(&args);
#else
	if (likely(!PgBackendShouldPublishWaitCompletion(CurrentPgBackend)))
		return WaitEventSetWaitInternal(&args);
	else
	{
		PgWaitSpec	wait_spec;
		uint32		wake_events = 0;
		pgsocket	wait_socket = PGINVALID_SOCKET;
		bool		found_wait_socket = false;
		bool		multiple_wait_sockets = false;

		for (int i = 0; i < set->nevents; i++)
		{
			wake_events |= set->events[i].events;
			if ((set->events[i].events & WL_SOCKET_MASK) != 0 &&
				set->events[i].fd != PGINVALID_SOCKET)
			{
				if (!found_wait_socket)
				{
					wait_socket = set->events[i].fd;
					found_wait_socket = true;
				}
				else if (wait_socket != set->events[i].fd)
					multiple_wait_sockets = true;
			}
		}
		if (multiple_wait_sockets)
			wait_socket = PGINVALID_SOCKET;
		if (timeout >= 0)
			wake_events |= WL_TIMEOUT;

		wait_spec.kind = PG_WAIT_KIND_EVENT_SET;
		wait_spec.wait_event_info = wait_event_info;
		wait_spec.wake_events = wake_events;
		wait_spec.socket = wait_socket;
		wait_spec.timeout = timeout;

		return PgSuspend(&wait_spec, WaitEventSetWaitInternal, &args);
	}
#endif
}

static int
WaitEventSetWaitInternal(void *callback_arg)
{
	WaitEventSetWaitArgs *args = (WaitEventSetWaitArgs *) callback_arg;
	WaitEventSet *set = args->set;
	long		timeout = args->timeout;
	WaitEvent  *occurred_events = args->occurred_events;
	int			nevents = args->nevents;
	uint32		wait_event_info = args->wait_event_info;
	int			returned_events = 0;
	instr_time	start_time;
	instr_time	cur_time;
	long		cur_timeout = -1;

	Assert(nevents > 0);

	/*
	 * Initialize timeout if requested.  We must record the current time so
	 * that we can determine the remaining timeout if interrupted.
	 */
	if (timeout >= 0)
	{
		INSTR_TIME_SET_CURRENT(start_time);
		Assert(timeout >= 0 && timeout <= INT_MAX);
		cur_timeout = timeout;
	}
	else
		INSTR_TIME_SET_ZERO(start_time);

	pgstat_report_wait_start(wait_event_info);

#ifndef WIN32
	waiting = true;
#else
	/* Ensure that signals are serviced even if latch is already set */
	pgwin32_dispatch_queued_signals();
#endif
	while (returned_events == 0)
	{
		int			rc;
		long		block_timeout;
		long		logical_timeout;

		/*
		 * Check if the latch is set already first.  If so, we either exit
		 * immediately or ask the kernel for further events available right
		 * now without waiting, depending on how many events the caller wants.
		 *
		 * If someone sets the latch between this and the
		 * WaitEventSetWaitBlock() below, the setter will write a byte to the
		 * pipe (or signal us and the signal handler will do that), and the
		 * readiness routine will return immediately.
		 *
		 * On unix, If there's a pending byte in the self pipe, we'll notice
		 * whenever blocking. Only clearing the pipe in that case avoids
		 * having to drain it every time WaitLatchOrSocket() is used. Should
		 * the pipe-buffer fill up we're still ok, because the pipe is in
		 * nonblocking mode. It's unlikely for that to happen, because the
		 * self pipe isn't filled unless we're blocking (waiting = true), or
		 * from inside a signal handler in latch_sigurg_handler().
		 *
		 * On windows, we'll also notice if there's a pending event for the
		 * latch when blocking, but there's no danger of anything filling up,
		 * as "Setting an event that is already set has no effect.".
		 *
		 * Note: we assume that the kernel calls involved in latch management
		 * will provide adequate synchronization on machines with weak memory
		 * ordering, so that we cannot miss seeing is_set if a notification
		 * has already been queued.
		 */
		if (set->latch && !set->latch->is_set)
		{
			/* about to sleep on a latch */
			set->latch->maybe_sleeping = true;
			pg_memory_barrier();
			/* and recheck */
		}

		if (set->latch && set->latch->is_set)
		{
			occurred_events->fd = PGINVALID_SOCKET;
			occurred_events->pos = set->latch_pos;
			occurred_events->user_data =
				set->events[set->latch_pos].user_data;
			occurred_events->events = WL_LATCH_SET;
			occurred_events++;
			returned_events++;

			/* could have been set above */
			set->latch->maybe_sleeping = false;

			if (returned_events == nevents)
				break;			/* output buffer full already */

			/*
			 * Even though we already have an event, we'll poll just once with
			 * zero timeout to see what non-latch events we can fit into the
			 * output buffer at the same time.
			 */
			cur_timeout = 0;
			timeout = 0;
		}

		block_timeout = cur_timeout;

		/*
		 * Logical backends do not use process SIGALRM for timeout delivery.
		 * Clamp the kernel sleep to the next backend-local timeout so the
		 * normal timeout handlers can run while this backend is blocked.
		 */
		logical_timeout = get_logical_timeout_delay_ms();
		if (logical_timeout >= 0 &&
			(block_timeout < 0 || logical_timeout < block_timeout))
			block_timeout = logical_timeout;

		/*
		 * Wait for events using the readiness primitive chosen at the top of
		 * this file. If -1 is returned, a timeout has occurred, if 0 we have
		 * to retry, everything >= 1 is the number of returned events.
		 */
		rc = WaitEventSetWaitBlock(set, block_timeout,
								   occurred_events, nevents - returned_events);

		if (set->latch &&
			set->latch->maybe_sleeping)
			set->latch->maybe_sleeping = false;

		if (rc == -1)
		{
			if (logical_timeout >= 0 && process_due_logical_timeouts())
				continue;
			break;				/* timeout occurred */
		}
		else
			returned_events += rc;

		/* If we're not done, update cur_timeout for next iteration */
		if (returned_events == 0 && timeout >= 0)
		{
			INSTR_TIME_SET_CURRENT(cur_time);
			INSTR_TIME_SUBTRACT(cur_time, start_time);
			cur_timeout = timeout - (long) INSTR_TIME_GET_MILLISEC(cur_time);
			if (cur_timeout <= 0)
				break;
		}
	}
#ifndef WIN32
	waiting = false;
#endif

	pgstat_report_wait_end();

	return returned_events;
}


#if defined(WAIT_USE_EPOLL)

/*
 * Wait using linux's epoll_wait(2).
 *
 * This is the preferable wait method, as several readiness notifications are
 * delivered, without having to iterate through all of set->events. The return
 * epoll_event struct contain a pointer to our events, making association
 * easy.
 */
static inline int
WaitEventSetWaitBlock(WaitEventSet *set, int cur_timeout,
					  WaitEvent *occurred_events, int nevents)
{
	int			returned_events = 0;
	int			rc;
	WaitEvent  *cur_event;
	struct epoll_event *cur_epoll_event;

#ifdef USE_XTC_CARRIER
	/*
	 * xtc-carrier: while this backend runs as an xtc fiber, do not block the
	 * carrier thread in epoll_wait().  The epoll fd is itself pollable: it
	 * becomes readable when any registered event is ready.  Yield the fiber
	 * on set->epoll_fd via the xtc loop, then harvest with a non-blocking
	 * epoll_wait(..., 0).  All event decoding below is reused unchanged.
	 *
	 * The latch signalfd and postmaster-death fd are among the registered
	 * events, so a SetLatch wake (which writes the signalfd) also makes the
	 * epoll fd readable and unparks the fiber -- no separate latch path
	 * needed for this single-backend bringup.
	 */
	if (xtc_in_backend_fiber || !xtc_pid_is_none(xtc_self()))
	{
		int			wl;

		/*
		 * Record THIS fiber as the wait latch's fiber owner right before
		 * parking, so a cross-thread SetLatch (e.g. an io worker completing an
		 * async read on another carrier) can xtc_proc_wake() the parked fiber's
		 * loop -- the guaranteed cross-thread resume (libxtc >= v1.8.0).
		 * Capturing here, at the actual park point, is authoritative: xtc_self()
		 * is the fiber that is about to block on this latch's fd, regardless of
		 * where/when the latch was OwnLatch'd (OwnLatch at InitProcess can run
		 * in a context where xtc_self() is not yet the final owner).  Only the
		 * WL_LATCH_SET latch matters for cross-thread wake.
		 */
		if (set->latch != NULL)
		{
			xtc_pid_t	self = xtc_self();

			if (!xtc_pid_is_none(self))
			{
				set->latch->owner_fiber_valid = true;
				set->latch->owner_fiber_loop = self.loop_id;
				set->latch->owner_fiber_local = self.local_id;
				set->latch->owner_fiber_gen = self.gen;
			}
		}

		/* Wait for the epoll fd to become readable (or timeout). */
		wl = xtc_pg_wait_fd(set->epoll_fd, WL_SOCKET_READABLE,
							(long) cur_timeout);
		if (wl & WL_TIMEOUT)
			return -1;			/* timeout occurred */
		/* Harvest ready events without blocking. */
		rc = epoll_wait(set->epoll_fd, set->epoll_ret_events,
						Min(nevents, set->nevents_space), 0);
		goto xtc_have_rc;
	}
#endif

	/* Sleep */
	rc = epoll_wait(set->epoll_fd, set->epoll_ret_events,
					Min(nevents, set->nevents_space), cur_timeout);

#ifdef USE_XTC_CARRIER
xtc_have_rc:
#endif

	/* Check return code */
	if (rc < 0)
	{
		/* EINTR is okay, otherwise complain */
		if (errno != EINTR)
		{
			waiting = false;
			ereport(ERROR,
					(errcode_for_socket_access(),
					 errmsg("%s() failed: %m",
							"epoll_wait")));
		}
		return 0;
	}
	else if (rc == 0)
	{
		/* timeout exceeded */
		return -1;
	}

	/*
	 * At least one event occurred, iterate over the returned epoll events
	 * until they're either all processed, or we've returned all the events
	 * the caller desired.
	 */
	for (cur_epoll_event = set->epoll_ret_events;
		 cur_epoll_event < (set->epoll_ret_events + rc) &&
		 returned_events < nevents;
		 cur_epoll_event++)
	{
		/* epoll's data pointer is set to the associated WaitEvent */
		cur_event = (WaitEvent *) cur_epoll_event->data.ptr;

		occurred_events->pos = cur_event->pos;
		occurred_events->user_data = cur_event->user_data;
		occurred_events->events = 0;

		if (cur_event->events == WL_LATCH_SET &&
			cur_epoll_event->events & (EPOLLIN | EPOLLERR | EPOLLHUP))
		{
			/* Drain the signalfd. */
			drain();

			if (set->latch && set->latch->maybe_sleeping && set->latch->is_set)
			{
				occurred_events->fd = PGINVALID_SOCKET;
				occurred_events->events = WL_LATCH_SET;
				occurred_events++;
				returned_events++;
			}
		}
		else if (cur_event->events == WL_POSTMASTER_DEATH &&
				 cur_epoll_event->events & (EPOLLIN | EPOLLERR | EPOLLHUP))
		{
			/*
			 * We expect an EPOLLHUP when the remote end is closed, but
			 * because we don't expect the pipe to become readable or to have
			 * any errors either, treat those cases as postmaster death, too.
			 *
			 * Be paranoid about a spurious event signaling the postmaster as
			 * being dead.  There have been reports about that happening with
			 * older primitives (select(2) to be specific), and a spurious
			 * WL_POSTMASTER_DEATH event would be painful. Re-checking doesn't
			 * cost much.
			 */
			if (!PostmasterIsAliveInternal())
			{
				if (set->exit_on_postmaster_death)
					proc_exit(1);
				occurred_events->fd = PGINVALID_SOCKET;
				occurred_events->events = WL_POSTMASTER_DEATH;
				occurred_events++;
				returned_events++;
			}
		}
		else if (cur_event->events & (WL_SOCKET_READABLE |
									  WL_SOCKET_WRITEABLE |
									  WL_SOCKET_CLOSED))
		{
			Assert(cur_event->fd != PGINVALID_SOCKET);

			if ((cur_event->events & WL_SOCKET_READABLE) &&
				(cur_epoll_event->events & (EPOLLIN | EPOLLERR | EPOLLHUP)))
			{
				/* data available in socket, or EOF */
				occurred_events->events |= WL_SOCKET_READABLE;
			}

			if ((cur_event->events & WL_SOCKET_WRITEABLE) &&
				(cur_epoll_event->events & (EPOLLOUT | EPOLLERR | EPOLLHUP)))
			{
				/* writable, or EOF */
				occurred_events->events |= WL_SOCKET_WRITEABLE;
			}

			if ((cur_event->events & WL_SOCKET_CLOSED) &&
				(cur_epoll_event->events & (EPOLLRDHUP | EPOLLERR | EPOLLHUP)))
			{
				/* remote peer shut down, or error */
				occurred_events->events |= WL_SOCKET_CLOSED;
			}

			if (occurred_events->events != 0)
			{
				occurred_events->fd = cur_event->fd;
				occurred_events++;
				returned_events++;
			}
		}
	}

	return returned_events;
}

#elif defined(WAIT_USE_KQUEUE)

/*
 * Wait using kevent(2) on BSD-family systems and macOS.
 *
 * For now this mirrors the epoll code, but in future it could modify the fd
 * set in the same call to kevent as it uses for waiting instead of doing that
 * with separate system calls.
 */
static int
WaitEventSetWaitBlock(WaitEventSet *set, int cur_timeout,
					  WaitEvent *occurred_events, int nevents)
{
	int			returned_events = 0;
	int			rc;
	WaitEvent  *cur_event;
	struct kevent *cur_kqueue_event;
	struct timespec timeout;
	struct timespec *timeout_p;

	if (cur_timeout < 0)
		timeout_p = NULL;
	else
	{
		timeout.tv_sec = cur_timeout / 1000;
		timeout.tv_nsec = (cur_timeout % 1000) * 1000000;
		timeout_p = &timeout;
	}

	/*
	 * Report postmaster events discovered by WaitEventAdjustKqueue() or an
	 * earlier call to WaitEventSetWait().
	 */
	if (unlikely(set->report_postmaster_not_running))
	{
		if (set->exit_on_postmaster_death)
			proc_exit(1);
		occurred_events->fd = PGINVALID_SOCKET;
		occurred_events->events = WL_POSTMASTER_DEATH;
		return 1;
	}

	/* Sleep */
	rc = kevent(set->kqueue_fd, NULL, 0,
				set->kqueue_ret_events,
				Min(nevents, set->nevents_space),
				timeout_p);

	/* Check return code */
	if (rc < 0)
	{
		/* EINTR is okay, otherwise complain */
		if (errno != EINTR)
		{
			waiting = false;
			ereport(ERROR,
					(errcode_for_socket_access(),
					 errmsg("%s() failed: %m",
							"kevent")));
		}
		return 0;
	}
	else if (rc == 0)
	{
		/* timeout exceeded */
		return -1;
	}

	/*
	 * At least one event occurred, iterate over the returned kqueue events
	 * until they're either all processed, or we've returned all the events
	 * the caller desired.
	 */
	for (cur_kqueue_event = set->kqueue_ret_events;
		 cur_kqueue_event < (set->kqueue_ret_events + rc) &&
		 returned_events < nevents;
		 cur_kqueue_event++)
	{
		/* kevent's udata points to the associated WaitEvent */
		cur_event = AccessWaitEvent(cur_kqueue_event);
		if (cur_event == NULL &&
			cur_kqueue_event->filter == EVFILT_USER &&
			cur_kqueue_event->ident == WL_LATCH_SET &&
			set->latch != NULL)
			cur_event = &set->events[set->latch_pos];
		if (cur_event == NULL)
			continue;

		occurred_events->pos = cur_event->pos;
		occurred_events->user_data = cur_event->user_data;
		occurred_events->events = 0;

		if (cur_event->events == WL_LATCH_SET &&
			(cur_kqueue_event->filter == EVFILT_SIGNAL ||
			 cur_kqueue_event->filter == EVFILT_USER))
		{
			if (set->latch && set->latch->maybe_sleeping && set->latch->is_set)
			{
				occurred_events->fd = PGINVALID_SOCKET;
				occurred_events->events = WL_LATCH_SET;
				occurred_events++;
				returned_events++;
			}
		}
		else if (cur_event->events == WL_POSTMASTER_DEATH &&
				 cur_kqueue_event->filter == EVFILT_PROC &&
				 (cur_kqueue_event->fflags & NOTE_EXIT) != 0)
		{
			/*
			 * The kernel will tell this kqueue object only once about the
			 * exit of the postmaster, so let's remember that for next time so
			 * that we provide level-triggered semantics.
			 */
			set->report_postmaster_not_running = true;

			if (set->exit_on_postmaster_death)
				proc_exit(1);
			occurred_events->fd = PGINVALID_SOCKET;
			occurred_events->events = WL_POSTMASTER_DEATH;
			occurred_events++;
			returned_events++;
		}
		else if (cur_event->events & (WL_SOCKET_READABLE |
									  WL_SOCKET_WRITEABLE |
									  WL_SOCKET_CLOSED))
		{
			Assert(cur_event->fd >= 0);

			if ((cur_event->events & WL_SOCKET_READABLE) &&
				(cur_kqueue_event->filter == EVFILT_READ))
			{
				/* readable, or EOF */
				occurred_events->events |= WL_SOCKET_READABLE;
			}

			if ((cur_event->events & WL_SOCKET_CLOSED) &&
				(cur_kqueue_event->filter == EVFILT_READ) &&
				(cur_kqueue_event->flags & EV_EOF))
			{
				/* the remote peer has shut down */
				occurred_events->events |= WL_SOCKET_CLOSED;
			}

			if ((cur_event->events & WL_SOCKET_WRITEABLE) &&
				(cur_kqueue_event->filter == EVFILT_WRITE))
			{
				/* writable, or EOF */
				occurred_events->events |= WL_SOCKET_WRITEABLE;
			}

			if (occurred_events->events != 0)
			{
				occurred_events->fd = cur_event->fd;
				occurred_events++;
				returned_events++;
			}
		}
	}

	return returned_events;
}

#elif defined(WAIT_USE_POLL)

/*
 * Wait using poll(2).
 *
 * This allows to receive readiness notifications for several events at once,
 * but requires iterating through all of set->pollfds.
 */
static inline int
WaitEventSetWaitBlock(WaitEventSet *set, int cur_timeout,
					  WaitEvent *occurred_events, int nevents)
{
	int			returned_events = 0;
	int			rc;
	WaitEvent  *cur_event;
	struct pollfd *cur_pollfd;

	/* Sleep */
	rc = poll(set->pollfds, set->nevents, cur_timeout);

	/* Check return code */
	if (rc < 0)
	{
		/* EINTR is okay, otherwise complain */
		if (errno != EINTR)
		{
			waiting = false;
			ereport(ERROR,
					(errcode_for_socket_access(),
					 errmsg("%s() failed: %m",
							"poll")));
		}
		return 0;
	}
	else if (rc == 0)
	{
		/* timeout exceeded */
		return -1;
	}

	for (cur_event = set->events, cur_pollfd = set->pollfds;
		 cur_event < (set->events + set->nevents) &&
		 returned_events < nevents;
		 cur_event++, cur_pollfd++)
	{
		/* no activity on this FD, skip */
		if (cur_pollfd->revents == 0)
			continue;

		occurred_events->pos = cur_event->pos;
		occurred_events->user_data = cur_event->user_data;
		occurred_events->events = 0;

		if (cur_event->events == WL_LATCH_SET &&
			(cur_pollfd->revents & (POLLIN | POLLHUP | POLLERR | POLLNVAL)))
		{
			/* There's data in the self-pipe, clear it. */
			drain();

			if (set->latch && set->latch->maybe_sleeping && set->latch->is_set)
			{
				occurred_events->fd = PGINVALID_SOCKET;
				occurred_events->events = WL_LATCH_SET;
				occurred_events++;
				returned_events++;
			}
		}
		else if (cur_event->events == WL_POSTMASTER_DEATH &&
				 (cur_pollfd->revents & (POLLIN | POLLHUP | POLLERR | POLLNVAL)))
		{
			/*
			 * We expect a POLLHUP when the remote end is closed, but because
			 * we don't expect the pipe to become readable or to have any
			 * errors either, treat those cases as postmaster death, too.
			 *
			 * Be paranoid about a spurious event signaling the postmaster as
			 * being dead.  There have been reports about that happening with
			 * older primitives (select(2) to be specific), and a spurious
			 * WL_POSTMASTER_DEATH event would be painful.  Re-checking
			 * doesn't cost much.
			 */
			if (!PostmasterIsAliveInternal())
			{
				if (set->exit_on_postmaster_death)
					proc_exit(1);
				occurred_events->fd = PGINVALID_SOCKET;
				occurred_events->events = WL_POSTMASTER_DEATH;
				occurred_events++;
				returned_events++;
			}
		}
		else if (cur_event->events & (WL_SOCKET_READABLE |
									  WL_SOCKET_WRITEABLE |
									  WL_SOCKET_CLOSED))
		{
			int			errflags = POLLHUP | POLLERR | POLLNVAL;

			Assert(cur_event->fd >= PGINVALID_SOCKET);

			if ((cur_event->events & WL_SOCKET_READABLE) &&
				(cur_pollfd->revents & (POLLIN | errflags)))
			{
				/* data available in socket, or EOF */
				occurred_events->events |= WL_SOCKET_READABLE;
			}

			if ((cur_event->events & WL_SOCKET_WRITEABLE) &&
				(cur_pollfd->revents & (POLLOUT | errflags)))
			{
				/* writeable, or EOF */
				occurred_events->events |= WL_SOCKET_WRITEABLE;
			}

#ifdef POLLRDHUP
			if ((cur_event->events & WL_SOCKET_CLOSED) &&
				(cur_pollfd->revents & (POLLRDHUP | errflags)))
			{
				/* remote peer closed, or error */
				occurred_events->events |= WL_SOCKET_CLOSED;
			}
#endif

			if (occurred_events->events != 0)
			{
				occurred_events->fd = cur_event->fd;
				occurred_events++;
				returned_events++;
			}
		}
	}
	return returned_events;
}

#elif defined(WAIT_USE_WIN32)

/*
 * Wait using Windows' WaitForMultipleObjects().  Each call only "consumes" one
 * event, so we keep calling until we've filled up our output buffer to match
 * the behavior of the other implementations.
 *
 * https://blogs.msdn.microsoft.com/oldnewthing/20150409-00/?p=44273
 */
static inline int
WaitEventSetWaitBlock(WaitEventSet *set, int cur_timeout,
					  WaitEvent *occurred_events, int nevents)
{
	int			returned_events = 0;
	DWORD		rc;
	WaitEvent  *cur_event;

	/* Reset any wait events that need it */
	for (cur_event = set->events;
		 cur_event < (set->events + set->nevents);
		 cur_event++)
	{
		if (cur_event->reset)
		{
			WaitEventAdjustWin32(set, cur_event);
			cur_event->reset = false;
		}

		/*
		 * We associate the socket with a new event handle for each
		 * WaitEventSet.  FD_CLOSE is only generated once if the other end
		 * closes gracefully.  Therefore we might miss the FD_CLOSE
		 * notification, if it was delivered to another event after we stopped
		 * waiting for it.  Close that race by peeking for EOF after setting
		 * up this handle to receive notifications, and before entering the
		 * sleep.
		 *
		 * XXX If we had one event handle for the lifetime of a socket, we
		 * wouldn't need this.
		 */
		if (cur_event->events & WL_SOCKET_READABLE)
		{
			char		c;
			WSABUF		buf;
			DWORD		received;
			DWORD		flags;

			buf.buf = &c;
			buf.len = 1;
			flags = MSG_PEEK;
			if (WSARecv(cur_event->fd, &buf, 1, &received, &flags, NULL, NULL) == 0)
			{
				occurred_events->pos = cur_event->pos;
				occurred_events->user_data = cur_event->user_data;
				occurred_events->events = WL_SOCKET_READABLE;
				occurred_events->fd = cur_event->fd;
				return 1;
			}
		}

		/*
		 * Windows does not guarantee to log an FD_WRITE network event
		 * indicating that more data can be sent unless the previous send()
		 * failed with WSAEWOULDBLOCK.  While our caller might well have made
		 * such a call, we cannot assume that here.  Therefore, if waiting for
		 * write-ready, force the issue by doing a dummy send().  If the dummy
		 * send() succeeds, assume that the socket is in fact write-ready, and
		 * return immediately.  Also, if it fails with something other than
		 * WSAEWOULDBLOCK, return a write-ready indication to let our caller
		 * deal with the error condition.
		 */
		if (cur_event->events & WL_SOCKET_WRITEABLE)
		{
			char		c;
			WSABUF		buf;
			DWORD		sent;
			int			r;

			buf.buf = &c;
			buf.len = 0;

			r = WSASend(cur_event->fd, &buf, 1, &sent, 0, NULL, NULL);
			if (r == 0 || WSAGetLastError() != WSAEWOULDBLOCK)
			{
				occurred_events->pos = cur_event->pos;
				occurred_events->user_data = cur_event->user_data;
				occurred_events->events = WL_SOCKET_WRITEABLE;
				occurred_events->fd = cur_event->fd;
				return 1;
			}
		}
	}

	/*
	 * Sleep.
	 *
	 * Need to wait for ->nevents + 1, because signal handle is in [0].
	 */
	rc = WaitForMultipleObjects(set->nevents + 1, set->handles, FALSE,
								cur_timeout);

	/* Check return code */
	if (rc == WAIT_FAILED)
		elog(ERROR, "WaitForMultipleObjects() failed: error code %lu",
			 GetLastError());
	else if (rc == WAIT_TIMEOUT)
	{
		/* timeout exceeded */
		return -1;
	}

	if (rc == WAIT_OBJECT_0)
	{
		/* Service newly-arrived signals */
		pgwin32_dispatch_queued_signals();
		return 0;				/* retry */
	}

	/*
	 * With an offset of one, due to the always present pgwin32_signal_event,
	 * the handle offset directly corresponds to a wait event.
	 */
	cur_event = (WaitEvent *) &set->events[rc - WAIT_OBJECT_0 - 1];

	for (;;)
	{
		int			next_pos;
		int			count;

		occurred_events->pos = cur_event->pos;
		occurred_events->user_data = cur_event->user_data;
		occurred_events->events = 0;

		if (cur_event->events == WL_LATCH_SET)
		{
			/*
			 * We cannot use set->latch->event to reset the fired event if we
			 * aren't waiting on this latch now.
			 */
			if (!ResetEvent(set->handles[cur_event->pos + 1]))
				elog(ERROR, "ResetEvent failed: error code %lu", GetLastError());

			if (set->latch && set->latch->maybe_sleeping && set->latch->is_set)
			{
				occurred_events->fd = PGINVALID_SOCKET;
				occurred_events->events = WL_LATCH_SET;
				occurred_events++;
				returned_events++;
			}
		}
		else if (cur_event->events == WL_POSTMASTER_DEATH)
		{
			/*
			 * Postmaster apparently died.  Since the consequences of falsely
			 * returning WL_POSTMASTER_DEATH could be pretty unpleasant, we
			 * take the trouble to positively verify this with
			 * PostmasterIsAlive(), even though there is no known reason to
			 * think that the event could be falsely set on Windows.
			 */
			if (!PostmasterIsAliveInternal())
			{
				if (set->exit_on_postmaster_death)
					proc_exit(1);
				occurred_events->fd = PGINVALID_SOCKET;
				occurred_events->events = WL_POSTMASTER_DEATH;
				occurred_events++;
				returned_events++;
			}
		}
		else if (cur_event->events & WL_SOCKET_MASK)
		{
			WSANETWORKEVENTS resEvents;
			HANDLE		handle = set->handles[cur_event->pos + 1];

			Assert(cur_event->fd);

			occurred_events->fd = cur_event->fd;

			ZeroMemory(&resEvents, sizeof(resEvents));
			if (WSAEnumNetworkEvents(cur_event->fd, handle, &resEvents) != 0)
				elog(ERROR, "failed to enumerate network events: error code %d",
					 WSAGetLastError());
			if ((cur_event->events & WL_SOCKET_READABLE) &&
				(resEvents.lNetworkEvents & FD_READ))
			{
				/* data available in socket */
				occurred_events->events |= WL_SOCKET_READABLE;

				/*------
				 * WaitForMultipleObjects doesn't guarantee that a read event
				 * will be returned if the latch is set at the same time.  Even
				 * if it did, the caller might drop that event expecting it to
				 * reoccur on next call.  So, we must force the event to be
				 * reset if this WaitEventSet is used again in order to avoid
				 * an indefinite hang.
				 *
				 * Refer
				 * https://msdn.microsoft.com/en-us/library/windows/desktop/ms741576(v=vs.85).aspx
				 * for the behavior of socket events.
				 *------
				 */
				cur_event->reset = true;
			}
			if ((cur_event->events & WL_SOCKET_WRITEABLE) &&
				(resEvents.lNetworkEvents & FD_WRITE))
			{
				/* writeable */
				occurred_events->events |= WL_SOCKET_WRITEABLE;
			}
			if ((cur_event->events & WL_SOCKET_CONNECTED) &&
				(resEvents.lNetworkEvents & FD_CONNECT))
			{
				/* connected */
				occurred_events->events |= WL_SOCKET_CONNECTED;
			}
			if ((cur_event->events & WL_SOCKET_ACCEPT) &&
				(resEvents.lNetworkEvents & FD_ACCEPT))
			{
				/* incoming connection could be accepted */
				occurred_events->events |= WL_SOCKET_ACCEPT;
			}
			if (resEvents.lNetworkEvents & FD_CLOSE)
			{
				/* EOF/error, so signal all caller-requested socket flags */
				occurred_events->events |= (cur_event->events & WL_SOCKET_MASK);
			}

			if (occurred_events->events != 0)
			{
				occurred_events++;
				returned_events++;
			}
		}

		/* Is the output buffer full? */
		if (returned_events == nevents)
			break;

		/* Have we run out of possible events? */
		next_pos = cur_event->pos + 1;
		if (next_pos == set->nevents)
			break;

		/*
		 * Poll the rest of the event handles in the array starting at
		 * next_pos being careful to skip over the initial signal handle too.
		 * This time we use a zero timeout.
		 */
		count = set->nevents - next_pos;
		rc = WaitForMultipleObjects(count,
									set->handles + 1 + next_pos,
									false,
									0);

		/*
		 * We don't distinguish between errors and WAIT_TIMEOUT here because
		 * we already have events to report.
		 */
		if (rc < WAIT_OBJECT_0 || rc >= WAIT_OBJECT_0 + count)
			break;

		/* We have another event to decode. */
		cur_event = &set->events[next_pos + (rc - WAIT_OBJECT_0)];
	}

	return returned_events;
}
#endif

/*
 * Return whether the current build options can report WL_SOCKET_CLOSED.
 */
bool
WaitEventSetCanReportClosed(void)
{
#if (defined(WAIT_USE_POLL) && defined(POLLRDHUP)) || \
	defined(WAIT_USE_EPOLL) || \
	defined(WAIT_USE_KQUEUE)
	return true;
#else
	return false;
#endif
}

/*
 * Get the number of wait events registered in a given WaitEventSet.
 */
int
GetNumRegisteredWaitEvents(WaitEventSet *set)
{
	return set->nevents;
}

static bool
WaitEventLatchFdNeedsRefresh(WaitEvent *event)
{
	if (event->events != WL_LATCH_SET)
		return false;

#if defined(WAIT_USE_SELF_PIPE)
	return event->fd != selfpipe_readfd;
#elif defined(WAIT_USE_SIGNALFD)
	return event->fd != signal_fd;
#else
	return false;
#endif
}

static void
WaitEventRefreshLatchFd(WaitEventSet *set, WaitEvent *event)
{
#if defined(WAIT_USE_SELF_PIPE) || defined(WAIT_USE_SIGNALFD)
	pgsocket	oldfd = event->fd;
#if defined(WAIT_USE_SELF_PIPE)
	pgsocket	newfd = selfpipe_readfd;
#elif defined(WAIT_USE_SIGNALFD)
	pgsocket	newfd = signal_fd;
#endif

	if (oldfd == newfd)
		return;

#if defined(WAIT_USE_EPOLL)
	{
		WaitEvent	old_event = *event;

		event->fd = newfd;
		if (oldfd != PGINVALID_SOCKET)
			WaitEventAdjustEpoll(set, &old_event, EPOLL_CTL_DEL);
		WaitEventAdjustEpoll(set, event, EPOLL_CTL_ADD);
	}
#elif defined(WAIT_USE_POLL)
	event->fd = newfd;
	WaitEventAdjustPoll(set, event);
#else
	event->fd = newfd;
#endif
#else
	(void) set;
	(void) event;
#endif
}

#if defined(WAIT_USE_SELF_PIPE)

/*
 * SetLatch uses SIGURG to wake up the process waiting on the latch.
 *
 * Wake up WaitLatch, if we're waiting.
 */
static void
latch_sigurg_handler(SIGNAL_ARGS)
{
	if (waiting)
		sendSelfPipeByte();
}

/* Send one byte to the self-pipe, to wake up WaitLatch */
static void
sendSelfPipeByte(void)
{
	sendSelfPipeByteToFd(selfpipe_writefd);
}

static void
sendSelfPipeByteToFd(int fd)
{
	ssize_t		rc;
	char		dummy = 0;

retry:
	rc = write(fd, &dummy, 1);
	if (rc < 0)
	{
		/* If interrupted by signal, just retry */
		if (errno == EINTR)
			goto retry;

		/*
		 * If the pipe is full, we don't need to retry, the data that's there
		 * already is enough to wake up WaitLatch.
		 */
		if (errno == EAGAIN || errno == EWOULDBLOCK)
			return;

		/*
		 * Oops, the write() failed for some other reason. We might be in a
		 * signal handler, so it's not safe to elog(). We have no choice but
		 * silently ignore the error.
		 */
		return;
	}
}

#endif

#if defined(WAIT_USE_SELF_PIPE) || defined(WAIT_USE_SIGNALFD)

/*
 * Read all available data from self-pipe or signalfd.
 *
 * Note: this is only called when waiting = true.  If it fails and doesn't
 * return, it must reset that flag first (though ideally, this will never
 * happen).
 */
static void
drain(void)
{
	char		buf[1024];
	ssize_t		rc;
	int			fd;

#ifdef WAIT_USE_SELF_PIPE
	fd = selfpipe_readfd;
#else
	fd = signal_fd;
#endif

	for (;;)
	{
		rc = read(fd, buf, sizeof(buf));
		if (rc < 0)
		{
			if (errno == EAGAIN || errno == EWOULDBLOCK)
				break;			/* the descriptor is empty */
			else if (errno == EINTR)
				continue;		/* retry */
			else
			{
				waiting = false;
#ifdef WAIT_USE_SELF_PIPE
				elog(ERROR, "read() on self-pipe failed: %m");
#else
				elog(ERROR, "read() on signalfd failed: %m");
#endif
			}
		}
		else if (rc == 0)
		{
			waiting = false;
#ifdef WAIT_USE_SELF_PIPE
			elog(ERROR, "unexpected EOF on self-pipe");
#else
			elog(ERROR, "unexpected EOF on signalfd");
#endif
		}
		else if (rc < sizeof(buf))
		{
			/* we successfully drained the pipe; no need to read() again */
			break;
		}
		/* else buffer wasn't big enough, so read again */
	}
}

#endif

static void
ResOwnerReleaseWaitEventSet(Datum res)
{
	WaitEventSet *set = (WaitEventSet *) DatumGetPointer(res);

	Assert(set->owner != NULL);
	set->owner = NULL;
	FreeWaitEventSet(set);
}

#ifndef WIN32
int
GetWaitEventSetLatchWakeupFd(void)
{
#if defined(WAIT_USE_SELF_PIPE)
	return selfpipe_writefd;
#else
	return -1;
#endif
}

/*
 * Wake up my process if it's currently sleeping in WaitEventSetWaitBlock()
 *
 * NB: be sure to save and restore errno around it.  (That's standard practice
 * in most signal handlers, of course, but we used to omit it in handlers that
 * only set a flag.) XXX
 *
 * NB: this function is called from critical sections and signal handlers so
 * throwing an error is not a good idea.
 *
 * On Windows, Latch uses SetEvent directly and this is not used.
 */
void
WakeupMyProc(void)
{
#if defined(WAIT_USE_SELF_PIPE)
	if (waiting)
		sendSelfPipeByte();
#else
	if (waiting)
		kill(MyProcPid, SIGURG);
#endif
}

void
WakeupOtherProcFd(int fd)
{
#if defined(WAIT_USE_SELF_PIPE)
	if (fd >= 0)
		sendSelfPipeByteToFd(fd);
#elif defined(WAIT_USE_KQUEUE)
	if (fd >= 0)
	{
		struct kevent k_ev;

		EV_SET(&k_ev, WL_LATCH_SET, EVFILT_USER, 0, NOTE_TRIGGER, 0, NULL);
		(void) kevent(fd, &k_ev, 1, NULL, 0, NULL);
	}
#else
	(void) fd;
#endif
}

/* Similar to WakeupMyProc, but wake up another process */
void
WakeupOtherProc(int pid)
{
	kill(pid, SIGURG);
}
#endif
