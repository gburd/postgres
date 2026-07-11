/*-------------------------------------------------------------------------
 *
 * pg_thread.c
 *	  Backend thread portability layer.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/port/pg_thread.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <errno.h>
#include <stdlib.h>

#ifdef WIN32
#include <process.h>
#endif

#include "libpq/pqsignal.h"
#include "port/pg_thread.h"

#define PG_THREAD_NAME_MAX 64

#ifndef WIN32
/*
 * Secondary pthread stacks can be much smaller than the process stack limit
 * used by max_stack_depth.  Keep backend threads large enough for PostgreSQL's
 * existing stack-depth guard to error before the platform stack is exhausted.
 */
#define PG_THREAD_STACK_SIZE ((size_t) 8 * 1024 * 1024)
#endif

typedef struct PgThreadStartData
{
	PgThreadRoutine routine;
	void	   *arg;
	char		name[PG_THREAD_NAME_MAX];
} PgThreadStartData;

#ifdef WIN32
static unsigned __stdcall pg_thread_start(void *arg);
#else
static void *pg_thread_start(void *arg);
#endif

int
pg_thread_create(PgThread *thread, const char *name,
				 PgThreadRoutine routine, void *arg)
{
	PgThreadStartData *start_data;

	Assert(thread != NULL);
	Assert(routine != NULL);

	start_data = malloc(sizeof(PgThreadStartData));
	if (start_data == NULL)
		return ENOMEM;

	start_data->routine = routine;
	start_data->arg = arg;
	strlcpy(start_data->name, name != NULL ? name : "postgres",
			sizeof(start_data->name));

#ifdef WIN32
	thread->handle = (HANDLE) _beginthreadex(NULL, 0, pg_thread_start,
											 start_data, 0,
											 &thread->thread_id);
	if (thread->handle == NULL)
	{
		int			save_errno = errno != 0 ? errno : EAGAIN;

		free(start_data);
		return save_errno;
	}
#else
	{
		pthread_attr_t attr;
		int			rc;
		sigset_t	save_mask;

		rc = pthread_attr_init(&attr);
		if (rc != 0)
		{
			free(start_data);
			return rc;
		}

		rc = pthread_attr_setstacksize(&attr, PG_THREAD_STACK_SIZE);

		/*
		 * A new thread inherits the creating thread's signal mask.  The
		 * postmaster runs ServerLoop with signals unblocked, so without care a
		 * process-directed signal (e.g. SIGCHLD) could be delivered to the new
		 * thread in the window before it has run its own signal setup and set
		 * MyProcPid -- which trips Assert(MyProcPid) in wrapper_handler under
		 * cassert (and would otherwise be re-raised to reach the intended
		 * handler).  Block all signals across pthread_create() so the child
		 * starts fully blocked, then restore our own mask.  This mirrors
		 * fork_process(), which blocks signals around fork() for the same
		 * reason; the child unblocks once its per-backend signal handling is
		 * installed.
		 */
		if (rc == 0)
		{
			sigprocmask(SIG_SETMASK, &BlockSig, &save_mask);
			rc = pthread_create(&thread->thread, &attr, pg_thread_start,
								start_data);
			sigprocmask(SIG_SETMASK, &save_mask, NULL);
		}
		(void) pthread_attr_destroy(&attr);
		if (rc != 0)
		{
			free(start_data);
			return rc;
		}
	}
#endif

	return 0;
}

int
pg_thread_join(PgThread *thread)
{
	Assert(thread != NULL);

#ifdef WIN32
	if (WaitForSingleObject(thread->handle, INFINITE) == WAIT_FAILED)
		return EINVAL;

	CloseHandle(thread->handle);
	thread->handle = NULL;
	thread->thread_id = 0;
	return 0;
#else
	return pthread_join(thread->thread, NULL);
#endif
}

int
pg_thread_detach(PgThread *thread)
{
	Assert(thread != NULL);

#ifdef WIN32
	if (!CloseHandle(thread->handle))
		return EINVAL;

	thread->handle = NULL;
	thread->thread_id = 0;
	return 0;
#else
	return pthread_detach(thread->thread);
#endif
}

void
pg_thread_set_name(const char *name)
{
	if (name == NULL || name[0] == '\0')
		return;

#if defined(__APPLE__)
	(void) pthread_setname_np(name);
#elif defined(WIN32) && defined(_WIN32_WINNT) && _WIN32_WINNT >= 0x0A00
	{
		WCHAR		wide_name[PG_THREAD_NAME_MAX];
		int			wide_len;

		wide_len = MultiByteToWideChar(CP_UTF8, 0, name, -1, wide_name,
									   lengthof(wide_name));
		if (wide_len > 0)
			(void) SetThreadDescription(GetCurrentThread(), wide_name);
	}
#endif
}

void
pg_thread_exit(void)
{
#ifdef WIN32
	_endthreadex(0);
#else
	pthread_exit(NULL);
#endif

	pg_unreachable();
}

#ifdef WIN32
static unsigned __stdcall
pg_thread_start(void *arg)
#else
static void *
pg_thread_start(void *arg)
#endif
{
	PgThreadStartData *start_data = (PgThreadStartData *) arg;
	PgThreadRoutine routine = start_data->routine;
	void	   *routine_arg = start_data->arg;
	char		name[PG_THREAD_NAME_MAX];

	strlcpy(name, start_data->name, sizeof(name));
	free(start_data);

	pg_thread_set_name(name);
	routine(routine_arg);

#ifdef WIN32
	return 0;
#else
	return NULL;
#endif
}
