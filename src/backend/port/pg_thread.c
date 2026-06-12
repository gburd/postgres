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

#include "port/pg_thread.h"

#define PG_THREAD_NAME_MAX 64

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
		int			rc;

		rc = pthread_create(&thread->thread, NULL, pg_thread_start, start_data);
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
