/*-------------------------------------------------------------------------
 *
 * pg_thread.h
 *	  Backend thread portability layer.
 *
 * This is intentionally small.  Phase 10 only needs to create one carrier
 * thread for one logical backend; richer scheduler primitives should live in
 * the runtime layer, not in this port wrapper.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/include/port/pg_thread.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_THREAD_H
#define PG_THREAD_H

#ifdef WIN32
#include <windows.h>
#else
#include "port/pg_pthread.h"
#endif

typedef void (*PgThreadRoutine) (void *arg);

typedef struct PgThread
{
#ifdef WIN32
	HANDLE		handle;
	unsigned	thread_id;
#else
	pthread_t	thread;
#endif
} PgThread;

extern int	pg_thread_create(PgThread *thread, const char *name,
							 PgThreadRoutine routine, void *arg);
extern int	pg_thread_join(PgThread *thread);
extern int	pg_thread_detach(PgThread *thread);
extern void pg_thread_set_name(const char *name);
pg_noreturn extern void pg_thread_exit(void);

#endif							/* PG_THREAD_H */
