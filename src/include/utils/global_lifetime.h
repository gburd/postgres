/*-------------------------------------------------------------------------
 *
 * global_lifetime.h
 *	  Annotations for classifying backend global variable lifetime.
 *
 * The PG_GLOBAL_* annotations are intentionally code-generation-neutral.  They
 * make mutable process globals visible to review and static tooling while
 * later phases migrate state onto explicit runtime/backend/session/execution
 * objects.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/include/utils/global_lifetime.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GLOBAL_LIFETIME_H
#define GLOBAL_LIFETIME_H

/*
 * Thread-local storage bridge for state that is private to one backend,
 * session, execution, carrier, or connection in the initial thread-per-session
 * runtime.  Pooled scheduling must move this state onto explicit owner
 * objects, but thread-local storage preserves today's process-per-session
 * semantics while multiple backends share an address space.
 */
#if defined(_MSC_VER)
#define PG_THREAD_LOCAL __declspec(thread)
#elif defined(__GNUC__) || defined(__clang__)
#define PG_THREAD_LOCAL __thread
#elif defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L
#define PG_THREAD_LOCAL _Thread_local
#else
#error "thread-local storage is required for multithreaded backend globals"
#endif

/*
 * Server/runtime-wide singleton state.  These variables are expected to remain
 * shared across logical backends in a threaded runtime.
 */
#define PG_GLOBAL_RUNTIME

/*
 * Immutable singleton state.  The scanner does not require annotations for
 * plain const objects, but this marker is available when classification is
 * helpful for review.
 */
#define PG_GLOBAL_IMMUTABLE

/*
 * Mutable singleton state whose ownership is intentionally singular, but not
 * naturally part of the runtime object yet.
 */
#define PG_GLOBAL_DYNAMIC

/* State that belongs to a logical backend. */
#define PG_GLOBAL_BACKEND

/* State that belongs to a user session. */
#define PG_GLOBAL_SESSION

/* State that belongs to one command or protected execution step. */
#define PG_GLOBAL_EXECUTION

/* State that belongs to the carrier running backend work. */
#define PG_GLOBAL_CARRIER

/* State that belongs to a client connection. */
#define PG_GLOBAL_CONNECTION

/* State stored in or directly representing shared memory. */
#define PG_GLOBAL_SHMEM

/*
 * A few historically global hot-path macros live in broad headers that cannot
 * include backend_runtime.h.  Keep the canonical TLS bridge declarations in
 * backend_runtime.h, and use this helper only for those narrow imports.
 */
#define PG_RUNTIME_BRIDGE_EXTERN(type, variable) \
extern PGDLLIMPORT PG_THREAD_LOCAL PG_GLOBAL_CARRIER type variable

#endif							/* GLOBAL_LIFETIME_H */
