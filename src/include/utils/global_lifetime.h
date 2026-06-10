/*-------------------------------------------------------------------------
 *
 * global_lifetime.h
 *	  No-op annotations for classifying backend global variable lifetime.
 *
 * These annotations are intentionally code-generation-neutral.  They make
 * mutable process globals visible to review and static tooling while later
 * phases migrate state onto explicit runtime/backend/session/execution
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

#endif							/* GLOBAL_LIFETIME_H */
