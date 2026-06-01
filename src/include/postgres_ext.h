/*-------------------------------------------------------------------------
 *
 * postgres_ext.h
 *
 *	   This file contains declarations of things that are visible everywhere
 *	in PostgreSQL *and* are visible to clients of frontend interface libraries.
 *	For example, the Oid type is part of the API of libpq and other libraries.
 *
 *	   Declarations which are specific to a particular interface should
 *	go in the header file for that interface (such as libpq-fe.h).  This
 *	file is only for fundamental Postgres declarations.
 *
 *	   User-written C functions don't count as "external to Postgres."
 *	Those function much as local modifications to the backend itself, and
 *	use header files that are otherwise internal to Postgres to interface
 *	with the backend.
 *
 * src/include/postgres_ext.h
 *
 *-------------------------------------------------------------------------
 */
/* IWYU pragma: always_keep */

#ifndef POSTGRES_EXT_H
#define POSTGRES_EXT_H

#include <stdint.h>

/*
 * Global-variable storage-lifetime annotations.
 *
 * PostgreSQL uses global variables for many different things: some are
 * effectively constants initialized at postmaster startup, some hold GUC
 * values, some hold per-session state.  These annotations document how a
 * global is used.  They are checked by the pgguclifetimes tool (in
 * src/tools/pgguclifetimes).  When the compiler lacks
 * __attribute__((annotate(...))) they reduce to nothing.
 *
 * When PostgreSQL runs in the multithreaded thread-per-connection model,
 * the per-session variables (session_local) become thread-local.
 *
 * GUC-specific lifetime annotations (internal_guc, postmaster_guc,
 * session_guc, ...) document which GUC context governs a global that backs a
 * GUC.  They live here (rather than in postgres.h) so that they are available
 * in translation units that include only c.h / postgres_ext.h, such as the
 * src/port files that pull in miscadmin.h without postgres.h.
 */
/*
 * session_local (and the GUC annotations that become thread-local in the
 * multithreaded model) must expand to a thread-local storage qualifier that
 * the active compiler accepts: GCC/Clang spell it __thread.
 *
 * MSVC is deliberately excluded: __declspec(thread) cannot be combined with
 * __declspec(dllimport)/dllexport (C2492), and MSVC cannot resolve a
 * thread-local symbol referenced by name across a DLL boundary at link time.
 * Several of these globals (e.g. MyPendingInterrupts, CheckForInterruptsMask)
 * are read directly by loadable modules.  Since the multithreaded build is not
 * enabled on Windows, the annotation expands to nothing there: the variable
 * stays an ordinary process-global exported via PGDLLIMPORT, with no change in
 * the process model.  The session_local intent is still recorded in source for
 * the platforms that actually thread.
 */
#if defined(_MSC_VER)
#define pg_attribute_thread_local
#else
#define pg_attribute_thread_local __thread
#endif

#if defined(__has_attribute) && __has_attribute (annotate)
#define pg_global __attribute__((annotate("pg_global")))
#define dynamic_singleton __attribute__((annotate("dynamic_singleton")))
#define static_singleton __attribute__((annotate("static_singleton")))
#define session_local pg_attribute_thread_local __attribute__((annotate("session_local")))
#define internal_guc __attribute__((annotate("internal_guc")))
#define postmaster_guc __attribute__((annotate("postmaster_guc")))
#define session_guc pg_attribute_thread_local __attribute__((annotate("session_guc")))
#define sighup_guc pg_attribute_thread_local __attribute__((annotate("sighup_guc")))
#define suset_guc pg_attribute_thread_local __attribute__((annotate("suset_guc")))
#define userset_guc pg_attribute_thread_local __attribute__((annotate("userset_guc")))
#else
#define pg_global
#define dynamic_singleton
#define static_singleton
#define session_local pg_attribute_thread_local
#define internal_guc
#define postmaster_guc
#define session_guc pg_attribute_thread_local
#define sighup_guc pg_attribute_thread_local
#define suset_guc pg_attribute_thread_local
#define userset_guc pg_attribute_thread_local
#endif

/*
 * Object ID is a fundamental type in Postgres.
 */
typedef unsigned int Oid;

#ifdef __cplusplus
#define InvalidOid		(Oid(0))
#else
#define InvalidOid		((Oid) 0)
#endif

#define OID_MAX  UINT_MAX
/* you will need to include <limits.h> to use the above #define */

#define atooid(x) ((Oid) strtoul((x), NULL, 10))
/* the above needs <stdlib.h> */


/* deprecated name for int64_t, formerly used in client API declarations */
typedef int64_t pg_int64;

/*
 * Identifiers of error message fields.  Kept here to keep common
 * between frontend and backend, and also to export them to libpq
 * applications.
 */
#define PG_DIAG_SEVERITY		'S'
#define PG_DIAG_SEVERITY_NONLOCALIZED 'V'
#define PG_DIAG_SQLSTATE		'C'
#define PG_DIAG_MESSAGE_PRIMARY 'M'
#define PG_DIAG_MESSAGE_DETAIL	'D'
#define PG_DIAG_MESSAGE_HINT	'H'
#define PG_DIAG_STATEMENT_POSITION 'P'
#define PG_DIAG_INTERNAL_POSITION 'p'
#define PG_DIAG_INTERNAL_QUERY	'q'
#define PG_DIAG_CONTEXT			'W'
#define PG_DIAG_SCHEMA_NAME		's'
#define PG_DIAG_TABLE_NAME		't'
#define PG_DIAG_COLUMN_NAME		'c'
#define PG_DIAG_DATATYPE_NAME	'd'
#define PG_DIAG_CONSTRAINT_NAME 'n'
#define PG_DIAG_SOURCE_FILE		'F'
#define PG_DIAG_SOURCE_LINE		'L'
#define PG_DIAG_SOURCE_FUNCTION 'R'

#endif							/* POSTGRES_EXT_H */
