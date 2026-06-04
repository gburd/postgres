/*-------------------------------------------------------------------------
 *
 * mysession.h
 *	  Top-level per-session state aggregate (the "MySession" struct).
 *
 * Background
 * ----------
 * The threading re-derivation classifies every backend global as either
 * shared across the whole server (pg_global) or private to one logical
 * session (session_local, i.e. __thread under -Dmultithreaded=true).  F4
 * has been collapsing clusters of related session_local globals into named
 * per-subsystem structs (XxxState), each with a single session_local
 * instance.  See docs/threading/F4_SESSION_STATE.md.
 *
 * MySession is the top-level aggregate those per-subsystem structs are
 * intended to roll up into.  The end goal of the threading model is for a
 * thread that serves a logical session to reach all of that session's
 * mutable state through a single anchor, so that the per-session TLS
 * footprint is one object rather than hundreds.  This header introduces
 * that anchor.
 *
 * Migration is incremental and reviewable: subsystems move into MySession
 * one at a time (one commit each), exactly as the per-module XxxState
 * consolidation proceeded.  Until a subsystem is migrated it keeps its own
 * file-local session_local instance; nothing here forces a flag day.
 *
 * The names Session / CurrentSession are already taken by the parallel-query
 * DSM session (access/session.h), which is unrelated; hence MySession.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/mysession.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MYSESSION_H
#define MYSESSION_H

#include <signal.h>				/* for sig_atomic_t */

#include "executor/instrument.h"
#include "portability/instr_time.h"
#include "utils/hsearch.h"
#include "utils/palloc.h"

/*
 * Per-session state aggregate.
 *
 * Members are added here as subsystems migrate in.  Plain-typed members
 * (no private-type dependency) live directly in this struct; subsystems
 * with richer private state are added as sub-structs whose typedefs become
 * visible here at migration time.
 */
typedef struct MySession
{
	/*
	 * Trigger nesting depth (commands/trigger.c).  Incremented around each
	 * trigger invocation; exposed to SQL via pg_trigger_depth().
	 */
	int			trigger_depth;

	/*
	 * Total time charged to functions so far in this backend
	 * (utils/activity/pgstat_function.c).  Used to separate "self" and
	 * "other" time charges.  Initializes to zero.
	 */
	instr_time	total_func_time;

	/*
	 * WAL usage counters saved from pgWalUsage at the previous
	 * pgstat_report_wal() (utils/activity/pgstat_wal.c).  Subtracted from the
	 * current counters to compute WAL usage between reports.
	 */
	WalUsage	prev_wal_usage;

	/*
	 * Is a deadlock check pending? (storage/lmgr/proc.c)  Set by the deadlock
	 * timeout signal handler, consumed by the lock-wait loop.
	 */
	volatile sig_atomic_t got_deadlock_timeout;

	/*
	 * Memory context holding all MdfdVec objects for the md.c storage
	 * manager (storage/smgr/md.c).  Created in mdinit().
	 */
	MemoryContext md_cxt;

	/*
	 * Cache of resolved C-language function info, keyed by pg_proc OID
	 * (utils/fmgr/fmgr.c).  Lazily created; NULL until first use.
	 */
	HTAB	   *cfunc_hash;

	/*
	 * Cache of per-tablespace options, keyed by tablespace OID
	 * (utils/cache/spccache.c).  Lazily created; NULL until first use.
	 */
	HTAB	   *tablespace_cache_hash;

	/*
	 * Cache of per-attribute options, keyed by (attrelid, attnum)
	 * (utils/cache/attoptcache.c).  Lazily created; NULL until first use.
	 */
	HTAB	   *attopt_cache_hash;
} MySession;

/*
 * The single per-session instance.  Defined in
 * src/backend/utils/init/globals.c alongside the other top-level session
 * globals (MyProcPid, etc.).
 */
extern PGDLLIMPORT_TLS session_local MySession MySessionData;

#endif							/* MYSESSION_H */
