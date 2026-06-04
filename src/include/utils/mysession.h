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

#include "portability/instr_time.h"

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
} MySession;

/*
 * The single per-session instance.  Defined in
 * src/backend/utils/init/globals.c alongside the other top-level session
 * globals (MyProcPid, etc.).
 */
extern PGDLLIMPORT_TLS session_local MySession MySessionData;

#endif							/* MYSESSION_H */
