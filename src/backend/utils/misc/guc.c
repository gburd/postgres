/*--------------------------------------------------------------------
 * guc.c
 *
 * Support for grand unified configuration scheme, including SET
 * command, configuration file, and command line options.
 *
 * This file contains the generic option processing infrastructure.
 * guc_funcs.c contains SQL-level functionality, including SET/SHOW
 * commands and various system-administration SQL functions.
 * guc_tables.c contains the arrays that define all the built-in
 * GUC variables.  Code that implements variable-specific behavior
 * is scattered around the system in check, assign, and show hooks.
 *
 * See src/backend/utils/misc/README for more information.
 *
 *
 * Copyright (c) 2000-2026, PostgreSQL Global Development Group
 * Written by Peter Eisentraut <peter_e@gmx.net>.
 *
 * IDENTIFICATION
 *	  src/backend/utils/misc/guc.c
 *
 *--------------------------------------------------------------------
 */
#include "postgres.h"

#include <limits.h>
#include <math.h>
#ifndef WIN32
#include <pthread.h>
#ifdef USE_XTC_CARRIER
#include "xtc_sync.h"			/* xtc_amutex_* -- fiber-aware GUC critical section */
#include "postmaster/pg_xtc_carrier.h"	/* XtcPgVerifyCurrentWorkIsSelf */
#endif
#endif
#include <sys/stat.h>
#include <unistd.h>

#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_parameter_acl.h"
#include "catalog/pg_type.h"
#include "commands/tablespace.h"
#include "commands/vacuum.h"
#include "guc_internal.h"
#include "libpq/pqformat.h"
#include "libpq/protocol.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "optimizer/cost.h"
#include "optimizer/geqo.h"
#include "optimizer/optimizer.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "parser/parser.h"
#include "parser/parse_expr.h"
#include "parser/scansup.h"
#include "port/pg_bitutils.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "tcop/tcopprot.h"
#include "utils/acl.h"
#include "utils/backend_runtime.h"
#include "utils/builtins.h"
#include "utils/conffiles.h"
#include "utils/guc_tables.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"


#define CONFIG_FILENAME "postgresql.conf"
#define HBA_FILENAME	"pg_hba.conf"
#define IDENT_FILENAME	"pg_ident.conf"
#define HOSTS_FILENAME	"pg_hosts.conf"

#define CONFIG_EXEC_PARAMS "global/config_exec_params"
#define CONFIG_EXEC_PARAMS_NEW "global/config_exec_params.new"

/*
 * Precision with which REAL type guc values are to be printed for GUC
 * serialization.
 */
#define REALTYPE_PRECISION 17

/*
 * Safe search path when executing code as the table owner, such as during
 * maintenance operations.
 */
#define GUC_SAFE_SEARCH_PATH "pg_catalog, pg_temp"

#define GUC_check_errcode_value (*PgCurrentGUCCheckErrcodeValueRef())
#define GUCMemoryContext (*PgCurrentGUCMemoryContextRef())
#define guc_variables (*PgCurrentGUCVariablesRef())
#define guc_variable_states (*PgCurrentGUCVariableStatesRef())
#define num_guc_variables (*PgCurrentNumGUCVariablesRef())
#define guc_hashtab (*PgCurrentGUCHashTableRef())
#define guc_nondef_list (*PgCurrentGUCNondefListRef())
#define guc_stack_list (*PgCurrentGUCStackListRef())
#define guc_report_list (*PgCurrentGUCReportListRef())
#define reporting_enabled (*PgCurrentGUCReportingEnabledRef())
#define GUCNestLevel (*PgCurrentGUCNestLevelRef())

static PG_GLOBAL_RUNTIME List *reserved_class_prefix = NIL;
static PG_GLOBAL_RUNTIME MemoryContext GUCReservedPrefixMemoryContext = NULL;

#ifndef WIN32
/*
 * The threaded GUC critical section serializes GUC startup / SET / RESET across
 * carrier threads.  It MUST be fiber-aware: a raw pthread_mutex_lock blocks the
 * carrier OS THREAD, so a backend fiber that holds it and then yields the loop
 * (an allocation that parks, an internal wait, or the scheduler switching
 * fibers on that carrier) would deadlock another fiber on the same carrier that
 * waits on it.  Under USE_XTC_CARRIER use libxtc's xtc_amutex (a contended
 * fiber PARKS/yields the loop instead of blocking the OS thread; a caller not on
 * a loop -- process mode / a raw thread -- falls back to a condvar).  We use a
 * process-global, lazily-created, never-freed xtc_amutex_static() slot so there
 * is no init-order/shmem dependency.  Recursion is still driven by the
 * per-backend ThreadedGUCMutexDepth counter below (we take/drop the amutex only
 * on the outermost 0<->1 transition), matching the prior semantics.
 *
 * Without USE_XTC_CARRIER (no threaded runtime) keep the plain pthread mutex.
 */
#ifdef USE_XTC_CARRIER
#define THREADED_GUC_AMUTEX_SLOT 0u
#else
static PG_GLOBAL_RUNTIME pthread_mutex_t ThreadedGUCMutex = PTHREAD_MUTEX_INITIALIZER;
#endif
#define ThreadedGUCMutexDepth (*PgCurrentThreadedGUCMutexDepthRef())
#endif


static bool
ThreadedGUCLock(void)
{
#ifndef WIN32
	int			rc;

	if (!multithreaded)
		return false;
	if (ThreadedGUCMutexDepth++ > 0)
		return false;

	/*
	 * A die interrupt while this process-wide mutex is held can strand other
	 * backend threads in GUC startup or SET/RESET.  Match PostgreSQL lock
	 * primitives by deferring interrupts until the outermost unlock.
	 */
	HOLD_INTERRUPTS();
#ifdef USE_XTC_CARRIER

	/*
	 * SEAM the amutex park.  On contention xtc_amutex_lock() enqueues this
	 * fiber and xtc_yield()s the carrier loop (xtc/src/ptc/sync.c); libxtc
	 * preserves only __current_proc across that park, NOT PG's six current-
	 * work roots.  A migratable backend fiber woken from that park can be
	 * work-stolen and resumed on a DIFFERENT carrier thread whose bridge still
	 * points at whatever fiber last ran there -- so GUC code after the lock
	 * would mutate the wrong session's state (cross-session corruption).
	 *
	 * Mirror the xtc_pg_wait_fd seam (pg_xtc_carrier.c): snapshot the six
	 * roots on a STACK-LOCAL that rides with the fiber across a steal, then
	 * restore them after xtc_amutex_lock() returns so the resuming thread's
	 * bridge is repointed to THIS fiber's own roots regardless of which loop
	 * resumed it.  On the uncontended fast path the lock returns without
	 * yielding and the restore just rewrites identical values (harmless).
	 *
	 * This wraps ONLY the outermost lock: nested re-entrant ThreadedGUCLock
	 * calls returned above at `ThreadedGUCMutexDepth++ > 0` WITHOUT touching
	 * the amutex, so they never park and never reach this seam -- no double
	 * save/restore.  The paired outermost ThreadedGUCUnlock does NOT need a
	 * park-seam: xtc_amutex_unlock() hands off + wakes a waiter but never
	 * xtc_yield()s the caller, so it cannot park or leak the bridge.
	 */
	{
		PgCurrentWorkSnapshot snap;

		PgRuntimeSaveCurrentWork(&snap);
		XtcPgVerifySnapshotIsSelf(&snap);
		rc = xtc_amutex_lock(xtc_amutex_static(THREADED_GUC_AMUTEX_SLOT), -1);
		PgRuntimeRestoreCurrentWork(&snap);
		XtcPgVerifyCurrentWorkIsSelf();
	}
#else
	rc = pthread_mutex_lock(&ThreadedGUCMutex);
#endif
	if (rc != 0)
	{
		ThreadedGUCMutexDepth--;
		RESUME_INTERRUPTS();
		errno = rc;
		ereport(FATAL,
				(errmsg("could not enter threaded GUC critical section: %m")));
	}

	return true;
#else
	return false;
#endif
}

static void
ThreadedGUCUnlock(bool locked)
{
#ifndef WIN32
	int			rc;

	/*
	 * Drive everything off the per-carrier depth counter, NOT off a fresh read
	 * of `multithreaded` -- setting the `multithreaded` GUC itself flips that
	 * flag between the paired lock and unlock (ThreadedGUCLock runs with the
	 * OLD value, this unlock would see the NEW one), which previously tripped
	 * Assert(ThreadedGUCMutexDepth > 0) on the off->on transition (and, in a
	 * non-assert build, underflowed the depth + RESUME_INTERRUPTS unbalanced).
	 * The depth counter is self-describing: ThreadedGUCLock incremented it iff
	 * it engaged (multithreaded was true at lock time), and took the pthread
	 * mutex + HOLD_INTERRUPTS only on the outermost (0 -> 1) increment.  So:
	 * depth == 0 means the paired lock did not engage -> nothing to undo;
	 * otherwise decrement, and on the outermost release (1 -> 0) drop the mutex
	 * and RESUME_INTERRUPTS.  `locked` (true iff this call took the outermost
	 * lock) is cross-checked against that transition under assert.
	 */
	if (ThreadedGUCMutexDepth == 0)
	{
		Assert(!locked);
		return;
	}

	if (--ThreadedGUCMutexDepth > 0)
	{
		Assert(!locked);
		return;
	}

	/* outermost release */
	Assert(locked);
#ifdef USE_XTC_CARRIER
	rc = xtc_amutex_unlock(xtc_amutex_static(THREADED_GUC_AMUTEX_SLOT));
#else
	rc = pthread_mutex_unlock(&ThreadedGUCMutex);
#endif
	RESUME_INTERRUPTS();
	if (rc != 0)
	{
		errno = rc;
		elog(FATAL, "could not leave threaded GUC critical section: %m");
	}
#else
	(void) locked;
#endif
}

static bool
GUCRecordIsCurrentSessionBuiltin(const struct config_generic *record)
{
	uintptr_t	record_addr;
	uintptr_t	start_addr;
	uintptr_t	end_addr;
	Size		offset;

	if (guc_variables == NULL || num_guc_variables <= 0)
		return false;

	record_addr = (uintptr_t) record;
	start_addr = (uintptr_t) guc_variables;
	end_addr = start_addr + sizeof(struct config_generic) * num_guc_variables;

	if (record_addr < start_addr || record_addr >= end_addr)
		return false;

	offset = record_addr - start_addr;
	return (offset % sizeof(struct config_generic)) == 0;
}

static int
GUCRecordBuiltinIndex(const struct config_generic *record)
{
	uintptr_t	record_addr;
	uintptr_t	start_addr;
	Size		offset;

	Assert(GUCRecordIsCurrentSessionBuiltin(record));

	record_addr = (uintptr_t) record;
	start_addr = (uintptr_t) guc_variables;
	offset = record_addr - start_addr;

	return offset / sizeof(struct config_generic);
}

static config_generic_state *
GUCRecordState(const struct config_generic *record)
{
	if (GUCRecordIsCurrentSessionBuiltin(record))
	{
		int			index = GUCRecordBuiltinIndex(record);

		Assert(guc_variable_states != NULL);
		Assert(index >= 0);
		Assert(index < num_guc_variables);

		return &guc_variable_states[index];
	}

	Assert(record->state != NULL);
	return record->state;
}

static config_generic_cold_state *
GUCRecordColdStateIfAllocated(const struct config_generic *record)
{
	return GUCRecordState(record)->cold;
}

static config_generic_cold_state *
GUCRecordColdState(const struct config_generic *record)
{
	config_generic_state *state = GUCRecordState(record);

	if (state->cold == NULL)
	{
		state->cold = MemoryContextAllocZero(GUCMemoryContext,
											 sizeof(config_generic_cold_state));
		state->cold->record = record;
		state->cold->reset_source = PGC_S_DEFAULT;
		state->cold->reset_scontext = PGC_INTERNAL;
		state->cold->reset_srole = BOOTSTRAP_SUPERUSERID;
	}

	return state->cold;
}

static GucSource
GUCRecordResetSource(const struct config_generic *record)
{
	config_generic_cold_state *cold = GUCRecordColdStateIfAllocated(record);

	return cold != NULL ? cold->reset_source : PGC_S_DEFAULT;
}

static GucSource *
GUCRecordResetSourceRef(const struct config_generic *record)
{
	return &GUCRecordColdState(record)->reset_source;
}

static GucContext
GUCRecordResetSContext(const struct config_generic *record)
{
	config_generic_cold_state *cold = GUCRecordColdStateIfAllocated(record);

	return cold != NULL ? cold->reset_scontext : PGC_INTERNAL;
}

static GucContext *
GUCRecordResetSContextRef(const struct config_generic *record)
{
	return &GUCRecordColdState(record)->reset_scontext;
}

static Oid
GUCRecordResetSRole(const struct config_generic *record)
{
	config_generic_cold_state *cold = GUCRecordColdStateIfAllocated(record);

	return cold != NULL ? cold->reset_srole : BOOTSTRAP_SUPERUSERID;
}

static Oid *
GUCRecordResetSRoleRef(const struct config_generic *record)
{
	return &GUCRecordColdState(record)->reset_srole;
}

static GucStack *
GUCRecordStack(const struct config_generic *record)
{
	config_generic_cold_state *cold = GUCRecordColdStateIfAllocated(record);

	return cold != NULL ? cold->stack : NULL;
}

static void
GUCRecordSetStack(const struct config_generic *record, GucStack *stack)
{
	config_generic_cold_state *cold = GUCRecordColdStateIfAllocated(record);

	if (cold == NULL && stack == NULL)
		return;

	if (cold == NULL)
		cold = GUCRecordColdState(record);

	cold->stack = stack;
}

static void *
GUCRecordExtra(const struct config_generic *record)
{
	config_generic_cold_state *cold = GUCRecordColdStateIfAllocated(record);

	return cold != NULL ? cold->extra : NULL;
}

static void **
GUCRecordExtraRef(const struct config_generic *record)
{
	return &GUCRecordColdState(record)->extra;
}

static void
GUCRecordSetExtra(const struct config_generic *record, void *extra)
{
	config_generic_cold_state *cold = GUCRecordColdStateIfAllocated(record);

	if (cold == NULL && extra == NULL)
		return;

	if (cold == NULL)
		cold = GUCRecordColdState(record);

	cold->extra = extra;
}

static void *
GUCRecordResetExtra(const struct config_generic *record)
{
	config_generic_cold_state *cold = GUCRecordColdStateIfAllocated(record);

	return cold != NULL ? cold->reset_extra : NULL;
}

static void **
GUCRecordResetExtraRef(const struct config_generic *record)
{
	return &GUCRecordColdState(record)->reset_extra;
}

static void
GUCRecordSetResetExtra(const struct config_generic *record, void *reset_extra)
{
	config_generic_cold_state *cold = GUCRecordColdStateIfAllocated(record);

	if (cold == NULL && reset_extra == NULL)
		return;

	if (cold == NULL)
		cold = GUCRecordColdState(record);

	cold->reset_extra = reset_extra;
}

static char *
GUCRecordLastReported(const struct config_generic *record)
{
	config_generic_cold_state *cold = GUCRecordColdStateIfAllocated(record);

	return cold != NULL ? cold->last_reported : NULL;
}

static char *
GUCRecordSourceFile(const struct config_generic *record)
{
	config_generic_cold_state *cold = GUCRecordColdStateIfAllocated(record);

	return cold != NULL ? cold->sourcefile : NULL;
}

static int
GUCRecordSourceLine(const struct config_generic *record)
{
	config_generic_cold_state *cold = GUCRecordColdStateIfAllocated(record);

	return cold != NULL ? cold->sourceline : 0;
}

static void
GUCRecordResetColdFields(const struct config_generic *record)
{
	config_generic_cold_state *cold = GUCRecordColdStateIfAllocated(record);

	if (cold == NULL)
		return;

	cold->stack = NULL;
	cold->extra = NULL;
	cold->reset_extra = NULL;
	cold->reset_source = PGC_S_DEFAULT;
	cold->reset_scontext = PGC_INTERNAL;
	cold->reset_srole = BOOTSTRAP_SUPERUSERID;
	cold->last_reported = NULL;
	cold->sourcefile = NULL;
	cold->sourceline = 0;
}

#define GUC_STATE(record)			(GUCRecordState(record))
#define GUC_COLD(record)			(GUCRecordColdState(record))
#define GUC_NONDEF_LINK(record)		(&GUC_COLD(record)->nondef_link)
#define GUC_STACK_LINK(record)		(&GUC_COLD(record)->stack_link)
#define GUC_REPORT_LINK(record)		(&GUC_COLD(record)->report_link)
#define GUC_STATUS(record)			(GUC_STATE(record)->status)
#define GUC_SOURCE(record)			(GUC_STATE(record)->source)
#define GUC_RESET_SOURCE(record)	(GUCRecordResetSource(record))
#define GUC_RESET_SOURCE_REF(record) \
	GUCRecordResetSourceRef(record)
#define GUC_SCONTEXT(record)		(GUC_STATE(record)->scontext)
#define GUC_RESET_SCONTEXT(record)	(GUCRecordResetSContext(record))
#define GUC_RESET_SCONTEXT_REF(record) \
	GUCRecordResetSContextRef(record)
#define GUC_SROLE(record)			(GUC_STATE(record)->srole)
#define GUC_RESET_SROLE(record)		(GUCRecordResetSRole(record))
#define GUC_RESET_SROLE_REF(record) \
	GUCRecordResetSRoleRef(record)
#define GUC_STACK(record)			(GUCRecordStack(record))
#define GUC_SET_STACK(record, value) \
	GUCRecordSetStack((record), (value))
#define GUC_EXTRA(record)			(GUCRecordExtra(record))
#define GUC_EXTRA_REF(record)		(GUCRecordExtraRef(record))
#define GUC_SET_EXTRA(record, value) \
	GUCRecordSetExtra((record), (value))
#define GUC_RESET_EXTRA(record)		(GUCRecordResetExtra(record))
#define GUC_RESET_EXTRA_REF(record)	(GUCRecordResetExtraRef(record))
#define GUC_SET_RESET_EXTRA(record, value) \
	GUCRecordSetResetExtra((record), (value))
#define GUC_LAST_REPORTED(record)	(GUCRecordLastReported(record))
#define GUC_SET_LAST_REPORTED(record, value) \
	(GUC_COLD(record)->last_reported = (value))
#define GUC_SOURCEFILE(record)		(GUCRecordSourceFile(record))
#define GUC_SET_SOURCEFILE(record, value) \
	(GUC_COLD(record)->sourcefile = (value))
#define GUC_SOURCELINE(record)		(GUCRecordSourceLine(record))
#define GUC_SET_SOURCELINE(record, value) \
	(GUC_COLD(record)->sourceline = (value))
#define GUC_VARIABLE_BOOL(record)	(GUC_STATE(record)->variable.boolvar)
#define GUC_VARIABLE_INT(record)	(GUC_STATE(record)->variable.intvar)
#define GUC_VARIABLE_REAL(record)	(GUC_STATE(record)->variable.realvar)
#define GUC_VARIABLE_STRING(record)	(GUC_STATE(record)->variable.stringvar)
#define GUC_VARIABLE_ENUM(record)	(GUC_STATE(record)->variable.enumvar)
#define GUC_RESET_BOOL(record)		(GUC_STATE(record)->reset_val.boolval)
#define GUC_RESET_INT(record)		(GUC_STATE(record)->reset_val.intval)
#define GUC_RESET_REAL(record)		(GUC_STATE(record)->reset_val.realval)
#define GUC_RESET_STRING(record)	(GUC_STATE(record)->reset_val.stringval)
#define GUC_RESET_ENUM(record)		(GUC_STATE(record)->reset_val.enumval)
#define GUC_COLD_STATE_RECORD(cold) \
	(unconstify(struct config_generic *, (cold)->record))

static bool
GUCRecordVariableIsCurrentSessionOwned(const struct config_generic *record)
{
	const void *variable;

	switch (record->vartype)
	{
		case PGC_BOOL:
			variable = GUC_VARIABLE_BOOL(record);
			break;
		case PGC_INT:
			variable = GUC_VARIABLE_INT(record);
			break;
		case PGC_REAL:
			variable = GUC_VARIABLE_REAL(record);
			break;
		case PGC_STRING:
			variable = GUC_VARIABLE_STRING(record);
			break;
		case PGC_ENUM:
			variable = GUC_VARIABLE_ENUM(record);
			break;
		default:
			pg_unreachable();
	}

	return PgCurrentSessionOwnsPointer(variable);
}

static bool
GUCThreadedBackendReplayActive(bool is_reload)
{
	return is_reload &&
		multithreaded &&
		IsUnderPostmaster &&
		CurrentPgCarrier != NULL &&
		CurrentPgCarrier->kind == PG_CARRIER_THREAD;
}

static bool
GUCRecordHasAssignHook(const struct config_generic *record)
{
	switch (record->vartype)
	{
		case PGC_BOOL:
			return record->_bool.assign_hook != NULL;
		case PGC_INT:
			return record->_int.assign_hook != NULL;
		case PGC_REAL:
			return record->_real.assign_hook != NULL;
		case PGC_STRING:
			return record->_string.assign_hook != NULL;
		case PGC_ENUM:
			return record->_enum.assign_hook != NULL;
	}

	pg_unreachable();
}

static bool
GUCRecordHasShowHook(const struct config_generic *record)
{
	switch (record->vartype)
	{
		case PGC_BOOL:
			return record->_bool.show_hook != NULL;
		case PGC_INT:
			return record->_int.show_hook != NULL;
		case PGC_REAL:
			return record->_real.show_hook != NULL;
		case PGC_STRING:
			return record->_string.show_hook != NULL;
		case PGC_ENUM:
			return record->_enum.show_hook != NULL;
	}

	pg_unreachable();
}

static bool
GUCSetOptionNeedsThreadedLock(const struct config_generic *record)
{
	if (!multithreaded)
		return false;

	/*
	 * Built-in GUC descriptors are immutable, while the current value,
	 * reset/source metadata, and list membership live in PgSession-owned state.
	 * A simple built-in GUC whose direct variable also lives in PgSession and
	 * has no assign hook mutates only this logical backend's GUC state, so it
	 * does not need the temporary process-wide GUC mutex.  Check hooks must not
	 * be guarded merely because they are hooks: some validate against catalogs
	 * and can wait on heavyweight locks, so holding the process-wide GUC mutex
	 * across them can deadlock threaded sessions.
	 *
	 * Keep all ambiguous paths serialized: custom/extension records,
	 * placeholders, assign-hook-backed records, execution-owned active
	 * transaction GUCs, and records whose direct variable still points at
	 * process-global storage.
	 */
	if (GUCRecordIsCurrentSessionBuiltin(record) &&
		GUCRecordVariableIsCurrentSessionOwned(record) &&
		!GUCRecordHasAssignHook(record))
		return false;

	return true;
}

static bool
GUCShowOptionNeedsThreadedLock(const struct config_generic *record)
{
	if (!multithreaded)
		return false;

	/*
	 * Ordinary built-in GUC records share immutable descriptors, with their
	 * direct-variable slots rebound through per-session state. Showing such a
	 * record reads only this logical backend's state, so it need not serialize
	 * with other threaded sessions.
	 *
	 * Keep hook-backed and custom/extension records under the runtime GUC
	 * mutex. Show hooks can inspect subsystem state, and custom records may
	 * still depend on extension code or shared module lifecycle.
	 */
	if (GUCRecordIsCurrentSessionBuiltin(record) &&
		!GUCRecordHasShowHook(record))
		return false;

	return true;
}

static MemoryContext
GUCReservedPrefixContext(void)
{
	if (GUCReservedPrefixMemoryContext == NULL)
	{
		GUCReservedPrefixMemoryContext =
			AllocSetContextCreate(PgCurrentRuntimeExtensionModuleMemoryContext(),
								  "reserved GUC prefixes",
								  ALLOCSET_DEFAULT_SIZES);
	}

	return GUCReservedPrefixMemoryContext;
}


/*
 * Unit conversion tables.
 *
 * There are two tables, one for memory units, and another for time units.
 * For each supported conversion from one unit to another, we have an entry
 * in the table.
 *
 * To keep things simple, and to avoid possible roundoff error,
 * conversions are never chained.  There needs to be a direct conversion
 * between all units (of the same type).
 *
 * The conversions for each base unit must be kept in order from greatest to
 * smallest human-friendly unit; convert_xxx_from_base_unit() rely on that.
 * (The order of the base-unit groups does not matter.)
 */
#define MAX_UNIT_LEN		3	/* length of longest recognized unit string */

typedef struct
{
	char		unit[MAX_UNIT_LEN + 1]; /* unit, as a string, like "kB" or
										 * "min" */
	int			base_unit;		/* GUC_UNIT_XXX */
	double		multiplier;		/* Factor for converting unit -> base_unit */
} unit_conversion;

/* Ensure that the constants in the tables don't overflow or underflow */
#if BLCKSZ < 1024 || BLCKSZ > (1024*1024)
#error BLCKSZ must be between 1KB and 1MB
#endif
#if XLOG_BLCKSZ < 1024 || XLOG_BLCKSZ > (1024*1024)
#error XLOG_BLCKSZ must be between 1KB and 1MB
#endif

static PG_GLOBAL_IMMUTABLE const char *const memory_units_hint = gettext_noop("Valid units for this parameter are \"B\", \"kB\", \"MB\", \"GB\", and \"TB\".");

static PG_GLOBAL_IMMUTABLE const unit_conversion memory_unit_conversion_table[] =
{
	{"TB", GUC_UNIT_BYTE, 1024.0 * 1024.0 * 1024.0 * 1024.0},
	{"GB", GUC_UNIT_BYTE, 1024.0 * 1024.0 * 1024.0},
	{"MB", GUC_UNIT_BYTE, 1024.0 * 1024.0},
	{"kB", GUC_UNIT_BYTE, 1024.0},
	{"B", GUC_UNIT_BYTE, 1.0},

	{"TB", GUC_UNIT_KB, 1024.0 * 1024.0 * 1024.0},
	{"GB", GUC_UNIT_KB, 1024.0 * 1024.0},
	{"MB", GUC_UNIT_KB, 1024.0},
	{"kB", GUC_UNIT_KB, 1.0},
	{"B", GUC_UNIT_KB, 1.0 / 1024.0},

	{"TB", GUC_UNIT_MB, 1024.0 * 1024.0},
	{"GB", GUC_UNIT_MB, 1024.0},
	{"MB", GUC_UNIT_MB, 1.0},
	{"kB", GUC_UNIT_MB, 1.0 / 1024.0},
	{"B", GUC_UNIT_MB, 1.0 / (1024.0 * 1024.0)},

	{"TB", GUC_UNIT_BLOCKS, (1024.0 * 1024.0 * 1024.0) / (BLCKSZ / 1024)},
	{"GB", GUC_UNIT_BLOCKS, (1024.0 * 1024.0) / (BLCKSZ / 1024)},
	{"MB", GUC_UNIT_BLOCKS, 1024.0 / (BLCKSZ / 1024)},
	{"kB", GUC_UNIT_BLOCKS, 1.0 / (BLCKSZ / 1024)},
	{"B", GUC_UNIT_BLOCKS, 1.0 / BLCKSZ},

	{"TB", GUC_UNIT_XBLOCKS, (1024.0 * 1024.0 * 1024.0) / (XLOG_BLCKSZ / 1024)},
	{"GB", GUC_UNIT_XBLOCKS, (1024.0 * 1024.0) / (XLOG_BLCKSZ / 1024)},
	{"MB", GUC_UNIT_XBLOCKS, 1024.0 / (XLOG_BLCKSZ / 1024)},
	{"kB", GUC_UNIT_XBLOCKS, 1.0 / (XLOG_BLCKSZ / 1024)},
	{"B", GUC_UNIT_XBLOCKS, 1.0 / XLOG_BLCKSZ},

	{""}						/* end of table marker */
};

static PG_GLOBAL_IMMUTABLE const char *const time_units_hint = gettext_noop("Valid units for this parameter are \"us\", \"ms\", \"s\", \"min\", \"h\", and \"d\".");

static PG_GLOBAL_IMMUTABLE const unit_conversion time_unit_conversion_table[] =
{
	{"d", GUC_UNIT_MS, 1000 * 60 * 60 * 24},
	{"h", GUC_UNIT_MS, 1000 * 60 * 60},
	{"min", GUC_UNIT_MS, 1000 * 60},
	{"s", GUC_UNIT_MS, 1000},
	{"ms", GUC_UNIT_MS, 1},
	{"us", GUC_UNIT_MS, 1.0 / 1000},

	{"d", GUC_UNIT_S, 60 * 60 * 24},
	{"h", GUC_UNIT_S, 60 * 60},
	{"min", GUC_UNIT_S, 60},
	{"s", GUC_UNIT_S, 1},
	{"ms", GUC_UNIT_S, 1.0 / 1000},
	{"us", GUC_UNIT_S, 1.0 / (1000 * 1000)},

	{"d", GUC_UNIT_MIN, 60 * 24},
	{"h", GUC_UNIT_MIN, 60},
	{"min", GUC_UNIT_MIN, 1},
	{"s", GUC_UNIT_MIN, 1.0 / 60},
	{"ms", GUC_UNIT_MIN, 1.0 / (1000 * 60)},
	{"us", GUC_UNIT_MIN, 1.0 / (1000 * 1000 * 60)},

	{""}						/* end of table marker */
};

/*
 * To allow continued support of obsolete names for GUC variables, we apply
 * the following mappings to any unrecognized name.  Note that an old name
 * should be mapped to a new one only if the new variable has very similar
 * semantics to the old.
 */
static PG_GLOBAL_IMMUTABLE const char *const map_old_guc_names[] = {
	"sort_mem", "work_mem",
	"vacuum_mem", "maintenance_work_mem",
	"ssl_ecdh_curve", "ssl_groups",
	NULL
};


/*
 * Per-session lookup state for custom GUCs and placeholders.  Built-in GUCs
 * use the immutable ConfigureNames[] descriptor table plus per-session
 * config_generic_state overlays, so only truly dynamic records need hash
 * storage here.
 *
 * The gucname field is redundant with gucvar->name, but dynahash makes it too
 * painful to not store the hash key separately.
 */
typedef struct
{
	const char *gucname;		/* hash key */
	struct config_generic *gucvar;	/* -> GUC's defining structure */
} GUCHashEntry;

/*
 * Built-in GUC names are immutable after guc_parameters.dat generation, so the
 * name-to-index lookup table can be shared by all logical backends.  Custom
 * GUCs and placeholders remain per-session in guc_hashtab.
 */
typedef struct
{
	const char *gucname;		/* hash key */
	int			index;			/* index in ConfigureNames/guc_variables */
} GUCBuiltinHashEntry;

static PG_GLOBAL_RUNTIME HTAB *guc_builtin_hashtab = NULL;

/*
 * In addition to the hash table, variables having certain properties are
 * linked into these lists, so that we can find them without scanning the
 * whole hash table.  In most applications, only a small fraction of the
 * GUCs appear in these lists at any given time.  The usage of the stack
 * and report lists is stylized enough that they can be slists, but the
 * nondef list has to be a dlist to avoid O(N) deletes in common cases.
 */

/* true to enable GUC_REPORT */

/* 1 when in main transaction */


static int	guc_var_compare(const void *a, const void *b);
static uint32 guc_name_hash(const void *key, Size keysize);
static int	guc_name_match(const void *key1, const void *key2, Size keysize);
static void ensure_builtin_guc_name_index(void);
static struct config_generic *find_builtin_option(const char *name);
static int	guc_custom_variable_count(void);
static HTAB *ensure_guc_custom_hashtab(int nelem);
static void InitializeGUCVariableStatePointers(void);
static void InitializeGUCOptionsFromEnvironment(void);
static void InitializeOneGUCOption(struct config_generic *gconf);
static void InitializeOneGUCOptionResetMetadata(struct config_generic *gconf);
static const void *GUCOptionVariablePointer(struct config_generic *gconf);
static void InitializeThreadedSessionReboundGUCOptions(void);
static void InitializeThreadedSessionCompatibilityGUCOptions(void);
static bool ThreadedGUCLock(void);
static void ThreadedGUCUnlock(bool locked);
static bool GUCSetOptionNeedsThreadedLock(const struct config_generic *record);
static bool GUCShowOptionNeedsThreadedLock(const struct config_generic *record);
static MemoryContext GUCReservedPrefixContext(void);
static int	set_config_with_handle_internal(const char *name,
											config_handle *handle,
											const char *value,
											GucContext context,
											GucSource source, Oid srole,
											GucAction action, bool changeVal,
											int elevel, bool is_reload);
static char *ShowGUCOptionInternal(const struct config_generic *record,
								   bool use_units);
static void RemoveGUCFromLists(struct config_generic *gconf);
static void set_guc_source(struct config_generic *gconf, GucSource newsource);
static void reset_guc_record_at_backend_exit(struct config_generic *gconf);
static void pg_timezone_abbrev_initialize(void);
static void push_old_value(struct config_generic *gconf, GucAction action);
static void ReportGUCOption(struct config_generic *record);
static void set_config_sourcefile(const char *name, char *sourcefile,
								  int sourceline);
static void reapply_stacked_values(struct config_generic *variable,
								   struct config_generic *pHolder,
								   GucStack *stack,
								   const char *curvalue,
								   GucContext curscontext, GucSource cursource,
								   Oid cursrole);
static void free_placeholder(struct config_generic *pHolder);
static bool validate_option_array_item(const char *name, const char *value,
									   bool skipIfNoPermissions);
static void write_auto_conf_file(int fd, const char *filename, ConfigVariable *head);
static void replace_auto_config_value(ConfigVariable **head_p, ConfigVariable **tail_p,
									  const char *name, const char *value);
static bool valid_custom_variable_name(const char *name);
static bool assignable_custom_variable_name(const char *name, bool skip_errors,
											int elevel);
static void do_serialize(char **destptr, Size *maxbytes,
						 const char *fmt, ...) pg_attribute_printf(3, 4);
static bool call_bool_check_hook(const struct config_generic *conf, bool *newval,
								 void **extra, GucSource source, int elevel);
static bool call_int_check_hook(const struct config_generic *conf, int *newval,
								void **extra, GucSource source, int elevel);
static bool call_real_check_hook(const struct config_generic *conf, double *newval,
								 void **extra, GucSource source, int elevel);
static bool call_string_check_hook(const struct config_generic *conf, char **newval,
								   void **extra, GucSource source, int elevel);
static bool call_enum_check_hook(const struct config_generic *conf, int *newval,
								 void **extra, GucSource source, int elevel);


/*
 * This function handles both actual config file (re)loads and execution of
 * show_all_file_settings() (i.e., the pg_file_settings view).  In the latter
 * case we don't apply any of the settings, but we make all the usual validity
 * checks, and we return the ConfigVariable list so that it can be printed out
 * by show_all_file_settings().
 */
ConfigVariable *
ProcessConfigFileInternal(GucContext context, bool applySettings, int elevel)
{
	bool		error = false;
	bool		applying = false;
	const char *ConfFileWithError;
	ConfigVariable *head,
			   *tail;
	HASH_SEQ_STATUS status;
	GUCHashEntry *hentry;

	/* Parse the main config file into a list of option names and values */
	ConfFileWithError = ConfigFileName;
	head = tail = NULL;

	if (!ParseConfigFile(ConfigFileName, true,
						 NULL, 0, CONF_FILE_START_DEPTH, elevel,
						 &head, &tail))
	{
		/* Syntax error(s) detected in the file, so bail out */
		error = true;
		goto bail_out;
	}

	/*
	 * Parse the PG_AUTOCONF_FILENAME file, if present, after the main file to
	 * replace any parameters set by ALTER SYSTEM command.  Because this file
	 * is in the data directory, we can't read it until the DataDir has been
	 * set.
	 */
	if (DataDir)
	{
		if (!ParseConfigFile(PG_AUTOCONF_FILENAME, false,
							 NULL, 0, CONF_FILE_START_DEPTH, elevel,
							 &head, &tail))
		{
			/* Syntax error(s) detected in the file, so bail out */
			error = true;
			ConfFileWithError = PG_AUTOCONF_FILENAME;
			goto bail_out;
		}
	}
	else
	{
		/*
		 * If DataDir is not set, the PG_AUTOCONF_FILENAME file cannot be
		 * read.  In this case, we don't want to accept any settings but
		 * data_directory from postgresql.conf, because they might be
		 * overwritten with settings in the PG_AUTOCONF_FILENAME file which
		 * will be read later. OTOH, since data_directory isn't allowed in the
		 * PG_AUTOCONF_FILENAME file, it will never be overwritten later.
		 */
		ConfigVariable *newlist = NULL;

		/*
		 * Prune all items except the last "data_directory" from the list.
		 */
		for (ConfigVariable *item = head; item; item = item->next)
		{
			if (!item->ignore &&
				strcmp(item->name, "data_directory") == 0)
				newlist = item;
		}

		if (newlist)
			newlist->next = NULL;
		head = tail = newlist;

		/*
		 * Quick exit if data_directory is not present in file.
		 *
		 * We need not do any further processing, in particular we don't set
		 * PgReloadTime; that will be set soon by subsequent full loading of
		 * the config file.
		 */
		if (head == NULL)
			goto bail_out;
	}

	/*
	 * Mark all extant GUC variables as not present in the config file. We
	 * need this so that we can tell below which ones have been removed from
	 * the file since we last processed it.
	 */
	for (int i = 0; i < num_guc_variables; i++)
		GUC_STATUS(&guc_variables[i]) &= ~GUC_IS_IN_FILE;
	if (guc_hashtab != NULL)
	{
		hash_seq_init(&status, guc_hashtab);
		while ((hentry = (GUCHashEntry *) hash_seq_search(&status)) != NULL)
		{
			struct config_generic *gconf = hentry->gucvar;

			GUC_STATUS(gconf) &= ~GUC_IS_IN_FILE;
		}
	}

	/*
	 * Check if all the supplied option names are valid, as an additional
	 * quasi-syntactic check on the validity of the config file.  It is
	 * important that the postmaster and all backends agree on the results of
	 * this phase, else we will have strange inconsistencies about which
	 * processes accept a config file update and which don't.  Hence, unknown
	 * custom variable names have to be accepted without complaint.  For the
	 * same reason, we don't attempt to validate the options' values here.
	 *
	 * In addition, the GUC_IS_IN_FILE flag is set on each existing GUC
	 * variable mentioned in the file; and we detect duplicate entries in the
	 * file and mark the earlier occurrences as ignorable.
	 */
	for (ConfigVariable *item = head; item; item = item->next)
	{
		struct config_generic *record;

		/* Ignore anything already marked as ignorable */
		if (item->ignore)
			continue;

		/*
		 * Try to find the variable; but do not create a custom placeholder if
		 * it's not there already.
		 */
		record = find_option(item->name, false, true, elevel);

		if (record)
		{
			/* If it's already marked, then this is a duplicate entry */
			if (GUC_STATUS(record) & GUC_IS_IN_FILE)
			{
				/*
				 * Mark the earlier occurrence(s) as dead/ignorable.  We could
				 * avoid the O(N^2) behavior here with some additional state,
				 * but it seems unlikely to be worth the trouble.
				 */
				for (ConfigVariable *pitem = head; pitem != item; pitem = pitem->next)
				{
					if (!pitem->ignore &&
						strcmp(pitem->name, item->name) == 0)
						pitem->ignore = true;
				}
			}
			/* Now mark it as present in file */
			GUC_STATUS(record) |= GUC_IS_IN_FILE;
		}
		else if (!valid_custom_variable_name(item->name))
		{
			/* Invalid non-custom variable, so complain */
			ereport(elevel,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("unrecognized configuration parameter \"%s\" in file \"%s\" line %d",
							item->name,
							item->filename, item->sourceline)));
			item->errmsg = pstrdup("unrecognized configuration parameter");
			error = true;
			ConfFileWithError = item->filename;
		}
	}

	/*
	 * If we've detected any errors so far, we don't want to risk applying any
	 * changes.
	 */
	if (error)
		goto bail_out;

	/* Otherwise, set flag that we're beginning to apply changes */
	applying = true;

	/*
	 * Check for variables having been removed from the config file, and
	 * revert their reset values (and perhaps also effective values) to the
	 * boot-time defaults.  If such a variable can't be changed after startup,
	 * report that and continue.
	 */
	for (int i = 0; i < num_guc_variables; i++)
	{
		struct config_generic *gconf = &guc_variables[i];

		if (GUC_RESET_SOURCE(gconf) != PGC_S_FILE ||
			(GUC_STATUS(gconf) & GUC_IS_IN_FILE))
			continue;
		if (gconf->context < PGC_SIGHUP)
		{
			/* The removal can't be effective without a restart */
			GUC_STATUS(gconf) |= GUC_PENDING_RESTART;
			ereport(elevel,
					(errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
					 errmsg("parameter \"%s\" cannot be changed without restarting the server",
							gconf->name)));
			record_config_file_error(psprintf("parameter \"%s\" cannot be changed without restarting the server",
											  gconf->name),
									 NULL, 0,
									 &head, &tail);
			error = true;
			continue;
		}

		/* No more to do if we're just doing show_all_file_settings() */
		if (!applySettings)
			continue;

		/*
		 * Reset any "file" sources to "default", else set_config_option will
		 * not override those settings.
		 */
		if (GUC_RESET_SOURCE(gconf) == PGC_S_FILE)
			*GUC_RESET_SOURCE_REF(gconf) = PGC_S_DEFAULT;
		if (GUC_SOURCE(gconf) == PGC_S_FILE)
			set_guc_source(gconf, PGC_S_DEFAULT);
		for (GucStack *stack = GUC_STACK(gconf); stack; stack = stack->prev)
		{
			if (stack->source == PGC_S_FILE)
				stack->source = PGC_S_DEFAULT;
		}

		/* Now we can re-apply the wired-in default (i.e., the boot_val) */
		if (set_config_option(gconf->name, NULL,
							  context, PGC_S_DEFAULT,
							  GUC_ACTION_SET, true, 0, false) > 0)
		{
			/* Log the change if appropriate */
			if (context == PGC_SIGHUP)
				ereport(elevel,
						(errmsg("parameter \"%s\" removed from configuration file, reset to default",
								gconf->name)));
		}
	}
	if (guc_hashtab != NULL)
	{
		hash_seq_init(&status, guc_hashtab);
		while ((hentry = (GUCHashEntry *) hash_seq_search(&status)) != NULL)
		{
			struct config_generic *gconf = hentry->gucvar;

			if (GUC_RESET_SOURCE(gconf) != PGC_S_FILE ||
				(GUC_STATUS(gconf) & GUC_IS_IN_FILE))
				continue;
			if (gconf->context < PGC_SIGHUP)
			{
				/* The removal can't be effective without a restart */
				GUC_STATUS(gconf) |= GUC_PENDING_RESTART;
				ereport(elevel,
						(errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
						 errmsg("parameter \"%s\" cannot be changed without restarting the server",
								gconf->name)));
				record_config_file_error(psprintf("parameter \"%s\" cannot be changed without restarting the server",
												  gconf->name),
										 NULL, 0,
										 &head, &tail);
				error = true;
				continue;
			}

			/* No more to do if we're just doing show_all_file_settings() */
			if (!applySettings)
				continue;

			/*
			 * Reset any "file" sources to "default", else set_config_option
			 * will not override those settings.
			 */
			if (GUC_RESET_SOURCE(gconf) == PGC_S_FILE)
				*GUC_RESET_SOURCE_REF(gconf) = PGC_S_DEFAULT;
			if (GUC_SOURCE(gconf) == PGC_S_FILE)
				set_guc_source(gconf, PGC_S_DEFAULT);
			for (GucStack *stack = GUC_STACK(gconf); stack; stack = stack->prev)
			{
				if (stack->source == PGC_S_FILE)
					stack->source = PGC_S_DEFAULT;
			}

			/* Now we can re-apply the wired-in default (i.e., the boot_val) */
			if (set_config_option(gconf->name, NULL,
								  context, PGC_S_DEFAULT,
								  GUC_ACTION_SET, true, 0, false) > 0)
			{
				/* Log the change if appropriate */
				if (context == PGC_SIGHUP)
					ereport(elevel,
							(errmsg("parameter \"%s\" removed from configuration file, reset to default",
									gconf->name)));
			}
		}
	}

	/*
	 * Restore any variables determined by environment variables or
	 * dynamically-computed defaults.  This is a no-op except in the case
	 * where one of these had been in the config file and is now removed.
	 *
	 * In particular, we *must not* do this during the postmaster's initial
	 * loading of the file, since the timezone functions in particular should
	 * be run only after initialization is complete.
	 *
	 * XXX this is an unmaintainable crock, because we have to know how to set
	 * (or at least what to call to set) every non-PGC_INTERNAL variable that
	 * could potentially have PGC_S_DYNAMIC_DEFAULT or PGC_S_ENV_VAR source.
	 */
	if (context == PGC_SIGHUP && applySettings)
	{
		InitializeGUCOptionsFromEnvironment();
		pg_timezone_abbrev_initialize();
		/* this selects SQL_ASCII in processes not connected to a database */
		SetConfigOption("client_encoding", GetDatabaseEncodingName(),
						PGC_BACKEND, PGC_S_DYNAMIC_DEFAULT);
	}

	/*
	 * Now apply the values from the config file.
	 */
	for (ConfigVariable *item = head; item; item = item->next)
	{
		char	   *pre_value = NULL;
		int			scres;

		/* Ignore anything marked as ignorable */
		if (item->ignore)
			continue;

		/* In SIGHUP cases in the postmaster, we want to report changes */
		if (context == PGC_SIGHUP && applySettings && !IsUnderPostmaster)
		{
			const char *preval = GetConfigOption(item->name, true, false);

			/* If option doesn't exist yet or is NULL, treat as empty string */
			if (!preval)
				preval = "";
			/* must dup, else might have dangling pointer below */
			pre_value = pstrdup(preval);
		}

		scres = set_config_option(item->name, item->value,
								  context, PGC_S_FILE,
								  GUC_ACTION_SET, applySettings, 0, false);
		if (scres > 0)
		{
			/* variable was updated, so log the change if appropriate */
			if (pre_value)
			{
				const char *post_value = GetConfigOption(item->name, true, false);

				if (!post_value)
					post_value = "";
				if (strcmp(pre_value, post_value) != 0)
					ereport(elevel,
							(errmsg("parameter \"%s\" changed to \"%s\"",
									item->name, item->value)));
			}
			item->applied = true;
		}
		else if (scres == 0)
		{
			error = true;
			item->errmsg = pstrdup("setting could not be applied");
			ConfFileWithError = item->filename;
		}
		else
		{
			/* no error, but variable's active value was not changed */
			item->applied = true;
		}

		/*
		 * We should update source location unless there was an error, since
		 * even if the active value didn't change, the reset value might have.
		 * (In the postmaster, there won't be a difference, but it does matter
		 * in backends.)
		 */
		if (scres != 0 && applySettings)
			set_config_sourcefile(item->name, item->filename,
								  item->sourceline);

		if (pre_value)
			pfree(pre_value);
	}

	/* Remember when we last successfully loaded the config file. */
	if (applySettings)
		PgReloadTime = GetCurrentTimestamp();

bail_out:
	if (error && applySettings)
	{
		/* During postmaster startup, any error is fatal */
		if (context == PGC_POSTMASTER)
			ereport(ERROR,
					(errcode(ERRCODE_CONFIG_FILE_ERROR),
					 errmsg("configuration file \"%s\" contains errors",
							ConfFileWithError)));
		else if (applying)
			ereport(elevel,
					(errcode(ERRCODE_CONFIG_FILE_ERROR),
					 errmsg("configuration file \"%s\" contains errors; unaffected changes were applied",
							ConfFileWithError)));
		else
			ereport(elevel,
					(errcode(ERRCODE_CONFIG_FILE_ERROR),
					 errmsg("configuration file \"%s\" contains errors; no changes were applied",
							ConfFileWithError)));
	}

	/* Successful or otherwise, return the collected data list */
	return head;
}


/*
 * Some infrastructure for GUC-related memory allocation
 *
 * These functions are generally modeled on libc's malloc/realloc/etc,
 * but any OOM issue is reported at the specified elevel.
 * (Thus, control returns only if that's less than ERROR.)
 */
void *
guc_malloc(int elevel, size_t size)
{
	void	   *data;

	data = MemoryContextAllocExtended(GUCMemoryContext, size,
									  MCXT_ALLOC_NO_OOM);
	if (unlikely(data == NULL))
		ereport(elevel,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	return data;
}

void *
guc_realloc(int elevel, void *old, size_t size)
{
	void	   *data;

	if (old != NULL)
	{
		/* This is to help catch old code that malloc's GUC data. */
		Assert(GetMemoryChunkContext(old) == GUCMemoryContext);
		data = repalloc_extended(old, size,
								 MCXT_ALLOC_NO_OOM);
	}
	else
	{
		/* Like realloc(3), but not like repalloc(), we allow old == NULL. */
		data = MemoryContextAllocExtended(GUCMemoryContext, size,
										  MCXT_ALLOC_NO_OOM);
	}
	if (unlikely(data == NULL))
		ereport(elevel,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	return data;
}

char *
guc_strdup(int elevel, const char *src)
{
	char	   *data;
	size_t		len = strlen(src) + 1;

	data = guc_malloc(elevel, len);
	if (likely(data != NULL))
		memcpy(data, src, len);
	return data;
}

void
guc_free(void *ptr)
{
	/*
	 * Historically, GUC-related code has relied heavily on the ability to do
	 * free(NULL), so we allow that here even though pfree() doesn't.
	 */
	if (ptr != NULL)
	{
		/* This is to help catch old code that malloc's GUC data. */
		Assert(GetMemoryChunkContext(ptr) == GUCMemoryContext);
		pfree(ptr);
	}
}

/*
 * guc_free_if_current_context --- free a GUC allocation only if it belongs to
 * the CURRENT GUCMemoryContext; otherwise leave it alone.
 *
 * The threaded runtime gives each session its own GUCMemoryContext, but GUC
 * records are shared metadata (guc_variables[]) whose string/extra allocations
 * for PGC_POSTMASTER options (e.g. config_file, data_directory) were made once
 * by the postmaster into the PROCESS GUC context, not any session's.  A
 * session's backend-exit reset (ResetGUCStateAtBackendExit) walks ALL records,
 * so it must not guc_free() a pointer owned by a different (process) GUC
 * context -- guc_free()'s own assert (GetMemoryChunkContext == GUCMemoryContext)
 * catches exactly that, and in a release build pfree() on the wrong context
 * would corrupt it.  Reclaim only what this session's context owns; the process
 * context's allocations are released when the process itself exits.
 */
static void
guc_free_if_current_context(void *ptr)
{
	if (ptr != NULL && GetMemoryChunkContext(ptr) == GUCMemoryContext)
		pfree(ptr);
}


/*
 * Detect whether strval is referenced anywhere in a GUC string item
 */
static bool
string_field_used(struct config_generic *conf, char *strval)
{
	if (strval == *GUC_VARIABLE_STRING(conf) ||
		strval == GUC_RESET_STRING(conf) ||
		strval == conf->_string.boot_val ||
		strval == GUC_LAST_REPORTED(conf))
		return true;
	for (GucStack *stack = GUC_STACK(conf); stack; stack = stack->prev)
	{
		if (strval == stack->prior.val.stringval ||
			strval == stack->masked.val.stringval)
			return true;
	}
	return false;
}

/*
 * Forget the last value reported to the frontend.  In threaded builds, copied
 * session GUC records can transiently have last_reported sharing storage with
 * another string field.  Avoid freeing such storage until the owning field is
 * replaced or discarded.
 */
static void
clear_last_reported(struct config_generic *conf)
{
	char	   *last_reported = GUC_LAST_REPORTED(conf);

	if (last_reported == NULL)
		return;

	GUC_SET_LAST_REPORTED(conf, NULL);
	if (conf->vartype != PGC_STRING ||
		!string_field_used(conf, last_reported))
		guc_free(last_reported);
}

static char *
canonicalize_default_string_value(struct config_generic *conf, char *newval)
{
	const char *boot_val = conf->_string.boot_val;

	if (boot_val == NULL || newval == NULL || newval == boot_val)
		return newval;
	if (strcmp(newval, boot_val) != 0)
		return newval;

	guc_free(newval);
	return unconstify(char *, boot_val);
}

static void
guc_free_string_value(struct config_generic *conf, char *strval)
{
	if (strval != NULL && strval != conf->_string.boot_val)
		guc_free(strval);
}

/*
 * Support for assigning to a field of a string GUC item.  Free the prior
 * value if it's not referenced anywhere else in the item (including stacked
 * states).
 */
static void
set_string_field(struct config_generic *conf, char **field, char *newval)
{
	char	   *oldval = *field;

	/* Do the assignment */
	*field = newval;

	/*
	 * Free old value if it's not NULL and isn't referenced anymore.
	 *
	 * Under the threaded runtime a GUC record is shared metadata whose string
	 * allocations may have been made once by the postmaster (or an earlier
	 * startup fiber) into the PROCESS/early-fallback GUC context, not this
	 * session's GUCMemoryContext -- e.g. a PGC_POSTMASTER/PGC_SIGHUP option's
	 * boot/reset string set before any session existed.  A backend session
	 * that later replaces such a field (config reload, SET/RESET) must not
	 * pfree() a pointer owned by a foreign GUC context: guc_free()'s assert
	 * (GetMemoryChunkContext == GUCMemoryContext) catches exactly that, and a
	 * release-build pfree() on the wrong context would corrupt it (or another
	 * session's context under migration).  Reclaim only what this session owns;
	 * the process/early context frees its own allocations at process exit.
	 * Gated on the threaded runtime so process mode (and the non-USE_XTC_CARRIER
	 * build) keeps the exact original unconditional free -- byte-for-byte.
	 */
	if (oldval && !string_field_used(conf, oldval))
	{
#ifdef USE_XTC_CARRIER
		if (multithreaded)
			guc_free_if_current_context(oldval);
		else
#endif
			guc_free(oldval);
	}
}

/*
 * Detect whether an "extra" struct is referenced anywhere in a GUC item
 */
static bool
extra_field_used(struct config_generic *gconf, void *extra)
{
	if (extra == GUC_EXTRA(gconf))
		return true;
	if (extra == GUC_RESET_EXTRA(gconf))
		return true;
	for (GucStack *stack = GUC_STACK(gconf); stack; stack = stack->prev)
	{
		if (extra == stack->prior.extra ||
			extra == stack->masked.extra)
			return true;
	}

	return false;
}

/*
 * Support for assigning to an "extra" field of a GUC item.  Free the prior
 * value if it's not referenced anywhere else in the item (including stacked
 * states).
 */
static void
set_extra_field(struct config_generic *gconf, void **field, void *newval)
{
	void	   *oldval = *field;

	/* Do the assignment */
	*field = newval;

	/*
	 * Free old value if it's not NULL and isn't referenced anymore.  As in
	 * set_string_field(), the prior "extra" struct may be owned by the shared
	 * process/early-fallback GUC context (a boot/reset extra computed before
	 * any session existed), so a session must only reclaim what its own
	 * GUCMemoryContext owns -- see the ownership note there.  Threaded-only;
	 * process mode keeps the original unconditional free byte-for-byte.
	 */
	if (oldval && !extra_field_used(gconf, oldval))
	{
#ifdef USE_XTC_CARRIER
		if (multithreaded)
			guc_free_if_current_context(oldval);
		else
#endif
			guc_free(oldval);
	}
}

static void
clear_guc_stack(struct config_generic *gconf)
{
	GucStack   *stack;

	while ((stack = GUC_STACK(gconf)) != NULL)
	{
		GUC_SET_STACK(gconf, stack->prev);

		if (gconf->vartype == PGC_STRING)
		{
			set_string_field(gconf, &stack->prior.val.stringval, NULL);
			set_string_field(gconf, &stack->masked.val.stringval, NULL);
		}
		set_extra_field(gconf, &stack->prior.extra, NULL);
		set_extra_field(gconf, &stack->masked.extra, NULL);
		guc_free(stack);
	}
}

static void
reset_guc_record_at_backend_exit(struct config_generic *gconf)
{
	void	   *extra = GUC_EXTRA(gconf);
	void	   *reset_extra = GUC_RESET_EXTRA(gconf);

	RemoveGUCFromLists(gconf);
	clear_guc_stack(gconf);
	clear_last_reported(gconf);
	guc_free_if_current_context(GUC_SOURCEFILE(gconf));
	GUC_SET_SOURCEFILE(gconf, NULL);

	if (gconf->vartype == PGC_STRING)
	{
		/*
		 * Free string values only if they belong to THIS session's GUC
		 * context.  A PGC_POSTMASTER option's strings (e.g. config_file) were
		 * allocated once by the postmaster into the process GUC context; a
		 * session's exit reset must not free those (see
		 * guc_free_if_current_context).  Clear the fields regardless so the
		 * record is left in a clean state.  The !string_field_used guard mirrors
		 * set_string_field(): a string shared by the variable and reset fields
		 * must be freed once, not twice.
		 */
		if (GUCRecordVariableIsCurrentSessionOwned(gconf))
		{
			char	   *varval = *GUC_VARIABLE_STRING(gconf);

			*GUC_VARIABLE_STRING(gconf) = NULL;
			if (varval != NULL && !string_field_used(gconf, varval))
				guc_free_if_current_context(varval);
		}
		{
			char	   *resetval = GUC_RESET_STRING(gconf);

			GUC_RESET_STRING(gconf) = NULL;
			if (resetval != NULL && !string_field_used(gconf, resetval))
				guc_free_if_current_context(resetval);
		}
	}

	GUC_SET_EXTRA(gconf, NULL);
	if (extra != NULL && !extra_field_used(gconf, extra))
		guc_free_if_current_context(extra);

	GUC_SET_RESET_EXTRA(gconf, NULL);
	if (reset_extra != NULL && !extra_field_used(gconf, reset_extra))
		guc_free_if_current_context(reset_extra);

	GUC_STATUS(gconf) = 0;
	GUC_SOURCE(gconf) = PGC_S_DEFAULT;
	GUC_SCONTEXT(gconf) = PGC_INTERNAL;
	GUC_SROLE(gconf) = BOOTSTRAP_SUPERUSERID;
}

void
ResetGUCStateAtBackendExit(void)
{
	if (guc_variables == NULL)
		return;

	for (int i = 0; i < num_guc_variables; i++)
		reset_guc_record_at_backend_exit(&guc_variables[i]);

	if (guc_hashtab != NULL)
	{
		HASH_SEQ_STATUS status;
		GUCHashEntry *hentry;

		hash_seq_init(&status, guc_hashtab);
		while ((hentry = (GUCHashEntry *) hash_seq_search(&status)) != NULL)
			reset_guc_record_at_backend_exit(hentry->gucvar);
	}
}

/*
 * Support for copying a variable's active value into a stack entry.
 * The "extra" field associated with the active value is copied, too.
 *
 * NB: be sure stringval and extra fields of a new stack entry are
 * initialized to NULL before this is used, else we'll try to guc_free() them.
 */
static void
set_stack_value(struct config_generic *gconf, config_var_value *val)
{
	switch (gconf->vartype)
	{
		case PGC_BOOL:
			val->val.boolval = *GUC_VARIABLE_BOOL(gconf);
			break;
		case PGC_INT:
			val->val.intval = *GUC_VARIABLE_INT(gconf);
			break;
		case PGC_REAL:
			val->val.realval = *GUC_VARIABLE_REAL(gconf);
			break;
		case PGC_STRING:
			set_string_field(gconf, &(val->val.stringval), *GUC_VARIABLE_STRING(gconf));
			break;
		case PGC_ENUM:
			val->val.enumval = *GUC_VARIABLE_ENUM(gconf);
			break;
	}
	set_extra_field(gconf, &(val->extra), GUC_EXTRA(gconf));
}

/*
 * Support for discarding a no-longer-needed value in a stack entry.
 * The "extra" field associated with the stack entry is cleared, too.
 */
static void
discard_stack_value(struct config_generic *gconf, config_var_value *val)
{
	switch (gconf->vartype)
	{
		case PGC_BOOL:
		case PGC_INT:
		case PGC_REAL:
		case PGC_ENUM:
			/* no need to do anything */
			break;
		case PGC_STRING:
			set_string_field(gconf,
							 &(val->val.stringval),
							 NULL);
			break;
	}
	set_extra_field(gconf, &(val->extra), NULL);
}


/*
 * Fetch a palloc'd, sorted array of GUC struct pointers
 *
 * The array length is returned into *num_vars.
 */
struct config_generic **
get_guc_variables(int *num_vars)
{
	struct config_generic **result;
	HASH_SEQ_STATUS status;
	GUCHashEntry *hentry;
	int			i;

	*num_vars = num_guc_variables + guc_custom_variable_count();
	result = palloc_array(struct config_generic *, *num_vars);

	/* Extract pointers from the built-in array and custom hash table. */
	i = 0;
	for (int j = 0; j < num_guc_variables; j++)
		result[i++] = &guc_variables[j];
	if (guc_hashtab != NULL)
	{
		hash_seq_init(&status, guc_hashtab);
		while ((hentry = (GUCHashEntry *) hash_seq_search(&status)) != NULL)
			result[i++] = hentry->gucvar;
	}
	Assert(i == *num_vars);

	/* Sort by name */
	qsort(result, *num_vars,
		  sizeof(struct config_generic *), guc_var_compare);

	return result;
}


/*
 * Build the GUC hash table.  This is split out so that help_config.c can
 * extract all the variables without running all of InitializeGUCOptions.
 * It's not meant for use anyplace else.
 */
void
build_guc_variables(void)
{
	int			num_vars = 0;

	/*
	 * Create the memory context that will hold all GUC-related data.
	 *
	 * Peek the cell without the allocating accessor: reading GUCMemoryContext
	 * (== *PgCurrentGUCMemoryContextRef()) here would, before any session is
	 * installed, lazily create the early-fallback context and make this Assert
	 * trip on the value the read itself produced.  See
	 * PgCurrentGUCMemoryContextPeek().
	 */
	Assert(PgCurrentGUCMemoryContextPeek() == NULL);
	GUCMemoryContext =
		PgRuntimeGetOwnedMemoryContextWithSizes(PgCurrentGUCMemoryContextRef(),
												"GUCMemoryContext",
												ALLOCSET_START_SMALL_SIZES);

	/*
	 * Count all the built-in variables.
	 */
	for (int i = 0; ConfigureNames[i].name; i++)
		num_vars++;
	ensure_builtin_guc_name_index();
	num_guc_variables = num_vars;
	guc_variables = ConfigureNames;
	guc_variable_states = MemoryContextAllocZero(GUCMemoryContext,
												 sizeof(config_generic_state) *
												 (num_vars + 1));
	InitializeGUCVariableStatePointers();

	dlist_init(&guc_nondef_list);
	slist_init(&guc_stack_list);
	slist_init(&guc_report_list);
}

static void
InitializeGUCVariableStatePointers(void)
{
	struct config_generic *variables;

	Assert(guc_variables == ConfigureNames);
	Assert(guc_variable_states != NULL);

	/*
	 * The generated binder writes through config_generic._type.variable
	 * fields.  Keep that generated API as a compatibility bridge, but do it
	 * against a short-lived copy and retain only the per-session live backing
	 * addresses in config_generic_state.
	 */
	variables = MemoryContextAlloc(TopMemoryContext,
								   sizeof(struct config_generic) *
								   (num_guc_variables + 1));
	memcpy(variables, ConfigureNames,
		   sizeof(struct config_generic) * (num_guc_variables + 1));
	InitializeGUCVariablePointers(variables);

	for (int i = 0; i < num_guc_variables; i++)
	{
		config_generic_state *state = &guc_variable_states[i];

		switch (guc_variables[i].vartype)
		{
			case PGC_BOOL:
				state->variable.boolvar = variables[i]._bool.variable;
				break;
			case PGC_INT:
				state->variable.intvar = variables[i]._int.variable;
				break;
			case PGC_REAL:
				state->variable.realvar = variables[i]._real.variable;
				break;
			case PGC_STRING:
				state->variable.stringvar = variables[i]._string.variable;
				break;
			case PGC_ENUM:
				state->variable.enumvar = variables[i]._enum.variable;
				break;
		}
	}

	pfree(variables);
}

/*
 * Add a new GUC variable to the hash of known variables. The
 * hash is expanded if needed.
 */
static bool
add_guc_variable(struct config_generic *var, int elevel)
{
	HTAB	   *custom_hashtab;
	GUCHashEntry *hentry;
	bool		found;

	custom_hashtab = ensure_guc_custom_hashtab(guc_custom_variable_count() + 1);
	hentry = (GUCHashEntry *) hash_search(custom_hashtab,
										  &var->name,
										  HASH_ENTER_NULL,
										  &found);
	if (unlikely(hentry == NULL))
	{
		ereport(elevel,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
		return false;			/* out of memory */
	}
	Assert(!found);
	hentry->gucvar = var;
	return true;
}

/*
 * Decide whether a proposed custom variable name is allowed.
 *
 * It must be two or more identifiers separated by dots, where the rules
 * for what is an identifier agree with scan.l.  (If you change this rule,
 * adjust the errdetail in assignable_custom_variable_name().)
 */
static bool
valid_custom_variable_name(const char *name)
{
	bool		saw_sep = false;
	bool		name_start = true;

	for (const char *p = name; *p; p++)
	{
		if (*p == GUC_QUALIFIER_SEPARATOR)
		{
			if (name_start)
				return false;	/* empty name component */
			saw_sep = true;
			name_start = true;
		}
		else if (strchr("ABCDEFGHIJKLMNOPQRSTUVWXYZ"
						"abcdefghijklmnopqrstuvwxyz_", *p) != NULL ||
				 IS_HIGHBIT_SET(*p))
		{
			/* okay as first or non-first character */
			name_start = false;
		}
		else if (!name_start && strchr("0123456789$", *p) != NULL)
			 /* okay as non-first character */ ;
		else
			return false;
	}
	if (name_start)
		return false;			/* empty name component */
	/* OK if we found at least one separator */
	return saw_sep;
}

/*
 * Decide whether an unrecognized variable name is allowed to be SET.
 *
 * It must pass the syntactic rules of valid_custom_variable_name(),
 * and it must not be in any namespace already reserved by an extension.
 * (We make this separate from valid_custom_variable_name() because we don't
 * apply the reserved-namespace test when reading configuration files.)
 *
 * If valid, return true.  Otherwise, return false if skip_errors is true,
 * else throw a suitable error at the specified elevel (and return false
 * if that's less than ERROR).
 */
static bool
assignable_custom_variable_name(const char *name, bool skip_errors, int elevel)
{
	/* If there's no separator, it can't be a custom variable */
	const char *sep = strchr(name, GUC_QUALIFIER_SEPARATOR);

	if (sep != NULL)
	{
		size_t		classLen = sep - name;
		ListCell   *lc;

		/* The name must be syntactically acceptable ... */
		if (!valid_custom_variable_name(name))
		{
			if (!skip_errors)
				ereport(elevel,
						(errcode(ERRCODE_INVALID_NAME),
						 errmsg("invalid configuration parameter name \"%s\"",
								name),
						 errdetail("Custom parameter names must be two or more simple identifiers separated by dots.")));
			return false;
		}
		/* ... and it must not match any previously-reserved prefix */
		foreach(lc, reserved_class_prefix)
		{
			const char *rcprefix = lfirst(lc);

			if (strlen(rcprefix) == classLen &&
				strncmp(name, rcprefix, classLen) == 0)
			{
				if (!skip_errors)
					ereport(elevel,
							(errcode(ERRCODE_INVALID_NAME),
							 errmsg("invalid configuration parameter name \"%s\"",
									name),
							 errdetail("\"%s\" is a reserved prefix.",
									   rcprefix)));
				return false;
			}
		}
		/* OK to create it */
		return true;
	}

	/* Unrecognized single-part name */
	if (!skip_errors)
		ereport(elevel,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("unrecognized configuration parameter \"%s\"",
						name)));
	return false;
}

/*
 * Create and add a placeholder variable for a custom variable name.
 */
static struct config_generic *
add_placeholder_variable(const char *name, int elevel)
{
	struct config_generic *var;
	config_generic_state *state;

	var = (struct config_generic *) guc_malloc(elevel,
											   sizeof(struct config_generic));
	if (var == NULL)
		return NULL;
	memset(var, 0, sizeof(struct config_generic));

	state = (config_generic_state *) guc_malloc(elevel,
												sizeof(config_generic_state) +
												sizeof(char *));
	if (state == NULL)
	{
		guc_free(var);
		return NULL;
	}
	memset(state, 0, sizeof(config_generic_state) + sizeof(char *));
	var->state = state;

	var->name = guc_strdup(elevel, name);
	if (var->name == NULL)
	{
		guc_free(state);
		guc_free(var);
		return NULL;
	}

	var->context = PGC_USERSET;
	var->group = CUSTOM_OPTIONS;
	var->short_desc = "GUC placeholder variable";
	var->flags = GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_CUSTOM_PLACEHOLDER;
	var->vartype = PGC_STRING;

	/*
	 * The char* is allocated at the end of the struct since we have no
	 * 'static' place to point to.  Note that the current value, as well as
	 * the boot and reset values, start out NULL.
	 */
	GUC_VARIABLE_STRING(var) = (char **) (state + 1);
	state->variable.stringvar = GUC_VARIABLE_STRING(var);

	if (!add_guc_variable(var, elevel))
	{
		guc_free(unconstify(char *, var->name));
		guc_free(state);
		guc_free(var);
		return NULL;
	}

	return var;
}

/*
 * Look up option "name".  If it exists, return a pointer to its record.
 * Otherwise, if create_placeholders is true and name is a valid-looking
 * custom variable name, we'll create and return a placeholder record.
 * Otherwise, if skip_errors is true, then we silently return NULL for
 * an unrecognized or invalid name.  Otherwise, the error is reported at
 * error level elevel (and we return NULL if that's less than ERROR).
 *
 * Note: internal errors, primarily out-of-memory, draw an elevel-level
 * report and NULL return regardless of skip_errors.  Hence, callers must
 * handle a NULL return whenever elevel < ERROR, but they should not need
 * to emit any additional error message.  (In practice, internal errors
 * can only happen when create_placeholders is true, so callers passing
 * false need not think terribly hard about this.)
 */
struct config_generic *
find_option(const char *name, bool create_placeholders, bool skip_errors,
			int elevel)
{
	GUCHashEntry *hentry;
	struct config_generic *record;

	Assert(name);

	/* Look it up using the shared built-in index, then custom variables. */
	record = find_builtin_option(name);
	if (record != NULL)
		return record;
	if (guc_hashtab != NULL)
	{
		hentry = (GUCHashEntry *) hash_search(guc_hashtab,
											  &name,
											  HASH_FIND,
											  NULL);
		if (hentry)
			return hentry->gucvar;
	}

	/*
	 * See if the name is an obsolete name for a variable.  We assume that the
	 * set of supported old names is short enough that a brute-force search is
	 * the best way.
	 */
	for (int i = 0; map_old_guc_names[i] != NULL; i += 2)
	{
		if (guc_name_compare(name, map_old_guc_names[i]) == 0)
			return find_option(map_old_guc_names[i + 1], false,
							   skip_errors, elevel);
	}

	if (create_placeholders)
	{
		/*
		 * Check if the name is valid, and if so, add a placeholder.
		 */
		if (assignable_custom_variable_name(name, skip_errors, elevel))
			return add_placeholder_variable(name, elevel);
		else
			return NULL;		/* error message, if any, already emitted */
	}

	/* Unknown name and we're not supposed to make a placeholder */
	if (!skip_errors)
		ereport(elevel,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("unrecognized configuration parameter \"%s\"",
						name)));
	return NULL;
}


/*
 * comparator for qsorting an array of GUC pointers
 */
static int
guc_var_compare(const void *a, const void *b)
{
	const struct config_generic *ca = *(const struct config_generic *const *) a;
	const struct config_generic *cb = *(const struct config_generic *const *) b;

	return guc_name_compare(ca->name, cb->name);
}

/*
 * the bare comparison function for GUC names
 */
int
guc_name_compare(const char *namea, const char *nameb)
{
	/*
	 * The temptation to use strcasecmp() here must be resisted, because the
	 * hash mapping has to remain stable across setlocale() calls. So, build
	 * our own with a simple ASCII-only downcasing.
	 */
	while (*namea && *nameb)
	{
		char		cha = *namea++;
		char		chb = *nameb++;

		if (cha >= 'A' && cha <= 'Z')
			cha += 'a' - 'A';
		if (chb >= 'A' && chb <= 'Z')
			chb += 'a' - 'A';
		if (cha != chb)
			return cha - chb;
	}
	if (*namea)
		return 1;				/* a is longer */
	if (*nameb)
		return -1;				/* b is longer */
	return 0;
}

/*
 * Hash function that's compatible with guc_name_compare
 */
static uint32
guc_name_hash(const void *key, Size keysize)
{
	uint32		result = 0;
	const char *name = *(const char *const *) key;

	while (*name)
	{
		char		ch = *name++;

		/* Case-fold in the same way as guc_name_compare */
		if (ch >= 'A' && ch <= 'Z')
			ch += 'a' - 'A';

		/* Merge into hash ... not very bright, but it needn't be */
		result = pg_rotate_left32(result, 5);
		result ^= (uint32) ch;
	}
	return result;
}

/*
 * Dynahash match function to use in guc_hashtab
 */
static int
guc_name_match(const void *key1, const void *key2, Size keysize)
{
	const char *name1 = *(const char *const *) key1;
	const char *name2 = *(const char *const *) key2;

	return guc_name_compare(name1, name2);
}

/*
 * Build the shared lookup table for immutable built-in GUC names.  The table
 * points at ConfigureNames[] indexes rather than per-session records, so
 * logical backend sessions can avoid rebuilding hundreds of identical hash
 * entries.
 */
static void
ensure_builtin_guc_name_index(void)
{
	int			num_vars = 0;
	int			size_vars;
	HASHCTL		hash_ctl;
	HTAB	   *builtin_hashtab;
	bool		found;

	if (guc_builtin_hashtab != NULL)
		return;

	Assert(TopMemoryContext != NULL);

	for (int i = 0; ConfigureNames[i].name; i++)
		num_vars++;

	size_vars = num_vars + num_vars / 4;

	hash_ctl.keysize = sizeof(char *);
	hash_ctl.entrysize = sizeof(GUCBuiltinHashEntry);
	hash_ctl.hash = guc_name_hash;
	hash_ctl.match = guc_name_match;
	hash_ctl.hcxt = TopMemoryContext;
	builtin_hashtab = hash_create("GUC builtin lookup table",
								  size_vars,
								  &hash_ctl,
								  HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);

	for (int i = 0; i < num_vars; i++)
	{
		const char *name = ConfigureNames[i].name;
		GUCBuiltinHashEntry *hentry;

		hentry = (GUCBuiltinHashEntry *) hash_search(builtin_hashtab,
													 &name,
													 HASH_ENTER,
													 &found);
		Assert(!found);
		hentry->index = i;
	}

	guc_builtin_hashtab = builtin_hashtab;
}

static struct config_generic *
find_builtin_option(const char *name)
{
	GUCBuiltinHashEntry *hentry;

	if (guc_variables == NULL)
		return NULL;

	ensure_builtin_guc_name_index();
	hentry = (GUCBuiltinHashEntry *) hash_search(guc_builtin_hashtab,
												 &name,
												 HASH_FIND,
												 NULL);
	if (hentry == NULL)
		return NULL;

	Assert(hentry->index >= 0);
	Assert(hentry->index < num_guc_variables);
	return &guc_variables[hentry->index];
}

static int
guc_custom_variable_count(void)
{
	if (guc_hashtab == NULL)
		return 0;

	return hash_get_num_entries(guc_hashtab);
}

static Size
guc_cstring_payload_size(const char *str)
{
	return str != NULL ? strlen(str) + 1 : 0;
}

static void
guc_record_string_payload_size(const struct config_generic *gconf,
							   Size *current_string_bytes,
							   Size *reset_string_bytes)
{
	const char *current;
	const char *reset;

	if (gconf->vartype != PGC_STRING)
		return;

	current = *GUC_VARIABLE_STRING(gconf);
	if (current == NULL || current == gconf->_string.boot_val)
		current = NULL;
	else
		*current_string_bytes += guc_cstring_payload_size(current);

	reset = GUC_RESET_STRING(gconf);
	if (reset == NULL || reset == gconf->_string.boot_val || reset == current)
		reset = NULL;
	else
		*reset_string_bytes += guc_cstring_payload_size(reset);

	return;
}

static Size
guc_record_stack_memory(const struct config_generic *gconf, Size *stack_count)
{
	Size		bytes = 0;
	const char *current = NULL;
	const char *reset = NULL;

	if (gconf->vartype == PGC_STRING)
	{
		current = *GUC_VARIABLE_STRING(gconf);
		reset = GUC_RESET_STRING(gconf);
	}

	for (GucStack *stack = GUC_STACK(gconf); stack; stack = stack->prev)
	{
		(*stack_count)++;
		bytes += sizeof(GucStack);

		if (gconf->vartype == PGC_STRING)
		{
			if (stack->prior.val.stringval != NULL &&
				stack->prior.val.stringval != gconf->_string.boot_val &&
				stack->prior.val.stringval != current &&
				stack->prior.val.stringval != reset)
				bytes += guc_cstring_payload_size(stack->prior.val.stringval);
			if (stack->masked.val.stringval != NULL &&
				stack->masked.val.stringval != gconf->_string.boot_val &&
				stack->masked.val.stringval != current &&
				stack->masked.val.stringval != reset &&
				stack->masked.val.stringval != stack->prior.val.stringval)
				bytes += guc_cstring_payload_size(stack->masked.val.stringval);
		}
	}

	return bytes;
}

static void
guc_record_memory_stats(const struct config_generic *gconf,
						Size *cold_count,
						Size *cold_direct_bytes,
						Size *current_string_bytes,
						Size *reset_string_bytes,
						Size *last_reported_bytes,
						Size *sourcefile_bytes,
						Size *stack_count,
						Size *stack_direct_bytes)
{
	config_generic_cold_state *cold;

	guc_record_string_payload_size(gconf, current_string_bytes,
								   reset_string_bytes);
	*stack_direct_bytes += guc_record_stack_memory(gconf, stack_count);

	cold = GUCRecordColdStateIfAllocated(gconf);
	if (cold == NULL)
		return;

	(*cold_count)++;
	*cold_direct_bytes += sizeof(config_generic_cold_state);
	*last_reported_bytes += guc_cstring_payload_size(cold->last_reported);
	*sourcefile_bytes += guc_cstring_payload_size(cold->sourcefile);
}

void
PgLogProtocolParkGUCMemory(uint32 backend_id, uint64 generation)
{
	MemoryContextCounters context;
	Size		context_used;
	Size		custom_count;
	Size		state_array_bytes;
	Size		cold_count = 0;
	Size		cold_direct_bytes = 0;
	Size		current_string_bytes = 0;
	Size		reset_string_bytes = 0;
	Size		last_reported_bytes = 0;
	Size		sourcefile_bytes = 0;
	Size		stack_count = 0;
	Size		stack_direct_bytes = 0;
	Size		custom_record_bytes = 0;
	Size		attributed_bytes;
	Size		unattributed_used_bytes;

	if (GUCMemoryContext == NULL)
		return;

	MemoryContextMemConsumed(GUCMemoryContext, &context);
	context_used = context.totalspace - context.freespace;
	custom_count = guc_custom_variable_count();
	state_array_bytes = sizeof(config_generic_state) * (num_guc_variables + 1);

	for (int i = 0; i < num_guc_variables; i++)
		guc_record_memory_stats(&guc_variables[i],
								&cold_count,
								&cold_direct_bytes,
								&current_string_bytes,
								&reset_string_bytes,
								&last_reported_bytes,
								&sourcefile_bytes,
								&stack_count,
								&stack_direct_bytes);

	if (guc_hashtab != NULL)
	{
		HASH_SEQ_STATUS status;
		GUCHashEntry *hentry;

		hash_seq_init(&status, guc_hashtab);
		while ((hentry = (GUCHashEntry *) hash_seq_search(&status)) != NULL)
		{
			struct config_generic *gconf = hentry->gucvar;

			custom_record_bytes += sizeof(struct config_generic);
			if (gconf->state != NULL)
				custom_record_bytes += sizeof(config_generic_state);
			guc_record_memory_stats(gconf,
									&cold_count,
									&cold_direct_bytes,
									&current_string_bytes,
									&reset_string_bytes,
									&last_reported_bytes,
									&sourcefile_bytes,
									&stack_count,
									&stack_direct_bytes);
		}
	}

	attributed_bytes = state_array_bytes + cold_direct_bytes +
		current_string_bytes + reset_string_bytes + last_reported_bytes +
		sourcefile_bytes + stack_direct_bytes + custom_record_bytes;
	unattributed_used_bytes = context_used > attributed_bytes ?
		context_used - attributed_bytes : 0;

	ereport(LOG_SERVER_ONLY,
			(errhidestmt(true),
			 errhidecontext(true),
			 errmsg_internal("protocol_park_guc_memory pid=%d backend_id=%u generation=%llu "
							 "context_total_bytes=%zu context_free_bytes=%zu context_used_bytes=%zu context_blocks=%zu "
							 "builtin_count=%d custom_count=%zu state_array_bytes=%zu "
							 "cold_count=%zu cold_direct_bytes=%zu "
							 "current_string_bytes=%zu reset_string_bytes=%zu "
							 "last_reported_bytes=%zu sourcefile_bytes=%zu "
							 "stack_count=%zu stack_direct_bytes=%zu custom_record_bytes=%zu "
							 "attributed_bytes=%zu unattributed_used_bytes=%zu",
							 PgCurrentBackendSignalPid(),
							 backend_id,
							 (unsigned long long) generation,
							 context.totalspace,
							 context.freespace,
							 context_used,
							 context.nblocks,
							 num_guc_variables,
							 custom_count,
							 state_array_bytes,
							 cold_count,
							 cold_direct_bytes,
							 current_string_bytes,
							 reset_string_bytes,
							 last_reported_bytes,
							 sourcefile_bytes,
							 stack_count,
							 stack_direct_bytes,
							 custom_record_bytes,
							 attributed_bytes,
							 unattributed_used_bytes)));
}

static HTAB *
ensure_guc_custom_hashtab(int nelem)
{
	HASHCTL		hash_ctl;

	if (guc_hashtab != NULL)
		return guc_hashtab;

	hash_ctl.keysize = sizeof(char *);
	hash_ctl.entrysize = sizeof(GUCHashEntry);
	hash_ctl.hash = guc_name_hash;
	hash_ctl.match = guc_name_match;
	hash_ctl.hcxt = GUCMemoryContext;
	guc_hashtab = hash_create("custom GUC hash table",
							  Max(nelem, 8),
							  &hash_ctl,
							  HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);

	return guc_hashtab;
}


/*
 * Convert a GUC name to the form that should be used in pg_parameter_acl.
 *
 * We need to canonicalize entries since, for example, case should not be
 * significant.  In addition, we apply the map_old_guc_names[] mapping so that
 * any obsolete names will be converted when stored in a new PG version.
 * Note however that this function does not verify legality of the name.
 *
 * The result is a palloc'd string.
 */
char *
convert_GUC_name_for_parameter_acl(const char *name)
{
	char	   *result;

	/* Apply old-GUC-name mapping. */
	for (int i = 0; map_old_guc_names[i] != NULL; i += 2)
	{
		if (guc_name_compare(name, map_old_guc_names[i]) == 0)
		{
			name = map_old_guc_names[i + 1];
			break;
		}
	}

	/* Apply case-folding that matches guc_name_compare(). */
	result = pstrdup(name);
	for (char *ptr = result; *ptr != '\0'; ptr++)
	{
		char		ch = *ptr;

		if (ch >= 'A' && ch <= 'Z')
		{
			ch += 'a' - 'A';
			*ptr = ch;
		}
	}

	return result;
}

/*
 * Check whether we should allow creation of a pg_parameter_acl entry
 * for the given name.  (This can be applied either before or after
 * canonicalizing it.)  Throws error if not.
 */
void
check_GUC_name_for_parameter_acl(const char *name)
{
	/* OK if the GUC exists. */
	if (find_option(name, false, true, DEBUG5) != NULL)
		return;
	/* Otherwise, it'd better be a valid custom GUC name. */
	(void) assignable_custom_variable_name(name, false, ERROR);
}

/*
 * Routine in charge of checking various states of a GUC.
 *
 * This performs two sanity checks.  First, it checks that the initial
 * value of a GUC is the same when declared and when loaded to prevent
 * anybody looking at the C declarations of these GUCs from being fooled by
 * mismatched values.  Second, it checks for incorrect flag combinations.
 *
 * The following validation rules apply for the values:
 * bool - can be false, otherwise must be same as the boot_val
 * int  - can be 0, otherwise must be same as the boot_val
 * real - can be 0.0, otherwise must be same as the boot_val
 * string - can be NULL, otherwise must be strcmp equal to the boot_val
 * enum - must be same as the boot_val
 */
#ifdef USE_ASSERT_CHECKING
static bool
check_GUC_init(const struct config_generic *gconf)
{
	/* Checks on values */
	switch (gconf->vartype)
	{
		case PGC_BOOL:
			{
				const struct config_bool *conf = &gconf->_bool;
				bool	   *variable = GUC_VARIABLE_BOOL(gconf);

				if (*variable && !conf->boot_val)
				{
					elog(LOG, "GUC (PGC_BOOL) %s, boot_val=%d, C-var=%d",
						 gconf->name, conf->boot_val, *variable);
					return false;
				}
				break;
			}
		case PGC_INT:
			{
				const struct config_int *conf = &gconf->_int;
				int		   *variable = GUC_VARIABLE_INT(gconf);

				if (*variable != 0 && *variable != conf->boot_val)
				{
					elog(LOG, "GUC (PGC_INT) %s, boot_val=%d, C-var=%d",
						 gconf->name, conf->boot_val, *variable);
					return false;
				}
				break;
			}
		case PGC_REAL:
			{
				const struct config_real *conf = &gconf->_real;
				double	   *variable = GUC_VARIABLE_REAL(gconf);

				if (*variable != 0.0 && *variable != conf->boot_val)
				{
					elog(LOG, "GUC (PGC_REAL) %s, boot_val=%g, C-var=%g",
						 gconf->name, conf->boot_val, *variable);
					return false;
				}
				break;
			}
		case PGC_STRING:
			{
				const struct config_string *conf = &gconf->_string;
				char	  **variable = GUC_VARIABLE_STRING(gconf);

				if (*variable != NULL &&
					(conf->boot_val == NULL ||
					 strcmp(*variable, conf->boot_val) != 0))
				{
					elog(LOG, "GUC (PGC_STRING) %s, boot_val=%s, C-var=%s",
						 gconf->name, conf->boot_val ? conf->boot_val : "<null>", *variable);
					return false;
				}
				break;
			}
		case PGC_ENUM:
			{
				const struct config_enum *conf = &gconf->_enum;
				int		   *variable = GUC_VARIABLE_ENUM(gconf);

				if (*variable != conf->boot_val)
				{
					elog(LOG, "GUC (PGC_ENUM) %s, boot_val=%d, C-var=%d",
						 gconf->name, conf->boot_val, *variable);
					return false;
				}
				break;
			}
	}

	/* Flag combinations */

	/*
	 * GUC_NO_SHOW_ALL requires GUC_NOT_IN_SAMPLE, as a parameter not part of
	 * SHOW ALL should not be hidden in postgresql.conf.sample.
	 */
	if ((gconf->flags & GUC_NO_SHOW_ALL) &&
		!(gconf->flags & GUC_NOT_IN_SAMPLE))
	{
		elog(LOG, "GUC %s flags: NO_SHOW_ALL and !NOT_IN_SAMPLE",
			 gconf->name);
		return false;
	}

	return true;
}
#endif

/*
 * Initialize GUC options during program startup.
 *
 * Note that we cannot read the config file yet, since we have not yet
 * processed command-line switches.
 */
void
InitializeGUCOptions(void)
{
	/*
	 * Before log_line_prefix could possibly receive a nonempty setting, make
	 * sure that timezone processing is minimally alive (see elog.c).
	 */
	pg_timezone_initialize();

	/*
	 * Create GUCMemoryContext and build hash table of all GUC variables.
	 */
	build_guc_variables();

	/*
	 * Load all variables with their compiled-in defaults, and initialize
	 * status fields as needed.
	 */
	for (int i = 0; i < num_guc_variables; i++)
	{
		/* Check mapping between initial and default value */
		Assert(check_GUC_init(&guc_variables[i]));

		InitializeOneGUCOption(&guc_variables[i]);
	}

	reporting_enabled = false;

	/*
	 * Prevent any attempt to override the transaction modes from
	 * non-interactive sources.
	 */
	SetConfigOption("transaction_isolation", "read committed",
					PGC_POSTMASTER, PGC_S_OVERRIDE);
	SetConfigOption("transaction_read_only", "no",
					PGC_POSTMASTER, PGC_S_OVERRIDE);
	SetConfigOption("transaction_deferrable", "no",
					PGC_POSTMASTER, PGC_S_OVERRIDE);

	/*
	 * For historical reasons, some GUC parameters can receive defaults from
	 * environment variables.  Process those settings.
	 */
	InitializeGUCOptionsFromEnvironment();
}

/*
 * Initialize the early session GUC state needed by threaded backend startup.
 *
 * Process backends run InitializeGUCOptions() before shared-memory and catalog
 * initialization, then replay the postmaster's non-default configuration.
 * Threaded backend carriers share the postmaster address space, so they must
 * not reset every GUC variable to its boot default against process-global
 * storage while bootstrapping a session.  Build this carrier's GUC table,
 * rebind records whose backing variables now live in PgSession/PgExecution
 * state, and initialize exactly those rebound records.  That keeps the
 * per-session direct-variable state internally consistent without reapplying
 * boot defaults to the postmaster's shared process-global variables.
 */
void
InitializeThreadedSessionGUCOptions(void)
{
	bool		locked;

	/*
	 * Thread entry initializes GUC state before runtime installation so early
	 * fallback state can be adopted into PgSession.  InitPostgres() can reach
	 * this path again for normal client backends.
	 */
	if (guc_variables != NULL)
		return;

	locked = ThreadedGUCLock();
	PG_TRY();
	{
		if (guc_variables != NULL)
			goto done;

		build_guc_variables();

		RebindSessionGUCVariablePointers();
		InitializeThreadedSessionReboundGUCOptions();

		InitializeThreadedSessionCompatibilityGUCOptions();
done:
		;
	}
	PG_FINALLY();
	{
		ThreadedGUCUnlock(locked);
	}
	PG_END_TRY();
}

void
InitializeThreadedSessionRequiredGUCOptions(void)
{
	static const char *const compatibility_options[] = {
		"client_encoding",
	};
	bool		locked;

	if (guc_variables == NULL)
		return;

	locked = ThreadedGUCLock();
	PG_TRY();
	{
	/*
	 * RebindSessionGUCVariablePointers() can be called before the final
	 * PgSession is installed, so the pointer-change pass used during early GUC
	 * setup can miss string GUCs that already point at fallback session
	 * accessors.  After PgSetCurrentSession(), initialize any built-in string
	 * GUC whose backing pointer now lives inside the current PgSession and
	 * still lacks string storage.  That avoids writing boot defaults into
	 * process/runtime globals while keeping future PgSession-owned string GUCs
	 * out of a hand-maintained required list.
	 */
	for (int i = 0; i < num_guc_variables; i++)
	{
		struct config_generic *gconf = &guc_variables[i];

		if (gconf->vartype != PGC_STRING)
			continue;
		if (!PgCurrentSessionOwnsPointer(GUC_VARIABLE_STRING(gconf)))
			continue;
		if (*GUC_VARIABLE_STRING(gconf) == NULL)
			InitializeOneGUCOption(gconf);
	}

	/*
	 * client_encoding exposes a string GUC, but its authoritative state is the
	 * session encoding object rather than a direct char * field in PgSession.
	 * Keep this narrow compatibility exception until encoding GUC storage is
	 * represented as an ordinary session-owned string pointer.
	 */
	for (int i = 0; i < lengthof(compatibility_options); i++)
	{
		struct config_generic *gconf;

		gconf = find_option(compatibility_options[i], false, false, PANIC);
		Assert(gconf->vartype == PGC_STRING);
		if (*GUC_VARIABLE_STRING(gconf) == NULL)
			InitializeOneGUCOption(gconf);
	}
	}
	PG_FINALLY();
	{
		ThreadedGUCUnlock(locked);
	}
	PG_END_TRY();
}

static const void *
GUCOptionVariablePointer(struct config_generic *gconf)
{
	switch (gconf->vartype)
	{
		case PGC_BOOL:
			return GUC_VARIABLE_BOOL(gconf);
		case PGC_INT:
			return GUC_VARIABLE_INT(gconf);
		case PGC_REAL:
			return GUC_VARIABLE_REAL(gconf);
		case PGC_STRING:
			return GUC_VARIABLE_STRING(gconf);
		case PGC_ENUM:
			return GUC_VARIABLE_ENUM(gconf);
	}

	pg_unreachable();
}

static void
InitializeThreadedSessionReboundGUCOptions(void)
{
	for (int i = 0; i < num_guc_variables; i++)
	{
		struct config_generic *gconf = &guc_variables[i];
		const void *variable = GUCOptionVariablePointer(gconf);

		if (!PgCurrentOrEarlySessionOwnsPointer(variable))
		{
			InitializeOneGUCOptionResetMetadata(gconf);
			continue;
		}

		InitializeOneGUCOption(gconf);
	}
}

static void
InitializeThreadedSessionCompatibilityGUCOptions(void)
{
	static const char *const compatibility_options[] = {
		"session_authorization",
		"server_encoding",
		"client_encoding",
	};

	for (int i = 0; i < lengthof(compatibility_options); i++)
	{
		struct config_generic *gconf;

		gconf = find_option(compatibility_options[i], false, false, PANIC);
		InitializeOneGUCOption(gconf);
	}
}

/*
 * Refresh direct GUC variable pointers that now live behind the current
 * PgSession.  build_guc_variables() copies static GUC metadata and stores raw
 * C-variable addresses, so a later logical session switch must update any
 * records whose backing storage moved from TLS globals into PgSession.  The
 * built-in rebind registry is generated from threaded_accessor entries in
 * guc_parameters.dat, keeping ownership next to each GUC definition.
 */
static void
RebindSessionGUCVariablePointer(const ThreadedSessionGUCRebind *rebind)
{
	struct config_generic *gconf;

	gconf = find_option(rebind->name, false, false, PANIC);
	Assert(gconf->vartype == rebind->vartype);

	switch (rebind->vartype)
	{
		case PGC_BOOL:
			GUC_VARIABLE_BOOL(gconf) = rebind->accessor.bool_ref();
			break;
		case PGC_INT:
			GUC_VARIABLE_INT(gconf) = rebind->accessor.int_ref();
			break;
		case PGC_REAL:
			GUC_VARIABLE_REAL(gconf) = rebind->accessor.real_ref();
			break;
		case PGC_STRING:
			GUC_VARIABLE_STRING(gconf) = rebind->accessor.string_ref();
			break;
		case PGC_ENUM:
			GUC_VARIABLE_ENUM(gconf) = rebind->accessor.enum_ref();
			break;
	}
}

void
RebindSessionGUCVariablePointers(void)
{
	if (guc_variables == NULL)
		return;

	for (int i = 0; i < NumThreadedSessionGUCRebinds; i++)
		RebindSessionGUCVariablePointer(&ThreadedSessionGUCRebinds[i]);
}

int
ValidateSessionGUCVariableRebinds(void)
{
	if (guc_variables == NULL)
		return 0;

	for (int i = 0; i < NumThreadedSessionGUCRebinds; i++)
	{
		const ThreadedSessionGUCRebind *rebind = &ThreadedSessionGUCRebinds[i];
		struct config_generic *gconf;
		const void *expected;

		gconf = find_option(rebind->name, false, false, PANIC);
		if (gconf->vartype != rebind->vartype)
			elog(ERROR,
				 "session GUC rebind entry \"%s\" expected vartype %d, found %d",
				 rebind->name, rebind->vartype, gconf->vartype);

		switch (rebind->vartype)
		{
			case PGC_BOOL:
				expected = rebind->accessor.bool_ref();
				break;
			case PGC_INT:
				expected = rebind->accessor.int_ref();
				break;
			case PGC_REAL:
				expected = rebind->accessor.real_ref();
				break;
			case PGC_STRING:
				expected = rebind->accessor.string_ref();
				break;
			case PGC_ENUM:
				expected = rebind->accessor.enum_ref();
				break;
			default:
				pg_unreachable();
		}

		if (GUCOptionVariablePointer(gconf) != expected)
			elog(ERROR,
				 "session GUC rebind entry \"%s\" points at stale storage",
				 rebind->name);
	}

	return NumThreadedSessionGUCRebinds;
}

/*
 * Assign any GUC values that can come from the server's environment.
 *
 * This is called from InitializeGUCOptions, and also from ProcessConfigFile
 * to deal with the possibility that a setting has been removed from
 * postgresql.conf and should now get a value from the environment.
 * (The latter is a kludge that should probably go away someday; if so,
 * fold this back into InitializeGUCOptions.)
 */
static void
InitializeGUCOptionsFromEnvironment(void)
{
	char	   *env;
	ssize_t		stack_rlimit;

	env = getenv("PGPORT");
	if (env != NULL)
		SetConfigOption("port", env, PGC_POSTMASTER, PGC_S_ENV_VAR);

	env = getenv("PGDATESTYLE");
	if (env != NULL)
		SetConfigOption("datestyle", env, PGC_POSTMASTER, PGC_S_ENV_VAR);

	env = getenv("PGCLIENTENCODING");
	if (env != NULL)
		SetConfigOption("client_encoding", env, PGC_POSTMASTER, PGC_S_ENV_VAR);

	/*
	 * rlimit isn't exactly an "environment variable", but it behaves about
	 * the same.  If we can identify the platform stack depth rlimit, increase
	 * default stack depth setting up to whatever is safe (but at most 2MB).
	 * Report the value's source as PGC_S_DYNAMIC_DEFAULT if it's 2MB, or as
	 * PGC_S_ENV_VAR if it's reflecting the rlimit limit.
	 */
	stack_rlimit = get_stack_depth_rlimit();
	if (stack_rlimit > 0)
	{
		ssize_t		new_limit = (stack_rlimit - STACK_DEPTH_SLOP) / 1024;

		if (new_limit > 100)
		{
			GucSource	source;
			char		limbuf[16];

			if (new_limit < 2048)
				source = PGC_S_ENV_VAR;
			else
			{
				new_limit = 2048;
				source = PGC_S_DYNAMIC_DEFAULT;
			}
			snprintf(limbuf, sizeof(limbuf), "%zd", new_limit);
			SetConfigOption("max_stack_depth", limbuf,
							PGC_POSTMASTER, source);
		}
	}
}

/*
 * Initialize one GUC option variable to its compiled-in default.
 *
 * Note: the reason for calling check_hooks is not that we think the boot_val
 * might fail, but that the hooks might wish to compute an "extra" struct.
 */
static void
InitializeOneGUCOption(struct config_generic *gconf)
{
	void	   *extra = NULL;

	GUC_STATUS(gconf) = 0;
	GUC_SOURCE(gconf) = PGC_S_DEFAULT;
	GUC_SCONTEXT(gconf) = PGC_INTERNAL;
	GUC_SROLE(gconf) = BOOTSTRAP_SUPERUSERID;
	GUC_SET_STACK(gconf, NULL);
	GUC_SET_EXTRA(gconf, NULL);
	GUCRecordResetColdFields(gconf);

	switch (gconf->vartype)
	{
		case PGC_BOOL:
			{
				struct config_bool *conf = &gconf->_bool;
				bool		newval = conf->boot_val;

				if (!call_bool_check_hook(gconf, &newval, &extra,
										  PGC_S_DEFAULT, LOG))
					elog(FATAL, "failed to initialize %s to %d",
						 gconf->name, (int) newval);
				if (conf->assign_hook)
					conf->assign_hook(newval, extra);
				*GUC_VARIABLE_BOOL(gconf) = GUC_RESET_BOOL(gconf) = newval;
				break;
			}
		case PGC_INT:
			{
				struct config_int *conf = &gconf->_int;
				int			newval = conf->boot_val;

				Assert(newval >= conf->min);
				Assert(newval <= conf->max);
				if (!call_int_check_hook(gconf, &newval, &extra,
										 PGC_S_DEFAULT, LOG))
					elog(FATAL, "failed to initialize %s to %d",
						 gconf->name, newval);
				if (conf->assign_hook)
					conf->assign_hook(newval, extra);
				*GUC_VARIABLE_INT(gconf) = GUC_RESET_INT(gconf) = newval;
				break;
			}
		case PGC_REAL:
			{
				struct config_real *conf = &gconf->_real;
				double		newval = conf->boot_val;

				Assert(newval >= conf->min);
				Assert(newval <= conf->max);
				if (!call_real_check_hook(gconf, &newval, &extra,
										  PGC_S_DEFAULT, LOG))
					elog(FATAL, "failed to initialize %s to %g",
						 gconf->name, newval);
				if (conf->assign_hook)
					conf->assign_hook(newval, extra);
				*GUC_VARIABLE_REAL(gconf) = GUC_RESET_REAL(gconf) = newval;
				break;
			}
		case PGC_STRING:
			{
				struct config_string *conf = &gconf->_string;
				char	   *newval;

				/* non-NULL boot_val must always get strdup'd */
				if (conf->boot_val != NULL)
					newval = guc_strdup(FATAL, conf->boot_val);
				else
					newval = NULL;

				if (!call_string_check_hook(gconf, &newval, &extra,
											PGC_S_DEFAULT, LOG))
					elog(FATAL, "failed to initialize %s to \"%s\"",
						 gconf->name, newval ? newval : "");
				newval = canonicalize_default_string_value(gconf, newval);
				if (conf->assign_hook)
					conf->assign_hook(newval, extra);
				*GUC_VARIABLE_STRING(gconf) = GUC_RESET_STRING(gconf) = newval;
				break;
			}
		case PGC_ENUM:
			{
				struct config_enum *conf = &gconf->_enum;
				int			newval = conf->boot_val;

				if (!call_enum_check_hook(gconf, &newval, &extra,
										  PGC_S_DEFAULT, LOG))
					elog(FATAL, "failed to initialize %s to %d",
						 gconf->name, newval);
				if (conf->assign_hook)
					conf->assign_hook(newval, extra);
				*GUC_VARIABLE_ENUM(gconf) = GUC_RESET_ENUM(gconf) = newval;
				break;
			}
	}

	GUC_SET_EXTRA(gconf, extra);
	GUC_SET_RESET_EXTRA(gconf, extra);
}

/*
 * Initialize reset/default metadata for a GUC record without writing the live
 * backing variable.  Threaded backend sessions build a private GUC registry in
 * a process that already has postmaster/runtime GUC variables.  Records whose
 * variables are not session-owned still need valid reset values for RESET and
 * pg_settings, but initializing them with InitializeOneGUCOption() would
 * overwrite shared process/runtime state.
 */
static void
InitializeOneGUCOptionResetMetadata(struct config_generic *gconf)
{
	void	   *extra = NULL;

	GUC_STATUS(gconf) = 0;
	GUC_SOURCE(gconf) = PGC_S_DEFAULT;
	GUC_SCONTEXT(gconf) = PGC_INTERNAL;
	GUC_SROLE(gconf) = BOOTSTRAP_SUPERUSERID;
	GUC_SET_STACK(gconf, NULL);
	GUC_SET_EXTRA(gconf, NULL);
	GUCRecordResetColdFields(gconf);

	switch (gconf->vartype)
	{
		case PGC_BOOL:
			{
				struct config_bool *conf = &gconf->_bool;
				bool		newval = conf->boot_val;

				if (!call_bool_check_hook(gconf, &newval, &extra,
										  PGC_S_DEFAULT, LOG))
					elog(FATAL, "failed to initialize %s reset value to %d",
						 gconf->name, (int) newval);
				GUC_RESET_BOOL(gconf) = newval;
				break;
			}
		case PGC_INT:
			{
				struct config_int *conf = &gconf->_int;
				int			newval = conf->boot_val;

				Assert(newval >= conf->min);
				Assert(newval <= conf->max);
				if (!call_int_check_hook(gconf, &newval, &extra,
										 PGC_S_DEFAULT, LOG))
					elog(FATAL, "failed to initialize %s reset value to %d",
						 gconf->name, newval);
				GUC_RESET_INT(gconf) = newval;
				break;
			}
		case PGC_REAL:
			{
				struct config_real *conf = &gconf->_real;
				double		newval = conf->boot_val;

				Assert(newval >= conf->min);
				Assert(newval <= conf->max);
				if (!call_real_check_hook(gconf, &newval, &extra,
										  PGC_S_DEFAULT, LOG))
					elog(FATAL, "failed to initialize %s reset value to %g",
						 gconf->name, newval);
				GUC_RESET_REAL(gconf) = newval;
				break;
			}
		case PGC_STRING:
			{
				struct config_string *conf = &gconf->_string;
				char	   *newval;

				if (!PgCurrentOrEarlySessionOwnsPointer(GUC_VARIABLE_STRING(gconf)))
				{
					GUC_RESET_STRING(gconf) = *GUC_VARIABLE_STRING(gconf);
					break;
				}

				if (conf->boot_val != NULL)
					newval = guc_strdup(FATAL, conf->boot_val);
				else
					newval = NULL;

				if (!call_string_check_hook(gconf, &newval, &extra,
											PGC_S_DEFAULT, LOG))
					elog(FATAL, "failed to initialize %s reset value to \"%s\"",
						 gconf->name, newval ? newval : "");
				newval = canonicalize_default_string_value(gconf, newval);
				GUC_RESET_STRING(gconf) = newval;
				break;
			}
		case PGC_ENUM:
			{
				struct config_enum *conf = &gconf->_enum;
				int			newval = conf->boot_val;

				if (!call_enum_check_hook(gconf, &newval, &extra,
										  PGC_S_DEFAULT, LOG))
					elog(FATAL, "failed to initialize %s reset value to %d",
						 gconf->name, newval);
				GUC_RESET_ENUM(gconf) = newval;
				break;
			}
	}

	GUC_SET_RESET_EXTRA(gconf, extra);
}

/*
 * Summarily remove a GUC variable from any linked lists it's in.
 *
 * We use this in cases where the variable is about to be deleted or reset.
 * These aren't common operations, so it's okay if this is a bit slow.
 */
static void
RemoveGUCFromLists(struct config_generic *gconf)
{
	if (GUC_SOURCE(gconf) != PGC_S_DEFAULT)
		dlist_delete(GUC_NONDEF_LINK(gconf));
	if (GUC_STACK(gconf) != NULL)
		slist_delete(&guc_stack_list, GUC_STACK_LINK(gconf));
	if (GUC_STATUS(gconf) & GUC_NEEDS_REPORT)
		slist_delete(&guc_report_list, GUC_REPORT_LINK(gconf));
}


/*
 * Select the configuration files and data directory to be used, and
 * do the initial read of postgresql.conf.
 *
 * This is called after processing command-line switches.
 *		userDoption is the -D switch value if any (NULL if unspecified).
 *		progname is just for use in error messages.
 *
 * Returns true on success; on failure, prints a suitable error message
 * to stderr and returns false.
 */
bool
SelectConfigFiles(const char *userDoption, const char *progname)
{
	char	   *configdir;
	char	   *fname;
	bool		fname_is_malloced;
	struct stat stat_buf;
	struct config_generic *data_directory_rec;

	/* configdir is -D option, or $PGDATA if no -D */
	if (userDoption)
		configdir = make_absolute_path(userDoption);
	else
		configdir = make_absolute_path(getenv("PGDATA"));

	if (configdir && stat(configdir, &stat_buf) != 0)
	{
		write_stderr("%s: could not access directory \"%s\": %m\n",
					 progname,
					 configdir);
		if (errno == ENOENT)
			write_stderr("Run initdb or pg_basebackup to initialize a PostgreSQL data directory.\n");
		goto fail;
	}

	/*
	 * Find the configuration file: if config_file was specified on the
	 * command line, use it, else use configdir/postgresql.conf.  In any case
	 * ensure the result is an absolute path, so that it will be interpreted
	 * the same way by future backends.
	 */
	if (ConfigFileName)
	{
		fname = make_absolute_path(ConfigFileName);
		fname_is_malloced = true;
	}
	else if (configdir)
	{
		fname = guc_malloc(FATAL,
						   strlen(configdir) + strlen(CONFIG_FILENAME) + 2);
		sprintf(fname, "%s/%s", configdir, CONFIG_FILENAME);
		fname_is_malloced = false;
	}
	else
	{
		write_stderr("%s does not know where to find the server configuration file.\n"
					 "You must specify the --config-file or -D invocation "
					 "option or set the PGDATA environment variable.\n",
					 progname);
		goto fail;
	}

	/*
	 * Set the ConfigFileName GUC variable to its final value, ensuring that
	 * it can't be overridden later.
	 */
	SetConfigOption("config_file", fname, PGC_POSTMASTER, PGC_S_OVERRIDE);

	if (fname_is_malloced)
		free(fname);
	else
		guc_free(fname);

	/*
	 * Now read the config file for the first time.
	 */
	if (stat(ConfigFileName, &stat_buf) != 0)
	{
		write_stderr("%s: could not access the server configuration file \"%s\": %m\n",
					 progname, ConfigFileName);
		goto fail;
	}

	/*
	 * Read the configuration file for the first time.  This time only the
	 * data_directory parameter is picked up to determine the data directory,
	 * so that we can read the PG_AUTOCONF_FILENAME file next time.
	 */
	ProcessConfigFile(PGC_POSTMASTER);

	/*
	 * If the data_directory GUC variable has been set, use that as DataDir;
	 * otherwise use configdir if set; else punt.
	 *
	 * Note: SetDataDir will copy and absolute-ize its argument, so we don't
	 * have to.
	 */
	data_directory_rec =
		find_option("data_directory", false, false, PANIC);
	if (*GUC_VARIABLE_STRING(data_directory_rec))
		SetDataDir(*GUC_VARIABLE_STRING(data_directory_rec));
	else if (configdir)
		SetDataDir(configdir);
	else
	{
		write_stderr("%s does not know where to find the database system data.\n"
					 "This can be specified as \"data_directory\" in \"%s\", "
					 "or by the -D invocation option, or by the "
					 "PGDATA environment variable.\n",
					 progname, ConfigFileName);
		goto fail;
	}

	/*
	 * Reflect the final DataDir value back into the data_directory GUC var.
	 * (If you are wondering why we don't just make them a single variable,
	 * it's because the EXEC_BACKEND case needs DataDir to be transmitted to
	 * child backends specially.  XXX is that still true?  Given that we now
	 * chdir to DataDir, EXEC_BACKEND can read the config file without knowing
	 * DataDir in advance.)
	 */
	SetConfigOption("data_directory", DataDir, PGC_POSTMASTER, PGC_S_OVERRIDE);

	/*
	 * Now read the config file a second time, allowing any settings in the
	 * PG_AUTOCONF_FILENAME file to take effect.  (This is pretty ugly, but
	 * since we have to determine the DataDir before we can find the autoconf
	 * file, the alternatives seem worse.)
	 */
	ProcessConfigFile(PGC_POSTMASTER);

	/*
	 * If timezone_abbreviations wasn't set in the configuration file, install
	 * the default value.  We do it this way because we can't safely install a
	 * "real" value until my_exec_path is set, which may not have happened
	 * when InitializeGUCOptions runs, so the bootstrap default value cannot
	 * be the real desired default.
	 */
	pg_timezone_abbrev_initialize();

	/*
	 * Figure out where pg_hba.conf is, and make sure the path is absolute.
	 */
	if (HbaFileName)
	{
		fname = make_absolute_path(HbaFileName);
		fname_is_malloced = true;
	}
	else if (configdir)
	{
		fname = guc_malloc(FATAL,
						   strlen(configdir) + strlen(HBA_FILENAME) + 2);
		sprintf(fname, "%s/%s", configdir, HBA_FILENAME);
		fname_is_malloced = false;
	}
	else
	{
		write_stderr("%s does not know where to find the \"hba\" configuration file.\n"
					 "This can be specified as \"hba_file\" in \"%s\", "
					 "or by the -D invocation option, or by the "
					 "PGDATA environment variable.\n",
					 progname, ConfigFileName);
		goto fail;
	}
	SetConfigOption("hba_file", fname, PGC_POSTMASTER, PGC_S_OVERRIDE);

	if (fname_is_malloced)
		free(fname);
	else
		guc_free(fname);

	/*
	 * Likewise for pg_ident.conf.
	 */
	if (IdentFileName)
	{
		fname = make_absolute_path(IdentFileName);
		fname_is_malloced = true;
	}
	else if (configdir)
	{
		fname = guc_malloc(FATAL,
						   strlen(configdir) + strlen(IDENT_FILENAME) + 2);
		sprintf(fname, "%s/%s", configdir, IDENT_FILENAME);
		fname_is_malloced = false;
	}
	else
	{
		write_stderr("%s does not know where to find the \"ident\" configuration file.\n"
					 "This can be specified as \"ident_file\" in \"%s\", "
					 "or by the -D invocation option, or by the "
					 "PGDATA environment variable.\n",
					 progname, ConfigFileName);
		goto fail;
	}
	SetConfigOption("ident_file", fname, PGC_POSTMASTER, PGC_S_OVERRIDE);

	if (fname_is_malloced)
		free(fname);
	else
		guc_free(fname);

	/*
	 * Likewise for pg_hosts.conf.
	 */
	if (HostsFileName)
	{
		fname = make_absolute_path(HostsFileName);
		fname_is_malloced = true;
	}
	else if (configdir)
	{
		fname = guc_malloc(FATAL,
						   strlen(configdir) + strlen(HOSTS_FILENAME) + 2);
		sprintf(fname, "%s/%s", configdir, HOSTS_FILENAME);
		fname_is_malloced = false;
	}
	else
	{
		write_stderr("%s does not know where to find the \"hosts\" configuration file.\n"
					 "This can be specified as \"hosts_file\" in \"%s\", "
					 "or by the -D invocation option, or by the "
					 "PGDATA environment variable.\n",
					 progname, ConfigFileName);
		goto fail;
	}
	SetConfigOption("hosts_file", fname, PGC_POSTMASTER, PGC_S_OVERRIDE);

	if (fname_is_malloced)
		free(fname);
	else
		guc_free(fname);

	free(configdir);

	return true;

fail:
	free(configdir);

	return false;
}

/*
 * pg_timezone_abbrev_initialize --- set default value if not done already
 *
 * This is called after initial loading of postgresql.conf.  If no
 * timezone_abbreviations setting was found therein, select default.
 * If a non-default value is already installed, nothing will happen.
 *
 * This can also be called from ProcessConfigFile to establish the default
 * value after a postgresql.conf entry for it is removed.
 */
static void
pg_timezone_abbrev_initialize(void)
{
	SetConfigOption("timezone_abbreviations", "Default",
					PGC_POSTMASTER, PGC_S_DYNAMIC_DEFAULT);
}


/*
 * Reset all options to their saved default values (implements RESET ALL)
 */
void
ResetAllOptions(void)
{
	dlist_mutable_iter iter;

	/* We need only consider GUCs not already at PGC_S_DEFAULT */
	dlist_foreach_modify(iter, &guc_nondef_list)
	{
		config_generic_cold_state *cold = dlist_container(config_generic_cold_state,
														  nondef_link, iter.cur);
		struct config_generic *gconf = GUC_COLD_STATE_RECORD(cold);

		/* Don't reset non-SET-able values */
		if (gconf->context != PGC_SUSET &&
			gconf->context != PGC_USERSET)
			continue;
		/* Don't reset if special exclusion from RESET ALL */
		if (gconf->flags & GUC_NO_RESET_ALL)
			continue;
		/* No need to reset if wasn't SET */
		if (GUC_SOURCE(gconf) <= PGC_S_OVERRIDE)
			continue;

		/* Save old value to support transaction abort */
		push_old_value(gconf, GUC_ACTION_SET);

		switch (gconf->vartype)
		{
			case PGC_BOOL:
				{
					struct config_bool *conf = &gconf->_bool;

					if (conf->assign_hook)
						conf->assign_hook(GUC_RESET_BOOL(gconf),
										  GUC_RESET_EXTRA(gconf));
					*GUC_VARIABLE_BOOL(gconf) = GUC_RESET_BOOL(gconf);
					set_extra_field(gconf, GUC_EXTRA_REF(gconf),
									GUC_RESET_EXTRA(gconf));
					break;
				}
			case PGC_INT:
				{
					struct config_int *conf = &gconf->_int;

					if (conf->assign_hook)
						conf->assign_hook(GUC_RESET_INT(gconf),
										  GUC_RESET_EXTRA(gconf));
					*GUC_VARIABLE_INT(gconf) = GUC_RESET_INT(gconf);
					set_extra_field(gconf, GUC_EXTRA_REF(gconf),
									GUC_RESET_EXTRA(gconf));
					break;
				}
			case PGC_REAL:
				{
					struct config_real *conf = &gconf->_real;

					if (conf->assign_hook)
						conf->assign_hook(GUC_RESET_REAL(gconf),
										  GUC_RESET_EXTRA(gconf));
					*GUC_VARIABLE_REAL(gconf) = GUC_RESET_REAL(gconf);
					set_extra_field(gconf, GUC_EXTRA_REF(gconf),
									GUC_RESET_EXTRA(gconf));
					break;
				}
			case PGC_STRING:
				{
					struct config_string *conf = &gconf->_string;

					if (conf->assign_hook)
						conf->assign_hook(GUC_RESET_STRING(gconf),
										  GUC_RESET_EXTRA(gconf));
					set_string_field(gconf, GUC_VARIABLE_STRING(gconf),
									 GUC_RESET_STRING(gconf));
					set_extra_field(gconf, GUC_EXTRA_REF(gconf),
									GUC_RESET_EXTRA(gconf));
					break;
				}
			case PGC_ENUM:
				{
					struct config_enum *conf = &gconf->_enum;

					if (conf->assign_hook)
						conf->assign_hook(GUC_RESET_ENUM(gconf),
										  GUC_RESET_EXTRA(gconf));
					*GUC_VARIABLE_ENUM(gconf) = GUC_RESET_ENUM(gconf);
					set_extra_field(gconf, GUC_EXTRA_REF(gconf),
									GUC_RESET_EXTRA(gconf));
					break;
				}
		}

		set_guc_source(gconf, GUC_RESET_SOURCE(gconf));
		GUC_SCONTEXT(gconf) = GUC_RESET_SCONTEXT(gconf);
		GUC_SROLE(gconf) = GUC_RESET_SROLE(gconf);

		if ((gconf->flags & GUC_REPORT) && !(GUC_STATUS(gconf) & GUC_NEEDS_REPORT))
		{
			GUC_STATUS(gconf) |= GUC_NEEDS_REPORT;
			slist_push_head(&guc_report_list, GUC_REPORT_LINK(gconf));
		}
	}
}


/*
 * Apply a change to a GUC variable's "source" field.
 *
 * Use this rather than just assigning, to ensure that the variable's
 * membership in guc_nondef_list is updated correctly.
 */
static void
set_guc_source(struct config_generic *gconf, GucSource newsource)
{
	/* Adjust nondef list membership if appropriate for change */
	if (GUC_SOURCE(gconf) == PGC_S_DEFAULT)
	{
		if (newsource != PGC_S_DEFAULT)
			dlist_push_tail(&guc_nondef_list, GUC_NONDEF_LINK(gconf));
	}
	else
	{
		if (newsource == PGC_S_DEFAULT)
			dlist_delete(GUC_NONDEF_LINK(gconf));
	}
	/* Now update the source field */
	GUC_SOURCE(gconf) = newsource;
}


/*
 * push_old_value
 *		Push previous state during transactional assignment to a GUC variable.
 */
static void
push_old_value(struct config_generic *gconf, GucAction action)
{
	GucStack   *stack;

	/* If we're not inside a nest level, do nothing */
	if (GUCNestLevel == 0)
		return;

	/* Do we already have a stack entry of the current nest level? */
	stack = GUC_STACK(gconf);
	if (stack && stack->nest_level >= GUCNestLevel)
	{
		/* Yes, so adjust its state if necessary */
		Assert(stack->nest_level == GUCNestLevel);
		switch (action)
		{
			case GUC_ACTION_SET:
				/* SET overrides any prior action at same nest level */
				if (stack->state == GUC_SET_LOCAL)
				{
					/* must discard old masked value */
					discard_stack_value(gconf, &stack->masked);
				}
				stack->state = GUC_SET;
				break;
			case GUC_ACTION_LOCAL:
				if (stack->state == GUC_SET)
				{
					/* SET followed by SET LOCAL, remember SET's value */
					stack->masked_scontext = GUC_SCONTEXT(gconf);
					stack->masked_srole = GUC_SROLE(gconf);
					set_stack_value(gconf, &stack->masked);
					stack->state = GUC_SET_LOCAL;
				}
				/* in all other cases, no change to stack entry */
				break;
			case GUC_ACTION_SAVE:
				/* Could only have a prior SAVE of same variable */
				Assert(stack->state == GUC_SAVE);
				break;
		}
		return;
	}

	/*
	 * Push a new stack entry
	 *
	 * We keep all the stack entries in TopTransactionContext for simplicity.
	 */
	stack = (GucStack *) MemoryContextAllocZero(TopTransactionContext,
												sizeof(GucStack));

	stack->prev = GUC_STACK(gconf);
	stack->nest_level = GUCNestLevel;
	switch (action)
	{
		case GUC_ACTION_SET:
			stack->state = GUC_SET;
			break;
		case GUC_ACTION_LOCAL:
			stack->state = GUC_LOCAL;
			break;
		case GUC_ACTION_SAVE:
			stack->state = GUC_SAVE;
			break;
	}
	stack->source = GUC_SOURCE(gconf);
	stack->scontext = GUC_SCONTEXT(gconf);
	stack->srole = GUC_SROLE(gconf);
	set_stack_value(gconf, &stack->prior);

	if (GUC_STACK(gconf) == NULL)
		slist_push_head(&guc_stack_list, GUC_STACK_LINK(gconf));
	GUC_SET_STACK(gconf, stack);
}


/*
 * Do GUC processing at main transaction start.
 */
void
AtStart_GUC(void)
{
	/*
	 * The nest level should be 0 between transactions; if it isn't, somebody
	 * didn't call AtEOXact_GUC, or called it with the wrong nestLevel.  We
	 * throw a warning but make no other effort to clean up.
	 */
	if (GUCNestLevel != 0)
		elog(WARNING, "GUC nest level = %d at transaction start",
			 GUCNestLevel);
	GUCNestLevel = 1;
}

/*
 * Enter a new nesting level for GUC values.  This is called at subtransaction
 * start, and when entering a function that has proconfig settings, and in
 * some other places where we want to set GUC variables transiently.
 * NOTE we must not risk error here, else subtransaction start will be unhappy.
 */
int
NewGUCNestLevel(void)
{
	return ++GUCNestLevel;
}

/*
 * Set search_path to a fixed value for maintenance operations. No effect
 * during bootstrap, when the search_path is already set to a fixed value and
 * cannot be changed.
 */
void
RestrictSearchPath(void)
{
	if (!IsBootstrapProcessingMode())
		set_config_option("search_path", GUC_SAFE_SEARCH_PATH, PGC_USERSET,
						  PGC_S_SESSION, GUC_ACTION_SAVE, true, 0, false);
}

/*
 * Do GUC processing at transaction or subtransaction commit or abort, or
 * when exiting a function that has proconfig settings, or when undoing a
 * transient assignment to some GUC variables.  (The name is thus a bit of
 * a misnomer; perhaps it should be ExitGUCNestLevel or some such.)
 * During abort, we discard all GUC settings that were applied at nesting
 * levels >= nestLevel.  nestLevel == 1 corresponds to the main transaction.
 */
void
AtEOXact_GUC(bool isCommit, int nestLevel)
{
	slist_mutable_iter iter;

	/*
	 * Note: it's possible to get here with GUCNestLevel == nestLevel-1 during
	 * abort, if there is a failure during transaction start before
	 * AtStart_GUC is called.
	 */
	Assert(nestLevel > 0 &&
		   (nestLevel <= GUCNestLevel ||
			(nestLevel == GUCNestLevel + 1 && !isCommit)));

	/* We need only process GUCs having nonempty stacks */
	slist_foreach_modify(iter, &guc_stack_list)
	{
		config_generic_cold_state *cold = slist_container(config_generic_cold_state,
														  stack_link, iter.cur);
		struct config_generic *gconf = GUC_COLD_STATE_RECORD(cold);
		GucStack   *stack;

		/*
		 * Process and pop each stack entry within the nest level. To simplify
		 * fmgr_security_definer() and other places that use GUC_ACTION_SAVE,
		 * we allow failure exit from code that uses a local nest level to be
		 * recovered at the surrounding transaction or subtransaction abort;
		 * so there could be more than one stack entry to pop.
		 */
		while ((stack = GUC_STACK(gconf)) != NULL &&
			   stack->nest_level >= nestLevel)
		{
			GucStack   *prev = stack->prev;
			bool		restorePrior = false;
			bool		restoreMasked = false;
			bool		changed;

			/*
			 * In this next bit, if we don't set either restorePrior or
			 * restoreMasked, we must "discard" any unwanted fields of the
			 * stack entries to avoid leaking memory.  If we do set one of
			 * those flags, unused fields will be cleaned up after restoring.
			 */
			if (!isCommit)		/* if abort, always restore prior value */
				restorePrior = true;
			else if (stack->state == GUC_SAVE)
				restorePrior = true;
			else if (stack->nest_level == 1)
			{
				/* transaction commit */
				if (stack->state == GUC_SET_LOCAL)
					restoreMasked = true;
				else if (stack->state == GUC_SET)
				{
					/* we keep the current active value */
					discard_stack_value(gconf, &stack->prior);
				}
				else			/* must be GUC_LOCAL */
					restorePrior = true;
			}
			else if (prev == NULL ||
					 prev->nest_level < stack->nest_level - 1)
			{
				/* decrement entry's level and do not pop it */
				stack->nest_level--;
				continue;
			}
			else
			{
				/*
				 * We have to merge this stack entry into prev. See README for
				 * discussion of this bit.
				 */
				switch (stack->state)
				{
					case GUC_SAVE:
						Assert(false);	/* can't get here */
						break;

					case GUC_SET:
						/* next level always becomes SET */
						discard_stack_value(gconf, &stack->prior);
						if (prev->state == GUC_SET_LOCAL)
							discard_stack_value(gconf, &prev->masked);
						prev->state = GUC_SET;
						break;

					case GUC_LOCAL:
						if (prev->state == GUC_SET)
						{
							/* LOCAL migrates down */
							prev->masked_scontext = stack->scontext;
							prev->masked_srole = stack->srole;
							prev->masked = stack->prior;
							prev->state = GUC_SET_LOCAL;
						}
						else
						{
							/* else just forget this stack level */
							discard_stack_value(gconf, &stack->prior);
						}
						break;

					case GUC_SET_LOCAL:
						/* prior state at this level no longer wanted */
						discard_stack_value(gconf, &stack->prior);
						/* copy down the masked state */
						prev->masked_scontext = stack->masked_scontext;
						prev->masked_srole = stack->masked_srole;
						if (prev->state == GUC_SET_LOCAL)
							discard_stack_value(gconf, &prev->masked);
						prev->masked = stack->masked;
						prev->state = GUC_SET_LOCAL;
						break;
				}
			}

			changed = false;

			if (restorePrior || restoreMasked)
			{
				/* Perform appropriate restoration of the stacked value */
				config_var_value newvalue;
				GucSource	newsource;
				GucContext	newscontext;
				Oid			newsrole;

				if (restoreMasked)
				{
					newvalue = stack->masked;
					newsource = PGC_S_SESSION;
					newscontext = stack->masked_scontext;
					newsrole = stack->masked_srole;
				}
				else
				{
					newvalue = stack->prior;
					newsource = stack->source;
					newscontext = stack->scontext;
					newsrole = stack->srole;
				}

				switch (gconf->vartype)
				{
					case PGC_BOOL:
						{
							struct config_bool *conf = &gconf->_bool;
							bool		newval = newvalue.val.boolval;
							void	   *newextra = newvalue.extra;

							if (*GUC_VARIABLE_BOOL(gconf) != newval ||
								GUC_EXTRA(gconf) != newextra)
							{
								if (conf->assign_hook)
									conf->assign_hook(newval, newextra);
								*GUC_VARIABLE_BOOL(gconf) = newval;
								set_extra_field(gconf, GUC_EXTRA_REF(gconf),
												newextra);
								changed = true;
							}
							break;
						}
					case PGC_INT:
						{
							struct config_int *conf = &gconf->_int;
							int			newval = newvalue.val.intval;
							void	   *newextra = newvalue.extra;

							if (*GUC_VARIABLE_INT(gconf) != newval ||
								GUC_EXTRA(gconf) != newextra)
							{
								if (conf->assign_hook)
									conf->assign_hook(newval, newextra);
								*GUC_VARIABLE_INT(gconf) = newval;
								set_extra_field(gconf, GUC_EXTRA_REF(gconf),
												newextra);
								changed = true;
							}
							break;
						}
					case PGC_REAL:
						{
							struct config_real *conf = &gconf->_real;
							double		newval = newvalue.val.realval;
							void	   *newextra = newvalue.extra;

							if (*GUC_VARIABLE_REAL(gconf) != newval ||
								GUC_EXTRA(gconf) != newextra)
							{
								if (conf->assign_hook)
									conf->assign_hook(newval, newextra);
								*GUC_VARIABLE_REAL(gconf) = newval;
								set_extra_field(gconf, GUC_EXTRA_REF(gconf),
												newextra);
								changed = true;
							}
							break;
						}
					case PGC_STRING:
						{
							struct config_string *conf = &gconf->_string;
							char	   *newval = newvalue.val.stringval;
							void	   *newextra = newvalue.extra;

							if (*GUC_VARIABLE_STRING(gconf) != newval ||
								GUC_EXTRA(gconf) != newextra)
							{
								if (conf->assign_hook)
									conf->assign_hook(newval, newextra);
								set_string_field(gconf, GUC_VARIABLE_STRING(gconf),
												 newval);
								set_extra_field(gconf, GUC_EXTRA_REF(gconf),
												newextra);
								changed = true;
							}

							/*
							 * Release stacked values if not used anymore. We
							 * could use discard_stack_value() here, but since
							 * we have type-specific code anyway, might as
							 * well inline it.
							 */
							set_string_field(gconf, &stack->prior.val.stringval, NULL);
							set_string_field(gconf, &stack->masked.val.stringval, NULL);
							break;
						}
					case PGC_ENUM:
						{
							struct config_enum *conf = &gconf->_enum;
							int			newval = newvalue.val.enumval;
							void	   *newextra = newvalue.extra;

							if (*GUC_VARIABLE_ENUM(gconf) != newval ||
								GUC_EXTRA(gconf) != newextra)
							{
								if (conf->assign_hook)
									conf->assign_hook(newval, newextra);
								*GUC_VARIABLE_ENUM(gconf) = newval;
								set_extra_field(gconf, GUC_EXTRA_REF(gconf),
												newextra);
								changed = true;
							}
							break;
						}
				}

				/*
				 * Release stacked extra values if not used anymore.
				 */
				set_extra_field(gconf, &(stack->prior.extra), NULL);
				set_extra_field(gconf, &(stack->masked.extra), NULL);

				/* And restore source information */
				set_guc_source(gconf, newsource);
				GUC_SCONTEXT(gconf) = newscontext;
				GUC_SROLE(gconf) = newsrole;
			}

			/*
			 * Pop the GUC's state stack; if it's now empty, remove the GUC
			 * from guc_stack_list.
			 */
			GUC_SET_STACK(gconf, prev);
			if (prev == NULL)
				slist_delete_current(&iter);
			pfree(stack);

			/* Report new value if we changed it */
			if (changed && (gconf->flags & GUC_REPORT) &&
				!(GUC_STATUS(gconf) & GUC_NEEDS_REPORT))
			{
				GUC_STATUS(gconf) |= GUC_NEEDS_REPORT;
				slist_push_head(&guc_report_list, GUC_REPORT_LINK(gconf));
			}
		}						/* end of stack-popping loop */
	}

	/* Update nesting level */
	GUCNestLevel = nestLevel - 1;
}


/*
 * Start up automatic reporting of changes to variables marked GUC_REPORT.
 * This is executed at completion of backend startup.
 */
void
BeginReportingGUCOptions(void)
{
	HASH_SEQ_STATUS status;
	GUCHashEntry *hentry;

	/*
	 * Don't do anything unless talking to an interactive frontend.
	 */
	if (whereToSendOutput != DestRemote)
		return;

	reporting_enabled = true;

	/*
	 * Hack for in_hot_standby: set the GUC value true if appropriate.  This
	 * is kind of an ugly place to do it, but there's few better options.
	 *
	 * (This could be out of date by the time we actually send it, in which
	 * case the next ReportChangedGUCOptions call will send a duplicate
	 * report.)
	 */
	if (RecoveryInProgress())
		SetConfigOption("in_hot_standby", "true",
						PGC_INTERNAL, PGC_S_OVERRIDE);

	/* Transmit initial values of interesting variables */
	for (int i = 0; i < num_guc_variables; i++)
	{
		struct config_generic *conf = &guc_variables[i];

		if (conf->flags & GUC_REPORT)
			ReportGUCOption(conf);
	}
	if (guc_hashtab != NULL)
	{
		hash_seq_init(&status, guc_hashtab);
		while ((hentry = (GUCHashEntry *) hash_seq_search(&status)) != NULL)
		{
			struct config_generic *conf = hentry->gucvar;

			if (conf->flags & GUC_REPORT)
				ReportGUCOption(conf);
		}
	}
}

/*
 * ReportChangedGUCOptions: report recently-changed GUC_REPORT variables
 *
 * This is called just before we wait for a new client query.
 *
 * By handling things this way, we ensure that a ParameterStatus message
 * is sent at most once per variable per query, even if the variable
 * changed multiple times within the query.  That's quite possible when
 * using features such as function SET clauses.  Function SET clauses
 * also tend to cause values to change intraquery but eventually revert
 * to their prevailing values; ReportGUCOption is responsible for avoiding
 * redundant reports in such cases.
 */
void
ReportChangedGUCOptions(void)
{
	slist_mutable_iter iter;

	/* Quick exit if not (yet) enabled */
	if (!reporting_enabled)
		return;

	/*
	 * Since in_hot_standby isn't actually changed by normal GUC actions, we
	 * need a hack to check whether a new value needs to be reported to the
	 * client.  For speed, we rely on the assumption that it can never
	 * transition from false to true.
	 */
	if (in_hot_standby_guc && !RecoveryInProgress())
		SetConfigOption("in_hot_standby", "false",
						PGC_INTERNAL, PGC_S_OVERRIDE);

	/* Transmit new values of interesting variables */
	slist_foreach_modify(iter, &guc_report_list)
	{
		config_generic_cold_state *cold = slist_container(config_generic_cold_state,
														  report_link, iter.cur);
		struct config_generic *conf = GUC_COLD_STATE_RECORD(cold);

		Assert((conf->flags & GUC_REPORT) && (GUC_STATUS(conf) & GUC_NEEDS_REPORT));
		ReportGUCOption(conf);
		GUC_STATUS(conf) &= ~GUC_NEEDS_REPORT;
		slist_delete_current(&iter);
	}
}

/*
 * ReportGUCOption: if appropriate, transmit option value to frontend
 *
 * We need not transmit the value if it's the same as what we last
 * transmitted.
 */
static void
ReportGUCOption(struct config_generic *record)
{
	char	   *val = ShowGUCOption(record, false);

	if (GUC_LAST_REPORTED(record) == NULL ||
		strcmp(val, GUC_LAST_REPORTED(record)) != 0)
	{
		StringInfoData msgbuf;

		pq_beginmessage(&msgbuf, PqMsg_ParameterStatus);
		pq_sendstring(&msgbuf, record->name);
		pq_sendstring(&msgbuf, val);
		pq_endmessage(&msgbuf);

		/*
		 * We need a long-lifespan copy.  If guc_strdup() fails due to OOM,
		 * we'll set last_reported to NULL and thereby possibly make a
		 * duplicate report later.
		 */
		clear_last_reported(record);
		GUC_SET_LAST_REPORTED(record, guc_strdup(LOG, val));
	}

	pfree(val);
}

/*
 * Convert a value from one of the human-friendly units ("kB", "min" etc.)
 * to the given base unit.  'value' and 'unit' are the input value and unit
 * to convert from (there can be trailing spaces in the unit string).
 * The converted value is stored in *base_value.
 * It's caller's responsibility to round off the converted value as necessary
 * and check for out-of-range.
 *
 * Returns true on success, false if the input unit is not recognized.
 */
static bool
convert_to_base_unit(double value, const char *unit,
					 int base_unit, double *base_value)
{
	char		unitstr[MAX_UNIT_LEN + 1];
	int			unitlen;
	const unit_conversion *table;

	/* extract unit string to compare to table entries */
	unitlen = 0;
	while (*unit != '\0' && !isspace((unsigned char) *unit) &&
		   unitlen < MAX_UNIT_LEN)
		unitstr[unitlen++] = *(unit++);
	unitstr[unitlen] = '\0';
	/* allow whitespace after unit */
	while (isspace((unsigned char) *unit))
		unit++;
	if (*unit != '\0')
		return false;			/* unit too long, or garbage after it */

	/* now search the appropriate table */
	if (base_unit & GUC_UNIT_MEMORY)
		table = memory_unit_conversion_table;
	else
		table = time_unit_conversion_table;

	for (int i = 0; *table[i].unit; i++)
	{
		if (base_unit == table[i].base_unit &&
			strcmp(unitstr, table[i].unit) == 0)
		{
			double		cvalue = value * table[i].multiplier;

			/*
			 * If the user gave a fractional value such as "30.1GB", round it
			 * off to the nearest multiple of the next smaller unit, if there
			 * is one.
			 */
			if (*table[i + 1].unit &&
				base_unit == table[i + 1].base_unit)
				cvalue = rint(cvalue / table[i + 1].multiplier) *
					table[i + 1].multiplier;

			*base_value = cvalue;
			return true;
		}
	}
	return false;
}

/*
 * Convert an integer value in some base unit to a human-friendly unit.
 *
 * The output unit is chosen so that it's the greatest unit that can represent
 * the value without loss.  For example, if the base unit is GUC_UNIT_KB, 1024
 * is converted to 1 MB, but 1025 is represented as 1025 kB.
 */
static void
convert_int_from_base_unit(int64 base_value, int base_unit,
						   int64 *value, const char **unit)
{
	const unit_conversion *table;

	*unit = NULL;

	if (base_unit & GUC_UNIT_MEMORY)
		table = memory_unit_conversion_table;
	else
		table = time_unit_conversion_table;

	for (int i = 0; *table[i].unit; i++)
	{
		if (base_unit == table[i].base_unit)
		{
			/*
			 * Accept the first conversion that divides the value evenly.  We
			 * assume that the conversions for each base unit are ordered from
			 * greatest unit to the smallest!
			 */
			if (table[i].multiplier <= 1.0 ||
				base_value % (int64) table[i].multiplier == 0)
			{
				*value = (int64) rint(base_value / table[i].multiplier);
				*unit = table[i].unit;
				break;
			}
		}
	}

	Assert(*unit != NULL);
}

/*
 * Convert a floating-point value in some base unit to a human-friendly unit.
 *
 * Same as above, except we have to do the math a bit differently, and
 * there's a possibility that we don't find any exact divisor.
 */
static void
convert_real_from_base_unit(double base_value, int base_unit,
							double *value, const char **unit)
{
	const unit_conversion *table;

	*unit = NULL;

	if (base_unit & GUC_UNIT_MEMORY)
		table = memory_unit_conversion_table;
	else
		table = time_unit_conversion_table;

	for (int i = 0; *table[i].unit; i++)
	{
		if (base_unit == table[i].base_unit)
		{
			/*
			 * Accept the first conversion that divides the value evenly; or
			 * if there is none, use the smallest (last) target unit.
			 *
			 * What we actually care about here is whether snprintf with "%g"
			 * will print the value as an integer, so the obvious test of
			 * "*value == rint(*value)" is too strict; roundoff error might
			 * make us choose an unreasonably small unit.  As a compromise,
			 * accept a divisor that is within 1e-8 of producing an integer.
			 */
			*value = base_value / table[i].multiplier;
			*unit = table[i].unit;
			if (*value > 0 &&
				fabs((rint(*value) / *value) - 1.0) <= 1e-8)
				break;
		}
	}

	Assert(*unit != NULL);
}

/*
 * Return the name of a GUC's base unit (e.g. "ms") given its flags.
 * Return NULL if the GUC is unitless.
 */
const char *
get_config_unit_name(int flags)
{
	switch (flags & GUC_UNIT)
	{
		case 0:
			return NULL;		/* GUC has no units */
		case GUC_UNIT_BYTE:
			return "B";
		case GUC_UNIT_KB:
			return "kB";
		case GUC_UNIT_MB:
			return "MB";
		case GUC_UNIT_BLOCKS:
			{
				static char bbuf[8];

				/* initialize if first time through */
				if (bbuf[0] == '\0')
					snprintf(bbuf, sizeof(bbuf), "%dkB", BLCKSZ / 1024);
				return bbuf;
			}
		case GUC_UNIT_XBLOCKS:
			{
				static char xbuf[8];

				/* initialize if first time through */
				if (xbuf[0] == '\0')
					snprintf(xbuf, sizeof(xbuf), "%dkB", XLOG_BLCKSZ / 1024);
				return xbuf;
			}
		case GUC_UNIT_MS:
			return "ms";
		case GUC_UNIT_S:
			return "s";
		case GUC_UNIT_MIN:
			return "min";
		default:
			elog(ERROR, "unrecognized GUC units value: %d",
				 flags & GUC_UNIT);
			return NULL;
	}
}


/*
 * Try to parse value as an integer.  The accepted formats are the
 * usual decimal, octal, or hexadecimal formats, as well as floating-point
 * formats (which will be rounded to integer after any units conversion).
 * Optionally, the value can be followed by a unit name if "flags" indicates
 * a unit is allowed.
 *
 * If the string parses okay, return true, else false.
 * If okay and result is not NULL, return the value in *result.
 * If not okay and hintmsg is not NULL, *hintmsg is set to a suitable
 * HINT message, or NULL if no hint provided.
 */
bool
parse_int(const char *value, int *result, int flags, const char **hintmsg)
{
	/*
	 * We assume here that double is wide enough to represent any integer
	 * value with adequate precision.
	 */
	double		val;
	char	   *endptr;

	/* To suppress compiler warnings, always set output params */
	if (result)
		*result = 0;
	if (hintmsg)
		*hintmsg = NULL;

	/*
	 * Try to parse as an integer (allowing octal or hex input).  If the
	 * conversion stops at a decimal point or 'e', or overflows, re-parse as
	 * float.  This should work fine as long as we have no unit names starting
	 * with 'e'.  If we ever do, the test could be extended to check for a
	 * sign or digit after 'e', but for now that's unnecessary.
	 */
	errno = 0;
	val = strtol(value, &endptr, 0);
	if (*endptr == '.' || *endptr == 'e' || *endptr == 'E' ||
		errno == ERANGE)
	{
		errno = 0;
		val = strtod(value, &endptr);
	}

	if (endptr == value || errno == ERANGE)
		return false;			/* no HINT for these cases */

	/* reject NaN (infinities will fail range check below) */
	if (isnan(val))
		return false;			/* treat same as syntax error; no HINT */

	/* allow whitespace between number and unit */
	while (isspace((unsigned char) *endptr))
		endptr++;

	/* Handle possible unit */
	if (*endptr != '\0')
	{
		if ((flags & GUC_UNIT) == 0)
			return false;		/* this setting does not accept a unit */

		if (!convert_to_base_unit(val,
								  endptr, (flags & GUC_UNIT),
								  &val))
		{
			/* invalid unit, or garbage after the unit; set hint and fail. */
			if (hintmsg)
			{
				if (flags & GUC_UNIT_MEMORY)
					*hintmsg = memory_units_hint;
				else
					*hintmsg = time_units_hint;
			}
			return false;
		}
	}

	/* Round to int, then check for overflow */
	val = rint(val);

	if (val > INT_MAX || val < INT_MIN)
	{
		if (hintmsg)
			*hintmsg = gettext_noop("Value exceeds integer range.");
		return false;
	}

	if (result)
		*result = (int) val;
	return true;
}

/*
 * Try to parse value as a floating point number in the usual format.
 * Optionally, the value can be followed by a unit name if "flags" indicates
 * a unit is allowed.
 *
 * If the string parses okay, return true, else false.
 * If okay and result is not NULL, return the value in *result.
 * If not okay and hintmsg is not NULL, *hintmsg is set to a suitable
 * HINT message, or NULL if no hint provided.
 */
bool
parse_real(const char *value, double *result, int flags, const char **hintmsg)
{
	double		val;
	char	   *endptr;

	/* To suppress compiler warnings, always set output params */
	if (result)
		*result = 0;
	if (hintmsg)
		*hintmsg = NULL;

	errno = 0;
	val = strtod(value, &endptr);

	if (endptr == value || errno == ERANGE)
		return false;			/* no HINT for these cases */

	/* reject NaN (infinities will fail range checks later) */
	if (isnan(val))
		return false;			/* treat same as syntax error; no HINT */

	/* allow whitespace between number and unit */
	while (isspace((unsigned char) *endptr))
		endptr++;

	/* Handle possible unit */
	if (*endptr != '\0')
	{
		if ((flags & GUC_UNIT) == 0)
			return false;		/* this setting does not accept a unit */

		if (!convert_to_base_unit(val,
								  endptr, (flags & GUC_UNIT),
								  &val))
		{
			/* invalid unit, or garbage after the unit; set hint and fail. */
			if (hintmsg)
			{
				if (flags & GUC_UNIT_MEMORY)
					*hintmsg = memory_units_hint;
				else
					*hintmsg = time_units_hint;
			}
			return false;
		}
	}

	if (result)
		*result = val;
	return true;
}


/*
 * Lookup the name for an enum option with the selected value.
 * Should only ever be called with known-valid values, so throws
 * an elog(ERROR) if the enum option is not found.
 *
 * The returned string is a pointer to static data and not
 * allocated for modification.
 */
const char *
config_enum_lookup_by_value(const struct config_generic *record, int val)
{
	for (const struct config_enum_entry *entry = record->_enum.options; entry && entry->name; entry++)
	{
		if (entry->val == val)
			return entry->name;
	}

	elog(ERROR, "could not find enum option %d for %s",
		 val, record->name);
	return NULL;				/* silence compiler */
}


/*
 * Lookup the value for an enum option with the selected name
 * (case-insensitive).
 * If the enum option is found, sets the retval value and returns
 * true. If it's not found, return false and retval is set to 0.
 */
bool
config_enum_lookup_by_name(const struct config_enum *record, const char *value,
						   int *retval)
{
	for (const struct config_enum_entry *entry = record->options; entry && entry->name; entry++)
	{
		if (pg_strcasecmp(value, entry->name) == 0)
		{
			*retval = entry->val;
			return true;
		}
	}

	*retval = 0;
	return false;
}


/*
 * Return a palloc'd string listing all the available options for an enum GUC
 * (excluding hidden ones), separated by the given separator.
 * If prefix is non-NULL, it is added before the first enum value.
 * If suffix is non-NULL, it is added to the end of the string.
 */
char *
config_enum_get_options(const struct config_enum *record, const char *prefix,
						const char *suffix, const char *separator)
{
	StringInfoData retstr;
	int			seplen;

	initStringInfo(&retstr);
	appendStringInfoString(&retstr, prefix);

	seplen = strlen(separator);
	for (const struct config_enum_entry *entry = record->options; entry && entry->name; entry++)
	{
		if (!entry->hidden)
		{
			appendStringInfoString(&retstr, entry->name);
			appendBinaryStringInfo(&retstr, separator, seplen);
		}
	}

	/*
	 * All the entries may have been hidden, leaving the string empty if no
	 * prefix was given. This indicates a broken GUC setup, since there is no
	 * use for an enum without any values, so we just check to make sure we
	 * don't write to invalid memory instead of actually trying to do
	 * something smart with it.
	 */
	if (retstr.len >= seplen)
	{
		/* Replace final separator */
		retstr.data[retstr.len - seplen] = '\0';
		retstr.len -= seplen;
	}

	appendStringInfoString(&retstr, suffix);

	return retstr.data;
}

/*
 * Parse and validate a proposed value for the specified configuration
 * parameter.
 *
 * This does built-in checks (such as range limits for an integer parameter)
 * and also calls any check hook the parameter may have.
 *
 * record: GUC variable's info record
 * value: proposed value, as a string
 * source: identifies source of value (check hooks may need this)
 * elevel: level to log any error reports at
 * newval: on success, converted parameter value is returned here
 * newextra: on success, receives any "extra" data returned by check hook
 *	(caller must initialize *newextra to NULL)
 *
 * Returns true if OK, false if not (or throws error, if elevel >= ERROR)
 */
static bool
parse_and_validate_value(const struct config_generic *record,
						 const char *value,
						 GucSource source, int elevel,
						 union config_var_val *newval, void **newextra)
{
	switch (record->vartype)
	{
		case PGC_BOOL:
			{
				if (!parse_bool(value, &newval->boolval))
				{
					ereport(elevel,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("parameter \"%s\" requires a Boolean value",
									record->name)));
					return false;
				}

				if (!call_bool_check_hook(record, &newval->boolval, newextra,
										  source, elevel))
					return false;
			}
			break;
		case PGC_INT:
			{
				const struct config_int *conf = &record->_int;
				const char *hintmsg;

				if (!parse_int(value, &newval->intval,
							   record->flags, &hintmsg))
				{
					ereport(elevel,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("invalid value for parameter \"%s\": \"%s\"",
									record->name, value),
							 hintmsg ? errhint("%s", _(hintmsg)) : 0));
					return false;
				}

				if (newval->intval < conf->min || newval->intval > conf->max)
				{
					const char *unit = get_config_unit_name(record->flags);
					const char *unitspace;

					if (unit)
						unitspace = " ";
					else
						unit = unitspace = "";

					ereport(elevel,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("%d%s%s is outside the valid range for parameter \"%s\" (%d%s%s .. %d%s%s)",
									newval->intval, unitspace, unit,
									record->name,
									conf->min, unitspace, unit,
									conf->max, unitspace, unit)));
					return false;
				}

				if (!call_int_check_hook(record, &newval->intval, newextra,
										 source, elevel))
					return false;
			}
			break;
		case PGC_REAL:
			{
				const struct config_real *conf = &record->_real;
				const char *hintmsg;

				if (!parse_real(value, &newval->realval,
								record->flags, &hintmsg))
				{
					ereport(elevel,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("invalid value for parameter \"%s\": \"%s\"",
									record->name, value),
							 hintmsg ? errhint("%s", _(hintmsg)) : 0));
					return false;
				}

				if (newval->realval < conf->min || newval->realval > conf->max)
				{
					const char *unit = get_config_unit_name(record->flags);
					const char *unitspace;

					if (unit)
						unitspace = " ";
					else
						unit = unitspace = "";

					ereport(elevel,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("%g%s%s is outside the valid range for parameter \"%s\" (%g%s%s .. %g%s%s)",
									newval->realval, unitspace, unit,
									record->name,
									conf->min, unitspace, unit,
									conf->max, unitspace, unit)));
					return false;
				}

				if (!call_real_check_hook(record, &newval->realval, newextra,
										  source, elevel))
					return false;
			}
			break;
		case PGC_STRING:
			{
				/*
				 * The value passed by the caller could be transient, so we
				 * always strdup it.
				 */
				newval->stringval = guc_strdup(elevel, value);
				if (newval->stringval == NULL)
					return false;

				/*
				 * The only built-in "parsing" check we have is to apply
				 * truncation if GUC_IS_NAME.
				 */
				if (record->flags & GUC_IS_NAME)
					truncate_identifier(newval->stringval,
										strlen(newval->stringval),
										true);

				if (!call_string_check_hook(record, &newval->stringval, newextra,
											source, elevel))
				{
					guc_free(newval->stringval);
					newval->stringval = NULL;
					return false;
				}
			}
			break;
		case PGC_ENUM:
			{
				const struct config_enum *conf = &record->_enum;

				if (!config_enum_lookup_by_name(conf, value, &newval->enumval))
				{
					char	   *hintmsg;

					hintmsg = config_enum_get_options(conf,
													  _("Available values: "),

					/*
					 * translator: This is the terminator of a list of entity
					 * names.
					 */
													  _("."),

					/*
					 * translator: This is a separator in a list of entity
					 * names.
					 */
													  _(", "));

					ereport(elevel,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("invalid value for parameter \"%s\": \"%s\"",
									record->name, value),
							 hintmsg ? errhint("%s", hintmsg) : 0));

					if (hintmsg)
						pfree(hintmsg);
					return false;
				}

				if (!call_enum_check_hook(record, &newval->enumval, newextra,
										  source, elevel))
					return false;
			}
			break;
	}

	return true;
}


/*
 * set_config_option: sets option `name' to given value.
 *
 * The value should be a string, which will be parsed and converted to
 * the appropriate data type.  The context and source parameters indicate
 * in which context this function is being called, so that it can apply the
 * access restrictions properly.
 *
 * If value is NULL, set the option to its default value (normally the
 * reset_val, but if source == PGC_S_DEFAULT we instead use the boot_val).
 *
 * action indicates whether to set the value globally in the session, locally
 * to the current top transaction, or just for the duration of a function call.
 *
 * If changeVal is false then don't really set the option but do all
 * the checks to see if it would work.
 *
 * elevel should normally be passed as zero, allowing this function to make
 * its standard choice of ereport level.  However some callers need to be
 * able to override that choice; they should pass the ereport level to use.
 *
 * is_reload should be true only when called from read_nondefault_variables()
 * or RestoreGUCState(), where we are trying to load some other process's
 * GUC settings into a new process.
 *
 * Return value:
 *	+1: the value is valid and was successfully applied.
 *	0:	the name or value is invalid, or it's invalid to try to set
 *		this GUC now; but elevel was less than ERROR (see below).
 *	-1: no error detected, but the value was not applied, either
 *		because changeVal is false or there is some overriding setting.
 *
 * If there is an error (non-existing option, invalid value, etc) then an
 * ereport(ERROR) is thrown *unless* this is called for a source for which
 * we don't want an ERROR (currently, those are defaults, the config file,
 * and per-database or per-user settings, as well as callers who specify
 * a less-than-ERROR elevel).  In those cases we write a suitable error
 * message via ereport() and return 0.
 *
 * See also SetConfigOption for an external interface.
 */
int
set_config_option(const char *name, const char *value,
				  GucContext context, GucSource source,
				  GucAction action, bool changeVal, int elevel,
				  bool is_reload)
{
	Oid			srole;

	/*
	 * Non-interactive sources should be treated as having all privileges,
	 * except for PGC_S_CLIENT.  Note in particular that this is true for
	 * pg_db_role_setting sources (PGC_S_GLOBAL etc): we assume a suitable
	 * privilege check was done when the pg_db_role_setting entry was made.
	 */
	if (source >= PGC_S_INTERACTIVE || source == PGC_S_CLIENT)
		srole = GetUserId();
	else
		srole = BOOTSTRAP_SUPERUSERID;

	return set_config_with_handle(name, NULL, value,
								  context, source, srole,
								  action, changeVal, elevel,
								  is_reload);
}

/*
 * set_config_option_ext: sets option `name' to given value.
 *
 * This API adds the ability to explicitly specify which role OID
 * is considered to be setting the value.  Most external callers can use
 * set_config_option() and let it determine that based on the GucSource,
 * but there are a few that are supplying a value that was determined
 * in some special way and need to override the decision.  Also, when
 * restoring a previously-assigned value, it's important to supply the
 * same role OID that set the value originally; so all guc.c callers
 * that are doing that type of thing need to call this directly.
 *
 * Generally, srole should be GetUserId() when the source is a SQL operation,
 * or BOOTSTRAP_SUPERUSERID if the source is a config file or similar.
 */
int
set_config_option_ext(const char *name, const char *value,
					  GucContext context, GucSource source, Oid srole,
					  GucAction action, bool changeVal, int elevel,
					  bool is_reload)
{
	return set_config_with_handle(name, NULL, value,
								  context, source, srole,
								  action, changeVal, elevel,
								  is_reload);
}


/*
 * set_config_with_handle: sets option `name' to given value.
 *
 * This API adds the ability to pass a 'handle' argument, which can be
 * obtained by the caller from get_config_handle().  NULL has no effect,
 * but a non-null value avoids the need to search the GUC tables.
 *
 * This should be used by callers which repeatedly set the same config
 * option(s), and want to avoid the overhead of a hash lookup each time.
 */
int
set_config_with_handle(const char *name, config_handle *handle,
					   const char *value,
					   GucContext context, GucSource source, Oid srole,
					   GucAction action, bool changeVal, int elevel,
					   bool is_reload)
{
	int			result;
	bool		locked;

	if (multithreaded)
	{
		struct config_generic *record = handle;

		if (record == NULL)
			record = find_option(name, false, true, 0);

		if (record != NULL && !GUCSetOptionNeedsThreadedLock(record))
			return set_config_with_handle_internal(name, record, value,
												   context, source, srole,
												   action, changeVal, elevel,
												   is_reload);
	}

	locked = ThreadedGUCLock();
	PG_TRY();
	{
		result = set_config_with_handle_internal(name, handle, value,
												 context, source, srole,
												 action, changeVal, elevel,
												 is_reload);
	}
	PG_FINALLY();
	{
		ThreadedGUCUnlock(locked);
	}
	PG_END_TRY();

	return result;
}

static int
set_config_with_handle_internal(const char *name, config_handle *handle,
								const char *value,
								GucContext context, GucSource source,
								Oid srole, GucAction action, bool changeVal,
								int elevel, bool is_reload)
{
	struct config_generic *record;
	union config_var_val newval_union;
	void	   *newextra = NULL;
	bool		prohibitValueChange = false;
	bool		makeDefault;

	if (elevel == 0)
	{
		if (source == PGC_S_DEFAULT || source == PGC_S_FILE)
		{
			/*
			 * To avoid cluttering the log, only the postmaster bleats loudly
			 * about problems with the config file.
			 */
			elevel = IsUnderPostmaster ? DEBUG3 : LOG;
		}
		else if (source == PGC_S_GLOBAL ||
				 source == PGC_S_DATABASE ||
				 source == PGC_S_USER ||
				 source == PGC_S_DATABASE_USER)
			elevel = WARNING;
		else
			elevel = ERROR;
	}

	/* if handle is specified, no need to look up option */
	if (!handle)
	{
		record = find_option(name, true, false, elevel);
		if (record == NULL)
			return 0;
	}
	else
		record = handle;

	/*
	 * GUC_ACTION_SAVE changes are acceptable during a parallel operation,
	 * because the current worker will also pop the change.  We're probably
	 * dealing with a function having a proconfig entry.  Only the function's
	 * body should observe the change, and peer workers do not share in the
	 * execution of a function call started by this worker.
	 *
	 * Also allow normal setting if the GUC is marked GUC_ALLOW_IN_PARALLEL.
	 *
	 * Other changes might need to affect other workers, so forbid them. Note,
	 * that parallel autovacuum leader is an exception because cost-based
	 * delays need to be affected to parallel autovacuum workers. These
	 * parameters are propagated to its workers during parallel vacuum (see
	 * vacuumparallel.c for details). All other changes will affect only the
	 * parallel autovacuum leader.
	 */
	if (IsInParallelMode() && !AmAutoVacuumWorkerProcess() && changeVal &&
		action != GUC_ACTION_SAVE &&
		(record->flags & GUC_ALLOW_IN_PARALLEL) == 0)
	{
		ereport(elevel,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("parameter \"%s\" cannot be set during a parallel operation",
						record->name)));
		return 0;
	}

	/*
	 * Check if the option can be set at this time. See guc.h for the precise
	 * rules.
	 */
	switch (record->context)
	{
		case PGC_INTERNAL:
			if (context != PGC_INTERNAL)
			{
				ereport(elevel,
						(errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
						 errmsg("parameter \"%s\" cannot be changed",
								record->name)));
				return 0;
			}
			break;
		case PGC_POSTMASTER:
			if (context == PGC_SIGHUP)
			{
				/*
				 * We are re-reading a PGC_POSTMASTER variable from
				 * postgresql.conf.  We can't change the setting, so we should
				 * give a warning if the DBA tries to change it.  However,
				 * because of variant formats, canonicalization by check
				 * hooks, etc, we can't just compare the given string directly
				 * to what's stored.  Set a flag to check below after we have
				 * the final storable value.
				 */
				prohibitValueChange = true;
			}
			else if (context != PGC_POSTMASTER)
			{
				ereport(elevel,
						(errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
						 errmsg("parameter \"%s\" cannot be changed without restarting the server",
								record->name)));
				return 0;
			}
			break;
		case PGC_SIGHUP:
			if (context != PGC_SIGHUP && context != PGC_POSTMASTER)
			{
				ereport(elevel,
						(errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
						 errmsg("parameter \"%s\" cannot be changed now",
								record->name)));
				return 0;
			}

			/*
			 * Hmm, the idea of the SIGHUP context is "ought to be global, but
			 * can be changed after postmaster start". But there's nothing
			 * that prevents a crafty administrator from sending SIGHUP
			 * signals to individual backends only.
			 */
			break;
		case PGC_SU_BACKEND:
			if (context == PGC_BACKEND)
			{
				/*
				 * Check whether the requesting user has been granted
				 * privilege to set this GUC.
				 */
				AclResult	aclresult;

				aclresult = pg_parameter_aclcheck(record->name, srole, ACL_SET);
				if (aclresult != ACLCHECK_OK)
				{
					/* No granted privilege */
					ereport(elevel,
							(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
							 errmsg("permission denied to set parameter \"%s\"",
									record->name)));
					return 0;
				}
			}
			/* fall through to process the same as PGC_BACKEND */
			pg_fallthrough;
		case PGC_BACKEND:
			if (context == PGC_SIGHUP)
			{
				/*
				 * If a PGC_BACKEND or PGC_SU_BACKEND parameter is changed in
				 * the config file, we want to accept the new value in the
				 * postmaster (whence it will propagate to
				 * subsequently-started backends), but ignore it in existing
				 * backends.  This is a tad klugy, but necessary because we
				 * don't re-read the config file during backend start.
				 *
				 * However, if changeVal is false then plow ahead anyway since
				 * we are trying to find out if the value is potentially good,
				 * not actually use it.
				 *
				 * In EXEC_BACKEND builds, this works differently: we load all
				 * non-default settings from the CONFIG_EXEC_PARAMS file
				 * during backend start.  In that case we must accept
				 * PGC_SIGHUP settings, so as to have the same value as if
				 * we'd forked from the postmaster.  This can also happen when
				 * using RestoreGUCState() within a background worker that
				 * needs to have the same settings as the user backend that
				 * started it. is_reload will be true when either situation
				 * applies.
				 */
				if (IsUnderPostmaster && changeVal && !is_reload)
					return -1;
			}
			else if (context != PGC_POSTMASTER &&
					 context != PGC_BACKEND &&
					 context != PGC_SU_BACKEND &&
					 source != PGC_S_CLIENT)
			{
				ereport(elevel,
						(errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
						 errmsg("parameter \"%s\" cannot be set after connection start",
								record->name)));
				return 0;
			}
			break;
		case PGC_SUSET:
			if (context == PGC_USERSET || context == PGC_BACKEND)
			{
				/*
				 * Check whether the requesting user has been granted
				 * privilege to set this GUC.
				 */
				AclResult	aclresult;

				aclresult = pg_parameter_aclcheck(record->name, srole, ACL_SET);
				if (aclresult != ACLCHECK_OK)
				{
					/* No granted privilege */
					ereport(elevel,
							(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
							 errmsg("permission denied to set parameter \"%s\"",
									record->name)));
					return 0;
				}
			}
			break;
		case PGC_USERSET:
			/* always okay */
			break;
	}

	/*
	 * Disallow changing GUC_NOT_WHILE_SEC_REST values if we are inside a
	 * security restriction context.  We can reject this regardless of the GUC
	 * context or source, mainly because sources that it might be reasonable
	 * to override for won't be seen while inside a function.
	 *
	 * Note: variables marked GUC_NOT_WHILE_SEC_REST should usually be marked
	 * GUC_NO_RESET_ALL as well, because ResetAllOptions() doesn't check this.
	 * An exception might be made if the reset value is assumed to be "safe".
	 *
	 * Note: this flag is currently used for "session_authorization" and
	 * "role".  We need to prohibit changing these inside a local userid
	 * context because when we exit it, GUC won't be notified, leaving things
	 * out of sync.  (This could be fixed by forcing a new GUC nesting level,
	 * but that would change behavior in possibly-undesirable ways.)  Also, we
	 * prohibit changing these in a security-restricted operation because
	 * otherwise RESET could be used to regain the session user's privileges.
	 */
	if (record->flags & GUC_NOT_WHILE_SEC_REST)
	{
		if (InLocalUserIdChange())
		{
			/*
			 * Phrasing of this error message is historical, but it's the most
			 * common case.
			 */
			ereport(elevel,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("cannot set parameter \"%s\" within security-definer function",
							record->name)));
			return 0;
		}
		if (InSecurityRestrictedOperation())
		{
			ereport(elevel,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("cannot set parameter \"%s\" within security-restricted operation",
							record->name)));
			return 0;
		}
	}

	/* Disallow resetting and saving GUC_NO_RESET values */
	if (record->flags & GUC_NO_RESET)
	{
		if (value == NULL)
		{
			ereport(elevel,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("parameter \"%s\" cannot be reset", record->name)));
			return 0;
		}
		if (action == GUC_ACTION_SAVE)
		{
			ereport(elevel,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("parameter \"%s\" cannot be set locally in functions",
							record->name)));
			return 0;
		}
	}

	/*
	 * Should we set reset/stacked values?	(If so, the behavior is not
	 * transactional.)	This is done either when we get a default value from
	 * the database's/user's/client's default settings or when we reset a
	 * value to its default.
	 */
	makeDefault = changeVal && (source <= PGC_S_OVERRIDE) &&
		((value != NULL) || source == PGC_S_DEFAULT);

	/*
	 * Ignore attempted set if overridden by previously processed setting.
	 * However, if changeVal is false then plow ahead anyway since we are
	 * trying to find out if the value is potentially good, not actually use
	 * it. Also keep going if makeDefault is true, since we may want to set
	 * the reset/stacked values even if we can't set the variable itself.
	 */
	if (GUC_SOURCE(record) > source)
	{
		if (changeVal && !makeDefault)
		{
			elog(DEBUG3, "\"%s\": setting ignored because previous source is higher priority",
				 record->name);
			return -1;
		}
		changeVal = false;
	}

	/*
	 * Evaluate value and set variable.
	 */
	switch (record->vartype)
	{
		case PGC_BOOL:
			{
				struct config_bool *conf = &record->_bool;

#define newval (newval_union.boolval)

				if (value)
				{
					if (!parse_and_validate_value(record, value,
												  source, elevel,
												  &newval_union, &newextra))
						return 0;
				}
				else if (source == PGC_S_DEFAULT)
				{
					newval = conf->boot_val;
					if (!call_bool_check_hook(record, &newval, &newextra,
											  source, elevel))
						return 0;
				}
				else
				{
					newval = GUC_RESET_BOOL(record);
					newextra = GUC_RESET_EXTRA(record);
					source = GUC_RESET_SOURCE(record);
					context = GUC_RESET_SCONTEXT(record);
					srole = GUC_RESET_SROLE(record);
				}

				if (prohibitValueChange)
				{
					/* Release newextra, unless it's reset_extra */
					if (newextra && !extra_field_used(record, newextra))
						guc_free(newextra);

					if (*GUC_VARIABLE_BOOL(record) != newval)
					{
						GUC_STATUS(record) |= GUC_PENDING_RESTART;
						ereport(elevel,
								(errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
								 errmsg("parameter \"%s\" cannot be changed without restarting the server",
										record->name)));
						return 0;
					}
					GUC_STATUS(record) &= ~GUC_PENDING_RESTART;
					return -1;
				}

				if (changeVal)
				{
					/* Save old value to support transaction abort */
					if (!makeDefault)
						push_old_value(record, action);

					if (conf->assign_hook)
						conf->assign_hook(newval, newextra);
					*GUC_VARIABLE_BOOL(record) = newval;
					set_extra_field(record, GUC_EXTRA_REF(record),
									newextra);
					set_guc_source(record, source);
					GUC_SCONTEXT(record) = context;
					GUC_SROLE(record) = srole;
				}
				if (makeDefault)
				{
					if (GUC_RESET_SOURCE(record) <= source)
					{
						GUC_RESET_BOOL(record) = newval;
						set_extra_field(record, GUC_RESET_EXTRA_REF(record),
										newextra);
						*GUC_RESET_SOURCE_REF(record) = source;
						*GUC_RESET_SCONTEXT_REF(record) = context;
						*GUC_RESET_SROLE_REF(record) = srole;
					}
					for (GucStack *stack = GUC_STACK(record); stack; stack = stack->prev)
					{
						if (stack->source <= source)
						{
							stack->prior.val.boolval = newval;
							set_extra_field(record, &stack->prior.extra,
											newextra);
							stack->source = source;
							stack->scontext = context;
							stack->srole = srole;
						}
					}
				}

				/* Perhaps we didn't install newextra anywhere */
				if (newextra && !extra_field_used(record, newextra))
					guc_free(newextra);
				break;

#undef newval
			}

		case PGC_INT:
			{
				struct config_int *conf = &record->_int;

#define newval (newval_union.intval)

				if (value)
				{
					if (!parse_and_validate_value(record, value,
												  source, elevel,
												  &newval_union, &newextra))
						return 0;
				}
				else if (source == PGC_S_DEFAULT)
				{
					newval = conf->boot_val;
					if (!call_int_check_hook(record, &newval, &newextra,
											 source, elevel))
						return 0;
				}
				else
				{
					newval = GUC_RESET_INT(record);
					newextra = GUC_RESET_EXTRA(record);
					source = GUC_RESET_SOURCE(record);
					context = GUC_RESET_SCONTEXT(record);
					srole = GUC_RESET_SROLE(record);
				}

				if (prohibitValueChange)
				{
					/* Release newextra, unless it's reset_extra */
					if (newextra && !extra_field_used(record, newextra))
						guc_free(newextra);

					if (*GUC_VARIABLE_INT(record) != newval)
					{
						GUC_STATUS(record) |= GUC_PENDING_RESTART;
						ereport(elevel,
								(errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
								 errmsg("parameter \"%s\" cannot be changed without restarting the server",
										record->name)));
						return 0;
					}
					GUC_STATUS(record) &= ~GUC_PENDING_RESTART;
					return -1;
				}

				if (changeVal)
				{
					/* Save old value to support transaction abort */
					if (!makeDefault)
						push_old_value(record, action);

					if (conf->assign_hook)
						conf->assign_hook(newval, newextra);
					*GUC_VARIABLE_INT(record) = newval;
					set_extra_field(record, GUC_EXTRA_REF(record),
									newextra);
					set_guc_source(record, source);
					GUC_SCONTEXT(record) = context;
					GUC_SROLE(record) = srole;
				}
				if (makeDefault)
				{
					if (GUC_RESET_SOURCE(record) <= source)
					{
						GUC_RESET_INT(record) = newval;
						set_extra_field(record, GUC_RESET_EXTRA_REF(record),
										newextra);
						*GUC_RESET_SOURCE_REF(record) = source;
						*GUC_RESET_SCONTEXT_REF(record) = context;
						*GUC_RESET_SROLE_REF(record) = srole;
					}
					for (GucStack *stack = GUC_STACK(record); stack; stack = stack->prev)
					{
						if (stack->source <= source)
						{
							stack->prior.val.intval = newval;
							set_extra_field(record, &stack->prior.extra,
											newextra);
							stack->source = source;
							stack->scontext = context;
							stack->srole = srole;
						}
					}
				}

				/* Perhaps we didn't install newextra anywhere */
				if (newextra && !extra_field_used(record, newextra))
					guc_free(newextra);
				break;

#undef newval
			}

		case PGC_REAL:
			{
				struct config_real *conf = &record->_real;

#define newval (newval_union.realval)

				if (value)
				{
					if (!parse_and_validate_value(record, value,
												  source, elevel,
												  &newval_union, &newextra))
						return 0;
				}
				else if (source == PGC_S_DEFAULT)
				{
					newval = conf->boot_val;
					if (!call_real_check_hook(record, &newval, &newextra,
											  source, elevel))
						return 0;
				}
				else
				{
					newval = GUC_RESET_REAL(record);
					newextra = GUC_RESET_EXTRA(record);
					source = GUC_RESET_SOURCE(record);
					context = GUC_RESET_SCONTEXT(record);
					srole = GUC_RESET_SROLE(record);
				}

				if (prohibitValueChange)
				{
					/* Release newextra, unless it's reset_extra */
					if (newextra && !extra_field_used(record, newextra))
						guc_free(newextra);

					if (*GUC_VARIABLE_REAL(record) != newval)
					{
						GUC_STATUS(record) |= GUC_PENDING_RESTART;
						ereport(elevel,
								(errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
								 errmsg("parameter \"%s\" cannot be changed without restarting the server",
										record->name)));
						return 0;
					}
					GUC_STATUS(record) &= ~GUC_PENDING_RESTART;
					return -1;
				}

				if (changeVal)
				{
					/* Save old value to support transaction abort */
					if (!makeDefault)
						push_old_value(record, action);

					if (conf->assign_hook)
						conf->assign_hook(newval, newextra);
					*GUC_VARIABLE_REAL(record) = newval;
					set_extra_field(record, GUC_EXTRA_REF(record),
									newextra);
					set_guc_source(record, source);
					GUC_SCONTEXT(record) = context;
					GUC_SROLE(record) = srole;
				}
				if (makeDefault)
				{
					if (GUC_RESET_SOURCE(record) <= source)
					{
						GUC_RESET_REAL(record) = newval;
						set_extra_field(record, GUC_RESET_EXTRA_REF(record),
										newextra);
						*GUC_RESET_SOURCE_REF(record) = source;
						*GUC_RESET_SCONTEXT_REF(record) = context;
						*GUC_RESET_SROLE_REF(record) = srole;
					}
					for (GucStack *stack = GUC_STACK(record); stack; stack = stack->prev)
					{
						if (stack->source <= source)
						{
							stack->prior.val.realval = newval;
							set_extra_field(record, &stack->prior.extra,
											newextra);
							stack->source = source;
							stack->scontext = context;
							stack->srole = srole;
						}
					}
				}

				/* Perhaps we didn't install newextra anywhere */
				if (newextra && !extra_field_used(record, newextra))
					guc_free(newextra);
				break;

#undef newval
			}

		case PGC_STRING:
			{
				struct config_string *conf = &record->_string;
				GucContext	orig_context = context;
				GucSource	orig_source = source;
				Oid			orig_srole = srole;
				bool		assign_variable;

#define newval (newval_union.stringval)

				/*
				 * Threaded backends can replay another process's non-default
				 * GUCs into a copied GUC table.  If a string GUC still points
				 * at process-global backing storage, do not replace that
				 * global with a string allocated in this session's GUC
				 * context.  Ordinary SET processing must still assign
				 * custom and extension GUCs.
				 */
				assign_variable =
					!GUCThreadedBackendReplayActive(is_reload) ||
					PgCurrentOrEarlySessionOwnsPointer(GUC_VARIABLE_STRING(record));

				if (value)
				{
					if (!parse_and_validate_value(record, value,
												  source, elevel,
												  &newval_union, &newextra))
						return 0;
				}
				else if (source == PGC_S_DEFAULT)
				{
					/* non-NULL boot_val must always get strdup'd */
					if (conf->boot_val != NULL)
					{
						newval = guc_strdup(elevel, conf->boot_val);
						if (newval == NULL)
							return 0;
					}
					else
						newval = NULL;

					if (!call_string_check_hook(record, &newval, &newextra,
												source, elevel))
					{
						guc_free(newval);
						return 0;
					}
				}
				else
				{
					/*
					 * strdup not needed, since reset_val is already under
				 * guc.c's control
					 */
					newval = GUC_RESET_STRING(record);
					newextra = GUC_RESET_EXTRA(record);
					source = GUC_RESET_SOURCE(record);
					context = GUC_RESET_SCONTEXT(record);
					srole = GUC_RESET_SROLE(record);
				}

				if (prohibitValueChange)
				{
					bool		newval_different;

					/* newval shouldn't be NULL, so we're a bit sloppy here */
					newval_different = (*GUC_VARIABLE_STRING(record) == NULL ||
										newval == NULL ||
										strcmp(*GUC_VARIABLE_STRING(record), newval) != 0);

					/* Release newval, unless it's reset_val */
					if (newval && !string_field_used(record, newval))
						guc_free(newval);
					/* Release newextra, unless it's reset_extra */
					if (newextra && !extra_field_used(record, newextra))
						guc_free(newextra);

					if (newval_different)
					{
						GUC_STATUS(record) |= GUC_PENDING_RESTART;
						ereport(elevel,
								(errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
								 errmsg("parameter \"%s\" cannot be changed without restarting the server",
										record->name)));
						return 0;
					}
					GUC_STATUS(record) &= ~GUC_PENDING_RESTART;
					return -1;
				}

				if (changeVal)
				{
					/* Save old value to support transaction abort */
					if (!makeDefault)
						push_old_value(record, action);

					if (assign_variable)
					{
						if (conf->assign_hook)
							conf->assign_hook(newval, newextra);
						set_string_field(record, GUC_VARIABLE_STRING(record),
										 newval);
						set_extra_field(record, GUC_EXTRA_REF(record),
										newextra);
					}
					set_guc_source(record, source);
					GUC_SCONTEXT(record) = context;
					GUC_SROLE(record) = srole;

					/*
					 * Ugly hack: during SET session_authorization, forcibly
					 * do SET ROLE NONE with the same context/source/etc, so
					 * that the effects will have identical lifespan.  This is
					 * required by the SQL spec, and it's not possible to do
					 * it within the variable's check hook or assign hook
					 * because our APIs for those don't pass enough info.
					 * However, don't do it if is_reload: in that case we
					 * expect that if "role" isn't supposed to be default, it
					 * has been or will be set by a separate reload action.
					 *
					 * Also, for the call from InitializeSessionUserId with
					 * source == PGC_S_OVERRIDE, use PGC_S_DYNAMIC_DEFAULT for
					 * "role"'s source, so that it's still possible to set
					 * "role" from pg_db_role_setting entries.  (See notes in
					 * InitializeSessionUserId before changing this.)
					 *
					 * A fine point: for RESET session_authorization, we do
					 * "RESET role" not "SET ROLE NONE" (by passing down NULL
					 * rather than "none" for the value).  This would have the
					 * same effects in typical cases, but if the reset value
					 * of "role" is not "none" it seems better to revert to
					 * that.
					 */
					if (!is_reload &&
						strcmp(record->name, "session_authorization") == 0)
						(void) set_config_with_handle("role", NULL,
													  value ? "none" : NULL,
													  orig_context,
													  (orig_source == PGC_S_OVERRIDE)
													  ? PGC_S_DYNAMIC_DEFAULT
													  : orig_source,
													  orig_srole,
													  action,
													  true,
													  elevel,
													  false);
				}

				if (makeDefault)
				{
					if (GUC_RESET_SOURCE(record) <= source)
					{
						set_string_field(record, &GUC_RESET_STRING(record),
										 newval);
						set_extra_field(record, GUC_RESET_EXTRA_REF(record),
										newextra);
						*GUC_RESET_SOURCE_REF(record) = source;
						*GUC_RESET_SCONTEXT_REF(record) = context;
						*GUC_RESET_SROLE_REF(record) = srole;
					}
					for (GucStack *stack = GUC_STACK(record); stack; stack = stack->prev)
					{
						if (stack->source <= source)
						{
							set_string_field(record, &stack->prior.val.stringval,
											 newval);
							set_extra_field(record, &stack->prior.extra,
											newextra);
							stack->source = source;
							stack->scontext = context;
							stack->srole = srole;
						}
					}
				}

				/* Perhaps we didn't install newval anywhere */
				if (newval && !string_field_used(record, newval))
					guc_free(newval);
				/* Perhaps we didn't install newextra anywhere */
				if (newextra && !extra_field_used(record, newextra))
					guc_free(newextra);
				break;

#undef newval
			}

		case PGC_ENUM:
			{
				struct config_enum *conf = &record->_enum;

#define newval (newval_union.enumval)

				if (value)
				{
					if (!parse_and_validate_value(record, value,
												  source, elevel,
												  &newval_union, &newextra))
						return 0;
				}
				else if (source == PGC_S_DEFAULT)
				{
					newval = conf->boot_val;
					if (!call_enum_check_hook(record, &newval, &newextra,
											  source, elevel))
						return 0;
				}
				else
				{
					newval = GUC_RESET_ENUM(record);
					newextra = GUC_RESET_EXTRA(record);
					source = GUC_RESET_SOURCE(record);
					context = GUC_RESET_SCONTEXT(record);
					srole = GUC_RESET_SROLE(record);
				}

				if (prohibitValueChange)
				{
					/* Release newextra, unless it's reset_extra */
					if (newextra && !extra_field_used(record, newextra))
						guc_free(newextra);

					if (*GUC_VARIABLE_ENUM(record) != newval)
					{
						GUC_STATUS(record) |= GUC_PENDING_RESTART;
						ereport(elevel,
								(errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
								 errmsg("parameter \"%s\" cannot be changed without restarting the server",
										record->name)));
						return 0;
					}
					GUC_STATUS(record) &= ~GUC_PENDING_RESTART;
					return -1;
				}

				if (changeVal)
				{
					/* Save old value to support transaction abort */
					if (!makeDefault)
						push_old_value(record, action);

					if (conf->assign_hook)
						conf->assign_hook(newval, newextra);
					*GUC_VARIABLE_ENUM(record) = newval;
					set_extra_field(record, GUC_EXTRA_REF(record),
									newextra);
					set_guc_source(record, source);
					GUC_SCONTEXT(record) = context;
					GUC_SROLE(record) = srole;
				}
				if (makeDefault)
				{
					if (GUC_RESET_SOURCE(record) <= source)
					{
						GUC_RESET_ENUM(record) = newval;
						set_extra_field(record, GUC_RESET_EXTRA_REF(record),
										newextra);
						*GUC_RESET_SOURCE_REF(record) = source;
						*GUC_RESET_SCONTEXT_REF(record) = context;
						*GUC_RESET_SROLE_REF(record) = srole;
					}
					for (GucStack *stack = GUC_STACK(record); stack; stack = stack->prev)
					{
						if (stack->source <= source)
						{
							stack->prior.val.enumval = newval;
							set_extra_field(record, &stack->prior.extra,
											newextra);
							stack->source = source;
							stack->scontext = context;
							stack->srole = srole;
						}
					}
				}

				/* Perhaps we didn't install newextra anywhere */
				if (newextra && !extra_field_used(record, newextra))
					guc_free(newextra);
				break;

#undef newval
			}
	}

	if (changeVal && (record->flags & GUC_REPORT) &&
		!(GUC_STATUS(record) & GUC_NEEDS_REPORT))
	{
		GUC_STATUS(record) |= GUC_NEEDS_REPORT;
		slist_push_head(&guc_report_list, GUC_REPORT_LINK(record));
	}

	return changeVal ? 1 : -1;
}


/*
 * Retrieve a config_handle for the given name, suitable for calling
 * set_config_with_handle(). Only return handle to permanent GUC.
 */
config_handle *
get_config_handle(const char *name)
{
	struct config_generic *gen = find_option(name, false, false, 0);

	if (gen && ((gen->flags & GUC_CUSTOM_PLACEHOLDER) == 0))
		return gen;

	return NULL;
}


/*
 * Set the fields for source file and line number the setting came from.
 */
static void
set_config_sourcefile(const char *name, char *sourcefile, int sourceline)
{
	struct config_generic *record;
	int			elevel;

	/*
	 * To avoid cluttering the log, only the postmaster bleats loudly about
	 * problems with the config file.
	 */
	elevel = IsUnderPostmaster ? DEBUG3 : LOG;

	record = find_option(name, true, false, elevel);
	/* should not happen */
	if (record == NULL)
		return;

	sourcefile = guc_strdup(elevel, sourcefile);
	guc_free(GUC_SOURCEFILE(record));
	GUC_SET_SOURCEFILE(record, sourcefile);
	GUC_SET_SOURCELINE(record, sourceline);
}

/*
 * Set a config option to the given value.
 *
 * See also set_config_option; this is just the wrapper to be called from
 * outside GUC.  (This function should be used when possible, because its API
 * is more stable than set_config_option's.)
 *
 * Note: there is no support here for setting source file/line, as it
 * is currently not needed.
 */
void
SetConfigOption(const char *name, const char *value,
				GucContext context, GucSource source)
{
	(void) set_config_option(name, value, context, source,
							 GUC_ACTION_SET, true, 0, false);
}



/*
 * Fetch the current value of the option `name', as a string.
 *
 * If the option doesn't exist, return NULL if missing_ok is true,
 * otherwise throw an ereport and don't return.
 *
 * If restrict_privileged is true, we also enforce that only superusers and
 * members of the pg_read_all_settings role can see GUC_SUPERUSER_ONLY
 * variables.  This should only be passed as true in user-driven calls.
 *
 * The string is *not* allocated for modification and is really only
 * valid until the next call to configuration related functions.
 */
const char *
GetConfigOption(const char *name, bool missing_ok, bool restrict_privileged)
{
	struct config_generic *record;
	static char buffer[256];

	record = find_option(name, false, missing_ok, ERROR);
	if (record == NULL)
		return NULL;
	if (restrict_privileged &&
		!ConfigOptionIsVisible(record))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to examine \"%s\"", name),
				 errdetail("Only roles with privileges of the \"%s\" role may examine this parameter.",
						   "pg_read_all_settings")));

	switch (record->vartype)
	{
		case PGC_BOOL:
			return *GUC_VARIABLE_BOOL(record) ? "on" : "off";

		case PGC_INT:
			snprintf(buffer, sizeof(buffer), "%d",
					 *GUC_VARIABLE_INT(record));
			return buffer;

		case PGC_REAL:
			snprintf(buffer, sizeof(buffer), "%g",
					 *GUC_VARIABLE_REAL(record));
			return buffer;

		case PGC_STRING:
			return *GUC_VARIABLE_STRING(record) ?
				*GUC_VARIABLE_STRING(record) : "";

		case PGC_ENUM:
			return config_enum_lookup_by_value(record,
											   *GUC_VARIABLE_ENUM(record));
	}
	return NULL;
}

/*
 * Get the RESET value associated with the given option.
 *
 * Note: this is not re-entrant, due to use of static result buffer;
 * not to mention that a string variable could have its reset_val changed.
 * Beware of assuming the result value is good for very long.
 */
const char *
GetConfigOptionResetString(const char *name)
{
	struct config_generic *record;
	static char buffer[256];

	record = find_option(name, false, false, ERROR);
	Assert(record != NULL);
	if (!ConfigOptionIsVisible(record))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to examine \"%s\"", name),
				 errdetail("Only roles with privileges of the \"%s\" role may examine this parameter.",
						   "pg_read_all_settings")));

	switch (record->vartype)
	{
		case PGC_BOOL:
			return GUC_RESET_BOOL(record) ? "on" : "off";

		case PGC_INT:
			snprintf(buffer, sizeof(buffer), "%d",
					 GUC_RESET_INT(record));
			return buffer;

		case PGC_REAL:
			snprintf(buffer, sizeof(buffer), "%g",
					 GUC_RESET_REAL(record));
			return buffer;

		case PGC_STRING:
			return GUC_RESET_STRING(record) ?
				GUC_RESET_STRING(record) : "";

		case PGC_ENUM:
			return config_enum_lookup_by_value(record,
											   GUC_RESET_ENUM(record));
	}
	return NULL;
}

/*
 * Get the GUC flags associated with the given option.
 *
 * If the option doesn't exist, return 0 if missing_ok is true,
 * otherwise throw an ereport and don't return.
 */
int
GetConfigOptionFlags(const char *name, bool missing_ok)
{
	struct config_generic *record;

	record = find_option(name, false, missing_ok, ERROR);
	if (record == NULL)
		return 0;
	return record->flags;
}

const union config_var_val *
ConfigOptionResetValue(const struct config_generic *conf)
{
	return &GUC_STATE(conf)->reset_val;
}

GucSource
ConfigOptionSource(const struct config_generic *conf)
{
	return GUC_SOURCE(conf);
}

GucContext
ConfigOptionSetContext(const struct config_generic *conf)
{
	return GUC_SCONTEXT(conf);
}

Oid
ConfigOptionSetRole(const struct config_generic *conf)
{
	return GUC_SROLE(conf);
}

const char *
ConfigOptionSourceFile(const struct config_generic *conf)
{
	return GUC_SOURCEFILE(conf);
}

int
ConfigOptionSourceLine(const struct config_generic *conf)
{
	return GUC_SOURCELINE(conf);
}

bool
ConfigOptionPendingRestart(const struct config_generic *conf)
{
	return (GUC_STATUS(conf) & GUC_PENDING_RESTART) != 0;
}


/*
 * Write updated configuration parameter values into a temporary file.
 * This function traverses the list of parameters and quotes the string
 * values before writing them.
 */
static void
write_auto_conf_file(int fd, const char *filename, ConfigVariable *head)
{
	StringInfoData buf;

	initStringInfo(&buf);

	/* Emit file header containing warning comment */
	appendStringInfoString(&buf, "# Do not edit this file manually!\n");
	appendStringInfoString(&buf, "# It will be overwritten by the ALTER SYSTEM command.\n");

	errno = 0;
	if (write(fd, buf.data, buf.len) != buf.len)
	{
		/* if write didn't set errno, assume problem is no disk space */
		if (errno == 0)
			errno = ENOSPC;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m", filename)));
	}

	/* Emit each parameter, properly quoting the value */
	for (ConfigVariable *item = head; item != NULL; item = item->next)
	{
		char	   *escaped;

		resetStringInfo(&buf);

		appendStringInfoString(&buf, item->name);
		appendStringInfoString(&buf, " = '");

		escaped = escape_single_quotes_ascii(item->value);
		if (!escaped)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
		appendStringInfoString(&buf, escaped);
		free(escaped);

		appendStringInfoString(&buf, "'\n");

		errno = 0;
		if (write(fd, buf.data, buf.len) != buf.len)
		{
			/* if write didn't set errno, assume problem is no disk space */
			if (errno == 0)
				errno = ENOSPC;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write to file \"%s\": %m", filename)));
		}
	}

	/* fsync before considering the write to be successful */
	if (pg_fsync(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m", filename)));

	pfree(buf.data);
}

/*
 * Update the given list of configuration parameters, adding, replacing
 * or deleting the entry for item "name" (delete if "value" == NULL).
 */
static void
replace_auto_config_value(ConfigVariable **head_p, ConfigVariable **tail_p,
						  const char *name, const char *value)
{
	ConfigVariable *newitem,
			   *next,
			   *prev = NULL;

	/*
	 * Remove any existing match(es) for "name".  Normally there'd be at most
	 * one, but if external tools have modified the config file, there could
	 * be more.
	 */
	for (ConfigVariable *item = *head_p; item != NULL; item = next)
	{
		next = item->next;
		if (guc_name_compare(item->name, name) == 0)
		{
			/* found a match, delete it */
			if (prev)
				prev->next = next;
			else
				*head_p = next;
			if (next == NULL)
				*tail_p = prev;

			pfree(item->name);
			pfree(item->value);
			pfree(item->filename);
			pfree(item);
		}
		else
			prev = item;
	}

	/* Done if we're trying to delete it */
	if (value == NULL)
		return;

	/* OK, append a new entry */
	newitem = palloc_object(ConfigVariable);
	newitem->name = pstrdup(name);
	newitem->value = pstrdup(value);
	newitem->errmsg = NULL;
	newitem->filename = pstrdup("");	/* new item has no location */
	newitem->sourceline = 0;
	newitem->ignore = false;
	newitem->applied = false;
	newitem->next = NULL;

	if (*head_p == NULL)
		*head_p = newitem;
	else
		(*tail_p)->next = newitem;
	*tail_p = newitem;
}


/*
 * Execute ALTER SYSTEM statement.
 *
 * Read the old PG_AUTOCONF_FILENAME file, merge in the new variable value,
 * and write out an updated file.  If the command is ALTER SYSTEM RESET ALL,
 * we can skip reading the old file and just write an empty file.
 *
 * An LWLock is used to serialize updates of the configuration file.
 *
 * In case of an error, we leave the original automatic
 * configuration file (PG_AUTOCONF_FILENAME) intact.
 */
void
AlterSystemSetConfigFile(AlterSystemStmt *altersysstmt)
{
	char	   *name;
	char	   *value;
	bool		resetall = false;
	ConfigVariable *head = NULL;
	ConfigVariable *tail = NULL;
	volatile int Tmpfd;
	char		AutoConfFileName[MAXPGPATH];
	char		AutoConfTmpFileName[MAXPGPATH];

	/*
	 * Extract statement arguments
	 */
	name = altersysstmt->setstmt->name;

	if (!AllowAlterSystem)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("ALTER SYSTEM is not allowed in this environment")));

	switch (altersysstmt->setstmt->kind)
	{
		case VAR_SET_VALUE:
			value = ExtractSetVariableArgs(altersysstmt->setstmt);
			break;

		case VAR_SET_DEFAULT:
		case VAR_RESET:
			value = NULL;
			break;

		case VAR_RESET_ALL:
			value = NULL;
			resetall = true;
			break;

		default:
			elog(ERROR, "unrecognized alter system stmt type: %d",
				 altersysstmt->setstmt->kind);
			break;
	}

	/*
	 * Check permission to run ALTER SYSTEM on the target variable
	 */
	if (!superuser())
	{
		if (resetall)
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("permission denied to perform ALTER SYSTEM RESET ALL")));
		else
		{
			AclResult	aclresult;

			aclresult = pg_parameter_aclcheck(name, GetUserId(),
											  ACL_ALTER_SYSTEM);
			if (aclresult != ACLCHECK_OK)
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						 errmsg("permission denied to set parameter \"%s\"",
								name)));
		}
	}

	/*
	 * Unless it's RESET_ALL, validate the target variable and value
	 */
	if (!resetall)
	{
		struct config_generic *record;

		/* We don't want to create a placeholder if there's not one already */
		record = find_option(name, false, true, DEBUG5);
		if (record != NULL)
		{
			/*
			 * Don't allow parameters that can't be set in configuration files
			 * to be set in PG_AUTOCONF_FILENAME file.
			 */
			if ((record->context == PGC_INTERNAL) ||
				(record->flags & GUC_DISALLOW_IN_FILE) ||
				(record->flags & GUC_DISALLOW_IN_AUTO_FILE))
				ereport(ERROR,
						(errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM),
						 errmsg("parameter \"%s\" cannot be changed",
								name)));

			/*
			 * If a value is specified, verify that it's sane.
			 */
			if (value)
			{
				union config_var_val newval;
				void	   *newextra = NULL;

				if (!parse_and_validate_value(record, value,
											  PGC_S_FILE, ERROR,
											  &newval, &newextra))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("invalid value for parameter \"%s\": \"%s\"",
									name, value)));

				if (record->vartype == PGC_STRING && newval.stringval != NULL)
					guc_free(newval.stringval);
				guc_free(newextra);
			}
		}
		else
		{
			/*
			 * Variable not known; check we'd be allowed to create it.  (We
			 * cannot validate the value, but that's fine.  A non-core GUC in
			 * the config file cannot cause postmaster start to fail, so we
			 * don't have to be too tense about possibly installing a bad
			 * value.)
			 *
			 * As an exception, we skip this check if this is a RESET command
			 * for an unknown custom GUC, else there'd be no way for users to
			 * remove such settings with reserved prefixes.
			 */
			if (value || !valid_custom_variable_name(name))
				(void) assignable_custom_variable_name(name, false, ERROR);
		}

		/*
		 * We must also reject values containing newlines, because the grammar
		 * for config files doesn't support embedded newlines in string
		 * literals.
		 */
		if (value && strchr(value, '\n'))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("parameter value for ALTER SYSTEM must not contain a newline")));
	}

	/*
	 * PG_AUTOCONF_FILENAME and its corresponding temporary file are always in
	 * the data directory, so we can reference them by simple relative paths.
	 */
	snprintf(AutoConfFileName, sizeof(AutoConfFileName), "%s",
			 PG_AUTOCONF_FILENAME);
	snprintf(AutoConfTmpFileName, sizeof(AutoConfTmpFileName), "%s.%s",
			 AutoConfFileName,
			 "tmp");

	/*
	 * Only one backend is allowed to operate on PG_AUTOCONF_FILENAME at a
	 * time.  Use AutoFileLock to ensure that.  We must hold the lock while
	 * reading the old file contents.
	 */
	LWLockAcquire(AutoFileLock, LW_EXCLUSIVE);

	/*
	 * If we're going to reset everything, then no need to open or parse the
	 * old file.  We'll just write out an empty list.
	 */
	if (!resetall)
	{
		struct stat st;

		if (stat(AutoConfFileName, &st) == 0)
		{
			/* open old file PG_AUTOCONF_FILENAME */
			FILE	   *infile;

			infile = AllocateFile(AutoConfFileName, "r");
			if (infile == NULL)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not open file \"%s\": %m",
								AutoConfFileName)));

			/* parse it */
			if (!ParseConfigFp(infile, AutoConfFileName, CONF_FILE_START_DEPTH,
							   LOG, &head, &tail))
				ereport(ERROR,
						(errcode(ERRCODE_CONFIG_FILE_ERROR),
						 errmsg("could not parse contents of file \"%s\"",
								AutoConfFileName)));

			FreeFile(infile);
		}

		/*
		 * Now, replace any existing entry with the new value, or add it if
		 * not present.
		 */
		replace_auto_config_value(&head, &tail, name, value);
	}

	/*
	 * Invoke the post-alter hook for setting this GUC variable.  GUCs
	 * typically do not have corresponding entries in pg_parameter_acl, so we
	 * call the hook using the name rather than a potentially-non-existent
	 * OID.  Nonetheless, we pass ParameterAclRelationId so that this call
	 * context can be distinguished from others.  (Note that "name" will be
	 * NULL in the RESET ALL case.)
	 *
	 * We do this here rather than at the end, because ALTER SYSTEM is not
	 * transactional.  If the hook aborts our transaction, it will be cleaner
	 * to do so before we touch any files.
	 */
	InvokeObjectPostAlterHookArgStr(ParameterAclRelationId, name,
									ACL_ALTER_SYSTEM,
									altersysstmt->setstmt->kind,
									false);

	/*
	 * To ensure crash safety, first write the new file data to a temp file,
	 * then atomically rename it into place.
	 *
	 * If there is a temp file left over due to a previous crash, it's okay to
	 * truncate and reuse it.
	 */
	Tmpfd = BasicOpenFile(AutoConfTmpFileName,
						  O_CREAT | O_RDWR | O_TRUNC);
	if (Tmpfd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m",
						AutoConfTmpFileName)));

	/*
	 * Use a TRY block to clean up the file if we fail.  Since we need a TRY
	 * block anyway, OK to use BasicOpenFile rather than OpenTransientFile.
	 */
	PG_TRY();
	{
		/* Write and sync the new contents to the temporary file */
		write_auto_conf_file(Tmpfd, AutoConfTmpFileName, head);

		/* Close before renaming; may be required on some platforms */
		close(Tmpfd);
		Tmpfd = -1;

		/*
		 * As the rename is atomic operation, if any problem occurs after this
		 * at worst it can lose the parameters set by last ALTER SYSTEM
		 * command.
		 */
		durable_rename(AutoConfTmpFileName, AutoConfFileName, ERROR);
	}
	PG_CATCH();
	{
		/* Close file first, else unlink might fail on some platforms */
		if (Tmpfd >= 0)
			close(Tmpfd);

		/* Unlink, but ignore any error */
		(void) unlink(AutoConfTmpFileName);

		PG_RE_THROW();
	}
	PG_END_TRY();

	FreeConfigVariables(head);

	LWLockRelease(AutoFileLock);
}


/*
 * Common code for DefineCustomXXXVariable subroutines: allocate the
 * new variable's config struct and fill in generic fields.
 */
static struct config_generic *
init_custom_variable(const char *name,
					 const char *short_desc,
					 const char *long_desc,
					 GucContext context,
					 int flags,
					 enum config_type type)
{
	struct config_generic *gen;
	config_generic_state *state;

	/*
	 * Only allow custom PGC_POSTMASTER variables to be created during shared
	 * library preload; any later than that, we can't ensure that the value
	 * doesn't change after startup.  This is a fatal elog if it happens; just
	 * erroring out isn't safe because we don't know what the calling loadable
	 * module might already have hooked into.
	 */
	if (context == PGC_POSTMASTER &&
		!process_shared_preload_libraries_in_progress)
		elog(FATAL, "cannot create PGC_POSTMASTER variables after startup");

	/*
	 * We can't support custom GUC_LIST_QUOTE variables, because the wrong
	 * things would happen if such a variable were set or pg_dump'd when the
	 * defining extension isn't loaded.  Again, treat this as fatal because
	 * the loadable module may be partly initialized already.
	 */
	if (flags & GUC_LIST_QUOTE)
		elog(FATAL, "extensions cannot define GUC_LIST_QUOTE variables");

	/*
	 * Before pljava commit 398f3b876ed402bdaec8bc804f29e2be95c75139
	 * (2015-12-15), two of that module's PGC_USERSET variables facilitated
	 * trivial escalation to superuser privileges.  Restrict the variables to
	 * protect sites that have yet to upgrade pljava.
	 */
	if (context == PGC_USERSET &&
		(strcmp(name, "pljava.classpath") == 0 ||
		 strcmp(name, "pljava.vmoptions") == 0))
		context = PGC_SUSET;

	/* As above, an OOM here is FATAL */
	gen = (struct config_generic *) guc_malloc(FATAL, sizeof(struct config_generic));
	memset(gen, 0, sizeof(struct config_generic));
	state = (config_generic_state *) guc_malloc(FATAL,
											   sizeof(config_generic_state));
	memset(state, 0, sizeof(config_generic_state));
	gen->state = state;

	gen->name = guc_strdup(FATAL, name);
	gen->context = context;
	gen->group = CUSTOM_OPTIONS;
	gen->short_desc = short_desc;
	gen->long_desc = long_desc;
	gen->flags = flags;
	gen->vartype = type;

	return gen;
}

/*
 * Common code for DefineCustomXXXVariable subroutines: insert the new
 * variable into the GUC variable hash, replacing any placeholder.
 */
static void
define_custom_variable(struct config_generic *variable)
{
	const char *name = variable->name;
	GUCHashEntry *hentry;
	struct config_generic *pHolder;

	/* Check mapping between initial and default value */
	Assert(check_GUC_init(variable));

	if (find_builtin_option(name) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("attempt to redefine parameter \"%s\"", name)));

	/*
	 * See if there's a placeholder by the same name.
	 */
	hentry = NULL;
	if (guc_hashtab != NULL)
		hentry = (GUCHashEntry *) hash_search(guc_hashtab,
											  &name,
											  HASH_FIND,
											  NULL);
	if (hentry == NULL)
	{
		/*
		 * No placeholder to replace, so we can just add it ... but first,
		 * make sure it's initialized to its default value.
		 */
		InitializeOneGUCOption(variable);
		add_guc_variable(variable, ERROR);
		return;
	}

	/*
	 * This better be a placeholder
	 */
	if ((hentry->gucvar->flags & GUC_CUSTOM_PLACEHOLDER) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("attempt to redefine parameter \"%s\"", name)));

	Assert(hentry->gucvar->vartype == PGC_STRING);
	pHolder = hentry->gucvar;

	/*
	 * First, set the variable to its default value.  We must do this even
	 * though we intend to immediately apply a new value, since it's possible
	 * that the new value is invalid.
	 */
	InitializeOneGUCOption(variable);

	/*
	 * Replace the placeholder in the hash table.  We aren't changing the name
	 * (at least up to case-folding), so the hash value is unchanged.
	 */
	hentry->gucname = name;
	hentry->gucvar = variable;

	/*
	 * Remove the placeholder from any lists it's in, too.
	 */
	RemoveGUCFromLists(pHolder);

	/*
	 * Assign the string value(s) stored in the placeholder to the real
	 * variable.  Essentially, we need to duplicate all the active and stacked
	 * values, but with appropriate validation and datatype adjustment.
	 *
	 * If an assignment fails, we report a WARNING and keep going.  We don't
	 * want to throw ERROR for bad values, because it'd bollix the add-on
	 * module that's presumably halfway through getting loaded.  In such cases
	 * the default or previous state will become active instead.
	 */

	/* First, apply the reset value if any */
	if (GUC_RESET_STRING(pHolder))
		(void) set_config_option_ext(name, GUC_RESET_STRING(pHolder),
									 GUC_RESET_SCONTEXT(pHolder),
									 GUC_RESET_SOURCE(pHolder),
									 GUC_RESET_SROLE(pHolder),
									 GUC_ACTION_SET, true, WARNING, false);
	/* That should not have resulted in stacking anything */
	Assert(GUC_STACK(variable) == NULL);

	/* Now, apply current and stacked values, in the order they were stacked */
	reapply_stacked_values(variable, pHolder, GUC_STACK(pHolder),
						   *(GUC_VARIABLE_STRING(pHolder)),
						   GUC_SCONTEXT(pHolder), GUC_SOURCE(pHolder),
						   GUC_SROLE(pHolder));

	/* Also copy over any saved source-location information */
	if (GUC_SOURCEFILE(pHolder))
		set_config_sourcefile(name, GUC_SOURCEFILE(pHolder),
							  GUC_SOURCELINE(pHolder));

	/* Now we can free the no-longer-referenced placeholder variable */
	free_placeholder(pHolder);
}

/*
 * Recursive subroutine for define_custom_variable: reapply non-reset values
 *
 * We recurse so that the values are applied in the same order as originally.
 * At each recursion level, apply the upper-level value (passed in) in the
 * fashion implied by the stack entry.
 */
static void
reapply_stacked_values(struct config_generic *variable,
					   struct config_generic *pHolder,
					   GucStack *stack,
					   const char *curvalue,
					   GucContext curscontext, GucSource cursource,
					   Oid cursrole)
{
	const char *name = variable->name;
	GucStack   *oldvarstack = GUC_STACK(variable);

	if (stack != NULL)
	{
		/* First, recurse, so that stack items are processed bottom to top */
		reapply_stacked_values(variable, pHolder, stack->prev,
							   stack->prior.val.stringval,
							   stack->scontext, stack->source, stack->srole);

		/* See how to apply the passed-in value */
		switch (stack->state)
		{
			case GUC_SAVE:
				(void) set_config_option_ext(name, curvalue,
											 curscontext, cursource, cursrole,
											 GUC_ACTION_SAVE, true,
											 WARNING, false);
				break;

			case GUC_SET:
				(void) set_config_option_ext(name, curvalue,
											 curscontext, cursource, cursrole,
											 GUC_ACTION_SET, true,
											 WARNING, false);
				break;

			case GUC_LOCAL:
				(void) set_config_option_ext(name, curvalue,
											 curscontext, cursource, cursrole,
											 GUC_ACTION_LOCAL, true,
											 WARNING, false);
				break;

			case GUC_SET_LOCAL:
				/* first, apply the masked value as SET */
				(void) set_config_option_ext(name, stack->masked.val.stringval,
											 stack->masked_scontext,
											 PGC_S_SESSION,
											 stack->masked_srole,
											 GUC_ACTION_SET, true,
											 WARNING, false);
				/* then apply the current value as LOCAL */
				(void) set_config_option_ext(name, curvalue,
											 curscontext, cursource, cursrole,
											 GUC_ACTION_LOCAL, true,
											 WARNING, false);
				break;
		}

		/* If we successfully made a stack entry, adjust its nest level */
		if (GUC_STACK(variable) != oldvarstack)
			GUC_STACK(variable)->nest_level = stack->nest_level;
	}
	else
	{
		/*
		 * We are at the end of the stack.  If the active/previous value is
		 * different from the reset value, it must represent a previously
		 * committed session value.  Apply it, and then drop the stack entry
		 * that set_config_option will have created under the impression that
		 * this is to be just a transactional assignment.  (We leak the stack
		 * entry.)
		 */
		if (curvalue != GUC_RESET_STRING(pHolder) ||
			curscontext != GUC_RESET_SCONTEXT(pHolder) ||
			cursource != GUC_RESET_SOURCE(pHolder) ||
			cursrole != GUC_RESET_SROLE(pHolder))
		{
			(void) set_config_option_ext(name, curvalue,
										 curscontext, cursource, cursrole,
										 GUC_ACTION_SET, true, WARNING, false);
			if (GUC_STACK(variable) != NULL)
			{
				slist_delete(&guc_stack_list, GUC_STACK_LINK(variable));
				GUC_SET_STACK(variable, NULL);
			}
		}
	}
}

/*
 * Free up a no-longer-referenced placeholder GUC variable.
 *
 * This neglects any stack items, so it's possible for some memory to be
 * leaked.  Since this can only happen once per session per variable, it
 * doesn't seem worth spending much code on.
 */
static void
free_placeholder(struct config_generic *pHolder)
{
	/* Placeholders are always STRING type, so free their values */
	Assert(pHolder->vartype == PGC_STRING);
	set_string_field(pHolder, GUC_VARIABLE_STRING(pHolder), NULL);
	set_string_field(pHolder, &GUC_RESET_STRING(pHolder), NULL);

	guc_free(unconstify(char *, pHolder->name));
	guc_free(pHolder->state);
	guc_free(pHolder);
}

/*
 * Functions for extensions to call to define their custom GUC variables.
 */
void
DefineCustomBoolVariable(const char *name,
						 const char *short_desc,
						 const char *long_desc,
						 bool *valueAddr,
						 bool bootValue,
						 GucContext context,
						 int flags,
						 GucBoolCheckHook check_hook,
						 GucBoolAssignHook assign_hook,
						 GucShowHook show_hook)
{
	struct config_generic *var;

	var = init_custom_variable(name, short_desc, long_desc, context, flags, PGC_BOOL);
	GUC_VARIABLE_BOOL(var) = valueAddr;
	var->_bool.boot_val = bootValue;
	GUC_RESET_BOOL(var) = bootValue;
	var->_bool.check_hook = check_hook;
	var->_bool.assign_hook = assign_hook;
	var->_bool.show_hook = show_hook;
	define_custom_variable(var);
}

void
DefineCustomIntVariable(const char *name,
						const char *short_desc,
						const char *long_desc,
						int *valueAddr,
						int bootValue,
						int minValue,
						int maxValue,
						GucContext context,
						int flags,
						GucIntCheckHook check_hook,
						GucIntAssignHook assign_hook,
						GucShowHook show_hook)
{
	struct config_generic *var;

	var = init_custom_variable(name, short_desc, long_desc, context, flags, PGC_INT);
	GUC_VARIABLE_INT(var) = valueAddr;
	var->_int.boot_val = bootValue;
	GUC_RESET_INT(var) = bootValue;
	var->_int.min = minValue;
	var->_int.max = maxValue;
	var->_int.check_hook = check_hook;
	var->_int.assign_hook = assign_hook;
	var->_int.show_hook = show_hook;
	define_custom_variable(var);
}

void
DefineCustomRealVariable(const char *name,
						 const char *short_desc,
						 const char *long_desc,
						 double *valueAddr,
						 double bootValue,
						 double minValue,
						 double maxValue,
						 GucContext context,
						 int flags,
						 GucRealCheckHook check_hook,
						 GucRealAssignHook assign_hook,
						 GucShowHook show_hook)
{
	struct config_generic *var;

	var = init_custom_variable(name, short_desc, long_desc, context, flags, PGC_REAL);
	GUC_VARIABLE_REAL(var) = valueAddr;
	var->_real.boot_val = bootValue;
	GUC_RESET_REAL(var) = bootValue;
	var->_real.min = minValue;
	var->_real.max = maxValue;
	var->_real.check_hook = check_hook;
	var->_real.assign_hook = assign_hook;
	var->_real.show_hook = show_hook;
	define_custom_variable(var);
}

void
DefineCustomStringVariable(const char *name,
						   const char *short_desc,
						   const char *long_desc,
						   char **valueAddr,
						   const char *bootValue,
						   GucContext context,
						   int flags,
						   GucStringCheckHook check_hook,
						   GucStringAssignHook assign_hook,
						   GucShowHook show_hook)
{
	struct config_generic *var;

	var = init_custom_variable(name, short_desc, long_desc, context, flags, PGC_STRING);
	GUC_VARIABLE_STRING(var) = valueAddr;
	var->_string.boot_val = bootValue;
	var->_string.check_hook = check_hook;
	var->_string.assign_hook = assign_hook;
	var->_string.show_hook = show_hook;
	define_custom_variable(var);
}

void
DefineCustomEnumVariable(const char *name,
						 const char *short_desc,
						 const char *long_desc,
						 int *valueAddr,
						 int bootValue,
						 const struct config_enum_entry *options,
						 GucContext context,
						 int flags,
						 GucEnumCheckHook check_hook,
						 GucEnumAssignHook assign_hook,
						 GucShowHook show_hook)
{
	struct config_generic *var;

	var = init_custom_variable(name, short_desc, long_desc, context, flags, PGC_ENUM);
	GUC_VARIABLE_ENUM(var) = valueAddr;
	var->_enum.boot_val = bootValue;
	GUC_RESET_ENUM(var) = bootValue;
	var->_enum.options = options;
	var->_enum.check_hook = check_hook;
	var->_enum.assign_hook = assign_hook;
	var->_enum.show_hook = show_hook;
	define_custom_variable(var);
}

/*
 * Mark the given GUC prefix as "reserved".
 *
 * This deletes any existing placeholders matching the prefix,
 * and then prevents new ones from being created.
 * Extensions should call this after they've defined all of their custom
 * GUCs, to help catch misspelled config-file entries.
 */
void
MarkGUCPrefixReserved(const char *className)
{
	int			classLen = strlen(className);
	HASH_SEQ_STATUS status;
	GUCHashEntry *hentry;
	MemoryContext oldcontext;
	bool		locked;

	locked = ThreadedGUCLock();
	PG_TRY();
	{
		/*
		 * Check for existing placeholders.  We must actually remove invalid
		 * placeholders, else future parallel worker startups will fail.
		 */
		if (guc_hashtab != NULL)
		{
			hash_seq_init(&status, guc_hashtab);
			while ((hentry = (GUCHashEntry *) hash_seq_search(&status)) != NULL)
			{
				struct config_generic *var = hentry->gucvar;

				if ((var->flags & GUC_CUSTOM_PLACEHOLDER) != 0 &&
					strncmp(className, var->name, classLen) == 0 &&
					var->name[classLen] == GUC_QUALIFIER_SEPARATOR)
				{
					ereport(WARNING,
							(errcode(ERRCODE_INVALID_NAME),
							 errmsg("invalid configuration parameter name \"%s\", removing it",
									var->name),
							 errdetail("\"%s\" is now a reserved prefix.",
									   className)));
					/* Remove it from the hash table */
					hash_search(guc_hashtab,
								&var->name,
								HASH_REMOVE,
								NULL);
					/* Remove it from any lists it's in, too */
					RemoveGUCFromLists(var);
					/* And free it */
					free_placeholder(var);
				}
			}
		}

		/* And remember the name so we can prevent future mistakes. */
		oldcontext = MemoryContextSwitchTo(GUCReservedPrefixContext());
		reserved_class_prefix = lappend(reserved_class_prefix, pstrdup(className));
		MemoryContextSwitchTo(oldcontext);
	}
	PG_FINALLY();
	{
		ThreadedGUCUnlock(locked);
	}
	PG_END_TRY();
}


/*
 * Return an array of modified GUC options to show in EXPLAIN.
 *
 * We only report options related to query planning (marked with GUC_EXPLAIN),
 * with values different from their built-in defaults.
 */
struct config_generic **
get_explain_guc_options(int *num)
{
	struct config_generic **result;
	dlist_iter	iter;

	*num = 0;

	/*
	 * While only a fraction of all the GUC variables are marked GUC_EXPLAIN,
	 * it doesn't seem worth dynamically resizing this array.
	 */
	result = palloc_array(struct config_generic *,
						  num_guc_variables + guc_custom_variable_count());

	/* We need only consider GUCs with source not PGC_S_DEFAULT */
	dlist_foreach(iter, &guc_nondef_list)
	{
		config_generic_cold_state *cold = dlist_container(config_generic_cold_state,
														  nondef_link, iter.cur);
		struct config_generic *conf = GUC_COLD_STATE_RECORD(cold);
		bool		modified;

		/* return only parameters marked for inclusion in explain */
		if (!(conf->flags & GUC_EXPLAIN))
			continue;

		/* return only options visible to the current user */
		if (!ConfigOptionIsVisible(conf))
			continue;

		/* return only options that are different from their boot values */
		modified = false;

		switch (conf->vartype)
		{
			case PGC_BOOL:
				{
					struct config_bool *lconf = &conf->_bool;

					modified = (lconf->boot_val != *GUC_VARIABLE_BOOL(conf));
				}
				break;

			case PGC_INT:
				{
					struct config_int *lconf = &conf->_int;

					modified = (lconf->boot_val != *GUC_VARIABLE_INT(conf));
				}
				break;

			case PGC_REAL:
				{
					struct config_real *lconf = &conf->_real;

					modified = (lconf->boot_val != *GUC_VARIABLE_REAL(conf));
				}
				break;

			case PGC_STRING:
				{
					struct config_string *lconf = &conf->_string;

					if (lconf->boot_val == NULL &&
						*GUC_VARIABLE_STRING(conf) == NULL)
						modified = false;
					else if (lconf->boot_val == NULL ||
							 *GUC_VARIABLE_STRING(conf) == NULL)
						modified = true;
					else
						modified = (strcmp(lconf->boot_val,
										   *GUC_VARIABLE_STRING(conf)) != 0);
				}
				break;

			case PGC_ENUM:
				{
					struct config_enum *lconf = &conf->_enum;

					modified = (lconf->boot_val != *GUC_VARIABLE_ENUM(conf));
				}
				break;

			default:
				elog(ERROR, "unexpected GUC type: %d", conf->vartype);
		}

		if (!modified)
			continue;

		/* OK, report it */
		result[*num] = conf;
		*num = *num + 1;
	}

	return result;
}

/*
 * Return GUC variable value by name; optionally return canonical form of
 * name.  If the GUC is unset, then throw an error unless missing_ok is true,
 * in which case return NULL.  Return value is palloc'd (but *varname isn't).
 */
char *
GetConfigOptionByName(const char *name, const char **varname, bool missing_ok)
{
	struct config_generic *record;

	record = find_option(name, false, missing_ok, ERROR);
	if (record == NULL)
	{
		if (varname)
			*varname = NULL;
		return NULL;
	}

	if (!ConfigOptionIsVisible(record))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to examine \"%s\"", name),
				 errdetail("Only roles with privileges of the \"%s\" role may examine this parameter.",
						   "pg_read_all_settings")));

	if (varname)
		*varname = record->name;

	return ShowGUCOption(record, true);
}

/*
 * ShowGUCOption: get string value of variable
 *
 * We express a numeric value in appropriate units if it has units and
 * use_units is true; else you just get the raw number.
 * The result string is palloc'd.
 */
char *
ShowGUCOption(const struct config_generic *record, bool use_units)
{
	char	   *result;
	bool		locked = false;
	bool		needs_lock;

	needs_lock = GUCShowOptionNeedsThreadedLock(record);
	if (needs_lock)
		locked = ThreadedGUCLock();
	PG_TRY();
	{
		result = ShowGUCOptionInternal(record, use_units);
	}
	PG_FINALLY();
	{
		if (needs_lock)
			ThreadedGUCUnlock(locked);
	}
	PG_END_TRY();

	return result;
}

static char *
ShowGUCOptionInternal(const struct config_generic *record, bool use_units)
{
	char		buffer[256];
	const char *val;

	switch (record->vartype)
	{
		case PGC_BOOL:
			{
				const struct config_bool *conf = &record->_bool;

				if (conf->show_hook)
					val = conf->show_hook();
				else
					val = *GUC_VARIABLE_BOOL(record) ? "on" : "off";
			}
			break;

		case PGC_INT:
			{
				const struct config_int *conf = &record->_int;

				if (conf->show_hook)
					val = conf->show_hook();
				else
				{
					/*
					 * Use int64 arithmetic to avoid overflows in units
					 * conversion.
					 */
					int64		result = *GUC_VARIABLE_INT(record);
					const char *unit;

					if (use_units && result > 0 && (record->flags & GUC_UNIT))
						convert_int_from_base_unit(result,
												   record->flags & GUC_UNIT,
												   &result, &unit);
					else
						unit = "";

					snprintf(buffer, sizeof(buffer), INT64_FORMAT "%s",
							 result, unit);
					val = buffer;
				}
			}
			break;

		case PGC_REAL:
			{
				const struct config_real *conf = &record->_real;

				if (conf->show_hook)
					val = conf->show_hook();
				else
				{
					double		result = *GUC_VARIABLE_REAL(record);
					const char *unit;

					if (use_units && result > 0 && (record->flags & GUC_UNIT))
						convert_real_from_base_unit(result,
													record->flags & GUC_UNIT,
													&result, &unit);
					else
						unit = "";

					snprintf(buffer, sizeof(buffer), "%g%s",
							 result, unit);
					val = buffer;
				}
			}
			break;

		case PGC_STRING:
			{
				const struct config_string *conf = &record->_string;

				if (conf->show_hook)
					val = conf->show_hook();
				else if (*GUC_VARIABLE_STRING(record) &&
						 **GUC_VARIABLE_STRING(record))
					val = *GUC_VARIABLE_STRING(record);
				else
					val = "";
			}
			break;

		case PGC_ENUM:
			{
				const struct config_enum *conf = &record->_enum;

				if (conf->show_hook)
					val = conf->show_hook();
				else
					val = config_enum_lookup_by_value(record,
													  *GUC_VARIABLE_ENUM(record));
			}
			break;

		default:
			/* just to keep compiler quiet */
			val = "???";
			break;
	}

	return pstrdup(val);
}


/*
 *	These routines dump out all non-default GUC options into a binary
 *	file that is read by all exec'ed or threaded backends.  The format is:
 *
 *		variable name, string, null terminated
 *		variable value, string, null terminated
 *		variable sourcefile, string, null terminated (empty if none)
 *		variable sourceline, integer
 *		variable source, integer
 *		variable scontext, integer
 *		variable srole, OID
 */
static void
write_one_nondefault_variable(FILE *fp, struct config_generic *gconf)
{
	Assert(GUC_SOURCE(gconf) != PGC_S_DEFAULT);

	fprintf(fp, "%s", gconf->name);
	fputc(0, fp);

	switch (gconf->vartype)
	{
		case PGC_BOOL:
			{
				if (*GUC_VARIABLE_BOOL(gconf))
					fprintf(fp, "true");
				else
					fprintf(fp, "false");
			}
			break;

		case PGC_INT:
			{
				fprintf(fp, "%d", *GUC_VARIABLE_INT(gconf));
			}
			break;

		case PGC_REAL:
			{
				fprintf(fp, "%.17g", *GUC_VARIABLE_REAL(gconf));
			}
			break;

		case PGC_STRING:
			{
				if (strcmp(gconf->name, "client_encoding") == 0)
					fprintf(fp, "%s", pg_get_client_encoding_name());
				else if (*GUC_VARIABLE_STRING(gconf))
					fprintf(fp, "%s", *GUC_VARIABLE_STRING(gconf));
			}
			break;

		case PGC_ENUM:
			{
				fprintf(fp, "%s",
						config_enum_lookup_by_value(gconf,
												   *GUC_VARIABLE_ENUM(gconf)));
			}
			break;
	}

	fputc(0, fp);

	if (GUC_SOURCEFILE(gconf))
		fprintf(fp, "%s", GUC_SOURCEFILE(gconf));
	fputc(0, fp);

	{
		int			sourceline = GUC_SOURCELINE(gconf);
		GucSource	source = GUC_SOURCE(gconf);
		GucContext	scontext = GUC_SCONTEXT(gconf);
		Oid			srole = GUC_SROLE(gconf);

		fwrite(&sourceline, 1, sizeof(sourceline), fp);
		fwrite(&source, 1, sizeof(source), fp);
		fwrite(&scontext, 1, sizeof(scontext), fp);
		fwrite(&srole, 1, sizeof(srole), fp);
	}
}

void
write_nondefault_variables(GucContext context)
{
	int			elevel;
	FILE	   *fp;
	dlist_iter	iter;

	Assert(context == PGC_POSTMASTER || context == PGC_SIGHUP);

	elevel = (context == PGC_SIGHUP) ? LOG : ERROR;

	/*
	 * Open file
	 */
	fp = AllocateFile(CONFIG_EXEC_PARAMS_NEW, "w");
	if (!fp)
	{
		ereport(elevel,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m",
						CONFIG_EXEC_PARAMS_NEW)));
		return;
	}

	/* We need only consider GUCs with source not PGC_S_DEFAULT */
	dlist_foreach(iter, &guc_nondef_list)
	{
		config_generic_cold_state *cold = dlist_container(config_generic_cold_state,
														  nondef_link, iter.cur);
		struct config_generic *gconf = GUC_COLD_STATE_RECORD(cold);

		write_one_nondefault_variable(fp, gconf);
	}

	if (FreeFile(fp))
	{
		ereport(elevel,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m",
						CONFIG_EXEC_PARAMS_NEW)));
		return;
	}

	/*
	 * Put new file in place.  This could delay on Win32, but we don't hold
	 * any exclusive locks.
	 */
	rename(CONFIG_EXEC_PARAMS_NEW, CONFIG_EXEC_PARAMS);
}


/*
 *	Read string, including null byte from file
 *
 *	Return NULL on EOF and nothing read
 */
static char *
read_string_with_null(FILE *fp)
{
	int			i = 0,
				ch,
				maxlen = 256;
	char	   *str = NULL;

	do
	{
		if ((ch = fgetc(fp)) == EOF)
		{
			if (i == 0)
				return NULL;
			else
				elog(FATAL, "invalid format of exec config params file");
		}
		if (i == 0)
			str = guc_malloc(FATAL, maxlen);
		else if (i == maxlen)
			str = guc_realloc(FATAL, str, maxlen *= 2);
		str[i++] = ch;
	} while (ch != 0);

	return str;
}


/*
 *	This routine loads a previous postmaster dump of its non-default
 *	settings.
 */
void
read_nondefault_variables(void)
{
	FILE	   *fp;
	char	   *varname,
			   *varvalue,
			   *varsourcefile;
	int			varsourceline;
	GucSource	varsource;
	GucContext	varscontext;
	Oid			varsrole;

	/*
	 * Open file
	 */
	fp = AllocateFile(CONFIG_EXEC_PARAMS, "r");
	if (!fp)
	{
		/* File not found is fine */
		if (errno != ENOENT)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not read from file \"%s\": %m",
							CONFIG_EXEC_PARAMS)));
		return;
	}

	for (;;)
	{
		struct config_generic *record;

		if ((varname = read_string_with_null(fp)) == NULL)
			break;

		record = find_option(varname, true, false, FATAL);
		if (record == NULL)
			elog(FATAL, "failed to locate variable \"%s\" in exec config params file", varname);

		if ((varvalue = read_string_with_null(fp)) == NULL)
			elog(FATAL, "invalid format of exec config params file");
		if ((varsourcefile = read_string_with_null(fp)) == NULL)
			elog(FATAL, "invalid format of exec config params file");
		if (fread(&varsourceline, 1, sizeof(varsourceline), fp) != sizeof(varsourceline))
			elog(FATAL, "invalid format of exec config params file");
		if (fread(&varsource, 1, sizeof(varsource), fp) != sizeof(varsource))
			elog(FATAL, "invalid format of exec config params file");
		if (fread(&varscontext, 1, sizeof(varscontext), fp) != sizeof(varscontext))
			elog(FATAL, "invalid format of exec config params file");
		if (fread(&varsrole, 1, sizeof(varsrole), fp) != sizeof(varsrole))
			elog(FATAL, "invalid format of exec config params file");

		/*
		 * Threaded backends share the postmaster address space.  Postmaster
		 * and internal GUCs are already present in runtime-global storage; a
		 * thread carrier must not replay them through a session GUC context,
		 * because doing so can replace/free strings owned by the postmaster's
		 * GUC context.  Still replay backend/session/user settings so logical
		 * backends see the same effective configuration as forked children.
		 */
		if (multithreaded &&
			IsUnderPostmaster &&
			(record->context == PGC_POSTMASTER ||
			 record->context == PGC_INTERNAL))
		{
			guc_free(varname);
			guc_free(varvalue);
			guc_free(varsourcefile);
			continue;
		}

		(void) set_config_option_ext(varname, varvalue,
									 varscontext, varsource, varsrole,
									 GUC_ACTION_SET, true, 0, true);
		if (varsourcefile[0])
			set_config_sourcefile(varname, varsourcefile, varsourceline);

		guc_free(varname);
		guc_free(varvalue);
		guc_free(varsourcefile);
	}

	FreeFile(fp);
}

/*
 * can_skip_gucvar:
 * Decide whether SerializeGUCState can skip sending this GUC variable,
 * or whether RestoreGUCState can skip resetting this GUC to default.
 *
 * It is somewhat magical and fragile that the same test works for both cases.
 * Realize in particular that we are very likely selecting different sets of
 * GUCs on the leader and worker sides!  Be sure you've understood the
 * comments here and in RestoreGUCState thoroughly before changing this.
 */
static bool
can_skip_gucvar(struct config_generic *gconf)
{
	/*
	 * We can skip GUCs that are guaranteed to have the same values in leaders
	 * and workers.  (Note it is critical that the leader and worker have the
	 * same idea of which GUCs fall into this category.  It's okay to consider
	 * context and name for this purpose, since those are unchanging
	 * properties of a GUC.)
	 *
	 * PGC_POSTMASTER variables always have the same value in every child of a
	 * particular postmaster, so the worker will certainly have the right
	 * value already.  Likewise, PGC_INTERNAL variables are set by special
	 * mechanisms (if indeed they aren't compile-time constants).  So we may
	 * always skip these.
	 *
	 * For all other GUCs, we skip if the GUC has its compiled-in default
	 * value (i.e., source == PGC_S_DEFAULT).  On the leader side, this means
	 * we don't send GUCs that have their default values, which typically
	 * saves lots of work.  On the worker side, this means we don't need to
	 * reset the GUC to default because it already has that value.  See
	 * comments in RestoreGUCState for more info.
	 *
	 * Threaded workers share an address space with the leader and postmaster.
	 * Until every shippable GUC has PgSession-owned backing storage, do not
	 * reset or replay records whose direct variable slot is still
	 * process-global.  Process-mode workers keep the historical behavior
	 * because their address-space copy makes those writes private.
	 */
	if (multithreaded &&
		IsUnderPostmaster &&
		!GUCRecordVariableIsCurrentSessionOwned(gconf))
		return true;

	return gconf->context == PGC_POSTMASTER ||
		gconf->context == PGC_INTERNAL ||
		GUC_SOURCE(gconf) == PGC_S_DEFAULT;
}

/*
 * estimate_variable_size:
 *		Compute space needed for dumping the given GUC variable.
 *
 * It's OK to overestimate, but not to underestimate.
 */
static Size
estimate_variable_size(struct config_generic *gconf)
{
	Size		size;
	Size		valsize = 0;

	/* Skippable GUCs consume zero space. */
	if (can_skip_gucvar(gconf))
		return 0;

	/* Name, plus trailing zero byte. */
	size = strlen(gconf->name) + 1;

	/* Get the maximum display length of the GUC value. */
	switch (gconf->vartype)
	{
		case PGC_BOOL:
			{
				valsize = 5;	/* max(strlen('true'), strlen('false')) */
			}
			break;

		case PGC_INT:
			{
				/*
				 * Instead of getting the exact display length, use max
				 * length.  Also reduce the max length for typical ranges of
				 * small values.  Maximum value is 2147483647, i.e. 10 chars.
				 * Include one byte for sign.
				 */
				if (abs(*GUC_VARIABLE_INT(gconf)) < 1000)
					valsize = 3 + 1;
				else
					valsize = 10 + 1;
			}
			break;

		case PGC_REAL:
			{
				/*
				 * We are going to print it with %e with REALTYPE_PRECISION
				 * fractional digits.  Account for sign, leading digit,
				 * decimal point, and exponent with up to 3 digits.  E.g.
				 * -3.99329042340000021e+110
				 */
				valsize = 1 + 1 + 1 + REALTYPE_PRECISION + 5;
			}
			break;

		case PGC_STRING:
			{
				/*
				 * If the value is NULL, we transmit it as an empty string.
				 * Although this is not physically the same value, GUC
				 * generally treats a NULL the same as empty string.
				 */
				if (*GUC_VARIABLE_STRING(gconf))
					valsize = strlen(*GUC_VARIABLE_STRING(gconf));
				else
					valsize = 0;
			}
			break;

		case PGC_ENUM:
			{
				valsize = strlen(config_enum_lookup_by_value(gconf,
															 *GUC_VARIABLE_ENUM(gconf)));
			}
			break;
	}

	/* Allow space for terminating zero-byte for value */
	size = add_size(size, valsize + 1);

	if (GUC_SOURCEFILE(gconf))
		size = add_size(size, strlen(GUC_SOURCEFILE(gconf)));

	/* Allow space for terminating zero-byte for sourcefile */
	size = add_size(size, 1);

	/* Include line whenever file is nonempty. */
	if (GUC_SOURCEFILE(gconf) && GUC_SOURCEFILE(gconf)[0])
		size = add_size(size, sizeof(GUC_SOURCELINE(gconf)));

	size = add_size(size, sizeof(GucSource));
	size = add_size(size, sizeof(GucContext));
	size = add_size(size, sizeof(Oid));

	return size;
}

/*
 * EstimateGUCStateSpace:
 * Returns the size needed to store the GUC state for the current process
 */
Size
EstimateGUCStateSpace(void)
{
	Size		size;
	dlist_iter	iter;

	/* Add space reqd for saving the data size of the guc state */
	size = sizeof(Size);

	/*
	 * Add up the space needed for each GUC variable.
	 *
	 * We need only process non-default GUCs.
	 */
	dlist_foreach(iter, &guc_nondef_list)
	{
		config_generic_cold_state *cold = dlist_container(config_generic_cold_state,
														  nondef_link, iter.cur);
		struct config_generic *gconf = GUC_COLD_STATE_RECORD(cold);

		size = add_size(size, estimate_variable_size(gconf));
	}

	return size;
}

/*
 * do_serialize:
 * Copies the formatted string into the destination.  Moves ahead the
 * destination pointer, and decrements the maxbytes by that many bytes. If
 * maxbytes is not sufficient to copy the string, error out.
 */
static void
do_serialize(char **destptr, Size *maxbytes, const char *fmt, ...)
{
	va_list		vargs;
	int			n;

	if (*maxbytes <= 0)
		elog(ERROR, "not enough space to serialize GUC state");

	va_start(vargs, fmt);
	n = vsnprintf(*destptr, *maxbytes, fmt, vargs);
	va_end(vargs);

	if (n < 0)
	{
		/* Shouldn't happen. Better show errno description. */
		elog(ERROR, "vsnprintf failed: %m with format string \"%s\"", fmt);
	}
	if (n >= *maxbytes)
	{
		/* This shouldn't happen either, really. */
		elog(ERROR, "not enough space to serialize GUC state");
	}

	/* Shift the destptr ahead of the null terminator */
	*destptr += n + 1;
	*maxbytes -= n + 1;
}

/* Binary copy version of do_serialize() */
static void
do_serialize_binary(char **destptr, Size *maxbytes, void *val, Size valsize)
{
	if (valsize > *maxbytes)
		elog(ERROR, "not enough space to serialize GUC state");

	memcpy(*destptr, val, valsize);
	*destptr += valsize;
	*maxbytes -= valsize;
}

/*
 * serialize_variable:
 * Dumps name, value and other information of a GUC variable into destptr.
 */
static void
serialize_variable(char **destptr, Size *maxbytes,
				   struct config_generic *gconf)
{
	/* Ignore skippable GUCs. */
	if (can_skip_gucvar(gconf))
		return;

	do_serialize(destptr, maxbytes, "%s", gconf->name);

	switch (gconf->vartype)
	{
		case PGC_BOOL:
			{
				do_serialize(destptr, maxbytes,
							 (*GUC_VARIABLE_BOOL(gconf) ? "true" : "false"));
			}
			break;

		case PGC_INT:
			{
				do_serialize(destptr, maxbytes, "%d",
							 *GUC_VARIABLE_INT(gconf));
			}
			break;

		case PGC_REAL:
			{
				do_serialize(destptr, maxbytes, "%.*e",
							 REALTYPE_PRECISION, *GUC_VARIABLE_REAL(gconf));
			}
			break;

		case PGC_STRING:
			{
				/* NULL becomes empty string, see estimate_variable_size() */
				do_serialize(destptr, maxbytes, "%s",
							 *GUC_VARIABLE_STRING(gconf) ?
							 *GUC_VARIABLE_STRING(gconf) : "");
			}
			break;

		case PGC_ENUM:
			{
				do_serialize(destptr, maxbytes, "%s",
							 config_enum_lookup_by_value(gconf,
														 *GUC_VARIABLE_ENUM(gconf)));
			}
			break;
	}

	do_serialize(destptr, maxbytes, "%s",
				 (GUC_SOURCEFILE(gconf) ? GUC_SOURCEFILE(gconf) : ""));

	if (GUC_SOURCEFILE(gconf) && GUC_SOURCEFILE(gconf)[0])
	{
		int			sourceline = GUC_SOURCELINE(gconf);

		do_serialize_binary(destptr, maxbytes, &sourceline,
							sizeof(sourceline));
	}

	{
		GucSource	source = GUC_SOURCE(gconf);
		GucContext	scontext = GUC_SCONTEXT(gconf);
		Oid			srole = GUC_SROLE(gconf);

		do_serialize_binary(destptr, maxbytes, &source, sizeof(source));
		do_serialize_binary(destptr, maxbytes, &scontext, sizeof(scontext));
		do_serialize_binary(destptr, maxbytes, &srole, sizeof(srole));
	}
}

/*
 * SerializeGUCState:
 * Dumps the complete GUC state onto the memory location at start_address.
 */
void
SerializeGUCState(Size maxsize, char *start_address)
{
	char	   *curptr;
	Size		actual_size;
	Size		bytes_left;
	dlist_iter	iter;

	/* Reserve space for saving the actual size of the guc state */
	Assert(maxsize > sizeof(actual_size));
	curptr = start_address + sizeof(actual_size);
	bytes_left = maxsize - sizeof(actual_size);

	/* We need only consider GUCs with source not PGC_S_DEFAULT */
	dlist_foreach(iter, &guc_nondef_list)
	{
		config_generic_cold_state *cold = dlist_container(config_generic_cold_state,
														  nondef_link, iter.cur);
		struct config_generic *gconf = GUC_COLD_STATE_RECORD(cold);

		serialize_variable(&curptr, &bytes_left, gconf);
	}

	/* Store actual size without assuming alignment of start_address. */
	actual_size = maxsize - bytes_left - sizeof(actual_size);
	memcpy(start_address, &actual_size, sizeof(actual_size));
}

/*
 * read_gucstate:
 * Actually it does not read anything, just returns the srcptr. But it does
 * move the srcptr past the terminating zero byte, so that the caller is ready
 * to read the next string.
 */
static char *
read_gucstate(char **srcptr, char *srcend)
{
	char	   *retptr = *srcptr;
	char	   *ptr;

	if (*srcptr >= srcend)
		elog(ERROR, "incomplete GUC state");

	/* The string variables are all null terminated */
	for (ptr = *srcptr; ptr < srcend && *ptr != '\0'; ptr++)
		;

	if (ptr >= srcend)
		elog(ERROR, "could not find null terminator in GUC state");

	/* Set the new position to the byte following the terminating NUL */
	*srcptr = ptr + 1;

	return retptr;
}

/* Binary read version of read_gucstate(). Copies into dest */
static void
read_gucstate_binary(char **srcptr, char *srcend, void *dest, Size size)
{
	if (*srcptr + size > srcend)
		elog(ERROR, "incomplete GUC state");

	memcpy(dest, *srcptr, size);
	*srcptr += size;
}

/*
 * Callback used to add a context message when reporting errors that occur
 * while trying to restore GUCs in parallel workers.
 */
static void
guc_restore_error_context_callback(void *arg)
{
	char	  **error_context_name_and_value = (char **) arg;

	if (error_context_name_and_value)
		errcontext("while setting parameter \"%s\" to \"%s\"",
				   error_context_name_and_value[0],
				   error_context_name_and_value[1]);
}

/*
 * RestoreGUCState:
 * Reads the GUC state at the specified address and sets this process's
 * GUCs to match.
 *
 * Note that this provides the worker with only a very shallow view of the
 * leader's GUC state: we'll know about the currently active values, but not
 * about stacked or reset values.  That's fine since the worker is just
 * executing one part of a query, within which the active values won't change
 * and the stacked values are invisible.
 */
void
RestoreGUCState(void *gucstate)
{
	char	   *varname,
			   *varvalue,
			   *varsourcefile;
	int			varsourceline;
	GucSource	varsource;
	GucContext	varscontext;
	Oid			varsrole;
	char	   *srcptr = (char *) gucstate;
	char	   *srcend;
	Size		len;
	dlist_mutable_iter iter;
	ErrorContextCallback error_context_callback;

	/*
	 * First, ensure that all potentially-shippable GUCs are reset to their
	 * default values.  We must not touch those GUCs that the leader will
	 * never ship, while there is no need to touch those that are shippable
	 * but already have their default values.  Thus, this ends up being the
	 * same test that SerializeGUCState uses, even though the sets of
	 * variables involved may well be different since the leader's set of
	 * variables-not-at-default-values can differ from the set that are
	 * not-default in this freshly started worker.
	 *
	 * Once we have set all the potentially-shippable GUCs to default values,
	 * restoring the GUCs that the leader sent (because they had non-default
	 * values over there) leads us to exactly the set of GUC values that the
	 * leader has.  This is true even though the worker may have initially
	 * absorbed postgresql.conf settings that the leader hasn't yet seen, or
	 * ALTER USER/DATABASE SET settings that were established after the leader
	 * started.
	 *
	 * Note that ensuring all the potential target GUCs are at PGC_S_DEFAULT
	 * also ensures that set_config_option won't refuse to set them because of
	 * source-priority comparisons.
	 */
	dlist_foreach_modify(iter, &guc_nondef_list)
	{
		config_generic_cold_state *cold = dlist_container(config_generic_cold_state,
														  nondef_link, iter.cur);
		struct config_generic *gconf = GUC_COLD_STATE_RECORD(cold);

		/* Do nothing if non-shippable or if already at PGC_S_DEFAULT. */
		if (can_skip_gucvar(gconf))
			continue;

		/*
		 * We can use InitializeOneGUCOption to reset the GUC to default, but
		 * first we must free any existing subsidiary data to avoid leaking
		 * memory.  The stack must be empty, but we have to clean up all other
		 * fields.  Beware that there might be duplicate value or "extra"
		 * pointers.  We also have to be sure to take it out of any lists it's
		 * in.
		 */
		Assert(GUC_STACK(gconf) == NULL);
		guc_free(GUC_EXTRA(gconf));
		clear_last_reported(gconf);
		guc_free(GUC_SOURCEFILE(gconf));
		switch (gconf->vartype)
		{
			case PGC_BOOL:
			case PGC_INT:
			case PGC_REAL:
			case PGC_ENUM:
				/* no need to do anything */
				break;
			case PGC_STRING:
				{
					guc_free_string_value(gconf,
										  *GUC_VARIABLE_STRING(gconf));
					if (GUC_RESET_STRING(gconf) &&
						GUC_RESET_STRING(gconf) != *GUC_VARIABLE_STRING(gconf))
						guc_free_string_value(gconf,
											  GUC_RESET_STRING(gconf));
					break;
				}
		}
		if (GUC_RESET_EXTRA(gconf) && GUC_RESET_EXTRA(gconf) != GUC_EXTRA(gconf))
			guc_free(GUC_RESET_EXTRA(gconf));
		/* Remove it from any lists it's in. */
		RemoveGUCFromLists(gconf);
		/* Now we can reset the struct to PGS_S_DEFAULT state. */
		InitializeOneGUCOption(gconf);
	}

	/* First item is the length of the subsequent data */
	memcpy(&len, gucstate, sizeof(len));

	srcptr += sizeof(len);
	srcend = srcptr + len;

	/* If the GUC value check fails, we want errors to show useful context. */
	error_context_callback.callback = guc_restore_error_context_callback;
	error_context_callback.previous = error_context_stack;
	error_context_callback.arg = NULL;
	error_context_stack = &error_context_callback;

	/* Restore all the listed GUCs. */
	while (srcptr < srcend)
	{
		int			result;
		char	   *error_context_name_and_value[2];

		varname = read_gucstate(&srcptr, srcend);
		varvalue = read_gucstate(&srcptr, srcend);
		varsourcefile = read_gucstate(&srcptr, srcend);
		if (varsourcefile[0])
			read_gucstate_binary(&srcptr, srcend,
								 &varsourceline, sizeof(varsourceline));
		else
			varsourceline = 0;
		read_gucstate_binary(&srcptr, srcend,
							 &varsource, sizeof(varsource));
		read_gucstate_binary(&srcptr, srcend,
							 &varscontext, sizeof(varscontext));
		read_gucstate_binary(&srcptr, srcend,
							 &varsrole, sizeof(varsrole));

		error_context_name_and_value[0] = varname;
		error_context_name_and_value[1] = varvalue;
		error_context_callback.arg = &error_context_name_and_value[0];
		result = set_config_option_ext(varname, varvalue,
									   varscontext, varsource, varsrole,
									   GUC_ACTION_SET, true, ERROR, true);
		if (result <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("parameter \"%s\" could not be set", varname)));
		if (varsourcefile[0])
			set_config_sourcefile(varname, varsourcefile, varsourceline);
		error_context_callback.arg = NULL;
	}

	error_context_stack = error_context_callback.previous;
}

/*
 * A little "long argument" simulation, although not quite GNU
 * compliant. Takes a string of the form "some-option=some value" and
 * returns name = "some_option" and value = "some value" in palloc'ed
 * storage. Note that '-' is converted to '_' in the option name. If
 * there is no '=' in the input string then value will be NULL.
 */
void
ParseLongOption(const char *string, char **name, char **value)
{
	size_t		equal_pos;

	Assert(string);
	Assert(name);
	Assert(value);

	equal_pos = strcspn(string, "=");

	if (string[equal_pos] == '=')
	{
		*name = palloc(equal_pos + 1);
		strlcpy(*name, string, equal_pos + 1);

		*value = pstrdup(&string[equal_pos + 1]);
	}
	else
	{
		/* no equal sign in string */
		*name = pstrdup(string);
		*value = NULL;
	}

	for (char *cp = *name; *cp; cp++)
		if (*cp == '-')
			*cp = '_';
}


/*
 * Transform array of GUC settings into lists of names and values. The lists
 * are faster to process in cases where the settings must be applied
 * repeatedly (e.g. for each function invocation).
 */
void
TransformGUCArray(ArrayType *array, List **names, List **values)
{
	Assert(array != NULL);
	Assert(ARR_ELEMTYPE(array) == TEXTOID);
	Assert(ARR_NDIM(array) == 1);
	Assert(ARR_LBOUND(array)[0] == 1);

	*names = NIL;
	*values = NIL;
	for (int i = 1; i <= ARR_DIMS(array)[0]; i++)
	{
		Datum		d;
		bool		isnull;
		char	   *s;
		char	   *name;
		char	   *value;

		d = array_ref(array, 1, &i,
					  -1 /* varlenarray */ ,
					  -1 /* TEXT's typlen */ ,
					  false /* TEXT's typbyval */ ,
					  TYPALIGN_INT /* TEXT's typalign */ ,
					  &isnull);

		if (isnull)
			continue;

		s = TextDatumGetCString(d);

		ParseLongOption(s, &name, &value);
		if (!value)
		{
			ereport(WARNING,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("could not parse setting for parameter \"%s\"",
							name)));
			pfree(name);
			continue;
		}

		*names = lappend(*names, name);
		*values = lappend(*values, value);

		pfree(s);
	}
}


/*
 * Handle options fetched from pg_db_role_setting.setconfig,
 * pg_proc.proconfig, etc.  Caller must specify proper context/source/action.
 *
 * The array parameter must be an array of TEXT (it must not be NULL).
 */
void
ProcessGUCArray(ArrayType *array,
				GucContext context, GucSource source, GucAction action)
{
	List	   *gucNames;
	List	   *gucValues;
	ListCell   *lc1;
	ListCell   *lc2;

	TransformGUCArray(array, &gucNames, &gucValues);
	forboth(lc1, gucNames, lc2, gucValues)
	{
		char	   *name = lfirst(lc1);
		char	   *value = lfirst(lc2);

		(void) set_config_option(name, value,
								 context, source,
								 action, true, 0, false);

		pfree(name);
		pfree(value);
	}

	list_free(gucNames);
	list_free(gucValues);
}


/*
 * Add an entry to an option array.  The array parameter may be NULL
 * to indicate the current table entry is NULL.
 */
ArrayType *
GUCArrayAdd(ArrayType *array, const char *name, const char *value)
{
	struct config_generic *record;
	Datum		datum;
	char	   *newval;
	ArrayType  *a;

	Assert(name);
	Assert(value);

	/* test if the option is valid and we're allowed to set it */
	(void) validate_option_array_item(name, value, false);

	/* normalize name (converts obsolete GUC names to modern spellings) */
	record = find_option(name, false, true, WARNING);
	if (record)
		name = record->name;

	/* build new item for array */
	newval = psprintf("%s=%s", name, value);
	datum = CStringGetTextDatum(newval);

	if (array)
	{
		int			index;
		bool		isnull;

		Assert(ARR_ELEMTYPE(array) == TEXTOID);
		Assert(ARR_NDIM(array) == 1);
		Assert(ARR_LBOUND(array)[0] == 1);

		index = ARR_DIMS(array)[0] + 1; /* add after end */

		for (int i = 1; i <= ARR_DIMS(array)[0]; i++)
		{
			Datum		d;
			char	   *current;

			d = array_ref(array, 1, &i,
						  -1 /* varlenarray */ ,
						  -1 /* TEXT's typlen */ ,
						  false /* TEXT's typbyval */ ,
						  TYPALIGN_INT /* TEXT's typalign */ ,
						  &isnull);
			if (isnull)
				continue;
			current = TextDatumGetCString(d);

			/* check for match up through and including '=' */
			if (strncmp(current, newval, strlen(name) + 1) == 0)
			{
				index = i;
				break;
			}
		}

		a = array_set(array, 1, &index,
					  datum,
					  false,
					  -1 /* varlena array */ ,
					  -1 /* TEXT's typlen */ ,
					  false /* TEXT's typbyval */ ,
					  TYPALIGN_INT /* TEXT's typalign */ );
	}
	else
		a = construct_array_builtin(&datum, 1, TEXTOID);

	return a;
}


/*
 * Delete an entry from an option array.  The array parameter may be NULL
 * to indicate the current table entry is NULL.  Also, if the return value
 * is NULL then a null should be stored.
 */
ArrayType *
GUCArrayDelete(ArrayType *array, const char *name)
{
	struct config_generic *record;
	ArrayType  *newarray;
	int			index;

	Assert(name);

	/* test if the option is valid and we're allowed to set it */
	(void) validate_option_array_item(name, NULL, false);

	/* normalize name (converts obsolete GUC names to modern spellings) */
	record = find_option(name, false, true, WARNING);
	if (record)
		name = record->name;

	/* if array is currently null, then surely nothing to delete */
	if (!array)
		return NULL;

	newarray = NULL;
	index = 1;

	for (int i = 1; i <= ARR_DIMS(array)[0]; i++)
	{
		Datum		d;
		char	   *val;
		bool		isnull;

		d = array_ref(array, 1, &i,
					  -1 /* varlenarray */ ,
					  -1 /* TEXT's typlen */ ,
					  false /* TEXT's typbyval */ ,
					  TYPALIGN_INT /* TEXT's typalign */ ,
					  &isnull);
		if (isnull)
			continue;
		val = TextDatumGetCString(d);

		/* ignore entry if it's what we want to delete */
		if (strncmp(val, name, strlen(name)) == 0
			&& val[strlen(name)] == '=')
			continue;

		/* else add it to the output array */
		if (newarray)
			newarray = array_set(newarray, 1, &index,
								 d,
								 false,
								 -1 /* varlenarray */ ,
								 -1 /* TEXT's typlen */ ,
								 false /* TEXT's typbyval */ ,
								 TYPALIGN_INT /* TEXT's typalign */ );
		else
			newarray = construct_array_builtin(&d, 1, TEXTOID);

		index++;
	}

	return newarray;
}


/*
 * Given a GUC array, delete all settings from it that our permission
 * level allows: if superuser, delete them all; if regular user, only
 * those that are PGC_USERSET or we have permission to set
 */
ArrayType *
GUCArrayReset(ArrayType *array)
{
	ArrayType  *newarray;
	int			index;

	/* if array is currently null, nothing to do */
	if (!array)
		return NULL;

	/* if we're superuser, we can delete everything, so just do it */
	if (superuser())
		return NULL;

	newarray = NULL;
	index = 1;

	for (int i = 1; i <= ARR_DIMS(array)[0]; i++)
	{
		Datum		d;
		char	   *val;
		char	   *eqsgn;
		bool		isnull;

		d = array_ref(array, 1, &i,
					  -1 /* varlenarray */ ,
					  -1 /* TEXT's typlen */ ,
					  false /* TEXT's typbyval */ ,
					  TYPALIGN_INT /* TEXT's typalign */ ,
					  &isnull);
		if (isnull)
			continue;
		val = TextDatumGetCString(d);

		eqsgn = strchr(val, '=');
		*eqsgn = '\0';

		/* skip if we have permission to delete it */
		if (validate_option_array_item(val, NULL, true))
			continue;

		/* else add it to the output array */
		if (newarray)
			newarray = array_set(newarray, 1, &index,
								 d,
								 false,
								 -1 /* varlenarray */ ,
								 -1 /* TEXT's typlen */ ,
								 false /* TEXT's typbyval */ ,
								 TYPALIGN_INT /* TEXT's typalign */ );
		else
			newarray = construct_array_builtin(&d, 1, TEXTOID);

		index++;
		pfree(val);
	}

	return newarray;
}

/*
 * Validate a proposed option setting for GUCArrayAdd/Delete/Reset.
 *
 * name is the option name.  value is the proposed value for the Add case,
 * or NULL for the Delete/Reset cases.  If skipIfNoPermissions is true, it's
 * not an error to have no permissions to set the option.
 *
 * Returns true if OK, false if skipIfNoPermissions is true and user does not
 * have permission to change this option (all other error cases result in an
 * error being thrown).
 */
static bool
validate_option_array_item(const char *name, const char *value,
						   bool skipIfNoPermissions)

{
	struct config_generic *gconf;
	bool		reset_custom;

	/*
	 * There are three cases to consider:
	 *
	 * name is a known GUC variable.  Check the value normally, check
	 * permissions normally (i.e., allow if variable is USERSET, or if it's
	 * SUSET and user is superuser or holds ACL_SET permissions).
	 *
	 * name is not known, but exists or can be created as a placeholder (i.e.,
	 * it has a valid custom name).  We allow this case if you're a superuser,
	 * otherwise not.  Superusers are assumed to know what they're doing. We
	 * can't allow it for other users, because when the placeholder is
	 * resolved it might turn out to be a SUSET variable.  (With currently
	 * available infrastructure, we can actually handle such cases within the
	 * current session --- but once an entry is made in pg_db_role_setting,
	 * it's assumed to be fully validated.)
	 *
	 * name is not known and can't be created as a placeholder.  Throw error,
	 * unless skipIfNoPermissions or reset_custom is true.  If reset_custom is
	 * true, this is a RESET or RESET ALL operation for an unknown custom GUC
	 * with a reserved prefix, in which case we want to fall through to the
	 * placeholder case described in the preceding paragraph (else there'd be
	 * no way for users to remove them).  Otherwise, return false.
	 */
	reset_custom = (!value && valid_custom_variable_name(name));
	gconf = find_option(name, true, skipIfNoPermissions || reset_custom, ERROR);
	if (!gconf && !reset_custom)
	{
		/* not known, failed to make a placeholder */
		return false;
	}

	if (!gconf || gconf->flags & GUC_CUSTOM_PLACEHOLDER)
	{
		/*
		 * We cannot do any meaningful check on the value, so only permissions
		 * are useful to check.
		 */
		if (superuser() ||
			pg_parameter_aclcheck(name, GetUserId(), ACL_SET) == ACLCHECK_OK)
			return true;
		if (skipIfNoPermissions)
			return false;
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to set parameter \"%s\"", name)));
	}

	/* manual permissions check so we can avoid an error being thrown */
	if (gconf->context == PGC_USERSET)
		 /* ok */ ;
	else if (gconf->context == PGC_SUSET &&
			 (superuser() ||
			  pg_parameter_aclcheck(name, GetUserId(), ACL_SET) == ACLCHECK_OK))
		 /* ok */ ;
	else if (skipIfNoPermissions)
		return false;
	/* if a permissions error should be thrown, let set_config_option do it */

	/* test for permissions and valid option value */
	(void) set_config_option(name, value,
							 superuser() ? PGC_SUSET : PGC_USERSET,
							 PGC_S_TEST, GUC_ACTION_SET, false, 0, false);

	return true;
}


/*
 * Called by check_hooks that want to override the normal
 * ERRCODE_INVALID_PARAMETER_VALUE SQLSTATE for check hook failures.
 *
 * Note that GUC_check_errmsg() etc are just macros that result in a direct
 * assignment to the associated variables.  That is ugly, but forced by the
 * limitations of C's macro mechanisms.
 */
void
GUC_check_errcode(int sqlerrcode)
{
	GUC_check_errcode_value = sqlerrcode;
}


/*
 * Convenience functions to manage calling a variable's check_hook.
 * These mostly take care of the protocol for letting check hooks supply
 * portions of the error report on failure.
 */

static bool
call_bool_check_hook(const struct config_generic *conf, bool *newval, void **extra,
					 GucSource source, int elevel)
{
	/* Quick success if no hook */
	if (!conf->_bool.check_hook)
		return true;

	/* Reset variables that might be set by hook */
	GUC_check_errcode_value = ERRCODE_INVALID_PARAMETER_VALUE;
	GUC_check_errmsg_string = NULL;
	GUC_check_errdetail_string = NULL;
	GUC_check_errhint_string = NULL;

	if (!conf->_bool.check_hook(newval, extra, source))
	{
		ereport(elevel,
				(errcode(GUC_check_errcode_value),
				 GUC_check_errmsg_string ?
				 errmsg_internal("%s", GUC_check_errmsg_string) :
				 errmsg("invalid value for parameter \"%s\": %d",
						conf->name, (int) *newval),
				 GUC_check_errdetail_string ?
				 errdetail_internal("%s", GUC_check_errdetail_string) : 0,
				 GUC_check_errhint_string ?
				 errhint("%s", GUC_check_errhint_string) : 0));
		/* Flush strings created in ErrorContext (ereport might not have) */
		FlushErrorState();
		return false;
	}

	return true;
}

static bool
call_int_check_hook(const struct config_generic *conf, int *newval, void **extra,
					GucSource source, int elevel)
{
	/* Quick success if no hook */
	if (!conf->_int.check_hook)
		return true;

	/* Reset variables that might be set by hook */
	GUC_check_errcode_value = ERRCODE_INVALID_PARAMETER_VALUE;
	GUC_check_errmsg_string = NULL;
	GUC_check_errdetail_string = NULL;
	GUC_check_errhint_string = NULL;

	if (!conf->_int.check_hook(newval, extra, source))
	{
		ereport(elevel,
				(errcode(GUC_check_errcode_value),
				 GUC_check_errmsg_string ?
				 errmsg_internal("%s", GUC_check_errmsg_string) :
				 errmsg("invalid value for parameter \"%s\": %d",
						conf->name, *newval),
				 GUC_check_errdetail_string ?
				 errdetail_internal("%s", GUC_check_errdetail_string) : 0,
				 GUC_check_errhint_string ?
				 errhint("%s", GUC_check_errhint_string) : 0));
		/* Flush strings created in ErrorContext (ereport might not have) */
		FlushErrorState();
		return false;
	}

	return true;
}

static bool
call_real_check_hook(const struct config_generic *conf, double *newval, void **extra,
					 GucSource source, int elevel)
{
	/* Quick success if no hook */
	if (!conf->_real.check_hook)
		return true;

	/* Reset variables that might be set by hook */
	GUC_check_errcode_value = ERRCODE_INVALID_PARAMETER_VALUE;
	GUC_check_errmsg_string = NULL;
	GUC_check_errdetail_string = NULL;
	GUC_check_errhint_string = NULL;

	if (!conf->_real.check_hook(newval, extra, source))
	{
		ereport(elevel,
				(errcode(GUC_check_errcode_value),
				 GUC_check_errmsg_string ?
				 errmsg_internal("%s", GUC_check_errmsg_string) :
				 errmsg("invalid value for parameter \"%s\": %g",
						conf->name, *newval),
				 GUC_check_errdetail_string ?
				 errdetail_internal("%s", GUC_check_errdetail_string) : 0,
				 GUC_check_errhint_string ?
				 errhint("%s", GUC_check_errhint_string) : 0));
		/* Flush strings created in ErrorContext (ereport might not have) */
		FlushErrorState();
		return false;
	}

	return true;
}

static bool
call_string_check_hook(const struct config_generic *conf, char **newval, void **extra,
					   GucSource source, int elevel)
{
	volatile bool result = true;

	/* Quick success if no hook */
	if (!conf->_string.check_hook)
		return true;

	/*
	 * If elevel is ERROR, or if the check_hook itself throws an elog
	 * (undesirable, but not always avoidable), make sure we don't leak the
	 * already-malloc'd newval string.
	 */
	PG_TRY();
	{
		/* Reset variables that might be set by hook */
		GUC_check_errcode_value = ERRCODE_INVALID_PARAMETER_VALUE;
		GUC_check_errmsg_string = NULL;
		GUC_check_errdetail_string = NULL;
		GUC_check_errhint_string = NULL;

		if (!conf->_string.check_hook(newval, extra, source))
		{
			ereport(elevel,
					(errcode(GUC_check_errcode_value),
					 GUC_check_errmsg_string ?
					 errmsg_internal("%s", GUC_check_errmsg_string) :
					 errmsg("invalid value for parameter \"%s\": \"%s\"",
							conf->name, *newval ? *newval : ""),
					 GUC_check_errdetail_string ?
					 errdetail_internal("%s", GUC_check_errdetail_string) : 0,
					 GUC_check_errhint_string ?
					 errhint("%s", GUC_check_errhint_string) : 0));
			/* Flush strings created in ErrorContext (ereport might not have) */
			FlushErrorState();
			result = false;
		}
	}
	PG_CATCH();
	{
		guc_free(*newval);
		PG_RE_THROW();
	}
	PG_END_TRY();

	return result;
}

static bool
call_enum_check_hook(const struct config_generic *conf, int *newval, void **extra,
					 GucSource source, int elevel)
{
	/* Quick success if no hook */
	if (!conf->_enum.check_hook)
		return true;

	/* Reset variables that might be set by hook */
	GUC_check_errcode_value = ERRCODE_INVALID_PARAMETER_VALUE;
	GUC_check_errmsg_string = NULL;
	GUC_check_errdetail_string = NULL;
	GUC_check_errhint_string = NULL;

	if (!conf->_enum.check_hook(newval, extra, source))
	{
		ereport(elevel,
				(errcode(GUC_check_errcode_value),
				 GUC_check_errmsg_string ?
				 errmsg_internal("%s", GUC_check_errmsg_string) :
				 errmsg("invalid value for parameter \"%s\": \"%s\"",
						conf->name,
						config_enum_lookup_by_value(conf, *newval)),
				 GUC_check_errdetail_string ?
				 errdetail_internal("%s", GUC_check_errdetail_string) : 0,
				 GUC_check_errhint_string ?
				 errhint("%s", GUC_check_errhint_string) : 0));
		/* Flush strings created in ErrorContext (ereport might not have) */
		FlushErrorState();
		return false;
	}

	return true;
}
