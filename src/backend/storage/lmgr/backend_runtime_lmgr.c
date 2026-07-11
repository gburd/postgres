/*-------------------------------------------------------------------------
 *
 * backend_runtime_lmgr.c
 *	  Runtime bridge accessors for backend-local lock-manager state.
 *
 * These accessors keep lock-manager compatibility globals mapped onto the
 * current PgBackend while leaving runtime construction and early fallback
 * ownership in utils/init/backend_runtime.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/storage/lmgr/backend_runtime_lmgr.c
 *
 *-------------------------------------------------------------------------
 */
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "../../utils/init/backend_runtime_internal.h"

static inline PgBackendLockState *
PgCurrentBackendLockStateFast(void)
{
	PgBackendLockState *locks;

	if (likely(CurrentPgBackendLockRuntimeState != NULL))
		locks = CurrentPgBackendLockRuntimeState;
	else
		locks = PgCurrentBackendLockState();

	if (unlikely(locks->held_lwlocks_array == NULL ||
				 locks->held_lwlocks_capacity <= 0))
	{
		locks->held_lwlocks_array = locks->held_lwlocks_inline;
		locks->held_lwlocks_capacity = PG_BACKEND_MAX_INLINE_LWLOCKS;
	}

	return locks;
}

int *
PgCurrentDeadlockTimeoutRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLockWaitRuntimeState, PgCurrentSessionLockWaitState)->deadlock_timeout_ms;
}

int *
PgCurrentStatementTimeoutRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLockWaitRuntimeState, PgCurrentSessionLockWaitState)->statement_timeout_ms;
}

int *
PgCurrentLockTimeoutRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLockWaitRuntimeState, PgCurrentSessionLockWaitState)->lock_timeout_ms;
}

int *
PgCurrentIdleInTransactionSessionTimeoutRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLockWaitRuntimeState, PgCurrentSessionLockWaitState)->idle_in_transaction_session_timeout_ms;
}

int *
PgCurrentTransactionTimeoutRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLockWaitRuntimeState, PgCurrentSessionLockWaitState)->transaction_timeout_ms;
}

int *
PgCurrentIdleSessionTimeoutRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLockWaitRuntimeState, PgCurrentSessionLockWaitState)->idle_session_timeout_ms;
}

bool *
PgCurrentLogLockWaitsRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLockWaitRuntimeState, PgCurrentSessionLockWaitState)->log_lock_waits_value;
}

bool *
PgCurrentLogLockFailuresRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLockWaitRuntimeState, PgCurrentSessionLockWaitState)->log_lock_failures_value;
}

int *
PgCurrentTraceLockOidMinRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLockWaitRuntimeState, PgCurrentSessionLockWaitState)->trace_lock_oidmin_value;
}

bool *
PgCurrentTraceLocksRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLockWaitRuntimeState, PgCurrentSessionLockWaitState)->trace_locks_value;
}

bool *
PgCurrentTraceUserlocksRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLockWaitRuntimeState, PgCurrentSessionLockWaitState)->trace_userlocks_value;
}

int *
PgCurrentTraceLockTableRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLockWaitRuntimeState, PgCurrentSessionLockWaitState)->trace_lock_table_value;
}

bool *
PgCurrentDebugDeadlocksRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLockWaitRuntimeState, PgCurrentSessionLockWaitState)->debug_deadlocks_value;
}

bool *
PgCurrentTraceLwlocksRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLockWaitRuntimeState, PgCurrentSessionLockWaitState)->trace_lwlocks_value;
}

void **
PgCurrentFastPathLocalUseCountsRef(void)
{
	return &PgCurrentBackendLockStateFast()->fast_path_local_use_counts;
}

bool *
PgCurrentFastPathLocalUseCountsOwnedRef(void)
{
	return &PgCurrentBackendLockStateFast()->fast_path_local_use_counts_owned;
}

PgBackendLWLockHandle *
PgCurrentHeldLWLocks(void)
{
	return PgCurrentBackendLockStateFast()->held_lwlocks_array;
}

int *
PgCurrentNumHeldLWLocksRef(void)
{
	return &PgCurrentBackendLockStateFast()->num_held_lwlocks;
}

HTAB **
PgCurrentLWLockStatsHashRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendLockRuntimeState, PgCurrentBackendLockState)->lwlock_stats_htab;
}

PgBackendLWLockStats *
PgCurrentLWLockStatsDummy(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendLockRuntimeState, PgCurrentBackendLockState)->lwlock_stats_dummy;
}

MemoryContext *
PgCurrentLWLockStatsContextRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendLockRuntimeState, PgCurrentBackendLockState)->lwlock_stats_context;
}

bool *
PgCurrentLWLockStatsExitRegisteredRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendLockRuntimeState, PgCurrentBackendLockState)->lwlock_stats_exit_registered;
}

int *
PgCurrentLocalNumUserDefinedLWLockTranchesRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendLockRuntimeState, PgCurrentBackendLockState)->local_num_user_defined_lwlock_tranches;
}

bool *
PgCurrentRelationExtensionLockHeldRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendLockRuntimeState, PgCurrentBackendLockState)->relation_extension_lock_held;
}

HTAB **
PgCurrentLockMethodLocalHashRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendLockRuntimeState, PgCurrentBackendLockState)->lock_method_local_hash;
}

void **
PgCurrentStrongLockInProgressRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendLockRuntimeState, PgCurrentBackendLockState)->strong_lock_in_progress;
}

void **
PgCurrentAwaitedLockRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendLockRuntimeState, PgCurrentBackendLockState)->awaited_lock;
}

void **
PgCurrentAwaitedOwnerRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendLockRuntimeState, PgCurrentBackendLockState)->awaited_owner;
}

volatile sig_atomic_t *
PgCurrentDeadlockTimeoutPendingRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendLockRuntimeState, PgCurrentBackendLockState)->deadlock_timeout_pending;
}

void **
PgCurrentConditionVariableSleepTargetRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendLockRuntimeState, PgCurrentBackendLockState)->condition_variable_sleep_target;
}

uint32 *
PgCurrentSpeculativeInsertionTokenRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendLockRuntimeState, PgCurrentBackendLockState)->speculative_insertion_token;
}

void **
PgCurrentDeadlockVisitedProcsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendLockRuntimeState, PgCurrentBackendLockState)->deadlock_visited_procs;
}

int *
PgCurrentDeadlockNVisitedProcsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendLockRuntimeState, PgCurrentBackendLockState)->deadlock_n_visited_procs;
}

void **
PgCurrentDeadlockTopoProcsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendLockRuntimeState, PgCurrentBackendLockState)->deadlock_topo_procs;
}

void **
PgCurrentDeadlockBeforeConstraintsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendLockRuntimeState, PgCurrentBackendLockState)->deadlock_before_constraints;
}

void **
PgCurrentDeadlockAfterConstraintsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendLockRuntimeState, PgCurrentBackendLockState)->deadlock_after_constraints;
}

void **
PgCurrentDeadlockWaitOrdersRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendLockRuntimeState, PgCurrentBackendLockState)->deadlock_wait_orders;
}

int *
PgCurrentDeadlockNWaitOrdersRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendLockRuntimeState, PgCurrentBackendLockState)->deadlock_n_wait_orders;
}

void **
PgCurrentDeadlockWaitOrderProcsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendLockRuntimeState, PgCurrentBackendLockState)->deadlock_wait_order_procs;
}

void **
PgCurrentDeadlockCurConstraintsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendLockRuntimeState, PgCurrentBackendLockState)->deadlock_cur_constraints;
}

int *
PgCurrentDeadlockNCurConstraintsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendLockRuntimeState, PgCurrentBackendLockState)->deadlock_n_cur_constraints;
}

int *
PgCurrentDeadlockMaxCurConstraintsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendLockRuntimeState, PgCurrentBackendLockState)->deadlock_max_cur_constraints;
}

void **
PgCurrentDeadlockPossibleConstraintsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendLockRuntimeState, PgCurrentBackendLockState)->deadlock_possible_constraints;
}

int *
PgCurrentDeadlockNPossibleConstraintsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendLockRuntimeState, PgCurrentBackendLockState)->deadlock_n_possible_constraints;
}

int *
PgCurrentDeadlockMaxPossibleConstraintsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendLockRuntimeState, PgCurrentBackendLockState)->deadlock_max_possible_constraints;
}

void **
PgCurrentDeadlockDetailsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendLockRuntimeState, PgCurrentBackendLockState)->deadlock_details;
}

int *
PgCurrentDeadlockNDetailsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendLockRuntimeState, PgCurrentBackendLockState)->deadlock_n_details;
}

bool *
PgCurrentDeadlockWorkspaceOwnedRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendLockRuntimeState, PgCurrentBackendLockState)->deadlock_workspace_owned;
}

void **
PgCurrentBlockingAutovacuumProcRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendLockRuntimeState, PgCurrentBackendLockState)->blocking_autovacuum_proc;
}

HTAB **
PgCurrentLocalPredicateLockHashRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendLockRuntimeState, PgCurrentBackendLockState)->local_predicate_lock_hash;
}

void **
PgCurrentMySerializableXactRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendLockRuntimeState, PgCurrentBackendLockState)->my_serializable_xact;
}

bool *
PgCurrentMyXactDidWriteRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendLockRuntimeState, PgCurrentBackendLockState)->my_xact_did_write;
}

void **
PgCurrentSavedSerializableXactRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendLockRuntimeState, PgCurrentBackendLockState)->saved_serializable_xact;
}
