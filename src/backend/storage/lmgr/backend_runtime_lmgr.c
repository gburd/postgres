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
#include "postgres.h"

#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "../../utils/init/backend_runtime_internal.h"

void **
PgCurrentFastPathLocalUseCountsRef(void)
{
	return &PgCurrentBackendLockState()->fast_path_local_use_counts;
}

bool *
PgCurrentFastPathLocalUseCountsOwnedRef(void)
{
	return &PgCurrentBackendLockState()->fast_path_local_use_counts_owned;
}

PgBackendLWLockHandle *
PgCurrentHeldLWLocks(void)
{
	return PgCurrentBackendLockState()->held_lwlocks;
}

int *
PgCurrentNumHeldLWLocksRef(void)
{
	return &PgCurrentBackendLockState()->num_held_lwlocks;
}

HTAB **
PgCurrentLWLockStatsHashRef(void)
{
	return &PgCurrentBackendLockState()->lwlock_stats_htab;
}

PgBackendLWLockStats *
PgCurrentLWLockStatsDummy(void)
{
	return &PgCurrentBackendLockState()->lwlock_stats_dummy;
}

MemoryContext *
PgCurrentLWLockStatsContextRef(void)
{
	return &PgCurrentBackendLockState()->lwlock_stats_context;
}

bool *
PgCurrentLWLockStatsExitRegisteredRef(void)
{
	return &PgCurrentBackendLockState()->lwlock_stats_exit_registered;
}

int *
PgCurrentLocalNumUserDefinedLWLockTranchesRef(void)
{
	return &PgCurrentBackendLockState()->local_num_user_defined_lwlock_tranches;
}

bool *
PgCurrentRelationExtensionLockHeldRef(void)
{
	return &PgCurrentBackendLockState()->relation_extension_lock_held;
}

HTAB **
PgCurrentLockMethodLocalHashRef(void)
{
	return &PgCurrentBackendLockState()->lock_method_local_hash;
}

void **
PgCurrentStrongLockInProgressRef(void)
{
	return &PgCurrentBackendLockState()->strong_lock_in_progress;
}

void **
PgCurrentAwaitedLockRef(void)
{
	return &PgCurrentBackendLockState()->awaited_lock;
}

void **
PgCurrentAwaitedOwnerRef(void)
{
	return &PgCurrentBackendLockState()->awaited_owner;
}

volatile sig_atomic_t *
PgCurrentDeadlockTimeoutPendingRef(void)
{
	return &PgCurrentBackendLockState()->deadlock_timeout_pending;
}

void **
PgCurrentConditionVariableSleepTargetRef(void)
{
	return &PgCurrentBackendLockState()->condition_variable_sleep_target;
}

uint32 *
PgCurrentSpeculativeInsertionTokenRef(void)
{
	return &PgCurrentBackendLockState()->speculative_insertion_token;
}

void **
PgCurrentDeadlockVisitedProcsRef(void)
{
	return &PgCurrentBackendLockState()->deadlock_visited_procs;
}

int *
PgCurrentDeadlockNVisitedProcsRef(void)
{
	return &PgCurrentBackendLockState()->deadlock_n_visited_procs;
}

void **
PgCurrentDeadlockTopoProcsRef(void)
{
	return &PgCurrentBackendLockState()->deadlock_topo_procs;
}

void **
PgCurrentDeadlockBeforeConstraintsRef(void)
{
	return &PgCurrentBackendLockState()->deadlock_before_constraints;
}

void **
PgCurrentDeadlockAfterConstraintsRef(void)
{
	return &PgCurrentBackendLockState()->deadlock_after_constraints;
}

void **
PgCurrentDeadlockWaitOrdersRef(void)
{
	return &PgCurrentBackendLockState()->deadlock_wait_orders;
}

int *
PgCurrentDeadlockNWaitOrdersRef(void)
{
	return &PgCurrentBackendLockState()->deadlock_n_wait_orders;
}

void **
PgCurrentDeadlockWaitOrderProcsRef(void)
{
	return &PgCurrentBackendLockState()->deadlock_wait_order_procs;
}

void **
PgCurrentDeadlockCurConstraintsRef(void)
{
	return &PgCurrentBackendLockState()->deadlock_cur_constraints;
}

int *
PgCurrentDeadlockNCurConstraintsRef(void)
{
	return &PgCurrentBackendLockState()->deadlock_n_cur_constraints;
}

int *
PgCurrentDeadlockMaxCurConstraintsRef(void)
{
	return &PgCurrentBackendLockState()->deadlock_max_cur_constraints;
}

void **
PgCurrentDeadlockPossibleConstraintsRef(void)
{
	return &PgCurrentBackendLockState()->deadlock_possible_constraints;
}

int *
PgCurrentDeadlockNPossibleConstraintsRef(void)
{
	return &PgCurrentBackendLockState()->deadlock_n_possible_constraints;
}

int *
PgCurrentDeadlockMaxPossibleConstraintsRef(void)
{
	return &PgCurrentBackendLockState()->deadlock_max_possible_constraints;
}

void **
PgCurrentDeadlockDetailsRef(void)
{
	return &PgCurrentBackendLockState()->deadlock_details;
}

int *
PgCurrentDeadlockNDetailsRef(void)
{
	return &PgCurrentBackendLockState()->deadlock_n_details;
}

bool *
PgCurrentDeadlockWorkspaceOwnedRef(void)
{
	return &PgCurrentBackendLockState()->deadlock_workspace_owned;
}

void **
PgCurrentBlockingAutovacuumProcRef(void)
{
	return &PgCurrentBackendLockState()->blocking_autovacuum_proc;
}

HTAB **
PgCurrentLocalPredicateLockHashRef(void)
{
	return &PgCurrentBackendLockState()->local_predicate_lock_hash;
}

void **
PgCurrentMySerializableXactRef(void)
{
	return &PgCurrentBackendLockState()->my_serializable_xact;
}

bool *
PgCurrentMyXactDidWriteRef(void)
{
	return &PgCurrentBackendLockState()->my_xact_did_write;
}

void **
PgCurrentSavedSerializableXactRef(void)
{
	return &PgCurrentBackendLockState()->saved_serializable_xact;
}
