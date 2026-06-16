/*-------------------------------------------------------------------------
 *
 * backend_runtime_internal.h
 *	  Internal declarations shared by fork-owned runtime support files.
 *
 * This header is deliberately backend-private.  Public compatibility
 * accessors remain declared in utils/backend_runtime.h; this file only exposes
 * small current-bucket helpers so subsystem-owned source files can host their
 * own runtime bridge code without growing backend_runtime.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/utils/init/backend_runtime_internal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BACKEND_RUNTIME_INTERNAL_H
#define BACKEND_RUNTIME_INTERNAL_H

#include "utils/backend_runtime.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

/*
 * Lifecycle action vocabulary used by backend_runtime_*_buckets.def and
 * owner-adjacent runtime teardown files.
 *
 * Keep this vocabulary small and mechanically checked.  Routine no-op cells
 * should be named here rather than written as anonymous C expressions in the
 * bucket definition files, so the lifecycle checker can distinguish explicit
 * intent from forgotten lifecycle work.
 */
#define PG_RUNTIME_NOOP ((void) 0)
#define PG_RUNTIME_DELETE_MEMORY_CONTEXT(context) \
	PgRuntimeDeleteOwnedMemoryContext(&(context))
#define PG_RUNTIME_RESET_THROUGH_INITIALIZER(init_expr) \
	do { \
		init_expr; \
	} while (0)
#define PG_RUNTIME_DELETE_MEMORY_CONTEXT_AND_RESET(context, init_expr) \
	do { \
		PG_RUNTIME_DELETE_MEMORY_CONTEXT(context); \
		PG_RUNTIME_RESET_THROUGH_INITIALIZER(init_expr); \
	} while (0)
#define PG_RUNTIME_DESTROY_HASH(hash) \
	do { \
		if ((hash) != NULL) \
		{ \
			hash_destroy(hash); \
			(hash) = NULL; \
		} \
	} while (0)
#define PG_RUNTIME_LIST_FREE(list_head) \
	do { \
		list_free(list_head); \
		(list_head) = NIL; \
	} while (0)
#define PG_RUNTIME_LIST_FREE_DEEP(list_head) \
	do { \
		list_free_deep(list_head); \
		(list_head) = NIL; \
	} while (0)

/*
 * Routine lifecycle helper definitions.  Use these for plain whole-bucket
 * initialization/adoption patterns; keep ownership-sensitive pointer fixups
 * and ordered cleanup handwritten near the owning subsystem.
 */
#define PG_RUNTIME_DEFINE_ZERO_INIT(function_name, state_type, state_arg) \
void \
function_name(state_type *state_arg) \
{ \
	Assert(state_arg != NULL); \
	MemSet(state_arg, 0, sizeof(*state_arg)); \
}

#define PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(function_name, object_type, object_arg, field_name, early_state, init_function) \
static void \
function_name(object_type *object_arg) \
{ \
	Assert(object_arg != NULL); \
	object_arg->field_name = early_state; \
	init_function(&early_state); \
}

#define PG_RUNTIME_DEFINE_ADOPT_EARLY_ZERO(function_name, object_type, object_arg, field_name, early_state) \
static void \
function_name(object_type *object_arg) \
{ \
	Assert(object_arg != NULL); \
	object_arg->field_name = early_state; \
	MemSet(&early_state, 0, sizeof(early_state)); \
}

#define PG_RUNTIME_DEFINE_ADOPT_EARLY_INITIALIZED(function_name, object_type, object_arg, field_name, early_state, init_function) \
static void \
function_name(object_type *object_arg) \
{ \
	Assert(object_arg != NULL); \
	if (!early_state.initialized) \
		init_function(&early_state); \
	object_arg->field_name = early_state; \
	init_function(&early_state); \
}

#define PG_RUNTIME_DEFINE_ADOPT_EARLY_INITIALIZED_WITH_RESET(function_name, object_type, object_arg, field_name, early_state, init_function, reset_function) \
static void \
function_name(object_type *object_arg) \
{ \
	Assert(object_arg != NULL); \
	if (!early_state.initialized) \
		init_function(&early_state); \
	object_arg->field_name = early_state; \
	reset_function(&early_state); \
}

extern PgCarrier *PgCurrentCarrierState(void);
extern PgRuntimeServerGUCState *PgCurrentRuntimeServerGUCState(void);
extern PgSessionTcopState *PgCurrentSessionTcopState(void);
extern PgSessionDatabaseState *PgCurrentSessionDatabaseState(void);
extern PgSessionTablespaceState *PgCurrentSessionTablespaceState(void);
extern PgSessionBinaryUpgradeState *PgCurrentSessionBinaryUpgradeState(void);
extern PgSessionCatalogLookupState *PgCurrentSessionCatalogLookupState(void);
extern PgSessionTextSearchState *PgCurrentSessionTextSearchState(void);
extern PgSessionConnectionGUCState *PgCurrentSessionConnectionGUCState(void);
extern PgSessionDateTimeState *PgCurrentSessionDateTimeState(void);
extern PgSessionEncodingState *PgCurrentSessionEncodingState(void);
extern PgSessionTempFileState *PgCurrentSessionTempFileState(void);
extern PgSessionParserState *PgCurrentSessionParserState(void);
extern PgSessionVacuumState *PgCurrentSessionVacuumState(void);
extern PgSessionBufferIOState *PgCurrentSessionBufferIOState(void);
extern PgSessionXactDefaultState *PgCurrentSessionXactDefaultState(void);
extern PgSessionLockWaitState *PgCurrentSessionLockWaitState(void);
extern PgSessionNamespaceState *PgCurrentSessionNamespaceState(void);
extern PgSessionLocaleState *PgCurrentSessionLocaleState(void);
extern PgSessionExtensionModuleState *PgCurrentSessionExtensionModuleState(void);
extern PgSessionInvalidationCallbackState *PgCurrentSessionInvalidationCallbackState(void);
extern PgSessionRIGlobalsState *PgCurrentSessionRIGlobalsState(void);
extern PgSessionRelMapState *PgCurrentSessionRelMapState(void);
extern PgSessionPreparedStatementState *PgCurrentSessionPreparedStatementState(void);
extern PgSessionPlanCacheState *PgCurrentSessionPlanCacheState(void);
extern PgSessionOnCommitState *PgCurrentSessionOnCommitState(void);
extern PgSessionSequenceState *PgCurrentSessionSequenceState(void);
extern PgSessionXactCallbackState *PgCurrentSessionXactCallbackState(void);
extern PgSessionBackupState *PgCurrentSessionBackupState(void);
extern PgSessionRegexState *PgCurrentSessionRegexState(void);
extern PgSessionPortalManagerState *PgCurrentSessionPortalManagerState(void);
extern PgSessionLargeObjectState *PgCurrentSessionLargeObjectState(void);
extern PgSessionFunctionManagerState *PgCurrentSessionFunctionManagerState(void);
extern PgSessionGeneralGUCState *PgCurrentSessionGeneralGUCState(void);
extern PgSessionQueryIdState *PgCurrentSessionQueryIdState(void);
extern PgSessionStorageGUCState *PgCurrentSessionStorageGUCState(void);
extern PgSessionUserGUCState *PgCurrentSessionUserGUCState(void);
extern PgSessionCommandGUCState *PgCurrentSessionCommandGUCState(void);
extern PgSessionReplicationGUCState *PgCurrentSessionReplicationGUCState(void);
extern PgSessionLogicalReplicationState *PgCurrentSessionLogicalReplicationState(void);
extern PgSessionAccessWalGUCState *PgCurrentSessionAccessWalGUCState(void);
extern PgSessionJitGUCState *PgCurrentSessionJitGUCState(void);
extern PgSessionJitProviderState *PgCurrentSessionJitProviderState(void);
extern PgSessionLLVMJitState *PgCurrentSessionLLVMJitState(void);
extern PgSessionLoggingState *PgCurrentSessionLoggingState(void);
extern PgSessionSortGUCState *PgCurrentSessionSortGUCState(void);
extern PgSessionQueryMemoryState *PgCurrentSessionQueryMemoryState(void);
extern PgSessionPlannerCostState *PgCurrentSessionPlannerCostState(void);
extern PgSessionPlannerMethodState *PgCurrentSessionPlannerMethodState(void);
extern PgSessionPgStatState *PgCurrentSessionPgStatState(void);
extern PgSessionMiscGUCState *PgCurrentSessionMiscGUCState(void);
extern PgSessionGUCState *PgCurrentSessionGUCState(void);
extern PgExecutionErrorState *PgCurrentExecutionErrorState(void);
extern PgExecutionMemoryContextState *PgCurrentExecutionMemoryContexts(void);
extern PgExecutionResourceOwnerState *PgCurrentExecutionResourceOwners(void);
extern PgExecutionSPIState *PgCurrentExecutionSPIState(void);
extern PgExecutionVacuumState *PgCurrentExecutionVacuumState(void);
extern PgExecutionAnalyzeState *PgCurrentExecutionAnalyzeState(void);
extern PgExecutionCatalogState *PgCurrentExecutionCatalogState(void);
extern PgExecutionCatalogCacheState *PgCurrentExecutionCatalogCacheState(void);
extern PgExecutionRelMapState *PgCurrentExecutionRelMapState(void);
extern PgExecutionInvalidationState *PgCurrentExecutionInvalidationState(void);
extern PgExecutionXactState *PgCurrentExecutionXactState(void);
extern PgExecutionGUCErrorState *PgCurrentExecutionGUCErrorState(void);
extern PgExecutionSnapshotState *PgCurrentExecutionSnapshotState(void);
extern PgExecutionComboCidState *PgCurrentExecutionComboCidState(void);
extern PgExecutionXLogInsertState *PgCurrentExecutionXLogInsertState(void);
extern PgExecutionRegexState *PgCurrentExecutionRegexState(void);
extern PgConnectionIdentityState *PgConnectionIdentityStateRef(PgConnection *connection);
extern PgConnectionSocketIOState *PgConnectionSocketIOStateRef(PgConnection *connection);
extern PgConnectionProtocolState *PgConnectionProtocolStateRef(PgConnection *connection);
extern PgConnectionOutputState *PgConnectionOutputStateRef(PgConnection *connection);
extern PgConnectionInterruptState *PgConnectionInterruptStateRef(PgConnection *connection);
extern PgConnectionStartupState *PgConnectionStartupStateRef(PgConnection *connection);
extern PgConnectionClientConnectionInfoState *PgConnectionClientConnectionInfoStateRef(PgConnection *connection);
extern bool *PgConnectionClientConnectionInfoAuthnIdOwnedRef(PgConnection *connection);
extern PgConnectionSecurityState *PgConnectionRuntimeSecurityStateRef(PgConnection *connection);
extern PgBackendInstrumentationState *PgCurrentBackendInstrumentationState(void);
extern PgBackendBufferState *PgCurrentBackendBufferState(void);
extern MemoryContext PgBackendBufferAllocationContext(void);
extern PgBackendStorageState *PgCurrentBackendStorageState(void);
extern void PgBackendResetFileAccessClosedState(PgBackendStorageState *storage);
extern void PgBackendResetStorageClosedState(PgBackendStorageState *storage);
extern PgBackendLockState *PgCurrentBackendLockState(void);
extern PgBackendIPCState *PgCurrentBackendIPCState(void);
extern void PgBackendResetTimeoutClosedState(PgBackendTimeoutState *timeout);
extern PgBackendPgStatPendingState *PgCurrentBackendPgStatPendingState(void);
extern PgBackendMemoryManagerState *PgCurrentBackendMemoryManagerState(void);
extern PgBackendTransactionState *PgCurrentBackendTransactionState(void);
extern PgBackendUtilityState *PgCurrentBackendUtilityState(void);
extern PgBackendParallelState *PgCurrentBackendParallelState(void);

extern void PgSessionInitializeVacuumState(PgSessionVacuumState *vacuum);
extern void PgSessionInitializeLockWaitState(PgSessionLockWaitState *lock_wait);
extern void PgSessionInitializeGUCState(PgSessionGUCState *guc);
extern void PgSessionInitializePgStatState(PgSessionPgStatState *pgstat);
extern void PgSessionInitializeExtensionModuleState(PgSessionExtensionModuleState *extension_modules);
extern void PgSessionResetCatalogLookupClosedState(PgSession *session);
extern void PgSessionInitializeInvalidationCallbackState(PgSessionInvalidationCallbackState *invalidation_callbacks);
extern void PgSessionInitializeRelMapState(PgSessionRelMapState *relmap);
extern void PgSessionInitializeRegexState(PgSessionRegexState *regex);
extern void PgSessionInitializePortalManagerState(PgSessionPortalManagerState *portal_manager);
extern void PgSessionInitializeLargeObjectState(PgSessionLargeObjectState *large_object);
extern void PgSessionInitializeEncodingState(PgSessionEncodingState *encoding);
extern void PgSessionInitializeTempFileState(PgSessionTempFileState *temp_file);
extern void PgSessionInitializePlanCacheState(PgSessionPlanCacheState *plan_cache);
extern void PgSessionInitializeNamespaceState(PgSessionNamespaceState *namespace_state);
extern void PgBackendInitializeParallelState(PgBackendParallelState *parallel);
extern void PgBackendInitializeBufferState(PgBackendBufferState *buffers);
extern void PgBackendInitializeStorageState(PgBackendStorageState *storage);
extern void PgBackendInitializeLockState(PgBackendLockState *locks);
extern void PgBackendInitializeIPCState(PgBackendIPCState *ipc);
extern void PgBackendInitializePgStatPendingState(PgBackendPgStatPendingState *pgstat_pending);
extern void PgBackendInitializeWaitState(PgBackendWaitState *wait_state);
extern void PgBackendInitializeTransactionState(PgBackendTransactionState *transaction);
extern void PgBackendInitializeRecoveryState(PgBackendRecoveryState *recovery);
extern void PgBackendInitializeRepackState(PgBackendRepackState *repack);
extern void PgBackendInitializeExtensionModuleState(PgBackendExtensionModuleState *extension_modules);
extern void PgExecutionInitializeErrorState(PgExecutionErrorState *error);
extern void PgExecutionInitializeSPIState(PgExecutionSPIState *spi);
extern void PgExecutionInitializeVacuumState(PgExecutionVacuumState *vacuum);
extern void PgExecutionInitializeNodeIOState(PgExecutionNodeIOState *node_io);
extern void PgExecutionInitializeBaseBackupState(PgExecutionBaseBackupState *basebackup);
extern void PgExecutionInitializeAnalyzeState(PgExecutionAnalyzeState *analyze);
extern void PgExecutionInitializeExtensionState(PgExecutionExtensionState *extension);
extern void PgExecutionInitializeMatViewState(PgExecutionMatViewState *matview);
extern void PgExecutionInitializeSnapshotState(PgExecutionSnapshotState *snapshot);
extern void PgExecutionInitializeComboCidState(PgExecutionComboCidState *combo_cid);
extern void PgExecutionInitializeXLogInsertState(PgExecutionXLogInsertState *xloginsert);
extern void PgExecutionInitializeXactState(PgExecutionXactState *xact);
extern void PgExecutionInitializeTransactionCleanupState(PgExecutionTransactionCleanupState *transaction_cleanup);
extern void PgExecutionInitializeReplicationScratchState(PgExecutionReplicationScratchState *replication_scratch);
extern void PgExecutionInitializeGUCErrorState(PgExecutionGUCErrorState *guc_error);
extern void PgExecutionInitializeAsyncState(PgExecutionAsyncState *async);
extern void PgExecutionInitializeCatalogState(PgExecutionCatalogState *catalog);
extern void PgExecutionInitializeCatalogCacheState(PgExecutionCatalogCacheState *catalog_cache);
extern void PgExecutionInitializeRelMapState(PgExecutionRelMapState *relmap);
extern void PgExecutionInitializeInvalidationState(PgExecutionInvalidationState *invalidation);
extern void PgExecutionInitializeTwoPhaseRecordState(PgExecutionTwoPhaseRecordState *two_phase_records);
extern void PgExecutionInitializeTriggerState(PgExecutionTriggerState *trigger);
extern void PgExecutionInitializeRegexState(PgExecutionRegexState *regex);
extern void PgExecutionInitializeValgrindState(PgExecutionValgrindState *valgrind);
extern void PgExecutionInitializeSnapBuildState(PgExecutionSnapBuildState *snapbuild);

#endif							/* BACKEND_RUNTIME_INTERNAL_H */
