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

#define PG_RUNTIME_RETURN_CURRENT_EXECUTION_BUCKET(variable, field) \
	do { \
		typeof(variable) bucket = (variable); \
		if (likely(bucket != NULL)) \
			return bucket; \
		return &PgCurrentOrEarlyExecution()->field; \
	} while (0)

#define PG_RUNTIME_RETURN_CURRENT_SESSION_BUCKET(variable, field) \
	do { \
		typeof(variable) bucket = (variable); \
		if (likely(bucket != NULL)) \
			return bucket; \
		return &PgCurrentOrEarlySession()->field; \
	} while (0)

#define PG_RUNTIME_RETURN_CURRENT_BACKEND_BUCKET(variable, field, early_state) \
	do { \
		typeof(variable) bucket = (variable); \
		PgBackend  *backend; \
		if (likely(bucket != NULL)) \
			return bucket; \
		backend = CurrentPgBackend; \
		if (backend == NULL) \
			return &(early_state); \
		return &backend->field; \
	} while (0)

#define PG_RUNTIME_RETURN_INITIALIZED_SESSION_BUCKET(variable, field, early_state, init_function) \
	do { \
		typeof(variable) bucket = (variable); \
		PgSession *session; \
		if (likely(bucket != NULL && bucket->initialized)) \
			return bucket; \
		session = CurrentPgSession; \
		if (session == NULL) \
			bucket = &(early_state); \
		else \
			bucket = &session->field; \
		if (!bucket->initialized) \
			init_function(bucket); \
		return bucket; \
	} while (0)

extern PgCarrier *PgCurrentCarrierState(void);
extern void PgRuntimeFlushCurrentHotMirrors(void);
extern void PgRuntimeReloadCurrentHotMirrors(void);
extern void PgRuntimeFlushCurrentHotCells(void);
extern void PgRuntimeReloadCurrentHotCells(void);
extern void PgBackendInitializeIdCounter(void);
extern void PgBackendInitializeRuntimeObject(PgBackend *backend,
											 PgRuntime *runtime,
											 PgCarrier *carrier,
											 PgSession *session,
											 PgConnection *connection,
											 PgExecution *execution,
											 BackendType backend_type,
											 struct Latch *interrupt_latch);
extern void PgBackendResetEarlyFallbackAfterFork(int proc_pid);
extern PgSession *PgProcessSessionState(void);
extern PgSession *PgCurrentOrEarlySession(void);
extern void PgSessionInitializeRuntimeObject(PgSession *session,
											 PgBackend *backend,
											 PgConnection *connection,
											 PgExecution *execution);
extern void PgSessionAdoptEarlyState(PgSession *session);
extern PgExecution *PgCurrentOrEarlyExecution(void);
extern void PgExecutionInitializeRuntimeObject(PgExecution *execution,
											   PgBackend *backend,
											   PgSession *session,
											   PgCarrier *carrier);
extern void PgRuntimeInitializeServerGUCState(PgRuntimeServerGUCState *server_guc);
extern void PgRuntimeAdoptEarlyServerGUCState(PgRuntime *runtime);
extern bool PgRuntimeServerGUCStateHasConfigPaths(PgRuntimeServerGUCState *server_guc);
extern PgRuntimeServerGUCState *PgEarlyRuntimeServerGUCState(void);
extern PgRuntimeServerGUCState *PgCurrentRuntimeServerGUCState(void);
extern void PgRuntimeInitializeExtensionModuleState(PgRuntimeExtensionModuleState *extension_modules);
extern void PgRuntimeAdoptEarlyExtensionModuleState(PgRuntime *runtime);
extern PgRuntimeExtensionModuleState *PgCurrentRuntimeExtensionModuleState(void);
extern void PgRuntimeProtocolSchedulerCarrierBecameActive(PgCarrier *carrier);
extern void PgRuntimeProtocolSchedulerCarrierBecameIdle(PgCarrier *carrier);
#ifndef PgCurrentSessionLoopState
extern PgSessionLoopState *PgCurrentSessionLoopState(void);
#endif
#ifndef PgCurrentSessionTcopState
extern PgSessionTcopState *PgCurrentSessionTcopState(void);
#endif
#ifndef PgCurrentSessionDatabaseState
extern PgSessionDatabaseState *PgCurrentSessionDatabaseState(void);
#endif
#ifndef PgCurrentSessionTablespaceState
extern PgSessionTablespaceState *PgCurrentSessionTablespaceState(void);
#endif
#ifndef PgCurrentSessionBinaryUpgradeState
extern PgSessionBinaryUpgradeState *PgCurrentSessionBinaryUpgradeState(void);
#endif
#ifndef PgCurrentSessionCatalogLookupState
extern PgSessionCatalogLookupState *PgCurrentSessionCatalogLookupState(void);
#endif
#ifndef PgCurrentSessionTextSearchState
extern PgSessionTextSearchState *PgCurrentSessionTextSearchState(void);
#endif
#ifndef PgCurrentSessionConnectionGUCState
extern PgSessionConnectionGUCState *PgCurrentSessionConnectionGUCState(void);
#endif
#ifndef PgCurrentSessionDateTimeState
extern PgSessionDateTimeState *PgCurrentSessionDateTimeState(void);
#endif
#ifndef PgCurrentSessionEncodingState
extern PgSessionEncodingState *PgCurrentSessionEncodingState(void);
#endif
#ifndef PgCurrentSessionTempFileState
extern PgSessionTempFileState *PgCurrentSessionTempFileState(void);
#endif
#ifndef PgCurrentSessionParserState
extern PgSessionParserState *PgCurrentSessionParserState(void);
#endif
#ifndef PgCurrentSessionVacuumState
extern PgSessionVacuumState *PgCurrentSessionVacuumState(void);
#endif
#ifndef PgCurrentSessionBufferIOState
extern PgSessionBufferIOState *PgCurrentSessionBufferIOState(void);
#endif
#ifndef PgCurrentSessionXactDefaultState
extern PgSessionXactDefaultState *PgCurrentSessionXactDefaultState(void);
#endif
#ifndef PgCurrentSessionLockWaitState
extern PgSessionLockWaitState *PgCurrentSessionLockWaitState(void);
#endif
extern PgSessionNamespaceState *PgCurrentSessionNamespaceState(void);
extern PgSessionLocaleState *PgCurrentSessionLocaleState(void);
#ifndef PgCurrentSessionExtensionModuleState
extern PgSessionExtensionModuleState *PgCurrentSessionExtensionModuleState(void);
#endif
extern PgSessionInvalidationCallbackState *PgCurrentSessionInvalidationCallbackState(void);
#ifndef PgCurrentSessionRIGlobalsState
extern PgSessionRIGlobalsState *PgCurrentSessionRIGlobalsState(void);
#endif
#ifndef PgCurrentSessionRelMapState
extern PgSessionRelMapState *PgCurrentSessionRelMapState(void);
#endif
#ifndef PgCurrentSessionPreparedStatementState
extern PgSessionPreparedStatementState *PgCurrentSessionPreparedStatementState(void);
#endif
#ifndef PgCurrentSessionPlanCacheState
extern PgSessionPlanCacheState *PgCurrentSessionPlanCacheState(void);
#endif
#ifndef PgCurrentSessionOnCommitState
extern PgSessionOnCommitState *PgCurrentSessionOnCommitState(void);
#endif
#ifndef PgCurrentSessionSequenceState
extern PgSessionSequenceState *PgCurrentSessionSequenceState(void);
#endif
#ifndef PgCurrentSessionXactCallbackState
extern PgSessionXactCallbackState *PgCurrentSessionXactCallbackState(void);
#endif
#ifndef PgCurrentSessionBackupState
extern PgSessionBackupState *PgCurrentSessionBackupState(void);
#endif
#ifndef PgCurrentSessionRegexState
extern PgSessionRegexState *PgCurrentSessionRegexState(void);
#endif
#ifndef PgCurrentSessionPortalManagerState
extern PgSessionPortalManagerState *PgCurrentSessionPortalManagerState(void);
#endif
#ifndef PgCurrentSessionLargeObjectState
extern PgSessionLargeObjectState *PgCurrentSessionLargeObjectState(void);
#endif
#ifndef PgCurrentSessionAsyncState
extern PgSessionAsyncState *PgCurrentSessionAsyncState(void);
#endif
#ifndef PgCurrentSessionRandomState
extern PgSessionRandomState *PgCurrentSessionRandomState(void);
#endif
#ifndef PgCurrentSessionOptimizerState
extern PgSessionOptimizerState *PgCurrentSessionOptimizerState(void);
#endif
#ifndef PgCurrentSessionFunctionManagerState
extern PgSessionFunctionManagerState *PgCurrentSessionFunctionManagerState(void);
#endif
#ifndef PgCurrentSessionGeneralGUCState
extern PgSessionGeneralGUCState *PgCurrentSessionGeneralGUCState(void);
#endif
#ifndef PgCurrentSessionQueryIdState
extern PgSessionQueryIdState *PgCurrentSessionQueryIdState(void);
#endif
#ifndef PgCurrentSessionStorageGUCState
extern PgSessionStorageGUCState *PgCurrentSessionStorageGUCState(void);
#endif
#ifndef PgCurrentSessionUserGUCState
extern PgSessionUserGUCState *PgCurrentSessionUserGUCState(void);
#endif
#ifndef PgCurrentSessionCommandGUCState
extern PgSessionCommandGUCState *PgCurrentSessionCommandGUCState(void);
#endif
#ifndef PgCurrentSessionReplicationGUCState
extern PgSessionReplicationGUCState *PgCurrentSessionReplicationGUCState(void);
#endif
#ifndef PgCurrentSessionLogicalReplicationState
extern PgSessionLogicalReplicationState *PgCurrentSessionLogicalReplicationState(void);
#endif
#ifndef PgCurrentSessionAccessWalGUCState
extern PgSessionAccessWalGUCState *PgCurrentSessionAccessWalGUCState(void);
#endif
#ifndef PgCurrentSessionJitGUCState
extern PgSessionJitGUCState *PgCurrentSessionJitGUCState(void);
#endif
#ifndef PgCurrentSessionJitProviderState
extern PgSessionJitProviderState *PgCurrentSessionJitProviderState(void);
#endif
#ifndef PgCurrentSessionLLVMJitState
extern PgSessionLLVMJitState *PgCurrentSessionLLVMJitState(void);
#endif
#ifndef PgCurrentSessionLoggingState
extern PgSessionLoggingState *PgCurrentSessionLoggingState(void);
#endif
#ifndef PgCurrentSessionSortGUCState
extern PgSessionSortGUCState *PgCurrentSessionSortGUCState(void);
#endif
#ifndef PgCurrentSessionQueryMemoryState
extern PgSessionQueryMemoryState *PgCurrentSessionQueryMemoryState(void);
#endif
#ifndef PgCurrentSessionPlannerCostState
extern PgSessionPlannerCostState *PgCurrentSessionPlannerCostState(void);
#endif
#ifndef PgCurrentSessionPlannerMethodState
extern PgSessionPlannerMethodState *PgCurrentSessionPlannerMethodState(void);
#endif
#ifndef PgCurrentSessionPgStatState
extern PgSessionPgStatState *PgCurrentSessionPgStatState(void);
#endif
#ifndef PgCurrentSessionMiscGUCState
extern PgSessionMiscGUCState *PgCurrentSessionMiscGUCState(void);
#endif
#ifndef PgCurrentSessionGUCState
extern PgSessionGUCState *PgCurrentSessionGUCState(void);
#endif
#ifndef PgCurrentBackendCommandState
extern PgBackendCommandState *PgCurrentBackendCommandState(void);
#endif
#ifndef PgCurrentBackendActivityState
extern PgBackendActivityState *PgCurrentBackendActivityState(void);
#endif
#ifndef PgCurrentBackendLogState
extern PgBackendLogState *PgCurrentBackendLogState(void);
#endif
extern MemoryContext PgRuntimeEnsureExtensionModuleMemoryContext(PgRuntimeExtensionModuleState *extension_modules);
#ifndef PgCurrentExecutionErrorState
extern PgExecutionErrorState *PgCurrentExecutionErrorState(void);
#endif
#ifndef PgCurrentExecutionDebugState
extern PgExecutionDebugState *PgCurrentExecutionDebugState(void);
#endif
#ifndef PgCurrentExecutionExtensionState
extern PgExecutionExtensionState *PgCurrentExecutionExtensionState(void);
#endif
#ifndef PgCurrentExecutionMemoryContexts
extern PgExecutionMemoryContextState *PgCurrentExecutionMemoryContexts(void);
#endif
#ifndef PgCurrentExecutionResourceOwners
extern PgExecutionResourceOwnerState *PgCurrentExecutionResourceOwners(void);
#endif
#ifndef PgCurrentExecutionSPIState
extern PgExecutionSPIState *PgCurrentExecutionSPIState(void);
#endif
#ifndef PgCurrentExecutionPortalState
extern PgExecutionPortalState *PgCurrentExecutionPortalState(void);
#endif
#ifndef PgCurrentExecutionVacuumState
extern PgExecutionVacuumState *PgCurrentExecutionVacuumState(void);
#endif
#ifndef PgCurrentExecutionAnalyzeState
extern PgExecutionAnalyzeState *PgCurrentExecutionAnalyzeState(void);
#endif
#ifndef PgCurrentExecutionNodeIOState
extern PgExecutionNodeIOState *PgCurrentExecutionNodeIOState(void);
#endif
#ifndef PgCurrentExecutionBaseBackupState
extern PgExecutionBaseBackupState *PgCurrentExecutionBaseBackupState(void);
#endif
#ifndef PgCurrentExecutionMatViewState
extern PgExecutionMatViewState *PgCurrentExecutionMatViewState(void);
#endif
#ifndef PgCurrentExecutionCatalogState
extern PgExecutionCatalogState *PgCurrentExecutionCatalogState(void);
#endif
#ifndef PgCurrentExecutionCatalogCacheState
extern PgExecutionCatalogCacheState *PgCurrentExecutionCatalogCacheState(void);
#endif
#ifndef PgCurrentExecutionRelMapState
extern PgExecutionRelMapState *PgCurrentExecutionRelMapState(void);
#endif
#ifndef PgCurrentExecutionInvalidationState
extern PgExecutionInvalidationState *PgCurrentExecutionInvalidationState(void);
#endif
#ifndef PgCurrentExecutionTwoPhaseRecordState
extern PgExecutionTwoPhaseRecordState *PgCurrentExecutionTwoPhaseRecordState(void);
#endif
#ifndef PgCurrentExecutionAsyncState
extern PgExecutionAsyncState *PgCurrentExecutionAsyncState(void);
#endif
#ifndef PgCurrentExecutionXactState
extern PgExecutionXactState *PgCurrentExecutionXactState(void);
#endif
#ifndef PgCurrentExecutionGUCErrorState
extern PgExecutionGUCErrorState *PgCurrentExecutionGUCErrorState(void);
#endif
#ifndef PgCurrentExecutionSnapshotState
extern PgExecutionSnapshotState *PgCurrentExecutionSnapshotState(void);
#endif
#ifndef PgCurrentExecutionComboCidState
extern PgExecutionComboCidState *PgCurrentExecutionComboCidState(void);
#endif
#ifndef PgCurrentExecutionXLogInsertState
extern PgExecutionXLogInsertState *PgCurrentExecutionXLogInsertState(void);
#endif
#ifndef PgCurrentExecutionTransactionCleanupState
extern PgExecutionTransactionCleanupState *PgCurrentExecutionTransactionCleanupState(void);
#endif
#ifndef PgCurrentExecutionRegexState
extern PgExecutionRegexState *PgCurrentExecutionRegexState(void);
#endif
#ifndef PgCurrentExecutionTriggerState
extern PgExecutionTriggerState *PgCurrentExecutionTriggerState(void);
#endif
#ifndef PgCurrentExecutionValgrindState
extern PgExecutionValgrindState *PgCurrentExecutionValgrindState(void);
#endif
#ifndef PgCurrentExecutionReplicationScratchState
extern PgExecutionReplicationScratchState *PgCurrentExecutionReplicationScratchState(void);
#endif
#ifndef PgCurrentExecutionSnapBuildState
extern PgExecutionSnapBuildState *PgCurrentExecutionSnapBuildState(void);
#endif
extern PgConnectionIdentityState *PgConnectionIdentityStateRef(PgConnection *connection);
extern PgConnectionSocketIOState *PgConnectionSocketIOStateRef(PgConnection *connection);
extern PgConnectionProtocolState *PgConnectionProtocolStateRef(PgConnection *connection);
extern PgConnectionOutputState *PgConnectionOutputStateRef(PgConnection *connection);
extern PgConnectionInterruptState *PgConnectionInterruptStateRef(PgConnection *connection);
extern PgConnectionStartupState *PgConnectionStartupStateRef(PgConnection *connection);
extern PgConnectionClientConnectionInfoState *PgConnectionClientConnectionInfoStateRef(PgConnection *connection);
extern bool *PgConnectionClientConnectionInfoAuthnIdOwnedRef(PgConnection *connection);
extern PgConnectionSecurityState *PgConnectionRuntimeSecurityStateRef(PgConnection *connection);
extern void PgConnectionInitializeRuntimeObject(PgConnection *connection,
												PgBackend *backend,
												PgSession *session,
												struct Port *port);
#ifndef PgCurrentCoreState
extern PgBackendCoreState *PgCurrentCoreState(void);
#endif
#ifndef PgCurrentBackendInstrumentationState
extern PgBackendInstrumentationState *PgCurrentBackendInstrumentationState(void);
#endif
#ifndef PgCurrentBackendBufferState
extern PgBackendBufferState *PgCurrentBackendBufferState(void);
#endif
extern MemoryContext PgBackendBufferAllocationContext(void);
#ifndef PgCurrentBackendStorageState
extern PgBackendStorageState *PgCurrentBackendStorageState(void);
#endif
extern void PgBackendResetFileAccessClosedState(PgBackendStorageState *storage);
extern void PgBackendResetStorageClosedState(PgBackendStorageState *storage);
#ifndef PgCurrentBackendLockState
extern PgBackendLockState *PgCurrentBackendLockState(void);
#endif
#ifndef PgCurrentBackendIPCState
extern PgBackendIPCState *PgCurrentBackendIPCState(void);
#endif
#ifndef PgCurrentBackendWaitState
extern PgBackendWaitState *PgCurrentBackendWaitState(void);
#endif
extern void PgBackendResetTimeoutClosedState(PgBackendTimeoutState *timeout);
#ifndef PgCurrentBackendPgStatPendingState
extern PgBackendPgStatPendingState *PgCurrentBackendPgStatPendingState(void);
#endif
extern PgStat_LocalState *PgCurrentPgStatLocalStateSlow(void);
#ifndef PgCurrentBackendMemoryManagerState
extern PgBackendMemoryManagerState *PgCurrentBackendMemoryManagerState(void);
#endif
#ifndef PgCurrentBackendTransactionState
extern PgBackendTransactionState *PgCurrentBackendTransactionState(void);
#endif
#ifndef PgCurrentBackendUtilityState
extern PgBackendUtilityState *PgCurrentBackendUtilityState(void);
#endif
#ifndef PgCurrentBackendParallelState
extern PgBackendParallelState *PgCurrentBackendParallelState(void);
#endif
#ifndef PgCurrentPendingInterrupts
extern PgBackendPendingInterruptState *PgCurrentPendingInterrupts(void);
#endif
#ifndef PgCurrentInterruptHoldoffs
extern PgBackendInterruptHoldoffState *PgCurrentInterruptHoldoffs(void);
#endif

extern void PgSessionInitializeDateTimeState(PgSessionDateTimeState *datetime);
extern void PgSessionInitializeVacuumState(PgSessionVacuumState *vacuum);
extern void PgSessionInitializeLockWaitState(PgSessionLockWaitState *lock_wait);
extern void PgSessionInitializeParserState(PgSessionParserState *parser);
extern void PgSessionInitializeGUCState(PgSessionGUCState *guc);
extern bool PgSessionSetStaticGUCDefaultsForInitialization(bool use_static);
extern void PgSessionResetEarlyFallbackForNewBackend(void);
extern void PgSessionInitializePgStatState(PgSessionPgStatState *pgstat);
extern void PgSessionInitializeExtensionModuleState(PgSessionExtensionModuleState *extension_modules);
extern void PgSessionResetCatalogLookupClosedState(PgSession *session);
extern void PgSessionInitializeInvalidationCallbackState(PgSessionInvalidationCallbackState *invalidation_callbacks);
extern void PgSessionInitializeRIGlobalsState(PgSessionRIGlobalsState *ri_globals);
extern void PgSessionInitializeRelMapState(PgSessionRelMapState *relmap);
extern void PgSessionInitializeRegexState(PgSessionRegexState *regex);
extern void PgSessionInitializePortalManagerState(PgSessionPortalManagerState *portal_manager);
extern void PgSessionInitializeLargeObjectState(PgSessionLargeObjectState *large_object);
extern void PgSessionInitializeEncodingState(PgSessionEncodingState *encoding);
extern void PgSessionInitializeTempFileState(PgSessionTempFileState *temp_file);
extern void PgSessionInitializePlanCacheState(PgSessionPlanCacheState *plan_cache);
extern void PgSessionInitializeNamespaceState(PgSessionNamespaceState *namespace_state);
extern void PgSessionInitializeLocaleState(PgSessionLocaleState *locale);
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
