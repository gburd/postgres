/*-------------------------------------------------------------------------
 *
 * backend_runtime_current.h
 *	  Carrier-local current runtime pointer bridge.
 *
 * Broad compatibility headers such as palloc.h cannot include the full
 * backend_runtime.h object definitions, but they still need the historical
 * CurrentPg* names as assignable lvalues.  Keep those root current pointers
 * in one TLS object so hot paths pay for one carrier-local bridge address
 * instead of one TLS variable per root pointer.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/include/utils/backend_runtime_current.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BACKEND_RUNTIME_CURRENT_H
#define BACKEND_RUNTIME_CURRENT_H

#include <setjmp.h>
#include <signal.h>

#include "utils/global_lifetime.h"

typedef struct PgRuntime PgRuntime;
typedef struct PgCarrier PgCarrier;
typedef struct PgBackend PgBackend;
typedef struct PgSession PgSession;
typedef struct PgConnection PgConnection;
typedef struct PgExecution PgExecution;
typedef struct MemoryContextData *MemoryContext;
typedef struct BufferUsage BufferUsage;
typedef struct WalUsage WalUsage;
typedef struct PgStat_BackendPending PgStat_BackendPending;
typedef struct TransactionStateData TransactionStateData;

#include "utils/backend_runtime_current_state_forward_decls.def"

typedef struct PgBackendBufferState PgBackendBufferState;
typedef struct PgBackendCoreState PgBackendCoreState;
typedef struct PgBackendExprInterpState PgBackendExprInterpState;
typedef struct PgBackendInstrumentationState PgBackendInstrumentationState;
typedef struct PgBackendIPCState PgBackendIPCState;
typedef struct PgBackendInterruptHoldoffState PgBackendInterruptHoldoffState;
typedef struct PgBackendLockState PgBackendLockState;
typedef struct PgBackendLWLockHandle PgBackendLWLockHandle;
typedef struct PgBackendMemoryManagerState PgBackendMemoryManagerState;
typedef struct PgBackendParallelState PgBackendParallelState;
typedef struct PgBackendPendingInterruptState PgBackendPendingInterruptState;
typedef struct PgBackendPgStatPendingState PgBackendPgStatPendingState;
typedef struct PgBackendStorageState PgBackendStorageState;
typedef struct PgBackendTimeoutState PgBackendTimeoutState;
typedef struct PgBackendTransactionState PgBackendTransactionState;
typedef struct PgBackendUtilityState PgBackendUtilityState;
typedef struct PgBackendWaitState PgBackendWaitState;
typedef struct PgBackendStatus PgBackendStatus;
typedef struct PgBackendXLogState PgBackendXLogState;
typedef struct PgConnectionProtocolState PgConnectionProtocolState;
typedef struct PgConnectionSocketIOState PgConnectionSocketIOState;
typedef struct PgExecutionCatalogState PgExecutionCatalogState;
typedef struct PgExecutionCatalogCacheState PgExecutionCatalogCacheState;
typedef struct PgExecutionDebugState PgExecutionDebugState;
typedef struct PgExecutionErrorState PgExecutionErrorState;
typedef struct PgExecutionSPIState PgExecutionSPIState;
typedef struct PgExecutionMemoryContextState PgExecutionMemoryContextState;
typedef struct PgExecutionPortalState PgExecutionPortalState;
typedef struct PgExecutionVacuumState PgExecutionVacuumState;
typedef struct PgExecutionNodeIOState PgExecutionNodeIOState;
typedef struct PgExecutionBaseBackupState PgExecutionBaseBackupState;
typedef struct PgExecutionAnalyzeState PgExecutionAnalyzeState;
typedef struct PgExecutionExtensionState PgExecutionExtensionState;
typedef struct PgExecutionMatViewState PgExecutionMatViewState;
typedef struct PgExecutionResourceOwnerState PgExecutionResourceOwnerState;
typedef struct PgExecutionSnapshotState PgExecutionSnapshotState;
typedef struct PgExecutionComboCidState PgExecutionComboCidState;
typedef struct PgExecutionXLogInsertState PgExecutionXLogInsertState;
typedef struct PgExecutionXactState PgExecutionXactState;
typedef struct PgExecutionTransactionCleanupState PgExecutionTransactionCleanupState;
typedef struct PgExecutionReplicationScratchState PgExecutionReplicationScratchState;
typedef struct PgExecutionGUCErrorState PgExecutionGUCErrorState;
typedef struct PgExecutionAsyncState PgExecutionAsyncState;
typedef struct PgExecutionRelMapState PgExecutionRelMapState;
typedef struct PgExecutionInvalidationState PgExecutionInvalidationState;
typedef struct PgExecutionTwoPhaseRecordState PgExecutionTwoPhaseRecordState;
typedef struct PgExecutionTriggerState PgExecutionTriggerState;
typedef struct PgExecutionRegexState PgExecutionRegexState;
typedef struct PgExecutionValgrindState PgExecutionValgrindState;
typedef struct PgExecutionSnapBuildState PgExecutionSnapBuildState;
typedef struct PgSessionCatalogLookupState PgSessionCatalogLookupState;
typedef struct PgSessionDateTimeState PgSessionDateTimeState;
typedef struct PgSessionXactDefaultState PgSessionXactDefaultState;
typedef struct PgSessionLockWaitState PgSessionLockWaitState;
typedef struct PgSessionParserState PgSessionParserState;
typedef struct PgSessionVacuumState PgSessionVacuumState;
typedef struct PgSessionRegexState PgSessionRegexState;
typedef struct PgSessionRIGlobalsState PgSessionRIGlobalsState;
typedef struct PgSessionEncodingState PgSessionEncodingState;
typedef struct PgSessionPortalManagerState PgSessionPortalManagerState;
typedef struct PgSessionGUCState PgSessionGUCState;
typedef struct PgSessionLocaleState PgSessionLocaleState;
typedef struct PgSessionLoggingState PgSessionLoggingState;
typedef struct PgSessionLoopState PgSessionLoopState;
typedef struct PgSessionMiscGUCState PgSessionMiscGUCState;
typedef struct PgSessionNamespaceState PgSessionNamespaceState;
typedef struct PgSessionPgStatState PgSessionPgStatState;
typedef struct PgSessionPlannerCostState PgSessionPlannerCostState;
typedef struct PgSessionPlannerMethodState PgSessionPlannerMethodState;
typedef struct PgSessionQueryMemoryState PgSessionQueryMemoryState;
struct catcache;
struct ErrorContextCallback;
struct HTAB;
struct PgStat_PendingIO;
struct PQcommMethods;

typedef struct PgRuntimeCurrentBridge
{
	PgRuntime  *runtime;
	PgCarrier  *carrier;
	PgBackend  *backend;
	PgSession  *session;
	PgConnection *connection;
	PgExecution *execution;

#define PG_RUNTIME_HOT_CELL(variable, owner, owner_type, type, field) \
	type	   *variable;
#include "utils/backend_runtime_hot_cells.def"
#undef PG_RUNTIME_HOT_CELL

#define PG_RUNTIME_HOT_BUCKET(variable, type, owner, field) \
	type	   *variable;
#include "utils/backend_runtime_hot_buckets.def"
#undef PG_RUNTIME_HOT_BUCKET

#define PG_RUNTIME_HOT_MIRROR(variable, owner, owner_type, type, field) \
	type		variable; \
	const void *variable##Owner;
#include "utils/backend_runtime_hot_mirrors.def"
#undef PG_RUNTIME_HOT_MIRROR

#define PG_RUNTIME_HOT_FIELD(variable, owner, type, expr) \
	type	   *variable; \
	const void *variable##Owner;
#include "utils/backend_runtime_hot_fields.def"
#undef PG_RUNTIME_HOT_FIELD
} PgRuntimeCurrentBridge;

extern PGDLLIMPORT PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgRuntimeCurrentBridge
			PgRuntimeCurrentBridgeState;

typedef struct PgRuntimeBridgeFallbackStats
{
	uint64		hot_cell;
	uint64		hot_mirror;
	uint64		hot_field;
	uint64		hot_bucket;
	uint64		fast_bucket;
	uint64		fast_initialized_bucket;
	uint64		carrier;
	uint64		interrupts;
	uint64		memory_contexts;
	uint64		session_catalog_lookup;
	uint64		after_triggers;
} PgRuntimeBridgeFallbackStats;

extern PGDLLIMPORT PG_THREAD_LOCAL PG_GLOBAL_CARRIER
			PgRuntimeBridgeFallbackStats PgRuntimeBridgeFallbackStatsState;

#define PG_RUNTIME_BRIDGE_COUNT_FALLBACK(member) \
	do { \
		PgRuntimeBridgeFallbackStatsState.member++; \
	} while (0)
#define PG_RUNTIME_BRIDGE_COUNT_FALLBACK_EXPR(member) \
	((void) (PgRuntimeBridgeFallbackStatsState.member++))

typedef enum PgRuntimeHotCurrentCellMode
{
	PG_RUNTIME_HOT_CURRENT_CELLS_FALLBACK = 0,
	PG_RUNTIME_HOT_CURRENT_CELLS_PROCESS = 1,
	PG_RUNTIME_HOT_CURRENT_CELLS_THREAD = 2
} PgRuntimeHotCurrentCellMode;

extern PGDLLIMPORT PG_THREAD_LOCAL PG_GLOBAL_CARRIER int PgRuntimeHotCurrentCellModeState;

#define PG_RUNTIME_CURRENT_ROOT_REF_DECL(name, type) \
extern PGDLLIMPORT PG_GLOBAL_RUNTIME type *name##ProcessRef; \
extern PGDLLIMPORT PG_THREAD_LOCAL PG_GLOBAL_CARRIER type *name##ThreadRef;
PG_RUNTIME_CURRENT_ROOT_REF_DECL(PgCurrentRuntimeHotRef, PgRuntime *)
PG_RUNTIME_CURRENT_ROOT_REF_DECL(PgCurrentCarrierHotRef, PgCarrier *)
PG_RUNTIME_CURRENT_ROOT_REF_DECL(PgCurrentBackendHotRef, PgBackend *)
PG_RUNTIME_CURRENT_ROOT_REF_DECL(PgCurrentSessionHotRef, PgSession *)
PG_RUNTIME_CURRENT_ROOT_REF_DECL(PgCurrentConnectionHotRef, PgConnection *)
PG_RUNTIME_CURRENT_ROOT_REF_DECL(PgCurrentExecutionHotRef, PgExecution *)
#undef PG_RUNTIME_CURRENT_ROOT_REF_DECL

#define PG_RUNTIME_HOT_CELL(variable, owner, owner_type, type, field) \
extern PGDLLIMPORT PG_GLOBAL_RUNTIME type *variable##ProcessCell; \
extern PGDLLIMPORT PG_THREAD_LOCAL PG_GLOBAL_CARRIER type *variable##ThreadCell;
#include "utils/backend_runtime_hot_cells.def"
#undef PG_RUNTIME_HOT_CELL

#define PG_RUNTIME_HOT_FIELD(variable, owner, type, expr) \
extern PGDLLIMPORT PG_GLOBAL_RUNTIME type *variable##ProcessRef; \
extern PGDLLIMPORT PG_GLOBAL_RUNTIME const void *variable##ProcessOwner; \
extern PGDLLIMPORT PG_THREAD_LOCAL PG_GLOBAL_CARRIER type *variable##ThreadRef; \
extern PGDLLIMPORT PG_THREAD_LOCAL PG_GLOBAL_CARRIER const void *variable##ThreadOwner;
#include "utils/backend_runtime_hot_fields.def"
#undef PG_RUNTIME_HOT_FIELD

#define PG_RUNTIME_CURRENT_ROOT_REF(name, type, field) \
static inline type * \
name##MaybeRef(void) \
{ \
	return &PgRuntimeCurrentBridgeState.field; \
}
PG_RUNTIME_CURRENT_ROOT_REF(PgCurrentRuntimeHotRef, PgRuntime *, runtime)
PG_RUNTIME_CURRENT_ROOT_REF(PgCurrentCarrierHotRef, PgCarrier *, carrier)
PG_RUNTIME_CURRENT_ROOT_REF(PgCurrentBackendHotRef, PgBackend *, backend)
PG_RUNTIME_CURRENT_ROOT_REF(PgCurrentSessionHotRef, PgSession *, session)
PG_RUNTIME_CURRENT_ROOT_REF(PgCurrentConnectionHotRef, PgConnection *, connection)
PG_RUNTIME_CURRENT_ROOT_REF(PgCurrentExecutionHotRef, PgExecution *, execution)
#undef PG_RUNTIME_CURRENT_ROOT_REF

#define CurrentPgRuntime (*PgCurrentRuntimeHotRefMaybeRef())
#define CurrentPgCarrier (*PgCurrentCarrierHotRefMaybeRef())
#define CurrentPgBackend (*PgCurrentBackendHotRefMaybeRef())
#define CurrentPgSession (*PgCurrentSessionHotRefMaybeRef())
#define CurrentPgConnection (*PgCurrentConnectionHotRefMaybeRef())
#define CurrentPgExecution (*PgCurrentExecutionHotRefMaybeRef())

#define PG_RUNTIME_HOT_CELL(variable, owner, owner_type, type, field) \
static inline type * \
variable##MaybeRef(type *(*fallback) (void)) \
{ \
	type	   *slot; \
 \
	slot = PgRuntimeCurrentBridgeState.variable; \
	if (likely(slot != NULL)) \
		return slot; \
 \
	PG_RUNTIME_BRIDGE_COUNT_FALLBACK(hot_cell); \
	return fallback(); \
}
#include "utils/backend_runtime_hot_cells.def"
#undef PG_RUNTIME_HOT_CELL

#define PG_RUNTIME_HOT_MIRROR(variable, owner, owner_type, type, field) \
static inline type * \
variable##MaybeRef(type *(*fallback) (void)) \
{ \
	PgRuntimeCurrentBridge *bridge = &PgRuntimeCurrentBridgeState; \
 \
	if (likely(bridge->variable##Owner != NULL && \
			   bridge->variable##Owner == (const void *) bridge->owner)) \
		return &bridge->variable; \
 \
	PG_RUNTIME_BRIDGE_COUNT_FALLBACK(hot_mirror); \
	return fallback(); \
}
#include "utils/backend_runtime_hot_mirrors.def"
#undef PG_RUNTIME_HOT_MIRROR

#define PG_RUNTIME_HOT_FIELD(variable, owner, type, expr) \
static inline type * \
variable##MaybeRef(type *(*fallback) (void)) \
{ \
	PgRuntimeCurrentBridge *bridge = &PgRuntimeCurrentBridgeState; \
	type	   *slot; \
 \
	slot = bridge->variable; \
	if (likely(slot != NULL && \
			   bridge->variable##Owner == \
			   (const void *) bridge->owner)) \
		return slot; \
 \
	PG_RUNTIME_BRIDGE_COUNT_FALLBACK(hot_field); \
	return fallback(); \
}
#include "utils/backend_runtime_hot_fields.def"
#undef PG_RUNTIME_HOT_FIELD

#define PG_RUNTIME_HOT_FIELD_REF(variable) \
	(PgRuntimeCurrentBridgeState.variable)
#define PG_RUNTIME_HOT_FIELD_OWNER(variable) \
	(PgRuntimeCurrentBridgeState.variable##Owner)
#define PG_RUNTIME_CURRENT_HOT_FIELD_REF(variable, owner, fallback) \
	variable##MaybeRef(fallback)

#define PG_RUNTIME_HOT_BUCKET(variable, type, owner, field) \
static inline type * \
variable##Maybe(void) \
{ \
	type	   *bucket; \
 \
	bucket = PgRuntimeCurrentBridgeState.variable; \
	if (likely(bucket != NULL)) \
		return bucket; \
 \
	PG_RUNTIME_BRIDGE_COUNT_FALLBACK(hot_bucket); \
	return NULL; \
}
#include "utils/backend_runtime_hot_buckets.def"
#undef PG_RUNTIME_HOT_BUCKET

#ifndef BACKEND_RUNTIME_CURRENT_NO_BUCKET_ALIASES
#define CurrentPgBackendExitRuntimeState \
	(CurrentPgBackendExitRuntimeStateMaybe())
#define CurrentPgBackendCoreRuntimeState \
	(CurrentPgBackendCoreRuntimeStateMaybe())
#define CurrentPgBackendCommandRuntimeState \
	(CurrentPgBackendCommandRuntimeStateMaybe())
#define CurrentPgBackendLogRuntimeState \
	(CurrentPgBackendLogRuntimeStateMaybe())
#define CurrentPgBackendExprInterpRuntimeState \
	(CurrentPgBackendExprInterpRuntimeStateMaybe())
#define CurrentPgBackendTimeoutRuntimeState \
	(CurrentPgBackendTimeoutRuntimeStateMaybe())
#define CurrentPgBackendWalSenderRuntimeState \
	(CurrentPgBackendWalSenderRuntimeStateMaybe())
#define CurrentPgBackendReplicationRuntimeState \
	(CurrentPgBackendReplicationRuntimeStateMaybe())
#define CurrentPgBackendLogicalReplicationRuntimeState \
	(CurrentPgBackendLogicalReplicationRuntimeStateMaybe())
#define CurrentPgBackendXLogRuntimeState \
	(CurrentPgBackendXLogRuntimeStateMaybe())
#define CurrentPgBackendRecoveryRuntimeState \
	(CurrentPgBackendRecoveryRuntimeStateMaybe())
#define CurrentPgBackendMaintenanceWorkerRuntimeState \
	(CurrentPgBackendMaintenanceWorkerRuntimeStateMaybe())
#define CurrentPgBackendAutovacuumRuntimeState \
	(CurrentPgBackendAutovacuumRuntimeStateMaybe())
#define CurrentPgBackendRepackRuntimeState \
	(CurrentPgBackendRepackRuntimeStateMaybe())
#define CurrentPgBackendAioRuntimeState \
	(CurrentPgBackendAioRuntimeStateMaybe())
#define CurrentPgBackendExtensionModuleRuntimeState \
	(CurrentPgBackendExtensionModuleRuntimeStateMaybe())
#define CurrentPgBackendPgStatPendingRuntimeState \
	(CurrentPgBackendPgStatPendingRuntimeStateMaybe())
#define CurrentPgBackendActivityRuntimeState \
	(CurrentPgBackendActivityRuntimeStateMaybe())
#define CurrentPgBackendMemoryManagerRuntimeState \
	(CurrentPgBackendMemoryManagerRuntimeStateMaybe())
#define CurrentPgBackendUtilityRuntimeState \
	(CurrentPgBackendUtilityRuntimeStateMaybe())
#define CurrentPgBackendParallelRuntimeState \
	(CurrentPgBackendParallelRuntimeStateMaybe())
#define CurrentPgBackendInstrumentationRuntimeState \
	(CurrentPgBackendInstrumentationRuntimeStateMaybe())
#define CurrentPgBackendBufferRuntimeState \
	(CurrentPgBackendBufferRuntimeStateMaybe())
#define CurrentPgBackendStorageRuntimeState \
	(CurrentPgBackendStorageRuntimeStateMaybe())
#define CurrentPgBackendLockRuntimeState \
	(CurrentPgBackendLockRuntimeStateMaybe())
#define CurrentPgBackendIPCRuntimeState \
	(CurrentPgBackendIPCRuntimeStateMaybe())
#define CurrentPgBackendTransactionRuntimeState \
	(CurrentPgBackendTransactionRuntimeStateMaybe())
#define CurrentPgBackendPendingInterruptRuntimeState \
	(CurrentPgBackendPendingInterruptRuntimeStateMaybe())
#define CurrentPgBackendInterruptHoldoffRuntimeState \
	(CurrentPgBackendInterruptHoldoffRuntimeStateMaybe())
#define CurrentPgBackendWaitRuntimeState \
	(CurrentPgBackendWaitRuntimeStateMaybe())
#define CurrentPgSessionLoopRuntimeState \
	(CurrentPgSessionLoopRuntimeStateMaybe())
#define CurrentPgSessionTcopRuntimeState \
	(CurrentPgSessionTcopRuntimeStateMaybe())
#define CurrentPgSessionDatabaseRuntimeState \
	(CurrentPgSessionDatabaseRuntimeStateMaybe())
#define CurrentPgSessionTablespaceRuntimeState \
	(CurrentPgSessionTablespaceRuntimeStateMaybe())
#define CurrentPgSessionBinaryUpgradeRuntimeState \
	(CurrentPgSessionBinaryUpgradeRuntimeStateMaybe())
#define CurrentPgSessionDateTimeRuntimeState \
	(CurrentPgSessionDateTimeRuntimeStateMaybe())
#define CurrentPgSessionParserRuntimeState \
	(CurrentPgSessionParserRuntimeStateMaybe())
#define CurrentPgSessionVacuumRuntimeState \
	(CurrentPgSessionVacuumRuntimeStateMaybe())
#define CurrentPgSessionBufferIORuntimeState \
	(CurrentPgSessionBufferIORuntimeStateMaybe())
#define CurrentPgSessionXactDefaultRuntimeState \
	(CurrentPgSessionXactDefaultRuntimeStateMaybe())
#define CurrentPgSessionLockWaitRuntimeState \
	(CurrentPgSessionLockWaitRuntimeStateMaybe())
#define CurrentPgSessionLoggingRuntimeState \
	(CurrentPgSessionLoggingRuntimeStateMaybe())
#define CurrentPgSessionMiscGUCRuntimeState \
	(CurrentPgSessionMiscGUCRuntimeStateMaybe())
#define CurrentPgSessionGUCRuntimeState \
	(CurrentPgSessionGUCRuntimeStateMaybe())
#define CurrentPgSessionPgStatRuntimeState \
	(CurrentPgSessionPgStatRuntimeStateMaybe())
#define CurrentPgSessionQueryIdRuntimeState \
	(CurrentPgSessionQueryIdRuntimeStateMaybe())
#define CurrentPgSessionStorageGUCRuntimeState \
	(CurrentPgSessionStorageGUCRuntimeStateMaybe())
#define CurrentPgSessionUserGUCRuntimeState \
	(CurrentPgSessionUserGUCRuntimeStateMaybe())
#define CurrentPgSessionUserIdentityRuntimeState \
	(CurrentPgSessionUserIdentityRuntimeStateMaybe())
#define CurrentPgSessionCommandGUCRuntimeState \
	(CurrentPgSessionCommandGUCRuntimeStateMaybe())
#define CurrentPgSessionReplicationGUCRuntimeState \
	(CurrentPgSessionReplicationGUCRuntimeStateMaybe())
#define CurrentPgSessionLogicalReplicationRuntimeState \
	(CurrentPgSessionLogicalReplicationRuntimeStateMaybe())
#define CurrentPgSessionGeneralGUCRuntimeState \
	(CurrentPgSessionGeneralGUCRuntimeStateMaybe())
#define CurrentPgSessionAccessWalGUCRuntimeState \
	(CurrentPgSessionAccessWalGUCRuntimeStateMaybe())
#define CurrentPgSessionJitGUCRuntimeState \
	(CurrentPgSessionJitGUCRuntimeStateMaybe())
#define CurrentPgSessionJitProviderRuntimeState \
	(CurrentPgSessionJitProviderRuntimeStateMaybe())
#define CurrentPgSessionLLVMJitRuntimeState \
	(CurrentPgSessionLLVMJitRuntimeStateMaybe())
#define CurrentPgSessionSortGUCRuntimeState \
	(CurrentPgSessionSortGUCRuntimeStateMaybe())
#define CurrentPgSessionTextSearchRuntimeState \
	(CurrentPgSessionTextSearchRuntimeStateMaybe())
#define CurrentPgSessionConnectionGUCRuntimeState \
	(CurrentPgSessionConnectionGUCRuntimeStateMaybe())
#define CurrentPgSessionQueryMemoryRuntimeState \
	(CurrentPgSessionQueryMemoryRuntimeStateMaybe())
#define CurrentPgSessionPlannerCostRuntimeState \
	(CurrentPgSessionPlannerCostRuntimeStateMaybe())
#define CurrentPgSessionPlannerMethodRuntimeState \
	(CurrentPgSessionPlannerMethodRuntimeStateMaybe())
#define CurrentPgSessionFunctionManagerRuntimeState \
	(CurrentPgSessionFunctionManagerRuntimeStateMaybe())
#define CurrentPgSessionExtensionModuleRuntimeState \
	(CurrentPgSessionExtensionModuleRuntimeStateMaybe())
#define CurrentPgSessionCatalogLookupRuntimeState \
	(CurrentPgSessionCatalogLookupRuntimeStateMaybe())
#define CurrentPgSessionInvalidationCallbackRuntimeState \
	(CurrentPgSessionInvalidationCallbackRuntimeStateMaybe())
#define CurrentPgSessionRIGlobalsRuntimeState \
	(CurrentPgSessionRIGlobalsRuntimeStateMaybe())
#define CurrentPgSessionRelMapRuntimeState \
	(CurrentPgSessionRelMapRuntimeStateMaybe())
#define CurrentPgSessionPreparedStatementRuntimeState \
	(CurrentPgSessionPreparedStatementRuntimeStateMaybe())
#define CurrentPgSessionOnCommitRuntimeState \
	(CurrentPgSessionOnCommitRuntimeStateMaybe())
#define CurrentPgSessionSequenceRuntimeState \
	(CurrentPgSessionSequenceRuntimeStateMaybe())
#define CurrentPgSessionXactCallbackRuntimeState \
	(CurrentPgSessionXactCallbackRuntimeStateMaybe())
#define CurrentPgSessionBackupRuntimeState \
	(CurrentPgSessionBackupRuntimeStateMaybe())
#define CurrentPgSessionRegexRuntimeState \
	(CurrentPgSessionRegexRuntimeStateMaybe())
#define CurrentPgSessionPortalManagerRuntimeState \
	(CurrentPgSessionPortalManagerRuntimeStateMaybe())
#define CurrentPgSessionLargeObjectRuntimeState \
	(CurrentPgSessionLargeObjectRuntimeStateMaybe())
#define CurrentPgSessionAsyncRuntimeState \
	(CurrentPgSessionAsyncRuntimeStateMaybe())
#define CurrentPgSessionEncodingRuntimeState \
	(CurrentPgSessionEncodingRuntimeStateMaybe())
#define CurrentPgSessionTempFileRuntimeState \
	(CurrentPgSessionTempFileRuntimeStateMaybe())
#define CurrentPgSessionRandomRuntimeState \
	(CurrentPgSessionRandomRuntimeStateMaybe())
#define CurrentPgSessionOptimizerRuntimeState \
	(CurrentPgSessionOptimizerRuntimeStateMaybe())
#define CurrentPgSessionPlanCacheRuntimeState \
	(CurrentPgSessionPlanCacheRuntimeStateMaybe())
#define CurrentPgSessionNamespaceRuntimeState \
	(CurrentPgSessionNamespaceRuntimeStateMaybe())
#define CurrentPgSessionLocaleRuntimeState \
	(CurrentPgSessionLocaleRuntimeStateMaybe())
#define CurrentPgConnectionIdentityRuntimeState \
	(CurrentPgConnectionIdentityRuntimeStateMaybe())
#define CurrentPgConnectionSocketIORuntimeState \
	(CurrentPgConnectionSocketIORuntimeStateMaybe())
#define CurrentPgConnectionProtocolRuntimeState \
	(CurrentPgConnectionProtocolRuntimeStateMaybe())
#define CurrentPgConnectionOutputRuntimeState \
	(CurrentPgConnectionOutputRuntimeStateMaybe())
#define CurrentPgConnectionInterruptRuntimeState \
	(CurrentPgConnectionInterruptRuntimeStateMaybe())
#define CurrentPgConnectionStartupRuntimeState \
	(CurrentPgConnectionStartupRuntimeStateMaybe())
#define CurrentPgConnectionClientConnectionInfoRuntimeState \
	(CurrentPgConnectionClientConnectionInfoRuntimeStateMaybe())
#define CurrentPgConnectionSecurityRuntimeState \
	(CurrentPgConnectionSecurityRuntimeStateMaybe())
#define CurrentPgExecutionDebugRuntimeState \
	(CurrentPgExecutionDebugRuntimeStateMaybe())
#define CurrentPgExecutionErrorRuntimeState \
	(CurrentPgExecutionErrorRuntimeStateMaybe())
#define CurrentPgExecutionMemoryContextRuntimeState \
	(CurrentPgExecutionMemoryContextRuntimeStateMaybe())
#define CurrentPgExecutionResourceOwnerRuntimeState \
	(CurrentPgExecutionResourceOwnerRuntimeStateMaybe())
#define CurrentPgExecutionSPIRuntimeState \
	(CurrentPgExecutionSPIRuntimeStateMaybe())
#define CurrentPgExecutionPortalRuntimeState \
	(CurrentPgExecutionPortalRuntimeStateMaybe())
#define CurrentPgExecutionVacuumRuntimeState \
	(CurrentPgExecutionVacuumRuntimeStateMaybe())
#define CurrentPgExecutionNodeIORuntimeState \
	(CurrentPgExecutionNodeIORuntimeStateMaybe())
#define CurrentPgExecutionBaseBackupRuntimeState \
	(CurrentPgExecutionBaseBackupRuntimeStateMaybe())
#define CurrentPgExecutionAnalyzeRuntimeState \
	(CurrentPgExecutionAnalyzeRuntimeStateMaybe())
#define CurrentPgExecutionExtensionRuntimeState \
	(CurrentPgExecutionExtensionRuntimeStateMaybe())
#define CurrentPgExecutionMatViewRuntimeState \
	(CurrentPgExecutionMatViewRuntimeStateMaybe())
#define CurrentPgExecutionSnapshotRuntimeState \
	(CurrentPgExecutionSnapshotRuntimeStateMaybe())
#define CurrentPgExecutionComboCidRuntimeState \
	(CurrentPgExecutionComboCidRuntimeStateMaybe())
#define CurrentPgExecutionXLogInsertRuntimeState \
	(CurrentPgExecutionXLogInsertRuntimeStateMaybe())
#define CurrentPgExecutionXactRuntimeState \
	(CurrentPgExecutionXactRuntimeStateMaybe())
#define CurrentPgExecutionTransactionCleanupRuntimeState \
	(CurrentPgExecutionTransactionCleanupRuntimeStateMaybe())
#define CurrentPgExecutionReplicationScratchRuntimeState \
	(CurrentPgExecutionReplicationScratchRuntimeStateMaybe())
#define CurrentPgExecutionGUCErrorRuntimeState \
	(CurrentPgExecutionGUCErrorRuntimeStateMaybe())
#define CurrentPgExecutionAsyncRuntimeState \
	(CurrentPgExecutionAsyncRuntimeStateMaybe())
#define CurrentPgExecutionCatalogRuntimeState \
	(CurrentPgExecutionCatalogRuntimeStateMaybe())
#define CurrentPgExecutionCatalogCacheRuntimeState \
	(CurrentPgExecutionCatalogCacheRuntimeStateMaybe())
#define CurrentPgExecutionRelMapRuntimeState \
	(CurrentPgExecutionRelMapRuntimeStateMaybe())
#define CurrentPgExecutionInvalidationRuntimeState \
	(CurrentPgExecutionInvalidationRuntimeStateMaybe())
#define CurrentPgExecutionTwoPhaseRecordRuntimeState \
	(CurrentPgExecutionTwoPhaseRecordRuntimeStateMaybe())
#define CurrentPgExecutionTriggerRuntimeState \
	(CurrentPgExecutionTriggerRuntimeStateMaybe())
#define CurrentPgExecutionRegexRuntimeState \
	(CurrentPgExecutionRegexRuntimeStateMaybe())
#define CurrentPgExecutionValgrindRuntimeState \
	(CurrentPgExecutionValgrindRuntimeStateMaybe())
#define CurrentPgExecutionSnapBuildRuntimeState \
	(CurrentPgExecutionSnapBuildRuntimeStateMaybe())
#endif

#endif							/* BACKEND_RUNTIME_CURRENT_H */
