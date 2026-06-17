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

typedef enum PgRuntimeHotCurrentCellMode
{
	PG_RUNTIME_HOT_CURRENT_CELLS_FALLBACK = 0,
	PG_RUNTIME_HOT_CURRENT_CELLS_PROCESS = 1,
	PG_RUNTIME_HOT_CURRENT_CELLS_THREAD = 2
} PgRuntimeHotCurrentCellMode;

extern PGDLLIMPORT PG_GLOBAL_RUNTIME int PgRuntimeHotCurrentCellModeState;

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

#define PG_RUNTIME_HOT_BUCKET(variable, type, owner, field) \
extern PGDLLIMPORT PG_GLOBAL_RUNTIME type *variable##ProcessBucket; \
extern PGDLLIMPORT PG_THREAD_LOCAL PG_GLOBAL_CARRIER type *variable##ThreadBucket;
#include "utils/backend_runtime_hot_buckets.def"
#undef PG_RUNTIME_HOT_BUCKET

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
	type	   *slot; \
 \
	if (likely(PgRuntimeHotCurrentCellModeState == \
			   PG_RUNTIME_HOT_CURRENT_CELLS_PROCESS)) \
	{ \
		slot = name##ProcessRef; \
		if (likely(slot != NULL)) \
			return slot; \
	} \
	else if (PgRuntimeHotCurrentCellModeState == \
			 PG_RUNTIME_HOT_CURRENT_CELLS_THREAD) \
		return &PgRuntimeCurrentBridgeState.field; \
 \
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
	if (likely(PgRuntimeHotCurrentCellModeState == \
			   PG_RUNTIME_HOT_CURRENT_CELLS_PROCESS)) \
	{ \
		slot = variable##ProcessCell; \
		if (likely(slot != NULL)) \
			return slot; \
	} \
	else if (PgRuntimeHotCurrentCellModeState == \
			 PG_RUNTIME_HOT_CURRENT_CELLS_THREAD) \
	{ \
		slot = PgRuntimeCurrentBridgeState.variable; \
		if (likely(slot != NULL)) \
			return slot; \
	} \
 \
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
	return fallback(); \
}
#include "utils/backend_runtime_hot_mirrors.def"
#undef PG_RUNTIME_HOT_MIRROR

#define PG_RUNTIME_HOT_FIELD_CURRENT_OWNER_carrier CurrentPgCarrier
#define PG_RUNTIME_HOT_FIELD_CURRENT_OWNER_backend CurrentPgBackend
#define PG_RUNTIME_HOT_FIELD_CURRENT_OWNER_session CurrentPgSession
#define PG_RUNTIME_HOT_FIELD_CURRENT_OWNER_connection CurrentPgConnection
#define PG_RUNTIME_HOT_FIELD_CURRENT_OWNER_execution CurrentPgExecution
#define PG_RUNTIME_HOT_FIELD_CURRENT_OWNER_(owner) \
	PG_RUNTIME_HOT_FIELD_CURRENT_OWNER_##owner
#define PG_RUNTIME_HOT_FIELD_CURRENT_OWNER(owner) \
	PG_RUNTIME_HOT_FIELD_CURRENT_OWNER_(owner)

#define PG_RUNTIME_HOT_FIELD(variable, owner, type, expr) \
static inline type * \
variable##MaybeRef(type *(*fallback) (void)) \
{ \
	type	   *slot; \
 \
	if (likely(PgRuntimeHotCurrentCellModeState == \
			   PG_RUNTIME_HOT_CURRENT_CELLS_PROCESS)) \
	{ \
		slot = variable##ProcessRef; \
		if (likely(slot != NULL)) \
			return slot; \
	} \
	else if (PgRuntimeHotCurrentCellModeState == \
			 PG_RUNTIME_HOT_CURRENT_CELLS_THREAD) \
	{ \
		PgRuntimeCurrentBridge *bridge = &PgRuntimeCurrentBridgeState; \
 \
		slot = bridge->variable; \
		if (likely(slot != NULL)) \
			return slot; \
	} \
 \
	return fallback(); \
}
#include "utils/backend_runtime_hot_fields.def"
#undef PG_RUNTIME_HOT_FIELD

#undef PG_RUNTIME_HOT_FIELD_CURRENT_OWNER
#undef PG_RUNTIME_HOT_FIELD_CURRENT_OWNER_
#undef PG_RUNTIME_HOT_FIELD_CURRENT_OWNER_execution
#undef PG_RUNTIME_HOT_FIELD_CURRENT_OWNER_connection
#undef PG_RUNTIME_HOT_FIELD_CURRENT_OWNER_session
#undef PG_RUNTIME_HOT_FIELD_CURRENT_OWNER_backend
#undef PG_RUNTIME_HOT_FIELD_CURRENT_OWNER_carrier

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
	if (likely(PgRuntimeHotCurrentCellModeState == \
			   PG_RUNTIME_HOT_CURRENT_CELLS_PROCESS)) \
	{ \
		bucket = variable##ProcessBucket; \
		if (likely(bucket != NULL)) \
			return bucket; \
	} \
	else if (PgRuntimeHotCurrentCellModeState == \
			 PG_RUNTIME_HOT_CURRENT_CELLS_THREAD) \
	{ \
		bucket = PgRuntimeCurrentBridgeState.variable; \
		if (likely(bucket != NULL)) \
			return bucket; \
	} \
 \
	return PgRuntimeCurrentBridgeState.variable; \
}
#include "utils/backend_runtime_hot_buckets.def"
#undef PG_RUNTIME_HOT_BUCKET

#ifndef BACKEND_RUNTIME_CURRENT_NO_BUCKET_ALIASES
#define CurrentPgBackendBufferRuntimeState \
	(CurrentPgBackendBufferRuntimeStateMaybe())
#define CurrentPgBackendCoreRuntimeState \
	(CurrentPgBackendCoreRuntimeStateMaybe())
#define CurrentPgBackendExprInterpRuntimeState \
	(CurrentPgBackendExprInterpRuntimeStateMaybe())
#define CurrentPgBackendIPCRuntimeState \
	(CurrentPgBackendIPCRuntimeStateMaybe())
#define CurrentPgBackendInterruptHoldoffRuntimeState \
	(CurrentPgBackendInterruptHoldoffRuntimeStateMaybe())
#define CurrentPgBackendInstrumentationRuntimeState \
	(CurrentPgBackendInstrumentationRuntimeStateMaybe())
#define CurrentPgBackendLockRuntimeState \
	(CurrentPgBackendLockRuntimeStateMaybe())
#define CurrentPgBackendMemoryManagerRuntimeState \
	(CurrentPgBackendMemoryManagerRuntimeStateMaybe())
#define CurrentPgBackendParallelRuntimeState \
	(CurrentPgBackendParallelRuntimeStateMaybe())
#define CurrentPgBackendPendingInterruptRuntimeState \
	(CurrentPgBackendPendingInterruptRuntimeStateMaybe())
#define CurrentPgBackendPgStatPendingRuntimeState \
	(CurrentPgBackendPgStatPendingRuntimeStateMaybe())
#define CurrentPgBackendStorageRuntimeState \
	(CurrentPgBackendStorageRuntimeStateMaybe())
#define CurrentPgBackendTimeoutRuntimeState \
	(CurrentPgBackendTimeoutRuntimeStateMaybe())
#define CurrentPgBackendTransactionRuntimeState \
	(CurrentPgBackendTransactionRuntimeStateMaybe())
#define CurrentPgBackendUtilityRuntimeState \
	(CurrentPgBackendUtilityRuntimeStateMaybe())
#define CurrentPgBackendWaitRuntimeState \
	(CurrentPgBackendWaitRuntimeStateMaybe())
#define CurrentPgBackendXLogRuntimeState \
	(CurrentPgBackendXLogRuntimeStateMaybe())
#define CurrentPgConnectionProtocolRuntimeState \
	(CurrentPgConnectionProtocolRuntimeStateMaybe())
#define CurrentPgConnectionSocketIORuntimeState \
	(CurrentPgConnectionSocketIORuntimeStateMaybe())
#define CurrentPgExecutionCatalogRuntimeState \
	(CurrentPgExecutionCatalogRuntimeStateMaybe())
#define CurrentPgExecutionCatalogCacheRuntimeState \
	(CurrentPgExecutionCatalogCacheRuntimeStateMaybe())
#define CurrentPgExecutionDebugRuntimeState \
	(CurrentPgExecutionDebugRuntimeStateMaybe())
#define CurrentPgExecutionErrorRuntimeState \
	(CurrentPgExecutionErrorRuntimeStateMaybe())
#define CurrentPgExecutionSPIRuntimeState \
	(CurrentPgExecutionSPIRuntimeStateMaybe())
#define CurrentPgExecutionMemoryContextRuntimeState \
	(CurrentPgExecutionMemoryContextRuntimeStateMaybe())
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
#define CurrentPgExecutionResourceOwnerRuntimeState \
	(CurrentPgExecutionResourceOwnerRuntimeStateMaybe())
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
#define CurrentPgSessionCatalogLookupRuntimeState \
	(CurrentPgSessionCatalogLookupRuntimeStateMaybe())
#define CurrentPgSessionDateTimeRuntimeState \
	(CurrentPgSessionDateTimeRuntimeStateMaybe())
#define CurrentPgSessionXactDefaultRuntimeState \
	(CurrentPgSessionXactDefaultRuntimeStateMaybe())
#define CurrentPgSessionLockWaitRuntimeState \
	(CurrentPgSessionLockWaitRuntimeStateMaybe())
#define CurrentPgSessionParserRuntimeState \
	(CurrentPgSessionParserRuntimeStateMaybe())
#define CurrentPgSessionVacuumRuntimeState \
	(CurrentPgSessionVacuumRuntimeStateMaybe())
#define CurrentPgSessionRegexRuntimeState \
	(CurrentPgSessionRegexRuntimeStateMaybe())
#define CurrentPgSessionRIGlobalsRuntimeState \
	(CurrentPgSessionRIGlobalsRuntimeStateMaybe())
#define CurrentPgSessionEncodingRuntimeState \
	(CurrentPgSessionEncodingRuntimeStateMaybe())
#define CurrentPgSessionPortalManagerRuntimeState \
	(CurrentPgSessionPortalManagerRuntimeStateMaybe())
#define CurrentPgSessionGUCRuntimeState \
	(CurrentPgSessionGUCRuntimeStateMaybe())
#define CurrentPgSessionLocaleRuntimeState \
	(CurrentPgSessionLocaleRuntimeStateMaybe())
#define CurrentPgSessionLoggingRuntimeState \
	(CurrentPgSessionLoggingRuntimeStateMaybe())
#define CurrentPgSessionLoopRuntimeState \
	(CurrentPgSessionLoopRuntimeStateMaybe())
#define CurrentPgSessionMiscGUCRuntimeState \
	(CurrentPgSessionMiscGUCRuntimeStateMaybe())
#define CurrentPgSessionNamespaceRuntimeState \
	(CurrentPgSessionNamespaceRuntimeStateMaybe())
#define CurrentPgSessionPgStatRuntimeState \
	(CurrentPgSessionPgStatRuntimeStateMaybe())
#define CurrentPgSessionPlannerCostRuntimeState \
	(CurrentPgSessionPlannerCostRuntimeStateMaybe())
#define CurrentPgSessionPlannerMethodRuntimeState \
	(CurrentPgSessionPlannerMethodRuntimeStateMaybe())
#define CurrentPgSessionQueryMemoryRuntimeState \
	(CurrentPgSessionQueryMemoryRuntimeStateMaybe())
#endif

#endif							/* BACKEND_RUNTIME_CURRENT_H */
