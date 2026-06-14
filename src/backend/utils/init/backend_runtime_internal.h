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
extern PgSessionNamespaceState *PgCurrentSessionNamespaceState(void);
extern PgSessionLocaleState *PgCurrentSessionLocaleState(void);
extern PgSessionExtensionModuleState *PgCurrentSessionExtensionModuleState(void);
extern PgSessionInvalidationCallbackState *PgCurrentSessionInvalidationCallbackState(void);
extern PgSessionRIGlobalsState *PgCurrentSessionRIGlobalsState(void);
extern PgSessionRelMapState *PgCurrentSessionRelMapState(void);
extern PgSessionPreparedStatementState *PgCurrentSessionPreparedStatementState(void);
extern PgSessionOnCommitState *PgCurrentSessionOnCommitState(void);
extern PgSessionSequenceState *PgCurrentSessionSequenceState(void);
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
extern PgSessionSortGUCState *PgCurrentSessionSortGUCState(void);
extern PgSessionQueryMemoryState *PgCurrentSessionQueryMemoryState(void);
extern PgSessionPlannerCostState *PgCurrentSessionPlannerCostState(void);
extern PgSessionPlannerMethodState *PgCurrentSessionPlannerMethodState(void);
extern PgSessionPgStatState *PgCurrentSessionPgStatState(void);
extern PgSessionMiscGUCState *PgCurrentSessionMiscGUCState(void);
extern PgSessionGUCState *PgCurrentSessionGUCState(void);
extern PgExecutionGUCErrorState *PgCurrentExecutionGUCErrorState(void);
extern PgConnectionIdentityState *PgConnectionIdentityStateRef(PgConnection *connection);
extern PgConnectionSocketIOState *PgConnectionSocketIOStateRef(PgConnection *connection);
extern PgConnectionProtocolState *PgConnectionProtocolStateRef(PgConnection *connection);
extern PgConnectionOutputState *PgConnectionOutputStateRef(PgConnection *connection);
extern PgConnectionInterruptState *PgConnectionInterruptStateRef(PgConnection *connection);
extern PgConnectionStartupState *PgConnectionStartupStateRef(PgConnection *connection);
extern PgConnectionClientConnectionInfoState *PgConnectionClientConnectionInfoStateRef(PgConnection *connection);
extern bool *PgConnectionClientConnectionInfoAuthnIdOwnedRef(PgConnection *connection);
extern PgConnectionSecurityState *PgConnectionRuntimeSecurityStateRef(PgConnection *connection);
extern PgBackendBufferState *PgCurrentBackendBufferState(void);
extern MemoryContext PgBackendBufferAllocationContext(void);
extern PgBackendStorageState *PgCurrentBackendStorageState(void);
extern PgBackendLockState *PgCurrentBackendLockState(void);
extern PgBackendIPCState *PgCurrentBackendIPCState(void);
extern PgBackendPgStatPendingState *PgCurrentBackendPgStatPendingState(void);
extern PgBackendUtilityState *PgCurrentBackendUtilityState(void);
extern PgBackendParallelState *PgCurrentBackendParallelState(void);

#endif							/* BACKEND_RUNTIME_INTERNAL_H */
