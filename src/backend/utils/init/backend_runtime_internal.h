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

extern PgSessionCatalogLookupState *PgCurrentSessionCatalogLookupState(void);
extern PgSessionConnectionGUCState *PgCurrentSessionConnectionGUCState(void);
extern PgSessionDateTimeState *PgCurrentSessionDateTimeState(void);
extern PgSessionEncodingState *PgCurrentSessionEncodingState(void);
extern PgSessionFunctionManagerState *PgCurrentSessionFunctionManagerState(void);
extern PgSessionGeneralGUCState *PgCurrentSessionGeneralGUCState(void);
extern PgSessionJitProviderState *PgCurrentSessionJitProviderState(void);
extern PgSessionPgStatState *PgCurrentSessionPgStatState(void);
extern PgBackendPgStatPendingState *PgCurrentBackendPgStatPendingState(void);

#endif							/* BACKEND_RUNTIME_INTERNAL_H */
