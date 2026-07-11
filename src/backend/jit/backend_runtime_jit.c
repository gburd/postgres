/*-------------------------------------------------------------------------
 *
 * backend_runtime_jit.c
 *	  Runtime bridge accessors for session-owned JIT provider state.
 *
 * These accessors keep provider-independent JIT cache state mapped onto the
 * current PgSession while leaving runtime construction and top-level lifecycle
 * orchestration in utils/init/backend_runtime.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/jit/backend_runtime_jit.c
 *
 *-------------------------------------------------------------------------
 */
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include "jit/jit.h"
#include "utils/backend_runtime.h"
#include "../utils/init/backend_runtime_internal.h"

JitProviderCallbacks *
PgCurrentJitProviderCallbacksRef(void)
{
	return &PgCurrentSessionJitProviderState()->provider;
}

bool *
PgCurrentJitProviderSuccessfullyLoadedRef(void)
{
	return &PgCurrentSessionJitProviderState()->provider_successfully_loaded;
}

bool *
PgCurrentJitProviderFailedLoadingRef(void)
{
	return &PgCurrentSessionJitProviderState()->provider_failed_loading;
}

#ifdef USE_LLVM
PgSessionLLVMJitState *
PgCurrentLLVMJitState(void)
{
	return PgCurrentSessionLLVMJitState();
}
#endif
