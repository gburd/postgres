/*-------------------------------------------------------------------------
 *
 * backend_runtime_mb.c
 *	  Runtime bridge accessors for session encoding state.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/utils/mb/backend_runtime_mb.c
 *
 *-------------------------------------------------------------------------
 */
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include "mb/pg_wchar.h"
#include "utils/backend_runtime.h"
#include "utils/memutils.h"
#include "../init/backend_runtime_internal.h"

PgSessionEncodingState *
PgCurrentSessionEncodingState(void)
{
	PgSessionEncodingState *encoding;

	if (likely(CurrentPgSessionEncodingRuntimeState != NULL &&
			   CurrentPgSessionEncodingRuntimeState->client_encoding != NULL))
		return CurrentPgSessionEncodingRuntimeState;

	encoding = &PgCurrentOrEarlySession()->encoding;
	if (encoding->client_encoding == NULL)
		PgSessionInitializeEncodingState(encoding);

	return encoding;
}

List **
PgCurrentEncodingConvProcListRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR_INITIALIZED_BY(CurrentPgSessionEncodingRuntimeState, PgCurrentSessionEncodingState, client_encoding)->conv_proc_list;
}

MemoryContext
PgCurrentEncodingCacheMemoryContext(void)
{
	PgSessionEncodingState *encoding;

	encoding = PgCurrentSessionEncodingState();

	return PgRuntimeGetOwnedMemoryContext(&encoding->encoding_cache_context,
										  "encoding conversion cache");
}

FmgrInfo **
PgCurrentToServerConvProcRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR_INITIALIZED_BY(CurrentPgSessionEncodingRuntimeState, PgCurrentSessionEncodingState, client_encoding)->to_server_conv_proc;
}

FmgrInfo **
PgCurrentToClientConvProcRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR_INITIALIZED_BY(CurrentPgSessionEncodingRuntimeState, PgCurrentSessionEncodingState, client_encoding)->to_client_conv_proc;
}

FmgrInfo **
PgCurrentUtf8ToServerConvProcRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR_INITIALIZED_BY(CurrentPgSessionEncodingRuntimeState, PgCurrentSessionEncodingState, client_encoding)->utf8_to_server_conv_proc;
}

const pg_enc2name **
PgCurrentClientEncodingRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR_INITIALIZED_BY(CurrentPgSessionEncodingRuntimeState, PgCurrentSessionEncodingState, client_encoding)->client_encoding;
}

const pg_enc2name **
PgCurrentDatabaseEncodingRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR_INITIALIZED_BY(CurrentPgSessionEncodingRuntimeState, PgCurrentSessionEncodingState, client_encoding)->database_encoding;
}

const pg_enc2name **
PgCurrentMessageEncodingRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR_INITIALIZED_BY(CurrentPgSessionEncodingRuntimeState, PgCurrentSessionEncodingState, client_encoding)->message_encoding;
}

bool *
PgCurrentEncodingStartupCompleteRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR_INITIALIZED_BY(CurrentPgSessionEncodingRuntimeState, PgCurrentSessionEncodingState, client_encoding)->backend_startup_complete;
}

int *
PgCurrentPendingClientEncodingRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR_INITIALIZED_BY(CurrentPgSessionEncodingRuntimeState, PgCurrentSessionEncodingState, client_encoding)->pending_client_encoding;
}
