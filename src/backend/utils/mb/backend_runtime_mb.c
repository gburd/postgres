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
#include "postgres.h"

#include "mb/pg_wchar.h"
#include "utils/backend_runtime.h"
#include "utils/memutils.h"
#include "../init/backend_runtime_internal.h"

List **
PgCurrentEncodingConvProcListRef(void)
{
	return &PgCurrentSessionEncodingState()->conv_proc_list;
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
	return &PgCurrentSessionEncodingState()->to_server_conv_proc;
}

FmgrInfo **
PgCurrentToClientConvProcRef(void)
{
	return &PgCurrentSessionEncodingState()->to_client_conv_proc;
}

FmgrInfo **
PgCurrentUtf8ToServerConvProcRef(void)
{
	return &PgCurrentSessionEncodingState()->utf8_to_server_conv_proc;
}

const pg_enc2name **
PgCurrentClientEncodingRef(void)
{
	return &PgCurrentSessionEncodingState()->client_encoding;
}

const pg_enc2name **
PgCurrentDatabaseEncodingRef(void)
{
	return &PgCurrentSessionEncodingState()->database_encoding;
}

const pg_enc2name **
PgCurrentMessageEncodingRef(void)
{
	return &PgCurrentSessionEncodingState()->message_encoding;
}

bool *
PgCurrentEncodingStartupCompleteRef(void)
{
	return &PgCurrentSessionEncodingState()->backend_startup_complete;
}

int *
PgCurrentPendingClientEncodingRef(void)
{
	return &PgCurrentSessionEncodingState()->pending_client_encoding;
}
