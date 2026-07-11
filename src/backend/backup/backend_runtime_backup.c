/*-------------------------------------------------------------------------
 *
 * backend_runtime_backup.c
 *	  Runtime bridge accessors for backup-owned session state.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/backup/backend_runtime_backup.c
 *
 *-------------------------------------------------------------------------
 */
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include "utils/backend_runtime.h"
#include "../utils/init/backend_runtime_internal.h"

PgExecutionBaseBackupState *
PgCurrentExecutionBaseBackupState(void)
{
	PG_RUNTIME_RETURN_CURRENT_EXECUTION_BUCKET(CurrentPgExecutionBaseBackupRuntimeState,
											   basebackup);
}

struct BackupState **
PgCurrentBackupStateRef(void)
{
	return &PgCurrentSessionBackupState()->backup_state;
}

StringInfo *
PgCurrentTablespaceMapRef(void)
{
	return &PgCurrentSessionBackupState()->tablespace_map;
}

MemoryContext *
PgCurrentBackupContextRef(void)
{
	return &PgCurrentSessionBackupState()->backup_context;
}

uint8 *
PgCurrentSessionBackupStateRef(void)
{
	return &PgCurrentSessionBackupState()->session_backup_state;
}

bool *
PgCurrentBaseBackupStartedInRecoveryRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionBaseBackupRuntimeState, PgCurrentExecutionBaseBackupState)->backup_started_in_recovery;
}

long long int *
PgCurrentBaseBackupTotalChecksumFailuresRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionBaseBackupRuntimeState, PgCurrentExecutionBaseBackupState)->total_checksum_failures;
}

bool *
PgCurrentBaseBackupNoVerifyChecksumsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionBaseBackupRuntimeState, PgCurrentExecutionBaseBackupState)->noverify_checksums;
}
