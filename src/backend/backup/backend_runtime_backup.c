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
#include "postgres.h"

#include "utils/backend_runtime.h"
#include "../utils/init/backend_runtime_internal.h"

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
	return &PgCurrentExecutionBaseBackupState()->backup_started_in_recovery;
}

long long int *
PgCurrentBaseBackupTotalChecksumFailuresRef(void)
{
	return &PgCurrentExecutionBaseBackupState()->total_checksum_failures;
}

bool *
PgCurrentBaseBackupNoVerifyChecksumsRef(void)
{
	return &PgCurrentExecutionBaseBackupState()->noverify_checksums;
}
