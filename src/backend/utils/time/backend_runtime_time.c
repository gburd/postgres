/*-------------------------------------------------------------------------
 *
 * backend_runtime_time.c
 *	  Runtime bridge accessors for snapshot and combo-CID state.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/utils/time/backend_runtime_time.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/backend_runtime.h"
#include "../init/backend_runtime_internal.h"

PgExecutionSnapshotState *
PgCurrentExecutionSnapshotState(void)
{
	return &PgCurrentOrEarlyExecution()->snapshot;
}

PgExecutionComboCidState *
PgCurrentExecutionComboCidState(void)
{
	return &PgCurrentOrEarlyExecution()->combo_cid;
}

SnapshotData *
PgCurrentSnapshotDataRef(void)
{
	return &PgCurrentExecutionSnapshotState()->current_snapshot_data;
}

SnapshotData *
PgCurrentSecondarySnapshotDataRef(void)
{
	return &PgCurrentExecutionSnapshotState()->secondary_snapshot_data;
}

SnapshotData *
PgCurrentCatalogSnapshotDataRef(void)
{
	return &PgCurrentExecutionSnapshotState()->catalog_snapshot_data;
}

Snapshot *
PgCurrentSnapshotRef(void)
{
	return &PgCurrentExecutionSnapshotState()->current_snapshot;
}

Snapshot *
PgCurrentSecondarySnapshotRef(void)
{
	return &PgCurrentExecutionSnapshotState()->secondary_snapshot;
}

Snapshot *
PgCurrentCatalogSnapshotRef(void)
{
	return &PgCurrentExecutionSnapshotState()->catalog_snapshot;
}

Snapshot *
PgCurrentHistoricSnapshotRef(void)
{
	return &PgCurrentExecutionSnapshotState()->historic_snapshot;
}

TransactionId *
PgCurrentTransactionXminRef(void)
{
	return &PgCurrentExecutionSnapshotState()->transaction_xmin;
}

TransactionId *
PgCurrentRecentXminRef(void)
{
	return &PgCurrentExecutionSnapshotState()->recent_xmin;
}

HTAB **
PgCurrentTupleCidDataRef(void)
{
	return &PgCurrentExecutionSnapshotState()->tuplecid_data;
}

void **
PgCurrentActiveSnapshotRef(void)
{
	return &PgCurrentExecutionSnapshotState()->active_snapshot;
}

pairingheap *
PgCurrentRegisteredSnapshotsRef(void)
{
	return &PgCurrentExecutionSnapshotState()->registered_snapshots;
}

bool *
PgCurrentFirstSnapshotSetRef(void)
{
	return &PgCurrentExecutionSnapshotState()->first_snapshot_set;
}

Snapshot *
PgCurrentFirstXactSnapshotRef(void)
{
	return &PgCurrentExecutionSnapshotState()->first_xact_snapshot;
}

List **
PgCurrentExportedSnapshotsRef(void)
{
	return &PgCurrentExecutionSnapshotState()->exported_snapshots;
}

HTAB **
PgCurrentComboCidHashRef(void)
{
	return &PgCurrentExecutionComboCidState()->hash;
}

void **
PgCurrentComboCidsRef(void)
{
	return &PgCurrentExecutionComboCidState()->cids;
}

int *
PgCurrentUsedComboCidsRef(void)
{
	return &PgCurrentExecutionComboCidState()->used;
}

int *
PgCurrentSizeComboCidsRef(void)
{
	return &PgCurrentExecutionComboCidState()->size;
}
