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
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include "utils/backend_runtime.h"
#include "../init/backend_runtime_internal.h"

PgExecutionSnapshotState *
PgCurrentExecutionSnapshotState(void)
{
	if (likely(CurrentPgExecutionSnapshotRuntimeState != NULL))
		return CurrentPgExecutionSnapshotRuntimeState;

	return &PgCurrentOrEarlyExecution()->snapshot;
}

PgExecutionComboCidState *
PgCurrentExecutionComboCidState(void)
{
	PG_RUNTIME_RETURN_CURRENT_EXECUTION_BUCKET(CurrentPgExecutionComboCidRuntimeState,
											   combo_cid);
}

SnapshotData *
PgCurrentSnapshotDataRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionSnapshotRuntimeState, PgCurrentExecutionSnapshotState)->current_snapshot_data;
}

SnapshotData *
PgCurrentSecondarySnapshotDataRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionSnapshotRuntimeState, PgCurrentExecutionSnapshotState)->secondary_snapshot_data;
}

SnapshotData *
PgCurrentCatalogSnapshotDataRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionSnapshotRuntimeState, PgCurrentExecutionSnapshotState)->catalog_snapshot_data;
}

Snapshot *
PgCurrentSnapshotRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionSnapshotRuntimeState, PgCurrentExecutionSnapshotState)->current_snapshot;
}

Snapshot *
PgCurrentSecondarySnapshotRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionSnapshotRuntimeState, PgCurrentExecutionSnapshotState)->secondary_snapshot;
}

Snapshot *
PgCurrentCatalogSnapshotRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionSnapshotRuntimeState, PgCurrentExecutionSnapshotState)->catalog_snapshot;
}

Snapshot *
PgCurrentHistoricSnapshotRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionSnapshotRuntimeState, PgCurrentExecutionSnapshotState)->historic_snapshot;
}

TransactionId *
PgCurrentTransactionXminRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionSnapshotRuntimeState, PgCurrentExecutionSnapshotState)->transaction_xmin;
}

TransactionId *
PgCurrentRecentXminRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionSnapshotRuntimeState, PgCurrentExecutionSnapshotState)->recent_xmin;
}

HTAB **
PgCurrentTupleCidDataRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionSnapshotRuntimeState, PgCurrentExecutionSnapshotState)->tuplecid_data;
}

void **
PgCurrentActiveSnapshotRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionSnapshotRuntimeState, PgCurrentExecutionSnapshotState)->active_snapshot;
}

pairingheap *
PgCurrentRegisteredSnapshotsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionSnapshotRuntimeState, PgCurrentExecutionSnapshotState)->registered_snapshots;
}

bool *
PgCurrentFirstSnapshotSetRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionSnapshotRuntimeState, PgCurrentExecutionSnapshotState)->first_snapshot_set;
}

Snapshot *
PgCurrentFirstXactSnapshotRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionSnapshotRuntimeState, PgCurrentExecutionSnapshotState)->first_xact_snapshot;
}

List **
PgCurrentExportedSnapshotsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionSnapshotRuntimeState, PgCurrentExecutionSnapshotState)->exported_snapshots;
}

HTAB **
PgCurrentComboCidHashRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionComboCidRuntimeState, PgCurrentExecutionComboCidState)->hash;
}

void **
PgCurrentComboCidsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionComboCidRuntimeState, PgCurrentExecutionComboCidState)->cids;
}

int *
PgCurrentUsedComboCidsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionComboCidRuntimeState, PgCurrentExecutionComboCidState)->used;
}

int *
PgCurrentSizeComboCidsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionComboCidRuntimeState, PgCurrentExecutionComboCidState)->size;
}
