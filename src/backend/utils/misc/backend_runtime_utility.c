/*-------------------------------------------------------------------------
 *
 * backend_runtime_utility.c
 *	  Runtime bridge accessors for backend-local utility state.
 *
 * These accessors keep miscadmin core, utility, formatting, sampling,
 * superuser, and resource-owner compatibility globals mapped onto the current
 * PgBackend while leaving runtime construction and early fallback ownership in
 * utils/init/backend_runtime.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/utils/misc/backend_runtime_utility.c
 *
 *-------------------------------------------------------------------------
 */
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include "utils/backend_runtime.h"
#include "utils/memutils.h"
#include "../init/backend_runtime_internal.h"

bool *
PgCurrentExitOnAnyErrorRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendCoreRuntimeState, PgCurrentCoreState)->exit_on_any_error;
}

int *
PgCurrentMyProcPidRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendCoreRuntimeState, PgCurrentCoreState)->proc_pid;
}

pg_time_t *
PgCurrentMyStartTimeRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendCoreRuntimeState, PgCurrentCoreState)->start_time;
}

TimestampTz *
PgCurrentMyStartTimestampRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendCoreRuntimeState, PgCurrentCoreState)->start_timestamp;
}

struct Latch **
PgCurrentMyLatchRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendCoreRuntimeState, PgCurrentCoreState)->latch;
}

int *
PgCurrentMyPMChildSlotRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendCoreRuntimeState, PgCurrentCoreState)->pm_child_slot;
}

char *
PgCurrentOutputFileNameRef(void)
{
	return PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendCoreRuntimeState, PgCurrentCoreState)->output_file_name;
}

ProcessingMode *
PgCurrentProcessingModeRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendCoreRuntimeState, PgCurrentCoreState)->mode;
}

bool *
PgCurrentIgnoreSystemIndexesRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendCoreRuntimeState, PgCurrentCoreState)->ignore_system_indexes;
}

HTAB **
PgCurrentSeqScanTables(void)
{
	return PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendUtilityRuntimeState, PgCurrentBackendUtilityState)->seq_scan_tables;
}

char **
PgCurrentStackBasePtrRef(void)
{
	return &PgCurrentCarrierState()->stack_base_ptr;
}

int *
PgCurrentSeqScanLevels(void)
{
	return PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendUtilityRuntimeState, PgCurrentBackendUtilityState)->seq_scan_levels;
}

int *
PgCurrentNumSeqScansRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendUtilityRuntimeState, PgCurrentBackendUtilityState)->num_seq_scans;
}

volatile sig_atomic_t *
PgCurrentNotifyInterruptPendingRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendUtilityRuntimeState, PgCurrentBackendUtilityState)->notify_interrupt_pending;
}

bool *
PgCurrentAsyncUnlistenExitRegisteredRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendUtilityRuntimeState, PgCurrentBackendUtilityState)->async_unlisten_exit_registered;
}

dshash_table **
PgCurrentAsyncGlobalChannelTableRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendUtilityRuntimeState, PgCurrentBackendUtilityState)->async_global_channel_table;
}

struct dsa_area **
PgCurrentAsyncGlobalChannelDSARef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendUtilityRuntimeState, PgCurrentBackendUtilityState)->async_global_channel_dsa;
}

struct ExtensionSiblingCache **
PgCurrentExtensionSiblingListRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendUtilityRuntimeState, PgCurrentBackendUtilityState)->extension_sibling_list;
}

HTAB **
PgCurrentInjectionPointCacheRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendUtilityRuntimeState, PgCurrentBackendUtilityState)->injection_point_cache;
}

MemoryContext
PgCurrentUtilityCacheMemoryContext(void)
{
	PgBackendUtilityState *utility = PgCurrentBackendUtilityState();

	return PgRuntimeGetOwnedMemoryContext(&utility->utility_cache_context,
										  "utility cache backend state");
}

ReservoirStateData *
PgCurrentSamplingOldReservoirRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendUtilityRuntimeState, PgCurrentBackendUtilityState)->sampling_old_reservoir;
}

bool *
PgCurrentSamplingOldReservoirInitializedRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendUtilityRuntimeState, PgCurrentBackendUtilityState)->sampling_old_reservoir_initialized;
}

Oid *
PgCurrentSuperuserLastRoleIdRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendUtilityRuntimeState, PgCurrentBackendUtilityState)->superuser_last_roleid;
}

bool *
PgCurrentSuperuserLastRoleIdIsSuperRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendUtilityRuntimeState, PgCurrentBackendUtilityState)->superuser_last_roleid_is_super;
}

bool *
PgCurrentSuperuserRoleIdCallbackRegisteredRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendUtilityRuntimeState, PgCurrentBackendUtilityState)->superuser_roleid_callback_registered;
}

void **
PgCurrentResourceReleaseCallbacksRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendUtilityRuntimeState, PgCurrentBackendUtilityState)->resource_release_callbacks;
}

#ifdef RESOWNER_STATS
int *
PgCurrentResourceOwnerArrayLookupsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendUtilityRuntimeState, PgCurrentBackendUtilityState)->resource_owner_array_lookups;
}

int *
PgCurrentResourceOwnerHashLookupsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendUtilityRuntimeState, PgCurrentBackendUtilityState)->resource_owner_hash_lookups;
}
#endif

const void **
PgCurrentDateTokenCache(void)
{
	return PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendUtilityRuntimeState, PgCurrentBackendUtilityState)->date_cache;
}

const void **
PgCurrentDeltaTokenCache(void)
{
	return PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendUtilityRuntimeState, PgCurrentBackendUtilityState)->delta_cache;
}

bool *
PgCurrentDegreeConstsSetRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendUtilityRuntimeState, PgCurrentBackendUtilityState)->degree_consts_set;
}

float8 *
PgCurrentDegreeSin30Ref(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendUtilityRuntimeState, PgCurrentBackendUtilityState)->degree_sin_30;
}

float8 *
PgCurrentDegreeOneMinusCos60Ref(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendUtilityRuntimeState, PgCurrentBackendUtilityState)->degree_one_minus_cos_60;
}

float8 *
PgCurrentDegreeAsin05Ref(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendUtilityRuntimeState, PgCurrentBackendUtilityState)->degree_asin_0_5;
}

float8 *
PgCurrentDegreeAcos05Ref(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendUtilityRuntimeState, PgCurrentBackendUtilityState)->degree_acos_0_5;
}

float8 *
PgCurrentDegreeAtan10Ref(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendUtilityRuntimeState, PgCurrentBackendUtilityState)->degree_atan_1_0;
}

float8 *
PgCurrentDegreeTan45Ref(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendUtilityRuntimeState, PgCurrentBackendUtilityState)->degree_tan_45;
}

float8 *
PgCurrentDegreeCot45Ref(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendUtilityRuntimeState, PgCurrentBackendUtilityState)->degree_cot_45;
}

void **
PgCurrentDCHCache(void)
{
	return PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendUtilityRuntimeState, PgCurrentBackendUtilityState)->dch_cache;
}

int *
PgCurrentNumDCHCacheRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendUtilityRuntimeState, PgCurrentBackendUtilityState)->n_dch_cache;
}

int *
PgCurrentDCHCounterRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendUtilityRuntimeState, PgCurrentBackendUtilityState)->dch_counter;
}

void **
PgCurrentNUMCache(void)
{
	return PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendUtilityRuntimeState, PgCurrentBackendUtilityState)->num_cache;
}

int *
PgCurrentNumNUMCacheRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendUtilityRuntimeState, PgCurrentBackendUtilityState)->n_num_cache;
}

int *
PgCurrentNUMCounterRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendUtilityRuntimeState, PgCurrentBackendUtilityState)->num_counter;
}

MemoryContext
PgCurrentFormatCacheMemoryContext(void)
{
	PgBackendUtilityState *utility = PgCurrentBackendUtilityState();

	return PgRuntimeGetOwnedMemoryContext(&utility->format_cache_context,
										  "format cache backend state");
}

MemoryContext *
PgCurrentLibxmlContextRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendUtilityRuntimeState, PgCurrentBackendUtilityState)->libxml_context;
}

HTAB **
PgCurrentMissingAttrCacheRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendUtilityRuntimeState, PgCurrentBackendUtilityState)->missing_attr_cache;
}
