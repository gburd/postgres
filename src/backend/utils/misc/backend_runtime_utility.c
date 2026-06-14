/*-------------------------------------------------------------------------
 *
 * backend_runtime_utility.c
 *	  Runtime bridge accessors for backend-local utility state.
 *
 * These accessors keep utility, formatting, sampling, superuser, and
 * resource-owner compatibility globals mapped onto the current PgBackend
 * while leaving runtime construction and early fallback ownership in
 * utils/init/backend_runtime.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/utils/misc/backend_runtime_utility.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/backend_runtime.h"
#include "../init/backend_runtime_internal.h"

HTAB **
PgCurrentSeqScanTables(void)
{
	return PgCurrentBackendUtilityState()->seq_scan_tables;
}

char **
PgCurrentStackBasePtrRef(void)
{
	return &PgCurrentCarrierState()->stack_base_ptr;
}

int *
PgCurrentSeqScanLevels(void)
{
	return PgCurrentBackendUtilityState()->seq_scan_levels;
}

int *
PgCurrentNumSeqScansRef(void)
{
	return &PgCurrentBackendUtilityState()->num_seq_scans;
}

volatile sig_atomic_t *
PgCurrentNotifyInterruptPendingRef(void)
{
	return &PgCurrentBackendUtilityState()->notify_interrupt_pending;
}

bool *
PgCurrentAsyncUnlistenExitRegisteredRef(void)
{
	return &PgCurrentBackendUtilityState()->async_unlisten_exit_registered;
}

struct ExtensionSiblingCache **
PgCurrentExtensionSiblingListRef(void)
{
	return &PgCurrentBackendUtilityState()->extension_sibling_list;
}

HTAB **
PgCurrentInjectionPointCacheRef(void)
{
	return &PgCurrentBackendUtilityState()->injection_point_cache;
}

ReservoirStateData *
PgCurrentSamplingOldReservoirRef(void)
{
	return &PgCurrentBackendUtilityState()->sampling_old_reservoir;
}

bool *
PgCurrentSamplingOldReservoirInitializedRef(void)
{
	return &PgCurrentBackendUtilityState()->sampling_old_reservoir_initialized;
}

Oid *
PgCurrentSuperuserLastRoleIdRef(void)
{
	return &PgCurrentBackendUtilityState()->superuser_last_roleid;
}

bool *
PgCurrentSuperuserLastRoleIdIsSuperRef(void)
{
	return &PgCurrentBackendUtilityState()->superuser_last_roleid_is_super;
}

bool *
PgCurrentSuperuserRoleIdCallbackRegisteredRef(void)
{
	return &PgCurrentBackendUtilityState()->superuser_roleid_callback_registered;
}

void **
PgCurrentResourceReleaseCallbacksRef(void)
{
	return &PgCurrentBackendUtilityState()->resource_release_callbacks;
}

#ifdef RESOWNER_STATS
int *
PgCurrentResourceOwnerArrayLookupsRef(void)
{
	return &PgCurrentBackendUtilityState()->resource_owner_array_lookups;
}

int *
PgCurrentResourceOwnerHashLookupsRef(void)
{
	return &PgCurrentBackendUtilityState()->resource_owner_hash_lookups;
}
#endif

const void **
PgCurrentDateTokenCache(void)
{
	return PgCurrentBackendUtilityState()->date_cache;
}

const void **
PgCurrentDeltaTokenCache(void)
{
	return PgCurrentBackendUtilityState()->delta_cache;
}

bool *
PgCurrentDegreeConstsSetRef(void)
{
	return &PgCurrentBackendUtilityState()->degree_consts_set;
}

float8 *
PgCurrentDegreeSin30Ref(void)
{
	return &PgCurrentBackendUtilityState()->degree_sin_30;
}

float8 *
PgCurrentDegreeOneMinusCos60Ref(void)
{
	return &PgCurrentBackendUtilityState()->degree_one_minus_cos_60;
}

float8 *
PgCurrentDegreeAsin05Ref(void)
{
	return &PgCurrentBackendUtilityState()->degree_asin_0_5;
}

float8 *
PgCurrentDegreeAcos05Ref(void)
{
	return &PgCurrentBackendUtilityState()->degree_acos_0_5;
}

float8 *
PgCurrentDegreeAtan10Ref(void)
{
	return &PgCurrentBackendUtilityState()->degree_atan_1_0;
}

float8 *
PgCurrentDegreeTan45Ref(void)
{
	return &PgCurrentBackendUtilityState()->degree_tan_45;
}

float8 *
PgCurrentDegreeCot45Ref(void)
{
	return &PgCurrentBackendUtilityState()->degree_cot_45;
}

void **
PgCurrentDCHCache(void)
{
	return PgCurrentBackendUtilityState()->dch_cache;
}

int *
PgCurrentNumDCHCacheRef(void)
{
	return &PgCurrentBackendUtilityState()->n_dch_cache;
}

int *
PgCurrentDCHCounterRef(void)
{
	return &PgCurrentBackendUtilityState()->dch_counter;
}

void **
PgCurrentNUMCache(void)
{
	return PgCurrentBackendUtilityState()->num_cache;
}

int *
PgCurrentNumNUMCacheRef(void)
{
	return &PgCurrentBackendUtilityState()->n_num_cache;
}

int *
PgCurrentNUMCounterRef(void)
{
	return &PgCurrentBackendUtilityState()->num_counter;
}

MemoryContext *
PgCurrentLibxmlContextRef(void)
{
	return &PgCurrentBackendUtilityState()->libxml_context;
}

HTAB **
PgCurrentMissingAttrCacheRef(void)
{
	return &PgCurrentBackendUtilityState()->missing_attr_cache;
}
