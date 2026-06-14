/*-------------------------------------------------------------------------
 *
 * backend_runtime_cache.c
 *	  Runtime bridge accessors for session-owned cache state.
 *
 * These accessors keep legacy cache subsystem names mapped onto the current
 * PgSession while leaving runtime construction and top-level orchestration in
 * utils/init/backend_runtime.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/utils/cache/backend_runtime_cache.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/backend_runtime.h"
#include "utils/memutils.h"
#include "../init/backend_runtime_internal.h"

MemoryContext *
PgCacheMemoryContextRef(void)
{
	return &PgCurrentSessionCatalogLookupState()->cache_memory_context;
}

CatCache **
PgCurrentSysCacheArray(void)
{
	return PgCurrentSessionCatalogLookupState()->sys_cache;
}

bool *
PgCurrentSysCacheInitializedRef(void)
{
	return &PgCurrentSessionCatalogLookupState()->sys_cache_initialized;
}

Oid *
PgCurrentSysCacheRelationOidArray(void)
{
	return PgCurrentSessionCatalogLookupState()->sys_cache_relation_oid;
}

int *
PgCurrentSysCacheRelationOidSizeRef(void)
{
	return &PgCurrentSessionCatalogLookupState()->sys_cache_relation_oid_size;
}

Oid *
PgCurrentSysCacheSupportingRelOidArray(void)
{
	return PgCurrentSessionCatalogLookupState()->sys_cache_supporting_rel_oid;
}

int *
PgCurrentSysCacheSupportingRelOidSizeRef(void)
{
	return &PgCurrentSessionCatalogLookupState()->sys_cache_supporting_rel_oid_size;
}

CatCacheHeader **
PgCurrentCatCacheHeaderRef(void)
{
	return &PgCurrentSessionCatalogLookupState()->cat_cache_header;
}

HTAB **
PgCurrentRelationIdCacheRef(void)
{
	return &PgCurrentSessionCatalogLookupState()->relcache_relation_id_cache;
}

bool *
PgCurrentCriticalRelcachesBuiltRef(void)
{
	return &PgCurrentSessionCatalogLookupState()->relcache_critical_built;
}

bool *
PgCurrentCriticalSharedRelcachesBuiltRef(void)
{
	return &PgCurrentSessionCatalogLookupState()->relcache_critical_shared_built;
}

long *
PgCurrentRelcacheInvalsReceivedRef(void)
{
	return &PgCurrentSessionCatalogLookupState()->relcache_invals_received;
}

HTAB **
PgCurrentOpClassCacheRef(void)
{
	return &PgCurrentSessionCatalogLookupState()->relcache_opclass_cache;
}

HTAB **
PgCurrentTypeCacheHashRef(void)
{
	return &PgCurrentSessionCatalogLookupState()->typcache_type_cache_hash;
}

HTAB **
PgCurrentRelIdToTypeIdCacheHashRef(void)
{
	return &PgCurrentSessionCatalogLookupState()->typcache_relid_to_typeid_hash;
}

TypeCacheEntry **
PgCurrentFirstDomainTypeEntryRef(void)
{
	return &PgCurrentSessionCatalogLookupState()->typcache_first_domain_type_entry;
}

Oid **
PgCurrentTypCacheInProgressListRef(void)
{
	return &PgCurrentSessionCatalogLookupState()->typcache_in_progress_list;
}

int *
PgCurrentTypCacheInProgressListLenRef(void)
{
	return &PgCurrentSessionCatalogLookupState()->typcache_in_progress_list_len;
}

int *
PgCurrentTypCacheInProgressListMaxLenRef(void)
{
	return &PgCurrentSessionCatalogLookupState()->typcache_in_progress_list_maxlen;
}

HTAB **
PgCurrentRecordCacheHashRef(void)
{
	return &PgCurrentSessionCatalogLookupState()->typcache_record_cache_hash;
}

RecordCacheArrayEntry **
PgCurrentRecordCacheArrayRef(void)
{
	return &PgCurrentSessionCatalogLookupState()->typcache_record_cache_array;
}

int32 *
PgCurrentRecordCacheArrayLenRef(void)
{
	return &PgCurrentSessionCatalogLookupState()->typcache_record_cache_array_len;
}

int32 *
PgCurrentNextRecordTypmodRef(void)
{
	return &PgCurrentSessionCatalogLookupState()->typcache_next_record_typmod;
}

uint64 *
PgCurrentTupleDescIdCounterRef(void)
{
	return &PgCurrentSessionCatalogLookupState()->typcache_tupledesc_id_counter;
}

HTAB **
PgCurrentAttoptCacheHashRef(void)
{
	return &PgCurrentSessionCatalogLookupState()->attopt_cache_hash;
}

HTAB **
PgCurrentRelfilenumberMapHashRef(void)
{
	return &PgCurrentSessionCatalogLookupState()->relfilenumber_map_hash;
}

ScanKeyData *
PgCurrentRelfilenumberScanKeyArray(void)
{
	return PgCurrentSessionCatalogLookupState()->relfilenumber_skey;
}

HTAB **
PgCurrentTableSpaceCacheHashRef(void)
{
	return &PgCurrentSessionCatalogLookupState()->tablespace_cache_hash;
}

HTAB **
PgCurrentEventTriggerCacheRef(void)
{
	return &PgCurrentSessionCatalogLookupState()->event_trigger_cache;
}

MemoryContext *
PgCurrentEventTriggerCacheContextRef(void)
{
	return &PgCurrentSessionCatalogLookupState()->event_trigger_cache_context;
}

int *
PgCurrentEventTriggerCacheStateRef(void)
{
	return &PgCurrentSessionCatalogLookupState()->event_trigger_cache_state;
}

struct _SPI_plan **
PgCurrentRuleutilsRuleByOidPlanRef(void)
{
	return &PgCurrentSessionCatalogLookupState()->ruleutils_rule_by_oid_plan;
}

struct _SPI_plan **
PgCurrentRuleutilsViewRulePlanRef(void)
{
	return &PgCurrentSessionCatalogLookupState()->ruleutils_view_rule_plan;
}
