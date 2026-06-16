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

#include "executor/spi.h"
#include "utils/backend_runtime.h"
#include "utils/memutils.h"
#include "../init/backend_runtime_internal.h"

dlist_head *
PgCurrentSavedPlanListRef(void)
{
	return &PgCurrentSessionPlanCacheState()->saved_plan_list;
}

dlist_head *
PgCurrentCachedExpressionListRef(void)
{
	return &PgCurrentSessionPlanCacheState()->cached_expression_list;
}

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

MemoryContext *
PgCurrentFunctionManagerMemoryContextRef(void)
{
	return &PgCurrentSessionFunctionManagerState()->function_manager_context;
}

MemoryContext
PgCurrentFunctionManagerMemoryContext(void)
{
	MemoryContext *context = PgCurrentFunctionManagerMemoryContextRef();

	return PgRuntimeGetOwnedMemoryContextWithSizes(context,
												  "FunctionManagerMemoryContext",
												  ALLOCSET_DEFAULT_SIZES);
}

HTAB **
PgCurrentCFuncHashRef(void)
{
	return &PgCurrentSessionFunctionManagerState()->c_func_hash;
}

HTAB **
PgCurrentCachedFunctionHashRef(void)
{
	return &PgCurrentSessionFunctionManagerState()->cached_function_hash;
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

TupleDesc *
PgCurrentPgClassDescriptorRef(void)
{
	return &PgCurrentSessionCatalogLookupState()->relcache_pg_class_descriptor;
}

TupleDesc *
PgCurrentPgIndexDescriptorRef(void)
{
	return &PgCurrentSessionCatalogLookupState()->relcache_pg_index_descriptor;
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

void
PgSessionResetCatalogLookupClosedState(PgSession *session)
{
	Assert(session != NULL);

	PG_RUNTIME_DESTROY_HASH(session->catalog_lookup.attopt_cache_hash);
	PG_RUNTIME_DESTROY_HASH(session->catalog_lookup.relfilenumber_map_hash);
	MemSet(session->catalog_lookup.relfilenumber_skey, 0,
		   sizeof(session->catalog_lookup.relfilenumber_skey));
	PG_RUNTIME_DESTROY_HASH(session->catalog_lookup.tablespace_cache_hash);
	if (session->catalog_lookup.event_trigger_cache_context != NULL)
	{
		PG_RUNTIME_DELETE_MEMORY_CONTEXT(session->catalog_lookup.event_trigger_cache_context);
		session->catalog_lookup.event_trigger_cache = NULL;
	}
	else if (session->catalog_lookup.event_trigger_cache != NULL)
	{
		/*
		 * BuildEventTriggerCache() always allocates the hash under
		 * EventTriggerCacheContext.  Without that context, a remaining hash
		 * pointer is stale closed-session state rather than a valid owner.
		 */
		session->catalog_lookup.event_trigger_cache = NULL;
	}
	session->catalog_lookup.event_trigger_cache_state = 0;
	if (session->catalog_lookup.ruleutils_rule_by_oid_plan != NULL)
	{
		SPI_freeplan(session->catalog_lookup.ruleutils_rule_by_oid_plan);
		session->catalog_lookup.ruleutils_rule_by_oid_plan = NULL;
	}
	if (session->catalog_lookup.ruleutils_view_rule_plan != NULL)
	{
		SPI_freeplan(session->catalog_lookup.ruleutils_view_rule_plan);
		session->catalog_lookup.ruleutils_view_rule_plan = NULL;
	}
	if (session->catalog_lookup.cache_memory_context != NULL)
	{
		if (CurrentMemoryContext == session->catalog_lookup.cache_memory_context)
			MemoryContextSwitchTo(TopMemoryContext);
		PG_RUNTIME_DELETE_MEMORY_CONTEXT(session->catalog_lookup.cache_memory_context);
		session->catalog_lookup.cache_memory_context = NULL;
	}
	MemSet(session->catalog_lookup.sys_cache, 0,
		   sizeof(session->catalog_lookup.sys_cache));
	session->catalog_lookup.sys_cache_initialized = false;
	MemSet(session->catalog_lookup.sys_cache_relation_oid, 0,
		   sizeof(session->catalog_lookup.sys_cache_relation_oid));
	session->catalog_lookup.sys_cache_relation_oid_size = 0;
	MemSet(session->catalog_lookup.sys_cache_supporting_rel_oid, 0,
		   sizeof(session->catalog_lookup.sys_cache_supporting_rel_oid));
	session->catalog_lookup.sys_cache_supporting_rel_oid_size = 0;
	session->catalog_lookup.cat_cache_header = NULL;
	session->catalog_lookup.relcache_relation_id_cache = NULL;
	session->catalog_lookup.relcache_critical_built = false;
	session->catalog_lookup.relcache_critical_shared_built = false;
	session->catalog_lookup.relcache_invals_received = 0;
	session->catalog_lookup.relcache_pg_class_descriptor = NULL;
	session->catalog_lookup.relcache_pg_index_descriptor = NULL;
	session->catalog_lookup.relcache_opclass_cache = NULL;
	session->catalog_lookup.typcache_type_cache_hash = NULL;
	session->catalog_lookup.typcache_relid_to_typeid_hash = NULL;
	session->catalog_lookup.typcache_first_domain_type_entry = NULL;
	session->catalog_lookup.typcache_in_progress_list = NULL;
	session->catalog_lookup.typcache_in_progress_list_len = 0;
	session->catalog_lookup.typcache_in_progress_list_maxlen = 0;
	session->catalog_lookup.typcache_record_cache_hash = NULL;
	session->catalog_lookup.typcache_record_cache_array = NULL;
	session->catalog_lookup.typcache_record_cache_array_len = 0;
	session->catalog_lookup.typcache_next_record_typmod = 0;
	session->catalog_lookup.typcache_tupledesc_id_counter =
		INVALID_TUPLEDESC_IDENTIFIER;
}
