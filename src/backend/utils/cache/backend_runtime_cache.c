/*-------------------------------------------------------------------------
 *
 * backend_runtime_cache.c
 *	  Runtime bridge accessors for cache state.
 *
 * These accessors keep legacy cache subsystem names mapped onto the current
 * PgSession/PgExecution while leaving runtime construction and top-level
 * orchestration in utils/init/backend_runtime.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/utils/cache/backend_runtime_cache.c
 *
 *-------------------------------------------------------------------------
 */
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include "executor/spi.h"
#include "utils/backend_runtime.h"
#include "utils/memutils.h"
#include "../init/backend_runtime_internal.h"

PgExecutionCatalogState *
PgCurrentExecutionCatalogState(void)
{
	if (likely(CurrentPgExecutionCatalogRuntimeState != NULL))
		return CurrentPgExecutionCatalogRuntimeState;

	return &PgCurrentOrEarlyExecution()->catalog;
}

PgExecutionCatalogCacheState *
PgCurrentExecutionCatalogCacheState(void)
{
	if (likely(CurrentPgExecutionCatalogCacheRuntimeState != NULL))
		return CurrentPgExecutionCatalogCacheRuntimeState;

	return &PgCurrentOrEarlyExecution()->catalog_cache;
}

PgExecutionRelMapState *
PgCurrentExecutionRelMapState(void)
{
	PG_RUNTIME_RETURN_CURRENT_EXECUTION_BUCKET(CurrentPgExecutionRelMapRuntimeState,
											   relmap);
}

PgExecutionInvalidationState *
PgCurrentExecutionInvalidationState(void)
{
	PG_RUNTIME_RETURN_CURRENT_EXECUTION_BUCKET(CurrentPgExecutionInvalidationRuntimeState,
											   invalidation);
}

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
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionCatalogLookupRuntimeState, PgCurrentSessionCatalogLookupState)->cache_memory_context;
}

CatCache **
PgCurrentSysCacheArray(void)
{
	return PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionCatalogLookupRuntimeState, PgCurrentSessionCatalogLookupState)->sys_cache;
}

bool *
PgCurrentSysCacheInitializedRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionCatalogLookupRuntimeState, PgCurrentSessionCatalogLookupState)->sys_cache_initialized;
}

Oid *
PgCurrentSysCacheRelationOidArray(void)
{
	return PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionCatalogLookupRuntimeState, PgCurrentSessionCatalogLookupState)->sys_cache_relation_oid;
}

int *
PgCurrentSysCacheRelationOidSizeRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionCatalogLookupRuntimeState, PgCurrentSessionCatalogLookupState)->sys_cache_relation_oid_size;
}

Oid *
PgCurrentSysCacheSupportingRelOidArray(void)
{
	return PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionCatalogLookupRuntimeState, PgCurrentSessionCatalogLookupState)->sys_cache_supporting_rel_oid;
}

int *
PgCurrentSysCacheSupportingRelOidSizeRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionCatalogLookupRuntimeState, PgCurrentSessionCatalogLookupState)->sys_cache_supporting_rel_oid_size;
}

CatCacheHeader **
PgCurrentCatCacheHeaderRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionCatalogLookupRuntimeState, PgCurrentSessionCatalogLookupState)->cat_cache_header;
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
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionCatalogLookupRuntimeState, PgCurrentSessionCatalogLookupState)->relcache_relation_id_cache;
}

bool *
PgCurrentCriticalRelcachesBuiltRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionCatalogLookupRuntimeState, PgCurrentSessionCatalogLookupState)->relcache_critical_built;
}

bool *
PgCurrentCriticalSharedRelcachesBuiltRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionCatalogLookupRuntimeState, PgCurrentSessionCatalogLookupState)->relcache_critical_shared_built;
}

long *
PgCurrentRelcacheInvalsReceivedRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionCatalogLookupRuntimeState, PgCurrentSessionCatalogLookupState)->relcache_invals_received;
}

TupleDesc *
PgCurrentPgClassDescriptorRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionCatalogLookupRuntimeState, PgCurrentSessionCatalogLookupState)->relcache_pg_class_descriptor;
}

TupleDesc *
PgCurrentPgIndexDescriptorRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionCatalogLookupRuntimeState, PgCurrentSessionCatalogLookupState)->relcache_pg_index_descriptor;
}

HTAB **
PgCurrentOpClassCacheRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionCatalogLookupRuntimeState, PgCurrentSessionCatalogLookupState)->relcache_opclass_cache;
}

HTAB **
PgCurrentTypeCacheHashRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionCatalogLookupRuntimeState, PgCurrentSessionCatalogLookupState)->typcache_type_cache_hash;
}

HTAB **
PgCurrentRelIdToTypeIdCacheHashRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionCatalogLookupRuntimeState, PgCurrentSessionCatalogLookupState)->typcache_relid_to_typeid_hash;
}

TypeCacheEntry **
PgCurrentFirstDomainTypeEntryRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionCatalogLookupRuntimeState, PgCurrentSessionCatalogLookupState)->typcache_first_domain_type_entry;
}

Oid **
PgCurrentTypCacheInProgressListRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionCatalogLookupRuntimeState, PgCurrentSessionCatalogLookupState)->typcache_in_progress_list;
}

int *
PgCurrentTypCacheInProgressListLenRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionCatalogLookupRuntimeState, PgCurrentSessionCatalogLookupState)->typcache_in_progress_list_len;
}

int *
PgCurrentTypCacheInProgressListMaxLenRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionCatalogLookupRuntimeState, PgCurrentSessionCatalogLookupState)->typcache_in_progress_list_maxlen;
}

HTAB **
PgCurrentRecordCacheHashRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionCatalogLookupRuntimeState, PgCurrentSessionCatalogLookupState)->typcache_record_cache_hash;
}

RecordCacheArrayEntry **
PgCurrentRecordCacheArrayRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionCatalogLookupRuntimeState, PgCurrentSessionCatalogLookupState)->typcache_record_cache_array;
}

int32 *
PgCurrentRecordCacheArrayLenRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionCatalogLookupRuntimeState, PgCurrentSessionCatalogLookupState)->typcache_record_cache_array_len;
}

int32 *
PgCurrentNextRecordTypmodRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionCatalogLookupRuntimeState, PgCurrentSessionCatalogLookupState)->typcache_next_record_typmod;
}

uint64 *
PgCurrentTupleDescIdCounterRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionCatalogLookupRuntimeState, PgCurrentSessionCatalogLookupState)->typcache_tupledesc_id_counter;
}

HTAB **
PgCurrentAttoptCacheHashRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionCatalogLookupRuntimeState, PgCurrentSessionCatalogLookupState)->attopt_cache_hash;
}

HTAB **
PgCurrentRelfilenumberMapHashRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionCatalogLookupRuntimeState, PgCurrentSessionCatalogLookupState)->relfilenumber_map_hash;
}

ScanKeyData *
PgCurrentRelfilenumberScanKeyArray(void)
{
	return PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionCatalogLookupRuntimeState, PgCurrentSessionCatalogLookupState)->relfilenumber_skey;
}

HTAB **
PgCurrentTableSpaceCacheHashRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionCatalogLookupRuntimeState, PgCurrentSessionCatalogLookupState)->tablespace_cache_hash;
}

HTAB **
PgCurrentEventTriggerCacheRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionCatalogLookupRuntimeState, PgCurrentSessionCatalogLookupState)->event_trigger_cache;
}

MemoryContext *
PgCurrentEventTriggerCacheContextRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionCatalogLookupRuntimeState, PgCurrentSessionCatalogLookupState)->event_trigger_cache_context;
}

int *
PgCurrentEventTriggerCacheStateRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionCatalogLookupRuntimeState, PgCurrentSessionCatalogLookupState)->event_trigger_cache_state;
}

HTAB **
PgCurrentUncommittedEnumTypesRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionCatalogRuntimeState, PgCurrentExecutionCatalogState)->uncommitted_enum_types;
}

HTAB **
PgCurrentUncommittedEnumValuesRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionCatalogRuntimeState, PgCurrentExecutionCatalogState)->uncommitted_enum_values;
}

Oid *
PgCurrentReindexedHeapRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionCatalogRuntimeState, PgCurrentExecutionCatalogState)->currently_reindexed_heap;
}

Oid *
PgCurrentReindexedIndexRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionCatalogRuntimeState, PgCurrentExecutionCatalogState)->currently_reindexed_index;
}

List **
PgCurrentPendingReindexedIndexesRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionCatalogRuntimeState, PgCurrentExecutionCatalogState)->pending_reindexed_indexes;
}

int *
PgCurrentReindexingNestLevelRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionCatalogRuntimeState, PgCurrentExecutionCatalogState)->reindexing_nest_level;
}

struct PendingRelDelete **
PgCurrentPendingRelDeletesRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionCatalogRuntimeState, PgCurrentExecutionCatalogState)->pending_rel_deletes;
}

HTAB **
PgCurrentPendingSyncHashRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionCatalogRuntimeState, PgCurrentExecutionCatalogState)->pending_sync_hash;
}

CatCInProgress **
PgCurrentCatCacheInProgressStackRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionCatalogCacheRuntimeState, PgCurrentExecutionCatalogCacheState)->catcache_in_progress_stack;
}

InProgressEnt **
PgCurrentRelcacheInProgressListRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionCatalogCacheRuntimeState, PgCurrentExecutionCatalogCacheState)->relcache_in_progress_list;
}

int *
PgCurrentRelcacheInProgressListLenRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionCatalogCacheRuntimeState, PgCurrentExecutionCatalogCacheState)->relcache_in_progress_list_len;
}

int *
PgCurrentRelcacheInProgressListMaxLenRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionCatalogCacheRuntimeState, PgCurrentExecutionCatalogCacheState)->relcache_in_progress_list_maxlen;
}

Oid *
PgCurrentRelcacheEOXactList(void)
{
	return PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionCatalogCacheRuntimeState, PgCurrentExecutionCatalogCacheState)->relcache_eoxact_list;
}

int *
PgCurrentRelcacheEOXactListLenRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionCatalogCacheRuntimeState, PgCurrentExecutionCatalogCacheState)->relcache_eoxact_list_len;
}

bool *
PgCurrentRelcacheEOXactListOverflowedRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionCatalogCacheRuntimeState, PgCurrentExecutionCatalogCacheState)->relcache_eoxact_list_overflowed;
}

TupleDesc **
PgCurrentRelcacheEOXactTupleDescArrayRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionCatalogCacheRuntimeState, PgCurrentExecutionCatalogCacheState)->relcache_eoxact_tupledesc_array;
}

int *
PgCurrentRelcacheNextEOXactTupleDescNumRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionCatalogCacheRuntimeState, PgCurrentExecutionCatalogCacheState)->relcache_next_eoxact_tupledesc_num;
}

int *
PgCurrentRelcacheEOXactTupleDescArrayLenRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionCatalogCacheRuntimeState, PgCurrentExecutionCatalogCacheState)->relcache_eoxact_tupledesc_array_len;
}

PgExecutionRelMapFile *
PgCurrentRelMapActiveSharedUpdatesRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionRelMapRuntimeState, PgCurrentExecutionRelMapState)->active_shared_updates;
}

PgExecutionRelMapFile *
PgCurrentRelMapActiveLocalUpdatesRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionRelMapRuntimeState, PgCurrentExecutionRelMapState)->active_local_updates;
}

PgExecutionRelMapFile *
PgCurrentRelMapPendingSharedUpdatesRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionRelMapRuntimeState, PgCurrentExecutionRelMapState)->pending_shared_updates;
}

PgExecutionRelMapFile *
PgCurrentRelMapPendingLocalUpdatesRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionRelMapRuntimeState, PgCurrentExecutionRelMapState)->pending_local_updates;
}

PgExecutionInvalMessageArray *
PgCurrentInvalMessageArrays(void)
{
	return PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionInvalidationRuntimeState, PgCurrentExecutionInvalidationState)->message_arrays;
}

struct TransInvalidationInfo **
PgCurrentTransInvalInfoRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionInvalidationRuntimeState, PgCurrentExecutionInvalidationState)->trans_info;
}

struct InvalidationInfo **
PgCurrentInplaceInvalInfoRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionInvalidationRuntimeState, PgCurrentExecutionInvalidationState)->inplace_info;
}

struct _SPI_plan **
PgCurrentRuleutilsRuleByOidPlanRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionCatalogLookupRuntimeState, PgCurrentSessionCatalogLookupState)->ruleutils_rule_by_oid_plan;
}

struct _SPI_plan **
PgCurrentRuleutilsViewRulePlanRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionCatalogLookupRuntimeState, PgCurrentSessionCatalogLookupState)->ruleutils_view_rule_plan;
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
