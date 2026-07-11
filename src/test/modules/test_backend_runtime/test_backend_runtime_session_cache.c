/*--------------------------------------------------------------------------
 *
 * test_backend_runtime_session_cache.c
 *		Session cache, module, and reset runtime state tests.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_backend_runtime/test_backend_runtime_session_cache.c
 *
 * -------------------------------------------------------------------------
 */
#include "test_backend_runtime.h"

static void
test_backend_runtime_syscache_callback(Datum arg, SysCacheIdentifier cacheid,
									   uint32 hashvalue)
{
}

static void
test_backend_runtime_relcache_callback(Datum arg, Oid relid)
{
}

static void
test_backend_runtime_relsync_callback(Datum arg, Oid relid)
{
}

static void
test_backend_runtime_xact_callback(XactEvent event, void *arg)
{
}

static void
test_backend_runtime_subxact_callback(SubXactEvent event,
									  SubTransactionId mySubid,
									  SubTransactionId parentSubid,
									  void *arg)
{
}

static void
test_backend_runtime_session_reset_callback(void *arg)
{
	int		   *counter = (int *) arg;

	(*counter)++;
}

typedef struct TestBackendRuntimeExtensionPrivateState
{
	int			value;
	const char *label;
	int		   *cleanup_count;
} TestBackendRuntimeExtensionPrivateState;

static const char test_backend_runtime_private_state_key[] =
	"test_backend_runtime.private_state";

static void
test_backend_runtime_extension_private_state_cleanup(void *arg)
{
	TestBackendRuntimeExtensionPrivateState *state =
		(TestBackendRuntimeExtensionPrivateState *) arg;

	if (state->cleanup_count != NULL)
		(*state->cleanup_count)++;
}

PG_FUNCTION_INFO_V1(test_session_catalog_lookup_state_is_session_local);
Datum
test_session_catalog_lookup_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	MemoryContext saved_cache_memory_context;
	CatCache   *saved_sys_cache[SysCacheSize];
	bool		saved_sys_cache_initialized;
	Oid			saved_sys_cache_relation_oid[SysCacheSize];
	int			saved_sys_cache_relation_oid_size;
	Oid			saved_sys_cache_supporting_rel_oid[SysCacheSize * 2];
	int			saved_sys_cache_supporting_rel_oid_size;
	CatCacheHeader *saved_cat_cache_header;
	HTAB	   *saved_relation_id_cache;
	bool		saved_critical_relcaches_built;
	bool		saved_critical_shared_relcaches_built;
	long		saved_relcache_invals_received;
	TupleDesc	saved_pg_class_descriptor;
	TupleDesc	saved_pg_index_descriptor;
	HTAB	   *saved_opclass_cache;
	HTAB	   *saved_type_cache_hash;
	HTAB	   *saved_relid_to_typeid_cache_hash;
	TypeCacheEntry *saved_first_domain_type_entry;
	Oid		   *saved_typcache_in_progress_list;
	int			saved_typcache_in_progress_list_len;
	int			saved_typcache_in_progress_list_maxlen;
	HTAB	   *saved_record_cache_hash;
	RecordCacheArrayEntry *saved_record_cache_array;
	int32		saved_record_cache_array_len;
	int32		saved_next_record_typmod;
	uint64		saved_tupledesc_id_counter;
	HTAB	   *saved_attopt_cache_hash;
	HTAB	   *saved_relfilenumber_map_hash;
	ScanKeyData saved_relfilenumber_skey[2];
	HTAB	   *saved_tablespace_cache_hash;
	HTAB	   *saved_event_trigger_cache;
	MemoryContext saved_event_trigger_cache_context;
	int			saved_event_trigger_cache_state;
	struct _SPI_plan *saved_rule_by_oid_plan;
	struct _SPI_plan *saved_view_rule_plan;
	HTAB	   *session1_hash_marker;
	HTAB	   *session2_hash_marker;
	MemoryContext session1_context_marker;
	MemoryContext session2_context_marker;
	struct _SPI_plan *session1_plan_marker;
	struct _SPI_plan *session2_plan_marker;
	CatCache   *session1_syscache_marker;
	CatCache   *session2_syscache_marker;
	CatCacheHeader *session1_catcache_header_marker;
	CatCacheHeader *session2_catcache_header_marker;
	HTAB	   *session1_relcache_marker;
	HTAB	   *session2_relcache_marker;
	TupleDesc	session1_pg_class_descriptor_marker;
	TupleDesc	session2_pg_class_descriptor_marker;
	TupleDesc	session1_pg_index_descriptor_marker;
	TupleDesc	session2_pg_index_descriptor_marker;
	HTAB	   *session1_opclass_marker;
	HTAB	   *session2_opclass_marker;
	TypeCacheEntry *session1_typentry_marker;
	TypeCacheEntry *session2_typentry_marker;
	Oid		   *session1_oid_array_marker;
	Oid		   *session2_oid_array_marker;
	RecordCacheArrayEntry *session1_record_array_marker;
	RecordCacheArrayEntry *session2_record_array_marker;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_cache_memory_context = CacheMemoryContext;
	memcpy(saved_sys_cache, PgCurrentSysCacheArray(),
		   sizeof(saved_sys_cache));
	saved_sys_cache_initialized = *PgCurrentSysCacheInitializedRef();
	memcpy(saved_sys_cache_relation_oid, PgCurrentSysCacheRelationOidArray(),
		   sizeof(saved_sys_cache_relation_oid));
	saved_sys_cache_relation_oid_size = *PgCurrentSysCacheRelationOidSizeRef();
	memcpy(saved_sys_cache_supporting_rel_oid,
		   PgCurrentSysCacheSupportingRelOidArray(),
		   sizeof(saved_sys_cache_supporting_rel_oid));
	saved_sys_cache_supporting_rel_oid_size =
		*PgCurrentSysCacheSupportingRelOidSizeRef();
	saved_cat_cache_header = *PgCurrentCatCacheHeaderRef();
	saved_relation_id_cache = *PgCurrentRelationIdCacheRef();
	saved_critical_relcaches_built = *PgCurrentCriticalRelcachesBuiltRef();
	saved_critical_shared_relcaches_built =
		*PgCurrentCriticalSharedRelcachesBuiltRef();
	saved_relcache_invals_received = *PgCurrentRelcacheInvalsReceivedRef();
	saved_pg_class_descriptor = *PgCurrentPgClassDescriptorRef();
	saved_pg_index_descriptor = *PgCurrentPgIndexDescriptorRef();
	saved_opclass_cache = *PgCurrentOpClassCacheRef();
	saved_type_cache_hash = *PgCurrentTypeCacheHashRef();
	saved_relid_to_typeid_cache_hash = *PgCurrentRelIdToTypeIdCacheHashRef();
	saved_first_domain_type_entry = *PgCurrentFirstDomainTypeEntryRef();
	saved_typcache_in_progress_list = *PgCurrentTypCacheInProgressListRef();
	saved_typcache_in_progress_list_len =
		*PgCurrentTypCacheInProgressListLenRef();
	saved_typcache_in_progress_list_maxlen =
		*PgCurrentTypCacheInProgressListMaxLenRef();
	saved_record_cache_hash = *PgCurrentRecordCacheHashRef();
	saved_record_cache_array = *PgCurrentRecordCacheArrayRef();
	saved_record_cache_array_len = *PgCurrentRecordCacheArrayLenRef();
	saved_next_record_typmod = *PgCurrentNextRecordTypmodRef();
	saved_tupledesc_id_counter = *PgCurrentTupleDescIdCounterRef();
	saved_attopt_cache_hash = *PgCurrentAttoptCacheHashRef();
	saved_relfilenumber_map_hash = *PgCurrentRelfilenumberMapHashRef();
	memcpy(saved_relfilenumber_skey, PgCurrentRelfilenumberScanKeyArray(),
		   sizeof(saved_relfilenumber_skey));
	saved_tablespace_cache_hash = *PgCurrentTableSpaceCacheHashRef();
	saved_event_trigger_cache = *PgCurrentEventTriggerCacheRef();
	saved_event_trigger_cache_context = *PgCurrentEventTriggerCacheContextRef();
	saved_event_trigger_cache_state = *PgCurrentEventTriggerCacheStateRef();
	saved_rule_by_oid_plan = *PgCurrentRuleutilsRuleByOidPlanRef();
	saved_view_rule_plan = *PgCurrentRuleutilsViewRulePlanRef();

	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);
	fake_session1.catalog_lookup.typcache_tupledesc_id_counter = (uint64) 1;
	fake_session2.catalog_lookup.typcache_tupledesc_id_counter = (uint64) 1;
	session1_hash_marker = (HTAB *) &fake_session1;
	session2_hash_marker = (HTAB *) &fake_session2;
	session1_context_marker = (MemoryContext) &fake_session1;
	session2_context_marker = (MemoryContext) &fake_session2;
	session1_plan_marker = (struct _SPI_plan *) &fake_session1;
	session2_plan_marker = (struct _SPI_plan *) &fake_session2;
	session1_syscache_marker = (CatCache *) &fake_session1;
	session2_syscache_marker = (CatCache *) &fake_session2;
	session1_catcache_header_marker = (CatCacheHeader *) &fake_session1;
	session2_catcache_header_marker = (CatCacheHeader *) &fake_session2;
	session1_relcache_marker = (HTAB *) &fake_session1;
	session2_relcache_marker = (HTAB *) &fake_session2;
	session1_pg_class_descriptor_marker = (TupleDesc) &fake_session1;
	session2_pg_class_descriptor_marker = (TupleDesc) &fake_session2;
	session1_pg_index_descriptor_marker = (TupleDesc) &session1_hash_marker;
	session2_pg_index_descriptor_marker = (TupleDesc) &session2_hash_marker;
	session1_opclass_marker = (HTAB *) &fake_session1;
	session2_opclass_marker = (HTAB *) &fake_session2;
	session1_typentry_marker = (TypeCacheEntry *) &fake_session1;
	session2_typentry_marker = (TypeCacheEntry *) &fake_session2;
	session1_oid_array_marker = (Oid *) &fake_session1;
	session2_oid_array_marker = (Oid *) &fake_session2;
	session1_record_array_marker = (RecordCacheArrayEntry *) &fake_session1;
	session2_record_array_marker = (RecordCacheArrayEntry *) &fake_session2;

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && CacheMemoryContext == NULL;
		ok = ok && PgCurrentSysCacheArray()[0] == NULL;
		ok = ok && *PgCurrentSysCacheInitializedRef() == false;
		ok = ok && PgCurrentSysCacheRelationOidArray()[0] == InvalidOid;
		ok = ok && *PgCurrentSysCacheRelationOidSizeRef() == 0;
		ok = ok && PgCurrentSysCacheSupportingRelOidArray()[0] == InvalidOid;
		ok = ok && *PgCurrentSysCacheSupportingRelOidSizeRef() == 0;
		ok = ok && *PgCurrentCatCacheHeaderRef() == NULL;
		ok = ok && *PgCurrentRelationIdCacheRef() == NULL;
		ok = ok && *PgCurrentCriticalRelcachesBuiltRef() == false;
		ok = ok && *PgCurrentCriticalSharedRelcachesBuiltRef() == false;
		ok = ok && *PgCurrentRelcacheInvalsReceivedRef() == 0;
		ok = ok && *PgCurrentPgClassDescriptorRef() == NULL;
		ok = ok && *PgCurrentPgIndexDescriptorRef() == NULL;
		ok = ok && *PgCurrentOpClassCacheRef() == NULL;
		ok = ok && *PgCurrentTypeCacheHashRef() == NULL;
		ok = ok && *PgCurrentRelIdToTypeIdCacheHashRef() == NULL;
		ok = ok && *PgCurrentFirstDomainTypeEntryRef() == NULL;
		ok = ok && *PgCurrentTypCacheInProgressListRef() == NULL;
		ok = ok && *PgCurrentTypCacheInProgressListLenRef() == 0;
		ok = ok && *PgCurrentTypCacheInProgressListMaxLenRef() == 0;
		ok = ok && *PgCurrentRecordCacheHashRef() == NULL;
		ok = ok && *PgCurrentRecordCacheArrayRef() == NULL;
		ok = ok && *PgCurrentRecordCacheArrayLenRef() == 0;
		ok = ok && *PgCurrentNextRecordTypmodRef() == 0;
		ok = ok && *PgCurrentTupleDescIdCounterRef() == (uint64) 1;
		ok = ok && *PgCurrentAttoptCacheHashRef() == NULL;
		ok = ok && *PgCurrentRelfilenumberMapHashRef() == NULL;
		ok = ok && PgCurrentRelfilenumberScanKeyArray()[0].sk_attno == 0;
		ok = ok && *PgCurrentTableSpaceCacheHashRef() == NULL;
		ok = ok && *PgCurrentEventTriggerCacheRef() == NULL;
		ok = ok && *PgCurrentEventTriggerCacheContextRef() == NULL;
		ok = ok && *PgCurrentEventTriggerCacheStateRef() == 0;
		ok = ok && *PgCurrentRuleutilsRuleByOidPlanRef() == NULL;
		ok = ok && *PgCurrentRuleutilsViewRulePlanRef() == NULL;
		CacheMemoryContext = session1_context_marker;
		PgCurrentSysCacheArray()[0] = session1_syscache_marker;
		*PgCurrentSysCacheInitializedRef() = true;
		PgCurrentSysCacheRelationOidArray()[0] = 11;
		*PgCurrentSysCacheRelationOidSizeRef() = 1;
		PgCurrentSysCacheSupportingRelOidArray()[0] = 12;
		*PgCurrentSysCacheSupportingRelOidSizeRef() = 1;
		*PgCurrentCatCacheHeaderRef() = session1_catcache_header_marker;
		*PgCurrentRelationIdCacheRef() = session1_relcache_marker;
		*PgCurrentCriticalRelcachesBuiltRef() = true;
		*PgCurrentCriticalSharedRelcachesBuiltRef() = true;
		*PgCurrentRelcacheInvalsReceivedRef() = 11;
		*PgCurrentPgClassDescriptorRef() = session1_pg_class_descriptor_marker;
		*PgCurrentPgIndexDescriptorRef() = session1_pg_index_descriptor_marker;
		*PgCurrentOpClassCacheRef() = session1_opclass_marker;
		*PgCurrentTypeCacheHashRef() = session1_hash_marker;
		*PgCurrentRelIdToTypeIdCacheHashRef() = session1_hash_marker;
		*PgCurrentFirstDomainTypeEntryRef() = session1_typentry_marker;
		*PgCurrentTypCacheInProgressListRef() = session1_oid_array_marker;
		*PgCurrentTypCacheInProgressListLenRef() = 1;
		*PgCurrentTypCacheInProgressListMaxLenRef() = 2;
		*PgCurrentRecordCacheHashRef() = session1_hash_marker;
		*PgCurrentRecordCacheArrayRef() = session1_record_array_marker;
		*PgCurrentRecordCacheArrayLenRef() = 3;
		*PgCurrentNextRecordTypmodRef() = 4;
		*PgCurrentTupleDescIdCounterRef() = 5;
		*PgCurrentAttoptCacheHashRef() = session1_hash_marker;
		*PgCurrentRelfilenumberMapHashRef() = session1_hash_marker;
		PgCurrentRelfilenumberScanKeyArray()[0].sk_attno = 11;
		PgCurrentRelfilenumberScanKeyArray()[1].sk_attno = 12;
		*PgCurrentTableSpaceCacheHashRef() = session1_hash_marker;
		*PgCurrentEventTriggerCacheRef() = session1_hash_marker;
		*PgCurrentEventTriggerCacheContextRef() = session1_context_marker;
		*PgCurrentEventTriggerCacheStateRef() = 1;
		*PgCurrentRuleutilsRuleByOidPlanRef() = session1_plan_marker;
		*PgCurrentRuleutilsViewRulePlanRef() = session1_plan_marker;

		PgSetCurrentSession(&fake_session2);
		ok = ok && CacheMemoryContext == NULL;
		ok = ok && PgCurrentSysCacheArray()[0] == NULL;
		ok = ok && *PgCurrentSysCacheInitializedRef() == false;
		ok = ok && PgCurrentSysCacheRelationOidArray()[0] == InvalidOid;
		ok = ok && *PgCurrentSysCacheRelationOidSizeRef() == 0;
		ok = ok && PgCurrentSysCacheSupportingRelOidArray()[0] == InvalidOid;
		ok = ok && *PgCurrentSysCacheSupportingRelOidSizeRef() == 0;
		ok = ok && *PgCurrentCatCacheHeaderRef() == NULL;
		ok = ok && *PgCurrentRelationIdCacheRef() == NULL;
		ok = ok && *PgCurrentCriticalRelcachesBuiltRef() == false;
		ok = ok && *PgCurrentCriticalSharedRelcachesBuiltRef() == false;
		ok = ok && *PgCurrentRelcacheInvalsReceivedRef() == 0;
		ok = ok && *PgCurrentPgClassDescriptorRef() == NULL;
		ok = ok && *PgCurrentPgIndexDescriptorRef() == NULL;
		ok = ok && *PgCurrentOpClassCacheRef() == NULL;
		ok = ok && *PgCurrentTypeCacheHashRef() == NULL;
		ok = ok && *PgCurrentRelIdToTypeIdCacheHashRef() == NULL;
		ok = ok && *PgCurrentFirstDomainTypeEntryRef() == NULL;
		ok = ok && *PgCurrentTypCacheInProgressListRef() == NULL;
		ok = ok && *PgCurrentTypCacheInProgressListLenRef() == 0;
		ok = ok && *PgCurrentTypCacheInProgressListMaxLenRef() == 0;
		ok = ok && *PgCurrentRecordCacheHashRef() == NULL;
		ok = ok && *PgCurrentRecordCacheArrayRef() == NULL;
		ok = ok && *PgCurrentRecordCacheArrayLenRef() == 0;
		ok = ok && *PgCurrentNextRecordTypmodRef() == 0;
		ok = ok && *PgCurrentTupleDescIdCounterRef() == (uint64) 1;
		ok = ok && *PgCurrentAttoptCacheHashRef() == NULL;
		ok = ok && *PgCurrentRelfilenumberMapHashRef() == NULL;
		ok = ok && PgCurrentRelfilenumberScanKeyArray()[0].sk_attno == 0;
		ok = ok && *PgCurrentTableSpaceCacheHashRef() == NULL;
		ok = ok && *PgCurrentEventTriggerCacheRef() == NULL;
		ok = ok && *PgCurrentEventTriggerCacheContextRef() == NULL;
		ok = ok && *PgCurrentEventTriggerCacheStateRef() == 0;
		ok = ok && *PgCurrentRuleutilsRuleByOidPlanRef() == NULL;
		ok = ok && *PgCurrentRuleutilsViewRulePlanRef() == NULL;
		CacheMemoryContext = session2_context_marker;
		PgCurrentSysCacheArray()[0] = session2_syscache_marker;
		*PgCurrentSysCacheInitializedRef() = true;
		PgCurrentSysCacheRelationOidArray()[0] = 21;
		*PgCurrentSysCacheRelationOidSizeRef() = 1;
		PgCurrentSysCacheSupportingRelOidArray()[0] = 22;
		*PgCurrentSysCacheSupportingRelOidSizeRef() = 1;
		*PgCurrentCatCacheHeaderRef() = session2_catcache_header_marker;
		*PgCurrentRelationIdCacheRef() = session2_relcache_marker;
		*PgCurrentCriticalRelcachesBuiltRef() = true;
		*PgCurrentCriticalSharedRelcachesBuiltRef() = false;
		*PgCurrentRelcacheInvalsReceivedRef() = 22;
		*PgCurrentPgClassDescriptorRef() = session2_pg_class_descriptor_marker;
		*PgCurrentPgIndexDescriptorRef() = session2_pg_index_descriptor_marker;
		*PgCurrentOpClassCacheRef() = session2_opclass_marker;
		*PgCurrentTypeCacheHashRef() = session2_hash_marker;
		*PgCurrentRelIdToTypeIdCacheHashRef() = session2_hash_marker;
		*PgCurrentFirstDomainTypeEntryRef() = session2_typentry_marker;
		*PgCurrentTypCacheInProgressListRef() = session2_oid_array_marker;
		*PgCurrentTypCacheInProgressListLenRef() = 11;
		*PgCurrentTypCacheInProgressListMaxLenRef() = 12;
		*PgCurrentRecordCacheHashRef() = session2_hash_marker;
		*PgCurrentRecordCacheArrayRef() = session2_record_array_marker;
		*PgCurrentRecordCacheArrayLenRef() = 13;
		*PgCurrentNextRecordTypmodRef() = 14;
		*PgCurrentTupleDescIdCounterRef() = 15;
		*PgCurrentAttoptCacheHashRef() = session2_hash_marker;
		*PgCurrentRelfilenumberMapHashRef() = session2_hash_marker;
		PgCurrentRelfilenumberScanKeyArray()[0].sk_attno = 21;
		PgCurrentRelfilenumberScanKeyArray()[1].sk_attno = 22;
		*PgCurrentTableSpaceCacheHashRef() = session2_hash_marker;
		*PgCurrentEventTriggerCacheRef() = session2_hash_marker;
		*PgCurrentEventTriggerCacheContextRef() = session2_context_marker;
		*PgCurrentEventTriggerCacheStateRef() = 2;
		*PgCurrentRuleutilsRuleByOidPlanRef() = session2_plan_marker;
		*PgCurrentRuleutilsViewRulePlanRef() = session2_plan_marker;

		PgSetCurrentSession(&fake_session1);
		ok = ok && CacheMemoryContext == session1_context_marker;
		ok = ok && PgCurrentSysCacheArray()[0] == session1_syscache_marker;
		ok = ok && *PgCurrentSysCacheInitializedRef() == true;
		ok = ok && PgCurrentSysCacheRelationOidArray()[0] == 11;
		ok = ok && *PgCurrentSysCacheRelationOidSizeRef() == 1;
		ok = ok && PgCurrentSysCacheSupportingRelOidArray()[0] == 12;
		ok = ok && *PgCurrentSysCacheSupportingRelOidSizeRef() == 1;
		ok = ok && *PgCurrentCatCacheHeaderRef() ==
			session1_catcache_header_marker;
		ok = ok && *PgCurrentRelationIdCacheRef() == session1_relcache_marker;
		ok = ok && *PgCurrentCriticalRelcachesBuiltRef() == true;
		ok = ok && *PgCurrentCriticalSharedRelcachesBuiltRef() == true;
		ok = ok && *PgCurrentRelcacheInvalsReceivedRef() == 11;
		ok = ok && *PgCurrentPgClassDescriptorRef() ==
			session1_pg_class_descriptor_marker;
		ok = ok && *PgCurrentPgIndexDescriptorRef() ==
			session1_pg_index_descriptor_marker;
		ok = ok && *PgCurrentOpClassCacheRef() == session1_opclass_marker;
		ok = ok && *PgCurrentTypeCacheHashRef() == session1_hash_marker;
		ok = ok && *PgCurrentRelIdToTypeIdCacheHashRef() == session1_hash_marker;
		ok = ok && *PgCurrentFirstDomainTypeEntryRef() == session1_typentry_marker;
		ok = ok && *PgCurrentTypCacheInProgressListRef() ==
			session1_oid_array_marker;
		ok = ok && *PgCurrentTypCacheInProgressListLenRef() == 1;
		ok = ok && *PgCurrentTypCacheInProgressListMaxLenRef() == 2;
		ok = ok && *PgCurrentRecordCacheHashRef() == session1_hash_marker;
		ok = ok && *PgCurrentRecordCacheArrayRef() ==
			session1_record_array_marker;
		ok = ok && *PgCurrentRecordCacheArrayLenRef() == 3;
		ok = ok && *PgCurrentNextRecordTypmodRef() == 4;
		ok = ok && *PgCurrentTupleDescIdCounterRef() == 5;
		ok = ok && *PgCurrentAttoptCacheHashRef() == session1_hash_marker;
		ok = ok && *PgCurrentRelfilenumberMapHashRef() == session1_hash_marker;
		ok = ok && PgCurrentRelfilenumberScanKeyArray()[0].sk_attno == 11;
		ok = ok && PgCurrentRelfilenumberScanKeyArray()[1].sk_attno == 12;
		ok = ok && *PgCurrentTableSpaceCacheHashRef() == session1_hash_marker;
		ok = ok && *PgCurrentEventTriggerCacheRef() == session1_hash_marker;
		ok = ok && *PgCurrentEventTriggerCacheContextRef() == session1_context_marker;
		ok = ok && *PgCurrentEventTriggerCacheStateRef() == 1;
		ok = ok && *PgCurrentRuleutilsRuleByOidPlanRef() == session1_plan_marker;
		ok = ok && *PgCurrentRuleutilsViewRulePlanRef() == session1_plan_marker;

		PgSetCurrentSession(&fake_session2);
		ok = ok && CacheMemoryContext == session2_context_marker;
		ok = ok && PgCurrentSysCacheArray()[0] == session2_syscache_marker;
		ok = ok && *PgCurrentSysCacheInitializedRef() == true;
		ok = ok && PgCurrentSysCacheRelationOidArray()[0] == 21;
		ok = ok && *PgCurrentSysCacheRelationOidSizeRef() == 1;
		ok = ok && PgCurrentSysCacheSupportingRelOidArray()[0] == 22;
		ok = ok && *PgCurrentSysCacheSupportingRelOidSizeRef() == 1;
		ok = ok && *PgCurrentCatCacheHeaderRef() ==
			session2_catcache_header_marker;
		ok = ok && *PgCurrentRelationIdCacheRef() == session2_relcache_marker;
		ok = ok && *PgCurrentCriticalRelcachesBuiltRef() == true;
		ok = ok && *PgCurrentCriticalSharedRelcachesBuiltRef() == false;
		ok = ok && *PgCurrentRelcacheInvalsReceivedRef() == 22;
		ok = ok && *PgCurrentPgClassDescriptorRef() ==
			session2_pg_class_descriptor_marker;
		ok = ok && *PgCurrentPgIndexDescriptorRef() ==
			session2_pg_index_descriptor_marker;
		ok = ok && *PgCurrentOpClassCacheRef() == session2_opclass_marker;
		ok = ok && *PgCurrentTypeCacheHashRef() == session2_hash_marker;
		ok = ok && *PgCurrentRelIdToTypeIdCacheHashRef() == session2_hash_marker;
		ok = ok && *PgCurrentFirstDomainTypeEntryRef() == session2_typentry_marker;
		ok = ok && *PgCurrentTypCacheInProgressListRef() ==
			session2_oid_array_marker;
		ok = ok && *PgCurrentTypCacheInProgressListLenRef() == 11;
		ok = ok && *PgCurrentTypCacheInProgressListMaxLenRef() == 12;
		ok = ok && *PgCurrentRecordCacheHashRef() == session2_hash_marker;
		ok = ok && *PgCurrentRecordCacheArrayRef() ==
			session2_record_array_marker;
		ok = ok && *PgCurrentRecordCacheArrayLenRef() == 13;
		ok = ok && *PgCurrentNextRecordTypmodRef() == 14;
		ok = ok && *PgCurrentTupleDescIdCounterRef() == 15;
		ok = ok && *PgCurrentAttoptCacheHashRef() == session2_hash_marker;
		ok = ok && *PgCurrentRelfilenumberMapHashRef() == session2_hash_marker;
		ok = ok && PgCurrentRelfilenumberScanKeyArray()[0].sk_attno == 21;
		ok = ok && PgCurrentRelfilenumberScanKeyArray()[1].sk_attno == 22;
		ok = ok && *PgCurrentTableSpaceCacheHashRef() == session2_hash_marker;
		ok = ok && *PgCurrentEventTriggerCacheRef() == session2_hash_marker;
		ok = ok && *PgCurrentEventTriggerCacheContextRef() == session2_context_marker;
		ok = ok && *PgCurrentEventTriggerCacheStateRef() == 2;
		ok = ok && *PgCurrentRuleutilsRuleByOidPlanRef() == session2_plan_marker;
		ok = ok && *PgCurrentRuleutilsViewRulePlanRef() == session2_plan_marker;

		PgSetCurrentSession(saved_session);
		CacheMemoryContext = saved_cache_memory_context;
		memcpy(PgCurrentSysCacheArray(), saved_sys_cache,
			   sizeof(saved_sys_cache));
		*PgCurrentSysCacheInitializedRef() = saved_sys_cache_initialized;
		memcpy(PgCurrentSysCacheRelationOidArray(),
			   saved_sys_cache_relation_oid,
			   sizeof(saved_sys_cache_relation_oid));
		*PgCurrentSysCacheRelationOidSizeRef() =
			saved_sys_cache_relation_oid_size;
		memcpy(PgCurrentSysCacheSupportingRelOidArray(),
			   saved_sys_cache_supporting_rel_oid,
			   sizeof(saved_sys_cache_supporting_rel_oid));
		*PgCurrentSysCacheSupportingRelOidSizeRef() =
			saved_sys_cache_supporting_rel_oid_size;
		*PgCurrentCatCacheHeaderRef() = saved_cat_cache_header;
		*PgCurrentRelationIdCacheRef() = saved_relation_id_cache;
		*PgCurrentCriticalRelcachesBuiltRef() = saved_critical_relcaches_built;
		*PgCurrentCriticalSharedRelcachesBuiltRef() =
			saved_critical_shared_relcaches_built;
		*PgCurrentRelcacheInvalsReceivedRef() = saved_relcache_invals_received;
		*PgCurrentPgClassDescriptorRef() = saved_pg_class_descriptor;
		*PgCurrentPgIndexDescriptorRef() = saved_pg_index_descriptor;
		*PgCurrentOpClassCacheRef() = saved_opclass_cache;
		*PgCurrentTypeCacheHashRef() = saved_type_cache_hash;
		*PgCurrentRelIdToTypeIdCacheHashRef() = saved_relid_to_typeid_cache_hash;
		*PgCurrentFirstDomainTypeEntryRef() = saved_first_domain_type_entry;
		*PgCurrentTypCacheInProgressListRef() = saved_typcache_in_progress_list;
		*PgCurrentTypCacheInProgressListLenRef() =
			saved_typcache_in_progress_list_len;
		*PgCurrentTypCacheInProgressListMaxLenRef() =
			saved_typcache_in_progress_list_maxlen;
		*PgCurrentRecordCacheHashRef() = saved_record_cache_hash;
		*PgCurrentRecordCacheArrayRef() = saved_record_cache_array;
		*PgCurrentRecordCacheArrayLenRef() = saved_record_cache_array_len;
		*PgCurrentNextRecordTypmodRef() = saved_next_record_typmod;
		*PgCurrentTupleDescIdCounterRef() = saved_tupledesc_id_counter;
		*PgCurrentAttoptCacheHashRef() = saved_attopt_cache_hash;
		*PgCurrentRelfilenumberMapHashRef() = saved_relfilenumber_map_hash;
		memcpy(PgCurrentRelfilenumberScanKeyArray(), saved_relfilenumber_skey,
			   sizeof(saved_relfilenumber_skey));
		*PgCurrentTableSpaceCacheHashRef() = saved_tablespace_cache_hash;
		*PgCurrentEventTriggerCacheRef() = saved_event_trigger_cache;
		*PgCurrentEventTriggerCacheContextRef() = saved_event_trigger_cache_context;
		*PgCurrentEventTriggerCacheStateRef() = saved_event_trigger_cache_state;
		*PgCurrentRuleutilsRuleByOidPlanRef() = saved_rule_by_oid_plan;
		*PgCurrentRuleutilsViewRulePlanRef() = saved_view_rule_plan;
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		CacheMemoryContext = saved_cache_memory_context;
		memcpy(PgCurrentSysCacheArray(), saved_sys_cache,
			   sizeof(saved_sys_cache));
		*PgCurrentSysCacheInitializedRef() = saved_sys_cache_initialized;
		memcpy(PgCurrentSysCacheRelationOidArray(),
			   saved_sys_cache_relation_oid,
			   sizeof(saved_sys_cache_relation_oid));
		*PgCurrentSysCacheRelationOidSizeRef() =
			saved_sys_cache_relation_oid_size;
		memcpy(PgCurrentSysCacheSupportingRelOidArray(),
			   saved_sys_cache_supporting_rel_oid,
			   sizeof(saved_sys_cache_supporting_rel_oid));
		*PgCurrentSysCacheSupportingRelOidSizeRef() =
			saved_sys_cache_supporting_rel_oid_size;
		*PgCurrentCatCacheHeaderRef() = saved_cat_cache_header;
		*PgCurrentRelationIdCacheRef() = saved_relation_id_cache;
		*PgCurrentCriticalRelcachesBuiltRef() = saved_critical_relcaches_built;
		*PgCurrentCriticalSharedRelcachesBuiltRef() =
			saved_critical_shared_relcaches_built;
		*PgCurrentRelcacheInvalsReceivedRef() = saved_relcache_invals_received;
		*PgCurrentPgClassDescriptorRef() = saved_pg_class_descriptor;
		*PgCurrentPgIndexDescriptorRef() = saved_pg_index_descriptor;
		*PgCurrentOpClassCacheRef() = saved_opclass_cache;
		*PgCurrentTypeCacheHashRef() = saved_type_cache_hash;
		*PgCurrentRelIdToTypeIdCacheHashRef() = saved_relid_to_typeid_cache_hash;
		*PgCurrentFirstDomainTypeEntryRef() = saved_first_domain_type_entry;
		*PgCurrentTypCacheInProgressListRef() = saved_typcache_in_progress_list;
		*PgCurrentTypCacheInProgressListLenRef() =
			saved_typcache_in_progress_list_len;
		*PgCurrentTypCacheInProgressListMaxLenRef() =
			saved_typcache_in_progress_list_maxlen;
		*PgCurrentRecordCacheHashRef() = saved_record_cache_hash;
		*PgCurrentRecordCacheArrayRef() = saved_record_cache_array;
		*PgCurrentRecordCacheArrayLenRef() = saved_record_cache_array_len;
		*PgCurrentNextRecordTypmodRef() = saved_next_record_typmod;
		*PgCurrentTupleDescIdCounterRef() = saved_tupledesc_id_counter;
		*PgCurrentAttoptCacheHashRef() = saved_attopt_cache_hash;
		*PgCurrentRelfilenumberMapHashRef() = saved_relfilenumber_map_hash;
		memcpy(PgCurrentRelfilenumberScanKeyArray(), saved_relfilenumber_skey,
			   sizeof(saved_relfilenumber_skey));
		*PgCurrentTableSpaceCacheHashRef() = saved_tablespace_cache_hash;
		*PgCurrentEventTriggerCacheRef() = saved_event_trigger_cache;
		*PgCurrentEventTriggerCacheContextRef() = saved_event_trigger_cache_context;
		*PgCurrentEventTriggerCacheStateRef() = saved_event_trigger_cache_state;
		*PgCurrentRuleutilsRuleByOidPlanRef() = saved_rule_by_oid_plan;
		*PgCurrentRuleutilsViewRulePlanRef() = saved_view_rule_plan;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "catalog lookup state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_extension_module_state_is_session_local);
Datum
test_session_extension_module_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	int			session1_private;
	int			session2_private;
	int			session1_reset_count = 0;
	int			session2_reset_count = 0;
	PgSessionExtensionModuleState *extension_modules;
	char		session1_label[] = "session1 label";
	char		session2_label[] = "session2 label";
	char		session1_proc_hash[] = "session1 proc hash";
	char		session2_proc_hash[] = "session2 proc hash";
	int			session1_private_state_cleanup_count = 0;
	int			session2_private_state_cleanup_count = 0;
	TestBackendRuntimeExtensionPrivateState *session1_extension_private_state;
	TestBackendRuntimeExtensionPrivateState *session2_extension_private_state;
	MemoryContext session1_plpython_context = NULL;
	MemoryContext session1_plperl_context = NULL;
	MemoryContext session1_pltcl_context = NULL;
	MemoryContext session1_plsample_context = NULL;
	MemoryContext session2_plpython_context = NULL;
	MemoryContext session2_plperl_context = NULL;
	MemoryContext session2_pltcl_context = NULL;
	MemoryContext session2_plsample_context = NULL;
	bool		ok = true;

	saved_session = CurrentPgSession;
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		extension_modules = PgCurrentSessionExtensionModuleState();
		ok = ok && extension_modules->plpgsql_state == NULL;
		ok = ok && extension_modules->plpython_procedure_cache == NULL;
		ok = ok && extension_modules->plpython_memory_context == NULL;
		ok = ok && !extension_modules->plpython_reset_registered;
		ok = ok && extension_modules->plperl_memory_context == NULL;
		ok = ok && extension_modules->pltcl_start_proc == NULL;
		ok = ok && extension_modules->pltclu_start_proc == NULL;
		ok = ok && extension_modules->pltcl_memory_context == NULL;
		ok = ok && extension_modules->pltcl_hold_interp == NULL;
		ok = ok && extension_modules->pltcl_interp_hash == NULL;
		ok = ok && extension_modules->pltcl_proc_hash == NULL;
		ok = ok && extension_modules->pltcl_current_call_state == NULL;
		ok = ok && !extension_modules->pltcl_reset_registered;
		ok = ok && extension_modules->plsample_memory_context == NULL;
		ok = ok && extension_modules->private_states == NIL;
		ok = ok && PgSessionGetExtensionPrivateState(
			test_backend_runtime_private_state_key) == NULL;

		session1_plpython_context =
			AllocSetContextCreate(TopMemoryContext,
							  "test session1 PL/Python context",
							  ALLOCSET_SMALL_SIZES);
		session1_plperl_context =
			AllocSetContextCreate(TopMemoryContext,
							  "test session1 PL/Perl context",
							  ALLOCSET_SMALL_SIZES);
		session1_pltcl_context =
			AllocSetContextCreate(TopMemoryContext,
							  "test session1 PL/Tcl context",
							  ALLOCSET_SMALL_SIZES);
		session1_plsample_context =
			AllocSetContextCreate(TopMemoryContext,
							  "test session1 PL/Sample context",
							  ALLOCSET_SMALL_SIZES);
		extension_modules->plpython_procedure_cache = &session1_private;
		extension_modules->plpython_memory_context = session1_plpython_context;
		extension_modules->plpython_reset_registered = true;
		extension_modules->plperl_memory_context = session1_plperl_context;
		extension_modules->pltcl_start_proc = session1_label;
		extension_modules->pltclu_start_proc = session1_label;
		extension_modules->pltcl_memory_context = session1_pltcl_context;
		extension_modules->pltcl_hold_interp = &session1_private;
		extension_modules->pltcl_interp_hash = &session1_reset_count;
		extension_modules->pltcl_proc_hash = session1_proc_hash;
		extension_modules->pltcl_current_call_state = &session1_private;
		extension_modules->pltcl_reset_registered = true;
		extension_modules->plsample_memory_context =
			session1_plsample_context;
		session1_extension_private_state =
			(TestBackendRuntimeExtensionPrivateState *)
			PgSessionEnsureExtensionPrivateState(
				test_backend_runtime_private_state_key,
				sizeof(TestBackendRuntimeExtensionPrivateState),
				test_backend_runtime_extension_private_state_cleanup);
		session1_extension_private_state->value = 101;
		session1_extension_private_state->label = "session1 private state";
		session1_extension_private_state->cleanup_count =
			&session1_private_state_cleanup_count;
		ok = ok && PgSessionEnsureExtensionPrivateState(
			test_backend_runtime_private_state_key,
			sizeof(TestBackendRuntimeExtensionPrivateState),
			test_backend_runtime_extension_private_state_cleanup) ==
			session1_extension_private_state;
		ok = ok && *PgCurrentPLpgSQLSessionStateRef() == NULL;
		*PgCurrentPLpgSQLSessionStateRef() = &session1_private;
		PgSessionRegisterResetCallback(test_backend_runtime_session_reset_callback,
								   &session1_reset_count);

		PgSetCurrentSession(&fake_session2);
		extension_modules = PgCurrentSessionExtensionModuleState();
		ok = ok && extension_modules->plpgsql_state == NULL;
		ok = ok && extension_modules->plpython_procedure_cache == NULL;
		ok = ok && extension_modules->plpython_memory_context == NULL;
		ok = ok && !extension_modules->plpython_reset_registered;
		ok = ok && extension_modules->plperl_memory_context == NULL;
		ok = ok && extension_modules->pltcl_start_proc == NULL;
		ok = ok && extension_modules->pltclu_start_proc == NULL;
		ok = ok && extension_modules->pltcl_memory_context == NULL;
		ok = ok && extension_modules->pltcl_hold_interp == NULL;
		ok = ok && extension_modules->pltcl_interp_hash == NULL;
		ok = ok && extension_modules->pltcl_proc_hash == NULL;
		ok = ok && extension_modules->pltcl_current_call_state == NULL;
		ok = ok && !extension_modules->pltcl_reset_registered;
		ok = ok && extension_modules->plsample_memory_context == NULL;
		ok = ok && extension_modules->private_states == NIL;
		ok = ok && PgSessionGetExtensionPrivateState(
			test_backend_runtime_private_state_key) == NULL;

		session2_plpython_context =
			AllocSetContextCreate(TopMemoryContext,
							  "test session2 PL/Python context",
							  ALLOCSET_SMALL_SIZES);
		session2_plperl_context =
			AllocSetContextCreate(TopMemoryContext,
							  "test session2 PL/Perl context",
							  ALLOCSET_SMALL_SIZES);
		session2_pltcl_context =
			AllocSetContextCreate(TopMemoryContext,
							  "test session2 PL/Tcl context",
							  ALLOCSET_SMALL_SIZES);
		session2_plsample_context =
			AllocSetContextCreate(TopMemoryContext,
							  "test session2 PL/Sample context",
							  ALLOCSET_SMALL_SIZES);
		extension_modules->plpython_procedure_cache = &session2_private;
		extension_modules->plpython_memory_context = session2_plpython_context;
		extension_modules->plpython_reset_registered = true;
		extension_modules->plperl_memory_context = session2_plperl_context;
		extension_modules->pltcl_start_proc = session2_label;
		extension_modules->pltclu_start_proc = session2_label;
		extension_modules->pltcl_memory_context = session2_pltcl_context;
		extension_modules->pltcl_hold_interp = &session2_private;
		extension_modules->pltcl_interp_hash = &session2_reset_count;
		extension_modules->pltcl_proc_hash = session2_proc_hash;
		extension_modules->pltcl_current_call_state = &session2_private;
		extension_modules->pltcl_reset_registered = true;
		extension_modules->plsample_memory_context =
			session2_plsample_context;
		session2_extension_private_state =
			(TestBackendRuntimeExtensionPrivateState *)
			PgSessionEnsureExtensionPrivateState(
				test_backend_runtime_private_state_key,
				sizeof(TestBackendRuntimeExtensionPrivateState),
				test_backend_runtime_extension_private_state_cleanup);
		session2_extension_private_state->value = 201;
		session2_extension_private_state->label = "session2 private state";
		session2_extension_private_state->cleanup_count =
			&session2_private_state_cleanup_count;
		ok = ok && PgSessionEnsureExtensionPrivateState(
			test_backend_runtime_private_state_key,
			sizeof(TestBackendRuntimeExtensionPrivateState),
			test_backend_runtime_extension_private_state_cleanup) ==
			session2_extension_private_state;
		ok = ok && *PgCurrentPLpgSQLSessionStateRef() == NULL;
		*PgCurrentPLpgSQLSessionStateRef() = &session2_private;
		PgSessionRegisterResetCallback(test_backend_runtime_session_reset_callback,
								   &session2_reset_count);

		PgSetCurrentSession(&fake_session1);
		extension_modules = PgCurrentSessionExtensionModuleState();
		ok = ok && *PgCurrentPLpgSQLSessionStateRef() == &session1_private;
		ok = ok && extension_modules->plpython_procedure_cache ==
			&session1_private;
		ok = ok && extension_modules->plpython_memory_context ==
			session1_plpython_context;
		ok = ok && extension_modules->plpython_reset_registered;
		ok = ok && extension_modules->plperl_memory_context ==
			session1_plperl_context;
		ok = ok && strcmp(extension_modules->pltcl_start_proc,
					  "session1 label") == 0;
		ok = ok && strcmp(extension_modules->pltclu_start_proc,
					  "session1 label") == 0;
		ok = ok && extension_modules->pltcl_memory_context ==
			session1_pltcl_context;
		ok = ok && extension_modules->plsample_memory_context ==
			session1_plsample_context;
		ok = ok && extension_modules->pltcl_hold_interp == &session1_private;
		ok = ok && extension_modules->pltcl_interp_hash ==
			&session1_reset_count;
		ok = ok && extension_modules->pltcl_proc_hash ==
			session1_proc_hash;
		ok = ok && extension_modules->pltcl_current_call_state ==
			&session1_private;
		ok = ok && extension_modules->pltcl_reset_registered;
		ok = ok && PgSessionGetExtensionPrivateState(
			test_backend_runtime_private_state_key) ==
			session1_extension_private_state;
		ok = ok && session1_extension_private_state->value == 101;
		ok = ok && strcmp(session1_extension_private_state->label,
					  "session1 private state") == 0;
		ok = ok && session1_private_state_cleanup_count == 0;

		PgSetCurrentSession(&fake_session2);
		extension_modules = PgCurrentSessionExtensionModuleState();
		ok = ok && *PgCurrentPLpgSQLSessionStateRef() == &session2_private;
		ok = ok && extension_modules->plpython_procedure_cache ==
			&session2_private;
		ok = ok && extension_modules->plpython_memory_context ==
			session2_plpython_context;
		ok = ok && extension_modules->plpython_reset_registered;
		ok = ok && extension_modules->plperl_memory_context ==
			session2_plperl_context;
		ok = ok && strcmp(extension_modules->pltcl_start_proc,
					  "session2 label") == 0;
		ok = ok && strcmp(extension_modules->pltclu_start_proc,
					  "session2 label") == 0;
		ok = ok && extension_modules->pltcl_memory_context ==
			session2_pltcl_context;
		ok = ok && extension_modules->plsample_memory_context ==
			session2_plsample_context;
		ok = ok && extension_modules->pltcl_hold_interp == &session2_private;
		ok = ok && extension_modules->pltcl_interp_hash ==
			&session2_reset_count;
		ok = ok && extension_modules->pltcl_proc_hash ==
			session2_proc_hash;
		ok = ok && extension_modules->pltcl_current_call_state ==
			&session2_private;
		ok = ok && extension_modules->pltcl_reset_registered;
		ok = ok && PgSessionGetExtensionPrivateState(
			test_backend_runtime_private_state_key) ==
			session2_extension_private_state;
		ok = ok && session2_extension_private_state->value == 201;
		ok = ok && strcmp(session2_extension_private_state->label,
					  "session2 private state") == 0;
		ok = ok && session2_private_state_cleanup_count == 0;

		PgSetCurrentSession(saved_session);
		PgSessionResetClosedState(&fake_session1);
		session1_plpython_context = NULL;
		session1_plperl_context = NULL;
		session1_pltcl_context = NULL;
		session1_plsample_context = NULL;
		ok = ok && session1_reset_count == 1;
		ok = ok && session2_reset_count == 0;
		ok = ok && fake_session1.extension_modules.plpgsql_state == NULL;
		ok = ok && fake_session1.extension_modules.plpython_procedure_cache == NULL;
		ok = ok && fake_session1.extension_modules.plpython_memory_context == NULL;
		ok = ok && !fake_session1.extension_modules.plpython_reset_registered;
		ok = ok && fake_session1.extension_modules.plperl_memory_context == NULL;
		ok = ok && fake_session1.extension_modules.pltcl_start_proc == NULL;
		ok = ok && fake_session1.extension_modules.pltclu_start_proc == NULL;
		ok = ok && fake_session1.extension_modules.pltcl_memory_context == NULL;
		ok = ok && fake_session1.extension_modules.plsample_memory_context ==
			NULL;
		ok = ok && fake_session1.extension_modules.pltcl_hold_interp == NULL;
		ok = ok && fake_session1.extension_modules.pltcl_interp_hash == NULL;
		ok = ok && fake_session1.extension_modules.pltcl_proc_hash == NULL;
		ok = ok && fake_session1.extension_modules.pltcl_current_call_state == NULL;
		ok = ok && !fake_session1.extension_modules.pltcl_reset_registered;
		ok = ok && fake_session1.extension_modules.private_states == NIL;
		ok = ok && session1_private_state_cleanup_count == 1;
		ok = ok && fake_session1.extension_modules.reset_callbacks == NIL;

		ok = ok && fake_session2.extension_modules.plpgsql_state == &session2_private;
		ok = ok && fake_session2.extension_modules.plpython_procedure_cache ==
			&session2_private;
		ok = ok && fake_session2.extension_modules.plpython_memory_context ==
			session2_plpython_context;
		ok = ok && fake_session2.extension_modules.plpython_reset_registered;
		ok = ok && fake_session2.extension_modules.plperl_memory_context ==
			session2_plperl_context;
		ok = ok && strcmp(fake_session2.extension_modules.pltcl_start_proc,
					  "session2 label") == 0;
		ok = ok && strcmp(fake_session2.extension_modules.pltclu_start_proc,
					  "session2 label") == 0;
		ok = ok && fake_session2.extension_modules.pltcl_memory_context ==
			session2_pltcl_context;
		ok = ok && fake_session2.extension_modules.plsample_memory_context ==
			session2_plsample_context;
		ok = ok && fake_session2.extension_modules.pltcl_hold_interp ==
			&session2_private;
		ok = ok && fake_session2.extension_modules.pltcl_interp_hash ==
			&session2_reset_count;
		ok = ok && fake_session2.extension_modules.pltcl_proc_hash ==
			session2_proc_hash;
		ok = ok && fake_session2.extension_modules.pltcl_current_call_state ==
			&session2_private;
		ok = ok && fake_session2.extension_modules.pltcl_reset_registered;
		ok = ok && fake_session2.extension_modules.private_states != NIL;
		ok = ok && session2_private_state_cleanup_count == 0;
		ok = ok && fake_session2.extension_modules.reset_callbacks != NIL;

		PgSessionResetClosedState(&fake_session2);
		session2_plpython_context = NULL;
		session2_plperl_context = NULL;
		session2_pltcl_context = NULL;
		session2_plsample_context = NULL;
		ok = ok && session2_reset_count == 1;
		ok = ok && fake_session2.extension_modules.plpgsql_state == NULL;
		ok = ok && fake_session2.extension_modules.plpython_procedure_cache == NULL;
		ok = ok && fake_session2.extension_modules.plpython_memory_context == NULL;
		ok = ok && !fake_session2.extension_modules.plpython_reset_registered;
		ok = ok && fake_session2.extension_modules.plperl_memory_context == NULL;
		ok = ok && fake_session2.extension_modules.pltcl_start_proc == NULL;
		ok = ok && fake_session2.extension_modules.pltclu_start_proc == NULL;
		ok = ok && fake_session2.extension_modules.pltcl_memory_context == NULL;
		ok = ok && fake_session2.extension_modules.plsample_memory_context ==
			NULL;
		ok = ok && fake_session2.extension_modules.pltcl_hold_interp == NULL;
		ok = ok && fake_session2.extension_modules.pltcl_interp_hash == NULL;
		ok = ok && fake_session2.extension_modules.pltcl_proc_hash == NULL;
		ok = ok && fake_session2.extension_modules.pltcl_current_call_state == NULL;
		ok = ok && !fake_session2.extension_modules.pltcl_reset_registered;
		ok = ok && fake_session2.extension_modules.private_states == NIL;
		ok = ok && session2_private_state_cleanup_count == 1;
		ok = ok && fake_session2.extension_modules.reset_callbacks == NIL;
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		if (session1_plpython_context != NULL)
			MemoryContextDelete(session1_plpython_context);
		if (session1_plperl_context != NULL)
			MemoryContextDelete(session1_plperl_context);
		if (session1_pltcl_context != NULL)
			MemoryContextDelete(session1_pltcl_context);
		if (session1_plsample_context != NULL)
			MemoryContextDelete(session1_plsample_context);
		if (session2_plpython_context != NULL)
			MemoryContextDelete(session2_plpython_context);
		if (session2_plperl_context != NULL)
			MemoryContextDelete(session2_plperl_context);
		if (session2_pltcl_context != NULL)
			MemoryContextDelete(session2_pltcl_context);
		if (session2_plsample_context != NULL)
			MemoryContextDelete(session2_plsample_context);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "extension module state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_prepared_statement_state_is_session_local);
Datum
test_session_prepared_statement_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	HTAB	   *saved_prepared_queries;
	MemoryContext saved_function_manager_context;
	HTAB	   *saved_c_func_hash;
	HTAB	   *saved_cached_function_hash;
	HTAB	   *session1_marker;
	HTAB	   *session2_marker;
	MemoryContext session1_context_marker;
	MemoryContext session2_context_marker;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_prepared_queries = *PgCurrentPreparedQueriesRef();
	saved_function_manager_context = *PgCurrentFunctionManagerMemoryContextRef();
	saved_c_func_hash = *PgCurrentCFuncHashRef();
	saved_cached_function_hash = *PgCurrentCachedFunctionHashRef();
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);
	session1_marker = (HTAB *) &fake_session1;
	session2_marker = (HTAB *) &fake_session2;
	session1_context_marker = (MemoryContext) &fake_session1;
	session2_context_marker = (MemoryContext) &fake_session2;

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentPreparedQueriesRef() == NULL;
		ok = ok && *PgCurrentFunctionManagerMemoryContextRef() == NULL;
		ok = ok && *PgCurrentCFuncHashRef() == NULL;
		ok = ok && *PgCurrentCachedFunctionHashRef() == NULL;
		*PgCurrentPreparedQueriesRef() = session1_marker;
		*PgCurrentFunctionManagerMemoryContextRef() = session1_context_marker;
		*PgCurrentCFuncHashRef() = session1_marker;
		*PgCurrentCachedFunctionHashRef() = session1_marker;
		ok = ok && *PgCurrentPreparedQueriesRef() == session1_marker;
		ok = ok && *PgCurrentFunctionManagerMemoryContextRef() == session1_context_marker;
		ok = ok && *PgCurrentCFuncHashRef() == session1_marker;
		ok = ok && *PgCurrentCachedFunctionHashRef() == session1_marker;

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentPreparedQueriesRef() == NULL;
		ok = ok && *PgCurrentFunctionManagerMemoryContextRef() == NULL;
		ok = ok && *PgCurrentCFuncHashRef() == NULL;
		ok = ok && *PgCurrentCachedFunctionHashRef() == NULL;
		*PgCurrentPreparedQueriesRef() = session2_marker;
		*PgCurrentFunctionManagerMemoryContextRef() = session2_context_marker;
		*PgCurrentCFuncHashRef() = session2_marker;
		*PgCurrentCachedFunctionHashRef() = session2_marker;
		ok = ok && *PgCurrentPreparedQueriesRef() == session2_marker;
		ok = ok && *PgCurrentFunctionManagerMemoryContextRef() == session2_context_marker;
		ok = ok && *PgCurrentCFuncHashRef() == session2_marker;
		ok = ok && *PgCurrentCachedFunctionHashRef() == session2_marker;

		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentPreparedQueriesRef() == session1_marker;
		ok = ok && *PgCurrentFunctionManagerMemoryContextRef() == session1_context_marker;
		ok = ok && *PgCurrentCFuncHashRef() == session1_marker;
		ok = ok && *PgCurrentCachedFunctionHashRef() == session1_marker;

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentPreparedQueriesRef() == session2_marker;
		ok = ok && *PgCurrentFunctionManagerMemoryContextRef() == session2_context_marker;
		ok = ok && *PgCurrentCFuncHashRef() == session2_marker;
		ok = ok && *PgCurrentCachedFunctionHashRef() == session2_marker;

		PgSetCurrentSession(saved_session);
		*PgCurrentPreparedQueriesRef() = saved_prepared_queries;
		*PgCurrentFunctionManagerMemoryContextRef() = saved_function_manager_context;
		*PgCurrentCFuncHashRef() = saved_c_func_hash;
		*PgCurrentCachedFunctionHashRef() = saved_cached_function_hash;
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		*PgCurrentPreparedQueriesRef() = saved_prepared_queries;
		*PgCurrentFunctionManagerMemoryContextRef() = saved_function_manager_context;
		*PgCurrentCFuncHashRef() = saved_c_func_hash;
		*PgCurrentCachedFunctionHashRef() = saved_cached_function_hash;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "prepared statement/function manager state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_invalidation_callback_state_is_session_local);
Datum
test_session_invalidation_callback_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	PgSessionInvalidationCallbackState saved_invalidation_callbacks;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_invalidation_callbacks = *PgCurrentInvalidationCallbackState();
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && PgCurrentInvalidationCallbackState()->syscache_callback_count == 0;
		ok = ok && PgCurrentInvalidationCallbackState()->relcache_callback_count == 0;
		ok = ok && PgCurrentInvalidationCallbackState()->relsync_callback_count == 0;
		CacheRegisterSyscacheCallback(ATTNUM,
									  test_backend_runtime_syscache_callback,
									  UInt32GetDatum(11));
		CacheRegisterRelcacheCallback(test_backend_runtime_relcache_callback,
									  UInt32GetDatum(12));
		CacheRegisterRelSyncCallback(test_backend_runtime_relsync_callback,
									 UInt32GetDatum(13));
		ok = ok && PgCurrentInvalidationCallbackState()->syscache_callback_count == 1;
		ok = ok && PgCurrentInvalidationCallbackState()->syscache_callback_links[ATTNUM] == 1;
		ok = ok && PgCurrentInvalidationCallbackState()->syscache_callback_list[0].function ==
			test_backend_runtime_syscache_callback;
		ok = ok && DatumGetUInt32(PgCurrentInvalidationCallbackState()->syscache_callback_list[0].arg) == 11;
		ok = ok && PgCurrentInvalidationCallbackState()->relcache_callback_count == 1;
		ok = ok && PgCurrentInvalidationCallbackState()->relcache_callback_list[0].function ==
			test_backend_runtime_relcache_callback;
		ok = ok && DatumGetUInt32(PgCurrentInvalidationCallbackState()->relcache_callback_list[0].arg) == 12;
		ok = ok && PgCurrentInvalidationCallbackState()->relsync_callback_count == 1;
		ok = ok && PgCurrentInvalidationCallbackState()->relsync_callback_list[0].function ==
			test_backend_runtime_relsync_callback;
		ok = ok && DatumGetUInt32(PgCurrentInvalidationCallbackState()->relsync_callback_list[0].arg) == 13;

		PgSetCurrentSession(&fake_session2);
		ok = ok && PgCurrentInvalidationCallbackState()->syscache_callback_count == 0;
		ok = ok && PgCurrentInvalidationCallbackState()->relcache_callback_count == 0;
		ok = ok && PgCurrentInvalidationCallbackState()->relsync_callback_count == 0;
		CacheRegisterSyscacheCallback(PROCOID,
									  test_backend_runtime_syscache_callback,
									  UInt32GetDatum(21));
		ok = ok && PgCurrentInvalidationCallbackState()->syscache_callback_count == 1;
		ok = ok && PgCurrentInvalidationCallbackState()->syscache_callback_links[PROCOID] == 1;
		ok = ok && PgCurrentInvalidationCallbackState()->syscache_callback_links[ATTNUM] == 0;

		PgSetCurrentSession(&fake_session1);
		ok = ok && PgCurrentInvalidationCallbackState()->syscache_callback_count == 1;
		ok = ok && PgCurrentInvalidationCallbackState()->syscache_callback_links[ATTNUM] == 1;
		ok = ok && PgCurrentInvalidationCallbackState()->relcache_callback_count == 1;
		ok = ok && PgCurrentInvalidationCallbackState()->relsync_callback_count == 1;

		PgSetCurrentSession(saved_session);
		*PgCurrentInvalidationCallbackState() = saved_invalidation_callbacks;
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		*PgCurrentInvalidationCallbackState() = saved_invalidation_callbacks;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "invalidation callback state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_ri_globals_state_is_session_local);
Datum
test_session_ri_globals_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	bool		ok = true;

	saved_session = CurrentPgSession;
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(NULL);
		*PgCurrentRIConstraintCacheRef() = (HTAB *) &fake_session1;
		*PgCurrentRIQueryCacheRef() = (HTAB *) &fake_session1;
		*PgCurrentRICompareCacheRef() = (HTAB *) &fake_session1;
		*PgCurrentRIFastPathXactCallbackRegisteredRef() = true;
		*PgCurrentDebugDiscardCachesRef() = 3;

		PgSessionAdoptEarlyState(&fake_session1);

		ok = ok && fake_session1.ri_globals.constraint_cache ==
			(HTAB *) &fake_session1;
		ok = ok && fake_session1.ri_globals.query_cache ==
			(HTAB *) &fake_session1;
		ok = ok && fake_session1.ri_globals.compare_cache ==
			(HTAB *) &fake_session1;
		ok = ok && fake_session1.ri_globals.fastpath_xact_callback_registered;
		ok = ok && fake_session1.ri_globals.debug_discard_caches_value == 3;
		ok = ok && *PgCurrentRIConstraintCacheRef() == NULL;
		ok = ok && *PgCurrentRIQueryCacheRef() == NULL;
		ok = ok && *PgCurrentRICompareCacheRef() == NULL;
		ok = ok && !*PgCurrentRIFastPathXactCallbackRegisteredRef();
		ok = ok && *PgCurrentDebugDiscardCachesRef() ==
			DEFAULT_DEBUG_DISCARD_CACHES;
		ok = ok && dclist_is_empty(PgCurrentRIConstraintCacheValidListRef());

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentRIConstraintCacheRef() == NULL;
		ok = ok && *PgCurrentRIQueryCacheRef() == NULL;
		ok = ok && *PgCurrentRICompareCacheRef() == NULL;
		ok = ok && !*PgCurrentRIFastPathXactCallbackRegisteredRef();
		ok = ok && *PgCurrentDebugDiscardCachesRef() ==
			DEFAULT_DEBUG_DISCARD_CACHES;
		*PgCurrentRIConstraintCacheRef() = (HTAB *) &fake_session2;
		*PgCurrentRIQueryCacheRef() = (HTAB *) &fake_session2;
		*PgCurrentRICompareCacheRef() = (HTAB *) &fake_session2;
		*PgCurrentRIFastPathXactCallbackRegisteredRef() = false;
		*PgCurrentDebugDiscardCachesRef() = 4;

		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentRIConstraintCacheRef() ==
			(HTAB *) &fake_session1;
		ok = ok && *PgCurrentRIQueryCacheRef() == (HTAB *) &fake_session1;
		ok = ok && *PgCurrentRICompareCacheRef() == (HTAB *) &fake_session1;
		ok = ok && *PgCurrentRIFastPathXactCallbackRegisteredRef();
		ok = ok && *PgCurrentDebugDiscardCachesRef() == 3;

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentRIConstraintCacheRef() ==
			(HTAB *) &fake_session2;
		ok = ok && *PgCurrentRIQueryCacheRef() == (HTAB *) &fake_session2;
		ok = ok && *PgCurrentRICompareCacheRef() == (HTAB *) &fake_session2;
		ok = ok && !*PgCurrentRIFastPathXactCallbackRegisteredRef();
		ok = ok && *PgCurrentDebugDiscardCachesRef() == 4;

		PgSetCurrentSession(saved_session);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session RI globals state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_relmap_state_is_session_local);
Datum
test_session_relmap_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	bool		ok = true;

	saved_session = CurrentPgSession;
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(NULL);
		PgCurrentRelMapSharedMapRef()->magic = 11;
		PgCurrentRelMapSharedMapRef()->num_mappings = 1;
		PgCurrentRelMapSharedMapRef()->mappings[0].mapoid = 101;
		PgCurrentRelMapSharedMapRef()->mappings[0].mapfilenumber = 201;
		PgCurrentRelMapLocalMapRef()->magic = 12;
		PgCurrentRelMapLocalMapRef()->num_mappings = 2;

		PgSessionAdoptEarlyState(&fake_session1);

		ok = ok && fake_session1.relmap.shared_map.magic == 11;
		ok = ok && fake_session1.relmap.shared_map.num_mappings == 1;
		ok = ok && fake_session1.relmap.shared_map.mappings[0].mapoid == 101;
		ok = ok && fake_session1.relmap.shared_map.mappings[0].mapfilenumber == 201;
		ok = ok && fake_session1.relmap.local_map.magic == 12;
		ok = ok && fake_session1.relmap.local_map.num_mappings == 2;
		ok = ok && PgCurrentRelMapSharedMapRef()->magic == 0;
		ok = ok && PgCurrentRelMapSharedMapRef()->num_mappings == 0;
		ok = ok && PgCurrentRelMapLocalMapRef()->magic == 0;
		ok = ok && PgCurrentRelMapLocalMapRef()->num_mappings == 0;

		PgSetCurrentSession(&fake_session2);
		ok = ok && PgCurrentRelMapSharedMapRef()->magic == 0;
		ok = ok && PgCurrentRelMapLocalMapRef()->magic == 0;
		PgCurrentRelMapSharedMapRef()->magic = 21;
		PgCurrentRelMapSharedMapRef()->num_mappings = 3;
		PgCurrentRelMapSharedMapRef()->mappings[0].mapoid = 301;
		PgCurrentRelMapSharedMapRef()->mappings[0].mapfilenumber = 401;
		PgCurrentRelMapLocalMapRef()->magic = 22;
		PgCurrentRelMapLocalMapRef()->num_mappings = 4;

		PgSetCurrentSession(&fake_session1);
		ok = ok && PgCurrentRelMapSharedMapRef()->magic == 11;
		ok = ok && PgCurrentRelMapSharedMapRef()->num_mappings == 1;
		ok = ok && PgCurrentRelMapLocalMapRef()->magic == 12;
		ok = ok && PgCurrentRelMapLocalMapRef()->num_mappings == 2;

		PgSetCurrentSession(&fake_session2);
		ok = ok && PgCurrentRelMapSharedMapRef()->magic == 21;
		ok = ok && PgCurrentRelMapSharedMapRef()->num_mappings == 3;
		ok = ok && PgCurrentRelMapSharedMapRef()->mappings[0].mapoid == 301;
		ok = ok && PgCurrentRelMapSharedMapRef()->mappings[0].mapfilenumber == 401;
		ok = ok && PgCurrentRelMapLocalMapRef()->magic == 22;
		ok = ok && PgCurrentRelMapLocalMapRef()->num_mappings == 4;

		PgSetCurrentSession(saved_session);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session relmap state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_reset_closed_state);
Datum
test_session_reset_closed_state(PG_FUNCTION_ARGS)
{
	PgSession	fake_session;
	PgSession	active_session;
	PgSession  *saved_session;
	HASHCTL		hash_ctl;
	MemoryContext oldcontext;
	MemoryContext saved_context;
	MemoryContext dynamic_library_context;
	MemoryContext xact_callback_context;
	Session    *legacy_session;
	TSParserCacheEntry *parser_entry;
	TSDictionaryCacheEntry *dictionary_entry;
	TSConfigCacheEntry *config_entry;
	Oid			test_key = BOOLOID;
	Oid			temp_table_spaces[2] = {BOOLOID, INT4OID};
	bool		found;
	bool		ok = true;

	saved_session = CurrentPgSession;
	MemSet(&fake_session, 0, sizeof(fake_session));
	MemSet(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(Oid);

	fake_session.database.database_path_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test database path state",
							  ALLOCSET_SMALL_SIZES);
	oldcontext = MemoryContextSwitchTo(
		fake_session.database.database_path_context);
	fake_session.database.database_path = pstrdup("base/1");
	MemoryContextSwitchTo(oldcontext);
	fake_session.database.database_path_owned = true;
	fake_session.prepared_statement.prepared_queries =
		hash_create("test prepared statement cache", 8, &hash_ctl,
					HASH_ELEM | HASH_BLOBS);
	fake_session.on_commit.on_commits = list_make1(palloc(8));
	fake_session.parser.operator_lookup_cache =
		hash_create("test operator lookup cache", 8, &hash_ctl,
					HASH_ELEM | HASH_BLOBS);

	fake_session.catalog_lookup.cache_memory_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test catalog lookup cache context",
							  ALLOCSET_SMALL_SIZES);
	hash_ctl.hcxt = fake_session.catalog_lookup.cache_memory_context;
	fake_session.catalog_lookup.relcache_relation_id_cache =
		hash_create("test relcache relation cache", 8, &hash_ctl,
					HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	fake_session.catalog_lookup.relcache_opclass_cache =
		hash_create("test opclass cache", 8, &hash_ctl,
					HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	fake_session.catalog_lookup.relcache_pg_class_descriptor =
		(TupleDesc) MemoryContextAlloc(
			fake_session.catalog_lookup.cache_memory_context, 8);
	fake_session.catalog_lookup.relcache_pg_index_descriptor =
		(TupleDesc) MemoryContextAlloc(
			fake_session.catalog_lookup.cache_memory_context, 8);
	fake_session.catalog_lookup.typcache_type_cache_hash =
		hash_create("test typcache cache", 8, &hash_ctl,
					HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	fake_session.catalog_lookup.typcache_relid_to_typeid_hash =
		hash_create("test typcache relid cache", 8, &hash_ctl,
					HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	fake_session.catalog_lookup.typcache_record_cache_hash =
		hash_create("test record cache", 8, &hash_ctl,
					HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	hash_ctl.hcxt = NULL;

	fake_session.function_manager.function_manager_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test function manager cache context",
							  ALLOCSET_SMALL_SIZES);
	hash_ctl.hcxt = fake_session.function_manager.function_manager_context;
	fake_session.function_manager.c_func_hash =
		hash_create("test C function cache", 8, &hash_ctl,
					HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	hash_ctl.hcxt = NULL;
	fake_session.sequence.seqhashtab =
		hash_create("test sequence cache", 8, &hash_ctl,
					HASH_ELEM | HASH_BLOBS);
	fake_session.sequence.last_used_seq =
		(struct SeqTableData *) &fake_session;
	fake_session.async.local_channel_table =
		hash_create("test async channel cache", 8, &hash_ctl,
					HASH_ELEM | HASH_BLOBS);
	fake_session.async.registered_listener = true;
	fake_session.invalidation_callbacks.syscache_callback_count = 1;
	fake_session.invalidation_callbacks.syscache_callback_links[ATTNUM] = 1;
	fake_session.invalidation_callbacks.syscache_callback_list[0].id = ATTNUM;
	fake_session.invalidation_callbacks.syscache_callback_list[0].function =
		test_backend_runtime_syscache_callback;
	fake_session.invalidation_callbacks.relcache_callback_count = 1;
	fake_session.invalidation_callbacks.relcache_callback_list[0].function =
		test_backend_runtime_relcache_callback;
	fake_session.invalidation_callbacks.relsync_callback_count = 1;
	fake_session.invalidation_callbacks.relsync_callback_list[0].function =
		test_backend_runtime_relsync_callback;
	fake_session.user_identity.cached_role[0] = BOOLOID;
	fake_session.user_identity.cached_roles[0] = list_make1_oid(BOOLOID);
	fake_session.user_identity.system_user_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test system user state",
							  ALLOCSET_SMALL_SIZES);
	oldcontext = MemoryContextSwitchTo(
		fake_session.user_identity.system_user_context);
	fake_session.user_identity.system_user = pstrdup("trust:test");
	MemoryContextSwitchTo(oldcontext);
	fake_session.user_identity.system_user_owned = true;
	fake_session.user_identity.cached_db_hash = 12345;
	fake_session.vacuum.initialized = true;
	fake_session.vacuum.vacuum_buffer_usage_limit_kb = 9999;
	fake_session.vacuum.vacuum_cost_limit_value = 9999;
	fake_session.vacuum.vacuum_truncate_value = false;
	fake_session.lock_wait.initialized = true;
	fake_session.lock_wait.deadlock_timeout_ms = 9999;
	fake_session.lock_wait.log_lock_waits_value = false;
	fake_session.pgstat.initialized = true;
	fake_session.pgstat.track_counts = false;
	fake_session.pgstat.track_functions = TRACK_FUNC_ALL;
	fake_session.pgstat.fetch_consistency = PGSTAT_FETCH_CONSISTENCY_NONE;
	fake_session.pgstat.track_activities = false;
	fake_session.pgstat.session_end_cause = DISCONNECT_KILLED;
	fake_session.pgstat.last_session_report_time = 12345;
	fake_session.temp_file.initialized = true;
	fake_session.temp_file.temporary_files_size = 98765;
	fake_session.temp_file.temp_file_counter = 99;
	fake_session.temp_file.temp_table_spaces = temp_table_spaces;
	fake_session.temp_file.num_temp_table_spaces = lengthof(temp_table_spaces);
	fake_session.temp_file.next_temp_table_space = 1;
	fake_session.plan_cache.initialized = true;
	dlist_init(&fake_session.plan_cache.saved_plan_list);
	dlist_init(&fake_session.plan_cache.cached_expression_list);
	fake_session.namespace_state.initialized = true;
	fake_session.namespace_state.search_path_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test namespace search path",
							  ALLOCSET_SMALL_SIZES);
	oldcontext = MemoryContextSwitchTo(
		fake_session.namespace_state.search_path_context);
	fake_session.namespace_state.active_search_path = list_make1_oid(BOOLOID);
	MemoryContextSwitchTo(oldcontext);
	fake_session.namespace_state.active_creation_namespace = BOOLOID;
	fake_session.namespace_state.active_path_generation = 42;
	fake_session.namespace_state.base_search_path =
		fake_session.namespace_state.active_search_path;
	fake_session.namespace_state.base_creation_namespace = BOOLOID;
	fake_session.namespace_state.namespace_user = BOOLOID;
	fake_session.namespace_state.base_search_path_valid = true;
	fake_session.namespace_state.search_path_cache_valid = true;
	fake_session.namespace_state.search_path_cache_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test search path cache",
							  ALLOCSET_SMALL_SIZES);
	fake_session.namespace_state.my_temp_namespace = BOOLOID;
	fake_session.namespace_state.my_temp_toast_namespace = INT4OID;
	fake_session.namespace_state.my_temp_namespace_subid = 1;
	fake_session.namespace_state.namespace_search_path_value =
		"test_namespace_path";
	fake_session.namespace_state.search_path_cache = &fake_session;
	fake_session.namespace_state.last_search_path_cache_entry = &fake_session;

	hash_ctl.entrysize = sizeof(TSParserCacheEntry);
	fake_session.text_search.parser_cache_hash =
		hash_create("test text-search parser cache", 8, &hash_ctl,
					HASH_ELEM | HASH_BLOBS);
	parser_entry = hash_search(fake_session.text_search.parser_cache_hash,
							   &test_key, HASH_ENTER, &found);
	parser_entry->prsId = test_key;
	parser_entry->isvalid = true;
	fake_session.text_search.last_used_parser = parser_entry;

	hash_ctl.entrysize = sizeof(TSDictionaryCacheEntry);
	fake_session.text_search.dictionary_cache_hash =
		hash_create("test text-search dictionary cache", 8, &hash_ctl,
					HASH_ELEM | HASH_BLOBS);
	dictionary_entry =
		hash_search(fake_session.text_search.dictionary_cache_hash,
					&test_key, HASH_ENTER, &found);
	dictionary_entry->dictId = test_key;
	dictionary_entry->isvalid = true;
	dictionary_entry->dictCtx =
		AllocSetContextCreate(TopMemoryContext,
							  "test text-search dictionary",
							  ALLOCSET_SMALL_SIZES);
	dictionary_entry->dictData =
		MemoryContextAlloc(dictionary_entry->dictCtx, 8);
	fake_session.text_search.last_used_dictionary = dictionary_entry;

	hash_ctl.entrysize = sizeof(TSConfigCacheEntry);
	fake_session.text_search.config_cache_hash =
		hash_create("test text-search config cache", 8, &hash_ctl,
					HASH_ELEM | HASH_BLOBS);
	config_entry = hash_search(fake_session.text_search.config_cache_hash,
							   &test_key, HASH_ENTER, &found);
	config_entry->cfgId = test_key;
	config_entry->isvalid = true;
	config_entry->lenmap = 1;
	config_entry->map = palloc0(sizeof(ListDictionary));
	config_entry->map[0].len = 1;
	config_entry->map[0].dictIds = palloc(sizeof(Oid));
	config_entry->map[0].dictIds[0] = test_key;
	fake_session.text_search.last_used_config = config_entry;
	fake_session.text_search.current_config_cache = test_key;

	hash_ctl.entrysize = sizeof(Oid);
	fake_session.optimizer.planner_extension_names =
		(const char **) palloc(sizeof(char *));
	fake_session.optimizer.planner_extension_names[0] = "test";
	fake_session.optimizer.planner_extension_names_assigned = 1;
	fake_session.optimizer.planner_extension_names_allocated = 1;
	fake_session.optimizer.opr_proof_cache_hash =
		hash_create("test operator proof cache", 8, &hash_ctl,
					HASH_ELEM | HASH_BLOBS);
	fake_session.locale.collation_cache_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test collation cache",
							  ALLOCSET_SMALL_SIZES);
	fake_session.locale.locale_conv_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test localeconv cache",
							  ALLOCSET_SMALL_SIZES);
	fake_session.locale.locale_time_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test localized time cache",
							  ALLOCSET_SMALL_SIZES);
	fake_session.locale.localized_abbrev_days_values[0] =
		MemoryContextStrdup(fake_session.locale.locale_time_context, "Sun");
	fake_session.locale.localized_full_days_values[0] =
		MemoryContextStrdup(fake_session.locale.locale_time_context, "Sunday");
	fake_session.locale.localized_abbrev_months_values[0] =
		MemoryContextStrdup(fake_session.locale.locale_time_context, "Jan");
	fake_session.locale.localized_full_months_values[0] =
		MemoryContextStrdup(fake_session.locale.locale_time_context, "January");
	fake_session.locale.locale_time_valid = true;
	fake_session.locale.current_locale_conv =
		MemoryContextAllocZero(fake_session.locale.locale_conv_context,
							   sizeof(struct lconv));
	fake_session.locale.locale_conv_valid = true;
	fake_session.locale.collation_cache = &fake_session;
	fake_session.locale.last_collation_cache_oid = BOOLOID;
	fake_session.locale.last_collation_cache_locale = &fake_session;
	fake_session.ri_globals.fastpath_xact_callback_registered = true;

	PgSetCurrentSession(&fake_session);
	RegisterXactCallback(test_backend_runtime_xact_callback, &fake_session);
	RegisterSubXactCallback(test_backend_runtime_subxact_callback,
							&fake_session);
	PgSetCurrentSession(saved_session);
	xact_callback_context = fake_session.xact_callbacks.xact_callback_context;
	ok = ok && xact_callback_context != NULL;
	ok = ok && GetMemoryChunkContext(fake_session.xact_callbacks.xact_callbacks) ==
		xact_callback_context;
	ok = ok &&
		GetMemoryChunkContext(fake_session.xact_callbacks.subxact_callbacks) ==
		xact_callback_context;

	dynamic_library_context =
		PgSessionGetDynamicLibraryMemoryContext(&fake_session);
	ok = ok && dynamic_library_context != NULL;
	ok = ok && fake_session.dynamic_library_context ==
		dynamic_library_context;

	oldcontext = MemoryContextSwitchTo(dynamic_library_context);
	fake_session.dynamic_library_inits =
		lappend(fake_session.dynamic_library_inits, &fake_session);
	MemoryContextSwitchTo(oldcontext);

	ok = ok && fake_session.dynamic_library_inits != NIL;
	ok = ok && GetMemoryChunkContext(fake_session.dynamic_library_inits) ==
		dynamic_library_context;

	PgSessionResetClosedState(&fake_session);

	ok = ok && fake_session.dynamic_library_context == NULL;
	ok = ok && fake_session.dynamic_library_inits == NIL;
	ok = ok && fake_session.database.database_path == NULL;
	ok = ok && fake_session.database.database_path_context == NULL;
	ok = ok && !fake_session.database.database_path_owned;
	ok = ok && fake_session.prepared_statement.prepared_queries == NULL;
	ok = ok && fake_session.vacuum.initialized;
	ok = ok && fake_session.vacuum.vacuum_buffer_usage_limit_kb == 2048;
	ok = ok && fake_session.vacuum.vacuum_cost_limit_value == 200;
	ok = ok && fake_session.vacuum.vacuum_truncate_value;
	ok = ok && fake_session.lock_wait.initialized;
	ok = ok && fake_session.lock_wait.deadlock_timeout_ms == 1000;
	ok = ok && fake_session.lock_wait.log_lock_waits_value;
	ok = ok && fake_session.pgstat.initialized;
	ok = ok && fake_session.pgstat.track_counts;
	ok = ok && fake_session.pgstat.track_functions == TRACK_FUNC_OFF;
	ok = ok && fake_session.pgstat.fetch_consistency ==
		PGSTAT_FETCH_CONSISTENCY_CACHE;
	ok = ok && fake_session.pgstat.track_activities;
	ok = ok && fake_session.pgstat.session_end_cause == DISCONNECT_NORMAL;
	ok = ok && fake_session.pgstat.last_session_report_time == 0;
	ok = ok && fake_session.large_object.heap_relation == NULL;
	ok = ok && fake_session.large_object.index_relation == NULL;
	ok = ok && fake_session.temp_file.initialized;
	ok = ok && fake_session.temp_file.temporary_files_size == 0;
	ok = ok && fake_session.temp_file.temp_file_counter == 0;
	ok = ok && fake_session.temp_file.temp_table_spaces == NULL;
	ok = ok && fake_session.temp_file.num_temp_table_spaces == -1;
	ok = ok && fake_session.temp_file.next_temp_table_space == 0;
	ok = ok && fake_session.plan_cache.initialized;
	ok = ok && dlist_is_empty(&fake_session.plan_cache.saved_plan_list);
	ok = ok && dlist_is_empty(&fake_session.plan_cache.cached_expression_list);
	ok = ok && fake_session.namespace_state.initialized;
	ok = ok && fake_session.namespace_state.active_search_path == NIL;
	ok = ok && fake_session.namespace_state.active_creation_namespace == InvalidOid;
	ok = ok && fake_session.namespace_state.active_path_generation == 1;
	ok = ok && fake_session.namespace_state.base_search_path == NIL;
	ok = ok && fake_session.namespace_state.base_creation_namespace == InvalidOid;
	ok = ok && fake_session.namespace_state.namespace_user == InvalidOid;
	ok = ok && fake_session.namespace_state.base_search_path_valid;
	ok = ok && !fake_session.namespace_state.search_path_cache_valid;
	ok = ok && fake_session.namespace_state.search_path_context == NULL;
	ok = ok && fake_session.namespace_state.search_path_cache_context == NULL;
	ok = ok && fake_session.namespace_state.my_temp_namespace == InvalidOid;
	ok = ok && fake_session.namespace_state.my_temp_toast_namespace == InvalidOid;
	ok = ok && fake_session.namespace_state.my_temp_namespace_subid == InvalidSubTransactionId;
	ok = ok && fake_session.namespace_state.namespace_search_path_value == NULL;
	ok = ok && fake_session.namespace_state.search_path_cache == NULL;
	ok = ok && fake_session.namespace_state.last_search_path_cache_entry == NULL;
	ok = ok && fake_session.on_commit.on_commits == NIL;
	ok = ok && fake_session.xact_callbacks.xact_callbacks == NULL;
	ok = ok && fake_session.xact_callbacks.subxact_callbacks == NULL;
	ok = ok && fake_session.xact_callbacks.xact_callback_context == NULL;
	ok = ok && fake_session.parser.operator_lookup_cache == NULL;
	ok = ok && fake_session.catalog_lookup.cache_memory_context == NULL;
	ok = ok && fake_session.catalog_lookup.relcache_relation_id_cache == NULL;
	ok = ok && fake_session.catalog_lookup.relcache_opclass_cache == NULL;
	ok = ok && fake_session.catalog_lookup.relcache_pg_class_descriptor == NULL;
	ok = ok && fake_session.catalog_lookup.relcache_pg_index_descriptor == NULL;
	ok = ok && fake_session.catalog_lookup.typcache_type_cache_hash == NULL;
	ok = ok && fake_session.catalog_lookup.typcache_relid_to_typeid_hash == NULL;
	ok = ok && fake_session.catalog_lookup.typcache_record_cache_hash == NULL;
	ok = ok && fake_session.function_manager.function_manager_context == NULL;
	ok = ok && fake_session.function_manager.c_func_hash == NULL;
	ok = ok && fake_session.function_manager.cached_function_hash == NULL;
	ok = ok && fake_session.sequence.seqhashtab == NULL;
	ok = ok && fake_session.sequence.last_used_seq == NULL;
	ok = ok && fake_session.async.local_channel_table == NULL;
	ok = ok && !fake_session.async.registered_listener;
	ok = ok && fake_session.invalidation_callbacks.syscache_callback_count == 0;
	ok = ok && fake_session.invalidation_callbacks.syscache_callback_links[ATTNUM] == 0;
	ok = ok && fake_session.invalidation_callbacks.relcache_callback_count == 0;
	ok = ok && fake_session.invalidation_callbacks.relsync_callback_count == 0;
	ok = ok && fake_session.user_identity.cached_role[0] == InvalidOid;
	ok = ok && fake_session.user_identity.cached_roles[0] == NIL;
	ok = ok && fake_session.user_identity.system_user == NULL;
	ok = ok && fake_session.user_identity.system_user_context == NULL;
	ok = ok && !fake_session.user_identity.system_user_owned;
	ok = ok && fake_session.user_identity.cached_db_hash == 0;
	ok = ok && fake_session.text_search.parser_cache_hash == NULL;
	ok = ok && fake_session.text_search.last_used_parser == NULL;
	ok = ok && fake_session.text_search.dictionary_cache_hash == NULL;
	ok = ok && fake_session.text_search.last_used_dictionary == NULL;
	ok = ok && fake_session.text_search.config_cache_hash == NULL;
	ok = ok && fake_session.text_search.last_used_config == NULL;
	ok = ok && fake_session.text_search.current_config_cache == InvalidOid;
	ok = ok && fake_session.regex.ctype_cache_list == NULL;
	ok = ok && fake_session.optimizer.planner_extension_names == NULL;
	ok = ok && fake_session.optimizer.planner_extension_names_assigned == 0;
	ok = ok && fake_session.optimizer.planner_extension_names_allocated == 0;
	ok = ok && fake_session.optimizer.opr_proof_cache_hash == NULL;
	ok = ok && fake_session.locale.locale_time_context == NULL;
	ok = ok && fake_session.locale.localized_abbrev_days_values[0] == NULL;
	ok = ok && fake_session.locale.localized_full_days_values[0] == NULL;
	ok = ok && fake_session.locale.localized_abbrev_months_values[0] == NULL;
	ok = ok && fake_session.locale.localized_full_months_values[0] == NULL;
	ok = ok && !fake_session.locale.locale_time_valid;
	ok = ok && fake_session.locale.locale_conv_context == NULL;
	ok = ok && fake_session.locale.current_locale_conv == NULL;
	ok = ok && !fake_session.locale.locale_conv_valid;
	ok = ok && fake_session.locale.collation_cache_context == NULL;
	ok = ok && fake_session.locale.collation_cache == NULL;
	ok = ok && fake_session.locale.last_collation_cache_oid == InvalidOid;
	ok = ok && fake_session.locale.last_collation_cache_locale == NULL;
	ok = ok && !fake_session.ri_globals.fastpath_xact_callback_registered;

	legacy_session = PgSessionGetLegacySession(&fake_session);
	ok = ok && legacy_session != NULL;
	ok = ok && fake_session.legacy_session == legacy_session;
	ok = ok && fake_session.legacy_session_context != NULL;
	ok = ok && GetMemoryChunkContext(legacy_session) ==
		fake_session.legacy_session_context;

	legacy_session->segment = (dsm_segment *) &fake_session;
	legacy_session->area = (dsa_area *) &fake_session;

	PgSetCurrentSession(&fake_session);
	CurrentSession = legacy_session;
	ok = ok && fake_session.legacy_session == legacy_session;
	CurrentSession = NULL;
	ok = ok && fake_session.legacy_session == NULL;
	CurrentSession = legacy_session;
	PgSetCurrentSession(saved_session);

	/*
	 * Also cover the legacy fallback where a list exists before the dedicated
	 * session context has been created.
	 */
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	fake_session.dynamic_library_inits =
		lappend(fake_session.dynamic_library_inits, &fake_session);
	MemoryContextSwitchTo(oldcontext);

	ok = ok && fake_session.dynamic_library_context == NULL;
	ok = ok && fake_session.dynamic_library_inits != NIL;

	PgSessionResetClosedState(&fake_session);

	ok = ok && fake_session.dynamic_library_context == NULL;
	ok = ok && fake_session.dynamic_library_inits == NIL;
	ok = ok && fake_session.legacy_session_context == NULL;
	ok = ok && fake_session.legacy_session == NULL;

	MemSet(&active_session, 0, sizeof(active_session));
	active_session.catalog_lookup.cache_memory_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test active catalog lookup cache context",
							  ALLOCSET_SMALL_SIZES);

	PgSetCurrentSession(&active_session);
	saved_context = CurrentMemoryContext;
	PG_TRY();
	{
		MemoryContextSwitchTo(active_session.catalog_lookup.cache_memory_context);
		PgSessionResetClosedState(&active_session);
		ok = ok && active_session.catalog_lookup.cache_memory_context == NULL;
		ok = ok && CurrentMemoryContext == TopMemoryContext;
		MemoryContextSwitchTo(saved_context);
		PgSetCurrentSession(saved_session);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(saved_context);
		PgSetCurrentSession(saved_session);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "closed session runtime state was not reset");

	PG_RETURN_BOOL(true);
}
