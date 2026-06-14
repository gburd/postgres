/*-------------------------------------------------------------------------
 *
 * backend_runtime_session.c
 *	  Runtime bridge accessors for session-owned compatibility state.
 *
 * These accessors keep session-facing legacy names mapped onto the current
 * PgSession while leaving fallback selection, construction, and top-level
 * lifecycle orchestration in backend_runtime.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/utils/init/backend_runtime_session.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/backend_runtime.h"
#include "utils/memutils.h"
#include "backend_runtime_internal.h"

PgSessionNamespaceState *
PgCurrentNamespaceState(void)
{
	return PgCurrentSessionNamespaceState();
}

char **
PgCurrentNamespaceSearchPathRef(void)
{
	return &PgCurrentSessionNamespaceState()->namespace_search_path_value;
}

PgSessionLocaleState *
PgCurrentLocaleState(void)
{
	return PgCurrentSessionLocaleState();
}

void **
PgCurrentIcuConverterRef(void)
{
	return &PgCurrentSessionLocaleState()->icu_converter;
}

char **
PgCurrentLocaleMessagesRef(void)
{
	return &PgCurrentSessionLocaleState()->locale_messages_value;
}

char **
PgCurrentLocaleMonetaryRef(void)
{
	return &PgCurrentSessionLocaleState()->locale_monetary_value;
}

char **
PgCurrentLocaleNumericRef(void)
{
	return &PgCurrentSessionLocaleState()->locale_numeric_value;
}

char **
PgCurrentLocaleTimeRef(void)
{
	return &PgCurrentSessionLocaleState()->locale_time_value;
}

int *
PgCurrentIcuValidationLevelRef(void)
{
	return &PgCurrentSessionLocaleState()->icu_validation_level_value;
}

Oid *
PgCurrentMyDatabaseIdRef(void)
{
	return &PgCurrentSessionDatabaseState()->database_id;
}

Oid *
PgCurrentMyDatabaseTableSpaceRef(void)
{
	return &PgCurrentSessionDatabaseState()->database_tablespace;
}

bool *
PgCurrentMyDatabaseHasLoginEventTriggersRef(void)
{
	return &PgCurrentSessionDatabaseState()->database_has_login_event_triggers;
}

char **
PgCurrentDatabasePathRef(void)
{
	return &PgCurrentSessionDatabaseState()->database_path;
}

bool *
PgCurrentDatabasePathOwnedRef(void)
{
	return &PgCurrentSessionDatabaseState()->database_path_owned;
}

char **
PgCurrentDefaultTablespaceRef(void)
{
	return &PgCurrentSessionTablespaceState()->default_tablespace_name;
}

char **
PgCurrentTempTablespacesRef(void)
{
	return &PgCurrentSessionTablespaceState()->temp_tablespaces_names;
}

bool *
PgCurrentAllowInPlaceTablespacesRef(void)
{
	return &PgCurrentSessionTablespaceState()->allow_in_place_tablespaces_value;
}

Oid *
PgCurrentBinaryUpgradeNextPgTablespaceOidRef(void)
{
	return &PgCurrentSessionTablespaceState()->binary_upgrade_next_pg_tablespace_oid_value;
}

Oid *
PgCurrentBinaryUpgradeNextPgTypeOidRef(void)
{
	return &PgCurrentSessionBinaryUpgradeState()->binary_upgrade_next_pg_type_oid_value;
}

Oid *
PgCurrentBinaryUpgradeNextArrayPgTypeOidRef(void)
{
	return &PgCurrentSessionBinaryUpgradeState()->binary_upgrade_next_array_pg_type_oid_value;
}

Oid *
PgCurrentBinaryUpgradeNextMrngPgTypeOidRef(void)
{
	return &PgCurrentSessionBinaryUpgradeState()->binary_upgrade_next_mrng_pg_type_oid_value;
}

Oid *
PgCurrentBinaryUpgradeNextMrngArrayPgTypeOidRef(void)
{
	return &PgCurrentSessionBinaryUpgradeState()->binary_upgrade_next_mrng_array_pg_type_oid_value;
}

Oid *
PgCurrentBinaryUpgradeNextHeapPgClassOidRef(void)
{
	return &PgCurrentSessionBinaryUpgradeState()->binary_upgrade_next_heap_pg_class_oid_value;
}

RelFileNumber *
PgCurrentBinaryUpgradeNextHeapPgClassRelfilenumberRef(void)
{
	return &PgCurrentSessionBinaryUpgradeState()->binary_upgrade_next_heap_pg_class_relfilenumber_value;
}

Oid *
PgCurrentBinaryUpgradeNextIndexPgClassOidRef(void)
{
	return &PgCurrentSessionBinaryUpgradeState()->binary_upgrade_next_index_pg_class_oid_value;
}

RelFileNumber *
PgCurrentBinaryUpgradeNextIndexPgClassRelfilenumberRef(void)
{
	return &PgCurrentSessionBinaryUpgradeState()->binary_upgrade_next_index_pg_class_relfilenumber_value;
}

Oid *
PgCurrentBinaryUpgradeNextToastPgClassOidRef(void)
{
	return &PgCurrentSessionBinaryUpgradeState()->binary_upgrade_next_toast_pg_class_oid_value;
}

RelFileNumber *
PgCurrentBinaryUpgradeNextToastPgClassRelfilenumberRef(void)
{
	return &PgCurrentSessionBinaryUpgradeState()->binary_upgrade_next_toast_pg_class_relfilenumber_value;
}

Oid *
PgCurrentBinaryUpgradeNextPgEnumOidRef(void)
{
	return &PgCurrentSessionBinaryUpgradeState()->binary_upgrade_next_pg_enum_oid_value;
}

Oid *
PgCurrentBinaryUpgradeNextPgAuthidOidRef(void)
{
	return &PgCurrentSessionBinaryUpgradeState()->binary_upgrade_next_pg_authid_oid_value;
}

bool *
PgCurrentBinaryUpgradeRecordInitPrivsRef(void)
{
	return &PgCurrentSessionBinaryUpgradeState()->binary_upgrade_record_init_privs_value;
}

int *
PgCurrentDateStyleRef(void)
{
	return &PgCurrentSessionDateTimeState()->date_style;
}

int *
PgCurrentDateOrderRef(void)
{
	return &PgCurrentSessionDateTimeState()->date_order;
}

int *
PgCurrentIntervalStyleRef(void)
{
	return &PgCurrentSessionDateTimeState()->interval_style;
}

char **
PgCurrentTimeZoneStringRef(void)
{
	return &PgCurrentSessionDateTimeState()->timezone_string_value;
}

char **
PgCurrentLogTimeZoneStringRef(void)
{
	return &PgCurrentSessionDateTimeState()->log_timezone_string_value;
}

pg_tz **
PgCurrentSessionTimeZoneRef(void)
{
	return &PgCurrentSessionDateTimeState()->session_timezone_value;
}

pg_tz **
PgCurrentLogTimeZoneRef(void)
{
	return &PgCurrentSessionDateTimeState()->log_timezone_value;
}

TimeZoneAbbrevTable **
PgCurrentTimeZoneAbbrevTableRef(void)
{
	return &PgCurrentSessionDateTimeState()->timezone_abbrev_table;
}

PgSessionTzAbbrevCache *
PgCurrentTimeZoneAbbrevCache(void)
{
	return PgCurrentSessionDateTimeState()->timezone_abbrev_cache;
}

char **
PgCurrentTSCurrentConfigRef(void)
{
	return &PgCurrentSessionTextSearchState()->current_config_value;
}

Oid *
PgCurrentTSCurrentConfigCacheRef(void)
{
	return &PgCurrentSessionTextSearchState()->current_config_cache;
}

HTAB **
PgCurrentTSParserCacheHashRef(void)
{
	return &PgCurrentSessionTextSearchState()->parser_cache_hash;
}

TSParserCacheEntry **
PgCurrentTSLastUsedParserRef(void)
{
	return &PgCurrentSessionTextSearchState()->last_used_parser;
}

HTAB **
PgCurrentTSDictionaryCacheHashRef(void)
{
	return &PgCurrentSessionTextSearchState()->dictionary_cache_hash;
}

TSDictionaryCacheEntry **
PgCurrentTSLastUsedDictionaryRef(void)
{
	return &PgCurrentSessionTextSearchState()->last_used_dictionary;
}

HTAB **
PgCurrentTSConfigCacheHashRef(void)
{
	return &PgCurrentSessionTextSearchState()->config_cache_hash;
}

TSConfigCacheEntry **
PgCurrentTSLastUsedConfigRef(void)
{
	return &PgCurrentSessionTextSearchState()->last_used_config;
}

CachedPlanSource **
PgCurrentUnnamedStmtPsrcRef(void)
{
	return &PgCurrentSessionTcopState()->unnamed_stmt_psrc;
}

bool *
PgCurrentEchoQueryRef(void)
{
	return &PgCurrentSessionTcopState()->echo_query;
}

bool *
PgCurrentUseSemiNewlineNewlineRef(void)
{
	return &PgCurrentSessionTcopState()->use_semi_newline_newline;
}

MemoryContext *
PgCurrentRowDescriptionContextRef(void)
{
	return &PgCurrentSessionTcopState()->row_description_context;
}

StringInfoData *
PgCurrentRowDescriptionBufRef(void)
{
	return &PgCurrentSessionTcopState()->row_description_buf;
}

void **
PgCurrentPLpgSQLSessionStateRef(void)
{
	return &PgCurrentSessionExtensionModuleState()->plpgsql_state;
}

void **
PgCurrentDblinkPersistentConnectionRef(void)
{
	return &PgCurrentSessionExtensionModuleState()->dblink_persistent_connection;
}

void **
PgCurrentDblinkRemoteConnHashRef(void)
{
	return &PgCurrentSessionExtensionModuleState()->dblink_remote_conn_hash;
}

bool *
PgCurrentDblinkResetRegisteredRef(void)
{
	return &PgCurrentSessionExtensionModuleState()->dblink_reset_registered;
}

void
PgSessionRegisterResetCallback(PgSessionResetCallback callback, void *arg)
{
	PgSessionExtensionModuleState *extension_modules;
	PgSessionResetCallbackItem *item;
	MemoryContext oldcontext;

	Assert(callback != NULL);

	extension_modules = PgCurrentSessionExtensionModuleState();

	if (CurrentPgSession != NULL)
		oldcontext = MemoryContextSwitchTo(PgSessionGetDynamicLibraryMemoryContext(CurrentPgSession));
	else
		oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	item = palloc_object(PgSessionResetCallbackItem);
	item->callback = callback;
	item->arg = arg;
	extension_modules->reset_callbacks =
		lappend(extension_modules->reset_callbacks, item);

	MemoryContextSwitchTo(oldcontext);
}

PgSessionInvalidationCallbackState *
PgCurrentInvalidationCallbackState(void)
{
	return PgCurrentSessionInvalidationCallbackState();
}

HTAB **
PgCurrentRIConstraintCacheRef(void)
{
	return &PgCurrentSessionRIGlobalsState()->constraint_cache;
}

HTAB **
PgCurrentRIQueryCacheRef(void)
{
	return &PgCurrentSessionRIGlobalsState()->query_cache;
}

HTAB **
PgCurrentRICompareCacheRef(void)
{
	return &PgCurrentSessionRIGlobalsState()->compare_cache;
}

dclist_head *
PgCurrentRIConstraintCacheValidListRef(void)
{
	return &PgCurrentSessionRIGlobalsState()->constraint_cache_valid_list;
}

bool *
PgCurrentRIFastPathXactCallbackRegisteredRef(void)
{
	return &PgCurrentSessionRIGlobalsState()->fastpath_xact_callback_registered;
}

int *
PgCurrentDebugDiscardCachesRef(void)
{
	return &PgCurrentSessionRIGlobalsState()->debug_discard_caches_value;
}

PgExecutionRelMapFile *
PgCurrentRelMapSharedMapRef(void)
{
	return &PgCurrentSessionRelMapState()->shared_map;
}

PgExecutionRelMapFile *
PgCurrentRelMapLocalMapRef(void)
{
	return &PgCurrentSessionRelMapState()->local_map;
}

HTAB **
PgCurrentPreparedQueriesRef(void)
{
	return &PgCurrentSessionPreparedStatementState()->prepared_queries;
}

List **
PgCurrentOnCommitActionsRef(void)
{
	return &PgCurrentSessionOnCommitState()->on_commits;
}

HTAB **
PgCurrentSequenceHashTableRef(void)
{
	return &PgCurrentSessionSequenceState()->seqhashtab;
}

struct SeqTableData **
PgCurrentLastUsedSequenceRef(void)
{
	return &PgCurrentSessionSequenceState()->last_used_seq;
}
