/*----------
 *
 * test_backend_runtime_session_guc.c
 *		Session and runtime GUC backend runtime state tests.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_backend_runtime/test_backend_runtime_session_guc.c
 *
 * -------------------------------------------------------------------------
 */
#include "test_backend_runtime.h"

PG_FUNCTION_INFO_V1(test_runtime_server_guc_state_is_runtime_local);
Datum
test_runtime_server_guc_state_is_runtime_local(PG_FUNCTION_ARGS)
{
	PgRuntime  *saved_runtime;
	PgRuntime	fake_runtime1;
	PgRuntime	fake_runtime2;
	char	   *saved_cluster_name;
	char	   *saved_config_file_name;
	char	   *saved_hba_file_name;
	char	   *saved_ident_file_name;
	char	   *saved_hosts_file_name;
	char	   *saved_external_pid_file;
	const char *stage = "initial";
	bool		ok = true;

	saved_runtime = CurrentPgRuntime;
	saved_cluster_name = cluster_name ? pstrdup(cluster_name) : NULL;
	saved_config_file_name = ConfigFileName ? pstrdup(ConfigFileName) : NULL;
	saved_hba_file_name = HbaFileName ? pstrdup(HbaFileName) : NULL;
	saved_ident_file_name = IdentFileName ? pstrdup(IdentFileName) : NULL;
	saved_hosts_file_name = HostsFileName ? pstrdup(HostsFileName) : NULL;
	saved_external_pid_file =
		external_pid_file ? pstrdup(external_pid_file) : NULL;
	MemSet(&fake_runtime1, 0, sizeof(fake_runtime1));
	MemSet(&fake_runtime2, 0, sizeof(fake_runtime2));

	PG_TRY();
	{
		stage = "runtime1 default";
		CurrentPgRuntime = &fake_runtime1;
		RebindSessionGUCVariablePointers();
		ok = ok && strcmp(cluster_name, "") == 0;
		ok = ok && ConfigFileName == NULL;
		ok = ok && HbaFileName == NULL;
		ok = ok && IdentFileName == NULL;
		ok = ok && HostsFileName == NULL;
		ok = ok && external_pid_file == NULL;
		if (!ok)
			elog(ERROR,
				 "runtime server GUC state was not runtime-local at %s",
				 stage);

		stage = "runtime1 set";
		cluster_name = "phase12_runtime_one";
		ConfigFileName = "/tmp/phase12_runtime_one.conf";
		HbaFileName = "/tmp/phase12_runtime_one_hba.conf";
		IdentFileName = "/tmp/phase12_runtime_one_ident.conf";
		HostsFileName = "/tmp/phase12_runtime_one_hosts.conf";
		external_pid_file = "/tmp/phase12_runtime_one.pid";
		ok = ok && strcmp(cluster_name, "phase12_runtime_one") == 0;
		ok = ok && strcmp(ConfigFileName,
						  "/tmp/phase12_runtime_one.conf") == 0;
		ok = ok && strcmp(HbaFileName,
						  "/tmp/phase12_runtime_one_hba.conf") == 0;
		ok = ok && strcmp(IdentFileName,
						  "/tmp/phase12_runtime_one_ident.conf") == 0;
		ok = ok && strcmp(HostsFileName,
						  "/tmp/phase12_runtime_one_hosts.conf") == 0;
		ok = ok && strcmp(external_pid_file,
						  "/tmp/phase12_runtime_one.pid") == 0;
		ok = ok && strcmp(GetConfigOption("cluster_name", false, false),
						  "phase12_runtime_one") == 0;
		ok = ok && strcmp(GetConfigOption("config_file", false, false),
						  "/tmp/phase12_runtime_one.conf") == 0;
		if (!ok)
			elog(ERROR,
				 "runtime server GUC state was not runtime-local at %s",
				 stage);

		stage = "runtime2 default";
		CurrentPgRuntime = &fake_runtime2;
		RebindSessionGUCVariablePointers();
		ok = ok && strcmp(cluster_name, "") == 0;
		ok = ok && ConfigFileName == NULL;
		ok = ok && HbaFileName == NULL;
		ok = ok && IdentFileName == NULL;
		ok = ok && HostsFileName == NULL;
		ok = ok && external_pid_file == NULL;
		if (!ok)
			elog(ERROR,
				 "runtime server GUC state was not runtime-local at %s",
				 stage);
		stage = "runtime2 set";
		cluster_name = "phase12_runtime_two";
		ConfigFileName = "/tmp/phase12_runtime_two.conf";
		HbaFileName = "/tmp/phase12_runtime_two_hba.conf";
		IdentFileName = "/tmp/phase12_runtime_two_ident.conf";
		HostsFileName = "/tmp/phase12_runtime_two_hosts.conf";
		external_pid_file = "/tmp/phase12_runtime_two.pid";
		ok = ok && strcmp(cluster_name, "phase12_runtime_two") == 0;
		ok = ok && strcmp(ConfigFileName,
						  "/tmp/phase12_runtime_two.conf") == 0;
		ok = ok && strcmp(HbaFileName,
						  "/tmp/phase12_runtime_two_hba.conf") == 0;
		ok = ok && strcmp(IdentFileName,
						  "/tmp/phase12_runtime_two_ident.conf") == 0;
		ok = ok && strcmp(HostsFileName,
						  "/tmp/phase12_runtime_two_hosts.conf") == 0;
		ok = ok && strcmp(external_pid_file,
						  "/tmp/phase12_runtime_two.pid") == 0;
		ok = ok && strcmp(GetConfigOption("cluster_name", false, false),
						  "phase12_runtime_two") == 0;
		ok = ok && strcmp(GetConfigOption("config_file", false, false),
						  "/tmp/phase12_runtime_two.conf") == 0;
		if (!ok)
			elog(ERROR,
				 "runtime server GUC state was not runtime-local at %s",
				 stage);

		stage = "runtime1 restore";
		CurrentPgRuntime = &fake_runtime1;
		RebindSessionGUCVariablePointers();
		ok = ok && strcmp(cluster_name, "phase12_runtime_one") == 0;
		ok = ok && strcmp(ConfigFileName,
						  "/tmp/phase12_runtime_one.conf") == 0;
		ok = ok && strcmp(HbaFileName,
						  "/tmp/phase12_runtime_one_hba.conf") == 0;
		ok = ok && strcmp(IdentFileName,
						  "/tmp/phase12_runtime_one_ident.conf") == 0;
		ok = ok && strcmp(HostsFileName,
						  "/tmp/phase12_runtime_one_hosts.conf") == 0;
		ok = ok && strcmp(external_pid_file,
						  "/tmp/phase12_runtime_one.pid") == 0;
		ok = ok && strcmp(GetConfigOption("cluster_name", false, false),
						  "phase12_runtime_one") == 0;
		ok = ok && strcmp(GetConfigOption("config_file", false, false),
						  "/tmp/phase12_runtime_one.conf") == 0;
		if (!ok)
			elog(ERROR,
				 "runtime server GUC state was not runtime-local at %s: cluster=%s config=%s hba=%s ident=%s hosts=%s pid=%s",
				 stage,
				 cluster_name ? cluster_name : "<null>",
				 ConfigFileName ? ConfigFileName : "<null>",
				 HbaFileName ? HbaFileName : "<null>",
				 IdentFileName ? IdentFileName : "<null>",
				 HostsFileName ? HostsFileName : "<null>",
				 external_pid_file ? external_pid_file : "<null>");

		stage = "saved runtime restore";
		CurrentPgRuntime = saved_runtime;
		RebindSessionGUCVariablePointers();
		cluster_name = saved_cluster_name;
		ConfigFileName = saved_config_file_name;
		HbaFileName = saved_hba_file_name;
		IdentFileName = saved_ident_file_name;
		HostsFileName = saved_hosts_file_name;
		external_pid_file = saved_external_pid_file;
	}
	PG_CATCH();
	{
		CurrentPgRuntime = saved_runtime;
		RebindSessionGUCVariablePointers();
		cluster_name = saved_cluster_name;
		ConfigFileName = saved_config_file_name;
		HbaFileName = saved_hba_file_name;
		IdentFileName = saved_ident_file_name;
		HostsFileName = saved_hosts_file_name;
		external_pid_file = saved_external_pid_file;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "runtime server GUC state was not runtime-local at %s",
			 stage);

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_runtime_extension_module_state_is_runtime_local);
Datum
test_runtime_extension_module_state_is_runtime_local(PG_FUNCTION_ARGS)
{
	PgRuntime  *saved_runtime;
	PgRuntime	fake_runtime1;
	PgRuntime	fake_runtime2;
	PgRuntimeExtensionModuleState *extension_modules;
	MemoryContext runtime1_context = NULL;
	MemoryContext runtime1_bloom_context = NULL;
	MemoryContext runtime2_context = NULL;
	MemoryContext runtime2_bloom_context = NULL;
	List	   *runtime1_advisors = (List *) &fake_runtime1;
	List	   *runtime2_advisors = (List *) &fake_runtime2;
	const char *stage = "initial";
	bool		ok = true;

	saved_runtime = CurrentPgRuntime;
	MemSet(&fake_runtime1, 0, sizeof(fake_runtime1));
	MemSet(&fake_runtime2, 0, sizeof(fake_runtime2));

	PG_TRY();
	{
		stage = "runtime1 default";
		CurrentPgRuntime = &fake_runtime1;
		extension_modules = PgCurrentRuntimeExtensionModuleState();
		ok = ok && extension_modules->pg_plan_advice_context == NULL;
		ok = ok && extension_modules->pg_plan_advice_advisor_hook_list == NIL;
		ok = ok && extension_modules->bloom_context == NULL;

		stage = "runtime1 set";
		runtime1_context =
			AllocSetContextCreate(TopMemoryContext,
								  "test runtime1 pg_plan_advice context",
								  ALLOCSET_SMALL_SIZES);
		runtime1_bloom_context =
			AllocSetContextCreate(TopMemoryContext,
								  "test runtime1 bloom context",
								  ALLOCSET_SMALL_SIZES);
		*PgCurrentPgPlanAdviceContextRef() = runtime1_context;
		*PgCurrentPgPlanAdviceAdvisorHookListRef() = runtime1_advisors;
		*PgCurrentBloomContextRef() = runtime1_bloom_context;
		extension_modules = PgCurrentRuntimeExtensionModuleState();
		ok = ok && extension_modules->pg_plan_advice_context == runtime1_context;
		ok = ok && extension_modules->pg_plan_advice_advisor_hook_list ==
			runtime1_advisors;
		ok = ok && extension_modules->bloom_context == runtime1_bloom_context;

		stage = "runtime2 default";
		CurrentPgRuntime = &fake_runtime2;
		extension_modules = PgCurrentRuntimeExtensionModuleState();
		ok = ok && extension_modules->pg_plan_advice_context == NULL;
		ok = ok && extension_modules->pg_plan_advice_advisor_hook_list == NIL;
		ok = ok && extension_modules->bloom_context == NULL;

		stage = "runtime2 set";
		runtime2_context =
			AllocSetContextCreate(TopMemoryContext,
								  "test runtime2 pg_plan_advice context",
								  ALLOCSET_SMALL_SIZES);
		runtime2_bloom_context =
			AllocSetContextCreate(TopMemoryContext,
								  "test runtime2 bloom context",
								  ALLOCSET_SMALL_SIZES);
		*PgCurrentPgPlanAdviceContextRef() = runtime2_context;
		*PgCurrentPgPlanAdviceAdvisorHookListRef() = runtime2_advisors;
		*PgCurrentBloomContextRef() = runtime2_bloom_context;
		extension_modules = PgCurrentRuntimeExtensionModuleState();
		ok = ok && extension_modules->pg_plan_advice_context == runtime2_context;
		ok = ok && extension_modules->pg_plan_advice_advisor_hook_list ==
			runtime2_advisors;
		ok = ok && extension_modules->bloom_context == runtime2_bloom_context;

		stage = "runtime1 restore";
		CurrentPgRuntime = &fake_runtime1;
		extension_modules = PgCurrentRuntimeExtensionModuleState();
		ok = ok && extension_modules->pg_plan_advice_context == runtime1_context;
		ok = ok && extension_modules->pg_plan_advice_advisor_hook_list ==
			runtime1_advisors;
		ok = ok && extension_modules->bloom_context == runtime1_bloom_context;

		CurrentPgRuntime = saved_runtime;
	}
	PG_CATCH();
	{
		CurrentPgRuntime = saved_runtime;
		if (runtime1_context != NULL)
			MemoryContextDelete(runtime1_context);
		if (runtime1_bloom_context != NULL)
			MemoryContextDelete(runtime1_bloom_context);
		if (runtime2_context != NULL)
			MemoryContextDelete(runtime2_context);
		if (runtime2_bloom_context != NULL)
			MemoryContextDelete(runtime2_bloom_context);
		PG_RE_THROW();
	}
	PG_END_TRY();

	CurrentPgRuntime = saved_runtime;
	if (runtime1_context != NULL)
		MemoryContextDelete(runtime1_context);
	if (runtime1_bloom_context != NULL)
		MemoryContextDelete(runtime1_bloom_context);
	if (runtime2_context != NULL)
		MemoryContextDelete(runtime2_context);
	if (runtime2_bloom_context != NULL)
		MemoryContextDelete(runtime2_bloom_context);

	if (!ok)
		elog(ERROR, "runtime extension module state was not runtime-local at %s",
			 stage);

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_guc_rebind_table_matches_registry);
Datum
test_session_guc_rebind_table_matches_registry(PG_FUNCTION_ARGS)
{
	int			rebound;

	RebindSessionGUCVariablePointers();
	rebound = ValidateSessionGUCVariableRebinds();
	if (rebound <= 0)
		elog(ERROR, "session GUC rebind table did not validate any entries");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_connection_guc_state_is_session_local);
Datum
test_session_connection_guc_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_application_name;
	char	   *saved_log_disconnections;
	char	   *saved_log_statement;
	char	   *saved_post_auth_delay;
	char	   *saved_restrict_relation_kind;
	char	   *saved_tcp_keepalives_idle;
	char	   *saved_tcp_keepalives_interval;
	char	   *saved_tcp_keepalives_count;
	char	   *saved_tcp_user_timeout;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_application_name =
		pstrdup(GetConfigOption("application_name", false, false));
	saved_log_disconnections =
		pstrdup(GetConfigOption("log_disconnections", false, false));
	saved_log_statement =
		pstrdup(GetConfigOption("log_statement", false, false));
	saved_post_auth_delay =
		pstrdup(GetConfigOption("post_auth_delay", false, false));
	saved_restrict_relation_kind =
		pstrdup(GetConfigOption("restrict_nonsystem_relation_kind",
								false, false));
	saved_tcp_keepalives_idle =
		pstrdup(GetConfigOption("tcp_keepalives_idle", false, false));
	saved_tcp_keepalives_interval =
		pstrdup(GetConfigOption("tcp_keepalives_interval", false, false));
	saved_tcp_keepalives_count =
		pstrdup(GetConfigOption("tcp_keepalives_count", false, false));
	saved_tcp_user_timeout =
		pstrdup(GetConfigOption("tcp_user_timeout", false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && strcmp(application_name, "") == 0;
		ok = ok && tcp_keepalives_idle == 0;
		ok = ok && tcp_keepalives_interval == 0;
		ok = ok && tcp_keepalives_count == 0;
		ok = ok && tcp_user_timeout == 0;
		ok = ok && !Log_disconnections;
		ok = ok && log_statement == LOGSTMT_NONE;
		ok = ok && PostAuthDelay == 0;
		ok = ok && strcmp(*PgCurrentRestrictNonsystemRelationKindStringRef(),
						  "") == 0;
		ok = ok && restrict_nonsystem_relation_kind == 0;

		SetConfigOption("application_name", "phase12_conn_one",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("tcp_keepalives_idle", "11",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("tcp_keepalives_interval", "12",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("tcp_keepalives_count", "13",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("tcp_user_timeout", "14",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_disconnections", "on",
						PGC_SU_BACKEND, PGC_S_CLIENT);
		SetConfigOption("log_statement", "ddl",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("post_auth_delay", "15",
						PGC_BACKEND, PGC_S_CLIENT);
		SetConfigOption("restrict_nonsystem_relation_kind", "view",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && strcmp(application_name, "phase12_conn_one") == 0;
		ok = ok && tcp_keepalives_idle == 11;
		ok = ok && tcp_keepalives_interval == 12;
		ok = ok && tcp_keepalives_count == 13;
		ok = ok && tcp_user_timeout == 14;
		ok = ok && Log_disconnections;
		ok = ok && log_statement == LOGSTMT_DDL;
		ok = ok && PostAuthDelay == 15;
		ok = ok && restrict_nonsystem_relation_kind == RESTRICT_RELKIND_VIEW;

		PgSetCurrentSession(&fake_session2);
		ok = ok && strcmp(application_name, "") == 0;
		ok = ok && tcp_keepalives_idle == 0;
		ok = ok && tcp_keepalives_interval == 0;
		ok = ok && tcp_keepalives_count == 0;
		ok = ok && tcp_user_timeout == 0;
		ok = ok && !Log_disconnections;
		ok = ok && log_statement == LOGSTMT_NONE;
		ok = ok && PostAuthDelay == 0;
		ok = ok && restrict_nonsystem_relation_kind == 0;
		SetConfigOption("application_name", "phase12_conn_two",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("tcp_keepalives_idle", "21",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("tcp_keepalives_interval", "22",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("tcp_keepalives_count", "23",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("tcp_user_timeout", "24",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_disconnections", "off",
						PGC_SU_BACKEND, PGC_S_CLIENT);
		SetConfigOption("log_statement", "all",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("post_auth_delay", "25",
						PGC_BACKEND, PGC_S_CLIENT);
		SetConfigOption("restrict_nonsystem_relation_kind",
						"view, foreign-table",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && strcmp(application_name, "phase12_conn_two") == 0;
		ok = ok && tcp_keepalives_idle == 21;
		ok = ok && tcp_keepalives_interval == 22;
		ok = ok && tcp_keepalives_count == 23;
		ok = ok && tcp_user_timeout == 24;
		ok = ok && !Log_disconnections;
		ok = ok && log_statement == LOGSTMT_ALL;
		ok = ok && PostAuthDelay == 25;
		ok = ok && restrict_nonsystem_relation_kind ==
			(RESTRICT_RELKIND_VIEW | RESTRICT_RELKIND_FOREIGN_TABLE);

		PgSetCurrentSession(&fake_session1);
		ok = ok && strcmp(application_name, "phase12_conn_one") == 0;
		ok = ok && tcp_keepalives_idle == 11;
		ok = ok && tcp_keepalives_interval == 12;
		ok = ok && tcp_keepalives_count == 13;
		ok = ok && tcp_user_timeout == 14;
		ok = ok && Log_disconnections;
		ok = ok && log_statement == LOGSTMT_DDL;
		ok = ok && PostAuthDelay == 15;
		ok = ok && restrict_nonsystem_relation_kind == RESTRICT_RELKIND_VIEW;

		PgSetCurrentSession(&fake_session2);
		ok = ok && strcmp(application_name, "phase12_conn_two") == 0;
		ok = ok && tcp_keepalives_idle == 21;
		ok = ok && tcp_keepalives_interval == 22;
		ok = ok && tcp_keepalives_count == 23;
		ok = ok && tcp_user_timeout == 24;
		ok = ok && !Log_disconnections;
		ok = ok && log_statement == LOGSTMT_ALL;
		ok = ok && PostAuthDelay == 25;
		ok = ok && restrict_nonsystem_relation_kind ==
			(RESTRICT_RELKIND_VIEW | RESTRICT_RELKIND_FOREIGN_TABLE);

		PgSetCurrentSession(saved_session);
		SetConfigOption("application_name", saved_application_name,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_disconnections", saved_log_disconnections,
						PGC_SU_BACKEND, PGC_S_CLIENT);
		SetConfigOption("log_statement", saved_log_statement,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("post_auth_delay", saved_post_auth_delay,
						PGC_BACKEND, PGC_S_CLIENT);
		SetConfigOption("restrict_nonsystem_relation_kind",
						saved_restrict_relation_kind,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("tcp_keepalives_idle", saved_tcp_keepalives_idle,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("tcp_keepalives_interval",
						saved_tcp_keepalives_interval,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("tcp_keepalives_count", saved_tcp_keepalives_count,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("tcp_user_timeout", saved_tcp_user_timeout,
						PGC_USERSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("application_name", saved_application_name,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_disconnections", saved_log_disconnections,
						PGC_SU_BACKEND, PGC_S_CLIENT);
		SetConfigOption("log_statement", saved_log_statement,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("post_auth_delay", saved_post_auth_delay,
						PGC_BACKEND, PGC_S_CLIENT);
		SetConfigOption("restrict_nonsystem_relation_kind",
						saved_restrict_relation_kind,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("tcp_keepalives_idle", saved_tcp_keepalives_idle,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("tcp_keepalives_interval",
						saved_tcp_keepalives_interval,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("tcp_keepalives_count", saved_tcp_keepalives_count,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("tcp_user_timeout", saved_tcp_user_timeout,
						PGC_USERSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session connection GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_parser_state_is_session_local);
Datum
test_session_parser_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_backslash_quote;
	char	   *saved_transform_null_equals;
	HTAB	   *saved_operator_lookup_cache;
	HTAB	   *session1_operator_cache;
	HTAB	   *session2_operator_cache;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_backslash_quote =
		pstrdup(GetConfigOption("backslash_quote", false, false));
	saved_transform_null_equals =
		pstrdup(GetConfigOption("transform_null_equals", false, false));
	saved_operator_lookup_cache = *PgCurrentOperatorLookupCacheRef();
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);
	session1_operator_cache = (HTAB *) &fake_session1;
	session2_operator_cache = (HTAB *) &fake_session2;

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && backslash_quote == BACKSLASH_QUOTE_SAFE_ENCODING;
		ok = ok && !Transform_null_equals;
		ok = ok && *PgCurrentOperatorLookupCacheRef() == NULL;
		SetConfigOption("backslash_quote", "on",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("transform_null_equals", "on",
						PGC_USERSET, PGC_S_SESSION);
		*PgCurrentOperatorLookupCacheRef() = session1_operator_cache;
		ok = ok && backslash_quote == BACKSLASH_QUOTE_ON;
		ok = ok && Transform_null_equals;
		ok = ok && *PgCurrentOperatorLookupCacheRef() ==
			session1_operator_cache;

		PgSetCurrentSession(&fake_session2);
		ok = ok && backslash_quote == BACKSLASH_QUOTE_SAFE_ENCODING;
		ok = ok && !Transform_null_equals;
		ok = ok && *PgCurrentOperatorLookupCacheRef() == NULL;
		SetConfigOption("backslash_quote", "off",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("transform_null_equals", "off",
						PGC_USERSET, PGC_S_SESSION);
		*PgCurrentOperatorLookupCacheRef() = session2_operator_cache;
		ok = ok && backslash_quote == BACKSLASH_QUOTE_OFF;
		ok = ok && !Transform_null_equals;
		ok = ok && *PgCurrentOperatorLookupCacheRef() ==
			session2_operator_cache;

		PgSetCurrentSession(&fake_session1);
		ok = ok && backslash_quote == BACKSLASH_QUOTE_ON;
		ok = ok && Transform_null_equals;
		ok = ok && *PgCurrentOperatorLookupCacheRef() ==
			session1_operator_cache;

		PgSetCurrentSession(&fake_session2);
		ok = ok && backslash_quote == BACKSLASH_QUOTE_OFF;
		ok = ok && !Transform_null_equals;
		ok = ok && *PgCurrentOperatorLookupCacheRef() ==
			session2_operator_cache;

		PgSetCurrentSession(saved_session);
		*PgCurrentOperatorLookupCacheRef() = saved_operator_lookup_cache;
		SetConfigOption("backslash_quote", saved_backslash_quote,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("transform_null_equals",
						saved_transform_null_equals,
						PGC_USERSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		*PgCurrentOperatorLookupCacheRef() = saved_operator_lookup_cache;
		SetConfigOption("backslash_quote", saved_backslash_quote,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("transform_null_equals",
						saved_transform_null_equals,
						PGC_USERSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session parser state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_vacuum_state_is_session_local);
Datum
test_session_vacuum_state_is_session_local(PG_FUNCTION_ARGS)
{
	enum
	{
		TEST_VACUUM_GUC_COUNT = 16
	};
	const char *guc_names[TEST_VACUUM_GUC_COUNT] = {
		"default_statistics_target",
		"track_cost_delay_timing",
		"vacuum_buffer_usage_limit",
		"vacuum_cost_delay",
		"vacuum_cost_limit",
		"vacuum_cost_page_dirty",
		"vacuum_cost_page_hit",
		"vacuum_cost_page_miss",
		"vacuum_failsafe_age",
		"vacuum_freeze_min_age",
		"vacuum_freeze_table_age",
		"vacuum_max_eager_freeze_failure_rate",
		"vacuum_multixact_failsafe_age",
		"vacuum_multixact_freeze_min_age",
		"vacuum_multixact_freeze_table_age",
		"vacuum_truncate"
	};
	const char *session1_values[TEST_VACUUM_GUC_COUNT] = {
		"101",
		"on",
		"4096",
		"2",
		"301",
		"31",
		"3",
		"5",
		"1700000000",
		"60000000",
		"160000000",
		"0.04",
		"1700000000",
		"6000000",
		"160000000",
		"off"
	};
	const char *session2_values[TEST_VACUUM_GUC_COUNT] = {
		"102",
		"off",
		"8192",
		"3",
		"302",
		"32",
		"4",
		"6",
		"1800000000",
		"70000000",
		"170000000",
		"0.05",
		"1800000000",
		"7000000",
		"170000000",
		"on"
	};
	char	   *saved_values[TEST_VACUUM_GUC_COUNT];
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	bool		ok = true;
	int			i;

	saved_session = CurrentPgSession;
	for (i = 0; i < TEST_VACUUM_GUC_COUNT; i++)
		saved_values[i] = pstrdup(GetConfigOption(guc_names[i], false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && default_statistics_target == 100;
		ok = ok && !track_cost_delay_timing;
		ok = ok && VacuumBufferUsageLimit == 2048;
		ok = ok && VacuumCostDelay == 0;
		ok = ok && VacuumCostLimit == 200;
		ok = ok && VacuumCostPageDirty == 20;
		ok = ok && VacuumCostPageHit == 1;
		ok = ok && VacuumCostPageMiss == 2;
		ok = ok && vacuum_failsafe_age == 1600000000;
		ok = ok && vacuum_freeze_min_age == 50000000;
		ok = ok && vacuum_freeze_table_age == 150000000;
		ok = ok && vacuum_max_eager_freeze_failure_rate > 0.029;
		ok = ok && vacuum_max_eager_freeze_failure_rate < 0.031;
		ok = ok && vacuum_multixact_failsafe_age == 1600000000;
		ok = ok && vacuum_multixact_freeze_min_age == 5000000;
		ok = ok && vacuum_multixact_freeze_table_age == 150000000;
		ok = ok && vacuum_truncate;
		ok = ok && vacuum_cost_delay == 0;
		ok = ok && vacuum_cost_limit == 200;
		for (i = 0; i < TEST_VACUUM_GUC_COUNT; i++)
			SetConfigOption(guc_names[i], session1_values[i],
							PGC_USERSET, PGC_S_SESSION);
		vacuum_cost_delay = 7.0;
		vacuum_cost_limit = 701;
		ok = ok && default_statistics_target == 101;
		ok = ok && track_cost_delay_timing;
		ok = ok && VacuumBufferUsageLimit == 4096;
		ok = ok && VacuumCostDelay == 2.0;
		ok = ok && VacuumCostLimit == 301;
		ok = ok && VacuumCostPageDirty == 31;
		ok = ok && VacuumCostPageHit == 3;
		ok = ok && VacuumCostPageMiss == 5;
		ok = ok && vacuum_failsafe_age == 1700000000;
		ok = ok && vacuum_freeze_min_age == 60000000;
		ok = ok && vacuum_freeze_table_age == 160000000;
		ok = ok && vacuum_max_eager_freeze_failure_rate > 0.039;
		ok = ok && vacuum_max_eager_freeze_failure_rate < 0.041;
		ok = ok && vacuum_multixact_failsafe_age == 1700000000;
		ok = ok && vacuum_multixact_freeze_min_age == 6000000;
		ok = ok && vacuum_multixact_freeze_table_age == 160000000;
		ok = ok && !vacuum_truncate;
		ok = ok && vacuum_cost_delay == 7.0;
		ok = ok && vacuum_cost_limit == 701;

		PgSetCurrentSession(&fake_session2);
		ok = ok && default_statistics_target == 100;
		ok = ok && !track_cost_delay_timing;
		ok = ok && VacuumBufferUsageLimit == 2048;
		ok = ok && VacuumCostDelay == 0;
		ok = ok && VacuumCostLimit == 200;
		ok = ok && VacuumCostPageDirty == 20;
		ok = ok && VacuumCostPageHit == 1;
		ok = ok && VacuumCostPageMiss == 2;
		ok = ok && vacuum_truncate;
		ok = ok && vacuum_cost_delay == 0;
		ok = ok && vacuum_cost_limit == 200;
		for (i = 0; i < TEST_VACUUM_GUC_COUNT; i++)
			SetConfigOption(guc_names[i], session2_values[i],
							PGC_USERSET, PGC_S_SESSION);
		vacuum_cost_delay = 9.0;
		vacuum_cost_limit = 901;
		ok = ok && default_statistics_target == 102;
		ok = ok && !track_cost_delay_timing;
		ok = ok && VacuumBufferUsageLimit == 8192;
		ok = ok && VacuumCostDelay == 3.0;
		ok = ok && VacuumCostLimit == 302;
		ok = ok && VacuumCostPageDirty == 32;
		ok = ok && VacuumCostPageHit == 4;
		ok = ok && VacuumCostPageMiss == 6;
		ok = ok && vacuum_failsafe_age == 1800000000;
		ok = ok && vacuum_freeze_min_age == 70000000;
		ok = ok && vacuum_freeze_table_age == 170000000;
		ok = ok && vacuum_max_eager_freeze_failure_rate > 0.049;
		ok = ok && vacuum_max_eager_freeze_failure_rate < 0.051;
		ok = ok && vacuum_multixact_failsafe_age == 1800000000;
		ok = ok && vacuum_multixact_freeze_min_age == 7000000;
		ok = ok && vacuum_multixact_freeze_table_age == 170000000;
		ok = ok && vacuum_truncate;
		ok = ok && vacuum_cost_delay == 9.0;
		ok = ok && vacuum_cost_limit == 901;

		PgSetCurrentSession(&fake_session1);
		ok = ok && default_statistics_target == 101;
		ok = ok && track_cost_delay_timing;
		ok = ok && VacuumBufferUsageLimit == 4096;
		ok = ok && VacuumCostDelay == 2.0;
		ok = ok && VacuumCostLimit == 301;
		ok = ok && VacuumCostPageDirty == 31;
		ok = ok && VacuumCostPageHit == 3;
		ok = ok && VacuumCostPageMiss == 5;
		ok = ok && vacuum_failsafe_age == 1700000000;
		ok = ok && !vacuum_truncate;
		ok = ok && vacuum_cost_delay == 7.0;
		ok = ok && vacuum_cost_limit == 701;

		PgSetCurrentSession(&fake_session2);
		ok = ok && default_statistics_target == 102;
		ok = ok && !track_cost_delay_timing;
		ok = ok && VacuumBufferUsageLimit == 8192;
		ok = ok && VacuumCostDelay == 3.0;
		ok = ok && VacuumCostLimit == 302;
		ok = ok && VacuumCostPageDirty == 32;
		ok = ok && VacuumCostPageHit == 4;
		ok = ok && VacuumCostPageMiss == 6;
		ok = ok && vacuum_failsafe_age == 1800000000;
		ok = ok && vacuum_truncate;
		ok = ok && vacuum_cost_delay == 9.0;
		ok = ok && vacuum_cost_limit == 901;

		PgSetCurrentSession(saved_session);
		for (i = 0; i < TEST_VACUUM_GUC_COUNT; i++)
			SetConfigOption(guc_names[i], saved_values[i],
							PGC_USERSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		for (i = 0; i < TEST_VACUUM_GUC_COUNT; i++)
			SetConfigOption(guc_names[i], saved_values[i],
							PGC_USERSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session vacuum GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_buffer_io_state_is_session_local);
Datum
test_session_buffer_io_state_is_session_local(PG_FUNCTION_ARGS)
{
	enum
	{
		TEST_BUFFER_IO_GUC_COUNT = 6
	};
	const char *guc_names[TEST_BUFFER_IO_GUC_COUNT] = {
		"backend_flush_after",
		"effective_io_concurrency",
		"io_combine_limit",
		"maintenance_io_concurrency",
		"track_io_timing",
		"zero_damaged_pages"
	};
	const char *session1_values[TEST_BUFFER_IO_GUC_COUNT] = {
		"8",
		"32",
		"8",
		"24",
		"on",
		"on"
	};
	const char *session2_values[TEST_BUFFER_IO_GUC_COUNT] = {
		"4",
		"16",
		"4",
		"12",
		"off",
		"off"
	};
	char	   *saved_values[TEST_BUFFER_IO_GUC_COUNT];
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	bool		ok = true;
	int			i;

	saved_session = CurrentPgSession;
	for (i = 0; i < TEST_BUFFER_IO_GUC_COUNT; i++)
		saved_values[i] = pstrdup(GetConfigOption(guc_names[i], false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && backend_flush_after == DEFAULT_BACKEND_FLUSH_AFTER;
		ok = ok && effective_io_concurrency == DEFAULT_EFFECTIVE_IO_CONCURRENCY;
		ok = ok && io_combine_limit == DEFAULT_IO_COMBINE_LIMIT;
		ok = ok && io_combine_limit_guc == DEFAULT_IO_COMBINE_LIMIT;
		ok = ok && maintenance_io_concurrency == DEFAULT_MAINTENANCE_IO_CONCURRENCY;
		ok = ok && !track_io_timing;
		ok = ok && !zero_damaged_pages;
		for (i = 0; i < TEST_BUFFER_IO_GUC_COUNT; i++)
			SetConfigOption(guc_names[i], session1_values[i],
							PGC_USERSET, PGC_S_SESSION);
		ok = ok && backend_flush_after == 8;
		ok = ok && effective_io_concurrency == 32;
		ok = ok && io_combine_limit == 8;
		ok = ok && io_combine_limit_guc == 8;
		ok = ok && maintenance_io_concurrency == 24;
		ok = ok && track_io_timing;
		ok = ok && zero_damaged_pages;

		PgSetCurrentSession(&fake_session2);
		ok = ok && backend_flush_after == DEFAULT_BACKEND_FLUSH_AFTER;
		ok = ok && effective_io_concurrency == DEFAULT_EFFECTIVE_IO_CONCURRENCY;
		ok = ok && io_combine_limit == DEFAULT_IO_COMBINE_LIMIT;
		ok = ok && io_combine_limit_guc == DEFAULT_IO_COMBINE_LIMIT;
		ok = ok && maintenance_io_concurrency == DEFAULT_MAINTENANCE_IO_CONCURRENCY;
		ok = ok && !track_io_timing;
		ok = ok && !zero_damaged_pages;
		for (i = 0; i < TEST_BUFFER_IO_GUC_COUNT; i++)
			SetConfigOption(guc_names[i], session2_values[i],
							PGC_USERSET, PGC_S_SESSION);
		ok = ok && backend_flush_after == 4;
		ok = ok && effective_io_concurrency == 16;
		ok = ok && io_combine_limit == 4;
		ok = ok && io_combine_limit_guc == 4;
		ok = ok && maintenance_io_concurrency == 12;
		ok = ok && !track_io_timing;
		ok = ok && !zero_damaged_pages;

		PgSetCurrentSession(&fake_session1);
		ok = ok && backend_flush_after == 8;
		ok = ok && effective_io_concurrency == 32;
		ok = ok && io_combine_limit == 8;
		ok = ok && io_combine_limit_guc == 8;
		ok = ok && maintenance_io_concurrency == 24;
		ok = ok && track_io_timing;
		ok = ok && zero_damaged_pages;

		PgSetCurrentSession(&fake_session2);
		ok = ok && backend_flush_after == 4;
		ok = ok && effective_io_concurrency == 16;
		ok = ok && io_combine_limit == 4;
		ok = ok && io_combine_limit_guc == 4;
		ok = ok && maintenance_io_concurrency == 12;
		ok = ok && !track_io_timing;
		ok = ok && !zero_damaged_pages;

		PgSetCurrentSession(saved_session);
		for (i = 0; i < TEST_BUFFER_IO_GUC_COUNT; i++)
			SetConfigOption(guc_names[i], saved_values[i],
							PGC_USERSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		for (i = 0; i < TEST_BUFFER_IO_GUC_COUNT; i++)
			SetConfigOption(guc_names[i], saved_values[i],
							PGC_USERSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session buffer I/O GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_xact_defaults_are_session_local);
Datum
test_session_xact_defaults_are_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_default_xact_deferrable;
	char	   *saved_default_xact_isolation;
	char	   *saved_default_xact_read_only;
	char	   *saved_synchronous_commit;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_default_xact_deferrable =
		pstrdup(GetConfigOption("default_transaction_deferrable", false,
								false));
	saved_default_xact_isolation =
		pstrdup(GetConfigOption("default_transaction_isolation", false,
								false));
	saved_default_xact_read_only =
		pstrdup(GetConfigOption("default_transaction_read_only", false,
								false));
	saved_synchronous_commit =
		pstrdup(GetConfigOption("synchronous_commit", false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && DefaultXactIsoLevel == XACT_READ_COMMITTED;
		ok = ok && !DefaultXactReadOnly;
		ok = ok && !DefaultXactDeferrable;
		ok = ok && synchronous_commit == SYNCHRONOUS_COMMIT_ON;
		SetConfigOption("default_transaction_isolation", "serializable",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("default_transaction_read_only", "on",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("default_transaction_deferrable", "on",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("synchronous_commit", "remote_apply",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && DefaultXactIsoLevel == XACT_SERIALIZABLE;
		ok = ok && DefaultXactReadOnly;
		ok = ok && DefaultXactDeferrable;
		ok = ok && synchronous_commit == SYNCHRONOUS_COMMIT_REMOTE_APPLY;

		PgSetCurrentSession(&fake_session2);
		ok = ok && DefaultXactIsoLevel == XACT_READ_COMMITTED;
		ok = ok && !DefaultXactReadOnly;
		ok = ok && !DefaultXactDeferrable;
		ok = ok && synchronous_commit == SYNCHRONOUS_COMMIT_ON;
		SetConfigOption("default_transaction_isolation", "repeatable read",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("default_transaction_read_only", "off",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("default_transaction_deferrable", "off",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("synchronous_commit", "local",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && DefaultXactIsoLevel == XACT_REPEATABLE_READ;
		ok = ok && !DefaultXactReadOnly;
		ok = ok && !DefaultXactDeferrable;
		ok = ok && synchronous_commit == SYNCHRONOUS_COMMIT_LOCAL_FLUSH;

		PgSetCurrentSession(&fake_session1);
		ok = ok && DefaultXactIsoLevel == XACT_SERIALIZABLE;
		ok = ok && DefaultXactReadOnly;
		ok = ok && DefaultXactDeferrable;
		ok = ok && synchronous_commit == SYNCHRONOUS_COMMIT_REMOTE_APPLY;

		PgSetCurrentSession(&fake_session2);
		ok = ok && DefaultXactIsoLevel == XACT_REPEATABLE_READ;
		ok = ok && !DefaultXactReadOnly;
		ok = ok && !DefaultXactDeferrable;
		ok = ok && synchronous_commit == SYNCHRONOUS_COMMIT_LOCAL_FLUSH;

		PgSetCurrentSession(saved_session);
		SetConfigOption("default_transaction_deferrable",
						saved_default_xact_deferrable,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("default_transaction_isolation",
						saved_default_xact_isolation,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("default_transaction_read_only",
						saved_default_xact_read_only,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("synchronous_commit", saved_synchronous_commit,
						PGC_USERSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("default_transaction_deferrable",
						saved_default_xact_deferrable,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("default_transaction_isolation",
						saved_default_xact_isolation,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("default_transaction_read_only",
						saved_default_xact_read_only,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("synchronous_commit", saved_synchronous_commit,
						PGC_USERSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session transaction default GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_lock_wait_state_is_session_local);
Datum
test_session_lock_wait_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_deadlock_timeout;
	char	   *saved_statement_timeout;
	char	   *saved_lock_timeout;
	char	   *saved_idle_in_transaction_session_timeout;
	char	   *saved_transaction_timeout;
	char	   *saved_idle_session_timeout;
	char	   *saved_log_lock_waits;
	char	   *saved_log_lock_failures;
#ifdef LOCK_DEBUG
	char	   *saved_debug_deadlocks;
	char	   *saved_trace_lock_oidmin;
	char	   *saved_trace_lock_table;
	char	   *saved_trace_locks;
	char	   *saved_trace_lwlocks;
	char	   *saved_trace_userlocks;
#endif
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_deadlock_timeout =
		pstrdup(GetConfigOption("deadlock_timeout", false, false));
	saved_statement_timeout =
		pstrdup(GetConfigOption("statement_timeout", false, false));
	saved_lock_timeout =
		pstrdup(GetConfigOption("lock_timeout", false, false));
	saved_idle_in_transaction_session_timeout =
		pstrdup(GetConfigOption("idle_in_transaction_session_timeout",
								false, false));
	saved_transaction_timeout =
		pstrdup(GetConfigOption("transaction_timeout", false, false));
	saved_idle_session_timeout =
		pstrdup(GetConfigOption("idle_session_timeout", false, false));
	saved_log_lock_waits =
		pstrdup(GetConfigOption("log_lock_waits", false, false));
	saved_log_lock_failures =
		pstrdup(GetConfigOption("log_lock_failures", false, false));
#ifdef LOCK_DEBUG
	saved_debug_deadlocks =
		pstrdup(GetConfigOption("debug_deadlocks", false, false));
	saved_trace_lock_oidmin =
		pstrdup(GetConfigOption("trace_lock_oidmin", false, false));
	saved_trace_lock_table =
		pstrdup(GetConfigOption("trace_lock_table", false, false));
	saved_trace_locks =
		pstrdup(GetConfigOption("trace_locks", false, false));
	saved_trace_lwlocks =
		pstrdup(GetConfigOption("trace_lwlocks", false, false));
	saved_trace_userlocks =
		pstrdup(GetConfigOption("trace_userlocks", false, false));
#endif
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && DeadlockTimeout == 1000;
		ok = ok && StatementTimeout == 0;
		ok = ok && LockTimeout == 0;
		ok = ok && IdleInTransactionSessionTimeout == 0;
		ok = ok && TransactionTimeout == 0;
		ok = ok && IdleSessionTimeout == 0;
		ok = ok && log_lock_waits;
		ok = ok && !log_lock_failures;
		SetConfigOption("deadlock_timeout", "2000",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("statement_timeout", "3000",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("lock_timeout", "4000",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("idle_in_transaction_session_timeout", "5000",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("transaction_timeout", "6000",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("idle_session_timeout", "7000",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_lock_waits", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_lock_failures", "on",
						PGC_SUSET, PGC_S_SESSION);
#ifdef LOCK_DEBUG
		SetConfigOption("debug_deadlocks", "on",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_lock_oidmin", "20000",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_lock_table", "30000",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_locks", "on",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_lwlocks", "on",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_userlocks", "on",
						PGC_SUSET, PGC_S_SESSION);
#endif
		ok = ok && DeadlockTimeout == 2000;
		ok = ok && StatementTimeout == 3000;
		ok = ok && LockTimeout == 4000;
		ok = ok && IdleInTransactionSessionTimeout == 5000;
		ok = ok && TransactionTimeout == 6000;
		ok = ok && IdleSessionTimeout == 7000;
		ok = ok && !log_lock_waits;
		ok = ok && log_lock_failures;
#ifdef LOCK_DEBUG
		ok = ok && Debug_deadlocks;
		ok = ok && Trace_lock_oidmin == 20000;
		ok = ok && Trace_lock_table == 30000;
		ok = ok && Trace_locks;
		ok = ok && Trace_lwlocks;
		ok = ok && Trace_userlocks;
#endif

		PgSetCurrentSession(&fake_session2);
		ok = ok && DeadlockTimeout == 1000;
		ok = ok && StatementTimeout == 0;
		ok = ok && LockTimeout == 0;
		ok = ok && IdleInTransactionSessionTimeout == 0;
		ok = ok && TransactionTimeout == 0;
		ok = ok && IdleSessionTimeout == 0;
		ok = ok && log_lock_waits;
		ok = ok && !log_lock_failures;
		SetConfigOption("deadlock_timeout", "1100",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("statement_timeout", "1200",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("lock_timeout", "1300",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("idle_in_transaction_session_timeout", "1400",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("transaction_timeout", "1500",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("idle_session_timeout", "1600",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_lock_waits", "on",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_lock_failures", "off",
						PGC_SUSET, PGC_S_SESSION);
#ifdef LOCK_DEBUG
		SetConfigOption("debug_deadlocks", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_lock_oidmin", "10000",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_lock_table", "0",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_locks", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_lwlocks", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_userlocks", "off",
						PGC_SUSET, PGC_S_SESSION);
#endif
		ok = ok && DeadlockTimeout == 1100;
		ok = ok && StatementTimeout == 1200;
		ok = ok && LockTimeout == 1300;
		ok = ok && IdleInTransactionSessionTimeout == 1400;
		ok = ok && TransactionTimeout == 1500;
		ok = ok && IdleSessionTimeout == 1600;
		ok = ok && log_lock_waits;
		ok = ok && !log_lock_failures;
#ifdef LOCK_DEBUG
		ok = ok && !Debug_deadlocks;
		ok = ok && Trace_lock_oidmin == 10000;
		ok = ok && Trace_lock_table == 0;
		ok = ok && !Trace_locks;
		ok = ok && !Trace_lwlocks;
		ok = ok && !Trace_userlocks;
#endif

		PgSetCurrentSession(&fake_session1);
		ok = ok && DeadlockTimeout == 2000;
		ok = ok && StatementTimeout == 3000;
		ok = ok && LockTimeout == 4000;
		ok = ok && IdleInTransactionSessionTimeout == 5000;
		ok = ok && TransactionTimeout == 6000;
		ok = ok && IdleSessionTimeout == 7000;
		ok = ok && !log_lock_waits;
		ok = ok && log_lock_failures;

		PgSetCurrentSession(&fake_session2);
		ok = ok && DeadlockTimeout == 1100;
		ok = ok && StatementTimeout == 1200;
		ok = ok && LockTimeout == 1300;
		ok = ok && IdleInTransactionSessionTimeout == 1400;
		ok = ok && TransactionTimeout == 1500;
		ok = ok && IdleSessionTimeout == 1600;
		ok = ok && log_lock_waits;
		ok = ok && !log_lock_failures;

		PgSetCurrentSession(saved_session);
		SetConfigOption("deadlock_timeout", saved_deadlock_timeout,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("statement_timeout", saved_statement_timeout,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("lock_timeout", saved_lock_timeout,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("idle_in_transaction_session_timeout",
						saved_idle_in_transaction_session_timeout,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("transaction_timeout", saved_transaction_timeout,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("idle_session_timeout", saved_idle_session_timeout,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_lock_waits", saved_log_lock_waits,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_lock_failures", saved_log_lock_failures,
						PGC_SUSET, PGC_S_SESSION);
#ifdef LOCK_DEBUG
		SetConfigOption("debug_deadlocks", saved_debug_deadlocks,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_lock_oidmin", saved_trace_lock_oidmin,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_lock_table", saved_trace_lock_table,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_locks", saved_trace_locks,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_lwlocks", saved_trace_lwlocks,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_userlocks", saved_trace_userlocks,
						PGC_SUSET, PGC_S_SESSION);
#endif
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("deadlock_timeout", saved_deadlock_timeout,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("statement_timeout", saved_statement_timeout,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("lock_timeout", saved_lock_timeout,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("idle_in_transaction_session_timeout",
						saved_idle_in_transaction_session_timeout,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("transaction_timeout", saved_transaction_timeout,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("idle_session_timeout", saved_idle_session_timeout,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_lock_waits", saved_log_lock_waits,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_lock_failures", saved_log_lock_failures,
						PGC_SUSET, PGC_S_SESSION);
#ifdef LOCK_DEBUG
		SetConfigOption("debug_deadlocks", saved_debug_deadlocks,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_lock_oidmin", saved_trace_lock_oidmin,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_lock_table", saved_trace_lock_table,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_locks", saved_trace_locks,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_lwlocks", saved_trace_lwlocks,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_userlocks", saved_trace_userlocks,
						PGC_SUSET, PGC_S_SESSION);
#endif
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session lock/wait GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_logging_state_is_session_local);
Datum
test_session_logging_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_debug_pretty_print;
	char	   *saved_debug_print_parse;
	char	   *saved_debug_print_plan;
	char	   *saved_debug_print_raw_parse;
	char	   *saved_debug_print_rewritten;
	char	   *saved_log_parser_stats;
	char	   *saved_log_planner_stats;
	char	   *saved_log_executor_stats;
	char	   *saved_log_statement_stats;
#ifdef BTREE_BUILD_STATS
	char	   *saved_log_btree_build_stats;
#endif
	char	   *saved_log_duration;
	char	   *saved_log_error_verbosity;
	char	   *saved_log_parameter_max_length;
	char	   *saved_log_parameter_max_length_on_error;
	char	   *saved_log_min_error_statement;
	char	   *saved_log_min_messages;
	char	   *saved_client_min_messages;
	char	   *saved_log_min_duration_sample;
	char	   *saved_log_min_duration_statement;
	char	   *saved_log_temp_files;
	char	   *saved_log_statement_sample_rate;
	char	   *saved_log_transaction_sample_rate;
	char	   *saved_backtrace_functions;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_debug_pretty_print =
		pstrdup(GetConfigOption("debug_pretty_print", false, false));
	saved_debug_print_parse =
		pstrdup(GetConfigOption("debug_print_parse", false, false));
	saved_debug_print_plan =
		pstrdup(GetConfigOption("debug_print_plan", false, false));
	saved_debug_print_raw_parse =
		pstrdup(GetConfigOption("debug_print_raw_parse", false, false));
	saved_debug_print_rewritten =
		pstrdup(GetConfigOption("debug_print_rewritten", false, false));
	saved_log_parser_stats =
		pstrdup(GetConfigOption("log_parser_stats", false, false));
	saved_log_planner_stats =
		pstrdup(GetConfigOption("log_planner_stats", false, false));
	saved_log_executor_stats =
		pstrdup(GetConfigOption("log_executor_stats", false, false));
	saved_log_statement_stats =
		pstrdup(GetConfigOption("log_statement_stats", false, false));
#ifdef BTREE_BUILD_STATS
	saved_log_btree_build_stats =
		pstrdup(GetConfigOption("log_btree_build_stats", false, false));
#endif
	saved_log_duration =
		pstrdup(GetConfigOption("log_duration", false, false));
	saved_log_error_verbosity =
		pstrdup(GetConfigOption("log_error_verbosity", false, false));
	saved_log_parameter_max_length =
		pstrdup(GetConfigOption("log_parameter_max_length", false, false));
	saved_log_parameter_max_length_on_error =
		pstrdup(GetConfigOption("log_parameter_max_length_on_error",
								false, false));
	saved_log_min_error_statement =
		pstrdup(GetConfigOption("log_min_error_statement", false, false));
	saved_log_min_messages =
		pstrdup(GetConfigOption("log_min_messages", false, false));
	saved_client_min_messages =
		pstrdup(GetConfigOption("client_min_messages", false, false));
	saved_log_min_duration_sample =
		pstrdup(GetConfigOption("log_min_duration_sample", false, false));
	saved_log_min_duration_statement =
		pstrdup(GetConfigOption("log_min_duration_statement", false, false));
	saved_log_temp_files =
		pstrdup(GetConfigOption("log_temp_files", false, false));
	saved_log_statement_sample_rate =
		pstrdup(GetConfigOption("log_statement_sample_rate", false, false));
	saved_log_transaction_sample_rate =
		pstrdup(GetConfigOption("log_transaction_sample_rate", false, false));
	saved_backtrace_functions =
		pstrdup(GetConfigOption("backtrace_functions", false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && !Debug_print_plan;
		ok = ok && !Debug_print_parse;
		ok = ok && !Debug_print_raw_parse;
		ok = ok && !Debug_print_rewritten;
		ok = ok && Debug_pretty_print;
		ok = ok && !log_parser_stats;
		ok = ok && !log_planner_stats;
		ok = ok && !log_executor_stats;
		ok = ok && !log_statement_stats;
#ifdef BTREE_BUILD_STATS
		ok = ok && !log_btree_build_stats;
#endif
		ok = ok && !log_duration;
		ok = ok && Log_error_verbosity == PGERROR_DEFAULT;
		ok = ok && log_parameter_max_length == -1;
		ok = ok && log_parameter_max_length_on_error == 0;
		ok = ok && log_min_error_statement == ERROR;
		ok = ok && log_min_messages[MyBackendType] == WARNING;
		ok = ok && client_min_messages == NOTICE;
		ok = ok && log_min_duration_sample == -1;
		ok = ok && log_min_duration_statement == -1;
		ok = ok && log_temp_files == -1;
		ok = ok && log_statement_sample_rate == 1.0;
		ok = ok && log_xact_sample_rate == 0;

		SetConfigOption("debug_pretty_print", "off",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_print_parse", "on",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_print_plan", "on",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_print_raw_parse", "on",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_print_rewritten", "on",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_statement_stats", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_parser_stats", "on",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_planner_stats", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_executor_stats", "off",
						PGC_SUSET, PGC_S_SESSION);
#ifdef BTREE_BUILD_STATS
		SetConfigOption("log_btree_build_stats", "on",
						PGC_SUSET, PGC_S_SESSION);
#endif
		SetConfigOption("log_duration", "on",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_error_verbosity", "verbose",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_parameter_max_length", "128",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_parameter_max_length_on_error", "256",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_min_error_statement", "fatal",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_min_messages", "error",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("client_min_messages", "warning",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_min_duration_sample", "1000",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_min_duration_statement", "2000",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_temp_files", "3000",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_statement_sample_rate", "0.25",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_transaction_sample_rate", "0.5",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("backtrace_functions", "errstart",
						PGC_SUSET, PGC_S_SESSION);
		ok = ok && !Debug_pretty_print;
		ok = ok && Debug_print_parse;
		ok = ok && Debug_print_plan;
		ok = ok && Debug_print_raw_parse;
		ok = ok && Debug_print_rewritten;
		ok = ok && log_parser_stats;
		ok = ok && !log_planner_stats;
		ok = ok && !log_executor_stats;
		ok = ok && !log_statement_stats;
#ifdef BTREE_BUILD_STATS
		ok = ok && log_btree_build_stats;
#endif
		ok = ok && log_duration;
		ok = ok && Log_error_verbosity == PGERROR_VERBOSE;
		ok = ok && log_parameter_max_length == 128;
		ok = ok && log_parameter_max_length_on_error == 256;
		ok = ok && log_min_error_statement == FATAL;
		ok = ok && log_min_messages[MyBackendType] == ERROR;
		ok = ok && client_min_messages == WARNING;
		ok = ok && log_min_duration_sample == 1000;
		ok = ok && log_min_duration_statement == 2000;
		ok = ok && log_temp_files == 3000;
		ok = ok && log_statement_sample_rate == 0.25;
		ok = ok && log_xact_sample_rate == 0.5;
		ok = ok && backtrace_functions != NULL &&
			strcmp(backtrace_functions, "errstart") == 0;

		PgSetCurrentSession(&fake_session2);
		ok = ok && !Debug_print_plan;
		ok = ok && !Debug_print_parse;
		ok = ok && !Debug_print_raw_parse;
		ok = ok && !Debug_print_rewritten;
		ok = ok && Debug_pretty_print;
		ok = ok && log_min_messages[MyBackendType] == WARNING;
		SetConfigOption("debug_pretty_print", "on",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_print_parse", "off",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_print_plan", "off",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_print_raw_parse", "off",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_print_rewritten", "off",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_parser_stats", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_planner_stats", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_executor_stats", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_statement_stats", "on",
						PGC_SUSET, PGC_S_SESSION);
#ifdef BTREE_BUILD_STATS
		SetConfigOption("log_btree_build_stats", "off",
						PGC_SUSET, PGC_S_SESSION);
#endif
		SetConfigOption("log_duration", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_error_verbosity", "terse",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_parameter_max_length", "64",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_parameter_max_length_on_error", "32",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_min_error_statement", "panic",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_min_messages", "debug1",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("client_min_messages", "debug1",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_min_duration_sample", "3000",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_min_duration_statement", "4000",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_temp_files", "5000",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_statement_sample_rate", "0.5",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_transaction_sample_rate", "0.25",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("backtrace_functions", "errmsg",
						PGC_SUSET, PGC_S_SESSION);
		ok = ok && Debug_pretty_print;
		ok = ok && !Debug_print_parse;
		ok = ok && !Debug_print_plan;
		ok = ok && !Debug_print_raw_parse;
		ok = ok && !Debug_print_rewritten;
		ok = ok && !log_parser_stats;
		ok = ok && !log_planner_stats;
		ok = ok && !log_executor_stats;
		ok = ok && log_statement_stats;
#ifdef BTREE_BUILD_STATS
		ok = ok && !log_btree_build_stats;
#endif
		ok = ok && !log_duration;
		ok = ok && Log_error_verbosity == PGERROR_TERSE;
		ok = ok && log_parameter_max_length == 64;
		ok = ok && log_parameter_max_length_on_error == 32;
		ok = ok && log_min_error_statement == PANIC;
		ok = ok && log_min_messages[MyBackendType] == DEBUG1;
		ok = ok && client_min_messages == DEBUG1;
		ok = ok && log_min_duration_sample == 3000;
		ok = ok && log_min_duration_statement == 4000;
		ok = ok && log_temp_files == 5000;
		ok = ok && log_statement_sample_rate == 0.5;
		ok = ok && log_xact_sample_rate == 0.25;
		ok = ok && backtrace_functions != NULL &&
			strcmp(backtrace_functions, "errmsg") == 0;

		PgSetCurrentSession(&fake_session1);
		ok = ok && !Debug_pretty_print;
		ok = ok && Debug_print_parse;
		ok = ok && Debug_print_plan;
		ok = ok && Debug_print_raw_parse;
		ok = ok && Debug_print_rewritten;
		ok = ok && log_parser_stats;
		ok = ok && !log_statement_stats;
		ok = ok && log_min_messages[MyBackendType] == ERROR;
		ok = ok && client_min_messages == WARNING;
		ok = ok && backtrace_functions != NULL &&
			strcmp(backtrace_functions, "errstart") == 0;

		PgSetCurrentSession(&fake_session2);
		ok = ok && Debug_pretty_print;
		ok = ok && !Debug_print_parse;
		ok = ok && log_statement_stats;
		ok = ok && log_min_messages[MyBackendType] == DEBUG1;
		ok = ok && client_min_messages == DEBUG1;
		ok = ok && backtrace_functions != NULL &&
			strcmp(backtrace_functions, "errmsg") == 0;

		PgSetCurrentSession(saved_session);
		SetConfigOption("log_statement_stats", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_parser_stats", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_planner_stats", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_executor_stats", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("debug_pretty_print", saved_debug_pretty_print,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_print_parse", saved_debug_print_parse,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_print_plan", saved_debug_print_plan,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_print_raw_parse", saved_debug_print_raw_parse,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_print_rewritten", saved_debug_print_rewritten,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_parser_stats", saved_log_parser_stats,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_planner_stats", saved_log_planner_stats,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_executor_stats", saved_log_executor_stats,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_statement_stats", saved_log_statement_stats,
						PGC_SUSET, PGC_S_SESSION);
#ifdef BTREE_BUILD_STATS
		SetConfigOption("log_btree_build_stats", saved_log_btree_build_stats,
						PGC_SUSET, PGC_S_SESSION);
#endif
		SetConfigOption("log_duration", saved_log_duration,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_error_verbosity", saved_log_error_verbosity,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_parameter_max_length",
						saved_log_parameter_max_length,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_parameter_max_length_on_error",
						saved_log_parameter_max_length_on_error,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_min_error_statement",
						saved_log_min_error_statement,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_min_messages", saved_log_min_messages,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("client_min_messages", saved_client_min_messages,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_min_duration_sample",
						saved_log_min_duration_sample,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_min_duration_statement",
						saved_log_min_duration_statement,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_temp_files", saved_log_temp_files,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_statement_sample_rate",
						saved_log_statement_sample_rate,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_transaction_sample_rate",
						saved_log_transaction_sample_rate,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("backtrace_functions", saved_backtrace_functions,
						PGC_SUSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("log_statement_stats", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_parser_stats", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_planner_stats", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_executor_stats", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("debug_pretty_print", saved_debug_pretty_print,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_print_parse", saved_debug_print_parse,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_print_plan", saved_debug_print_plan,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_print_raw_parse", saved_debug_print_raw_parse,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_print_rewritten", saved_debug_print_rewritten,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_parser_stats", saved_log_parser_stats,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_planner_stats", saved_log_planner_stats,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_executor_stats", saved_log_executor_stats,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_statement_stats", saved_log_statement_stats,
						PGC_SUSET, PGC_S_SESSION);
#ifdef BTREE_BUILD_STATS
		SetConfigOption("log_btree_build_stats", saved_log_btree_build_stats,
						PGC_SUSET, PGC_S_SESSION);
#endif
		SetConfigOption("log_duration", saved_log_duration,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_error_verbosity", saved_log_error_verbosity,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_parameter_max_length",
						saved_log_parameter_max_length,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_parameter_max_length_on_error",
						saved_log_parameter_max_length_on_error,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_min_error_statement",
						saved_log_min_error_statement,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_min_messages", saved_log_min_messages,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("client_min_messages", saved_client_min_messages,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_min_duration_sample",
						saved_log_min_duration_sample,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_min_duration_statement",
						saved_log_min_duration_statement,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_temp_files", saved_log_temp_files,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_statement_sample_rate",
						saved_log_statement_sample_rate,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_transaction_sample_rate",
						saved_log_transaction_sample_rate,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("backtrace_functions", saved_backtrace_functions,
						PGC_SUSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session logging GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_pgstat_state_is_session_local);
Datum
test_session_pgstat_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_stats_fetch_consistency;
	char	   *saved_track_activities;
	char	   *saved_track_counts;
	char	   *saved_track_functions;
	SessionEndType saved_session_end_cause;
	PgStat_Counter saved_last_session_report_time;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_stats_fetch_consistency =
		pstrdup(GetConfigOption("stats_fetch_consistency", false, false));
	saved_track_activities =
		pstrdup(GetConfigOption("track_activities", false, false));
	saved_track_counts =
		pstrdup(GetConfigOption("track_counts", false, false));
	saved_track_functions =
		pstrdup(GetConfigOption("track_functions", false, false));
	saved_session_end_cause = pgStatSessionEndCause;
	saved_last_session_report_time = *PgCurrentPgStatLastSessionReportTimeRef();
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && pgstat_track_counts;
		ok = ok && pgstat_track_activities;
		ok = ok && pgstat_track_functions == TRACK_FUNC_OFF;
		ok = ok &&
			pgstat_fetch_consistency == PGSTAT_FETCH_CONSISTENCY_CACHE;
		ok = ok && pgStatSessionEndCause == DISCONNECT_NORMAL;
		ok = ok && *PgCurrentPgStatLastSessionReportTimeRef() == 0;

		SetConfigOption("track_counts", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("track_activities", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("track_functions", "all",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("stats_fetch_consistency", "none",
						PGC_USERSET, PGC_S_SESSION);
		pgStatSessionEndCause = DISCONNECT_CLIENT_EOF;
		*PgCurrentPgStatLastSessionReportTimeRef() = 12345;
		ok = ok && !pgstat_track_counts;
		ok = ok && !pgstat_track_activities;
		ok = ok && pgstat_track_functions == TRACK_FUNC_ALL;
		ok = ok &&
			pgstat_fetch_consistency == PGSTAT_FETCH_CONSISTENCY_NONE;
		ok = ok && pgStatSessionEndCause == DISCONNECT_CLIENT_EOF;
		ok = ok && *PgCurrentPgStatLastSessionReportTimeRef() == 12345;

		PgSetCurrentSession(&fake_session2);
		ok = ok && pgstat_track_counts;
		ok = ok && pgstat_track_activities;
		ok = ok && pgstat_track_functions == TRACK_FUNC_OFF;
		ok = ok &&
			pgstat_fetch_consistency == PGSTAT_FETCH_CONSISTENCY_CACHE;
		ok = ok && pgStatSessionEndCause == DISCONNECT_NORMAL;
		ok = ok && *PgCurrentPgStatLastSessionReportTimeRef() == 0;
		SetConfigOption("track_counts", "on",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("track_activities", "on",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("track_functions", "pl",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("stats_fetch_consistency", "snapshot",
						PGC_USERSET, PGC_S_SESSION);
		pgStatSessionEndCause = DISCONNECT_KILLED;
		*PgCurrentPgStatLastSessionReportTimeRef() = 67890;
		ok = ok && pgstat_track_counts;
		ok = ok && pgstat_track_activities;
		ok = ok && pgstat_track_functions == TRACK_FUNC_PL;
		ok = ok &&
			pgstat_fetch_consistency == PGSTAT_FETCH_CONSISTENCY_SNAPSHOT;
		ok = ok && pgStatSessionEndCause == DISCONNECT_KILLED;
		ok = ok && *PgCurrentPgStatLastSessionReportTimeRef() == 67890;

		PgSetCurrentSession(&fake_session1);
		ok = ok && !pgstat_track_counts;
		ok = ok && !pgstat_track_activities;
		ok = ok && pgstat_track_functions == TRACK_FUNC_ALL;
		ok = ok &&
			pgstat_fetch_consistency == PGSTAT_FETCH_CONSISTENCY_NONE;
		ok = ok && pgStatSessionEndCause == DISCONNECT_CLIENT_EOF;
		ok = ok && *PgCurrentPgStatLastSessionReportTimeRef() == 12345;

		PgSetCurrentSession(&fake_session2);
		ok = ok && pgstat_track_counts;
		ok = ok && pgstat_track_activities;
		ok = ok && pgstat_track_functions == TRACK_FUNC_PL;
		ok = ok &&
			pgstat_fetch_consistency == PGSTAT_FETCH_CONSISTENCY_SNAPSHOT;
		ok = ok && pgStatSessionEndCause == DISCONNECT_KILLED;
		ok = ok && *PgCurrentPgStatLastSessionReportTimeRef() == 67890;

		PgSetCurrentSession(saved_session);
		SetConfigOption("stats_fetch_consistency",
						saved_stats_fetch_consistency,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("track_activities", saved_track_activities,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("track_counts", saved_track_counts,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("track_functions", saved_track_functions,
						PGC_SUSET, PGC_S_SESSION);
		pgStatSessionEndCause = saved_session_end_cause;
		*PgCurrentPgStatLastSessionReportTimeRef() =
			saved_last_session_report_time;
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("stats_fetch_consistency",
						saved_stats_fetch_consistency,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("track_activities", saved_track_activities,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("track_counts", saved_track_counts,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("track_functions", saved_track_functions,
						PGC_SUSET, PGC_S_SESSION);
		pgStatSessionEndCause = saved_session_end_cause;
		*PgCurrentPgStatLastSessionReportTimeRef() =
			saved_last_session_report_time;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session pgstat state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_query_id_state_is_session_local);
Datum
test_session_query_id_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_compute_query_id;
	bool		saved_query_id_enabled;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_compute_query_id =
		pstrdup(GetConfigOption("compute_query_id", false, false));
	saved_query_id_enabled = query_id_enabled;
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && compute_query_id == COMPUTE_QUERY_ID_AUTO;
		ok = ok && !query_id_enabled;
		ok = ok && !IsQueryIdEnabled();

		SetConfigOption("compute_query_id", "off",
						PGC_SUSET, PGC_S_SESSION);
		query_id_enabled = true;
		ok = ok && compute_query_id == COMPUTE_QUERY_ID_OFF;
		ok = ok && query_id_enabled;
		ok = ok && !IsQueryIdEnabled();

		PgSetCurrentSession(&fake_session2);
		ok = ok && compute_query_id == COMPUTE_QUERY_ID_AUTO;
		ok = ok && !query_id_enabled;
		ok = ok && !IsQueryIdEnabled();

		SetConfigOption("compute_query_id", "auto",
						PGC_SUSET, PGC_S_SESSION);
		EnableQueryId();
		ok = ok && compute_query_id == COMPUTE_QUERY_ID_AUTO;
		ok = ok && query_id_enabled;
		ok = ok && IsQueryIdEnabled();

		PgSetCurrentSession(&fake_session1);
		ok = ok && compute_query_id == COMPUTE_QUERY_ID_OFF;
		ok = ok && query_id_enabled;
		ok = ok && !IsQueryIdEnabled();

		PgSetCurrentSession(&fake_session2);
		ok = ok && compute_query_id == COMPUTE_QUERY_ID_AUTO;
		ok = ok && query_id_enabled;
		ok = ok && IsQueryIdEnabled();

		PgSetCurrentSession(saved_session);
		SetConfigOption("compute_query_id", saved_compute_query_id,
						PGC_SUSET, PGC_S_SESSION);
		query_id_enabled = saved_query_id_enabled;
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("compute_query_id", saved_compute_query_id,
						PGC_SUSET, PGC_S_SESSION);
		query_id_enabled = saved_query_id_enabled;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session query ID state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_storage_guc_state_is_session_local);
Datum
test_session_storage_guc_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_ignore_checksum_failure;
	char	   *saved_file_copy_method;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_ignore_checksum_failure =
		pstrdup(GetConfigOption("ignore_checksum_failure", false, false));
	saved_file_copy_method =
		pstrdup(GetConfigOption("file_copy_method", false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && !ignore_checksum_failure;
		ok = ok && file_copy_method == FILE_COPY_METHOD_COPY;

		SetConfigOption("ignore_checksum_failure", "on",
						PGC_SUSET, PGC_S_SESSION);
		file_copy_method = FILE_COPY_METHOD_CLONE;
		ok = ok && ignore_checksum_failure;
		ok = ok && file_copy_method == FILE_COPY_METHOD_CLONE;

		PgSetCurrentSession(&fake_session2);
		ok = ok && !ignore_checksum_failure;
		ok = ok && file_copy_method == FILE_COPY_METHOD_COPY;
		SetConfigOption("ignore_checksum_failure", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("file_copy_method", "copy",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && !ignore_checksum_failure;
		ok = ok && file_copy_method == FILE_COPY_METHOD_COPY;

		PgSetCurrentSession(&fake_session1);
		ok = ok && ignore_checksum_failure;
		ok = ok && file_copy_method == FILE_COPY_METHOD_CLONE;

		PgSetCurrentSession(&fake_session2);
		ok = ok && !ignore_checksum_failure;
		ok = ok && file_copy_method == FILE_COPY_METHOD_COPY;

		PgSetCurrentSession(saved_session);
		SetConfigOption("ignore_checksum_failure",
						saved_ignore_checksum_failure,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("file_copy_method", saved_file_copy_method,
						PGC_USERSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("ignore_checksum_failure",
						saved_ignore_checksum_failure,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("file_copy_method", saved_file_copy_method,
						PGC_USERSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session storage GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_user_guc_state_is_session_local);
Datum
test_session_user_guc_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_password_encryption;
	char	   *saved_createrole_self_grant;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_password_encryption =
		pstrdup(GetConfigOption("password_encryption", false, false));
	saved_createrole_self_grant =
		pstrdup(GetConfigOption("createrole_self_grant", false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && Password_encryption == PASSWORD_TYPE_SCRAM_SHA_256;
		ok = ok && strcmp(createrole_self_grant, "") == 0;
		ok = ok && !*PgCurrentCreateRoleSelfGrantEnabledRef();

		SetConfigOption("password_encryption", "md5",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("createrole_self_grant", "set, inherit",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && Password_encryption == PASSWORD_TYPE_MD5;
		ok = ok && strcmp(createrole_self_grant, "set, inherit") == 0;
		ok = ok && *PgCurrentCreateRoleSelfGrantEnabledRef();
		ok = ok && *PgCurrentCreateRoleSelfGrantOptionsSpecifiedRef() != 0;
		ok = ok && !*PgCurrentCreateRoleSelfGrantOptionsAdminRef();
		ok = ok && *PgCurrentCreateRoleSelfGrantOptionsInheritRef();
		ok = ok && *PgCurrentCreateRoleSelfGrantOptionsSetRef();

		PgSetCurrentSession(&fake_session2);
		ok = ok && Password_encryption == PASSWORD_TYPE_SCRAM_SHA_256;
		ok = ok && strcmp(createrole_self_grant, "") == 0;
		ok = ok && !*PgCurrentCreateRoleSelfGrantEnabledRef();
		SetConfigOption("password_encryption", "scram-sha-256",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("createrole_self_grant", "set",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && Password_encryption == PASSWORD_TYPE_SCRAM_SHA_256;
		ok = ok && strcmp(createrole_self_grant, "set") == 0;
		ok = ok && *PgCurrentCreateRoleSelfGrantEnabledRef();
		ok = ok && !*PgCurrentCreateRoleSelfGrantOptionsInheritRef();
		ok = ok && *PgCurrentCreateRoleSelfGrantOptionsSetRef();

		PgSetCurrentSession(&fake_session1);
		ok = ok && Password_encryption == PASSWORD_TYPE_MD5;
		ok = ok && strcmp(createrole_self_grant, "set, inherit") == 0;
		ok = ok && *PgCurrentCreateRoleSelfGrantOptionsInheritRef();
		ok = ok && *PgCurrentCreateRoleSelfGrantOptionsSetRef();

		PgSetCurrentSession(&fake_session2);
		ok = ok && Password_encryption == PASSWORD_TYPE_SCRAM_SHA_256;
		ok = ok && strcmp(createrole_self_grant, "set") == 0;
		ok = ok && !*PgCurrentCreateRoleSelfGrantOptionsInheritRef();
		ok = ok && *PgCurrentCreateRoleSelfGrantOptionsSetRef();

		PgSetCurrentSession(saved_session);
		SetConfigOption("password_encryption", saved_password_encryption,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("createrole_self_grant",
						saved_createrole_self_grant,
						PGC_USERSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("password_encryption", saved_password_encryption,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("createrole_self_grant",
						saved_createrole_self_grant,
						PGC_USERSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session user GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_user_identity_state_is_session_local);
Datum
test_session_user_identity_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	PgSessionUserIdentityState *identity_state;
	Oid			userid;
	int			sec_context;
	bool		sec_def_context;
	bool		ok = true;

	saved_session = CurrentPgSession;
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		identity_state = PgCurrentUserIdentityState();
		ok = ok && identity_state->initialized;
		ok = ok && identity_state->authenticated_user_id == InvalidOid;
		ok = ok && identity_state->session_user_id == InvalidOid;
		ok = ok && identity_state->outer_user_id == InvalidOid;
		ok = ok && identity_state->current_user_id == InvalidOid;
		ok = ok && identity_state->system_user == NULL;
		ok = ok && !identity_state->session_user_is_superuser;
		ok = ok && identity_state->security_restriction_context == 0;
		ok = ok && !identity_state->set_role_is_active;
		ok = ok && identity_state->cached_role[0] == InvalidOid;
		ok = ok && identity_state->cached_roles[0] == NIL;
		ok = ok && identity_state->cached_db_hash == 0;

		identity_state->authenticated_user_id = 10;
		identity_state->session_user_id = 11;
		identity_state->outer_user_id = 12;
		identity_state->current_user_id = 13;
		identity_state->system_user = "auth_method_a:authn_id_a";
		identity_state->session_user_is_superuser = true;
		identity_state->security_restriction_context =
			SECURITY_RESTRICTED_OPERATION | SECURITY_NOFORCE_RLS;
		identity_state->set_role_is_active = true;
		identity_state->cached_role[0] = 31;
		identity_state->cached_roles[0] = list_make1_oid(31);
		identity_state->cached_db_hash = 41;

		ok = ok && GetAuthenticatedUserId() == 10;
		ok = ok && GetSessionUserId() == 11;
		ok = ok && GetOuterUserId() == 12;
		ok = ok && GetUserId() == 13;
		ok = ok && GetSessionUserIsSuperuser();
		ok = ok && strcmp(GetSystemUser(), "auth_method_a:authn_id_a") == 0;
		ok = ok && GetCurrentRoleId() == 12;
		ok = ok && InSecurityRestrictedOperation();
		ok = ok && InNoForceRLSOperation();
		ok = ok && !InLocalUserIdChange();
		GetUserIdAndSecContext(&userid, &sec_context);
		ok = ok && userid == 13;
		ok = ok && sec_context == (SECURITY_RESTRICTED_OPERATION |
								   SECURITY_NOFORCE_RLS);

		SetUserIdAndSecContext(14, SECURITY_LOCAL_USERID_CHANGE);
		ok = ok && GetUserId() == 14;
		ok = ok && InLocalUserIdChange();
		GetUserIdAndContext(&userid, &sec_def_context);
		ok = ok && userid == 14;
		ok = ok && sec_def_context;
		SetUserIdAndContext(15, false);
		ok = ok && GetUserId() == 15;
		ok = ok && !InLocalUserIdChange();

		PgSetCurrentSession(&fake_session2);
		identity_state = PgCurrentUserIdentityState();
		ok = ok && identity_state->initialized;
		ok = ok && identity_state->authenticated_user_id == InvalidOid;
		ok = ok && identity_state->session_user_id == InvalidOid;
		ok = ok && identity_state->outer_user_id == InvalidOid;
		ok = ok && identity_state->current_user_id == InvalidOid;
		ok = ok && identity_state->system_user == NULL;
		ok = ok && !identity_state->session_user_is_superuser;
		ok = ok && identity_state->security_restriction_context == 0;
		ok = ok && !identity_state->set_role_is_active;
		ok = ok && identity_state->cached_role[0] == InvalidOid;
		ok = ok && identity_state->cached_roles[0] == NIL;
		ok = ok && identity_state->cached_db_hash == 0;

		identity_state->authenticated_user_id = 20;
		identity_state->session_user_id = 21;
		identity_state->outer_user_id = 22;
		identity_state->current_user_id = 23;
		identity_state->system_user = "auth_method_b:authn_id_b";
		identity_state->session_user_is_superuser = false;
		identity_state->set_role_is_active = false;
		identity_state->cached_role[0] = 32;
		identity_state->cached_roles[0] = list_make1_oid(32);
		identity_state->cached_db_hash = 42;

		ok = ok && GetAuthenticatedUserId() == 20;
		ok = ok && GetSessionUserId() == 21;
		ok = ok && GetOuterUserId() == 22;
		ok = ok && GetUserId() == 23;
		ok = ok && !GetSessionUserIsSuperuser();
		ok = ok && strcmp(GetSystemUser(), "auth_method_b:authn_id_b") == 0;
		ok = ok && GetCurrentRoleId() == InvalidOid;
		ok = ok && !InSecurityRestrictedOperation();
		ok = ok && !InNoForceRLSOperation();
		ok = ok && !InLocalUserIdChange();

		PgSetCurrentSession(&fake_session1);
		ok = ok && GetAuthenticatedUserId() == 10;
		ok = ok && GetSessionUserId() == 11;
		ok = ok && GetOuterUserId() == 12;
		ok = ok && GetUserId() == 15;
		ok = ok && GetSessionUserIsSuperuser();
		ok = ok && strcmp(GetSystemUser(), "auth_method_a:authn_id_a") == 0;
		ok = ok && GetCurrentRoleId() == 12;
		ok = ok && !InLocalUserIdChange();
		ok = ok && PgCurrentUserIdentityState()->cached_role[0] == 31;
		ok = ok && list_member_oid(PgCurrentUserIdentityState()->cached_roles[0],
								   31);
		ok = ok && PgCurrentUserIdentityState()->cached_db_hash == 41;

		PgSetCurrentSession(&fake_session2);
		ok = ok && PgCurrentUserIdentityState()->cached_role[0] == 32;
		ok = ok && list_member_oid(PgCurrentUserIdentityState()->cached_roles[0],
								   32);
		ok = ok && PgCurrentUserIdentityState()->cached_db_hash == 42;

		PgSetCurrentSession(saved_session);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "user identity state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_command_guc_state_is_session_local);
Datum
test_session_command_guc_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_session_replication_role;
	char	   *saved_event_triggers;
	char	   *saved_trace_notify;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_session_replication_role =
		pstrdup(GetConfigOption("session_replication_role", false, false));
	saved_event_triggers =
		pstrdup(GetConfigOption("event_triggers", false, false));
	saved_trace_notify =
		pstrdup(GetConfigOption("trace_notify", false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && SessionReplicationRole == SESSION_REPLICATION_ROLE_ORIGIN;
		ok = ok && event_triggers;
		ok = ok && !Trace_notify;

		SetConfigOption("session_replication_role", "replica",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("event_triggers", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_notify", "on",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && SessionReplicationRole == SESSION_REPLICATION_ROLE_REPLICA;
		ok = ok && !event_triggers;
		ok = ok && Trace_notify;

		PgSetCurrentSession(&fake_session2);
		ok = ok && SessionReplicationRole == SESSION_REPLICATION_ROLE_ORIGIN;
		ok = ok && event_triggers;
		ok = ok && !Trace_notify;
		SetConfigOption("session_replication_role", "local",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("event_triggers", "on",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_notify", "off",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && SessionReplicationRole == SESSION_REPLICATION_ROLE_LOCAL;
		ok = ok && event_triggers;
		ok = ok && !Trace_notify;

		PgSetCurrentSession(&fake_session1);
		ok = ok && SessionReplicationRole == SESSION_REPLICATION_ROLE_REPLICA;
		ok = ok && !event_triggers;
		ok = ok && Trace_notify;

		PgSetCurrentSession(&fake_session2);
		ok = ok && SessionReplicationRole == SESSION_REPLICATION_ROLE_LOCAL;
		ok = ok && event_triggers;
		ok = ok && !Trace_notify;

		PgSetCurrentSession(saved_session);
		SetConfigOption("session_replication_role",
						saved_session_replication_role,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("event_triggers", saved_event_triggers,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_notify", saved_trace_notify,
						PGC_USERSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("session_replication_role",
						saved_session_replication_role,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("event_triggers", saved_event_triggers,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_notify", saved_trace_notify,
						PGC_USERSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session command GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_replication_guc_state_is_session_local);
Datum
test_session_replication_guc_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_wal_sender_timeout;
	char	   *saved_wal_sender_shutdown_timeout;
	char	   *saved_log_replication_commands;
	char	   *saved_wal_receiver_timeout;
	char	   *saved_logical_decoding_work_mem;
	char	   *saved_debug_logical_replication_streaming;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_wal_sender_timeout =
		pstrdup(GetConfigOption("wal_sender_timeout", false, false));
	saved_wal_sender_shutdown_timeout =
		pstrdup(GetConfigOption("wal_sender_shutdown_timeout", false, false));
	saved_log_replication_commands =
		pstrdup(GetConfigOption("log_replication_commands", false, false));
	saved_wal_receiver_timeout =
		pstrdup(GetConfigOption("wal_receiver_timeout", false, false));
	saved_logical_decoding_work_mem =
		pstrdup(GetConfigOption("logical_decoding_work_mem", false, false));
	saved_debug_logical_replication_streaming =
		pstrdup(GetConfigOption("debug_logical_replication_streaming",
								false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && wal_sender_timeout == 60 * 1000;
		ok = ok && wal_sender_shutdown_timeout == -1;
		ok = ok && !log_replication_commands;
		ok = ok && wal_receiver_timeout == 60 * 1000;
		ok = ok && logical_decoding_work_mem == 65536;
		ok = ok && debug_logical_replication_streaming ==
			DEBUG_LOGICAL_REP_STREAMING_BUFFERED;

		SetConfigOption("wal_sender_timeout", "7000",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("wal_sender_shutdown_timeout", "8000",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_replication_commands", "on",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_receiver_timeout", "9000",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("logical_decoding_work_mem", "128MB",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_logical_replication_streaming", "immediate",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && wal_sender_timeout == 7000;
		ok = ok && wal_sender_shutdown_timeout == 8000;
		ok = ok && log_replication_commands;
		ok = ok && wal_receiver_timeout == 9000;
		ok = ok && logical_decoding_work_mem == 131072;
		ok = ok && debug_logical_replication_streaming ==
			DEBUG_LOGICAL_REP_STREAMING_IMMEDIATE;

		PgSetCurrentSession(&fake_session2);
		ok = ok && wal_sender_timeout == 60 * 1000;
		ok = ok && wal_sender_shutdown_timeout == -1;
		ok = ok && !log_replication_commands;
		ok = ok && wal_receiver_timeout == 60 * 1000;
		ok = ok && logical_decoding_work_mem == 65536;
		ok = ok && debug_logical_replication_streaming ==
			DEBUG_LOGICAL_REP_STREAMING_BUFFERED;
		SetConfigOption("wal_sender_timeout", "1000",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("wal_sender_shutdown_timeout", "-1",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_replication_commands", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_receiver_timeout", "2000",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("logical_decoding_work_mem", "64kB",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_logical_replication_streaming", "buffered",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && wal_sender_timeout == 1000;
		ok = ok && wal_sender_shutdown_timeout == -1;
		ok = ok && !log_replication_commands;
		ok = ok && wal_receiver_timeout == 2000;
		ok = ok && logical_decoding_work_mem == 64;
		ok = ok && debug_logical_replication_streaming ==
			DEBUG_LOGICAL_REP_STREAMING_BUFFERED;

		PgSetCurrentSession(&fake_session1);
		ok = ok && wal_sender_timeout == 7000;
		ok = ok && wal_sender_shutdown_timeout == 8000;
		ok = ok && log_replication_commands;
		ok = ok && wal_receiver_timeout == 9000;
		ok = ok && logical_decoding_work_mem == 131072;
		ok = ok && debug_logical_replication_streaming ==
			DEBUG_LOGICAL_REP_STREAMING_IMMEDIATE;

		PgSetCurrentSession(&fake_session2);
		ok = ok && wal_sender_timeout == 1000;
		ok = ok && wal_sender_shutdown_timeout == -1;
		ok = ok && !log_replication_commands;
		ok = ok && wal_receiver_timeout == 2000;
		ok = ok && logical_decoding_work_mem == 64;
		ok = ok && debug_logical_replication_streaming ==
			DEBUG_LOGICAL_REP_STREAMING_BUFFERED;

		PgSetCurrentSession(saved_session);
		SetConfigOption("debug_logical_replication_streaming",
						saved_debug_logical_replication_streaming,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("logical_decoding_work_mem",
						saved_logical_decoding_work_mem,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_replication_commands",
						saved_log_replication_commands,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_receiver_timeout", saved_wal_receiver_timeout,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("wal_sender_shutdown_timeout",
						saved_wal_sender_shutdown_timeout,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("wal_sender_timeout", saved_wal_sender_timeout,
						PGC_USERSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("debug_logical_replication_streaming",
						saved_debug_logical_replication_streaming,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("logical_decoding_work_mem",
						saved_logical_decoding_work_mem,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_replication_commands",
						saved_log_replication_commands,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_receiver_timeout", saved_wal_receiver_timeout,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("wal_sender_shutdown_timeout",
						saved_wal_sender_shutdown_timeout,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("wal_sender_timeout", saved_wal_sender_timeout,
						PGC_USERSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session replication GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_general_guc_state_is_session_local);
Datum
test_session_general_guc_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_allow_alter_system;
	char	   *saved_row_security;
	char	   *saved_check_function_bodies;
	char	   *saved_is_superuser;
	char	   *saved_temp_file_limit;
	char	   *saved_temp_buffers;
	char	   *saved_role;
	char	   *saved_lo_compat_privileges;
	char	   *saved_extra_float_digits;
	char	   *saved_array_nulls;
	char	   *saved_bytea_output;
	char	   *saved_xmlbinary;
	char	   *saved_xmloption;
	char	   *saved_quote_all_identifiers;
	char	   *saved_plan_cache_mode;
	char	   *saved_gin_fuzzy_search_limit;
	char	   *saved_gin_pending_list_limit;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_allow_alter_system =
		pstrdup(GetConfigOption("allow_alter_system", false, false));
	saved_row_security =
		pstrdup(GetConfigOption("row_security", false, false));
	saved_check_function_bodies =
		pstrdup(GetConfigOption("check_function_bodies", false, false));
	saved_is_superuser =
		pstrdup(GetConfigOption("is_superuser", false, false));
	saved_temp_file_limit =
		pstrdup(GetConfigOption("temp_file_limit", false, false));
	saved_temp_buffers =
		pstrdup(GetConfigOption("temp_buffers", false, false));
	saved_role = pstrdup(GetConfigOption("role", false, false));
	saved_lo_compat_privileges =
		pstrdup(GetConfigOption("lo_compat_privileges", false, false));
	saved_extra_float_digits =
		pstrdup(GetConfigOption("extra_float_digits", false, false));
	saved_array_nulls =
		pstrdup(GetConfigOption("array_nulls", false, false));
	saved_bytea_output =
		pstrdup(GetConfigOption("bytea_output", false, false));
	saved_xmlbinary =
		pstrdup(GetConfigOption("xmlbinary", false, false));
	saved_xmloption =
		pstrdup(GetConfigOption("xmloption", false, false));
	saved_quote_all_identifiers =
		pstrdup(GetConfigOption("quote_all_identifiers", false, false));
	saved_plan_cache_mode =
		pstrdup(GetConfigOption("plan_cache_mode", false, false));
	saved_gin_fuzzy_search_limit =
		pstrdup(GetConfigOption("gin_fuzzy_search_limit", false, false));
	saved_gin_pending_list_limit =
		pstrdup(GetConfigOption("gin_pending_list_limit", false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && AllowAlterSystem;
		ok = ok && row_security;
		ok = ok && check_function_bodies;
		ok = ok && current_role_is_superuser;
		ok = ok && temp_file_limit == -1;
		ok = ok && num_temp_buffers == 1024;
		ok = ok && strcmp(role_string, "none") == 0;
		ok = ok && !lo_compat_privileges;
		ok = ok && extra_float_digits == 1;
		ok = ok && Array_nulls;
		ok = ok && bytea_output == BYTEA_OUTPUT_HEX;
		ok = ok && xmlbinary == XMLBINARY_BASE64;
		ok = ok && *PgCurrentXmlOptionRef() == XMLOPTION_CONTENT;
		ok = ok && !quote_all_identifiers;
		ok = ok && plan_cache_mode == PLAN_CACHE_MODE_AUTO;
		ok = ok && GinFuzzySearchLimit == 0;
		ok = ok && gin_pending_list_limit == 4096;

		SetConfigOption("allow_alter_system", "off",
						PGC_SIGHUP, PGC_S_SESSION);
		SetConfigOption("row_security", "off",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("check_function_bodies", "off",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("is_superuser", "on",
						PGC_INTERNAL, PGC_S_OVERRIDE);
		SetConfigOption("temp_file_limit", "64MB",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("temp_buffers", "800",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("role", "none",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("lo_compat_privileges", "on",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("extra_float_digits", "2",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("array_nulls", "off",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("bytea_output", "escape",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("xmlbinary", "hex",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("xmloption", "document",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("quote_all_identifiers", "on",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("plan_cache_mode", "force_generic_plan",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("gin_fuzzy_search_limit", "7",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("gin_pending_list_limit", "8MB",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && !AllowAlterSystem;
		ok = ok && !row_security;
		ok = ok && !check_function_bodies;
		ok = ok && current_role_is_superuser;
		ok = ok && temp_file_limit == 65536;
		ok = ok && num_temp_buffers == 800;
		ok = ok && strcmp(role_string, "none") == 0;
		ok = ok && lo_compat_privileges;
		ok = ok && extra_float_digits == 2;
		ok = ok && !Array_nulls;
		ok = ok && bytea_output == BYTEA_OUTPUT_ESCAPE;
		ok = ok && xmlbinary == XMLBINARY_HEX;
		ok = ok && *PgCurrentXmlOptionRef() == XMLOPTION_DOCUMENT;
		ok = ok && quote_all_identifiers;
		ok = ok && plan_cache_mode == PLAN_CACHE_MODE_FORCE_GENERIC_PLAN;
		ok = ok && GinFuzzySearchLimit == 7;
		ok = ok && gin_pending_list_limit == 8192;

		PgSetCurrentSession(&fake_session2);
		ok = ok && AllowAlterSystem;
		ok = ok && row_security;
		ok = ok && check_function_bodies;
		ok = ok && current_role_is_superuser;
		ok = ok && temp_file_limit == -1;
		ok = ok && num_temp_buffers == 1024;
		ok = ok && strcmp(role_string, "none") == 0;
		ok = ok && !lo_compat_privileges;
		ok = ok && extra_float_digits == 1;
		ok = ok && Array_nulls;
		ok = ok && bytea_output == BYTEA_OUTPUT_HEX;
		ok = ok && xmlbinary == XMLBINARY_BASE64;
		ok = ok && *PgCurrentXmlOptionRef() == XMLOPTION_CONTENT;
		ok = ok && !quote_all_identifiers;
		ok = ok && plan_cache_mode == PLAN_CACHE_MODE_AUTO;
		ok = ok && GinFuzzySearchLimit == 0;
		ok = ok && gin_pending_list_limit == 4096;
		SetConfigOption("row_security", "on",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("check_function_bodies", "on",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("is_superuser", "off",
						PGC_INTERNAL, PGC_S_OVERRIDE);
		SetConfigOption("temp_file_limit", "128MB",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("temp_buffers", "900",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("lo_compat_privileges", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("extra_float_digits", "3",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("array_nulls", "on",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("bytea_output", "hex",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("xmlbinary", "base64",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("xmloption", "content",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("quote_all_identifiers", "off",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("plan_cache_mode", "force_custom_plan",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("gin_fuzzy_search_limit", "11",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("gin_pending_list_limit", "16MB",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && row_security;
		ok = ok && check_function_bodies;
		ok = ok && !current_role_is_superuser;
		ok = ok && temp_file_limit == 131072;
		ok = ok && num_temp_buffers == 900;
		ok = ok && !lo_compat_privileges;
		ok = ok && extra_float_digits == 3;
		ok = ok && Array_nulls;
		ok = ok && bytea_output == BYTEA_OUTPUT_HEX;
		ok = ok && xmlbinary == XMLBINARY_BASE64;
		ok = ok && *PgCurrentXmlOptionRef() == XMLOPTION_CONTENT;
		ok = ok && !quote_all_identifiers;
		ok = ok && plan_cache_mode == PLAN_CACHE_MODE_FORCE_CUSTOM_PLAN;
		ok = ok && GinFuzzySearchLimit == 11;
		ok = ok && gin_pending_list_limit == 16384;

		PgSetCurrentSession(&fake_session1);
		ok = ok && !AllowAlterSystem;
		ok = ok && !row_security;
		ok = ok && !check_function_bodies;
		ok = ok && current_role_is_superuser;
		ok = ok && temp_file_limit == 65536;
		ok = ok && num_temp_buffers == 800;
		ok = ok && lo_compat_privileges;
		ok = ok && extra_float_digits == 2;
		ok = ok && !Array_nulls;
		ok = ok && bytea_output == BYTEA_OUTPUT_ESCAPE;
		ok = ok && xmlbinary == XMLBINARY_HEX;
		ok = ok && *PgCurrentXmlOptionRef() == XMLOPTION_DOCUMENT;
		ok = ok && quote_all_identifiers;
		ok = ok && plan_cache_mode == PLAN_CACHE_MODE_FORCE_GENERIC_PLAN;
		ok = ok && GinFuzzySearchLimit == 7;
		ok = ok && gin_pending_list_limit == 8192;

		PgSetCurrentSession(&fake_session2);
		ok = ok && AllowAlterSystem;
		ok = ok && row_security;
		ok = ok && check_function_bodies;
		ok = ok && !current_role_is_superuser;
		ok = ok && temp_file_limit == 131072;
		ok = ok && num_temp_buffers == 900;
		ok = ok && !lo_compat_privileges;
		ok = ok && extra_float_digits == 3;
		ok = ok && Array_nulls;
		ok = ok && bytea_output == BYTEA_OUTPUT_HEX;
		ok = ok && xmlbinary == XMLBINARY_BASE64;
		ok = ok && *PgCurrentXmlOptionRef() == XMLOPTION_CONTENT;
		ok = ok && !quote_all_identifiers;
		ok = ok && plan_cache_mode == PLAN_CACHE_MODE_FORCE_CUSTOM_PLAN;
		ok = ok && GinFuzzySearchLimit == 11;
		ok = ok && gin_pending_list_limit == 16384;

		PgSetCurrentSession(saved_session);
		SetConfigOption("gin_pending_list_limit",
						saved_gin_pending_list_limit,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("gin_fuzzy_search_limit",
						saved_gin_fuzzy_search_limit,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("plan_cache_mode", saved_plan_cache_mode,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("quote_all_identifiers",
						saved_quote_all_identifiers,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("xmlbinary", saved_xmlbinary,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("bytea_output", saved_bytea_output,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("array_nulls", saved_array_nulls,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("extra_float_digits", saved_extra_float_digits,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("xmloption", saved_xmloption,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("lo_compat_privileges", saved_lo_compat_privileges,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("role", saved_role,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("temp_buffers", saved_temp_buffers,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("temp_file_limit", saved_temp_file_limit,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("is_superuser", saved_is_superuser,
						PGC_INTERNAL, PGC_S_OVERRIDE);
		SetConfigOption("check_function_bodies",
						saved_check_function_bodies,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("row_security", saved_row_security,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("allow_alter_system", saved_allow_alter_system,
						PGC_SIGHUP, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("gin_pending_list_limit",
						saved_gin_pending_list_limit,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("gin_fuzzy_search_limit",
						saved_gin_fuzzy_search_limit,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("plan_cache_mode", saved_plan_cache_mode,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("quote_all_identifiers",
						saved_quote_all_identifiers,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("xmlbinary", saved_xmlbinary,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("bytea_output", saved_bytea_output,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("array_nulls", saved_array_nulls,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("extra_float_digits", saved_extra_float_digits,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("xmloption", saved_xmloption,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("lo_compat_privileges", saved_lo_compat_privileges,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("role", saved_role,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("temp_buffers", saved_temp_buffers,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("temp_file_limit", saved_temp_file_limit,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("is_superuser", saved_is_superuser,
						PGC_INTERNAL, PGC_S_OVERRIDE);
		SetConfigOption("check_function_bodies",
						saved_check_function_bodies,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("row_security", saved_row_security,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("allow_alter_system", saved_allow_alter_system,
						PGC_SIGHUP, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session general GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_compat_guc_state_is_session_local);
Datum
test_session_compat_guc_state_is_session_local(PG_FUNCTION_ARGS)
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
		PgSetCurrentSession(&fake_session1);
		ok = ok && !*PgCurrentDefaultWithOidsRef();
		ok = ok && *PgCurrentStandardConformingStringsRef();
		ok = ok && *PgCurrentPhonyRandomSeedRef() == 0.0;
		ok = ok && *PgCurrentSslRenegotiationLimitRef() == 0;
		ok = ok && strcmp(*PgCurrentDateStyleStringRef(), "ISO, MDY") == 0;
		ok = ok && strcmp(*PgCurrentClientEncodingStringRef(), "SQL_ASCII") == 0;
		ok = ok && strcmp(*PgCurrentServerEncodingStringRef(), "SQL_ASCII") == 0;
		ok = ok && *PgCurrentTimeZoneAbbreviationsStringRef() == NULL;
		ok = ok && *PgCurrentSessionAuthorizationStringRef() == NULL;
		*PgCurrentDefaultWithOidsRef() = true;
		*PgCurrentStandardConformingStringsRef() = false;
		*PgCurrentPhonyRandomSeedRef() = 0.25;
		*PgCurrentSslRenegotiationLimitRef() = 11;
		*PgCurrentDateStyleStringRef() = "session1_datestyle";
		*PgCurrentClientEncodingStringRef() = "session1_client_encoding";
		*PgCurrentServerEncodingStringRef() = "session1_server_encoding";
		*PgCurrentTimeZoneAbbreviationsStringRef() = "session1_tz_abbrevs";
		*PgCurrentSessionAuthorizationStringRef() = "session1_auth";

		PgSetCurrentSession(&fake_session2);
		ok = ok && !*PgCurrentDefaultWithOidsRef();
		ok = ok && *PgCurrentStandardConformingStringsRef();
		ok = ok && *PgCurrentPhonyRandomSeedRef() == 0.0;
		ok = ok && *PgCurrentSslRenegotiationLimitRef() == 0;
		ok = ok && strcmp(*PgCurrentDateStyleStringRef(), "ISO, MDY") == 0;
		ok = ok && strcmp(*PgCurrentClientEncodingStringRef(), "SQL_ASCII") == 0;
		ok = ok && strcmp(*PgCurrentServerEncodingStringRef(), "SQL_ASCII") == 0;
		ok = ok && *PgCurrentTimeZoneAbbreviationsStringRef() == NULL;
		ok = ok && *PgCurrentSessionAuthorizationStringRef() == NULL;
		*PgCurrentDefaultWithOidsRef() = false;
		*PgCurrentStandardConformingStringsRef() = true;
		*PgCurrentPhonyRandomSeedRef() = -0.25;
		*PgCurrentSslRenegotiationLimitRef() = 22;
		*PgCurrentDateStyleStringRef() = "session2_datestyle";
		*PgCurrentClientEncodingStringRef() = "session2_client_encoding";
		*PgCurrentServerEncodingStringRef() = "session2_server_encoding";
		*PgCurrentTimeZoneAbbreviationsStringRef() = "session2_tz_abbrevs";
		*PgCurrentSessionAuthorizationStringRef() = "session2_auth";

		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentDefaultWithOidsRef();
		ok = ok && !*PgCurrentStandardConformingStringsRef();
		ok = ok && *PgCurrentPhonyRandomSeedRef() == 0.25;
		ok = ok && *PgCurrentSslRenegotiationLimitRef() == 11;
		ok = ok && strcmp(*PgCurrentDateStyleStringRef(),
						  "session1_datestyle") == 0;
		ok = ok && strcmp(*PgCurrentClientEncodingStringRef(),
						  "session1_client_encoding") == 0;
		ok = ok && strcmp(*PgCurrentServerEncodingStringRef(),
						  "session1_server_encoding") == 0;
		ok = ok && strcmp(*PgCurrentTimeZoneAbbreviationsStringRef(),
						  "session1_tz_abbrevs") == 0;
		ok = ok && strcmp(*PgCurrentSessionAuthorizationStringRef(),
						  "session1_auth") == 0;

		PgSetCurrentSession(&fake_session2);
		ok = ok && !*PgCurrentDefaultWithOidsRef();
		ok = ok && *PgCurrentStandardConformingStringsRef();
		ok = ok && *PgCurrentPhonyRandomSeedRef() == -0.25;
		ok = ok && *PgCurrentSslRenegotiationLimitRef() == 22;
		ok = ok && strcmp(*PgCurrentDateStyleStringRef(),
						  "session2_datestyle") == 0;
		ok = ok && strcmp(*PgCurrentClientEncodingStringRef(),
						  "session2_client_encoding") == 0;
		ok = ok && strcmp(*PgCurrentServerEncodingStringRef(),
						  "session2_server_encoding") == 0;
		ok = ok && strcmp(*PgCurrentTimeZoneAbbreviationsStringRef(),
						  "session2_tz_abbrevs") == 0;
		ok = ok && strcmp(*PgCurrentSessionAuthorizationStringRef(),
						  "session2_auth") == 0;

		PgSetCurrentSession(saved_session);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session compatibility GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_access_wal_guc_state_is_session_local);
Datum
test_session_access_wal_guc_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_synchronize_seqscans;
	char	   *saved_wal_compression;
	char	   *saved_wal_init_zero;
	char	   *saved_wal_recycle;
	char	   *saved_wal_consistency_checking;
	char	   *saved_commit_delay;
	char	   *saved_commit_siblings;
	char	   *saved_track_wal_io_timing;
	char	   *saved_wal_skip_threshold;
	bool	   *fake1_wal_consistency_checking;
	bool	   *fake2_wal_consistency_checking;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_synchronize_seqscans =
		pstrdup(GetConfigOption("synchronize_seqscans", false, false));
	saved_wal_compression =
		pstrdup(GetConfigOption("wal_compression", false, false));
	saved_wal_init_zero =
		pstrdup(GetConfigOption("wal_init_zero", false, false));
	saved_wal_recycle =
		pstrdup(GetConfigOption("wal_recycle", false, false));
	saved_wal_consistency_checking =
		pstrdup(GetConfigOption("wal_consistency_checking", false, false));
	saved_commit_delay =
		pstrdup(GetConfigOption("commit_delay", false, false));
	saved_commit_siblings =
		pstrdup(GetConfigOption("commit_siblings", false, false));
	saved_track_wal_io_timing =
		pstrdup(GetConfigOption("track_wal_io_timing", false, false));
	saved_wal_skip_threshold =
		pstrdup(GetConfigOption("wal_skip_threshold", false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && strcmp(default_table_access_method,
						  DEFAULT_TABLE_ACCESS_METHOD) == 0;
		ok = ok && synchronize_seqscans;
		ok = ok && default_toast_compression == DEFAULT_TOAST_COMPRESSION;
		ok = ok && wal_compression == WAL_COMPRESSION_NONE;
		ok = ok && wal_init_zero;
		ok = ok && wal_recycle;
		ok = ok && strcmp(wal_consistency_checking_string, "") == 0;
		ok = ok && wal_consistency_checking != NULL;
		ok = ok && CommitDelay == 0;
		ok = ok && CommitSiblings == 5;
		ok = ok && !track_wal_io_timing;
		ok = ok && wal_skip_threshold == 2048;

		default_table_access_method = "session1_tableam";
		default_toast_compression = TOAST_LZ4_COMPRESSION;
		SetConfigOption("synchronize_seqscans", "off",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("wal_compression", "pglz",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_init_zero", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_recycle", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_consistency_checking", "all",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("commit_delay", "100",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("commit_siblings", "8",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("track_wal_io_timing", "on",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_skip_threshold", "3MB",
						PGC_USERSET, PGC_S_SESSION);
		fake1_wal_consistency_checking = wal_consistency_checking;
		ok = ok && strcmp(default_table_access_method,
						  "session1_tableam") == 0;
		ok = ok && !synchronize_seqscans;
		ok = ok && default_toast_compression == TOAST_LZ4_COMPRESSION;
		ok = ok && wal_compression == WAL_COMPRESSION_PGLZ;
		ok = ok && !wal_init_zero;
		ok = ok && !wal_recycle;
		ok = ok && strcmp(wal_consistency_checking_string, "all") == 0;
		ok = ok && fake1_wal_consistency_checking != NULL;
		ok = ok && CommitDelay == 100;
		ok = ok && CommitSiblings == 8;
		ok = ok && track_wal_io_timing;
		ok = ok && wal_skip_threshold == 3072;

		PgSetCurrentSession(&fake_session2);
		ok = ok && strcmp(default_table_access_method,
						  DEFAULT_TABLE_ACCESS_METHOD) == 0;
		ok = ok && synchronize_seqscans;
		ok = ok && default_toast_compression == DEFAULT_TOAST_COMPRESSION;
		ok = ok && wal_compression == WAL_COMPRESSION_NONE;
		ok = ok && wal_init_zero;
		ok = ok && wal_recycle;
		ok = ok && strcmp(wal_consistency_checking_string, "") == 0;
		ok = ok && wal_consistency_checking != NULL;
		ok = ok && CommitDelay == 0;
		ok = ok && CommitSiblings == 5;
		ok = ok && !track_wal_io_timing;
		ok = ok && wal_skip_threshold == 2048;

		default_table_access_method = "session2_tableam";
		default_toast_compression = TOAST_PGLZ_COMPRESSION;
		SetConfigOption("synchronize_seqscans", "on",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("wal_compression", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_init_zero", "on",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_recycle", "on",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_consistency_checking", "",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("commit_delay", "200",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("commit_siblings", "9",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("track_wal_io_timing", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_skip_threshold", "4MB",
						PGC_USERSET, PGC_S_SESSION);
		fake2_wal_consistency_checking = wal_consistency_checking;
		ok = ok && strcmp(default_table_access_method,
						  "session2_tableam") == 0;
		ok = ok && synchronize_seqscans;
		ok = ok && default_toast_compression == TOAST_PGLZ_COMPRESSION;
		ok = ok && wal_compression == WAL_COMPRESSION_NONE;
		ok = ok && wal_init_zero;
		ok = ok && wal_recycle;
		ok = ok && strcmp(wal_consistency_checking_string, "") == 0;
		ok = ok && fake2_wal_consistency_checking != NULL;
		ok = ok && fake2_wal_consistency_checking !=
			fake1_wal_consistency_checking;
		ok = ok && CommitDelay == 200;
		ok = ok && CommitSiblings == 9;
		ok = ok && !track_wal_io_timing;
		ok = ok && wal_skip_threshold == 4096;

		PgSetCurrentSession(&fake_session1);
		ok = ok && strcmp(default_table_access_method,
						  "session1_tableam") == 0;
		ok = ok && !synchronize_seqscans;
		ok = ok && default_toast_compression == TOAST_LZ4_COMPRESSION;
		ok = ok && wal_compression == WAL_COMPRESSION_PGLZ;
		ok = ok && !wal_init_zero;
		ok = ok && !wal_recycle;
		ok = ok && strcmp(wal_consistency_checking_string, "all") == 0;
		ok = ok && wal_consistency_checking ==
			fake1_wal_consistency_checking;
		ok = ok && CommitDelay == 100;
		ok = ok && CommitSiblings == 8;
		ok = ok && track_wal_io_timing;
		ok = ok && wal_skip_threshold == 3072;

		PgSetCurrentSession(&fake_session2);
		ok = ok && strcmp(default_table_access_method,
						  "session2_tableam") == 0;
		ok = ok && synchronize_seqscans;
		ok = ok && default_toast_compression == TOAST_PGLZ_COMPRESSION;
		ok = ok && wal_compression == WAL_COMPRESSION_NONE;
		ok = ok && wal_init_zero;
		ok = ok && wal_recycle;
		ok = ok && strcmp(wal_consistency_checking_string, "") == 0;
		ok = ok && wal_consistency_checking ==
			fake2_wal_consistency_checking;
		ok = ok && CommitDelay == 200;
		ok = ok && CommitSiblings == 9;
		ok = ok && !track_wal_io_timing;
		ok = ok && wal_skip_threshold == 4096;

		PgSetCurrentSession(saved_session);
		SetConfigOption("wal_skip_threshold", saved_wal_skip_threshold,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("track_wal_io_timing", saved_track_wal_io_timing,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("commit_siblings", saved_commit_siblings,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("commit_delay", saved_commit_delay,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_consistency_checking",
						saved_wal_consistency_checking,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_recycle", saved_wal_recycle,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_init_zero", saved_wal_init_zero,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_compression", saved_wal_compression,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("synchronize_seqscans", saved_synchronize_seqscans,
						PGC_USERSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("wal_skip_threshold", saved_wal_skip_threshold,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("track_wal_io_timing", saved_track_wal_io_timing,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("commit_siblings", saved_commit_siblings,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("commit_delay", saved_commit_delay,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_consistency_checking",
						saved_wal_consistency_checking,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_recycle", saved_wal_recycle,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_init_zero", saved_wal_init_zero,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_compression", saved_wal_compression,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("synchronize_seqscans", saved_synchronize_seqscans,
						PGC_USERSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session access/WAL GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_misc_guc_state_is_session_local);
Datum
test_session_misc_guc_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_allow_system_table_mods;
	char	   *saved_dynamic_library_path;
	char	   *saved_extension_control_path;
	char	   *saved_local_preload_libraries;
	char	   *saved_max_stack_depth;
	char	   *saved_session_preload_libraries;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_allow_system_table_mods =
		pstrdup(GetConfigOption("allow_system_table_mods", false, false));
	saved_dynamic_library_path =
		pstrdup(GetConfigOption("dynamic_library_path", false, false));
	saved_extension_control_path =
		pstrdup(GetConfigOption("extension_control_path", false, false));
	saved_local_preload_libraries =
		pstrdup(GetConfigOption("local_preload_libraries", false, false));
	saved_max_stack_depth =
		pstrdup(GetConfigOption("max_stack_depth", false, false));
	saved_session_preload_libraries =
		pstrdup(GetConfigOption("session_preload_libraries", false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && !allowSystemTableMods;
		ok = ok && max_stack_depth == 100;
		ok = ok && *PgCurrentMaxStackDepthBytesRef() == 100 * (ssize_t) 1024;
		ok = ok && session_preload_libraries_string != NULL &&
			session_preload_libraries_string[0] == '\0';
		ok = ok && local_preload_libraries_string != NULL &&
			local_preload_libraries_string[0] == '\0';
		ok = ok && Dynamic_library_path != NULL &&
			strcmp(Dynamic_library_path, "$libdir") == 0;
		ok = ok && strcmp(Extension_control_path, "$system") == 0;
		ok = ok && update_process_title == DEFAULT_UPDATE_PROCESS_TITLE;

		SetConfigOption("allow_system_table_mods", "on",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("max_stack_depth", "101",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("session_preload_libraries", "auto_explain",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("local_preload_libraries", "pg_stat_statements",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("dynamic_library_path", "$libdir/plugins",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("extension_control_path", "$system:/tmp/session1",
						PGC_SUSET, PGC_S_SESSION);
		update_process_title = !DEFAULT_UPDATE_PROCESS_TITLE;
		ok = ok && allowSystemTableMods;
		ok = ok && max_stack_depth == 101;
		ok = ok && *PgCurrentMaxStackDepthBytesRef() == 101 * (ssize_t) 1024;
		ok = ok && session_preload_libraries_string != NULL &&
			strcmp(session_preload_libraries_string, "auto_explain") == 0;
		ok = ok && local_preload_libraries_string != NULL &&
			strcmp(local_preload_libraries_string, "pg_stat_statements") == 0;
		ok = ok && Dynamic_library_path != NULL &&
			strcmp(Dynamic_library_path, "$libdir/plugins") == 0;
		ok = ok && strcmp(Extension_control_path,
						  "$system:/tmp/session1") == 0;
		ok = ok && update_process_title == !DEFAULT_UPDATE_PROCESS_TITLE;

		PgSetCurrentSession(&fake_session2);
		ok = ok && !allowSystemTableMods;
		ok = ok && max_stack_depth == 100;
		ok = ok && *PgCurrentMaxStackDepthBytesRef() == 100 * (ssize_t) 1024;
		ok = ok && strcmp(Extension_control_path, "$system") == 0;
		ok = ok && update_process_title == DEFAULT_UPDATE_PROCESS_TITLE;
		SetConfigOption("allow_system_table_mods", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("max_stack_depth", "102",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("session_preload_libraries", "pg_prewarm",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("local_preload_libraries", "pg_trgm",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("dynamic_library_path", "$libdir",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("extension_control_path", "$system:/tmp/session2",
						PGC_SUSET, PGC_S_SESSION);
		update_process_title = DEFAULT_UPDATE_PROCESS_TITLE;
		ok = ok && !allowSystemTableMods;
		ok = ok && max_stack_depth == 102;
		ok = ok && *PgCurrentMaxStackDepthBytesRef() == 102 * (ssize_t) 1024;
		ok = ok && session_preload_libraries_string != NULL &&
			strcmp(session_preload_libraries_string, "pg_prewarm") == 0;
		ok = ok && local_preload_libraries_string != NULL &&
			strcmp(local_preload_libraries_string, "pg_trgm") == 0;
		ok = ok && Dynamic_library_path != NULL &&
			strcmp(Dynamic_library_path, "$libdir") == 0;
		ok = ok && strcmp(Extension_control_path,
						  "$system:/tmp/session2") == 0;
		ok = ok && update_process_title == DEFAULT_UPDATE_PROCESS_TITLE;

		PgSetCurrentSession(&fake_session1);
		ok = ok && allowSystemTableMods;
		ok = ok && max_stack_depth == 101;
		ok = ok && *PgCurrentMaxStackDepthBytesRef() == 101 * (ssize_t) 1024;
		ok = ok && session_preload_libraries_string != NULL &&
			strcmp(session_preload_libraries_string, "auto_explain") == 0;
		ok = ok && local_preload_libraries_string != NULL &&
			strcmp(local_preload_libraries_string, "pg_stat_statements") == 0;
		ok = ok && Dynamic_library_path != NULL &&
			strcmp(Dynamic_library_path, "$libdir/plugins") == 0;
		ok = ok && strcmp(Extension_control_path,
						  "$system:/tmp/session1") == 0;
		ok = ok && update_process_title == !DEFAULT_UPDATE_PROCESS_TITLE;

		PgSetCurrentSession(&fake_session2);
		ok = ok && !allowSystemTableMods;
		ok = ok && max_stack_depth == 102;
		ok = ok && *PgCurrentMaxStackDepthBytesRef() == 102 * (ssize_t) 1024;
		ok = ok && session_preload_libraries_string != NULL &&
			strcmp(session_preload_libraries_string, "pg_prewarm") == 0;
		ok = ok && local_preload_libraries_string != NULL &&
			strcmp(local_preload_libraries_string, "pg_trgm") == 0;
		ok = ok && Dynamic_library_path != NULL &&
			strcmp(Dynamic_library_path, "$libdir") == 0;
		ok = ok && strcmp(Extension_control_path,
						  "$system:/tmp/session2") == 0;
		ok = ok && update_process_title == DEFAULT_UPDATE_PROCESS_TITLE;

		PgSetCurrentSession(saved_session);
		SetConfigOption("allow_system_table_mods",
						saved_allow_system_table_mods,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("dynamic_library_path", saved_dynamic_library_path,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("extension_control_path",
						saved_extension_control_path,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("local_preload_libraries",
						saved_local_preload_libraries,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("max_stack_depth", saved_max_stack_depth,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("session_preload_libraries",
						saved_session_preload_libraries,
						PGC_SUSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("allow_system_table_mods",
						saved_allow_system_table_mods,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("dynamic_library_path", saved_dynamic_library_path,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("extension_control_path",
						saved_extension_control_path,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("local_preload_libraries",
						saved_local_preload_libraries,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("max_stack_depth", saved_max_stack_depth,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("session_preload_libraries",
						saved_session_preload_libraries,
						PGC_SUSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session miscellaneous GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_guc_state_is_session_local);
Datum
test_session_guc_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	dlist_node	nondef1;
	dlist_node	nondef2;
	slist_node	stack1;
	slist_node	stack2;
	slist_node	report1;
	slist_node	report2;
	bool		ok = true;

	saved_session = CurrentPgSession;
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));

	PG_TRY();
	{
		PgSetCurrentSession(NULL);
		*PgCurrentGUCMemoryContextRef() = (MemoryContext) &fake_session1;
		*PgCurrentGUCVariablesRef() =
			(struct config_generic *) &fake_session1;
		*PgCurrentNumGUCVariablesRef() = 11;
		*PgCurrentGUCHashTableRef() = (HTAB *) &fake_session1;
		dlist_push_head(PgCurrentGUCNondefListRef(), &nondef1);
		slist_push_head(PgCurrentGUCStackListRef(), &stack1);
		slist_push_head(PgCurrentGUCReportListRef(), &report1);
		*PgCurrentGUCReportingEnabledRef() = true;
		*PgCurrentGUCNestLevelRef() = 3;

		PgSessionAdoptEarlyState(&fake_session1);
		ok = ok && fake_session1.guc.memory_context ==
			(MemoryContext) &fake_session1;
		ok = ok && fake_session1.guc.variables ==
			(struct config_generic *) &fake_session1;
		ok = ok && fake_session1.guc.num_variables == 11;
		ok = ok && fake_session1.guc.hash_table == (HTAB *) &fake_session1;
		ok = ok && !dlist_is_empty(&fake_session1.guc.nondef_list);
		ok = ok && dlist_head_node(&fake_session1.guc.nondef_list) ==
			&nondef1;
		ok = ok && !slist_is_empty(&fake_session1.guc.stack_list);
		ok = ok && slist_head_node(&fake_session1.guc.stack_list) == &stack1;
		ok = ok && !slist_is_empty(&fake_session1.guc.report_list);
		ok = ok && slist_head_node(&fake_session1.guc.report_list) ==
			&report1;
		ok = ok && fake_session1.guc.reporting_enabled;
		ok = ok && fake_session1.guc.nest_level == 3;
		ok = ok && *PgCurrentNumGUCVariablesRef() == 0;
		ok = ok && dlist_is_empty(PgCurrentGUCNondefListRef());
		ok = ok && slist_is_empty(PgCurrentGUCStackListRef());
		ok = ok && slist_is_empty(PgCurrentGUCReportListRef());

		CurrentPgSession = &fake_session2;
		ok = ok && *PgCurrentGUCMemoryContextRef() == NULL;
		ok = ok && *PgCurrentGUCVariablesRef() == NULL;
		ok = ok && *PgCurrentNumGUCVariablesRef() == 0;
		ok = ok && *PgCurrentGUCHashTableRef() == NULL;
		ok = ok && dlist_is_empty(PgCurrentGUCNondefListRef());
		ok = ok && slist_is_empty(PgCurrentGUCStackListRef());
		ok = ok && slist_is_empty(PgCurrentGUCReportListRef());
		ok = ok && !*PgCurrentGUCReportingEnabledRef();
		ok = ok && *PgCurrentGUCNestLevelRef() == 0;

		*PgCurrentGUCMemoryContextRef() = (MemoryContext) &fake_session2;
		*PgCurrentGUCVariablesRef() =
			(struct config_generic *) &fake_session2;
		*PgCurrentNumGUCVariablesRef() = 22;
		*PgCurrentGUCHashTableRef() = (HTAB *) &fake_session2;
		dlist_push_head(PgCurrentGUCNondefListRef(), &nondef2);
		slist_push_head(PgCurrentGUCStackListRef(), &stack2);
		slist_push_head(PgCurrentGUCReportListRef(), &report2);
		*PgCurrentGUCReportingEnabledRef() = false;
		*PgCurrentGUCNestLevelRef() = 4;

		CurrentPgSession = &fake_session1;
		ok = ok && *PgCurrentGUCMemoryContextRef() ==
			(MemoryContext) &fake_session1;
		ok = ok && *PgCurrentGUCVariablesRef() ==
			(struct config_generic *) &fake_session1;
		ok = ok && *PgCurrentNumGUCVariablesRef() == 11;
		ok = ok && *PgCurrentGUCHashTableRef() == (HTAB *) &fake_session1;
		ok = ok && dlist_head_node(PgCurrentGUCNondefListRef()) == &nondef1;
		ok = ok && slist_head_node(PgCurrentGUCStackListRef()) == &stack1;
		ok = ok && slist_head_node(PgCurrentGUCReportListRef()) == &report1;
		ok = ok && *PgCurrentGUCReportingEnabledRef();
		ok = ok && *PgCurrentGUCNestLevelRef() == 3;

		CurrentPgSession = &fake_session2;
		ok = ok && *PgCurrentGUCMemoryContextRef() ==
			(MemoryContext) &fake_session2;
		ok = ok && *PgCurrentGUCVariablesRef() ==
			(struct config_generic *) &fake_session2;
		ok = ok && *PgCurrentNumGUCVariablesRef() == 22;
		ok = ok && *PgCurrentGUCHashTableRef() == (HTAB *) &fake_session2;
		ok = ok && dlist_head_node(PgCurrentGUCNondefListRef()) == &nondef2;
		ok = ok && slist_head_node(PgCurrentGUCStackListRef()) == &stack2;
		ok = ok && slist_head_node(PgCurrentGUCReportListRef()) == &report2;
		ok = ok && !*PgCurrentGUCReportingEnabledRef();
		ok = ok && *PgCurrentGUCNestLevelRef() == 4;

		PgSetCurrentSession(saved_session);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session GUC state was not session-local");

	PG_RETURN_BOOL(true);
}
