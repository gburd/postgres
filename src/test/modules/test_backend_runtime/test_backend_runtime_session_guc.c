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
		PgSetCurrentRuntime(&fake_runtime1);
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
		PgSetCurrentRuntime(&fake_runtime2);
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
		PgSetCurrentRuntime(&fake_runtime1);
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
		PgSetCurrentRuntime(saved_runtime);
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
		PgSetCurrentRuntime(saved_runtime);
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
	void	  **runtime1_private = NULL;
	void	  **runtime2_private = NULL;
	const char *runtime_private_key = "test_backend_runtime.runtime_private";
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
		PgSetCurrentRuntime(&fake_runtime1);
		extension_modules = PgCurrentRuntimeExtensionModuleState();
		ok = ok && extension_modules->private_states == NIL;
		ok = ok && PgRuntimeGetExtensionPrivateState(runtime_private_key) == NULL;

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
		runtime1_private = (void **)
			PgRuntimeEnsureExtensionPrivateState(runtime_private_key,
												 sizeof(void *),
												 NULL);
		*runtime1_private = &fake_runtime1;
		extension_modules = PgCurrentRuntimeExtensionModuleState();
		ok = ok && *PgCurrentPgPlanAdviceContextRef() == runtime1_context;
		ok = ok && *PgCurrentPgPlanAdviceAdvisorHookListRef() ==
			runtime1_advisors;
		ok = ok && *PgCurrentBloomContextRef() == runtime1_bloom_context;
		ok = ok && *(void **) PgRuntimeGetExtensionPrivateState(runtime_private_key) ==
			&fake_runtime1;

		stage = "runtime2 default";
		PgSetCurrentRuntime(&fake_runtime2);
		extension_modules = PgCurrentRuntimeExtensionModuleState();
		ok = ok && extension_modules->private_states == NIL;
		ok = ok && PgRuntimeGetExtensionPrivateState(runtime_private_key) == NULL;

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
		runtime2_private = (void **)
			PgRuntimeEnsureExtensionPrivateState(runtime_private_key,
												 sizeof(void *),
												 NULL);
		*runtime2_private = &fake_runtime2;
		extension_modules = PgCurrentRuntimeExtensionModuleState();
		ok = ok && *PgCurrentPgPlanAdviceContextRef() == runtime2_context;
		ok = ok && *PgCurrentPgPlanAdviceAdvisorHookListRef() ==
			runtime2_advisors;
		ok = ok && *PgCurrentBloomContextRef() == runtime2_bloom_context;
		ok = ok && *(void **) PgRuntimeGetExtensionPrivateState(runtime_private_key) ==
			&fake_runtime2;

		stage = "runtime1 restore";
		PgSetCurrentRuntime(&fake_runtime1);
		extension_modules = PgCurrentRuntimeExtensionModuleState();
		ok = ok && *PgCurrentPgPlanAdviceContextRef() == runtime1_context;
		ok = ok && *PgCurrentPgPlanAdviceAdvisorHookListRef() ==
			runtime1_advisors;
		ok = ok && *PgCurrentBloomContextRef() == runtime1_bloom_context;
		ok = ok && *(void **) PgRuntimeGetExtensionPrivateState(runtime_private_key) ==
			&fake_runtime1;

		PgSetCurrentRuntime(saved_runtime);
	}
	PG_CATCH();
	{
		PgSetCurrentRuntime(saved_runtime);
		if (runtime1_context != NULL)
			MemoryContextDelete(runtime1_context);
		if (runtime1_bloom_context != NULL)
			MemoryContextDelete(runtime1_bloom_context);
		if (runtime2_context != NULL)
			MemoryContextDelete(runtime2_context);
		if (runtime2_bloom_context != NULL)
			MemoryContextDelete(runtime2_bloom_context);
		if (fake_runtime1.extension_modules.memory_context != NULL)
			MemoryContextDelete(fake_runtime1.extension_modules.memory_context);
		if (fake_runtime2.extension_modules.memory_context != NULL)
			MemoryContextDelete(fake_runtime2.extension_modules.memory_context);
		PG_RE_THROW();
	}
	PG_END_TRY();

	PgSetCurrentRuntime(saved_runtime);
	if (runtime1_context != NULL)
		MemoryContextDelete(runtime1_context);
	if (runtime1_bloom_context != NULL)
		MemoryContextDelete(runtime1_bloom_context);
	if (runtime2_context != NULL)
		MemoryContextDelete(runtime2_context);
	if (runtime2_bloom_context != NULL)
		MemoryContextDelete(runtime2_bloom_context);
	if (fake_runtime1.extension_modules.memory_context != NULL)
		MemoryContextDelete(fake_runtime1.extension_modules.memory_context);
	if (fake_runtime2.extension_modules.memory_context != NULL)
		MemoryContextDelete(fake_runtime2.extension_modules.memory_context);

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
