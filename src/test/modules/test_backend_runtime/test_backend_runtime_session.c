/*--------------------------------------------------------------------------
 *
 * test_backend_runtime_session.c
 *		Session-owned backend runtime state tests.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_backend_runtime/test_backend_runtime_session.c
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

static void
test_backend_runtime_seed_auto_explain_defaults(PgSessionExtensionModuleState *extension_modules)
{
	extension_modules->auto_explain_log_min_duration = -1;
	extension_modules->auto_explain_log_parameter_max_length = -1;
	extension_modules->auto_explain_log_timing = true;
	extension_modules->auto_explain_log_format = EXPLAIN_FORMAT_TEXT;
	extension_modules->auto_explain_log_level = LOG;
	extension_modules->auto_explain_sample_rate = 1.0;
}

static bool
test_backend_runtime_auto_explain_defaults_ok(PgSessionExtensionModuleState *extension_modules)
{
	return extension_modules->auto_explain_log_min_duration == -1 &&
		extension_modules->auto_explain_log_parameter_max_length == -1 &&
		!extension_modules->auto_explain_log_analyze &&
		!extension_modules->auto_explain_log_verbose &&
		!extension_modules->auto_explain_log_buffers &&
		!extension_modules->auto_explain_log_io &&
		!extension_modules->auto_explain_log_wal &&
		!extension_modules->auto_explain_log_triggers &&
		extension_modules->auto_explain_log_timing &&
		!extension_modules->auto_explain_log_settings &&
		extension_modules->auto_explain_log_format == EXPLAIN_FORMAT_TEXT &&
		extension_modules->auto_explain_log_level == LOG &&
		!extension_modules->auto_explain_log_nested_statements &&
		extension_modules->auto_explain_sample_rate == 1.0 &&
		extension_modules->auto_explain_log_extension_options == NULL &&
		extension_modules->auto_explain_extension_options == NULL;
}

static bool
test_backend_runtime_postgres_fdw_defaults_ok(PgSessionExtensionModuleState *extension_modules)
{
	return extension_modules->postgres_fdw_connection_hash == NULL &&
		extension_modules->postgres_fdw_shippable_cache_hash == NULL &&
		extension_modules->postgres_fdw_cursor_number == 0 &&
		extension_modules->postgres_fdw_prep_stmt_number == 0 &&
		!extension_modules->postgres_fdw_xact_got_connection &&
		extension_modules->postgres_fdw_read_only_level == 0 &&
		!extension_modules->postgres_fdw_connection_callbacks_registered &&
		!extension_modules->postgres_fdw_shippable_callbacks_registered;
}

static bool
test_backend_runtime_refint_defaults_ok(PgSessionExtensionModuleState *extension_modules)
{
	return extension_modules->refint_foreign_plans == NULL &&
		extension_modules->refint_num_foreign_plans == 0 &&
		extension_modules->refint_primary_plans == NULL &&
		extension_modules->refint_num_primary_plans == 0 &&
		!extension_modules->refint_reset_registered;
}

static bool
test_backend_runtime_small_contrib_defaults_ok(PgSessionExtensionModuleState *extension_modules)
{
	return extension_modules->auth_delay_milliseconds == 0 &&
		strcmp(extension_modules->basebackup_to_shell_command, "") == 0 &&
		strcmp(extension_modules->basebackup_to_shell_required_role, "") == 0 &&
		!extension_modules->isn_weak &&
		extension_modules->passwordcheck_min_password_length == 8;
}

static void
test_backend_runtime_seed_small_contrib_defaults(PgSessionExtensionModuleState *extension_modules)
{
	extension_modules->auth_delay_milliseconds = 0;
	extension_modules->basebackup_to_shell_command = "";
	extension_modules->basebackup_to_shell_required_role = "";
	extension_modules->isn_weak = false;
	extension_modules->passwordcheck_min_password_length = 8;
}

void
test_copy_current_user_identity(PgSession *session)
{
	PgSession  *saved_session;

	Assert(session != NULL);

	session->user_identity = *PgCurrentUserIdentityState();
	session->user_identity.system_user_owned = false;
	for (int i = 0; i < lengthof(session->user_identity.cached_roles); i++)
	{
		session->user_identity.cached_role[i] = InvalidOid;
		session->user_identity.cached_roles[i] = NIL;
	}
	session->user_identity.cached_db_hash = 0;

	/*
	 * Many runtime tests use partial fake sessions and then exercise GUC APIs.
	 * Once the GUC registry is PgSession-owned, those fake sessions need their
	 * own variable array/hash instead of falling through to the live backend's
	 * registry.
	 */
	saved_session = CurrentPgSession;
	PgSetCurrentSession(session);
	InitializeThreadedSessionGUCOptions();
	InitializeThreadedSessionRequiredGUCOptions();
	PgSetCurrentSession(saved_session);
}

PG_FUNCTION_INFO_V1(test_session_loop_state_is_session_local);
Datum
test_session_loop_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	bool		ok = true;

	saved_session = CurrentPgSession;
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));

	PG_TRY();
	{
		CurrentPgSession = &fake_session1;
		CurrentPgSession->loop_state.send_ready_for_query = true;
		CurrentPgSession->loop_state.idle_in_transaction_timeout_enabled = true;
		CurrentPgSession->loop_state.doing_extended_query_message = true;
		CurrentPgSession->loop_state.transaction_started = true;

		CurrentPgSession = &fake_session2;
		ok = ok && !CurrentPgSession->loop_state.send_ready_for_query;
		ok = ok && !CurrentPgSession->loop_state.idle_in_transaction_timeout_enabled;
		ok = ok && !CurrentPgSession->loop_state.doing_extended_query_message;
		ok = ok && !CurrentPgSession->loop_state.transaction_started;
		CurrentPgSession->loop_state.send_ready_for_query = true;
		CurrentPgSession->loop_state.idle_session_timeout_enabled = true;
		CurrentPgSession->loop_state.ignore_till_sync = true;
		CurrentPgSession->loop_state.step_error_boundary_active = true;

		CurrentPgSession = &fake_session1;
		ok = ok && CurrentPgSession->loop_state.send_ready_for_query;
		ok = ok && CurrentPgSession->loop_state.idle_in_transaction_timeout_enabled;
		ok = ok && !CurrentPgSession->loop_state.idle_session_timeout_enabled;
		ok = ok && CurrentPgSession->loop_state.doing_extended_query_message;
		ok = ok && !CurrentPgSession->loop_state.ignore_till_sync;
		ok = ok && !CurrentPgSession->loop_state.step_error_boundary_active;
		ok = ok && CurrentPgSession->loop_state.transaction_started;

		CurrentPgSession = &fake_session2;
		ok = ok && CurrentPgSession->loop_state.send_ready_for_query;
		ok = ok && !CurrentPgSession->loop_state.idle_in_transaction_timeout_enabled;
		ok = ok && CurrentPgSession->loop_state.idle_session_timeout_enabled;
		ok = ok && !CurrentPgSession->loop_state.doing_extended_query_message;
		ok = ok && CurrentPgSession->loop_state.ignore_till_sync;
		ok = ok && CurrentPgSession->loop_state.step_error_boundary_active;
		ok = ok && !CurrentPgSession->loop_state.transaction_started;

		CurrentPgSession = saved_session;
	}
	PG_CATCH();
	{
		CurrentPgSession = saved_session;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session loop state was not session-local");

	PG_RETURN_BOOL(true);
}
PG_FUNCTION_INFO_V1(test_session_tcop_state_is_session_local);
Datum
test_session_tcop_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	bool		ok = true;

	saved_session = CurrentPgSession;
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));

	PG_TRY();
	{
		PgSetCurrentSession(NULL);
		*PgCurrentEchoQueryRef() = true;
		*PgCurrentUseSemiNewlineNewlineRef() = true;

		PgSessionAdoptEarlyState(&fake_session1);

		ok = ok && fake_session1.tcop.echo_query;
		ok = ok && fake_session1.tcop.use_semi_newline_newline;
		ok = ok && !*PgCurrentEchoQueryRef();
		ok = ok && !*PgCurrentUseSemiNewlineNewlineRef();

		PgSetCurrentSession(&fake_session1);
		*PgCurrentUnnamedStmtPsrcRef() =
			(CachedPlanSource *) &fake_session1;
		*PgCurrentEchoQueryRef() = false;
		*PgCurrentUseSemiNewlineNewlineRef() = true;
		*PgCurrentRowDescriptionContextRef() =
			(MemoryContext) &fake_session1;
		PgCurrentRowDescriptionBufRef()->data = (char *) "session one";
		PgCurrentRowDescriptionBufRef()->len = 11;

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentUnnamedStmtPsrcRef() == NULL;
		ok = ok && !*PgCurrentEchoQueryRef();
		ok = ok && !*PgCurrentUseSemiNewlineNewlineRef();
		ok = ok && *PgCurrentRowDescriptionContextRef() == NULL;
		ok = ok && PgCurrentRowDescriptionBufRef()->data == NULL;
		*PgCurrentUnnamedStmtPsrcRef() =
			(CachedPlanSource *) &fake_session2;
		*PgCurrentEchoQueryRef() = true;
		*PgCurrentUseSemiNewlineNewlineRef() = false;
		*PgCurrentRowDescriptionContextRef() =
			(MemoryContext) &fake_session2;
		PgCurrentRowDescriptionBufRef()->data = (char *) "session two";
		PgCurrentRowDescriptionBufRef()->len = 22;

		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentUnnamedStmtPsrcRef() ==
			(CachedPlanSource *) &fake_session1;
		ok = ok && !*PgCurrentEchoQueryRef();
		ok = ok && *PgCurrentUseSemiNewlineNewlineRef();
		ok = ok && *PgCurrentRowDescriptionContextRef() ==
			(MemoryContext) &fake_session1;
		ok = ok && strcmp(PgCurrentRowDescriptionBufRef()->data,
						  "session one") == 0;
		ok = ok && PgCurrentRowDescriptionBufRef()->len == 11;

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentUnnamedStmtPsrcRef() ==
			(CachedPlanSource *) &fake_session2;
		ok = ok && *PgCurrentEchoQueryRef();
		ok = ok && !*PgCurrentUseSemiNewlineNewlineRef();
		ok = ok && *PgCurrentRowDescriptionContextRef() ==
			(MemoryContext) &fake_session2;
		ok = ok && strcmp(PgCurrentRowDescriptionBufRef()->data,
						  "session two") == 0;
		ok = ok && PgCurrentRowDescriptionBufRef()->len == 22;

		PgSetCurrentSession(saved_session);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session tcop state was not session-local");

	PG_RETURN_BOOL(true);
}
PG_FUNCTION_INFO_V1(test_session_xact_callback_state_is_session_local);
Datum
test_session_xact_callback_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	bool		ok = true;

	saved_session = CurrentPgSession;
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));

	PG_TRY();
	{
		PgSetCurrentSession(NULL);
		*PgCurrentXactCallbacksRef() = (XactCallbackItem *) &fake_session1;
		*PgCurrentSubXactCallbacksRef() =
			(SubXactCallbackItem *) &fake_session1;

		PgSessionAdoptEarlyState(&fake_session1);

		ok = ok && fake_session1.xact_callbacks.xact_callbacks ==
			(XactCallbackItem *) &fake_session1;
		ok = ok && fake_session1.xact_callbacks.subxact_callbacks ==
			(SubXactCallbackItem *) &fake_session1;
		ok = ok && *PgCurrentXactCallbacksRef() == NULL;
		ok = ok && *PgCurrentSubXactCallbacksRef() == NULL;

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentXactCallbacksRef() == NULL;
		ok = ok && *PgCurrentSubXactCallbacksRef() == NULL;
		*PgCurrentXactCallbacksRef() = (XactCallbackItem *) &fake_session2;
		*PgCurrentSubXactCallbacksRef() =
			(SubXactCallbackItem *) &fake_session2;

		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentXactCallbacksRef() ==
			(XactCallbackItem *) &fake_session1;
		ok = ok && *PgCurrentSubXactCallbacksRef() ==
			(SubXactCallbackItem *) &fake_session1;

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentXactCallbacksRef() ==
			(XactCallbackItem *) &fake_session2;
		ok = ok && *PgCurrentSubXactCallbacksRef() ==
			(SubXactCallbackItem *) &fake_session2;

		PgSetCurrentSession(saved_session);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session xact callback state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_backup_state_is_session_local);
Datum
test_session_backup_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	bool		ok = true;

	saved_session = CurrentPgSession;
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));

	PG_TRY();
	{
		PgSetCurrentSession(NULL);
		*PgCurrentBackupStateRef() = (struct BackupState *) &fake_session1;
		*PgCurrentTablespaceMapRef() = (StringInfo) &fake_session1;
		*PgCurrentBackupContextRef() = (MemoryContext) &fake_session1;
		*PgCurrentSessionBackupStateRef() = SESSION_BACKUP_RUNNING;

		PgSessionAdoptEarlyState(&fake_session1);

		ok = ok && fake_session1.backup.backup_state ==
			(struct BackupState *) &fake_session1;
		ok = ok && fake_session1.backup.tablespace_map ==
			(StringInfo) &fake_session1;
		ok = ok && fake_session1.backup.backup_context ==
			(MemoryContext) &fake_session1;
		ok = ok && fake_session1.backup.session_backup_state ==
			SESSION_BACKUP_RUNNING;
		ok = ok && *PgCurrentBackupStateRef() == NULL;
		ok = ok && *PgCurrentTablespaceMapRef() == NULL;
		ok = ok && *PgCurrentBackupContextRef() == NULL;
		ok = ok && *PgCurrentSessionBackupStateRef() ==
			SESSION_BACKUP_NONE;

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentBackupStateRef() == NULL;
		ok = ok && *PgCurrentTablespaceMapRef() == NULL;
		ok = ok && *PgCurrentBackupContextRef() == NULL;
		ok = ok && *PgCurrentSessionBackupStateRef() ==
			SESSION_BACKUP_NONE;
		*PgCurrentBackupStateRef() = (struct BackupState *) &fake_session2;
		*PgCurrentTablespaceMapRef() = (StringInfo) &fake_session2;
		*PgCurrentBackupContextRef() = (MemoryContext) &fake_session2;
		*PgCurrentSessionBackupStateRef() = SESSION_BACKUP_RUNNING;

		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentBackupStateRef() ==
			(struct BackupState *) &fake_session1;
		ok = ok && *PgCurrentTablespaceMapRef() ==
			(StringInfo) &fake_session1;
		ok = ok && *PgCurrentBackupContextRef() ==
			(MemoryContext) &fake_session1;
		ok = ok && *PgCurrentSessionBackupStateRef() ==
			SESSION_BACKUP_RUNNING;

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentBackupStateRef() ==
			(struct BackupState *) &fake_session2;
		ok = ok && *PgCurrentTablespaceMapRef() ==
			(StringInfo) &fake_session2;
		ok = ok && *PgCurrentBackupContextRef() ==
			(MemoryContext) &fake_session2;
		ok = ok && *PgCurrentSessionBackupStateRef() ==
			SESSION_BACKUP_RUNNING;

		PgSetCurrentSession(saved_session);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session backup state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_database_state_is_session_local);
Datum
test_session_database_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	Oid			saved_database_id;
	Oid			saved_database_tablespace;
	bool		saved_database_has_login_event_triggers;
	char	   *saved_database_path;
	char	   *fake_path1 = "base/1";
	char	   *fake_path2 = "base/2";
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_database_id = MyDatabaseId;
	saved_database_tablespace = MyDatabaseTableSpace;
	saved_database_has_login_event_triggers =
		MyDatabaseHasLoginEventTriggers;
	saved_database_path = DatabasePath;
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	fake_session1.catalog_lookup.typcache_tupledesc_id_counter = (uint64) 1;
	fake_session2.catalog_lookup.typcache_tupledesc_id_counter = (uint64) 1;
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		CurrentPgSession = &fake_session1;
		MyDatabaseId = 1111;
		MyDatabaseTableSpace = 2222;
		MyDatabaseHasLoginEventTriggers = true;
		DatabasePath = fake_path1;

		CurrentPgSession = &fake_session2;
		ok = ok && MyDatabaseId == InvalidOid;
		ok = ok && MyDatabaseTableSpace == InvalidOid;
		ok = ok && !MyDatabaseHasLoginEventTriggers;
		ok = ok && DatabasePath == NULL;
		MyDatabaseId = 3333;
		MyDatabaseTableSpace = 4444;
		MyDatabaseHasLoginEventTriggers = false;
		DatabasePath = fake_path2;

		CurrentPgSession = &fake_session1;
		ok = ok && MyDatabaseId == 1111;
		ok = ok && MyDatabaseTableSpace == 2222;
		ok = ok && MyDatabaseHasLoginEventTriggers;
		ok = ok && DatabasePath == fake_path1;

		CurrentPgSession = &fake_session2;
		ok = ok && MyDatabaseId == 3333;
		ok = ok && MyDatabaseTableSpace == 4444;
		ok = ok && !MyDatabaseHasLoginEventTriggers;
		ok = ok && DatabasePath == fake_path2;

		CurrentPgSession = saved_session;
		MyDatabaseId = saved_database_id;
		MyDatabaseTableSpace = saved_database_tablespace;
		MyDatabaseHasLoginEventTriggers =
			saved_database_has_login_event_triggers;
		DatabasePath = saved_database_path;
	}
	PG_CATCH();
	{
		CurrentPgSession = saved_session;
		MyDatabaseId = saved_database_id;
		MyDatabaseTableSpace = saved_database_tablespace;
		MyDatabaseHasLoginEventTriggers =
			saved_database_has_login_event_triggers;
		DatabasePath = saved_database_path;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session database state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_tablespace_state_is_session_local);
Datum
test_session_tablespace_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_default_tablespace;
	char	   *saved_temp_tablespaces;
	char	   *saved_allow_in_place_tablespaces;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_default_tablespace =
		pstrdup(GetConfigOption("default_tablespace", false, false));
	saved_temp_tablespaces =
		pstrdup(GetConfigOption("temp_tablespaces", false, false));
	saved_allow_in_place_tablespaces =
		pstrdup(GetConfigOption("allow_in_place_tablespaces", false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && default_tablespace != NULL;
		ok = ok && default_tablespace[0] == '\0';
		ok = ok && temp_tablespaces != NULL;
		ok = ok && temp_tablespaces[0] == '\0';
		ok = ok && !allow_in_place_tablespaces;
		ok = ok && binary_upgrade_next_pg_tablespace_oid == InvalidOid;
		SetConfigOption("default_tablespace", "pg_default",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("temp_tablespaces", "pg_default",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("allow_in_place_tablespaces", "on",
						PGC_SUSET, PGC_S_SESSION);
		binary_upgrade_next_pg_tablespace_oid = 12345;
		ok = ok && strcmp(default_tablespace, "pg_default") == 0;
		ok = ok && strcmp(temp_tablespaces, "pg_default") == 0;
		ok = ok && allow_in_place_tablespaces;
		ok = ok && binary_upgrade_next_pg_tablespace_oid == 12345;

		PgSetCurrentSession(&fake_session2);
		ok = ok && default_tablespace != NULL;
		ok = ok && default_tablespace[0] == '\0';
		ok = ok && temp_tablespaces != NULL;
		ok = ok && temp_tablespaces[0] == '\0';
		ok = ok && !allow_in_place_tablespaces;
		ok = ok && binary_upgrade_next_pg_tablespace_oid == InvalidOid;
		SetConfigOption("default_tablespace", "",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("temp_tablespaces", "",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("allow_in_place_tablespaces", "off",
						PGC_SUSET, PGC_S_SESSION);
		binary_upgrade_next_pg_tablespace_oid = 67890;
		ok = ok && default_tablespace != NULL;
		ok = ok && default_tablespace[0] == '\0';
		ok = ok && temp_tablespaces != NULL;
		ok = ok && temp_tablespaces[0] == '\0';
		ok = ok && !allow_in_place_tablespaces;
		ok = ok && binary_upgrade_next_pg_tablespace_oid == 67890;

		PgSetCurrentSession(&fake_session1);
		ok = ok && strcmp(default_tablespace, "pg_default") == 0;
		ok = ok && strcmp(temp_tablespaces, "pg_default") == 0;
		ok = ok && allow_in_place_tablespaces;
		ok = ok && binary_upgrade_next_pg_tablespace_oid == 12345;

		PgSetCurrentSession(saved_session);
		SetConfigOption("default_tablespace", saved_default_tablespace,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("temp_tablespaces", saved_temp_tablespaces,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("allow_in_place_tablespaces",
						saved_allow_in_place_tablespaces,
						PGC_SUSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("default_tablespace", saved_default_tablespace,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("temp_tablespaces", saved_temp_tablespaces,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("allow_in_place_tablespaces",
						saved_allow_in_place_tablespaces,
						PGC_SUSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session tablespace state was not session-local");

PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_binary_upgrade_state_is_session_local);
Datum
test_session_binary_upgrade_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	Oid			saved_pg_type_oid;
	Oid			saved_array_pg_type_oid;
	Oid			saved_mrng_pg_type_oid;
	Oid			saved_mrng_array_pg_type_oid;
	Oid			saved_heap_pg_class_oid;
	RelFileNumber saved_heap_pg_class_relfilenumber;
	Oid			saved_index_pg_class_oid;
	RelFileNumber saved_index_pg_class_relfilenumber;
	Oid			saved_toast_pg_class_oid;
	RelFileNumber saved_toast_pg_class_relfilenumber;
	Oid			saved_pg_enum_oid;
	Oid			saved_pg_authid_oid;
	bool		saved_record_init_privs;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_pg_type_oid = binary_upgrade_next_pg_type_oid;
	saved_array_pg_type_oid = binary_upgrade_next_array_pg_type_oid;
	saved_mrng_pg_type_oid = binary_upgrade_next_mrng_pg_type_oid;
	saved_mrng_array_pg_type_oid = binary_upgrade_next_mrng_array_pg_type_oid;
	saved_heap_pg_class_oid = binary_upgrade_next_heap_pg_class_oid;
	saved_heap_pg_class_relfilenumber =
		binary_upgrade_next_heap_pg_class_relfilenumber;
	saved_index_pg_class_oid = binary_upgrade_next_index_pg_class_oid;
	saved_index_pg_class_relfilenumber =
		binary_upgrade_next_index_pg_class_relfilenumber;
	saved_toast_pg_class_oid = binary_upgrade_next_toast_pg_class_oid;
	saved_toast_pg_class_relfilenumber =
		binary_upgrade_next_toast_pg_class_relfilenumber;
	saved_pg_enum_oid = binary_upgrade_next_pg_enum_oid;
	saved_pg_authid_oid = binary_upgrade_next_pg_authid_oid;
	saved_record_init_privs = binary_upgrade_record_init_privs;
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && binary_upgrade_next_pg_type_oid == InvalidOid;
		ok = ok && binary_upgrade_next_array_pg_type_oid == InvalidOid;
		ok = ok && binary_upgrade_next_mrng_pg_type_oid == InvalidOid;
		ok = ok && binary_upgrade_next_mrng_array_pg_type_oid == InvalidOid;
		ok = ok && binary_upgrade_next_heap_pg_class_oid == InvalidOid;
		ok = ok && binary_upgrade_next_heap_pg_class_relfilenumber ==
			InvalidRelFileNumber;
		ok = ok && binary_upgrade_next_index_pg_class_oid == InvalidOid;
		ok = ok && binary_upgrade_next_index_pg_class_relfilenumber ==
			InvalidRelFileNumber;
		ok = ok && binary_upgrade_next_toast_pg_class_oid == InvalidOid;
		ok = ok && binary_upgrade_next_toast_pg_class_relfilenumber ==
			InvalidRelFileNumber;
		ok = ok && binary_upgrade_next_pg_enum_oid == InvalidOid;
		ok = ok && binary_upgrade_next_pg_authid_oid == InvalidOid;
		ok = ok && !binary_upgrade_record_init_privs;
		binary_upgrade_next_pg_type_oid = 1001;
		binary_upgrade_next_array_pg_type_oid = 1002;
		binary_upgrade_next_mrng_pg_type_oid = 1003;
		binary_upgrade_next_mrng_array_pg_type_oid = 1004;
		binary_upgrade_next_heap_pg_class_oid = 1005;
		binary_upgrade_next_heap_pg_class_relfilenumber = 1006;
		binary_upgrade_next_index_pg_class_oid = 1007;
		binary_upgrade_next_index_pg_class_relfilenumber = 1008;
		binary_upgrade_next_toast_pg_class_oid = 1009;
		binary_upgrade_next_toast_pg_class_relfilenumber = 1010;
		binary_upgrade_next_pg_enum_oid = 1011;
		binary_upgrade_next_pg_authid_oid = 1012;
		binary_upgrade_record_init_privs = true;

		PgSetCurrentSession(&fake_session2);
		ok = ok && binary_upgrade_next_pg_type_oid == InvalidOid;
		ok = ok && binary_upgrade_next_array_pg_type_oid == InvalidOid;
		ok = ok && binary_upgrade_next_mrng_pg_type_oid == InvalidOid;
		ok = ok && binary_upgrade_next_mrng_array_pg_type_oid == InvalidOid;
		ok = ok && binary_upgrade_next_heap_pg_class_oid == InvalidOid;
		ok = ok && binary_upgrade_next_heap_pg_class_relfilenumber ==
			InvalidRelFileNumber;
		ok = ok && binary_upgrade_next_index_pg_class_oid == InvalidOid;
		ok = ok && binary_upgrade_next_index_pg_class_relfilenumber ==
			InvalidRelFileNumber;
		ok = ok && binary_upgrade_next_toast_pg_class_oid == InvalidOid;
		ok = ok && binary_upgrade_next_toast_pg_class_relfilenumber ==
			InvalidRelFileNumber;
		ok = ok && binary_upgrade_next_pg_enum_oid == InvalidOid;
		ok = ok && binary_upgrade_next_pg_authid_oid == InvalidOid;
		ok = ok && !binary_upgrade_record_init_privs;
		binary_upgrade_next_pg_type_oid = 2001;
		binary_upgrade_next_array_pg_type_oid = 2002;
		binary_upgrade_next_mrng_pg_type_oid = 2003;
		binary_upgrade_next_mrng_array_pg_type_oid = 2004;
		binary_upgrade_next_heap_pg_class_oid = 2005;
		binary_upgrade_next_heap_pg_class_relfilenumber = 2006;
		binary_upgrade_next_index_pg_class_oid = 2007;
		binary_upgrade_next_index_pg_class_relfilenumber = 2008;
		binary_upgrade_next_toast_pg_class_oid = 2009;
		binary_upgrade_next_toast_pg_class_relfilenumber = 2010;
		binary_upgrade_next_pg_enum_oid = 2011;
		binary_upgrade_next_pg_authid_oid = 2012;
		binary_upgrade_record_init_privs = false;

		PgSetCurrentSession(&fake_session1);
		ok = ok && binary_upgrade_next_pg_type_oid == 1001;
		ok = ok && binary_upgrade_next_array_pg_type_oid == 1002;
		ok = ok && binary_upgrade_next_mrng_pg_type_oid == 1003;
		ok = ok && binary_upgrade_next_mrng_array_pg_type_oid == 1004;
		ok = ok && binary_upgrade_next_heap_pg_class_oid == 1005;
		ok = ok && binary_upgrade_next_heap_pg_class_relfilenumber == 1006;
		ok = ok && binary_upgrade_next_index_pg_class_oid == 1007;
		ok = ok && binary_upgrade_next_index_pg_class_relfilenumber == 1008;
		ok = ok && binary_upgrade_next_toast_pg_class_oid == 1009;
		ok = ok && binary_upgrade_next_toast_pg_class_relfilenumber == 1010;
		ok = ok && binary_upgrade_next_pg_enum_oid == 1011;
		ok = ok && binary_upgrade_next_pg_authid_oid == 1012;
		ok = ok && binary_upgrade_record_init_privs;

		PgSetCurrentSession(&fake_session2);
		ok = ok && binary_upgrade_next_pg_type_oid == 2001;
		ok = ok && binary_upgrade_next_array_pg_type_oid == 2002;
		ok = ok && binary_upgrade_next_mrng_pg_type_oid == 2003;
		ok = ok && binary_upgrade_next_mrng_array_pg_type_oid == 2004;
		ok = ok && binary_upgrade_next_heap_pg_class_oid == 2005;
		ok = ok && binary_upgrade_next_heap_pg_class_relfilenumber == 2006;
		ok = ok && binary_upgrade_next_index_pg_class_oid == 2007;
		ok = ok && binary_upgrade_next_index_pg_class_relfilenumber == 2008;
		ok = ok && binary_upgrade_next_toast_pg_class_oid == 2009;
		ok = ok && binary_upgrade_next_toast_pg_class_relfilenumber == 2010;
		ok = ok && binary_upgrade_next_pg_enum_oid == 2011;
		ok = ok && binary_upgrade_next_pg_authid_oid == 2012;
		ok = ok && !binary_upgrade_record_init_privs;

		PgSetCurrentSession(saved_session);
		binary_upgrade_next_pg_type_oid = saved_pg_type_oid;
		binary_upgrade_next_array_pg_type_oid = saved_array_pg_type_oid;
		binary_upgrade_next_mrng_pg_type_oid = saved_mrng_pg_type_oid;
		binary_upgrade_next_mrng_array_pg_type_oid =
			saved_mrng_array_pg_type_oid;
		binary_upgrade_next_heap_pg_class_oid = saved_heap_pg_class_oid;
		binary_upgrade_next_heap_pg_class_relfilenumber =
			saved_heap_pg_class_relfilenumber;
		binary_upgrade_next_index_pg_class_oid = saved_index_pg_class_oid;
		binary_upgrade_next_index_pg_class_relfilenumber =
			saved_index_pg_class_relfilenumber;
		binary_upgrade_next_toast_pg_class_oid = saved_toast_pg_class_oid;
		binary_upgrade_next_toast_pg_class_relfilenumber =
			saved_toast_pg_class_relfilenumber;
		binary_upgrade_next_pg_enum_oid = saved_pg_enum_oid;
		binary_upgrade_next_pg_authid_oid = saved_pg_authid_oid;
		binary_upgrade_record_init_privs = saved_record_init_privs;
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		binary_upgrade_next_pg_type_oid = saved_pg_type_oid;
		binary_upgrade_next_array_pg_type_oid = saved_array_pg_type_oid;
		binary_upgrade_next_mrng_pg_type_oid = saved_mrng_pg_type_oid;
		binary_upgrade_next_mrng_array_pg_type_oid =
			saved_mrng_array_pg_type_oid;
		binary_upgrade_next_heap_pg_class_oid = saved_heap_pg_class_oid;
		binary_upgrade_next_heap_pg_class_relfilenumber =
			saved_heap_pg_class_relfilenumber;
		binary_upgrade_next_index_pg_class_oid = saved_index_pg_class_oid;
		binary_upgrade_next_index_pg_class_relfilenumber =
			saved_index_pg_class_relfilenumber;
		binary_upgrade_next_toast_pg_class_oid = saved_toast_pg_class_oid;
		binary_upgrade_next_toast_pg_class_relfilenumber =
			saved_toast_pg_class_relfilenumber;
		binary_upgrade_next_pg_enum_oid = saved_pg_enum_oid;
		binary_upgrade_next_pg_authid_oid = saved_pg_authid_oid;
		binary_upgrade_record_init_privs = saved_record_init_privs;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session binary-upgrade state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_datetime_state_is_session_local);
Datum
test_session_datetime_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	int			saved_date_style;
	int			saved_date_order;
	char	   *saved_interval_style;
	char	   *saved_timezone;
	char	   *saved_log_timezone;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_date_style = DateStyle;
	saved_date_order = DateOrder;
	saved_interval_style = pstrdup(GetConfigOption("IntervalStyle",
												   false, false));
	saved_timezone = pstrdup(GetConfigOption("TimeZone", false, false));
	saved_log_timezone = pstrdup(GetConfigOption("log_timezone",
												 false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && DateStyle == USE_ISO_DATES;
		ok = ok && DateOrder == DATEORDER_MDY;
		ok = ok && IntervalStyle == INTSTYLE_POSTGRES;
		ok = ok && strcmp(*PgCurrentTimeZoneStringRef(), "GMT") == 0;
		ok = ok && strcmp(*PgCurrentLogTimeZoneStringRef(), "GMT") == 0;
		ok = ok && session_timezone != NULL &&
			strcmp(pg_get_timezone_name(session_timezone), "GMT") == 0;
		ok = ok && log_timezone != NULL &&
			strcmp(pg_get_timezone_name(log_timezone), "GMT") == 0;
		ok = ok && *PgCurrentTimeZoneAbbrevTableRef() == NULL;
		ok = ok && PgCurrentTimeZoneAbbrevCache()[0].abbrev[0] == '\0';
		ok = ok && *PgCurrentReplicationOriginSessionStateRef() == NULL;
		ok = ok && *PgCurrentLogicalRepRelMapContextRef() == NULL;
		ok = ok && *PgCurrentLogicalRepRelMapRef() == NULL;
		ok = ok && *PgCurrentLogicalRepPartMapContextRef() == NULL;
		ok = ok && *PgCurrentLogicalRepPartMapRef() == NULL;
		ok = ok && !*PgCurrentPgOutputPublicationsValidRef();
		ok = ok && *PgCurrentPgOutputRelationSyncCacheRef() == NULL;
		ok = ok && *PgCurrentLogicalRepSyncingRelationsStateRef() == 0;
		DateStyle = USE_SQL_DATES;
		DateOrder = DATEORDER_DMY;
		SetConfigOption("IntervalStyle", "sql_standard",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("TimeZone", "UTC",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_timezone", "UTC",
						PGC_SIGHUP, PGC_S_FILE);
		ok = ok && IntervalStyle == INTSTYLE_SQL_STANDARD;
		ok = ok && session_timezone != NULL &&
			strcmp(pg_get_timezone_name(session_timezone), "UTC") == 0;
		ok = ok && log_timezone != NULL &&
			strcmp(pg_get_timezone_name(log_timezone), "UTC") == 0;
		*PgCurrentTimeZoneAbbrevTableRef() =
			(TimeZoneAbbrevTable *) &fake_session1;
		strlcpy(PgCurrentTimeZoneAbbrevCache()[0].abbrev, "utc",
				TOKMAXLEN + 1);
		PgCurrentTimeZoneAbbrevCache()[0].ftype = TZ;
		PgCurrentTimeZoneAbbrevCache()[0].offset = 11;
		PgCurrentTimeZoneAbbrevCache()[0].tz = (pg_tz *) &fake_session1;
		*PgCurrentReplicationOriginSessionStateRef() =
			(struct ReplicationState *) &fake_session1;
		*PgCurrentLogicalRepRelMapContextRef() = (MemoryContext) &fake_session1;
		*PgCurrentLogicalRepRelMapRef() = (HTAB *) &fake_session1;
		*PgCurrentLogicalRepPartMapContextRef() = (MemoryContext) &fake_session1;
		*PgCurrentLogicalRepPartMapRef() = (HTAB *) &fake_session1;
		*PgCurrentPgOutputPublicationsValidRef() = true;
		*PgCurrentPgOutputRelationSyncCacheRef() = (HTAB *) &fake_session1;
		*PgCurrentLogicalRepSyncingRelationsStateRef() = 17;

		PgSetCurrentSession(&fake_session2);
		ok = ok && DateStyle == USE_ISO_DATES;
		ok = ok && DateOrder == DATEORDER_MDY;
		ok = ok && IntervalStyle == INTSTYLE_POSTGRES;
		ok = ok && strcmp(*PgCurrentTimeZoneStringRef(), "GMT") == 0;
		ok = ok && strcmp(*PgCurrentLogTimeZoneStringRef(), "GMT") == 0;
		ok = ok && session_timezone != NULL &&
			strcmp(pg_get_timezone_name(session_timezone), "GMT") == 0;
		ok = ok && log_timezone != NULL &&
			strcmp(pg_get_timezone_name(log_timezone), "GMT") == 0;
		ok = ok && *PgCurrentTimeZoneAbbrevTableRef() == NULL;
		ok = ok && PgCurrentTimeZoneAbbrevCache()[0].abbrev[0] == '\0';
		ok = ok && *PgCurrentReplicationOriginSessionStateRef() == NULL;
		ok = ok && *PgCurrentLogicalRepRelMapContextRef() == NULL;
		ok = ok && *PgCurrentLogicalRepRelMapRef() == NULL;
		ok = ok && *PgCurrentLogicalRepPartMapContextRef() == NULL;
		ok = ok && *PgCurrentLogicalRepPartMapRef() == NULL;
		ok = ok && !*PgCurrentPgOutputPublicationsValidRef();
		ok = ok && *PgCurrentPgOutputRelationSyncCacheRef() == NULL;
		ok = ok && *PgCurrentLogicalRepSyncingRelationsStateRef() == 0;
		DateStyle = USE_GERMAN_DATES;
		DateOrder = DATEORDER_YMD;
		SetConfigOption("IntervalStyle", "iso_8601",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("TimeZone", "Europe/London",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_timezone", "Europe/London",
						PGC_SIGHUP, PGC_S_FILE);
		ok = ok && IntervalStyle == INTSTYLE_ISO_8601;
		ok = ok && session_timezone != NULL &&
			strcmp(pg_get_timezone_name(session_timezone), "Europe/London") == 0;
		ok = ok && log_timezone != NULL &&
			strcmp(pg_get_timezone_name(log_timezone), "Europe/London") == 0;
		*PgCurrentTimeZoneAbbrevTableRef() =
			(TimeZoneAbbrevTable *) &fake_session2;
		strlcpy(PgCurrentTimeZoneAbbrevCache()[0].abbrev, "bst",
				TOKMAXLEN + 1);
		PgCurrentTimeZoneAbbrevCache()[0].ftype = DYNTZ;
		PgCurrentTimeZoneAbbrevCache()[0].offset = 22;
		PgCurrentTimeZoneAbbrevCache()[0].tz = (pg_tz *) &fake_session2;
		*PgCurrentReplicationOriginSessionStateRef() =
			(struct ReplicationState *) &fake_session2;
		*PgCurrentLogicalRepRelMapContextRef() = (MemoryContext) &fake_session2;
		*PgCurrentLogicalRepRelMapRef() = (HTAB *) &fake_session2;
		*PgCurrentLogicalRepPartMapContextRef() = (MemoryContext) &fake_session2;
		*PgCurrentLogicalRepPartMapRef() = (HTAB *) &fake_session2;
		*PgCurrentPgOutputPublicationsValidRef() = true;
		*PgCurrentPgOutputRelationSyncCacheRef() = (HTAB *) &fake_session2;
		*PgCurrentLogicalRepSyncingRelationsStateRef() = 23;

		PgSetCurrentSession(&fake_session1);
		ok = ok && DateStyle == USE_SQL_DATES;
		ok = ok && DateOrder == DATEORDER_DMY;
		ok = ok && IntervalStyle == INTSTYLE_SQL_STANDARD;
		ok = ok && session_timezone != NULL &&
			strcmp(pg_get_timezone_name(session_timezone), "UTC") == 0;
		ok = ok && log_timezone != NULL &&
			strcmp(pg_get_timezone_name(log_timezone), "UTC") == 0;
		ok = ok && *PgCurrentTimeZoneAbbrevTableRef() ==
			(TimeZoneAbbrevTable *) &fake_session1;
		ok = ok && strcmp(PgCurrentTimeZoneAbbrevCache()[0].abbrev,
						  "utc") == 0;
		ok = ok && PgCurrentTimeZoneAbbrevCache()[0].ftype == TZ;
		ok = ok && PgCurrentTimeZoneAbbrevCache()[0].offset == 11;
		ok = ok && PgCurrentTimeZoneAbbrevCache()[0].tz ==
			(pg_tz *) &fake_session1;
		ok = ok && *PgCurrentReplicationOriginSessionStateRef() ==
			(struct ReplicationState *) &fake_session1;
		ok = ok && *PgCurrentLogicalRepRelMapContextRef() ==
			(MemoryContext) &fake_session1;
		ok = ok && *PgCurrentLogicalRepRelMapRef() ==
			(HTAB *) &fake_session1;
		ok = ok && *PgCurrentLogicalRepPartMapContextRef() ==
			(MemoryContext) &fake_session1;
		ok = ok && *PgCurrentLogicalRepPartMapRef() ==
			(HTAB *) &fake_session1;
		ok = ok && *PgCurrentPgOutputPublicationsValidRef();
		ok = ok && *PgCurrentPgOutputRelationSyncCacheRef() ==
			(HTAB *) &fake_session1;
		ok = ok && *PgCurrentLogicalRepSyncingRelationsStateRef() == 17;

		PgSetCurrentSession(&fake_session2);
		ok = ok && DateStyle == USE_GERMAN_DATES;
		ok = ok && DateOrder == DATEORDER_YMD;
		ok = ok && IntervalStyle == INTSTYLE_ISO_8601;
		ok = ok && session_timezone != NULL &&
			strcmp(pg_get_timezone_name(session_timezone), "Europe/London") == 0;
		ok = ok && log_timezone != NULL &&
			strcmp(pg_get_timezone_name(log_timezone), "Europe/London") == 0;
		ok = ok && *PgCurrentTimeZoneAbbrevTableRef() ==
			(TimeZoneAbbrevTable *) &fake_session2;
		ok = ok && strcmp(PgCurrentTimeZoneAbbrevCache()[0].abbrev,
						  "bst") == 0;
		ok = ok && PgCurrentTimeZoneAbbrevCache()[0].ftype == DYNTZ;
		ok = ok && PgCurrentTimeZoneAbbrevCache()[0].offset == 22;
		ok = ok && PgCurrentTimeZoneAbbrevCache()[0].tz ==
			(pg_tz *) &fake_session2;
		ok = ok && *PgCurrentReplicationOriginSessionStateRef() ==
			(struct ReplicationState *) &fake_session2;
		ok = ok && *PgCurrentLogicalRepRelMapContextRef() ==
			(MemoryContext) &fake_session2;
		ok = ok && *PgCurrentLogicalRepRelMapRef() ==
			(HTAB *) &fake_session2;
		ok = ok && *PgCurrentLogicalRepPartMapContextRef() ==
			(MemoryContext) &fake_session2;
		ok = ok && *PgCurrentLogicalRepPartMapRef() ==
			(HTAB *) &fake_session2;
		ok = ok && *PgCurrentPgOutputPublicationsValidRef();
		ok = ok && *PgCurrentPgOutputRelationSyncCacheRef() ==
			(HTAB *) &fake_session2;
		ok = ok && *PgCurrentLogicalRepSyncingRelationsStateRef() == 23;

		PgSetCurrentSession(saved_session);
		DateStyle = saved_date_style;
		DateOrder = saved_date_order;
		SetConfigOption("IntervalStyle", saved_interval_style,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("TimeZone", saved_timezone,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_timezone", saved_log_timezone,
						PGC_SIGHUP, PGC_S_FILE);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		DateStyle = saved_date_style;
		DateOrder = saved_date_order;
		SetConfigOption("IntervalStyle", saved_interval_style,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("TimeZone", saved_timezone,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_timezone", saved_log_timezone,
						PGC_SIGHUP, PGC_S_FILE);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session date/time GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_text_search_state_is_session_local);
Datum
test_session_text_search_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_text_search_config;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_text_search_config =
		pstrdup(GetConfigOption("default_text_search_config", false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && strcmp(TSCurrentConfig, "pg_catalog.simple") == 0;
		ok = ok && !OidIsValid(*PgCurrentTSCurrentConfigCacheRef());
		SetConfigOption("default_text_search_config", "pg_catalog.english",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && strcmp(TSCurrentConfig, "pg_catalog.english") == 0;
		ok = ok && !OidIsValid(*PgCurrentTSCurrentConfigCacheRef());
		*PgCurrentTSCurrentConfigCacheRef() = 12345;
		*PgCurrentTSParserCacheHashRef() = (HTAB *) &fake_session1;
		*PgCurrentTSLastUsedParserRef() =
			(TSParserCacheEntry *) &fake_session1;
		*PgCurrentTSDictionaryCacheHashRef() = (HTAB *) &fake_session1;
		*PgCurrentTSLastUsedDictionaryRef() =
			(TSDictionaryCacheEntry *) &fake_session1;
		*PgCurrentTSConfigCacheHashRef() = (HTAB *) &fake_session1;
		*PgCurrentTSLastUsedConfigRef() =
			(TSConfigCacheEntry *) &fake_session1;

		PgSetCurrentSession(&fake_session2);
		ok = ok && strcmp(TSCurrentConfig, "pg_catalog.simple") == 0;
		ok = ok && !OidIsValid(*PgCurrentTSCurrentConfigCacheRef());
		ok = ok && *PgCurrentTSParserCacheHashRef() == NULL;
		ok = ok && *PgCurrentTSLastUsedParserRef() == NULL;
		ok = ok && *PgCurrentTSDictionaryCacheHashRef() == NULL;
		ok = ok && *PgCurrentTSLastUsedDictionaryRef() == NULL;
		ok = ok && *PgCurrentTSConfigCacheHashRef() == NULL;
		ok = ok && *PgCurrentTSLastUsedConfigRef() == NULL;
		SetConfigOption("default_text_search_config", "pg_catalog.simple",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && strcmp(TSCurrentConfig, "pg_catalog.simple") == 0;
		*PgCurrentTSCurrentConfigCacheRef() = 67890;
		*PgCurrentTSParserCacheHashRef() = (HTAB *) &fake_session2;
		*PgCurrentTSLastUsedParserRef() =
			(TSParserCacheEntry *) &fake_session2;
		*PgCurrentTSDictionaryCacheHashRef() = (HTAB *) &fake_session2;
		*PgCurrentTSLastUsedDictionaryRef() =
			(TSDictionaryCacheEntry *) &fake_session2;
		*PgCurrentTSConfigCacheHashRef() = (HTAB *) &fake_session2;
		*PgCurrentTSLastUsedConfigRef() =
			(TSConfigCacheEntry *) &fake_session2;

		PgSetCurrentSession(&fake_session1);
		ok = ok && strcmp(TSCurrentConfig, "pg_catalog.english") == 0;
		ok = ok && *PgCurrentTSCurrentConfigCacheRef() == 12345;
		ok = ok && *PgCurrentTSParserCacheHashRef() ==
			(HTAB *) &fake_session1;
		ok = ok && *PgCurrentTSLastUsedParserRef() ==
			(TSParserCacheEntry *) &fake_session1;
		ok = ok && *PgCurrentTSDictionaryCacheHashRef() ==
			(HTAB *) &fake_session1;
		ok = ok && *PgCurrentTSLastUsedDictionaryRef() ==
			(TSDictionaryCacheEntry *) &fake_session1;
		ok = ok && *PgCurrentTSConfigCacheHashRef() ==
			(HTAB *) &fake_session1;
		ok = ok && *PgCurrentTSLastUsedConfigRef() ==
			(TSConfigCacheEntry *) &fake_session1;

		PgSetCurrentSession(&fake_session2);
		ok = ok && strcmp(TSCurrentConfig, "pg_catalog.simple") == 0;
		ok = ok && *PgCurrentTSCurrentConfigCacheRef() == 67890;
		ok = ok && *PgCurrentTSParserCacheHashRef() ==
			(HTAB *) &fake_session2;
		ok = ok && *PgCurrentTSLastUsedParserRef() ==
			(TSParserCacheEntry *) &fake_session2;
		ok = ok && *PgCurrentTSDictionaryCacheHashRef() ==
			(HTAB *) &fake_session2;
		ok = ok && *PgCurrentTSLastUsedDictionaryRef() ==
			(TSDictionaryCacheEntry *) &fake_session2;
		ok = ok && *PgCurrentTSConfigCacheHashRef() ==
			(HTAB *) &fake_session2;
		ok = ok && *PgCurrentTSLastUsedConfigRef() ==
			(TSConfigCacheEntry *) &fake_session2;

		PgSetCurrentSession(saved_session);
		SetConfigOption("default_text_search_config",
						saved_text_search_config,
						PGC_USERSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("default_text_search_config",
						saved_text_search_config,
						PGC_USERSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session text-search state was not session-local");

PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_on_commit_state_is_session_local);
Datum
test_session_on_commit_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	List	   *saved_on_commits;
	List	   *session1_marker;
	List	   *session2_marker;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_on_commits = *PgCurrentOnCommitActionsRef();
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);
	session1_marker = (List *) &fake_session1;
	session2_marker = (List *) &fake_session2;

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentOnCommitActionsRef() == NIL;
		*PgCurrentOnCommitActionsRef() = session1_marker;
		ok = ok && *PgCurrentOnCommitActionsRef() == session1_marker;

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentOnCommitActionsRef() == NIL;
		*PgCurrentOnCommitActionsRef() = session2_marker;
		ok = ok && *PgCurrentOnCommitActionsRef() == session2_marker;

		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentOnCommitActionsRef() == session1_marker;

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentOnCommitActionsRef() == session2_marker;

		PgSetCurrentSession(saved_session);
		*PgCurrentOnCommitActionsRef() = saved_on_commits;
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		*PgCurrentOnCommitActionsRef() = saved_on_commits;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "ON COMMIT state was not session-local");

PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_sequence_state_is_session_local);
Datum
test_session_sequence_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	HTAB	   *saved_seqhashtab;
	struct SeqTableData *saved_last_used_seq;
	HTAB	   *session1_hash_marker;
	HTAB	   *session2_hash_marker;
	struct SeqTableData *session1_last_marker;
	struct SeqTableData *session2_last_marker;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_seqhashtab = *PgCurrentSequenceHashTableRef();
	saved_last_used_seq = *PgCurrentLastUsedSequenceRef();
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);
	fake_session1.catalog_lookup.typcache_tupledesc_id_counter = (uint64) 1;
	fake_session2.catalog_lookup.typcache_tupledesc_id_counter = (uint64) 1;
	session1_hash_marker = (HTAB *) &fake_session1;
	session2_hash_marker = (HTAB *) &fake_session2;
	session1_last_marker = (struct SeqTableData *) &fake_session1;
	session2_last_marker = (struct SeqTableData *) &fake_session2;

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentSequenceHashTableRef() == NULL;
		ok = ok && *PgCurrentLastUsedSequenceRef() == NULL;
		*PgCurrentSequenceHashTableRef() = session1_hash_marker;
		*PgCurrentLastUsedSequenceRef() = session1_last_marker;
		ok = ok && *PgCurrentSequenceHashTableRef() == session1_hash_marker;
		ok = ok && *PgCurrentLastUsedSequenceRef() == session1_last_marker;

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentSequenceHashTableRef() == NULL;
		ok = ok && *PgCurrentLastUsedSequenceRef() == NULL;
		*PgCurrentSequenceHashTableRef() = session2_hash_marker;
		*PgCurrentLastUsedSequenceRef() = session2_last_marker;
		ok = ok && *PgCurrentSequenceHashTableRef() == session2_hash_marker;
		ok = ok && *PgCurrentLastUsedSequenceRef() == session2_last_marker;

		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentSequenceHashTableRef() == session1_hash_marker;
		ok = ok && *PgCurrentLastUsedSequenceRef() == session1_last_marker;

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentSequenceHashTableRef() == session2_hash_marker;
		ok = ok && *PgCurrentLastUsedSequenceRef() == session2_last_marker;

		PgSetCurrentSession(saved_session);
		*PgCurrentSequenceHashTableRef() = saved_seqhashtab;
		*PgCurrentLastUsedSequenceRef() = saved_last_used_seq;
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		*PgCurrentSequenceHashTableRef() = saved_seqhashtab;
		*PgCurrentLastUsedSequenceRef() = saved_last_used_seq;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "sequence state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_large_object_state_is_session_local);
Datum
test_session_large_object_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	Relation	saved_heap_relation;
	Relation	saved_index_relation;
	Relation	session1_heap_marker;
	Relation	session1_index_marker;
	Relation	session2_heap_marker;
	Relation	session2_index_marker;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_heap_relation = *PgCurrentLargeObjectHeapRelationRef();
	saved_index_relation = *PgCurrentLargeObjectIndexRelationRef();
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);
	session1_heap_marker = (Relation) &fake_session1;
	session1_index_marker = (Relation) &fake_session2;
	session2_heap_marker = (Relation) &saved_session;
	session2_index_marker = (Relation) &saved_heap_relation;

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentLargeObjectHeapRelationRef() == NULL;
		ok = ok && *PgCurrentLargeObjectIndexRelationRef() == NULL;
		*PgCurrentLargeObjectHeapRelationRef() = session1_heap_marker;
		*PgCurrentLargeObjectIndexRelationRef() = session1_index_marker;
		ok = ok && *PgCurrentLargeObjectHeapRelationRef() == session1_heap_marker;
		ok = ok && *PgCurrentLargeObjectIndexRelationRef() == session1_index_marker;

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentLargeObjectHeapRelationRef() == NULL;
		ok = ok && *PgCurrentLargeObjectIndexRelationRef() == NULL;
		*PgCurrentLargeObjectHeapRelationRef() = session2_heap_marker;
		*PgCurrentLargeObjectIndexRelationRef() = session2_index_marker;
		ok = ok && *PgCurrentLargeObjectHeapRelationRef() == session2_heap_marker;
		ok = ok && *PgCurrentLargeObjectIndexRelationRef() == session2_index_marker;

		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentLargeObjectHeapRelationRef() == session1_heap_marker;
		ok = ok && *PgCurrentLargeObjectIndexRelationRef() == session1_index_marker;

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentLargeObjectHeapRelationRef() == session2_heap_marker;
		ok = ok && *PgCurrentLargeObjectIndexRelationRef() == session2_index_marker;

		PgSetCurrentSession(saved_session);
		*PgCurrentLargeObjectHeapRelationRef() = saved_heap_relation;
		*PgCurrentLargeObjectIndexRelationRef() = saved_index_relation;
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		*PgCurrentLargeObjectHeapRelationRef() = saved_heap_relation;
		*PgCurrentLargeObjectIndexRelationRef() = saved_index_relation;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "large-object state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_regex_portal_state_is_session_local);
Datum
test_session_regex_portal_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	MemoryContext saved_regex_context;
	int			saved_regex_count;
	MemoryContext saved_portal_context;
	HTAB	   *saved_portal_hash;
	unsigned int saved_unnamed_count;
	MemoryContext session1_regex_context;
	MemoryContext session2_regex_context;
	MemoryContext session1_portal_context;
	MemoryContext session2_portal_context;
	HTAB	   *session1_portal_hash;
	HTAB	   *session2_portal_hash;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_regex_context = *PgCurrentRegexpCacheMemoryContextRef();
	saved_regex_count = *PgCurrentRegexpNumCachedResRef();
	saved_portal_context = *PgCurrentTopPortalContextRef();
	saved_portal_hash = *PgCurrentPortalHashTableRef();
	saved_unnamed_count = *PgCurrentUnnamedPortalCountRef();
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);
	session1_regex_context = (MemoryContext) &fake_session1;
	session2_regex_context = (MemoryContext) &fake_session2;
	session1_portal_context = (MemoryContext) &fake_session1.portal_manager;
	session2_portal_context = (MemoryContext) &fake_session2.portal_manager;
	session1_portal_hash = (HTAB *) &fake_session1;
	session2_portal_hash = (HTAB *) &fake_session2;

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentRegexpCacheMemoryContextRef() == NULL;
		ok = ok && *PgCurrentRegexpNumCachedResRef() == 0;
		ok = ok && PgCurrentRegexpCachedResArray() ==
			fake_session1.regex.cached_res;
		ok = ok && *PgCurrentTopPortalContextRef() == NULL;
		ok = ok && *PgCurrentPortalHashTableRef() == NULL;
		ok = ok && *PgCurrentUnnamedPortalCountRef() == 0;
		*PgCurrentRegexpCacheMemoryContextRef() = session1_regex_context;
		*PgCurrentRegexpNumCachedResRef() = 7;
		*PgCurrentTopPortalContextRef() = session1_portal_context;
		*PgCurrentPortalHashTableRef() = session1_portal_hash;
		*PgCurrentUnnamedPortalCountRef() = 11;

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentRegexpCacheMemoryContextRef() == NULL;
		ok = ok && *PgCurrentRegexpNumCachedResRef() == 0;
		ok = ok && PgCurrentRegexpCachedResArray() ==
			fake_session2.regex.cached_res;
		ok = ok && *PgCurrentTopPortalContextRef() == NULL;
		ok = ok && *PgCurrentPortalHashTableRef() == NULL;
		ok = ok && *PgCurrentUnnamedPortalCountRef() == 0;
		*PgCurrentRegexpCacheMemoryContextRef() = session2_regex_context;
		*PgCurrentRegexpNumCachedResRef() = 13;
		*PgCurrentTopPortalContextRef() = session2_portal_context;
		*PgCurrentPortalHashTableRef() = session2_portal_hash;
		*PgCurrentUnnamedPortalCountRef() = 17;

		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentRegexpCacheMemoryContextRef() ==
			session1_regex_context;
		ok = ok && *PgCurrentRegexpNumCachedResRef() == 7;
		ok = ok && *PgCurrentTopPortalContextRef() == session1_portal_context;
		ok = ok && *PgCurrentPortalHashTableRef() == session1_portal_hash;
		ok = ok && *PgCurrentUnnamedPortalCountRef() == 11;

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentRegexpCacheMemoryContextRef() ==
			session2_regex_context;
		ok = ok && *PgCurrentRegexpNumCachedResRef() == 13;
		ok = ok && *PgCurrentTopPortalContextRef() == session2_portal_context;
		ok = ok && *PgCurrentPortalHashTableRef() == session2_portal_hash;
		ok = ok && *PgCurrentUnnamedPortalCountRef() == 17;

		PgSetCurrentSession(saved_session);
		*PgCurrentRegexpCacheMemoryContextRef() = saved_regex_context;
		*PgCurrentRegexpNumCachedResRef() = saved_regex_count;
		*PgCurrentTopPortalContextRef() = saved_portal_context;
		*PgCurrentPortalHashTableRef() = saved_portal_hash;
		*PgCurrentUnnamedPortalCountRef() = saved_unnamed_count;
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		*PgCurrentRegexpCacheMemoryContextRef() = saved_regex_context;
		*PgCurrentRegexpNumCachedResRef() = saved_regex_count;
		*PgCurrentTopPortalContextRef() = saved_portal_context;
		*PgCurrentPortalHashTableRef() = saved_portal_hash;
		*PgCurrentUnnamedPortalCountRef() = saved_unnamed_count;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "regex/portal manager state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_async_state_is_session_local);
Datum
test_session_async_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	HTAB	   *saved_local_channel_table;
	bool		saved_registered_listener;
	HTAB	   *session1_table_marker;
	HTAB	   *session2_table_marker;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_local_channel_table = *PgCurrentAsyncLocalChannelTableRef();
	saved_registered_listener = *PgCurrentAsyncRegisteredListenerRef();
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);
	session1_table_marker = (HTAB *) &fake_session1;
	session2_table_marker = (HTAB *) &fake_session2;

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentAsyncLocalChannelTableRef() == NULL;
		ok = ok && !*PgCurrentAsyncRegisteredListenerRef();
		*PgCurrentAsyncLocalChannelTableRef() = session1_table_marker;
		*PgCurrentAsyncRegisteredListenerRef() = true;
		ok = ok && *PgCurrentAsyncLocalChannelTableRef() == session1_table_marker;
		ok = ok && *PgCurrentAsyncRegisteredListenerRef();

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentAsyncLocalChannelTableRef() == NULL;
		ok = ok && !*PgCurrentAsyncRegisteredListenerRef();
		*PgCurrentAsyncLocalChannelTableRef() = session2_table_marker;
		*PgCurrentAsyncRegisteredListenerRef() = false;
		ok = ok && *PgCurrentAsyncLocalChannelTableRef() == session2_table_marker;
		ok = ok && !*PgCurrentAsyncRegisteredListenerRef();

		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentAsyncLocalChannelTableRef() == session1_table_marker;
		ok = ok && *PgCurrentAsyncRegisteredListenerRef();

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentAsyncLocalChannelTableRef() == session2_table_marker;
		ok = ok && !*PgCurrentAsyncRegisteredListenerRef();

		PgSetCurrentSession(saved_session);
		*PgCurrentAsyncLocalChannelTableRef() = saved_local_channel_table;
		*PgCurrentAsyncRegisteredListenerRef() = saved_registered_listener;
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		*PgCurrentAsyncLocalChannelTableRef() = saved_local_channel_table;
		*PgCurrentAsyncRegisteredListenerRef() = saved_registered_listener;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "async listener state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_encoding_state_is_session_local);
Datum
test_session_encoding_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	List	   *saved_conv_proc_list;
	FmgrInfo   *saved_to_server_conv_proc;
	FmgrInfo   *saved_to_client_conv_proc;
	FmgrInfo   *saved_utf8_to_server_conv_proc;
	const pg_enc2name *saved_client_encoding;
	const pg_enc2name *saved_database_encoding;
	const pg_enc2name *saved_message_encoding;
	bool		saved_startup_complete;
	int			saved_pending_client_encoding;
	List	   *session1_list_marker;
	List	   *session2_list_marker;
	MemoryContext session1_encoding_context = NULL;
	MemoryContext session2_encoding_context = NULL;
	void	   *session1_context_marker;
	void	   *session2_context_marker;
	FmgrInfo   *session1_to_server_marker;
	FmgrInfo   *session1_to_client_marker;
	FmgrInfo   *session1_utf8_marker;
	FmgrInfo   *session2_to_server_marker;
	FmgrInfo   *session2_to_client_marker;
	FmgrInfo   *session2_utf8_marker;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_conv_proc_list = *PgCurrentEncodingConvProcListRef();
	saved_to_server_conv_proc = *PgCurrentToServerConvProcRef();
	saved_to_client_conv_proc = *PgCurrentToClientConvProcRef();
	saved_utf8_to_server_conv_proc = *PgCurrentUtf8ToServerConvProcRef();
	saved_client_encoding = *PgCurrentClientEncodingRef();
	saved_database_encoding = *PgCurrentDatabaseEncodingRef();
	saved_message_encoding = *PgCurrentMessageEncodingRef();
	saved_startup_complete = *PgCurrentEncodingStartupCompleteRef();
	saved_pending_client_encoding = *PgCurrentPendingClientEncodingRef();
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);
	session1_list_marker = (List *) &fake_session1;
	session2_list_marker = (List *) &fake_session2;
	session1_to_server_marker = (FmgrInfo *) &fake_session1;
	session1_to_client_marker = (FmgrInfo *) &fake_session2;
	session1_utf8_marker = (FmgrInfo *) &saved_session;
	session2_to_server_marker = (FmgrInfo *) &saved_conv_proc_list;
	session2_to_client_marker = (FmgrInfo *) &saved_to_server_conv_proc;
	session2_utf8_marker = (FmgrInfo *) &saved_to_client_conv_proc;

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentEncodingConvProcListRef() == NIL;
		ok = ok && fake_session1.encoding.encoding_cache_context == NULL;
		ok = ok && *PgCurrentToServerConvProcRef() == NULL;
		ok = ok && *PgCurrentToClientConvProcRef() == NULL;
		ok = ok && *PgCurrentUtf8ToServerConvProcRef() == NULL;
		ok = ok && *PgCurrentClientEncodingRef() == &pg_enc2name_tbl[PG_SQL_ASCII];
		ok = ok && *PgCurrentDatabaseEncodingRef() == &pg_enc2name_tbl[PG_SQL_ASCII];
		ok = ok && *PgCurrentMessageEncodingRef() == &pg_enc2name_tbl[PG_SQL_ASCII];
		ok = ok && !*PgCurrentEncodingStartupCompleteRef();
		ok = ok && *PgCurrentPendingClientEncodingRef() == PG_SQL_ASCII;
		*PgCurrentEncodingConvProcListRef() = session1_list_marker;
		*PgCurrentToServerConvProcRef() = session1_to_server_marker;
		*PgCurrentToClientConvProcRef() = session1_to_client_marker;
		*PgCurrentUtf8ToServerConvProcRef() = session1_utf8_marker;
		*PgCurrentClientEncodingRef() = &pg_enc2name_tbl[PG_UTF8];
		*PgCurrentDatabaseEncodingRef() = &pg_enc2name_tbl[PG_UTF8];
		*PgCurrentMessageEncodingRef() = &pg_enc2name_tbl[PG_UTF8];
		*PgCurrentEncodingStartupCompleteRef() = true;
		*PgCurrentPendingClientEncodingRef() = PG_UTF8;

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentEncodingConvProcListRef() == NIL;
		ok = ok && fake_session2.encoding.encoding_cache_context == NULL;
		ok = ok && *PgCurrentToServerConvProcRef() == NULL;
		ok = ok && *PgCurrentToClientConvProcRef() == NULL;
		ok = ok && *PgCurrentUtf8ToServerConvProcRef() == NULL;
		ok = ok && *PgCurrentClientEncodingRef() == &pg_enc2name_tbl[PG_SQL_ASCII];
		ok = ok && *PgCurrentDatabaseEncodingRef() == &pg_enc2name_tbl[PG_SQL_ASCII];
		ok = ok && *PgCurrentMessageEncodingRef() == &pg_enc2name_tbl[PG_SQL_ASCII];
		ok = ok && !*PgCurrentEncodingStartupCompleteRef();
		ok = ok && *PgCurrentPendingClientEncodingRef() == PG_SQL_ASCII;
		*PgCurrentEncodingConvProcListRef() = session2_list_marker;
		*PgCurrentToServerConvProcRef() = session2_to_server_marker;
		*PgCurrentToClientConvProcRef() = session2_to_client_marker;
		*PgCurrentUtf8ToServerConvProcRef() = session2_utf8_marker;
		*PgCurrentClientEncodingRef() = &pg_enc2name_tbl[PG_SQL_ASCII];
		*PgCurrentDatabaseEncodingRef() = &pg_enc2name_tbl[PG_SQL_ASCII];
		*PgCurrentMessageEncodingRef() = &pg_enc2name_tbl[PG_SQL_ASCII];
		*PgCurrentEncodingStartupCompleteRef() = false;
		*PgCurrentPendingClientEncodingRef() = PG_SQL_ASCII;

		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentEncodingConvProcListRef() == session1_list_marker;
		session1_encoding_context = PgCurrentEncodingCacheMemoryContext();
		session1_context_marker =
			MemoryContextAlloc(session1_encoding_context, 8);
		ok = ok && session1_encoding_context != TopMemoryContext;
		ok = ok && MemoryContextGetParent(session1_encoding_context) ==
			TopMemoryContext;
		ok = ok && GetMemoryChunkContext(session1_context_marker) ==
			session1_encoding_context;
		ok = ok && *PgCurrentToServerConvProcRef() == session1_to_server_marker;
		ok = ok && *PgCurrentToClientConvProcRef() == session1_to_client_marker;
		ok = ok && *PgCurrentUtf8ToServerConvProcRef() == session1_utf8_marker;
		ok = ok && *PgCurrentClientEncodingRef() == &pg_enc2name_tbl[PG_UTF8];
		ok = ok && *PgCurrentDatabaseEncodingRef() == &pg_enc2name_tbl[PG_UTF8];
		ok = ok && *PgCurrentMessageEncodingRef() == &pg_enc2name_tbl[PG_UTF8];
		ok = ok && *PgCurrentEncodingStartupCompleteRef();
		ok = ok && *PgCurrentPendingClientEncodingRef() == PG_UTF8;

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentEncodingConvProcListRef() == session2_list_marker;
		session2_encoding_context = PgCurrentEncodingCacheMemoryContext();
		session2_context_marker =
			MemoryContextAlloc(session2_encoding_context, 8);
		ok = ok && session2_encoding_context != session1_encoding_context;
		ok = ok && MemoryContextGetParent(session2_encoding_context) ==
			TopMemoryContext;
		ok = ok && GetMemoryChunkContext(session2_context_marker) ==
			session2_encoding_context;
		ok = ok && *PgCurrentToServerConvProcRef() == session2_to_server_marker;
		ok = ok && *PgCurrentToClientConvProcRef() == session2_to_client_marker;
		ok = ok && *PgCurrentUtf8ToServerConvProcRef() == session2_utf8_marker;
		ok = ok && *PgCurrentClientEncodingRef() == &pg_enc2name_tbl[PG_SQL_ASCII];
		ok = ok && *PgCurrentDatabaseEncodingRef() == &pg_enc2name_tbl[PG_SQL_ASCII];
		ok = ok && *PgCurrentMessageEncodingRef() == &pg_enc2name_tbl[PG_SQL_ASCII];
		ok = ok && !*PgCurrentEncodingStartupCompleteRef();
		ok = ok && *PgCurrentPendingClientEncodingRef() == PG_SQL_ASCII;

		PgSetCurrentSession(saved_session);
		*PgCurrentEncodingConvProcListRef() = saved_conv_proc_list;
		*PgCurrentToServerConvProcRef() = saved_to_server_conv_proc;
		*PgCurrentToClientConvProcRef() = saved_to_client_conv_proc;
		*PgCurrentUtf8ToServerConvProcRef() = saved_utf8_to_server_conv_proc;
		*PgCurrentClientEncodingRef() = saved_client_encoding;
		*PgCurrentDatabaseEncodingRef() = saved_database_encoding;
		*PgCurrentMessageEncodingRef() = saved_message_encoding;
		*PgCurrentEncodingStartupCompleteRef() = saved_startup_complete;
		*PgCurrentPendingClientEncodingRef() = saved_pending_client_encoding;
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		if (session1_encoding_context != NULL)
			MemoryContextDelete(session1_encoding_context);
		if (session2_encoding_context != NULL)
			MemoryContextDelete(session2_encoding_context);
		*PgCurrentEncodingConvProcListRef() = saved_conv_proc_list;
		*PgCurrentToServerConvProcRef() = saved_to_server_conv_proc;
		*PgCurrentToClientConvProcRef() = saved_to_client_conv_proc;
		*PgCurrentUtf8ToServerConvProcRef() = saved_utf8_to_server_conv_proc;
		*PgCurrentClientEncodingRef() = saved_client_encoding;
		*PgCurrentDatabaseEncodingRef() = saved_database_encoding;
		*PgCurrentMessageEncodingRef() = saved_message_encoding;
		*PgCurrentEncodingStartupCompleteRef() = saved_startup_complete;
		*PgCurrentPendingClientEncodingRef() = saved_pending_client_encoding;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (session1_encoding_context != NULL)
		MemoryContextDelete(session1_encoding_context);
	if (session2_encoding_context != NULL)
		MemoryContextDelete(session2_encoding_context);

	if (!ok)
		elog(ERROR, "encoding state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_temp_file_state_is_session_local);
Datum
test_session_temp_file_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	uint64		saved_temporary_files_size;
	long		saved_temp_file_counter;
	Oid		   *saved_temp_table_spaces;
	int			saved_num_temp_table_spaces;
	int			saved_next_temp_table_space;
	Oid			session1_table_spaces[2] = {1111, 2222};
	Oid			session2_table_spaces[1] = {3333};
	Oid			copied_table_spaces[2];
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_temporary_files_size = *PgCurrentTemporaryFilesSizeRef();
	saved_temp_file_counter = *PgCurrentTempFileCounterRef();
	saved_temp_table_spaces = *PgCurrentTempTableSpaceOidsRef();
	saved_num_temp_table_spaces = *PgCurrentNumTempTableSpacesRef();
	saved_next_temp_table_space = *PgCurrentNextTempTableSpaceRef();
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentTemporaryFilesSizeRef() == 0;
		ok = ok && *PgCurrentTempFileCounterRef() == 0;
		ok = ok && *PgCurrentTempTableSpaceOidsRef() == NULL;
		ok = ok && TempTablespacesAreSet();
		ok = ok && *PgCurrentNumTempTableSpacesRef() == 0;
		ok = ok && *PgCurrentNextTempTableSpaceRef() == 0;
		*PgCurrentTemporaryFilesSizeRef() = 1234;
		*PgCurrentTempFileCounterRef() = 42;
		SetTempTablespaces(session1_table_spaces, lengthof(session1_table_spaces));
		ok = ok && TempTablespacesAreSet();
		ok = ok && GetTempTablespaces(copied_table_spaces,
									  lengthof(copied_table_spaces)) == 2;
		ok = ok && copied_table_spaces[0] == session1_table_spaces[0];
		ok = ok && copied_table_spaces[1] == session1_table_spaces[1];

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentTemporaryFilesSizeRef() == 0;
		ok = ok && *PgCurrentTempFileCounterRef() == 0;
		ok = ok && *PgCurrentTempTableSpaceOidsRef() == NULL;
		ok = ok && TempTablespacesAreSet();
		ok = ok && *PgCurrentNumTempTableSpacesRef() == 0;
		ok = ok && *PgCurrentNextTempTableSpaceRef() == 0;
		*PgCurrentTemporaryFilesSizeRef() = 9876;
		*PgCurrentTempFileCounterRef() = 84;
		SetTempTablespaces(session2_table_spaces, lengthof(session2_table_spaces));
		ok = ok && TempTablespacesAreSet();
		ok = ok && GetTempTablespaces(copied_table_spaces,
									  lengthof(copied_table_spaces)) == 1;
		ok = ok && copied_table_spaces[0] == session2_table_spaces[0];

		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentTemporaryFilesSizeRef() == 1234;
		ok = ok && *PgCurrentTempFileCounterRef() == 42;
		ok = ok && TempTablespacesAreSet();
		ok = ok && GetTempTablespaces(copied_table_spaces,
									  lengthof(copied_table_spaces)) == 2;
		ok = ok && copied_table_spaces[0] == session1_table_spaces[0];
		ok = ok && copied_table_spaces[1] == session1_table_spaces[1];

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentTemporaryFilesSizeRef() == 9876;
		ok = ok && *PgCurrentTempFileCounterRef() == 84;
		ok = ok && TempTablespacesAreSet();
		ok = ok && GetTempTablespaces(copied_table_spaces,
									  lengthof(copied_table_spaces)) == 1;
		ok = ok && copied_table_spaces[0] == session2_table_spaces[0];

		PgSetCurrentSession(saved_session);
		*PgCurrentTemporaryFilesSizeRef() = saved_temporary_files_size;
		*PgCurrentTempFileCounterRef() = saved_temp_file_counter;
		*PgCurrentTempTableSpaceOidsRef() = saved_temp_table_spaces;
		*PgCurrentNumTempTableSpacesRef() = saved_num_temp_table_spaces;
		*PgCurrentNextTempTableSpaceRef() = saved_next_temp_table_space;
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		*PgCurrentTemporaryFilesSizeRef() = saved_temporary_files_size;
		*PgCurrentTempFileCounterRef() = saved_temp_file_counter;
		*PgCurrentTempTableSpaceOidsRef() = saved_temp_table_spaces;
		*PgCurrentNumTempTableSpacesRef() = saved_num_temp_table_spaces;
		*PgCurrentNextTempTableSpaceRef() = saved_next_temp_table_space;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "temporary file state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_random_state_is_session_local);
Datum
test_session_random_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	pg_prng_state expected1;
	pg_prng_state expected2;
	float8		expected1_first;
	float8		expected1_second;
	float8		expected2_first;
	float8		expected2_second;
	float8		session1_first = 0;
	float8		session1_second = 0;
	float8		session2_first = 0;
	float8		session2_second = 0;
	bool		ok = true;

	saved_session = CurrentPgSession;
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	pg_prng_fseed(&expected1, 0.25);
	expected1_first = pg_prng_double(&expected1);
	expected1_second = pg_prng_double(&expected1);
	pg_prng_fseed(&expected2, -0.5);
	expected2_first = pg_prng_double(&expected2);
	expected2_second = pg_prng_double(&expected2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && !*PgCurrentPseudoRandomSeedSetRef();
		DirectFunctionCall1(setseed, Float8GetDatum(0.25));
		ok = ok && *PgCurrentPseudoRandomSeedSetRef();
		session1_first = DatumGetFloat8(OidFunctionCall0(F_RANDOM_));

		PgSetCurrentSession(&fake_session2);
		ok = ok && !*PgCurrentPseudoRandomSeedSetRef();
		DirectFunctionCall1(setseed, Float8GetDatum(-0.5));
		ok = ok && *PgCurrentPseudoRandomSeedSetRef();
		session2_first = DatumGetFloat8(OidFunctionCall0(F_RANDOM_));

		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentPseudoRandomSeedSetRef();
		session1_second = DatumGetFloat8(OidFunctionCall0(F_RANDOM_));

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentPseudoRandomSeedSetRef();
		session2_second = DatumGetFloat8(OidFunctionCall0(F_RANDOM_));

		PgSetCurrentSession(saved_session);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		PG_RE_THROW();
	}
	PG_END_TRY();

	ok = ok && session1_first == expected1_first;
	ok = ok && session1_second == expected1_second;
	ok = ok && session2_first == expected2_first;
	ok = ok && session2_second == expected2_second;

	if (!ok)
		elog(ERROR, "random state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_optimizer_state_is_session_local);
Datum
test_session_optimizer_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	HTAB	   *session1_proof_marker;
	HTAB	   *session2_proof_marker;
	bool		ok = true;

	saved_session = CurrentPgSession;
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);
	session1_proof_marker = (HTAB *) &fake_session1;
	session2_proof_marker = (HTAB *) &fake_session2;

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentPlannerExtensionNameArrayRef() == NULL;
		ok = ok && *PgCurrentPlannerExtensionNamesAssignedRef() == 0;
		ok = ok && *PgCurrentPlannerExtensionNamesAllocatedRef() == 0;
		ok = ok && GetPlannerExtensionId("phase12_optimizer_a") == 0;
		ok = ok && GetPlannerExtensionId("phase12_optimizer_b") == 1;
		ok = ok && GetPlannerExtensionId("phase12_optimizer_a") == 0;
		ok = ok && *PgCurrentPlannerExtensionNamesAssignedRef() == 2;
		ok = ok && *PgCurrentOprProofCacheHashRef() == NULL;
		*PgCurrentOprProofCacheHashRef() = session1_proof_marker;

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentPlannerExtensionNameArrayRef() == NULL;
		ok = ok && *PgCurrentPlannerExtensionNamesAssignedRef() == 0;
		ok = ok && *PgCurrentPlannerExtensionNamesAllocatedRef() == 0;
		ok = ok && GetPlannerExtensionId("phase12_optimizer_b") == 0;
		ok = ok && *PgCurrentPlannerExtensionNamesAssignedRef() == 1;
		ok = ok && *PgCurrentOprProofCacheHashRef() == NULL;
		*PgCurrentOprProofCacheHashRef() = session2_proof_marker;

		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentPlannerExtensionNamesAssignedRef() == 2;
		ok = ok && *PgCurrentOprProofCacheHashRef() == session1_proof_marker;

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentPlannerExtensionNamesAssignedRef() == 1;
		ok = ok && *PgCurrentOprProofCacheHashRef() == session2_proof_marker;

		PgSetCurrentSession(saved_session);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "optimizer state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_plan_cache_state_is_session_local);
Datum
test_session_plan_cache_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	dlist_node	session1_saved_node;
	dlist_node	session1_expr_node;
	dlist_node	session2_saved_node;
	dlist_node	session2_expr_node;
	bool		ok = true;

	saved_session = CurrentPgSession;
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);
	dlist_node_init(&session1_saved_node);
	dlist_node_init(&session1_expr_node);
	dlist_node_init(&session2_saved_node);
	dlist_node_init(&session2_expr_node);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && dlist_is_empty(PgCurrentSavedPlanListRef());
		ok = ok && dlist_is_empty(PgCurrentCachedExpressionListRef());
		dlist_push_tail(PgCurrentSavedPlanListRef(), &session1_saved_node);
		dlist_push_tail(PgCurrentCachedExpressionListRef(), &session1_expr_node);
		ok = ok && !dlist_is_empty(PgCurrentSavedPlanListRef());
		ok = ok && !dlist_is_empty(PgCurrentCachedExpressionListRef());

		PgSetCurrentSession(&fake_session2);
		ok = ok && dlist_is_empty(PgCurrentSavedPlanListRef());
		ok = ok && dlist_is_empty(PgCurrentCachedExpressionListRef());
		dlist_push_tail(PgCurrentSavedPlanListRef(), &session2_saved_node);
		dlist_push_tail(PgCurrentCachedExpressionListRef(), &session2_expr_node);
		ok = ok && !dlist_is_empty(PgCurrentSavedPlanListRef());
		ok = ok && !dlist_is_empty(PgCurrentCachedExpressionListRef());

		PgSetCurrentSession(&fake_session1);
		ok = ok && PgCurrentSavedPlanListRef()->head.next == &session1_saved_node;
		ok = ok && PgCurrentCachedExpressionListRef()->head.next == &session1_expr_node;

		PgSetCurrentSession(&fake_session2);
		ok = ok && PgCurrentSavedPlanListRef()->head.next == &session2_saved_node;
		ok = ok && PgCurrentCachedExpressionListRef()->head.next == &session2_expr_node;

		PgSetCurrentSession(saved_session);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "plan cache state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_namespace_state_is_session_local);
Datum
test_session_namespace_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	PgSessionNamespaceState *namespace_state;
	List	   *session1_path;
	List	   *session2_path;
	bool		ok = true;

	saved_session = CurrentPgSession;
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);
	session1_path = list_make2_oid(11, 12);
	session2_path = list_make1_oid(21);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		namespace_state = PgCurrentNamespaceState();
		ok = ok && namespace_state->initialized;
		ok = ok && namespace_state->active_path_generation == 1;
		ok = ok && !namespace_state->base_search_path_valid;
		ok = ok && namespace_state->my_temp_namespace == InvalidOid;
		namespace_state->active_search_path = session1_path;
		namespace_state->active_creation_namespace = 11;
		namespace_state->active_temp_creation_pending = true;
		namespace_state->active_path_generation = 101;
		namespace_state->base_search_path = session1_path;
		namespace_state->base_creation_namespace = 12;
		namespace_state->base_temp_creation_pending = true;
		namespace_state->namespace_user = 10;
		namespace_state->base_search_path_valid = false;
		namespace_state->search_path_cache_valid = true;
		namespace_state->my_temp_namespace = 13;
		namespace_state->my_temp_toast_namespace = 14;
		namespace_state->my_temp_namespace_subid = 15;
		namespace_state->namespace_search_path_value = "phase12_namespace_a";
		namespace_state->search_path_cache = &fake_session1;
		namespace_state->last_search_path_cache_entry = &fake_session1;

		PgSetCurrentSession(&fake_session2);
		namespace_state = PgCurrentNamespaceState();
		ok = ok && namespace_state->initialized;
		ok = ok && namespace_state->active_search_path == NIL;
		ok = ok && namespace_state->active_path_generation == 1;
		ok = ok && !namespace_state->base_search_path_valid;
		ok = ok && !namespace_state->search_path_cache_valid;
		ok = ok && namespace_state->my_temp_namespace == InvalidOid;
		ok = ok && namespace_state->namespace_search_path_value != NULL;
		ok = ok && strcmp(namespace_state->namespace_search_path_value,
						  "\"$user\", public") == 0;
		namespace_state->active_search_path = session2_path;
		namespace_state->active_creation_namespace = 21;
		namespace_state->active_path_generation = 202;
		namespace_state->base_search_path = session2_path;
		namespace_state->base_creation_namespace = 22;
		namespace_state->namespace_user = 20;
		namespace_state->my_temp_namespace = 23;
		namespace_state->my_temp_toast_namespace = 24;
		namespace_state->namespace_search_path_value = "phase12_namespace_b";
		namespace_state->search_path_cache = &fake_session2;
		namespace_state->last_search_path_cache_entry = &fake_session2;

		PgSetCurrentSession(&fake_session1);
		namespace_state = PgCurrentNamespaceState();
		ok = ok && namespace_state->active_search_path == session1_path;
		ok = ok && namespace_state->active_creation_namespace == 11;
		ok = ok && namespace_state->active_temp_creation_pending;
		ok = ok && namespace_state->active_path_generation == 101;
		ok = ok && namespace_state->base_search_path == session1_path;
		ok = ok && namespace_state->base_creation_namespace == 12;
		ok = ok && namespace_state->base_temp_creation_pending;
		ok = ok && namespace_state->namespace_user == 10;
		ok = ok && !namespace_state->base_search_path_valid;
		ok = ok && namespace_state->search_path_cache_valid;
		ok = ok && namespace_state->my_temp_namespace == 13;
		ok = ok && namespace_state->my_temp_toast_namespace == 14;
		ok = ok && namespace_state->my_temp_namespace_subid == 15;
		ok = ok && strcmp(namespace_state->namespace_search_path_value,
						  "phase12_namespace_a") == 0;
		ok = ok && namespace_state->search_path_cache == &fake_session1;
		ok = ok && namespace_state->last_search_path_cache_entry == &fake_session1;

		PgSetCurrentSession(&fake_session2);
		namespace_state = PgCurrentNamespaceState();
		ok = ok && namespace_state->active_search_path == session2_path;
		ok = ok && namespace_state->active_creation_namespace == 21;
		ok = ok && namespace_state->active_path_generation == 202;
		ok = ok && namespace_state->base_search_path == session2_path;
		ok = ok && namespace_state->base_creation_namespace == 22;
		ok = ok && namespace_state->namespace_user == 20;
		ok = ok && namespace_state->my_temp_namespace == 23;
		ok = ok && namespace_state->my_temp_toast_namespace == 24;
		ok = ok && strcmp(namespace_state->namespace_search_path_value,
						  "phase12_namespace_b") == 0;
		ok = ok && namespace_state->search_path_cache == &fake_session2;
		ok = ok && namespace_state->last_search_path_cache_entry == &fake_session2;

		PgSetCurrentSession(saved_session);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "namespace state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_locale_state_is_session_local);
Datum
test_session_locale_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	PgSessionLocaleState *locale_state;
	bool		ok = true;

	saved_session = CurrentPgSession;
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		locale_state = PgCurrentLocaleState();
		ok = ok && locale_state->initialized;
		ok = ok && locale_state->icu_validation_level_value == WARNING;
		ok = ok && locale_state->last_collation_cache_oid == InvalidOid;
		locale_state->locale_messages_value = "locale_messages_a";
		locale_state->locale_monetary_value = "locale_monetary_a";
		locale_state->locale_numeric_value = "locale_numeric_a";
		locale_state->locale_time_value = "locale_time_a";
		locale_state->icu_validation_level_value = ERROR;
		locale_state->localized_abbrev_days_values[0] = "SunA";
		locale_state->localized_full_days_values[0] = "SundayA";
		locale_state->localized_abbrev_months_values[0] = "JanA";
		locale_state->localized_full_months_values[0] = "JanuaryA";
		locale_state->locale_conv_valid = true;
		locale_state->locale_time_valid = true;
		locale_state->current_locale_conv = &fake_session1;
		locale_state->current_locale_conv_allocated = true;
		locale_state->collation_cache_context = (MemoryContext) &fake_session1;
		locale_state->collation_cache = &fake_session1;
		locale_state->last_collation_cache_oid = 111;
		locale_state->last_collation_cache_locale = &fake_session1;
		locale_state->icu_converter = &fake_session1;

		PgSetCurrentSession(&fake_session2);
		locale_state = PgCurrentLocaleState();
		ok = ok && locale_state->initialized;
		ok = ok && locale_state->locale_messages_value == NULL;
		ok = ok && locale_state->locale_monetary_value == NULL;
		ok = ok && locale_state->locale_numeric_value == NULL;
		ok = ok && locale_state->locale_time_value == NULL;
		ok = ok && locale_state->icu_validation_level_value == WARNING;
		ok = ok && !locale_state->locale_conv_valid;
		ok = ok && !locale_state->locale_time_valid;
		ok = ok && locale_state->last_collation_cache_oid == InvalidOid;
		locale_state->locale_messages_value = "locale_messages_b";
		locale_state->locale_monetary_value = "locale_monetary_b";
		locale_state->locale_numeric_value = "locale_numeric_b";
		locale_state->locale_time_value = "locale_time_b";
		locale_state->icu_validation_level_value = WARNING;
		locale_state->localized_abbrev_days_values[0] = "SunB";
		locale_state->localized_full_days_values[0] = "SundayB";
		locale_state->localized_abbrev_months_values[0] = "JanB";
		locale_state->localized_full_months_values[0] = "JanuaryB";
		locale_state->current_locale_conv = &fake_session2;
		locale_state->collation_cache_context = (MemoryContext) &fake_session2;
		locale_state->collation_cache = &fake_session2;
		locale_state->last_collation_cache_oid = 222;
		locale_state->last_collation_cache_locale = &fake_session2;
		locale_state->icu_converter = &fake_session2;

		PgSetCurrentSession(&fake_session1);
		locale_state = PgCurrentLocaleState();
		ok = ok && strcmp(locale_messages, "locale_messages_a") == 0;
		ok = ok && strcmp(locale_monetary, "locale_monetary_a") == 0;
		ok = ok && strcmp(locale_numeric, "locale_numeric_a") == 0;
		ok = ok && strcmp(locale_time, "locale_time_a") == 0;
		ok = ok && icu_validation_level == ERROR;
		ok = ok && strcmp(localized_abbrev_days[0], "SunA") == 0;
		ok = ok && strcmp(localized_full_days[0], "SundayA") == 0;
		ok = ok && strcmp(localized_abbrev_months[0], "JanA") == 0;
		ok = ok && strcmp(localized_full_months[0], "JanuaryA") == 0;
		ok = ok && locale_state->locale_conv_valid;
		ok = ok && locale_state->locale_time_valid;
		ok = ok && locale_state->current_locale_conv == &fake_session1;
		ok = ok && locale_state->current_locale_conv_allocated;
		ok = ok && locale_state->collation_cache_context == (MemoryContext) &fake_session1;
		ok = ok && locale_state->collation_cache == &fake_session1;
		ok = ok && locale_state->last_collation_cache_oid == 111;
		ok = ok && locale_state->last_collation_cache_locale == &fake_session1;
		ok = ok && *PgCurrentIcuConverterRef() == &fake_session1;

		PgSetCurrentSession(&fake_session2);
		locale_state = PgCurrentLocaleState();
		ok = ok && strcmp(locale_messages, "locale_messages_b") == 0;
		ok = ok && strcmp(locale_monetary, "locale_monetary_b") == 0;
		ok = ok && strcmp(locale_numeric, "locale_numeric_b") == 0;
		ok = ok && strcmp(locale_time, "locale_time_b") == 0;
		ok = ok && icu_validation_level == WARNING;
		ok = ok && strcmp(localized_abbrev_days[0], "SunB") == 0;
		ok = ok && strcmp(localized_full_days[0], "SundayB") == 0;
		ok = ok && strcmp(localized_abbrev_months[0], "JanB") == 0;
		ok = ok && strcmp(localized_full_months[0], "JanuaryB") == 0;
		ok = ok && locale_state->current_locale_conv == &fake_session2;
		ok = ok && locale_state->collation_cache_context == (MemoryContext) &fake_session2;
		ok = ok && locale_state->collation_cache == &fake_session2;
		ok = ok && locale_state->last_collation_cache_oid == 222;
		ok = ok && locale_state->last_collation_cache_locale == &fake_session2;
		ok = ok && *PgCurrentIcuConverterRef() == &fake_session2;

		PgSetCurrentSession(saved_session);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "locale state was not session-local");

	PG_RETURN_BOOL(true);
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
	char		session1_advice[] = "session1 advice";
	char		session2_advice[] = "session2 advice";
	char		session1_stash[] = "session1_stash";
	char		session2_stash[] = "session2_stash";
	char		session1_auto_explain_options[] = "debug";
	char		session2_auto_explain_options[] = "range_table";
	char		session1_shell_command[] = "session1cmd";
	char		session1_shell_role[] = "session1role";
	char		session2_shell_command[] = "session2cmd";
	char		session2_shell_role[] = "session2role";
	int			session1_refint_foreign;
	int			session1_refint_primary;
	int			session2_refint_foreign;
	int			session2_refint_primary;
	bool		ok = true;

	saved_session = CurrentPgSession;
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		fake_session1.extension_modules.pg_trgm_similarity_threshold = 0.3;
		fake_session1.extension_modules.pg_trgm_word_similarity_threshold = 0.6;
		fake_session1.extension_modules.pg_trgm_strict_word_similarity_threshold = 0.5;
		fake_session1.extension_modules.pg_plan_advice_always_explain_supplied_advice = true;
		fake_session1.extension_modules.pg_stash_advice_stash_name = "";
		test_backend_runtime_seed_auto_explain_defaults(&fake_session1.extension_modules);
		test_backend_runtime_seed_small_contrib_defaults(&fake_session1.extension_modules);
		fake_session2.extension_modules.pg_trgm_similarity_threshold = 0.3;
		fake_session2.extension_modules.pg_trgm_word_similarity_threshold = 0.6;
		fake_session2.extension_modules.pg_trgm_strict_word_similarity_threshold = 0.5;
		fake_session2.extension_modules.pg_plan_advice_always_explain_supplied_advice = true;
		fake_session2.extension_modules.pg_stash_advice_stash_name = "";
		test_backend_runtime_seed_auto_explain_defaults(&fake_session2.extension_modules);
		test_backend_runtime_seed_small_contrib_defaults(&fake_session2.extension_modules);

		PgSetCurrentSession(&fake_session1);
		extension_modules = PgCurrentSessionExtensionModuleState();
		ok = ok && extension_modules->pg_trgm_similarity_threshold == 0.3;
		ok = ok && extension_modules->pg_trgm_word_similarity_threshold == 0.6;
		ok = ok && extension_modules->pg_trgm_strict_word_similarity_threshold == 0.5;
		ok = ok && extension_modules->pg_plan_advice_advice == NULL;
		ok = ok && !extension_modules->pg_plan_advice_always_store_advice_details;
		ok = ok && extension_modules->pg_plan_advice_always_explain_supplied_advice;
		ok = ok && !extension_modules->pg_plan_advice_feedback_warnings;
		ok = ok && !extension_modules->pg_plan_advice_trace_mask;
		ok = ok && extension_modules->pg_plan_advice_generate_advice == 0;
		ok = ok && strcmp(extension_modules->pg_stash_advice_stash_name, "") == 0;
		ok = ok && extension_modules->plpython_procedure_cache == NULL;
		ok = ok && !extension_modules->plpython_reset_registered;
		ok = ok && extension_modules->pltcl_start_proc == NULL;
		ok = ok && extension_modules->pltclu_start_proc == NULL;
		ok = ok && extension_modules->pltcl_hold_interp == NULL;
		ok = ok && extension_modules->pltcl_interp_hash == NULL;
		ok = ok && extension_modules->pltcl_proc_hash == NULL;
		ok = ok && extension_modules->pltcl_current_call_state == NULL;
		ok = ok && !extension_modules->pltcl_reset_registered;
		ok = ok && test_backend_runtime_refint_defaults_ok(extension_modules);
		ok = ok && test_backend_runtime_small_contrib_defaults_ok(extension_modules);
		ok = ok && extension_modules->dblink_persistent_connection == NULL;
		ok = ok && extension_modules->dblink_remote_conn_hash == NULL;
		ok = ok && !extension_modules->dblink_reset_registered;
		ok = ok && test_backend_runtime_postgres_fdw_defaults_ok(extension_modules);
		ok = ok && test_backend_runtime_auto_explain_defaults_ok(extension_modules);
		extension_modules->pg_trgm_similarity_threshold = 0.11;
		extension_modules->pg_trgm_word_similarity_threshold = 0.12;
		extension_modules->pg_trgm_strict_word_similarity_threshold = 0.13;
		extension_modules->pg_plan_advice_advice = session1_advice;
		extension_modules->pg_plan_advice_always_store_advice_details = true;
		extension_modules->pg_plan_advice_always_explain_supplied_advice = false;
		extension_modules->pg_plan_advice_feedback_warnings = true;
		extension_modules->pg_plan_advice_trace_mask = true;
		extension_modules->pg_plan_advice_generate_advice = 1;
		extension_modules->pg_stash_advice_stash_name = session1_stash;
		extension_modules->auto_explain_log_min_duration = 10;
		extension_modules->auto_explain_log_parameter_max_length = 64;
		extension_modules->auto_explain_log_analyze = true;
		extension_modules->auto_explain_log_verbose = true;
		extension_modules->auto_explain_log_buffers = true;
		extension_modules->auto_explain_log_io = true;
		extension_modules->auto_explain_log_wal = true;
		extension_modules->auto_explain_log_triggers = true;
		extension_modules->auto_explain_log_timing = false;
		extension_modules->auto_explain_log_settings = true;
		extension_modules->auto_explain_log_format = EXPLAIN_FORMAT_JSON;
		extension_modules->auto_explain_log_level = WARNING;
		extension_modules->auto_explain_log_nested_statements = true;
		extension_modules->auto_explain_sample_rate = 0.25;
		extension_modules->auto_explain_log_extension_options =
			session1_auto_explain_options;
		extension_modules->auto_explain_extension_options = &session1_private;
		extension_modules->plpython_procedure_cache = &session1_private;
		extension_modules->plpython_reset_registered = true;
		extension_modules->pltcl_start_proc = session1_advice;
		extension_modules->pltclu_start_proc = session1_stash;
		extension_modules->pltcl_hold_interp = &session1_private;
		extension_modules->pltcl_interp_hash = &session1_reset_count;
		extension_modules->pltcl_proc_hash = session1_auto_explain_options;
		extension_modules->pltcl_current_call_state = &session1_private;
		extension_modules->pltcl_reset_registered = true;
		extension_modules->refint_foreign_plans = &session1_refint_foreign;
		extension_modules->refint_num_foreign_plans = 31;
		extension_modules->refint_primary_plans = &session1_refint_primary;
		extension_modules->refint_num_primary_plans = 32;
		extension_modules->refint_reset_registered = true;
		extension_modules->auth_delay_milliseconds = 31;
		extension_modules->basebackup_to_shell_command = session1_shell_command;
		extension_modules->basebackup_to_shell_required_role = session1_shell_role;
		extension_modules->isn_weak = true;
		extension_modules->passwordcheck_min_password_length = 32;
		extension_modules->dblink_persistent_connection = &session1_private;
		extension_modules->dblink_remote_conn_hash = &session1_reset_count;
		extension_modules->dblink_reset_registered = true;
		extension_modules->postgres_fdw_connection_hash = &session1_private;
		extension_modules->postgres_fdw_shippable_cache_hash =
			&session1_reset_count;
		extension_modules->postgres_fdw_cursor_number = 11;
		extension_modules->postgres_fdw_prep_stmt_number = 12;
		extension_modules->postgres_fdw_xact_got_connection = true;
		extension_modules->postgres_fdw_read_only_level = 13;
		extension_modules->postgres_fdw_connection_callbacks_registered = true;
		extension_modules->postgres_fdw_shippable_callbacks_registered = true;

		ok = ok && *PgCurrentPLpgSQLSessionStateRef() == NULL;
		*PgCurrentPLpgSQLSessionStateRef() = &session1_private;
		PgSessionRegisterResetCallback(test_backend_runtime_session_reset_callback,
									   &session1_reset_count);

		PgSetCurrentSession(&fake_session2);
		extension_modules = PgCurrentSessionExtensionModuleState();
		ok = ok && extension_modules->pg_trgm_similarity_threshold == 0.3;
		ok = ok && extension_modules->pg_trgm_word_similarity_threshold == 0.6;
		ok = ok && extension_modules->pg_trgm_strict_word_similarity_threshold == 0.5;
		ok = ok && extension_modules->pg_plan_advice_advice == NULL;
		ok = ok && !extension_modules->pg_plan_advice_always_store_advice_details;
		ok = ok && extension_modules->pg_plan_advice_always_explain_supplied_advice;
		ok = ok && !extension_modules->pg_plan_advice_feedback_warnings;
		ok = ok && !extension_modules->pg_plan_advice_trace_mask;
		ok = ok && extension_modules->pg_plan_advice_generate_advice == 0;
		ok = ok && strcmp(extension_modules->pg_stash_advice_stash_name, "") == 0;
		ok = ok && extension_modules->plpython_procedure_cache == NULL;
		ok = ok && !extension_modules->plpython_reset_registered;
		ok = ok && extension_modules->pltcl_start_proc == NULL;
		ok = ok && extension_modules->pltclu_start_proc == NULL;
		ok = ok && extension_modules->pltcl_hold_interp == NULL;
		ok = ok && extension_modules->pltcl_interp_hash == NULL;
		ok = ok && extension_modules->pltcl_proc_hash == NULL;
		ok = ok && extension_modules->pltcl_current_call_state == NULL;
		ok = ok && !extension_modules->pltcl_reset_registered;
		ok = ok && test_backend_runtime_refint_defaults_ok(extension_modules);
		ok = ok && test_backend_runtime_small_contrib_defaults_ok(extension_modules);
		ok = ok && extension_modules->dblink_persistent_connection == NULL;
		ok = ok && extension_modules->dblink_remote_conn_hash == NULL;
		ok = ok && !extension_modules->dblink_reset_registered;
		ok = ok && test_backend_runtime_postgres_fdw_defaults_ok(extension_modules);
		ok = ok && test_backend_runtime_auto_explain_defaults_ok(extension_modules);
		extension_modules->pg_trgm_similarity_threshold = 0.21;
		extension_modules->pg_trgm_word_similarity_threshold = 0.22;
		extension_modules->pg_trgm_strict_word_similarity_threshold = 0.23;
		extension_modules->pg_plan_advice_advice = session2_advice;
		extension_modules->pg_plan_advice_always_store_advice_details = false;
		extension_modules->pg_plan_advice_always_explain_supplied_advice = true;
		extension_modules->pg_plan_advice_feedback_warnings = false;
		extension_modules->pg_plan_advice_trace_mask = true;
		extension_modules->pg_plan_advice_generate_advice = 2;
		extension_modules->pg_stash_advice_stash_name = session2_stash;
		extension_modules->auto_explain_log_min_duration = 20;
		extension_modules->auto_explain_log_parameter_max_length = 128;
		extension_modules->auto_explain_log_analyze = false;
		extension_modules->auto_explain_log_verbose = true;
		extension_modules->auto_explain_log_buffers = false;
		extension_modules->auto_explain_log_io = true;
		extension_modules->auto_explain_log_wal = false;
		extension_modules->auto_explain_log_triggers = true;
		extension_modules->auto_explain_log_timing = true;
		extension_modules->auto_explain_log_settings = false;
		extension_modules->auto_explain_log_format = EXPLAIN_FORMAT_XML;
		extension_modules->auto_explain_log_level = NOTICE;
		extension_modules->auto_explain_log_nested_statements = false;
		extension_modules->auto_explain_sample_rate = 0.75;
		extension_modules->auto_explain_log_extension_options =
			session2_auto_explain_options;
		extension_modules->auto_explain_extension_options = &session2_private;
		extension_modules->plpython_procedure_cache = &session2_private;
		extension_modules->plpython_reset_registered = true;
		extension_modules->pltcl_start_proc = session2_advice;
		extension_modules->pltclu_start_proc = session2_stash;
		extension_modules->pltcl_hold_interp = &session2_private;
		extension_modules->pltcl_interp_hash = &session2_reset_count;
		extension_modules->pltcl_proc_hash = session2_auto_explain_options;
		extension_modules->pltcl_current_call_state = &session2_private;
		extension_modules->pltcl_reset_registered = true;
		extension_modules->refint_foreign_plans = &session2_refint_foreign;
		extension_modules->refint_num_foreign_plans = 41;
		extension_modules->refint_primary_plans = &session2_refint_primary;
		extension_modules->refint_num_primary_plans = 42;
		extension_modules->refint_reset_registered = true;
		extension_modules->auth_delay_milliseconds = 41;
		extension_modules->basebackup_to_shell_command = session2_shell_command;
		extension_modules->basebackup_to_shell_required_role = session2_shell_role;
		extension_modules->isn_weak = true;
		extension_modules->passwordcheck_min_password_length = 42;
		extension_modules->dblink_persistent_connection = &session2_private;
		extension_modules->dblink_remote_conn_hash = &session2_reset_count;
		extension_modules->dblink_reset_registered = true;
		extension_modules->postgres_fdw_connection_hash = &session2_private;
		extension_modules->postgres_fdw_shippable_cache_hash =
			&session2_reset_count;
		extension_modules->postgres_fdw_cursor_number = 21;
		extension_modules->postgres_fdw_prep_stmt_number = 22;
		extension_modules->postgres_fdw_xact_got_connection = true;
		extension_modules->postgres_fdw_read_only_level = 23;
		extension_modules->postgres_fdw_connection_callbacks_registered = true;
		extension_modules->postgres_fdw_shippable_callbacks_registered = true;

		ok = ok && *PgCurrentPLpgSQLSessionStateRef() == NULL;
		*PgCurrentPLpgSQLSessionStateRef() = &session2_private;
		PgSessionRegisterResetCallback(test_backend_runtime_session_reset_callback,
									   &session2_reset_count);

		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentPLpgSQLSessionStateRef() == &session1_private;
		extension_modules = PgCurrentSessionExtensionModuleState();
		ok = ok && extension_modules->pg_trgm_similarity_threshold == 0.11;
		ok = ok && extension_modules->pg_trgm_word_similarity_threshold == 0.12;
		ok = ok && extension_modules->pg_trgm_strict_word_similarity_threshold == 0.13;
		ok = ok && strcmp(extension_modules->pg_plan_advice_advice,
						  "session1 advice") == 0;
		ok = ok && extension_modules->pg_plan_advice_always_store_advice_details;
		ok = ok && !extension_modules->pg_plan_advice_always_explain_supplied_advice;
		ok = ok && extension_modules->pg_plan_advice_feedback_warnings;
		ok = ok && extension_modules->pg_plan_advice_trace_mask;
		ok = ok && extension_modules->pg_plan_advice_generate_advice == 1;
		ok = ok && strcmp(extension_modules->pg_stash_advice_stash_name,
						  "session1_stash") == 0;
		ok = ok && extension_modules->auto_explain_log_min_duration == 10;
		ok = ok && extension_modules->auto_explain_log_parameter_max_length == 64;
		ok = ok && extension_modules->auto_explain_log_analyze;
		ok = ok && extension_modules->auto_explain_log_verbose;
		ok = ok && extension_modules->auto_explain_log_buffers;
		ok = ok && extension_modules->auto_explain_log_io;
		ok = ok && extension_modules->auto_explain_log_wal;
		ok = ok && extension_modules->auto_explain_log_triggers;
		ok = ok && !extension_modules->auto_explain_log_timing;
		ok = ok && extension_modules->auto_explain_log_settings;
		ok = ok && extension_modules->auto_explain_log_format == EXPLAIN_FORMAT_JSON;
		ok = ok && extension_modules->auto_explain_log_level == WARNING;
		ok = ok && extension_modules->auto_explain_log_nested_statements;
		ok = ok && extension_modules->auto_explain_sample_rate == 0.25;
		ok = ok && strcmp(extension_modules->auto_explain_log_extension_options,
						  "debug") == 0;
		ok = ok && extension_modules->auto_explain_extension_options ==
			&session1_private;
		ok = ok && extension_modules->plpython_procedure_cache ==
			&session1_private;
		ok = ok && extension_modules->plpython_reset_registered;
		ok = ok && strcmp(extension_modules->pltcl_start_proc,
						  "session1 advice") == 0;
		ok = ok && strcmp(extension_modules->pltclu_start_proc,
						  "session1_stash") == 0;
		ok = ok && extension_modules->pltcl_hold_interp == &session1_private;
		ok = ok && extension_modules->pltcl_interp_hash ==
			&session1_reset_count;
		ok = ok && extension_modules->pltcl_proc_hash ==
			session1_auto_explain_options;
		ok = ok && extension_modules->pltcl_current_call_state ==
			&session1_private;
		ok = ok && extension_modules->pltcl_reset_registered;
		ok = ok && extension_modules->refint_foreign_plans ==
			&session1_refint_foreign;
		ok = ok && extension_modules->refint_num_foreign_plans == 31;
		ok = ok && extension_modules->refint_primary_plans ==
			&session1_refint_primary;
		ok = ok && extension_modules->refint_num_primary_plans == 32;
		ok = ok && extension_modules->refint_reset_registered;
		ok = ok && extension_modules->auth_delay_milliseconds == 31;
		ok = ok && strcmp(extension_modules->basebackup_to_shell_command,
						  "session1cmd") == 0;
		ok = ok && strcmp(extension_modules->basebackup_to_shell_required_role,
						  "session1role") == 0;
		ok = ok && extension_modules->isn_weak;
		ok = ok && extension_modules->passwordcheck_min_password_length == 32;
		ok = ok && extension_modules->dblink_persistent_connection ==
			&session1_private;
		ok = ok && extension_modules->dblink_remote_conn_hash ==
			&session1_reset_count;
		ok = ok && extension_modules->dblink_reset_registered;
		ok = ok && extension_modules->postgres_fdw_connection_hash ==
			&session1_private;
		ok = ok && extension_modules->postgres_fdw_shippable_cache_hash ==
			&session1_reset_count;
		ok = ok && extension_modules->postgres_fdw_cursor_number == 11;
		ok = ok && extension_modules->postgres_fdw_prep_stmt_number == 12;
		ok = ok && extension_modules->postgres_fdw_xact_got_connection;
		ok = ok && extension_modules->postgres_fdw_read_only_level == 13;
		ok = ok && extension_modules->postgres_fdw_connection_callbacks_registered;
		ok = ok && extension_modules->postgres_fdw_shippable_callbacks_registered;

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentPLpgSQLSessionStateRef() == &session2_private;
		extension_modules = PgCurrentSessionExtensionModuleState();
		ok = ok && extension_modules->pg_trgm_similarity_threshold == 0.21;
		ok = ok && extension_modules->pg_trgm_word_similarity_threshold == 0.22;
		ok = ok && extension_modules->pg_trgm_strict_word_similarity_threshold == 0.23;
		ok = ok && strcmp(extension_modules->pg_plan_advice_advice,
						  "session2 advice") == 0;
		ok = ok && !extension_modules->pg_plan_advice_always_store_advice_details;
		ok = ok && extension_modules->pg_plan_advice_always_explain_supplied_advice;
		ok = ok && !extension_modules->pg_plan_advice_feedback_warnings;
		ok = ok && extension_modules->pg_plan_advice_trace_mask;
		ok = ok && extension_modules->pg_plan_advice_generate_advice == 2;
		ok = ok && strcmp(extension_modules->pg_stash_advice_stash_name,
						  "session2_stash") == 0;
		ok = ok && extension_modules->auto_explain_log_min_duration == 20;
		ok = ok && extension_modules->auto_explain_log_parameter_max_length == 128;
		ok = ok && !extension_modules->auto_explain_log_analyze;
		ok = ok && extension_modules->auto_explain_log_verbose;
		ok = ok && !extension_modules->auto_explain_log_buffers;
		ok = ok && extension_modules->auto_explain_log_io;
		ok = ok && !extension_modules->auto_explain_log_wal;
		ok = ok && extension_modules->auto_explain_log_triggers;
		ok = ok && extension_modules->auto_explain_log_timing;
		ok = ok && !extension_modules->auto_explain_log_settings;
		ok = ok && extension_modules->auto_explain_log_format == EXPLAIN_FORMAT_XML;
		ok = ok && extension_modules->auto_explain_log_level == NOTICE;
		ok = ok && !extension_modules->auto_explain_log_nested_statements;
		ok = ok && extension_modules->auto_explain_sample_rate == 0.75;
		ok = ok && strcmp(extension_modules->auto_explain_log_extension_options,
						  "range_table") == 0;
		ok = ok && extension_modules->auto_explain_extension_options ==
			&session2_private;
		ok = ok && extension_modules->plpython_procedure_cache ==
			&session2_private;
		ok = ok && extension_modules->plpython_reset_registered;
		ok = ok && strcmp(extension_modules->pltcl_start_proc,
						  "session2 advice") == 0;
		ok = ok && strcmp(extension_modules->pltclu_start_proc,
						  "session2_stash") == 0;
		ok = ok && extension_modules->pltcl_hold_interp == &session2_private;
		ok = ok && extension_modules->pltcl_interp_hash ==
			&session2_reset_count;
		ok = ok && extension_modules->pltcl_proc_hash ==
			session2_auto_explain_options;
		ok = ok && extension_modules->pltcl_current_call_state ==
			&session2_private;
		ok = ok && extension_modules->pltcl_reset_registered;
		ok = ok && extension_modules->refint_foreign_plans ==
			&session2_refint_foreign;
		ok = ok && extension_modules->refint_num_foreign_plans == 41;
		ok = ok && extension_modules->refint_primary_plans ==
			&session2_refint_primary;
		ok = ok && extension_modules->refint_num_primary_plans == 42;
		ok = ok && extension_modules->refint_reset_registered;
		ok = ok && extension_modules->auth_delay_milliseconds == 41;
		ok = ok && strcmp(extension_modules->basebackup_to_shell_command,
						  "session2cmd") == 0;
		ok = ok && strcmp(extension_modules->basebackup_to_shell_required_role,
						  "session2role") == 0;
		ok = ok && extension_modules->isn_weak;
		ok = ok && extension_modules->passwordcheck_min_password_length == 42;
		ok = ok && extension_modules->dblink_persistent_connection ==
			&session2_private;
		ok = ok && extension_modules->dblink_remote_conn_hash ==
			&session2_reset_count;
		ok = ok && extension_modules->dblink_reset_registered;
		ok = ok && extension_modules->postgres_fdw_connection_hash ==
			&session2_private;
		ok = ok && extension_modules->postgres_fdw_shippable_cache_hash ==
			&session2_reset_count;
		ok = ok && extension_modules->postgres_fdw_cursor_number == 21;
		ok = ok && extension_modules->postgres_fdw_prep_stmt_number == 22;
		ok = ok && extension_modules->postgres_fdw_xact_got_connection;
		ok = ok && extension_modules->postgres_fdw_read_only_level == 23;
		ok = ok && extension_modules->postgres_fdw_connection_callbacks_registered;
		ok = ok && extension_modules->postgres_fdw_shippable_callbacks_registered;

		PgSetCurrentSession(saved_session);
		PgSessionResetClosedState(&fake_session1);
		ok = ok && session1_reset_count == 1;
		ok = ok && session2_reset_count == 0;
		ok = ok && fake_session1.extension_modules.plpgsql_state == NULL;
		ok = ok && fake_session1.extension_modules.plpython_procedure_cache == NULL;
		ok = ok && !fake_session1.extension_modules.plpython_reset_registered;
		ok = ok && fake_session1.extension_modules.pltcl_start_proc == NULL;
		ok = ok && fake_session1.extension_modules.pltclu_start_proc == NULL;
		ok = ok && fake_session1.extension_modules.pltcl_hold_interp == NULL;
		ok = ok && fake_session1.extension_modules.pltcl_interp_hash == NULL;
		ok = ok && fake_session1.extension_modules.pltcl_proc_hash == NULL;
		ok = ok && fake_session1.extension_modules.pltcl_current_call_state == NULL;
		ok = ok && !fake_session1.extension_modules.pltcl_reset_registered;
		ok = ok && test_backend_runtime_refint_defaults_ok(&fake_session1.extension_modules);
		ok = ok && test_backend_runtime_small_contrib_defaults_ok(&fake_session1.extension_modules);
		ok = ok && fake_session1.extension_modules.reset_callbacks == NIL;
		ok = ok && fake_session1.extension_modules.pg_trgm_similarity_threshold == 0.3;
		ok = ok && fake_session1.extension_modules.pg_trgm_word_similarity_threshold == 0.6;
		ok = ok && fake_session1.extension_modules.pg_trgm_strict_word_similarity_threshold == 0.5;
		ok = ok && fake_session1.extension_modules.pg_plan_advice_advice == NULL;
		ok = ok && !fake_session1.extension_modules.pg_plan_advice_always_store_advice_details;
		ok = ok && fake_session1.extension_modules.pg_plan_advice_always_explain_supplied_advice;
		ok = ok && !fake_session1.extension_modules.pg_plan_advice_feedback_warnings;
		ok = ok && !fake_session1.extension_modules.pg_plan_advice_trace_mask;
		ok = ok && fake_session1.extension_modules.pg_plan_advice_generate_advice == 0;
		ok = ok && strcmp(fake_session1.extension_modules.pg_stash_advice_stash_name,
						  "") == 0;
		ok = ok && fake_session1.extension_modules.dblink_persistent_connection == NULL;
		ok = ok && fake_session1.extension_modules.dblink_remote_conn_hash == NULL;
		ok = ok && !fake_session1.extension_modules.dblink_reset_registered;
		ok = ok && test_backend_runtime_postgres_fdw_defaults_ok(&fake_session1.extension_modules);
		ok = ok && test_backend_runtime_auto_explain_defaults_ok(&fake_session1.extension_modules);
		ok = ok && fake_session2.extension_modules.plpgsql_state == &session2_private;
		ok = ok && fake_session2.extension_modules.plpython_procedure_cache ==
			&session2_private;
		ok = ok && fake_session2.extension_modules.plpython_reset_registered;
		ok = ok && strcmp(fake_session2.extension_modules.pltcl_start_proc,
						  "session2 advice") == 0;
		ok = ok && strcmp(fake_session2.extension_modules.pltclu_start_proc,
						  "session2_stash") == 0;
		ok = ok && fake_session2.extension_modules.pltcl_hold_interp ==
			&session2_private;
		ok = ok && fake_session2.extension_modules.pltcl_interp_hash ==
			&session2_reset_count;
		ok = ok && fake_session2.extension_modules.pltcl_proc_hash ==
			session2_auto_explain_options;
		ok = ok && fake_session2.extension_modules.pltcl_current_call_state ==
			&session2_private;
		ok = ok && fake_session2.extension_modules.pltcl_reset_registered;
		ok = ok && fake_session2.extension_modules.refint_foreign_plans ==
			&session2_refint_foreign;
		ok = ok && fake_session2.extension_modules.refint_num_foreign_plans == 41;
		ok = ok && fake_session2.extension_modules.refint_primary_plans ==
			&session2_refint_primary;
		ok = ok && fake_session2.extension_modules.refint_num_primary_plans == 42;
		ok = ok && fake_session2.extension_modules.refint_reset_registered;
		ok = ok && fake_session2.extension_modules.auth_delay_milliseconds == 41;
		ok = ok && strcmp(fake_session2.extension_modules.basebackup_to_shell_command,
						  "session2cmd") == 0;
		ok = ok && strcmp(fake_session2.extension_modules.basebackup_to_shell_required_role,
						  "session2role") == 0;
		ok = ok && fake_session2.extension_modules.isn_weak;
		ok = ok && fake_session2.extension_modules.passwordcheck_min_password_length == 42;
		ok = ok && fake_session2.extension_modules.reset_callbacks != NIL;
		ok = ok && fake_session2.extension_modules.pg_trgm_similarity_threshold == 0.21;
		ok = ok && fake_session2.extension_modules.pg_trgm_word_similarity_threshold == 0.22;
		ok = ok && fake_session2.extension_modules.pg_trgm_strict_word_similarity_threshold == 0.23;
		ok = ok && strcmp(fake_session2.extension_modules.pg_plan_advice_advice,
						  "session2 advice") == 0;
		ok = ok && !fake_session2.extension_modules.pg_plan_advice_always_store_advice_details;
		ok = ok && fake_session2.extension_modules.pg_plan_advice_always_explain_supplied_advice;
		ok = ok && !fake_session2.extension_modules.pg_plan_advice_feedback_warnings;
		ok = ok && fake_session2.extension_modules.pg_plan_advice_trace_mask;
		ok = ok && fake_session2.extension_modules.pg_plan_advice_generate_advice == 2;
		ok = ok && strcmp(fake_session2.extension_modules.pg_stash_advice_stash_name,
						  "session2_stash") == 0;
		ok = ok && fake_session2.extension_modules.auto_explain_log_min_duration == 20;
		ok = ok && fake_session2.extension_modules.auto_explain_log_parameter_max_length == 128;
		ok = ok && !fake_session2.extension_modules.auto_explain_log_analyze;
		ok = ok && fake_session2.extension_modules.auto_explain_log_verbose;
		ok = ok && !fake_session2.extension_modules.auto_explain_log_buffers;
		ok = ok && fake_session2.extension_modules.auto_explain_log_io;
		ok = ok && !fake_session2.extension_modules.auto_explain_log_wal;
		ok = ok && fake_session2.extension_modules.auto_explain_log_triggers;
		ok = ok && fake_session2.extension_modules.auto_explain_log_timing;
		ok = ok && !fake_session2.extension_modules.auto_explain_log_settings;
		ok = ok && fake_session2.extension_modules.auto_explain_log_format == EXPLAIN_FORMAT_XML;
		ok = ok && fake_session2.extension_modules.auto_explain_log_level == NOTICE;
		ok = ok && !fake_session2.extension_modules.auto_explain_log_nested_statements;
		ok = ok && fake_session2.extension_modules.auto_explain_sample_rate == 0.75;
		ok = ok && strcmp(fake_session2.extension_modules.auto_explain_log_extension_options,
						  "range_table") == 0;
		ok = ok && fake_session2.extension_modules.auto_explain_extension_options ==
			&session2_private;
		ok = ok && fake_session2.extension_modules.dblink_persistent_connection ==
			&session2_private;
		ok = ok && fake_session2.extension_modules.dblink_remote_conn_hash ==
			&session2_reset_count;
		ok = ok && fake_session2.extension_modules.dblink_reset_registered;
		ok = ok && fake_session2.extension_modules.postgres_fdw_connection_hash ==
			&session2_private;
		ok = ok && fake_session2.extension_modules.postgres_fdw_shippable_cache_hash ==
			&session2_reset_count;
		ok = ok && fake_session2.extension_modules.postgres_fdw_cursor_number == 21;
		ok = ok && fake_session2.extension_modules.postgres_fdw_prep_stmt_number == 22;
		ok = ok && fake_session2.extension_modules.postgres_fdw_xact_got_connection;
		ok = ok && fake_session2.extension_modules.postgres_fdw_read_only_level == 23;
		ok = ok && fake_session2.extension_modules.postgres_fdw_connection_callbacks_registered;
		ok = ok && fake_session2.extension_modules.postgres_fdw_shippable_callbacks_registered;

		PgSessionResetClosedState(&fake_session2);
		ok = ok && session2_reset_count == 1;
		ok = ok && fake_session2.extension_modules.plpgsql_state == NULL;
		ok = ok && fake_session2.extension_modules.plpython_procedure_cache == NULL;
		ok = ok && !fake_session2.extension_modules.plpython_reset_registered;
		ok = ok && fake_session2.extension_modules.pltcl_start_proc == NULL;
		ok = ok && fake_session2.extension_modules.pltclu_start_proc == NULL;
		ok = ok && fake_session2.extension_modules.pltcl_hold_interp == NULL;
		ok = ok && fake_session2.extension_modules.pltcl_interp_hash == NULL;
		ok = ok && fake_session2.extension_modules.pltcl_proc_hash == NULL;
		ok = ok && fake_session2.extension_modules.pltcl_current_call_state == NULL;
		ok = ok && !fake_session2.extension_modules.pltcl_reset_registered;
		ok = ok && test_backend_runtime_refint_defaults_ok(&fake_session2.extension_modules);
		ok = ok && test_backend_runtime_small_contrib_defaults_ok(&fake_session2.extension_modules);
		ok = ok && fake_session2.extension_modules.reset_callbacks == NIL;
		ok = ok && fake_session2.extension_modules.pg_trgm_similarity_threshold == 0.3;
		ok = ok && fake_session2.extension_modules.pg_trgm_word_similarity_threshold == 0.6;
		ok = ok && fake_session2.extension_modules.pg_trgm_strict_word_similarity_threshold == 0.5;
		ok = ok && fake_session2.extension_modules.pg_plan_advice_advice == NULL;
		ok = ok && !fake_session2.extension_modules.pg_plan_advice_always_store_advice_details;
		ok = ok && fake_session2.extension_modules.pg_plan_advice_always_explain_supplied_advice;
		ok = ok && !fake_session2.extension_modules.pg_plan_advice_feedback_warnings;
		ok = ok && !fake_session2.extension_modules.pg_plan_advice_trace_mask;
		ok = ok && fake_session2.extension_modules.pg_plan_advice_generate_advice == 0;
		ok = ok && strcmp(fake_session2.extension_modules.pg_stash_advice_stash_name,
						  "") == 0;
		ok = ok && fake_session2.extension_modules.dblink_persistent_connection == NULL;
		ok = ok && fake_session2.extension_modules.dblink_remote_conn_hash == NULL;
		ok = ok && !fake_session2.extension_modules.dblink_reset_registered;
		ok = ok && test_backend_runtime_postgres_fdw_defaults_ok(&fake_session2.extension_modules);
		ok = ok && test_backend_runtime_auto_explain_defaults_ok(&fake_session2.extension_modules);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
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

	fake_session.database.database_path = pstrdup("base/1");
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
	fake_session.user_identity.system_user = pstrdup("trust:test");
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
