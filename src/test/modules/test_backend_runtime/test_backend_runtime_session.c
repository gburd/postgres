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
		PgSetCurrentSession(&fake_session1);
		CurrentPgSession->loop_state.send_ready_for_query = true;
		CurrentPgSession->loop_state.idle_in_transaction_timeout_enabled = true;
		CurrentPgSession->loop_state.doing_extended_query_message = true;
		CurrentPgSession->loop_state.transaction_started = true;

		PgSetCurrentSession(&fake_session2);
		ok = ok && !CurrentPgSession->loop_state.send_ready_for_query;
		ok = ok && !CurrentPgSession->loop_state.idle_in_transaction_timeout_enabled;
		ok = ok && !CurrentPgSession->loop_state.doing_extended_query_message;
		ok = ok && !CurrentPgSession->loop_state.transaction_started;
		CurrentPgSession->loop_state.send_ready_for_query = true;
		CurrentPgSession->loop_state.idle_session_timeout_enabled = true;
		CurrentPgSession->loop_state.ignore_till_sync = true;
		CurrentPgSession->loop_state.step_error_boundary_active = true;

		PgSetCurrentSession(&fake_session1);
		ok = ok && CurrentPgSession->loop_state.send_ready_for_query;
		ok = ok && CurrentPgSession->loop_state.idle_in_transaction_timeout_enabled;
		ok = ok && !CurrentPgSession->loop_state.idle_session_timeout_enabled;
		ok = ok && CurrentPgSession->loop_state.doing_extended_query_message;
		ok = ok && !CurrentPgSession->loop_state.ignore_till_sync;
		ok = ok && !CurrentPgSession->loop_state.step_error_boundary_active;
		ok = ok && CurrentPgSession->loop_state.transaction_started;

		PgSetCurrentSession(&fake_session2);
		ok = ok && CurrentPgSession->loop_state.send_ready_for_query;
		ok = ok && !CurrentPgSession->loop_state.idle_in_transaction_timeout_enabled;
		ok = ok && CurrentPgSession->loop_state.idle_session_timeout_enabled;
		ok = ok && !CurrentPgSession->loop_state.doing_extended_query_message;
		ok = ok && CurrentPgSession->loop_state.ignore_till_sync;
		ok = ok && CurrentPgSession->loop_state.step_error_boundary_active;
		ok = ok && !CurrentPgSession->loop_state.transaction_started;

		PgSetCurrentSession(saved_session);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
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
		PgSetCurrentSession(&fake_session1);
		MyDatabaseId = 1111;
		MyDatabaseTableSpace = 2222;
		MyDatabaseHasLoginEventTriggers = true;
		DatabasePath = fake_path1;

		PgSetCurrentSession(&fake_session2);
		ok = ok && MyDatabaseId == InvalidOid;
		ok = ok && MyDatabaseTableSpace == InvalidOid;
		ok = ok && !MyDatabaseHasLoginEventTriggers;
		ok = ok && DatabasePath == NULL;
		MyDatabaseId = 3333;
		MyDatabaseTableSpace = 4444;
		MyDatabaseHasLoginEventTriggers = false;
		DatabasePath = fake_path2;

		PgSetCurrentSession(&fake_session1);
		ok = ok && MyDatabaseId == 1111;
		ok = ok && MyDatabaseTableSpace == 2222;
		ok = ok && MyDatabaseHasLoginEventTriggers;
		ok = ok && DatabasePath == fake_path1;

		PgSetCurrentSession(&fake_session2);
		ok = ok && MyDatabaseId == 3333;
		ok = ok && MyDatabaseTableSpace == 4444;
		ok = ok && !MyDatabaseHasLoginEventTriggers;
		ok = ok && DatabasePath == fake_path2;

		PgSetCurrentSession(saved_session);
		MyDatabaseId = saved_database_id;
		MyDatabaseTableSpace = saved_database_tablespace;
		MyDatabaseHasLoginEventTriggers =
			saved_database_has_login_event_triggers;
		DatabasePath = saved_database_path;
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
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
		locale_state->locale_time_context = (MemoryContext) &fake_session1;
		locale_state->locale_conv_valid = true;
		locale_state->locale_time_valid = true;
		locale_state->locale_conv_context = (MemoryContext) &fake_session1;
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
		ok = ok && locale_state->locale_time_context == NULL;
		ok = ok && locale_state->locale_conv_context == NULL;
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
		locale_state->locale_time_context = (MemoryContext) &fake_session2;
		locale_state->locale_conv_context = (MemoryContext) &fake_session2;
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
		ok = ok && locale_state->locale_time_context == (MemoryContext) &fake_session1;
		ok = ok && locale_state->locale_conv_context == (MemoryContext) &fake_session1;
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
		ok = ok && locale_state->locale_time_context == (MemoryContext) &fake_session2;
		ok = ok && locale_state->locale_conv_context == (MemoryContext) &fake_session2;
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
