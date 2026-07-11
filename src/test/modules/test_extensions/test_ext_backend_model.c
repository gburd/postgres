/*
 * test_ext_backend_model.c
 *
 * Test helpers for extension backend-model loader policy.
 *
 * Portions Copyright (c) 2026, PostgreSQL Global Development Group
 */
#include "postgres.h"

#include "fmgr.h"
#include "utils/backend_runtime.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/memutils.h"

PG_MODULE_MAGIC_EXT(
					.name = "test_ext_backend_model",
					.version = PG_VERSION,
					PG_MODULE_MAGIC_BACKEND_MODEL_POOLED_SCHEDULER
);

PG_FUNCTION_INFO_V1(test_ext_backend_model_get);
PG_FUNCTION_INFO_V1(test_ext_backend_model_set);
PG_FUNCTION_INFO_V1(test_ext_backend_model_expect_load_error);
PG_FUNCTION_INFO_V1(test_ext_backend_model_expect_lookup_error);
PG_FUNCTION_INFO_V1(test_ext_backend_model_expect_set_error);

static const char *test_ext_backend_model_name(PgBackendModel backend_model);
static PgBackendModel test_ext_backend_model_parse(const char *name);

Datum
test_ext_backend_model_get(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(test_ext_backend_model_name(
									 PgRuntimeGetExtensionBackendModel())));
}

Datum
test_ext_backend_model_set(PG_FUNCTION_ARGS)
{
	char	   *name;
	PgBackendModel backend_model;

	name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	backend_model = test_ext_backend_model_parse(name);
	PgRuntimeSetExtensionBackendModel(backend_model);

	PG_RETURN_TEXT_P(cstring_to_text(test_ext_backend_model_name(
									 backend_model)));
}

Datum
test_ext_backend_model_expect_load_error(PG_FUNCTION_ARGS)
{
	char	   *libname;
	char	   *expected_detail;
	char	   *message;
	bool		matched;
	MemoryContext oldcontext;

	libname = text_to_cstring(PG_GETARG_TEXT_PP(0));
	expected_detail = text_to_cstring(PG_GETARG_TEXT_PP(1));
	message = NULL;
	matched = false;
	oldcontext = CurrentMemoryContext;

	PG_TRY();
	{
		load_file(libname, false);
		ereport(ERROR,
				(errmsg("LOAD \"%s\" unexpectedly succeeded", libname)));
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		MemoryContextSwitchTo(oldcontext);
		edata = CopyErrorData();
		FlushErrorState();

		if (strstr(edata->message, expected_detail) != NULL)
			matched = true;

		message = pstrdup(edata->message);
		FreeErrorData(edata);
	}
	PG_END_TRY();

	if (matched)
		PG_RETURN_TEXT_P(cstring_to_text("ok"));

	ereport(ERROR,
			(errmsg("unexpected LOAD error for \"%s\"", libname),
			 errdetail("Expected error to contain \"%s\", got \"%s\".",
					   expected_detail, message)));
}

Datum
test_ext_backend_model_expect_lookup_error(PG_FUNCTION_ARGS)
{
	char	   *libname;
	char	   *funcname;
	char	   *name;
	char	   *expected_detail;
	char	   *message;
	volatile PgBackendModel old_backend_model;
	PgBackendModel forced_backend_model;
	void	   *filehandle;
	bool		matched;
	MemoryContext oldcontext;

	libname = text_to_cstring(PG_GETARG_TEXT_PP(0));
	funcname = text_to_cstring(PG_GETARG_TEXT_PP(1));
	name = text_to_cstring(PG_GETARG_TEXT_PP(2));
	expected_detail = text_to_cstring(PG_GETARG_TEXT_PP(3));
	old_backend_model = PgRuntimeGetExtensionBackendModel();
	forced_backend_model = test_ext_backend_model_parse(name);
	filehandle = NULL;
	message = NULL;
	matched = false;
	oldcontext = CurrentMemoryContext;

	PG_TRY();
	{
		(void) load_external_function(libname, funcname, true, &filehandle);

		if (CurrentPgRuntime == NULL)
			elog(ERROR, "runtime is not initialized");

		/*
		 * Bypass PgRuntimeSetExtensionBackendModel() only inside this test so
		 * lookup_external_function() can be tested against an incompatible
		 * already-loaded handle.  The public setter rejects this transition.
		 */
		CurrentPgRuntime->extension_backend_model = forced_backend_model;

		(void) lookup_external_function(filehandle, funcname);

		CurrentPgRuntime->extension_backend_model =
			(PgBackendModel) old_backend_model;

		ereport(ERROR,
				(errmsg("lookup of \"%s\" in \"%s\" unexpectedly succeeded",
						funcname, libname)));
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		MemoryContextSwitchTo(oldcontext);
		if (CurrentPgRuntime != NULL)
			CurrentPgRuntime->extension_backend_model =
				(PgBackendModel) old_backend_model;
		edata = CopyErrorData();
		FlushErrorState();

		if (strstr(edata->message, expected_detail) != NULL)
			matched = true;

		message = pstrdup(edata->message);
		FreeErrorData(edata);
	}
	PG_END_TRY();

	if (matched)
		PG_RETURN_TEXT_P(cstring_to_text("ok"));

	ereport(ERROR,
			(errmsg("unexpected lookup error for \"%s\" in \"%s\"",
					funcname, libname),
			 errdetail("Expected error to contain \"%s\", got \"%s\".",
					   expected_detail, message)));
}

Datum
test_ext_backend_model_expect_set_error(PG_FUNCTION_ARGS)
{
	char	   *name;
	char	   *expected_detail;
	char	   *message;
	bool		matched;
	MemoryContext oldcontext;

	name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	expected_detail = text_to_cstring(PG_GETARG_TEXT_PP(1));
	message = NULL;
	matched = false;
	oldcontext = CurrentMemoryContext;

	PG_TRY();
	{
		PgRuntimeSetExtensionBackendModel(test_ext_backend_model_parse(name));
		ereport(ERROR,
				(errmsg("setting backend model to \"%s\" unexpectedly succeeded",
						name)));
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		MemoryContextSwitchTo(oldcontext);
		edata = CopyErrorData();
		FlushErrorState();

		if (strstr(edata->message, expected_detail) != NULL)
			matched = true;

		message = pstrdup(edata->message);
		FreeErrorData(edata);
	}
	PG_END_TRY();

	if (matched)
		PG_RETURN_TEXT_P(cstring_to_text("ok"));

	ereport(ERROR,
			(errmsg("unexpected backend model set error for \"%s\"", name),
			 errdetail("Expected error to contain \"%s\", got \"%s\".",
					   expected_detail, message)));
}

static const char *
test_ext_backend_model_name(PgBackendModel backend_model)
{
	switch (backend_model)
	{
		case PG_BACKEND_MODEL_PROCESS:
			return "process";
		case PG_BACKEND_MODEL_THREAD_PER_SESSION:
			return "thread-per-session";
		case PG_BACKEND_MODEL_POOLED_SCHEDULER:
			return "pooled-scheduler";
		case PG_BACKEND_MODEL_POOLED_PROTOCOL_AFFINE:
			return "pooled-protocol-affine";
		case PG_BACKEND_MODEL_POOLED_PROTOCOL_MIGRATABLE:
			return "pooled-protocol-migratable";
		case PG_BACKEND_MODEL_TASK_REENTRANT:
			return "task-reentrant";
	}

	return "unknown";
}

static PgBackendModel
test_ext_backend_model_parse(const char *name)
{
	if (pg_strcasecmp(name, "process") == 0)
		return PG_BACKEND_MODEL_PROCESS;
	if (pg_strcasecmp(name, "thread-per-session") == 0)
		return PG_BACKEND_MODEL_THREAD_PER_SESSION;
	if (pg_strcasecmp(name, "pooled-scheduler") == 0)
		return PG_BACKEND_MODEL_POOLED_SCHEDULER;
	if (pg_strcasecmp(name, "pooled-protocol-affine") == 0)
		return PG_BACKEND_MODEL_POOLED_PROTOCOL_AFFINE;
	if (pg_strcasecmp(name, "pooled-protocol-migratable") == 0)
		return PG_BACKEND_MODEL_POOLED_PROTOCOL_MIGRATABLE;
	if (pg_strcasecmp(name, "task-reentrant") == 0)
		return PG_BACKEND_MODEL_TASK_REENTRANT;

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("unrecognized backend model \"%s\"", name)));
	pg_unreachable();
}
