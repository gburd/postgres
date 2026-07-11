/*----------
 *
 * test_backend_runtime_session_guc_planner.c
 *		Session planner and JIT GUC backend runtime state tests.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_backend_runtime/test_backend_runtime_session_guc_planner.c
 *
 * -------------------------------------------------------------------------
 */
#include "test_backend_runtime.h"

typedef struct TestBoolGUCSetting
{
	const char *name;
	bool	   *(*ref) (void);
	bool		default_value;
	const char *session1_value;
	bool		session1_expected;
	const char *session2_value;
	bool		session2_expected;
} TestBoolGUCSetting;

typedef struct TestIntGUCSetting
{
	const char *name;
	int		   *(*ref) (void);
	int			default_value;
	const char *session1_value;
	int			session1_expected;
	const char *session2_value;
	int			session2_expected;
} TestIntGUCSetting;

typedef struct TestRealGUCSetting
{
	const char *name;
	double	   *(*ref) (void);
	double		default_value;
	const char *session1_value;
	double		session1_expected;
	const char *session2_value;
	double		session2_expected;
} TestRealGUCSetting;

PG_FUNCTION_INFO_V1(test_session_sort_guc_state_is_session_local);
Datum
test_session_sort_guc_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_trace_sort;
#ifdef DEBUG_BOUNDED_SORT
	char	   *saved_optimize_bounded_sort;
#endif
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_trace_sort = pstrdup(GetConfigOption("trace_sort", false, false));
#ifdef DEBUG_BOUNDED_SORT
	saved_optimize_bounded_sort =
		pstrdup(GetConfigOption("optimize_bounded_sort", false, false));
#endif
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && !trace_sort;
#ifdef DEBUG_BOUNDED_SORT
		ok = ok && optimize_bounded_sort;
#endif
		SetConfigOption("trace_sort", "on",
						PGC_USERSET, PGC_S_SESSION);
#ifdef DEBUG_BOUNDED_SORT
		SetConfigOption("optimize_bounded_sort", "off",
						PGC_USERSET, PGC_S_SESSION);
#endif
		ok = ok && trace_sort;
#ifdef DEBUG_BOUNDED_SORT
		ok = ok && !optimize_bounded_sort;
#endif

		PgSetCurrentSession(&fake_session2);
		ok = ok && !trace_sort;
#ifdef DEBUG_BOUNDED_SORT
		ok = ok && optimize_bounded_sort;
#endif
		SetConfigOption("trace_sort", "off",
						PGC_USERSET, PGC_S_SESSION);
#ifdef DEBUG_BOUNDED_SORT
		SetConfigOption("optimize_bounded_sort", "on",
						PGC_USERSET, PGC_S_SESSION);
#endif
		ok = ok && !trace_sort;
#ifdef DEBUG_BOUNDED_SORT
		ok = ok && optimize_bounded_sort;
#endif

		PgSetCurrentSession(&fake_session1);
		ok = ok && trace_sort;
#ifdef DEBUG_BOUNDED_SORT
		ok = ok && !optimize_bounded_sort;
#endif

		PgSetCurrentSession(&fake_session2);
		ok = ok && !trace_sort;
#ifdef DEBUG_BOUNDED_SORT
		ok = ok && optimize_bounded_sort;
#endif

		PgSetCurrentSession(saved_session);
		SetConfigOption("trace_sort", saved_trace_sort,
						PGC_USERSET, PGC_S_SESSION);
#ifdef DEBUG_BOUNDED_SORT
		SetConfigOption("optimize_bounded_sort",
						saved_optimize_bounded_sort,
						PGC_USERSET, PGC_S_SESSION);
#endif
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("trace_sort", saved_trace_sort,
						PGC_USERSET, PGC_S_SESSION);
#ifdef DEBUG_BOUNDED_SORT
		SetConfigOption("optimize_bounded_sort",
						saved_optimize_bounded_sort,
						PGC_USERSET, PGC_S_SESSION);
#endif
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session sort GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_jit_guc_state_is_session_local);
Datum
test_session_jit_guc_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_jit;
	char	   *saved_jit_dump_bitcode;
	char	   *saved_jit_expressions;
	char	   *saved_jit_tuple_deforming;
	char	   *saved_jit_above_cost;
	char	   *saved_jit_inline_above_cost;
	char	   *saved_jit_optimize_above_cost;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_jit = pstrdup(GetConfigOption("jit", false, false));
	saved_jit_dump_bitcode =
		pstrdup(GetConfigOption("jit_dump_bitcode", false, false));
	saved_jit_expressions =
		pstrdup(GetConfigOption("jit_expressions", false, false));
	saved_jit_tuple_deforming =
		pstrdup(GetConfigOption("jit_tuple_deforming", false, false));
	saved_jit_above_cost =
		pstrdup(GetConfigOption("jit_above_cost", false, false));
	saved_jit_inline_above_cost =
		pstrdup(GetConfigOption("jit_inline_above_cost", false, false));
	saved_jit_optimize_above_cost =
		pstrdup(GetConfigOption("jit_optimize_above_cost", false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && !jit_enabled;
		ok = ok && strcmp(jit_provider, "llvmjit") == 0;
		ok = ok && !jit_debugging_support;
		ok = ok && !jit_dump_bitcode;
		ok = ok && jit_expressions;
		ok = ok && !jit_profiling_support;
		ok = ok && jit_tuple_deforming;
		ok = ok && jit_above_cost == 100000.0;
		ok = ok && jit_inline_above_cost == 500000.0;
		ok = ok && jit_optimize_above_cost == 500000.0;
		SetConfigOption("jit", "on",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_dump_bitcode", "on",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("jit_expressions", "off",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_tuple_deforming", "off",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_above_cost", "11",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_inline_above_cost", "12",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_optimize_above_cost", "13",
						PGC_USERSET, PGC_S_SESSION);
		jit_provider = "session1_jit_provider";
		jit_debugging_support = true;
		jit_profiling_support = true;
		ok = ok && jit_enabled;
		ok = ok && strcmp(jit_provider, "session1_jit_provider") == 0;
		ok = ok && jit_debugging_support;
		ok = ok && jit_dump_bitcode;
		ok = ok && !jit_expressions;
		ok = ok && jit_profiling_support;
		ok = ok && !jit_tuple_deforming;
		ok = ok && jit_above_cost == 11.0;
		ok = ok && jit_inline_above_cost == 12.0;
		ok = ok && jit_optimize_above_cost == 13.0;

		PgSetCurrentSession(&fake_session2);
		ok = ok && !jit_enabled;
		ok = ok && strcmp(jit_provider, "llvmjit") == 0;
		ok = ok && !jit_debugging_support;
		ok = ok && !jit_dump_bitcode;
		ok = ok && jit_expressions;
		ok = ok && !jit_profiling_support;
		ok = ok && jit_tuple_deforming;
		ok = ok && jit_above_cost == 100000.0;
		ok = ok && jit_inline_above_cost == 500000.0;
		ok = ok && jit_optimize_above_cost == 500000.0;
		SetConfigOption("jit", "off",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_dump_bitcode", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("jit_expressions", "on",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_tuple_deforming", "on",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_above_cost", "21",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_inline_above_cost", "22",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_optimize_above_cost", "23",
						PGC_USERSET, PGC_S_SESSION);
		jit_provider = "session2_jit_provider";
		jit_debugging_support = false;
		jit_profiling_support = false;
		ok = ok && !jit_enabled;
		ok = ok && strcmp(jit_provider, "session2_jit_provider") == 0;
		ok = ok && !jit_debugging_support;
		ok = ok && !jit_dump_bitcode;
		ok = ok && jit_expressions;
		ok = ok && !jit_profiling_support;
		ok = ok && jit_tuple_deforming;
		ok = ok && jit_above_cost == 21.0;
		ok = ok && jit_inline_above_cost == 22.0;
		ok = ok && jit_optimize_above_cost == 23.0;

		PgSetCurrentSession(&fake_session1);
		ok = ok && jit_enabled;
		ok = ok && strcmp(jit_provider, "session1_jit_provider") == 0;
		ok = ok && jit_debugging_support;
		ok = ok && jit_dump_bitcode;
		ok = ok && !jit_expressions;
		ok = ok && jit_profiling_support;
		ok = ok && !jit_tuple_deforming;
		ok = ok && jit_above_cost == 11.0;
		ok = ok && jit_inline_above_cost == 12.0;
		ok = ok && jit_optimize_above_cost == 13.0;

		PgSetCurrentSession(&fake_session2);
		ok = ok && !jit_enabled;
		ok = ok && strcmp(jit_provider, "session2_jit_provider") == 0;
		ok = ok && !jit_debugging_support;
		ok = ok && !jit_dump_bitcode;
		ok = ok && jit_expressions;
		ok = ok && !jit_profiling_support;
		ok = ok && jit_tuple_deforming;
		ok = ok && jit_above_cost == 21.0;
		ok = ok && jit_inline_above_cost == 22.0;
		ok = ok && jit_optimize_above_cost == 23.0;

		PgSetCurrentSession(saved_session);
		SetConfigOption("jit_optimize_above_cost",
						saved_jit_optimize_above_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_inline_above_cost",
						saved_jit_inline_above_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_above_cost", saved_jit_above_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_tuple_deforming", saved_jit_tuple_deforming,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_expressions", saved_jit_expressions,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_dump_bitcode", saved_jit_dump_bitcode,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("jit", saved_jit,
						PGC_USERSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("jit_optimize_above_cost",
						saved_jit_optimize_above_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_inline_above_cost",
						saved_jit_inline_above_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_above_cost", saved_jit_above_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_tuple_deforming", saved_jit_tuple_deforming,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_expressions", saved_jit_expressions,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_dump_bitcode", saved_jit_dump_bitcode,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("jit", saved_jit,
						PGC_USERSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session JIT GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

static void
test_jit_provider_reset_after_error_1(void)
{
}

static void
test_jit_provider_reset_after_error_2(void)
{
}

static void
test_jit_provider_release_context_1(JitContext *context)
{
}

static void
test_jit_provider_release_context_2(JitContext *context)
{
}

static bool
test_jit_provider_compile_expr_1(struct ExprState *state)
{
	return false;
}

static bool
test_jit_provider_compile_expr_2(struct ExprState *state)
{
	return false;
}

PG_FUNCTION_INFO_V1(test_session_jit_provider_state_is_session_local);
Datum
test_session_jit_provider_state_is_session_local(PG_FUNCTION_ARGS)
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
		ok = ok && PgCurrentJitProviderCallbacksRef()->reset_after_error == NULL;
		ok = ok && PgCurrentJitProviderCallbacksRef()->release_context == NULL;
		ok = ok && PgCurrentJitProviderCallbacksRef()->compile_expr == NULL;
		ok = ok && !*PgCurrentJitProviderSuccessfullyLoadedRef();
		ok = ok && !*PgCurrentJitProviderFailedLoadingRef();
		PgCurrentJitProviderCallbacksRef()->reset_after_error =
			test_jit_provider_reset_after_error_1;
		PgCurrentJitProviderCallbacksRef()->release_context =
			test_jit_provider_release_context_1;
		PgCurrentJitProviderCallbacksRef()->compile_expr =
			test_jit_provider_compile_expr_1;
		*PgCurrentJitProviderSuccessfullyLoadedRef() = true;
		*PgCurrentJitProviderFailedLoadingRef() = false;

		PgSetCurrentSession(&fake_session2);
		ok = ok && PgCurrentJitProviderCallbacksRef()->reset_after_error == NULL;
		ok = ok && PgCurrentJitProviderCallbacksRef()->release_context == NULL;
		ok = ok && PgCurrentJitProviderCallbacksRef()->compile_expr == NULL;
		ok = ok && !*PgCurrentJitProviderSuccessfullyLoadedRef();
		ok = ok && !*PgCurrentJitProviderFailedLoadingRef();
		PgCurrentJitProviderCallbacksRef()->reset_after_error =
			test_jit_provider_reset_after_error_2;
		PgCurrentJitProviderCallbacksRef()->release_context =
			test_jit_provider_release_context_2;
		PgCurrentJitProviderCallbacksRef()->compile_expr =
			test_jit_provider_compile_expr_2;
		*PgCurrentJitProviderSuccessfullyLoadedRef() = false;
		*PgCurrentJitProviderFailedLoadingRef() = true;

		PgSetCurrentSession(&fake_session1);
		ok = ok && PgCurrentJitProviderCallbacksRef()->reset_after_error ==
			test_jit_provider_reset_after_error_1;
		ok = ok && PgCurrentJitProviderCallbacksRef()->release_context ==
			test_jit_provider_release_context_1;
		ok = ok && PgCurrentJitProviderCallbacksRef()->compile_expr ==
			test_jit_provider_compile_expr_1;
		ok = ok && *PgCurrentJitProviderSuccessfullyLoadedRef();
		ok = ok && !*PgCurrentJitProviderFailedLoadingRef();

		PgSetCurrentSession(&fake_session2);
		ok = ok && PgCurrentJitProviderCallbacksRef()->reset_after_error ==
			test_jit_provider_reset_after_error_2;
		ok = ok && PgCurrentJitProviderCallbacksRef()->release_context ==
			test_jit_provider_release_context_2;
		ok = ok && PgCurrentJitProviderCallbacksRef()->compile_expr ==
			test_jit_provider_compile_expr_2;
		ok = ok && !*PgCurrentJitProviderSuccessfullyLoadedRef();
		ok = ok && *PgCurrentJitProviderFailedLoadingRef();

		PgSetCurrentSession(saved_session);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session JIT provider state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_query_memory_state_is_session_local);
Datum
test_session_query_memory_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_work_mem;
	char	   *saved_hash_mem_multiplier;
	char	   *saved_maintenance_work_mem;
	char	   *saved_max_parallel_maintenance_workers;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_work_mem = pstrdup(GetConfigOption("work_mem", false, false));
	saved_hash_mem_multiplier =
		pstrdup(GetConfigOption("hash_mem_multiplier", false, false));
	saved_maintenance_work_mem =
		pstrdup(GetConfigOption("maintenance_work_mem", false, false));
	saved_max_parallel_maintenance_workers =
		pstrdup(GetConfigOption("max_parallel_maintenance_workers",
								false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && work_mem == 4096;
		ok = ok && hash_mem_multiplier == 2.0;
		ok = ok && maintenance_work_mem == 65536;
		ok = ok && max_parallel_maintenance_workers == 2;
		SetConfigOption("work_mem", "8MB", PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("hash_mem_multiplier", "3",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("maintenance_work_mem", "128MB",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("max_parallel_maintenance_workers", "3",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && work_mem == 8192;
		ok = ok && hash_mem_multiplier == 3.0;
		ok = ok && maintenance_work_mem == 131072;
		ok = ok && max_parallel_maintenance_workers == 3;

		PgSetCurrentSession(&fake_session2);
		ok = ok && work_mem == 4096;
		ok = ok && hash_mem_multiplier == 2.0;
		ok = ok && maintenance_work_mem == 65536;
		ok = ok && max_parallel_maintenance_workers == 2;
		SetConfigOption("work_mem", "9MB", PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("hash_mem_multiplier", "4",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("maintenance_work_mem", "96MB",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("max_parallel_maintenance_workers", "1",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && work_mem == 9216;
		ok = ok && hash_mem_multiplier == 4.0;
		ok = ok && maintenance_work_mem == 98304;
		ok = ok && max_parallel_maintenance_workers == 1;

		PgSetCurrentSession(&fake_session1);
		ok = ok && work_mem == 8192;
		ok = ok && hash_mem_multiplier == 3.0;
		ok = ok && maintenance_work_mem == 131072;
		ok = ok && max_parallel_maintenance_workers == 3;

		PgSetCurrentSession(&fake_session2);
		ok = ok && work_mem == 9216;
		ok = ok && hash_mem_multiplier == 4.0;
		ok = ok && maintenance_work_mem == 98304;
		ok = ok && max_parallel_maintenance_workers == 1;

		PgSetCurrentSession(saved_session);
		SetConfigOption("work_mem", saved_work_mem, PGC_USERSET,
						PGC_S_SESSION);
		SetConfigOption("hash_mem_multiplier", saved_hash_mem_multiplier,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("maintenance_work_mem", saved_maintenance_work_mem,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("max_parallel_maintenance_workers",
						saved_max_parallel_maintenance_workers,
						PGC_USERSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("work_mem", saved_work_mem, PGC_USERSET,
						PGC_S_SESSION);
		SetConfigOption("hash_mem_multiplier", saved_hash_mem_multiplier,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("maintenance_work_mem", saved_maintenance_work_mem,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("max_parallel_maintenance_workers",
						saved_max_parallel_maintenance_workers,
						PGC_USERSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session query memory GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_planner_cost_state_is_session_local);
Datum
test_session_planner_cost_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_seq_page_cost;
	char	   *saved_random_page_cost;
	char	   *saved_cpu_tuple_cost;
	char	   *saved_cpu_index_tuple_cost;
	char	   *saved_cpu_operator_cost;
	char	   *saved_parallel_tuple_cost;
	char	   *saved_parallel_setup_cost;
	char	   *saved_recursive_worktable_factor;
	char	   *saved_effective_cache_size;
	char	   *saved_max_parallel_workers_per_gather;
	char	   *saved_debug_parallel_query;
	char	   *saved_parallel_leader_participation;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_seq_page_cost = pstrdup(GetConfigOption("seq_page_cost",
												  false, false));
	saved_random_page_cost = pstrdup(GetConfigOption("random_page_cost",
													 false, false));
	saved_cpu_tuple_cost = pstrdup(GetConfigOption("cpu_tuple_cost",
												   false, false));
	saved_cpu_index_tuple_cost =
		pstrdup(GetConfigOption("cpu_index_tuple_cost", false, false));
	saved_cpu_operator_cost =
		pstrdup(GetConfigOption("cpu_operator_cost", false, false));
	saved_parallel_tuple_cost =
		pstrdup(GetConfigOption("parallel_tuple_cost", false, false));
	saved_parallel_setup_cost =
		pstrdup(GetConfigOption("parallel_setup_cost", false, false));
	saved_recursive_worktable_factor =
		pstrdup(GetConfigOption("recursive_worktable_factor", false, false));
	saved_effective_cache_size =
		pstrdup(GetConfigOption("effective_cache_size", false, false));
	saved_max_parallel_workers_per_gather =
		pstrdup(GetConfigOption("max_parallel_workers_per_gather",
								false, false));
	saved_debug_parallel_query =
		pstrdup(GetConfigOption("debug_parallel_query", false, false));
	saved_parallel_leader_participation =
		pstrdup(GetConfigOption("parallel_leader_participation",
								false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && seq_page_cost == DEFAULT_SEQ_PAGE_COST;
		ok = ok && random_page_cost == DEFAULT_RANDOM_PAGE_COST;
		ok = ok && cpu_tuple_cost == DEFAULT_CPU_TUPLE_COST;
		ok = ok && cpu_index_tuple_cost == DEFAULT_CPU_INDEX_TUPLE_COST;
		ok = ok && cpu_operator_cost == DEFAULT_CPU_OPERATOR_COST;
		ok = ok && parallel_tuple_cost == DEFAULT_PARALLEL_TUPLE_COST;
		ok = ok && parallel_setup_cost == DEFAULT_PARALLEL_SETUP_COST;
		ok = ok && recursive_worktable_factor ==
			DEFAULT_RECURSIVE_WORKTABLE_FACTOR;
		ok = ok && effective_cache_size == DEFAULT_EFFECTIVE_CACHE_SIZE;
		ok = ok && disable_cost == 1.0e10;
		ok = ok && max_parallel_workers_per_gather == 2;
		ok = ok && debug_parallel_query == DEBUG_PARALLEL_OFF;
		ok = ok && parallel_leader_participation;
		SetConfigOption("seq_page_cost", "1.25",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("random_page_cost", "3.5",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("cpu_tuple_cost", "0.02",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("cpu_index_tuple_cost", "0.01",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("cpu_operator_cost", "0.005",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("parallel_tuple_cost", "0.2",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("parallel_setup_cost", "2000",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("recursive_worktable_factor", "12",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("effective_cache_size", "4096",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("max_parallel_workers_per_gather", "3",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_parallel_query", "regress",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("parallel_leader_participation", "off",
						PGC_USERSET, PGC_S_SESSION);
		disable_cost = 42.0;
		ok = ok && seq_page_cost == 1.25;
		ok = ok && random_page_cost == 3.5;
		ok = ok && cpu_tuple_cost == 0.02;
		ok = ok && cpu_index_tuple_cost == 0.01;
		ok = ok && cpu_operator_cost == 0.005;
		ok = ok && parallel_tuple_cost == 0.2;
		ok = ok && parallel_setup_cost == 2000.0;
		ok = ok && recursive_worktable_factor == 12.0;
		ok = ok && effective_cache_size == 4096;
		ok = ok && disable_cost == 42.0;
		ok = ok && max_parallel_workers_per_gather == 3;
		ok = ok && debug_parallel_query == DEBUG_PARALLEL_REGRESS;
		ok = ok && !parallel_leader_participation;

		PgSetCurrentSession(&fake_session2);
		ok = ok && seq_page_cost == DEFAULT_SEQ_PAGE_COST;
		ok = ok && random_page_cost == DEFAULT_RANDOM_PAGE_COST;
		ok = ok && cpu_tuple_cost == DEFAULT_CPU_TUPLE_COST;
		ok = ok && cpu_index_tuple_cost == DEFAULT_CPU_INDEX_TUPLE_COST;
		ok = ok && cpu_operator_cost == DEFAULT_CPU_OPERATOR_COST;
		ok = ok && parallel_tuple_cost == DEFAULT_PARALLEL_TUPLE_COST;
		ok = ok && parallel_setup_cost == DEFAULT_PARALLEL_SETUP_COST;
		ok = ok && recursive_worktable_factor ==
			DEFAULT_RECURSIVE_WORKTABLE_FACTOR;
		ok = ok && effective_cache_size == DEFAULT_EFFECTIVE_CACHE_SIZE;
		ok = ok && disable_cost == 1.0e10;
		ok = ok && max_parallel_workers_per_gather == 2;
		ok = ok && debug_parallel_query == DEBUG_PARALLEL_OFF;
		ok = ok && parallel_leader_participation;
		SetConfigOption("seq_page_cost", "1.5",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("random_page_cost", "2.5",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("cpu_tuple_cost", "0.03",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("cpu_index_tuple_cost", "0.015",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("cpu_operator_cost", "0.0075",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("parallel_tuple_cost", "0.3",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("parallel_setup_cost", "3000",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("recursive_worktable_factor", "13",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("effective_cache_size", "8192",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("max_parallel_workers_per_gather", "1",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_parallel_query", "on",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("parallel_leader_participation", "on",
						PGC_USERSET, PGC_S_SESSION);
		disable_cost = 84.0;
		ok = ok && seq_page_cost == 1.5;
		ok = ok && random_page_cost == 2.5;
		ok = ok && cpu_tuple_cost == 0.03;
		ok = ok && cpu_index_tuple_cost == 0.015;
		ok = ok && cpu_operator_cost == 0.0075;
		ok = ok && parallel_tuple_cost == 0.3;
		ok = ok && parallel_setup_cost == 3000.0;
		ok = ok && recursive_worktable_factor == 13.0;
		ok = ok && effective_cache_size == 8192;
		ok = ok && disable_cost == 84.0;
		ok = ok && max_parallel_workers_per_gather == 1;
		ok = ok && debug_parallel_query == DEBUG_PARALLEL_ON;
		ok = ok && parallel_leader_participation;

		PgSetCurrentSession(&fake_session1);
		ok = ok && seq_page_cost == 1.25;
		ok = ok && random_page_cost == 3.5;
		ok = ok && cpu_tuple_cost == 0.02;
		ok = ok && cpu_index_tuple_cost == 0.01;
		ok = ok && cpu_operator_cost == 0.005;
		ok = ok && parallel_tuple_cost == 0.2;
		ok = ok && parallel_setup_cost == 2000.0;
		ok = ok && recursive_worktable_factor == 12.0;
		ok = ok && effective_cache_size == 4096;
		ok = ok && disable_cost == 42.0;
		ok = ok && max_parallel_workers_per_gather == 3;
		ok = ok && debug_parallel_query == DEBUG_PARALLEL_REGRESS;
		ok = ok && !parallel_leader_participation;

		PgSetCurrentSession(saved_session);
		SetConfigOption("seq_page_cost", saved_seq_page_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("random_page_cost", saved_random_page_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("cpu_tuple_cost", saved_cpu_tuple_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("cpu_index_tuple_cost", saved_cpu_index_tuple_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("cpu_operator_cost", saved_cpu_operator_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("parallel_tuple_cost", saved_parallel_tuple_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("parallel_setup_cost", saved_parallel_setup_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("recursive_worktable_factor",
						saved_recursive_worktable_factor,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("effective_cache_size", saved_effective_cache_size,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("max_parallel_workers_per_gather",
						saved_max_parallel_workers_per_gather,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_parallel_query", saved_debug_parallel_query,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("parallel_leader_participation",
						saved_parallel_leader_participation,
						PGC_USERSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("seq_page_cost", saved_seq_page_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("random_page_cost", saved_random_page_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("cpu_tuple_cost", saved_cpu_tuple_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("cpu_index_tuple_cost", saved_cpu_index_tuple_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("cpu_operator_cost", saved_cpu_operator_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("parallel_tuple_cost", saved_parallel_tuple_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("parallel_setup_cost", saved_parallel_setup_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("recursive_worktable_factor",
						saved_recursive_worktable_factor,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("effective_cache_size", saved_effective_cache_size,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("max_parallel_workers_per_gather",
						saved_max_parallel_workers_per_gather,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_parallel_query", saved_debug_parallel_query,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("parallel_leader_participation",
						saved_parallel_leader_participation,
						PGC_USERSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session planner cost GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_planner_method_state_is_session_local);
Datum
test_session_planner_method_state_is_session_local(PG_FUNCTION_ARGS)
{
	static const TestBoolGUCSetting bool_settings[] = {
		{"enable_async_append", PgCurrentEnableAsyncAppendRef, true,
			"off", false, "on", true},
		{"enable_bitmapscan", PgCurrentEnableBitmapscanRef, true,
			"off", false, "on", true},
		{"enable_distinct_reordering", PgCurrentEnableDistinctReorderingRef,
			true, "off", false, "on", true},
		{"enable_eager_aggregate", PgCurrentEnableEagerAggregateRef, true,
			"off", false, "on", true},
		{"enable_gathermerge", PgCurrentEnableGathermergeRef, true,
			"off", false, "on", true},
		{"enable_group_by_reordering", PgCurrentEnableGroupByReorderingRef,
			true, "off", false, "on", true},
		{"enable_hashagg", PgCurrentEnableHashaggRef, true,
			"off", false, "on", true},
		{"enable_hashjoin", PgCurrentEnableHashjoinRef, true,
			"off", false, "on", true},
		{"enable_incremental_sort", PgCurrentEnableIncrementalSortRef, true,
			"off", false, "on", true},
		{"enable_indexonlyscan", PgCurrentEnableIndexonlyscanRef, true,
			"off", false, "on", true},
		{"enable_indexscan", PgCurrentEnableIndexscanRef, true,
			"off", false, "on", true},
		{"enable_material", PgCurrentEnableMaterialRef, true,
			"off", false, "on", true},
		{"enable_memoize", PgCurrentEnableMemoizeRef, true,
			"off", false, "on", true},
		{"enable_mergejoin", PgCurrentEnableMergejoinRef, true,
			"off", false, "on", true},
		{"enable_nestloop", PgCurrentEnableNestloopRef, true,
			"off", false, "on", true},
		{"enable_parallel_append", PgCurrentEnableParallelAppendRef, true,
			"off", false, "on", true},
		{"enable_parallel_hash", PgCurrentEnableParallelHashRef, true,
			"off", false, "on", true},
		{"enable_partition_pruning", PgCurrentEnablePartitionPruningRef, true,
			"off", false, "on", true},
		{"enable_partitionwise_aggregate",
			PgCurrentEnablePartitionwiseAggregateRef, false,
			"on", true, "off", false},
		{"enable_partitionwise_join", PgCurrentEnablePartitionwiseJoinRef,
			false, "on", true, "off", false},
		{"enable_presorted_aggregate", PgCurrentEnablePresortedAggregateRef,
			true, "off", false, "on", true},
		{"enable_self_join_elimination",
			PgCurrentEnableSelfJoinEliminationRef, true,
			"off", false, "on", true},
		{"enable_seqscan", PgCurrentEnableSeqscanRef, true,
			"off", false, "on", true},
		{"enable_sort", PgCurrentEnableSortRef, true,
			"off", false, "on", true},
		{"enable_tidscan", PgCurrentEnableTidscanRef, true,
			"off", false, "on", true},
		{"geqo", PgCurrentEnableGeqoRef, true,
			"off", false, "on", true}
	};
	static const TestIntGUCSetting int_settings[] = {
		{"constraint_exclusion", PgCurrentConstraintExclusionRef,
			CONSTRAINT_EXCLUSION_PARTITION,
			"off", CONSTRAINT_EXCLUSION_OFF, "on", CONSTRAINT_EXCLUSION_ON},
		{"from_collapse_limit", PgCurrentFromCollapseLimitRef, 8,
			"4", 4, "5", 5},
		{"geqo_effort", PgCurrentGeqoEffortRef, DEFAULT_GEQO_EFFORT,
			"6", 6, "7", 7},
		{"geqo_generations", PgCurrentGeqoGenerationsRef, 0,
			"20", 20, "22", 22},
		{"geqo_pool_size", PgCurrentGeqoPoolSizeRef, 0,
			"10", 10, "12", 12},
		{"geqo_threshold", PgCurrentGeqoThresholdRef, 12,
			"13", 13, "14", 14},
		{"join_collapse_limit", PgCurrentJoinCollapseLimitRef, 8,
			"6", 6, "7", 7},
		{"min_parallel_index_scan_size",
			PgCurrentMinParallelIndexScanSizeRef,
			(512 * 1024) / BLCKSZ, "32", 32, "64", 64},
		{"min_parallel_table_scan_size",
			PgCurrentMinParallelTableScanSizeRef,
			(8 * 1024 * 1024) / BLCKSZ, "64", 64, "128", 128}
	};
	static const TestRealGUCSetting real_settings[] = {
		{"cursor_tuple_fraction", PgCurrentCursorTupleFractionRef,
			DEFAULT_CURSOR_TUPLE_FRACTION, "0.25", 0.25, "0.75", 0.75},
		{"geqo_seed", PgCurrentGeqoSeedRef, 0.0, "0.11", 0.11,
			"0.22", 0.22},
		{"geqo_selection_bias", PgCurrentGeqoSelectionBiasRef,
			DEFAULT_GEQO_SELECTION_BIAS, "1.75", 1.75, "2.0", 2.0},
		{"min_eager_agg_group_size", PgCurrentMinEagerAggGroupSizeRef,
			8.0, "5.5", 5.5, "9.5", 9.5}
	};
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_bool_values[lengthof(bool_settings)];
	char	   *saved_int_values[lengthof(int_settings)];
	char	   *saved_real_values[lengthof(real_settings)];
	bool		ok = true;
	int			i;

	saved_session = CurrentPgSession;
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	for (i = 0; i < lengthof(bool_settings); i++)
		saved_bool_values[i] =
			pstrdup(GetConfigOption(bool_settings[i].name, false, false));
	for (i = 0; i < lengthof(int_settings); i++)
		saved_int_values[i] =
			pstrdup(GetConfigOption(int_settings[i].name, false, false));
	for (i = 0; i < lengthof(real_settings); i++)
		saved_real_values[i] =
			pstrdup(GetConfigOption(real_settings[i].name, false, false));

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		for (i = 0; i < lengthof(bool_settings); i++)
		{
			ok = ok && *bool_settings[i].ref() ==
				bool_settings[i].default_value;
			SetConfigOption(bool_settings[i].name,
							bool_settings[i].session1_value,
							PGC_USERSET, PGC_S_SESSION);
			ok = ok && *bool_settings[i].ref() ==
				bool_settings[i].session1_expected;
		}
		for (i = 0; i < lengthof(int_settings); i++)
		{
			ok = ok && *int_settings[i].ref() ==
				int_settings[i].default_value;
			SetConfigOption(int_settings[i].name,
							int_settings[i].session1_value,
							PGC_USERSET, PGC_S_SESSION);
			ok = ok && *int_settings[i].ref() ==
				int_settings[i].session1_expected;
		}
		for (i = 0; i < lengthof(real_settings); i++)
		{
			ok = ok && *real_settings[i].ref() ==
				real_settings[i].default_value;
			SetConfigOption(real_settings[i].name,
							real_settings[i].session1_value,
							PGC_USERSET, PGC_S_SESSION);
			ok = ok && *real_settings[i].ref() ==
				real_settings[i].session1_expected;
		}
		Geqo_planner_extension_id = 17;
		ok = ok && Geqo_planner_extension_id == 17;

		PgSetCurrentSession(&fake_session2);
		for (i = 0; i < lengthof(bool_settings); i++)
		{
			ok = ok && *bool_settings[i].ref() ==
				bool_settings[i].default_value;
			SetConfigOption(bool_settings[i].name,
							bool_settings[i].session2_value,
							PGC_USERSET, PGC_S_SESSION);
			ok = ok && *bool_settings[i].ref() ==
				bool_settings[i].session2_expected;
		}
		for (i = 0; i < lengthof(int_settings); i++)
		{
			ok = ok && *int_settings[i].ref() ==
				int_settings[i].default_value;
			SetConfigOption(int_settings[i].name,
							int_settings[i].session2_value,
							PGC_USERSET, PGC_S_SESSION);
			ok = ok && *int_settings[i].ref() ==
				int_settings[i].session2_expected;
		}
		for (i = 0; i < lengthof(real_settings); i++)
		{
			ok = ok && *real_settings[i].ref() ==
				real_settings[i].default_value;
			SetConfigOption(real_settings[i].name,
							real_settings[i].session2_value,
							PGC_USERSET, PGC_S_SESSION);
			ok = ok && *real_settings[i].ref() ==
				real_settings[i].session2_expected;
		}
		Geqo_planner_extension_id = 23;
		ok = ok && Geqo_planner_extension_id == 23;

		PgSetCurrentSession(&fake_session1);
		for (i = 0; i < lengthof(bool_settings); i++)
			ok = ok && *bool_settings[i].ref() ==
				bool_settings[i].session1_expected;
		for (i = 0; i < lengthof(int_settings); i++)
			ok = ok && *int_settings[i].ref() ==
				int_settings[i].session1_expected;
		for (i = 0; i < lengthof(real_settings); i++)
			ok = ok && *real_settings[i].ref() ==
				real_settings[i].session1_expected;
		ok = ok && Geqo_planner_extension_id == 17;

		PgSetCurrentSession(saved_session);
		for (i = 0; i < lengthof(bool_settings); i++)
			SetConfigOption(bool_settings[i].name, saved_bool_values[i],
							PGC_USERSET, PGC_S_SESSION);
		for (i = 0; i < lengthof(int_settings); i++)
			SetConfigOption(int_settings[i].name, saved_int_values[i],
							PGC_USERSET, PGC_S_SESSION);
		for (i = 0; i < lengthof(real_settings); i++)
			SetConfigOption(real_settings[i].name, saved_real_values[i],
							PGC_USERSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		for (i = 0; i < lengthof(bool_settings); i++)
			SetConfigOption(bool_settings[i].name, saved_bool_values[i],
							PGC_USERSET, PGC_S_SESSION);
		for (i = 0; i < lengthof(int_settings); i++)
			SetConfigOption(int_settings[i].name, saved_int_values[i],
							PGC_USERSET, PGC_S_SESSION);
		for (i = 0; i < lengthof(real_settings); i++)
			SetConfigOption(real_settings[i].name, saved_real_values[i],
							PGC_USERSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session planner method GUC state was not session-local");

	PG_RETURN_BOOL(true);
}
