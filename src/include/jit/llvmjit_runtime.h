/*-------------------------------------------------------------------------
 *
 * llvmjit_runtime.h
 *	  Runtime state bucket for the LLVM JIT provider.
 *
 * This header is intentionally small because LLVM C++ source files include
 * llvmjit.h before LLVM C++ headers.  Do not include backend_runtime.h here:
 * it pulls PostgreSQL compatibility macros that collide with LLVM names.
 *
 * Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/include/jit/llvmjit_runtime.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LLVMJIT_RUNTIME_H
#define LLVMJIT_RUNTIME_H

#include <stdbool.h>
#include <stddef.h>

#ifdef USE_LLVM

typedef struct LLVMOpaqueContext *LLVMContextRef;
typedef struct LLVMOpaqueModule *LLVMModuleRef;
typedef struct LLVMTarget *LLVMTargetRef;
typedef struct LLVMOpaqueType *LLVMTypeRef;
typedef struct LLVMOpaqueValue *LLVMValueRef;
typedef struct LLVMOrcOpaqueLLJIT *LLVMOrcLLJITRef;
typedef struct LLVMOrcOpaqueThreadSafeContext *LLVMOrcThreadSafeContextRef;

typedef struct PgSessionLLVMJitState
{
	LLVMTypeRef type_param_bool;
	LLVMTypeRef type_pg_function;
	LLVMTypeRef type_size_t;
	LLVMTypeRef type_datum;
	LLVMTypeRef type_storage_bool;
	LLVMTypeRef struct_nullable_datum;
	LLVMTypeRef struct_tuple_desc_data;
	LLVMTypeRef struct_heap_tuple_data;
	LLVMTypeRef struct_heap_tuple_header_data;
	LLVMTypeRef struct_minimal_tuple_data;
	LLVMTypeRef struct_tuple_table_slot;
	LLVMTypeRef struct_heap_tuple_table_slot;
	LLVMTypeRef struct_minimal_tuple_table_slot;
	LLVMTypeRef struct_memory_context_data;
	LLVMTypeRef struct_function_call_info_data;
	LLVMTypeRef struct_expr_context;
	LLVMTypeRef struct_expr_eval_step;
	LLVMTypeRef struct_expr_state;
	LLVMTypeRef struct_agg_state;
	LLVMTypeRef struct_agg_state_per_trans_data;
	LLVMTypeRef struct_agg_state_per_group_data;
	LLVMTypeRef struct_plan_state;
	LLVMValueRef attribute_template;
	LLVMValueRef exec_eval_bool_subroutine_template;
	LLVMValueRef exec_eval_subroutine_template;
	LLVMModuleRef types_module;
	bool		session_initialized;
	size_t		generation;
	size_t		jit_context_in_use_count;
	size_t		llvm_context_reuse_count;
	const char *triple;
	const char *layout;
	LLVMContextRef context;
	LLVMTargetRef targetref;
	LLVMOrcThreadSafeContextRef ts_context;
	LLVMOrcLLJITRef opt0_orc;
	LLVMOrcLLJITRef opt3_orc;
} PgSessionLLVMJitState;

extern PgSessionLLVMJitState *PgCurrentLLVMJitState(void);

#else

typedef struct PgSessionLLVMJitState
{
	bool		not_available;
} PgSessionLLVMJitState;

#endif

#endif							/* LLVMJIT_RUNTIME_H */
