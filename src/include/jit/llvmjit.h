/*-------------------------------------------------------------------------
 * llvmjit.h
 *	  LLVM JIT provider.
 *
 * Copyright (c) 2016-2026, PostgreSQL Global Development Group
 *
 * src/include/jit/llvmjit.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LLVMJIT_H
#define LLVMJIT_H

/*
 * To avoid breaking cpluspluscheck, allow including the file even when LLVM
 * is not available.
 */
#ifdef USE_LLVM

#include "jit/llvmjit_backport.h"

/*
 * PostgreSQL has a few historical short-name macros that collide with LLVM C
 * headers in newer LLVM releases.  llvmjit sources should not rely on these
 * names after including this header.
 */
#ifdef AM
#undef AM
#endif
#ifdef PM
#undef PM
#endif
#ifdef TZ
#undef TZ
#endif
#ifdef Mode
#undef Mode
#endif

#include <llvm-c/Types.h>
#ifdef USE_LLVM_BACKPORT_SECTION_MEMORY_MANAGER
#include <llvm-c/OrcEE.h>
#endif


/*
 * File needs to be includable by both C and C++ code, and include other
 * headers doing the same. Therefore wrap C portion in our own extern "C" if
 * in C++ mode.
 */
#ifdef __cplusplus
extern "C"
{
#endif

#include "access/tupdesc.h"
#include "fmgr.h"
#include "jit/jit.h"
#include "jit/llvmjit_runtime.h"
#include "nodes/pg_list.h"

typedef struct LLVMJitContext
{
	JitContext	base;

	/* used to ensure cleanup of context */
	ResourceOwner resowner;

	/* number of modules created */
	size_t		module_generation;

	/*
	 * The LLVM Context used by this JIT context. An LLVM context is reused
	 * across many compilations, but occasionally reset to prevent it using
	 * too much memory due to more and more types accumulating.
	 */
	LLVMContextRef llvm_context;

	/* current, "open for write", module */
	LLVMModuleRef module;

	/* is there any pending code that needs to be emitted */
	bool		compiled;

	/* # of objects emitted, used to generate non-conflicting names */
	int			counter;

	/* list of handles for code emitted via Orc */
	List	   *handles;
} LLVMJitContext;

/* type and struct definitions */
#define TypeParamBool (PgCurrentLLVMJitState()->type_param_bool)
#define TypePGFunction (PgCurrentLLVMJitState()->type_pg_function)
#define TypeSizeT (PgCurrentLLVMJitState()->type_size_t)
#define TypeDatum (PgCurrentLLVMJitState()->type_datum)
#define TypeStorageBool (PgCurrentLLVMJitState()->type_storage_bool)

#define StructNullableDatum (PgCurrentLLVMJitState()->struct_nullable_datum)
#define StructTupleDescData (PgCurrentLLVMJitState()->struct_tuple_desc_data)
#define StructHeapTupleData (PgCurrentLLVMJitState()->struct_heap_tuple_data)
#define StructHeapTupleHeaderData (PgCurrentLLVMJitState()->struct_heap_tuple_header_data)
#define StructMinimalTupleData (PgCurrentLLVMJitState()->struct_minimal_tuple_data)
#define StructTupleTableSlot (PgCurrentLLVMJitState()->struct_tuple_table_slot)
#define StructHeapTupleTableSlot (PgCurrentLLVMJitState()->struct_heap_tuple_table_slot)
#define StructMinimalTupleTableSlot (PgCurrentLLVMJitState()->struct_minimal_tuple_table_slot)
#define StructMemoryContextData (PgCurrentLLVMJitState()->struct_memory_context_data)
#define StructFunctionCallInfoData (PgCurrentLLVMJitState()->struct_function_call_info_data)
#define StructExprContext (PgCurrentLLVMJitState()->struct_expr_context)
#define StructExprEvalStep (PgCurrentLLVMJitState()->struct_expr_eval_step)
#define StructExprState (PgCurrentLLVMJitState()->struct_expr_state)
#define StructAggState (PgCurrentLLVMJitState()->struct_agg_state)
#define StructAggStatePerTransData (PgCurrentLLVMJitState()->struct_agg_state_per_trans_data)
#define StructAggStatePerGroupData (PgCurrentLLVMJitState()->struct_agg_state_per_group_data)
#define StructPlanState (PgCurrentLLVMJitState()->struct_plan_state)

#define AttributeTemplate (PgCurrentLLVMJitState()->attribute_template)
#define ExecEvalBoolSubroutineTemplate (PgCurrentLLVMJitState()->exec_eval_bool_subroutine_template)
#define ExecEvalSubroutineTemplate (PgCurrentLLVMJitState()->exec_eval_subroutine_template)

extern void llvm_enter_fatal_on_oom(void);
extern void llvm_leave_fatal_on_oom(void);
extern bool llvm_in_fatal_on_oom(void);
extern void llvm_reset_after_error(void);
extern void llvm_assert_in_fatal_section(void);

extern LLVMJitContext *llvm_create_context(int jitFlags);
extern LLVMModuleRef llvm_mutable_module(LLVMJitContext *context);
extern char *llvm_expand_funcname(LLVMJitContext *context, const char *basename);
extern void *llvm_get_function(LLVMJitContext *context, const char *funcname);
extern void llvm_split_symbol_name(const char *name, char **modname, char **funcname);
extern LLVMTypeRef llvm_pg_var_type(const char *varname);
extern LLVMTypeRef llvm_pg_var_func_type(const char *varname);
extern LLVMValueRef llvm_pg_func(LLVMModuleRef mod, const char *funcname);
extern void llvm_copy_attributes(LLVMValueRef v_from, LLVMValueRef v_to);
extern LLVMValueRef llvm_function_reference(LLVMJitContext *context,
						LLVMBuilderRef builder,
						LLVMModuleRef mod,
						FunctionCallInfo fcinfo);

extern void llvm_inline_reset_caches(void);
extern void llvm_inline(LLVMModuleRef mod);

/*
 ****************************************************************************
 * Code generation functions.
 ****************************************************************************
 */
extern bool llvm_compile_expr(struct ExprState *state);
struct TupleTableSlotOps;
extern LLVMValueRef slot_compile_deform(struct LLVMJitContext *context, TupleDesc desc,
										const struct TupleTableSlotOps *ops, int natts);

/*
 ****************************************************************************
 * Extensions / Backward compatibility section of the LLVM C API
 * Error handling related functions.
 ****************************************************************************
 */
extern LLVMTypeRef LLVMGetFunctionReturnType(LLVMValueRef r);
extern LLVMTypeRef LLVMGetFunctionType(LLVMValueRef r);
#ifdef USE_LLVM_BACKPORT_SECTION_MEMORY_MANAGER
extern LLVMOrcObjectLayerRef LLVMOrcCreateRTDyldObjectLinkingLayerWithSafeSectionMemoryManager(LLVMOrcExecutionSessionRef ES);
#endif

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif							/* USE_LLVM */
#endif							/* LLVMJIT_H */
