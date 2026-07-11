/*-------------------------------------------------------------------------
 * jit.h
 *	  Provider independent JIT infrastructure.
 *
 * Copyright (c) 2016-2026, PostgreSQL Global Development Group
 *
 * src/include/jit/jit.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef JIT_H
#define JIT_H

#include "executor/instrument.h"
#include "utils/global_lifetime.h"
#include "utils/resowner.h"


/* Flags determining what kind of JIT operations to perform */
#define PGJIT_NONE     0
#define PGJIT_PERFORM  (1 << 0)
#define PGJIT_OPT3     (1 << 1)
#define PGJIT_INLINE   (1 << 2)
#define PGJIT_EXPR	   (1 << 3)
#define PGJIT_DEFORM   (1 << 4)


typedef struct JitInstrumentation
{
	/* number of emitted functions */
	size_t		created_functions;

	/* accumulated time to generate code */
	instr_time	generation_counter;

	/* accumulated time to deform tuples, included into generation_counter */
	instr_time	deform_counter;

	/* accumulated time for inlining */
	instr_time	inlining_counter;

	/* accumulated time for optimization */
	instr_time	optimization_counter;

	/* accumulated time for code emission */
	instr_time	emission_counter;
} JitInstrumentation;

/*
 * DSM structure for accumulating jit instrumentation of all workers.
 */
typedef struct SharedJitInstrumentation
{
	int			num_workers;
	JitInstrumentation jit_instr[FLEXIBLE_ARRAY_MEMBER];
} SharedJitInstrumentation;

typedef struct JitContext
{
	/* see PGJIT_* above */
	int			flags;

	JitInstrumentation instr;
} JitContext;

typedef struct JitProviderCallbacks JitProviderCallbacks;

extern PGDLLEXPORT void _PG_jit_provider_init(JitProviderCallbacks *cb);
typedef void (*JitProviderInit) (JitProviderCallbacks *cb);
typedef void (*JitProviderResetAfterErrorCB) (void);
typedef void (*JitProviderReleaseContextCB) (JitContext *context);
struct ExprState;
typedef bool (*JitProviderCompileExprCB) (struct ExprState *state);

struct JitProviderCallbacks
{
	JitProviderResetAfterErrorCB reset_after_error;
	JitProviderReleaseContextCB release_context;
	JitProviderCompileExprCB compile_expr;
};


/* GUCs */
#ifndef PgCurrentJitEnabledRef
extern bool *PgCurrentJitEnabledRef(void);
#endif
#ifndef PgCurrentJitProviderRef
extern char **PgCurrentJitProviderRef(void);
#endif
#ifndef PgCurrentJitDebuggingSupportRef
extern bool *PgCurrentJitDebuggingSupportRef(void);
#endif
#ifndef PgCurrentJitDumpBitcodeRef
extern bool *PgCurrentJitDumpBitcodeRef(void);
#endif
#ifndef PgCurrentJitExpressionsRef
extern bool *PgCurrentJitExpressionsRef(void);
#endif
#ifndef PgCurrentJitProfilingSupportRef
extern bool *PgCurrentJitProfilingSupportRef(void);
#endif
#ifndef PgCurrentJitTupleDeformingRef
extern bool *PgCurrentJitTupleDeformingRef(void);
#endif
#ifndef PgCurrentJitAboveCostRef
extern double *PgCurrentJitAboveCostRef(void);
#endif
#ifndef PgCurrentJitInlineAboveCostRef
extern double *PgCurrentJitInlineAboveCostRef(void);
#endif
#ifndef PgCurrentJitOptimizeAboveCostRef
extern double *PgCurrentJitOptimizeAboveCostRef(void);
#endif
#ifndef PgCurrentJitProviderCallbacksRef
extern JitProviderCallbacks *PgCurrentJitProviderCallbacksRef(void);
#endif
#ifndef PgCurrentJitProviderSuccessfullyLoadedRef
extern bool *PgCurrentJitProviderSuccessfullyLoadedRef(void);
#endif
#ifndef PgCurrentJitProviderFailedLoadingRef
extern bool *PgCurrentJitProviderFailedLoadingRef(void);
#endif
#define jit_enabled (*PgCurrentJitEnabledRef())
#define jit_provider (*PgCurrentJitProviderRef())
#define jit_debugging_support (*PgCurrentJitDebuggingSupportRef())
#define jit_dump_bitcode (*PgCurrentJitDumpBitcodeRef())
#define jit_expressions (*PgCurrentJitExpressionsRef())
#define jit_profiling_support (*PgCurrentJitProfilingSupportRef())
#define jit_tuple_deforming (*PgCurrentJitTupleDeformingRef())
#define jit_above_cost (*PgCurrentJitAboveCostRef())
#define jit_inline_above_cost (*PgCurrentJitInlineAboveCostRef())
#define jit_optimize_above_cost (*PgCurrentJitOptimizeAboveCostRef())


extern void jit_reset_after_error(void);
extern void jit_release_context(JitContext *context);

/*
 * Functions for attempting to JIT code. Callers must accept that these might
 * not be able to perform JIT (i.e. return false).
 */
extern bool jit_compile_expr(struct ExprState *state);
extern void InstrJitAgg(JitInstrumentation *dst, JitInstrumentation *add);


#endif							/* JIT_H */
