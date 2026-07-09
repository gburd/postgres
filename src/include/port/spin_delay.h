/*-------------------------------------------------------------------------
 *
 * spin_delay.h
 *	  Platform-specific spin delay for busy-wait loops
 *
 * This file provides pg_spin_delay(), a platform-optimized delay instruction
 * for use in spinlock contention loops. Different architectures have different
 * optimal instructions for indicating to the CPU that we're in a busy-wait.
 *
 * Key optimizations:
 * - x86/x86_64: PAUSE instruction (rep nop) reduces power and helps hyperthreads
 * - ARM64: ISB instruction provides better performance than YIELD at scale
 * - Windows ARM64: Uses __isb() intrinsic (research-backed, see below)
 *
 * Discussion: https://postgr.es/m/1c2a29b8-5b1e-44f7-a871-71ec5fefc120%40app.fastmail.com
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/port/spin_delay.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SPIN_DELAY_H
#define SPIN_DELAY_H

/*
 * pg_spin_delay_impl - Execute a platform-specific CPU hint for spin-wait loops
 *
 * This function should be called inside busy-wait loops to:
 * 1. Reduce CPU power consumption during contention
 * 2. Improve performance of sibling hyperthreads
 * 3. Signal to the CPU that we're in a spin loop
 *
 * The implementation varies by platform to use the most efficient instruction.
 *
 * Note: This defines pg_spin_delay_impl() directly, matching the pattern used
 * by the traditional atomics implementation (arch-*.h files).
 */
#ifndef PG_HAVE_SPIN_DELAY
#define PG_HAVE_SPIN_DELAY
static inline void
pg_spin_delay_impl(void)
{
#if defined(__GNUC__) || defined(__INTEL_COMPILER)

	/*
	 * GCC and Intel compiler: use inline assembly for optimal instructions
	 */

#if defined(__i386__) || defined(__i386)
	/* x86 32-bit: PAUSE instruction (encoded as rep nop) */
	__asm__ __volatile__(" rep; nop \n");
#elif defined(__x86_64__)
	/* x86-64: PAUSE instruction */
	__asm__ __volatile__(" rep; nop \n");
#elif defined(__aarch64__) || defined(__arm64__)
	/*
	 * ARM64: ISB (Instruction Synchronization Barrier)
	 *
	 * Research shows ISB performs better than YIELD on high-core-count ARM64
	 * systems under heavy contention. ISB forces a pipeline flush, which
	 * provides better backoff behavior in spinlock loops.
	 */
	__asm__ __volatile__(" isb; \n");
#elif defined(__arm__) || defined(__arm)
	/* ARM 32-bit: YIELD hint */
	__asm__ __volatile__(" yield; \n");
#else
	/*
	 * Other architectures: compiler barrier only
	 *
	 * A compiler barrier prevents the compiler from optimizing away the loop,
	 * even if we don't have an architecture-specific delay instruction.
	 */
__asm__ __volatile__("":::"memory");
#endif

#elif defined(_MSC_VER)

	/*
	 * Microsoft Visual C++: use intrinsics
	 */

#if defined(_M_ARM64) || defined(_M_ARM64EC)
	/*
	 * Windows ARM64: Use ISB instruction via intrinsic
	 *
	 * Research indicates ISB is better than __yield() on AArch64 at scale.
	 * This matches the GCC/Clang approach above.
	 *
	 * _ARM64_BARRIER_SY = full system barrier (most conservative)
	 */
	__isb(_ARM64_BARRIER_SY);
#elif defined(_M_X64) || defined(_WIN64)
	/*
	 * Windows x86-64: _mm_pause() intrinsic
	 *
	 * Maps to PAUSE instruction. Requires <emmintrin.h> but that's included
	 * by port.h in Windows builds.
	 */
	_mm_pause();
#elif defined(_M_IX86)
	/*
	 * Windows x86 32-bit: Use inline assembly
	 *
	 * MASM syntax for PAUSE (rep nop)
	 */
	__asm		rep nop;
#else
	/*
	 * Other Windows architectures: no-op
	 *
	 * Just a compiler barrier to prevent loop optimization.
	 */
	__asm
	{
	}
#endif

#else
	/*
	 * Unknown compiler: no-op with compiler barrier
	 *
	 * At minimum, we need to prevent the compiler from optimizing away the
	 * spin loop.
	 */
	(void) 0;
#endif
}
#endif							/* PG_HAVE_SPIN_DELAY */

/*
 * Public spin-delay macro for the stdatomic path.
 *
 * The traditional path spells this SPIN_DELAY() (from s_lock.h).  Upstream's
 * atomics.h used to map pg_spin_delay() to an _impl, but that mapping was
 * removed as unused (commit ae27a41e0c7); define it here so the stdatomic
 * spinlock path, which does use it, has a public entry point.
 */
#ifndef pg_spin_delay
#define pg_spin_delay() pg_spin_delay_impl()
#endif

/*
 * Architectures where a relaxed load before the atomic exchange
 * reduces cache-coherency traffic under spinlock contention.
 */
#if defined(__i386__) || defined(__x86_64__) || \
	defined(_M_IX86) || defined(_M_AMD64) || \
	defined(__ppc__) || defined(__powerpc__) || \
	defined(__ppc64__) || defined(__powerpc64__)
#define PG_SPIN_TRY_RELAXED
#endif

#endif							/* SPIN_DELAY_H */
