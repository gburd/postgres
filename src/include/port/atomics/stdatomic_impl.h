/*-------------------------------------------------------------------------
 *
 * stdatomic_impl.h
 *	  Atomic operations implementation using C11 stdatomic.h
 *
 * This file provides PostgreSQL atomic operations using the C11 standard
 * <stdatomic.h>. It is only included when USE_STDATOMIC_H is defined.
 *
 * The traditional platform-specific implementation (arch-*.h, generic-*.h,
 * fallback.h) remains available and is used when stdatomic.h is not
 * available or when explicitly requested via -Duse_stdatomic=no.
 *
 * IMPORTANT: This is an alternative implementation. The traditional path is
 * deprecated and will be removed in a future major release.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/port/atomics/stdatomic_impl.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef STDATOMIC_IMPL_H
#define STDATOMIC_IMPL_H

/*
 * Only include this file when USE_STDATOMIC_H is defined at build time.
 * This ensures the traditional implementation remains available as fallback.
 */
#ifdef USE_STDATOMIC_H

/*
 * C++ Compatibility
 *
 * C++11 through C++20 cannot include C's <stdatomic.h> directly. Instead,
 * we use C++11's <atomic> header and map to std::atomic types. C++23 allows
 * <stdatomic.h> but we handle older versions.
 *
 * This follows the approach from C++23 standard section 33.5.12 which
 * allows C and C++ atomic types to interoperate.
 */
#if defined(__cplusplus) && __cplusplus < 202302L
extern "C++"
{
#include <atomic>

	/* Map to C++ atomic types */
#define pg_atomic(T) std::atomic<T>

	/* Import memory order constants into global namespace */
	using std::memory_order_relaxed;
	using std::memory_order_acquire;
	using std::memory_order_release;
	using std::memory_order_acq_rel;
	using std::memory_order_seq_cst;
}
#else
/* C11 or C++23+: use standard <stdatomic.h> */
#include <stdatomic.h>
#define pg_atomic(T) _Atomic(T)
#endif

/*
 * Type Definitions
 *
 * We use C11 _Atomic qualifier (or C++ std::atomic) with PostgreSQL's
 * standard integer types. These types are compatible with atomic operations
 * and provide sequential consistency guarantees.
 *
 * Note: pg_atomic(T) is defined above and maps to either _Atomic(T) for C
 * or std::atomic<T> for C++.
 */
typedef pg_atomic(uint8)  pg_atomic_flag;
typedef pg_atomic(uint8)  pg_atomic_uint8;
typedef pg_atomic(uint16) pg_atomic_uint16;
typedef pg_atomic(uint32) pg_atomic_uint32;
typedef pg_atomic(uint64) pg_atomic_uint64;

/*
 * Signal atomic operation support to atomics.h
 *
 * stdatomic.h provides native support for all atomic types on all platforms
 * it compiles on, so we unconditionally define these.  PG_HAVE_ATOMIC_U64_SIMULATION
 * is intentionally NOT defined — stdatomic provides real 64-bit atomics.
 */
#define PG_HAVE_ATOMIC_U32_SUPPORT
#define PG_HAVE_ATOMIC_U64_SUPPORT

/*
 * 8-byte single-copy atomicity.
 *
 * All 64-bit architectures that support C11 stdatomic.h guarantee 8-byte
 * single-copy atomicity for aligned loads/stores.  This is used by bufmgr.c
 * and other performance-critical paths to avoid unnecessary atomic operations.
 */
#if SIZEOF_VOID_P >= 8
#define PG_HAVE_8BYTE_SINGLE_COPY_ATOMICITY
#endif

/*
 * Memory Barrier Implementation
 *
 * PostgreSQL's memory barrier API maps directly to C11 atomic_thread_fence.
 * We use memory_order_seq_cst (sequential consistency) by default, matching
 * PostgreSQL's existing semantics.
 *
 * Note: These are implementation functions (_impl suffix) that will be
 * mapped to the public API by atomics.h.
 */
#define pg_compiler_barrier_impl()	atomic_signal_fence(memory_order_seq_cst)

#define pg_memory_barrier_impl()	atomic_thread_fence(memory_order_seq_cst)
#define pg_read_barrier_impl()		atomic_thread_fence(memory_order_acquire)
#define pg_write_barrier_impl()		atomic_thread_fence(memory_order_release)

/*
 * Spin Delay Implementation
 *
 * On x86, a spinloop without a "pause" instruction can waste a lot of power
 * and slow down the adjacent hyperthread. On ARM64, ISB provides better
 * backoff than YIELD. This provides platform-specific spin delay hints.
 *
 * The implementation is in spin_delay.h which directly defines pg_spin_delay_impl(),
 * matching the pattern used by the traditional atomics implementation.
 */
#include "port/spin_delay.h"

/*
 * =================================================================
 * pg_atomic_flag Operations
 * =================================================================
 *
 * Atomic flag type used primarily for spinlocks.
 *
 * CRITICAL FLAG CONVENTION DIFFERENCE FROM TRADITIONAL IMPLEMENTATION:
 *
 * stdatomic.h implementation (this file):
 *   1 = unlocked, 0 = locked
 *
 * Traditional implementation (generic.h):
 *   0 = unlocked, 1 = locked
 *
 * This polarity difference is INTENTIONAL and internal to the implementation.
 * The public API behavior is identical in both implementations:
 * - pg_atomic_init_flag() initializes to unlocked state
 * - pg_atomic_test_set_flag() returns true if lock was successfully acquired
 * - pg_atomic_clear_flag() releases the lock
 *
 * The stdatomic.h convention (1=unlocked) is more natural for C11 atomics
 * and avoids double-negation logic in the implementation.
 */

/*
 * pg_atomic_init_flag_impl - Initialize atomic flag to unlocked state
 *
 * In the stdatomic.h implementation, we initialize to 1 (unlocked).
 */
static inline void
pg_atomic_init_flag_impl(pg_atomic_flag *ptr)
{
	atomic_init(ptr, 1);  /* 1 = unlocked */
}

/*
 * pg_atomic_test_set_flag_impl - Try to acquire lock
 *
 * Atomically clears the flag (AND with 0) and returns previous value.
 * Returns true if we successfully acquired the lock (old value was 1,
 * meaning unlocked), false if already locked (old value was 0).
 *
 * Memory ordering: acquire (synchronizes-with prior release)
 */
static inline bool
pg_atomic_test_set_flag_impl(pg_atomic_flag *ptr)
{
	/*
	 * AND flag with 0 to clear it (locked). If previous value was 1
	 * (unlocked), we succeeded in acquiring. If previous value was 0
	 * (already locked), we failed.
	 *
	 * Return true if we SUCCEEDED in acquiring the lock.
	 */
	return atomic_fetch_and_explicit(ptr, 0, memory_order_acquire) != 0;
}

/*
 * pg_atomic_unlocked_test_flag_impl - Test if flag is unlocked without acquiring
 *
 * Returns true if the flag is currently unlocked (value == 1).
 * Uses relaxed ordering since this is typically used for optimization hints.
 */
static inline bool
pg_atomic_unlocked_test_flag_impl(pg_atomic_flag *ptr)
{
	return atomic_load_explicit(ptr, memory_order_relaxed) != 0;
}

/*
 * pg_atomic_clear_flag_impl - Release lock
 *
 * Sets flag to 1 (unlocked).
 * Memory ordering: release (makes prior writes visible)
 */
static inline void
pg_atomic_clear_flag_impl(pg_atomic_flag *ptr)
{
	atomic_store_explicit(ptr, 1, memory_order_release);
}

/*
 * =================================================================
 * pg_atomic_uint32 Operations
 * =================================================================
 *
 * 32-bit atomic unsigned integer operations.
 * All operations use sequential consistency unless otherwise noted.
 */

/*
 * pg_atomic_init_u32_impl - Initialize atomic uint32
 *
 * This is not an atomic operation - must only be called during initialization
 * when no concurrent access is possible.
 */
static inline void
pg_atomic_init_u32_impl(pg_atomic_uint32 *ptr, uint32 val)
{
	atomic_init(ptr, val);
}

/*
 * pg_atomic_read_u32_impl - Atomically read uint32 value
 *
 * Memory ordering: relaxed (matching traditional volatile semantics).
 * Callers that need ordering should use pg_atomic_read_membarrier_u32_impl().
 */
static inline uint32
pg_atomic_read_u32_impl(pg_atomic_uint32 *ptr)
{
	return atomic_load_explicit(ptr, memory_order_relaxed);
}

/*
 * pg_atomic_write_u32_impl - Atomically write uint32 value
 *
 * Memory ordering: relaxed (matching traditional volatile semantics).
 * Callers that need ordering should use pg_atomic_write_membarrier_u32_impl().
 */
static inline void
pg_atomic_write_u32_impl(pg_atomic_uint32 *ptr, uint32 val)
{
	atomic_store_explicit(ptr, val, memory_order_relaxed);
}

/*
 * pg_atomic_exchange_u32_impl - Atomically exchange uint32 value
 *
 * Atomically replaces the value with newval and returns the old value.
 * Memory ordering: seq_cst
 */
static inline uint32
pg_atomic_exchange_u32_impl(pg_atomic_uint32 *ptr, uint32 newval)
{
	return atomic_exchange_explicit(ptr, newval, memory_order_seq_cst);
}

/*
 * pg_atomic_compare_exchange_u32_impl - Atomic compare-and-swap
 *
 * Compares *ptr with *expected. If equal, replaces *ptr with newval and
 * returns true. If not equal, updates *expected with current *ptr value
 * and returns false.
 *
 * This is the strong version (no spurious failures).
 * Memory ordering: seq_cst for both success and failure
 */
static inline bool
pg_atomic_compare_exchange_u32_impl(pg_atomic_uint32 *ptr,
									uint32 *expected, uint32 newval)
{
	return atomic_compare_exchange_strong_explicit(ptr, expected, newval,
												   memory_order_seq_cst,
												   memory_order_seq_cst);
}

/*
 * pg_atomic_fetch_add_u32_impl - Atomic fetch-and-add
 *
 * Atomically adds add_ to *ptr and returns the old value.
 * Memory ordering: seq_cst
 */
static inline uint32
pg_atomic_fetch_add_u32_impl(pg_atomic_uint32 *ptr, int32 add_)
{
	return atomic_fetch_add_explicit(ptr, add_, memory_order_seq_cst);
}

/*
 * pg_atomic_fetch_sub_u32_impl - Atomic fetch-and-subtract
 *
 * Atomically subtracts sub_ from *ptr and returns the old value.
 * Memory ordering: seq_cst
 */
static inline uint32
pg_atomic_fetch_sub_u32_impl(pg_atomic_uint32 *ptr, int32 sub_)
{
	return atomic_fetch_sub_explicit(ptr, sub_, memory_order_seq_cst);
}

/*
 * pg_atomic_fetch_and_u32_impl - Atomic fetch-and-AND
 *
 * Atomically performs *ptr &= and_ and returns the old value.
 * Memory ordering: seq_cst
 */
static inline uint32
pg_atomic_fetch_and_u32_impl(pg_atomic_uint32 *ptr, uint32 and_)
{
	return atomic_fetch_and_explicit(ptr, and_, memory_order_seq_cst);
}

/*
 * pg_atomic_fetch_or_u32_impl - Atomic fetch-and-OR
 *
 * Atomically performs *ptr |= or_ and returns the old value.
 * Memory ordering: seq_cst
 */
static inline uint32
pg_atomic_fetch_or_u32_impl(pg_atomic_uint32 *ptr, uint32 or_)
{
	return atomic_fetch_or_explicit(ptr, or_, memory_order_seq_cst);
}

/*
 * pg_atomic_add_fetch_u32_impl - Atomic add-and-fetch
 *
 * Atomically adds add_ to *ptr and returns the NEW value.
 * Implemented using fetch_add + add_ since C11 doesn't have add_fetch.
 */
static inline uint32
pg_atomic_add_fetch_u32_impl(pg_atomic_uint32 *ptr, int32 add_)
{
	return atomic_fetch_add_explicit(ptr, add_, memory_order_seq_cst) + add_;
}

/*
 * pg_atomic_sub_fetch_u32_impl - Atomic subtract-and-fetch
 *
 * Atomically subtracts sub_ from *ptr and returns the NEW value.
 */
static inline uint32
pg_atomic_sub_fetch_u32_impl(pg_atomic_uint32 *ptr, int32 sub_)
{
	return atomic_fetch_sub_explicit(ptr, sub_, memory_order_seq_cst) - sub_;
}

/*
 * pg_atomic_read_membarrier_u32_impl - Read with full memory barrier
 *
 * Atomic read with sequential consistency. Useful for cases where
 * correctness is more important than performance.
 */
static inline uint32
pg_atomic_read_membarrier_u32_impl(pg_atomic_uint32 *ptr)
{
	return atomic_load_explicit(ptr, memory_order_seq_cst);
}

/*
 * pg_atomic_unlocked_write_u32_impl - Non-atomic write
 *
 * Write without atomicity guarantees. Only safe when exclusive access
 * is guaranteed externally. Uses relaxed ordering.
 */
static inline void
pg_atomic_unlocked_write_u32_impl(pg_atomic_uint32 *ptr, uint32 val)
{
	atomic_store_explicit(ptr, val, memory_order_relaxed);
}

/*
 * pg_atomic_write_membarrier_u32_impl - Write with full memory barrier
 *
 * Atomic write with sequential consistency.
 */
static inline void
pg_atomic_write_membarrier_u32_impl(pg_atomic_uint32 *ptr, uint32 val)
{
	atomic_store_explicit(ptr, val, memory_order_seq_cst);
}

/*
 * =================================================================
 * pg_atomic_uint64 Operations
 * =================================================================
 *
 * 64-bit equivalents of the u32 operations above.  See the u32 comments
 * for API documentation; semantics and memory ordering are identical.
 */

static inline void
pg_atomic_init_u64_impl(pg_atomic_uint64 *ptr, uint64 val)
{
	atomic_init(ptr, val);
}

static inline uint64
pg_atomic_read_u64_impl(pg_atomic_uint64 *ptr)
{
	return atomic_load_explicit(ptr, memory_order_relaxed);
}

static inline void
pg_atomic_write_u64_impl(pg_atomic_uint64 *ptr, uint64 val)
{
	atomic_store_explicit(ptr, val, memory_order_relaxed);
}

static inline uint64
pg_atomic_exchange_u64_impl(pg_atomic_uint64 *ptr, uint64 newval)
{
	return atomic_exchange_explicit(ptr, newval, memory_order_seq_cst);
}

static inline bool
pg_atomic_compare_exchange_u64_impl(pg_atomic_uint64 *ptr,
									uint64 *expected, uint64 newval)
{
	return atomic_compare_exchange_strong_explicit(ptr, expected, newval,
												   memory_order_seq_cst,
												   memory_order_seq_cst);
}

static inline uint64
pg_atomic_fetch_add_u64_impl(pg_atomic_uint64 *ptr, int64 add_)
{
	return atomic_fetch_add_explicit(ptr, add_, memory_order_seq_cst);
}

static inline uint64
pg_atomic_fetch_sub_u64_impl(pg_atomic_uint64 *ptr, int64 sub_)
{
	return atomic_fetch_sub_explicit(ptr, sub_, memory_order_seq_cst);
}

static inline uint64
pg_atomic_fetch_and_u64_impl(pg_atomic_uint64 *ptr, uint64 and_)
{
	return atomic_fetch_and_explicit(ptr, and_, memory_order_seq_cst);
}

static inline uint64
pg_atomic_fetch_or_u64_impl(pg_atomic_uint64 *ptr, uint64 or_)
{
	return atomic_fetch_or_explicit(ptr, or_, memory_order_seq_cst);
}

static inline uint64
pg_atomic_add_fetch_u64_impl(pg_atomic_uint64 *ptr, int64 add_)
{
	return atomic_fetch_add_explicit(ptr, add_, memory_order_seq_cst) + add_;
}

static inline uint64
pg_atomic_sub_fetch_u64_impl(pg_atomic_uint64 *ptr, int64 sub_)
{
	return atomic_fetch_sub_explicit(ptr, sub_, memory_order_seq_cst) - sub_;
}

static inline uint64
pg_atomic_read_membarrier_u64_impl(pg_atomic_uint64 *ptr)
{
	return atomic_load_explicit(ptr, memory_order_seq_cst);
}

static inline void
pg_atomic_unlocked_write_u64_impl(pg_atomic_uint64 *ptr, uint64 val)
{
	atomic_store_explicit(ptr, val, memory_order_relaxed);
}

static inline void
pg_atomic_write_membarrier_u64_impl(pg_atomic_uint64 *ptr, uint64 val)
{
	atomic_store_explicit(ptr, val, memory_order_seq_cst);
}

#endif /* USE_STDATOMIC_H */

#endif /* STDATOMIC_IMPL_H */
