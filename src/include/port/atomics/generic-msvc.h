/*-------------------------------------------------------------------------
 *
 * generic-msvc.h
 *	  Atomic operations support when using MSVC
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * NOTES:
 *
 * Documentation:
 * * Interlocked Variable Access
 *   http://msdn.microsoft.com/en-us/library/ms684122%28VS.85%29.aspx
 *
 * src/include/port/atomics/generic-msvc.h
 *
 *-------------------------------------------------------------------------
 */
#include <intrin.h>

/* intentionally no include guards, should only be included by atomics.h */
#ifndef INSIDE_ATOMICS_H
#error "should be included via atomics.h"
#endif

#pragma intrinsic(_ReadWriteBarrier)
#define pg_compiler_barrier_impl()	_ReadWriteBarrier()

#ifndef pg_memory_barrier_impl
#define pg_memory_barrier_impl()	MemoryBarrier()
#endif

/*
 * On ARM64 (aarch64) the ordering guarantees PostgreSQL's lock-free code
 * assumes are not reliably met by the plain interlocked intrinsics:
 *
 *  - _ReadWriteBarrier() is a compiler barrier only; it emits no instruction.
 *
 *  - A plain volatile load/store is not an acquire/release on this weakly
 *    ordered architecture (unlike x86/x64 TSO, where it is).
 *
 *  - MSVC may satisfy an interlocked operation with an out-of-line helper
 *    that, on ARMv8.1 targets, uses an LSE "casal" instruction.  "casal"
 *    provides acquire/release semantics for that single location but is NOT
 *    a full fence; it is weaker than the "dmb ish" that PostgreSQL's barrier
 *    macros are expected to provide.  Code that assumes full-fence semantics
 *    (e.g. the standalone pg_memory_barrier()/pg_read_barrier() used around
 *    lock-free LSN bookkeeping) can then misbehave -- observed as incorrect
 *    runtime behaviour on Windows ARM64 and reported to Microsoft (MSVC
 *    Developer Community feedback 10982137).
 *
 * To be safe we force an explicit full data memory barrier
 * (__dmb(_ARM64_BARRIER_ISH)) on the atomic read/write and compare-exchange
 * paths on ARM64, rather than relying on the intrinsics' implicit ordering.
 * The x86/x64 paths below are unchanged.
 */
#ifdef _M_ARM64
#define PG_MSVC_ARM64_FENCE	1
#endif

#define PG_HAVE_ATOMIC_U32_SUPPORT
typedef struct pg_atomic_uint32
{
	volatile uint32 value;
} pg_atomic_uint32;

#define PG_HAVE_ATOMIC_U64_SUPPORT
typedef struct pg_atomic_uint64
{
	alignas(8) volatile uint64 value;
} pg_atomic_uint64;


#define PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U32
static inline bool
pg_atomic_compare_exchange_u32_impl(volatile pg_atomic_uint32 *ptr,
									uint32 *expected, uint32 newval)
{
	bool	ret;
	uint32	current;
#ifdef PG_MSVC_ARM64_FENCE
	__dmb(_ARM64_BARRIER_ISH);
#endif
	current = InterlockedCompareExchange(&ptr->value, newval, *expected);
#ifdef PG_MSVC_ARM64_FENCE
	__dmb(_ARM64_BARRIER_ISH);
#endif
	ret = current == *expected;
	*expected = current;
	return ret;
}

#ifdef PG_MSVC_ARM64_FENCE
/*
 * Acquire load / release store for u32.  A plain volatile access is not
 * ordered on ARM64, so bracket it with a full data memory barrier.
 */
#define PG_HAVE_ATOMIC_READ_U32
static inline uint32
pg_atomic_read_u32_impl(volatile pg_atomic_uint32 *ptr)
{
	uint32		v = ptr->value;

	__dmb(_ARM64_BARRIER_ISH);	/* acquire */
	return v;
}

#define PG_HAVE_ATOMIC_WRITE_U32
static inline void
pg_atomic_write_u32_impl(volatile pg_atomic_uint32 *ptr, uint32 val)
{
	__dmb(_ARM64_BARRIER_ISH);	/* release */
	ptr->value = val;
}
#endif							/* PG_MSVC_ARM64_FENCE */

/*
 * On ARM64 leave exchange/fetch_add undefined so generic.h derives them from
 * the fenced compare-exchange above.
 */
#ifndef PG_MSVC_ARM64_FENCE

#define PG_HAVE_ATOMIC_EXCHANGE_U32
static inline uint32
pg_atomic_exchange_u32_impl(volatile pg_atomic_uint32 *ptr, uint32 newval)
{
	return InterlockedExchange(&ptr->value, newval);
}

#define PG_HAVE_ATOMIC_FETCH_ADD_U32
static inline uint32
pg_atomic_fetch_add_u32_impl(volatile pg_atomic_uint32 *ptr, int32 add_)
{
	return InterlockedExchangeAdd(&ptr->value, add_);
}

#endif							/* !PG_MSVC_ARM64_FENCE */

/*
 * The non-intrinsics versions are only available in vista upwards, so use the
 * intrinsic version. Only supported on >486, but we require XP as a minimum
 * baseline, which doesn't support the 486, so we don't need to add checks for
 * that case.
 */
#pragma intrinsic(_InterlockedCompareExchange64)

#define PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U64
static inline bool
pg_atomic_compare_exchange_u64_impl(volatile pg_atomic_uint64 *ptr,
									uint64 *expected, uint64 newval)
{
	bool	ret;
	uint64	current;
#ifdef PG_MSVC_ARM64_FENCE
	__dmb(_ARM64_BARRIER_ISH);
#endif
	current = _InterlockedCompareExchange64(&ptr->value, newval, *expected);
#ifdef PG_MSVC_ARM64_FENCE
	__dmb(_ARM64_BARRIER_ISH);
#endif
	ret = current == *expected;
	*expected = current;
	return ret;
}

#ifdef PG_MSVC_ARM64_FENCE
/*
 * Acquire load / release store for u64 (see the u32 versions above for the
 * rationale).  generic-msvc.h otherwise leaves read/write_u64 to the plain
 * volatile implementations in generic.h, which are not ordered on ARM64.
 */
#define PG_HAVE_ATOMIC_READ_U64
static inline uint64
pg_atomic_read_u64_impl(volatile pg_atomic_uint64 *ptr)
{
	uint64		v;

	AssertPointerAlignment(ptr, 8);
	v = ptr->value;
	__dmb(_ARM64_BARRIER_ISH);	/* acquire */
	return v;
}

#define PG_HAVE_ATOMIC_WRITE_U64
static inline void
pg_atomic_write_u64_impl(volatile pg_atomic_uint64 *ptr, uint64 val)
{
	AssertPointerAlignment(ptr, 8);
	__dmb(_ARM64_BARRIER_ISH);	/* release */
	ptr->value = val;
}
#endif							/* PG_MSVC_ARM64_FENCE */

/* Only implemented on 64bit builds */
#ifdef _WIN64

/*
 * On ARM64 leave exchange/fetch_add undefined so generic.h derives them from
 * the fenced compare-exchange above (see the u32 note).  On x86/x64 (TSO) the
 * direct intrinsics are fine.
 */
#ifndef PG_MSVC_ARM64_FENCE

#pragma intrinsic(_InterlockedExchange64)

#define PG_HAVE_ATOMIC_EXCHANGE_U64
static inline uint64
pg_atomic_exchange_u64_impl(volatile pg_atomic_uint64 *ptr, uint64 newval)
{
	return _InterlockedExchange64(&ptr->value, newval);
}

#pragma intrinsic(_InterlockedExchangeAdd64)

#define PG_HAVE_ATOMIC_FETCH_ADD_U64
static inline uint64
pg_atomic_fetch_add_u64_impl(volatile pg_atomic_uint64 *ptr, int64 add_)
{
	return _InterlockedExchangeAdd64(&ptr->value, add_);
}

#endif							/* !PG_MSVC_ARM64_FENCE */

#endif /* _WIN64 */
