/*--------------------------------------------------------------------------
 *
 * test_atomics.c
 *		Test module for comparing stdatomic.h vs traditional atomics
 *
 * This module provides comprehensive testing and benchmarking of PostgreSQL's
 * atomic operations to ensure correctness and measure performance of both
 * stdatomic.h and traditional implementations.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/modules/test_atomics/test_atomics.c
 *
 *--------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "port/atomics.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;

/* Test counters */
static pg_atomic_uint32 test_counter_u32;
static pg_atomic_uint64 test_counter_u64;
static pg_atomic_flag test_flag;

/*
 * test_atomic_flag_operations
 *
 * Test atomic flag operations (used for spinlocks).
 */
PG_FUNCTION_INFO_V1(test_atomic_flag_operations);
Datum
test_atomic_flag_operations(PG_FUNCTION_ARGS)
{
	int32		iterations = PG_GETARG_INT32(0);
	int32		i;
	bool		success = true;
	StringInfoData buf;

	initStringInfo(&buf);

	/* Initialize flag to unlocked state */
	pg_atomic_init_flag(&test_flag);

	/* Test basic flag operations */
	for (i = 0; i < iterations; i++)
	{
		/* Test that flag starts unlocked */
		if (!pg_atomic_unlocked_test_flag(&test_flag))
		{
			success = false;
			appendStringInfo(&buf, "ERROR: Flag should be unlocked at iteration %d\n", i);
			break;
		}

		/* Acquire the flag (test_set should succeed, returning true) */
		if (!pg_atomic_test_set_flag(&test_flag))
		{
			success = false;
			appendStringInfo(&buf, "ERROR: Failed to acquire flag at iteration %d\n", i);
			break;
		}

		/* Flag should now be locked */
		if (pg_atomic_unlocked_test_flag(&test_flag))
		{
			success = false;
			appendStringInfo(&buf, "ERROR: Flag should be locked at iteration %d\n", i);
			break;
		}

		/* Release the flag */
		pg_atomic_clear_flag(&test_flag);

		/* Flag should be unlocked again */
		if (!pg_atomic_unlocked_test_flag(&test_flag))
		{
			success = false;
			appendStringInfo(&buf, "ERROR: Flag should be unlocked after release at iteration %d\n", i);
			break;
		}
	}

	if (success)
		appendStringInfo(&buf, "SUCCESS: All %d atomic flag operations completed correctly\n", iterations);

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

/*
 * test_atomic_uint32_operations
 *
 * Test 32-bit atomic operations.
 */
PG_FUNCTION_INFO_V1(test_atomic_uint32_operations);
Datum
test_atomic_uint32_operations(PG_FUNCTION_ARGS)
{
	int32		iterations = PG_GETARG_INT32(0);
	int32		i;
	uint32		value;
	uint32		expected;
	bool		success = true;
	StringInfoData buf;

	initStringInfo(&buf);

	/* Initialize counter */
	pg_atomic_init_u32(&test_counter_u32, 0);

	/* Test fetch_add */
	for (i = 0; i < iterations; i++)
	{
		pg_atomic_fetch_add_u32(&test_counter_u32, 1);
	}

	value = pg_atomic_read_u32(&test_counter_u32);
	if (value != (uint32) iterations)
	{
		success = false;
		appendStringInfo(&buf, "ERROR: fetch_add failed, expected %u, got %u\n",
						 (uint32) iterations, value);
	}

	/* Test compare_exchange */
	pg_atomic_write_u32(&test_counter_u32, 100);
	expected = 100;
	if (!pg_atomic_compare_exchange_u32(&test_counter_u32, &expected, 200))
	{
		success = false;
		appendStringInfo(&buf, "ERROR: compare_exchange failed when it should succeed\n");
	}

	value = pg_atomic_read_u32(&test_counter_u32);
	if (value != 200)
	{
		success = false;
		appendStringInfo(&buf, "ERROR: compare_exchange value incorrect, expected 200, got %u\n", value);
	}

	/* Test exchange */
	value = pg_atomic_exchange_u32(&test_counter_u32, 42);
	if (value != 200)
	{
		success = false;
		appendStringInfo(&buf, "ERROR: exchange returned wrong old value, expected 200, got %u\n", value);
	}

	value = pg_atomic_read_u32(&test_counter_u32);
	if (value != 42)
	{
		success = false;
		appendStringInfo(&buf, "ERROR: exchange failed, expected 42, got %u\n", value);
	}

	if (success)
		appendStringInfo(&buf, "SUCCESS: All %d uint32 atomic operations completed correctly\n", iterations);

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

/*
 * test_atomic_uint64_operations
 *
 * Test 64-bit atomic operations.
 */
PG_FUNCTION_INFO_V1(test_atomic_uint64_operations);
Datum
test_atomic_uint64_operations(PG_FUNCTION_ARGS)
{
	int32		iterations = PG_GETARG_INT32(0);
	int32		i;
	uint64		value;
	uint64		expected;
	bool		success = true;
	StringInfoData buf;

	initStringInfo(&buf);

	/* Initialize counter */
	pg_atomic_init_u64(&test_counter_u64, 0);

	/* Test fetch_add */
	for (i = 0; i < iterations; i++)
	{
		pg_atomic_fetch_add_u64(&test_counter_u64, 1);
	}

	value = pg_atomic_read_u64(&test_counter_u64);
	if (value != (uint64) iterations)
	{
		success = false;
		appendStringInfo(&buf, "ERROR: fetch_add failed, expected %llu, got %llu\n",
						 (unsigned long long) iterations, (unsigned long long) value);
	}

	/* Test compare_exchange */
	pg_atomic_write_u64(&test_counter_u64, 100);
	expected = 100;
	if (!pg_atomic_compare_exchange_u64(&test_counter_u64, &expected, 200))
	{
		success = false;
		appendStringInfo(&buf, "ERROR: compare_exchange failed when it should succeed\n");
	}

	value = pg_atomic_read_u64(&test_counter_u64);
	if (value != 200)
	{
		success = false;
		appendStringInfo(&buf, "ERROR: compare_exchange value incorrect, expected 200, got %llu\n",
						 (unsigned long long) value);
	}

	/* Test exchange */
	value = pg_atomic_exchange_u64(&test_counter_u64, 42);
	if (value != 200)
	{
		success = false;
		appendStringInfo(&buf, "ERROR: exchange returned wrong old value, expected 200, got %llu\n",
						 (unsigned long long) value);
	}

	value = pg_atomic_read_u64(&test_counter_u64);
	if (value != 42)
	{
		success = false;
		appendStringInfo(&buf, "ERROR: exchange failed, expected 42, got %llu\n",
						 (unsigned long long) value);
	}

	if (success)
		appendStringInfo(&buf, "SUCCESS: All %d uint64 atomic operations completed correctly\n", iterations);

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

/*
 * benchmark_atomic_operations
 *
 * Benchmark atomic operations to measure performance.
 */
PG_FUNCTION_INFO_V1(benchmark_atomic_operations);
Datum
benchmark_atomic_operations(PG_FUNCTION_ARGS)
{
	int32		iterations = PG_GETARG_INT32(0);
	int32		i;
	TimestampTz start,
				end;
	float8		elapsed_ms;
	StringInfoData buf;

	initStringInfo(&buf);

	/* Benchmark atomic flag operations */
	pg_atomic_init_flag(&test_flag);
	start = GetCurrentTimestamp();
	for (i = 0; i < iterations; i++)
	{
		pg_atomic_test_set_flag(&test_flag);
		pg_atomic_clear_flag(&test_flag);
	}
	end = GetCurrentTimestamp();
	elapsed_ms = (float8) (end - start) / 1000.0;
	appendStringInfo(&buf, "Atomic flag: %d iterations in %.3f ms (%.2f M ops/sec)\n",
					 iterations, elapsed_ms,
					 (float8) iterations / elapsed_ms / 1000.0);

	/* Benchmark uint32 fetch_add */
	pg_atomic_init_u32(&test_counter_u32, 0);
	start = GetCurrentTimestamp();
	for (i = 0; i < iterations; i++)
	{
		pg_atomic_fetch_add_u32(&test_counter_u32, 1);
	}
	end = GetCurrentTimestamp();
	elapsed_ms = (float8) (end - start) / 1000.0;
	appendStringInfo(&buf, "uint32 fetch_add: %d iterations in %.3f ms (%.2f M ops/sec)\n",
					 iterations, elapsed_ms,
					 (float8) iterations / elapsed_ms / 1000.0);

	/* Benchmark uint64 fetch_add */
	pg_atomic_init_u64(&test_counter_u64, 0);
	start = GetCurrentTimestamp();
	for (i = 0; i < iterations; i++)
	{
		pg_atomic_fetch_add_u64(&test_counter_u64, 1);
	}
	end = GetCurrentTimestamp();
	elapsed_ms = (float8) (end - start) / 1000.0;
	appendStringInfo(&buf, "uint64 fetch_add: %d iterations in %.3f ms (%.2f M ops/sec)\n",
					 iterations, elapsed_ms,
					 (float8) iterations / elapsed_ms / 1000.0);

	/* Benchmark uint32 compare_exchange */
	pg_atomic_init_u32(&test_counter_u32, 0);
	start = GetCurrentTimestamp();
	for (i = 0; i < iterations; i++)
	{
		uint32		expected = (uint32) i;

		pg_atomic_compare_exchange_u32(&test_counter_u32, &expected, (uint32) (i + 1));
	}
	end = GetCurrentTimestamp();
	elapsed_ms = (float8) (end - start) / 1000.0;
	appendStringInfo(&buf, "uint32 compare_exchange: %d iterations in %.3f ms (%.2f M ops/sec)\n",
					 iterations, elapsed_ms,
					 (float8) iterations / elapsed_ms / 1000.0);

#ifdef USE_STDATOMIC_H
	appendStringInfo(&buf, "\nImplementation: stdatomic.h (C11)\n");
#else
	appendStringInfo(&buf, "\nImplementation: traditional (platform-specific)\n");
#endif

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}
