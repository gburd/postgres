-- Test atomic operations module
--
-- Correctness tests only for PostgreSQL's C11 stdatomic.h atomic
-- operations.  The benchmark_atomic_operations() function is intentionally
-- NOT exercised here because its timing output is non-deterministic; run it
-- by hand for ad-hoc measurement.

CREATE EXTENSION test_atomics;

-- Test atomic flag operations (moderate iteration count for CI)
SELECT test_atomic_flag_operations(1000);

-- Test uint32 atomic operations
SELECT test_atomic_uint32_operations(1000);

-- Test uint64 atomic operations
SELECT test_atomic_uint64_operations(1000);

DROP EXTENSION test_atomics;
