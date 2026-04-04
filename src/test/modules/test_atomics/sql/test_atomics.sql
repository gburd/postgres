-- Test atomic operations module

CREATE EXTENSION test_atomics;

-- Test atomic flag operations (moderate iteration count for CI)
SELECT test_atomic_flag_operations(1000);

-- Test uint32 atomic operations
SELECT test_atomic_uint32_operations(1000);

-- Test uint64 atomic operations
SELECT test_atomic_uint64_operations(1000);

-- Benchmark atomic operations (lower iteration count for CI)
SELECT benchmark_atomic_operations(10000);

DROP EXTENSION test_atomics;
