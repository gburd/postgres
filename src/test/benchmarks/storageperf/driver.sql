--
-- RECNO vs Heap Performance Benchmark Driver
--
-- Run with: psql -d <dbname> -f src/test/storageperf/driver.sql
--
-- This driver runs all benchmark SQL files and prints a summary.
--

\echo '================================================================'
\echo ' RECNO vs Heap Performance Benchmark Suite'
\echo '================================================================'
\echo ''
\echo ' PostgreSQL version:'
SELECT version();
\echo ''

-- Create a results table to collect summary metrics
DROP TABLE IF EXISTS _perf_results;
CREATE TEMP TABLE _perf_results (
    benchmark text,
    metric text,
    heap_value numeric,
    recno_value numeric,
    unit text
);

-- ================================================================
-- Run individual benchmarks
-- ================================================================

\echo '================================================================'
\echo ' Benchmark 1: Insert Throughput'
\echo '================================================================'
\i sql/insert_throughput.sql

\echo ''
\echo '================================================================'
\echo ' Benchmark 2: Update Performance'
\echo '================================================================'
\i sql/update_performance.sql

\echo ''
\echo '================================================================'
\echo ' Benchmark 3: VACUUM Overhead'
\echo '================================================================'
\i sql/vacuum_overhead.sql

\echo ''
\echo '================================================================'
\echo ' Benchmark 4: Rollback Cost'
\echo '================================================================'
\i sql/rollback_cost.sql

\echo ''
\echo '================================================================'
\echo ' Benchmark 5: Storage Footprint'
\echo '================================================================'
\i sql/storage_footprint.sql

\echo ''
\echo '================================================================'
\echo ' Benchmark 6: Read Under Writes'
\echo '================================================================'
\i sql/read_under_writes.sql

\echo ''
\echo '================================================================'
\echo ' Benchmark 7: TOAST vs Overflow (Large Column Storage)'
\echo '================================================================'
\i sql/toast_overflow.sql

\echo ''
\echo '================================================================'
\echo ' Benchmark 8: TOAST vs Overflow (Compression OFF)'
\echo '================================================================'
\i sql/toast_overflow_nocomp.sql

\echo ''
\echo '================================================================'
\echo ' Benchmark 9: Compression Matrix (RECNO vs HEAP TOAST)'
\echo '================================================================'
\i sql/compression_matrix.sql

-- ================================================================
-- Summary: Collect key comparison metrics
-- ================================================================

\echo ''
\echo '================================================================'
\echo ' SUMMARY: RECNO vs Heap Comparison'
\echo '================================================================'
\echo ''

-- Run a quick summary comparison using fresh tables
DROP TABLE IF EXISTS _sum_heap;
DROP TABLE IF EXISTS _sum_recno;

CREATE TABLE _sum_heap (id integer, counter integer DEFAULT 0, data text) USING heap;
CREATE TABLE _sum_recno (id integer, counter integer DEFAULT 0, data text) USING recno;

-- Insert test data
INSERT INTO _sum_heap SELECT i, 0, md5(i::text) FROM generate_series(1, 20000) i;
INSERT INTO _sum_recno SELECT i, 0, md5(i::text) FROM generate_series(1, 20000) i;

-- Run 5 update rounds to create bloat
UPDATE _sum_heap SET counter = counter + 1;
UPDATE _sum_heap SET counter = counter + 1;
UPDATE _sum_heap SET counter = counter + 1;
UPDATE _sum_heap SET counter = counter + 1;
UPDATE _sum_heap SET counter = counter + 1;

UPDATE _sum_recno SET counter = counter + 1;
UPDATE _sum_recno SET counter = counter + 1;
UPDATE _sum_recno SET counter = counter + 1;
UPDATE _sum_recno SET counter = counter + 1;
UPDATE _sum_recno SET counter = counter + 1;

\echo '--- Storage After 5 Update Rounds (20,000 rows) ---'
SELECT
    'Storage Comparison' AS metric,
    pg_size_pretty(heap.bytes) AS heap_size,
    pg_size_pretty(recno.bytes) AS recno_size,
    round(heap.bytes::numeric / GREATEST(recno.bytes, 1), 2) AS bloat_ratio
FROM
    (SELECT pg_total_relation_size('_sum_heap') AS bytes) heap,
    (SELECT pg_total_relation_size('_sum_recno') AS bytes) recno;

\echo ''
\echo '--- Row Counts (sanity check) ---'
SELECT 'HEAP' AS am, count(*) AS rows, sum(counter) AS total FROM _sum_heap
UNION ALL
SELECT 'RECNO' AS am, count(*) AS rows, sum(counter) AS total FROM _sum_recno;

-- Cleanup summary tables
DROP TABLE _sum_heap;
DROP TABLE _sum_recno;
DROP TABLE IF EXISTS _perf_results;

\echo ''
\echo '================================================================'
\echo ' Benchmark suite complete.'
\echo '================================================================'
