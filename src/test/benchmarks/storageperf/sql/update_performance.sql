--
-- Benchmark: Update Performance (RECNO vs Heap)
--
-- RECNO performs in-place updates with UNDO logging, avoiding the
-- copy-on-write dead tuple overhead of heap. This benchmark measures
-- the performance difference over repeated update rounds and the
-- resulting storage bloat.
--

-- ================================================================
-- Setup
-- ================================================================
DROP TABLE IF EXISTS perf_update_heap;
DROP TABLE IF EXISTS perf_update_recno;

CREATE TABLE perf_update_heap (
    id integer PRIMARY KEY,
    counter integer DEFAULT 0,
    payload text DEFAULT repeat('x', 100)
) USING heap;

CREATE TABLE perf_update_recno (
    id integer PRIMARY KEY,
    counter integer DEFAULT 0,
    payload text DEFAULT repeat('x', 100)
) USING recno;

INSERT INTO perf_update_heap (id)
SELECT i FROM generate_series(1, 50000) i;

INSERT INTO perf_update_recno (id)
SELECT i FROM generate_series(1, 50000) i;

-- Record initial sizes
\echo '=== Initial Storage Sizes ==='
SELECT 'HEAP' AS am,
       pg_size_pretty(pg_relation_size('perf_update_heap')) AS table_size,
       pg_size_pretty(pg_total_relation_size('perf_update_heap')) AS total_size
UNION ALL
SELECT 'RECNO' AS am,
       pg_size_pretty(pg_relation_size('perf_update_recno')) AS table_size,
       pg_size_pretty(pg_total_relation_size('perf_update_recno')) AS total_size;

-- ================================================================
-- Full-table update: 10 rounds
-- ================================================================
\echo ''
\echo '=== Full-Table Update: 10 Rounds of 50,000 Rows ==='

\timing on

\echo '--- HEAP updates (10 rounds) ---'
UPDATE perf_update_heap SET counter = counter + 1;
UPDATE perf_update_heap SET counter = counter + 1;
UPDATE perf_update_heap SET counter = counter + 1;
UPDATE perf_update_heap SET counter = counter + 1;
UPDATE perf_update_heap SET counter = counter + 1;
UPDATE perf_update_heap SET counter = counter + 1;
UPDATE perf_update_heap SET counter = counter + 1;
UPDATE perf_update_heap SET counter = counter + 1;
UPDATE perf_update_heap SET counter = counter + 1;
UPDATE perf_update_heap SET counter = counter + 1;

\echo '--- RECNO updates (10 rounds) ---'
UPDATE perf_update_recno SET counter = counter + 1;
UPDATE perf_update_recno SET counter = counter + 1;
UPDATE perf_update_recno SET counter = counter + 1;
UPDATE perf_update_recno SET counter = counter + 1;
UPDATE perf_update_recno SET counter = counter + 1;
UPDATE perf_update_recno SET counter = counter + 1;
UPDATE perf_update_recno SET counter = counter + 1;
UPDATE perf_update_recno SET counter = counter + 1;
UPDATE perf_update_recno SET counter = counter + 1;
UPDATE perf_update_recno SET counter = counter + 1;

\timing off

-- ================================================================
-- Post-update storage comparison (bloat measurement)
-- ================================================================
\echo ''
\echo '=== Post-Update Storage Sizes (Bloat Comparison) ==='
SELECT 'HEAP' AS am,
       pg_size_pretty(pg_relation_size('perf_update_heap')) AS table_size,
       pg_size_pretty(pg_total_relation_size('perf_update_heap')) AS total_size
UNION ALL
SELECT 'RECNO' AS am,
       pg_size_pretty(pg_relation_size('perf_update_recno')) AS table_size,
       pg_size_pretty(pg_total_relation_size('perf_update_recno')) AS total_size;

SELECT
    'Bloat Ratio' AS metric,
    round(heap.bytes::numeric / recno.bytes, 2) AS heap_to_recno_ratio
FROM
    (SELECT pg_total_relation_size('perf_update_heap') AS bytes) heap,
    (SELECT pg_total_relation_size('perf_update_recno') AS bytes) recno;

-- Verify data integrity
SELECT 'HEAP' AS am, count(*) AS rows, sum(counter) AS total_counter FROM perf_update_heap
UNION ALL
SELECT 'RECNO' AS am, count(*) AS rows, sum(counter) AS total_counter FROM perf_update_recno;

-- ================================================================
-- Targeted update: 10% of rows, 5 rounds
-- ================================================================
\echo ''
\echo '=== Targeted Update: 10% of Rows, 5 Rounds ==='

\timing on

\echo '--- HEAP targeted updates ---'
UPDATE perf_update_heap SET counter = counter + 1 WHERE id % 10 = 0;
UPDATE perf_update_heap SET counter = counter + 1 WHERE id % 10 = 0;
UPDATE perf_update_heap SET counter = counter + 1 WHERE id % 10 = 0;
UPDATE perf_update_heap SET counter = counter + 1 WHERE id % 10 = 0;
UPDATE perf_update_heap SET counter = counter + 1 WHERE id % 10 = 0;

\echo '--- RECNO targeted updates ---'
UPDATE perf_update_recno SET counter = counter + 1 WHERE id % 10 = 0;
UPDATE perf_update_recno SET counter = counter + 1 WHERE id % 10 = 0;
UPDATE perf_update_recno SET counter = counter + 1 WHERE id % 10 = 0;
UPDATE perf_update_recno SET counter = counter + 1 WHERE id % 10 = 0;
UPDATE perf_update_recno SET counter = counter + 1 WHERE id % 10 = 0;

\timing off

-- ================================================================
-- Cleanup
-- ================================================================
DROP TABLE perf_update_heap;
DROP TABLE perf_update_recno;
