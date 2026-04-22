--
-- Benchmark: Insert Throughput (RECNO vs Heap)
--
-- Tests both bulk inserts (generate_series) and individual row inserts
-- to measure the overhead of RECNO's timestamp and UNDO bookkeeping.
--

-- ================================================================
-- Setup
-- ================================================================
DROP TABLE IF EXISTS perf_insert_heap;
DROP TABLE IF EXISTS perf_insert_recno;

CREATE TABLE perf_insert_heap (
    id serial,
    value integer,
    data text
) USING heap;

CREATE TABLE perf_insert_recno (
    id serial,
    value integer,
    data text
) USING recno;

-- ================================================================
-- Bulk Insert: 100,000 rows
-- ================================================================
\echo '=== Bulk Insert: 100,000 rows ==='

\timing on

\echo '--- HEAP bulk insert ---'
INSERT INTO perf_insert_heap (value, data)
SELECT i, md5(i::text)
FROM generate_series(1, 100000) i;

\echo '--- RECNO bulk insert ---'
INSERT INTO perf_insert_recno (value, data)
SELECT i, md5(i::text)
FROM generate_series(1, 100000) i;

\timing off

-- Verify counts
SELECT 'HEAP' AS am, count(*) AS rows FROM perf_insert_heap
UNION ALL
SELECT 'RECNO' AS am, count(*) AS rows FROM perf_insert_recno;

-- Compare storage sizes after bulk insert
SELECT 'HEAP' AS am,
       pg_size_pretty(pg_relation_size('perf_insert_heap')) AS table_size,
       pg_size_pretty(pg_total_relation_size('perf_insert_heap')) AS total_size
UNION ALL
SELECT 'RECNO' AS am,
       pg_size_pretty(pg_relation_size('perf_insert_recno')) AS table_size,
       pg_size_pretty(pg_total_relation_size('perf_insert_recno')) AS total_size;

-- ================================================================
-- Individual Inserts: 1,000 single-row inserts in a transaction
-- ================================================================
TRUNCATE perf_insert_heap;
TRUNCATE perf_insert_recno;

\echo ''
\echo '=== Individual Inserts: 1,000 single-row inserts ==='

\timing on

\echo '--- HEAP individual inserts ---'
DO $$
BEGIN
    FOR i IN 1..1000 LOOP
        INSERT INTO perf_insert_heap (value, data) VALUES (i, md5(i::text));
    END LOOP;
END
$$;

\echo '--- RECNO individual inserts ---'
DO $$
BEGIN
    FOR i IN 1..1000 LOOP
        INSERT INTO perf_insert_recno (value, data) VALUES (i, md5(i::text));
    END LOOP;
END
$$;

\timing off

-- Verify counts
SELECT 'HEAP' AS am, count(*) AS rows FROM perf_insert_heap
UNION ALL
SELECT 'RECNO' AS am, count(*) AS rows FROM perf_insert_recno;

-- ================================================================
-- Cleanup
-- ================================================================
DROP TABLE perf_insert_heap;
DROP TABLE perf_insert_recno;
