--
-- bench_seqscan.sql
--
-- Measures sequential scan performance, aggregation, and filter
-- operations for RECNO vs HEAP.
--
-- RECNO with compression may have CPU overhead during decompression
-- but can benefit from reduced I/O due to smaller table size.
--

\timing on
SET enable_indexscan = off;
SET enable_bitmapscan = off;

-- ======================================================================
-- Setup: Use 1M-row tables from bulk insert, or create fresh ones
-- ======================================================================
\echo '=== Sequential Scan Benchmark ==='

DROP TABLE IF EXISTS heap_scan_test CASCADE;
DROP TABLE IF EXISTS recno_scan_test CASCADE;

CREATE TABLE heap_scan_test (
    id       INT4,
    category INT4,
    amount   NUMERIC(12,2),
    label    TEXT,
    payload  TEXT
) USING heap;

CREATE TABLE recno_scan_test (
    id       INT4,
    category INT4,
    amount   NUMERIC(12,2),
    label    TEXT,
    payload  TEXT
) USING recno;

-- Insert 500K rows with mixed data
INSERT INTO heap_scan_test
SELECT i,
       i % 100,
       (random() * 10000)::numeric(12,2),
       'Category-' || (i % 100) || '-Item-' || (i % 1000),
       'Detailed payload data for record ' || i ||
       '. This text is moderately long to test scan throughput ' ||
       'with compressed vs uncompressed storage.'
FROM generate_series(1, 500000) i;

INSERT INTO recno_scan_test
SELECT i,
       i % 100,
       (random() * 10000)::numeric(12,2),
       'Category-' || (i % 100) || '-Item-' || (i % 1000),
       'Detailed payload data for record ' || i ||
       '. This text is moderately long to test scan throughput ' ||
       'with compressed vs uncompressed storage.'
FROM generate_series(1, 500000) i;

ANALYZE heap_scan_test;
ANALYZE recno_scan_test;

-- Show table sizes before scanning
SELECT
    'heap' AS am,
    pg_size_pretty(pg_relation_size('heap_scan_test')) AS table_size,
    pg_relation_size('heap_scan_test') AS size_bytes
UNION ALL
SELECT
    'recno',
    pg_size_pretty(pg_relation_size('recno_scan_test')),
    pg_relation_size('recno_scan_test');

-- ======================================================================
-- Test 1: Full table COUNT(*) - minimal per-row processing
-- ======================================================================
\echo '=== Test 1: Full Table COUNT(*) ==='

\echo 'HEAP:'
SELECT count(*) FROM heap_scan_test;

\echo 'RECNO:'
SELECT count(*) FROM recno_scan_test;

-- ======================================================================
-- Test 2: Aggregation (SUM, AVG, MIN, MAX) - numeric column scan
-- ======================================================================
\echo '=== Test 2: Aggregation ==='

\echo 'HEAP:'
SELECT
    count(*) AS cnt,
    avg(amount) AS avg_amt,
    sum(amount) AS sum_amt,
    min(amount) AS min_amt,
    max(amount) AS max_amt
FROM heap_scan_test;

\echo 'RECNO:'
SELECT
    count(*) AS cnt,
    avg(amount) AS avg_amt,
    sum(amount) AS sum_amt,
    min(amount) AS min_amt,
    max(amount) AS max_amt
FROM recno_scan_test;

-- ======================================================================
-- Test 3: Filtered scan (10% selectivity)
-- ======================================================================
\echo '=== Test 3: Filtered Scan (10% selectivity) ==='

\echo 'HEAP:'
SELECT count(*), avg(amount)
FROM heap_scan_test
WHERE category < 10;

\echo 'RECNO:'
SELECT count(*), avg(amount)
FROM recno_scan_test
WHERE category < 10;

-- ======================================================================
-- Test 4: Text column scan (forces decompression per row)
-- ======================================================================
\echo '=== Test 4: Text Column Scan ==='

\echo 'HEAP:'
SELECT count(*), avg(length(payload)), avg(length(label))
FROM heap_scan_test;

\echo 'RECNO:'
SELECT count(*), avg(length(payload)), avg(length(label))
FROM recno_scan_test;

-- ======================================================================
-- Test 5: Filtered text scan with LIKE
-- ======================================================================
\echo '=== Test 5: LIKE Filter Scan ==='

\echo 'HEAP:'
SELECT count(*)
FROM heap_scan_test
WHERE label LIKE 'Category-42-%';

\echo 'RECNO:'
SELECT count(*)
FROM recno_scan_test
WHERE label LIKE 'Category-42-%';

-- ======================================================================
-- Test 6: GROUP BY aggregation (hash aggregate over full scan)
-- ======================================================================
\echo '=== Test 6: GROUP BY Aggregation ==='

\echo 'HEAP:'
SELECT category, count(*), avg(amount)
FROM heap_scan_test
GROUP BY category
ORDER BY category
LIMIT 10;

\echo 'RECNO:'
SELECT category, count(*), avg(amount)
FROM recno_scan_test
GROUP BY category
ORDER BY category
LIMIT 10;

-- ======================================================================
-- Test 7: EXPLAIN ANALYZE comparison
-- ======================================================================
\echo '=== Test 7: EXPLAIN ANALYZE ==='

\echo 'HEAP full scan:'
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT count(*), sum(amount) FROM heap_scan_test;

\echo 'RECNO full scan:'
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT count(*), sum(amount) FROM recno_scan_test;

\timing off
RESET enable_indexscan;
RESET enable_bitmapscan;
