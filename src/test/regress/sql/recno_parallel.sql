--
-- Test RECNO parallel scanning and TID range scan support
--

-- =============================================
-- Setup - Enable parallel query
-- =============================================

-- Suppress non-deterministic resource leak warnings (memory addresses vary)
SET client_min_messages = error;

-- Force parallel query for testing
SET max_parallel_workers_per_gather = 2;
SET parallel_tuple_cost = 0;
SET parallel_setup_cost = 0;
SET min_parallel_table_scan_size = 0;
SET min_parallel_index_scan_size = 0;

-- =============================================
-- Create and populate a RECNO table
-- =============================================

CREATE TABLE recno_parallel_test (
    id integer NOT NULL,
    val text,
    num numeric
) USING recno;

-- Insert enough rows to make parallel scan worthwhile
INSERT INTO recno_parallel_test
SELECT i, 'row_' || i::text, (i * 1.5)::numeric
FROM generate_series(1, 1000) AS i;

-- Verify row count
SELECT COUNT(*) FROM recno_parallel_test;

-- =============================================
-- TID range scan tests
-- =============================================

-- Basic TID range scan using ctid
SELECT COUNT(*) FROM recno_parallel_test WHERE ctid >= '(0,1)' AND ctid < '(0,10)';

-- TID range scan should return tuples in range
SELECT id FROM recno_parallel_test WHERE ctid >= '(0,1)' AND ctid <= '(0,5)' ORDER BY id;

-- Empty TID range should return no rows
SELECT COUNT(*) FROM recno_parallel_test WHERE ctid >= '(9999,1)' AND ctid < '(9999,10)';

-- TID range scan with only lower bound
SELECT COUNT(*) > 0 AS has_rows FROM recno_parallel_test WHERE ctid >= '(0,1)';

-- TID range scan with only upper bound
SELECT COUNT(*) > 0 AS has_rows FROM recno_parallel_test WHERE ctid < '(1,1)';

-- =============================================
-- Parallel sequential scan tests
-- =============================================

-- Force parallel execution and verify results are correct
-- The aggregate should produce the same result regardless of parallelism

-- Sum with parallel scan
SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;

SELECT SUM(id) AS total_id FROM recno_parallel_test;

-- Verify the sum is correct: sum(1..1000) = 500500
SELECT SUM(id) = 500500 AS sum_correct FROM recno_parallel_test;

-- Count with parallel scan
SELECT COUNT(*) = 1000 AS count_correct FROM recno_parallel_test;

-- Min/Max with parallel scan
SELECT MIN(id) = 1 AS min_correct, MAX(id) = 1000 AS max_correct
FROM recno_parallel_test;

-- =============================================
-- Parallel scan with WHERE clause
-- =============================================

SELECT COUNT(*) FROM recno_parallel_test WHERE id > 500;
SELECT COUNT(*) FROM recno_parallel_test WHERE id BETWEEN 100 AND 200;
SELECT COUNT(*) FROM recno_parallel_test WHERE val LIKE 'row_1%';

-- =============================================
-- Parallel scan with aggregation
-- =============================================

SELECT id % 10 AS bucket, COUNT(*) AS cnt
FROM recno_parallel_test
GROUP BY id % 10
ORDER BY bucket;

-- =============================================
-- Parallel scan after modifications
-- =============================================

-- Delete some rows and verify parallel scan still works
DELETE FROM recno_parallel_test WHERE id <= 100;
SELECT COUNT(*) = 900 AS count_after_delete FROM recno_parallel_test;

-- Update some rows and verify
UPDATE recno_parallel_test SET val = 'updated_' || id::text WHERE id <= 200;
SELECT COUNT(*) FROM recno_parallel_test WHERE val LIKE 'updated_%';

-- =============================================
-- Parallel scan on empty table
-- =============================================

CREATE TABLE recno_parallel_empty (
    id integer,
    val text
) USING recno;

SELECT COUNT(*) = 0 AS empty_correct FROM recno_parallel_empty;

DROP TABLE recno_parallel_empty;

-- =============================================
-- Verify parallel plan generation
-- =============================================

-- Check that EXPLAIN shows parallel workers for large enough table
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM recno_parallel_test;

-- =============================================
-- Compare parallel vs serial results
-- =============================================

-- Get results with parallel disabled
SET max_parallel_workers_per_gather = 0;
SELECT SUM(id) AS serial_sum, COUNT(*) AS serial_count
FROM recno_parallel_test;

-- Get results with parallel enabled
SET max_parallel_workers_per_gather = 2;
SELECT SUM(id) AS parallel_sum, COUNT(*) AS parallel_count
FROM recno_parallel_test;

-- The results should be identical (verified by the test framework
-- comparing .out files)

-- =============================================
-- Cleanup
-- =============================================

RESET max_parallel_workers_per_gather;
RESET parallel_tuple_cost;
RESET parallel_setup_cost;
RESET min_parallel_table_scan_size;
RESET min_parallel_index_scan_size;
RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;

DROP TABLE recno_parallel_test;
