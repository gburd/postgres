--
-- Benchmark: Read Performance Under Write Pressure (RECNO vs Heap)
--
-- Measures how read (scan) performance is affected when interleaved
-- with write operations. RECNO's in-place updates and UNDO-based
-- versioning should maintain more consistent read performance than
-- heap's copy-on-write approach which creates dead tuples.
--

-- ================================================================
-- Setup
-- ================================================================
DROP TABLE IF EXISTS perf_ruw_heap;
DROP TABLE IF EXISTS perf_ruw_recno;

CREATE TABLE perf_ruw_heap (
    id integer PRIMARY KEY,
    value integer,
    data text
) USING heap;

CREATE TABLE perf_ruw_recno (
    id integer PRIMARY KEY,
    value integer,
    data text
) USING recno;

INSERT INTO perf_ruw_heap
SELECT i, i % 1000, md5(i::text)
FROM generate_series(1, 50000) i;

INSERT INTO perf_ruw_recno
SELECT i, i % 1000, md5(i::text)
FROM generate_series(1, 50000) i;

-- Create indexes for index scan tests
CREATE INDEX perf_ruw_heap_value_idx ON perf_ruw_heap (value);
CREATE INDEX perf_ruw_recno_value_idx ON perf_ruw_recno (value);

-- ================================================================
-- Baseline: Read performance on clean tables
-- ================================================================
\echo '=== Baseline Sequential Scan Performance ==='

\timing on

\echo '--- HEAP sequential scan (baseline) ---'
SELECT count(*), sum(value) FROM perf_ruw_heap;

\echo '--- RECNO sequential scan (baseline) ---'
SELECT count(*), sum(value) FROM perf_ruw_recno;

\timing off

-- ================================================================
-- Phase 1: Read after updates (no vacuum)
-- ================================================================
\echo ''
\echo '=== Phase 1: Read After 5 Update Rounds (no VACUUM) ==='

UPDATE perf_ruw_heap SET value = value + 1;
UPDATE perf_ruw_heap SET value = value + 1;
UPDATE perf_ruw_heap SET value = value + 1;
UPDATE perf_ruw_heap SET value = value + 1;
UPDATE perf_ruw_heap SET value = value + 1;

UPDATE perf_ruw_recno SET value = value + 1;
UPDATE perf_ruw_recno SET value = value + 1;
UPDATE perf_ruw_recno SET value = value + 1;
UPDATE perf_ruw_recno SET value = value + 1;
UPDATE perf_ruw_recno SET value = value + 1;

\timing on

\echo '--- HEAP sequential scan (after updates, no vacuum) ---'
SELECT count(*), sum(value) FROM perf_ruw_heap;

\echo '--- RECNO sequential scan (after updates, no vacuum) ---'
SELECT count(*), sum(value) FROM perf_ruw_recno;

\timing off

-- ================================================================
-- Phase 2: Index scan after updates
-- ================================================================
\echo ''
\echo '=== Phase 2: Index Scan After Updates ==='
SET enable_seqscan = off;

\timing on

\echo '--- HEAP index scan (after updates) ---'
SELECT count(*) FROM perf_ruw_heap WHERE value BETWEEN 100 AND 200;

\echo '--- RECNO index scan (after updates) ---'
SELECT count(*) FROM perf_ruw_recno WHERE value BETWEEN 100 AND 200;

\timing off

RESET enable_seqscan;

-- ================================================================
-- Phase 3: Interleaved read/write pattern
-- ================================================================
\echo ''
\echo '=== Phase 3: Interleaved Read/Write ==='

\timing on

\echo '--- HEAP: update then read (5 cycles) ---'
UPDATE perf_ruw_heap SET value = value + 1 WHERE id % 5 = 0;
SELECT count(*), sum(value) FROM perf_ruw_heap;
UPDATE perf_ruw_heap SET value = value + 1 WHERE id % 5 = 1;
SELECT count(*), sum(value) FROM perf_ruw_heap;
UPDATE perf_ruw_heap SET value = value + 1 WHERE id % 5 = 2;
SELECT count(*), sum(value) FROM perf_ruw_heap;
UPDATE perf_ruw_heap SET value = value + 1 WHERE id % 5 = 3;
SELECT count(*), sum(value) FROM perf_ruw_heap;
UPDATE perf_ruw_heap SET value = value + 1 WHERE id % 5 = 4;
SELECT count(*), sum(value) FROM perf_ruw_heap;

\echo '--- RECNO: update then read (5 cycles) ---'
UPDATE perf_ruw_recno SET value = value + 1 WHERE id % 5 = 0;
SELECT count(*), sum(value) FROM perf_ruw_recno;
UPDATE perf_ruw_recno SET value = value + 1 WHERE id % 5 = 1;
SELECT count(*), sum(value) FROM perf_ruw_recno;
UPDATE perf_ruw_recno SET value = value + 1 WHERE id % 5 = 2;
SELECT count(*), sum(value) FROM perf_ruw_recno;
UPDATE perf_ruw_recno SET value = value + 1 WHERE id % 5 = 3;
SELECT count(*), sum(value) FROM perf_ruw_recno;
UPDATE perf_ruw_recno SET value = value + 1 WHERE id % 5 = 4;
SELECT count(*), sum(value) FROM perf_ruw_recno;

\timing off

-- ================================================================
-- Phase 4: Read after VACUUM (heap should improve)
-- ================================================================
\echo ''
\echo '=== Phase 4: Read After VACUUM ==='

VACUUM perf_ruw_heap;
VACUUM perf_ruw_recno;

\timing on

\echo '--- HEAP sequential scan (after vacuum) ---'
SELECT count(*), sum(value) FROM perf_ruw_heap;

\echo '--- RECNO sequential scan (after vacuum) ---'
SELECT count(*), sum(value) FROM perf_ruw_recno;

\timing off

-- ================================================================
-- Cleanup
-- ================================================================
DROP TABLE perf_ruw_heap;
DROP TABLE perf_ruw_recno;
