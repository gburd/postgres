--
-- bench_update.sql
--
-- Measures UPDATE performance and storage bloat.
-- RECNO should excel at in-place updates (no dead tuples).
-- HEAP creates dead tuple versions requiring VACUUM.
--
-- Validates design doc claim: 40-60% less bloat than heap.
--

\timing on

-- ======================================================================
-- Setup: Create identical 100K-row tables
-- ======================================================================
\echo '=== Update Benchmark Setup ==='

DROP TABLE IF EXISTS heap_update_test CASCADE;
DROP TABLE IF EXISTS recno_update_test CASCADE;

CREATE TABLE heap_update_test (
    id       INT4 PRIMARY KEY,
    counter  INT4,
    status   TEXT,
    amount   NUMERIC(12,2),
    notes    TEXT
) USING heap;

CREATE TABLE recno_update_test (
    id       INT4 PRIMARY KEY,
    counter  INT4,
    status   TEXT,
    amount   NUMERIC(12,2),
    notes    TEXT
) USING recno;

INSERT INTO heap_update_test
SELECT i, 0, 'active',
       (random() * 10000)::numeric(12,2),
       'Initial note for record ' || i
FROM generate_series(1, 100000) i;

INSERT INTO recno_update_test
SELECT i, 0, 'active',
       (random() * 10000)::numeric(12,2),
       'Initial note for record ' || i
FROM generate_series(1, 100000) i;

-- Record baseline sizes
SELECT
    'Baseline' AS phase,
    'heap' AS am,
    pg_size_pretty(pg_relation_size('heap_update_test')) AS table_size,
    pg_relation_size('heap_update_test') AS size_bytes
UNION ALL
SELECT
    'Baseline',
    'recno',
    pg_size_pretty(pg_relation_size('recno_update_test')),
    pg_relation_size('recno_update_test');

-- ======================================================================
-- Test 1: In-place numeric update (same-size, no row growth)
-- This is RECNO's sweet spot: counter increment, no size change.
-- ======================================================================
\echo '=== Test 1: In-Place Counter Increment (50K updates) ==='

\echo 'HEAP:'
UPDATE heap_update_test SET counter = counter + 1
WHERE id <= 50000;

\echo 'RECNO:'
UPDATE recno_update_test SET counter = counter + 1
WHERE id <= 50000;

SELECT
    'After 50K counter updates' AS phase,
    'heap' AS am,
    pg_size_pretty(pg_relation_size('heap_update_test')) AS table_size,
    pg_relation_size('heap_update_test') AS size_bytes
UNION ALL
SELECT
    'After 50K counter updates',
    'recno',
    pg_size_pretty(pg_relation_size('recno_update_test')),
    pg_relation_size('recno_update_test');

-- ======================================================================
-- Test 2: Repeated updates (simulates high-update OLTP workload)
-- Each row updated 5 times. HEAP creates 5 dead versions per row.
-- ======================================================================
\echo '=== Test 2: Repeated Updates (5 rounds x 20K rows) ==='

DO $$
BEGIN
    FOR round IN 1..5 LOOP
        UPDATE heap_update_test
        SET counter = counter + 1,
            amount = amount + 1.00
        WHERE id BETWEEN 1 AND 20000;
    END LOOP;
END $$;

DO $$
BEGIN
    FOR round IN 1..5 LOOP
        UPDATE recno_update_test
        SET counter = counter + 1,
            amount = amount + 1.00
        WHERE id BETWEEN 1 AND 20000;
    END LOOP;
END $$;

SELECT
    'After 5x20K repeated updates' AS phase,
    'heap' AS am,
    pg_size_pretty(pg_relation_size('heap_update_test')) AS table_size,
    pg_relation_size('heap_update_test') AS size_bytes
UNION ALL
SELECT
    'After 5x20K repeated updates',
    'recno',
    pg_size_pretty(pg_relation_size('recno_update_test')),
    pg_relation_size('recno_update_test');

-- Show bloat difference
SELECT
    'Storage bloat comparison' AS metric,
    pg_relation_size('heap_update_test') AS heap_bytes,
    pg_relation_size('recno_update_test') AS recno_bytes,
    CASE WHEN pg_relation_size('heap_update_test') > 0
         THEN round(100.0 * (1.0 - pg_relation_size('recno_update_test')::numeric /
              pg_relation_size('heap_update_test')), 1)
         ELSE 0
    END AS recno_savings_pct;

-- ======================================================================
-- Test 3: Variable-length field update (may cause row movement)
-- Status field changes length: 'active' (6) -> 'pending_review' (14)
-- ======================================================================
\echo '=== Test 3: Variable-Length Field Update (30K rows) ==='

\echo 'HEAP:'
UPDATE heap_update_test
SET status = 'pending_review',
    notes = 'Updated status to pending review at ' || now()::text
WHERE id BETWEEN 30001 AND 60000;

\echo 'RECNO:'
UPDATE recno_update_test
SET status = 'pending_review',
    notes = 'Updated status to pending review at ' || now()::text
WHERE id BETWEEN 30001 AND 60000;

SELECT
    'After variable-length updates' AS phase,
    'heap' AS am,
    pg_size_pretty(pg_relation_size('heap_update_test')) AS table_size,
    pg_relation_size('heap_update_test') AS size_bytes
UNION ALL
SELECT
    'After variable-length updates',
    'recno',
    pg_size_pretty(pg_relation_size('recno_update_test')),
    pg_relation_size('recno_update_test');

-- ======================================================================
-- Test 4: VACUUM impact
-- HEAP should reclaim significant space; RECNO should have little to reclaim.
-- ======================================================================
\echo '=== Test 4: Post-VACUUM Sizes ==='

VACUUM heap_update_test;
VACUUM recno_update_test;

SELECT
    'After VACUUM' AS phase,
    'heap' AS am,
    pg_size_pretty(pg_relation_size('heap_update_test')) AS table_size,
    pg_relation_size('heap_update_test') AS size_bytes
UNION ALL
SELECT
    'After VACUUM',
    'recno',
    pg_size_pretty(pg_relation_size('recno_update_test')),
    pg_relation_size('recno_update_test');

-- ======================================================================
-- Test 5: Full update cycle + VACUUM FULL comparison
-- ======================================================================
\echo '=== Test 5: VACUUM FULL comparison ==='

VACUUM FULL heap_update_test;
VACUUM FULL recno_update_test;

SELECT
    'After VACUUM FULL' AS phase,
    'heap' AS am,
    pg_size_pretty(pg_relation_size('heap_update_test')) AS table_size,
    pg_relation_size('heap_update_test') AS size_bytes
UNION ALL
SELECT
    'After VACUUM FULL',
    'recno',
    pg_size_pretty(pg_relation_size('recno_update_test')),
    pg_relation_size('recno_update_test');

-- Verify data integrity after all updates
SELECT
    'heap' AS am,
    count(*) AS rows,
    sum(counter) AS total_counter,
    count(DISTINCT status) AS distinct_statuses
FROM heap_update_test
UNION ALL
SELECT
    'recno',
    count(*),
    sum(counter),
    count(DISTINCT status)
FROM recno_update_test;

\timing off
