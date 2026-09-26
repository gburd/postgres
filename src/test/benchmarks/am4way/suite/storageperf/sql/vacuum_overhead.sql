--
-- Benchmark: VACUUM Overhead (RECNO vs Heap)
--
-- Heap requires VACUUM to reclaim dead tuples after updates/deletes.
-- RECNO uses UNDO-based cleanup, which should eliminate or reduce
-- the need for VACUUM. This benchmark measures the overhead difference.
--

-- ================================================================
-- Setup
-- ================================================================
DROP TABLE IF EXISTS perf_vacuum_heap;
DROP TABLE IF EXISTS perf_vacuum_recno;

CREATE TABLE perf_vacuum_heap (
    id integer PRIMARY KEY,
    value integer,
    data text
) USING heap;

CREATE TABLE perf_vacuum_recno (
    id integer PRIMARY KEY,
    value integer,
    data text
) USING recno;

INSERT INTO perf_vacuum_heap
SELECT i, i, md5(i::text) FROM generate_series(1, 50000) i;

INSERT INTO perf_vacuum_recno
SELECT i, i, md5(i::text) FROM generate_series(1, 50000) i;

-- ================================================================
-- Phase 1: Create dead tuples via updates
-- ================================================================
\echo '=== Phase 1: Creating Dead Tuples via 5 Update Rounds ==='

UPDATE perf_vacuum_heap SET value = value + 1;
UPDATE perf_vacuum_heap SET value = value + 1;
UPDATE perf_vacuum_heap SET value = value + 1;
UPDATE perf_vacuum_heap SET value = value + 1;
UPDATE perf_vacuum_heap SET value = value + 1;

UPDATE perf_vacuum_recno SET value = value + 1;
UPDATE perf_vacuum_recno SET value = value + 1;
UPDATE perf_vacuum_recno SET value = value + 1;
UPDATE perf_vacuum_recno SET value = value + 1;
UPDATE perf_vacuum_recno SET value = value + 1;

-- Show sizes before vacuum (heap should be bloated)
\echo ''
\echo '=== Pre-VACUUM Storage Sizes ==='
SELECT 'HEAP' AS am,
       pg_size_pretty(pg_relation_size('perf_vacuum_heap')) AS table_size,
       pg_size_pretty(pg_total_relation_size('perf_vacuum_heap')) AS total_size
UNION ALL
SELECT 'RECNO' AS am,
       pg_size_pretty(pg_relation_size('perf_vacuum_recno')) AS table_size,
       pg_size_pretty(pg_total_relation_size('perf_vacuum_recno')) AS total_size;

-- Dead tuple stats (heap should have many, RECNO should have few/none)
\echo ''
\echo '=== Dead Tuple Statistics ==='
SELECT 'HEAP' AS am, n_dead_tup, n_live_tup
  FROM pg_stat_user_tables
 WHERE relname = 'perf_vacuum_heap'
UNION ALL
SELECT 'RECNO' AS am, n_dead_tup, n_live_tup
  FROM pg_stat_user_tables
 WHERE relname = 'perf_vacuum_recno';

-- ================================================================
-- Phase 2: VACUUM timing
-- ================================================================
\echo ''
\echo '=== VACUUM Timing ==='

\timing on

\echo '--- HEAP VACUUM ---'
VACUUM perf_vacuum_heap;

\echo '--- RECNO VACUUM (should be minimal work) ---'
VACUUM perf_vacuum_recno;

\timing off

-- Post-VACUUM sizes
\echo ''
\echo '=== Post-VACUUM Storage Sizes ==='
SELECT 'HEAP' AS am,
       pg_size_pretty(pg_relation_size('perf_vacuum_heap')) AS table_size,
       pg_size_pretty(pg_total_relation_size('perf_vacuum_heap')) AS total_size
UNION ALL
SELECT 'RECNO' AS am,
       pg_size_pretty(pg_relation_size('perf_vacuum_recno')) AS table_size,
       pg_size_pretty(pg_total_relation_size('perf_vacuum_recno')) AS total_size;

-- ================================================================
-- Phase 3: DELETE + VACUUM cycle
-- ================================================================
\echo ''
\echo '=== Phase 3: DELETE 50% + VACUUM Cycle ==='

\timing on

\echo '--- HEAP delete 50% ---'
DELETE FROM perf_vacuum_heap WHERE id % 2 = 0;

\echo '--- RECNO delete 50% ---'
DELETE FROM perf_vacuum_recno WHERE id % 2 = 0;

\echo '--- HEAP VACUUM after delete ---'
VACUUM perf_vacuum_heap;

\echo '--- RECNO VACUUM after delete ---'
VACUUM perf_vacuum_recno;

\timing off

-- Final sizes
\echo ''
\echo '=== Post-Delete+VACUUM Storage Sizes ==='
SELECT 'HEAP' AS am,
       pg_size_pretty(pg_relation_size('perf_vacuum_heap')) AS table_size,
       pg_size_pretty(pg_total_relation_size('perf_vacuum_heap')) AS total_size
UNION ALL
SELECT 'RECNO' AS am,
       pg_size_pretty(pg_relation_size('perf_vacuum_recno')) AS table_size,
       pg_size_pretty(pg_total_relation_size('perf_vacuum_recno')) AS total_size;

-- ================================================================
-- Cleanup
-- ================================================================
DROP TABLE perf_vacuum_heap;
DROP TABLE perf_vacuum_recno;
