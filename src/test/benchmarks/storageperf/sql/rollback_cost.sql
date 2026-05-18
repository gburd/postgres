--
-- Benchmark: Rollback Cost (ATM Instant vs Synchronous UNDO)
--
-- Tests the cost of rolling back transactions of various sizes.
-- With undo_instant_abort_threshold = 0, ATM instant abort is used
-- for all transactions. With the default threshold, transactions whose
-- per-relation UNDO stays under the threshold use synchronous UNDO and
-- larger ones use ATM.
--

-- ================================================================
-- Setup
-- ================================================================
DROP TABLE IF EXISTS perf_rollback_test;
CREATE TABLE perf_rollback_test (id integer, data text) USING recno;

-- ================================================================
-- Test 1: Synchronous UNDO rollback (default threshold)
-- ================================================================
\echo '=== Synchronous UNDO Rollback (default threshold) ==='
RESET undo_instant_abort_threshold;
SHOW undo_instant_abort_threshold;

\echo ''
\echo '--- Rollback 100 inserts (sync UNDO) ---'
\timing on
BEGIN;
INSERT INTO perf_rollback_test SELECT i, md5(i::text) FROM generate_series(1, 100) i;
ROLLBACK;
\timing off

SELECT count(*) AS rows_after FROM perf_rollback_test;

\echo '--- Rollback 1,000 inserts (sync UNDO) ---'
\timing on
BEGIN;
INSERT INTO perf_rollback_test SELECT i, md5(i::text) FROM generate_series(1, 1000) i;
ROLLBACK;
\timing off

SELECT count(*) AS rows_after FROM perf_rollback_test;

\echo '--- Rollback 10,000 inserts (sync UNDO) ---'
\timing on
BEGIN;
INSERT INTO perf_rollback_test SELECT i, md5(i::text) FROM generate_series(1, 10000) i;
ROLLBACK;
\timing off

SELECT count(*) AS rows_after FROM perf_rollback_test;

-- ================================================================
-- Test 2: ATM instant abort rollback (threshold = 0)
-- ================================================================
\echo ''
\echo '=== ATM Instant Abort Rollback (threshold = 0) ==='
SET undo_instant_abort_threshold = 0;

\echo '--- Rollback 100 inserts (ATM) ---'
\timing on
BEGIN;
INSERT INTO perf_rollback_test SELECT i, md5(i::text) FROM generate_series(1, 100) i;
ROLLBACK;
\timing off

SELECT count(*) AS rows_after FROM perf_rollback_test;

\echo '--- Rollback 1,000 inserts (ATM) ---'
\timing on
BEGIN;
INSERT INTO perf_rollback_test SELECT i, md5(i::text) FROM generate_series(1, 1000) i;
ROLLBACK;
\timing off

SELECT count(*) AS rows_after FROM perf_rollback_test;

\echo '--- Rollback 10,000 inserts (ATM) ---'
\timing on
BEGIN;
INSERT INTO perf_rollback_test SELECT i, md5(i::text) FROM generate_series(1, 10000) i;
ROLLBACK;
\timing off

SELECT count(*) AS rows_after FROM perf_rollback_test;

\echo '--- Rollback 100,000 inserts (ATM) ---'
\timing on
BEGIN;
INSERT INTO perf_rollback_test SELECT i, md5(i::text) FROM generate_series(1, 100000) i;
ROLLBACK;
\timing off

SELECT count(*) AS rows_after FROM perf_rollback_test;

-- ================================================================
-- Test 3: Compare rollback of updates
-- ================================================================
\echo ''
\echo '=== Update Rollback Comparison ==='

-- Insert baseline data
INSERT INTO perf_rollback_test SELECT i, 'baseline_' || i FROM generate_series(1, 10000) i;

\echo '--- Sync UNDO: rollback update 10,000 rows ---'
RESET undo_instant_abort_threshold;
\timing on
BEGIN;
UPDATE perf_rollback_test SET data = 'modified';
ROLLBACK;
\timing off

SELECT count(*) AS original_preserved
  FROM perf_rollback_test
 WHERE data LIKE 'baseline_%';

\echo '--- ATM: rollback update 10,000 rows ---'
SET undo_instant_abort_threshold = 0;
\timing on
BEGIN;
UPDATE perf_rollback_test SET data = 'modified';
ROLLBACK;
\timing off

SELECT count(*) AS original_preserved
  FROM perf_rollback_test
 WHERE data LIKE 'baseline_%';

-- ================================================================
-- Cleanup
-- ================================================================
RESET undo_instant_abort_threshold;
DROP TABLE perf_rollback_test;
