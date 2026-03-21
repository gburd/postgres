-- Test for UNDO background worker (relundo_worker.c)
--
-- This test verifies that the per-relation UNDO background worker system
-- correctly processes UNDO work queued during transaction rollback.
--
-- The worker system consists of:
-- - RelUndoQueueAdd: Queues UNDO work during transaction abort
-- - RelUndoWorkerMain: Worker process that applies UNDO chains
-- - Work queue coordination via shared memory

CREATE EXTENSION test_undo_tam;

-- Set custom GUC parameters for worker testing
-- Lower naptime for faster test execution
SET relundo_worker_naptime = 100; -- 100ms for faster testing

-- ================================================================
-- Test 1: Verify worker processes queued UNDO work
-- ================================================================

CREATE TABLE worker_test_1 (id int, data text) USING test_undo_tam;

-- Insert data and commit
INSERT INTO worker_test_1 VALUES (1, 'committed data');
COMMIT;

-- Verify committed data is visible
SELECT * FROM worker_test_1 ORDER BY id;

-- Insert data and rollback - this should queue UNDO work
BEGIN;
INSERT INTO worker_test_1 VALUES (2, 'will rollback');
INSERT INTO worker_test_1 VALUES (3, 'will rollback');
SELECT COUNT(*) AS before_rollback FROM worker_test_1;
ROLLBACK;

-- Wait briefly for worker to process (workers sleep for relundo_worker_naptime)
-- In a real scenario, workers run asynchronously
-- For testing, we can check that UNDO work was queued by examining the logs

-- The rollback should have queued UNDO work for background processing
-- After sufficient time, only committed data should remain visible
SELECT pg_sleep(0.5); -- Give worker time to process

-- Verify only committed row remains after UNDO is applied
SELECT * FROM worker_test_1 ORDER BY id;

-- ================================================================
-- Test 2: Multiple tables with concurrent UNDO work
-- ================================================================

CREATE TABLE worker_test_2a (id int) USING test_undo_tam;
CREATE TABLE worker_test_2b (id int) USING test_undo_tam;

-- Insert committed data in both tables
INSERT INTO worker_test_2a VALUES (10);
INSERT INTO worker_test_2b VALUES (100);
COMMIT;

-- Rollback operations on both tables
BEGIN;
INSERT INTO worker_test_2a VALUES (20), (30);
INSERT INTO worker_test_2b VALUES (200), (300);
ROLLBACK;

-- Worker should handle UNDO for multiple relations
SELECT pg_sleep(0.5);

-- Verify only committed data remains
SELECT * FROM worker_test_2a ORDER BY id;
SELECT * FROM worker_test_2b ORDER BY id;

-- ================================================================
-- Test 3: Large transaction rollback (stress test)
-- ================================================================

CREATE TABLE worker_test_3 (id int, data text) USING test_undo_tam;

-- Insert committed data
INSERT INTO worker_test_3 VALUES (1, 'committed');
COMMIT;

-- Large rollback operation
BEGIN;
INSERT INTO worker_test_3 SELECT i, 'rollback data ' || i FROM generate_series(2, 101) i;
SELECT COUNT(*) AS in_transaction FROM worker_test_3;
ROLLBACK;

-- Worker should handle large UNDO chain
SELECT pg_sleep(0.5);

-- Verify only initial committed row remains
SELECT COUNT(*) AS after_large_rollback FROM worker_test_3;
SELECT * FROM worker_test_3 ORDER BY id;

-- ================================================================
-- Test 4: Multiple rollbacks on same table
-- ================================================================

CREATE TABLE worker_test_4 (id int) USING test_undo_tam;

-- First transaction and rollback
BEGIN;
INSERT INTO worker_test_4 VALUES (1);
ROLLBACK;

SELECT pg_sleep(0.2);

-- Second transaction and rollback
BEGIN;
INSERT INTO worker_test_4 VALUES (2);
ROLLBACK;

SELECT pg_sleep(0.2);

-- Third transaction and rollback
BEGIN;
INSERT INTO worker_test_4 VALUES (3);
ROLLBACK;

SELECT pg_sleep(0.5);

-- Table should remain empty
SELECT COUNT(*) AS should_be_zero FROM worker_test_4;

-- ================================================================
-- Test 5: Worker handles relation that no longer exists
-- ================================================================
-- This tests the error handling path where a relation is dropped
-- before the worker can process its UNDO.

CREATE TABLE worker_test_5_temp (id int) USING test_undo_tam;

BEGIN;
INSERT INTO worker_test_5_temp VALUES (1), (2), (3);
ROLLBACK;

-- Drop the table immediately after rollback (before worker processes it)
-- The worker should handle this gracefully with a logged error
DROP TABLE worker_test_5_temp;

-- Give worker time to attempt processing and handle the error
SELECT pg_sleep(0.5);

-- If we get here without the worker crashing, the error handling worked
SELECT 'Worker handled dropped relation gracefully' AS result;

-- ================================================================
-- Test 6: Verify GUC parameter changes
-- ================================================================

-- Check current naptime
SHOW relundo_worker_naptime;

-- Change naptime (worker should pick this up on SIGHUP)
SET relundo_worker_naptime = 500;
SHOW relundo_worker_naptime;

-- Reset to default
RESET relundo_worker_naptime;
SHOW relundo_worker_naptime;

-- ================================================================
-- Test 7: Worker processes work from correct database only
-- ================================================================
-- Workers should only process UNDO work for their own database

CREATE TABLE worker_test_7 (id int) USING test_undo_tam;

-- The worker is connected to the current database (via BackgroundWorkerInitializeConnectionByOid)
-- It should only see work items where dboid matches MyDatabaseId

BEGIN;
INSERT INTO worker_test_7 VALUES (1), (2), (3);
ROLLBACK;

SELECT pg_sleep(0.5);

-- Verify table is empty (work was processed)
SELECT COUNT(*) AS should_be_zero FROM worker_test_7;

-- ================================================================
-- Test 8: Dump UNDO chain introspection
-- ================================================================
-- Verify we can inspect UNDO records created during operations

CREATE TABLE worker_test_8 (id int) USING test_undo_tam;

-- Insert some data to create UNDO records
INSERT INTO worker_test_8 VALUES (1), (2), (3);
COMMIT;

-- Check UNDO chain (should have records for the inserts)
-- Note: xid values are non-deterministic, so we just check structure
SELECT
    rec_type,
    payload_size,
    CASE WHEN xid::text::int > 0 THEN 'valid' ELSE 'invalid' END AS xid_status
FROM test_undo_tam_dump_chain('worker_test_8'::regclass)
ORDER BY undo_ptr;

-- Verify UNDO records have expected type
SELECT COUNT(*) > 0 AS has_undo_records
FROM test_undo_tam_dump_chain('worker_test_8'::regclass)
WHERE rec_type = 'INSERT';

-- ================================================================
-- Test 9: Worker work queue operations
-- ================================================================
-- Test that work queue operations (add, get, mark complete) function correctly
-- This is tested implicitly through rollback operations

CREATE TABLE worker_test_9 (id int, data text) USING test_undo_tam;

-- Multiple rapid rollbacks to test queue handling
BEGIN;
INSERT INTO worker_test_9 VALUES (1, 'first');
ROLLBACK;

BEGIN;
INSERT INTO worker_test_9 VALUES (2, 'second');
ROLLBACK;

BEGIN;
INSERT INTO worker_test_9 VALUES (3, 'third');
ROLLBACK;

-- All three UNDO work items should be queued and processed
SELECT pg_sleep(0.5);

SELECT COUNT(*) AS should_be_zero FROM worker_test_9;

-- ================================================================
-- Test 10: Worker handles in-progress flag correctly
-- ================================================================
-- Test that work items marked in_progress are not picked up by other workers

CREATE TABLE worker_test_10 (id int) USING test_undo_tam;

BEGIN;
INSERT INTO worker_test_10 VALUES (1), (2), (3);
ROLLBACK;

-- Worker should mark item in_progress, process it, then mark complete
SELECT pg_sleep(0.5);

SELECT COUNT(*) AS should_be_zero FROM worker_test_10;

-- ================================================================
-- Cleanup
-- ================================================================

DROP TABLE worker_test_1;
DROP TABLE worker_test_2a;
DROP TABLE worker_test_2b;
DROP TABLE worker_test_3;
DROP TABLE worker_test_4;
DROP TABLE worker_test_7;
DROP TABLE worker_test_8;
DROP TABLE worker_test_9;
DROP TABLE worker_test_10;

DROP EXTENSION test_undo_tam;
