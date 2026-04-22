--
-- RECNO ATM (Asynchronous Transaction Manager) Instant Abort Tests
--
-- Tests that the ATM instant abort path correctly undoes operations
-- when transactions are rolled back. Setting undo_instant_abort_threshold = 0
-- forces the ATM path for all transactions regardless of size.
--

-- Force ATM instant abort path for all transactions
SET undo_instant_abort_threshold = 0;

-- ================================================================
-- Setup: Create test table
-- ================================================================
CREATE TABLE recno_atm_test (id int, val text) USING recno;

-- ================================================================
-- Test 1: Small INSERT + ROLLBACK via ATM
-- ================================================================
BEGIN;
INSERT INTO recno_atm_test SELECT g, 'row_' || g FROM generate_series(1, 10) g;
ROLLBACK;

-- Should be 0 rows after ATM instant abort
SELECT count(*) AS after_small_rollback FROM recno_atm_test;

-- ================================================================
-- Test 2: Larger INSERT (1000 rows) + ROLLBACK via ATM
-- ================================================================
BEGIN;
INSERT INTO recno_atm_test SELECT g, 'data_' || g FROM generate_series(1, 1000) g;
ROLLBACK;

-- Should still be 0 rows
SELECT count(*) AS after_large_rollback FROM recno_atm_test;

-- ================================================================
-- Test 3: UPDATE + ROLLBACK via ATM
-- Insert baseline data first, then test update rollback.
-- ================================================================
INSERT INTO recno_atm_test SELECT g, 'original_' || g FROM generate_series(1, 100) g;
SELECT count(*) AS baseline_rows FROM recno_atm_test;

BEGIN;
UPDATE recno_atm_test SET val = 'modified_' || id;
ROLLBACK;

-- Values should be restored to original after ATM instant abort
SELECT count(*) AS rows_after_update_rollback FROM recno_atm_test;
SELECT count(*) AS original_values_preserved
  FROM recno_atm_test
 WHERE val LIKE 'original_%';

-- ================================================================
-- Test 4: DELETE + ROLLBACK via ATM
-- ================================================================
BEGIN;
DELETE FROM recno_atm_test;
ROLLBACK;

-- All rows should still exist after ATM instant abort
SELECT count(*) AS rows_after_delete_rollback FROM recno_atm_test;

-- ================================================================
-- Test 5: Mixed operations in a single transaction + ROLLBACK
-- ================================================================
BEGIN;
INSERT INTO recno_atm_test VALUES (1001, 'new_row');
UPDATE recno_atm_test SET val = 'changed' WHERE id <= 10;
DELETE FROM recno_atm_test WHERE id > 90;
ROLLBACK;

-- Should still have exactly 100 original rows
SELECT count(*) AS rows_after_mixed_rollback FROM recno_atm_test;
SELECT count(*) AS original_values_intact
  FROM recno_atm_test
 WHERE val LIKE 'original_%';

-- ================================================================
-- Test 6: Verify ATM does not interfere with COMMIT
-- ================================================================
BEGIN;
INSERT INTO recno_atm_test VALUES (200, 'committed_row');
COMMIT;

SELECT count(*) AS total_after_commit FROM recno_atm_test;
SELECT val FROM recno_atm_test WHERE id = 200;

-- ================================================================
-- Cleanup
-- ================================================================
RESET undo_instant_abort_threshold;
DROP TABLE recno_atm_test;
