-- Test rollback capability for per-relation UNDO
--
-- This test verifies that transaction rollback correctly applies
-- per-relation UNDO chains to undo changes.
--
-- Per-relation UNDO is applied asynchronously by background workers.
-- After each ROLLBACK we call test_undo_tam_process_pending() to drain
-- the work queue synchronously so the results are immediately visible.

CREATE EXTENSION test_relundo_am;

-- ================================================================
-- Test 1: INSERT rollback
-- ================================================================

CREATE TABLE rollback_test (id int, data text) USING test_relundo_am;

-- Insert and rollback
BEGIN;
INSERT INTO rollback_test VALUES (1, 'should rollback');
INSERT INTO rollback_test VALUES (2, 'also rollback');
SELECT * FROM rollback_test ORDER BY id;
ROLLBACK;

-- Process pending UNDO work synchronously
SELECT test_undo_tam_process_pending();

-- Table should be empty after rollback
SELECT * FROM rollback_test;
SELECT COUNT(*) AS should_be_zero FROM rollback_test;

-- ================================================================
-- Test 2: Multiple operations then rollback
-- ================================================================

-- Insert some data and commit
BEGIN;
INSERT INTO rollback_test VALUES (10, 'committed');
INSERT INTO rollback_test VALUES (20, 'committed');
COMMIT;

-- Verify data is there
SELECT * FROM rollback_test ORDER BY id;

-- Now do more operations and rollback
BEGIN;
INSERT INTO rollback_test VALUES (30, 'will rollback');
INSERT INTO rollback_test VALUES (40, 'will rollback');
SELECT * FROM rollback_test ORDER BY id;
ROLLBACK;

-- Process pending UNDO work synchronously
SELECT test_undo_tam_process_pending();

-- Should only see the committed data
SELECT * FROM rollback_test ORDER BY id;
SELECT COUNT(*) AS should_be_two FROM rollback_test;

-- ================================================================
-- Test 3: Multiple tables with rollback
-- ================================================================

CREATE TABLE rollback_a (id int) USING test_relundo_am;
CREATE TABLE rollback_b (id int) USING test_relundo_am;

-- Insert and commit to both
BEGIN;
INSERT INTO rollback_a VALUES (1);
INSERT INTO rollback_b VALUES (100);
COMMIT;

-- Insert more and rollback
BEGIN;
INSERT INTO rollback_a VALUES (2), (3);
INSERT INTO rollback_b VALUES (200), (300);
SELECT * FROM rollback_a ORDER BY id;
SELECT * FROM rollback_b ORDER BY id;
ROLLBACK;

-- Process pending UNDO work synchronously
SELECT test_undo_tam_process_pending();

-- Should only see the committed rows
SELECT * FROM rollback_a ORDER BY id;
SELECT * FROM rollback_b ORDER BY id;

-- ================================================================
-- Test 4: Savepoint rollback (known limitation)
--
-- Subtransaction UNDO is not yet implemented. ROLLBACK TO SAVEPOINT
-- does not queue per-relation UNDO work, so the data inserted after
-- the savepoint remains visible. This test documents the current
-- behavior until subtransaction UNDO support is added.
-- ================================================================

CREATE TABLE savepoint_test (id int, data text) USING test_relundo_am;

BEGIN;
INSERT INTO savepoint_test VALUES (1, 'before savepoint');
SAVEPOINT sp1;
INSERT INTO savepoint_test VALUES (2, 'after savepoint - will rollback');
INSERT INTO savepoint_test VALUES (3, 'after savepoint - will rollback');
SELECT * FROM savepoint_test ORDER BY id;
ROLLBACK TO sp1;

-- Process pending UNDO work synchronously (returns 0: subtxn UNDO not yet implemented)
SELECT test_undo_tam_process_pending();

-- Currently shows all rows (subtransaction UNDO not yet applied)
SELECT * FROM savepoint_test ORDER BY id;
COMMIT;

-- All rows visible after commit (subtransaction UNDO limitation)
SELECT * FROM savepoint_test;

-- ================================================================
-- Test 5: Coexistence with standard heap
-- ================================================================

CREATE TABLE heap_table (id int);
CREATE TABLE relundo_table (id int) USING test_relundo_am;

BEGIN;
INSERT INTO heap_table VALUES (1);
INSERT INTO relundo_table VALUES (100);
ROLLBACK;

-- Process pending UNDO work synchronously
SELECT test_undo_tam_process_pending();

-- Both should be empty
SELECT COUNT(*) AS heap_should_be_zero FROM heap_table;
SELECT COUNT(*) AS relundo_should_be_zero FROM relundo_table;

-- Now commit
BEGIN;
INSERT INTO heap_table VALUES (2);
INSERT INTO relundo_table VALUES (200);
COMMIT;

-- Both should have one row
SELECT * FROM heap_table;
SELECT * FROM relundo_table;

-- ================================================================
-- Test 6: Large transaction rollback
-- ================================================================

CREATE TABLE large_rollback (id int, data text) USING test_relundo_am;

BEGIN;
INSERT INTO large_rollback SELECT i, 'row ' || i FROM generate_series(1, 100) i;
SELECT COUNT(*) FROM large_rollback;
ROLLBACK;

-- Process pending UNDO work synchronously
SELECT test_undo_tam_process_pending();

-- Should be empty
SELECT COUNT(*) AS should_be_zero FROM large_rollback;

-- ================================================================
-- Cleanup
-- ================================================================

DROP TABLE rollback_test;
DROP TABLE rollback_a;
DROP TABLE rollback_b;
DROP TABLE savepoint_test;
DROP TABLE heap_table;
DROP TABLE relundo_table;
DROP TABLE large_rollback;

DROP EXTENSION test_relundo_am;
