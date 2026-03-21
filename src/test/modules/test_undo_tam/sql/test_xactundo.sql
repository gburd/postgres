-- Test transaction-level UNDO (xactundo.c)
--
-- This test validates the transaction-level UNDO management functions in xactundo.c
-- covering AtCommit_XactUndo(), AtAbort_XactUndo(), subtransactions, and
-- per-relation UNDO tracking.
--
-- The test_undo_tam extension provides a table access method that exercises
-- the xactundo.c APIs, allowing us to verify the transaction lifecycle hooks
-- work correctly.

CREATE EXTENSION test_undo_tam;

-- Suppress OID details in error messages for deterministic test output
\set VERBOSITY terse

-- ================================================================
-- Test 1: AtCommit_XactUndo() - Verify cleanup on commit
-- ================================================================
-- After a successful commit, UNDO records should be freed and state reset.
-- We can't directly observe internal state, but we can verify that multiple
-- transactions work correctly (implying proper cleanup).

CREATE TABLE xact_commit_test (id int, data text) USING test_undo_tam;

-- First transaction: insert and commit
BEGIN;
INSERT INTO xact_commit_test VALUES (1, 'first txn');
SELECT * FROM xact_commit_test ORDER BY id;
COMMIT;

-- Verify data persisted
SELECT * FROM xact_commit_test ORDER BY id;

-- Second transaction: insert and commit
-- If AtCommit_XactUndo() didn't clean up properly, this would fail
BEGIN;
INSERT INTO xact_commit_test VALUES (2, 'second txn');
SELECT * FROM xact_commit_test ORDER BY id;
COMMIT;

-- Verify both rows persisted
SELECT * FROM xact_commit_test ORDER BY id;

-- Third transaction with multiple inserts
BEGIN;
INSERT INTO xact_commit_test VALUES (3, 'third txn');
INSERT INTO xact_commit_test VALUES (4, 'third txn');
INSERT INTO xact_commit_test VALUES (5, 'third txn');
COMMIT;

-- All rows should be visible
SELECT COUNT(*) AS should_be_five FROM xact_commit_test;

-- ================================================================
-- Test 2: AtAbort_XactUndo() - Verify UNDO application on abort
-- ================================================================
-- On abort, AtAbort_XactUndo() should apply per-relation UNDO chains
-- to roll back changes.

CREATE TABLE xact_abort_test (id int, data text) USING test_undo_tam;

-- Insert some baseline data
INSERT INTO xact_abort_test VALUES (10, 'baseline');

-- Start a transaction and abort it
BEGIN;
INSERT INTO xact_abort_test VALUES (20, 'will be rolled back');
INSERT INTO xact_abort_test VALUES (30, 'will be rolled back');
SELECT * FROM xact_abort_test ORDER BY id;
ROLLBACK;

-- Process pending UNDO work synchronously
SELECT test_undo_tam_process_pending();

-- Should only see baseline data
SELECT * FROM xact_abort_test ORDER BY id;
SELECT COUNT(*) AS should_be_one FROM xact_abort_test;

-- ================================================================
-- Test 3: Multiple UNDO records in single transaction
-- ================================================================
-- Test that a transaction with many UNDO records is handled correctly.

CREATE TABLE multi_undo_test (id int, data text) USING test_undo_tam;

BEGIN;
-- Generate many UNDO records in one transaction
INSERT INTO multi_undo_test SELECT i, 'row ' || i FROM generate_series(1, 50) i;
SELECT COUNT(*) FROM multi_undo_test;
ROLLBACK;

-- Process pending UNDO work synchronously
SELECT test_undo_tam_process_pending();

-- Table should be empty
SELECT COUNT(*) AS should_be_zero FROM multi_undo_test;

-- Now commit a similar transaction
BEGIN;
INSERT INTO multi_undo_test SELECT i, 'row ' || i FROM generate_series(1, 50) i;
COMMIT;

-- All rows should be visible
SELECT COUNT(*) AS should_be_fifty FROM multi_undo_test;

-- ================================================================
-- Test 4: Subtransactions - SAVEPOINT and ROLLBACK TO SAVEPOINT
-- ================================================================
-- Test subtransaction handling: AtSubCommit_XactUndo() and AtSubAbort_XactUndo()
-- Note: Current implementation has limited subtransaction UNDO support.

CREATE TABLE subxact_test (id int, data text) USING test_undo_tam;

-- Test case 4a: SAVEPOINT with COMMIT
BEGIN;
INSERT INTO subxact_test VALUES (1, 'before savepoint');
SAVEPOINT sp1;
INSERT INTO subxact_test VALUES (2, 'after savepoint');
SAVEPOINT sp2;
INSERT INTO subxact_test VALUES (3, 'after sp2');
-- Commit both savepoints and top-level transaction
COMMIT;

-- All rows should be visible
SELECT * FROM subxact_test ORDER BY id;
SELECT COUNT(*) AS should_be_three FROM subxact_test;

TRUNCATE subxact_test;

-- Test case 4b: ROLLBACK TO SAVEPOINT (known limitation)
-- Subtransaction UNDO is not yet fully implemented, so this documents
-- current behavior.
BEGIN;
INSERT INTO subxact_test VALUES (10, 'before savepoint');
SAVEPOINT sp1;
INSERT INTO subxact_test VALUES (20, 'after sp1 - should rollback');
INSERT INTO subxact_test VALUES (30, 'after sp1 - should rollback');
SELECT * FROM subxact_test ORDER BY id;
ROLLBACK TO sp1;

-- Process pending UNDO (may not apply subtransaction UNDO yet)
SELECT test_undo_tam_process_pending();

-- Due to subtransaction UNDO limitations, rows may still be visible
SELECT * FROM subxact_test ORDER BY id;
COMMIT;

TRUNCATE subxact_test;

-- Test case 4c: Nested savepoints with mixed commit/rollback
BEGIN;
INSERT INTO subxact_test VALUES (100, 'level 0');
SAVEPOINT sp1;
INSERT INTO subxact_test VALUES (200, 'level 1');
SAVEPOINT sp2;
INSERT INTO subxact_test VALUES (300, 'level 2 - will rollback');
ROLLBACK TO sp2;
-- sp2 rolled back, sp1 still active
INSERT INTO subxact_test VALUES (400, 'level 1 again');
COMMIT;

-- Expected: rows 100, 200, 400 (but 300 rolled back)
-- Note: Due to subtxn UNDO limitations, 300 may still appear
SELECT * FROM subxact_test ORDER BY id;

TRUNCATE subxact_test;

-- Test case 4d: Subtransaction abort then top-level commit
BEGIN;
INSERT INTO subxact_test VALUES (1000, 'top level');
SAVEPOINT sp1;
INSERT INTO subxact_test VALUES (2000, 'sub level - will abort');
ROLLBACK TO sp1;
INSERT INTO subxact_test VALUES (3000, 'top level after abort');
COMMIT;

-- Expected: 1000, 3000 (2000 rolled back)
SELECT * FROM subxact_test ORDER BY id;

-- ================================================================
-- Test 5: Prepared transactions with UNDO
-- ================================================================
-- Test that UNDO records survive PREPARE TRANSACTION and are
-- properly handled on COMMIT/ROLLBACK PREPARED.

CREATE TABLE prepared_test (id int, data text) USING test_undo_tam;

-- Test case 5a: PREPARE and COMMIT PREPARED
BEGIN;
INSERT INTO prepared_test VALUES (1, 'prepared transaction');
INSERT INTO prepared_test VALUES (2, 'prepared transaction');
PREPARE TRANSACTION 'test_xact_1';

-- Data not yet committed
SELECT COUNT(*) AS should_be_zero FROM prepared_test;

-- Commit the prepared transaction
COMMIT PREPARED 'test_xact_1';

-- Data should now be visible
SELECT * FROM prepared_test ORDER BY id;
SELECT COUNT(*) AS should_be_two FROM prepared_test;

-- Test case 5b: PREPARE and ROLLBACK PREPARED
BEGIN;
INSERT INTO prepared_test VALUES (10, 'will be rolled back');
INSERT INTO prepared_test VALUES (20, 'will be rolled back');
PREPARE TRANSACTION 'test_xact_2';

-- Data not yet committed
SELECT * FROM prepared_test ORDER BY id;

-- Rollback the prepared transaction
ROLLBACK PREPARED 'test_xact_2';

-- Process pending UNDO work synchronously
SELECT test_undo_tam_process_pending();

-- Should still only see the two rows from test case 5a
SELECT * FROM prepared_test ORDER BY id;
SELECT COUNT(*) AS should_be_two FROM prepared_test;

-- ================================================================
-- Test 6: Multiple persistence levels
-- ================================================================
-- xactundo.c maintains separate record sets for permanent, unlogged,
-- and temporary tables. Test that they are handled independently.

CREATE TABLE perm_test (id int) USING test_undo_tam;
CREATE UNLOGGED TABLE unlog_test (id int) USING test_undo_tam;
CREATE TEMP TABLE temp_test (id int) USING test_undo_tam;

BEGIN;
INSERT INTO perm_test VALUES (1);
INSERT INTO unlog_test VALUES (2);
INSERT INTO temp_test VALUES (3);
SELECT * FROM perm_test;
SELECT * FROM unlog_test;
SELECT * FROM temp_test;
ROLLBACK;

-- Process pending UNDO work synchronously
SELECT test_undo_tam_process_pending();

-- All tables should be empty after rollback
SELECT COUNT(*) AS perm_should_be_zero FROM perm_test;
SELECT COUNT(*) AS unlog_should_be_zero FROM unlog_test;
SELECT COUNT(*) AS temp_should_be_zero FROM temp_test;

-- Now commit
BEGIN;
INSERT INTO perm_test VALUES (10);
INSERT INTO unlog_test VALUES (20);
INSERT INTO temp_test VALUES (30);
COMMIT;

-- All should have one row
SELECT * FROM perm_test;
SELECT * FROM unlog_test;
SELECT * FROM temp_test;

-- ================================================================
-- Test 7: RegisterPerRelUndo() and GetPerRelUndoPtr()
-- ================================================================
-- Test the per-relation UNDO tracking functions.

CREATE TABLE relundo_track_test (id int) USING test_undo_tam;

-- Insert data which triggers RegisterPerRelUndo()
BEGIN;
INSERT INTO relundo_track_test VALUES (1);
INSERT INTO relundo_track_test VALUES (2);
-- Each insert updates the per-relation UNDO pointer via GetPerRelUndoPtr()
COMMIT;

-- Verify data persisted
SELECT COUNT(*) AS should_be_two FROM relundo_track_test;

-- Test abort with multiple relations
CREATE TABLE relundo_a (id int) USING test_undo_tam;
CREATE TABLE relundo_b (id int) USING test_undo_tam;

BEGIN;
INSERT INTO relundo_a VALUES (100);
INSERT INTO relundo_b VALUES (200);
INSERT INTO relundo_a VALUES (101);
INSERT INTO relundo_b VALUES (201);
ROLLBACK;

-- Process pending UNDO work synchronously
SELECT test_undo_tam_process_pending();

-- Both tables should be empty
SELECT COUNT(*) AS relundo_a_empty FROM relundo_a;
SELECT COUNT(*) AS relundo_b_empty FROM relundo_b;

-- ================================================================
-- Test 8: Transaction abort after multiple operations
-- ================================================================
-- Test that AtAbort_XactUndo() correctly applies all UNDO records
-- regardless of the number of operations.

CREATE TABLE complex_abort_test (id int, data text) USING test_undo_tam;

-- Insert baseline data
INSERT INTO complex_abort_test VALUES (1, 'baseline');

BEGIN;
-- Mix of operations on same table
INSERT INTO complex_abort_test VALUES (2, 'abort me');
INSERT INTO complex_abort_test VALUES (3, 'abort me');
INSERT INTO complex_abort_test VALUES (4, 'abort me');
INSERT INTO complex_abort_test VALUES (5, 'abort me');
INSERT INTO complex_abort_test VALUES (6, 'abort me');
SELECT COUNT(*) FROM complex_abort_test;
ROLLBACK;

-- Process pending UNDO work synchronously
SELECT test_undo_tam_process_pending();

-- Should only see baseline
SELECT * FROM complex_abort_test;
SELECT COUNT(*) AS should_be_one FROM complex_abort_test;

-- ================================================================
-- Test 9: Empty transaction (no UNDO generated)
-- ================================================================
-- Test that transactions without UNDO operations are handled correctly.

CREATE TABLE no_undo_test (id int) USING test_undo_tam;

-- Transaction that doesn't modify any UNDO tables
BEGIN;
SELECT 1;
COMMIT;

-- Should succeed without error
SELECT COUNT(*) AS should_be_zero FROM no_undo_test;

-- ================================================================
-- Test 10: AtProcExit_XactUndo() - Process exit cleanup
-- ================================================================
-- We can't directly test process exit, but we can verify that
-- multiple transactions in sequence work correctly, implying
-- proper cleanup at each transaction boundary.

CREATE TABLE proc_exit_test (id int) USING test_undo_tam;

-- Run several transactions in sequence
BEGIN;
INSERT INTO proc_exit_test VALUES (1);
COMMIT;

BEGIN;
INSERT INTO proc_exit_test VALUES (2);
ROLLBACK;

-- Process pending UNDO work synchronously
SELECT test_undo_tam_process_pending();

BEGIN;
INSERT INTO proc_exit_test VALUES (3);
COMMIT;

-- Should see rows 1 and 3 (2 was rolled back)
SELECT * FROM proc_exit_test ORDER BY id;
SELECT COUNT(*) AS should_be_two FROM proc_exit_test;

-- ================================================================
-- Cleanup
-- ================================================================

DROP TABLE xact_commit_test;
DROP TABLE xact_abort_test;
DROP TABLE multi_undo_test;
DROP TABLE subxact_test;
DROP TABLE prepared_test;
DROP TABLE perm_test;
DROP TABLE unlog_test;
DROP TABLE relundo_track_test;
DROP TABLE relundo_a;
DROP TABLE relundo_b;
DROP TABLE complex_abort_test;
DROP TABLE no_undo_test;
DROP TABLE proc_exit_test;

DROP EXTENSION test_undo_tam;
