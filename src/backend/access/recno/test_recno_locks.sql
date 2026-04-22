-- Test script for RECNO locking and UNDO/REDO functionality
-- This should be run after building PostgreSQL with RECNO support

-- Create a test table using RECNO storage
CREATE TABLE recno_test_table (
    id INTEGER
) USING recno;

-- Test basic insert (should work)
INSERT INTO recno_test_table VALUES (1);
INSERT INTO recno_test_table VALUES (2);
INSERT INTO recno_test_table VALUES (3);

-- Test READ COMMITTED isolation level
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
INSERT INTO recno_test_table VALUES (100);
-- This should commit successfully
COMMIT;

-- Test REPEATABLE READ isolation level
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
INSERT INTO recno_test_table VALUES (200);
UPDATE recno_test_table SET id = 201 WHERE id = 200;
-- This should commit successfully
COMMIT;

-- Test SERIALIZABLE isolation level
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
INSERT INTO recno_test_table VALUES (300);
UPDATE recno_test_table SET id = 301 WHERE id = 300;
-- This should commit successfully
COMMIT;

-- Test rollback scenario
BEGIN TRANSACTION;
INSERT INTO recno_test_table VALUES (400);
UPDATE recno_test_table SET id = 401 WHERE id = 400;
-- This should rollback and undo the changes
ROLLBACK;

-- Verify final state
SELECT * FROM recno_test_table ORDER BY id;

-- Test concurrent access (would need multiple sessions in real test)
-- For now, just test that locks are properly acquired and released
BEGIN TRANSACTION;
UPDATE recno_test_table SET id = 999 WHERE id = 1;
-- In a real concurrent test, another session would try to access this tuple
-- and should be blocked until this transaction commits
COMMIT;

-- Clean up
DROP TABLE recno_test_table;