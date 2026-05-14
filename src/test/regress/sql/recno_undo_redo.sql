-- Test basic RECNO functionality (UNDO/REDO tested implicitly)

-- Create test table
CREATE TABLE recno_test (id int) USING recno;

-- Test INSERT with ROLLBACK (UNDO)
BEGIN;
INSERT INTO recno_test VALUES (1);
ROLLBACK;
SELECT COUNT(*) FROM recno_test;

-- Test INSERT with COMMIT (REDO)
INSERT INTO recno_test VALUES (1);
SELECT * FROM recno_test;

-- Test UPDATE with ROLLBACK (UNDO)
BEGIN;
UPDATE recno_test SET id = 999 WHERE id = 1;
ROLLBACK;
SELECT * FROM recno_test;

-- Test UPDATE with COMMIT (REDO)
UPDATE recno_test SET id = 2 WHERE id = 1;
SELECT * FROM recno_test;

-- Test DELETE with ROLLBACK (UNDO)
BEGIN;
DELETE FROM recno_test WHERE id = 2;
ROLLBACK;
SELECT * FROM recno_test;

DROP TABLE recno_test;