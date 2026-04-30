--
-- undo_toast.sql
--
-- Tests for transactional TOAST UNDO: rolling back UPDATEs on TOASTed
-- columns must fully restore the original large datum.
--
-- Requires enable_undo = on at server startup (set in undo_regress.conf).
--

-- ================================================================
-- Setup
-- ================================================================
CREATE TABLE undo_toast_tbl (
    id      integer PRIMARY KEY,
    payload text
) WITH (enable_undo = on);

-- Insert rows with large TOASTed values (> 8KB to force TOAST storage)
INSERT INTO undo_toast_tbl VALUES (1, repeat('x', 100000));
INSERT INTO undo_toast_tbl VALUES (2, repeat('y', 100000));

-- ================================================================
-- Test 1: UPDATE of TOASTed column + ROLLBACK restores old value
-- ================================================================
BEGIN;
UPDATE undo_toast_tbl SET payload = repeat('z', 100000) WHERE id = 1;
ROLLBACK;

-- After rollback, old value must be fully restored
SELECT id, length(payload), left(payload, 5) FROM undo_toast_tbl ORDER BY id;

-- ================================================================
-- Test 2: DELETE of TOASTed row + ROLLBACK restores row
-- ================================================================
BEGIN;
DELETE FROM undo_toast_tbl WHERE id = 2;
ROLLBACK;

SELECT id, length(payload) FROM undo_toast_tbl ORDER BY id;

-- ================================================================
-- Test 3: Subtransaction ROLLBACK restores TOASTed value
-- ================================================================
BEGIN;
SAVEPOINT sp1;
UPDATE undo_toast_tbl SET payload = repeat('a', 100000) WHERE id = 1;
ROLLBACK TO SAVEPOINT sp1;
COMMIT;

SELECT id, length(payload), left(payload, 5) FROM undo_toast_tbl ORDER BY id;

-- ================================================================
-- Test 4: ALTER TABLE SET (enable_undo=off) propagates to TOAST table
-- ================================================================
ALTER TABLE undo_toast_tbl SET (enable_undo = off);

-- Check that the TOAST table's reloptions were updated
SELECT (SELECT option_value FROM pg_options_to_table(c.reloptions)
        WHERE option_name = 'enable_undo') AS undo_setting
FROM pg_class c
WHERE c.oid = (SELECT reltoastrelid FROM pg_class WHERE relname = 'undo_toast_tbl');

-- Re-enable
ALTER TABLE undo_toast_tbl SET (enable_undo = on);

SELECT (SELECT option_value FROM pg_options_to_table(c.reloptions)
        WHERE option_name = 'enable_undo') AS undo_setting
FROM pg_class c
WHERE c.oid = (SELECT reltoastrelid FROM pg_class WHERE relname = 'undo_toast_tbl');

-- ================================================================
-- Test 5: CREATE TABLE with enable_undo propagates to TOAST table
-- ================================================================
CREATE TABLE undo_toast_tbl2 (id int, payload text) WITH (enable_undo = on);
INSERT INTO undo_toast_tbl2 VALUES (1, repeat('q', 100000));

-- Verify TOAST table has enable_undo set
SELECT (SELECT option_value FROM pg_options_to_table(c.reloptions)
        WHERE option_name = 'enable_undo') AS undo_setting
FROM pg_class c
WHERE c.oid = (SELECT reltoastrelid FROM pg_class WHERE relname = 'undo_toast_tbl2');

-- ================================================================
-- Cleanup
-- ================================================================
DROP TABLE undo_toast_tbl;
DROP TABLE undo_toast_tbl2;
