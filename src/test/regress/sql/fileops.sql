--
-- Tests for transactional file operations (FILEOPS)
--

-- ================================================================
-- Section 1: enable_transactional_fileops GUC
-- ================================================================

-- Show current value (default is off)
SHOW enable_transactional_fileops;

-- It's a USERSET GUC, so we can toggle it
SET enable_transactional_fileops = on;
SHOW enable_transactional_fileops;

SET enable_transactional_fileops = off;
SHOW enable_transactional_fileops;

-- Invalid values
SET enable_transactional_fileops = 'invalid';

-- ================================================================
-- Section 2: CREATE TABLE with transactional fileops enabled
-- ================================================================

SET enable_transactional_fileops = on;

CREATE TABLE fileops_t1 (id int, data text);
INSERT INTO fileops_t1 VALUES (1, 'created');
SELECT * FROM fileops_t1;

-- Verify the file was created
SELECT pg_relation_filepath('fileops_t1') IS NOT NULL AS has_filepath;

-- ================================================================
-- Section 3: DROP TABLE with transactional fileops
-- ================================================================

CREATE TABLE fileops_drop_me (id int);
INSERT INTO fileops_drop_me VALUES (1);

DROP TABLE fileops_drop_me;

-- Table should no longer exist
SELECT * FROM fileops_drop_me;

-- ================================================================
-- Section 4: CREATE TABLE in transaction then rollback
-- ================================================================

BEGIN;
CREATE TABLE fileops_rollback (id int);
INSERT INTO fileops_rollback VALUES (1);
SELECT count(*) FROM fileops_rollback;
ROLLBACK;

-- Table should not exist after rollback
SELECT * FROM fileops_rollback;

-- ================================================================
-- Section 5: DROP TABLE in transaction then rollback
-- ================================================================

CREATE TABLE fileops_keep (id int);
INSERT INTO fileops_keep VALUES (42);

BEGIN;
DROP TABLE fileops_keep;
ROLLBACK;

-- Table should still exist after rollback of DROP
SELECT * FROM fileops_keep;

-- ================================================================
-- Section 6: Multiple DDL operations in a single transaction
-- ================================================================

BEGIN;
CREATE TABLE fileops_multi1 (id int);
CREATE TABLE fileops_multi2 (id int);
CREATE TABLE fileops_multi3 (id int);
INSERT INTO fileops_multi1 VALUES (1);
INSERT INTO fileops_multi2 VALUES (2);
INSERT INTO fileops_multi3 VALUES (3);
DROP TABLE fileops_multi2;
COMMIT;

-- multi1 and multi3 should exist, multi2 should not
SELECT * FROM fileops_multi1;
SELECT * FROM fileops_multi3;
SELECT * FROM fileops_multi2;

-- ================================================================
-- Section 7: DDL with subtransactions
-- ================================================================

BEGIN;
CREATE TABLE fileops_sp_parent (id int);
INSERT INTO fileops_sp_parent VALUES (1);

SAVEPOINT sp1;
CREATE TABLE fileops_sp_child (id int);
INSERT INTO fileops_sp_child VALUES (2);
ROLLBACK TO sp1;

-- parent table should still exist within the transaction
SELECT * FROM fileops_sp_parent;
COMMIT;

-- After commit, verify parent exists and child does not
SELECT * FROM fileops_sp_parent;
SELECT * FROM fileops_sp_child;

-- ================================================================
-- Section 8: CREATE TABLE with transactional fileops disabled
-- ================================================================

SET enable_transactional_fileops = off;

CREATE TABLE fileops_nontxn (id int);
INSERT INTO fileops_nontxn VALUES (1);
SELECT * FROM fileops_nontxn;

-- Re-enable for cleanup
SET enable_transactional_fileops = on;

-- ================================================================
-- Section 9: TRUNCATE with transactional fileops
-- ================================================================

CREATE TABLE fileops_trunc (id int);
INSERT INTO fileops_trunc SELECT generate_series(1, 100);
SELECT count(*) FROM fileops_trunc;

BEGIN;
TRUNCATE fileops_trunc;
SELECT count(*) FROM fileops_trunc;
ROLLBACK;

-- Should have all rows back after rollback
SELECT count(*) FROM fileops_trunc;

-- ================================================================
-- Section 10: CREATE INDEX (also creates files)
-- ================================================================

CREATE TABLE fileops_idx (id int);
INSERT INTO fileops_idx SELECT generate_series(1, 100);

BEGIN;
CREATE INDEX fileops_idx_id ON fileops_idx(id);
-- Verify index is usable within transaction
SET enable_seqscan = off;
SELECT count(*) FROM fileops_idx WHERE id = 50;
RESET enable_seqscan;
COMMIT;

-- Index should persist
SELECT count(*) FROM fileops_idx WHERE id = 50;

-- ================================================================
-- Cleanup
-- ================================================================

DROP TABLE fileops_t1;
DROP TABLE fileops_keep;
DROP TABLE fileops_multi1;
DROP TABLE fileops_multi3;
DROP TABLE fileops_sp_parent;
DROP TABLE fileops_nontxn;
DROP TABLE fileops_trunc;
DROP TABLE fileops_idx;
