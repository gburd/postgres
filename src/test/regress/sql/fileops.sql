--
-- Tests for transactional file operations (FILEOPS)
--

-- ================================================================
-- Section 1: CREATE TABLE with transactional fileops
-- ================================================================

CREATE TABLE fileops_t1 (id int, data text);
INSERT INTO fileops_t1 VALUES (1, 'created');
SELECT * FROM fileops_t1;

-- Verify the file was created
SELECT pg_relation_filepath('fileops_t1') IS NOT NULL AS has_filepath;

-- ================================================================
-- Section 2: DROP TABLE with transactional fileops
-- ================================================================

CREATE TABLE fileops_drop_me (id int);
INSERT INTO fileops_drop_me VALUES (1);

DROP TABLE fileops_drop_me;

-- Table should no longer exist
SELECT * FROM fileops_drop_me;

-- ================================================================
-- Section 3: CREATE TABLE in transaction then rollback
-- ================================================================

BEGIN;
CREATE TABLE fileops_rollback (id int);
INSERT INTO fileops_rollback VALUES (1);
SELECT count(*) FROM fileops_rollback;
ROLLBACK;

-- Table should not exist after rollback
SELECT * FROM fileops_rollback;

-- ================================================================
-- Section 4: DROP TABLE in transaction then rollback
-- ================================================================

CREATE TABLE fileops_keep (id int);
INSERT INTO fileops_keep VALUES (42);

BEGIN;
DROP TABLE fileops_keep;
ROLLBACK;

-- Table should still exist after rollback of DROP
SELECT * FROM fileops_keep;

-- ================================================================
-- Section 5: Multiple DDL operations in a single transaction
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
-- Section 6: DDL with subtransactions
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
-- Section 7: TRUNCATE with transactional fileops
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
-- Section 8: CREATE INDEX (also creates files)
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
-- Section 9: CREATE DATABASE with FILEOPS integration
-- (WAL_LOG strategy uses CreateDirAndVersionFile with FileOps)
-- ================================================================

CREATE DATABASE fileops_testdb;

-- Verify database exists
SELECT datname FROM pg_database WHERE datname = 'fileops_testdb';

DROP DATABASE fileops_testdb;

-- Verify database is gone
SELECT datname FROM pg_database WHERE datname = 'fileops_testdb';

-- ================================================================
-- Cleanup
-- ================================================================

DROP TABLE fileops_t1;
DROP TABLE fileops_keep;
DROP TABLE fileops_multi1;
DROP TABLE fileops_multi3;
DROP TABLE fileops_sp_parent;
DROP TABLE fileops_trunc;
DROP TABLE fileops_idx;
