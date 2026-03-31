<--
-- Additional Noxu Coverage Tests
--
-- These tests are designed to achieve >95% line coverage and >85% branch coverage
-- by exercising code paths not covered by the base noxu.sql test suite.
--

-- Deep B-tree with 100K rows (covers multi-level tree operations)
-- This triggers deep tree splits and complex navigation logic
CREATE TABLE t_deep_btree(id bigserial, data text) USING noxu;
INSERT INTO t_deep_btree(data)
  SELECT 'row_' || i FROM generate_series(1, 100000) i;
SELECT COUNT(*) FROM t_deep_btree;
-- Verify deep tree navigation with range query
SELECT COUNT(*) FROM t_deep_btree WHERE id BETWEEN 50000 AND 50100;
DROP TABLE t_deep_btree;

-- Scattered Delete/Merge Pattern
-- Tests TID array merging logic when gaps are created and filled
CREATE TABLE t_merge(id int, val int) USING noxu;
INSERT INTO t_merge SELECT i, i*2 FROM generate_series(1, 10000) i;
-- Delete every 3rd row to create scattered gaps
DELETE FROM t_merge WHERE id % 3 = 0;
SELECT COUNT(*) FROM t_merge; -- Should be ~6667
-- Insert into gaps (triggers merge logic in TID arrays)
INSERT INTO t_merge SELECT i, i*3 FROM generate_series(1, 10000, 3) i;
SELECT COUNT(*) FROM t_merge; -- Should be ~10000
-- Verify correctness
SELECT COUNT(DISTINCT id) FROM t_merge;
DROP TABLE t_merge;

-- Wide Table (100 columns)
-- Tests attribute page handling with many columns
-- This also tests column projection with wide tables
DO $$
DECLARE
  sql text;
BEGIN
  sql := 'CREATE TABLE t_wide(';
  FOR i IN 1..100 LOOP
    sql := sql || 'col' || i || ' int';
    IF i < 100 THEN
      sql := sql || ', ';
    END IF;
  END LOOP;
  sql := sql || ') USING noxu';
  EXECUTE sql;
END $$;

-- Insert data into wide table
DO $$
DECLARE
  sql text;
  vals text;
BEGIN
  vals := '';
  FOR i IN 1..100 LOOP
    vals := vals || i;
    IF i < 100 THEN
      vals := vals || ', ';
    END IF;
  END LOOP;

  FOR j IN 1..100 LOOP
    sql := 'INSERT INTO t_wide VALUES (' || vals || ')';
    EXECUTE sql;
  END LOOP;
END $$;

-- Test column projection on wide table (should only read subset)
SELECT col1, col50, col100 FROM t_wide LIMIT 1;

-- Count rows
SELECT COUNT(*) FROM t_wide;

DROP TABLE t_wide;

-- Large Transaction with UNDO log
-- Tests UNDO log management with many operations in single transaction
CREATE TABLE t_large_txn(id int, val int) USING noxu;
INSERT INTO t_large_txn SELECT i, i FROM generate_series(1, 10000) i;

-- Large transaction that modifies all rows
BEGIN;
UPDATE t_large_txn SET val = val + 1 WHERE id <= 5000;
UPDATE t_large_txn SET val = val + 2 WHERE id > 5000;
-- Verify within transaction
SELECT COUNT(*) FROM t_large_txn WHERE val = id + 1 OR val = id + 2;
ROLLBACK;

-- Verify rollback worked (all values should be original)
SELECT COUNT(*) FROM t_large_txn WHERE val = id;
SELECT COUNT(*) FROM t_large_txn WHERE val != id;

DROP TABLE t_large_txn;

-- Very Large Values (multi-page overflow chains)
-- Tests overflow handling with values >1MB
CREATE TABLE t_huge_overflow(id int, huge text) USING noxu;
-- Insert 2MB text values (requires multiple overflow pages)
INSERT INTO t_huge_overflow
  SELECT i, repeat('x' || i::text, 200000) FROM generate_series(1, 5) i;

-- Verify lengths
SELECT id, length(huge) FROM t_huge_overflow ORDER BY id;

-- Verify we can fetch partial data
SELECT id, substring(huge from 1 for 10) FROM t_huge_overflow ORDER BY id;

-- Update with another large value
UPDATE t_huge_overflow SET huge = repeat('y', 1500000) WHERE id = 1;
SELECT id, length(huge) FROM t_huge_overflow WHERE id = 1;

DROP TABLE t_huge_overflow;

-- Free Space Reuse Pattern
-- Tests free page map management and reuse
CREATE TABLE t_reuse(id int, data text) USING noxu;
-- Fill table
INSERT INTO t_reuse SELECT i, 'data' || i FROM generate_series(1, 10000) i;
-- Delete half the rows (creates free space)
DELETE FROM t_reuse WHERE id % 2 = 0;
SELECT COUNT(*) FROM t_reuse; -- Should be 5000
-- Insert more rows (should reuse some freed space)
INSERT INTO t_reuse SELECT i, 'new' || i FROM generate_series(10001, 20000) i;
SELECT COUNT(*) FROM t_reuse; -- Should be 15000
-- Verify data integrity
SELECT COUNT(*) FROM t_reuse WHERE data LIKE 'data%';
SELECT COUNT(*) FROM t_reuse WHERE data LIKE 'new%';
DROP TABLE t_reuse;

-- Mixed Workload (INSERT/UPDATE/DELETE interleaved)
-- Tests various code paths in combination
CREATE TABLE t_mixed(id int PRIMARY KEY, val int, txt text) USING noxu;

-- Interleaved operations
INSERT INTO t_mixed SELECT i, i*2, 'text'||i FROM generate_series(1, 1000) i;
UPDATE t_mixed SET val = val * 2 WHERE id % 10 = 0;
DELETE FROM t_mixed WHERE id % 7 = 0;
INSERT INTO t_mixed SELECT i, i*3, 'new'||i FROM generate_series(1001, 2000) i;
UPDATE t_mixed SET txt = 'updated' WHERE id > 1500;
DELETE FROM t_mixed WHERE id BETWEEN 500 AND 600;

-- Verify final state
SELECT COUNT(*) FROM t_mixed;

-- Test index on mixed workload table
CREATE INDEX ON t_mixed(val);
SET enable_seqscan = off;
SELECT COUNT(*) FROM t_mixed WHERE val < 100;
SET enable_seqscan = on;

DROP TABLE t_mixed;

-- Transaction Isolation and Visibility
-- Tests visibility checks and MVCC behavior
CREATE TABLE t_visibility(id int, val int) USING noxu;
INSERT INTO t_visibility VALUES (1, 100), (2, 200), (3, 300);

-- UPDATE visibility
BEGIN;
UPDATE t_visibility SET val = 150 WHERE id = 1;
-- Within same transaction, should see update
SELECT val FROM t_visibility WHERE id = 1;
COMMIT;
-- After commit, update should be visible
SELECT val FROM t_visibility WHERE id = 1;

-- DELETE visibility
BEGIN;
DELETE FROM t_visibility WHERE id = 2;
-- Within transaction, row should be gone
SELECT COUNT(*) FROM t_visibility WHERE id = 2;
ROLLBACK;
-- After rollback, row should be back
SELECT COUNT(*) FROM t_visibility WHERE id = 2;

-- INSERT visibility
BEGIN;
INSERT INTO t_visibility VALUES (4, 400);
-- Within transaction, new row visible
SELECT COUNT(*) FROM t_visibility WHERE id = 4;
ROLLBACK;
-- After rollback, row should not exist
SELECT COUNT(*) FROM t_visibility WHERE id = 4;

DROP TABLE t_visibility;

-- Edge Cases

-- Empty table operations
CREATE TABLE t_empty(id int, val int) USING noxu;
-- SELECT on empty table
SELECT * FROM t_empty;
SELECT COUNT(*) FROM t_empty;
-- UPDATE on empty table
UPDATE t_empty SET val = 100;
-- DELETE on empty table
DELETE FROM t_empty;
-- VACUUM on empty table
VACUUM t_empty;
DROP TABLE t_empty;

-- Single row table
CREATE TABLE t_single(id int) USING noxu;
INSERT INTO t_single VALUES (1);
SELECT * FROM t_single;
UPDATE t_single SET id = 2;
SELECT * FROM t_single;
DELETE FROM t_single;
SELECT * FROM t_single;
DROP TABLE t_single;

-- Column Operations

-- Add multiple columns of different types
CREATE TABLE t_addcols(a int) USING noxu;
INSERT INTO t_addcols VALUES (1), (2), (3);

-- Add int column with default
ALTER TABLE t_addcols ADD COLUMN b int DEFAULT 10;
SELECT * FROM t_addcols;

-- Add text column with default
ALTER TABLE t_addcols ADD COLUMN c text DEFAULT 'hello';
SELECT * FROM t_addcols;

-- Add column without default
ALTER TABLE t_addcols ADD COLUMN d int;
SELECT * FROM t_addcols;

-- Insert after multiple ALTERs
INSERT INTO t_addcols VALUES (4, 20, 'world', 30);
SELECT * FROM t_addcols ORDER BY a;

DROP TABLE t_addcols;

-- Compression Verification

-- Create table with compressible data
CREATE TABLE t_compress(id int, data text) USING noxu;

-- Insert highly compressible data (repeated patterns)
INSERT INTO t_compress
  SELECT i, repeat('compressible_data_', 1000)
  FROM generate_series(1, 100) i;

-- Verify data integrity after compression
SELECT id, length(data), substring(data from 1 for 30)
  FROM t_compress
  WHERE id <= 5
  ORDER BY id;

-- Insert incompressible data (random)
INSERT INTO t_compress
  SELECT i, md5(random()::text)
  FROM generate_series(101, 200) i;

SELECT COUNT(*) FROM t_compress;

DROP TABLE t_compress;

-- Stress Test - Many Small Transactions

-- Simulate workload with many small transactions
CREATE TABLE t_stress(id int, val int) USING noxu;

DO $$
BEGIN
  FOR i IN 1..100 LOOP
    BEGIN
      INSERT INTO t_stress VALUES (i, i*10);
      UPDATE t_stress SET val = val + 1 WHERE id = i;
      IF i % 10 = 0 THEN
        ROLLBACK;
      ELSE
        COMMIT;
      END IF;
    END;
  END LOOP;
END $$;

-- Should have ~90 rows (10 rolled back)
SELECT COUNT(*) FROM t_stress;

DROP TABLE t_stress;
