--
-- RECNO Integration Test: HOT + VACUUM + FSM + MultiXact
--
-- This test validates the integration between:
-- 1. HOT (Heap-Only Tuples) optimization
-- 2. VACUUM with MultiXact freezing
-- 3. FSM (Free Space Map) management
-- 4. MultiXact concurrent locking
--

-- =============================================
-- Setup
-- =============================================

CREATE TABLE recno_integration_test (
    id integer PRIMARY KEY,
    data text,
    value integer,
    category text
) USING recno;

-- Disable autovacuum early to prevent interference
ALTER TABLE recno_integration_test SET (autovacuum_enabled = false);

-- Create indexes to test HOT optimization
CREATE INDEX idx_value ON recno_integration_test(value);
CREATE INDEX idx_category ON recno_integration_test(category);

-- Insert initial data
INSERT INTO recno_integration_test
SELECT i, 'initial_' || i, i * 10, 'cat_' || (i % 5)
FROM generate_series(1, 100) i;

-- =============================================
-- HOT Updates (non-indexed columns)
-- =============================================

-- These updates should be HOT because 'data' is not indexed
UPDATE recno_integration_test
SET data = 'hot_update_1'
WHERE id BETWEEN 1 AND 20;

-- Verify data after HOT updates
SELECT COUNT(*) FROM recno_integration_test WHERE data = 'hot_update_1';

-- =============================================
-- MultiXact with Concurrent Locks
-- =============================================

-- Lock rows for share (creates MultiXact if multiple sessions)
BEGIN;
SELECT * FROM recno_integration_test
WHERE id IN (10, 20, 30)
FOR SHARE;
COMMIT;

-- =============================================
-- VACUUM with MultiXact Freezing
-- =============================================

-- Create some dead tuples
DELETE FROM recno_integration_test WHERE id BETWEEN 91 AND 100;

-- Create old MultiXacts that need freezing
BEGIN;
SELECT * FROM recno_integration_test WHERE id BETWEEN 21 AND 30 FOR SHARE;
COMMIT;

-- Run VACUUM to clean up
VACUUM recno_integration_test;

-- Verify row count after VACUUM (90 rows: 100 - 10 deleted)
SELECT COUNT(*) FROM recno_integration_test;

-- =============================================
-- FSM Integration with HOT
-- =============================================

-- Fill pages to test FSM allocation
INSERT INTO recno_integration_test
SELECT i, 'filler_' || i, i * 10, 'fill_' || (i % 3)
FROM generate_series(101, 200) i;

-- Delete some tuples to create free space
DELETE FROM recno_integration_test
WHERE id BETWEEN 110 AND 120;

-- VACUUM to update FSM
VACUUM recno_integration_test;

-- Insert should reuse free space from FSM
INSERT INTO recno_integration_test
VALUES (110, 'reused_space', 1100, 'reused');

-- HOT update should use in-page space
UPDATE recno_integration_test
SET data = 'hot_after_fsm'
WHERE id = 110;

-- =============================================
-- Page Pruning with HOT Chains
-- =============================================

-- Create HOT chains
UPDATE recno_integration_test SET data = 'chain_1' WHERE id = 50;
UPDATE recno_integration_test SET data = 'chain_2' WHERE id = 50;
UPDATE recno_integration_test SET data = 'chain_3' WHERE id = 50;
UPDATE recno_integration_test SET data = 'chain_4' WHERE id = 50;

-- Page should be pruned opportunistically during scan
SELECT COUNT(*) FROM recno_integration_test WHERE id = 50;

-- =============================================
-- Index-Only Scans with Visibility Map
-- =============================================

-- VACUUM to set visibility map bits
VACUUM recno_integration_test;

-- Verify we can retrieve data via value index
SELECT COUNT(*) FROM recno_integration_test
WHERE value BETWEEN 100 AND 500;

-- =============================================
-- Foreign Key with MultiXact
-- =============================================

CREATE TABLE recno_parent_integ (
    id integer PRIMARY KEY,
    name text
) USING recno;

ALTER TABLE recno_parent_integ SET (autovacuum_enabled = false);

CREATE TABLE recno_child_integ (
    id integer PRIMARY KEY,
    parent_id integer REFERENCES recno_parent_integ(id),
    data text
) USING recno;

ALTER TABLE recno_child_integ SET (autovacuum_enabled = false);

INSERT INTO recno_parent_integ VALUES (1, 'parent1'), (2, 'parent2');

-- Multiple children reference same parent (creates MultiXact on parent)
INSERT INTO recno_child_integ VALUES
    (1, 1, 'child1_of_1'),
    (2, 1, 'child2_of_1'),
    (3, 2, 'child1_of_2');

-- HOT update on parent (non-key column)
UPDATE recno_parent_integ SET name = 'updated_parent1' WHERE id = 1;

-- VACUUM should handle MultiXact on parent row
VACUUM recno_parent_integ;

-- =============================================
-- Concurrent Updates with HOT
-- =============================================

-- Simulate concurrent HOT updates within a transaction
BEGIN;
UPDATE recno_integration_test SET data = 'concurrent_1' WHERE id = 60;
UPDATE recno_integration_test SET data = 'concurrent_3' WHERE id = 62;
COMMIT;

-- =============================================
-- VACUUM FULL Integration
-- =============================================

-- Create fragmentation with cross-page UPDATE (regression test for CID fix)
UPDATE recno_integration_test SET data = REPEAT('x', 100) WHERE id % 2 = 0;
DELETE FROM recno_integration_test WHERE id % 3 = 0;

-- Count rows before VACUUM FULL
SELECT COUNT(*) FROM recno_integration_test;

-- VACUUM FULL should:
-- 1. Compact the table
-- 2. Rebuild indexes
-- 3. Reset FSM
-- 4. Clear all MultiXacts
VACUUM FULL recno_integration_test;

-- Verify row count after VACUUM FULL (same as before)
SELECT COUNT(*) FROM recno_integration_test;

-- =============================================
-- Verification Queries
-- =============================================

-- Verify data integrity
SELECT COUNT(*) FROM recno_integration_test WHERE data IS NOT NULL;
SELECT COUNT(*) FROM recno_parent_integ;
SELECT COUNT(*) FROM recno_child_integ;

-- =============================================
-- Cleanup
-- =============================================

DROP TABLE recno_child_integ;
DROP TABLE recno_parent_integ;
DROP TABLE recno_integration_test;
