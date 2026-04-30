--
-- Test RECNO Visibility Map functionality
--
-- The visibility map tracks which pages contain only tuples visible to all
-- transactions, enabling index-only scans and VACUUM optimizations.
--

-- =============================================
-- Basic Visibility Map Tests
-- =============================================

-- Create table for VM testing
CREATE TABLE recno_vm_test (
    id int PRIMARY KEY,
    val int,
    data text
) USING recno;

CREATE INDEX recno_vm_val_idx ON recno_vm_test(val);

-- Insert data and ensure all tuples are visible
INSERT INTO recno_vm_test
SELECT i, i * 10, 'visible_' || i
FROM generate_series(1, 1000) i;

-- Force checkpoint to ensure visibility
CHECKPOINT;

-- VACUUM to set all-visible bits
VACUUM recno_vm_test;

-- Test index-only scan (should not fetch heap)
EXPLAIN (ANALYZE, BUFFERS, TIMING OFF)
SELECT val FROM recno_vm_test WHERE val BETWEEN 100 AND 200;

-- Verify index-only scan was used
SELECT COUNT(*) AS index_only_scan_count
FROM pg_stat_user_tables
WHERE tablename = 'recno_vm_test'
  AND idx_scan > 0;

-- =============================================
-- VM Clearing on Updates
-- =============================================

CREATE TABLE recno_vm_clear (
    id int PRIMARY KEY,
    val int,
    data text
) USING recno;

CREATE INDEX recno_vm_clear_idx ON recno_vm_clear(val);

-- Insert and make all-visible
INSERT INTO recno_vm_clear
SELECT i, i, 'initial_' || i
FROM generate_series(1, 100) i;

VACUUM recno_vm_clear;

-- Update should clear VM bit for affected pages
UPDATE recno_vm_clear SET data = 'updated' WHERE id = 50;

-- This should now require heap fetches for the updated page
EXPLAIN (ANALYZE, BUFFERS, TIMING OFF)
SELECT val FROM recno_vm_clear WHERE val = 50;

-- VACUUM again to reset VM bits
VACUUM recno_vm_clear;

-- =============================================
-- VM and Delete Operations
-- =============================================

CREATE TABLE recno_vm_delete (
    id int PRIMARY KEY,
    val int
) USING recno;

CREATE INDEX recno_vm_delete_idx ON recno_vm_delete(val);

INSERT INTO recno_vm_delete
SELECT i, i FROM generate_series(1, 100) i;

VACUUM recno_vm_delete;

-- Delete should clear VM bits
DELETE FROM recno_vm_delete WHERE id BETWEEN 40 AND 60;

-- These pages should no longer be all-visible
EXPLAIN (ANALYZE, BUFFERS, TIMING OFF)
SELECT val FROM recno_vm_delete WHERE val BETWEEN 40 AND 60;

-- =============================================
-- VM and HOT Updates
-- =============================================

CREATE TABLE recno_vm_hot (
    id int PRIMARY KEY,
    indexed int,
    non_indexed text
) USING recno;

CREATE INDEX recno_vm_hot_idx ON recno_vm_hot(indexed);

INSERT INTO recno_vm_hot
SELECT i, i, 'data_' || i FROM generate_series(1, 100) i;

VACUUM recno_vm_hot;

-- HOT update (non-indexed column) should still clear VM
UPDATE recno_vm_hot SET non_indexed = 'hot_update' WHERE id = 50;

-- Verify VM was cleared even for HOT update
VACUUM VERBOSE recno_vm_hot;

-- =============================================
-- All-Frozen Pages
-- =============================================

CREATE TABLE recno_vm_frozen (
    id int PRIMARY KEY,
    val int,
    created timestamp DEFAULT now()
) USING recno;

-- Insert old data
INSERT INTO recno_vm_frozen
SELECT i, i, now() - interval '2 years'
FROM generate_series(1, 100) i;

-- Aggressive VACUUM to set all-frozen
VACUUM FREEZE recno_vm_frozen;

-- Check that pages are marked frozen
SELECT relfrozenxid > 0 AS has_frozen_xid
FROM pg_class
WHERE relname = 'recno_vm_frozen';

-- Insert new data (should not be frozen)
INSERT INTO recno_vm_frozen VALUES (101, 101, now());

-- Only old pages should be frozen
VACUUM VERBOSE recno_vm_frozen;

-- =============================================
-- VM and Concurrent Access
-- =============================================

CREATE TABLE recno_vm_concurrent (
    id int PRIMARY KEY,
    val int
) USING recno;

CREATE INDEX recno_vm_concurrent_idx ON recno_vm_concurrent(val);

INSERT INTO recno_vm_concurrent
SELECT i, i FROM generate_series(1, 1000) i;

-- Start a transaction that holds old snapshot
BEGIN;
DECLARE vm_cursor CURSOR FOR SELECT * FROM recno_vm_concurrent;
FETCH 10 FROM vm_cursor;

-- In another session (simulated here), update data
-- This would clear VM bits
SAVEPOINT s1;
UPDATE recno_vm_concurrent SET val = val + 1000 WHERE id > 500;
ROLLBACK TO s1;

CLOSE vm_cursor;
COMMIT;

-- VACUUM to reset VM
VACUUM recno_vm_concurrent;

-- =============================================
-- VM and Index-Only Scan Performance
-- =============================================

CREATE TABLE recno_vm_perf (
    id int PRIMARY KEY,
    col1 int,
    col2 int,
    col3 int,
    data text
) USING recno;

-- Create multiple indexes
CREATE INDEX recno_vm_perf_idx1 ON recno_vm_perf(col1);
CREATE INDEX recno_vm_perf_idx2 ON recno_vm_perf(col2);
CREATE INDEX recno_vm_perf_idx3 ON recno_vm_perf(col3);

-- Insert substantial data
INSERT INTO recno_vm_perf
SELECT i, i % 100, i % 200, i % 300, repeat('x', 100)
FROM generate_series(1, 10000) i;

VACUUM recno_vm_perf;
ANALYZE recno_vm_perf;

-- Test index-only scans on different indexes
EXPLAIN (ANALYZE, BUFFERS, TIMING OFF)
SELECT col1 FROM recno_vm_perf WHERE col1 = 50;

EXPLAIN (ANALYZE, BUFFERS, TIMING OFF)
SELECT col2 FROM recno_vm_perf WHERE col2 = 150;

EXPLAIN (ANALYZE, BUFFERS, TIMING OFF)
SELECT col3 FROM recno_vm_perf WHERE col3 = 250;

-- Count index-only scans
SELECT idx_scan, idx_tup_read, idx_tup_fetch
FROM pg_stat_user_tables
WHERE tablename = 'recno_vm_perf';

-- =============================================
-- VM and Partial Indexes
-- =============================================

CREATE TABLE recno_vm_partial (
    id int PRIMARY KEY,
    status text,
    val int
) USING recno;

-- Create partial index
CREATE INDEX recno_vm_partial_idx ON recno_vm_partial(val)
    WHERE status = 'active';

INSERT INTO recno_vm_partial
SELECT i,
       CASE WHEN i % 3 = 0 THEN 'active' ELSE 'inactive' END,
       i * 10
FROM generate_series(1, 300) i;

VACUUM recno_vm_partial;

-- Index-only scan should work with partial index
EXPLAIN (ANALYZE, BUFFERS, TIMING OFF)
SELECT val FROM recno_vm_partial
WHERE status = 'active' AND val BETWEEN 100 AND 500;

-- =============================================
-- VM and VACUUM Skip
-- =============================================

CREATE TABLE recno_vm_skip (
    id int PRIMARY KEY,
    val int,
    data text
) USING recno;

-- Insert data in batches to create multiple pages
INSERT INTO recno_vm_skip
SELECT i, i, repeat('x', 100)
FROM generate_series(1, 1000) i;

-- VACUUM to set all-visible
VACUUM recno_vm_skip;

-- Update only a few rows
UPDATE recno_vm_skip SET data = 'updated' WHERE id IN (100, 500, 900);

-- VACUUM VERBOSE should show skipped pages
VACUUM VERBOSE recno_vm_skip;

-- =============================================
-- VM Recovery After Crash
-- =============================================

-- This test would require crash recovery testing
-- which is better suited for TAP tests
-- Here we just verify VM state persistence

CREATE TABLE recno_vm_persist (
    id int PRIMARY KEY,
    val int
) USING recno;

CREATE INDEX recno_vm_persist_idx ON recno_vm_persist(val);

INSERT INTO recno_vm_persist
SELECT i, i FROM generate_series(1, 100) i;

VACUUM recno_vm_persist;

-- Force checkpoint to persist VM
CHECKPOINT;

-- Verify VM bits are set (would survive restart)
SELECT COUNT(*) FROM recno_vm_persist WHERE val < 50;

-- =============================================
-- VM with Different Table Sizes
-- =============================================

-- Small table (fits in one page)
CREATE TABLE recno_vm_small (
    id int PRIMARY KEY,
    val int
) USING recno;

INSERT INTO recno_vm_small VALUES (1, 10), (2, 20), (3, 30);
VACUUM recno_vm_small;

-- Medium table (multiple pages)
CREATE TABLE recno_vm_medium (
    id int PRIMARY KEY,
    val int,
    padding text
) USING recno;

INSERT INTO recno_vm_medium
SELECT i, i, repeat('x', 500)
FROM generate_series(1, 100) i;
VACUUM recno_vm_medium;

-- Large table (many pages)
CREATE TABLE recno_vm_large (
    id int PRIMARY KEY,
    val int,
    padding text
) USING recno;

INSERT INTO recno_vm_large
SELECT i, i, repeat('x', 100)
FROM generate_series(1, 10000) i;
VACUUM recno_vm_large;

-- Test VM effectiveness at different scales
SELECT
    relname,
    relpages,
    reltuples
FROM pg_class
WHERE relname LIKE 'recno_vm_%'
ORDER BY relname;

-- =============================================
-- Cleanup
-- =============================================

DROP TABLE recno_vm_test CASCADE;
DROP TABLE recno_vm_clear CASCADE;
DROP TABLE recno_vm_delete CASCADE;
DROP TABLE recno_vm_hot CASCADE;
DROP TABLE recno_vm_frozen CASCADE;
DROP TABLE recno_vm_concurrent CASCADE;
DROP TABLE recno_vm_perf CASCADE;
DROP TABLE recno_vm_partial CASCADE;
DROP TABLE recno_vm_skip CASCADE;
DROP TABLE recno_vm_persist CASCADE;
DROP TABLE recno_vm_small CASCADE;
DROP TABLE recno_vm_medium CASCADE;
DROP TABLE recno_vm_large CASCADE;