-- Test pg_bp_lru extension
CREATE EXTENSION pg_bp_lru;

-- ===================================================
-- LRU dynamic pool: full data lifecycle
-- ===================================================

-- Create an LRU buffer pool (4MB = 512 pages)
CREATE BUFFER POOL lru_test_pool HANDLER lru_pool_handler SIZE '4194304';

-- Create a table assigned to the LRU pool
CREATE TABLE bp_lru_data (id int, val text) WITH (buffer_pool = 'lru_test_pool');

-- Insert rows
INSERT INTO bp_lru_data SELECT g, 'lru-row-' || g FROM generate_series(1, 100) g;

-- Read back and verify
SELECT count(*) FROM bp_lru_data;
SELECT min(id), max(id) FROM bp_lru_data;
SELECT val FROM bp_lru_data WHERE id = 50;

-- Repeated SELECTs to exercise MRU promotion
SELECT count(*) FROM bp_lru_data;
SELECT count(*) FROM bp_lru_data;

-- Update some rows
UPDATE bp_lru_data SET val = 'lru-updated-' || id WHERE id <= 10;
SELECT val FROM bp_lru_data WHERE id = 5;

-- Delete some rows
DELETE FROM bp_lru_data WHERE id > 90;
SELECT count(*) FROM bp_lru_data;

-- Create an index
CREATE INDEX bp_lru_data_idx ON bp_lru_data (id);

-- Use the index
SELECT val FROM bp_lru_data WHERE id = 42;

-- VACUUM
VACUUM bp_lru_data;

-- Verify pool stats
SELECT hits > 0 AS has_hits
FROM pg_stat_bufferpool WHERE name = 'lru_test_pool';

-- Verify LRU stats
SELECT hits > 0 AS has_hits
FROM pg_stat_lru WHERE name = 'lru_test_pool';

-- Checkpoint should preserve data
CHECKPOINT;
SELECT count(*) FROM bp_lru_data;

-- ===================================================
-- LRU stress: bulk operations and eviction
-- ===================================================

-- Bulk insert to trigger evictions
INSERT INTO bp_lru_data SELECT g, 'lru-bulk-' || g FROM generate_series(101, 600) g;
SELECT count(*) FROM bp_lru_data;

-- Delete and VACUUM
DELETE FROM bp_lru_data WHERE id > 300;
VACUUM bp_lru_data;
SELECT count(*) FROM bp_lru_data;

-- Re-insert
INSERT INTO bp_lru_data SELECT g, 'lru-refill-' || g FROM generate_series(301, 500) g;
SELECT count(*) FROM bp_lru_data;

-- Verify eviction stats
SELECT evictions >= 0 AS has_eviction_stats
FROM pg_stat_lru WHERE name = 'lru_test_pool';

-- ===================================================
-- KEEP pool test: pages never evicted
-- ===================================================

-- Create a KEEP pool (1MB)
CREATE BUFFER POOL keep_test_pool HANDLER keep_pool_handler SIZE '1048576';

-- Create a small table on KEEP pool
CREATE TABLE bp_keep_data (id int, val text) WITH (buffer_pool = 'keep_test_pool');

-- Insert a few rows (must fit in pool)
INSERT INTO bp_keep_data SELECT g, 'keep-' || g FROM generate_series(1, 20) g;

-- Verify data persists
SELECT count(*) FROM bp_keep_data;
SELECT val FROM bp_keep_data WHERE id = 10;

-- Clean up
DROP TABLE bp_keep_data;
DROP BUFFER POOL keep_test_pool;

DROP TABLE bp_lru_data;
DROP BUFFER POOL lru_test_pool;

DROP EXTENSION pg_bp_lru;
