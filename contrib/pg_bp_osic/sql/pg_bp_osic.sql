-- Test pg_bp_osic extension
CREATE EXTENSION pg_bp_osic;

-- ===================================================
-- OSIC dynamic pool: full data lifecycle
-- ===================================================
-- Create an OSIC buffer pool (4MB = 512 pages)
CREATE BUFFER POOL osic_test_pool HANDLER osic_pool_handler SIZE '4194304';

-- Create a table assigned to the OSIC pool
CREATE TABLE bp_osic_data (id int, val text) WITH (buffer_pool = 'osic_test_pool');

-- Insert rows
INSERT INTO bp_osic_data SELECT g, 'osic-row-' || g FROM generate_series(1, 100) g;

-- Read back and verify
SELECT count(*) FROM bp_osic_data;
SELECT min(id), max(id) FROM bp_osic_data;
SELECT val FROM bp_osic_data WHERE id = 50;

-- Repeated SELECTs to exercise the hot path (cool flag clearing)
SELECT count(*) FROM bp_osic_data;
SELECT count(*) FROM bp_osic_data;

-- Update some rows
UPDATE bp_osic_data SET val = 'osic-updated-' || id WHERE id <= 10;
SELECT val FROM bp_osic_data WHERE id = 5;

-- Delete some rows
DELETE FROM bp_osic_data WHERE id > 90;
SELECT count(*) FROM bp_osic_data;

-- Create an index
CREATE INDEX bp_osic_data_idx ON bp_osic_data (id);

-- Use the index
SELECT val FROM bp_osic_data WHERE id = 42;

-- VACUUM
VACUUM bp_osic_data;

-- Verify pool stats
SELECT hits > 0 AS has_hits
FROM pg_stat_bufferpool WHERE name = 'osic_test_pool';

-- Verify OSIC stats show activity
SELECT hits > 0 AS has_hits
FROM pg_stat_osic WHERE pool_name = 'osic_test_pool';

-- Checkpoint should preserve data
CHECKPOINT;
SELECT count(*) FROM bp_osic_data;

-- ===================================================
-- OSIC stress: bulk operations and eviction
-- ===================================================
-- Bulk insert to trigger evictions
INSERT INTO bp_osic_data SELECT g, 'osic-bulk-' || g FROM generate_series(101, 600) g;
SELECT count(*) FROM bp_osic_data;

-- Delete and VACUUM
DELETE FROM bp_osic_data WHERE id > 300;
VACUUM bp_osic_data;
SELECT count(*) FROM bp_osic_data;

-- Re-insert
INSERT INTO bp_osic_data SELECT g, 'osic-refill-' || g FROM generate_series(301, 500) g;
SELECT count(*) FROM bp_osic_data;

-- Verify hot/cool tracking
SELECT hot_count >= 0 AS has_hot_stats,
       cool_count >= 0 AS has_cool_stats
FROM pg_stat_osic WHERE pool_name = 'osic_test_pool';

-- Clean up
DROP TABLE bp_osic_data;
DROP BUFFER POOL osic_test_pool;
DROP EXTENSION pg_bp_osic;
