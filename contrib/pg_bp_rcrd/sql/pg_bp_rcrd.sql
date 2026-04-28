-- Test pg_bp_rcrd extension
CREATE EXTENSION pg_bp_rcrd;

-- ===================================================
-- RCRD dynamic pool: full data lifecycle
-- ===================================================

-- Create an RCRD buffer pool (4MB = 512 pages)
CREATE BUFFER POOL rcrd_test_pool HANDLER rcrd_pool_handler SIZE '4194304';

-- Create a table assigned to the RCRD pool
CREATE TABLE bp_rcrd_data (id int, val text) WITH (buffer_pool = 'rcrd_test_pool');

-- Insert rows
INSERT INTO bp_rcrd_data SELECT g, 'rcrd-row-' || g FROM generate_series(1, 100) g;

-- Read back and verify
SELECT count(*) FROM bp_rcrd_data;
SELECT min(id), max(id) FROM bp_rcrd_data;
SELECT val FROM bp_rcrd_data WHERE id = 50;

-- Repeated SELECT to trigger HOT recency list operations
SELECT count(*) FROM bp_rcrd_data;
SELECT count(*) FROM bp_rcrd_data;

-- Update some rows
UPDATE bp_rcrd_data SET val = 'rcrd-updated-' || id WHERE id <= 10;
SELECT val FROM bp_rcrd_data WHERE id = 5;

-- Delete some rows
DELETE FROM bp_rcrd_data WHERE id > 90;
SELECT count(*) FROM bp_rcrd_data;

-- Create an index on the RCRD-pooled table
CREATE INDEX bp_rcrd_data_idx ON bp_rcrd_data (id);

-- Use the index
SELECT val FROM bp_rcrd_data WHERE id = 42;

-- VACUUM the RCRD-pooled table
VACUUM bp_rcrd_data;

-- Verify pool stats show activity
SELECT hits > 0 AS has_hits
FROM pg_stat_bufferpool WHERE name = 'rcrd_test_pool';

-- Verify RCRD stats show activity
SELECT lookups > 0 AS has_lookups
FROM pg_stat_rcrd WHERE name = 'rcrd_test_pool';

-- Checkpoint should preserve data
CHECKPOINT;
SELECT count(*) FROM bp_rcrd_data;
SELECT val FROM bp_rcrd_data WHERE id = 42;

-- ===================================================
-- RCRD stress tests: bulk INSERT, DELETE, multi-VACUUM
-- ===================================================

-- Bulk INSERT: 500 rows
INSERT INTO bp_rcrd_data SELECT g, 'rcrd-bulk-' || g FROM generate_series(101, 600) g;
SELECT count(*) FROM bp_rcrd_data;

-- DELETE many rows then VACUUM
DELETE FROM bp_rcrd_data WHERE id > 300;
SELECT count(*) FROM bp_rcrd_data;

VACUUM bp_rcrd_data;

-- Second round: re-insert and VACUUM (ghost hits expected)
INSERT INTO bp_rcrd_data SELECT g, 'rcrd-refill-' || g FROM generate_series(301, 500) g;
SELECT count(*) FROM bp_rcrd_data;

VACUUM bp_rcrd_data;

-- Third VACUUM cycle
VACUUM bp_rcrd_data;

SELECT count(*) FROM bp_rcrd_data;

-- ===================================================
-- RCRD algorithm behavior: recency list and Q dynamics
-- ===================================================

-- Use a small pool to force evictions and ghost tracking
CREATE BUFFER POOL rcrd_adapt_pool HANDLER rcrd_pool_handler SIZE '1048576';
CREATE TABLE bp_rcrd_adapt (id int, payload text) WITH (buffer_pool = 'rcrd_adapt_pool');

-- Phase 1: Sequential insert (pages enter as HOT during warm-up)
INSERT INTO bp_rcrd_adapt SELECT g, repeat('x', 100) FROM generate_series(1, 200) g;

-- Verify HOT pages exist
SELECT hot_size > 0 AS hot_has_pages
FROM pg_stat_rcrd WHERE name = 'rcrd_adapt_pool';

-- Phase 2: Repeated scans trigger recency list reordering
SELECT count(*) FROM bp_rcrd_adapt;
SELECT count(*) FROM bp_rcrd_adapt;
SELECT count(*) FROM bp_rcrd_adapt;

-- Verify recency list has entries
SELECT r_size > 0 AS r_has_entries
FROM pg_stat_rcrd WHERE name = 'rcrd_adapt_pool';

-- Phase 3: Insert enough new rows to force evictions from Q
INSERT INTO bp_rcrd_adapt SELECT g, repeat('y', 100) FROM generate_series(201, 500) g;

-- Verify eviction counters work
SELECT evictions > 0 AS has_evictions
FROM pg_stat_rcrd WHERE name = 'rcrd_adapt_pool';

-- Phase 4: Re-access evicted pages (ghost hits expected)
SELECT count(*) FROM bp_rcrd_adapt WHERE id <= 50;
SELECT count(*) FROM bp_rcrd_adapt WHERE id <= 50;

-- Ghost tracking should exist
SELECT ghost_size >= 0 AS ghost_tracking_exists
FROM pg_stat_rcrd WHERE name = 'rcrd_adapt_pool';

-- Verify hit counters work
SELECT (hot_hits + cold_hits) > 0 AS has_cache_hits
FROM pg_stat_rcrd WHERE name = 'rcrd_adapt_pool';

-- Verify HOT capacity is set
SELECT hot_capacity > 0 AS has_hot_capacity
FROM pg_stat_rcrd WHERE name = 'rcrd_adapt_pool';

-- Phase 5: VACUUM with fast-eviction placement
VACUUM bp_rcrd_adapt;
SELECT count(*) FROM bp_rcrd_adapt;

-- Verify size advisory function works
SELECT current_size > 0 AS has_current_size,
       hit_ratio >= 0 AS has_hit_ratio
FROM pg_bp_rcrd_size_recommendation('rcrd_adapt_pool')
    AS (current_size int, recommended_size int,
        ghost_pressure float8, hit_ratio float8);

-- Clean up adaptation test
DROP TABLE bp_rcrd_adapt;
DROP BUFFER POOL rcrd_adapt_pool;

-- Clean up
DROP TABLE bp_rcrd_data;
DROP BUFFER POOL rcrd_test_pool;

DROP EXTENSION pg_bp_rcrd;
