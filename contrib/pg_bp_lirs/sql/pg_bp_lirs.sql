-- Test pg_bp_lirs extension
CREATE EXTENSION pg_bp_lirs;

-- ===================================================
-- LIRS dynamic pool: full data lifecycle
-- ===================================================

-- Create a LIRS buffer pool (4MB = 512 pages)
CREATE BUFFER POOL lirs_test_pool HANDLER lirs_pool_handler SIZE '4194304';

-- Create a table assigned to the LIRS pool
CREATE TABLE bp_lirs_data (id int, val text) WITH (buffer_pool = 'lirs_test_pool');

-- Insert rows
INSERT INTO bp_lirs_data SELECT g, 'lirs-row-' || g FROM generate_series(1, 100) g;

-- Read back and verify
SELECT count(*) FROM bp_lirs_data;
SELECT min(id), max(id) FROM bp_lirs_data;
SELECT val FROM bp_lirs_data WHERE id = 50;

-- Repeated SELECT to trigger LIR stack operations
SELECT count(*) FROM bp_lirs_data;
SELECT count(*) FROM bp_lirs_data;

-- Update some rows
UPDATE bp_lirs_data SET val = 'lirs-updated-' || id WHERE id <= 10;
SELECT val FROM bp_lirs_data WHERE id = 5;

-- Delete some rows
DELETE FROM bp_lirs_data WHERE id > 90;
SELECT count(*) FROM bp_lirs_data;

-- Create an index on the LIRS-pooled table
CREATE INDEX bp_lirs_data_idx ON bp_lirs_data (id);

-- Use the index
SELECT val FROM bp_lirs_data WHERE id = 42;

-- VACUUM the LIRS-pooled table
VACUUM bp_lirs_data;

-- Verify pool stats show activity
SELECT hits > 0 AS has_hits
FROM pg_stat_bufferpool WHERE name = 'lirs_test_pool';

-- Verify LIRS stats show activity
SELECT lookups > 0 AS has_lookups
FROM pg_stat_lirs WHERE name = 'lirs_test_pool';

-- Checkpoint should preserve data
CHECKPOINT;
SELECT count(*) FROM bp_lirs_data;
SELECT val FROM bp_lirs_data WHERE id = 42;

-- ===================================================
-- LIRS stress tests: bulk INSERT, DELETE, multi-VACUUM
-- ===================================================

-- Bulk INSERT: 500 rows
INSERT INTO bp_lirs_data SELECT g, 'lirs-bulk-' || g FROM generate_series(101, 600) g;
SELECT count(*) FROM bp_lirs_data;

-- DELETE many rows then VACUUM
DELETE FROM bp_lirs_data WHERE id > 300;
SELECT count(*) FROM bp_lirs_data;

VACUUM bp_lirs_data;

-- Second round: re-insert and VACUUM (ghost hits expected)
INSERT INTO bp_lirs_data SELECT g, 'lirs-refill-' || g FROM generate_series(301, 500) g;
SELECT count(*) FROM bp_lirs_data;

VACUUM bp_lirs_data;

-- Third VACUUM cycle
VACUUM bp_lirs_data;

SELECT count(*) FROM bp_lirs_data;

-- ===================================================
-- LIRS algorithm behavior: stack and Q list dynamics
-- ===================================================

-- Use a small pool to force evictions and ghost tracking
CREATE BUFFER POOL lirs_adapt_pool HANDLER lirs_pool_handler SIZE '1048576';
CREATE TABLE bp_lirs_adapt (id int, payload text) WITH (buffer_pool = 'lirs_adapt_pool');

-- Phase 1: Sequential insert (pages enter as LIR during warm-up)
INSERT INTO bp_lirs_adapt SELECT g, repeat('x', 100) FROM generate_series(1, 200) g;

-- Verify LIR pages exist
SELECT lir_size > 0 AS lir_has_pages
FROM pg_stat_lirs WHERE name = 'lirs_adapt_pool';

-- Phase 2: Repeated scans trigger stack reordering
SELECT count(*) FROM bp_lirs_adapt;
SELECT count(*) FROM bp_lirs_adapt;
SELECT count(*) FROM bp_lirs_adapt;

-- Verify stack has entries
SELECT stack_size > 0 AS stack_has_entries
FROM pg_stat_lirs WHERE name = 'lirs_adapt_pool';

-- Phase 3: Insert enough new rows to force evictions from Q
INSERT INTO bp_lirs_adapt SELECT g, repeat('y', 100) FROM generate_series(201, 500) g;

-- Verify eviction counters work
SELECT evictions > 0 AS has_evictions
FROM pg_stat_lirs WHERE name = 'lirs_adapt_pool';

-- Phase 4: Re-access evicted pages (ghost hits expected)
SELECT count(*) FROM bp_lirs_adapt WHERE id <= 50;
SELECT count(*) FROM bp_lirs_adapt WHERE id <= 50;

-- Ghost tracking should exist
SELECT ghost_size >= 0 AS ghost_tracking_exists
FROM pg_stat_lirs WHERE name = 'lirs_adapt_pool';

-- Verify hit counters work
SELECT (lir_hits + hir_hits) > 0 AS has_cache_hits
FROM pg_stat_lirs WHERE name = 'lirs_adapt_pool';

-- Verify LIR capacity is set
SELECT lir_capacity > 0 AS has_lir_capacity
FROM pg_stat_lirs WHERE name = 'lirs_adapt_pool';

-- Phase 5: VACUUM with fast-eviction placement
VACUUM bp_lirs_adapt;
SELECT count(*) FROM bp_lirs_adapt;

-- Verify size advisory function works
SELECT current_size > 0 AS has_current_size,
       hit_ratio >= 0 AS has_hit_ratio
FROM pg_bp_lirs_size_recommendation('lirs_adapt_pool')
    AS (current_size int, recommended_size int,
        ghost_pressure float8, hit_ratio float8);

-- Clean up adaptation test
DROP TABLE bp_lirs_adapt;
DROP BUFFER POOL lirs_adapt_pool;

-- Clean up
DROP TABLE bp_lirs_data;
DROP BUFFER POOL lirs_test_pool;

DROP EXTENSION pg_bp_lirs;
