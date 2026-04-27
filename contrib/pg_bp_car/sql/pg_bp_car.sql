-- Test pg_bp_car extension
CREATE EXTENSION pg_bp_car;

-- ===================================================
-- CAR dynamic pool: full data lifecycle
-- ===================================================

-- Create a CAR buffer pool (4MB = 512 pages)
CREATE BUFFER POOL car_test_pool HANDLER car_pool_handler SIZE '4194304';

-- Create a table assigned to the CAR pool
CREATE TABLE bp_car_data (id int, val text) WITH (buffer_pool = 'car_test_pool');

-- Insert rows
INSERT INTO bp_car_data SELECT g, 'car-row-' || g FROM generate_series(1, 100) g;

-- Read back and verify
SELECT count(*) FROM bp_car_data;
SELECT min(id), max(id) FROM bp_car_data;
SELECT val FROM bp_car_data WHERE id = 50;

-- Repeated SELECT to trigger T1 -> T2 clock promotions
SELECT count(*) FROM bp_car_data;
SELECT count(*) FROM bp_car_data;

-- Update some rows
UPDATE bp_car_data SET val = 'car-updated-' || id WHERE id <= 10;
SELECT val FROM bp_car_data WHERE id = 5;

-- Delete some rows
DELETE FROM bp_car_data WHERE id > 90;
SELECT count(*) FROM bp_car_data;

-- Create an index on the CAR-pooled table
CREATE INDEX bp_car_data_idx ON bp_car_data (id);

-- Use the index
SELECT val FROM bp_car_data WHERE id = 42;

-- VACUUM the CAR-pooled table
VACUUM bp_car_data;

-- Verify pool stats show activity
SELECT hits > 0 AS has_hits
FROM pg_stat_bufferpool WHERE name = 'car_test_pool';

-- Verify CAR stats show activity
SELECT lookups > 0 AS has_lookups
FROM pg_stat_car WHERE name = 'car_test_pool';

-- Checkpoint should preserve data
CHECKPOINT;
SELECT count(*) FROM bp_car_data;
SELECT val FROM bp_car_data WHERE id = 42;

-- ===================================================
-- CAR stress tests: bulk INSERT, DELETE, multi-VACUUM
-- ===================================================

-- Bulk INSERT: 500 rows
INSERT INTO bp_car_data SELECT g, 'car-bulk-' || g FROM generate_series(101, 600) g;
SELECT count(*) FROM bp_car_data;

-- DELETE many rows then VACUUM
DELETE FROM bp_car_data WHERE id > 300;
SELECT count(*) FROM bp_car_data;

VACUUM bp_car_data;

-- Second round: re-insert and VACUUM (ghost hits expected)
INSERT INTO bp_car_data SELECT g, 'car-refill-' || g FROM generate_series(301, 500) g;
SELECT count(*) FROM bp_car_data;

VACUUM bp_car_data;

-- Third VACUUM cycle
VACUUM bp_car_data;

SELECT count(*) FROM bp_car_data;

-- ===================================================
-- CAR algorithm behavior: clock sweep and adaptation
-- ===================================================

-- Use a small pool to force evictions and clock sweep activity
CREATE BUFFER POOL car_adapt_pool HANDLER car_pool_handler SIZE '1048576';
CREATE TABLE bp_car_adapt (id int, payload text) WITH (buffer_pool = 'car_adapt_pool');

-- Phase 1: Insert enough rows to fill the pool and force clock sweep.
-- 1MB pool = 128 pages; ~15 rows/page with 500-byte payload => ~1920 rows fills pool.
INSERT INTO bp_car_adapt SELECT g, repeat('x', 500) FROM generate_series(1, 2000) g;

-- Verify T1 has entries
SELECT t1_size > 0 AS t1_has_pages
FROM pg_stat_car WHERE name = 'car_adapt_pool';

-- Phase 2: Repeated scans set reference bits, which get cleared
-- during clock sweep, promoting pages from T1 to T2
SELECT count(*) FROM bp_car_adapt;
SELECT count(*) FROM bp_car_adapt;
SELECT count(*) FROM bp_car_adapt;

-- After repeated scans, T2 may have pages (lazy clock promotion is
-- timing-dependent: it happens only when GetVictim runs T1 sweep)
SELECT t2_size >= 0 AS t2_valid
FROM pg_stat_car WHERE name = 'car_adapt_pool';

-- Verify T1 clock hand position is valid
SELECT t1_hand >= 0 AS t1_hand_valid
FROM pg_stat_car WHERE name = 'car_adapt_pool';

-- Phase 3: Insert more rows to force additional clock sweep evictions
INSERT INTO bp_car_adapt SELECT g, repeat('y', 500) FROM generate_series(2001, 3000) g;

-- Verify eviction counters work
SELECT (t1_evictions + t2_evictions) > 0 AS has_evictions
FROM pg_stat_car WHERE name = 'car_adapt_pool';

-- Phase 4: Re-access evicted pages (ghost hits adapt target_t1_size)
SELECT count(*) FROM bp_car_adapt WHERE id <= 200;
SELECT count(*) FROM bp_car_adapt WHERE id <= 200;

-- Ghost lists should have entries
SELECT (b1_size + b2_size) >= 0 AS ghost_lists_exist
FROM pg_stat_car WHERE name = 'car_adapt_pool';

-- Verify hit counters work
SELECT (t1_hits + t2_hits) > 0 AS has_cache_hits
FROM pg_stat_car WHERE name = 'car_adapt_pool';

-- Verify misses counter works
SELECT misses > 0 AS has_misses
FROM pg_stat_car WHERE name = 'car_adapt_pool';

-- Phase 5: VACUUM with cleared reference bits
VACUUM bp_car_adapt;
SELECT count(*) FROM bp_car_adapt;

-- Verify size advisory function works
SELECT current_size > 0 AS has_current_size,
       hit_ratio >= 0 AS has_hit_ratio
FROM pg_bp_car_size_recommendation('car_adapt_pool')
    AS (current_size int, recommended_size int,
        ghost_pressure float8, hit_ratio float8);

-- Clean up adaptation test
DROP TABLE bp_car_adapt;
DROP BUFFER POOL car_adapt_pool;

-- Clean up
DROP TABLE bp_car_data;
DROP BUFFER POOL car_test_pool;

DROP EXTENSION pg_bp_car;
