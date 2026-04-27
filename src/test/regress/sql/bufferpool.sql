--
-- BUFFERPOOL
-- Tests for dynamic buffer pool infrastructure: CREATE, ALTER, DROP
-- BUFFER POOL, pg_bufferpool catalog, and pg_stat_bufferpool view.
--

-- View the default pool in the stats view
SELECT name FROM pg_stat_bufferpool;

-- Verify the default pool exists in the catalog
SELECT bpname, bpsize FROM pg_bufferpool WHERE bpname = 'default';

-- Create a clock-sweep buffer pool (256KB = 32 pages)
CREATE BUFFER POOL test_pool HANDLER clock_pool_handler SIZE '262144';

-- Verify catalog entry
SELECT bpname, bpsize FROM pg_bufferpool WHERE bpname = 'test_pool';

-- Verify it shows up in the stats view
SELECT name, nbuffers FROM pg_stat_bufferpool WHERE name = 'test_pool';

-- Rename the pool
ALTER BUFFER POOL test_pool RENAME TO test_pool_renamed;

-- Verify the rename in the catalog
SELECT bpname FROM pg_bufferpool WHERE bpname = 'test_pool_renamed';

-- Old name should be gone
SELECT count(*) FROM pg_bufferpool WHERE bpname = 'test_pool';

-- Verify the renamed pool is in the stats view
SELECT name FROM pg_stat_bufferpool WHERE name = 'test_pool_renamed';

-- Create a second pool to verify multiple pools work
CREATE BUFFER POOL test_pool2 HANDLER clock_pool_handler SIZE '262144';

-- Should have the default plus two custom pools
SELECT count(*) FROM pg_stat_bufferpool;

-- ===================================================
-- Data flow: actual reads/writes through dynamic pool
-- ===================================================

-- Create a data pool (1MB = 128 pages) with clock-sweep
CREATE BUFFER POOL data_pool HANDLER clock_pool_handler SIZE '1048576';

-- Create a table assigned to the pool
CREATE TABLE bp_data (id int, val text) WITH (buffer_pool = 'data_pool');

-- Insert 1000 rows
INSERT INTO bp_data SELECT g, 'row-' || g FROM generate_series(1, 1000) g;

-- Verify data reads back correctly
SELECT count(*) FROM bp_data;
SELECT min(id), max(id) FROM bp_data;
SELECT val FROM bp_data WHERE id = 500;

-- Update some rows
UPDATE bp_data SET val = 'updated-' || id WHERE id <= 10;
SELECT val FROM bp_data WHERE id = 5;

-- Delete some rows
DELETE FROM bp_data WHERE id > 990;
SELECT count(*) FROM bp_data;

-- Create an index on the pooled table
CREATE INDEX bp_data_idx ON bp_data (id);

-- Use the index
SELECT val FROM bp_data WHERE id = 42;

-- VACUUM the pooled table
VACUUM bp_data;

-- Check pool stats (hits expected from repeated access; reads may or may not
-- be positive depending on OS page cache behavior)
SELECT hits > 0 AS has_hits
FROM pg_stat_bufferpool WHERE name = 'data_pool';

-- Clean up
DROP TABLE bp_data;
DROP BUFFER POOL data_pool;

-- ===================================================
-- ALTER BUFFER POOL SET SIZE (online resize)
-- ===================================================

-- Create a pool we can resize (256KB = 32 pages)
CREATE BUFFER POOL resize_pool HANDLER clock_pool_handler SIZE '262144';

-- Verify initial size
SELECT name, nbuffers FROM pg_stat_bufferpool WHERE name = 'resize_pool';

-- Resize to 512KB = 64 pages (destroy-and-recreate)
ALTER BUFFER POOL resize_pool SET SIZE '524288';

-- Verify new size
SELECT name, nbuffers FROM pg_stat_bufferpool WHERE name = 'resize_pool';
SELECT bpsize FROM pg_bufferpool WHERE bpname = 'resize_pool';

-- No-op resize (same size)
ALTER BUFFER POOL resize_pool SET SIZE '524288';

-- Cannot resize the default pool
ALTER BUFFER POOL "default" SET SIZE '1048576';

-- Cannot resize with too-small size
ALTER BUFFER POOL resize_pool SET SIZE '8192';

-- Clean up
DROP BUFFER POOL resize_pool;

-- ===================================================
-- ALTER BUFFER POOL SET (options)
-- ===================================================

-- Create a pool to test options
CREATE BUFFER POOL option_pool HANDLER clock_pool_handler SIZE '262144';

-- Set direct_io option
ALTER BUFFER POOL option_pool SET (direct_io 'true');

-- Set direct_io back to false
ALTER BUFFER POOL option_pool SET (direct_io 'false');

-- Cannot alter the default pool
ALTER BUFFER POOL "default" SET (direct_io 'true');

-- Unrecognized option
ALTER BUFFER POOL option_pool SET (bogus_option 'true');

-- Clean up
DROP BUFFER POOL option_pool;

-- ===================================================
-- Multi-pool: two pools with tables, cross-pool JOIN
-- ===================================================

CREATE BUFFER POOL pool_a HANDLER clock_pool_handler SIZE '524288';
CREATE BUFFER POOL pool_b HANDLER clock_pool_handler SIZE '524288';

CREATE TABLE bp_tbl_a (id int, label text) WITH (buffer_pool = 'pool_a');
CREATE TABLE bp_tbl_b (id int, score int) WITH (buffer_pool = 'pool_b');

INSERT INTO bp_tbl_a SELECT g, 'item-' || g FROM generate_series(1, 100) g;
INSERT INTO bp_tbl_b SELECT g, g * 10 FROM generate_series(1, 100) g;

-- Cross-pool join
SELECT a.label, b.score FROM bp_tbl_a a JOIN bp_tbl_b b ON a.id = b.id
WHERE a.id = 50;

-- Both pools should be visible in stats
SELECT name FROM pg_stat_bufferpool
WHERE name IN ('pool_a', 'pool_b') ORDER BY name;

-- Clean up
DROP TABLE bp_tbl_a;
DROP TABLE bp_tbl_b;
DROP BUFFER POOL pool_a;
DROP BUFFER POOL pool_b;

-- ===================================================
-- Checkpoint persistence through a dynamic pool
-- ===================================================

CREATE BUFFER POOL ckpt_pool HANDLER clock_pool_handler SIZE '524288';
CREATE TABLE bp_ckpt_tbl (id int, data text) WITH (buffer_pool = 'ckpt_pool');

INSERT INTO bp_ckpt_tbl SELECT g, 'persist-' || g FROM generate_series(1, 100) g;

-- Force a checkpoint
CHECKPOINT;

-- Verify data survived the checkpoint
SELECT count(*) FROM bp_ckpt_tbl;
SELECT data FROM bp_ckpt_tbl WHERE id = 42;

-- Clean up
DROP TABLE bp_ckpt_tbl;
DROP BUFFER POOL ckpt_pool;


-- ===================================================
-- REMAINDER pool: auto-sized to unclaimed buffer space
-- ===================================================

-- Create a REMAINDER pool (size computed automatically)
-- Suppress NOTICE about exact size since it depends on shared_buffers
SET client_min_messages = 'warning';
CREATE BUFFER POOL remainder_test REMAINDER HANDLER clock_pool_handler;
SET client_min_messages = 'notice';

-- Verify it exists in the catalog (bpsize=0 marks it as REMAINDER)
SELECT bpname, bpsize FROM pg_bufferpool WHERE bpname = 'remainder_test';

-- Verify it shows up in the stats view with buffers > 0
SELECT name, nbuffers > 0 AS has_buffers FROM pg_stat_bufferpool WHERE name = 'remainder_test';

-- Cannot create a second REMAINDER pool
CREATE BUFFER POOL remainder_test2 REMAINDER HANDLER clock_pool_handler;

-- Drop the REMAINDER pool
DROP BUFFER POOL remainder_test;

-- Verify it is gone
SELECT count(*) FROM pg_bufferpool WHERE bpname = 'remainder_test';
--
-- Error cases
--

-- Cannot create a pool named "default" (already exists)
CREATE BUFFER POOL "default" HANDLER clock_pool_handler SIZE '262144';

-- Duplicate pool name
CREATE BUFFER POOL dup_test HANDLER clock_pool_handler SIZE '262144';
CREATE BUFFER POOL dup_test HANDLER clock_pool_handler SIZE '262144';

-- DROP IF EXISTS on a nonexistent pool should succeed with NOTICE
DROP BUFFER POOL IF EXISTS no_such_pool;

-- Creating a table with a nonexistent buffer_pool reloption just stores the string
CREATE TABLE bp_nopool (id int) WITH (buffer_pool = 'nonexistent');
DROP TABLE bp_nopool;

-- Cannot drop the default pool
DROP BUFFER POOL "default";

-- Cannot drop a pool with dependent relations
CREATE BUFFER POOL dep_test HANDLER clock_pool_handler SIZE '262144';
CREATE TABLE bp_dep_tbl (id int) WITH (buffer_pool = 'dep_test');
DROP BUFFER POOL dep_test;
DROP TABLE bp_dep_tbl;

-- Pool with SIZE too small (< 16 * 8192 = 131072)
CREATE BUFFER POOL tiny_pool HANDLER clock_pool_handler SIZE '8192';

-- Invalid handler function name
CREATE BUFFER POOL bad_handler HANDLER nonexistent_handler SIZE '262144';

-- DROP without IF EXISTS on nonexistent pool
DROP BUFFER POOL no_such_pool;

--
-- DROP BUFFER POOL (successful cases)
--

-- Drop the pools created above (also terminates their trickle writers)
DROP BUFFER POOL test_pool_renamed;
DROP BUFFER POOL test_pool2;
DROP BUFFER POOL dup_test;
DROP BUFFER POOL dep_test;

-- Verify they are gone from the catalog
SELECT bpname FROM pg_bufferpool WHERE bpname IN ('test_pool_renamed', 'test_pool2', 'dup_test', 'dep_test');

-- Only the default pool should remain in the stats view
SELECT count(*) FROM pg_stat_bufferpool;
