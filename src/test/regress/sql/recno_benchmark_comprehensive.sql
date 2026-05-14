--
-- RECNO Comprehensive Performance Benchmark
--
-- This test exercises the major RECNO operations across
-- bulk inserts, sequential scans, index scans, updates,
-- and deletes to verify correct behavior under load.
--

-- ================================================================
-- Setup
-- ================================================================
CREATE TABLE recno_bench (
    id serial,
    data text,
    value int,
    ts timestamp default now()
) USING recno;

CREATE INDEX recno_bench_id_idx ON recno_bench (id);
CREATE INDEX recno_bench_value_idx ON recno_bench (value);

-- ================================================================
-- Bulk Insert Benchmark
-- ================================================================
INSERT INTO recno_bench (data, value)
SELECT 'row_' || g, g % 100
FROM generate_series(1, 1000) g;

SELECT count(*) AS bulk_insert_count FROM recno_bench;

-- ================================================================
-- Sequential Scan Benchmark
-- ================================================================
SELECT count(*) AS seqscan_count FROM recno_bench WHERE value < 50;

-- ================================================================
-- Index Scan Benchmark
-- ================================================================
SET enable_seqscan = off;
SELECT count(*) AS idxscan_count FROM recno_bench WHERE id BETWEEN 100 AND 200;
RESET enable_seqscan;

-- ================================================================
-- Update Benchmark (in-place)
-- ================================================================
UPDATE recno_bench SET value = value + 1 WHERE id <= 100;
SELECT count(*) AS updated_rows FROM recno_bench WHERE id <= 100;

-- ================================================================
-- Mixed Workload
-- ================================================================
-- Concurrent-style: insert + update + delete in a single transaction
BEGIN;
INSERT INTO recno_bench (data, value) VALUES ('txn_insert', 999);
UPDATE recno_bench SET data = 'txn_updated' WHERE id = 500;
DELETE FROM recno_bench WHERE id = 1;
COMMIT;

SELECT count(*) AS after_mixed FROM recno_bench;

-- ================================================================
-- Rollback Verification
-- ================================================================
BEGIN;
DELETE FROM recno_bench WHERE id <= 50;
SELECT count(*) AS during_delete FROM recno_bench;
ROLLBACK;

SELECT count(*) AS after_rollback FROM recno_bench;

-- ================================================================
-- Cleanup
-- ================================================================
DROP TABLE recno_bench;
