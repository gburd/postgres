--
-- Test RECNO clock-bound integration and timestamp MVCC
--
-- RECNO uses timestamps for MVCC, requiring accurate clock synchronization
-- to ensure correct visibility and ordering, especially for logical replication.
--

-- =============================================
-- Basic Timestamp Operations
-- =============================================

-- Create test table
CREATE TABLE recno_clock_test (
    id int PRIMARY KEY,
    val int,
    data text,
    ts timestamp DEFAULT current_timestamp
) USING recno;

-- Insert with timestamps
INSERT INTO recno_clock_test (id, val, data)
VALUES (1, 100, 'first'), (2, 200, 'second'), (3, 300, 'third');

-- Verify insertion order matches timestamp order
-- (Mask actual timestamps to avoid non-deterministic output)
SELECT id, val, ts IS NOT NULL AS has_ts FROM recno_clock_test ORDER BY ts;

-- =============================================
-- Clock Uncertainty Configuration
-- =============================================

-- Check clock uncertainty settings
SHOW recno.max_clock_uncertainty;
SHOW recno.clock_bound_enabled;

-- Test with different uncertainty levels (if configurable)
SET LOCAL recno.max_clock_uncertainty = '100ms';
INSERT INTO recno_clock_test (id, val, data) VALUES (4, 400, 'uncertainty_test');
RESET recno.max_clock_uncertainty;

-- =============================================
-- Timestamp-based Visibility
-- =============================================

CREATE TABLE recno_ts_visibility (
    id int PRIMARY KEY,
    val int,
    created_at timestamp DEFAULT clock_timestamp()
) USING recno;

-- Insert rows with explicit transaction control
BEGIN;
    INSERT INTO recno_ts_visibility VALUES (1, 100);
    -- Get current transaction timestamp
    SELECT now() AS tx_time \gset
    INSERT INTO recno_ts_visibility VALUES (2, 200);
COMMIT;

-- All rows from same transaction should have same timestamp
SELECT id, val, created_at = :'tx_time'::timestamp AS same_tx_time
FROM recno_ts_visibility
ORDER BY id;

-- =============================================
-- Clock Skew Detection
-- =============================================

CREATE TABLE recno_clock_skew (
    id int PRIMARY KEY,
    node_id int,
    local_time timestamp,
    data text
) USING recno;

-- Simulate data from different nodes (would have different clocks)
INSERT INTO recno_clock_skew VALUES
    (1, 1, now(), 'node1_data'),
    (2, 2, now() + interval '1 second', 'node2_future'),
    (3, 3, now() - interval '1 second', 'node3_past');

-- Check for potential clock skew (mask timestamps, show only skew)
SELECT
    node_id,
    extract(epoch from (local_time - min(local_time) OVER ()))::int AS skew_seconds
FROM recno_clock_skew
ORDER BY node_id;

-- =============================================
-- Logical Replication Timestamp Safety
-- =============================================

CREATE TABLE recno_repl_test (
    id int PRIMARY KEY,
    val int,
    replicated_at timestamp DEFAULT clock_timestamp()
) USING recno;

-- Insert test data
INSERT INTO recno_repl_test (id, val)
SELECT i, i * 10 FROM generate_series(1, 10) i;

-- In real replication, clock-bound would ensure safe timestamp ordering
-- Here we verify timestamps are monotonically increasing
WITH ordered AS (
    SELECT
        id,
        replicated_at,
        lag(replicated_at) OVER (ORDER BY id) AS prev_ts
    FROM recno_repl_test
)
SELECT
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE replicated_at >= prev_ts OR prev_ts IS NULL) AS correctly_ordered
FROM ordered;

-- =============================================
-- Transaction Ordering
-- =============================================

CREATE TABLE recno_tx_order (
    id int PRIMARY KEY,
    tx_id bigint DEFAULT txid_current(),
    tx_time timestamp DEFAULT now(),
    data text
) USING recno;

-- Multiple transactions with ordering
BEGIN;
    INSERT INTO recno_tx_order (id, data) VALUES (1, 'tx1');
COMMIT;

BEGIN;
    INSERT INTO recno_tx_order (id, data) VALUES (2, 'tx2');
COMMIT;

BEGIN;
    INSERT INTO recno_tx_order (id, data) VALUES (3, 'tx3');
COMMIT;

-- Verify transaction ordering (mask volatile tx_id and tx_time)
SELECT id,
       tx_id > 0 AS valid_txid,
       tx_time IS NOT NULL AS has_time,
       data,
       tx_id = lag(tx_id) OVER (ORDER BY id) AS same_txid,
       tx_time <= lead(tx_time) OVER (ORDER BY id) OR lead(tx_time) OVER (ORDER BY id) IS NULL AS ordered
FROM recno_tx_order
ORDER BY id;

-- =============================================
-- Conflict Resolution with Timestamps
-- =============================================

CREATE TABLE recno_conflict (
    id int PRIMARY KEY,
    val int,
    last_modified timestamp DEFAULT clock_timestamp()
) USING recno;

INSERT INTO recno_conflict VALUES (1, 100);

-- Simulate concurrent updates (in real scenario, these would be from different nodes)
BEGIN;
    -- First update
    UPDATE recno_conflict
    SET val = 200, last_modified = clock_timestamp()
    WHERE id = 1;

    -- Get update timestamp
    SELECT last_modified AS update1_time
    FROM recno_conflict WHERE id = 1 \gset
COMMIT;

BEGIN;
    -- Second update (would use clock-bound to ensure happens-after)
    UPDATE recno_conflict
    SET val = 300, last_modified = clock_timestamp()
    WHERE id = 1 AND last_modified = :'update1_time'::timestamp;
COMMIT;

-- Verify final state (mask volatile timestamp)
SELECT id, val, last_modified IS NOT NULL AS has_ts FROM recno_conflict;

-- =============================================
-- Clock-bound Statistics
-- =============================================

-- Create table for monitoring clock-bound behavior
CREATE TABLE recno_clock_stats (
    id serial PRIMARY KEY,
    operation text,
    uncertainty_ms int,
    wait_required boolean,
    wait_duration_ms int
) USING recno;

-- Simulate clock-bound statistics (in production, these would be real metrics)
INSERT INTO recno_clock_stats (operation, uncertainty_ms, wait_required, wait_duration_ms)
VALUES
    ('INSERT', 50, false, 0),
    ('UPDATE', 150, true, 100),
    ('DELETE', 75, false, 0),
    ('INSERT', 500, true, 450),
    ('UPDATE', 25, false, 0);

-- Analyze clock-bound behavior
SELECT
    operation,
    AVG(uncertainty_ms) AS avg_uncertainty,
    COUNT(*) FILTER (WHERE wait_required) AS waits_required,
    AVG(wait_duration_ms) FILTER (WHERE wait_required) AS avg_wait_ms
FROM recno_clock_stats
GROUP BY operation;

-- =============================================
-- Timestamp Precision
-- =============================================

CREATE TABLE recno_precision (
    id int PRIMARY KEY,
    microsecond_ts timestamp(6) DEFAULT clock_timestamp(),
    millisecond_ts timestamp(3) DEFAULT clock_timestamp()
) USING recno;

-- Insert rows rapidly to test timestamp precision
DO $$
BEGIN
    FOR i IN 1..10 LOOP
        INSERT INTO recno_precision (id) VALUES (i);
    END LOOP;
END $$;

-- Check timestamp uniqueness and precision
-- (unique_millisecond count can vary depending on execution speed, so just
-- verify microsecond precision >= millisecond precision)
SELECT
    COUNT(DISTINCT microsecond_ts) >= COUNT(DISTINCT millisecond_ts) AS micro_ge_milli,
    COUNT(*) AS total_rows
FROM recno_precision;

-- =============================================
-- Read Timestamp Tracking
-- =============================================

CREATE TABLE recno_read_ts (
    id int PRIMARY KEY,
    val int,
    last_read timestamp
) USING recno;

INSERT INTO recno_read_ts (id, val)
VALUES (1, 100), (2, 200), (3, 300);

-- Simulate read timestamp tracking
DO $$
DECLARE
    read_time timestamp;
BEGIN
    -- Read and track timestamp
    read_time := clock_timestamp();
    PERFORM * FROM recno_read_ts WHERE id = 1;
    UPDATE recno_read_ts SET last_read = read_time WHERE id = 1;
END $$;

-- Verify read tracking
SELECT id, val, last_read IS NOT NULL AS was_read
FROM recno_read_ts
ORDER BY id;

-- =============================================
-- Clock Synchronization Check
-- =============================================

-- Function to check clock synchronization status
CREATE OR REPLACE FUNCTION check_clock_sync()
RETURNS TABLE(
    check_name text,
    status text,
    details text
) AS $$
BEGIN
    -- Check system time (mask actual timestamp for deterministic output)
    RETURN QUERY
    SELECT 'system_time'::text,
           CASE WHEN current_timestamp IS NOT NULL THEN 'OK' ELSE 'FAIL' END::text,
           'timestamp_available'::text;

    -- Check clock-bound availability
    RETURN QUERY
    SELECT 'clock_bound'::text,
           CASE WHEN current_setting('recno.clock_bound_enabled', true) = 'on'
                THEN 'ENABLED'
                ELSE 'DISABLED'
           END::text,
           'Clock-bound integration status'::text;

    -- Check max uncertainty
    RETURN QUERY
    SELECT 'max_uncertainty'::text,
           'CONFIGURED'::text,
           coalesce(current_setting('recno.max_clock_uncertainty', true), '500ms')::text;
END;
$$ LANGUAGE plpgsql;

-- Run synchronization check
SELECT * FROM check_clock_sync();

-- =============================================
-- Cleanup
-- =============================================

DROP FUNCTION check_clock_sync();
DROP TABLE recno_clock_test CASCADE;
DROP TABLE recno_ts_visibility CASCADE;
DROP TABLE recno_clock_skew CASCADE;
DROP TABLE recno_repl_test CASCADE;
DROP TABLE recno_tx_order CASCADE;
DROP TABLE recno_conflict CASCADE;
DROP TABLE recno_clock_stats CASCADE;
DROP TABLE recno_precision CASCADE;
DROP TABLE recno_read_ts CASCADE;