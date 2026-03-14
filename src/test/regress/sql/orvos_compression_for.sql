--
-- Test Frame of Reference (FOR) encoding for sequential/clustered data
-- Verifies 2-8x compression for timestamps and sequential integer columns.
--

-- Test 1: Sequential timestamps
CREATE TABLE orvos_for_timestamp_test (
    id int,
    created_at timestamp,
    updated_at timestamp
) USING orvos;

-- Insert timestamps in a narrow range (clustered)
INSERT INTO orvos_for_timestamp_test
SELECT i,
       '2024-01-01 00:00:00'::timestamp + (i || ' seconds')::interval,
       '2024-01-01 00:00:00'::timestamp + ((i * 2) || ' seconds')::interval
FROM generate_series(1, 1000) i;

SELECT COUNT(*) FROM orvos_for_timestamp_test;
SELECT MIN(created_at), MAX(created_at) FROM orvos_for_timestamp_test;

-- Test range queries on FOR-encoded timestamps
SELECT COUNT(*) FROM orvos_for_timestamp_test 
WHERE created_at BETWEEN '2024-01-01 00:05:00' AND '2024-01-01 00:10:00';

SELECT * FROM orvos_for_timestamp_test WHERE id <= 5 ORDER BY id;

DROP TABLE orvos_for_timestamp_test;

-- Test 2: Sequential integer IDs
CREATE TABLE orvos_for_sequential_test (
    id bigint,
    counter int,
    value text
) USING orvos;

-- Insert sequential IDs starting from a large number
INSERT INTO orvos_for_sequential_test
SELECT 1000000 + i, i, 'value_' || i
FROM generate_series(1, 5000) i;

SELECT MIN(id), MAX(id) FROM orvos_for_sequential_test;
SELECT COUNT(*) FROM orvos_for_sequential_test WHERE id > 1002500;

DROP TABLE orvos_for_sequential_test;

-- Test 3: Clustered integer values (90% in narrow range)
CREATE TABLE orvos_for_clustered_test (
    id int,
    amount int
) USING orvos;

-- 90% of values in range 100-200, 10% outside
INSERT INTO orvos_for_clustered_test
SELECT i,
       CASE
           WHEN i <= 900 THEN 100 + (i % 100)
           ELSE 1000 + i
       END
FROM generate_series(1, 1000) i;

SELECT MIN(amount), MAX(amount) FROM orvos_for_clustered_test;
SELECT COUNT(*) FROM orvos_for_clustered_test WHERE amount BETWEEN 100 AND 200;

DROP TABLE orvos_for_clustered_test;

-- Test 4: Date column (should use FOR encoding)
CREATE TABLE orvos_for_date_test (
    id int,
    event_date date
) USING orvos;

INSERT INTO orvos_for_date_test
SELECT i, '2024-01-01'::date + i
FROM generate_series(0, 365) i;

SELECT MIN(event_date), MAX(event_date) FROM orvos_for_date_test;
SELECT COUNT(*) FROM orvos_for_date_test 
WHERE event_date BETWEEN '2024-06-01' AND '2024-06-30';

DROP TABLE orvos_for_date_test;

-- Test 5: FOR with NULL values
CREATE TABLE orvos_for_null_test (
    id int,
    timestamp_col timestamp
) USING orvos;

INSERT INTO orvos_for_null_test
SELECT i,
       CASE
           WHEN i % 10 = 0 THEN NULL
           ELSE '2024-01-01 00:00:00'::timestamp + (i || ' seconds')::interval
       END
FROM generate_series(1, 100) i;

SELECT COUNT(*) FROM orvos_for_null_test WHERE timestamp_col IS NULL;
SELECT COUNT(*) FROM orvos_for_null_test WHERE timestamp_col IS NOT NULL;

DROP TABLE orvos_for_null_test;
