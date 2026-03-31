--
-- Comprehensive verification and benchmark suite for Noxu compression
--
-- Tests all compression algorithms for correctness (round-trip encoding)
-- and measures compression ratios against target thresholds.
--
-- Compression targets:
--   Arrays:     5-10x compression
--   Floats:     4-8x  compression (Chimp algorithm)
--   Timestamps: 4-6x  compression (delta-of-delta encoding)
--   UUIDs:      3-5x  compression (UUID v7 delta encoding)
--   Vectors:    5-8x  compression (scalar float16 quantization)
--

-- ============================================================
-- Helper: function to compute compression ratio from noxu vs heap
-- We compare pg_relation_size of a noxu table to the equivalent
-- raw data size computed from row count and column widths.
-- ============================================================

-- ============================================================
-- SECTION 1: Array Element-Level Compression
-- Target: 5-10x compression for int[], float[], bool[], timestamp[]
-- ============================================================

-- 1a: Integer arrays
CREATE TABLE noxu_verify_int_array (
    id int,
    values int[]
) USING noxu;

INSERT INTO noxu_verify_int_array
SELECT i, ARRAY(SELECT (i * 100 + j) FROM generate_series(1, 50) j)
FROM generate_series(1, 2000) i;

-- Round-trip correctness: verify all data survives encode/decode
SELECT COUNT(*) AS total_rows FROM noxu_verify_int_array;

-- Verify specific values survive compression round-trip
SELECT id, values[1], values[25], values[50]
FROM noxu_verify_int_array WHERE id IN (1, 1000, 2000) ORDER BY id;

-- Verify array length preserved
SELECT COUNT(*) AS correct_length_count
FROM noxu_verify_int_array
WHERE array_length(values, 1) = 50;

-- Verify sum of elements (strong correctness check)
SELECT id,
       (SELECT SUM(v) FROM unnest(values) v) AS elem_sum
FROM noxu_verify_int_array WHERE id <= 3 ORDER BY id;

-- Size measurement for compression ratio
SELECT pg_relation_size('noxu_verify_int_array') AS noxu_int_array_bytes;

-- Equivalent raw size: 2000 rows * (4 + 50*4) = 2000 * 204 = 408000 bytes
-- Expected compression: 5-10x => noxu size should be 40800 - 81600 bytes
SELECT CASE
    WHEN pg_relation_size('noxu_verify_int_array') > 0
    THEN round((2000.0 * 204) / pg_relation_size('noxu_verify_int_array'), 1)
    ELSE 0
END AS int_array_compression_ratio;

DROP TABLE noxu_verify_int_array;

-- 1b: Float arrays
CREATE TABLE noxu_verify_float_array (
    id int,
    measurements float8[]
) USING noxu;

-- Insert arrays of similar floats (common in time-series)
INSERT INTO noxu_verify_float_array
SELECT i,
       ARRAY(SELECT 100.0 + (j * 0.01) + (random() * 0.001)
             FROM generate_series(1, 50) j)
FROM generate_series(1, 2000) i;

SELECT COUNT(*) AS total_rows FROM noxu_verify_float_array;

-- Correctness: verify array elements are close to expected values
SELECT id, measurements[1], measurements[50]
FROM noxu_verify_float_array WHERE id IN (1, 1000, 2000) ORDER BY id;

SELECT COUNT(*) AS correct_length_count
FROM noxu_verify_float_array
WHERE array_length(measurements, 1) = 50;

SELECT pg_relation_size('noxu_verify_float_array') AS noxu_float_array_bytes;

SELECT CASE
    WHEN pg_relation_size('noxu_verify_float_array') > 0
    THEN round((2000.0 * (4 + 50 * 8)) / pg_relation_size('noxu_verify_float_array'), 1)
    ELSE 0
END AS float_array_compression_ratio;

DROP TABLE noxu_verify_float_array;

-- 1c: Boolean arrays
CREATE TABLE noxu_verify_bool_array (
    id int,
    flags boolean[]
) USING noxu;

INSERT INTO noxu_verify_bool_array
SELECT i, ARRAY(SELECT (j % 2 = 0) FROM generate_series(1, 64) j)
FROM generate_series(1, 2000) i;

SELECT COUNT(*) AS total_rows FROM noxu_verify_bool_array;

-- Correctness
SELECT id, flags[1], flags[2], flags[63], flags[64]
FROM noxu_verify_bool_array WHERE id IN (1, 1000, 2000) ORDER BY id;

SELECT COUNT(*) AS correct_length_count
FROM noxu_verify_bool_array
WHERE array_length(flags, 1) = 64;

-- Boolean arrays should compress extremely well (64 bools -> 8 bytes ideally)
SELECT pg_relation_size('noxu_verify_bool_array') AS noxu_bool_array_bytes;

SELECT CASE
    WHEN pg_relation_size('noxu_verify_bool_array') > 0
    THEN round((2000.0 * (4 + 64)) / pg_relation_size('noxu_verify_bool_array'), 1)
    ELSE 0
END AS bool_array_compression_ratio;

DROP TABLE noxu_verify_bool_array;

-- 1d: Timestamp arrays
CREATE TABLE noxu_verify_ts_array (
    id int,
    event_times timestamp[]
) USING noxu;

INSERT INTO noxu_verify_ts_array
SELECT i,
       ARRAY(SELECT '2024-01-01 00:00:00'::timestamp + ((i * 1000 + j) || ' seconds')::interval
             FROM generate_series(1, 30) j)
FROM generate_series(1, 2000) i;

SELECT COUNT(*) AS total_rows FROM noxu_verify_ts_array;

-- Correctness
SELECT id, event_times[1], event_times[30]
FROM noxu_verify_ts_array WHERE id IN (1, 1000, 2000) ORDER BY id;

SELECT COUNT(*) AS correct_length_count
FROM noxu_verify_ts_array
WHERE array_length(event_times, 1) = 30;

SELECT pg_relation_size('noxu_verify_ts_array') AS noxu_ts_array_bytes;

SELECT CASE
    WHEN pg_relation_size('noxu_verify_ts_array') > 0
    THEN round((2000.0 * (4 + 30 * 8)) / pg_relation_size('noxu_verify_ts_array'), 1)
    ELSE 0
END AS ts_array_compression_ratio;

DROP TABLE noxu_verify_ts_array;

-- 1e: Arrays with NULL elements (edge case)
CREATE TABLE noxu_verify_array_nulls (
    id int,
    values int[]
) USING noxu;

INSERT INTO noxu_verify_array_nulls
SELECT i,
       ARRAY(SELECT CASE WHEN j % 5 = 0 THEN NULL ELSE i * 10 + j END
             FROM generate_series(1, 20) j)
FROM generate_series(1, 500) i;

SELECT COUNT(*) AS total_rows FROM noxu_verify_array_nulls;

-- Verify NULLs survive round-trip
SELECT id, values[5], values[10], values[15], values[20]
FROM noxu_verify_array_nulls WHERE id IN (1, 250, 500) ORDER BY id;

DROP TABLE noxu_verify_array_nulls;


-- ============================================================
-- SECTION 2: Float Chimp Compression
-- Target: 4-8x compression for time-series float data
-- ============================================================

-- 2a: Slowly changing floats (typical sensor data)
CREATE TABLE noxu_verify_chimp_sensor (
    id int,
    temperature float8,
    humidity float8,
    pressure float8
) USING noxu;

-- Sensor-like data: slow variation around a mean
INSERT INTO noxu_verify_chimp_sensor
SELECT i,
       20.0 + sin(i::float8 / 100) * 5 + random() * 0.1,
       50.0 + cos(i::float8 / 200) * 10 + random() * 0.2,
       1013.25 + sin(i::float8 / 500) * 2 + random() * 0.05
FROM generate_series(1, 10000) i;

SELECT COUNT(*) AS total_rows FROM noxu_verify_chimp_sensor;

-- Correctness: verify values in expected ranges
SELECT COUNT(*) AS temp_in_range
FROM noxu_verify_chimp_sensor
WHERE temperature BETWEEN 14.0 AND 26.0;

SELECT COUNT(*) AS humidity_in_range
FROM noxu_verify_chimp_sensor
WHERE humidity BETWEEN 39.0 AND 61.0;

SELECT COUNT(*) AS pressure_in_range
FROM noxu_verify_chimp_sensor
WHERE pressure BETWEEN 1010.0 AND 1016.0;

-- Verify statistical properties survive compression
SELECT round(avg(temperature)::numeric, 1) AS avg_temp,
       round(stddev(temperature)::numeric, 1) AS stddev_temp,
       round(min(temperature)::numeric, 1) AS min_temp,
       round(max(temperature)::numeric, 1) AS max_temp
FROM noxu_verify_chimp_sensor;

SELECT pg_relation_size('noxu_verify_chimp_sensor') AS noxu_chimp_sensor_bytes;

-- Raw size: 10000 * (4 + 8 + 8 + 8) = 280000
SELECT CASE
    WHEN pg_relation_size('noxu_verify_chimp_sensor') > 0
    THEN round(280000.0 / pg_relation_size('noxu_verify_chimp_sensor'), 1)
    ELSE 0
END AS chimp_sensor_compression_ratio;

DROP TABLE noxu_verify_chimp_sensor;

-- 2b: Nearly constant floats (best case for Chimp)
CREATE TABLE noxu_verify_chimp_constant (
    id int,
    value float8
) USING noxu;

INSERT INTO noxu_verify_chimp_constant
SELECT i, 42.0 + random() * 0.001
FROM generate_series(1, 10000) i;

SELECT COUNT(*) AS total_rows FROM noxu_verify_chimp_constant;

SELECT round(avg(value)::numeric, 3) AS avg_val,
       round(stddev(value)::numeric, 6) AS stddev_val
FROM noxu_verify_chimp_constant;

SELECT pg_relation_size('noxu_verify_chimp_constant') AS noxu_chimp_const_bytes;

-- Raw size: 10000 * (4 + 8) = 120000
SELECT CASE
    WHEN pg_relation_size('noxu_verify_chimp_constant') > 0
    THEN round(120000.0 / pg_relation_size('noxu_verify_chimp_constant'), 1)
    ELSE 0
END AS chimp_constant_compression_ratio;

DROP TABLE noxu_verify_chimp_constant;

-- 2c: Floats with NULLs
CREATE TABLE noxu_verify_chimp_nulls (
    id int,
    value float8
) USING noxu;

INSERT INTO noxu_verify_chimp_nulls
SELECT i,
       CASE WHEN i % 7 = 0 THEN NULL
            ELSE 100.0 + sin(i::float8 / 50) * 10
       END
FROM generate_series(1, 5000) i;

SELECT COUNT(*) AS total_rows FROM noxu_verify_chimp_nulls;
SELECT COUNT(*) AS null_count FROM noxu_verify_chimp_nulls WHERE value IS NULL;
SELECT COUNT(*) AS nonnull_count FROM noxu_verify_chimp_nulls WHERE value IS NOT NULL;

-- Verify non-NULL values are in expected range
SELECT COUNT(*) AS values_in_range
FROM noxu_verify_chimp_nulls
WHERE value IS NOT NULL AND value BETWEEN 89.0 AND 111.0;

DROP TABLE noxu_verify_chimp_nulls;


-- ============================================================
-- SECTION 3: Timestamp Delta-of-Delta Encoding
-- Target: 4-6x compression for regular interval timestamps
-- ============================================================

-- 3a: Regular interval timestamps (ideal case)
CREATE TABLE noxu_verify_dod_regular (
    id int,
    event_time timestamp,
    event_time_tz timestamptz
) USING noxu;

-- Perfectly regular 1-second intervals
INSERT INTO noxu_verify_dod_regular
SELECT i,
       '2024-01-01 00:00:00'::timestamp + (i || ' seconds')::interval,
       '2024-01-01 00:00:00+00'::timestamptz + (i || ' seconds')::interval
FROM generate_series(1, 10000) i;

SELECT COUNT(*) AS total_rows FROM noxu_verify_dod_regular;

-- Correctness: verify first/last/boundary values
SELECT id, event_time, event_time_tz
FROM noxu_verify_dod_regular WHERE id IN (1, 5000, 10000) ORDER BY id;

-- Verify ordering is preserved
SELECT COUNT(*) AS monotonic_count
FROM (
    SELECT event_time,
           lag(event_time) OVER (ORDER BY id) AS prev_time
    FROM noxu_verify_dod_regular
) sub
WHERE prev_time IS NULL OR event_time > prev_time;

-- Verify interval arithmetic
SELECT id, event_time - lag(event_time) OVER (ORDER BY id) AS interval_gap
FROM noxu_verify_dod_regular
WHERE id <= 5
ORDER BY id;

SELECT pg_relation_size('noxu_verify_dod_regular') AS noxu_dod_regular_bytes;

-- Raw size: 10000 * (4 + 8 + 8) = 200000
SELECT CASE
    WHEN pg_relation_size('noxu_verify_dod_regular') > 0
    THEN round(200000.0 / pg_relation_size('noxu_verify_dod_regular'), 1)
    ELSE 0
END AS dod_regular_compression_ratio;

DROP TABLE noxu_verify_dod_regular;

-- 3b: Slightly irregular timestamps (real-world scenario)
CREATE TABLE noxu_verify_dod_irregular (
    id int,
    event_time timestamp
) USING noxu;

-- Timestamps with small jitter (typical of real sensor data)
INSERT INTO noxu_verify_dod_irregular
SELECT i,
       '2024-01-01 00:00:00'::timestamp
           + (i || ' seconds')::interval
           + ((random() * 100)::int || ' milliseconds')::interval
FROM generate_series(1, 10000) i;

SELECT COUNT(*) AS total_rows FROM noxu_verify_dod_irregular;

-- Correctness: verify timestamps are approximately 1 second apart
SELECT round(avg(EXTRACT(epoch FROM gap))::numeric, 2) AS avg_gap_seconds
FROM (
    SELECT event_time - lag(event_time) OVER (ORDER BY id) AS gap
    FROM noxu_verify_dod_irregular
) sub
WHERE gap IS NOT NULL;

SELECT pg_relation_size('noxu_verify_dod_irregular') AS noxu_dod_irregular_bytes;

-- Raw size: 10000 * (4 + 8) = 120000
SELECT CASE
    WHEN pg_relation_size('noxu_verify_dod_irregular') > 0
    THEN round(120000.0 / pg_relation_size('noxu_verify_dod_irregular'), 1)
    ELSE 0
END AS dod_irregular_compression_ratio;

DROP TABLE noxu_verify_dod_irregular;

-- 3c: Timestamps with NULLs
CREATE TABLE noxu_verify_dod_nulls (
    id int,
    event_time timestamp
) USING noxu;

INSERT INTO noxu_verify_dod_nulls
SELECT i,
       CASE WHEN i % 10 = 0 THEN NULL
            ELSE '2024-01-01 00:00:00'::timestamp + (i || ' seconds')::interval
       END
FROM generate_series(1, 5000) i;

SELECT COUNT(*) AS total_rows FROM noxu_verify_dod_nulls;
SELECT COUNT(*) AS null_count FROM noxu_verify_dod_nulls WHERE event_time IS NULL;
SELECT COUNT(*) AS nonnull_count FROM noxu_verify_dod_nulls WHERE event_time IS NOT NULL;

-- Verify non-NULL values round-trip correctly
SELECT id, event_time FROM noxu_verify_dod_nulls WHERE id IN (1, 9, 10, 11) ORDER BY id;

DROP TABLE noxu_verify_dod_nulls;

-- 3d: Date column (should also benefit from delta-of-delta)
CREATE TABLE noxu_verify_dod_date (
    id int,
    event_date date
) USING noxu;

INSERT INTO noxu_verify_dod_date
SELECT i, '2024-01-01'::date + i
FROM generate_series(1, 5000) i;

SELECT COUNT(*) AS total_rows FROM noxu_verify_dod_date;
SELECT MIN(event_date), MAX(event_date) FROM noxu_verify_dod_date;

-- Verify consecutive dates
SELECT id, event_date
FROM noxu_verify_dod_date WHERE id IN (1, 2500, 5000) ORDER BY id;

DROP TABLE noxu_verify_dod_date;


-- ============================================================
-- SECTION 4: UUID v7 Time-Ordered Compression
-- Target: 3-5x compression for time-ordered UUIDs
-- ============================================================

-- 4a: Time-ordered UUIDs (simulated UUID v7 with monotonic prefix)
CREATE TABLE noxu_verify_uuid_v7 (
    id int,
    uuid_col uuid
) USING noxu;

-- Generate UUIDs with a time-ordered prefix (simulating UUID v7 structure)
-- UUID v7 has millisecond timestamp in the first 48 bits
INSERT INTO noxu_verify_uuid_v7
SELECT i,
       -- Build UUID with monotonic time prefix: hex(timestamp_ms) + random suffix
       lpad(to_hex((1700000000000 + i)::bigint), 12, '0')
       || '-'
       || lpad(to_hex((i % 65536)::int), 4, '0')
       || '-7'  -- version 7
       || lpad(to_hex((random() * 4095)::int), 3, '0')
       || '-'
       || (ARRAY['8','9','a','b'])[1 + (i % 4)]
       || lpad(to_hex((random() * 4095)::int), 3, '0')
       || '-'
       || lpad(to_hex(floor(random() * 281474976710656)::bigint), 12, '0')
FROM generate_series(1, 10000) i;

SELECT COUNT(*) AS total_rows FROM noxu_verify_uuid_v7;

-- Correctness: verify all UUIDs are valid
SELECT COUNT(*) AS valid_uuid_count
FROM noxu_verify_uuid_v7
WHERE uuid_col IS NOT NULL;

-- Verify distinctness
SELECT COUNT(DISTINCT uuid_col) AS distinct_count FROM noxu_verify_uuid_v7;

-- Verify ordering preserved
SELECT id, uuid_col
FROM noxu_verify_uuid_v7 WHERE id IN (1, 5000, 10000) ORDER BY id;

SELECT pg_relation_size('noxu_verify_uuid_v7') AS noxu_uuid_v7_bytes;

-- Raw size: 10000 * (4 + 16) = 200000
SELECT CASE
    WHEN pg_relation_size('noxu_verify_uuid_v7') > 0
    THEN round(200000.0 / pg_relation_size('noxu_verify_uuid_v7'), 1)
    ELSE 0
END AS uuid_v7_compression_ratio;

DROP TABLE noxu_verify_uuid_v7;

-- 4b: Random UUIDs (worst case - should still store efficiently)
CREATE TABLE noxu_verify_uuid_random (
    id int,
    uuid_col uuid
) USING noxu;

INSERT INTO noxu_verify_uuid_random
SELECT i, gen_random_uuid()
FROM generate_series(1, 10000) i;

SELECT COUNT(*) AS total_rows FROM noxu_verify_uuid_random;
SELECT COUNT(DISTINCT uuid_col) AS distinct_count FROM noxu_verify_uuid_random;

-- Correctness: verify specific UUIDs survive round-trip
INSERT INTO noxu_verify_uuid_random VALUES
    (10001, '550e8400-e29b-41d4-a716-446655440000'::uuid),
    (10002, '00000000-0000-0000-0000-000000000000'::uuid),
    (10003, 'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid);

SELECT id, uuid_col FROM noxu_verify_uuid_random
WHERE id >= 10001 ORDER BY id;

SELECT pg_relation_size('noxu_verify_uuid_random') AS noxu_uuid_random_bytes;

DROP TABLE noxu_verify_uuid_random;

-- 4c: UUIDs with NULLs
CREATE TABLE noxu_verify_uuid_nulls (
    id int,
    uuid_col uuid
) USING noxu;

INSERT INTO noxu_verify_uuid_nulls
SELECT i,
       CASE WHEN i % 5 = 0 THEN NULL ELSE gen_random_uuid() END
FROM generate_series(1, 5000) i;

SELECT COUNT(*) AS total_rows FROM noxu_verify_uuid_nulls;
SELECT COUNT(*) AS null_count FROM noxu_verify_uuid_nulls WHERE uuid_col IS NULL;
SELECT COUNT(*) AS nonnull_count FROM noxu_verify_uuid_nulls WHERE uuid_col IS NOT NULL;

DROP TABLE noxu_verify_uuid_nulls;


-- ============================================================
-- SECTION 5: pg_vector Scalar Quantization (float16)
-- Target: 5-8x compression for high-dimensional embeddings
-- (Using float8[] to represent vectors since pgvector may not be installed)
-- ============================================================

-- 5a: High-dimensional embeddings (768-dim, typical BERT embedding)
CREATE TABLE noxu_verify_vector_768 (
    id int,
    embedding float8[]
) USING noxu;

-- Generate realistic-looking embeddings (small values near zero)
INSERT INTO noxu_verify_vector_768
SELECT i,
       ARRAY(SELECT (random() - 0.5) * 0.1
             FROM generate_series(1, 768) j)
FROM generate_series(1, 1000) i;

SELECT COUNT(*) AS total_rows FROM noxu_verify_vector_768;

-- Correctness: verify array dimensions
SELECT COUNT(*) AS correct_dim_count
FROM noxu_verify_vector_768
WHERE array_length(embedding, 1) = 768;

-- Verify values are in expected range
SELECT id,
       round(embedding[1]::numeric, 4) AS first_elem,
       round(embedding[384]::numeric, 4) AS mid_elem,
       round(embedding[768]::numeric, 4) AS last_elem
FROM noxu_verify_vector_768 WHERE id IN (1, 500, 1000) ORDER BY id;

-- Verify L2 norm is reasonable (should be around sqrt(768 * var) ~ sqrt(768 * 0.000833) ~ 0.8)
SELECT id,
       round(sqrt((SELECT SUM(v * v) FROM unnest(embedding) v))::numeric, 2) AS l2_norm
FROM noxu_verify_vector_768 WHERE id IN (1, 500, 1000) ORDER BY id;

SELECT pg_relation_size('noxu_verify_vector_768') AS noxu_vector_768_bytes;

-- Raw size: 1000 * (4 + 768 * 8) = 1000 * 6148 = 6148000
SELECT CASE
    WHEN pg_relation_size('noxu_verify_vector_768') > 0
    THEN round(6148000.0 / pg_relation_size('noxu_verify_vector_768'), 1)
    ELSE 0
END AS vector_768_compression_ratio;

DROP TABLE noxu_verify_vector_768;

-- 5b: Lower dimensional vectors (128-dim, for variation)
CREATE TABLE noxu_verify_vector_128 (
    id int,
    embedding float8[]
) USING noxu;

INSERT INTO noxu_verify_vector_128
SELECT i,
       ARRAY(SELECT (random() - 0.5) * 0.2
             FROM generate_series(1, 128) j)
FROM generate_series(1, 5000) i;

SELECT COUNT(*) AS total_rows FROM noxu_verify_vector_128;

SELECT COUNT(*) AS correct_dim_count
FROM noxu_verify_vector_128
WHERE array_length(embedding, 1) = 128;

SELECT pg_relation_size('noxu_verify_vector_128') AS noxu_vector_128_bytes;

-- Raw size: 5000 * (4 + 128 * 8) = 5000 * 1028 = 5140000
SELECT CASE
    WHEN pg_relation_size('noxu_verify_vector_128') > 0
    THEN round(5140000.0 / pg_relation_size('noxu_verify_vector_128'), 1)
    ELSE 0
END AS vector_128_compression_ratio;

DROP TABLE noxu_verify_vector_128;


-- ============================================================
-- SECTION 6: Mixed-Type Correctness Tests
-- Verify compression works correctly when multiple encoded types
-- coexist in the same table.
-- ============================================================

CREATE TABLE noxu_verify_mixed (
    id int,
    ts timestamp,
    sensor_reading float8,
    tags int[],
    device_uuid uuid,
    is_valid boolean
) USING noxu;

INSERT INTO noxu_verify_mixed
SELECT i,
       '2024-01-01 00:00:00'::timestamp + (i || ' seconds')::interval,
       20.0 + sin(i::float8 / 100) * 5 + random() * 0.01,
       ARRAY[i, i+1, i+2, i+3, i+4],
       gen_random_uuid(),
       (i % 3 != 0)
FROM generate_series(1, 5000) i;

SELECT COUNT(*) AS total_rows FROM noxu_verify_mixed;

-- Correctness: verify all columns round-trip
SELECT id, ts, round(sensor_reading::numeric, 2) AS reading,
       tags[1], tags[5],
       device_uuid IS NOT NULL AS has_uuid,
       is_valid
FROM noxu_verify_mixed WHERE id IN (1, 2500, 5000) ORDER BY id;

-- Verify filtering across compressed columns
SELECT COUNT(*) FROM noxu_verify_mixed
WHERE ts > '2024-01-01 01:00:00'
  AND sensor_reading > 20.0
  AND is_valid = true;

-- Verify aggregation
SELECT round(avg(sensor_reading)::numeric, 2) AS avg_reading,
       min(ts) AS min_ts,
       max(ts) AS max_ts,
       COUNT(*) FILTER (WHERE is_valid) AS valid_count
FROM noxu_verify_mixed;

DROP TABLE noxu_verify_mixed;


-- ============================================================
-- SECTION 7: Edge Cases and Boundary Conditions
-- ============================================================

-- 7a: Empty arrays
CREATE TABLE noxu_verify_empty_array (
    id int,
    values int[]
) USING noxu;

INSERT INTO noxu_verify_empty_array VALUES
    (1, '{}'),
    (2, '{1}'),
    (3, NULL);

SELECT * FROM noxu_verify_empty_array ORDER BY id;
DROP TABLE noxu_verify_empty_array;

-- 7b: Single-element arrays
CREATE TABLE noxu_verify_single_array (
    id int,
    values int[]
) USING noxu;

INSERT INTO noxu_verify_single_array
SELECT i, ARRAY[i * 42]
FROM generate_series(1, 1000) i;

SELECT COUNT(*) AS total_rows FROM noxu_verify_single_array;
SELECT id, values FROM noxu_verify_single_array WHERE id IN (1, 500, 1000) ORDER BY id;
DROP TABLE noxu_verify_single_array;

-- 7c: Extreme timestamp values
CREATE TABLE noxu_verify_extreme_ts (
    id int,
    ts timestamp
) USING noxu;

INSERT INTO noxu_verify_extreme_ts VALUES
    (1, '0001-01-01 00:00:00'),
    (2, '2024-06-15 12:30:45.123456'),
    (3, '9999-12-31 23:59:59.999999'),
    (4, NULL);

SELECT * FROM noxu_verify_extreme_ts ORDER BY id;
DROP TABLE noxu_verify_extreme_ts;

-- 7d: Float special values
CREATE TABLE noxu_verify_float_special (
    id int,
    value float8
) USING noxu;

INSERT INTO noxu_verify_float_special VALUES
    (1, 0.0),
    (2, -0.0),
    (3, 'Infinity'::float8),
    (4, '-Infinity'::float8),
    (5, 'NaN'::float8),
    (6, 1.7976931348623157e+308),  -- max float8
    (7, 2.2250738585072014e-308),  -- min positive normal float8
    (8, NULL);

SELECT id, value, value::text FROM noxu_verify_float_special ORDER BY id;
DROP TABLE noxu_verify_float_special;

-- 7e: UUID boundary values
CREATE TABLE noxu_verify_uuid_boundary (
    id int,
    uuid_col uuid
) USING noxu;

INSERT INTO noxu_verify_uuid_boundary VALUES
    (1, '00000000-0000-0000-0000-000000000000'::uuid),
    (2, 'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid),
    (3, '00000000-0000-7000-8000-000000000000'::uuid),  -- v7 min-ish
    (4, NULL);

SELECT * FROM noxu_verify_uuid_boundary ORDER BY id;
DROP TABLE noxu_verify_uuid_boundary;


-- ============================================================
-- SECTION 8: UPDATE and DELETE on Compressed Data
-- Verify MVCC operations work correctly with all encodings.
-- ============================================================

-- 8a: Update compressed float data
CREATE TABLE noxu_verify_update_float (
    id int,
    value float8
) USING noxu;

INSERT INTO noxu_verify_update_float
SELECT i, 100.0 + sin(i::float8 / 50) * 10
FROM generate_series(1, 1000) i;

UPDATE noxu_verify_update_float SET value = -999.0 WHERE id <= 10;
SELECT id, round(value::numeric, 2) AS value
FROM noxu_verify_update_float WHERE id <= 15 ORDER BY id;

DELETE FROM noxu_verify_update_float WHERE id <= 5;
SELECT COUNT(*) FROM noxu_verify_update_float;

DROP TABLE noxu_verify_update_float;

-- 8b: Update compressed timestamp data
CREATE TABLE noxu_verify_update_ts (
    id int,
    event_time timestamp
) USING noxu;

INSERT INTO noxu_verify_update_ts
SELECT i, '2024-01-01'::timestamp + (i || ' minutes')::interval
FROM generate_series(1, 1000) i;

UPDATE noxu_verify_update_ts SET event_time = '1999-12-31 23:59:59' WHERE id <= 10;
SELECT id, event_time
FROM noxu_verify_update_ts WHERE id <= 15 ORDER BY id;

DELETE FROM noxu_verify_update_ts WHERE id <= 5;
SELECT COUNT(*) FROM noxu_verify_update_ts;

DROP TABLE noxu_verify_update_ts;

-- 8c: Update compressed array data
CREATE TABLE noxu_verify_update_array (
    id int,
    values int[]
) USING noxu;

INSERT INTO noxu_verify_update_array
SELECT i, ARRAY[i, i*2, i*3]
FROM generate_series(1, 1000) i;

UPDATE noxu_verify_update_array SET values = ARRAY[0, 0, 0] WHERE id <= 10;
SELECT id, values
FROM noxu_verify_update_array WHERE id <= 15 ORDER BY id;

DELETE FROM noxu_verify_update_array WHERE id <= 5;
SELECT COUNT(*) FROM noxu_verify_update_array;

DROP TABLE noxu_verify_update_array;


-- ============================================================
-- SECTION 9: Performance Benchmarks
-- Measures scan/query speed on compressed data.
-- ============================================================

-- 9a: Large table for scan performance
CREATE TABLE noxu_verify_perf_scan (
    id int,
    ts timestamp,
    sensor_a float8,
    sensor_b float8,
    tags int[],
    device_id uuid
) USING noxu;

INSERT INTO noxu_verify_perf_scan
SELECT i,
       '2024-01-01'::timestamp + (i || ' seconds')::interval,
       20.0 + sin(i::float8 / 100) * 5,
       50.0 + cos(i::float8 / 200) * 10,
       ARRAY[i % 100, i % 50, i % 25],
       gen_random_uuid()
FROM generate_series(1, 50000) i;

-- Full table scan
SELECT COUNT(*) AS full_scan_count FROM noxu_verify_perf_scan;

-- Column projection (should be fast with columnar storage)
SELECT COUNT(*), round(avg(sensor_a)::numeric, 2)
FROM noxu_verify_perf_scan;

-- Filtered scan on timestamp
SELECT COUNT(*)
FROM noxu_verify_perf_scan
WHERE ts BETWEEN '2024-01-01 01:00:00' AND '2024-01-01 02:00:00';

-- Aggregation on float columns
SELECT round(avg(sensor_a)::numeric, 4) AS avg_a,
       round(avg(sensor_b)::numeric, 4) AS avg_b,
       round(stddev(sensor_a)::numeric, 4) AS stddev_a,
       round(stddev(sensor_b)::numeric, 4) AS stddev_b
FROM noxu_verify_perf_scan;

-- Storage size
SELECT pg_relation_size('noxu_verify_perf_scan') AS noxu_perf_total_bytes;

-- Overall compression ratio
-- Raw size: 50000 * (4 + 8 + 8 + 8 + 3*4 + 16) = 50000 * 56 = 2800000
SELECT CASE
    WHEN pg_relation_size('noxu_verify_perf_scan') > 0
    THEN round(2800000.0 / pg_relation_size('noxu_verify_perf_scan'), 1)
    ELSE 0
END AS overall_compression_ratio;

DROP TABLE noxu_verify_perf_scan;

-- 9b: VACUUM on compressed tables
CREATE TABLE noxu_verify_vacuum (
    id int,
    value float8,
    ts timestamp
) USING noxu;

INSERT INTO noxu_verify_vacuum
SELECT i,
       random() * 100,
       '2024-01-01'::timestamp + (i || ' seconds')::interval
FROM generate_series(1, 10000) i;

-- Delete half the rows
DELETE FROM noxu_verify_vacuum WHERE id % 2 = 0;
SELECT COUNT(*) AS after_delete FROM noxu_verify_vacuum;

-- VACUUM should reclaim space
VACUUM noxu_verify_vacuum;
SELECT COUNT(*) AS after_vacuum FROM noxu_verify_vacuum;

-- Verify data integrity after vacuum
SELECT COUNT(*) AS odd_only
FROM noxu_verify_vacuum
WHERE id % 2 = 1;

DROP TABLE noxu_verify_vacuum;


-- ============================================================
-- SECTION 10: Compression Ratio Summary
-- Final summary comparing all compression types against targets.
-- ============================================================

-- Create summary table to aggregate results
CREATE TABLE noxu_compression_summary (
    test_name text,
    raw_bytes bigint,
    compressed_bytes bigint,
    compression_ratio numeric,
    target_min numeric,
    target_max numeric,
    meets_target boolean
);

-- Integer arrays
CREATE TABLE _tmp_int_arr (id int, values int[]) USING noxu;
INSERT INTO _tmp_int_arr
SELECT i, ARRAY(SELECT (i * 100 + j) FROM generate_series(1, 50) j)
FROM generate_series(1, 5000) i;
INSERT INTO noxu_compression_summary
SELECT 'int[] arrays', 5000 * 204, pg_relation_size('_tmp_int_arr'),
       round((5000.0 * 204) / GREATEST(pg_relation_size('_tmp_int_arr'), 1), 1),
       5.0, 10.0,
       round((5000.0 * 204) / GREATEST(pg_relation_size('_tmp_int_arr'), 1), 1) BETWEEN 5.0 AND 10.0;
DROP TABLE _tmp_int_arr;

-- Float time-series (Chimp)
CREATE TABLE _tmp_float_ts (id int, value float8) USING noxu;
INSERT INTO _tmp_float_ts
SELECT i, 20.0 + sin(i::float8 / 100) * 5 + random() * 0.01
FROM generate_series(1, 50000) i;
INSERT INTO noxu_compression_summary
SELECT 'float8 Chimp', 50000 * 12, pg_relation_size('_tmp_float_ts'),
       round((50000.0 * 12) / GREATEST(pg_relation_size('_tmp_float_ts'), 1), 1),
       4.0, 8.0,
       round((50000.0 * 12) / GREATEST(pg_relation_size('_tmp_float_ts'), 1), 1) BETWEEN 4.0 AND 8.0;
DROP TABLE _tmp_float_ts;

-- Timestamps (delta-of-delta)
CREATE TABLE _tmp_ts_dod (id int, ts timestamp) USING noxu;
INSERT INTO _tmp_ts_dod
SELECT i, '2024-01-01'::timestamp + (i || ' seconds')::interval
FROM generate_series(1, 50000) i;
INSERT INTO noxu_compression_summary
SELECT 'timestamp DoD', 50000 * 12, pg_relation_size('_tmp_ts_dod'),
       round((50000.0 * 12) / GREATEST(pg_relation_size('_tmp_ts_dod'), 1), 1),
       4.0, 6.0,
       round((50000.0 * 12) / GREATEST(pg_relation_size('_tmp_ts_dod'), 1), 1) BETWEEN 4.0 AND 6.0;
DROP TABLE _tmp_ts_dod;

-- UUID v7
CREATE TABLE _tmp_uuid_v7 (id int, u uuid) USING noxu;
INSERT INTO _tmp_uuid_v7
SELECT i,
       lpad(to_hex((1700000000000 + i)::bigint), 12, '0')
       || '-' || lpad(to_hex((i % 65536)::int), 4, '0')
       || '-7' || lpad(to_hex((random() * 4095)::int), 3, '0')
       || '-' || (ARRAY['8','9','a','b'])[1 + (i % 4)]
       || lpad(to_hex((random() * 4095)::int), 3, '0')
       || '-' || lpad(to_hex(floor(random() * 281474976710656)::bigint), 12, '0')
FROM generate_series(1, 50000) i;
INSERT INTO noxu_compression_summary
SELECT 'UUID v7 delta', 50000 * 20, pg_relation_size('_tmp_uuid_v7'),
       round((50000.0 * 20) / GREATEST(pg_relation_size('_tmp_uuid_v7'), 1), 1),
       3.0, 5.0,
       round((50000.0 * 20) / GREATEST(pg_relation_size('_tmp_uuid_v7'), 1), 1) BETWEEN 3.0 AND 5.0;
DROP TABLE _tmp_uuid_v7;

-- Vector embeddings (768-dim)
CREATE TABLE _tmp_vec768 (id int, emb float8[]) USING noxu;
INSERT INTO _tmp_vec768
SELECT i,
       ARRAY(SELECT (random() - 0.5) * 0.1 FROM generate_series(1, 768) j)
FROM generate_series(1, 500) i;
INSERT INTO noxu_compression_summary
SELECT 'vector[768] SQ', 500 * 6148, pg_relation_size('_tmp_vec768'),
       round((500.0 * 6148) / GREATEST(pg_relation_size('_tmp_vec768'), 1), 1),
       5.0, 8.0,
       round((500.0 * 6148) / GREATEST(pg_relation_size('_tmp_vec768'), 1), 1) BETWEEN 5.0 AND 8.0;
DROP TABLE _tmp_vec768;

-- Display final summary
SELECT test_name,
       pg_size_pretty(raw_bytes) AS raw_size,
       pg_size_pretty(compressed_bytes) AS compressed_size,
       compression_ratio || 'x' AS ratio,
       target_min || '-' || target_max || 'x' AS target,
       CASE WHEN meets_target THEN 'PASS' ELSE 'FAIL' END AS result
FROM noxu_compression_summary
ORDER BY test_name;

-- Overall pass/fail
SELECT CASE
    WHEN bool_and(meets_target) THEN 'ALL COMPRESSION TARGETS MET'
    ELSE 'SOME TARGETS NOT MET - see details above'
END AS overall_result
FROM noxu_compression_summary;

DROP TABLE noxu_compression_summary;
