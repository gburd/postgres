--
-- Test Chimp float compression (NXBT_ATTR_FORMAT_CHIMP)
--
-- Chimp is an XOR-based float compression algorithm that exploits bit-level
-- similarity between consecutive float values.  Time-series and sensor data
-- where values change slowly between readings compress very well.
--
-- Also tests bfloat16 scalar quantization for embedding vectors.
--
-- Targets:
--   Chimp float8 time-series: 4-8x compression
--   Chimp float4 time-series: 2-4x compression
--   bfloat16 quantization:    ~2x compression (float32 -> 16 bits)
--

-- ============================================================
-- Test 1: Slowly changing floats (typical sensor data)
-- ============================================================
CREATE TABLE noxu_chimp_sensor_test (
    id int,
    temperature float8,
    humidity float8,
    pressure float8
) USING noxu;

-- Sensor-like data: slow sinusoidal variation with small noise
INSERT INTO noxu_chimp_sensor_test
SELECT i,
       20.0 + sin(i::float8 / 100) * 5 + random() * 0.1,
       50.0 + cos(i::float8 / 200) * 10 + random() * 0.2,
       1013.25 + sin(i::float8 / 500) * 2 + random() * 0.05
FROM generate_series(1, 10000) i;

SELECT COUNT(*) FROM noxu_chimp_sensor_test;

-- Correctness: verify values are in expected ranges
SELECT COUNT(*) AS temp_in_range
FROM noxu_chimp_sensor_test
WHERE temperature BETWEEN 14.0 AND 26.0;

SELECT COUNT(*) AS humidity_in_range
FROM noxu_chimp_sensor_test
WHERE humidity BETWEEN 39.0 AND 61.0;

SELECT COUNT(*) AS pressure_in_range
FROM noxu_chimp_sensor_test
WHERE pressure BETWEEN 1010.0 AND 1016.0;

-- Verify statistical properties survive compression
SELECT round(avg(temperature)::numeric, 1) AS avg_temp,
       round(stddev(temperature)::numeric, 1) AS stddev_temp,
       round(min(temperature)::numeric, 1) AS min_temp,
       round(max(temperature)::numeric, 1) AS max_temp
FROM noxu_chimp_sensor_test;

-- Compression ratio
SELECT pg_relation_size('noxu_chimp_sensor_test') AS noxu_chimp_sensor_bytes;

-- Raw size: 10000 * (4 + 8 + 8 + 8) = 280000
SELECT CASE
    WHEN pg_relation_size('noxu_chimp_sensor_test') > 0
    THEN round(280000.0 / pg_relation_size('noxu_chimp_sensor_test'), 1)
    ELSE 0
END AS chimp_sensor_compression_ratio;

DROP TABLE noxu_chimp_sensor_test;


-- ============================================================
-- Test 2: Nearly constant floats (best case for Chimp)
-- ============================================================
CREATE TABLE noxu_chimp_constant_test (
    id int,
    value float8
) USING noxu;

-- Values clustered very tightly around 42.0
INSERT INTO noxu_chimp_constant_test
SELECT i, 42.0 + random() * 0.001
FROM generate_series(1, 10000) i;

SELECT COUNT(*) FROM noxu_chimp_constant_test;

SELECT round(avg(value)::numeric, 3) AS avg_val,
       round(stddev(value)::numeric, 6) AS stddev_val
FROM noxu_chimp_constant_test;

SELECT pg_relation_size('noxu_chimp_constant_test') AS noxu_chimp_const_bytes;

-- Raw size: 10000 * (4 + 8) = 120000
SELECT CASE
    WHEN pg_relation_size('noxu_chimp_constant_test') > 0
    THEN round(120000.0 / pg_relation_size('noxu_chimp_constant_test'), 1)
    ELSE 0
END AS chimp_constant_compression_ratio;

DROP TABLE noxu_chimp_constant_test;


-- ============================================================
-- Test 3: Monotonically increasing floats (common in counters)
-- ============================================================
CREATE TABLE noxu_chimp_monotonic_test (
    id int,
    counter float8
) USING noxu;

INSERT INTO noxu_chimp_monotonic_test
SELECT i, i::float8 * 1.5 + random() * 0.01
FROM generate_series(1, 10000) i;

SELECT COUNT(*) FROM noxu_chimp_monotonic_test;

-- Verify monotonicity preserved
SELECT COUNT(*) AS monotonic_count
FROM (
    SELECT counter > lag(counter) OVER (ORDER BY id) AS ok
    FROM noxu_chimp_monotonic_test
) sub
WHERE ok IS NOT NULL AND ok = true;

-- Verify boundary values
SELECT id, round(counter::numeric, 4)
FROM noxu_chimp_monotonic_test WHERE id IN (1, 5000, 10000) ORDER BY id;

DROP TABLE noxu_chimp_monotonic_test;


-- ============================================================
-- Test 4: float4 columns (4-byte floats)
-- ============================================================
CREATE TABLE noxu_chimp_float4_test (
    id int,
    reading float4
) USING noxu;

INSERT INTO noxu_chimp_float4_test
SELECT i, (25.0 + sin(i::float8 / 50) * 3)::float4
FROM generate_series(1, 5000) i;

SELECT COUNT(*) FROM noxu_chimp_float4_test;

-- Verify values in expected range
SELECT COUNT(*) AS in_range
FROM noxu_chimp_float4_test
WHERE reading BETWEEN 21.0 AND 29.0;

-- Verify specific values round-trip
SELECT id, round(reading::numeric, 2)
FROM noxu_chimp_float4_test WHERE id IN (1, 2500, 5000) ORDER BY id;

DROP TABLE noxu_chimp_float4_test;


-- ============================================================
-- Test 5: Floats with NULLs (Chimp must skip NULLs correctly)
-- ============================================================
CREATE TABLE noxu_chimp_null_test (
    id int,
    value float8
) USING noxu;

INSERT INTO noxu_chimp_null_test
SELECT i,
       CASE WHEN i % 7 = 0 THEN NULL
            ELSE 100.0 + sin(i::float8 / 50) * 10
       END
FROM generate_series(1, 5000) i;

SELECT COUNT(*) FROM noxu_chimp_null_test;
SELECT COUNT(*) AS null_count FROM noxu_chimp_null_test WHERE value IS NULL;
SELECT COUNT(*) AS nonnull_count FROM noxu_chimp_null_test WHERE value IS NOT NULL;

-- Verify NULL pattern is correct (every 7th row)
SELECT COUNT(*) AS expected_nulls
FROM noxu_chimp_null_test WHERE id % 7 = 0 AND value IS NULL;

SELECT COUNT(*) AS expected_nonnulls
FROM noxu_chimp_null_test WHERE id % 7 != 0 AND value IS NOT NULL;

-- Verify non-NULL values are in expected range
SELECT COUNT(*) AS values_in_range
FROM noxu_chimp_null_test
WHERE value IS NOT NULL AND value BETWEEN 89.0 AND 111.0;

DROP TABLE noxu_chimp_null_test;


-- ============================================================
-- Test 6: Float special values (NaN, Infinity, -0.0)
-- ============================================================
CREATE TABLE noxu_chimp_special_test (
    id int,
    value float8
) USING noxu;

INSERT INTO noxu_chimp_special_test VALUES
    (1, 0.0),
    (2, -0.0),
    (3, 'Infinity'::float8),
    (4, '-Infinity'::float8),
    (5, 'NaN'::float8),
    (6, 1.7976931348623157e+308),   -- max float8
    (7, 2.2250738585072014e-308),   -- min positive normal float8
    (8, 5e-324),                    -- min positive subnormal
    (9, NULL);

SELECT id, value, value::text FROM noxu_chimp_special_test ORDER BY id;

-- Verify NaN comparison semantics survive
SELECT id FROM noxu_chimp_special_test WHERE value = 'NaN'::float8;

-- Verify infinity comparison
SELECT id FROM noxu_chimp_special_test WHERE value = 'Infinity'::float8;
SELECT id FROM noxu_chimp_special_test WHERE value = '-Infinity'::float8;

DROP TABLE noxu_chimp_special_test;


-- ============================================================
-- Test 7: Mixed Chimp + other compression in same table
-- ============================================================
CREATE TABLE noxu_chimp_mixed_test (
    id int,
    ts timestamp,
    sensor_reading float8,
    tags text[],
    status text
) USING noxu;

INSERT INTO noxu_chimp_mixed_test
SELECT i,
       '2024-01-01 00:00:00'::timestamp + (i || ' seconds')::interval,
       20.0 + sin(i::float8 / 100) * 5 + random() * 0.01,
       ARRAY[i % 100, i % 50, i % 25]::text[],
       (ARRAY['active','idle','error'])[1 + (i % 3)]
FROM generate_series(1, 5000) i;

SELECT COUNT(*) FROM noxu_chimp_mixed_test;

-- Verify all columns round-trip
SELECT id, ts, round(sensor_reading::numeric, 2) AS reading,
       tags[1], status
FROM noxu_chimp_mixed_test WHERE id IN (1, 2500, 5000) ORDER BY id;

-- Filtered scan on float column
SELECT COUNT(*)
FROM noxu_chimp_mixed_test
WHERE sensor_reading > 22.0 AND sensor_reading < 24.0;

-- Aggregation on compressed float
SELECT round(avg(sensor_reading)::numeric, 2) AS avg_reading,
       round(stddev(sensor_reading)::numeric, 2) AS stddev_reading
FROM noxu_chimp_mixed_test;

DROP TABLE noxu_chimp_mixed_test;


-- ============================================================
-- Test 8: High-variance random floats (worst case for Chimp)
-- ============================================================
CREATE TABLE noxu_chimp_random_test (
    id int,
    value float8
) USING noxu;

-- Fully random: Chimp should still store correctly, just less compression
INSERT INTO noxu_chimp_random_test
SELECT i, random() * 1000000 - 500000
FROM generate_series(1, 5000) i;

SELECT COUNT(*) FROM noxu_chimp_random_test;

-- Verify all values are non-NULL
SELECT COUNT(*) AS nonnull_count
FROM noxu_chimp_random_test WHERE value IS NOT NULL;

-- Verify range is approximately what we inserted
SELECT round(min(value)::numeric, 0) < -400000 AS has_low,
       round(max(value)::numeric, 0) > 400000 AS has_high
FROM noxu_chimp_random_test;

DROP TABLE noxu_chimp_random_test;


-- ============================================================
-- Test 9: Sensor data with small variations (typical IoT pattern)
-- ============================================================
CREATE TABLE noxu_chimp_iot_test (
    id int,
    device_id int,
    temp_celsius float8,
    voltage float4,
    signal_db float4
) USING noxu;

-- Multiple simulated sensors with slowly drifting values
INSERT INTO noxu_chimp_iot_test
SELECT i,
       i % 10,  -- 10 devices
       22.5 + sin(i::float8 / 200) * 0.5 + random() * 0.05,
       (3.3 + cos(i::float8 / 1000) * 0.01 + random() * 0.001)::float4,
       (-65.0 + sin(i::float8 / 150) * 2 + random() * 0.3)::float4
FROM generate_series(1, 20000) i;

SELECT COUNT(*) FROM noxu_chimp_iot_test;

-- Verify per-device statistics
SELECT device_id,
       round(avg(temp_celsius)::numeric, 2) AS avg_temp,
       round(avg(voltage::float8)::numeric, 3) AS avg_voltage,
       round(avg(signal_db::float8)::numeric, 1) AS avg_signal
FROM noxu_chimp_iot_test
WHERE device_id IN (0, 5, 9)
GROUP BY device_id ORDER BY device_id;

-- Compression ratio for multi-float table
SELECT pg_relation_size('noxu_chimp_iot_test') AS noxu_iot_bytes;

-- Raw size: 20000 * (4 + 4 + 8 + 4 + 4) = 20000 * 24 = 480000
SELECT CASE
    WHEN pg_relation_size('noxu_chimp_iot_test') > 0
    THEN round(480000.0 / pg_relation_size('noxu_chimp_iot_test'), 1)
    ELSE 0
END AS iot_compression_ratio;

DROP TABLE noxu_chimp_iot_test;


-- ============================================================
-- Test 10: UPDATE and DELETE on Chimp-compressed columns
-- ============================================================
CREATE TABLE noxu_chimp_update_test (
    id int,
    value float8
) USING noxu;

INSERT INTO noxu_chimp_update_test
SELECT i, 100.0 + sin(i::float8 / 50) * 10
FROM generate_series(1, 1000) i;

-- Update: overwrite compressed values
UPDATE noxu_chimp_update_test SET value = -999.0 WHERE id <= 10;
SELECT id, round(value::numeric, 2) AS value
FROM noxu_chimp_update_test WHERE id <= 15 ORDER BY id;

-- Delete from compressed data
DELETE FROM noxu_chimp_update_test WHERE id <= 5;
SELECT COUNT(*) FROM noxu_chimp_update_test;

-- Verify remaining rows are intact
SELECT id, round(value::numeric, 2) AS value
FROM noxu_chimp_update_test WHERE id IN (6, 7, 8, 9, 10, 11, 12) ORDER BY id;

DROP TABLE noxu_chimp_update_test;


-- ============================================================
-- Test 11: Large batch to trigger page splits with Chimp data
-- ============================================================
CREATE TABLE noxu_chimp_large_test (
    id int,
    value float8,
    payload text
) USING noxu;

INSERT INTO noxu_chimp_large_test
SELECT i,
       37.0 + sin(i::float8 / 300) * 8 + random() * 0.01,
       'row_' || i
FROM generate_series(1, 50000) i;

SELECT COUNT(*) FROM noxu_chimp_large_test;

-- Spot-check values at various points
SELECT id, round(value::numeric, 4)
FROM noxu_chimp_large_test
WHERE id IN (1, 10000, 25000, 50000)
ORDER BY id;

-- Full-table aggregation on compressed data
SELECT round(avg(value)::numeric, 4) AS avg_val,
       round(stddev(value)::numeric, 4) AS stddev_val
FROM noxu_chimp_large_test;

-- Filtered scan
SELECT COUNT(*)
FROM noxu_chimp_large_test
WHERE value BETWEEN 35.0 AND 39.0;

DROP TABLE noxu_chimp_large_test;


-- ============================================================
-- Test 12: Compression ratio verification for float8 time-series
-- ============================================================
CREATE TABLE noxu_chimp_ratio_test (
    id int,
    value float8
) USING noxu;

-- Slowly varying time-series (ideal Chimp input)
INSERT INTO noxu_chimp_ratio_test
SELECT i, 20.0 + sin(i::float8 / 100) * 5 + random() * 0.01
FROM generate_series(1, 50000) i;

SELECT pg_relation_size('noxu_chimp_ratio_test') AS noxu_chimp_ratio_bytes;

-- Raw size: 50000 * (4 + 8) = 600000
-- Target: 4-8x compression
SELECT CASE
    WHEN pg_relation_size('noxu_chimp_ratio_test') > 0
    THEN round(600000.0 / pg_relation_size('noxu_chimp_ratio_test'), 1)
    ELSE 0
END AS chimp_timeseries_compression_ratio;

DROP TABLE noxu_chimp_ratio_test;
