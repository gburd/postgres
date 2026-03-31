--
-- Test array element-level compression (NXBT_ATTR_FORMAT_ARRAY_DECOMPOSED)
--
-- Noxu decomposes PostgreSQL arrays into contiguous element streams,
-- eliminating ArrayType headers, alignment padding, and per-element
-- varlena headers.  Supported element types: bool, int2, int4, int8,
-- float4, float8, timestamp, text, uuid.
--
-- Target: 5-10x compression for homogeneous numeric/bool arrays.
--

-- ============================================================
-- Test 1: Boolean arrays (bitpacking within decomposed stream)
-- ============================================================
CREATE TABLE noxu_array_bool_test (
    id int,
    flags boolean[]
) USING noxu;

-- 64-element boolean arrays: alternating pattern
INSERT INTO noxu_array_bool_test
SELECT i, ARRAY(SELECT (j % 2 = 0) FROM generate_series(1, 64) j)
FROM generate_series(1, 1000) i;

SELECT COUNT(*) FROM noxu_array_bool_test;

-- Verify round-trip: first/last elements
SELECT id, flags[1], flags[2], flags[63], flags[64]
FROM noxu_array_bool_test WHERE id IN (1, 500, 1000) ORDER BY id;

-- Verify array length preserved
SELECT COUNT(*) AS correct_length
FROM noxu_array_bool_test WHERE array_length(flags, 1) = 64;

-- All TRUE / all FALSE patterns
INSERT INTO noxu_array_bool_test VALUES
    (1001, ARRAY(SELECT true FROM generate_series(1, 64))),
    (1002, ARRAY(SELECT false FROM generate_series(1, 64)));

SELECT id, flags[1], flags[32], flags[64]
FROM noxu_array_bool_test WHERE id >= 1001 ORDER BY id;

DROP TABLE noxu_array_bool_test;


-- ============================================================
-- Test 2: Integer arrays (FOR encoding within decomposed stream)
-- ============================================================
CREATE TABLE noxu_array_int_test (
    id int,
    values int[]
) USING noxu;

-- 50-element int arrays with sequential elements
INSERT INTO noxu_array_int_test
SELECT i, ARRAY(SELECT (i * 100 + j) FROM generate_series(1, 50) j)
FROM generate_series(1, 2000) i;

SELECT COUNT(*) FROM noxu_array_int_test;

-- Verify specific values round-trip correctly
SELECT id, values[1], values[25], values[50]
FROM noxu_array_int_test WHERE id IN (1, 1000, 2000) ORDER BY id;

-- Verify array length preserved
SELECT COUNT(*) AS correct_length
FROM noxu_array_int_test WHERE array_length(values, 1) = 50;

-- Verify sum of elements (strong correctness check)
SELECT id,
       (SELECT SUM(v) FROM unnest(values) v) AS elem_sum
FROM noxu_array_int_test WHERE id <= 3 ORDER BY id;

-- int2 arrays
CREATE TABLE noxu_array_int2_test (
    id int,
    vals int2[]
) USING noxu;

INSERT INTO noxu_array_int2_test
SELECT i, ARRAY(SELECT (j % 100)::int2 FROM generate_series(1, 30) j)
FROM generate_series(1, 500) i;

SELECT COUNT(*) FROM noxu_array_int2_test;
SELECT id, vals[1], vals[15], vals[30]
FROM noxu_array_int2_test WHERE id IN (1, 250, 500) ORDER BY id;

DROP TABLE noxu_array_int2_test;

-- int8 (bigint) arrays
CREATE TABLE noxu_array_int8_test (
    id int,
    vals bigint[]
) USING noxu;

INSERT INTO noxu_array_int8_test
SELECT i, ARRAY(SELECT (i::bigint * 1000000 + j) FROM generate_series(1, 20) j)
FROM generate_series(1, 500) i;

SELECT COUNT(*) FROM noxu_array_int8_test;
SELECT id, vals[1], vals[20]
FROM noxu_array_int8_test WHERE id IN (1, 250, 500) ORDER BY id;

DROP TABLE noxu_array_int8_test;
DROP TABLE noxu_array_int_test;


-- ============================================================
-- Test 3: Timestamp arrays (delta-of-delta within decomposed stream)
-- ============================================================
CREATE TABLE noxu_array_ts_test (
    id int,
    event_times timestamp[]
) USING noxu;

-- 30-element timestamp arrays with 1-second intervals
INSERT INTO noxu_array_ts_test
SELECT i,
       ARRAY(SELECT '2024-01-01 00:00:00'::timestamp + ((i * 1000 + j) || ' seconds')::interval
             FROM generate_series(1, 30) j)
FROM generate_series(1, 2000) i;

SELECT COUNT(*) FROM noxu_array_ts_test;

-- Verify first/last timestamps round-trip
SELECT id, event_times[1], event_times[30]
FROM noxu_array_ts_test WHERE id IN (1, 1000, 2000) ORDER BY id;

-- Verify array length
SELECT COUNT(*) AS correct_length
FROM noxu_array_ts_test WHERE array_length(event_times, 1) = 30;

-- Verify ordering within arrays is preserved
SELECT id,
       bool_and(event_times[j] < event_times[j+1]) AS monotonic
FROM noxu_array_ts_test,
     generate_series(1, 29) j
WHERE id = 1
GROUP BY id;

DROP TABLE noxu_array_ts_test;


-- ============================================================
-- Test 4: Float arrays (Chimp within decomposed stream)
-- ============================================================
CREATE TABLE noxu_array_float4_test (
    id int,
    measurements float4[]
) USING noxu;

INSERT INTO noxu_array_float4_test
SELECT i,
       ARRAY(SELECT (100.0 + j * 0.01 + random() * 0.001)::float4
             FROM generate_series(1, 40) j)
FROM generate_series(1, 1000) i;

SELECT COUNT(*) FROM noxu_array_float4_test;

SELECT id, measurements[1], measurements[40]
FROM noxu_array_float4_test WHERE id IN (1, 500, 1000) ORDER BY id;

SELECT COUNT(*) AS correct_length
FROM noxu_array_float4_test WHERE array_length(measurements, 1) = 40;

DROP TABLE noxu_array_float4_test;

-- float8 arrays
CREATE TABLE noxu_array_float8_test (
    id int,
    measurements float8[]
) USING noxu;

INSERT INTO noxu_array_float8_test
SELECT i,
       ARRAY(SELECT 100.0 + (j * 0.01) + (random() * 0.001)
             FROM generate_series(1, 50) j)
FROM generate_series(1, 2000) i;

SELECT COUNT(*) FROM noxu_array_float8_test;

SELECT id, measurements[1], measurements[50]
FROM noxu_array_float8_test WHERE id IN (1, 1000, 2000) ORDER BY id;

SELECT COUNT(*) AS correct_length
FROM noxu_array_float8_test WHERE array_length(measurements, 1) = 50;

DROP TABLE noxu_array_float8_test;


-- ============================================================
-- Test 5: Text arrays (Dictionary/FSST within decomposed stream)
-- ============================================================
CREATE TABLE noxu_array_text_test (
    id int,
    tags text[]
) USING noxu;

-- Low-cardinality text arrays (good for dictionary encoding)
INSERT INTO noxu_array_text_test
SELECT i,
       ARRAY(SELECT (ARRAY['alpha','beta','gamma','delta','epsilon'])[1 + (j % 5)]
             FROM generate_series(1, 10) j)
FROM generate_series(1, 1000) i;

SELECT COUNT(*) FROM noxu_array_text_test;

SELECT id, tags[1], tags[5], tags[10]
FROM noxu_array_text_test WHERE id IN (1, 500, 1000) ORDER BY id;

SELECT COUNT(*) AS correct_length
FROM noxu_array_text_test WHERE array_length(tags, 1) = 10;

-- High-cardinality text arrays (FSST)
CREATE TABLE noxu_array_text_unique_test (
    id int,
    labels text[]
) USING noxu;

INSERT INTO noxu_array_text_unique_test
SELECT i,
       ARRAY(SELECT 'label_' || i || '_' || j FROM generate_series(1, 8) j)
FROM generate_series(1, 500) i;

SELECT COUNT(*) FROM noxu_array_text_unique_test;

SELECT id, labels[1], labels[8]
FROM noxu_array_text_unique_test WHERE id IN (1, 250, 500) ORDER BY id;

DROP TABLE noxu_array_text_unique_test;
DROP TABLE noxu_array_text_test;


-- ============================================================
-- Test 6: Variable-length arrays (different lengths per row)
-- ============================================================
CREATE TABLE noxu_array_varlen_test (
    id int,
    values int[]
) USING noxu;

-- Arrays ranging from 1 to 100 elements
INSERT INTO noxu_array_varlen_test
SELECT i,
       ARRAY(SELECT j FROM generate_series(1, 1 + (i % 100)) j)
FROM generate_series(1, 1000) i;

SELECT COUNT(*) FROM noxu_array_varlen_test;

-- Verify lengths differ
SELECT id, array_length(values, 1) AS arrlen
FROM noxu_array_varlen_test WHERE id IN (1, 50, 99, 100) ORDER BY id;

-- Verify all elements survived
SELECT id, values[1], values[array_length(values, 1)] AS last_elem
FROM noxu_array_varlen_test WHERE id IN (1, 50, 100) ORDER BY id;

DROP TABLE noxu_array_varlen_test;


-- ============================================================
-- Test 7: NULL handling
-- ============================================================

-- 7a: Entire array column is NULL
CREATE TABLE noxu_array_null_whole_test (
    id int,
    values int[]
) USING noxu;

INSERT INTO noxu_array_null_whole_test
SELECT i,
       CASE WHEN i % 3 = 0 THEN NULL ELSE ARRAY[i, i+1, i+2] END
FROM generate_series(1, 300) i;

SELECT COUNT(*) FROM noxu_array_null_whole_test WHERE values IS NULL;
SELECT COUNT(*) FROM noxu_array_null_whole_test WHERE values IS NOT NULL;

-- Verify non-NULL arrays are correct
SELECT id, values
FROM noxu_array_null_whole_test WHERE id IN (1, 2, 3, 4, 5, 6) ORDER BY id;

DROP TABLE noxu_array_null_whole_test;

-- 7b: NULL elements inside arrays
CREATE TABLE noxu_array_null_elem_test (
    id int,
    values int[]
) USING noxu;

INSERT INTO noxu_array_null_elem_test
SELECT i,
       ARRAY(SELECT CASE WHEN j % 5 = 0 THEN NULL ELSE i * 10 + j END
             FROM generate_series(1, 20) j)
FROM generate_series(1, 500) i;

SELECT COUNT(*) FROM noxu_array_null_elem_test;

-- Verify NULL elements survive round-trip
SELECT id, values[4], values[5], values[6], values[10], values[15], values[20]
FROM noxu_array_null_elem_test WHERE id IN (1, 250, 500) ORDER BY id;

-- 7c: All-NULL element arrays
INSERT INTO noxu_array_null_elem_test VALUES
    (501, ARRAY[NULL, NULL, NULL]::int[]);

SELECT id, values FROM noxu_array_null_elem_test WHERE id = 501;

DROP TABLE noxu_array_null_elem_test;


-- ============================================================
-- Test 8: Large arrays (10,000+ elements)
-- ============================================================
CREATE TABLE noxu_array_large_test (
    id int,
    values int[]
) USING noxu;

-- 10,000-element arrays
INSERT INTO noxu_array_large_test
SELECT i, ARRAY(SELECT j FROM generate_series(1, 10000) j)
FROM generate_series(1, 50) i;

SELECT COUNT(*) FROM noxu_array_large_test;

-- Verify length
SELECT COUNT(*) AS correct_length
FROM noxu_array_large_test WHERE array_length(values, 1) = 10000;

-- Verify boundary elements
SELECT id, values[1], values[5000], values[10000]
FROM noxu_array_large_test WHERE id IN (1, 25, 50) ORDER BY id;

-- Verify element sum (should be 10000 * 10001 / 2 = 50005000)
SELECT id,
       (SELECT SUM(v) FROM unnest(values) v) AS elem_sum
FROM noxu_array_large_test WHERE id = 1;

DROP TABLE noxu_array_large_test;


-- ============================================================
-- Test 9: pg_vector integration (768-dimensional float32[])
-- ============================================================
CREATE TABLE noxu_array_vector_test (
    id int,
    embedding float4[]
) USING noxu;

-- 768-dimensional embeddings (typical BERT/transformer output)
INSERT INTO noxu_array_vector_test
SELECT i,
       ARRAY(SELECT ((random() - 0.5) * 0.1)::float4
             FROM generate_series(1, 768) j)
FROM generate_series(1, 500) i;

SELECT COUNT(*) FROM noxu_array_vector_test;

-- Verify dimensionality preserved
SELECT COUNT(*) AS correct_dim
FROM noxu_array_vector_test WHERE array_length(embedding, 1) = 768;

-- Verify values are in expected range
SELECT id,
       round(embedding[1]::numeric, 4) AS first_elem,
       round(embedding[384]::numeric, 4) AS mid_elem,
       round(embedding[768]::numeric, 4) AS last_elem
FROM noxu_array_vector_test WHERE id IN (1, 250, 500) ORDER BY id;

-- Verify L2 norm is reasonable (sqrt(768 * var) where var ~ 0.000833)
SELECT id,
       round(sqrt((SELECT SUM(v::float8 * v::float8) FROM unnest(embedding) v))::numeric, 2) AS l2_norm
FROM noxu_array_vector_test WHERE id IN (1, 250, 500) ORDER BY id;

-- 1536-dimensional embeddings (OpenAI ada-002 size)
CREATE TABLE noxu_array_vector_1536_test (
    id int,
    embedding float4[]
) USING noxu;

INSERT INTO noxu_array_vector_1536_test
SELECT i,
       ARRAY(SELECT ((random() - 0.5) * 0.05)::float4
             FROM generate_series(1, 1536) j)
FROM generate_series(1, 200) i;

SELECT COUNT(*) FROM noxu_array_vector_1536_test;

SELECT COUNT(*) AS correct_dim
FROM noxu_array_vector_1536_test WHERE array_length(embedding, 1) = 1536;

DROP TABLE noxu_array_vector_1536_test;
DROP TABLE noxu_array_vector_test;


-- ============================================================
-- Test 10: Empty arrays and edge cases
-- ============================================================
CREATE TABLE noxu_array_edge_test (
    id int,
    values int[]
) USING noxu;

INSERT INTO noxu_array_edge_test VALUES
    (1, '{}'),           -- empty array
    (2, '{42}'),         -- single element
    (3, '{1, 2}'),       -- two elements
    (4, NULL);           -- NULL array

SELECT * FROM noxu_array_edge_test ORDER BY id;

DROP TABLE noxu_array_edge_test;


-- ============================================================
-- Test 11: UUID arrays (decomposed UUID elements)
-- ============================================================
CREATE TABLE noxu_array_uuid_test (
    id int,
    uuids uuid[]
) USING noxu;

INSERT INTO noxu_array_uuid_test
SELECT i,
       ARRAY(SELECT gen_random_uuid() FROM generate_series(1, 5) j)
FROM generate_series(1, 200) i;

SELECT COUNT(*) FROM noxu_array_uuid_test;

SELECT id, array_length(uuids, 1) AS arrlen
FROM noxu_array_uuid_test WHERE id IN (1, 100, 200) ORDER BY id;

-- Verify all UUIDs are distinct within each array
SELECT id, COUNT(DISTINCT u) AS distinct_count
FROM noxu_array_uuid_test, unnest(uuids) u
WHERE id IN (1, 100, 200)
GROUP BY id ORDER BY id;

DROP TABLE noxu_array_uuid_test;


-- ============================================================
-- Test 12: UPDATE and DELETE on decomposed arrays
-- ============================================================
CREATE TABLE noxu_array_update_test (
    id int,
    values int[]
) USING noxu;

INSERT INTO noxu_array_update_test
SELECT i, ARRAY[i, i*2, i*3, i*4, i*5]
FROM generate_series(1, 500) i;

-- Update: replace arrays
UPDATE noxu_array_update_test SET values = ARRAY[0, 0, 0, 0, 0] WHERE id <= 10;
SELECT id, values
FROM noxu_array_update_test WHERE id <= 15 ORDER BY id;

-- Delete some rows
DELETE FROM noxu_array_update_test WHERE id <= 5;
SELECT COUNT(*) FROM noxu_array_update_test;

-- Verify remaining data integrity
SELECT id, values
FROM noxu_array_update_test WHERE id IN (6, 7, 8, 9, 10, 11) ORDER BY id;

DROP TABLE noxu_array_update_test;


-- ============================================================
-- Test 13: Compression ratio measurement
-- ============================================================
CREATE TABLE noxu_array_ratio_test (
    id int,
    values int[]
) USING noxu;

INSERT INTO noxu_array_ratio_test
SELECT i, ARRAY(SELECT (i * 100 + j) FROM generate_series(1, 50) j)
FROM generate_series(1, 5000) i;

SELECT pg_relation_size('noxu_array_ratio_test') AS noxu_array_bytes;

-- Raw size: 5000 rows * (4 + 50*4) = 5000 * 204 = 1020000 bytes
-- Expected compression: 5-10x => noxu size should be 102000 - 204000 bytes
SELECT CASE
    WHEN pg_relation_size('noxu_array_ratio_test') > 0
    THEN round((5000.0 * 204) / pg_relation_size('noxu_array_ratio_test'), 1)
    ELSE 0
END AS int_array_compression_ratio;

DROP TABLE noxu_array_ratio_test;
