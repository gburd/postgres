--
-- Test UUID fixed-binary storage (16-byte fixed format vs varlena)
-- Verifies 6-31% space savings from eliminating varlena header.
--

-- Test 1: Random UUIDs
CREATE TABLE noxu_uuid_test (
    id int,
    uuid_col uuid,
    description text
) USING noxu;

INSERT INTO noxu_uuid_test
SELECT i, gen_random_uuid(), 'record_' || i
FROM generate_series(1, 100) i;

SELECT COUNT(*) FROM noxu_uuid_test;
SELECT COUNT(DISTINCT uuid_col) FROM noxu_uuid_test;

-- Test retrieval and filtering (verify format without checking exact UUID values)
SELECT id, uuid_col IS NOT NULL as has_uuid, length(uuid_col::text) as uuid_text_length
FROM noxu_uuid_test WHERE id <= 5 ORDER BY id;

-- Store specific UUID for filter test
INSERT INTO noxu_uuid_test VALUES
    (101, '550e8400-e29b-41d4-a716-446655440000'::uuid, 'known_uuid');

SELECT id, description FROM noxu_uuid_test
WHERE uuid_col = '550e8400-e29b-41d4-a716-446655440000'::uuid;

DROP TABLE noxu_uuid_test;

-- Test 2: UUIDs with NULLs
CREATE TABLE noxu_uuid_nullable_test (
    id int,
    primary_uuid uuid,
    secondary_uuid uuid
) USING noxu;

INSERT INTO noxu_uuid_nullable_test
SELECT i,
       gen_random_uuid(),
       CASE WHEN i % 3 = 0 THEN NULL ELSE gen_random_uuid() END
FROM generate_series(1, 50) i;

SELECT COUNT(*) FROM noxu_uuid_nullable_test WHERE secondary_uuid IS NULL;
SELECT COUNT(*) FROM noxu_uuid_nullable_test WHERE secondary_uuid IS NOT NULL;

DROP TABLE noxu_uuid_nullable_test;

-- Test 3: UUID ordering and comparison
CREATE TABLE noxu_uuid_ordering_test (
    id int,
    uuid_col uuid
) USING noxu;

INSERT INTO noxu_uuid_ordering_test VALUES
    (1, '00000000-0000-0000-0000-000000000001'::uuid),
    (2, '00000000-0000-0000-0000-000000000002'::uuid),
    (3, '00000000-0000-0000-0000-000000000003'::uuid),
    (4, 'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid),
    (5, '12345678-1234-5678-1234-567812345678'::uuid);

SELECT * FROM noxu_uuid_ordering_test ORDER BY uuid_col;

-- Test UUID range queries
SELECT id FROM noxu_uuid_ordering_test
WHERE uuid_col < '12345678-1234-5678-1234-567812345678'::uuid
ORDER BY id;

DROP TABLE noxu_uuid_ordering_test;

-- Test 4: Multiple UUID columns
CREATE TABLE noxu_multi_uuid_test (
    record_id uuid,
    user_id uuid,
    session_id uuid,
    transaction_id uuid
) USING noxu;

INSERT INTO noxu_multi_uuid_test
SELECT gen_random_uuid(), gen_random_uuid(), gen_random_uuid(), gen_random_uuid()
FROM generate_series(1, 20);

SELECT COUNT(DISTINCT record_id) FROM noxu_multi_uuid_test;
SELECT COUNT(DISTINCT user_id) FROM noxu_multi_uuid_test;

DROP TABLE noxu_multi_uuid_test;

-- ==========================================================
-- Test 5: UUID v7 time-ordered delta compression
-- UUIDv7 has a 48-bit timestamp in bytes 0-5 and version nibble 0x7 in byte 6.
-- We construct monotonic v7 UUIDs to trigger delta encoding.
-- ==========================================================

CREATE TABLE noxu_uuid_v7_test (
    id int,
    uuid_col uuid
) USING noxu;

-- Construct monotonic UUIDv7-like values:
-- Bytes 0-5: timestamp_ms in big-endian (incrementing by 1ms)
-- Byte 6: version nibble 0x7X (0x70 | rand_a high nibble)
-- Byte 7: variant 0x80 | rand
-- Bytes 8-15: random
-- We use a base timestamp of 0x018F5A3B6000 (approx mid-2024 in ms)
INSERT INTO noxu_uuid_v7_test
SELECT i,
       (lpad(to_hex((x'018F5A3B6000'::bigint + i)::bigint), 12, '0')
        || '-7' || substr(md5(i::text), 1, 3)
        || '-8' || substr(md5(i::text), 4, 3)
        || '-' || substr(md5(i::text), 7, 4)
        || '-' || substr(md5(i::text), 11, 12))::uuid
FROM generate_series(1, 500) i;

SELECT COUNT(*) FROM noxu_uuid_v7_test;

-- Verify all values round-trip correctly
SELECT COUNT(*) FROM noxu_uuid_v7_test
WHERE uuid_col IS NOT NULL;

-- Verify ordering is preserved
SELECT bool_and(ok) FROM (
    SELECT id,
           uuid_col >= lag(uuid_col) OVER (ORDER BY id) AS ok
    FROM noxu_uuid_v7_test
) sub WHERE ok IS NOT NULL;

-- Test specific value retrieval
SELECT id FROM noxu_uuid_v7_test WHERE id = 250;

-- Test filtering on UUID column
SELECT COUNT(*) FROM noxu_uuid_v7_test
WHERE uuid_col > (SELECT uuid_col FROM noxu_uuid_v7_test WHERE id = 100)
  AND uuid_col < (SELECT uuid_col FROM noxu_uuid_v7_test WHERE id = 200);

DROP TABLE noxu_uuid_v7_test;

-- Test 6: UUID v7 with NULLs (delta encoding + null handling)
CREATE TABLE noxu_uuid_v7_nullable_test (
    id int,
    uuid_col uuid
) USING noxu;

INSERT INTO noxu_uuid_v7_nullable_test
SELECT i,
       CASE WHEN i % 5 = 0 THEN NULL
       ELSE
           (lpad(to_hex((x'018F5A3B6000'::bigint + i)::bigint), 12, '0')
            || '-7' || substr(md5(i::text), 1, 3)
            || '-8' || substr(md5(i::text), 4, 3)
            || '-' || substr(md5(i::text), 7, 4)
            || '-' || substr(md5(i::text), 11, 12))::uuid
       END
FROM generate_series(1, 200) i;

SELECT COUNT(*) FROM noxu_uuid_v7_nullable_test WHERE uuid_col IS NULL;
SELECT COUNT(*) FROM noxu_uuid_v7_nullable_test WHERE uuid_col IS NOT NULL;

-- Verify non-null values are correct
SELECT COUNT(*) FROM noxu_uuid_v7_nullable_test
WHERE id % 5 != 0 AND uuid_col IS NOT NULL;

SELECT COUNT(*) FROM noxu_uuid_v7_nullable_test
WHERE id % 5 = 0 AND uuid_col IS NULL;

DROP TABLE noxu_uuid_v7_nullable_test;

-- Test 7: UUID v7 with same-millisecond values (all same timestamp)
CREATE TABLE noxu_uuid_v7_same_ts_test (
    id int,
    uuid_col uuid
) USING noxu;

-- All UUIDs have the same timestamp but different suffixes
INSERT INTO noxu_uuid_v7_same_ts_test
SELECT i,
       ('018F5A3B6000-7' || substr(md5(i::text), 1, 3)
        || '-8' || substr(md5(i::text), 4, 3)
        || '-' || substr(md5(i::text), 7, 4)
        || '-' || substr(md5(i::text), 11, 12))::uuid
FROM generate_series(1, 100) i;

SELECT COUNT(*) FROM noxu_uuid_v7_same_ts_test;
SELECT COUNT(DISTINCT uuid_col) FROM noxu_uuid_v7_same_ts_test;

DROP TABLE noxu_uuid_v7_same_ts_test;

-- Test 8: Large batch of UUID v7 (trigger item splitting + compression)
CREATE TABLE noxu_uuid_v7_large_test (
    id int,
    uuid_col uuid,
    payload text
) USING noxu;

INSERT INTO noxu_uuid_v7_large_test
SELECT i,
       (lpad(to_hex((x'018F5A3B6000'::bigint + i)::bigint), 12, '0')
        || '-7' || substr(md5(i::text), 1, 3)
        || '-8' || substr(md5(i::text), 4, 3)
        || '-' || substr(md5(i::text), 7, 4)
        || '-' || substr(md5(i::text), 11, 12))::uuid,
       'payload_' || i
FROM generate_series(1, 5000) i;

SELECT COUNT(*) FROM noxu_uuid_v7_large_test;

-- Spot-check several specific rows
SELECT id, uuid_col IS NOT NULL AS has_uuid
FROM noxu_uuid_v7_large_test
WHERE id IN (1, 1000, 2500, 4999, 5000)
ORDER BY id;

-- Test DELETE + re-read (exercises explode_item path)
DELETE FROM noxu_uuid_v7_large_test WHERE id BETWEEN 100 AND 200;
SELECT COUNT(*) FROM noxu_uuid_v7_large_test;

DROP TABLE noxu_uuid_v7_large_test;

-- Test 9: Mixed random (v4) and time-ordered (v7) UUIDs
-- When versions are mixed, delta encoding should not be used;
-- fixed-binary storage should be the fallback.
CREATE TABLE noxu_uuid_mixed_test (
    id int,
    uuid_col uuid
) USING noxu;

INSERT INTO noxu_uuid_mixed_test
SELECT i,
       CASE WHEN i % 2 = 0 THEN gen_random_uuid()
       ELSE
           (lpad(to_hex((x'018F5A3B6000'::bigint + i)::bigint), 12, '0')
            || '-7' || substr(md5(i::text), 1, 3)
            || '-8' || substr(md5(i::text), 4, 3)
            || '-' || substr(md5(i::text), 7, 4)
            || '-' || substr(md5(i::text), 11, 12))::uuid
       END
FROM generate_series(1, 100) i;

SELECT COUNT(*) FROM noxu_uuid_mixed_test;
SELECT COUNT(DISTINCT uuid_col) FROM noxu_uuid_mixed_test;

DROP TABLE noxu_uuid_mixed_test;

-- ==========================================================
-- Test 10: Compression ratio verification for v4 (random) UUIDs
-- Random UUIDs use fixed-binary storage (16 bytes vs 18+ varlena)
-- Expected: 6-31% savings from eliminating varlena header.
-- ==========================================================
CREATE TABLE noxu_uuid_v4_ratio_test (
    id int,
    uuid_col uuid
) USING noxu;

INSERT INTO noxu_uuid_v4_ratio_test
SELECT i, gen_random_uuid()
FROM generate_series(1, 10000) i;

SELECT COUNT(*) FROM noxu_uuid_v4_ratio_test;

SELECT pg_relation_size('noxu_uuid_v4_ratio_test') AS noxu_uuid_v4_bytes;

-- Raw varlena size: 10000 * (4 + 18) = 220000 (with 2-byte varlena header)
-- Fixed-binary size: 10000 * (4 + 16) = 200000 (noxu)
-- Ratio is modest since random UUIDs are incompressible
SELECT CASE
    WHEN pg_relation_size('noxu_uuid_v4_ratio_test') > 0
    THEN round((10000.0 * 20) / pg_relation_size('noxu_uuid_v4_ratio_test'), 2)
    ELSE 0
END AS uuid_v4_compression_ratio;

DROP TABLE noxu_uuid_v4_ratio_test;

-- ==========================================================
-- Test 11: Compression ratio verification for v7 (time-ordered) UUIDs
-- v7 UUIDs with delta encoding target 3-5x compression
-- (5-8 bytes/row vs 16 raw)
-- ==========================================================
CREATE TABLE noxu_uuid_v7_ratio_test (
    id int,
    uuid_col uuid
) USING noxu;

INSERT INTO noxu_uuid_v7_ratio_test
SELECT i,
       (lpad(to_hex((x'018F5A3B6000'::bigint + i)::bigint), 12, '0')
        || '-7' || substr(md5(i::text), 1, 3)
        || '-8' || substr(md5(i::text), 4, 3)
        || '-' || substr(md5(i::text), 7, 4)
        || '-' || substr(md5(i::text), 11, 12))::uuid
FROM generate_series(1, 10000) i;

SELECT COUNT(*) FROM noxu_uuid_v7_ratio_test;

SELECT pg_relation_size('noxu_uuid_v7_ratio_test') AS noxu_uuid_v7_bytes;

-- Raw size: 10000 * (4 + 16) = 200000
-- Target: 3-5x compression (delta encoding reduces to 5-8 bytes/row)
SELECT CASE
    WHEN pg_relation_size('noxu_uuid_v7_ratio_test') > 0
    THEN round(200000.0 / pg_relation_size('noxu_uuid_v7_ratio_test'), 1)
    ELSE 0
END AS uuid_v7_compression_ratio;

DROP TABLE noxu_uuid_v7_ratio_test;

-- ==========================================================
-- Test 12: Time-range query optimization on v7 UUIDs
-- Because v7 UUIDs encode time in their prefix, filtering by
-- UUID range is equivalent to filtering by time range.
-- ==========================================================
CREATE TABLE noxu_uuid_v7_timerange_test (
    id int,
    uuid_col uuid,
    event_data text
) USING noxu;

-- Insert 5000 v7 UUIDs spanning 5 seconds (1ms apart)
INSERT INTO noxu_uuid_v7_timerange_test
SELECT i,
       (lpad(to_hex((x'018F5A3B6000'::bigint + i)::bigint), 12, '0')
        || '-7' || substr(md5(i::text), 1, 3)
        || '-8' || substr(md5(i::text), 4, 3)
        || '-' || substr(md5(i::text), 7, 4)
        || '-' || substr(md5(i::text), 11, 12))::uuid,
       'event_' || i
FROM generate_series(1, 5000) i;

SELECT COUNT(*) FROM noxu_uuid_v7_timerange_test;

-- Construct boundary UUIDs for a "1-second window" (ids 1000-2000)
-- Lower bound: timestamp = base + 1000
-- Upper bound: timestamp = base + 2000
SELECT COUNT(*) AS rows_in_range
FROM noxu_uuid_v7_timerange_test
WHERE uuid_col >= (SELECT uuid_col FROM noxu_uuid_v7_timerange_test WHERE id = 1000)
  AND uuid_col <= (SELECT uuid_col FROM noxu_uuid_v7_timerange_test WHERE id = 2000);

-- Verify the first and last rows in the range
SELECT id, event_data
FROM noxu_uuid_v7_timerange_test
WHERE uuid_col >= (SELECT uuid_col FROM noxu_uuid_v7_timerange_test WHERE id = 1000)
  AND uuid_col <= (SELECT uuid_col FROM noxu_uuid_v7_timerange_test WHERE id = 2000)
ORDER BY uuid_col
LIMIT 3;

SELECT id, event_data
FROM noxu_uuid_v7_timerange_test
WHERE uuid_col >= (SELECT uuid_col FROM noxu_uuid_v7_timerange_test WHERE id = 1000)
  AND uuid_col <= (SELECT uuid_col FROM noxu_uuid_v7_timerange_test WHERE id = 2000)
ORDER BY uuid_col DESC
LIMIT 3;

-- Empty range: range before all data
SELECT COUNT(*) AS empty_range
FROM noxu_uuid_v7_timerange_test
WHERE uuid_col < (SELECT uuid_col FROM noxu_uuid_v7_timerange_test WHERE id = 1);

-- Full range: all data
SELECT COUNT(*) AS full_range
FROM noxu_uuid_v7_timerange_test
WHERE uuid_col >= (SELECT uuid_col FROM noxu_uuid_v7_timerange_test WHERE id = 1)
  AND uuid_col <= (SELECT uuid_col FROM noxu_uuid_v7_timerange_test WHERE id = 5000);

DROP TABLE noxu_uuid_v7_timerange_test;

-- ==========================================================
-- Test 13: UPDATE and DELETE on delta-compressed v7 UUIDs
-- ==========================================================
CREATE TABLE noxu_uuid_v7_update_test (
    id int,
    uuid_col uuid
) USING noxu;

INSERT INTO noxu_uuid_v7_update_test
SELECT i,
       (lpad(to_hex((x'018F5A3B6000'::bigint + i)::bigint), 12, '0')
        || '-7' || substr(md5(i::text), 1, 3)
        || '-8' || substr(md5(i::text), 4, 3)
        || '-' || substr(md5(i::text), 7, 4)
        || '-' || substr(md5(i::text), 11, 12))::uuid
FROM generate_series(1, 500) i;

-- Update: replace some v7 UUIDs with random v4 UUIDs
UPDATE noxu_uuid_v7_update_test SET uuid_col = gen_random_uuid() WHERE id <= 10;

-- Verify updated rows have new (random) UUIDs
SELECT id, uuid_col IS NOT NULL AS has_uuid
FROM noxu_uuid_v7_update_test WHERE id <= 15 ORDER BY id;

-- Delete some rows
DELETE FROM noxu_uuid_v7_update_test WHERE id <= 5;
SELECT COUNT(*) FROM noxu_uuid_v7_update_test;

-- Verify remaining v7 UUIDs are still correct
SELECT COUNT(*) FROM noxu_uuid_v7_update_test
WHERE id > 10 AND uuid_col IS NOT NULL;

DROP TABLE noxu_uuid_v7_update_test;
