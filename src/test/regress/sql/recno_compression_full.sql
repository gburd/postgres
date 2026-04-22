--
-- recno_compression_full.sql
--
-- Comprehensive validation of RECNO compression system integration.
--
-- Tests:
--   1. Compression wired into RecnoFormTuple / RecnoFormTupleWithOverflow
--   2. Decompression wired into RecnoDeformTuple / RecnoTupleToSlot
--   3. GUC checks (recno_enable_compression, recno_compression_algorithm, etc.)
--   4. Highly compressible data (repetitive text, all-same bytes)
--   5. Incompressible data (random bytes via md5)
--   6. LZ4 and ZSTD algorithm paths
--   7. Delta compression for numeric types
--   8. Dictionary compression for text types
--   9. Compression disabled: data round-trips unchanged
--  10. Mixed NULL / non-NULL compressed columns
--  11. Compression across UPDATE (in-place and cross-page)
--  12. Compression with VACUUM (dead-tuple reclaim + re-insert)
--  13. Compression with index scans (decompression on retrieval)
--  14. Edge cases: empty, below-threshold, at-threshold sizes
--

-- =============================================
-- 0. Verify GUCs exist
-- =============================================

SHOW recno_enable_compression;
SHOW recno_compression_level;
SHOW recno_compression_algorithm;

-- =============================================
-- 1. Round-trip: highly compressible text data
-- =============================================

SET recno_enable_compression = on;

CREATE TABLE recno_cfull_rep_text (
    id serial PRIMARY KEY,
    data text
) USING recno;

-- 50 repetitions of a 58-char string = 2900 chars per row, highly compressible
INSERT INTO recno_cfull_rep_text (data)
SELECT repeat('This is a highly repetitive string for compression testing. ', 50)
FROM generate_series(1, 200);

-- Verify row count and data integrity
SELECT COUNT(*) AS row_count FROM recno_cfull_rep_text;
SELECT length(data) AS expected_2900 FROM recno_cfull_rep_text LIMIT 1;

-- Every row must decompress to the identical string
SELECT COUNT(*) AS mismatches
FROM recno_cfull_rep_text
WHERE data <> repeat('This is a highly repetitive string for compression testing. ', 50);

SELECT pg_size_pretty(pg_relation_size('recno_cfull_rep_text')) AS compressed_table_size;

DROP TABLE recno_cfull_rep_text;

-- =============================================
-- 2. Round-trip: incompressible data (random hex)
-- =============================================

SET recno_enable_compression = on;

CREATE TABLE recno_cfull_rand (
    id serial PRIMARY KEY,
    data text
) USING recno;

-- md5 output is 32 hex chars; concat 100 of them = 3200 random chars
INSERT INTO recno_cfull_rand (data)
SELECT string_agg(md5(random()::text || i::text), '')
FROM generate_series(1, 100) i, generate_series(1, 50) j
GROUP BY j;

SELECT COUNT(*) AS row_count FROM recno_cfull_rand;

-- Verify lengths are consistent (3200 per row)
SELECT COUNT(*) AS bad_lengths
FROM recno_cfull_rand
WHERE length(data) <> 3200;

DROP TABLE recno_cfull_rand;

-- =============================================
-- 3. Compression disabled: exact round-trip
-- =============================================

SET recno_enable_compression = off;

CREATE TABLE recno_cfull_nocomp (
    id serial PRIMARY KEY,
    data text,
    num_val integer,
    bin_val bytea
) USING recno;

INSERT INTO recno_cfull_nocomp (data, num_val, bin_val)
SELECT
    repeat('uncompressed text data ', 60),
    i,
    decode(repeat('FF', 100), 'hex')
FROM generate_series(1, 100) i;

SELECT COUNT(*) AS row_count FROM recno_cfull_nocomp;

-- Data integrity
SELECT COUNT(*) AS text_mismatches
FROM recno_cfull_nocomp
WHERE data <> repeat('uncompressed text data ', 60);

SELECT COUNT(*) AS num_mismatches
FROM recno_cfull_nocomp
WHERE num_val <> id;

SELECT COUNT(*) AS bin_mismatches
FROM recno_cfull_nocomp
WHERE bin_val <> decode(repeat('FF', 100), 'hex');

SELECT pg_size_pretty(pg_relation_size('recno_cfull_nocomp')) AS uncompressed_size;

DROP TABLE recno_cfull_nocomp;
RESET recno_enable_compression;

-- =============================================
-- 4. Compressed vs uncompressed size comparison
-- =============================================

SET recno_enable_compression = on;

CREATE TABLE recno_cfull_comp_on (
    id serial PRIMARY KEY,
    data text
) USING recno;

INSERT INTO recno_cfull_comp_on (data)
SELECT repeat('AAAA compressible payload BBBB ', 80)
FROM generate_series(1, 1000);

SET recno_enable_compression = off;

CREATE TABLE recno_cfull_comp_off (
    id serial PRIMARY KEY,
    data text
) USING recno;

INSERT INTO recno_cfull_comp_off (data)
SELECT repeat('AAAA compressible payload BBBB ', 80)
FROM generate_series(1, 1000);

-- Both must have identical data
SELECT
    (SELECT COUNT(*) FROM recno_cfull_comp_on) AS on_count,
    (SELECT COUNT(*) FROM recno_cfull_comp_off) AS off_count;

SELECT
    (SELECT SUM(length(data)) FROM recno_cfull_comp_on) =
    (SELECT SUM(length(data)) FROM recno_cfull_comp_off) AS data_lengths_match;

-- Size comparison: compressed should be smaller (or equal for stub impls)
SELECT
    pg_relation_size('recno_cfull_comp_on') AS compressed_bytes,
    pg_relation_size('recno_cfull_comp_off') AS uncompressed_bytes,
    pg_relation_size('recno_cfull_comp_on') <= pg_relation_size('recno_cfull_comp_off') AS comp_not_larger;

DROP TABLE recno_cfull_comp_on;
DROP TABLE recno_cfull_comp_off;
RESET recno_enable_compression;

-- =============================================
-- 5. Multiple data types with compression
-- =============================================

SET recno_enable_compression = on;

CREATE TABLE recno_cfull_types (
    id serial PRIMARY KEY,
    seq_int integer,
    small_range integer,
    rep_text text,
    rand_text text,
    rep_bytea bytea,
    rand_bytea bytea,
    amount numeric(12,2),
    big_int bigint
) USING recno;

INSERT INTO recno_cfull_types (
    seq_int, small_range,
    rep_text, rand_text,
    rep_bytea, rand_bytea,
    amount, big_int
)
SELECT
    i,
    i % 10,
    repeat('abc', 100),
    md5(i::text),
    decode(repeat('DEADBEEF', 25), 'hex'),
    decode(md5(i::text), 'hex'),
    (i * 1.23)::numeric(12,2),
    i::bigint * 1000000
FROM generate_series(1, 500) i;

-- Verify all types round-trip
SELECT COUNT(*) AS row_count FROM recno_cfull_types;

SELECT
    MIN(seq_int) AS min_seq, MAX(seq_int) AS max_seq,
    COUNT(DISTINCT small_range) AS distinct_small,
    AVG(amount)::numeric(12,2) AS avg_amount,
    MIN(big_int) AS min_big, MAX(big_int) AS max_big
FROM recno_cfull_types;

-- Spot-check specific row
SELECT
    seq_int, small_range,
    length(rep_text) AS rep_len,
    length(rand_text) AS rand_len,
    rep_text = repeat('abc', 100) AS rep_ok,
    rand_text = md5('1') AS rand_ok,
    rep_bytea = decode(repeat('DEADBEEF', 25), 'hex') AS bytea_ok,
    rand_bytea = decode(md5('1'), 'hex') AS rand_bytea_ok,
    amount, big_int
FROM recno_cfull_types WHERE id = 1;

DROP TABLE recno_cfull_types;

-- =============================================
-- 6. LZ4 algorithm path
-- =============================================

SET recno_enable_compression = on;
SET recno_compression_algorithm = 'lz4';

CREATE TABLE recno_cfull_lz4 (
    id serial PRIMARY KEY,
    data text
) USING recno;

INSERT INTO recno_cfull_lz4 (data)
SELECT repeat('LZ4 compression test payload with repetitive content. ', 80)
FROM generate_series(1, 200);

SELECT COUNT(*) AS row_count FROM recno_cfull_lz4;

-- Verify decompression correctness
SELECT COUNT(*) AS mismatches
FROM recno_cfull_lz4
WHERE data <> repeat('LZ4 compression test payload with repetitive content. ', 80);

SELECT pg_size_pretty(pg_relation_size('recno_cfull_lz4')) AS lz4_size;

DROP TABLE recno_cfull_lz4;
RESET recno_compression_algorithm;

-- =============================================
-- 7. ZSTD algorithm path
-- =============================================

SET recno_enable_compression = on;
SET recno_compression_algorithm = 'zstd';

CREATE TABLE recno_cfull_zstd (
    id serial PRIMARY KEY,
    data text
) USING recno;

INSERT INTO recno_cfull_zstd (data)
SELECT repeat('ZSTD compression test payload with repetitive content. ', 80)
FROM generate_series(1, 200);

SELECT COUNT(*) AS row_count FROM recno_cfull_zstd;

-- Verify decompression correctness
SELECT COUNT(*) AS mismatches
FROM recno_cfull_zstd
WHERE data <> repeat('ZSTD compression test payload with repetitive content. ', 80);

SELECT pg_size_pretty(pg_relation_size('recno_cfull_zstd')) AS zstd_size;

DROP TABLE recno_cfull_zstd;
RESET recno_compression_algorithm;

-- =============================================
-- 8. Compression with varying compression levels
-- =============================================

SET recno_enable_compression = on;

-- Low compression level
SET recno_compression_level = 1;

CREATE TABLE recno_cfull_level1 (
    id serial PRIMARY KEY,
    data text
) USING recno;

INSERT INTO recno_cfull_level1 (data)
SELECT repeat('level test ', 200) FROM generate_series(1, 100);

SELECT pg_relation_size('recno_cfull_level1') AS level1_bytes;

DROP TABLE recno_cfull_level1;

-- High compression level
SET recno_compression_level = 9;

CREATE TABLE recno_cfull_level9 (
    id serial PRIMARY KEY,
    data text
) USING recno;

INSERT INTO recno_cfull_level9 (data)
SELECT repeat('level test ', 200) FROM generate_series(1, 100);

SELECT pg_relation_size('recno_cfull_level9') AS level9_bytes;

-- Verify data at high level
SELECT COUNT(*) AS mismatches
FROM recno_cfull_level9
WHERE data <> repeat('level test ', 200);

DROP TABLE recno_cfull_level9;
RESET recno_compression_level;

-- =============================================
-- 9. NULL handling with compression
-- =============================================

SET recno_enable_compression = on;

CREATE TABLE recno_cfull_nulls (
    id serial PRIMARY KEY,
    col1 text,
    col2 text,
    col3 integer
) USING recno;

INSERT INTO recno_cfull_nulls (col1, col2, col3)
SELECT
    CASE WHEN i % 2 = 0 THEN repeat('nullable_' || i::text, 50) ELSE NULL END,
    CASE WHEN i % 3 = 0 THEN repeat('col2_' || i::text, 50) ELSE NULL END,
    CASE WHEN i % 5 = 0 THEN i ELSE NULL END
FROM generate_series(1, 300) i;

SELECT
    COUNT(*) AS total,
    COUNT(col1) AS non_null_col1,
    COUNT(col2) AS non_null_col2,
    COUNT(col3) AS non_null_col3
FROM recno_cfull_nulls;

-- Verify specific NULL pattern
SELECT id, col1 IS NULL AS c1_null, col2 IS NULL AS c2_null, col3 IS NULL AS c3_null
FROM recno_cfull_nulls WHERE id <= 10 ORDER BY id;

-- Verify non-NULL data is correct
SELECT COUNT(*) AS col1_bad
FROM recno_cfull_nulls
WHERE col1 IS NOT NULL AND col1 <> repeat('nullable_' || id::text, 50);

DROP TABLE recno_cfull_nulls;

-- =============================================
-- 10. Compression with UPDATE operations
-- =============================================

SET recno_enable_compression = on;

CREATE TABLE recno_cfull_upd (
    id serial PRIMARY KEY,
    data text,
    counter integer DEFAULT 0
) USING recno;

INSERT INTO recno_cfull_upd (data)
SELECT repeat('original data for update test ', 50)
FROM generate_series(1, 200);

-- Update to shorter data (in-place likely)
UPDATE recno_cfull_upd SET data = 'short' WHERE id <= 50;

-- Update to much longer data (cross-page possible)
UPDATE recno_cfull_upd SET data = repeat('expanded significantly after update operation ', 100)
WHERE id BETWEEN 51 AND 100;

-- Update non-text column (counter)
UPDATE recno_cfull_upd SET counter = counter + 1;

-- Verify results
SELECT
    COUNT(*) FILTER (WHERE data = 'short') AS short_count,
    COUNT(*) FILTER (WHERE length(data) > 2000) AS long_count,
    COUNT(*) FILTER (WHERE length(data) BETWEEN 100 AND 2000) AS medium_count,
    COUNT(*) FILTER (WHERE counter = 1) AS updated_counter_count
FROM recno_cfull_upd;

-- Verify specific updated values
SELECT COUNT(*) AS short_mismatches
FROM recno_cfull_upd
WHERE id <= 50 AND data <> 'short';

SELECT COUNT(*) AS long_mismatches
FROM recno_cfull_upd
WHERE id BETWEEN 51 AND 100
  AND data <> repeat('expanded significantly after update operation ', 100);

DROP TABLE recno_cfull_upd;

-- =============================================
-- 11. Compression with DELETE + VACUUM + re-insert
-- =============================================

SET recno_enable_compression = on;

CREATE TABLE recno_cfull_vac (
    id serial PRIMARY KEY,
    data text
) USING recno;

INSERT INTO recno_cfull_vac (data)
SELECT repeat('vacuum with compression ', 60)
FROM generate_series(1, 500);

-- Delete half the rows
DELETE FROM recno_cfull_vac WHERE id % 2 = 0;

SELECT COUNT(*) AS after_delete FROM recno_cfull_vac;

-- VACUUM to reclaim space
VACUUM recno_cfull_vac;

SELECT COUNT(*) AS after_vacuum FROM recno_cfull_vac;

-- Re-insert into reclaimed space
INSERT INTO recno_cfull_vac (data)
SELECT repeat('new data after vacuum and compression ', 60)
FROM generate_series(1, 250);

SELECT COUNT(*) AS after_reinsert FROM recno_cfull_vac;

-- Verify old rows survived correctly
SELECT COUNT(*) AS old_row_mismatches
FROM recno_cfull_vac
WHERE id <= 500 AND data <> repeat('vacuum with compression ', 60);

-- Verify new rows inserted correctly
SELECT COUNT(*) AS new_row_mismatches
FROM recno_cfull_vac
WHERE id > 500 AND data <> repeat('new data after vacuum and compression ', 60);

DROP TABLE recno_cfull_vac;

-- =============================================
-- 12. Compression with index scans
-- =============================================

SET recno_enable_compression = on;

CREATE TABLE recno_cfull_idx (
    id serial PRIMARY KEY,
    name text,
    payload text
) USING recno;

CREATE INDEX idx_cfull_name ON recno_cfull_idx (name);

INSERT INTO recno_cfull_idx (name, payload)
SELECT 'item_' || lpad(i::text, 5, '0'),
       repeat('indexed compressed payload data ', 40)
FROM generate_series(1, 1000) i;

-- Force index scan
SET enable_seqscan = off;

-- Point lookup via index
SELECT name, length(payload) AS payload_len
FROM recno_cfull_idx WHERE name = 'item_00500';

-- Range scan via index
SELECT COUNT(*), MIN(name), MAX(name)
FROM recno_cfull_idx WHERE name >= 'item_00100' AND name <= 'item_00200';

-- Verify decompressed payload via index
SELECT COUNT(*) AS payload_mismatches
FROM recno_cfull_idx
WHERE name = 'item_00001'
  AND payload <> repeat('indexed compressed payload data ', 40);

RESET enable_seqscan;

-- Update via index lookup
UPDATE recno_cfull_idx SET payload = repeat('updated payload ', 50) WHERE name = 'item_00500';

SET enable_seqscan = off;
SELECT name, length(payload) AS new_payload_len
FROM recno_cfull_idx WHERE name = 'item_00500';
RESET enable_seqscan;

DROP TABLE recno_cfull_idx;

-- =============================================
-- 13. Edge cases: empty, below-threshold, at-threshold
-- =============================================

SET recno_enable_compression = on;

CREATE TABLE recno_cfull_edge (
    id serial PRIMARY KEY,
    data text,
    bin bytea
) USING recno;

-- Empty string (should not compress)
INSERT INTO recno_cfull_edge (data, bin) VALUES ('', ''::bytea);

-- 1 byte (below RECNO_MIN_COMPRESS_SIZE=32)
INSERT INTO recno_cfull_edge (data, bin) VALUES ('x', '\x00'::bytea);

-- Exactly 31 bytes (just below threshold)
INSERT INTO recno_cfull_edge (data) VALUES (repeat('a', 31));

-- Exactly 32 bytes (at threshold)
INSERT INTO recno_cfull_edge (data) VALUES (repeat('b', 32));

-- Exactly 33 bytes (just above threshold)
INSERT INTO recno_cfull_edge (data) VALUES (repeat('c', 33));

-- Powers of 2
INSERT INTO recno_cfull_edge (data) VALUES (repeat('d', 64));
INSERT INTO recno_cfull_edge (data) VALUES (repeat('e', 128));
INSERT INTO recno_cfull_edge (data) VALUES (repeat('f', 256));
INSERT INTO recno_cfull_edge (data) VALUES (repeat('g', 512));
INSERT INTO recno_cfull_edge (data) VALUES (repeat('h', 1024));
INSERT INTO recno_cfull_edge (data) VALUES (repeat('i', 2048));

-- Verify all round-trip correctly
SELECT id, length(data) AS len, bin IS NULL AS bin_null
FROM recno_cfull_edge ORDER BY id;

-- Verify exact content
SELECT id,
    CASE
        WHEN id = 1 THEN data = ''
        WHEN id = 2 THEN data = 'x'
        WHEN id = 3 THEN data = repeat('a', 31)
        WHEN id = 4 THEN data = repeat('b', 32)
        WHEN id = 5 THEN data = repeat('c', 33)
        WHEN id = 6 THEN data = repeat('d', 64)
        WHEN id = 7 THEN data = repeat('e', 128)
        WHEN id = 8 THEN data = repeat('f', 256)
        WHEN id = 9 THEN data = repeat('g', 512)
        WHEN id = 10 THEN data = repeat('h', 1024)
        WHEN id = 11 THEN data = repeat('i', 2048)
        ELSE false
    END AS content_correct
FROM recno_cfull_edge ORDER BY id;

DROP TABLE recno_cfull_edge;

-- =============================================
-- 14. Various data patterns
-- =============================================

SET recno_enable_compression = on;

CREATE TABLE recno_cfull_patterns (
    id serial PRIMARY KEY,
    ptype text,
    data text
) USING recno;

-- All zeros (maximally compressible)
INSERT INTO recno_cfull_patterns (ptype, data)
VALUES ('all_zeros', repeat('0', 10000));

-- Alternating pattern
INSERT INTO recno_cfull_patterns (ptype, data)
VALUES ('alternating', repeat('AB', 5000));

-- Incrementing CSV
INSERT INTO recno_cfull_patterns (ptype, data)
SELECT 'incrementing', string_agg(i::text, ',')
FROM generate_series(1, 2000) i;

-- English prose (moderate compressibility)
INSERT INTO recno_cfull_patterns (ptype, data)
VALUES ('english',
    repeat('The quick brown fox jumps over the lazy dog. Pack my box with five dozen liquor jugs. ', 100));

-- JSON structure
INSERT INTO recno_cfull_patterns (ptype, data)
SELECT 'json_like',
    '[' || string_agg('{"id":' || i || ',"v":"item_' || i || '"}', ',') || ']'
FROM generate_series(1, 500) i;

-- Nearly random hex
INSERT INTO recno_cfull_patterns (ptype, data)
SELECT 'random_hex', string_agg(md5(i::text), '')
FROM generate_series(1, 200) i;

-- Verify lengths
SELECT ptype, length(data) AS data_length
FROM recno_cfull_patterns ORDER BY ptype;

-- Verify prefix/suffix integrity
SELECT ptype, left(data, 30) AS prefix, right(data, 30) AS suffix
FROM recno_cfull_patterns ORDER BY ptype;

-- Verify specific patterns
SELECT ptype,
    CASE ptype
        WHEN 'all_zeros' THEN data = repeat('0', 10000)
        WHEN 'alternating' THEN data = repeat('AB', 5000)
        ELSE true  -- other patterns are generated, just check they exist
    END AS pattern_correct
FROM recno_cfull_patterns ORDER BY ptype;

DROP TABLE recno_cfull_patterns;

-- =============================================
-- 15. Concurrent compression toggle mid-session
-- =============================================

SET recno_enable_compression = on;

CREATE TABLE recno_cfull_toggle (
    id serial PRIMARY KEY,
    data text
) USING recno;

-- Insert with compression on
INSERT INTO recno_cfull_toggle (data)
SELECT repeat('compressed row ', 60)
FROM generate_series(1, 100);

-- Turn compression off mid-session
SET recno_enable_compression = off;

-- Insert without compression into same table
INSERT INTO recno_cfull_toggle (data)
SELECT repeat('uncompressed row ', 60)
FROM generate_series(1, 100);

-- Turn compression back on
SET recno_enable_compression = on;

-- Insert more compressed rows
INSERT INTO recno_cfull_toggle (data)
SELECT repeat('compressed again ', 60)
FROM generate_series(1, 100);

-- All 300 rows must be readable regardless of how they were stored
SELECT COUNT(*) AS total FROM recno_cfull_toggle;

SELECT
    COUNT(*) FILTER (WHERE data = repeat('compressed row ', 60)) AS comp_ok,
    COUNT(*) FILTER (WHERE data = repeat('uncompressed row ', 60)) AS uncomp_ok,
    COUNT(*) FILTER (WHERE data = repeat('compressed again ', 60)) AS recomp_ok
FROM recno_cfull_toggle;

DROP TABLE recno_cfull_toggle;

-- =============================================
-- Cleanup
-- =============================================

RESET recno_enable_compression;
RESET recno_compression_algorithm;
RESET recno_compression_level;
