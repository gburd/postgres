--
-- Test RECNO compression: various algorithms, data patterns, edge cases
--

-- =============================================
-- Basic compression toggle
-- =============================================

-- Verify GUC exists and defaults
SHOW recno_enable_compression;

-- Create table with compression enabled
SET recno_enable_compression = on;

CREATE TABLE recno_comp_basic (
    id serial PRIMARY KEY,
    data text
) USING recno;

-- Insert compressible data
INSERT INTO recno_comp_basic (data)
SELECT repeat('This is a highly repetitive string for compression testing. ', 50)
FROM generate_series(1, 100);

-- Verify data integrity
SELECT COUNT(*) FROM recno_comp_basic;
SELECT length(data) AS data_length FROM recno_comp_basic LIMIT 1;

-- Check table size
SELECT pg_size_pretty(pg_relation_size('recno_comp_basic')) AS compressed_size;

DROP TABLE recno_comp_basic;

-- =============================================
-- Compression with different data types
-- =============================================

SET recno_enable_compression = on;

CREATE TABLE recno_comp_types (
    id serial PRIMARY KEY,
    -- Numeric types (delta encoding should work well) USING recno;
    sequential_int integer,
    small_range_int integer,
    -- Text types
    repetitive_text text,
    random_text text,
    -- Binary types
    repetitive_bytea bytea,
    random_bytea bytea,
    -- Numeric with decimal
    amount numeric(12,2)
) USING recno;

INSERT INTO recno_comp_types (
    sequential_int, small_range_int,
    repetitive_text, random_text,
    repetitive_bytea, random_bytea,
    amount
)
SELECT
    i,                                    -- Sequential: compresses well with delta
    i % 10,                               -- Small range: very compressible
    repeat('abc', 100),                   -- Repetitive text: very compressible
    md5(i::text),                         -- Random text: less compressible
    decode(repeat('DEADBEEF', 25), 'hex'),-- Repetitive binary: compressible
    decode(md5(i::text), 'hex'),          -- Random binary: less compressible
    (i * 1.23)::numeric(12,2)            -- Decimal amounts
FROM generate_series(1, 1000) i;

-- Verify data integrity for each type
SELECT
    COUNT(*) AS total,
    MIN(sequential_int) AS min_seq,
    MAX(sequential_int) AS max_seq,
    COUNT(DISTINCT small_range_int) AS distinct_small,
    AVG(amount)::numeric(12,2) AS avg_amount
FROM recno_comp_types;

-- Verify text data is fully retrievable
SELECT id, length(repetitive_text) AS rep_len, length(random_text) AS rand_len
FROM recno_comp_types WHERE id = 1;

-- Verify binary data round-trips correctly
SELECT id,
    repetitive_bytea = decode(repeat('DEADBEEF', 25), 'hex') AS bytea_matches,
    random_bytea = decode(md5('1'), 'hex') AS rand_bytea_matches
FROM recno_comp_types WHERE id = 1;

DROP TABLE recno_comp_types;

-- =============================================
-- Compression vs. uncompressed comparison
-- =============================================

SET recno_enable_compression = on;

CREATE TABLE recno_comp_on (
    id serial PRIMARY KEY,
    value integer,
    data text
) USING recno;

INSERT INTO recno_comp_on (value, data)
SELECT i, repeat('compressible data pattern ', 40)
FROM generate_series(1, 2000) i;

SELECT pg_size_pretty(pg_relation_size('recno_comp_on')) AS compressed_size;

SET recno_enable_compression = off;

CREATE TABLE recno_comp_off (
    id serial PRIMARY KEY,
    value integer,
    data text
) USING recno;

INSERT INTO recno_comp_off (value, data)
SELECT i, repeat('compressible data pattern ', 40)
FROM generate_series(1, 2000) i;

SELECT pg_size_pretty(pg_relation_size('recno_comp_off')) AS uncompressed_size;

-- Verify identical data
SELECT
    (SELECT COUNT(*) FROM recno_comp_on) = (SELECT COUNT(*) FROM recno_comp_off) AS counts_match,
    (SELECT SUM(value) FROM recno_comp_on) = (SELECT SUM(value) FROM recno_comp_off) AS sums_match;

DROP TABLE recno_comp_on;
DROP TABLE recno_comp_off;

RESET recno_enable_compression;

-- =============================================
-- Compression with various data patterns
-- =============================================

SET recno_enable_compression = on;

CREATE TABLE recno_comp_patterns (
    id serial PRIMARY KEY,
    pattern_type text,
    data text
) USING recno;

-- All zeros / all same character
INSERT INTO recno_comp_patterns (pattern_type, data)
VALUES ('all_zeros', repeat('0', 10000));

-- Incrementing numbers
INSERT INTO recno_comp_patterns (pattern_type, data)
SELECT 'incrementing',
    string_agg(i::text, ',')
FROM generate_series(1, 2000) i;

-- Alternating pattern
INSERT INTO recno_comp_patterns (pattern_type, data)
VALUES ('alternating', repeat('ABABABABAB', 1000));

-- English text (moderate compressibility)
INSERT INTO recno_comp_patterns (pattern_type, data)
VALUES ('english',
    repeat('The quick brown fox jumps over the lazy dog. Pack my box with five dozen liquor jugs. ', 100));

-- JSON-like structure (moderate compressibility)
INSERT INTO recno_comp_patterns (pattern_type, data)
SELECT 'json_like',
    '[' || string_agg('{"id": ' || i || ', "value": "item_' || i || '"}', ', ') || ']'
FROM generate_series(1, 500) i;

-- Nearly incompressible (random hex)
INSERT INTO recno_comp_patterns (pattern_type, data)
SELECT 'random',
    string_agg(md5(i::text), '')
FROM generate_series(1, 300) i;

-- Verify all patterns stored and retrieved correctly
SELECT pattern_type, length(data) AS data_length
FROM recno_comp_patterns ORDER BY pattern_type;

-- Verify specific pattern integrity
SELECT pattern_type, left(data, 20) AS prefix, right(data, 20) AS suffix
FROM recno_comp_patterns ORDER BY pattern_type;

DROP TABLE recno_comp_patterns;

-- =============================================
-- Compression with updates
-- =============================================

SET recno_enable_compression = on;

CREATE TABLE recno_comp_update (
    id serial PRIMARY KEY,
    data text,
    counter integer DEFAULT 0
) USING recno;

INSERT INTO recno_comp_update (data)
SELECT repeat('updateable data ', 50)
FROM generate_series(1, 200);

-- Update to shorter data
UPDATE recno_comp_update SET data = 'short' WHERE id <= 50;

-- Update to longer data
UPDATE recno_comp_update SET data = repeat('expanded after update ', 100) WHERE id BETWEEN 51 AND 100;

-- In-place update (same size, different content)
UPDATE recno_comp_update SET counter = counter + 1;

-- Verify all data is correct
SELECT
    COUNT(*) FILTER (WHERE data = 'short') AS short_count,
    COUNT(*) FILTER (WHERE length(data) > 1000) AS long_count,
    COUNT(*) FILTER (WHERE counter = 1) AS updated_count
FROM recno_comp_update;

DROP TABLE recno_comp_update;

-- =============================================
-- Compression with NULL values
-- =============================================

SET recno_enable_compression = on;

CREATE TABLE recno_comp_nulls (
    id serial PRIMARY KEY,
    col1 text,
    col2 text,
    col3 text
) USING recno;

-- Mix of NULL and non-NULL values
INSERT INTO recno_comp_nulls (col1, col2, col3)
SELECT
    CASE WHEN i % 2 = 0 THEN repeat('data_' || i::text, 50) ELSE NULL END,
    CASE WHEN i % 3 = 0 THEN repeat('col2_' || i::text, 50) ELSE NULL END,
    CASE WHEN i % 5 = 0 THEN repeat('col3_' || i::text, 50) ELSE NULL END
FROM generate_series(1, 300) i;

-- Verify NULL handling
SELECT
    COUNT(*) AS total,
    COUNT(col1) AS non_null_col1,
    COUNT(col2) AS non_null_col2,
    COUNT(col3) AS non_null_col3
FROM recno_comp_nulls;

-- Retrieve specific rows with NULLs
SELECT id, col1 IS NULL AS c1_null, col2 IS NULL AS c2_null, col3 IS NULL AS c3_null
FROM recno_comp_nulls WHERE id <= 10 ORDER BY id;

DROP TABLE recno_comp_nulls;

-- =============================================
-- Compression edge cases
-- =============================================

SET recno_enable_compression = on;

CREATE TABLE recno_comp_edge (
    id serial PRIMARY KEY,
    data text,
    data_bytea bytea
) USING recno;

-- Empty strings
INSERT INTO recno_comp_edge (data, data_bytea) VALUES ('', ''::bytea);

-- Very short strings (should not compress)
INSERT INTO recno_comp_edge (data, data_bytea) VALUES ('x', 'x'::bytea);

-- Exactly at threshold boundaries
INSERT INTO recno_comp_edge (data) VALUES (repeat('a', 64));
INSERT INTO recno_comp_edge (data) VALUES (repeat('a', 128));
INSERT INTO recno_comp_edge (data) VALUES (repeat('a', 256));
INSERT INTO recno_comp_edge (data) VALUES (repeat('a', 1024));
INSERT INTO recno_comp_edge (data) VALUES (repeat('a', 2048));

-- Verify all edge cases retrieve correctly
SELECT id, length(data) AS len, data_bytea IS NULL AS bytea_null
FROM recno_comp_edge ORDER BY id;

DROP TABLE recno_comp_edge;

-- =============================================
-- Compression with VACUUM
-- =============================================

SET recno_enable_compression = on;

CREATE TABLE recno_comp_vacuum (
    id serial PRIMARY KEY,
    data text
) USING recno;

INSERT INTO recno_comp_vacuum (data)
SELECT repeat('vacuum test data ', 40) FROM generate_series(1, 500);

-- Delete and vacuum
DELETE FROM recno_comp_vacuum WHERE id % 2 = 0;
VACUUM recno_comp_vacuum;

-- Verify surviving rows
SELECT COUNT(*), MIN(id), MAX(id) FROM recno_comp_vacuum;

-- Insert new rows into reclaimed space
INSERT INTO recno_comp_vacuum (data)
SELECT repeat('new data after vacuum ', 40) FROM generate_series(1, 250);

SELECT COUNT(*) FROM recno_comp_vacuum;

DROP TABLE recno_comp_vacuum;

-- =============================================
-- Compression with indexes
-- =============================================

SET recno_enable_compression = on;

CREATE TABLE recno_comp_idx (
    id serial PRIMARY KEY,
    name text,
    data text
) USING recno;

CREATE INDEX idx_comp_name ON recno_comp_idx (name);

INSERT INTO recno_comp_idx (name, data)
SELECT 'item_' || i, repeat('indexed compressed data ', 30)
FROM generate_series(1, 1000) i;

-- Verify index works with compressed data
SET enable_seqscan = off;
SELECT name, length(data) FROM recno_comp_idx WHERE name = 'item_500';
RESET enable_seqscan;

-- Update via index scan
UPDATE recno_comp_idx SET data = repeat('updated ', 50) WHERE name = 'item_500';

SET enable_seqscan = off;
SELECT name, length(data) FROM recno_comp_idx WHERE name = 'item_500';
RESET enable_seqscan;

DROP TABLE recno_comp_idx;

-- =============================================
-- Compression algorithm selection
-- =============================================

-- Test LZ4 if available
SET recno_compression_algorithm = 'lz4';

CREATE TABLE recno_comp_lz4 (
    id serial PRIMARY KEY,
    data text
) USING recno;

INSERT INTO recno_comp_lz4 (data)
SELECT repeat('lz4 compression test data ', 100)
FROM generate_series(1, 100);

SELECT COUNT(*), MIN(length(data)), MAX(length(data))
FROM recno_comp_lz4;

SELECT pg_size_pretty(pg_relation_size('recno_comp_lz4')) AS lz4_size;

DROP TABLE recno_comp_lz4;

-- Test ZSTD if available
SET recno_compression_algorithm = 'zstd';

CREATE TABLE recno_comp_zstd (
    id serial PRIMARY KEY,
    data text
) USING recno;

INSERT INTO recno_comp_zstd (data)
SELECT repeat('zstd compression test data ', 100)
FROM generate_series(1, 100);

SELECT COUNT(*), MIN(length(data)), MAX(length(data))
FROM recno_comp_zstd;

SELECT pg_size_pretty(pg_relation_size('recno_comp_zstd')) AS zstd_size;

DROP TABLE recno_comp_zstd;

-- =============================================
-- Trained dictionary compression (build_zstd_dict_for_attribute)
--
-- Once a dictionary is active, new tuples carry a non-zero dict_id in
-- their compression header.  Deforming such a tuple requires the owning
-- relation's OID so the trained blob can be located; the indexed-UPDATE
-- path must supply it or the old-tuple deform throws DATA_CORRUPTED.
-- =============================================

SET recno_enable_compression = on;
SET recno_compression_algorithm = 'zstd';

CREATE TABLE recno_comp_dict (
    id serial PRIMARY KEY,
    name text,
    payload text
) USING recno;

CREATE INDEX idx_comp_dict_name ON recno_comp_dict (name);

-- Seed with compressible payloads (plain codec, dict_id 0)
INSERT INTO recno_comp_dict (name, payload)
SELECT 'row_' || i,
       repeat('dictionary training corpus value ' || (i % 20)::text || ' ', 20)
FROM generate_series(1, 500) i;

-- Train and activate a dictionary over the payload column.
-- First dictionary id is 1 (monotonic, starts at 1).
SELECT build_zstd_dict_for_attribute('recno_comp_dict'::regclass, 3) AS dictid;

-- New rows are now dict-compressed (dict_id = 1).
INSERT INTO recno_comp_dict (name, payload)
SELECT 'row_' || i,
       repeat('dictionary training corpus value ' || (i % 20)::text || ' ', 20)
FROM generate_series(501, 700) i;

-- Index-driven UPDATE deforms the dict-compressed OLD tuple to compute the
-- index-update set.  This is the path that needs relation context.
SET enable_seqscan = off;
UPDATE recno_comp_dict SET name = 'updated_600' WHERE name = 'row_600';
SELECT id, payload = repeat('dictionary training corpus value ' || (600 % 20)::text || ' ', 20)
           AS payload_intact
FROM recno_comp_dict WHERE name = 'updated_600';
RESET enable_seqscan;

-- Full round-trip integrity across both plain- and dict-compressed rows.
SELECT COUNT(*) AS total,
       COUNT(*) FILTER (
           WHERE payload = repeat('dictionary training corpus value '
                                  || (id % 20)::text || ' ', 20)) AS intact
FROM recno_comp_dict
WHERE id <> 600;

DROP TABLE recno_comp_dict;

-- =============================================
-- ANALYZE-driven dictionary refresh (recno_analyze_refresh_dict)
--
-- ANALYZE reuses its own sampled rows to train a candidate ZSTD dictionary
-- over the first varlena column and activates it only when it compresses the
-- sample materially better than the current dictionary.  Existing rows are
-- never rewritten; both pre- and post-refresh rows must round-trip.
-- =============================================

SET recno_enable_compression = on;
SET recno_compression_algorithm = 'zstd';
SHOW recno_analyze_refresh_dict;

CREATE TABLE recno_comp_analyze (
    id serial PRIMARY KEY,
    payload text
) USING recno;

-- Seed a highly self-similar corpus so a trained dict beats the plain codec.
INSERT INTO recno_comp_analyze (payload)
SELECT repeat('analyze refresh corpus token ' || (i % 16)::text || ' ', 24)
FROM generate_series(1, 1000) i;

-- ANALYZE trains+activates a dictionary from the rows it already sampled.
ANALYZE recno_comp_analyze;

-- New rows now use whatever dictionary ANALYZE activated; old rows still read.
INSERT INTO recno_comp_analyze (payload)
SELECT repeat('analyze refresh corpus token ' || (i % 16)::text || ' ', 24)
FROM generate_series(1001, 1200) i;

-- Full round-trip integrity across pre- and post-ANALYZE rows.
SELECT COUNT(*) AS total,
       COUNT(*) FILTER (
           WHERE payload = repeat('analyze refresh corpus token '
                                  || (id % 16)::text || ' ', 24)) AS intact
FROM recno_comp_analyze;

-- With refresh disabled, ANALYZE must not change compression behavior.
SET recno_analyze_refresh_dict = off;
ANALYZE recno_comp_analyze;
INSERT INTO recno_comp_analyze (payload)
SELECT repeat('analyze refresh corpus token ' || (i % 16)::text || ' ', 24)
FROM generate_series(1201, 1300) i;
SELECT COUNT(*) AS total_after_disable,
       COUNT(*) FILTER (
           WHERE payload = repeat('analyze refresh corpus token '
                                  || (id % 16)::text || ' ', 24)) AS intact
FROM recno_comp_analyze;

DROP TABLE recno_comp_analyze;
RESET recno_analyze_refresh_dict;

-- Reset to defaults
RESET recno_compression_algorithm;
RESET recno_enable_compression;
