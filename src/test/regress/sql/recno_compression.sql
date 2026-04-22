--
-- Test RECNO compression: various algorithms, data patterns, edge cases
--

-- =============================================
-- Basic compression toggle
-- =============================================

-- Verify GUC exists and defaults

-- Create table with compression enabled

CREATE TABLE recno_comp_basic (
    id serial PRIMARY KEY,
    data text
) USING recno WITH (compression='auto');

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
) USING recno WITH (compression='auto');

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


CREATE TABLE recno_comp_on (
    id serial PRIMARY KEY,
    value integer,
    data text
) USING recno WITH (compression='auto');

INSERT INTO recno_comp_on (value, data)
SELECT i, repeat('compressible data pattern ', 40)
FROM generate_series(1, 2000) i;

SELECT pg_size_pretty(pg_relation_size('recno_comp_on')) AS compressed_size;


CREATE TABLE recno_comp_off (
    id serial PRIMARY KEY,
    value integer,
    data text
) USING recno WITH (compression='off');

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


-- =============================================
-- Compression with various data patterns
-- =============================================


CREATE TABLE recno_comp_patterns (
    id serial PRIMARY KEY,
    pattern_type text,
    data text
) USING recno WITH (compression='auto');

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


CREATE TABLE recno_comp_update (
    id serial PRIMARY KEY,
    data text,
    counter integer DEFAULT 0
) USING recno WITH (compression='auto');

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


CREATE TABLE recno_comp_nulls (
    id serial PRIMARY KEY,
    col1 text,
    col2 text,
    col3 text
) USING recno WITH (compression='auto');

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


CREATE TABLE recno_comp_edge (
    id serial PRIMARY KEY,
    data text,
    data_bytea bytea
) USING recno WITH (compression='auto');

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


CREATE TABLE recno_comp_vacuum (
    id serial PRIMARY KEY,
    data text
) USING recno WITH (compression='auto');

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


CREATE TABLE recno_comp_idx (
    id serial PRIMARY KEY,
    name text,
    data text
) USING recno WITH (compression='auto');

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

CREATE TABLE recno_comp_lz4 (
    id serial PRIMARY KEY,
    data text
) USING recno WITH (compression='lz4');

INSERT INTO recno_comp_lz4 (data)
SELECT repeat('lz4 compression test data ', 100)
FROM generate_series(1, 100);

SELECT COUNT(*), MIN(length(data)), MAX(length(data))
FROM recno_comp_lz4;

SELECT pg_size_pretty(pg_relation_size('recno_comp_lz4')) AS lz4_size;

DROP TABLE recno_comp_lz4;

-- Test ZSTD if available

CREATE TABLE recno_comp_zstd (
    id serial PRIMARY KEY,
    data text
) USING recno WITH (compression='zstd');

INSERT INTO recno_comp_zstd (data)
SELECT repeat('zstd compression test data ', 100)
FROM generate_series(1, 100);

SELECT COUNT(*), MIN(length(data)), MAX(length(data))
FROM recno_comp_zstd;

SELECT pg_size_pretty(pg_relation_size('recno_comp_zstd')) AS zstd_size;

DROP TABLE recno_comp_zstd;

-- Reset to defaults
