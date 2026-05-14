--
-- Test RECNO overflow: column-level overflow for large attributes
--

-- =============================================
-- Basic overflow with large text
-- =============================================

CREATE TABLE recno_ov_basic (
    id serial PRIMARY KEY,
    small_col text,
    large_col text
) USING recno;

-- Insert a row with data that should trigger overflow (>2KB per column)
INSERT INTO recno_ov_basic (small_col, large_col)
VALUES ('small', repeat('X', 10000));

-- Verify retrieval
SELECT id, small_col, length(large_col) AS large_len
FROM recno_ov_basic;

-- Verify exact content integrity (prefix and suffix)
SELECT
    left(large_col, 10) AS prefix,
    right(large_col, 10) AS suffix,
    large_col = repeat('X', 10000) AS content_matches
FROM recno_ov_basic WHERE id = 1;

DROP TABLE recno_ov_basic;

-- =============================================
-- Multiple overflow columns in one row
-- =============================================

CREATE TABLE recno_ov_multi (
    id serial PRIMARY KEY,
    col1 text,
    col2 text,
    col3 bytea,
    small_col integer
) USING recno;

-- All three varlena columns overflow
INSERT INTO recno_ov_multi (col1, col2, col3, small_col)
VALUES (
    repeat('A', 8000),
    repeat('B', 12000),
    decode(repeat('FF', 5000), 'hex'),
    42
);

-- Verify all columns are retrievable
SELECT
    id,
    length(col1) AS col1_len,
    length(col2) AS col2_len,
    length(col3) AS col3_len,
    small_col
FROM recno_ov_multi;

-- Verify content
SELECT
    col1 = repeat('A', 8000) AS col1_ok,
    col2 = repeat('B', 12000) AS col2_ok,
    col3 = decode(repeat('FF', 5000), 'hex') AS col3_ok,
    small_col = 42 AS small_ok
FROM recno_ov_multi WHERE id = 1;

DROP TABLE recno_ov_multi;

-- =============================================
-- Overflow with varying sizes
-- =============================================

CREATE TABLE recno_ov_sizes (
    id serial PRIMARY KEY,
    data text
) USING recno;

-- Insert data of various sizes around the overflow threshold
INSERT INTO recno_ov_sizes (data) VALUES
    (repeat('a', 100)),       -- Well below threshold, no overflow
    (repeat('b', 1000)),      -- Below threshold, no overflow
    (repeat('c', 2000)),      -- Near threshold
    (repeat('d', 4000)),      -- Above threshold, single overflow record likely
    (repeat('e', 8000)),      -- Well above threshold, needs chain
    (repeat('f', 16000)),     -- Multiple overflow records
    (repeat('g', 50000)),     -- Long chain
    (repeat('h', 80000));     -- Very long chain (within WAL segment limits)

-- Verify all sizes round-trip correctly
SELECT id, length(data) AS len,
    data = repeat(chr(ascii('a') + id - 1), length(data)) AS content_ok
FROM recno_ov_sizes ORDER BY id;

DROP TABLE recno_ov_sizes;

-- =============================================
-- Overflow with bytea data
-- =============================================

CREATE TABLE recno_ov_bytea (
    id serial PRIMARY KEY,
    binary_data bytea
) USING recno;

-- Insert binary data that should overflow
INSERT INTO recno_ov_bytea (binary_data)
VALUES (decode(repeat('CAFEBABE', 2500), 'hex'));

-- Verify binary integrity
SELECT
    length(binary_data) AS byte_len,
    binary_data = decode(repeat('CAFEBABE', 2500), 'hex') AS binary_matches
FROM recno_ov_bytea;

-- Insert varied binary data
INSERT INTO recno_ov_bytea (binary_data)
SELECT decode(repeat(md5(i::text), 200), 'hex')
FROM generate_series(1, 10) i;

SELECT id, length(binary_data) AS byte_len FROM recno_ov_bytea ORDER BY id;

DROP TABLE recno_ov_bytea;

-- =============================================
-- Update operations with overflow
-- =============================================

CREATE TABLE recno_ov_update (
    id serial PRIMARY KEY,
    name text,
    data text
) USING recno;

-- Insert with overflow
INSERT INTO recno_ov_update (name, data)
VALUES ('original', repeat('O', 10000));

-- Update: overflow to overflow (different size)
UPDATE recno_ov_update SET data = repeat('U', 20000) WHERE id = 1;
SELECT length(data) AS len, data = repeat('U', 20000) AS ok FROM recno_ov_update WHERE id = 1;

-- Update: overflow to non-overflow (shrink)
UPDATE recno_ov_update SET data = 'tiny' WHERE id = 1;
SELECT length(data) AS len, data = 'tiny' AS ok FROM recno_ov_update WHERE id = 1;

-- Update: non-overflow to overflow (grow)
UPDATE recno_ov_update SET data = repeat('G', 15000) WHERE id = 1;
SELECT length(data) AS len, data = repeat('G', 15000) AS ok FROM recno_ov_update WHERE id = 1;

-- Update non-overflow column on a row with overflow data
UPDATE recno_ov_update SET name = 'renamed' WHERE id = 1;
SELECT name, length(data) AS len FROM recno_ov_update WHERE id = 1;

DROP TABLE recno_ov_update;

-- =============================================
-- Delete operations with overflow cleanup
-- =============================================

CREATE TABLE recno_ov_delete (
    id serial PRIMARY KEY,
    data text
) USING recno;

-- Insert multiple overflow rows
INSERT INTO recno_ov_delete (data)
SELECT repeat('D' || i::text, 5000) FROM generate_series(1, 20) i;

SELECT COUNT(*) FROM recno_ov_delete;

-- Delete some rows (should clean up overflow chains)
DELETE FROM recno_ov_delete WHERE id <= 10;
SELECT COUNT(*) FROM recno_ov_delete;

-- Verify remaining rows are intact
SELECT id, length(data) > 0 AS has_data FROM recno_ov_delete ORDER BY id;

-- Delete all remaining
DELETE FROM recno_ov_delete;
SELECT COUNT(*) FROM recno_ov_delete;

DROP TABLE recno_ov_delete;

-- =============================================
-- VACUUM with overflow records
-- =============================================

CREATE TABLE recno_ov_vacuum (
    id serial PRIMARY KEY,
    data text
) USING recno;

-- Insert overflow data
INSERT INTO recno_ov_vacuum (data)
SELECT repeat('V', 8000) FROM generate_series(1, 50);

-- Delete some rows
DELETE FROM recno_ov_vacuum WHERE id % 2 = 0;

-- VACUUM should handle overflow record cleanup
VACUUM recno_ov_vacuum;

-- Verify survivors
SELECT COUNT(*) FROM recno_ov_vacuum;
SELECT id, length(data) = 8000 AS len_ok FROM recno_ov_vacuum LIMIT 5;

-- VACUUM FULL with overflow
VACUUM FULL recno_ov_vacuum;
SELECT COUNT(*) FROM recno_ov_vacuum;

DROP TABLE recno_ov_vacuum;

-- =============================================
-- Overflow with indexes
-- =============================================

CREATE TABLE recno_ov_idx (
    id serial PRIMARY KEY,
    name text,
    description text
) USING recno;

CREATE INDEX idx_ov_name ON recno_ov_idx (name);

-- Insert rows where description overflows but name is indexed
INSERT INTO recno_ov_idx (name, description)
SELECT 'item_' || i, repeat('Description for item ' || i || '. ', 500)
FROM generate_series(1, 100) i;

-- Index scan should work even when tuple has overflow columns
SET enable_seqscan = off;
SELECT name, length(description) AS desc_len
FROM recno_ov_idx WHERE name = 'item_50';
RESET enable_seqscan;

-- Update via index lookup
UPDATE recno_ov_idx SET description = repeat('Updated description. ', 600)
WHERE name = 'item_50';

SET enable_seqscan = off;
SELECT name, length(description) AS desc_len
FROM recno_ov_idx WHERE name = 'item_50';
RESET enable_seqscan;

-- Delete via index lookup
DELETE FROM recno_ov_idx WHERE name = 'item_50';

SET enable_seqscan = off;
SELECT COUNT(*) FROM recno_ov_idx WHERE name = 'item_50';
RESET enable_seqscan;

DROP TABLE recno_ov_idx;

-- =============================================
-- Overflow with inline prefix (GUC)
-- =============================================

-- Test configurable inline prefix
SHOW recno_overflow_inline_prefix;

-- Overflow rows should still work with different prefix sizes
-- (The inline prefix allows prefix-based operations without fetching overflow)

CREATE TABLE recno_ov_prefix (
    id serial PRIMARY KEY,
    data text
) USING recno;

INSERT INTO recno_ov_prefix (data)
VALUES (repeat('Prefix test data. ', 500));

-- The first N bytes should be accessible inline
SELECT left(data, 50) AS prefix_sample FROM recno_ov_prefix WHERE id = 1;

-- Full retrieval still works
SELECT length(data) AS full_len, data = repeat('Prefix test data. ', 500) AS full_ok
FROM recno_ov_prefix WHERE id = 1;

DROP TABLE recno_ov_prefix;

-- =============================================
-- Overflow with bulk operations
-- =============================================

CREATE TABLE recno_ov_bulk (
    id serial PRIMARY KEY,
    category text,
    data text
) USING recno;

-- Bulk insert with overflow
INSERT INTO recno_ov_bulk (category, data)
SELECT
    CASE i % 3
        WHEN 0 THEN 'large'
        WHEN 1 THEN 'medium'
        WHEN 2 THEN 'small'
    END,
    CASE i % 3
        WHEN 0 THEN repeat('L', 20000)   -- Overflows
        WHEN 1 THEN repeat('M', 5000)    -- May overflow
        WHEN 2 THEN repeat('S', 100)     -- No overflow
    END
FROM generate_series(1, 300) i;

-- Aggregation over mixed overflow/non-overflow
SELECT category, COUNT(*), AVG(length(data))::integer AS avg_len
FROM recno_ov_bulk GROUP BY category ORDER BY category;

-- Range query
SELECT COUNT(*) FROM recno_ov_bulk WHERE length(data) > 10000;

-- Bulk delete
DELETE FROM recno_ov_bulk WHERE category = 'large';
SELECT COUNT(*) FROM recno_ov_bulk;

-- VACUUM after bulk delete of overflow rows
VACUUM recno_ov_bulk;
SELECT COUNT(*) FROM recno_ov_bulk;

DROP TABLE recno_ov_bulk;

-- =============================================
-- Overflow with COPY
-- =============================================

CREATE TABLE recno_ov_copy (
    id integer,
    data text
) USING recno;

-- Generate a large string for COPY
COPY recno_ov_copy FROM stdin;
1	This is a short text value
\.

-- COPY a row with a long value constructed from SQL
INSERT INTO recno_ov_copy VALUES (2, repeat('CopyOverflow ', 1000));

COPY recno_ov_copy TO stdout WITH (FORMAT csv);

SELECT id, length(data) FROM recno_ov_copy ORDER BY id;

DROP TABLE recno_ov_copy;

-- =============================================
-- Overflow with transactions
-- =============================================

CREATE TABLE recno_ov_tx (
    id serial PRIMARY KEY,
    data text
) USING recno;

-- Insert overflow data, then rollback
BEGIN;
INSERT INTO recno_ov_tx (data) VALUES (repeat('Rollback', 5000));
ROLLBACK;

SELECT COUNT(*) FROM recno_ov_tx;

-- Insert overflow data, then commit
BEGIN;
INSERT INTO recno_ov_tx (data) VALUES (repeat('Commit', 5000));
COMMIT;

SELECT COUNT(*), length(data) AS len FROM recno_ov_tx GROUP BY data;

-- Update overflow in transaction, then rollback
BEGIN;
UPDATE recno_ov_tx SET data = repeat('Updated', 10000) WHERE id = 1;
ROLLBACK;

SELECT length(data) AS len, data = repeat('Commit', 5000) AS original_intact
FROM recno_ov_tx WHERE id = 1;

DROP TABLE recno_ov_tx;

-- =============================================
-- Overflow mixed with HEAP table cross-query
-- =============================================

-- Verify RECNO overflow tables can JOIN with heap tables
CREATE TABLE heap_ref (id serial PRIMARY KEY, label text) USING heap;
CREATE TABLE recno_ov_join (
    id serial PRIMARY KEY,
    heap_id integer REFERENCES heap_ref(id),
    big_data text
) USING recno;

INSERT INTO heap_ref (label) VALUES ('ref_a'), ('ref_b'), ('ref_c');
INSERT INTO recno_ov_join (heap_id, big_data) VALUES
    (1, repeat('Join test A. ', 1000)),
    (2, repeat('Join test B. ', 1000)),
    (3, repeat('Join test C. ', 500));

SELECT h.label, length(r.big_data) AS data_len
FROM heap_ref h JOIN recno_ov_join r ON h.id = r.heap_id
ORDER BY h.label;

DROP TABLE recno_ov_join;
DROP TABLE heap_ref;

-- =============================================
-- Extreme cases
-- =============================================

CREATE TABLE recno_ov_extreme (
    id serial PRIMARY KEY,
    data text
) USING recno;

-- Large value (~100KB, within WAL segment limits)
INSERT INTO recno_ov_extreme (data) VALUES (repeat('M', 100000));
SELECT id, length(data) AS len, data = repeat('M', 100000) AS ok
FROM recno_ov_extreme;

-- Multiple large values in succession
INSERT INTO recno_ov_extreme (data)
SELECT repeat(chr(65 + (i % 26)), 50000) FROM generate_series(1, 10) i;

SELECT id, length(data) AS len FROM recno_ov_extreme ORDER BY id;

-- Verify all data integrity
SELECT id,
    data = repeat(chr(65 + ((id - 2) % 26)), 50000) AS ok
FROM recno_ov_extreme WHERE id > 1 ORDER BY id;

DROP TABLE recno_ov_extreme;
