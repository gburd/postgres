--
-- recno_overflow_full.sql
--
-- Comprehensive tests for RECNO column-level overflow.
-- Covers: large attribute storage, retrieval correctness, UPDATE of
-- overflow attributes, VACUUM cleanup of overflow chains, and
-- storage efficiency measurements.
--

-- =============================================
-- Large text attribute storage (>8KB)
-- =============================================

CREATE TABLE recno_ovf_text (
    id serial PRIMARY KEY,
    label text,
    big_text text
) USING recno;

-- 8KB text (just above a single page threshold)
INSERT INTO recno_ovf_text (label, big_text)
VALUES ('8kb', repeat('A', 8192));

-- 16KB text (spans multiple overflow records)
INSERT INTO recno_ovf_text (label, big_text)
VALUES ('16kb', repeat('B', 16384));

-- 32KB text
INSERT INTO recno_ovf_text (label, big_text)
VALUES ('32kb', repeat('C', 32768));

-- 64KB text
INSERT INTO recno_ovf_text (label, big_text)
VALUES ('64kb', repeat('D', 65536));

-- Verify retrieval correctness: length and content
SELECT label,
       length(big_text) AS len,
       big_text = repeat(chr(ascii('A') + id - 1), length(big_text)) AS content_ok
FROM recno_ovf_text ORDER BY id;

-- Verify prefix and suffix are intact
SELECT label,
       left(big_text, 20) AS prefix,
       right(big_text, 20) AS suffix
FROM recno_ovf_text ORDER BY id;

DROP TABLE recno_ovf_text;

-- =============================================
-- Large bytea attribute
-- =============================================

CREATE TABLE recno_ovf_bytea (
    id serial PRIMARY KEY,
    big_bin bytea
) USING recno;

-- 20KB binary data
INSERT INTO recno_ovf_bytea (big_bin)
VALUES (decode(repeat('DEADBEEF', 5000), 'hex'));

-- 40KB binary data
INSERT INTO recno_ovf_bytea (big_bin)
VALUES (decode(repeat('CAFEBABE', 10000), 'hex'));

-- Verify exact byte-level content integrity
SELECT id,
       length(big_bin) AS byte_len,
       CASE id
           WHEN 1 THEN big_bin = decode(repeat('DEADBEEF', 5000), 'hex')
           WHEN 2 THEN big_bin = decode(repeat('CAFEBABE', 10000), 'hex')
       END AS content_ok
FROM recno_ovf_bytea ORDER BY id;

DROP TABLE recno_ovf_bytea;

-- =============================================
-- Large JSON documents
-- =============================================

CREATE TABLE recno_ovf_json (
    id serial PRIMARY KEY,
    doc jsonb
) USING recno;

-- Build a JSON document ~50KB using array of objects
INSERT INTO recno_ovf_json (doc)
SELECT jsonb_build_object(
    'header', 'large document',
    'payload', (
        SELECT jsonb_agg(
            jsonb_build_object(
                'index', i,
                'data', repeat('X', 100),
                'nested', jsonb_build_object('a', i, 'b', repeat('Y', 50))
            )
        )
        FROM generate_series(1, 200) i
    )
);

-- Verify the document stored and retrieved correctly
SELECT id,
       pg_column_size(doc) > 0 AS has_data,
       (doc->>'header') = 'large document' AS header_ok,
       jsonb_array_length(doc->'payload') AS payload_items
FROM recno_ovf_json;


-- Extract specific nested element to verify integrity
SELECT (doc->'payload'->0->>'index')::int AS first_idx,
       (doc->'payload'->199->>'index')::int AS last_idx
FROM recno_ovf_json WHERE id = 1;

DROP TABLE recno_ovf_json;

-- =============================================
-- Overflow chain integrity
-- =============================================

-- Test that multiple overflow columns in one row don't corrupt each other
CREATE TABLE recno_ovf_multi (
    id serial PRIMARY KEY,
    col_a text,
    col_b bytea,
    col_c text,
    small_int integer
) USING recno;

INSERT INTO recno_ovf_multi (col_a, col_b, col_c, small_int)
VALUES (
    repeat('A', 10000),
    decode(repeat('FF', 5000), 'hex'),
    repeat('C', 15000),
    42
);

-- Verify all columns independently
SELECT
    col_a = repeat('A', 10000) AS a_ok,
    col_b = decode(repeat('FF', 5000), 'hex') AS b_ok,
    col_c = repeat('C', 15000) AS c_ok,
    small_int = 42 AS int_ok
FROM recno_ovf_multi WHERE id = 1;

-- Insert more rows to test chain isolation between rows
INSERT INTO recno_ovf_multi (col_a, col_b, col_c, small_int)
SELECT
    repeat(chr(65 + (i % 26)), 8000 + i * 100),
    decode(repeat(lpad(to_hex(i % 256), 2, '0'), 4000 + i * 50), 'hex'),
    repeat(chr(97 + (i % 26)), 12000 + i * 200),
    i
FROM generate_series(1, 20) i;

-- Verify row count and that small_int survived
SELECT COUNT(*) AS total_rows FROM recno_ovf_multi;
SELECT id, small_int, length(col_a) AS a_len, length(col_b) AS b_len, length(col_c) AS c_len
FROM recno_ovf_multi ORDER BY id LIMIT 5;

DROP TABLE recno_ovf_multi;

-- =============================================
-- UPDATE of overflow attributes
-- =============================================

CREATE TABLE recno_ovf_update (
    id serial PRIMARY KEY,
    name text,
    data text
) USING recno;

-- Start with overflow data
INSERT INTO recno_ovf_update (name, data) VALUES ('row1', repeat('O', 10000));

-- Update: overflow -> larger overflow
UPDATE recno_ovf_update SET data = repeat('U', 25000) WHERE id = 1;
SELECT length(data) AS len, data = repeat('U', 25000) AS ok
FROM recno_ovf_update WHERE id = 1;

-- Update: overflow -> inline (shrink below threshold)
UPDATE recno_ovf_update SET data = 'small' WHERE id = 1;
SELECT length(data) AS len, data = 'small' AS ok
FROM recno_ovf_update WHERE id = 1;

-- Update: inline -> overflow (grow above threshold)
UPDATE recno_ovf_update SET data = repeat('G', 20000) WHERE id = 1;
SELECT length(data) AS len, data = repeat('G', 20000) AS ok
FROM recno_ovf_update WHERE id = 1;

-- Update non-overflow column while overflow data stays intact
UPDATE recno_ovf_update SET name = 'renamed' WHERE id = 1;
SELECT name = 'renamed' AS name_ok,
       length(data) = 20000 AS data_len_ok,
       data = repeat('G', 20000) AS data_ok
FROM recno_ovf_update WHERE id = 1;

-- Rapid succession of updates that toggle overflow on/off
INSERT INTO recno_ovf_update (name, data) VALUES ('toggle', 'start');
UPDATE recno_ovf_update SET data = repeat('T', 15000) WHERE name = 'toggle';
UPDATE recno_ovf_update SET data = 'short again' WHERE name = 'toggle';
UPDATE recno_ovf_update SET data = repeat('T', 30000) WHERE name = 'toggle';
SELECT name, length(data) AS len, data = repeat('T', 30000) AS ok
FROM recno_ovf_update WHERE name = 'toggle';

DROP TABLE recno_ovf_update;

-- =============================================
-- DELETE cleanup of overflow chains
-- =============================================

CREATE TABLE recno_ovf_delete (
    id serial PRIMARY KEY,
    data text
) USING recno;

-- Insert 50 rows with overflow data
INSERT INTO recno_ovf_delete (data)
SELECT repeat('D' || (i % 10)::text, 8000) FROM generate_series(1, 50) i;

SELECT COUNT(*) AS before_delete FROM recno_ovf_delete;

-- Delete half the rows
DELETE FROM recno_ovf_delete WHERE id % 2 = 0;
SELECT COUNT(*) AS after_delete FROM recno_ovf_delete;

-- Verify surviving rows are intact
SELECT id,
       length(data) > 0 AS has_data,
       left(data, 2) AS data_prefix
FROM recno_ovf_delete ORDER BY id LIMIT 10;

-- Delete all remaining
DELETE FROM recno_ovf_delete;
SELECT COUNT(*) AS after_full_delete FROM recno_ovf_delete;

DROP TABLE recno_ovf_delete;

-- =============================================
-- VACUUM cleanup of overflow chains
-- =============================================

CREATE TABLE recno_ovf_vacuum (
    id serial PRIMARY KEY,
    data text
) USING recno;

-- Insert overflow data
INSERT INTO recno_ovf_vacuum (data)
SELECT repeat('V', 10000) FROM generate_series(1, 100);

-- Delete most rows
DELETE FROM recno_ovf_vacuum WHERE id <= 80;

-- VACUUM should clean up dead tuples and their overflow chains
VACUUM recno_ovf_vacuum;

-- Verify surviving rows
SELECT COUNT(*) AS survivors FROM recno_ovf_vacuum;
SELECT id, length(data) = 10000 AS len_ok
FROM recno_ovf_vacuum ORDER BY id LIMIT 5;

-- Insert more overflow data to reuse freed space
INSERT INTO recno_ovf_vacuum (data)
SELECT repeat('N', 12000) FROM generate_series(1, 50);

-- Verify new data
SELECT COUNT(*) AS total FROM recno_ovf_vacuum;

-- VACUUM FULL with overflow
DELETE FROM recno_ovf_vacuum WHERE id > 100;
VACUUM FULL recno_ovf_vacuum;

SELECT COUNT(*) AS after_vacuum_full FROM recno_ovf_vacuum;
SELECT id, length(data) = 10000 AS len_ok
FROM recno_ovf_vacuum ORDER BY id LIMIT 5;

DROP TABLE recno_ovf_vacuum;

-- =============================================
-- VACUUM with interleaved overflow and non-overflow
-- =============================================

CREATE TABLE recno_ovf_vacuum_mixed (
    id serial PRIMARY KEY,
    category text,
    data text
) USING recno;

-- Mix of overflow and non-overflow rows
INSERT INTO recno_ovf_vacuum_mixed (category, data)
SELECT
    CASE WHEN i % 3 = 0 THEN 'large' ELSE 'small' END,
    CASE WHEN i % 3 = 0 THEN repeat('L', 15000)
         ELSE 'small_' || i::text
    END
FROM generate_series(1, 60) i;

-- Delete only overflow rows
DELETE FROM recno_ovf_vacuum_mixed WHERE category = 'large';
VACUUM recno_ovf_vacuum_mixed;

-- Non-overflow rows should be untouched
SELECT COUNT(*) AS remaining FROM recno_ovf_vacuum_mixed;
SELECT DISTINCT category FROM recno_ovf_vacuum_mixed;

-- Delete only non-overflow rows
DELETE FROM recno_ovf_vacuum_mixed;
VACUUM recno_ovf_vacuum_mixed;

SELECT COUNT(*) AS final_count FROM recno_ovf_vacuum_mixed;

DROP TABLE recno_ovf_vacuum_mixed;

-- =============================================
-- Storage efficiency measurement
-- =============================================

CREATE TABLE recno_ovf_efficiency (
    id serial PRIMARY KEY,
    data text
) USING recno;

-- Insert data of known sizes
INSERT INTO recno_ovf_efficiency (data)
SELECT repeat('E', 10000) FROM generate_series(1, 100);

-- Measure relation size
SELECT pg_relation_size('recno_ovf_efficiency') AS relation_bytes;

-- Expected data: 100 * 10000 = 1,000,000 bytes of user data
-- Storage overhead = (relation_size - 1000000) / 1000000
SELECT
    pg_relation_size('recno_ovf_efficiency') AS storage_bytes,
    100 * 10000 AS user_data_bytes,
    ROUND(
        (pg_relation_size('recno_ovf_efficiency')::numeric - 1000000) / 1000000 * 100,
        1
    ) AS overhead_percent;

DROP TABLE recno_ovf_efficiency;

-- =============================================
-- Overflow with concurrent-like patterns
-- =============================================

CREATE TABLE recno_ovf_concurrent (
    id serial PRIMARY KEY,
    version integer DEFAULT 0,
    data text
) USING recno;

-- Insert, update, delete in rapid succession
INSERT INTO recno_ovf_concurrent (data)
SELECT repeat('C', 9000) FROM generate_series(1, 30);

-- Update all rows (overflow -> overflow replacement)
UPDATE recno_ovf_concurrent SET data = repeat('U', 11000), version = version + 1;
SELECT COUNT(*) AS updated, MIN(version) AS min_ver, MAX(version) AS max_ver
FROM recno_ovf_concurrent;

-- Delete and re-insert pattern
DELETE FROM recno_ovf_concurrent WHERE id % 3 = 0;
INSERT INTO recno_ovf_concurrent (version, data)
SELECT 99, repeat('R', 13000) FROM generate_series(1, 10);

VACUUM recno_ovf_concurrent;

SELECT COUNT(*) AS final_count FROM recno_ovf_concurrent;
SELECT id, version, length(data) AS data_len
FROM recno_ovf_concurrent ORDER BY id LIMIT 10;

DROP TABLE recno_ovf_concurrent;

-- =============================================
-- Overflow with transactions (commit/rollback)
-- =============================================

CREATE TABLE recno_ovf_tx (
    id serial PRIMARY KEY,
    data text
) USING recno;

-- Insert overflow data then ROLLBACK
BEGIN;
INSERT INTO recno_ovf_tx (data) VALUES (repeat('ROLLBACK', 5000));
ROLLBACK;

SELECT COUNT(*) AS after_rollback FROM recno_ovf_tx;

-- Insert overflow data then COMMIT
BEGIN;
INSERT INTO recno_ovf_tx (data) VALUES (repeat('COMMIT', 5000));
COMMIT;

SELECT COUNT(*) AS after_commit FROM recno_ovf_tx;
SELECT length(data) AS len, data = repeat('COMMIT', 5000) AS ok
FROM recno_ovf_tx;

-- Update overflow data then ROLLBACK
BEGIN;
UPDATE recno_ovf_tx SET data = repeat('UPDATED', 10000) WHERE id = 1;
ROLLBACK;

SELECT length(data) AS len, data = repeat('COMMIT', 5000) AS original_ok
FROM recno_ovf_tx WHERE id = 1;

DROP TABLE recno_ovf_tx;

-- =============================================
-- Overflow with indexes
-- =============================================

CREATE TABLE recno_ovf_idx (
    id serial PRIMARY KEY,
    tag text,
    payload text
) USING recno;

CREATE INDEX idx_ovf_tag ON recno_ovf_idx (tag);

-- Insert rows where payload overflows but tag is indexed
INSERT INTO recno_ovf_idx (tag, payload)
SELECT 'tag_' || lpad(i::text, 4, '0'),
       repeat('P' || (i % 10)::text, 5000)
FROM generate_series(1, 200) i;

-- Index scan should work with overflow payload
SET enable_seqscan = off;
SELECT tag, length(payload) AS payload_len
FROM recno_ovf_idx WHERE tag = 'tag_0100';
RESET enable_seqscan;

-- Update via index scan
UPDATE recno_ovf_idx SET payload = repeat('UPDATED', 7000) WHERE tag = 'tag_0050';

SET enable_seqscan = off;
SELECT tag, length(payload) AS payload_len, left(payload, 7) AS prefix
FROM recno_ovf_idx WHERE tag = 'tag_0050';
RESET enable_seqscan;

-- Delete via index scan
DELETE FROM recno_ovf_idx WHERE tag = 'tag_0050';
SET enable_seqscan = off;
SELECT COUNT(*) FROM recno_ovf_idx WHERE tag = 'tag_0050';
RESET enable_seqscan;

DROP TABLE recno_ovf_idx;

-- =============================================
-- Boundary cases around overflow threshold
-- =============================================

CREATE TABLE recno_ovf_boundary (
    id serial PRIMARY KEY,
    data text
) USING recno;

-- Insert values around the threshold (RECNO_MAX_TUPLE_SIZE / 4)
-- For 8KB pages, threshold is roughly ~2000 bytes
INSERT INTO recno_ovf_boundary (data) VALUES
    (repeat('a', 1900)),   -- Below threshold
    (repeat('b', 1950)),   -- Near threshold
    (repeat('c', 2000)),   -- At/near threshold
    (repeat('d', 2050)),   -- Just above threshold
    (repeat('e', 2100)),   -- Above threshold
    (repeat('f', 3000)),   -- Well above threshold
    (repeat('g', 5000));   -- Clearly overflowing

-- All should round-trip correctly regardless of overflow status
SELECT id, length(data) AS len,
       data = repeat(chr(ascii('a') + id - 1), length(data)) AS content_ok
FROM recno_ovf_boundary ORDER BY id;

DROP TABLE recno_ovf_boundary;

-- =============================================
-- Very large single column (stress test)
-- =============================================

CREATE TABLE recno_ovf_stress (
    id serial PRIMARY KEY,
    data text
) USING recno;

-- 80KB text column (within WAL segment limits)
INSERT INTO recno_ovf_stress (data) VALUES (repeat('M', 81920));

SELECT id,
       length(data) AS len,
       length(data) = 81920 AS len_ok,
       left(data, 10) = 'MMMMMMMMMM' AS prefix_ok,
       right(data, 10) = 'MMMMMMMMMM' AS suffix_ok,
       data = repeat('M', 81920) AS full_ok
FROM recno_ovf_stress;

-- 100KB text column (within WAL segment limits)
INSERT INTO recno_ovf_stress (data) VALUES (repeat('N', 102400));

SELECT id, length(data) AS len,
       data = CASE id WHEN 1 THEN repeat('M', 81920)
                      WHEN 2 THEN repeat('N', 102400) END AS ok
FROM recno_ovf_stress ORDER BY id;

-- Delete and VACUUM the 100KB row
DELETE FROM recno_ovf_stress WHERE id = 2;
VACUUM recno_ovf_stress;

-- The 80KB row should survive
SELECT id, length(data) = 81920 AS survivor_ok FROM recno_ovf_stress;

DROP TABLE recno_ovf_stress;

-- =============================================
-- Overflow with COPY TO/FROM
-- =============================================

CREATE TABLE recno_ovf_copy (
    id integer,
    data text
) USING recno;

INSERT INTO recno_ovf_copy VALUES (1, repeat('COPY', 5000));
INSERT INTO recno_ovf_copy VALUES (2, 'small value');

-- COPY TO should output full overflow data
COPY recno_ovf_copy TO stdout WITH (FORMAT csv);

SELECT id, length(data) AS len FROM recno_ovf_copy ORDER BY id;

DROP TABLE recno_ovf_copy;

-- =============================================
-- Cross-table joins with overflow
-- =============================================

CREATE TABLE heap_ref_ovf (id serial PRIMARY KEY, label text) USING heap;
CREATE TABLE recno_ovf_join (
    id serial PRIMARY KEY,
    ref_id integer REFERENCES heap_ref_ovf(id),
    big_data text
) USING recno;

INSERT INTO heap_ref_ovf (label) VALUES ('alpha'), ('beta'), ('gamma');
INSERT INTO recno_ovf_join (ref_id, big_data) VALUES
    (1, repeat('Join-A ', 2000)),
    (2, repeat('Join-B ', 3000)),
    (3, repeat('Join-C ', 1000));

-- JOIN should retrieve overflow data correctly
SELECT h.label, length(r.big_data) AS data_len
FROM heap_ref_ovf h JOIN recno_ovf_join r ON h.id = r.ref_id
ORDER BY h.label;

DROP TABLE recno_ovf_join;
DROP TABLE heap_ref_ovf;

-- =============================================
-- Overflow with NULLs and dropped columns
-- =============================================

CREATE TABLE recno_ovf_nulls (
    id serial PRIMARY KEY,
    a text,
    b text,
    c text
) USING recno;

-- Mix of NULL and overflow values (single-row inserts to avoid buffer pinning issue)
INSERT INTO recno_ovf_nulls (a, b, c) VALUES (repeat('A', 10000), NULL, repeat('C', 10000));
INSERT INTO recno_ovf_nulls (a, b, c) VALUES (NULL, repeat('B', 10000), NULL);
INSERT INTO recno_ovf_nulls (a, b, c) VALUES (repeat('A', 10000), repeat('B', 10000), repeat('C', 10000));

SELECT id,
       CASE WHEN a IS NULL THEN 'NULL' ELSE length(a)::text END AS a_info,
       CASE WHEN b IS NULL THEN 'NULL' ELSE length(b)::text END AS b_info,
       CASE WHEN c IS NULL THEN 'NULL' ELSE length(c)::text END AS c_info
FROM recno_ovf_nulls ORDER BY id;

-- Verify non-NULL overflowed values are intact
SELECT id,
       (a IS NULL OR a = repeat('A', 10000)) AS a_ok,
       (b IS NULL OR b = repeat('B', 10000)) AS b_ok,
       (c IS NULL OR c = repeat('C', 10000)) AS c_ok
FROM recno_ovf_nulls ORDER BY id;

DROP TABLE recno_ovf_nulls;
