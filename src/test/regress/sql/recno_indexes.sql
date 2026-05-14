--
-- Test RECNO index operations: B-tree, hash, GIN, GiST, BRIN
-- Index-only scans, bitmap scans, expression indexes, partial indexes
--

-- =============================================
-- Setup
-- =============================================

CREATE TABLE recno_idx_test (
    id serial PRIMARY KEY,
    name text NOT NULL,
    value integer,
    category text,
    tags text[],
    point_val point,
    range_val int4range,
    tsvec_val tsvector,
    created_at timestamp DEFAULT now()
) USING recno;

-- Insert substantial data for index testing
INSERT INTO recno_idx_test (name, value, category, tags, point_val, range_val, tsvec_val)
SELECT
    'item_' || i,
    i % 1000,
    CASE i % 5
        WHEN 0 THEN 'electronics'
        WHEN 1 THEN 'books'
        WHEN 2 THEN 'clothing'
        WHEN 3 THEN 'food'
        WHEN 4 THEN 'tools'
    END,
    ARRAY['tag_' || (i % 10), 'tag_' || (i % 20)],
    point(i::float, (i * 2)::float),
    int4range(i, i + 10),
    to_tsvector('english', 'item number ' || i || ' in category ' ||
        CASE i % 5
            WHEN 0 THEN 'electronics'
            WHEN 1 THEN 'books'
            WHEN 2 THEN 'clothing'
            WHEN 3 THEN 'food'
            WHEN 4 THEN 'tools'
        END)
FROM generate_series(1, 5000) i;

-- =============================================
-- B-tree indexes
-- =============================================

-- Simple B-tree index
CREATE INDEX idx_recno_name ON recno_idx_test (name);
CREATE INDEX idx_recno_value ON recno_idx_test (value);

-- Multi-column B-tree index
CREATE INDEX idx_recno_cat_val ON recno_idx_test (category, value);

-- Verify index usage for equality
SET enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM recno_idx_test WHERE name = 'item_500';
SELECT name, value FROM recno_idx_test WHERE name = 'item_500';

-- Verify index usage for range query
EXPLAIN (COSTS OFF) SELECT * FROM recno_idx_test WHERE value BETWEEN 100 AND 110;
SELECT COUNT(*) FROM recno_idx_test WHERE value BETWEEN 100 AND 110;

-- Multi-column index usage
EXPLAIN (COSTS OFF) SELECT * FROM recno_idx_test WHERE category = 'books' AND value < 50;
SELECT COUNT(*) FROM recno_idx_test WHERE category = 'books' AND value < 50;

-- Index ordering
SELECT name FROM recno_idx_test ORDER BY name LIMIT 5;
SELECT name FROM recno_idx_test ORDER BY name DESC LIMIT 5;

RESET enable_seqscan;

-- =============================================
-- Index-only scans
-- =============================================

-- Create a covering index
CREATE INDEX idx_recno_value_name ON recno_idx_test (value) INCLUDE (name);

-- Force index-only scan
SET enable_seqscan = off;
SET enable_bitmapscan = off;

-- After VACUUM to set visibility map
VACUUM recno_idx_test;

EXPLAIN (COSTS OFF) SELECT value, name FROM recno_idx_test WHERE value = 500;
SELECT value, name FROM recno_idx_test WHERE value = 500;

RESET enable_seqscan;
RESET enable_bitmapscan;

-- =============================================
-- Bitmap scans
-- =============================================

SET enable_seqscan = off;
SET enable_indexscan = off;

EXPLAIN (COSTS OFF)
SELECT COUNT(*) FROM recno_idx_test WHERE value < 100 OR value > 900;

SELECT COUNT(*) FROM recno_idx_test WHERE value < 100 OR value > 900;

-- Bitmap AND of two indexes
EXPLAIN (COSTS OFF)
SELECT COUNT(*) FROM recno_idx_test WHERE value < 200 AND category = 'books';

RESET enable_seqscan;
RESET enable_indexscan;

-- =============================================
-- Hash index
-- =============================================

CREATE INDEX idx_recno_cat_hash ON recno_idx_test USING hash (category);

SET enable_seqscan = off;
SET enable_bitmapscan = off;

EXPLAIN (COSTS OFF) SELECT * FROM recno_idx_test WHERE category = 'electronics';
SELECT COUNT(*) FROM recno_idx_test WHERE category = 'electronics';

RESET enable_seqscan;
RESET enable_bitmapscan;

-- =============================================
-- GiST index (for points and ranges)
-- =============================================

CREATE INDEX idx_recno_point_gist ON recno_idx_test USING gist (point_val);
CREATE INDEX idx_recno_range_gist ON recno_idx_test USING gist (range_val);

SET enable_seqscan = off;
SET enable_bitmapscan = off;

-- Nearest-neighbor query
EXPLAIN (COSTS OFF)
SELECT name FROM recno_idx_test ORDER BY point_val <-> point(500, 1000) LIMIT 5;

SELECT name, point_val FROM recno_idx_test ORDER BY point_val <-> point(500, 1000) LIMIT 5;

-- Range containment
EXPLAIN (COSTS OFF)
SELECT COUNT(*) FROM recno_idx_test WHERE range_val @> 500;

SELECT COUNT(*) FROM recno_idx_test WHERE range_val @> 500;

RESET enable_bitmapscan;
RESET enable_seqscan;

-- =============================================
-- GIN index (for arrays and full-text search)
-- =============================================

CREATE INDEX idx_recno_tags_gin ON recno_idx_test USING gin (tags);
CREATE INDEX idx_recno_tsvec_gin ON recno_idx_test USING gin (tsvec_val);

SET enable_seqscan = off;

-- Array containment via GIN
EXPLAIN (COSTS OFF)
SELECT COUNT(*) FROM recno_idx_test WHERE tags @> ARRAY['tag_5'];

SELECT COUNT(*) FROM recno_idx_test WHERE tags @> ARRAY['tag_5'];

-- Full-text search via GIN
EXPLAIN (COSTS OFF)
SELECT COUNT(*) FROM recno_idx_test WHERE tsvec_val @@ to_tsquery('books');

SELECT COUNT(*) FROM recno_idx_test WHERE tsvec_val @@ to_tsquery('books');

RESET enable_seqscan;

-- =============================================
-- BRIN index
-- =============================================

CREATE INDEX idx_recno_id_brin ON recno_idx_test USING brin (id);

SET enable_seqscan = off;
SET enable_indexscan = off;

EXPLAIN (COSTS OFF)
SELECT COUNT(*) FROM recno_idx_test WHERE id BETWEEN 1000 AND 2000;

SELECT COUNT(*) FROM recno_idx_test WHERE id BETWEEN 1000 AND 2000;

RESET enable_seqscan;
RESET enable_indexscan;

-- =============================================
-- Expression and partial indexes
-- =============================================

-- Expression index
CREATE INDEX idx_recno_lower_name ON recno_idx_test (lower(name));

SET enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM recno_idx_test WHERE lower(name) = 'item_100';
SELECT name FROM recno_idx_test WHERE lower(name) = 'item_100';
RESET enable_seqscan;

-- Partial index
CREATE INDEX idx_recno_high_value ON recno_idx_test (value) WHERE value > 900;

SET enable_seqscan = off;
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM recno_idx_test WHERE value > 900;
SELECT COUNT(*) FROM recno_idx_test WHERE value > 900;
RESET enable_bitmapscan;
RESET enable_seqscan;

-- =============================================
-- Unique index
-- =============================================

CREATE TABLE recno_idx_unique (
    id serial,
    code text
) USING recno;

CREATE UNIQUE INDEX idx_recno_unique_code ON recno_idx_unique (code);

INSERT INTO recno_idx_unique (code) VALUES ('A'), ('B'), ('C');

-- Should fail
\set ON_ERROR_STOP off
INSERT INTO recno_idx_unique (code) VALUES ('A');
\set ON_ERROR_STOP on

DROP TABLE recno_idx_unique;

-- =============================================
-- Index maintenance during DML
-- =============================================

-- Insert new rows and verify index consistency
INSERT INTO recno_idx_test (name, value, category)
VALUES ('new_item_1', 42, 'books');

SET enable_seqscan = off;
SELECT name, value FROM recno_idx_test WHERE name = 'new_item_1';
RESET enable_seqscan;

-- Update indexed column
UPDATE recno_idx_test SET value = 9999 WHERE name = 'new_item_1';

SET enable_seqscan = off;
SELECT name, value FROM recno_idx_test WHERE value = 9999;
RESET enable_seqscan;

-- Delete row and verify index
DELETE FROM recno_idx_test WHERE name = 'new_item_1';

SET enable_seqscan = off;
SELECT COUNT(*) FROM recno_idx_test WHERE name = 'new_item_1';
RESET enable_seqscan;

-- =============================================
-- REINDEX
-- =============================================

REINDEX INDEX idx_recno_name;
REINDEX TABLE recno_idx_test;

-- Verify indexes still work after reindex
SET enable_seqscan = off;
SELECT name FROM recno_idx_test WHERE name = 'item_1';
RESET enable_seqscan;

-- =============================================
-- DROP and recreate index
-- =============================================

DROP INDEX idx_recno_name;

-- Recreate it
CREATE INDEX idx_recno_name ON recno_idx_test (name);

-- Verify it works again
SET enable_seqscan = off;
SELECT name FROM recno_idx_test WHERE name = 'item_2500';
RESET enable_seqscan;

-- =============================================
-- Concurrent index creation
-- =============================================

-- CREATE INDEX CONCURRENTLY (single-session, so it just works normally)
CREATE INDEX CONCURRENTLY idx_recno_concurrent ON recno_idx_test (value, category);

SET enable_seqscan = off;
SELECT COUNT(*) FROM recno_idx_test WHERE value = 500 AND category = 'electronics';
RESET enable_seqscan;

DROP INDEX idx_recno_concurrent;

-- =============================================
-- Index on table with many updates
-- =============================================

CREATE TABLE recno_idx_churn (
    id serial PRIMARY KEY,
    val integer
) USING recno;

CREATE INDEX idx_churn_val ON recno_idx_churn (val);

-- Insert, update, delete cycle
INSERT INTO recno_idx_churn (val) SELECT i FROM generate_series(1, 1000) i;

-- Update all rows
UPDATE recno_idx_churn SET val = val + 1000;

-- Delete half
DELETE FROM recno_idx_churn WHERE id % 2 = 0;

-- Re-insert
INSERT INTO recno_idx_churn (val) SELECT i + 2000 FROM generate_series(1, 500) i;

-- Vacuum to clean up
VACUUM recno_idx_churn;

-- Verify index still works correctly
SET enable_seqscan = off;
SELECT COUNT(*) FROM recno_idx_churn WHERE val BETWEEN 1001 AND 1500;
SELECT COUNT(*) FROM recno_idx_churn WHERE val BETWEEN 2001 AND 2500;
RESET enable_seqscan;

DROP TABLE recno_idx_churn;

-- =============================================
-- Cleanup
-- =============================================

DROP TABLE recno_idx_test;
