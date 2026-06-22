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

-- GiST is a lossy AM (amconsistentequality = false); on an in-place table AM
-- the planner restricts it to bitmap heap scans, so leave bitmapscan enabled.
SET enable_seqscan = off;

-- Nearest-neighbor query
EXPLAIN (COSTS OFF)
SELECT name FROM recno_idx_test ORDER BY point_val <-> point(500, 1000) LIMIT 5;

SELECT name, point_val FROM recno_idx_test ORDER BY point_val <-> point(500, 1000) LIMIT 5;

-- Range containment
EXPLAIN (COSTS OFF)
SELECT COUNT(*) FROM recno_idx_test WHERE range_val @> 500;

SELECT COUNT(*) FROM recno_idx_test WHERE range_val @> 500;

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
-- Stale-entry recheck: name_ops btree (storage type cstring != input type
-- name).  An in-place UPDATE of the indexed name column leaves a stale
-- (oldname -> tid) entry beside the new one; the index-scan recheck must
-- compare in the index storage representation and drop the stale entry.
-- =============================================

CREATE TABLE recno_idx_name (
    id serial PRIMARY KEY,
    nm name
) USING recno;

CREATE INDEX idx_name_nm ON recno_idx_name (nm);

INSERT INTO recno_idx_name (nm) VALUES ('alpha'), ('bravo'), ('charlie');

-- In-place UPDATE the indexed name column.
UPDATE recno_idx_name SET nm = 'zulu' WHERE nm = 'alpha';

SET enable_seqscan = off;
-- Index-only scan over the name key: stale 'alpha' entry must not appear.
SELECT nm FROM recno_idx_name ORDER BY nm;
-- Plain index scan probing the old key must return nothing.
SELECT count(*) FROM recno_idx_name WHERE nm = 'alpha';
-- Probing the new key must return exactly the updated row.
SELECT id, nm FROM recno_idx_name WHERE nm = 'zulu';
RESET enable_seqscan;

DROP TABLE recno_idx_name;

-- =============================================
-- Stale-entry recheck: INCLUDE index.  An in-place UPDATE of a non-key
-- INCLUDE column leaves the key equal but the stored payload stale; an
-- index-only scan must emit the live payload from the table, not the stored
-- index entry.
-- =============================================

CREATE TABLE recno_idx_incl (
    id integer PRIMARY KEY,
    k integer,
    payload integer
) USING recno;

CREATE INDEX idx_incl ON recno_idx_incl (k) INCLUDE (payload);

INSERT INTO recno_idx_incl VALUES (1, 100, 10), (2, 200, 20), (3, 300, 30);

-- In-place UPDATE of the INCLUDE (non-key) column only; key k is unchanged.
UPDATE recno_idx_incl SET payload = 999 WHERE k = 100;

SET enable_seqscan = off;
-- Index-only scan must reflect the live payload (999), not the stale 10.
SELECT k, payload FROM recno_idx_incl ORDER BY k;
RESET enable_seqscan;

DROP TABLE recno_idx_incl;

-- =============================================
-- Stale-entry recheck: bitmap heap scan.  A bitmap scan keeps only TIDs and
-- discards the index tuple, so the index-scan key recheck cannot run.  The AM
-- must force a bitmap recheck for in-place UPDATEs so an exact (non-lossy)
-- bitmap probing the OLD key does not trust the stale (oldkey -> tid) entry
-- and return a row whose live key no longer matches.
-- =============================================

CREATE TABLE recno_idx_bm (
    id integer PRIMARY KEY,
    k integer
) USING recno;

CREATE INDEX idx_bm_k ON recno_idx_bm (k);

INSERT INTO recno_idx_bm SELECT i, i FROM generate_series(1, 500) i;

-- In-place UPDATE of the indexed key: row id=100 moves from k=100 to k=999.
UPDATE recno_idx_bm SET k = 999 WHERE id = 100;

SET enable_seqscan = off;
SET enable_indexscan = off;
SET enable_indexonlyscan = off;
-- Force a bitmap scan probing the OLD key: the stale entry must be rechecked
-- away, so this returns zero rows.
EXPLAIN (COSTS OFF) SELECT id, k FROM recno_idx_bm WHERE k = 100;
SELECT id, k FROM recno_idx_bm WHERE k = 100;
-- Probing the NEW key returns exactly the updated row.
SELECT id, k FROM recno_idx_bm WHERE k = 999;
RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_indexonlyscan;

DROP TABLE recno_idx_bm;

-- =============================================
-- Stale-entry recheck: lossy GiST/SP-GiST.  These AMs store approximate
-- (lossy) keys and are not equality-comparable (amconsistentequality is
-- false), so the stored-key recheck used for btree/hash cannot run, and a
-- qual recheck cannot dedup two entries that chain-walk to the same live
-- tuple.  For an in-place table AM the planner therefore forbids plain index,
-- index-only, and index-orderby (KNN) paths on such indexes and allows only a
-- bitmap heap scan, which collapses the stale (oldkey -> tid) and live
-- (newkey -> tid) entries to one TID and rechecks against the live tuple.
-- =============================================

CREATE TABLE recno_idx_lossy (
    id integer PRIMARY KEY,
    p point,
    r int4range
) USING recno;

CREATE INDEX idx_lossy_p_gist ON recno_idx_lossy USING gist (p);
CREATE INDEX idx_lossy_r_gist ON recno_idx_lossy USING gist (r);
CREATE INDEX idx_lossy_p_spgist ON recno_idx_lossy USING spgist (p);

INSERT INTO recno_idx_lossy
    SELECT i, point(i, i * 2), int4range(i, i + 10)
    FROM generate_series(1, 200) i;

-- In-place UPDATE of GiST/SP-GiST indexed columns: row id=50 moves its point
-- and range, leaving stale (oldkey -> tid) entries in all three indexes.
UPDATE recno_idx_lossy
    SET p = point(9999, 9999), r = int4range(5000, 5010)
    WHERE id = 50;

SET enable_seqscan = off;

-- A GiST containment qual must plan as a bitmap heap scan, not a plain index
-- scan, on this in-place table AM.
EXPLAIN (COSTS OFF)
SELECT count(*) FROM recno_idx_lossy WHERE r @> 55;
-- The moved row's old range (50,60) contained 55; it must not leak.  Exactly
-- nine live rows (ids 46-55, minus the moved id=50) contain 55.
SELECT count(*) FROM recno_idx_lossy WHERE r @> 55;
-- The new range (5000,5010) must be found.
SELECT id FROM recno_idx_lossy WHERE r @> 5005;

-- A KNN ORDER BY on the in-place GiST index cannot use an ordered index scan;
-- it falls back to a sort.  The moved row must not appear near its old point.
EXPLAIN (COSTS OFF)
SELECT id FROM recno_idx_lossy ORDER BY p <-> point(50, 100) LIMIT 3;
SELECT id FROM recno_idx_lossy ORDER BY p <-> point(50, 100) LIMIT 3;

-- An SP-GiST containment qual must likewise plan as a bitmap heap scan.
EXPLAIN (COSTS OFF)
SELECT count(*) FROM recno_idx_lossy
    WHERE p <@ box(point(0, 0), point(60, 120));
SELECT count(*) FROM recno_idx_lossy
    WHERE p <@ box(point(0, 0), point(60, 120));

RESET enable_seqscan;

DROP TABLE recno_idx_lossy;

-- Lossy overlapping-key duplicate: the moved row's OLD and NEW key both
-- satisfy the qual.  A qual recheck would return the row twice (both entries
-- chain-walk to the one live tuple and pass recheck); the bitmap scan dedups
-- the TID so the row appears exactly once.
CREATE TABLE recno_idx_lossy_ov (
    id integer PRIMARY KEY,
    r int4range
) USING recno;

CREATE INDEX idx_lossy_ov_gist ON recno_idx_lossy_ov USING gist (r);

INSERT INTO recno_idx_lossy_ov VALUES (1, int4range(50, 60)), (2, int4range(100, 110));

-- Move id=1 from (50,60) to (55,5000); both ranges contain 57.
UPDATE recno_idx_lossy_ov SET r = int4range(55, 5000) WHERE id = 1;

SET enable_seqscan = off;
-- Must return exactly one row, not two.
SELECT count(*) FROM recno_idx_lossy_ov WHERE r @> 57;
RESET enable_seqscan;

DROP TABLE recno_idx_lossy_ov;

-- =============================================
-- Cleanup
-- =============================================

DROP TABLE recno_idx_test;
