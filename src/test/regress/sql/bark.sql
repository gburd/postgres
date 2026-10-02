--
-- BARK index access method: registration, opclasses, build, insert, and scan.
--
-- BARK is registered as an index AM, provides default operator classes for
-- the common scalar types (its operator families are btree operator
-- families), builds an index over a heap, accepts row inserts into an
-- existing index (splitting pages and growing the tree), and supports index
-- scans whose results match a sequential scan.
--

-- The AM is registered in pg_am, with a valid index_am_handler.
SELECT amname, amtype FROM pg_am WHERE amname = 'bark';
SELECT amname, amhandler::regproc FROM pg_am WHERE amname = 'bark';

-- An unknown AM is still rejected outright (contrast with bark below).
CREATE TABLE bark_tab (a int, b text);
CREATE INDEX ON bark_tab USING nosuchbark (a);

-- BARK provides default operator classes, so CREATE INDEX resolves the
-- opclass and builds the index.
INSERT INTO bark_tab SELECT g, 'row' || g FROM generate_series(1, 2000) g;
CREATE INDEX bark_int_idx ON bark_tab USING bark (a);
CREATE INDEX bark_text_idx ON bark_tab USING bark (b);

-- The indexes exist and have pages (a meta page plus the tree).
SELECT c.relname AS index, pg_relation_size(c.oid) > 0 AS has_storage
FROM pg_index i
  JOIN pg_class c ON c.oid = i.indexrelid
WHERE i.indrelid = 'bark_tab'::regclass
ORDER BY c.relname;

-- Build over a tiny table (unsorted input) and an empty table both work.
CREATE TABLE bark_small (a int);
INSERT INTO bark_small VALUES (3), (1), (2);
CREATE INDEX ON bark_small USING bark (a);

CREATE TABLE bark_empty (a int);
CREATE INDEX ON bark_empty USING bark (a);

-- Multicolumn build.
CREATE INDEX bark_multi_idx ON bark_tab USING bark (a, b);

-- The planner avoids a BARK index for now (prohibitive cost estimate) and no
-- longer errors, so a query runs via a sequential scan.
SELECT count(*) FROM bark_tab WHERE a = 1000;

-- Insert into an existing index.  Building the index empty and then inserting
-- rows forces leaf splits and tree-height growth through the aminsert path.
CREATE TABLE bark_ins (a int);
CREATE INDEX bark_ins_idx ON bark_ins USING bark (a);	-- empty index first
INSERT INTO bark_ins SELECT g FROM generate_series(1, 5000) g;	-- ascending
INSERT INTO bark_ins SELECT g FROM generate_series(10000, 5001, -1) g;	-- descending
INSERT INTO bark_ins SELECT 42 FROM generate_series(1, 50) g;	-- duplicates
SELECT count(*) FROM bark_ins;							-- all rows present (seqscan)
SELECT pg_relation_size('bark_ins_idx') > 8192 * 2 AS grew_past_two_pages;

-- Inserting into a freshly built (non-empty) index also works.
INSERT INTO bark_tab VALUES (99999, 'late');
SELECT count(*) FROM bark_tab WHERE a = 99999;

-- Index scans.  With scanning implemented the planner can choose a BARK
-- index; its results must match a sequential scan over the same data.
CREATE TABLE bark_scan (a int);
INSERT INTO bark_scan SELECT (g * 7919) % 100000 FROM generate_series(1, 20000) g;
CREATE INDEX bark_scan_idx ON bark_scan USING bark (a);

SET enable_seqscan = off;
SET enable_bitmapscan = off;

-- A point lookup uses the BARK index.
EXPLAIN (COSTS OFF) SELECT a FROM bark_scan WHERE a = 7919;

-- Equality, range, and ordered scans agree with a sequential scan.
SELECT (SELECT count(*) FROM bark_scan WHERE a = 7919) AS eq_idx,
       (SELECT count(*) FROM bark_scan a WHERE a.a = 7919) AS eq_check;
SELECT count(*) AS range_idx FROM bark_scan WHERE a BETWEEN 1000 AND 2000;
SELECT a FROM bark_scan WHERE a BETWEEN 0 AND 30 ORDER BY a;

-- Differential against a sequential scan over the full key space.
SET enable_seqscan = on;
SET enable_indexscan = off;
WITH seq AS (SELECT count(*) c FROM bark_scan WHERE a BETWEEN 1000 AND 2000)
SELECT c AS range_seq FROM seq;

-- Index-only scans: BARK can return indexed columns without a heap fetch.
SET enable_seqscan = off;
SET enable_indexscan = on;
VACUUM (ANALYZE) bark_scan;					-- succeeds; sets the visibility map
EXPLAIN (COSTS OFF)
  SELECT a FROM bark_scan WHERE a BETWEEN 1000 AND 1010;
SELECT a FROM bark_scan WHERE a BETWEEN 1000 AND 1010 ORDER BY a;
SELECT count(a) AS ios_count FROM bark_scan WHERE a < 5000;
SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_indexonlyscan = off;
SELECT count(a) AS seq_count FROM bark_scan WHERE a < 5000;

RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;
RESET enable_indexonlyscan;

-- Uniqueness: a unique BARK index rejects duplicate keys, treats NULLs as
-- distinct, and catches duplicates both at insert time and at build time.
CREATE TABLE bark_uniq (a int);
CREATE UNIQUE INDEX bark_uniq_idx ON bark_uniq USING bark (a);
INSERT INTO bark_uniq VALUES (1), (2), (3);
INSERT INTO bark_uniq VALUES (2);				-- duplicate: errors
INSERT INTO bark_uniq VALUES (NULL), (NULL);		-- NULLs are distinct: ok
UPDATE bark_uniq SET a = 1 WHERE a = 3;			-- would duplicate: errors
SELECT a FROM bark_uniq WHERE a IS NOT NULL ORDER BY a;
SELECT count(*) AS total, count(a) AS non_null FROM bark_uniq;

-- A unique index built over data that already contains a duplicate fails.
CREATE TABLE bark_uniq_build (a int);
INSERT INTO bark_uniq_build VALUES (10), (20), (10);
CREATE UNIQUE INDEX ON bark_uniq_build USING bark (a);	-- errors at build

DROP TABLE bark_uniq, bark_uniq_build;

-- Backward scans: an ordered index serves ORDER BY DESC without a sort and
-- supports scrollable cursors fetching in both directions.
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexscan = on;
CREATE TABLE bark_bwd (a int);
INSERT INTO bark_bwd SELECT g FROM generate_series(1, 1000) g;
CREATE INDEX bark_bwd_idx ON bark_bwd USING bark (a);
EXPLAIN (COSTS OFF) SELECT a FROM bark_bwd WHERE a <= 5 ORDER BY a DESC;
SELECT a FROM bark_bwd WHERE a <= 5 ORDER BY a DESC;
SELECT a FROM bark_bwd ORDER BY a DESC LIMIT 5;
BEGIN;
DECLARE bark_cur SCROLL CURSOR FOR
  SELECT a FROM bark_bwd WHERE a BETWEEN 10 AND 20 ORDER BY a;
FETCH 3 FROM bark_cur;
FETCH BACKWARD 2 FROM bark_cur;
FETCH 2 FROM bark_cur;
COMMIT;
RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;
DROP TABLE bark_bwd;

-- INCLUDE columns: non-key payload rides along in leaf tuples and is returned
-- by index-only scans, but is physically truncated from pivots (internal
-- pages and high keys), which route by key only.  Uniqueness and ordering use
-- the key alone.
CREATE TABLE bark_inc (a int, b int, c text);
INSERT INTO bark_inc SELECT g, g * 2, md5(g::text) FROM generate_series(1, 2000) g;
CREATE INDEX bark_inc_idx ON bark_inc USING bark (a) INCLUDE (b, c);
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexscan = on;
EXPLAIN (COSTS OFF) SELECT a, b, c FROM bark_inc WHERE a = 100;
SELECT a, b, c FROM bark_inc WHERE a = 100;
SELECT sum(b) AS inc_sum FROM bark_inc WHERE a < 500;
-- A lower-bound (>=) scan on an INCLUDE index forms a bounded search key
-- covering all index attributes, not just the key column.
SELECT count(*) AS inc_ge FROM bark_inc WHERE a >= 1500;
-- A unique key with differing INCLUDE values still conflicts on the key alone.
CREATE TABLE bark_inc_u (a int, b int);
CREATE UNIQUE INDEX ON bark_inc_u USING bark (a) INCLUDE (b);
INSERT INTO bark_inc_u VALUES (1, 10);
INSERT INTO bark_inc_u VALUES (1, 99);			-- same key, different payload: errors
RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;
DROP TABLE bark_inc, bark_inc_u;

-- VACUUM: deleting rows then vacuuming removes the dead index entries, and
-- the index keeps answering scans correctly (including after the freed key
-- range is reused by new rows).
CREATE TABLE bark_vac (a int);
INSERT INTO bark_vac SELECT g FROM generate_series(1, 3000) g;
CREATE INDEX bark_vac_idx ON bark_vac USING bark (a);
DELETE FROM bark_vac WHERE a <= 1500;
VACUUM bark_vac;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexscan = on;
SELECT count(*) AS dead_gone FROM bark_vac WHERE a <= 1500;		-- 0
SELECT count(*) AS live FROM bark_vac WHERE a > 1500;			-- 1500
INSERT INTO bark_vac SELECT g FROM generate_series(1, 750) g;	-- reuse freed range
SELECT count(*) AS reused FROM bark_vac WHERE a <= 750;			-- 750
RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;
DROP TABLE bark_vac;

-- Equal keys spanning multiple leaves: when one key value has enough rows that
-- its entries fill more than one leaf page, a leaf split lands two leaves whose
-- downlinks carry the same (equal) key.  A forward equality or lower-bound
-- scan must descend to the FIRST such leaf, not the last, or it silently skips
-- the earlier duplicates.  Build the index over scattered duplicates (so the
-- bulk loader stores many single-locator entries per key, not one coalesced
-- entry) and check every per-key index count matches a sequential scan.
CREATE TABLE bark_dup (a int, b int);
INSERT INTO bark_dup SELECT g % 100, g FROM generate_series(1, 20000) g;
CREATE INDEX bark_dup_idx ON bark_dup USING bark (a);
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexscan = on;
-- A deep key (its run starts partway through the tree) is the regression case.
SELECT count(*) AS eq42_idx FROM bark_dup WHERE a = 42;
SELECT count(*) AS eq99_idx FROM bark_dup WHERE a = 99;
-- Capture every key's index-scan count (one equality scan per key).
CREATE TEMP TABLE bark_dup_idx_counts AS
  SELECT k, (SELECT count(*) FROM bark_dup WHERE a = k) AS c
  FROM generate_series(0, 99) k;
-- Compare against the sequential-scan counts: no key may differ.
SET enable_seqscan = on;
SET enable_indexscan = off;
SELECT count(*) AS key_count_mismatches
FROM bark_dup_idx_counts ic
  JOIN (SELECT a AS k, count(*) AS c FROM bark_dup GROUP BY a) sc
    ON ic.k = sc.k
WHERE ic.c <> sc.c;
RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;
DROP TABLE bark_dup;

DROP TABLE bark_tab, bark_small, bark_empty, bark_ins, bark_scan;
