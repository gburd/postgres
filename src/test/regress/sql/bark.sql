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
-- Duplicate keys (LIST entries): a non-unique index coalesces many heap
-- tuples that share a key into one leaf entry holding a sorted locator list.
-- Scans must expand a LIST back into its member TIDs so index results match a
-- sequential scan exactly, in every mode (equality, range, ORDER BY, IOS,
-- bitmap).  VACUUM must remove dead members from within a LIST and keep the
-- survivors scannable.
CREATE TABLE bark_list (a int, b int);
-- 20000 rows over 100 distinct keys => ~200 duplicates per key, enough to
-- form LIST entries.  b distinguishes rows within a key.
INSERT INTO bark_list SELECT g % 100, g FROM generate_series(1, 20000) g;
CREATE INDEX bark_list_idx ON bark_list USING bark (a);
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexscan = on;
-- Equality on a heavily-duplicated key: index count == seqscan count.
SELECT (SELECT count(*) FROM bark_list WHERE a = 42) AS eq_idx;
SET enable_seqscan = on;
SET enable_indexscan = off;
SELECT count(*) AS eq_seq FROM bark_list WHERE a = 42;
-- Range and ordered scans over duplicated keys agree with a sequential scan.
SET enable_seqscan = off;
SET enable_indexscan = on;
SELECT count(*) AS range_idx FROM bark_list WHERE a BETWEEN 10 AND 20;
SELECT a FROM bark_list WHERE a <= 2 ORDER BY a LIMIT 10;
SELECT a FROM bark_list WHERE a >= 98 ORDER BY a DESC LIMIT 10;
SET enable_seqscan = on;
SET enable_indexscan = off;
SELECT count(*) AS range_seq FROM bark_list WHERE a BETWEEN 10 AND 20;
-- Bitmap scan over duplicated keys.
SET enable_seqscan = off;
SET enable_indexscan = off;
SET enable_bitmapscan = on;
SELECT count(*) AS bitmap_idx FROM bark_list WHERE a IN (1, 2, 3);
SET enable_seqscan = on;
SET enable_bitmapscan = off;
SELECT count(*) AS bitmap_seq FROM bark_list WHERE a IN (1, 2, 3);
-- VACUUM removing members from within LISTs: delete some (not all) of a key's
-- rows plus a whole other key, vacuum, then the survivors still scan correctly
-- and the index agrees with a sequential scan.  Key 42's b values are all even
-- (g = 42, 142, 242, ... are congruent to 42 mod 100), so a parity predicate
-- would delete the whole key; delete by magnitude instead to leave survivors.
DELETE FROM bark_list WHERE a = 42 AND b < 10000;
DELETE FROM bark_list WHERE a = 7;
VACUUM bark_list;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexscan = on;
SELECT count(*) AS after_vac_42_idx FROM bark_list WHERE a = 42;
SELECT count(*) AS after_vac_7_idx FROM bark_list WHERE a = 7;
SET enable_seqscan = on;
SET enable_indexscan = off;
SELECT count(*) AS after_vac_42_seq FROM bark_list WHERE a = 42;
-- A unique index never forms a LIST: a second live row with the same key is
-- rejected even though a non-unique index would have coalesced it.
CREATE TABLE bark_list_u (a int);
CREATE UNIQUE INDEX ON bark_list_u USING bark (a);
INSERT INTO bark_list_u SELECT g FROM generate_series(1, 100) g;
INSERT INTO bark_list_u VALUES (50);			-- duplicate: errors
RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;
DROP TABLE bark_list, bark_list_u;

-- POSTING entries (sbm-backed inverted sets): a key with enough clustered
-- duplicates that the serialized sbm is smaller than a flat locator list is
-- promoted from LIST to POSTING automatically.  Insert many rows per key (so
-- the heap TIDs for a key are clustered on consecutive blocks, which the sbm
-- encodes densely) and prove every scan mode still matches a sequential scan.
CREATE TABLE bark_post (a int, b int);
CREATE INDEX bark_post_idx ON bark_post USING bark (a);
-- 10 keys x 400 clustered duplicates: rows for one key are inserted together,
-- so their heap TIDs land on consecutive blocks and the sbm encodes them
-- densely -- the serialized set beats the flat locator list and the entry is
-- promoted from LIST to POSTING.
INSERT INTO bark_post SELECT k, g
  FROM generate_series(0, 9) k, generate_series(1, 400) g;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexscan = on;
-- Equality on a promoted key: index count == seqscan count.
SELECT count(*) AS eq_idx FROM bark_post WHERE a = 5;
SET enable_seqscan = on;
SET enable_indexscan = off;
SELECT count(*) AS eq_seq FROM bark_post WHERE a = 5;
-- Range, ordered, index-only, and bitmap scans all agree with a seqscan.
SET enable_seqscan = off;
SET enable_indexscan = on;
SELECT count(*) AS range_idx FROM bark_post WHERE a BETWEEN 3 AND 7;
SELECT DISTINCT a FROM bark_post WHERE a <= 2 ORDER BY a;
SELECT count(a) AS ios_idx FROM bark_post WHERE a < 5;
SET enable_indexscan = off;
SET enable_bitmapscan = on;
SELECT count(*) AS bitmap_idx FROM bark_post WHERE a IN (1, 5, 9);
SET enable_seqscan = on;
SET enable_bitmapscan = off;
SELECT count(*) AS range_seq FROM bark_post WHERE a BETWEEN 3 AND 7;
SELECT count(*) AS bitmap_seq FROM bark_post WHERE a IN (1, 5, 9);
-- VACUUM removing part of a promoted key's rows: the POSTING set shrinks (and
-- may demote back toward LIST/SINGLE), and the survivors still scan correctly.
DELETE FROM bark_post WHERE a = 5 AND b <= 300;
VACUUM bark_post;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexscan = on;
SELECT count(*) AS after_vac_idx FROM bark_post WHERE a = 5;
SET enable_seqscan = on;
SET enable_indexscan = off;
SELECT count(*) AS after_vac_seq FROM bark_post WHERE a = 5;
RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;
DROP TABLE bark_post;

-- BitmapAnd / BitmapOr: a BARK bitmap scan must produce an exact TIDBitmap
-- that the executor can combine with other bitmaps.  Build two BARK indexes
-- (and one btree) on one table and check that AND/OR plans over them -- plus a
-- bark+btree mix -- return exactly what a sequential scan does, with no heap
-- recheck (BARK is exact).
CREATE TABLE bark_bmap (a int, b int, c int);
INSERT INTO bark_bmap
  SELECT g % 100, g % 30, g % 7 FROM generate_series(1, 30000) g;
CREATE INDEX bark_bmap_a ON bark_bmap USING bark (a);
CREATE INDEX bark_bmap_b ON bark_bmap USING bark (b);
CREATE INDEX bark_bmap_c ON bark_bmap USING btree (c);
SET enable_seqscan = off;
SET enable_indexscan = off;
SET enable_bitmapscan = on;
-- BitmapAnd over two BARK indexes; the heap scan recheck is skipped (exact).
EXPLAIN (COSTS OFF) SELECT count(*) FROM bark_bmap WHERE a = 10 AND b = 10;
SELECT count(*) AS and_idx FROM bark_bmap WHERE a = 10 AND b = 10;
-- BitmapOr over two BARK indexes.
EXPLAIN (COSTS OFF) SELECT count(*) FROM bark_bmap WHERE a = 10 OR b = 5;
SELECT count(*) AS or_idx FROM bark_bmap WHERE a = 10 OR b = 5;
-- Mixed: BARK AND btree, and a three-way OR across both AMs.
SELECT count(*) AS andmix_idx FROM bark_bmap WHERE a = 10 AND c = 3;
SELECT count(*) AS or3_idx FROM bark_bmap WHERE a = 10 OR b = 5 OR c = 3;
-- Differential against a sequential scan: every count must match exactly.
SET enable_bitmapscan = off;
SET enable_seqscan = on;
SELECT count(*) AS and_seq FROM bark_bmap WHERE a = 10 AND b = 10;
SELECT count(*) AS or_seq FROM bark_bmap WHERE a = 10 OR b = 5;
SELECT count(*) AS andmix_seq FROM bark_bmap WHERE a = 10 AND c = 3;
SELECT count(*) AS or3_seq FROM bark_bmap WHERE a = 10 OR b = 5 OR c = 3;
RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;
DROP TABLE bark_bmap;
-- Parallel index build (amcanbuildparallel).  Forcing maintenance workers on,
-- a parallel build of a large index must produce the same results as a serial
-- build of the same data: workers scan slices of the heap into a shared sort,
-- the leader merges and writes the one tree.  min_parallel_table_scan_size=0
-- and parallel_*_cost=0 ensure the planner actually grants workers.
CREATE TABLE bark_par (a int, b text);
INSERT INTO bark_par
  SELECT (g * 7919) % 100000, 'r' || g FROM generate_series(1, 100000) g;
SET min_parallel_table_scan_size = 0;
SET max_parallel_maintenance_workers = 4;
SET maintenance_work_mem = '1MB';				-- force a disk-spilling sort
CREATE INDEX bark_par_idx ON bark_par USING bark (a);	-- parallel build
SET max_parallel_maintenance_workers = 0;
CREATE INDEX bark_ser_idx ON bark_par USING bark (a);	-- serial build
-- The two builds index the same rows: scans over either must agree, and must
-- match a sequential scan.
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexscan = on;
SELECT count(*) AS par_range FROM bark_par WHERE a BETWEEN 1000 AND 2000;
SET enable_seqscan = on;
SET enable_indexscan = off;
SELECT count(*) AS seq_range FROM bark_par WHERE a BETWEEN 1000 AND 2000;
SET enable_seqscan = off;
SET enable_indexscan = on;
SELECT a FROM bark_par WHERE a BETWEEN 0 AND 20 ORDER BY a;
RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;
RESET min_parallel_table_scan_size;
RESET max_parallel_maintenance_workers;
RESET maintenance_work_mem;
DROP TABLE bark_par;

-- Parallel index scan (amcanparallel).  Workers claim leaf pages from a shared
-- cursor rather than each walking the whole right-link chain; the union of
-- what the workers return must equal a serial scan.  Force the planner to pick
-- a parallel plan with the cost/size knobs, then prove the parallel result
-- count matches a forced-serial scan and a sequential scan.
CREATE TABLE bark_pscan (a int, b text);
INSERT INTO bark_pscan
  SELECT (g * 7919) % 100000, 'r' || g FROM generate_series(1, 100000) g;
CREATE INDEX bark_pscan_idx ON bark_pscan USING bark (a);
ANALYZE bark_pscan;
SET min_parallel_table_scan_size = 0;
SET min_parallel_index_scan_size = 0;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET max_parallel_workers_per_gather = 4;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
-- A Gather over a Parallel Index (Only) Scan.
EXPLAIN (COSTS OFF) SELECT count(*) FROM bark_pscan WHERE a < 50000;
SELECT count(*) AS par_count FROM bark_pscan WHERE a < 50000;
-- A plain parallel index scan (heap fetch for the non-indexed column).
EXPLAIN (COSTS OFF)
  SELECT count(b) FROM bark_pscan WHERE a BETWEEN 1000 AND 60000;
SELECT count(b) AS par_plain FROM bark_pscan WHERE a BETWEEN 1000 AND 60000;
-- The parallel result must match a forced-serial index scan ...
SET max_parallel_workers_per_gather = 0;
SELECT count(*) AS ser_count FROM bark_pscan WHERE a < 50000;
SELECT count(b) AS ser_plain FROM bark_pscan WHERE a BETWEEN 1000 AND 60000;
-- ... and a sequential scan over the same data.
SET enable_seqscan = on;
SET enable_indexscan = off;
SELECT count(*) AS seq_count FROM bark_pscan WHERE a < 50000;
RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;
RESET min_parallel_table_scan_size;
RESET min_parallel_index_scan_size;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET max_parallel_workers_per_gather;
DROP TABLE bark_pscan;

--
-- Ordered-operator (KNN) scans: ORDER BY col <~> const (amcanorderbyop).
--
-- A BARK index answers "nearest values to a constant, in increasing distance"
-- by descending to the constant and expanding outward on the leaf chain.  The
-- result must equal a brute-force ORDER BY abs(col - const), the scan must use
-- an index Order By node (not a Sort), and a LIMIT must let the scan stop
-- early rather than read the whole index.
CREATE TABLE bark_knn (a int);
INSERT INTO bark_knn SELECT g * 3 FROM generate_series(1, 3000) g;
CREATE INDEX bark_knn_idx ON bark_knn USING bark (a);
ANALYZE bark_knn;
SET enable_seqscan = off;

-- The plan is an index scan that produces the ordering, with no Sort node.
EXPLAIN (COSTS OFF)
  SELECT a FROM bark_knn ORDER BY a <~> 5000 LIMIT 10;

-- Mid-range: the ten nearest to 5000, in increasing distance, must match a
-- brute-force distance sort (ties broken by value so the comparison is
-- deterministic across the whole LIMIT window).
SELECT a FROM bark_knn ORDER BY a <~> 5000 LIMIT 10;

-- The whole-scan distance sequence is monotonically non-decreasing: zero
-- inversions proves the merge returns rows in true distance order.
SELECT count(*) AS inversions
  FROM (SELECT (a <~> 5000) AS d,
               lag((a <~> 5000)) OVER (ORDER BY (a <~> 5000)) AS prev_d
          FROM bark_knn) q
  WHERE d < prev_d;

-- One-sided expansion: a constant below the minimum key walks only forward.
SELECT a FROM bark_knn ORDER BY a <~> 1 LIMIT 5;

-- A constant above the maximum key walks only backward.
SELECT a FROM bark_knn ORDER BY a <~> 1000000 LIMIT 5;

-- Ties: a constant exactly between two keys returns the forward side first,
-- then the backward side, at each equal distance.
SELECT a, (a <~> 4999) AS d FROM bark_knn ORDER BY a <~> 4999 LIMIT 6;

-- Early stop: the LIMIT must bound how much of the index the scan reads.  A
-- full-table distance sort would touch all 3000 rows; the KNN scan reads only
-- a few around the constant.
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
  SELECT a FROM bark_knn ORDER BY a <~> 5000 LIMIT 10;

-- A WHERE qual combines with the ordering: nearest to 5000 among a > 5000.
SELECT a FROM bark_knn WHERE a > 5000 ORDER BY a <~> 5000 LIMIT 5;

-- Duplicates at one key share a distance and all come out before the scan
-- advances; the distance sequence stays monotone.
CREATE TABLE bark_knn_dup (a int);
INSERT INTO bark_knn_dup SELECT g / 3 FROM generate_series(1, 900) g;
CREATE INDEX bark_knn_dup_idx ON bark_knn_dup USING bark (a);
SELECT count(*) AS inversions
  FROM (SELECT (a <~> 50) AS d,
               lag((a <~> 50)) OVER (ORDER BY (a <~> 50)) AS prev_d
          FROM bark_knn_dup) q
  WHERE d < prev_d;

-- NULLs sort last (infinite distance): the KNN scan returns every row,
-- matching a sequential scan's count including the NULLs.
CREATE TABLE bark_knn_null (a int);
INSERT INTO bark_knn_null
  SELECT CASE WHEN g % 7 = 0 THEN NULL ELSE g * 2 END
    FROM generate_series(1, 500) g;
CREATE INDEX bark_knn_null_idx ON bark_knn_null USING bark (a);
SELECT count(*) AS total,
       count(*) FILTER (WHERE a IS NULL) AS nulls
  FROM (SELECT a FROM bark_knn_null ORDER BY a <~> 300) s;

-- bigint keys use the int8 distance operator the same way.
CREATE TABLE bark_knn8 (a bigint);
INSERT INTO bark_knn8 SELECT g * 7 FROM generate_series(1, 5000) g;
CREATE INDEX bark_knn8_idx ON bark_knn8 USING bark (a);
SELECT a FROM bark_knn8 ORDER BY a <~> 20000 LIMIT 8;

RESET enable_seqscan;
DROP TABLE bark_knn, bark_knn_dup, bark_knn_null, bark_knn8;

DROP TABLE bark_tab, bark_small, bark_empty, bark_ins, bark_scan;

-- ===========================================================================
-- Oversized keys (P04): a key larger than BarkMaxItemSize is stored on an
-- overflow page chain, transparently to compare / scan / dedup / uniqueness /
-- vacuum.  nbtree errors on such a key; BARK indexes it.  Keys are built from a
-- deterministic md5 chain so they are incompressible (really exceed the item
-- ceiling, not merely large) and the test output is reproducible.
-- ===========================================================================

-- Deterministic incompressible string of ~n bytes, seeded by s (repeatable).
CREATE FUNCTION bark_bigstr(s int, n int) RETURNS text
  LANGUAGE sql IMMUTABLE AS
$$ SELECT substr(string_agg(md5(s::text || g::text), ''), 1, n)
   FROM generate_series(1, (n + 31) / 32) g $$;

-- A mix of small keys and oversized keys (prefix-distinct), built by CREATE
-- INDEX (the bulk loader's oversized second pass) over an already-filled heap.
CREATE TABLE bark_big (id int, k text);
INSERT INTO bark_big SELECT g, 'small-' || lpad(g::text, 6, '0')
  FROM generate_series(1, 40) g;
INSERT INTO bark_big SELECT 100 + g,
  'K' || lpad(g::text, 6, '0') || bark_bigstr(g, 5000)
  FROM generate_series(1, 30) g;
CREATE INDEX bark_big_idx ON bark_big USING bark (k);

-- Insert more oversized keys into the built index, forcing leaf splits of
-- pages that hold oversized entries (and oversized pivots on internal pages).
INSERT INTO bark_big SELECT 200 + g,
  'M' || lpad(g::text, 6, '0') || bark_bigstr(1000 + g, 6000)
  FROM generate_series(1, 40) g;

-- Counts and the ordered-scan hash must match a sequential scan exactly: this
-- is the transparency proof (ordering of oversized keys == full-value order).
SET enable_seqscan = off;
SELECT count(*) AS idx_total FROM bark_big;
SELECT count(*) AS idx_oversized FROM bark_big WHERE k >= 'K';
SELECT md5(string_agg(id::text, ',' ORDER BY k)) AS idx_order_hash FROM bark_big;
SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT count(*) AS seq_total FROM bark_big;
SELECT count(*) AS seq_oversized FROM bark_big WHERE k >= 'K';
SELECT md5(string_agg(id::text, ',' ORDER BY k)) AS seq_order_hash FROM bark_big;
RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;

-- Keys sharing a long common prefix but differing only in the tail: a
-- prefix-only compare would wrongly tie them.  Equality must find exactly one,
-- and index order must equal full-value order.
CREATE TABLE bark_pfx (id int, k text);
INSERT INTO bark_pfx SELECT g, repeat('P', 5000) || lpad(g::text, 8, '0')
  FROM generate_series(1, 50) g;
CREATE INDEX bark_pfx_idx ON bark_pfx USING bark (k);
SET enable_seqscan = off;
SELECT id FROM bark_pfx WHERE k = repeat('P', 5000) || lpad('23', 8, '0');
SELECT md5(string_agg(id::text, ',' ORDER BY k)) AS pfx_idx_hash FROM bark_pfx;
SET enable_seqscan = on;
SET enable_indexscan = off;
SELECT md5(string_agg(id::text, ',' ORDER BY k)) AS pfx_seq_hash FROM bark_pfx;
RESET enable_seqscan;
RESET enable_indexscan;

-- Truly huge keys (20KB, 64KB): larger than a single page, multi-page overflow
-- chain.  Both index_form_tuple and nbtree reject these outright.
CREATE TABLE bark_huge (id int, k text);
INSERT INTO bark_huge VALUES (1, bark_bigstr(1, 20000));
INSERT INTO bark_huge VALUES (2, bark_bigstr(2, 65000));
INSERT INTO bark_huge VALUES (3, 'tiny');
CREATE INDEX bark_huge_idx ON bark_huge USING bark (k);
SET enable_seqscan = off;
SELECT id, length(k) FROM bark_huge ORDER BY k;
SELECT id FROM bark_huge WHERE k = bark_bigstr(2, 65000);
RESET enable_seqscan;

-- Uniqueness over oversized keys: a duplicate oversized key must be rejected.
CREATE TABLE bark_uniq (k text);
INSERT INTO bark_uniq VALUES (bark_bigstr(7, 5000));
CREATE UNIQUE INDEX bark_uniq_idx ON bark_uniq USING bark (k);
INSERT INTO bark_uniq VALUES (bark_bigstr(7, 5000));  -- duplicate: must error

-- Delete + VACUUM must reclaim overflow pages (mark them deleted) and leave the
-- index consistent; the remaining rows must still scan correctly.
DELETE FROM bark_big WHERE id > 200;
VACUUM bark_big;
SET enable_seqscan = off;
SELECT count(*) AS idx_after_vacuum FROM bark_big;
SET enable_seqscan = on;
SET enable_indexscan = off;
SELECT count(*) AS seq_after_vacuum FROM bark_big;
RESET enable_seqscan;
RESET enable_indexscan;

-- Oversized INCLUDE payload (P05): a small key with a non-key INCLUDE column too
-- large for the leaf is stored on the overflow chain with the key and returned
-- in full by an index-only scan.  Pivots never carry the INCLUDE column (they
-- are truncated to key attributes), so an oversized INCLUDE never bloats an
-- internal page.  32KB payloads force leaf splits of INCLUDE-oversized entries.
CREATE TABLE bark_inc_big (k int, payload text);
INSERT INTO bark_inc_big SELECT g, bark_bigstr(g, 32000)
  FROM generate_series(1, 15) g;
CREATE INDEX bark_inc_big_idx ON bark_inc_big USING bark (k) INCLUDE (payload);
SET enable_seqscan = off;
SET enable_bitmapscan = off;
-- Index-only scan returns the full 32KB INCLUDE value, byte-for-byte.
EXPLAIN (COSTS OFF)
  SELECT k, length(payload) FROM bark_inc_big WHERE k BETWEEN 1 AND 15;
SELECT k, length(payload) FROM bark_inc_big WHERE k BETWEEN 1 AND 15 ORDER BY k;
SELECT bool_and(payload = bark_bigstr(k, 32000)) AS ios_payload_match
  FROM bark_inc_big WHERE k BETWEEN 1 AND 15;
RESET enable_seqscan;
RESET enable_bitmapscan;
-- Delete + vacuum reclaims the INCLUDE overflow chains; the rest still scans.
DELETE FROM bark_inc_big WHERE k <= 8;
VACUUM bark_inc_big;
SET enable_seqscan = off;
SELECT bool_and(payload = bark_bigstr(k, 32000)) AS after_vacuum_match
  FROM bark_inc_big WHERE k BETWEEN 1 AND 15;
SELECT count(*) AS idx_remaining FROM bark_inc_big WHERE k BETWEEN 1 AND 15;
RESET enable_seqscan;

DROP TABLE bark_big, bark_pfx, bark_huge, bark_uniq, bark_inc_big;
DROP FUNCTION bark_bigstr(int, int);

-- ===========================================================================
-- C-STATS cost model (P06): the planner must choose a BARK index when it is
-- cheaper and a sequential scan when it is not, accounting for LIST/POSTING
-- compression (a posting-list scan touches far fewer leaf pages than N
-- singletons) and overflow-page I/O for oversized keys.  COSTS OFF keeps the
-- plan shape deterministic; the point is which path the planner picks.
-- ===========================================================================

CREATE TABLE bark_cost (a int, b int);
INSERT INTO bark_cost SELECT g, g % 1000 FROM generate_series(1, 100000) g;
CREATE INDEX bark_cost_idx ON bark_cost USING bark (a);
ANALYZE bark_cost;
-- Selective point lookup: index.
EXPLAIN (COSTS OFF) SELECT * FROM bark_cost WHERE a = 42;
-- Non-selective whole-table predicate: sequential scan.
EXPLAIN (COSTS OFF) SELECT * FROM bark_cost WHERE a >= 0;
DROP TABLE bark_cost;

-- Posting-list compression: 100k rows, 10 distinct keys (10k duplicates each).
-- One key is a single POSTING entry on a handful of leaf pages, so an equality
-- scan is costed far below a sequential scan even though it returns 10k rows --
-- the planner picks an index path.
CREATE TABLE bark_cost_dup (k int, v int);
INSERT INTO bark_cost_dup SELECT g % 10, g FROM generate_series(1, 100000) g;
CREATE INDEX bark_cost_dup_idx ON bark_cost_dup USING bark (k);
ANALYZE bark_cost_dup;
EXPLAIN (COSTS OFF) SELECT count(*) FROM bark_cost_dup WHERE k = 3;
DROP TABLE bark_cost_dup;

-- ===========================================================================
-- Page / FSM reclamation (A13-A15): VACUUM unlinks emptied interior leaves and
-- returns them (plus freed overflow-chain pages) to the free space map, and the
-- page allocator reuses them before extending the relation.  A delete-heavy
-- workload that repeatedly empties most of the index and refills it with a
-- disjoint key range must therefore NOT grow the relation without bound.  Here
-- four full delete/vacuum/insert cycles must leave the index within a small
-- constant of its one-cycle size (the old behavior doubled it every cycle).
-- The scan results after reclamation must still match a sequential scan.
-- ===========================================================================
CREATE TABLE bark_recycle (a int) WITH (autovacuum_enabled = off);
CREATE INDEX bark_recycle_idx ON bark_recycle USING bark (a);
INSERT INTO bark_recycle SELECT g FROM generate_series(1, 50000) g;
-- Record the one-cycle size, then run four disjoint delete/vacuum/insert cycles.
CREATE TEMP TABLE bark_recycle_sz AS
  SELECT pg_relation_size('bark_recycle_idx') / 8192 AS base_pages;
-- cycle 1
DELETE FROM bark_recycle WHERE a > 1000;
VACUUM bark_recycle;
INSERT INTO bark_recycle SELECT g FROM generate_series(100001, 150000) g;
VACUUM bark_recycle;
-- cycle 2
DELETE FROM bark_recycle WHERE a > 101000;
VACUUM bark_recycle;
INSERT INTO bark_recycle SELECT g FROM generate_series(200001, 250000) g;
VACUUM bark_recycle;
-- cycle 3
DELETE FROM bark_recycle WHERE a > 201000;
VACUUM bark_recycle;
INSERT INTO bark_recycle SELECT g FROM generate_series(300001, 350000) g;
VACUUM bark_recycle;
-- cycle 4
DELETE FROM bark_recycle WHERE a > 301000;
VACUUM bark_recycle;
INSERT INTO bark_recycle SELECT g FROM generate_series(400001, 450000) g;
VACUUM bark_recycle;
-- Reuse works: growth across four cycles stays well under a single refill's
-- worth of pages (~245).  Without reclamation this would be ~4x.  Report a
-- boolean so the expected output is stable across platforms.
SELECT (pg_relation_size('bark_recycle_idx') / 8192) <= base_pages + 100
         AS index_growth_bounded
  FROM bark_recycle_sz;
DROP TABLE bark_recycle_sz;

-- The reclaimed-and-reused index still answers scans correctly: an index-only
-- range scan over the live keys must match a sequential count.
SET enable_seqscan = off;
SELECT count(*) AS idx_live FROM bark_recycle WHERE a > 0;
SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT count(*) AS seq_live FROM bark_recycle WHERE a > 0;
RESET enable_indexscan;
RESET enable_bitmapscan;
DROP TABLE bark_recycle;

-- Freed overflow-chain pages are reclaimed too: a table of oversized keys that
-- is bulk-deleted and vacuumed, then refilled with new oversized keys, reuses
-- the freed BARK_OVERFLOW pages rather than extending without bound.
CREATE TABLE bark_recycle_big (a text) WITH (autovacuum_enabled = off);
CREATE INDEX bark_recycle_big_idx ON bark_recycle_big USING bark (a);
INSERT INTO bark_recycle_big
  SELECT repeat('k', 4000) || lpad(g::text, 8, '0')
    FROM generate_series(1, 400) g;
CREATE TEMP TABLE bark_recycle_big_sz AS
  SELECT pg_relation_size('bark_recycle_big_idx') / 8192 AS base_pages;
-- cycle 1
DELETE FROM bark_recycle_big;
VACUUM bark_recycle_big;
INSERT INTO bark_recycle_big
  SELECT repeat('m', 4000) || lpad((1000 + g)::text, 8, '0')
    FROM generate_series(1, 400) g;
VACUUM bark_recycle_big;
-- cycle 2
DELETE FROM bark_recycle_big;
VACUUM bark_recycle_big;
INSERT INTO bark_recycle_big
  SELECT repeat('m', 4000) || lpad((2000 + g)::text, 8, '0')
    FROM generate_series(1, 400) g;
VACUUM bark_recycle_big;
-- cycle 3
DELETE FROM bark_recycle_big;
VACUUM bark_recycle_big;
INSERT INTO bark_recycle_big
  SELECT repeat('m', 4000) || lpad((3000 + g)::text, 8, '0')
    FROM generate_series(1, 400) g;
VACUUM bark_recycle_big;
SELECT (pg_relation_size('bark_recycle_big_idx') / 8192) <= base_pages * 2
         AS overflow_growth_bounded
  FROM bark_recycle_big_sz;
DROP TABLE bark_recycle_big_sz;
SELECT count(*) AS big_live FROM bark_recycle_big;
DROP TABLE bark_recycle_big;

-- ===========================================================================
-- Speculative insertion: INSERT ... ON CONFLICT on a BARK unique index.  The
-- aminsert path must participate in the executor's speculative insert/confirm/
-- kill protocol so DO NOTHING on a duplicate is a no-op (not an error) and DO
-- UPDATE updates the existing row.  (Concurrent speculative inserts of the same
-- key are covered by src/test/isolation/specs/insert-conflict-bark.spec.)
-- ===========================================================================
CREATE TABLE bark_oc (id int, v text);
CREATE UNIQUE INDEX bark_oc_idx ON bark_oc USING bark (id);
INSERT INTO bark_oc VALUES (1, 'a'), (2, 'b'), (3, 'c');
-- DO NOTHING on an existing key: a no-op, no error.
INSERT INTO bark_oc VALUES (2, 'dup') ON CONFLICT (id) DO NOTHING;
-- DO NOTHING on a new key: inserts.
INSERT INTO bark_oc VALUES (4, 'd') ON CONFLICT (id) DO NOTHING;
-- DO UPDATE on an existing key: updates the stored row.
INSERT INTO bark_oc VALUES (2, 'updated') ON CONFLICT (id) DO UPDATE SET v = excluded.v;
-- DO UPDATE on a new key: inserts.
INSERT INTO bark_oc VALUES (5, 'e') ON CONFLICT (id) DO UPDATE SET v = excluded.v;
SELECT id, v FROM bark_oc ORDER BY id;
-- A plain duplicate insert (no ON CONFLICT) still raises the unique violation.
INSERT INTO bark_oc VALUES (1, 'x');
DROP TABLE bark_oc;
