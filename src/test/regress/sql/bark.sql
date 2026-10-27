--
-- BARK index access method: registration, opclasses, build, insert, and scan.
--
-- BARK is registered as an index AM, provides an operator class for every
-- btree operator class (its operator families are btree operator
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
-- The planner gives a build no workers below 32MB of maintenance_work_mem
-- each, unless the table's parallel_workers says how many.
ALTER TABLE bark_par SET (parallel_workers = 4);
CREATE INDEX bark_par_idx ON bark_par USING bark (a);	-- parallel build
ALTER TABLE bark_par RESET (parallel_workers);
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
-- Parallel merge join and LIMIT read only part of the scan.  Workers must not
-- wait for one another while a consumer has stopped pulling (that hung the
-- base code), and the results must equal a serial plan's.
SET statement_timeout = '30s';
CREATE TABLE bark_pscan2 (a int);
INSERT INTO bark_pscan2 SELECT g FROM generate_series(1, 100000, 2) g;
CREATE INDEX bark_pscan2_idx ON bark_pscan2 USING bark (a);
ANALYZE bark_pscan2;
SET min_parallel_table_scan_size = 0;
SET min_parallel_index_scan_size = 0;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET max_parallel_workers_per_gather = 4;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_hashjoin = off;
SET enable_nestloop = off;
SELECT count(*) AS par_mj FROM bark_pscan JOIN bark_pscan2 USING (a);
SELECT sum(a) AS par_limit FROM
  (SELECT a FROM bark_pscan WHERE a > 10 ORDER BY a LIMIT 100) x;
SELECT sum(a) AS par_limit_desc FROM
  (SELECT a FROM bark_pscan WHERE a < 90000 ORDER BY a DESC LIMIT 100) x;
SET max_parallel_workers_per_gather = 0;
SELECT count(*) AS ser_mj FROM bark_pscan JOIN bark_pscan2 USING (a);
SELECT sum(a) AS ser_limit FROM
  (SELECT a FROM bark_pscan WHERE a > 10 ORDER BY a LIMIT 100) x;
SELECT sum(a) AS ser_limit_desc FROM
  (SELECT a FROM bark_pscan WHERE a < 90000 ORDER BY a DESC LIMIT 100) x;
RESET enable_seqscan;
RESET enable_bitmapscan;
RESET enable_hashjoin;
RESET enable_nestloop;
RESET min_parallel_table_scan_size;
RESET min_parallel_index_scan_size;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET max_parallel_workers_per_gather;
RESET statement_timeout;
DROP TABLE bark_pscan2;
DROP TABLE bark_pscan;

-- Scroll cursors over a multi-page index: every FETCH direction, including
-- backward after the scan ran off the end, must return what a btree index
-- over the same data returns.
CREATE TABLE bark_scroll (a int, pad text);
INSERT INTO bark_scroll SELECT g, repeat('x', 200) FROM generate_series(1, 3000) g;
CREATE INDEX bark_scroll_idx ON bark_scroll USING bark (a);
ANALYZE bark_scroll;
CREATE TABLE bark_scroll_out (am text, n int, a int);
CREATE FUNCTION bark_scroll_run(am text) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  c refcursor := 'bark_scroll_c';
  v int;
  n int := 0;
  cmds text[] := ARRAY['NEXT', 'NEXT', 'NEXT', 'NEXT', 'NEXT', 'PRIOR', 'PRIOR',
                       'PRIOR', 'ABSOLUTE 1500', 'RELATIVE -700', 'LAST',
                       'PRIOR', 'PRIOR', 'FIRST', 'LAST', 'NEXT', 'PRIOR',
                       'PRIOR', 'NEXT', 'NEXT', 'ABSOLUTE 2', 'PRIOR', 'PRIOR',
                       'NEXT'];
  cmd text;
BEGIN
  OPEN c SCROLL FOR SELECT a FROM bark_scroll WHERE a > 100 ORDER BY a;
  FOREACH cmd IN ARRAY cmds LOOP
    EXECUTE format('FETCH %s FROM bark_scroll_c', cmd) INTO v;
    n := n + 1;
    INSERT INTO bark_scroll_out VALUES (am, n, v);
  END LOOP;
  CLOSE c;
END $$;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = off;
SELECT bark_scroll_run('bark');
SET enable_indexonlyscan = on;
SELECT bark_scroll_run('bark ios');
DROP INDEX bark_scroll_idx;
CREATE INDEX bark_scroll_bt ON bark_scroll USING btree (a);
SELECT bark_scroll_run('btree');
RESET enable_seqscan;
RESET enable_bitmapscan;
RESET enable_indexonlyscan;
SELECT b.n, b.a AS bark, i.a AS bark_ios, t.a AS btree
FROM bark_scroll_out b
  JOIN bark_scroll_out i ON i.am = 'bark ios' AND i.n = b.n
  JOIN bark_scroll_out t ON t.am = 'btree' AND t.n = b.n
WHERE b.am = 'bark' AND (b.a IS DISTINCT FROM t.a OR i.a IS DISTINCT FROM t.a);
SELECT string_agg(coalesce(a::text, 'none'), ',' ORDER BY n) AS fetched
FROM bark_scroll_out WHERE am = 'bark';
DROP FUNCTION bark_scroll_run(text);
DROP TABLE bark_scroll, bark_scroll_out;

-- Bounds that the base scan got wrong (known bug 10): a DESC column, and
-- cross-type keys whose constant is outside the column type's range.  Each
-- index count must equal a sequential scan's.
CREATE TABLE bark_bnd (a int);
INSERT INTO bark_bnd SELECT g FROM generate_series(-20000, 20000) g;
CREATE INDEX bark_bnd_desc ON bark_bnd USING bark (a DESC);
CREATE TABLE bark_bnd_q (q text);
INSERT INTO bark_bnd_q VALUES
  ('a > 5'), ('a >= 100'), ('a < 100'), ('a <= -100'), ('a BETWEEN -10 AND 10'),
  ('a > -4294967295::bigint'), ('a < 3000000000::bigint AND a > 5'),
  ('a > 3000000000::bigint'), ('a < -3000000000::bigint'),
  ('a = 42::bigint'), ('a = 42::smallint'), ('a >= 7::smallint AND a < 9::bigint');
CREATE FUNCTION bark_bnd_check(tbl text) RETURNS TABLE (q text, idx bigint, seq bigint)
LANGUAGE plpgsql AS $$
DECLARE
  r record;
BEGIN
  FOR r IN SELECT bark_bnd_q.q FROM bark_bnd_q LOOP
    q := r.q;
    SET LOCAL enable_seqscan = off;
    SET LOCAL enable_bitmapscan = off;
    EXECUTE format('SELECT count(*) FROM %I WHERE %s', tbl, r.q) INTO idx;
    SET LOCAL enable_seqscan = on;
    SET LOCAL enable_indexscan = off;
    SET LOCAL enable_indexonlyscan = off;
    EXECUTE format('SELECT count(*) FROM %I WHERE %s', tbl, r.q) INTO seq;
    RESET enable_seqscan;
    RESET enable_indexscan;
    RESET enable_indexonlyscan;
    RESET enable_bitmapscan;
    RETURN NEXT;
  END LOOP;
END $$;
SELECT * FROM bark_bnd_check('bark_bnd') WHERE idx <> seq;
SELECT count(*) AS queries FROM bark_bnd_check('bark_bnd');
DROP INDEX bark_bnd_desc;
CREATE INDEX bark_bnd_asc ON bark_bnd USING bark (a);
SELECT * FROM bark_bnd_check('bark_bnd') WHERE idx <> seq;
-- Backward scans stop at a lower bound and start at an upper one.
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT string_agg(a::text, ',') AS bwd FROM
  (SELECT a FROM bark_bnd WHERE a BETWEEN 10 AND 15 ORDER BY a DESC) x;
RESET enable_seqscan;
RESET enable_bitmapscan;
-- Cross-type keys position and stop the scan through the opfamily's ORDER
-- procs (int2, int4 and int8 against each other), so a point lookup on an
-- int8 column with an int4 constant reads a few pages, not the index.
CREATE TABLE bark_xt (a8 int8, a4 int4, a2 int2);
INSERT INTO bark_xt SELECT g, g, (g % 30000)::int2
  FROM generate_series(-200000, 200000) g;
CREATE INDEX bark_xt_a8 ON bark_xt USING bark (a8);
CREATE INDEX bark_xt_a4 ON bark_xt USING bark (a4 DESC);
CREATE INDEX bark_xt_a2 ON bark_xt USING bark (a2);
VACUUM ANALYZE bark_xt;
TRUNCATE bark_bnd_q;
INSERT INTO bark_bnd_q VALUES
  ('a8 = 42'), ('a8 = 42::int2'), ('a8 > 199990'), ('a8 < -199990::int4'),
  ('a8 BETWEEN 5::int2 AND 9::int4'), ('a8 > 3000000000::int8'),
  ('a4 = 42::int8'), ('a4 = 42::int2'), ('a4 > -4294967295::int8'),
  ('a4 < 3000000000::int8 AND a4 > 5'), ('a4 > 3000000000::int8'),
  ('a4 < -3000000000::int8'), ('a4 >= 199995::int8'), ('a4 = 4294967338::int8'),
  ('a2 = 42'), ('a2 = 42::int8'), ('a2 > 29990::int8'), ('a2 > 40000::int4');
SELECT * FROM bark_bnd_check('bark_xt') WHERE idx <> seq;
CREATE FUNCTION bark_xt_buffers(q text) RETURNS int LANGUAGE plpgsql AS $$
DECLARE
  warm refcursor;
  line text;
  n int := 0;
BEGIN
  SET LOCAL enable_seqscan = off;
  SET LOCAL enable_bitmapscan = off;
  -- The scan runs here, not in a parallel worker whose caches are cold,
  -- with the index open and its caches warm (an open cursor keeps them
  -- under RELCACHE_FORCE_RELEASE), so only the scan's own pages count.
  SET LOCAL debug_parallel_query = off;
  OPEN warm FOR EXECUTE q;
  MOVE warm;
  FOR line IN EXECUTE 'EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF, SUMMARY OFF) ' || q LOOP
    IF line ~ 'Buffers: shared' AND n = 0 THEN
      n := coalesce(substring(line from 'hit=([0-9]+)')::int, 0) +
           coalesce(substring(line from 'read=([0-9]+)')::int, 0);
    END IF;
  END LOOP;
  RETURN n;
END $$;
SELECT bark_xt_buffers('SELECT a8 FROM bark_xt WHERE a8 = 42') < 20 AS int8_eq_int4_positions,
       bark_xt_buffers('SELECT a4 FROM bark_xt WHERE a4 = 42::int8') < 20 AS int4_eq_int8_positions,
       bark_xt_buffers('SELECT a4 FROM bark_xt WHERE a4 < 1000::int8 ORDER BY a4 DESC LIMIT 10') < 20 AS desc_upper_positions,
       bark_xt_buffers('SELECT a8 FROM bark_xt WHERE a8 < 1000 ORDER BY a8 DESC LIMIT 10') < 20 AS backward_upper_positions;
DROP FUNCTION bark_xt_buffers(text);
DROP TABLE bark_xt;
DROP FUNCTION bark_bnd_check(text);
DROP TABLE bark_bnd, bark_bnd_q;

-- Row comparisons filter in value order (known bug 11 crashed the backend),
-- on ASC and mixed-direction indexes, and in a KNN scan.  A KNN scan also
-- filters by SAOP membership (it used to treat the array as a scalar).
CREATE TABLE bark_rowc (a int, b int);
INSERT INTO bark_rowc SELECT g / 10, g % 10 FROM generate_series(1, 5000) g;
CREATE INDEX bark_rowc_ab ON bark_rowc USING bark (a, b);
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) AS rc_gt FROM bark_rowc WHERE (a, b) > (100, 5);
SELECT count(*) AS rc_le FROM bark_rowc WHERE (a, b) <= (3, 2);
SELECT count(*) AS rc_knn FROM
  (SELECT a FROM bark_rowc WHERE (a, b) > (100, 5) ORDER BY a <~> 120 LIMIT 10) x;
SELECT string_agg(a::text, ',' ORDER BY a) AS knn_saop FROM
  (SELECT DISTINCT a FROM (SELECT a FROM bark_rowc WHERE a = ANY ('{5,7,9}')
   ORDER BY a <~> 6 LIMIT 30) y) x;
DROP INDEX bark_rowc_ab;
CREATE INDEX bark_rowc_abd ON bark_rowc USING bark (a, b DESC);
SELECT count(*) AS rc_gt_desc FROM bark_rowc WHERE (a, b) > (100, 5);
SELECT count(*) AS rc_le_desc FROM bark_rowc WHERE (a, b) <= (3, 2);
RESET enable_seqscan;
RESET enable_bitmapscan;
SET enable_indexscan = off;
SET enable_indexonlyscan = off;
SET enable_bitmapscan = off;
SELECT count(*) AS seq_rc_gt FROM bark_rowc WHERE (a, b) > (100, 5);
SELECT count(*) AS seq_rc_le FROM bark_rowc WHERE (a, b) <= (3, 2);
RESET enable_indexscan;
RESET enable_indexonlyscan;
RESET enable_bitmapscan;
DROP TABLE bark_rowc;

DROP TABLE bark_tab, bark_small, bark_empty, bark_ins, bark_scan;

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

-- The index correlation comes from the leading column's statistics (or an
-- expression index's own), as for btree.  On a table loaded in key order a
-- range scan reads the heap in order and is a plain index scan, as btree's
-- is; on one loaded in random order both take a bitmap scan.  30000 rows, so
-- that ANALYZE reads them all; SELECT * keeps index-only scans, and with them
-- the visibility map, out of the plans; temp tables keep autovacuum away.
CREATE TEMP TABLE bark_corr AS
  SELECT g AS a, md5(g::text) AS pad FROM generate_series(1, 30000) g;
SELECT setseed(0.5);
CREATE TEMP TABLE bark_uncorr AS SELECT * FROM bark_corr ORDER BY random();
CREATE INDEX bark_corr_idx ON bark_corr USING bark (a);
CREATE INDEX bark_corr_expr_idx ON bark_corr USING bark ((a + 1));
CREATE INDEX bark_uncorr_idx ON bark_uncorr USING bark (a);
ANALYZE bark_corr, bark_uncorr;
EXPLAIN (COSTS OFF) SELECT * FROM bark_corr WHERE a BETWEEN 1000 AND 6000;
EXPLAIN (COSTS OFF) SELECT * FROM bark_corr WHERE a + 1 BETWEEN 1000 AND 6000;
EXPLAIN (COSTS OFF) SELECT * FROM bark_uncorr WHERE a BETWEEN 1000 AND 6000;
DROP INDEX bark_corr_idx, bark_corr_expr_idx, bark_uncorr_idx;
CREATE INDEX bark_corr_idx ON bark_corr USING btree (a);
CREATE INDEX bark_corr_expr_idx ON bark_corr USING btree ((a + 1));
CREATE INDEX bark_uncorr_idx ON bark_uncorr USING btree (a);
ANALYZE bark_corr, bark_uncorr;
EXPLAIN (COSTS OFF) SELECT * FROM bark_corr WHERE a BETWEEN 1000 AND 6000;
EXPLAIN (COSTS OFF) SELECT * FROM bark_corr WHERE a + 1 BETWEEN 1000 AND 6000;
EXPLAIN (COSTS OFF) SELECT * FROM bark_uncorr WHERE a BETWEEN 1000 AND 6000;
DROP TABLE bark_corr, bark_uncorr;

-- Only the quals that bound a scan decide how many entries it reads, and a
-- partial index's share of its rows comes from the heap's row count, as for
-- btree.  A scan of bark_part_ac reads all of its 3000 entries, since c is
-- not its leading column, to return the 150 rows that bark_part_bc reaches
-- directly; costing every qual as a bound, and the predicate's selectivity
-- against the partial index's own entry count as well, would put that scan
-- at 15 entries, below bark_part_bc's 150, and prefer it.  a is a permutation of 1..30000, so that
-- neither index has a heap correlation the other lacks.
CREATE TEMP TABLE bark_part (a int, b int, c int);
INSERT INTO bark_part
  SELECT (g * 7907) % 30000, g % 10, (g / 10) % 1000
  FROM generate_series(1, 30000) g;
CREATE INDEX bark_part_bc ON bark_part USING bark (b, c);
CREATE INDEX bark_part_ac ON bark_part USING bark (a, c) WHERE b = 0;
ANALYZE bark_part;
EXPLAIN (COSTS OFF) SELECT * FROM bark_part WHERE b = 0 AND c BETWEEN 1 AND 50;
DROP INDEX bark_part_bc, bark_part_ac;
CREATE INDEX bark_part_bc ON bark_part USING btree (b, c);
CREATE INDEX bark_part_ac ON bark_part USING btree (a, c) WHERE b = 0;
EXPLAIN (COSTS OFF) SELECT * FROM bark_part WHERE b = 0 AND c BETWEEN 1 AND 50;
DROP TABLE bark_part;

-- Equality arrays bound the scan on every column of its equality prefix, in
-- either direction, with one descent per combination of their elements, as
-- for btree.  Costing an array past column 1 as a filter would charge these
-- scans the whole a group (half the index) and prefer a sequential scan.
CREATE TEMP TABLE bark_cost_arr (a int, b int, c text);
INSERT INTO bark_cost_arr
  SELECT g % 2, (g / 2) % 1000, repeat('x', 100) || g
  FROM generate_series(1, 50000) g;
CREATE INDEX bark_cost_arr_idx ON bark_cost_arr USING bark (a, b, c);
VACUUM ANALYZE bark_cost_arr;
EXPLAIN (COSTS OFF)
  SELECT * FROM bark_cost_arr WHERE a = 1 AND b IN (751, 84, 417);
EXPLAIN (COSTS OFF)
  SELECT * FROM bark_cost_arr WHERE a = 1 AND b IN (751, 84, 417)
  ORDER BY b DESC;
EXPLAIN (COSTS OFF)
  SELECT * FROM bark_cost_arr WHERE a IN (1, 0) AND b = 751;
DROP INDEX bark_cost_arr_idx;
CREATE INDEX bark_cost_arr_idx ON bark_cost_arr USING btree (a, b, c);
EXPLAIN (COSTS OFF)
  SELECT * FROM bark_cost_arr WHERE a = 1 AND b IN (751, 84, 417);
EXPLAIN (COSTS OFF)
  SELECT * FROM bark_cost_arr WHERE a = 1 AND b IN (751, 84, 417)
  ORDER BY b DESC;
EXPLAIN (COSTS OFF)
  SELECT * FROM bark_cost_arr WHERE a IN (1, 0) AND b = 751;
DROP TABLE bark_cost_arr;

-- ===========================================================================
-- Page / FSM reclamation (A13-A15): VACUUM unlinks emptied interior leaves and
-- returns them (plus freed overflow-chain pages) to the free space map, and the
-- page allocator reuses them before extending the relation.  A delete-heavy
-- workload that repeatedly empties most of the index and refills it with a
-- disjoint key range must therefore NOT grow the relation without bound.
--
-- A deleted page is reused only once every transaction that might still hold
-- a link to it has ended (its safexid), as in nbtree.  Each cycle below
-- deletes pages (first VACUUM), refills, and vacuums again; the refill's
-- transaction ends after the deletion, so the second VACUUM records the
-- deleted pages in the free space map, and the next cycle's refill reuses
-- them.  Only a refill that finds no recorded pages extends the index, and
-- that can happen once: in cycle 1, or in cycle 2 if another session's commit
-- let cycle 1's refill reuse its own cycle's pages.  So four cycles must
-- leave the index within one refill (about base_pages) of its starting size;
-- without reuse each cycle adds one.  On 8kB pages the index has 139 pages,
-- then 275, 278, 281 and 284 after each cycle.  Temp tables, so that other
-- sessions' snapshots do not hold back reuse.  The scan results after
-- reclamation must still match a sequential scan.
-- ===========================================================================
CREATE TEMP TABLE bark_recycle (a int);
CREATE INDEX bark_recycle_idx ON bark_recycle USING bark (a);
INSERT INTO bark_recycle SELECT g FROM generate_series(1, 50000) g;
-- Record the starting size, then run four disjoint cycles.
CREATE TEMP TABLE bark_recycle_sz AS
  SELECT pg_relation_size('bark_recycle_idx') / current_setting('block_size')::int AS base_pages;
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
-- Reuse works: growth across four cycles stays within one refill's worth of
-- pages.  Report a boolean so the expected output is stable across platforms.
SELECT (pg_relation_size('bark_recycle_idx') / 8192) <= 2 * base_pages + 20
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
-- the freed BARK_OVERFLOW pages rather than extending without bound.  As
-- above, the first refill cannot reuse them yet (on 8kB pages: 5 pages, then
-- 7, 8 and 8), and a temp table keeps other sessions' snapshots out of it.
CREATE TEMP TABLE bark_recycle_big (a text);
CREATE INDEX bark_recycle_big_idx ON bark_recycle_big USING bark (a);
INSERT INTO bark_recycle_big
  SELECT repeat('k', 4000) || lpad(g::text, 8, '0')
    FROM generate_series(1, 400) g;
CREATE TEMP TABLE bark_recycle_big_sz AS
  SELECT pg_relation_size('bark_recycle_big_idx') / current_setting('block_size')::int AS base_pages;
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
SELECT (pg_relation_size('bark_recycle_big_idx') / current_setting('block_size')::int) <= base_pages * 2
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

-- ===========================================================================
-- Incremental LIST/POSTING coalesce: building one key's duplicate set by many
-- single-row inserts must stay fast (O(1) amortized for the common append
-- case) and exactly correct.  Inserting 10000 rows that all share one key used
-- to be O(N^2) (each coalesce re-read, re-sorted and re-serialized the whole
-- set); the append fast path makes it quick.  Here we only assert correctness
-- -- count and ordering match a sequential scan -- and the test completing in
-- the regress run at all is the speed proof (the O(N^2) version took ~40s).
-- ===========================================================================
CREATE TABLE bark_coalesce (k int, seq int);
CREATE INDEX bark_coalesce_idx ON bark_coalesce USING bark (k);
INSERT INTO bark_coalesce SELECT 42, g FROM generate_series(1, 10000) g;
-- Mixed-order inserts into a second key exercise the non-append general path.
INSERT INTO bark_coalesce SELECT 7, g FROM generate_series(1, 2000) g
  ORDER BY random();
-- Count via index must match a sequential count for both keys.
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT k, count(*) AS idx_count FROM bark_coalesce WHERE k IN (7, 42)
  GROUP BY k ORDER BY k;
SET enable_seqscan = on;
SET enable_indexscan = off;
SELECT k, count(*) AS seq_count FROM bark_coalesce WHERE k IN (7, 42)
  GROUP BY k ORDER BY k;
-- Ordering: the index returns the heap TIDs for key 42 in ascending ctid order,
-- matching a sorted sequential scan (compared as a digest so the output is
-- stable regardless of the actual block numbers assigned).
SET enable_indexscan = on;
SET enable_seqscan = off;
SELECT md5(string_agg(ctid::text, ',')) =
       (SELECT md5(string_agg(ctid::text, ','))
          FROM (SELECT ctid FROM bark_coalesce WHERE k = 42 ORDER BY ctid) s)
         AS idx_order_matches_sorted
  FROM (SELECT ctid FROM bark_coalesce WHERE k = 42) t;
RESET enable_indexscan;
RESET enable_bitmapscan;
RESET enable_seqscan;
DROP TABLE bark_coalesce;
-- ScalarArrayOp (SAOP): `col = ANY(array)` / `col IN (...)` pushed into a
-- single BARK index scan (amsearcharray).  The scan sorts and de-duplicates
-- the array into index order and visits the matching keys in order, so the
-- result exactly matches a sequential scan and ORDER BY stays correct.  The
-- array must show up as an Index Cond, not a filter.
-- ===========================================================================
CREATE TABLE bark_saop (a int, b int);
-- 500 distinct keys, 20 dups each (POSTING-heavy), spanning several leaves.
INSERT INTO bark_saop SELECT g % 500, g FROM generate_series(1, 10000) g;
CREATE INDEX bark_saop_idx ON bark_saop USING bark (a);
ANALYZE bark_saop;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
-- The array is an Index Cond on the BARK scan, not a Filter.
EXPLAIN (COSTS OFF)
  SELECT count(*) FROM bark_saop WHERE a IN (3, 7, 42, 499);
EXPLAIN (COSTS OFF)
  SELECT count(*) FROM bark_saop WHERE a = ANY('{3,7,42,499}');
-- Counts match a seqscan: present values, absent values, mix, edges.
SELECT a, count(*) FROM bark_saop WHERE a IN (3, 7, 42, 499) GROUP BY a ORDER BY a;
SELECT count(*) AS absent_only FROM bark_saop WHERE a IN (500, 501, -1);
SELECT count(*) AS mix FROM bark_saop WHERE a IN (3, 500);
-- Unsorted + duplicate array input is handled (sort+dedup) and equals a seqscan.
SELECT count(*) AS unsorted_dup FROM bark_saop WHERE a = ANY('{499,42,7,3,3,42}');
-- Empty array returns nothing.
SELECT count(*) AS empty_arr FROM bark_saop WHERE a = ANY('{}'::int[]);
-- ORDER BY stays correct with an array qual (no Sort node; the scan is ordered).
EXPLAIN (COSTS OFF)
  SELECT a FROM bark_saop WHERE a IN (499, 3, 42, 7) ORDER BY a LIMIT 5;
SELECT a FROM bark_saop WHERE a IN (499, 3, 42, 7) ORDER BY a LIMIT 5;
-- SAOP combined with an ordinary qual filters correctly.
SELECT count(*) AS saop_and_qual
  FROM bark_saop WHERE a IN (3, 7) AND b > 5000;
-- Content equality against a seqscan over an array spanning many leaves.
SELECT (SELECT md5(string_agg(a||':'||b, ',' ORDER BY a, b))
          FROM bark_saop WHERE a = ANY('{10,250,499,3,7}'))
     = (SELECT md5(string_agg(a||':'||b, ',' ORDER BY a, b))
          FROM (SELECT a, b FROM bark_saop) s WHERE a = ANY('{10,250,499,3,7}'))
       AS saop_matches_full_set;
RESET enable_seqscan;
RESET enable_bitmapscan;

-- Multi-column SAOP: an array on the leading column and/or a trailing column.
-- The leading-column lower bound is a one-attribute pivot (minus-infinity on
-- the trailing columns), so the descent lands at the first match and does not
-- overshoot the run of equal leading keys.
CREATE TABLE bark_saop_mc (a int, b int);
INSERT INTO bark_saop_mc SELECT (g / 300) % 50, g % 30
  FROM generate_series(1, 60000) g;
CREATE INDEX bark_saop_mc_idx ON bark_saop_mc USING bark (a, b);
ANALYZE bark_saop_mc;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF)
  SELECT count(*) FROM bark_saop_mc WHERE a IN (3, 7, 40) AND b IN (5, 15);
-- Plain multi-column leading equality must also descend to the first match
-- (this exercises the one-attribute-pivot lower bound directly).
SELECT count(*) AS plain_mc_eq FROM bark_saop_mc WHERE a = 7;
SELECT count(*) AS plain_mc_eq2 FROM bark_saop_mc WHERE a = 7 AND b = 15;
SELECT a, b, count(*) FROM bark_saop_mc
  WHERE a IN (3, 7, 40) AND b IN (5, 15) GROUP BY a, b ORDER BY a, b;
SELECT count(*) AS lead_array_sec_eq
  FROM bark_saop_mc WHERE a IN (3, 7) AND b = 15;
SELECT count(*) AS lead_eq_sec_array
  FROM bark_saop_mc WHERE a = 7 AND b IN (5, 15, 25);
RESET enable_seqscan;
RESET enable_bitmapscan;

DROP TABLE bark_saop, bark_saop_mc;

-- ===========================================================================
-- Sharper scan positioning: a multi-column lower bound uses every usable
-- leading column (WHERE a = k AND b >= m descends near (k,m), not to the first
-- a = k leaf), and a forward scan terminates once the leading column passes an
-- upper bound (=, <, <=) instead of filtering the rest of the index.  The bar
-- is correctness (== seqscan); EXPLAIN (BUFFERS) is omitted because its page
-- counts are not reproducible, but the queries below would read the whole
-- index without the optimization.
-- ===========================================================================
CREATE TABLE bark_pos (a int, b int);
-- 5 distinct a-values, 2000 b-values each: an a-run spans several leaves, so a
-- second-column bound that starts at the first a-row would read many pages.
INSERT INTO bark_pos SELECT g / 2000, g % 2000 FROM generate_series(0, 9999) g;
CREATE INDEX bark_pos_idx ON bark_pos USING bark (a, b);
ANALYZE bark_pos;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
-- Multi-column lower bound: a = 3 AND b >= 1990 lands near the match.
SELECT count(*) AS mc_lb FROM bark_pos WHERE a = 3 AND b >= 1990;
SELECT a, b FROM bark_pos WHERE a = 3 AND b >= 1997 ORDER BY a, b;
-- a = 2 AND b BETWEEN 500 AND 509 (lower bound + per-tuple upper filter).
SELECT count(*) AS mc_between FROM bark_pos WHERE a = 2 AND b BETWEEN 500 AND 509;
-- Content equality against a seqscan for the multi-column bound.
SELECT (SELECT md5(string_agg(a||':'||b, ',' ORDER BY a, b))
          FROM bark_pos WHERE a = 1 AND b >= 1500)
     = (SELECT md5(string_agg(a||':'||b, ',' ORDER BY a, b))
          FROM (SELECT a, b FROM bark_pos) s WHERE a = 1 AND b >= 1500)
       AS mc_lb_matches;
RESET enable_seqscan;
RESET enable_bitmapscan;

-- Upper-bound early termination on a single-column index.
CREATE TABLE bark_ub (a int);
INSERT INTO bark_ub SELECT g FROM generate_series(1, 20000) g;
CREATE INDEX bark_ub_idx ON bark_ub USING bark (a);
ANALYZE bark_ub;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
-- < , <= and = all stop the forward scan early; results equal a seqscan.
SELECT count(*) AS ub_lt FROM bark_ub WHERE a < 10;
SELECT count(*) AS ub_le FROM bark_ub WHERE a <= 100;
SELECT count(*) AS ub_eq FROM bark_ub WHERE a = 12345;
SELECT count(*) AS ub_range FROM bark_ub WHERE a >= 5000 AND a <= 5010;
-- Early termination also applies to bitmap scans.
RESET enable_bitmapscan;
SET enable_indexscan = off;
SET enable_indexonlyscan = off;
SET enable_bitmapscan = on;
SELECT count(*) AS ub_bitmap FROM bark_ub WHERE a <= 50;
RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_indexonlyscan;
RESET enable_bitmapscan;

-- DESC index: the index-order upper bound is >, >=, = (physical order flipped),
-- so a forward scan over a DESC index also terminates early and stays correct.
CREATE TABLE bark_desc (a int);
INSERT INTO bark_desc SELECT g FROM generate_series(1, 20000) g;
CREATE INDEX bark_desc_idx ON bark_desc USING bark (a DESC);
ANALYZE bark_desc;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) AS desc_gt FROM bark_desc WHERE a > 19990;
SELECT a FROM bark_desc WHERE a > 19995 ORDER BY a DESC;
SELECT count(*) AS desc_lt FROM bark_desc WHERE a < 10;
RESET enable_seqscan;
RESET enable_bitmapscan;

DROP TABLE bark_pos, bark_ub, bark_desc;

-- KNN paths the scan can answer.  An ordering operator on a later column of
-- a multicolumn BARK index gets no KNN path (the scan walks outward in the
-- leading column's order only), and a SCROLL cursor over a KNN scan is
-- materialized, since the scan cannot run its distance order backward.  The
-- KNN scan's descent counts as an index search.
CREATE TABLE bark_knnx (a int, b int);
INSERT INTO bark_knnx SELECT g % 10, g FROM generate_series(1, 2000) g;
CREATE INDEX bark_knnx_ab ON bark_knnx USING bark (a, b);
ANALYZE bark_knnx;
SET enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT b FROM bark_knnx ORDER BY b <~> 1000 LIMIT 5;
SELECT string_agg(b::text, ',' ORDER BY b <~> 1000, b) AS nearest_b FROM
  (SELECT b FROM bark_knnx ORDER BY b <~> 1000 LIMIT 5) x;
-- Two ordering operators on the leading column: one goes to the index, the
-- second is an incremental sort.
EXPLAIN (COSTS OFF)
  SELECT a FROM bark_knnx ORDER BY a <~> 5, a <~> 7 LIMIT 3;
CREATE TABLE bark_knnsc (a int);
INSERT INTO bark_knnsc SELECT g FROM generate_series(1, 2000) g;
CREATE INDEX bark_knnsc_a ON bark_knnsc USING bark (a);
ANALYZE bark_knnsc;
EXPLAIN (COSTS OFF)
  DECLARE c SCROLL CURSOR FOR SELECT a FROM bark_knnsc ORDER BY a <~> 500;
BEGIN;
DECLARE c SCROLL CURSOR FOR SELECT a FROM bark_knnsc ORDER BY a <~> 500;
FETCH 3 FROM c;
FETCH BACKWARD 1 FROM c;
FETCH 2 FROM c;
COMMIT;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
  SELECT a, ctid FROM bark_knnsc ORDER BY a <~> 500 LIMIT 3;
RESET enable_seqscan;
DROP TABLE bark_knnx, bark_knnsc;

-- ScalarArrayOp with cross-type arrays and inequality operators.  An array
-- whose element type differs from the column's (int8 elements on an int4
-- column) is sorted with the element type's own comparator and compared
-- with column values through the cross-type one; comparing through the
-- column's comparator would truncate 4294967301 to 5.  An inequality array
-- (a < ANY(...)) is a single bound on its extreme element, not a membership
-- test.  bark_saop_check runs each query as an index (or index-only) scan,
-- as a bitmap scan and as a sequential scan, and says ok when all three
-- return the same rows in the same order.
CREATE TABLE bark_xt (a int4, b int8);
INSERT INTO bark_xt SELECT g, g FROM generate_series(1, 100000) g;
CREATE INDEX bark_xt_a ON bark_xt USING bark (a);
CREATE INDEX bark_xt_b ON bark_xt USING bark (b);
CREATE TABLE bark_xt_mc (a int4, c int);
INSERT INTO bark_xt_mc SELECT g, g % 3 FROM generate_series(1, 100000) g;
CREATE INDEX bark_xt_mc_ac ON bark_xt_mc USING bark (a, c);
CREATE TABLE bark_xt_desc (a int4, b int8);
INSERT INTO bark_xt_desc SELECT g, g FROM generate_series(1, 100000) g;
CREATE INDEX bark_xt_desc_a ON bark_xt_desc USING bark (a DESC);
VACUUM ANALYZE bark_xt, bark_xt_mc, bark_xt_desc;
CREATE FUNCTION bark_saop_check(q text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  how text;
  res text[] := '{}';
  cnt text[] := '{}';
  r record;
BEGIN
  FOREACH how IN ARRAY ARRAY['index', 'bitmap', 'seq'] LOOP
    PERFORM set_config('enable_seqscan', (how = 'seq')::text, true);
    PERFORM set_config('enable_indexscan', (how = 'index')::text, true);
    PERFORM set_config('enable_indexonlyscan', (how = 'index')::text, true);
    PERFORM set_config('enable_bitmapscan', (how = 'bitmap')::text, true);
    EXECUTE 'SELECT count(*) AS n, md5(string_agg(x::text, '','')) AS h FROM ('
      || q || ') x' INTO r;
    res := res || coalesce(r.h, '');
    cnt := cnt || r.n::text;
  END LOOP;
  IF res[1] = res[3] AND res[2] = res[3] THEN
    RETURN 'ok';
  END IF;
  RETURN format('mismatch: index %s, bitmap %s, seq %s rows',
                cnt[1], cnt[2], cnt[3]);
END $$;
-- The array is an Index Cond of the bark scan.
SET enable_seqscan = off;
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF)
  SELECT a, b FROM bark_xt WHERE a = ANY('{4294967301,7}'::int8[]) ORDER BY a;
EXPLAIN (COSTS OFF)
  SELECT a, b FROM bark_xt WHERE a < ANY('{5,7}'::int8[]) ORDER BY a;
RESET enable_bitmapscan;
RESET enable_seqscan;
SELECT bark_saop_check('SELECT a, b FROM bark_xt WHERE a = ANY(''{4294967301,7}''::int8[]) ORDER BY a');
SELECT bark_saop_check('SELECT a, b FROM bark_xt WHERE a = ANY(''{-4294967291}''::int8[]) ORDER BY a');
SELECT bark_saop_check('SELECT a, b FROM bark_xt WHERE a < ANY(''{5,7}''::int8[]) ORDER BY a');
SELECT bark_saop_check('SELECT a, b FROM bark_xt WHERE a <= ANY(''{5,7}''::int4[]) ORDER BY a');
SELECT bark_saop_check('SELECT a, b FROM bark_xt WHERE a >= ANY(''{99998,99990}''::int4[]) ORDER BY a');
SELECT bark_saop_check('SELECT a, b FROM bark_xt WHERE a > ANY(''{99990,3000000000}''::int8[]) ORDER BY a');
SELECT bark_saop_check('SELECT a, b FROM bark_xt WHERE a > ANY(''{99000,3000000000}''::int8[]) AND a < ANY(''{99010,5}''::int4[]) ORDER BY a');
SELECT bark_saop_check('SELECT a, b FROM bark_xt WHERE b = ANY(''{5,7,32767}''::int2[]) ORDER BY b');
SELECT bark_saop_check('SELECT a, b FROM bark_xt WHERE a = ANY(ARRAY[NULL, 7]::int8[]) ORDER BY a');
SELECT bark_saop_check('SELECT a, b FROM bark_xt WHERE a < ANY(ARRAY[NULL, 3]::int8[]) ORDER BY a');
SELECT bark_saop_check('SELECT a, b FROM bark_xt WHERE a > ANY(''{}''::int8[]) ORDER BY a');
SELECT bark_saop_check('SELECT a, b FROM bark_xt WHERE a > ANY(ARRAY[NULL]::int8[]) ORDER BY a');
-- A runtime array, re-evaluated on each rescan: the first is all NULL (the
-- key stays an empty array), the later ones reduce to plain bounds.
SELECT bark_saop_check('SELECT v.i, x.a FROM (VALUES (1, ARRAY[NULL]::int8[]), (2, ''{5,3}''::int8[]), (3, ''{}''::int8[]), (4, ''{4294967296,-1}''::int8[])) v(i, arr), LATERAL (SELECT a FROM bark_xt WHERE a < ANY(v.arr) ORDER BY a LIMIT 3) x ORDER BY v.i, x.a');
-- Index-only and backward scans of a cross-type array.
SELECT bark_saop_check('SELECT a FROM bark_xt WHERE a = ANY(''{3000000000,-3000000000,4294967303,5,99999}''::int8[]) ORDER BY a');
SELECT bark_saop_check('SELECT a, b FROM bark_xt WHERE a = ANY(''{3000000000,-3000000000,4294967303,5,99999}''::int8[]) ORDER BY a DESC');
SELECT bark_saop_check('SELECT a FROM bark_xt WHERE a = ANY(''{3000000000,-3000000000,4294967303,5,99999}''::int8[]) ORDER BY a DESC');
-- Multicolumn: 200 int8 elements spread over the index, 20 of them beyond
-- int4's range, drive the leading-array cursor and its re-descents; also an
-- inequality array on the second column.
SELECT bark_saop_check(format(
  'SELECT a, c FROM bark_xt_mc WHERE a = ANY(%L::int8[]) AND c = 1 ORDER BY a',
  (SELECT array_agg(CASE WHEN g % 10 = 0 THEN 4294967296 + g * 487 + 1
                    ELSE g * 487 END)
     FROM generate_series(1, 200) g)));
SELECT bark_saop_check(format(
  'SELECT a, c FROM bark_xt_mc WHERE a = ANY(%L::int8[]) AND c <= ANY(''{0,1}''::int8[]) ORDER BY a',
  (SELECT array_agg(CASE WHEN g % 10 = 0 THEN 4294967296 + g * 487 + 1
                    ELSE g * 487 END)
     FROM generate_series(1, 200) g)));
SELECT bark_saop_check('SELECT a, c FROM bark_xt_mc WHERE c > ANY(''{1,0}''::int8[]) AND a < 3000 ORDER BY a, c');
SELECT bark_saop_check('SELECT a, c FROM bark_xt_mc WHERE c >= ANY(''{1,9}''::int8[]) ORDER BY a, c');
-- DESC index, read forward (ORDER BY a DESC) and backward.
SELECT bark_saop_check('SELECT a, b FROM bark_xt_desc WHERE a = ANY(''{4294967301,7,-4294967291,99999,3}''::int8[]) ORDER BY a DESC');
SELECT bark_saop_check('SELECT a, b FROM bark_xt_desc WHERE a = ANY(''{4294967301,7,-4294967291,99999,3}''::int8[]) ORDER BY a');
SELECT bark_saop_check('SELECT a FROM bark_xt_desc WHERE a < ANY(''{5,7}''::int8[]) ORDER BY a DESC');
SELECT bark_saop_check('SELECT a FROM bark_xt_desc WHERE a >= ANY(''{99998,99990}''::int4[]) ORDER BY a DESC');
DROP FUNCTION bark_saop_check(text);
DROP TABLE bark_xt, bark_xt_mc, bark_xt_desc;

-- ===========================================================================
-- Operator classes.  BARK has one operator class for each btree class, in
-- btree's operator family, with the same name, input and storage types and
-- default, so CREATE INDEX ... USING bark accepts every type btree does.
-- ===========================================================================
SELECT b.opcname
FROM pg_opclass b JOIN pg_am ba ON ba.oid = b.opcmethod AND ba.amname = 'btree'
WHERE b.oid < 16384 AND NOT EXISTS
  (SELECT 1 FROM pg_opclass k JOIN pg_am ka ON ka.oid = k.opcmethod AND ka.amname = 'bark'
   WHERE (k.opcnamespace, k.opcname, k.opcfamily, k.opcintype, k.opckeytype, k.opcdefault) =
         (b.opcnamespace, b.opcname, b.opcfamily, b.opcintype, b.opckeytype, b.opcdefault));
SELECT count(*) AS bark_opclasses, count(*) FILTER (WHERE amvalidate(c.oid)) AS valid
FROM pg_opclass c JOIN pg_am a ON a.oid = c.opcmethod
WHERE a.amname = 'bark';
-- psql's \dAc sorts by type name in the database's collation; list them
-- in a fixed order instead
SELECT c.opcintype::regtype::text COLLATE "C" AS "Input type",
       CASE WHEN c.opckeytype <> 0 AND c.opckeytype <> c.opcintype
            THEN c.opckeytype::regtype::text END AS "Storage type",
       c.opcname AS "Operator class",
       CASE WHEN c.opcdefault THEN 'yes' ELSE 'no' END AS "Default?"
FROM pg_opclass c JOIN pg_am a ON a.oid = c.opcmethod
WHERE a.amname = 'bark' AND c.oid < 16384
ORDER BY 1, 3;

-- text_pattern_ops serves LIKE 'ab%' as a range scan under any collation,
-- as with btree.
CREATE TABLE bark_opc_like (t text);
INSERT INTO bark_opc_like SELECT md5(g::text) FROM generate_series(1, 4000) g;
CREATE INDEX bark_opc_like_idx ON bark_opc_like USING bark (t text_pattern_ops);
SET enable_seqscan = off;
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF) SELECT count(*) FROM bark_opc_like WHERE t LIKE 'ab%';
SELECT count(*) AS idx_count FROM bark_opc_like WHERE t LIKE 'ab%';
RESET enable_seqscan;
RESET enable_bitmapscan;
SET enable_indexscan = off;
SET enable_indexonlyscan = off;
SET enable_bitmapscan = off;
SELECT count(*) AS seq_count FROM bark_opc_like WHERE t LIKE 'ab%';
RESET enable_indexscan;
RESET enable_indexonlyscan;
RESET enable_bitmapscan;
DROP TABLE bark_opc_like;

-- A leaf read after a scan's first page skips the per-entry test of a row
-- comparison that its first and last entries both satisfy on the same
-- deciding member, and of an equality array whose column holds one of its
-- elements across the page (bark_page_satisfied_keys).  bark_cnt's operator
-- class counts calls of its comparator, which tests the row's members and
-- the array's elements, so a query reading many leaves makes a few calls per
-- leaf rather than one or more per entry.
CREATE SEQUENCE bark_cnt_seq;
CREATE FUNCTION bark_cnt_cmp(int4, int4) RETURNS int4 LANGUAGE plpgsql AS $$
BEGIN
  PERFORM nextval('public.bark_cnt_seq');
  RETURN btint4cmp($1, $2);
END $$;
CREATE OPERATOR #< (LEFTARG = int4, RIGHTARG = int4, FUNCTION = int4lt);
CREATE OPERATOR #<= (LEFTARG = int4, RIGHTARG = int4, FUNCTION = int4le);
CREATE OPERATOR #= (LEFTARG = int4, RIGHTARG = int4, FUNCTION = int4eq);
CREATE OPERATOR #>= (LEFTARG = int4, RIGHTARG = int4, FUNCTION = int4ge);
CREATE OPERATOR #> (LEFTARG = int4, RIGHTARG = int4, FUNCTION = int4gt);
CREATE OPERATOR FAMILY bark_cnt_fam USING btree;
CREATE OPERATOR CLASS bark_cnt_ops FOR TYPE int4 USING bark FAMILY bark_cnt_fam AS
  OPERATOR 1 #<, OPERATOR 2 #<=, OPERATOR 3 #=, OPERATOR 4 #>=, OPERATOR 5 #>,
  FUNCTION 1 bark_cnt_cmp(int4, int4);
CREATE TABLE bark_cnt (a int, b int);
INSERT INTO bark_cnt SELECT g / 5000, g FROM generate_series(0, 19999) g;
CREATE INDEX bark_cnt_i ON bark_cnt USING bark (a bark_cnt_ops, b bark_cnt_ops);
VACUUM ANALYZE bark_cnt;
CREATE FUNCTION bark_cnt_calls(q text, OUT n bigint, OUT few_calls bool)
LANGUAGE plpgsql AS $$
DECLARE
  plan json;
  node json;
BEGIN
  SET LOCAL enable_seqscan = off;
  SET LOCAL enable_bitmapscan = off;
  -- bark_cnt_cmp counts its calls with nextval(), which a parallel worker
  -- cannot run; the planner does not look at an opclass's support
  -- functions, so keep the scan in this backend.
  SET LOCAL debug_parallel_query = off;
  EXECUTE 'EXPLAIN (COSTS OFF, FORMAT JSON) ' || q INTO plan;
  node := plan->0->'Plan';
  WHILE node->>'Index Name' IS NULL AND node->'Plans' IS NOT NULL LOOP
    node := node->'Plans'->0;
  END LOOP;
  IF node->>'Index Name' IS DISTINCT FROM 'bark_cnt_i' THEN
    RAISE EXCEPTION 'not a scan of bark_cnt_i: %', plan;
  END IF;
  PERFORM setval('bark_cnt_seq', 1);
  EXECUTE 'SELECT count(*) FROM (' || q || ') x' INTO n;
  few_calls := currval('bark_cnt_seq') - 1 < n / 3;
END $$;
SELECT c.*, (SELECT count(*) FROM bark_cnt WHERE (a, b) > (0, 2500)) AS seqscan
FROM bark_cnt_calls('SELECT b FROM bark_cnt WHERE ROW(a, b) #> ROW(0, 2500)') c;
SELECT c.*, (SELECT count(*) FROM bark_cnt WHERE (a, b) <= (3, 17500)) AS seqscan
FROM bark_cnt_calls('SELECT b FROM bark_cnt WHERE ROW(a, b) #<= ROW(3, 17500) ORDER BY a DESC, b DESC') c;
SELECT c.*, (SELECT count(*) FROM bark_cnt WHERE a = ANY ('{1, 2, 3}')) AS seqscan
FROM bark_cnt_calls('SELECT b FROM bark_cnt WHERE a #= ANY (''{1, 2, 3}'')') c;
DROP TABLE bark_cnt;
DROP FUNCTION bark_cnt_calls(text);
DROP OPERATOR FAMILY bark_cnt_fam USING btree;
DROP OPERATOR #< (int4, int4), #<= (int4, int4), #= (int4, int4),
  #>= (int4, int4), #> (int4, int4);
DROP FUNCTION bark_cnt_cmp(int4, int4);
DROP SEQUENCE bark_cnt_seq;
