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

-- Deleting an empty interior leaf hands its key space to its right sibling in
-- the parent, as nbtree does.  Refilling the emptied range must leave every
-- key under the right downlink: amcheck verifies parent order, and the index
-- counts must equal a sequential scan's.  A temp table, so that no other
-- session's snapshot keeps VACUUM from removing the rows and deleting the
-- leaves.
CREATE EXTENSION IF NOT EXISTS amcheck;
CREATE TEMP TABLE bark_vgap (a int);
CREATE INDEX bark_vgap_idx ON bark_vgap USING bark (a);
INSERT INTO bark_vgap SELECT g * 10 FROM generate_series(1, 20000) g;
DELETE FROM bark_vgap WHERE a BETWEEN 50000 AND 150000;
VACUUM bark_vgap;
SELECT bark_index_check('bark_vgap_idx');
INSERT INTO bark_vgap SELECT g * 10 + 1 FROM generate_series(5000, 15000) g;
SELECT bark_index_check('bark_vgap_idx');
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) AS idx_all FROM bark_vgap WHERE a >= 0;
SELECT count(*) AS idx_gap FROM bark_vgap WHERE a BETWEEN 50000 AND 150010;
RESET enable_seqscan;
RESET enable_bitmapscan;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT count(*) AS seq_gap FROM bark_vgap WHERE a BETWEEN 50000 AND 150010;
RESET enable_indexscan;
RESET enable_bitmapscan;
DROP TABLE bark_vgap;

-- VACUUM rewrites a POSTING entry in place even when the survivors' sbm
-- encodes larger than the whole set did.  An sbm is not size-monotone:
-- removing every Nth TID of a dense run turns all-ones vectors (stored as a
-- 2-bit flag) into stored mixed vectors.  A POSTING entry therefore reserves
-- room for the largest encoding of any subset of its set, so VACUUM never
-- needs more room than the entry has: one pass removes every dead TID with no
-- page split (the index keeps its size), every scan agrees with a seqscan,
-- and bark_index_check (which checks the reserve and the item ceiling)
-- passes.  A second DELETE + VACUUM then shrinks the rewritten entries.
-- Temp tables, so that only this session's snapshot limits what VACUUM can
-- remove.
CREATE EXTENSION IF NOT EXISTS amcheck;
CREATE TEMP TABLE bark_vgrow (a int, b int);
CREATE INDEX bark_vgrow_idx ON bark_vgrow USING bark (a);
CREATE TEMP TABLE bark_vgrow_cnt (k text, n bigint);
CREATE TEMP TABLE bark_vgrow_pages (k text, before bigint, after bigint);
CREATE FUNCTION bark_vgrow_counts(tag text) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  UPDATE bark_vgrow_pages SET after = pg_relation_size('bark_vgrow_idx') / 8192
    WHERE k = tag;
  SET LOCAL enable_seqscan = off;
  SET LOCAL enable_bitmapscan = off;
  SET LOCAL enable_indexscan = on;
  SET LOCAL enable_indexonlyscan = on;
  INSERT INTO bark_vgrow_cnt SELECT tag || ' ios', count(*) FROM bark_vgrow WHERE a = 1;
  SET LOCAL enable_indexonlyscan = off;
  INSERT INTO bark_vgrow_cnt SELECT tag || ' idx', count(*) FROM bark_vgrow WHERE a = 1;
  SET LOCAL enable_indexscan = off;
  SET LOCAL enable_bitmapscan = on;
  INSERT INTO bark_vgrow_cnt SELECT tag || ' bitmap', count(*) FROM bark_vgrow WHERE a = 1;
  SET LOCAL enable_bitmapscan = off;
  SET LOCAL enable_seqscan = on;
  INSERT INTO bark_vgrow_cnt SELECT tag || ' seq', count(*) FROM bark_vgrow WHERE a = 1;
END $$;
-- 60000 TIDs of one key, every 10th deleted: VACUUM used to fail with
-- "failed to shrink BARK leaf entry during vacuum".
INSERT INTO bark_vgrow SELECT 1, g FROM generate_series(1, 60000) g;
WITH p AS (INSERT INTO bark_vgrow_pages
           VALUES ('60000/10 vac1', pg_relation_size('bark_vgrow_idx') / 8192))
DELETE FROM bark_vgrow WHERE b % 10 = 0;
VACUUM bark_vgrow;
SELECT bark_index_check('bark_vgrow_idx');
SELECT bark_vgrow_counts('60000/10 vac1');
WITH p AS (INSERT INTO bark_vgrow_pages
           VALUES ('60000/10 vac2', pg_relation_size('bark_vgrow_idx') / 8192))
DELETE FROM bark_vgrow WHERE b % 10 = 5;
VACUUM bark_vgrow;
SELECT bark_index_check('bark_vgrow_idx');
SELECT bark_vgrow_counts('60000/10 vac2');
-- 20000 TIDs, every 2nd deleted: VACUUM used to succeed but leave a
-- 3424-byte entry, over the item ceiling.
TRUNCATE bark_vgrow;
INSERT INTO bark_vgrow SELECT 1, g FROM generate_series(1, 20000) g;
WITH p AS (INSERT INTO bark_vgrow_pages
           VALUES ('20000/2 vac1', pg_relation_size('bark_vgrow_idx') / 8192))
DELETE FROM bark_vgrow WHERE b % 2 = 0;
VACUUM bark_vgrow;
SELECT bark_index_check('bark_vgrow_idx');
SELECT bark_vgrow_counts('20000/2 vac1');
WITH p AS (INSERT INTO bark_vgrow_pages
           VALUES ('20000/2 vac2', pg_relation_size('bark_vgrow_idx') / 8192))
DELETE FROM bark_vgrow WHERE b % 4 = 1;
VACUUM bark_vgrow;
SELECT bark_index_check('bark_vgrow_idx');
SELECT bark_vgrow_counts('20000/2 vac2');
-- 120000 TIDs over several leaves, every 2nd deleted.  A dead TID left
-- behind would point at a heap slot VACUUM has freed, and the index-only
-- scan, which trusts the all-visible pages, would count it.
TRUNCATE bark_vgrow;
INSERT INTO bark_vgrow SELECT 1, g FROM generate_series(1, 120000) g;
WITH p AS (INSERT INTO bark_vgrow_pages
           VALUES ('120000/2 vac1', pg_relation_size('bark_vgrow_idx') / 8192))
DELETE FROM bark_vgrow WHERE b % 2 = 0;
VACUUM bark_vgrow;
SELECT bark_index_check('bark_vgrow_idx');
SELECT bark_vgrow_counts('120000/2 vac1');
WITH p AS (INSERT INTO bark_vgrow_pages
           VALUES ('120000/2 vac2', pg_relation_size('bark_vgrow_idx') / 8192))
DELETE FROM bark_vgrow WHERE b % 4 = 1;
VACUUM bark_vgrow;
SELECT bark_index_check('bark_vgrow_idx');
SELECT bark_vgrow_counts('120000/2 vac2');
SELECT k, n FROM bark_vgrow_cnt ORDER BY k;
SELECT k, after = before AS in_place FROM bark_vgrow_pages ORDER BY k;
DROP FUNCTION bark_vgrow_counts(text);
DROP TABLE bark_vgrow, bark_vgrow_cnt, bark_vgrow_pages;

-- A second VACUUM after one that deleted a leaf page.  A deleted page once
-- kept the items it had when it was unlinked, its old high key among them,
-- and VACUUM used to read it as a live leaf and fail with "BARK leaf entry has
-- unexpected shape".  A temp table, so that only this session's snapshot
-- limits what VACUUM can remove.
CREATE TEMP TABLE bark_vac2 (a int);
CREATE INDEX bark_vac2_idx ON bark_vac2 USING bark (a);
INSERT INTO bark_vac2 SELECT g FROM generate_series(1, 3000) g;
DELETE FROM bark_vac2 WHERE a BETWEEN 1000 AND 2000;
VACUUM bark_vac2;						-- deletes the emptied leaves
DELETE FROM bark_vac2 WHERE a = 5;
VACUUM bark_vac2;						-- visits the deleted pages
SELECT bark_index_check('bark_vac2_idx');
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) AS idx_count FROM bark_vac2 WHERE a > 0;
RESET enable_seqscan;
RESET enable_bitmapscan;
SELECT count(*) AS heap_count FROM bark_vac2;
DROP TABLE bark_vac2;

-- Deleted leaves keep their sibling links.  Delete the middle of the key
-- range, VACUUM (deleting the emptied leaves), and scan across the deleted
-- range forward, backward, and with a scroll cursor that changes direction
-- there; each must return exactly the live keys.  amcheck checks the deleted
-- pages' layout and the live pages' links.  The deleted pages are reused once
-- no transaction older than their deletion remains: safexid is the next XID
-- at deletion, so a transaction that took an XID after it must have ended,
-- and the second VACUUM below then records them.  Refilling
-- the range then takes the deleted pages before it extends the index: on
-- 8kB pages the index has 19 pages, the VACUUM deletes 7, and the refill needs
-- 13, so it grows the index to 25 pages with reuse and to 32 without.  A temp
-- table, so that only this session's snapshot holds back reuse.
CREATE TEMP TABLE bark_dpg (a int);
CREATE INDEX bark_dpg_idx ON bark_dpg USING bark (a);
INSERT INTO bark_dpg SELECT g FROM generate_series(1, 6000) g;
CREATE TEMP TABLE bark_dpg_sz AS
  SELECT pg_relation_size('bark_dpg_idx') / 8192 AS built_pages;
DELETE FROM bark_dpg WHERE a BETWEEN 1500 AND 4500;
VACUUM bark_dpg;
SELECT bark_index_check('bark_dpg_idx');
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT (SELECT array_agg(a) FROM
          (SELECT a FROM bark_dpg WHERE a BETWEEN 1000 AND 5000 ORDER BY a) s) =
       (SELECT array_agg(g ORDER BY g) FROM generate_series(1000, 5000) g
         WHERE g NOT BETWEEN 1500 AND 4500) AS forward_ok,
       (SELECT array_agg(a) FROM
          (SELECT a FROM bark_dpg WHERE a BETWEEN 1000 AND 5000 ORDER BY a DESC) s) =
       (SELECT array_agg(g ORDER BY g DESC) FROM generate_series(1000, 5000) g
         WHERE g NOT BETWEEN 1500 AND 4500) AS backward_ok;
BEGIN;
DECLARE bark_dpg_c SCROLL CURSOR FOR SELECT a FROM bark_dpg ORDER BY a;
MOVE FORWARD 1497 IN bark_dpg_c;
FETCH FORWARD 3 FROM bark_dpg_c;
FETCH BACKWARD 3 FROM bark_dpg_c;
COMMIT;
RESET enable_seqscan;
RESET enable_bitmapscan;
SELECT pg_current_xact_id() > '0'::xid8 AS xid_assigned;
VACUUM bark_dpg;
INSERT INTO bark_dpg SELECT g FROM generate_series(1500, 4500) g;
SELECT bark_index_check('bark_dpg_idx');
SELECT pg_relation_size('bark_dpg_idx') / 8192 < built_pages + 10
         AS refill_reused_pages
  FROM bark_dpg_sz;
SELECT count(*) AS live_count FROM bark_dpg;
DROP TABLE bark_dpg, bark_dpg_sz;

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
  line text;
  n int := 0;
BEGIN
  SET LOCAL enable_seqscan = off;
  SET LOCAL enable_bitmapscan = off;
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

-- Parallelism is intrinsic to the single-center outward merge: the planner
-- never gives an ordered (amcanorderbyop) scan a parallel index path, so a KNN
-- scan always runs single-copy.  Forcing a parallel plan therefore yields a
-- single-copy Gather over the index Order By scan, and the result is identical
-- to the serial scan -- no duplicate or missing rows.
SET debug_parallel_query = on;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
EXPLAIN (COSTS OFF)
  SELECT a FROM bark_knn ORDER BY a <~> 5000 LIMIT 6;
SELECT a FROM bark_knn ORDER BY a <~> 5000 LIMIT 6;
RESET debug_parallel_query;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;

RESET enable_seqscan;

-- KNN scans against a sort of a sequential scan by distance: the two must
-- return the same keys in the same order.  Each case runs as an index-only
-- scan and as a plain index scan; bark_knn_check returns NULL, with a notice,
-- if the plan is not a KNN scan of the requested kind.  At equal distance the
-- scan returns its forward side first: keys >= the constant on an ASC index,
-- keys <= it on a DESC one, so the reference sorts ties the same way.
CREATE FUNCTION bark_knn_check(tbl text, expr text, c int, lim int, ios bool,
                               descidx bool)
RETURNS bool LANGUAGE plpgsql AS $$
DECLARE
  q text := format('SELECT (%s)::text FROM %I ORDER BY a <~> (%s) LIMIT %s',
                   expr, tbl, c, lim);
  line text;
  plan text := '';
  node text := CASE WHEN ios THEN 'Index Only Scan' ELSE 'Index Scan' END;
  knn text[];
  ref text[];
BEGIN
  SET LOCAL enable_seqscan = off;
  SET LOCAL enable_bitmapscan = off;
  SET LOCAL enable_sort = off;
  SET LOCAL enable_indexscan = on;
  EXECUTE format('SET LOCAL enable_indexonlyscan = %s', ios);
  FOR line IN EXECUTE 'EXPLAIN (COSTS OFF) ' || q LOOP
    plan := plan || line || E'\n';
  END LOOP;
  IF plan !~ node OR plan !~ 'Order By' OR plan ~ 'Sort' THEN
    RAISE NOTICE 'not a KNN scan: %', plan;
    RETURN NULL;
  END IF;
  EXECUTE format('SELECT array(%s)', q) INTO knn;
  SET LOCAL enable_seqscan = on;
  SET LOCAL enable_sort = on;
  SET LOCAL enable_indexscan = off;
  SET LOCAL enable_indexonlyscan = off;
  EXECUTE format('SELECT array(SELECT (%s)::text FROM %I ORDER BY a <~> (%s), a %s (%s) LIMIT %s)',
                 expr, tbl, c, CASE WHEN descidx THEN '>' ELSE '<' END, c, lim)
    INTO ref;
  RETURN knn IS NOT DISTINCT FROM ref;
END $$;
CREATE TEMP TABLE bark_knn_q (tbl text, expr text, c int, lim int,
                              descidx bool DEFAULT false);

-- Inside the key range, at and beyond either end, and the whole index.
INSERT INTO bark_knn_q (tbl, expr, c, lim) VALUES
  ('bark_knn', 'a', 5000, 10), ('bark_knn', 'a', 5000, 100000),
  ('bark_knn', 'a', 4999, 50), ('bark_knn', 'a', 3, 30),
  ('bark_knn', 'a', 1, 20), ('bark_knn', 'a', -100, 100000),
  ('bark_knn', 'a', 9000, 30), ('bark_knn', 'a', 1000000, 20),
  ('bark_knn', 'a', 1000000, 100000);

-- POSTING entries (300 rows per even key) and LIST entries (3 per odd key):
-- the center on a POSTING key, on a LIST key, and outside the range.
CREATE TABLE bark_knn_post (a int);
CREATE INDEX bark_knn_post_idx ON bark_knn_post USING bark (a);
INSERT INTO bark_knn_post SELECT k * 2 FROM generate_series(0, 29) k,
  generate_series(1, 300) g;
INSERT INTO bark_knn_post SELECT k * 2 + 1 FROM generate_series(0, 29) k,
  generate_series(1, 3) g;
VACUUM ANALYZE bark_knn_post;
-- As LIST entries the 9090 TIDs alone would fill more than 6 leaves.
SELECT pg_relation_size('bark_knn_post_idx') / 8192 < 8 AS post_has_posting;
INSERT INTO bark_knn_q (tbl, expr, c, lim) VALUES
  ('bark_knn_post', 'a', 30, 1000), ('bark_knn_post', 'a', 31, 1000),
  ('bark_knn_post', 'a', 31, 100000), ('bark_knn_post', 'a', -5, 1000),
  ('bark_knn_post', 'a', 100, 100000);

-- A DESC index, with NULLs (which sort first in it, and come last here).
CREATE TABLE bark_knn_desc (a int);
INSERT INTO bark_knn_desc
  SELECT CASE WHEN g % 97 = 0 THEN NULL ELSE g * 3 END
    FROM generate_series(1, 3000) g;
CREATE INDEX bark_knn_desc_idx ON bark_knn_desc USING bark (a DESC);
VACUUM ANALYZE bark_knn_desc;
INSERT INTO bark_knn_q VALUES
  ('bark_knn_desc', 'a', 5000, 100, true), ('bark_knn_desc', 'a', 4999, 50, true),
  ('bark_knn_desc', 'a', -10, 50, true), ('bark_knn_desc', 'a', 1000000, 50, true),
  ('bark_knn_desc', 'a', 5000, 100000, true);

-- Delete the middle of the key range and VACUUM, which deletes the emptied
-- leaves (as for bark_dpg above; a temp table so that only this session's
-- snapshot holds back removal), then scan across the range: from its right,
-- the backward side walks left over it; from its left, the forward side
-- walks right over it; from inside, both sides do.
CREATE TEMP TABLE bark_knn_del (a int);
CREATE INDEX bark_knn_del_idx ON bark_knn_del USING bark (a);
INSERT INTO bark_knn_del SELECT g FROM generate_series(1, 6000) g;
DELETE FROM bark_knn_del WHERE a BETWEEN 1500 AND 4500;
VACUUM ANALYZE bark_knn_del;
SELECT bark_index_check('bark_knn_del_idx');
INSERT INTO bark_knn_q (tbl, expr, c, lim) VALUES
  ('bark_knn_del', 'a', 4800, 1000), ('bark_knn_del', 'a', 4800, 100000),
  ('bark_knn_del', 'a', 1200, 1000), ('bark_knn_del', 'a', 3000, 500);

-- OVERSIZED entries: there is no distance operator for text, so no oversized
-- key can be KNN-ordered; an oversized INCLUDE payload makes the entries
-- OVERSIZED instead.  The distance and the index-only scan's copy must come
-- from the full tuple on the overflow chain, not from the small inline entry.
-- Every other row is small, so pages mix the two shapes.  4000 bytes of hex
-- digits cannot compress below the item size limit, so each oversized row
-- has an overflow page of its own (the size check below).
CREATE TABLE bark_knn_big (a int, p text);
INSERT INTO bark_knn_big
  SELECT g, CASE WHEN g % 2 = 0 THEN
              (SELECT string_agg(md5(g::text || h::text), '')
                 FROM generate_series(1, 125) h)
            ELSE 'small' || g END
    FROM generate_series(1, 400) g;
CREATE INDEX bark_knn_big_idx ON bark_knn_big USING bark (a) INCLUDE (p);
VACUUM ANALYZE bark_knn_big;
SELECT pg_relation_size('bark_knn_big_idx') / 8192 > 200 AS big_has_overflow;
INSERT INTO bark_knn_q (tbl, expr, c, lim) VALUES
  ('bark_knn_big', 'a', 200, 50), ('bark_knn_big', 'a', 200, 100000),
  ('bark_knn_big', 'a || '':'' || md5(p)', 200, 50),
  ('bark_knn_big', 'a || '':'' || md5(p)', 1, 100000);

SELECT count(*) AS knn_checks,
       string_agg(format('%s %s <~> %s limit %s%s', tbl, expr, c, lim,
                         CASE WHEN ios THEN ' ios' ELSE '' END), '; ')
         FILTER (WHERE ok IS NOT TRUE) AS knn_failed
  FROM (SELECT q.*, v.ios,
               bark_knn_check(q.tbl, q.expr, q.c, q.lim, v.ios, q.descidx) AS ok
          FROM bark_knn_q q, (VALUES (true), (false)) v(ios)) s;

-- The scan reads a page at a time and only when the merge needs it: the ten
-- nearest rows to 5000 lie on one or two leaves, so the scan reads at most
-- one more leaf on each side of them, beside the metapage and the root,
-- while a scan of the whole index reads every leaf.  A temp table, so that
-- VACUUM can set every heap page all-visible and the index-only scans count
-- index pages only.
CREATE FUNCTION bark_knn_buffers(q text) RETURNS int LANGUAGE plpgsql AS $$
DECLARE
  line text;
  n int := 0;
BEGIN
  SET LOCAL enable_seqscan = off;
  SET LOCAL enable_bitmapscan = off;
  FOR line IN EXECUTE 'EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF, SUMMARY OFF) ' || q LOOP
    IF line ~ 'Buffers:' THEN
      SELECT sum(m[2]::int) INTO n
        FROM regexp_matches(line, '(hit|read)=([0-9]+)', 'g') m;
      RETURN n;
    END IF;
  END LOOP;
  RETURN NULL;
END $$;
CREATE TEMP TABLE bark_knn_rd (a int);
INSERT INTO bark_knn_rd SELECT g * 3 FROM generate_series(1, 3000) g;
CREATE INDEX bark_knn_rd_idx ON bark_knn_rd USING bark (a);
VACUUM ANALYZE bark_knn_rd;
SELECT bark_knn_buffers('SELECT a FROM bark_knn_rd ORDER BY a <~> 5000 LIMIT 10') <= 6
         AS knn_limit_reads_few_pages,
       bark_knn_buffers('SELECT a FROM bark_knn_rd ORDER BY a <~> 5000') >=
         pg_relation_size('bark_knn_rd_idx') / 8192 - 2 AS knn_whole_scan_reads_all;
DROP FUNCTION bark_knn_buffers(text);
DROP FUNCTION bark_knn_check(text, text, int, int, bool, bool);
DROP TABLE bark_knn_q, bark_knn_post, bark_knn_desc, bark_knn_del, bark_knn_big,
  bark_knn_rd;

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

-- ===========================================================================
-- Oversized-key inline-prefix fast path: an OVERSIZED entry over a C-collation
-- text column stores a short prefix inline so two oversized keys that differ
-- early order without fetching the overflow chain; keys sharing a long prefix
-- fall through to the full value.  Correctness must match a sequential scan
-- either way.  (The index column uses the C collation so the bytewise prefix
-- shortcut is sound.)
-- ===========================================================================
CREATE TABLE bark_pfxfast (k text COLLATE "C");
-- Mix: keys differing in the first byte (prefix decides), keys sharing an
-- 8000-char prefix differing only in the tail (prefix ties -> full fetch), and
-- small keys (no overflow).  All oversized ones exceed the ~2.7KB item cap.
INSERT INTO bark_pfxfast VALUES
  (repeat('A', 9000) || '1'),
  (repeat('A', 9000) || '3'),
  (repeat('A', 9000) || '2'),
  (repeat('B', 9000)),
  (repeat('Z', 9000)),
  ('short-a'), ('short-b');
CREATE INDEX bark_pfxfast_idx ON bark_pfxfast USING bark (k);
SELECT bark_index_check('bark_pfxfast_idx');
SET enable_seqscan = off;
SET enable_bitmapscan = off;
-- Ordering of the shared-prefix group must break on the tail (1,2,3), proving
-- the full-fetch tie-break; the whole ordering must match a seqscan.
SELECT right(k, 1) AS tail FROM bark_pfxfast WHERE k LIKE 'A%' ORDER BY k;
-- Equality on an oversized key differing early (prefix decides, no fetch needed).
SELECT count(*) AS eq_b FROM bark_pfxfast WHERE k = repeat('B', 9000);
-- Range over the oversized keys agrees with a seqscan.
SELECT count(*) AS rng_idx FROM bark_pfxfast WHERE k >= repeat('A', 9000);
SET enable_seqscan = on;
SET enable_indexscan = off;
SELECT count(*) AS rng_seq FROM bark_pfxfast WHERE k >= repeat('A', 9000);
RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;
DROP TABLE bark_pfx;
-- The inline prefix must hold the value's bytes, not the compressed bytes:
-- an oversized key with a compressible tail is stored compressed in the
-- overflow chain (lz4 or pglz), and compressed images do not sort like the
-- values.  Keys share a 4-digit lead and differ in random md5 text.
CREATE TABLE bark_pfxcomp (t text COLLATE "C");
CREATE INDEX bark_pfxcomp_idx ON bark_pfxcomp USING bark (t);
INSERT INTO bark_pfxcomp
  SELECT lpad((g / 2)::text, 4, '0') ||
         (SELECT string_agg(md5((g / 2)::text || i::text), '')
          FROM generate_series(1, 100) i)
  FROM generate_series(1, 1000) g ORDER BY md5(g::text);
SELECT bark_index_check('bark_pfxcomp_idx');
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) AS idx_count FROM bark_pfxcomp WHERE t >= '0300';
RESET enable_seqscan;
RESET enable_bitmapscan;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT count(*) AS seq_count FROM bark_pfxcomp WHERE t >= '0300';
RESET enable_indexscan;
RESET enable_bitmapscan;
DROP TABLE bark_pfxcomp;
-- Equal keys share a LIST/POSTING entry only when they are byte-identical
-- (bark_allequalimage, nbtree's _bt_allequalimage rule).  An INCLUDE index
-- never coalesces: rows with the same key carry different payloads, and an
-- index-only scan must return each row's own payload, not the first row's.
CREATE TABLE bark_inc_dup (a int, p text);
CREATE INDEX bark_inc_dup_idx ON bark_inc_dup USING bark (a) INCLUDE (p);
INSERT INTO bark_inc_dup SELECT 1, 'row' || g FROM generate_series(1, 5) g;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF) SELECT a, p FROM bark_inc_dup WHERE a = 1;
SELECT a, p FROM bark_inc_dup WHERE a = 1 ORDER BY p;
RESET enable_seqscan;
RESET enable_bitmapscan;
SELECT bark_index_check('bark_inc_dup_idx');
DROP TABLE bark_inc_dup;

-- fillfactor reloption.  CREATE INDEX packs leaf pages to the fillfactor
-- (default 90), as nbtree does; out-of-range and unknown options are rejected.
CREATE TABLE bark_ff (a int);
INSERT INTO bark_ff SELECT g FROM generate_series(1, 100000) g;
CREATE INDEX bark_ff100_idx ON bark_ff USING bark (a) WITH (fillfactor = 100);
CREATE INDEX bark_ff90_idx ON bark_ff USING bark (a);
CREATE INDEX bark_ff50_idx ON bark_ff USING bark (a) WITH (fillfactor = 50);
SELECT relname, reloptions FROM pg_class
  WHERE relname IN ('bark_ff100_idx', 'bark_ff90_idx', 'bark_ff50_idx')
  ORDER BY relname;
SELECT pg_relation_size('bark_ff50_idx') > pg_relation_size('bark_ff90_idx') AND
       pg_relation_size('bark_ff90_idx') > pg_relation_size('bark_ff100_idx')
  AS ff_orders_size;
SELECT bark_index_check('bark_ff100_idx');
SELECT bark_index_check('bark_ff90_idx');
SELECT bark_index_check('bark_ff50_idx');
ALTER INDEX bark_ff50_idx SET (fillfactor = 80);
SELECT reloptions FROM pg_class WHERE relname = 'bark_ff50_idx';
ALTER INDEX bark_ff50_idx RESET (fillfactor);
SELECT reloptions FROM pg_class WHERE relname = 'bark_ff50_idx';
CREATE INDEX bark_ff_bad ON bark_ff USING bark (a) WITH (fillfactor = 5);
CREATE INDEX bark_ff_bad ON bark_ff USING bark (a) WITH (fillfactor = 101);
CREATE INDEX bark_ff_bad ON bark_ff USING bark (a) WITH (deduplicate_items = off);
-- A low fillfactor with wide keys still puts at least two entries on each
-- page: 200 incompressible ~1kB keys at fillfactor 10 must take fewer than
-- 200 pages, internal levels and meta page included.
CREATE TABLE bark_ff_wide (t text);
INSERT INTO bark_ff_wide
  SELECT lpad(g::text, 3, '0') ||
         (SELECT string_agg(md5(g::text || i::text), '') FROM generate_series(1, 30) i)
  FROM generate_series(1, 200) g;
CREATE INDEX bark_ff_wide_idx ON bark_ff_wide USING bark (t) WITH (fillfactor = 10);
SELECT pg_relation_size('bark_ff_wide_idx') / 8192 < 200 AS two_per_page;
SELECT bark_index_check('bark_ff_wide_idx');
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) FROM bark_ff_wide WHERE t >= '150';
RESET enable_seqscan;
RESET enable_bitmapscan;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT count(*) FROM bark_ff_wide WHERE t >= '150';
RESET enable_indexscan;
RESET enable_bitmapscan;
-- At fillfactor 100 a page fills to the last byte, so a wide key arriving
-- after narrow ones leaves no room for the high key formed from it: every
-- 300th key here is ~2.6kB.  The build then moves the page's last item to
-- the next page, as nbtree's build does, instead of failing.
CREATE TABLE bark_ff_room (t text);
INSERT INTO bark_ff_room
  SELECT lpad(g::text, 8, '0') ||
         CASE WHEN g % 300 = 0
              THEN (SELECT string_agg(md5(g::text || i::text), '')
                    FROM generate_series(1, 80) i)
              ELSE '' END
  FROM generate_series(1, 30000) g;
CREATE INDEX bark_ff_room_idx ON bark_ff_room USING bark (t) WITH (fillfactor = 100);
SELECT bark_index_check('bark_ff_room_idx');
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) AS idx_count FROM bark_ff_room WHERE t >= '00015000';
RESET enable_seqscan;
RESET enable_bitmapscan;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT count(*) AS seq_count FROM bark_ff_room WHERE t >= '00015000';
RESET enable_indexscan;
RESET enable_bitmapscan;
DROP TABLE bark_ff_room;
DROP TABLE bark_ff_wide;
DROP TABLE bark_ff;

-- ===========================================================================
-- Split-point choice (barksplitloc.c).  A split of the rightmost leaf leaves
-- the left page at the fillfactor, so ascending inserts fill pages as full as
-- CREATE INDEX packs them; other splits balance bytes, not items.  Page
-- counts are compared as booleans, against a build or a btree index on the
-- same data, so the expected output does not depend on exact sizes.
-- ===========================================================================
CREATE TABLE bark_split_seq (a int) WITH (autovacuum_enabled = off);
CREATE INDEX bark_split_seq90 ON bark_split_seq USING bark (a);
CREATE INDEX bark_split_seq70 ON bark_split_seq USING bark (a) WITH (fillfactor = 70);
INSERT INTO bark_split_seq SELECT g FROM generate_series(1, 100000) g;
CREATE INDEX bark_split_seq90_build ON bark_split_seq USING bark (a);
CREATE INDEX bark_split_seq70_build ON bark_split_seq USING bark (a) WITH (fillfactor = 70);
-- Inserted and built page counts agree within 5% at both fillfactors (a
-- 50/50 split of the rightmost leaf takes almost twice the built count).
SELECT abs(pg_relation_size('bark_split_seq90') -
           pg_relation_size('bark_split_seq90_build')) <=
       pg_relation_size('bark_split_seq90_build') * 0.05 AS seq_ff90_like_build,
       abs(pg_relation_size('bark_split_seq70') -
           pg_relation_size('bark_split_seq70_build')) <=
       pg_relation_size('bark_split_seq70_build') * 0.05 AS seq_ff70_like_build,
       pg_relation_size('bark_split_seq70') >
       pg_relation_size('bark_split_seq90') AS seq_ff70_larger;
SELECT bark_index_check('bark_split_seq90');
SELECT bark_index_check('bark_split_seq70');
DROP TABLE bark_split_seq;

-- Random and descending inserts take about as many pages as btree, and the
-- index agrees with a seqscan.
SELECT setseed(0.42);
CREATE TABLE bark_split_rnd (a int) WITH (autovacuum_enabled = off);
CREATE INDEX bark_split_rnd_idx ON bark_split_rnd USING bark (a);
CREATE INDEX bark_split_rnd_bt ON bark_split_rnd USING btree (a);
INSERT INTO bark_split_rnd
  SELECT (random() * 1000000000)::int FROM generate_series(1, 100000);
SELECT pg_relation_size('bark_split_rnd_idx') <=
       pg_relation_size('bark_split_rnd_bt') * 1.1 AS rnd_like_btree;
SELECT bark_index_check('bark_split_rnd_idx');
DROP INDEX bark_split_rnd_bt;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) FROM bark_split_rnd WHERE a >= 500000000;
RESET enable_seqscan;
RESET enable_bitmapscan;
SET enable_indexscan = off;
SET enable_indexonlyscan = off;
SET enable_bitmapscan = off;
SELECT count(*) FROM bark_split_rnd WHERE a >= 500000000;
RESET enable_indexscan;
RESET enable_indexonlyscan;
RESET enable_bitmapscan;
DROP TABLE bark_split_rnd;

CREATE TABLE bark_split_desc (a int) WITH (autovacuum_enabled = off);
CREATE INDEX bark_split_desc_idx ON bark_split_desc USING bark (a);
CREATE INDEX bark_split_desc_bt ON bark_split_desc USING btree (a);
INSERT INTO bark_split_desc SELECT g FROM generate_series(100000, 1, -1) g;
SELECT pg_relation_size('bark_split_desc_idx') <=
       pg_relation_size('bark_split_desc_bt') * 1.1 AS desc_like_btree;
SELECT bark_index_check('bark_split_desc_idx');
DROP INDEX bark_split_desc_bt;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*), min(a), max(a) FROM bark_split_desc WHERE a >= 0;
RESET enable_seqscan;
RESET enable_bitmapscan;
DROP TABLE bark_split_desc;

-- Mixed sizes: three ~2.6kB keys inserted after 61 short ones.  Splitting by
-- item count put all three big keys and half the short ones on the right
-- page, which did not fit ("failed to insert item into BARK page").
CREATE TABLE bark_split_mixed (t text) WITH (autovacuum_enabled = off);
CREATE INDEX bark_split_mixed_idx ON bark_split_mixed USING bark (t);
INSERT INTO bark_split_mixed
  SELECT 'a' || lpad(g::text, 4, '0') FROM generate_series(1, 61) g;
INSERT INTO bark_split_mixed
  SELECT 'b' || g || (SELECT string_agg(md5(g::text || i::text), '')
                      FROM generate_series(1, 80) i)
  FROM generate_series(1, 3) g;
SELECT bark_index_check('bark_split_mixed_idx');
-- Keys of very different lengths in random order.
SELECT setseed(0.17);
INSERT INTO bark_split_mixed
  SELECT lpad(g::text, 6, '0') ||
         CASE WHEN random() < 0.1
              THEN (SELECT string_agg(md5(g::text || i::text), '')
                    FROM generate_series(1, (random() * 70)::int + 1) i)
              ELSE '' END
  FROM generate_series(1, 5000) g ORDER BY random();
SELECT bark_index_check('bark_split_mixed_idx');
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*), sum(length(t)) FROM bark_split_mixed WHERE t >= '';
RESET enable_seqscan;
RESET enable_bitmapscan;
SELECT count(*), sum(length(t)) FROM bark_split_mixed;
DROP TABLE bark_split_mixed;

-- A key that fits a page but whose row, with its INCLUDE column, does not:
-- the leaf entry is OVERSIZED and small, but a high key made from it holds
-- the key inline, so the split must reserve room for the larger high key.
CREATE TABLE bark_split_incov (k text, p text) WITH (autovacuum_enabled = off);
CREATE INDEX bark_split_incov_idx ON bark_split_incov USING bark (k) INCLUDE (p);
INSERT INTO bark_split_incov
  SELECT lpad(g::text, 5, '0') ||
         (SELECT string_agg(md5(g::text || i::text), '') FROM generate_series(1, 60) i),
         (SELECT string_agg(md5(i::text || g::text), '') FROM generate_series(1, 30) i)
  FROM generate_series(1, 400) g;
SELECT bark_index_check('bark_split_incov_idx');
SELECT count(*) FROM bark_split_incov;
DROP TABLE bark_split_incov;

-- Duplicate runs.  With coalescing, 20k rows over 10 keys in random order
-- become LIST/POSTING entries; the index must agree with the heap per key.
SELECT setseed(0.5);
CREATE TABLE bark_split_dup (k int, x int) WITH (autovacuum_enabled = off);
CREATE INDEX bark_split_dup_idx ON bark_split_dup USING bark (k);
INSERT INTO bark_split_dup
  SELECT (random() * 9)::int, g FROM generate_series(1, 20000) g ORDER BY random();
SELECT bark_index_check('bark_split_dup_idx');
SET enable_seqscan = off;
SET enable_bitmapscan = off;
CREATE TEMP TABLE bark_split_dup_counts AS
  SELECT k, count(*) AS n FROM bark_split_dup WHERE k >= 0 GROUP BY k;
RESET enable_seqscan;
RESET enable_bitmapscan;
SET enable_indexscan = off;
SET enable_indexonlyscan = off;
SET enable_bitmapscan = off;
SELECT count(*) AS per_key_mismatches
  FROM (SELECT k, count(*) AS n FROM bark_split_dup GROUP BY k) h
  FULL JOIN bark_split_dup_counts i USING (k)
  WHERE h.n IS DISTINCT FROM i.n;
RESET enable_indexscan;
RESET enable_indexonlyscan;
RESET enable_bitmapscan;
DROP TABLE bark_split_dup_counts;
DROP TABLE bark_split_dup;

-- Without coalescing (an INCLUDE column), equal keys stay separate entries
-- and runs span pages.  A split inside a run moves to the edge of the run;
-- a page holding a single key, with no later page holding it, is left about
-- 96% full, since BARK appends equal keys to the end of their run and the
-- left page gets no more of them; and a run edge is not used when that would
-- start the right page with the new item of a descending sequence.  Each
-- case takes about as many pages as btree on the same data.
SELECT setseed(0.5);
CREATE TABLE bark_split_inc (k int, x int) WITH (autovacuum_enabled = off);
CREATE INDEX bark_split_inc_idx ON bark_split_inc USING bark (k) INCLUDE (x);
CREATE INDEX bark_split_inc_bt ON bark_split_inc USING btree (k) INCLUDE (x);
INSERT INTO bark_split_inc
  SELECT (random() * 9)::int, g FROM generate_series(1, 20000) g ORDER BY random();
SELECT pg_relation_size('bark_split_inc_idx') <=
       pg_relation_size('bark_split_inc_bt') * 1.1 AS dup10_like_btree;
SELECT bark_index_check('bark_split_inc_idx');
TRUNCATE bark_split_inc;
INSERT INTO bark_split_inc SELECT 1, g FROM generate_series(1, 20000) g;
SELECT pg_relation_size('bark_split_inc_idx') <=
       pg_relation_size('bark_split_inc_bt') * 1.1 AS single_value_like_btree;
SELECT bark_index_check('bark_split_inc_idx');
TRUNCATE bark_split_inc;
INSERT INTO bark_split_inc SELECT 0, g FROM generate_series(1, 5000) g;
INSERT INTO bark_split_inc SELECT 20000 - g, g FROM generate_series(1, 20000) g;
SELECT pg_relation_size('bark_split_inc_idx') <=
       pg_relation_size('bark_split_inc_bt') * 1.1 AS dups_then_desc_like_btree;
SELECT bark_index_check('bark_split_inc_idx');
DROP INDEX bark_split_inc_bt;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*), count(DISTINCT k) FROM bark_split_inc WHERE k >= 0;
RESET enable_seqscan;
RESET enable_bitmapscan;
DROP TABLE bark_split_inc;

-- ===========================================================================
-- Suffix truncation of pivots.  A leaf split's high key, and the downlink
-- copied from it, keep only the leading key attributes needed to separate the
-- last key on the left from the first key on the right; the rest compare as
-- minus infinity.  Every query below runs once through a BARK index and once
-- as a sequential scan, and the two results must agree.  bark_sfx_check
-- reports 'no index' when the plan did not use the BARK index.
-- ===========================================================================
CREATE FUNCTION bark_sfx_check(q text, bitmap bool DEFAULT false)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  plan text;
  viaidx text;
  viaseq text;
BEGIN
  PERFORM set_config('enable_seqscan', 'off', true);
  PERFORM set_config('enable_indexscan', (NOT bitmap)::text, true);
  PERFORM set_config('enable_indexonlyscan', (NOT bitmap)::text, true);
  PERFORM set_config('enable_bitmapscan', bitmap::text, true);
  PERFORM set_config('enable_hashjoin', 'off', true);
  PERFORM set_config('enable_mergejoin', 'off', true);
  PERFORM set_config('enable_material', 'off', true);
  EXECUTE 'EXPLAIN (COSTS OFF, FORMAT JSON) ' || q INTO plan;
  EXECUTE 'SELECT md5(string_agg(x::text, '' '')) FROM (' || q || ') x'
    INTO viaidx;
  PERFORM set_config('enable_seqscan', 'on', true);
  PERFORM set_config('enable_indexscan', 'off', true);
  PERFORM set_config('enable_indexonlyscan', 'off', true);
  PERFORM set_config('enable_bitmapscan', 'off', true);
  PERFORM set_config('enable_hashjoin', 'on', true);
  PERFORM set_config('enable_mergejoin', 'on', true);
  PERFORM set_config('enable_material', 'on', true);
  EXECUTE 'SELECT md5(string_agg(x::text, '' '')) FROM (' || q || ') x'
    INTO viaseq;
  IF position('"Index Name": "bark_sfx' IN plan) = 0 THEN
    RETURN 'no index';
  END IF;
  RETURN CASE WHEN viaidx IS NOT DISTINCT FROM viaseq THEN 'ok'
              ELSE 'MISMATCH' END;
END $$;

-- Three key columns: a has ten values, b about a thousand, c is unique.
-- Some rows have NULL b or NULL c, and one in eight repeats an earlier
-- (a, b), so equal keys on two attributes also span pages.
SELECT setseed(0.31);
CREATE TABLE bark_sfx_src AS
  SELECT g AS id, (random() * 9)::int AS a,
         CASE WHEN g % 97 = 0 THEN NULL ELSE (random() * 1000)::int END AS b,
         CASE WHEN g % 89 = 0 THEN NULL ELSE md5(g::text) END AS c,
         g % 7 AS d
  FROM generate_series(1, 8000) g;
UPDATE bark_sfx_src SET b = 500 WHERE id % 8 = 0;
CREATE TABLE bark_sfx (id int, a int, b int, c text, d int)
  WITH (autovacuum_enabled = off);

CREATE TABLE bark_sfx_queries (n int, q text, bitmap bool DEFAULT false);
INSERT INTO bark_sfx_queries (n, q) VALUES
  (1, 'SELECT a, count(*), sum(b) FROM bark_sfx WHERE a = 3 GROUP BY a'),
  (2, 'SELECT a, b, c FROM bark_sfx WHERE a = 3 AND b = 500 ORDER BY a, b, c'),
  (3, 'SELECT a, b, c FROM bark_sfx WHERE a = 3 AND b = 17 AND c IS NOT NULL ORDER BY a, b, c'),
  (4, 'SELECT a, b, c FROM bark_sfx WHERE a = 5 AND b BETWEEN 100 AND 300 ORDER BY a, b, c'),
  (5, 'SELECT a, b, c FROM bark_sfx WHERE a >= 4 AND a < 6 ORDER BY a, b, c'),
  (6, 'SELECT a, b, c FROM bark_sfx WHERE a > 7 ORDER BY a, b, c'),
  (7, 'SELECT a, b, c FROM bark_sfx WHERE a = 6 AND b >= 990 ORDER BY a, b, c'),
  (8, 'SELECT a, b, c FROM bark_sfx WHERE a = 2 ORDER BY a DESC, b DESC, c DESC'),
  (9, 'SELECT a, b, c FROM bark_sfx WHERE a <= 1 ORDER BY a DESC, b DESC, c DESC'),
  (10, 'SELECT a, b, c FROM bark_sfx WHERE a = ANY (''{1,4,8}'') ORDER BY a, b, c'),
  (11, 'SELECT a, b, c FROM bark_sfx WHERE a = 2 AND b = ANY (''{0,17,500,999,1000}'') ORDER BY a, b, c'),
  (12, 'SELECT a, b FROM bark_sfx WHERE a = 7 ORDER BY a, b'),
  (13, 'SELECT a, b, c FROM bark_sfx WHERE a = 3 AND b IS NULL ORDER BY a, b, c'),
  (14, 'SELECT a, b, c FROM bark_sfx WHERE a = 4 AND b = 250 AND c IS NULL ORDER BY a, b, c'),
  (15, 'SELECT a, b, c FROM bark_sfx WHERE a = 8 AND b >= 900 ORDER BY a, b, c'),
  (16, 'SELECT count(*), count(DISTINCT (a, b)) FROM bark_sfx WHERE a IS NOT NULL'),
  (17, 'SELECT s.a, count(t.a) FROM (SELECT DISTINCT a FROM bark_sfx_src) s
          LEFT JOIN bark_sfx t ON t.a = s.a GROUP BY s.a ORDER BY s.a'),
  (18, 'SELECT count(t.a), count(*) FILTER (WHERE t.a IS NULL)
          FROM (SELECT DISTINCT a, b FROM bark_sfx_src WHERE b % 10 = 0) s
          LEFT JOIN bark_sfx t ON t.a = s.a AND t.b = s.b'),
  (19, 'SELECT count(t.a), count(*) FILTER (WHERE t.a IS NULL)
          FROM (SELECT a, b, c FROM bark_sfx_src WHERE id % 50 = 0) s
          LEFT JOIN bark_sfx t ON t.a = s.a AND t.b = s.b AND t.c = s.c'),
  (20, 'SELECT a, d, count(*) FROM bark_sfx WHERE a = 6 AND d = 3 GROUP BY a, d');
INSERT INTO bark_sfx_queries VALUES
  (21, 'SELECT count(*) FROM bark_sfx WHERE a = 3 AND b = 500', true),
  (22, 'SELECT count(*) FROM bark_sfx WHERE a = ANY (''{0,9}'') AND b < 100', true);

-- Run every query against each index definition, built by CREATE INDEX or
-- by inserting the rows in random order into an empty index.  The (a, b)
-- and (a, d) indexes get LIST and POSTING entries either way.  A
-- definition cannot answer every query (the (a, d) index, for one, has no
-- b), so only queries that use the index are reported.
CREATE TABLE bark_sfx_defs (n int, def text, how text);
INSERT INTO bark_sfx_defs VALUES
  (1, '(a, b, c)', 'build'),
  (1, '(a, b, c)', 'insert'),
  (2, '(a DESC, b, c DESC)', 'build'),
  (2, '(a DESC, b, c DESC)', 'insert'),
  (3, '(a, b NULLS FIRST, c)', 'insert'),
  (4, '(a, b) INCLUDE (c)', 'build'),
  (5, '(a, b)', 'build'),
  (5, '(a, b)', 'insert'),
  (6, '(a, d)', 'build'),
  (6, '(a, d)', 'insert');
CREATE TABLE bark_sfx_results (def int, how text, q int, result text);
DO $$
DECLARE
  d record;
BEGIN
  FOR d IN SELECT * FROM bark_sfx_defs ORDER BY n, how LOOP
      TRUNCATE bark_sfx;
      IF d.how = 'build' THEN
        INSERT INTO bark_sfx SELECT * FROM bark_sfx_src;
        EXECUTE 'CREATE INDEX bark_sfx_idx ON bark_sfx USING bark ' || d.def;
      ELSE
        EXECUTE 'CREATE INDEX bark_sfx_idx ON bark_sfx USING bark ' || d.def;
        INSERT INTO bark_sfx SELECT * FROM bark_sfx_src ORDER BY md5(id::text);
      END IF;
      PERFORM bark_index_check('bark_sfx_idx');
      INSERT INTO bark_sfx_results
        SELECT d.n, d.how, q.n, bark_sfx_check(q.q, q.bitmap)
        FROM bark_sfx_queries q;
      DROP INDEX bark_sfx_idx;
  END LOOP;
END $$;
SELECT def, how, count(*) FILTER (WHERE result = 'ok') AS ok,
       count(*) FILTER (WHERE result = 'no index') AS unused,
       string_agg(q::text, ',' ORDER BY q) FILTER (WHERE result = 'MISMATCH')
         AS mismatches
  FROM bark_sfx_results GROUP BY def, how ORDER BY def, how DESC;

-- A unique index whose pivots are truncated: re-inserting every row with ON
-- CONFLICT finds each existing key, including the keys on either side of a
-- truncated pivot, whether the key routes left or right of it.
CREATE TABLE bark_sfx_u (a int, b int, c text, v int)
  WITH (autovacuum_enabled = off);
CREATE UNIQUE INDEX bark_sfx_u_idx ON bark_sfx_u USING bark (a, b, c);
INSERT INTO bark_sfx_u
  SELECT a, b, c, 0 FROM bark_sfx_src WHERE b IS NOT NULL AND c IS NOT NULL
  ORDER BY md5(id::text);
INSERT INTO bark_sfx_u
  SELECT a, b, c, 1 FROM bark_sfx_src WHERE b IS NOT NULL AND c IS NOT NULL
  ON CONFLICT DO NOTHING;
INSERT INTO bark_sfx_u
  SELECT a, b, c, 2 FROM bark_sfx_src WHERE b IS NOT NULL AND c IS NOT NULL
  ON CONFLICT (a, b, c) DO UPDATE SET v = excluded.v;
SELECT v, count(*) FROM bark_sfx_u GROUP BY v ORDER BY v;
SELECT bark_index_check('bark_sfx_u_idx');
INSERT INTO bark_sfx_u
  SELECT a, b, c, 3 FROM bark_sfx_u WHERE a = 4 ORDER BY a, b, c LIMIT 1;
DROP TABLE bark_sfx_u;

-- Truncation is what keeps internal pages small.  With a wide second column
-- and a first column that differs between most neighbors, nearly every pivot
-- keeps only the first column, so the index is about the size of a btree
-- index on the same rows (btree truncates too).  Keeping both columns in
-- every pivot made the BARK index about twice as large.
CREATE TABLE bark_sfx_wide (a int, w text) WITH (autovacuum_enabled = off);
INSERT INTO bark_sfx_wide
  SELECT g / 2, (SELECT string_agg(md5(g::text || i::text), '')
                 FROM generate_series(1, 60) i)
  FROM generate_series(1, 1200) g;
CREATE INDEX bark_sfx_wide_idx ON bark_sfx_wide USING bark (a, w);
CREATE INDEX bark_sfx_wide_bt ON bark_sfx_wide USING btree (a, w);
SELECT pg_relation_size('bark_sfx_wide_idx') <=
       pg_relation_size('bark_sfx_wide_bt') * 1.1 AS build_like_btree;
SELECT bark_index_check('bark_sfx_wide_idx');
DROP INDEX bark_sfx_wide_idx;
DROP INDEX bark_sfx_wide_bt;
TRUNCATE bark_sfx_wide;
CREATE INDEX bark_sfx_wide_idx ON bark_sfx_wide USING bark (a, w);
CREATE INDEX bark_sfx_wide_bt ON bark_sfx_wide USING btree (a, w);
SELECT setseed(0.7);
INSERT INTO bark_sfx_wide
  SELECT g / 2, (SELECT string_agg(md5(g::text || i::text), '')
                 FROM generate_series(1, 60) i)
  FROM generate_series(1, 1200) g ORDER BY random();
SELECT pg_relation_size('bark_sfx_wide_idx') <=
       pg_relation_size('bark_sfx_wide_bt') * 1.1 AS insert_like_btree;
SELECT bark_index_check('bark_sfx_wide_idx');
DROP INDEX bark_sfx_wide_bt;
SELECT bark_sfx_check('SELECT a, length(w) FROM bark_sfx_wide WHERE a >= 300 ORDER BY a, w');
SELECT bark_sfx_check('SELECT a, length(w) FROM bark_sfx_wide WHERE a = 450');
DROP TABLE bark_sfx_wide;

-- A lower-bound scan whose bound equals a truncated pivot.  Each value of a
-- has four rows, inserted in random order, so nearly every leaf split falls
-- between two values of a and its high key is (a) alone.  An equality or
-- lower-bound scan on a value whose run starts a leaf then has a bound equal
-- to a pivot; the descent starts one leaf to the left of the run, and the
-- scan must still return the whole run.
CREATE TABLE bark_sfx_lb (a int, b int) WITH (autovacuum_enabled = off);
CREATE INDEX bark_sfx_lb_idx ON bark_sfx_lb USING bark (a, b);
INSERT INTO bark_sfx_lb
  SELECT g / 4, g FROM generate_series(0, 11999) g ORDER BY md5(g::text);
SELECT bark_index_check('bark_sfx_lb_idx');
SELECT bark_sfx_check('SELECT s.a, count(t.a) FROM generate_series(0, 3000) s(a)
  LEFT JOIN bark_sfx_lb t ON t.a = s.a GROUP BY s.a ORDER BY s.a');
SELECT bark_sfx_check('SELECT s.a, count(t.a) FROM generate_series(0, 3000) s(a)
  LEFT JOIN bark_sfx_lb t ON t.a = s.a AND t.b >= s.a * 4 + 2 GROUP BY s.a ORDER BY s.a');
SELECT bark_sfx_check('SELECT a, b FROM bark_sfx_lb WHERE a >= 1234 AND a < 1240 ORDER BY a, b');
DROP TABLE bark_sfx_lb;

-- OVERSIZED keys in the first column: a pivot keeps the oversized value when
-- it is one of the attributes it needs, and stays OVERSIZED, recording the
-- attributes it kept; otherwise the pivot drops it.  (Storage is plain so
-- the keys are not compressed into short inline entries.)
CREATE TABLE bark_sfx_big (t text COLLATE "C", b int)
  WITH (autovacuum_enabled = off);
ALTER TABLE bark_sfx_big ALTER t SET STORAGE plain;
CREATE INDEX bark_sfx_big_idx ON bark_sfx_big USING bark (t, b);
INSERT INTO bark_sfx_big
  SELECT CASE WHEN g % 3 = 0
              THEN lpad((g / 6)::text, 4, '0') ||
                   (SELECT string_agg(md5((g / 6)::text || i::text), '')
                    FROM generate_series(1, 100) i)
              ELSE lpad((g / 6)::text, 4, '0') END,
         g
  FROM generate_series(1, 3000) g ORDER BY md5(g::text);
SELECT bark_index_check('bark_sfx_big_idx');
SELECT bark_sfx_check('SELECT left(t, 8), length(t), b FROM bark_sfx_big WHERE t >= ''0100'' ORDER BY t, b');
SELECT bark_sfx_check('SELECT left(t, 8), length(t), b FROM bark_sfx_big ORDER BY t DESC, b DESC');
SELECT bark_sfx_check('SELECT count(x.b), count(*) FILTER (WHERE x.b IS NULL)
  FROM (SELECT t, b FROM bark_sfx_big WHERE b % 7 = 0) s
  LEFT JOIN bark_sfx_big x ON x.t = s.t AND x.b = s.b');
SELECT bark_sfx_check('SELECT left(t, 8), b FROM bark_sfx_big WHERE t >= ''0300'' AND t < ''0302'' ORDER BY t, b');
DROP TABLE bark_sfx_big;

DROP TABLE bark_sfx_results;
DROP TABLE bark_sfx_defs;
DROP TABLE bark_sfx_queries;
DROP TABLE bark_sfx;
DROP TABLE bark_sfx_src;
DROP FUNCTION bark_sfx_check(text, bool);

-- ===========================================================================
-- CREATE INDEX forms LIST and POSTING entries where insert would coalesce.
-- The sort delivers each run of equal keys with its heap TIDs ascending, and
-- the build writes the run as the entries insert would choose, each at most
-- a tenth of a page, as nbtree's build limits its posting lists.  Page counts
-- are compared as booleans with the same rows inserted into an existing
-- index; bark_bld_check compares a query through a bark_bld index with a
-- sequential scan, as bark_sfx_check does.
-- ===========================================================================
CREATE FUNCTION bark_bld_check(q text, bitmap bool DEFAULT false)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  plan text;
  viaidx text;
  viaseq text;
BEGIN
  PERFORM set_config('enable_seqscan', 'off', true);
  PERFORM set_config('enable_indexscan', (NOT bitmap)::text, true);
  PERFORM set_config('enable_indexonlyscan', (NOT bitmap)::text, true);
  PERFORM set_config('enable_bitmapscan', bitmap::text, true);
  EXECUTE 'EXPLAIN (COSTS OFF, FORMAT JSON) ' || q INTO plan;
  EXECUTE 'SELECT md5(string_agg(x::text, '' '')) FROM (' || q || ') x'
    INTO viaidx;
  PERFORM set_config('enable_seqscan', 'on', true);
  PERFORM set_config('enable_indexscan', 'off', true);
  PERFORM set_config('enable_indexonlyscan', 'off', true);
  PERFORM set_config('enable_bitmapscan', 'off', true);
  EXECUTE 'SELECT md5(string_agg(x::text, '' '')) FROM (' || q || ') x'
    INTO viaseq;
  IF position('"Index Name": "bark_bld' IN plan) = 0 THEN
    RETURN 'no index';
  END IF;
  RETURN CASE WHEN viaidx IS NOT DISTINCT FROM viaseq THEN 'ok'
              ELSE 'MISMATCH' END;
END $$;

-- One hot key; 1000 keys with each key's rows together (and some NULL keys
-- scattered among them); 1000 keys round-robin, so each key's rows are
-- scattered over the heap.
CREATE TABLE bark_bld_one (k int, v int) WITH (autovacuum_enabled = off);
INSERT INTO bark_bld_one SELECT 1, g FROM generate_series(1, 100000) g;
CREATE TABLE bark_bld_kc (k int, v int) WITH (autovacuum_enabled = off);
INSERT INTO bark_bld_kc
  SELECT CASE WHEN g % 1001 = 0 THEN NULL ELSE (g - 1) / 100 END, g
  FROM generate_series(1, 100000) g;
CREATE TABLE bark_bld_ks (k int, v int) WITH (autovacuum_enabled = off);
INSERT INTO bark_bld_ks SELECT g % 1000, g FROM generate_series(1, 100000) g;
CREATE INDEX bark_bld_one_idx ON bark_bld_one USING bark (k);
CREATE INDEX bark_bld_kc_idx ON bark_bld_kc USING bark (k);
CREATE INDEX bark_bld_ks_idx ON bark_bld_ks USING bark (k);
CREATE TABLE bark_bld_one_r (k int, v int) WITH (autovacuum_enabled = off);
CREATE INDEX bark_bld_one_r_idx ON bark_bld_one_r USING bark (k);
INSERT INTO bark_bld_one_r SELECT * FROM bark_bld_one;
CREATE TABLE bark_bld_kc_r (k int, v int) WITH (autovacuum_enabled = off);
CREATE INDEX bark_bld_kc_r_idx ON bark_bld_kc_r USING bark (k);
INSERT INTO bark_bld_kc_r SELECT * FROM bark_bld_kc;
CREATE TABLE bark_bld_ks_r (k int, v int) WITH (autovacuum_enabled = off);
CREATE INDEX bark_bld_ks_r_idx ON bark_bld_ks_r USING bark (k);
INSERT INTO bark_bld_ks_r SELECT * FROM bark_bld_ks;
SELECT pg_relation_size('bark_bld_one_idx') <=
       pg_relation_size('bark_bld_one_r_idx') * 1.5 AS one_like_insert,
       pg_relation_size('bark_bld_kc_idx') <=
       pg_relation_size('bark_bld_kc_r_idx') * 1.5 AS kc_like_insert,
       pg_relation_size('bark_bld_ks_idx') <=
       pg_relation_size('bark_bld_ks_r_idx') * 1.5 AS ks_like_insert;
DROP TABLE bark_bld_one_r, bark_bld_kc_r, bark_bld_ks_r;
SELECT bark_index_check('bark_bld_one_idx');
SELECT bark_index_check('bark_bld_kc_idx');
SELECT bark_index_check('bark_bld_ks_idx');
-- A parallel build reads the same sorted stream, so it writes the same tree.
SET min_parallel_table_scan_size = 0;
SET max_parallel_maintenance_workers = 4;
CREATE INDEX bark_bld_ks_par ON bark_bld_ks USING bark (k);
RESET min_parallel_table_scan_size;
RESET max_parallel_maintenance_workers;
SELECT pg_relation_size('bark_bld_ks_par') =
       pg_relation_size('bark_bld_ks_idx') AS parallel_same_size;
SELECT bark_index_check('bark_bld_ks_par');
DROP INDEX bark_bld_ks_par;

-- Runs longer than one entry.  The hot key's rows fill many POSTING
-- entries.  Here each heap page holds about one row of a key, so the set
-- stays a LIST, and 400 rows of a key take three of them.  A key too wide
-- for a LIST of two stays SINGLE, and a narrower wide key forms a short LIST.
CREATE TABLE bark_bld_sparse (k int, v int, pad text) WITH (autovacuum_enabled = off);
INSERT INTO bark_bld_sparse SELECT g % 20, g, repeat('x', 400)
  FROM generate_series(1, 8000) g;
CREATE INDEX bark_bld_sparse_idx ON bark_bld_sparse USING bark (k);
SELECT bark_index_check('bark_bld_sparse_idx');
CREATE TABLE bark_bld_wkey (t text) WITH (autovacuum_enabled = off);
ALTER TABLE bark_bld_wkey ALTER t SET STORAGE plain;
INSERT INTO bark_bld_wkey
  SELECT lpad(n::text, 4, '0') ||
         (SELECT string_agg(md5(n::text || i::text), '')
          FROM generate_series(1, CASE WHEN n % 2 = 0 THEN 28 ELSE 12 END) i)
  FROM generate_series(1, 60) n, generate_series(1, 10) r;
CREATE INDEX bark_bld_wkey_idx ON bark_bld_wkey USING bark (t);
SELECT bark_index_check('bark_bld_wkey_idx');

CREATE TABLE bark_bld_queries (n int, q text, bitmap bool);
INSERT INTO bark_bld_queries VALUES
  (1, 'SELECT k, count(*) FROM bark_bld_one WHERE k >= 0 GROUP BY k', false),
  (2, 'SELECT count(*), sum(v) FROM bark_bld_one WHERE k = 1', true),
  (3, 'SELECT k, count(*) FROM bark_bld_kc WHERE k >= 0 GROUP BY k ORDER BY k', false),
  (4, 'SELECT count(*) FROM bark_bld_kc WHERE k IS NULL', false),
  (5, 'SELECT count(*), sum(v) FROM bark_bld_kc WHERE k BETWEEN 100 AND 199', true),
  (6, 'SELECT count(*), sum(v) FROM bark_bld_kc WHERE k IS NULL', true),
  (7, 'SELECT k, count(*) FROM bark_bld_ks WHERE k >= 0 GROUP BY k ORDER BY k', false),
  (8, 'SELECT k FROM bark_bld_ks WHERE k BETWEEN 10 AND 12 ORDER BY k DESC', false),
  (9, 'SELECT count(*), sum(v) FROM bark_bld_ks WHERE k BETWEEN 100 AND 199', true),
  (10, 'SELECT k, count(*) FROM bark_bld_sparse WHERE k >= 0 GROUP BY k ORDER BY k', false),
  (11, 'SELECT count(*), sum(length(pad)) FROM bark_bld_sparse WHERE k = 7', true),
  (12, 'SELECT left(t, 4), length(t), count(*) FROM bark_bld_wkey WHERE t >= ''0020'' GROUP BY t ORDER BY t', false),
  (13, 'SELECT count(*) FROM bark_bld_wkey WHERE t >= ''0020''', true);
SELECT n, bark_bld_check(q, bitmap) FROM bark_bld_queries ORDER BY n;

-- VACUUM after the build.  Built POSTING entries carry the removal reserve
-- (bark_form_posting sizes them for it), so VACUUM rewrites every entry in
-- place; amcheck checks the reserve and the item ceiling.
DELETE FROM bark_bld_one WHERE v % 3 = 0;
DELETE FROM bark_bld_kc WHERE v % 3 = 0;
DELETE FROM bark_bld_ks WHERE v % 3 = 0;
DELETE FROM bark_bld_sparse WHERE v % 3 = 0;
VACUUM bark_bld_one, bark_bld_kc, bark_bld_ks, bark_bld_sparse;
SELECT bark_index_check('bark_bld_one_idx');
SELECT bark_index_check('bark_bld_kc_idx');
SELECT bark_index_check('bark_bld_ks_idx');
SELECT bark_index_check('bark_bld_sparse_idx');
SELECT n, bark_bld_check(q, bitmap) FROM bark_bld_queries WHERE n <= 11 ORDER BY n;
-- Insert then grows the built entries, with the new rows' TIDs landing in
-- the space VACUUM freed, inside the existing sets.
INSERT INTO bark_bld_one SELECT 1, -g FROM generate_series(1, 20000) g;
INSERT INTO bark_bld_ks SELECT g % 1000, -g FROM generate_series(1, 20000) g;
SELECT bark_index_check('bark_bld_one_idx');
SELECT bark_index_check('bark_bld_ks_idx');
SELECT n, bark_bld_check(q, bitmap) FROM bark_bld_queries
  WHERE n IN (1, 2, 7, 8, 9) ORDER BY n;
DROP TABLE bark_bld_queries;
DROP TABLE bark_bld_one, bark_bld_kc, bark_bld_ks, bark_bld_sparse, bark_bld_wkey;

-- Where insert does not coalesce, neither does the build.  A unique index
-- holds equal keys only when they are NULL, and builds the same tree as an
-- index whose INCLUDE column (here also NULL, so every tuple has the same
-- size) rules coalescing out; a plain index on the same rows coalesces
-- them.  An INCLUDE index returns each row's own payload.
CREATE TABLE bark_bld_nc (k int, z int, p text) WITH (autovacuum_enabled = off);
INSERT INTO bark_bld_nc SELECT NULL, NULL, 'row' || g FROM generate_series(1, 20000) g;
CREATE UNIQUE INDEX bark_bld_nc_u ON bark_bld_nc USING bark (k);
CREATE INDEX bark_bld_nc_inc ON bark_bld_nc USING bark (k) INCLUDE (z);
CREATE INDEX bark_bld_nc_plain ON bark_bld_nc USING bark (k);
SELECT pg_relation_size('bark_bld_nc_u') =
       pg_relation_size('bark_bld_nc_inc') AS unique_single_only,
       pg_relation_size('bark_bld_nc_plain') * 4 <
       pg_relation_size('bark_bld_nc_u') AS plain_coalesced;
SELECT bark_index_check('bark_bld_nc_u');
SELECT bark_index_check('bark_bld_nc_plain');
TRUNCATE bark_bld_nc;
DROP INDEX bark_bld_nc_u, bark_bld_nc_inc, bark_bld_nc_plain;
INSERT INTO bark_bld_nc SELECT 1, NULL, 'row' || g FROM generate_series(1, 5) g;
CREATE INDEX bark_bld_nc_idx ON bark_bld_nc USING bark (k) INCLUDE (p);
SET enable_seqscan = off;
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF) SELECT k, p FROM bark_bld_nc WHERE k = 1;
SELECT k, p FROM bark_bld_nc WHERE k = 1 ORDER BY p;
RESET enable_seqscan;
RESET enable_bitmapscan;
DROP TABLE bark_bld_nc;
DROP FUNCTION bark_bld_check(text, bool);

-- Parallel VACUUM (amparallelvacuumoptions): bulk delete and cleanup of BARK
-- indexes in parallel workers.  Two indexes above min_parallel_index_scan_size
-- make the VACUUM parallel, as in vacuum_parallel.sql.  The result must be the
-- same as a serial VACUUM's: dead entries gone, emptied leaves deleted and
-- later reused, amcheck clean, index counts equal to a sequential scan.
SET max_parallel_maintenance_workers TO 4;
SET min_parallel_index_scan_size TO '128kB';
CREATE TABLE bark_pvac (a int, b int) WITH (autovacuum_enabled = off);
INSERT INTO bark_pvac SELECT g, g % 100 FROM generate_series(1, 40000) g;
CREATE INDEX bark_pvac_a ON bark_pvac USING bark (a);
CREATE INDEX bark_pvac_b ON bark_pvac USING bark (b);
CREATE INDEX bark_pvac_ab ON bark_pvac USING bark (a, b);
SELECT count(*) AS parallel_eligible_indexes
FROM pg_class
WHERE oid IN ('bark_pvac_a'::regclass, 'bark_pvac_b'::regclass,
              'bark_pvac_ab'::regclass) AND
  pg_relation_size(oid) >=
  pg_size_bytes(current_setting('min_parallel_index_scan_size'));
DELETE FROM bark_pvac WHERE a BETWEEN 5000 AND 30000 OR b = 7;
VACUUM (PARALLEL 4, INDEX_CLEANUP ON) bark_pvac;
SELECT bark_index_check('bark_pvac_a'), bark_index_check('bark_pvac_b'),
       bark_index_check('bark_pvac_ab');
-- Cleanup alone in parallel (no dead rows, so no bulk delete).
VACUUM (PARALLEL 4, INDEX_CLEANUP ON) bark_pvac;
INSERT INTO bark_pvac SELECT g, g % 100 FROM generate_series(5000, 30000) g;
SELECT bark_index_check('bark_pvac_a'), bark_index_check('bark_pvac_ab');
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) AS idx_a FROM bark_pvac WHERE a > 0;
SELECT count(*) AS idx_b FROM bark_pvac WHERE b = 7;
SELECT count(*) AS idx_ab FROM bark_pvac WHERE a > 0 AND b >= 0;
RESET enable_seqscan;
RESET enable_bitmapscan;
SET enable_indexscan = off;
SET enable_indexonlyscan = off;
SET enable_bitmapscan = off;
SELECT count(*) AS seq_a FROM bark_pvac WHERE a > 0;
SELECT count(*) AS seq_b FROM bark_pvac WHERE b = 7;
RESET enable_indexscan;
RESET enable_indexonlyscan;
RESET enable_bitmapscan;
RESET max_parallel_maintenance_workers;
RESET min_parallel_index_scan_size;
DROP TABLE bark_pvac;
-- ===========================================================================
-- Mark/restore (ammarkpos/amrestrpos).  A merge join marks its inner scan at
-- the first inner row of each run of equal keys and restores the mark for
-- every further outer row with that key, so a BARK index scan can be the
-- inner side directly, with no Materialize node over it.  The outer side
-- here is an ordered subquery, which cannot itself be restored, so the plans
-- put the BARK index on the inner side.  Each outer key appears three times,
-- so each inner run is read once and restored twice.
--
-- bark_mj_check runs a query as a merge join and as a hash join and compares
-- the results; it reports 'not merge inner' when the merge plan does not
-- have the named index directly under a merge join's inner side.
-- ===========================================================================
CREATE FUNCTION bark_mj_check(q text, idx text)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  plan jsonb;
  viamerge text;
  viahash text;
BEGIN
  PERFORM set_config('enable_hashjoin', 'off', true);
  PERFORM set_config('enable_nestloop', 'off', true);
  PERFORM set_config('enable_material', 'off', true);
  PERFORM set_config('enable_sort', 'off', true);
  PERFORM set_config('enable_bitmapscan', 'off', true);
  EXECUTE 'EXPLAIN (COSTS OFF, FORMAT JSON) ' || q INTO plan;
  EXECUTE 'SELECT x::text FROM (' || q || ') x' INTO viamerge;
  PERFORM set_config('enable_mergejoin', 'off', true);
  PERFORM set_config('enable_hashjoin', 'on', true);
  PERFORM set_config('enable_sort', 'on', true);
  EXECUTE 'SELECT x::text FROM (' || q || ') x' INTO viahash;
  IF NOT jsonb_path_exists(plan,
         '$.** ? (@."Node Type" == "Merge Join" && @.Plans[1]."Index Name" == $i)',
         jsonb_build_object('i', idx)) THEN
    RETURN 'not merge inner';
  END IF;
  RETURN CASE WHEN viamerge IS NOT DISTINCT FROM viahash
              THEN 'ok ' || viamerge
              ELSE 'MISMATCH ' || viamerge || ' vs ' || viahash END;
END $$;

-- The outer side: keys 0..119, three rows each (keys 100..119 match nothing).
CREATE TABLE bark_mj_o (k int, a int, b int) WITH (autovacuum_enabled = off);
INSERT INTO bark_mj_o SELECT g % 120, g % 25, g % 9 FROM generate_series(1, 360) g;
-- LIST keys: 100 keys, each key's rows scattered over the heap.
CREATE TABLE bark_mj_list (k int, v int) WITH (autovacuum_enabled = off);
INSERT INTO bark_mj_list SELECT g % 100, g FROM generate_series(1, 20000) g;
CREATE INDEX bark_mj_list_idx ON bark_mj_list USING bark (k);
-- POSTING keys: 100 keys, each key's 400 rows together on the heap.
CREATE TABLE bark_mj_post (k int, v int) WITH (autovacuum_enabled = off);
INSERT INTO bark_mj_post SELECT k, g FROM generate_series(0, 99) k, generate_series(1, 400) g;
CREATE INDEX bark_mj_post_idx ON bark_mj_post USING bark (k);
-- NULL keys (one row in ten) and a two-column key; the same rows under a
-- DESC column.
CREATE TABLE bark_mj_misc (k int, a int, b int, v int) WITH (autovacuum_enabled = off);
INSERT INTO bark_mj_misc
  SELECT CASE WHEN g % 10 = 0 THEN NULL ELSE g % 100 END, g % 20, g % 7, g
  FROM generate_series(1, 20000) g;
CREATE INDEX bark_mj_null_idx ON bark_mj_misc USING bark (k);
CREATE INDEX bark_mj_ab_idx ON bark_mj_misc USING bark (a, b);
CREATE TABLE bark_mj_desc (k int, v int) WITH (autovacuum_enabled = off);
INSERT INTO bark_mj_desc SELECT k, v FROM bark_mj_misc;
CREATE INDEX bark_mj_desc_idx ON bark_mj_desc USING bark (k DESC);
-- Wide INCLUDE rows (no coalescing): key 50's 3000 rows span many leaves.
-- Each row's payload p is its own number, so an index-only scan that returns
-- another row's copy changes the sum.
CREATE TABLE bark_mj_wide (k int, v int, p text) WITH (autovacuum_enabled = off);
INSERT INTO bark_mj_wide
  SELECT CASE WHEN g % 2 = 0 THEN 50 ELSE g % 100 END, g, lpad(g::text, 100, '0')
  FROM generate_series(1, 6000) g;
CREATE INDEX bark_mj_wide_idx ON bark_mj_wide USING bark (k) INCLUDE (p);
VACUUM ANALYZE bark_mj_o, bark_mj_list, bark_mj_post, bark_mj_misc, bark_mj_desc,
  bark_mj_wide;
SELECT pg_relation_size('bark_mj_wide_idx') / 8192 > 40 AS wide_run_spans_leaves;

-- The inner BARK scans need no Materialize: neither an index scan nor an
-- index-only scan.
SET enable_hashjoin = off;
SET enable_nestloop = off;
SET enable_material = off;
SET enable_sort = off;
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF)
SELECT count(*), sum(i.v)
  FROM (SELECT k FROM bark_mj_o ORDER BY k OFFSET 0) o
  JOIN bark_mj_list i ON o.k = i.k;
EXPLAIN (COSTS OFF)
SELECT count(*), sum(i.p::int)
  FROM (SELECT k FROM bark_mj_o ORDER BY k OFFSET 0) o
  JOIN bark_mj_wide i ON o.k = i.k;
RESET enable_hashjoin;
RESET enable_nestloop;
RESET enable_material;
RESET enable_sort;
RESET enable_bitmapscan;

-- Merge join equals hash join.  With a filter on the inner side, the first
-- member of a LIST or POSTING entry is often rejected, so the mark falls on a
-- later member of the entry, and the scan must resume at that member.
SELECT bark_mj_check('SELECT count(*), sum(i.v)
  FROM (SELECT k FROM bark_mj_o ORDER BY k OFFSET 0) o
  JOIN bark_mj_list i ON o.k = i.k', 'bark_mj_list_idx') AS list;
SELECT bark_mj_check('SELECT count(*), sum(i.v)
  FROM (SELECT k FROM bark_mj_o ORDER BY k OFFSET 0) o
  JOIN bark_mj_list i ON o.k = i.k AND i.v % 3 <> 1', 'bark_mj_list_idx') AS list_mid;
SELECT bark_mj_check('SELECT count(*), sum(i.v)
  FROM (SELECT k FROM bark_mj_o ORDER BY k OFFSET 0) o
  JOIN bark_mj_post i ON o.k = i.k', 'bark_mj_post_idx') AS post;
SELECT bark_mj_check('SELECT count(*), sum(i.v)
  FROM (SELECT k FROM bark_mj_o ORDER BY k OFFSET 0) o
  JOIN bark_mj_post i ON o.k = i.k AND i.v % 3 <> 1', 'bark_mj_post_idx') AS post_mid;
SELECT bark_mj_check('SELECT count(*), sum(i.k)
  FROM (SELECT k FROM bark_mj_o ORDER BY k OFFSET 0) o
  JOIN bark_mj_post i ON o.k = i.k', 'bark_mj_post_idx') AS post_ios;
SELECT bark_mj_check('SELECT count(*), sum(i.v), count(o.k), count(i.k)
  FROM (SELECT k FROM bark_mj_o ORDER BY k OFFSET 0) o
  FULL JOIN bark_mj_misc i ON o.k = i.k', 'bark_mj_null_idx') AS nulls;
SELECT bark_mj_check('SELECT count(*), sum(v) FROM
  (SELECT o.k, i.v FROM (SELECT k FROM bark_mj_o ORDER BY k DESC OFFSET 0) o
   JOIN bark_mj_desc i ON o.k = i.k ORDER BY o.k DESC) j',
  'bark_mj_desc_idx') AS desc_fwd;
SELECT bark_mj_check('SELECT count(*), sum(i.v)
  FROM (SELECT k FROM bark_mj_o ORDER BY k OFFSET 0) o
  JOIN bark_mj_desc i ON o.k = i.k', 'bark_mj_desc_idx') AS desc_bwd;
SELECT bark_mj_check('SELECT count(*), sum(i.v)
  FROM (SELECT a, b FROM bark_mj_o ORDER BY a, b OFFSET 0) o
  JOIN bark_mj_misc i ON o.a = i.a AND o.b = i.b', 'bark_mj_ab_idx') AS two_col;
-- Key 50's run spans many leaves, so the scan leaves the marked page and the
-- mark is restored from its saved copy, for a plain and an index-only scan.
SELECT bark_mj_check('SELECT count(*), sum(i.v)
  FROM (SELECT k FROM bark_mj_o ORDER BY k OFFSET 0) o
  JOIN bark_mj_wide i ON o.k = i.k', 'bark_mj_wide_idx') AS long_run;
SELECT bark_mj_check('SELECT count(*), sum(i.p::int)
  FROM (SELECT k FROM bark_mj_o ORDER BY k OFFSET 0) o
  JOIN bark_mj_wide i ON o.k = i.k', 'bark_mj_wide_idx') AS long_run_ios;
-- A leading-array (SAOP) inner scan, whose position carries the array cursor.
SELECT bark_mj_check('SELECT count(*), sum(i.v)
  FROM (SELECT k FROM bark_mj_o ORDER BY k OFFSET 0) o
  JOIN bark_mj_wide i ON o.k = i.k WHERE i.k = ANY (''{11,21,49,50,51,91}'')',
  'bark_mj_wide_idx') AS saop;
SELECT bark_mj_check('SELECT count(*), sum(i.v)
  FROM (SELECT k FROM bark_mj_o ORDER BY k OFFSET 0) o
  JOIN bark_mj_post i ON o.k = i.k WHERE i.k = ANY (''{3,17,40,41,77,99}'')',
  'bark_mj_post_idx') AS saop_post;

-- An ordered-operator (KNN) scan cannot be restored: the executor reorders
-- its tuples in a queue that restoring the index position does not rewind.
-- So a KNN scan (the planner considers one only for an ORDER BY on the
-- distance) is never a merge join's inner side on its own.  With Materialize
-- disabled, the plan puts it on the outer side, and the join returns what a
-- hash join does.
CREATE TABLE bark_mj_knn (a int) WITH (autovacuum_enabled = off);
INSERT INTO bark_mj_knn SELECT g FROM generate_series(1, 10000) g;
CREATE INDEX bark_mj_knn_idx ON bark_mj_knn USING bark (a);
CREATE TABLE bark_mj_dist (f float8) WITH (autovacuum_enabled = off);
INSERT INTO bark_mj_dist SELECT (g % 50)::float8 FROM generate_series(1, 300) g;
CREATE INDEX bark_mj_dist_idx ON bark_mj_dist USING btree (f);
VACUUM ANALYZE bark_mj_knn, bark_mj_dist;
SET enable_hashjoin = off;
SET enable_nestloop = off;
SET enable_material = off;
SET enable_sort = off;
SET enable_seqscan = off;
EXPLAIN (COSTS OFF)
SELECT d.f, i.a FROM bark_mj_dist d JOIN bark_mj_knn i ON d.f = (i.a <~> 5000)
  ORDER BY i.a <~> 5000;
SELECT count(*), sum(a) FROM
  (SELECT d.f, i.a FROM bark_mj_dist d JOIN bark_mj_knn i ON d.f = (i.a <~> 5000)
   ORDER BY i.a <~> 5000) x;
RESET enable_hashjoin;
RESET enable_nestloop;
RESET enable_material;
RESET enable_sort;
RESET enable_seqscan;
SET enable_mergejoin = off;
SELECT count(*), sum(a) FROM
  (SELECT d.f, i.a FROM bark_mj_dist d JOIN bark_mj_knn i ON d.f = (i.a <~> 5000)
   ORDER BY i.a <~> 5000) x;
RESET enable_mergejoin;
DROP TABLE bark_mj_o, bark_mj_list, bark_mj_post, bark_mj_misc, bark_mj_desc,
  bark_mj_wide, bark_mj_knn, bark_mj_dist;
DROP FUNCTION bark_mj_check(text, text);

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
  SELECT a FROM bark_knnsc ORDER BY a <~> 500 LIMIT 3;
RESET enable_seqscan;
DROP TABLE bark_knnx, bark_knnsc;

-- Skip scan: no key on column 1, a key bounding column 2.  The scan
-- re-descends past each column-1 group once column 2 is past its bounds
-- (or to the bounds within the group) instead of reading every leaf.  The
-- results match a sequential scan; bark_skip_stats shows the scan made a
-- descent per group and read well under half of the index.
CREATE TABLE bark_skip (a int, b int, t text);
INSERT INTO bark_skip SELECT g % 10, g, 'k' || (g % 10) ||
  CASE WHEN g % 10 = 7 THEN repeat('z', 3000) ELSE '' END
  FROM generate_series(1, 100000) g;
INSERT INTO bark_skip SELECT NULL, g, NULL FROM generate_series(1, 300) g;
CREATE INDEX bark_skip_ab ON bark_skip USING bark (a, b);
CREATE INDEX bark_skip_tb ON bark_skip USING bark (t DESC, b);
VACUUM ANALYZE bark_skip;
CREATE FUNCTION bark_skip_check(q text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  r1 text;
  r2 text;
BEGIN
  SET LOCAL enable_seqscan = off;
  SET LOCAL enable_bitmapscan = off;
  EXECUTE 'SELECT md5(string_agg(x::text, '','')) FROM (' || q || ') x' INTO r1;
  RESET enable_seqscan;
  SET LOCAL enable_indexscan = off;
  SET LOCAL enable_indexonlyscan = off;
  SET LOCAL enable_bitmapscan = off;
  EXECUTE 'SELECT md5(string_agg(x::text, '','')) FROM (' || q || ') x' INTO r2;
  RETURN CASE WHEN r1 = r2 THEN 'ok' ELSE 'mismatch' END;
END $$;
CREATE FUNCTION bark_skip_stats(q text, idx text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  plan json;
  node json;
BEGIN
  SET LOCAL enable_seqscan = off;
  SET LOCAL enable_bitmapscan = off;
  EXECUTE 'EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF, FORMAT JSON) ' || q
    INTO plan;
  node := plan->0->'Plan';
  WHILE node->>'Index Name' IS NULL LOOP
    node := node->'Plans'->0;
  END LOOP;
  RETURN format('searches>=10 %s, blocks<pages/2 %s',
    (node->>'Index Searches')::int >= 10,
    (node->>'Shared Hit Blocks')::int + (node->>'Shared Read Blocks')::int <
      pg_relation_size(idx::regclass) / current_setting('block_size')::int / 2);
END $$;
SELECT bark_skip_check('SELECT a, b FROM bark_skip WHERE b = 4321 ORDER BY a, b');
SELECT bark_skip_check('SELECT a, b FROM bark_skip WHERE b BETWEEN 5000 AND 5100 ORDER BY a, b');
SELECT bark_skip_check('SELECT a, b FROM bark_skip WHERE b BETWEEN 5000 AND 5100 ORDER BY a DESC, b DESC');
SELECT bark_skip_check('SELECT a, b FROM bark_skip WHERE b < 40 ORDER BY a NULLS FIRST, b');
SELECT bark_skip_check('SELECT a, b FROM bark_skip WHERE b > 99950 ORDER BY a, b');
SELECT bark_skip_check('SELECT a, b FROM bark_skip WHERE b >= 7000::int8 AND b < 7030::int8 AND b <> 7010::int2 ORDER BY a, b');
SELECT bark_skip_check('SELECT t, b FROM bark_skip WHERE b BETWEEN 300 AND 420 ORDER BY t DESC, b');
SELECT bark_skip_check('SELECT count(*) FROM bark_skip WHERE b BETWEEN 300 AND 420');
SELECT bark_skip_stats('SELECT a, b FROM bark_skip WHERE b = 4321', 'bark_skip_ab');
SELECT bark_skip_stats('SELECT a, b FROM bark_skip WHERE b BETWEEN 5000 AND 5100', 'bark_skip_ab');
SELECT bark_skip_stats('SELECT t, b FROM bark_skip WHERE b BETWEEN 5000 AND 5100', 'bark_skip_tb');
-- Column 1 is int4, whose opclass has skip support: one descent per group
-- (10 groups, then the NULL group past the last), each straight to
-- (next value, 5000), plus the first descent.
SET enable_seqscan = off;
SET enable_bitmapscan = off;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
  SELECT a, b FROM bark_skip WHERE b BETWEEN 5000 AND 5100;
RESET enable_bitmapscan;
RESET enable_seqscan;
-- A scroll cursor that reverses inside the scan returns the same rows as a
-- sort over a sequential scan.
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_sort = off;
BEGIN;
DECLARE c SCROLL CURSOR FOR
  SELECT a, b FROM bark_skip WHERE b BETWEEN 3000 AND 3400 ORDER BY a, b;
FETCH 3 FROM c;
FETCH BACKWARD 2 FROM c;
MOVE 20 IN c;
FETCH 2 FROM c;
FETCH BACKWARD 3 FROM c;
COMMIT;
RESET enable_sort;
RESET enable_bitmapscan;
RESET enable_seqscan;
SELECT a, b FROM bark_skip WHERE b BETWEEN 3000 AND 3400 ORDER BY a, b
  LIMIT 25;
DROP FUNCTION bark_skip_check(text), bark_skip_stats(text, text);
DROP TABLE bark_skip;
