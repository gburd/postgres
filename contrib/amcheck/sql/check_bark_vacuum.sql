--
-- VACUUM of BARK indexes: emptied leaves, POSTING entries rewritten in place,
-- deleted pages, and the counts VACUUM reports.
--
-- These cases check the index with bark_index_check() after the changes
-- they make, so they live here rather than in the core regression suite.
-- (The amcheck extension is created by the earlier "check" test.)
--

-- Deleting an empty interior leaf hands its key space to its right sibling in
-- the parent, as nbtree does.  Refilling the emptied range must leave every
-- key under the right downlink: amcheck verifies parent order, and the index
-- counts must equal a sequential scan's.  A temp table, so that no other
-- session's snapshot keeps VACUUM from removing the rows and deleting the
-- leaves.
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
CREATE TEMP TABLE bark_vgrow (a int, b int);
CREATE INDEX bark_vgrow_idx ON bark_vgrow USING bark (a);
CREATE TEMP TABLE bark_vgrow_cnt (k text, n bigint);
CREATE TEMP TABLE bark_vgrow_pages (k text, before bigint, after bigint);
CREATE FUNCTION bark_vgrow_counts(tag text) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  UPDATE bark_vgrow_pages SET after = pg_relation_size('bark_vgrow_idx') / current_setting('block_size')::int
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
           VALUES ('60000/10 vac1', pg_relation_size('bark_vgrow_idx') / current_setting('block_size')::int))
DELETE FROM bark_vgrow WHERE b % 10 = 0;
VACUUM bark_vgrow;
SELECT bark_index_check('bark_vgrow_idx');
SELECT bark_vgrow_counts('60000/10 vac1');
WITH p AS (INSERT INTO bark_vgrow_pages
           VALUES ('60000/10 vac2', pg_relation_size('bark_vgrow_idx') / current_setting('block_size')::int))
DELETE FROM bark_vgrow WHERE b % 10 = 5;
VACUUM bark_vgrow;
SELECT bark_index_check('bark_vgrow_idx');
SELECT bark_vgrow_counts('60000/10 vac2');
-- 20000 TIDs, every 2nd deleted: VACUUM used to succeed but leave a
-- 3424-byte entry, over the item ceiling.
TRUNCATE bark_vgrow;
INSERT INTO bark_vgrow SELECT 1, g FROM generate_series(1, 20000) g;
WITH p AS (INSERT INTO bark_vgrow_pages
           VALUES ('20000/2 vac1', pg_relation_size('bark_vgrow_idx') / current_setting('block_size')::int))
DELETE FROM bark_vgrow WHERE b % 2 = 0;
VACUUM bark_vgrow;
SELECT bark_index_check('bark_vgrow_idx');
SELECT bark_vgrow_counts('20000/2 vac1');
WITH p AS (INSERT INTO bark_vgrow_pages
           VALUES ('20000/2 vac2', pg_relation_size('bark_vgrow_idx') / current_setting('block_size')::int))
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
           VALUES ('120000/2 vac1', pg_relation_size('bark_vgrow_idx') / current_setting('block_size')::int))
DELETE FROM bark_vgrow WHERE b % 2 = 0;
VACUUM bark_vgrow;
SELECT bark_index_check('bark_vgrow_idx');
SELECT bark_vgrow_counts('120000/2 vac1');
WITH p AS (INSERT INTO bark_vgrow_pages
           VALUES ('120000/2 vac2', pg_relation_size('bark_vgrow_idx') / current_setting('block_size')::int))
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
  SELECT pg_relation_size('bark_dpg_idx') / current_setting('block_size')::int AS built_pages;
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
SELECT pg_relation_size('bark_dpg_idx') / current_setting('block_size')::int < built_pages + 10
         AS refill_reused_pages
  FROM bark_dpg_sz;
SELECT count(*) AS live_count FROM bark_dpg;
DROP TABLE bark_dpg, bark_dpg_sz;

-- VACUUM reports the index's live heap TIDs, as nbtree does, not its entries:
-- the planner reads pg_class.reltuples of a partial index as its row count.
-- A scan that only counts (no dead rows) cannot see a LIST or POSTING
-- entry's TIDs without decoding it, so its count is an estimate and leaves
-- reltuples alone.  A temp table, so that the deleted rows are removable.
CREATE TEMP TABLE bark_vcount (k int, v int);
INSERT INTO bark_vcount SELECT g % 10, g FROM generate_series(1, 100000) g;
CREATE INDEX bark_vcount_idx ON bark_vcount USING bark (k) WHERE v <= 50000;
VACUUM bark_vcount;
SELECT reltuples = (SELECT count(*) FROM bark_vcount WHERE v <= 50000)
  FROM pg_class WHERE relname = 'bark_vcount_idx';
DELETE FROM bark_vcount WHERE v % 7 = 0;
VACUUM bark_vcount;
SELECT reltuples = (SELECT count(*) FROM bark_vcount WHERE v <= 50000)
  FROM pg_class WHERE relname = 'bark_vcount_idx';
SELECT bark_index_check('bark_vcount_idx');
DROP TABLE bark_vcount;
