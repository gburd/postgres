--
-- Dead-entry hints and simple deletion.  An index scan whose heap fetch
-- finds a row dead to every snapshot (kill_prior_tuple) marks its entry
-- LP_DEAD when the scan leaves the page (bark_killitems); later scans skip
-- marked entries, and an insert that would split the leaf first deletes them
-- (bark_simple_delete).  A LIST or POSTING entry is marked only when every
-- one of its TIDs was killed.  Each table is a temp table, so that no other
-- session's snapshot keeps its dead rows visible, and each result is
-- compared with a sequential scan.
--
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS pageinspect;
CREATE EXTENSION IF NOT EXISTS amcheck;
CREATE EXTENSION IF NOT EXISTS bark_multikey;
RESET client_min_messages;

-- The implementation of an index, '-' for its own AM.
CREATE FUNCTION implam(idx regclass) RETURNS name
LANGUAGE sql AS $$
  SELECT coalesce((SELECT amname FROM pg_am WHERE oid = i.indimplam), '-')
  FROM pg_index i WHERE i.indexrelid = idx
$$;

-- A leaf's live and dead entries and whether it has BARK_HAS_GARBAGE.
CREATE FUNCTION lpd_page(idx regclass, blk int8,
  OUT live int, OUT dead int, OUT has_garbage bool)
LANGUAGE sql AS $$
  SELECT live_items, dead_items, 'has_garbage' = ANY (flags)
  FROM bark_page_stats(idx::text, blk)
$$;

-- The LP_DEAD entries of a leaf: offset, shape and TID count.  bark_page_items
-- shows no line pointer flags; heap_page_items reads any page's.
CREATE FUNCTION lpd_dead(idx regclass, blk int8)
RETURNS TABLE (itemoffset smallint, shape text, ntids int4)
LANGUAGE sql AS $$
  SELECT i.itemoffset, i.shape, i.ntids
  FROM bark_page_items(idx::text, blk) i
  JOIN heap_page_items(get_raw_page(idx::text, blk::int)) h
    ON h.lp = i.itemoffset
  WHERE h.lp_flags = 3 ORDER BY 1
$$;

-- Count by plain index scan, which kills dead rows, and by seqscan.
CREATE FUNCTION lpd_count(q text, OUT idx bigint, OUT seq bigint)
LANGUAGE plpgsql AS $$
BEGIN
  PERFORM set_config('enable_seqscan', 'off', true);
  PERFORM set_config('enable_bitmapscan', 'off', true);
  PERFORM set_config('enable_indexonlyscan', 'off', true);
  EXECUTE 'SELECT count(*) FROM (' || q || ') x' INTO idx;
  PERFORM set_config('enable_seqscan', 'on', true);
  PERFORM set_config('enable_indexscan', 'off', true);
  EXECUTE 'SELECT count(*) FROM (' || q || ') x' INTO seq;
  PERFORM set_config('enable_indexscan', 'on', true);
END $$;

-- SINGLE entries.  A unique index never coalesces.  Its one leaf is full
-- (fillfactor 100); the rows 100-300 are deleted, and the first index scan
-- over them marks their 201 entries.  The scan after it returns the same
-- rows without them.
CREATE TEMP TABLE lpd (a int, b int);
CREATE UNIQUE INDEX lpd_a ON lpd USING bark (a) WITH (fillfactor = 100);
INSERT INTO lpd SELECT g, g FROM generate_series(1, 400) g;
SELECT * FROM lpd_page('lpd_a', 1);
DELETE FROM lpd WHERE a BETWEEN 100 AND 300;
SELECT * FROM lpd_count('SELECT * FROM lpd WHERE a BETWEEN 50 AND 350');
SELECT * FROM lpd_page('lpd_a', 1);
SELECT count(*), min(itemoffset), max(itemoffset), min(shape), max(shape)
  FROM lpd_dead('lpd_a', 1);
SELECT * FROM lpd_count('SELECT * FROM lpd WHERE a BETWEEN 50 AND 350');
-- Backward and index-only scans skip the marked entries too.
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) FROM (SELECT a FROM lpd WHERE a BETWEEN 50 AND 350
  ORDER BY a DESC) x;
SELECT array_agg(a ORDER BY a DESC) FROM (SELECT a FROM lpd
  WHERE a BETWEEN 95 AND 305 ORDER BY a DESC) x;
-- A KNN scan skips them as well; it marks none itself.  Its cursor walks
-- from 200 out over every marked entry, in both directions.
SELECT array_agg(a) FROM (SELECT a FROM lpd ORDER BY a <~> 200 LIMIT 4) x;
SELECT array_agg(a) FROM (SELECT a FROM lpd WHERE a BETWEEN 1 AND 400
  ORDER BY a <~> 200 LIMIT 2) x;
RESET enable_seqscan;
RESET enable_bitmapscan;
-- amcheck accepts the marks, and heapallindexed does not look for the dead
-- rows' entries.
SELECT bark_index_check('lpd_a', true);
SELECT bark_index_parent_check('lpd_a', true);
-- Fill the leaf until it would split: the first insert that does not fit
-- deletes the marked entries instead, and the leaf stays the only one (the
-- index is the meta page and the leaf).  A temp index writes no WAL; the
-- Bark/DELETE record is tested in t/014_lpdead_standby.pl.
INSERT INTO lpd SELECT g, g FROM generate_series(401, 420) g;
SELECT * FROM lpd_page('lpd_a', 1);
SELECT pg_relation_size('lpd_a') / 8192 AS pages;
SELECT * FROM lpd_count('SELECT * FROM lpd WHERE a > 0');
SELECT bark_index_check('lpd_a', true);
-- A new row whose key was a marked entry's goes in as a live entry; the
-- unique check finds no conflict with the dead row.
DELETE FROM lpd WHERE a = 50;
SELECT * FROM lpd_count('SELECT * FROM lpd WHERE a = 50');
SELECT count(*) AS dead FROM lpd_dead('lpd_a', 1);
INSERT INTO lpd VALUES (50, -50);
SELECT b FROM lpd WHERE a = 50;
SELECT bark_index_check('lpd_a', true);
DROP TABLE lpd;

-- The unique check marks the entries of dead rows it meets: a row deleted
-- and then inserted again with its key leaves the old row's entry marked.
CREATE TEMP TABLE lpdu (a int);
CREATE UNIQUE INDEX lpdu_a ON lpdu USING bark (a);
INSERT INTO lpdu SELECT g FROM generate_series(1, 10) g;
DELETE FROM lpdu WHERE a = 5;
INSERT INTO lpdu VALUES (5);
SELECT * FROM lpd_dead('lpdu_a', 1);
SELECT bark_index_check('lpdu_a', true);
DROP TABLE lpdu;

-- LIST entries.  Key k has 50 rows; a LIST is marked only when every one of
-- its rows is dead.  Key 3 keeps one live row and key 5 none.
CREATE TEMP TABLE lpdl (k int, v int);
INSERT INTO lpdl SELECT g % 10, g FROM generate_series(1, 500) g;
CREATE INDEX lpdl_k ON lpdl USING bark (k);
SELECT shape, ntids FROM bark_page_items('lpdl_k', 1) WHERE itemoffset <= 2;
DELETE FROM lpdl WHERE k = 3 AND v <> 3;
DELETE FROM lpdl WHERE k = 5;
SELECT * FROM lpd_count('SELECT * FROM lpdl WHERE k IN (3, 5)');
SELECT * FROM lpd_dead('lpdl_k', 1);
-- Only key 5's LIST is marked; key 3's still returns its live row.
SELECT * FROM lpd_count('SELECT * FROM lpdl WHERE k = 3');
SELECT bark_index_check('lpdl_k', true);
-- A scan whose snapshot sees a row stops it from being killed: a row
-- deleted in this transaction is not dead to every snapshot.
BEGIN;
DELETE FROM lpdl WHERE k = 7;
SELECT * FROM lpd_count('SELECT * FROM lpdl WHERE k = 7');
ROLLBACK;
SELECT * FROM lpd_dead('lpdl_k', 1);
SELECT * FROM lpd_count('SELECT * FROM lpdl WHERE k = 7');
-- An insert of key 5 above the marked LIST's TIDs gets a SINGLE of its own:
-- the dead LIST is not made live again.
INSERT INTO lpdl VALUES (5, -5);
SELECT * FROM lpd_dead('lpdl_k', 1);
SELECT * FROM lpd_count('SELECT * FROM lpdl WHERE k = 5');
SELECT bark_index_check('lpdl_k', true);
DROP TABLE lpdl;

-- A TID inside a marked LIST's range: the insert deletes the marked entries
-- first (simple deletion) rather than add the TID to the dead LIST and make
-- it live again.  Key 2's rows are on heap pages 0 and 2; once they are
-- deleted, a new key 2 row goes on a page the scan pruned, below the LIST's
-- last TID.
CREATE TEMP TABLE lpdi (k int, pad text);
INSERT INTO lpdi SELECT 2, repeat('x', 1000) FROM generate_series(1, 7);
INSERT INTO lpdi SELECT 9, repeat('x', 1000) FROM generate_series(1, 7);
INSERT INTO lpdi SELECT 2, repeat('x', 1000) FROM generate_series(1, 7);
CREATE INDEX lpdi_k ON lpdi USING bark (k);
DELETE FROM lpdi WHERE k = 9;
VACUUM lpdi;
DELETE FROM lpdi WHERE k = 2;
SELECT * FROM lpd_count('SELECT * FROM lpdi WHERE k = 2');
SELECT * FROM lpd_dead('lpdi_k', 1);
SELECT tids[1] AS lo, tids[ntids] AS hi FROM bark_page_items('lpdi_k', 1) \gset
INSERT INTO lpdi VALUES (2, 'new');
SELECT ctid > :'lo'::tid AND ctid < :'hi'::tid AS inside_dead_list FROM lpdi;
SELECT itemoffset, shape, ntids FROM bark_page_items('lpdi_k', 1);
SELECT * FROM lpd_count('SELECT * FROM lpdi WHERE k = 2');
SELECT bark_index_check('lpdi_k', true);
DROP TABLE lpdi;

-- A kill whose entry moved right on the page before the scan left it: an
-- index-only scan keeps its pin, so the page may change under it; the
-- cursor reads the leaf, kills the dead rows, and an insert of a lower key
-- moves their entries one offset right before the scan marks them.
CREATE TEMP TABLE lpdr (a int);
CREATE UNIQUE INDEX lpdr_a ON lpdr USING bark (a);
INSERT INTO lpdr SELECT g * 10 FROM generate_series(1, 50) g;
DELETE FROM lpdr WHERE a BETWEEN 100 AND 200;
BEGIN;
SET LOCAL enable_seqscan = off;
SET LOCAL enable_bitmapscan = off;
DECLARE c CURSOR FOR SELECT a FROM lpdr WHERE a > 0;
FETCH 12 FROM c;
INSERT INTO lpdr VALUES (5);
FETCH ALL FROM c \g /dev/null
COMMIT;
SELECT * FROM lpd_dead('lpdr_a', 1);
SELECT bark_index_check('lpdr_a', true);
DROP TABLE lpdr;

-- Coded LIST and POSTING entries: the same rule.  Keys 1 to 3 have ten
-- rows each, consecutive on one heap page, so each is a coded LIST; key 4's
-- 2000 rows are POSTING entries.
CREATE TEMP TABLE lpdp (k int);
INSERT INTO lpdp SELECT k FROM generate_series(1, 10) n, generate_series(1, 3) k
  ORDER BY k, n;
INSERT INTO lpdp SELECT 4 FROM generate_series(1, 2000);
CREATE INDEX lpdp_k ON lpdp USING bark (k);
SELECT itemoffset, shape, ntids, shape = 'LIST' AND itemlen < 16 + 6 * ntids
         AS coded
  FROM bark_page_items('lpdp_k', 1) ORDER BY itemoffset;
-- Key 1 dies whole; key 2 keeps its first row, key 4 its last.
DELETE FROM lpdp WHERE k = 1;
DELETE FROM lpdp WHERE k = 2 AND ctid <> (SELECT min(ctid) FROM lpdp WHERE k = 2);
DELETE FROM lpdp WHERE k = 4 AND ctid <> (SELECT max(ctid) FROM lpdp WHERE k = 4);
SELECT * FROM lpd_count('SELECT * FROM lpdp WHERE k > 0');
SELECT * FROM lpd_dead('lpdp_k', 1);
SELECT * FROM lpd_count('SELECT * FROM lpdp WHERE k > 0');
-- The last rows of keys 2 and 4 die: their entries are marked now.
DELETE FROM lpdp WHERE k IN (2, 4);
SELECT * FROM lpd_count('SELECT * FROM lpdp WHERE k > 0');
SELECT * FROM lpd_dead('lpdp_k', 1);
SELECT bark_index_check('lpdp_k', true);
DROP TABLE lpdp;

-- OVERSIZED entries are marked like SINGLEs, and scans skip them without
-- reading their overflow chains; simple deletion leaves them for VACUUM,
-- which frees their chains.
CREATE FUNCTION lpd_big(s int) RETURNS text LANGUAGE sql IMMUTABLE AS
$$ SELECT string_agg(md5(s::text || g::text), '') FROM generate_series(1, 150) g $$;
CREATE TEMP TABLE lpdo (id int, k text);
INSERT INTO lpdo SELECT g, lpd_big(g) FROM generate_series(1, 10) g;
CREATE INDEX lpdo_k ON lpdo USING bark (k);
DELETE FROM lpdo WHERE id <= 5;
SELECT * FROM lpd_count('SELECT * FROM lpdo WHERE k > ''''');
SELECT shape, count(*) FROM lpd_dead('lpdo_k', 1) GROUP BY shape;
SELECT * FROM lpd_count('SELECT * FROM lpdo WHERE k > ''''');
-- A backward scan reads the marked entry at the leaf's low end, whose key
-- may end the scan there, and so its overflow chain, and skips it.
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*), sum(id) FROM (SELECT id FROM lpdo WHERE k > ''
  ORDER BY k DESC) x;
RESET enable_seqscan;
RESET enable_bitmapscan;
SELECT bark_index_check('lpdo_k', true);
-- An insert that would split the leaf finds only marked OVERSIZED entries,
-- which simple deletion does not offer, so it splits.
SELECT pg_relation_size('lpdo_k') / 8192 AS pages_before;
INSERT INTO lpdo SELECT g, 'small' || g FROM generate_series(1, 300) g;
SELECT pg_relation_size('lpdo_k') / 8192 > 2 AS split;
SELECT * FROM lpd_count('SELECT * FROM lpdo WHERE k > ''''');
VACUUM lpdo;
SELECT * FROM lpd_page('lpdo_k', 1);
SELECT bark_index_check('lpdo_k', true);
DROP TABLE lpdo;

-- A multikey index: a row has an entry per key, and an entry is marked when
-- the scan returns the row from it.  Row 5's entry of key 1005 is not: the
-- scan's seen set drops the row there, since it returned it at key 5.
CREATE FUNCTION lpd_ndead(idx regclass) RETURNS bigint LANGUAGE sql AS $$
  SELECT sum(dead_items) FROM bark_multi_page_stats(idx::text, 1, -1)
   WHERE type = 'leaf'
$$;
CREATE TEMP TABLE lpdm (id int, a int4[]);
INSERT INTO lpdm SELECT g, ARRAY[g, g + 1000] FROM generate_series(1, 200) g;
CREATE INDEX lpdm_a ON lpdm USING bark (a bark_int4_array_ops);
DELETE FROM lpdm WHERE id <= 20;
SELECT * FROM lpd_count('SELECT * FROM lpdm WHERE a && ARRAY[5, 1005, 30]');
SELECT lpd_ndead('lpdm_a');
SELECT * FROM lpd_count('SELECT * FROM lpdm WHERE a && ARRAY[5, 1005, 30]');
-- Simple deletion offers each TID of the leaf once, though a row has two
-- entries there, known deletable if either is marked.
SELECT * FROM lpd_count('SELECT * FROM lpdm WHERE a && ARRAY[1, 2, 3, 4, 6]');
SELECT lpd_ndead('lpdm_a');
INSERT INTO lpdm SELECT g, ARRAY[g, g + 1000] FROM generate_series(201, 600) g;
SELECT lpd_ndead('lpdm_a');
SELECT * FROM lpd_count('SELECT * FROM lpdm WHERE a && ARRAY[1, 1001, 300]');
DROP TABLE lpdm;

-- A BARK-implemented btree index marks its entries the same way, from a
-- plain index scan, which drops its pin on each leaf once read and so marks
-- only a page whose LSN is unchanged, and from an index-only scan, which
-- keeps the pin (the temp table's pages are not all-visible, so it visits
-- the heap).
SET default_btree_implementation = bark;
CREATE TEMP TABLE lpdb (a int, b int);
CREATE INDEX lpdb_a ON lpdb (a);
RESET default_btree_implementation;
INSERT INTO lpdb SELECT g, g FROM generate_series(1, 300) g;
DELETE FROM lpdb WHERE a % 3 = 0;
SELECT * FROM lpd_count('SELECT * FROM lpdb WHERE a > 0');
SELECT lpd_ndead('lpdb_a');
DELETE FROM lpdb WHERE a % 3 = 1;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF) SELECT count(a) FROM lpdb WHERE a > 0;
SELECT count(a) FROM lpdb WHERE a > 0;
RESET enable_seqscan;
RESET enable_bitmapscan;
SELECT lpd_ndead('lpdb_a');
SELECT bark_index_check('lpdb_a', true);
DROP TABLE lpdb;

-- A plain index scan that dropped its pin gives up its kills on a page that
-- changed since it read it: VACUUM might have removed the entries and the
-- heap reused their TIDs.  The cursor's first fetch reads the leaf and
-- kills the dead rows 1 to 10 on its way to row 11; the insert changes the
-- leaf before the scan leaves it.  A later scan marks the entries.
CREATE TEMP TABLE lpdc (a int);
CREATE INDEX lpdc_a ON lpdc USING bark (a);
INSERT INTO lpdc SELECT g FROM generate_series(1, 100) g;
DELETE FROM lpdc WHERE a <= 10;
BEGIN;
SET LOCAL enable_seqscan = off;
SET LOCAL enable_bitmapscan = off;
SET LOCAL enable_indexonlyscan = off;
DECLARE c CURSOR FOR SELECT a FROM lpdc WHERE a > 0;
FETCH 1 FROM c;
INSERT INTO lpdc VALUES (1000);
FETCH ALL FROM c \g /dev/null
COMMIT;
SELECT lpd_ndead('lpdc_a');
SELECT * FROM lpd_count('SELECT * FROM lpdc WHERE a > 0');
SELECT lpd_ndead('lpdc_a');
DROP TABLE lpdc;

-- An index-only scan keeps its pin, so the leaf may change between the
-- read and the marking (see lpdr above).  Each cursor below passes dead
-- rows, which kills them, and stops on a live row before leaving the leaf;
-- then the leaf changes.
-- (1) A plain index scan marks the entries first: they are left as they are.
CREATE TEMP TABLE lpdk (a int);
CREATE INDEX lpdk_a ON lpdk USING bark (a);
INSERT INTO lpdk SELECT g FROM generate_series(1, 100) g;
DELETE FROM lpdk WHERE a <= 10;
BEGIN;
SET LOCAL enable_seqscan = off;
SET LOCAL enable_bitmapscan = off;
DECLARE c CURSOR FOR SELECT a FROM lpdk WHERE a > 0;
FETCH 1 FROM c;
SELECT * FROM lpd_count('SELECT * FROM lpdk WHERE a > 0');
SELECT count(*) AS dead FROM lpd_dead('lpdk_a', 1);
FETCH ALL FROM c \g /dev/null
COMMIT;
SELECT count(*) AS dead FROM lpd_dead('lpdk_a', 1);
DROP TABLE lpdk;
-- (2) The leaf, the rightmost, splits: its left half now has a high key at
-- offset 1, where the first dead entry was, and the entries are one offset
-- right.
CREATE TEMP TABLE lpds (a int);
CREATE INDEX lpds_a ON lpds USING bark (a);
INSERT INTO lpds SELECT g FROM generate_series(1, 300) g;
DELETE FROM lpds WHERE a <= 10;
BEGIN;
SET LOCAL enable_seqscan = off;
SET LOCAL enable_bitmapscan = off;
DECLARE c CURSOR FOR SELECT a FROM lpds WHERE a > 0;
FETCH 1 FROM c;
INSERT INTO lpds SELECT g FROM generate_series(301, 500) g;
FETCH ALL FROM c \g /dev/null
COMMIT;
SELECT pg_relation_size('lpds_a') / 8192 > 2 AS split,
       (SELECT count(*) FROM lpd_dead('lpds_a', 1)) AS dead;
SELECT * FROM lpd_count('SELECT * FROM lpds WHERE a > 0');
SELECT bark_index_check('lpds_a', true);
DROP TABLE lpds;
-- (3) An insert that would split the leaf deletes the marked entries 1 to
-- 20 instead, and in passing the killed ones, 300 to 310, whose heap block
-- it visits: the entries the scan killed are gone, and the ones now at
-- their offsets are of later rows.  Nothing is marked.
CREATE TEMP TABLE lpdv (a int);
CREATE INDEX lpdv_a ON lpdv USING bark (a);
INSERT INTO lpdv SELECT g FROM generate_series(1, 400) g;
DELETE FROM lpdv WHERE a <= 20;
SELECT * FROM lpd_count('SELECT * FROM lpdv WHERE a <= 20');
DELETE FROM lpdv WHERE a BETWEEN 300 AND 310;
BEGIN;
SET LOCAL enable_seqscan = off;
SET LOCAL enable_bitmapscan = off;
DECLARE c CURSOR FOR SELECT a FROM lpdv WHERE a > 0;
FETCH 280 FROM c \g /dev/null
INSERT INTO lpdv SELECT g FROM generate_series(1001, 1020) g;
SELECT * FROM lpd_page('lpdv_a', 1);
FETCH ALL FROM c \g /dev/null
COMMIT;
SELECT * FROM lpd_page('lpdv_a', 1);
SELECT * FROM lpd_count('SELECT * FROM lpdv WHERE a > 0');
DROP TABLE lpdv;
-- (4) A backward scan kills the LIST at the leaf's high end first; an
-- insert of its key adds a TID to it, which the scan never saw, so the
-- LIST is not marked.
CREATE TEMP TABLE lpdg (a int);
CREATE INDEX lpdg_a ON lpdg USING bark (a);
INSERT INTO lpdg SELECT g FROM generate_series(1, 100) g;
INSERT INTO lpdg SELECT 999 FROM generate_series(1, 5);
DELETE FROM lpdg WHERE a = 999;
BEGIN;
SET LOCAL enable_seqscan = off;
SET LOCAL enable_bitmapscan = off;
DECLARE c CURSOR FOR SELECT a FROM lpdg WHERE a > 0 ORDER BY a DESC;
FETCH 1 FROM c;
INSERT INTO lpdg VALUES (999);
FETCH ALL FROM c \g /dev/null
COMMIT;
SELECT shape, ntids FROM bark_page_items('lpdg_a', 1) WHERE itemoffset > 100;
SELECT count(*) AS dead FROM lpd_dead('lpdg_a', 1);
SELECT * FROM lpd_count('SELECT * FROM lpdg WHERE a = 999');
SELECT bark_index_check('lpdg_a', true);
DROP TABLE lpdg;

-- regress/mvcc's case, with BARK implementing the btree index: index scans
-- over the rows of rolled-back subtransactions mark their entries, and each
-- round's inserts delete them before the leaf splits, so the index keeps
-- its size, as nbtree's does.
BEGIN;
SET LOCAL default_btree_implementation = bark;
SET LOCAL enable_seqscan = false;
SET LOCAL enable_indexonlyscan = false;
SET LOCAL enable_bitmapscan = false;
CREATE TABLE clean_aborted_self(key int, data text);
CREATE INDEX clean_aborted_self_key ON clean_aborted_self(key);
INSERT INTO clean_aborted_self (key, data) VALUES (-1, 'just to allocate metapage');
SELECT pg_relation_size('clean_aborted_self_key') AS clean_aborted_self_key_before \gset
DO $$
BEGIN
    FOR i IN 1..100 LOOP
        BEGIN
            IF EXISTS(SELECT * FROM clean_aborted_self WHERE key > 0 AND key < 100) THEN
                RAISE data_corrupted USING MESSAGE = 'these rows should not exist';
            END IF;
            INSERT INTO clean_aborted_self SELECT g.i, 'rolling back in a sec' FROM generate_series(1, 100) g(i);
            RAISE reading_sql_data_not_permitted USING MESSAGE = 'round and round again';
        EXCEPTION WHEN reading_sql_data_not_permitted THEN END;
    END LOOP;
END;$$;
SELECT implam('clean_aborted_self_key'::regclass) AS impl,
       :clean_aborted_self_key_before AS size_before,
       pg_relation_size('clean_aborted_self_key') AS size_after;
ROLLBACK;

DROP FUNCTION lpd_page(regclass, int8);
DROP FUNCTION lpd_dead(regclass, int8);
DROP FUNCTION lpd_count(text);
DROP FUNCTION lpd_big(int);
DROP FUNCTION lpd_ndead(regclass);
DROP FUNCTION implam(regclass);
