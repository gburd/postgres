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
-- A KNN scan skips them as well; it marks none itself.
SELECT array_agg(a) FROM (SELECT a FROM lpd ORDER BY a <~> 200 LIMIT 4) x;
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
SELECT bark_index_check('lpdo_k', true);
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
