--
-- BARK leaf splits: the inline prefix of OVERSIZED keys, fillfactor, split
-- points, and suffix truncation of pivots.
--
-- These cases check the index with bark_index_check() after the changes
-- they make, so they live here rather than in the core regression suite.
-- (The amcheck extension is created by the earlier "check" test.)
--

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
-- btree's other parameters are accepted, so that BARK can implement btree
-- indexes (bark_btree_impl in src/test/modules/bark tests what
-- deduplicate_items = off stores).
CREATE INDEX bark_ff_dd ON bark_ff USING bark (a)
  WITH (deduplicate_items = off, vacuum_cleanup_index_scale_factor = 0.5);
SELECT reloptions FROM pg_class WHERE relname = 'bark_ff_dd';
SELECT bark_index_check('bark_ff_dd', true);
DROP INDEX bark_ff_dd;
CREATE INDEX bark_ff_bad ON bark_ff USING bark (a) WITH (deduplicate_items = 2);
CREATE INDEX bark_ff_bad ON bark_ff USING bark (a) WITH (vacuum_cleanup_index_scale_factor = -1);
-- A low fillfactor with wide keys still puts at least two entries on each
-- page: 200 incompressible ~1kB keys at fillfactor 10 must take fewer than
-- 200 pages, internal levels and meta page included.
CREATE TABLE bark_ff_wide (t text);
INSERT INTO bark_ff_wide
  SELECT lpad(g::text, 3, '0') ||
         (SELECT string_agg(md5(g::text || i::text), '') FROM generate_series(1, 30) i)
  FROM generate_series(1, 200) g;
CREATE INDEX bark_ff_wide_idx ON bark_ff_wide USING bark (t) WITH (fillfactor = 10);
SELECT pg_relation_size('bark_ff_wide_idx') / current_setting('block_size')::int < 200 AS two_per_page;
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

-- Such a bound sorts after a pivot that has exactly its columns and no heap
-- TID, as in nbtree: every entry left of the pivot is less than it, so the
-- descent goes right of it instead of reading a leaf with no match.  200000
-- rows over 100 values of a, inserted in random order, so that many leaf
-- splits fall between two values and leave a high key of (a) alone.  Below
-- the upper levels, a = v reads the leaves that hold v, and one more when v
-- ends a leaf, since the scan stops at the first entry past v.  Each of the
-- 99 boundaries between values adds one of the two, so the 100 queries
-- together read every leaf, 99 more, and a descent each: no more than the
-- leaves plus 100 times the pages of a descent (root to leaf).  A descent
-- that went left of a (v) pivot would read one more leaf for each.  The
-- leaf count and the height come from two scans this does not affect:
-- a = -1 reads the upper levels and one leaf, a >= 0 every leaf.
CREATE FUNCTION bark_sfx_blocks(q text) RETURNS int LANGUAGE plpgsql AS $$
DECLARE
  warm refcursor;
  plan json;
  node json;
BEGIN
  PERFORM set_config('enable_seqscan', 'off', true);
  PERFORM set_config('enable_indexscan', 'off', true);
  PERFORM set_config('enable_indexonlyscan', 'off', true);
  PERFORM set_config('enable_bitmapscan', 'on', true);
  PERFORM set_config('max_parallel_workers_per_gather', '0', true);
  SET LOCAL debug_parallel_query = off;	-- as in bark_xt_buffers
  OPEN warm FOR EXECUTE q;
  MOVE warm;
  EXECUTE 'EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF, FORMAT JSON) ' || q
    INTO plan;
  node := plan->0->'Plan';
  WHILE node->>'Node Type' <> 'Bitmap Index Scan' LOOP
    node := node->'Plans'->0;
  END LOOP;
  RETURN (node->>'Shared Hit Blocks')::int + (node->>'Shared Read Blocks')::int;
END $$;
CREATE TABLE bark_sfx_tp (a int, b int) WITH (autovacuum_enabled = off);
CREATE INDEX bark_sfx_tp_idx ON bark_sfx_tp USING bark (a, b);
INSERT INTO bark_sfx_tp
  SELECT g % 100, g FROM generate_series(1, 200000) g ORDER BY md5(g::text);
SELECT bark_index_check('bark_sfx_tp_idx');
SELECT bark_sfx_check('SELECT s.a, count(t.a) FROM generate_series(-1, 100) s(a)
  LEFT JOIN bark_sfx_tp t ON t.a = s.a GROUP BY s.a ORDER BY s.a');
SELECT bark_sfx_check('SELECT s.a, count(t.a) FROM generate_series(-1, 100) s(a)
  LEFT JOIN bark_sfx_tp t ON t.a = s.a GROUP BY s.a ORDER BY s.a', true);
SELECT sum(bark_sfx_blocks('SELECT count(*) FROM bark_sfx_tp WHERE a = ' || v)) <=
       bark_sfx_blocks('SELECT count(*) FROM bark_sfx_tp WHERE a >= 0') -
       bark_sfx_blocks('SELECT count(*) FROM bark_sfx_tp WHERE a = -1') + 1 +
       100 * bark_sfx_blocks('SELECT count(*) FROM bark_sfx_tp WHERE a = -1')
         AS no_leaf_left_of_pivot
  FROM generate_series(0, 99) v;
DROP TABLE bark_sfx_tp;
DROP FUNCTION bark_sfx_blocks(text);

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
