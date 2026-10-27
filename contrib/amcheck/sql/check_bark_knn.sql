--
-- Ordered-operator (KNN) scans of BARK indexes.
--
-- These cases check the index with bark_index_check() after the changes
-- they make, so they live here rather than in the core regression suite.
-- (The amcheck extension is created by the earlier "check" test.)
--

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
-- a few around the constant.  (ctid keeps this a plain index scan, whose
-- output does not depend on the visibility map.)
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
  SELECT a, ctid FROM bark_knn ORDER BY a <~> 5000 LIMIT 10;

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

-- A NULL constant makes every distance NULL, which sorts last and as equal,
-- so the scan returns every row, as a Sort does.  A generic plan keeps the
-- index scan with a parameter the executor sets to NULL.
SET plan_cache_mode = force_generic_plan;
PREPARE bark_knn_nullc(int) AS
  SELECT count(*) AS total, count(DISTINCT a) AS keys,
         count(*) FILTER (WHERE d IS NULL) AS nulldist
    FROM (SELECT a, a <~> $1 AS d FROM bark_knn_null ORDER BY a <~> $1) s;
EXPLAIN (COSTS OFF) EXECUTE bark_knn_nullc(NULL);
EXECUTE bark_knn_nullc(NULL);
PREPARE bark_knn_nullc_lim(int) AS
  SELECT count(*) FROM (SELECT a FROM bark_knn_null ORDER BY a <~> $1 LIMIT 5) s;
EXECUTE bark_knn_nullc_lim(NULL);
DEALLOCATE bark_knn_nullc;
DEALLOCATE bark_knn_nullc_lim;
RESET plan_cache_mode;

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
  warm refcursor;
  line text;
  n int := 0;
BEGIN
  SET LOCAL enable_seqscan = off;
  SET LOCAL enable_bitmapscan = off;
  SET LOCAL debug_parallel_query = off;	-- as in bark_xt_buffers
  OPEN warm FOR EXECUTE q;
  MOVE warm;
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
