--
-- Parallel VACUUM of BARK indexes, mark and restore under merge joins, and
-- required keys and arrays.
--
-- These cases check the index with bark_index_check() after the changes
-- they make, so they live here rather than in the core regression suite.
-- (The amcheck extension is created by the earlier "check" test.)
--

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

-- Required keys: a bound on a later column ends the scan while every earlier
-- column has an equality key, as nbtree's _bt_check_compare does.  The
-- results match a sequential scan on ASC and DESC/NULLS FIRST indexes, in
-- both directions, and a = AND b = point lookup reads a few pages instead of
-- the rest of the a group.
CREATE TABLE bark_req (a int, b int, c int, v int);
INSERT INTO bark_req SELECT g % 10, (g / 10) % 1000, g % 7, g
  FROM generate_series(1, 100000) g;
INSERT INTO bark_req VALUES (3, NULL, 1, -1), (3, NULL, NULL, -2),
  (NULL, 5, 1, -3), (3, 5, NULL, -4);
CREATE INDEX bark_req_abc ON bark_req USING bark (a, b, c);
VACUUM ANALYZE bark_req;
CREATE FUNCTION bark_req_check(q text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  r1 text;
  r2 text;
BEGIN
  SET LOCAL enable_seqscan = off;
  SET LOCAL enable_bitmapscan = off;
  SET LOCAL enable_sort = off;
  -- Rows can tie on (a, b, c): compare those in output order, and the rows
  -- as a set.
  q := 'SELECT md5(string_agg((a, b, c)::text, '','')) || ' ||
    'md5(string_agg(x::text, '','' ORDER BY x::text)) FROM (' || q || ') x';
  EXECUTE q INTO r1;
  RESET enable_seqscan;
  RESET enable_sort;
  SET LOCAL enable_indexscan = off;
  SET LOCAL enable_indexonlyscan = off;
  EXECUTE q INTO r2;
  RETURN CASE WHEN r1 IS NOT DISTINCT FROM r2 THEN 'ok' ELSE 'mismatch: ' || q END;
END $$;
CREATE FUNCTION bark_req_run() RETURNS SETOF text LANGUAGE plpgsql AS $$
DECLARE
  w text;
  o text;
BEGIN
  FOREACH w IN ARRAY ARRAY[
    'a = 3 AND b = 517', 'a = 3 AND b = 518', 'a = 3 AND b = 517 AND c = 2',
    'a = 3 AND b = 517 AND c = 3', 'a = 3 AND b < 5', 'a = 3 AND b <= 5',
    'a = 3 AND b > 995', 'a = 3 AND b >= 995', 'a = 3 AND b BETWEEN 10 AND 12',
    'a = 3 AND b = 5 AND c < 3', 'a = 3 AND b = 5 AND c > 3',
    'a = 3 AND b IS NULL', 'a = 3 AND b = 5 AND c IS NULL',
    'a = 3 AND b = ANY (ARRAY[5, 7, 900])', 'a = ANY (ARRAY[2, 3]) AND b = 5',
    'a IS NULL AND b = 5', 'a = 3 AND c = 2 AND b < 3', 'a = 3 AND b = 5::int8',
    'a = 3 AND b < 3000000000::int8', 'a >= 3 AND a <= 4 AND b = 5',
    'a = 9 AND b > 990 AND c = 6']
  LOOP
    FOREACH o IN ARRAY ARRAY['a, b, c', 'a DESC, b DESC, c DESC'] LOOP
      RETURN NEXT bark_req_check(
        'SELECT a, b, c, v FROM bark_req WHERE ' || w || ' ORDER BY ' || o);
    END LOOP;
  END LOOP;
END $$;
SELECT r, count(*) FROM bark_req_run() r GROUP BY r;
DROP INDEX bark_req_abc;
CREATE INDEX bark_req_abc ON bark_req USING bark (a, b DESC NULLS FIRST, c);
SELECT r, count(*) FROM bark_req_run() r GROUP BY r;
DROP INDEX bark_req_abc;
CREATE INDEX bark_req_abc ON bark_req USING bark (a, b, c);
-- The index scan of a point lookup on two columns reads a few pages: the
-- scan ends after (3, 517) instead of reading the rest of a = 3 (about 75
-- pages).  Heap fetches are counted out, as in bark_rowcmp_pages.
CREATE FUNCTION bark_req_pages(q text) RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
  warm refcursor;
  plan json;
  node json;
BEGIN
  SET LOCAL enable_seqscan = off;
  SET LOCAL enable_bitmapscan = off;
  SET LOCAL debug_parallel_query = off;	-- as in bark_xt_buffers
  OPEN warm FOR EXECUTE q;
  MOVE warm;
  EXECUTE 'EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF, FORMAT JSON) ' || q
    INTO plan;
  node := plan->0->'Plan';
  WHILE node->>'Index Name' IS NULL LOOP
    node := node->'Plans'->0;
  END LOOP;
  RETURN (node->>'Shared Hit Blocks')::int + (node->>'Shared Read Blocks')::int -
    coalesce((node->>'Heap Fetches')::int, 0) < 10;
END $$;
SELECT bark_req_pages('SELECT a, b, c FROM bark_req WHERE a = 3 AND b = 517') AS point_pages_lt_10,
  bark_req_pages('SELECT a, b, c FROM bark_req WHERE a = 3 AND b = 517 ORDER BY a DESC, b DESC') AS point_backward_pages_lt_10,
  bark_req_pages('SELECT a, b, c FROM bark_req WHERE a = 3 AND b < 5') AS range_pages_lt_10,
  bark_req_pages('SELECT a, b, c FROM bark_req WHERE a = 3 AND b > 995 ORDER BY a DESC, b DESC') AS range_backward_pages_lt_10;

-- Required array keys: the equality keys of the leading columns, scalars and
-- arrays alike, position the scan when one of them is an array, as in
-- nbtree.  The scan visits the combinations of their elements in index order
-- in both directions, ends a combination's run at a bound on the next
-- column, and re-descends to the next combination when it starts beyond the
-- page.  Arrays after the first column without an equality only filter.
-- Each query is checked against a sequential scan, as a plain and an
-- index-only scan, forward and backward, on an ASC index and on one with
-- DESC and NULLS FIRST columns.
CREATE FUNCTION bark_arr_run(fwd text, bwd text) RETURNS SETOF text
LANGUAGE plpgsql AS $$
DECLARE
  w text;
  o text;
  t text;
BEGIN
  FOREACH w IN ARRAY ARRAY[
    'a IN (7, 3, 2) AND b = 517', 'a = 3 AND b IN (900, 5, 517, 5)',
    'a IN (3, 9) AND b IN (5, 517, 999)',
    'a IN (3, 9) AND b IN (5, 517) AND c IN (1, 6)',
    'a IN (3, 9) AND b = 5 AND c IN (1, 6)',
    'a = 3 AND b IN (5, 517) AND c = 2', 'a IN (3, 9) AND b < 3',
    'a IN (3, 9) AND b > 997', 'a IN (3, 9) AND b BETWEEN 500 AND 502',
    'a IN (3, 9) AND b IN (5, 517) AND c < 3',
    'a IN (3, 9) AND b IN (5, 517) AND c >= 4',
    'a = 3 AND b IN (5, 517) AND c > 2 AND c < 6',
    'a IN (3, NULL) AND b = ANY (ARRAY[5, NULL, 517])',
    'a IN (3, 9) AND b = ANY (''{}''::int[])',
    'a = ANY (''{3, 4294967299}''::int8[]) AND b = ANY (''{5, 517, -4294967291}''::int8[])',
    'a IN (3, 9) AND b = 517::int8', 'a IS NULL AND b IN (5, 517)',
    'a IN (3, 9) AND b IS NULL', 'a IN (3, 9) AND b IN (5, 517) AND c IS NULL',
    'a IN (2, 3, 4) AND a IN (3, 4, 8) AND b IN (5, 517)',
    'a = 3 AND a IN (3, 4) AND b IN (5, 517)', 'a IN (3, 9) AND a >= 4 AND b = 5',
    'b IN (5, 517) AND c = 2', 'a IN (3, 9) AND c IN (1, 6)',
    'a IN (0, 9) AND b IN (0, 999)', 'a IN (-1, 3, 10) AND b IN (-5, 517, 2000)',
    'a IN (3, 9) AND b IN (5, 517) AND b > 6']
  LOOP
    FOREACH o IN ARRAY ARRAY[fwd, bwd] LOOP
      FOREACH t IN ARRAY ARRAY['a, b, c, v', 'a, b, c'] LOOP
        RETURN NEXT bark_req_check(
          'SELECT ' || t || ' FROM bark_req WHERE ' || w || ' ORDER BY ' || o);
      END LOOP;
    END LOOP;
  END LOOP;
END $$;
-- A scroll cursor moving both ways across combinations and pages returns
-- what a sequential scan's does, through a plain or an index-only scan
-- (ios).  q returns a, b and c, which can tie, so rows are compared on them.
CREATE FUNCTION bark_arr_scroll(q text, ios bool) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  c refcursor := 'bark_arr_c';
  cmds text[] := ARRAY['NEXT', 'NEXT', 'FORWARD 12', 'PRIOR', 'BACKWARD 7',
    'MOVE FORWARD 30', 'NEXT', 'PRIOR', 'PRIOR', 'MOVE BACKWARD 20', 'PRIOR',
    'LAST', 'PRIOR', 'BACKWARD 15', 'FIRST', 'MOVE FORWARD 60', 'NEXT',
    'MOVE FORWARD ALL', 'PRIOR', 'BACKWARD 12', 'ABSOLUTE 45', 'RELATIVE -10',
    'NEXT', 'FORWARD 22', 'PRIOR', 'MOVE BACKWARD ALL', 'NEXT'];
  cmd text;
  how text;
  va int;
  vb int;
  vc int;
  res text[] := '{}';
  out text;
BEGIN
  FOREACH how IN ARRAY ARRAY['index', 'seq'] LOOP
    PERFORM set_config('enable_seqscan', (how = 'seq')::text, true);
    PERFORM set_config('enable_indexscan', (how = 'index')::text, true);
    PERFORM set_config('enable_indexonlyscan', (how = 'index' AND ios)::text,
                       true);
    PERFORM set_config('enable_bitmapscan', 'off', true);
    PERFORM set_config('enable_sort', (how = 'seq')::text, true);
    OPEN c SCROLL FOR EXECUTE q;
    out := '';
    FOREACH cmd IN ARRAY cmds LOOP
      IF cmd LIKE 'MOVE%' THEN
        EXECUTE format('%s FROM bark_arr_c', cmd);
      ELSE
        EXECUTE format('FETCH %s FROM bark_arr_c', cmd) INTO va, vb, vc;
        out := out || format('(%s,%s,%s)', va, vb, vc);
      END IF;
    END LOOP;
    CLOSE c;
    res := res || md5(out);
  END LOOP;
  RETURN CASE WHEN res[1] = res[2] THEN 'ok' ELSE 'mismatch: ' || q END;
END $$;
-- Index blocks read by q's index scan, heap fetches counted out, are at
-- most lim.
CREATE FUNCTION bark_arr_pages(q text, lim int) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  warm refcursor;
  plan json;
  node json;
  n int;
BEGIN
  SET LOCAL enable_seqscan = off;
  SET LOCAL enable_bitmapscan = off;
  SET LOCAL debug_parallel_query = off;	-- as in bark_xt_buffers
  OPEN warm FOR EXECUTE q;
  MOVE warm;
  EXECUTE 'EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF, FORMAT JSON) ' || q
    INTO plan;
  node := plan->0->'Plan';
  WHILE node->>'Index Name' IS NULL LOOP
    node := node->'Plans'->0;
  END LOOP;
  n := (node->>'Shared Hit Blocks')::int + (node->>'Shared Read Blocks')::int -
    coalesce((node->>'Heap Fetches')::int, 0);
  RETURN CASE WHEN n <= lim THEN 'ok'
    ELSE format('%s blocks, %s searches: %s', n, node->>'Index Searches', q) END;
END $$;
BEGIN;
SELECT r, count(*) FROM bark_arr_run('a, b, c', 'a DESC, b DESC, c DESC') r
  GROUP BY r;
SELECT bark_arr_scroll('SELECT a, b, c FROM bark_req WHERE a IN (2, 5, 8) AND b IN (5, 517, 999) ORDER BY a, b, c', false) AS scroll_fwd,
  bark_arr_scroll('SELECT a, b, c FROM bark_req WHERE a IN (2, 5, 8) AND b IN (5, 517, 999) ORDER BY a DESC, b DESC, c DESC', true) AS scroll_bwd_ios,
  bark_arr_scroll('SELECT a, b, c FROM bark_req WHERE a = 5 AND b IN (5, 517, 999) AND c < 5 ORDER BY a, b, c', false) AS scroll_range;
COMMIT;
-- A merge join restores marks on its inner scan; the marked position
-- carries the required keys' cursors.  Joined on a alone, an inner run spans
-- several combinations and re-descents.
CREATE TABLE bark_arr_o (a int, b int);
INSERT INTO bark_arr_o SELECT a, b FROM generate_series(1, 9) a,
  unnest(ARRAY[5, 6, 517, 999]) b, generate_series(1, 3);
ANALYZE bark_arr_o;
SELECT bark_mj_check('SELECT count(*), sum(i.v) FROM
  (SELECT a, b FROM bark_arr_o ORDER BY a, b OFFSET 0) o
  JOIN bark_req i ON o.a = i.a AND o.b = i.b
  WHERE i.a IN (2, 5, 8) AND i.b IN (5, 517, 999)', 'bark_req_abc') AS mj_ab;
SELECT bark_mj_check('SELECT count(*), sum(i.v) FROM
  (SELECT a FROM bark_arr_o ORDER BY a OFFSET 0) o
  JOIN bark_req i ON o.a = i.a
  WHERE i.a IN (2, 5, 8) AND i.b IN (5, 517, 999)', 'bark_req_abc') AS mj_a;
SELECT bark_mj_check('SELECT count(*), sum(i.c) FROM
  (SELECT a FROM bark_arr_o ORDER BY a OFFSET 0) o
  JOIN bark_req i ON o.a = i.a
  WHERE i.a IN (2, 5, 8) AND i.b IN (5, 517, 999)', 'bark_req_abc') AS mj_a_ios;
-- A parallel scan does not re-descend: workers read the pages the shared
-- cursor hands out, and the required keys only end page reads and the scan.
BEGIN;
SET LOCAL min_parallel_table_scan_size = 0;
SET LOCAL min_parallel_index_scan_size = 0;
SET LOCAL parallel_setup_cost = 0;
SET LOCAL parallel_tuple_cost = 0;
SET LOCAL max_parallel_workers_per_gather = 2;
SET LOCAL enable_seqscan = off;
SET LOCAL enable_bitmapscan = off;
EXPLAIN (COSTS OFF) SELECT count(*), sum(v) FROM bark_req
  WHERE a IN (2, 5, 8) AND b < 500;
SELECT count(*), sum(v) FROM bark_req WHERE a IN (2, 5, 8) AND b < 500;
SELECT count(*), sum(v) FROM bark_req
  WHERE a IN (2, 5, 8) AND b > 100 AND c IN (1, 2);
SET LOCAL max_parallel_workers_per_gather = 0;
SET LOCAL enable_seqscan = on;
SET LOCAL enable_indexscan = off;
SET LOCAL enable_indexonlyscan = off;
SELECT count(*), sum(v) FROM bark_req WHERE a IN (2, 5, 8) AND b < 500;
SELECT count(*), sum(v) FROM bark_req
  WHERE a IN (2, 5, 8) AND b > 100 AND c IN (1, 2);
COMMIT;
-- Arrays on the first, the second and both columns read a few pages per
-- combination (one descent each, about 3 pages), not the rest of each a
-- group (about 35 pages); so does a range on the column after the arrays.
SELECT q, bark_arr_pages('SELECT a, b, c FROM bark_req WHERE ' || q, lim)
FROM (VALUES ('a IN (2, 5, 8) AND b = 517', 12),
  ('a IN (2, 5, 8) AND b = 517 ORDER BY a DESC, b DESC', 12),
  ('a = 5 AND b IN (5, 517, 999)', 12),
  ('a = 5 AND b IN (5, 517, 999) ORDER BY a DESC, b DESC', 12),
  ('a IN (2, 8) AND b IN (5, 999)', 16),
  ('a IN (2, 8) AND b IN (5, 999) ORDER BY a DESC, b DESC', 16),
  ('a IN (2, 8) AND b < 3', 9),
  ('a IN (2, 8) AND b > 997 ORDER BY a DESC, b DESC', 9),
  ('a IN (2, 3) AND b IN (5, 6, 7, 8)', 9)) v(q, lim);
DROP INDEX bark_req_abc;
CREATE INDEX bark_req_abc ON bark_req USING bark (a DESC, b, c DESC NULLS FIRST);
BEGIN;
SELECT r, count(*) FROM bark_arr_run('a DESC, b, c DESC', 'a, b DESC, c') r
  GROUP BY r;
SELECT bark_arr_scroll('SELECT a, b, c FROM bark_req WHERE a IN (2, 5, 8) AND b IN (5, 517, 999) ORDER BY a DESC, b, c DESC', false) AS scroll_fwd,
  bark_arr_scroll('SELECT a, b, c FROM bark_req WHERE a IN (2, 5, 8) AND b IN (5, 517, 999) ORDER BY a, b DESC, c', true) AS scroll_bwd_ios;
COMMIT;
SELECT q, bark_arr_pages('SELECT a, b, c FROM bark_req WHERE ' || q, lim)
FROM (VALUES ('a IN (2, 5, 8) AND b = 517 ORDER BY a DESC', 12),
  ('a IN (2, 5, 8) AND b = 517 ORDER BY a', 12),
  ('a = 5 AND b IN (5, 517, 999) ORDER BY a DESC, b', 12),
  ('a = 5 AND b IN (5, 517, 999) ORDER BY a, b DESC', 12)) v(q, lim);
SELECT bark_index_check('bark_req_abc');
DROP TABLE bark_arr_o;
DROP FUNCTION bark_arr_run(text, text), bark_arr_scroll(text, bool),
  bark_arr_pages(text, int), bark_mj_check(text, text);
DROP FUNCTION bark_req_check(text), bark_req_run(), bark_req_pages(text);
DROP TABLE bark_req;
