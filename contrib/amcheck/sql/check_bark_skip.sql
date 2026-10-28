--
-- BARK skip scan.
--
-- These cases check the index with bark_index_check() after the changes
-- they make, so they live here rather than in the core regression suite.
-- (The amcheck extension is created by the earlier "check" test.)
--

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
  RETURN format('searches>=10 %s, blocks<pages/2 %s',
    (node->>'Index Searches')::int >= 10,
    (node->>'Shared Hit Blocks')::int + (node->>'Shared Read Blocks')::int <
      pg_relation_size(idx::regclass) / current_setting('block_size')::int / 2);
END $$;
CREATE FUNCTION bark_skip_searches(q text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  plan json;
  node json;
BEGIN
  SET LOCAL debug_parallel_query = off;	-- as in bark_xt_buffers
  EXECUTE 'EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, FORMAT JSON) ' || q INTO plan;
  node := plan->0->'Plan';
  WHILE node->>'Index Name' IS NULL LOOP
    node := node->'Plans'->0;
  END LOOP;
  RETURN format('%s: %s searches', node->>'Index Name', node->>'Index Searches');
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
SELECT bark_skip_searches('SELECT a, b FROM bark_skip WHERE b BETWEEN 5000 AND 5100');
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
SELECT bark_index_check('bark_skip_ab'), bark_index_check('bark_skip_tb');
-- Groups shorter than a page and about a page long: the next possible
-- match after a failing entry is often later on the same page (found by
-- binary search) or past the high key (the scan re-descends at once).
-- Both agree with a sequential scan, with and without skip support
-- (int4 and text column 1).
CREATE TABLE bark_skip2 (a int, b int, t text);
INSERT INTO bark_skip2 SELECT g / 7, g % 7, 'k' || (g / 7)
  FROM generate_series(1, 20000) g;
INSERT INTO bark_skip2 SELECT 10000 + g / 400, g % 400, 'k' || (10000 + g / 400)
  FROM generate_series(1, 40000) g;
CREATE INDEX bark_skip2_ab ON bark_skip2 USING bark (a, b);
CREATE INDEX bark_skip2_tb ON bark_skip2 USING bark (t DESC, b);
VACUUM ANALYZE bark_skip2;
SELECT bark_skip_check('SELECT a, b FROM bark_skip2 WHERE b = 3 ORDER BY a, b');
SELECT bark_skip_check('SELECT a, b FROM bark_skip2 WHERE b BETWEEN 5 AND 6 ORDER BY a, b');
SELECT bark_skip_check('SELECT a, b FROM bark_skip2 WHERE b > 395 ORDER BY a, b');
SELECT bark_skip_check('SELECT a, b FROM bark_skip2 WHERE b < 2 ORDER BY a, b');
SELECT bark_skip_check('SELECT t, b FROM bark_skip2 WHERE b = 3 ORDER BY t DESC, b');
SELECT bark_skip_check('SELECT t, b FROM bark_skip2 WHERE b > 395 ORDER BY t DESC, b');
SELECT bark_index_check('bark_skip2_ab'), bark_index_check('bark_skip2_tb');
DROP TABLE bark_skip2;
DROP FUNCTION bark_skip_check(text), bark_skip_stats(text, text),
  bark_skip_searches(text);
DROP TABLE bark_skip;
