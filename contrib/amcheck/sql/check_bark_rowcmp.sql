--
-- BARK row comparisons that position and stop a scan.
--
-- These cases check the index with bark_index_check() after the changes
-- they make, so they live here rather than in the core regression suite.
-- (The amcheck extension is created by the earlier "check" test.)
--

-- Row comparisons position and stop the scan.  A row comparison on column c
-- bounds the descent with its leading members that sit on columns c, c+1, ...
-- in the same direction (its usable prefix), and one on column 1 ends the scan
-- once an entry sorts past that prefix.  Every case matches a sequential scan
-- on each index shape; bark_rowcmp_check also names the scan direction.
CREATE TABLE bark_rowcmp (a int, b int, c text);
INSERT INTO bark_rowcmp SELECT g % 5000, (g / 5000) * 2, chr(97 + g % 26)
  FROM generate_series(1, 50000) g;
INSERT INTO bark_rowcmp VALUES (NULL, 1, 'n'), (NULL, NULL, NULL),
  (2500, NULL, 'n'), (7, NULL, 'z'), (7, 5, NULL);
VACUUM ANALYZE bark_rowcmp;
CREATE FUNCTION bark_rowcmp_check(q text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  plan json;
  node json;
  r1 text;
  r2 text;
BEGIN
  SET LOCAL enable_seqscan = off;
  SET LOCAL enable_bitmapscan = off;
  SET LOCAL enable_sort = off;
  EXECUTE 'EXPLAIN (COSTS OFF, FORMAT JSON) ' || q INTO plan;
  node := plan->0->'Plan';
  WHILE node->>'Index Name' IS NULL AND node->'Plans' IS NOT NULL LOOP
    node := node->'Plans'->0;
  END LOOP;
  EXECUTE 'SELECT md5(string_agg(x::text, '','')) FROM (' || q || ') x' INTO r1;
  RESET enable_seqscan;
  RESET enable_sort;
  SET LOCAL enable_indexscan = off;
  SET LOCAL enable_indexonlyscan = off;
  EXECUTE 'SELECT md5(string_agg(x::text, '','')) FROM (' || q || ') x' INTO r2;
  RETURN format('%s %s', coalesce(node->>'Scan Direction', 'no index scan'),
    CASE WHEN r1 IS NOT DISTINCT FROM r2 THEN 'ok' ELSE 'mismatch' END);
END $$;
CREATE FUNCTION bark_rowcmp_run(fwd text, bwd text) RETURNS SETOF text
LANGUAGE plpgsql AS $$
DECLARE
  q text;
BEGIN
  FOREACH q IN ARRAY ARRAY[
    'WHERE (a, b) > (2500, 10) ORDER BY ' || fwd,
    'WHERE (a, b) > (2500, 10) ORDER BY ' || bwd,
    'WHERE (a, b) >= (2500, 10) ORDER BY ' || fwd,
    'WHERE (a, b) >= (2500, 10) ORDER BY ' || bwd,
    'WHERE (a, b) < (2500, 10) ORDER BY ' || fwd,
    'WHERE (a, b) < (2500, 10) ORDER BY ' || bwd,
    'WHERE (a, b) <= (2500, 10) ORDER BY ' || fwd,
    'WHERE (a, b) <= (2500, 10) ORDER BY ' || bwd,
    'WHERE (a, b, c) > (2500, 10, ''k'') ORDER BY ' || fwd,
    'WHERE (a, b, c) <= (2500, 10, ''k'') ORDER BY ' || bwd,
    'WHERE (a, b) > (2500, NULL) ORDER BY ' || fwd,
    'WHERE (a, b) < (2500, NULL) ORDER BY ' || bwd,
    'WHERE (a, b) > (NULL, 1) ORDER BY ' || fwd,
    'WHERE (a, b) > (2500::int8, 10::int2) ORDER BY ' || fwd,
    'WHERE (a, b) <= (2500::int8, 10::int2) ORDER BY ' || bwd,
    'WHERE a = 7 AND (b, c) > (5, ''x'') ORDER BY ' || fwd,
    'WHERE a = 7 AND (b, c) >= (6, ''f'') ORDER BY ' || bwd,
    'WHERE (a, b) > (2500, 10) ORDER BY ' || fwd || ' LIMIT 20',
    'WHERE (a, b) < (2500, 10) ORDER BY ' || bwd || ' LIMIT 20']
  LOOP
    RETURN NEXT bark_rowcmp_check('SELECT a, b FROM bark_rowcmp ' || q) ||
      ': ' || q;
  END LOOP;
END $$;
CREATE INDEX bark_rowcmp_i ON bark_rowcmp USING bark (a, b);
SELECT bark_rowcmp_run('a, b', 'a DESC, b DESC');
DROP INDEX bark_rowcmp_i;
CREATE INDEX bark_rowcmp_i ON bark_rowcmp USING bark (a DESC, b);
SELECT bark_rowcmp_run('a DESC, b', 'a, b DESC');
DROP INDEX bark_rowcmp_i;
CREATE INDEX bark_rowcmp_i ON bark_rowcmp USING bark (a, b DESC);
SELECT bark_rowcmp_run('a, b DESC', 'a DESC, b');
DROP INDEX bark_rowcmp_i;
CREATE INDEX bark_rowcmp_i ON bark_rowcmp USING bark (a, b, c);
SELECT bark_rowcmp_run('a, b', 'a DESC, b DESC');
DROP INDEX bark_rowcmp_i;
-- Keyset pagination reads a few pages from where the row starts, and an
-- upper-bound row on column 1 stops the scan early: each reads under 10
-- index pages of a 50000-row index.
CREATE INDEX bark_rowcmp_i ON bark_rowcmp USING bark (a, b);
CREATE FUNCTION bark_rowcmp_pages(q text) RETURNS boolean LANGUAGE plpgsql AS $$
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
  -- The node's buffers include the heap pages an index-only scan fetches
  -- when the visibility map is not set, which depends on concurrent
  -- snapshots; at most one heap page per fetch, so count them out.
  RETURN (node->>'Shared Hit Blocks')::int + (node->>'Shared Read Blocks')::int -
    coalesce((node->>'Heap Fetches')::int, 0) < 10;
END $$;
SELECT bark_rowcmp_pages('SELECT a, b FROM bark_rowcmp WHERE (a, b) > (2500, 10) ORDER BY a, b LIMIT 20') AS keyset_pages_lt_10;
SELECT bark_rowcmp_pages('SELECT a, b FROM bark_rowcmp WHERE (a, b) < (5, 3) ORDER BY a, b') AS early_stop_pages_lt_10;
-- A scroll cursor moving back and forth over a row comparison returns the
-- same rows as a sort over a sequential scan.
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_sort = off;
BEGIN;
DECLARE c SCROLL CURSOR FOR
  SELECT a, b FROM bark_rowcmp WHERE (a, b) > (2500, 10) ORDER BY a, b;
FETCH 3 FROM c;
FETCH BACKWARD 5 FROM c;
FETCH 4 FROM c;
MOVE 25 IN c;
FETCH 2 FROM c;
FETCH BACKWARD 3 FROM c;
FETCH LAST FROM c;
FETCH BACKWARD 2 FROM c;
FETCH ABSOLUTE 2 FROM c;
COMMIT;
RESET enable_sort;
RESET enable_bitmapscan;
SET enable_indexscan = off;
SET enable_indexonlyscan = off;
SELECT a, b FROM bark_rowcmp WHERE (a, b) > (2500, 10) ORDER BY a, b LIMIT 32;
SELECT a, b FROM bark_rowcmp WHERE (a, b) > (2500, 10) ORDER BY a DESC, b DESC
  LIMIT 3;
RESET enable_indexonlyscan;
RESET enable_indexscan;
RESET enable_seqscan;
DROP FUNCTION bark_rowcmp_check(text), bark_rowcmp_run(text, text),
  bark_rowcmp_pages(text);
DROP TABLE bark_rowcmp;
-- Bottom-up deletion.  Every UPDATE of v is non-HOT (v is indexed) and leaves
-- k and u unchanged, so the executor inserts each new version into the k and
-- u indexes with indexUnchanged set.  When such an insert would split a
-- leaf, BARK first asks the heap which versions on the leaf are dead and
-- deletes their entries, as nbtree does, so eight rounds of UPDATEs leave
-- the k index about the size of a btree index on k instead of twice it.  The
-- u index (four rows per value, prefix-coded leaves) has LIST entries that
-- lose some members and are re-formed.  A temp table, so that only this
-- session's snapshot decides which versions are dead.
CREATE TEMP TABLE bark_churn (id int, k int, v int, u text)
  WITH (fillfactor = 100, autovacuum_enabled = off);
CREATE INDEX bark_churn_k ON bark_churn USING bark (k);
CREATE INDEX bark_churn_kbt ON bark_churn USING btree (k);
CREATE INDEX bark_churn_v ON bark_churn USING bark (v);
CREATE INDEX bark_churn_u ON bark_churn USING bark (u)
  WITH (prefix_compression = on);
CREATE INDEX bark_churn_ubt ON bark_churn USING btree (u);
INSERT INTO bark_churn
  SELECT g, g, 0, 'https://www.example.com/item/' || lpad((g % 2500)::text, 8, '0')
  FROM generate_series(1, 10000) g;
DO $$
BEGIN
  FOR i IN 1..8 LOOP
    UPDATE bark_churn SET v = v + 1;
    COMMIT;
  END LOOP;
END $$;
SELECT pg_relation_size('bark_churn_k') <=
         pg_relation_size('bark_churn_kbt') * 1.25 AS k_like_btree,
       pg_relation_size('bark_churn_u') <=
         pg_relation_size('bark_churn_ubt') * 1.25 AS u_like_btree;
SELECT bark_index_check('bark_churn_k'), bark_index_check('bark_churn_v'),
       bark_index_check('bark_churn_u');
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*), sum(k), sum(v) FROM bark_churn WHERE k > 0;
SELECT count(*), count(DISTINCT u) FROM bark_churn WHERE u > 'h';
RESET enable_seqscan;
RESET enable_bitmapscan;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT count(*), sum(k), sum(v) FROM bark_churn WHERE k > 0;
SELECT count(*), count(DISTINCT u) FROM bark_churn WHERE u > 'h';
RESET enable_indexscan;
RESET enable_bitmapscan;
DROP TABLE bark_churn;
