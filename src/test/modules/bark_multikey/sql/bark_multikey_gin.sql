-- BARK multikey scans through GIN's query interface (procedures 10-13).
-- Every query compares a forced bitmap scan of the BARK index with a
-- sequential scan and with a GIN index on the same table.
CREATE EXTENSION IF NOT EXISTS bark_multikey;
SELECT setseed(0.42);
CREATE TABLE mg (id int PRIMARY KEY, a int4[]);
INSERT INTO mg SELECT i,
  CASE WHEN i % 50 = 0 THEN NULL
       WHEN i % 47 = 0 THEN '{}'
       WHEN i % 43 = 0 THEN ARRAY[NULL, (i % 20)]::int4[]
       ELSE ARRAY(SELECT (random() * 30)::int4
                  FROM generate_series(1, 1 + i % 5)) END
FROM generate_series(1, 3000) i;
CREATE INDEX mg_bark ON mg USING bark (a bark_gin_int4_array_ops);
CREATE INDEX mg_bark11 ON mg USING bark (a bark_gin11_int4_array_ops);
CREATE INDEX mg_gin ON mg USING gin (a);
SELECT bark_index_check('mg_bark'), bark_index_check('mg_bark11');

-- Rows of a query through one index (NULL: seqscan), as a sorted id array.
CREATE FUNCTION mg_run(idx text, q text) RETURNS int[] LANGUAGE plpgsql AS $$
DECLARE r int[];
BEGIN
  PERFORM set_config('enable_seqscan', (idx IS NULL)::text, true);
  PERFORM set_config('enable_bitmapscan', (idx IS NOT NULL)::text, true);
  PERFORM set_config('enable_indexscan', 'off', true);
  IF idx IS NOT NULL THEN
    EXECUTE format('UPDATE pg_index SET indisvalid = (indexrelid = %L::regclass) WHERE indrelid = ''mg''::regclass AND indexrelid <> ''mg_pkey''::regclass', idx);
  END IF;
  EXECUTE 'SELECT coalesce(array_agg(id ORDER BY id), ''{}'') FROM mg WHERE ' || q INTO r;
  UPDATE pg_index SET indisvalid = true WHERE indrelid = 'mg'::regclass;
  RETURN r;
END $$;
CREATE TABLE mg_q (q text);
INSERT INTO mg_q SELECT 'a ' || op || ' ' || quote_literal(arg) || '::int4[]'
FROM unnest(ARRAY['&&', '@>', '<@', '=']) op,
     unnest(ARRAY['{}', '{NULL}', '{3}', '{3,7}', '{1,2,3,4,5,6,7,8,9,10}',
                  '{100,200}', '{3,NULL}', '{5,30}', '{0}']) arg;
INSERT INTO mg_q VALUES ('a IS NULL'), ('a IS NOT NULL'),
  ('a && ''{3}'' AND a && ''{7}'''), ('a @> ''{3}'' AND a <@ ''{1,2,3,4,5,6,7,8,9,10,11,12}''');
SELECT q, cardinality(mg_run(NULL, q)) AS n,
       mg_run('mg_bark', q) = mg_run(NULL, q) AS bark_ok,
       mg_run('mg_bark11', q) = mg_run(NULL, q) AS bark11_ok,
       mg_run('mg_gin', q) = mg_run(NULL, q) AS gin_ok
FROM mg_q ORDER BY q;
-- = ANY over int4[] values cannot be written as an index qual (an array of
-- int4[] is a two-dimensional int4[]), so the scan's ScalarArrayOp path is
-- unreachable for this class; ANY over a sub-select is a filter, checked
-- here for its rows.
CREATE TABLE mg_arr (q int4[]);
INSERT INTO mg_arr VALUES ('{3}'), ('{100}'), ('{5,6}');
CREATE FUNCTION mg_saop(idx text) RETURNS int[] LANGUAGE plpgsql AS $$
DECLARE r int[]; qs int4[][];
BEGIN
  PERFORM set_config('enable_seqscan', (idx IS NULL)::text, true);
  PERFORM set_config('enable_bitmapscan', (idx IS NOT NULL)::text, true);
  PERFORM set_config('enable_indexscan', 'off', true);
  IF idx IS NOT NULL THEN
    EXECUTE format('UPDATE pg_index SET indisvalid = (indexrelid = %L::regclass) WHERE indrelid = ''mg''::regclass AND indexrelid <> ''mg_pkey''::regclass', idx);
  END IF;
  SELECT coalesce(array_agg(id ORDER BY id), '{}') INTO r FROM mg
   WHERE a && ANY (SELECT q FROM mg_arr);
  UPDATE pg_index SET indisvalid = true WHERE indrelid = 'mg'::regclass;
  RETURN r;
END $$;
SELECT cardinality(mg_saop(NULL)) AS n, mg_saop('mg_bark') = mg_saop(NULL) AS bark_ok,
       mg_saop('mg_gin') = mg_saop(NULL) AS gin_ok;

-- Bitmap and index scans (bark_multikey_ginscan compares the latter).
UPDATE pg_index SET indisvalid = (indexrelid = 'mg_bark'::regclass)
  WHERE indrelid = 'mg'::regclass AND indexrelid <> 'mg_pkey'::regclass;
SET enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT id FROM mg WHERE a @> '{3}';
RESET enable_seqscan;
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF) SELECT id FROM mg WHERE a @> '{3}';
RESET enable_bitmapscan;
RESET enable_seqscan;
UPDATE pg_index SET indisvalid = true WHERE indrelid = 'mg'::regclass;
SELECT pg_index_column_has_property('mg_bark', 1, 'orderable'),
       pg_index_has_property('mg_bark', 'index_scan'),
       pg_index_has_property('mg_bark', 'bitmap_scan');
-- A direct caller of amgettuple reads the result both ways.
SELECT bark_multikey_reverse('mg_bark', '@>(anyarray,anyarray)', '{3}'::int4[], 2);

-- Changes after the build, then the comparison again.
DELETE FROM mg WHERE id % 7 = 0;
INSERT INTO mg SELECT i, ARRAY[i % 13, i % 17] FROM generate_series(3001, 4000) i;
VACUUM mg;
SELECT bark_index_check('mg_bark');
SELECT count(*) FILTER (WHERE mg_run('mg_bark', q) = mg_run(NULL, q)) AS bark_ok,
       count(*) FILTER (WHERE mg_run('mg_gin', q) = mg_run(NULL, q)) AS gin_ok,
       count(*) AS total
FROM mg_q;

-- Partial match: comparePartial (12) ends the range.
CREATE TABLE mp (id int, t text[] COLLATE "C");
INSERT INTO mp SELECT i, ARRAY['p' || (i % 37), 'q' || (i % 11), chr(97 + i % 26) || i]
FROM generate_series(1, 2000) i;
INSERT INTO mp VALUES (2001, NULL), (2002, '{}'), (2003, '{NULL,p}');
CREATE INDEX mp_bark ON mp USING bark (t bark_text_prefix_ops);
SELECT bark_index_check('mp_bark');
SET enable_seqscan = off;
SET enable_indexscan = off;
EXPLAIN (COSTS OFF) SELECT id FROM mp WHERE t %^% 'p1';
CREATE TABLE mp_res AS
SELECT p, (SELECT array_agg(id ORDER BY id) FROM mp WHERE t %^% p) AS bark
FROM unnest(ARRAY['p', 'p1', 'p36', 'q1', 'z', 'a1', 'zz', '']) p;
RESET enable_seqscan;
RESET enable_indexscan;
SELECT p, cardinality(bark) AS n,
  bark IS NOT DISTINCT FROM (SELECT array_agg(id ORDER BY id) FROM mp
    WHERE EXISTS (SELECT 1 FROM unnest(t) e WHERE e LIKE p || '%')) AS like_ok
FROM mp_res ORDER BY p;

-- = ANY over the prefix class's scalar query: each element's rows are
-- unioned.  A NULL element is skipped, and a NULL argument (in a generic
-- plan, so the planner cannot fold it) matches nothing.
SET enable_seqscan = off;
SET enable_indexscan = off;
EXPLAIN (COSTS OFF) SELECT id FROM mp WHERE t %^% ANY (ARRAY['p1', 'q1', NULL]);
CREATE TABLE mp_any AS
  SELECT array_agg(id ORDER BY id) AS bark FROM mp WHERE t %^% ANY (ARRAY['p1', 'q1', NULL]);
SET plan_cache_mode = force_generic_plan;
PREPARE mp_p(text) AS SELECT count(*) FROM mp WHERE t %^% $1;
EXPLAIN (COSTS OFF) EXECUTE mp_p(NULL);
EXECUTE mp_p(NULL);
EXECUTE mp_p('p36');
DEALLOCATE mp_p;
RESET plan_cache_mode;
RESET enable_seqscan;
RESET enable_indexscan;
SELECT cardinality(bark) AS n,
  bark = (SELECT array_agg(id ORDER BY id) FROM mp
           WHERE EXISTS (SELECT 1 FROM unnest(t) e WHERE e LIKE 'p1%' OR e LIKE 'q1%'))
  AS like_ok
FROM mp_any;

-- A bitmap scan of a partial match reads each boundary straight into the
-- TIDBitmap (bark_mk_gin_key), so the bitmap's work_mem bounds it: at 64kB
-- it goes lossy, at 64MB it stays exact, and the rows are the seqscan's
-- either way.  Every row holds 'pp', one key of 60000 rows in POSTING
-- entries; 'p' matches it and the 'p<n>' keys, 'q1' some rows, 'zz' none.
-- The heap rows are wide, so that 64kB of bitmap cannot hold every page.
CREATE TABLE mb (id int, t text[] COLLATE "C", pad text);
INSERT INTO mb SELECT i, ARRAY['pp', 'p' || (i % 37), 'q' || (i % 11)], repeat('x', 200)
FROM generate_series(1, 60000) i;
CREATE INDEX mb_bark ON mb USING bark (t bark_text_prefix_ops);
VACUUM ANALYZE mb;
SELECT bark_index_check('mb_bark');
CREATE EXTENSION pageinspect;
SELECT count(*) FILTER (WHERE shape = 'POSTING') > 0 AS has_posting
FROM generate_series(1, pg_relation_size('mb_bark') / 8192 - 1) b,
     bark_page_items('mb_bark', b);
DROP EXTENSION pageinspect;
-- The rows of q through a bitmap scan (bitmap) or a seqscan, and whether
-- the scan's heap blocks went lossy.
CREATE FUNCTION mb_run(q text, bitmap bool, OUT ids int[], OUT lossy bool)
LANGUAGE plpgsql AS $$
DECLARE l text;
BEGIN
  PERFORM set_config('enable_seqscan', (NOT bitmap)::text, true);
  PERFORM set_config('enable_bitmapscan', bitmap::text, true);
  PERFORM set_config('enable_indexscan', 'off', true);
  EXECUTE format('SELECT coalesce(array_agg(id ORDER BY id), ''{}'') FROM mb WHERE %s', q)
    INTO ids;
  lossy := false;
  FOR l IN EXECUTE format('EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) SELECT id FROM mb WHERE %s', q) LOOP
    lossy := lossy OR l LIKE '%lossy=%';
  END LOOP;
END $$;
CREATE TABLE mb_q (q text);
INSERT INTO mb_q VALUES ($$t %^% 'p'$$), ($$t %^% 'pp'$$), ($$t %^% 'p1'$$),
  ($$t %^% 'q1'$$), ($$t %^% 'zz'$$), ($$t %^% ANY (ARRAY['p3', 'q10'])$$),
  ($$t %^% 'p1' AND t %^% 'q1'$$);
SET work_mem = '64kB';
SELECT q, cardinality(b.ids) AS n, b.ids = s.ids AS ok, b.lossy
FROM mb_q, mb_run(q, true) b, mb_run(q, false) s ORDER BY q;
SET work_mem = '64MB';
SELECT q, cardinality(b.ids) AS n, b.ids = s.ids AS ok, b.lossy
FROM mb_q, mb_run(q, true) b, mb_run(q, false) s ORDER BY q;
RESET work_mem;
DROP TABLE mb, mb_q;
DROP FUNCTION mb_run(text, bool);

-- An open-ended partial-match boundary read forward on the leading,
-- ascending column stops comparing its keys with the start once one is
-- inside it (bark_mk_gin_first's startlatch).  A DESC column and a later
-- column (whose keys restart at each value of the first) compare every key.
-- Each index's rows, through a bitmap scan and an index scan, are the
-- seqscan's; the NULL and empty arrays sort at one end and never match.
CREATE TABLE ml_asc (id int, g int, t text[] COLLATE "C");
INSERT INTO ml_asc SELECT i, i % 5, ARRAY['k' || (i % 97), 'm' || (i % 13), 'z' || i]
FROM generate_series(1, 5000) i;
INSERT INTO ml_asc VALUES (5001, 1, NULL), (5002, 2, '{}'), (5003, 3, '{NULL,k1}');
CREATE TABLE ml_desc AS SELECT * FROM ml_asc;
CREATE TABLE ml_two AS SELECT * FROM ml_asc;
CREATE INDEX ON ml_asc USING bark (t bark_text_prefix_ops);
CREATE INDEX ON ml_desc USING bark (t bark_text_prefix_ops DESC);
CREATE INDEX ON ml_two USING bark (g, t bark_text_prefix_ops);
SELECT bark_index_check(indexrelid) FROM pg_index
WHERE indrelid IN ('ml_asc'::regclass, 'ml_desc'::regclass, 'ml_two'::regclass);
CREATE FUNCTION ml_run(tab text, q text, how text) RETURNS int[]
LANGUAGE plpgsql AS $$
DECLARE r int[];
BEGIN
  PERFORM set_config('enable_seqscan', (how = 'seq')::text, true);
  PERFORM set_config('enable_bitmapscan', (how = 'bitmap')::text, true);
  PERFORM set_config('enable_indexscan', (how = 'index')::text, true);
  EXECUTE format('SELECT coalesce(array_agg(id ORDER BY id), ''{}'') FROM %I WHERE %s', tab, q) INTO r;
  RETURN r;
END $$;
CREATE TABLE ml_q (q text);
INSERT INTO ml_q VALUES ($$t %^% 'k'$$), ($$t %^% 'k1'$$), ($$t %^% 'k96'$$),
  ($$t %^% 'm'$$), ($$t %^% 'z49'$$), ($$t %^% ''$$), ($$t %^% 'zzz'$$),
  ($$g = 3 AND t %^% 'k2'$$), ($$g IN (1, 4) AND t %^% 'm1'$$);
SELECT tab, q, cardinality(s) AS n,
  ml_run(tab, q, 'bitmap') = s AS bitmap_ok,
  ml_run(tab, q, 'index') = s AS index_ok
FROM unnest(ARRAY['ml_asc', 'ml_desc', 'ml_two']) tab, ml_q,
     LATERAL ml_run(tab, q, 'seq') s
ORDER BY tab, q;
DROP TABLE ml_asc, ml_desc, ml_two, ml_q;
DROP FUNCTION ml_run(text, text, text);

DROP TABLE mg, mg_q, mg_arr, mp, mp_res, mp_any;
DROP FUNCTION mg_run(text, text), mg_saop(text);
DROP EXTENSION bark_multikey;
