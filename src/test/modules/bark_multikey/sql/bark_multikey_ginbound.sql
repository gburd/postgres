-- BARK scans through GIN's query interface whose streams are bounded: each
-- boundary is read into a TIDBitmap of its own (work_mem, lossy beyond)
-- and the streams merge a heap page at a time.  Every query runs as a
-- bitmap scan and as an index scan of each BARK index, at a work_mem that
-- keeps every bitmap exact and at one that makes them lossy, and is
-- compared with a sequential scan and a GIN index on the same table.
CREATE EXTENSION IF NOT EXISTS bark_multikey;
CREATE EXTENSION IF NOT EXISTS pageinspect;
SELECT setseed(0.31);
-- 0 is in every row with a non-empty array (its rows span many leaves of
-- POSTING entries), 1 in half, 2 in a quarter, -1 in a third, -2 in a
-- fifth, 3 in one row in a hundred; the rest of the elements spread over
-- 4 .. 4003.  pad makes the table about 1600 heap pages, more than a 64kB
-- TIDBitmap holds exactly, so at that work_mem the frequent elements'
-- streams go lossy.
CREATE TABLE gb (id int PRIMARY KEY, a int4[], t text[], pad text);
INSERT INTO gb SELECT i,
  CASE WHEN i % 211 = 0 THEN NULL
       WHEN i % 199 = 0 THEN '{}'
       WHEN i % 193 = 0 THEN ARRAY[NULL, 4 + i % 4000]::int4[]
       ELSE ARRAY[0] || CASE WHEN i % 2 = 0 THEN ARRAY[1] ELSE '{}' END
            || CASE WHEN i % 4 = 0 THEN ARRAY[2] ELSE '{}' END
            || CASE WHEN i % 3 = 0 THEN ARRAY[-1] ELSE '{}' END
            || CASE WHEN i % 5 = 0 THEN ARRAY[-2] ELSE '{}' END
            || CASE WHEN i % 100 = 0 THEN ARRAY[3] ELSE '{}' END
            || ARRAY(SELECT 4 + (random() * 3999)::int4
                     FROM generate_series(1, 1 + i % 4)) END,
  CASE WHEN i % 211 = 0 THEN NULL
       ELSE ARRAY['k' || (i % 500), 'p' || (i % 37) || '_' || (i % 11)] END,
  repeat('x', 160)
FROM generate_series(1, 40000) i;
CREATE INDEX gb_bark ON gb USING bark (a bark_gin_int4_array_ops);
CREATE INDEX gb_bark11 ON gb USING bark (a bark_gin11_int4_array_ops);
CREATE INDEX gb_t ON gb USING bark (t bark_text_prefix_ops);
CREATE INDEX gb_gin ON gb USING gin (a);
VACUUM ANALYZE gb;
SELECT bark_index_check('gb_bark'), bark_index_check('gb_bark11'),
       bark_index_check('gb_t');
-- 0's entries span many leaves, and the heap is past a 64kB bitmap.
SELECT pg_relation_size('gb') / 8192 > 1200 AS big_heap;
SELECT count(*) > 20 AS many_leaves
FROM generate_series(1, pg_relation_size('gb_bark') / 8192 - 1) b,
     bark_page_items('gb_bark', b) p
WHERE p.shape IN ('LIST', 'POSTING');

-- Rows of q on gb through index idx alone (NULL: seqscan) as a bitmap scan
-- (bitmap) or an index scan, and whether a bitmap scan's heap blocks went
-- lossy.
CREATE FUNCTION gb_run(idx text, q text, bitmap bool, OUT ids int[],
                       OUT lossy bool)
LANGUAGE plpgsql AS $$
DECLARE l text;
BEGIN
  PERFORM set_config('enable_seqscan', (idx IS NULL)::text, true);
  PERFORM set_config('enable_bitmapscan', (idx IS NOT NULL AND bitmap)::text, true);
  PERFORM set_config('enable_indexscan', (idx IS NOT NULL AND NOT bitmap)::text, true);
  PERFORM set_config('enable_indexonlyscan', 'off', true);
  IF idx IS NOT NULL THEN
    EXECUTE format('UPDATE pg_index SET indisvalid = (indexrelid = %L::regclass) WHERE indrelid = ''gb''::regclass AND indexrelid <> ''gb_pkey''::regclass', idx);
  END IF;
  EXECUTE 'SELECT coalesce(array_agg(id ORDER BY id), ''{}'') FROM gb WHERE ' || q INTO ids;
  lossy := false;
  FOR l IN EXECUTE 'EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) SELECT id FROM gb WHERE ' || q LOOP
    lossy := lossy OR l LIKE '%lossy=%';
    IF idx IS NOT NULL AND l LIKE '%Seq Scan%' THEN
      RAISE EXCEPTION 'no scan of % for % (work_mem %): %', idx, q,
        current_setting('work_mem'), l;
    END IF;
  END LOOP;
  UPDATE pg_index SET indisvalid = true WHERE indrelid = 'gb'::regclass;
END $$;
CREATE TABLE gb_q (q text, idx text);
INSERT INTO gb_q (q) VALUES
  -- a key whose rows exceed many leaves, alone, with a rare and a frequent
  -- one (AND), and with another frequent one
  ($$a @> '{0}'$$), ($$a @> '{0,3}'$$), ($$a @> '{3,0}'$$), ($$a @> '{1,2}'$$),
  ($$a @> '{0,1,2,3}'$$), ($$a @> '{0,4000}'$$),
  -- five frequent keys: more MAYBE entries than consistent's shim tries
  ($$a @> '{0,1,2,-1,-2}'$$),
  -- OR of keys, frequent and rare
  ($$a && '{1,3}'$$), ($$a && '{3,5,6,7,8,9}'$$),
  -- two scan keys (AND of keys), and = ANY over int4[] rows of a sub-select
  ($$a @> '{1}' AND a @> '{2}'$$), ($$a && '{3}' AND a @> '{0}'$$),
  ($$a @> '{3}' AND a @> '{0}'$$),
  ($$a @> '{0}' AND a <@ '{0,1,2,3,4,5,6,7,8,9,10}'$$),
  -- NULL and empty handling: <@ and = with an empty array take the empty
  -- rows (hidden streams), NULL elements, IS NULL, IS NOT NULL
  ($$a <@ '{0,1}'$$), ($$a = '{}'$$), ($$a <@ '{}'$$), ($$a @> '{}'$$),
  ($$a && '{NULL}'$$), ($$a @> '{0,NULL}'$$), ($$a IS NULL$$), ($$a IS NOT NULL$$),
  ($$a = '{0,1}'$$);
UPDATE gb_q SET idx = 'int4';
INSERT INTO gb_q VALUES
  -- partial match spanning many keys (every k* key, and the p1* prefix)
  ($$t %^% 'k'$$, 'text'), ($$t %^% 'p1'$$, 'text'), ($$t %^% 'k1' AND t %^% 'p2'$$, 'text'),
  ($$t %^% ANY (ARRAY['k49', 'p3_', 'zz'])$$, 'text'), ($$t %^% 'zz'$$, 'text');
-- Every scan of every query, at the current work_mem.  GIN has no IS NULL
-- search (amsearchnulls), and no index scans.  At 64kB the planner prices
-- BARK's scans for IS NULL and IS NOT NULL (a lossy bitmap over the whole
-- heap) above even a disabled seqscan, so those two run at 64MB only.
CREATE TABLE gb_res (mem text, q text, kind text, idx text, bitmap bool,
                     ids int[], lossy bool);
CREATE FUNCTION gb_all() RETURNS void LANGUAGE sql AS $$
INSERT INTO gb_res
SELECT current_setting('work_mem'), q.q, q.idx, i.idx, m.bitmap, r.ids, r.lossy
FROM gb_q q
CROSS JOIN LATERAL unnest(CASE q.idx WHEN 'int4' THEN ARRAY['gb_bark', 'gb_bark11', 'gb_gin']
                                    ELSE ARRAY['gb_t'] END) i(idx)
CROSS JOIN unnest(ARRAY[true, false]) m(bitmap)
CROSS JOIN LATERAL gb_run(i.idx, q.q, m.bitmap) r
WHERE (m.bitmap OR i.idx <> 'gb_gin') AND
      NOT (i.idx = 'gb_gin' AND q.q LIKE '%IS %NULL') AND
      NOT (q.q LIKE '%IS %NULL' AND current_setting('work_mem') = '64kB');
$$;
SET work_mem = '64kB';
SELECT gb_all();
SET work_mem = '64MB';
SELECT gb_all();
RESET work_mem;
DROP FUNCTION gb_all();
CREATE TABLE gb_seq AS
SELECT q, (gb_run(NULL, q, false)).ids FROM gb_q;
-- Every scan's rows are the seqscan's.
SELECT r.q, cardinality(s.ids) AS n,
       bool_and(r.ids = s.ids) AS ok,
       count(*) AS scans
FROM gb_res r JOIN gb_seq s USING (q)
GROUP BY r.q, s.ids ORDER BY r.q COLLATE "C";
-- The small work_mem made the BARK bitmap scans' bitmaps lossy, the large
-- one left them exact.
SELECT mem, bool_or(lossy) AS any_lossy, bool_and(NOT lossy) AS all_exact
FROM gb_res WHERE idx <> 'gb_gin' AND bitmap GROUP BY mem ORDER BY mem COLLATE "C";
DROP TABLE gb_res;

-- The merge's corners, at 64kB.  With a key column after the
-- GIN-interface column every point's stream turns into a bitmap, and the
-- frequent elements' bitmaps go lossy.  Without triConsistent
-- (bark_gin11_int4_array_ops) consistent then decides a lossy page through
-- ginlogic.c's shim, which tries every combination of up to four MAYBE
-- entries (two or three frequent elements) and answers MAYBE beyond: five
-- copies of one element are five identical streams, lossy on the same
-- pages.
-- A key whose result is empty ends a two-key AND before the second key is
-- read.
CREATE INDEX gb_bark11_a2 ON gb USING bark (a bark_gin11_int4_array_ops, (id % 3));
SET work_mem = '64kB';
SELECT q, (gb_run('gb_bark11_a2', q, true)).ids = s.ids AS bark11_ok,
       (gb_run('gb_bark', q, true)).ids = s.ids AS bark_ok
FROM unnest(ARRAY[$$a @> '{0,1}'$$, $$a @> '{1,-1}'$$, $$a && '{2,-2}'$$,
                  $$a @> '{0,1,2}'$$, $$a @> '{0,1,2,-1,-2}'$$, $$a @> '{0,0,0,0,0}'$$,
                  $$a <@ '{0,1,2,-1,-2,3}'$$,
                  $$a @> '{4100}' AND a @> '{0}'$$]) q,
     LATERAL gb_run(NULL, q, true) s ORDER BY q COLLATE "C";
RESET work_mem;
DROP INDEX gb_bark11_a2;

-- After DELETE, INSERT (LIST entries and new rows of the rare key) and
-- VACUUM, the results still agree.
DELETE FROM gb WHERE id % 7 = 0;
INSERT INTO gb SELECT i, ARRAY[0, 3, 4 + i % 13], ARRAY['k' || (i % 5)], ''
FROM generate_series(40001, 41000) i;
VACUUM gb;
SELECT bark_index_check('gb_bark'), bark_index_check('gb_t');
-- The seqscan's rows of each query, for this and the checks below.
DROP TABLE gb_seq;
CREATE TABLE gb_seq AS
SELECT q, (gb_run(NULL, q, false)).ids FROM gb_q;
SELECT count(*) FILTER (WHERE (gb_run(i, q, true)).ids = s.ids
                          AND (gb_run(i, q, false)).ids = s.ids) AS ok,
       count(*) AS total
FROM gb_q JOIN gb_seq s USING (q), LATERAL unnest(CASE idx WHEN 'int4' THEN ARRAY['gb_bark', 'gb_bark11']
                                  ELSE ARRAY['gb_t'] END) i;

-- An index with a column before the GIN-interface one (bitmap scans only):
-- a point's entries are not in TID order across the column's values, which
-- the per-boundary bitmap absorbs.
CREATE INDEX gb_bark_g ON gb USING bark ((id % 3), a bark_gin_int4_array_ops);
SELECT count(*) FILTER (WHERE (gb_run('gb_bark_g', q, true)).ids = s.ids) AS ok,
       count(*) AS total
FROM gb_q JOIN gb_seq s USING (q) WHERE idx = 'int4';
-- A key column after it: a point's entries are in (key, id % 3, TID)
-- order, so its TIDs stop ascending at each new value of the later column,
-- and the exact stream of a point turns into a bitmap there.  At 64kB the
-- bitmaps go lossy too.
CREATE INDEX gb_bark_a2 ON gb USING bark (a bark_gin_int4_array_ops, (id % 3));
SET work_mem = '64kB';
SELECT count(*) FILTER (WHERE (gb_run('gb_bark_a2', q, true)).ids = s.ids
                          AND (gb_run('gb_bark_a2', q, false)).ids = s.ids) AS ok,
       count(*) AS total
FROM gb_q JOIN gb_seq s USING (q) WHERE idx = 'int4' AND q NOT LIKE '%IS %NULL';
RESET work_mem;

DROP TABLE gb, gb_q, gb_seq;
DROP FUNCTION gb_run(text, text, bool);
DROP EXTENSION pageinspect;
DROP EXTENSION bark_multikey;
