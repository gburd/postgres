--
-- BARK indexes of every entry shape, checked the same way at any block size.
--
-- Each index below is built over rows that make it form one entry shape:
-- SINGLE, OVERSIZED, plain and coded LIST, POSTING, a multikey column, and
-- a three-level tree.  Each query is answered by every scan mode the index
-- offers and compared with a sequential scan, before and after a DELETE
-- and VACUUM, and every index passes bark_index_check with heapallindexed.
-- Only row counts and booleans are shown, so the expected output is the
-- same whatever the block size and alignment: a -Dblocksize=4 or 32 build
-- passes this file exactly when BARK returns the right rows there.
-- (The amcheck extension is created by the earlier "check" test.)
--
CREATE EXTENSION IF NOT EXISTS bark_multikey;
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS pageinspect;
RESET client_min_messages;

-- The rows of query q (a WHERE clause on table t) as one md5, by scan mode
-- m: seq, index, ios (index-only), bitmap, backward (an index scan in
-- descending order), cursor (a scroll cursor read forward, then backward
-- from its end).  The plan must use the mode asked for, or the result says
-- so.
CREATE FUNCTION ab_run(t text, cols text, q text, m text) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  sql text;
  plan text;
  ok bool := m = 'seq';
  r text;
  c refcursor;
  rec record;
  fwd text[] := '{}';
  bwd text[] := '{}';
BEGIN
  PERFORM set_config('enable_seqscan', (m = 'seq')::text, true);
  PERFORM set_config('enable_indexscan', (m IN ('index', 'backward', 'cursor'))::text, true);
  PERFORM set_config('enable_indexonlyscan', (m = 'ios')::text, true);
  PERFORM set_config('enable_bitmapscan', (m = 'bitmap')::text, true);
  PERFORM set_config('enable_sort', (m NOT IN ('backward', 'cursor'))::text, true);
  PERFORM set_config('debug_parallel_query', 'off', true);
  sql := format('SELECT %s FROM %I WHERE %s ORDER BY %s', cols, t, q,
                CASE WHEN m = 'backward'
                     THEN replace(cols, ',', ' DESC,') || ' DESC' ELSE cols END);
  FOR plan IN EXECUTE 'EXPLAIN (COSTS OFF) ' || sql LOOP
    ok := ok OR (m IN ('index', 'backward', 'cursor') AND plan ~ 'Index Scan')
             OR (m = 'ios' AND plan ~ 'Index Only Scan')
             OR (m = 'bitmap' AND plan ~ 'Bitmap Index Scan');
  END LOOP;
  IF NOT ok THEN
    RETURN 'no ' || m || ' plan';
  END IF;
  IF m = 'cursor' THEN
    OPEN c SCROLL FOR EXECUTE sql;
    LOOP
      FETCH NEXT FROM c INTO rec;
      EXIT WHEN NOT FOUND;
      fwd := fwd || rec::text;
    END LOOP;
    LOOP
      FETCH PRIOR FROM c INTO rec;
      EXIT WHEN NOT FOUND;
      bwd := rec::text || bwd;
    END LOOP;
    CLOSE c;
    IF fwd IS DISTINCT FROM bwd THEN
      RETURN 'cursor directions differ';
    END IF;
    RETURN cardinality(fwd) || ':' || md5(array_to_string(fwd, ','));
  END IF;
  EXECUTE format('SELECT count(*) || '':'' || md5(coalesce(string_agg(x::text, '','' ORDER BY rn), '''')) '
                 'FROM (SELECT x, row_number() OVER () AS rn FROM (%s) x) s', sql) INTO r;
  IF m = 'backward' THEN
    -- the same rows in the other order: compare as the ascending list
    EXECUTE format('SELECT count(*) || '':'' || md5(coalesce(string_agg(x::text, '','' ORDER BY rn DESC), '''')) '
                   'FROM (SELECT x, row_number() OVER () AS rn FROM (%s) x) s', sql) INTO r;
  END IF;
  RETURN r;
END $$;

-- For each query: the number of rows, and whether every mode in modes
-- returned the sequential scan's rows (the modes that disagree otherwise).
CREATE FUNCTION ab_check(t text, cols text, q text, modes text[],
                         OUT nrows int, OUT same text)
LANGUAGE plpgsql AS $$
DECLARE
  seq text := ab_run(t, cols, q, 'seq');
  m text;
  bad text[] := '{}';
BEGIN
  nrows := split_part(seq, ':', 1)::int;
  FOREACH m IN ARRAY modes LOOP
    IF ab_run(t, cols, q, m) IS DISTINCT FROM seq THEN
      bad := bad || m;
    END IF;
  END LOOP;
  same := CASE WHEN bad = '{}' THEN 'all' ELSE array_to_string(bad, ',') END;
END $$;

-- The leaf entry shapes of an index (pageinspect), a LIST shown as coded
-- when its stored offset has BARK_LIST_CODED (0x1000).
CREATE FUNCTION ab_shapes(idx text) RETURNS text[] LANGUAGE sql AS $$
  SELECT array_agg(DISTINCT s ORDER BY s)
    FROM (SELECT i.shape || CASE WHEN i.shape = 'LIST' AND
                                      ((i.ctid::text::point)[1]::int & 4096) <> 0
                                 THEN ' coded' ELSE '' END AS s
            FROM bark_multi_page_stats(idx, 1, -1) p,
                 LATERAL bark_page_items(idx, p.blkno) i
           WHERE p.type IN ('leaf', 'root') AND i.shape <> 'PIVOT') e $$;

-- A deterministic incompressible string of n bytes.
CREATE FUNCTION ab_str(s int, n int) RETURNS text LANGUAGE sql IMMUTABLE AS
$$ SELECT substr(string_agg(md5(s::text || g::text), ''), 1, n)
   FROM generate_series(1, (n + 31) / 32) g $$;

-- The table: k has every shape.  Keys 1..3000 are distinct (SINGLE);
-- 10001..10100 have three rows each;
-- 20001..20020 have two rows each (a plain LIST: coding two TIDs saves
-- nothing);
-- 30001 and 30002 have 3000 rows each (POSTING).  t holds keys longer than
-- an item of any block size (OVERSIZED) and short ones; a holds arrays for
-- the multikey index, whose keys gather rows adjacent in the heap (coded
-- LISTs) and many rows (POSTING).  The table grows by inserts after the indexes exist,
-- so that they split, and the k index reaches three levels.
CREATE TABLE ab (id int, k int, t text COLLATE "C", a int4[])
  WITH (autovacuum_enabled = off);
CREATE INDEX ab_k ON ab USING bark (k);
CREATE INDEX ab_kid ON ab USING bark (k, id);
CREATE INDEX ab_t ON ab USING bark (t);
CREATE INDEX ab_a ON ab USING bark (a bark_int4_array_ops);
INSERT INTO ab SELECT g, g, 'short' || g, ARRAY[g % 50, g % 7 + 100]
  FROM generate_series(1, 3000) g;
INSERT INTO ab SELECT 3000 + g, 10000 + (g + 2) / 3, NULL, ARRAY[g % 50]
  FROM generate_series(1, 300) g;
INSERT INTO ab SELECT 3300 + g, 30001 + g % 2, NULL, NULL
  FROM generate_series(1, 6000) g;
INSERT INTO ab SELECT 9300 + g, 20000 + (g - 1) % 20 + 1, NULL, '{}'
  FROM generate_series(1, 40) g;
INSERT INTO ab SELECT 9400 + g, -g, ab_str(g, 40000), ARRAY[g, g + 1, g + 2]
  FROM generate_series(1, 30) g;
-- Wide keys (a twentieth of a page, incompressible), so that this index
-- reaches three levels at any block size within the table's 15000 rows.
CREATE FUNCTION ab_wkey(k int) RETURNS text LANGUAGE sql IMMUTABLE AS
$$ SELECT lpad(k::text, 8, '0') || public.ab_str(k, current_setting('block_size')::int / 20) $$;
CREATE INDEX ab_wide ON ab USING bark ((ab_wkey(k)), id);
INSERT INTO ab SELECT 20000 + g, 40000 + g, 'more' || g, ARRAY[g % 300]
  FROM generate_series(1, 6000) g;
VACUUM ANALYZE ab;

-- What the indexes hold: every shape, and a tree of three levels or more.
SELECT ab_shapes('ab_k') @> '{LIST,POSTING,SINGLE}' AS k_shapes,
       ab_shapes('ab_t') @> '{OVERSIZED,SINGLE}' AS t_shapes,
       ab_shapes('ab_a') @> '{LIST,"LIST coded",POSTING}' AS a_shapes,
       (SELECT level FROM bark_metap('ab_wide')) >= 2 AS three_levels;
SELECT bark_index_check(i, true), bark_index_parent_check(i, true)
  FROM unnest('{ab_k,ab_kid,ab_t,ab_a,ab_wide}'::regclass[]) i;

CREATE TABLE ab_q (n serial, t text, cols text, q text, modes text[]);
INSERT INTO ab_q (t, cols, q, modes) VALUES
  ('ab', 'k', 'k BETWEEN 100 AND 2000', '{index,ios,bitmap,backward,cursor}'),
  ('ab', 'k', 'k BETWEEN 10001 AND 10100', '{index,ios,bitmap,backward,cursor}'),
  ('ab', 'k', 'k BETWEEN 20001 AND 20020', '{index,ios,bitmap,backward,cursor}'),
  ('ab', 'k', 'k = 30001', '{index,ios,bitmap}'),
  ('ab', 'k', 'k IN (5, 10050, 20007, 30002, 45000)', '{index,ios,bitmap}'),
  ('ab', 'k', 'k > 40000', '{index,ios,bitmap,backward,cursor}'),
  ('ab', 'k, id', 'k = 30002 AND id > 6000', '{index,ios,bitmap,backward}'),
  ('ab', 't', 't > ''''', '{index,bitmap,backward,cursor}'),
  ('ab', 't', 't = ab_str(7, 40000)', '{index,bitmap}'),
  ('ab', 'id', 'a && ''{3,105}''', '{index,bitmap}'),
  ('ab', 'id', 'a @> ''{8,9}''', '{index,bitmap}'),
  ('ab', 'id', 'ab_wkey(k) > ab_wkey(41000)', '{index,bitmap}');
SELECT n, q, c.* FROM ab_q, ab_check(t, cols, q, modes) c ORDER BY n;

-- Delete a part of every shape and VACUUM: the emptied leaves go, the
-- POSTING and LIST entries shrink, and every scan still agrees.
DELETE FROM ab WHERE k % 3 = 0 OR id % 5 = 0 OR k BETWEEN 40001 AND 45000;
VACUUM ab;
SELECT bark_index_check(i, true), bark_index_parent_check(i, true)
  FROM unnest('{ab_k,ab_kid,ab_t,ab_a,ab_wide}'::regclass[]) i;
SELECT n, q, c.* FROM ab_q, ab_check(t, cols, q, modes) c ORDER BY n;

-- Refill the freed space, and check once more.
INSERT INTO ab SELECT 30000 + g, 40000 + g, 'again' || g, ARRAY[g % 30]
  FROM generate_series(1, 5000) g;
SELECT bark_index_check(i, true), bark_index_parent_check(i, true)
  FROM unnest('{ab_k,ab_kid,ab_t,ab_a,ab_wide}'::regclass[]) i;
SELECT n, q, c.* FROM ab_q, ab_check(t, cols, q, modes) c ORDER BY n;

DROP TABLE ab, ab_q;
DROP FUNCTION ab_run(text, text, text, text), ab_check(text, text, text, text[]),
  ab_shapes(text), ab_wkey(int), ab_str(int, int);
DROP EXTENSION bark_multikey;
DROP EXTENSION pageinspect;
