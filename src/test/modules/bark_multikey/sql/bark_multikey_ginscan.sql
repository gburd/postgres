-- BARK index scans (amgettuple) through GIN's query interface: the scan
-- computes its whole result, in TID order, before the first row.  Every
-- query of bark_multikey_gin and bark_multikey_ginmerge runs as an index
-- scan and is compared with a sequential scan, whole and under LIMIT, then
-- again after DELETE, INSERT and VACUUM.
CREATE EXTENSION IF NOT EXISTS bark_multikey;

-- Rows of a query on table tab through index idx alone (NULL: seqscan),
-- the first lim of them (NULL: all, as a sorted array).  The index path
-- must be an Index Scan of idx.  A scan through GIN's interface returns
-- rows in TID order, so its first lim rows are a seqscan's first lim by
-- ctid.
CREATE FUNCTION gs_run(tab text, idx text, q text, lim int DEFAULT NULL)
RETURNS int[] LANGUAGE plpgsql AS $$
DECLARE r int[]; s text; plan text := ''; l text;
BEGIN
  PERFORM set_config('enable_seqscan', (idx IS NULL)::text, true);
  PERFORM set_config('enable_bitmapscan', 'off', true);
  PERFORM set_config('enable_indexscan', (idx IS NOT NULL)::text, true);
  PERFORM set_config('enable_indexonlyscan', 'off', true);
  IF idx IS NOT NULL THEN
    EXECUTE format('UPDATE pg_index SET indisvalid = (indexrelid = %L::regclass) WHERE indrelid = %L::regclass',
                   idx, tab);
  END IF;
  IF lim IS NULL THEN
    s := format('SELECT coalesce(array_agg(id ORDER BY id), ''{}'') FROM (SELECT id FROM %I WHERE %s) s', tab, q);
  ELSE
    s := format('SELECT coalesce(array_agg(id), ''{}'') FROM (SELECT id FROM %I WHERE %s %s LIMIT %s) s',
                tab, q, CASE WHEN idx IS NULL THEN 'ORDER BY ctid' ELSE '' END, lim);
  END IF;
  IF idx IS NOT NULL THEN
    FOR l IN EXECUTE 'EXPLAIN (COSTS OFF) ' || s LOOP
      plan := plan || l || E'\n';
    END LOOP;
    IF position('Index Scan using ' || idx || ' ' IN plan) = 0 THEN
      RAISE EXCEPTION 'no index scan of % for %: %', idx, q, plan;
    END IF;
  END IF;
  EXECUTE s INTO r;
  EXECUTE format('UPDATE pg_index SET indisvalid = true WHERE indrelid = %L::regclass', tab);
  RETURN r;
END $$;

-- bark_multikey_gin's table and queries.
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
SELECT pg_index_has_property('mg_bark', 'index_scan'),
       pg_index_has_property('mg_bark', 'backward_scan');
CREATE TABLE mg_q (q text);
INSERT INTO mg_q SELECT 'a ' || op || ' ' || quote_literal(arg) || '::int4[]'
FROM unnest(ARRAY['&&', '@>', '<@', '=']) op,
     unnest(ARRAY['{}', '{NULL}', '{3}', '{3,7}', '{1,2,3,4,5,6,7,8,9,10}',
                  '{100,200}', '{3,NULL}', '{5,30}', '{0}']) arg;
INSERT INTO mg_q VALUES ('a IS NULL'), ('a IS NOT NULL'),
  ('a && ''{3}'' AND a && ''{7}'''), ('a @> ''{3}'' AND a <@ ''{1,2,3,4,5,6,7,8,9,10,11,12}''');
CREATE FUNCTION gs_check(tab text, qtab text, idxs text[])
RETURNS TABLE (q text, n int, ok bool[], lim1_ok bool[], lim7_ok bool[])
LANGUAGE plpgsql AS $$
BEGIN
  FOR q IN EXECUTE format('SELECT q FROM %I ORDER BY q', qtab) LOOP
    n := cardinality(gs_run(tab, NULL, q));
    SELECT array_agg(gs_run(tab, i, q) = gs_run(tab, NULL, q) ORDER BY o),
           array_agg(gs_run(tab, i, q, 1) = gs_run(tab, NULL, q, 1) ORDER BY o),
           array_agg(gs_run(tab, i, q, 7) = gs_run(tab, NULL, q, 7) ORDER BY o)
      INTO ok, lim1_ok, lim7_ok FROM unnest(idxs) WITH ORDINALITY u(i, o);
    RETURN NEXT;
  END LOOP;
END $$;
SELECT * FROM gs_check('mg', 'mg_q', '{mg_bark,mg_bark11}');

-- bark_multikey_ginmerge's tables and queries.
SELECT setseed(0.7);
CREATE TABLE gm (id int PRIMARY KEY, g int, a int4[]);
INSERT INTO gm SELECT i, i % 3,
  CASE WHEN i % 97 = 0 THEN NULL
       WHEN i % 89 = 0 THEN '{}'
       ELSE ARRAY(SELECT floor(200 * random() ^ 3)::int4
                  FROM generate_series(1, 3 + i % 12)) END
FROM generate_series(1, 8000) i;
CREATE INDEX gm_bark ON gm USING bark (a bark_gin_int4_array_ops);
CREATE INDEX gm_bark11 ON gm USING bark (a bark_gin11_int4_array_ops);
CREATE INDEX gm_bark_g ON gm USING bark (g, a bark_gin_int4_array_ops);
CREATE TABLE gm_q (q text);
INSERT INTO gm_q SELECT 'a ' || op || ' ' || quote_literal(arg) || '::int4[]'
FROM unnest(ARRAY['&&', '@>', '<@', '=']) op,
     unnest(ARRAY['{190,0}', '{180,0,1,2,3}', '{0,1}', '{150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170}',
                  '{0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40}',
                  '{199,0,1}', '{0,0,1}', '{5000,0}', '{0,NULL}']) arg;
INSERT INTO gm_q VALUES ('a @> ''{190}'' AND a @> ''{0,1}'''),
  ('a && ''{150,160}'' AND a @> ''{0}'''), ('a @> ''{0}'' AND a <@ ''{0,1,2,3,4,5,6,7,8,9}''');
SELECT * FROM gs_check('gm', 'gm_q', '{gm_bark,gm_bark11}');

-- An index with a column before the GIN-interface column returns rows in
-- TID order, not in that column's order: it stays bitmap-only.
SELECT pg_index_has_property('gm_bark_g', 'index_scan'),
       pg_index_has_property('gm_bark_g', 'bitmap_scan');
BEGIN;
UPDATE pg_index SET indisvalid = (indexrelid = 'gm_bark_g'::regclass)
  WHERE indrelid = 'gm'::regclass;
SET LOCAL enable_seqscan = off;
SET LOCAL enable_bitmapscan = off;
EXPLAIN (COSTS OFF) SELECT id FROM gm WHERE g = 1 AND a @> '{190}' ORDER BY g;
ROLLBACK;

-- The tsvector class of bark_multikey_ginmerge: point and prefix keys.
CREATE OPERATOR FAMILY bark_gin_tsvector_ops USING bark;
CREATE OPERATOR CLASS bark_gin_tsvector_ops
FOR TYPE tsvector USING bark FAMILY bark_gin_tsvector_ops AS
    OPERATOR 1 @@ (tsvector, tsquery),
    FUNCTION 1 (tsvector, tsvector) gin_cmp_tslexeme(text, text),
    FUNCTION 7 gin_extract_tsvector(tsvector, internal, internal),
    FUNCTION 10 gin_extract_tsquery(tsvector, internal, int2, internal,
                                    internal, internal, internal),
    FUNCTION 11 gin_tsquery_consistent(internal, int2, tsvector, int4,
                                       internal, internal, internal, internal),
    FUNCTION 12 gin_cmp_prefix(text, text, int2, internal),
    FUNCTION 13 gin_tsquery_triconsistent(internal, int2, tsvector, int4,
                                          internal, internal, internal),
    STORAGE text;
CREATE TABLE gt (id int PRIMARY KEY, v tsvector);
INSERT INTO gt SELECT i, to_tsvector('simple',
  (SELECT string_agg(w, ' ') FROM (SELECT (ARRAY['apple', 'apricot', 'banana',
     'band', 'bandit', 'cherry', 'cat', 'catalog', 'dog', 'zebra'])[1 + floor(10 * random() ^ 2)::int]
     || CASE WHEN random() < 0.1 THEN (i % 50)::text ELSE '' END AS w
   FROM generate_series(1, 2 + i % 6)) s))
FROM generate_series(1, 4000) i;
INSERT INTO gt VALUES (4001, ''), (4002, NULL);
CREATE INDEX gt_bark ON gt USING bark (v bark_gin_tsvector_ops);
CREATE TABLE gt_q (q text);
INSERT INTO gt_q SELECT format('v @@ %L::tsquery', t) FROM unnest(ARRAY[
  'apple', 'zebra & apple', 'ban:* & cherry', 'ban:* | cat:*', 'apple & ban:* & cat:* & dog',
  'zebra & !apple', '!apple', 'ap:* & !apricot', 'apple <-> banana', 'cat:* & zebra1:*',
  'nomatch:*', 'zebra & nomatch', 'b:* & c:* & d:*']) t;
SELECT * FROM gs_check('gt', 'gt_q', '{gt_bark}');

-- After DELETE, INSERT and VACUUM, every query again.
DELETE FROM mg WHERE id % 7 = 0;
INSERT INTO mg SELECT i, ARRAY[i % 13, i % 17] FROM generate_series(3001, 4000) i;
VACUUM mg;
DELETE FROM gm WHERE id % 5 = 0;
INSERT INTO gm SELECT i, i % 3, ARRAY[0, i % 7, 150 + i % 41]
FROM generate_series(8001, 9000) i;
VACUUM gm;
DELETE FROM gt WHERE id % 3 = 0;
INSERT INTO gt SELECT i, to_tsvector('simple', 'apple band' || i % 4 || ' zebra')
FROM generate_series(4003, 4600) i;
VACUUM gt;
SELECT bark_index_check('mg_bark'), bark_index_check('gm_bark'),
       bark_index_check('gt_bark');
SELECT t, count(*) FILTER (WHERE true = ALL (ok || lim1_ok || lim7_ok)) AS all_ok,
       count(*) AS total
FROM (SELECT 'mg' t, * FROM gs_check('mg', 'mg_q', '{mg_bark,mg_bark11}')
      UNION ALL SELECT 'gm', * FROM gs_check('gm', 'gm_q', '{gm_bark,gm_bark11}')
      UNION ALL SELECT 'gt', * FROM gs_check('gt', 'gt_q', '{gt_bark}')) s
GROUP BY t ORDER BY t;

-- Only mg_bark and gt_bark from here on.
DROP INDEX mg_bark11, gm_bark11, gm_bark_g;
SET enable_seqscan = off;
SET enable_bitmapscan = off;

-- A NO SCROLL cursor, read forward in pieces, returns the seqscan's rows.
-- (A SCROLL cursor is read through a Material node: the index has no
-- backward scans.)
EXPLAIN (COSTS OFF) SELECT id FROM mg WHERE a && '{3,7}';
BEGIN;
DECLARE c1 NO SCROLL CURSOR FOR SELECT id FROM mg WHERE a && '{3,7}';
CREATE TEMP TABLE got (id int);
DO $$
DECLARE c refcursor := 'c1'; v int;
BEGIN
  FOR i IN 1..3 LOOP
    FOR j IN 1..(i * 50) LOOP
      FETCH c INTO v;
      EXIT WHEN NOT FOUND;
      INSERT INTO got VALUES (v);
    END LOOP;
  END LOOP;
  LOOP
    FETCH c INTO v;
    EXIT WHEN NOT FOUND;
    INSERT INTO got VALUES (v);
  END LOOP;
END $$;
SELECT (SELECT array_agg(id ORDER BY id) FROM got) = gs_run('mg', NULL, 'a && ''{3,7}''') AS cursor_ok,
       (SELECT count(*) FROM got) AS n;
COMMIT;

RESET enable_seqscan;
SET enable_indexscan = off;
-- The scan read in both directions by a direct caller of the index AM: it
-- turns around in place, and one that ran off an end restarts from it.
SELECT bark_multikey_reverse('mg_bark', '@>(anyarray,anyarray)', '{3}'::int4[], 5);
SELECT w[1:4] = (SELECT array_agg(ctid::text ORDER BY ctid) FROM
                 (SELECT ctid FROM mg WHERE a @> '{3,7}' ORDER BY ctid LIMIT 4) s) AS first_ok,
       w[5:7] = ARRAY[w[3], w[2], w[1]] AND w[8:9] = '{-,-}' AND w[10] = w[1] AS turn_ok
FROM string_to_array(bark_multikey_walk('mg_bark', '@>(anyarray,anyarray)', '{3,7}'::int4[],
                                        'ffffbbbbbf'), ' ') w;
SELECT bark_multikey_walk('mg_bark', '@>(anyarray,anyarray)', '{3,7}'::int4[], 'bb') AS walk,
       (SELECT string_agg(ctid::text, ' ' ORDER BY ctid DESC) FROM
          (SELECT ctid FROM mg WHERE a @> '{3,7}' ORDER BY ctid DESC LIMIT 2) s) AS seq;
SELECT bark_multikey_walk('mg_bark', '@>(anyarray,anyarray)', '{100,200}'::int4[], 'fbf');
-- The TIDs are collected with no leaf pinned, which only an MVCC snapshot
-- makes safe, and the scan has no order: a caller asking otherwise gets an
-- error.
SELECT bark_multikey_walk('mg_bark', '@>(anyarray,anyarray)', '{3}'::int4[], 'af');
SELECT bark_multikey_walk('mg_bark', '@>(anyarray,anyarray)', '{3}'::int4[], 'of');
SELECT bark_multikey_parallel('mg_bark', '@>(anyarray,anyarray)', '{3}'::int4[]);

RESET enable_indexscan;
SET enable_seqscan = off;
-- Mark and restore keep a place in the result, as a merge join uses them.
SELECT m, r, bark_multikey_mark_restore('mg_bark', '@>(anyarray,anyarray)', '{3}'::int4[], m, r)
FROM (VALUES (0, 0), (0, 5), (3, 4), (10, 1000000)) v(m, r);

-- SELECT ... LIMIT 1 FOR UPDATE, as DocumentDB updates by a filter.
EXPLAIN (COSTS OFF) SELECT id FROM mg WHERE a @> '{3,7}' LIMIT 1 FOR UPDATE;
SELECT (SELECT id FROM mg WHERE a @> '{3,7}' LIMIT 1 FOR UPDATE) =
       (gs_run('mg', NULL, 'a @> ''{3,7}''', 1))[1] AS for_update_ok;

-- A nested loop with the index scan as its parameterized inner side: a
-- rescan with new keys for every outer row, a NULL among them.
CREATE TABLE mg_outer (k int, q int4[]);
INSERT INTO mg_outer VALUES (1, '{3}'), (2, '{3,7}'), (3, '{100}'), (4, NULL),
  (5, '{}'), (6, '{0,29}'), (7, '{3}'), (8, '{11,12}');
ANALYZE mg_outer;
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_material = off;
EXPLAIN (COSTS OFF) SELECT k, id FROM mg_outer JOIN mg ON mg.a @> mg_outer.q;
CREATE TABLE nl AS SELECT k, id FROM mg_outer JOIN mg ON mg.a @> mg_outer.q;
RESET enable_seqscan;
SET enable_indexscan = off;
SELECT (SELECT array_agg(k * 10000 + id ORDER BY k, id) FROM nl) =
       (SELECT array_agg(k * 10000 + id ORDER BY k, id)
          FROM mg_outer JOIN mg ON mg.a @> mg_outer.q) AS nestloop_ok,
       (SELECT count(*) FROM nl) AS n;
RESET enable_indexscan;
RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_material;
RESET enable_bitmapscan;

-- The planner's choice: the index scan's whole cost is startup cost, so a
-- LIMIT does not make it look cheaper, and LIMIT 1 here takes a seqscan
-- that stops at its first match.
ANALYZE gm;
EXPLAIN (COSTS OFF) SELECT * FROM gm WHERE a @> '{199}' LIMIT 1;
EXPLAIN (COSTS OFF) SELECT * FROM gm WHERE a @> '{199}' LIMIT 1 FOR UPDATE;
SELECT (SELECT id FROM gm WHERE a @> '{199}' LIMIT 1) =
       (gs_run('gm', NULL, 'a @> ''{199}''', 1))[1] AS limit_ok;

DROP TABLE mg, mg_q, gm, gm_q, gt, gt_q, mg_outer, nl;
DROP FUNCTION gs_run(text, text, text, int), gs_check(text, text, text[]);
DROP OPERATOR FAMILY bark_gin_tsvector_ops USING bark;
DROP EXTENSION bark_multikey;
