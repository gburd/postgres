-- Scans of BARK indexes with an extracted column.  Every query below is
-- answered by a BARK index and compared with a sequential scan and, where
-- GIN's array_ops has the operator, with a GIN index over the same rows:
-- the count, the number of distinct rows and an md5 of their ids must all
-- agree, so a row returned twice, a row missing or a wrong row shows up.
CREATE EXTENSION bark_multikey;

-- The same rows in several tables, each with one index, so each query can
-- be forced onto one index.  NULL arrays, empty arrays, NULL elements and
-- repeated elements are among them.
CREATE FUNCTION mk_rows() RETURNS TABLE (id int, tenant int, a int4[])
LANGUAGE sql AS $$
  SELECT g, g % 7,
    CASE WHEN g % 50 = 0 THEN NULL
         WHEN g % 51 = 0 THEN '{}'::int4[]
         WHEN g % 53 = 0 THEN '{NULL,3}'::int4[]
         WHEN g % 59 = 0 THEN '{7,7,NULL}'::int4[]
         ELSE ARRAY(SELECT (g * 37 + i * i * 11) % 300
                    FROM generate_series(1, 1 + g % 9) i) END
  FROM generate_series(1, 20000) g
$$;
CREATE TABLE mk_seq AS SELECT * FROM mk_rows();
CREATE TABLE mk_gin AS SELECT * FROM mk_rows();
CREATE INDEX ON mk_gin USING gin (a);
CREATE TABLE mk_b1 AS SELECT * FROM mk_rows();
CREATE INDEX mk_b1_a ON mk_b1 USING bark (a bark_int4_array_ops);
CREATE TABLE mk_b2 AS SELECT * FROM mk_rows();
CREATE INDEX mk_b2_ta ON mk_b2 USING bark (tenant, a bark_int4_array_ops);
-- A DESC NULLS LAST extracted column after the fixed one, with the range
-- class, whose boundaries are ranges as well as points.
CREATE TABLE mk_bd AS SELECT * FROM mk_rows();
CREATE INDEX mk_bd_ta ON mk_bd USING bark
  (tenant, a bark_int4_array_range_ops DESC NULLS LAST);
-- A DESC extracted column, whose NULL entries sort first.
CREATE TABLE mk_bn AS SELECT * FROM mk_rows();
CREATE INDEX mk_bn_ta ON mk_bn USING bark (tenant, a bark_int4_array_ops DESC);
CREATE TABLE mk_rn AS SELECT * FROM mk_rows();
CREATE INDEX mk_rn_a ON mk_rn USING bark (a bark_int4_array_range_ops NULLS FIRST);
CREATE TABLE mk_r1 AS SELECT * FROM mk_rows();
CREATE INDEX mk_r1_a ON mk_r1 USING bark (a bark_int4_array_range_ops);
-- The rows again, inserted rather than built.
CREATE TABLE mk_ins (id int, tenant int, a int4[]);
CREATE INDEX mk_ins_ta ON mk_ins USING bark (tenant, a bark_int4_array_ops);
INSERT INTO mk_ins SELECT * FROM mk_rows();
ANALYZE mk_seq, mk_gin, mk_b1, mk_b2, mk_bd, mk_bn, mk_rn, mk_r1, mk_ins;
SELECT bark_index_check('mk_b1_a'), bark_index_check('mk_b2_ta'),
       bark_index_check('mk_bd_ta'), bark_index_check('mk_ins_ta');

-- mk_run(table, qual, mode): count, distinct ids and md5 of the ids of the
-- rows of table that satisfy qual, read by a sequential scan ('seq'), an
-- index scan ('index') or a bitmap scan ('bitmap').  'wrong plan' when the
-- plan does not read the way asked.
CREATE FUNCTION mk_run(tbl text, qual text, mode text) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  sql text := format('SELECT count(*) || '':'' || count(DISTINCT id) || '':'' ||
                        coalesce(md5(string_agg(id::text, '','' ORDER BY id)), ''-'')
                      FROM (SELECT id FROM %I WHERE %s) s', tbl, qual);
  plan text;
  r text;
BEGIN
  PERFORM set_config('enable_seqscan', (mode = 'seq')::text, true);
  PERFORM set_config('enable_indexscan', (mode = 'index')::text, true);
  PERFORM set_config('enable_indexonlyscan', 'off', true);
  PERFORM set_config('enable_bitmapscan', (mode = 'bitmap')::text, true);
  FOR plan IN EXECUTE 'EXPLAIN (COSTS OFF) ' || sql LOOP
    IF (mode = 'seq' AND plan LIKE '%Seq Scan%') OR
       (mode = 'index' AND plan LIKE '%Index Scan%') OR
       (mode = 'bitmap' AND plan LIKE '%Bitmap Index Scan%') THEN
      EXECUTE sql INTO r;
      RETURN r;
    END IF;
  END LOOP;
  RETURN 'wrong plan';
END $$;

-- mk_compare(qual, tables): run qual on mk_seq and on each table, by index
-- and bitmap scans; show the sequential scan's answer, and every answer
-- that differs from it (NULL when none does).
CREATE FUNCTION mk_compare(qual text, tables text[], OUT seq text,
                           OUT mismatches text)
LANGUAGE plpgsql AS $$
DECLARE
  t text;
  m text;
  r text;
BEGIN
  seq := mk_run('mk_seq', qual, 'seq');
  FOREACH t IN ARRAY tables LOOP
    FOREACH m IN ARRAY CASE WHEN t = 'mk_gin' THEN ARRAY['bitmap']
                            ELSE ARRAY['index', 'bitmap'] END LOOP
      r := mk_run(t, qual, m);
      IF r IS DISTINCT FROM seq THEN
        mismatches := concat_ws(' ', mismatches, t || '/' || m || '=' || r);
      END IF;
    END LOOP;
  END LOOP;
END $$;

-- Operators GIN's array_ops has, on the extracted column alone and after
-- an equality (or IN-list) on the fixed one; empty queries, which read the
-- NULL entries (searchnulls); two keys on the column (a row may satisfy
-- them through different keys); IN-lists of arrays; IS [NOT] NULL.
CREATE TABLE mk_quals (n serial, q text, gin bool DEFAULT true);
INSERT INTO mk_quals (q) VALUES
  ('a && ''{1,5,77}'''), ('a && ''{2}'''), ('a && ''{}'''),
  ('a && ''{NULL,3}'''), ('a @> ''{28,80}'''), ('a @> ''{40}'''),
  ('a @> ''{}'''), ('a @> ''{NULL}'''), ('a <@ ''{1,2,3,4,5,6,7,8,9,10,11}'''),
  ('a <@ ''{}'''), ('a <@ ''{3,NULL}'''), ('a = ''{44}'''), ('a = ''{3,NULL}'''), ('a = ''{}'''),
  ('a = ''{7,7,NULL}'''), ('a = ''{213,180}'''),
  ('a && ''{198}'' AND a && ''{234}'''), ('a && ''{1,2}'' AND a @> ''{2}'''),
  ('a IN (''{44}''::int4[], ''{3}'', ''{213,180}'')'),
  ('tenant = 3 AND a && ''{1,5,77}'''), ('tenant = 3 AND a @> ''{9}'''),
  ('tenant = 3 AND a <@ ''{1,2,3,4,5,6,7,8,9,10,11}'''),
  ('tenant = 6 AND a = ''{}'''), ('tenant = 0 AND a @> ''{}'''),
  ('tenant IN (2, 4) AND a && ''{10,20,30}'''),
  ('tenant IN (1, 5) AND a IN (''{44}''::int4[], ''{110}'', ''{180,213}'')'),
  ('tenant = 2 AND a && ''{198}'' AND a && ''{234}'''),
  ('tenant > 4 AND a && ''{1,5,77}'''), ('tenant < 2 AND a <@ ''{}''');
INSERT INTO mk_quals (q, gin) VALUES
  ('a IS NULL', false), ('a IS NOT NULL', false),
  ('tenant = 4 AND a IS NULL', false), ('tenant = 4 AND a IS NOT NULL', false),
  ('tenant = 5', false), ('tenant IN (1, 6)', false), ('tenant >= 5', false),
  ('a && ''{1,5,77}'' AND id < 5000', false);
SELECT n, q, (mk_compare(q, CASE WHEN gin
    THEN '{mk_gin,mk_b1,mk_b2,mk_bn,mk_ins}'::text[]
    WHEN q LIKE '%a %' THEN '{mk_b1,mk_b2,mk_bn,mk_ins}'
    ELSE '{mk_b2,mk_bn,mk_ins}' END)).*
  FROM mk_quals ORDER BY n;

-- The range class: boundaries that are half-bounded, strict, closed below
-- and open above, unions of ranges (a ScalarArrayOp), and points that
-- procedure 9 accepts or drops, ascending and descending.
CREATE TABLE mk_rquals (n serial, q text);
INSERT INTO mk_rquals (q) VALUES
  ('a |>| 295'), ('a |<=| 3'), ('a |><| ''{100,103}'''), ('a |><| ''{100,100}'''),
  ('a |><| ''{5,NULL}'''), ('a |><| ''{103,100}'''), ('a |%| ''{1,2,3,4,100,101}'''), ('a |=| 7'),
  ('a |=| ANY (ARRAY[5, 9, 299, 9, NULL])'),
  ('a |=| ANY (ARRAY[]::int4[])'), ('a |=| ANY (NULL::int4[])'),
  ('a |>| ANY (ARRAY[5, 3, 100])'), ('a |<=| ANY (ARRAY[NULL::int4])'),
  ('a |<=| ANY (ARRAY[3, 10])'), ('a |>| ANY (ARRAY[290, 295])'),
  ('a |>| 290 AND a |<=| 20'), ('a |=| 198 AND a && ''{234}'''),
  ('tenant = 2 AND a |>| 280'), ('tenant = 2 AND a |<=| 10'),
  ('tenant = 2 AND a |=| 7'), ('tenant IN (1, 3) AND a |><| ''{200,260}'''),
  ('tenant = 4 AND a |%| ''{2,3,4,5,6,7,8}'''),
  ('tenant IN (0, 4) AND a |=| ANY (ARRAY[299, 1, 150])'),
  ('tenant = 1 AND a |<=| ANY (ARRAY[3, 10])'),
  ('tenant = 3 AND a |>| 290 AND a && ''{1,2,3}'''),
  ('a |?| 10'), ('tenant = 2 AND a |?| 40'), ('a |<>| 7'), ('tenant = 2 AND a |<>| 100'),
  ('a |~| 1000'), ('a |~| 1001'), ('a |~| 1002'), ('a |~| 1003'),
  ('tenant = 5 AND a |~| 503'), ('a |~| ANY (ARRAY[1000, 1040, 1063])'),
  ('a |~| ANY (ARRAY[1003, 1043])'), ('a |~| ANY (ARRAY[1001, 1040])'),
  ('a |~| ANY (ARRAY[1002, 1040])'), ('a |~| ANY (ARRAY[1003, 1030, 1050])'),
  ('a |~| ANY (ARRAY[1000, 1010])'), ('a |>| ANY (ARRAY[5, 3])'),
  ('a |<=| ANY (ARRAY[5, 3])'), ('a |?| ANY (ARRAY[5, 3])'),
  ('a |=| ANY (ARRAY[3, 5]) AND a |=| 7'),
  ('tenant IN (2, 3) AND a |?| ANY (ARRAY[30, 3])'),
  ('a |~| 1004'), ('a |~| ANY (ARRAY[1004, 1050])'),
  ('a |~| ANY (ARRAY[1000, 1003])'), ('a |~| ANY (ARRAY[1002, 1001])');
SELECT n, q, (mk_compare(q, '{mk_r1,mk_rn,mk_bd}')).* FROM mk_rquals ORDER BY n;

-- NULLs in the fixed column form a group of their own.
CREATE TABLE mk_nulls (id int, tenant int, a int4[]);
INSERT INTO mk_nulls SELECT g, CASE WHEN g % 3 = 0 THEN NULL ELSE g % 4 END,
  ARRAY[g % 10, g % 7, g % 5] FROM generate_series(1, 3000) g;
CREATE INDEX mk_nulls_ta ON mk_nulls USING bark (tenant, a bark_int4_array_ops);
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*), count(DISTINCT id), md5(string_agg(id::text, ',' ORDER BY id))
  FROM mk_nulls WHERE a && '{1,2,3}';
SELECT count(*), count(DISTINCT id), md5(string_agg(id::text, ',' ORDER BY id))
  FROM mk_nulls WHERE tenant IS NULL AND a && '{1,2,3}';
RESET enable_seqscan;
RESET enable_bitmapscan;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT count(*), count(DISTINCT id), md5(string_agg(id::text, ',' ORDER BY id))
  FROM mk_nulls WHERE a && '{1,2,3}';
SELECT count(*), count(DISTINCT id), md5(string_agg(id::text, ',' ORDER BY id))
  FROM mk_nulls WHERE tenant IS NULL AND a && '{1,2,3}';
RESET enable_indexscan;
RESET enable_bitmapscan;

-- A procedure 8 declared with six arguments, as pg_extended_btree's are,
-- gets no BarkQueryFlags; a class without procedure 8 can be scanned for
-- NULLs only; one whose procedure 8 asks for a recheck needs procedure 9.
CREATE FUNCTION mk_query6(int4[], int2, internal, internal, internal, internal)
  RETURNS void AS '$libdir/bark_multikey', 'bark_multikey_extract_query'
  LANGUAGE C IMMUTABLE STRICT;
CREATE OPERATOR FAMILY mk_six_ops USING bark;
CREATE OPERATOR CLASS mk_six_ops FOR TYPE int4[] USING bark
  FAMILY mk_six_ops AS
    OPERATOR 1 && (anyarray, anyarray),
    OPERATOR 2 @> (anyarray, anyarray),
    FUNCTION 1 (int4[], int4[]) btint4cmp(int4, int4),
    FUNCTION 7 bark_multikey_extract_value(int4[], internal, internal),
    FUNCTION 8 mk_query6(int4[], int2, internal, internal, internal, internal),
    STORAGE int4;
CREATE OPERATOR FAMILY mk_noquery_ops USING bark;
CREATE OPERATOR CLASS mk_noquery_ops FOR TYPE int4[] USING bark
  FAMILY mk_noquery_ops AS
    OPERATOR 1 && (anyarray, anyarray),
    FUNCTION 1 (int4[], int4[]) btint4cmp(int4, int4),
    FUNCTION 7 bark_multikey_extract_value(int4[], internal, internal),
    STORAGE int4;
CREATE TABLE mk_six AS SELECT * FROM mk_rows() WHERE id <= 2000;
CREATE INDEX mk_six_a ON mk_six USING bark (a mk_six_ops);
CREATE TABLE mk_noquery AS SELECT * FROM mk_rows() WHERE id <= 2000;
CREATE INDEX mk_noquery_a ON mk_noquery USING bark (a mk_noquery_ops);
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*), md5(string_agg(id::text, ',' ORDER BY id))
  FROM mk_six WHERE a && '{1,5,77}';
SELECT count(*), md5(string_agg(id::text, ',' ORDER BY id))
  FROM mk_noquery WHERE a IS NULL;
SELECT count(*) FROM mk_noquery WHERE a && '{1}';
SELECT count(*) FROM mk_six WHERE a @> '{1}';
RESET enable_seqscan;
RESET enable_bitmapscan;
SELECT count(*), md5(string_agg(id::text, ',' ORDER BY id))
  FROM mk_seq WHERE id <= 2000 AND a && '{1,5,77}';
SELECT count(*), md5(string_agg(id::text, ',' ORDER BY id))
  FROM mk_seq WHERE id <= 2000 AND a IS NULL;
DROP TABLE mk_six, mk_noquery;
DROP OPERATOR CLASS mk_six_ops USING bark CASCADE;
DROP OPERATOR CLASS mk_noquery_ops USING bark CASCADE;
DROP FUNCTION mk_query6(int4[], int2, internal, internal, internal, internal);

-- A row whose keys meet the query several times is returned once, by an
-- index scan, with and without a fixed column before the extracted one.
SET enable_seqscan = off;
SET enable_bitmapscan = off;
CREATE TABLE mk_once (id int, tenant int, a int4[]);
INSERT INTO mk_once VALUES (1, 1, '{1,2,3,4,5}'), (2, 1, '{5,4,3}'),
  (3, 2, '{1,1,1}'), (4, 1, '{6}'), (5, 1, NULL), (6, 1, '{}');
CREATE INDEX mk_once_a ON mk_once USING bark (a bark_int4_array_ops);
CREATE INDEX mk_once_ta ON mk_once USING bark (tenant, a bark_int4_array_ops);
EXPLAIN (COSTS OFF) SELECT id FROM mk_once WHERE a && '{1,2,3,4,5}';
SELECT id FROM mk_once WHERE a && '{1,2,3,4,5}' ORDER BY id;
DROP INDEX mk_once_a;
EXPLAIN (COSTS OFF) SELECT id FROM mk_once WHERE tenant = 1 AND a && '{1,2,3,4,5}';
SELECT id FROM mk_once WHERE tenant = 1 AND a && '{1,2,3,4,5}' ORDER BY id;
SELECT id FROM mk_once WHERE tenant = 1 ORDER BY id;
SELECT id FROM mk_once WHERE a <@ '{1,2,3,4,5}' ORDER BY id;
RESET enable_seqscan;
RESET enable_bitmapscan;

-- LIMIT, and a forward cursor read in pieces.
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*), count(DISTINCT id)
  FROM (SELECT id FROM mk_b1 WHERE a && '{1,5,77}' LIMIT 150) s;
SELECT count(*), count(DISTINCT id)
  FROM (SELECT id FROM mk_b2 WHERE tenant IN (1, 2) AND a && '{1,5,77}' LIMIT 90) s;
CREATE FUNCTION mk_cursor(qual text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  c refcursor;
  r record;
  ids int[] := '{}';
BEGIN
  OPEN c NO SCROLL FOR EXECUTE 'SELECT id FROM mk_b2 WHERE ' || qual;
  FOR i IN 1 .. 10 LOOP
    FETCH c INTO r;
    ids := ids || r.id;
  END LOOP;
  MOVE FORWARD 200 IN c;
  LOOP
    FETCH c INTO r;
    EXIT WHEN NOT FOUND;
    ids := ids || r.id;
  END LOOP;
  CLOSE c;
  RETURN cardinality(ids) || ':' ||
    (SELECT count(DISTINCT x) FROM unnest(ids) x);
END $$;
SELECT mk_cursor('a && ''{1,5,77}''');
SELECT mk_cursor('tenant IN (1, 3, 5) AND a && ''{1,5,77}''');
RESET enable_seqscan;
SELECT mk_run('mk_seq', 'a && ''{1,5,77}''', 'seq'),
       mk_run('mk_seq', 'tenant IN (1, 3, 5) AND a && ''{1,5,77}''', 'seq');
SET enable_seqscan = off;
-- The rows a scan with a seen set returned would be dropped if it walked
-- back over them, so an index with an extracted column has no backward
-- scans (backward_scan is false, from its operator classes, whatever the
-- meta page says), and a scroll cursor over it reads through a Material
-- node, as over a KNN scan.  A scalar BARK index scans backward itself.
CREATE TABLE mk_prop (id int, tenant int, a int4[], t text);
CREATE INDEX mk_prop_it ON mk_prop USING bark (id DESC, t NULLS FIRST)
  INCLUDE (tenant);
CREATE INDEX mk_prop_bt ON mk_prop USING btree (id DESC, t NULLS FIRST)
  INCLUDE (tenant);
CREATE INDEX mk_prop_ta ON mk_prop USING bark
  (tenant DESC NULLS LAST, a bark_int4_array_ops) INCLUDE (t);
CREATE INDEX mk_prop_a ON mk_prop USING bark (a bark_int4_array_ops DESC, id);
CREATE INDEX mk_prop_tai ON mk_prop USING bark
  (tenant, a bark_int4_array_ops NULLS FIRST, id DESC);
CREATE INDEX mk_prop_t ON mk_prop USING bark (t);
SELECT i::regclass, pg_index_has_property(i, 'backward_scan') AS backward,
       pg_index_has_property(i, 'index_scan') AS index_scan,
       pg_index_has_property(i, 'bitmap_scan') AS bitmap_scan
  FROM unnest('{mk_prop_it,mk_prop_ta,mk_prop_a,mk_prop_tai,mk_b1_a}'::regclass[]) i;
-- Column properties.  The extracted column and every key column after it
-- have no order the planner can use (the index orders the column's keys,
-- not its values), so they report orderable, asc, desc and the nulls
-- properties false, as for a GIN column; an INCLUDE column and the columns
-- before the extracted one get the generic answers.  The extracted column
-- is not returnable (its entries hold keys).  distance_orderable needs an
-- ordering operator on the leading column: int4's <~>, or the class's |<|
-- and |>>| on an extracted column.
CREATE FUNCTION mk_props(i regclass) RETURNS TABLE (c int, props text)
LANGUAGE sql AS $$
  SELECT c, string_agg(p || '=' ||
           coalesce(pg_index_column_has_property(i, c, p)::text, 'null'),
           ' ' ORDER BY o)
    FROM generate_series(1, (SELECT indnatts FROM pg_index
                              WHERE indexrelid = i)) c,
         unnest('{orderable,asc,desc,nulls_first,nulls_last,distance_orderable,returnable,search_array,search_nulls}'::text[])
           WITH ORDINALITY u(p, o)
   GROUP BY c
$$;
SELECT i::regclass, c, props
  FROM unnest('{mk_prop_it,mk_prop_ta,mk_prop_a,mk_prop_tai,mk_prop_t}'::regclass[]) i,
       LATERAL mk_props(i)
  ORDER BY 1, 2;
-- A scalar BARK index answers as btree does, but for distance_orderable
-- (btree has no ordering operators).
SELECT b.c, b.props AS bark, t.props AS btree
  FROM mk_props('mk_prop_it') b JOIN mk_props('mk_prop_bt') t USING (c)
  WHERE b.props IS DISTINCT FROM t.props;
DROP TABLE mk_prop;
DROP FUNCTION mk_props(regclass);
-- The order the planner reads from an index stops before the extracted
-- column: ORDER BY tenant, a sorts within each tenant on top of the index.
EXPLAIN (COSTS OFF)
SELECT id FROM mk_b2 WHERE tenant IN (1, 2) AND a && '{5,77}' ORDER BY tenant, a;
EXPLAIN (COSTS OFF)
DECLARE c SCROLL CURSOR FOR SELECT id FROM mk_b1 WHERE a && '{1,5,77}';
EXPLAIN (COSTS OFF)
DECLARE c SCROLL CURSOR FOR SELECT tenant FROM mk_b2 WHERE tenant = 3;
-- mk_scroll(qual): read a scroll cursor over mk_b1 forward 3 rows, back 1
-- (the second row again), then to the end, then back to the start; the
-- step back must repeat the second row, and both passes must return the
-- sequential scan's rows.
CREATE FUNCTION mk_scroll(qual text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  c refcursor;
  r record;
  first int[] := '{}';
  fwd int[] := '{}';
  bwd int[] := '{}';
  again int;
BEGIN
  OPEN c SCROLL FOR EXECUTE 'SELECT id FROM mk_b1 WHERE ' || qual;
  FOR i IN 1 .. 3 LOOP
    FETCH c INTO r;
    first := first || r.id;
  END LOOP;
  FETCH PRIOR FROM c INTO r;
  again := r.id;
  MOVE ABSOLUTE 0 IN c;
  LOOP
    FETCH c INTO r;
    EXIT WHEN NOT FOUND;
    fwd := fwd || r.id;
  END LOOP;
  LOOP
    FETCH PRIOR FROM c INTO r;
    EXIT WHEN NOT FOUND;
    bwd := bwd || r.id;
  END LOOP;
  CLOSE c;
  RETURN (again = first[2]) || ':' ||
    (SELECT count(*) || ':' || count(DISTINCT x) || ':' ||
            md5(string_agg(x::text, ',' ORDER BY x)) FROM unnest(fwd) x) ||
    ':' || (fwd = (SELECT array_agg(x ORDER BY o DESC)
                     FROM unnest(bwd) WITH ORDINALITY u(x, o)));
END $$;
SELECT mk_scroll('a && ''{1,5,77}''');
SELECT mk_scroll('a <@ ''{1,2,3,4,5,6,7,8,9,10,11}''');
RESET enable_seqscan;
SELECT 'true:' || mk_run('mk_seq', 'a && ''{1,5,77}''', 'seq') || ':true',
       'true:' || mk_run('mk_seq', 'a <@ ''{1,2,3,4,5,6,7,8,9,10,11}''', 'seq') ||
       ':true';
SET enable_seqscan = off;
-- A cursor not declared SCROLL is not made scrollable over such an index
-- (PostgreSQL makes it so only when the plan can run backward without a
-- Material node), so it fetches forward only, as over a GIN or KNN scan;
-- declare it SCROLL to fetch backward.  An explicit NO SCROLL cursor too.
EXPLAIN (COSTS OFF)
DECLARE c CURSOR FOR SELECT id FROM mk_b1 WHERE a && '{1,5,77}';
BEGIN;
DECLARE c NO SCROLL CURSOR FOR SELECT id FROM mk_b1 WHERE a && '{1,5,77}';
FETCH 2 FROM c;
FETCH BACKWARD 1 FROM c;
ROLLBACK;
BEGIN;
DECLARE c CURSOR FOR SELECT id FROM mk_b1 WHERE a && '{1,5,77}';
FETCH 3 FROM c;
FETCH BACKWARD 1 FROM c;
ROLLBACK;
-- A caller of the index AM that does not ask, and changes direction, gets
-- an error from a scan that keeps a seen set, never wrong rows; a scan for
-- one point keeps none and steps back.
SELECT bark_multikey_reverse('mk_b1_a', '&&(anyarray,anyarray)',
                             '{1,5,77}'::int4[], 3);
SELECT bark_multikey_reverse('mk_b1_a', '&&(anyarray,anyarray)',
                             '{5}'::int4[], 3);
-- Likewise a direct parallel scan: one that needs the set is refused,
-- never answered with a row twice; a scan for one point needs none.
SELECT bark_multikey_parallel('mk_b1_a', '&&(anyarray,anyarray)',
                              '{1,5,77}'::int4[]);
SELECT bark_multikey_parallel('mk_b1_a', '&&(anyarray,anyarray)',
                              '{5}'::int4[]);
-- A scan that starts backward is fine.
EXPLAIN (COSTS OFF)
SELECT id FROM mk_b2 WHERE tenant IN (1, 3) AND a && '{1,5,77}'
  ORDER BY tenant DESC;
SELECT count(*), count(DISTINCT id), md5(string_agg(id::text, ',' ORDER BY id)),
       bool_and(tenant <= prev) AS descending
FROM (SELECT id, tenant, lag(tenant, 1, 7) OVER () AS prev
        FROM (SELECT id, tenant FROM mk_b2
               WHERE tenant IN (1, 3) AND a && '{1,5,77}'
               ORDER BY tenant DESC) s) s;
SELECT mk_run('mk_seq', 'tenant IN (1, 3) AND a && ''{1,5,77}''', 'seq');
SELECT count(*), count(DISTINCT id), bool_and(tenant <= prev) AS descending
FROM (SELECT id, tenant, lag(tenant, 1, 7) OVER () AS prev
        FROM (SELECT id, tenant FROM mk_b2 ORDER BY tenant DESC) s) s;
RESET enable_seqscan;
RESET enable_bitmapscan;

-- A merge join reads the index scan again from a mark: the rows after the
-- mark come back after a restore, though they are in the seen set.
SET enable_hashjoin = off;
SET enable_nestloop = off;
SET enable_sort = off;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
CREATE TABLE mk_outer (tenant int, k int);
INSERT INTO mk_outer SELECT g % 7, g FROM generate_series(1, 30) g;
CREATE INDEX ON mk_outer (tenant);
EXPLAIN (COSTS OFF)
SELECT count(*) FROM mk_outer o JOIN mk_b2 t ON t.tenant = o.tenant
  WHERE t.a && '{1,2,3,4,5,6,7,8,9,10,11,12}';
SELECT count(*), md5(string_agg(o.k || '/' || t.id, ',' ORDER BY o.k, t.id))
  FROM mk_outer o JOIN mk_b2 t ON t.tenant = o.tenant
  WHERE t.a && '{1,2,3,4,5,6,7,8,9,10,11,12}';
SELECT count(*), md5(string_agg(o.k || '/' || t.id, ',' ORDER BY o.k, t.id))
  FROM mk_outer o JOIN mk_b2 t ON t.tenant = o.tenant
  WHERE t.tenant >= 0;
RESET enable_hashjoin;
RESET enable_nestloop;
RESET enable_sort;
RESET enable_seqscan;
RESET enable_bitmapscan;
SELECT count(*), md5(string_agg(o.k || '/' || t.id, ',' ORDER BY o.k, t.id))
  FROM mk_outer o JOIN mk_seq t ON t.tenant = o.tenant
  WHERE t.a && '{1,2,3,4,5,6,7,8,9,10,11,12}';
SELECT count(*), md5(string_agg(o.k || '/' || t.id, ',' ORDER BY o.k, t.id))
  FROM mk_outer o JOIN mk_seq t ON t.tenant = o.tenant;

-- Restoring a mark taken before the first row restarts the scan with an
-- empty seen set; one taken later takes out of the set only the rows
-- returned after it, on the marked page or on pages read since.
SELECT m, r, bark_multikey_mark_restore('mk_b1_a', '&&(anyarray,anyarray)',
                                        '{1,5,77}'::int4[], m, r)
  FROM (VALUES (0, 0), (0, 500), (3, 2), (10, 600), (500, 300), (900, 100))
       AS v(m, r);
SELECT bark_multikey_mark_restore('mk_b1_a', '<@(anyarray,anyarray)',
                                  '{1,2,3,4,5,6,7,8,9,10,11}'::int4[], 100, 200);

-- A key on a column after the extracted one: inside a point boundary its
-- bound positions the scan and ends each run of the point; inside a range
-- it only filters.
CREATE TABLE mk_3 AS SELECT * FROM mk_rows();
CREATE INDEX mk_3_tai ON mk_3 USING bark
  (tenant, a bark_int4_array_range_ops, id);
SELECT q, (mk_compare(q, '{mk_3}')).* FROM (VALUES
  ('tenant = 1 AND a && ''{5,77,150}'' AND id < 9000'),
  ('tenant = 1 AND a && ''{5,77,150}'' AND id > 9000'),
  ('tenant IN (1, 2) AND a |=| ANY (ARRAY[5, 77]) AND id BETWEEN 3000 AND 6000'),
  ('tenant = 1 AND a |><| ''{5,40}'' AND id < 9000'),
  ('tenant = 3 AND a |>| 250 AND id > 15000')) AS v(q);

-- Backward over ranges, strict ends and the NULL entries, on an ascending
-- and a descending extracted column, and with a key after it.
CREATE FUNCTION mk_backward(tbl text, qual text) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  r text;
  plan text;
  sql text := format('SELECT count(*) || '':'' || count(DISTINCT id) || '':'' ||
                        coalesce(md5(string_agg(id::text, '','' ORDER BY id)), ''-'')
                      FROM (SELECT id FROM %I WHERE %s ORDER BY tenant DESC) s',
                     tbl, qual);
BEGIN
  PERFORM set_config('enable_sort', 'off', true);
  PERFORM set_config('enable_seqscan', 'off', true);
  PERFORM set_config('enable_indexscan', 'on', true);
  PERFORM set_config('enable_bitmapscan', 'off', true);
  PERFORM set_config('enable_indexonlyscan', 'off', true);
  FOR plan IN EXECUTE 'EXPLAIN (COSTS OFF) ' || sql LOOP
    IF plan LIKE '%Index Scan Backward%' THEN
      EXECUTE sql INTO r;
      RETURN r;
    END IF;
  END LOOP;
  RETURN 'wrong plan';
END $$;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = off;
SELECT q, t, mk_backward(t, q) = mk_run('mk_seq', q, 'seq') AS same
FROM (VALUES ('tenant IN (1, 3) AND a |><| ''{100,160}'''),
             ('tenant IN (1, 3) AND a |?| 20'),
             ('tenant IN (2, 5) AND a |<>| 150'),
             ('tenant IN (2, 5) AND a |~| ANY (ARRAY[1003, 1043, 1004])'),
             ('tenant IN (4, 5) AND a |>| 280'),
             ('tenant IN (4, 5) AND a |<=| 5'),
             ('tenant IN (0, 6) AND a IS NULL'),
             ('tenant IN (0, 6) AND a IS NOT NULL')) AS v(q),
     (VALUES ('mk_bd'), ('mk_3'), ('mk_bn')) AS w(t)
WHERE t <> 'mk_bn' OR q !~ '\|'
ORDER BY t, q;
SELECT mk_backward('mk_3', 'tenant IN (1, 2) AND a |=| ANY (ARRAY[5, 77]) AND id BETWEEN 3000 AND 6000')
  = mk_run('mk_seq', 'tenant IN (1, 2) AND a |=| ANY (ARRAY[5, 77]) AND id BETWEEN 3000 AND 6000', 'seq')
  AS same;
RESET enable_seqscan;
RESET enable_bitmapscan;
RESET enable_indexonlyscan;

-- Entries too large for a page (OVERSIZED, read from their overflow chain)
-- are filtered, rechecked and dropped as repeats like any other.
CREATE TABLE mk_big (id int, a int4[], t text);
INSERT INTO mk_big SELECT g, ARRAY[g % 5, g % 7, (g + 1) % 5],
    CASE WHEN g % 3 = 0 THEN repeat(md5(g::text), 100) ELSE 'small' END
  FROM generate_series(1, 300) g;
CREATE INDEX mk_big_at ON mk_big USING bark (a bark_int4_array_range_ops, t);
CREATE INDEX mk_big_ida ON mk_big USING bark (id, a bark_int4_array_range_ops, t);
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*), count(DISTINCT id), md5(string_agg(id::text, ',' ORDER BY id))
  FROM mk_big WHERE a && '{1,2,3}';
SELECT count(*), count(DISTINCT id), md5(string_agg(id::text, ',' ORDER BY id))
  FROM mk_big WHERE a |%| '{1,2,3,4}';
RESET enable_seqscan;
RESET enable_bitmapscan;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT count(*), count(DISTINCT id), md5(string_agg(id::text, ',' ORDER BY id))
  FROM mk_big WHERE a && '{1,2,3}';
SELECT count(*), count(DISTINCT id), md5(string_agg(id::text, ',' ORDER BY id))
  FROM mk_big WHERE a |%| '{1,2,3,4}';
RESET enable_indexscan;
RESET enable_bitmapscan;
-- ... and by a KNN scan on the fixed leading column.
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_sort = off;
SELECT count(*), count(DISTINCT id), md5(string_agg(id::text, ',' ORDER BY id))
  FROM (SELECT id FROM mk_big WHERE a |%| '{1,2,3,4}' ORDER BY id <~> 150) s;
RESET enable_seqscan;
RESET enable_bitmapscan;
RESET enable_sort;

-- An empty index.
CREATE TABLE mk_empty (a int4[]);
CREATE INDEX mk_empty_a ON mk_empty USING bark (a bark_int4_array_ops);
SET enable_seqscan = off;
SELECT count(*) FROM mk_empty WHERE a && '{1}';
RESET enable_seqscan;

-- A bitmap scan of the multikey index ANDed with another index's.
CREATE INDEX mk_b1_id ON mk_b1 (id);
SET enable_seqscan = off;
SET enable_indexscan = off;
EXPLAIN (COSTS OFF)
SELECT count(*) FROM mk_b1 WHERE a && '{1,5,77}' AND id BETWEEN 1000 AND 9000;
SELECT count(*), md5(string_agg(id::text, ',' ORDER BY id))
  FROM mk_b1 WHERE a && '{1,5,77}' AND id BETWEEN 1000 AND 9000;
RESET enable_seqscan;
RESET enable_indexscan;
SELECT count(*), md5(string_agg(id::text, ',' ORDER BY id))
  FROM mk_seq WHERE a && '{1,5,77}' AND id BETWEEN 1000 AND 9000;
DROP INDEX mk_b1_id;

-- Index-only scans return the fixed columns, each row once; the extracted
-- column holds keys, not values, so only a class with procedure 14 returns
-- it.
SET enable_seqscan = off;
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF) SELECT tenant FROM mk_b2 WHERE tenant = 3;
SELECT count(*), sum(tenant) FROM mk_b2 WHERE tenant = 3;
EXPLAIN (COSTS OFF) SELECT tenant FROM mk_b2 WHERE tenant IN (3, 5);
SELECT count(*), sum(tenant) FROM mk_b2 WHERE tenant IN (3, 5);
EXPLAIN (COSTS OFF) SELECT a FROM mk_b1 WHERE a && '{1}';
RESET enable_seqscan;
RESET enable_bitmapscan;
SELECT count(*), sum(tenant) FROM mk_seq WHERE tenant IN (3, 5);
CREATE FUNCTION mk_fetch(int4) RETURNS int4[]
  AS 'SELECT ARRAY[$1]' LANGUAGE sql IMMUTABLE STRICT;
CREATE OPERATOR FAMILY mk_single_ops USING bark;
CREATE OPERATOR CLASS mk_single_ops FOR TYPE int4[] USING bark
  FAMILY mk_single_ops AS
    OPERATOR 1 && (anyarray, anyarray),
    OPERATOR 3 <@ (anyarray, anyarray),
    FUNCTION 1 (int4[], int4[]) btint4cmp(int4, int4),
    FUNCTION 7 bark_multikey_extract_value(int4[], internal, internal),
    FUNCTION 8 bark_multikey_extract_query(int4[], int2, internal, internal,
                                           internal, internal, internal),
    FUNCTION 9 bark_multikey_recheck(int4, int2, internal),
    FUNCTION 14 mk_fetch(int4),
    STORAGE int4;
CREATE TABLE mk_single (id int, a int4[], n name);
INSERT INTO mk_single SELECT g, CASE WHEN g % 10 = 0 THEN NULL
                                     ELSE ARRAY[g % 100] END, 'n' || g % 3
  FROM generate_series(1, 5000) g;
CREATE INDEX mk_single_a ON mk_single USING bark (a mk_single_ops, n, id);
VACUUM ANALYZE mk_single;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
-- Procedure 14 is reserved and not used: an index-only scan never returns
-- the multikey column (nor serves a query whose quals name it), since the
-- NULL key stands for a NULL value, '{}' and '{NULL}' alike, and a row with
-- several keys has an entry per key.  Rows with '{}', '{NULL}' and NULL
-- (each one NULL entry, so the index stays single-key) show the values
-- come from the heap.
INSERT INTO mk_single VALUES (90001, '{}', 'n1'), (90002, '{NULL}', 'n1'),
  (90003, NULL, 'n1');
EXPLAIN (COSTS OFF) SELECT a, n, id FROM mk_single WHERE a && '{5,7}';
SELECT count(*), md5(string_agg(a::text || n || id, ',' ORDER BY id))
  FROM mk_single WHERE a && '{5,7}';
SELECT id, a FROM mk_single WHERE id > 90000 AND a IS NOT DISTINCT FROM a
  AND (a = '{}' OR a = '{NULL}' OR a IS NULL) ORDER BY id;
-- With procedure 14 rebuilding values, an index-only scan here returned {5}
-- for '{5,6}' and dropped '{}' (its NULL entry rebuilt as NULL fails the
-- recheck); both must come from the heap.
CREATE TABLE mk_fetchless (id int, a int4[]);
INSERT INTO mk_fetchless VALUES (1, '{}'), (2, '{NULL}'), (3, NULL), (4, '{5}'),
  (5, '{5,6}');
CREATE INDEX mk_fetchless_a ON mk_fetchless USING bark (a mk_single_ops, id);
VACUUM ANALYZE mk_fetchless;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF) SELECT id, a FROM mk_fetchless WHERE a <@ '{5,6}';
SELECT id, a FROM mk_fetchless WHERE a <@ '{5,6}' ORDER BY id;
RESET enable_seqscan;
RESET enable_bitmapscan;
DROP TABLE mk_fetchless;
SELECT count(*), md5(string_agg(coalesce(a::text, '-') || n || id, ',' ORDER BY id))
  FROM mk_single WHERE id < 300;
RESET enable_seqscan;
RESET enable_bitmapscan;
SELECT count(*), md5(string_agg(a::text || n || id, ',' ORDER BY id))
  FROM mk_single WHERE a && '{5,7}';
SELECT count(*), md5(string_agg(coalesce(a::text, '-') || n || id, ',' ORDER BY id))
  FROM mk_single WHERE id < 300;

-- An index that has never held a row with two entries (every array of one
-- element, NULL or empty) needs no seen set, nor does a scan for one point;
-- every other scan uses one.  CLUSTER reads the index with a non-MVCC
-- snapshot, which always uses the set, and so copies each row once.
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET client_min_messages = debug1;
SELECT count(*) FROM mk_single WHERE a && '{5,7}';
SELECT count(*) FROM mk_b1 WHERE a && '{5}';
SELECT count(*) FROM mk_b1 WHERE a && '{5,7}';
RESET client_min_messages;
SELECT multikey FROM bark_multikey_meta('mk_single_a');
RESET enable_seqscan;
RESET enable_bitmapscan;
CLUSTER mk_b2 USING mk_b2_ta;
SELECT count(*), count(DISTINCT id), md5(string_agg(id::text, ',' ORDER BY id))
  FROM mk_b2;
SELECT count(*), count(DISTINCT id), md5(string_agg(id::text, ',' ORDER BY id))
  FROM mk_seq;
SELECT n, q, (mk_compare(q, '{mk_b2}')).* FROM mk_quals
  WHERE q IN ('a && ''{1,5,77}''', 'tenant = 3 AND a && ''{1,5,77}''',
              'tenant IN (2, 4) AND a && ''{10,20,30}''')
  ORDER BY n;

-- Ordered-operator (KNN) scans on the fixed leading column keep one seen
-- set for the whole scan.
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_sort = off;
EXPLAIN (COSTS OFF)
SELECT id FROM mk_b2 WHERE a && '{1,5,77}' ORDER BY tenant <~> 3;
SELECT count(*), count(DISTINCT id), md5(string_agg(id::text, ',' ORDER BY id)),
       bool_and(d >= prev) AS ordered
FROM (SELECT id, d, lag(d, 1, 0) OVER () AS prev
        FROM (SELECT id, abs(tenant - 3) AS d FROM mk_b2
               WHERE a && '{1,5,77}' ORDER BY tenant <~> 3) s) s;
SELECT count(*), count(DISTINCT id)
  FROM (SELECT id FROM mk_b2 ORDER BY tenant <~> 3) s;
-- Index-only, and with entries procedure 9 drops.  The LIMIT takes the
-- rows of tenants 2, 3 and 4 exactly, so ties at the cut do not matter.
EXPLAIN (COSTS OFF)
SELECT tenant FROM mk_b2 ORDER BY tenant <~> 3 LIMIT 8571;
SELECT count(*), sum(tenant), bool_and(d >= prev) AS ordered
FROM (SELECT tenant, d, lag(d, 1, 0) OVER () AS prev
        FROM (SELECT tenant, abs(tenant - 3) AS d FROM mk_b2
               ORDER BY tenant <~> 3 LIMIT 8571) s) s;
SELECT count(*), sum(tenant) FROM mk_seq WHERE tenant BETWEEN 2 AND 4;
SET enable_indexonlyscan = off;
SELECT count(*), count(DISTINCT id), md5(string_agg(id::text, ',' ORDER BY id))
  FROM (SELECT id FROM mk_bd WHERE a |%| '{2,3,4,5,6,7,8}'
         ORDER BY tenant <~> 3) s;
RESET enable_indexonlyscan;
RESET enable_seqscan;
RESET enable_bitmapscan;
RESET enable_sort;
SELECT mk_run('mk_seq', 'a |%| ''{2,3,4,5,6,7,8}''', 'seq');
SELECT mk_run('mk_seq', 'a && ''{1,5,77}''', 'seq');

-- An ordering operator on the extracted column, MongoDB's sort rule for
-- arrays: |<| orders rows by their smallest element, and |>>| (sorted by
-- >>>) by their largest, descending.  A row with no element (a NULL or
-- empty array, or NULL elements only) has a NULL result and comes last.
-- The index serves both without a Sort, returning each row once, at its
-- first key, whatever the column's order and NULLS option, and a filter on
-- the column does not change the order (the executor rechecks it).
-- Compared with a Sort over unnest from a sequential scan: the sequence of
-- ORDER BY values, and the (id, value) pairs.  (enable_sort is off because
-- a selective filter would otherwise be planned as an Index Cond under a
-- Sort, which is a different plan, not the one under test.)
CREATE TABLE mk_onf AS SELECT * FROM mk_rows();
CREATE INDEX mk_onf_a ON mk_onf USING bark (a bark_int4_array_ops NULLS FIRST);
CREATE TABLE mk_od AS SELECT * FROM mk_rows();
CREATE INDEX mk_od_a ON mk_od USING bark (a bark_int4_array_ops DESC);
CREATE FUNCTION mk_order(tbl text, op text, filter text) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  q text := format('SELECT id, a %s 0 AS k FROM %I WHERE %s ORDER BY a %s 0 %s',
                   op, tbl, filter, op,
                   CASE op WHEN '|>>|' THEN 'USING >>>' ELSE '' END);
  ref text := format('SELECT id, (SELECT %s(e) FROM unnest(a) e) AS k
                        FROM mk_seq WHERE %s',
                     CASE op WHEN '|<|' THEN 'min' ELSE 'max' END, filter);
  plan text;
  sorted bool := false;
  indexed bool := false;
  r record;
  ids int[] := '{}';
  ks int[] := '{}';
  refseq text;
  refpairs text;
BEGIN
  PERFORM set_config('enable_seqscan', 'off', true);
  PERFORM set_config('enable_bitmapscan', 'off', true);
  PERFORM set_config('enable_sort', 'off', true);
  FOR plan IN EXECUTE 'EXPLAIN (COSTS OFF) ' || q LOOP
    sorted := sorted OR plan LIKE '%Sort%';
    indexed := indexed OR plan LIKE '%Index Scan using%';
  END LOOP;
  IF sorted OR NOT indexed THEN
    RETURN 'wrong plan';
  END IF;
  FOR r IN EXECUTE q LOOP
    ids := array_append(ids, r.id);
    ks := array_append(ks, r.k);
  END LOOP;
  PERFORM set_config('enable_seqscan', 'on', true);
  EXECUTE format('SELECT md5(string_agg(coalesce(k::text, ''N''), '',''
                                        ORDER BY k %s NULLS LAST)),
                         md5(string_agg(id || '':'' || coalesce(k::text, ''N''),
                                        '','' ORDER BY id))
                    FROM (%s) s',
                 CASE op WHEN '|<|' THEN 'ASC' ELSE 'DESC' END, ref)
    INTO refseq, refpairs;
  RETURN cardinality(ids) || ' rows, order ' ||
    (md5(array_to_string(ks, ',', 'N')) IS NOT DISTINCT FROM refseq) ||
    ', pairs ' ||
    ((SELECT md5(string_agg(i || ':' || coalesce(k::text, 'N'), ',' ORDER BY i))
        FROM unnest(ids, ks) u(i, k)) IS NOT DISTINCT FROM refpairs);
END $$;
SELECT t, op, f, mk_order(t, op, f)
  FROM unnest('{mk_b1,mk_onf,mk_od}'::text[]) t,
       unnest('{|<|,|>>|}'::text[]) op,
       unnest(ARRAY['true', 'a && ''{1,5,77}''',
                    'a <@ ''{1,2,3,4,5,6,7,8,9,10}''', 'a IS NOT NULL']) f
  ORDER BY 1, 2, 3;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF)
SELECT a |<| 0 FROM mk_b1 WHERE a && '{1,5,77}' ORDER BY a |<| 0 LIMIT 3;
SELECT a |<| 0 FROM mk_b1 WHERE a && '{1,5,77}' ORDER BY a |<| 0 LIMIT 3;
EXPLAIN (COSTS OFF)
SELECT a |>>| 0 FROM mk_od ORDER BY a |>>| 0 USING >>> LIMIT 3;
SELECT a |>>| 0 FROM mk_od ORDER BY a |>>| 0 USING >>> LIMIT 3;
-- A NULL argument makes every result NULL: any order, but every row once.
SET plan_cache_mode = force_generic_plan;
PREPARE mk_onull(int) AS
  SELECT count(*), count(DISTINCT id)
    FROM (SELECT id FROM mk_onf ORDER BY a |<| $1) s;
EXPLAIN (COSTS OFF) EXECUTE mk_onull(NULL);
EXECUTE mk_onull(NULL);
DEALLOCATE mk_onull;
RESET plan_cache_mode;
-- The scan reports a row's key as its ORDER BY value, so an ordering
-- operator must return the key type: |<<| returns int8, and is refused,
-- and distance_orderable says so.
CREATE TABLE mk_o8 AS SELECT * FROM mk_rows() WHERE id <= 200;
CREATE INDEX mk_o8_a ON mk_o8 USING bark (a bark_int4_array_range_ops);
SET enable_sort = off;
SELECT id FROM mk_o8 ORDER BY a |<<| 0 LIMIT 1;
RESET enable_sort;
SELECT i, pg_index_column_has_property(i, 1, 'distance_orderable')
  FROM unnest('{mk_b1_a,mk_onf_a,mk_od_a,mk_o8_a}'::regclass[]) i;
DROP TABLE mk_onf, mk_od, mk_o8;
DROP FUNCTION mk_order;

-- No parallel index scan of an index with an extracted column: the workers
-- would read a row's entries on different leaves, and could not share the
-- seen set.  The planner never offers one, even for a scan that needs no
-- set (one point), or for an index-only scan of the fixed column.  A
-- parallel bitmap heap scan is fine: one process builds the bitmap.
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_index_scan_size = 0;
SET min_parallel_table_scan_size = 0;
SET max_parallel_workers_per_gather = 2;
SET random_page_cost = 1;
ALTER TABLE mk_b1 SET (parallel_workers = 2);
ALTER TABLE mk_b2 SET (parallel_workers = 2);
EXPLAIN (COSTS OFF) SELECT count(*) FROM mk_b1 WHERE a @> '{}';
SELECT count(*) FROM mk_b1 WHERE a @> '{}';
SELECT mk_run('mk_seq', 'a @> ''{}''', 'seq');
EXPLAIN (COSTS OFF) SELECT count(*) FROM mk_b1 WHERE a && '{5}';
SELECT count(*) FROM mk_b1 WHERE a && '{5}';
SELECT mk_run('mk_seq', 'a && ''{5}''', 'seq');
EXPLAIN (COSTS OFF) SELECT count(*) FROM mk_b2 WHERE tenant > 2;
SELECT count(*) FROM mk_b2 WHERE tenant > 2;
SET enable_bitmapscan = on;
EXPLAIN (COSTS OFF) SELECT count(*) FROM mk_b1 WHERE a && '{1,5,77}';
SELECT count(*) FROM mk_b1 WHERE a && '{1,5,77}';
SELECT mk_run('mk_seq', 'a && ''{1,5,77}''', 'seq');
SELECT count(*) FROM mk_seq WHERE tenant > 2;
ALTER TABLE mk_b1 RESET (parallel_workers);
ALTER TABLE mk_b2 RESET (parallel_workers);
RESET random_page_cost;
RESET max_parallel_workers_per_gather;
RESET min_parallel_table_scan_size;
RESET min_parallel_index_scan_size;
RESET parallel_tuple_cost;
RESET parallel_setup_cost;
RESET enable_seqscan;
RESET enable_bitmapscan;

-- A broken class: boundaries of one call that intersect, a search element
-- whose strategy does not fit its side, and procedure 9 answers outside
-- 0-2, are errors.
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) FROM mk_r1 WHERE a |!| 5;
SELECT count(*) FROM mk_r1 WHERE a |!| -5;
SELECT count(*) FROM mk_r1 WHERE a |!| -500;
SELECT count(*) FROM mk_r1 WHERE a |!| 0;
RESET enable_seqscan;
RESET enable_bitmapscan;

-- Costing.  An index scan that needs the seen set is not planned when its
-- rows per group of the fixed columns, at 16 bytes each, exceed work_mem
-- times hash_mem_multiplier: at work_mem = 64 and a multiplier of 1 that is
-- 4096 rows, below a tenant's 5000.  The heap is in tenant order, so at the
-- default work_mem an index scan of one tenant wins.  Every row carries
-- 1000, so a && '{1000}' is one point that selects the whole tenant; such a
-- scan needs no seen set and keeps its index scan.  The range class makes
-- |=| ANY a ScalarArrayOp on the extracted column.  A bitmap scan builds no
-- set and takes over when the index scan is ruled out.  Each plan's answer
-- must match the sequential scan's at the end.
CREATE TABLE mk_cost AS
  SELECT g AS id, g / 5000 AS tenant, ARRAY[g % 300, (g * 7) % 300, 1000] AS a
  FROM generate_series(1, 200000) g;
CREATE INDEX mk_cost_ta ON mk_cost USING bark (tenant, a bark_int4_array_range_ops);
VACUUM ANALYZE mk_cost;
CREATE VIEW mk_cost_q AS
  SELECT 1 AS q, count(*) AS n, count(DISTINCT id) AS ndistinct, sum(id) AS total FROM mk_cost
    WHERE tenant = 3
  UNION ALL
  SELECT 2, count(*) AS n, count(DISTINCT id) AS ndistinct, sum(id) AS total FROM mk_cost
    WHERE tenant = 3 AND a && '{1,2,3}'
  UNION ALL
  SELECT 3, count(*) AS n, count(DISTINCT id) AS ndistinct, sum(id) AS total FROM mk_cost
    WHERE tenant = 3 AND a && '{1000}'
  UNION ALL
  SELECT 4, count(*) AS n, count(DISTINCT id) AS ndistinct, sum(id) AS total FROM mk_cost
    WHERE tenant = 3 AND a |=| ANY (ARRAY[1, 2, 3]);
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM mk_cost_q;
SELECT * FROM mk_cost_q;
SET work_mem = 64;
SET hash_mem_multiplier = 1;
EXPLAIN (COSTS OFF) SELECT * FROM mk_cost_q;
SET client_min_messages = debug1;
SELECT * FROM mk_cost_q;
RESET client_min_messages;
RESET enable_bitmapscan;
EXPLAIN (COSTS OFF) SELECT * FROM mk_cost_q;
SELECT * FROM mk_cost_q;
RESET work_mem;
RESET hash_mem_multiplier;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM mk_cost_q;
SELECT * FROM mk_cost_q;
RESET enable_indexscan;
RESET enable_bitmapscan;
DROP VIEW mk_cost_q;

-- An ordered scan on the extracted column reads every entry, but fetches
-- only the rows the fixed columns' quals pass.  With LIMIT it wins over a
-- sort; a filter on the extracted column alone is only rechecked, so it
-- does not make the ordered scan look cheap.  Answers match the seqscan.
CREATE INDEX mk_cost_a ON mk_cost USING bark (a bark_int4_array_ops);
EXPLAIN (COSTS OFF) SELECT id FROM mk_cost ORDER BY a |<| 0 LIMIT 5;
WITH s AS (SELECT id, a, a |<| 0 AS k FROM mk_cost ORDER BY a |<| 0 LIMIT 300)
SELECT count(*), count(DISTINCT id),
       bool_and(k = (SELECT min(e) FROM unnest(a) e)) AS exact,
       max(k) <= (SELECT k FROM (SELECT (SELECT min(e) FROM unnest(a) e) AS k
                                   FROM mk_cost ORDER BY 1 OFFSET 299 LIMIT 1) r)
         AS smallest
  FROM s;
-- Only the kind of plan is shown: the index or bitmap scan under the sort
-- depends on ANALYZE's sample.
CREATE FUNCTION mk_cost_plan(q text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  l text;
  sorted bool := false;
  ordered bool := false;
BEGIN
  FOR l IN EXECUTE 'EXPLAIN (COSTS OFF) ' || q LOOP
    sorted := sorted OR l ~ '^ *Sort$';
    ordered := ordered OR l ~ 'Order By:';
  END LOOP;
  RETURN CASE WHEN ordered THEN 'ordered index scan'
              WHEN sorted THEN 'sort' ELSE 'other' END;
END $$;
SELECT mk_cost_plan($q$SELECT id FROM mk_cost WHERE a && '{7}' ORDER BY a |<| 0$q$);
DROP FUNCTION mk_cost_plan;
DROP INDEX mk_cost_a;
DROP TABLE mk_cost;

DROP TABLE mk_3, mk_big, mk_empty;
DROP TABLE mk_seq, mk_gin, mk_b1, mk_b2, mk_bd, mk_bn, mk_rn, mk_r1, mk_ins, mk_once,
  mk_outer, mk_single, mk_quals, mk_rquals, mk_nulls;
DROP OPERATOR CLASS mk_single_ops USING bark CASCADE;
DROP FUNCTION mk_rows(), mk_run(text, text, text), mk_compare(text, text[]),
  mk_fetch(int4), mk_cursor(text), mk_backward(text, text), mk_scroll(text);
DROP EXTENSION bark_multikey;
