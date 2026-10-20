--
-- BARK as the implementation of btree indexes: with
-- default_btree_implementation = bark, an index created USING btree keeps
-- btree's operator classes and semantics but is stored and searched by
-- BARK's code (pg_index.indimplam).  Each case checks the implementation,
-- compares the index's answers with a sequential scan, and checks the index
-- with bark_index_check(heapallindexed).
--
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS amcheck;
CREATE EXTENSION IF NOT EXISTS pageinspect;
CREATE EXTENSION IF NOT EXISTS pgstattuple;
RESET client_min_messages;

-- The implementation of an index, '-' for its own AM.
CREATE FUNCTION implam(idx regclass) RETURNS name
LANGUAGE sql AS $$
  SELECT coalesce((SELECT amname FROM pg_am WHERE oid = i.indimplam), '-')
  FROM pg_index i WHERE i.indexrelid = idx
$$;

-- Every index of a table, its AM, implementation, and amcheck's verdict.
CREATE FUNCTION checked(tab regclass)
RETURNS TABLE (idx name, am name, impl name, ok bool)
LANGUAGE plpgsql AS $$
DECLARE r record;
BEGIN
  FOR r IN SELECT c.oid, c.relname, a.amname
           FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
           JOIN pg_am a ON a.oid = c.relam
           WHERE i.indrelid = tab ORDER BY c.relname
  LOOP
    idx := r.relname; am := r.amname; impl := implam(r.oid);
    PERFORM bark_index_check(r.oid, true);
    ok := true;
    RETURN NEXT;
  END LOOP;
END $$;

-- The rows of query q (an int-valued select list, as text) under a forced
-- plan: 'seq', 'index', 'ios' or 'bitmap'.  Returns them in their order.
CREATE FUNCTION run(q text, how text) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE r text;
BEGIN
  PERFORM set_config('enable_seqscan', (how = 'seq')::text, true);
  PERFORM set_config('enable_indexscan', (how = 'index')::text, true);
  PERFORM set_config('enable_indexonlyscan', (how IN ('ios', 'index'))::text, true);
  PERFORM set_config('enable_bitmapscan', (how = 'bitmap')::text, true);
  PERFORM set_config('enable_sort', (how = 'seq')::text, true);
  EXECUTE format('SELECT string_agg(x::text, '','' ORDER BY n) FROM '
                 '(SELECT row_number() OVER () n, x FROM (%s) s(x)) w', q)
    INTO r;
  RETURN r;
END $$;
-- Does each forced plan agree with the sequential scan, row for row for an
-- ordered query, or as a multiset (ordered = false)?
CREATE FUNCTION agrees(q text, ordered bool DEFAULT true,
                       hows text[] DEFAULT '{index,ios,bitmap}')
RETURNS TABLE (how text, same bool)
LANGUAGE plpgsql AS $$
DECLARE
  sq text := CASE WHEN ordered THEN q
             ELSE format('SELECT x FROM (%s) s(x) ORDER BY x::text', q) END;
  seq text := run(sq, 'seq');
BEGIN
  FOREACH how IN ARRAY hows LOOP
    IF ordered THEN
      same := run(q, how) IS NOT DISTINCT FROM seq;
    ELSE
      same := (SELECT string_agg(v, ',' ORDER BY v)
               FROM unnest(string_to_array(run(q, how), ',')) v)
              IS NOT DISTINCT FROM seq;
    END IF;
    RETURN NEXT;
  END LOOP;
END $$;
-- The plan's node types and index names, to show which index served it.
CREATE FUNCTION plan_of(q text) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE r text; l text;
BEGIN
  r := '';
  FOR l IN EXECUTE 'EXPLAIN (COSTS OFF) ' || q LOOP
    r := r || l || E'\n';
  END LOOP;
  RETURN r;
END $$;

SET default_btree_implementation = bark;

-- The setting picks BARK; the index is a btree index in every catalog
-- answer but its implementation.
CREATE TABLE t (id int, a int, b text, c int, d numeric);
INSERT INTO t SELECT g, g % 500, 'v' || (g % 1000), g, round((g % 7) / 3.0, g % 3)
FROM generate_series(1, 20000) g;
UPDATE t SET a = NULL WHERE id % 97 = 0;
UPDATE t SET b = NULL WHERE id % 89 = 0;
CREATE INDEX t_a ON t (a);
CREATE INDEX t_ab ON t (a, b);
CREATE INDEX t_b_desc ON t (b DESC NULLS FIRST, a);
CREATE UNIQUE INDEX t_c ON t (c) INCLUDE (b);
CREATE INDEX t_expr ON t ((a * 2 + 1));
CREATE INDEX t_part ON t (c) WHERE a < 50;
CREATE INDEX t_d ON t (d);
ALTER TABLE t ADD PRIMARY KEY (id);
SELECT * FROM checked('t');
SELECT pg_get_indexdef('t_ab'::regclass);
\d t_ab
SELECT amname FROM pg_am a JOIN pg_class c ON c.relam = a.oid
WHERE c.relname = 't_a';
-- Every btree index property; the implementation's routine answers
-- index-level ones, as BARK's do.
SELECT p, pg_index_has_property('t_a', p) AS idx,
       pg_index_column_has_property('t_a', 1, p) AS col
FROM unnest('{clusterable,index_scan,bitmap_scan,backward_scan,asc,desc,nulls_first,nulls_last,orderable,distance_orderable,returnable,search_array,search_nulls}'::text[]) p;
-- nbtree's tools refuse it; BARK's read it.
SELECT bt_index_check('t_a');
SELECT bt_index_parent_check('t_a');
SELECT bt_metap('t_a');
SELECT pgstatindex('t_a');
SELECT bark_index_parent_check('t_a', true);
SELECT magic, root > 0 AS has_root FROM bark_metap('t_a');
SELECT count(*) > 0 AS has_items FROM bark_page_items('t_a', 1);
SELECT leaf_pages > 0 AS has_leaves FROM pgstatbarkindex('t_a');
ANALYZE t;

-- Equality, ranges, NULLs, IN lists (SAOP), on each kind of scan.
SELECT * FROM agrees('SELECT id FROM t WHERE a = 7 ORDER BY a, id', false);
SELECT * FROM agrees('SELECT a FROM t WHERE a BETWEEN 10 AND 20 ORDER BY a');
SELECT * FROM agrees('SELECT a FROM t WHERE a > 480 ORDER BY a DESC');
SELECT * FROM agrees('SELECT id FROM t WHERE a IS NULL', false);
SELECT * FROM agrees('SELECT id FROM t WHERE a IS NOT NULL AND a < 3', false);
SELECT * FROM agrees('SELECT a FROM t WHERE a = ANY (''{3,77,499,600}'') ORDER BY a');
SELECT * FROM agrees('SELECT a FROM t WHERE a = ANY (''{3,77,NULL}'') ORDER BY a DESC');
SELECT * FROM agrees('SELECT a FROM t WHERE a < ANY (''{3,5}'') ORDER BY a');
-- Multicolumn, with a condition on the second column only (skip scan).
SELECT * FROM agrees('SELECT a FROM t WHERE b = ''v77'' ORDER BY a', true, '{index,ios}');
SELECT plan_of('SELECT a FROM t WHERE b = ''v77'' ORDER BY a') ~ 't_ab|t_b_desc' AS uses_index;
SELECT * FROM agrees('SELECT a FROM t WHERE a BETWEEN 5 AND 9 AND b > ''v8'' ORDER BY a, b', true, '{index,ios}');
-- DESC NULLS FIRST, forward and backward.
SELECT * FROM agrees('SELECT c FROM t WHERE b < ''v2'' OR b IS NULL ORDER BY b DESC NULLS FIRST, a, c', true, '{index}');
SELECT * FROM agrees('SELECT c FROM t WHERE b < ''v2'' OR b IS NULL ORDER BY b ASC NULLS LAST, a DESC, c', true, '{index}');
-- INCLUDE, expression and partial indexes.
SELECT * FROM agrees('SELECT c FROM t WHERE c BETWEEN 100 AND 140 ORDER BY c');
SELECT * FROM agrees('SELECT id FROM t WHERE a * 2 + 1 = 15', false);
SELECT * FROM agrees('SELECT c FROM t WHERE a < 50 AND c < 3000 ORDER BY c');
SELECT plan_of('SELECT c FROM t WHERE a < 50 AND c < 3000 ORDER BY c') ~ 't_part' AS uses_partial;
-- numeric has no equalimage: 1, 1.0 and 1.00 are equal but stored
-- differently, so their entries do not coalesce, and an index-only scan
-- returns each row's own image.
SELECT count(DISTINCT d::text) FROM t WHERE d = 1;
SELECT * FROM agrees('SELECT d::text FROM t WHERE d = 1', false, '{ios,index}');
SELECT plan_of('SELECT d::text FROM t WHERE d = 1') ~ 'Index Only Scan using t_d' AS ios;
-- Backward scans and scroll cursors.
BEGIN;
SET LOCAL enable_seqscan = off;
SET LOCAL enable_bitmapscan = off;
DECLARE cur SCROLL CURSOR FOR SELECT a FROM t WHERE a BETWEEN 10 AND 12 ORDER BY a;
FETCH FORWARD 5 FROM cur;
FETCH BACKWARD 3 FROM cur;
FETCH LAST FROM cur;
FETCH ABSOLUTE 41 FROM cur;
FETCH RELATIVE -40 FROM cur;
COMMIT;
-- Mark and restore: a merge join over duplicates on both sides.
CREATE TABLE u (a int);
INSERT INTO u SELECT g % 50 FROM generate_series(1, 300) g;
CREATE INDEX u_a ON u (a);
ANALYZE u;
BEGIN;
SET LOCAL enable_hashjoin = off;
SET LOCAL enable_nestloop = off;
SET LOCAL enable_sort = off;
SET LOCAL enable_seqscan = off;
SET LOCAL enable_bitmapscan = off;
SELECT plan_of('SELECT count(*) FROM t JOIN u USING (a) WHERE t.a < 50') ~ 'Merge Join' AS merge;
SELECT count(*) FROM t JOIN u USING (a) WHERE t.a < 50;
COMMIT;
SELECT count(*) FROM t JOIN u USING (a) WHERE t.a < 50;
-- WHERE CURRENT OF through an index scan.
BEGIN;
SET LOCAL enable_seqscan = off;
SET LOCAL enable_bitmapscan = off;
DECLARE wc CURSOR FOR SELECT id, c FROM t WHERE c BETWEEN 5000 AND 5005 ORDER BY c FOR UPDATE;
FETCH 3 FROM wc;
UPDATE t SET b = 'current' WHERE CURRENT OF wc;
COMMIT;
SELECT c FROM t WHERE b = 'current';
SELECT * FROM checked('t');

-- KNN through BARK's <~> operators, which live in btree's integer_ops
-- family: a capability of the implementation.  The nbtree-implemented index
-- on the same column gets a Sort.
CREATE TABLE k (a int, b int);
INSERT INTO k SELECT (g * 7919) % 1000, g FROM generate_series(1, 5000) g;
INSERT INTO k VALUES (NULL, 0), (NULL, -1);
CREATE INDEX k_bark ON k (a);
RESET default_btree_implementation;
CREATE TABLE kn (a int, b int);
INSERT INTO kn SELECT * FROM k;
CREATE INDEX kn_btree ON kn (a);
SET default_btree_implementation = bark;
ANALYZE k, kn;
SELECT implam('k_bark'), implam('kn_btree');
SET enable_seqscan = off;
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF) SELECT a FROM k ORDER BY a <~> 500 LIMIT 5;
EXPLAIN (COSTS OFF) SELECT a FROM kn ORDER BY a <~> 500 LIMIT 5;
RESET enable_seqscan;
RESET enable_bitmapscan;
-- Distances, with ties broken by b, then the NULLs last; LIMIT and the
-- whole result.
CREATE FUNCTION knn(tab text, lim int) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE r text;
BEGIN
  EXECUTE format('SELECT string_agg(coalesce(d::text, ''-''), '','') FROM '
                 '(SELECT a <~> 500 AS d FROM %I ORDER BY a <~> 500 LIMIT %s) s',
                 tab, lim) INTO r;
  RETURN r;
END $$;
CREATE FUNCTION knn_agrees(lim int) RETURNS bool
LANGUAGE plpgsql AS $$
DECLARE i text; s text;
BEGIN
  SET LOCAL enable_seqscan = off;
  SET LOCAL enable_bitmapscan = off;
  i := knn('k', lim);
  SET LOCAL enable_seqscan = on;
  SET LOCAL enable_indexscan = off;
  SET LOCAL enable_indexonlyscan = off;
  s := knn('k', lim);
  RETURN i = s;
END $$;
SELECT knn_agrees(1), knn_agrees(17), knn_agrees(5002);
SELECT knn('k', 9);
-- The rows tied at one distance are the same rows, whatever their order.
SELECT array_agg(a ORDER BY a) FROM (SELECT a FROM k ORDER BY a <~> 500 LIMIT 15) s;
-- Backward: a scroll cursor over a KNN scan materializes.
BEGIN;
SET LOCAL enable_seqscan = off;
SET LOCAL enable_bitmapscan = off;
DECLARE kc SCROLL CURSOR FOR SELECT a <~> 500 FROM k ORDER BY a <~> 500 LIMIT 6;
FETCH 6 FROM kc;
FETCH BACKWARD 3 FROM kc;
COMMIT;
SELECT * FROM checked('k');

-- Unique, primary key, deferrable unique, NULLS NOT DISTINCT, ON CONFLICT.
CREATE TABLE uq (id int PRIMARY KEY, v int UNIQUE, w int,
                 x int UNIQUE DEFERRABLE INITIALLY DEFERRED,
                 n int, CONSTRAINT uq_n UNIQUE NULLS NOT DISTINCT (n));
INSERT INTO uq SELECT g, g, g, g, g FROM generate_series(1, 1000) g;
SELECT * FROM checked('uq');
INSERT INTO uq VALUES (1, 0, 0, 0, 0);
INSERT INTO uq VALUES (0, 1, 0, 0, 0);
INSERT INTO uq VALUES (0, 0, 0, 0, 1);
INSERT INTO uq VALUES (0, 0, 0, 0, NULL);
INSERT INTO uq VALUES (-1, -1, 0, -1, NULL);
-- deferred: a transient duplicate is fine, a lasting one fails at COMMIT.
BEGIN;
UPDATE uq SET x = x + 1 WHERE id <= 1000;
COMMIT;
BEGIN;
UPDATE uq SET x = 5 WHERE id = 10;
COMMIT;
SELECT min(x), max(x) FROM uq;
-- ON CONFLICT on the primary key, on v, and on a unique index inferred.
INSERT INTO uq VALUES (5, 2000, 0, 2000, 2000)
  ON CONFLICT (id) DO UPDATE SET w = uq.w + 100;
INSERT INTO uq VALUES (2001, 6, 0, 2001, 2001)
  ON CONFLICT (v) DO NOTHING;
INSERT INTO uq VALUES (2002, 2002, 0, 2002, 2002), (7, 7, 0, 7, 7)
  ON CONFLICT ON CONSTRAINT uq_pkey DO UPDATE SET w = -excluded.w - 1;
SELECT id, v, w FROM uq WHERE id IN (5, 7, 2001, 2002) ORDER BY id;
SELECT * FROM agrees('SELECT v FROM uq WHERE v BETWEEN 1 AND 30 ORDER BY v');
SELECT * FROM checked('uq');

-- Foreign keys: the RI fast path probes the referenced BARK index.
CREATE TABLE fk (id int PRIMARY KEY, ref int REFERENCES uq (v), refn int);
INSERT INTO fk SELECT g, g, g FROM generate_series(1, 500) g;
INSERT INTO fk VALUES (501, 99999, 0);
DELETE FROM uq WHERE v = 3;
UPDATE uq SET v = v + 10000 WHERE v = 4;
DELETE FROM uq WHERE v = 2000;
SELECT count(*) FROM fk WHERE NOT EXISTS (SELECT 1 FROM uq WHERE uq.v = fk.ref);
-- composite, ON DELETE CASCADE
CREATE TABLE pk2 (a int, b int, PRIMARY KEY (a, b));
INSERT INTO pk2 SELECT g % 10, g FROM generate_series(1, 200) g;
CREATE TABLE fk2 (a int, b int, FOREIGN KEY (a, b) REFERENCES pk2 ON DELETE CASCADE);
INSERT INTO fk2 SELECT a, b FROM pk2;
INSERT INTO fk2 VALUES (1, 2);
DELETE FROM pk2 WHERE a = 3;
SELECT count(*), count(*) FILTER (WHERE a = 3) FROM fk2;
SELECT * FROM checked('pk2');

-- Exclusion constraint with btree's = operator.
CREATE TABLE ex (a int, b int, EXCLUDE USING btree (a WITH =));
INSERT INTO ex SELECT g, g FROM generate_series(1, 100) g;
INSERT INTO ex VALUES (5, 0);
SELECT * FROM checked('ex');

-- deduplicate_items: on (default), equal keys share LIST and POSTING
-- entries; off, every row is a SINGLE, also after ALTER INDEX for new rows.
CREATE TABLE dd (a int);
INSERT INTO dd SELECT g % 10 FROM generate_series(1, 2000) g;
CREATE INDEX dd_on ON dd (a);
CREATE INDEX dd_off ON dd (a)
  WITH (deduplicate_items = off, vacuum_cleanup_index_scale_factor = 0.1);
CREATE FUNCTION shapes(idx regclass) RETURNS TABLE (shape text, n bigint, tids bigint)
LANGUAGE sql AS $$
  SELECT i.shape, count(*), sum(i.ntids)
  FROM generate_series(1, pg_relation_size(idx) / 8192 - 1) b,
       bark_page_items(idx::text, b) i
  WHERE i.shape <> 'PIVOT' GROUP BY 1 ORDER BY 1
$$;
SELECT * FROM shapes('dd_on');
SELECT * FROM shapes('dd_off');
SELECT reloptions FROM pg_class WHERE relname = 'dd_off';
ALTER INDEX dd_on SET (deduplicate_items = off);
INSERT INTO dd SELECT g % 10 FROM generate_series(1, 100) g;
SELECT * FROM shapes('dd_on');
SELECT * FROM shapes('dd_off');
ALTER INDEX dd_off SET (deduplicate_items = on);
REINDEX INDEX dd_off;
SELECT * FROM shapes('dd_off');
SELECT * FROM agrees('SELECT a FROM dd WHERE a IN (3, 4) ORDER BY a');
SELECT * FROM checked('dd');
-- BARK's own parameters, and btree's range checks.
CREATE INDEX dd_pc ON dd (a) WITH (prefix_compression = on, fillfactor = 50);
CREATE INDEX dd_bad ON dd (a) WITH (fillfactor = 5);
CREATE INDEX dd_bad ON dd (a) WITH (deduplicate_items = 3);
CREATE INDEX dd_bad ON dd (a) WITH (no_such_option = 1);

-- Parallel CREATE INDEX and parallel index scans.
CREATE TABLE par (a int, b int) WITH (parallel_workers = 2);
INSERT INTO par SELECT g % 3000, g FROM generate_series(1, 120000) g;
SET max_parallel_maintenance_workers = 2;
SET maintenance_work_mem = '64MB';
CREATE INDEX par_a ON par (a);
RESET max_parallel_maintenance_workers;
RESET maintenance_work_mem;
ANALYZE par;
BEGIN;
SET LOCAL parallel_setup_cost = 0;
SET LOCAL parallel_tuple_cost = 0;
SET LOCAL min_parallel_index_scan_size = 0;
SET LOCAL min_parallel_table_scan_size = 0;
SET LOCAL enable_seqscan = off;
SET LOCAL enable_bitmapscan = off;
SET LOCAL max_parallel_workers_per_gather = 2;
SELECT plan_of('SELECT count(*), sum(b) FROM par WHERE a BETWEEN 100 AND 2000') ~ 'Parallel Index' AS parallel;
SELECT count(*), sum(b) FROM par WHERE a BETWEEN 100 AND 2000;
SELECT count(*), sum(a) FROM par WHERE a = ANY ('{5,500,2999}');
COMMIT;
SELECT count(*), sum(b) FROM par WHERE a BETWEEN 100 AND 2000;
SELECT count(*), sum(a) FROM par WHERE a = ANY ('{5,500,2999}');
SELECT * FROM checked('par');

-- CIC and REINDEX CONCURRENTLY keep the implementation.
CREATE INDEX CONCURRENTLY t_cic ON t (c, a);
REINDEX INDEX CONCURRENTLY t_a;
REINDEX TABLE CONCURRENTLY uq;
REINDEX TABLE t;
SELECT * FROM checked('t');
SELECT * FROM checked('uq');
SELECT * FROM agrees('SELECT c FROM t WHERE c BETWEEN 7000 AND 7100 AND a < 100 ORDER BY c, a', true, '{index,ios}');

-- An nbtree index whose setting is reset keeps nbtree; a BARK one stays
-- BARK whatever the setting says now.
RESET default_btree_implementation;
REINDEX TABLE t;
ALTER TABLE t ALTER COLUMN c TYPE bigint;
SELECT idx, impl FROM checked('t') WHERE impl <> 'bark';
SELECT implam('kn_btree');
SET default_btree_implementation = bark;

-- REPACK and CLUSTER on a BARK-implemented index, by sorting and by an
-- index scan, give a correctly ordered table.
CREATE TABLE rp (a int, b text);
INSERT INTO rp SELECT (g * 7919) % 10007, md5(g::text) FROM generate_series(1, 10000) g;
CREATE INDEX rp_a ON rp (a);
CREATE INDEX rp_b ON rp (b DESC);
-- Is the table stored in col's order (ascending, or descending when desc)?
CREATE FUNCTION physically_ordered(tab text, col text, descending bool DEFAULT false)
RETURNS bool LANGUAGE plpgsql AS $$
DECLARE r bool;
BEGIN
  EXECUTE format('SELECT coalesce(bool_and(CASE WHEN %L THEN prev >= v ELSE prev <= v END), true) '
                 'FROM (SELECT %I AS v, lag(%I) OVER (ORDER BY ctid) AS prev FROM %I) s '
                 'WHERE prev IS NOT NULL', descending, col, col, tab) INTO r;
  RETURN r;
END $$;
SELECT physically_ordered('rp', 'a') AS before;
\set VERBOSITY terse
SET client_min_messages = info;
SET enable_indexscan = off;
REPACK (VERBOSE) rp USING INDEX rp_a;
RESET enable_indexscan;
RESET client_min_messages;
SELECT physically_ordered('rp', 'a') AS sorted;
SET client_min_messages = info;
SET enable_sort = off;
REPACK (VERBOSE) rp USING INDEX rp_b;
RESET enable_sort;
RESET client_min_messages;
\set VERBOSITY default
SELECT physically_ordered('rp', 'b', true) AS scanned;
CLUSTER rp USING rp_a;
SELECT physically_ordered('rp', 'a') AS clustered;
SELECT count(*), count(DISTINCT a) FROM rp;
SELECT * FROM checked('rp');

-- VACUUM after deletes; inserts reuse the freed space.
DELETE FROM t WHERE id % 3 = 0;
VACUUM t;
SELECT * FROM agrees('SELECT a FROM t WHERE a BETWEEN 100 AND 110 ORDER BY a');
INSERT INTO t SELECT g, g % 500, 'w' || g, g, 1 FROM generate_series(30000, 31000) g;
SELECT * FROM checked('t');

-- Partitions: each partition's index is BARK's; the partitioned index has
-- none.
CREATE TABLE pt (a int, b int) PARTITION BY RANGE (a);
CREATE TABLE pt1 PARTITION OF pt FOR VALUES FROM (0) TO (1000);
CREATE TABLE pt2 PARTITION OF pt FOR VALUES FROM (1000) TO (2000);
CREATE INDEX pt_b ON pt (b);
ALTER TABLE pt ADD PRIMARY KEY (a);
INSERT INTO pt SELECT g, g % 77 FROM generate_series(0, 1999) g;
SELECT c.relname, c.relkind, implam(c.oid) FROM pg_class c
WHERE c.relname ~ '^pt[0-9]?_' ORDER BY 1;
SELECT * FROM agrees('SELECT a FROM pt WHERE b = 5 ORDER BY a', false);

-- Temporary and unlogged tables.
CREATE TEMP TABLE tmp (a int PRIMARY KEY);
INSERT INTO tmp SELECT generate_series(1, 1000);
CREATE UNLOGGED TABLE unl (a int PRIMARY KEY);
INSERT INTO unl SELECT generate_series(1, 1000);
SELECT * FROM checked('tmp');
SELECT * FROM checked('unl');
SELECT * FROM agrees('SELECT a FROM unl WHERE a > 990 ORDER BY a');

-- Catalog and TOAST indexes keep nbtree.
CREATE TABLE toasty (a int, b text);
SELECT count(*) FROM pg_index WHERE indimplam <> 0 AND indexrelid < 16384;
SELECT implam(i.indexrelid) FROM pg_index i
WHERE i.indrelid = (SELECT reltoastrelid FROM pg_class WHERE relname = 'toasty');

-- pg_amcheck-style selection skips BARK-implemented btree indexes; the
-- server's own check tells why.
SELECT bt_index_check(c.oid) FROM pg_class c WHERE c.relname = 'kn_btree';

RESET default_btree_implementation;
DROP TABLE t, u, k, kn, uq, fk, pk2, fk2, ex, dd, par, rp, pt, toasty, unl;
DROP FUNCTION implam(regclass), checked(regclass), run(text, text),
  agrees(text, bool, text[]), plan_of(text), knn(text, int), knn_agrees(int),
  shapes(regclass), physically_ordered(text, text, bool);
