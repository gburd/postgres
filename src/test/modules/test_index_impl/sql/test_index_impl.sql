--
-- Indexes created USING btree but implemented by another index AM's code.
--
CREATE EXTENSION test_index_impl;
CREATE EXTENSION amcheck;
CREATE EXTENSION pageinspect;
CREATE EXTENSION pgstattuple;

CREATE FUNCTION implam(idx regclass) RETURNS name
LANGUAGE sql AS $$
  SELECT coalesce((SELECT amname FROM pg_am WHERE oid = i.indimplam), '-')
  FROM pg_index i WHERE i.indexrelid = idx
$$;

-- The setting is validated when it can be.
SET default_btree_implementation = no_such_am;
SET default_btree_implementation = heap;
SET default_btree_implementation = btree;
CREATE TABLE plain (a int);
CREATE INDEX plain_a ON plain (a);
SELECT implam('plain_a');
RESET default_btree_implementation;

-- The setting picks the implementation; the index is still btree.
SET default_btree_implementation = test_btree_impl;
CREATE TABLE t (a int, b text, c int);
CREATE INDEX t_a ON t (a);
CREATE UNIQUE INDEX t_c ON t (c) INCLUDE (b);
ALTER TABLE t ADD PRIMARY KEY (a);
CREATE INDEX t_ab ON t (a, b);
CREATE INDEX t_b_hash ON t USING hash (b);
RESET default_btree_implementation;
SELECT c.relname, a.amname AS relam, implam(c.oid)
  FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
  JOIN pg_am a ON a.oid = c.relam
  WHERE i.indrelid = 't'::regclass ORDER BY 1;
SELECT pg_get_indexdef('t_a'::regclass);
\d t_a
\d t_pkey
\d t

-- Its code serves the index: inserts and scans go through it.
SELECT test_index_impl_reset();
INSERT INTO t SELECT g, 'v' || g, g FROM generate_series(1, 1000) g;
SELECT test_index_impl_inserts();
ANALYZE t;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT test_index_impl_reset();
SELECT b FROM t WHERE a = 42;
SELECT test_index_impl_scans() > 0 AS scanned;

-- It behaves as a btree index.
INSERT INTO t VALUES (1001, 'x', 5);
INSERT INTO t VALUES (5, 'x', 2000);
EXPLAIN (COSTS OFF) SELECT a FROM t WHERE a < 5 ORDER BY a;
SELECT a FROM t WHERE a < 5 ORDER BY a;
EXPLAIN (COSTS OFF) SELECT a FROM t WHERE a > 995 ORDER BY a DESC;
SELECT a FROM t WHERE a > 995 ORDER BY a DESC;
BEGIN;
DECLARE cur SCROLL CURSOR FOR SELECT a FROM t WHERE a BETWEEN 10 AND 12 ORDER BY a;
FETCH FORWARD 3 FROM cur;
FETCH BACKWARD 2 FROM cur;
COMMIT;
VACUUM t;
EXPLAIN (COSTS OFF) SELECT a FROM t WHERE a BETWEEN 20 AND 22;
SELECT a FROM t WHERE a BETWEEN 20 AND 22;
SET enable_indexscan = off;
SET enable_bitmapscan = on;
EXPLAIN (COSTS OFF) SELECT count(*) FROM t WHERE a < 100;
SELECT count(*) FROM t WHERE a < 100;
RESET enable_indexscan;
RESET enable_bitmapscan;
SELECT a FROM t WHERE a = ANY ('{3,7,11}') ORDER BY a;
SELECT count(*) FROM t WHERE c IS NULL;

-- Merge join with mark and restore.
CREATE TABLE u (a int);
INSERT INTO u SELECT g % 50 FROM generate_series(1, 200) g;
CREATE INDEX u_a ON u (a);
SET default_btree_implementation = test_btree_impl;
CREATE TABLE dup (a int);
INSERT INTO dup SELECT g % 50 FROM generate_series(1, 200) g;
CREATE INDEX dup_a ON dup (a);
RESET default_btree_implementation;
ANALYZE u, dup;
SET enable_hashjoin = off;
SET enable_nestloop = off;
SET enable_sort = off;
EXPLAIN (COSTS OFF) SELECT count(*) FROM u JOIN dup USING (a);
SELECT count(*) FROM u JOIN dup USING (a);
RESET enable_hashjoin;
RESET enable_nestloop;
RESET enable_sort;
RESET enable_seqscan;

-- Index and column properties come from the implementation.
SELECT p, pg_index_has_property('t_a', p)
  FROM unnest('{clusterable,index_scan,bitmap_scan,backward_scan}'::text[]) p;
SELECT pg_index_column_has_property('t_a', 1, 'returnable');

-- Tools that read nbtree pages refuse it.
SELECT bt_index_check('t_a');
SELECT bt_index_parent_check('t_a');
SELECT bt_metap('t_a');
SELECT pgstatindex('t_a');
SELECT pgstattuple('t_a');
SELECT bt_index_check('plain_a');

-- An implementation must be able to implement the index.
SET default_btree_implementation = test_hash_impl;
CREATE INDEX t_bad ON t (a);
SET default_btree_implementation = test_btree_impl_nounique;
CREATE UNIQUE INDEX t_bad ON t (a);
ALTER TABLE t ADD CONSTRAINT t_bad UNIQUE (b);
CREATE INDEX t_ok ON t (b);
SELECT implam('t_ok');
DROP INDEX t_ok;
-- Another strategy count is refused; more support procedures, and any
-- strategy numbering (amstrategies 0), are accepted.
SET default_btree_implementation = test_btree_impl_badstrat;
CREATE INDEX t_bad ON t (a);
SET default_btree_implementation = test_btree_impl_wide;
CREATE INDEX t_ok ON t (b);
SELECT implam('t_ok');
DROP INDEX t_ok;
RESET default_btree_implementation;

-- An implementation can't be dropped while indexes use it.
CREATE ACCESS METHOD test_btree_impl2 TYPE INDEX HANDLER test_btree_impl_handler;
SET default_btree_implementation = test_btree_impl2;
CREATE INDEX t_impl2 ON t (b);
RESET default_btree_implementation;
DROP ACCESS METHOD test_btree_impl2;
DROP INDEX t_impl2;
DROP ACCESS METHOD test_btree_impl2;

-- The table AM's choice comes first; without one, the setting applies.
SET default_btree_implementation = test_btree_impl_nounique;
CREATE TABLE ta (a int) USING test_impl_heap;
CREATE TABLE tn (a int) USING test_impl_heap_nopref;
CREATE INDEX ta_a ON ta (a);
CREATE INDEX tn_a ON tn (a);
CREATE UNIQUE INDEX tn_bad ON tn (a);
RESET default_btree_implementation;
SELECT implam('ta_a'), implam('tn_a');
CREATE INDEX tn_a2 ON tn (a);
SELECT implam('tn_a2');
-- test_impl_heap asks for test_hash_impl for hash indexes, which can't be.
CREATE INDEX ta_hash ON ta USING hash (a);
-- Non-btree indexes are not affected by the setting.
CREATE INDEX tn_hash ON tn USING hash (a);
SELECT implam('tn_hash');

-- Partitions choose for themselves; the partitioned index has none.
CREATE TABLE p (a int NOT NULL, b int NOT NULL, c varchar(10))
  PARTITION BY RANGE (a);
CREATE TABLE p1 PARTITION OF p FOR VALUES FROM (0) TO (100) USING test_impl_heap;
CREATE TABLE p2 PARTITION OF p FOR VALUES FROM (100) TO (200) USING heap;
SET default_btree_implementation = test_btree_impl_nounique;
CREATE INDEX p_a ON p (a);
CREATE INDEX p_c ON p (c);
RESET default_btree_implementation;
ALTER TABLE p ADD PRIMARY KEY (a, b);
CREATE TABLE p3 PARTITION OF p FOR VALUES FROM (200) TO (300) USING test_impl_heap;
-- ATTACH keeps a matching index whatever implements it, else creates one.
CREATE TABLE p4 (a int NOT NULL, b int NOT NULL, c varchar(10)) USING heap;
SET default_btree_implementation = test_btree_impl;
CREATE INDEX p4_a ON p4 (a);
RESET default_btree_implementation;
ALTER TABLE p ATTACH PARTITION p4 FOR VALUES FROM (300) TO (400);
CREATE TEMP TABLE pbefore AS
  SELECT c.relname, c.relkind, implam(c.oid) AS impl
  FROM pg_class c WHERE c.relname ~ '^p[0-9]?_' AND c.relkind IN ('i', 'I');
SELECT * FROM pbefore ORDER BY 1;
INSERT INTO p SELECT g, g, g FROM generate_series(0, 399) g;
-- Re-created partition indexes keep their own implementations.
ALTER TABLE p ALTER COLUMN c TYPE varchar(20);
SELECT b.relname, b.impl, implam(c.oid)
  FROM pbefore b LEFT JOIN pg_class c USING (relname)
  WHERE implam(c.oid) IS DISTINCT FROM b.impl;
SET enable_seqscan = off;
SELECT count(*) FROM p WHERE c IN ('50', '150', '250', '350');
RESET enable_seqscan;

-- Catalog and TOAST indexes never get one.
SET default_btree_implementation = test_btree_impl;
CREATE TABLE toasty (a int, b text);
REINDEX TABLE toasty;
SELECT count(*) FROM pg_index i
  WHERE i.indimplam <> 0 AND i.indexrelid < 16384;
SELECT implam(i.indexrelid) FROM pg_index i
  WHERE i.indrelid = (SELECT reltoastrelid FROM pg_class WHERE relname = 'toasty');
RESET default_btree_implementation;

-- Rebuilds keep the implementation.
CREATE TEMP TABLE before AS
  SELECT c.relname, implam(c.oid) AS impl FROM pg_index i
  JOIN pg_class c ON c.oid = i.indexrelid
  WHERE i.indrelid IN ('t'::regclass, 'ta'::regclass);
CREATE FUNCTION changed() RETURNS TABLE (relname name, was name, now name)
LANGUAGE sql AS $$
  SELECT b.relname, b.impl, implam(c.oid) FROM before b
  LEFT JOIN pg_class c ON c.relname = b.relname
  WHERE implam(c.oid) IS DISTINCT FROM b.impl
$$;
REINDEX TABLE t;
REINDEX INDEX CONCURRENTLY t_a;
REINDEX TABLE CONCURRENTLY ta;
SELECT * FROM changed();
CLUSTER t USING t_a;
VACUUM FULL t;
TRUNCATE t;
SELECT * FROM changed();
-- ALTER COLUMN TYPE re-creates indexes; they keep their implementation,
-- whatever the setting now says, also when they keep their storage.
SET default_btree_implementation = test_btree_impl_nounique;
ALTER TABLE t ALTER COLUMN b TYPE varchar(100);
ALTER TABLE t ALTER COLUMN c TYPE bigint;
ALTER TABLE t ALTER COLUMN b TYPE varchar(200);
RESET default_btree_implementation;
SELECT * FROM changed();
-- Changing the table AM rebuilds indexes but keeps their implementation.
ALTER TABLE ta SET ACCESS METHOD heap;
ALTER TABLE t SET ACCESS METHOD test_impl_heap_nopref;
SELECT * FROM changed();
INSERT INTO t SELECT g, 'v' || g, g FROM generate_series(1, 1000) g;

-- Implemented indexes return what a sequential scan does, and their code
-- serves the scans.
CREATE FUNCTION agrees() RETURNS TABLE (scan text, same bool, impl_scans bool)
LANGUAGE plpgsql AS $$
DECLARE
  q text := 'SELECT count(*) || '','' || sum(a) || '','' || sum(c) FROM t
             WHERE a BETWEEN 100 AND 899 AND c BETWEEN 100 AND 899';
  seq text;
  r text;
BEGIN
  SET LOCAL enable_indexscan = off;
  SET LOCAL enable_indexonlyscan = off;
  SET LOCAL enable_bitmapscan = off;
  EXECUTE q INTO seq;
  SET LOCAL enable_seqscan = off;
  SET LOCAL enable_indexscan = on;
  PERFORM test_index_impl_reset();
  EXECUTE q INTO r;
  RETURN QUERY SELECT 'index', r = seq, test_index_impl_scans() > 0;
  SET LOCAL enable_indexscan = off;
  SET LOCAL enable_bitmapscan = on;
  PERFORM test_index_impl_reset();
  EXECUTE q INTO r;
  RETURN QUERY SELECT 'bitmap', r = seq, test_index_impl_scans() > 0;
END $$;
SELECT * FROM agrees();

-- REPACK on an implemented index, by sorting and by scanning it.  Terse,
-- to leave out the timing DETAIL.
\set VERBOSITY terse
SET client_min_messages = info;
SET enable_indexscan = off;
REPACK (VERBOSE) t USING INDEX t_a;
RESET enable_indexscan;
SET enable_sort = off;
REPACK (VERBOSE) t USING INDEX t_a;
RESET enable_sort;
RESET client_min_messages;
\set VERBOSITY default
SELECT count(*), sum(a) FROM t;
SELECT * FROM agrees();
SELECT * FROM changed();

-- One operator class serves indexes whose implementations number different
-- support procedures: nbtree's 6 and test_btree_impl_wide's 8.  The
-- relcache keeps an operator class's procedures once per backend, so load
-- it in both orders, each in a fresh backend, with a class no other index
-- uses, and rebuild the relcache entries after.
CREATE OPERATOR FAMILY tii_int4_ops USING btree;
CREATE OPERATOR CLASS tii_int4_ops FOR TYPE int4 USING btree
  FAMILY tii_int4_ops AS
    OPERATOR 1 <, OPERATOR 2 <=, OPERATOR 3 =, OPERATOR 4 >=, OPERATOR 5 >,
    FUNCTION 1 btint4cmp(int4, int4);
CREATE TABLE onarrow (a int);
CREATE TABLE owide (a int);
INSERT INTO onarrow SELECT g % 100 FROM generate_series(1, 1000) g;
INSERT INTO owide SELECT g % 100 FROM generate_series(1, 1000) g;
CREATE INDEX onarrow_a ON onarrow (a tii_int4_ops);
SET default_btree_implementation = test_btree_impl_wide;
CREATE INDEX owide_a ON owide (a tii_int4_ops);
RESET default_btree_implementation;
SELECT implam('onarrow_a'), implam('owide_a');
ANALYZE onarrow, owide;
CREATE FUNCTION ocounts() RETURNS TABLE (narrow bigint, wide bigint)
LANGUAGE plpgsql AS $$
BEGIN
  SET LOCAL enable_seqscan = off;
  SET LOCAL enable_bitmapscan = off;
  RETURN QUERY SELECT (SELECT count(*) FROM onarrow WHERE a BETWEEN 10 AND 19),
                      (SELECT count(*) FROM owide WHERE a BETWEEN 10 AND 19);
END $$;
\c
SET enable_seqscan = off;
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF) SELECT count(*) FROM onarrow WHERE a BETWEEN 10 AND 19;
SELECT count(*) FROM onarrow WHERE a BETWEEN 10 AND 19;
SELECT count(*) FROM owide WHERE a BETWEEN 10 AND 19;
RESET enable_seqscan;
RESET enable_bitmapscan;
ALTER INDEX owide_a SET (fillfactor = 80);
ALTER INDEX onarrow_a SET (fillfactor = 80);
SELECT * FROM ocounts();
\c
SET enable_seqscan = off;
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF) SELECT count(*) FROM owide WHERE a BETWEEN 10 AND 19;
SELECT count(*) FROM owide WHERE a BETWEEN 10 AND 19;
SELECT count(*) FROM onarrow WHERE a BETWEEN 10 AND 19;
RESET enable_seqscan;
RESET enable_bitmapscan;
ALTER INDEX onarrow_a RESET (fillfactor);
ALTER INDEX owide_a RESET (fillfactor);
REINDEX TABLE owide;
SELECT * FROM ocounts();
SELECT implam('owide_a');
-- A support procedure numbered past nbtree's range is still an error for
-- the nbtree index, whichever index loaded the class first; the wider
-- implementation accepts it.  SQL cannot add one to a btree family, so the
-- catalog is edited directly.
CREATE FUNCTION ocount(tab text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE r bigint;
BEGIN
  EXECUTE format('SELECT count(*) FROM %I WHERE a = 5', tab) INTO r;
  RETURN r::text;
EXCEPTION WHEN OTHERS THEN
  RETURN regexp_replace(SQLERRM, '[0-9]+$', 'N');
END $$;
INSERT INTO pg_amproc
  SELECT (SELECT max(oid::int8) + 1 FROM pg_amproc)::oid, f.oid, 'int4'::regtype,
         'int4'::regtype, 7, 'btint4cmp'::regproc
  FROM pg_opfamily f WHERE f.opfname = 'tii_int4_ops';
\c
SELECT ocount('owide') AS wide, ocount('onarrow') AS narrow;
\c
SELECT ocount('onarrow') AS narrow, ocount('owide') AS wide;
DELETE FROM pg_amproc USING pg_opfamily f
  WHERE amprocfamily = f.oid AND f.opfname = 'tii_int4_ops' AND amprocnum = 7;
\c
SELECT ocount('onarrow') AS narrow, ocount('owide') AS wide;
