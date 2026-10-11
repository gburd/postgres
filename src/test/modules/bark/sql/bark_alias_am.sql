--
-- BARK under an alias access method: CREATE ACCESS METHOD ... HANDLER
-- barkhandler runs BARK's code under another name and OID.  An operator
-- family created USING the alias is one of BARK's own (a multikey family),
-- and every tool that reads a BARK index accepts the alias's indexes.
-- Each scan is compared with a sequential scan of the same query.
--
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS amcheck;
CREATE EXTENSION IF NOT EXISTS pageinspect;
CREATE EXTENSION IF NOT EXISTS pgstattuple;
CREATE EXTENSION IF NOT EXISTS bark_multikey;
RESET client_min_messages;

CREATE ACCESS METHOD bark_alias TYPE INDEX HANDLER barkhandler;

-- A scalar class in btree's integer_ops, whose operators and comparator
-- it shares, as BARK's own int4_ops does.
CREATE OPERATOR CLASS bark_alias_int4_ops DEFAULT FOR TYPE int4
  USING bark_alias FAMILY integer_ops AS STORAGE int4;

-- bark_multikey's int4[] class (see bark_multikey--1.0.sql), in a family
-- of the alias.
CREATE OPERATOR FAMILY bark_alias_int4_array_ops USING bark_alias;
CREATE OPERATOR CLASS bark_alias_int4_array_ops
FOR TYPE int4[] USING bark_alias FAMILY bark_alias_int4_array_ops AS
    OPERATOR 1 && (anyarray, anyarray),
    OPERATOR 1 |&&| (int4[], int4[]),
    OPERATOR 2 @> (anyarray, anyarray),
    OPERATOR 3 <@ (anyarray, anyarray),
    OPERATOR 4 = (anyarray, anyarray),
    OPERATOR 5 |<| (int4[], int4) FOR ORDER BY integer_ops,
    FUNCTION 1 (int4[], int4[]) btint4cmp(int4, int4),
    FUNCTION 2 btint4sortsupport(internal),
    FUNCTION 4 btequalimage(oid),
    FUNCTION 6 btint4skipsupport(internal),
    FUNCTION 7 bark_multikey_extract_value(int4[], internal, internal),
    FUNCTION 8 bark_multikey_extract_query(int4[], int2, internal, internal,
                                           internal, internal, internal),
    FUNCTION 9 bark_multikey_recheck(int4, int2, internal),
    FUNCTION 15 ("any", "any") bark_multikey_translate_cmptype(int4),
    STORAGE int4;

-- And its GIN-interface class, which has bitmap scans only.
CREATE OPERATOR FAMILY bark_alias_gin_int4_array_ops USING bark_alias;
CREATE OPERATOR CLASS bark_alias_gin_int4_array_ops
FOR TYPE int4[] USING bark_alias FAMILY bark_alias_gin_int4_array_ops AS
    OPERATOR 1 && (anyarray, anyarray),
    OPERATOR 2 @> (anyarray, anyarray),
    OPERATOR 3 <@ (anyarray, anyarray),
    OPERATOR 4 = (anyarray, anyarray),
    FUNCTION 1 (int4[], int4[]) btint4cmp(int4, int4),
    FUNCTION 4 btequalimage(oid),
    FUNCTION 7 ginarrayextract(anyarray, internal, internal),
    FUNCTION 10 ginqueryarrayextract(anyarray, internal, int2, internal,
                                     internal, internal, internal),
    FUNCTION 11 ginarrayconsistent(internal, int2, anyarray, int4, internal,
                                   internal, internal, internal),
    FUNCTION 13 ginarraytriconsistent(internal, int2, anyarray, int4,
                                      internal, internal, internal),
    STORAGE int4;

SELECT opcname, amvalidate(c.oid) FROM pg_opclass c JOIN pg_am a ON a.oid = opcmethod
  WHERE amname = 'bark_alias' ORDER BY opcname;

CREATE TABLE ba (id int, b int, a int4[], c int4[]);
INSERT INTO ba SELECT g, g % 97, ARRAY[g % 7, g % 11, g % 13 + 20],
  ARRAY[g % 7, g % 11, g % 13 + 20] FROM generate_series(1, 3000) g;
CREATE INDEX ba_b ON ba USING bark_alias (b);
CREATE INDEX ba_a ON ba USING bark_alias (a bark_alias_int4_array_ops);
CREATE INDEX ba_c ON ba USING bark_alias (c bark_alias_gin_int4_array_ops);
INSERT INTO ba SELECT g, g % 97, ARRAY[g % 7, g % 11, g % 13 + 20],
  ARRAY[g % 7, g % 11, g % 13 + 20] FROM generate_series(3001, 5000) g;
INSERT INTO ba VALUES (5001, NULL, NULL, NULL), (5002, 3, '{}', '{}');
VACUUM ANALYZE ba;

-- Run q with a sequential scan, then with the scan named by node (which
-- must appear in the plan), and compare the rows.
CREATE FUNCTION ba_cmp(q text, node text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  digest text := 'SELECT count(*) || '' rows '' || '
    'coalesce(md5(string_agg(r::text, '','' ORDER BY r::text)), '''') '
    'FROM (' || q || ') r';
  want text;
  got text;
  plan text := '';
  l text;
BEGIN
  PERFORM set_config('enable_indexscan', 'off', true);
  PERFORM set_config('enable_indexonlyscan', 'off', true);
  PERFORM set_config('enable_bitmapscan', 'off', true);
  EXECUTE digest INTO want;
  PERFORM set_config('enable_seqscan', 'off', true);
  PERFORM set_config(CASE node WHEN 'Index Only Scan' THEN 'enable_indexonlyscan'
                     WHEN 'Bitmap Index Scan' THEN 'enable_bitmapscan'
                     ELSE 'enable_indexscan' END, 'on', true);
  FOR l IN EXECUTE 'EXPLAIN (COSTS OFF) ' || q LOOP
    plan := plan || l || E'\n';
  END LOOP;
  EXECUTE digest INTO got;
  PERFORM set_config('enable_seqscan', 'on', true);
  PERFORM set_config('enable_indexscan', 'on', true);
  PERFORM set_config('enable_indexonlyscan', 'on', true);
  PERFORM set_config('enable_bitmapscan', 'on', true);
  IF position(node || ' ' IN plan) = 0 THEN
    RETURN 'no ' || node || ' in plan: ' || plan;
  END IF;
  RETURN CASE WHEN got = want THEN 'ok ' || split_part(got, ' ', 1) || ' rows'
         ELSE 'MISMATCH seqscan ' || want || ', ' || node || ' ' || got END;
END $$;

SELECT q, node, ba_cmp(q, node) FROM (VALUES
  ('SELECT id, b FROM ba WHERE b = 5', 'Index Scan'),
  ('SELECT id, b FROM ba WHERE b BETWEEN 10 AND 20', 'Index Scan'),
  ('SELECT b FROM ba WHERE b > 90', 'Index Only Scan'),
  ('SELECT id, b FROM ba WHERE b < 4 ORDER BY b DESC', 'Index Scan Backward'),
  ('SELECT id FROM ba WHERE b IN (1, 2, 3)', 'Bitmap Index Scan'),
  ('SELECT id FROM ba WHERE a @> ''{3}''', 'Index Scan'),
  ('SELECT id FROM ba WHERE a @> ''{3,25}''', 'Index Scan'),
  ('SELECT id FROM ba WHERE a && ''{5,21}''', 'Index Scan'),
  ('SELECT id FROM ba WHERE a <@ ''{0,1,2,3,4,20,21,22,23,24,25}''', 'Index Scan'),
  ('SELECT id FROM ba WHERE a = ''{1,1,21}''', 'Index Scan'),
  ('SELECT id FROM ba WHERE a |&&| ''{6,32}''', 'Index Scan'),
  ('SELECT a |<| 0 FROM ba ORDER BY a |<| 0 LIMIT 30', 'Index Scan'),
  ('SELECT id FROM ba WHERE a @> ''{3}''', 'Bitmap Index Scan'),
  ('SELECT id FROM ba WHERE a && ''{5,21}''', 'Bitmap Index Scan'),
  ('SELECT id FROM ba WHERE c @> ''{3,25}''', 'Bitmap Index Scan'),
  ('SELECT id FROM ba WHERE c && ''{5,21}''', 'Bitmap Index Scan'),
  ('SELECT id FROM ba WHERE c <@ ''{0,1,2,3,4,20,21,22,23,24,25}''', 'Bitmap Index Scan')
) v(q, node);

-- Scroll cursors: the scalar index scans back itself; the multikey index
-- cannot (its seen set), so the executor reads it through a Material node.
BEGIN;
SET LOCAL enable_seqscan = off;
SET LOCAL enable_bitmapscan = off;
EXPLAIN (COSTS OFF) SELECT id, b FROM ba WHERE b BETWEEN 3 AND 4 ORDER BY b;
DECLARE c1 SCROLL CURSOR FOR SELECT id, b FROM ba WHERE b BETWEEN 3 AND 4 ORDER BY b;
FETCH 3 FROM c1;
FETCH BACKWARD 2 FROM c1;
FETCH LAST FROM c1;
FETCH ABSOLUTE 53 FROM c1;
FETCH PRIOR FROM c1;
EXPLAIN (COSTS OFF) SELECT id FROM ba WHERE a @> '{3}';
DECLARE c2 SCROLL CURSOR FOR SELECT id FROM ba WHERE a @> '{3}';
FETCH 3 FROM c2;
FETCH BACKWARD 2 FROM c2;
MOVE FORWARD ALL FROM c2;
FETCH BACKWARD 3 FROM c2;
COMMIT;

-- The tools that read a BARK index accept the alias's.
-- (heapallindexed is for indexes without a multikey column.)
SELECT bark_index_check('ba_b', true, true);
SELECT bark_index_parent_check('ba_b', true, false);
SELECT bark_index_check('ba_a');
SELECT bark_index_parent_check('ba_c', false, false);
SELECT magic, version, level, nkeys > 0 AS has_keys FROM bark_metap('ba_a');
SELECT count(*) > 0 AS has_items FROM bark_page_items('ba_b', 1);
SELECT count(*) > 0 AS has_items FROM bark_page_items('ba_a', 1);
SELECT version, tree_level, leaf_pages > 0 AS has_leaves FROM pgstatbarkindex('ba_a');
SELECT version, tree_level, leaf_pages > 0 AS has_leaves FROM pgstatbarkindex('ba_c');
SELECT * FROM bark_multikey_meta('ba_a');

DROP TABLE ba;
DROP FUNCTION ba_cmp;
DROP ACCESS METHOD bark_alias CASCADE;
