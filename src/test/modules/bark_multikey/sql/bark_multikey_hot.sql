-- HOT updates of BARK indexes with an extracted column (barkcomparedatums):
-- an UPDATE whose new value gives the index exactly the entries the old one
-- gave is HOT, and every other is not.  After each set of updates the
-- index is checked with heapallindexed, and index and bitmap scans must
-- return the rows a sequential scan returns.  Assert-enabled builds also
-- form both values' entries for every HOT answer and compare them.  The
-- tables leave most of each page free, since the old versions cannot be
-- pruned before the transaction that counts the updates ends.
CREATE EXTENSION IF NOT EXISTS bark_multikey;

-- (updates, HOT updates) of a table so far in this transaction.
CREATE FUNCTION mh_count(rel regclass, OUT upd bigint, OUT hot bigint)
LANGUAGE sql AS $$
  SELECT pg_stat_get_xact_tuples_updated(rel),
         pg_stat_get_xact_tuples_hot_updated(rel) $$;
-- The ids of tab's rows that satisfy q, by a sequential, an index or a
-- bitmap scan; the 'index' and 'bitmap' results are NULL when the plan
-- does not read the index that way.
CREATE FUNCTION mh_run(tab text, q text, how text) RETURNS int[]
LANGUAGE plpgsql AS $$
DECLARE
  sql text := format('SELECT coalesce(array_agg(id ORDER BY id), ''{}'') FROM %I WHERE %s',
                     tab, q);
  plan text;
  r int[];
BEGIN
  PERFORM set_config('enable_seqscan', (how = 'seq')::text, true);
  PERFORM set_config('enable_indexscan', (how = 'index')::text, true);
  PERFORM set_config('enable_indexonlyscan', 'off', true);
  PERFORM set_config('enable_bitmapscan', (how = 'bitmap')::text, true);
  FOR plan IN EXECUTE 'EXPLAIN (COSTS OFF) ' || sql LOOP
    IF (how = 'seq' AND plan LIKE '%Seq Scan%') OR
       (how = 'index' AND plan LIKE '%Index Scan%') OR
       (how = 'bitmap' AND plan LIKE '%Bitmap Index Scan%') THEN
      EXECUTE sql INTO r;
      RETURN r;
    END IF;
  END LOOP;
  RETURN NULL;
END $$;
-- The queries of qs on tab whose index or bitmap scan differs from the
-- sequential scan, and how many of each kind ran.
CREATE FUNCTION mh_check(tab text, qs text[], OUT mismatches text[],
                         OUT n int)
LANGUAGE plpgsql AS $$
DECLARE q text; m text; s int[]; r int[];
BEGIN
  n := 0;
  FOREACH q IN ARRAY qs LOOP
    s := mh_run(tab, q, 'seq');
    FOREACH m IN ARRAY ARRAY['index', 'bitmap'] LOOP
      r := mh_run(tab, q, m);
      n := n + 1;
      IF r IS DISTINCT FROM s THEN
        mismatches := mismatches || (m || ': ' || q);
      END IF;
    END LOOP;
  END LOOP;
END $$;

-- int4[] under bark_int4_array_ops, alone and after a fixed column.
-- Reordering or repeating elements leaves the key set as it is: HOT.
-- Adding or removing an element, and going between {} and {1}, change
-- it: not HOT.  Changes to and from NULL are core's to decide (never
-- HOT).  {NULL} and {} give the same single NULL key, as the class
-- defines them (bark_extract_row_keys), so their entries are the same
-- bytes and an update between them is HOT; a scan that must tell them
-- apart rechecks the row, which is the new version.
CREATE TABLE mh (id int, tenant int, a int4[], v int)
  WITH (fillfactor = 10, autovacuum_enabled = off);
CREATE INDEX mh_a ON mh USING bark (a bark_int4_array_ops);
CREATE INDEX mh_ta ON mh USING bark (tenant, a bark_int4_array_ops);
INSERT INTO mh SELECT g, g % 3, ARRAY[g, g + 1, g + 2], 0
  FROM generate_series(1, 40) g;
INSERT INTO mh VALUES (41, 1, NULL, 0), (42, 1, '{}', 0), (43, 1, '{NULL}', 0),
  (44, 1, '{1}', 0), (45, 1, '{NULL,7}', 0);
BEGIN;
UPDATE mh SET v = 1;
SELECT 'other column' AS step, * FROM mh_count('mh');
UPDATE mh SET a = ARRAY[a[3], a[1], a[2], a[1]] WHERE id <= 40;
SELECT 'reorder, repeat' AS step, * FROM mh_count('mh');
UPDATE mh SET a = ARRAY[a[2], a[3], a[1], NULL] WHERE id = 1;
SELECT 'add NULL element' AS step, * FROM mh_count('mh');
UPDATE mh SET a = a || 99 WHERE id = 2;
SELECT 'add element' AS step, * FROM mh_count('mh');
UPDATE mh SET a = a[2:3] WHERE id = 3;
SELECT 'remove element' AS step, * FROM mh_count('mh');
UPDATE mh SET a = '{5}' WHERE id = 41;
UPDATE mh SET a = NULL WHERE id = 44;
SELECT 'to and from NULL' AS step, * FROM mh_count('mh');
UPDATE mh SET a = '{1}' WHERE id = 42;
SELECT '{} to {1}' AS step, * FROM mh_count('mh');
UPDATE mh SET a = '{}' WHERE id = 43;
UPDATE mh SET a = '{NULL,NULL}' WHERE id = 42;
SELECT '{NULL} to {}, {1} to {NULL,NULL}' AS step, * FROM mh_count('mh');
UPDATE mh SET a = '{NULL}' WHERE id = 43;
UPDATE mh SET a = '{7,7,NULL}' WHERE id = 45;
SELECT '{} to {NULL}, {NULL,7} to {7,7,NULL}' AS step, * FROM mh_count('mh');
UPDATE mh SET tenant = tenant + 10 WHERE id = 4;
SELECT 'fixed column' AS step, * FROM mh_count('mh');
COMMIT;
SELECT bark_index_check('mh_a', true), bark_index_check('mh_ta', true);
SELECT * FROM mh_check('mh', ARRAY['a && ''{1}''', 'a && ''{3,4,5}''', 'a @> ''{7}''',
  'a && ''{99}''', 'a <@ ''{1,2,3,4,5}''', 'a = ''{}''', 'a IS NULL', 'a @> ''{}''',
  'tenant = 11 AND a && ''{4,5}''', 'tenant = 1 AND a @> ''{}''']);

-- The column named again as an INCLUDE column is stored whole, and an
-- index-only scan returns it, so a reorder is not HOT there; the scan
-- returns the new value.
CREATE TABLE mh_i (id int, a int4[]) WITH (fillfactor = 10);
CREATE INDEX mh_i_a ON mh_i USING bark (a bark_int4_array_ops) INCLUDE (a, id);
INSERT INTO mh_i VALUES (1, '{1,2}'), (2, '{3,4}');
BEGIN;
UPDATE mh_i SET a = '{2,1}' WHERE id = 1;
SELECT 'INCLUDE of the column: reorder' AS step, * FROM mh_count('mh_i');
COMMIT;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF) SELECT id, a FROM mh_i WHERE a && '{1}';
SELECT id, a FROM mh_i WHERE a && '{1}';
RESET enable_seqscan;
RESET enable_bitmapscan;
SELECT bark_index_check('mh_i_a', true);

-- Markers: the marked class gives a marker for an array of two or more
-- elements.  An update that gives a marker the index does not hold is an
-- insert the index needs, so it is not HOT, even when the keys are the
-- same; one that drops the marker, or gives one the old value already
-- gave, is HOT.
CREATE TABLE mh_m (id int, a int4[]) WITH (fillfactor = 10);
CREATE INDEX mh_m_a ON mh_m USING bark (a bark_int4_array_marked_ops);
INSERT INTO mh_m VALUES (1, '{1}'), (2, '{2,3}'), (3, '{4,4}');
BEGIN;
UPDATE mh_m SET a = '{1,1}' WHERE id = 1;
SELECT 'marker appears' AS step, * FROM mh_count('mh_m');
UPDATE mh_m SET a = '{3,2}' WHERE id = 2;
SELECT 'marker kept' AS step, * FROM mh_count('mh_m');
UPDATE mh_m SET a = '{4}' WHERE id = 3;
SELECT 'marker goes' AS step, * FROM mh_count('mh_m');
COMMIT;
SELECT key, marker FROM bark_multikey_entries('mh_m_a') ORDER BY key, marker;
SELECT bark_index_check('mh_m_a', true);

-- GIN's query interface: equal keys of one row are one entry, so a
-- reorder is HOT for int4 (equalimage) as for bark_int4_array_ops.
-- numeric is not equalimage, and 1 and 1.0 compare equal with different
-- images.  An index that coalesces by GIN's key rule keeps the image of the
-- first row that formed an entry, so a row's own image need not be in it;
-- 1 to 1.0 is HOT there, as for a GIN index.  With an INCLUDE column the
-- index does not coalesce, keeps every row's image, and the update is not
-- HOT.  heapallindexed is refused for the coalescing index (bark_multikey_ginco).
CREATE TABLE mh_g (id int, a int4[]) WITH (fillfactor = 10);
CREATE INDEX mh_g_a ON mh_g USING bark (a bark_gin_int4_array_ops);
INSERT INTO mh_g SELECT g, ARRAY[g, g + 1] FROM generate_series(1, 20) g;
BEGIN;
UPDATE mh_g SET a = ARRAY[a[2], a[1], a[2]];
SELECT 'GIN interface: reorder' AS step, * FROM mh_count('mh_g');
UPDATE mh_g SET a = a || 0 WHERE id = 1;
SELECT 'GIN interface: add' AS step, * FROM mh_count('mh_g');
COMMIT;
SELECT bark_index_check('mh_g_a', true);
SELECT * FROM mh_check('mh_g', ARRAY['a && ''{1}''', 'a @> ''{0}''', 'a @> ''{2,3}''',
  'a <@ ''{0,1,2,3}''', 'a = ''{3,2}''']);

CREATE OPERATOR FAMILY mh_numeric_ops USING bark;
CREATE OPERATOR CLASS mh_numeric_ops
FOR TYPE numeric[] USING bark FAMILY mh_numeric_ops AS
    OPERATOR 1 && (anyarray, anyarray),
    OPERATOR 2 @> (anyarray, anyarray),
    OPERATOR 3 <@ (anyarray, anyarray),
    OPERATOR 4 = (anyarray, anyarray),
    FUNCTION 1 (numeric[], numeric[]) numeric_cmp(numeric, numeric),
    FUNCTION 7 ginarrayextract(anyarray, internal, internal),
    FUNCTION 10 ginqueryarrayextract(anyarray, internal, int2, internal,
                                     internal, internal, internal),
    FUNCTION 11 ginarrayconsistent(internal, int2, anyarray, int4, internal,
                                   internal, internal, internal),
    FUNCTION 13 ginarraytriconsistent(internal, int2, anyarray, int4,
                                      internal, internal, internal),
    STORAGE numeric;
CREATE TABLE mh_n (id int, a numeric[], b numeric[]) WITH (fillfactor = 10);
CREATE INDEX mh_n_a ON mh_n USING bark (a mh_numeric_ops);
CREATE INDEX mh_n_b ON mh_n USING bark (b mh_numeric_ops) INCLUDE (id);
INSERT INTO mh_n SELECT g, ARRAY[1, g + 1], ARRAY[1, g + 1]
  FROM generate_series(1, 20) g;
BEGIN;
UPDATE mh_n SET a = ARRAY[1.0, id + 1];
SELECT 'numeric 1 to 1.0, coalescing' AS step, * FROM mh_count('mh_n');
UPDATE mh_n SET b = ARRAY[1.0, id + 1];
SELECT 'numeric 1 to 1.0, INCLUDE' AS step, * FROM mh_count('mh_n');
UPDATE mh_n SET a = ARRAY[1.00, id + 1, 0] WHERE id = 1;
SELECT 'numeric add' AS step, * FROM mh_count('mh_n');
COMMIT;
SELECT bark_index_check('mh_n_a'), bark_index_check('mh_n_b', true);
SELECT * FROM mh_check('mh_n', ARRAY['a && ''{1.000}''', 'a @> ''{1,2}''', 'a = ''{1,3}''',
  'a @> ''{0}''', 'b && ''{1.000}''', 'b @> ''{1,2}''', 'b = ''{1,3}''']);

-- A unique index over the keys: core leaves a plain unique index's
-- columns out of what an access method compares (they are candidate keys),
-- so a reorder is not HOT there; a partial one is asked, and a reorder
-- that keeps the key set is HOT.  Its old entries reach the live version
-- through the HOT chain, so an insert of one of its keys still conflicts.
CREATE TABLE mh_u (id int, a int4[]) WITH (fillfactor = 10);
CREATE UNIQUE INDEX mh_u_a ON mh_u USING bark (a bark_int4_array_ops);
CREATE TABLE mh_up (id int, a int4[]) WITH (fillfactor = 10);
CREATE UNIQUE INDEX mh_up_a ON mh_up USING bark (a bark_int4_array_ops)
  WHERE id > 0;
INSERT INTO mh_u SELECT g, ARRAY[10 * g, 10 * g + 1] FROM generate_series(1, 10) g;
INSERT INTO mh_up SELECT * FROM mh_u;
BEGIN;
UPDATE mh_u SET a = ARRAY[a[2], a[1]];
UPDATE mh_up SET a = ARRAY[a[2], a[1], a[2]];
SELECT 'unique: reorder' AS step, * FROM mh_count('mh_u');
SELECT 'partial unique: reorder' AS step, * FROM mh_count('mh_up');
COMMIT;
INSERT INTO mh_u VALUES (100, '{11,500}');
INSERT INTO mh_up VALUES (100, '{11,500}');
INSERT INTO mh_up VALUES (100, '{12,500}');
SELECT bark_index_check('mh_u_a', true, true), bark_index_check('mh_up_a', true, true);
SELECT * FROM mh_check('mh_up', ARRAY['id > 0 AND a && ''{11}''',
  'id > 0 AND a && ''{20,500}''', 'id > 0 AND a @> ''{101,100}''']);

DROP TABLE mh, mh_i, mh_m, mh_g, mh_n, mh_u, mh_up;
DROP OPERATOR FAMILY mh_numeric_ops USING bark;
DROP FUNCTION mh_count(regclass), mh_run(text, text, text),
  mh_check(text, text[]);
DROP EXTENSION bark_multikey;
