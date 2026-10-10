-- Multikey operator classes in BARK's own operator families: the class
-- below validates and its procedures do what the design says, and an index
-- that uses it holds one entry per distinct key of each row.
CREATE EXTENSION bark_multikey;

-- The class is valid, lives in a BARK family, stores int4, and is reported
-- by \dAc and \dAp.
SELECT c.opcname, f.opfname, a.amname AS family_am,
       c.opckeytype::regtype AS storage, amvalidate(c.oid) AS valid
FROM pg_opclass c JOIN pg_opfamily f ON f.oid = c.opcfamily
JOIN pg_am a ON a.oid = f.opfmethod
WHERE c.opcname = 'bark_int4_array_ops';
\dAc bark integer[]
\dAp bark bark_int4_array_ops
\dAo bark bark_int4_array_ops

-- A BARK operator class by name, for the checks below.
CREATE FUNCTION mk_opc(text) RETURNS oid AS
  $$ SELECT oid FROM pg_opclass WHERE opcname = $1
       AND opcmethod = (SELECT oid FROM pg_am WHERE amname = 'bark') $$
  LANGUAGE sql STABLE STRICT;

-- Procedure 7 returns the elements as they come, NULLs flagged.
SELECT v, bark_multikey_keys(mk_opc('bark_int4_array_ops'), v)
FROM (VALUES ('{3,1,NULL,3}'::int4[]), ('{}'), ('{7}'), ('{NULL}')) AS t(v);

-- Procedure 8's boundaries, recheck and BarkQueryFlags, and procedure 9's
-- answer for the first boundary's key.
SELECT s, q, bark_multikey_boundaries(mk_opc('bark_int4_array_ops'), q, s)
FROM (VALUES ('{5,1,5}'::int4[]), ('{}'), ('{NULL}'), ('{2,NULL}')) AS t(q),
     (VALUES (1::int2), (2::int2), (3::int2), (4::int2)) AS u(s)
ORDER BY s, q;
-- |<| reads every row: the whole key range and the NULL entries; |>>|
-- the same, backward.
SELECT bark_multikey_boundaries(mk_opc('bark_int4_array_ops'), 0, 5::int2);
SELECT bark_multikey_boundaries(mk_opc('bark_int4_array_ops'), 0, 15::int2);
SELECT a, a |<| 0 AS least, a |>>| 0 AS greatest
FROM (VALUES ('{4,2,9}'::int4[]), ('{}'), ('{NULL,8}'), (NULL)) AS t(a);

-- What is never allowed on an extracted column: two of them, an exclusion
-- constraint, and NULLS NOT DISTINCT.  A unique index is allowed
-- (bark_multikey_unique has its tests).
CREATE TABLE mk (id int, a int4[], b int4[]);
INSERT INTO mk VALUES (1, '{1,2}', '{3}'), (2, '{}', NULL);
CREATE INDEX ON mk USING bark (a bark_int4_array_ops, b bark_int4_array_ops);
ALTER TABLE mk ADD CONSTRAINT mk_excl
  EXCLUDE USING bark (a bark_int4_array_ops WITH =);
CREATE UNIQUE INDEX ON mk USING bark (a bark_int4_array_ops) NULLS NOT DISTINCT;
-- With no unique index on an extracted column, ON CONFLICT has no arbiter
-- on one.
INSERT INTO mk VALUES (3, '{1}', NULL) ON CONFLICT (a) DO NOTHING;
-- An INCLUDE column has no operator class, so it is never extracted.
CREATE INDEX mk_incl ON mk USING bark (id) INCLUDE (a);
DROP INDEX mk_incl;
DROP TABLE mk;

-- The (key, heap TID) members an index on mk_t (a) should hold: each
-- distinct non-NULL element of the row's array, and one NULL key for a NULL
-- array, an empty one, or one with a NULL element.  Compared both ways with
-- what the index holds, and the index checked.
CREATE TABLE mk_t (id int, a int4[], b int4[]);
CREATE VIEW mk_expected AS
  SELECT DISTINCT e AS key, ctid AS tid FROM mk_t, unnest(a) e
    WHERE e IS NOT NULL
  UNION
  SELECT NULL, ctid FROM mk_t
    WHERE a IS NULL OR cardinality(a) = 0 OR array_position(a, NULL) IS NOT NULL;
CREATE FUNCTION mk_check(idx text) RETURNS TABLE (missing int8, extra int8,
                                                  members int8, markers int8)
LANGUAGE sql AS $$
  SELECT (SELECT count(*) FROM (SELECT key, tid FROM mk_expected EXCEPT
            SELECT key, tid FROM bark_multikey_entries(idx) WHERE NOT marker) x),
         (SELECT count(*) FROM (SELECT key, tid FROM bark_multikey_entries(idx)
            WHERE NOT marker EXCEPT SELECT key, tid FROM mk_expected) y),
         (SELECT count(*) FROM bark_multikey_entries(idx) WHERE NOT marker),
         (SELECT count(*) FROM bark_multikey_entries(idx) WHERE marker)
$$;

-- Duplicates, NULLs, empty arrays and NULL elements, by CREATE INDEX ...
INSERT INTO mk_t VALUES
  (1, '{3,1,2,3,1}', '{1}'), (2, '{2}', NULL), (3, NULL, '{}'),
  (4, '{}', '{4}'), (5, '{NULL}', NULL), (6, '{5,NULL,5,NULL}', '{5}'),
  (7, '{-1,0,1}', '{7}'), (8, '{2,2,2}', NULL);
CREATE INDEX mk_a ON mk_t USING bark (a bark_int4_array_ops);
SELECT bark_index_check('mk_a');
SELECT * FROM mk_check('mk_a');
SELECT key, count(*) FROM bark_multikey_entries('mk_a') GROUP BY key ORDER BY key;
SELECT * FROM bark_multikey_meta('mk_a');
-- ... and by insert, into an index with a fixed column before the extracted
-- one, a DESC NULLS FIRST extracted column, and an INCLUDE column.
CREATE INDEX mk_id_a ON mk_t USING bark (id, a bark_int4_array_ops);
CREATE INDEX mk_a_desc ON mk_t USING bark (a bark_int4_array_ops DESC NULLS FIRST);
CREATE INDEX mk_a_incl ON mk_t USING bark (a bark_int4_array_ops) INCLUDE (b);
TRUNCATE mk_t;
SELECT * FROM bark_multikey_meta('mk_a');
INSERT INTO mk_t VALUES
  (1, '{3,1,2,3,1}', '{1}'), (2, '{2}', NULL), (3, NULL, '{}'),
  (4, '{}', '{4}'), (5, '{NULL}', NULL), (6, '{5,NULL,5,NULL}', '{5}'),
  (7, '{-1,0,1}', '{7}'), (8, '{2,2,2}', NULL);
SELECT bark_index_check('mk_a'), bark_index_check('mk_id_a'),
       bark_index_check('mk_a_desc'), bark_index_check('mk_a_incl');
SELECT * FROM mk_check('mk_a');
SELECT * FROM mk_check('mk_a_desc');
SELECT * FROM mk_check('mk_a_incl');
SELECT array_agg(key ORDER BY n) AS keys_in_index_order
  FROM (SELECT key, row_number() OVER () AS n
          FROM bark_multikey_entries('mk_a_desc')) e;
SELECT count(*) FROM bark_multikey_entries('mk_id_a');
SELECT * FROM bark_multikey_meta('mk_a');
-- The flag is set by the first row with two keys, and never cleared: not
-- by deleting that row, nor by VACUUM.  bark_nkeys is the members CREATE
-- INDEX or VACUUM left.
CREATE TABLE mk_one (a int4[]);
INSERT INTO mk_one VALUES ('{1}'), ('{2,2}'), (NULL), ('{}');
CREATE INDEX mk_one_a ON mk_one USING bark (a bark_int4_array_ops);
SELECT * FROM bark_multikey_meta('mk_one_a');
INSERT INTO mk_one VALUES ('{3}'), ('{4,4,NULL}');
SELECT * FROM bark_multikey_meta('mk_one_a');
DELETE FROM mk_one WHERE a = '{4,4,NULL}';
VACUUM mk_one;
SELECT * FROM bark_multikey_meta('mk_one_a');
SELECT bark_index_check('mk_one_a');
DROP TABLE mk_one;

-- Scans of such an index return each row once, whatever its keys
-- (bark_multikey_scan has the scan tests).
SET enable_seqscan = off;
SELECT count(*) FROM mk_t WHERE a && '{2}';
SELECT count(*) FROM mk_t WHERE a IS NULL;
RESET enable_seqscan;
SELECT count(*) FROM mk_t WHERE a && '{2}';
-- The index is not ordered by the extracted column's values, nor by any
-- column after it (plancat gives it no sort family there), so queries that
-- want an order on the column plan and run without asking the index for
-- one: ORDER BY, DISTINCT, UNION and a merge join.  On mk_id_a the planner
-- sees an order on id only.
SELECT id FROM mk_t ORDER BY a, id LIMIT 3;
SELECT DISTINCT a FROM mk_t ORDER BY a LIMIT 2;
SELECT a FROM mk_t WHERE b IS NULL UNION SELECT a FROM mk_t WHERE b = '{1}'
  ORDER BY a;
SET enable_hashjoin = off;
SET enable_nestloop = off;
EXPLAIN (COSTS OFF) SELECT count(*) FROM mk_t x JOIN mk_t y USING (a);
SELECT count(*) FROM mk_t x JOIN mk_t y USING (a);
RESET enable_hashjoin;
RESET enable_nestloop;
SET enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT id FROM mk_t ORDER BY id, a;
RESET enable_seqscan;

-- Big arrays split leaves: each row's keys go in one pass, staying on a
-- leaf, stepping right or descending again.  Then VACUUM removes every
-- member of a deleted row, whatever leaf it is on.
TRUNCATE mk_t;
INSERT INTO mk_t SELECT g, ARRAY(SELECT (g * 37 + i * 101) % 20000
                                 FROM generate_series(1, 40) i)
  FROM generate_series(1, 2000) g;
INSERT INTO mk_t SELECT 3000, ARRAY(SELECT generate_series(1, 20000, 3));
INSERT INTO mk_t SELECT 3001, ARRAY(SELECT generate_series(20000, 1, -7));
SELECT pg_relation_size('mk_a') / 8192 > 20 AS several_leaves;
SELECT bark_index_check('mk_a'), bark_index_check('mk_id_a'),
       bark_index_check('mk_a_desc'), bark_index_check('mk_a_incl');
SELECT * FROM mk_check('mk_a');
SELECT * FROM mk_check('mk_a_desc');
DELETE FROM mk_t WHERE id % 3 = 0 OR id = 3000;
VACUUM mk_t;
SELECT bark_index_check('mk_a'), bark_index_check('mk_a_desc');
SELECT * FROM mk_check('mk_a');
SELECT nkeys = (SELECT count(*) FROM mk_expected) AS nkeys_ok
  FROM bark_multikey_meta('mk_a');
-- REINDEX and CREATE INDEX of the same rows, serial and parallel, hold
-- the same members.
REINDEX INDEX mk_a;
SELECT bark_index_check('mk_a');
SELECT * FROM mk_check('mk_a');
SET max_parallel_maintenance_workers = 2;
SET min_parallel_table_scan_size = 0;
CREATE INDEX mk_a_par ON mk_t USING bark (a bark_int4_array_ops);
RESET max_parallel_maintenance_workers;
RESET min_parallel_table_scan_size;
SELECT bark_index_check('mk_a_par');
SELECT * FROM mk_check('mk_a_par');
SELECT * FROM bark_multikey_meta('mk_a_par') m1, bark_multikey_meta('mk_a') m2
  WHERE m1 IS DISTINCT FROM m2;
DROP INDEX mk_a_par;

-- Oversized keys: a wide fixed column makes every entry of the row too
-- large for a page, by CREATE INDEX (its second pass runs procedure 7
-- again) and by insert.
CREATE TABLE mk_big (a int4[], t text);
CREATE FUNCTION mk_wide(int) RETURNS text LANGUAGE sql AS
  $$ SELECT string_agg(md5(($1 * 1000 + i)::text), '') FROM generate_series(1, 300) i $$;
INSERT INTO mk_big VALUES ('{1,2,3}', mk_wide(1)), ('{2,4}', 'small'),
  ('{3,3}', mk_wide(2)), (NULL, mk_wide(3));
CREATE INDEX mk_big_at ON mk_big USING bark (a bark_int4_array_ops, t);
INSERT INTO mk_big VALUES ('{5,1}', mk_wide(4)), ('{6}', 'small'), ('{}', mk_wide(5));
SELECT bark_index_check('mk_big_at');
SELECT key, count(*) FROM bark_multikey_entries('mk_big_at') GROUP BY key ORDER BY key;
SELECT * FROM bark_multikey_meta('mk_big_at');
DROP TABLE mk_big;
DROP FUNCTION mk_wide(int);

-- Markers.  The marked class records "has held an array of two or more
-- elements" as the key -2147483648 with the reserved heap TID, once, by
-- CREATE INDEX and by insert; it is no row and holds no member.
CREATE TABLE mk_m (id int, a int4[]);
INSERT INTO mk_m VALUES (1, '{1}'), (2, NULL);
CREATE INDEX mk_m_a ON mk_m USING bark (a bark_int4_array_marked_ops);
SELECT bark_multikey_has_marker('mk_m_a', -2147483648);
INSERT INTO mk_m VALUES (3, '{1,2}'), (4, '{2,3}'), (5, '{4}');
SELECT bark_multikey_has_marker('mk_m_a', -2147483648),
       bark_multikey_has_marker('mk_m_a', 1);
SELECT key, tid, marker FROM bark_multikey_entries('mk_m_a') WHERE marker;
SELECT bark_index_check('mk_m_a');
SELECT * FROM bark_multikey_meta('mk_m_a');
-- A new backend has not seen the marker, finds it in the index and does not
-- insert it again.
\c
INSERT INTO mk_m VALUES (6, '{5,6}');
SELECT count(*) FROM bark_multikey_entries('mk_m_a') WHERE marker;
-- CREATE INDEX writes it once, serial or parallel, with NULL in the fixed
-- column, and VACUUM keeps it when every row is gone.
INSERT INTO mk_m SELECT g, ARRAY[g, g + 1] FROM generate_series(10, 3000) g;
CREATE INDEX mk_m_id_a ON mk_m USING bark (id, a bark_int4_array_marked_ops);
SET max_parallel_maintenance_workers = 2;
SET min_parallel_table_scan_size = 0;
CREATE INDEX mk_m_a2 ON mk_m USING bark (a bark_int4_array_marked_ops);
RESET max_parallel_maintenance_workers;
RESET min_parallel_table_scan_size;
SELECT bark_index_check('mk_m_id_a'), bark_index_check('mk_m_a2');
SELECT count(*) FILTER (WHERE marker) AS markers,
       count(*) FILTER (WHERE key IS NULL) AS null_id
  FROM bark_multikey_entries('mk_m_id_a');
SELECT count(*) FROM bark_multikey_entries('mk_m_a2') WHERE marker;
SELECT bark_multikey_has_marker('mk_m_a2', -2147483648);
INSERT INTO mk_m VALUES (7, '{-2147483648}');
DELETE FROM mk_m;
VACUUM mk_m;
SELECT key, marker FROM bark_multikey_entries('mk_m_a');
SELECT bark_index_check('mk_m_a'), bark_index_check('mk_m_id_a');
SELECT * FROM bark_multikey_meta('mk_m_a');
-- Markers are for a class's own use; scalar indexes have none.
SELECT bark_multikey_has_marker('mk_m_id_a', 1);
CREATE INDEX mk_m_id ON mk_m USING bark (id);
SELECT bark_multikey_has_marker('mk_m_id', 1);
DROP TABLE mk_m;

-- An unlogged index sets the flag without WAL.
CREATE UNLOGGED TABLE mk_unlogged (a int4[]);
CREATE INDEX mk_unlogged_a ON mk_unlogged USING bark (a bark_int4_array_ops);
INSERT INTO mk_unlogged VALUES ('{1,2}'), ('{3}');
SELECT * FROM bark_multikey_meta('mk_unlogged_a');
SELECT bark_index_check('mk_unlogged_a');
DROP TABLE mk_unlogged;
DROP VIEW mk_expected;
DROP FUNCTION mk_check(text);
DROP TABLE mk_t;

-- barkvalidate's rules for a BARK family.  Each class below breaks one.
CREATE FUNCTION mk_cmp(int4[], int4[]) RETURNS int4
  AS 'btarraycmp' LANGUAGE internal IMMUTABLE STRICT;
CREATE FUNCTION mk_query6(int4[], int2, internal, internal, internal, internal)
  RETURNS void AS '$libdir/bark_multikey', 'bark_multikey_extract_query'
  LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION mk_inrange(int4, int4, int4, bool, bool) RETURNS bool
  AS 'in_range_int4_int4' LANGUAGE internal IMMUTABLE STRICT;
CREATE FUNCTION mk_fetch(int4) RETURNS int4[]
  AS 'SELECT ARRAY[$1]' LANGUAGE sql IMMUTABLE STRICT;

-- Procedure 1 registered under its argument types, as CREATE OPERATOR
-- CLASS registers a comparator by default, is never found; the hint says
-- how to register it.
CREATE OPERATOR CLASS mk_bad_order FOR TYPE int4[] USING bark AS
    OPERATOR 1 && (anyarray, anyarray),
    FUNCTION 1 btint4cmp(int4, int4),
    FUNCTION 7 bark_multikey_extract_value(int4[], internal, internal),
    STORAGE int4;
SELECT amvalidate(mk_opc('mk_bad_order'));
-- Without procedure 7 a class in a BARK family extracts nothing; it is
-- refused, rather than taken for a scalar class.
CREATE OPERATOR CLASS mk_no_extract FOR TYPE int4[] USING bark AS
    OPERATOR 1 && (anyarray, anyarray),
    FUNCTION 1 (int4[], int4[]) btint4cmp(int4, int4),
    STORAGE int4;
SELECT amvalidate(mk_opc('mk_no_extract'));
-- CREATE INDEX refuses it too.  A scalar class in a family of BARK's own
-- would have its operators read as btree strategies: here < is strategy 4
-- and > strategy 2, which BARK would take for >= and <, positioning and
-- stopping the scan wrongly.
CREATE OPERATOR CLASS mk_scalar_renumbered FOR TYPE int4 USING bark AS
    OPERATOR 4 < (int4, int4),
    OPERATOR 2 > (int4, int4),
    FUNCTION 1 (int4, int4) btint4cmp(int4, int4);
CREATE TABLE mk_scalar (a int4, b int4[]);
INSERT INTO mk_scalar SELECT g, ARRAY[g] FROM generate_series(1, 1000) g;
CREATE INDEX mk_scalar_i ON mk_scalar USING bark (a mk_scalar_renumbered);
CREATE INDEX mk_scalar_i ON mk_scalar USING bark (b mk_no_extract);
DROP TABLE mk_scalar;
DROP OPERATOR CLASS mk_scalar_renumbered USING bark CASCADE;
-- Procedure 8 without 9.  A six-argument procedure 8 is fine.
CREATE OPERATOR CLASS mk_no_recheck FOR TYPE int4[] USING bark AS
    OPERATOR 1 && (anyarray, anyarray),
    FUNCTION 1 (int4[], int4[]) btint4cmp(int4, int4),
    FUNCTION 7 bark_multikey_extract_value(int4[], internal, internal),
    FUNCTION 8 mk_query6(int4[], int2, internal, internal, internal, internal),
    STORAGE int4;
SELECT amvalidate(mk_opc('mk_no_recheck'));
-- in_range (3) is refused, and the comparator must take the storage type,
-- not the input type.
CREATE OPERATOR CLASS mk_bad_procs FOR TYPE int4[] USING bark AS
    OPERATOR 1 && (anyarray, anyarray),
    FUNCTION 1 (int4[], int4[]) mk_cmp(int4[], int4[]),
    FUNCTION 3 (int4[], int4[]) mk_inrange(int4, int4, int4, bool, bool),
    FUNCTION 7 bark_multikey_extract_value(int4[], internal, internal),
    STORAGE int4;
SELECT amvalidate(mk_opc('mk_bad_procs'));
-- Procedure 14, value (key), and any strategy number are accepted.
CREATE OPERATOR CLASS mk_fetch_ok FOR TYPE int4[] USING bark AS
    OPERATOR 1 && (anyarray, anyarray),
    OPERATOR 300 @> (anyarray, anyarray),
    FUNCTION 1 (int4[], int4[]) btint4cmp(int4, int4),
    FUNCTION 7 bark_multikey_extract_value(int4[], internal, internal),
    FUNCTION 14 mk_fetch(int4),
    STORAGE int4;
SELECT amvalidate(mk_opc('mk_fetch_ok'));

-- A class in a btree family keeps btree's rules.  CREATE OPERATOR CLASS
-- refuses the strategies and support functions it refused before BARK's
-- amstrategies and amsupport grew for its own families, so a BARK class
-- cannot make btree's classes in the family invalid, nor give the class a
-- storage type.
CREATE OPERATOR FAMILY mk_bt USING btree;
CREATE OPERATOR CLASS mk_bt_wide FOR TYPE int4 USING bark FAMILY mk_bt AS
    OPERATOR 1 <, OPERATOR 9 <>,
    FUNCTION 1 btint4cmp(int4, int4);
CREATE OPERATOR CLASS mk_bt_wide FOR TYPE int4 USING bark FAMILY mk_bt AS
    OPERATOR 1 <,
    FUNCTION 1 btint4cmp(int4, int4),
    FUNCTION 7 (int4, int4) bark_multikey_extract_value(int4[], internal, internal);
CREATE OPERATOR CLASS mk_bt_storage FOR TYPE int4 USING bark FAMILY mk_bt AS
    OPERATOR 1 <, OPERATOR 2 <=, OPERATOR 3 =, OPERATOR 4 >=, OPERATOR 5 >,
    FUNCTION 1 btint4cmp(int4, int4),
    STORAGE int8;
-- The scalar classes are all still valid, btree's and BARK's.
SELECT a.amname, count(*) FILTER (WHERE NOT amvalidate(c.oid)) AS invalid
FROM pg_opclass c JOIN pg_am a ON a.oid = c.opcmethod
WHERE a.amname IN ('btree', 'bark') AND c.oid < 16384
GROUP BY 1 ORDER BY 1;

DROP OPERATOR CLASS mk_bad_order USING bark CASCADE;
DROP OPERATOR CLASS mk_no_extract USING bark CASCADE;
DROP OPERATOR CLASS mk_no_recheck USING bark CASCADE;
DROP OPERATOR CLASS mk_bad_procs USING bark CASCADE;
DROP OPERATOR CLASS mk_fetch_ok USING bark CASCADE;
DROP OPERATOR FAMILY mk_bt USING btree;
DROP FUNCTION mk_opc(text), mk_cmp(int4[], int4[]), mk_query6(int4[], int2, internal, internal, internal, internal),
  mk_inrange(int4, int4, int4, bool, bool), mk_fetch(int4);

-- Bottom-up deletion on a leaf holding two entries of one row: a row of a
-- multikey index has an entry per key, and when two of them are on the leaf
-- that a non-HOT update fills, its TID must be offered to the table AM only
-- once (heapam's sort refuses two equal TIDs, an assertion failure).  The
-- update leaves a unchanged and indexes v, so each new version comes back
-- to the full leaf with indexUnchanged set.  Both entries of row 0 go
-- together: deleted with the row's old version, or both kept.
CREATE TABLE mk_bu (id int, a int4[], v int)
  WITH (fillfactor = 100, autovacuum_enabled = off);
INSERT INTO mk_bu SELECT g, ARRAY[g], 0 FROM generate_series(1, 2000) g;
INSERT INTO mk_bu VALUES (0, '{1,2}', 0);
CREATE INDEX mk_bu_a ON mk_bu USING bark (a bark_int4_array_ops)
  WITH (fillfactor = 100);
CREATE INDEX mk_bu_v ON mk_bu (v);
UPDATE mk_bu SET v = 1 WHERE id BETWEEN 3 AND 40;
UPDATE mk_bu SET v = 2 WHERE id = 0;
UPDATE mk_bu SET v = 3 WHERE id BETWEEN 3 AND 40;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*), count(DISTINCT id) FROM mk_bu WHERE a && '{1,2,3,40}';
RESET enable_seqscan;
RESET enable_bitmapscan;
SELECT count(*), count(DISTINCT id) FROM mk_bu WHERE a && '{1,2,3,40}';
SELECT bark_index_check('mk_bu_a');
WITH heap AS (SELECT DISTINCT e AS key, ctid AS tid FROM mk_bu, unnest(a) e),
     idx AS (SELECT key, tid FROM bark_multikey_entries('mk_bu_a')
              WHERE NOT marker AND tid IN (SELECT ctid FROM mk_bu))
SELECT (SELECT count(*) FROM (TABLE heap EXCEPT TABLE idx) x) AS missing,
       (SELECT count(*) FROM (TABLE idx EXCEPT TABLE heap) y) AS extra;
DROP TABLE mk_bu;
DROP EXTENSION bark_multikey;
