-- Multikey operator classes in BARK's own operator families.  M1a adds the
-- catalog side only: the class below validates, its procedures do what the
-- design says, and CREATE INDEX refuses every index that uses it, with the
-- error the index's shape calls for.
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
-- |<| reads every row: the whole key range and the NULL entries.
SELECT bark_multikey_boundaries(mk_opc('bark_int4_array_ops'), 0, 5::int2);
SELECT a, a |<| 0 AS least
FROM (VALUES ('{4,2,9}'::int4[]), ('{}'), ('{NULL,8}'), (NULL)) AS t(a);

-- CREATE INDEX refuses an index with an extracted column until the insert
-- path extracts keys; first the errors for what M1 never allows.
CREATE TABLE mk (id int, a int4[], b int4[]);
INSERT INTO mk VALUES (1, '{1,2}', '{3}'), (2, '{}', NULL);
CREATE INDEX ON mk USING bark (a bark_int4_array_ops);
CREATE INDEX ON mk USING bark (id, a bark_int4_array_ops);
CREATE INDEX ON mk USING bark (a bark_int4_array_ops) INCLUDE (b);
CREATE INDEX ON mk USING bark (a bark_int4_array_ops, b bark_int4_array_ops);
CREATE UNIQUE INDEX ON mk USING bark (a bark_int4_array_ops);
ALTER TABLE mk ADD CONSTRAINT mk_excl
  EXCLUDE USING bark (a bark_int4_array_ops WITH =);
-- With no unique or exclusion index on an extracted column, ON CONFLICT
-- has no arbiter on one.
INSERT INTO mk VALUES (3, '{1}', NULL) ON CONFLICT (a) DO NOTHING;
CREATE UNLOGGED TABLE mk_unlogged (a int4[]);
CREATE INDEX ON mk_unlogged USING bark (a bark_int4_array_ops);
-- An INCLUDE column has no operator class, so it is never extracted.
CREATE INDEX mk_incl ON mk USING bark (id) INCLUDE (a);
DROP INDEX mk_incl;
-- Nothing was left behind, except what a failed CREATE INDEX CONCURRENTLY
-- always leaves: an invalid index.
SELECT count(*) FROM pg_index WHERE indrelid IN ('mk'::regclass, 'mk_unlogged'::regclass);
CREATE INDEX CONCURRENTLY mk_cic ON mk USING bark (a bark_int4_array_ops);
SELECT indexrelid::regclass, indisvalid FROM pg_index WHERE indrelid = 'mk'::regclass;
DROP INDEX mk_cic;

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
DROP TABLE mk, mk_unlogged;
DROP FUNCTION mk_opc(text), mk_cmp(int4[], int4[]), mk_query6(int4[], int2, internal, internal, internal, internal),
  mk_inrange(int4, int4, int4, bool, bool), mk_fetch(int4);
DROP EXTENSION bark_multikey;
